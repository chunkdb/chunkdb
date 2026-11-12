#include <cassert>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "txn_test_utils.hpp"
#include "typed_chunk_form.hpp"

namespace {
using namespace chunkdb;
using Parameters = std::vector<std::optional<std::string>>;
enum class Write { kSetBlock, kDeleteBlock, kSetChunk };

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-ordinary-write-failure"};
    CatalogConfig config;
    std::shared_ptr<TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    SessionState session;
    explicit Fixture(DurabilityMode mode) {
        config.data_dir = directory.path();
        config.background_maintenance = false;
        config.default_options.durability_mode = mode;
        config.default_options.checkpoint_update_interval = 1000000U;
        config.default_options.checkpoint_wal_bytes = 1000000U;
        config.default_options.wal_group_commit_updates = 1U;
        Open();
        assert(Run("CREATE TABLE t (n u8) CHUNK 2 x 2 LARGE 1 x 1 CHUNK COLUMNS (owner u8 DEFAULT 0)") == "+OK\r\n");
    }
    void Open() {
        catalog = std::make_shared<TableCatalog>(config);
        engine = std::make_unique<CommandEngine>(EngineConfig{.require_auth = false}, catalog);
        session = {};
        assert(Run("HELLO 3").starts_with("%"));
    }
    void Reopen() {
        engine.reset();
        catalog.reset();
        Open();
    }
    std::string Run(std::string_view statement, const Parameters& parameters = {}) {
        return engine->Execute(session, std::string(statement) + "\r\n", parameters);
    }
    ChunkState State() {
        auto lease = catalog->Require("t")->Acquire();
        return lease->store().ReadChunkState(ChunkCoord{});
    }
    void Seed() {
        assert(Run("SET CHUNK 0 0 IN t META owner=5").starts_with(":"));
        assert(Run("SET BLOCK 0 0 IN t n=7").starts_with(":"));
        assert(Run("SET BLOCK 1 1 IN t n=9").starts_with(":"));
        auto lease = catalog->Require("t")->Acquire();
        lease->store().WalBarrier();
    }
    std::string Apply(Write kind) {
        if (kind == Write::kSetBlock) return Run("SET BLOCK 0 0 IN t n=3");
        if (kind == Write::kDeleteBlock) return Run("DELETE BLOCK 0 0 FROM t");
        auto next = State();
        {
            auto lease = catalog->Require("t")->Acquire();
            lease->store().SetBlockInState(next, 0, 0, {{"n", std::uint64_t{3U}}});
            lease->store().SetBlockInState(next, 1, 1, {{"n", std::uint64_t{4U}}});
        }
        const auto bytes = EncodeSectionedChunkState(catalog->Require("t")->geometry(), next, kVarMaxChunkBytesLimit);
        return Run("SET CHUNK 0 0 IN t $1", {std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size())});
    }
};

void Same(const ChunkState& actual, const ChunkState& expected) {
    assert(actual.version == expected.version && actual.commit_time_ms == expected.commit_time_ms);
    assert(actual.written == expected.written && actual.payload == expected.payload);
    assert(actual.presence_bitmap == expected.presence_bitmap && actual.vars == expected.vars);
    assert(static_cast<bool>(actual.chunk_columns) == static_cast<bool>(expected.chunk_columns));
    if (actual.chunk_columns) assert(actual.chunk_columns->bytes == expected.chunk_columns->bytes);
}

void RefusedWrite(Write kind, DurabilityMode mode, bool seeded, const char* failpoint) {
    Fixture fixture(mode);
    if (seeded) fixture.Seed();
    const auto before = fixture.State();
    std::string reply;
    {
        txn_test::ScopedEnv failure(failpoint, "1");
        reply = fixture.Apply(kind);
        const char* remaining = std::getenv(failpoint);
        assert(remaining == nullptr || remaining[0] == '\0');
    }
    if (reply != "-ERR INTERNAL reason=internal internal error\r\n") {
        std::cerr << "ordinary write kind=" << static_cast<unsigned>(kind) << " mode=" << DurabilityModeName(mode)
                  << " seeded=" << seeded << " expected refusal, received " << reply;
        assert(false);
    }
    Same(fixture.State(), before);
    // A later write uses a different chunk so it cannot hide the rejected
    // chunk's state or supersede a frame incorrectly left in its WAL.
    assert(fixture.Run("SET BLOCK 4 4 IN t n=43").starts_with(":"));
    Same(fixture.State(), before);
    fixture.Reopen();
    Same(fixture.State(), before);
    assert(fixture.Run("GET BLOCK 4 4 FROM t") == "*1\r\n:43\r\n");
    assert(fixture.Run("SET BLOCK 0 0 IN t n=11").starts_with(":"));
    assert(fixture.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:11\r\n");
}
}

int main() {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal, DurabilityMode::kFsyncCheckpoint}) {
        for (const auto kind : {Write::kSetBlock, Write::kDeleteBlock, Write::kSetChunk})
            RefusedWrite(kind, mode, true, "CHUNKDB_FAILPOINT_VERSION_RESERVE_FAIL_ONCE");
        for (const auto kind : {Write::kSetBlock, Write::kSetChunk})
            RefusedWrite(kind, mode, false, "CHUNKDB_FAILPOINT_VERSION_RESERVE_FAIL_ONCE");
        for (const auto kind : {Write::kDeleteBlock, Write::kSetChunk})
            RefusedWrite(kind, mode, true, "CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE");
    }
}
