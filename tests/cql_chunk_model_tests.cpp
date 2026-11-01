#include <cassert>
#include <charconv>
#include <iostream>

#include "chunkdb/chunk_columns.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "chunk_coordinate_index.hpp"
#include "sectioned_chunk_form.hpp"
#include "typed_chunk_form.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;
using Parameters = std::vector<std::optional<std::string>>;

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-cql-chunk-model"};
    std::shared_ptr<TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    SessionState session;
    Fixture() {
        CatalogConfig config;
        config.data_dir = directory.path();
        config.background_maintenance = false;
        catalog = std::make_shared<TableCatalog>(config);
        engine = std::make_unique<CommandEngine>(EngineConfig{.require_auth = false}, catalog);
        assert(engine->Execute(session, "HELLO 3\r\n").starts_with("%"));
    }
    std::string Run(std::string_view statement, const Parameters& parameters = {}) {
        return engine->Execute(session, std::string(statement) + "\r\n", parameters);
    }
};

void Reply(const std::string& actual, std::string_view expected) {
    if (actual != expected) {
        std::cerr << "expected " << expected << ", received " << actual << '\n';
        assert(false);
    }
}

void Error(const std::string& actual, std::string_view code) {
    if (!actual.starts_with("-ERR " + std::string(code) + " ")) {
        std::cerr << "expected " << code << ", received " << actual << '\n';
        assert(false);
    }
}

std::uint64_t Revision(const std::string& reply) {
    if (!reply.starts_with(":")) std::cerr << "expected producing revision, received " << reply << '\n';
    assert(reply.starts_with(":"));
    std::uint64_t revision = 0U;
    const auto parsed = std::from_chars(reply.data() + 1U, reply.data() + reply.size() - 2U, revision);
    assert(parsed.ec == std::errc{} && parsed.ptr == reply.data() + reply.size() - 2U && revision != 0U);
    return revision;
}

std::span<const std::uint8_t> Bulk(const std::string& reply) {
    assert(reply.starts_with("$"));
    const auto end = reply.find("\r\n");
    std::size_t count = 0U;
    const auto parsed = std::from_chars(reply.data() + 1U, reply.data() + end, count);
    assert(parsed.ec == std::errc{} && parsed.ptr == reply.data() + end && end + 4U + count == reply.size());
    assert(reply.ends_with("\r\n"));
    return {reinterpret_cast<const std::uint8_t*>(reply.data() + end + 2U), count};
}

ChunkState Read(Fixture& fixture, std::string_view command, std::string_view name = "t", bool private_state = false) {
    const auto reply = fixture.Run(command);
    const auto bytes = Bulk(reply);
    const auto header = ParseChunkFormSections(bytes, bytes.size(), ChunkFormReadMode::kRead).header;
    assert(((header.flags & kChunkFormPrivate) != 0U) == private_state);
    if (private_state) assert(header.chunk_version == 0U && header.commit_time_ms == 0U);
    else assert(header.chunk_version != 0U && header.commit_time_ms != 0U);
    const auto geometry = fixture.catalog->Require(name)->geometry();
    auto state = DecodeSectionedChunkState(geometry, bytes, kVarMaxChunkBytesLimit);
    state.version = header.chunk_version;
    state.commit_time_ms = header.commit_time_ms;
    state.written = true;
    return state;
}

std::string Form(const Geometry& geometry, const ChunkState& state, std::span<const std::size_t> projection = {}) {
    const auto bytes = EncodeSectionedChunkState(geometry, state, kVarMaxChunkBytesLimit, projection);
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

void MetadataProjectionAndTombstone() {
    Fixture f;
    Reply(f.Run("CREATE TABLE t (a u8 DEFAULT 7, b u16 DEFAULT 9) CHUNK 2 x 2 CHUNK COLUMNS (owner u8 DEFAULT 0, tag bytes(128) NULL)"), "+OK\r\n");
    Reply(f.Run("GET CHUNK 0 0 FROM t"), "_\r\n");
    const auto plan = f.engine->PlanPayload(f.session, "SET CHUNK 0 0 IN t META owner=$1, tag=$2\r\n");
    assert(plan.plan == CommandEngine::PayloadPlan::kParameters);
    assert((plan.parameter_limits == std::vector<std::size_t>{8U, 128U}));
    const auto first = Revision(f.Run("SET CHUNK 0 0 IN t META owner=3, tag=$1", {"abc"}));
    auto state = Read(f, "GET CHUNK 0 0 FROM t");
    assert(state.version == first && state.presence_bitmap == std::vector<std::uint8_t>{0U});
    const auto table = f.catalog->Require("t");
    auto geometry = table->geometry();
    const auto metadata = DecodeChunkColumns(geometry.chunk_schema(), state.chunk_columns->bytes);
    assert(std::get<std::uint64_t>(metadata[0]) == 3U && std::get<BytesValue>(metadata[1]).bytes.size() == 3U);
    Reply(f.Run("ALTER TABLE t SET var_max_chunk_bytes=16"), "+OK\r\n");
    const auto full_state = Form(geometry, state);
    const auto full_plan = f.engine->PlanPayload(f.session, "SET CHUNK 0 0 IN t $1\r\n");
    assert(full_plan.plan == CommandEngine::PayloadPlan::kParameters && full_plan.parameter_limits.size() == 1U);
    assert(full_plan.parameter_limits[0] >= full_state.size());
    Revision(f.Run("SET CHUNK 0 0 IN t $1", {full_state}));
    Revision(f.Run("SET BLOCK 0 0 IN t a=11, b=12"));
    state = Read(f, "GET CHUNK 0 0 FROM t");
    {
        auto lease = table->Acquire();
        lease->store().SetBlockInState(state, 0, 0, {{"a", std::uint64_t{5U}}, {"b", std::uint64_t{99U}}});
    }
    const std::vector<std::size_t> projection{0U};
    const auto patch = Form(geometry, state, projection);
    Revision(f.Run("SET CHUNK 0 0 IN t COLUMNS a $1", {patch}));
    Reply(f.Run("GET BLOCK 0 0 FROM t"), "*2\r\n:5\r\n:12\r\n");
    Error(f.Run("SET CHUNK 0 0 IN t COLUMNS a $1 IF ABSENT", {patch}), "VERSION_MISMATCH");
    const auto described = f.Run("DESCRIBE t");
    assert(described.find("chunk_columns") != std::string::npos && described.find("owner") != std::string::npos);
    Revision(f.Run("DELETE BLOCK 0 0 FROM t"));
    const auto last = Revision(f.Run("SET CHUNK 0 0 IN t META owner=0, tag=NULL"));
    {
        auto lease = table->Acquire();
        lease->store().CheckpointForTests(0, 0);
    }
    state = Read(f, "GET CHUNK 0 0 FROM t");
    assert(state.version == last && state.presence_bitmap == std::vector<std::uint8_t>{0U});
    assert(f.Run("SCAN CHUNKS FROM t").starts_with("%5\r\n$6\r\nchunks\r\n*0\r\n"));
    assert(f.Run("SCAN CHUNKS FROM t SINCE 0 WITH DATA").starts_with(
        "%5\r\n$6\r\nchunks\r\n*1\r\n*4\r\n:0\r\n:0\r\n:" + std::to_string(last) + "\r\n_\r\n"));
    Revision(f.Run("SET CHUNK 0 0 IN t COLUMNS a $1 IF ABSENT", {patch}));
    Reply(f.Run("GET BLOCK 0 0 FROM t"), "*2\r\n:5\r\n:9\r\n");
    Error(f.Run("GET BLOCK 0 0 0 FROM t"), "INVALID_ARGUMENT");
    Error(f.Run("GET CHUNK 0 0 0 FROM t"), "INVALID_ARGUMENT");
    Error(f.Run("GET AREA 0 0 0 TO 1 1 1 FROM t"), "INVALID_ARGUMENT");
}

void RequiredMetadataAndPrivateForms() {
    Fixture f;
    Reply(f.Run("CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2 CHUNK COLUMNS (owner u8 REQUIRED)"), "+OK\r\n");
    const auto table = f.catalog->Require("t");
    ChunkState patch_state;
    {
        auto lease = table->Acquire();
        patch_state = lease->store().ReadChunkState(0, 0);
    }
    const std::vector<std::size_t> projection{0U};
    const auto patch = Form(table->geometry(), patch_state, projection);
    Error(f.Run("SET CHUNK 0 0 IN t COLUMNS a $1", {patch}), "INVALID_ARGUMENT");
    Reply(f.Run("BEGIN"), "+OK\r\n");
    Error(f.Run("SET CHUNK 0 0 IN t COLUMNS a $1", {patch}), "INVALID_ARGUMENT");
    Reply(f.Run("SET CHUNK 0 0 IN t META owner=0"), "_\r\n");
    const auto own = Read(f, "GET CHUNK 0 0 FROM t", "t", true);
    assert(own.chunk_columns && own.presence_bitmap == std::vector<std::uint8_t>{0U});
    Revision(f.Run("COMMIT"));
    Read(f, "GET CHUNK 0 0 FROM t");
    {
        auto current = table->Acquire();
        current->store().CheckpointForTests(0, 0);
        const auto root = f.directory.path() / "tables/t";
        assert(!std::filesystem::exists(ChunkDataPath(root, current->store().geometry(), {0, 0})));
        assert(!std::filesystem::exists(ChunkWalPath(root, current->store().geometry(), {0, 0})));
    }
    Reply(f.Run("ALTER TABLE t ADD CHUNK COLUMN extra u8 DEFAULT 4"), "+OK\r\n");
    auto state = Read(f, "GET CHUNK 0 0 FROM t");
    assert(DecodeChunkColumns(table->Info().chunk_schema, state.chunk_columns->bytes).size() == 2U);
    Error(f.Run("ALTER TABLE t ADD CHUNK COLUMN needs u8 REQUIRED"), "INVALID_ARGUMENT");
    Reply(f.Run("ALTER TABLE t RENAME CHUNK COLUMN extra TO renamed"), "+OK\r\n");
    Reply(f.Run("ALTER TABLE t DROP CHUNK COLUMN renamed"), "+OK\r\n");
}

void MigrationDomains() {
    Fixture f;
    Reply(f.Run("MIGRATE 'create' CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2 CHUNK COLUMNS (owner u8 DEFAULT 0)"), "+applied\r\n");
    const auto table = f.catalog->Require("t");
    assert(std::filesystem::is_regular_file(f.directory.path() / "tables/t" / kChunkIndexFileName));
    Revision(f.Run("SET CHUNK 0 0 IN t META owner=3"));
    Reply(f.Run("MIGRATE 'add' ALTER TABLE t ADD CHUNK COLUMN extra u8 DEFAULT 4"), "+applied\r\n");
    const auto info = table->Info();
    assert(info.schema.version == 2U && info.chunk_schema.version == 2U && info.schema.columns.size() == 1U);
    Reply(f.Run("MIGRATE 'noop' ALTER TABLE t ADD CHUNK COLUMN IF NOT EXISTS extra u8 DEFAULT 99"), "+applied\r\n");
    Reply(f.Run("MIGRATE 'noop' ALTER TABLE t ADD CHUNK COLUMN IF NOT EXISTS extra u8 DEFAULT 99"), "+skipped\r\n");
    Reply(f.Run("MIGRATE 'absent' ALTER TABLE t DROP CHUNK COLUMN IF EXISTS a"), "+applied\r\n");
    assert(table->Info().schema.version == info.schema.version && table->Info().schema.columns.size() == 1U);
    Revision(f.Run("SET CHUNK 0 0 IN t META owner=0"));
    {
        auto current = table->Acquire();
        current->store().CheckpointForTests(0, 0);
        const auto root = f.directory.path() / "tables/t";
        assert(!std::filesystem::exists(ChunkDataPath(root, current->store().geometry(), {0, 0})));
        assert(!std::filesystem::exists(ChunkWalPath(root, current->store().geometry(), {0, 0})));
    }
    Error(f.Run("MIGRATE 'required' ALTER TABLE t ADD CHUNK COLUMN needs u8 REQUIRED"), "INVALID_ARGUMENT");
    Reply(f.Run("MIGRATE 'rename' ALTER TABLE t RENAME CHUNK COLUMN extra TO renamed"), "+applied\r\n");
    Reply(f.Run("MIGRATE 'drop' ALTER TABLE t DROP CHUNK COLUMN renamed"), "+applied\r\n");
}

void TransactionForms() {
    Fixture f;
    Reply(f.Run("CREATE TABLE t (a u8 DEFAULT 0, b u8 DEFAULT 9) CHUNK 2 x 2 CHUNK COLUMNS (owner u8 DEFAULT 0)"), "+OK\r\n");
    Revision(f.Run("SET BLOCK 0 0 IN t a=11, b=12"));
    Revision(f.Run("SET CHUNK 0 0 IN t META owner=3"));
    const auto table = f.catalog->Require("t");
    const auto geometry = table->geometry();
    auto replacement = Read(f, "GET CHUNK 0 0 FROM t");
    {
        auto lease = table->Acquire();
        lease->store().SetBlockInState(replacement, 0, 0, {{"a", std::uint64_t{42U}}, {"b", std::uint64_t{99U}}});
    }
    replacement.chunk_columns = std::make_shared<const ChunkColumnState>(AssignChunkColumns(
        geometry.chunk_schema(), replacement.chunk_columns.get(), {{"owner", std::uint64_t{8U}}}, false));
    const std::vector<std::size_t> projection{0U};
    const auto partial = Form(geometry, replacement, projection);
    const auto full = Form(geometry, replacement);
    Reply(f.Run("BEGIN"), "+OK\r\n");
    Reply(f.Run("SET CHUNK 0 0 IN t COLUMNS a $1", {partial}), "_\r\n");
    Reply(f.Run("GET BLOCK 0 0 FROM t"), "*2\r\n:42\r\n:12\r\n");
    auto own = Read(f, "GET CHUNK 0 0 FROM t", "t", true);
    assert(std::get<std::uint64_t>(DecodeChunkColumns(geometry.chunk_schema(), own.chunk_columns->bytes)[0]) == 3U);
    Reply(f.Run("SET CHUNK 0 0 IN t $1", {full}), "_\r\n");
    own = Read(f, "GET CHUNK 0 0 FROM t", "t", true);
    assert(std::get<std::uint64_t>(DecodeChunkColumns(geometry.chunk_schema(), own.chunk_columns->bytes)[0]) == 8U);
    Reply(f.Run("GET BLOCK 0 0 FROM t"), "*2\r\n:42\r\n:99\r\n");
    Reply(f.Run("ROLLBACK"), "+OK\r\n");
    Reply(f.Run("GET BLOCK 0 0 FROM t"), "*2\r\n:11\r\n:12\r\n");
    Reply(f.Run("BEGIN"), "+OK\r\n");
    Reply(f.Run("SET CHUNK 0 0 IN t COLUMNS a $1", {partial}), "_\r\n");
    Revision(f.Run("COMMIT"));
    const auto committed = Read(f, "GET CHUNK 0 0 FROM t");
    assert(std::get<std::uint64_t>(DecodeChunkColumns(geometry.chunk_schema(), committed.chunk_columns->bytes)[0]) == 3U);
}

void ScanHistoryLoss() {
    test::ScopedTempDir directory("chunkdb-cql-scan-history");
    CatalogConfig config;
    config.data_dir = directory.path();
    std::uint64_t floor = 0U;
    {
        auto catalog = std::make_shared<TableCatalog>(config);
        CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
        SessionState session;
        assert(engine.Execute(session, "HELLO 3\r\n").starts_with("%"));
        Reply(engine.Execute(session, "CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2\r\n"), "+OK\r\n");
        Reply(engine.Execute(session, "COMPACT TOMBSTONES t BEFORE 0\r\n"), ":0\r\n");
        Revision(engine.Execute(session, "SET BLOCK 0 0 IN t a=1\r\n"));
        Revision(engine.Execute(session, "DELETE BLOCK 0 0 FROM t\r\n"));
        const auto table = catalog->Require("t");
        {
            auto lease = table->Acquire();
            lease->store().CheckpointForTests(0, 0);
        }
        floor = Revision(engine.Execute(session, "SET BLOCK 2 0 IN t a=2\r\n"));
    }
    {
        auto catalog = std::make_shared<TableCatalog>(config);
        CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
        SessionState session;
        assert(engine.Execute(session, "HELLO 3\r\n").starts_with("%"));
        assert(engine.Execute(session, "GET CHUNK 0 0 FROM t\r\n").starts_with("$"));
        assert(engine.Execute(session, "SHOW TABLE STATS t\r\n").find("$10\r\ntombstones\r\n:1\r\n") != std::string::npos);
        Reply(engine.Execute(session, "COMPACT TOMBSTONES t BEFORE " + std::to_string(floor + 1U) + "\r\n"),
            ":" + std::to_string(floor) + "\r\n");
        Reply(engine.Execute(session, "GET CHUNK 0 0 FROM t\r\n"), "_\r\n");
        assert(engine.Execute(session, "SHOW TABLE STATS t\r\n").find("$10\r\ntombstones\r\n:0\r\n") != std::string::npos);
    }
    for (const auto access : {AccessMode::kReadWrite, AccessMode::kReadOnly}) {
        config.access_mode = access;
        auto catalog = std::make_shared<TableCatalog>(config);
        CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
        SessionState session;
        assert(engine.Execute(session, "HELLO 3\r\n").starts_with("%"));
        const auto reply = engine.Execute(session, "SCAN CHUNKS FROM t SINCE 0 WITH DATA\r\n");
        Error(reply, "RESYNC_REQUIRED");
        assert(reply.find("reason=history_lost") != std::string::npos);
        assert(reply.find("floor=" + std::to_string(floor)) != std::string::npos);
        assert((reply.find("current=") != std::string::npos) == (access == AccessMode::kReadWrite));
        if (access == AccessMode::kReadOnly) {
            Error(engine.Execute(session, "COMPACT TOMBSTONES t BEFORE 0\r\n"), "INVALID_ARGUMENT");
            assert(engine.Execute(session, "SHOW TABLE STATS t\r\n").find("$8\r\nrevision\r\n_\r\n") != std::string::npos);
        }
    }
}

void ThreeDimensions() {
    Fixture f;
    Reply(f.Run("CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2 x 2 LARGE 2 x 2 x 2"), "+OK\r\n");
    Revision(f.Run("SET BLOCK -1 -1 -1 IN t a=7"));
    Reply(f.Run("GET BLOCK -1 -1 -1 FROM t WITH SCHEMA"), "*2\r\n:1\r\n*1\r\n:7\r\n");
    Reply(f.Run("GET BLOCK -1 -1 0 FROM t"), "_\r\n");
    const auto state = Read(f, "GET CHUNK -1 -1 -1 FROM t");
    assert(state.presence_bitmap == std::vector<std::uint8_t>{128U});
    assert(f.Run("GET AREA -1 -1 -1 TO -1 -1 -1 FROM t").starts_with("*1\r\n*4\r\n:-1\r\n:-1\r\n:-1\r\n$"));
    Revision(f.Run("SET BLOCK -1 -1 0 IN t a=8"));
    Revision(f.Run("SET BLOCK 0 0 0 IN t a=9"));
    assert(f.Run("GET AREA AROUND -1 -1 -1 RADIUS 1 FROM t").starts_with("*2\r\n"));
    assert(f.Run("SCAN CHUNKS FROM t LIMIT 1 WITH DATA").starts_with(
        "%5\r\n$6\r\nchunks\r\n*1\r\n*5\r\n:-1\r\n:-1\r\n:-1\r\n:"));
    Error(f.Run("SCAN CHUNKS FROM t AFTER -1 -1"), "INVALID_ARGUMENT");
    Reply(f.Run("BEGIN"), "+OK\r\n");
    Reply(f.Run("SET BLOCK -1 -1 -1 IN t a=11"), "_\r\n");
    Read(f, "GET CHUNK -1 -1 -1 FROM t", "t", true);
    Revision(f.Run("COMMIT"));
    Reply(f.Run("GET BLOCK -1 -1 -1 FROM t"), "*1\r\n:11\r\n");
    Error(f.Run("GET BLOCK -1 -1 FROM t"), "INVALID_ARGUMENT");
    Error(f.Run("GET CHUNK -1 -1 FROM t"), "INVALID_ARGUMENT");
    Error(f.Run("GET AREA -1 -1 TO 0 0 FROM t"), "INVALID_ARGUMENT");
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--3d") ThreeDimensions();
    else if (argc == 2 && std::string_view(argv[1]) == "--metadata") MetadataProjectionAndTombstone();
    else if (argc == 2 && std::string_view(argv[1]) == "--required") RequiredMetadataAndPrivateForms();
    else if (argc == 2 && std::string_view(argv[1]) == "--migrations") MigrationDomains();
    else if (argc == 2 && std::string_view(argv[1]) == "--transactions") TransactionForms();
    else if (argc == 2 && std::string_view(argv[1]) == "--history") ScanHistoryLoss();
    else {
        assert(argc == 1);
        MetadataProjectionAndTombstone();
        RequiredMetadataAndPrivateForms();
        MigrationDomains();
        TransactionForms();
        ScanHistoryLoss();
    }
}
