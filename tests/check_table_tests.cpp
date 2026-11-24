// CHECK TABLE (docs/reference/TABLES.md): the chunkdb_verify checks of each
// chunk's files while the table serves, with damaged images and WALs.

#include <algorithm>
#include <cassert>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "login_helpers.hpp"
#include "statement_control.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;

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

bool Contains(const std::string& text, std::string_view part) { return text.find(part) != std::string::npos; }

// Flips one byte of `path`; for a WAL, inside the first frame's header, so a
// later valid frame shows the damage is not a crash tail.
void Damage(const std::filesystem::path& path) {
    std::vector<char> bytes;
    {
        std::ifstream input(path, std::ios::binary);
        bytes.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    }
    assert(!bytes.empty());
    std::size_t at = bytes.size() / 2U;
    if (path.extension() == ".wal") {
        const std::string magic = "FRM2";
        const auto frame = std::search(bytes.begin(), bytes.end(), magic.begin(), magic.end());
        assert(frame != bytes.end());
        at = static_cast<std::size_t>(frame - bytes.begin()) + 6U;
    }
    bytes[at] = static_cast<char>(bytes[at] ^ 0x5A);
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    assert(output);
}

std::string ChunkReply(std::int64_t x, std::int64_t y) {
    return "$5\r\nchunk\r\n*2\r\n:" + std::to_string(x) + "\r\n:" + std::to_string(y) + "\r\n";
}

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-check-table"};
    CatalogConfig config;
    std::shared_ptr<TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    SessionState session;

    explicit Fixture(std::size_t max_loaded_chunks = kDefaultMaxLoadedChunks) {
        config.data_dir = directory.path();
        config.background_maintenance = false;
        config.max_loaded_chunks = max_loaded_chunks;
        Open();
    }
    void Open() {
        engine.reset();
        catalog.reset();
        catalog = std::make_shared<TableCatalog>(config);
        engine = std::make_unique<CommandEngine>(EngineConfig{.require_auth = false}, catalog);
        session = SessionState{};
        assert(engine->Execute(session, "HELLO 3\r\n").starts_with("%"));
    }
    std::string Run(std::string_view statement) { return engine->Execute(session, std::string(statement) + "\r\n"); }
    std::filesystem::path File(std::int64_t x, std::int64_t y, bool wal) {
        const auto lease = catalog->Require("t")->Acquire();
        const auto& store = lease->store();
        const ChunkCoord coord{x, y};
        return wal ? ChunkWalPath(store.data_dir(), store.geometry(), coord)
                   : ChunkDataPath(store.data_dir(), store.geometry(), coord);
    }
    void Checkpoint(std::int64_t x, std::int64_t y) {
        auto lease = catalog->Require("t")->Acquire();
        lease->store().CheckpointForTests(x, y);
    }
};

// Images and WALs of cold chunks damaged after the table opened; the area
// selects which chunks are checked.
void DamagedColdChunks() {
    Fixture f;
    Reply(f.Run("CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2"), "+OK\r\n");
    for (const auto* statement : {"SET BLOCK 0 0 IN t a = 1", "SET BLOCK 2 0 IN t a = 2", "SET BLOCK 4 0 IN t a = 3",
             "SET BLOCK 5 0 IN t a = 4", "SET BLOCK 10 10 IN t a = 5"})
        assert(f.Run(statement).starts_with(":"));
    f.Checkpoint(0, 0);
    f.Checkpoint(1, 0);
    Reply(f.Run("FLUSH WAL"), "+OK\r\n");
    Reply(f.Run("CHECK TABLE t"), "%2\r\n$6\r\nchunks\r\n:4\r\n$7\r\ndamaged\r\n*0\r\n");
    // Reopened, every chunk is cold; damage then appears on disk.
    f.Open();
    Damage(f.File(1, 0, false));
    Damage(f.File(2, 0, true));
    const auto reply = f.Run("CHECK TABLE t");
    assert(reply.starts_with("%2\r\n$6\r\nchunks\r\n:4\r\n$7\r\ndamaged\r\n*2\r\n"));
    assert(Contains(reply, ChunkReply(1, 0)) && Contains(reply, "chunk_image_invalid"));
    assert(Contains(reply, ChunkReply(2, 0)));
    assert(Contains(reply, "C_2_0.wal") && Contains(reply, "C_1_0.chk"));
    assert(!Contains(reply, ChunkReply(0, 0)) && !Contains(reply, ChunkReply(5, 5)));
    const auto area = f.Run("CHECK TABLE t AREA 0 0 TO 1 0");
    assert(area.starts_with("%2\r\n$6\r\nchunks\r\n:2\r\n$7\r\ndamaged\r\n*1\r\n") && Contains(area, ChunkReply(1, 0)));
    Reply(f.Run("CHECK TABLE t CHUNK 5 5"), "%2\r\n$6\r\nchunks\r\n:1\r\n$7\r\ndamaged\r\n*0\r\n");
    Reply(f.Run("CHECK TABLE t AREA AROUND 0 0 RADIUS 0"), "%2\r\n$6\r\nchunks\r\n:1\r\n$7\r\ndamaged\r\n*0\r\n");
    Error(f.Run("CHECK TABLE t CHUNK 0 0 0"), "INVALID_ARGUMENT");
    Error(f.Run("CHECK TABLE missing"), "NO_TABLE");
    // The undamaged chunks still serve.
    Reply(f.Run("GET BLOCK 10 10 FROM t"), "*1\r\n:5\r\n");
    Reply(f.Run("BEGIN"), "+OK\r\n");
    Error(f.Run("CHECK TABLE t"), "INVALID_ARGUMENT");
    Reply(f.Run("ROLLBACK"), "+OK\r\n");
}

// A cached chunk is checked under its lock: its files, not its memory.
void DamagedCachedChunk() {
    Fixture f;
    Reply(f.Run("CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2"), "+OK\r\n");
    assert(f.Run("SET BLOCK 0 0 IN t a = 1").starts_with(":"));
    f.Checkpoint(0, 0);
    assert(f.Run("SET BLOCK 1 0 IN t a = 2").starts_with(":"));
    Damage(f.File(0, 0, false));
    const auto reply = f.Run("CHECK TABLE t");
    assert(reply.starts_with("%2\r\n$6\r\nchunks\r\n:1\r\n$7\r\ndamaged\r\n*1\r\n"));
    assert(Contains(reply, ChunkReply(0, 0)) && Contains(reply, "chunk_image_invalid"));
    Reply(f.Run("GET BLOCK 1 0 FROM t"), "*1\r\n:2\r\n");
    // Writes to the chunk continue once the check ends.
    assert(f.Run("SET BLOCK 1 1 IN t a = 3").starts_with(":"));
}

// A crash-shaped WAL tail is repaired at the next load: it is not damage.
void CrashTailIsNotDamage() {
    Fixture f;
    Reply(f.Run("CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2"), "+OK\r\n");
    assert(f.Run("SET BLOCK 0 0 IN t a = 1").starts_with(":"));
    f.Open();
    {
        std::ofstream wal(f.File(0, 0, true), std::ios::binary | std::ios::app);
        wal.write("FRM2\x01", 5);
    }
    Reply(f.Run("CHECK TABLE t"), "%2\r\n$6\r\nchunks\r\n:1\r\n$7\r\ndamaged\r\n*0\r\n");
}

// ADMIN on the table or MONITOR; a stopped statement ends the check.
void RightsAndCancellation() {
    Fixture f;
    Reply(f.Run("CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2"), "+OK\r\n");
    assert(f.Run("SET BLOCK 0 0 IN t a = 1").starts_with(":"));
    const auto users = test::MakeUsers(f.directory.path(), "admin", "admin-password");
    for (const auto* name : {"reader", "monitor", "owner"})
        users->Create(name, test::PrepareLoginVerifier("password", crypto::RandomBytes(16U), scram::kMinIterations), false);
    users->Grant("reader", "t", Right::kWrite);
    users->Grant("monitor", "*", std::nullopt, static_cast<std::uint8_t>(ServerRight::kMonitor));
    users->Grant("owner", "t", Right::kAdmin);
    CommandEngine engine(EngineConfig{.require_auth = true, .users = users}, f.catalog);
    SessionState reader, monitor, owner;
    assert(test::LoginOnEngine(engine, reader, "reader", std::string("password")).starts_with("%"));
    assert(test::LoginOnEngine(engine, monitor, "monitor", std::string("password")).starts_with("%"));
    assert(test::LoginOnEngine(engine, owner, "owner", std::string("password")).starts_with("%"));
    Error(engine.Execute(reader, "CHECK TABLE t\r\n"), "PERMISSION_DENIED");
    assert(engine.Execute(monitor, "CHECK TABLE t\r\n").starts_with("%2\r\n$6\r\nchunks\r\n:"));
    assert(engine.Execute(owner, "CHECK TABLE t\r\n").starts_with("%2\r\n$6\r\nchunks\r\n:"));
    StatementControl control(StatementControl::Clock::now(), 0U);
    assert(control.Start());
    assert(control.RequestStop(StatementStopReason::kCancelled));
    ScopedStatementControl context(&control);
    Error(engine.Execute(owner, "CHECK TABLE t\r\n"), "CANCELLED");
}

// Evictions that race every read of a chunk outside the cache: the check
// loads the chunk, checks it there and evicts it again; when the load
// refuses damaged files, the damage the last read found is the report.
void RacedReadsUseTheCache() {
    Fixture f(1U);
    Reply(f.Run("CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2"), "+OK\r\n");
    for (const auto* statement : {"SET BLOCK 0 0 IN t a = 1", "SET BLOCK 2 0 IN t a = 2", "SET BLOCK 4 0 IN t a = 3",
             "SET BLOCK 6 0 IN t a = 4"})
        assert(f.Run(statement).starts_with(":"));
    f.Checkpoint(0, 0);
    Reply(f.Run("FLUSH WAL"), "+OK\r\n");
    f.Open();
    Damage(f.File(0, 0, false));
    for (const std::int64_t x : {1, 0}) {
        const auto table = f.catalog->Require("t");
        auto lease = table->Acquire();
        auto& store = lease->store();
        const ChunkCoord coord{x, 0};
        using Point = CachePausePoint;
        store.ArmCachePauseForTests(Point::kCheckTableBeforeRead, coord);
        std::string reply;
        std::thread checker([&] { reply = f.Run("CHECK TABLE t CHUNK " + std::to_string(x) + " 0"); });
        SessionState other;
        assert(f.engine->Execute(other, "HELLO 3\r\n").starts_with("%"));
        for (int attempt = 0; attempt < 3; ++attempt) {
            assert(store.WaitForCachePauseForTests(Point::kCheckTableBeforeRead));
            // With room for one chunk, each load evicts the other.
            Reply(f.engine->Execute(other, "GET BLOCK 4 0 FROM t\r\n"), "*1\r\n:3\r\n");
            Reply(f.engine->Execute(other, "GET BLOCK 6 0 FROM t\r\n"), "*1\r\n:4\r\n");
            store.ArmCachePauseForTests(Point::kCheckTableAfterRead, coord);
            store.ResumeCachePauseForTests(Point::kCheckTableBeforeRead);
            assert(store.WaitForCachePauseForTests(Point::kCheckTableAfterRead));
            if (attempt != 2) store.ArmCachePauseForTests(Point::kCheckTableBeforeRead, coord);
            store.ResumeCachePauseForTests(Point::kCheckTableAfterRead);
        }
        checker.join();
        if (x == 1) {
            Reply(reply, "%2\r\n$6\r\nchunks\r\n:1\r\n$7\r\ndamaged\r\n*0\r\n");
            assert(!store.IsChunkLoadedForTests(1, 0));
        } else {
            assert(reply.starts_with("%2\r\n$6\r\nchunks\r\n:1\r\n$7\r\ndamaged\r\n*1\r\n"));
            assert(Contains(reply, "chunk_image_invalid") && !store.IsChunkLoadedForTests(0, 0));
        }
    }
}

// A read-only server leaves file checks to chunkdb_verify.
void ReadOnlyRefuses() {
    Fixture f;
    Reply(f.Run("CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2"), "+OK\r\n");
    assert(f.Run("SET BLOCK 0 0 IN t a = 1").starts_with(":"));
    Reply(f.Run("FLUSH WAL"), "+OK\r\n");
    f.engine.reset();
    f.catalog.reset();
    f.config.access_mode = AccessMode::kReadOnly;
    f.Open();
    Error(f.Run("CHECK TABLE t"), "INVALID_ARGUMENT");
}

}  // namespace

int main() {
    DamagedColdChunks();
    DamagedCachedChunk();
    CrashTailIsNotDamage();
    RightsAndCancellation();
    RacedReadsUseTheCache();
    ReadOnlyRefuses();
    std::cout << "check_table tests passed\n";
    return 0;
}
