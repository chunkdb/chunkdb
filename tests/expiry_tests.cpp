// DEFAULT COMMIT_TIME and expiry by expire_column (#145), with an injected
// clock: the commit time on every write path, the schema and option rules,
// bounded expiry passes, sweeps on the maintenance pool with their feed
// events and lag, and a crash between passes.

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feed_test_utils.hpp"
#include "table_expiry.hpp"
#include "test_utils.hpp"

#ifndef _WIN32
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

using namespace chunkdb;
using chunkdb::test::ScopedTempDir;

std::atomic<std::uint64_t> g_now{1'000'000U};
std::uint64_t FakeNow() { return g_now.load(); }

bool Contains(const std::string& text, const std::string& part) { return text.find(part) != std::string::npos; }

void Expect(const std::string& reply, const std::string& part) {
    if (!Contains(reply, part)) {
        std::fprintf(stderr, "expected '%s' in '%s'\n", part.c_str(), reply.c_str());
        std::abort();
    }
}

CatalogConfig Config(const std::filesystem::path& dir) {
    auto config = feed_test::Config(dir);
    // Sweeps run only when a test starts them.
    config.expiry_interval = std::chrono::milliseconds(0);
    return config;
}

struct Server {
    explicit Server(const std::filesystem::path& dir)
        : catalog(std::make_shared<TableCatalog>(Config(dir))), engine({.require_auth = false}, catalog) {
        Expect(Run("HELLO 3"), "%");
    }
    std::string Run(const std::string& statement) { return engine.Execute(session, statement); }
    void Ok(const std::string& statement) { Expect(Run(statement), "+OK"); }
    void Write(const std::string& statement) {
        const auto reply = Run(statement);
        if (reply.empty() || reply.front() != ':') {
            std::fprintf(stderr, "%s: %s\n", statement.c_str(), reply.c_str());
            std::abort();
        }
    }
    std::string Get(const std::string& x_y, const std::string& table, const std::string& column) {
        return Run("GET BLOCK " + x_y + " FROM " + table + " COLUMNS " + column);
    }
    std::shared_ptr<TableCatalog> catalog;
    CommandEngine engine;
    SessionState session;
};

std::string Int(std::uint64_t value) { return "*1\r\n:" + std::to_string(value) + "\r\n"; }

void TestCommitTimeRules() {
    Column column{.id = 1, .name = "at", .type = {.kind = ColumnKind::kUnsigned, .size = 64U}};
    column.has_default = true;
    column.default_value.assign(8U, 0U);
    column.default_commit_time = true;
    TableSchema schema{.version = 1, .next_column_id = 2, .columns = {column}, .history = {}, .pending = std::nullopt};
    ValidateTableSchema(schema);
    const auto bytes = EncodeTableSchema(schema);
    assert(DecodeTableSchema(bytes.data(), bytes.size()) == schema);
    // Only a u64 column that is not NULL, with a zero stored default.
    auto narrow = schema;
    narrow.columns[0].type.size = 32U;
    narrow.columns[0].default_value.assign(4U, 0U);
    bool refused = false;
    try { ValidateTableSchema(narrow); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
    auto nullable = schema;
    nullable.columns[0].nullable = true;
    refused = false;
    try { ValidateTableSchema(nullable); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);

    ScopedTempDir dir("chunkdb-expiry-rules");
    Server server(dir.path());
    Expect(server.Run("CREATE TABLE bad (v u8, at u32 DEFAULT COMMIT_TIME)"), "needs a u64 column");
    Expect(server.Run("CREATE TABLE bad (v u8, at u64 NULL DEFAULT COMMIT_TIME)"), "needs a u64 column");
    Expect(server.Run("CREATE TABLE bad (v u8, at u64 DEFAULT COMMIT_TIME DEFAULT 1)"), "DEFAULT is given twice");
    server.Ok("CREATE TABLE t (v u8, at u64 DEFAULT COMMIT_TIME) CHUNK 4 x 4");
    Expect(server.Run("DESCRIBE t"), "$11\r\nCOMMIT_TIME");
    Expect(server.Run("ALTER TABLE t ALTER COLUMN at TYPE u32 USING CLAMP"), "DROP DEFAULT first");
    Expect(server.Run("ALTER TABLE t ADD CHUNK COLUMN c u64 DEFAULT COMMIT_TIME"), "cannot take DEFAULT COMMIT_TIME");
    server.Ok("ALTER TABLE t ALTER COLUMN at DROP DEFAULT");
    assert(!Contains(server.Run("DESCRIBE t"), "COMMIT_TIME"));
    server.Ok("ALTER TABLE t ALTER COLUMN at SET DEFAULT COMMIT_TIME");
    Expect(server.Run("DESCRIBE t"), "COMMIT_TIME");
    server.Ok("ALTER TABLE t ALTER COLUMN at SET DEFAULT 5");
    assert(!Contains(server.Run("DESCRIBE t"), "COMMIT_TIME"));
}

// A commit that changes a block, or creates it, sets its DEFAULT COMMIT_TIME
// columns to its commit time unless it changes them itself, on every path.
void TestCommitTimeWrites() {
    ScopedTempDir dir("chunkdb-expiry-commit-time");
    {
        Server server(dir.path());
        server.Ok("CREATE TABLE t (v u8, note text(8) NULL, at u64 DEFAULT COMMIT_TIME) CHUNK 4 x 4");
        auto feed = server.catalog->Find("t")->SubscribeFeed();
        g_now = 2'000U;
        server.Write("SET BLOCK 0 0 IN t v = 1");
        assert(server.Get("0 0", "t", "at") == Int(2'000U));
        assert(feed_test::Next(*feed)->commit_time_ms == 2'000U);
        g_now = 3'000U;
        server.Write("SET BLOCK 0 0 IN t v = 2");
        assert(server.Get("0 0", "t", "at") == Int(3'000U));
        // A write that changes nothing commits nothing.
        g_now = 4'000U;
        server.Write("SET BLOCK 0 0 IN t v = 2");
        assert(server.Get("0 0", "t", "at") == Int(3'000U));
        // A text value is another value too.
        server.Write("SET BLOCK 0 0 IN t note = 'hi'");
        assert(server.Get("0 0", "t", "at") == Int(4'000U));
        // A value the write gives the column stays; the same value is no change.
        g_now = 5'000U;
        server.Write("SET BLOCK 0 0 IN t at = 7");
        assert(server.Get("0 0", "t", "at") == Int(7U));
        server.Write("SET BLOCK 0 0 IN t v = 3, at = 8");
        assert(server.Get("0 0", "t", "at") == Int(8U));
        g_now = 6'000U;
        server.Write("SET BLOCK 0 0 IN t v = 4, at = 8");
        assert(server.Get("0 0", "t", "at") == Int(6'000U));
        // Computed values and conditions take the commit time they decide with.
        g_now = 7'000U;
        server.Write("SET BLOCK 0 0 IN t v = v + 1");
        assert(server.Get("0 0", "t", "at") == Int(7'000U));
        g_now = 7'500U;
        server.Write("SET BLOCK 0 0 IN t v = 9 IF v = 5");
        assert(server.Get("0 0", "t", "at") == Int(7'500U));
        // A transaction stamps the blocks it changes, and only those.
        g_now = 8'000U;
        server.Ok("BEGIN");
        Expect(server.Run("SET BLOCK 1 0 IN t v = 1"), "_");
        Expect(server.Run("COMMIT"), ":");
        assert(server.Get("1 0", "t", "at") == Int(8'000U));
        assert(server.Get("0 0", "t", "at") == Int(7'500U));
        // Whole-chunk writes, unconditional and conditional.
        {
            auto lease = server.catalog->Find("t")->Acquire();
            auto& store = lease->store();
            g_now = 9'000U;
            auto state = store.ReadChunkState(ChunkCoord{0, 0});
            store.SetBlockInState(state, 2, 0, {{"v", std::uint64_t{1}}});
            (void)store.WriteChunkState(ChunkCoord{0, 0}, std::move(state), std::nullopt);
            g_now = 10'000U;
            (void)store.TransformChunkState(ChunkCoord{0, 0}, std::nullopt, false, [&](ChunkState& next) {
                store.SetBlockInState(next, 3, 0, {{"v", std::uint64_t{1}}});
            });
        }
        assert(server.Get("2 0", "t", "at") == Int(9'000U));
        assert(server.Get("3 0", "t", "at") == Int(10'000U));
        assert(server.Get("1 0", "t", "at") == Int(8'000U));
        // Existing blocks of a column added later hold zero.
        server.Ok("ALTER TABLE t ADD COLUMN seen u64 DEFAULT COMMIT_TIME");
        assert(server.Get("1 0", "t", "seen") == Int(0U));
        g_now = 11'000U;
        server.Write("SET BLOCK 1 0 IN t v = 2");
        assert(server.Get("1 0", "t", "seen") == Int(11'000U) && server.Get("1 0", "t", "at") == Int(11'000U));
    }
    // The stamped values are in the WAL.
    Server server(dir.path());
    assert(server.Get("0 0", "t", "at") == Int(7'500U));
    assert(server.Get("3 0", "t", "at") == Int(10'000U));
}

void TestExpireColumnOption() {
    ScopedTempDir dir("chunkdb-expiry-option");
    {
        Server server(dir.path());
        Expect(server.Run("CREATE TABLE bad (v u8, until u32) WITH expire_column = until"), "must be u64");
        Expect(server.Run("CREATE TABLE bad (v u8) WITH expire_column = until"), "no column until");
        server.Ok("CREATE TABLE e (v u8, until u64 NULL) CHUNK 2 x 2 WITH expire_column = until");
        Expect(server.Run("DESCRIBE e"), "$13\r\nexpire_column\r\n$5\r\nuntil");
        Expect(server.Run("ALTER TABLE e DROP COLUMN until"), "set expire_column = NULL first");
        Expect(server.Run("ALTER TABLE e ALTER COLUMN until TYPE u32 USING CLAMP"), "must be u64");
        Expect(server.Run("ALTER TABLE e ALTER COLUMN until TYPE u32"), "must stay u64");
        server.Ok("ALTER TABLE e RENAME COLUMN until TO deadline");
        Expect(server.Run("DESCRIBE e"), "$13\r\nexpire_column\r\n$8\r\ndeadline");
        server.Ok("ALTER TABLE e SET expire_column = NULL");
        assert(!Contains(server.Run("DESCRIBE e"), "expire_column"));
        server.Ok("ALTER TABLE e SET expire_column = 'deadline'");
        Expect(server.Run("MIGRATE 'drop_expiry' ALTER TABLE e SET expire_column = NULL"), "+applied");
        assert(!Contains(server.Run("DESCRIBE e"), "expire_column"));
        Expect(server.Run("MIGRATE 'add_expiry' ALTER TABLE e SET expire_column = deadline"), "+applied");
    }
    // Option type 13 survives a restart.
    Server server(dir.path());
    Expect(server.Run("DESCRIBE e"), "$13\r\nexpire_column\r\n$8\r\ndeadline");
}

// Writes `count` chunks of a 2 x 2 table, each with an expired block (time
// 500), a block without expiry (NULL), one with time 0 and one expiring later
// (time 5000).
void WriteChunks(Server& server, const std::string& table, int count) {
    for (int i = 0; i < count; ++i) {
        const auto x = std::to_string(2 * i);
        const auto x1 = std::to_string(2 * i + 1);
        server.Write("SET BLOCK " + x + " 0 IN " + table + " v = 1, until = 500");
        server.Write("SET BLOCK " + x1 + " 0 IN " + table + " v = 2");
        server.Write("SET BLOCK " + x + " 1 IN " + table + " v = 3, until = 0");
        server.Write("SET BLOCK " + x1 + " 1 IN " + table + " v = 4, until = 5000");
    }
}

int Present(Server& server, const std::string& table, int count) {
    int present = 0;
    for (int i = 0; i < count; ++i) {
        for (const auto* y : {"0", "1"}) {
            for (const auto x : {2 * i, 2 * i + 1}) {
                if (server.Get(std::to_string(x) + " " + y, table, "v") != "_\r\n") ++present;
            }
        }
    }
    return present;
}

// Passes read at most their chunk bound, resume after their cursor, delete
// only blocks whose time has passed, and decide again under the chunk lock.
void TestBoundedPasses() {
    ScopedTempDir dir("chunkdb-expiry-passes");
    Server server(dir.path());
    server.Ok("CREATE TABLE e (v u8, until u64 NULL) CHUNK 2 x 2 WITH expire_column = until");
    WriteChunks(server, "e", 5);
    g_now = 1'000U;
    auto table = server.catalog->Find("e");
    const auto column = table->Info().options.expire_column;
    auto lease = table->Acquire();
    auto& store = lease->store();
    // A write moves one expired time ahead before the sweep reaches it.
    server.Write("SET BLOCK 8 0 IN e until = 9000");
    std::size_t deleted = 0U;
    std::size_t passes = 0U;
    bool has_cursor = false;
    ChunkCoord cursor{};
    while (true) {
        const auto pass = store.ExpireBlocks(column, has_cursor, cursor, 2U, g_now.load());
        assert(pass.chunks <= 2U && pass.deleted <= pass.chunks);
        deleted += pass.deleted;
        ++passes;
        if (!pass.more) break;
        has_cursor = true;
        cursor = pass.next;
    }
    assert(passes == 3U && deleted == 4U);
    assert(Present(server, "e", 5) == 16);
    assert(server.Get("8 0", "e", "v") == Int(1U));
    // A later sweep with a later clock takes the later times.
    g_now = 9'000U;
    const auto all = store.ExpireBlocks(column, false, {}, 64U, g_now.load());
    assert(!all.more && all.chunks == 5U && all.deleted == 6U);
    assert(Present(server, "e", 5) == 10);
}

// Sweeps on the maintenance pool: many chunks take several turns, each
// deletion is a feed change of user @expire, and the lag metric follows the
// start of the last completed sweep.
void TestSweepsFeedAndLag() {
    ScopedTempDir dir("chunkdb-expiry-sweeps");
    Server server(dir.path());
    server.Ok("CREATE TABLE e (v u8, until u64 NULL) CHUNK 2 x 2 WITH expire_column = until");
    constexpr int kChunks = static_cast<int>(kExpiryPassChunks) + 6;
    WriteChunks(server, "e", kChunks);
    auto table = server.catalog->Find("e");
    auto feed = table->SubscribeFeed();
    g_now = 1'000U;
    ExpiryTestAccess::RunSweep(*server.catalog, "e");
    assert(Present(server, "e", kChunks) == 3 * kChunks);
    for (int i = 0; i < kChunks; ++i) {
        const auto entry = feed_test::Next(*feed);
        assert(entry->user == std::optional<std::string>("@expire"));
        assert(entry->blocks.size() == 1U && !entry->blocks[0].after);
        assert(entry->blocks[0].before == (std::vector<ColumnValue>{std::uint64_t{1}, std::uint64_t{500}}));
        assert(entry->blocks[0].y == 0 && entry->blocks[0].x == 2 * entry->blocks[0].chunk.x);
    }
    auto status = table->Expiry(g_now.load());
    assert(status && status->expired_blocks == static_cast<std::uint64_t>(kChunks) && status->lag_ms == 0U &&
           status->failed_passes == 0U);
    g_now = 6'000U;
    assert(table->Expiry(g_now.load())->lag_ms == 5'000U);
    const auto metrics = server.Run("SHOW METRICS");
    Expect(metrics, "chunkdb_expiry_lag_seconds{table=\"e\"} 5.000000");
    Expect(metrics, "chunkdb_expired_blocks_total{table=\"e\"} " + std::to_string(kChunks));
    Expect(metrics, "chunkdb_expiry_failed_passes_total{table=\"e\"} 0");
    // The next sweep starts now and takes the blocks whose time has passed.
    ExpiryTestAccess::RunSweep(*server.catalog, "e");
    assert(table->Expiry(g_now.load())->lag_ms == 0U);
    assert(Present(server, "e", kChunks) == 2 * kChunks);
    g_now = 6'250U;
    assert(table->Expiry(g_now.load())->lag_ms == 250U);
    // Without an expire_column, no sweep runs and no metric is reported.
    server.Ok("ALTER TABLE e SET expire_column = NULL");
    table = server.catalog->Find("e");
    server.Write("SET BLOCK 0 0 IN e v = 1, until = 10");
    ExpiryTestAccess::RunSweep(*server.catalog, "e");
    assert(server.Get("0 0", "e", "v") == Int(1U) && !table->Expiry(g_now.load()));
    assert(!Contains(server.Run("SHOW METRICS"), "chunkdb_expiry_lag_seconds"));
}

#ifndef _WIN32
// A crash between the passes of a sweep: the committed deletions stay, and
// the sweep after the restart takes the rest.
void TestCrashBetweenPasses() {
    ScopedTempDir dir("chunkdb-expiry-crash");
    constexpr int kChunks = 6;
    const pid_t child = ::fork();
    assert(child >= 0);
    if (child == 0) {
        Server server(dir.path());
        server.Ok("CREATE TABLE e (v u8, until u64 NULL) CHUNK 2 x 2 WITH expire_column = until, durability_mode = 'fsync-wal'");
        WriteChunks(server, "e", kChunks);
        g_now = 1'000U;
        auto table = server.catalog->Find("e");
        const auto column = table->Info().options.expire_column;
        auto lease = table->Acquire();
        const auto pass = lease->store().ExpireBlocks(column, false, {}, 2U, g_now.load());
        std::_Exit(pass.more && pass.deleted == 2U ? 0 : 1);
    }
    int status = 0;
    assert(::waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    Server server(dir.path());
    assert(Present(server, "e", kChunks) == 4 * kChunks - 2);
    assert(server.Get("0 0", "e", "v") == "_\r\n" && server.Get("2 0", "e", "v") == "_\r\n");
    g_now = 1'000U;
    ExpiryTestAccess::RunSweep(*server.catalog, "e");
    assert(Present(server, "e", kChunks) == 3 * kChunks);
    assert(server.catalog->Find("e")->Expiry(g_now.load())->expired_blocks == static_cast<std::uint64_t>(kChunks - 2));
}
#endif

}  // namespace

int main() {
    SetUnixMillisClockForTests(&FakeNow);
#ifndef _WIN32
    // First, while this process has no other threads.
    TestCrashBetweenPasses();
#endif
    TestCommitTimeRules();
    TestCommitTimeWrites();
    TestExpireColumnOption();
    TestBoundedPasses();
    TestSweepsFeedAndLag();
    SetUnixMillisClockForTests(nullptr);
    return 0;
}
