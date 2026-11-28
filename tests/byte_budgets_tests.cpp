// Server-wide byte budgets (--cache-max-bytes, --feed-total-bytes,
// --txn-history-total-bytes), the free-disk floor, the max_disk_bytes table
// option, and the outcome of ENOSPC in each durability mode.

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#endif

#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "change_feed.hpp"
#include "chunk_store_internal.hpp"
#include "maintenance_pool.hpp"
#include "store_manifest.hpp"
#include "txn_test_utils.hpp"

namespace {
using namespace chunkdb;
using namespace std::chrono_literals;
using Parameters = std::vector<std::optional<std::string>>;

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-byte-budgets"};
    CatalogConfig config;
    std::filesystem::path backup_dir{};
    std::optional<std::uint64_t> available;
    std::shared_ptr<TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    SessionState session;

    explicit Fixture(DurabilityMode mode = DurabilityMode::kRelaxed) {
        config.data_dir = directory.path();
        config.background_maintenance = false;
        config.feed_linger = 0ms;
        config.disk_check_interval = 1h;
        config.default_options.durability_mode = mode;
        config.default_options.checkpoint_update_interval = 1000000U;
        config.default_options.checkpoint_wal_bytes = 1000000U;
        config.default_options.wal_group_commit_updates = 1U;
    }
    void Open() {
        catalog = std::make_shared<TableCatalog>(config);
        catalog->SetDiskSpaceProbeForTests([this](const std::filesystem::path&) { return available; });
        engine = std::make_unique<CommandEngine>(EngineConfig{.require_auth = false, .backup_dir = backup_dir}, catalog);
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
    std::string Run(SessionState& other, std::string_view statement) {
        return engine->Execute(other, std::string(statement) + "\r\n", {});
    }
    SessionState NewSession() {
        SessionState other;
        assert(Run(other, "HELLO 3").starts_with("%"));
        return other;
    }
};

void Expect(const std::string& actual, std::string_view prefix) {
    if (!actual.starts_with(prefix)) {
        std::cerr << "expected a reply starting with " << prefix << ", received " << actual;
        assert(false);
    }
}

// The cache byte limit evicts across tables, coldest first, as the chunk
// count limit does, and evicted chunks read back from their files.
void CacheBytesBoundAllTables() {
    Fixture f;
    f.Open();
    assert(f.Run("CREATE TABLE a (n u32) CHUNK 4 x 4 LARGE 2 x 2") == "+OK\r\n");
    assert(f.Run("CREATE TABLE b (n u32) CHUNK 4 x 4 LARGE 2 x 2") == "+OK\r\n");
    // One chunk's charge, measured, sets a limit of about eight chunks.
    Expect(f.Run("SET BLOCK 0 0 IN a n=1"), ":");
    const auto one = f.catalog->resources()->CacheBytes();
    assert(one > 0U);
    f.config.byte_limits.cache_max_bytes = one * 8U;
    f.Reopen();
    const auto& resources = *f.catalog->resources();
    assert(resources.max_cache_bytes() == one * 8U);
    for (int i = 0; i < 40; ++i) {
        Expect(f.Run("SET BLOCK " + std::to_string(i * 4) + " 0 IN a n=" + std::to_string(i + 1)), ":");
        assert(resources.CacheBytes() <= resources.max_cache_bytes() + one);
    }
    const auto loaded_a = [&] { return f.catalog->Require("a")->Acquire()->store().ApproxLoadedChunkCount(); };
    const auto a_before = loaded_a();
    assert(a_before <= 9U);
    for (int i = 0; i < 40; ++i) {
        Expect(f.Run("SET BLOCK " + std::to_string(i * 4) + " 4 IN b n=" + std::to_string(i + 1)), ":");
        assert(resources.CacheBytes() <= resources.max_cache_bytes() + one);
    }
    // Table b's loads took the memory of table a's colder chunks.
    assert(loaded_a() < a_before);
    for (int i = 0; i < 40; ++i)
        assert(f.Run("GET BLOCK " + std::to_string(i * 4) + " 0 FROM a") == "*1\r\n:" + std::to_string(i + 1) + "\r\n");
    // Without the byte limit the same chunks stay cached.
    f.config.byte_limits.cache_max_bytes = 0U;
    f.Reopen();
    for (int i = 0; i < 40; ++i) Expect(f.Run("GET BLOCK " + std::to_string(i * 4) + " 0 FROM a"), "*1");
    assert(loaded_a() == 40U);
}

// COMPACT TOMBSTONES checkpoints chunks that are not cached through temporary
// copies outside the cache; they never count toward the cache bytes.
void OffCacheCheckpointsLeaveCacheBytes() {
    Fixture f;
    f.Open();
    assert(f.Run("CREATE TABLE t (n u32) CHUNK 4 x 4") == "+OK\r\n");
    for (int i = 0; i < 30; ++i) Expect(f.Run("SET BLOCK " + std::to_string(i * 4) + " 0 IN t n=1"), ":");
    for (int i = 0; i < 10; ++i) Expect(f.Run("DELETE BLOCK " + std::to_string(i * 4) + " 0 FROM t"), ":");
    f.Reopen();
    const auto before = f.catalog->resources()->CacheBytes();
    Expect(f.Run("COMPACT TOMBSTONES t BEFORE 30"), ":");
    assert(f.catalog->resources()->CacheBytes() == before);
    assert(f.Run("GET BLOCK 40 0 FROM t") == "*1\r\n:1\r\n");
}

struct CheckpointEvictionTurns {
    std::promise<void> parked;
    std::shared_future<void> release;
    std::promise<void> completed;
    unsigned turns = 0U;
};

CheckpointEvictionTurns* checkpoint_eviction_turns = nullptr;

void ObserveCheckpointEvictionTurn() {
    auto& turns = *checkpoint_eviction_turns;
    if (++turns.turns == 1U) {
        turns.parked.set_value();
        turns.release.wait();
    } else if (turns.turns == 2U) {
        turns.completed.set_value();
    }
}

void CheckpointGrowthQueuesEviction(bool fence_before_eviction) {
    std::promise<void> release;
    CheckpointEvictionTurns turns{.parked = {}, .release = release.get_future().share(), .completed = {}};
    auto worker_parked = turns.parked.get_future();
    auto eviction_finished = turns.completed.get_future();
    Fixture f;
    f.config.maintenance_workers = 1U;
    f.config.byte_limits.cache_max_bytes = 8192U;
    f.config.default_options.checkpoint_update_interval = 2U;
    f.Open();
    assert(f.Run("CREATE TABLE t (n u8, note text(32768)) CHUNK 4 x 4") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN t n=1, note='small'"), ":");
    const auto& resources = *f.catalog->resources();
    assert(resources.CacheBytes() < resources.max_cache_bytes());

    // Park the only worker after FLUSH WAL, so the inline checkpoint can
    // finish and release the chunk before the queued pass picks its victims.
    checkpoint_eviction_turns = &turns;
    MaintenancePool::SetTurnEndHookForTests(&ObserveCheckpointEvictionTurn);
    assert(f.Run("FLUSH WAL") == "+OK\r\n");
    assert(worker_parked.wait_for(5s) == std::future_status::ready);
    Expect(f.Run("SET BLOCK 0 0 IN t note='" + std::string(16384U, 'x') + "'"), ":");
    assert(resources.CacheBytes() > resources.max_cache_bytes());
    const auto table = f.catalog->Require("t");
    {
        const auto lease = table->Acquire();
        assert(lease->store().RuntimeStats().checkpoints == 1U);
        assert(lease->store().LoadedChunkBytes() == resources.CacheBytes());
    }
    if (fence_before_eviction) {
        // Run this barrier directly while the pool worker is still parked.
        txn_test::ScopedEnv failed("CHUNKDB_FAILPOINT_BARRIER_FSYNC_EIO_ONCE", "1");
        bool unknown = false;
        try { table->Acquire()->store().WalBarrier(); }
        catch (const WriteOutcomeUnknownError&) { unknown = true; }
        assert(unknown && f.catalog->Fence("t"));
    }
    release.set_value();
    assert(eviction_finished.wait_for(5s) == std::future_status::ready);
    MaintenancePool::SetTurnEndHookForTests(nullptr);
    const auto stats = table->Acquire()->store().RuntimeStats();
    if (fence_before_eviction) {
        assert(resources.CacheBytes() > resources.max_cache_bytes() && stats.evictions == 0U);
    } else {
        assert(resources.CacheBytes() <= resources.max_cache_bytes() && stats.evictions == 1U);
    }
}

// A failed sync in FLUSH WAL leaves the table fail-closed: a retried sync
// could succeed after the system dropped the data it was to write.
void BarrierSyncFailureFailsClosed() {
    Fixture f;
    f.Open();
    assert(f.Run("CREATE TABLE t (n u32) CHUNK 4 x 4") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN t n=1"), ":");
    {
        txn_test::ScopedEnv failed("CHUNKDB_FAILPOINT_BARRIER_FSYNC_EIO_ONCE", "1");
        Expect(f.Run("FLUSH WAL"), "-ERR OUTCOME_UNKNOWN stage=barrier_sync ");
    }
    Expect(f.Run("FLUSH WAL"), "-ERR ");
    Expect(f.Run("SET BLOCK 4 0 IN t n=2"), "-ERR ");
    assert(f.Run("RECOVER TABLE t") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 4 0 IN t n=2"), ":");
    assert(f.Run("FLUSH WAL") == "+OK\r\n");
    assert(f.Run("GET BLOCK 4 0 FROM t") == "*1\r\n:2\r\n");
}

void QueuedBarrierObservesSyncFailure() {
    Fixture f;
    f.Open();
    assert(f.Run("CREATE TABLE t (n u32) CHUNK 4 x 4") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN t n=1"), ":");
    auto lease = f.catalog->Require("t")->Acquire();
    auto& store = lease->store();
    store.ArmBarrierBeforeSyncPauseForTests();
    txn_test::ScopedEnv failed("CHUNKDB_FAILPOINT_BARRIER_FSYNC_EIO_ONCE", "1");
    auto first = std::async(std::launch::async, [&] {
        try {
            store.WalBarrier();
        } catch (const WriteOutcomeUnknownError& error) {
            return error.stage() == "barrier_sync";
        }
        return false;
    });
    assert(store.WaitForBarrierBeforeSyncForTests());
    std::promise<void> arrived;
    auto before_lock = arrived.get_future();
    const std::function<void()> hook = [&] { arrived.set_value(); };
    SetBarrierBeforeLockHookForTests(&hook);
    auto second = std::async(std::launch::async, [&] {
        try {
            store.WalBarrier();
        } catch (const std::runtime_error& error) {
            return std::string_view(error.what()).starts_with("store is fail-closed");
        }
        return false;
    });
    assert(before_lock.wait_for(5s) == std::future_status::ready);
    store.ResumeBarrierBeforeSyncForTests();
    assert(first.get());
    assert(second.get());
    SetBarrierBeforeLockHookForTests(nullptr);
}

void BarrierBatchSyncFailureFailsClosed() {
    Fixture f;
    f.config.default_options.wal_group_commit_updates = 8U;
    f.Open();
    assert(f.Run("CREATE TABLE t (n u32) CHUNK 4 x 4") == "+OK\r\n");
    assert(f.Run("BEGIN") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN t n=1"), "_");
    Expect(f.Run("COMMIT"), ":");
    // COMMIT left a durable boundary. The next relaxed write stays in a
    // batch, which this barrier syncs in its first phase.
    Expect(f.Run("SET BLOCK 1 0 IN t n=2"), ":");
    {
        txn_test::ScopedEnv failed("CHUNKDB_FAILPOINT_WAL_BATCH_FSYNC_EIO_ONCE", "1");
        Expect(f.Run("FLUSH WAL"), "-ERR OUTCOME_UNKNOWN stage=barrier_sync ");
    }
    Expect(f.Run("FLUSH WAL"), "-ERR INTERNAL ");
    Expect(f.Run("SET BLOCK 2 0 IN t n=3"), "-ERR INTERNAL ");
}

void MultiTableBarrierFailures(std::string_view statement, bool sync_first) {
    Fixture f;
    f.config.maintenance_workers = 1U;
    f.Open();
    const std::string definite = sync_first ? "beta" : "alpha";
    const std::string unknown = sync_first ? "alpha" : "beta";
    assert(f.Run("CREATE TABLE " + definite + " (n u32) CHUNK 4 x 4 WITH wal_group_commit_updates = 2") == "+OK\r\n");
    assert(f.Run("CREATE TABLE " + unknown + " (n u32) CHUNK 4 x 4 WITH wal_group_commit_updates = 1") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN " + definite + " n=1"), ":");
    Expect(f.Run("SET BLOCK 0 0 IN " + unknown + " n=1"), ":");
    const bool runs_all = statement == "FLUSH WAL";
    const bool expect_unknown = runs_all || sync_first;
    {
        // The definite table still has a batch. Its append fails before it
        // can consume the sync failpoint, which the other table reaches.
        txn_test::ScopedEnv full("CHUNKDB_FAILPOINT_WAL_APPEND_ENOSPC_ONCE", "1");
        txn_test::ScopedEnv sync("CHUNKDB_FAILPOINT_BARRIER_FSYNC_EIO_ONCE", "1");
        if (expect_unknown) {
            Expect(f.Run(statement), "-ERR OUTCOME_UNKNOWN stage=barrier_sync table=" + unknown + " ");
        } else {
            Expect(f.Run(statement), "-ERR DISK_FULL reason=disk_full ");
        }
        const auto* append_left = std::getenv("CHUNKDB_FAILPOINT_WAL_APPEND_ENOSPC_ONCE");
        const bool append_consumed = append_left == nullptr || append_left[0] == '\0';
        assert(append_consumed == (runs_all || !sync_first));
    }
    assert(f.catalog->Fence(unknown).has_value() == expect_unknown && !f.catalog->Fence(definite));
    if (expect_unknown) {
        Expect(f.Run("SET BLOCK 1 0 IN " + unknown + " n=2"), "-ERR INTERNAL ");
        assert(f.Run("RECOVER TABLE " + unknown) == "+OK\r\n");
    }
    Expect(f.Run("SET BLOCK 1 0 IN " + definite + " n=2"), ":");
    assert(f.Run("FLUSH WAL") == "+OK\r\n");
}

void EvictionOpenStreamRepairFailureIsInternal() {
    Fixture f;
    f.config.max_loaded_chunks = 1U;
    f.config.default_options.wal_group_commit_updates = 2U;
    f.Open();
    assert(f.Run("CREATE TABLE t (n u32) CHUNK 4 x 4") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN t n=1"), ":");
    Expect(f.Run("SET BLOCK 1 0 IN t n=2"), ":");
    assert(f.catalog->resources()->OpenWalStreamCount() == 1U);
    Expect(f.Run("SET BLOCK 2 0 IN t n=3"), ":");
    {
        txn_test::ScopedEnv full("CHUNKDB_FAILPOINT_WAL_APPEND_ENOSPC_ONCE", "1");
        txn_test::ScopedEnv repair("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1");
        Expect(f.Run("SET BLOCK 4 0 IN t n=4"), "-ERR INTERNAL ");
    }
    assert(f.Run("GET BLOCK 4 0 FROM t") == "_\r\n");
    Expect(f.Run("SET BLOCK 4 0 IN t n=4"), "-ERR INTERNAL ");
}

// Past the server-wide history total, a write that must keep a state ends
// the oldest transactions of its own table; another table's transactions
// keep their snapshots.
void TxnHistoryTotal() {
    Fixture f;
    f.config.byte_limits.txn_history_total_bytes = 1U;
    f.Open();
    assert(f.Run("CREATE TABLE a (n u32) CHUNK 4 x 4") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN a n=1"), ":");
    assert(f.catalog->resources()->txn_history_budget()->limit() == 1U);
    {
        // Nothing fits: the reader's snapshot ends, the write succeeds.
        auto r = f.NewSession();
        assert(f.Run(r, "BEGIN") == "+OK\r\n");
        Expect(f.Run(r, "GET BLOCK 0 0 FROM a"), "*1\r\n:1");
        Expect(f.Run("SET BLOCK 0 0 IN a n=2"), ":");
        Expect(f.Run(r, "GET BLOCK 0 0 FROM a"), "-ERR CONFLICT reason=history_limit");
        assert(f.Run(r, "ROLLBACK") == "+OK\r\n");
        assert(f.catalog->resources()->txn_history_budget()->used() == 0U);
    }
    // Room for a few states: table a holds them, table b's write must keep
    // one more and ends b's reader, while a's reader keeps its snapshot.
    f.config.byte_limits.txn_history_total_bytes = 4096U;
    f.Reopen();
    assert(f.Run("CREATE TABLE b (n u32) CHUNK 4 x 4") == "+OK\r\n");
    const auto& budget = *f.catalog->resources()->txn_history_budget();
    auto a_reader = f.NewSession();
    assert(f.Run(a_reader, "BEGIN") == "+OK\r\n");
    Expect(f.Run(a_reader, "GET BLOCK 0 0 FROM a"), "*1\r\n:2");
    std::size_t per_state = 0U;
    int chunk = 0;
    for (;; ++chunk) {
        const auto before = budget.used();
        if (per_state != 0U && before + per_state > budget.limit()) break;
        Expect(f.Run("SET BLOCK " + std::to_string(chunk * 4) + " 0 IN a n=9"), ":");
        if (per_state == 0U) per_state = budget.used() - before;
        assert(per_state > 0U && budget.used() == before + per_state);
    }
    assert(chunk >= 2);
    auto b_reader = f.NewSession();
    assert(f.Run(b_reader, "BEGIN") == "+OK\r\n");
    assert(f.Run(b_reader, "GET BLOCK 0 0 FROM b") == "_\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN b n=5"), ":");
    assert(budget.used() <= budget.limit());
    Expect(f.Run(b_reader, "GET BLOCK 0 0 FROM b"), "-ERR CONFLICT reason=history_limit");
    // a's reader still reads its snapshot.
    Expect(f.Run(a_reader, "GET BLOCK 0 0 FROM a"), "*1\r\n:2");
    assert(f.Run(a_reader, "GET BLOCK 4 0 FROM a") == "_\r\n");
    assert(f.Run(a_reader, "ROLLBACK") == "+OK\r\n");
    assert(f.Run(b_reader, "ROLLBACK") == "+OK\r\n");
    assert(budget.used() == 0U);
}

std::shared_ptr<const FeedEntry> NextEntry(FeedSubscription& subscription) {
    auto entry = subscription.Next(10s);
    assert(entry != nullptr);
    return entry;
}

// The live feeds of all tables share --feed-total-bytes; a capture that does
// not fit is lost for that table's readers, who resync.
void FeedTotal() {
    {
        Fixture f;
        f.config.byte_limits.feed_total_bytes = 1U;
        f.Open();
        assert(f.Run("CREATE TABLE a (n u32) CHUNK 4 x 4") == "+OK\r\n");
        const auto table = f.catalog->Require("a");
        auto watch = table->SubscribeFeed();
        Expect(f.Run("SET BLOCK 0 0 IN a n=1"), ":");
        assert(NextEntry(*watch)->kind == FeedEntry::Kind::kResync);
        watch.reset();
        table->StopFeed();
        assert(f.catalog->resources()->feed_budget()->used() == 0U);
    }
    Fixture f;
    f.config.byte_limits.feed_total_bytes = 64U * 1024U;
    f.Open();
    assert(f.Run("CREATE TABLE a (n u32, note text(256)) CHUNK 4 x 4") == "+OK\r\n");
    assert(f.Run("CREATE TABLE b (n u32, note text(256)) CHUNK 4 x 4") == "+OK\r\n");
    const auto& budget = *f.catalog->resources()->feed_budget();
    const auto a = f.catalog->Require("a");
    const auto b = f.catalog->Require("b");
    {
        // Readers that never read keep their tables' entries.
        auto a_watch = a->SubscribeFeed();
        auto b_watch = b->SubscribeFeed();
        const std::string note(200, 'x');
        for (int i = 0; i < 400; ++i) {
            Expect(f.Run("SET BLOCK " + std::to_string(i % 64) + " 0 IN " + (i % 2 == 0 ? "a" : "b") +
                " n=" + std::to_string(i) + ", note='" + note + "'"), ":");
            assert(budget.used() <= budget.limit());
        }
        assert(budget.used() > budget.limit() / 2U);
    }
    a->StopFeed();
    b->StopFeed();
    assert(budget.used() == 0U);
}

// --min-free-disk-bytes: data writes, COMMIT with writes, CREATE TABLE,
// RESTORE TABLE and MIGRATE are refused; reads, DDL that frees or limits
// space, and FLUSH WAL run.
void DiskFloor() {
    test::ScopedTempDir backups{"chunkdb-byte-budgets-backups"};
    Fixture f;
    f.backup_dir = backups.path();
    f.config.min_free_disk_bytes = 1000U;
    f.available = 5000U;
    f.Open();
    f.catalog->SampleDisk();
    assert(f.Run("CREATE TABLE t (n u32) CHUNK 4 x 4 CHUNK COLUMNS (owner u8 DEFAULT 0)") == "+OK\r\n");
    assert(f.Run("CREATE TABLE spare (n u32) CHUNK 4 x 4") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN t n=1"), ":");
    Expect(f.Run("BACKUP TO 'snapshot'"), "%");
    auto txn = f.NewSession();
    assert(f.Run(txn, "BEGIN") == "+OK\r\n");
    Expect(f.Run(txn, "SET BLOCK 4 4 IN t n=7"), "_");

    f.available = 999U;
    f.catalog->SampleDisk();
    const std::string refused = "-ERR DISK_FULL reason=min_free_disk available=999 limit=1000 ";
    Expect(f.Run("SET BLOCK 0 0 IN t n=2"), refused);
    Expect(f.Run("DELETE BLOCK 0 0 FROM t"), refused);
    Expect(f.Run("SET CHUNK 0 0 IN t META owner=7"), refused);
    Expect(f.Run("SET BLOCK 0 0 IN t n = n + 1 IF n = 1"), refused);
    Expect(f.Run("CREATE TABLE other (n u8)"), refused);
    Expect(f.Run("MIGRATE 'm1' CREATE TABLE other (n u8)"), refused);
    Expect(f.Run("RESTORE TABLE t FROM 'snapshot' AS restored"), refused);
    assert(!f.catalog->Find("restored"));
    // A transaction stages privately; its COMMIT is refused and ends it.
    Expect(f.Run(txn, "SET BLOCK 8 8 IN t n=8"), "_");
    Expect(f.Run(txn, "COMMIT"), refused);
    Expect(f.Run(txn, "GET BLOCK 4 4 FROM t"), "_");
    assert(f.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:1\r\n");
    assert(f.Run("FLUSH WAL") == "+OK\r\n");
    assert(f.Run("ALTER TABLE t SET checkpoint_updates = 64") == "+OK\r\n");
    assert(f.Run("DROP TABLE spare") == "+OK\r\n");
    // A failed query keeps the last result.
    f.available.reset();
    f.catalog->SampleDisk();
    Expect(f.Run("SET BLOCK 0 0 IN t n=2"), refused);

    f.available = 1000U;
    f.catalog->SampleDisk();
    Expect(f.Run("SET BLOCK 0 0 IN t n=2"), ":");
    assert(f.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:2\r\n");
    Expect(f.Run("RESTORE TABLE t FROM 'snapshot' AS restored"), "%");
    assert(f.Run("GET BLOCK 0 0 FROM restored") == "*1\r\n:1\r\n");

    // A server that starts below the floor refuses from its first write.
    f.available = 10U;
    f.engine.reset();
    f.catalog.reset();
    {
        // Probe before the catalog's first sample: through the real query,
        // a floor above any file system's free space.
        f.config.min_free_disk_bytes = std::uint64_t{1} << 60U;
        f.catalog = std::make_shared<TableCatalog>(f.config);
        f.engine = std::make_unique<CommandEngine>(EngineConfig{.require_auth = false}, f.catalog);
        f.session = {};
        assert(f.Run("HELLO 3").starts_with("%"));
        Expect(f.Run("SET BLOCK 0 0 IN t n=3"), "-ERR DISK_FULL reason=min_free_disk ");
    }
}

// max_disk_bytes: persisted, shown by DESCRIBE when set, refuses that
// table's data writes only, and zero removes it.
void TableMaxDiskBytes() {
    Fixture f;
    f.config.min_free_disk_bytes = 1U;
    f.available = 5000U;
    f.Open();
    assert(f.Run("CREATE TABLE t (n u32) CHUNK 4 x 4 CHUNK COLUMNS (owner u8 DEFAULT 0) WITH max_disk_bytes = 1") == "+OK\r\n");
    assert(f.Run("CREATE TABLE u (n u32) CHUNK 4 x 4") == "+OK\r\n");
    assert(f.Run("DESCRIBE t").find("$14\r\nmax_disk_bytes\r\n:1\r\n") != std::string::npos);
    assert(f.Run("DESCRIBE u").find("max_disk_bytes") == std::string::npos);
    Expect(f.Run("CREATE TABLE bad (n u8) WITH max_disk_bytes = -1"), "-ERR");
    // Writes are accepted until a sample finds the table at its limit.
    Expect(f.Run("SET BLOCK 0 0 IN t n=1"), ":");
    f.catalog->SampleDisk();
    Expect(f.Run("SET BLOCK 0 0 IN t n=2"), "-ERR DISK_FULL reason=max_disk_bytes table=t used=");
    Expect(f.Run("SET BLOCK 0 0 IN u n=2"), ":");
    Expect(f.Run("DELETE BLOCK 0 0 FROM t"), "-ERR DISK_FULL reason=max_disk_bytes table=t ");
    Expect(f.Run("SET CHUNK 0 0 IN t META owner=7"), "-ERR DISK_FULL reason=max_disk_bytes table=t ");
    assert(f.Run("BEGIN") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN t n=3"), "_");
    Expect(f.Run("COMMIT"), "-ERR DISK_FULL reason=max_disk_bytes table=t ");
    assert(f.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:1\r\n");
    // The option survives a restart.
    f.Reopen();
    assert(f.Run("DESCRIBE t").find("$14\r\nmax_disk_bytes\r\n:1\r\n") != std::string::npos);
    Expect(f.Run("SET BLOCK 0 0 IN t n=2"), "-ERR DISK_FULL reason=max_disk_bytes table=t ");
    // The ALTER itself wakes the sampler, whose periodic interval is one hour.
    const auto alter_and_wait = [&](const char* value) {
        std::promise<void> probed;
        auto sample = probed.get_future();
        f.catalog->SetDiskSpaceProbeForTests([&](const std::filesystem::path&) -> std::optional<std::uint64_t> {
            probed.set_value();
            return 5000U;
        });
        assert(f.Run(std::string("ALTER TABLE t SET max_disk_bytes = ") + value) == "+OK\r\n");
        assert(sample.wait_for(5s) == std::future_status::ready);
        // Replacing the probe takes the sample mutex after the table scan.
        f.catalog->SetDiskSpaceProbeForTests({});
    };
    alter_and_wait("1073741824");
    Expect(f.Run("SET BLOCK 0 0 IN t n=3"), ":");
    assert(f.Run("DESCRIBE t").find("max_disk_bytes\r\n:1073741824\r\n") != std::string::npos);
    alter_and_wait("1");
    Expect(f.Run("SET BLOCK 0 0 IN t n=4"), "-ERR DISK_FULL reason=max_disk_bytes ");
    alter_and_wait("0");
    assert(f.Run("DESCRIBE t").find("max_disk_bytes") == std::string::npos);
    Expect(f.Run("SET BLOCK 0 0 IN t n=4"), ":");

    TableOptions options;
    options.max_disk_bytes = 4096U;
    assert(DecodeTableOptions(EncodeTableOptions(options)).max_disk_bytes == 4096U);
    assert(!DecodeTableOptions(EncodeTableOptions(TableOptions{})).max_disk_bytes);
}

void IncompleteDiskScanKeepsRefusal() {
#ifndef _WIN32
    if (::geteuid() == 0) return;  // Root can list mode-000 directories.
    namespace fs = std::filesystem;
    Fixture f;
    f.Open();
    assert(f.Run("CREATE TABLE t (n u32) CHUNK 4 x 4 WITH max_disk_bytes = 1048576") == "+OK\r\n");
    const auto table_dir = f.catalog->Require("t")->Acquire()->store().data_dir();
    const auto sentinel = table_dir / "sample-size";
    const auto nested = table_dir / "unreadable";
    std::ofstream(sentinel).close();
    fs::resize_file(sentinel, 1048576U);
    assert(fs::create_directory(nested));
    f.catalog->SampleDisk();
    const auto write = "SET BLOCK 0 0 IN t n=1";
    const auto refused = "-ERR DISK_FULL reason=max_disk_bytes table=t ";
    Expect(f.Run(write), refused);
    assert(fs::remove(sentinel));
    fs::permissions(nested, fs::perms::none);
    f.catalog->SampleDisk();
    fs::permissions(nested, fs::perms::owner_all);
    Expect(f.Run(write), refused);
    f.catalog->SampleDisk();
    Expect(f.Run(write), ":");
#endif
}

// ENOSPC appending a WAL batch: the write that hit it answers DISK_FULL and
// is not applied, the WAL keeps no part of it, and earlier acknowledged
// writes stay (in relaxed mode in the batch, which a later flush retries).
void WalAppendNoSpace(DurabilityMode mode) {
    Fixture f(mode);
    f.config.default_options.wal_group_commit_updates = 2U;
    f.Open();
    assert(f.Run("CREATE TABLE t (n u32) CHUNK 4 x 4") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN t n=1"), ":");
    {
        txn_test::ScopedEnv full("CHUNKDB_FAILPOINT_WAL_APPEND_ENOSPC_ONCE", "1");
        // Relaxed: the second write fills the batch and flushes both.
        Expect(f.Run("SET BLOCK 1 0 IN t n=2"), "-ERR DISK_FULL reason=disk_full ");
    }
    assert(f.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:1\r\n");
    assert(f.Run("GET BLOCK 1 0 FROM t") == "_\r\n");
    Expect(f.Run("SET BLOCK 2 0 IN t n=3"), ":");
    assert(f.Run("FLUSH WAL") == "+OK\r\n");
    f.Reopen();
    assert(f.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:1\r\n");
    assert(f.Run("GET BLOCK 1 0 FROM t") == "_\r\n");
    assert(f.Run("GET BLOCK 2 0 FROM t") == "*1\r\n:3\r\n");
    if (mode != DurabilityMode::kRelaxed) return;
    // A barrier that meets ENOSPC answers DISK_FULL and keeps the batch for
    // the next one.
    Expect(f.Run("SET BLOCK 3 0 IN t n=4"), ":");
    {
        txn_test::ScopedEnv full("CHUNKDB_FAILPOINT_WAL_APPEND_ENOSPC_ONCE", "1");
        Expect(f.Run("FLUSH WAL"), "-ERR DISK_FULL reason=disk_full ");
    }
    assert(f.Run("GET BLOCK 3 0 FROM t") == "*1\r\n:4\r\n");
    assert(f.Run("FLUSH WAL") == "+OK\r\n");
    f.Reopen();
    assert(f.Run("GET BLOCK 3 0 FROM t") == "*1\r\n:4\r\n");
}

// ENOSPC writing a checkpoint image: the write that triggered it stays
// acknowledged, the WAL keeps its records, and a later checkpoint retries.
void CheckpointNoSpace(DurabilityMode mode) {
    Fixture f(mode);
    f.config.default_options.checkpoint_update_interval = 1U;
    f.Open();
    assert(f.Run("CREATE TABLE t (n u32) CHUNK 4 x 4") == "+OK\r\n");
    {
        txn_test::ScopedEnv full("CHUNKDB_FAILPOINT_CHECKPOINT_IMAGE_ENOSPC_ONCE", "1");
        Expect(f.Run("SET BLOCK 0 0 IN t n=1"), ":");
        const char* left = std::getenv("CHUNKDB_FAILPOINT_CHECKPOINT_IMAGE_ENOSPC_ONCE");
        assert(left == nullptr || left[0] == '\0');
    }
    assert(f.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:1\r\n");
    Expect(f.Run("SET BLOCK 1 0 IN t n=2"), ":");
    assert(f.Run("FLUSH WAL") == "+OK\r\n");
    f.Reopen();
    assert(f.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:1\r\n");
    assert(f.Run("GET BLOCK 1 0 FROM t") == "*1\r\n:2\r\n");
}

// ENOSPC writing or closing a transaction frame: COMMIT answers DISK_FULL,
// nothing commits, and the table keeps serving.
void TxnCommitNoSpace(DurabilityMode mode, const char* failpoint) {
    Fixture f(mode);
    f.Open();
    assert(f.Run("CREATE TABLE t (n u32) CHUNK 4 x 4") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN t n=1"), ":");
    assert(f.Run("BEGIN") == "+OK\r\n");
    Expect(f.Run("SET BLOCK 0 0 IN t n=2"), "_");
    Expect(f.Run("SET BLOCK 4 0 IN t n=3"), "_");
    {
        txn_test::ScopedEnv full(failpoint, "1");
        Expect(f.Run("COMMIT"), "-ERR DISK_FULL reason=disk_full ");
    }
    assert(f.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:1\r\n");
    assert(f.Run("GET BLOCK 4 0 FROM t") == "_\r\n");
    Expect(f.Run("SET BLOCK 4 0 IN t n=4"), ":");
    f.Reopen();
    assert(f.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:1\r\n");
    assert(f.Run("GET BLOCK 4 0 FROM t") == "*1\r\n:4\r\n");
}

}  // namespace

int main() {
    CacheBytesBoundAllTables();
    OffCacheCheckpointsLeaveCacheBytes();
    CheckpointGrowthQueuesEviction(false);
    CheckpointGrowthQueuesEviction(true);
    BarrierSyncFailureFailsClosed();
    QueuedBarrierObservesSyncFailure();
    BarrierBatchSyncFailureFailsClosed();
    MultiTableBarrierFailures("FLUSH WAL", false);
    MultiTableBarrierFailures("FLUSH WAL", true);
#ifndef _WIN32
    MultiTableBarrierFailures("SWAP TABLES alpha, beta", false);
    MultiTableBarrierFailures("SWAP TABLES alpha, beta", true);
#endif
    EvictionOpenStreamRepairFailureIsInternal();
    TxnHistoryTotal();
    FeedTotal();
    DiskFloor();
    TableMaxDiskBytes();
    IncompleteDiskScanKeepsRefusal();
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal, DurabilityMode::kFsyncCheckpoint}) {
        WalAppendNoSpace(mode);
        CheckpointNoSpace(mode);
        TxnCommitNoSpace(mode, "CHUNKDB_FAILPOINT_TXN_APPEND_ENOSPC_ONCE");
        TxnCommitNoSpace(mode, "CHUNKDB_FAILPOINT_TXN_APPEND_CLOSE_ENOSPC_ONCE");
    }
    std::cout << "byte budget tests passed\n";
}
