#include <array>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <future>
#include <limits>
#include <memory>
#include <optional>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;

class PausedRead {
  public:
    PausedRead(ChunkStore& store, CachePausePoint point) : store_(store), point_(point) {
        store_.ArmCachePauseForTests(point_, {});
        result_ = std::async(std::launch::async, [&store] { return store.ReadChunkState(ChunkCoord{}); });
    }
    ~PausedRead() { Release(); if (result_.valid()) result_.wait(); }
    void Wait() { assert(store_.WaitForCachePauseForTests(point_)); }
    void Release() { store_.ResumeCachePauseForTests(point_); }
    ChunkState Get() { Release(); return result_.get(); }
  private:
    ChunkStore& store_;
    CachePausePoint point_;
    std::future<ChunkState> result_;
};

struct ReleaseReads {
    std::array<PausedRead*, 3> reads;
    ~ReleaseReads() { for (auto* read : reads) read->Release(); }
};

StoreConfig Config(const std::filesystem::path& root, const std::shared_ptr<StoreResources>& resources) {
    StoreConfig config;
    config.data_dir = root;
    config.geometry = {.large_chunk_width_chunks = 1U, .large_chunk_height_chunks = 1U,
        .chunk_width_blocks = 1U, .chunk_height_blocks = 1U, .block_bits = 8U};
    config.resources = resources;
    config.max_loaded_chunks = 1U;
    config.background_maintenance = false;
    config.durability_mode = DurabilityMode::kRelaxed;
    return config;
}

void SharedConstructionAndReply() {
    test::ScopedTempDir temporary("large-container-shared");
    std::shared_ptr<StoreResources> resources;
    {
        CatalogConfig config;
        config.data_dir = temporary.path();
        config.max_loaded_chunks = 1U;
        config.background_maintenance = false;
        auto catalog = std::make_shared<TableCatalog>(config);
        resources = catalog->resources();
        CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
        SessionState session;
        assert(engine.Execute(session, "HELLO 3").starts_with("%"));
        for (const auto name : {"a", "b", "c", "d"})
            assert(engine.Execute(session, "CREATE TABLE " + std::string(name) +
                " (n u8) CHUNK 1 x 1 LARGE 1 x 1") == "+OK\r\n");
        auto a = catalog->Require("a")->Acquire(), b = catalog->Require("b")->Acquire();
        auto c = catalog->Require("c")->Acquire(), d = catalog->Require("d")->Acquire();
        PausedRead first(a->store(), CachePausePoint::kLargeChunkAfterReservation); first.Wait();
        PausedRead second(b->store(), CachePausePoint::kLargeChunkAfterReservation); second.Wait();
        PausedRead third(c->store(), CachePausePoint::kLargeChunkAfterReservation); third.Wait();
        ReleaseReads release{{&first, &second, &third}};
        assert(resources->max_large_chunks() == 3U && resources->LargeChunkCount() == 3U);
        const auto before = d->store().CompletedRevision();
        assert(engine.Execute(session, "SET BLOCK 0 0 IN d n=7").starts_with(
            "-ERR BUSY reason=large_chunk_cache_full "));
        assert(d->store().CompletedRevision() == before && d->store().ApproxLoadedChunkCount() == 0U);
        assert(resources->LargeChunkRefusals() == 1U);
        const auto metrics = engine.Execute(session, "SHOW METRICS");
        assert(metrics.find("chunkdb_large_chunk_containers 3\n") != std::string::npos);
        assert(metrics.find("chunkdb_large_chunk_container_limit 3\n") != std::string::npos);
        assert(metrics.find("chunkdb_large_chunk_refusals_total 1\n") != std::string::npos);
        auto snapshot = d->store().BeginTxnSnapshot(std::chrono::minutes{1});
        ChunkState pending;
        pending.payload = {7U};
        pending.presence_bitmap = {1U};
        bool refused = false;
        try { (void)d->store().CommitTransaction(*snapshot, {}, {{{}, std::move(pending)}}); }
        catch (const ChunkCacheFullError&) { refused = true; }
        assert(refused && d->store().TxnRegisteredCountForTests() == 0U);
        assert(d->store().CompletedRevision() == before && d->store().ApproxLoadedChunkCount() == 0U);
        assert(d->store().ScanChunkIdentities(false, {}, 1U).entries.empty());
        assert(resources->LargeChunkRefusals() == 2U);
        // An unconditional whole-state SET CHUNK, here a first write, is an
        // ordinary write: it is refused before its version, coordinate and WAL.
        const auto whole = [] {
            ChunkState state;
            state.payload = {7U};
            state.presence_bitmap = {1U};
            return state;
        };
        refused = false;
        try { (void)d->store().WriteChunkState(ChunkCoord{}, whole(), std::nullopt); }
        catch (const ChunkCacheFullError&) { refused = true; }
        assert(refused && d->store().CompletedRevision() == before && d->store().ApproxLoadedChunkCount() == 0U);
        assert(!std::filesystem::exists(ChunkWalPath(d->store().data_dir(), d->store().geometry(), {})));
        assert(d->store().ScanChunkIdentities(false, {}, 1U).entries.empty());
        assert(resources->LargeChunkRefusals() == 3U);
        assert(!first.Get().written);
        // Existing container hits remain usable with every credit held.
        assert(engine.Execute(session, "GET CHUNK 0 0 FROM a") == "_\r\n");
        second.Release(); third.Release();
        assert(!second.Get().written && !third.Get().written);
        assert(!d->store().ReadChunkState(ChunkCoord{}).written);
        const auto retried = d->store().WriteChunkState(ChunkCoord{}, whole(), std::nullopt);
        assert(retried.ok && retried.version != 0U && d->store().ReadChunkState(ChunkCoord{}).written);
        assert(resources->LargeChunkCount() <= resources->max_large_chunks());
    }
    assert(resources->LargeChunkCount() == 0U);
}

void RetiredOwnerKeepsCredit() {
    test::ScopedTempDir temporary("large-container-retired");
    auto resources = std::make_shared<StoreResources>(1U, 16U);
    ChunkStore first(Config(temporary.path() / "a", resources));
    ChunkStore second(Config(temporary.path() / "b", resources));
    ChunkStore damaged(Config(temporary.path() / "bad", resources));
    ChunkStore last(Config(temporary.path() / "last", resources));
    PausedRead a(first, CachePausePoint::kLargeChunkAfterReservation); a.Wait();
    PausedRead b(second, CachePausePoint::kLargeChunkAfterReservation); b.Wait();
    const auto path = ChunkDataPath(damaged.data_dir(), damaged.geometry(), {});
    std::filesystem::create_directories(path.parent_path());
    { std::ofstream file(path, std::ios::binary); file << "damaged"; assert(file.good()); }
    PausedRead failed(damaged, CachePausePoint::kLargeChunkAfterRetirement); failed.Wait();
    ReleaseReads release{{&a, &b, &failed}};
    assert(resources->LargeChunkCount() == 3U && damaged.EvictionLargeChunkRingSizeForTests() == 0U);
    bool full = false;
    try { (void)last.ReadChunkState(ChunkCoord{}); }
    catch (const ChunkCacheFullError&) { full = true; }
    assert(full);
    bool rejected = false;
    try { (void)failed.Get(); }
    catch (const std::runtime_error&) { rejected = true; }
    assert(rejected && resources->LargeChunkCount() == 2U);
    assert(!last.ReadChunkState(ChunkCoord{}).written);
    a.Release(); b.Release();
    (void)a.Get(); (void)b.Get();
}

void AllocationRollback() {
    test::ScopedTempDir temporary("large-container-allocation");
    auto resources = std::make_shared<StoreResources>(1U, 16U);
    ChunkStore store(Config(temporary.path(), resources));
    for (const auto name : {"CHUNKDB_FAILPOINT_LARGE_CHUNK_AFTER_RESERVATION_ONCE",
                           "CHUNKDB_FAILPOINT_LARGE_CHUNK_BEFORE_RING_ONCE"}) {
#ifdef _WIN32
        assert(_putenv_s(name, "1") == 0);
#else
        assert(setenv(name, "1", 1) == 0);
#endif
        bool rejected = false;
        try { (void)store.ReadChunkState(ChunkCoord{}); }
        catch (const std::bad_alloc&) { rejected = true; }
        assert(rejected && resources->LargeChunkCount() == 0U && store.ApproxLoadedChunkCount() == 0U);
        assert(store.EvictionLargeChunkRingSizeForTests() == 0U);
    }
    assert(!store.ReadChunkState(ChunkCoord{}).written && resources->LargeChunkCount() == 1U);
    bool overflow = false;
    try { StoreResources invalid(std::numeric_limits<std::size_t>::max(), 16U); }
    catch (const std::invalid_argument&) { overflow = true; }
    assert(overflow);
}

void CompactionAtCapacity() {
    test::ScopedTempDir temporary("large-container-compaction");
    CatalogConfig config;
    config.data_dir = temporary.path();
    config.max_loaded_chunks = 1U;
    config.background_maintenance = false;
    config.default_options.durability_mode = DurabilityMode::kRelaxed;
    ChunkState expected;
    std::uint64_t deleted = 0U, completed = 0U;
    {
        auto catalog = std::make_shared<TableCatalog>(config);
        CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
        SessionState session;
        assert(engine.Execute(session, "HELLO 3").starts_with("%"));
        assert(engine.Execute(session,
            "CREATE TABLE t (n u8) CHUNK 1 x 1 LARGE 1 x 1 CHUNK COLUMNS (owner u8 REQUIRED)") == "+OK\r\n");
        assert(engine.Execute(session, "SET CHUNK 1 0 IN t META owner=7").starts_with(":"));
        assert(engine.Execute(session, "SET BLOCK 1 0 IN t n=9").starts_with(":"));
        const auto empty = engine.Execute(session, "SET CHUNK 0 0 IN t META owner=0");
        assert(empty.starts_with(":"));
        deleted = std::stoull(empty.substr(1U));
        auto lease = catalog->Require("t")->Acquire();
        lease->store().CheckpointForTests(0, 0);
        expected = lease->store().ReadChunkState(ChunkCoord{1, 0});
    }
    auto catalog = std::make_shared<TableCatalog>(config);
    auto resources = catalog->resources();
    {
        auto lease = catalog->Require("t")->Acquire();
        completed = *lease->store().CompletedRevision();
    }
    CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
    SessionState session;
    assert(engine.Execute(session, "HELLO 3").starts_with("%"));
    for (const auto name : {"a", "b", "c"})
        assert(engine.Execute(session, "CREATE TABLE " + std::string(name) +
            " (n u8) CHUNK 1 x 1 LARGE 1 x 1") == "+OK\r\n");
    auto a = catalog->Require("a")->Acquire(), b = catalog->Require("b")->Acquire();
    auto c = catalog->Require("c")->Acquire();
    PausedRead first(a->store(), CachePausePoint::kLargeChunkAfterReservation); first.Wait();
    PausedRead second(b->store(), CachePausePoint::kLargeChunkAfterReservation); second.Wait();
    PausedRead third(c->store(), CachePausePoint::kLargeChunkAfterReservation); third.Wait();
    ReleaseReads release{{&first, &second, &third}};
    assert(resources->LargeChunkCount() == resources->max_large_chunks());
    assert(catalog->CompactTombstones("t", deleted + 1U) == deleted);
    assert(resources->LargeChunkCount() == resources->max_large_chunks() && resources->LargeChunkRefusals() == 0U);
    first.Release(); second.Release(); third.Release();
    (void)first.Get(); (void)second.Get(); (void)third.Get();
    auto lease = catalog->Require("t")->Acquire();
    const auto actual = lease->store().ReadChunkState(ChunkCoord{1, 0});
    assert(actual.written && actual.version == expected.version && actual.commit_time_ms == expected.commit_time_ms);
    assert(actual.payload == expected.payload && actual.presence_bitmap == expected.presence_bitmap);
    assert(actual.chunk_columns && expected.chunk_columns && actual.chunk_columns->bytes == expected.chunk_columns->bytes);
    const auto forgotten = lease->store().ReadChunkState(ChunkCoord{0, 0});
    assert(!forgotten.written);
    assert(forgotten.version == 0U);
    assert(lease->store().CompletedRevision() == completed);
}

// A background checkpoint in its unlocked window holds its chunk, so that
// chunk and its container stay cached. With every container credit held,
// writes to that chunk are hits and go on, a new miss is refused at once
// without waiting for the files, and tombstone compaction prepares cold
// chunks off-cache and waits for the window before it checkpoints the
// cached chunk.
void CheckpointWindowAtCapacity() {
#ifndef _WIN32
    // On Windows the barrier holds the publish lock through its syncs, so no
    // checkpoint publishes inside them.
    test::ScopedTempDir temporary("large-container-checkpoint-window");
    CatalogConfig config;
    config.data_dir = temporary.path();
    config.max_loaded_chunks = 1U;
    config.background_maintenance = false;
    config.default_options.durability_mode = DurabilityMode::kRelaxed;
    config.default_options.checkpoint_update_interval = 1U;
    GeometryConfig geometry;
    geometry.large_chunk_width_chunks = 1U;
    geometry.large_chunk_height_chunks = 1U;
    geometry.chunk_width_blocks = 1U;
    geometry.chunk_height_blocks = 1U;
    geometry.block_bits = 8U;
    std::uint64_t deleted = 0U;
    {
        TableCatalog catalog(config);
        for (const auto name : {"t", "a", "b"}) (void)catalog.Create(name, geometry, catalog.default_options());
        auto lease = catalog.Require("t")->Acquire();
        auto& store = lease->store();
        store.SetBlockBits(0, 0, "10000000");
        store.UnsetBlock(0, 0);
        deleted = store.GetChunkVersion(0, 0);
        store.CheckpointForTests(0, 0);
        assert(store.RetainedTombstoneCount() == 1U);
        store.SetBlockBits(1, 0, "00000010");
        store.SetBlockBits(2, 0, "00000001");
    }
    config.background_maintenance = true;
    {
        TableCatalog catalog(config);
        const auto resources = catalog.resources();
        ChunkStore* store = nullptr;
        {
            auto lease = catalog.Require("t")->Acquire();
            store = &lease->store();
            (void)store->GetChunkVersion(1, 0);
        }
        assert(resources->LargeChunkCount() == 1U);
        auto a = catalog.Require("a")->Acquire(), b = catalog.Require("b")->Acquire();
        PausedRead first(a->store(), CachePausePoint::kLargeChunkAfterReservation); first.Wait();
        PausedRead second(b->store(), CachePausePoint::kLargeChunkAfterReservation); second.Wait();
        ReleaseReads release{{&first, &second, &second}};
        assert(resources->LargeChunkCount() == resources->max_large_chunks());

        store->ArmCheckpointBeforeWalRemovalPauseForTests();
        store->SetBlockBits(1, 0, "00000100");
        assert(store->WaitForCheckpointBeforeWalRemovalForTests());
        auto hit = std::async(std::launch::async, [&] { store->SetBlockBits(1, 0, "00001000"); });
        const bool hit_returned = hit.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
        const auto before = store->CompletedRevision();
        auto miss = std::async(std::launch::async, [&] {
            try { store->SetBlockBits(3, 0, "00000001"); }
            catch (const ChunkCacheFullError&) { return true; }
            return false;
        });
        const bool miss_returned = miss.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
        store->ResumeCheckpointBeforeWalRemovalForTests();
        hit.get();
        assert(hit_returned && miss_returned && miss.get());
        assert(store->CompletedRevision() == before && resources->LargeChunkRefusals() == 1U);
        assert(!std::filesystem::exists(ChunkWalPath(store->data_dir(), store->geometry(), {3, 0})));
        // Absorb the hit's pending update, so the write below only makes a
        // checkpoint due for a worker.
        store->CheckpointForTests(1, 0);

        store->ArmBarrierBeforeSyncPauseForTests();
        auto compaction = std::async(std::launch::async, [&] { return catalog.CompactTombstones("t", deleted + 1U); });
        assert(store->WaitForBarrierBeforeSyncForTests());
        // Compaction flushed every chunk and holds the table exclusively. It
        // reaches the cached chunk before the cold one, whose checkpoint
        // would wait for the publish lock the window holds.
        store->ArmCheckpointBeforeWalRemovalPauseForTests();
        store->SetBlockBits(1, 0, "00010000");
        assert(store->WaitForCheckpointBeforeWalRemovalForTests());
        store->ResumeBarrierBeforeSyncForTests();
        const bool waited = compaction.wait_for(std::chrono::milliseconds(200)) != std::future_status::ready;
        store->ResumeCheckpointBeforeWalRemovalForTests();
        assert(compaction.get() == deleted);
        assert(waited && resources->LargeChunkRefusals() == 1U);
        assert(resources->LargeChunkCount() <= resources->max_large_chunks());
        first.Release(); second.Release();
        assert(!first.Get().written && !second.Get().written);
        auto lease = catalog.Require("t")->Acquire();
        assert(lease->store().GetBlockBits(1, 0) == "00010000");
        lease->store().WalBarrier();
    }
    config.background_maintenance = false;
    TableCatalog reopened(config);
    auto lease = reopened.Require("t")->Acquire();
    auto& store = lease->store();
    assert(store.GetBlockBits(1, 0) == "00010000" && store.GetBlockBits(2, 0) == "00000001");
    assert(!store.ReadChunkState(0, 0).written && !store.ReadChunkState(3, 0).written);
    assert(store.RetainedTombstoneCount() == 0U && store.RetainedDeletionFloor() == deleted);
#endif
}

void DropReleasesOwnership() {
    test::ScopedTempDir temporary("large-container-drop");
    CatalogConfig config;
    config.data_dir = temporary.path(); config.max_loaded_chunks = 1U; config.background_maintenance = false;
    auto catalog = std::make_shared<TableCatalog>(config);
    CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
    SessionState session;
    assert(engine.Execute(session, "HELLO 3").starts_with("%"));
    assert(engine.Execute(session, "CREATE TABLE t (n u8) CHUNK 1 x 1") == "+OK\r\n");
    assert(engine.Execute(session, "SET BLOCK 0 0 IN t n=7").starts_with(":"));
    assert(catalog->resources()->LargeChunkCount() == 1U);
    assert(engine.Execute(session, "DROP TABLE t") == "+OK\r\n");
    assert(catalog->resources()->LargeChunkCount() == 0U);
}
}

int main() {
    SharedConstructionAndReply();
    RetiredOwnerKeepsCredit();
    AllocationRollback();
    CompactionAtCapacity();
    CheckpointWindowAtCapacity();
    DropReleasesOwnership();
}
