// Tests for world-oriented reads (CHUNKSCAN/CHUNKRANGE), chunk versions and
// conditional mutations (CHUNKVER/CHUNKCAS/CHUNKBATCH), the explicit WAL
// durability barrier (WALFLUSH), empty-chunk garbage collection,
// recency-aware eviction, background maintenance, and metrics rendering.

#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/metrics.hpp"
#include "chunkdb/zrle.hpp"
#include "test_utils.hpp"

namespace {

chunkdb::StoreConfig BaseConfig(const std::filesystem::path& data_dir) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 5,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 1000,
        .checkpoint_wal_bytes = 1024 * 1024,
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
    };
}

void TestScanVisitsOnlyNeededLargeChunkColumns() {
    // Large chunks are 2x2 regular chunks (BaseConfig), so a 24x24 chunk
    // world spans 144 L_ directories in 12 columns. Every page must list only
    // the columns it needs, and pagination must still enumerate everything in
    // ascending (cx, cy) order, negatives included.
    chunkdb::test::ScopedTempDir dir("chunkdb-world-scan-columns");
    auto config = BaseConfig(dir.path());
    config.max_loaded_chunks = 8;  // keep the world on disk, not in cache
    chunkdb::ChunkStore store(config);
    const auto& geo = store.geometry().config();
    assert(geo.large_chunk_width_chunks == 2 && geo.chunk_width_blocks == 4);

    std::vector<chunkdb::ChunkCoord> expected;
    for (std::int64_t cx = -12; cx < 12; ++cx) {
        for (std::int64_t cy = -12; cy < 12; ++cy) {
            store.SetBlockBits(cx * 4, cy * 4, "10001");
            expected.push_back({cx, cy});
        }
    }
    store.WalBarrier();

    // A full walk from the start touches every column.
    const auto before_full = store.ScanLargeDirsListedForTests();
    std::vector<chunkdb::ChunkCoord> seen;
    bool has_cursor = false;
    chunkdb::ChunkCoord cursor{};
    std::size_t pages = 0;
    while (true) {
        const auto page = store.ScanPopulatedChunks(has_cursor, cursor, 100);
        ++pages;
        seen.insert(seen.end(), page.coords.begin(), page.coords.end());
        if (!page.has_more) {
            break;
        }
        has_cursor = true;
        cursor = page.coords.back();
    }
    assert(seen.size() == expected.size());
    for (std::size_t i = 0; i < seen.size(); ++i) {
        assert(seen[i].x == expected[i].x && seen[i].y == expected[i].y);
    }
    assert(pages == 6);
    const auto listed_full = store.ScanLargeDirsListedForTests() - before_full;
    // 6 pages over 144 directories: the old walk listed all 144 per page
    // (864); the column walk lists each column at most once per page it
    // contributes to, plus the column that closes the window.
    assert(listed_full < 6 * 144);
    assert(listed_full <= 144 + 6 * 12);

    // A page deep in the world skips every column before the cursor and
    // stops right after its own.
    const auto before_page = store.ScanLargeDirsListedForTests();
    const auto page = store.ScanPopulatedChunks(true, {9, 3}, 10);
    assert(page.has_more);
    assert(page.coords.size() == 10);
    assert(page.coords.front().x == 9 && page.coords.front().y == 4);
    // (9,4)..(9,11) are 8 chunks, then the column cx=10 starts at cy=-12.
    assert(page.coords.back().x == 10 && page.coords.back().y == -11);
    const auto listed_page = store.ScanLargeDirsListedForTests() - before_page;
    // Columns lx=4 (cx 8..9), lx=5 (cx 10..11), and lx=6 closes the window:
    // at most 3 columns of 12 directories each.
    assert(listed_page <= 36);

    // The last page hits the end without a resume loop.
    const auto tail = store.ScanPopulatedChunks(true, {11, 9}, 10);
    assert(!tail.has_more);
    assert(tail.coords.size() == 2);
    assert(tail.coords[0].x == 11 && tail.coords[0].y == 10);
    assert(tail.coords[1].x == 11 && tail.coords[1].y == 11);
}

void TestScanAndRange() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-scan");
    chunkdb::ChunkStore store(BaseConfig(dir.path()));

    // Populate chunks at negative and positive coordinates; leave (5,5) absent
    // and (7,7) explicitly emptied.
    store.SetBlockBits(0, 0, "10101");        // chunk (0,0)
    store.SetBlockBits(-1, -1, "11111");      // chunk (-1,-1)
    store.SetBlockBits(9, 1, "00001");        // chunk (2,0)
    store.SetBlockBits(4, 4, "01110");        // chunk (1,1)
    store.SetBlockBits(28, 28, "00110");      // chunk (7,7)
    store.UnsetBlock(28, 28);                 // now empty again

    // Full scan, ascending (cx, cy).
    const auto page = store.ScanPopulatedChunks(false, {}, 16);
    assert(!page.has_more);
    assert(page.coords.size() == 4);
    assert(page.coords[0].x == -1 && page.coords[0].y == -1);
    assert(page.coords[1].x == 0 && page.coords[1].y == 0);
    assert(page.coords[2].x == 1 && page.coords[2].y == 1);
    assert(page.coords[3].x == 2 && page.coords[3].y == 0);

    // Pagination with limit 2 and cursor continuation.
    const auto first = store.ScanPopulatedChunks(false, {}, 2);
    assert(first.has_more);
    assert(first.coords.size() == 2);
    const auto second = store.ScanPopulatedChunks(true, first.coords.back(), 2);
    assert(!second.has_more);
    assert(second.coords.size() == 2);
    assert(second.coords[0].x == 1 && second.coords[0].y == 1);
    assert(second.coords[1].x == 2 && second.coords[1].y == 0);

    // Range read returns exact per-block state and skips absent chunks
    // without inserting them into the cache.
    const auto loaded_before = store.ApproxLoadedChunkCount();
    const auto entries = store.ReadChunkRange(-2, -2, 2, 2);
    assert(store.ApproxLoadedChunkCount() == loaded_before);
    assert(entries.size() == 4);  // (-1,-1), (0,0), (1,1), (2,0) within the rect
    assert(entries[0].coord.x == -1 && entries[0].coord.y == -1);
    assert(entries[0].presence_bits.find('1') != std::string::npos);
    assert(entries[0].payload_bits.substr(15 * 5, 5) == "11111");

    // Absent chunk probes do not pollute the cache.
    assert(!store.IsChunkLoadedForTests(50, 50));
    const auto far_entries = store.ReadChunkRange(50, 50, 51, 51);
    assert(far_entries.empty());
    assert(!store.IsChunkLoadedForTests(50, 50));
    assert(!store.IsChunkLoadedForTests(51, 51));

    // Limits are enforced.
    bool threw = false;
    try {
        (void)store.ReadChunkRange(0, 0, 1000, 1000);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);
    threw = false;
    try {
        (void)store.ScanPopulatedChunks(false, {}, 0);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);
}

// Duplicate-heavy worlds (chunks contributing a `.chk`, a `.wal`, and a
// cached entry at once) must stay fully enumerable with stable order and
// bounded per-page memory — duplicates collapse instead of counting against
// any candidate cap.
void TestScanDuplicateArtifactsStayEnumerable() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-scan-dups");
    auto config = BaseConfig(dir.path());
    // Force a checkpoint per update so every chunk has a `.chk`, then more
    // writes recreate a `.wal`, while the chunk also stays cached.
    config.checkpoint_update_interval = 1;
    chunkdb::ChunkStore store(config);

    constexpr int kChunks = 24;
    for (int i = 0; i < kChunks; ++i) {
        store.SetBlockBits(i * 4, 0, "10101");
        store.SetBlockBits(i * 4 + 1, 0, "01010");
        store.SetBlockBits(i * 4 + 2, 0, "11111");
    }

    // Enumerate with a small page size and verify the full ascending set.
    std::vector<chunkdb::ChunkCoord> seen;
    bool has_cursor = false;
    chunkdb::ChunkCoord cursor{};
    while (true) {
        const auto page = store.ScanPopulatedChunks(has_cursor, cursor, 5);
        for (const auto& coord : page.coords) {
            seen.push_back(coord);
        }
        if (!page.has_more) {
            break;
        }
        has_cursor = true;
        cursor = page.coords.back();
    }
    assert(seen.size() == static_cast<std::size_t>(kChunks));
    for (int i = 0; i < kChunks; ++i) {
        assert(seen[static_cast<std::size_t>(i)].x == i);
        assert(seen[static_cast<std::size_t>(i)].y == 0);
    }
}

void TestScanSeesUnloadedCheckpoints() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-scan-disk");
    auto config = BaseConfig(dir.path());
    config.checkpoint_update_interval = 1;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        store.SetBlockBits(0, 0, "10100");  // trigger checkpoint hysteresis
        store.SetBlockBits(0, 0, "10101");
    }
    // Fresh store: nothing loaded, chunk only exists on disk.
    chunkdb::ChunkStore store(config);
    const auto page = store.ScanPopulatedChunks(false, {}, 16);
    assert(page.coords.size() == 1);
    assert(page.coords[0].x == 0 && page.coords[0].y == 0);
    // Scanning did not load the chunk into the cache.
    assert(!store.IsChunkLoadedForTests(0, 0));
}

void TestVersionsCasBatch() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-cas");
    chunkdb::ChunkStore store(BaseConfig(dir.path()));
    const auto payload_bits = std::string(store.geometry().ChunkPayloadBits(), '0');
    const auto presence_all = std::string(store.geometry().ChunkBlockCount(), '1');
    const auto presence_none = std::string(store.geometry().ChunkBlockCount(), '0');

    const auto v1 = store.GetChunkVersion(0, 0);
    assert(v1 != 0);
    assert(store.GetChunkVersion(0, 0) == v1);  // reads do not change versions

    store.SetBlockBits(0, 0, "10101");
    const auto v2 = store.GetChunkVersion(0, 0);
    assert(v2 != v1);

    // CAS with the correct version succeeds and returns a new version.
    const auto cas_ok = store.CasChunkState(0, 0, v2, payload_bits, presence_all);
    assert(cas_ok.ok);
    assert(cas_ok.version != v2);

    // CAS with a stale version fails and reports the current version.
    const auto cas_stale = store.CasChunkState(0, 0, v2, payload_bits, presence_none);
    assert(!cas_stale.ok);
    assert(cas_stale.version == cas_ok.version);
    assert(store.GetChunkStateBits(0, 0).find(presence_all) != std::string::npos);

    // Batch applies atomically and bumps the version once.
    std::vector<chunkdb::ChunkBatchOp> ops;
    ops.push_back({.set = true, .x = 0, .y = 0, .bits = "11111"});
    ops.push_back({.set = true, .x = 1, .y = 1, .bits = "00111"});
    ops.push_back({.set = false, .x = 2, .y = 2, .bits = ""});
    const auto batch_ok = store.ApplyChunkBatch(0, 0, true, cas_ok.version, ops);
    assert(batch_ok.ok);
    assert(store.GetBlockBits(0, 0) == "11111");
    assert(store.GetBlockBits(1, 1) == "00111");
    assert(!store.BlockExists(2, 2));

    // Version mismatch leaves state untouched.
    std::vector<chunkdb::ChunkBatchOp> stale_ops;
    stale_ops.push_back({.set = true, .x = 0, .y = 0, .bits = "00000"});
    const auto batch_stale = store.ApplyChunkBatch(0, 0, true, cas_ok.version, stale_ops);
    assert(!batch_stale.ok);
    assert(store.GetBlockBits(0, 0) == "11111");

    // Validation failure (block outside chunk) rejects the whole batch.
    std::vector<chunkdb::ChunkBatchOp> bad_ops;
    bad_ops.push_back({.set = true, .x = 0, .y = 0, .bits = "00000"});
    bad_ops.push_back({.set = true, .x = 100, .y = 100, .bits = "00000"});
    bool threw = false;
    try {
        (void)store.ApplyChunkBatch(0, 0, false, 0, bad_ops);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);
    assert(store.GetBlockBits(0, 0) == "11111");
}

void TestVersionStableAcrossReload() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-ver-reload");
    auto config = BaseConfig(dir.path());
    std::uint64_t before = 0;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        before = store.GetChunkVersion(0, 0);
    }
    chunkdb::ChunkStore store(config);
    // Format v2: the revision is persisted with the mutation, so a restart
    // (or eviction) does not change it and a token read before the restart
    // still matches unchanged content.
    const auto after = store.GetChunkVersion(0, 0);
    assert(after == before);
    const auto cas = store.CasChunkState(
        0,
        0,
        before,
        std::string(store.geometry().ChunkPayloadBits(), '0'),
        std::string(store.geometry().ChunkBlockCount(), '0'));
    assert(cas.ok);
    assert(cas.version > before);
    assert(store.GetChunkVersion(0, 0) == cas.version);
    // The stale token is rejected once content moved on.
    const auto stale = store.CasChunkState(
        0,
        0,
        before,
        std::string(store.geometry().ChunkPayloadBits(), '1'),
        std::string(store.geometry().ChunkBlockCount(), '1'));
    assert(!stale.ok);
}

void TestWalBarrier() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-barrier");
    auto config = BaseConfig(dir.path());
    config.wal_group_commit_updates = 64;  // keep updates batched in memory
    chunkdb::ChunkStore store(config);

    store.SetBlockBits(0, 0, "10101");
    const auto wal_path = chunkdb::ChunkWalPath(dir.path(), store.geometry(), {0, 0});
    // Relaxed mode with a large group-commit window: nothing flushed yet.
    assert(!std::filesystem::exists(wal_path));

    store.WalBarrier();
    assert(std::filesystem::exists(wal_path));
    assert(store.RuntimeStats().wal_barriers == 1);

    // Barrier is idempotent and cheap when there is nothing pending.
    store.WalBarrier();
    assert(store.RuntimeStats().wal_barriers == 2);
}

void TestEmptyChunkGc() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-gc");
    auto config = BaseConfig(dir.path());
    config.checkpoint_update_interval = 1;
    chunkdb::ChunkStore store(config);

    const auto data_path = chunkdb::ChunkDataPath(dir.path(), store.geometry(), {0, 0});
    const auto wal_path = chunkdb::ChunkWalPath(dir.path(), store.geometry(), {0, 0});

    store.SetBlockBits(0, 0, "10101");
    store.SetBlockBits(1, 0, "11111");
    assert(std::filesystem::exists(data_path));

    // Empty the chunk: the next checkpoint reclaims image, WAL, and the
    // parent directory once it is empty.
    store.UnsetBlock(0, 0);
    store.UnsetBlock(1, 0);
    assert(store.RuntimeStats().empty_chunk_gcs >= 1);
    assert(!std::filesystem::exists(data_path));
    assert(!std::filesystem::exists(wal_path));
    assert(!std::filesystem::exists(data_path.parent_path()));

    // Rewriting the chunk recreates storage; GC must not resurrect the old
    // data or drop the new write.
    store.SetBlockBits(0, 0, "01110");
    assert(store.GetBlockBits(0, 0) == "01110");
    assert(std::filesystem::exists(data_path));

    // An explicit all-zero-payload chunk is present, not empty: no GC.
    const std::string zero_payload(store.geometry().ChunkPayloadBits(), '0');
    const std::string full_presence(store.geometry().ChunkBlockCount(), '1');
    store.SetChunkStateBits(4, 4, zero_payload, full_presence);
    store.SetChunkStateBits(4, 4, zero_payload, full_presence);
    const auto zero_data_path = chunkdb::ChunkDataPath(dir.path(), store.geometry(), {4, 4});
    assert(std::filesystem::exists(zero_data_path));
    assert(store.ChunkExists(4, 4));
}

void TestEmptyChunkGcSurvivesRestart() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-gc-restart");
    auto config = BaseConfig(dir.path());
    config.checkpoint_update_interval = 1;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        store.UnsetBlock(0, 0);
    }
    chunkdb::ChunkStore store(config);
    assert(!store.ChunkExists(0, 0));
    assert(!store.BlockExists(0, 0));
    assert(store.GetBlockBits(0, 0) == "00000");
}

void TestRecencyAwareEviction() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-lru");
    auto config = BaseConfig(dir.path());
    config.max_loaded_chunks = 8;
    chunkdb::ChunkStore store(config);

    // Load 8 chunks in distinct large chunks (span 2x2 chunks -> use even
    // coordinates far apart).
    for (int i = 0; i < 8; ++i) {
        store.SetBlockBits(static_cast<std::int64_t>(i) * 8, 0, "10101");
    }
    // Touch chunk 0 repeatedly so it is the most recently used.
    for (int repeat = 0; repeat < 4; ++repeat) {
        (void)store.GetBlockBits(0, 0);
    }
    // Load enough new chunks to push the cache over its bound and force
    // eviction down to the lower watermark (1, given the small bound).
    for (int i = 8; i < 24; ++i) {
        store.SetBlockBits(static_cast<std::int64_t>(i) * 8, 0, "11111");
    }

    const auto stats = store.RuntimeStats();
    assert(stats.evictions > 0);
    // All data remains readable regardless of eviction.
    assert(store.GetBlockBits(0, 0) == "10101");
    assert(store.GetBlockBits(9 * 8, 0) == "11111");
}

void TestBackgroundMaintenance() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-bg");
    auto config = BaseConfig(dir.path());
    config.checkpoint_update_interval = 1;
    config.background_maintenance = true;
    const auto data_path_00 = chunkdb::ChunkDataPath(dir.path(), chunkdb::Geometry(config.geometry), {0, 0});

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        store.SetBlockBits(0, 0, "10100");
        store.SetBlockBits(0, 0, "10101");

        // The checkpoint happens on the maintenance thread; wait bounded.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while ((!std::filesystem::exists(data_path_00) ||
                store.RuntimeStats().background_checkpoints < 1) &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        assert(std::filesystem::exists(data_path_00));
        assert(store.RuntimeStats().background_checkpoints >= 1);

        // Queue drains on shutdown: write more without waiting.
        store.SetBlockBits(4, 0, "11111");
        store.SetBlockBits(4, 0, "11110");
    }

    // After a clean shutdown all state is recoverable.
    config.background_maintenance = false;
    chunkdb::ChunkStore store(config);
    assert(store.GetBlockBits(0, 0) == "10101");
    assert(store.GetBlockBits(4, 0) == "11110");
}

void TestZrleCodec() {
    // Round trips: sparse, dense, empty, all-zero.
    const std::vector<std::vector<std::uint8_t>> cases = {
        {},
        std::vector<std::uint8_t>(1024, 0U),
        [] {
            std::vector<std::uint8_t> sparse(1024, 0U);
            sparse[3] = 0xAB;
            sparse[700] = 0x01;
            return sparse;
        }(),
        [] {
            std::vector<std::uint8_t> dense(1024);
            for (std::size_t i = 0; i < dense.size(); ++i) {
                dense[i] = static_cast<std::uint8_t>((i * 31 + 7) & 0xFF);
            }
            return dense;
        }(),
    };
    for (const auto& original : cases) {
        const auto compressed = chunkdb::ZrleCompress(original);
        const auto restored = chunkdb::ZrleDecompress(compressed, original.size());
        assert(restored == original);
    }

    // Sparse data compresses well.
    const auto sparse_compressed = chunkdb::ZrleCompress(cases[2]);
    assert(sparse_compressed.size() < cases[2].size() / 4);

    // Corrupt, truncated, and bomb inputs fail safely.
    auto compressed = chunkdb::ZrleCompress(cases[2]);
    bool threw = false;
    try {  // truncation
        (void)chunkdb::ZrleDecompress(compressed.data(), compressed.size() - 3, cases[2].size());
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);
    threw = false;
    try {  // declared-size mismatch (decompression bomb guard)
        (void)chunkdb::ZrleDecompress(compressed, cases[2].size() * 100);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);
    threw = false;
    compressed[0] = 0x7F;  // unknown codec id
    try {
        (void)chunkdb::ZrleDecompress(compressed, cases[2].size());
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);
}

void TestCheckpointCompression() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-zrle");
    auto config = BaseConfig(dir.path());
    config.checkpoint_update_interval = 1;
    config.checkpoint_compression = chunkdb::CheckpointCompression::kZrle;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        store.SetBlockBits(1, 1, "11111");
    }

    // Compressed images are readable by a store with compression disabled
    // (read support is unconditional; only writing is opt-in) ...
    config.checkpoint_compression = chunkdb::CheckpointCompression::kNone;
    {
        chunkdb::ChunkStore store(config);
        assert(store.GetBlockBits(0, 0) == "10101");
        assert(store.GetBlockBits(1, 1) == "11111");
        // ... and this store rewrites uncompressed images on checkpoint.
        store.SetBlockBits(2, 2, "01010");
    }
    // ... which stay readable by a compressing store (mixed images).
    config.checkpoint_compression = chunkdb::CheckpointCompression::kZrle;
    chunkdb::ChunkStore store(config);
    assert(store.GetBlockBits(0, 0) == "10101");
    assert(store.GetBlockBits(2, 2) == "01010");
}

void TestMetricsRegistry() {
    chunkdb::MetricsRegistry registry;

    // Concurrent observation and rendering must not crash or lose counts.
    std::atomic<bool> stop{false};
    std::thread renderer([&]() {
        chunkdb::StoreRuntimeStats stats{};
        while (!stop.load()) {
            (void)registry.RenderPrometheus(stats, 0);
        }
    });
    std::vector<std::thread> writers;
    constexpr int kThreads = 4;
    constexpr int kPerThread = 5000;
    for (int t = 0; t < kThreads; ++t) {
        writers.emplace_back([&]() {
            for (int i = 0; i < kPerThread; ++i) {
                registry.ObserveCommand(
                    chunkdb::MetricsRegistry::CommandClass::kPointWrite, 0.0001, true);
            }
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }
    stop.store(true);
    renderer.join();

    chunkdb::StoreRuntimeStats stats{};
    const auto text = registry.RenderPrometheus(stats, 3);
    const std::string expected_count =
        "chunkdb_commands_total{class=\"point_write\",outcome=\"ok\"} " +
        std::to_string(kThreads * kPerThread);
    assert(text.find(expected_count) != std::string::npos);
    assert(text.find("chunkdb_command_duration_seconds_bucket") != std::string::npos);
    assert(text.find("chunkdb_loaded_chunks 3") != std::string::npos);
    assert(text.find("# TYPE chunkdb_command_duration_seconds histogram") != std::string::npos);
}

void TestEngineCommands() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-engine");
    auto store = std::make_shared<chunkdb::ChunkStore>(BaseConfig(dir.path()));
    chunkdb::EngineConfig engine_config;
    engine_config.require_auth = false;
    chunkdb::CommandEngine engine(engine_config, store);
    chunkdb::SessionState session;

    assert(engine.Execute(session, "SET 0 0 10101\n") == "+OK\r\n");
    assert(engine.Execute(session, "SET -1 -1 11111\n") == "+OK\r\n");

    const auto scan = engine.Execute(session, "CHUNKSCAN 10\n");
    assert(scan.rfind("*3\r\n", 0) == 0);
    assert(scan.find("END") != std::string::npos);
    assert(scan.find("-1 -1") != std::string::npos);
    assert(scan.find("0 0") != std::string::npos);

    const auto range = engine.Execute(session, "CHUNKRANGE -1 -1 0 0\n");
    assert(range.rfind("*2\r\n", 0) == 0);
    assert(range.find('|') != std::string::npos);

    const auto ver_reply = engine.Execute(session, "CHUNKVER 0 0\n");
    assert(ver_reply[0] == '$');
    const auto ver_begin = ver_reply.find("\r\n") + 2;
    const auto version = ver_reply.substr(ver_begin, ver_reply.find("\r\n", ver_begin) - ver_begin);

    const std::string payload(store->geometry().ChunkPayloadBits(), '0');
    const std::string presence(store->geometry().ChunkBlockCount(), '1');
    const auto cas_ok = engine.Execute(
        session, "CHUNKCAS 0 0 " + version + " STATE " + payload + "|" + presence + "\n");
    assert(cas_ok[0] == '$');
    const auto cas_stale = engine.Execute(
        session, "CHUNKCAS 0 0 " + version + " STATE " + payload + "|" + presence + "\n");
    assert(cas_stale.rfind("-ERR VERSION_MISMATCH", 0) == 0);

    const auto batch = engine.Execute(session, "CHUNKBATCH 0 0 - SET 0 0 11111 UNSET 1 1\n");
    assert(batch[0] == '$');
    assert(engine.Execute(session, "GET 0 0\n") == "$5\r\n11111\r\n");

    assert(engine.Execute(session, "WALFLUSH\n") == "+OK\r\n");

    const auto metrics = engine.Execute(session, "METRICS\n");
    assert(metrics[0] == '$');
    assert(metrics.find("chunkdb_commands_total") != std::string::npos);
    assert(metrics.find("chunkdb_wal_barriers_total 1") != std::string::npos);

    const auto info = engine.Execute(session, "INFO\n");
    assert(info.find("wal_barriers=1") != std::string::npos);
    assert(info.find("empty_chunk_gcs=") != std::string::npos);
}

}  // namespace

int main() {
    TestScanVisitsOnlyNeededLargeChunkColumns();
    TestScanAndRange();
    TestScanDuplicateArtifactsStayEnumerable();
    TestScanSeesUnloadedCheckpoints();
    TestVersionsCasBatch();
    TestVersionStableAcrossReload();
    TestWalBarrier();
    TestEmptyChunkGc();
    TestEmptyChunkGcSurvivesRestart();
    TestRecencyAwareEviction();
    TestBackgroundMaintenance();
    TestZrleCodec();
    TestCheckpointCompression();
    TestMetricsRegistry();
    TestEngineCommands();
    return 0;
}
