// Tests for world-oriented reads (SCAN CHUNKS/GET AREA), chunk versions and
// conditional mutations (GET CHUNK/SET CHUNK IF VERSION, chunk batches), the
// explicit WAL durability barrier (FLUSH WAL), empty-chunk garbage collection,
// recency-aware eviction, background maintenance, and metrics rendering.

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/table_catalog.hpp"
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

// The first scan discovers cold directories once. Later pages and newly
// loaded large chunks reuse/update the catalog, including cache-only writes.
void TestScanCatalogTracksNewDirectoriesAndEviction() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-scan-catalog");
    auto config = BaseConfig(dir.path());
    config.max_loaded_chunks = 1;
    config.checkpoint_update_interval = 1;
    {
        chunkdb::ChunkStore writer(config);
        for (std::int64_t i = 0; i < 40; ++i) {
            writer.SetBlockBits(i * 16, 0, "10001");
        }
    }
    chunkdb::ChunkStore store(config);
    assert(store.ScanCatalogBuildsForTests() == 0);
    const auto first = store.ScanPopulatedChunks(false, {}, 2);
    assert(first.coords.size() == 2 && first.has_more);
    assert(store.ScanCatalogBuildsForTests() == 1);
    // New directories after initialization must not be hidden by the index.
    store.SetBlockBits(4000, 0, "10001");
    store.SetBlockBits(-4000, 0, "10001");
    store.UnsetBlock(4000, 0);
    store.SetBlockBits(8000, 0, "10001");
    // Re-create a directory removed by empty-chunk GC and eviction.
    store.SetBlockBits(4000, 0, "11011");
    std::size_t seen = first.coords.size();
    auto cursor = first.coords.back();
    for (;;) {
        const auto page = store.ScanPopulatedChunks(true, cursor, 2);
        seen += page.coords.size();
        if (!page.has_more) {
            break;
        }
        cursor = page.coords.back();
    }
    assert(seen == 42);  // the negative coordinate is behind the cursor
    assert(store.ScanCatalogBuildsForTests() == 1);
}

void TestReadOnlyScanCatalogRefreshesAfterWriterChanges() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-scan-catalog-reader");
    auto config = BaseConfig(dir.path());
    chunkdb::ChunkStore writer(config);
    writer.SetBlockBits(0, 0, "10001");
    writer.WalBarrier();
    auto reader_config = config;
    reader_config.access_mode = chunkdb::AccessMode::kReadOnly;
    chunkdb::ChunkStore reader(reader_config);
    assert(reader.ScanPopulatedChunks(false, {}, 10).coords.size() == 1);
    assert(reader.ScanCatalogBuildsForTests() == 1);
    assert(reader.ScanPopulatedChunks(false, {}, 10).coords.size() == 1);
    assert(reader.ScanCatalogBuildsForTests() == 1);
    writer.SetBlockBits(4000, 0, "10001");
    writer.WalBarrier();
    const auto changed = reader.ScanPopulatedChunks(false, {}, 10);
    assert(changed.coords.size() == 2);
    assert(changed.coords.back().x == 1000);
    assert(reader.ScanCatalogBuildsForTests() == 2);
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

// A fully resident world must not make a page more expensive than a cold one:
// the cached chunks are merged per visited large chunk, under the same cursor
// and page-window pruning as the on-disk artifacts, instead of being poured
// into every pass wholesale.
void TestScanWarmCacheMergesOnlyVisitedLargeChunks() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-scan-warm");
    auto config = BaseConfig(dir.path());
    config.max_loaded_chunks = 4096;  // the whole world stays resident
    chunkdb::ChunkStore store(config);

    // 24x24 chunks over 144 large chunks (2x2 chunks each), 12 columns.
    std::vector<chunkdb::ChunkCoord> expected;
    for (std::int64_t cx = -12; cx < 12; ++cx) {
        for (std::int64_t cy = -12; cy < 12; ++cy) {
            store.SetBlockBits(cx * 4, cy * 4, "10001");
            expected.push_back({cx, cy});
        }
    }
    assert(store.ApproxLoadedChunkCount() == expected.size());

    // Warm pagination still enumerates the world exactly once, in order.
    const auto merged_before_full = store.ScanCachedLargeChunksMergedForTests();
    std::vector<chunkdb::ChunkCoord> seen;
    bool has_cursor = false;
    chunkdb::ChunkCoord cursor{};
    while (true) {
        const auto page = store.ScanPopulatedChunks(has_cursor, cursor, 100);
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
    // Six pages over 144 resident large chunks: the unconditional merge cost
    // 6 * 144 = 864 large-chunk visits.
    const auto merged_full = store.ScanCachedLargeChunksMergedForTests() - merged_before_full;
    assert(merged_full < 6 * 144);

    // One page deep in the world touches only the large chunks around it,
    // whether their chunks come from disk or from the cache.
    const auto dirs_before = store.ScanLargeDirsListedForTests();
    const auto merged_before = store.ScanCachedLargeChunksMergedForTests();
    const auto page = store.ScanPopulatedChunks(true, {9, 3}, 10);
    assert(page.has_more);
    assert(page.coords.size() == 10);
    assert(page.coords.front().x == 9 && page.coords.front().y == 4);
    assert(page.coords.back().x == 10 && page.coords.back().y == -11);
    const auto dirs_page = store.ScanLargeDirsListedForTests() - dirs_before;
    const auto merged_page = store.ScanCachedLargeChunksMergedForTests() - merged_before;
    assert(dirs_page <= 36);
    // The cache merge is bounded by the same visit set, not by the 144
    // resident large chunks.
    assert(merged_page <= 36);
}

// A world no wider than one large-chunk column has no column to skip, so the
// only available cut is inside the column: large chunks whose lowest
// coordinate after the cursor already sorts beyond the page window are not
// listed and not merged.
void TestScanNarrowWorldPrunesInsideTheColumn() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-scan-narrow");
    auto config = BaseConfig(dir.path());
    config.max_loaded_chunks = 4096;
    chunkdb::ChunkStore store(config);

    // cx in {0, 1} is exactly one large-chunk column (width 2); 200 rows of
    // chunks spread over 100 large chunks in that single column.
    constexpr std::int64_t kRows = 200;
    for (std::int64_t cy = 0; cy < kRows; ++cy) {
        store.SetBlockBits(0, cy * 4, "10001");
        store.SetBlockBits(4, cy * 4, "10001");
    }

    const auto dirs_before = store.ScanLargeDirsListedForTests();
    const auto merged_before = store.ScanCachedLargeChunksMergedForTests();
    const auto page = store.ScanPopulatedChunks(true, {0, 100}, 10);
    assert(page.has_more);
    assert(page.coords.size() == 10);
    for (std::size_t i = 0; i < page.coords.size(); ++i) {
        assert(page.coords[i].x == 0);
        assert(page.coords[i].y == 101 + static_cast<std::int64_t>(i));
    }
    const auto dirs_page = store.ScanLargeDirsListedForTests() - dirs_before;
    const auto merged_page = store.ScanCachedLargeChunksMergedForTests() - merged_before;
    // Before the y-aware cut this listed and merged all 100 large chunks of
    // the column on every page.
    assert(dirs_page <= 30);
    assert(merged_page <= 30);

    // Pruning must not cost coordinates: the whole column still enumerates.
    std::vector<chunkdb::ChunkCoord> seen;
    bool has_cursor = false;
    chunkdb::ChunkCoord cursor{};
    while (true) {
        const auto walk = store.ScanPopulatedChunks(has_cursor, cursor, 16);
        seen.insert(seen.end(), walk.coords.begin(), walk.coords.end());
        if (!walk.has_more) {
            break;
        }
        has_cursor = true;
        cursor = walk.coords.back();
    }
    assert(seen.size() == static_cast<std::size_t>(2 * kRows));
    for (std::size_t i = 0; i < seen.size(); ++i) {
        const auto expected_x = static_cast<std::int64_t>(i) / kRows;
        const auto expected_y = static_cast<std::int64_t>(i) % kRows;
        assert(seen[i].x == expected_x && seen[i].y == expected_y);
    }
}

// Chunks that were never flushed exist only in the cache. They must still be
// enumerable, including from large chunks that have no directory on disk at
// all, and pagination across them must not lose or repeat one.
void TestScanEnumeratesCacheOnlyChunks() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-scan-cache-only");
    auto config = BaseConfig(dir.path());
    config.max_loaded_chunks = 4096;
    // No group-commit flush and no checkpoint: nothing reaches the filesystem.
    config.wal_group_commit_updates = 1000000;
    config.checkpoint_update_interval = 1000000;
    chunkdb::ChunkStore store(config);

    std::vector<chunkdb::ChunkCoord> expected;
    for (std::int64_t cx = -3; cx < 4; ++cx) {
        for (std::int64_t cy = -3; cy < 4; ++cy) {
            store.SetBlockBits(cx * 4, cy * 4, "10001");
            expected.push_back({cx, cy});
        }
    }

    // Nothing was written out: every candidate can only come from the cache.
    std::size_t large_dirs = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path())) {
        if (entry.is_directory() &&
            entry.path().filename().string().rfind("L_", 0) == 0) {
            ++large_dirs;
        }
    }
    assert(large_dirs == 0);

    std::vector<chunkdb::ChunkCoord> seen;
    bool has_cursor = false;
    chunkdb::ChunkCoord cursor{};
    while (true) {
        const auto page = store.ScanPopulatedChunks(has_cursor, cursor, 5);
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
}

// CHUNKSCAN is not a global snapshot, but it does promise ordering and a
// cursor contract: a chunk that stays populated for the whole walk is
// returned exactly once, chunks created behind the cursor are never
// resurrected, and no coordinate is ever emitted out of order or twice --
// even when the world is mutated between pages and candidates move between
// the cache and the disk.
void RunScanMutationBetweenPagesCase(const char* label, std::size_t cache_chunks) {
    chunkdb::test::ScopedTempDir dir(label);
    auto config = BaseConfig(dir.path());
    config.max_loaded_chunks = cache_chunks;
    chunkdb::ChunkStore store(config);

    // Stable set: chunks (0..15, 0..15), never touched during the scan.
    std::set<std::pair<std::int64_t, std::int64_t>> stable;
    for (std::int64_t cx = 0; cx < 16; ++cx) {
        for (std::int64_t cy = 0; cy < 16; ++cy) {
            store.SetBlockBits(cx * 4, cy * 4, "10001");
            stable.emplace(cx, cy);
        }
    }
    // Doomed set: populated now, emptied while the scan is in flight.
    for (std::int64_t cy = 0; cy < 8; ++cy) {
        store.SetBlockBits(40 * 4, cy * 4, "11111");  // chunk (40, cy)
    }
    store.WalBarrier();

    std::vector<chunkdb::ChunkCoord> seen;
    bool has_cursor = false;
    chunkdb::ChunkCoord cursor{};
    std::size_t pages = 0;
    while (true) {
        const auto page = store.ScanPopulatedChunks(has_cursor, cursor, 7);
        for (const auto& coord : page.coords) {
            if (!seen.empty()) {
                // Strictly ascending across page boundaries: no repeats.
                assert(
                    seen.back().x < coord.x ||
                    (seen.back().x == coord.x && seen.back().y < coord.y));
            }
            // Nothing created behind the cursor may surface.
            assert(coord.x >= 0);
            seen.push_back(coord);
        }
        if (!page.has_more) {
            break;
        }
        has_cursor = true;
        cursor = page.coords.back();
        ++pages;

        // Mutate between pages: create chunks behind the cursor, create
        // chunks far ahead, empty a chunk that is still ahead, and force
        // cache/disk movement for part of the stable set.
        store.SetBlockBits(-static_cast<std::int64_t>(pages) * 4 - 4, 0, "11011");
        store.SetBlockBits(60 * 4 + static_cast<std::int64_t>(pages), 0, "10011");
        if (pages <= 8) {
            store.UnsetBlock(40 * 4, (static_cast<std::int64_t>(pages) - 1) * 4);
        }
        if (pages % 3 == 0) {
            store.WalBarrier();
        }
    }

    // Every stable chunk appears exactly once.
    std::set<std::pair<std::int64_t, std::int64_t>> seen_stable;
    for (const auto& coord : seen) {
        if (stable.count({coord.x, coord.y}) != 0U) {
            assert(seen_stable.emplace(coord.x, coord.y).second);
        }
    }
    assert(seen_stable == stable);
}

void TestScanMutationsBetweenPagesPreserveTheContract() {
    RunScanMutationBetweenPagesCase("chunkdb-world-scan-mutate-cold", 8);
    RunScanMutationBetweenPagesCase("chunkdb-world-scan-mutate-warm", 4096);
}

// Brute-force reference: the page CHUNKSCAN must return for `cursor`/`limit`
// given the exact set of populated coordinates.
struct ExpectedPage {
    std::vector<chunkdb::ChunkCoord> coords;
    bool has_more = false;
};

ExpectedPage ExpectedScanPage(
    const std::vector<chunkdb::ChunkCoord>& sorted_populated,
    bool has_cursor,
    chunkdb::ChunkCoord cursor,
    std::size_t limit) {
    ExpectedPage expected;
    for (const auto& coord : sorted_populated) {
        if (has_cursor &&
            !(cursor.x < coord.x || (cursor.x == coord.x && cursor.y < coord.y))) {
            continue;
        }
        if (expected.coords.size() == limit) {
            expected.has_more = true;
            break;
        }
        expected.coords.push_back(coord);
    }
    return expected;
}

void AssertScanPageMatches(
    chunkdb::ChunkStore* store,
    const std::vector<chunkdb::ChunkCoord>& sorted_populated,
    bool has_cursor,
    chunkdb::ChunkCoord cursor,
    std::size_t limit) {
    const auto expected = ExpectedScanPage(sorted_populated, has_cursor, cursor, limit);
    const auto page = store->ScanPopulatedChunks(has_cursor, cursor, limit);
    assert(page.coords.size() == expected.coords.size());
    for (std::size_t i = 0; i < page.coords.size(); ++i) {
        assert(page.coords[i].x == expected.coords[i].x);
        assert(page.coords[i].y == expected.coords[i].y);
    }
    assert(page.has_more == expected.has_more);
}

// Exhaustive cursor sweep against a brute-force reference. Every cursor
// position is tried, including coordinates that are not populated, that sit
// on a large-chunk edge, and that fall outside the world on either side --
// this is what pins the per-large-chunk cursor test (the `y` cut inside a
// column) down to an exact page, not just to a cheaper one. The same sweep
// runs against a cold store and a fully resident one: the two candidate
// sources are pruned by the same rule, so they must agree coordinate for
// coordinate.
void RunScanCursorSweep(chunkdb::ChunkStore* store,
                        const std::vector<chunkdb::ChunkCoord>& sorted_populated) {
    for (const std::size_t limit : {std::size_t{1}, std::size_t{3}, std::size_t{7}}) {
        AssertScanPageMatches(store, sorted_populated, false, {}, limit);
        for (std::int64_t cx = -7; cx <= 7; ++cx) {
            for (std::int64_t cy = -7; cy <= 7; ++cy) {
                AssertScanPageMatches(store, sorted_populated, true, {cx, cy}, limit);
            }
        }
    }
}

// A large chunk can hold both kinds of candidate at once: chunks already
// flushed to `L_<lx>_<ly>` and chunks that live only in the cache because
// their WAL batch has not been written yet. Visiting such a large chunk must
// merge the cache *and* list the directory -- skipping the merge because the
// directory exists would silently drop the unflushed chunks.
void TestScanMergesCacheEvenWhereADirectoryExists() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-scan-mixed-source");
    auto config = BaseConfig(dir.path());
    config.max_loaded_chunks = 4096;
    // Nothing reaches the filesystem on its own; only an explicit barrier
    // flushes, so each large chunk's split between disk and cache is exact.
    config.wal_group_commit_updates = 1000000;
    config.checkpoint_update_interval = 1000000;
    chunkdb::ChunkStore store(config);

    // Flushed halves: chunk (2*k+1, 1) of large chunk (k, 0), for k in 0..5.
    // These create the L_k_0 directories.
    for (std::int64_t k = 0; k < 6; ++k) {
        store.SetBlockBits((2 * k + 1) * 4, 1 * 4, "10001");
    }
    store.WalBarrier();

    std::size_t large_dirs = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path())) {
        if (entry.is_directory() && entry.path().filename().string().rfind("L_", 0) == 0) {
            ++large_dirs;
        }
    }
    assert(large_dirs == 6);

    // Cache-only halves inside the very same large chunks: chunk (2*k, 0).
    // They sort *before* the flushed ones, so a page that dropped them would
    // both lose coordinates and emit the rest out of order.
    for (std::int64_t k = 0; k < 6; ++k) {
        store.SetBlockBits(2 * k * 4, 0, "11011");
    }

    std::vector<chunkdb::ChunkCoord> expected;
    for (std::int64_t k = 0; k < 6; ++k) {
        expected.push_back({2 * k, 0});
        expected.push_back({2 * k + 1, 1});
    }

    // Whole-world enumeration, one page at a time so the pruning runs on
    // every large chunk with a cursor in hand.
    for (const std::size_t limit : {std::size_t{1}, std::size_t{3}, std::size_t{12}}) {
        std::vector<chunkdb::ChunkCoord> seen;
        bool has_cursor = false;
        chunkdb::ChunkCoord cursor{};
        while (true) {
            const auto page = store.ScanPopulatedChunks(has_cursor, cursor, limit);
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
    }

    // The same holds mid-world, where the large chunk carrying the cursor is
    // itself split across the two sources.
    const auto page = store.ScanPopulatedChunks(true, {4, 0}, 3);
    assert(page.coords.size() == 3);
    assert(page.coords[0].x == 5 && page.coords[0].y == 1);
    assert(page.coords[1].x == 6 && page.coords[1].y == 0);
    assert(page.coords[2].x == 7 && page.coords[2].y == 1);
    assert(page.has_more);
}

void TestScanCursorSweepAgreesWarmAndCold() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-scan-sweep");

    // Sparse and irregular on purpose: gaps inside a large chunk, whole large
    // chunks missing, and both signs of both axes.
    std::vector<chunkdb::ChunkCoord> populated;
    for (std::int64_t cx = -5; cx <= 5; ++cx) {
        for (std::int64_t cy = -5; cy <= 5; ++cy) {
            if (((cx * 7 + cy * 3) & 3) == 0) {
                continue;  // punch holes
            }
            populated.push_back({cx, cy});
        }
    }
    std::sort(populated.begin(), populated.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.x != rhs.x ? lhs.x < rhs.x : lhs.y < rhs.y;
    });
    assert(!populated.empty());

    {
        auto config = BaseConfig(dir.path());
        config.max_loaded_chunks = 4096;
        chunkdb::ChunkStore store(config);
        for (const auto& coord : populated) {
            store.SetBlockBits(coord.x * 4, coord.y * 4, "10001");
        }
        store.WalBarrier();
    }

    // Cold: nothing resident, every candidate comes from the directories.
    {
        auto config = BaseConfig(dir.path());
        config.max_loaded_chunks = 4;
        chunkdb::ChunkStore store(config);
        assert(store.ApproxLoadedChunkCount() == 0);
        RunScanCursorSweep(&store, populated);
    }

    // Warm: the whole world resident, so every large chunk is in the cache as
    // well as on disk and each visit merges both.
    {
        auto config = BaseConfig(dir.path());
        config.max_loaded_chunks = 4096;
        chunkdb::ChunkStore store(config);
        for (const auto& coord : populated) {
            (void)store.GetChunkVersion(coord.x, coord.y);
        }
        assert(store.ApproxLoadedChunkCount() == populated.size());
        RunScanCursorSweep(&store, populated);
    }
}

// The pruning arithmetic multiplies large-chunk coordinates by the large
// chunk size, so the boxes of chunks living at the int64 edges are exactly
// where an overflow would show up. Enumeration and the cursor contract must
// hold there too.
void TestScanExtremeCoordinateLargeChunks() {
    constexpr std::int64_t kMin = std::numeric_limits<std::int64_t>::min();
    constexpr std::int64_t kMax = std::numeric_limits<std::int64_t>::max();

    chunkdb::test::ScopedTempDir dir("chunkdb-world-scan-extreme");
    auto config = BaseConfig(dir.path());
    config.max_loaded_chunks = 4096;
    chunkdb::ChunkStore store(config);

    const std::vector<chunkdb::ChunkCoord> populated = {
        {kMin, kMin}, {kMin, 0}, {kMin, kMax}, {0, 0}, {kMax, kMin}, {kMax, kMax},
    };
    const auto payload = std::string(store.geometry().ChunkPayloadBits(), '1');
    const auto presence = std::string(store.geometry().ChunkBlockCount(), '1');
    for (const auto& coord : populated) {
        store.SetChunkStateBits(coord.x, coord.y, payload, presence);
    }
    store.WalBarrier();

    for (const std::size_t limit : {std::size_t{1}, std::size_t{2}, std::size_t{6}}) {
        std::vector<chunkdb::ChunkCoord> seen;
        bool has_cursor = false;
        chunkdb::ChunkCoord cursor{};
        while (true) {
            const auto page = store.ScanPopulatedChunks(has_cursor, cursor, limit);
            seen.insert(seen.end(), page.coords.begin(), page.coords.end());
            if (!page.has_more) {
                break;
            }
            has_cursor = true;
            cursor = page.coords.back();
        }
        assert(seen.size() == populated.size());
        for (std::size_t i = 0; i < seen.size(); ++i) {
            assert(seen[i].x == populated[i].x && seen[i].y == populated[i].y);
        }
    }

    // Cursors sitting on the extreme coordinates themselves.
    const auto at_max = store.ScanPopulatedChunks(true, {kMax, kMax}, 4);
    assert(at_max.coords.empty() && !at_max.has_more);
    const auto after_min_row = store.ScanPopulatedChunks(true, {kMin, kMax}, 2);
    assert(after_min_row.coords.size() == 2);
    assert(after_min_row.coords[0].x == 0 && after_min_row.coords[0].y == 0);
    assert(after_min_row.coords[1].x == kMax && after_min_row.coords[1].y == kMin);
    assert(after_min_row.has_more);
}

// CHUNKRANGE and CHUNKRADIUS probe each coordinate of their shape directly;
// they never run the scan-candidate walk. Pin that down so the walk stays
// free to prune: the results must not depend on the cache state, and the two
// commands must not touch the scan counters at all.
void TestRangeAndRadiusDoNotUseTheScanWalk() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-range-no-scan-walk");

    std::vector<chunkdb::ChunkCoord> populated;
    for (std::int64_t cx = -4; cx <= 4; ++cx) {
        for (std::int64_t cy = -4; cy <= 4; ++cy) {
            if (((cx + cy) & 1) == 0) {
                continue;
            }
            populated.push_back({cx, cy});
        }
    }
    {
        auto config = BaseConfig(dir.path());
        config.max_loaded_chunks = 4096;
        chunkdb::ChunkStore store(config);
        for (const auto& coord : populated) {
            store.SetBlockBits(coord.x * 4, coord.y * 4, "10001");
        }
        store.WalBarrier();
    }

    const auto collect = [&](std::size_t cache_chunks, bool warm) {
        auto config = BaseConfig(dir.path());
        config.max_loaded_chunks = cache_chunks;
        chunkdb::ChunkStore store(config);
        if (warm) {
            for (const auto& coord : populated) {
                (void)store.GetChunkVersion(coord.x, coord.y);
            }
        }
        const auto dirs_before = store.ScanLargeDirsListedForTests();
        const auto merged_before = store.ScanCachedLargeChunksMergedForTests();
        std::vector<chunkdb::ChunkCoord> coords;
        for (const auto& entry : store.ReadChunkRange(-4, -4, 4, 4)) {
            coords.push_back(entry.coord);
        }
        for (const auto& entry : store.ReadChunkRadius(0, 0, 3)) {
            coords.push_back(entry.coord);
        }
        // Neither command walks large chunks; both counters stay put.
        assert(store.ScanLargeDirsListedForTests() == dirs_before);
        assert(store.ScanCachedLargeChunksMergedForTests() == merged_before);
        return coords;
    };

    const auto cold = collect(4, false);
    const auto warm = collect(4096, true);
    assert(!cold.empty());
    assert(cold.size() == warm.size());
    for (std::size_t i = 0; i < cold.size(); ++i) {
        assert(cold[i].x == warm[i].x && cold[i].y == warm[i].y);
    }
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
    assert(std::any_of(entries[0].presence_bitmap.begin(), entries[0].presence_bitmap.end(),
                       [](std::uint8_t byte) { return byte != 0U; }));
    assert(chunkdb::BitCodec::ExtractBits(entries[0].payload, 15 * 5, 5) == "11111");

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

    // Data the run encoding would expand (short runs alternating with
    // literals) costs at most kZrleMaxOverheadBytes over the input, and still
    // round-trips.
    std::vector<std::vector<std::uint8_t>> expanding;
    for (const std::size_t period : {2U, 3U, 4U, 5U}) {
        std::vector<std::uint8_t> pattern(65536, 0U);
        for (std::size_t i = 0; i < pattern.size(); i += period) {
            pattern[i] = 0x5A;
        }
        expanding.push_back(std::move(pattern));
    }
    std::uint32_t seed = 12345;
    for (int n = 0; n < 64; ++n) {
        std::vector<std::uint8_t> random(static_cast<std::size_t>(n) * 97U);
        for (auto& byte : random) {
            seed = seed * 1664525U + 1013904223U;
            byte = (seed >> 24U) & 1U ? static_cast<std::uint8_t>(seed >> 16U) : 0U;
        }
        expanding.push_back(std::move(random));
    }
    expanding.push_back({0x01});
    for (const auto& original : expanding) {
        const auto encoded = chunkdb::ZrleCompress(original);
        assert(encoded.size() <= original.size() + chunkdb::kZrleMaxOverheadBytes);
        assert(chunkdb::ZrleDecompress(encoded, original.size()) == original);
    }
    // A 1-in-4 pattern used to grow by a quarter; now it is one literal run.
    assert(chunkdb::ZrleCompress(expanding[2]).size() == expanding[2].size() + 5U + 1U + 3U);
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

using Parameters = std::vector<std::optional<std::string>>;

// ":<n>\r\n" -> n.
std::uint64_t VersionOf(const std::string& reply) {
    assert(reply.size() > 3 && reply[0] == ':' && reply.find("\r\n") == reply.size() - 2);
    return std::stoull(reply.substr(1));
}

std::string BulkBody(const std::string& reply) {
    assert(reply.rfind("$", 0) == 0);
    const auto header_end = reply.find("\r\n");
    const auto length = std::stoull(reply.substr(1, header_end - 1));
    assert(reply.size() == header_end + 2 + length + 2);
    return reply.substr(header_end + 2, length);
}

// The version at the start of a chunk form (u64 little-endian).
std::uint64_t FormVersion(const std::string& form) {
    assert(form.size() >= 8);
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(form[i])) << (8U * i);
    }
    return value;
}

struct AreaEntry {
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::string form;
};

// The entries of a GET AREA reply: `*n` of `*3 :cx :cy $form`.
std::vector<AreaEntry> AreaEntries(const std::string& reply) {
    assert(reply.rfind("*", 0) == 0);
    std::size_t cursor = reply.find("\r\n") + 2;
    const auto count = std::stoull(reply.substr(1, cursor - 3));
    const auto integer = [&]() {
        assert(reply[cursor] == ':');
        const auto end = reply.find("\r\n", cursor);
        const auto value = std::stoll(reply.substr(cursor + 1, end - cursor - 1));
        cursor = end + 2;
        return static_cast<std::int64_t>(value);
    };
    std::vector<AreaEntry> entries;
    for (std::size_t i = 0; i < count; ++i) {
        assert(reply.compare(cursor, 4, "*3\r\n") == 0);
        cursor += 4;
        AreaEntry entry;
        entry.x = integer();
        entry.y = integer();
        assert(reply[cursor] == '$');
        const auto header_end = reply.find("\r\n", cursor);
        const auto length = std::stoull(reply.substr(cursor + 1, header_end - cursor - 1));
        entry.form = reply.substr(header_end + 2, length);
        assert(reply.compare(header_end + 2 + length, 2, "\r\n") == 0);
        cursor = header_end + 2 + length + 2;
        entries.push_back(std::move(entry));
    }
    assert(cursor == reply.size());
    return entries;
}

// Each GET AREA entry (box and AROUND) carries the chunk form exactly as
// GET CHUNK with the same COLUMNS returns it.
void TestEngineAreaReadsMatchChunkGet() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-area-forms");
    auto catalog = std::make_shared<chunkdb::TableCatalog>(
        chunkdb::CatalogConfigFromStoreConfig(BaseConfig(dir.path())));
    chunkdb::EngineConfig engine_config;
    engine_config.require_auth = false;
    chunkdb::CommandEngine engine(engine_config, catalog);
    chunkdb::SessionState session;
    assert(engine.Execute(session, "HELLO 3\n").rfind("%8\r\n", 0) == 0);

    // Full, sparse and dense chunks; (1, 1) stays absent and is omitted.
    (void)VersionOf(engine.Execute(session, "SET BLOCK 0 0 IN default bits = b'10101'\n"));
    (void)VersionOf(engine.Execute(session, "SET BLOCK -3 -1 IN default bits = b'00000'\n"));
    // Version (not read), schema version 1, every block present, 16 blocks
    // of 5 bits.
    std::string dense = std::string(8, '\0') + std::string("\x01\0\0\0\0\0\0\0", 8);
    dense += std::string(2, '\xff');
    for (std::size_t i = 0; i < 10; ++i) {
        dense.push_back(static_cast<char>(i * 53 + 9));
    }
    (void)VersionOf(engine.Execute(session, "SET CHUNK 1 0 IN default $1\n", Parameters{dense}));
    assert(BulkBody(engine.Execute(session, "GET CHUNK 1 0 FROM default\n")).substr(8) == dense.substr(8));

    const std::vector<std::string> forms = {"", " COLUMNS bits"};
    for (const auto& form : forms) {
        for (const std::string& area :
             {std::string("GET AREA -1 -1 TO 1 1 FROM default"), std::string("GET AREA AROUND 0 0 RADIUS 2 FROM default")}) {
            const auto entries = AreaEntries(engine.Execute(session, area + form + "\n"));
            const std::vector<std::pair<std::int64_t, std::int64_t>> expected_coords = {{-1, -1}, {0, 0}, {1, 0}};
            assert(entries.size() == expected_coords.size());
            for (std::size_t i = 0; i < expected_coords.size(); ++i) {
                const auto [x, y] = expected_coords[i];
                assert(entries[i].x == x && entries[i].y == y);
                const auto get = engine.Execute(
                    session, "GET CHUNK " + std::to_string(x) + " " + std::to_string(y) + " FROM default" + form + "\n");
                assert(entries[i].form == BulkBody(get));
            }
        }
    }
}

// SET CHUNK ... IF VERSION on a geometry with padding bits: padding is
// ignored and stored as zero, so a write differing only in padding changes
// nothing.
void TestEngineChunkPutIfIgnoresPadding() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-put-padding");
    auto config = BaseConfig(dir.path());
    config.geometry.chunk_width_blocks = 3;
    config.geometry.chunk_height_blocks = 3;
    config.geometry.block_bits = 3;  // 9 presence bits in 2 bytes, 27 payload bits in 4
    auto catalog = std::make_shared<chunkdb::TableCatalog>(
        chunkdb::CatalogConfigFromStoreConfig(config));
    chunkdb::EngineConfig engine_config;
    engine_config.require_auth = false;
    chunkdb::CommandEngine engine(engine_config, catalog);
    chunkdb::SessionState session;
    assert(engine.Execute(session, "HELLO 3\n").rfind("%8\r\n", 0) == 0);

    assert(engine.Execute(session, "GET CHUNK 0 0 FROM default\n") == "_\r\n");
    // NULL has no CAS token. Populate one block to read the initial version.
    (void)VersionOf(engine.Execute(session, "SET BLOCK 0 0 IN default bits = b'000'\n"));
    const auto initial = FormVersion(BulkBody(engine.Execute(session, "GET CHUNK 0 0 FROM default\n")));
    // The version (not read) and schema version 1.
    const std::string version_field = std::string(8, '\0') + std::string("\x01\0\0\0\0\0\0\0", 8);
    const std::string all_ones = version_field + std::string(6, '\xff');
    const auto first = VersionOf(engine.Execute(
        session, "SET CHUNK 0 0 IN default $1 IF VERSION " + std::to_string(initial) + "\n", Parameters{all_ones}));
    assert(first != initial);
    const auto stored = BulkBody(engine.Execute(session, "GET CHUNK 0 0 FROM default\n"));
    assert(FormVersion(stored) == first);
    assert(stored.substr(16) == std::string("\xff\x01\xff\xff\xff\x07", 6));

    // Same blocks, padding cleared: no change, version kept.
    const std::string no_padding = version_field + std::string("\xff\x01\xff\xff\xff\x07", 6);
    assert(VersionOf(engine.Execute(
               session, "SET CHUNK 0 0 IN default $1 IF VERSION " + std::to_string(first) + "\n",
               Parameters{no_padding})) == first);
    // A stale version is still refused.
    assert(engine.Execute(
               session, "SET CHUNK 0 0 IN default $1 IF VERSION " + std::to_string(initial) + "\n",
               Parameters{no_padding}) == "-ERR VERSION_MISMATCH current=" + std::to_string(first) + "\r\n");
}

// A GET AREA reply stays inside the response cap: the per-entry cost covers
// the entry's framing and the version in its chunk form, so the most entries
// the cost admits still fit.
void TestEngineAreaStaysWithinResponseCap() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-area-cap");
    auto config = BaseConfig(dir.path());
    config.geometry.chunk_width_blocks = 512;
    config.geometry.chunk_height_blocks = 512;
    config.geometry.block_bits = 8;
    config.max_loaded_chunks = 227;
    config.checkpoint_update_interval = 1000000;
    config.checkpoint_wal_bytes = 1ULL << 40U;
    auto catalog = std::make_shared<chunkdb::TableCatalog>(
        chunkdb::CatalogConfigFromStoreConfig(config));
    {
        auto lease = *catalog->Find("default")->Acquire();
        auto& store = lease.store();
        std::vector<std::uint8_t> payload(store.geometry().ChunkPayloadBytes(), 0U);
        for (std::size_t i = 0; i < payload.size(); i += 4) {
            payload[i] = 0x5A;
        }
        const std::vector<std::uint8_t> presence(store.geometry().ChunkBlockCount() / 8U, 0xFFU);
        // The 64 MiB cap admits 227 entries of this geometry: the 15 x 15
        // chunks from (0, 0) and two more in column 15.
        for (std::int64_t cx = 0; cx < 15; ++cx) {
            for (std::int64_t cy = 0; cy < 15; ++cy) {
                (void)store.SetChunkStateBytes(cx, cy, payload, presence);
            }
        }
        (void)store.SetChunkStateBytes(15, 0, payload, presence);
        (void)store.SetChunkStateBytes(15, 1, payload, presence);
    }
    chunkdb::EngineConfig engine_config;
    engine_config.require_auth = false;
    chunkdb::CommandEngine engine(engine_config, catalog);
    chunkdb::SessionState session;
    assert(engine.Execute(session, "HELLO 3\n").rfind("%8\r\n", 0) == 0);
    const auto reply = engine.Execute(session, "GET AREA 0 0 TO 15 14 FROM default\n");
    assert(reply.rfind("*227\r\n", 0) == 0);
    assert(reply.size() <= chunkdb::kMaxChunkRangeResponseBytes);
}

void TestEngineCommands() {
    chunkdb::test::ScopedTempDir dir("chunkdb-world-engine");
    auto catalog = std::make_shared<chunkdb::TableCatalog>(
        chunkdb::CatalogConfigFromStoreConfig(BaseConfig(dir.path())));
    auto lease = *catalog->Find("default")->Acquire();
    auto* store = &lease.store();
    chunkdb::EngineConfig engine_config;
    engine_config.require_auth = false;
    chunkdb::CommandEngine engine(engine_config, catalog);
    chunkdb::SessionState session;
    assert(engine.Execute(session, "HELLO 3\n").rfind("%8\r\n", 0) == 0);

    (void)VersionOf(engine.Execute(session, "SET BLOCK 0 0 IN default bits = b'10101'\n"));
    (void)VersionOf(engine.Execute(session, "SET BLOCK -1 -1 IN default bits = b'11111'\n"));

    assert(engine.Execute(session, "SCAN CHUNKS FROM default LIMIT 10\n") ==
           "%2\r\n$6\r\nchunks\r\n*2\r\n*2\r\n:-1\r\n:-1\r\n*2\r\n:0\r\n:0\r\n$4\r\nmore\r\n#f\r\n");

    // Two populated chunks, each with its coordinates and chunk form.
    const auto range = AreaEntries(engine.Execute(session, "GET AREA -1 -1 TO 0 0 FROM default\n"));
    assert(range.size() == 2);
    assert(range[0].x == -1 && range[0].y == -1);
    assert(range[1].x == 0 && range[1].y == 0);

    const auto version = FormVersion(BulkBody(engine.Execute(session, "GET CHUNK 0 0 FROM default\n")));

    // Version (not read), schema version 1, then presence and payload.
    const std::string state =
        std::string(8, '\0') + std::string("\x01\0\0\0\0\0\0\0", 8) +
        std::string((store->geometry().ChunkBlockCount() + 7U) / 8U + store->geometry().ChunkPayloadBytes(), '\xff');
    const std::string put = "SET CHUNK 0 0 IN default $1 IF VERSION " + std::to_string(version) + "\n";
    const auto written = VersionOf(engine.Execute(session, put, Parameters{state}));
    assert(engine.Execute(session, put, Parameters{state}) ==
           "-ERR VERSION_MISMATCH current=" + std::to_string(written) + "\r\n");
    assert(engine.Execute(session, "GET BLOCK 0 0 FROM default\n") == "*1\r\n$1\r\n\x1f\r\n");

    assert(engine.Execute(session, "FLUSH WAL\n") == "+OK\r\n");

    const auto metrics = BulkBody(engine.Execute(session, "SHOW METRICS\n"));
    assert(metrics.find("chunkdb_commands_total") != std::string::npos);
    assert(metrics.find("chunkdb_wal_barriers_total 1") != std::string::npos);
    assert(metrics.find("chunkdb_empty_chunk_gcs_total") != std::string::npos);
}

}  // namespace

int main() {
    TestScanCatalogTracksNewDirectoriesAndEviction();
    TestReadOnlyScanCatalogRefreshesAfterWriterChanges();
    TestScanVisitsOnlyNeededLargeChunkColumns();
    TestScanWarmCacheMergesOnlyVisitedLargeChunks();
    TestScanNarrowWorldPrunesInsideTheColumn();
    TestScanEnumeratesCacheOnlyChunks();
    TestScanMutationsBetweenPagesPreserveTheContract();
    TestScanMergesCacheEvenWhereADirectoryExists();
    TestScanCursorSweepAgreesWarmAndCold();
    TestScanExtremeCoordinateLargeChunks();
    TestRangeAndRadiusDoNotUseTheScanWalk();
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
    TestEngineAreaReadsMatchChunkGet();
    TestEngineChunkPutIfIgnoresPadding();
    TestEngineAreaStaysWithinResponseCap();
    return 0;
}
