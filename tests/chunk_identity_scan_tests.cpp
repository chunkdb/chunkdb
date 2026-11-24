#include <cassert>
#include <chrono>
#include <future>
#include <set>

#include "area_cursor.hpp"

#include "chunkdb/file_layout.hpp"
#include "test_utils.hpp"
#include "txn_test_utils.hpp"

namespace {
using namespace chunkdb;
void IdentityPagesIncludeRetainedDeletions() {
    test::ScopedTempDir directory("chunkdb-identity-scan");
    auto config = txn_test::Config(directory.path(), DurabilityMode::kFsyncWal, 8U);
    config.background_maintenance = false;
    config.checkpoint_update_interval = 1000U;
    std::uint64_t deleted_revision = 0U;
    {
        ChunkStore store(config);
        (void)store.ReadChunkState(9, 9);
        store.SetBlockBits(-4, 0, "10000000");
        store.SetBlockBits(0, 0, "01000000");
        const auto first = store.ScanChunkIdentities(false, {}, 1U, {}, true);
        assert(first.position && first.entries.size() == 1U && first.has_more);
        assert((first.entries[0].coord == ChunkCoord{-1, 0}));
        assert(first.entries[0].data && first.entries[0].data->payload[0] == 1U);
        const auto second = store.ScanChunkIdentities(true, first.entries[0].coord, 1U);
        assert(second.entries.size() == 1U && !second.has_more && !second.entries[0].data);
        const auto before = *first.position;
        store.UnsetBlock(-4, 0);
        deleted_revision = store.ReadChunkState(-1, 0).version;
        const auto changed = store.ScanChunkIdentities(false, {}, 10U, before, true);
        assert(changed.entries.size() == 1U && changed.entries[0].version == deleted_revision);
        assert(!changed.entries[0].data && changed.entries[0].commit_time_ms != 0U);
        store.CheckpointForTests(-1, 0);
        assert(!std::filesystem::exists(ChunkDataPath(directory.path(), store.geometry(), {-1, 0})));
        assert(!std::filesystem::exists(ChunkWalPath(directory.path(), store.geometry(), {-1, 0})));
        const auto retained = store.ScanChunkIdentities(false, {}, 10U, before, true);
        assert(retained.entries.size() == 1U && !retained.entries[0].data);
        assert(retained.entries[0].version == deleted_revision);
        const auto populated = store.ScanChunkIdentities(false, {}, 10U);
        assert(populated.entries.size() == 1U && (populated.entries[0].coord == ChunkCoord{0, 0}));
        auto readonly = config; readonly.access_mode = AccessMode::kReadOnly;
        ChunkStore reader(readonly);
        const auto read_page = reader.ScanChunkIdentities(false, {}, 10U, before, true);
        assert(!read_page.position && read_page.entries.size() == 1U);
        assert(read_page.entries[0].version == deleted_revision && !read_page.entries[0].data);
    }
    ChunkStore reopened(config);
    const auto all = reopened.ScanChunkIdentities(false, {}, 1024U, 0U, true);
    assert(all.entries.size() == 2U && all.entries[0].version == deleted_revision && !all.entries[0].data);
    for (const auto limit : {0U, 1025U}) {
        bool refused = false;
        try { (void)reopened.ScanChunkIdentities(false, {}, limit); }
        catch (const std::invalid_argument&) { refused = true; }
        assert(refused);
    }
    bool refused = false;
    try { (void)reopened.ScanChunkIdentities(true, {0, 0, 1}, 10U); }
    catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}
void CandidateChangedDuringDiskRead() {
    test::ScopedTempDir directory("chunkdb-scan-changing-candidate");
    auto config = txn_test::Config(directory.path(), DurabilityMode::kRelaxed, 8U);
    config.background_maintenance = false;
    config.wal_group_commit_updates = 1000U;
    {
        ChunkStore store(config);
        store.SetBlockBits(0, 0, "10000000");
        store.CheckpointForTests(0, 0);
    }
    ChunkStore store(config);
    store.ArmCachePauseForTests(CachePausePoint::kNoCacheReadBeforeWal, {0, 0});
    auto scanning = std::async(std::launch::async, [&] {
        return store.ScanChunkIdentities(false, {}, 10U, 0U, true);
    });
    assert(store.WaitForCachePauseForTests(CachePausePoint::kNoCacheReadBeforeWal));
    store.UnsetBlock(0, 0);
    const auto deleted = store.ReadChunkState(0, 0).version;
    store.ResumeCachePauseForTests(CachePausePoint::kNoCacheReadBeforeWal);
    const auto page = scanning.get();
    assert(page.position && deleted > *page.position);
    assert(page.entries.size() == 1U && page.entries[0].version == deleted && !page.entries[0].data);
}
void DataPagesBoundRetainedCopies() {
    test::ScopedTempDir directory("chunkdb-scan-data-budget");
    auto config = txn_test::Config(directory.path(), DurabilityMode::kRelaxed, 64U);
    config.geometry.chunk_width_blocks = 1024U;
    config.geometry.chunk_height_blocks = 1024U;
    config.background_maintenance = false;
    config.checkpoint_update_interval = 1000U;
    ChunkStore store(config);
    for (std::int64_t coordinate = 0; coordinate != 8; ++coordinate)
        store.SetBlockBits(coordinate * 1024, 0, std::string(64U, '1'));
    const auto all = store.ScanChunkIdentities(false, {}, 32U);
    assert(all.entries.size() == 8U && !all.has_more);
    const auto first = store.ScanChunkIdentities(false, {}, 32U, {}, true);
    assert(first.entries.size() == 7U && first.has_more);
    assert(first.entries.back().data && first.entries.back().data->payload.size() == 8U * 1024U * 1024U);
    const auto second = store.ScanChunkIdentities(true, first.entries.back().coord, 32U, {}, true);
    assert(second.entries.size() == 1U && !second.has_more);
    test::ScopedTempDir large_directory("chunkdb-scan-single-data-budget");
    config.data_dir = large_directory.path();
    config.geometry.block_bits = 512U;
    ChunkStore large(config);
    large.SetBlockBits(0, 0, std::string(512U, '1'));
    bool refused = false;
    try { (void)large.ScanChunkIdentities(false, {}, 32U, {}, true); }
    catch (const std::out_of_range&) { refused = true; }
    assert(refused);
}
// SCAN CHUNKS with an AREA or AREA AROUND lists only the chunks inside it,
// ascending, at every page size and from any cursor, for current, SINCE and
// snapshot pages.
void AreaPagesListOnlyTheirCells() {
    for (const std::uint8_t dims : {std::uint8_t{2U}, std::uint8_t{3U}}) {
        test::ScopedTempDir directory("chunkdb-scan-area");
        auto config = txn_test::Config(directory.path(), DurabilityMode::kRelaxed, 8U);
        config.background_maintenance = false;
        config.max_loaded_chunks = 4096U;
        if (dims == 3U) {
            config.geometry.dims = 3U;
            config.geometry.chunk_depth_blocks = 4U;
            config.geometry.large_chunk_depth_chunks = 2U;
        }
        ChunkStore store(config);
        const auto write = [&](const ChunkCoord& chunk, std::string_view bits) {
            if (dims == 3U) store.SetBlockBits3D(chunk.x * 4, chunk.y * 4, chunk.z * 4, bits);
            else store.SetBlockBits(chunk.x * 4, chunk.y * 4, bits);
        };
        const std::int64_t span = dims == 3U ? 4 : 9;
        std::set<ChunkCoord> populated;
        for (std::int64_t x = -span; x <= span; ++x)
            for (std::int64_t y = -span; y <= span; ++y)
                for (std::int64_t z = dims == 3U ? -span : 0; z <= (dims == 3U ? span : 0); ++z) {
                    if (((x * 7 + y * 3 + z * 5) % 4 + 4) % 4 == 0) continue;
                    write({x, y, z}, "10000000");
                    populated.insert({x, y, z});
                }
        const std::int64_t depth = dims == 3U ? 1 : 0;
        const std::vector<ChunkAreaSelector> areas{
            {.dims = dims, .first = {-3, -2, -depth}, .last = {4, 5, 2 * depth}},
            {.dims = dims, .first = {1, -1, depth}, .around = true, .radius = 3},
            {.dims = dims, .first = {2, -span, -depth}, .last = {2, span, depth}},
            {.dims = dims, .first = {100, 100, 0}, .last = {101, 101, 0}},
        };
        const auto inside = [](const ChunkAreaSelector& area, const std::set<ChunkCoord>& coords) {
            const detail::AreaCursor cells(area);
            std::vector<ChunkCoord> kept;
            for (const auto& coord : coords)
                if (cells.Contains(coord)) kept.push_back(coord);
            return kept;
        };
        // Pages from `start` (or the beginning) until one says no more.
        const auto collect = [&](const ChunkAreaSelector& area, std::size_t limit, const ChunkCoord* start,
                                 std::optional<std::uint64_t> since, const TxnSnapshot* snapshot) {
            std::vector<ChunkCoord> coords;
            bool has_cursor = start != nullptr;
            ChunkCoord cursor = start != nullptr ? *start : ChunkCoord{};
            for (;;) {
                const auto page = snapshot != nullptr
                    ? store.ScanChunkIdentitiesAt(*snapshot, has_cursor, cursor, limit, false, nullptr, &area)
                    : store.ScanChunkIdentities(has_cursor, cursor, limit, since, false, nullptr, &area);
                for (const auto& entry : page.entries) coords.push_back(entry.coord);
                if (!page.has_more) return coords;
                has_cursor = true;
                cursor = page.entries.back().coord;
            }
        };
        const ChunkCoord before_all{-1000, 0, 0}, after_all{1000, 0, 0};
        for (const auto& area : areas) {
            const auto expected = inside(area, populated);
            for (const std::size_t limit : {std::size_t{1U}, std::size_t{4U}, kMaxChunkScanLimit}) {
                assert(collect(area, limit, nullptr, std::nullopt, nullptr) == expected);
                assert(collect(area, limit, &before_all, std::nullopt, nullptr) == expected);
            }
            assert(collect(area, 4U, &after_all, std::nullopt, nullptr).empty());
            if (!expected.empty()) {
                const auto rest = collect(area, 3U, &expected.front(), std::nullopt, nullptr);
                assert(std::equal(rest.begin(), rest.end(), expected.begin() + 1, expected.end()));
            }
        }

        const auto position = store.ScanChunkIdentities(false, {}, 1U).position;
        assert(position);
        const auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(60));
        std::set<ChunkCoord> changed;
        for (const auto& coord : populated) {
            if (((coord.x + coord.y + coord.z) % 3 + 3) % 3 != 0) continue;
            write(coord, "01000000");
            changed.insert(coord);
        }
        // A chunk emptied after the snapshot: SINCE lists its deletion, the
        // snapshot still lists it, current pages no longer do.
        const ChunkCoord emptied = inside(areas[0], populated).front();
        if (dims == 3U) store.UnsetBlock3D(emptied.x * 4, emptied.y * 4, emptied.z * 4);
        else store.UnsetBlock(emptied.x * 4, emptied.y * 4);
        changed.insert(emptied);
        auto now = populated;
        now.erase(emptied);
        for (const auto& area : areas) {
            for (const std::size_t limit : {std::size_t{1U}, std::size_t{5U}, kMaxChunkScanLimit}) {
                assert(collect(area, limit, nullptr, *position, nullptr) == inside(area, changed));
                assert(collect(area, limit, nullptr, std::nullopt, snapshot.get()) == inside(area, populated));
                assert(collect(area, limit, nullptr, std::nullopt, nullptr) == inside(area, now));
            }
        }
        store.EndTxnSnapshot(*snapshot);

        bool refused = false;
        ChunkAreaSelector other{.dims = static_cast<std::uint8_t>(dims == 3U ? 2U : 3U)};
        try { (void)store.ScanChunkIdentities(false, {}, 4U, {}, false, nullptr, &other); }
        catch (const std::invalid_argument&) { refused = true; }
        assert(refused);
    }
}
}
int main() {
    IdentityPagesIncludeRetainedDeletions();
    CandidateChangedDuringDiskRead();
    DataPagesBoundRetainedCopies();
    AreaPagesListOnlyTheirCells();
}
