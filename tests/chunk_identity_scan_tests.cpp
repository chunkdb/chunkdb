#include <cassert>
#include <future>

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
}
int main() {
    IdentityPagesIncludeRetainedDeletions();
    CandidateChangedDuringDiskRead();
    DataPagesBoundRetainedCopies();
}
