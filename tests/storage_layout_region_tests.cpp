#include <algorithm>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace {

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-storage-layout-region-test-" + std::to_string(tick));
}

std::size_t CountFilesWithExtension(const std::filesystem::path& root, const std::string& ext) {
    if (!std::filesystem::exists(root)) {
        return 0;
    }
    std::size_t count = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() == ext) {
            ++count;
        }
    }
    return count;
}

chunkdb::StoreConfig BaseConfig(const std::filesystem::path& data_dir) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 8,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = 16,
            .chunk_height_blocks = 16,
            .block_bits = 8,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 1,
        .checkpoint_wal_bytes = 1024,
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 256,
        .allow_multiple_processes = false,
        .storage_layout_mode = chunkdb::StorageLayoutMode::kFsRegionV1Experimental,
        .experimental_region_span_chunks = 16,
    };
}

}  // namespace

int main() {
    const auto data_dir = TempDataDir();

    // Checkpoint persistence in region image files.
    {
        auto config = BaseConfig(data_dir);
        config.checkpoint_update_interval = 1;
        config.checkpoint_wal_bytes = 1;

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "10101010");
            store.SetBlockBits(31, 31, "11110000");
            store.SetBlockBits(-1, -1, "00001111");
            store.SetBlockBits(1, 1, "00000000");
            store.UnsetBlock(1, 1);
        }

        {
            chunkdb::ChunkStore store(config);
            assert(store.BlockExists(0, 0));
            assert(store.GetBlockBits(0, 0) == "10101010");
            assert(store.BlockExists(31, 31));
            assert(store.GetBlockBits(31, 31) == "11110000");
            assert(store.BlockExists(-1, -1));
            assert(store.GetBlockBits(-1, -1) == "00001111");
            assert(!store.BlockExists(1, 1));
            assert(store.GetBlockBits(1, 1) == "00000000");
        }

        assert(CountFilesWithExtension(data_dir, ".rgn") > 0);
        assert(CountFilesWithExtension(data_dir, ".chk") == 0);
    }

    // WAL replay path should still work with region image layout.
    {
        auto config = BaseConfig(data_dir);
        config.checkpoint_update_interval = 1'000'000;
        config.checkpoint_wal_bytes = 1'000'000;

        {
            chunkdb::ChunkStore store(config);
            store.SetChunkBits(1, 0, std::string(store.geometry().ChunkPayloadBits(), '0'));
            store.SetBlockBits(2, 3, "01010101");
            store.SetBlockBits(3, 3, "00110011");
            store.SetBlockBits(4, 3, "00000000");
        }

        assert(CountFilesWithExtension(data_dir, ".wal") > 0);

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.ChunkExists(1, 0));
            assert(recovered.GetBlockBits(16, 0) == "00000000");
            assert(recovered.GetBlockBits(2, 3) == "01010101");
            assert(recovered.GetBlockBits(3, 3) == "00110011");
            assert(recovered.BlockExists(4, 3));
            assert(recovered.GetBlockBits(4, 3) == "00000000");
        }
    }

    // CHUNKSCAN over the region layout. The region walk itself is still a
    // full pass over every .rgn file, but the cache merge is now scoped to the
    // large chunks the page visits, so this pins down that scoping: the same
    // world must enumerate identically with a cold cache and with a fully
    // resident one, in ascending (cx, cy) order and with an exact cursor.
    {
        const auto scan_dir = data_dir.string() + "-scan";
        auto config = BaseConfig(scan_dir);
        config.checkpoint_update_interval = 1;
        config.checkpoint_wal_bytes = 1;

        std::vector<std::pair<std::int64_t, std::int64_t>> populated;
        for (std::int64_t cx = -3; cx <= 3; ++cx) {
            for (std::int64_t cy = -3; cy <= 3; ++cy) {
                if (((cx + 2 * cy) & 3) == 0) {
                    continue;  // leave holes inside the large chunks
                }
                populated.emplace_back(cx, cy);
            }
        }
        std::sort(populated.begin(), populated.end());

        {
            config.max_loaded_chunks = 4096;
            chunkdb::ChunkStore store(config);
            for (const auto& [cx, cy] : populated) {
                store.SetBlockBits(cx * 16, cy * 16, "10001001");
            }
            store.WalBarrier();
        }

        const auto enumerate = [&](std::size_t cache_chunks, bool warm, std::size_t limit) {
            auto scan_config = config;
            scan_config.max_loaded_chunks = cache_chunks;
            chunkdb::ChunkStore store(scan_config);
            if (warm) {
                for (const auto& [cx, cy] : populated) {
                    (void)store.GetChunkVersion(cx, cy);
                }
                assert(store.ApproxLoadedChunkCount() == populated.size());
            }
            std::vector<std::pair<std::int64_t, std::int64_t>> seen;
            bool has_cursor = false;
            chunkdb::ChunkCoord cursor{};
            while (true) {
                const auto page = store.ScanPopulatedChunks(has_cursor, cursor, limit);
                for (const auto& coord : page.coords) {
                    if (!seen.empty()) {
                        assert(seen.back() < std::make_pair(coord.x, coord.y));
                    }
                    seen.emplace_back(coord.x, coord.y);
                }
                if (!page.has_more) {
                    break;
                }
                has_cursor = true;
                cursor = page.coords.back();
            }
            return seen;
        };

        assert(enumerate(8, false, 5) == populated);
        assert(enumerate(4096, true, 5) == populated);
        assert(enumerate(4096, true, 1) == populated);

        // A mid-world cursor returns the same page whatever the cache holds.
        const auto page_at = [&](std::size_t cache_chunks, bool warm) {
            auto scan_config = config;
            scan_config.max_loaded_chunks = cache_chunks;
            chunkdb::ChunkStore store(scan_config);
            if (warm) {
                for (const auto& [cx, cy] : populated) {
                    (void)store.GetChunkVersion(cx, cy);
                }
            }
            return store.ScanPopulatedChunks(true, {0, 0}, 4);
        };
        const auto cold_page = page_at(8, false);
        const auto warm_page = page_at(4096, true);
        assert(cold_page.coords.size() == 4);
        assert(cold_page.has_more == warm_page.has_more);
        assert(cold_page.coords.size() == warm_page.coords.size());
        for (std::size_t i = 0; i < cold_page.coords.size(); ++i) {
            assert(cold_page.coords[i].x == warm_page.coords[i].x);
            assert(cold_page.coords[i].y == warm_page.coords[i].y);
        }

        std::filesystem::remove_all(scan_dir);
    }

    std::filesystem::remove_all(data_dir);
    return 0;
}
