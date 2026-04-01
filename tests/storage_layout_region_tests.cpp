#include <cassert>
#include <filesystem>
#include <string>

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
            store.SetBlockBits(2, 3, "01010101");
            store.SetBlockBits(3, 3, "00110011");
            store.SetBlockBits(4, 3, "00000000");
        }

        assert(CountFilesWithExtension(data_dir, ".wal") > 0);

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(2, 3) == "01010101");
            assert(recovered.GetBlockBits(3, 3) == "00110011");
            assert(recovered.BlockExists(4, 3));
            assert(recovered.GetBlockBits(4, 3) == "00000000");
        }
    }

    std::filesystem::remove_all(data_dir);
    return 0;
}
