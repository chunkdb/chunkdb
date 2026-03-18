#include <cassert>
#include <filesystem>
#include <string>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace {

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-cache-test-" + std::to_string(tick));
}

}  // namespace

int main() {
    const auto data_dir = TempDataDir();

    chunkdb::StoreConfig config{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 4,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 2,
        .checkpoint_wal_bytes = 128,
        .max_loaded_chunks = 3,
        .allow_multiple_processes = false,
    };

    std::vector<std::pair<int, int>> written_coords;
    written_coords.reserve(24);

    {
        chunkdb::ChunkStore store(config);

        for (int i = 0; i < 24; ++i) {
            const int bx = i * 4;
            const int by = 0;
            store.SetBlockBits(bx, by, (i % 2 == 0) ? "1010" : "0101");
            written_coords.emplace_back(bx, by);
        }

        assert(store.ApproxLoadedChunkCount() <= config.max_loaded_chunks);
    }

    {
        auto aggressive = config;
        aggressive.data_dir = TempDataDir();
        aggressive.max_loaded_chunks = 8;
        aggressive.checkpoint_update_interval = 10'000;
        aggressive.checkpoint_wal_bytes = 10'000'000;

        chunkdb::ChunkStore store(aggressive);
        for (int i = 0; i < 200; ++i) {
            store.SetBlockBits(i * 8, 0, (i % 2 == 0) ? "1111" : "0001");
            assert(store.ApproxLoadedChunkCount() <= aggressive.max_loaded_chunks + 2);
        }

        assert(store.EvictionSnapshotBuildCountForTests() < 30);
        std::filesystem::remove_all(aggressive.data_dir);
    }

    {
        chunkdb::ChunkStore reopened(config);
        for (int i = 0; i < static_cast<int>(written_coords.size()); ++i) {
            const auto [bx, by] = written_coords[i];
            const std::string expected = (i % 2 == 0) ? "1010" : "0101";
            assert(reopened.GetBlockBits(bx, by) == expected);
        }
    }

    std::filesystem::remove_all(data_dir);
    return 0;
}
