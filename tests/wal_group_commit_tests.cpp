#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"

namespace {

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-wal-group-commit-" + suffix + "-" + std::to_string(tick));
}

chunkdb::StoreConfig BaseConfig(const std::filesystem::path& data_dir) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 8,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 10'000,
        .checkpoint_wal_bytes = 10'000'000,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
        .wal_group_commit_updates = 4,
    };
}

std::string MakeBits(std::uint32_t v) {
    std::string bits(8, '0');
    for (int i = 0; i < 8; ++i) {
        bits[i] = ((v >> i) & 1U) != 0U ? '1' : '0';
    }
    return bits;
}

void TestRelaxedGroupCommitThreshold() {
    const auto data_dir = TempDataDir("threshold");
    auto config = BaseConfig(data_dir);

    chunkdb::ChunkCoord coord;
    chunkdb::Geometry geometry(config.geometry);

    {
        chunkdb::ChunkStore store(config);
        coord = store.geometry().BlockToChunk(0, 0);
        geometry = store.geometry();

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);

        store.SetBlockBits(0, 0, MakeBits(1));
        store.SetBlockBits(0, 0, MakeBits(2));
        store.SetBlockBits(0, 0, MakeBits(3));

        // Before threshold flush, relaxed-mode group commit should keep WAL unsynced in memory.
        assert(!std::filesystem::exists(wal_path));

        store.SetBlockBits(0, 0, MakeBits(4));
        assert(std::filesystem::exists(wal_path));
        assert(std::filesystem::file_size(wal_path) > 0);
    }

    std::filesystem::remove_all(data_dir);
}

void TestGroupCommitFlushOnCleanShutdown() {
    const auto data_dir = TempDataDir("shutdown");
    auto config = BaseConfig(data_dir);
    config.wal_group_commit_updates = 64;

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101010");
        store.SetBlockBits(1, 0, "01010101");
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "10101010");
        assert(recovered.GetBlockBits(1, 0) == "01010101");
    }

    std::filesystem::remove_all(data_dir);
}

}  // namespace

int main() {
    TestRelaxedGroupCommitThreshold();
    TestGroupCommitFlushOnCleanShutdown();
    return 0;
}
