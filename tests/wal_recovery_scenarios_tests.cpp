#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"

namespace {

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-wal-recovery-" + suffix + "-" + std::to_string(tick));
}

chunkdb::StoreConfig BuildConfig(const std::filesystem::path& data_dir) {
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
        .checkpoint_update_interval = 1'000'000,
        .checkpoint_wal_bytes = 1'000'000,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
    };
}

void AppendBytes(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    assert(out.is_open());
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.flush();
}

void WriteBytes(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    assert(out.is_open());
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.flush();
}

}  // namespace

int main() {
    // Scenario 1: trailing truncated WAL record should be ignored during replay.
    {
        const auto data_dir = TempDataDir("truncated-record");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
            store.SetBlockBits(1, 0, "00001111");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        assert(std::filesystem::exists(wal_path));

        // Add a deliberately incomplete record tail: magic + partial header bytes.
        AppendBytes(wal_path, std::string("DLT1", 4) + std::string("\x01\x02\x03", 3));

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "11110000");
            assert(recovered.GetBlockBits(1, 0) == "00001111");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 2: truncated WAL header should be ignored when a checkpoint image exists.
    {
        const auto data_dir = TempDataDir("truncated-header");
        auto config = BuildConfig(data_dir);
        config.checkpoint_update_interval = 1;

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "10101010");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();

            const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
            assert(std::filesystem::exists(data_path));
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        WriteBytes(wal_path, "CHKW");

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "10101010");
            recovered.SetBlockBits(0, 0, "01010101");
            assert(recovered.GetBlockBits(0, 0) == "01010101");
        }

        std::filesystem::remove_all(data_dir);
    }

    return 0;
}
