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
    return base / ("chunkdb-wal-checkpoint-cycle-" + suffix + "-" + std::to_string(tick));
}

std::string MakeBits(std::uint32_t v) {
    std::string bits(8, '0');
    for (int i = 0; i < 8; ++i) {
        bits[i] = ((v >> i) & 1U) != 0U ? '1' : '0';
    }
    return bits;
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
        .checkpoint_update_interval = 256,
        .checkpoint_wal_bytes = 1'000'000,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
    };
}

void TestUpdateIntervalCheckpointCycles() {
    const auto data_dir = TempDataDir("interval");
    auto config = BaseConfig(data_dir);
    config.checkpoint_update_interval = 40;
    config.checkpoint_wal_bytes = 1'000'000;

    std::string expected = "00000000";

    chunkdb::ChunkCoord coord;
    chunkdb::Geometry geometry(config.geometry);

    int checkpoint_events = 0;

    {
        chunkdb::ChunkStore store(config);
        coord = store.geometry().BlockToChunk(0, 0);
        geometry = store.geometry();

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        const auto chk_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);

        constexpr int kCycles = 8;
        constexpr int kUpdatesBeforeThreshold = 39;
        std::uint32_t seq = 1;

        for (int cycle = 0; cycle < kCycles; ++cycle) {
            std::uintmax_t last_wal_size = 0;
            for (int i = 0; i < kUpdatesBeforeThreshold; ++i) {
                expected = MakeBits(seq++);
                store.SetBlockBits(0, 0, expected);

                assert(std::filesystem::exists(wal_path));
                const auto wal_size = std::filesystem::file_size(wal_path);
                assert(wal_size >= last_wal_size);
                last_wal_size = wal_size;
            }

            // Update #40 triggers interval checkpoint.
            expected = MakeBits(seq++);
            store.SetBlockBits(0, 0, expected);
            assert(std::filesystem::exists(chk_path));
            assert(!std::filesystem::exists(wal_path));
            ++checkpoint_events;

        }
    }

    assert(checkpoint_events == 8);

    {
        chunkdb::ChunkStore reloaded(config);
        assert(reloaded.GetBlockBits(0, 0) == expected);
    }

    std::filesystem::remove_all(data_dir);
}

void TestWalBytesCheckpointCycles() {
    const auto data_dir = TempDataDir("bytes");
    auto config = BaseConfig(data_dir);
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 240;

    std::string expected = "00000000";

    chunkdb::ChunkCoord coord;
    chunkdb::Geometry geometry(config.geometry);

    int checkpoint_events = 0;
    int wal_growth_events = 0;

    {
        chunkdb::ChunkStore store(config);
        coord = store.geometry().BlockToChunk(0, 0);
        geometry = store.geometry();

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        const auto chk_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);

        bool previous_wal_exists = false;
        std::uintmax_t previous_wal_size = 0;

        for (std::uint32_t seq = 1; seq <= 1600; ++seq) {
            expected = MakeBits(seq);
            store.SetBlockBits(0, 0, expected);

            const bool wal_exists = std::filesystem::exists(wal_path);
            if (!wal_exists) {
                assert(std::filesystem::exists(chk_path));
                ++checkpoint_events;
                previous_wal_exists = false;
                previous_wal_size = 0;
                continue;
            }

            const auto wal_size = std::filesystem::file_size(wal_path);
            if (previous_wal_exists && wal_size > previous_wal_size) {
                ++wal_growth_events;
            }
            previous_wal_exists = true;
            previous_wal_size = wal_size;
        }
    }

    assert(checkpoint_events > 0);
    assert(wal_growth_events > 0);

    {
        chunkdb::ChunkStore reloaded(config);
        assert(reloaded.GetBlockBits(0, 0) == expected);
    }

    std::filesystem::remove_all(data_dir);
}

}  // namespace

int main() {
    TestUpdateIntervalCheckpointCycles();
    TestWalBytesCheckpointCycles();
    return 0;
}
