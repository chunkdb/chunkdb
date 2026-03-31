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
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
    };
}

std::size_t HysteresisTarget(std::size_t lower_bound) {
    if (lower_bound <= 1) {
        return lower_bound;
    }
    return lower_bound + std::max<std::size_t>(1, lower_bound / 2);
}

void TestUpdateIntervalCheckpointCycles() {
    const auto data_dir = TempDataDir("interval");
    auto config = BaseConfig(data_dir);
    config.checkpoint_update_interval = 40;
    config.checkpoint_wal_bytes = 1'000'000;
    const auto upper_threshold = HysteresisTarget(config.checkpoint_update_interval);

    std::string expected = "00000000";

    {
        chunkdb::ChunkStore store(config);
        const auto coord = store.geometry().BlockToChunk(0, 0);
        const auto geometry = store.geometry();

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        const auto chk_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
        std::uint32_t seq = 1;
        for (std::size_t update = 1; update < config.checkpoint_update_interval; ++update) {
            expected = MakeBits(seq++);
            store.SetBlockBits(0, 0, expected);
            assert(std::filesystem::exists(wal_path));
            assert(!std::filesystem::exists(chk_path));
        }

        expected = MakeBits(seq++);
        store.SetBlockBits(0, 0, expected);
        assert(std::filesystem::exists(wal_path));
        assert(!std::filesystem::exists(chk_path));

        for (std::size_t update = config.checkpoint_update_interval + 1; update < upper_threshold; ++update) {
            expected = MakeBits(seq++);
            store.SetBlockBits(0, 0, expected);
            assert(std::filesystem::exists(wal_path));
            assert(!std::filesystem::exists(chk_path));
        }

        expected = MakeBits(seq++);
        store.SetBlockBits(0, 0, expected);
        assert(std::filesystem::exists(chk_path));
        assert(!std::filesystem::exists(wal_path));
    }

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
    config.checkpoint_wal_bytes = 1'000;
    const auto upper_threshold = HysteresisTarget(config.checkpoint_wal_bytes);

    std::string expected = "00000000";

    {
        chunkdb::ChunkStore store(config);
        const auto coord = store.geometry().BlockToChunk(0, 0);
        const auto geometry = store.geometry();

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        const auto chk_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
        bool saw_lower_threshold = false;
        bool saw_forced_checkpoint = false;
        std::uintmax_t last_wal_size = 0;

        for (std::uint32_t seq = 1; seq <= 1600; ++seq) {
            expected = MakeBits(seq);
            store.SetBlockBits(0, 0, expected);

            const bool wal_exists = std::filesystem::exists(wal_path);
            if (!wal_exists) {
                assert(saw_lower_threshold);
                assert(std::filesystem::exists(chk_path));
                saw_forced_checkpoint = true;
                break;
            }

            const auto wal_size = std::filesystem::file_size(wal_path);
            assert(wal_size >= last_wal_size);
            assert(!std::filesystem::exists(chk_path));
            last_wal_size = wal_size;

            if (!saw_lower_threshold && wal_size >= config.checkpoint_wal_bytes) {
                saw_lower_threshold = true;
                assert(wal_size < upper_threshold);
            }

            if (saw_lower_threshold) {
                assert(std::filesystem::exists(wal_path));
                assert(!std::filesystem::exists(chk_path));
            }
        }

        assert(saw_lower_threshold);
        assert(saw_forced_checkpoint);
    }

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
