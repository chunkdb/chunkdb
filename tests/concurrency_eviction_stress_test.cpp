#include <atomic>
#include <cassert>
#include <filesystem>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace {

struct BlockCoord {
    int x = 0;
    int y = 0;

    bool operator==(const BlockCoord& other) const noexcept {
        return x == other.x && y == other.y;
    }
};

struct BlockCoordHash {
    std::size_t operator()(const BlockCoord& c) const noexcept {
        const std::size_t h1 = std::hash<int>{}(c.x);
        const std::size_t h2 = std::hash<int>{}(c.y);
        return h1 ^ (h2 << 1U);
    }
};

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-concurrency-evict-stress-" + std::to_string(tick));
}

std::string MakeBits(std::uint32_t v) {
    std::string bits(8, '0');
    for (int i = 0; i < 8; ++i) {
        bits[i] = ((v >> i) & 1U) != 0U ? '1' : '0';
    }
    return bits;
}

}  // namespace

int main() {
    const auto data_dir = TempDataDir();

    constexpr int kThreadCount = 12;
    constexpr int kOpsPerThread = 2500;
    constexpr std::size_t kMaxLoadedChunks = 16;

    chunkdb::StoreConfig config{
        .geometry = {
            .large_chunk_width_chunks = 4,
            .large_chunk_height_chunks = 4,
            .chunk_width_blocks = 8,
            .chunk_height_blocks = 8,
            .block_bits = 8,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 128,
        .checkpoint_wal_bytes = 16 * 1024,
        .max_loaded_chunks = kMaxLoadedChunks,
        .allow_multiple_processes = false,
    };

    std::vector<BlockCoord> block_pool;
    block_pool.reserve(600);
    for (int i = 0; i < 600; ++i) {
        const int chunk_x = i - 300;
        const int chunk_y = (i % 37) - 18;
        block_pool.push_back(BlockCoord{chunk_x * 8, chunk_y * 8});
    }

    std::unordered_map<BlockCoord, std::string, BlockCoordHash> expected;
    std::mutex expected_mutex;
    std::atomic<bool> start{false};

    {
        chunkdb::ChunkStore store(config);

        std::vector<std::thread> workers;
        workers.reserve(kThreadCount);

        for (int tid = 0; tid < kThreadCount; ++tid) {
            workers.emplace_back([&, tid]() {
                std::mt19937 rng(static_cast<std::uint32_t>(0xC001D00D + tid * 31));
                std::uniform_int_distribution<int> pick(0, static_cast<int>(block_pool.size() - 1));

                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }

                for (int i = 0; i < kOpsPerThread; ++i) {
                    const BlockCoord coord = block_pool[static_cast<std::size_t>(pick(rng))];

                    if ((i % 4) == 0) {
                        (void)store.GetBlockBits(coord.x, coord.y);
                        continue;
                    }

                    const std::string bits = MakeBits(static_cast<std::uint32_t>(tid * 1315423911U + i));
                    store.SetBlockBits(coord.x, coord.y, bits);

                    std::lock_guard lock(expected_mutex);
                    expected[coord] = bits;
                }
            });
        }

        start.store(true, std::memory_order_release);
        for (auto& worker : workers) {
            worker.join();
        }

        for (const auto& [coord, bits] : expected) {
            assert(store.GetBlockBits(coord.x, coord.y) == bits);
        }

        assert(store.ApproxLoadedChunkCount() <= kMaxLoadedChunks + 8);
    }

    {
        chunkdb::ChunkStore reloaded(config);
        std::lock_guard lock(expected_mutex);
        for (const auto& [coord, bits] : expected) {
            assert(reloaded.GetBlockBits(coord.x, coord.y) == bits);
        }
    }

    std::filesystem::remove_all(data_dir);
    return 0;
}
