#include <atomic>
#include <chrono>
#include <cassert>
#include <filesystem>
#include <exception>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace {

struct BlockCoord {
    int x = 0;
    int y = 0;
};

struct ThreadState {
    BlockCoord hot_coord;
    std::string hot_bits = "00000000";
    std::vector<BlockCoord> cold_coords;
    std::vector<std::string> cold_bits;
};

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-hot-contention-evict-cycle-" + std::to_string(tick));
}

void RemoveAllWithRetry(const std::filesystem::path& dir) {
    for (int attempt = 0; attempt < 25; ++attempt) {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        if (!std::filesystem::exists(dir)) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

std::string MakeBits(std::uint32_t v) {
    std::string bits(8, '0');
    for (int i = 0; i < 8; ++i) {
        bits[i] = ((v >> i) & 1U) != 0U ? '1' : '0';
    }
    return bits;
}

void VerifyState(
    chunkdb::ChunkStore* store,
    const std::vector<ThreadState>& states) {
    assert(store != nullptr);

    for (const auto& state : states) {
        assert(store->GetBlockBits(state.hot_coord.x, state.hot_coord.y) == state.hot_bits);
        for (std::size_t i = 0; i < state.cold_coords.size(); ++i) {
            assert(store->GetBlockBits(state.cold_coords[i].x, state.cold_coords[i].y) == state.cold_bits[i]);
        }
    }
}

}  // namespace

int main() {
    try {
    const auto data_dir = TempDataDir();

    constexpr int kCycles = 6;
    constexpr int kThreadCount = 8;
    constexpr int kOpsPerThread = 1200;
    constexpr int kColdCoordsPerThread = 32;
    constexpr std::size_t kMaxLoadedChunks = 12;
#ifdef _WIN32
    constexpr std::size_t kLoadedChunkAssertSlack = 128;
#else
    constexpr std::size_t kLoadedChunkAssertSlack = 16;
#endif

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
        .checkpoint_wal_bytes = 8 * 1024,
        .max_loaded_chunks = kMaxLoadedChunks,
        .allow_multiple_processes = false,
    };

    std::vector<ThreadState> states(static_cast<std::size_t>(kThreadCount));
    for (int tid = 0; tid < kThreadCount; ++tid) {
        ThreadState state;
        state.hot_coord = BlockCoord{
            .x = tid % 8,
            .y = tid / 8,
        };

        state.cold_coords.reserve(kColdCoordsPerThread);
        state.cold_bits.assign(static_cast<std::size_t>(kColdCoordsPerThread), "00000000");

        for (int i = 0; i < kColdCoordsPerThread; ++i) {
            const int chunk_x = (tid + 1) * 200 + i * 3;
            const int chunk_y = -((tid + 1) * 150 + i * 5);
            state.cold_coords.push_back(BlockCoord{
                .x = chunk_x * 8,
                .y = chunk_y * 8,
            });
        }

        states[static_cast<std::size_t>(tid)] = std::move(state);
    }

    for (int cycle = 0; cycle < kCycles; ++cycle) {
        {
            chunkdb::ChunkStore store(config);
            std::atomic<bool> start{false};
            std::vector<std::thread> workers;
            workers.reserve(kThreadCount);

            for (int tid = 0; tid < kThreadCount; ++tid) {
                workers.emplace_back([&, tid]() {
                    std::mt19937 rng(
                        static_cast<std::uint32_t>(0xBAD50000U + cycle * 131U + tid * 17U));
                    std::uniform_int_distribution<int> cold_pick(0, kColdCoordsPerThread - 1);

                    ThreadState& state = states[static_cast<std::size_t>(tid)];

                    while (!start.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }

                    for (int op = 0; op < kOpsPerThread; ++op) {
                        if ((op % 6) == 0) {
                            (void)store.GetBlockBits(state.hot_coord.x, state.hot_coord.y);
                            const int idx = cold_pick(rng);
                            (void)store.GetBlockBits(state.cold_coords[static_cast<std::size_t>(idx)].x,
                                                     state.cold_coords[static_cast<std::size_t>(idx)].y);
                        } else if ((op % 2) == 0) {
                            const auto bits = MakeBits(
                                static_cast<std::uint32_t>((cycle + 1) * 0x1F1FU + tid * 257U + op));
                            store.SetBlockBits(state.hot_coord.x, state.hot_coord.y, bits);
                            state.hot_bits = bits;
                        } else {
                            const int idx = cold_pick(rng);
                            const auto bits = MakeBits(static_cast<std::uint32_t>(
                                0xA5A50000U + cycle * 37U + tid * 19U + op + idx));
                            store.SetBlockBits(
                                state.cold_coords[static_cast<std::size_t>(idx)].x,
                                state.cold_coords[static_cast<std::size_t>(idx)].y,
                                bits);
                            state.cold_bits[static_cast<std::size_t>(idx)] = bits;
                        }

                        // Force repeated loading of distant chunks to pressure eviction under contention.
                        if ((op % 11) == 0) {
                            const int spill_x = ((tid + 1) * 10000 + op) * 8;
                            const int spill_y = -((tid + 1) * 7000 + op * 3) * 8;
                            (void)store.GetBlockBits(spill_x, spill_y);
                        }
                    }
                });
            }

            start.store(true, std::memory_order_release);
            for (auto& worker : workers) {
                worker.join();
            }

            VerifyState(&store, states);
            assert(store.ApproxLoadedChunkCount() <= kMaxLoadedChunks + kLoadedChunkAssertSlack);
        }

        // Repeat load/unload cycle and re-validate persisted correctness.
        {
            chunkdb::ChunkStore reloaded(config);
            VerifyState(&reloaded, states);
            assert(reloaded.ApproxLoadedChunkCount() <= kMaxLoadedChunks + kLoadedChunkAssertSlack);
        }
    }

    RemoveAllWithRetry(data_dir);
    return 0;
    } catch (const std::exception& e) {
        std::cerr << "stress test failed: " << e.what() << std::endl;
        return 1;
    }
}
