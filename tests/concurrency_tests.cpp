#include <cassert>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace {

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-concurrency-test-" + std::to_string(tick));
}

}  // namespace

int main() {
    const auto data_dir = TempDataDir();

    chunkdb::StoreConfig config{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 16,
            .chunk_height_blocks = 16,
            .block_bits = 8,
        },
        .data_dir = data_dir,
    };

    constexpr int thread_count = 8;
    constexpr int updates_per_thread = 128;

    {
        chunkdb::ChunkStore store(config);

        std::vector<std::thread> workers;
        workers.reserve(thread_count);

        for (int tid = 0; tid < thread_count; ++tid) {
            workers.emplace_back([&store, tid]() {
                for (int i = 0; i < updates_per_thread; ++i) {
                    const int x = tid;
                    const int y = i;
                    const std::string bits = ((i + tid) % 2 == 0) ? "10101010" : "01010101";
                    store.SetBlockBits(x, y, bits);
                }
            });
        }

        for (auto& worker : workers) {
            worker.join();
        }

        for (int tid = 0; tid < thread_count; ++tid) {
            for (int i = 0; i < updates_per_thread; ++i) {
                const std::string expected = ((i + tid) % 2 == 0) ? "10101010" : "01010101";
                assert(store.GetBlockBits(tid, i) == expected);
            }
        }
    }

    {
        chunkdb::ChunkStore reloaded(config);
        for (int tid = 0; tid < thread_count; ++tid) {
            for (int i = 0; i < updates_per_thread; ++i) {
                const std::string expected = ((i + tid) % 2 == 0) ? "10101010" : "01010101";
                assert(reloaded.GetBlockBits(tid, i) == expected);
            }
        }
    }

    std::filesystem::remove_all(data_dir);
    return 0;
}
