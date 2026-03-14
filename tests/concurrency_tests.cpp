#include <cassert>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace {

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto wall_tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    const auto mono_tick = static_cast<unsigned long long>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const auto tid = static_cast<unsigned long long>(
        std::hash<std::thread::id>{}(std::this_thread::get_id()));
    return base / (
        "chunkdb-concurrency-test-" + std::to_string(wall_tick) + "-" +
        std::to_string(mono_tick) + "-" + std::to_string(tid));
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

std::unique_ptr<chunkdb::ChunkStore> OpenStoreWithRetry(const chunkdb::StoreConfig& config) {
    for (int attempt = 0; attempt < 25; ++attempt) {
        try {
            return std::make_unique<chunkdb::ChunkStore>(config);
        } catch (const std::runtime_error& e) {
            const std::string_view message(e.what());
            if (message.find("active writer") == std::string_view::npos || attempt + 1 == 25) {
                throw;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
    return std::make_unique<chunkdb::ChunkStore>(config);
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
        auto store = OpenStoreWithRetry(config);

        std::vector<std::thread> workers;
        workers.reserve(thread_count);

        for (int tid = 0; tid < thread_count; ++tid) {
            workers.emplace_back([&store, tid]() {
                for (int i = 0; i < updates_per_thread; ++i) {
                    const int x = tid;
                    const int y = i;
                    const std::string bits = ((i + tid) % 2 == 0) ? "10101010" : "01010101";
                    store->SetBlockBits(x, y, bits);
                }
            });
        }

        for (auto& worker : workers) {
            worker.join();
        }

        for (int tid = 0; tid < thread_count; ++tid) {
            for (int i = 0; i < updates_per_thread; ++i) {
                const std::string expected = ((i + tid) % 2 == 0) ? "10101010" : "01010101";
                assert(store->GetBlockBits(tid, i) == expected);
            }
        }
    }

    {
        auto reloaded = OpenStoreWithRetry(config);
        for (int tid = 0; tid < thread_count; ++tid) {
            for (int i = 0; i < updates_per_thread; ++i) {
                const std::string expected = ((i + tid) % 2 == 0) ? "10101010" : "01010101";
                assert(reloaded->GetBlockBits(tid, i) == expected);
            }
        }
    }

    RemoveAllWithRetry(data_dir);
    return 0;
}
