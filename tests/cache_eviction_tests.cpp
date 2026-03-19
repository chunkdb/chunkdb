#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace {

std::uint64_t CurrentPid() {
#ifdef _WIN32
    return static_cast<std::uint64_t>(::GetCurrentProcessId());
#else
    return static_cast<std::uint64_t>(::getpid());
#endif
}

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    static std::atomic<std::uint64_t> counter{0};
    const std::uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);
    return base / ("chunkdb-cache-test-" + std::to_string(tick) + "-" + std::to_string(CurrentPid()) +
                   "-" + std::to_string(seq));
}

bool RemoveAllWithRetry(const std::filesystem::path& path) {
    constexpr int kAttempts = 20;
    constexpr auto kSleep = std::chrono::milliseconds(25);

    std::error_code exists_ec;
    if (!std::filesystem::exists(path, exists_ec) || exists_ec) {
        return true;
    }

    for (int attempt = 0; attempt < kAttempts; ++attempt) {
        std::error_code remove_ec;
        std::filesystem::remove_all(path, remove_ec);
        if (!remove_ec) {
            return true;
        }
        std::this_thread::sleep_for(kSleep);
    }

    std::error_code final_ec;
    return !std::filesystem::exists(path, final_ec) && !final_ec;
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
        const auto aggressive_data_dir = TempDataDir();
        aggressive.data_dir = aggressive_data_dir;
        aggressive.max_loaded_chunks = 8;
        aggressive.checkpoint_update_interval = 10'000;
        aggressive.checkpoint_wal_bytes = 10'000'000;

        {
            chunkdb::ChunkStore store(aggressive);
            for (int i = 0; i < 200; ++i) {
                store.SetBlockBits(i * 8, 0, (i % 2 == 0) ? "1111" : "0001");
                assert(store.ApproxLoadedChunkCount() <= aggressive.max_loaded_chunks + 2);
            }

            assert(store.EvictionSnapshotBuildCountForTests() < 30);
        }

        if (!RemoveAllWithRetry(aggressive_data_dir)) {
            throw std::runtime_error(
                "failed to remove aggressive cache-eviction temp dir: " + aggressive_data_dir.string());
        }
    }

    {
        chunkdb::ChunkStore reopened(config);
        for (int i = 0; i < static_cast<int>(written_coords.size()); ++i) {
            const auto [bx, by] = written_coords[i];
            const std::string expected = (i % 2 == 0) ? "1010" : "0101";
            assert(reopened.GetBlockBits(bx, by) == expected);
        }
    }

    if (!RemoveAllWithRetry(data_dir)) {
        throw std::runtime_error("failed to remove cache-eviction temp dir: " + data_dir.string());
    }
    return 0;
}
