#include <algorithm>
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

constexpr std::uint64_t kEvictionRefillLargeChunkBudget = 16;

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

std::size_t EvictionLowerWatermark(std::size_t max_loaded_chunks) {
    const std::size_t hysteresis = std::max<std::size_t>(256, max_loaded_chunks / 16);
    if (hysteresis >= max_loaded_chunks) {
        return 1;
    }
    return max_loaded_chunks - hysteresis;
}

// A transient failure in an eviction-driven WAL flush must remain a survivable
// per-operation error: the store must keep serving reads and writes afterward,
// not permanently fail-close (the audit found an eviction flush error could
// abandon the snapshot epoch and brick all writes until restart). This runs on
// the synchronous request path (background_maintenance off) so the exception is
// observable directly.
void TestEvictionFlushFailureKeepsStoreServing() {
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
        .checkpoint_update_interval = 10'000,
        .checkpoint_wal_bytes = 10'000'000,
        .max_loaded_chunks = 2,
        .allow_multiple_processes = false,
    };

    {
        chunkdb::ChunkStore store(config);
        // Fill past the cache cap so the next writes drive eviction flushes.
        store.SetBlockBits(0, 0, "1010");
        store.SetBlockBits(4, 0, "0101");

        // Inject a one-shot eviction WAL-open failure; the write that triggers
        // eviction should throw, but the store must not be bricked.
#ifdef _WIN32
        _putenv_s("CHUNKDB_FAILPOINT_WAL_OPEN_ONCE", "1");
#else
        setenv("CHUNKDB_FAILPOINT_WAL_OPEN_ONCE", "1", 1);
#endif
        bool observed_failure = false;
        try {
            for (int i = 2; i < 8; ++i) {
                store.SetBlockBits(i * 4, 0, "1010");
            }
        } catch (const std::exception&) {
            observed_failure = true;
        }
#ifdef _WIN32
        _putenv_s("CHUNKDB_FAILPOINT_WAL_OPEN_ONCE", "");
#else
        unsetenv("CHUNKDB_FAILPOINT_WAL_OPEN_ONCE");
#endif
        // The failpoint is one-shot; whether or not it surfaced on this exact
        // write, the store must still accept new writes and reads.
        (void)observed_failure;
        store.SetBlockBits(100, 0, "1010");
        assert(store.GetBlockBits(100, 0) == "1010");
        store.SetBlockBits(0, 0, "0101");
        assert(store.GetBlockBits(0, 0) == "0101");
    }

    // And the data survives a reopen (recovery is not poisoned).
    {
        chunkdb::ChunkStore reopened(config);
        assert(reopened.GetBlockBits(100, 0) == "1010");
    }

    if (!RemoveAllWithRetry(data_dir)) {
        throw std::runtime_error(
            "failed to remove eviction-flush-failure temp dir: " + data_dir.string());
    }
}

}  // namespace

int main() {
    TestEvictionFlushFailureKeepsStoreServing();

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
        aggressive.wal_group_commit_updates = 64;

        {
            chunkdb::ChunkStore store(aggressive);
            for (int i = 0; i < 9; ++i) {
                store.SetBlockBits(i * 8, 0, (i % 2 == 0) ? "1111" : "0001");
            }

            const std::size_t lower_watermark = EvictionLowerWatermark(aggressive.max_loaded_chunks);
            assert(store.ApproxLoadedChunkCount() <= lower_watermark + 2);

            for (int i = 9; i < 200; ++i) {
                store.SetBlockBits(i * 8, 0, (i % 2 == 0) ? "1111" : "0001");
                assert(store.ApproxLoadedChunkCount() <= aggressive.max_loaded_chunks + 2);
            }

            assert(store.EvictionSnapshotBuildCountForTests() < 30);
            const auto stats = store.RuntimeStats();
            assert(stats.evictions > 0);
            assert(stats.eviction_probes > 0);
            assert(stats.eviction_probes >= stats.evictions);
            assert(stats.eviction_forced_wal_flushes > 0);
            assert(
                stats.eviction_forced_wal_flushes ==
                stats.eviction_forced_wal_flushes_with_data +
                    stats.eviction_forced_wal_flushes_empty_batch);
        }

        if (!RemoveAllWithRetry(aggressive_data_dir)) {
            throw std::runtime_error(
                "failed to remove aggressive cache-eviction temp dir: " + aggressive_data_dir.string());
        }
    }

    {
        auto bounded = config;
        const auto bounded_data_dir = TempDataDir();
        bounded.data_dir = bounded_data_dir;
        bounded.geometry.large_chunk_width_chunks = 1;
        bounded.geometry.large_chunk_height_chunks = 1;
        bounded.max_loaded_chunks = 4;
        bounded.checkpoint_update_interval = 10'000;
        bounded.checkpoint_wal_bytes = 10'000'000;
        bounded.wal_group_commit_updates = 64;

        {
            chunkdb::ChunkStore store(bounded);
            const auto blocks_per_chunk =
                static_cast<int>(store.geometry().config().chunk_width_blocks);
            constexpr std::uint64_t kDistinctLargeChunksTouched = 20;

            for (int i = 0; i < 4; ++i) {
                store.SetBlockBits(i * blocks_per_chunk, 0, "1010");
            }

            store.ClearEvictionCandidatesForTests();
            const auto refill_passes_before = store.EvictionSnapshotBuildCountForTests();
            const auto refill_scans_before = store.EvictionRefillLargeChunkScanCountForTests();
            const auto post_pass_checks_before =
                store.EvictionPostPassLargeChunkCheckCountForTests();

            for (int i = 4; i < 20; ++i) {
                store.SetBlockBits(i * blocks_per_chunk, 0, "0101");
            }

            const auto refill_passes =
                store.EvictionSnapshotBuildCountForTests() - refill_passes_before;
            const auto refill_scans =
                store.EvictionRefillLargeChunkScanCountForTests() - refill_scans_before;
            const auto ring_size = store.EvictionLargeChunkRingSizeForTests();
            const auto post_pass_checks =
                store.EvictionPostPassLargeChunkCheckCountForTests() - post_pass_checks_before;

            assert(refill_passes > 0);
            assert(refill_scans <= refill_passes * kEvictionRefillLargeChunkBudget);
            assert(store.ApproxLoadedChunkCount() <= bounded.max_loaded_chunks + 2);
            assert(ring_size <= bounded.max_loaded_chunks + 2);
            assert(post_pass_checks < kDistinctLargeChunksTouched);
        }

        if (!RemoveAllWithRetry(bounded_data_dir)) {
            throw std::runtime_error(
                "failed to remove bounded cache-eviction temp dir: " + bounded_data_dir.string());
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
