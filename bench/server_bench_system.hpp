#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb::server_bench {

// Process, machine and engine counters of one measured window, for
// --system-stats (report-only). A counter the platform does not provide is
// left out.
struct SystemSample {
    struct Thread {
        double cpu_s = 0.0;
        double runqueue_s = 0.0;
        double blkio_s = 0.0;
        std::uint64_t start_ticks = 0U;
    };
    std::chrono::steady_clock::time_point at{};
    // Cumulative counters in their reported units, in report order.
    std::vector<std::pair<std::string, double>> counters;
    // Current values, reported at the start and at the end of the window.
    std::vector<std::pair<std::string, double>> levels;
    // Threads of this process by id and start time (Linux).
    std::vector<std::pair<std::string, Thread>> threads;
    std::optional<StoreRuntimeStats> engine;
};

[[nodiscard]] std::vector<std::pair<std::string, std::string>> SystemStatsBetween(
    const SystemSample& start, const SystemSample& end);

class SystemSamplerGaps {
  public:
    explicit SystemSamplerGaps(std::chrono::steady_clock::time_point start) : last_(start) {}
    void Observe(std::chrono::steady_clock::time_point at);
    [[nodiscard]] double max_gap_ms() const noexcept { return max_gap_ms_; }
    [[nodiscard]] std::size_t gaps_over_100ms() const noexcept { return gaps_over_100ms_; }

  private:
    std::chrono::steady_clock::time_point last_{};
    double max_gap_ms_ = 0.0;
    std::size_t gaps_over_100ms_ = 0U;
};

// The constructor takes the initial snapshot and starts a sampler with a 10 ms
// timer. Finish ends sampling and takes the final snapshot.
class SystemWindow {
  public:
    using EngineStats = std::function<std::optional<StoreRuntimeStats>()>;

    explicit SystemWindow(EngineStats engine);
    ~SystemWindow();
    SystemWindow(const SystemWindow&) = delete;
    SystemWindow& operator=(const SystemWindow&) = delete;

    [[nodiscard]] std::vector<std::pair<std::string, std::string>> Finish();

  private:
    void StopSampler();

    EngineStats engine_;
    SystemSample start_;
    SystemSamplerGaps gaps_;
    std::mutex mutex_;
    std::condition_variable stop_changed_;
    bool stop_ = false;
    std::jthread sampler_;
};

}  // namespace chunkdb::server_bench
