#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
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
    };
    std::chrono::steady_clock::time_point at;
    // Cumulative counters in their reported units, in report order.
    std::vector<std::pair<std::string, double>> counters;
    // Current values, reported at the start and at the end of the window.
    std::vector<std::pair<std::string, double>> levels;
    // Threads of this process by id (Linux).
    std::vector<std::pair<std::string, Thread>> threads;
    std::optional<StoreRuntimeStats> engine;
};

// The constructor reads the starting values and starts a thread that wakes
// every 10 ms and records the longest gap between its wake-ups; Finish reads
// the end values and returns the window's name/value pairs.
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
    std::mutex mutex_;
    std::condition_variable stop_changed_;
    bool stop_ = false;
    double max_gap_ms_ = 0.0;
    std::size_t gaps_over_100ms_ = 0;
    std::thread sampler_;
};

}  // namespace chunkdb::server_bench
