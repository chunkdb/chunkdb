#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "chunkdb/logging.hpp"

namespace chunkdb::server_bench {

enum class ServerMode {
    kExternal = 0,
    kSpawn = 1,
};

enum class OutputMode {
    kHuman = 0,
    kJson = 1,
};

enum class Scenario {
    kPing = 0,
    kSet = 1,
    kGet = 2,
    kChunkGetState = 3,
    kMixed = 4,
    // Workloads of grid worlds; each fills its region before it is timed.
    kWorld = 5,
    kCanvas = 6,
    kSimulation = 7,
};

struct Args {
    ServerMode server_mode = ServerMode::kExternal;
    std::string host = "127.0.0.1";
    std::uint16_t port = 4242;
    std::size_t clients = 50;
    std::size_t pipeline = 1;
    std::size_t requests = 5000;
    std::vector<Scenario> tests;
    std::size_t keyspace = 512;
    std::uint32_t seed = 1337;
    OutputMode output_mode = OutputMode::kHuman;
    chunkdb::LogLevel log_level = chunkdb::LogLevel::kInfo;
    // Logs in with SCRAM-SHA-256 when not empty; spawn mode creates the user.
    std::string user{};
    std::string password{};
    // Durability mode of the server spawn mode starts.
    std::string durability_mode = "relaxed";
    // Worker threads of the server spawn mode starts. A connection holds a
    // worker until it closes, so fewer workers than clients serialize them.
    std::size_t server_workers = 4;
    bool show_help = false;
};

struct ScenarioResult {
    std::string name;
    std::size_t completed_requests = 0;
    double duration_s = 0.0;
    double throughput_req_s = 0.0;
    double latency_avg_ms = 0.0;
    double latency_min_ms = 0.0;
    double latency_p50_ms = 0.0;
    double latency_p95_ms = 0.0;
    double latency_p99_ms = 0.0;
    double latency_max_ms = 0.0;
    std::vector<std::pair<double, double>> percentiles_ms;
    std::size_t payload_bytes = 0;
    std::string payload_label;
    std::size_t max_in_flight = 0;
    bool keepalive = true;
};

struct BenchmarkReport {
    ServerMode server_mode = ServerMode::kExternal;
    bool spawned_server = false;
    std::string host = "127.0.0.1";
    std::uint16_t port = 4242;
    std::size_t requested_clients = 0;
    std::size_t active_clients = 0;
    std::size_t pipeline = 0;
    std::size_t requests = 0;
    std::size_t keyspace = 0;
    std::uint32_t seed = 0;
    // The spawned server's durability mode; empty against an external server.
    std::string durability_mode;
    // The chunk locks of the spawned server (this build); "unknown" against
    // an external one.
    std::string chunk_lock_mode = "unknown";
    std::vector<ScenarioResult> results;
};

[[nodiscard]] const char* ServerModeName(ServerMode mode) noexcept;
[[nodiscard]] const char* OutputModeName(OutputMode mode) noexcept;
[[nodiscard]] const char* ScenarioName(Scenario scenario) noexcept;
[[nodiscard]] std::vector<Scenario> DefaultScenarios();

[[nodiscard]] std::string UsageText();
[[nodiscard]] Args ParseArgs(int argc, char** argv);
[[nodiscard]] Args ParseArgs(const std::vector<std::string>& argv);

[[nodiscard]] BenchmarkReport Run(const Args& args);
[[nodiscard]] std::string RenderHumanReport(const BenchmarkReport& report);
[[nodiscard]] std::string RenderJsonReport(const BenchmarkReport& report);

}  // namespace chunkdb::server_bench
