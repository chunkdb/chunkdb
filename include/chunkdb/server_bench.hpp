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
    kInfo = 1,
    kSet = 2,
    kGet = 3,
    kChunk = 4,
    kChunkBin = 5,
    kMixed = 6,
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
    std::string auth_token;
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
    std::size_t clients = 0;
    std::size_t pipeline = 0;
    std::size_t requests = 0;
    std::size_t keyspace = 0;
    std::uint32_t seed = 0;
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
