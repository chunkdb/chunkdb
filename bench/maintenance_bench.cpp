#include "chunkdb/table_catalog.hpp"
#include "chunkdb/logging.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;

std::size_t Positive(const std::string& text) {
    std::size_t used = 0U;
    if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
        throw std::invalid_argument("expected a positive integer");
    const auto number = std::stoull(text, &used);
    if (used != text.size() || number == 0U || number > std::numeric_limits<std::size_t>::max())
        throw std::invalid_argument("expected a positive integer");
    return static_cast<std::size_t>(number);
}

struct ProcessStats { std::uint64_t threads = 0U, rss_bytes = 0U; };
std::optional<ProcessStats> CurrentStats() {
#ifdef __linux__
    std::ifstream status("/proc/self/status");
    if (!status) throw std::runtime_error("cannot read /proc/self/status");
    ProcessStats result;
    std::string line;
    while (std::getline(status, line)) {
        std::istringstream fields(line);
        std::string key;
        std::uint64_t value = 0U;
        if (!(fields >> key >> value)) continue;
        if (key == "Threads:") result.threads = value;
        if (key == "VmRSS:") result.rss_bytes = value * 1024U;
    }
    if (result.threads == 0U || result.rss_bytes == 0U)
        throw std::runtime_error("process thread or RSS counters are unavailable");
    return result;
#else
    return std::nullopt;
#endif
}

// The same driver can be compiled against the baseline's public headers.
template <typename Config>
bool ConfigureWorkers(Config& config, std::size_t workers) {
    if constexpr (requires { config.maintenance_workers = workers; }) {
        config.maintenance_workers = workers;
        return true;
    }
    return false;
}

void Measure(const std::filesystem::path& directory, std::size_t count,
             std::size_t requests, std::size_t workers) {
    chunkdb::CatalogConfig config;
    config.data_dir = directory;
    config.background_maintenance = true;
    config.default_options.checkpoint_update_interval = 32U;
    const bool shared = ConfigureWorkers(config, workers);
    chunkdb::TableCatalog catalog(config);
    std::vector<std::shared_ptr<chunkdb::Table>> tables;
    const chunkdb::GeometryConfig geometry{
        .large_chunk_width_chunks = 2U, .large_chunk_height_chunks = 2U,
        .chunk_width_blocks = 4U, .chunk_height_blocks = 4U, .block_bits = 8U,
    };
    for (std::size_t i = 0U; i < count; ++i) {
        auto table = catalog.Create("table_" + std::to_string(i), geometry, config.default_options);
        table->Acquire()->store().SetBlockBits(0, 0, "00000000");
        tables.push_back(std::move(table));
    }
    catalog.WalBarrier();
    const auto started = Clock::now();
    for (std::size_t i = 0U; i < requests; ++i) {
        auto lease = tables[i % count]->Acquire();
        lease->store().SetBlockBits(0, 0, (i / count) % 2U == 0U ? "10000000" : "01000000");
    }
    const auto written = Clock::now();
    const auto stats = CurrentStats();
    const auto flush_started = Clock::now();
    catalog.WalBarrier();
    const auto flushed = Clock::now();
    const double seconds = std::chrono::duration<double>(written - started).count();
    if (seconds <= 0.0) throw std::runtime_error("write window is below the clock resolution");
    std::cout << "{\"tables\":" << count << ",\"requests\":" << requests
        << ",\"maintenance_workers\":" << (shared ? workers : 0U)
        << ",\"threads\":";
    if (stats) std::cout << stats->threads; else std::cout << "null";
    std::cout << ",\"rss_bytes\":";
    if (stats) std::cout << stats->rss_bytes; else std::cout << "null";
    std::cout << ",\"write_seconds\":" << seconds
        << ",\"write_ops_per_second\":" << static_cast<double>(requests) / seconds
        << ",\"flush_ms\":" << std::chrono::duration<double, std::milli>(flushed - flush_started).count()
        << "}\n";
}
}

int main(int argc, char** argv) {
    try {
        std::filesystem::path directory;
        std::vector<std::size_t> counts{1U, 100U, 1000U};
        std::size_t requests = 200000U, workers = 2U;
        for (int i = 1; i < argc; ++i) {
            const std::string option(argv[i]);
            if (option == "--help") {
                std::cout << "Usage: chunkdb_maintenance_bench --data-dir NEW_PATH [--tables N] [--requests N] [--maintenance-workers N]\n";
                return 0;
            }
            if (i + 1 >= argc) throw std::invalid_argument("missing option value");
            const std::string value(argv[++i]);
            if (option == "--data-dir") directory = value;
            else if (option == "--tables") counts = {Positive(value)};
            else if (option == "--requests") requests = Positive(value);
            else if (option == "--maintenance-workers") workers = Positive(value);
            else throw std::invalid_argument("unknown option: " + option);
        }
        if (directory.empty() || std::filesystem::exists(directory))
            throw std::invalid_argument("--data-dir must name a new directory");
        chunkdb::SetLogLevel(chunkdb::LogLevel::kError);
        for (const auto count : counts) Measure(directory / std::to_string(count), count, requests, workers);
    } catch (const std::exception& error) {
        std::cerr << "maintenance benchmark failed: " << error.what() << '\n';
        return 1;
    }
}
