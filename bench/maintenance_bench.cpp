#include "chunkdb/table_catalog.hpp"
#include "chunkdb/logging.hpp"

#include <algorithm>
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

bool Switch(const std::string& text) {
    if (text == "on") return true;
    if (text == "off") return false;
    throw std::invalid_argument("expected on or off");
}

struct ProcessStats { std::uint64_t threads = 0U, rss_bytes = 0U, peak_rss_bytes = 0U; };
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
        if (key == "VmHWM:") result.peak_rss_bytes = value * 1024U;
    }
    if (result.threads == 0U || result.rss_bytes == 0U || result.peak_rss_bytes == 0U)
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

double Milliseconds(Clock::duration duration) {
    return std::chrono::duration<double, std::milli>(duration).count();
}

double TimedBarrier(chunkdb::TableCatalog& catalog) {
    const auto started = Clock::now();
    catalog.WalBarrier();
    return Milliseconds(Clock::now() - started);
}

// Nearest-rank percentile of a non-empty sample.
double Percentile(std::vector<double> samples, double fraction) {
    std::sort(samples.begin(), samples.end());
    const auto rank = static_cast<std::size_t>(fraction * static_cast<double>(samples.size()) + 0.999999);
    return samples[std::clamp<std::size_t>(rank, 1U, samples.size()) - 1U];
}

void PrintStat(const char* name, const std::optional<ProcessStats>& stats, std::uint64_t ProcessStats::*field) {
    std::cout << ",\"" << name << "\":";
    if (stats) std::cout << (*stats).*field; else std::cout << "null";
}

// Checkpoints of every table so far, and how many ran on maintenance workers.
struct CheckpointCounts { std::uint64_t total = 0U, background = 0U; };
CheckpointCounts Checkpoints(const std::vector<std::shared_ptr<chunkdb::Table>>& tables) {
    CheckpointCounts counts;
    for (const auto& table : tables) {
        const auto stats = table->Acquire()->store().RuntimeStats();
        counts.total += stats.checkpoints;
        counts.background += stats.background_checkpoints;
    }
    return counts;
}

void PrintSamples(const char* name, const std::vector<double>& samples) {
    std::cout << ",\"" << name << "_p50\":" << Percentile(samples, 0.50)
        << ",\"" << name << "_p99\":" << Percentile(samples, 0.99)
        << ",\"" << name << "_max\":" << Percentile(samples, 1.0);
}

struct Options {
    std::size_t requests = 200000U, workers = 2U, flushes = 20U;
    bool background_maintenance = true;
};

void Measure(const std::filesystem::path& directory, std::size_t count, const Options& options) {
    chunkdb::CatalogConfig config;
    config.data_dir = directory;
    config.background_maintenance = options.background_maintenance;
    config.default_options.checkpoint_update_interval = 32U;
    const bool shared = ConfigureWorkers(config, options.workers);
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
    std::size_t written = 0U;
    const auto write = [&](std::size_t table) {
        auto lease = tables[table]->Acquire();
        lease->store().SetBlockBits(0, 0, (written++ / count) % 2U == 0U ? "10000000" : "01000000");
    };
    const auto checkpoints_before = Checkpoints(tables);
    const auto started = Clock::now();
    for (std::size_t i = 0U; i < options.requests; ++i) write(i % count);
    const auto finished = Clock::now();
    const auto checkpoints_after = Checkpoints(tables);
    const auto stats = CurrentStats();
    const double flush_ms = TimedBarrier(catalog);
    // Each dirty barrier follows one write to every table; idle barriers
    // follow none and price the scheduling of an empty barrier.
    std::vector<double> dirty, idle;
    for (std::size_t round = 0U; round < options.flushes; ++round) {
        for (std::size_t table = 0U; table < count; ++table) write(table);
        dirty.push_back(TimedBarrier(catalog));
    }
    for (std::size_t round = 0U; round < options.flushes; ++round) idle.push_back(TimedBarrier(catalog));
    const auto after = CurrentStats();
    const double seconds = std::chrono::duration<double>(finished - started).count();
    if (seconds <= 0.0) throw std::runtime_error("write window is below the clock resolution");
    std::cout << "{\"tables\":" << count << ",\"requests\":" << options.requests
        << ",\"background_maintenance\":" << (options.background_maintenance ? "true" : "false")
        << ",\"maintenance_workers\":" << (shared ? options.workers : 0U);
    PrintStat("threads", stats, &ProcessStats::threads);
    PrintStat("rss_bytes", stats, &ProcessStats::rss_bytes);
    PrintStat("threads_after_flushes", after, &ProcessStats::threads);
    PrintStat("peak_rss_bytes", after, &ProcessStats::peak_rss_bytes);
    std::cout << ",\"write_seconds\":" << seconds
        << ",\"write_ops_per_second\":" << static_cast<double>(options.requests) / seconds
        << ",\"write_checkpoints\":" << checkpoints_after.total - checkpoints_before.total
        << ",\"write_inline_checkpoints\":"
        << (checkpoints_after.total - checkpoints_before.total) -
               (checkpoints_after.background - checkpoints_before.background)
        << ",\"flush_ms\":" << flush_ms << ",\"flushes\":" << options.flushes;
    PrintSamples("dirty_flush_ms", dirty);
    PrintSamples("idle_flush_ms", idle);
    std::cout << "}" << std::endl;
}
}

int main(int argc, char** argv) {
    try {
        std::filesystem::path directory;
        std::vector<std::size_t> counts{1U, 100U, 1000U};
        Options options;
        for (int i = 1; i < argc; ++i) {
            const std::string option(argv[i]);
            if (option == "--help") {
                std::cout << "Usage: chunkdb_maintenance_bench --data-dir NEW_PATH [--tables N] [--requests N]"
                    " [--flushes N] [--maintenance-workers N] [--background-maintenance on|off]\n";
                return 0;
            }
            if (i + 1 >= argc) throw std::invalid_argument("missing option value");
            const std::string value(argv[++i]);
            if (option == "--data-dir") directory = value;
            else if (option == "--tables") counts = {Positive(value)};
            else if (option == "--requests") options.requests = Positive(value);
            else if (option == "--flushes") options.flushes = Positive(value);
            else if (option == "--maintenance-workers") options.workers = Positive(value);
            else if (option == "--background-maintenance") options.background_maintenance = Switch(value);
            else throw std::invalid_argument("unknown option: " + option);
        }
        if (directory.empty() || std::filesystem::exists(directory))
            throw std::invalid_argument("--data-dir must name a new directory");
        chunkdb::SetLogLevel(chunkdb::LogLevel::kError);
        for (const auto count : counts) Measure(directory / std::to_string(count), count, options);
    } catch (const std::exception& error) {
        std::cerr << "maintenance benchmark failed: " << error.what() << '\n';
        return 1;
    }
}
