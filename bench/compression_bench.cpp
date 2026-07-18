// Reproducible micro-benchmark for the zrle chunk-state codec.
//
// Measures compressed size ratio, compress/decompress throughput, and
// per-operation latency percentiles for representative sparse and dense
// chunk states. Results are printed as key=value lines; record them in
// bench/artifacts/ when making compression decisions.
//
// Usage: chunkdb_compression_bench [--iterations <n>] [--state-bytes <n>]

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "chunkdb/zrle.hpp"

namespace {

struct Workload {
    std::string name;
    std::vector<std::uint8_t> state;
};

struct LatencyStats {
    double p50_us = 0;
    double p99_us = 0;
    double total_seconds = 0;
};

LatencyStats Percentiles(std::vector<double>* samples_us, double total_seconds) {
    std::sort(samples_us->begin(), samples_us->end());
    LatencyStats stats;
    stats.total_seconds = total_seconds;
    if (!samples_us->empty()) {
        stats.p50_us = (*samples_us)[samples_us->size() / 2];
        stats.p99_us = (*samples_us)[std::min(
            samples_us->size() - 1, (samples_us->size() * 99) / 100)];
    }
    return stats;
}

}  // namespace

int main(int argc, char** argv) {
    std::size_t iterations = 20000;
    std::size_t state_bytes = 544;  // default geometry: 512B payload + 32B presence

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--iterations" && i + 1 < argc) {
            iterations = static_cast<std::size_t>(std::stoull(argv[++i]));
        } else if (arg == "--state-bytes" && i + 1 < argc) {
            state_bytes = static_cast<std::size_t>(std::stoull(argv[++i]));
        } else {
            std::cerr << "usage: chunkdb_compression_bench [--iterations n] [--state-bytes n]\n";
            return 2;
        }
    }

    std::mt19937_64 rng(42);  // fixed seed for reproducibility
    std::vector<Workload> workloads;

    {
        Workload sparse{.name = "sparse_2pct", .state = std::vector<std::uint8_t>(state_bytes, 0U)};
        for (std::size_t i = 0; i < state_bytes / 50 + 1; ++i) {
            sparse.state[rng() % state_bytes] = static_cast<std::uint8_t>(rng() & 0xFF);
        }
        workloads.push_back(std::move(sparse));
    }
    {
        Workload half{.name = "half_dense", .state = std::vector<std::uint8_t>(state_bytes, 0U)};
        for (std::size_t i = 0; i < state_bytes / 2; ++i) {
            half.state[rng() % state_bytes] = static_cast<std::uint8_t>(rng() & 0xFF);
        }
        workloads.push_back(std::move(half));
    }
    {
        Workload dense{.name = "dense_random", .state = std::vector<std::uint8_t>(state_bytes)};
        for (auto& byte : dense.state) {
            byte = static_cast<std::uint8_t>(rng() & 0xFF);
        }
        workloads.push_back(std::move(dense));
    }

    std::cout << "iterations=" << iterations << " state_bytes=" << state_bytes << "\n";

    for (const auto& workload : workloads) {
        const auto compressed = chunkdb::ZrleCompress(workload.state);

        std::vector<double> compress_us;
        compress_us.reserve(iterations);
        const auto compress_start = std::chrono::steady_clock::now();
        for (std::size_t i = 0; i < iterations; ++i) {
            const auto op_start = std::chrono::steady_clock::now();
            const auto out = chunkdb::ZrleCompress(workload.state);
            compress_us.push_back(
                std::chrono::duration<double, std::micro>(
                    std::chrono::steady_clock::now() - op_start)
                    .count());
            if (out.empty()) {
                return 1;
            }
        }
        const double compress_total =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - compress_start)
                .count();

        std::vector<double> decompress_us;
        decompress_us.reserve(iterations);
        const auto decompress_start = std::chrono::steady_clock::now();
        for (std::size_t i = 0; i < iterations; ++i) {
            const auto op_start = std::chrono::steady_clock::now();
            const auto out = chunkdb::ZrleDecompress(compressed, workload.state.size());
            decompress_us.push_back(
                std::chrono::duration<double, std::micro>(
                    std::chrono::steady_clock::now() - op_start)
                    .count());
            if (out.size() != workload.state.size()) {
                return 1;
            }
        }
        const double decompress_total =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - decompress_start)
                .count();

        const auto compress_stats = Percentiles(&compress_us, compress_total);
        const auto decompress_stats = Percentiles(&decompress_us, decompress_total);
        const double mib = static_cast<double>(state_bytes) * static_cast<double>(iterations) /
                           (1024.0 * 1024.0);

        std::cout << "workload=" << workload.name
                  << " raw_bytes=" << workload.state.size()
                  << " compressed_bytes=" << compressed.size()
                  << " ratio=" << (static_cast<double>(compressed.size()) /
                                   static_cast<double>(workload.state.size()))
                  << " compress_mib_s=" << (mib / compress_stats.total_seconds)
                  << " compress_p50_us=" << compress_stats.p50_us
                  << " compress_p99_us=" << compress_stats.p99_us
                  << " decompress_mib_s=" << (mib / decompress_stats.total_seconds)
                  << " decompress_p50_us=" << decompress_stats.p50_us
                  << " decompress_p99_us=" << decompress_stats.p99_us
                  << "\n";
    }
    return 0;
}
