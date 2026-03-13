#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace {

using Clock = std::chrono::steady_clock;

struct BenchResult {
    std::string name;
    std::size_t ops = 0;
    double seconds = 0.0;
    double ops_per_sec = 0.0;
    double p50_us = 0.0;
    double p95_us = 0.0;
    double p99_us = 0.0;
};

double Percentile(std::vector<double> values, double p) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const double rank = (p / 100.0) * static_cast<double>(values.size() - 1);
    const std::size_t index = static_cast<std::size_t>(rank);
    return values[index];
}

BenchResult Measure(
    std::string name,
    std::size_t ops,
    const std::function<void(std::size_t)>& fn) {
    std::vector<double> latencies_us;
    latencies_us.reserve(ops);

    const auto started = Clock::now();
    for (std::size_t i = 0; i < ops; ++i) {
        const auto op_started = Clock::now();
        fn(i);
        const auto op_finished = Clock::now();
        const auto op_us = std::chrono::duration<double, std::micro>(op_finished - op_started).count();
        latencies_us.push_back(op_us);
    }
    const auto finished = Clock::now();

    const double seconds = std::chrono::duration<double>(finished - started).count();

    BenchResult result;
    result.name = std::move(name);
    result.ops = ops;
    result.seconds = seconds;
    result.ops_per_sec = seconds == 0.0 ? 0.0 : (static_cast<double>(ops) / seconds);
    result.p50_us = Percentile(latencies_us, 50.0);
    result.p95_us = Percentile(latencies_us, 95.0);
    result.p99_us = Percentile(latencies_us, 99.0);
    return result;
}

struct Args {
    std::size_t ops = 20000;
    std::filesystem::path data_dir;
    bool keep_data = false;
};

Args ParseArgs(int argc, char** argv) {
    Args args;
    const auto default_dir = std::filesystem::temp_directory_path() /
                             ("chunkdb-bench-" +
                              std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    args.data_dir = default_dir;

    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        auto require_value = [&](const char* name) {
            if (i + 1 >= argc) {
                throw std::invalid_argument(std::string("missing value for ") + name);
            }
            ++i;
            return std::string(argv[i]);
        };

        if (arg == "--ops") {
            const auto value = require_value("--ops");
            std::size_t consumed = 0;
            args.ops = std::stoull(value, &consumed, 10);
            if (consumed != value.size() || args.ops == 0) {
                throw std::invalid_argument("invalid --ops value: " + value);
            }
        } else if (arg == "--data-dir") {
            args.data_dir = require_value("--data-dir");
        } else if (arg == "--keep-data") {
            args.keep_data = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: chunkdb_bench [--ops N] [--data-dir PATH] [--keep-data]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument("unknown argument: " + std::string(arg));
        }
    }

    return args;
}

chunkdb::StoreConfig BuildStoreConfig(const std::filesystem::path& data_dir) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 8,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = 16,
            .chunk_height_blocks = 16,
            .block_bits = 16,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 512,
        .checkpoint_wal_bytes = 1024 * 1024,
        .wal_group_commit_updates = 8,
        .max_loaded_chunks = 16384,
        .allow_multiple_processes = false,
    };
}

void Print(const BenchResult& result) {
    std::cout << std::left << std::setw(22) << result.name
              << " ops=" << std::setw(8) << result.ops
              << " total_s=" << std::setw(10) << std::fixed << std::setprecision(4) << result.seconds
              << " ops_s=" << std::setw(12) << std::fixed << std::setprecision(2) << result.ops_per_sec
              << " p50_us=" << std::setw(9) << std::fixed << std::setprecision(2) << result.p50_us
              << " p95_us=" << std::setw(9) << std::fixed << std::setprecision(2) << result.p95_us
              << " p99_us=" << std::setw(9) << std::fixed << std::setprecision(2) << result.p99_us
              << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Args args = ParseArgs(argc, argv);

        if (std::filesystem::exists(args.data_dir)) {
            std::filesystem::remove_all(args.data_dir);
        }

        std::mt19937_64 rng(1337);
        std::uniform_int_distribution<int> dense_dist(0, 511);
        std::uniform_int_distribution<int> sparse_dist(-200000, 200000);

        const std::string bits_a = "0000000000000000";
        const std::string bits_b = "1111000011110000";

        auto store = std::make_unique<chunkdb::ChunkStore>(BuildStoreConfig(args.data_dir));

        std::vector<BenchResult> results;
        results.reserve(10);

        results.push_back(Measure("point_writes", args.ops, [&](std::size_t i) {
            const int x = dense_dist(rng);
            const int y = dense_dist(rng);
            store->SetBlockBits(x, y, (i % 2 == 0) ? bits_a : bits_b);
        }));

        results.push_back(Measure("point_reads", args.ops, [&](std::size_t) {
            const int x = dense_dist(rng);
            const int y = dense_dist(rng);
            (void)store->GetBlockBits(x, y);
        }));

        results.push_back(Measure("hot_chunk_writes", args.ops, [&](std::size_t i) {
            const int x = static_cast<int>(i % 16);
            const int y = static_cast<int>((i / 16) % 16);
            store->SetBlockBits(x, y, (i % 2 == 0) ? bits_b : bits_a);
        }));

        results.push_back(Measure("chunk_reads_text", args.ops / 4, [&](std::size_t i) {
            const int cx = static_cast<int>(i % 8);
            const int cy = static_cast<int>((i / 8) % 8);
            (void)store->GetChunkBits(cx, cy);
        }));

        results.push_back(Measure("chunk_reads_binary", args.ops / 4, [&](std::size_t i) {
            const int cx = static_cast<int>(i % 8);
            const int cy = static_cast<int>((i / 8) % 8);
            (void)store->GetChunkPayloadBytes(cx, cy);
        }));

        results.push_back(Measure("mixed_rw_70_30", args.ops, [&](std::size_t i) {
            const int x = dense_dist(rng);
            const int y = dense_dist(rng);
            if ((i % 10) < 7) {
                (void)store->GetBlockBits(x, y);
            } else {
                store->SetBlockBits(x, y, (i % 2 == 0) ? bits_a : bits_b);
            }
        }));

        results.push_back(Measure("sparse_world_writes", args.ops, [&](std::size_t i) {
            const int x = sparse_dist(rng);
            const int y = sparse_dist(rng);
            store->SetBlockBits(x, y, (i % 2 == 0) ? bits_a : bits_b);
        }));

        results.push_back(Measure("dense_world_writes", args.ops, [&](std::size_t i) {
            const int x = static_cast<int>(i % 512);
            const int y = static_cast<int>((i / 512) % 512);
            store->SetBlockBits(x, y, (i % 2 == 0) ? bits_b : bits_a);
        }));

        std::vector<std::pair<int, int>> probe_coords;
        probe_coords.reserve(args.ops / 4);
        for (std::size_t i = 0; i < args.ops / 4; ++i) {
            probe_coords.emplace_back(dense_dist(rng), dense_dist(rng));
        }

        store.reset();

        {
            auto cold_store = std::make_unique<chunkdb::ChunkStore>(BuildStoreConfig(args.data_dir));
            results.push_back(Measure("cold_start_reads", probe_coords.size(), [&](std::size_t i) {
                (void)cold_store->GetBlockBits(probe_coords[i].first, probe_coords[i].second);
            }));

            results.push_back(Measure("warm_cache_reads", probe_coords.size(), [&](std::size_t i) {
                (void)cold_store->GetBlockBits(probe_coords[i].first, probe_coords[i].second);
            }));
        }

        std::cout << "chunkdb benchmark scenarios\n";
        std::cout << "data_dir=" << args.data_dir << " ops=" << args.ops << "\n\n";
        for (const auto& result : results) {
            Print(result);
        }

        if (!args.keep_data) {
            std::filesystem::remove_all(args.data_dir);
        }

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "benchmark failed: " << e.what() << std::endl;
        return 1;
    }
}
