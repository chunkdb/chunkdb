#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/chunk_store.hpp"

#ifdef _WIN32
#include <windows.h>
#include <psapi.h>
#else
#include <sys/resource.h>
#endif

namespace {

using Clock = std::chrono::steady_clock;

struct BenchResult {
    std::size_t ops = 0;
    double seconds = 0.0;
    double ops_per_sec = 0.0;
    double p50_us = 0.0;
    double p95_us = 0.0;
    double p99_us = 0.0;
};

struct FsMetrics {
    std::uint64_t file_count = 0;
    std::uint64_t dir_count = 0;
    std::uint64_t bytes_chk = 0;
    std::uint64_t bytes_wal = 0;
};

struct ResourceMetrics {
    double cpu_user_s = 0.0;
    double cpu_sys_s = 0.0;
    std::uint64_t peak_rss_bytes = 0;
};

struct Args {
    std::string scenario = "sparse_world_writes";
    std::size_t ops = 20000;
    std::filesystem::path data_dir;
    bool keep_data = false;
    bool no_reset = false;
    chunkdb::DurabilityMode durability_mode = chunkdb::DurabilityMode::kRelaxed;
    chunkdb::StorageLayoutMode storage_layout_mode = chunkdb::StorageLayoutMode::kFsSplitV1;
    std::size_t region_span_chunks = 16;
    std::uint64_t seed = 1337;
};

double Percentile(std::vector<double> values, double p) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    if (p <= 0.0) {
        return values.front();
    }
    if (p >= 100.0) {
        return values.back();
    }
    const double rank = (p / 100.0) * static_cast<double>(values.size() - 1U);
    const auto lower = static_cast<std::size_t>(rank);
    const auto upper = std::min<std::size_t>(lower + 1, values.size() - 1U);
    const double fraction = rank - static_cast<double>(lower);
    return values[lower] + (values[upper] - values[lower]) * fraction;
}

BenchResult Measure(std::size_t ops, const std::function<void(std::size_t)>& fn) {
    std::vector<double> latencies_us;
    latencies_us.reserve(ops);

    const auto started = Clock::now();
    for (std::size_t i = 0; i < ops; ++i) {
        const auto op_started = Clock::now();
        fn(i);
        const auto op_finished = Clock::now();
        latencies_us.push_back(
            std::chrono::duration<double, std::micro>(op_finished - op_started).count());
    }
    const auto finished = Clock::now();

    const double seconds = std::chrono::duration<double>(finished - started).count();
    return BenchResult{
        .ops = ops,
        .seconds = seconds,
        .ops_per_sec = seconds == 0.0 ? 0.0 : (static_cast<double>(ops) / seconds),
        .p50_us = Percentile(latencies_us, 50.0),
        .p95_us = Percentile(latencies_us, 95.0),
        .p99_us = Percentile(latencies_us, 99.0),
    };
}

ResourceMetrics GetResourceMetrics() {
#ifdef _WIN32
    ResourceMetrics out;
    FILETIME created{};
    FILETIME exited{};
    FILETIME kernel{};
    FILETIME user{};
    if (GetProcessTimes(GetCurrentProcess(), &created, &exited, &kernel, &user) != 0) {
        ULARGE_INTEGER k{};
        k.LowPart = kernel.dwLowDateTime;
        k.HighPart = kernel.dwHighDateTime;
        ULARGE_INTEGER u{};
        u.LowPart = user.dwLowDateTime;
        u.HighPart = user.dwHighDateTime;
        out.cpu_sys_s = static_cast<double>(k.QuadPart) / 10'000'000.0;
        out.cpu_user_s = static_cast<double>(u.QuadPart) / 10'000'000.0;
    }

    PROCESS_MEMORY_COUNTERS mem{};
    mem.cb = sizeof(mem);
    if (GetProcessMemoryInfo(GetCurrentProcess(), &mem, sizeof(mem)) != 0) {
        out.peak_rss_bytes = static_cast<std::uint64_t>(mem.PeakWorkingSetSize);
    }
    return out;
#else
    struct rusage usage {};
    if (getrusage(RUSAGE_SELF, &usage) != 0) {
        return {};
    }

    ResourceMetrics out;
    out.cpu_user_s =
        static_cast<double>(usage.ru_utime.tv_sec) +
        static_cast<double>(usage.ru_utime.tv_usec) / 1'000'000.0;
    out.cpu_sys_s =
        static_cast<double>(usage.ru_stime.tv_sec) +
        static_cast<double>(usage.ru_stime.tv_usec) / 1'000'000.0;
#ifdef __APPLE__
    out.peak_rss_bytes = static_cast<std::uint64_t>(usage.ru_maxrss);
#else
    out.peak_rss_bytes = static_cast<std::uint64_t>(usage.ru_maxrss) * 1024ULL;
#endif
    return out;
#endif
}

FsMetrics CollectFsMetrics(const std::filesystem::path& data_dir) {
    FsMetrics metrics;
    if (!std::filesystem::exists(data_dir)) {
        return metrics;
    }

    for (const auto& entry : std::filesystem::recursive_directory_iterator(data_dir)) {
        if (entry.is_directory()) {
            ++metrics.dir_count;
            continue;
        }
        if (!entry.is_regular_file()) {
            continue;
        }

        ++metrics.file_count;
        std::error_code ec;
        const auto size = entry.file_size(ec);
        if (ec) {
            continue;
        }
        const auto ext = entry.path().extension().string();
        if (ext == ".chk" || ext == ".rgn") {
            metrics.bytes_chk += static_cast<std::uint64_t>(size);
        } else if (ext == ".wal") {
            metrics.bytes_wal += static_cast<std::uint64_t>(size);
        }
    }
    return metrics;
}

Args ParseArgs(int argc, char** argv) {
    Args args;
    args.data_dir = std::filesystem::temp_directory_path() /
                    ("chunkdb-layout-ab-" +
                     std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));

    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        auto require_value = [&](const char* name) {
            if (i + 1 >= argc) {
                throw std::invalid_argument(std::string("missing value for ") + name);
            }
            ++i;
            return std::string(argv[i]);
        };

        if (arg == "--scenario") {
            args.scenario = require_value("--scenario");
        } else if (arg == "--ops") {
            const auto value = require_value("--ops");
            std::size_t consumed = 0;
            args.ops = std::stoull(value, &consumed, 10);
            if (consumed != value.size() || args.ops == 0) {
                throw std::invalid_argument("invalid --ops value: " + value);
            }
        } else if (arg == "--data-dir") {
            args.data_dir = require_value("--data-dir");
        } else if (arg == "--durability") {
            args.durability_mode = chunkdb::ParseDurabilityMode(require_value("--durability"));
        } else if (arg == "--layout") {
            args.storage_layout_mode = chunkdb::ParseStorageLayoutMode(require_value("--layout"));
        } else if (arg == "--region-span") {
            const auto value = require_value("--region-span");
            std::size_t consumed = 0;
            args.region_span_chunks = std::stoull(value, &consumed, 10);
            if (consumed != value.size() || args.region_span_chunks == 0) {
                throw std::invalid_argument("invalid --region-span value: " + value);
            }
        } else if (arg == "--seed") {
            const auto value = require_value("--seed");
            std::size_t consumed = 0;
            args.seed = std::stoull(value, &consumed, 10);
            if (consumed != value.size()) {
                throw std::invalid_argument("invalid --seed value: " + value);
            }
        } else if (arg == "--keep-data") {
            args.keep_data = true;
        } else if (arg == "--no-reset") {
            args.no_reset = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout
                << "Usage: layout_ab_bench "
                << "[--scenario NAME] [--ops N] [--data-dir PATH] "
                << "[--durability relaxed|fsync-wal|fsync-checkpoint] "
                << "[--layout fs_split_v1|fs_region_v1] [--region-span N] "
                << "[--seed N] [--keep-data] [--no-reset]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument("unknown argument: " + std::string(arg));
        }
    }

    return args;
}

chunkdb::StoreConfig BuildStoreConfig(const Args& args) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 8,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = 16,
            .chunk_height_blocks = 16,
            .block_bits = 16,
        },
        .data_dir = args.data_dir,
        .durability_mode = args.durability_mode,
        .checkpoint_update_interval = 512,
        .checkpoint_wal_bytes = 1024 * 1024,
        .wal_group_commit_updates = 8,
        .max_loaded_chunks = 16384,
        .allow_multiple_processes = false,
        .storage_layout_mode = args.storage_layout_mode,
        .experimental_region_span_chunks = args.region_span_chunks,
    };
}

void SeedDenseDataset(chunkdb::ChunkStore* store, std::size_t ops, std::mt19937_64* rng) {
    std::uniform_int_distribution<int> dense_dist(0, 511);
    for (std::size_t i = 0; i < ops; ++i) {
        const int x = dense_dist(*rng);
        const int y = dense_dist(*rng);
        const std::string bits = (i % 2 == 0) ? "0000000000000000" : "1111000011110000";
        store->SetBlockBits(x, y, bits);
    }
}

BenchResult RunScenario(const Args& args) {
    std::mt19937_64 rng(args.seed);
    std::uniform_int_distribution<int> dense_dist(0, 511);
    // Keep sparse coordinates broadly distributed but bounded so 100k-op runs stay reproducible
    // and practical for repeated A/B matrix execution.
    std::uniform_int_distribution<int> sparse_dist(-1023, 1023);
    const std::string bits_a = "0000000000000000";
    const std::string bits_b = "1111000011110000";

    if (args.scenario == "write_loop") {
        auto store = std::make_unique<chunkdb::ChunkStore>(BuildStoreConfig(args));
        std::size_t i = 0;
        while (true) {
            const int x = sparse_dist(rng);
            const int y = sparse_dist(rng);
            store->SetBlockBits(x, y, (i % 2 == 0) ? bits_a : bits_b);
            ++i;
        }
    }

    if (args.scenario == "cold_start_reads" || args.scenario == "warm_cache_reads") {
        {
            auto seed_store = std::make_unique<chunkdb::ChunkStore>(BuildStoreConfig(args));
            SeedDenseDataset(seed_store.get(), args.ops, &rng);
        }

        std::vector<std::pair<int, int>> coords;
        coords.reserve(args.ops);
        for (std::size_t i = 0; i < args.ops; ++i) {
            coords.emplace_back(dense_dist(rng), dense_dist(rng));
        }

        auto read_store = std::make_unique<chunkdb::ChunkStore>(BuildStoreConfig(args));
        if (args.scenario == "warm_cache_reads") {
            for (const auto [x, y] : coords) {
                (void)read_store->GetBlockBits(x, y);
            }
        }
        return Measure(coords.size(), [&](std::size_t i) {
            (void)read_store->GetBlockBits(coords[i].first, coords[i].second);
        });
    }

    auto store = std::make_unique<chunkdb::ChunkStore>(BuildStoreConfig(args));
    if (args.scenario == "sparse_world_writes") {
        return Measure(args.ops, [&](std::size_t i) {
            const int x = sparse_dist(rng);
            const int y = sparse_dist(rng);
            store->SetBlockBits(x, y, (i % 2 == 0) ? bits_a : bits_b);
        });
    }
    if (args.scenario == "dense_world_writes") {
        return Measure(args.ops, [&](std::size_t i) {
            const int x = static_cast<int>(i % 512U);
            const int y = static_cast<int>((i / 512U) % 512U);
            store->SetBlockBits(x, y, (i % 2 == 0) ? bits_a : bits_b);
        });
    }
    if (args.scenario == "mixed_rw_70_30") {
        return Measure(args.ops, [&](std::size_t i) {
            const int x = dense_dist(rng);
            const int y = dense_dist(rng);
            if ((i % 10U) < 7U) {
                (void)store->GetBlockBits(x, y);
            } else {
                store->SetBlockBits(x, y, (i % 2 == 0) ? bits_a : bits_b);
            }
        });
    }
    if (args.scenario == "recovery_probe") {
        std::vector<std::pair<int, int>> coords(args.ops, std::pair<int, int>{0, 0});
        return Measure(coords.size(), [&](std::size_t i) {
            (void)store->GetBlockBits(coords[i].first, coords[i].second);
        });
    }

    throw std::invalid_argument("unsupported scenario: " + args.scenario);
}

void PrintResultLine(
    const Args& args,
    const BenchResult& result,
    const FsMetrics& fs,
    const ResourceMetrics& resources) {
    std::cout
        << "RESULT"
        << " scenario=" << args.scenario
        << " layout=" << chunkdb::StorageLayoutModeName(args.storage_layout_mode)
        << " durability=" << chunkdb::DurabilityModeName(args.durability_mode)
        << " ops=" << result.ops
        << " total_s=" << std::fixed << std::setprecision(6) << result.seconds
        << " ops_s=" << std::fixed << std::setprecision(2) << result.ops_per_sec
        << " p50_us=" << std::fixed << std::setprecision(2) << result.p50_us
        << " p95_us=" << std::fixed << std::setprecision(2) << result.p95_us
        << " p99_us=" << std::fixed << std::setprecision(2) << result.p99_us
        << " files=" << fs.file_count
        << " dirs=" << fs.dir_count
        << " bytes_chk=" << fs.bytes_chk
        << " bytes_wal=" << fs.bytes_wal
        << " cpu_user_s=" << std::fixed << std::setprecision(6) << resources.cpu_user_s
        << " cpu_sys_s=" << std::fixed << std::setprecision(6) << resources.cpu_sys_s
        << " peak_rss_bytes=" << resources.peak_rss_bytes
        << "\n";
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Args args = ParseArgs(argc, argv);
        if (!args.no_reset && std::filesystem::exists(args.data_dir)) {
            std::filesystem::remove_all(args.data_dir);
        }

        const auto resources_before = GetResourceMetrics();
        const BenchResult result = RunScenario(args);
        const auto resources_after = GetResourceMetrics();
        const FsMetrics fs = CollectFsMetrics(args.data_dir);

        const ResourceMetrics resources_delta{
            .cpu_user_s = std::max(0.0, resources_after.cpu_user_s - resources_before.cpu_user_s),
            .cpu_sys_s = std::max(0.0, resources_after.cpu_sys_s - resources_before.cpu_sys_s),
            .peak_rss_bytes = resources_after.peak_rss_bytes,
        };

        PrintResultLine(args, result, fs, resources_delta);

        if (!args.keep_data) {
            std::filesystem::remove_all(args.data_dir);
        }
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "layout_ab_bench failed: " << e.what() << std::endl;
        return 1;
    }
}
