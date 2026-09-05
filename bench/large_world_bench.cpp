// Large-world sparse-write benchmark.
//
// Purpose: produce ONE reproducible number for the sparse (new-chunk) write
// path instead of the several non-comparable throughput figures that used to
// live in docs/. The figure that does not depend on the "cache size / number
// of fresh chunks" ratio is the steady-state cost of a single eviction
// (`s_per_eviction`), so that is what this benchmark reports, together with
// the eviction counters it is derived from.
//
// Three deliberate differences from the ad-hoc harnesses that produced the
// historical numbers:
//
//   1. The warm-up phase (exactly `max_loaded_chunks` fresh chunks, which cost
//      no eviction because the cache is still filling) is NOT part of the
//      measured window. Mixing it in is what produced averages such as
//      "355 ops/s" where only ~25% of the operations actually evicted.
//   2. Memory is reported from the CURRENT resident set size
//      (`mach_task_basic_info` on macOS, /proc/self/statm on Linux,
//      GetProcessMemoryInfo on Windows), not from `getrusage(ru_maxrss)`.
//      `ru_maxrss` is a high-water mark that also captures transient peaks and
//      therefore overstates the per-resident-chunk cost.
//   3. The output always carries `evictions`, `eviction_forced_wal_flushes`,
//      `wal_batch_flushes` and the derived `s_per_eviction`, so a reader can
//      tell how diluted an averaged throughput number is.
//
// The store configuration is pinned (geometry, checkpoint thresholds, group
// commit, background maintenance) so that runs on different hosts and dates
// stay comparable; only `--cache`, `--chunks`, `--threads` and `--durability`
// are variable.
//
// Store teardown is skipped by default (`--close-store` enables it): closing
// the store flushes every resident dirty chunk, which costs O(cache) durable
// snapshot-generation brackets and would dominate wall time without telling us
// anything the measured window does not already report.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if defined(_WIN32)
#include <windows.h>
#include <psapi.h>
#else
#include <fcntl.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <unistd.h>
#endif

#if defined(__APPLE__)
#include <mach/mach.h>
#include <sys/mount.h>
#endif

#if defined(__linux__)
#include <sys/statfs.h>
#endif

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/logging.hpp"

namespace {

using Clock = std::chrono::steady_clock;

double Secs(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double>(b - a).count();
}

// ---------------------------------------------------------------------------
// Current (not peak) resident set size.
// ---------------------------------------------------------------------------

std::uint64_t CurrentRssBytes() {
#if defined(__APPLE__)
    mach_task_basic_info info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            reinterpret_cast<task_info_t>(&info),
            &count) != KERN_SUCCESS) {
        return 0;
    }
    return static_cast<std::uint64_t>(info.resident_size);
#elif defined(__linux__)
    std::FILE* file = std::fopen("/proc/self/statm", "r");
    if (file == nullptr) {
        return 0;
    }
    unsigned long long total_pages = 0;
    unsigned long long resident_pages = 0;
    const int scanned = std::fscanf(file, "%llu %llu", &total_pages, &resident_pages);
    std::fclose(file);
    if (scanned != 2) {
        return 0;
    }
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        return 0;
    }
    return static_cast<std::uint64_t>(resident_pages) * static_cast<std::uint64_t>(page_size);
#elif defined(_WIN32)
    PROCESS_MEMORY_COUNTERS mem{};
    mem.cb = sizeof(mem);
    if (GetProcessMemoryInfo(GetCurrentProcess(), &mem, sizeof(mem)) == 0) {
        return 0;
    }
    return static_cast<std::uint64_t>(mem.WorkingSetSize);
#else
    return 0;
#endif
}

// ---------------------------------------------------------------------------
// Host facts that the numbers are only reproducible together with: the
// filesystem backing the data directory, and whether a full barrier sync
// (`F_FULLFSYNC` on macOS) is actually honored there.
//
// src/durability_io.cpp falls back to plain fsync when F_FULLFSYNC reports
// EINVAL/ENOTSUP/ENOTTY, and a run that silently took the fallback is not
// comparable with one that did not.
// ---------------------------------------------------------------------------

std::string FilesystemName(const std::filesystem::path& dir) {
#if defined(__APPLE__) || defined(BSD)
    struct statfs info {};
    if (::statfs(dir.string().c_str(), &info) != 0) {
        return "unknown";
    }
    return std::string(info.f_fstypename);
#elif defined(__linux__)
    struct statfs info {};
    if (::statfs(dir.string().c_str(), &info) != 0) {
        return "unknown";
    }
    switch (static_cast<unsigned long>(info.f_type)) {
        case 0xEF53UL: return "ext2/3/4";
        case 0x58465342UL: return "xfs";
        case 0x9123683EUL: return "btrfs";
        case 0x01021994UL: return "tmpfs";
        case 0x2FC12FC1UL: return "zfs";
        case 0x65735546UL: return "fuse";
        default: {
            char buffer[32];
            std::snprintf(
                buffer,
                sizeof(buffer),
                "0x%lx",
                static_cast<unsigned long>(info.f_type));
            return std::string(buffer);
        }
    }
#else
    (void)dir;
    return "unknown";
#endif
}

struct SyncProbe {
    // "fullfsync", "fullfsync-fallback-fsync", "fdatasync", "unknown".
    std::string kind = "unknown";
    double median_us = 0.0;
};

// Times the same durable-sync primitive the write path uses, on the same
// filesystem, so that `s_per_eviction` can be compared against the number of
// syncs one eviction performs.
SyncProbe ProbeDurableSync(const std::filesystem::path& dir) {
    SyncProbe probe;
#if defined(_WIN32)
    (void)dir;
    return probe;
#else
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    const std::filesystem::path probe_path = dir / "chunkdb-sync-probe.tmp";

    const int fd = ::open(probe_path.string().c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        return probe;
    }

    std::vector<double> samples;
    samples.reserve(9);
    bool fell_back = false;
    for (int i = 0; i < 9; ++i) {
        const char byte = static_cast<char>('a' + i);
        if (::write(fd, &byte, 1) != 1) {
            break;
        }
        const auto started = Clock::now();
#if defined(__APPLE__)
        if (::fcntl(fd, F_FULLFSYNC, 0) != 0) {
            fell_back = true;
            if (::fsync(fd) != 0) {
                break;
            }
        }
#else
        if (::fdatasync(fd) != 0) {
            break;
        }
#endif
        samples.push_back(
            std::chrono::duration<double, std::micro>(Clock::now() - started).count());
    }
    ::close(fd);
    std::filesystem::remove(probe_path, ec);

    if (samples.empty()) {
        return probe;
    }
    std::sort(samples.begin(), samples.end());
    probe.median_us = samples[samples.size() / 2];
#if defined(__APPLE__)
    probe.kind = fell_back ? "fullfsync-fallback-fsync" : "fullfsync";
#else
    (void)fell_back;
    probe.kind = "fdatasync";
#endif
    return probe;
#endif
}

// ---------------------------------------------------------------------------
// Arguments.
// ---------------------------------------------------------------------------

struct Args {
    std::size_t chunks = 5000;
    std::size_t cache = 16384;
    unsigned threads = 1;
    std::size_t repeats = 5;
    std::filesystem::path data_dir;
    chunkdb::DurabilityMode durability = chunkdb::DurabilityMode::kRelaxed;
    std::string durability_name = "relaxed";
    bool csv = false;
    bool close_store = false;
    bool keep_data = false;
    chunkdb::LogLevel log_level = chunkdb::LogLevel::kWarn;
};

constexpr char kUsage[] =
    "Usage: chunkdb_large_world_bench [options]\n"
    "  --chunks N       fresh chunks written inside the measured window (default 5000)\n"
    "  --cache N        max_loaded_chunks; also the size of the unmeasured warm-up (default 16384)\n"
    "  --threads N      concurrent writers in the measured window (default 1)\n"
    "  --repeats N      independent repeats, each on a fresh store (default 5)\n"
    "  --data-dir PATH  parent directory for the per-repeat data dirs; use real\n"
    "                   device-backed storage, not tmpfs (default: temp dir)\n"
    "  --durability M   relaxed | fsync-wal | fsync-checkpoint (default relaxed)\n"
    "  --output MODE    human | csv (default human)\n"
    "  --close-store    also close the store per repeat (adds O(cache) flushes)\n"
    "  --log-level M    info | warn | error (default warn; store logs go to stdout)\n"
    "  --keep-data      do not delete the data directories afterwards\n";

Args ParseArgs(int argc, char** argv) {
    Args args;
    args.data_dir = std::filesystem::temp_directory_path() / "chunkdb-large-world-bench";

    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        auto require_value = [&](const char* name) {
            if (i + 1 >= argc) {
                throw std::invalid_argument(std::string("missing value for ") + name);
            }
            ++i;
            return std::string(argv[i]);
        };
        auto parse_size = [](const std::string& value, const char* name) {
            std::size_t consumed = 0;
            const auto parsed = std::stoull(value, &consumed, 10);
            if (consumed != value.size() || parsed == 0) {
                throw std::invalid_argument(std::string("invalid ") + name + " value: " + value);
            }
            return static_cast<std::size_t>(parsed);
        };

        if (arg == "--chunks") {
            args.chunks = parse_size(require_value("--chunks"), "--chunks");
        } else if (arg == "--cache") {
            args.cache = parse_size(require_value("--cache"), "--cache");
        } else if (arg == "--threads") {
            args.threads = static_cast<unsigned>(parse_size(require_value("--threads"), "--threads"));
        } else if (arg == "--repeats") {
            args.repeats = parse_size(require_value("--repeats"), "--repeats");
        } else if (arg == "--data-dir") {
            args.data_dir = require_value("--data-dir");
        } else if (arg == "--durability") {
            const auto value = require_value("--durability");
            if (value == "relaxed") {
                args.durability = chunkdb::DurabilityMode::kRelaxed;
            } else if (value == "fsync-wal") {
                args.durability = chunkdb::DurabilityMode::kFsyncWal;
            } else if (value == "fsync-checkpoint") {
                args.durability = chunkdb::DurabilityMode::kFsyncCheckpoint;
            } else {
                throw std::invalid_argument("invalid --durability value: " + value);
            }
            args.durability_name = value;
        } else if (arg == "--output") {
            const auto value = require_value("--output");
            if (value == "csv") {
                args.csv = true;
            } else if (value == "human") {
                args.csv = false;
            } else {
                throw std::invalid_argument("invalid --output value: " + value);
            }
        } else if (arg == "--log-level") {
            args.log_level = chunkdb::ParseLogLevel(require_value("--log-level"));
        } else if (arg == "--close-store") {
            args.close_store = true;
        } else if (arg == "--keep-data") {
            args.keep_data = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout << kUsage;
            std::exit(0);
        } else {
            throw std::invalid_argument("unknown argument: " + std::string(arg));
        }
    }

    return args;
}

// ---------------------------------------------------------------------------
// Pinned store configuration. Keep in sync with the profile recorded in
// bench/artifacts/manual-runs/*-metadata.txt; changing any of these values
// invalidates comparisons with the committed artifacts.
// ---------------------------------------------------------------------------

chunkdb::StoreConfig BuildStoreConfig(
    const std::filesystem::path& data_dir,
    std::size_t cache,
    chunkdb::DurabilityMode durability) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 8,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = 16,
            .chunk_height_blocks = 16,
            .block_bits = 16,
        },
        .data_dir = data_dir,
        .durability_mode = durability,
        .checkpoint_update_interval = 256,
        .checkpoint_wal_bytes = 1024 * 1024,
        .wal_group_commit_updates = 8,
        .max_loaded_chunks = cache,
        .allow_multiple_processes = false,
        .background_maintenance = false,
    };
}

// Row-major chunk grid, matching the historical harnesses so the numbers stay
// comparable with the 2026-09-03 measurements.
constexpr std::int64_t kGridWidth = 1024;

std::pair<std::int64_t, std::int64_t> ChunkOf(std::size_t index) {
    return {
        static_cast<std::int64_t>(index % kGridWidth),
        static_cast<std::int64_t>(index / kGridWidth),
    };
}

struct RepeatResult {
    std::size_t run = 0;
    double seconds = 0.0;
    double ops_per_sec = 0.0;
    std::uint64_t evictions = 0;
    std::uint64_t eviction_forced_wal_flushes = 0;
    std::uint64_t wal_batch_flushes = 0;
    std::uint64_t checkpoints = 0;
    double s_per_eviction = 0.0;
    std::size_t loaded_chunks = 0;
    std::uint64_t rss_baseline_bytes = 0;
    std::uint64_t rss_current_bytes = 0;
    double rss_bytes_per_resident_chunk = 0.0;
    std::uint64_t rss_after_warmup_bytes = 0;
    std::size_t loaded_after_warmup = 0;
    double rss_bytes_per_warm_chunk = 0.0;
    double warmup_seconds = 0.0;
    double close_seconds = 0.0;
};

RepeatResult RunRepeat(const Args& args, std::size_t run_index) {
    const std::filesystem::path dir =
        args.data_dir / ("run-" + std::to_string(run_index) + "-t" + std::to_string(args.threads));

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    std::filesystem::create_directories(dir, ec);

    RepeatResult result;
    result.run = run_index;
    result.rss_baseline_bytes = CurrentRssBytes();

    // The store is intentionally released rather than destroyed unless
    // --close-store is given; see the file header.
    auto store = std::make_unique<chunkdb::ChunkStore>(
        BuildStoreConfig(dir, args.cache, args.durability));

    const std::string bits = "1111000011110000";

    // ---- warm-up: fill the cache to exactly `cache` resident chunks -------
    // Not measured. These writes hit no eviction, so including them is what
    // dilutes an averaged sparse throughput number.
    const auto warmup_started = Clock::now();
    for (std::size_t i = 0; i < args.cache; ++i) {
        const auto [cx, cy] = ChunkOf(i);
        store->SetBlockBits(cx * 16, cy * 16, bits);
    }
    result.warmup_seconds = Secs(warmup_started, Clock::now());

    // Cache is now exactly full and no chunk has been evicted yet: this is the
    // cleanest point to price a resident chunk, before eviction churn adds
    // freed-but-still-mapped heap to the resident set.
    result.loaded_after_warmup = store->ApproxLoadedChunkCount();
    result.rss_after_warmup_bytes = CurrentRssBytes();
    if (result.loaded_after_warmup != 0 &&
        result.rss_after_warmup_bytes > result.rss_baseline_bytes) {
        result.rss_bytes_per_warm_chunk =
            static_cast<double>(result.rss_after_warmup_bytes - result.rss_baseline_bytes) /
            static_cast<double>(result.loaded_after_warmup);
    }

    // ---- measured window: every fresh chunk now costs one eviction -------
    const chunkdb::StoreRuntimeStats before = store->RuntimeStats();
    std::atomic<std::size_t> next_index{args.cache};
    const std::size_t end_index = args.cache + args.chunks;

    const auto started = Clock::now();
    {
        std::vector<std::thread> pool;
        pool.reserve(args.threads);
        for (unsigned t = 0; t < args.threads; ++t) {
            pool.emplace_back([&]() {
                for (;;) {
                    const std::size_t i = next_index.fetch_add(1, std::memory_order_relaxed);
                    if (i >= end_index) {
                        return;
                    }
                    const auto [cx, cy] = ChunkOf(i);
                    store->SetBlockBits(cx * 16, cy * 16, bits);
                }
            });
        }
        for (auto& thread : pool) {
            thread.join();
        }
    }
    result.seconds = Secs(started, Clock::now());

    const chunkdb::StoreRuntimeStats after = store->RuntimeStats();
    result.evictions = after.evictions - before.evictions;
    result.eviction_forced_wal_flushes =
        after.eviction_forced_wal_flushes - before.eviction_forced_wal_flushes;
    result.wal_batch_flushes = after.wal_batch_flushes - before.wal_batch_flushes;
    result.checkpoints = after.checkpoints - before.checkpoints;
    result.ops_per_sec =
        result.seconds == 0.0 ? 0.0 : static_cast<double>(args.chunks) / result.seconds;
    result.s_per_eviction =
        result.evictions == 0 ? 0.0 : result.seconds / static_cast<double>(result.evictions);

    result.loaded_chunks = store->ApproxLoadedChunkCount();
    result.rss_current_bytes = CurrentRssBytes();
    if (result.loaded_chunks != 0 && result.rss_current_bytes > result.rss_baseline_bytes) {
        result.rss_bytes_per_resident_chunk =
            static_cast<double>(result.rss_current_bytes - result.rss_baseline_bytes) /
            static_cast<double>(result.loaded_chunks);
    }

    if (args.close_store) {
        const auto close_started = Clock::now();
        store.reset();
        result.close_seconds = Secs(close_started, Clock::now());
    } else {
        (void)store.release();
    }

    if (!args.keep_data) {
        std::filesystem::remove_all(dir, ec);
    }
    return result;
}

double Mean(const std::vector<double>& values) {
    if (values.empty()) {
        return 0.0;
    }
    return std::accumulate(values.begin(), values.end(), 0.0) / static_cast<double>(values.size());
}

double StdDev(const std::vector<double>& values) {
    if (values.size() < 2) {
        return 0.0;
    }
    const double mean = Mean(values);
    double sum = 0.0;
    for (const double value : values) {
        sum += (value - mean) * (value - mean);
    }
    return std::sqrt(sum / static_cast<double>(values.size() - 1));
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Args args = ParseArgs(argc, argv);
        chunkdb::SetLogLevel(args.log_level);

        std::error_code ec;
        std::filesystem::create_directories(args.data_dir, ec);

        const std::string fs_name = FilesystemName(args.data_dir);
        const SyncProbe sync_probe = ProbeDurableSync(args.data_dir);

        if (!args.csv) {
            std::cout << "profile"
                      << " geometry=8x8x16x16x16"
                      << " durability=" << args.durability_name
                      << " max_loaded_chunks=" << args.cache
                      << " wal_group_commit_updates=8"
                      << " checkpoint_update_interval=256"
                      << " checkpoint_wal_bytes=1048576"
                      << " background_maintenance=off"
                      << " threads=" << args.threads
                      << " measured_chunks=" << args.chunks
                      << " repeats=" << args.repeats
                      << " close_store=" << (args.close_store ? 1 : 0)
                      << '\n'
                      << "host data_dir=" << args.data_dir
                      << " fs=" << fs_name
                      << " durable_sync=" << sync_probe.kind
                      << " durable_sync_median_us=" << std::fixed << std::setprecision(1)
                      << sync_probe.median_us
                      << '\n' << std::flush;
        } else {
            std::cout
                << "run,threads,cache,measured_chunks,durability,total_s,ops_s,evictions,"
                   "eviction_forced_wal_flushes,wal_batch_flushes,checkpoints,s_per_eviction,"
                   "ms_per_eviction,loaded_chunks,rss_baseline_bytes,rss_current_bytes,"
                   "rss_bytes_per_resident_chunk,loaded_after_warmup,rss_after_warmup_bytes,"
                   "rss_bytes_per_warm_chunk,warmup_s,close_s,fs,durable_sync,"
                   "durable_sync_median_us\n"
                << std::flush;
        }

        std::vector<double> ops_s_values;
        std::vector<double> s_per_eviction_values;
        std::vector<double> rss_per_chunk_values;
        std::vector<double> rss_per_warm_chunk_values;
        ops_s_values.reserve(args.repeats);
        s_per_eviction_values.reserve(args.repeats);
        rss_per_chunk_values.reserve(args.repeats);
        rss_per_warm_chunk_values.reserve(args.repeats);

        for (std::size_t run = 1; run <= args.repeats; ++run) {
            const RepeatResult r = RunRepeat(args, run);
            ops_s_values.push_back(r.ops_per_sec);
            s_per_eviction_values.push_back(r.s_per_eviction);
            rss_per_chunk_values.push_back(r.rss_bytes_per_resident_chunk);
            rss_per_warm_chunk_values.push_back(r.rss_bytes_per_warm_chunk);

            if (args.csv) {
                std::cout << r.run
                          << ',' << args.threads
                          << ',' << args.cache
                          << ',' << args.chunks
                          << ',' << args.durability_name
                          << ',' << std::fixed << std::setprecision(4) << r.seconds
                          << ',' << std::setprecision(2) << r.ops_per_sec
                          << ',' << r.evictions
                          << ',' << r.eviction_forced_wal_flushes
                          << ',' << r.wal_batch_flushes
                          << ',' << r.checkpoints
                          << ',' << std::setprecision(6) << r.s_per_eviction
                          << ',' << std::setprecision(3) << (r.s_per_eviction * 1000.0)
                          << ',' << r.loaded_chunks
                          << ',' << r.rss_baseline_bytes
                          << ',' << r.rss_current_bytes
                          << ',' << std::setprecision(1) << r.rss_bytes_per_resident_chunk
                          << ',' << r.loaded_after_warmup
                          << ',' << r.rss_after_warmup_bytes
                          << ',' << std::setprecision(1) << r.rss_bytes_per_warm_chunk
                          << ',' << std::setprecision(3) << r.warmup_seconds
                          << ',' << std::setprecision(3) << r.close_seconds
                          << ',' << fs_name
                          << ',' << sync_probe.kind
                          << ',' << std::setprecision(1) << sync_probe.median_us
                          << '\n' << std::flush;
            } else {
                std::cout << "run=" << r.run
                          << " threads=" << args.threads
                          << " total_s=" << std::fixed << std::setprecision(2) << r.seconds
                          << " ops_s=" << std::setprecision(1) << r.ops_per_sec
                          << " evictions=" << r.evictions
                          << " eviction_forced_wal_flushes=" << r.eviction_forced_wal_flushes
                          << " wal_batch_flushes=" << r.wal_batch_flushes
                          << " checkpoints=" << r.checkpoints
                          << " ms_per_eviction=" << std::setprecision(3)
                          << (r.s_per_eviction * 1000.0)
                          << " loaded=" << r.loaded_chunks
                          << " rss_mb=" << std::setprecision(1)
                          << static_cast<double>(r.rss_current_bytes) / (1024.0 * 1024.0)
                          << " rss_b_per_chunk=" << std::setprecision(0)
                          << r.rss_bytes_per_resident_chunk
                          << " rss_b_per_warm_chunk=" << std::setprecision(0)
                          << r.rss_bytes_per_warm_chunk
                          << " warmup_s=" << std::setprecision(2) << r.warmup_seconds
                          << " close_s=" << std::setprecision(2) << r.close_seconds
                          << '\n' << std::flush;
            }
        }

        if (!args.csv) {
            std::cout << "summary"
                      << " runs=" << args.repeats
                      << " threads=" << args.threads
                      << " avg_ops_s=" << std::fixed << std::setprecision(1) << Mean(ops_s_values)
                      << " stddev_ops_s=" << std::setprecision(1) << StdDev(ops_s_values)
                      << " avg_ms_per_eviction=" << std::setprecision(3)
                      << (Mean(s_per_eviction_values) * 1000.0)
                      << " stddev_ms_per_eviction=" << std::setprecision(3)
                      << (StdDev(s_per_eviction_values) * 1000.0)
                      << " avg_rss_b_per_chunk=" << std::setprecision(0)
                      << Mean(rss_per_chunk_values)
                      << " avg_rss_b_per_warm_chunk=" << std::setprecision(0)
                      << Mean(rss_per_warm_chunk_values)
                      << '\n' << std::flush;
        }

        if (!args.keep_data) {
            std::filesystem::remove_all(args.data_dir, ec);
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "large_world_bench error: " << error.what() << '\n' << kUsage;
        return 1;
    }
}
