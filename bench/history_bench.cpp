// Block history costs against the budgets of docs/HISTORY_DESIGN.md: write
// throughput and checkpoint latency with and without history, bytes per
// event, and read latency of history pages and reads AT a past revision.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/logging.hpp"

namespace {

using Clock = std::chrono::steady_clock;

struct Args {
    std::size_t writes = 200000;
    std::size_t checkpoints = 200;
    std::size_t reads = 2000;
    std::filesystem::path dir;
    std::string durability = "relaxed";
};

double Percentile(std::vector<double> values, double p) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const auto rank = static_cast<std::size_t>(p / 100.0 * static_cast<double>(values.size() - 1));
    return values[rank];
}

chunkdb::StoreConfig Config(const std::filesystem::path& dir, const Args& args, bool history) {
    chunkdb::StoreConfig config;
    config.data_dir = dir;
    config.geometry = chunkdb::GeometryConfig{};  // 16x16 blocks of 16 bits, 8x8 chunks
    config.durability_mode = chunkdb::ParseDurabilityMode(args.durability);
    config.history = history;
    return config;
}

std::string Bits(std::mt19937_64& rng) {
    std::string bits(16, '0');
    for (auto& bit : bits) {
        bit = (rng() & 1U) != 0U ? '1' : '0';
    }
    return bits;
}

std::uint64_t DirectoryBytes(const std::filesystem::path& dir) {
    std::uint64_t bytes = 0;
    if (std::filesystem::exists(dir)) {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(dir)) {
            if (entry.is_regular_file()) {
                bytes += entry.file_size();
            }
        }
    }
    return bytes;
}

void Row(std::string_view name, double value, std::string_view unit) {
    std::cout << std::left << std::setw(44) << name << std::right << std::setw(14) << std::fixed
              << std::setprecision(3) << value << " " << unit << "\n";
}

// Point writes that change state on 16 hot chunks, checkpoints at the
// default interval: ops/s.
double WriteThroughput(const std::filesystem::path& dir, const Args& args, bool history) {
    std::filesystem::remove_all(dir);
    chunkdb::ChunkStore store(Config(dir, args, history));
    std::mt19937_64 rng(7);
    const auto started = Clock::now();
    for (std::size_t i = 0; i < args.writes; ++i) {
        store.SetBlockBits(static_cast<std::int64_t>(rng() % 64), static_cast<std::int64_t>(rng() % 64), Bits(rng));
    }
    return static_cast<double>(args.writes) / std::chrono::duration<double>(Clock::now() - started).count();
}

// A checkpoint after 256 point writes to one chunk: milliseconds.
std::vector<double> CheckpointLatency(const std::filesystem::path& dir, const Args& args, bool history) {
    std::filesystem::remove_all(dir);
    auto config = Config(dir, args, history);
    config.checkpoint_update_interval = 1U << 30U;
    chunkdb::ChunkStore store(config);
    std::mt19937_64 rng(11);
    std::vector<double> ms;
    for (std::size_t round = 0; round < args.checkpoints; ++round) {
        for (int i = 0; i < 256; ++i) {
            store.SetBlockBits(static_cast<std::int64_t>(rng() % 16), static_cast<std::int64_t>(rng() % 16), Bits(rng));
        }
        const auto started = Clock::now();
        store.CheckpointForTests(0, 0);
        ms.push_back(std::chrono::duration<double, std::milli>(Clock::now() - started).count());
    }
    return ms;
}

}  // namespace

int main(int argc, char** argv) {
    Args args;
    args.dir = std::filesystem::temp_directory_path() /
               ("chunkdb-history-bench-" + std::to_string(Clock::now().time_since_epoch().count()));
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        const auto value = [&]() -> std::string {
            if (i + 1 >= argc) {
                throw std::invalid_argument("missing value for " + std::string(arg));
            }
            return argv[++i];
        };
        if (arg == "--writes") {
            args.writes = std::stoull(value());
        } else if (arg == "--checkpoints") {
            args.checkpoints = std::stoull(value());
        } else if (arg == "--reads") {
            args.reads = std::stoull(value());
        } else if (arg == "--durability-mode") {
            args.durability = value();
        } else if (arg == "--data-dir") {
            args.dir = value();
        } else {
            std::cout << "Usage: chunkdb_history_bench [--writes N] [--checkpoints N] [--reads N] "
                         "[--durability-mode relaxed|fsync-wal|fsync-checkpoint] [--data-dir PATH]\n";
            return arg == "--help" || arg == "-h" ? 0 : 2;
        }
    }
    chunkdb::SetLogLevel(chunkdb::LogLevel::kError);
    std::cout << "chunkdb history benchmark durability_mode=" << args.durability << " writes=" << args.writes
              << " checkpoints=" << args.checkpoints << " reads=" << args.reads << "\n";

    const auto plain_dir = args.dir / "plain";
    const auto history_dir = args.dir / "history";
    const double plain_ops = WriteThroughput(plain_dir, args, false);
    const double history_ops = WriteThroughput(history_dir, args, true);
    Row("point_writes_without_history", plain_ops, "ops/s");
    Row("point_writes_with_history", history_ops, "ops/s");
    Row("point_writes_change", (history_ops / plain_ops - 1.0) * 100.0, "%");

    const auto plain_checkpoints = CheckpointLatency(plain_dir, args, false);
    const auto history_checkpoints = CheckpointLatency(history_dir, args, true);
    Row("checkpoint_without_history_p50", Percentile(plain_checkpoints, 50), "ms");
    Row("checkpoint_without_history_p99", Percentile(plain_checkpoints, 99), "ms");
    Row("checkpoint_with_history_p50", Percentile(history_checkpoints, 50), "ms");
    Row("checkpoint_with_history_p99", Percentile(history_checkpoints, 99), "ms");

    // The history the checkpoint scenario wrote: bytes per event.
    {
        chunkdb::ChunkStore store(Config(history_dir, args, true));
        const auto bytes = DirectoryBytes(history_dir / "history");
        const double events = static_cast<double>(args.checkpoints) * 256.0;
        Row("history_bytes_per_event_upper_bound", static_cast<double>(bytes) / events, "B");

        // Reads on that chunk: newest page, one block's page, a page from
        // a random place, and the chunk AT a random revision.
        const auto all = store.ReadHistory({.descending = false, .limit = 1});
        const std::uint64_t first = all.events.empty() ? 1 : all.events.front().revision;
        const std::uint64_t last = store.GetChunkVersion(0, 0);
        std::mt19937_64 rng(13);
        std::vector<double> newest;
        std::vector<double> block;
        std::vector<double> middle;
        std::vector<double> at;
        std::size_t events_read = 0;
        for (std::size_t i = 0; i < args.reads; ++i) {
            auto started = Clock::now();
            events_read += store.ReadHistory({.limit = 100}).events.size();
            newest.push_back(std::chrono::duration<double, std::milli>(Clock::now() - started).count());

            started = Clock::now();
            events_read += store.ReadHistory({.block_index = static_cast<std::uint32_t>(rng() % 256), .limit = 100}).events.size();
            block.push_back(std::chrono::duration<double, std::milli>(Clock::now() - started).count());

            const std::uint64_t cursor = first + rng() % (last - first + 1U);
            started = Clock::now();
            events_read += store.ReadHistory({.descending = false, .limit = 100, .after = chunkdb::HistoryCursor{.revision = cursor}}).events.size();
            middle.push_back(std::chrono::duration<double, std::milli>(Clock::now() - started).count());

            started = Clock::now();
            (void)store.ReadChunkAt(0, 0, {.revision = first + rng() % (last - first + 1U)});
            at.push_back(std::chrono::duration<double, std::milli>(Clock::now() - started).count());
        }
        Row("history_newest_limit100_p50", Percentile(newest, 50), "ms");
        Row("history_newest_limit100_p99", Percentile(newest, 99), "ms");
        Row("history_one_block_limit100_p99", Percentile(block, 99), "ms");
        Row("history_random_cursor_limit100_p99", Percentile(middle, 99), "ms");
        Row("chunk_at_random_revision_p50", Percentile(at, 50), "ms");
        Row("chunk_at_random_revision_p99", Percentile(at, 99), "ms");
        Row("events_read", static_cast<double>(events_read), "");
    }
    std::filesystem::remove_all(args.dir);
    return 0;
}
