// Options and data directories of chunkdb_chunk_map_bench.
#pragma once

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace chunkdb::chunk_map_bench {

inline constexpr std::size_t kSegmentOps = 1024;

struct Args {
    std::size_t chunks = 16384;
    std::size_t ops = 200 * kSegmentOps;
    std::size_t runs = 15;
    std::filesystem::path data_dir;
    bool keep_data = false;
    bool help = false;
};

inline constexpr char kUsage[] =
    "Usage: chunkdb_chunk_map_bench [options]\n"
    "  --chunks N       loaded chunks the many side spreads over, at least 2 (default 16384)\n"
    "  --ops N          operations per timed window, a multiple of 1024 (default 204800)\n"
    "  --runs N         runs, each on a fresh store (default 15)\n"
    "  --data-dir PATH  where the run-N store directories are made; a run-N that exists stops the benchmark,\n"
    "                   and PATH itself is removed only if the benchmark made it and it is empty at the end\n"
    "                   (default: a new directory under the temp dir)\n"
    "  --keep-data      do not delete the directories the benchmark made\n";

// A positive decimal number of digits only.
inline std::size_t ParseSize(const std::string& value, const char* name) {
    const auto invalid = [&] { return std::invalid_argument(std::string("invalid ") + name + " value: " + value); };
    if (value.empty() || value.front() < '0' || value.front() > '9') {
        throw invalid();
    }
    std::size_t consumed = 0;
    unsigned long long parsed = 0;
    try {
        parsed = std::stoull(value, &consumed, 10);
    } catch (const std::out_of_range&) {
        throw invalid();
    }
    if (consumed != value.size() || parsed == 0) {
        throw invalid();
    }
    return static_cast<std::size_t>(parsed);
}

// A directory name under the temp dir that concurrent runs do not share.
inline std::filesystem::path DefaultDataDir() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           ("chunkdb-chunk-map-bench-" + std::to_string(now) + "-" + std::to_string(std::random_device{}()));
}

inline Args ParseArgs(int argc, const char* const* argv) {
    Args args;
    bool data_dir_given = false;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        auto require_value = [&](const char* name) {
            if (i + 1 >= argc) {
                throw std::invalid_argument(std::string("missing value for ") + name);
            }
            ++i;
            return std::string(argv[i]);
        };
        if (arg == "--chunks") {
            args.chunks = ParseSize(require_value("--chunks"), "--chunks");
            if (args.chunks < 2 || args.chunks > (std::size_t{1} << 24)) {
                throw std::invalid_argument("--chunks must be between 2 and 16777216");
            }
        } else if (arg == "--ops") {
            args.ops = ParseSize(require_value("--ops"), "--ops");
            if (args.ops % kSegmentOps != 0) {
                throw std::invalid_argument("--ops must be a multiple of 1024");
            }
        } else if (arg == "--runs") {
            args.runs = ParseSize(require_value("--runs"), "--runs");
        } else if (arg == "--data-dir") {
            args.data_dir = require_value("--data-dir");
            if (args.data_dir.empty()) {
                throw std::invalid_argument("--data-dir must not be empty");
            }
            data_dir_given = true;
        } else if (arg == "--keep-data") {
            args.keep_data = true;
        } else if (arg == "--help" || arg == "-h") {
            args.help = true;
        } else {
            throw std::invalid_argument("unknown argument: " + std::string(arg));
        }
    }
    if (!data_dir_given) {
        args.data_dir = DefaultDataDir();
    }
    return args;
}

// The directory the run directories are made in.
// The benchmark deletes only what it made: each run's directory, and this
// directory if it made it and nothing else is in it at the end.
class DataDirs {
  public:
    DataDirs(std::filesystem::path parent, bool keep) : parent_(std::move(parent)), keep_(keep) {
        created_ = std::filesystem::create_directories(parent_);
    }
    ~DataDirs() {
        if (keep_ || !created_) {
            return;
        }
        std::error_code ec;
        if (std::filesystem::is_empty(parent_, ec) && !ec) {
            std::filesystem::remove(parent_, ec);
        }
        if (ec) {
            std::cerr << "warning: could not remove " << parent_.string() << ": " << ec.message() << '\n';
        }
    }
    DataDirs(const DataDirs&) = delete;
    DataDirs& operator=(const DataDirs&) = delete;

    [[nodiscard]] const std::filesystem::path& parent() const { return parent_; }
    [[nodiscard]] bool keep() const { return keep_; }

  private:
    std::filesystem::path parent_;
    bool keep_ = false;
    bool created_ = false;
};

// One run's store directory, which must not exist yet; deleted when the run
// ends, also on failure, unless the data is kept.
class RunDir {
  public:
    RunDir(const DataDirs& dirs, std::size_t run)
        : path_(dirs.parent() / ("run-" + std::to_string(run))), keep_(dirs.keep()) {
        if (std::filesystem::exists(std::filesystem::symlink_status(path_))) {
            throw std::runtime_error(path_.string() + " already exists; remove it or pass another --data-dir");
        }
    }
    ~RunDir() {
        if (keep_) {
            return;
        }
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
        if (ec) {
            std::cerr << "warning: could not remove " << path_.string() << ": " << ec.message() << '\n';
        }
    }
    RunDir(const RunDir&) = delete;
    RunDir& operator=(const RunDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const { return path_; }

  private:
    std::filesystem::path path_;
    bool keep_ = false;
};

}  // namespace chunkdb::chunk_map_bench
