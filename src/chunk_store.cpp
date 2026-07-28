#include "chunkdb/chunk_store.hpp"

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "eviction.hpp"
#include "process_lock.hpp"
#include "wal_replay.hpp"
#include "wal_stream_pool.hpp"
#include "wal_writer.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <vector>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/zrle.hpp"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <stdio.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <unistd.h>
#endif

namespace chunkdb {

[[nodiscard]] std::string CanonicalPathKey(const std::filesystem::path& path) {
    std::error_code ec;
    const auto canonical = std::filesystem::weakly_canonical(path, ec);
    if (!ec) {
        return canonical.lexically_normal().string();
    }
    ec.clear();
    const auto absolute = std::filesystem::absolute(path, ec);
    if (!ec) {
        return absolute.lexically_normal().string();
    }
    return path.lexically_normal().string();
}


namespace {
// Maximum number of files examined during the startup scan. The scan is
// informational only (results appear in the startup log message); it does not
// affect correctness. Capping it keeps startup latency bounded on large worlds
// that contain hundreds of thousands of chunk files.
constexpr std::uint64_t kStartupScanFileLimit = 100'000;

struct StartupRecoveryScan {
    std::uint64_t wal_files = 0;
    std::uint64_t checkpoint_files = 0;
    std::uint64_t region_files = 0;
    bool scan_capped = false;
};

StartupRecoveryScan ScanStartupRecovery(const std::filesystem::path& data_dir) {
    StartupRecoveryScan result;
    std::error_code exists_ec;
    if (!std::filesystem::exists(data_dir, exists_ec) || exists_ec) {
        return result;
    }

    std::uint64_t examined = 0;
    const std::filesystem::recursive_directory_iterator end;
    std::error_code it_ec;
    for (std::filesystem::recursive_directory_iterator it(data_dir, it_ec);
         it != end && !it_ec;
         it.increment(it_ec)) {
        if (!it->is_regular_file()) {
            continue;
        }
        if (examined >= kStartupScanFileLimit) {
            result.scan_capped = true;
            break;
        }
        ++examined;
        const auto ext = it->path().extension();
        if (ext == ".wal") {
            ++result.wal_files;
        } else if (ext == ".chk") {
            ++result.checkpoint_files;
        } else if (ext == ".rgn") {
            ++result.region_files;
        }
    }
    return result;
}
}  // namespace

std::uint64_t CurrentProcessIdValue() {
#ifdef _WIN32
    return static_cast<std::uint64_t>(GetCurrentProcessId());
#else
    return static_cast<std::uint64_t>(::getpid());
#endif
}

std::uint64_t UnixMillisNow() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

std::int64_t FloorDiv(std::int64_t value, std::int64_t divisor) {
    if (divisor <= 0) {
        throw std::invalid_argument("divisor must be > 0");
    }
    std::int64_t q = value / divisor;
    const std::int64_t r = value % divisor;
    if (r != 0 && ((r < 0) != (divisor < 0))) {
        --q;
    }
    return q;
}

bool ConsumeFailpointEnv(const char* key) {
    const char* value = std::getenv(key);
    if (value == nullptr || value[0] == '\0') {
        return false;
    }
#ifdef _WIN32
    (void)_putenv_s(key, "");
#else
    (void)unsetenv(key);
#endif
    return true;
}

[[nodiscard]] std::chrono::milliseconds ConsumeFailpointDelayMs(const char* key) {
    const char* value = std::getenv(key);
    if (value == nullptr || value[0] == '\0') {
        return std::chrono::milliseconds(0);
    }

    std::uint64_t delay_ms = 0;
    try {
        std::size_t consumed = 0;
        delay_ms = static_cast<std::uint64_t>(std::stoull(value, &consumed, 10));
        if (consumed != std::strlen(value)) {
            delay_ms = 0;
        }
    } catch (...) {
        delay_ms = 0;
    }
#ifdef _WIN32
    (void)_putenv_s(key, "");
#else
    (void)unsetenv(key);
#endif
    return std::chrono::milliseconds(delay_ms);
}


DurabilityMode ParseDurabilityMode(std::string_view text) {
    if (text == "relaxed") {
        return DurabilityMode::kRelaxed;
    }
    if (text == "fsync-wal") {
        return DurabilityMode::kFsyncWal;
    }
    if (text == "fsync-checkpoint") {
        return DurabilityMode::kFsyncCheckpoint;
    }
    throw std::invalid_argument(
        "invalid durability mode: " + std::string(text) +
        " (expected relaxed|fsync-wal|fsync-checkpoint)");
}

const char* DurabilityModeName(DurabilityMode mode) noexcept {
    switch (mode) {
        case DurabilityMode::kRelaxed:
            return "relaxed";
        case DurabilityMode::kFsyncWal:
            return "fsync-wal";
        case DurabilityMode::kFsyncCheckpoint:
            return "fsync-checkpoint";
    }
    return "unknown";
}

const char* AccessModeName(AccessMode mode) noexcept {
    switch (mode) {
        case AccessMode::kReadWrite:
            return "read-write";
        case AccessMode::kReadOnly:
            return "read-only";
    }
    return "unknown";
}

StorageLayoutMode ParseStorageLayoutMode(std::string_view text) {
    if (text == "fs_split_v1") {
        return StorageLayoutMode::kFsSplitV1;
    }
    if (text == "fs_region_v1") {
        return StorageLayoutMode::kFsRegionV1Experimental;
    }
    throw std::invalid_argument(
        "invalid storage layout mode: " + std::string(text) +
        " (expected fs_split_v1|fs_region_v1)");
}

const char* StorageLayoutModeName(StorageLayoutMode mode) noexcept {
    switch (mode) {
        case StorageLayoutMode::kFsSplitV1:
            return "fs_split_v1";
        case StorageLayoutMode::kFsRegionV1Experimental:
            return "fs_region_v1";
    }
    return "unknown";
}

CheckpointCompression ParseCheckpointCompression(std::string_view text) {
    if (text == "none") {
        return CheckpointCompression::kNone;
    }
    if (text == "zrle") {
        return CheckpointCompression::kZrle;
    }
    throw std::invalid_argument(
        "invalid checkpoint compression: " + std::string(text) + " (expected none|zrle)");
}

const char* CheckpointCompressionName(CheckpointCompression compression) noexcept {
    switch (compression) {
        case CheckpointCompression::kNone:
            return "none";
        case CheckpointCompression::kZrle:
            return "zrle";
    }
    return "unknown";
}

ChunkStore::ChunkStore(StoreConfig config)
    : geometry_(config.geometry),
      data_dir_(std::move(config.data_dir)),
      durability_mode_(config.durability_mode),
      access_mode_(config.access_mode),
      storage_layout_mode_(config.storage_layout_mode),
      experimental_region_span_chunks_(config.experimental_region_span_chunks),
      checkpoint_update_interval_(config.checkpoint_update_interval),
      checkpoint_wal_bytes_(config.checkpoint_wal_bytes),
      wal_group_commit_updates_(config.wal_group_commit_updates),
      max_loaded_chunks_(config.max_loaded_chunks),
      max_open_wal_streams_(config.max_open_wal_streams),
      checkpoint_compression_(config.checkpoint_compression),
      background_maintenance_(config.background_maintenance),
      background_checkpoint_queue_limit_(config.background_checkpoint_queue_limit) {
    if (data_dir_.empty()) {
        throw std::invalid_argument("data_dir must not be empty");
    }
    if (checkpoint_update_interval_ == 0) {
        throw std::invalid_argument("checkpoint_update_interval must be > 0");
    }
    if (checkpoint_wal_bytes_ == 0) {
        throw std::invalid_argument("checkpoint_wal_bytes must be > 0");
    }
    if (wal_group_commit_updates_ == 0) {
        throw std::invalid_argument("wal_group_commit_updates must be > 0");
    }
    if (max_loaded_chunks_ == 0) {
        throw std::invalid_argument("max_loaded_chunks must be > 0");
    }
    if (max_open_wal_streams_ == 0) {
        throw std::invalid_argument("max_open_wal_streams must be > 0");
    }
    if (experimental_region_span_chunks_ == 0) {
        throw std::invalid_argument("experimental_region_span_chunks must be > 0");
    }
    if (experimental_region_span_chunks_ > 64) {
        throw std::invalid_argument("experimental_region_span_chunks must be <= 64");
    }
    if (background_maintenance_ && background_checkpoint_queue_limit_ == 0) {
        throw std::invalid_argument("background_checkpoint_queue_limit must be > 0");
    }

#ifndef _WIN32
    {
        struct rlimit limit {};
        if (getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur != RLIM_INFINITY) {
            const std::size_t soft_limit = static_cast<std::size_t>(limit.rlim_cur);
            const std::size_t clamped =
                soft_limit > kWalOpenStreamsFdReserve
                    ? (soft_limit - kWalOpenStreamsFdReserve)
                    : 1U;
            if (max_open_wal_streams_ > clamped) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kStore,
                    "max_open_wal_streams clamped by RLIMIT_NOFILE reserve",
                    {
                        {"configured", std::to_string(max_open_wal_streams_)},
                        {"effective", std::to_string(clamped)},
                        {"rlimit_nofile_soft", std::to_string(soft_limit)},
                        {"reserve", std::to_string(kWalOpenStreamsFdReserve)},
                    });
                max_open_wal_streams_ = clamped;
            }
        }
    }
#else
    {
        // Windows has no RLIMIT_NOFILE, but the C runtime caps the number of
        // simultaneously open stdio streams (_getmaxstdio, default 512). The
        // WAL stream pool can keep up to max_open_wal_streams files open, so
        // without this an open-heavy workload hits EMFILE ("Too many open
        // files"). Raise the CRT limit toward its maximum, then clamp the WAL
        // pool to the effective limit minus a reserve for other handles.
        constexpr int kWindowsStdioTarget = 8192;  // CRT hard maximum
        if (_getmaxstdio() < kWindowsStdioTarget) {
            (void)_setmaxstdio(kWindowsStdioTarget);
        }
        const int effective_stdio = _getmaxstdio();
        if (effective_stdio > 0) {
            const std::size_t budget = static_cast<std::size_t>(effective_stdio);
            const std::size_t clamped =
                budget > kWalOpenStreamsFdReserve
                    ? (budget - kWalOpenStreamsFdReserve)
                    : 1U;
            if (max_open_wal_streams_ > clamped) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kStore,
                    "max_open_wal_streams clamped by Windows CRT stdio limit",
                    {
                        {"configured", std::to_string(max_open_wal_streams_)},
                        {"effective", std::to_string(clamped)},
                        {"crt_maxstdio", std::to_string(effective_stdio)},
                        {"reserve", std::to_string(kWalOpenStreamsFdReserve)},
                    });
                max_open_wal_streams_ = clamped;
            }
        }
    }
#endif

    const auto recovery_start = std::chrono::steady_clock::now();
    const auto startup_scan = ScanStartupRecovery(data_dir_);

    // Decide, before this process creates any artifact, whether the store was
    // already initialized. The explicit initialized marker is written only
    // after the first valid version record. A lock directory alone is not
    // sufficient evidence because lock bootstrap precedes version
    // initialization and may survive a crash or failed constructor.
    bool store_preexisting =
        startup_scan.wal_files > 0 || startup_scan.checkpoint_files > 0 ||
        startup_scan.region_files > 0 || startup_scan.scan_capped;
    {
        std::error_code marker_ec;
        if (std::filesystem::exists(data_dir_ / "chunkdb.version", marker_ec) || marker_ec) {
            store_preexisting = true;
        }
        std::error_code initialized_ec;
        if (std::filesystem::exists(
                data_dir_ / ".chunkdb.initialized", initialized_ec) ||
            initialized_ec) {
            store_preexisting = true;
        }
    }

    if (access_mode_ == AccessMode::kReadWrite) {
        std::filesystem::create_directories(data_dir_);
    } else {
        std::error_code data_dir_ec;
        const auto data_dir_status =
            std::filesystem::status(data_dir_, data_dir_ec);
        if (data_dir_ec ||
            !std::filesystem::is_directory(data_dir_status)) {
            throw std::runtime_error(
                "read-only data directory is unavailable: " +
                data_dir_.string() +
                (data_dir_ec
                     ? " (" + data_dir_ec.message() + ")"
                     : std::string()));
        }
    }
    AcquireProcessLock(config.allow_multiple_processes);
    try {
        InitializeSnapshotGeneration(store_preexisting);
        InitializeVersionClock(store_preexisting);
        if (access_mode_ == AccessMode::kReadWrite && store_preexisting) {
            // Persist the barrier durability floor conservatively across
            // restart without another fragile bookkeeping file: every
            // previously initialized store uses durable checkpoint
            // replacement from this point forward.
            barrier_durability_floor_.store(true, std::memory_order_release);
        }
        RecoverConditionalRollbackIntents();
        FinishSnapshotGenerationRecovery();
    } catch (...) {
        // AcquireProcessLock starts the metadata heartbeat. A throwing
        // constructor does not run ChunkStore's destructor, so release the
        // lock explicitly before unwinding (otherwise std::thread destruction
        // would terminate the process).
        ReleaseProcessLock();
        throw;
    }

    const auto recovery_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - recovery_start);
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kRecovery,
        "startup recovery summary",
        {
            {"checkpoint_files", std::to_string(startup_scan.checkpoint_files)},
            {"region_files", std::to_string(startup_scan.region_files)},
            {"wal_files", std::to_string(startup_scan.wal_files)},
            {"scan_capped", startup_scan.scan_capped ? "true" : "false"},
            {"replay_mode", "lazy-on-load"},
            {"elapsed_ms", std::to_string(recovery_elapsed_ms.count())},
        });
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kStore,
        "store initialized",
        {
            {"data_dir", data_dir_.string()},
            {"durability_mode", DurabilityModeName(durability_mode_)},
            {"access_mode", AccessModeName(access_mode_)},
            {"storage_layout_mode", StorageLayoutModeName(storage_layout_mode_)},
            {"max_loaded_chunks", std::to_string(max_loaded_chunks_)},
            {"max_open_wal_streams", std::to_string(max_open_wal_streams_)},
            {"background_maintenance", background_maintenance_ ? "on" : "off"},
        });

    if (background_maintenance_ && access_mode_ != AccessMode::kReadOnly) {
        StartMaintenanceThread();
    }
}

ChunkStore::~ChunkStore() {
    StopMaintenanceThread();
    FlushAllPendingWalBatches();
    ReleaseProcessLock();
}

std::size_t ChunkStore::ApproxLoadedChunkCount() const {
    std::size_t loaded = 0;
    std::lock_guard global_lock(large_chunks_mutex_);
    for (const auto& [_, large_chunk] : large_chunks_) {
        std::lock_guard chunk_lock(large_chunk->mutex);
        loaded += large_chunk->chunks.size();
    }
    return loaded;
}

StoreRuntimeStats ChunkStore::RuntimeStats() const noexcept {
    const auto forced_with_data = stats_eviction_forced_wal_flushes_with_data_.load(std::memory_order_relaxed);
    const auto forced_empty = stats_eviction_forced_wal_flushes_empty_batch_.load(std::memory_order_relaxed);
    return StoreRuntimeStats{
        .evictions = stats_evictions_.load(std::memory_order_relaxed),
        .checkpoints = stats_checkpoints_.load(std::memory_order_relaxed),
        .wal_batch_flushes = stats_wal_batch_flushes_.load(std::memory_order_relaxed),
        .unique_loaded_chunks = stats_unique_loaded_chunks_.load(std::memory_order_relaxed),
        .open_wal_streams = stats_open_wal_streams_current_.load(std::memory_order_relaxed),
        .eviction_snapshot_builds = stats_eviction_snapshot_builds_.load(std::memory_order_relaxed),
        .eviction_probes = stats_eviction_probes_.load(std::memory_order_relaxed),
        .eviction_no_progress_cycles = stats_eviction_no_progress_cycles_.load(std::memory_order_relaxed),
        .eviction_forced_wal_flushes = forced_with_data + forced_empty,
        .eviction_forced_wal_flushes_with_data = forced_with_data,
        .eviction_forced_wal_flushes_empty_batch = forced_empty,
        .eviction_recency_skips = stats_eviction_recency_skips_.load(std::memory_order_relaxed),
        .empty_chunk_gcs = stats_empty_chunk_gcs_.load(std::memory_order_relaxed),
        .wal_barriers = stats_wal_barriers_.load(std::memory_order_relaxed),
        .wal_barrier_full_syncs = stats_wal_barrier_full_syncs_.load(std::memory_order_relaxed),
        .background_checkpoints = stats_background_checkpoints_.load(std::memory_order_relaxed),
        .background_checkpoint_failures =
            stats_background_checkpoint_failures_.load(std::memory_order_relaxed),
        .background_queue_full_inline =
            stats_background_queue_full_inline_.load(std::memory_order_relaxed),
        .background_queue_depth =
            [this]() -> std::uint64_t {
                std::lock_guard lock(maintenance_mutex_);
                return maintenance_checkpoint_queue_.size();
            }(),
        .compressed_checkpoint_images =
            stats_compressed_checkpoint_images_.load(std::memory_order_relaxed),
    };
}

std::uint64_t ChunkStore::WalOpenCountForTests() const noexcept {
    return stats_wal_open_count_.load(std::memory_order_relaxed);
}

std::uint64_t ChunkStore::WalParentPrepareCountForTests() const noexcept {
    return stats_wal_parent_prepare_calls_.load(std::memory_order_relaxed);
}

std::uint64_t ChunkStore::OpenWalStreamCountForTests() const noexcept {
    return stats_open_wal_streams_current_.load(std::memory_order_relaxed);
}

}  // namespace chunkdb
