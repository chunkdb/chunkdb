#include "chunkdb/chunk_store.hpp"
#include "feed_slots.hpp"

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "eviction.hpp"
#include "process_lock.hpp"
#include "store_manifest.hpp"
#include "backup.hpp"
#include "txn_history.hpp"
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
#include <optional>
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
        // Only chunkdb's own entries; others may not even be readable.
        if (it.depth() == 0 && !IsStoreEntryName(it->path().filename().string())) {
            it.disable_recursion_pending();
            continue;
        }
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
        }
    }
    return result;
}

// Returns the store manifest, or std::nullopt when `data_dir` may be
// initialized as a new store. Throws when it holds chunkdb state without a
// manifest, or when read-only mode finds no manifest.
std::optional<StoreManifest> ReadManifestOrRequireNewStore(
    const std::filesystem::path& data_dir,
    AccessMode access_mode) {
    auto manifest = ReadStoreManifest(data_dir);
    if (manifest.has_value()) {
        return manifest;
    }
    if (access_mode == AccessMode::kReadOnly) {
        throw std::runtime_error(
            "store directory " + data_dir.string() + " has no " +
            std::string(kStoreManifestFileName) +
            "; read-only mode opens only an initialized store");
    }
    if (const auto entry = FindStoreEntry(data_dir)) {
        // A concurrent first start (possible without the writer lock) may
        // have published its manifest after the read above.
        manifest = ReadStoreManifest(data_dir);
        if (manifest.has_value()) {
            return manifest;
        }
        throw std::runtime_error(
            "store directory " + data_dir.string() + " has no " +
            std::string(kStoreManifestFileName) + " but holds chunkdb data (found '" +
            *entry + "'): it is not a table of this chunkdb build (written by an older "
            "build, or a data directory rather than a table)");
    }
    return std::nullopt;
}

// Chooses the geometry a store opens with before anything on disk is touched,
// so a refused open leaves the data directory exactly as it was. The writer
// lock is not held yet; InitializeStoreManifest repeats the check under it.
Geometry OpenStoreGeometry(const StoreConfig& config) {
    RequireNotBackupDirectory(config.data_dir);
    if (config.data_dir.empty()) {
        throw std::invalid_argument("data_dir must not be empty");
    }
    const auto manifest = ReadManifestOrRequireNewStore(config.data_dir, config.access_mode);
    if (!manifest.has_value()) {
        if (!config.schema.has_value()) {
            return Geometry(config.geometry);
        }
        if (const auto reason = UnsupportedSchemaReason(*config.schema); !reason.empty()) {
            throw std::invalid_argument(reason);
        }
        return Geometry(config.geometry, *config.schema);
    }
    RequireOpenableFeatures(manifest->features, config.access_mode);
    if (const auto reason = UnsupportedSchemaReason(manifest->schema); !reason.empty()) {
        throw std::runtime_error("table " + config.data_dir.string() + " cannot be opened: " + reason);
    }
    if (config.schema.has_value() && *config.schema != manifest->schema) {
        throw std::runtime_error(
            "data directory " + config.data_dir.string() +
            " was created with different columns; omit the schema or pass the stored one");
    }

    const auto& stored = manifest->geometry;
    const auto& requested = config.geometry;
    const std::uint32_t fields = config.geometry_fields;
    std::string mismatches;
    const auto check = [&](GeometryField field, const char* name,
                           std::uint32_t stored_value, std::uint32_t requested_value) {
        if ((fields & field) != 0U && stored_value != requested_value) {
            mismatches += std::string(mismatches.empty() ? "" : ", ") + name + " " +
                          std::to_string(requested_value) + " (stored " +
                          std::to_string(stored_value) + ")";
        }
    };
    check(kGeometryLargeChunkWidth, "large_chunk_width",
          stored.large_chunk_width_chunks, requested.large_chunk_width_chunks);
    check(kGeometryLargeChunkHeight, "large_chunk_height",
          stored.large_chunk_height_chunks, requested.large_chunk_height_chunks);
    check(kGeometryChunkWidth, "chunk_width",
          stored.chunk_width_blocks, requested.chunk_width_blocks);
    check(kGeometryChunkHeight, "chunk_height",
          stored.chunk_height_blocks, requested.chunk_height_blocks);
    check(kGeometryBlockBits, "block_bits", stored.block_bits, requested.block_bits);
    if (!mismatches.empty()) {
        throw std::runtime_error(
            "data directory " + config.data_dir.string() +
            " was created with a different geometry: requested " + mismatches +
            "; stored geometry is " + DescribeGeometry(stored) +
            ". Geometry is fixed when a store is created: omit the geometry "
            "settings or pass the stored values");
    }
    return Geometry(stored, manifest->schema);
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
    : geometry_(OpenStoreGeometry(config)),
      data_dir_(std::move(config.data_dir)),
      durability_mode_(config.durability_mode),
      access_mode_(config.access_mode),
      allow_multiple_processes_(config.allow_multiple_processes),
      checkpoint_update_interval_(config.checkpoint_update_interval),
      checkpoint_wal_bytes_(config.checkpoint_wal_bytes),
      wal_group_commit_updates_(config.wal_group_commit_updates),
      checkpoint_compression_(config.checkpoint_compression),
      var_max_chunk_bytes_(config.var_max_chunk_bytes),
      resources_(
          config.resources != nullptr
              ? std::move(config.resources)
              : std::make_shared<StoreResources>(
                    config.max_loaded_chunks, config.max_open_wal_streams)),
      acquire_process_lock_(config.acquire_process_lock),
      initial_version_floor_(config.initial_version_floor),
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
    if (background_maintenance_ && background_checkpoint_queue_limit_ == 0) {
        throw std::invalid_argument("background_checkpoint_queue_limit must be > 0");
    }
    RequireValidVarLimit(var_max_chunk_bytes_);
    if (config.txn_history_bytes == 0) {
        throw std::invalid_argument("txn_history_bytes must be > 0");
    }
    txn_history_ = std::make_shared<TxnHistory>(config.txn_history_bytes);


    const auto recovery_start = std::chrono::steady_clock::now();
    const auto startup_scan = ScanStartupRecovery(data_dir_);

    // Decide, before this process creates any artifact, whether the store was
    // already initialized. The explicit initialized marker is written only
    // after the first valid version record. A lock directory alone is not
    // sufficient evidence because lock bootstrap precedes version
    // initialization and may survive a crash or failed constructor.
    bool store_preexisting =
        startup_scan.wal_files > 0 || startup_scan.checkpoint_files > 0 ||
        startup_scan.scan_capped;
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
        // The manifest precedes every other artifact a store writes.
        InitializeStoreManifest();
        // Slots refuse multi-process writers before recovery can change any
        // artifact. Read-only opening only inspects their checked metadata.
        const auto slots = ReadFeedSlotRecords(data_dir_, store_id_);
        if ((slots || std::filesystem::exists(data_dir_ / kFeedArchiveDirName)) && (features_.incompat & kFeatureFeedSlots) == 0U)
            throw std::runtime_error("chunkdb.slots requires the feed slots storage feature");
        if (config.allow_multiple_processes && slots &&
            std::any_of(slots->slots.begin(), slots->slots.end(), [](const auto& slot) { return !slot.lost; }))
            throw std::invalid_argument("feed slots require a single-process table");
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
        RecoverTransactionIntents();
        FinishSnapshotGenerationRecovery();
        feed_slots_ = std::make_shared<FeedSlots>(*this, config.slot_max_bytes, config.slot_sync_interval);
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
            {"store_id", StoreIdHex(store_id_)},
            {"geometry", DescribeGeometry(geometry_.config())},
            {"durability_mode", DurabilityModeName(durability_mode_)},
            {"access_mode", AccessModeName(access_mode_)},
            {"max_loaded_chunks", std::to_string(resources_->max_loaded_chunks())},
            {"max_open_wal_streams", std::to_string(resources_->max_open_wal_streams())},
            {"background_maintenance", background_maintenance_ ? "on" : "off"},
        });

    resources_->RegisterStore(this);
    if (background_maintenance_ && access_mode_ != AccessMode::kReadOnly) {
        try {
            StartMaintenanceThread();
        } catch (...) {
            resources_->UnregisterStore(this);
            ShutdownSnapshotGenerationLinger();
            ReleaseProcessLock();
            throw;
        }
    }
}

void ChunkStore::InitializeStoreManifest() {
    const auto manifest_path = StoreManifestPath(data_dir_);
    auto manifest = ReadManifestOrRequireNewStore(data_dir_, access_mode_);
    if (access_mode_ != AccessMode::kReadOnly) {
        // Stale temp files of an initialization that crashed before
        // publishing (or, on POSIX, before dropping the temp name).
        CleanupAtomicTmpArtifacts(manifest_path);
    }
    if (!manifest.has_value()) {
        const TableOptions options{
            .durability_mode = durability_mode_,
            .checkpoint_update_interval = checkpoint_update_interval_,
            .checkpoint_wal_bytes = checkpoint_wal_bytes_,
            .wal_group_commit_updates = wal_group_commit_updates_,
            .checkpoint_compression = checkpoint_compression_,
            .var_max_chunk_bytes = var_max_chunk_bytes_,
        };
        const StoreManifest created{
            .features = {},
            .geometry = geometry_.config(),
            .store_id = NewStoreId(),
            .options = EncodeTableOptions(options),
            .schema = geometry_.layout().schema(),
        };
        if (PublishNewFile(
                manifest_path,
                SerializeStoreManifest(created),
                "CHUNKDB_FAILPOINT_CRASH_MANIFEST_BEFORE_PUBLISH_ONCE",
                "CHUNKDB_FAILPOINT_CRASH_MANIFEST_AFTER_PUBLISH_ONCE")) {
            store_id_ = created.store_id;
            features_ = created.features;
            LogMessage(
                LogLevel::kInfo,
                LogComponent::kStore,
                "created store manifest",
                {
                    {"path", manifest_path.string()},
                    {"store_id", StoreIdHex(store_id_)},
                    {"geometry", DescribeGeometry(created.geometry)},
                });
            return;
        }
        // Another process initialized the directory first (only possible
        // without the writer lock); open what it created.
        manifest = ReadStoreManifest(data_dir_);
        if (!manifest.has_value()) {
            throw std::runtime_error(
                "store manifest " + manifest_path.string() +
                " disappeared while the store was being initialized");
        }
    }
    RequireOpenableFeatures(manifest->features, access_mode_);
    if (!SameGeometry(manifest->geometry, geometry_.config())) {
        throw std::runtime_error(
            "store manifest " + manifest_path.string() +
            " changed while the store was opening: it records " +
            DescribeGeometry(manifest->geometry) + ", the store opened with " +
            DescribeGeometry(geometry_.config()));
    }
    if (manifest->schema != geometry_.layout().schema()) {
        throw std::runtime_error(
            "store manifest " + manifest_path.string() + " changed its columns while the store was opening");
    }
    store_id_ = manifest->store_id;
    features_ = manifest->features;
}

void ChunkStore::SyncUnsyncedOnClose() noexcept {
    // A clean close makes what this store wrote without a sync durable, as
    // WALFLUSH does: a WALFLUSH in the next process does not know about it.
    // A store that TABLESET or a failed TABLEDROP replaces hands it to the
    // new store instead.
    if (access_mode_ == AccessMode::kReadOnly || durability_poisoned_.load(std::memory_order_acquire)) {
        return;
    }
    {
        std::lock_guard lock(unsynced_mutex_);
        if (unsynced_handover_ != nullptr) {
            return;
        }
    }
    const auto log_failure = [this](const char* error) noexcept {
        try {
            LogMessage(
                LogLevel::kError,
                LogComponent::kStore,
                "close could not sync writes made without a sync; they may be lost on power loss",
                {{"data_dir", data_dir_.string()}, {"error", error}});
        } catch (...) {
        }
    };
    try {
        WalBarrier();
    } catch (const std::exception& e) {
        log_failure(e.what());
    } catch (...) {
        log_failure("unknown");
    }
}

void ChunkStore::RequireStoreStillOnDisk() const {
    const auto manifest = ReadStoreManifest(data_dir_);
    if (!manifest.has_value() || manifest->store_id != store_id_) {
        throw std::runtime_error(
            "table directory " + data_dir_.string() + " no longer holds store " + StoreIdHex(store_id_) +
            " that this reader opened: the table was dropped");
    }
}

ChunkStore::~ChunkStore() {
    if (feed_slots_) feed_slots_->Stop();
    // First, so no eviction pass of another store works on this one while it
    // shuts down. Its chunks leave the shared cache with it.
    resources_->UnregisterStore(this);
    StopMaintenanceThread();
    FlushAllPendingWalBatches();
    SyncUnsyncedOnClose();
    // The stream pool is shared with other stores: give its slots back now
    // instead of when the chunks are destroyed.
    CloseAllWalStreams();
    resources_->ForgetWalStreams(this);
    // Closes the snapshot-generation bracket the shutdown flush may have left
    // lingering, so a cleanly closed store leaves an even (stable) generation
    // behind instead of forcing the next reader to fail closed.
    ShutdownSnapshotGenerationLinger();
    {
        std::lock_guard lock(unsynced_mutex_);
        if (unsynced_handover_ != nullptr) {
            unsynced_handover_->files = std::move(unsynced_files_);
            unsynced_handover_->dirs = std::move(unsynced_dirs_);
            unsynced_handover_->overflow = unsynced_overflow_;
        }
    }
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
        .open_wal_streams = OpenWalStreamCountForTests(),
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
    std::lock_guard lock(resources_->wal_stream_mutex_);
    std::uint64_t open = 0;
    for (const auto& [_, state] : resources_->open_wal_streams_) {
        if (state.owner == this) {
            ++open;
        }
    }
    return open;
}

std::size_t ChunkStore::MaxOpenWalStreamsForTests() const noexcept {
    return resources_->max_open_wal_streams();
}

}  // namespace chunkdb
