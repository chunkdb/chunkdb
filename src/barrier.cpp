#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/logging.hpp"
#include "store_manifest.hpp"
#include "wal_writer.hpp"

namespace chunkdb {

namespace {

// A durable sync mostly waits for the device. On Linux a journaling file
// system (ext4, XFS) commits syncs that arrive together in one journal
// commit, so a barrier spreads its syncs over up to kMaxBarrierSyncThreads
// threads, one per kSyncsPerBarrierThread syncs; a small barrier stays on the
// calling thread. Elsewhere the gain is smaller: on macOS each F_FULLFSYNC
// flushes the device cache, and those flushes are serialized.
constexpr std::size_t kMaxBarrierSyncThreads = 16;
constexpr std::size_t kSyncsPerBarrierThread = 8;

// Calls `sync` on every path, from the calling thread and the threads above.
// After the first failure no thread starts another sync, and that failure is
// rethrown once every thread has stopped.
template <typename Sync>
void SyncEach(const std::vector<std::filesystem::path>& paths, const Sync& sync) {
    const std::size_t thread_count =
        std::min(kMaxBarrierSyncThreads, paths.size() / kSyncsPerBarrierThread);
    if (thread_count <= 1U) {
        for (const auto& path : paths) {
            sync(path);
        }
        return;
    }
    std::atomic<std::size_t> next{0};
    std::atomic<bool> failed{false};
    std::mutex failure_mutex;
    std::exception_ptr failure;
    const auto work = [&]() noexcept {
        while (!failed.load(std::memory_order_acquire)) {
            const std::size_t index = next.fetch_add(1, std::memory_order_relaxed);
            if (index >= paths.size()) {
                return;
            }
            try {
                sync(paths[index]);
            } catch (const std::exception&) {
                // Rethrown by the calling thread below.
                std::lock_guard lock(failure_mutex);
                if (failure == nullptr) {
                    failure = std::current_exception();
                }
                failed.store(true, std::memory_order_release);
            }
        }
    };
    std::vector<std::thread> threads;
    threads.reserve(thread_count - 1U);
    for (std::size_t i = 1; i < thread_count; ++i) {
        try {
            threads.emplace_back(work);
        } catch (const std::system_error&) {
            // The threads already running and this one sync the rest: a
            // thread that cannot start costs time, never a sync.
            break;
        }
    }
    work();
    for (auto& thread : threads) {
        thread.join();
    }
    if (failure != nullptr) {
        std::rethrow_exception(failure);
    }
}

// Syncs `files` with SyncEach. A file removed since it was recorded (e.g. a
// checkpoint deleted the WAL) is skipped; durability of the removal is
// covered by syncing the parent directory.
void SyncFilesIfPresent(const std::vector<std::filesystem::path>& files) {
    // Deterministic fault hook: fail one file sync, possibly on another
    // thread, so a test can prove the failure still reaches the caller. It
    // is read here, once, so the sync threads never touch the environment.
    std::atomic<bool> inject_failure{
        ConsumeFailpointEnv("CHUNKDB_FAILPOINT_BARRIER_FILE_SYNC_FAIL_ONCE")};
    SyncEach(files, [&inject_failure](const std::filesystem::path& path) {
        if (inject_failure.exchange(false, std::memory_order_relaxed)) {
            throw std::runtime_error("injected barrier file sync failure: " + path.string());
        }
        (void)SyncFilePathIfPresent(path);
    });
}

void SyncDirIfPresent(const std::filesystem::path& path) {
    (void)SyncDirectoryPathIfPresent(path);
}

// Orders directories so that deeper paths sort first; syncing children before
// their parents makes each newly created directory entry durable in an order
// a crash can never observe out of sequence.
[[nodiscard]] std::size_t PathDepth(const std::filesystem::path& path) {
    return static_cast<std::size_t>(std::distance(path.begin(), path.end()));
}

// Syncs `dirs` deepest first. Directories of one depth sync together.
void SyncDirsDeepestFirst(std::vector<std::filesystem::path> dirs) {
    std::sort(
        dirs.begin(),
        dirs.end(),
        [](const std::filesystem::path& lhs, const std::filesystem::path& rhs) {
            return PathDepth(lhs) > PathDepth(rhs);
        });
    std::vector<std::filesystem::path> level;
    for (std::size_t i = 0; i < dirs.size();) {
        const std::size_t depth = PathDepth(dirs[i]);
        level.clear();
        while (i < dirs.size() && PathDepth(dirs[i]) == depth) {
            level.push_back(std::move(dirs[i]));
            ++i;
        }
        SyncEach(level, SyncDirIfPresent);
    }
}

[[noreturn]] void ThrowListingError(
    const char* what,
    const std::filesystem::path& path,
    const std::error_code& ec) {
    throw std::runtime_error(
        std::string(what) + " during WAL barrier: " + path.string() +
        " (ec=" + std::to_string(ec.value()) + ", msg='" + ec.message() + "')");
}

// Appends the regular files in `dir`, and its subdirectories when `subdirs`
// is set. Returns false when `dir` no longer exists: it was removed after it
// was recorded, and syncing its parent covers the removal. An entry removed
// while it is listed is skipped for the same reason. Any other error fails
// the barrier, so no file it should sync is silently left out. The store
// creates no symbolic links, and none is followed.
bool ListDirectoryForSync(
    const std::filesystem::path& dir,
    std::vector<std::filesystem::path>* files,
    std::vector<std::filesystem::path>* subdirs) {
    std::error_code ec;
    std::filesystem::directory_iterator it(dir, ec);
    if (ec == std::errc::no_such_file_or_directory) {
        return false;
    }
    if (ec) {
        ThrowListingError("failed to open directory", dir, ec);
    }
    const std::filesystem::directory_iterator end;
    while (it != end) {
        std::error_code type_ec;
        const auto status = it->symlink_status(type_ec);
        if (status.type() != std::filesystem::file_type::not_found) {
            if (type_ec) {
                ThrowListingError("failed to classify entry", it->path(), type_ec);
            }
            if (std::filesystem::is_directory(status)) {
                if (subdirs != nullptr) {
                    subdirs->push_back(it->path());
                }
            } else if (std::filesystem::is_regular_file(status)) {
                files->push_back(it->path());
            }
        }
        it.increment(ec);
        if (ec) {
            ThrowListingError("failed to list directory", dir, ec);
        }
    }
    return true;
}

// The fallback when even directories are too many to track: every file and
// directory the store owns in its data directory, files first.
void SyncWholeDataDirectory(const std::filesystem::path& data_dir) {
    std::vector<std::filesystem::path> top_files;
    std::vector<std::filesystem::path> top_dirs;
    if (!ListDirectoryForSync(data_dir, &top_files, &top_dirs)) {
        throw std::runtime_error(
            "data directory disappeared during full barrier sync: " + data_dir.string());
    }
    // Entries chunkdb does not own (`lost+found` on a volume root, for
    // example) are not storage state and may not even be readable. The
    // process-lock and conditional-intent control directories are not
    // storage state either, and the writer holds .chunkdb.lock/writer.lock
    // with exclusive open semantics on Windows, so sync-opening it would
    // fail. None of them is synced or descended into.
    const auto owned = [](const std::filesystem::path& path) {
        const auto name = path.filename().string();
        return IsStoreEntryName(name) && name != kProcessLockDirName &&
               name != kConditionalIntentDirName;
    };
    std::vector<std::filesystem::path> files;
    std::vector<std::filesystem::path> dirs;
    std::vector<std::filesystem::path> pending;
    for (auto& file : top_files) {
        if (owned(file)) {
            files.push_back(std::move(file));
        }
    }
    for (auto& dir : top_dirs) {
        if (owned(dir)) {
            pending.push_back(std::move(dir));
        }
    }
    while (!pending.empty()) {
        auto dir = std::move(pending.back());
        pending.pop_back();
        std::vector<std::filesystem::path> subdirs;
        if (!ListDirectoryForSync(dir, &files, &subdirs)) {
            continue;
        }
        dirs.push_back(std::move(dir));
        for (auto& subdir : subdirs) {
            pending.push_back(std::move(subdir));
        }
    }
    SyncFilesIfPresent(files);
    SyncDirsDeepestFirst(std::move(dirs));
    SyncDirIfPresent(data_dir);
}

// Syncs what a barrier drained: the tracked files and every file of each
// swept directory first, so their contents are durable before the directory
// entries that reference them, then the directories, then the data directory.
void SyncTrackedArtifacts(
    const std::unordered_set<std::string>& files,
    const std::unordered_set<std::string>& dirs,
    const std::unordered_set<std::string>& swept_dirs,
    const std::filesystem::path& data_dir) {
    std::vector<std::filesystem::path> file_paths;
    file_paths.reserve(files.size());
    for (const auto& file : files) {
        std::filesystem::path path(file);
        // Recorded before its directory was swept; the sweep covers it.
        if (!swept_dirs.empty() && swept_dirs.contains(path.parent_path().string())) {
            continue;
        }
        file_paths.push_back(std::move(path));
    }
    for (const auto& dir : swept_dirs) {
        (void)ListDirectoryForSync(dir, &file_paths, nullptr);
    }
    SyncFilesIfPresent(file_paths);

    std::vector<std::filesystem::path> dir_paths;
    dir_paths.reserve(dirs.size() + swept_dirs.size());
    for (const auto& dir : dirs) {
        dir_paths.emplace_back(dir);
    }
    for (const auto& dir : swept_dirs) {
        if (!dirs.contains(dir)) {
            dir_paths.emplace_back(dir);
        }
    }
    SyncDirsDeepestFirst(std::move(dir_paths));
    // Directory entries for newly created per-large-chunk directories live in
    // the data directory itself; sync it so those creations (and any GC
    // removals) are durable too.
    SyncDirIfPresent(data_dir);
}

}  // namespace

void ChunkStore::LimitUnsyncedLocked() {
    const auto tracked = [this] {
        return unsynced_files_.size() + unsynced_dirs_.size() + unsynced_swept_dirs_.size();
    };
    if (tracked() <= unsynced_limit_) {
        return;
    }
    // Track the files of each large-chunk directory by the directory: the
    // barrier then syncs every file in it and the directory itself, which
    // bounds memory by the number of changed directories instead of files.
    // A directory is in one set only, so each counts once against the limit.
    for (auto it = unsynced_files_.begin(); it != unsynced_files_.end();) {
        const auto parent = std::filesystem::path(*it).parent_path();
        if (parent.filename().string().starts_with("L_")) {
            auto parent_key = parent.string();
            unsynced_dirs_.erase(parent_key);
            unsynced_swept_dirs_.insert(std::move(parent_key));
            it = unsynced_files_.erase(it);
        } else {
            ++it;
        }
    }
    if (tracked() <= unsynced_limit_) {
        return;
    }
    // Still too many: the next barrier syncs the whole data directory.
    unsynced_overflow_ = true;
    unsynced_files_.clear();
    unsynced_dirs_.clear();
    unsynced_swept_dirs_.clear();
}

void ChunkStore::InsertUnsyncedLocked(const UnsyncedArtifacts& artifacts) {
    if (unsynced_overflow_) {
        return;
    }
    if (artifacts.overflow) {
        unsynced_overflow_ = true;
        unsynced_files_.clear();
        unsynced_dirs_.clear();
        unsynced_swept_dirs_.clear();
        return;
    }
    for (const auto& dir : artifacts.swept_dirs) {
        unsynced_dirs_.erase(dir);
        unsynced_swept_dirs_.insert(dir);
    }
    for (const auto& file : artifacts.files) {
        if (unsynced_swept_dirs_.empty() ||
            !unsynced_swept_dirs_.contains(std::filesystem::path(file).parent_path().string())) {
            unsynced_files_.insert(file);
        }
    }
    for (const auto& dir : artifacts.dirs) {
        if (!unsynced_swept_dirs_.contains(dir)) {
            unsynced_dirs_.insert(dir);
        }
    }
    LimitUnsyncedLocked();
}

void ChunkStore::NoteUnsyncedFile(const std::filesystem::path& path) {
    std::lock_guard lock(unsynced_mutex_);
    if (unsynced_overflow_) {
        return;
    }
    if (!unsynced_swept_dirs_.empty() && unsynced_swept_dirs_.contains(path.parent_path().string())) {
        return;
    }
    unsynced_files_.insert(path.string());
    LimitUnsyncedLocked();
}

void ChunkStore::NoteUnsyncedDir(const std::filesystem::path& path) {
    std::lock_guard lock(unsynced_mutex_);
    if (unsynced_overflow_) {
        return;
    }
    auto key = path.string();
    if (!unsynced_swept_dirs_.empty() && unsynced_swept_dirs_.contains(key)) {
        return;
    }
    unsynced_dirs_.insert(std::move(key));
    LimitUnsyncedLocked();
}

void ChunkStore::HandOverUnsyncedOnClose(std::shared_ptr<UnsyncedArtifacts> sink) {
    std::lock_guard lock(unsynced_mutex_);
    unsynced_handover_ = std::move(sink);
}

void ChunkStore::AdoptUnsynced(const UnsyncedArtifacts& artifacts) {
    std::lock_guard lock(unsynced_mutex_);
    InsertUnsyncedLocked(artifacts);
}

void ChunkStore::RestoreUnsynced(UnsyncedArtifacts drained) {
    std::lock_guard lock(unsynced_mutex_);
    InsertUnsyncedLocked(drained);
}

void ChunkStore::ForceUnsyncedOverflowForTests() {
    std::lock_guard lock(unsynced_mutex_);
    unsynced_overflow_ = true;
    unsynced_files_.clear();
    unsynced_dirs_.clear();
    unsynced_swept_dirs_.clear();
}

std::size_t ChunkStore::UnsyncedTrackedCountForTests() const {
    std::lock_guard lock(unsynced_mutex_);
    return unsynced_files_.size() + unsynced_dirs_.size() + unsynced_swept_dirs_.size();
}

std::size_t ChunkStore::UnsyncedSweptDirCountForTests() const {
    std::lock_guard lock(unsynced_mutex_);
    return unsynced_swept_dirs_.size();
}

bool ChunkStore::UnsyncedOverflowFlagForTests() const {
    std::lock_guard lock(unsynced_mutex_);
    return unsynced_overflow_;
}

void ChunkStore::SetUnsyncedLimitForTests(std::size_t limit) {
    std::lock_guard lock(unsynced_mutex_);
    unsynced_limit_ = limit;
    LimitUnsyncedLocked();
}

void ChunkStore::ArmBarrierBeforeDrainPauseForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    barrier_before_drain_armed_ = true;
    barrier_before_drain_reached_ = false;
    barrier_before_drain_resumed_ = false;
}

bool ChunkStore::WaitForBarrierBeforeDrainForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    return durability_hook_cv_.wait_for(
        lock,
        std::chrono::seconds(5),
        [this] { return barrier_before_drain_reached_; });
}

void ChunkStore::ResumeBarrierBeforeDrainForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    barrier_before_drain_resumed_ = true;
    durability_hook_cv_.notify_all();
}

void ChunkStore::ArmWalBoundarySyncPauseForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    wal_boundary_sync_pause_reached_ = false;
    wal_boundary_sync_pause_resumed_ = false;
    wal_boundary_sync_pause_armed_.store(true, std::memory_order_release);
}

bool ChunkStore::WaitForWalBoundarySyncPauseForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    return durability_hook_cv_.wait_for(
        lock,
        std::chrono::seconds(5),
        [this] { return wal_boundary_sync_pause_reached_; });
}

void ChunkStore::ResumeWalBoundarySyncForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    wal_boundary_sync_pause_resumed_ = true;
    durability_hook_cv_.notify_all();
}

void ChunkStore::PauseWalBoundarySyncForTests() {
    if (!wal_boundary_sync_pause_armed_.load(std::memory_order_acquire)) {
        return;
    }
    std::unique_lock lock(durability_hook_mutex_);
    if (!wal_boundary_sync_pause_armed_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    wal_boundary_sync_pause_reached_ = true;
    durability_hook_cv_.notify_all();
    durability_hook_cv_.wait(lock, [this] { return wal_boundary_sync_pause_resumed_; });
}

void ChunkStore::ArmBarrierBeforeSyncPauseForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    barrier_before_sync_armed_ = true;
    barrier_before_sync_reached_ = false;
    barrier_before_sync_resumed_ = false;
}

bool ChunkStore::WaitForBarrierBeforeSyncForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    return durability_hook_cv_.wait_for(
        lock,
        std::chrono::seconds(5),
        [this] { return barrier_before_sync_reached_; });
}

void ChunkStore::ResumeBarrierBeforeSyncForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    barrier_before_sync_resumed_ = true;
    durability_hook_cv_.notify_all();
}

void ChunkStore::ArmBarrierAfterDrainPauseForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    barrier_after_drain_armed_ = true;
    barrier_after_drain_reached_ = false;
    barrier_after_drain_resumed_ = false;
}

bool ChunkStore::WaitForBarrierAfterDrainForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    return durability_hook_cv_.wait_for(
        lock,
        std::chrono::seconds(5),
        [this] { return barrier_after_drain_reached_; });
}

void ChunkStore::ResumeBarrierAfterDrainForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    barrier_after_drain_resumed_ = true;
    durability_hook_cv_.notify_all();
}

void ChunkStore::ArmCheckpointBeforeWalRemovalPauseForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    checkpoint_before_wal_remove_armed_ = true;
    checkpoint_before_wal_remove_reached_ = false;
    checkpoint_before_wal_remove_resumed_ = false;
}

bool ChunkStore::WaitForCheckpointBeforeWalRemovalForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    return durability_hook_cv_.wait_for(
        lock,
        std::chrono::seconds(5),
        [this] { return checkpoint_before_wal_remove_reached_; });
}

void ChunkStore::ResumeCheckpointBeforeWalRemovalForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    checkpoint_before_wal_remove_resumed_ = true;
    durability_hook_cv_.notify_all();
}

void ChunkStore::ArmWalOpenPauseForTests(const ChunkCoord& chunk) {
    std::lock_guard lock(durability_hook_mutex_);
    wal_open_pause_chunk_ = chunk;
    wal_open_pause_armed_.store(true, std::memory_order_release);
    wal_open_pause_reached_ = false;
    wal_open_pause_resumed_ = false;
}

bool ChunkStore::WaitForWalOpenPauseForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    return durability_hook_cv_.wait_for(
        lock,
        std::chrono::seconds(5),
        [this] { return wal_open_pause_reached_; });
}

void ChunkStore::ResumeWalOpenForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    wal_open_pause_resumed_ = true;
    durability_hook_cv_.notify_all();
}

void ChunkStore::ArmWalDirPreparePauseForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    wal_dir_prepare_pause_reached_ = false;
    wal_dir_prepare_pause_resumed_ = false;
    wal_dir_prepare_pause_armed_.store(true, std::memory_order_release);
}

bool ChunkStore::WaitForWalDirPreparePauseForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    return durability_hook_cv_.wait_for(
        lock,
        std::chrono::seconds(5),
        [this] { return wal_dir_prepare_pause_reached_; });
}

void ChunkStore::ResumeWalDirPrepareForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    wal_dir_prepare_pause_resumed_ = true;
    durability_hook_cv_.notify_all();
}

std::size_t ChunkStore::WalDirPrepareWaitersForTests() const {
    std::lock_guard lock(wal_parent_cache_mutex_);
    return wal_parent_dir_waiters_;
}

void ChunkStore::ArmCheckpointPublishAttemptForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    checkpoint_publish_attempt_armed_ = true;
    checkpoint_publish_attempt_reached_ = false;
}

bool ChunkStore::WaitForCheckpointPublishAttemptForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    return durability_hook_cv_.wait_for(
        lock,
        std::chrono::seconds(5),
        [this] { return checkpoint_publish_attempt_reached_; });
}

void ChunkStore::ArmConditionalMutationPauseForTests(
    ConditionalMutationPausePoint point) {
    std::lock_guard lock(durability_hook_mutex_);
    conditional_pause_point_ = point;
    conditional_pause_reached_ = false;
    conditional_pause_resumed_ = false;
}

bool ChunkStore::WaitForConditionalMutationPauseForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    return durability_hook_cv_.wait_for(
        lock,
        std::chrono::seconds(5),
        [this] { return conditional_pause_reached_; });
}

void ChunkStore::ResumeConditionalMutationForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    conditional_pause_resumed_ = true;
    durability_hook_cv_.notify_all();
}

void ChunkStore::NoteCheckpointPublishAttemptForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    if (!checkpoint_publish_attempt_armed_) {
        return;
    }
    checkpoint_publish_attempt_reached_ = true;
    checkpoint_publish_attempt_armed_ = false;
    durability_hook_cv_.notify_all();
}

void ChunkStore::PauseCheckpointBeforeWalRemovalForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    if (!checkpoint_before_wal_remove_armed_) {
        return;
    }
    checkpoint_before_wal_remove_reached_ = true;
    durability_hook_cv_.notify_all();
    durability_hook_cv_.wait(
        lock,
        [this] { return checkpoint_before_wal_remove_resumed_; });
    checkpoint_before_wal_remove_armed_ = false;
}

void ChunkStore::PauseWalOpenForTests(const ChunkCoord& chunk) {
    if (!wal_open_pause_armed_.load(std::memory_order_acquire)) {
        return;
    }
    std::unique_lock lock(durability_hook_mutex_);
    if (wal_open_pause_chunk_ != chunk) {
        return;
    }
    wal_open_pause_chunk_.reset();
    wal_open_pause_armed_.store(false, std::memory_order_release);
    wal_open_pause_reached_ = true;
    durability_hook_cv_.notify_all();
    durability_hook_cv_.wait(lock, [this] { return wal_open_pause_resumed_; });
}

void ChunkStore::PauseWalDirPrepareForTests() {
    if (!wal_dir_prepare_pause_armed_.load(std::memory_order_acquire)) {
        return;
    }
    std::unique_lock lock(durability_hook_mutex_);
    if (!wal_dir_prepare_pause_armed_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    wal_dir_prepare_pause_reached_ = true;
    durability_hook_cv_.notify_all();
    durability_hook_cv_.wait(lock, [this] { return wal_dir_prepare_pause_resumed_; });
}

void ChunkStore::PauseConditionalMutationForTests(
    ConditionalMutationPausePoint point) {
    std::unique_lock lock(durability_hook_mutex_);
    if (conditional_pause_point_ != point) {
        return;
    }
    conditional_pause_reached_ = true;
    durability_hook_cv_.notify_all();
    durability_hook_cv_.wait(
        lock,
        [this] { return conditional_pause_resumed_; });
    conditional_pause_point_ = ConditionalMutationPausePoint::kNone;
}

void ChunkStore::ArmReadOnlySnapshotPausesForTests(
    std::vector<ReadOnlySnapshotPausePoint> points) {
    std::lock_guard lock(read_only_snapshot_pause_mutex_);
    read_only_snapshot_pause_points_ = std::move(points);
    read_only_snapshot_pause_index_ = 0;
    read_only_snapshot_pause_reached_ = false;
    read_only_snapshot_pause_resumed_ = false;
}

bool ChunkStore::WaitForReadOnlySnapshotPauseForTests() {
    std::unique_lock lock(read_only_snapshot_pause_mutex_);
    return read_only_snapshot_pause_cv_.wait_for(
        lock,
        std::chrono::seconds(5),
        [this] { return read_only_snapshot_pause_reached_; });
}

void ChunkStore::ResumeReadOnlySnapshotForTests() {
    std::lock_guard lock(read_only_snapshot_pause_mutex_);
    read_only_snapshot_pause_resumed_ = true;
    read_only_snapshot_pause_reached_ = false;
    read_only_snapshot_pause_cv_.notify_all();
}

void ChunkStore::PauseReadOnlySnapshotForTests(
    std::size_t collection,
    ReadOnlySnapshotArtifact artifact) {
    std::unique_lock lock(read_only_snapshot_pause_mutex_);
    if (read_only_snapshot_pause_index_ >=
        read_only_snapshot_pause_points_.size()) {
        return;
    }
    const auto& point =
        read_only_snapshot_pause_points_[read_only_snapshot_pause_index_];
    if (point.collection != collection ||
        point.artifact != artifact) {
        return;
    }
    read_only_snapshot_pause_reached_ = true;
    read_only_snapshot_pause_resumed_ = false;
    read_only_snapshot_pause_cv_.notify_all();
    read_only_snapshot_pause_cv_.wait(
        lock,
        [this] { return read_only_snapshot_pause_resumed_; });
    ++read_only_snapshot_pause_index_;
    read_only_snapshot_pause_reached_ = false;
}

void ChunkStore::WalBarrier() {
    if (access_mode_ == AccessMode::kReadOnly) {
        return;
    }
    ThrowIfDurabilityPoisoned();

    // Serialize barriers so each caller's success covers everything
    // acknowledged before its own call began.
    std::lock_guard barrier_lock(wal_barrier_mutex_);

    // Step 1: write every loaded chunk's pending in-memory WAL batch. Any
    // failure aborts the barrier and reaches the caller. A batch is written
    // without a sync and recorded as unsynced, so step 2 syncs it together
    // with every other file. A chunk whose WAL boundary this process already
    // made durable is synced here, so that boundary stays durable.
    std::vector<std::shared_ptr<LargeChunk>> large_chunks;
    {
        std::lock_guard lock(large_chunks_mutex_);
        large_chunks.reserve(large_chunks_.size());
        for (const auto& [_, large_chunk] : large_chunks_) {
            large_chunks.push_back(large_chunk);
        }
    }
    for (const auto& large_chunk : large_chunks) {
        std::vector<std::pair<ChunkCoord, std::shared_ptr<RegularChunk>>> chunks;
        {
            std::lock_guard lock(large_chunk->mutex);
            chunks.reserve(large_chunk->chunks.size());
            for (const auto& [coord, chunk] : large_chunk->chunks) {
                chunks.emplace_back(coord, chunk);
            }
        }
        for (const auto& [coord, chunk] : chunks) {
            std::unique_lock chunk_lock(chunk->mutex);
            FlushWalBatch(coord, chunk, /*force_sync=*/chunk->wal_boundary_durable);
        }
    }

    // Pause after the batches are written, before the drain, so a test can
    // start a rollback boundary on a chunk this barrier has already visited.
    {
        std::unique_lock hook_lock(durability_hook_mutex_);
        if (barrier_before_drain_armed_) {
            barrier_before_drain_reached_ = true;
            durability_hook_cv_.notify_all();
            durability_hook_cv_.wait(
                hook_lock,
                [this] { return barrier_before_drain_resumed_; });
            barrier_before_drain_armed_ = false;
        }
    }

    // Step 2: sync every artifact recorded as written without a sync.
    //
    // The drain runs under checkpoint_publish_mutex_ and sets the durability
    // floor; outside Windows the syncs run without that lock, so checkpoints,
    // empty-chunk collection and WAL trims are not blocked while the syncs
    // wait for the device. Every step that removes or replaces a chunk's
    // image or WAL holds checkpoint_publish_mutex_ and decides under it
    // whether to sync, so:
    // - a step that finished before the drain recorded what it left unsynced,
    //   and the drain hands that to this barrier;
    // - a step that runs after the drain sees the floor and is strict: it
    //   syncs the replacement image (or WAL) and its directory entry before
    //   it removes the WAL, and then syncs the removal. The state it carries
    //   over is durable without this barrier, which may then find a drained
    //   path gone (and skip it) or sync a path that now names the new,
    //   already synced file.
    // A rollback boundary (MakeWalBoundaryDurableLocked) takes its chunk's
    // artifacts out of the set to sync them itself, holding only the chunk.
    // Until its syncs end they are in flight, and the drain includes them, so
    // this barrier syncs them as well and never relies on a sync it does not
    // wait for; a boundary that ended before the drain made them durable or
    // put them back.
    // Appends after the drain only add bytes behind the ones this barrier
    // must make durable, and a sync covers them whenever it runs. Setting the
    // floor before the syncs succeed only makes later checkpoints stricter.
    //
    // On Windows the barrier keeps the lock through the syncs. Its sync
    // handles would otherwise overlap checkpoint publication, and a rename
    // over a file another handle has open fails there unless the file system
    // offers POSIX rename semantics, as does opening a file whose deletion is
    // pending.
    UnsyncedArtifacts drained;
    std::unique_lock<std::mutex> publish_lock(checkpoint_publish_mutex_);
    barrier_durability_floor_.store(true, std::memory_order_release);
    {
        std::lock_guard lock(unsynced_mutex_);
        drained.files.swap(unsynced_files_);
        drained.dirs.swap(unsynced_dirs_);
        drained.swept_dirs.swap(unsynced_swept_dirs_);
        drained.overflow = unsynced_overflow_;
        unsynced_overflow_ = false;
        drained.files.insert(unsynced_files_in_flight_.begin(), unsynced_files_in_flight_.end());
        drained.dirs.insert(unsynced_dirs_in_flight_.begin(), unsynced_dirs_in_flight_.end());
    }

    // Deterministic coordination hooks: pause after draining, while still
    // holding the publish lock, so a test can prove a concurrent checkpoint
    // publishes either before the drain or after the floor.
    {
        std::unique_lock hook_lock(durability_hook_mutex_);
        if (barrier_after_drain_armed_) {
            barrier_after_drain_reached_ = true;
            durability_hook_cv_.notify_all();
            durability_hook_cv_.wait(
                hook_lock,
                [this] { return barrier_after_drain_resumed_; });
            barrier_after_drain_armed_ = false;
        }
    }
    if (const auto hold = ConsumeFailpointDelayMs("CHUNKDB_FAILPOINT_BARRIER_AFTER_DRAIN_HOLD_MS_ONCE");
        hold.count() > 0) {
        std::this_thread::sleep_for(hold);
    }
    // Clears the sync-phase flag when the syncs end, before a Windows
    // barrier releases the publish lock.
    struct SyncPhase {
        std::atomic<bool>& syncing;
        ~SyncPhase() { syncing.store(false, std::memory_order_release); }
    };
    barrier_syncing_.store(true, std::memory_order_release);
    const SyncPhase sync_phase{barrier_syncing_};
#ifndef _WIN32
    publish_lock.unlock();
#endif

    // Pause before the first sync, once publication is allowed again outside
    // Windows, so a test can publish a checkpoint while this barrier still
    // has to sync.
    {
        std::unique_lock hook_lock(durability_hook_mutex_);
        if (barrier_before_sync_armed_) {
            barrier_before_sync_reached_ = true;
            durability_hook_cv_.notify_all();
            durability_hook_cv_.wait(
                hook_lock,
                [this] { return barrier_before_sync_resumed_; });
            barrier_before_sync_armed_ = false;
        }
    }

    try {
        // Deterministic fault hook for barrier durability tests: fail after the
        // bookkeeping has been drained so the retry-retention path is exercised
        // without needing a real filesystem fault.
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_BARRIER_SYNC_FAIL_ONCE")) {
            throw std::runtime_error("injected barrier sync failure");
        }
        if (drained.overflow) {
            SyncWholeDataDirectory(data_dir_);
            stats_wal_barrier_full_syncs_.fetch_add(1, std::memory_order_relaxed);
        } else {
            SyncTrackedArtifacts(drained.files, drained.dirs, drained.swept_dirs, data_dir_);
        }
    } catch (...) {
        // Restore the drained bookkeeping so a retried barrier still covers
        // the artifacts this one failed to sync.
        RestoreUnsynced(std::move(drained));
        throw;
    }

    stats_wal_barriers_.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace chunkdb
