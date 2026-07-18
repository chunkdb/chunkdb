#include <algorithm>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/logging.hpp"
#include "wal_writer.hpp"

namespace chunkdb {

namespace {

// When more distinct relaxed-mode artifacts than this accumulate between
// barriers, the tracking sets are dropped and the next barrier syncs the
// whole data directory instead, keeping barrier bookkeeping memory bounded.
constexpr std::size_t kMaxUnsyncedTracked = 65536;

// Returns true if the path exists, false if it is definitively absent.
// Throws when existence cannot be determined: an errored stat is not proof
// of absence, and skipping such a path would silently drop a durability step.
[[nodiscard]] bool ExistsOrThrow(const std::filesystem::path& path, const char* what) {
    std::error_code exists_ec;
    const bool present = std::filesystem::exists(path, exists_ec);
    if (exists_ec) {
        throw std::runtime_error(
            std::string("failed to stat ") + what + " during WAL barrier: " + path.string() +
            " (ec=" + std::to_string(exists_ec.value()) + ", msg='" + exists_ec.message() + "')");
    }
    return present;
}

void SyncFileIfPresent(const std::filesystem::path& path) {
    // Removed since it was recorded (e.g. a checkpoint deleted the WAL);
    // durability of the removal is covered by syncing the parent directory.
    if (!ExistsOrThrow(path, "file")) {
        return;
    }
    SyncFilePath(path);
}

void SyncDirIfPresent(const std::filesystem::path& path) {
    if (!ExistsOrThrow(path, "directory")) {
        return;
    }
    SyncDirectoryPath(path);
}

// Orders directories so that deeper paths sort first; syncing children before
// their parents makes each newly created directory entry durable in an order
// a crash can never observe out of sequence.
[[nodiscard]] std::size_t PathDepth(const std::filesystem::path& path) {
    return static_cast<std::size_t>(std::distance(path.begin(), path.end()));
}

}  // namespace

void ChunkStore::NoteUnsyncedFile(const std::filesystem::path& path) {
    std::lock_guard lock(unsynced_mutex_);
    if (unsynced_overflow_) {
        return;
    }
    if (unsynced_files_.size() + unsynced_dirs_.size() >= kMaxUnsyncedTracked) {
        unsynced_overflow_ = true;
        unsynced_files_.clear();
        unsynced_dirs_.clear();
        return;
    }
    unsynced_files_.insert(path.string());
}

void ChunkStore::NoteUnsyncedDir(const std::filesystem::path& path) {
    std::lock_guard lock(unsynced_mutex_);
    if (unsynced_overflow_) {
        return;
    }
    if (unsynced_files_.size() + unsynced_dirs_.size() >= kMaxUnsyncedTracked) {
        unsynced_overflow_ = true;
        unsynced_files_.clear();
        unsynced_dirs_.clear();
        return;
    }
    unsynced_dirs_.insert(path.string());
}

void ChunkStore::ForceUnsyncedOverflowForTests() {
    std::lock_guard lock(unsynced_mutex_);
    unsynced_overflow_ = true;
    unsynced_files_.clear();
    unsynced_dirs_.clear();
}

std::size_t ChunkStore::UnsyncedTrackedCountForTests() const {
    std::lock_guard lock(unsynced_mutex_);
    return unsynced_files_.size() + unsynced_dirs_.size();
}

bool ChunkStore::UnsyncedOverflowFlagForTests() const {
    std::lock_guard lock(unsynced_mutex_);
    return unsynced_overflow_;
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

    // Step 1: durably flush every loaded chunk's pending in-memory WAL batch.
    // Any failure aborts the barrier and reaches the caller.
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
            FlushWalBatch(coord, chunk, /*force_sync=*/true);
        }
    }

    // Step 2: sync every artifact recorded as written without an fsync.
    //
    // Hold checkpoint_publish_mutex_ across the drain AND the sync so no
    // checkpoint can replace a tracked WAL with an unsynced image while this
    // barrier runs. Any checkpoint that published before this point already
    // recorded its replacement image in the unsynced set (drained below);
    // any checkpoint that would publish now blocks until the barrier finishes,
    // by which time the WAL it would remove has already been synced. This makes
    // "the tracked path disappeared" safe: its durable replacement is always
    // covered by the same barrier.
    std::unique_lock<std::mutex> publish_lock(checkpoint_publish_mutex_);

    std::unordered_set<std::string> files;
    std::unordered_set<std::string> dirs;
    bool overflow = false;
    {
        std::lock_guard lock(unsynced_mutex_);
        files.swap(unsynced_files_);
        dirs.swap(unsynced_dirs_);
        overflow = unsynced_overflow_;
        unsynced_overflow_ = false;
    }

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

    // Deterministic coordination hook: pause after draining (while still holding
    // the publish lock) so a test can prove a concurrent checkpoint cannot
    // replace a tracked WAL until this barrier completes.
    if (const auto hold = ConsumeFailpointDelayMs("CHUNKDB_FAILPOINT_BARRIER_AFTER_DRAIN_HOLD_MS_ONCE");
        hold.count() > 0) {
        std::this_thread::sleep_for(hold);
    }

    try {
        // Deterministic fault hook for barrier durability tests: fail after the
        // bookkeeping has been drained so the retry-retention path is exercised
        // without needing a real filesystem fault.
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_BARRIER_SYNC_FAIL_ONCE")) {
            throw std::runtime_error("injected barrier sync failure");
        }
        if (overflow) {
            // Bounded-tracking fallback: enumerate the whole data directory,
            // fail closed on any traversal error, and sync files before the
            // directories that reference them (deepest directories first).
            std::vector<std::filesystem::path> file_paths;
            std::vector<std::filesystem::path> dir_paths;
            std::error_code it_ec;
            std::filesystem::recursive_directory_iterator it(
                data_dir_, std::filesystem::directory_options::none, it_ec);
            if (it_ec) {
                throw std::runtime_error(
                    "failed to open data directory for full barrier sync: " + data_dir_.string() +
                    " (ec=" + std::to_string(it_ec.value()) + ", msg='" + it_ec.message() + "')");
            }
            const std::filesystem::recursive_directory_iterator end;
            while (it != end) {
                std::error_code type_ec;
                const bool is_dir = it->is_directory(type_ec);
                if (type_ec) {
                    throw std::runtime_error(
                        "failed to classify entry during full barrier sync: " +
                        it->path().string() + " (ec=" + std::to_string(type_ec.value()) +
                        ", msg='" + type_ec.message() + "')");
                }
                // Skip the process-lock and conditional-intent control
                // directories: they are not storage state, and the writer holds
                // .chunkdb.lock/writer.lock with exclusive open semantics on
                // Windows, so sync-opening it would fail (and terminate the
                // barrier). Match only a top-level entry of the data directory
                // by exact name, and do not descend into it.
                if (is_dir && it.depth() == 0 &&
                    (it->path().filename() == kProcessLockDirName ||
                     it->path().filename() == kConditionalIntentDirName)) {
                    it.disable_recursion_pending();
                    it.increment(it_ec);
                    if (it_ec) {
                        throw std::runtime_error(
                            "failed to traverse data directory during full barrier sync: " +
                            data_dir_.string() + " (ec=" + std::to_string(it_ec.value()) +
                            ", msg='" + it_ec.message() + "')");
                    }
                    continue;
                }
                if (is_dir) {
                    dir_paths.push_back(it->path());
                } else {
                    std::error_code file_ec;
                    if (it->is_regular_file(file_ec) && !file_ec) {
                        file_paths.push_back(it->path());
                    } else if (file_ec) {
                        throw std::runtime_error(
                            "failed to classify entry during full barrier sync: " +
                            it->path().string() + " (ec=" + std::to_string(file_ec.value()) +
                            ", msg='" + file_ec.message() + "')");
                    }
                }
                it.increment(it_ec);
                if (it_ec) {
                    throw std::runtime_error(
                        "failed to traverse data directory during full barrier sync: " +
                        data_dir_.string() + " (ec=" + std::to_string(it_ec.value()) +
                        ", msg='" + it_ec.message() + "')");
                }
            }

            for (const auto& file : file_paths) {
                SyncFileIfPresent(file);
            }
            std::sort(
                dir_paths.begin(),
                dir_paths.end(),
                [](const std::filesystem::path& lhs, const std::filesystem::path& rhs) {
                    return PathDepth(lhs) > PathDepth(rhs);
                });
            for (const auto& dir : dir_paths) {
                SyncDirIfPresent(dir);
            }
            SyncDirIfPresent(data_dir_);
            stats_wal_barrier_full_syncs_.fetch_add(1, std::memory_order_relaxed);
        } else {
            // Files first so their contents are durable before the directory
            // entries that reference them.
            for (const auto& file : files) {
                SyncFileIfPresent(file);
            }
            std::vector<std::filesystem::path> dir_paths;
            dir_paths.reserve(dirs.size());
            for (const auto& dir : dirs) {
                dir_paths.emplace_back(dir);
            }
            std::sort(
                dir_paths.begin(),
                dir_paths.end(),
                [](const std::filesystem::path& lhs, const std::filesystem::path& rhs) {
                    return PathDepth(lhs) > PathDepth(rhs);
                });
            for (const auto& dir : dir_paths) {
                SyncDirIfPresent(dir);
            }
            // Directory entries for newly created per-large-chunk directories
            // live in the data directory itself; sync it so those creations
            // (and any GC removals) are durable too.
            SyncDirIfPresent(data_dir_);
        }
    } catch (...) {
        // Restore the drained bookkeeping so a retried barrier still covers
        // the artifacts this one failed to sync.
        std::lock_guard lock(unsynced_mutex_);
        if (overflow ||
            files.size() + dirs.size() + unsynced_files_.size() + unsynced_dirs_.size() >
                kMaxUnsyncedTracked) {
            unsynced_overflow_ = true;
            unsynced_files_.clear();
            unsynced_dirs_.clear();
        } else {
            unsynced_files_.insert(files.begin(), files.end());
            unsynced_dirs_.insert(dirs.begin(), dirs.end());
        }
        throw;
    }

    // Set while checkpoint publication is still excluded. A checkpoint that
    // was waiting behind this barrier observes the floor and durably publishes
    // its replacement before removing the WAL this barrier just synced.
    barrier_durability_floor_.store(true, std::memory_order_release);
    stats_wal_barriers_.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace chunkdb
