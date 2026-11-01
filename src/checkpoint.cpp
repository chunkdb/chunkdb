#include "checkpoint.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <vector>

#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/zrle.hpp"
#include "durability_io.hpp"
#include "feature_flags.hpp"
#include "feed_slots.hpp"
#include "feed_prefix.hpp"
#include "wal_stream_pool.hpp"
#include "wal_writer.hpp"
#include "chunk_coordinate_index.hpp"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace chunkdb {

[[nodiscard]] std::size_t CheckpointHysteresisTarget(std::size_t lower_bound) noexcept {
    if (lower_bound <= 1) {
        return lower_bound;
    }

    const std::size_t extra = std::max<std::size_t>(1, lower_bound / 2);
    if (lower_bound > std::numeric_limits<std::size_t>::max() - extra) {
        return std::numeric_limits<std::size_t>::max();
    }
    return lower_bound + extra;
}

[[nodiscard]] bool CheckpointMetricReachedTrigger(std::size_t value, std::size_t lower_bound) noexcept {
    if (lower_bound == 0) {
        return false;
    }
    return value >= CheckpointHysteresisTarget(lower_bound);
}
bool ChunkStore::IsCheckpointDue(const std::shared_ptr<RegularChunk>& chunk) noexcept {
    if (chunk == nullptr) {
        return false;
    }

    const bool updates_eligible = chunk->pending_updates >= checkpoint_update_interval_;
    const bool wal_eligible = chunk->wal_bytes >= checkpoint_wal_bytes_;
    const bool eligible = updates_eligible || wal_eligible;

    if (!eligible) {
        chunk->checkpoint_due_armed = false;
        return false;
    }

    chunk->checkpoint_due_armed = true;

    if (CheckpointMetricReachedTrigger(chunk->pending_updates, checkpoint_update_interval_) ||
        CheckpointMetricReachedTrigger(chunk->wal_bytes, checkpoint_wal_bytes_)) {
        return true;
    }

    return false;
}

void ChunkStore::MaybeCheckpointChunk(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool* out_image_committed) {
    if (out_image_committed != nullptr) {
        *out_image_committed = false;
    }
    if (!IsCheckpointDue(chunk)) {
        return;
    }

    if (background_maintenance_ && !chunk->background_checkpoint_failed) {
        // Bounded deferral: while checkpoints run in the background the WAL
        // may keep growing, but only up to a hard multiple of the configured
        // thresholds. Past that the writer checkpoints inline (backpressure).
        const bool hard_bound_exceeded =
            chunk->pending_updates >= checkpoint_update_interval_ * 4U ||
            chunk->wal_bytes >= checkpoint_wal_bytes_ * 4U;
        if (!hard_bound_exceeded && EnqueueBackgroundCheckpoint(chunk_coord)) {
            return;
        }
    }

    // A failed background checkpoint is retried inline on the next eligible
    // write so the error reaches a caller instead of only the log.
    if (CheckpointChunk(chunk_coord, chunk, out_image_committed))
        chunk->background_checkpoint_failed = false;
}

void ChunkStore::CheckpointForTests(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(chunk->mutex);
    CheckpointChunk(chunk_coord, chunk);
}

void ChunkStore::EnsureImageDirectoryPrepared(const std::filesystem::path& directory) {
    const std::string key = CanonicalPathKey(directory);
    {
        std::lock_guard lock(image_dir_cache_mutex_);
        if (image_dir_cache_.contains(key)) {
            return;
        }
    }
    if (!EnsureDirectoryPathExists(directory, /*durable_sync=*/true)) {
        SyncParentDirectory(directory);
    }
    std::lock_guard lock(image_dir_cache_mutex_);
    image_dir_cache_.insert(key);
}

bool ChunkStore::CheckpointChunk(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool* out_image_committed) {
    if (out_image_committed != nullptr) {
        *out_image_committed = false;
    }
    std::shared_lock maintenance_lock(backup_maintenance_mutex_, std::try_to_lock);
    if (!maintenance_lock.owns_lock()) {
        chunk->checkpoint_due_armed = true;
        return false;
    }
    // A poisoned store may hold a rejected frame in a WAL that a pending
    // rollback intent still needs at the next start; replacing that WAL with
    // an image would make the store unopenable.
    ThrowIfDurabilityPoisoned();
    // The image and the WAL change, in relaxed mode without a sync.
    chunk->wal_boundary_durable = false;
    if (!ChunkPopulated(geometry_, chunk->presence_bitmap, chunk->chunk_columns.get())) {
        // The retained producing boundary replaces the image as replay base.
        // Make the producing WAL durable before publishing that authority.
        FlushWalBatch(chunk_coord, chunk, true);
    }
    if (feed_slots_->ArchiveRequired()) {
        // Nonempty checkpoints must not publish an image ahead of staged
        // history. The archive keeps each individual frame, including relaxed
        // group-commit frames that ordinary checkpoints absorb into the image.
        FlushWalBatch(chunk_coord, chunk, false);
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_WAL_FLUSH_ONCE")) std::_Exit(86);
    }
    SnapshotGenerationWriteGuard snapshot_write(this);
    bool image_committed = false;
    const auto data_path = ChunkDataPath(data_dir_, geometry_, chunk_coord);
    const auto wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);

    // Serialize the entire publish step (image replace + WAL removal + unsynced
    // bookkeeping) against WalBarrier's drain, which sets the durability floor.
    // Without this a barrier could drain the tracked WAL, then this checkpoint
    // could remove that WAL and add an unsynced replacement image that the
    // in-flight barrier never syncs, silently dropping a write the barrier
    // promised durable. With it, the publication either completes before the
    // drain, which hands its unsynced artifacts to the barrier, or reads the
    // floor below and publishes strictly. Lock ordering is chunk->mutex
    // (already held) -> checkpoint_publish_mutex_; WalBarrier's drain takes
    // only checkpoint_publish_mutex_, so there is no cycle.
    NoteCheckpointPublishAttemptForTests();
    std::unique_lock<std::mutex> publish_lock(checkpoint_publish_mutex_);
    if (barrier_syncing_.load(std::memory_order_acquire)) {
        checkpoint_publishes_during_barrier_syncs_.fetch_add(1, std::memory_order_relaxed);
    }

    try {
        const bool archive_enabled = feed_slots_->ArchiveRequired();
        const auto archive_path = archive_enabled ? feed_slots_->PrepareArchive(chunk_coord) : std::filesystem::path{};
        // The checkpoint replaces the WAL, so in every mode where
        // acknowledgements promise durability (fsync-wal as well as
        // fsync-checkpoint) the image must be durable before the WAL is
        // removed; otherwise removing a durable WAL would silently downgrade
        // the contract to the strength of an unsynced image.
        const bool strict =
            durability_mode_ != DurabilityMode::kRelaxed ||
            barrier_durability_floor_.load(std::memory_order_acquire) || archive_enabled;
        const bool chunk_populated = ChunkPopulated(geometry_, chunk->presence_bitmap, chunk->chunk_columns.get());
        if (!chunk_populated) {
            if (chunk->version != 0U) {
                if (std::filesystem::exists(wal_path)) SyncFilePath(wal_path);
                coordinate_index_->PublishTombstone({.kind = ChunkIndexKind::kDeleted, .coord = chunk_coord,
                    .revision = chunk->version, .commit_time_ms = chunk->commit_time_ms,
                    .schema_version = geometry_.chunk_schema().version});
            }
            // Empty-chunk garbage collection: a chunk with no present blocks
            // is observably identical to an absent chunk, so its storage
            // artifacts are reclaimed instead of writing an empty image. The
            // data image is removed before the WAL so a crash between the two
            // steps replays the (empty-state) WAL over an absent image.
            std::error_code remove_ec;
            std::filesystem::remove(data_path, remove_ec);
            if (remove_ec) {
                throw std::runtime_error(
                    "failed to remove empty chunk image: " + data_path.string() +
                    " (ec=" + std::to_string(remove_ec.value()) +
                    ", msg='" + remove_ec.message() + "')");
            }
            if (ConsumeFailpointEnv(
                    "CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_IMAGE_REMOVE_ONCE")) {
                throw std::runtime_error(
                    "injected empty-chunk GC failure after image removal");
            }
            // In strict modes, make the image removal durable before
            // deleting the WAL that carries the empty state. A crash at
            // any later boundary then sees either the old image plus the
            // empty WAL, or no image plus the empty WAL.
            if (strict) {
                SyncDirectoryPath(data_path.parent_path());
                if (ConsumeFailpointEnv(
                        "CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_IMAGE_DIR_SYNC_ONCE")) {
                    throw std::runtime_error(
                        "injected empty-chunk GC failure after image directory sync");
                }
            } else {
                NoteUnsyncedDir(data_path.parent_path());
            }
            image_committed = true;
            stats_empty_chunk_gcs_.fetch_add(1, std::memory_order_relaxed);
        } else {
            const auto image = SerializeChunkImage(
                geometry_,
                chunk_coord,
                chunk->payload,
                chunk->presence_bitmap,
                checkpoint_compression_,
                chunk->version,
                chunk->commit_time_ms,
                store_id_,
                &chunk->vars,
                chunk->chunk_columns.get());
            if (ConsumeFailpointEnv(
                    "CHUNKDB_FAILPOINT_CHECKPOINT_BEFORE_IMAGE_REPLACE_ONCE")) {
                throw std::runtime_error(
                    "injected checkpoint failure before image replacement");
            }
            if (strict) {
                EnsureImageDirectoryPrepared(data_path.parent_path());
            }
            AtomicWrite(data_path, image, strict, strict, &image_committed);
            if (checkpoint_compression_ == CheckpointCompression::kZrle) {
                stats_compressed_checkpoint_images_.fetch_add(1, std::memory_order_relaxed);
            }
            if (!strict) {
                NoteUnsyncedFile(data_path);
            }
        }

        if (ConsumeFailpointEnv(
                "CHUNKDB_FAILPOINT_CHECKPOINT_AFTER_IMAGE_REPLACE_ONCE")) {
            throw std::runtime_error(
                "injected checkpoint failure after image replacement");
        }
        PauseCheckpointBeforeWalRemovalForTests();
        if (archive_enabled && ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_IMAGE_PUBLISH_ONCE")) std::_Exit(86);
        CloseWalAppendStream(chunk);
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CHECKPOINT_WAL_REMOVE_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected checkpoint WAL removal failure: " + wal_path.string());
        }
        std::error_code ec;
        if (!archive_path.empty()) {
            ec = MovePathNoReplace(wal_path, archive_path);
            if (ec) {
                // A failed exclusive-rename fallback may have published the
                // archive without removing the live name. Do not continue
                // writing ambiguous duplicate segments.
                if (std::filesystem::exists(archive_path))
                    PoisonDurability("archive WAL rename failed after target publication: " + ec.message());
            }
        } else {
            std::filesystem::remove(wal_path, ec);
        }
        if (ec) {
            throw std::runtime_error(
                "failed to remove checkpointed WAL: " + wal_path.string() +
                " (ec=" + std::to_string(ec.value()) +
                ", msg='" + ec.message() + "')");
        }
        // The WAL is gone even if a later step fails: the next append must
        // start a new WAL with its header (and sync its directory entry), and
        // the WAL size that schedules checkpoints starts again from zero.
        chunk->wal_header_written = false;
        chunk->wal_bytes = 0;
        if (feed_slots_) feed_slots_->prefix_index().Truncate(chunk_coord, 0U);
        if (!archive_path.empty()) {
            SyncDirectoryPath(archive_path.parent_path());
            SyncDirectoryPath(wal_path.parent_path());
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_WAL_RENAME_ONCE")) std::_Exit(86);
        }
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_WAL_REMOVE_ONCE")) {
            throw std::runtime_error(
                "injected empty-chunk GC failure after WAL removal");
        }
        // The image and the WAL share the chunk's large-chunk directory.
        if (strict) {
            SyncDirectoryPath(data_path.parent_path());
        } else {
            NoteUnsyncedDir(data_path.parent_path());
        }

        if (!chunk_populated) {
            // Opportunistically drop the per-large-chunk directory once it is
            // empty. Losing the race against a concurrent create is fine: the
            // remove fails with directory-not-empty, or the creator retries
            // through the existing invalidate-and-recreate path.
            const auto parent = data_path.parent_path();
            std::error_code dir_ec;
            const bool dir_removed = std::filesystem::remove(parent, dir_ec);
            if (dir_ec && dir_ec != std::errc::directory_not_empty &&
                dir_ec != std::errc::no_such_file_or_directory) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kStore,
                    "failed to remove empty chunk directory",
                    {
                        {"path", parent.string()},
                        {"ec", std::to_string(dir_ec.value())},
                        {"msg", dir_ec.message()},
                    });
            }
            if (dir_removed) {
                InvalidateWalParentDirectoryCache(parent);
                {
                    std::lock_guard lock(image_dir_cache_mutex_);
                    image_dir_cache_.erase(CanonicalPathKey(parent));
                }
                if (strict) {
                    SyncDirectoryPath(data_dir_);
                } else {
                    NoteUnsyncedDir(data_dir_);
                }
            }
        }

        stats_checkpoints_.fetch_add(1, std::memory_order_relaxed);
        chunk->pending_updates = 0;
        chunk->wal_bytes = 0;
        chunk->checkpoint_due_armed = false;
        chunk->deferred_wal_compaction = false;
        chunk->pending_wal_flush_updates = 0;
        chunk->wal_batch.clear();
        chunk->wal_header_written = false;
        if (out_image_committed != nullptr) {
            *out_image_committed = image_committed;
        }
        snapshot_write.Finish();
        return true;
    } catch (const std::exception& e) {
        if (out_image_committed != nullptr) {
            *out_image_committed = image_committed;
        }
        // Every checkpoint failure boundary leaves a reader-safe old/new
        // combination: the prior WAL remains until image publication is
        // complete, and a published image may coexist with that WAL. Close
        // this generation so the existing same-process retry path remains
        // available; a failure publishing the even generation still leaves
        // the store odd and fail-closed.
        snapshot_write.Finish();
        LogMessage(
            LogLevel::kError,
            LogComponent::kRecovery,
            "checkpoint failed",
            {
                {"chunk_x", std::to_string(chunk_coord.x)},
                {"chunk_y", std::to_string(chunk_coord.y)},
                {"image_committed", image_committed ? "true" : "false"},
                {"error", e.what()},
            });
        throw;
    }
}

}  // namespace chunkdb
