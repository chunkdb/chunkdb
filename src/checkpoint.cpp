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
#include "wal_stream_pool.hpp"
#include "wal_writer.hpp"

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
    CheckpointChunk(chunk_coord, chunk, out_image_committed);
    chunk->background_checkpoint_failed = false;
}

void ChunkStore::CheckpointForTests(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(chunk->mutex);
    CheckpointChunk(chunk_coord, chunk);
}

void ChunkStore::CheckpointChunk(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool* out_image_committed) {
    if (out_image_committed != nullptr) {
        *out_image_committed = false;
    }
    // A poisoned store may hold a rejected frame in a WAL that a pending
    // rollback intent still needs at the next start; replacing that WAL with
    // an image would make the store unopenable.
    ThrowIfDurabilityPoisoned();
    if (!ChunkPresent(chunk->presence_bitmap)) {
        // Empty-chunk collection removes the image before the WAL, and a
        // crash or failure between the two replays the WAL over no image.
        // That must end in the empty state. A WAL that outlived an earlier
        // checkpoint is only right over that image: replay skips its older
        // frames, and frames the image took from the batch may never have
        // reached it. A last frame that sets the whole state to empty makes
        // such a WAL end empty on its own. The batch (with that frame) is
        // flushed before the publish lock, which the barrier takes after the
        // flush path's own locks.
        if (std::filesystem::exists(ChunkDataPath(data_dir_, geometry_, chunk_coord))) {
            const std::vector<std::uint8_t> payload(chunk->payload.size(), 0U);
            const std::vector<std::uint8_t> presence(chunk->presence_bitmap.size(), 0U);
            WalFrameBuilder frame(&chunk->wal_batch);
            frame.AppendSpan(0U, payload.data(), payload.size());
            frame.AppendSpan(
                static_cast<std::uint32_t>(geometry_.ChunkPayloadBytes()), presence.data(), presence.size());
            if (HasExtraData(features_)) {
                frame.AppendExtraReplace(ChunkExtra{});
            }
            const std::size_t appended_bytes = frame.Finish(NextChunkVersion(), NextCommitTimeMs(*chunk));
            chunk->pending_wal_flush_updates += frame.record_count();
            chunk->wal_bytes += appended_bytes;
        }
        FlushWalBatch(
            chunk_coord,
            chunk,
            durability_mode_ != DurabilityMode::kRelaxed ||
                barrier_durability_floor_.load(std::memory_order_acquire));
    }
    SnapshotGenerationWriteGuard snapshot_write(this);
    bool image_committed = false;
    const auto data_path = ChunkDataPath(data_dir_, geometry_, chunk_coord);
    const auto wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);

    // Serialize the entire publish step (image replace + WAL removal + unsynced
    // bookkeeping) against WalBarrier's drain+sync. Without this a barrier could
    // drain the tracked WAL, then this checkpoint could remove that WAL and add
    // an unsynced replacement image that the in-flight barrier never syncs,
    // silently dropping a write the barrier promised durable. Lock ordering is
    // chunk->mutex (already held) -> checkpoint_publish_mutex_;
    // WalBarrier's step 2 takes only checkpoint_publish_mutex_, so there is no
    // cycle.
    NoteCheckpointPublishAttemptForTests();
    std::unique_lock<std::mutex> publish_lock(checkpoint_publish_mutex_);

    try {
        // The checkpoint replaces the WAL, so in every mode where
        // acknowledgements promise durability (fsync-wal as well as
        // fsync-checkpoint) the image must be durable before the WAL is
        // removed; otherwise removing a durable WAL would silently downgrade
        // the contract to the strength of an unsynced image.
        const bool strict =
            durability_mode_ != DurabilityMode::kRelaxed ||
            barrier_durability_floor_.load(std::memory_order_acquire);
        const bool chunk_populated = ChunkPresent(chunk->presence_bitmap);
        if (!chunk_populated) {
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
                &chunk->extra);
            if (ConsumeFailpointEnv(
                    "CHUNKDB_FAILPOINT_CHECKPOINT_BEFORE_IMAGE_REPLACE_ONCE")) {
                throw std::runtime_error(
                    "injected checkpoint failure before image replacement");
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
        CloseWalAppendStream(chunk);
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CHECKPOINT_WAL_REMOVE_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected checkpoint WAL removal failure: " + wal_path.string());
        }
        std::error_code ec;
        std::filesystem::remove(wal_path, ec);
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
