#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "chunkdb/geometry.hpp"
#include "chunkdb/server_defaults.hpp"
#include "chunkdb/types.hpp"

namespace chunkdb {

#if defined(__MINGW32__) && \
    (!defined(CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS) || CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS)
// MinGW winpthreads shared_mutex has shown unstable lock assertions under high contention
// in CI; fallback preserves correctness by serializing shared/exclusive access on Windows+MinGW.
class RegularChunkMutex {
  public:
    void lock() { mutex_.lock(); }
    bool try_lock() { return mutex_.try_lock(); }
    void unlock() { mutex_.unlock(); }

    void lock_shared() { mutex_.lock(); }
    bool try_lock_shared() { return mutex_.try_lock(); }
    void unlock_shared() { mutex_.unlock(); }

  private:
    std::mutex mutex_;
};
inline constexpr const char* kChunkLockModeName = "serial-mutex";
#else
using RegularChunkMutex = std::shared_mutex;
inline constexpr const char* kChunkLockModeName = "shared-mutex";
#endif

[[nodiscard]] inline constexpr const char* ChunkLockModeName() noexcept {
    return kChunkLockModeName;
}

enum class DurabilityMode {
    kRelaxed = 0,
    kFsyncWal = 1,
    kFsyncCheckpoint = 2,
};

enum class AccessMode {
    kReadWrite = 0,
    kReadOnly = 1,
};

enum class StorageLayoutMode {
    kFsSplitV1 = 0,
    kFsRegionV1Experimental = 1,
};

enum class CheckpointCompression {
    kNone = 0,
    kZrle = 1,
};

enum class ConditionalMutationPausePoint {
    kNone = 0,
    kBeforeRollbackIntentPublish,
    kAfterRollbackIntentPublish,
    kAfterWalAppend,
    kAfterCommitIntentPublish,
    kAfterCommitIntentUnlink,
};

enum class ReadOnlySnapshotArtifact {
    kImage,
    kWal,
    kIntent,
};

struct ReadOnlySnapshotPausePoint {
    std::size_t collection = 0;
    ReadOnlySnapshotArtifact artifact = ReadOnlySnapshotArtifact::kImage;
};

[[nodiscard]] CheckpointCompression ParseCheckpointCompression(std::string_view text);
[[nodiscard]] const char* CheckpointCompressionName(CheckpointCompression compression) noexcept;

[[nodiscard]] DurabilityMode ParseDurabilityMode(std::string_view text);
[[nodiscard]] const char* DurabilityModeName(DurabilityMode mode) noexcept;
[[nodiscard]] const char* AccessModeName(AccessMode mode) noexcept;
[[nodiscard]] StorageLayoutMode ParseStorageLayoutMode(std::string_view text);
[[nodiscard]] const char* StorageLayoutModeName(StorageLayoutMode mode) noexcept;

struct StoreRuntimeStats {
    std::uint64_t evictions = 0;
    std::uint64_t checkpoints = 0;
    std::uint64_t wal_batch_flushes = 0;
    std::uint64_t unique_loaded_chunks = 0;
    std::uint64_t open_wal_streams = 0;
    std::uint64_t eviction_snapshot_builds = 0;
    std::uint64_t eviction_probes = 0;
    std::uint64_t eviction_no_progress_cycles = 0;
    std::uint64_t eviction_forced_wal_flushes = 0;
    std::uint64_t eviction_forced_wal_flushes_with_data = 0;
    std::uint64_t eviction_forced_wal_flushes_empty_batch = 0;
    std::uint64_t eviction_recency_skips = 0;
    std::uint64_t empty_chunk_gcs = 0;
    std::uint64_t wal_barriers = 0;
    std::uint64_t wal_barrier_full_syncs = 0;
    std::uint64_t background_checkpoints = 0;
    std::uint64_t background_checkpoint_failures = 0;
    std::uint64_t background_queue_full_inline = 0;
    std::uint64_t background_queue_depth = 0;
    std::uint64_t compressed_checkpoint_images = 0;
};

struct StoreConfig {
    GeometryConfig geometry;
    std::filesystem::path data_dir;

    DurabilityMode durability_mode = DurabilityMode::kRelaxed;
    std::size_t checkpoint_update_interval = 256;
    std::size_t checkpoint_wal_bytes = 1024 * 1024;
    std::size_t wal_group_commit_updates = kDefaultWalGroupCommitUpdates;

    std::size_t max_loaded_chunks = kDefaultMaxLoadedChunks;
    std::size_t max_open_wal_streams = 1024;
    bool allow_multiple_processes = false;
    AccessMode access_mode = AccessMode::kReadWrite;
    StorageLayoutMode storage_layout_mode = StorageLayoutMode::kFsSplitV1;
    std::size_t experimental_region_span_chunks = 16;

    // When true, checkpoint compaction and cache eviction run on a dedicated
    // maintenance thread with a bounded queue instead of on request threads.
    bool background_maintenance = false;
    std::size_t background_checkpoint_queue_limit = 4096;

    // Optional compression for newly written split-layout checkpoint images.
    // Off by default; images written by older versions remain readable
    // either way. Region files are never compressed.
    CheckpointCompression checkpoint_compression = CheckpointCompression::kNone;
};

// Server-side hard limits for world-oriented read operations.
inline constexpr std::size_t kMaxChunkRangeChunks = 256;
inline constexpr std::size_t kMaxChunkScanLimit = 1024;
inline constexpr std::size_t kMaxChunkBatchOps = 1024;
// Hard byte budget for one CHUNKRANGE/CHUNKRADIUS response body. Enforced
// before per-chunk state strings are extracted so a request over a large
// geometry fails with a bounded error instead of allocating the response.
inline constexpr std::size_t kMaxChunkRangeResponseBytes = 64ULL * 1024ULL * 1024ULL;
// One WAL record carries at most this many payload bytes (u16 length field).
// Conditional mutations (CHUNKCAS/CHUNKBATCH) are only accepted when the
// full chunk state fits in a single record, because multi-record spans can
// replay a prefix after a crash and would break their atomicity contract.
inline constexpr std::size_t kMaxAtomicChunkStateBytes = 65535;

struct ChunkScanPage {
    std::vector<ChunkCoord> coords;
    bool has_more = false;
};

struct ChunkRangeEntry {
    ChunkCoord coord;
    std::string payload_bits;
    std::string presence_bits;
};

struct ChunkBatchOp {
    bool set = false;
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::string bits;
};

struct ChunkMutationResult {
    bool ok = false;
    // On success: the chunk version after the mutation.
    // On version mismatch: the current chunk version.
    std::uint64_t version = 0;
};

// Bounded scan-candidate accumulator; defined in world_read.cpp.
class ScanCandidateAccumulator;

class ChunkStore {
  public:
    explicit ChunkStore(StoreConfig config);
    ~ChunkStore();

    ChunkStore(const ChunkStore&) = delete;
    ChunkStore& operator=(const ChunkStore&) = delete;

    [[nodiscard]] const Geometry& geometry() const noexcept { return geometry_; }
    [[nodiscard]] const std::filesystem::path& data_dir() const noexcept { return data_dir_; }
    [[nodiscard]] DurabilityMode durability_mode() const noexcept { return durability_mode_; }
    [[nodiscard]] AccessMode access_mode() const noexcept { return access_mode_; }
    [[nodiscard]] CheckpointCompression checkpoint_compression() const noexcept {
        return checkpoint_compression_;
    }

    [[nodiscard]] bool BlockExists(std::int64_t block_x, std::int64_t block_y);
    [[nodiscard]] std::string GetBlockBits(std::int64_t block_x, std::int64_t block_y);
    void SetBlockBits(std::int64_t block_x, std::int64_t block_y, std::string_view bits);
    void UnsetBlock(std::int64_t block_x, std::int64_t block_y);

    [[nodiscard]] bool ChunkExists(std::int64_t chunk_x, std::int64_t chunk_y);
    void SetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y, std::string_view bits);
    [[nodiscard]] std::string GetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] std::vector<std::uint8_t> GetChunkPayloadBytes(std::int64_t chunk_x, std::int64_t chunk_y);
    void SetChunkStateBits(
        std::int64_t chunk_x,
        std::int64_t chunk_y,
        std::string_view payload_bits,
        std::string_view presence_bits);
    [[nodiscard]] std::string GetChunkStateBits(std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] std::vector<std::uint8_t> GetChunkStateBytes(std::int64_t chunk_x, std::int64_t chunk_y);
    // Packed-byte counterparts of SetChunkBits / SetChunkStateBits. The
    // payload must be exactly ChunkPayloadBytes() long and the presence bitmap
    // exactly ChunkPresenceBitmapBytes() long (the layout CHUNKBIN returns);
    // padding bits past the used range are ignored and stored as zero.
    void SetChunkPayloadBytes(
        std::int64_t chunk_x,
        std::int64_t chunk_y,
        const std::vector<std::uint8_t>& payload);
    void SetChunkStateBytes(
        std::int64_t chunk_x,
        std::int64_t chunk_y,
        const std::vector<std::uint8_t>& payload,
        const std::vector<std::uint8_t>& presence_bitmap);

    // World-oriented reads. Populated-ness of each chunk is evaluated
    // atomically per chunk at read time. Absent chunks probed by these reads
    // are not inserted into the cache, with one bounded exception: after
    // kNoCacheReadAttempts contended attempts on one chunk, the read falls
    // back to the authoritative cache path, which loads (and caches) that
    // chunk to preserve read-your-writes consistency.
    [[nodiscard]] ChunkScanPage ScanPopulatedChunks(
        bool has_cursor,
        ChunkCoord cursor,
        std::size_t limit);
    [[nodiscard]] std::vector<ChunkRangeEntry> ReadChunkRange(
        std::int64_t chunk_x0,
        std::int64_t chunk_y0,
        std::int64_t chunk_x1,
        std::int64_t chunk_y1);
    // Radius-oriented world read: returns the populated chunks whose chunk
    // coordinate lies within Euclidean distance `radius_chunks` of the
    // center, ordered by ascending cx then cy. Bounded by the same chunk
    // count and response-byte limits as ReadChunkRange.
    [[nodiscard]] std::vector<ChunkRangeEntry> ReadChunkRadius(
        std::int64_t center_x,
        std::int64_t center_y,
        std::int64_t radius_chunks);

    // Chunk concurrency primitives. Versions are opaque 64-bit tokens drawn
    // from a store-wide monotonic clock whose ceiling is persisted in the
    // data directory, so on a read-write store a version issued before a
    // mutation, eviction, or restart can never match a version issued
    // afterwards. Read-only stores fall back to random epoch tokens (they
    // cannot persist the clock and reject CAS anyway).
    [[nodiscard]] std::uint64_t GetChunkVersion(std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] ChunkMutationResult CasChunkState(
        std::int64_t chunk_x,
        std::int64_t chunk_y,
        std::uint64_t expected_version,
        std::string_view payload_bits,
        std::string_view presence_bits);
    [[nodiscard]] ChunkMutationResult ApplyChunkBatch(
        std::int64_t chunk_x,
        std::int64_t chunk_y,
        bool has_expected_version,
        std::uint64_t expected_version,
        const std::vector<ChunkBatchOp>& ops);

    // Explicit global durability barrier: when this returns, every write
    // acknowledged before the call began is durable on stable storage,
    // regardless of the configured durability mode. Failures propagate.
    void WalBarrier();

    [[nodiscard]] std::size_t ApproxLoadedChunkCount() const;
    [[nodiscard]] StoreRuntimeStats RuntimeStats() const noexcept;
    [[nodiscard]] std::uint64_t WalOpenCountForTests() const noexcept;
    [[nodiscard]] std::uint64_t WalParentPrepareCountForTests() const noexcept;
    [[nodiscard]] std::uint64_t OpenWalStreamCountForTests() const noexcept;
    [[nodiscard]] std::size_t MaxOpenWalStreamsForTests() const noexcept { return max_open_wal_streams_; }
    [[nodiscard]] std::uint64_t EvictionSnapshotBuildCountForTests() const noexcept;
    [[nodiscard]] std::uint64_t EvictionRefillLargeChunkScanCountForTests() const noexcept;
    [[nodiscard]] std::size_t EvictionLargeChunkRingSizeForTests() const noexcept;
    [[nodiscard]] std::uint64_t EvictionPostPassLargeChunkCheckCountForTests() const noexcept;
    void ClearEvictionCandidatesForTests();
    [[nodiscard]] bool IsChunkLoadedForTests(std::int64_t chunk_x, std::int64_t chunk_y) const;
    void ForceUnsyncedOverflowForTests();
    [[nodiscard]] std::size_t UnsyncedTrackedCountForTests() const;
    [[nodiscard]] bool UnsyncedOverflowFlagForTests() const;
    void ArmBarrierAfterDrainPauseForTests();
    [[nodiscard]] bool WaitForBarrierAfterDrainForTests();
    void ResumeBarrierAfterDrainForTests();
    void ArmCheckpointBeforeWalRemovalPauseForTests();
    [[nodiscard]] bool WaitForCheckpointBeforeWalRemovalForTests();
    void ResumeCheckpointBeforeWalRemovalForTests();
    void ArmCheckpointPublishAttemptForTests();
    [[nodiscard]] bool WaitForCheckpointPublishAttemptForTests();
    void ArmConditionalMutationPauseForTests(
        ConditionalMutationPausePoint point);
    [[nodiscard]] bool WaitForConditionalMutationPauseForTests();
    void ResumeConditionalMutationForTests();
    void ArmReadOnlySnapshotPausesForTests(
        std::vector<ReadOnlySnapshotPausePoint> points);
    [[nodiscard]] bool WaitForReadOnlySnapshotPauseForTests();
    void ResumeReadOnlySnapshotForTests();

  private:
    class SnapshotGenerationWriteGuard {
      public:
        explicit SnapshotGenerationWriteGuard(ChunkStore* store);
        ~SnapshotGenerationWriteGuard();

        SnapshotGenerationWriteGuard(
            const SnapshotGenerationWriteGuard&) = delete;
        SnapshotGenerationWriteGuard& operator=(
            const SnapshotGenerationWriteGuard&) = delete;

        void Finish();

      private:
        ChunkStore* store_ = nullptr;
        bool nested_on_same_thread_ = false;
        bool finished_ = false;
    };

    struct RegularChunk {
        explicit RegularChunk(
            std::vector<std::uint8_t> payload_bytes,
            std::vector<std::uint8_t> presence_bytes)
            : payload(std::move(payload_bytes)),
              presence_bitmap(std::move(presence_bytes)) {}

        std::vector<std::uint8_t> payload;
        std::vector<std::uint8_t> presence_bitmap;
        std::size_t pending_updates = 0;
        std::size_t wal_bytes = 0;
        bool checkpoint_due_armed = false;
        bool deferred_wal_compaction = false;

        std::size_t pending_wal_flush_updates = 0;
        std::uint64_t version = 0;
        bool background_checkpoint_failed = false;
        std::vector<std::uint8_t> wal_batch;
        std::vector<std::uint8_t> scratch_before;
        std::filesystem::path wal_path;
        std::ofstream wal_append_stream;
        bool wal_header_written = false;
        // Mirrors wal_append_stream.is_open(). Written only under `mutex`;
        // atomic because the WAL stream cache reads it for other chunks under
        // wal_stream_cache_mutex_ alone, where touching the ofstream is a race.
        std::atomic<bool> wal_stream_initialized{false};

        std::atomic<std::uint64_t> last_access_tick{0};
        mutable RegularChunkMutex mutex;
    };

    struct EvictionCandidate {
        LargeChunkCoord large_coord;
        ChunkCoord chunk_coord;
        std::uint64_t recorded_tick = 0;
    };

    struct LargeChunk {
        std::mutex mutex;
        std::unordered_map<ChunkCoord, std::shared_ptr<RegularChunk>, ChunkCoordHash> chunks;
    };

    Geometry geometry_;
    std::filesystem::path data_dir_;
    DurabilityMode durability_mode_;
    AccessMode access_mode_;
    StorageLayoutMode storage_layout_mode_;
    std::size_t experimental_region_span_chunks_;
    std::size_t checkpoint_update_interval_;
    std::size_t checkpoint_wal_bytes_;
    std::size_t wal_group_commit_updates_;
    std::size_t max_loaded_chunks_;
    std::size_t max_open_wal_streams_;
    CheckpointCompression checkpoint_compression_ = CheckpointCompression::kNone;

#ifdef _WIN32
    void* process_lock_handle_ = nullptr;
#else
    int process_lock_fd_ = -1;
#endif

    std::filesystem::path process_lock_dir_;
    std::filesystem::path process_lock_file_path_;
    std::filesystem::path process_lock_meta_path_;
    std::string process_lock_session_id_;
    std::atomic<bool> process_lock_heartbeat_stop_{false};
    std::thread process_lock_heartbeat_thread_;
    mutable std::mutex process_lock_meta_mutex_;

    std::filesystem::path snapshot_generation_path_;
    std::uint64_t snapshot_generation_ = 0;
    std::size_t snapshot_generation_active_writers_ = 0;
    bool snapshot_generation_epoch_failed_ = false;
    std::mutex snapshot_generation_mutex_;

    std::atomic<std::uint64_t> access_clock_{0};

    std::atomic<std::uint64_t> loaded_chunk_count_{0};
    std::atomic<std::uint64_t> stats_evictions_{0};
    std::atomic<std::uint64_t> stats_checkpoints_{0};
    std::atomic<std::uint64_t> stats_wal_batch_flushes_{0};
    std::atomic<std::uint64_t> stats_unique_loaded_chunks_{0};
    std::atomic<std::uint64_t> stats_wal_open_count_{0};
    std::atomic<std::uint64_t> stats_open_wal_streams_current_{0};
    std::atomic<std::uint64_t> stats_eviction_snapshot_builds_{0};
    std::atomic<std::uint64_t> stats_eviction_probes_{0};
    std::atomic<std::uint64_t> stats_eviction_no_progress_cycles_{0};
    std::atomic<std::uint64_t> stats_eviction_post_pass_large_chunk_checks_{0};
    std::atomic<std::uint64_t> stats_eviction_forced_wal_flushes_{0};
    std::atomic<std::uint64_t> stats_eviction_forced_wal_flushes_with_data_{0};
    std::atomic<std::uint64_t> stats_eviction_forced_wal_flushes_empty_batch_{0};
    std::atomic<std::uint64_t> stats_eviction_refill_large_chunk_scans_{0};
    std::atomic<std::uint64_t> stats_wal_parent_prepare_calls_{0};
    std::atomic<std::uint64_t> stats_eviction_recency_skips_{0};
    std::atomic<std::uint64_t> stats_empty_chunk_gcs_{0};
    std::atomic<std::uint64_t> stats_wal_barriers_{0};
    std::atomic<std::uint64_t> stats_wal_barrier_full_syncs_{0};
    std::atomic<std::uint64_t> stats_background_checkpoints_{0};
    std::atomic<std::uint64_t> stats_background_checkpoint_failures_{0};
    std::atomic<std::uint64_t> stats_background_eviction_failures_{0};
    std::atomic<std::uint64_t> stats_background_queue_full_inline_{0};
    std::atomic<std::uint64_t> stats_compressed_checkpoint_images_{0};
    std::atomic<std::uint64_t> wal_stream_clock_{0};

    // Store-wide monotonic chunk version clock. Versions are issued strictly
    // below version_clock_ceiling_, and the ceiling is persisted (fsynced)
    // before any version in its range is issued, so versions never repeat
    // across eviction or restart of a read-write store. Read-only stores
    // leave the clock unused and issue random epoch tokens instead.
    std::atomic<std::uint64_t> version_clock_{0};
    std::atomic<std::uint64_t> version_clock_ceiling_{0};
    std::mutex version_clock_mutex_;
    std::filesystem::path version_clock_path_;

    // Set when a durability/rollback step could not be completed. Once set the
    // store refuses further mutations and barriers (fail-closed); the reason is
    // preserved for the error surfaced to callers.
    std::atomic<bool> durability_poisoned_{false};
    mutable std::mutex poison_mutex_;
    std::string poison_reason_;

    // Serializes a checkpoint's image-replace + WAL-removal "publish" step
    // against WalBarrier's drain+sync, so a barrier can never observe a tracked
    // WAL disappear without also covering the replacement image that now
    // carries the promised state.
    mutable std::mutex checkpoint_publish_mutex_;

    // Files/directories written without an fsync in relaxed paths. The WAL
    // barrier drains this set. When the set would exceed its bound, the
    // overflow flag forces the next barrier to sync the whole data directory.
    mutable std::mutex unsynced_mutex_;
    std::unordered_set<std::string> unsynced_files_;
    std::unordered_set<std::string> unsynced_dirs_;
    bool unsynced_overflow_ = false;
    // Serializes concurrent WalBarrier callers so each caller's guarantee
    // covers everything acknowledged before its own call began.
    mutable std::mutex wal_barrier_mutex_;
    mutable std::mutex durability_hook_mutex_;
    std::condition_variable durability_hook_cv_;
    bool barrier_after_drain_armed_ = false;
    bool barrier_after_drain_reached_ = false;
    bool barrier_after_drain_resumed_ = false;
    bool checkpoint_before_wal_remove_armed_ = false;
    bool checkpoint_before_wal_remove_reached_ = false;
    bool checkpoint_before_wal_remove_resumed_ = false;
    bool checkpoint_publish_attempt_armed_ = false;
    bool checkpoint_publish_attempt_reached_ = false;
    ConditionalMutationPausePoint conditional_pause_point_ =
        ConditionalMutationPausePoint::kNone;
    bool conditional_pause_reached_ = false;
    bool conditional_pause_resumed_ = false;
    mutable std::mutex read_only_snapshot_pause_mutex_;
    std::condition_variable read_only_snapshot_pause_cv_;
    std::vector<ReadOnlySnapshotPausePoint> read_only_snapshot_pause_points_;
    std::size_t read_only_snapshot_pause_index_ = 0;
    bool read_only_snapshot_pause_reached_ = false;
    bool read_only_snapshot_pause_resumed_ = false;
    // Once a successful barrier has established a durability floor, later
    // relaxed-mode checkpoint/GC replacement must not downgrade durable state
    // by deleting a synced WAL in favor of an unsynced image.
    std::atomic<bool> barrier_durability_floor_{false};

    // Background maintenance (checkpoints + eviction) state.
    bool background_maintenance_ = false;
    std::size_t background_checkpoint_queue_limit_ = 4096;
    mutable std::mutex maintenance_mutex_;
    std::condition_variable maintenance_cv_;
    std::vector<ChunkCoord> maintenance_checkpoint_queue_;
    std::unordered_set<std::string> maintenance_checkpoint_queued_keys_;
    bool maintenance_eviction_requested_ = false;
    bool maintenance_stop_ = false;
    std::thread maintenance_thread_;

    struct WalStreamState {
        std::weak_ptr<RegularChunk> chunk;
        std::uint64_t last_used_tick = 0;
    };
    mutable std::mutex wal_open_mutex_;
    mutable std::mutex wal_stream_cache_mutex_;
    mutable std::condition_variable wal_stream_cache_cv_;
    std::unordered_map<RegularChunk*, WalStreamState> open_wal_streams_;
    mutable std::mutex wal_parent_cache_mutex_;
    std::unordered_set<std::string> wal_parent_dir_cache_;

    mutable std::mutex large_chunks_mutex_;
    std::unordered_map<LargeChunkCoord, std::shared_ptr<LargeChunk>, LargeChunkCoordHash> large_chunks_;
    std::vector<LargeChunkCoord> eviction_large_chunk_ring_;
    std::size_t eviction_large_chunk_cursor_ = 0;
    mutable std::mutex eviction_state_mutex_;
    std::vector<EvictionCandidate> eviction_candidates_;
    std::size_t eviction_cursor_ = 0;

    struct LoadedChunkPayload {
        std::vector<std::uint8_t> payload;
        std::vector<std::uint8_t> presence_bitmap;
        std::size_t wal_bytes = 0;
        bool deferred_wal_compaction = false;
        bool wal_header_written = false;
        std::filesystem::path wal_path;
    };

    [[nodiscard]] std::shared_ptr<LargeChunk> GetOrCreateLargeChunk(const LargeChunkCoord& large_coord);
    [[nodiscard]] std::shared_ptr<RegularChunk> GetOrLoadRegularChunk(const ChunkCoord& chunk_coord);

    [[nodiscard]] std::vector<std::uint8_t> EmptyPayload() const;
    [[nodiscard]] std::vector<std::uint8_t> EmptyPresenceBitmap() const;
    // Shared tail of every full-chunk replace: takes canonical-size packed
    // buffers, canonicalizes absent blocks, and applies them under the chunk
    // lock with the ordinary WAL/rollback discipline.
    void ApplyChunkState(
        const ChunkCoord& chunk_coord,
        std::vector<std::uint8_t> payload,
        std::vector<std::uint8_t> presence_bitmap);
    [[nodiscard]] LoadedChunkPayload LoadChunkPayload(const ChunkCoord& chunk_coord);

    void TouchChunk(const std::shared_ptr<RegularChunk>& chunk) noexcept;
    void RegisterEvictionCandidate(
        const LargeChunkCoord& large_coord,
        const ChunkCoord& chunk_coord,
        std::uint64_t recorded_tick);
    void RemoveLargeChunkFromEvictionRing(const LargeChunkCoord& large_coord);
    [[nodiscard]] bool RefillEvictionCandidatesBounded();
    [[nodiscard]] bool TryEvictCandidate(
        const EvictionCandidate& candidate,
        bool respect_recency,
        std::size_t* removed,
        std::vector<LargeChunkCoord>* maybe_empty_large_chunks);
    void MaybeEvictChunks();
    void RequestEviction();

    [[nodiscard]] std::shared_ptr<RegularChunk> TryGetLoadedChunk(const ChunkCoord& chunk_coord) const;
    [[nodiscard]] bool ReadPopulatedChunkStateNoCache(
        const ChunkCoord& chunk_coord,
        std::string* payload_bits,
        std::string* presence_bits);
    [[nodiscard]] bool ReadPopulatedChunkStateFromDisk(
        const ChunkCoord& chunk_coord,
        std::string* payload_bits,
        std::string* presence_bits);
    void CollectPopulatedCandidatesFromDisk(
        ScanCandidateAccumulator* candidates) const;
    [[nodiscard]] std::size_t ChunkRangeEntryCostBytes() const noexcept;
    void AppendPopulatedChunkRangeEntry(
        const ChunkCoord& coord,
        std::size_t max_entries,
        const char* operation_name,
        std::vector<ChunkRangeEntry>* entries);

    // Issues the next version token; requires a read-write store.
    [[nodiscard]] std::uint64_t NextChunkVersion();
    // Loads, initializes, or migrates the persisted version clock.
    // `store_preexisting` identifies legacy chunk/WAL/region state so the
    // migration can be reported; only the checked initialized marker proves
    // that this store previously exposed deterministic version tokens.
    void InitializeVersionClock(bool store_preexisting);
    void ExtendVersionClockCeilingLocked(std::uint64_t minimum_exclusive);
    void RecoverConditionalRollbackIntents();
    void InitializeSnapshotGeneration(bool store_preexisting);
    void FinishSnapshotGenerationRecovery();
    void BeginSnapshotGenerationWriteLocked();
    void FinishSnapshotGenerationWriteLocked();
    void AbandonSnapshotGenerationWriteLocked(
        bool fail_epoch) noexcept;

    // Fail-closed durability guard. When a rollback or durability step cannot
    // be completed, the store is poisoned so it stops accepting mutations and
    // barriers rather than continuing to serve while on-disk state may be
    // inconsistent with acknowledged results.
    void PoisonDurability(std::string reason) noexcept;
    void ThrowIfDurabilityPoisoned() const;

    [[nodiscard]] bool ApplyFullChunkStateLocked(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        std::vector<std::uint8_t> new_payload,
        std::vector<std::uint8_t> new_presence);

    void NoteUnsyncedFile(const std::filesystem::path& path);
    void NoteUnsyncedDir(const std::filesystem::path& path);

    void StartMaintenanceThread();
    void StopMaintenanceThread() noexcept;
    void MaintenanceLoop();
    [[nodiscard]] bool EnqueueBackgroundCheckpoint(const ChunkCoord& chunk_coord);
    void RunBackgroundCheckpoint(const ChunkCoord& chunk_coord) noexcept;

    void FlushWalBatch(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool force_sync);
    [[nodiscard]] std::uint64_t CurrentWalFileSize(
        const std::shared_ptr<RegularChunk>& chunk) const;
    // Truncates the chunk's WAL file back to `committed_size` bytes (removing
    // records appended by a failed mutation) and re-syncs it in synced modes.
    // A committed_size of zero removes the WAL file entirely.
    void TruncateWalTail(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        std::uint64_t committed_size,
        bool force_sync);
    [[nodiscard]] std::filesystem::path WriteConditionalRollbackIntent(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        std::uint64_t committed_size);
    [[nodiscard]] bool PublishConditionalCommitIntent(
        const std::filesystem::path& intent_path,
        std::uint64_t committed_size);
    void ClearCommittedConditionalIntent(
        const std::filesystem::path& intent_path);
    void PauseCheckpointBeforeWalRemovalForTests();
    void NoteCheckpointPublishAttemptForTests();
    void PauseConditionalMutationForTests(
        ConditionalMutationPausePoint point);
    void PauseReadOnlySnapshotForTests(
        std::size_t collection,
        ReadOnlySnapshotArtifact artifact);
    void FlushWalBatchForEviction(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool force_sync);
    void EnsureWalAppendStream(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool* first_create,
        bool* artifact_touched);
    void EnsureWalParentDirectoryCached(
        const std::filesystem::path& wal_parent_path,
        bool force_refresh,
        bool durable_sync);
    void InvalidateWalParentDirectoryCache(const std::filesystem::path& wal_parent_path);
    void CloseWalAppendStream(const std::shared_ptr<RegularChunk>& chunk) noexcept;
    [[nodiscard]] bool TryCloseLeastRecentlyUsedIdleWalStream(
        const std::shared_ptr<RegularChunk>& opening_chunk);
    void EnsureWalStreamCapacity(const std::shared_ptr<RegularChunk>& opening_chunk);
    void TouchWalStreamState(const std::shared_ptr<RegularChunk>& chunk) noexcept;

    void FlushAllPendingWalBatches() noexcept;
    [[nodiscard]] bool IsCheckpointDue(const std::shared_ptr<RegularChunk>& chunk) noexcept;

    // Shared tail of every ordinary (non-conditional) mutation. The caller
    // has already staged this mutation's delta records into chunk->wal_batch
    // (memory only). Bumps the pending counters, performs the mode-required
    // flush, and — once the flush has succeeded (the commit point) — assigns
    // the reserved version and runs the inline checkpoint, whose failure is
    // post-commit and therefore logged and retained for retry rather than
    // returned as a command error. A flush failure throws with the WAL file
    // already neutralized by FlushWalBatch's repair; the caller must then
    // restore its memory state, wal_batch, and counters.
    void FinishOrdinaryMutationLocked(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        std::size_t appended_bytes,
        std::size_t appended_record_count,
        std::uint64_t reserved_version);

    void MaybeCheckpointChunk(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool* out_image_committed = nullptr);
    // Writes/removes the on-disk image for the chunk and drops the WAL. When
    // `out_image_committed` is non-null it is set true once the on-disk image
    // reflects the checkpoint's target state, so a caller whose atomicity
    // depends on the image can distinguish a pre-replace failure (nothing
    // committed, safe to roll back) from a post-replace durability failure.
    void CheckpointChunk(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool* out_image_committed = nullptr);

    void AcquireProcessLock(bool allow_multiple_processes);
    void ReleaseProcessLock() noexcept;

    [[nodiscard]] std::string BuildWriterMetadata() const;
    void WriteWriterMetadata();
    void StartWriterHeartbeat();
    void StopWriterHeartbeat() noexcept;
};

}  // namespace chunkdb
