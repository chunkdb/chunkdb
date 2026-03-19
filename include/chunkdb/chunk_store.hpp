#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
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
};

struct StoreConfig {
    GeometryConfig geometry;
    std::filesystem::path data_dir;

    DurabilityMode durability_mode = DurabilityMode::kRelaxed;
    std::size_t checkpoint_update_interval = 256;
    std::size_t checkpoint_wal_bytes = 1024 * 1024;
    std::size_t wal_group_commit_updates = 1;

    std::size_t max_loaded_chunks = 8192;
    std::size_t max_open_wal_streams = 1024;
    bool allow_multiple_processes = false;
    AccessMode access_mode = AccessMode::kReadWrite;
    StorageLayoutMode storage_layout_mode = StorageLayoutMode::kFsSplitV1;
    std::size_t experimental_region_span_chunks = 16;
};

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

    [[nodiscard]] std::string GetBlockBits(std::int64_t block_x, std::int64_t block_y);
    void SetBlockBits(std::int64_t block_x, std::int64_t block_y, std::string_view bits);

    [[nodiscard]] std::string GetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] std::vector<std::uint8_t> GetChunkPayloadBytes(std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] std::size_t ApproxLoadedChunkCount() const;
    [[nodiscard]] StoreRuntimeStats RuntimeStats() const noexcept;
    [[nodiscard]] std::uint64_t WalOpenCountForTests() const noexcept;
    [[nodiscard]] std::uint64_t WalParentPrepareCountForTests() const noexcept;
    [[nodiscard]] std::uint64_t OpenWalStreamCountForTests() const noexcept;
    [[nodiscard]] std::uint64_t EvictionSnapshotBuildCountForTests() const noexcept;

  private:
    struct RegularChunk {
        explicit RegularChunk(std::vector<std::uint8_t> payload_bytes)
            : payload(std::move(payload_bytes)) {}

        std::vector<std::uint8_t> payload;
        std::size_t pending_updates = 0;
        std::size_t wal_bytes = 0;

        std::size_t pending_wal_flush_updates = 0;
        std::vector<std::uint8_t> wal_batch;
        std::vector<std::uint8_t> scratch_before;
        std::filesystem::path wal_path;
        std::ofstream wal_append_stream;
        bool wal_header_written = false;
        bool wal_stream_initialized = false;

        std::atomic<std::uint64_t> last_access_tick{0};
        mutable RegularChunkMutex mutex;
    };

    struct EvictionCandidate {
        LargeChunkCoord large_coord;
        ChunkCoord chunk_coord;
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
    std::atomic<std::uint64_t> stats_eviction_forced_wal_flushes_{0};
    std::atomic<std::uint64_t> stats_eviction_forced_wal_flushes_with_data_{0};
    std::atomic<std::uint64_t> stats_eviction_forced_wal_flushes_empty_batch_{0};
    std::atomic<std::uint64_t> stats_wal_parent_prepare_calls_{0};
    std::atomic<std::uint64_t> wal_stream_clock_{0};

    struct WalStreamState {
        std::weak_ptr<RegularChunk> chunk;
        std::uint64_t last_used_tick = 0;
    };
    mutable std::mutex wal_open_mutex_;
    mutable std::mutex wal_stream_cache_mutex_;
    std::unordered_map<RegularChunk*, WalStreamState> open_wal_streams_;
    mutable std::mutex wal_parent_cache_mutex_;
    std::unordered_set<std::string> wal_parent_dir_cache_;

    mutable std::mutex large_chunks_mutex_;
    std::unordered_map<LargeChunkCoord, std::shared_ptr<LargeChunk>, LargeChunkCoordHash> large_chunks_;
    mutable std::mutex eviction_state_mutex_;
    std::vector<EvictionCandidate> eviction_candidates_;
    std::size_t eviction_cursor_ = 0;

    [[nodiscard]] std::shared_ptr<LargeChunk> GetOrCreateLargeChunk(const LargeChunkCoord& large_coord);
    [[nodiscard]] std::shared_ptr<RegularChunk> GetOrLoadRegularChunk(const ChunkCoord& chunk_coord);

    [[nodiscard]] std::vector<std::uint8_t> EmptyPayload() const;
    [[nodiscard]] std::vector<std::uint8_t> LoadChunkPayload(const ChunkCoord& chunk_coord);

    void TouchChunk(const std::shared_ptr<RegularChunk>& chunk) noexcept;
    void RegisterEvictionCandidate(const LargeChunkCoord& large_coord, const ChunkCoord& chunk_coord);
    void BuildEvictionSnapshot();
    [[nodiscard]] bool TryEvictCandidate(
        const EvictionCandidate& candidate,
        std::size_t* removed);
    void MaybeEvictChunks();

    void AppendWalDelta(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        std::uint32_t byte_offset,
        const std::uint8_t* payload_bytes,
        std::size_t payload_size,
        std::size_t* appended_record_bytes);

    void FlushWalBatch(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool force_sync);
    void FlushWalBatchForEviction(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool force_sync);
    void EnsureWalAppendStream(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool* first_create);
    void EnsureWalParentDirectoryCached(const std::filesystem::path& wal_parent_path, bool force_refresh);
    void InvalidateWalParentDirectoryCache(const std::filesystem::path& wal_parent_path);
    void CloseWalAppendStream(const std::shared_ptr<RegularChunk>& chunk) noexcept;
    [[nodiscard]] bool TryCloseLeastRecentlyUsedIdleWalStream(
        const std::shared_ptr<RegularChunk>& opening_chunk);
    void EnsureWalStreamCapacity(const std::shared_ptr<RegularChunk>& opening_chunk);
    void TouchWalStreamState(const std::shared_ptr<RegularChunk>& chunk) noexcept;

    void FlushAllPendingWalBatches() noexcept;

    void MaybeCheckpointChunk(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk);
    void CheckpointChunk(const ChunkCoord& chunk_coord, const std::shared_ptr<RegularChunk>& chunk);

    void AcquireProcessLock(bool allow_multiple_processes);
    void ReleaseProcessLock() noexcept;

    [[nodiscard]] std::string BuildWriterMetadata() const;
    void WriteWriterMetadata();
    void StartWriterHeartbeat();
    void StopWriterHeartbeat() noexcept;
};

}  // namespace chunkdb
