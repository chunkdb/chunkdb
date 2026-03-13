#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "chunkdb/geometry.hpp"
#include "chunkdb/types.hpp"

namespace chunkdb {

enum class DurabilityMode {
    kRelaxed = 0,
    kFsyncWal = 1,
    kFsyncCheckpoint = 2,
};

[[nodiscard]] DurabilityMode ParseDurabilityMode(std::string_view text);
[[nodiscard]] const char* DurabilityModeName(DurabilityMode mode) noexcept;

struct StoreConfig {
    GeometryConfig geometry;
    std::filesystem::path data_dir;

    DurabilityMode durability_mode = DurabilityMode::kRelaxed;
    std::size_t checkpoint_update_interval = 256;
    std::size_t checkpoint_wal_bytes = 1024 * 1024;
    std::size_t wal_group_commit_updates = 1;

    std::size_t max_loaded_chunks = 8192;
    bool allow_multiple_processes = false;
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

    [[nodiscard]] std::string GetBlockBits(std::int64_t block_x, std::int64_t block_y);
    void SetBlockBits(std::int64_t block_x, std::int64_t block_y, std::string_view bits);

    [[nodiscard]] std::string GetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] std::vector<std::uint8_t> GetChunkPayloadBytes(std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] std::size_t ApproxLoadedChunkCount() const;

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

        std::atomic<std::uint64_t> last_access_tick{0};
        mutable std::shared_mutex mutex;
    };

    struct LargeChunk {
        std::mutex mutex;
        std::unordered_map<ChunkCoord, std::shared_ptr<RegularChunk>, ChunkCoordHash> chunks;
    };

    Geometry geometry_;
    std::filesystem::path data_dir_;
    DurabilityMode durability_mode_;
    std::size_t checkpoint_update_interval_;
    std::size_t checkpoint_wal_bytes_;
    std::size_t wal_group_commit_updates_;
    std::size_t max_loaded_chunks_;

#ifdef _WIN32
    void* process_lock_handle_ = nullptr;
#else
    int process_lock_fd_ = -1;
#endif

    std::atomic<std::uint64_t> access_clock_{0};

    mutable std::mutex large_chunks_mutex_;
    std::unordered_map<LargeChunkCoord, std::shared_ptr<LargeChunk>, LargeChunkCoordHash> large_chunks_;

    [[nodiscard]] std::shared_ptr<LargeChunk> GetOrCreateLargeChunk(const LargeChunkCoord& large_coord);
    [[nodiscard]] std::shared_ptr<RegularChunk> GetOrLoadRegularChunk(const ChunkCoord& chunk_coord);

    [[nodiscard]] std::vector<std::uint8_t> EmptyPayload() const;
    [[nodiscard]] std::vector<std::uint8_t> LoadChunkPayload(const ChunkCoord& chunk_coord);

    void TouchChunk(const std::shared_ptr<RegularChunk>& chunk) noexcept;
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

    void FlushAllPendingWalBatches() noexcept;

    void MaybeCheckpointChunk(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk);
    void CheckpointChunk(const ChunkCoord& chunk_coord, const std::shared_ptr<RegularChunk>& chunk);

    void AcquireProcessLock(bool allow_multiple_processes);
    void ReleaseProcessLock() noexcept;
};

}  // namespace chunkdb
