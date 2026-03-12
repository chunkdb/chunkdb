#pragma once

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

struct StoreConfig {
    GeometryConfig geometry;
    std::filesystem::path data_dir;
};

class ChunkStore {
  public:
    explicit ChunkStore(StoreConfig config);

    [[nodiscard]] const Geometry& geometry() const noexcept { return geometry_; }
    [[nodiscard]] const std::filesystem::path& data_dir() const noexcept { return data_dir_; }

    [[nodiscard]] std::string GetBlockBits(std::int64_t block_x, std::int64_t block_y);
    void SetBlockBits(std::int64_t block_x, std::int64_t block_y, std::string_view bits);
    [[nodiscard]] std::string GetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y);

  private:
    struct RegularChunk {
        explicit RegularChunk(std::vector<std::uint8_t> payload_bytes)
            : payload(std::move(payload_bytes)) {}

        std::vector<std::uint8_t> payload;
        mutable std::shared_mutex mutex;
    };

    struct LargeChunk {
        std::mutex mutex;
        std::unordered_map<ChunkCoord, std::shared_ptr<RegularChunk>, ChunkCoordHash> chunks;
    };

    Geometry geometry_;
    std::filesystem::path data_dir_;

    std::mutex large_chunks_mutex_;
    std::unordered_map<LargeChunkCoord, std::shared_ptr<LargeChunk>, LargeChunkCoordHash> large_chunks_;

    [[nodiscard]] std::shared_ptr<LargeChunk> GetOrCreateLargeChunk(const LargeChunkCoord& large_coord);
    [[nodiscard]] std::shared_ptr<RegularChunk> GetOrLoadRegularChunk(const ChunkCoord& chunk_coord);

    [[nodiscard]] std::vector<std::uint8_t> EmptyPayload() const;
    [[nodiscard]] std::vector<std::uint8_t> LoadChunkPayload(const ChunkCoord& chunk_coord);
    void PersistChunkPayload(const ChunkCoord& chunk_coord, const std::vector<std::uint8_t>& payload);
};

}  // namespace chunkdb
