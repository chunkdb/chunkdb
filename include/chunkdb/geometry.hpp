#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>

#include "chunkdb/types.hpp"

namespace chunkdb {

struct GeometryConfig {
    std::uint32_t large_chunk_width_chunks = 8;
    std::uint32_t large_chunk_height_chunks = 8;
    std::uint32_t chunk_width_blocks = 16;
    std::uint32_t chunk_height_blocks = 16;
    std::uint32_t block_bits = 16;
};

class Geometry {
  public:
    explicit Geometry(GeometryConfig config);

    [[nodiscard]] const GeometryConfig& config() const noexcept { return config_; }
    [[nodiscard]] std::size_t ChunkBlockCount() const noexcept;
    [[nodiscard]] std::size_t ChunkPayloadBits() const noexcept;
    [[nodiscard]] std::size_t ChunkPayloadBytes() const noexcept;

    [[nodiscard]] ChunkCoord BlockToChunk(std::int64_t block_x, std::int64_t block_y) const noexcept;
    [[nodiscard]] std::pair<std::uint32_t, std::uint32_t> BlockToLocal(
        std::int64_t block_x,
        std::int64_t block_y) const noexcept;
    [[nodiscard]] LargeChunkCoord ChunkToLarge(const ChunkCoord& chunk_coord) const noexcept;
    [[nodiscard]] std::size_t LocalBlockIndex(std::uint32_t local_x, std::uint32_t local_y) const;

  private:
    GeometryConfig config_;

    static std::int64_t FloorDiv(std::int64_t value, std::int64_t divisor) noexcept;
    static std::int64_t FloorMod(std::int64_t value, std::int64_t divisor) noexcept;
};

}  // namespace chunkdb
