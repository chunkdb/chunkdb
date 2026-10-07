#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>

#include "chunkdb/chunk_layout.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/types.hpp"

namespace chunkdb {

struct GeometryConfig {
    std::uint32_t large_chunk_width_chunks = 8;
    std::uint32_t large_chunk_height_chunks = 8;
    std::uint32_t chunk_width_blocks = 16;
    std::uint32_t chunk_height_blocks = 16;
    // The fixed-width bits of a block: FixedBitsPerBlock of the table's
    // schema.
    std::uint32_t block_bits = 16;
};

class Geometry {
  public:
    // A table with one column bits(config.block_bits).
    explicit Geometry(GeometryConfig config);
    // Throws std::invalid_argument unless config.block_bits is
    // FixedBitsPerBlock(schema).
    Geometry(GeometryConfig config, TableSchema schema);

    [[nodiscard]] const GeometryConfig& config() const noexcept { return config_; }
    [[nodiscard]] std::size_t ChunkBlockCount() const noexcept;
    // block_count * block_bits: the length of a chunk's payload as a bit
    // string when layout().bit_string_blocks().
    [[nodiscard]] std::size_t ChunkPayloadBits() const noexcept;
    [[nodiscard]] std::size_t ChunkPayloadBytes() const noexcept { return layout_->payload_bytes(); }
    [[nodiscard]] const ChunkLayout& layout() const noexcept { return *layout_; }

    [[nodiscard]] ChunkCoord BlockToChunk(std::int64_t block_x, std::int64_t block_y) const noexcept;
    [[nodiscard]] std::pair<std::uint32_t, std::uint32_t> BlockToLocal(
        std::int64_t block_x,
        std::int64_t block_y) const noexcept;
    [[nodiscard]] LargeChunkCoord ChunkToLarge(const ChunkCoord& chunk_coord) const noexcept;
    [[nodiscard]] std::size_t LocalBlockIndex(std::uint32_t local_x, std::uint32_t local_y) const;

  private:
    GeometryConfig config_;
    std::shared_ptr<const ChunkLayout> layout_;

    static std::int64_t FloorDiv(std::int64_t value, std::int64_t divisor) noexcept;
    static std::int64_t FloorMod(std::int64_t value, std::int64_t divisor) noexcept;
};

}  // namespace chunkdb
