#include "chunkdb/geometry.hpp"

namespace chunkdb {

Geometry::Geometry(GeometryConfig config) : config_(config) {
    if (config_.large_chunk_width_chunks == 0 || config_.large_chunk_height_chunks == 0) {
        throw std::invalid_argument("large chunk dimensions must be > 0");
    }
    if (config_.chunk_width_blocks == 0 || config_.chunk_height_blocks == 0) {
        throw std::invalid_argument("chunk dimensions must be > 0");
    }
    if (config_.block_bits == 0) {
        throw std::invalid_argument("block_bits must be > 0");
    }
}

std::size_t Geometry::ChunkBlockCount() const noexcept {
    return static_cast<std::size_t>(config_.chunk_width_blocks) *
           static_cast<std::size_t>(config_.chunk_height_blocks);
}

std::size_t Geometry::ChunkPayloadBits() const noexcept {
    return ChunkBlockCount() * static_cast<std::size_t>(config_.block_bits);
}

std::size_t Geometry::ChunkPayloadBytes() const noexcept {
    const std::size_t bits = ChunkPayloadBits();
    return (bits + 7U) / 8U;
}

ChunkCoord Geometry::BlockToChunk(std::int64_t block_x, std::int64_t block_y) const noexcept {
    return {
        FloorDiv(block_x, static_cast<std::int64_t>(config_.chunk_width_blocks)),
        FloorDiv(block_y, static_cast<std::int64_t>(config_.chunk_height_blocks)),
    };
}

std::pair<std::uint32_t, std::uint32_t> Geometry::BlockToLocal(
    std::int64_t block_x,
    std::int64_t block_y) const noexcept {
    const auto local_x = FloorMod(block_x, static_cast<std::int64_t>(config_.chunk_width_blocks));
    const auto local_y = FloorMod(block_y, static_cast<std::int64_t>(config_.chunk_height_blocks));
    return {static_cast<std::uint32_t>(local_x), static_cast<std::uint32_t>(local_y)};
}

LargeChunkCoord Geometry::ChunkToLarge(const ChunkCoord& chunk_coord) const noexcept {
    return {
        FloorDiv(chunk_coord.x, static_cast<std::int64_t>(config_.large_chunk_width_chunks)),
        FloorDiv(chunk_coord.y, static_cast<std::int64_t>(config_.large_chunk_height_chunks)),
    };
}

std::size_t Geometry::LocalBlockIndex(std::uint32_t local_x, std::uint32_t local_y) const {
    if (local_x >= config_.chunk_width_blocks || local_y >= config_.chunk_height_blocks) {
        throw std::out_of_range("local block coordinate out of range");
    }
    return static_cast<std::size_t>(local_y) * static_cast<std::size_t>(config_.chunk_width_blocks) +
           static_cast<std::size_t>(local_x);
}

std::int64_t Geometry::FloorDiv(std::int64_t value, std::int64_t divisor) noexcept {
    const std::int64_t quotient = value / divisor;
    const std::int64_t remainder = value % divisor;
    if (remainder != 0 && ((remainder > 0) != (divisor > 0))) {
        return quotient - 1;
    }
    return quotient;
}

std::int64_t Geometry::FloorMod(std::int64_t value, std::int64_t divisor) noexcept {
    const std::int64_t mod = value % divisor;
    if (mod < 0) {
        return mod + divisor;
    }
    return mod;
}

}  // namespace chunkdb
