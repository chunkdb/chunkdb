#include "chunkdb/geometry.hpp"

#include <limits>
#include <string>
#include <utility>

namespace chunkdb {

namespace {

constexpr std::uint32_t kMaxLargeChunkDimensionChunks = 1'000'000;
constexpr std::uint32_t kMaxChunkDimensionBlocks = 4096;
constexpr std::uint64_t kMaxChunkBlockCount = 1'048'576;
// The manifest stores block_bits as u32, but wider blocks have no test
// coverage, so the limit stays until it is raised deliberately.
constexpr std::uint32_t kMaxBlockBits = 65'535;
constexpr std::uint64_t kMaxChunkPayloadBytes = 64ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kBitsPerByte = 8;

void RequireAtMost(std::uint32_t value, std::uint32_t limit, const char* name) {
    if (value > limit) {
        throw std::invalid_argument(
            std::string(name) + " must be <= " + std::to_string(limit));
    }
}

}  // namespace

Geometry::Geometry(GeometryConfig config) : Geometry(config, SingleBitsColumnSchema(config.block_bits)) {}

Geometry::Geometry(GeometryConfig config, TableSchema schema) : config_(config) {
    if (config_.dims != 2 && config_.dims != 3) {
        throw std::invalid_argument("table dimensions must be 2 or 3");
    }
    if (config_.dims == 2 &&
        (config_.chunk_depth_blocks != 1 || config_.large_chunk_depth_chunks != 1)) {
        throw std::invalid_argument("inactive depth dimensions must be 1");
    }
    if (config_.chunk_depth_blocks == 0 || config_.large_chunk_depth_chunks == 0) {
        throw std::invalid_argument("depth dimensions must be > 0");
    }
    if (config_.large_chunk_width_chunks == 0 || config_.large_chunk_height_chunks == 0) {
        throw std::invalid_argument("large chunk dimensions must be > 0");
    }
    if (config_.chunk_width_blocks == 0 || config_.chunk_height_blocks == 0) {
        throw std::invalid_argument("chunk dimensions must be > 0");
    }
    if (config_.block_bits == 0) {
        throw std::invalid_argument("block_bits must be > 0");
    }
    RequireAtMost(
        config_.large_chunk_width_chunks,
        kMaxLargeChunkDimensionChunks,
        "large_chunk_width_chunks");
    RequireAtMost(
        config_.large_chunk_height_chunks,
        kMaxLargeChunkDimensionChunks,
        "large_chunk_height_chunks");
    RequireAtMost(config_.chunk_width_blocks, kMaxChunkDimensionBlocks, "chunk_width_blocks");
    RequireAtMost(config_.chunk_height_blocks, kMaxChunkDimensionBlocks, "chunk_height_blocks");
    RequireAtMost(config_.block_bits, kMaxBlockBits, "block_bits");
    RequireAtMost(config_.chunk_depth_blocks, kMaxChunkDimensionBlocks, "chunk_depth_blocks");
    RequireAtMost(config_.large_chunk_depth_chunks, kMaxLargeChunkDimensionChunks, "large_chunk_depth_chunks");

    const auto chunk_blocks =
        static_cast<std::uint64_t>(config_.chunk_width_blocks) *
        static_cast<std::uint64_t>(config_.chunk_height_blocks);
    if (chunk_blocks > kMaxChunkBlockCount / config_.chunk_depth_blocks) {
        throw std::invalid_argument(
            "chunk block count must be <= " +
            std::to_string(kMaxChunkBlockCount));
    }
    const auto active_blocks = chunk_blocks * config_.chunk_depth_blocks;

    const auto max_payload_bits = kMaxChunkPayloadBytes * kBitsPerByte;
    if (active_blocks != 0 &&
        static_cast<std::uint64_t>(config_.block_bits) >
            max_payload_bits / active_blocks) {
        throw std::invalid_argument(
            "chunk payload must be <= " + std::to_string(kMaxChunkPayloadBytes) + " bytes");
    }
    if (config_.block_bits != FixedBitsPerBlock(schema)) {
        throw std::invalid_argument(
            "block_bits " + std::to_string(config_.block_bits) + " does not match the columns' " +
            std::to_string(FixedBitsPerBlock(schema)) + " fixed bits per block");
    }
    layout_ = std::make_shared<const ChunkLayout>(
        std::move(schema), static_cast<std::size_t>(active_blocks), kMaxChunkPayloadBytes);
}

const ChunkLayout& Geometry::LayoutAt(std::uint64_t version) const {
    if (version == layout_->schema().version) {
        return *layout_;
    }
    std::lock_guard lock(earlier_->mutex);
    if (const auto found = earlier_->by_version.find(version); found != earlier_->by_version.end()) {
        return *found->second;
    }
    // Built before it is stored, so a version the table lacks stores nothing.
    auto layout = std::make_shared<const ChunkLayout>(
        SchemaAtVersion(layout_->schema(), version), ChunkBlockCount(), layout_->payload_bytes_limit());
    return *earlier_->by_version.emplace(version, std::move(layout)).first->second;
}

std::size_t Geometry::ChunkBlockCount() const noexcept {
    return static_cast<std::size_t>(config_.chunk_width_blocks) *
           static_cast<std::size_t>(config_.chunk_height_blocks) * config_.chunk_depth_blocks;
}

std::size_t Geometry::ChunkPayloadBits() const noexcept {
    return ChunkBlockCount() * static_cast<std::size_t>(config_.block_bits);
}


ChunkCoord Geometry::BlockToChunk(std::int64_t block_x, std::int64_t block_y) const {
    if (config_.dims != 2) throw std::invalid_argument("block coordinates require 3 axes");
    return {
        FloorDiv(block_x, static_cast<std::int64_t>(config_.chunk_width_blocks)),
        FloorDiv(block_y, static_cast<std::int64_t>(config_.chunk_height_blocks)),
    };
}

ChunkCoord Geometry::BlockToChunk(
    std::int64_t block_x, std::int64_t block_y, std::int64_t block_z) const {
    if (config_.dims != 3) throw std::invalid_argument("block coordinates require 2 axes");
    return {FloorDiv(block_x, config_.chunk_width_blocks),
            FloorDiv(block_y, config_.chunk_height_blocks),
            FloorDiv(block_z, config_.chunk_depth_blocks)};
}

std::pair<std::uint32_t, std::uint32_t> Geometry::BlockToLocal(
    std::int64_t block_x,
    std::int64_t block_y) const {
    if (config_.dims != 2) throw std::invalid_argument("block coordinates require 3 axes");
    const auto local_x = FloorMod(block_x, static_cast<std::int64_t>(config_.chunk_width_blocks));
    const auto local_y = FloorMod(block_y, static_cast<std::int64_t>(config_.chunk_height_blocks));
    return {static_cast<std::uint32_t>(local_x), static_cast<std::uint32_t>(local_y)};
}

std::array<std::uint32_t, 3> Geometry::BlockToLocal(
    std::int64_t block_x, std::int64_t block_y, std::int64_t block_z) const {
    if (config_.dims != 3) throw std::invalid_argument("block coordinates require 2 axes");
    return {static_cast<std::uint32_t>(FloorMod(block_x, config_.chunk_width_blocks)),
            static_cast<std::uint32_t>(FloorMod(block_y, config_.chunk_height_blocks)),
            static_cast<std::uint32_t>(FloorMod(block_z, config_.chunk_depth_blocks))};
}

LargeChunkCoord Geometry::ChunkToLarge(const ChunkCoord& chunk_coord) const {
    if (config_.dims == 2 && chunk_coord.z != 0)
        throw std::invalid_argument("inactive chunk z coordinate must be 0");
    return {
        FloorDiv(chunk_coord.x, static_cast<std::int64_t>(config_.large_chunk_width_chunks)),
        FloorDiv(chunk_coord.y, static_cast<std::int64_t>(config_.large_chunk_height_chunks)),
        config_.dims == 2 ? 0 : FloorDiv(chunk_coord.z, config_.large_chunk_depth_chunks),
    };
}

std::size_t Geometry::LocalBlockIndex(std::uint32_t local_x, std::uint32_t local_y) const {
    if (config_.dims != 2) throw std::invalid_argument("local coordinates require 3 axes");
    if (local_x >= config_.chunk_width_blocks || local_y >= config_.chunk_height_blocks) {
        throw std::out_of_range("local block coordinate out of range");
    }
    return static_cast<std::size_t>(local_y) * static_cast<std::size_t>(config_.chunk_width_blocks) +
           static_cast<std::size_t>(local_x);
}

std::size_t Geometry::LocalBlockIndex(
    std::uint32_t local_x, std::uint32_t local_y, std::uint32_t local_z) const {
    if (config_.dims != 3) throw std::invalid_argument("local coordinates require 2 axes");
    if (local_x >= config_.chunk_width_blocks || local_y >= config_.chunk_height_blocks ||
        local_z >= config_.chunk_depth_blocks) {
        throw std::out_of_range("local block coordinate out of range");
    }
    return local_x + static_cast<std::size_t>(config_.chunk_width_blocks) *
        (local_y + static_cast<std::size_t>(config_.chunk_height_blocks) * local_z);
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
