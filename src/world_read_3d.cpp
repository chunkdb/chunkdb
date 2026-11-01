#include "chunkdb/chunk_store.hpp"

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace chunkdb {
namespace {

void RequireThreeDimensions(const Geometry& geometry) {
    if (geometry.dims() != 3U) throw std::invalid_argument("chunk coordinates require 2 axes");
}

// The caller bounds radius below the area count limit before squaring it.
std::int64_t IntegerRoot(std::uint64_t square, std::int64_t radius) noexcept {
    std::uint64_t low = 0U;
    auto high = static_cast<std::uint64_t>(radius);
    while (low < high) {
        const auto middle = low + (high - low + 1U) / 2U;
        if (middle <= square / middle) low = middle;
        else high = middle - 1U;
    }
    return static_cast<std::int64_t>(low);
}

bool AddOffset(std::int64_t center, std::int64_t offset, std::int64_t* result) noexcept {
    if ((offset < 0 && center < std::numeric_limits<std::int64_t>::min() - offset) ||
        (offset > 0 && center > std::numeric_limits<std::int64_t>::max() - offset)) return false;
    *result = center + offset;
    return true;
}

template <typename Visitor>
void VisitBallRows(std::int64_t radius, Visitor visitor) {
    const auto square = static_cast<std::uint64_t>(radius) * static_cast<std::uint64_t>(radius);
    for (std::int64_t dx = -radius; dx <= radius; ++dx) {
        const auto abs_x = static_cast<std::uint64_t>(dx < 0 ? -dx : dx);
        const auto yz_square = square - abs_x * abs_x;
        const auto half_y = IntegerRoot(yz_square, radius);
        for (std::int64_t dy = -half_y; dy <= half_y; ++dy) {
            const auto abs_y = static_cast<std::uint64_t>(dy < 0 ? -dy : dy);
            visitor(dx, dy, IntegerRoot(yz_square - abs_y * abs_y, radius));
        }
    }
}

}  // namespace

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRange3D(
    std::int64_t x0, std::int64_t y0, std::int64_t z0,
    std::int64_t x1, std::int64_t y1, std::int64_t z1, bool with_vars) {
    return ReadChunkRange3DImpl(nullptr, nullptr, x0, y0, z0, x1, y1, z1, with_vars);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRange3DAt(
    const TxnSnapshot& snapshot,
    std::int64_t x0, std::int64_t y0, std::int64_t z0,
    std::int64_t x1, std::int64_t y1, std::int64_t z1,
    bool with_vars, const TxnAreaRead& txn_read) {
    RequireOwnTxnSnapshot(snapshot);
    return ReadChunkRange3DImpl(&snapshot, &txn_read, x0, y0, z0, x1, y1, z1, with_vars);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRange3DImpl(
    const TxnSnapshot* snapshot, const TxnAreaRead* txn_read,
    std::int64_t x0, std::int64_t y0, std::int64_t z0,
    std::int64_t x1, std::int64_t y1, std::int64_t z1, bool with_vars) {
    RequireThreeDimensions(geometry_);
    if (x0 > x1 || y0 > y1 || z0 > z1)
        throw std::invalid_argument("chunk range corners must satisfy x0<=x1, y0<=y1 and z0<=z1");
    const std::uint64_t spans[]{static_cast<std::uint64_t>(x1) - static_cast<std::uint64_t>(x0),
        static_cast<std::uint64_t>(y1) - static_cast<std::uint64_t>(y0),
        static_cast<std::uint64_t>(z1) - static_cast<std::uint64_t>(z0)};
    std::uint64_t count = 1U;
    for (const auto span : spans) {
        if (span >= kMaxChunkRangeChunks || count > kMaxChunkRangeChunks / (span + 1U))
            throw std::invalid_argument("chunk range must cover at most " +
                std::to_string(kMaxChunkRangeChunks) + " chunks");
        count *= span + 1U;
    }
    const auto max_entries = kMaxChunkRangeResponseBytes / ChunkRangeEntryCostBytes();
    std::vector<ChunkRangeEntry> entries;
    std::size_t vars_bytes = 0U;
    for (auto x = x0;; ++x) {
        for (auto y = y0;; ++y) {
            for (auto z = z0;; ++z) {
                AppendPopulatedChunkRangeEntry({x, y, z}, max_entries, "GET AREA", with_vars,
                    &vars_bytes, &entries, snapshot, txn_read);
                if (z == z1) break;
            }
            if (y == y1) break;
        }
        if (x == x1) break;
    }
    return entries;
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRadius3D(
    std::int64_t cx, std::int64_t cy, std::int64_t cz, std::int64_t radius_chunks, bool with_vars) {
    return ReadChunkRadius3DImpl(nullptr, nullptr, cx, cy, cz, radius_chunks, with_vars);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRadius3DAt(
    const TxnSnapshot& snapshot, std::int64_t cx, std::int64_t cy, std::int64_t cz,
    std::int64_t radius_chunks, bool with_vars, const TxnAreaRead& txn_read) {
    RequireOwnTxnSnapshot(snapshot);
    return ReadChunkRadius3DImpl(&snapshot, &txn_read, cx, cy, cz, radius_chunks, with_vars);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRadius3DImpl(
    const TxnSnapshot* snapshot, const TxnAreaRead* txn_read,
    std::int64_t cx, std::int64_t cy, std::int64_t cz, std::int64_t radius_chunks, bool with_vars) {
    RequireThreeDimensions(geometry_);
    if (radius_chunks < 0) throw std::invalid_argument("chunk radius must be >= 0");
    if (static_cast<std::uint64_t>(radius_chunks) >= kMaxChunkRangeChunks)
        throw std::invalid_argument("chunk radius exceeds the covered-chunk limit");
    std::uint64_t count = 0U;
    VisitBallRows(radius_chunks, [&](auto, auto, std::int64_t half_z) {
        const auto row = 2U * static_cast<std::uint64_t>(half_z) + 1U;
        if (row > kMaxChunkRangeChunks - count)
            throw std::invalid_argument("chunk radius must cover at most " +
                std::to_string(kMaxChunkRangeChunks) + " chunks");
        count += row;
    });
    const auto max_entries = kMaxChunkRangeResponseBytes / ChunkRangeEntryCostBytes();
    std::vector<ChunkRangeEntry> entries;
    std::size_t vars_bytes = 0U;
    VisitBallRows(radius_chunks, [&](std::int64_t dx, std::int64_t dy, std::int64_t half_z) {
        std::int64_t x = 0, y = 0;
        if (!AddOffset(cx, dx, &x) || !AddOffset(cy, dy, &y)) return;
        for (auto dz = -half_z; dz <= half_z; ++dz) {
            std::int64_t z = 0;
            if (!AddOffset(cz, dz, &z)) continue;
            AppendPopulatedChunkRangeEntry({x, y, z}, max_entries, "GET AREA AROUND", with_vars,
                &vars_bytes, &entries, snapshot, txn_read);
        }
    });
    return entries;
}

}  // namespace chunkdb
