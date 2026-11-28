#include "block_selection.hpp"

#include <array>
#include <stdexcept>

#include "chunk_store_internal.hpp"
#include "statement_control.hpp"

namespace chunkdb {

namespace {

constexpr std::size_t kBlocksPerCheck = 65536U;

// The chunk and local coordinate of block `point` on each axis.
struct Located {
    ChunkCoord chunk;
    std::array<std::uint32_t, 3> local{};
};

Located Locate(const Geometry& geometry, const ChunkCoord& point) {
    if (geometry.dims() == 3U)
        return {.chunk = geometry.BlockToChunk(point.x, point.y, point.z),
            .local = geometry.BlockToLocal(point.x, point.y, point.z)};
    const auto local = geometry.BlockToLocal(point.x, point.y);
    return {.chunk = geometry.BlockToChunk(point.x, point.y), .local = {local.first, local.second, 0U}};
}

}  // namespace

AreaSelection SelectArea(const Geometry& geometry, bool around, bool blocks, std::uint8_t dims,
    const ChunkCoord& first, const ChunkCoord& last, std::int64_t radius) {
    if (!blocks)
        return {.chunks = {.dims = dims, .first = first, .last = last, .around = around, .radius = radius}};
    if (dims == 2U && (first.z != 0 || last.z != 0))
        throw std::invalid_argument("two-dimensional area has an inactive coordinate");
    if (first.x > last.x || first.y > last.y || first.z > last.z)
        throw std::invalid_argument("block range corners are reversed");
    return {.chunks = {.dims = dims, .first = Locate(geometry, first).chunk, .last = Locate(geometry, last).chunk},
        .blocks = BlockBox{.first = first, .last = last}};
}

std::size_t ClipToBox(const Geometry& geometry, const BlockBox& box, const ChunkCoord& coord,
    std::span<std::uint8_t> presence) {
    const auto& config = geometry.config();
    const std::array<std::uint32_t, 3> sides{config.chunk_width_blocks, config.chunk_height_blocks,
        geometry.dims() == 3U ? config.chunk_depth_blocks : 1U};
    const auto low = Locate(geometry, box.first);
    const auto high = Locate(geometry, box.last);
    const std::array<std::int64_t, 3> chunk{coord.x, coord.y, coord.z};
    const std::array<std::int64_t, 3> low_chunk{low.chunk.x, low.chunk.y, low.chunk.z};
    const std::array<std::int64_t, 3> high_chunk{high.chunk.x, high.chunk.y, high.chunk.z};
    // The box's local range on each axis of this chunk; none outside it.
    std::array<std::uint32_t, 3> from{};
    std::array<std::uint32_t, 3> to{};
    bool inside = true;
    for (std::size_t axis = 0; axis < 3U; ++axis) {
        if (chunk[axis] < low_chunk[axis] || chunk[axis] > high_chunk[axis]) inside = false;
        from[axis] = chunk[axis] == low_chunk[axis] ? low.local[axis] : 0U;
        to[axis] = chunk[axis] == high_chunk[axis] ? high.local[axis] : sides[axis] - 1U;
    }
    const std::size_t count = geometry.layout().block_count();
    std::size_t kept = 0;
    for (std::size_t block = 0; block < count && block / 8U < presence.size(); ++block) {
        if (block != 0U && block % kBlocksPerCheck == 0U) CheckStatementCancellation();
        const auto bit = static_cast<std::uint8_t>(1U << (block % 8U));
        if ((presence[block / 8U] & bit) == 0U) continue;
        const std::array<std::size_t, 3> local{
            block % sides[0], (block / sides[0]) % sides[1], block / (static_cast<std::size_t>(sides[0]) * sides[1])};
        bool keep = inside;
        for (std::size_t axis = 0; keep && axis < 3U; ++axis)
            keep = local[axis] >= from[axis] && local[axis] <= to[axis];
        if (keep) ++kept;
        else presence[block / 8U] = static_cast<std::uint8_t>(presence[block / 8U] & ~bit);
    }
    return kept;
}

void DropAbsentBlocks(const Geometry& geometry, const std::vector<std::uint8_t>& presence,
    std::vector<std::uint8_t>* payload, ChunkVars* vars) {
    CanonicalizeAbsentBlocks(geometry, presence, payload);
    std::vector<VarKey> absent;
    for (const auto value : *vars)
        if (!BlockPresent(presence, value.key.block_index)) absent.push_back(value.key);
    for (const auto& key : absent) (void)vars->Remove(key);
}

}  // namespace chunkdb
