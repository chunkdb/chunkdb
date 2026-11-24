#pragma once

// The selections of area statements (docs/DESIGN-2.0.md E4): AREA and AREA
// AROUND name chunks; BLOCKS names a box of block coordinates, which covers
// the chunks that hold any of its blocks and keeps only its blocks.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "chunkdb/area_selector.hpp"
#include "chunkdb/chunk_vars.hpp"
#include "chunkdb/geometry.hpp"
#include "chunkdb/types.hpp"

namespace chunkdb {

// Block coordinates, inclusive on every active axis; z is 0 in 2D.
struct BlockBox {
    ChunkCoord first{};
    ChunkCoord last{};
};

struct AreaSelection {
    ChunkAreaSelector chunks{};
    // BLOCKS: only these blocks of the chunks.
    std::optional<BlockBox> blocks{};
};

// AREA (`first` to `last`), AREA AROUND (`first` and `radius`) in chunk
// coordinates, or with `blocks` BLOCKS (`first` to `last`) in block
// coordinates. Throws std::invalid_argument for reversed corners, a negative
// radius or a z coordinate of a 2D selection.
[[nodiscard]] AreaSelection SelectArea(const Geometry& geometry, bool around, bool blocks, std::uint8_t dims,
    const ChunkCoord& first, const ChunkCoord& last, std::int64_t radius);

// Clears in `presence` the bit of every block of chunk `coord` outside
// `box`, and returns the present blocks that stay.
std::size_t ClipToBox(const Geometry& geometry, const BlockBox& box, const ChunkCoord& coord,
    std::span<std::uint8_t> presence);

// Drops what the blocks absent in `presence` hold: their payload bits and
// their text and bytes values, as a chunk without them stores it.
void DropAbsentBlocks(const Geometry& geometry, const std::vector<std::uint8_t>& presence,
    std::vector<std::uint8_t>* payload, ChunkVars* vars);

}  // namespace chunkdb
