#pragma once

#include <cstddef>
#include <span>

#include "chunkdb/chunk_store.hpp"
#include "sectioned_chunk_form.hpp"

// Downsampled area reads (GET AREA ... SCALE k USING ...): each group of k
// blocks on every active axis of a chunk becomes one cell.
namespace chunkdb {

// The geometry of one downsampled chunk. Throws std::invalid_argument unless
// the factor divides every active chunk side and each projected column (every
// block column for an empty projection) accepts the reduction.
[[nodiscard]] Geometry ScaledGeometry(
    const Geometry& geometry, const ChunkFormScale& scale, std::span<const std::size_t> projection);

// One cell per group of `state`, laid out by `scaled`: a cell is present when
// a block of its group is present, and each projected column takes the value
// of the block the reduction selects. Unprojected columns stay empty; the
// producing identity and chunk columns are kept.
[[nodiscard]] ChunkState ScaleChunkState(const Geometry& geometry, const Geometry& scaled,
    const ChunkFormScale& scale, std::span<const std::size_t> projection, const ChunkState& state);

}  // namespace chunkdb
