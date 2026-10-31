#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb {

// An empty projection means the full block schema; a nonempty projection
// retains its requested order, even when it names every block column.
// Reads use the hard variable-byte bound so lowering a table's write quota
// cannot make an already stored chunk unreadable.
[[nodiscard]] std::vector<std::uint8_t> EncodeSectionedChunkState(
    const Geometry& geometry, const ChunkState& state, std::size_t max_variable_bytes,
    std::span<const std::size_t> projection = {}, bool private_state = false,
    std::size_t max_form_bytes = std::numeric_limits<std::uint32_t>::max());

// Incoming mutations require the current shared schema revision.
// Their producing identity is discarded; a projected mutation can omit its
// metadata row and any included row is validated but never selects metadata.
[[nodiscard]] ChunkState DecodeSectionedChunkState(
    const Geometry& geometry, std::span<const std::uint8_t> bytes, std::size_t max_variable_bytes,
    std::span<const std::size_t> projection = {},
    std::size_t max_form_bytes = std::numeric_limits<std::uint32_t>::max());

// Presence is replaced; continuing blocks retain unnamed values, new blocks
// initialize them from defaults, and absent blocks lose all their values.
// Stored metadata is preserved, including its ownership and quota cost.
[[nodiscard]] ChunkState MergeProjectedChunkState(
    const Geometry& geometry, const ChunkState& before, const ChunkState& patch,
    std::span<const std::size_t> projection, std::size_t var_max_chunk_bytes);

}  // namespace chunkdb
