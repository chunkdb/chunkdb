#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "chunkdb/schema.hpp"

namespace chunkdb {

// Canonical one-row chunk-column section, independent of block presence.
struct ChunkColumnState {
    std::vector<std::uint8_t> bytes;
    std::size_t variable_bytes = 0U;
};

// Values are in the chunk schema's order; bytes use stable column-ID order.
[[nodiscard]] ChunkColumnState EncodeChunkColumns(
    const TableSchema& schema, const std::vector<ColumnValue>& values);
[[nodiscard]] std::vector<ColumnValue> DecodeChunkColumns(
    const TableSchema& schema, std::span<const std::uint8_t> bytes);
// Checks the complete row before any value is decoded or published.
[[nodiscard]] std::size_t ValidateChunkColumns(
    const TableSchema& schema, std::span<const std::uint8_t> bytes);
[[nodiscard]] ChunkColumnState DefaultChunkColumns(const TableSchema& schema);
[[nodiscard]] ChunkColumnState AssignChunkColumns(
    const TableSchema& schema, const ChunkColumnState* current,
    const std::vector<ColumnAssignment>& assignments, bool initializing);
[[nodiscard]] ChunkColumnState TranslateChunkColumns(
    const TableSchema& from, const TableSchema& to, const ChunkColumnState& state);

}  // namespace chunkdb
