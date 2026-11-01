#pragma once

#include <cstddef>
#include <memory_resource>
#include <span>

#include "chunkdb/chunk_layout.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/txn_block_row.hpp"

namespace chunkdb {

[[nodiscard]] TxnBlockRow CaptureTxnBlockRow(const ChunkLayout& layout,
    std::span<const std::uint8_t> payload, std::span<const std::uint8_t> presence,
    const ChunkVars& vars, std::size_t block_index, std::pmr::memory_resource* resource);
[[nodiscard]] TxnBlockRow CloneTxnBlockRow(const TxnBlockRow& row, std::pmr::memory_resource* resource);
void ValidateTxnBlockRow(const ChunkLayout& layout, const TxnBlockRow& row);
[[nodiscard]] TxnBlockRow AssignTxnBlockRow(const ChunkLayout& layout, const TxnBlockRow& current,
    std::span<const ColumnAssignment> assignments, std::pmr::memory_resource* resource);
// Compares the current stored row without materializing another row.
[[nodiscard]] bool MatchesTxnBlockRow(const ChunkLayout& layout,
    std::span<const std::uint8_t> payload, std::span<const std::uint8_t> presence,
    const ChunkVars& vars, std::size_t block_index, const TxnBlockRow& expected);

}  // namespace chunkdb
