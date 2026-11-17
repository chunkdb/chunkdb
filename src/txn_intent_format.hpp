#pragma once

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "txn_history.hpp"

namespace chunkdb {

// CKTI v2 is the only accepted encoding here. These pure codecs do not publish
// intents or repair WALs. Recovery must additionally validate every member's
// canonical path and actual WAL boundary before changing any file.
[[nodiscard]] std::vector<std::uint8_t> SerializeCheckedTxnIntent(
    const TxnIntent& intent, std::uint8_t dims, const StoreId& table_id,
    std::size_t written_chunk_limit);

// Validate the complete record, expected table/dimensions and all coordinates
// before allocating entries. Any malformed member refuses the whole record.
[[nodiscard]] TxnIntent ParseCheckedTxnIntent(
    std::span<const std::uint8_t> bytes, std::uint8_t expected_dims,
    const StoreId& expected_table_id);

// Admit recovery independently of current write limits, without allocating
// owned entries or a complete body before its shape, context and CRC pass.
void ValidateCheckedTxnIntent(std::span<const std::uint8_t> bytes,
    std::uint8_t expected_dims, const StoreId& expected_table_id);
void ValidateCheckedTxnIntentStream(std::istream& input, std::uint64_t measured_bytes,
    std::uint8_t expected_dims, const StoreId& expected_table_id);

[[nodiscard]] std::string CanonicalConditionalIntentFileName(
    const Geometry& geometry, const ChunkCoord& coord);
// Strict coordinate/path grammar and large-chunk placement. On refusal leaves
// out unchanged; no filesystem access and no recovery action.
[[nodiscard]] bool ParseConditionalIntentFileName(
    std::string_view name, const Geometry& geometry, ChunkCoord* out);

}  // namespace chunkdb
