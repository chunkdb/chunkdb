#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "chunkdb/schema.hpp"

namespace chunkdb {

// Where the fixed-width values of a schema live in a chunk's PAYLOAD
// (docs/STORAGE_FORMAT.md Section 3): for each fixed-width column in schema
// order, its values (block_count values of `width` bits, the lowest bit
// first) padded to a byte, then, for a NULL column, its validity bits (one
// per block, 1 = has a value) padded to a byte.
class ChunkLayout {
  public:
    static constexpr std::size_t kNoValidity = std::numeric_limits<std::size_t>::max();

    struct FixedColumn {
        // Index in schema().columns.
        std::size_t column = 0;
        // Bits of one value.
        std::uint32_t width = 0;
        // Byte offset of the values in PAYLOAD.
        std::size_t values = 0;
        // Byte offset of the validity bits in PAYLOAD, or kNoValidity.
        std::size_t validity = kNoValidity;
    };

    // Throws std::invalid_argument when `schema` breaks a rule
    // (ValidateTableSchema) or the payload would exceed `max_payload_bytes`.
    ChunkLayout(TableSchema schema, std::size_t block_count, std::size_t max_payload_bytes);

    [[nodiscard]] const TableSchema& schema() const noexcept { return schema_; }
    [[nodiscard]] std::size_t block_count() const noexcept { return block_count_; }
    [[nodiscard]] std::size_t payload_bytes() const noexcept { return payload_bytes_; }
    [[nodiscard]] const std::vector<FixedColumn>& fixed_columns() const noexcept { return fixed_; }
    // The fixed-width place of schema().columns[index], or nullptr for a
    // variable-length column.
    [[nodiscard]] const FixedColumn* FixedColumnAt(std::size_t index) const noexcept;
    // Index in schema().columns of the column called `name`, or npos.
    [[nodiscard]] std::size_t FindColumn(std::string_view name) const noexcept;
    // True for one bits(N) column that cannot be null: PAYLOAD is one N-bit
    // string per block, block after block, which the bit-string commands
    // read and write.
    [[nodiscard]] bool bit_string_blocks() const noexcept { return bit_string_blocks_; }

    // Zeroes the bits between and after the arrays.
    void MaskPadding(std::vector<std::uint8_t>* payload) const;
    // Zeroes every value and validity bit of a block absent in `presence`,
    // and the value bits of a NULL value.
    void ClearEmptyValues(const std::vector<std::uint8_t>& presence, std::vector<std::uint8_t>* payload) const;

  private:
    TableSchema schema_;
    std::size_t block_count_ = 0;
    std::size_t payload_bytes_ = 0;
    std::vector<FixedColumn> fixed_;
    // Per schema column: index in fixed_, or npos.
    std::vector<std::size_t> fixed_index_;
    struct NameHash {
        using is_transparent = void;
        std::size_t operator()(std::string_view name) const noexcept { return std::hash<std::string_view>{}(name); }
    };
    std::unordered_map<std::string, std::size_t, NameHash, std::equal_to<>> by_name_;
    bool bit_string_blocks_ = false;
};

// Bit copies between a value (as EncodeColumnValue writes it) and an array
// of PAYLOAD; `bit_offset` counts from the start of `payload`.
void WriteValueBits(std::uint8_t* payload, std::size_t bit_offset, const std::uint8_t* value, std::uint32_t width);
// Writes (width + 7) / 8 bytes to `value`, bits past the width zero.
void ReadValueBits(const std::uint8_t* payload, std::size_t bit_offset, std::uint8_t* value, std::uint32_t width);

}  // namespace chunkdb
