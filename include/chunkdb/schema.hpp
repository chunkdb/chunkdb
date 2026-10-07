#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

// The columns of a table (docs/COLUMNS_DESIGN.md): what one block holds.
namespace chunkdb {

enum class ColumnKind : std::uint8_t {
    kUnsigned = 1,  // uN, N in 1..64
    kSigned = 2,    // iN, N in 2..64, two's complement
    kBool = 3,      // 1 bit
    kFloat32 = 4,   // IEEE 754 binary32
    kFloat64 = 5,   // IEEE 754 binary64
    kBits = 6,      // bits(N), N in 1..65535: an opaque bit string
    kText = 7,      // text(max): UTF-8, at most max bytes
    kBytes = 8,     // bytes(max): opaque, at most max bytes
};

struct ColumnType {
    ColumnKind kind = ColumnKind::kBits;
    // Bits of a fixed-width value (uN, iN, bits(N); 1 for bool, 32 and 64
    // for floats), or the most bytes of a text or bytes value.
    std::uint32_t size = 0;

    friend bool operator==(const ColumnType&, const ColumnType&) = default;
};

inline constexpr std::uint32_t kMaxColumnsPerTable = 1024;
inline constexpr std::uint32_t kMaxColumnNameBytes = 63;
inline constexpr std::uint32_t kMaxFixedBitsPerBlock = 65535;
inline constexpr std::uint32_t kMaxVariableValueBytes = 16U * 1024U * 1024U;

struct Column {
    // Never reused within a table.
    std::uint32_t id = 0;
    std::string name;
    ColumnType type;
    // May hold no value.
    bool nullable = false;
    // A new block must give a value.
    bool required = false;
    // The value a column takes when it is not given, encoded as
    // EncodeColumnValue does; only meaningful with has_default.
    bool has_default = false;
    std::vector<std::uint8_t> default_value;

    friend bool operator==(const Column&, const Column&) = default;
};

struct TableSchema {
    std::uint64_t version = 1;
    // The id the next added column gets; above every id in use or used.
    std::uint32_t next_column_id = 1;
    std::vector<Column> columns;

    friend bool operator==(const TableSchema&, const TableSchema&) = default;
};

[[nodiscard]] bool IsFixedWidth(ColumnKind kind) noexcept;
// Bits one value of a fixed-width column takes.
[[nodiscard]] std::uint32_t FixedWidthBits(const ColumnType& type) noexcept;
// "u10", "bits(16)", "text(256)".
[[nodiscard]] std::string ColumnTypeName(const ColumnType& type);
// The schema of a table created with a block width only: one column `bits`
// of type bits(block_bits). Its chunks hold exactly the bytes a block of
// `block_bits` bits always had.
[[nodiscard]] TableSchema SingleBitsColumnSchema(std::uint32_t block_bits);
// Bits of every fixed-width value of a block together.
[[nodiscard]] std::uint32_t FixedBitsPerBlock(const TableSchema& schema) noexcept;
// Throws std::invalid_argument naming the first rule `schema` breaks: column
// count, unique ids below next_column_id, unique valid names, type sizes,
// flag combinations, defaults, the total of fixed bits, a version above 0.
void ValidateTableSchema(const TableSchema& schema);
// Whether this build can store a table with `schema` (fixed-width columns
// only until text and bytes columns land); empty when it can, otherwise the
// reason.
[[nodiscard]] std::string UnsupportedSchemaReason(const TableSchema& schema);

// The schema area of the table manifest (docs/STORAGE_FORMAT.md Section 1.2).
[[nodiscard]] std::vector<std::uint8_t> EncodeTableSchema(const TableSchema& schema);
// Throws std::runtime_error for malformed bytes and std::invalid_argument
// (through ValidateTableSchema) for a schema that breaks a rule.
[[nodiscard]] TableSchema DecodeTableSchema(const std::uint8_t* data, std::size_t size);

// A bits(N) value: N characters '0' or '1', the first one the lowest bit.
struct BitsValue {
    std::string digits;

    friend bool operator==(const BitsValue&, const BitsValue&) = default;
};

// The value of one column of one block. std::monostate is NULL; uN takes
// std::uint64_t, iN std::int64_t, bool bool, f32 float, f64 double and
// bits(N) BitsValue.
using ColumnValue =
    std::variant<std::monostate, std::uint64_t, std::int64_t, bool, float, double, BitsValue>;

struct ColumnAssignment {
    std::string column;
    ColumnValue value;
};

// A fixed-width value as bytes: (width + 7) / 8 of them, the lowest bit
// first, bits past the width zero. Throws std::invalid_argument naming the
// column when `value` is NULL, of another type, or out of the type's range.
[[nodiscard]] std::vector<std::uint8_t> EncodeColumnValue(const Column& column, const ColumnValue& value);
// The same into `out`, which has room for (width + 7) / 8 bytes; `out` is
// unspecified when it throws.
void EncodeColumnValue(const Column& column, const ColumnValue& value, std::uint8_t* out);
// The value `bytes` (as EncodeColumnValue writes them) hold.
[[nodiscard]] ColumnValue DecodeColumnValue(const Column& column, const std::uint8_t* bytes);

}  // namespace chunkdb
