#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

// The columns of a table (docs/design/COLUMNS_DESIGN.md): what one block holds.
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
// A table has at most this many versions above 1 (docs/design/COLUMNS_DESIGN.md).
inline constexpr std::uint64_t kMaxSchemaVersions = 65535;
// CHECK constraints of a table, and the bytes of one's expression.
inline constexpr std::size_t kMaxChecksPerTable = 64;
inline constexpr std::size_t kMaxCheckExpressionBytes = 16384;

// A CHECK constraint (docs/reference/TABLES.md): a condition over the block
// columns and positions that every present block must not make false. NULL
// (unknown) passes, as in SQL.
struct CheckConstraint {
    // Unique among the table's checks; the column-name rule.
    std::string name;
    // The condition's text (docs/design/EXPRESSIONS_DESIGN.md), without
    // parameters, chunk columns, NOW() or EXISTS.
    std::string expression;

    friend bool operator==(const CheckConstraint&, const CheckConstraint&) = default;
};

// A block of a write or of stored data that makes a CHECK false.
class CheckFailedError : public std::invalid_argument {
  public:
    CheckFailedError(std::string check, const std::string& message)
        : std::invalid_argument(message), check_(std::move(check)) {}
    [[nodiscard]] const std::string& check() const noexcept { return check_; }

  private:
    std::string check_;
};

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
    // DEFAULT COMMIT_TIME, block u64 columns only: has_default with a zero
    // default_value, and every commit that changes the block's other values
    // or creates it, without changing this column, sets it to its commit
    // time in Unix ms.
    bool default_commit_time = false;

    friend bool operator==(const Column&, const Column&) = default;
};

// What a type change does with a value that does not fit the new type.
enum class Conversion : std::uint8_t {
    // Every value fits: the new type widens the old one.
    kExact = 0,
    // Numbers: the nearest value the new type holds.
    kClamp = 1,
    // The column's DEFAULT, else NULL, else zero or empty.
    kDefault = 2,
    // Text, bytes and bits: the first bytes or bits that fit (text at a
    // character boundary).
    kTruncate = 3,
};

// How a schema version differs from the one before it.
struct SchemaChange {
    enum class Kind : std::uint8_t {
        kAddColumn = 1,
        kDropColumn = 2,
        kRenameColumn = 3,
        kChangeType = 4,
        // The shared schema revision changed only the other column domain.
        kOtherDomain = 5,
        // A block column's NULL, REQUIRED or DEFAULT changed; its id, name
        // and type did not.
        kChangeAttributes = 6,
        // A CHECK constraint was added at `position` of the check list, or
        // dropped from it; block columns only.
        kAddCheck = 7,
        kDropCheck = 8,
    };
    Kind kind = Kind::kAddColumn;
    // Index in the column list where the column was added, dropped, or is.
    std::uint32_t position = 0;
    // The added column, the dropped column, or the column after its rename.
    Column column{};
    // kRenameColumn: the name before.
    std::string old_name{};
    // kChangeType and kChangeAttributes: the column before; kChangeType: how
    // its values were converted.
    Column previous{};
    Conversion conversion = Conversion::kExact;
    // kAddCheck and kDropCheck: the check added or dropped.
    CheckConstraint check{};

    friend bool operator==(const SchemaChange&, const SchemaChange&) = default;
};

// The changes that made `version` from version - 1.
struct SchemaStep {
    std::uint64_t version = 0;
    std::vector<SchemaChange> changes{};

    friend bool operator==(const SchemaStep&, const SchemaStep&) = default;
};

// A narrowing in progress (TableCatalog::NarrowColumn): while it lasts,
// every write to the column must fit `type` too. With `not_null` it is a
// DROP NULL check instead (TableCatalog::DropColumnNull): `type` is the
// column's own, and no write may leave the column NULL. With `check` it is
// an ADD CHECK scan (TableCatalog::AddCheck): `column_id` is 0, and every
// write must also pass that check.
struct PendingNarrowing {
    std::uint32_t column_id = 0;
    ColumnType type{};
    bool not_null = false;
    std::optional<CheckConstraint> check{};

    friend bool operator==(const PendingNarrowing&, const PendingNarrowing&) = default;
};

struct TableSchema {
    std::uint64_t version = 1;
    // The id the next added column gets; above every id in use or used.
    std::uint32_t next_column_id = 1;
    std::vector<Column> columns;
    // One step per version above 1, ascending: the schema of any earlier
    // version is this one with later steps undone (SchemaAtVersion).
    std::vector<SchemaStep> history{};
    // Of the current version only.
    std::optional<PendingNarrowing> pending{};
    // The CHECK constraints of this version; block columns only.
    std::vector<CheckConstraint> checks{};

    friend bool operator==(const TableSchema&, const TableSchema&) = default;
};

enum class ColumnDomain { kBlock, kChunk };

// Well-formed UTF-8: no overlong forms, no surrogates, at most U+10FFFF.
[[nodiscard]] bool IsUtf8(std::span<const std::uint8_t> bytes) noexcept;
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
// flag combinations, defaults, the total of fixed bits, a version above 0,
// and a history of exactly version - 1 steps that undo to valid schemas.
void ValidateTableSchema(const TableSchema& schema);
// Chunk columns may be empty and may contain only variable-width columns.
// History and value validation remain the same as for block columns.
void ValidateChunkSchema(const TableSchema& schema);
// Both histories share every revision; exactly one changes after creation.
void ValidateSchemaDomains(const TableSchema& blocks, const TableSchema& chunks);
// An unused chunk domain accompanying a block-only schema history.
[[nodiscard]] TableSchema EmptyChunkSchemaFor(const TableSchema& blocks);
// Throws std::invalid_argument, as ValidateTableSchema does, when the size of
// `column`'s type is outside the range its kind takes (uN 1 to 64, ...).
void ValidateColumnType(const Column& column);
// Whether this build can store a table with `schema` (it needs at least one
// fixed-width column); empty when it can, otherwise the reason.
[[nodiscard]] std::string UnsupportedSchemaReason(const TableSchema& schema);

// The schema `schema` had at `version` (1 to schema.version), with the
// history up to it. Throws std::invalid_argument for another version.
[[nodiscard]] TableSchema SchemaAtVersion(const TableSchema& schema, std::uint64_t version);
// The next version of `schema` with one change. Throw std::invalid_argument
// when the result breaks a rule: an unknown or taken name, a REQUIRED column
// added without a DEFAULT (existing blocks could not have it), or dropping
// the last fixed-width column.
// With allow_required_without_default, the caller must check table emptiness
// under exclusive admission before publishing the schema.
[[nodiscard]] TableSchema AddColumn(const TableSchema& schema, Column column, bool allow_required_without_default = false);
[[nodiscard]] TableSchema DropColumn(const TableSchema& schema, std::string_view name);
[[nodiscard]] TableSchema RenameColumn(const TableSchema& schema, std::string_view name, std::string new_name);
[[nodiscard]] TableSchema AddChunkColumn(
    const TableSchema& schema, Column column, bool allow_required_without_default = false);
[[nodiscard]] TableSchema DropChunkColumn(const TableSchema& schema, std::string_view name);
[[nodiscard]] TableSchema RenameChunkColumn(const TableSchema& schema, std::string_view name, std::string new_name);
[[nodiscard]] TableSchema AdvanceOtherDomain(const TableSchema& schema, ColumnDomain domain);
// Changes a column's type within its family (integers uN and iN, floats,
// text, bytes, bits; docs/design/COLUMNS_DESIGN.md). kExact needs a type that holds
// every value of the old one (uN to uM or iM with more bits, iN to iM with
// at least as many, f32 to f64, a larger max or N); kClamp takes numbers,
// kTruncate text, bytes and bits. The column's DEFAULT is converted the same
// way. Throws std::invalid_argument for anything else.
[[nodiscard]] TableSchema ChangeColumnType(
    const TableSchema& schema,
    std::string_view name,
    ColumnType type,
    Conversion conversion);
// Whether `to` is of the family of `from` and holds every value of it, so
// ChangeColumnType with kExact takes it.
[[nodiscard]] bool HoldsEveryValue(const ColumnType& from, const ColumnType& to) noexcept;

// The steps of a narrowing check (TableCatalog::NarrowColumn).
// WithPendingNarrowing starts one: column `name` to `type` of its family
// that does not hold every value of its type (else ChangeColumnType with
// kExact needs no check); its DEFAULT must fit. NarrowColumnType ends one
// whose check found every stored value fitting: the next version, recorded
// as an exact conversion. WithoutPendingNarrowing ends one that failed or
// was interrupted. All throw std::invalid_argument for a change the rules
// refuse.
[[nodiscard]] TableSchema WithPendingNarrowing(const TableSchema& schema, std::string_view name, ColumnType type);
[[nodiscard]] TableSchema NarrowColumnType(const TableSchema& schema, std::string_view name, ColumnType type);
[[nodiscard]] TableSchema WithoutPendingNarrowing(const TableSchema& schema);

// The next version with block column `name` taking the NULL, REQUIRED and
// DEFAULT of `attributes` (its id, name and type are ignored): ALTER COLUMN
// SET or DROP DEFAULT, SET NULL, SET or DROP REQUIRED. Throws
// std::invalid_argument when nothing changes, for a column both NULL and
// REQUIRED, a DEFAULT the type does not hold, and for removing NULL, which
// needs the check below.
[[nodiscard]] TableSchema ChangeColumnAttributes(const TableSchema& schema, std::string_view name, const Column& attributes);
// The steps of a DROP NULL check (TableCatalog::DropColumnNull), as for a
// narrowing: WithPendingNotNull starts one on a NULL column, DropColumnNull
// ends one whose check found no NULL with the next version, where the
// column refuses NULL. WithoutPendingNarrowing ends one that failed.
[[nodiscard]] TableSchema WithPendingNotNull(const TableSchema& schema, std::string_view name);
[[nodiscard]] TableSchema DropColumnNull(const TableSchema& schema, std::string_view name);

// CHECK constraints. ValidateCheck throws std::invalid_argument for a name
// that breaks the column-name rule, a text over kMaxCheckExpressionBytes, a
// condition that does not parse (the message of cql::ParseError), uses
// parameters, chunk columns, NOW() or EXISTS, or does not plan against
// `columns` (expr::PlanCondition).
void ValidateCheck(const CheckConstraint& check, const std::vector<Column>& columns);
// The block columns `check` reads, each once, in the order first read.
[[nodiscard]] std::vector<std::string> CheckColumnNames(const CheckConstraint& check);
// A name for an unnamed check: `<column>_check` for `column`, else for the
// first column `expression` reads (else `check`), with `_2`, `_3`, ... when
// another check of `schema` has it.
[[nodiscard]] std::string NewCheckName(
    const TableSchema& schema, const std::string& expression, std::string_view column = {});
// The steps of an ADD CHECK scan (TableCatalog::AddCheck), as for a
// narrowing: WithPendingCheck starts one, AddPendingCheck ends one whose
// scan found no block failing it with the next version, which has the
// check. WithoutPendingNarrowing ends one that failed.
[[nodiscard]] TableSchema WithPendingCheck(const TableSchema& schema, CheckConstraint check);
[[nodiscard]] TableSchema AddPendingCheck(const TableSchema& schema);
// The next version with check `name` dropped. Throws std::invalid_argument
// when the table has no check of that name.
[[nodiscard]] TableSchema DropCheck(const TableSchema& schema, std::string_view name);
[[nodiscard]] bool HasCheck(const TableSchema& schema, std::string_view name) noexcept;

// The schema area of the table manifest (docs/STORAGE_FORMAT.md Section 1.2).
[[nodiscard]] std::vector<std::uint8_t> EncodeTableSchema(const TableSchema& schema);
// Throws std::runtime_error for malformed bytes and std::invalid_argument
// (through ValidateTableSchema) for a schema that breaks a rule.
[[nodiscard]] TableSchema DecodeTableSchema(const std::uint8_t* data, std::size_t size);
[[nodiscard]] std::vector<std::uint8_t> EncodeChunkSchema(const TableSchema& schema);
[[nodiscard]] TableSchema DecodeChunkSchema(const std::uint8_t* data, std::size_t size);

// A bits(N) value: N characters '0' or '1', the first one the lowest bit.
struct BitsValue {
    std::string digits;

    friend bool operator==(const BitsValue&, const BitsValue&) = default;
};

// A bytes(max) value.
struct BytesValue {
    std::vector<std::uint8_t> bytes;

    friend bool operator==(const BytesValue&, const BytesValue&) = default;
};

// The value of one column of one block. std::monostate is NULL; uN takes
// std::uint64_t, iN std::int64_t, bool bool, f32 float, f64 double,
// bits(N) BitsValue, text(max) std::string (UTF-8) and bytes(max)
// BytesValue.
using ColumnValue = std::variant<
    std::monostate,
    std::uint64_t,
    std::int64_t,
    bool,
    float,
    double,
    BitsValue,
    std::string,
    BytesValue>;

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
// The bytes of a text or bytes value. Throws std::invalid_argument naming
// the column when `value` is NULL, of another type, longer than the column
// allows, or (text) not UTF-8.
[[nodiscard]] std::vector<std::uint8_t> EncodeVarValue(const Column& column, const ColumnValue& value);
// The text or bytes value `bytes` hold.
[[nodiscard]] ColumnValue DecodeVarValue(const Column& column, std::span<const std::uint8_t> bytes);

// `value` of column `from` as a value of `to` (the same column after a type
// change) by `conversion`. NULL stays NULL. Throws std::logic_error when
// kExact meets a value that does not fit.
[[nodiscard]] ColumnValue ConvertValue(
    const Column& from,
    const Column& to,
    Conversion conversion,
    const ColumnValue& value);

// Whether `value` is one `type` holds (NULL always is).
[[nodiscard]] bool ValueFits(const ColumnType& type, const ColumnValue& value);
// A user-facing description of the numeric range or maximum value length.
[[nodiscard]] std::string ColumnTypeRange(const ColumnType& type);

}  // namespace chunkdb
