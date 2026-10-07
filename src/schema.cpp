#include "chunkdb/schema.hpp"

#include <algorithm>
#include <bit>
#include <limits>
#include <stdexcept>
#include <unordered_set>

#include "chunk_store_internal.hpp"

namespace chunkdb {

namespace {

constexpr std::uint8_t kFlagNullable = 0x1U;
constexpr std::uint8_t kFlagRequired = 0x2U;
constexpr std::uint8_t kFlagHasDefault = 0x4U;
constexpr std::uint8_t kKnownFlags = kFlagNullable | kFlagRequired | kFlagHasDefault;

[[nodiscard]] bool IsKnownKind(std::uint8_t kind) noexcept {
    return kind >= static_cast<std::uint8_t>(ColumnKind::kUnsigned) &&
           kind <= static_cast<std::uint8_t>(ColumnKind::kBytes);
}

[[nodiscard]] bool IsValidName(std::string_view name) noexcept {
    if (name.empty() || name.size() > kMaxColumnNameBytes) {
        return false;
    }
    const auto lower_or_underscore = [](char c) { return (c >= 'a' && c <= 'z') || c == '_'; };
    if (!lower_or_underscore(name.front())) {
        return false;
    }
    for (const char c : name) {
        if (!lower_or_underscore(c) && !(c >= '0' && c <= '9')) {
            return false;
        }
    }
    return true;
}


void ValidateType(const Column& column) {
    const auto& type = column.type;
    const auto bad = [&](const std::string& rule) {
        throw std::invalid_argument("column " + column.name + ": " + rule);
    };
    switch (type.kind) {
        case ColumnKind::kUnsigned:
            if (type.size < 1U || type.size > 64U) {
                bad("uN takes N from 1 to 64");
            }
            return;
        case ColumnKind::kSigned:
            if (type.size < 2U || type.size > 64U) {
                bad("iN takes N from 2 to 64");
            }
            return;
        case ColumnKind::kBool:
            if (type.size != 1U) {
                bad("bool is 1 bit");
            }
            return;
        case ColumnKind::kFloat32:
            if (type.size != 32U) {
                bad("f32 is 32 bits");
            }
            return;
        case ColumnKind::kFloat64:
            if (type.size != 64U) {
                bad("f64 is 64 bits");
            }
            return;
        case ColumnKind::kBits:
            if (type.size < 1U || type.size > kMaxFixedBitsPerBlock) {
                bad("bits(N) takes N from 1 to " + std::to_string(kMaxFixedBitsPerBlock));
            }
            return;
        case ColumnKind::kText:
        case ColumnKind::kBytes:
            if (type.size < 1U || type.size > kMaxVariableValueBytes) {
                bad(std::string(type.kind == ColumnKind::kText ? "text(max)" : "bytes(max)") +
                    " takes max from 1 to " + std::to_string(kMaxVariableValueBytes));
            }
            return;
    }
    bad("unknown type");
}

void ValidateDefault(const Column& column) {
    if (!column.has_default) {
        if (!column.default_value.empty()) {
            throw std::invalid_argument("column " + column.name + ": a default value without DEFAULT");
        }
        return;
    }
    const auto& value = column.default_value;
    if (IsFixedWidth(column.type.kind)) {
        const std::uint32_t bits = FixedWidthBits(column.type);
        if (value.size() != (bits + 7U) / 8U) {
            throw std::invalid_argument(
                "column " + column.name + ": a default of " + ColumnTypeName(column.type) + " takes " +
                std::to_string((bits + 7U) / 8U) + " bytes");
        }
        if (bits % 8U != 0U && (value.back() >> (bits % 8U)) != 0U) {
            throw std::invalid_argument("column " + column.name + ": the default has bits past the type's width");
        }
        return;
    }
    if (value.size() > column.type.size) {
        throw std::invalid_argument(
            "column " + column.name + ": the default is longer than " + ColumnTypeName(column.type));
    }
    if (column.type.kind == ColumnKind::kText && !IsUtf8(value)) {
        throw std::invalid_argument("column " + column.name + ": the default is not UTF-8");
    }
}

class Reader {
  public:
    Reader(const std::uint8_t* data, std::size_t size) : data_(data), size_(size) {}

    [[nodiscard]] std::uint64_t Le(std::size_t bytes) {
        Need(bytes);
        std::uint64_t value = 0;
        for (std::size_t i = 0; i < bytes; ++i) {
            value |= static_cast<std::uint64_t>(data_[at_ + i]) << (8U * i);
        }
        at_ += bytes;
        return value;
    }
    [[nodiscard]] std::vector<std::uint8_t> Bytes(std::size_t count) {
        Need(count);
        std::vector<std::uint8_t> out(data_ + at_, data_ + at_ + count);
        at_ += count;
        return out;
    }
    [[nodiscard]] bool AtEnd() const noexcept { return at_ == size_; }

  private:
    void Need(std::size_t bytes) const {
        if (size_ - at_ < bytes) {
            throw std::runtime_error("schema area is truncated");
        }
    }

    const std::uint8_t* data_;
    std::size_t size_;
    std::size_t at_ = 0;
};

}  // namespace

bool IsUtf8(std::span<const std::uint8_t> bytes) noexcept {
    std::size_t i = 0;
    while (i < bytes.size()) {
        const std::uint8_t lead = bytes[i];
        std::size_t length = 0;
        std::uint32_t code = 0;
        if (lead < 0x80U) {
            ++i;
            continue;
        }
        if (lead >= 0xC2U && lead <= 0xDFU) {
            length = 2;
            code = lead & 0x1FU;
        } else if (lead >= 0xE0U && lead <= 0xEFU) {
            length = 3;
            code = lead & 0x0FU;
        } else if (lead >= 0xF0U && lead <= 0xF4U) {
            length = 4;
            code = lead & 0x07U;
        } else {
            return false;
        }
        if (bytes.size() - i < length) {
            return false;
        }
        for (std::size_t k = 1; k < length; ++k) {
            const std::uint8_t next = bytes[i + k];
            if ((next & 0xC0U) != 0x80U) {
                return false;
            }
            code = (code << 6U) | (next & 0x3FU);
        }
        if ((length == 3 && (code < 0x800U || (code >= 0xD800U && code <= 0xDFFFU))) ||
            (length == 4 && (code < 0x10000U || code > 0x10FFFFU))) {
            return false;
        }
        i += length;
    }
    return true;
}

bool IsFixedWidth(ColumnKind kind) noexcept {
    return kind != ColumnKind::kText && kind != ColumnKind::kBytes;
}

std::uint32_t FixedWidthBits(const ColumnType& type) noexcept {
    return IsFixedWidth(type.kind) ? type.size : 0U;
}

std::string ColumnTypeName(const ColumnType& type) {
    const std::string size = std::to_string(type.size);
    switch (type.kind) {
        case ColumnKind::kUnsigned:
            return "u" + size;
        case ColumnKind::kSigned:
            return "i" + size;
        case ColumnKind::kBool:
            return "bool";
        case ColumnKind::kFloat32:
            return "f32";
        case ColumnKind::kFloat64:
            return "f64";
        case ColumnKind::kBits:
            return "bits(" + size + ")";
        case ColumnKind::kText:
            return "text(" + size + ")";
        case ColumnKind::kBytes:
            return "bytes(" + size + ")";
    }
    return "unknown";
}

TableSchema SingleBitsColumnSchema(std::uint32_t block_bits) {
    return TableSchema{
        .version = 1,
        .next_column_id = 2,
        .columns = {Column{
            .id = 1,
            .name = "bits",
            .type = ColumnType{.kind = ColumnKind::kBits, .size = block_bits},
            .nullable = false,
            .required = false,
            .has_default = false,
            .default_value = {},
        }},
    };
}

std::uint32_t FixedBitsPerBlock(const TableSchema& schema) noexcept {
    std::uint64_t total = 0;
    for (const auto& column : schema.columns) {
        total += FixedWidthBits(column.type);
    }
    return total > 0xFFFFFFFFU ? 0xFFFFFFFFU : static_cast<std::uint32_t>(total);
}

namespace {

// The rules for the columns of one version.
void ValidateColumns(const TableSchema& schema) {
    if (schema.columns.empty()) {
        throw std::invalid_argument("a table needs at least one column");
    }
    if (schema.columns.size() > kMaxColumnsPerTable) {
        throw std::invalid_argument(
            "a table has at most " + std::to_string(kMaxColumnsPerTable) + " columns");
    }
    std::unordered_set<std::uint32_t> ids;
    std::unordered_set<std::string> names;
    for (const auto& column : schema.columns) {
        if (!IsValidName(column.name)) {
            throw std::invalid_argument(
                "column name '" + column.name + "' must be 1 to " + std::to_string(kMaxColumnNameBytes) +
                " characters: a-z, 0-9 and _, not starting with a digit");
        }
        if (!names.insert(column.name).second) {
            throw std::invalid_argument("column " + column.name + " appears twice");
        }
        if (column.id == 0U || column.id >= schema.next_column_id || !ids.insert(column.id).second) {
            throw std::invalid_argument("column " + column.name + " has an invalid id");
        }
        ValidateType(column);
        if (column.nullable && column.required) {
            throw std::invalid_argument("column " + column.name + " cannot be both NULL and REQUIRED");
        }
        ValidateDefault(column);
    }
    std::uint64_t fixed_bits = 0;
    for (const auto& column : schema.columns) {
        fixed_bits += FixedWidthBits(column.type);
    }
    if (fixed_bits > kMaxFixedBitsPerBlock) {
        throw std::invalid_argument(
            "the fixed-width columns of a block take " + std::to_string(fixed_bits) + " bits, at most " +
            std::to_string(kMaxFixedBitsPerBlock));
    }
}

// Turns `schema` into the version before it by undoing its last step.
// Throws std::invalid_argument when the step does not match the columns.
void UndoLastStep(TableSchema* schema) {
    const SchemaStep step = std::move(schema->history.back());
    schema->history.pop_back();
    auto& columns = schema->columns;
    for (auto change = step.changes.rbegin(); change != step.changes.rend(); ++change) {
        const std::size_t at = change->position;
        const auto mismatch = [&] {
            return std::invalid_argument(
                "schema history: version " + std::to_string(step.version) + " does not match its columns");
        };
        switch (change->kind) {
            case SchemaChange::Kind::kAddColumn:
                if (at >= columns.size() || columns[at] != change->column) {
                    throw mismatch();
                }
                columns.erase(columns.begin() + static_cast<std::ptrdiff_t>(at));
                // Adding is the only change that takes an id.
                schema->next_column_id = change->column.id;
                break;
            case SchemaChange::Kind::kDropColumn:
                if (at > columns.size()) {
                    throw mismatch();
                }
                columns.insert(columns.begin() + static_cast<std::ptrdiff_t>(at), change->column);
                break;
            case SchemaChange::Kind::kRenameColumn:
                if (at >= columns.size() || columns[at] != change->column) {
                    throw mismatch();
                }
                columns[at].name = change->old_name;
                break;
        }
    }
    schema->version = step.version - 1U;
}

}  // namespace

void ValidateTableSchema(const TableSchema& schema) {
    if (schema.version == 0U) {
        throw std::invalid_argument("schema version must be at least 1");
    }
    ValidateColumns(schema);
    if (schema.version - 1U > kMaxSchemaVersions) {
        throw std::invalid_argument("a table has at most " + std::to_string(kMaxSchemaVersions + 1U) + " schema versions");
    }
    if (schema.history.size() != schema.version - 1U) {
        throw std::invalid_argument(
            "schema history has " + std::to_string(schema.history.size()) + " steps for version " +
            std::to_string(schema.version));
    }
    // Every earlier version is a valid schema too.
    TableSchema earlier = schema;
    while (!earlier.history.empty()) {
        const auto& step = earlier.history.back();
        if (step.version != earlier.version || step.changes.empty()) {
            throw std::invalid_argument("schema history step for version " + std::to_string(step.version) + " is malformed");
        }
        UndoLastStep(&earlier);
        ValidateColumns(earlier);
    }
}

TableSchema SchemaAtVersion(const TableSchema& schema, std::uint64_t version) {
    if (version == 0U || version > schema.version) {
        throw std::invalid_argument(
            "the table has no schema version " + std::to_string(version) + " (current " +
            std::to_string(schema.version) + ")");
    }
    TableSchema earlier = schema;
    while (earlier.version > version) {
        UndoLastStep(&earlier);
    }
    return earlier;
}

namespace {

[[nodiscard]] std::size_t ColumnIndex(const TableSchema& schema, std::string_view name) {
    for (std::size_t i = 0; i < schema.columns.size(); ++i) {
        if (schema.columns[i].name == name) {
            return i;
        }
    }
    throw std::invalid_argument("the table has no column " + std::string(name));
}

[[nodiscard]] TableSchema NextVersion(const TableSchema& schema, SchemaChange change, TableSchema next) {
    next.version = schema.version + 1U;
    next.history.push_back(SchemaStep{.version = next.version, .changes = {std::move(change)}});
    ValidateTableSchema(next);
    return next;
}

}  // namespace

TableSchema AddColumn(const TableSchema& schema, Column column) {
    if (column.required && !column.has_default) {
        throw std::invalid_argument(
            "column " + column.name + ": a REQUIRED column added to a table needs a DEFAULT for the blocks it has");
    }
    TableSchema next = schema;
    column.id = next.next_column_id++;
    next.columns.push_back(column);
    // Built before the call: arguments may be evaluated in any order, and
    // `next` is moved into it.
    SchemaChange change{
        .kind = SchemaChange::Kind::kAddColumn,
        .position = static_cast<std::uint32_t>(next.columns.size() - 1U),
        .column = std::move(column),
        .old_name = {},
    };
    return NextVersion(schema, std::move(change), std::move(next));
}

TableSchema DropColumn(const TableSchema& schema, std::string_view name) {
    const std::size_t at = ColumnIndex(schema, name);
    TableSchema next = schema;
    Column dropped = next.columns[at];
    next.columns.erase(next.columns.begin() + static_cast<std::ptrdiff_t>(at));
    if (FixedBitsPerBlock(next) == 0U) {
        throw std::invalid_argument("column " + dropped.name + " is the last fixed-width column; a table needs one");
    }
    SchemaChange change{
        .kind = SchemaChange::Kind::kDropColumn,
        .position = static_cast<std::uint32_t>(at),
        .column = std::move(dropped),
        .old_name = {},
    };
    return NextVersion(schema, std::move(change), std::move(next));
}

TableSchema RenameColumn(const TableSchema& schema, std::string_view name, std::string new_name) {
    const std::size_t at = ColumnIndex(schema, name);
    TableSchema next = schema;
    next.columns[at].name = std::move(new_name);
    SchemaChange change{
        .kind = SchemaChange::Kind::kRenameColumn,
        .position = static_cast<std::uint32_t>(at),
        .column = next.columns[at],
        .old_name = std::string(name),
    };
    return NextVersion(schema, std::move(change), std::move(next));
}

std::string UnsupportedSchemaReason(const TableSchema& schema) {
    if (FixedBitsPerBlock(schema) == 0U) {
        return "a table needs at least one fixed-width column";
    }
    return {};
}

namespace {

[[nodiscard]] std::string ValueTypeName(const ColumnValue& value) {
    switch (value.index()) {
        case 0:
            return "NULL";
        case 1:
            return "an unsigned integer";
        case 2:
            return "a signed integer";
        case 3:
            return "a bool";
        case 4:
            return "an f32";
        case 5:
            return "an f64";
        case 6:
            return "bits(" + std::to_string(std::get<BitsValue>(value).digits.size()) + ")";
        case 7:
            return "text";
        default:
            return "bytes";
    }
}

template <typename T>
[[nodiscard]] const T& ValueOf(const Column& column, const ColumnValue& value) {
    const T* typed = std::get_if<T>(&value);
    if (typed == nullptr) {
        throw std::invalid_argument(
            "column " + column.name + " is " + ColumnTypeName(column.type) + ", not " + ValueTypeName(value));
    }
    return *typed;
}

}  // namespace

std::vector<std::uint8_t> EncodeColumnValue(const Column& column, const ColumnValue& value) {
    std::vector<std::uint8_t> bytes((FixedWidthBits(column.type) + 7U) / 8U, 0U);
    EncodeColumnValue(column, value, bytes.data());
    return bytes;
}

void EncodeColumnValue(const Column& column, const ColumnValue& value, std::uint8_t* bytes) {
    const std::uint32_t width = FixedWidthBits(column.type);
    if (width == 0U) {
        throw std::invalid_argument("column " + column.name + ": text and bytes columns are not supported yet");
    }
    if (std::holds_alternative<std::monostate>(value)) {
        throw std::invalid_argument("column " + column.name + " cannot be NULL");
    }
    std::fill_n(bytes, (width + 7U) / 8U, std::uint8_t{0});
    if (column.type.kind == ColumnKind::kBits) {
        const auto& digits = ValueOf<BitsValue>(column, value).digits;
        if (digits.size() != width) {
            throw std::invalid_argument(
                "column " + column.name + " is " + ColumnTypeName(column.type) + ", not " + ValueTypeName(value));
        }
        for (std::size_t i = 0; i < digits.size(); ++i) {
            if (digits[i] != '0' && digits[i] != '1') {
                throw std::invalid_argument("column " + column.name + ": a bits value holds only 0 and 1");
            }
            if (digits[i] == '1') {
                bytes[i / 8U] |= static_cast<std::uint8_t>(1U << (i % 8U));
            }
        }
        return;
    }
    std::uint64_t raw = 0;
    switch (column.type.kind) {
        case ColumnKind::kUnsigned: {
            raw = ValueOf<std::uint64_t>(column, value);
            if (width < 64U && (raw >> width) != 0U) {
                throw std::invalid_argument(
                    "column " + column.name + " is " + ColumnTypeName(column.type) + ": " + std::to_string(raw) +
                    " is out of range 0.." + std::to_string((std::uint64_t{1} << width) - 1U));
            }
            break;
        }
        case ColumnKind::kSigned: {
            const std::int64_t signed_value = ValueOf<std::int64_t>(column, value);
            if (width < 64U) {
                const std::int64_t max = (std::int64_t{1} << (width - 1U)) - 1;
                const std::int64_t min = -max - 1;
                if (signed_value < min || signed_value > max) {
                    throw std::invalid_argument(
                        "column " + column.name + " is " + ColumnTypeName(column.type) + ": " +
                        std::to_string(signed_value) + " is out of range " + std::to_string(min) + ".." +
                        std::to_string(max));
                }
            }
            raw = static_cast<std::uint64_t>(signed_value);
            break;
        }
        case ColumnKind::kBool:
            raw = ValueOf<bool>(column, value) ? 1U : 0U;
            break;
        case ColumnKind::kFloat32:
            raw = std::bit_cast<std::uint32_t>(ValueOf<float>(column, value));
            break;
        case ColumnKind::kFloat64:
            raw = std::bit_cast<std::uint64_t>(ValueOf<double>(column, value));
            break;
        default:
            break;
    }
    if (width < 64U) {
        raw &= (std::uint64_t{1} << width) - 1U;
    }
    for (std::size_t i = 0; i < (width + 7U) / 8U; ++i) {
        bytes[i] = static_cast<std::uint8_t>(raw >> (8U * i));
    }
}

ColumnValue DecodeColumnValue(const Column& column, const std::uint8_t* bytes) {
    const std::uint32_t width = FixedWidthBits(column.type);
    if (column.type.kind == ColumnKind::kBits) {
        BitsValue bits;
        bits.digits.resize(width);
        for (std::size_t i = 0; i < width; ++i) {
            bits.digits[i] = ((bytes[i / 8U] >> (i % 8U)) & 1U) != 0U ? '1' : '0';
        }
        return bits;
    }
    std::uint64_t raw = 0;
    for (std::size_t i = 0; i < (width + 7U) / 8U; ++i) {
        raw |= static_cast<std::uint64_t>(bytes[i]) << (8U * i);
    }
    switch (column.type.kind) {
        case ColumnKind::kUnsigned:
            return raw;
        case ColumnKind::kSigned:
            if (width < 64U && ((raw >> (width - 1U)) & 1U) != 0U) {
                raw |= ~((std::uint64_t{1} << width) - 1U);
            }
            return static_cast<std::int64_t>(raw);
        case ColumnKind::kBool:
            return raw != 0U;
        case ColumnKind::kFloat32:
            return std::bit_cast<float>(static_cast<std::uint32_t>(raw));
        case ColumnKind::kFloat64:
            return std::bit_cast<double>(raw);
        default:
            throw std::invalid_argument("column " + column.name + ": text and bytes columns are not supported yet");
    }
}

namespace {

void EncodeColumn(std::vector<std::uint8_t>& out, const Column& column) {
    WriteLe32(out, column.id);
    out.push_back(static_cast<std::uint8_t>(column.type.kind));
    WriteLe32(out, column.type.size);
    out.push_back(static_cast<std::uint8_t>(
        (column.nullable ? kFlagNullable : 0U) | (column.required ? kFlagRequired : 0U) |
        (column.has_default ? kFlagHasDefault : 0U)));
    out.push_back(static_cast<std::uint8_t>(column.name.size()));
    out.insert(out.end(), column.name.begin(), column.name.end());
    if (column.has_default) {
        WriteLe32(out, static_cast<std::uint32_t>(column.default_value.size()));
        out.insert(out.end(), column.default_value.begin(), column.default_value.end());
    }
}

[[nodiscard]] Column DecodeColumn(Reader& in) {
    Column column;
    column.id = static_cast<std::uint32_t>(in.Le(4));
    const auto kind = static_cast<std::uint8_t>(in.Le(1));
    if (!IsKnownKind(kind)) {
        throw std::runtime_error("unknown column type " + std::to_string(kind));
    }
    column.type = ColumnType{.kind = static_cast<ColumnKind>(kind), .size = static_cast<std::uint32_t>(in.Le(4))};
    const auto flags = static_cast<std::uint8_t>(in.Le(1));
    if ((flags & ~kKnownFlags) != 0U) {
        throw std::runtime_error("unknown column flags " + std::to_string(flags));
    }
    column.nullable = (flags & kFlagNullable) != 0U;
    column.required = (flags & kFlagRequired) != 0U;
    column.has_default = (flags & kFlagHasDefault) != 0U;
    const auto name_length = static_cast<std::size_t>(in.Le(1));
    const auto name = in.Bytes(name_length);
    column.name.assign(name.begin(), name.end());
    if (column.has_default) {
        const auto length = static_cast<std::size_t>(in.Le(4));
        if (length > kMaxVariableValueBytes) {
            throw std::runtime_error("column default of " + std::to_string(length) + " bytes");
        }
        column.default_value = in.Bytes(length);
    }
    return column;
}

[[nodiscard]] bool IsKnownChange(std::uint8_t kind) noexcept {
    return kind >= static_cast<std::uint8_t>(SchemaChange::Kind::kAddColumn) &&
           kind <= static_cast<std::uint8_t>(SchemaChange::Kind::kRenameColumn);
}

}  // namespace

std::vector<std::uint8_t> EncodeTableSchema(const TableSchema& schema) {
    ValidateTableSchema(schema);
    std::vector<std::uint8_t> out;
    WriteLe64(out, schema.version);
    WriteLe32(out, schema.next_column_id);
    WriteLe32(out, static_cast<std::uint32_t>(schema.columns.size()));
    for (const auto& column : schema.columns) {
        EncodeColumn(out, column);
    }
    for (const auto& step : schema.history) {
        WriteLe32(out, static_cast<std::uint32_t>(step.changes.size()));
        for (const auto& change : step.changes) {
            out.push_back(static_cast<std::uint8_t>(change.kind));
            WriteLe32(out, change.position);
            EncodeColumn(out, change.column);
            if (change.kind == SchemaChange::Kind::kRenameColumn) {
                out.push_back(static_cast<std::uint8_t>(change.old_name.size()));
                out.insert(out.end(), change.old_name.begin(), change.old_name.end());
            }
        }
    }
    return out;
}

TableSchema DecodeTableSchema(const std::uint8_t* data, std::size_t size) {
    Reader in(data, size);
    TableSchema schema;
    schema.version = in.Le(8);
    schema.next_column_id = static_cast<std::uint32_t>(in.Le(4));
    const auto count = static_cast<std::uint32_t>(in.Le(4));
    if (count > kMaxColumnsPerTable) {
        throw std::runtime_error("schema area declares " + std::to_string(count) + " columns");
    }
    schema.columns.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        schema.columns.push_back(DecodeColumn(in));
    }
    if (schema.version == 0U || schema.version - 1U > kMaxSchemaVersions) {
        throw std::runtime_error("schema version " + std::to_string(schema.version) + " is out of range");
    }
    // One step per version above 1, each creating the next version.
    for (std::uint64_t version = 2; version <= schema.version; ++version) {
        SchemaStep step{.version = version, .changes = {}};
        const auto changes = static_cast<std::uint32_t>(in.Le(4));
        if (changes == 0U || changes > kMaxColumnsPerTable * 2U) {
            throw std::runtime_error("schema version " + std::to_string(version) + " has " + std::to_string(changes) + " changes");
        }
        for (std::uint32_t i = 0; i < changes; ++i) {
            const auto kind = static_cast<std::uint8_t>(in.Le(1));
            if (!IsKnownChange(kind)) {
                throw std::runtime_error("unknown schema change " + std::to_string(kind));
            }
            SchemaChange change;
            change.kind = static_cast<SchemaChange::Kind>(kind);
            change.position = static_cast<std::uint32_t>(in.Le(4));
            change.column = DecodeColumn(in);
            if (change.kind == SchemaChange::Kind::kRenameColumn) {
                const auto name = in.Bytes(static_cast<std::size_t>(in.Le(1)));
                change.old_name.assign(name.begin(), name.end());
            }
            step.changes.push_back(std::move(change));
        }
        schema.history.push_back(std::move(step));
    }
    if (!in.AtEnd()) {
        throw std::runtime_error("schema area has trailing bytes");
    }
    ValidateTableSchema(schema);
    return schema;
}

std::vector<std::uint8_t> EncodeVarValue(const Column& column, const ColumnValue& value) {
    if (std::holds_alternative<std::monostate>(value)) {
        throw std::invalid_argument("column " + column.name + " cannot be NULL");
    }
    std::vector<std::uint8_t> bytes;
    if (column.type.kind == ColumnKind::kText) {
        const auto& text = ValueOf<std::string>(column, value);
        bytes.assign(text.begin(), text.end());
        if (!IsUtf8(bytes)) {
            throw std::invalid_argument("column " + column.name + ": the text is not UTF-8");
        }
    } else if (column.type.kind == ColumnKind::kBytes) {
        bytes = ValueOf<BytesValue>(column, value).bytes;
    } else {
        throw std::invalid_argument("column " + column.name + " is " + ColumnTypeName(column.type) + ", not text or bytes");
    }
    if (bytes.size() > column.type.size) {
        throw std::invalid_argument(
            "column " + column.name + " is " + ColumnTypeName(column.type) + ": " + std::to_string(bytes.size()) +
            " bytes are too long");
    }
    return bytes;
}

ColumnValue DecodeVarValue(const Column& column, std::span<const std::uint8_t> bytes) {
    if (column.type.kind == ColumnKind::kText) {
        return std::string(bytes.begin(), bytes.end());
    }
    return BytesValue{.bytes = std::vector<std::uint8_t>(bytes.begin(), bytes.end())};
}

}  // namespace chunkdb
