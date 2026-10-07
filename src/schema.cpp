#include "chunkdb/schema.hpp"

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

// Well-formed UTF-8: no overlong forms, no surrogates, at most U+10FFFF.
[[nodiscard]] bool IsUtf8(const std::vector<std::uint8_t>& bytes) noexcept {
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

void ValidateTableSchema(const TableSchema& schema) {
    if (schema.version == 0U) {
        throw std::invalid_argument("schema version must be at least 1");
    }
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

std::string UnsupportedSchemaReason(const TableSchema& schema) {
    if (schema.columns.size() != 1U || !IsFixedWidth(schema.columns.front().type.kind) ||
        schema.columns.front().nullable) {
        return "tables with more than one column, NULL columns and text or bytes columns are not "
               "supported by this build yet";
    }
    return {};
}

std::vector<std::uint8_t> EncodeTableSchema(const TableSchema& schema) {
    ValidateTableSchema(schema);
    std::vector<std::uint8_t> out;
    WriteLe64(out, schema.version);
    WriteLe32(out, schema.next_column_id);
    WriteLe32(out, static_cast<std::uint32_t>(schema.columns.size()));
    for (const auto& column : schema.columns) {
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
        schema.columns.push_back(std::move(column));
    }
    if (!in.AtEnd()) {
        throw std::runtime_error("schema area has trailing bytes");
    }
    ValidateTableSchema(schema);
    return schema;
}

}  // namespace chunkdb
