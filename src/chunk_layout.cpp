#include "chunkdb/chunk_layout.hpp"

#include <algorithm>
#include <optional>
#include <variant>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>

namespace chunkdb {

namespace {

[[nodiscard]] std::size_t BytesForBits(std::size_t bits) noexcept {
    return (bits + 7U) / 8U;
}

[[nodiscard]] bool GetBit(const std::uint8_t* data, std::size_t bit) noexcept {
    return ((data[bit / 8U] >> (bit % 8U)) & 1U) != 0U;
}

void PutBit(std::uint8_t* data, std::size_t bit, bool value) noexcept {
    const auto mask = static_cast<std::uint8_t>(1U << (bit % 8U));
    if (value) {
        data[bit / 8U] |= mask;
    } else {
        data[bit / 8U] &= static_cast<std::uint8_t>(~mask);
    }
}

void ClearBits(std::uint8_t* data, std::size_t bit_offset, std::size_t width) noexcept {
    if (bit_offset % 8U == 0U && width % 8U == 0U) {
        std::memset(data + bit_offset / 8U, 0, width / 8U);
        return;
    }
    for (std::size_t i = 0; i < width; ++i) {
        PutBit(data, bit_offset + i, false);
    }
}

// Zeroes the bits of the byte range [begin, begin + bytes) from `used_bits`
// on.
void ClearTail(std::uint8_t* data, std::size_t begin, std::size_t bytes, std::size_t used_bits) noexcept {
    for (std::size_t bit = used_bits; bit < bytes * 8U; ++bit) {
        PutBit(data + begin, bit, false);
    }
}

}  // namespace

ChunkLayout::ChunkLayout(TableSchema schema, std::size_t block_count, std::size_t max_payload_bytes)
    : schema_(std::move(schema)), block_count_(block_count), payload_bytes_limit_(max_payload_bytes) {
    ValidateTableSchema(schema_);
    fixed_index_.assign(schema_.columns.size(), std::string_view::npos);
    std::size_t offset = 0;
    for (std::size_t i = 0; i < schema_.columns.size(); ++i) {
        const auto& column = schema_.columns[i];
        by_name_.emplace(column.name, i);
        by_id_.emplace(column.id, i);
        const std::uint32_t width = FixedWidthBits(column.type);
        if (width == 0U) {
            var_by_id_.emplace(column.id, i);
            continue;
        }
        FixedColumn fixed{.column = i, .width = width, .values = offset, .validity = kNoValidity};
        offset += BytesForBits(block_count_ * width);
        if (column.nullable) {
            fixed.validity = offset;
            offset += BytesForBits(block_count_);
        }
        if (offset > max_payload_bytes) {
            throw std::invalid_argument("chunk payload must be <= " + std::to_string(max_payload_bytes) + " bytes");
        }
        fixed_index_[i] = fixed_.size();
        fixed_.push_back(fixed);
    }
    payload_bytes_ = offset;
    bit_string_blocks_ = schema_.columns.size() == 1U && schema_.columns.front().type.kind == ColumnKind::kBits &&
                         !schema_.columns.front().nullable;
}

const ChunkLayout::FixedColumn* ChunkLayout::FixedColumnAt(std::size_t index) const noexcept {
    if (index >= fixed_index_.size() || fixed_index_[index] == std::string_view::npos) {
        return nullptr;
    }
    return &fixed_[fixed_index_[index]];
}

const Column* ChunkLayout::VarColumn(std::uint32_t column_id) const noexcept {
    const auto found = var_by_id_.find(column_id);
    return found == var_by_id_.end() ? nullptr : &schema_.columns[found->second];
}

void ChunkLayout::RequireValidVars(const ChunkVars& vars, const std::vector<std::uint8_t>& presence) const {
    for (const auto entry : vars) {
        const auto where = "column " + std::to_string(entry.key.column_id) + " block " +
                           std::to_string(entry.key.block_index);
        const Column* column = VarColumn(entry.key.column_id);
        if (column == nullptr) {
            throw std::invalid_argument(where + ": no text or bytes column has this id");
        }
        if (entry.key.block_index >= block_count_ || !GetBit(presence.data(), entry.key.block_index)) {
            throw std::invalid_argument(where + ": a value of an absent block");
        }
        if (entry.value.size() > column->type.size) {
            throw std::invalid_argument(where + ": longer than " + ColumnTypeName(column->type));
        }
        if (entry.value.empty() && !column->nullable) {
            throw std::invalid_argument(where + ": an empty value stored in a column that cannot be NULL");
        }
        if (column->type.kind == ColumnKind::kText && !IsUtf8(entry.value)) {
            throw std::invalid_argument(where + ": text that is not UTF-8");
        }
    }
}

std::size_t ChunkLayout::IndexOfId(std::uint32_t column_id) const noexcept {
    const auto found = by_id_.find(column_id);
    return found == by_id_.end() ? std::string_view::npos : found->second;
}

std::size_t ChunkLayout::FindColumn(std::string_view name) const noexcept {
    const auto found = by_name_.find(name);
    return found == by_name_.end() ? std::string_view::npos : found->second;
}

void ChunkLayout::MaskPadding(std::vector<std::uint8_t>* payload) const {
    if (payload->size() != payload_bytes_) {
        throw std::invalid_argument("payload size does not match the table's columns");
    }
    for (const auto& fixed : fixed_) {
        const std::size_t value_bits = block_count_ * fixed.width;
        ClearTail(payload->data(), fixed.values, BytesForBits(value_bits), value_bits);
        if (fixed.validity != kNoValidity) {
            ClearTail(payload->data(), fixed.validity, BytesForBits(block_count_), block_count_);
        }
    }
}

void ChunkLayout::ClearEmptyValues(
    const std::vector<std::uint8_t>& presence,
    std::vector<std::uint8_t>* payload) const {
    if (payload->size() != payload_bytes_ || presence.size() != BytesForBits(block_count_)) {
        throw std::invalid_argument("chunk state size does not match the table's columns");
    }
    std::uint8_t* data = payload->data();
    for (const auto& fixed : fixed_) {
        // Eight blocks at a time: a block keeps its value when it is present
        // and (for a NULL column) has one.
        for (std::size_t group = 0; group * 8U < block_count_; ++group) {
            std::uint8_t kept = presence[group];
            if (fixed.validity != kNoValidity) {
                kept &= data[fixed.validity + group];
            }
            if (kept != 0xFFU) {
                const std::size_t end = std::min(block_count_, group * 8U + 8U);
                for (std::size_t block = group * 8U; block < end; ++block) {
                    if (((kept >> (block % 8U)) & 1U) == 0U) {
                        ClearBits(data, fixed.values * 8U + block * fixed.width, fixed.width);
                    }
                }
            }
            if (fixed.validity != kNoValidity) {
                data[fixed.validity + group] &= presence[group];
            }
        }
    }
}

namespace {

// The conversion the step that made `to` gives column `column_id`; `to` must
// be the version right after `from`.
[[nodiscard]] Conversion ConversionOf(const ChunkLayout& from, const ChunkLayout& to, std::uint32_t column_id) {
    const auto& history = to.schema().history;
    if (to.schema().version != from.schema().version + 1U || history.empty()) {
        throw std::logic_error("a type change is translated one version at a time");
    }
    for (const auto& change : history.back().changes) {
        if (change.kind == SchemaChange::Kind::kChangeType && change.column.id == column_id) {
            return change.conversion;
        }
    }
    throw std::logic_error("column " + std::to_string(column_id) + " changed its type without a recorded change");
}

// Converts the values of one column whose type changed between the adjacent
// versions `from` and `to`.
void ConvertColumn(
    const ChunkLayout& from,
    const ChunkLayout& to,
    std::size_t from_index,
    std::size_t to_index,
    const std::vector<std::uint8_t>& presence,
    const std::vector<std::uint8_t>& payload,
    std::vector<std::uint8_t>* next,
    const ChunkVars& vars,
    std::vector<VarChange>* var_changes) {
    const Column& before = from.schema().columns[from_index];
    const Column& after = to.schema().columns[to_index];
    const Conversion conversion = ConversionOf(from, to, after.id);
    const auto* old_fixed = from.FixedColumnAt(from_index);
    const auto* new_fixed = to.FixedColumnAt(to_index);
    if (new_fixed == nullptr) {
        // Text or bytes: each stored value converts; an empty value of a
        // column that cannot be NULL, or a NULL, is no entry.
        for (const auto entry : vars) {
            if (entry.key.column_id != after.id) {
                continue;
            }
            const ColumnValue value =
                ConvertValue(before, after, conversion, DecodeVarValue(before, entry.value));
            std::optional<std::vector<std::uint8_t>> bytes;
            if (!std::holds_alternative<std::monostate>(value)) {
                bytes = EncodeVarValue(after, value);
                if (bytes->empty() && !after.nullable) {
                    bytes.reset();
                }
            }
            const bool same = bytes.has_value() &&
                              std::equal(bytes->begin(), bytes->end(), entry.value.begin(), entry.value.end());
            if (!same) {
                var_changes->push_back(VarChange{.key = entry.key, .value = std::move(bytes)});
            }
        }
        return;
    }
    std::vector<std::uint8_t> bytes((old_fixed->width + 7U) / 8U);
    std::vector<std::uint8_t> encoded((new_fixed->width + 7U) / 8U);
    for (std::size_t block = 0; block < to.block_count(); ++block) {
        if (!GetBit(presence.data(), block)) {
            continue;
        }
        ColumnValue value = std::monostate{};
        if (old_fixed->validity == ChunkLayout::kNoValidity || GetBit(payload.data() + old_fixed->validity, block)) {
            ReadValueBits(payload.data(), old_fixed->values * 8U + block * old_fixed->width, bytes.data(), old_fixed->width);
            value = DecodeColumnValue(before, bytes.data());
        }
        value = ConvertValue(before, after, conversion, value);
        if (std::holds_alternative<std::monostate>(value)) {
            continue;  // NULL: value and validity bits stay zero
        }
        EncodeColumnValue(after, value, encoded.data());
        WriteValueBits(next->data(), new_fixed->values * 8U + block * new_fixed->width, encoded.data(), new_fixed->width);
        if (new_fixed->validity != ChunkLayout::kNoValidity) {
            PutBit(next->data() + new_fixed->validity, block, true);
        }
    }
}

}  // namespace

void TranslateChunk(
    const ChunkLayout& from,
    const ChunkLayout& to,
    const std::vector<std::uint8_t>& presence,
    std::vector<std::uint8_t>* payload,
    ChunkVars* vars) {
    const std::size_t block_count = to.block_count();
    if (from.block_count() != block_count || payload->size() != from.payload_bytes() ||
        presence.size() != BytesForBits(block_count)) {
        throw std::invalid_argument("chunk state does not match the layout it is translated from");
    }
    std::vector<std::uint8_t> next(to.payload_bytes(), 0U);
    std::vector<VarChange> var_changes;
    const auto& columns = to.schema().columns;
    for (std::size_t index = 0; index < columns.size(); ++index) {
        const Column& column = columns[index];
        const auto* fixed = to.FixedColumnAt(index);
        const std::size_t from_index = from.IndexOfId(column.id);
        if (from_index != std::string_view::npos) {
            const Column& before = from.schema().columns[from_index];
            if (before.type != column.type) {
                ConvertColumn(from, to, from_index, index, presence, *payload, &next, *vars, &var_changes);
                continue;
            }
            if (fixed != nullptr) {
                // Byte-aligned arrays of the same size: one copy each.
                const auto* old = from.FixedColumnAt(from_index);
                std::memcpy(
                    next.data() + fixed->values, payload->data() + old->values,
                    BytesForBits(block_count * fixed->width));
                if (fixed->validity != ChunkLayout::kNoValidity) {
                    std::memcpy(
                        next.data() + fixed->validity, payload->data() + old->validity, BytesForBits(block_count));
                }
            }
            continue;
        }
        // A column added since `from`: what a new block would get.
        for (std::size_t block = 0; block < block_count; ++block) {
            if (!GetBit(presence.data(), block)) {
                continue;
            }
            if (fixed == nullptr) {
                if (column.has_default && (!column.default_value.empty() || column.nullable)) {
                    var_changes.push_back(VarChange{
                        .key = VarKey{.column_id = column.id, .block_index = static_cast<std::uint32_t>(block)},
                        .value = column.default_value,
                    });
                }
                continue;
            }
            if (column.has_default) {
                WriteValueBits(next.data(), fixed->values * 8U + block * fixed->width, column.default_value.data(), fixed->width);
            }
            if (fixed->validity != ChunkLayout::kNoValidity && (column.has_default || !column.nullable)) {
                PutBit(next.data() + fixed->validity, block, true);
            }
        }
    }
    // The values of dropped columns go.
    for (const auto entry : *vars) {
        if (to.VarColumn(entry.key.column_id) == nullptr) {
            var_changes.push_back(VarChange{.key = entry.key, .value = std::nullopt});
        }
    }
    if (!var_changes.empty()) {
        std::sort(var_changes.begin(), var_changes.end(), [](const VarChange& lhs, const VarChange& rhs) {
            return lhs.key < rhs.key;
        });
        *vars = ChunkVars::Merge(*vars, var_changes);
    }
    *payload = std::move(next);
}

void WriteValueBits(std::uint8_t* payload, std::size_t bit_offset, const std::uint8_t* value, std::uint32_t width) {
    if (bit_offset % 8U == 0U && width % 8U == 0U) {
        std::memcpy(payload + bit_offset / 8U, value, width / 8U);
        return;
    }
    for (std::size_t i = 0; i < width; ++i) {
        PutBit(payload, bit_offset + i, GetBit(value, i));
    }
}

void ReadValueBits(const std::uint8_t* payload, std::size_t bit_offset, std::uint8_t* value, std::uint32_t width) {
    if (bit_offset % 8U == 0U && width % 8U == 0U) {
        std::memcpy(value, payload + bit_offset / 8U, width / 8U);
        return;
    }
    std::memset(value, 0, BytesForBits(width));
    for (std::size_t i = 0; i < width; ++i) {
        if (GetBit(payload, bit_offset + i)) {
            PutBit(value, i, true);
        }
    }
}

}  // namespace chunkdb
