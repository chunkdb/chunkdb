#include "chunkdb/chunk_layout.hpp"

#include <algorithm>
#include <array>
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
    : ChunkLayout(std::move(schema), block_count, max_payload_bytes, ColumnDomain::kBlock) {}

ChunkLayout::ChunkLayout(
    TableSchema schema, std::size_t block_count, std::size_t max_payload_bytes, ColumnDomain domain)
    : schema_(std::move(schema)), block_count_(block_count), payload_bytes_limit_(max_payload_bytes) {
    if (domain == ColumnDomain::kChunk) {
        if (block_count != 1U) throw std::invalid_argument("chunk columns require exactly one row");
        ValidateChunkSchema(schema_);
    }
    else if (domain == ColumnDomain::kBlock) ValidateTableSchema(schema_);
    else throw std::invalid_argument("unknown column domain");
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
        if (column.default_commit_time) commit_time_columns_.push_back(fixed_.size());
        fixed_.push_back(fixed);
    }
    payload_bytes_ = offset;
    bit_string_blocks_ = schema_.columns.size() == 1U && schema_.columns.front().type.kind == ColumnKind::kBits &&
                         !schema_.columns.front().nullable;
}

namespace {

// Bit `bit` of `bytes`; bits past the end are zero (a chunk without state).
[[nodiscard]] bool BitOrZero(std::span<const std::uint8_t> bytes, std::size_t bit) noexcept {
    return bit / 8U < bytes.size() && GetBit(bytes.data(), bit);
}

}  // namespace

void ChunkLayout::StampCommitTime(std::span<const std::uint8_t> before_payload,
    std::span<const std::uint8_t> before_presence, std::span<const std::uint8_t> presence,
    const std::vector<bool>& var_changed, std::uint64_t commit_time_ms, std::vector<std::uint8_t>* payload) const {
    if (commit_time_columns_.empty()) return;
    std::array<std::uint8_t, 8> time{};
    for (std::size_t i = 0; i < time.size(); ++i) time[i] = static_cast<std::uint8_t>(commit_time_ms >> (8U * i));
    const auto stamped = [this](std::size_t fixed) {
        return std::find(commit_time_columns_.begin(), commit_time_columns_.end(), fixed) != commit_time_columns_.end();
    };
    // A chunk without state reads as zero bytes.
    std::vector<std::uint8_t> zero;
    if (before_payload.size() < payload_bytes_) {
        zero.assign(payload_bytes_, 0U);
        before_payload = zero;
    }
    std::vector<std::uint8_t> was;
    std::vector<std::uint8_t> now;
    for (std::size_t block = 0; block < block_count_; ++block) {
        if (!BitOrZero(presence, block)) continue;
        // Compares the value's bytes, as ReadValueBits gives them.
        const auto differs = [&](const FixedColumn& column) {
            if (column.validity != kNoValidity &&
                BitOrZero(before_payload, column.validity * 8U + block) != BitOrZero(*payload, column.validity * 8U + block))
                return true;
            const std::size_t first = column.values * 8U + block * column.width;
            was.resize((column.width + 7U) / 8U);
            now.resize(was.size());
            ReadValueBits(before_payload.data(), first, was.data(), column.width);
            ReadValueBits(payload->data(), first, now.data(), column.width);
            return was != now;
        };
        bool other = !BitOrZero(before_presence, block) || (block < var_changed.size() && var_changed[block]);
        for (std::size_t fixed = 0; !other && fixed < fixed_.size(); ++fixed)
            other = !stamped(fixed) && differs(fixed_[fixed]);
        if (!other) continue;
        for (const auto fixed : commit_time_columns_) {
            // A value the commit gives the column itself stays.
            if (differs(fixed_[fixed])) continue;
            WriteValueBits(payload->data(), fixed_[fixed].values * 8U + block * fixed_[fixed].width, time.data(), 64U);
        }
    }
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
    RequireValidVarsView(vars, presence);
}

void ChunkLayout::RequireValidVarsView(const ChunkVars& vars, std::span<const std::uint8_t> presence) const {
    if (presence.size() != (block_count_ + 7U) / 8U)
        throw std::invalid_argument("variable values require the complete presence bitmap");
    for (const auto entry : vars) {
        const auto fail = [&](std::string reason) {
            throw std::invalid_argument("column " + std::to_string(entry.key.column_id) + " block " +
                std::to_string(entry.key.block_index) + ": " + reason);
        };
        const Column* column = VarColumn(entry.key.column_id);
        if (column == nullptr) {
            fail("no text or bytes column has this id");
        }
        if (entry.key.block_index >= block_count_ || !GetBit(presence.data(), entry.key.block_index)) {
            fail("a value of an absent block");
        }
        if (entry.value.size() > column->type.size) {
            fail("longer than " + ColumnTypeName(column->type));
        }
        if (entry.value.empty() && !column->nullable) {
            fail("an empty value stored in a column that cannot be NULL");
        }
        if (column->type.kind == ColumnKind::kText && !IsUtf8(entry.value)) {
            fail("text that is not UTF-8");
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
                    // SET NULL: every present block has a value. A NULL that
                    // DROP NULL found none of keeps its zero value bits.
                    std::memcpy(
                        next.data() + fixed->validity,
                        old->validity != ChunkLayout::kNoValidity ? payload->data() + old->validity : presence.data(),
                        BytesForBits(block_count));
                }
            } else if (before.nullable != column.nullable) {
                // No entry is NULL of a NULL column and the empty value of
                // another: SET NULL keeps empty values as entries, DROP NULL
                // drops them.
                std::vector<bool> stored(column.nullable ? block_count : 0U, false);
                for (const auto entry : *vars) {
                    if (entry.key.column_id != column.id) continue;
                    if (column.nullable) stored[entry.key.block_index] = true;
                    else if (entry.value.empty()) var_changes.push_back(VarChange{.key = entry.key, .value = std::nullopt});
                }
                for (std::size_t block = 0; block < stored.size(); ++block) {
                    if (stored[block] || !GetBit(presence.data(), block)) continue;
                    var_changes.push_back(VarChange{
                        .key = VarKey{.column_id = column.id, .block_index = static_cast<std::uint32_t>(block)},
                        .value = std::vector<std::uint8_t>{},
                    });
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
