#include "chunkdb/chunk_layout.hpp"

#include <algorithm>
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
    : schema_(std::move(schema)), block_count_(block_count) {
    ValidateTableSchema(schema_);
    fixed_index_.assign(schema_.columns.size(), std::string_view::npos);
    std::size_t offset = 0;
    for (std::size_t i = 0; i < schema_.columns.size(); ++i) {
        const auto& column = schema_.columns[i];
        by_name_.emplace(column.name, i);
        const std::uint32_t width = FixedWidthBits(column.type);
        if (width == 0U) {
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
