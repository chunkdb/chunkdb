#include "chunkdb/extra_data.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace chunkdb {

namespace {

[[nodiscard]] std::uint32_t ReadU32(const std::uint8_t* data) noexcept {
    return static_cast<std::uint32_t>(data[0]) | (static_cast<std::uint32_t>(data[1]) << 8U) |
           (static_cast<std::uint32_t>(data[2]) << 16U) |
           (static_cast<std::uint32_t>(data[3]) << 24U);
}

// Mask of the bits of a value's last byte that hold data.
[[nodiscard]] std::uint8_t LastByteMask(std::uint32_t bit_length) noexcept {
    const unsigned used = bit_length % 8U;
    return used == 0U ? std::uint8_t{0xFFU} : static_cast<std::uint8_t>(0xFFU >> (8U - used));
}

// Throws std::invalid_argument unless `value` is one MakeExtraValue accepts
// with ExtraPadding::kReject.
void RequireWellFormed(const ExtraValue& value) {
    if (value.bit_length == 0U || value.bytes.size() != ExtraValueBytes(value.bit_length)) {
        throw std::invalid_argument("malformed extra data value");
    }
    if ((value.bytes.back() & static_cast<std::uint8_t>(~LastByteMask(value.bit_length))) != 0U) {
        throw std::invalid_argument("extra data has set bits past its bit length");
    }
}

[[nodiscard]] std::size_t EntryBytes(const ExtraValue& value) noexcept {
    return kExtraEntryHeaderBytes + value.bytes.size();
}

void WriteU32At(std::uint8_t* at, std::uint32_t value) noexcept {
    for (unsigned i = 0; i < 4U; ++i) {
        at[i] = static_cast<std::uint8_t>((value >> (8U * i)) & 0xFFU);
    }
}

}  // namespace

void RequireValidExtraLimits(std::uint32_t max_block_bits, std::size_t max_chunk_bytes) {
    if (max_block_bits == 0U || max_block_bits > kExtraMaxBlockBitsLimit) {
        throw std::invalid_argument(
            "extra_max_block_bits must be between 1 and " +
            std::to_string(kExtraMaxBlockBitsLimit));
    }
    if (max_chunk_bytes <= kExtraEntryHeaderBytes || max_chunk_bytes > kExtraMaxChunkBytesLimit) {
        throw std::invalid_argument(
            "extra_max_chunk_bytes must be between " + std::to_string(kExtraEntryHeaderBytes + 1U) +
            " and " + std::to_string(kExtraMaxChunkBytesLimit));
    }
    if (kExtraEntryHeaderBytes + ExtraValueBytes(max_block_bits) > max_chunk_bytes) {
        throw std::invalid_argument(
            "extra_max_chunk_bytes (" + std::to_string(max_chunk_bytes) +
            ") must hold one value of extra_max_block_bits (" + std::to_string(max_block_bits) +
            " bits take " + std::to_string(kExtraEntryHeaderBytes + ExtraValueBytes(max_block_bits)) +
            " bytes)");
    }
}

ExtraValue MakeExtraValue(
    std::uint32_t bit_length,
    std::vector<std::uint8_t> bytes,
    ExtraPadding padding) {
    if (bit_length == 0U) {
        throw std::invalid_argument("extra data must have at least 1 bit");
    }
    if (bytes.size() != ExtraValueBytes(bit_length)) {
        throw std::invalid_argument(
            "extra data of " + std::to_string(bit_length) + " bits takes " +
            std::to_string(ExtraValueBytes(bit_length)) + " bytes, got " +
            std::to_string(bytes.size()));
    }
    const std::uint8_t mask = LastByteMask(bit_length);
    if ((bytes.back() & static_cast<std::uint8_t>(~mask)) != 0U) {
        if (padding == ExtraPadding::kReject) {
            throw std::invalid_argument("extra data has set bits past its bit length");
        }
        bytes.back() &= mask;
    }
    return ExtraValue{.bit_length = bit_length, .bytes = std::move(bytes)};
}

bool operator==(const ExtraValueView& lhs, const ExtraValueView& rhs) noexcept {
    return lhs.bit_length == rhs.bit_length &&
           std::equal(lhs.bytes.begin(), lhs.bytes.end(), rhs.bytes.begin(), rhs.bytes.end());
}

bool operator==(const ExtraValueView& lhs, const ExtraValue& rhs) noexcept {
    return lhs.bit_length == rhs.bit_length &&
           std::equal(lhs.bytes.begin(), lhs.bytes.end(), rhs.bytes.begin(), rhs.bytes.end());
}

std::uint32_t ChunkExtra::BlockAt(std::uint32_t offset) const noexcept {
    return ReadU32(data_.data() + offset);
}

std::size_t ChunkExtra::EntryBytesAt(std::uint32_t offset) const noexcept {
    return kExtraEntryHeaderBytes + ExtraValueBytes(ReadU32(data_.data() + offset + 4U));
}

std::size_t ChunkExtra::LowerBound(std::uint32_t block_index) const noexcept {
    const auto it = std::lower_bound(
        offsets_.begin(), offsets_.end(), block_index,
        [this](std::uint32_t offset, std::uint32_t index) { return BlockAt(offset) < index; });
    return static_cast<std::size_t>(it - offsets_.begin());
}

ChunkExtra::Entry ChunkExtra::entry(std::size_t index) const {
    const std::uint32_t offset = offsets_.at(index);
    const std::uint32_t bit_length = ReadU32(data_.data() + offset + 4U);
    return Entry{
        .block_index = BlockAt(offset),
        .value =
            ExtraValueView{
                .bit_length = bit_length,
                .bytes = std::span<const std::uint8_t>(
                    data_.data() + offset + kExtraEntryHeaderBytes, ExtraValueBytes(bit_length)),
            },
    };
}

std::optional<ExtraValueView> ChunkExtra::Find(std::uint32_t block_index) const noexcept {
    const std::size_t at = LowerBound(block_index);
    if (at == offsets_.size() || BlockAt(offsets_[at]) != block_index) {
        return std::nullopt;
    }
    const std::uint32_t offset = offsets_[at];
    const std::uint32_t bit_length = ReadU32(data_.data() + offset + 4U);
    return ExtraValueView{
        .bit_length = bit_length,
        .bytes = std::span<const std::uint8_t>(
            data_.data() + offset + kExtraEntryHeaderBytes, ExtraValueBytes(bit_length)),
    };
}

void ChunkExtra::Assign(std::uint32_t block_index, const ExtraValue& value) {
    RequireWellFormed(value);
    const std::size_t new_bytes = EntryBytes(value);
    const std::size_t at = LowerBound(block_index);
    const bool exists = at < offsets_.size() && BlockAt(offsets_[at]) == block_index;
    const std::size_t old_bytes = exists ? EntryBytesAt(offsets_[at]) : 0U;
    const std::size_t offset = at < offsets_.size() ? offsets_[at] : data_.size();
    if (data_.size() - old_bytes + new_bytes > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("extra data of one chunk exceeds 4 GiB");
    }
    // Allocate first, so a failure leaves the value as it was.
    if (new_bytes > old_bytes) {
        data_.reserve(data_.size() + (new_bytes - old_bytes));
    }
    if (!exists) {
        offsets_.reserve(offsets_.size() + 1U);
        offsets_.insert(offsets_.begin() + static_cast<std::ptrdiff_t>(at), static_cast<std::uint32_t>(offset));
    }
    // Resize the entry's room in place, then write it.
    const auto begin = data_.begin() + static_cast<std::ptrdiff_t>(offset);
    if (new_bytes > old_bytes) {
        data_.insert(begin + static_cast<std::ptrdiff_t>(old_bytes), new_bytes - old_bytes, std::uint8_t{0});
    } else if (new_bytes < old_bytes) {
        data_.erase(begin + static_cast<std::ptrdiff_t>(new_bytes), begin + static_cast<std::ptrdiff_t>(old_bytes));
    }
    std::uint8_t* out = data_.data() + offset;
    WriteU32At(out, block_index);
    WriteU32At(out + 4U, value.bit_length);
    std::copy(value.bytes.begin(), value.bytes.end(), out + kExtraEntryHeaderBytes);
    const auto shift = static_cast<std::int64_t>(new_bytes) - static_cast<std::int64_t>(old_bytes);
    for (std::size_t i = at + 1U; i < offsets_.size(); ++i) {
        offsets_[i] = static_cast<std::uint32_t>(static_cast<std::int64_t>(offsets_[i]) + shift);
    }
}

bool ChunkExtra::Remove(std::uint32_t block_index) noexcept {
    const std::size_t at = LowerBound(block_index);
    if (at == offsets_.size() || BlockAt(offsets_[at]) != block_index) {
        return false;
    }
    const std::uint32_t offset = offsets_[at];
    const std::size_t bytes = EntryBytesAt(offset);
    const auto begin = data_.begin() + static_cast<std::ptrdiff_t>(offset);
    data_.erase(begin, begin + static_cast<std::ptrdiff_t>(bytes));
    offsets_.erase(offsets_.begin() + static_cast<std::ptrdiff_t>(at));
    for (std::size_t i = at; i < offsets_.size(); ++i) {
        offsets_[i] -= static_cast<std::uint32_t>(bytes);
    }
    return true;
}

std::optional<ExtraValue> ChunkExtra::Put(std::uint32_t block_index, const ExtraValue& value) {
    auto previous = Find(block_index);
    std::optional<ExtraValue> result;
    if (previous.has_value()) {
        result = previous->ToValue();
    }
    Assign(block_index, value);
    return result;
}

std::optional<ExtraValue> ChunkExtra::Erase(std::uint32_t block_index) {
    auto previous = Find(block_index);
    if (!previous.has_value()) {
        return std::nullopt;
    }
    auto result = previous->ToValue();
    (void)Remove(block_index);
    return result;
}

void ChunkExtra::reserve(std::size_t entries, std::size_t bytes) {
    offsets_.reserve(offsets_.size() + entries);
    data_.reserve(data_.size() + bytes);
}

ChunkExtra ChunkExtra::Merge(const ChunkExtra& base, const std::vector<ExtraChange>& changes) {
    std::size_t bytes = base.data_.size();
    for (std::size_t i = 0; i < changes.size(); ++i) {
        if (i > 0 && changes[i].block_index <= changes[i - 1].block_index) {
            throw std::logic_error("extra-data changes are not in strictly ascending block order");
        }
        if (changes[i].value.has_value()) {
            RequireWellFormed(*changes[i].value);
            bytes += EntryBytes(*changes[i].value);
        }
    }
    ChunkExtra merged;
    merged.data_.reserve(bytes);
    merged.offsets_.reserve(base.offsets_.size() + changes.size());
    const auto append = [&merged](const std::uint8_t* entry, std::size_t size) {
        merged.offsets_.push_back(static_cast<std::uint32_t>(merged.data_.size()));
        merged.data_.insert(merged.data_.end(), entry, entry + size);
    };
    std::size_t next = 0;
    for (const auto& change : changes) {
        // Base entries before the changed block stay.
        while (next < base.offsets_.size() && base.BlockAt(base.offsets_[next]) < change.block_index) {
            const std::uint32_t offset = base.offsets_[next++];
            append(base.data_.data() + offset, base.EntryBytesAt(offset));
        }
        if (next < base.offsets_.size() && base.BlockAt(base.offsets_[next]) == change.block_index) {
            ++next;
        }
        if (change.value.has_value()) {
            std::uint8_t header[kExtraEntryHeaderBytes];
            WriteU32At(header, change.block_index);
            WriteU32At(header + 4U, change.value->bit_length);
            merged.offsets_.push_back(static_cast<std::uint32_t>(merged.data_.size()));
            merged.data_.insert(merged.data_.end(), header, header + kExtraEntryHeaderBytes);
            merged.data_.insert(merged.data_.end(), change.value->bytes.begin(), change.value->bytes.end());
        }
    }
    while (next < base.offsets_.size()) {
        const std::uint32_t offset = base.offsets_[next++];
        append(base.data_.data() + offset, base.EntryBytesAt(offset));
    }
    if (merged.data_.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("extra data of one chunk exceeds 4 GiB");
    }
    return merged;
}

void ChunkExtra::EncodeTo(std::vector<std::uint8_t>* out) const {
    out->insert(out->end(), data_.begin(), data_.end());
}

ChunkExtra ChunkExtra::Decode(
    const std::uint8_t* data,
    std::size_t size,
    std::size_t block_count,
    ExtraPadding padding) {
    if (size > kExtraMaxChunkBytesLimit) {
        throw std::invalid_argument(
            "extra data section of " + std::to_string(size) + " bytes exceeds " +
            std::to_string(kExtraMaxChunkBytesLimit));
    }
    ChunkExtra extra;
    extra.data_.assign(data, data + size);
    std::size_t at = 0;
    while (at < size) {
        if (size - at < kExtraEntryHeaderBytes) {
            throw std::invalid_argument("extra data entry header extends past the section");
        }
        const std::uint32_t block_index = ReadU32(data + at);
        const std::uint32_t bit_length = ReadU32(data + at + 4U);
        if (block_index >= block_count) {
            throw std::invalid_argument(
                "extra data for block index " + std::to_string(block_index) +
                ", the chunk has " + std::to_string(block_count) + " blocks");
        }
        if (!extra.offsets_.empty() && block_index <= extra.BlockAt(extra.offsets_.back())) {
            throw std::invalid_argument(
                "extra data block indexes are not strictly ascending (" +
                std::to_string(block_index) + " after " +
                std::to_string(extra.BlockAt(extra.offsets_.back())) + ")");
        }
        if (bit_length == 0U) {
            throw std::invalid_argument(
                "extra data of block index " + std::to_string(block_index) + " has 0 bits");
        }
        const std::size_t value_bytes = ExtraValueBytes(bit_length);
        if (size - at - kExtraEntryHeaderBytes < value_bytes) {
            throw std::invalid_argument("extra data value extends past the section");
        }
        const std::uint8_t mask = LastByteMask(bit_length);
        std::uint8_t& last = extra.data_[at + kExtraEntryHeaderBytes + value_bytes - 1U];
        if ((last & static_cast<std::uint8_t>(~mask)) != 0U) {
            if (padding == ExtraPadding::kReject) {
                throw std::invalid_argument("extra data has set bits past its bit length");
            }
            last &= mask;
        }
        extra.offsets_.push_back(static_cast<std::uint32_t>(at));
        at += kExtraEntryHeaderBytes + value_bytes;
    }
    return extra;
}

std::size_t ExtraSizeAfter(const ChunkExtra& extra, const ExtraUpdate& update) {
    if (update.replace.has_value()) {
        return update.replace->encoded_size();
    }
    std::size_t size = extra.encoded_size();
    for (const auto& change : update.changes) {
        if (const auto current = extra.Find(change.block_index); current.has_value()) {
            size -= kExtraEntryHeaderBytes + current->bytes.size();
        }
        if (change.value.has_value()) {
            size += EntryBytes(*change.value);
        }
    }
    return size;
}

ExtraUndo ApplyExtraUpdate(ChunkExtra* extra, ExtraUpdate update) {
    ExtraUndo undo;
    if (update.replace.has_value()) {
        if (!update.changes.empty()) {
            throw std::logic_error("an extra-data update has both changes and a replacement");
        }
        undo.replaced = true;
        undo.previous = std::exchange(*extra, std::move(*update.replace));
        return undo;
    }
    // Everything that can fail or allocate happens before the first change.
    undo.blocks.reserve(update.changes.size());
    for (std::size_t i = 0; i < update.changes.size(); ++i) {
        const auto& change = update.changes[i];
        if (i > 0 && change.block_index <= update.changes[i - 1].block_index) {
            throw std::logic_error("extra-data changes are not in strictly ascending block order");
        }
        // Every change is real, so the WAL records it produces describe
        // exactly what changed.
        const auto current = extra->Find(change.block_index);
        if (change.value.has_value() ? current.has_value() && *current == *change.value
                                     : !current.has_value()) {
            throw std::logic_error("an extra-data change does not change anything");
        }
        if (change.value.has_value()) {
            RequireWellFormed(*change.value);
        }
        undo.blocks.push_back(change.block_index);
    }
    if (update.changes.size() != 1U) {
        // One pass over the chunk's values, however many change.
        undo.previous = std::exchange(*extra, ChunkExtra::Merge(*extra, update.changes));
        return undo;
    }
    // One value changes in place; the undo keeps just its previous value,
    // and the room to put it back without allocating.
    const auto& change = update.changes.front();
    const auto current = extra->Find(change.block_index);
    undo.previous_values.push_back(ExtraChange{
        .block_index = change.block_index,
        .value = current.has_value() ? std::optional<ExtraValue>(current->ToValue()) : std::nullopt,
    });
    const std::size_t room = std::max(
        change.value.has_value() ? EntryBytes(*change.value) : 0U,
        current.has_value() ? kExtraEntryHeaderBytes + current->bytes.size() : 0U);
    extra->reserve(1U, room);
    if (change.value.has_value()) {
        extra->Assign(change.block_index, *change.value);
    } else {
        (void)extra->Remove(change.block_index);
    }
    return undo;
}

void UndoExtraUpdate(ChunkExtra* extra, ExtraUndo undo) noexcept {
    if (undo.previous.has_value()) {
        *extra = std::move(*undo.previous);
        return;
    }
    for (auto& change : undo.previous_values) {
        if (change.value.has_value()) {
            extra->Assign(change.block_index, *change.value);
        } else {
            (void)extra->Remove(change.block_index);
        }
    }
}

}  // namespace chunkdb
