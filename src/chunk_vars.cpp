#include "chunkdb/chunk_vars.hpp"

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

void WriteU32At(std::uint8_t* at, std::uint32_t value) noexcept {
    for (unsigned i = 0; i < 4U; ++i) {
        at[i] = static_cast<std::uint8_t>((value >> (8U * i)) & 0xFFU);
    }
}

void WriteHeader(std::uint8_t* at, VarKey key, std::size_t value_bytes) noexcept {
    WriteU32At(at, key.column_id);
    WriteU32At(at + 4U, key.block_index);
    WriteU32At(at + 8U, static_cast<std::uint32_t>(value_bytes));
}

[[nodiscard]] std::string KeyText(VarKey key) {
    return "column " + std::to_string(key.column_id) + " block " + std::to_string(key.block_index);
}

}  // namespace

void RequireValidVarLimit(std::size_t max_chunk_bytes) {
    if (max_chunk_bytes <= kVarEntryHeaderBytes || max_chunk_bytes > kVarMaxChunkBytesLimit) {
        throw std::invalid_argument(
            "var_max_chunk_bytes must be between " + std::to_string(kVarEntryHeaderBytes + 1U) + " and " +
            std::to_string(kVarMaxChunkBytesLimit));
    }
}

VarKey ChunkVars::KeyAt(std::uint32_t offset) const noexcept {
    return VarKey{.column_id = ReadU32(data_.data() + offset), .block_index = ReadU32(data_.data() + offset + 4U)};
}

std::size_t ChunkVars::EntryBytesAt(std::uint32_t offset) const noexcept {
    return kVarEntryHeaderBytes + ReadU32(data_.data() + offset + 8U);
}

std::size_t ChunkVars::LowerBound(VarKey key) const noexcept {
    const auto it = std::lower_bound(
        offsets_.begin(), offsets_.end(), key,
        [this](std::uint32_t offset, VarKey wanted) { return KeyAt(offset) < wanted; });
    return static_cast<std::size_t>(it - offsets_.begin());
}

ChunkVars::Entry ChunkVars::entry(std::size_t index) const {
    const std::uint32_t offset = offsets_.at(index);
    return Entry{
        .key = KeyAt(offset),
        .value = std::span<const std::uint8_t>(
            data_.data() + offset + kVarEntryHeaderBytes, ReadU32(data_.data() + offset + 8U)),
    };
}

std::optional<std::span<const std::uint8_t>> ChunkVars::Find(VarKey key) const noexcept {
    const std::size_t at = LowerBound(key);
    if (at == offsets_.size() || KeyAt(offsets_[at]) != key) {
        return std::nullopt;
    }
    const std::uint32_t offset = offsets_[at];
    return std::span<const std::uint8_t>(
        data_.data() + offset + kVarEntryHeaderBytes, ReadU32(data_.data() + offset + 8U));
}

void ChunkVars::Assign(VarKey key, std::span<const std::uint8_t> value) {
    const std::size_t new_bytes = kVarEntryHeaderBytes + value.size();
    const std::size_t at = LowerBound(key);
    const bool exists = at < offsets_.size() && KeyAt(offsets_[at]) == key;
    const std::size_t old_bytes = exists ? EntryBytesAt(offsets_[at]) : 0U;
    const std::size_t offset = at < offsets_.size() ? offsets_[at] : data_.size();
    if (data_.size() - old_bytes + new_bytes > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("variable-length values of one chunk exceed 4 GiB");
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
    WriteHeader(out, key, value.size());
    std::copy(value.begin(), value.end(), out + kVarEntryHeaderBytes);
    const auto shift = static_cast<std::int64_t>(new_bytes) - static_cast<std::int64_t>(old_bytes);
    for (std::size_t i = at + 1U; i < offsets_.size(); ++i) {
        offsets_[i] = static_cast<std::uint32_t>(static_cast<std::int64_t>(offsets_[i]) + shift);
    }
}

bool ChunkVars::Remove(VarKey key) noexcept {
    const std::size_t at = LowerBound(key);
    if (at == offsets_.size() || KeyAt(offsets_[at]) != key) {
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

void ChunkVars::reserve(std::size_t entries, std::size_t bytes) {
    // Geometric growth, so a chunk filled one value at a time does not copy
    // all of its values on every write.
    const auto grow = [](auto& buffer, std::size_t needed) {
        if (needed > buffer.capacity()) {
            buffer.reserve(std::max(needed, 2U * buffer.capacity()));
        }
    };
    grow(offsets_, offsets_.size() + entries);
    grow(data_, data_.size() + bytes);
}

ChunkVars ChunkVars::Merge(const ChunkVars& base, const std::vector<VarChange>& changes) {
    std::size_t bytes = base.data_.size();
    for (std::size_t i = 0; i < changes.size(); ++i) {
        if (i > 0 && changes[i].key <= changes[i - 1].key) {
            throw std::logic_error("value changes are not in strictly ascending key order");
        }
        if (changes[i].value.has_value()) {
            bytes += kVarEntryHeaderBytes + changes[i].value->size();
        }
    }
    ChunkVars merged;
    merged.data_.reserve(bytes);
    merged.offsets_.reserve(base.offsets_.size() + changes.size());
    const auto append = [&merged](const std::uint8_t* entry, std::size_t size) {
        merged.offsets_.push_back(static_cast<std::uint32_t>(merged.data_.size()));
        merged.data_.insert(merged.data_.end(), entry, entry + size);
    };
    std::size_t next = 0;
    for (const auto& change : changes) {
        // Base entries before the changed key stay.
        while (next < base.offsets_.size() && base.KeyAt(base.offsets_[next]) < change.key) {
            const std::uint32_t offset = base.offsets_[next++];
            append(base.data_.data() + offset, base.EntryBytesAt(offset));
        }
        if (next < base.offsets_.size() && base.KeyAt(base.offsets_[next]) == change.key) {
            ++next;
        }
        if (change.value.has_value()) {
            std::uint8_t header[kVarEntryHeaderBytes];
            WriteHeader(header, change.key, change.value->size());
            merged.offsets_.push_back(static_cast<std::uint32_t>(merged.data_.size()));
            merged.data_.insert(merged.data_.end(), header, header + kVarEntryHeaderBytes);
            merged.data_.insert(merged.data_.end(), change.value->begin(), change.value->end());
        }
    }
    while (next < base.offsets_.size()) {
        const std::uint32_t offset = base.offsets_[next++];
        append(base.data_.data() + offset, base.EntryBytesAt(offset));
    }
    if (merged.data_.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("variable-length values of one chunk exceed 4 GiB");
    }
    return merged;
}

ChunkVars ChunkVars::Decode(const std::uint8_t* data, std::size_t size, std::size_t block_count) {
    if (size > kVarMaxChunkBytesLimit) {
        throw std::invalid_argument(
            "VARS section of " + std::to_string(size) + " bytes exceeds " + std::to_string(kVarMaxChunkBytesLimit));
    }
    ChunkVars vars;
    vars.data_.assign(data, data + size);
    std::size_t at = 0;
    while (at < size) {
        if (size - at < kVarEntryHeaderBytes) {
            throw std::invalid_argument("VARS entry header extends past the section");
        }
        const VarKey key{.column_id = ReadU32(data + at), .block_index = ReadU32(data + at + 4U)};
        const std::uint32_t value_bytes = ReadU32(data + at + 8U);
        if (key.block_index >= block_count) {
            throw std::invalid_argument(
                "VARS entry for " + KeyText(key) + ", the chunk has " + std::to_string(block_count) + " blocks");
        }
        if (!vars.offsets_.empty() && key <= vars.KeyAt(vars.offsets_.back())) {
            throw std::invalid_argument(
                "VARS keys are not strictly ascending (" + KeyText(key) + " after " +
                KeyText(vars.KeyAt(vars.offsets_.back())) + ")");
        }
        if (size - at - kVarEntryHeaderBytes < value_bytes) {
            throw std::invalid_argument("VARS value of " + KeyText(key) + " extends past the section");
        }
        vars.offsets_.push_back(static_cast<std::uint32_t>(at));
        at += kVarEntryHeaderBytes + value_bytes;
    }
    return vars;
}

std::size_t VarsSizeAfter(const ChunkVars& vars, const VarUpdate& update) {
    if (update.replace.has_value()) {
        return update.replace->encoded_size();
    }
    std::size_t size = vars.encoded_size();
    for (const auto& change : update.changes) {
        if (const auto current = vars.Find(change.key); current.has_value()) {
            size -= kVarEntryHeaderBytes + current->size();
        }
        if (change.value.has_value()) {
            size += kVarEntryHeaderBytes + change.value->size();
        }
    }
    return size;
}

VarUndo ApplyVarUpdate(ChunkVars* vars, VarUpdate update) {
    VarUndo undo;
    if (update.replace.has_value()) {
        if (!update.changes.empty()) {
            throw std::logic_error("a value update has both changes and a replacement");
        }
        undo.replaced = true;
        undo.previous = std::exchange(*vars, std::move(*update.replace));
        return undo;
    }
    // Everything that can fail or allocate happens before the first change.
    undo.keys.reserve(update.changes.size());
    for (std::size_t i = 0; i < update.changes.size(); ++i) {
        const auto& change = update.changes[i];
        if (i > 0 && change.key <= update.changes[i - 1].key) {
            throw std::logic_error("value changes are not in strictly ascending key order");
        }
        // Every change is real, so the WAL records it produces describe
        // exactly what changed.
        const auto current = vars->Find(change.key);
        const bool same = change.value.has_value()
                              ? current.has_value() && std::equal(current->begin(), current->end(),
                                                                  change.value->begin(), change.value->end())
                              : !current.has_value();
        if (same) {
            throw std::logic_error("a value change does not change anything");
        }
        undo.keys.push_back(change.key);
    }
    if (update.changes.size() != 1U) {
        // One pass over the chunk's values, however many change.
        undo.previous = std::exchange(*vars, ChunkVars::Merge(*vars, update.changes));
        return undo;
    }
    // One value changes in place; the undo keeps just its previous value,
    // and the room to put it back without allocating.
    const auto& change = update.changes.front();
    const auto current = vars->Find(change.key);
    undo.previous_values.push_back(VarChange{
        .key = change.key,
        .value = current.has_value() ? std::optional<std::vector<std::uint8_t>>(
                                           std::vector<std::uint8_t>(current->begin(), current->end()))
                                     : std::nullopt,
    });
    const std::size_t room = std::max(
        change.value.has_value() ? kVarEntryHeaderBytes + change.value->size() : 0U,
        current.has_value() ? kVarEntryHeaderBytes + current->size() : 0U);
    vars->reserve(1U, room);
    if (change.value.has_value()) {
        vars->Assign(change.key, *change.value);
    } else {
        (void)vars->Remove(change.key);
    }
    return undo;
}

void UndoVarUpdate(ChunkVars* vars, VarUndo undo) noexcept {
    if (undo.previous.has_value()) {
        *vars = std::move(*undo.previous);
        return;
    }
    for (auto& change : undo.previous_values) {
        if (change.value.has_value()) {
            vars->Assign(change.key, *change.value);
        } else {
            (void)vars->Remove(change.key);
        }
    }
}

}  // namespace chunkdb
