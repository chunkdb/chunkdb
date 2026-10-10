#pragma once

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <optional>
#include <span>
#include <vector>

namespace chunkdb {

// The values of a chunk's variable-length columns (text and bytes;
// docs/design/COLUMNS_DESIGN.md), stored per chunk in its VARS section. A block
// without a value for a column has no entry.

// Encoded size of one VARS entry before its value bytes: column_id u32,
// block_index u32, byte_length u32.
inline constexpr std::size_t kVarEntryHeaderBytes = 12U;
// Largest var_max_chunk_bytes a table may set, and the most a VARS section
// can hold.
inline constexpr std::size_t kVarMaxChunkBytesLimit = 64U * 1024U * 1024U;
inline constexpr std::size_t kDefaultVarMaxChunkBytes = 1024U * 1024U;

// Throws std::invalid_argument unless max_chunk_bytes is in
// [kVarEntryHeaderBytes + 1, kVarMaxChunkBytesLimit].
void RequireValidVarLimit(std::size_t max_chunk_bytes);

// Where a value lives: entries are ordered by column, then block.
struct VarKey {
    std::uint32_t column_id = 0;
    std::uint32_t block_index = 0;

    [[nodiscard]] std::uint64_t packed() const noexcept {
        return (static_cast<std::uint64_t>(column_id) << 32U) | block_index;
    }
    friend bool operator==(const VarKey&, const VarKey&) = default;
    friend bool operator<(const VarKey& lhs, const VarKey& rhs) noexcept { return lhs.packed() < rhs.packed(); }
    friend bool operator<=(const VarKey& lhs, const VarKey& rhs) noexcept { return lhs.packed() <= rhs.packed(); }
};

// A change to one value.
struct VarChange {
    VarKey key{};
    // The value afterwards; std::nullopt removes it.
    std::optional<std::vector<std::uint8_t>> value{};
};

// The values of one chunk, sorted by key, at most one per key. Held as the
// encoded VARS section itself plus one u32 offset per entry, so its memory
// is its encoded size plus 4 bytes per value.
class ChunkVars {
  public:
    struct Entry {
        VarKey key{};
        std::span<const std::uint8_t> value{};
    };

    class Iterator {
      public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = Entry;
        using difference_type = std::ptrdiff_t;
        using pointer = void;
        using reference = Entry;

        Iterator() = default;
        Iterator(const ChunkVars* vars, std::size_t index) : vars_(vars), index_(index) {}
        [[nodiscard]] Entry operator*() const { return vars_->entry(index_); }
        Iterator& operator++() {
            ++index_;
            return *this;
        }
        Iterator operator++(int) {
            Iterator copy = *this;
            ++index_;
            return copy;
        }
        friend bool operator==(const Iterator& lhs, const Iterator& rhs) noexcept {
            return lhs.index_ == rhs.index_;
        }

      private:
        const ChunkVars* vars_ = nullptr;
        std::size_t index_ = 0;
    };

    // The entries in ascending key order: `for (const auto entry : vars)`.
    [[nodiscard]] Iterator begin() const noexcept { return Iterator(this, 0); }
    [[nodiscard]] Iterator end() const noexcept { return Iterator(this, offsets_.size()); }
    [[nodiscard]] Entry entry(std::size_t index) const;

    // The value at `key`; valid until this ChunkVars changes.
    [[nodiscard]] std::optional<std::span<const std::uint8_t>> Find(VarKey key) const noexcept;
    // Sets a value. Does not allocate when reserve() made room; on any
    // failure the value is unchanged.
    void Assign(VarKey key, std::span<const std::uint8_t> value);
    // Removes a value; false when there was none. Never allocates.
    bool Remove(VarKey key) noexcept;
    // Room for `entries` more values of `bytes` more encoded bytes in all,
    // growing geometrically.
    void reserve(std::size_t entries, std::size_t bytes);
    // `base` with `changes` applied, built in one pass (linear in both).
    // Throws std::logic_error unless the changes are in strictly ascending
    // key order.
    [[nodiscard]] static ChunkVars Merge(const ChunkVars& base, const std::vector<VarChange>& changes);

    [[nodiscard]] bool empty() const noexcept { return offsets_.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return offsets_.size(); }
    // Size of the encoded section: kVarEntryHeaderBytes plus the value bytes
    // for every entry. Table limits are checked against it.
    [[nodiscard]] std::size_t encoded_size() const noexcept { return data_.size(); }

    // The VARS section: for each entry in ascending key order, column_id
    // u32le, block_index u32le, byte_length u32le, then the value bytes.
    [[nodiscard]] const std::vector<std::uint8_t>& Encode() const noexcept { return data_; }
    // Parses a VARS section of a chunk with `block_count` blocks. Throws
    // std::invalid_argument naming the defect: an entry out of the section,
    // keys not strictly ascending, a block index out of range, or more than
    // kVarMaxChunkBytesLimit bytes. Which columns and values the table
    // allows is the caller's to check.
    [[nodiscard]] static ChunkVars Decode(const std::uint8_t* data, std::size_t size, std::size_t block_count);

    friend bool operator==(const ChunkVars& lhs, const ChunkVars& rhs) noexcept {
        return lhs.data_ == rhs.data_;
    }

  private:
    // Index into offsets_ of the first entry whose key is not below `key`.
    [[nodiscard]] std::size_t LowerBound(VarKey key) const noexcept;
    [[nodiscard]] VarKey KeyAt(std::uint32_t offset) const noexcept;
    [[nodiscard]] std::size_t EntryBytesAt(std::uint32_t offset) const noexcept;

    std::vector<std::uint8_t> data_{};
    std::vector<std::uint32_t> offsets_{};
};

// The change one mutation makes to a chunk's values: changes in strictly
// ascending key order, or (with nothing in `changes`) a replacement of all
// of them.
struct VarUpdate {
    std::vector<VarChange> changes{};
    std::optional<ChunkVars> replace{};

    [[nodiscard]] bool empty() const noexcept { return changes.empty() && !replace.has_value(); }
};

// What ApplyVarUpdate changed, and how to take it back.
struct VarUndo {
    // The update replaced all of the values.
    bool replaced = false;
    // The keys a per-value update changed, ascending.
    std::vector<VarKey> keys{};
    // The previous values as a whole, or (a one-value update) the previous
    // value at that key.
    std::optional<ChunkVars> previous{};
    std::vector<VarChange> previous_values{};

    [[nodiscard]] bool empty() const noexcept { return !replaced && keys.empty(); }
};

// Encoded size of `vars` after `update`.
[[nodiscard]] std::size_t VarsSizeAfter(const ChunkVars& vars, const VarUpdate& update);
// Applies `update` to `vars`. Throws, before changing anything,
// std::logic_error for changes out of order, a change that changes nothing,
// or changes with a replacement; std::bad_alloc.
[[nodiscard]] VarUndo ApplyVarUpdate(ChunkVars* vars, VarUpdate update);
// Reverts ApplyVarUpdate. Does not allocate.
void UndoVarUpdate(ChunkVars* vars, VarUndo undo) noexcept;

}  // namespace chunkdb
