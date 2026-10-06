#pragma once

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <optional>
#include <span>
#include <vector>

namespace chunkdb {

// Per-block extra data (docs/EXTRA_DATA.md): any present block of a table
// that enables it may carry one opaque value of 1 or more bits. Values are
// stored per chunk, in the chunk's EXTRA section.

// Encoded size of one EXTRA entry before its value bytes: block_index u32,
// bit_length u32.
inline constexpr std::size_t kExtraEntryHeaderBytes = 8U;
// Largest extra_max_chunk_bytes a table may set, and the most extra data
// (encoded) any chunk can hold.
inline constexpr std::size_t kExtraMaxChunkBytesLimit = 16U * 1024U * 1024U;
inline constexpr std::size_t kDefaultExtraMaxChunkBytes = 64U * 1024U;
// Largest value one block can hold in a chunk of kExtraMaxChunkBytesLimit.
inline constexpr std::uint32_t kExtraMaxBlockBitsLimit =
    static_cast<std::uint32_t>((kExtraMaxChunkBytesLimit - kExtraEntryHeaderBytes) * 8U);

// Bytes that hold `bit_length` bits.
[[nodiscard]] constexpr std::size_t ExtraValueBytes(std::uint32_t bit_length) noexcept {
    return (static_cast<std::size_t>(bit_length) + 7U) / 8U;
}

// Throws std::invalid_argument unless the limits are usable: max_block_bits
// in [1, kExtraMaxBlockBitsLimit], max_chunk_bytes in
// [kExtraEntryHeaderBytes + 1, kExtraMaxChunkBytesLimit], and one value of
// max_block_bits fits in max_chunk_bytes.
void RequireValidExtraLimits(std::uint32_t max_block_bits, std::size_t max_chunk_bytes);

// One block's value. Bit i is bit (i % 8) of byte (i / 8), least significant
// first, as in chunk payloads; the bits of the last byte past bit_length are
// zero.
struct ExtraValue {
    std::uint32_t bit_length = 0;
    std::vector<std::uint8_t> bytes{};

    friend bool operator==(const ExtraValue&, const ExtraValue&) = default;
};

// A value held by a ChunkExtra; valid until that ChunkExtra changes.
struct ExtraValueView {
    std::uint32_t bit_length = 0;
    std::span<const std::uint8_t> bytes{};

    [[nodiscard]] ExtraValue ToValue() const {
        return ExtraValue{.bit_length = bit_length, .bytes = {bytes.begin(), bytes.end()}};
    }
    friend bool operator==(const ExtraValueView& lhs, const ExtraValueView& rhs) noexcept;
    friend bool operator==(const ExtraValueView& lhs, const ExtraValue& rhs) noexcept;
};

// How a decoder treats set bits past bit_length in a value's last byte.
enum class ExtraPadding {
    // Stored data: they mean damage.
    kReject,
    // Client input: they are ignored and stored as zero.
    kClear,
};

// A value from `bit_length` bits held in `bytes` (ExtraValueBytes of them).
// Throws std::invalid_argument for a zero bit_length, a wrong byte count, or
// (kReject) set padding bits.
[[nodiscard]] ExtraValue MakeExtraValue(
    std::uint32_t bit_length,
    std::vector<std::uint8_t> bytes,
    ExtraPadding padding);

// A change to one block's extra data.
struct ExtraChange {
    std::uint32_t block_index = 0;
    // The value afterwards; std::nullopt removes it.
    std::optional<ExtraValue> value{};
};

// The extra data of one chunk: values by local block index
// (Geometry::LocalBlockIndex), sorted, at most one per block. Held as the
// encoded EXTRA section itself plus one u32 offset per entry, so its memory
// is its encoded size plus 4 bytes per value.
class ChunkExtra {
  public:
    struct Entry {
        std::uint32_t block_index = 0;
        ExtraValueView value{};
    };

    class Iterator {
      public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = Entry;
        using difference_type = std::ptrdiff_t;
        using pointer = void;
        using reference = Entry;

        Iterator() = default;
        Iterator(const ChunkExtra* extra, std::size_t index) : extra_(extra), index_(index) {}
        [[nodiscard]] Entry operator*() const { return extra_->entry(index_); }
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
        const ChunkExtra* extra_ = nullptr;
        std::size_t index_ = 0;
    };

    // The entries in ascending block order: `for (const auto entry : extra)`.
    [[nodiscard]] Iterator begin() const noexcept { return Iterator(this, 0); }
    [[nodiscard]] Iterator end() const noexcept { return Iterator(this, offsets_.size()); }
    [[nodiscard]] Entry entry(std::size_t index) const;

    [[nodiscard]] std::optional<ExtraValueView> Find(std::uint32_t block_index) const noexcept;
    // Sets the value of a block. Throws std::invalid_argument for a value
    // MakeExtraValue would reject (kReject). Does not allocate when reserve()
    // made room; on any failure the value is unchanged.
    void Assign(std::uint32_t block_index, const ExtraValue& value);
    // Removes the value of a block; false when it had none. Never allocates.
    bool Remove(std::uint32_t block_index) noexcept;
    // Assign and Remove that return the previous value.
    std::optional<ExtraValue> Put(std::uint32_t block_index, const ExtraValue& value);
    std::optional<ExtraValue> Erase(std::uint32_t block_index);
    // Room for `entries` more values of `bytes` more encoded bytes in all.
    void reserve(std::size_t entries, std::size_t bytes);
    // `base` with `changes` applied, built in one pass (linear in both):
    // a value sets the block's value, std::nullopt removes it if present.
    // Throws std::logic_error unless the changes are in strictly ascending
    // block order, std::invalid_argument for a malformed value.
    [[nodiscard]] static ChunkExtra Merge(const ChunkExtra& base, const std::vector<ExtraChange>& changes);

    [[nodiscard]] bool empty() const noexcept { return offsets_.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return offsets_.size(); }
    // Size of the encoded section: kExtraEntryHeaderBytes plus the value
    // bytes for every entry. Table limits are checked against it.
    [[nodiscard]] std::size_t encoded_size() const noexcept { return data_.size(); }

    // The EXTRA section: for each entry in ascending block order,
    // block_index u32le, bit_length u32le, then the value bytes.
    [[nodiscard]] const std::vector<std::uint8_t>& Encode() const noexcept { return data_; }
    void EncodeTo(std::vector<std::uint8_t>* out) const;
    // Parses an EXTRA section of a chunk with `block_count` blocks. Throws
    // std::invalid_argument naming the defect: an entry out of the section,
    // block indexes not strictly ascending or out of range, a zero bit
    // length, padding (per `padding`), or more than kExtraMaxChunkBytesLimit
    // bytes.
    [[nodiscard]] static ChunkExtra Decode(
        const std::uint8_t* data,
        std::size_t size,
        std::size_t block_count,
        ExtraPadding padding);

    friend bool operator==(const ChunkExtra& lhs, const ChunkExtra& rhs) noexcept {
        return lhs.data_ == rhs.data_;
    }

  private:
    // Index into offsets_ of the first entry whose block is not below
    // `block_index`.
    [[nodiscard]] std::size_t LowerBound(std::uint32_t block_index) const noexcept;
    [[nodiscard]] std::uint32_t BlockAt(std::uint32_t offset) const noexcept;
    [[nodiscard]] std::size_t EntryBytesAt(std::uint32_t offset) const noexcept;

    std::vector<std::uint8_t> data_{};
    std::vector<std::uint32_t> offsets_{};
};

// The change one mutation makes to a chunk's extra data: per-block changes
// in strictly ascending block order, or (with nothing in `changes`) a
// replacement of all of it.
struct ExtraUpdate {
    std::vector<ExtraChange> changes{};
    std::optional<ChunkExtra> replace{};

    [[nodiscard]] bool empty() const noexcept { return changes.empty() && !replace.has_value(); }
};

// What ApplyExtraUpdate changed, and how to take it back.
struct ExtraUndo {
    // The update replaced all of the extra data.
    bool replaced = false;
    // The blocks a per-block update changed, ascending.
    std::vector<std::uint32_t> blocks{};
    // The previous extra data as a whole, or (a one-block update) the
    // previous value of that block.
    std::optional<ChunkExtra> previous{};
    std::vector<ExtraChange> previous_values{};

    [[nodiscard]] bool empty() const noexcept { return !replaced && blocks.empty(); }
};

// Encoded size of `extra` after `update`.
[[nodiscard]] std::size_t ExtraSizeAfter(const ChunkExtra& extra, const ExtraUpdate& update);
// Applies `update` to `extra`. Throws, before changing anything,
// std::logic_error for changes out of order, a change that changes nothing,
// or changes with a replacement; std::invalid_argument for a malformed
// value; std::bad_alloc.
[[nodiscard]] ExtraUndo ApplyExtraUpdate(ChunkExtra* extra, ExtraUpdate update);
// Reverts ApplyExtraUpdate. Does not allocate.
void UndoExtraUpdate(ChunkExtra* extra, ExtraUndo undo) noexcept;

}  // namespace chunkdb
