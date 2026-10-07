#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "chunk_store_internal.hpp"

// Block history segments (docs/HISTORY_DESIGN.md): the encoding of block
// changes, without any engine wiring yet.
namespace chunkdb::history {

// One block's state after a change: present with `bits` (block_bits bits,
// least significant first, padding zero), or absent with no bits.
struct BlockChange {
    std::uint32_t block_index = 0;
    bool present = false;
    std::vector<std::uint8_t> bits{};

    friend bool operator==(const BlockChange&, const BlockChange&) = default;
};

// The changes one mutation made, in ascending block index; only blocks whose
// state differs.
struct Mutation {
    std::uint64_t revision = 0;
    std::uint64_t time_ms = 0;
    std::vector<std::uint8_t> tag{};
    std::vector<BlockChange> changes{};

    friend bool operator==(const Mutation&, const Mutation&) = default;
};

// The blocks whose state differs between two states of a chunk (payload and
// presence as ChunkStore holds them), restricted to `candidates` when given
// (ascending, unique), else all blocks.
[[nodiscard]] std::vector<BlockChange> DiffBlocks(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& payload_before,
    const std::vector<std::uint8_t>& presence_before,
    const std::vector<std::uint8_t>& payload_after,
    const std::vector<std::uint8_t>& presence_after,
    const std::vector<std::uint32_t>* candidates);

// Blocks whose bytes a write at state bytes [offset, offset + size) can
// change (state = payload then presence), ascending.
[[nodiscard]] std::vector<std::uint32_t> BlocksTouchedBySpan(
    const Geometry& geometry,
    std::size_t offset,
    std::size_t size);

// Records: one per checkpoint append, holding consecutive mutations.
//   magic "HREC", first_revision u64, last_revision u64, first_time u64,
//   last_time u64, mutation_count u32, event_count u32, block_mask[32],
//   body_size u32, header_crc u32, body, body_crc u32
// Body, per mutation: varint revision delta (from the previous mutation, the
// first from first_revision), varint time delta, varint (change_count << 2 |
// tagged << 1 | bitmap_form), for a tagged mutation varint tag length (1 to
// kMaxTagBytes) and the tag, then either
//   list form:   per change, varint (block_index << 1 | present), then for
//                present blocks their bits, all values packed together
//   bitmap form: a changed-block bitmap and a present bitmap over the
//                changed blocks, then the packed values of present ones
// whichever is smaller. block_mask has bit (block_index * 256 / block_count)
// set for every block a mutation in the record changes.
inline constexpr std::size_t kRecordHeaderSize = 4U + 8U * 4U + 4U + 4U + 32U + 4U + 4U;
inline constexpr std::size_t kBlockMaskBits = 256U;
// Hard bounds a decoder enforces before it allocates.
inline constexpr std::size_t kMaxTagBytes = 255U;
inline constexpr std::size_t kMaxRecordBodyBytes = 64U * 1024U * 1024U;

using BlockMask = std::array<std::uint8_t, kBlockMaskBits / 8U>;

struct RecordSummary {
    std::uint64_t first_revision = 0;
    std::uint64_t last_revision = 0;
    std::uint64_t first_time_ms = 0;
    std::uint64_t last_time_ms = 0;
    std::uint32_t mutation_count = 0;
    std::uint32_t event_count = 0;
    BlockMask block_mask{};
    // Bytes of the whole record.
    std::size_t size = 0;
};

[[nodiscard]] std::size_t BlockMaskBit(const Geometry& geometry, std::uint32_t block_index) noexcept;

// Appends one record of `mutations` (at least one, strictly increasing
// revisions, non-decreasing times, changes in ascending block order, values of
// the geometry's block_bits, tags of at most kMaxTagBytes) to `out`. Throws
// std::invalid_argument when they are not.
void EncodeRecord(
    const Geometry& geometry,
    const std::vector<Mutation>& mutations,
    std::vector<std::uint8_t>* out);

enum class RecordStatus {
    kOk,
    // Fewer bytes than the record needs, from a header that is valid so far
    // or no header at all: what an interrupted append leaves at the end.
    kTruncated,
    // Anything else: a checksum, a field, or the body breaks a rule.
    kDamaged,
};

struct RecordReadResult {
    RecordStatus status = RecordStatus::kDamaged;
    RecordSummary summary{};
    std::vector<Mutation> mutations{};
    // Why the record is damaged; empty otherwise.
    const char* problem = "";
};

// Reads the record at data[0, size). `decode_body` false checks the header
// and the body checksum only (enough to skip by the block mask).
[[nodiscard]] RecordReadResult ReadRecord(
    const Geometry& geometry,
    const std::uint8_t* data,
    std::size_t size,
    bool decode_body);

// Segment file header:
//   magic "CHKHSEG1", version u16 = 1, flags u16 (bit 0 keyframe),
//   store_id[16], chunk_x i64, chunk_y i64, history_start u64,
//   base_revision u64, base_time u64, keyframe_stored_size u32,
//   keyframe_crc u32, header_crc u32, then the keyframe (the zrle-encoded
//   chunk state: payload then presence) when flag bit 0 is set.
struct SegmentHeader {
    StoreId store_id{};
    ChunkCoord chunk{};
    std::uint64_t history_start = 0;
    // The state the segment's records start from is the chunk at this
    // revision (0: absent); the keyframe holds it when present.
    std::uint64_t base_revision = 0;
    std::uint64_t base_time_ms = 0;
    std::optional<std::vector<std::uint8_t>> keyframe_state{};
};

[[nodiscard]] std::vector<std::uint8_t> EncodeSegmentHeader(
    const Geometry& geometry,
    const SegmentHeader& header);

// Parses and validates a segment header for this store and chunk; returns
// its size in *header_size. Throws std::runtime_error naming the defect.
[[nodiscard]] SegmentHeader ReadSegmentHeader(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& bytes,
    const StoreId& store_id,
    const ChunkCoord& chunk,
    std::size_t* header_size);

}  // namespace chunkdb::history
