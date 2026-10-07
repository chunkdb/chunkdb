#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "chunk_store_internal.hpp"

// Block history segments (docs/STORAGE_FORMAT.md Section 9): the encoding of
// block changes, records and segment headers.
namespace chunkdb::history {

// What a change does to a present block's extra data.
enum class ExtraChangeKind : std::uint8_t {
    kUnchanged = 0,
    kSet = 1,
    kRemoved = 2,
};

// One block's state after a change: present with `bits` (block_bits bits,
// least significant first, padding zero) and its extra data changed as
// `extra_change` says (to `extra` for kSet), or absent with no bits and no
// extra data.
struct BlockChange {
    std::uint32_t block_index = 0;
    bool present = false;
    std::vector<std::uint8_t> bits{};
    ExtraChangeKind extra_change = ExtraChangeKind::kUnchanged;
    ExtraValue extra{};

    friend bool operator==(const BlockChange&, const BlockChange&) = default;
};

// The changes one mutation made, in ascending block index; only blocks whose
// state (presence, bits or extra data) differs.
struct Mutation {
    std::uint64_t revision = 0;
    std::uint64_t time_ms = 0;
    std::vector<std::uint8_t> tag{};
    std::vector<BlockChange> changes{};

    friend bool operator==(const Mutation&, const Mutation&) = default;
};

// A chunk state: payload then presence bytes (ChunkStateBytes long) and the
// extra data, every entry on a present block.
struct ChunkState {
    std::vector<std::uint8_t> state{};
    ChunkExtra extra{};

    friend bool operator==(const ChunkState&, const ChunkState&) = default;
};

[[nodiscard]] ChunkState EmptyChunkState(const Geometry& geometry);

// Finds what a step changed among some blocks of a chunk state (packed
// payload then presence, and its extra data): Capture the blocks before the
// step, Changes after it.
class BlockDiffer {
  public:
    explicit BlockDiffer(const Geometry& geometry);

    // `blocks` ascending and unique, each inside the chunk.
    void Capture(
        const std::vector<std::uint8_t>& state,
        const ChunkExtra& extra,
        const std::vector<std::uint32_t>& blocks);
    // The captured blocks whose state differs now, ascending.
    [[nodiscard]] std::vector<BlockChange> Changes(
        const std::vector<std::uint8_t>& state,
        const ChunkExtra& extra) const;

  private:
    const Geometry& geometry_;
    std::size_t value_bytes_ = 0;
    std::vector<std::uint32_t> blocks_;
    // Per captured block: its bits, presence and extra data.
    std::vector<std::uint8_t> bits_;
    std::vector<bool> present_;
    std::vector<std::optional<ExtraValue>> extra_;
};

// The blocks whose state differs between two states of a chunk, restricted to
// `candidates` when given (ascending, unique), else all blocks.
[[nodiscard]] std::vector<BlockChange> DiffBlocks(
    const Geometry& geometry,
    const ChunkState& before,
    const ChunkState& after,
    const std::vector<std::uint32_t>* candidates);

// Applies the changes of `mutation` to `state`. Throws std::runtime_error
// for a change that does not fit it (removing extra data a block does not
// have); `state` may then be partly changed. With `reject_unchanged`, a change
// that leaves its block as it was is such a misfit too.
void ApplyMutation(
    const Geometry& geometry,
    const Mutation& mutation,
    ChunkState* state,
    bool reject_unchanged = false);

// Blocks whose bytes a write at state bytes [offset, offset + size) can
// change (state = payload then presence), ascending.
[[nodiscard]] std::vector<std::uint32_t> BlocksTouchedBySpan(
    const Geometry& geometry,
    std::size_t offset,
    std::size_t size);

// Records: one or more per checkpoint append, holding consecutive mutations.
//   magic "HREC", first_revision u64, last_revision u64, first_time u64,
//   last_time u64, mutation_count u32, event_count u32, block_mask[32],
//   body_size u32, header_crc u32, body, body_crc u32
// Body, per mutation: varint revision delta (from the previous mutation, the
// first from first_revision), varint time delta, varint (change_count << 3 |
// extra << 2 | tagged << 1 | bitmap_form), for a tagged mutation varint tag
// length (1 to kMaxTagBytes) and the tag, then either
//   list form:   per change, varint (block_index << 1 | present), or with
//                the extra flag (block_index << 3 | extra_kind << 1 |
//                present)
//   bitmap form: a changed-block bitmap, a present bitmap over the changed
//                blocks and, with the extra flag, their extra kinds (2 bits
//                each)
// whichever is smaller, then the packed bits of present blocks, then for
// each change of kind kSet varint bit_length and the value bytes. The extra
// flag is set exactly when a change has another kind than kUnchanged; an
// absent block's kind is kUnchanged. block_mask has bit (block_index * 256 /
// block_count) set for every block a mutation in the record changes.
inline constexpr std::size_t kRecordHeaderSize = 4U + 8U * 4U + 4U + 4U + 32U + 4U + 4U;
inline constexpr std::size_t kBlockMaskBits = 256U;
// Hard bounds a decoder enforces before it allocates. One mutation of the
// largest chunk with the most extra data fits a record.
inline constexpr std::size_t kMaxTagBytes = 255U;
inline constexpr std::size_t kMaxRecordBodyBytes = 128U * 1024U * 1024U;
// An encoder starts another record once a body passes this many bytes.
inline constexpr std::size_t kTargetRecordBodyBytes = 1024U * 1024U;

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
[[nodiscard]] bool MaskHasBlock(
    const Geometry& geometry,
    const BlockMask& mask,
    std::uint32_t block_index) noexcept;

// Appends one record of `mutations` (at least one, strictly increasing
// revisions, non-decreasing times, changes in ascending block order, values of
// the geometry's block_bits, valid extra values, tags of at most
// kMaxTagBytes) to `out`. Throws std::invalid_argument when they are not, or
// when the body would pass kMaxRecordBodyBytes.
void EncodeRecord(
    const Geometry& geometry,
    std::span<const Mutation> mutations,
    std::vector<std::uint8_t>* out);

// EncodeRecord of `mutations` split into records of about
// kTargetRecordBodyBytes each.
void EncodeRecords(
    const Geometry& geometry,
    std::span<const Mutation> mutations,
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
//   magic "CHKHSEG1", version u16 = 1, flags u16 (bit 0 keyframe, bit 1
//   cut, bit 2 first), store_id[16], chunk_x i64, chunk_y i64, history_start u64,
//   base_revision u64, base_time u64, keyframe_stored_size u32,
//   keyframe_crc u32, header_crc u32, then the keyframe when flag bit 0 is
//   set: the zrle-encoded chunk state (payload then presence) followed by
//   its EXTRA section, the CRC over those raw bytes.
inline constexpr std::size_t kSegmentHeaderSize = 8U + 2U + 2U + 16U + 8U * 5U + 4U + 4U + 4U;

struct SegmentHeader {
    StoreId store_id{};
    ChunkCoord chunk{};
    std::uint64_t history_start = 0;
    // The segment's records start from the chunk at this revision (0: the
    // empty chunk). A keyframe holds that state; without one it is where the
    // previous segment ends.
    std::uint64_t base_revision = 0;
    std::uint64_t base_time_ms = 0;
    std::optional<ChunkState> keyframe{};
    // Written by a trim: the segments of the chunk before this one are
    // garbage. Only on a segment with a keyframe.
    bool cut = false;
    // The chunk's first segment: its base is where the chunk's history
    // starts, the empty chunk when it has no keyframe.
    bool first = false;
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
