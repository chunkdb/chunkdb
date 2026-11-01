#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <istream>
#include <map>
#include <optional>
#include <ostream>
#include <span>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "crypto.hpp"

namespace chunkdb {

struct ChunkIndexHeader {
    StoreId table_id{};
    std::uint8_t dims = 2;
    LargeChunkCoord large_coord{};
};

enum class ChunkIndexKind : std::uint8_t { kLive = 1, kDeleted = 2, kForget = 3 };

struct ChunkIndexRecord {
    ChunkIndexKind kind = ChunkIndexKind::kLive;
    ChunkCoord coord{};
    std::uint64_t revision = 0;
    std::uint64_t commit_time_ms = 0;
    std::uint64_t schema_version = 0;
};

[[nodiscard]] std::vector<std::uint8_t> SerializeChunkIndexHeader(const ChunkIndexHeader& header);
[[nodiscard]] ChunkIndexHeader ParseChunkIndexHeader(
    std::span<const std::uint8_t> bytes, const Geometry& geometry,
    const StoreId& table_id, const LargeChunkCoord& large_coord);
[[nodiscard]] std::vector<std::uint8_t> SerializeChunkIndexRecord(
    const ChunkIndexRecord& record, const Geometry& geometry, const LargeChunkCoord& large_coord);
[[nodiscard]] ChunkIndexRecord ParseChunkIndexRecord(
    std::span<const std::uint8_t> bytes, const Geometry& geometry, const LargeChunkCoord& large_coord);

struct CheckedChunkIndexLog {
    // Includes forgotten decisions so later records cannot reuse an older identity.
    std::map<ChunkCoord, ChunkIndexRecord> latest;
    // Last durable empty/forgotten boundary, even after a newer live hint.
    std::map<ChunkCoord, ChunkIndexRecord> empty_boundaries;
    std::uint64_t valid_bytes = 0;
    bool interrupted_tail = false;
};

// Validate the entire stream before returning a candidate state to its owner.
// No file is changed here. A writer may trim only an interrupted final append
// at valid_bytes, after the remaining recovery structures have passed admission.
// Equal live/deleted boundaries still need committed-empty artifact reconciliation.
[[nodiscard]] CheckedChunkIndexLog ReadChunkIndexLog(
    std::istream& input, std::uint64_t file_bytes, const Geometry& geometry,
    const StoreId& table_id, const LargeChunkCoord& large_coord,
    std::optional<std::uint64_t> committed_bytes = std::nullopt);

struct IndexInventoryHeader {
    StoreId table_id{};
    std::uint8_t dims = 2;
    bool compaction_pending = false;
    std::uint64_t retained_deletion_floor = 0;
    std::uint64_t compaction_through = 0;
    std::uint64_t count = 0;
    std::uint64_t generation = 1U;
};

struct IndexInventoryEntry {
    LargeChunkCoord coord{};
    std::uint64_t committed_bytes = 0;
    // Nonzero only in a pending compaction; binds the complete staged image.
    std::uint64_t replacement_bytes = 0;
    crypto::Sha256Digest replacement_sha256{};
};

// Hash exact borrowed bytes with fixed storage; structure/context admission is
// separate. The stream starts at zero and must end at the measured extent.
[[nodiscard]] crypto::Sha256Digest IndexFileDigest(std::istream& input, std::uint64_t file_bytes);

// Count is a u64, with no fixed shard-count ceiling. Reject length overflow
// before reading/allocating coordinate entries.
[[nodiscard]] std::uint64_t IndexInventoryFileBytes(const IndexInventoryHeader& header);

// The independently checked fixed header is a cache identity only: reuse
// requires an already completely admitted matching epoch/generation inside
// the caller's stable snapshot bracket. It grants no new entry ownership.
[[nodiscard]] IndexInventoryHeader ReadIndexInventoryHeader(
    std::istream& input, std::uint64_t file_bytes, const Geometry& geometry, const StoreId& table_id);

// Validate the whole borrowed stream using fixed-size buffers, returning no
// coordinate list. The stream must start at its header. Callers retain their
// existing stable-generation/immutable-file ownership while rereading entries;
// no recovery mutation or candidate publication is authorized before success.
// The optional callback may populate only private candidate state; its entries
// remain untrusted until this function returns after checking the final CRC.
[[nodiscard]] IndexInventoryHeader ReadIndexInventory(
    std::istream& input, std::uint64_t file_bytes, const Geometry& geometry, const StoreId& table_id,
    const std::function<void(const IndexInventoryEntry&)>& candidate = {});

// Validate header/count and every coordinate before writing any stream bytes.
// Output publication/directory sync belongs to the caller, not this codec.
void WriteIndexInventory(
    std::ostream& output, const IndexInventoryHeader& header, std::span<const IndexInventoryEntry> entries);

struct TombstoneCompactionBounds {
    std::uint64_t through = 0;
    std::uint64_t retained_deletion_floor = 0;
};

// BEFORE 0 is a no-op. Persist this operation's exact inclusive through bound,
// independently of the cumulative floor; redo must use that persisted bound.
[[nodiscard]] std::optional<TombstoneCompactionBounds> ComputeTombstoneCompactionBounds(
    std::uint64_t before, std::uint64_t completed_frontier, std::uint64_t old_floor) noexcept;
[[nodiscard]] bool TombstoneVersionDiscarded(std::uint64_t revision, std::uint64_t through) noexcept;

}  // namespace chunkdb
