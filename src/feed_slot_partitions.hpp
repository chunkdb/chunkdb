#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "chunkdb/feed_slots.hpp"
#include "feed_slot_records.hpp"

// Partitioned slots (docs/design/SLOT_PARTITIONS.md).
namespace chunkdb {

// The partition of chunk `coord` in a slot of `count` partitions: SplitMix64
// of its coordinates, fixed in the storage format (STORAGE_FORMAT.md).
[[nodiscard]] std::uint32_t FeedSlotPartitionOf(const ChunkCoord& coord, std::uint32_t count) noexcept;

// 1 for a slot without partitions.
[[nodiscard]] inline std::uint32_t FeedSlotPartitionCount(const std::vector<FeedSlotPartition>& partitions) noexcept {
    return partitions.empty() ? 1U : static_cast<std::uint32_t>(partitions.size());
}

// The partitions a watch names, sorted; empty when they are every partition.
// Throws std::invalid_argument for a repeated one or one at or above `count`.
[[nodiscard]] std::vector<std::uint16_t> NormalizeFeedSlotPartitions(std::vector<std::uint16_t> partitions, std::uint32_t count);

// Whether two watches' partitions (empty: every partition) share one.
[[nodiscard]] bool FeedSlotPartitionsOverlap(const std::vector<std::uint16_t>& first, const std::vector<std::uint16_t>& second) noexcept;

// `slot` as a watch holding `held` (empty: every partition) sees it: its
// position is the lowest of those partitions, its resync any of theirs.
[[nodiscard]] FeedSlot FeedSlotPartitionView(FeedSlot slot, const std::vector<std::uint16_t>& held);

// Per partition, the revision through which a watch holding `held` already
// has the partition's changes: its position when held, the maximum
// otherwise. Empty for a slot without partitions.
[[nodiscard]] std::vector<std::uint64_t> FeedSlotPartitionFloors(const FeedSlot& slot, const std::vector<std::uint16_t>& held);

// Keeps of a change or chunks entry the blocks, chunk changes and chunk forms
// of the chunks whose partition floor is below the entry's revision; nullptr
// when none is kept. Other entries pass unchanged.
[[nodiscard]] std::shared_ptr<const FeedEntry> PartitionFeedEntry(const std::shared_ptr<const FeedEntry>& entry,
    const std::vector<std::uint64_t>& floors);

// Record updates that keep `written` the lowest partition position and
// `resync` set exactly when a partition has it.
void SetFeedSlotRecordPosition(FeedSlotRecord& slot, std::uint64_t revision);
void LowerFeedSlotRecordPositions(FeedSlotRecord& slot, std::uint64_t ceiling);
void MarkFeedSlotRecordResync(FeedSlotRecord& slot);
void MarkFeedSlotRecordLost(FeedSlotRecord& slot);
// Raises the held partitions (empty: all) to `revision` where it is higher,
// with `clear_resync` also clearing their resync; true when something changed.
bool AdvanceFeedSlotRecord(FeedSlotRecord& slot, const std::vector<std::uint16_t>& held, std::uint64_t revision,
    bool clear_resync = true);

}  // namespace chunkdb
