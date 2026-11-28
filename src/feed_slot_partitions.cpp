#include "feed_slot_partitions.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>

namespace chunkdb {
namespace {
constexpr std::uint64_t SplitMix64(std::uint64_t value) noexcept {
    value += UINT64_C(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30U)) * UINT64_C(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27U)) * UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31U);
}
bool Holds(const std::vector<std::uint16_t>& held, std::size_t partition) {
    return held.empty() || std::binary_search(held.begin(), held.end(), static_cast<std::uint16_t>(partition));
}
// The slot's position and resync from its partitions.
void Summarize(FeedSlotRecord& slot) {
    if (slot.partitions.empty()) return;
    slot.written = std::numeric_limits<std::uint64_t>::max();
    slot.resync = false;
    for (const auto& partition : slot.partitions) {
        slot.written = std::min(slot.written, partition.written);
        slot.resync = slot.resync || partition.resync;
    }
}
}  // namespace

std::uint32_t FeedSlotPartitionOf(const ChunkCoord& coord, std::uint32_t count) noexcept {
    if (count <= 1U) return 0U;
    auto hash = SplitMix64(static_cast<std::uint64_t>(coord.x));
    hash = SplitMix64(hash ^ static_cast<std::uint64_t>(coord.y));
    hash = SplitMix64(hash ^ static_cast<std::uint64_t>(coord.z));
    return static_cast<std::uint32_t>(hash % count);
}

std::vector<std::uint16_t> NormalizeFeedSlotPartitions(std::vector<std::uint16_t> partitions, std::uint32_t count) {
    std::sort(partitions.begin(), partitions.end());
    for (std::size_t i = 0U; i < partitions.size(); ++i) {
        if (partitions[i] >= count)
            throw std::invalid_argument("partition " + std::to_string(partitions[i]) + " is not below the slot's " +
                                        std::to_string(count) + " partitions");
        if (i != 0U && partitions[i - 1U] == partitions[i])
            throw std::invalid_argument("partition " + std::to_string(partitions[i]) + " is given twice");
    }
    if (partitions.size() == count) partitions.clear();
    return partitions;
}

bool FeedSlotPartitionsOverlap(const std::vector<std::uint16_t>& first, const std::vector<std::uint16_t>& second) noexcept {
    if (first.empty() || second.empty()) return true;
    auto a = first.begin();
    auto b = second.begin();
    while (a != first.end() && b != second.end()) {
        if (*a == *b) return true;
        if (*a < *b) ++a; else ++b;
    }
    return false;
}

FeedSlot FeedSlotPartitionView(FeedSlot slot, const std::vector<std::uint16_t>& held) {
    if (slot.partitions.empty() || held.empty()) return slot;
    auto lowest = std::numeric_limits<std::uint64_t>::max();
    bool resync = false;
    for (const auto partition : held) {
        if (partition >= slot.partitions.size()) throw std::logic_error("held partition is outside the slot");
        lowest = std::min(lowest, slot.partitions[partition].written);
        resync = resync || slot.partitions[partition].resync;
    }
    slot.position.revision = lowest;
    slot.resync = resync;
    return slot;
}

std::vector<std::uint64_t> FeedSlotPartitionFloors(const FeedSlot& slot, const std::vector<std::uint16_t>& held) {
    std::vector<std::uint64_t> floors;
    floors.reserve(slot.partitions.size());
    for (std::size_t i = 0U; i < slot.partitions.size(); ++i)
        floors.push_back(Holds(held, i) ? slot.partitions[i].written : std::numeric_limits<std::uint64_t>::max());
    return floors;
}

bool FeedSlotPartitionIncludes(std::span<const std::uint64_t> floors, const ChunkCoord& chunk,
    std::uint64_t revision) noexcept {
    return floors.empty() || floors[FeedSlotPartitionOf(chunk, static_cast<std::uint32_t>(floors.size()))] < revision;
}

std::shared_ptr<const FeedEntry> PartitionFeedEntry(const std::shared_ptr<const FeedEntry>& entry,
    const std::vector<std::uint64_t>& floors) {
    if (floors.empty() || !entry) return entry;
    if (entry->kind != FeedEntry::Kind::kChange && entry->kind != FeedEntry::Kind::kChunks) return entry;
    const auto revision = entry->position.revision;
    const auto keep = [&](const ChunkCoord& chunk) { return FeedSlotPartitionIncludes(floors, chunk, revision); };
    const bool all = std::all_of(entry->blocks.begin(), entry->blocks.end(), [&](const auto& block) { return keep(block.chunk); }) &&
        std::all_of(entry->chunk_changes.begin(), entry->chunk_changes.end(), [&](const auto& change) { return keep(change.chunk); }) &&
        std::all_of(entry->chunk_forms.begin(), entry->chunk_forms.end(), [&](const auto& form) { return keep(form.chunk); });
    const bool empty = entry->blocks.empty() && entry->chunk_changes.empty() && entry->chunk_forms.empty();
    // A change without chunks belongs to no partition, as to no AREA.
    if (all) return empty ? nullptr : entry;
    auto kept = std::make_shared<FeedEntry>(*entry);
    kept->protocol_frame.reset();
    std::erase_if(kept->blocks, [&](const auto& block) { return !keep(block.chunk); });
    std::erase_if(kept->chunk_changes, [&](const auto& change) { return !keep(change.chunk); });
    std::erase_if(kept->chunk_forms, [&](const auto& form) { return !keep(form.chunk); });
    if (kept->blocks.empty() && kept->chunk_changes.empty() && kept->chunk_forms.empty()) return nullptr;
    return kept;
}

void SetFeedSlotRecordPosition(FeedSlotRecord& slot, std::uint64_t revision) {
    slot.written = revision;
    for (auto& partition : slot.partitions) partition.written = revision;
}

void LowerFeedSlotRecordPositions(FeedSlotRecord& slot, std::uint64_t ceiling) {
    slot.written = std::min(slot.written, ceiling);
    for (auto& partition : slot.partitions) partition.written = std::min(partition.written, ceiling);
    Summarize(slot);
}

void MarkFeedSlotRecordResync(FeedSlotRecord& slot) {
    slot.resync = true;
    for (auto& partition : slot.partitions) partition.resync = true;
}

void MarkFeedSlotRecordLost(FeedSlotRecord& slot) {
    slot.lost = true;
    slot.resync = false;
    for (auto& partition : slot.partitions) partition.resync = false;
}

bool AdvanceFeedSlotRecord(FeedSlotRecord& slot, const std::vector<std::uint16_t>& held, std::uint64_t revision,
    bool clear_resync) {
    if (slot.partitions.empty()) {
        if (revision <= slot.written) return false;
        slot.written = revision;
        if (clear_resync) slot.resync = false;
        return true;
    }
    bool changed = false;
    for (std::size_t i = 0U; i < slot.partitions.size(); ++i) {
        auto& partition = slot.partitions[i];
        if (!Holds(held, i) || revision <= partition.written) continue;
        partition.written = revision;
        if (clear_resync) partition.resync = false;
        changed = true;
    }
    if (changed) Summarize(slot);
    return changed;
}

}  // namespace chunkdb
