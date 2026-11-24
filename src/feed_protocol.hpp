#pragma once
#include <bit>
#include <span>
#include <type_traits>
#include <unordered_map>
#include "chunkdb/change_feed.hpp"
#include "chunkdb/chunk_vars.hpp"
namespace chunkdb {
inline bool SameFeedValues(const std::vector<ColumnValue>& a, const std::vector<ColumnValue>& b) {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (a[i].index() != b[i].index()) return false;
        const bool same = std::visit([&](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            const auto& w = std::get<T>(b[i]);
            // Keep signed zero and NaN representations distinct.
            if constexpr (std::is_same_v<T, float>) return std::bit_cast<std::uint32_t>(v) == std::bit_cast<std::uint32_t>(w);
            else if constexpr (std::is_same_v<T, double>) return std::bit_cast<std::uint64_t>(v) == std::bit_cast<std::uint64_t>(w);
            else return v == w;
        }, a[i]);
        if (!same) return false;
    }
    return true;
}
std::string EncodeChunkForm(const ChunkLayout& layout, std::uint64_t version,
    const std::vector<std::uint8_t>& payload, const std::vector<std::uint8_t>& presence,
    const ChunkVars& vars, const std::vector<std::size_t>& columns);
std::shared_ptr<const FeedEntry> ChunkFeedEntry(const std::shared_ptr<const FeedEntry>& entry,
    const Geometry& geometry, std::span<const FeedArea> areas,
    const std::optional<std::vector<std::uint32_t>>& ids);
std::shared_ptr<const FeedEntry> FilterFeedEntry(const std::shared_ptr<const FeedEntry>& entry,
    std::span<const FeedArea> areas, const std::optional<std::vector<std::uint32_t>>& ids,
    bool no_before, const std::vector<Column>& columns);
std::string EncodeFeedEntry(const FeedEntry& entry);
std::size_t FeedEntryRetainedBytes(const FeedEntry& entry);
std::size_t FeedRowRetainedBytes(const std::optional<std::vector<ColumnValue>>& row);
// Lower bounds of the bytes EncodeFeedEntry writes for one row, for one block or chunk change, and for one chunk
// of a chunks frame.
std::size_t FeedRowMinimumEncodedBytes(const std::optional<std::vector<ColumnValue>>& row);
std::size_t FeedChangeItemMinimumEncodedBytes(std::uint8_t dims,
    const std::optional<std::vector<ColumnValue>>& before, const std::optional<std::vector<ColumnValue>>& after);
std::size_t FeedChunkFormMinimumEncodedBytes(std::uint8_t dims, const std::shared_ptr<const std::string>& form);
// A slot filter area as a list: empty or one area.
inline std::span<const FeedArea> FeedAreas(const std::optional<FeedArea>& area) noexcept {
    return area ? std::span<const FeedArea>(&*area, 1U) : std::span<const FeedArea>{};
}
// Whether a filtered change that kept this of its commit is sent: AREA drops
// a change that keeps no data, with its messages; a change that carries a
// message is otherwise sent even without data.
inline bool FeedEntryKept(const FeedEntry& entry, bool area_filtered) noexcept {
    return !entry.blocks.empty() || !entry.chunk_changes.empty() || !entry.chunk_forms.empty() ||
        (!area_filtered && !entry.messages.empty());
}
// True when `areas` is empty or one of them holds chunk `coord`.
bool FeedAreaHolds(std::span<const FeedArea> areas, std::uint8_t dims, ChunkCoord coord);
// The indexes in `columns` of the projected column `ids`, in their order, as FilterFeedEntry and ChunkFeedEntry
// project them; throws when one was dropped or repeats.
std::vector<std::size_t> FeedProjectionIndexes(const std::vector<Column>& columns, const std::vector<std::uint32_t>& ids);
std::string EncodeFeedResync(FeedPosition position, const std::optional<TableLineage>& lineage);
// The exact size of EncodeFeedEntry for a chunks entry.
std::size_t FeedChunksEncodedBytes(const FeedEntry& entry);

// Merges consecutive chunks pushes of a lagging watch into one push. Each chunk
// keeps its newest version and form, in the order chunks first appear; the push
// takes the position, commit time and user of the newest merged push.
class FeedChunksMerge {
  public:
    // Adds a chunks push above the merged ones. Returns false and changes
    // nothing when its schema version differs from theirs or the merged push
    // would encode to more than `limit` bytes.
    bool Add(const std::shared_ptr<const FeedEntry>& entry, std::size_t limit);
    [[nodiscard]] bool empty() const noexcept { return newest_ == nullptr; }
    // The merged push with its frame, or the only push as it was added; empties the merge.
    [[nodiscard]] std::shared_ptr<const FeedEntry> Take();
    void Clear() noexcept;

  private:
    struct Item { std::size_t index; std::size_t bytes; };
    std::shared_ptr<const FeedEntry> newest_;
    std::shared_ptr<FeedEntry> merged_;  // Set from the second push on.
    std::unordered_map<ChunkCoord, Item, ChunkCoordHash> items_;
    std::size_t item_bytes_ = 0U;
};
}
