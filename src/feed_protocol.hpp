#pragma once
#include <bit>
#include <type_traits>
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
    const Geometry& geometry, const std::optional<FeedArea>& area,
    const std::optional<std::vector<std::uint32_t>>& ids);
std::shared_ptr<const FeedEntry> FilterFeedEntry(const std::shared_ptr<const FeedEntry>& entry,
    const std::optional<FeedArea>& area, const std::optional<std::vector<std::uint32_t>>& ids,
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
// True when `area` is unset or holds chunk `coord`.
bool FeedAreaHolds(const std::optional<FeedArea>& area, std::uint8_t dims, ChunkCoord coord);
// The indexes in `columns` of the projected column `ids`, in their order, as FilterFeedEntry and ChunkFeedEntry
// project them; throws when one was dropped or repeats.
std::vector<std::size_t> FeedProjectionIndexes(const std::vector<Column>& columns, const std::vector<std::uint32_t>& ids);
std::string EncodeFeedResync(FeedPosition position, const std::optional<TableLineage>& lineage);
}
