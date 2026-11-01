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
std::string EncodeFeedResync(FeedPosition position, const std::optional<TableLineage>& lineage);
}
