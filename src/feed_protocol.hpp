#pragma once
#include <bit>
#include <type_traits>
#include "chunkdb/change_feed.hpp"
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
std::string EncodeFeedEntry(const FeedEntry& entry);
}
