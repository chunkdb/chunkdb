#pragma once

#include <filesystem>
#include <memory>
#include <vector>

#include "chunkdb/feed_slots.hpp"
#include "chunkdb/geometry.hpp"

namespace chunkdb {
struct FeedWalPrefix;

// The caller holds the table's exclusive lease and checkpoint publication
// mutex while capturing the snapshot. The pin prevents deletion of archives
// and base images, including WALs archived after this call, until destruction.
struct FeedArchiveAccess {
    [[nodiscard]] static FeedArchiveReader Create(
        const std::filesystem::path& root,
        const Geometry& geometry,
        const StoreId& epoch,
        FeedPosition from,
        std::uint64_t through_durable,
        std::shared_ptr<void> retention_pin,
        FeatureFlags features = {});
    // The caller captures completed WAL byte boundaries and the retention pin
    // under publication/slot locks. All filesystem reads happen after release;
    // a live cursor never reads bytes beyond its captured completed prefix.
    [[nodiscard]] static FeedArchiveReader CreateCompletedPrefix(
        const std::filesystem::path& root,
        const Geometry& geometry,
        const StoreId& epoch,
        FeedPosition from,
        std::uint64_t through_durable,
        std::shared_ptr<void> retention_pin,
        const std::vector<FeedWalPrefix>& prefixes,
        FeatureFlags features = {});
};

}  // namespace chunkdb
