#pragma once

#include <filesystem>
#include <memory>

#include "chunkdb/feed_slots.hpp"
#include "chunkdb/geometry.hpp"

namespace chunkdb {

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
};

}  // namespace chunkdb
