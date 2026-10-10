#pragma once

#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "chunkdb/types.hpp"
#include "feature_flags.hpp"

namespace chunkdb {
class Geometry;

struct FeedWalPrefix {
    ChunkCoord coord;
    std::uint64_t first = 0, last = 0, limit = 0;
};

// Tracks complete appended frame boundaries, not durability. Capture is always
// restricted by the separately persisted completed durable watermark. Producers
// reserve every map node before writing; publication afterwards cannot allocate.
class FeedWalPrefixIndex {
    struct Entry {
        std::map<std::uint64_t, std::uint64_t> ends;
        std::string error;
    };
  public:
    struct Prepared {
        std::shared_ptr<Entry> entry;
        std::map<std::uint64_t, std::uint64_t> added;
        bool reset = false;
    };
    // Only while opening a recovered store, before any producer can enter it.
    void Seed(const std::filesystem::path& root, const Geometry& geometry, const StoreId& epoch, FeatureFlags features);
    [[nodiscard]] Prepared Prepare(ChunkCoord coord, std::uint64_t before,
        std::span<const std::uint8_t> bytes);
    void Commit(Prepared&& prepared) noexcept;
    void Truncate(ChunkCoord coord, std::uint64_t boundary) noexcept;
    void Clear() noexcept;
    [[nodiscard]] std::vector<FeedWalPrefix> Capture(std::uint64_t through) const;
  private:
    mutable std::mutex mutex_;
    std::map<std::pair<std::int64_t, std::int64_t>, std::shared_ptr<Entry>> entries_;
};

} // namespace chunkdb
