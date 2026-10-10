#pragma once

#include <atomic>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "chunkdb/types.hpp"
#include "feature_flags.hpp"
#include "wal_replay.hpp"

namespace chunkdb {
class Geometry;
struct FeedWalPrefixTestHook {
    enum class Point { kImageRead, kBeforeCatchUpSeed, kBeforeDirectoryRead, kAfterRecoveryTrim };
    virtual ~FeedWalPrefixTestHook() = default;
    virtual void Run(Point point, ChunkCoord coord) = 0;
};
struct FeedWalPrefixTestAccess {
    static void SetHook(FeedWalPrefixTestHook* hook) noexcept;
    static void Run(FeedWalPrefixTestHook::Point point, ChunkCoord coord);
};

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
        std::uint64_t generation = 0;
        bool seeded = false;
    };
  public:
    struct Prepared {
        std::shared_ptr<Entry> entry;
        std::map<std::uint64_t, std::uint64_t> added;
        bool reset = false;
    };
    struct SeedToken {
        std::shared_ptr<Entry> entry;
        std::uint64_t generation = 0;
    };
    [[nodiscard]] SeedToken BeginSeed(ChunkCoord coord);
    // Authoritative loads are serialized against other producers of this chunk.
    // An optimistic cold replay may publish only while its token is unchanged.
    void SeedReplay(ChunkCoord coord, const std::vector<WalFrameBoundary>& boundaries,
        const SeedToken* token = nullptr, std::string error = {},
        std::mutex* publication_mutex = nullptr);
    void SeedFile(const std::filesystem::path& root, const Geometry& geometry,
        ChunkCoord coord, const StoreId& epoch, FeatureFlags features,
        std::mutex* publication_mutex = nullptr);
    void SeedMissing(const std::filesystem::path& root, const Geometry& geometry,
        const StoreId& epoch, FeatureFlags features, std::mutex& publication_mutex);
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
