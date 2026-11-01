#pragma once

#include <atomic>
#include <filesystem>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

#include "populated_index_format.hpp"

namespace chunkdb {

inline constexpr std::string_view kChunkIndexFileName = "chunkdb.index";

class CoordinateArtifactTestHook {
  public:
    virtual ~CoordinateArtifactTestHook() = default;
    virtual void BeforeStatus(const std::filesystem::path& path) = 0;
};
void SetCoordinateArtifactTestHook(CoordinateArtifactTestHook* hook) noexcept;

struct ChunkCoordinateNode {
    std::atomic<std::uint64_t> revision{0U};
    std::atomic<bool> populated{false};
    // The owning index mutex protects this independently checked boundary.
    std::optional<ChunkIndexRecord> retained_empty;
};

class ChunkCoordinateIndex;

// The caller retains its pinned chunk and exclusive chunk lock until this
// reservation is published or destroyed; cache_slot is borrowed for that span.
class CoordinateReservation {
  public:
    CoordinateReservation() = default;
    CoordinateReservation(const CoordinateReservation&) = delete;
    CoordinateReservation& operator=(const CoordinateReservation&) = delete;
    CoordinateReservation(CoordinateReservation&& other) noexcept;
    CoordinateReservation& operator=(CoordinateReservation&& other) noexcept;
    ~CoordinateReservation();
    void Publish(bool populated, std::uint64_t revision) noexcept;

  private:
    friend class ChunkCoordinateIndex;
    void Reset() noexcept;
    ChunkCoordinateIndex* owner_ = nullptr;
    ChunkCoordinateNode* node_ = nullptr;
    ChunkCoordinateNode** cache_slot_ = nullptr;
    ChunkCoord coord_{};
    bool inserted_ = false;
    std::set<ChunkCoord>::node_type live_node_;
};

struct CoordinatePage {
    std::vector<ChunkCoord> coords;
    bool more = false;
};

class ChunkCoordinateIndex {
  public:
    ChunkCoordinateIndex(std::filesystem::path root, const Geometry& geometry,
        StoreId table_id, bool read_only);
    // Publishes the empty inventory before its owning manifest or table name.
    static bool InitializeEmpty(const std::filesystem::path& root, const Geometry& geometry,
        const StoreId& table_id);
    [[nodiscard]] static std::optional<StoreId> UnpublishedEmptyIdentity(
        const std::filesystem::path& root, std::uint8_t dims);

    // Admission is read-only; repairs start only after all other structures pass.
    void CompleteRecovery();
    void AdmitUnlistedLog(const LargeChunkCoord& coord);
    // Candidate construction only; no request may use this owner yet.
    void ObserveRecovered(const ChunkCoord& coord, const ChunkState& state);
    void ObserveRecoveredIdentity(const ChunkCoord& coord, std::uint64_t revision, bool populated);
    // A borrowed node is used only under its canonical chunk's exclusive lock.
    [[nodiscard]] ChunkCoordinateNode* Find(const ChunkCoord& coord);
    [[nodiscard]] bool IsPopulationCandidate(const ChunkCoord& coord) const;
    [[nodiscard]] CoordinateReservation Prepare(const ChunkCoord& coord, ChunkCoordinateNode** cache_slot);
    [[nodiscard]] CoordinatePage Page(bool has_cursor, const ChunkCoord& cursor, std::size_t limit,
        std::optional<std::uint64_t> since = {}) const;
    // Exclusive current-state preparation does not make a history promise.
    [[nodiscard]] CoordinatePage CurrentPage(bool has_cursor, const ChunkCoord& cursor, std::size_t limit) const;
    [[nodiscard]] std::optional<ChunkIndexRecord> EmptyBoundary(const ChunkCoord& coord) const;
    [[nodiscard]] std::uint64_t maximum_recovered_revision() const noexcept { return maximum_recovered_revision_; }
    [[nodiscard]] std::uint64_t retained_deletion_floor() const noexcept { return deletion_floor_.load(std::memory_order_acquire); }
    [[nodiscard]] bool failed() const noexcept { return failed_.load(std::memory_order_acquire); }
    [[nodiscard]] bool has_written_identities() const noexcept { return any_written_.load(std::memory_order_acquire); }
    [[nodiscard]] std::uint64_t inventory_generation() const noexcept { return inventory_generation_.load(std::memory_order_acquire); }
    [[nodiscard]] std::uint64_t tombstone_count() const;
    void PublishTombstone(const ChunkIndexRecord& record);
    void Compact(std::uint64_t before, std::uint64_t completed_frontier);

  private:
    friend class CoordinateReservation;
    struct Shard {
        IndexInventoryEntry inventory;
        std::uint64_t observed_bytes = 0U;
        bool installed_replacement = false;
    };
    void Check() const;
    void ReleaseReservation(CoordinateReservation& reservation) noexcept;
    void PublishReservation(CoordinateReservation& reservation, bool populated, std::uint64_t revision) noexcept;
    void PublishInventory(const IndexInventoryHeader& header, std::span<const IndexInventoryEntry> entries);
    void AdmitLog(const Shard& shard, const std::filesystem::path& path, std::uint64_t cut);
    [[nodiscard]] std::filesystem::path LogPath(const LargeChunkCoord& coord) const;
    [[nodiscard]] std::filesystem::path StagePath(const LargeChunkCoord& coord) const;

    std::filesystem::path root_;
    const Geometry& geometry_;
    StoreId table_id_;
    bool read_only_;
    mutable std::mutex mutex_;
    std::map<ChunkCoord, ChunkCoordinateNode> coordinates_;
    std::set<ChunkCoord> populated_;
    std::mutex publication_mutex_;
    IndexInventoryHeader inventory_;
    std::map<LargeChunkCoord, Shard> shards_;
    std::map<LargeChunkCoord, Shard> unlisted_;
    std::uint64_t maximum_recovered_revision_ = 0U;
    std::atomic<std::uint64_t> deletion_floor_{0U};
    std::atomic<bool> failed_{false};
    std::atomic<std::uint64_t> inventory_generation_{0U};
    std::atomic<bool> any_written_{false};
};

}  // namespace chunkdb
