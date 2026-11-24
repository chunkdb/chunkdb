#pragma once

#include <atomic>
#include <deque>
#include <filesystem>
#include <functional>
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

class CoordinatePublicationTestHook {
  public:
    virtual ~CoordinatePublicationTestHook() = default;
    // A write published a population change of `coord`: its revision and
    // then its populated flag.
    virtual void AfterPopulationChange(const ChunkCoord& coord) noexcept = 0;
};
void SetCoordinatePublicationTestHook(CoordinatePublicationTestHook* hook) noexcept;

// Consecutive coordinates of the index with an upper bound of their
// revisions, so a page of changes after a revision skips an unchanged run
// whole instead of visiting each coordinate in it.
// One cache line each, so writers raising neighbouring runs do not share one.
struct alignas(64) CoordinateRun {
    std::atomic<std::uint64_t> maximum_revision{0U};
    // The owning index mutex protects the boundary and the count.
    ChunkCoord first{};
    std::size_t nodes = 0U;
};

struct ChunkCoordinateNode {
    std::atomic<std::uint64_t> revision{0U};
    std::atomic<bool> populated{false};
    // The owning index mutex protects this independently checked boundary.
    std::optional<ChunkIndexRecord> retained_empty;
    // Set and moved under the owning index mutex; a writer reads it after
    // storing the revision (sequentially consistent on both sides).
    std::atomic<CoordinateRun*> run{nullptr};
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

struct RecoveredCoordinateIdentity {
    ChunkCoord coord;
    std::uint64_t revision = 0U;
    bool populated = false;
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
    // Prepare every allocation and validate the complete publication before
    // atomically replacing only the changed nodes of a read-only index.
    void RefreshReadOnly(std::span<const RecoveredCoordinateIdentity> updates,
        const ChunkCoordinateIndex& retained, const std::function<void()>& validate);
    // A borrowed node is used only under its canonical chunk's exclusive lock.
    [[nodiscard]] ChunkCoordinateNode* Find(const ChunkCoord& coord);
    [[nodiscard]] bool IsPopulationCandidate(const ChunkCoord& coord) const;
    [[nodiscard]] CoordinateReservation Prepare(const ChunkCoord& coord, ChunkCoordinateNode** cache_slot);
    [[nodiscard]] CoordinatePage Page(bool has_cursor, const ChunkCoord& cursor, std::size_t limit,
        std::optional<std::uint64_t> since = {}) const;
    // The coordinates populated at revision `snapshot`, with every write up to
    // it published: those populated now and those changed after it. A reader
    // checks each one against the snapshot.
    [[nodiscard]] CoordinatePage SnapshotPage(bool has_cursor, const ChunkCoord& cursor, std::size_t limit,
        std::uint64_t snapshot) const;
    // Exclusive current-state preparation does not make a history promise.
    [[nodiscard]] CoordinatePage CurrentPage(bool has_cursor, const ChunkCoord& cursor, std::size_t limit) const;
    [[nodiscard]] std::optional<ChunkIndexRecord> EmptyBoundary(const ChunkCoord& coord) const;
    [[nodiscard]] std::uint64_t maximum_recovered_revision() const noexcept { return maximum_recovered_revision_; }
    [[nodiscard]] std::uint64_t retained_deletion_floor() const noexcept { return deletion_floor_.load(std::memory_order_acquire); }
    [[nodiscard]] bool failed() const noexcept { return failed_.load(std::memory_order_acquire); }
    [[nodiscard]] bool has_written_identities() const noexcept { return any_written_.load(std::memory_order_acquire); }
    [[nodiscard]] std::uint64_t inventory_generation() const noexcept { return inventory_generation_.load(std::memory_order_acquire); }
    [[nodiscard]] std::uint64_t tombstone_count() const;
    [[nodiscard]] std::uint64_t populated_count() const;
    void PublishTombstone(const ChunkIndexRecord& record);
    void Compact(std::uint64_t before, std::uint64_t completed_frontier);

  private:
    friend class CoordinateReservation;
    struct Shard {
        IndexInventoryEntry inventory;
        std::uint64_t observed_bytes = 0U;
        bool installed_replacement = false;
    };
    using Coordinates = std::map<ChunkCoord, ChunkCoordinateNode>;
    using Runs = std::map<ChunkCoord, CoordinateRun*>;
    void Check() const;
    // Run upkeep, under mutex_ once requests may use the index. A run splits
    // above kRunNodes; ReserveRunSplits makes the next `splits` splits
    // allocation-free, so attaching a node never throws.
    void ReserveRunSplits(std::size_t splits);
    void AttachRun(Coordinates::iterator at) noexcept;
    void DetachRun(ChunkCoordinateNode& node) noexcept;
    void SplitRun(Runs::iterator run) noexcept;
    // Under mutex_: visits in order, after `from` (from it when `inclusive`) or
    // from the start without `has_from`, each
    // coordinate whose revision exceeds `since`, skipping runs whose maximum
    // does not. Stops before a coordinate `visit` refuses, or once `budget`
    // runs and coordinates are examined, setting where to resume
    // (inclusive) in `resume` and returning true; returns false at the end.
    template <class Visit>
    bool WalkChanged(bool has_from, const ChunkCoord& from, bool inclusive, std::uint64_t since,
        std::size_t budget, ChunkCoord& resume, Visit&& visit) const;
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
    Coordinates coordinates_;
    std::set<ChunkCoord> populated_;
    // Lower boundary -> run; the first run starts at the lowest coordinate
    // and is never removed. Runs are reused, not freed: a writer may still
    // raise the maximum of a run its node just left, which only widens it.
    Runs runs_;
    std::deque<CoordinateRun> run_storage_;
    std::vector<CoordinateRun*> free_runs_;
    std::vector<Runs::node_type> spare_run_entries_;
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
