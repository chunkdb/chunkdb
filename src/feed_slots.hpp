#pragma once

#include <atomic>
#include <condition_variable>
#include <chrono>
#include <mutex>
#include <map>
#include <thread>

#include "feed_slot_records.hpp"
#include "feed_prefix.hpp"

namespace chunkdb {
class ChangeFeed;
struct FeedSlotTestHook {
    enum class Point { kBeforeFlush, kBeforeSync, kBeforePersist, kAfterRetention, kFeedDisabled, kAfterAckPersist, kAfterDrain };
    virtual ~FeedSlotTestHook() = default;
    virtual void Run(Point point, std::uint64_t captured) = 0;
};
struct FeedSlotTestAccess {
    static void Sync(Table& table);
    static FeedArchiveReader CompletedPrefix(Table& table, std::string_view name, FeedPosition after);
    static void EnableChunkForms(FeedArchiveReader& reader, std::size_t budget);
    static void SetEntryBudget(FeedArchiveReader& reader, std::size_t budget);
    static void Filter(FeedArchiveReader& reader, const FeedSlotFilters& filters);
    static void Retain(Table& table);
    static void SetHook(Table& table, FeedSlotTestHook* hook);
    static void StageAck(Table& table, std::string_view name, FeedPosition position, const std::vector<std::uint16_t>& partitions = {});
    static bool FlushAcks(Table& table, bool force, std::chrono::steady_clock::time_point now);
};

// The Table owns this state across store replacements. The current manager's
// records mutex protects it; replacement drains leases and stops that manager.
struct FeedSlotAckState {
    // Slots without partitions, by name.
    std::map<std::string, std::uint64_t> pending;
    // Partitioned slots, by name and partition.
    std::map<std::pair<std::string, std::uint16_t>, std::uint64_t> partition_pending;
    std::chrono::steady_clock::time_point flushed = std::chrono::steady_clock::now();
    [[nodiscard]] bool empty() const noexcept { return pending.empty() && partition_pending.empty(); }
    void Erase(const std::string& name) {
        pending.erase(name);
        std::erase_if(partition_pending, [&](const auto& item) { return item.first.first == name; });
    }
};

// Store-owned state. The table stops its worker before pausing/replacing the
// feed or the store. Lock order is chunk -> publish -> records; no operation
// holding records takes a chunk or publish lock.
class FeedSlots {
  public:
    FeedSlots(ChunkStore& store, std::size_t max_bytes, std::chrono::milliseconds interval);
    // Read-only admission before rollback/tail repair or alias removal.
    static void ValidateRecoveryAliases(const ChunkStore& store);
    ~FeedSlots();
    void Start(std::shared_ptr<ChangeFeed> feed);
    void Stop();
    [[nodiscard]] bool active() const noexcept;
    [[nodiscard]] std::size_t max_bytes() const noexcept { return max_bytes_; }
    [[nodiscard]] bool ArchiveRequired() const noexcept;
    // `partitions` is 1 for a slot without partitions, or 2 to 256.
    [[nodiscard]] FeedSlot Create(std::string_view name, std::uint64_t completed, bool if_not_exists = false, const FeedSlotFilters& filters = {},
        std::optional<std::string> owner = std::nullopt, std::uint32_t partitions = 1U);
    // Records `owner` for a slot that has none; a recorded owner never changes.
    void BindOwner(std::string_view name, const std::string& owner);
    void Drop(std::string_view name, bool if_exists = false);
    void Advance(std::string_view name, FeedPosition position);
    void UseAckState(const std::shared_ptr<FeedSlotAckState>& state) noexcept { acknowledgements_ = state; }
    // For the given partitions of a partitioned slot; empty for all of them.
    void StageAck(std::string_view name, FeedPosition position, const std::vector<std::uint16_t>& partitions = {});
    bool FlushAcks(bool force, std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());
    [[nodiscard]] std::vector<FeedSlot> List(bool include_lost = false) const;
    [[nodiscard]] FeedSlot Get(std::string_view name) const;
    // Slots, lost ones included.
    [[nodiscard]] std::size_t size() const;
    [[nodiscard]] FeedArchiveReader Reader(FeedPosition after);
    [[nodiscard]] FeedArchiveReader ReaderCompletedPrefix(std::string_view slot_name, FeedPosition after);
    [[nodiscard]] FeedWalPrefixIndex& prefix_index() noexcept { return prefix_index_; }
    // Under the chunk and checkpoint publish locks. Returns the target WAL
    // path, or empty when this segment has no frames.
    [[nodiscard]] std::filesystem::path PrepareArchive(ChunkCoord coord);
    void Sync(std::uint64_t completed);
    void Retain();

  private:
    friend struct FeedSlotTestAccess;
    void Persist(FeedSlotRecords next);
    void Run();
    bool SyncImpl(std::uint64_t completed);
    void RecoverAliases();
    // Archive bytes retained after each written position, from one listing.
    [[nodiscard]] std::vector<std::uint64_t> RetainedBytes(const std::vector<std::uint64_t>& positions) const;
    ChunkStore& store_;
    const std::size_t max_bytes_;
    const std::chrono::milliseconds interval_;
    mutable std::mutex mutex_;
    FeedSlotRecords records_;
    FeedWalPrefixIndex prefix_index_;
    std::shared_ptr<FeedSlotAckState> acknowledgements_ = std::make_shared<FeedSlotAckState>();
    std::shared_ptr<std::atomic<std::size_t>> readers_;
    std::shared_ptr<ChangeFeed> feed_;
    std::mutex worker_mutex_;
    std::condition_variable worker_cv_;
    bool stop_ = true;
    std::thread worker_;
    std::atomic<FeedSlotTestHook*> hook_{nullptr};
    // Whether this store prepared the archive directory, syncing its parent.
    std::atomic<bool> archive_dir_prepared_{false};
    // Whether a pass synced every WAL and image; under the WAL barrier lock.
    bool synced_ = false;
};

}  // namespace chunkdb
