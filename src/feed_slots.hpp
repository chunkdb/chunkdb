#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "feed_slot_records.hpp"

namespace chunkdb {
class ChangeFeed;
struct FeedSlotTestHook {
    enum class Point { kBeforeFlush, kBeforeSync, kBeforePersist };
    virtual ~FeedSlotTestHook() = default;
    virtual void Run(Point point, std::uint64_t captured) = 0;
};
struct FeedSlotTestAccess {
    static void Sync(Table& table);
    static void Retain(Table& table);
    static void SetHook(Table& table, FeedSlotTestHook* hook);
};

// Store-owned state. The table stops its worker before pausing/replacing the
// feed or the store. Lock order is chunk -> publish -> records; no operation
// holding records takes a chunk or publish lock.
class FeedSlots {
  public:
    FeedSlots(ChunkStore& store, std::size_t max_bytes, std::chrono::milliseconds interval);
    ~FeedSlots();
    void Start(std::shared_ptr<ChangeFeed> feed);
    void Stop();
    [[nodiscard]] bool active() const noexcept;
    [[nodiscard]] bool ArchiveRequired() const noexcept;
    [[nodiscard]] FeedSlot Create(std::string_view name, std::uint64_t completed);
    void Drop(std::string_view name);
    void Advance(std::string_view name, FeedPosition position);
    [[nodiscard]] std::vector<FeedSlot> List() const;
    [[nodiscard]] FeedArchiveReader Reader(FeedPosition after);
    // Under the chunk and checkpoint publish locks. Returns the target WAL
    // path, or empty when this segment has no frames.
    [[nodiscard]] std::filesystem::path PrepareArchive(ChunkCoord coord);
    void Sync(std::uint64_t completed);
    void Retain();

  private:
    friend struct FeedSlotTestAccess;
    void Persist(FeedSlotRecords next);
    void Run();
    void SyncImpl(std::uint64_t completed);
    void RecoverAliases();
    [[nodiscard]] std::uint64_t RetainedBytes(std::uint64_t written) const;
    ChunkStore& store_;
    const std::size_t max_bytes_;
    const std::chrono::milliseconds interval_;
    mutable std::mutex mutex_;
    FeedSlotRecords records_;
    std::shared_ptr<std::atomic<std::size_t>> readers_;
    std::shared_ptr<ChangeFeed> feed_;
    std::mutex worker_mutex_;
    std::condition_variable worker_cv_;
    bool stop_ = true;
    std::thread worker_;
    std::atomic<FeedSlotTestHook*> hook_{nullptr};
};

}  // namespace chunkdb
