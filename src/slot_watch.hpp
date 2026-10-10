#pragma once

#include <atomic>
#include <deque>
#include <mutex>

#include "chunkdb/table_catalog.hpp"

namespace chunkdb {
class ChunkServer;

// Isolated delivery schedule hook. Install before test writes and retain it
// until the server stops; callbacks never change delivery decisions.
struct FeedDeliveryTestHook {
    enum class Point { kBeforeIoScan, kAfterIoAdd, kAfterAdmission, kBeforeReturnClient };
    virtual ~FeedDeliveryTestHook() = default;
    virtual void Run(Point point, std::size_t bytes) = 0;
};
struct FeedDeliveryTestAccess {
    static void SetHook(ChunkServer& server, FeedDeliveryTestHook* hook);
};

struct FeedSlotClaim {
    std::string name;
    std::atomic<bool> valid{true};
    const std::uint64_t completed;
    const std::shared_ptr<std::atomic<std::size_t>> output_bytes;
    FeedSlotClaim(std::string value, std::uint64_t revision, std::shared_ptr<std::atomic<std::size_t>> bytes)
        : name(std::move(value)), completed(revision), output_bytes(std::move(bytes)) {}
};

// Socket operations only touch the bounded queue and control fields. A single
// catch-up worker owns the archive and subscription, including their cleanup.
class SlotWatch {
  public:
    struct Output {
        std::shared_ptr<const std::string> bytes;
        std::optional<std::uint64_t> revision{};
        bool close = false;
        bool resume = false;
        bool charged = true;
    };
    static std::shared_ptr<SlotWatch> Create(std::shared_ptr<Table> table,
        std::string name, FeedOptions options);
    ~SlotWatch();
    [[nodiscard]] FeedPosition position() const noexcept { return start_; }
    [[nodiscard]] std::size_t budget() const noexcept { return options_.buffer_bytes.value_or(table_->FeedBufferBytes()); }
    [[nodiscard]] std::optional<Output> Take(std::size_t room);
    void SetQuota(std::size_t bytes);
    void Sent(std::uint64_t revision);
    void Consumed(std::size_t bytes);
    // Accepted acknowledgements have no wire reply.
    void Ack(std::uint64_t revision);
    void Unwatch();
    void Cancel() noexcept;
    void Activate();
    // Only the catch-up worker calls these operations.
    void WorkStep();
    [[nodiscard]] bool Finished() const;

  private:
    friend class FeedIo;
    friend struct FeedDeliveryTestAccess;
    std::atomic<FeedDeliveryTestHook*> hook_{nullptr};
    SlotWatch(std::shared_ptr<Table> table, std::shared_ptr<FeedSlotClaim> claim,
        FeedPosition start, FeedOptions options, bool resync);
    bool Publish(const std::shared_ptr<const FeedEntry>& entry);
    void Finish(std::optional<Output> control);
    void Work();
    bool FlushAck(bool force);
    const std::shared_ptr<Table> table_;
    std::shared_ptr<FeedSlotClaim> claim_;
    const std::shared_ptr<std::atomic<std::size_t>> table_bytes_;
    const FeedPosition start_;
    const FeedOptions options_;
    mutable std::mutex mutex_;
    std::deque<Output> output_;
    std::size_t bytes_ = 0, unsent_ = 0;
    std::size_t quota_;
    std::uint64_t sent_, acknowledged_, written_;
    bool cancelled_ = false, unwatch_ = false, finished_ = false, active_ = false;
    // Worker-only fields.
    bool resync_, joined_ = false;
    FeedPosition cursor_;
    std::uint64_t join_ = 0, schema_ = 0;
    std::unique_ptr<FeedSubscription> live_;
    std::optional<FeedArchiveReader> archive_;
    std::shared_ptr<const FeedEntry> pending_;
};
}  // namespace chunkdb
