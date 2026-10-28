#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <list>
#include <mutex>

#include "chunkdb/table_catalog.hpp"

namespace chunkdb {
class ChunkServer;

// Isolated delivery schedule hook. Install before test writes and retain it
// until the server stops; callbacks never change delivery decisions.
struct FeedDeliveryTestHook {
    enum class Point { kBeforeIoDrain, kBeforeIoJoin, kAfterIoJoin, kBeforeCatchUpJoin, kAfterCatchUpJoin,
        kBeforeWorkerJoin, kAfterWorkerJoin, kBeforeClientShutdown, kAfterClientShutdown, kBeforeClientIdleRead,
        kBeforePendingWait, kBeforePendingShutdown, kAfterRunPendingStop,
        kWorkerReady, kAfterPendingWait, kBeforeWorkerHandle, kAfterWorkerHandle,
        kBeforeWorkerClose, kAfterWorkerClose, kBeforeClientRead, kAfterClientRead,
        kBeforeIoScan, kAfterIoAdd, kAfterAdmission, kAfterProgressAdmission, kAfterPartialFeedWrite, kAfterBlockedFeedWrite, kAfterAckTimeoutClose, kBeforeSlotAck, kAfterSlotFinish, kBeforeReturnClient,
        kAfterClientIoCancel, kBeforeWorkerRegister, kBeforeAccept, kAfterTakeoverFence, kAfterTakeoverClose, kAfterSlotAck, kAfterTakeoverReserved };
    virtual ~FeedDeliveryTestHook() = default;
    virtual void Run(Point point, std::size_t bytes) = 0;
};
struct FeedDeliveryTestAccess {
    static void SetHook(ChunkServer& server, FeedDeliveryTestHook* hook);
    // Observe existing scheduling without adding a wake before shutdown.
    static void SetObserverHook(ChunkServer& server, FeedDeliveryTestHook* hook);
    static void SetPendingObserverHook(ChunkServer& server, FeedDeliveryTestHook* hook);
  private:
    static void InstallHook(ChunkServer& server, FeedDeliveryTestHook* hook, bool wake);
};

struct FeedSlotClaim {
    std::string name;
    std::atomic<bool> valid{true};
    std::atomic<bool> takeover_pending{false};
    std::mutex owner_mutex;
    std::weak_ptr<SlotWatch> owner;
    const std::string user;
    const std::uint64_t completed;
    const std::shared_ptr<std::atomic<std::size_t>> output_bytes;
    FeedSlotClaim(std::string value, std::uint64_t revision, std::shared_ptr<std::atomic<std::size_t>> bytes, std::string owner_user)
        : name(std::move(value)), user(std::move(owner_user)), completed(revision), output_bytes(std::move(bytes)) {}
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
        std::string name, FeedOptions options, bool takeover = false, std::string user = {},
        const std::function<void(std::string_view)>& authorize_owner = {});
    ~SlotWatch();
    [[nodiscard]] FeedPosition position() const noexcept { return start_; }
    [[nodiscard]] std::size_t budget() const noexcept { return options_.buffer_bytes.value_or(table_->FeedBufferBytes()); }
    [[nodiscard]] std::optional<Output> Take(std::size_t room);
    void SetQuota(std::size_t bytes);
    void Sent(std::uint64_t revision);
    void Consumed(std::size_t bytes);
    // Accepted acknowledgements have no wire reply. A queued terminal
    // error ends control processing without accepting another ACK.
    enum class AckResult { kAccepted, kTerminal };
    AckResult Ack(std::uint64_t revision);
    void Unwatch();
    void Cancel() noexcept;
    // True when the watch was not active yet.
    bool Activate();
    // Only the catch-up worker calls these operations. A step returns false
    // when it changed nothing: it waits for an event that wakes the worker.
    bool WorkStep();
    [[nodiscard]] bool Finished() const;
    [[nodiscard]] bool AckTimedOut() const;
    [[nodiscard]] bool TakingOver() const;
    void SocketClosed() noexcept;
    [[nodiscard]] bool ReleaseConnectionForReturn();

  private:
    friend class FeedIo;
    friend class Table;
    friend struct FeedDeliveryTestAccess;
    friend struct SlotAckDeadlineTestAccess;
    std::atomic<FeedDeliveryTestHook*> hook_{nullptr};
    std::atomic<bool> io_ready_{false};
    SlotWatch(std::shared_ptr<Table> table, std::shared_ptr<FeedSlotClaim> claim,
        FeedPosition start, FeedOptions options, bool resync);
    bool Publish(const std::shared_ptr<const FeedEntry>& entry);
    void Finish(std::optional<Output> control);
    bool Work();
    bool FlushAck(bool force);
    // True when it published a progress frame.
    bool Progress(const FeedSlot& slot);
    void SentAt(std::uint64_t revision, std::chrono::steady_clock::time_point now);
    bool ExpireAckDeadline(std::chrono::steady_clock::time_point now);
    bool AckDeadlineExpiredLocked(std::chrono::steady_clock::time_point now);
    void ClearReceipts() noexcept;
    void RequestTakeoverAndWait();
    struct MutationReceipt {
        std::uint64_t revision;
        std::optional<std::chrono::steady_clock::time_point> sent{};
    };
    // Includes the list node and allocator bookkeeping; allocation precedes admission.
    static constexpr std::size_t kReceiptBytes = sizeof(MutationReceipt) + 4U * sizeof(void*);
    const std::shared_ptr<Table> table_;
    std::shared_ptr<FeedSlotClaim> claim_;
    const std::weak_ptr<FeedSlotClaim> generation_;
    const std::shared_ptr<std::atomic<std::size_t>> table_bytes_;
    const FeedPosition start_;
    const FeedOptions options_;
    mutable std::mutex mutex_;
    std::condition_variable closed_cv_;
    std::exception_ptr takeover_error_;
    bool takeover_ = false, socket_closed_ = false;
    std::deque<Output> output_;
    std::size_t bytes_ = 0, unsent_ = 0;
    std::size_t quota_;
    std::uint64_t sent_, acknowledged_, written_;
    std::list<MutationReceipt> receipts_;
    bool ack_expired_ = false;
    bool cancelled_ = false, unwatch_ = false, finished_ = false, active_ = false;
    // Worker-only fields.
    bool resync_, joined_ = false;
    FeedPosition cursor_;
    std::uint64_t join_ = 0, schema_ = 0;
    std::unique_ptr<FeedSubscription> live_;
    std::optional<FeedArchiveReader> archive_;
    std::shared_ptr<const FeedEntry> pending_;
    std::chrono::steady_clock::time_point next_progress_;
};
}  // namespace chunkdb
