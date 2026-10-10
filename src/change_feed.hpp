#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <span>
#include <thread>

#include "chunkdb/change_feed.hpp"
#include "chunkdb/geometry.hpp"

namespace chunkdb {

// The current write scope's identity; an empty view denotes anonymity.
[[nodiscard]] std::string_view CurrentWriteUser() noexcept;

// Isolated deterministic hook, like the transaction pause points. Tests must
// install it before starting writers and retain it until the feed stops.
struct FeedTestHook {
    enum class Point { kBeforeSlot, kAfterVersion, kAfterClock, kBeforeMerge, kBeforeResume, kAfterResume, kBeforeLingerPause };
    virtual ~FeedTestHook() = default;
    virtual void Run(Point point, std::uint64_t revision) = 0;
};
struct FeedTestAccess {
    static void SetHook(Table& table, FeedTestHook* hook);
    // Quiescent tests only: install before starting writers, remove after joining them.
    static void SetWriteHook(Table& table, FeedTestHook* hook);
    static std::uint64_t Watermark(Table& table);
    static std::size_t BufferedBytes(Table& table);
    static bool WaitLingerExpired(Table& table, std::chrono::milliseconds timeout);
    static bool Capturing(Table& table);
    static void ExpireLinger(Table& table);
    static bool WaitLingerDraining(Table& table, std::chrono::milliseconds timeout);
    static void CancelLingerTimer(Table& table);
};

class FeedProducerRegistry;

class ChangeFeed : public std::enable_shared_from_this<ChangeFeed> {
  public:
    ChangeFeed(StoreId epoch, std::size_t budget);
    ~ChangeFeed();
    void Resume(ChunkStore& store);
    void Pause();
    // Exclusive table control only, after Pause has drained and joined the sender.
    void ResizeBudget(std::size_t budget);
    void End();
    [[nodiscard]] bool attached() const noexcept { return clock_ != nullptr; }
    [[nodiscard]] std::size_t budget() const noexcept { return budget_.load(std::memory_order_acquire); }
    [[nodiscard]] std::uint64_t CompletedWatermark() const;
    void NotifyDurableWatermark();
    [[nodiscard]] std::unique_ptr<FeedSubscription> Subscribe(std::weak_ptr<Table> table, const FeedOptions& options);

  private:
    friend class FeedWriteGuard;
    friend class FeedProducerRegistry;
    friend class FeedSubscription;
    friend struct FeedTestAccess;
    friend class Table;
    struct RawFrame {
        ChunkCoord coord;
        std::optional<std::size_t> block;
        std::vector<std::uint8_t> payload;
        std::vector<std::uint8_t> presence;
        std::vector<std::uint8_t> vars;
        std::vector<std::uint8_t> wal;
    };
    struct RawWrite {
        RawWrite* next = nullptr;
        RawWrite* allocated_next = nullptr;
        std::uint64_t revision = 0;
        std::vector<std::uint8_t> user;
        std::size_t bytes = 0;
        std::size_t used = 0;
        std::vector<RawFrame> frames;
    };
    struct alignas(128) Producer {
        struct alignas(128) WriteContext {
            ChangeFeed* feed = nullptr;
            Producer* producer = nullptr;
            RawWrite* write = nullptr;
            std::uint64_t revision = 0;
            bool dropped = false;
            bool active = false;
        };
        Producer* next = nullptr;  // Immutable after registration.
        std::atomic<std::uint64_t> bound{0};
        FeedTestHook* hook = nullptr; // Writer-private; changed only by quiescent tests.
        std::atomic<RawWrite*> incoming{nullptr};
        std::atomic<RawWrite*> recycled{nullptr};
        // Sender takes this only when reclaiming idle capacity at the byte limit.
        std::atomic<bool> reclaiming{false};
        RawWrite* allocated = nullptr;  // Edited under reclaiming; freed after quiescence.
        RawWrite* pending = nullptr;    // Sender only, in ascending order.
        RawWrite* tail = nullptr;
        WriteContext context;  // Writer only; at most one active guard per producer.
        ~Producer();
        RawWrite* Take(ChangeFeed& feed);
        void Recycle(RawWrite* write) noexcept;
        void Publish(RawWrite* write) noexcept;
        void Collect() noexcept;
    };
    struct Retained {
        std::shared_ptr<const FeedEntry> entry;
        std::size_t bytes;
    };
    Producer& ThreadProducer();
    bool Reserve(std::size_t bytes) noexcept;
    void Release(std::size_t bytes) noexcept;
    void Lost(std::uint64_t revision) noexcept;
    void Wake() noexcept;
    std::uint64_t Watermark() const;
    void ResumeImpl(ChunkStore& store);
    // Exclusive timer restoration must fail before reopening a table if Resume
    // captured an error for subscribers instead of throwing it.
    void RethrowError() const;
    [[nodiscard]] bool HasError() const;
    void Fail(std::exception_ptr error);
    void Send();
    void Merge(std::uint64_t watermark);
    void Append(std::shared_ptr<const FeedEntry> entry, std::size_t bytes, RawWrite* consumed = nullptr,
                bool notify = true);
    bool ReclaimSpare(std::size_t needed) noexcept;
    void DiscardBuffers(RawWrite& write) noexcept;
    void Recycle(Producer& producer, RawWrite* write) noexcept;
    void ClearRing();
    std::shared_ptr<const FeedEntry> Next(FeedSubscription& subscription, std::chrono::milliseconds timeout);
    std::shared_ptr<const FeedEntry> Decode(const RawWrite& write) const;
    void RunHook(FeedTestHook::Point point, std::uint64_t version) const;

    const StoreId epoch_;
    std::atomic<std::size_t> budget_;
    std::shared_ptr<FeedProducerRegistry> producers_;
    void ClearProducerBuffers();
    // Raw node/buffer capacity (staged, queued and cached) plus typed ring entries.
    std::atomic<std::size_t> bytes_{0};
    std::atomic<std::uint64_t> lost_until_{0};
    std::atomic<bool> wake_{false};
    std::atomic<bool> stopping_{false};
    std::atomic<FeedTestHook*> hook_{nullptr};
    std::thread sender_;
    // Sender sees a stable store/geometry between Resume and Pause. Readers
    // hold mutex_ while using clock_; Pause clears it before the store closes.
    std::atomic<std::uint64_t>* clock_ = nullptr;
    std::uint64_t ceiling_ = 0;
    std::atomic<std::uint64_t>* ceiling_clock_ = nullptr;
    std::optional<Geometry> geometry_;
    FeatureFlags features_{};
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<Retained> ring_;
    std::uint64_t floor_ = 0;
    std::uint64_t watermark_ = 0;
    std::uint64_t reset_ = 0;
    bool closed_ = false;
    bool subscribed_ = false;
    std::exception_ptr error_;
    mutable ChunkState before_scratch_, after_scratch_;
    mutable std::vector<std::uint8_t> value_scratch_;
    std::vector<std::weak_ptr<const std::function<void()>>> notifications_;
    void Notify();
};

// The store owns this registry independently of subscriptions. Registered nodes
// stay alive until the store closes, including across feed End/Resume cycles.
class FeedProducerRegistry : public std::enable_shared_from_this<FeedProducerRegistry> {
  public:
    FeedProducerRegistry();
    ~FeedProducerRegistry();
    [[nodiscard]] bool Completed(std::atomic<std::uint64_t>& clock, std::uint64_t through) const;
  private:
    friend class ChangeFeed;
    friend class FeedWriteGuard;
    friend struct BackupTestAccess;
    friend struct FeedTestAccess;
    ChangeFeed::Producer& ThreadProducer();
    const std::uint64_t id_;
    std::atomic<ChangeFeed::Producer*> head_{nullptr};
    std::atomic<FeedTestHook*> hook_{nullptr};
};

// Every mutation publishes its own producer bound through postcommit work.
// A table lease pins the optional capture feed for the guard lifetime.
class FeedWriteGuard {
  public:
    explicit FeedWriteGuard(ChunkStore& store);
    ~FeedWriteGuard();
    FeedWriteGuard(const FeedWriteGuard&) = delete;
    FeedWriteGuard& operator=(const FeedWriteGuard&) = delete;
    void Before(ChunkCoord coord, const std::vector<std::uint8_t>& payload,
                const std::vector<std::uint8_t>& presence, const ChunkVars& vars) {
        if (state_ != nullptr && !state_->dropped) CopyBefore(coord, payload, presence, vars.Encode());
    }
    void BeforeBlock(ChunkCoord coord, const std::vector<std::uint8_t>& payload,
                     const std::vector<std::uint8_t>& presence, const ChunkVars& vars,
                     const ChunkLayout& layout, std::size_t block) {
        if (state_ != nullptr && !state_->dropped) CopyBlock(coord, payload, presence, vars, layout, block);
    }
    void Version(std::uint64_t revision) {
        if (auto* hook = producer_->hook) hook->Run(FeedTestHook::Point::kAfterVersion, revision);
        if (state_ != nullptr) {
            state_->revision = revision;
            state_->feed->RunHook(FeedTestHook::Point::kAfterVersion, revision);
        }
    }
    void Capture(const std::vector<std::uint8_t>& batch, std::size_t frame_bytes) {
        if (state_ != nullptr && !state_->dropped) CopyFrame(batch, frame_bytes);
    }
    void Commit() noexcept { if (state_ != nullptr) Publish(); }

  private:
    // Releases a capacity charge if reserve() throws before allocating it.
    struct GrowthReservation {
        FeedWriteGuard& guard;
        std::size_t bytes;
        bool allocated = false;
        ~GrowthReservation() {
            if (!allocated) {
                guard.state_->feed->Release(bytes);
                guard.state_->write->bytes -= bytes;
            }
        }
    };
    void Start(ChangeFeed& feed, std::atomic<std::uint64_t>& clock);
    void Finish() noexcept;
    void CopyBefore(ChunkCoord coord, const std::vector<std::uint8_t>& payload,
                    const std::vector<std::uint8_t>& presence, const std::vector<std::uint8_t>& vars);
    void CopyFrame(const std::vector<std::uint8_t>& batch, std::size_t frame_bytes);
    bool Charge(std::size_t bytes) noexcept;
    ChangeFeed::RawFrame* NewFrame(ChunkCoord coord);
    void CopyBlock(ChunkCoord coord, const std::vector<std::uint8_t>& payload,
                   const std::vector<std::uint8_t>& presence, const ChunkVars& vars,
                   const ChunkLayout& layout, std::size_t block);
    bool ResizeBuffer(std::vector<std::uint8_t>& target, std::size_t size);
    bool CopyBuffer(std::vector<std::uint8_t>& target, std::span<const std::uint8_t> source);
    void Publish() noexcept;
    ChangeFeed::Producer* producer_ = nullptr;
    ChangeFeed::Producer::WriteContext* state_ = nullptr;
};

}  // namespace chunkdb
