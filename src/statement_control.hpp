#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <shared_mutex>

namespace chunkdb {
class RegularChunkMutex;
enum class StatementStopReason { kNone, kCancelled, kKilled, kTimeout };

class StatementStoppedError : public std::runtime_error {
  public:
    explicit StatementStoppedError(StatementStopReason reason);
    [[nodiscard]] StatementStopReason reason() const noexcept { return reason_; }
  private:
    StatementStopReason reason_;
};

// Decision admission and stopping use the same mutex, so a not-applied reply
// cannot replace an operation that has already entered its irreversible phase.
class StatementControl {
  public:
    using Clock = std::chrono::steady_clock;
    using Now = Clock::time_point (*)() noexcept;
    enum class Phase { kQueued, kRunning, kDecisionEntered, kComplete };
    inline static constexpr std::uint64_t kMaxTimeoutMs = 86400000U;
    StatementControl(Clock::time_point admitted, std::uint64_t timeout_ms, Now now = &Clock::now);
    bool Start();
    bool RequestStop(StatementStopReason reason);
    std::optional<StatementStopReason> TakeQueuedStop();
    void Check();
    void EnterDecision();
    // Result admission must observe any accepted pre-decision stop.
    std::optional<StatementStopReason> Complete();
    [[nodiscard]] Phase phase() const;
    [[nodiscard]] bool decision_entered() const;
    [[nodiscard]] std::optional<Clock::time_point> deadline() const noexcept { return deadline_; }
    [[nodiscard]] std::stop_token StopToken();
  private:
    friend struct StatementControlTestAccess;
    bool ExpireLocked();
    static std::optional<Clock::time_point> Deadline(Clock::time_point admitted, std::uint64_t timeout_ms);
    const std::optional<Clock::time_point> deadline_;
    const Now now_;
    mutable std::mutex mutex_;
    Phase phase_ = Phase::kQueued;
    StatementStopReason reason_ = StatementStopReason::kNone;
    bool decision_entered_ = false;
    std::stop_source stop_{std::nostopstate};
};

class ScopedStatementControl {
  public:
    explicit ScopedStatementControl(StatementControl* current) noexcept;
    ~ScopedStatementControl();
    ScopedStatementControl(const ScopedStatementControl&) = delete;
    ScopedStatementControl& operator=(const ScopedStatementControl&) = delete;
  private:
    StatementControl* previous_;
};

void CheckStatementCancellation();
void EnterStatementDecision();
[[nodiscard]] std::string StatementStoppedReply(StatementStopReason reason);
[[nodiscard]] std::stop_token StatementStopToken();
[[nodiscard]] std::unique_lock<RegularChunkMutex> LockStatementChunk(RegularChunkMutex& mutex);
[[nodiscard]] std::shared_lock<RegularChunkMutex> LockStatementChunkShared(RegularChunkMutex& mutex);
}  // namespace chunkdb
