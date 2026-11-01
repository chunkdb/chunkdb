#include "statement_control.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/statement_mutex.hpp"

namespace chunkdb {
namespace {
thread_local StatementControl* current_statement = nullptr;

const char* StopMessage(StatementStopReason reason) {
    switch (reason) {
        case StatementStopReason::kCancelled: return "statement cancelled before its commit decision";
        case StatementStopReason::kKilled: return "connection killed before the statement commit decision";
        case StatementStopReason::kTimeout: return "statement deadline expired before its commit decision";
        case StatementStopReason::kNone: break;
    }
    throw std::logic_error("a stopped statement needs a stop reason");
}
}  // namespace

StatementStoppedError::StatementStoppedError(StatementStopReason reason)
    : std::runtime_error(StopMessage(reason)), reason_(reason) {}

std::optional<StatementControl::Clock::time_point> StatementControl::Deadline(
    Clock::time_point admitted, std::uint64_t timeout_ms) {
    if (timeout_ms == 0U) return std::nullopt;
    if (timeout_ms > kMaxTimeoutMs) throw std::invalid_argument("statement_timeout_ms exceeds 86400000");
    const auto interval = std::chrono::duration_cast<Clock::duration>(std::chrono::milliseconds(timeout_ms));
    if (admitted > Clock::time_point::max() - interval)
        throw std::invalid_argument("statement deadline exceeds the monotonic clock range");
    return admitted + interval;
}

StatementControl::StatementControl(Clock::time_point admitted, std::uint64_t timeout_ms, Now now)
    : deadline_(Deadline(admitted, timeout_ms)), now_(now) {
    if (now_ == nullptr) throw std::invalid_argument("a statement needs a monotonic clock");
}

bool StatementControl::ExpireLocked() {
    if (phase_ >= Phase::kDecisionEntered || reason_ != StatementStopReason::kNone ||
        !deadline_) return false;
    if (now_() < *deadline_) return false;
    reason_ = StatementStopReason::kTimeout;
    return true;
}

bool StatementControl::Start() {
    std::stop_source stop(std::nostopstate);
    bool started = false;
    {
        std::lock_guard lock(mutex_);
        if (ExpireLocked()) stop = stop_;
        if (phase_ == Phase::kQueued && reason_ == StatementStopReason::kNone) {
            phase_ = Phase::kRunning;
            started = true;
        }
    }
    stop.request_stop();
    return started;
}

bool StatementControl::RequestStop(StatementStopReason reason) {
    if (reason == StatementStopReason::kNone) throw std::invalid_argument("stop reason must be specified");
    std::stop_source stop(std::nostopstate);
    bool accepted = false;
    {
        std::lock_guard lock(mutex_);
        if (phase_ >= Phase::kDecisionEntered || reason_ != StatementStopReason::kNone) return false;
        const bool expired = ExpireLocked();
        if (reason == StatementStopReason::kTimeout && !expired) return false;
        if (!expired) {
            reason_ = reason;
            accepted = true;
        } else accepted = reason == StatementStopReason::kTimeout;
        stop = stop_;
    }
    stop.request_stop();
    return accepted;
}

std::optional<StatementStopReason> StatementControl::TakeQueuedStop() {
    std::stop_source stop(std::nostopstate);
    std::optional<StatementStopReason> result;
    {
        std::lock_guard lock(mutex_);
        if (ExpireLocked()) stop = stop_;
        if (phase_ == Phase::kQueued && reason_ != StatementStopReason::kNone) {
            result = reason_;
            phase_ = Phase::kComplete;
        }
    }
    stop.request_stop();
    return result;
}

void StatementControl::Check() {
    std::stop_source stop(std::nostopstate);
    StatementStopReason reason;
    {
        std::lock_guard lock(mutex_);
        if (phase_ >= Phase::kDecisionEntered) return;
        if (ExpireLocked()) stop = stop_;
        reason = reason_;
    }
    stop.request_stop();
    if (reason != StatementStopReason::kNone) throw StatementStoppedError(reason);
}

void StatementControl::EnterDecision() {
    std::stop_source stop(std::nostopstate);
    StatementStopReason reason;
    {
        std::lock_guard lock(mutex_);
        if (phase_ == Phase::kDecisionEntered) return;
        if (phase_ == Phase::kComplete) throw std::logic_error("completed statement cannot enter a decision");
        if (phase_ == Phase::kQueued) throw std::logic_error("queued statement cannot enter a decision");
        if (ExpireLocked()) stop = stop_;
        reason = reason_;
        if (reason == StatementStopReason::kNone) phase_ = Phase::kDecisionEntered;
    }
    stop.request_stop();
    if (reason != StatementStopReason::kNone) throw StatementStoppedError(reason);
}

std::optional<StatementStopReason> StatementControl::Complete() {
    std::stop_source stop(std::nostopstate);
    std::optional<StatementStopReason> result;
    {
        std::lock_guard lock(mutex_);
        if (ExpireLocked()) stop = stop_;
        if (reason_ != StatementStopReason::kNone) result = reason_;
        phase_ = Phase::kComplete;
    }
    stop.request_stop();
    return result;
}

StatementControl::Phase StatementControl::phase() const {
    std::lock_guard lock(mutex_);
    return phase_;
}

std::stop_token StatementControl::StopToken() {
    std::stop_source stop(std::nostopstate);
    bool stopped;
    {
        std::lock_guard lock(mutex_);
        if (!stop_.stop_possible()) stop_ = std::stop_source{};
        stop = stop_;
        stopped = reason_ != StatementStopReason::kNone;
    }
    if (stopped) stop.request_stop();
    return stop.get_token();
}

ScopedStatementControl::ScopedStatementControl(StatementControl* current) noexcept
    : previous_(current_statement) { current_statement = current; }
ScopedStatementControl::~ScopedStatementControl() { current_statement = previous_; }
void CheckStatementCancellation() { if (current_statement) current_statement->Check(); }
void EnterStatementDecision() { if (current_statement) current_statement->EnterDecision(); }
std::stop_token StatementStopToken() {
    return current_statement ? current_statement->StopToken() : std::stop_token{};
}
void StatementMutex::lock() {
    std::unique_lock lock(mutex_);
    const auto stopped = StatementStopToken();
    if (!changed_.wait(lock, stopped, [this] { return !locked_; })) {
        changed_.notify_all();
        CheckStatementCancellation();
    }
    try { CheckStatementCancellation(); }
    catch (const StatementStoppedError&) { changed_.notify_all(); throw; }
    locked_ = true;
}
void StatementMutex::unlock() noexcept {
    { std::lock_guard lock(mutex_); locked_ = false; }
    changed_.notify_all();
}
std::unique_lock<RegularChunkMutex> LockStatementChunk(RegularChunkMutex& mutex) {
    if (!mutex.try_lock() && !mutex.lock(StatementStopToken())) CheckStatementCancellation();
    std::unique_lock lock(mutex, std::adopt_lock);
    CheckStatementCancellation();
    return lock;
}
std::shared_lock<RegularChunkMutex> LockStatementChunkShared(RegularChunkMutex& mutex) {
    if (!mutex.try_lock_shared() && !mutex.lock_shared(StatementStopToken())) CheckStatementCancellation();
    std::shared_lock lock(mutex, std::adopt_lock);
    CheckStatementCancellation();
    return lock;
}
std::string StatementStoppedReply(StatementStopReason reason) {
    const auto code = reason == StatementStopReason::kTimeout ? "TIMEOUT" : "CANCELLED";
    return Protocol::Error(code, StopMessage(reason), {{"applied", "false"}});
}
}  // namespace chunkdb
