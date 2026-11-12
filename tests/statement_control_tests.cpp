#include "statement_control.hpp"

#include <atomic>
#include <barrier>
#include <cassert>
#include <future>
#include <limits>
#include <thread>

namespace chunkdb {
struct StatementControlTestAccess {
    static bool TryStateLock(StatementControl& control) {
        if (!control.mutex_.try_lock()) return false;
        control.mutex_.unlock();
        return true;
    }
};
}  // namespace chunkdb

namespace {
using namespace chunkdb;
using Control = StatementControl;
using Clock = Control::Clock;
using namespace std::chrono_literals;
Clock::time_point simulated_now;
Clock::time_point SimulatedNow() noexcept { return simulated_now; }

template <typename Function>
void Stopped(Function&& function, StatementStopReason reason) {
    bool refused = false;
    try { function(); }
    catch (const StatementStoppedError& error) { refused = error.reason() == reason; }
    assert(refused);
}

void QueueStopAndDeadline() {
    const Clock::time_point admitted{};
    Control queued(admitted, 0U);
    assert(queued.RequestStop(StatementStopReason::kCancelled));
    assert(!queued.Start());
    assert(queued.TakeQueuedStop() == StatementStopReason::kCancelled);
    assert(!queued.TakeQueuedStop());
    assert(!queued.RequestStop(StatementStopReason::kKilled));

    simulated_now = admitted + 9ms;
    Control timed(admitted, 10U, SimulatedNow);
    assert(!timed.RequestStop(StatementStopReason::kTimeout));
    simulated_now = admitted + 10ms;
    assert(!timed.RequestStop(StatementStopReason::kCancelled));
    assert(timed.TakeQueuedStop() == StatementStopReason::kTimeout);

    simulated_now = admitted + 9ms;
    Control running(admitted, 10U, SimulatedNow);
    assert(running.Start());
    simulated_now = admitted + 10ms;
    Stopped([&] { running.EnterDecision(); }, StatementStopReason::kTimeout);
    simulated_now = admitted + 11ms;
    Stopped([&] { running.Check(); }, StatementStopReason::kTimeout);
}

void IrreversibleDecisionKeepsItsOutcome() {
    const Clock::time_point admitted{};
    simulated_now = admitted;
    Control control(admitted, 10U, SimulatedNow);
    assert(control.Start());
    simulated_now = admitted + 9ms;
    control.EnterDecision();
    simulated_now = admitted + 10ms;
    assert(!control.RequestStop(StatementStopReason::kCancelled));
    assert(!control.RequestStop(StatementStopReason::kKilled));
    control.Check();
    control.EnterDecision();
    assert(control.phase() == Control::Phase::kDecisionEntered);
    assert(!control.Complete());
    assert(control.phase() == Control::Phase::kComplete);
}

void CompletionAndCancelArbitrate() {
    Control cancelled(Clock::now(), 0U);
    assert(cancelled.Start());
    assert(cancelled.RequestStop(StatementStopReason::kCancelled));
    assert(cancelled.Complete() == StatementStopReason::kCancelled);
    for (std::size_t run = 0U; run < 64U; ++run) {
        Control control(Clock::now(), 0U);
        assert(control.Start());
        control.Check();
        std::barrier ready(2);
        std::atomic<StatementStopReason> result{StatementStopReason::kNone};
        std::thread reader([&] {
            ready.arrive_and_wait();
            result.store(control.Complete().value_or(StatementStopReason::kNone));
        });
        ready.arrive_and_wait();
        const auto cancelled = control.RequestStop(StatementStopReason::kCancelled);
        reader.join();
        assert(cancelled == (result.load() == StatementStopReason::kCancelled));
    }
    simulated_now = Clock::time_point{};
    Control timed(simulated_now, 10U, SimulatedNow);
    assert(timed.Start());
    timed.Check();
    simulated_now += 10ms;
    assert(timed.Complete() == StatementStopReason::kTimeout);
}

struct ClockGate {
    Clock::time_point value{};
    std::atomic<bool> armed{false};
    std::promise<void> entered, release;
};
ClockGate* decision_clock = nullptr;
Clock::time_point DecisionClock() noexcept {
    auto& clock = *decision_clock;
    if (clock.armed.exchange(false)) {
        clock.entered.set_value();
        clock.release.get_future().wait();
    }
    return clock.value;
}

void DecisionSamplesClockUnderArbitrationLock() {
    ClockGate clock;
    decision_clock = &clock;
    Control control(clock.value, 10U, DecisionClock);
    assert(control.Start());
    clock.armed.store(true);
    std::atomic<bool> timed_out{false};
    std::thread writer([&] {
        try { control.EnterDecision(); }
        catch (const StatementStoppedError& error) { timed_out.store(error.reason() == StatementStopReason::kTimeout); }
    });
    assert(clock.entered.get_future().wait_for(10s) == std::future_status::ready);
    assert(!StatementControlTestAccess::TryStateLock(control));
    clock.value += 10ms;
    clock.release.set_value();
    writer.join();
    assert(timed_out.load() && control.phase() == Control::Phase::kRunning);
    assert(control.Complete() == StatementStopReason::kTimeout);
    decision_clock = nullptr;
}

void DecisionAndCancelArbitrate() {
    for (std::size_t run = 0U; run < 64U; ++run) {
        Control control(Clock::now(), 0U);
        assert(control.Start());
        std::barrier ready(2);
        std::atomic<bool> cancelled{false}, decided{false};
        std::thread writer([&] {
            ready.arrive_and_wait();
            try { control.EnterDecision(); decided.store(true); }
            catch (const StatementStoppedError& error) { assert(error.reason() == StatementStopReason::kCancelled); }
        });
        ready.arrive_and_wait();
        cancelled.store(control.RequestStop(StatementStopReason::kCancelled));
        writer.join();
        assert(cancelled.load() != decided.load());
    }
}

void StopCallbacksRunOutsideAdmissionLock() {
    Control control(Clock::now(), 0U);
    assert(control.Start());
    std::atomic<bool> called{false};
    std::stop_callback callback(control.StopToken(), [&] {
        assert(control.phase() == Control::Phase::kRunning);
        called.store(true);
    });
    assert(control.RequestStop(StatementStopReason::kKilled));
    assert(called.load());
    Stopped([&] { control.Check(); }, StatementStopReason::kKilled);

    Control stopped(Clock::now(), 0U);
    assert(stopped.RequestStop(StatementStopReason::kCancelled));
    assert(stopped.StopToken().stop_requested());
}

void ScopedContextAndOverflow() {
    Control control(Clock::now(), 0U);
    assert(control.Start());
    {
        ScopedStatementControl context(&control);
        assert(control.RequestStop(StatementStopReason::kCancelled));
        Stopped([] { CheckStatementCancellation(); }, StatementStopReason::kCancelled);
        {
            ScopedStatementControl nested(nullptr);
            CheckStatementCancellation();
            assert(!StatementStopToken().stop_possible());
        }
        Stopped([] { EnterStatementDecision(); }, StatementStopReason::kCancelled);
    }
    CheckStatementCancellation();
    assert(!StatementStopToken().stop_possible());
    for (const auto timeout : {Control::kMaxTimeoutMs + 1U, std::numeric_limits<std::uint64_t>::max()}) {
        bool rejected = false;
        try { Control invalid(Clock::now(), timeout); }
        catch (const std::invalid_argument&) { rejected = true; }
        assert(rejected);
    }
    bool overflow = false;
    try { Control invalid(Clock::time_point::max(), 1U); }
    catch (const std::invalid_argument&) { overflow = true; }
    assert(overflow);
}
}  // namespace

int main() {
    QueueStopAndDeadline();
    IrreversibleDecisionKeepsItsOutcome();
    DecisionAndCancelArbitrate();
    CompletionAndCancelArbitrate();
    DecisionSamplesClockUnderArbitrationLock();
    StopCallbacksRunOutsideAdmissionLock();
    ScopedContextAndOverflow();
}
