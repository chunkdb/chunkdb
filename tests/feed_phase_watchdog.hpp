#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <windows.h>
#include <process.h>
#else
#include <unistd.h>
#endif

namespace chunkdb::test {
// Only the thread running a group publishes phases. The observer owns no
// server pointers and leaves production deadlines and scheduling unchanged.
class FeedPhaseWatchdog {
  public:
    explicit FeedPhaseWatchdog(std::string name,
                              std::chrono::milliseconds limit = std::chrono::seconds(60))
        : name_(std::move(name)), limit_(limit), previous_(active_) {
        std::fprintf(stderr, "BEGIN %s pid=%ld at=%s\n", name_.c_str(), Pid(), HostClocks::Now().utc.data());
        std::fflush(stderr);
        observer_ = std::thread([state = state_, name = name_, limit = limit_] {
            std::unique_lock lock(state->mutex);
            while (!state->done) {
                const auto generation = state->generation;
                if (state->changed.wait_for(lock, limit, [&] { return state->done || state->generation != generation; })) continue;
                const auto now = HostClocks::Now();
                std::fprintf(stderr, "TIMEOUT pid=%ld case=%s phase=%s worker=%s limit_ms=%lld last_change=%s now=%s "
                             "waited_ms=%lld suspended_ms=%s\n", Pid(), name.c_str(), state->phase.data(),
                             state->worker_phase.data(), static_cast<long long>(limit.count()),
                             state->changed_at.utc.data(), now.utc.data(),
                             static_cast<long long>(now.total_ms - state->changed_at.total_ms),
                             now.Suspended(state->changed_at).c_str());
                std::fflush(stderr);
                std::_Exit(124);
            }
        });
        active_ = this;
    }
    FeedPhaseWatchdog(const FeedPhaseWatchdog&) = delete;
    FeedPhaseWatchdog& operator=(const FeedPhaseWatchdog&) = delete;
    ~FeedPhaseWatchdog() {
        active_ = previous_;
        { std::lock_guard lock(state_->mutex); state_->done = true; }
        state_->changed.notify_all();
        observer_.join();
        std::fprintf(stderr, "END %s pid=%ld at=%s\n", name_.c_str(), Pid(), HostClocks::Now().utc.data());
        std::fflush(stderr);
    }
    static void Phase(std::string_view phase) {
        if (active_) Publish(active_->state_, active_->name_, phase);
    }
    // Worker observers retain only a weak phase state, never a watchdog or
    // server pointer. A callback after group teardown is a harmless no-op.
    static std::function<void(std::string_view)> Reporter() {
        if (!active_) return [](std::string_view) {};
        return [state = std::weak_ptr<State>(active_->state_), name = active_->name_](std::string_view phase) {
            if (auto locked = state.lock()) Publish(locked, name, phase, true);
        };
    }
    static std::string_view Verb(std::string_view request) {
        const auto token = request.substr(0, request.find_first_of(" \t\r\n"));
        constexpr std::string_view verbs[]{"HELLO", "AUTH", "PING", "WATCH", "UNWATCH", "ACK",
            "CREATE", "DROP", "ALTER", "GRANT", "REVOKE", "MIGRATE", "BACKUP", "SHOW", "DESCRIBE",
            "SET", "GET", "UNSET", "SCAN", "FLUSH", "TXN", "BEGIN", "COMMIT", "ROLLBACK", "DISCARD"};
        for (const auto verb : verbs) if (token == verb) return verb;
        return "<bytes>"; // Raw parameters and binary fragments must never reach diagnostics.
    }
    static void Command(std::string_view action, std::string_view request) {
        Phase(std::string(action) + " " + std::string(Verb(request)));
    }
    static void StalledControl() {
        FeedPhaseWatchdog watchdog("PhaseWatchdogStall", std::chrono::milliseconds(100));
        Phase("control: waiting for unreleased CV");
        std::mutex mutex;
        std::condition_variable never;
        std::unique_lock lock(mutex);
        never.wait(lock, [] { return false; });
    }
    static void SuppressWindowsDialogs() {
#ifdef _WIN32
        SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
        _set_error_mode(_OUT_TO_STDERR);
#endif
    }
  private:
    static long Pid() {
#ifdef _WIN32
        return static_cast<long>(_getpid());
#else
        return static_cast<long>(getpid());
#endif
    }
    // When a phase began, on two clocks: `running_ms` stops while the host is
    // suspended (asleep), `total_ms` does not, so their difference over a wait
    // is the time the host slept. A timeout with suspended_ms near waited_ms
    // is the host, not the test. A host that freezes without suspending, or a
    // paused VM, advances both clocks; only other processes' timestamps tell
    // that apart from a hang of this one.
    struct HostClocks {
        std::int64_t running_ms = 0;
        std::int64_t total_ms = 0;
        std::array<char, 32> utc{};
        static HostClocks Now() {
            HostClocks clocks;
#if defined(__APPLE__)
            clocks.running_ms = Milliseconds(CLOCK_UPTIME_RAW);
            clocks.total_ms = Milliseconds(CLOCK_MONOTONIC_RAW);
#elif defined(__linux__)
            clocks.running_ms = Milliseconds(CLOCK_MONOTONIC);
            clocks.total_ms = Milliseconds(CLOCK_BOOTTIME);
#else
            clocks.total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            clocks.running_ms = -1;
#endif
            const auto wall = std::chrono::system_clock::now();
            const std::time_t seconds = std::chrono::system_clock::to_time_t(wall);
            std::tm utc{};
#ifdef _WIN32
            gmtime_s(&utc, &seconds);
#else
            gmtime_r(&seconds, &utc);
#endif
            const auto millis = static_cast<unsigned>(std::chrono::duration_cast<std::chrono::milliseconds>(
                wall.time_since_epoch()).count() % 1000);
            // strftime bounds the date part by the buffer, so the compiler
            // need not prove the field widths; ".mmmZ" fills the rest.
            const std::size_t length = std::strftime(clocks.utc.data(), clocks.utc.size(), "%Y-%m-%dT%H:%M:%S", &utc);
            const char fraction[] = {'.', static_cast<char>('0' + millis / 100), static_cast<char>('0' + millis / 10 % 10),
                                     static_cast<char>('0' + millis % 10), 'Z', '\0'};
            if (length != 0 && length + sizeof(fraction) <= clocks.utc.size()) {
                std::copy(std::begin(fraction), std::end(fraction), clocks.utc.begin() + static_cast<std::ptrdiff_t>(length));
            }
            return clocks;
        }
        [[nodiscard]] std::string Suspended(const HostClocks& since) const {
            if (running_ms < 0 || since.running_ms < 0) return "unknown";
            return std::to_string((total_ms - since.total_ms) - (running_ms - since.running_ms));
        }
#if defined(__APPLE__) || defined(__linux__)
        static std::int64_t Milliseconds(clockid_t clock) {
            timespec value{};
            if (clock_gettime(clock, &value) != 0) return -1;
            return static_cast<std::int64_t>(value.tv_sec) * 1000 + value.tv_nsec / 1000000;
        }
#endif
    };
    inline static thread_local FeedPhaseWatchdog* active_ = nullptr;
    std::string name_;
    std::chrono::milliseconds limit_;
    FeedPhaseWatchdog* previous_;
    struct State {
        std::mutex mutex;
        std::condition_variable changed;
        std::array<char, 256> phase{"group entry"};
        std::array<char, 256> worker_phase{"none"};
        HostClocks changed_at = HostClocks::Now();
        std::size_t generation = 0U;
        bool done = false;
    };
    static void Publish(const std::shared_ptr<State>& state, const std::string& name, std::string_view phase,
                        bool worker = false) {
        {
            std::lock_guard lock(state->mutex);
            if (state->done) return;
            auto& destination = worker ? state->worker_phase : state->phase;
            std::snprintf(destination.data(), destination.size(), "%.*s",
                          static_cast<int>(std::min(phase.size(), destination.size() - 1U)), phase.data());
            ++state->generation;
            state->changed_at = HostClocks::Now();
            std::fprintf(stderr, "PHASE pid=%ld case=%s phase=%s\n", Pid(), name.c_str(), destination.data());
            std::fflush(stderr);
        }
        state->changed.notify_all();
    }
    std::shared_ptr<State> state_ = std::make_shared<State>();
    std::thread observer_;
};
} // namespace chunkdb::test
