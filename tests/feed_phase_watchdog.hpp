#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <functional>
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
        std::fprintf(stderr, "BEGIN %s pid=%ld\n", name_.c_str(), Pid());
        std::fflush(stderr);
        observer_ = std::thread([state = state_, name = name_, limit = limit_] {
            std::unique_lock lock(state->mutex);
            while (!state->done) {
                const auto generation = state->generation;
                if (state->changed.wait_for(lock, limit, [&] { return state->done || state->generation != generation; })) continue;
                std::fprintf(stderr, "TIMEOUT pid=%ld case=%s phase=%s worker=%s limit_ms=%lld\n", Pid(),
                             name.c_str(), state->phase.data(), state->worker_phase.data(), static_cast<long long>(limit.count()));
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
        std::fprintf(stderr, "END %s pid=%ld\n", name_.c_str(), Pid());
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
    inline static thread_local FeedPhaseWatchdog* active_ = nullptr;
    std::string name_;
    std::chrono::milliseconds limit_;
    FeedPhaseWatchdog* previous_;
    struct State {
        std::mutex mutex;
        std::condition_variable changed;
        std::array<char, 256> phase{"group entry"};
        std::array<char, 256> worker_phase{"none"};
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
            std::fprintf(stderr, "PHASE pid=%ld case=%s phase=%s\n", Pid(), name.c_str(), destination.data());
            std::fflush(stderr);
        }
        state->changed.notify_all();
    }
    std::shared_ptr<State> state_ = std::make_shared<State>();
    std::thread observer_;
};
} // namespace chunkdb::test
