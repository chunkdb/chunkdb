#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#ifdef _WIN32
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
        observer_ = std::thread([this] {
            std::unique_lock lock(mutex_);
            while (!done_) {
                const auto generation = generation_;
                if (changed_.wait_for(lock, limit_, [&] { return done_ || generation_ != generation; })) continue;
                std::fprintf(stderr, "TIMEOUT pid=%ld case=%s phase=%s limit_ms=%lld\n", Pid(),
                             name_.c_str(), phase_.data(), static_cast<long long>(limit_.count()));
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
        { std::lock_guard lock(mutex_); done_ = true; }
        changed_.notify_all();
        observer_.join();
        std::fprintf(stderr, "END %s pid=%ld\n", name_.c_str(), Pid());
        std::fflush(stderr);
    }
    static void Phase(std::string_view phase) {
        if (!active_) return;
        {
            std::lock_guard lock(active_->mutex_);
            std::snprintf(active_->phase_.data(), active_->phase_.size(), "%.*s",
                          static_cast<int>(std::min(phase.size(), active_->phase_.size() - 1U)), phase.data());
            ++active_->generation_;
            std::fprintf(stderr, "PHASE pid=%ld case=%s phase=%s\n", Pid(),
                         active_->name_.c_str(), active_->phase_.data());
            std::fflush(stderr);
        }
        active_->changed_.notify_all();
    }
    static void Command(std::string_view action, std::string_view request) {
        const auto size = std::min(request.find_first_of(" \t\r\n"), std::size_t{32});
        Phase(std::string(action) + " " + std::string(request.substr(0, size)));
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
    std::array<char, 256> phase_{"group entry"};
    std::chrono::milliseconds limit_;
    FeedPhaseWatchdog* previous_;
    std::mutex mutex_;
    std::condition_variable changed_;
    std::thread observer_;
    std::size_t generation_ = 0;
    bool done_ = false;
};
} // namespace chunkdb::test
