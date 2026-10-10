#pragma once

#include <condition_variable>
#include <mutex>
#include <stop_token>

namespace chunkdb {
// Maintenance never waits while holding a chunk lock. Backup can wait for
// existing maintenance, with shutdown notifying the same condition directly.
class BackupMaintenanceGate {
  public:
    void lock() {
        std::unique_lock lock(mutex_);
        ExclusiveWaiter waiter(*this);
        cv_.wait(lock, [&] { return !exclusive_ && readers_ == 0U; });
        exclusive_ = true;
    }
    [[nodiscard]] bool lock(std::stop_token cancelled) {
        std::unique_lock lock(mutex_);
        ExclusiveWaiter waiter(*this);
        const bool ready = cv_.wait(lock, cancelled, [&] { return !exclusive_ && readers_ == 0U; });
        if (!ready || cancelled.stop_requested()) return false;
        exclusive_ = true;
        return true;
    }
    bool try_lock() {
        std::lock_guard lock(mutex_);
        if (exclusive_ || readers_ != 0U) return false;
        exclusive_ = true;
        return true;
    }
    void unlock() {
        std::lock_guard lock(mutex_);
        exclusive_ = false;
        cv_.notify_all();
    }
    void lock_shared() {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [&] { return !exclusive_ && exclusive_waiters_ == 0U; });
        ++readers_;
    }
    [[nodiscard]] bool lock_shared(std::stop_token cancelled) {
        std::unique_lock lock(mutex_);
        SharedWaiter waiter(*this);
        const bool ready = cv_.wait(lock, cancelled, [&] { return !exclusive_ && exclusive_waiters_ == 0U; });
        if (!ready || cancelled.stop_requested()) return false;
        ++readers_;
        return true;
    }
    bool try_lock_shared() {
        std::lock_guard lock(mutex_);
        if (exclusive_ || exclusive_waiters_ != 0U) return false;
        ++readers_;
        return true;
    }
    void unlock_shared() {
        std::lock_guard lock(mutex_);
        --readers_;
        if (readers_ == 0U) cv_.notify_all();
    }
  private:
    friend struct BackupTestAccess;
    // Constructed and destroyed under mutex_, including cancellation and
    // exceptional waits. A queued backup closes admission to new maintenance.
    struct ExclusiveWaiter {
        explicit ExclusiveWaiter(BackupMaintenanceGate& gate) : gate_(gate) {
            ++gate_.exclusive_waiters_;
            gate_.cv_.notify_all();
        }
        ~ExclusiveWaiter() { --gate_.exclusive_waiters_; gate_.cv_.notify_all(); }
        BackupMaintenanceGate& gate_;
    };
    struct SharedWaiter {
        explicit SharedWaiter(BackupMaintenanceGate& gate) : gate_(gate) {
            ++gate_.shared_waiters_;
            gate_.cv_.notify_all();
        }
        ~SharedWaiter() { --gate_.shared_waiters_; gate_.cv_.notify_all(); }
        BackupMaintenanceGate& gate_;
    };
    std::size_t shared_waiters_ = 0;
    std::size_t exclusive_waiters_ = 0;
    std::mutex mutex_;
    std::condition_variable_any cv_;
    std::size_t readers_ = 0;
    bool exclusive_ = false;
};
} // namespace chunkdb
