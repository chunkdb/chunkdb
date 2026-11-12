#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace chunkdb::server_detail {
// Reservations charge retained input through all ownership transfers.
// A return wakes the owner only after a refused reservation: nothing waits
// for credit that no one was refused.
class ConnectionByteBudget : public std::enable_shared_from_this<ConnectionByteBudget> {
  public:
    class Credit {
      public:
        ~Credit() {
            if (!held_) return;
            bool wake = false;
            {
                std::lock_guard lock(pool_->mutex_);
                pool_->used_ -= bytes_;
                wake = std::exchange(pool_->refused_, false);
            }
            if (wake) pool_->wake_();
        }
        Credit(const Credit&) = delete;
        Credit& operator=(const Credit&) = delete;
      private:
        friend class ConnectionByteBudget;
        Credit(std::shared_ptr<ConnectionByteBudget> pool, std::size_t bytes)
            : pool_(std::move(pool)), bytes_(bytes) {}
        std::shared_ptr<ConnectionByteBudget> pool_;
        std::size_t bytes_;
        bool held_ = false;
    };
    ConnectionByteBudget(std::size_t limit, std::function<void()> wake)
        : limit_(limit), wake_(std::move(wake)) {}
    [[nodiscard]] std::shared_ptr<Credit> TryAcquire(std::size_t bytes) {
        if (bytes > limit_) throw std::invalid_argument("request exceeds the aggregate input byte bound");
        // Allocate before changing usage, so allocation failure consumes nothing.
        auto credit = std::shared_ptr<Credit>(new Credit(shared_from_this(), bytes));
        std::lock_guard lock(mutex_);
        if (bytes > limit_ - used_) { refused_ = true; return {}; }
        used_ += bytes;
        credit->held_ = true;
        return credit;
    }
    [[nodiscard]] bool Owns(const std::shared_ptr<Credit>& credit) const noexcept {
        return credit && credit->pool_.get() == this;
    }
    [[nodiscard]] bool TryResize(const std::shared_ptr<Credit>& credit, std::size_t bytes) {
        if (!Owns(credit) || bytes > limit_) return false;
        bool released = false;
        {
            std::lock_guard lock(mutex_);
            if (!credit->held_) return false;
            if (bytes > credit->bytes_) {
                const auto extra = bytes - credit->bytes_;
                if (extra > limit_ - used_) { refused_ = true; return false; }
                used_ += extra;
            } else {
                used_ -= credit->bytes_ - bytes;
                released = bytes != credit->bytes_ && std::exchange(refused_, false);
            }
            credit->bytes_ = bytes;
        }
        if (released) wake_();
        return true;
    }
  private:
    const std::size_t limit_;
    std::function<void()> wake_;
    std::mutex mutex_;
    std::size_t used_ = 0U;
    bool refused_ = false;
};
}  // namespace chunkdb::server_detail
