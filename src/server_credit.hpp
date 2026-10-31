#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>

namespace chunkdb::server_detail {
// Reservations charge retained input through all ownership transfers.
class ConnectionByteBudget : public std::enable_shared_from_this<ConnectionByteBudget> {
  public:
    class Credit {
      public:
        ~Credit() {
            if (!held_) return;
            {
                std::lock_guard lock(pool_->mutex_);
                pool_->used_ -= bytes_;
            }
            pool_->wake_();
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
        if (bytes > limit_ - used_) return {};
        used_ += bytes;
        credit->held_ = true;
        return credit;
    }
  private:
    const std::size_t limit_;
    std::function<void()> wake_;
    std::mutex mutex_;
    std::size_t used_ = 0U;
};
}  // namespace chunkdb::server_detail
