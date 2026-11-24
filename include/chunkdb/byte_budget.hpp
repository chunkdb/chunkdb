#pragma once

#include <atomic>
#include <cstddef>

namespace chunkdb {

// A byte limit that several owners share, such as the change feeds or the
// transaction histories of every table of a server. Each owner keeps its own
// count as well and mirrors every reservation and release here.
class ServerByteBudget {
  public:
    explicit ServerByteBudget(std::size_t limit) noexcept : limit_(limit) {}
    ServerByteBudget(const ServerByteBudget&) = delete;
    ServerByteBudget& operator=(const ServerByteBudget&) = delete;

    // Takes `bytes` when they fit under the limit; never takes part of them.
    [[nodiscard]] bool TryReserve(std::size_t bytes) noexcept {
        auto used = used_.load(std::memory_order_relaxed);
        do {
            if (bytes > limit_ || used > limit_ - bytes) return false;
        } while (!used_.compare_exchange_weak(used, used + bytes, std::memory_order_relaxed));
        return true;
    }
    void Release(std::size_t bytes) noexcept { used_.fetch_sub(bytes, std::memory_order_relaxed); }
    // Whether `bytes` more would pass the limit now.
    [[nodiscard]] bool WouldExceed(std::size_t bytes) const noexcept {
        const auto used = used_.load(std::memory_order_relaxed);
        return bytes > limit_ || used > limit_ - bytes;
    }
    [[nodiscard]] std::size_t used() const noexcept { return used_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::size_t limit() const noexcept { return limit_; }

  private:
    const std::size_t limit_;
    std::atomic<std::size_t> used_{0U};
};

}  // namespace chunkdb
