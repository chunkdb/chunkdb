#pragma once

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <limits>
#include <new>

namespace txn_block_allocation {
inline std::atomic<std::size_t> largest_allowed{std::numeric_limits<std::size_t>::max()};
class Guard {
  public:
    explicit Guard(std::size_t limit) : previous_(largest_allowed.exchange(limit)) {}
    ~Guard() { largest_allowed.store(previous_); }
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
  private:
    std::size_t previous_;
};
}

#if defined(__GNUC__)
__attribute__((noinline))
#endif
void* operator new(std::size_t bytes) {
    if (bytes > txn_block_allocation::largest_allowed.load(std::memory_order_relaxed)) throw std::bad_alloc();
    void* memory = std::malloc(bytes == 0U ? 1U : bytes);
    if (!memory) throw std::bad_alloc();
    return memory;
}
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void operator delete(void* memory) noexcept { std::free(memory); }
void* operator new[](std::size_t bytes) { return ::operator new(bytes); }
void operator delete[](void* memory) noexcept { ::operator delete(memory); }
void operator delete(void* memory, std::size_t) noexcept { ::operator delete(memory); }
void operator delete[](void* memory, std::size_t) noexcept { ::operator delete(memory); }
