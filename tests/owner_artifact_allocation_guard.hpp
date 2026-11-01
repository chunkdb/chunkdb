#pragma once

#include <atomic>
#include <cstdlib>
#include <limits>
#include <new>

namespace owner_artifact_allocation {
inline std::atomic<std::size_t> limit{std::numeric_limits<std::size_t>::max()};
inline std::atomic<std::size_t> calls{0U}, denied{0U};
class Guard {
  public:
    explicit Guard(std::size_t bytes) : previous_(limit.exchange(bytes)) {}
    ~Guard() { limit.store(previous_); }
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
  private:
    std::size_t previous_;
};
}

void* operator new(std::size_t bytes) {
    ++owner_artifact_allocation::calls;
    if (bytes > owner_artifact_allocation::limit.load()) {
        ++owner_artifact_allocation::denied;
        throw std::bad_alloc();
    }
    auto* memory = std::malloc(bytes == 0U ? 1U : bytes);
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
