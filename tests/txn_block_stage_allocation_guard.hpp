#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
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

// This executable is single-threaded; callbacks only read counters and update
// this fixed ledger, never allocate or call back into storage.
struct UpstreamRequests {
    struct Request { void* memory = nullptr; std::size_t bytes = 0U; bool live = false; };
    std::array<Request, 4096U> requests{};
    const std::size_t* private_bytes = nullptr;
    const std::atomic<std::size_t>* global_bytes = nullptr;
    std::size_t count = 0U, live = 0U, peak = 0U, fail_at = 0U;
    bool active = false;
    bool exact = false;
    void Start(const std::size_t& local, const std::atomic<std::size_t>& global,
        std::size_t failure = 0U, bool exact_admission = false) noexcept {
        assert(!active && live == 0U);
        count = 0U; peak = 0U; fail_at = failure;
        private_bytes = &local; global_bytes = &global; active = true;
        exact = exact_admission;
    }
    void Before(std::size_t bytes) const {
        if (!active) return;
        assert(live <= *private_bytes && bytes <= *private_bytes - live);
        if (exact) assert(bytes == *private_bytes - live);
        const auto global = global_bytes->load();
        assert(live <= global && bytes <= global - live);
        if (exact) assert(global == *private_bytes);
        if (count + 1U == fail_at) throw std::bad_alloc();
    }
    void Allocated(void* memory, std::size_t bytes) noexcept {
        if (!active) return;
        assert(count < requests.size());
        requests[count++] = {memory, bytes, true};
        live += bytes; peak = std::max(peak, *private_bytes);
    }
    void Freeing(void* memory) noexcept {
        if (!active) return;
        for (std::size_t i = count; i != 0U; --i) {
            auto& request = requests[i - 1U];
            if (request.memory != memory || !request.live) continue;
            assert(*private_bytes >= live && global_bytes->load() >= live);
            request.live = false; live -= request.bytes;
            return;
        }
    }
    [[nodiscard]] std::size_t Contains(const void* pointer) const noexcept {
        const auto address = reinterpret_cast<std::uintptr_t>(pointer);
        for (std::size_t i = 0U; i < count; ++i) {
            const auto begin = reinterpret_cast<std::uintptr_t>(requests[i].memory);
            if (requests[i].live && address >= begin && address - begin < requests[i].bytes) return i;
        }
        assert(false && "no actual upstream request contains retained storage");
        return requests.size();
    }
    void Pause() noexcept { active = false; }
};
inline UpstreamRequests upstream;

inline void* Allocate(std::size_t bytes, std::size_t requested_alignment) {
    if (bytes > largest_allowed.load(std::memory_order_relaxed)) throw std::bad_alloc();
    upstream.Before(bytes);
    const auto alignment = std::max(requested_alignment, alignof(void*));
    const auto overhead = alignment - 1U + sizeof(void*);
    if (bytes > std::numeric_limits<std::size_t>::max() - overhead) throw std::bad_alloc();
    auto* allocation = std::malloc((bytes == 0U ? 1U : bytes) + overhead);
    if (!allocation) throw std::bad_alloc();
    const auto start = reinterpret_cast<std::uintptr_t>(allocation) + sizeof(void*);
    auto* result = reinterpret_cast<void*>((start + alignment - 1U) & ~(alignment - 1U));
    *(static_cast<void**>(result) - 1) = allocation;
    upstream.Allocated(result, bytes);
    return result;
}
inline void Free(void* memory) noexcept {
    if (!memory) return;
    upstream.Freeing(memory);
    std::free(*(static_cast<void**>(memory) - 1));
}
}  // namespace txn_block_allocation

#if defined(__GNUC__)
__attribute__((noinline))
#endif
void* operator new(std::size_t bytes) { return txn_block_allocation::Allocate(bytes, alignof(std::max_align_t)); }
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void operator delete(void* memory) noexcept { txn_block_allocation::Free(memory); }
void* operator new[](std::size_t bytes) { return ::operator new(bytes); }
void operator delete[](void* memory) noexcept { ::operator delete(memory); }
void operator delete(void* memory, std::size_t) noexcept { ::operator delete(memory); }
void operator delete[](void* memory, std::size_t) noexcept { ::operator delete(memory); }
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void* operator new(std::size_t bytes, std::align_val_t alignment) {
    return txn_block_allocation::Allocate(bytes, static_cast<std::size_t>(alignment));
}
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void operator delete(void* memory, std::align_val_t) noexcept { txn_block_allocation::Free(memory); }
void* operator new[](std::size_t bytes, std::align_val_t alignment) { return ::operator new(bytes, alignment); }
void operator delete[](void* memory, std::align_val_t alignment) noexcept { ::operator delete(memory, alignment); }
void operator delete(void* memory, std::size_t, std::align_val_t alignment) noexcept { ::operator delete(memory, alignment); }
void operator delete[](void* memory, std::size_t, std::align_val_t alignment) noexcept { ::operator delete(memory, alignment); }
void* operator new(std::size_t bytes, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    try { return ::operator new(bytes, alignment); } catch (const std::bad_alloc&) { return nullptr; }
}
void* operator new[](std::size_t bytes, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    return ::operator new(bytes, alignment, std::nothrow);
}
void operator delete(void* memory, std::align_val_t alignment, const std::nothrow_t&) noexcept { ::operator delete(memory, alignment); }
void operator delete[](void* memory, std::align_val_t alignment, const std::nothrow_t&) noexcept { ::operator delete(memory, alignment); }

namespace txn_block_allocation {
inline void CalibrateAligned() {
    bool refused = false;
    {
        Guard bounded(128U);
        try { (void)::operator new(1024U, std::align_val_t{128U}); } catch (const std::bad_alloc&) { refused = true; }
    }
    assert(refused);
    auto* memory = ::operator new(1024U, std::align_val_t{128U});
    assert(reinterpret_cast<std::uintptr_t>(memory) % 128U == 0U);
    ::operator delete(memory, std::align_val_t{128U});
}
}  // namespace txn_block_allocation
