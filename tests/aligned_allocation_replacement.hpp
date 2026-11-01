#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>

// Included after a test's ordinary replacement new/delete definitions.
// Backing storage uses that same matched family on POSIX and MinGW, so its
// existing refusal/accounting hook also observes PMR aligned requests.
// No CRT-aligned allocation is paired with another runtime's free operation.
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void* operator new(std::size_t bytes, std::align_val_t requested) {
    const auto alignment = std::max(static_cast<std::size_t>(requested), alignof(void*));
    const auto overhead = alignment - 1U + sizeof(void*);
    if (bytes > std::numeric_limits<std::size_t>::max() - overhead) throw std::bad_alloc();
    auto* allocation = ::operator new(bytes + overhead);
    const auto start = reinterpret_cast<std::uintptr_t>(allocation) + sizeof(void*);
    const auto aligned = (start + alignment - 1U) & ~(alignment - 1U);
    auto* result = reinterpret_cast<void*>(aligned);
    *(static_cast<void**>(result) - 1) = allocation;
    return result;
}
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void operator delete(void* pointer, std::align_val_t) noexcept {
    if (pointer) ::operator delete(*(static_cast<void**>(pointer) - 1));
}
void* operator new[](std::size_t bytes, std::align_val_t alignment) { return ::operator new(bytes, alignment); }
void operator delete[](void* pointer, std::align_val_t alignment) noexcept { ::operator delete(pointer, alignment); }
void operator delete(void* pointer, std::size_t, std::align_val_t alignment) noexcept { ::operator delete(pointer, alignment); }
void operator delete[](void* pointer, std::size_t, std::align_val_t alignment) noexcept { ::operator delete(pointer, alignment); }
void* operator new(std::size_t bytes, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    try { return ::operator new(bytes, alignment); } catch (const std::bad_alloc&) { return nullptr; }
}
void* operator new[](std::size_t bytes, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    return ::operator new(bytes, alignment, std::nothrow);
}
void operator delete(void* pointer, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    ::operator delete(pointer, alignment);
}
void operator delete[](void* pointer, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    ::operator delete(pointer, alignment);
}

namespace aligned_allocation_test {
inline constexpr auto kAlignment = std::align_val_t{128U};
inline bool Refused(std::size_t bytes) {
    try {
        auto* memory = ::operator new(bytes, kAlignment);
        ::operator delete(memory, kAlignment);
    } catch (const std::bad_alloc&) { return true; }
    return false;
}
inline void Succeeds(std::size_t bytes) {
    auto* memory = ::operator new(bytes, kAlignment);
    assert(reinterpret_cast<std::uintptr_t>(memory) % 128U == 0U);
    auto* observed = static_cast<volatile std::byte*>(memory);
    if (bytes != 0U) { observed[0] = std::byte{1U}; observed[bytes - 1U] = std::byte{2U}; }
    ::operator delete(memory, kAlignment);
}
}  // namespace aligned_allocation_test
