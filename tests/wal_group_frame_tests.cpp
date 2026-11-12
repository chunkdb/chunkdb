#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <vector>

#include "wal_writer.hpp"

namespace {
std::atomic<bool> fail_allocation{false};
std::atomic<std::size_t> remaining_allocations{0U};

void* Allocate(std::size_t bytes) {
    if (fail_allocation.load(std::memory_order_relaxed) &&
        remaining_allocations.fetch_sub(1U, std::memory_order_relaxed) == 0U) {
        fail_allocation.store(false, std::memory_order_relaxed);
        throw std::bad_alloc();
    }
    if (auto* result = std::malloc(bytes == 0U ? 1U : bytes)) return result;
    throw std::bad_alloc();
}
}  // namespace

#if defined(__GNUC__)
__attribute__((noinline))
#endif
void* operator new(std::size_t bytes) { return Allocate(bytes); }
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void operator delete(void* memory) noexcept { std::free(memory); }
void* operator new[](std::size_t bytes) { return ::operator new(bytes); }
void operator delete[](void* memory) noexcept { ::operator delete(memory); }
void operator delete(void* memory, std::size_t) noexcept { ::operator delete(memory); }
void operator delete[](void* memory, std::size_t) noexcept { ::operator delete(memory); }

#include "aligned_allocation_replacement.hpp"

int main() {
    remaining_allocations = 0U; fail_allocation = true;
    assert(aligned_allocation_test::Refused(1024U) && !fail_allocation.load());
    aligned_allocation_test::Succeeds(1024U);
    const chunkdb::WalGroupRef group{{1U}, 2U, 0U};
    std::size_t failed = 0U;
    std::size_t succeeded = 0U;
    for (std::size_t allowed = 0U; allowed < 4U; ++allowed) {
        std::vector<std::uint8_t> batch{0x11U, 0x22U};
        const auto before = batch;
        remaining_allocations.store(allowed, std::memory_order_relaxed);
        fail_allocation.store(true, std::memory_order_relaxed);
        try {
            chunkdb::WalFrameBuilder frame(&batch, 1U, {}, {}, false, group);
            fail_allocation.store(false, std::memory_order_relaxed);
            const std::uint8_t value = 7U;
            frame.AppendSpan(0U, &value, 1U);
            (void)frame.Finish(3U, 9U);
            assert(batch[0] == before[0] && batch[1] == before[1]);
            ++succeeded;
        } catch (const std::bad_alloc&) {
            fail_allocation.store(false, std::memory_order_relaxed);
            assert(batch == before);
            ++failed;
        }
    }
    assert(failed != 0U && succeeded != 0U);
    std::vector<std::uint8_t> aliased{1U, 2U};
    const auto before = aliased;
    bool refused = false;
    try { chunkdb::WalFrameBuilder frame(&aliased, 1U, aliased, {}, false, group); }
    catch (const std::invalid_argument&) { refused = true; }
    assert(refused && aliased == before);
    for (const auto offset : {0U, 1U}) {
        const std::string_view user(reinterpret_cast<const char*>(aliased.data() + offset), aliased.size() - offset);
        refused = false;
        try { chunkdb::WalFrameBuilder frame(&aliased, 1U, {}, user, false, group); }
        catch (const std::invalid_argument&) { refused = true; }
        assert(refused && aliased == before);
    }
}
