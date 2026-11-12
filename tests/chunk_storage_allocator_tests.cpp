#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

#include "chunk_vars_storage.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::detail;

struct Account final : ChunkStorageAccount {
    std::size_t bytes = 0U;
    std::size_t calls = 0U;
    std::size_t limit = std::numeric_limits<std::size_t>::max();
    void Reserve(std::size_t amount) override {
        ++calls;
        if (bytes > limit || amount > limit - bytes) throw std::invalid_argument("test storage quota");
        bytes += amount;
    }
    void Release(std::size_t amount) noexcept override {
        assert(amount <= bytes);
        bytes -= amount;
    }
};

struct Upstream final : std::pmr::memory_resource {
    Account& account;
    std::size_t live = 0U;
    bool fail = false;
    bool charged = true;
    explicit Upstream(Account& owner) : account(owner) {}
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        // Admission must already include the complete request before forwarding it.
        if (charged) assert(account.bytes >= live + bytes);
        if (std::exchange(fail, false)) throw std::bad_alloc();
        auto* memory = std::pmr::new_delete_resource()->allocate(bytes, alignment);
        live += bytes;
        return memory;
    }
    void do_deallocate(void* memory, std::size_t bytes, std::size_t alignment) override {
        assert(bytes <= live);
        if (charged) assert(account.bytes >= live);
        std::pmr::new_delete_resource()->deallocate(memory, bytes, alignment);
        live -= bytes;
    }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
};

void OrdinaryCopiesAndActualExtents() {
    ChunkStorageAllocator<std::uint8_t> first;
    auto second = first;  // Equal before either allocator has allocated.
    assert(first == second && first.policy() == nullptr);
    auto* bytes = first.allocate(13U);
    assert(first == second && first.policy() == nullptr);
    assert(ChunkStorageAllocationBytes(bytes) > 13U);
    std::fill_n(bytes, 13U, std::uint8_t{7U});
    second.deallocate(bytes, 13U);
    struct alignas(128) Aligned { std::array<std::uint8_t, 5U> bytes; };
    ChunkStorageAllocator<Aligned> aligned;
    auto* value = aligned.allocate(1U);
    assert(reinterpret_cast<std::uintptr_t>(value) % alignof(Aligned) == 0U);
    aligned.deallocate(value, 1U);

    ChunkVars empty;
    assert(ChunkVarsStorageAccess::BufferAllocationBytes(empty) == 0U);
    assert(empty.Encode().get_allocator().policy() == nullptr);
    const std::array<std::uint8_t, 3U> input{1U, 2U, 3U};
    empty.Assign({1U, 0U}, input);
    assert(ChunkVarsStorageAccess::BufferAllocationBytes(empty) > empty.Encode().capacity() +
        ChunkVarsStorageAccess::OffsetCapacityBytes(empty));
    auto owned = empty.Encode();
    static_assert(std::is_same_v<decltype(owned), ChunkVars::EncodedStorage>);
    empty.Assign({1U, 0U}, std::array<std::uint8_t, 1U>{9U});
    assert(owned != empty.Encode());
    assert(owned.get_allocator().policy() == nullptr);
    assert(empty.EncodedBytes().size() == empty.Encode().size());
}

void ExactAdmissionAndFailedAllocation() {
    auto account = std::make_shared<Account>();
    Upstream upstream(*account);
    upstream.fail = true;
    bool failed = false;
    try { (void)ChunkStoragePolicy::Create(account, &upstream); } catch (const std::bad_alloc&) { failed = true; }
    assert(failed && account->bytes == 0U && upstream.live == 0U);
    {
        auto policy = ChunkStoragePolicy::Create(account, &upstream);
        const auto control = account->bytes;
        assert(control != 0U && control == upstream.live && policy->allocation_bytes() == control);
        ChunkStorageAllocator<std::uint32_t> allocator(policy);
        upstream.fail = true;
        failed = false;
        try { (void)allocator.allocate(17U); } catch (const std::bad_alloc&) { failed = true; }
        assert(failed && account->bytes == control && upstream.live == control);
        account->limit = control;
        bool refused = false;
        try { (void)allocator.allocate(1U); } catch (const std::invalid_argument&) { refused = true; }
        assert(refused && account->bytes == control && upstream.live == control);
        account->limit = std::numeric_limits<std::size_t>::max();
        auto* values = allocator.allocate(17U);
        assert(account->bytes == control + ChunkStorageAllocationBytes(values));
        assert(policy->allocation_bytes() == account->bytes && upstream.live == account->bytes);
        allocator.deallocate(values, 17U);
        assert(account->bytes == control);
        refused = false;
        try { (void)allocator.allocate(std::numeric_limits<std::size_t>::max()); }
        catch (const std::bad_array_new_length&) { refused = true; }
        assert(refused && account->bytes == control && upstream.live == control);
    }
    assert(account->bytes == 0U && upstream.live == 0U);
}

void ChargedCloneAndCrossDeallocation() {
    auto account = std::make_shared<Account>();
    {
        auto policy = ChunkStoragePolicy::Create(account);
        const auto control = account->bytes;
        ChunkVars original;
        original.Assign({2U, 0U}, std::array<std::uint8_t, 2U>{3U, 4U});
        {
            auto prepared = ChunkVarsStorageAccess::Clone(original, policy);
            assert(prepared == original);
            assert(account->bytes == control + ChunkVarsStorageAccess::BufferAllocationBytes(prepared));
            auto ordinary_copy = prepared;
            assert(ordinary_copy == prepared && ordinary_copy.Encode().get_allocator().policy() == nullptr);
            assert(account->bytes == control + ChunkVarsStorageAccess::BufferAllocationBytes(prepared));
        }
        assert(account->bytes == control);
        {
            auto admitted = ChunkVarsStorageAccess::Clone(original, policy);
            const auto data_bytes = ChunkStorageAllocationBytes(admitted.Encode().data());
            admitted = ChunkVars{};
            assert(account->bytes == control);
            account->limit = control + data_bytes;
            bool refused = false;
            try { (void)ChunkVarsStorageAccess::Clone(original, policy); }
            catch (const std::invalid_argument&) { refused = true; }
            assert(refused && account->bytes == control && original.Find({2U, 0U})->size() == 2U);
            account->limit = std::numeric_limits<std::size_t>::max();
        }
        ChunkStorageAllocator<std::uint8_t> charged(policy), ordinary;
        assert(charged == ordinary);  // Same upstream; prefix selects the real originating owner.
        auto* bytes = charged.allocate(11U);
        ordinary.deallocate(bytes, 11U);
        assert(account->bytes == control);
        auto* uncharged = ordinary.allocate(7U);
        charged.deallocate(uncharged, 7U);
        assert(account->bytes == control);
    }
    assert(account->bytes == 0U);
    std::weak_ptr<Account> lifetime = account;
    std::uint8_t* held;
    {
        auto policy = ChunkStoragePolicy::Create(account);
        ChunkStorageAllocator<std::uint8_t> allocator(policy);
        held = allocator.allocate(3U);
    }
    account.reset();
    assert(!lifetime.expired());  // The allocation prefix owns the account through final free.
    ChunkStorageAllocator<std::uint8_t>{}.deallocate(held, 3U);
    assert(lifetime.expired());
}

void HandoffStopsFutureTransactionAdmission() {
    auto account = std::make_shared<Account>();
    std::weak_ptr<Account> lifetime = account;
    auto policy = ChunkStoragePolicy::Create(account);
    auto undo_policy = ChunkStoragePolicy::Create(account);
    ChunkVars source;
    source.Assign({1U, 0U}, std::array<std::uint8_t, 1U>{1U});
    {
        auto live = ChunkVarsStorageAccess::Clone(source, policy);
        auto undo = ChunkVarsStorageAccess::Clone(source, undo_policy);
        const auto undo_bytes = undo_policy->transaction_bytes();
        assert(account->bytes == policy->transaction_bytes() + undo_bytes);
        policy->TransferToRegularStorage();
        assert(policy->transaction_bytes() == 0U && account->bytes == undo_bytes);
        const auto calls = account->calls;
        live.Assign({1U, 1U}, std::vector<std::uint8_t>(8192U, 2U));
        assert(account->calls == calls && account->bytes == undo_bytes);
        assert(live.Encode().get_allocator().policy() == policy.get());
        auto moved = std::move(live);
        static_assert(std::is_nothrow_move_assignable_v<ChunkVars>);
        ChunkVars destination;
        destination = std::move(moved);
        assert(destination.Encode().get_allocator().policy() == policy.get());
    }
    assert(account->bytes == undo_policy->transaction_bytes());
    undo_policy = {};
    assert(account->bytes == 0U);
    account.reset();
    assert(lifetime.expired());  // The live policy must not retain an ended account.
    assert(policy->transaction_bytes() == 0U);
}
}  // namespace

int main() {
    OrdinaryCopiesAndActualExtents();
    ExactAdmissionAndFailedAllocation();
    ChargedCloneAndCrossDeallocation();
    HandoffStopsFutureTransactionAdmission();
}
