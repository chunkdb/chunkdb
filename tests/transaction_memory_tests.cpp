#include "chunkdb/transaction_memory.hpp"
#include "chunkdb/engine.hpp"

#include <array>
#include <atomic>
#include <barrier>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <optional>
#include <string_view>
#include <thread>
#include <utility>

#include "txn_block_memory.hpp"
#include "txn_block_allocation_guard.hpp"

namespace {
using namespace chunkdb;
constexpr auto kUnlimited = std::numeric_limits<std::size_t>::max();
TxnMemoryBudget* diagnostic_budget = nullptr;
TxnMemoryGlobal* diagnostic_global = nullptr;
std::size_t diagnostic_private = 0U;
std::size_t diagnostic_shared = 0U;
std::atomic<std::size_t> diagnostic_requests{0U};

void ObserveDiagnostic(std::size_t) noexcept {
    assert(diagnostic_budget && diagnostic_global);
    assert(diagnostic_budget->bytes() == diagnostic_private);
    assert(diagnostic_global->bytes() == diagnostic_shared);
    diagnostic_requests.fetch_add(1U, std::memory_order_relaxed);
}

template <class Error, class Operation> void Refuses(Operation&& operation) {
    bool refused = false;
    try { operation(); } catch (const Error&) { refused = true; }
    assert(refused);
}

struct Upstream final : std::pmr::memory_resource {
    TxnMemoryGlobal& global;
    TxnMemoryBudget* budget = nullptr;
    void* ledger = nullptr;
    std::size_t live = 0U;
    std::size_t calls = 0U;
    std::size_t last_request = 0U;
    bool fail = false;
    bool charged = true;
    explicit Upstream(TxnMemoryGlobal& owner) : global(owner) {}
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        ++calls;
        last_request = bytes;
        if (charged) {
            assert(live <= global.bytes() && bytes <= global.bytes() - live);
            if (budget) assert(live <= budget->bytes() && bytes <= budget->bytes() - live);
        }
        if (std::exchange(fail, false)) throw std::bad_alloc();
        auto* memory = std::pmr::new_delete_resource()->allocate(bytes, alignment);
        if (!ledger) ledger = memory;
        live += bytes;
        return memory;
    }
    void do_deallocate(void* memory, std::size_t bytes, std::size_t alignment) override {
        assert(bytes <= live);
        // The charge still covers the request while upstream frees it.
        if (charged) {
            assert(global.bytes() >= live);
            if (budget && memory != ledger) assert(budget->bytes() >= live);
        }
        std::pmr::new_delete_resource()->deallocate(memory, bytes, alignment);
        live -= bytes;
    }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
};

std::size_t LedgerBytes() {
    auto global = std::make_shared<TxnMemoryGlobal>(kUnlimited);
    auto budget = TxnMemoryBudget::Create(global, kUnlimited);
    return budget->bytes();
}

void LedgerAdmission() {
    const auto self = LedgerBytes();
    assert(self != 0U);
    auto global = std::make_shared<TxnMemoryGlobal>(kUnlimited);
    Upstream upstream(*global);
    auto budget = TxnMemoryBudget::Create(global, self, &upstream);
    upstream.budget = budget.get();
    assert(budget->bytes() == self && global->bytes() == self && upstream.live == self);
    Refuses<TxnMemoryLimitError>([&] { budget->Reserve(1U); });
    const auto calls = upstream.calls;
    Refuses<TxnMemoryLimitError>([&] { (void)TxnMemoryBudget::Create(global, self - 1U, &upstream); });
    assert(upstream.calls == calls && budget->bytes() == self && global->bytes() == self);
    budget = {};
    assert(global->bytes() == 0U && upstream.live == 0U);
    auto exact_global = std::make_shared<TxnMemoryGlobal>(self);
    auto exact = TxnMemoryBudget::Create(exact_global, self);
    assert(exact->bytes() == self && exact_global->bytes() == self);
    exact = {};
    assert(exact_global->bytes() == 0U);
    auto short_global = std::make_shared<TxnMemoryGlobal>(self - 1U);
    Upstream unused(*short_global);
    Refuses<TxnMemoryLimitError>([&] { (void)TxnMemoryBudget::Create(short_global, self, &unused); });
    assert(short_global->bytes() == 0U && unused.calls == 0U);
    Refuses<std::invalid_argument>([] { (void)MakeTxnStorageAccount({}); });
}

void AccountControlAdmission() {
    auto global = std::make_shared<TxnMemoryGlobal>(kUnlimited);
    Upstream upstream(*global);
    auto budget = TxnMemoryBudget::Create(global, kUnlimited, &upstream);
    upstream.budget = budget.get();
    const auto self = budget->bytes();
    auto account = MakeTxnStorageAccount(budget);
    const auto control = budget->bytes() - self;
    assert(control != 0U && control == upstream.last_request && upstream.live == self + control);
    account.reset();
    assert(global->bytes() == self && budget->bytes() == self && upstream.live == self);
    budget = {};
    assert(global->bytes() == 0U && upstream.live == 0U);
    for (const auto limit : {self + control - 1U, self + control}) {
        Upstream measured(*global);
        auto current = TxnMemoryBudget::Create(global, limit, &measured);
        measured.budget = current.get();
        if (limit == self + control - 1U) {
            Refuses<TxnMemoryLimitError>([&] { (void)MakeTxnStorageAccount(current); });
            assert(measured.calls == 1U && measured.live == self && current->bytes() == self);
        } else {
            auto admitted = MakeTxnStorageAccount(current);
            assert(current->bytes() == limit && measured.live == limit);
        }
        current = {};
        assert(global->bytes() == 0U && measured.live == 0U);
    }
}

void MixedDomainsAndAbort() {
    auto global = std::make_shared<TxnMemoryGlobal>(kUnlimited);
    Upstream upstream(*global);
    auto budget = TxnMemoryBudget::Create(global, kUnlimited, &upstream);
    upstream.budget = budget.get();
    auto account = MakeTxnStorageAccount(budget);
    auto policy = detail::ChunkStoragePolicy::Create(account, &upstream);
    const auto controls = budget->bytes();
    {
        SessionTransaction transaction(budget, std::chrono::steady_clock::now());
        transaction.memory_budget->Reserve(100U);
        transaction.bytes = 100U; // Legacy logical charge, not an upstream extent.
        TxnBlockMemoryResource journal(budget);
        auto* row = journal.allocate(17U, alignof(std::uint64_t));
        detail::ChunkStorageAllocator<std::uint8_t> physical(policy);
        auto* bytes = physical.allocate(13U);
        const auto physical_extent = detail::ChunkStorageAllocationBytes(bytes);
        assert(physical_extent > 13U);
        assert(budget->bytes() == controls + 100U + 17U + physical_extent);
        assert(upstream.live == controls + 17U + physical_extent && journal.allocated_bytes() == 17U);
        transaction.Abort("conflict");
        transaction.Abort("conflict");
        assert(transaction.bytes == 0U && budget->bytes() == controls + 17U + physical_extent);
        journal.deallocate(row, 17U, alignof(std::uint64_t));
        physical.deallocate(bytes, 13U);
        assert(budget->bytes() == controls && upstream.live == controls);
    }
    assert(budget->bytes() == controls && global->bytes() == controls);
}

void UpstreamFailures() {
    auto global = std::make_shared<TxnMemoryGlobal>(kUnlimited);
    Upstream upstream(*global);
    upstream.fail = true;
    Refuses<std::bad_alloc>([&] { (void)TxnMemoryBudget::Create(global, kUnlimited, &upstream); });
    assert(global->bytes() == 0U && upstream.live == 0U);
    auto budget = TxnMemoryBudget::Create(global, kUnlimited, &upstream);
    upstream.budget = budget.get();
    const auto self = budget->bytes();
    upstream.fail = true;
    Refuses<std::bad_alloc>([&] { (void)MakeTxnStorageAccount(budget); });
    assert(budget->bytes() == self && global->bytes() == self && upstream.live == self);
    TxnBlockMemoryResource memory(budget);
    upstream.fail = true;
    Refuses<std::bad_alloc>([&] { (void)memory.allocate(31U, alignof(std::uint64_t)); });
    assert(memory.allocated_bytes() == 0U && budget->bytes() == self && global->bytes() == self && upstream.live == self);
}

void EscapedOwnerAndHandoff() {
    auto global = std::make_shared<TxnMemoryGlobal>(kUnlimited);
    std::weak_ptr<TxnMemoryGlobal> weak = global;
    Upstream upstream(*global);
    auto budget = TxnMemoryBudget::Create(global, kUnlimited, &upstream);
    upstream.budget = budget.get();
    auto account = MakeTxnStorageAccount(budget);
    auto policy = detail::ChunkStoragePolicy::Create(account, &upstream);
    std::optional<detail::ChunkStorageAllocator<std::uint8_t>> allocator(std::in_place, policy);
    auto* bytes = allocator->allocate(31U);
    account.reset(); policy = {}; budget = {}; global.reset();
    assert(!weak.expired());
    allocator->deallocate(bytes, 31U);
    assert(!weak.expired()); // The allocator still retains its policy control.
    allocator.reset();
    assert(weak.expired() && upstream.live == 0U);

    auto regular_global = std::make_shared<TxnMemoryGlobal>(kUnlimited);
    Upstream regular(*regular_global);
    auto owner = TxnMemoryBudget::Create(regular_global, kUnlimited, &regular);
    regular.budget = owner.get();
    const auto self = owner->bytes();
    auto staged = detail::ChunkStoragePolicy::Create(MakeTxnStorageAccount(owner), &regular);
    detail::ChunkStorageAllocator<std::uint8_t> storage(staged);
    auto* installed = storage.allocate(19U);
    regular.charged = false; // These exact domains now belong to regular storage.
    staged->TransferToRegularStorage();
    assert(owner->bytes() == self && regular_global->bytes() == self && staged->transaction_bytes() == 0U);
    storage.deallocate(installed, 19U);
    assert(owner->bytes() == self);
}

void WeakAccountControlLifetime() {
    auto global = std::make_shared<TxnMemoryGlobal>(kUnlimited);
    std::weak_ptr<TxnMemoryGlobal> weak_global = global;
    Upstream upstream(*global);
    auto budget = TxnMemoryBudget::Create(global, kUnlimited, &upstream);
    upstream.budget = budget.get();
    const auto self = budget->bytes();
    auto account = MakeTxnStorageAccount(budget);
    std::weak_ptr<detail::ChunkStorageAccount> weak_account = account;
    const auto control = budget->bytes() - self;
    assert(control != 0U && control == upstream.last_request);
    account.reset();
    assert(weak_account.expired());
    assert(budget->bytes() == self + control && upstream.live == self + control);
    budget = {};
    global.reset();
    {
        const auto retained = weak_global.lock();
        assert(retained && retained->bytes() == self + control);
        assert(upstream.live == self + control);
    }
    weak_account.reset();
    assert(weak_global.expired() && upstream.live == 0U);
}

void PrivateContention() {
    constexpr std::size_t kWorkers = 8U;
    constexpr std::size_t kRounds = 256U;
    constexpr std::size_t kReservation = 64U;
    const auto self = LedgerBytes();
    auto global = std::make_shared<TxnMemoryGlobal>(kUnlimited);
    auto budget = TxnMemoryBudget::Create(global, self + kReservation);
    std::barrier<> start(kWorkers + 1U), held(kWorkers + 1U), release(kWorkers + 1U), done(kWorkers + 1U);
    std::atomic<unsigned> winners{0U};
    const auto run = [&] {
        for (std::size_t round = 0U; round < kRounds; ++round) {
            start.arrive_and_wait();
            bool admitted = false;
            try {
                budget->Reserve(kReservation);
                admitted = true;
                winners.fetch_add(1U, std::memory_order_relaxed);
            } catch (const TxnMemoryLimitError& error) {
                assert(error.scope() == TxnMemoryScope::kPrivate);
            }
            held.arrive_and_wait();
            release.arrive_and_wait();
            if (admitted) budget->Release(kReservation);
            done.arrive_and_wait();
        }
    };
    std::array<std::thread, kWorkers> workers;
    for (auto& worker : workers) worker = std::thread(run);
    for (std::size_t round = 0U; round < kRounds; ++round) {
        winners.store(0U, std::memory_order_relaxed);
        start.arrive_and_wait();
        held.arrive_and_wait();
        assert(winners.load(std::memory_order_relaxed) == 1U);
        assert(budget->bytes() == self + kReservation && global->bytes() == self + kReservation);
        release.arrive_and_wait();
        done.arrive_and_wait();
        assert(budget->bytes() == self && global->bytes() == self);
    }
    for (auto& worker : workers) worker.join();
    budget = {};
    assert(global->bytes() == 0U);
}

void GlobalContentionAndOverflow() {
    const auto self = LedgerBytes();
    auto global = std::make_shared<TxnMemoryGlobal>(2U * self + 64U);
    auto first = TxnMemoryBudget::Create(global, self + 64U);
    auto second = TxnMemoryBudget::Create(global, self + 64U);
    std::barrier<> start(3), attempted(3), release(3);
    std::atomic<unsigned> winners{0U};
    const auto run = [&](TxnMemoryBudgetPtr budget) {
        start.arrive_and_wait();
        bool admitted = false;
        try { budget->Reserve(64U); admitted = true; winners.fetch_add(1U); }
        catch (const TxnMemoryLimitError& error) { assert(error.scope() == TxnMemoryScope::kGlobal); }
        attempted.arrive_and_wait();
        release.arrive_and_wait();
        if (admitted) budget->Release(64U);
    };
    std::thread a(run, first), b(run, second);
    start.arrive_and_wait(); attempted.arrive_and_wait();
    assert(winners.load() == 1U && global->bytes() == 2U * self + 64U);
    release.arrive_and_wait(); a.join(); b.join();
    assert(first->bytes() == self && second->bytes() == self && global->bytes() == 2U * self);
    first = {}; second = {};
    assert(global->bytes() == 0U);
    auto wide_global = std::make_shared<TxnMemoryGlobal>(kUnlimited);
    auto wide = TxnMemoryBudget::Create(wide_global, kUnlimited);
    wide->Reserve(kUnlimited - self);
    Refuses<TxnMemoryLimitError>([&] { wide->Reserve(1U); });
    assert(wide->bytes() == kUnlimited && wide_global->bytes() == kUnlimited);
    wide->Release(kUnlimited - self);
}

void DiagnosticFailureAndBorrowedLifetime() {
    const auto self = LedgerBytes();
    auto global = std::make_shared<TxnMemoryGlobal>(2U * self);
    auto budget = TxnMemoryBudget::Create(global, kUnlimited);
    auto other = TxnMemoryBudget::Create(global, self);
    diagnostic_budget = budget.get();
    diagnostic_global = global.get();
    diagnostic_private = self;
    diagnostic_shared = 2U * self;
    diagnostic_requests.store(0U);
    {
        txn_block_allocation::Observe observe(ObserveDiagnostic);
        txn_block_allocation::Guard no_allocations(0U);
        Refuses<std::bad_alloc>([&] { budget->Reserve(1U); });
    }
    assert(txn_block_allocation::observer.load() == nullptr && diagnostic_requests.load() != 0U);
    diagnostic_budget = nullptr;
    diagnostic_global = nullptr;
    assert(budget->bytes() == self && global->bytes() == 2U * self);
    other = {};
    assert(global->bytes() == self);
    std::atomic<std::size_t> borrowed_global{0U};
    std::size_t local = 0U;
    {
        TxnBlockMemoryResource borrowed(local, borrowed_global, 32U, 32U);
        auto* bytes = borrowed.allocate(13U, alignof(std::uint8_t));
        assert(local == 13U && borrowed_global == 13U);
        borrowed.deallocate(bytes, 13U, alignof(std::uint8_t));
    }
    assert(local == 0U && borrowed_global == 0U);
    {
        SessionTransaction borrowed(&borrowed_global, std::chrono::steady_clock::now());
        assert(!borrowed.memory_budget);
        borrowed_global.store(7U); borrowed.bytes = 7U;
        Refuses<std::invalid_argument>([&] { (void)MakeTxnStorageAccount(borrowed.memory_budget); });
    }
    assert(borrowed_global == 0U);
}
}

int main(int argc, char** argv) {
    txn_block_allocation::CalibrateAligned();
    const std::string_view only = argc > 1 ? argv[1] : "";
    const auto run = [&](std::string_view name, auto test) { if (only.empty() || only == name) test(); };
    run("ledger", LedgerAdmission);
    run("account", AccountControlAdmission);
    run("mixed", MixedDomainsAndAbort);
    run("failure", UpstreamFailures);
    run("escape", EscapedOwnerAndHandoff);
    run("weak", WeakAccountControlLifetime);
    run("private", PrivateContention);
    run("contention", GlobalContentionAndOverflow);
    run("diagnostic", DiagnosticFailureAndBorrowedLifetime);
    std::puts("transaction memory tests passed");
}
