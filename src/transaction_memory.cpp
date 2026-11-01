#include "chunkdb/transaction_memory.hpp"
#include "txn_memory_admission.hpp"

#include <cassert>
#include <limits>
#include <new>
#include <string>
#include <utility>

namespace chunkdb {

TxnMemoryLimitError::TxnMemoryLimitError(TxnMemoryScope scope, std::size_t used,
    std::size_t requested, std::size_t limit)
    : std::invalid_argument(std::string("transaction temporary memory exceeds ") +
          (scope == TxnMemoryScope::kPrivate ? "--txn-max-bytes (" : "--txn-total-bytes (") +
          std::to_string(limit) + ")"), scope_(scope), used_(used), requested_(requested), limit_(limit) {}

namespace detail {
bool TryReserveTxnGlobalBytes(std::atomic<std::size_t>& counter, std::size_t limit,
    std::size_t bytes, std::size_t& observed) noexcept {
    auto current = counter.load(std::memory_order_acquire);
    for (;;) {
        observed = current;
        if (current > limit || bytes > limit - current) return false;
        if (counter.compare_exchange_weak(current, current + bytes,
                std::memory_order_acq_rel, std::memory_order_acquire)) return true;
    }
}
void ReserveTxnGlobalBytes(std::atomic<std::size_t>& counter, std::size_t limit, std::size_t bytes) {
    std::size_t observed = 0U;
    if (!TryReserveTxnGlobalBytes(counter, limit, bytes, observed))
        throw TxnMemoryLimitError(TxnMemoryScope::kGlobal, observed, bytes, limit);
}
void ReleaseTxnGlobalBytes(std::atomic<std::size_t>& counter, std::size_t bytes) noexcept {
    const auto previous = counter.fetch_sub(bytes, std::memory_order_acq_rel);
    assert(previous >= bytes);
    (void)previous;
}
}

TxnMemoryGlobal::TxnMemoryGlobal(std::size_t limit) : limit_(limit) {
    if (limit == 0U) throw std::invalid_argument("transaction global byte limit must be positive");
}
void TxnMemoryGlobal::Reserve(std::size_t bytes) { detail::ReserveTxnGlobalBytes(bytes_, limit_, bytes); }
bool TxnMemoryGlobal::TryReserve(std::size_t bytes, std::size_t& observed) noexcept {
    return detail::TryReserveTxnGlobalBytes(bytes_, limit_, bytes, observed);
}
void TxnMemoryGlobal::Release(std::size_t bytes) noexcept { detail::ReleaseTxnGlobalBytes(bytes_, bytes); }

TxnMemoryBudgetPtr::TxnMemoryBudgetPtr(const TxnMemoryBudgetPtr& other) noexcept : budget_(other.budget_) {
    if (budget_) budget_->AddRef();
}
TxnMemoryBudgetPtr::TxnMemoryBudgetPtr(TxnMemoryBudgetPtr&& other) noexcept
    : budget_(std::exchange(other.budget_, nullptr)) {}
TxnMemoryBudgetPtr& TxnMemoryBudgetPtr::operator=(const TxnMemoryBudgetPtr& other) noexcept {
    if (this != &other) { auto copy = other; std::swap(budget_, copy.budget_); }
    return *this;
}
TxnMemoryBudgetPtr& TxnMemoryBudgetPtr::operator=(TxnMemoryBudgetPtr&& other) noexcept {
    if (this != &other) { auto moved = std::move(other); std::swap(budget_, moved.budget_); }
    return *this;
}
TxnMemoryBudgetPtr::~TxnMemoryBudgetPtr() { if (budget_) budget_->ReleaseRef(); }

TxnMemoryBudget::TxnMemoryBudget(std::shared_ptr<TxnMemoryGlobal> global, std::size_t private_limit,
    std::pmr::memory_resource* upstream) noexcept
    : bytes_(sizeof(TxnMemoryBudget)), global_(std::move(global)), private_limit_(private_limit), upstream_(upstream) {}

TxnMemoryBudgetPtr TxnMemoryBudget::Create(std::shared_ptr<TxnMemoryGlobal> global,
    std::size_t private_limit, std::pmr::memory_resource* upstream) {
    if (!global || !upstream || private_limit == 0U)
        throw std::invalid_argument("owned transaction accounting needs positive limits and an upstream resource");
    constexpr auto bytes = sizeof(TxnMemoryBudget);
    if (bytes > private_limit) throw TxnMemoryLimitError(TxnMemoryScope::kPrivate, 0U, bytes, private_limit);
    global->Reserve(bytes);
    struct Reservation {
        TxnMemoryGlobal& global;
        bool committed = false;
        ~Reservation() { if (!committed) global.Release(sizeof(TxnMemoryBudget)); }
    } reservation{*global};
    void* memory = upstream->allocate(bytes, alignof(TxnMemoryBudget));
    auto* budget = new (memory) TxnMemoryBudget(std::move(global), private_limit, upstream);
    reservation.committed = true;
    return TxnMemoryBudgetPtr(budget);
}

void TxnMemoryBudget::AddRef() noexcept {
    const auto previous = references_.fetch_add(1U, std::memory_order_relaxed);
    assert(previous != 0U && previous != std::numeric_limits<std::size_t>::max());
    (void)previous;
}
void TxnMemoryBudget::ReleaseRef() noexcept {
    if (references_.fetch_sub(1U, std::memory_order_acq_rel) != 1U) return;
    assert(bytes_.load(std::memory_order_acquire) == sizeof(TxnMemoryBudget));
    auto global = std::move(global_);
    auto* upstream = upstream_;
    this->~TxnMemoryBudget();
    upstream->deallocate(this, sizeof(TxnMemoryBudget), alignof(TxnMemoryBudget));
    global->Release(sizeof(TxnMemoryBudget));
}
void TxnMemoryBudget::Reserve(std::size_t bytes) {
    auto current = bytes_.load(std::memory_order_acquire);
    for (;;) {
        if (current > private_limit_ || bytes > private_limit_ - current)
            throw TxnMemoryLimitError(TxnMemoryScope::kPrivate, current, bytes, private_limit_);
        if (bytes_.compare_exchange_weak(current, current + bytes,
                std::memory_order_acq_rel, std::memory_order_acquire)) break;
    }
    std::size_t observed = 0U;
    if (!global_->TryReserve(bytes, observed)) {
        const auto previous = bytes_.fetch_sub(bytes, std::memory_order_acq_rel);
        assert(previous >= sizeof(TxnMemoryBudget) && bytes <= previous - sizeof(TxnMemoryBudget));
        (void)previous;
        // No private reservation remains while constructing a refusal diagnostic.
        throw TxnMemoryLimitError(TxnMemoryScope::kGlobal, observed, bytes, global_->limit());
    }
}
void TxnMemoryBudget::Release(std::size_t bytes) noexcept {
    const auto previous = bytes_.fetch_sub(bytes, std::memory_order_acq_rel);
    assert(previous >= sizeof(TxnMemoryBudget) && bytes <= previous - sizeof(TxnMemoryBudget));
    (void)previous;
    global_->Release(bytes);
}

namespace {
class TxnStorageAccount final : public detail::ChunkStorageAccount {
  public:
    explicit TxnStorageAccount(TxnMemoryBudgetPtr budget) noexcept : budget_(std::move(budget)) {}
    void Reserve(std::size_t bytes) override { budget_->Reserve(bytes); }
    void Release(std::size_t bytes) noexcept override { budget_->Release(bytes); }
  private:
    TxnMemoryBudgetPtr budget_;
};

template <class T> class AccountAllocator {
  public:
    using value_type = T;
    explicit AccountAllocator(TxnMemoryBudgetPtr budget) noexcept : budget_(std::move(budget)) {}
    template <class U> AccountAllocator(const AccountAllocator<U>& other) noexcept : budget_(other.budget_) {}
    [[nodiscard]] T* allocate(std::size_t count) {
        if (count > std::numeric_limits<std::size_t>::max() / sizeof(T)) throw std::bad_array_new_length();
        const auto bytes = count * sizeof(T);
        budget_->Reserve(bytes);
        struct Reservation {
            TxnMemoryBudget& budget;
            std::size_t bytes;
            bool committed = false;
            ~Reservation() { if (!committed) budget.Release(bytes); }
        } reservation{*budget_.get(), bytes};
        auto* memory = budget_->upstream()->allocate(bytes, alignof(T));
        reservation.committed = true;
        return static_cast<T*>(memory);
    }
    void deallocate(T* memory, std::size_t count) noexcept {
        const auto bytes = count * sizeof(T);
        budget_->upstream()->deallocate(memory, bytes, alignof(T));
        budget_->Release(bytes);
    }
    template <class U> bool operator==(const AccountAllocator<U>& other) const noexcept {
        return budget_.get() == other.budget_.get();
    }
  private:
    template <class U> friend class AccountAllocator;
    TxnMemoryBudgetPtr budget_;
};
}

std::shared_ptr<detail::ChunkStorageAccount> MakeTxnStorageAccount(TxnMemoryBudgetPtr budget) {
    if (!budget) throw std::invalid_argument("an owning storage account needs an owned transaction ledger");
    const AccountAllocator<TxnStorageAccount> allocator(budget);
    return std::allocate_shared<TxnStorageAccount>(allocator, std::move(budget));
}

}  // namespace chunkdb
