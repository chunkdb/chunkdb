#include "txn_block_memory.hpp"
#include "txn_memory_admission.hpp"

#include <cassert>
#include <stdexcept>

namespace chunkdb {

TxnBlockMemoryResource::TxnBlockMemoryResource(std::size_t& private_bytes,
    std::atomic<std::size_t>& global_bytes, std::size_t private_limit, std::size_t global_limit,
    std::pmr::memory_resource* upstream)
    : private_bytes_(&private_bytes), global_bytes_(&global_bytes), private_limit_(private_limit),
      global_limit_(global_limit), upstream_(upstream) {
    if (private_limit == 0U || global_limit == 0U || !upstream || upstream == this)
        throw std::invalid_argument("invalid transaction block memory budget");
}

TxnBlockMemoryResource::TxnBlockMemoryResource(TxnMemoryBudgetPtr budget)
    : budget_(std::move(budget)), private_limit_(budget_ ? budget_->private_limit() : 0U),
      global_limit_(budget_ ? budget_->global_limit() : 0U),
      upstream_(budget_ ? budget_->upstream() : nullptr) {
    if (!budget_) throw std::invalid_argument("owned transaction memory needs an owned ledger");
}

TxnBlockMemoryResource::~TxnBlockMemoryResource() { assert(allocated_ == 0U); }

void TxnBlockMemoryResource::Reserve(std::size_t bytes) {
    if (budget_) {
        budget_->Reserve(bytes);
        allocated_ += bytes;
        return;
    }
    if (*private_bytes_ > private_limit_ || bytes > private_limit_ - *private_bytes_)
        throw std::invalid_argument("transaction block state exceeds its private byte limit");
    detail::ReserveTxnGlobalBytes(*global_bytes_, global_limit_, bytes);
    *private_bytes_ += bytes;
    allocated_ += bytes;
}

void TxnBlockMemoryResource::Release(std::size_t bytes) noexcept {
    assert(bytes <= allocated_);
    allocated_ -= bytes;
    if (budget_) {
        budget_->Release(bytes);
        return;
    }
    assert(bytes <= *private_bytes_);
    *private_bytes_ -= bytes;
    detail::ReleaseTxnGlobalBytes(*global_bytes_, bytes);
}

void* TxnBlockMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
    Reserve(bytes);
    struct UndoReservation {
        TxnBlockMemoryResource& owner;
        std::size_t bytes;
        bool committed = false;
        ~UndoReservation() { if (!committed) owner.Release(bytes); }
    } reservation{*this, bytes};
    void* memory = upstream_->allocate(bytes, alignment);
    reservation.committed = true;
    return memory;
}

void TxnBlockMemoryResource::do_deallocate(void* memory, std::size_t bytes, std::size_t alignment) {
    upstream_->deallocate(memory, bytes, alignment);
    Release(bytes);
}

bool TxnBlockMemoryResource::do_is_equal(const std::pmr::memory_resource& other) const noexcept {
    return this == &other;
}

}  // namespace chunkdb
