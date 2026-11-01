#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <memory_resource>
#include <stdexcept>

#include "chunkdb/chunk_storage_allocator.hpp"

namespace chunkdb {

enum class TxnMemoryScope { kPrivate, kGlobal };

class TxnMemoryLimitError final : public std::invalid_argument {
  public:
    TxnMemoryLimitError(TxnMemoryScope scope, std::size_t used, std::size_t requested, std::size_t limit);
    [[nodiscard]] TxnMemoryScope scope() const noexcept { return scope_; }
    [[nodiscard]] std::size_t used() const noexcept { return used_; }
    [[nodiscard]] std::size_t requested() const noexcept { return requested_; }
    [[nodiscard]] std::size_t limit() const noexcept { return limit_; }

  private:
    TxnMemoryScope scope_;
    std::size_t used_;
    std::size_t requested_;
    std::size_t limit_;
};

// Fixed engine infrastructure; transactions retain this owner independently.
class TxnMemoryGlobal {
  public:
    explicit TxnMemoryGlobal(std::size_t limit);
    [[nodiscard]] std::size_t bytes() const noexcept { return bytes_.load(std::memory_order_acquire); }
    [[nodiscard]] std::size_t limit() const noexcept { return limit_; }

  private:
    friend class TxnMemoryBudget;
    [[nodiscard]] bool TryReserve(std::size_t bytes, std::size_t& observed) noexcept;
    void Reserve(std::size_t bytes);
    void Release(std::size_t bytes) noexcept;
    std::atomic<std::size_t> bytes_{0U};
    const std::size_t limit_;
};

class TxnMemoryBudget;

class TxnMemoryBudgetPtr {
  public:
    TxnMemoryBudgetPtr() noexcept = default;
    TxnMemoryBudgetPtr(const TxnMemoryBudgetPtr& other) noexcept;
    TxnMemoryBudgetPtr(TxnMemoryBudgetPtr&& other) noexcept;
    TxnMemoryBudgetPtr& operator=(const TxnMemoryBudgetPtr& other) noexcept;
    TxnMemoryBudgetPtr& operator=(TxnMemoryBudgetPtr&& other) noexcept;
    ~TxnMemoryBudgetPtr();
    [[nodiscard]] TxnMemoryBudget* get() const noexcept { return budget_; }
    [[nodiscard]] TxnMemoryBudget* operator->() const noexcept { return budget_; }
    [[nodiscard]] explicit operator bool() const noexcept { return budget_ != nullptr; }

  private:
    friend class TxnMemoryBudget;
    explicit TxnMemoryBudgetPtr(TxnMemoryBudget* budget) noexcept : budget_(budget) {}
    TxnMemoryBudget* budget_ = nullptr;
};

// The ledger and its control extent are admitted before upstream allocation.
// The upstream must outlive all allocations, including escaped storage domains.
class TxnMemoryBudget {
  public:
    [[nodiscard]] static TxnMemoryBudgetPtr Create(std::shared_ptr<TxnMemoryGlobal> global,
        std::size_t private_limit, std::pmr::memory_resource* upstream = std::pmr::new_delete_resource());
    void Reserve(std::size_t bytes);
    void Release(std::size_t bytes) noexcept;
    [[nodiscard]] std::size_t bytes() const noexcept { return bytes_.load(std::memory_order_acquire); }
    [[nodiscard]] std::size_t private_limit() const noexcept { return private_limit_; }
    [[nodiscard]] std::size_t global_limit() const noexcept { return global_->limit(); }
    [[nodiscard]] std::pmr::memory_resource* upstream() const noexcept { return upstream_; }

  private:
    friend class TxnMemoryBudgetPtr;
    friend struct SessionTransaction;
    [[nodiscard]] std::atomic<std::size_t>& global_counter() const noexcept { return global_->bytes_; }
    TxnMemoryBudget(std::shared_ptr<TxnMemoryGlobal> global, std::size_t private_limit,
        std::pmr::memory_resource* upstream) noexcept;
    ~TxnMemoryBudget() = default;
    void AddRef() noexcept;
    void ReleaseRef() noexcept;
    std::atomic<std::size_t> references_{1U};
    std::atomic<std::size_t> bytes_;
    std::shared_ptr<TxnMemoryGlobal> global_;
    const std::size_t private_limit_;
    std::pmr::memory_resource* const upstream_;
};

// Empty/borrowed ledgers cannot create an escaping allocation account.
[[nodiscard]] std::shared_ptr<detail::ChunkStorageAccount> MakeTxnStorageAccount(TxnMemoryBudgetPtr budget);

}  // namespace chunkdb
