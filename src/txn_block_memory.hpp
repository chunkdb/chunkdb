#pragma once

#include <atomic>
#include <cstddef>
#include <memory_resource>

#include "chunkdb/transaction_memory.hpp"

namespace chunkdb {

// This resource outlives every allocation it supplies.
// Borrowed counters also outlive those allocations and serialize private use.
// The owned overload retains a stable ledger shared with other allocation domains.
class TxnBlockMemoryResource final : public std::pmr::memory_resource {
  public:
    TxnBlockMemoryResource(std::size_t& private_bytes, std::atomic<std::size_t>& global_bytes,
        std::size_t private_limit, std::size_t global_limit,
        std::pmr::memory_resource* upstream = std::pmr::new_delete_resource());
    explicit TxnBlockMemoryResource(TxnMemoryBudgetPtr budget);
    ~TxnBlockMemoryResource() override;
    TxnBlockMemoryResource(const TxnBlockMemoryResource&) = delete;
    TxnBlockMemoryResource& operator=(const TxnBlockMemoryResource&) = delete;
    [[nodiscard]] std::size_t allocated_bytes() const noexcept { return allocated_; }

  private:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override;
    void do_deallocate(void* memory, std::size_t bytes, std::size_t alignment) override;
    [[nodiscard]] bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override;
    void Reserve(std::size_t bytes);
    void Release(std::size_t bytes) noexcept;

    TxnMemoryBudgetPtr budget_;
    std::size_t* private_bytes_ = nullptr;
    std::atomic<std::size_t>* global_bytes_ = nullptr;
    const std::size_t private_limit_;
    const std::size_t global_limit_;
    std::pmr::memory_resource* const upstream_;
    std::size_t allocated_ = 0U;
};

}  // namespace chunkdb
