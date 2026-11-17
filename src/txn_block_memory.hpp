#pragma once

#include <atomic>
#include <cstddef>
#include <memory_resource>

namespace chunkdb {

// Counter owners and this resource outlive every allocation it supplies.
// The transaction serializes its private counter; the global counter is shared.
class TxnBlockMemoryResource final : public std::pmr::memory_resource {
  public:
    TxnBlockMemoryResource(std::size_t& private_bytes, std::atomic<std::size_t>& global_bytes,
        std::size_t private_limit, std::size_t global_limit,
        std::pmr::memory_resource* upstream = std::pmr::new_delete_resource());
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

    std::size_t& private_bytes_;
    std::atomic<std::size_t>& global_bytes_;
    const std::size_t private_limit_;
    const std::size_t global_limit_;
    std::pmr::memory_resource* const upstream_;
    std::size_t allocated_ = 0U;
};

}  // namespace chunkdb
