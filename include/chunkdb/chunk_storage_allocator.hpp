#pragma once

#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <type_traits>
#include <utility>

namespace chunkdb::detail {

// Accounts and upstream resources outlive the allocations they supply.
// Reserve throws before allocation; Release and upstream deallocation do not throw.
// Operations are non-reentrant and never enter storage or acquire its locks.
class ChunkStorageAccount {
  public:
    virtual ~ChunkStorageAccount() = default;
    virtual void Reserve(std::size_t bytes) = 0;
    virtual void Release(std::size_t bytes) noexcept = 0;
};

class ChunkStoragePolicy;

class ChunkStoragePolicyPtr {
  public:
    ChunkStoragePolicyPtr() noexcept = default;
    ChunkStoragePolicyPtr(const ChunkStoragePolicyPtr& other) noexcept;
    ChunkStoragePolicyPtr(ChunkStoragePolicyPtr&& other) noexcept;
    ChunkStoragePolicyPtr& operator=(const ChunkStoragePolicyPtr& other) noexcept;
    ChunkStoragePolicyPtr& operator=(ChunkStoragePolicyPtr&& other) noexcept;
    ~ChunkStoragePolicyPtr();
    [[nodiscard]] ChunkStoragePolicy* get() const noexcept { return policy_; }
    [[nodiscard]] ChunkStoragePolicy* operator->() const noexcept { return policy_; }

  private:
    friend class ChunkStoragePolicy;
    explicit ChunkStoragePolicyPtr(ChunkStoragePolicy* policy) noexcept : policy_(policy) {}
    ChunkStoragePolicy* policy_ = nullptr;
};

// Explicit staged policy; ordinary allocators never create one.
// The owning subsystem serializes allocation, deallocation and handoff.
// Staging and live mutations use the original chunk locks; final destruction
// has exclusive ownership. History destruction never acquires a chunk lock.
class ChunkStoragePolicy {
  public:
    [[nodiscard]] static ChunkStoragePolicyPtr Create(std::shared_ptr<ChunkStorageAccount> account,
        std::pmr::memory_resource* upstream = std::pmr::new_delete_resource());
    [[nodiscard]] std::size_t allocation_bytes() const noexcept;
    [[nodiscard]] std::size_t transaction_bytes() const noexcept;
    [[nodiscard]] std::pmr::memory_resource* upstream() const noexcept { return upstream_; }
    // Only after irreversible publication establishes the RegularChunk owner.
    // Future growth through any allocator copy then follows ordinary storage policy.
    void TransferToRegularStorage() noexcept;

  private:
    friend class ChunkStoragePolicyPtr;
    friend void* AllocateChunkStorage(std::size_t, std::size_t, ChunkStoragePolicy*, std::pmr::memory_resource*);
    friend void DeallocateChunkStorage(void*) noexcept;
    ChunkStoragePolicy(std::shared_ptr<ChunkStorageAccount> account, std::pmr::memory_resource* upstream) noexcept;
    ~ChunkStoragePolicy() = default;
    void AddRef() noexcept;
    void ReleaseRef() noexcept;
    void ReserveBuffer(std::size_t bytes);
    void ReleaseBuffer(std::size_t bytes) noexcept;
    std::atomic<std::size_t> references_{1U};
    std::shared_ptr<ChunkStorageAccount> account_;
    std::pmr::memory_resource* upstream_;
    std::size_t buffer_bytes_ = 0U;
};

[[nodiscard]] void* AllocateChunkStorage(std::size_t bytes, std::size_t alignment,
    ChunkStoragePolicy* policy, std::pmr::memory_resource* upstream);
void DeallocateChunkStorage(void* memory) noexcept;
// Includes the exact prefix and alignment padding requested upstream.
[[nodiscard]] std::size_t ChunkStorageAllocationBytes(const void* memory) noexcept;

template <class T> class ChunkStorageAllocator {
  public:
    using value_type = T;
    using propagate_on_container_move_assignment = std::true_type;
    using propagate_on_container_swap = std::true_type;
    using is_always_equal = std::false_type;

    ChunkStorageAllocator() noexcept = default;
    explicit ChunkStorageAllocator(ChunkStoragePolicyPtr policy) noexcept : policy_(std::move(policy)) {}
    template <class U> ChunkStorageAllocator(const ChunkStorageAllocator<U>& other) noexcept : policy_(other.policy_) {}
    [[nodiscard]] T* allocate(std::size_t count) {
        if (count > std::numeric_limits<std::size_t>::max() / sizeof(T)) throw std::bad_array_new_length();
        return static_cast<T*>(AllocateChunkStorage(count * sizeof(T), alignof(T), policy_.get(), upstream()));
    }
    void deallocate(T* memory, std::size_t) noexcept { DeallocateChunkStorage(memory); }
    [[nodiscard]] ChunkStorageAllocator select_on_container_copy_construction() const noexcept { return {}; }
    [[nodiscard]] std::pmr::memory_resource* upstream() const noexcept {
        return policy_.get() ? policy_->upstream() : std::pmr::new_delete_resource();
    }
    [[nodiscard]] ChunkStoragePolicy* policy() const noexcept { return policy_.get(); }
    template <class U> bool operator==(const ChunkStorageAllocator<U>& other) const noexcept {
        // Prefix ownership makes cross-deallocation genuine, regardless of accounting policy.
        return upstream() == other.upstream();
    }

  private:
    template <class U> friend class ChunkStorageAllocator;
    ChunkStoragePolicyPtr policy_;
};

struct ChunkVarsStorageAccess;

}  // namespace chunkdb::detail
