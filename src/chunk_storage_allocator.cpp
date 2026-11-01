#include "chunkdb/chunk_storage_allocator.hpp"

#include <algorithm>
#include <cassert>
#include <stdexcept>

namespace chunkdb::detail {
namespace {
struct Prefix {
    void* allocation;
    std::size_t bytes;
    std::size_t alignment;
    std::pmr::memory_resource* upstream;
    ChunkStoragePolicy* policy;
};
const Prefix& Header(const void* memory) noexcept {
    return *reinterpret_cast<const Prefix*>(static_cast<const std::byte*>(memory) - sizeof(Prefix));
}
}  // namespace

ChunkStoragePolicyPtr::ChunkStoragePolicyPtr(const ChunkStoragePolicyPtr& other) noexcept : policy_(other.policy_) {
    if (policy_) policy_->AddRef();
}
ChunkStoragePolicyPtr::ChunkStoragePolicyPtr(ChunkStoragePolicyPtr&& other) noexcept
    : policy_(std::exchange(other.policy_, nullptr)) {}
ChunkStoragePolicyPtr& ChunkStoragePolicyPtr::operator=(const ChunkStoragePolicyPtr& other) noexcept {
    if (this != &other) { auto copy = other; std::swap(policy_, copy.policy_); }
    return *this;
}
ChunkStoragePolicyPtr& ChunkStoragePolicyPtr::operator=(ChunkStoragePolicyPtr&& other) noexcept {
    if (this != &other) { auto moved = std::move(other); std::swap(policy_, moved.policy_); }
    return *this;
}
ChunkStoragePolicyPtr::~ChunkStoragePolicyPtr() { if (policy_) policy_->ReleaseRef(); }

ChunkStoragePolicy::ChunkStoragePolicy(std::shared_ptr<ChunkStorageAccount> account,
    std::pmr::memory_resource* upstream) noexcept : account_(std::move(account)), upstream_(upstream) {}
ChunkStoragePolicyPtr ChunkStoragePolicy::Create(std::shared_ptr<ChunkStorageAccount> account,
    std::pmr::memory_resource* upstream) {
    if (!account || !upstream) throw std::invalid_argument("staged chunk storage needs an account and upstream resource");
    account->Reserve(sizeof(ChunkStoragePolicy));
    struct Reservation {
        ChunkStorageAccount& account;
        bool committed = false;
        ~Reservation() { if (!committed) account.Release(sizeof(ChunkStoragePolicy)); }
    } reservation{*account};
    auto* memory = upstream->allocate(sizeof(ChunkStoragePolicy), alignof(ChunkStoragePolicy));
    auto* policy = new (memory) ChunkStoragePolicy(std::move(account), upstream);
    reservation.committed = true;
    return ChunkStoragePolicyPtr(policy);
}
void ChunkStoragePolicy::AddRef() noexcept {
    const auto previous = references_.fetch_add(1U, std::memory_order_relaxed);
    assert(previous != 0U && previous != std::numeric_limits<std::size_t>::max());
    (void)previous;
}
void ChunkStoragePolicy::ReleaseRef() noexcept {
    if (references_.fetch_sub(1U, std::memory_order_acq_rel) != 1U) return;
    assert(buffer_bytes_ == 0U);
    auto account = std::move(account_);
    auto* upstream = upstream_;
    this->~ChunkStoragePolicy();
    upstream->deallocate(this, sizeof(ChunkStoragePolicy), alignof(ChunkStoragePolicy));
    if (account) account->Release(sizeof(ChunkStoragePolicy));
}
std::size_t ChunkStoragePolicy::allocation_bytes() const noexcept { return sizeof(ChunkStoragePolicy) + buffer_bytes_; }
std::size_t ChunkStoragePolicy::transaction_bytes() const noexcept { return account_ ? allocation_bytes() : 0U; }
void ChunkStoragePolicy::ReserveBuffer(std::size_t bytes) {
    if (bytes > std::numeric_limits<std::size_t>::max() - allocation_bytes()) throw std::bad_array_new_length();
    if (account_) account_->Reserve(bytes);
    buffer_bytes_ += bytes;
}
void ChunkStoragePolicy::ReleaseBuffer(std::size_t bytes) noexcept {
    assert(bytes <= buffer_bytes_);
    buffer_bytes_ -= bytes;
    if (account_) account_->Release(bytes);
}
void ChunkStoragePolicy::TransferToRegularStorage() noexcept {
    auto account = std::move(account_);
    if (account) account->Release(allocation_bytes());
}

void* AllocateChunkStorage(std::size_t bytes, std::size_t alignment,
    ChunkStoragePolicy* policy, std::pmr::memory_resource* upstream) {
    if (!upstream || alignment == 0U || (alignment & (alignment - 1U)) != 0U ||
        (policy && policy->upstream() != upstream))
        throw std::invalid_argument("invalid chunk storage allocation domain or alignment");
    const auto native_alignment = std::max(alignment, alignof(Prefix));
    const auto padding = (native_alignment - sizeof(Prefix) % native_alignment) % native_alignment;
    const auto prefix_bytes = sizeof(Prefix) + padding;
    if (bytes > std::numeric_limits<std::size_t>::max() - prefix_bytes) throw std::bad_array_new_length();
    const auto total = prefix_bytes + bytes;
    if (policy) policy->ReserveBuffer(total);
    struct Reservation {
        ChunkStoragePolicy* policy;
        std::size_t bytes;
        bool committed = false;
        ~Reservation() { if (policy && !committed) policy->ReleaseBuffer(bytes); }
    } reservation{policy, total};
    auto* memory = upstream->allocate(total, native_alignment);
    auto* result = static_cast<std::byte*>(memory) + prefix_bytes;
    if (policy) policy->AddRef();
    new (result - sizeof(Prefix)) Prefix{memory, total, native_alignment, upstream, policy};
    reservation.committed = true;
    return result;
}
void DeallocateChunkStorage(void* memory) noexcept {
    if (!memory) return;
    const auto header = Header(memory);
    header.upstream->deallocate(header.allocation, header.bytes, header.alignment);
    if (header.policy) {
        header.policy->ReleaseBuffer(header.bytes);
        header.policy->ReleaseRef();
    }
}
std::size_t ChunkStorageAllocationBytes(const void* memory) noexcept { return memory ? Header(memory).bytes : 0U; }

}  // namespace chunkdb::detail
