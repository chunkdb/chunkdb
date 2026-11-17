#pragma once

#include "chunkdb/chunk_storage_allocator.hpp"

namespace chunkdb::detail {

struct ChunkStorageAllocationRecord {
    std::size_t bytes = 0U;
    std::pmr::memory_resource* upstream = nullptr;
    ChunkStoragePolicy* policy = nullptr;
};

// Internal prepared-ownership operations; the original owners remain locked.
struct ChunkStoragePolicyAccess {
    [[nodiscard]] static ChunkStorageAllocationRecord Inspect(const void* memory) noexcept;
    [[nodiscard]] static ChunkStoragePolicyPtr Retain(ChunkStoragePolicy* policy) noexcept;
    [[nodiscard]] static std::size_t ControlBytes() noexcept;
    [[nodiscard]] static bool IsRegular(const ChunkStoragePolicy& policy) noexcept;
    static void AdmitExisting(ChunkStoragePolicy& policy, std::shared_ptr<ChunkStorageAccount> account);
    static void ReserveExistingBuffers(ChunkStoragePolicy& policy, std::size_t bytes);
    static void CancelExistingBuffers(ChunkStoragePolicy& policy, std::size_t bytes) noexcept;
    static void Attach(void* memory, ChunkStoragePolicy& policy) noexcept;
    static void Detach(void* memory, ChunkStoragePolicy& policy) noexcept;
    // The destination has already admitted the complete retained extent once.
    // No Reserve callback or allocation is permitted while consuming admission.
    static void ConsumeHistoryAdmission(ChunkStoragePolicy& policy,
        const std::shared_ptr<ChunkStorageAccount>& destination) noexcept;
};

}  // namespace chunkdb::detail
