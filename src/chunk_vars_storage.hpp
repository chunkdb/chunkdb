#pragma once

#include <stdexcept>

#include "chunkdb/chunk_vars.hpp"

namespace chunkdb::detail {

// Internal physical-staging access; no runtime grouped writer is enabled here.
struct ChunkVarsStorageAccess {
    [[nodiscard]] static ChunkVars Clone(const ChunkVars& source, const ChunkStoragePolicyPtr& policy) {
        if (!policy.get()) throw std::invalid_argument("charged chunk storage clone needs an explicit policy");
        ChunkVars result(policy);
        result.data_.assign(source.data_.begin(), source.data_.end());
        result.offsets_.assign(source.offsets_.begin(), source.offsets_.end());
        return result;
    }
    [[nodiscard]] static std::size_t BufferAllocationBytes(const ChunkVars& vars) noexcept {
        return ChunkStorageAllocationBytes(vars.data_.data()) + ChunkStorageAllocationBytes(vars.offsets_.data());
    }
    [[nodiscard]] static std::size_t OffsetCapacityBytes(const ChunkVars& vars) noexcept {
        return vars.offsets_.capacity() * sizeof(std::uint32_t);
    }
};

}  // namespace chunkdb::detail
