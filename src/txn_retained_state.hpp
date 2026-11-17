#pragma once

#include <memory>
#include <span>

#include "chunkdb/chunk_store.hpp"
#include "txn_original_variables.hpp"

namespace chunkdb {

// Read-only source ownership remains with the chunk lock or history callback.
struct TxnRetainedStateView {
    std::uint64_t version = 0U;
    std::uint64_t commit_time_ms = 0U;
    bool written = false;
    std::span<const std::uint8_t> payload;
    std::span<const std::uint8_t> presence_bitmap;
    const ChunkVars& vars;
    std::shared_ptr<const ChunkColumnState> legacy_metadata;
    bool metadata_present = false;
    std::span<const std::uint8_t> metadata_bytes;
    std::size_t metadata_variable_bytes = 0U;

    [[nodiscard]] std::shared_ptr<const ChunkColumnState> CopyMetadata() const;
    [[nodiscard]] ChunkState CopyState() const;
};

[[nodiscard]] TxnRetainedStateView ViewTxnState(const ChunkState& state) noexcept;

// All newly allocated state is charged through the node's dedicated policy.
// Original variables arrive only during nonallocating stage application.
struct TxnRetainedState {
    using Bytes = ChunkVars::EncodedStorage;
    detail::ChunkStoragePolicyPtr policy;
    // Destroy vars before its ownership admission so attached discarded state
    // returns buffer charges before dropping the last policy-control owners.
    std::optional<TxnOriginalVariables> originals;
    ChunkVars vars;
    Bytes payload;
    Bytes presence_bitmap;
    Bytes metadata;
    bool metadata_present = false;
    std::size_t metadata_variable_bytes = 0U;
    std::uint64_t version = 0U;
    std::uint64_t commit_time_ms = 0U;
    bool written = false;

    explicit TxnRetainedState(const detail::ChunkStoragePolicyPtr& allocation_policy,
        const TxnRetainedStateView& source);
    TxnRetainedState(TxnRetainedState&&) noexcept = default;
    TxnRetainedState& operator=(TxnRetainedState&&) = delete;
    [[nodiscard]] TxnRetainedStateView View() const noexcept;
    [[nodiscard]] std::size_t allocation_bytes() const noexcept;
    void ConsumeHistoryAdmission(const std::shared_ptr<detail::ChunkStorageAccount>& account) noexcept;
};

}  // namespace chunkdb
