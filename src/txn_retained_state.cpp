#include "txn_retained_state.hpp"

#include <cassert>

namespace chunkdb {

std::shared_ptr<const ChunkColumnState> TxnRetainedStateView::CopyMetadata() const {
    if (legacy_metadata) return legacy_metadata;
    if (!metadata_present) return {};
    return std::make_shared<const ChunkColumnState>(ChunkColumnState{
        std::vector<std::uint8_t>(metadata_bytes.begin(), metadata_bytes.end()), metadata_variable_bytes});
}
ChunkState TxnRetainedStateView::CopyState() const {
    return {.version = version, .payload = std::vector<std::uint8_t>(payload.begin(), payload.end()),
        .presence_bitmap = std::vector<std::uint8_t>(presence_bitmap.begin(), presence_bitmap.end()),
        .vars = vars, .written = written, .commit_time_ms = commit_time_ms, .chunk_columns = CopyMetadata()};
}
TxnRetainedStateView ViewTxnState(const ChunkState& state) noexcept {
    return {.version = state.version, .commit_time_ms = state.commit_time_ms, .written = state.written,
        .payload = state.payload, .presence_bitmap = state.presence_bitmap, .vars = state.vars,
        .legacy_metadata = state.chunk_columns, .metadata_present = state.chunk_columns != nullptr,
        .metadata_bytes = state.chunk_columns ? std::span<const std::uint8_t>(state.chunk_columns->bytes) : std::span<const std::uint8_t>{},
        .metadata_variable_bytes = state.chunk_columns ? state.chunk_columns->variable_bytes : 0U};
}

TxnRetainedState::TxnRetainedState(const detail::ChunkStoragePolicyPtr& allocation_policy,
    const TxnRetainedStateView& source)
    : policy(allocation_policy), payload(detail::ChunkStorageAllocator<std::uint8_t>(allocation_policy)),
      presence_bitmap(detail::ChunkStorageAllocator<std::uint8_t>(allocation_policy)),
      metadata(detail::ChunkStorageAllocator<std::uint8_t>(allocation_policy)),
      metadata_present(source.metadata_present), metadata_variable_bytes(source.metadata_variable_bytes),
      version(source.version), commit_time_ms(source.commit_time_ms), written(source.written) {
    payload.assign(source.payload.begin(), source.payload.end());
    presence_bitmap.assign(source.presence_bitmap.begin(), source.presence_bitmap.end());
    if (metadata_present) metadata.assign(source.metadata_bytes.begin(), source.metadata_bytes.end());
}
TxnRetainedStateView TxnRetainedState::View() const noexcept {
    return {.version = version, .commit_time_ms = commit_time_ms, .written = written,
        .payload = payload, .presence_bitmap = presence_bitmap, .vars = vars, .legacy_metadata = {},
        .metadata_present = metadata_present, .metadata_bytes = metadata,
        .metadata_variable_bytes = metadata_variable_bytes};
}
std::size_t TxnRetainedState::allocation_bytes() const noexcept {
    assert(originals);
    return policy->allocation_bytes() + originals->allocation_bytes();
}
void TxnRetainedState::ConsumeHistoryAdmission(
    const std::shared_ptr<detail::ChunkStorageAccount>& account) noexcept {
    assert(originals);
    originals->ConsumeHistoryAdmission(account);
    detail::ChunkStoragePolicyAccess::ConsumeHistoryAdmission(*policy.get(), account);
}

}  // namespace chunkdb
