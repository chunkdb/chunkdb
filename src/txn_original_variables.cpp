#include "txn_original_variables.hpp"

#include <cassert>
#include <limits>
#include <stdexcept>
#include <utility>

namespace chunkdb {
using detail::ChunkStoragePolicyAccess;

TxnOriginalVariables::TxnOriginalVariables(TxnOriginalVariables&& other) noexcept
    : slots_(std::move(other.slots_)), policies_(other.policies_), buffers_(other.buffers_), buffer_slots_(other.buffer_slots_),
      needs_attach_(other.needs_attach_), slot_count_(other.slot_count_),
      state_(std::exchange(other.state_, State::kEmpty)) {}

TxnOriginalVariables::~TxnOriginalVariables() {
    if (state_ == State::kPrepared) {
        for (std::size_t i = 0U; i < slot_count_; ++i) {
            auto& slot = slots_[i];
            if (!slot.admitted) continue;
            if (slot.created) ChunkStoragePolicyAccess::CancelExistingBuffers(*slot.policy.get(), slot.original_bytes);
            else slot.policy->TransferToRegularStorage();
        }
    } else if (state_ == State::kAttached) {
        // An attached discarded owner must destroy its retained vars first.
        for (std::size_t i = 0U; i < slot_count_; ++i)
            assert(slots_[i].policy->allocation_bytes() == ChunkStoragePolicyAccess::ControlBytes());
    }
}

TxnOriginalVariables TxnOriginalVariables::Inspect(ChunkVars& vars, std::pmr::memory_resource* expected_upstream) {
    if (!expected_upstream) throw std::invalid_argument("original variables need an upstream domain");
    TxnOriginalVariables prepared;
    prepared.buffers_ = detail::ChunkVarsStorageAccess::Buffers(vars);
    const auto allocator_policies = detail::ChunkVarsStorageAccess::Policies(vars);
    std::array<detail::ChunkStorageAllocationRecord, 2U> records;
    // Validate every original domain before reserving or changing any policy.
    for (std::size_t i = 0U; i < records.size(); ++i) {
        if (!prepared.buffers_[i] && !allocator_policies[i]) continue;
        records[i] = ChunkStoragePolicyAccess::Inspect(prepared.buffers_[i]);
        const auto& record = records[i];
        auto* policy = allocator_policies[i];
        if ((prepared.buffers_[i] && (record.upstream != expected_upstream || record.policy != policy)) ||
            (policy && (policy->upstream() != expected_upstream || !ChunkStoragePolicyAccess::IsRegular(*policy))))
            throw std::invalid_argument("original variables have a foreign or already charged allocation policy");
        std::size_t slot = 0U;
        while (slot < prepared.slot_count_ && prepared.policies_[slot] != policy) ++slot;
        if (slot == prepared.slot_count_) prepared.policies_[prepared.slot_count_++] = policy;
        auto& bytes = prepared.slots_[slot].original_bytes;
        if (record.bytes > std::numeric_limits<std::size_t>::max() - bytes) throw std::invalid_argument("original variable extent overflows");
        bytes += record.bytes;
        prepared.buffer_slots_[i] = slot;
        prepared.needs_attach_[i] = policy == nullptr;
    }
    for (std::size_t i = 0U; i < prepared.slot_count_; ++i) {
        if (prepared.slots_[i].original_bytes > std::numeric_limits<std::size_t>::max() - ChunkStoragePolicyAccess::ControlBytes())
            throw std::invalid_argument("original variable policy extent overflows");
        if (prepared.policies_[i] && prepared.policies_[i]->allocation_bytes() !=
                prepared.slots_[i].original_bytes + ChunkStoragePolicyAccess::ControlBytes())
            throw std::invalid_argument("original variable policy owns buffers outside the locked section");
    }
    return prepared;
}
void TxnOriginalVariables::Validate(ChunkVars& vars, std::pmr::memory_resource* expected_upstream) {
    (void)Inspect(vars, expected_upstream);
}
TxnOriginalVariables TxnOriginalVariables::Prepare(ChunkVars& vars,
    const std::shared_ptr<detail::ChunkStorageAccount>& account, std::pmr::memory_resource* expected_upstream) {
    if (!account) throw std::invalid_argument("original variables need an owning account");
    auto prepared = Inspect(vars, expected_upstream);
    prepared.state_ = State::kPrepared;
    for (std::size_t i = 0U; i < prepared.slot_count_; ++i) {
        auto& slot = prepared.slots_[i];
        if (prepared.policies_[i]) {
            slot.policy = ChunkStoragePolicyAccess::Retain(prepared.policies_[i]);
            ChunkStoragePolicyAccess::AdmitExisting(*slot.policy.get(), account);
        } else {
            slot.created = true;
            slot.policy = detail::ChunkStoragePolicy::Create(account, expected_upstream);
            ChunkStoragePolicyAccess::ReserveExistingBuffers(*slot.policy.get(), slot.original_bytes);
        }
        slot.admitted = true;
    }
    return prepared;
}

std::size_t TxnOriginalVariables::allocation_bytes() const noexcept {
    std::size_t result = 0U;
    for (std::size_t i = 0U; i < slot_count_; ++i)
        if (slots_[i].policy.get()) result += slots_[i].policy->allocation_bytes();
    return result;
}
void TxnOriginalVariables::Attach() noexcept {
    if (state_ != State::kPrepared) std::terminate();
    for (std::size_t i = 0U; i < buffers_.size(); ++i)
        if (buffers_[i] && needs_attach_[i])
            ChunkStoragePolicyAccess::Attach(buffers_[i], *slots_[buffer_slots_[i]].policy.get());
    state_ = State::kAttached;
}
void TxnOriginalVariables::RestoreRegular() noexcept {
    if (state_ != State::kAttached) std::terminate();
    for (std::size_t i = 0U; i < buffers_.size(); ++i)
        if (buffers_[i] && needs_attach_[i])
            ChunkStoragePolicyAccess::Detach(buffers_[i], *slots_[buffer_slots_[i]].policy.get());
    for (std::size_t i = 0U; i < slot_count_; ++i) {
        auto& slot = slots_[i];
        if (slot.created) ChunkStoragePolicyAccess::CancelExistingBuffers(*slot.policy.get(), slot.original_bytes);
        else slot.policy->TransferToRegularStorage();
        // A privately created control is freed through its original account;
        // undo never leaves it attached to previously ordinary source storage.
        slot.policy = {};
    }
    state_ = State::kRegular;
}
void TxnOriginalVariables::ConsumeHistoryAdmission(
    const std::shared_ptr<detail::ChunkStorageAccount>& destination) noexcept {
    if (state_ != State::kAttached) std::terminate();
    for (std::size_t i = 0U; i < slot_count_; ++i)
        ChunkStoragePolicyAccess::ConsumeHistoryAdmission(*slots_[i].policy.get(), destination);
    state_ = State::kHistory;
}

}  // namespace chunkdb
