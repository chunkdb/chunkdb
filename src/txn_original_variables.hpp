#pragma once

#include <array>
#include <memory>

#include "chunk_vars_storage.hpp"
#include "chunk_storage_policy_access.hpp"

namespace chunkdb {

// The original variables and their policies are exclusively owned by the
// locked cohort. Declare this owner before retained vars so vars free first.
class TxnOriginalVariables {
  public:
    TxnOriginalVariables() noexcept = default;
    ~TxnOriginalVariables();
    TxnOriginalVariables(TxnOriginalVariables&& other) noexcept;
    TxnOriginalVariables& operator=(TxnOriginalVariables&&) = delete;
    TxnOriginalVariables(const TxnOriginalVariables&) = delete;
    TxnOriginalVariables& operator=(const TxnOriginalVariables&) = delete;
    [[nodiscard]] static TxnOriginalVariables Prepare(ChunkVars& vars,
        const std::shared_ptr<detail::ChunkStorageAccount>& account,
        std::pmr::memory_resource* expected_upstream = std::pmr::new_delete_resource());
    static void Validate(ChunkVars& vars,
        std::pmr::memory_resource* expected_upstream = std::pmr::new_delete_resource());
    [[nodiscard]] std::size_t allocation_bytes() const noexcept;
    void Attach() noexcept;
    void RestoreRegular() noexcept;
    void ConsumeHistoryAdmission(const std::shared_ptr<detail::ChunkStorageAccount>& destination) noexcept;

  private:
    enum class State { kEmpty, kPrepared, kAttached, kRegular, kHistory };
    struct Slot {
        detail::ChunkStoragePolicyPtr policy;
        std::size_t original_bytes = 0U;
        bool created = false;
        bool admitted = false;
    };
    std::array<Slot, 2U> slots_;
    std::array<detail::ChunkStoragePolicy*, 2U> policies_{};
    std::array<void*, 2U> buffers_{};
    std::array<std::size_t, 2U> buffer_slots_{};
    std::array<bool, 2U> needs_attach_{};
    std::size_t slot_count_ = 0U;
    State state_ = State::kEmpty;
    [[nodiscard]] static TxnOriginalVariables Inspect(ChunkVars& vars, std::pmr::memory_resource* expected_upstream);
};

}  // namespace chunkdb
