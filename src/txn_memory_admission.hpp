#pragma once

#include <atomic>
#include <cstddef>

namespace chunkdb::detail {

// Lifetime-bound borrowed resources share the owned ledger's checked admission.
[[nodiscard]] bool TryReserveTxnGlobalBytes(std::atomic<std::size_t>& counter, std::size_t limit,
    std::size_t bytes, std::size_t& observed) noexcept;
void ReserveTxnGlobalBytes(std::atomic<std::size_t>& counter, std::size_t limit, std::size_t bytes);
void ReleaseTxnGlobalBytes(std::atomic<std::size_t>& counter, std::size_t bytes) noexcept;

}  // namespace chunkdb::detail
