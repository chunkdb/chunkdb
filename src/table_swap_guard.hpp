#pragma once

#include <array>
#include <filesystem>
#include <string>
#include <string_view>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb {

inline constexpr std::string_view kTableSwapPendingFileName = "chunkdb.swap.pending";
inline constexpr char kFenceSwapPending[] = "a swap could not finish; RECOVER TABLE or a restart finishes it";

[[nodiscard]] bool TableSwapPending(const std::filesystem::path& root);
void RequireNoPendingTableSwap(const std::filesystem::path& root);

#ifdef _WIN32
// The caller holds the exclusive writer lock and catalog table operations.
// If this throws with recovery_required set, neither name may be reopened
// until recovery finishes the decision or makes its removal durable.
void SwapTableDirectoriesGuarded(const std::filesystem::path& root,
    const std::array<std::string, 2U>& names, const std::array<StoreId, 2U>& ids, bool& recovery_required);
#endif

// Startup runs under the exclusive writer lock. RECOVER TABLE runs under
// catalog table operations, and finishes only a swap that names its table.
void RecoverPendingTableSwap(const std::filesystem::path& root);
void RecoverFencedTableSwap(const std::filesystem::path& root, std::string_view name);
[[nodiscard]] bool ValidatePendingTableSwap(const std::filesystem::path& root);

}  // namespace chunkdb
