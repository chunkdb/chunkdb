#pragma once

#include <array>
#include <cstdint>

namespace chunkdb {

// Native facts only; this value grants no manifest, lease or publication authority.
struct CommitOwnerNativeIdentity {
    std::uint64_t volume = 0U;
    std::array<std::uint8_t, 16U> file{};
    friend bool operator==(const CommitOwnerNativeIdentity&, const CommitOwnerNativeIdentity&) = default;
};

#ifdef _WIN32
[[nodiscard]] CommitOwnerNativeIdentity ReadCommitOwnerNativeIdentity(void* native_handle);
#endif

}  // namespace chunkdb
