#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace chunkdb {

using CommitGroupId = std::array<std::uint8_t, 16>;

struct WalGroupRef {
    CommitGroupId id{};
    std::uint64_t member_count = 0;
    std::uint64_t member_ordinal = 0;
    friend bool operator==(const WalGroupRef&, const WalGroupRef&) = default;
};

inline constexpr std::size_t kWalGroupRefBytes = 32U;

// These codecs admit only the reference shape. The directory owner must
// validate the referenced complete group before publishing replayed state.
[[nodiscard]] std::array<std::uint8_t, kWalGroupRefBytes> SerializeWalGroupRef(const WalGroupRef& reference);
[[nodiscard]] WalGroupRef ParseWalGroupRef(std::span<const std::uint8_t> bytes);

}  // namespace chunkdb
