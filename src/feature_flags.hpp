#pragma once

#include <cstdint>
#include <string>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb {

// Bits this build implements. 2.0.0 defines none.
inline constexpr FeatureFlags kKnownFeatures{};

[[nodiscard]] constexpr FeatureFlags UnknownFeatures(const FeatureFlags& flags) noexcept {
    return FeatureFlags{
        .incompat = flags.incompat & ~kKnownFeatures.incompat,
        .ro_compat = flags.ro_compat & ~kKnownFeatures.ro_compat,
        .compat = flags.compat & ~kKnownFeatures.compat,
    };
}

[[nodiscard]] constexpr bool IsSubsetOf(const FeatureFlags& lhs, const FeatureFlags& rhs) noexcept {
    return (lhs.incompat & ~rhs.incompat) == 0U && (lhs.ro_compat & ~rhs.ro_compat) == 0U &&
           (lhs.compat & ~rhs.compat) == 0U;
}

// Whether a reader may skip a type it does not know inside a structure with
// these flags: only when the structure uses a feature this build does not
// know, which then owns the type. Opening rules (RequireOpenableFeatures)
// still refuse an unknown incompat feature and writing with an unknown
// ro_compat one.
[[nodiscard]] constexpr bool MaySkipUnknownTypes(const FeatureFlags& flags) noexcept {
    const auto unknown = UnknownFeatures(flags);
    return unknown.incompat != 0U || unknown.ro_compat != 0U || unknown.compat != 0U;
}

// "incompat=0x0 ro_compat=0x4 compat=0x0"
[[nodiscard]] std::string DescribeFeatures(const FeatureFlags& flags);

// Throws when a store with these flags may not be opened in `access_mode` by
// this build: any unknown incompat bit, or an unknown ro_compat bit for a
// read-write open. Unknown compat bits are allowed.
void RequireOpenableFeatures(const FeatureFlags& flags, AccessMode access_mode);

}  // namespace chunkdb
