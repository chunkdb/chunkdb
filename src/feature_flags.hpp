#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb {

// Slot metadata, archived WALs, USER TLVs and collection frame flags.
inline constexpr std::uint32_t kFeatureFeedSlots = 1U;
// Data-directory migration ledger and redo journal; older writers must refuse.
inline constexpr std::uint32_t kFeatureMigrations = 2U;
// Immediate restore parent and the completed revision at its fork.
inline constexpr std::uint32_t kFeatureTableLineage = 4U;
// Persistent projection IDs, area/before/chunks filters and schema-filter loss.
inline constexpr std::uint32_t kFeatureSlotFilters = 8U;
// Checked multi-manifest activation decisions; belongs only to the directory.
inline constexpr std::uint32_t kFeatureActivation = 16U;

struct FeatureDefinition {
    std::string_view name;
    std::uint32_t incompat;
    bool table_scoped;
    std::string_view ownership;
};
inline constexpr std::array<FeatureDefinition, 5> kFeatureRegistry{{
    {"feed_slots", kFeatureFeedSlots, true, "slot records, feed archive and USER WAL metadata"},
    {"migrations", kFeatureMigrations, true, "migration ledger and redo decisions"},
    {"table_lineage", kFeatureTableLineage, true, "table parent-epoch and fork-revision TLVs"},
    {"slot_filters", kFeatureSlotFilters, true, "slot filter records and stable projection IDs"},
    {"feature_activation", kFeatureActivation, false, "directory feature activation decisions"},
}};
inline constexpr FeatureFlags kKnownFeatures{.incompat = kFeatureFeedSlots | kFeatureMigrations |
    kFeatureTableLineage | kFeatureSlotFilters | kFeatureActivation};
inline constexpr FeatureFlags kTableBaselineFeatures{.incompat =
    kKnownFeatures.incompat & ~kFeatureActivation};

[[nodiscard]] constexpr const FeatureDefinition* FindFeature(std::string_view name) noexcept {
    for (const auto& feature : kFeatureRegistry) {
        if (feature.name == name) return &feature;
    }
    return nullptr;
}

[[nodiscard]] constexpr FeatureFlags TableScopedFeatures(const FeatureFlags& flags) noexcept {
    return FeatureFlags{.incompat = flags.incompat & ~kFeatureActivation,
        .ro_compat = flags.ro_compat, .compat = flags.compat};
}

[[nodiscard]] constexpr FeatureFlags UnionFeatures(
    const FeatureFlags& lhs,
    const FeatureFlags& rhs) noexcept {
    return FeatureFlags{
        .incompat = lhs.incompat | rhs.incompat,
        .ro_compat = lhs.ro_compat | rhs.ro_compat,
        .compat = lhs.compat | rhs.compat,
    };
}

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
// know, which then owns the type. Callers pass the flags of the file united
// with the store's: a WAL created before a feature was enabled keeps its
// header but may hold the feature's records. Opening rules (RequireOpenableFeatures)
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
