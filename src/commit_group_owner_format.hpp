#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "commit_group_format.hpp"

namespace chunkdb {

inline constexpr std::size_t kCommitOwnerScopeBytes = 40U;
inline constexpr std::size_t kCommitOwnerReferenceBytes = 48U;
inline constexpr std::size_t kCommitOwnerHeaderBytes = 64U;
inline constexpr std::size_t kCommitOwnerRootBytes = 512U;
inline constexpr std::size_t kCommitOwnerEmptyPageBytes = 92U;
inline constexpr std::size_t kCommitOwnerMemberBytes = 204U;
inline constexpr std::size_t kMaxCommitOwnerPageBytes = 64U * 1024U;
inline constexpr std::uint64_t kMaxCommitCoveredTreeBytes = 256U * 1024U * 1024U;

enum class CommitOwnerRole : std::uint8_t { kStandalone = 1U, kCatalog = 2U };
enum class CommitOwnerObjectKind : std::uint8_t {
    kNone = 0U, kRoot = 1U, kPage = 2U, kCoverageValue = 3U, kGroupValue = 4U,
    kMemberValue = 5U, kObservation = 6U, kReplacementPlan = 7U, kRestoreProvenance = 8U,
    kJournalInventory = 9U, kIdInventory = 10U, kOwnerEdge = 11U
};
enum class CommitOwnerState : std::uint8_t { kInactive = 0U, kActive = 1U, kPending = 2U };
enum class CommitOwnerTreeKind : std::uint8_t {
    kCoverage = 1U, kGroups = 2U, kMembers = 3U, kOwnerEdges = 4U, kObservations = 5U
};
enum class CommitOwnerNodeKind : std::uint8_t { kEmpty = 0U, kLeaf = 1U, kBranch = 2U };

struct CommitOwnerScope {
    CommitGroupId directory_id{};
    CommitGroupId owner_id{};
    CommitOwnerRole role = CommitOwnerRole::kStandalone;
    friend bool operator==(const CommitOwnerScope&, const CommitOwnerScope&) = default;
};

struct CommitOwnerObjectRef {
    CommitOwnerObjectKind kind = CommitOwnerObjectKind::kNone;
    std::uint64_t bytes = 0U;
    crypto::Sha256Digest sha256{};
    friend bool operator==(const CommitOwnerObjectRef&, const CommitOwnerObjectRef&) = default;
};

struct CommitOwnerRoot {
    CommitOwnerScope scope;
    std::uint64_t generation = 1U;
    CommitOwnerState state = CommitOwnerState::kInactive;
    CommitOwnerObjectRef coverage_tree;
    CommitOwnerObjectRef group_tree;
    CommitOwnerObjectRef journal_inventory;
    CommitOwnerObjectRef id_inventory;
    CommitJournalContext reference_context;
    CommitJournalCut reference_replay{0U, {}, CommitRecordKind::kNone, {}};
    CommitGroupId counter_owner{};
    std::uint64_t counter_ceiling = 0U;
    CommitOwnerObjectRef replacement_plan;
    CommitOwnerObjectRef previous_root;
    friend bool operator==(const CommitOwnerRoot&, const CommitOwnerRoot&) = default;
};

struct CommitOwnerHeader {
    CommitOwnerObjectKind kind = CommitOwnerObjectKind::kNone;
    CommitOwnerScope scope;
    std::uint64_t generation = 0U;
    std::uint64_t bytes = 0U;
    friend bool operator==(const CommitOwnerHeader&, const CommitOwnerHeader&) = default;
};

struct CommitOwnerChild {
    std::uint8_t edge = 0U;
    std::uint64_t subtree_keys = 0U;
    std::uint64_t subtree_bytes = 0U;
    CommitOwnerObjectRef page;
    friend bool operator==(const CommitOwnerChild&, const CommitOwnerChild&) = default;
};

struct CommitOwnerPage {
    CommitOwnerScope scope;
    std::uint64_t generation = 1U;
    CommitOwnerTreeKind tree = CommitOwnerTreeKind::kCoverage;
    CommitOwnerNodeKind node = CommitOwnerNodeKind::kEmpty;
    std::vector<std::uint8_t> prefix;
    std::uint64_t subtree_keys = 0U;
    std::uint64_t subtree_bytes = kCommitOwnerEmptyPageBytes;
    std::optional<CommitOwnerObjectRef> value;
    std::vector<CommitOwnerChild> children;
    friend bool operator==(const CommitOwnerPage&, const CommitOwnerPage&) = default;
};

// Supplied only after complete replacement-plan admission; not a file or sync proof.
struct CommitOwnerPlanProof {
    CommitOwnerScope scope;
    CommitOwnerObjectRef plan;
    CommitOwnerObjectRef previous_root;
    CommitOwnerObjectRef final_root;
    CommitOwnerRoot previous;
    CommitOwnerRoot target;
};

// Native callers derive these facts with bounded reads and complete typed admission.
struct CommitOwnerSelectionProof {
    CommitOwnerObjectRef journal_object;
    CommitJournalInventory journal;
    CommitJournalPrefixProof certified_prefix;
    CommitOwnerObjectRef id_object;
    CommitIdInventoryHeader ids;
    CommitJournalPrefixProof id_cursor;
    CommitJournalPrefixProof reference_cursor;
};

struct CommitOwnerCoverageKey {
    CommitGroupId table_epoch{};
    std::uint8_t dims = 2U;
    ChunkCoord coord{};
    friend bool operator==(const CommitOwnerCoverageKey&, const CommitOwnerCoverageKey&) = default;
};
struct CommitOwnerMemberKey {
    CommitGroupId group_id{};
    std::uint64_t ordinal = 0U;
    friend bool operator==(const CommitOwnerMemberKey&, const CommitOwnerMemberKey&) = default;
};

struct CommitOwnerMemberValue {
    CommitOwnerScope scope;
    std::uint64_t generation = 1U;
    CommitGroupId group_id{};
    crypto::Sha256Digest decision_sha256{};
    std::uint64_t ordinal = 0U;
    std::uint64_t member_count = 0U;
    CommitOwnerCoverageKey coverage;
    CommitGroupMemberKind kind = CommitGroupMemberKind::kChanged;
    std::uint64_t revision = 0U;
    std::uint64_t commit_time_ms = 0U;
    std::uint64_t schema_version = 0U;
    friend bool operator==(const CommitOwnerMemberValue&, const CommitOwnerMemberValue&) = default;
};

[[nodiscard]] std::array<std::uint8_t, kCommitOwnerScopeBytes> SerializeCommitOwnerScope(const CommitOwnerScope& scope);
[[nodiscard]] CommitOwnerScope ParseCommitOwnerScope(std::span<const std::uint8_t> bytes);
void ValidateCommitOwnerStoreScope(const CommitOwnerScope& scope, const CommitGroupId& store_id,
    const std::optional<CommitOwnerScope>& directory_scope = {});

[[nodiscard]] CommitOwnerHeader InspectCommitOwnerHeader(std::span<const std::uint8_t> bytes,
    const CommitOwnerScope& expected, CommitOwnerObjectKind expected_kind,
    std::optional<std::uint64_t> maximum_generation = {});
[[nodiscard]] std::array<std::uint8_t, kCommitOwnerRootBytes> SerializeCommitOwnerRoot(const CommitOwnerRoot& root);
[[nodiscard]] CommitOwnerRoot ParseCommitOwnerRoot(std::span<const std::uint8_t> bytes,
    const CommitOwnerScope& expected, std::optional<std::uint64_t> expected_generation = {});
void ValidateCommitOwnerRootSuccessor(const CommitOwnerRoot& previous, const CommitOwnerRoot& next,
    const std::optional<CommitOwnerPlanProof>& plan = {});
void ValidateCommitOwnerSelections(const CommitOwnerRoot& root, const CommitOwnerSelectionProof& checked);
void ValidateCommitOwnerCounter(const CommitOwnerRoot& root, const CommitGroupId& counter_owner,
    std::uint64_t reserved_ceiling);

[[nodiscard]] std::vector<std::uint8_t> SerializeCommitOwnerPage(const CommitOwnerPage& page);
[[nodiscard]] CommitOwnerPage ParseCommitOwnerPage(std::span<const std::uint8_t> bytes,
    const CommitOwnerScope& expected, CommitOwnerTreeKind tree, std::uint64_t selecting_generation);
void ValidateCommitOwnerRootPage(const CommitOwnerRoot& root, CommitOwnerTreeKind tree,
    const CommitOwnerPage& page, const CommitOwnerObjectRef& checked_object);
void ValidateCommitOwnerChild(const CommitOwnerPage& parent, std::size_t child_index,
    const CommitOwnerPage& child, const CommitOwnerObjectRef& checked_object,
    std::uint64_t selecting_generation);

[[nodiscard]] std::array<std::uint8_t, 41U> EncodeCommitOwnerCoverageKey(const CommitOwnerCoverageKey& key);
[[nodiscard]] CommitOwnerCoverageKey DecodeCommitOwnerCoverageKey(std::span<const std::uint8_t> bytes);
[[nodiscard]] std::array<std::uint8_t, 16U> EncodeCommitOwnerGroupKey(const CommitGroupId& id);
[[nodiscard]] CommitGroupId DecodeCommitOwnerGroupKey(std::span<const std::uint8_t> bytes);
[[nodiscard]] std::array<std::uint8_t, 24U> EncodeCommitOwnerMemberKey(const CommitOwnerMemberKey& key);
[[nodiscard]] CommitOwnerMemberKey DecodeCommitOwnerMemberKey(std::span<const std::uint8_t> bytes);

[[nodiscard]] std::array<std::uint8_t, kCommitOwnerMemberBytes> SerializeCommitOwnerMember(
    const CommitOwnerMemberValue& value);
[[nodiscard]] CommitOwnerMemberValue ParseCommitOwnerMember(std::span<const std::uint8_t> bytes,
    const CommitOwnerScope& scope, std::uint64_t selecting_generation);
// All decision facts and the raw digest must come from complete native admission.
void ValidateCommitOwnerMemberBinding(const CommitOwnerMemberValue& value,
    const CommitOwnerMemberKey& key, const CommitOwnerCoverageKey& coverage,
    const CommitGroupDecision& decision, const crypto::Sha256Digest& measured_decision_sha256);

}  // namespace chunkdb
