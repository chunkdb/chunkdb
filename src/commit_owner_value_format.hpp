#pragma once

#include <array>
#include <optional>
#include <span>
#include <variant>
#include <vector>

#include "commit_group_owner_format.hpp"

namespace chunkdb {

// Checked facts only; none of these values owns native recovery or publication.
inline constexpr std::size_t kCommitOwnerEdgeKeyBytes = 49U;
inline constexpr std::size_t kCommitCounterBytes = 96U;

struct CommitOwnerIdentity {
    std::uint64_t revision = 0U, commit_time_ms = 0U, schema_version = 0U;
    friend bool operator==(const CommitOwnerIdentity&, const CommitOwnerIdentity&) = default;
};
struct CommitOwnerSet {
    CommitOwnerObjectRef root;
    std::uint64_t count = 0U, owned_bytes = 0U;
    crypto::Sha256Digest sha256{};
    friend bool operator==(const CommitOwnerSet&, const CommitOwnerSet&) = default;
};

enum class CommitOwnerNativeKind : std::uint8_t { kImage = 1U, kWalPrefix = 2U, kFeedSlots = 3U };
struct CommitOwnerNativeRef {
    CommitOwnerNativeKind kind = CommitOwnerNativeKind::kImage;
    std::uint64_t bytes = 0U;
    crypto::Sha256Digest sha256{};
    friend bool operator==(const CommitOwnerNativeRef&, const CommitOwnerNativeRef&) = default;
};
struct CommitOwnerJournalRecord {
    CommitJournalContext context;
    CommitRecordKind kind = CommitRecordKind::kDecision;
    std::uint64_t offset = 0U, bytes = 0U;
    crypto::Sha256Digest sha256{};
    friend bool operator==(const CommitOwnerJournalRecord&, const CommitOwnerJournalRecord&) = default;
};
struct CommitOwnerSealSource {
    std::uint64_t bytes = 0U;
    crypto::Sha256Digest sha256{};
    friend bool operator==(const CommitOwnerSealSource&, const CommitOwnerSealSource&) = default;
};
using CommitOwnerDecisionSource = std::variant<CommitOwnerJournalRecord, CommitOwnerSealSource>;

// Self-contained copies of admitted native context, not a partial inventory
// promoted to new authority. Later CKII replacement cannot invalidate them.
struct CommitOwnerRetainedProof {
    std::uint64_t inventory_bytes = 0U, record_offset = 0U;
    crypto::Sha256Digest inventory_sha256{};
    std::array<std::uint8_t, 60U> inventory_header{};
    std::array<std::uint8_t, 72U> inventory_entry{};
    std::array<std::uint8_t, 52U> log_header{};
    std::array<std::uint8_t, 60U> record{};
    friend bool operator==(const CommitOwnerRetainedProof&, const CommitOwnerRetainedProof&) = default;
};
struct CommitOwnerFloorProof {
    std::uint64_t inventory_bytes = 0U;
    crypto::Sha256Digest inventory_sha256{};
    std::array<std::uint8_t, 60U> inventory_header{};
    friend bool operator==(const CommitOwnerFloorProof&, const CommitOwnerFloorProof&) = default;
};
enum class CommitOwnerCoverageState : std::uint8_t {
    kNeverWritten = 0U, kImage = 1U, kRetainedEmpty = 2U, kForgottenEmpty = 3U
};
struct CommitOwnerCoverageValue {
    CommitOwnerScope scope;
    std::uint64_t generation = 1U;
    CommitOwnerCoverageKey key;
    CommitOwnerCoverageState state = CommitOwnerCoverageState::kNeverWritten;
    CommitOwnerIdentity identity;
    std::optional<CommitOwnerNativeRef> image;
    std::optional<CommitOwnerRetainedProof> retained;
    std::optional<CommitOwnerFloorProof> floor;
    CommitOwnerSet covered;
    std::optional<CommitOwnerMemberValue> producer;
    friend bool operator==(const CommitOwnerCoverageValue&, const CommitOwnerCoverageValue&) = default;
};

struct CommitOwnerNeverWrittenBase {
    friend bool operator==(const CommitOwnerNeverWrittenBase&, const CommitOwnerNeverWrittenBase&) = default;
};
struct CommitOwnerImageBase {
    CommitOwnerIdentity identity;
    CommitOwnerNativeRef image;
    friend bool operator==(const CommitOwnerImageBase&, const CommitOwnerImageBase&) = default;
};
using CommitOwnerObservationBase = std::variant<CommitOwnerNeverWrittenBase, CommitOwnerImageBase, CommitOwnerRetainedProof>;
struct CommitOwnerObservationWal {
    CommitOwnerObservationBase base;
    CommitOwnerNativeRef prefix{CommitOwnerNativeKind::kWalPrefix};
    friend bool operator==(const CommitOwnerObservationWal&, const CommitOwnerObservationWal&) = default;
};
using CommitOwnerObservationSource = std::variant<CommitOwnerNativeRef, CommitOwnerRetainedProof, CommitOwnerObservationWal>;
struct CommitOwnerObservation {
    CommitOwnerScope scope;
    std::uint64_t generation = 1U;
    CommitOwnerMemberValue member;
    CommitOwnerObservationSource source;
    friend bool operator==(const CommitOwnerObservation&, const CommitOwnerObservation&) = default;
};

enum class CommitOwnerDurabilityProof : std::uint8_t {
    kUnsettled = 0U, kSyncedDecision = 1U, kReceipt = 2U, kCompleteCoverage = 3U
};
struct CommitOwnerGroupValue {
    CommitOwnerScope scope;
    std::uint64_t generation = 1U;
    CommitGroupId group_id{};
    crypto::Sha256Digest decision_sha256{};
    CommitOwnerDecisionSource source;
    CommitOwnerDurabilityProof durability = CommitOwnerDurabilityProof::kUnsettled;
    std::optional<CommitOwnerJournalRecord> receipt;
    CommitOwnerSet members, owners, observations;
    friend bool operator==(const CommitOwnerGroupValue&, const CommitOwnerGroupValue&) = default;
};

enum class CommitOwnerEdgeClass : std::uint8_t {
    kProducer = 1U, kCovered = 2U, kRollback = 3U, kFeed = 4U, kId = 5U, kTransition = 6U
};
struct CommitOwnerProducerEdge {
    CommitOwnerCoverageKey coverage;
    CommitOwnerObjectRef value;
    std::uint64_t target_ordinal = 0U;
    friend bool operator==(const CommitOwnerProducerEdge&, const CommitOwnerProducerEdge&) = default;
};
struct CommitOwnerCoveredEdge {
    CommitOwnerCoverageKey coverage;
    CommitOwnerObjectRef value;
    std::uint64_t target_ordinal = 0U;
    friend bool operator==(const CommitOwnerCoveredEdge&, const CommitOwnerCoveredEdge&) = default;
};
struct CommitOwnerRollbackEdge {
    CommitGroupId source_group{};
    crypto::Sha256Digest source_decision_sha256{};
    CommitOwnerObjectRef source_value;
    std::uint64_t source_ordinal = 0U, target_ordinal = 0U;
    CommitOwnerCoverageKey coverage;
    CommitOwnerIdentity anchor;
    friend bool operator==(const CommitOwnerRollbackEdge&, const CommitOwnerRollbackEdge&) = default;
};
struct CommitOwnerFeedEdge {
    CommitOwnerCoverageKey coverage;
    CommitOwnerNativeRef slots{CommitOwnerNativeKind::kFeedSlots};
    std::array<char, 63U> slot_name{};
    std::uint8_t slot_name_bytes = 0U;
    std::uint64_t source_ordinal = 0U, frame_offset = 0U, frame_bytes = 0U;
    CommitOwnerNativeRef archive{CommitOwnerNativeKind::kWalPrefix};
    crypto::Sha256Digest frame_sha256{};
    friend bool operator==(const CommitOwnerFeedEdge&, const CommitOwnerFeedEdge&) = default;
};
struct CommitOwnerIdEdge {
    CommitOwnerObjectRef inventory;
    std::uint64_t generation = 0U, entry_offset = 0U, entry_bytes = 0U;
    crypto::Sha256Digest entry_sha256{}, key_sha256{};
    friend bool operator==(const CommitOwnerIdEdge&, const CommitOwnerIdEdge&) = default;
};
struct CommitOwnerTransitionEdge {
    CommitOwnerObjectRef source;
    std::uint64_t source_generation = 0U, participant_ordinal = 0U;
    friend bool operator==(const CommitOwnerTransitionEdge&, const CommitOwnerTransitionEdge&) = default;
};
using CommitOwnerEdgeSource = std::variant<CommitOwnerProducerEdge, CommitOwnerCoveredEdge, CommitOwnerRollbackEdge,
    CommitOwnerFeedEdge, CommitOwnerIdEdge, CommitOwnerTransitionEdge>;
struct CommitOwnerEdge {
    CommitOwnerScope scope;
    std::uint64_t generation = 1U;
    CommitGroupId group_id{};
    crypto::Sha256Digest decision_sha256{};
    CommitOwnerEdgeSource source;
    friend bool operator==(const CommitOwnerEdge&, const CommitOwnerEdge&) = default;
};

struct CommitCounterRecord {
    CommitOwnerScope scope;
    CommitGroupId counter_owner{};
    std::uint64_t generation = 1U, exclusive_ceiling = 1U;
    friend bool operator==(const CommitCounterRecord&, const CommitCounterRecord&) = default;
};

// The caller still measures exact native length and digest before any authority.
[[nodiscard]] std::vector<std::uint8_t> SerializeCommitOwnerCoverage(const CommitOwnerCoverageValue& value);
[[nodiscard]] CommitOwnerCoverageValue ParseCommitOwnerCoverage(std::span<const std::uint8_t> bytes,
    const CommitOwnerScope& scope, std::uint64_t selecting_generation, const CommitOwnerCoverageKey& key);
[[nodiscard]] std::vector<std::uint8_t> SerializeCommitOwnerGroup(const CommitOwnerGroupValue& value);
[[nodiscard]] CommitOwnerGroupValue ParseCommitOwnerGroup(std::span<const std::uint8_t> bytes,
    const CommitOwnerScope& scope, std::uint64_t selecting_generation, const CommitGroupId& key);
[[nodiscard]] std::vector<std::uint8_t> SerializeCommitOwnerObservation(const CommitOwnerObservation& value);
[[nodiscard]] CommitOwnerObservation ParseCommitOwnerObservation(std::span<const std::uint8_t> bytes,
    const CommitOwnerScope& scope, std::uint64_t selecting_generation, const CommitOwnerMemberKey& key);
[[nodiscard]] std::vector<std::uint8_t> SerializeCommitOwnerEdge(const CommitOwnerEdge& value);
[[nodiscard]] CommitOwnerEdge ParseCommitOwnerEdge(std::span<const std::uint8_t> bytes,
    const CommitOwnerScope& scope, std::uint64_t selecting_generation,
    std::span<const std::uint8_t, kCommitOwnerEdgeKeyBytes> key);
[[nodiscard]] std::array<std::uint8_t, kCommitOwnerEdgeKeyBytes> EncodeCommitOwnerEdgeKey(const CommitOwnerEdgeSource& source);
void ValidateCommitOwnerEdgeKey(std::span<const std::uint8_t> bytes);

[[nodiscard]] std::array<std::uint8_t, kCommitCounterBytes> SerializeCommitCounter(const CommitCounterRecord& value);
[[nodiscard]] CommitCounterRecord ParseCommitCounter(std::span<const std::uint8_t> bytes, const CommitOwnerScope& scope);
void ValidateCommitCounterSuccessor(const CommitCounterRecord& before, const CommitCounterRecord& after);
void ValidateCommitCounterBinding(const CommitOwnerRoot& root, const CommitCounterRecord& counter);

// A fixed-storage streaming digest, not a completed native set proof.
class CommitOwnerSetHasher {
  public:
    explicit CommitOwnerSetHasher(CommitOwnerTreeKind tree);
    void Add(std::span<const std::uint8_t> key, const CommitOwnerObjectRef& value);
    [[nodiscard]] crypto::Sha256Digest Finish();
    [[nodiscard]] std::uint64_t count() const noexcept { return count_; }
  private:
    crypto::Sha256 digest_;
    CommitOwnerTreeKind tree_;
    std::array<std::uint8_t, kCommitOwnerEdgeKeyBytes> previous_{};
    std::uint64_t count_ = 0U;
    bool finished_ = false;
};

class CommitOwnerIdKeyHasher {
  public:
    CommitOwnerIdKeyHasher(std::uint64_t principal_bytes, std::uint64_t id_bytes);
    void Principal(std::span<const std::uint8_t> bytes);
    void Id(std::span<const std::uint8_t> bytes);
    [[nodiscard]] crypto::Sha256Digest Finish();
  private:
    crypto::Sha256 digest_;
    std::uint64_t principal_remaining_, id_bytes_, id_remaining_;
    bool id_started_ = false, finished_ = false;
};

// Values are deliberately facts, not caller-constructible authorization tokens.
struct CommitOwnerDecisionFacts {
    CommitGroupId directory_id{}, group_id{};
    crypto::Sha256Digest sha256{};
    CommitGroupDurability durability = CommitGroupDurability::kRelaxed;
    std::uint64_t member_count = 0U, nonzero_observed_count = 0U;
};
void ValidateCommitOwnerGroupBinding(const CommitOwnerGroupValue& value, const CommitOwnerDecisionFacts& decision);
void ValidateCommitOwnerObservationBinding(const CommitOwnerObservation& observation, const CommitOwnerMemberValue& member);
void ValidateCommitOwnerEdgeBinding(const CommitOwnerEdge& edge, const CommitOwnerGroupValue& target);
void ValidateCommitOwnerProducerSource(const CommitOwnerEdge& edge, const CommitOwnerCoverageValue& coverage);
void ValidateCommitOwnerCoveredSource(const CommitOwnerEdge& edge, const CommitOwnerCoverageValue& coverage,
    const CommitOwnerMemberValue& member);
void ValidateCommitOwnerIdSource(const CommitOwnerEdge& edge, const CommitIdEntry& entry,
    const CommitOwnerObjectRef& selected_inventory, std::uint64_t inventory_generation,
    const crypto::Sha256Digest& measured_entry_sha256);
void ValidateCommitOwnerSetRoot(const CommitOwnerSet& set, CommitOwnerTreeKind tree,
    const CommitOwnerScope& scope, const CommitOwnerPage& page,
    const CommitOwnerObjectRef& measured, std::uint64_t selecting_generation);

}  // namespace chunkdb
