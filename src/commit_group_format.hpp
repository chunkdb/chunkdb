#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "chunkdb/types.hpp"
#include "commit_group_identity.hpp"
#include "crypto.hpp"

namespace chunkdb {

inline constexpr std::size_t kCommitRecordHeaderBytes = 64U;
inline constexpr std::size_t kCommitGroupMemberBytes = 248U;
inline constexpr std::size_t kMaxCommitDecisionBytes = 256U * 1024U * 1024U;
inline constexpr std::size_t kCommitReceiptBytes = 100U;
inline constexpr std::size_t kMaxCommitSealBytes = kMaxCommitDecisionBytes + 108U;
inline constexpr std::size_t kCommitJournalHeaderBytes = 64U;
inline constexpr std::size_t kCommitJournalInventoryBytes = 128U;
inline constexpr std::size_t kCommitIdInventoryHeaderBytes = 160U;
inline constexpr std::size_t kMaxCommitIdInventoryBytes = 256U * 1024U * 1024U;

enum class CommitRecordKind : std::uint8_t { kNone = 0U, kDecision = 1U, kReceipt = 2U, kSeal = 3U };
enum class CommitGroupDurability : std::uint8_t { kRelaxed = 0U, kSynced = 1U };
enum class CommitGroupMemberKind : std::uint8_t { kChanged = 1U, kObserved = 2U };
enum class CommitRollbackBaseKind : std::uint8_t { kNeverWritten = 0U, kImage = 1U, kRetainedEmpty = 2U };

struct CommitRollbackBase {
    CommitRollbackBaseKind kind = CommitRollbackBaseKind::kNeverWritten;
    std::uint64_t revision = 0U;
    std::uint64_t commit_time_ms = 0U;
    std::uint64_t schema_version = 0U;
    std::uint64_t image_bytes = 0U;
    crypto::Sha256Digest image_sha256{};
    friend bool operator==(const CommitRollbackBase&, const CommitRollbackBase&) = default;
};

struct CommitWalPrefixCertificate {
    std::uint64_t bytes = 0U;
    crypto::Sha256Digest sha256{};
    friend bool operator==(const CommitWalPrefixCertificate&, const CommitWalPrefixCertificate&) = default;
};

struct CommitGroupMember {
    CommitGroupId table_epoch{};
    std::uint8_t dims = 2U;
    CommitGroupMemberKind kind = CommitGroupMemberKind::kChanged;
    ChunkCoord coord{};
    std::uint64_t revision = 0U;
    std::uint64_t commit_time_ms = 0U;
    std::uint64_t schema_version = 0U;
    std::uint64_t wal_before_bytes = 0U;
    std::uint64_t frame_offset = 0U;
    std::uint64_t frame_bytes = 0U;
    crypto::Sha256Digest frame_sha256{};
    CommitRollbackBase rollback_base;
    CommitWalPrefixCertificate certified_prefix;
    friend bool operator==(const CommitGroupMember&, const CommitGroupMember&) = default;
};

struct CommitTableResult {
    CommitGroupId table_epoch{};
    std::uint64_t revision = 0U;
    std::string table_name;
    friend bool operator==(const CommitTableResult&, const CommitTableResult&) = default;
};

struct CommitRequestBinding {
    std::string user;
    std::vector<std::uint8_t> id;
    crypto::Sha256Digest fingerprint{};
    friend bool operator==(const CommitRequestBinding&, const CommitRequestBinding&) = default;
};

struct CommitGroupDecision {
    CommitGroupId directory_id{};
    CommitGroupId id{};
    CommitGroupDurability durability = CommitGroupDurability::kRelaxed;
    std::uint64_t txn = 0U;
    std::uint64_t created_ms = 0U;
    std::optional<CommitRequestBinding> request;
    std::optional<std::vector<std::uint8_t>> tag;
    std::vector<CommitGroupMember> members;
    std::vector<CommitTableResult> versions;
    friend bool operator==(const CommitGroupDecision&, const CommitGroupDecision&) = default;
};

struct CommitGroupReceipt {
    CommitGroupId directory_id{};
    CommitGroupId id{};
    crypto::Sha256Digest decision_sha256{};
    friend bool operator==(const CommitGroupReceipt&, const CommitGroupReceipt&) = default;
};

struct CommitGroupSeal {
    CommitGroupDecision decision;
    crypto::Sha256Digest decision_sha256{};
    friend bool operator==(const CommitGroupSeal&, const CommitGroupSeal&) = default;
};

struct CommitRecordHeader {
    CommitRecordKind kind = CommitRecordKind::kNone;
    CommitGroupId directory_id{};
    CommitGroupId id{};
    std::uint64_t record_bytes = 0U;
    std::uint64_t payload_bytes = 0U;
    friend bool operator==(const CommitRecordHeader&, const CommitRecordHeader&) = default;
};

struct CommitJournalContext {
    CommitGroupId directory_id{};
    CommitGroupId journal_id{};
    std::uint64_t generation = 0U;
    friend bool operator==(const CommitJournalContext&, const CommitJournalContext&) = default;
};

struct CommitJournalCut {
    std::uint64_t bytes = kCommitJournalHeaderBytes;
    CommitGroupId last_group{};
    CommitRecordKind last_kind = CommitRecordKind::kNone;
    crypto::Sha256Digest sha256{};
    friend bool operator==(const CommitJournalCut&, const CommitJournalCut&) = default;
};

// The owner derives these facts by bounded reads and complete record admission.
struct CommitJournalPrefixProof {
    CommitJournalContext context;
    CommitJournalCut cut;
    std::uint64_t last_complete_record_end = kCommitJournalHeaderBytes;
};

struct CommitJournalInventory {
    CommitJournalContext context;
    std::uint64_t generation = 0U;
    CommitJournalCut certified;
    friend bool operator==(const CommitJournalInventory&, const CommitJournalInventory&) = default;
};

struct CommitIdEntry {
    std::string user;
    std::vector<std::uint8_t> id;
    CommitGroupId group_id{};
    crypto::Sha256Digest decision_sha256{};
    crypto::Sha256Digest fingerprint{};
    std::uint64_t created_ms = 0U;
    friend bool operator==(const CommitIdEntry&, const CommitIdEntry&) = default;
};

struct CommitIdInventory {
    CommitJournalContext context;
    std::uint64_t generation = 0U;
    CommitJournalCut replay;
    std::vector<CommitIdEntry> entries;
    friend bool operator==(const CommitIdInventory&, const CommitIdInventory&) = default;
};

struct CommitIdInventoryHeader {
    CommitJournalContext context;
    std::uint64_t generation = 0U;
    CommitJournalCut replay;
    std::uint64_t entry_count = 0U;
    std::uint64_t payload_bytes = 0U;
    std::uint64_t record_bytes = 0U;
};

// These codecs publish nothing and never authorize replay or ID resurrection.
// Header inspection bounds the declared buffer before allocation; the owner
// separately checks available file bytes before reading the complete extent.
[[nodiscard]] CommitRecordHeader InspectCommitGroupRecordHeader(
    std::span<const std::uint8_t> header, const CommitGroupId& expected_directory);
[[nodiscard]] std::vector<std::uint8_t> SerializeCommitGroupDecision(const CommitGroupDecision& decision);
[[nodiscard]] CommitGroupDecision ParseCommitGroupDecision(
    std::span<const std::uint8_t> bytes, const CommitGroupId& expected_directory);
[[nodiscard]] std::array<std::uint8_t, kCommitReceiptBytes> SerializeCommitGroupReceipt(const CommitGroupReceipt& receipt);
[[nodiscard]] CommitGroupReceipt ParseCommitGroupReceipt(
    std::span<const std::uint8_t> bytes, const CommitGroupId& expected_directory);
[[nodiscard]] std::vector<std::uint8_t> SerializeCommitGroupSeal(const CommitGroupSeal& seal);
[[nodiscard]] CommitGroupSeal ParseCommitGroupSeal(
    std::span<const std::uint8_t> bytes, const CommitGroupId& expected_directory);
void ValidateCommitGroupReceipt(const CommitGroupReceipt& receipt, const CommitGroupDecision& decision,
    const crypto::Sha256Digest& checked_decision_sha256);

[[nodiscard]] std::array<std::uint8_t, kCommitJournalHeaderBytes> SerializeCommitJournalHeader(const CommitJournalContext& context);
[[nodiscard]] CommitJournalContext ParseCommitJournalHeader(
    std::span<const std::uint8_t> bytes, const CommitJournalContext& expected);
[[nodiscard]] std::array<std::uint8_t, kCommitJournalInventoryBytes> SerializeCommitJournalInventory(const CommitJournalInventory& inventory);
[[nodiscard]] CommitJournalInventory ParseCommitJournalInventory(
    std::span<const std::uint8_t> bytes, const CommitGroupId& expected_directory);
void ValidateCommitJournalPrefix(const CommitJournalInventory& inventory, const CommitJournalPrefixProof& checked_prefix);

[[nodiscard]] std::vector<std::uint8_t> SerializeCommitIdInventory(const CommitIdInventory& inventory);
[[nodiscard]] CommitIdInventoryHeader InspectCommitIdInventoryHeader(
    std::span<const std::uint8_t> header, const CommitJournalContext& expected);
[[nodiscard]] CommitIdInventory ParseCommitIdInventory(
    std::span<const std::uint8_t> bytes, const CommitJournalContext& expected);
void ValidateCommitIdInventoryCursor(const CommitIdInventory& inventory, const CommitJournalInventory& certified,
    const CommitJournalPrefixProof& checked_cursor);
void ValidateCommitIdEntry(const CommitIdEntry& entry, const CommitGroupId& expected_directory, const CommitGroupDecision& decision,
    const crypto::Sha256Digest& checked_decision_sha256);

}  // namespace chunkdb
