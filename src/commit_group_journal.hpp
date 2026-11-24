#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "commit_group_format.hpp"

namespace chunkdb {

// Journal generations live in this directory under the owner root.
inline constexpr std::string_view kCommitJournalDirectoryName = "chunkdb.groups.journal";

// "<32 lowercase hex journal id>-<16 lowercase hex generation>"; refuses a
// context with a zero identity or generation.
[[nodiscard]] std::string CommitJournalLeafName(const CommitJournalContext& context);

struct CommitJournalRecord {
    std::uint64_t offset = 0U;
    std::uint64_t bytes = 0U;
    CommitRecordKind kind = CommitRecordKind::kNone;
    CommitGroupId group{};
    crypto::Sha256Digest sha256{};
};

// Group rules over one journal generation: each group has at most one decision,
// and at most one receipt, which follows that decision and names its digest.
// Seal records are refused: this build places no seal in a live journal.
class CommitJournalGroups {
  public:
    // Completely admits one record that starts at `offset`; errors name it.
    CommitJournalRecord Admit(std::span<const std::uint8_t> record, const CommitGroupId& directory,
        std::uint64_t offset, bool certified);

  private:
    struct Group {
        crypto::Sha256Digest decision{};
        bool receipt = false;
    };
    std::map<CommitGroupId, Group> groups_;
};

struct CommitJournalScan {
    CommitJournalPrefixProof certified;             // measured at the inventory cut
    std::vector<CommitJournalPrefixProof> cursors;  // in request order
    std::vector<CommitJournalRecord> uncertified;   // complete admitted records above the cut
    std::uint64_t complete_bytes = kCommitJournalHeaderBytes;  // end of the last complete record
    std::uint64_t measured_bytes = kCommitJournalHeaderBytes;  // the tail is measured - complete
    CommitJournalGroups groups;                     // state after the last complete record
};

using CommitJournalReader = std::function<void(std::uint64_t offset, std::span<std::uint8_t> bytes)>;

// Admits a journal of `measured_bytes` against its certified inventory without
// materializing it. Every byte at or below the cut must match the inventory.
// Above the cut, a final record whose header or final checksum fails is an
// interrupted append only when no later checksum-valid record header follows;
// a checksum-valid record with an invalid structure is damage. Each cursor must
// be a complete record boundary within the cut. Refusals throw
// std::runtime_error naming the byte offset.
[[nodiscard]] CommitJournalScan ScanCommitJournal(const CommitJournalInventory& certified,
    std::uint64_t measured_bytes, const CommitJournalReader& read, std::span<const std::uint64_t> cursors = {});

}  // namespace chunkdb
