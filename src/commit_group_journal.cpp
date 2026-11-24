#include "commit_group_journal.hpp"

#include <algorithm>
#include <array>
#include <optional>
#include <stdexcept>
#include <utility>

#include "chunkdb/crc32.hpp"

namespace chunkdb {
namespace {
constexpr std::size_t kHeader = kCommitRecordHeaderBytes;
constexpr std::size_t kSearchWindow = 64U * 1024U;

[[noreturn]] void Fail(const std::string& reason) { throw std::runtime_error("commit group journal: " + reason); }
std::string At(std::uint64_t value) { return std::to_string(value); }
// The codecs refuse with std::invalid_argument; this adds where the bytes were.
template <typename F> auto Admitted(const std::string& where, F&& call) {
    try {
        return call();
    } catch (const std::invalid_argument& error) {
        Fail(where + ": " + error.what());
    }
}
std::uint32_t Stored(std::span<const std::uint8_t> bytes, std::size_t at) {
    std::uint32_t value = 0U;
    for (unsigned i = 0U; i < 4U; ++i) value |= static_cast<std::uint32_t>(bytes[at + i]) << (8U * i);
    return value;
}
bool HeaderChecksum(std::span<const std::uint8_t> header) {
    return Stored(header, 60U) == Crc32(header.data(), 60U);
}
bool FinalChecksum(std::span<const std::uint8_t> record) {
    return Stored(record, record.size() - 4U) == Crc32(record.data(), record.size() - 4U);
}
bool FramedHeader(std::span<const std::uint8_t> bytes) {
    return bytes[0] == 'C' && bytes[1] == 'K' && bytes[2] == 'G' && bytes[3] == 'R' && HeaderChecksum(bytes);
}
std::string Where(bool certified, std::uint64_t offset) {
    return std::string(certified ? "certified record" : "record") + " at offset " + At(offset);
}
CommitRecordHeader Inspect(std::span<const std::uint8_t> header, const CommitGroupId& directory, const std::string& where) {
    const auto admitted = Admitted(where, [&] { return InspectCommitGroupRecordHeader(header, directory); });
    if (admitted.kind == CommitRecordKind::kSeal) Fail(where + ": seal records are not admitted in a live journal by this build");
    return admitted;
}
std::vector<std::uint8_t> ReadRecord(const CommitJournalReader& read, std::uint64_t offset,
    std::span<const std::uint8_t> header, std::uint64_t bytes) {
    std::vector<std::uint8_t> record(static_cast<std::size_t>(bytes));
    std::copy(header.begin(), header.end(), record.begin());
    read(offset + kHeader, std::span(record).subspan(kHeader));
    return record;
}
// Any checksum-valid record header after an invalid record is evidence of
// later framing, so the invalid record is damage rather than an interrupted append.
void RequireNoLaterFraming(const CommitJournalReader& read, std::uint64_t invalid, std::uint64_t from, std::uint64_t end) {
    std::vector<std::uint8_t> window;
    for (std::uint64_t at = from; at < end && end - at >= kHeader;) {
        const auto amount = static_cast<std::size_t>(std::min<std::uint64_t>(kSearchWindow, end - at));
        window.resize(amount);
        read(at, window);
        for (std::size_t i = 0U; i + kHeader <= amount; ++i) {
            if (FramedHeader(std::span(window).subspan(i, kHeader)))
                Fail("framing continues at offset " + At(at + i) + " after an invalid record at offset " + At(invalid));
        }
        if (amount == end - at) break;
        at += amount - (kHeader - 1U);
    }
}
}  // namespace

std::string CommitJournalLeafName(const CommitJournalContext& context) {
    Admitted("journal context", [&] { (void)SerializeCommitJournalHeader(context); });
    constexpr char digits[] = "0123456789abcdef";
    std::string name;
    name.reserve(49U);
    for (const auto byte : context.journal_id) { name.push_back(digits[byte >> 4U]); name.push_back(digits[byte & 15U]); }
    name.push_back('-');
    for (int shift = 60; shift >= 0; shift -= 4) name.push_back(digits[(context.generation >> shift) & 15U]);
    return name;
}

CommitJournalRecord CommitJournalGroups::Admit(std::span<const std::uint8_t> record, const CommitGroupId& directory,
    std::uint64_t offset, bool certified) {
    const auto where = Where(certified, offset);
    if (record.size() < kHeader) Fail(where + ": record is shorter than its header");
    const auto header = Inspect(record.first(kHeader), directory, where);
    if (header.record_bytes != record.size()) Fail(where + ": complete extent mismatch");
    CommitJournalRecord admitted{offset, record.size(), header.kind, header.id, crypto::Sha256Of(record)};
    if (header.kind == CommitRecordKind::kDecision) {
        Admitted(where, [&] { (void)ParseCommitGroupDecision(record, directory); });
        if (!groups_.emplace(header.id, Group{admitted.sha256, false}).second)
            Fail("conflicting decisions for one group at offset " + At(offset));
        return admitted;
    }
    const auto receipt = Admitted(where, [&] { return ParseCommitGroupReceipt(record, directory); });
    const auto found = groups_.find(header.id);
    if (found == groups_.end() || found->second.decision != receipt.decision_sha256)
        Fail("receipt at offset " + At(offset) + " has no matching decision");
    if (found->second.receipt) Fail("duplicate receipt for one group at offset " + At(offset));
    found->second.receipt = true;
    return admitted;
}

CommitJournalScan ScanCommitJournal(const CommitJournalInventory& certified, std::uint64_t measured,
    const CommitJournalReader& read, std::span<const std::uint64_t> cursors) {
    Admitted("certified inventory", [&] { (void)SerializeCommitJournalInventory(certified); });
    const auto& context = certified.context;
    const auto& directory = context.directory_id;
    const auto cut = certified.certified.bytes;
    if (measured < kCommitJournalHeaderBytes) Fail("journal is shorter than its header");
    std::array<std::uint8_t, kCommitJournalHeaderBytes> journal_header{};
    read(0U, journal_header);
    Admitted("header", [&] { (void)ParseCommitJournalHeader(journal_header, context); });
    if (measured < cut) Fail("journal is shorter than its certified cut " + At(cut));

    CommitJournalScan scan;
    scan.measured_bytes = measured;
    crypto::Sha256 prefix;
    prefix.Update(journal_header);
    CommitJournalCut last{kCommitJournalHeaderBytes, {}, CommitRecordKind::kNone, {}};
    std::vector<std::optional<CommitJournalPrefixProof>> found(cursors.size());
    const auto boundary = [&](std::uint64_t at) {
        if (std::find(cursors.begin(), cursors.end(), at) == cursors.end()) return;
        auto copy = prefix;
        last.bytes = at;
        last.sha256 = copy.Finish();
        for (std::size_t i = 0U; i < cursors.size(); ++i) {
            if (cursors[i] == at) found[i] = CommitJournalPrefixProof{context, last, at};
        }
    };

    std::uint64_t offset = kCommitJournalHeaderBytes;
    boundary(offset);
    std::array<std::uint8_t, kHeader> header{};
    while (offset < cut) {
        if (cut - offset < kHeader) Fail("certified cut " + At(cut) + " is between records");
        read(offset, header);
        // A damaged length field below the cut is damage, never an interrupted append.
        const auto declared = Inspect(header, directory, Where(true, offset));
        if (declared.record_bytes > cut - offset) Fail("certified cut " + At(cut) + " is between records");
        const auto record = ReadRecord(read, offset, header, declared.record_bytes);
        const auto admitted = scan.groups.Admit(record, directory, offset, true);
        prefix.Update(record);
        offset += record.size();
        last.last_group = admitted.group;
        last.last_kind = admitted.kind;
        boundary(offset);
    }
    auto copy = prefix;
    if (copy.Finish() != certified.certified.sha256) Fail("certified prefix digest mismatch");
    if (last.last_group != certified.certified.last_group || last.last_kind != certified.certified.last_kind)
        Fail("certified last record mismatch");
    scan.certified = {context, certified.certified, cut};
    Admitted("certified prefix", [&] { ValidateCommitJournalPrefix(certified, scan.certified); });
    scan.cursors.reserve(cursors.size());
    for (std::size_t i = 0U; i < cursors.size(); ++i) {
        if (!found[i]) Fail("cursor " + At(cursors[i]) + " is not a complete record boundary within the certified cut");
        scan.cursors.push_back(*found[i]);
    }

    scan.complete_bytes = offset;
    while (offset < measured && measured - offset >= kHeader) {
        const auto remaining = measured - offset;
        read(offset, header);
        if (!HeaderChecksum(header)) {
            RequireNoLaterFraming(read, offset, offset + 1U, measured);
            break;
        }
        const auto declared = Inspect(header, directory, Where(false, offset));
        // The remainder is the declared body itself, so no later framing can follow it.
        if (declared.record_bytes > remaining) break;
        const auto record = ReadRecord(read, offset, header, declared.record_bytes);
        if (!FinalChecksum(record)) {
            RequireNoLaterFraming(read, offset, offset + record.size(), measured);
            break;
        }
        scan.uncertified.push_back(scan.groups.Admit(record, directory, offset, false));
        offset += record.size();
        scan.complete_bytes = offset;
    }
    return scan;
}

}  // namespace chunkdb
