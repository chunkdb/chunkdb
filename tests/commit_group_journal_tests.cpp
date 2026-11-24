#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "chunkdb/crc32.hpp"
#include "commit_group_journal.hpp"
#include "owner_artifact_allocation_guard.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
const CommitGroupId kDirectory{0x11U}, kOtherDirectory{0x12U}, kA{0xA1U}, kB{0xB2U}, kC{0xC3U};
const CommitGroupId kJournalId{1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U, 9U, 10U, 11U, 12U, 13U, 14U, 15U, 16U};
const CommitJournalContext kContext{kDirectory, kJournalId, 0x102U};

template <typename F> void RejectWith(std::string_view reason, F action) {
    bool rejected = false;
    try { action(); }
    catch (const std::runtime_error& error) {
        rejected = std::string_view(error.what()).find(reason) != std::string_view::npos;
        if (!rejected) std::fprintf(stderr, "unexpected refusal: %s\n", error.what());
    }
    assert(rejected);
}
void Put(Bytes& bytes, std::size_t at, std::uint64_t value, unsigned width) {
    for (unsigned i = 0U; i < width; ++i) bytes[at + i] = static_cast<std::uint8_t>(value >> (8U * i));
}
void Recrc(Bytes& bytes, std::size_t record, std::size_t size) {
    Put(bytes, record + 60U, Crc32(bytes.data() + record, 60U), 4U);
    Put(bytes, record + size - 4U, Crc32(bytes.data() + record, size - 4U), 4U);
}
Bytes Decision(const CommitGroupId& id, std::uint64_t created = 1U, std::optional<Bytes> tag = {},
    const CommitGroupId& directory = kDirectory) {
    CommitGroupDecision decision;
    decision.directory_id = directory; decision.id = id; decision.created_ms = created; decision.tag = std::move(tag);
    return SerializeCommitGroupDecision(decision);
}
Bytes Receipt(const CommitGroupId& id, std::span<const std::uint8_t> decision) {
    const auto bytes = SerializeCommitGroupReceipt({kDirectory, id, crypto::Sha256Of(decision)});
    return {bytes.begin(), bytes.end()};
}
Bytes Seal(const Bytes& decision) {
    return SerializeCommitGroupSeal({ParseCommitGroupDecision(decision, kDirectory), crypto::Sha256Of(decision)});
}
struct Journal {
    Bytes bytes;
    std::vector<std::uint64_t> ends;
    explicit Journal(const CommitJournalContext& context = kContext) {
        const auto header = SerializeCommitJournalHeader(context);
        bytes.assign(header.begin(), header.end());
        ends.push_back(bytes.size());
    }
    Journal& Add(const Bytes& record) {
        bytes.insert(bytes.end(), record.begin(), record.end()); ends.push_back(bytes.size()); return *this;
    }
};
crypto::Sha256Digest Prefix(const Bytes& bytes, std::uint64_t end) {
    return crypto::Sha256Of(std::span(bytes).first(static_cast<std::size_t>(end)));
}
CommitJournalInventory Certify(const Bytes& bytes, std::uint64_t cut, const CommitGroupId& group = {},
    CommitRecordKind kind = CommitRecordKind::kNone, const CommitJournalContext& context = kContext) {
    return {context, 1U, {cut, group, kind, Prefix(bytes, cut)}};
}
CommitJournalScan Scan(const Bytes& bytes, const CommitJournalInventory& inventory, std::vector<std::uint64_t> cursors = {}) {
    const CommitJournalReader read = [&](std::uint64_t at, std::span<std::uint8_t> out) {
        assert(at <= bytes.size() && out.size() <= bytes.size() - at);
        std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(at), out.size(), out.begin());
    };
    return ScanCommitJournal(inventory, bytes.size(), read, cursors);
}

void LeafName() {
    assert(CommitJournalLeafName(kContext) == "0102030405060708090a0b0c0d0e0f10-0000000000000102");
    auto zero_id = kContext; zero_id.journal_id = {};
    RejectWith("journal context", [&] { (void)CommitJournalLeafName(zero_id); });
    auto zero_generation = kContext; zero_generation.generation = 0U;
    RejectWith("journal context", [&] { (void)CommitJournalLeafName(zero_generation); });
    auto zero_directory = kContext; zero_directory.directory_id = {};
    RejectWith("journal context", [&] { (void)CommitJournalLeafName(zero_directory); });
}
void Empty() {
    const Journal journal;
    const auto scan = Scan(journal.bytes, Certify(journal.bytes, 64U), {64U});
    assert(scan.certified.cut.bytes == 64U && scan.certified.last_complete_record_end == 64U);
    assert(scan.cursors.size() == 1U && scan.cursors[0].cut == Certify(journal.bytes, 64U).certified);
    assert(scan.uncertified.empty() && scan.complete_bytes == 64U && scan.measured_bytes == 64U);
}
void CertifiedSuffix() {
    const auto a = Decision(kA), b = Decision(kB);
    Journal journal; journal.Add(a).Add(Receipt(kA, a)).Add(b);
    const auto cut = journal.ends[2];
    const auto inventory = Certify(journal.bytes, cut, kA, CommitRecordKind::kReceipt);
    const auto scan = Scan(journal.bytes, inventory, {64U, journal.ends[1], cut});
    assert(scan.certified.cut == inventory.certified && scan.certified.context == kContext);
    assert(scan.uncertified.size() == 1U && scan.uncertified[0].group == kB && scan.uncertified[0].offset == cut);
    assert(scan.uncertified[0].kind == CommitRecordKind::kDecision && scan.uncertified[0].sha256 == crypto::Sha256Of(b));
    assert(scan.complete_bytes == journal.bytes.size() && scan.measured_bytes == journal.bytes.size());
    assert(scan.cursors.size() == 3U);
    assert(scan.cursors[0].cut == (CommitJournalCut{64U, {}, CommitRecordKind::kNone, Prefix(journal.bytes, 64U)}));
    assert(scan.cursors[1].cut == (CommitJournalCut{journal.ends[1], kA, CommitRecordKind::kDecision, Prefix(journal.bytes, journal.ends[1])}));
    assert(scan.cursors[2].cut == inventory.certified && scan.cursors[2].last_complete_record_end == cut);
    RejectWith("is not a complete record boundary", [&] { (void)Scan(journal.bytes, inventory, {74U}); });
    RejectWith("is not a complete record boundary", [&] { (void)Scan(journal.bytes, inventory, {journal.ends[3]}); });
    RejectWith("is not a complete record boundary", [&] { (void)Scan(journal.bytes, inventory, {0U}); });
}
void Truncation() {
    const auto a = Decision(kA), b = Decision(kB);
    Journal journal; journal.Add(a).Add(Receipt(kA, a)).Add(b);
    const auto cut = journal.ends[2];
    const auto inventory = Certify(journal.bytes, cut, kA, CommitRecordKind::kReceipt);
    for (std::size_t length = 0U; length <= journal.bytes.size(); ++length) {
        const Bytes prefix(journal.bytes.begin(), journal.bytes.begin() + static_cast<std::ptrdiff_t>(length));
        if (length < 64U) { RejectWith("shorter than its header", [&] { (void)Scan(prefix, inventory); }); continue; }
        if (length < cut) { RejectWith("shorter than its certified cut", [&] { (void)Scan(prefix, inventory); }); continue; }
        const auto scan = Scan(prefix, inventory);
        const auto boundary = length == journal.bytes.size() ? journal.bytes.size() : cut;
        assert(scan.complete_bytes == boundary && scan.measured_bytes == length);
        assert(scan.uncertified.size() == (boundary == cut ? 0U : 1U));
    }
}
void CertifiedBytes() {
    const auto a = Decision(kA);
    Journal journal; journal.Add(a).Add(Receipt(kA, a));
    const auto cut = journal.ends[2];
    const auto inventory = Certify(journal.bytes, cut, kA, CommitRecordKind::kReceipt);
    (void)Scan(journal.bytes, inventory);
    for (std::size_t at = 0U; at < cut; ++at) {
        auto damaged = journal.bytes; damaged[at] ^= 0x01U;
        // Throwing rather than returning is the proof that no certified byte becomes a tail.
        RejectWith("commit group journal:", [&] { (void)Scan(damaged, inventory); });
    }
    // A damaged length field of the last certified record is damage, not an interrupted append.
    auto lengthened = journal.bytes; Put(lengthened, journal.ends[1] + 8U, 101U, 8U); Put(lengthened, journal.ends[1] + 48U, 33U, 8U);
    Recrc(lengthened, journal.ends[1], 100U);
    RejectWith("certified record at offset", [&] { (void)Scan(lengthened, inventory); });
}
void TailShapes() {
    const auto a = Decision(kA), b = Decision(kB), c = Decision(kC);
    Journal base; base.Add(a);
    const auto cut = base.ends[1];
    const auto inventory = Certify(base.bytes, cut, kA, CommitRecordKind::kDecision);
    const auto tail = [&](const Bytes& bytes, std::uint64_t complete) {
        const auto scan = Scan(bytes, inventory);
        assert(scan.complete_bytes == complete && scan.measured_bytes == bytes.size());
    };
    auto final_crc = base; final_crc.Add(b); final_crc.bytes.back() ^= 0x01U; tail(final_crc.bytes, cut);
    auto header_crc = base; header_crc.Add(b); header_crc.bytes[cut + 60U] ^= 0x01U; tail(header_crc.bytes, cut);
    auto zeros = base; zeros.Add(b); zeros.bytes.resize(zeros.bytes.size() + 200U, 0U); tail(zeros.bytes, zeros.ends[2]);
    auto torn = base; torn.Add(b); torn.bytes.resize(torn.bytes.size() - 7U); tail(torn.bytes, cut);
    auto short_tail = base; short_tail.bytes.resize(short_tail.bytes.size() + 63U, 0xFFU); tail(short_tail.bytes, cut);
    // A non-final invalid record followed by checksum-valid framing is damage.
    auto interior = base; interior.Add(b).Add(c); interior.bytes[interior.ends[2] - 1U] ^= 0x01U;
    RejectWith("framing continues at offset " + std::to_string(interior.ends[2]), [&] { (void)Scan(interior.bytes, inventory); });
    auto interior_header = base; interior_header.Add(b).Add(c); interior_header.bytes[cut + 60U] ^= 0x01U;
    RejectWith("framing continues", [&] { (void)Scan(interior_header.bytes, inventory); });
    // A zeroed seal header leaves its nested decision header valid, so the scan
    // reports later framing; no build writes seals into a live journal.
    auto seal = base; seal.Add(Seal(b)); std::fill_n(seal.bytes.begin() + static_cast<std::ptrdiff_t>(cut), 64U, 0U);
    RejectWith("framing continues at offset " + std::to_string(cut + 104U), [&] { (void)Scan(seal.bytes, inventory); });
    // Payload bytes that form a checksum-valid header behave the same way.
    const auto header = Receipt(kC, c);
    auto embedded = base; embedded.Add(Decision(kB, 1U, Bytes(header.begin(), header.begin() + 64)));
    std::fill_n(embedded.bytes.begin() + static_cast<std::ptrdiff_t>(cut), 64U, 0U);
    RejectWith("framing continues", [&] { (void)Scan(embedded.bytes, inventory); });
    // With a valid header the search starts after the declared body, so the
    // embedded header of a torn final record is not mistaken for later framing.
    auto embedded_crc = base; embedded_crc.Add(Decision(kB, 1U, Bytes(header.begin(), header.begin() + 64)));
    embedded_crc.bytes.back() ^= 0x01U; tail(embedded_crc.bytes, cut);
    // A seal is refused wherever its header is valid, even when its body is torn.
    auto whole_seal = base; whole_seal.Add(Seal(b));
    RejectWith("seal records are not admitted", [&] { (void)Scan(whole_seal.bytes, inventory); });
    auto torn_seal = whole_seal; torn_seal.bytes.resize(torn_seal.bytes.size() - 20U);
    RejectWith("seal records are not admitted", [&] { (void)Scan(torn_seal.bytes, inventory); });
}
void Malformed() {
    Journal foreign; foreign.Add(Decision(kA, 1U, {}, kOtherDirectory));
    RejectWith("record at offset 64", [&] { (void)Scan(foreign.bytes, Certify(foreign.bytes, 64U)); });
    auto decision = Decision(kA);
    decision[64U + 2U] = 1U; Recrc(decision, 0U, decision.size());
    Journal structure; structure.Add(decision);
    RejectWith("record at offset 64", [&] { (void)Scan(structure.bytes, Certify(structure.bytes, 64U)); });
    RejectWith("certified record at offset 64", [&] {
        (void)Scan(structure.bytes, Certify(structure.bytes, structure.ends[1], kA, CommitRecordKind::kDecision));
    });
}
void Groups() {
    const auto a = Decision(kA), other = Decision(kA, 2U), b = Decision(kB);
    const auto scan = [](const Journal& journal) { return Scan(journal.bytes, Certify(journal.bytes, 64U)); };
    RejectWith("conflicting decisions for one group", [&] { (void)scan(Journal().Add(a).Add(other)); });
    RejectWith("conflicting decisions for one group", [&] { (void)scan(Journal().Add(a).Add(a)); });
    RejectWith("has no matching decision", [&] { (void)scan(Journal().Add(Receipt(kA, a))); });
    RejectWith("has no matching decision", [&] { (void)scan(Journal().Add(a).Add(Receipt(kA, other))); });
    RejectWith("has no matching decision", [&] { (void)scan(Journal().Add(a).Add(Receipt(kB, a))); });
    RejectWith("duplicate receipt", [&] { (void)scan(Journal().Add(a).Add(Receipt(kA, a)).Add(Receipt(kA, a))); });
    RejectWith("seal records are not admitted", [&] { (void)scan(Journal().Add(a).Add(Seal(a))); });
    // The rules span the certified cut.
    Journal spanning; spanning.Add(a).Add(b).Add(Receipt(kA, a));
    const auto accepted = Scan(spanning.bytes, Certify(spanning.bytes, spanning.ends[1], kA, CommitRecordKind::kDecision));
    assert(accepted.uncertified.size() == 2U && accepted.uncertified[1].kind == CommitRecordKind::kReceipt);
    Journal repeated; repeated.Add(a).Add(a);
    RejectWith("conflicting decisions for one group at offset", [&] {
        (void)Scan(repeated.bytes, Certify(repeated.bytes, repeated.ends[2], kA, CommitRecordKind::kDecision));
    });
}
void InventoryBinding() {
    Journal journal; journal.Add(Decision(kA));
    const auto cut = journal.ends[1];
    auto other_id = kContext; other_id.journal_id[0U] ^= 0x80U;
    RejectWith("header:", [&] { (void)Scan(journal.bytes, Certify(journal.bytes, cut, kA, CommitRecordKind::kDecision, other_id)); });
    auto other_generation = kContext; ++other_generation.generation;
    RejectWith("header:", [&] {
        (void)Scan(journal.bytes, Certify(journal.bytes, cut, kA, CommitRecordKind::kDecision, other_generation));
    });
    RejectWith("certified last record mismatch", [&] {
        (void)Scan(journal.bytes, Certify(journal.bytes, cut, kA, CommitRecordKind::kReceipt));
    });
    RejectWith("certified last record mismatch", [&] {
        (void)Scan(journal.bytes, Certify(journal.bytes, cut, kB, CommitRecordKind::kDecision));
    });
    Journal twin; twin.Add(Decision(kA, 2U));
    assert(twin.bytes.size() == journal.bytes.size() && twin.bytes != journal.bytes);
    const auto own = Certify(journal.bytes, cut, kA, CommitRecordKind::kDecision);
    (void)Scan(journal.bytes, own);
    RejectWith("certified prefix digest mismatch", [&] { (void)Scan(twin.bytes, own); });
    auto zero_generation = own; zero_generation.generation = 0U;
    RejectWith("certified inventory", [&] { (void)Scan(journal.bytes, zero_generation); });
}
void Oversized() {
    const auto a = Decision(kA);
    Journal journal; journal.Add(a).Add(Decision(kB));
    const auto at = journal.ends[1];
    Put(journal.bytes, at + 8U, kMaxCommitDecisionBytes, 8U);
    Put(journal.bytes, at + 48U, kMaxCommitDecisionBytes - 68U, 8U);
    Put(journal.bytes, at + 60U, Crc32(journal.bytes.data() + at, 60U), 4U);
    const auto above = Certify(journal.bytes, at, kA, CommitRecordKind::kDecision);
    auto below = above; below.certified.bytes = journal.bytes.size(); below.certified.last_group = kB;
    below.certified.sha256 = Prefix(journal.bytes, journal.bytes.size());
    const auto& bytes = journal.bytes;
    const CommitJournalReader read = [&](std::uint64_t offset, std::span<std::uint8_t> out) {
        std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(offset), out.size(), out.begin());
    };
    std::optional<CommitJournalScan> scan;
    scan.emplace();
    {
        owner_artifact_allocation::Guard guard(1024U * 1024U);
        const auto calls = owner_artifact_allocation::calls.load();
        const auto denied = owner_artifact_allocation::denied.load();
        *scan = ScanCommitJournal(above, bytes.size(), read);
        RejectWith("is between records", [&] { (void)ScanCommitJournal(below, bytes.size(), read); });
        assert(owner_artifact_allocation::calls.load() > calls);
        assert(owner_artifact_allocation::denied.load() == denied);
    }
    assert(scan->complete_bytes == at && scan->measured_bytes == bytes.size());
}
}  // namespace

int main(int argc, char** argv) {
    owner_artifact_allocation::CalibrateAligned();
    using Case = std::pair<std::string_view, void (*)()>;
    const std::array cases{Case{"leaf-name", LeafName}, Case{"empty", Empty}, Case{"certified-suffix", CertifiedSuffix},
        Case{"truncation", Truncation}, Case{"certified-bytes", CertifiedBytes}, Case{"tail-shapes", TailShapes},
        Case{"malformed", Malformed}, Case{"groups", Groups}, Case{"inventory-binding", InventoryBinding},
        Case{"oversized", Oversized}};
    if (argc == 3 && std::string_view(argv[1]) == "--case") {
        for (const auto& [name, run] : cases) if (name == argv[2]) { run(); return 0; }
        return 2;
    }
    if (argc != 1) return 2;
    for (const auto& [name, run] : cases) { (void)name; run(); }
}
