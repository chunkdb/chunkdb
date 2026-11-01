#include "commit_group_format.hpp"
#include "chunkdb/crc32.hpp"
#include "test_utils.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <new>
#include <string_view>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <winioctl.h>
#endif

namespace allocation {
std::atomic<bool> active{false};
std::atomic<std::size_t> limit{65536U}, largest{0U};
}
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void* operator new(std::size_t bytes) {
    if (allocation::active.load(std::memory_order_relaxed)) {
        auto previous = allocation::largest.load(std::memory_order_relaxed);
        while (previous < bytes && !allocation::largest.compare_exchange_weak(previous, bytes, std::memory_order_relaxed)) {}
        if (bytes > allocation::limit.load(std::memory_order_relaxed)) throw std::bad_alloc();
    }
    if (auto* pointer = std::malloc(bytes == 0U ? 1U : bytes)) return pointer;
    throw std::bad_alloc();
}
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void operator delete(void* pointer) noexcept { std::free(pointer); }
void* operator new[](std::size_t bytes) { return ::operator new(bytes); }
void operator delete[](void* pointer) noexcept { ::operator delete(pointer); }
void operator delete(void* pointer, std::size_t) noexcept { ::operator delete(pointer); }
void operator delete[](void* pointer, std::size_t) noexcept { ::operator delete(pointer); }

#include "aligned_allocation_replacement.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
const CommitGroupId kDirectory{0x11U}, kGroup{0x22U}, kTable{0x33U}, kOtherTable{0x44U};
const CommitJournalContext kJournal{kDirectory, CommitGroupId{0x55U}, 7U};

template <typename F> void Reject(F call) {
    bool refused = false;
    try { call(); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}
void Put(std::span<std::uint8_t> bytes, std::size_t at, std::uint64_t value, unsigned width = 8U) {
    assert(at <= bytes.size() && width <= bytes.size() - at);
    for (unsigned i = 0U; i < width; ++i) bytes[at + i] = static_cast<std::uint8_t>(value >> (8U * i));
}
void Crc(std::span<std::uint8_t> bytes, std::size_t at) { Put(bytes, at, Crc32(bytes.data(), at), 4U); }
void EnvelopeCrc(Bytes& bytes) { Crc(std::span(bytes).first(64U), 60U); Crc(bytes, bytes.size() - 4U); }
crypto::Sha256Digest Digest(std::uint8_t value) { crypto::Sha256Digest digest{}; digest.front() = value; return digest; }
CommitGroupDecision Observed() {
    CommitGroupDecision decision;
    decision.directory_id = kDirectory; decision.id = kGroup; decision.durability = CommitGroupDurability::kSynced;
    decision.created_ms = 123U; decision.tag.emplace();
    decision.members.push_back({.table_epoch = kTable, .dims = 3U, .kind = CommitGroupMemberKind::kObserved,
        .coord = {-1, 2, -3}, .revision = 42U, .commit_time_ms = 9U, .schema_version = 7U,
        .rollback_base = {}, .certified_prefix = {}});
    decision.versions.push_back({kTable, 42U, "old-name"}); return decision;
}
CommitGroupDecision Changed() {
    auto decision = Observed(); decision.txn = 77U;
    decision.request = CommitRequestBinding{"alice", {}, Digest(0x61U)};
    auto& member = decision.members.front(); member.kind = CommitGroupMemberKind::kChanged;
    member.wal_before_bytes = 72U; member.frame_offset = 72U; member.frame_bytes = 128U; member.frame_sha256 = Digest(0x62U);
    member.rollback_base = {CommitRollbackBaseKind::kImage, 11U, 8U, 6U, 512U, Digest(0x63U)};
    member.certified_prefix = {72U, Digest(0x64U)}; return decision;
}

void GoldenDecision() {
    const auto decision = Observed();
    Bytes golden(444U, 0U);
    std::copy_n("CKGR", 4U, golden.begin()); golden[4] = 1U; golden[6] = 1U;
    Put(golden, 8U, 444U); golden[16] = 0x11U; golden[32] = 0x22U; Put(golden, 48U, 376U);
    golden[64] = 1U; golden[65] = 2U; Put(golden, 80U, 123U); Put(golden, 88U, 1U); Put(golden, 96U, 1U);
    golden[152] = 0x33U; golden[168] = 3U; golden[169] = 2U;
    Put(golden, 176U, UINT64_MAX); Put(golden, 184U, 2U); Put(golden, 192U, UINT64_MAX - 2U);
    Put(golden, 200U, 42U); Put(golden, 208U, 9U); Put(golden, 216U, 7U);
    golden[400] = 0x33U; Put(golden, 416U, 42U); Put(golden, 424U, 8U, 4U);
    std::copy_n("old-name", 8U, golden.begin() + 432U); EnvelopeCrc(golden);
    assert(SerializeCommitGroupDecision(decision) == golden);
    assert(ParseCommitGroupDecision(golden, kDirectory) == decision);
    for (std::size_t cut = 0U; cut < golden.size(); ++cut)
        Reject([&] { (void)ParseCommitGroupDecision(std::span(golden).first(cut), kDirectory); });
    auto extra = golden; extra.push_back(0U); Reject([&] { (void)ParseCommitGroupDecision(extra, kDirectory); });
    Reject([&] { (void)ParseCommitGroupDecision(golden, CommitGroupId{9U}); });
    for (const auto offset : {7U, 56U, 66U, 116U, 170U, 289U, 428U}) {
        auto wrong = golden; wrong[offset] = 1U; EnvelopeCrc(wrong);
        Reject([&] { (void)ParseCommitGroupDecision(wrong, kDirectory); });
    }
    for (const auto offset : {280U, 424U}) {
        auto wrong = golden; Put(wrong, offset, 1U); EnvelopeCrc(wrong);
        Reject([&] { (void)ParseCommitGroupDecision(wrong, kDirectory); });
    }
    auto low_result = golden; Put(low_result, 416U, 41U); EnvelopeCrc(low_result);
    Reject([&] { (void)ParseCommitGroupDecision(low_result, kDirectory); });
    auto observed_extent = golden; Put(observed_extent, 240U, 1U); EnvelopeCrc(observed_extent);
    Reject([&] { (void)ParseCommitGroupDecision(observed_extent, kDirectory); });
    auto unknown_mode = golden; unknown_mode[64] = 2U; EnvelopeCrc(unknown_mode);
    Reject([&] { (void)ParseCommitGroupDecision(unknown_mode, kDirectory); });
}

void MembersAndPresence() {
    auto decision = Changed();
    auto second = decision.members.front(); second.table_epoch = kOtherTable; second.dims = 2U; second.coord.z = 0;
    second.wal_before_bytes = 0U; second.frame_offset = 64U; second.certified_prefix = {};
    second.rollback_base = {CommitRollbackBaseKind::kRetainedEmpty, 10U, 7U, 6U, 0U, {}};
    decision.members.push_back(second); decision.versions.push_back({kOtherTable, 42U, "renamed-later"});
    auto bytes = SerializeCommitGroupDecision(decision); assert(ParseCommitGroupDecision(bytes, kDirectory) == decision);
    auto relaxed = decision; relaxed.durability = CommitGroupDurability::kRelaxed;
    assert(ParseCommitGroupDecision(SerializeCommitGroupDecision(relaxed), kDirectory) == relaxed);
    auto present = decision; present.request->id = {0U, 0xFFU}; present.tag = Bytes{0U};
    assert(ParseCommitGroupDecision(SerializeCommitGroupDecision(present), kDirectory) == present);
    auto bad = decision; bad.members.front().frame_offset += 1U; Reject([&] { (void)SerializeCommitGroupDecision(bad); });
    bad = decision; bad.members.back().coord.z = 1; Reject([&] { (void)SerializeCommitGroupDecision(bad); });
    bad = decision; bad.members.front().rollback_base.revision = 42U; Reject([&] { (void)SerializeCommitGroupDecision(bad); });
    bad = decision; bad.members.front().certified_prefix.bytes = 73U; Reject([&] { (void)SerializeCommitGroupDecision(bad); });
    bad = decision; std::swap(bad.members[0], bad.members[1]); Reject([&] { (void)SerializeCommitGroupDecision(bad); });
    bad = decision; bad.txn = 0U; Reject([&] { (void)SerializeCommitGroupDecision(bad); });
    bad = decision; bad.request->id.resize(65U); Reject([&] { (void)SerializeCommitGroupDecision(bad); });
    bad = decision; bad.tag->resize(257U); Reject([&] { (void)SerializeCommitGroupDecision(bad); });
    auto empty = decision; empty.members.clear(); empty.versions.clear();
    assert(ParseCommitGroupDecision(SerializeCommitGroupDecision(empty), kDirectory) == empty);
    auto many = Observed(); many.members.clear(); many.versions.front().revision = 0U;
    for (std::int64_t x = 0; x < 1025; ++x)
        many.members.push_back({.table_epoch = kTable, .kind = CommitGroupMemberKind::kObserved,
            .coord = {x, 0, 0}, .rollback_base = {}, .certified_prefix = {}});
    assert(ParseCommitGroupDecision(SerializeCommitGroupDecision(many), kDirectory) == many);
}

void ExactBindings() {
    const auto decision = Changed(); const auto bytes = SerializeCommitGroupDecision(decision); const auto digest = crypto::Sha256Of(bytes);
    const CommitGroupReceipt receipt{kDirectory, kGroup, digest};
    const auto receipt_bytes = SerializeCommitGroupReceipt(receipt);
    assert(ParseCommitGroupReceipt(receipt_bytes, kDirectory) == receipt); ValidateCommitGroupReceipt(receipt, decision, digest);
    auto changed = decision; changed.created_ms += 1U;
    const auto different = SerializeCommitGroupDecision(changed); const auto different_digest = crypto::Sha256Of(different);
    assert(different.size() == bytes.size() && Crc32(bytes) == Crc32(different) && digest != different_digest);
    Reject([&] { ValidateCommitGroupReceipt(receipt, changed, different_digest); });
    const CommitGroupSeal seal{decision, digest}; const auto capsule = SerializeCommitGroupSeal(seal);
    assert(capsule.size() == bytes.size() + 108U && ParseCommitGroupSeal(capsule, kDirectory) == seal);
    auto replaced = capsule; std::copy(different.begin(), different.end(), replaced.begin() + 104U); EnvelopeCrc(replaced);
    Reject([&] { (void)ParseCommitGroupSeal(replaced, kDirectory); });
    changed = decision; changed.id[0] += 1U;
    Reject([&] { ValidateCommitGroupReceipt(receipt, changed, digest); });
    for (std::size_t cut = 0U; cut < capsule.size(); ++cut)
        Reject([&] { (void)ParseCommitGroupSeal(std::span(capsule).first(cut), kDirectory); });
    for (std::size_t cut = 0U; cut < receipt_bytes.size(); ++cut)
        Reject([&] { (void)ParseCommitGroupReceipt(std::span(receipt_bytes).first(cut), kDirectory); });
}

void JournalAndIdSnapshots() {
    const auto header = SerializeCommitJournalHeader(kJournal);
    assert(ParseCommitJournalHeader(header, kJournal) == kJournal);
    auto wrong_context = kJournal; ++wrong_context.generation;
    Reject([&] { (void)ParseCommitJournalHeader(header, wrong_context); });
    const auto decision = Changed(); const auto bytes = SerializeCommitGroupDecision(decision); const auto digest = crypto::Sha256Of(bytes);
    Bytes journal(header.begin(), header.end()); journal.insert(journal.end(), bytes.begin(), bytes.end());
    CommitJournalPrefixProof proof{kJournal, {journal.size(), kGroup, CommitRecordKind::kDecision, crypto::Sha256Of(journal)}, journal.size()};
    const CommitJournalInventory certified{kJournal, 3U, proof.cut};
    assert(ParseCommitJournalInventory(SerializeCommitJournalInventory(certified), kDirectory) == certified);
    ValidateCommitJournalPrefix(certified, proof);
    auto incomplete = proof; --incomplete.last_complete_record_end;
    Reject([&] { ValidateCommitJournalPrefix(certified, incomplete); });
    auto changed_proof = proof; changed_proof.cut.sha256[0] ^= 1U;
    Reject([&] { ValidateCommitJournalPrefix(certified, changed_proof); });
    const CommitIdEntry entry{decision.request->user, decision.request->id, kGroup, digest,
        decision.request->fingerprint, decision.created_ms};
    const CommitIdInventory ids{kJournal, 4U, proof.cut, {entry}};
    const auto encoded = SerializeCommitIdInventory(ids);
    assert(ParseCommitIdInventory(encoded, kJournal) == ids);
    ValidateCommitIdInventoryCursor(ids, certified, proof); ValidateCommitIdEntry(entry, kDirectory, decision, digest);
    Reject([&] { ValidateCommitIdEntry(entry, CommitGroupId{1U}, decision, digest); });
    auto altered_entry = entry; altered_entry.fingerprint[0] ^= 1U;
    Reject([&] { ValidateCommitIdEntry(altered_entry, kDirectory, decision, digest); });
    auto old_cut = certified; --old_cut.certified.bytes;
    Reject([&] { ValidateCommitIdInventoryCursor(ids, old_cut, proof); });
    auto before = ids; before.replay.bytes = 64U; before.replay.last_group = {}; before.replay.last_kind = CommitRecordKind::kNone;
    before.replay.sha256 = crypto::Sha256Of(header);
    ValidateCommitIdInventoryCursor(before, certified, {kJournal, before.replay, 64U});
    for (std::size_t cut = 0U; cut < encoded.size(); ++cut)
        Reject([&] { (void)ParseCommitIdInventory(std::span(encoded).first(cut), kJournal); });
    auto duplicated = ids; duplicated.entries.push_back(entry);
    Reject([&] { (void)SerializeCommitIdInventory(duplicated); });
    auto ordered = ids; auto next = entry; next.id = {1U}; next.group_id[0] += 1U; ordered.entries.push_back(next);
    const auto ordered_bytes = SerializeCommitIdInventory(ordered);
    const auto first_size = 112U + entry.user.size() + entry.id.size();
    Bytes reversed(ordered_bytes.begin(), ordered_bytes.begin() + 160U);
    reversed.insert(reversed.end(), ordered_bytes.begin() + static_cast<std::ptrdiff_t>(160U + first_size), ordered_bytes.end() - 4);
    reversed.insert(reversed.end(), ordered_bytes.begin() + 160U, ordered_bytes.begin() + static_cast<std::ptrdiff_t>(160U + first_size));
    reversed.resize(reversed.size() + 4U); Crc(reversed, reversed.size() - 4U);
    Reject([&] { (void)ParseCommitIdInventory(reversed, kJournal); });
    auto absent_request = decision; absent_request.request.reset(); absent_request.txn = 0U;
    Reject([&] { ValidateCommitIdEntry(entry, kDirectory, absent_request, digest); });
}

void AllocationAndSparseHeader() {
    allocation::largest = 0U; allocation::limit = 65536U; allocation::active = true;
    void* calibration = ::operator new(32768U);
    assert(calibration != nullptr); ::operator delete(calibration);
    allocation::active = false; assert(allocation::largest >= 32768U);
    const auto decision = SerializeCommitGroupDecision(Observed());
    Bytes huge_header(decision.begin(), decision.begin() + 64U);
    constexpr std::uint64_t huge = std::uint64_t{1U} << 40U;
    Put(huge_header, 8U, huge); Put(huge_header, 48U, huge - 68U); Crc(huge_header, 60U);
    test::ScopedTempDir directory("chunkdb-group-record-sparse"); const auto path = directory.path() / "journal";
    { std::ofstream create(path, std::ios::binary); assert(create); }
#ifdef _WIN32
    HANDLE file = CreateFileW(path.c_str(), GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    assert(file != INVALID_HANDLE_VALUE); DWORD returned = 0U;
    assert(DeviceIoControl(file, FSCTL_SET_SPARSE, nullptr, 0U, nullptr, 0U, &returned, nullptr));
    assert(CloseHandle(file));
#endif
    { std::fstream output(path, std::ios::binary | std::ios::in | std::ios::out);
      output.write(reinterpret_cast<const char*>(huge_header.data()), static_cast<std::streamsize>(huge_header.size()));
      output.seekp(static_cast<std::streamoff>(huge - 1U)); output.put('\0'); output.close(); assert(output); }
    assert(std::filesystem::file_size(path) == huge);
    std::array<std::uint8_t, 64U> measured{};
    { std::ifstream input(path, std::ios::binary); input.read(reinterpret_cast<char*>(measured.data()), measured.size()); assert(input); }
    allocation::largest = 0U; allocation::limit = 16384U; allocation::active = true;
    Reject([&] { (void)InspectCommitGroupRecordHeader(measured, kDirectory); });
    auto count = decision; Put(count, 88U, UINT64_MAX); EnvelopeCrc(count);
    Reject([&] { (void)ParseCommitGroupDecision(count, kDirectory); });
    allocation::active = false; assert(allocation::largest <= 16384U);
    CommitIdInventory ids{kJournal, 1U, {64U, {}, CommitRecordKind::kNone, crypto::Sha256Of(SerializeCommitJournalHeader(kJournal))}, {}};
    auto id_header = SerializeCommitIdInventory(ids); id_header.resize(160U);
    for (const auto count_value : {std::uint64_t{1U}, UINT64_MAX}) {
        auto impossible_count = id_header; Put(impossible_count, 72U, count_value); Crc(impossible_count, 156U);
        Reject([&] { (void)InspectCommitIdInventoryHeader(impossible_count, kJournal); });
    }
    Put(id_header, 8U, huge); Put(id_header, 72U, 1U); Put(id_header, 136U, huge - 164U); Crc(id_header, 156U);
    allocation::largest = 0U; allocation::active = true;
    Reject([&] { (void)InspectCommitIdInventoryHeader(id_header, kJournal); });
    allocation::active = false; assert(allocation::largest <= 16384U);
}
}  // namespace

int main() {
    allocation::limit = 128U; allocation::active = true;
    assert(aligned_allocation_test::Refused(1024U));
    allocation::active = false;
    aligned_allocation_test::Succeeds(1024U);
    allocation::limit = 65536U; allocation::largest = 0U;
    GoldenDecision(); MembersAndPresence(); ExactBindings(); JournalAndIdSnapshots(); AllocationAndSparseHeader();
}
