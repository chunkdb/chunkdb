#include "commit_owner_value_format.hpp"

#include <algorithm>
#include <bit>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>

#include "chunkdb/crc32.hpp"
#include "chunk_file_header.hpp"
#include "feed_slot_records.hpp"

namespace chunkdb {
namespace {
using Bytes = std::span<const std::uint8_t>;
[[noreturn]] void Bad(const char* reason) { throw std::invalid_argument(std::string("commit owner value: ") + reason); }
template <class Range> bool Zero(const Range& bytes) { return std::all_of(bytes.begin(), bytes.end(), [](auto byte) { return byte == 0; }); }
void Zeros(Bytes bytes) { if (!Zero(bytes)) Bad("reserved bytes"); }
void Id(const CommitGroupId& id) { if (Zero(id)) Bad("zero identity"); }
void Digest(const crypto::Sha256Digest& sha) { if (Zero(sha)) Bad("zero digest"); }
void Scope(const CommitOwnerScope& scope, std::uint64_t generation) {
    (void)SerializeCommitOwnerScope(scope); if (generation == 0U) Bad("zero generation");
}
std::uint64_t Le(Bytes bytes) {
    std::uint64_t result = 0U;
    for (std::size_t i = 0U; i < bytes.size(); ++i) result |= static_cast<std::uint64_t>(bytes[i]) << (8U * i);
    return result;
}
void Put(std::span<std::uint8_t> bytes, std::uint64_t value) {
    for (std::size_t i = 0U; i < bytes.size(); ++i) bytes[i] = static_cast<std::uint8_t>(value >> (8U * i));
}
void Crc(Bytes bytes) {
    if (bytes.size() < 4U || Le(bytes.last(4U)) != Crc32(bytes.data(), bytes.size() - 4U)) Bad("checksum");
}
class Reader {
  public:
    explicit Reader(Bytes bytes) : bytes_(bytes) {}
    Bytes Take(std::size_t count) {
        if (count > bytes_.size()) Bad("truncated payload");
        const auto result = bytes_.first(count); bytes_ = bytes_.subspan(count); return result;
    }
    std::uint64_t UInt(unsigned width) { return Le(Take(width)); }
    template <std::size_t N> std::array<std::uint8_t, N> Array() {
        std::array<std::uint8_t, N> result{}; const auto bytes = Take(N); std::copy(bytes.begin(), bytes.end(), result.begin()); return result;
    }
    void ZeroBytes(std::size_t count) { Zeros(Take(count)); }
    void End() const { if (!bytes_.empty()) Bad("bytes after payload"); }
  private: Bytes bytes_;
};
class Writer {
  public:
    void UInt(std::uint64_t value, unsigned width) { Put(Take(width), value); }
    void Raw(Bytes bytes) { auto out = Take(bytes.size()); std::copy(bytes.begin(), bytes.end(), out.begin()); }
    void ZeroBytes(std::size_t count) { auto out = Take(count); std::fill(out.begin(), out.end(), 0U); }
    std::span<std::uint8_t> Take(std::size_t count) {
        if (count > bytes_.size() - used_) Bad("value exceeds bounded extent");
        const auto out = std::span(bytes_).subspan(used_, count); used_ += count; return out;
    }
    void Header(const CommitOwnerScope& scope, std::uint64_t generation, CommitOwnerObjectKind kind) {
        Scope(scope, generation);
        const std::array<std::uint8_t, 4U> magic{'C', 'K', 'G', 'O'};
        Raw(magic); UInt(1U, 2U); UInt(static_cast<std::uint8_t>(kind), 1U); UInt(static_cast<std::uint8_t>(scope.role), 1U);
        UInt(0U, 8U); Raw(scope.directory_id); Raw(scope.owner_id); UInt(generation, 8U); ZeroBytes(8U);
    }
    std::vector<std::uint8_t> Finish() {
        ZeroBytes(4U); Put(std::span(bytes_).subspan(8U, 8U), used_);
        Put(std::span(bytes_).subspan(60U, 4U), Crc32(bytes_.data(), 60U));
        Put(std::span(bytes_).subspan(used_ - 4U, 4U), Crc32(bytes_.data(), used_ - 4U));
        return {bytes_.begin(), bytes_.begin() + static_cast<std::ptrdiff_t>(used_)};
    }
  private:
    std::array<std::uint8_t, kMaxCommitOwnerPageBytes> bytes_{};
    std::size_t used_ = 0U;
};
Reader Payload(Bytes bytes, const CommitOwnerScope& scope, std::uint64_t selecting,
    CommitOwnerObjectKind kind, CommitOwnerHeader& header) {
    if (selecting == 0U || bytes.size() < 68U) Bad("value extent or selecting generation");
    header = InspectCommitOwnerHeader(bytes.first(64U), scope, kind, selecting);
    if (header.bytes != bytes.size()) Bad("complete value extent");
    Crc(bytes); return Reader(bytes.subspan(64U, bytes.size() - 68U));
}
void Ref(const CommitOwnerObjectRef& ref, CommitOwnerObjectKind kind) {
    if (ref.kind != kind) Bad("reference field kind");
    Digest(ref.sha256);
    std::uint64_t low = 68U, high = kMaxCommitOwnerPageBytes;
    switch (kind) {
        case CommitOwnerObjectKind::kPage: low = kCommitOwnerEmptyPageBytes; break;
        case CommitOwnerObjectKind::kRoot: low = high = kCommitOwnerRootBytes; break;
        case CommitOwnerObjectKind::kMemberValue: low = high = kCommitOwnerMemberBytes; break;
        case CommitOwnerObjectKind::kIdInventory: low = 164U; high = kMaxCommitIdInventoryBytes; break;
        case CommitOwnerObjectKind::kJournalInventory: low = high = kCommitJournalInventoryBytes; break;
        case CommitOwnerObjectKind::kCoverageValue: case CommitOwnerObjectKind::kGroupValue:
        case CommitOwnerObjectKind::kObservation: case CommitOwnerObjectKind::kOwnerEdge:
        case CommitOwnerObjectKind::kReplacementPlan: case CommitOwnerObjectKind::kRestoreProvenance: break;
        default: Bad("reference kind");
    }
    if (ref.bytes < low || ref.bytes > high) Bad("reference extent");
}
void WriteRef(Writer& out, const CommitOwnerObjectRef& ref) {
    Ref(ref, ref.kind); out.UInt(static_cast<std::uint8_t>(ref.kind), 1U); out.ZeroBytes(7U); out.UInt(ref.bytes, 8U); out.Raw(ref.sha256);
}
CommitOwnerObjectRef ReadRef(Reader& in) {
    const auto kind = static_cast<CommitOwnerObjectKind>(in.UInt(1U)); in.ZeroBytes(7U);
    CommitOwnerObjectRef ref{kind, in.UInt(8U), in.Array<32U>()}; Ref(ref, kind); return ref;
}
std::size_t Width(CommitOwnerTreeKind tree) {
    switch (tree) {
        case CommitOwnerTreeKind::kCoverage: return 41U;
        case CommitOwnerTreeKind::kGroups: return 16U;
        case CommitOwnerTreeKind::kMembers: case CommitOwnerTreeKind::kObservations: return 24U;
        case CommitOwnerTreeKind::kOwnerEdges: return kCommitOwnerEdgeKeyBytes;
    }
    Bad("set tree kind");
}
CommitOwnerObjectKind ValueKind(CommitOwnerTreeKind tree) {
    switch (tree) {
        case CommitOwnerTreeKind::kCoverage: return CommitOwnerObjectKind::kCoverageValue;
        case CommitOwnerTreeKind::kGroups: return CommitOwnerObjectKind::kGroupValue;
        case CommitOwnerTreeKind::kMembers: return CommitOwnerObjectKind::kMemberValue;
        case CommitOwnerTreeKind::kOwnerEdges: return CommitOwnerObjectKind::kOwnerEdge;
        case CommitOwnerTreeKind::kObservations: return CommitOwnerObjectKind::kObservation;
    }
    Bad("set tree kind");
}
void Set(const CommitOwnerSet& set, CommitOwnerTreeKind tree) {
    Ref(set.root, CommitOwnerObjectKind::kPage); Digest(set.sha256); (void)Width(tree);
    if (set.owned_bytes < set.root.bytes || set.owned_bytes > kMaxCommitCoveredTreeBytes || set.count > set.owned_bytes / 68U)
        Bad("set counts or owned bytes");
    if (set.count == 0U && (set.root.bytes != kCommitOwnerEmptyPageBytes || set.owned_bytes != kCommitOwnerEmptyPageBytes ||
            set.sha256 != CommitOwnerSetHasher(tree).Finish())) Bad("empty set commitment");
}
void WriteSet(Writer& out, const CommitOwnerSet& set, CommitOwnerTreeKind tree) {
    Set(set, tree); WriteRef(out, set.root); out.UInt(set.count, 8U); out.UInt(set.owned_bytes, 8U); out.Raw(set.sha256);
}
CommitOwnerSet ReadSet(Reader& in, CommitOwnerTreeKind tree) {
    CommitOwnerSet set{ReadRef(in), in.UInt(8U), in.UInt(8U), in.Array<32U>()}; Set(set, tree); return set;
}
void Identity(const CommitOwnerIdentity& identity) {
    if (identity.revision == 0U || identity.schema_version == 0U) Bad("producing identity");
}
void EmptyIdentity(const CommitOwnerIdentity& identity) {
    if (identity != CommitOwnerIdentity{}) Bad("absent producing identity");
}
void WriteIdentity(Writer& out, const CommitOwnerIdentity& identity) {
    out.UInt(identity.revision, 8U); out.UInt(identity.commit_time_ms, 8U); out.UInt(identity.schema_version, 8U);
}
CommitOwnerIdentity ReadIdentity(Reader& in) { return {in.UInt(8U), in.UInt(8U), in.UInt(8U)}; }
void Native(const CommitOwnerNativeRef& ref, CommitOwnerNativeKind kind) {
    Digest(ref.sha256);
    if (ref.kind != kind || ref.bytes == 0U || ref.bytes > std::numeric_limits<std::uint64_t>::max() / 8U)
        Bad("native source kind or extent");
    if (kind == CommitOwnerNativeKind::kFeedSlots && (ref.bytes < 40U || ref.bytes > 1024U * 1024U)) Bad("feed source extent");
}
void WriteNative(Writer& out, const CommitOwnerNativeRef& ref) {
    out.UInt(static_cast<std::uint8_t>(ref.kind), 1U); out.ZeroBytes(7U); out.UInt(ref.bytes, 8U); out.Raw(ref.sha256);
}
CommitOwnerNativeRef ReadNative(Reader& in) {
    const auto kind = static_cast<CommitOwnerNativeKind>(in.UInt(1U)); in.ZeroBytes(7U);
    return {kind, in.UInt(8U), in.Array<32U>()};
}
void WriteKey(Writer& out, const CommitOwnerCoverageKey& key) { out.Raw(EncodeCommitOwnerCoverageKey(key)); }
CommitOwnerCoverageKey ReadKey(Reader& in) { return DecodeCommitOwnerCoverageKey(in.Take(41U)); }
void Match(Bytes actual, Bytes expected) {
    if (!std::equal(actual.begin(), actual.end(), expected.begin(), expected.end())) Bad("copied native context");
}
struct InventoryFacts { std::uint64_t floor, count, generation; };
InventoryFacts Inventory(Bytes header, std::uint64_t bytes, const crypto::Sha256Digest& sha, const CommitOwnerCoverageKey& key) {
    (void)EncodeCommitOwnerCoverageKey(key); Digest(sha); Crc(header);
    const std::array<std::uint8_t, 4U> magic{'C', 'K', 'I', 'I'}; Match(header.first(4U), magic);
    if (Le(header.subspan(4U, 2U)) != 4U || header[6U] != key.dims || header[7U] != 0U) Bad("copied inventory tag or pending context");
    Match(header.subspan(8U, 16U), key.table_epoch);
    const auto count = Le(header.subspan(40U, 8U)), generation = Le(header.subspan(48U, 8U));
    const auto entry_bytes = 48U + 8U * key.dims;
    if (generation == 0U || Le(header.subspan(32U, 8U)) != 0U ||
        count > (std::numeric_limits<std::uint64_t>::max() - 64U) / entry_bytes || bytes != 64U + count * entry_bytes ||
        bytes > std::numeric_limits<std::uint64_t>::max() / 8U) Bad("copied inventory extent or generation");
    return {Le(header.subspan(24U, 8U)), count, generation};
}
CommitOwnerIdentity Retained(const CommitOwnerRetainedProof& proof, const CommitOwnerCoverageKey& key) {
    const auto context = Inventory(proof.inventory_header, proof.inventory_bytes, proof.inventory_sha256, key);
    if (context.count == 0U) Bad("retained proof has no inventory entry");
    const auto dims = key.dims;
    const auto coords = 8U * dims, log_header_bytes = 28U + coords, record_bytes = 36U + coords, entry_bytes = 48U + coords;
    const auto header = Bytes(proof.log_header).first(log_header_bytes), record = Bytes(proof.record).first(record_bytes);
    const auto entry = Bytes(proof.inventory_entry).first(entry_bytes);
    Zeros(Bytes(proof.log_header).subspan(log_header_bytes)); Zeros(Bytes(proof.record).subspan(record_bytes));
    Zeros(Bytes(proof.inventory_entry).subspan(entry_bytes)); Crc(header); Crc(record);
    const std::array<std::uint8_t, 4U> magic{'C', 'K', 'I', 'X'}; Match(header.first(4U), magic);
    if (Le(header.subspan(4U, 2U)) != 1U || header[6U] != dims || header[7U] != 0U) Bad("copied log header");
    Match(header.subspan(8U, 16U), key.table_epoch); Match(header.subspan(24U, coords), entry.first(coords));
    if (Le(record.first(4U)) != record_bytes || record[4U] != 2U) Bad("retained proof is not an exact deleted record");
    Zeros(record.subspan(5U, 3U)); Zeros(entry.subspan(coords + 8U));
    const std::array<std::int64_t, 3U> coordinate{key.coord.x, key.coord.y, key.coord.z};
    for (unsigned i = 0U; i < dims; ++i)
        if (Le(record.subspan(8U + i * 8U, 8U)) != std::bit_cast<std::uint64_t>(coordinate[i])) Bad("retained coordinate binding");
    const auto committed = Le(entry.subspan(coords, 8U));
    if (committed < log_header_bytes || (committed - log_header_bytes) % record_bytes != 0U ||
        proof.record_offset < log_header_bytes || (proof.record_offset - log_header_bytes) % record_bytes != 0U ||
        proof.record_offset > committed || record_bytes > committed - proof.record_offset) Bad("retained committed record boundary");
    const auto at = 8U + coords;
    CommitOwnerIdentity identity{Le(record.subspan(at, 8U)), Le(record.subspan(at + 8U, 8U)), Le(record.subspan(at + 16U, 8U))};
    Identity(identity); return identity;
}
void WriteRetained(Writer& out, const CommitOwnerRetainedProof& proof) {
    out.UInt(proof.inventory_bytes, 8U); out.UInt(proof.record_offset, 8U); out.Raw(proof.inventory_sha256);
    out.Raw(proof.inventory_header); out.Raw(proof.inventory_entry); out.Raw(proof.log_header); out.Raw(proof.record);
}
CommitOwnerRetainedProof ReadRetained(Reader& in) {
    return {in.UInt(8U), in.UInt(8U), in.Array<32U>(), in.Array<60U>(), in.Array<72U>(), in.Array<52U>(), in.Array<60U>()};
}
void WriteFloor(Writer& out, const CommitOwnerFloorProof& proof) {
    out.UInt(proof.inventory_bytes, 8U); out.Raw(proof.inventory_sha256); out.Raw(proof.inventory_header);
}
CommitOwnerFloorProof ReadFloor(Reader& in) { return {in.UInt(8U), in.Array<32U>(), in.Array<60U>()}; }
void Member(const CommitOwnerMemberValue& member, const CommitOwnerScope& scope, std::uint64_t generation) {
    (void)SerializeCommitOwnerMember(member);
    if (member.scope != scope || member.generation > generation) Bad("nested member scope or generation");
}
CommitOwnerIdentity Producing(const CommitOwnerMemberValue& member) {
    return {member.revision, member.commit_time_ms, member.schema_version};
}
void Coverage(const CommitOwnerCoverageValue& value) {
    Scope(value.scope, value.generation); (void)EncodeCommitOwnerCoverageKey(value.key); Set(value.covered, CommitOwnerTreeKind::kMembers);
    switch (value.state) {
        case CommitOwnerCoverageState::kNeverWritten:
            EmptyIdentity(value.identity);
            if (value.image || value.retained || value.floor || value.producer) Bad("never-written artifact or producer");
            break;
        case CommitOwnerCoverageState::kImage:
            Identity(value.identity);
            if (!value.image || value.retained || value.floor) Bad("image state shape");
            Native(*value.image, CommitOwnerNativeKind::kImage);
            if (value.image->bytes < ChunkImageFixedHeaderBytes(value.key.dims)) Bad("image source extent");
            break;
        case CommitOwnerCoverageState::kRetainedEmpty:
            Identity(value.identity);
            if (value.image || !value.retained || value.floor || Retained(*value.retained, value.key) != value.identity) Bad("retained state binding");
            break;
        case CommitOwnerCoverageState::kForgottenEmpty:
            EmptyIdentity(value.identity);
            if (value.image || !value.retained || !value.floor || value.producer) Bad("forgotten state shape");
            if (Inventory(value.floor->inventory_header, value.floor->inventory_bytes, value.floor->inventory_sha256, value.key).floor <
                Retained(*value.retained, value.key).revision) Bad("forgotten deletion floor");
            break;
        default: Bad("coverage state kind");
    }
    if (value.producer) {
        Member(*value.producer, value.scope, value.generation);
        if (value.producer->kind != CommitGroupMemberKind::kChanged || value.producer->coverage != value.key ||
            Producing(*value.producer) != value.identity) Bad("changed producer binding");
    }
}
void Journal(const CommitOwnerJournalRecord& source, const CommitOwnerScope& scope) {
    (void)SerializeCommitJournalHeader(source.context); Digest(source.sha256);
    std::uint64_t low = 156U, high = kMaxCommitDecisionBytes;
    switch (source.kind) {
        case CommitRecordKind::kDecision: break;
        case CommitRecordKind::kReceipt: low = high = kCommitReceiptBytes; break;
        case CommitRecordKind::kSeal: low = 264U; high = kMaxCommitSealBytes; break;
        default: Bad("journal source record kind");
    }
    if (source.context.directory_id != scope.directory_id || source.offset < kCommitJournalHeaderBytes || source.bytes < low ||
        source.bytes > high || source.bytes > std::numeric_limits<std::uint64_t>::max() - source.offset) Bad("journal source context or extent");
}
void WriteJournal(Writer& out, const CommitOwnerJournalRecord& source) {
    out.UInt(static_cast<std::uint8_t>(source.kind), 1U); out.ZeroBytes(7U); out.Raw(source.context.directory_id);
    out.Raw(source.context.journal_id); out.UInt(source.context.generation, 8U); out.UInt(source.offset, 8U);
    out.UInt(source.bytes, 8U); out.Raw(source.sha256);
}
CommitOwnerJournalRecord ReadJournal(Reader& in) {
    CommitOwnerJournalRecord source; source.kind = static_cast<CommitRecordKind>(in.UInt(1U)); in.ZeroBytes(7U);
    source.context = {in.Array<16U>(), in.Array<16U>(), in.UInt(8U)};
    source.offset = in.UInt(8U); source.bytes = in.UInt(8U); source.sha256 = in.Array<32U>(); return source;
}
void Group(const CommitOwnerGroupValue& value) {
    Scope(value.scope, value.generation); Id(value.group_id); Digest(value.decision_sha256);
    std::uint64_t decision_bytes = 0U;
    if (const auto* journal = std::get_if<CommitOwnerJournalRecord>(&value.source)) {
        Journal(*journal, value.scope);
        if (journal->kind == CommitRecordKind::kReceipt ||
            (journal->kind == CommitRecordKind::kDecision && journal->sha256 != value.decision_sha256)) Bad("decision source binding");
        decision_bytes = journal->kind == CommitRecordKind::kSeal ? journal->bytes - 108U : journal->bytes;
    } else {
        const auto& seal = std::get<CommitOwnerSealSource>(value.source); Digest(seal.sha256);
        if (seal.bytes < 264U || seal.bytes > kMaxCommitSealBytes) Bad("seal source extent");
        decision_bytes = seal.bytes - 108U;
    }
    switch (value.durability) {
        case CommitOwnerDurabilityProof::kUnsettled: case CommitOwnerDurabilityProof::kSyncedDecision:
        case CommitOwnerDurabilityProof::kCompleteCoverage: if (value.receipt) Bad("unexpected receipt source"); break;
        case CommitOwnerDurabilityProof::kReceipt:
            if (!value.receipt || value.receipt->kind != CommitRecordKind::kReceipt) Bad("missing exact receipt source");
            Journal(*value.receipt, value.scope); break;
        default: Bad("durability proof kind");
    }
    Set(value.members, CommitOwnerTreeKind::kMembers); Set(value.owners, CommitOwnerTreeKind::kOwnerEdges);
    Set(value.observations, CommitOwnerTreeKind::kObservations);
    if (value.observations.count > value.members.count) Bad("observation cardinality");
    if (value.members.count > (decision_bytes - 156U) / kCommitGroupMemberBytes ||
        (value.members.count != 0U && decision_bytes - 156U - value.members.count * kCommitGroupMemberBytes < 33U))
        Bad("member count exceeds exact decision source extent");
}
void Observation(const CommitOwnerObservation& value) {
    Scope(value.scope, value.generation); Member(value.member, value.scope, value.generation);
    if (value.member.kind != CommitGroupMemberKind::kObserved || value.member.revision == 0U) Bad("nonzero observed membership required");
    const auto& key = value.member.coverage;
    const auto expected = Producing(value.member);
    if (const auto* image = std::get_if<CommitOwnerNativeRef>(&value.source)) {
        Native(*image, CommitOwnerNativeKind::kImage);
        if (image->bytes < ChunkImageFixedHeaderBytes(key.dims)) Bad("observed image extent");
    } else if (const auto* retained = std::get_if<CommitOwnerRetainedProof>(&value.source)) {
        if (Retained(*retained, key) != expected) Bad("observed retained producing identity");
    } else {
        const auto& wal = std::get<CommitOwnerObservationWal>(value.source);
        Native(wal.prefix, CommitOwnerNativeKind::kWalPrefix);
        if (wal.prefix.bytes < ChunkWalHeaderBytes(key.dims)) Bad("observed WAL prefix extent");
        CommitOwnerIdentity base;
        if (const auto* image = std::get_if<CommitOwnerImageBase>(&wal.base)) {
            Identity(image->identity); Native(image->image, CommitOwnerNativeKind::kImage); base = image->identity;
            if (image->image.bytes < ChunkImageFixedHeaderBytes(key.dims)) Bad("observed base image extent");
        } else if (const auto* retained = std::get_if<CommitOwnerRetainedProof>(&wal.base)) base = Retained(*retained, key);
        if (base.revision > expected.revision || (base.revision == expected.revision && base != expected)) Bad("observed WAL base identity");
    }
}
}  // namespace

namespace {
void Edge(const CommitOwnerEdge& edge) {
    Scope(edge.scope, edge.generation); Id(edge.group_id); Digest(edge.decision_sha256);
    std::visit([&](const auto& source) {
        using Source = std::decay_t<decltype(source)>;
        if constexpr (std::is_same_v<Source, CommitOwnerProducerEdge> || std::is_same_v<Source, CommitOwnerCoveredEdge>) {
            (void)EncodeCommitOwnerCoverageKey(source.coverage); Ref(source.value, CommitOwnerObjectKind::kCoverageValue);
        } else if constexpr (std::is_same_v<Source, CommitOwnerRollbackEdge>) {
            Id(source.source_group); Digest(source.source_decision_sha256); Ref(source.source_value, CommitOwnerObjectKind::kGroupValue);
            (void)EncodeCommitOwnerCoverageKey(source.coverage); Identity(source.anchor);
            if (source.source_group == edge.group_id) Bad("self rollback owner");
        } else if constexpr (std::is_same_v<Source, CommitOwnerFeedEdge>) {
            (void)EncodeCommitOwnerCoverageKey(source.coverage);
            Native(source.slots, CommitOwnerNativeKind::kFeedSlots); Native(source.archive, CommitOwnerNativeKind::kWalPrefix);
            if (source.slot_name_bytes == 0U || source.slot_name_bytes > source.slot_name.size()) Bad("feed slot name extent");
            RequireValidFeedSlotName(std::string_view(source.slot_name.data(), source.slot_name_bytes));
            if (!std::all_of(source.slot_name.begin() + source.slot_name_bytes, source.slot_name.end(), [](char byte) { return byte == 0; }))
                Bad("feed slot name padding");
            Digest(source.frame_sha256);
            if (source.frame_offset < ChunkWalHeaderBytes(source.coverage.dims) || source.frame_bytes == 0U ||
                source.frame_offset > source.archive.bytes || source.frame_bytes > source.archive.bytes - source.frame_offset)
                Bad("feed retained frame extent");
        } else if constexpr (std::is_same_v<Source, CommitOwnerIdEdge>) {
            Ref(source.inventory, CommitOwnerObjectKind::kIdInventory); Digest(source.entry_sha256); Digest(source.key_sha256);
            if (source.generation == 0U || source.entry_offset < kCommitIdInventoryHeaderBytes || source.entry_bytes < 112U ||
                source.entry_offset > source.inventory.bytes - 4U || source.entry_bytes > source.inventory.bytes - 4U - source.entry_offset)
                Bad("ID entry source extent");
        } else {
            if (source.source.kind != CommitOwnerObjectKind::kReplacementPlan && source.source.kind != CommitOwnerObjectKind::kRestoreProvenance)
                Bad("transition source kind");
            Ref(source.source, source.source.kind);
            if (source.source_generation == 0U || source.source_generation >= edge.generation) Bad("transition source must pre-exist edge");
        }
    }, edge.source);
}
void WriteMember(Writer& out, const CommitOwnerMemberValue& member) { out.Raw(SerializeCommitOwnerMember(member)); }
CommitOwnerMemberValue ReadMember(Reader& in, const CommitOwnerScope& scope, std::uint64_t generation) {
    return ParseCommitOwnerMember(in.Take(kCommitOwnerMemberBytes), scope, generation);
}
bool Present(Reader& in) { const auto flag = in.UInt(1U); if (flag > 1U) Bad("optional field tag"); return flag == 1U; }
template <class T, class Write> void Optional(Writer& out, const std::optional<T>& field, Write write) {
    out.UInt(field ? 1U : 0U, 1U); if (field) write(out, *field);
}
void WriteObservationSource(Writer& out, const CommitOwnerObservationSource& source) {
    out.UInt(source.index() + 1U, 1U);
    if (const auto* image = std::get_if<CommitOwnerNativeRef>(&source)) WriteNative(out, *image);
    else if (const auto* retained = std::get_if<CommitOwnerRetainedProof>(&source)) WriteRetained(out, *retained);
    else {
        const auto& wal = std::get<CommitOwnerObservationWal>(source); out.UInt(wal.base.index(), 1U);
        if (const auto* image = std::get_if<CommitOwnerImageBase>(&wal.base)) {
            WriteIdentity(out, image->identity); WriteNative(out, image->image);
        } else if (const auto* retained = std::get_if<CommitOwnerRetainedProof>(&wal.base)) WriteRetained(out, *retained);
        WriteNative(out, wal.prefix);
    }
}
CommitOwnerObservationSource ReadObservationSource(Reader& in) {
    const auto kind = in.UInt(1U);
    if (kind == 1U) return ReadNative(in);
    if (kind == 2U) return ReadRetained(in);
    if (kind != 3U) Bad("observation source tag");
    CommitOwnerObservationWal wal;
    switch (in.UInt(1U)) {
        case 0U: wal.base = CommitOwnerNeverWrittenBase{}; break;
        case 1U: wal.base = CommitOwnerImageBase{ReadIdentity(in), ReadNative(in)}; break;
        case 2U: wal.base = ReadRetained(in); break;
        default: Bad("observation base tag");
    }
    wal.prefix = ReadNative(in); return wal;
}
void WriteEdgeSource(Writer& out, const CommitOwnerEdgeSource& source) {
    out.UInt(source.index() + 1U, 1U); out.ZeroBytes(7U);
    std::visit([&](const auto& field) {
        using Source = std::decay_t<decltype(field)>;
        if constexpr (std::is_same_v<Source, CommitOwnerProducerEdge> || std::is_same_v<Source, CommitOwnerCoveredEdge>) {
            WriteKey(out, field.coverage); WriteRef(out, field.value); out.UInt(field.target_ordinal, 8U);
        } else if constexpr (std::is_same_v<Source, CommitOwnerRollbackEdge>) {
            out.Raw(field.source_group); out.Raw(field.source_decision_sha256); WriteRef(out, field.source_value);
            out.UInt(field.source_ordinal, 8U); out.UInt(field.target_ordinal, 8U); WriteKey(out, field.coverage); WriteIdentity(out, field.anchor);
        } else if constexpr (std::is_same_v<Source, CommitOwnerFeedEdge>) {
            WriteKey(out, field.coverage); WriteNative(out, field.slots); out.UInt(field.slot_name_bytes, 1U);
            const auto bytes = std::as_bytes(std::span(field.slot_name));
            out.Raw({reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size()});
            out.UInt(field.source_ordinal, 8U); out.UInt(field.frame_offset, 8U); out.UInt(field.frame_bytes, 8U);
            WriteNative(out, field.archive); out.Raw(field.frame_sha256);
        } else if constexpr (std::is_same_v<Source, CommitOwnerIdEdge>) {
            WriteRef(out, field.inventory); out.UInt(field.generation, 8U); out.UInt(field.entry_offset, 8U); out.UInt(field.entry_bytes, 8U);
            out.Raw(field.entry_sha256); out.Raw(field.key_sha256);
        } else {
            WriteRef(out, field.source); out.UInt(field.source_generation, 8U); out.UInt(field.participant_ordinal, 8U);
        }
    }, source);
}
CommitOwnerEdgeSource ReadEdgeSource(Reader& in) {
    const auto kind = in.UInt(1U); in.ZeroBytes(7U);
    switch (kind) {
        case 1U: return CommitOwnerProducerEdge{ReadKey(in), ReadRef(in), in.UInt(8U)};
        case 2U: return CommitOwnerCoveredEdge{ReadKey(in), ReadRef(in), in.UInt(8U)};
        case 3U: return CommitOwnerRollbackEdge{in.Array<16U>(), in.Array<32U>(), ReadRef(in),
            in.UInt(8U), in.UInt(8U), ReadKey(in), ReadIdentity(in)};
        case 4U: {
            CommitOwnerFeedEdge source; source.coverage = ReadKey(in); source.slots = ReadNative(in);
            source.slot_name_bytes = static_cast<std::uint8_t>(in.UInt(1U));
            const auto name = in.Take(source.slot_name.size());
            std::copy(name.begin(), name.end(), source.slot_name.begin());
            source.source_ordinal = in.UInt(8U); source.frame_offset = in.UInt(8U); source.frame_bytes = in.UInt(8U);
            source.archive = ReadNative(in); source.frame_sha256 = in.Array<32U>(); return source;
        }
        case 5U: return CommitOwnerIdEdge{ReadRef(in), in.UInt(8U), in.UInt(8U), in.UInt(8U), in.Array<32U>(), in.Array<32U>()};
        case 6U: return CommitOwnerTransitionEdge{ReadRef(in), in.UInt(8U), in.UInt(8U)};
        default: Bad("owner edge source tag");
    }
}
void Counter(const CommitCounterRecord& value) {
    Scope(value.scope, value.generation); Id(value.counter_owner);
    if (value.exclusive_ceiling < value.generation || (value.generation == 1U && value.exclusive_ceiling != 1U)) Bad("counter initial or reserved range");
}
}  // namespace

std::vector<std::uint8_t> SerializeCommitOwnerCoverage(const CommitOwnerCoverageValue& value) {
    Coverage(value); Writer out; out.Header(value.scope, value.generation, CommitOwnerObjectKind::kCoverageValue);
    WriteKey(out, value.key); out.UInt(static_cast<std::uint8_t>(value.state), 1U); out.ZeroBytes(6U); WriteIdentity(out, value.identity);
    Optional(out, value.image, WriteNative); Optional(out, value.retained, WriteRetained); Optional(out, value.floor, WriteFloor);
    WriteSet(out, value.covered, CommitOwnerTreeKind::kMembers); Optional(out, value.producer, WriteMember); return out.Finish();
}
CommitOwnerCoverageValue ParseCommitOwnerCoverage(Bytes bytes, const CommitOwnerScope& scope,
    std::uint64_t selecting, const CommitOwnerCoverageKey& key) {
    CommitOwnerHeader header; auto in = Payload(bytes, scope, selecting, CommitOwnerObjectKind::kCoverageValue, header);
    CommitOwnerCoverageValue value; value.scope = scope; value.generation = header.generation; value.key = ReadKey(in);
    value.state = static_cast<CommitOwnerCoverageState>(in.UInt(1U)); in.ZeroBytes(6U); value.identity = ReadIdentity(in);
    if (Present(in)) value.image = ReadNative(in);
    if (Present(in)) value.retained = ReadRetained(in);
    if (Present(in)) value.floor = ReadFloor(in);
    value.covered = ReadSet(in, CommitOwnerTreeKind::kMembers);
    if (Present(in)) value.producer = ReadMember(in, scope, header.generation);
    in.End(); Coverage(value); if (value.key != key) Bad("coverage key binding"); return value;
}
std::vector<std::uint8_t> SerializeCommitOwnerGroup(const CommitOwnerGroupValue& value) {
    Group(value); Writer out; out.Header(value.scope, value.generation, CommitOwnerObjectKind::kGroupValue);
    out.Raw(value.group_id); out.Raw(value.decision_sha256); out.UInt(value.source.index() + 1U, 1U);
    if (const auto* journal = std::get_if<CommitOwnerJournalRecord>(&value.source)) WriteJournal(out, *journal);
    else { const auto& seal = std::get<CommitOwnerSealSource>(value.source); out.UInt(seal.bytes, 8U); out.Raw(seal.sha256); }
    out.UInt(static_cast<std::uint8_t>(value.durability), 1U); out.ZeroBytes(6U); Optional(out, value.receipt, WriteJournal);
    WriteSet(out, value.members, CommitOwnerTreeKind::kMembers); WriteSet(out, value.owners, CommitOwnerTreeKind::kOwnerEdges);
    WriteSet(out, value.observations, CommitOwnerTreeKind::kObservations); return out.Finish();
}
CommitOwnerGroupValue ParseCommitOwnerGroup(Bytes bytes, const CommitOwnerScope& scope,
    std::uint64_t selecting, const CommitGroupId& key) {
    CommitOwnerHeader header; auto in = Payload(bytes, scope, selecting, CommitOwnerObjectKind::kGroupValue, header);
    CommitOwnerGroupValue value; value.scope = scope; value.generation = header.generation; value.group_id = in.Array<16U>(); value.decision_sha256 = in.Array<32U>();
    switch (in.UInt(1U)) {
        case 1U: value.source = ReadJournal(in); break;
        case 2U: value.source = CommitOwnerSealSource{in.UInt(8U), in.Array<32U>()}; break;
        default: Bad("decision source tag");
    }
    value.durability = static_cast<CommitOwnerDurabilityProof>(in.UInt(1U)); in.ZeroBytes(6U);
    if (Present(in)) value.receipt = ReadJournal(in);
    value.members = ReadSet(in, CommitOwnerTreeKind::kMembers); value.owners = ReadSet(in, CommitOwnerTreeKind::kOwnerEdges);
    value.observations = ReadSet(in, CommitOwnerTreeKind::kObservations); in.End(); Group(value);
    if (value.group_id != key) Bad("group key binding");
    return value;
}
std::vector<std::uint8_t> SerializeCommitOwnerObservation(const CommitOwnerObservation& value) {
    Observation(value); Writer out; out.Header(value.scope, value.generation, CommitOwnerObjectKind::kObservation);
    WriteMember(out, value.member); WriteObservationSource(out, value.source); return out.Finish();
}
CommitOwnerObservation ParseCommitOwnerObservation(Bytes bytes, const CommitOwnerScope& scope,
    std::uint64_t selecting, const CommitOwnerMemberKey& key) {
    CommitOwnerHeader header; auto in = Payload(bytes, scope, selecting, CommitOwnerObjectKind::kObservation, header);
    CommitOwnerObservation value; value.scope = scope; value.generation = header.generation;
    value.member = ReadMember(in, scope, header.generation); value.source = ReadObservationSource(in); in.End(); Observation(value);
    if (value.member.group_id != key.group_id || value.member.ordinal != key.ordinal) Bad("observation key binding");
    return value;
}
std::vector<std::uint8_t> SerializeCommitOwnerEdge(const CommitOwnerEdge& value) {
    Edge(value); Writer out; out.Header(value.scope, value.generation, CommitOwnerObjectKind::kOwnerEdge);
    out.Raw(value.group_id); out.Raw(value.decision_sha256); WriteEdgeSource(out, value.source); return out.Finish();
}
CommitOwnerEdge ParseCommitOwnerEdge(Bytes bytes, const CommitOwnerScope& scope, std::uint64_t selecting,
    std::span<const std::uint8_t, kCommitOwnerEdgeKeyBytes> key) {
    ValidateCommitOwnerEdgeKey(key);
    CommitOwnerHeader header; auto in = Payload(bytes, scope, selecting, CommitOwnerObjectKind::kOwnerEdge, header);
    CommitOwnerEdge value; value.scope = scope; value.generation = header.generation;
    value.group_id = in.Array<16U>(); value.decision_sha256 = in.Array<32U>(); value.source = ReadEdgeSource(in); in.End(); Edge(value);
    const auto actual = EncodeCommitOwnerEdgeKey(value.source);
    if (!std::equal(actual.begin(), actual.end(), key.begin())) Bad("edge key/source binding");
    return value;
}

std::array<std::uint8_t, kCommitOwnerEdgeKeyBytes> EncodeCommitOwnerEdgeKey(const CommitOwnerEdgeSource& source) {
    std::array<std::uint8_t, kCommitOwnerEdgeKeyBytes> key{}; key[0U] = static_cast<std::uint8_t>(source.index() + 1U);
    const auto ordinal = [&](std::size_t at, std::uint64_t value) {
        for (unsigned i = 0U; i < 8U; ++i) key[at + i] = static_cast<std::uint8_t>(value >> (56U - 8U * i));
    };
    std::visit([&](const auto& field) {
        using Source = std::decay_t<decltype(field)>;
        if constexpr (std::is_same_v<Source, CommitOwnerProducerEdge> || std::is_same_v<Source, CommitOwnerCoveredEdge>) {
            const auto encoded = EncodeCommitOwnerCoverageKey(field.coverage); std::copy(encoded.begin(), encoded.end(), key.begin() + 1);
        } else if constexpr (std::is_same_v<Source, CommitOwnerRollbackEdge>) {
            std::copy(field.source_group.begin(), field.source_group.end(), key.begin() + 1); ordinal(17U, field.source_ordinal);
        } else if constexpr (std::is_same_v<Source, CommitOwnerFeedEdge>) {
            std::copy(field.slots.sha256.begin(), field.slots.sha256.end(), key.begin() + 1); ordinal(33U, field.source_ordinal);
        } else if constexpr (std::is_same_v<Source, CommitOwnerIdEdge>) {
            std::copy(field.key_sha256.begin(), field.key_sha256.end(), key.begin() + 1);
        } else {
            std::copy(field.source.sha256.begin(), field.source.sha256.end(), key.begin() + 1); ordinal(33U, field.participant_ordinal);
        }
    }, source);
    ValidateCommitOwnerEdgeKey(key); return key;
}
void ValidateCommitOwnerEdgeKey(Bytes key) {
    if (key.size() != kCommitOwnerEdgeKeyBytes) Bad("edge key width");
    switch (static_cast<CommitOwnerEdgeClass>(key[0U])) {
        case CommitOwnerEdgeClass::kProducer: case CommitOwnerEdgeClass::kCovered:
            (void)DecodeCommitOwnerCoverageKey(key.subspan(1U, 41U)); Zeros(key.subspan(42U)); break;
        case CommitOwnerEdgeClass::kRollback:
            if (Zero(key.subspan(1U, 16U))) Bad("rollback source key identity");
            Zeros(key.subspan(25U)); break;
        case CommitOwnerEdgeClass::kFeed: case CommitOwnerEdgeClass::kTransition:
            if (Zero(key.subspan(1U, 32U))) Bad("source key digest");
            Zeros(key.subspan(41U)); break;
        case CommitOwnerEdgeClass::kId:
            if (Zero(key.subspan(1U, 32U))) Bad("ID key digest");
            Zeros(key.subspan(33U)); break;
        default: Bad("owner edge key class");
    }
}

std::array<std::uint8_t, kCommitCounterBytes> SerializeCommitCounter(const CommitCounterRecord& value) {
    Counter(value); std::array<std::uint8_t, kCommitCounterBytes> bytes{};
    const std::array<std::uint8_t, 4U> magic{'C', 'K', 'T', 'C'}; std::copy(magic.begin(), magic.end(), bytes.begin());
    Put(std::span(bytes).subspan(4U, 2U), 1U); bytes[6U] = static_cast<std::uint8_t>(value.scope.role);
    Put(std::span(bytes).subspan(8U, 8U), bytes.size());
    std::copy(value.scope.directory_id.begin(), value.scope.directory_id.end(), bytes.begin() + 16);
    std::copy(value.scope.owner_id.begin(), value.scope.owner_id.end(), bytes.begin() + 32);
    std::copy(value.counter_owner.begin(), value.counter_owner.end(), bytes.begin() + 48);
    Put(std::span(bytes).subspan(64U, 8U), value.generation); Put(std::span(bytes).subspan(72U, 8U), value.exclusive_ceiling);
    Put(std::span(bytes).last(4U), Crc32(bytes.data(), bytes.size() - 4U)); return bytes;
}
CommitCounterRecord ParseCommitCounter(Bytes bytes, const CommitOwnerScope& scope) {
    if (bytes.size() != kCommitCounterBytes) Bad("counter complete extent");
    Crc(bytes); const std::array<std::uint8_t, 4U> magic{'C', 'K', 'T', 'C'}; Match(bytes.first(4U), magic);
    if (Le(bytes.subspan(4U, 2U)) != 1U || bytes[7U] != 0U || Le(bytes.subspan(8U, 8U)) != kCommitCounterBytes) Bad("counter envelope");
    Zeros(bytes.subspan(80U, 12U)); Reader in(bytes.subspan(16U, 64U));
    CommitCounterRecord value; value.scope = {in.Array<16U>(), in.Array<16U>(), static_cast<CommitOwnerRole>(bytes[6U])};
    value.counter_owner = in.Array<16U>(); value.generation = in.UInt(8U); value.exclusive_ceiling = in.UInt(8U); in.End(); Counter(value);
    if (value.scope != scope) Bad("counter scope binding");
    return value;
}
void ValidateCommitCounterSuccessor(const CommitCounterRecord& before, const CommitCounterRecord& after) {
    Counter(before); Counter(after);
    if (before.scope != after.scope || before.counter_owner != after.counter_owner || before.generation == std::numeric_limits<std::uint64_t>::max() ||
        after.generation != before.generation + 1U || after.exclusive_ceiling <= before.exclusive_ceiling) Bad("counter successor or exhaustion");
}
void ValidateCommitCounterBinding(const CommitOwnerRoot& root, const CommitCounterRecord& counter) {
    Counter(counter);
    if (root.scope != counter.scope) Bad("selected counter scope");
    ValidateCommitOwnerCounter(root, counter.counter_owner, counter.exclusive_ceiling);
}

CommitOwnerSetHasher::CommitOwnerSetHasher(CommitOwnerTreeKind tree) : tree_(tree) {
    const std::array<std::uint8_t, 12U> domain{'C', 'K', 'G', 'O', '-', 'S', 'E', 'T', 0U, 1U,
        static_cast<std::uint8_t>(tree), static_cast<std::uint8_t>(Width(tree))}; digest_.Update(domain);
}
void CommitOwnerSetHasher::Add(Bytes key, const CommitOwnerObjectRef& value) {
    if (finished_ || key.size() != Width(tree_) || count_ == std::numeric_limits<std::uint64_t>::max()) Bad("set digest state or key extent");
    switch (tree_) {
        case CommitOwnerTreeKind::kCoverage: (void)DecodeCommitOwnerCoverageKey(key); break;
        case CommitOwnerTreeKind::kGroups: (void)DecodeCommitOwnerGroupKey(key); break;
        case CommitOwnerTreeKind::kMembers: case CommitOwnerTreeKind::kObservations: (void)DecodeCommitOwnerMemberKey(key); break;
        case CommitOwnerTreeKind::kOwnerEdges: ValidateCommitOwnerEdgeKey(key); break;
    }
    Ref(value, ValueKind(tree_));
    if (count_ != 0U && !std::lexicographical_compare(previous_.begin(), previous_.begin() + static_cast<std::ptrdiff_t>(key.size()), key.begin(), key.end()))
        Bad("duplicate or unordered set key/source");
    std::array<std::uint8_t, kCommitOwnerReferenceBytes> encoded{}; encoded[0U] = static_cast<std::uint8_t>(value.kind);
    Put(std::span(encoded).subspan(8U, 8U), value.bytes); std::copy(value.sha256.begin(), value.sha256.end(), encoded.begin() + 16);
    digest_.Update(key); digest_.Update(encoded); std::copy(key.begin(), key.end(), previous_.begin()); ++count_;
}
crypto::Sha256Digest CommitOwnerSetHasher::Finish() {
    if (finished_) Bad("set digest already finished");
    std::array<std::uint8_t, 8U> count{}; Put(count, count_); digest_.Update(count); finished_ = true; return digest_.Finish();
}
CommitOwnerIdKeyHasher::CommitOwnerIdKeyHasher(std::uint64_t principal_bytes, std::uint64_t id_bytes)
    : principal_remaining_(principal_bytes), id_bytes_(id_bytes), id_remaining_(id_bytes) {
    if (id_bytes > 64U || principal_bytes > kMaxCommitIdInventoryBytes - 112U - id_bytes) Bad("ID key length");
    const std::array<std::uint8_t, 10U> domain{'C', 'K', 'G', 'O', '-', 'I', 'D', 0U, 1U, 0U};
    digest_.Update(domain); std::array<std::uint8_t, 8U> length{}; Put(length, principal_bytes); digest_.Update(length);
}
void CommitOwnerIdKeyHasher::Principal(Bytes bytes) {
    if (finished_ || id_started_ || bytes.size() > principal_remaining_) Bad("ID principal stream extent");
    digest_.Update(bytes); principal_remaining_ -= bytes.size();
}
void CommitOwnerIdKeyHasher::Id(Bytes bytes) {
    if (finished_ || principal_remaining_ != 0U || bytes.size() > id_remaining_) Bad("ID byte stream extent");
    if (!id_started_) {
        std::array<std::uint8_t, 8U> length{}; Put(length, id_bytes_); digest_.Update(length); id_started_ = true;
    }
    digest_.Update(bytes); id_remaining_ -= bytes.size();
}
crypto::Sha256Digest CommitOwnerIdKeyHasher::Finish() {
    if (finished_ || principal_remaining_ != 0U || id_remaining_ != 0U) Bad("incomplete ID key stream");
    if (!id_started_) Id({});
    finished_ = true; return digest_.Finish();
}
void ValidateCommitOwnerGroupBinding(const CommitOwnerGroupValue& value, const CommitOwnerDecisionFacts& decision) {
    Group(value);
    if (decision.directory_id != value.scope.directory_id || decision.group_id != value.group_id || decision.sha256 != value.decision_sha256 ||
        decision.member_count != value.members.count || decision.nonzero_observed_count > decision.member_count ||
        decision.nonzero_observed_count != value.observations.count) Bad("complete decision/member/observation binding");
    if ((decision.durability != CommitGroupDurability::kRelaxed && decision.durability != CommitGroupDurability::kSynced) ||
        (value.durability == CommitOwnerDurabilityProof::kSyncedDecision && decision.durability != CommitGroupDurability::kSynced) ||
        (value.durability == CommitOwnerDurabilityProof::kUnsettled && decision.durability != CommitGroupDurability::kRelaxed)) Bad("original durability binding");
}
void ValidateCommitOwnerObservationBinding(const CommitOwnerObservation& observation, const CommitOwnerMemberValue& member) {
    Observation(observation); Member(member, observation.scope, observation.generation);
    auto actual = observation.member, expected = member; actual.generation = expected.generation;
    if (actual != expected) Bad("original observation membership binding");
}
void ValidateCommitOwnerEdgeBinding(const CommitOwnerEdge& edge, const CommitOwnerGroupValue& target) {
    Edge(edge); Group(target);
    if (edge.scope != target.scope || edge.generation > target.generation || edge.group_id != target.group_id || edge.decision_sha256 != target.decision_sha256)
        Bad("exact owner edge target incarnation");
    std::visit([&](const auto& source) {
        using Source = std::decay_t<decltype(source)>;
        if constexpr (std::is_same_v<Source, CommitOwnerProducerEdge> || std::is_same_v<Source, CommitOwnerCoveredEdge> ||
            std::is_same_v<Source, CommitOwnerRollbackEdge>)
            if (source.target_ordinal >= target.members.count) Bad("target owner ordinal");
    }, edge.source);
}
void ValidateCommitOwnerProducerSource(const CommitOwnerEdge& edge, const CommitOwnerCoverageValue& coverage) {
    Edge(edge); Coverage(coverage);
    const auto* source = std::get_if<CommitOwnerProducerEdge>(&edge.source);
    const auto raw = SerializeCommitOwnerCoverage(coverage);
    if (!source || edge.scope != coverage.scope || coverage.generation > edge.generation || source->coverage != coverage.key ||
        source->value != CommitOwnerObjectRef{CommitOwnerObjectKind::kCoverageValue, raw.size(), crypto::Sha256Of(raw)} ||
        !coverage.producer || coverage.producer->group_id != edge.group_id || coverage.producer->decision_sha256 != edge.decision_sha256 ||
        coverage.producer->ordinal != source->target_ordinal) Bad("producer source incarnation");
}
void ValidateCommitOwnerCoveredSource(const CommitOwnerEdge& edge, const CommitOwnerCoverageValue& coverage,
    const CommitOwnerMemberValue& member) {
    Edge(edge); Coverage(coverage); Member(member, edge.scope, edge.generation);
    const auto* source = std::get_if<CommitOwnerCoveredEdge>(&edge.source);
    const auto raw = SerializeCommitOwnerCoverage(coverage);
    if (!source || edge.scope != coverage.scope || coverage.generation > edge.generation || source->coverage != coverage.key ||
        source->value != CommitOwnerObjectRef{CommitOwnerObjectKind::kCoverageValue, raw.size(), crypto::Sha256Of(raw)} ||
        member.group_id != edge.group_id || member.decision_sha256 != edge.decision_sha256 || member.ordinal != source->target_ordinal ||
        member.coverage != coverage.key || (coverage.state == CommitOwnerCoverageState::kNeverWritten && member.revision != 0U))
        Bad("covered source incarnation");
    // Membership in coverage.covered still requires the complete native path.
}
void ValidateCommitOwnerIdSource(const CommitOwnerEdge& edge, const CommitIdEntry& entry,
    const CommitOwnerObjectRef& selected_inventory, std::uint64_t inventory_generation,
    const crypto::Sha256Digest& measured_entry_sha256) {
    Edge(edge); const auto* source = std::get_if<CommitOwnerIdEdge>(&edge.source);
    CommitOwnerIdKeyHasher key(entry.user.size(), entry.id.size());
    key.Principal({reinterpret_cast<const std::uint8_t*>(entry.user.data()), entry.user.size()}); key.Id(entry.id);
    if (!source || source->inventory != selected_inventory || source->generation != inventory_generation ||
        source->entry_sha256 != measured_entry_sha256 || source->key_sha256 != key.Finish() ||
        entry.group_id != edge.group_id || entry.decision_sha256 != edge.decision_sha256 || source->entry_bytes != 112U + entry.user.size() + entry.id.size())
        Bad("ID source incarnation or key collision");
}
void ValidateCommitOwnerSetRoot(const CommitOwnerSet& set, CommitOwnerTreeKind tree, const CommitOwnerScope& scope,
    const CommitOwnerPage& page, const CommitOwnerObjectRef& measured, std::uint64_t selecting) {
    Set(set, tree); const auto raw = SerializeCommitOwnerPage(page);
    if (selecting == 0U || page.scope != scope || page.tree != tree || page.generation > selecting || measured != set.root ||
        measured != CommitOwnerObjectRef{CommitOwnerObjectKind::kPage, raw.size(), crypto::Sha256Of(raw)} ||
        page.subtree_keys != set.count || page.subtree_bytes != set.owned_bytes) Bad("set root context or measured totals");
}

}  // namespace chunkdb
