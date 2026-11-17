#include "commit_group_format.hpp"

#include <algorithm>
#include <bit>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "chunkdb/crc32.hpp"

namespace chunkdb {
namespace {
using Bytes = std::span<const std::uint8_t>;
constexpr std::size_t kDecisionPrefixBytes = 88U;
constexpr std::size_t kResultPrefixBytes = 32U;
constexpr std::size_t kIdEntryPrefixBytes = 112U;

[[noreturn]] void Bad(const char* reason) { throw std::invalid_argument(std::string("commit group format: ") + reason); }
template <std::size_t N> bool Zero(const std::array<std::uint8_t, N>& value) {
    return std::all_of(value.begin(), value.end(), [](auto byte) { return byte == 0U; });
}
Bytes Raw(std::string_view value) {
    return {reinterpret_cast<const std::uint8_t*>(value.data()), value.size()};
}
std::size_t Add(std::size_t before, std::uint64_t amount, std::size_t limit) {
    if (before > limit || amount > limit - before) Bad("byte extent exceeds format bound");
    return before + static_cast<std::size_t>(amount);
}
std::size_t Repeated(std::uint64_t count, std::size_t width, std::size_t available) {
    if (count > available / width) Bad("count exceeds checked extent");
    return static_cast<std::size_t>(count) * width;
}
void Length32(std::size_t size) {
    if (size > std::numeric_limits<std::uint32_t>::max()) Bad("string extent exceeds encoding");
}

class Reader {
  public:
    explicit Reader(Bytes bytes) : bytes_(bytes) {}
    [[nodiscard]] Bytes Take(std::size_t count) {
        if (count > bytes_.size()) Bad("truncated field");
        const auto value = bytes_.first(count); bytes_ = bytes_.subspan(count); return value;
    }
    [[nodiscard]] std::uint64_t UInt(unsigned width) {
        const auto bytes = Take(width); std::uint64_t value = 0U;
        for (unsigned i = 0U; i < width; ++i) value |= static_cast<std::uint64_t>(bytes[i]) << (8U * i);
        return value;
    }
    template <std::size_t N> [[nodiscard]] std::array<std::uint8_t, N> Array() {
        std::array<std::uint8_t, N> value{};
        const auto bytes = Take(N); std::copy(bytes.begin(), bytes.end(), value.begin()); return value;
    }
    void Zeros(std::size_t count) {
        const auto bytes = Take(count);
        if (std::any_of(bytes.begin(), bytes.end(), [](auto byte) { return byte != 0U; })) Bad("reserved bytes are nonzero");
    }
    [[nodiscard]] std::size_t remaining() const { return bytes_.size(); }
  private:
    Bytes bytes_;
};

class Writer {
  public:
    explicit Writer(std::span<std::uint8_t> bytes) : bytes_(bytes) {}
    void UInt(std::uint64_t value, unsigned width) {
        if (width > bytes_.size()) Bad("internal writer extent");
        for (unsigned i = 0U; i < width; ++i) bytes_[i] = static_cast<std::uint8_t>(value >> (8U * i));
        bytes_ = bytes_.subspan(width);
    }
    void Put(Bytes value) {
        if (value.size() > bytes_.size()) Bad("internal writer extent");
        std::copy(value.begin(), value.end(), bytes_.begin()); bytes_ = bytes_.subspan(value.size());
    }
    void Zeros(std::size_t count) {
        if (count > bytes_.size()) Bad("internal writer extent");
        std::fill_n(bytes_.begin(), count, 0U); bytes_ = bytes_.subspan(count);
    }
  private:
    std::span<std::uint8_t> bytes_;
};

void PutCrc(std::span<std::uint8_t> bytes, std::size_t at) {
    Writer(bytes.subspan(at, 4U)).UInt(Crc32(bytes.data(), at), 4U);
}
void CheckCrc(Bytes bytes, std::size_t at) {
    Reader crc(bytes.subspan(at, 4U));
    if (crc.UInt(4U) != Crc32(bytes.data(), at)) Bad("checksum mismatch");
}
void Magic(Reader& reader, std::string_view magic) {
    const auto bytes = reader.Take(magic.size());
    if (!std::equal(bytes.begin(), bytes.end(), magic.begin())) Bad("magic mismatch");
}
void Tag(Reader& reader) { if (reader.UInt(2U) != 1U) Bad("unsupported encoding tag"); }
void Identity(const CommitGroupId& identity) { if (Zero(identity)) Bad("identity is zero"); }
bool RecordKind(CommitRecordKind kind) {
    return kind == CommitRecordKind::kDecision || kind == CommitRecordKind::kReceipt || kind == CommitRecordKind::kSeal;
}
std::size_t RecordLimit(CommitRecordKind kind) {
    switch (kind) {
        case CommitRecordKind::kDecision: return kMaxCommitDecisionBytes;
        case CommitRecordKind::kReceipt: return kCommitReceiptBytes;
        case CommitRecordKind::kSeal: return kMaxCommitSealBytes;
        default: Bad("unknown record kind");
    }
}
std::size_t RecordMinimum(CommitRecordKind kind) {
    switch (kind) {
        case CommitRecordKind::kDecision: return 68U + kDecisionPrefixBytes;
        case CommitRecordKind::kReceipt: return kCommitReceiptBytes;
        case CommitRecordKind::kSeal: return 108U + 68U + kDecisionPrefixBytes;
        default: Bad("unknown record kind");
    }
}
void EnvelopeHeader(std::span<std::uint8_t> bytes, CommitRecordKind kind,
    const CommitGroupId& directory, const CommitGroupId& id) {
    Identity(directory); Identity(id);
    if (bytes.size() < RecordMinimum(kind) || bytes.size() > RecordLimit(kind) ||
        (kind == CommitRecordKind::kReceipt && bytes.size() != kCommitReceiptBytes)) Bad("record byte extent");
    Writer writer(bytes.first(kCommitRecordHeaderBytes));
    writer.Put(Raw("CKGR")); writer.UInt(1U, 2U); writer.UInt(static_cast<std::uint8_t>(kind), 1U); writer.Zeros(1U);
    writer.UInt(bytes.size(), 8U); writer.Put(directory); writer.Put(id); writer.UInt(bytes.size() - 68U, 8U); writer.Zeros(8U);
    PutCrc(bytes.first(kCommitRecordHeaderBytes), 60U);
}
Bytes Payload(Bytes bytes, const CommitGroupId& directory, CommitRecordKind expected, CommitRecordHeader* header) {
    if (bytes.size() < 68U) Bad("record is truncated");
    *header = InspectCommitGroupRecordHeader(bytes.first(kCommitRecordHeaderBytes), directory);
    if (header->kind != expected || header->record_bytes != bytes.size()) Bad("record kind or complete extent mismatch");
    CheckCrc(bytes, bytes.size() - 4U);
    return bytes.subspan(kCommitRecordHeaderBytes, bytes.size() - 68U);
}
bool MemberBefore(const CommitGroupMember& before, const CommitGroupMember& after) {
    return before.table_epoch < after.table_epoch || (before.table_epoch == after.table_epoch && before.coord < after.coord);
}
void RequireMember(const CommitGroupMember& member) {
    Identity(member.table_epoch);
    if ((member.dims != 2U && member.dims != 3U) || (member.dims == 2U && member.coord.z != 0)) Bad("member dimensions");
    if (member.kind == CommitGroupMemberKind::kObserved) {
        if (member.wal_before_bytes != 0U || member.frame_offset != 0U || member.frame_bytes != 0U || !Zero(member.frame_sha256) ||
            member.rollback_base != CommitRollbackBase{} || member.certified_prefix != CommitWalPrefixCertificate{})
            Bad("observed member carries mutation extents");
        if (member.revision == 0U ? member.commit_time_ms != 0U || member.schema_version != 0U : member.schema_version == 0U)
            Bad("observed producing identity");
        return;
    }
    if (member.kind != CommitGroupMemberKind::kChanged || member.revision == 0U || member.schema_version == 0U ||
        member.frame_bytes == 0U || member.frame_offset > std::numeric_limits<std::uint64_t>::max() - member.frame_bytes)
        Bad("changed member identity or frame extent");
    const auto header_bytes = 48U + 8U * member.dims;
    if (member.wal_before_bytes == 0U ? member.frame_offset != header_bytes :
        member.wal_before_bytes < header_bytes || member.frame_offset != member.wal_before_bytes) Bad("pre-group WAL extent");
    const auto& base = member.rollback_base;
    if (base.kind == CommitRollbackBaseKind::kNeverWritten) {
        if (base != CommitRollbackBase{}) Bad("never-written rollback base has state");
    } else {
        if ((base.kind != CommitRollbackBaseKind::kImage && base.kind != CommitRollbackBaseKind::kRetainedEmpty) ||
            base.revision == 0U || base.revision >= member.revision || base.schema_version == 0U) Bad("rollback base identity");
        if (base.kind == CommitRollbackBaseKind::kImage) {
            if (base.image_bytes == 0U) Bad("rollback image extent is zero");
        } else if (base.image_bytes != 0U || !Zero(base.image_sha256)) Bad("retained-empty base has image fields");
    }
    const auto& prefix = member.certified_prefix;
    if (prefix.bytes == 0U ? !Zero(prefix.sha256) : prefix.bytes < header_bytes || prefix.bytes > member.wal_before_bytes)
        Bad("certified pre-group prefix extent");
}
void WriteMember(Writer& writer, const CommitGroupMember& member, std::uint64_t ordinal) {
    writer.Put(member.table_epoch); writer.UInt(member.dims, 1U); writer.UInt(static_cast<std::uint8_t>(member.kind), 1U); writer.Zeros(6U);
    for (const auto coordinate : {member.coord.x, member.coord.y, member.coord.z}) writer.UInt(std::bit_cast<std::uint64_t>(coordinate), 8U);
    for (const auto value : {member.revision, member.commit_time_ms, member.schema_version, member.wal_before_bytes,
             member.frame_offset, member.frame_bytes}) writer.UInt(value, 8U);
    writer.Put(member.frame_sha256); writer.UInt(ordinal, 8U);
    const auto& base = member.rollback_base;
    writer.UInt(static_cast<std::uint8_t>(base.kind), 1U); writer.Zeros(7U);
    for (const auto value : {base.revision, base.commit_time_ms, base.schema_version, base.image_bytes}) writer.UInt(value, 8U);
    writer.Put(base.image_sha256); writer.UInt(member.certified_prefix.bytes, 8U); writer.Put(member.certified_prefix.sha256);
}
CommitGroupMember ReadMember(Reader& reader, std::uint64_t ordinal) {
    CommitGroupMember member;
    member.table_epoch = reader.Array<16U>(); member.dims = static_cast<std::uint8_t>(reader.UInt(1U));
    member.kind = static_cast<CommitGroupMemberKind>(reader.UInt(1U)); reader.Zeros(6U);
    member.coord = {std::bit_cast<std::int64_t>(reader.UInt(8U)), std::bit_cast<std::int64_t>(reader.UInt(8U)),
        std::bit_cast<std::int64_t>(reader.UInt(8U))};
    member.revision = reader.UInt(8U); member.commit_time_ms = reader.UInt(8U); member.schema_version = reader.UInt(8U);
    member.wal_before_bytes = reader.UInt(8U); member.frame_offset = reader.UInt(8U); member.frame_bytes = reader.UInt(8U);
    member.frame_sha256 = reader.Array<32U>(); if (reader.UInt(8U) != ordinal) Bad("member ordinal mismatch");
    member.rollback_base.kind = static_cast<CommitRollbackBaseKind>(reader.UInt(1U)); reader.Zeros(7U);
    member.rollback_base.revision = reader.UInt(8U); member.rollback_base.commit_time_ms = reader.UInt(8U);
    member.rollback_base.schema_version = reader.UInt(8U); member.rollback_base.image_bytes = reader.UInt(8U);
    member.rollback_base.image_sha256 = reader.Array<32U>();
    member.certified_prefix.bytes = reader.UInt(8U); member.certified_prefix.sha256 = reader.Array<32U>();
    RequireMember(member); return member;
}
void RequireTransaction(std::uint64_t txn, bool request, std::uint64_t result_count) {
    if ((request || result_count > 1U) ? txn == 0U : txn != 0U) Bad("visible transaction allocation does not match group");
}
std::size_t DecisionSize(const CommitGroupDecision& decision) {
    Identity(decision.directory_id); Identity(decision.id);
    if (decision.durability != CommitGroupDurability::kRelaxed && decision.durability != CommitGroupDurability::kSynced)
        Bad("unknown durability mode");
    RequireTransaction(decision.txn, decision.request.has_value(), decision.versions.size());
    auto size = 68U + kDecisionPrefixBytes;
    if (decision.request) {
        Length32(decision.request->user.size());
        if (decision.request->id.size() > 64U) Bad("ID exceeds format bound");
        size = Add(size, decision.request->user.size(), kMaxCommitDecisionBytes);
        size = Add(size, decision.request->id.size(), kMaxCommitDecisionBytes);
    }
    if (decision.tag) {
        if (decision.tag->size() > 256U) Bad("tag exceeds format bound");
        size = Add(size, decision.tag->size(), kMaxCommitDecisionBytes);
    }
    size = Add(size, Repeated(decision.members.size(), kCommitGroupMemberBytes, kMaxCommitDecisionBytes - size), kMaxCommitDecisionBytes);
    std::size_t member_at = 0U;
    for (std::size_t i = 0U; i < decision.members.size(); ++i) {
        RequireMember(decision.members[i]);
        if (i != 0U && (!MemberBefore(decision.members[i - 1U], decision.members[i]) ||
            (decision.members[i - 1U].table_epoch == decision.members[i].table_epoch && decision.members[i - 1U].dims != decision.members[i].dims)))
            Bad("member ordering or dimensions mismatch");
    }
    for (std::size_t i = 0U; i < decision.versions.size(); ++i) {
        const auto& result = decision.versions[i]; Identity(result.table_epoch); Length32(result.table_name.size());
        if (result.table_name.empty() || (i != 0U && !(decision.versions[i - 1U].table_epoch < result.table_epoch))) Bad("result labels or ordering");
        while (member_at < decision.members.size() && decision.members[member_at].table_epoch <= result.table_epoch) {
            const auto& member = decision.members[member_at++];
            if (member.table_epoch != result.table_epoch || member.revision > result.revision) Bad("member result binding");
        }
        size = Add(Add(size, kResultPrefixBytes, kMaxCommitDecisionBytes), result.table_name.size(), kMaxCommitDecisionBytes);
    }
    if (member_at != decision.members.size()) Bad("missing member result");
    return size;
}

struct DecisionView {
    CommitRecordHeader header;
    CommitGroupDurability durability;
    std::uint64_t txn, created_ms, member_count, result_count;
    bool request, tag_present;
    crypto::Sha256Digest fingerprint;
    Bytes user, id, tag, members, results;
};
DecisionView InspectDecision(Bytes bytes, const CommitGroupId& directory) {
    DecisionView view{};
    Reader reader(Payload(bytes, directory, CommitRecordKind::kDecision, &view.header));
    view.durability = static_cast<CommitGroupDurability>(reader.UInt(1U)); const auto flags = reader.UInt(1U); reader.Zeros(6U);
    if ((view.durability != CommitGroupDurability::kRelaxed && view.durability != CommitGroupDurability::kSynced) || flags > 3U)
        Bad("decision mode or presence flags");
    view.request = (flags & 1U) != 0U; view.tag_present = (flags & 2U) != 0U;
    view.txn = reader.UInt(8U); view.created_ms = reader.UInt(8U); view.member_count = reader.UInt(8U); view.result_count = reader.UInt(8U);
    const auto user_size = reader.UInt(4U), id_size = reader.UInt(4U), tag_size = reader.UInt(4U); reader.Zeros(4U);
    view.fingerprint = reader.Array<32U>();
    if ((!view.request && (user_size != 0U || id_size != 0U || !Zero(view.fingerprint))) || id_size > 64U ||
        (!view.tag_present && tag_size != 0U) || tag_size > 256U) Bad("request or tag presence mismatch");
    RequireTransaction(view.txn, view.request, view.result_count);
    view.user = reader.Take(static_cast<std::size_t>(user_size)); view.id = reader.Take(static_cast<std::size_t>(id_size));
    view.tag = reader.Take(static_cast<std::size_t>(tag_size));
    view.members = reader.Take(Repeated(view.member_count, kCommitGroupMemberBytes, reader.remaining()));
    (void)Repeated(view.result_count, kResultPrefixBytes, reader.remaining()); view.results = reader.Take(reader.remaining());
    Reader members(view.members); CommitGroupMember previous;
    for (std::uint64_t i = 0U; i < view.member_count; ++i) {
        auto member = ReadMember(members, i);
        if (i != 0U && (!MemberBefore(previous, member) || (previous.table_epoch == member.table_epoch && previous.dims != member.dims)))
            Bad("member ordering or dimensions mismatch");
        previous = member;
    }
    Reader results(view.results), binding_members(view.members);
    std::optional<CommitGroupMember> binding;
    std::uint64_t member_at = 0U; CommitGroupId previous_epoch{};
    for (std::uint64_t i = 0U; i < view.result_count; ++i) {
        const auto epoch = results.Array<16U>(); Identity(epoch); const auto revision = results.UInt(8U);
        const auto length = results.UInt(4U); results.Zeros(4U);
        if (length == 0U || (i != 0U && !(previous_epoch < epoch))) Bad("result labels or ordering");
        (void)results.Take(static_cast<std::size_t>(length)); previous_epoch = epoch;
        if (!binding && member_at < view.member_count) binding = ReadMember(binding_members, member_at++);
        while (binding && binding->table_epoch <= epoch) {
            if (binding->table_epoch != epoch || binding->revision > revision) Bad("member result binding");
            binding = member_at < view.member_count ? std::optional{ReadMember(binding_members, member_at++)} : std::nullopt;
        }
    }
    if (results.remaining() != 0U || binding || member_at != view.member_count) Bad("result extent or missing member result");
    return view;
}
}  // namespace

CommitRecordHeader InspectCommitGroupRecordHeader(Bytes bytes, const CommitGroupId& expected_directory) {
    Identity(expected_directory);
    if (bytes.size() != kCommitRecordHeaderBytes) Bad("record header length");
    CheckCrc(bytes, 60U); Reader reader(bytes); Magic(reader, "CKGR"); Tag(reader);
    CommitRecordHeader header;
    header.kind = static_cast<CommitRecordKind>(reader.UInt(1U)); reader.Zeros(1U); header.record_bytes = reader.UInt(8U);
    header.directory_id = reader.Array<16U>(); header.id = reader.Array<16U>(); header.payload_bytes = reader.UInt(8U); reader.Zeros(4U);
    Identity(header.id);
    if (header.directory_id != expected_directory || header.record_bytes < RecordMinimum(header.kind) || header.record_bytes > RecordLimit(header.kind) ||
        header.payload_bytes != header.record_bytes - 68U ||
        (header.kind == CommitRecordKind::kReceipt && header.record_bytes != kCommitReceiptBytes)) Bad("record context or byte extent");
    return header;
}

std::vector<std::uint8_t> SerializeCommitGroupDecision(const CommitGroupDecision& decision) {
    std::vector<std::uint8_t> bytes(DecisionSize(decision));
    EnvelopeHeader(bytes, CommitRecordKind::kDecision, decision.directory_id, decision.id);
    Writer writer{std::span(bytes).subspan(kCommitRecordHeaderBytes, bytes.size() - 68U)};
    writer.UInt(static_cast<std::uint8_t>(decision.durability), 1U);
    writer.UInt((decision.request ? 1U : 0U) | (decision.tag ? 2U : 0U), 1U); writer.Zeros(6U);
    for (const auto value : {decision.txn, decision.created_ms, static_cast<std::uint64_t>(decision.members.size()),
        static_cast<std::uint64_t>(decision.versions.size())}) writer.UInt(value, 8U);
    writer.UInt(decision.request ? decision.request->user.size() : 0U, 4U);
    writer.UInt(decision.request ? decision.request->id.size() : 0U, 4U); writer.UInt(decision.tag ? decision.tag->size() : 0U, 4U); writer.Zeros(4U);
    writer.Put(decision.request ? decision.request->fingerprint : crypto::Sha256Digest{});
    if (decision.request) { writer.Put(Raw(decision.request->user)); writer.Put(decision.request->id); }
    if (decision.tag) writer.Put(*decision.tag);
    for (std::size_t i = 0U; i < decision.members.size(); ++i) WriteMember(writer, decision.members[i], i);
    for (const auto& result : decision.versions) {
        writer.Put(result.table_epoch); writer.UInt(result.revision, 8U); writer.UInt(result.table_name.size(), 4U); writer.Zeros(4U); writer.Put(Raw(result.table_name));
    }
    PutCrc(bytes, bytes.size() - 4U); return bytes;
}

CommitGroupDecision ParseCommitGroupDecision(Bytes bytes, const CommitGroupId& expected_directory) {
    const auto view = InspectDecision(bytes, expected_directory);
    CommitGroupDecision decision{.directory_id = view.header.directory_id, .id = view.header.id,
        .durability = view.durability, .txn = view.txn, .created_ms = view.created_ms,
        .request = {}, .tag = {}, .members = {}, .versions = {}};
    if (view.request) decision.request = CommitRequestBinding{
        .user = std::string(reinterpret_cast<const char*>(view.user.data()), view.user.size()),
        .id = std::vector<std::uint8_t>(view.id.begin(), view.id.end()), .fingerprint = view.fingerprint};
    if (view.tag_present) decision.tag.emplace(view.tag.begin(), view.tag.end());
    decision.members.reserve(static_cast<std::size_t>(view.member_count)); Reader members(view.members);
    for (std::uint64_t i = 0U; i < view.member_count; ++i) decision.members.push_back(ReadMember(members, i));
    decision.versions.reserve(static_cast<std::size_t>(view.result_count)); Reader results(view.results);
    for (std::uint64_t i = 0U; i < view.result_count; ++i) {
        CommitTableResult result; result.table_epoch = results.Array<16U>(); result.revision = results.UInt(8U);
        const auto length = results.UInt(4U); results.Zeros(4U); const auto name = results.Take(static_cast<std::size_t>(length));
        result.table_name.assign(reinterpret_cast<const char*>(name.data()), name.size()); decision.versions.push_back(std::move(result));
    }
    return decision;
}

std::array<std::uint8_t, kCommitReceiptBytes> SerializeCommitGroupReceipt(const CommitGroupReceipt& receipt) {
    std::array<std::uint8_t, kCommitReceiptBytes> bytes{};
    EnvelopeHeader(bytes, CommitRecordKind::kReceipt, receipt.directory_id, receipt.id);
    Writer(std::span(bytes).subspan(kCommitRecordHeaderBytes, 32U)).Put(receipt.decision_sha256);
    PutCrc(bytes, bytes.size() - 4U); return bytes;
}

CommitGroupReceipt ParseCommitGroupReceipt(Bytes bytes, const CommitGroupId& expected_directory) {
    CommitRecordHeader header;
    Reader reader(Payload(bytes, expected_directory, CommitRecordKind::kReceipt, &header));
    return {header.directory_id, header.id, reader.Array<32U>()};
}

void ValidateCommitGroupReceipt(const CommitGroupReceipt& receipt, const CommitGroupDecision& decision,
    const crypto::Sha256Digest& checked_decision_sha256) {
    (void)DecisionSize(decision);
    if (receipt.directory_id != decision.directory_id || receipt.id != decision.id ||
        receipt.decision_sha256 != checked_decision_sha256) Bad("receipt decision binding");
}

std::vector<std::uint8_t> SerializeCommitGroupSeal(const CommitGroupSeal& seal) {
    const auto decision = SerializeCommitGroupDecision(seal.decision);
    if (crypto::Sha256Of(decision) != seal.decision_sha256) Bad("seal decision digest");
    std::vector<std::uint8_t> bytes(Add(108U, decision.size(), kMaxCommitSealBytes));
    EnvelopeHeader(bytes, CommitRecordKind::kSeal, seal.decision.directory_id, seal.decision.id);
    Writer writer{std::span(bytes).subspan(kCommitRecordHeaderBytes, bytes.size() - 68U)};
    writer.UInt(decision.size(), 8U); writer.Put(seal.decision_sha256); writer.Put(decision);
    PutCrc(bytes, bytes.size() - 4U); return bytes;
}

CommitGroupSeal ParseCommitGroupSeal(Bytes bytes, const CommitGroupId& expected_directory) {
    CommitRecordHeader header; Reader reader(Payload(bytes, expected_directory, CommitRecordKind::kSeal, &header));
    const auto length = reader.UInt(8U); const auto digest = reader.Array<32U>();
    if (length < 68U || length > kMaxCommitDecisionBytes || length != reader.remaining()) Bad("seal decision extent");
    const auto decision = reader.Take(static_cast<std::size_t>(length));
    const auto nested = InspectCommitGroupRecordHeader(decision.first(kCommitRecordHeaderBytes), expected_directory);
    if (nested.kind != CommitRecordKind::kDecision || nested.id != header.id || crypto::Sha256Of(decision) != digest)
        Bad("seal decision binding");
    return {ParseCommitGroupDecision(decision, expected_directory), digest};
}

namespace {
void RequireContext(const CommitJournalContext& context) {
    Identity(context.directory_id); Identity(context.journal_id);
    if (context.generation == 0U) Bad("journal generation is zero");
}
void WriteContext(Writer& writer, const CommitJournalContext& context) {
    writer.Put(context.directory_id); writer.Put(context.journal_id); writer.UInt(context.generation, 8U);
}
CommitJournalContext ReadContext(Reader& reader) {
    CommitJournalContext context{reader.Array<16U>(), reader.Array<16U>(), reader.UInt(8U)};
    RequireContext(context); return context;
}
void RequireCut(const CommitJournalCut& cut) {
    if (cut.bytes < kCommitJournalHeaderBytes) Bad("journal cut precedes header");
    if (cut.bytes == kCommitJournalHeaderBytes) {
        if (!Zero(cut.last_group) || cut.last_kind != CommitRecordKind::kNone) Bad("empty journal has a last record");
    } else if (Zero(cut.last_group) || !RecordKind(cut.last_kind)) Bad("journal cut lacks last record identity");
}
void WriteCut(Writer& writer, const CommitJournalCut& cut) {
    writer.UInt(cut.bytes, 8U); writer.Put(cut.last_group); writer.UInt(static_cast<std::uint8_t>(cut.last_kind), 1U);
    writer.Zeros(7U); writer.Put(cut.sha256);
}
CommitJournalCut ReadCut(Reader& reader) {
    CommitJournalCut cut;
    cut.bytes = reader.UInt(8U); cut.last_group = reader.Array<16U>(); cut.last_kind = static_cast<CommitRecordKind>(reader.UInt(1U));
    reader.Zeros(7U); cut.sha256 = reader.Array<32U>(); RequireCut(cut); return cut;
}
void RequireProof(const CommitJournalPrefixProof& proof) {
    RequireContext(proof.context); RequireCut(proof.cut);
    if (proof.last_complete_record_end != proof.cut.bytes) Bad("journal cut is between records");
}
bool BytesBefore(Bytes before, Bytes after) {
    return std::lexicographical_compare(before.begin(), before.end(), after.begin(), after.end());
}
bool KeyBefore(Bytes user, Bytes id, Bytes next_user, Bytes next_id) {
    return BytesBefore(user, next_user) || (std::equal(user.begin(), user.end(), next_user.begin(), next_user.end()) && BytesBefore(id, next_id));
}
void RequireIdEntry(const CommitIdEntry& entry) {
    Identity(entry.group_id); Length32(entry.user.size());
    if (entry.id.size() > 64U) Bad("retained ID exceeds format bound");
}
std::size_t IdInventorySize(const CommitIdInventory& inventory) {
    RequireContext(inventory.context); RequireCut(inventory.replay);
    if (inventory.generation == 0U) Bad("ID inventory generation is zero");
    std::size_t size = kCommitIdInventoryHeaderBytes + 4U;
    for (std::size_t i = 0U; i < inventory.entries.size(); ++i) {
        const auto& entry = inventory.entries[i]; RequireIdEntry(entry);
        if (i != 0U && !KeyBefore(Raw(inventory.entries[i - 1U].user), inventory.entries[i - 1U].id, Raw(entry.user), entry.id))
            Bad("retained IDs are not unique and ordered");
        size = Add(Add(Add(size, kIdEntryPrefixBytes, kMaxCommitIdInventoryBytes), entry.user.size(), kMaxCommitIdInventoryBytes),
            entry.id.size(), kMaxCommitIdInventoryBytes);
    }
    return size;
}
struct IdEntryView {
    CommitGroupId group;
    crypto::Sha256Digest decision, fingerprint;
    std::uint64_t created_ms;
    Bytes user, id;
};
IdEntryView ReadIdEntry(Reader& entries) {
    const auto bytes = entries.UInt(8U);
    if (bytes < kIdEntryPrefixBytes || bytes - 8U > entries.remaining()) Bad("retained ID entry extent");
    Reader entry(entries.Take(static_cast<std::size_t>(bytes - 8U)));
    IdEntryView view;
    view.group = entry.Array<16U>(); Identity(view.group); view.decision = entry.Array<32U>(); view.fingerprint = entry.Array<32U>();
    view.created_ms = entry.UInt(8U); const auto user = entry.UInt(4U), id = entry.UInt(4U); entry.Zeros(8U);
    if (id > 64U) Bad("retained ID exceeds format bound");
    view.user = entry.Take(static_cast<std::size_t>(user)); view.id = entry.Take(static_cast<std::size_t>(id));
    if (entry.remaining() != 0U) Bad("retained ID has trailing bytes");
    return view;
}
}  // namespace

std::array<std::uint8_t, kCommitJournalHeaderBytes> SerializeCommitJournalHeader(const CommitJournalContext& context) {
    RequireContext(context); std::array<std::uint8_t, kCommitJournalHeaderBytes> bytes{}; Writer writer(bytes);
    writer.Put(Raw("CKGJ")); writer.UInt(1U, 2U); writer.Zeros(2U); WriteContext(writer, context); writer.Zeros(16U);
    PutCrc(bytes, 60U); return bytes;
}

CommitJournalContext ParseCommitJournalHeader(Bytes bytes, const CommitJournalContext& expected) {
    RequireContext(expected);
    if (bytes.size() != kCommitJournalHeaderBytes) Bad("journal header length");
    CheckCrc(bytes, 60U); Reader reader(bytes); Magic(reader, "CKGJ"); Tag(reader); reader.Zeros(2U);
    const auto context = ReadContext(reader); reader.Zeros(12U);
    if (context != expected) Bad("journal header context");
    return context;
}

std::array<std::uint8_t, kCommitJournalInventoryBytes> SerializeCommitJournalInventory(const CommitJournalInventory& inventory) {
    RequireContext(inventory.context); RequireCut(inventory.certified);
    if (inventory.generation == 0U) Bad("journal inventory generation is zero");
    std::array<std::uint8_t, kCommitJournalInventoryBytes> bytes{}; Writer writer(bytes);
    writer.Put(Raw("CKGC")); writer.UInt(1U, 2U); writer.Zeros(2U); WriteContext(writer, inventory.context);
    WriteCut(writer, inventory.certified); writer.UInt(inventory.generation, 8U); writer.Zeros(8U);
    PutCrc(bytes, 124U); return bytes;
}

CommitJournalInventory ParseCommitJournalInventory(Bytes bytes, const CommitGroupId& expected_directory) {
    Identity(expected_directory);
    if (bytes.size() != kCommitJournalInventoryBytes) Bad("journal inventory length");
    CheckCrc(bytes, 124U); Reader reader(bytes); Magic(reader, "CKGC"); Tag(reader); reader.Zeros(2U);
    CommitJournalInventory inventory; inventory.context = ReadContext(reader); inventory.certified = ReadCut(reader);
    inventory.generation = reader.UInt(8U); reader.Zeros(4U);
    if (inventory.context.directory_id != expected_directory || inventory.generation == 0U) Bad("journal inventory context or generation");
    return inventory;
}

void ValidateCommitJournalPrefix(const CommitJournalInventory& inventory, const CommitJournalPrefixProof& checked_prefix) {
    RequireContext(inventory.context); RequireCut(inventory.certified); RequireProof(checked_prefix);
    if (inventory.generation == 0U || inventory.context != checked_prefix.context || inventory.certified != checked_prefix.cut)
        Bad("certified journal prefix binding");
}

std::vector<std::uint8_t> SerializeCommitIdInventory(const CommitIdInventory& inventory) {
    std::vector<std::uint8_t> bytes(IdInventorySize(inventory)); Writer writer(bytes);
    writer.Put(Raw("CKID")); writer.UInt(1U, 2U); writer.Zeros(2U); writer.UInt(bytes.size(), 8U);
    WriteContext(writer, inventory.context); writer.UInt(inventory.generation, 8U); writer.UInt(inventory.replay.bytes, 8U);
    writer.UInt(inventory.entries.size(), 8U); writer.Put(inventory.replay.last_group);
    writer.UInt(static_cast<std::uint8_t>(inventory.replay.last_kind), 1U); writer.Zeros(7U); writer.Put(inventory.replay.sha256);
    writer.UInt(bytes.size() - kCommitIdInventoryHeaderBytes - 4U, 8U); writer.Zeros(16U);
    PutCrc(std::span(bytes).first(kCommitIdInventoryHeaderBytes), 156U);
    for (const auto& entry : inventory.entries) {
        writer.UInt(kIdEntryPrefixBytes + entry.user.size() + entry.id.size(), 8U); writer.Put(entry.group_id);
        writer.Put(entry.decision_sha256); writer.Put(entry.fingerprint); writer.UInt(entry.created_ms, 8U);
        writer.UInt(entry.user.size(), 4U); writer.UInt(entry.id.size(), 4U); writer.Zeros(8U); writer.Put(Raw(entry.user)); writer.Put(entry.id);
    }
    PutCrc(bytes, bytes.size() - 4U); return bytes;
}

CommitIdInventoryHeader InspectCommitIdInventoryHeader(Bytes bytes, const CommitJournalContext& expected) {
    RequireContext(expected);
    if (bytes.size() != kCommitIdInventoryHeaderBytes) Bad("ID inventory header length");
    CheckCrc(bytes, 156U); Reader reader(bytes); Magic(reader, "CKID"); Tag(reader); reader.Zeros(2U);
    CommitIdInventoryHeader header; header.record_bytes = reader.UInt(8U); header.context = ReadContext(reader);
    header.generation = reader.UInt(8U); header.replay.bytes = reader.UInt(8U); header.entry_count = reader.UInt(8U);
    header.replay.last_group = reader.Array<16U>(); header.replay.last_kind = static_cast<CommitRecordKind>(reader.UInt(1U));
    reader.Zeros(7U); header.replay.sha256 = reader.Array<32U>(); header.payload_bytes = reader.UInt(8U); reader.Zeros(12U);
    RequireCut(header.replay);
    if (header.context != expected || header.generation == 0U || header.record_bytes < kCommitIdInventoryHeaderBytes + 4U ||
        header.record_bytes > kMaxCommitIdInventoryBytes || header.payload_bytes != header.record_bytes - kCommitIdInventoryHeaderBytes - 4U)
        Bad("ID inventory context or byte extent");
    (void)Repeated(header.entry_count, kIdEntryPrefixBytes, static_cast<std::size_t>(header.payload_bytes));
    if (header.entry_count == 0U && header.payload_bytes != 0U) Bad("empty ID inventory has payload bytes");
    return header;
}

CommitIdInventory ParseCommitIdInventory(Bytes bytes, const CommitJournalContext& expected) {
    if (bytes.size() < kCommitIdInventoryHeaderBytes + 4U) Bad("ID inventory is truncated");
    const auto header = InspectCommitIdInventoryHeader(bytes.first(kCommitIdInventoryHeaderBytes), expected);
    if (header.record_bytes != bytes.size()) Bad("ID inventory complete extent");
    CheckCrc(bytes, bytes.size() - 4U);
    const auto payload = bytes.subspan(kCommitIdInventoryHeaderBytes, bytes.size() - kCommitIdInventoryHeaderBytes - 4U);
    Reader admission(payload); Bytes previous_user, previous_id;
    for (std::uint64_t i = 0U; i < header.entry_count; ++i) {
        const auto entry = ReadIdEntry(admission);
        if (i != 0U && !KeyBefore(previous_user, previous_id, entry.user, entry.id)) Bad("retained IDs are not unique and ordered");
        previous_user = entry.user; previous_id = entry.id;
    }
    if (admission.remaining() != 0U) Bad("ID inventory trailing bytes");
    CommitIdInventory inventory{.context = header.context, .generation = header.generation, .replay = header.replay, .entries = {}};
    inventory.entries.reserve(static_cast<std::size_t>(header.entry_count)); Reader entries(payload);
    for (std::uint64_t i = 0U; i < header.entry_count; ++i) {
        const auto view = ReadIdEntry(entries);
        inventory.entries.push_back({.user = std::string(reinterpret_cast<const char*>(view.user.data()), view.user.size()),
            .id = std::vector<std::uint8_t>(view.id.begin(), view.id.end()), .group_id = view.group,
            .decision_sha256 = view.decision, .fingerprint = view.fingerprint, .created_ms = view.created_ms});
    }
    return inventory;
}

void ValidateCommitIdInventoryCursor(const CommitIdInventory& inventory, const CommitJournalInventory& certified,
    const CommitJournalPrefixProof& checked_cursor) {
    RequireContext(inventory.context); RequireCut(inventory.replay); RequireContext(certified.context); RequireCut(certified.certified);
    RequireProof(checked_cursor);
    if (inventory.generation == 0U || certified.generation == 0U || inventory.context != certified.context ||
        inventory.context != checked_cursor.context || inventory.replay != checked_cursor.cut ||
        inventory.replay.bytes > certified.certified.bytes ||
        (inventory.replay.bytes == certified.certified.bytes && inventory.replay != certified.certified)) Bad("retained ID journal cursor binding");
}

void ValidateCommitIdEntry(const CommitIdEntry& entry, const CommitGroupId& expected_directory, const CommitGroupDecision& decision,
    const crypto::Sha256Digest& checked_decision_sha256) {
    Identity(expected_directory); RequireIdEntry(entry); (void)DecisionSize(decision);
    if (decision.directory_id != expected_directory || !decision.request || entry.group_id != decision.id || entry.decision_sha256 != checked_decision_sha256 ||
        entry.user != decision.request->user || entry.id != decision.request->id ||
        entry.fingerprint != decision.request->fingerprint || entry.created_ms != decision.created_ms) Bad("retained ID decision binding");
}

}  // namespace chunkdb
