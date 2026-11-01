#include "commit_group_owner_format.hpp"

#include <algorithm>
#include <bit>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

#include "chunkdb/crc32.hpp"

namespace chunkdb {
namespace {
using Bytes = std::span<const std::uint8_t>;
[[noreturn]] void Bad(const char* reason) { throw std::invalid_argument(std::string("commit owner format: ") + reason); }
template <std::size_t N> bool Zero(const std::array<std::uint8_t, N>& value) {
    return std::all_of(value.begin(), value.end(), [](auto byte) { return byte == 0U; });
}
void Identity(const CommitGroupId& id) { if (Zero(id)) Bad("zero identity"); }
void Scope(const CommitOwnerScope& scope) {
    Identity(scope.directory_id); Identity(scope.owner_id);
    if (scope.role != CommitOwnerRole::kStandalone && scope.role != CommitOwnerRole::kCatalog) Bad("scope role");
}
std::uint64_t Add(std::uint64_t left, std::uint64_t right) {
    if (right > std::numeric_limits<std::uint64_t>::max() - left) Bad("extent overflow");
    return left + right;
}
class Reader {
  public:
    explicit Reader(Bytes bytes) : bytes_(bytes) {}
    Bytes Take(std::size_t count) {
        if (count > bytes_.size()) Bad("truncated field");
        const auto result = bytes_.first(count); bytes_ = bytes_.subspan(count); return result;
    }
    std::uint64_t UInt(unsigned width) {
        const auto data = Take(width); std::uint64_t value = 0U;
        for (unsigned i = 0U; i < width; ++i) value |= static_cast<std::uint64_t>(data[i]) << (i * 8U);
        return value;
    }
    template <std::size_t N> std::array<std::uint8_t, N> Array() {
        std::array<std::uint8_t, N> result{}; const auto bytes = Take(N);
        std::copy(bytes.begin(), bytes.end(), result.begin()); return result;
    }
    void Zeros(std::size_t count) {
        const auto bytes = Take(count);
        if (std::any_of(bytes.begin(), bytes.end(), [](auto byte) { return byte != 0U; })) Bad("reserved bytes");
    }
  private: Bytes bytes_;
};
class Writer {
  public:
    explicit Writer(std::span<std::uint8_t> bytes) : bytes_(bytes) {}
    void UInt(std::uint64_t value, unsigned width) {
        if (width > bytes_.size()) Bad("internal writer extent");
        for (unsigned i = 0U; i < width; ++i) bytes_[i] = static_cast<std::uint8_t>(value >> (i * 8U));
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
  private: std::span<std::uint8_t> bytes_;
};
void Crc(std::span<std::uint8_t> bytes, std::size_t at) { Writer(bytes.subspan(at, 4U)).UInt(Crc32(bytes.data(), at), 4U); }
void CheckCrc(Bytes bytes, std::size_t at) {
    if (at > bytes.size() || bytes.size() - at < 4U) Bad("checksum extent");
    if (Reader(bytes.subspan(at, 4U)).UInt(4U) != Crc32(bytes.data(), at)) Bad("checksum");
}
void Ref(const CommitOwnerObjectRef& ref) {
    const auto kind = static_cast<unsigned>(ref.kind);
    if (kind == 0U) {
        if (ref.bytes != 0U || !Zero(ref.sha256)) Bad("absent reference");
        return;
    }
    if (kind > 10U || Zero(ref.sha256)) Bad("reference kind or digest");
    std::uint64_t low = 68U, high = kMaxCommitOwnerPageBytes;
    switch (ref.kind) {
        case CommitOwnerObjectKind::kRoot: low = high = kCommitOwnerRootBytes; break;
        case CommitOwnerObjectKind::kPage: low = kCommitOwnerEmptyPageBytes; break;
        case CommitOwnerObjectKind::kMemberValue: low = high = kCommitOwnerMemberBytes; break;
        case CommitOwnerObjectKind::kJournalInventory: low = high = kCommitJournalInventoryBytes; break;
        case CommitOwnerObjectKind::kIdInventory: low = 164U; high = kMaxCommitIdInventoryBytes; break;
        default: break;
    }
    if (ref.bytes < low || ref.bytes > high) Bad("reference byte bound");
}
void Kind(const CommitOwnerObjectRef& ref, CommitOwnerObjectKind expected) {
    Ref(ref); if (ref.kind != expected) Bad("reference field kind");
}
void WriteRef(Writer& writer, const CommitOwnerObjectRef& ref) {
    Ref(ref); writer.UInt(static_cast<std::uint8_t>(ref.kind), 1U); writer.Zeros(7U);
    writer.UInt(ref.bytes, 8U); writer.Put(ref.sha256);
}
CommitOwnerObjectRef ReadRef(Reader& reader) {
    CommitOwnerObjectRef result; result.kind = static_cast<CommitOwnerObjectKind>(reader.UInt(1U)); reader.Zeros(7U);
    result.bytes = reader.UInt(8U); result.sha256 = reader.Array<32U>(); Ref(result); return result;
}
void Cursor(const CommitJournalContext& context, const CommitJournalCut& cut) {
    (void)SerializeCommitJournalHeader(context);
    CommitJournalInventory inventory{context, 1U, cut};
    (void)SerializeCommitJournalInventory(inventory);
}
void WriteCursor(Writer& writer, const CommitJournalContext& context, const CommitJournalCut& cut) {
    writer.Put(context.directory_id); writer.Put(context.journal_id); writer.UInt(context.generation, 8U);
    writer.UInt(cut.bytes, 8U); writer.Put(cut.last_group); writer.UInt(static_cast<std::uint8_t>(cut.last_kind), 1U);
    writer.Zeros(7U); writer.Put(cut.sha256);
}
void ReadCursor(Reader& reader, CommitJournalContext& context, CommitJournalCut& cut) {
    context.directory_id = reader.Array<16U>(); context.journal_id = reader.Array<16U>(); context.generation = reader.UInt(8U);
    cut.bytes = reader.UInt(8U); cut.last_group = reader.Array<16U>(); cut.last_kind = static_cast<CommitRecordKind>(reader.UInt(1U));
    reader.Zeros(7U); cut.sha256 = reader.Array<32U>();
}
void Root(const CommitOwnerRoot& root) {
    Scope(root.scope); if (root.generation == 0U) Bad("root generation");
    if (root.state == CommitOwnerState::kInactive) {
        CommitOwnerRoot empty; empty.scope = root.scope; empty.generation = root.generation;
        if (root != empty) Bad("inactive selected objects");
        return;
    }
    if (root.state != CommitOwnerState::kActive && root.state != CommitOwnerState::kPending) Bad("root state");
    Kind(root.coverage_tree, CommitOwnerObjectKind::kPage); Kind(root.group_tree, CommitOwnerObjectKind::kPage);
    Kind(root.journal_inventory, CommitOwnerObjectKind::kJournalInventory); Kind(root.id_inventory, CommitOwnerObjectKind::kIdInventory);
    Cursor(root.reference_context, root.reference_replay);
    if (root.reference_context.directory_id != root.scope.directory_id) Bad("replay directory");
    if (Zero(root.counter_owner) && root.counter_ceiling != 0U) Bad("counter binding");
    if (root.state == CommitOwnerState::kActive) {
        Kind(root.replacement_plan, CommitOwnerObjectKind::kNone); Kind(root.previous_root, CommitOwnerObjectKind::kNone);
    } else {
        Kind(root.replacement_plan, CommitOwnerObjectKind::kReplacementPlan); Kind(root.previous_root, CommitOwnerObjectKind::kRoot);
    }
}
void Header(Writer& writer, CommitOwnerObjectKind kind, const CommitOwnerScope& scope,
    std::uint64_t generation, std::size_t bytes) {
    static constexpr std::array<std::uint8_t, 4U> magic{'C', 'K', 'G', 'O'};
    writer.Put(magic); writer.UInt(1U, 2U); writer.UInt(static_cast<std::uint8_t>(kind), 1U);
    writer.UInt(static_cast<std::uint8_t>(scope.role), 1U); writer.UInt(bytes, 8U);
    writer.Put(scope.directory_id); writer.Put(scope.owner_id); writer.UInt(generation, 8U); writer.Zeros(8U);
}
std::size_t Width(CommitOwnerTreeKind tree) {
    switch (tree) {
        case CommitOwnerTreeKind::kCoverage: return 41U;
        case CommitOwnerTreeKind::kGroups: return 16U;
        case CommitOwnerTreeKind::kMembers: return 24U;
    }
    Bad("tree kind");
}
CommitOwnerObjectKind ValueKind(CommitOwnerTreeKind tree) {
    switch (tree) {
        case CommitOwnerTreeKind::kCoverage: return CommitOwnerObjectKind::kCoverageValue;
        case CommitOwnerTreeKind::kGroups: return CommitOwnerObjectKind::kGroupValue;
        case CommitOwnerTreeKind::kMembers: return CommitOwnerObjectKind::kMemberValue;
    }
    Bad("tree kind");
}
void Prefix(CommitOwnerTreeKind tree, Bytes prefix) {
    const auto width = Width(tree); if (prefix.size() > width) Bad("key prefix length");
    if (prefix.size() >= 16U && std::all_of(prefix.begin(), prefix.begin() + 16, [](auto b) { return b == 0U; }))
        Bad("zero key identity");
    if (tree == CommitOwnerTreeKind::kCoverage && prefix.size() >= 17U) {
        if (prefix[16U] != 2U && prefix[16U] != 3U) Bad("key dimensions");
        if (prefix[16U] == 2U && prefix.size() > 33U) {
            if (prefix[33U] != 0x80U || std::any_of(prefix.begin() + 34, prefix.end(), [](auto b) { return b != 0U; }))
                Bad("inactive key depth");
        }
    }
}
std::size_t Page(const CommitOwnerPage& page) {
    Scope(page.scope); if (page.generation == 0U) Bad("page generation");
    const auto width = Width(page.tree); Prefix(page.tree, page.prefix);
    if (page.children.size() > 256U) Bad("physical child count");
    std::size_t size = kCommitOwnerEmptyPageBytes + page.prefix.size();
    std::uint64_t keys = 0U, owned = 0U;
    if (page.node == CommitOwnerNodeKind::kEmpty) {
        if (!page.prefix.empty() || page.value || !page.children.empty()) Bad("empty page shape");
    } else if (page.node == CommitOwnerNodeKind::kLeaf) {
        if (page.prefix.size() != width || !page.value || !page.children.empty()) Bad("leaf shape");
        Kind(*page.value, ValueKind(page.tree)); size += kCommitOwnerReferenceBytes;
        keys = 1U; owned = page.value->bytes;
    } else if (page.node == CommitOwnerNodeKind::kBranch) {
        if (page.prefix.size() >= width || page.value || page.children.size() < 2U) Bad("branch shape");
        size += 72U * page.children.size();
        int previous = -1;
        for (const auto& child : page.children) {
            Kind(child.page, CommitOwnerObjectKind::kPage);
            if (child.edge <= previous || child.subtree_keys == 0U || child.subtree_bytes < child.page.bytes) Bad("child claims");
            previous = child.edge; keys = Add(keys, child.subtree_keys); owned = Add(owned, child.subtree_bytes);
        }
    } else Bad("page node kind");
    if (size > kMaxCommitOwnerPageBytes) Bad("page byte ceiling");
    owned = Add(owned, size);
    if (keys != page.subtree_keys || owned != page.subtree_bytes) Bad("subtree totals");
    if (page.tree == CommitOwnerTreeKind::kMembers && owned > kMaxCommitCoveredTreeBytes) Bad("covered metadata ceiling");
    return size;
}
CommitOwnerObjectRef Object(CommitOwnerObjectKind kind, Bytes bytes) {
    return {kind, static_cast<std::uint64_t>(bytes.size()), crypto::Sha256Of(bytes)};
}
void CounterSuccessor(const CommitOwnerRoot& before, const CommitOwnerRoot& after) {
    if ((!Zero(before.counter_owner) && before.counter_owner != after.counter_owner) ||
        after.counter_ceiling < before.counter_ceiling) Bad("counter ownership regression");
}
void MemberValue(const CommitOwnerMemberValue& value) {
    Scope(value.scope); Identity(value.group_id); Identity(value.coverage.table_epoch);
    if (value.generation == 0U || Zero(value.decision_sha256) || value.member_count == 0U ||
        value.ordinal >= value.member_count) Bad("covered member identity or ordinal");
    if ((value.coverage.dims != 2U && value.coverage.dims != 3U) ||
        (value.coverage.dims == 2U && value.coverage.coord.z != 0)) Bad("covered member dimensions");
    if (value.kind == CommitGroupMemberKind::kChanged) {
        if (value.revision == 0U || value.schema_version == 0U) Bad("covered changed identity");
    } else if (value.kind == CommitGroupMemberKind::kObserved) {
        if (value.revision == 0U ? value.commit_time_ms != 0U || value.schema_version != 0U :
            value.schema_version == 0U) Bad("covered observed identity");
    } else Bad("covered member kind");
}
void Plan(const CommitOwnerPlanProof& plan, const CommitOwnerScope& scope) {
    Scope(plan.scope); Root(plan.previous); Root(plan.target);
    Kind(plan.plan, CommitOwnerObjectKind::kReplacementPlan); Kind(plan.previous_root, CommitOwnerObjectKind::kRoot);
    Kind(plan.final_root, CommitOwnerObjectKind::kRoot);
    if (plan.scope != scope || plan.previous.scope != scope || plan.previous.state != CommitOwnerState::kActive ||
        plan.previous_root != Object(CommitOwnerObjectKind::kRoot, SerializeCommitOwnerRoot(plan.previous)) ||
        plan.previous.generation > std::numeric_limits<std::uint64_t>::max() - 2U ||
        plan.target.generation != plan.previous.generation + 2U ||
        plan.target.scope != scope || plan.target.state != CommitOwnerState::kActive ||
        plan.final_root != Object(CommitOwnerObjectKind::kRoot, SerializeCommitOwnerRoot(plan.target))) Bad("replacement target binding");
    CounterSuccessor(plan.previous, plan.target);
}
void BigEndian(std::span<std::uint8_t, 8U> output, std::uint64_t value) {
    for (unsigned i = 0U; i < 8U; ++i) output[i] = static_cast<std::uint8_t>(value >> ((7U - i) * 8U));
}
std::uint64_t BigEndian(Bytes input) {
    std::uint64_t value = 0U; for (const auto byte : input) value = (value << 8U) | byte; return value;
}
}  // namespace

std::array<std::uint8_t, kCommitOwnerScopeBytes> SerializeCommitOwnerScope(const CommitOwnerScope& scope) {
    Scope(scope); std::array<std::uint8_t, kCommitOwnerScopeBytes> bytes{}; Writer writer(bytes);
    writer.Put(scope.directory_id); writer.Put(scope.owner_id); writer.UInt(static_cast<std::uint8_t>(scope.role), 1U); writer.Zeros(7U); return bytes;
}
CommitOwnerScope ParseCommitOwnerScope(Bytes bytes) {
    if (bytes.size() != kCommitOwnerScopeBytes) Bad("scope extent");
    Reader reader(bytes);
    CommitOwnerScope scope; scope.directory_id = reader.Array<16U>(); scope.owner_id = reader.Array<16U>();
    scope.role = static_cast<CommitOwnerRole>(reader.UInt(1U)); reader.Zeros(7U); Scope(scope); return scope;
}
void ValidateCommitOwnerStoreScope(const CommitOwnerScope& scope, const CommitGroupId& store_id,
    const std::optional<CommitOwnerScope>& directory_scope) {
    Scope(scope); Identity(store_id);
    if (scope.role == CommitOwnerRole::kStandalone) {
        if (directory_scope || scope.directory_id != store_id) Bad("standalone store scope");
    } else {
        if (!directory_scope) Bad("missing catalog owner scope");
        Scope(*directory_scope); if (*directory_scope != scope) Bad("catalog owner scope");
    }
}
CommitOwnerHeader InspectCommitOwnerHeader(Bytes bytes, const CommitOwnerScope& expected,
    CommitOwnerObjectKind expected_kind, std::optional<std::uint64_t> maximum_generation) {
    Scope(expected); if (bytes.size() != kCommitOwnerHeaderBytes) Bad("header extent"); CheckCrc(bytes, 60U);
    Reader reader(bytes); const auto magic = reader.Array<4U>();
    if (magic != std::array<std::uint8_t, 4U>{'C', 'K', 'G', 'O'} || reader.UInt(2U) != 1U) Bad("header magic or tag");
    CommitOwnerHeader header; header.kind = static_cast<CommitOwnerObjectKind>(reader.UInt(1U));
    header.scope.role = static_cast<CommitOwnerRole>(reader.UInt(1U)); header.bytes = reader.UInt(8U);
    header.scope.directory_id = reader.Array<16U>(); header.scope.owner_id = reader.Array<16U>();
    header.generation = reader.UInt(8U); reader.Zeros(4U);
    if (header.scope != expected || header.kind != expected_kind || header.generation == 0U ||
        (maximum_generation && header.generation > *maximum_generation)) Bad("header context or generation");
    if (header.kind == CommitOwnerObjectKind::kRoot) {
        if (header.bytes != kCommitOwnerRootBytes) Bad("root extent");
    } else if (header.kind == CommitOwnerObjectKind::kPage) {
        if (header.bytes < kCommitOwnerEmptyPageBytes || header.bytes > kMaxCommitOwnerPageBytes) Bad("page extent");
    } else if (header.kind == CommitOwnerObjectKind::kMemberValue) {
        if (header.bytes != kCommitOwnerMemberBytes) Bad("covered member extent");
    } else Bad("unsupported owner envelope kind");
    return header;
}
std::array<std::uint8_t, kCommitOwnerRootBytes> SerializeCommitOwnerRoot(const CommitOwnerRoot& root) {
    Root(root); std::array<std::uint8_t, kCommitOwnerRootBytes> bytes{}; Writer writer(bytes);
    Header(writer, CommitOwnerObjectKind::kRoot, root.scope, root.generation, bytes.size());
    writer.UInt(static_cast<std::uint8_t>(root.state), 1U); writer.Zeros(7U);
    WriteRef(writer, root.coverage_tree); WriteRef(writer, root.group_tree); WriteRef(writer, root.journal_inventory); WriteRef(writer, root.id_inventory);
    WriteCursor(writer, root.reference_context, root.reference_replay); writer.Put(root.counter_owner); writer.UInt(root.counter_ceiling, 8U);
    WriteRef(writer, root.replacement_plan); WriteRef(writer, root.previous_root); writer.Zeros(24U);
    Crc(bytes, 60U); Crc(bytes, bytes.size() - 4U); return bytes;
}
CommitOwnerRoot ParseCommitOwnerRoot(Bytes bytes, const CommitOwnerScope& expected,
    std::optional<std::uint64_t> expected_generation) {
    if (bytes.size() != kCommitOwnerRootBytes) Bad("root extent");
    const auto header = InspectCommitOwnerHeader(bytes.first(kCommitOwnerHeaderBytes), expected, CommitOwnerObjectKind::kRoot, expected_generation);
    if (expected_generation && header.generation != *expected_generation) Bad("selected root generation");
    CheckCrc(bytes, bytes.size() - 4U); Reader reader(bytes.subspan(kCommitOwnerHeaderBytes));
    CommitOwnerRoot root; root.scope = header.scope; root.generation = header.generation;
    root.state = static_cast<CommitOwnerState>(reader.UInt(1U)); reader.Zeros(7U);
    root.coverage_tree = ReadRef(reader); root.group_tree = ReadRef(reader); root.journal_inventory = ReadRef(reader); root.id_inventory = ReadRef(reader);
    ReadCursor(reader, root.reference_context, root.reference_replay); root.counter_owner = reader.Array<16U>(); root.counter_ceiling = reader.UInt(8U);
    root.replacement_plan = ReadRef(reader); root.previous_root = ReadRef(reader); reader.Zeros(20U); Root(root); return root;
}
void ValidateCommitOwnerRootSuccessor(const CommitOwnerRoot& previous, const CommitOwnerRoot& next,
    const std::optional<CommitOwnerPlanProof>& plan) {
    Root(previous); Root(next);
    if (previous.scope != next.scope || previous.generation == std::numeric_limits<std::uint64_t>::max() ||
        next.generation != previous.generation + 1U) Bad("root successor context or generation");
    CounterSuccessor(previous, next);
    if (previous.state == CommitOwnerState::kInactive) {
        if (next.state != CommitOwnerState::kActive || plan) Bad("inactive successor");
    } else if (previous.state == CommitOwnerState::kActive && next.state == CommitOwnerState::kActive) {
        if (plan) Bad("unexpected replacement plan");
    } else if (previous.state == CommitOwnerState::kActive && next.state == CommitOwnerState::kPending) {
        if (!plan || previous.generation > std::numeric_limits<std::uint64_t>::max() - 2U) Bad("missing replacement plan");
        Plan(*plan, previous.scope);
        auto selected = next; selected.generation = previous.generation; selected.state = CommitOwnerState::kActive;
        selected.replacement_plan = {}; selected.previous_root = {};
        if (selected != previous || next.previous_root != Object(CommitOwnerObjectKind::kRoot, SerializeCommitOwnerRoot(previous)) ||
            next.previous_root != plan->previous_root || next.replacement_plan != plan->plan ||
            plan->target.generation != previous.generation + 2U) Bad("pending predecessor binding");
        CounterSuccessor(previous, plan->target);
    } else if (previous.state == CommitOwnerState::kPending && next.state == CommitOwnerState::kActive) {
        if (!plan) Bad("missing final plan");
        Plan(*plan, previous.scope);
        auto selected = previous; selected.generation = plan->previous.generation; selected.state = CommitOwnerState::kActive;
        selected.replacement_plan = {}; selected.previous_root = {};
        if (previous.replacement_plan != plan->plan || previous.previous_root != plan->previous_root || next != plan->target)
            Bad("final replacement binding");
        if (selected != plan->previous) Bad("pending old selected context");
    } else Bad("forbidden root transition");
}
void ValidateCommitOwnerSelections(const CommitOwnerRoot& root, const CommitOwnerSelectionProof& checked) {
    Root(root); if (root.state == CommitOwnerState::kInactive) Bad("inactive selections");
    Ref(checked.journal_object); Ref(checked.id_object);
    const auto raw = SerializeCommitJournalInventory(checked.journal);
    if (checked.journal_object != Object(CommitOwnerObjectKind::kJournalInventory, raw) || root.journal_inventory != checked.journal_object ||
        root.id_inventory != checked.id_object || checked.ids.record_bytes != checked.id_object.bytes || checked.ids.generation == 0U ||
        checked.ids.record_bytes < 164U || checked.ids.payload_bytes != checked.ids.record_bytes - 164U ||
        checked.ids.entry_count > checked.ids.payload_bytes / 112U || checked.journal.context.directory_id != root.scope.directory_id)
        Bad("selected inventory binding");
    ValidateCommitJournalPrefix(checked.journal, checked.certified_prefix);
    CommitIdInventory ids{checked.ids.context, checked.ids.generation, checked.ids.replay, {}};
    ValidateCommitIdInventoryCursor(ids, checked.journal, checked.id_cursor);
    CommitIdInventory references{root.reference_context, 1U, root.reference_replay, {}};
    ValidateCommitIdInventoryCursor(references, checked.journal, checked.reference_cursor);
}
void ValidateCommitOwnerCounter(const CommitOwnerRoot& root, const CommitGroupId& counter_owner, std::uint64_t reserved_ceiling) {
    Root(root);
    if (root.counter_owner != counter_owner || reserved_ceiling < root.counter_ceiling ||
        (Zero(counter_owner) && reserved_ceiling != 0U)) Bad("native counter ownership");
}
std::vector<std::uint8_t> SerializeCommitOwnerPage(const CommitOwnerPage& page) {
    std::vector<std::uint8_t> bytes(Page(page)); Writer writer(bytes);
    Header(writer, CommitOwnerObjectKind::kPage, page.scope, page.generation, bytes.size());
    writer.UInt(static_cast<std::uint8_t>(page.tree), 1U); writer.UInt(static_cast<std::uint8_t>(page.node), 1U);
    writer.UInt(Width(page.tree), 1U); writer.UInt(page.prefix.size(), 1U); writer.UInt(page.children.size(), 2U); writer.Zeros(2U);
    writer.UInt(page.subtree_keys, 8U); writer.UInt(page.subtree_bytes, 8U); writer.Put(page.prefix);
    if (page.value) WriteRef(writer, *page.value);
    for (const auto& child : page.children) {
        writer.UInt(child.edge, 1U); writer.Zeros(7U); writer.UInt(child.subtree_keys, 8U); writer.UInt(child.subtree_bytes, 8U); WriteRef(writer, child.page);
    }
    writer.Zeros(4U); Crc(bytes, 60U); Crc(bytes, bytes.size() - 4U); return bytes;
}
CommitOwnerPage ParseCommitOwnerPage(Bytes bytes, const CommitOwnerScope& expected, CommitOwnerTreeKind tree,
    std::uint64_t selecting_generation) {
    if (bytes.size() < kCommitOwnerHeaderBytes || selecting_generation == 0U) Bad("page extent or selecting generation");
    const auto header = InspectCommitOwnerHeader(bytes.first(kCommitOwnerHeaderBytes), expected, CommitOwnerObjectKind::kPage, selecting_generation);
    if (header.bytes != bytes.size()) Bad("complete page extent");
    CheckCrc(bytes, bytes.size() - 4U);
    Reader reader(bytes.subspan(kCommitOwnerHeaderBytes)); CommitOwnerPage page; page.scope = header.scope; page.generation = header.generation;
    page.tree = static_cast<CommitOwnerTreeKind>(reader.UInt(1U)); page.node = static_cast<CommitOwnerNodeKind>(reader.UInt(1U));
    const auto width = reader.UInt(1U), prefix = reader.UInt(1U), count = reader.UInt(2U); reader.Zeros(2U);
    page.subtree_keys = reader.UInt(8U); page.subtree_bytes = reader.UInt(8U);
    if (page.tree != tree || width != Width(tree) || prefix > width || count > 256U) Bad("physical page bounds");
    const auto node = static_cast<unsigned>(page.node);
    if (node > 2U || (page.node != CommitOwnerNodeKind::kBranch && count != 0U)) Bad("physical page shape");
    const auto extent = kCommitOwnerEmptyPageBytes + prefix +
        (page.node == CommitOwnerNodeKind::kLeaf ? kCommitOwnerReferenceBytes : 72U * count);
    if (extent != bytes.size()) Bad("physical page extent");
    const auto key = reader.Take(static_cast<std::size_t>(prefix)); page.prefix.assign(key.begin(), key.end());
    if (page.node == CommitOwnerNodeKind::kLeaf) page.value = ReadRef(reader);
    page.children.reserve(static_cast<std::size_t>(count));
    for (std::uint64_t i = 0U; i < count; ++i) {
        CommitOwnerChild child; child.edge = static_cast<std::uint8_t>(reader.UInt(1U)); reader.Zeros(7U);
        child.subtree_keys = reader.UInt(8U); child.subtree_bytes = reader.UInt(8U); child.page = ReadRef(reader); page.children.push_back(child);
    }
    (void)Page(page); return page;
}
void ValidateCommitOwnerRootPage(const CommitOwnerRoot& root, CommitOwnerTreeKind tree, const CommitOwnerPage& page,
    const CommitOwnerObjectRef& checked_object) {
    Root(root); (void)Page(page);
    if (root.state == CommitOwnerState::kInactive || tree == CommitOwnerTreeKind::kMembers) Bad("root tree selection");
    const auto& selected = tree == CommitOwnerTreeKind::kCoverage ? root.coverage_tree : root.group_tree;
    if (page.scope != root.scope || page.tree != tree || page.generation > root.generation || checked_object != selected ||
        checked_object != Object(CommitOwnerObjectKind::kPage, SerializeCommitOwnerPage(page))) Bad("selected tree binding");
}
void ValidateCommitOwnerChild(const CommitOwnerPage& parent, std::size_t child_index, const CommitOwnerPage& child,
    const CommitOwnerObjectRef& checked_object, std::uint64_t selecting_generation) {
    (void)Page(parent); (void)Page(child);
    if (parent.node != CommitOwnerNodeKind::kBranch || child_index >= parent.children.size()) Bad("parent child index");
    const auto& entry = parent.children[child_index];
    if (child.scope != parent.scope || child.tree != parent.tree || selecting_generation == 0U || parent.generation > selecting_generation ||
        child.generation > selecting_generation || child.node == CommitOwnerNodeKind::kEmpty || checked_object != entry.page ||
        checked_object != Object(CommitOwnerObjectKind::kPage, SerializeCommitOwnerPage(child)) || child.subtree_keys != entry.subtree_keys ||
        child.subtree_bytes != entry.subtree_bytes || child.prefix.size() <= parent.prefix.size() ||
        !std::equal(parent.prefix.begin(), parent.prefix.end(), child.prefix.begin()) || child.prefix[parent.prefix.size()] != entry.edge)
        Bad("child prefix, context or digest binding");
}
std::array<std::uint8_t, 41U> EncodeCommitOwnerCoverageKey(const CommitOwnerCoverageKey& key) {
    Identity(key.table_epoch); if ((key.dims != 2U && key.dims != 3U) || (key.dims == 2U && key.coord.z != 0)) Bad("coverage key arity");
    std::array<std::uint8_t, 41U> bytes{}; std::copy(key.table_epoch.begin(), key.table_epoch.end(), bytes.begin()); bytes[16U] = key.dims;
    const std::array<std::int64_t, 3U> coords{key.coord.x, key.coord.y, key.coord.z};
    for (std::size_t i = 0U; i < coords.size(); ++i)
        BigEndian(std::span<std::uint8_t, 8U>(bytes.data() + 17U + i * 8U, 8U), std::bit_cast<std::uint64_t>(coords[i]) ^ (std::uint64_t{1U} << 63U));
    return bytes;
}
CommitOwnerCoverageKey DecodeCommitOwnerCoverageKey(Bytes bytes) {
    if (bytes.size() != 41U) Bad("coverage key extent");
    Prefix(CommitOwnerTreeKind::kCoverage, bytes);
    CommitOwnerCoverageKey key; std::copy_n(bytes.begin(), 16U, key.table_epoch.begin()); key.dims = bytes[16U];
    std::array<std::int64_t, 3U> coords{};
    for (std::size_t i = 0U; i < coords.size(); ++i)
        coords[i] = std::bit_cast<std::int64_t>(BigEndian(bytes.subspan(17U + i * 8U, 8U)) ^ (std::uint64_t{1U} << 63U));
    key.coord = {coords[0], coords[1], coords[2]}; return key;
}
std::array<std::uint8_t, 16U> EncodeCommitOwnerGroupKey(const CommitGroupId& id) { Identity(id); return id; }
CommitGroupId DecodeCommitOwnerGroupKey(Bytes bytes) {
    if (bytes.size() != 16U) Bad("group key extent");
    CommitGroupId id{}; std::copy(bytes.begin(), bytes.end(), id.begin()); Identity(id); return id;
}
std::array<std::uint8_t, 24U> EncodeCommitOwnerMemberKey(const CommitOwnerMemberKey& key) {
    Identity(key.group_id); std::array<std::uint8_t, 24U> bytes{}; std::copy(key.group_id.begin(), key.group_id.end(), bytes.begin());
    BigEndian(std::span<std::uint8_t, 8U>(bytes.data() + 16U, 8U), key.ordinal); return bytes;
}
CommitOwnerMemberKey DecodeCommitOwnerMemberKey(Bytes bytes) {
    if (bytes.size() != 24U) Bad("member key extent");
    CommitOwnerMemberKey key;
    std::copy_n(bytes.begin(), 16U, key.group_id.begin()); Identity(key.group_id); key.ordinal = BigEndian(bytes.subspan(16U)); return key;
}
std::array<std::uint8_t, kCommitOwnerMemberBytes> SerializeCommitOwnerMember(const CommitOwnerMemberValue& value) {
    MemberValue(value);
    std::array<std::uint8_t, kCommitOwnerMemberBytes> bytes{}; Writer writer(bytes);
    Header(writer, CommitOwnerObjectKind::kMemberValue, value.scope, value.generation, bytes.size());
    writer.Put(value.group_id); writer.Put(value.decision_sha256);
    writer.UInt(value.ordinal, 8U); writer.UInt(value.member_count, 8U); writer.Put(value.coverage.table_epoch);
    writer.UInt(value.coverage.dims, 1U); writer.UInt(static_cast<std::uint8_t>(value.kind), 1U); writer.Zeros(6U);
    for (const auto coordinate : {value.coverage.coord.x, value.coverage.coord.y, value.coverage.coord.z})
        writer.UInt(std::bit_cast<std::uint64_t>(coordinate), 8U);
    writer.UInt(value.revision, 8U); writer.UInt(value.commit_time_ms, 8U); writer.UInt(value.schema_version, 8U);
    Crc(bytes, 60U); Crc(bytes, bytes.size() - 4U); return bytes;
}
CommitOwnerMemberValue ParseCommitOwnerMember(Bytes bytes, const CommitOwnerScope& scope,
    std::uint64_t selecting_generation) {
    if (bytes.size() != kCommitOwnerMemberBytes) Bad("covered member extent");
    const auto header = InspectCommitOwnerHeader(bytes.first(kCommitOwnerHeaderBytes), scope,
        CommitOwnerObjectKind::kMemberValue, selecting_generation);
    CheckCrc(bytes, bytes.size() - 4U); Reader reader(bytes.subspan(kCommitOwnerHeaderBytes));
    CommitOwnerMemberValue value; value.scope = header.scope; value.generation = header.generation;
    value.group_id = reader.Array<16U>(); value.decision_sha256 = reader.Array<32U>();
    value.ordinal = reader.UInt(8U); value.member_count = reader.UInt(8U); value.coverage.table_epoch = reader.Array<16U>();
    value.coverage.dims = static_cast<std::uint8_t>(reader.UInt(1U));
    value.kind = static_cast<CommitGroupMemberKind>(reader.UInt(1U)); reader.Zeros(6U);
    value.coverage.coord.x = std::bit_cast<std::int64_t>(reader.UInt(8U));
    value.coverage.coord.y = std::bit_cast<std::int64_t>(reader.UInt(8U));
    value.coverage.coord.z = std::bit_cast<std::int64_t>(reader.UInt(8U));
    value.revision = reader.UInt(8U); value.commit_time_ms = reader.UInt(8U); value.schema_version = reader.UInt(8U);
    MemberValue(value); return value;
}
void ValidateCommitOwnerMemberBinding(const CommitOwnerMemberValue& value, const CommitOwnerMemberKey& key,
    const CommitOwnerCoverageKey& coverage, const CommitGroupDecision& decision,
    const crypto::Sha256Digest& measured_decision_sha256) {
    MemberValue(value);
    (void)EncodeCommitOwnerMemberKey(key); (void)EncodeCommitOwnerCoverageKey(coverage);
    if (key.group_id != value.group_id || key.ordinal != value.ordinal || coverage != value.coverage)
        Bad("covered member key binding");
    if (decision.directory_id != value.scope.directory_id || decision.id != value.group_id ||
        measured_decision_sha256 != value.decision_sha256 || decision.members.size() != value.member_count)
        Bad("covered member decision binding");
    const auto& member = decision.members[static_cast<std::size_t>(value.ordinal)];
    if (member.table_epoch != value.coverage.table_epoch || member.dims != value.coverage.dims ||
        member.coord != value.coverage.coord || member.kind != value.kind || member.revision != value.revision ||
        member.commit_time_ms != value.commit_time_ms || member.schema_version != value.schema_version)
        Bad("covered member producing binding");
}
}  // namespace chunkdb
