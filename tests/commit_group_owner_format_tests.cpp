#include <algorithm>
#include <cassert>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include "chunkdb/crc32.hpp"
#include "commit_group_owner_format.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
const CommitOwnerScope kScope{{1U}, {2U}, CommitOwnerRole::kCatalog};
void Put(std::span<std::uint8_t> bytes, std::size_t at, std::uint64_t value, unsigned width) {
    for (unsigned i = 0U; i < width; ++i) bytes[at + i] = static_cast<std::uint8_t>(value >> (i * 8U));
}
void Crc(std::span<std::uint8_t> bytes, bool header = false) {
    if (header) Put(bytes, 60U, Crc32(bytes.data(), 60U), 4U);
    if (bytes.size() > 64U) Put(bytes, bytes.size() - 4U, Crc32(bytes.data(), bytes.size() - 4U), 4U);
}
template <typename F> void Reject(F action) {
    bool refused = false;
    try { action(); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}
CommitOwnerObjectRef Ref(CommitOwnerObjectKind kind, std::span<const std::uint8_t> bytes) {
    return {kind, bytes.size(), crypto::Sha256Of(bytes)};
}
CommitOwnerObjectRef Capsule(CommitOwnerObjectKind kind, std::uint8_t digest = 5U) {
    return {kind, 68U, {digest}};
}
CommitOwnerPage Empty(CommitOwnerTreeKind tree) {
    CommitOwnerPage page; page.scope = kScope; page.generation = 2U; page.tree = tree; return page;
}
struct Fixture {
    CommitOwnerRoot inactive;
    CommitOwnerRoot active;
    CommitOwnerPage coverage = Empty(CommitOwnerTreeKind::kCoverage);
    CommitOwnerPage groups = Empty(CommitOwnerTreeKind::kGroups);
    CommitOwnerSelectionProof selected;
    Fixture() {
        inactive.scope = kScope;
        active.scope = kScope; active.generation = 2U; active.state = CommitOwnerState::kActive;
        active.coverage_tree = Ref(CommitOwnerObjectKind::kPage, SerializeCommitOwnerPage(coverage));
        active.group_tree = Ref(CommitOwnerObjectKind::kPage, SerializeCommitOwnerPage(groups));
        selected.journal.context = {kScope.directory_id, {3U}, 1U}; selected.journal.generation = 1U;
        selected.journal.certified.sha256 = crypto::Sha256Of(SerializeCommitJournalHeader(selected.journal.context));
        selected.journal_object = Ref(CommitOwnerObjectKind::kJournalInventory, SerializeCommitJournalInventory(selected.journal));
        selected.certified_prefix = {selected.journal.context, selected.journal.certified, 64U};
        CommitIdInventory ids{selected.journal.context, 1U, selected.journal.certified, {}};
        const auto encoded = SerializeCommitIdInventory(ids);
        selected.ids = InspectCommitIdInventoryHeader(std::span(encoded).first(kCommitIdInventoryHeaderBytes), ids.context);
        selected.id_object = Ref(CommitOwnerObjectKind::kIdInventory, encoded);
        selected.id_cursor = selected.certified_prefix; selected.reference_cursor = selected.certified_prefix;
        active.journal_inventory = selected.journal_object; active.id_inventory = selected.id_object;
        active.reference_context = selected.journal.context; active.reference_replay = selected.journal.certified;
    }
};
void ScopeAndGoldenRoot() {
    const auto scope = SerializeCommitOwnerScope(kScope);
    std::array<std::uint8_t, 40U> expected_scope{}; expected_scope[0U] = 1U; expected_scope[16U] = 2U; expected_scope[32U] = 2U;
    assert(scope == expected_scope && ParseCommitOwnerScope(scope) == kScope);
    auto unknown_role = scope; unknown_role[32U] = 3U;
    Reject([&] { (void)ParseCommitOwnerScope(unknown_role); });
    ValidateCommitOwnerStoreScope(kScope, {8U}, kScope);
    Reject([&] { ValidateCommitOwnerStoreScope(kScope, {8U}); });
    auto foreign = kScope; foreign.owner_id[0U] = 7U;
    Reject([&] { ValidateCommitOwnerStoreScope(kScope, {8U}, foreign); });
    auto missing = kScope; missing.directory_id = {};
    Reject([&] { (void)SerializeCommitOwnerScope(missing); });
    auto standalone = kScope; standalone.role = CommitOwnerRole::kStandalone;
    ValidateCommitOwnerStoreScope(standalone, standalone.directory_id);
    Reject([&] { ValidateCommitOwnerStoreScope(standalone, {8U}); });
    Reject([&] { ValidateCommitOwnerStoreScope(standalone, standalone.directory_id, standalone); });
    Fixture fixture;
    const auto bytes = SerializeCommitOwnerRoot(fixture.inactive);
    std::array<std::uint8_t, 512U> golden{};
    golden[0U] = 'C'; golden[1U] = 'K'; golden[2U] = 'G'; golden[3U] = 'O'; golden[4U] = 1U;
    golden[6U] = 1U; golden[7U] = 2U; golden[9U] = 2U; golden[16U] = 1U; golden[32U] = 2U; golden[48U] = 1U;
    Crc(golden, true);
    assert(bytes == golden && ParseCommitOwnerRoot(bytes, kScope, 1U) == fixture.inactive);
    assert(ParseCommitOwnerRoot(SerializeCommitOwnerRoot(fixture.active), kScope, 2U) == fixture.active);
    for (std::size_t cut = 0U; cut < bytes.size(); ++cut)
        Reject([&] { (void)ParseCommitOwnerRoot(std::span(bytes).first(cut), kScope); });
    for (std::size_t cut = 0U; cut < scope.size(); ++cut)
        Reject([&] { (void)ParseCommitOwnerScope(std::span(scope).first(cut)); });
    auto invalid = fixture.inactive; invalid.coverage_tree = fixture.active.coverage_tree;
    Reject([&] { (void)SerializeCommitOwnerRoot(invalid); });
    auto damaged = bytes; damaged[65U] = 1U; Crc(damaged);
    Reject([&] { (void)ParseCommitOwnerRoot(damaged, kScope); });
    damaged = bytes; damaged.back() ^= 1U;
    Reject([&] { (void)ParseCommitOwnerRoot(damaged, kScope); });
    damaged = bytes; damaged[4U] = 2U; Crc(damaged, true);
    Reject([&] { (void)ParseCommitOwnerRoot(damaged, kScope); });
    Reject([&] { (void)ParseCommitOwnerRoot(bytes, foreign); });
    Reject([&] { (void)ParseCommitOwnerRoot(bytes, kScope, 2U); });
}
void RootTransitionsAndCounter() {
    Fixture f; ValidateCommitOwnerRootSuccessor(f.inactive, f.active);
    auto next = f.active; ++next.generation; ValidateCommitOwnerRootSuccessor(f.active, next);
    auto with_counter = f.active; with_counter.counter_owner = {9U};
    ValidateCommitOwnerCounter(with_counter, {9U}, 100U);
    Reject([&] { ValidateCommitOwnerCounter(with_counter, {8U}, 100U); });
    auto cleared = with_counter; ++cleared.generation; cleared.counter_owner = {};
    Reject([&] { ValidateCommitOwnerRootSuccessor(with_counter, cleared); });
    with_counter.counter_ceiling = 100U; next = with_counter; ++next.generation; next.counter_ceiling = 99U;
    Reject([&] { ValidateCommitOwnerRootSuccessor(with_counter, next); });
    Reject([&] { ValidateCommitOwnerCounter(with_counter, {9U}, 99U); });
    next = f.inactive; next.generation = 3U;
    Reject([&] { ValidateCommitOwnerRootSuccessor(f.active, next); });
    next = f.active; next.generation = std::numeric_limits<std::uint64_t>::max();
    auto wrapped = next; wrapped.generation = 1U;
    Reject([&] { ValidateCommitOwnerRootSuccessor(next, wrapped); });
    CommitOwnerPlanProof plan; plan.scope = kScope; plan.plan = Capsule(CommitOwnerObjectKind::kReplacementPlan);
    plan.previous = f.active;
    plan.previous_root = Ref(CommitOwnerObjectKind::kRoot, SerializeCommitOwnerRoot(f.active));
    plan.target = f.active; plan.target.generation = 4U;
    plan.final_root = Ref(CommitOwnerObjectKind::kRoot, SerializeCommitOwnerRoot(plan.target));
    auto pending = f.active; pending.generation = 3U; pending.state = CommitOwnerState::kPending;
    pending.replacement_plan = plan.plan; pending.previous_root = plan.previous_root;
    ValidateCommitOwnerRootSuccessor(f.active, pending, plan);
    ValidateCommitOwnerRootSuccessor(pending, plan.target, plan);
    assert(ParseCommitOwnerRoot(SerializeCommitOwnerRoot(pending), kScope) == pending);
    Reject([&] { ValidateCommitOwnerRootSuccessor(f.active, pending); });
    Reject([&] { ValidateCommitOwnerRootSuccessor(pending, plan.target); });
    auto inconsistent = pending; auto other_old_page = f.coverage; other_old_page.generation = 1U;
    inconsistent.coverage_tree = Ref(CommitOwnerObjectKind::kPage, SerializeCommitOwnerPage(other_old_page));
    assert(ParseCommitOwnerRoot(SerializeCommitOwnerRoot(inconsistent), kScope) == inconsistent);
    Reject([&] { ValidateCommitOwnerRootSuccessor(inconsistent, plan.target, plan); });
    auto bad = pending; bad.previous_root.sha256[0U] ^= 1U;
    Reject([&] { ValidateCommitOwnerRootSuccessor(f.active, bad, plan); });
    auto other = plan; other.target.counter_owner = {7U};
    assert(SerializeCommitOwnerRoot(other.target).size() == SerializeCommitOwnerRoot(plan.target).size());
    Reject([&] { ValidateCommitOwnerRootSuccessor(pending, other.target, other); });
    bad = pending; ++bad.generation;
    Reject([&] { ValidateCommitOwnerRootSuccessor(pending, bad, plan); });
}
void InventoryAndTreeSelections() {
    Fixture f; ValidateCommitOwnerSelections(f.active, f.selected);
    ValidateCommitOwnerRootPage(f.active, CommitOwnerTreeKind::kCoverage, f.coverage, f.active.coverage_tree);
    ValidateCommitOwnerRootPage(f.active, CommitOwnerTreeKind::kGroups, f.groups, f.active.group_tree);
    auto wrong_field = f.active; wrong_field.coverage_tree = f.active.group_tree;
    Reject([&] { ValidateCommitOwnerRootPage(wrong_field, CommitOwnerTreeKind::kCoverage, f.groups, f.active.group_tree); });
    auto wrong_digest = f.active; wrong_digest.coverage_tree.sha256[0U] ^= 1U;
    Reject([&] { ValidateCommitOwnerRootPage(wrong_digest, CommitOwnerTreeKind::kCoverage, f.coverage, wrong_digest.coverage_tree); });
    auto selected = f.selected; selected.reference_cursor.last_complete_record_end = 65U;
    Reject([&] { ValidateCommitOwnerSelections(f.active, selected); });
    selected = f.selected; ++selected.ids.context.generation;
    Reject([&] { ValidateCommitOwnerSelections(f.active, selected); });
    selected = f.selected; selected.id_object.sha256[0U] ^= 1U;
    Reject([&] { ValidateCommitOwnerSelections(f.active, selected); });
    selected = f.selected; selected.certified_prefix.cut.sha256[0U] ^= 1U;
    Reject([&] { ValidateCommitOwnerSelections(f.active, selected); });
    auto root = f.active; root.id_inventory.bytes = std::numeric_limits<std::uint64_t>::max();
    Reject([&] { (void)SerializeCommitOwnerRoot(root); });
    root = f.active; root.coverage_tree.bytes = kMaxCommitOwnerPageBytes + 1U;
    Reject([&] { (void)SerializeCommitOwnerRoot(root); });
    root = f.active; root.journal_inventory.bytes = 129U;
    Reject([&] { (void)SerializeCommitOwnerRoot(root); });
}
CommitOwnerPage Leaf(std::uint8_t id) {
    auto leaf = Empty(CommitOwnerTreeKind::kGroups); leaf.node = CommitOwnerNodeKind::kLeaf;
    const auto key = EncodeCommitOwnerGroupKey({id}); leaf.prefix.assign(key.begin(), key.end());
    leaf.value = Capsule(CommitOwnerObjectKind::kGroupValue); leaf.subtree_keys = 1U;
    leaf.subtree_bytes = 92U + leaf.prefix.size() + 48U + leaf.value->bytes; return leaf;
}
CommitOwnerPage Branch(const CommitOwnerPage& a, const CommitOwnerPage& b) {
    auto page = Empty(CommitOwnerTreeKind::kGroups); page.node = CommitOwnerNodeKind::kBranch;
    for (const auto* child : {&a, &b})
        page.children.push_back({child->prefix[0U], child->subtree_keys, child->subtree_bytes,
            Ref(CommitOwnerObjectKind::kPage, SerializeCommitOwnerPage(*child))});
    page.subtree_keys = 2U; page.subtree_bytes = 92U + 144U + a.subtree_bytes + b.subtree_bytes; return page;
}
void PageGoldenAndBindings() {
    const auto empty = Empty(CommitOwnerTreeKind::kGroups);
    const auto encoded_empty = SerializeCommitOwnerPage(empty);
    assert(encoded_empty.size() == 92U && encoded_empty[64U] == 2U && encoded_empty[66U] == 16U && encoded_empty[80U] == 92U);
    assert(ParseCommitOwnerPage(encoded_empty, kScope, CommitOwnerTreeKind::kGroups, 2U) == empty);
    const auto a = Leaf(10U), b = Leaf(11U); const auto parent = Branch(a, b);
    const auto raw = SerializeCommitOwnerPage(parent);
    assert(raw.size() == 236U && raw[68U] == 2U);
    assert(ParseCommitOwnerPage(raw, kScope, CommitOwnerTreeKind::kGroups, 2U) == parent);
    ValidateCommitOwnerChild(parent, 0U, a, parent.children[0U].page, 2U);
    ValidateCommitOwnerChild(parent, 1U, b, parent.children[1U].page, 2U);
    for (std::size_t cut = 0U; cut < raw.size(); ++cut)
        Reject([&] { (void)ParseCommitOwnerPage(std::span(raw).first(cut), kScope, CommitOwnerTreeKind::kGroups, 2U); });
    auto changed = a; changed.prefix[0U] = 12U;
    auto rebound = parent; rebound.children[0U].page = Ref(CommitOwnerObjectKind::kPage, SerializeCommitOwnerPage(changed));
    Reject([&] { ValidateCommitOwnerChild(rebound, 0U, changed, rebound.children[0U].page, 2U); });
    changed = a; changed.generation = 3U;
    Reject([&] { (void)ParseCommitOwnerPage(SerializeCommitOwnerPage(changed), kScope, changed.tree, 2U); });
    rebound = parent; rebound.children[0U].page = Ref(CommitOwnerObjectKind::kPage, SerializeCommitOwnerPage(changed));
    Reject([&] { ValidateCommitOwnerChild(rebound, 0U, changed, rebound.children[0U].page, 2U); });
    rebound = parent; ++rebound.children[0U].subtree_keys; ++rebound.subtree_keys;
    Reject([&] { ValidateCommitOwnerChild(rebound, 0U, a, rebound.children[0U].page, 2U); });
    rebound = parent; ++rebound.children[0U].subtree_bytes; ++rebound.subtree_bytes;
    Reject([&] { ValidateCommitOwnerChild(rebound, 0U, a, rebound.children[0U].page, 2U); });
    rebound = parent; rebound.children[0U].page.sha256[0U] ^= 1U;
    Reject([&] { ValidateCommitOwnerChild(rebound, 0U, a, rebound.children[0U].page, 2U); });
    auto wrong = parent; wrong.children[1U].edge = wrong.children[0U].edge;
    Reject([&] { (void)SerializeCommitOwnerPage(wrong); });
    wrong = parent; ++wrong.subtree_bytes;
    Reject([&] { (void)SerializeCommitOwnerPage(wrong); });
    wrong = parent; wrong.children[0U].subtree_keys = std::numeric_limits<std::uint64_t>::max();
    Reject([&] { (void)SerializeCommitOwnerPage(wrong); });
    wrong = a; wrong.value->kind = CommitOwnerObjectKind::kCoverageValue;
    Reject([&] { (void)SerializeCommitOwnerPage(wrong); });
    wrong = parent; wrong.tree = CommitOwnerTreeKind::kMembers;
    for (auto& child : wrong.children) child.subtree_bytes = kMaxCommitCoveredTreeBytes / 2U;
    wrong.subtree_bytes = 236U + kMaxCommitCoveredTreeBytes;
    Reject([&] { (void)SerializeCommitOwnerPage(wrong); });
    auto malformed = raw; Put(malformed, 68U, 65535U, 2U); Crc(malformed);
    Reject([&] { (void)ParseCommitOwnerPage(malformed, kScope, CommitOwnerTreeKind::kGroups, 2U); });
    malformed = raw; malformed[89U] = 1U; Crc(malformed);
    Reject([&] { (void)ParseCommitOwnerPage(malformed, kScope, CommitOwnerTreeKind::kGroups, 2U); });
}
void OrderedKeysAndBounds() {
    for (const auto dims : {std::uint8_t{2U}, std::uint8_t{3U}}) {
        const CommitOwnerCoverageKey key{{9U}, dims, {-7, std::numeric_limits<std::int64_t>::max(), dims == 3U ? -2 : 0}};
        const auto bytes = EncodeCommitOwnerCoverageKey(key);
        assert(DecodeCommitOwnerCoverageKey(bytes) == key && bytes[17U] == 0x7fU && bytes[24U] == 0xf9U);
        auto leaf = Empty(CommitOwnerTreeKind::kCoverage); leaf.node = CommitOwnerNodeKind::kLeaf;
        leaf.prefix.assign(bytes.begin(), bytes.end()); leaf.value = Capsule(CommitOwnerObjectKind::kCoverageValue);
        leaf.subtree_keys = 1U; leaf.subtree_bytes = 92U + 41U + 48U + 68U;
        assert(ParseCommitOwnerPage(SerializeCommitOwnerPage(leaf), kScope, leaf.tree, 2U) == leaf);
    }
    const CommitOwnerCoverageKey low{{9U}, 3U, {std::numeric_limits<std::int64_t>::min(), 0, 0}};
    auto high = low; high.coord.x = std::numeric_limits<std::int64_t>::max();
    assert(EncodeCommitOwnerCoverageKey(low) < EncodeCommitOwnerCoverageKey(high));
    high.dims = 2U; high.coord.z = 1;
    Reject([&] { (void)EncodeCommitOwnerCoverageKey(high); });
    high.coord.z = 0;
    auto inactive_depth = EncodeCommitOwnerCoverageKey(high); inactive_depth.back() = 1U;
    Reject([&] { (void)DecodeCommitOwnerCoverageKey(inactive_depth); });
    inactive_depth = EncodeCommitOwnerCoverageKey(high); inactive_depth[16U] = 4U;
    Reject([&] { (void)DecodeCommitOwnerCoverageKey(inactive_depth); });
    const std::array<std::uint8_t, 16U> zero_group{};
    Reject([&] { (void)DecodeCommitOwnerGroupKey(zero_group); });
    const CommitOwnerMemberKey member{{7U}, std::numeric_limits<std::uint64_t>::max()};
    assert(DecodeCommitOwnerMemberKey(EncodeCommitOwnerMemberKey(member)) == member);
    Fixture f; auto oversized = SerializeCommitOwnerRoot(f.active);
    Put(oversized, 8U, std::uint64_t{1U} << 40U, 8U); Crc(oversized, true);
    bool caller_requested_buffer = false;
    Reject([&] {
        const auto header = InspectCommitOwnerHeader(std::span(oversized).first(64U), kScope, CommitOwnerObjectKind::kRoot);
        caller_requested_buffer = header.bytes != 0U;
    });
    assert(!caller_requested_buffer);
    auto raw = SerializeCommitOwnerPage(Empty(CommitOwnerTreeKind::kMembers));
    Put(raw, 8U, 65537U, 8U); Crc(raw, true);
    Reject([&] { (void)InspectCommitOwnerHeader(std::span(raw).first(64U), kScope, CommitOwnerObjectKind::kPage); });
}
struct MemberFixture {
    CommitOwnerMemberValue value;
    CommitGroupDecision decision;
    MemberFixture() {
        value.scope = kScope; value.generation = 2U; value.group_id = {3U}; value.ordinal = 1U; value.member_count = 2U;
        value.coverage = {{4U}, 2U, {-7, std::numeric_limits<std::int64_t>::max(), 0}};
        value.revision = 10U; value.commit_time_ms = 11U; value.schema_version = 2U;
        CommitGroupMember member;
        member.table_epoch = value.coverage.table_epoch; member.dims = 2U; member.coord = value.coverage.coord;
        member.revision = value.revision; member.commit_time_ms = value.commit_time_ms; member.schema_version = value.schema_version;
        member.frame_offset = 64U; member.frame_bytes = 80U; member.frame_sha256 = {5U};
        auto first = member; --first.coord.x;
        decision.directory_id = kScope.directory_id; decision.id = value.group_id;
        decision.members = {first, member}; decision.versions = {{member.table_epoch, member.revision, "world"}};
        decision.durability = CommitGroupDurability::kSynced; decision.created_ms = 12U;
        value.decision_sha256 = crypto::Sha256Of(SerializeCommitGroupDecision(decision));
    }
    void Bind(const CommitOwnerMemberValue& candidate) const {
        ValidateCommitOwnerMemberBinding(candidate, {value.group_id, value.ordinal}, value.coverage,
            decision, value.decision_sha256);
    }
};
void MemberCapsules() {
    MemberFixture fixture;
    const auto& value = fixture.value;
    const auto raw = SerializeCommitOwnerMember(value);
    assert(raw.size() == 204U && ParseCommitOwnerMember(raw, kScope, 2U) == value);
    std::array<std::uint8_t, 204U> golden{};
    golden[0U] = 'C'; golden[1U] = 'K'; golden[2U] = 'G'; golden[3U] = 'O'; golden[4U] = 1U;
    golden[6U] = 5U; golden[7U] = 2U; golden[8U] = 204U; golden[16U] = 1U; golden[32U] = 2U; golden[48U] = 2U;
    golden[64U] = 3U; std::copy(value.decision_sha256.begin(), value.decision_sha256.end(), golden.begin() + 80);
    golden[112U] = 1U; golden[120U] = 2U; golden[128U] = 4U; golden[144U] = 2U; golden[145U] = 1U;
    Put(golden, 152U, std::numeric_limits<std::uint64_t>::max() - 6U, 8U);
    Put(golden, 160U, static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()), 8U);
    golden[176U] = 10U; golden[184U] = 11U; golden[192U] = 2U; Crc(golden, true);
    assert(raw == golden);
    fixture.Bind(value);
    for (std::size_t cut = 0U; cut < raw.size(); ++cut)
        Reject([&] { (void)ParseCommitOwnerMember(std::span(raw).first(cut), kScope, 2U); });
    auto extended = Bytes(raw.begin(), raw.end()); extended.push_back(0U);
    Reject([&] { (void)ParseCommitOwnerMember(extended, kScope, 2U); });
    Reject([&] { (void)ParseCommitOwnerMember(raw, kScope, 1U); });
    Reject([&] { (void)ParseCommitOwnerMember(raw, kScope, 0U); });
    auto foreign_scope = kScope; ++foreign_scope.owner_id[0U];
    Reject([&] { (void)ParseCommitOwnerMember(raw, foreign_scope, 2U); });
    auto corrupt = raw; corrupt[80U] ^= 1U;
    Reject([&] { (void)ParseCommitOwnerMember(corrupt, kScope, 2U); });
    for (const auto& [offset, changed] : {std::pair<std::size_t, std::uint8_t>{144U, 4U}, {145U, 3U}, {146U, 1U}, {168U, 1U}}) {
        corrupt = raw; corrupt[offset] = changed; Crc(corrupt);
        Reject([&] { (void)ParseCommitOwnerMember(corrupt, kScope, 2U); });
    }
    corrupt = raw; Put(corrupt, 112U, 2U, 8U); Crc(corrupt);
    Reject([&] { (void)ParseCommitOwnerMember(corrupt, kScope, 2U); });
    auto missing = value; missing.group_id = {};
    Reject([&] { (void)SerializeCommitOwnerMember(missing); });
    missing = value; missing.decision_sha256 = {};
    Reject([&] { (void)SerializeCommitOwnerMember(missing); });
    missing = value; missing.member_count = 0U;
    Reject([&] { (void)SerializeCommitOwnerMember(missing); });
    missing = value; missing.revision = 0U;
    Reject([&] { (void)SerializeCommitOwnerMember(missing); });
    missing = value; missing.schema_version = 0U;
    Reject([&] { (void)SerializeCommitOwnerMember(missing); });
    auto observed = value; observed.kind = CommitGroupMemberKind::kObserved;
    observed.revision = 0U; observed.commit_time_ms = 0U; observed.schema_version = 0U;
    assert(ParseCommitOwnerMember(SerializeCommitOwnerMember(observed), kScope, 3U) == observed);
    observed.commit_time_ms = 1U;
    Reject([&] { (void)SerializeCommitOwnerMember(observed); });
    observed.commit_time_ms = 0U; observed.schema_version = 1U;
    Reject([&] { (void)SerializeCommitOwnerMember(observed); });
    auto spatial = value; spatial.coverage.dims = 3U; spatial.coverage.coord.z = std::numeric_limits<std::int64_t>::min();
    assert(ParseCommitOwnerMember(SerializeCommitOwnerMember(spatial), kScope, 2U) == spatial);
    corrupt = raw; Put(corrupt, 8U, std::uint64_t{1U} << 40U, 8U); Crc(corrupt, true);
    bool requested_buffer = false;
    Reject([&] {
        (void)InspectCommitOwnerHeader(std::span(corrupt).first(64U), kScope, CommitOwnerObjectKind::kMemberValue, 2U);
        requested_buffer = true;
    });
    assert(!requested_buffer);
    auto leaf = Empty(CommitOwnerTreeKind::kMembers); leaf.node = CommitOwnerNodeKind::kLeaf;
    const auto key = EncodeCommitOwnerMemberKey({value.group_id, value.ordinal}); leaf.prefix.assign(key.begin(), key.end());
    leaf.value = Ref(CommitOwnerObjectKind::kMemberValue, raw); leaf.subtree_keys = 1U; leaf.subtree_bytes = 92U + 24U + 48U + raw.size();
    assert(ParseCommitOwnerPage(SerializeCommitOwnerPage(leaf), kScope, CommitOwnerTreeKind::kMembers, 2U) == leaf);
    --leaf.value->bytes; --leaf.subtree_bytes;
    Reject([&] { (void)SerializeCommitOwnerPage(leaf); });
}
void MemberBindings() {
    MemberFixture fixture;
    auto wrong_key = CommitOwnerMemberKey{fixture.value.group_id, fixture.value.ordinal}; ++wrong_key.group_id[0U];
    Reject([&] { ValidateCommitOwnerMemberBinding(fixture.value, wrong_key, fixture.value.coverage,
        fixture.decision, fixture.value.decision_sha256); });
    wrong_key = {fixture.value.group_id, 0U};
    Reject([&] { ValidateCommitOwnerMemberBinding(fixture.value, wrong_key, fixture.value.coverage,
        fixture.decision, fixture.value.decision_sha256); });
    for (unsigned field = 0U; field < 3U; ++field) {
        auto wrong_coverage = fixture.value.coverage;
        if (field == 0U) ++wrong_coverage.coord.x;
        else if (field == 1U) ++wrong_coverage.table_epoch[0U];
        else wrong_coverage.dims = 3U;
        Reject([&] { ValidateCommitOwnerMemberBinding(fixture.value, {fixture.value.group_id, fixture.value.ordinal},
            wrong_coverage, fixture.decision, fixture.value.decision_sha256); });
    }
    auto altered = fixture.value; ++altered.group_id[0U]; Reject([&] { fixture.Bind(altered); });
    altered = fixture.value; altered.ordinal = 0U; Reject([&] { fixture.Bind(altered); });
    altered = fixture.value; ++altered.coverage.coord.x; Reject([&] { fixture.Bind(altered); });
    altered = fixture.value; ++altered.decision_sha256[0U]; Reject([&] { fixture.Bind(altered); });
    altered = fixture.value; ++altered.revision; Reject([&] { fixture.Bind(altered); });
    altered = fixture.value; ++altered.commit_time_ms; Reject([&] { fixture.Bind(altered); });
    altered = fixture.value; ++altered.schema_version; Reject([&] { fixture.Bind(altered); });
    auto other = fixture.decision; ++other.directory_id[0U];
    Reject([&] { ValidateCommitOwnerMemberBinding(fixture.value, {fixture.value.group_id, 1U}, fixture.value.coverage,
        other, fixture.value.decision_sha256); });
    other = fixture.decision; ++other.id[0U];
    Reject([&] { ValidateCommitOwnerMemberBinding(fixture.value, {fixture.value.group_id, 1U}, fixture.value.coverage,
        other, fixture.value.decision_sha256); });
    // A different fully valid decision can retain this ordinal with another member count.
    // Digest and key checks alone cannot bind its complete set.
    other = fixture.decision; auto extra = other.members.back(); ++extra.coord.x; other.members.push_back(extra);
    altered = fixture.value; altered.decision_sha256 = crypto::Sha256Of(SerializeCommitGroupDecision(other));
    Reject([&] { ValidateCommitOwnerMemberBinding(altered, {altered.group_id, 1U}, altered.coverage,
        other, altered.decision_sha256); });
    for (unsigned field = 0U; field < 7U; ++field) {
        other = fixture.decision; auto& member = other.members[1U];
        switch (field) {
            case 0U: ++member.table_epoch[0U]; break;
            case 1U: member.dims = 3U; break;
            case 2U: ++member.coord.x; break;
            case 3U: member.kind = CommitGroupMemberKind::kObserved; break;
            case 4U: ++member.revision; break;
            case 5U: ++member.commit_time_ms; break;
            case 6U: ++member.schema_version; break;
        }
        // The binding also rejects inconsistent caller facts supplied with the same measured digest.
        Reject([&] { ValidateCommitOwnerMemberBinding(fixture.value, {fixture.value.group_id, 1U}, fixture.value.coverage,
            other, fixture.value.decision_sha256); });
    }
}
}
int main() {
    ScopeAndGoldenRoot(); RootTransitionsAndCounter(); InventoryAndTreeSelections(); PageGoldenAndBindings(); OrderedKeysAndBounds();
    MemberCapsules(); MemberBindings();
}
