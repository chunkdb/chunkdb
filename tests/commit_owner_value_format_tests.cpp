#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <source_location>
#include <sstream>
#include <stdexcept>
#include <string>

#include "chunkdb/crc32.hpp"
#include "commit_owner_value_format.hpp"
#include "populated_index_format.hpp"
#include "owner_artifact_allocation_guard.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
const CommitOwnerScope kScope{{1U}, {2U}, CommitOwnerRole::kCatalog};
void Put(std::span<std::uint8_t> bytes, std::size_t at, std::uint64_t value, unsigned width) {
    for (unsigned i = 0U; i < width; ++i) bytes[at + i] = static_cast<std::uint8_t>(value >> (i * 8U));
}
void Crc(std::span<std::uint8_t> bytes, bool header = false) {
    if (header) Put(bytes, 60U, Crc32(bytes.data(), 60U), 4U);
    Put(bytes, bytes.size() - 4U, Crc32(bytes.data(), bytes.size() - 4U), 4U);
}
template <class Call> void Reject(Call call, const std::source_location& where = std::source_location::current()) {
    bool refused = false;
    try { call(); } catch (const std::invalid_argument&) { refused = true; }
    if (!refused) std::fprintf(stderr, "missing refusal at %s:%u\n", where.file_name(), where.line());
    assert(refused);
}
CommitOwnerObjectRef Ref(CommitOwnerObjectKind kind, std::span<const std::uint8_t> bytes) {
    return {kind, bytes.size(), crypto::Sha256Of(bytes)};
}
CommitOwnerSet Empty(CommitOwnerTreeKind tree) {
    CommitOwnerPage page; page.scope = kScope; page.tree = tree;
    return {Ref(CommitOwnerObjectKind::kPage, SerializeCommitOwnerPage(page)), 0U, 92U, CommitOwnerSetHasher(tree).Finish()};
}
CommitOwnerSet One(CommitOwnerTreeKind tree, std::span<const std::uint8_t> key, const CommitOwnerObjectRef& value) {
    CommitOwnerPage page; page.scope = kScope; page.tree = tree; page.node = CommitOwnerNodeKind::kLeaf;
    page.prefix.assign(key.begin(), key.end()); page.value = value; page.subtree_keys = 1U;
    page.subtree_bytes = 92U + key.size() + 48U + value.bytes;
    CommitOwnerSetHasher digest(tree); digest.Add(key, value);
    return {Ref(CommitOwnerObjectKind::kPage, SerializeCommitOwnerPage(page)), 1U, page.subtree_bytes, digest.Finish()};
}
CommitOwnerCoverageKey Key(bool three = false) { return {{3U}, static_cast<std::uint8_t>(three ? 3U : 2U), {-4, 7, three ? -2 : 0}}; }
Geometry Shape(bool three = false) { return Geometry({1U, 1U, 2U, 2U, 8U, static_cast<std::uint8_t>(three ? 3U : 2U), three ? 2U : 1U, 1U}); }
CommitOwnerMemberValue Member(CommitOwnerCoverageKey key, CommitGroupMemberKind kind = CommitGroupMemberKind::kChanged) {
    return {kScope, 1U, {4U}, {5U}, 0U, 1U, key, kind, 7U, 11U, 1U};
}
CommitOwnerRetainedProof Retained(CommitOwnerCoverageKey key, std::uint64_t floor = 0U) {
    const auto geometry = Shape(key.dims == 3U); const auto large = geometry.ChunkToLarge(key.coord);
    const auto header = SerializeChunkIndexHeader({key.table_epoch, key.dims, large});
    const auto record = SerializeChunkIndexRecord({ChunkIndexKind::kDeleted, key.coord, 7U, 11U, 1U}, geometry, large);
    const IndexInventoryHeader inventory{key.table_epoch, key.dims, false, floor, 0U, 1U, 1U};
    const std::array entry{IndexInventoryEntry{large, header.size() + record.size(), 0U, {}}};
    std::ostringstream output(std::ios::binary); WriteIndexInventory(output, inventory, entry);
    const auto raw = output.str(); const auto bytes = std::span(reinterpret_cast<const std::uint8_t*>(raw.data()), raw.size());
    CommitOwnerRetainedProof proof; proof.inventory_bytes = raw.size(); proof.record_offset = header.size(); proof.inventory_sha256 = crypto::Sha256Of(bytes);
    std::copy_n(bytes.begin(), 60U, proof.inventory_header.begin());
    std::copy_n(bytes.begin() + 60, 48U + 8U * key.dims, proof.inventory_entry.begin());
    std::copy(header.begin(), header.end(), proof.log_header.begin()); std::copy(record.begin(), record.end(), proof.record.begin()); return proof;
}
CommitOwnerCoverageValue Coverage(CommitOwnerCoverageKey key) {
    CommitOwnerCoverageValue value; value.scope = kScope; value.key = key; value.covered = Empty(CommitOwnerTreeKind::kMembers); return value;
}
CommitOwnerGroupValue Group() {
    CommitOwnerGroupValue value; value.scope = kScope; value.generation = 2U; value.group_id = {4U}; value.decision_sha256 = {5U};
    value.source = CommitOwnerJournalRecord{{kScope.directory_id, {6U}, 1U}, CommitRecordKind::kDecision, 64U, 156U, {5U}};
    value.members = Empty(CommitOwnerTreeKind::kMembers); value.owners = Empty(CommitOwnerTreeKind::kOwnerEdges);
    value.observations = Empty(CommitOwnerTreeKind::kObservations); return value;
}
template <class Parse> void EnvelopeRefusals(const Bytes& raw, Parse parse) {
    for (std::size_t cut = 0U; cut < raw.size(); ++cut) Reject([&] { parse(std::span(raw).first(cut)); });
    auto damaged = raw; damaged.push_back(0U); Reject([&] { parse(damaged); });
    for (const auto at : {4U, 6U, 7U, 16U, 32U, 56U}) {
        damaged = raw; damaged[at] ^= 0x80U; Crc(damaged, true); Reject([&] { parse(damaged); });
    }
    damaged = raw; Put(damaged, 48U, 0U, 8U); Crc(damaged, true); Reject([&] { parse(damaged); });
    damaged = raw; Put(damaged, 48U, 9U, 8U); Crc(damaged, true); Reject([&] { parse(damaged); });
    damaged = raw; damaged.back() ^= 1U; Reject([&] { parse(damaged); });
}

void CoverageAndOwnedProofs() {
    for (const bool three : {false, true}) {
        const auto key = Key(three); auto value = Coverage(key);
        auto raw = SerializeCommitOwnerCoverage(value); assert(raw.size() == 240U);
        const auto parse = [&](std::span<const std::uint8_t> bytes) { return ParseCommitOwnerCoverage(bytes, kScope, 2U, key); };
        assert(parse(raw) == value); EnvelopeRefusals(raw, parse);
        Bytes golden(240U, 0U); golden[0U] = 'C'; golden[1U] = 'K'; golden[2U] = 'G'; golden[3U] = 'O';
        golden[4U] = 1U; golden[6U] = 3U; golden[7U] = 2U; Put(golden, 8U, 240U, 8U);
        golden[16U] = 1U; golden[32U] = 2U; golden[48U] = 1U;
        const auto encoded = EncodeCommitOwnerCoverageKey(key); std::copy(encoded.begin(), encoded.end(), golden.begin() + 64);
        golden[139U] = 2U; Put(golden, 147U, 92U, 8U);
        std::copy(value.covered.root.sha256.begin(), value.covered.root.sha256.end(), golden.begin() + 155);
        Put(golden, 195U, 92U, 8U); std::copy(value.covered.sha256.begin(), value.covered.sha256.end(), golden.begin() + 203);
        Crc(golden, true); assert(raw == golden);
        auto wrong_key = key; ++wrong_key.coord.x; Reject([&] { (void)ParseCommitOwnerCoverage(raw, kScope, 2U, wrong_key); });
        auto invalid = value; invalid.identity.revision = 1U; Reject([&] { (void)SerializeCommitOwnerCoverage(invalid); });
        value.state = CommitOwnerCoverageState::kImage; value.identity = {7U, 11U, 1U};
        value.image = CommitOwnerNativeRef{CommitOwnerNativeKind::kImage, 1024U, {9U}};
        value.producer = Member(key); raw = SerializeCommitOwnerCoverage(value); assert(raw.size() == 492U && parse(raw) == value);
        invalid = value; invalid.producer->kind = CommitGroupMemberKind::kObserved; Reject([&] { (void)SerializeCommitOwnerCoverage(invalid); });
        invalid = value; ++invalid.producer->commit_time_ms; Reject([&] { (void)SerializeCommitOwnerCoverage(invalid); });
        invalid = value; ++invalid.producer->coverage.coord.x; Reject([&] { (void)SerializeCommitOwnerCoverage(invalid); });
        value.producer.reset(); value.image.reset(); value.state = CommitOwnerCoverageState::kRetainedEmpty; value.retained = Retained(key);
        raw = SerializeCommitOwnerCoverage(value); assert(raw.size() == 532U && parse(raw) == value);
        const auto saved = raw;
        // The copy has no borrowed pointer to a later-replaced inventory/log.
        auto replacement = Retained(key, 100U); replacement.inventory_header.fill(0U);
        assert(parse(saved) == value);
        invalid = value; ++invalid.identity.revision; Reject([&] { (void)SerializeCommitOwnerCoverage(invalid); });
        invalid = value; ++invalid.retained->record_offset; Reject([&] { (void)SerializeCommitOwnerCoverage(invalid); });
        invalid = value; invalid.retained->record[4U] = 1U;
        Crc(std::span(invalid.retained->record).first(36U + 8U * key.dims));
        Reject([&] { (void)SerializeCommitOwnerCoverage(invalid); });
        invalid = value; invalid.retained->inventory_header[8U] ^= 1U; Crc(invalid.retained->inventory_header);
        Reject([&] { (void)SerializeCommitOwnerCoverage(invalid); });
        if (!three) { invalid = value; invalid.retained->record.back() = 1U; Reject([&] { (void)SerializeCommitOwnerCoverage(invalid); }); }
        const auto floor = Retained(key, 7U);
        value.state = CommitOwnerCoverageState::kForgottenEmpty; value.identity = {};
        value.floor = CommitOwnerFloorProof{floor.inventory_bytes, floor.inventory_sha256, floor.inventory_header};
        raw = SerializeCommitOwnerCoverage(value); assert(raw.size() == 632U && parse(raw) == value);
        invalid = value; Put(invalid.floor->inventory_header, 24U, 6U, 8U); Crc(invalid.floor->inventory_header);
        Reject([&] { (void)SerializeCommitOwnerCoverage(invalid); });
        invalid = value; invalid.producer = Member(key); Reject([&] { (void)SerializeCommitOwnerCoverage(invalid); });
        owner_artifact_allocation::Guard no_allocation(0U); assert(parse(raw) == value);
    }
}

void GroupAndObservationBindings() {
    const auto key = Key(true); auto member = Member(key, CommitGroupMemberKind::kObserved);
    CommitOwnerObservation observation{kScope, 2U, member, CommitOwnerNativeRef{CommitOwnerNativeKind::kImage, 1024U, {9U}}};
    auto raw = SerializeCommitOwnerObservation(observation); assert(raw.size() == 321U);
    const auto parse = [&](std::span<const std::uint8_t> bytes) { return ParseCommitOwnerObservation(bytes, kScope, 2U, {member.group_id, 0U}); };
    assert(parse(raw) == observation); EnvelopeRefusals(raw, parse); ValidateCommitOwnerObservationBinding(observation, member);
    auto invalid = observation; invalid.member.kind = CommitGroupMemberKind::kChanged; Reject([&] { (void)SerializeCommitOwnerObservation(invalid); });
    invalid = observation; invalid.member.revision = 0U; invalid.member.commit_time_ms = 0U; invalid.member.schema_version = 0U;
    Reject([&] { (void)SerializeCommitOwnerObservation(invalid); });
    auto other = member; ++other.commit_time_ms; Reject([&] { ValidateCommitOwnerObservationBinding(observation, other); });
    Reject([&] { (void)ParseCommitOwnerObservation(raw, kScope, 2U, {member.group_id, 1U}); });
    observation.source = Retained(key); raw = SerializeCommitOwnerObservation(observation); assert(raw.size() == 565U && parse(raw) == observation);
    observation.source = CommitOwnerObservationWal{CommitOwnerNeverWrittenBase{}, {CommitOwnerNativeKind::kWalPrefix, 72U, {8U}}};
    raw = SerializeCommitOwnerObservation(observation); assert(raw.size() == 322U && parse(raw) == observation);
    std::get<CommitOwnerObservationWal>(observation.source).base = CommitOwnerImageBase{{7U, 11U, 1U}, {CommitOwnerNativeKind::kImage, 1024U, {9U}}};
    raw = SerializeCommitOwnerObservation(observation); assert(raw.size() == 394U && parse(raw) == observation);
    invalid = observation; ++std::get<CommitOwnerImageBase>(std::get<CommitOwnerObservationWal>(invalid.source).base).identity.commit_time_ms;
    Reject([&] { (void)SerializeCommitOwnerObservation(invalid); });
    auto group = Group();
    group.members = One(CommitOwnerTreeKind::kMembers, EncodeCommitOwnerMemberKey({member.group_id, 0U}), Ref(CommitOwnerObjectKind::kMemberValue, SerializeCommitOwnerMember(member)));
    std::get<CommitOwnerJournalRecord>(group.source).bytes = 500U;
    group.observations = One(CommitOwnerTreeKind::kObservations, EncodeCommitOwnerMemberKey({member.group_id, 0U}), Ref(CommitOwnerObjectKind::kObservation, raw));
    auto group_raw = SerializeCommitOwnerGroup(group); assert(group_raw.size() == 509U);
    const auto parse_group = [&](std::span<const std::uint8_t> bytes) { return ParseCommitOwnerGroup(bytes, kScope, 2U, group.group_id); };
    assert(parse_group(group_raw) == group); EnvelopeRefusals(group_raw, parse_group);
    CommitOwnerDecisionFacts facts{kScope.directory_id, group.group_id, group.decision_sha256, CommitGroupDurability::kRelaxed, 1U, 1U};
    ValidateCommitOwnerGroupBinding(group, facts);
    auto wrong = facts; wrong.nonzero_observed_count = 0U; Reject([&] { ValidateCommitOwnerGroupBinding(group, wrong); });
    wrong = facts; wrong.sha256[0U] ^= 1U; Reject([&] { ValidateCommitOwnerGroupBinding(group, wrong); });
    wrong = facts; wrong.member_count = 2U; Reject([&] { ValidateCommitOwnerGroupBinding(group, wrong); });
    group.durability = CommitOwnerDurabilityProof::kSyncedDecision; Reject([&] { ValidateCommitOwnerGroupBinding(group, facts); });
    facts.durability = CommitGroupDurability::kSynced; ValidateCommitOwnerGroupBinding(group, facts);
    group.source = CommitOwnerSealSource{608U, {7U}}; group_raw = SerializeCommitOwnerGroup(group); assert(group_raw.size() == 453U && parse_group(group_raw) == group);
    group.durability = CommitOwnerDurabilityProof::kReceipt;
    Reject([&] { (void)SerializeCommitOwnerGroup(group); });
    group.receipt = CommitOwnerJournalRecord{{kScope.directory_id, {6U}, 1U}, CommitRecordKind::kReceipt, 220U, 100U, {8U}};
    group_raw = SerializeCommitOwnerGroup(group); assert(parse_group(group_raw) == group); ValidateCommitOwnerGroupBinding(group, facts);
    group.durability = CommitOwnerDurabilityProof::kCompleteCoverage; group.receipt.reset();
    group_raw = SerializeCommitOwnerGroup(group);
    owner_artifact_allocation::Guard no_allocation(0U); assert(parse_group(group_raw) == group && parse(raw) == observation);
}

std::array<CommitOwnerEdge, 6U> Edges() {
    const auto key = Key(); const CommitOwnerObjectRef coverage{CommitOwnerObjectKind::kCoverageValue, 240U, {9U}};
    const CommitOwnerObjectRef group{CommitOwnerObjectKind::kGroupValue, 509U, {8U}};
    CommitOwnerFeedEdge feed; feed.coverage = key; feed.slots = {CommitOwnerNativeKind::kFeedSlots, 40U, {7U}};
    feed.slot_name[0U] = 's'; feed.slot_name_bytes = 1U; feed.archive = {CommitOwnerNativeKind::kWalPrefix, 200U, {8U}};
    feed.frame_offset = 64U; feed.frame_bytes = 100U; feed.frame_sha256 = {9U};
    return {CommitOwnerEdge{kScope, 2U, {4U}, {5U}, CommitOwnerProducerEdge{key, coverage, 0U}},
        CommitOwnerEdge{kScope, 2U, {4U}, {5U}, CommitOwnerCoveredEdge{key, coverage, 0U}},
        CommitOwnerEdge{kScope, 2U, {4U}, {5U}, CommitOwnerRollbackEdge{{6U}, {7U}, group, 256U, 0U, key, {6U, 10U, 1U}}},
        CommitOwnerEdge{kScope, 2U, {4U}, {5U}, feed},
        CommitOwnerEdge{kScope, 2U, {4U}, {5U}, CommitOwnerIdEdge{{CommitOwnerObjectKind::kIdInventory, 400U, {9U}}, 1U, 160U, 113U, {8U}, {7U}}},
        CommitOwnerEdge{kScope, 2U, {4U}, {5U}, CommitOwnerTransitionEdge{{CommitOwnerObjectKind::kReplacementPlan, 68U, {9U}}, 1U, 256U}}};
}
void EdgeSetsAndExactSources() {
    const std::array<std::size_t, 6U> sizes{221U, 221U, 301U, 381U, 260U, 188U};
    auto edges = Edges(); auto target = Group();
    const auto member = Member(Key()); target.members = One(CommitOwnerTreeKind::kMembers, EncodeCommitOwnerMemberKey({member.group_id, 0U}), Ref(CommitOwnerObjectKind::kMemberValue, SerializeCommitOwnerMember(member)));
    std::get<CommitOwnerJournalRecord>(target.source).bytes = 500U;
    for (std::size_t i = 0U; i < edges.size(); ++i) {
        const auto& edge = edges[i]; const auto key = EncodeCommitOwnerEdgeKey(edge.source); const auto raw = SerializeCommitOwnerEdge(edge);
        assert(key[0U] == i + 1U && raw.size() == sizes[i]);
        const auto parse = [&](std::span<const std::uint8_t> bytes) { return ParseCommitOwnerEdge(bytes, kScope, 2U, key); };
        assert(parse(raw) == edge); EnvelopeRefusals(raw, parse); ValidateCommitOwnerEdgeBinding(edge, target);
        auto wrong_key = key; wrong_key.back() = 1U; Reject([&] { ValidateCommitOwnerEdgeKey(wrong_key); });
        wrong_key = key; wrong_key[1U] ^= 0x80U;
        Reject([&] { (void)ParseCommitOwnerEdge(raw, kScope, 2U, wrong_key); });
        auto wrong = edge; wrong.decision_sha256[0U] ^= 1U; Reject([&] { ValidateCommitOwnerEdgeBinding(wrong, target); });
        CommitOwnerSetHasher set(CommitOwnerTreeKind::kOwnerEdges); const auto ref = Ref(CommitOwnerObjectKind::kOwnerEdge, raw);
        set.Add(key, ref); Reject([&] { set.Add(key, ref); });
        auto other_ref = ref; other_ref.sha256[0U] ^= 1U; Reject([&] { set.Add(key, other_ref); });
        const auto commitment = One(CommitOwnerTreeKind::kOwnerEdges, key, ref);
        CommitOwnerPage page; page.scope = kScope; page.tree = CommitOwnerTreeKind::kOwnerEdges; page.node = CommitOwnerNodeKind::kLeaf;
        page.prefix.assign(key.begin(), key.end()); page.value = ref; page.subtree_keys = 1U; page.subtree_bytes = commitment.owned_bytes;
        ValidateCommitOwnerSetRoot(commitment, page.tree, kScope, page, commitment.root, 2U);
        auto changed_page = page; changed_page.scope.owner_id[0U] ^= 1U; Reject([&] { ValidateCommitOwnerSetRoot(commitment, page.tree, kScope, changed_page, commitment.root, 2U); });
        auto root = CommitOwnerRoot{}; root.scope = kScope; root.state = CommitOwnerState::kActive;
        root.generation = 2U; root.coverage_tree = Empty(CommitOwnerTreeKind::kCoverage).root;
        root.group_tree = commitment.root;
        root.journal_inventory = {CommitOwnerObjectKind::kJournalInventory, 128U, {1U}};
        root.id_inventory = {CommitOwnerObjectKind::kIdInventory, 164U, {2U}};
        root.reference_context = {kScope.directory_id, {3U}, 1U}; root.reference_replay = {64U, {}, CommitRecordKind::kNone, {4U}};
        Reject([&] { ValidateCommitOwnerRootPage(root, page.tree, page, commitment.root); });
        owner_artifact_allocation::Guard no_allocation(0U); assert(parse(raw) == edge);
    }
    const auto rollback_key = EncodeCommitOwnerEdgeKey(edges[2U].source);
    assert(rollback_key[17U] == 0U && rollback_key[23U] == 1U && rollback_key[24U] == 0U);
    auto lower = edges[2U]; std::get<CommitOwnerRollbackEdge>(lower.source).source_ordinal = 255U;
    assert(EncodeCommitOwnerEdgeKey(lower.source) < rollback_key);
    auto transition = edges[5U]; std::get<CommitOwnerTransitionEdge>(transition.source).source_generation = 2U;
    Reject([&] { (void)SerializeCommitOwnerEdge(transition); });
    auto coverage = Coverage(Key()); coverage.state = CommitOwnerCoverageState::kImage; coverage.identity = {7U, 11U, 1U};
    coverage.image = CommitOwnerNativeRef{CommitOwnerNativeKind::kImage, 1024U, {9U}}; coverage.producer = member;
    auto producer = edges[0U]; std::get<CommitOwnerProducerEdge>(producer.source).value = Ref(CommitOwnerObjectKind::kCoverageValue, SerializeCommitOwnerCoverage(coverage));
    ValidateCommitOwnerProducerSource(producer, coverage);
    auto different = coverage; different.image->sha256[0U] ^= 1U;
    assert(SerializeCommitOwnerCoverage(different).size() == SerializeCommitOwnerCoverage(coverage).size());
    Reject([&] { ValidateCommitOwnerProducerSource(producer, different); });
    auto covered = edges[1U]; std::get<CommitOwnerCoveredEdge>(covered.source).value = std::get<CommitOwnerProducerEdge>(producer.source).value;
    ValidateCommitOwnerCoveredSource(covered, coverage, member);
    auto other_member = member; other_member.decision_sha256[0U] ^= 1U; Reject([&] { ValidateCommitOwnerCoveredSource(covered, coverage, other_member); });
}

void IdKeyStreamingAndIncarnation() {
    const std::array<std::uint8_t, 3U> principal{'a', 'b', 'c'};
    const std::array<std::uint8_t, 2U> id{0U, 255U};
    CommitOwnerIdKeyHasher whole(3U, 2U); whole.Principal(principal); whole.Id(id); const auto expected = whole.Finish();
    CommitOwnerIdKeyHasher pieces(3U, 2U); pieces.Principal(std::span(principal).first(1U)); pieces.Principal(std::span(principal).subspan(1U));
    pieces.Id(std::span(id).first(1U)); pieces.Id(std::span(id).last(1U)); assert(pieces.Finish() == expected);
    Reject([&] { (void)pieces.Finish(); }); Reject([&] { CommitOwnerIdKeyHasher too_big(0U, 65U); });
    CommitOwnerIdKeyHasher incomplete(3U, 2U); incomplete.Principal(std::span(principal).first(1U));
    Reject([&] { incomplete.Id(id); }); Reject([&] { (void)incomplete.Finish(); });
    auto edge = Edges()[4U]; auto& source = std::get<CommitOwnerIdEdge>(edge.source); source.entry_bytes = 117U; source.key_sha256 = expected;
    CommitIdEntry entry{"abc", {0U, 255U}, edge.group_id, edge.decision_sha256, {8U}, 20U};
    ValidateCommitOwnerIdSource(edge, entry, source.inventory, 1U, source.entry_sha256);
    auto other = entry; other.id[0U] = 1U;
    Reject([&] { ValidateCommitOwnerIdSource(edge, other, source.inventory, 1U, source.entry_sha256); });
    Reject([&] { ValidateCommitOwnerIdSource(edge, entry, source.inventory, 2U, source.entry_sha256); });
    auto inventory = source.inventory; inventory.sha256[0U] ^= 1U;
    Reject([&] { ValidateCommitOwnerIdSource(edge, entry, inventory, 1U, source.entry_sha256); });
    CommitOwnerIdKeyHasher empty(0U, 0U); const auto empty_key = empty.Finish();
    CommitOwnerIdKeyHasher nonempty(0U, 1U); nonempty.Id(std::span(id).first(1U)); assert(nonempty.Finish() != empty_key);
}

void CounterOwnershipAndExhaustion() {
    CommitCounterRecord initial{kScope, {6U}, 1U, 1U}; const auto raw = SerializeCommitCounter(initial);
    assert(ParseCommitCounter(raw, kScope) == initial);
    std::array<std::uint8_t, 96U> golden{}; golden[0U] = 'C'; golden[1U] = 'K'; golden[2U] = 'T'; golden[3U] = 'C';
    golden[4U] = 1U; golden[6U] = 2U; golden[8U] = 96U; golden[16U] = 1U; golden[32U] = 2U; golden[48U] = 6U;
    golden[64U] = 1U; golden[72U] = 1U; Crc(golden); assert(raw == golden);
    for (std::size_t cut = 0U; cut < raw.size(); ++cut) Reject([&] { (void)ParseCommitCounter(std::span(raw).first(cut), kScope); });
    auto next = initial; next.generation = 2U; next.exclusive_ceiling = 128U; ValidateCommitCounterSuccessor(initial, next);
    assert(ParseCommitCounter(SerializeCommitCounter(next), kScope) == next);
    auto invalid = next; invalid.counter_owner[0U] ^= 1U; Reject([&] { ValidateCommitCounterSuccessor(initial, invalid); });
    invalid = next; invalid.exclusive_ceiling = 1U; Reject([&] { ValidateCommitCounterSuccessor(initial, invalid); });
    invalid = next; invalid.generation = 3U; Reject([&] { ValidateCommitCounterSuccessor(initial, invalid); });
    auto maximum = next; maximum.generation = maximum.exclusive_ceiling = std::numeric_limits<std::uint64_t>::max();
    assert(ParseCommitCounter(SerializeCommitCounter(maximum), kScope) == maximum);
    Reject([&] { ValidateCommitCounterSuccessor(maximum, initial); }); Reject([&] { ValidateCommitCounterSuccessor(maximum, maximum); });
    for (const auto at : {4U, 6U, 7U, 8U, 16U, 32U, 48U, 80U}) {
        auto corrupted = raw; corrupted[at] ^= 0x80U; Crc(corrupted);
        if (at == 48U) {
            const auto parsed = ParseCommitCounter(corrupted, kScope);
            Reject([&] { ValidateCommitCounterSuccessor(parsed, next); });
        } else Reject([&] { (void)ParseCommitCounter(corrupted, kScope); });
    }
    auto root = CommitOwnerRoot{}; root.scope = kScope;
    Reject([&] { ValidateCommitCounterBinding(root, next); });
    root.state = CommitOwnerState::kActive; root.generation = 2U;
    root.coverage_tree = Empty(CommitOwnerTreeKind::kCoverage).root; root.group_tree = Empty(CommitOwnerTreeKind::kGroups).root;
    root.journal_inventory = {CommitOwnerObjectKind::kJournalInventory, 128U, {1U}}; root.id_inventory = {CommitOwnerObjectKind::kIdInventory, 164U, {2U}};
    root.reference_context = {kScope.directory_id, {3U}, 1U}; root.reference_replay = {64U, {}, CommitRecordKind::kNone, {4U}};
    root.counter_owner = initial.counter_owner; root.counter_ceiling = 128U; ValidateCommitCounterBinding(root, next);
    root.counter_ceiling = 129U; Reject([&] { ValidateCommitCounterBinding(root, next); });
    owner_artifact_allocation::Guard no_allocation(0U); assert(ParseCommitCounter(raw, kScope) == initial);
}

void SparseHeaderRefusal() {
    const auto path = std::filesystem::temp_directory_path() / ("chunkdb-owner-values-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const bool created = std::filesystem::create_directory(path); assert(created);
    struct Cleanup { std::filesystem::path path; ~Cleanup() { std::error_code error; std::filesystem::remove_all(path, error); assert(!error); } } cleanup{path};
    const auto file = path / "oversized";
    auto raw = SerializeCommitOwnerGroup(Group()); Put(raw, 8U, 1ULL << 33U, 8U); Crc(raw, true);
    { std::ofstream out(file, std::ios::binary); out.write(reinterpret_cast<const char*>(raw.data()), 64); assert(out); }
    std::filesystem::resize_file(file, 1ULL << 33U);
    assert(std::filesystem::file_size(file) == (1ULL << 33U));
    std::array<std::uint8_t, 64U> header{};
    { std::ifstream in(file, std::ios::binary); in.read(reinterpret_cast<char*>(header.data()), header.size()); assert(in); }
    const auto denied = owner_artifact_allocation::denied.load(); bool refused = false;
    {
        owner_artifact_allocation::Guard bounded(4096U);
        try {
            const auto admitted = InspectCommitOwnerHeader(header, kScope, CommitOwnerObjectKind::kGroupValue, 2U);
            Bytes body(static_cast<std::size_t>(admitted.bytes)); (void)body;
        } catch (const std::invalid_argument&) { refused = true; } catch (const std::bad_alloc&) {}
    }
    assert(refused && owner_artifact_allocation::denied.load() == denied);
}
}  // namespace

int main() {
    owner_artifact_allocation::CalibrateAligned();
    CoverageAndOwnedProofs(); GroupAndObservationBindings(); EdgeSetsAndExactSources();
    IdKeyStreamingAndIncarnation(); CounterOwnershipAndExhaustion(); SparseHeaderRefusal();
}
