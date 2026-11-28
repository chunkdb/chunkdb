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
#include "chunk_file_header.hpp"
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
    damaged = raw; damaged.insert(damaged.end() - 4, 0U); Put(damaged, 8U, damaged.size(), 8U); Crc(damaged, true); Reject([&] { parse(damaged); });
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

using Where = std::source_location;
constexpr auto kU64 = std::numeric_limits<std::uint64_t>::max();
Bytes Encode(const CommitOwnerCoverageValue& value) { return SerializeCommitOwnerCoverage(value); }
Bytes Encode(const CommitOwnerGroupValue& value) { return SerializeCommitOwnerGroup(value); }
Bytes Encode(const CommitOwnerObservation& value) { return SerializeCommitOwnerObservation(value); }
Bytes Encode(const CommitOwnerEdge& value) { return SerializeCommitOwnerEdge(value); }
Bytes Encode(const CommitCounterRecord& value) { const auto raw = SerializeCommitCounter(value); return {raw.begin(), raw.end()}; }
// Serialization and parsing share one validator, so a refused encode pins the parse refusal too.
template <class Value, class Change> void RefuseValue(Value value, Change change, const Where& where = Where::current()) {
    change(value); Reject([&] { (void)Encode(value); }, where);
}
template <class Parse> void RefuseByte(const Bytes& raw, std::size_t at, std::uint64_t value, Parse parse, const Where& where = Where::current()) {
    auto damaged = raw; Put(damaged, at, value, 1U); Crc(damaged, true); Reject([&] { (void)parse(damaged); }, where);
}
void Seal(CommitOwnerRetainedProof& proof, unsigned dims) {
    Crc(proof.inventory_header); Crc(std::span(proof.log_header).first(28U + 8U * dims)); Crc(std::span(proof.record).first(36U + 8U * dims));
}
CommitOwnerCoverageValue ImageCoverage(CommitOwnerCoverageKey key) {
    auto value = Coverage(key); value.state = CommitOwnerCoverageState::kImage; value.identity = {7U, 11U, 1U};
    value.image = CommitOwnerNativeRef{CommitOwnerNativeKind::kImage, 1024U, {9U}}; value.producer = Member(key); return value;
}
CommitOwnerCoverageValue RetainedCoverage(CommitOwnerCoverageKey key) {
    auto value = Coverage(key); value.state = CommitOwnerCoverageState::kRetainedEmpty; value.identity = {7U, 11U, 1U};
    value.retained = Retained(key); return value;
}
CommitOwnerCoverageValue ForgottenCoverage(CommitOwnerCoverageKey key, std::uint64_t floor = 7U) {
    auto value = Coverage(key); value.state = CommitOwnerCoverageState::kForgottenEmpty; value.retained = Retained(key);
    const auto proof = Retained(key, floor); value.floor = CommitOwnerFloorProof{proof.inventory_bytes, proof.inventory_sha256, proof.inventory_header};
    return value;
}
CommitOwnerJournalRecord& Journal(CommitOwnerGroupValue& value) { return std::get<CommitOwnerJournalRecord>(value.source); }

void CoverageRefusalsAndBounds() {
    for (const bool three : {false, true}) {
        const auto key = Key(three); const unsigned dims = key.dims;
        const auto never = Coverage(key), image = ImageCoverage(key), retained = RetainedCoverage(key), forgotten = ForgottenCoverage(key);
        RefuseValue(never, [](auto& v) { v.generation = 0U; });
        RefuseValue(never, [](auto& v) { v.scope.owner_id = {}; });
        RefuseValue(never, [](auto& v) { v.key.dims = 4U; });
        RefuseValue(never, [](auto& v) { v.state = static_cast<CommitOwnerCoverageState>(4U); });
        RefuseValue(never, [](auto& v) { v.identity.commit_time_ms = 1U; });
        RefuseValue(never, [&](auto& v) { v.image = image.image; });
        RefuseValue(never, [&](auto& v) { v.retained = retained.retained; });
        RefuseValue(never, [&](auto& v) { v.floor = forgotten.floor; });
        RefuseValue(never, [&](auto& v) { v.producer = image.producer; });

        RefuseValue(image, [](auto& v) { v.producer.reset(); v.identity.revision = 0U; });
        RefuseValue(image, [](auto& v) { v.producer.reset(); v.identity.schema_version = 0U; });
        RefuseValue(image, [](auto& v) { v.image.reset(); });
        RefuseValue(image, [&](auto& v) { v.retained = retained.retained; });
        RefuseValue(image, [&](auto& v) { v.floor = forgotten.floor; });
        RefuseValue(image, [](auto& v) { v.image->kind = CommitOwnerNativeKind::kWalPrefix; });
        RefuseValue(image, [](auto& v) { v.image->sha256 = {}; });
        auto bound = image; bound.producer.reset(); bound.image->bytes = ChunkImageFixedHeaderBytes(key.dims); (void)Encode(bound);
        RefuseValue(bound, [](auto& v) { --v.image->bytes; });
        bound.image->bytes = kU64 / 8U; (void)Encode(bound);
        RefuseValue(bound, [](auto& v) { ++v.image->bytes; });
        RefuseValue(image, [](auto& v) { v.producer->kind = CommitGroupMemberKind::kObserved; });
        RefuseValue(image, [](auto& v) { v.producer->generation = 2U; });
        RefuseValue(image, [](auto& v) { v.producer->scope.owner_id[0U] ^= 1U; });
        RefuseValue(image, [](auto& v) { ++v.producer->coverage.coord.y; });
        RefuseValue(image, [](auto& v) { ++v.producer->schema_version; });

        RefuseValue(retained, [](auto& v) { v.retained.reset(); });
        RefuseValue(retained, [&](auto& v) { v.image = image.image; });
        RefuseValue(retained, [&](auto& v) { v.floor = forgotten.floor; });
        RefuseValue(retained, [](auto& v) { ++v.identity.commit_time_ms; });
        auto deleted = retained; deleted.producer = Member(key); (void)Encode(deleted);
        RefuseValue(deleted, [](auto& v) { ++v.producer->commit_time_ms; });

        const auto header_bytes = 28U + 8U * dims, record_bytes = 36U + 8U * dims, coords = 8U * dims;
        const auto proof = [&](auto change, const Where& where = Where::current()) {
            auto value = retained; change(*value.retained); Seal(*value.retained, dims); Reject([&] { (void)Encode(value); }, where);
        };
        proof([](auto& p) { ++p.inventory_bytes; });
        proof([](auto& p) { p.inventory_sha256 = {}; });
        proof([](auto& p) { p.inventory_header[0U] = 'X'; });
        proof([](auto& p) { p.inventory_header[4U] = 3U; });
        proof([](auto& p) { p.inventory_header[6U] ^= 1U; });
        proof([](auto& p) { p.inventory_header[7U] = 1U; });
        proof([](auto& p) { p.inventory_header[9U] ^= 1U; });
        proof([](auto& p) { p.inventory_header[32U] = 1U; });
        proof([](auto& p) { Put(p.inventory_header, 48U, 0U, 8U); });
        proof([](auto& p) { Put(p.inventory_header, 40U, 0U, 8U); p.inventory_bytes = 64U; });
        proof([](auto& p) { Put(p.inventory_header, 40U, kU64, 8U); });
        proof([](auto& p) { p.log_header[0U] = 'X'; });
        proof([](auto& p) { p.log_header[4U] = 2U; });
        proof([](auto& p) { p.log_header[6U] ^= 1U; });
        proof([](auto& p) { p.log_header[7U] = 1U; });
        proof([](auto& p) { p.log_header[9U] ^= 1U; });
        proof([](auto& p) { p.log_header[24U] ^= 1U; });
        proof([](auto& p) { p.inventory_entry[0U] ^= 1U; });
        proof([](auto& p) { p.record[0U] ^= 1U; });
        proof([](auto& p) { p.record[4U] = 1U; });
        proof([](auto& p) { p.record[4U] = 3U; });
        proof([](auto& p) { p.record[5U] = 1U; });
        proof([](auto& p) { p.record[8U] ^= 1U; });
        proof([&](auto& p) { Put(p.record, 8U + coords, 0U, 8U); });
        proof([&](auto& p) { Put(p.record, 8U + coords + 16U, 0U, 8U); });
        proof([&](auto& p) { p.inventory_entry[coords + 8U] = 1U; });
        proof([&](auto& p) { p.inventory_entry[coords + 47U] = 1U; });
        proof([&](auto& p) { Put(p.inventory_entry, coords, header_bytes + record_bytes + 1U, 8U); });
        proof([&](auto& p) { Put(p.inventory_entry, coords, header_bytes, 8U); });
        proof([&](auto& p) { Put(p.inventory_entry, coords, header_bytes - 1U, 8U); p.record_offset = header_bytes - 1U; });
        proof([&](auto& p) { p.record_offset = 0U; });
        proof([&](auto& p) { p.record_offset += record_bytes; });
        if (!three) {
            proof([](auto& p) { p.log_header.back() = 1U; });
            proof([](auto& p) { p.record.back() = 1U; });
            proof([](auto& p) { p.inventory_entry.back() = 1U; });
        }
        auto damaged = retained; damaged.retained->inventory_header[56U] ^= 1U; Reject([&] { (void)Encode(damaged); });
        damaged = retained; damaged.retained->log_header[header_bytes - 4U] ^= 1U; Reject([&] { (void)Encode(damaged); });
        damaged = retained; damaged.retained->record[record_bytes - 4U] ^= 1U; Reject([&] { (void)Encode(damaged); });
        auto later = retained; Put(later.retained->inventory_entry, coords, header_bytes + 2U * record_bytes, 8U);
        later.retained->record_offset = header_bytes + record_bytes; (void)Encode(later);

        auto equal_floor = ForgottenCoverage(key, 7U); (void)Encode(equal_floor);
        RefuseValue(forgotten, [](auto& v) { v.identity = {7U, 11U, 1U}; });
        RefuseValue(forgotten, [](auto& v) { v.floor.reset(); });
        RefuseValue(forgotten, [](auto& v) { v.retained.reset(); });
        RefuseValue(forgotten, [&](auto& v) { v.image = image.image; });
        RefuseValue(forgotten, [](auto& v) { v.floor->inventory_sha256 = {}; });
        RefuseValue(forgotten, [](auto& v) { v.floor->inventory_header[9U] ^= 1U; Crc(v.floor->inventory_header); });
        RefuseValue(forgotten, [](auto& v) { ++v.floor->inventory_bytes; });

        const auto raw = Encode(never);
        const auto parse = [&](std::span<const std::uint8_t> bytes) { return ParseCommitOwnerCoverage(bytes, kScope, 1U, key); };
        assert(parse(raw) == never);
        RefuseByte(raw, 105U, 4U, parse);
        for (std::size_t at = 106U; at < 112U; ++at) RefuseByte(raw, at, 1U, parse);
        for (const auto at : {136U, 137U, 138U, 235U}) RefuseByte(raw, at, 2U, parse);
        RefuseByte(raw, 139U, 0U, parse); RefuseByte(raw, 139U, 12U, parse); RefuseByte(raw, 140U, 1U, parse);
        Reject([&] { (void)ParseCommitOwnerCoverage(raw, kScope, 0U, key); });
        auto newer = never; newer.generation = 2U; const auto newer_raw = Encode(newer);
        assert(ParseCommitOwnerCoverage(newer_raw, kScope, 2U, key) == newer);
        Reject([&] { (void)ParseCommitOwnerCoverage(newer_raw, kScope, 1U, key); });
        auto other = kScope; other.role = CommitOwnerRole::kStandalone; Reject([&] { (void)ParseCommitOwnerCoverage(raw, other, 1U, key); });
    }
}

void SetCommitmentBounds() {
    const auto member = Member(Key()); const auto key = EncodeCommitOwnerMemberKey({member.group_id, 0U});
    const auto value = Ref(CommitOwnerObjectKind::kMemberValue, SerializeCommitOwnerMember(member));
    auto group = Group(); group.members = One(CommitOwnerTreeKind::kMembers, key, value); Journal(group).bytes = 1000U; (void)Encode(group);
    const std::uint64_t leaf = 92U + 24U + 48U, per_key = leaf + 204U;
    assert(group.members.owned_bytes == per_key && group.members.root.bytes == leaf);
    RefuseValue(group, [](auto& v) { v.members.root.kind = CommitOwnerObjectKind::kMemberValue; v.members.root.bytes = 204U; });
    RefuseValue(group, [](auto& v) { v.owners.root.bytes = 91U; v.owners.owned_bytes = 91U; });
    RefuseValue(group, [](auto& v) { v.members.root.sha256 = {}; });
    RefuseValue(group, [](auto& v) { v.members.sha256 = {}; });
    RefuseValue(group, [](auto& v) { v.members.owned_bytes = kMaxCommitCoveredTreeBytes + 1U; });
    auto ceiling = group; ceiling.members.owned_bytes = kMaxCommitCoveredTreeBytes; (void)Encode(ceiling);
    auto two = group; two.members.count = 2U; two.members.owned_bytes = 2U * per_key; (void)Encode(two);
    RefuseValue(two, [](auto& v) { --v.members.owned_bytes; });
    RefuseValue(group, [&](auto& v) { v.members.root.bytes = leaf - 1U; });
    RefuseValue(group, [&](auto& v) { v.members.root.bytes = 92U; });
    RefuseValue(group, [](auto& v) { v.owners.root.bytes = 93U; v.owners.owned_bytes = 93U; });
    RefuseValue(group, [](auto& v) { v.owners.sha256 = Empty(CommitOwnerTreeKind::kMembers).sha256; });
    for (const auto tree : {CommitOwnerTreeKind::kCoverage, CommitOwnerTreeKind::kGroups}) {
        const auto set = Empty(tree); CommitOwnerPage page; page.scope = kScope; page.tree = tree;
        Reject([&] { ValidateCommitOwnerSetRoot(set, tree, kScope, page, set.root, 2U); });
    }
    const auto empty = Empty(CommitOwnerTreeKind::kMembers); CommitOwnerPage page; page.scope = kScope; page.tree = CommitOwnerTreeKind::kMembers;
    ValidateCommitOwnerSetRoot(empty, page.tree, kScope, page, empty.root, 1U);
    Reject([&] { ValidateCommitOwnerSetRoot(empty, page.tree, kScope, page, empty.root, 0U); });
    auto newer = page; newer.generation = 2U; Reject([&] { ValidateCommitOwnerSetRoot(empty, page.tree, kScope, newer, empty.root, 1U); });
    auto other_tree = page; other_tree.tree = CommitOwnerTreeKind::kObservations;
    Reject([&] { ValidateCommitOwnerSetRoot(empty, page.tree, kScope, other_tree, empty.root, 1U); });
    auto measured = empty.root; measured.sha256[0U] ^= 1U; Reject([&] { ValidateCommitOwnerSetRoot(empty, page.tree, kScope, page, measured, 1U); });

    Reject([] { CommitOwnerSetHasher invalid(static_cast<CommitOwnerTreeKind>(6U)); });
    assert(CommitOwnerSetHasher(CommitOwnerTreeKind::kMembers).Finish() != CommitOwnerSetHasher(CommitOwnerTreeKind::kObservations).Finish());
    CommitOwnerSetHasher digest(CommitOwnerTreeKind::kMembers);
    Reject([&] { digest.Add(std::span(key).first(23U), value); });
    auto zero = key; std::fill_n(zero.begin(), 16U, 0U); Reject([&] { digest.Add(zero, value); });
    Reject([&] { digest.Add(key, Ref(CommitOwnerObjectKind::kObservation, SerializeCommitOwnerMember(member))); });
    const auto higher = EncodeCommitOwnerMemberKey({member.group_id, 1U});
    digest.Add(higher, value); Reject([&] { digest.Add(key, value); }); Reject([&] { digest.Add(higher, value); });
    assert(digest.count() == 1U); (void)digest.Finish();
    Reject([&] { (void)digest.Finish(); }); Reject([&] { digest.Add(EncodeCommitOwnerMemberKey({member.group_id, 2U}), value); });
}

void GroupRefusalsAndBounds() {
    const auto group = Group();
    RefuseValue(group, [](auto& v) { v.generation = 0U; });
    RefuseValue(group, [](auto& v) { v.group_id = {}; });
    RefuseValue(group, [](auto& v) { v.decision_sha256 = {}; Journal(v).sha256 = {}; });
    RefuseValue(group, [](auto& v) { Journal(v).kind = CommitRecordKind::kReceipt; Journal(v).bytes = kCommitReceiptBytes; });
    RefuseValue(group, [](auto& v) { Journal(v).sha256[0U] ^= 1U; });
    RefuseValue(group, [](auto& v) { Journal(v).kind = CommitRecordKind::kNone; });
    RefuseValue(group, [](auto& v) { Journal(v).kind = static_cast<CommitRecordKind>(4U); });
    RefuseValue(group, [](auto& v) { Journal(v).context.directory_id[0U] ^= 1U; });
    RefuseValue(group, [](auto& v) { Journal(v).context.journal_id = {}; });
    RefuseValue(group, [](auto& v) { Journal(v).offset = kCommitJournalHeaderBytes - 1U; });
    RefuseValue(group, [](auto& v) { Journal(v).bytes = 155U; });
    RefuseValue(group, [](auto& v) { Journal(v).bytes = kMaxCommitDecisionBytes + 1U; });
    auto last = group; Journal(last).bytes = kMaxCommitDecisionBytes; Journal(last).offset = kU64 - kMaxCommitDecisionBytes; (void)Encode(last);
    RefuseValue(last, [](auto& v) { ++Journal(v).offset; });
    auto sealed = group; Journal(sealed).kind = CommitRecordKind::kSeal; Journal(sealed).bytes = 264U; Journal(sealed).sha256 = {7U}; (void)Encode(sealed);
    RefuseValue(sealed, [](auto& v) { Journal(v).bytes = 263U; });
    RefuseValue(sealed, [](auto& v) { Journal(v).sha256 = {}; });
    Journal(sealed).bytes = kMaxCommitSealBytes; (void)Encode(sealed);
    RefuseValue(sealed, [](auto& v) { ++Journal(v).bytes; });
    auto seal = group; seal.source = CommitOwnerSealSource{264U, {7U}}; (void)Encode(seal);
    const auto seal_of = [](CommitOwnerGroupValue& v) -> CommitOwnerSealSource& { return std::get<CommitOwnerSealSource>(v.source); };
    RefuseValue(seal, [&](auto& v) { seal_of(v).bytes = 263U; });
    RefuseValue(seal, [&](auto& v) { seal_of(v).sha256 = {}; });
    seal_of(seal).bytes = kMaxCommitSealBytes; (void)Encode(seal);
    RefuseValue(seal, [&](auto& v) { ++seal_of(v).bytes; });

    const CommitOwnerJournalRecord receipt{{kScope.directory_id, {6U}, 1U}, CommitRecordKind::kReceipt, 220U, kCommitReceiptBytes, {8U}};
    for (const auto durability : {CommitOwnerDurabilityProof::kUnsettled, CommitOwnerDurabilityProof::kSyncedDecision, CommitOwnerDurabilityProof::kCompleteCoverage})
        RefuseValue(group, [&](auto& v) { v.durability = durability; v.receipt = receipt; });
    RefuseValue(group, [](auto& v) { v.durability = static_cast<CommitOwnerDurabilityProof>(4U); });
    auto receipted = group; receipted.durability = CommitOwnerDurabilityProof::kReceipt; receipted.receipt = receipt; (void)Encode(receipted);
    RefuseValue(receipted, [](auto& v) { v.receipt.reset(); });
    RefuseValue(receipted, [](auto& v) { v.receipt->kind = CommitRecordKind::kDecision; v.receipt->bytes = 156U; });
    RefuseValue(receipted, [](auto& v) { v.receipt->bytes = kCommitReceiptBytes + 1U; });
    RefuseValue(receipted, [](auto& v) { v.receipt->context.directory_id[0U] ^= 1U; });
    RefuseValue(receipted, [](auto& v) { v.receipt->sha256 = {}; });

    const auto observed = Member(Key(true), CommitGroupMemberKind::kObserved); const auto member_key = EncodeCommitOwnerMemberKey({observed.group_id, 0U});
    const auto members = One(CommitOwnerTreeKind::kMembers, member_key, Ref(CommitOwnerObjectKind::kMemberValue, SerializeCommitOwnerMember(observed)));
    const CommitOwnerObservation observation{kScope, 2U, observed, CommitOwnerNativeRef{CommitOwnerNativeKind::kImage, 1024U, {9U}}};
    const auto observations = One(CommitOwnerTreeKind::kObservations, member_key, Ref(CommitOwnerObjectKind::kObservation, Encode(observation)));
    RefuseValue(group, [&](auto& v) { v.observations = observations; });
    auto exact = group; exact.members = members; exact.observations = observations; Journal(exact).bytes = 156U + kCommitGroupMemberBytes + 33U; (void)Encode(exact);
    RefuseValue(exact, [](auto& v) { --Journal(v).bytes; });
    RefuseValue(exact, [](auto& v) { Journal(v).bytes = 156U + kCommitGroupMemberBytes - 1U; });

    const auto raw = Encode(group);
    const auto parse = [&](std::span<const std::uint8_t> bytes) { return ParseCommitOwnerGroup(bytes, kScope, 2U, group.group_id); };
    assert(parse(raw) == group);
    RefuseByte(raw, 112U, 0U, parse); RefuseByte(raw, 112U, 3U, parse); RefuseByte(raw, 114U, 1U, parse);
    RefuseByte(raw, 209U, 4U, parse);
    for (std::size_t at = 210U; at < 216U; ++at) RefuseByte(raw, at, 1U, parse);
    RefuseByte(raw, 216U, 2U, parse);
    Reject([&] { (void)ParseCommitOwnerGroup(raw, kScope, 1U, group.group_id); });
    Reject([&] { (void)ParseCommitOwnerGroup(raw, kScope, 0U, group.group_id); });
    Reject([&] { (void)ParseCommitOwnerGroup(raw, kScope, 2U, {9U}); });

    const CommitOwnerDecisionFacts facts{kScope.directory_id, exact.group_id, exact.decision_sha256, CommitGroupDurability::kRelaxed, 1U, 1U};
    ValidateCommitOwnerGroupBinding(exact, facts);
    const auto refuse = [&](auto change, const CommitOwnerGroupValue& value, const Where& where = Where::current()) {
        auto wrong = facts; change(wrong); Reject([&] { ValidateCommitOwnerGroupBinding(value, wrong); }, where);
    };
    refuse([](auto& f) { f.directory_id[0U] ^= 1U; }, exact);
    refuse([](auto& f) { f.group_id[0U] ^= 1U; }, exact);
    refuse([](auto& f) { f.sha256[0U] ^= 1U; }, exact);
    refuse([](auto& f) { f.member_count = 2U; }, exact);
    refuse([](auto& f) { f.nonzero_observed_count = 0U; }, exact);
    refuse([](auto& f) { f.nonzero_observed_count = 2U; f.member_count = 1U; }, exact);
    refuse([](auto& f) { f.durability = static_cast<CommitGroupDurability>(2U); }, exact);
    refuse([](auto& f) { f.durability = CommitGroupDurability::kSynced; }, exact);
    auto synced = exact; synced.durability = CommitOwnerDurabilityProof::kSyncedDecision;
    refuse([](auto&) {}, synced);
    auto synced_facts = facts; synced_facts.durability = CommitGroupDurability::kSynced; ValidateCommitOwnerGroupBinding(synced, synced_facts);
    for (const auto durability : {CommitOwnerDurabilityProof::kReceipt, CommitOwnerDurabilityProof::kCompleteCoverage}) {
        auto settled = exact; settled.durability = durability;
        if (durability == CommitOwnerDurabilityProof::kReceipt) settled.receipt = receipt;
        ValidateCommitOwnerGroupBinding(settled, facts); ValidateCommitOwnerGroupBinding(settled, synced_facts);
    }
}

void ObservationRefusalsAndBounds() {
    for (const bool three : {false, true}) {
        const auto key = Key(three); const auto member = Member(key, CommitGroupMemberKind::kObserved);
        const CommitOwnerObservation image{kScope, 2U, member, CommitOwnerNativeRef{CommitOwnerNativeKind::kImage, 1024U, {9U}}};
        const auto native = [](CommitOwnerObservation& v) -> CommitOwnerNativeRef& { return std::get<CommitOwnerNativeRef>(v.source); };
        RefuseValue(image, [](auto& v) { v.generation = 0U; });
        RefuseValue(image, [](auto& v) { v.member.generation = 3U; });
        RefuseValue(image, [](auto& v) { v.member.kind = CommitGroupMemberKind::kChanged; });
        RefuseValue(image, [](auto& v) { v.member.revision = v.member.commit_time_ms = v.member.schema_version = 0U; });
        RefuseValue(image, [&](auto& v) { native(v).kind = CommitOwnerNativeKind::kWalPrefix; });
        RefuseValue(image, [&](auto& v) { native(v).sha256 = {}; });
        auto bound = image; native(bound).bytes = ChunkImageFixedHeaderBytes(key.dims); (void)Encode(bound);
        RefuseValue(bound, [&](auto& v) { --native(v).bytes; });
        auto retained = image; retained.source = Retained(key); (void)Encode(retained);
        RefuseValue(retained, [](auto& v) { ++v.member.commit_time_ms; });

        const CommitOwnerNativeRef prefix{CommitOwnerNativeKind::kWalPrefix, ChunkWalHeaderBytes(key.dims), {8U}};
        auto wal = image; wal.source = CommitOwnerObservationWal{CommitOwnerNeverWrittenBase{}, prefix}; (void)Encode(wal);
        const auto log = [](CommitOwnerObservation& v) -> CommitOwnerObservationWal& { return std::get<CommitOwnerObservationWal>(v.source); };
        RefuseValue(wal, [&](auto& v) { --log(v).prefix.bytes; });
        RefuseValue(wal, [&](auto& v) { log(v).prefix.kind = CommitOwnerNativeKind::kImage; });
        RefuseValue(wal, [&](auto& v) { log(v).prefix.sha256 = {}; });
        for (const CommitOwnerIdentity older : {CommitOwnerIdentity{6U, 12U, 2U}, CommitOwnerIdentity{7U, 11U, 1U}}) {
            auto based = wal; log(based).base = CommitOwnerImageBase{older, {CommitOwnerNativeKind::kImage, ChunkImageFixedHeaderBytes(key.dims), {9U}}};
            (void)Encode(based);
        }
        const auto image_base = [&](auto& v) -> CommitOwnerImageBase& {
            if (!std::holds_alternative<CommitOwnerImageBase>(log(v).base))
                log(v).base = CommitOwnerImageBase{{6U, 10U, 1U}, {CommitOwnerNativeKind::kImage, 1024U, {9U}}};
            return std::get<CommitOwnerImageBase>(log(v).base);
        };
        RefuseValue(wal, [&](auto& v) { image_base(v).identity = {8U, 11U, 1U}; });
        RefuseValue(wal, [&](auto& v) { image_base(v).identity = {7U, 12U, 1U}; });
        RefuseValue(wal, [&](auto& v) { image_base(v).identity.revision = 0U; });
        RefuseValue(wal, [&](auto& v) { image_base(v).identity.schema_version = 0U; });
        RefuseValue(wal, [&](auto& v) { image_base(v).image.kind = CommitOwnerNativeKind::kWalPrefix; });
        RefuseValue(wal, [&](auto& v) { image_base(v).image.bytes = ChunkImageFixedHeaderBytes(v.member.coverage.dims) - 1U; });
        auto retained_base = wal; log(retained_base).base = Retained(key); (void)Encode(retained_base);
        RefuseValue(retained_base, [](auto& v) { v.member.revision = 6U; });

        const auto raw = Encode(image);
        const auto parse = [&](std::span<const std::uint8_t> bytes) { return ParseCommitOwnerObservation(bytes, kScope, 2U, {member.group_id, 0U}); };
        RefuseByte(raw, 268U, 0U, parse); RefuseByte(raw, 268U, 4U, parse); RefuseByte(raw, 270U, 1U, parse);
        RefuseByte(Encode(wal), 269U, 3U, parse);
        Reject([&] { (void)ParseCommitOwnerObservation(raw, kScope, 2U, {{9U}, 0U}); });
        Reject([&] { (void)ParseCommitOwnerObservation(raw, kScope, 1U, {member.group_id, 0U}); });
        auto later = member; later.generation = 2U; ValidateCommitOwnerObservationBinding(image, later);
        auto foreign = member; foreign.scope.owner_id[0U] ^= 1U; Reject([&] { ValidateCommitOwnerObservationBinding(image, foreign); });
        auto other = member; other.coverage.coord.x ^= 1; Reject([&] { ValidateCommitOwnerObservationBinding(image, other); });
    }
}

template <class Source> Source& As(CommitOwnerEdge& edge) { return std::get<Source>(edge.source); }
void EdgeRefusalsAndBounds() {
    const auto edges = Edges();
    for (const auto& edge : edges) {
        RefuseValue(edge, [](auto& v) { v.generation = 0U; });
        RefuseValue(edge, [](auto& v) { v.group_id = {}; });
        RefuseValue(edge, [](auto& v) { v.decision_sha256 = {}; });
        const auto key = EncodeCommitOwnerEdgeKey(edge.source); const auto raw = Encode(edge);
        const auto parse = [&](std::span<const std::uint8_t> bytes) { return ParseCommitOwnerEdge(bytes, kScope, 2U, key); };
        RefuseByte(raw, 112U, 0U, parse); RefuseByte(raw, 112U, 7U, parse);
        for (std::size_t at = 113U; at < 120U; ++at) RefuseByte(raw, at, 1U, parse);
        Reject([&] { (void)ParseCommitOwnerEdge(raw, kScope, 1U, key); });
        for (const auto& other : edges)
            if (other.source.index() != edge.source.index()) Reject([&] { (void)ParseCommitOwnerEdge(raw, kScope, 2U, EncodeCommitOwnerEdgeKey(other.source)); });
    }
    const auto& producer = edges[0U]; const auto& covered = edges[1U]; const auto& rollback = edges[2U];
    const auto& feed = edges[3U]; const auto& id = edges[4U]; const auto& transition = edges[5U];
    RefuseValue(producer, [](auto& v) { As<CommitOwnerProducerEdge>(v).value.kind = CommitOwnerObjectKind::kGroupValue; });
    RefuseValue(producer, [](auto& v) { As<CommitOwnerProducerEdge>(v).coverage.dims = 4U; });
    RefuseValue(covered, [](auto& v) { As<CommitOwnerCoveredEdge>(v).value.kind = CommitOwnerObjectKind::kOwnerEdge; });
    RefuseValue(covered, [](auto& v) { As<CommitOwnerCoveredEdge>(v).value.sha256 = {}; });
    RefuseValue(rollback, [](auto& v) { As<CommitOwnerRollbackEdge>(v).source_group = {}; });
    RefuseValue(rollback, [](auto& v) { As<CommitOwnerRollbackEdge>(v).source_group = v.group_id; });
    RefuseValue(rollback, [](auto& v) { As<CommitOwnerRollbackEdge>(v).source_decision_sha256 = {}; });
    RefuseValue(rollback, [](auto& v) { As<CommitOwnerRollbackEdge>(v).source_value.kind = CommitOwnerObjectKind::kCoverageValue; });
    RefuseValue(rollback, [](auto& v) { As<CommitOwnerRollbackEdge>(v).anchor.revision = 0U; });
    RefuseValue(rollback, [](auto& v) { As<CommitOwnerRollbackEdge>(v).coverage.coord.z = 1; });

    const auto slots = [](CommitOwnerEdge& v) -> CommitOwnerFeedEdge& { return As<CommitOwnerFeedEdge>(v); };
    RefuseValue(feed, [&](auto& v) { slots(v).slots.kind = CommitOwnerNativeKind::kImage; });
    RefuseValue(feed, [&](auto& v) { slots(v).slots.bytes = 39U; });
    auto bound = feed; slots(bound).slots.bytes = 1024U * 1024U; (void)Encode(bound);
    RefuseValue(bound, [&](auto& v) { ++slots(v).slots.bytes; });
    RefuseValue(feed, [&](auto& v) { slots(v).archive.kind = CommitOwnerNativeKind::kImage; });
    RefuseValue(feed, [&](auto& v) { slots(v).slot_name_bytes = 0U; });
    RefuseValue(feed, [&](auto& v) { slots(v).slot_name_bytes = 64U; });
    RefuseValue(feed, [&](auto& v) { slots(v).slot_name[0U] = 'A'; });
    RefuseValue(feed, [&](auto& v) { slots(v).slot_name[5U] = 'x'; });
    auto longest = feed; slots(longest).slot_name.fill('z'); slots(longest).slot_name_bytes = 63U;
    assert(ParseCommitOwnerEdge(Encode(longest), kScope, 2U, EncodeCommitOwnerEdgeKey(longest.source)) == longest);
    RefuseValue(feed, [&](auto& v) { slots(v).frame_sha256 = {}; });
    RefuseValue(feed, [&](auto& v) { --slots(v).frame_offset; });
    RefuseValue(feed, [&](auto& v) { slots(v).frame_bytes = 0U; });
    RefuseValue(feed, [&](auto& v) { slots(v).frame_offset = slots(v).archive.bytes + 1U; slots(v).frame_bytes = 1U; });
    auto frame = feed; slots(frame).frame_bytes = slots(frame).archive.bytes - slots(frame).frame_offset; (void)Encode(frame);
    RefuseValue(frame, [&](auto& v) { ++slots(v).frame_bytes; });

    const auto entry = [](CommitOwnerEdge& v) -> CommitOwnerIdEdge& { return As<CommitOwnerIdEdge>(v); };
    RefuseValue(id, [&](auto& v) { entry(v).inventory.kind = CommitOwnerObjectKind::kJournalInventory; });
    RefuseValue(id, [&](auto& v) { entry(v).inventory.bytes = 163U; });
    RefuseValue(id, [&](auto& v) { entry(v).generation = 0U; });
    RefuseValue(id, [&](auto& v) { entry(v).entry_offset = kCommitIdInventoryHeaderBytes - 1U; });
    RefuseValue(id, [&](auto& v) { entry(v).entry_bytes = 111U; });
    RefuseValue(id, [&](auto& v) { entry(v).entry_sha256 = {}; });
    RefuseValue(id, [&](auto& v) { entry(v).key_sha256 = {}; });
    auto whole = id; entry(whole).entry_bytes = entry(whole).inventory.bytes - 4U - entry(whole).entry_offset; (void)Encode(whole);
    RefuseValue(whole, [&](auto& v) { ++entry(v).entry_bytes; });
    RefuseValue(id, [&](auto& v) { entry(v).entry_offset = entry(v).inventory.bytes - 3U; });

    const auto plan = [](CommitOwnerEdge& v) -> CommitOwnerTransitionEdge& { return As<CommitOwnerTransitionEdge>(v); };
    RefuseValue(transition, [&](auto& v) { plan(v).source = {CommitOwnerObjectKind::kRoot, kCommitOwnerRootBytes, {9U}}; });
    RefuseValue(transition, [&](auto& v) { plan(v).source.bytes = 67U; });
    RefuseValue(transition, [&](auto& v) { plan(v).source.sha256 = {}; });
    RefuseValue(transition, [&](auto& v) { plan(v).source_generation = 0U; });
    RefuseValue(transition, [&](auto& v) { plan(v).source_generation = v.generation; });
    auto provenance = transition; plan(provenance).source.kind = CommitOwnerObjectKind::kRestoreProvenance; (void)Encode(provenance);

    const auto full = EncodeCommitOwnerEdgeKey(producer.source); Reject([&] { ValidateCommitOwnerEdgeKey(std::span(full).first(48U)); });
    for (const std::uint8_t cls : {std::uint8_t{0U}, std::uint8_t{7U}}) {
        auto key = EncodeCommitOwnerEdgeKey(producer.source); key[0U] = cls; Reject([&] { ValidateCommitOwnerEdgeKey(key); });
    }
    auto key = EncodeCommitOwnerEdgeKey(producer.source); key[17U] = 4U; Reject([&] { ValidateCommitOwnerEdgeKey(key); });
    key = EncodeCommitOwnerEdgeKey(rollback.source); std::fill_n(key.begin() + 1, 16U, 0U); Reject([&] { ValidateCommitOwnerEdgeKey(key); });
    for (const auto* edge : {&feed, &transition, &id}) {
        key = EncodeCommitOwnerEdgeKey(edge->source); std::fill_n(key.begin() + 1, 32U, 0U); Reject([&] { ValidateCommitOwnerEdgeKey(key); });
    }
    key = EncodeCommitOwnerEdgeKey(feed.source); key[40U] ^= 1U; ValidateCommitOwnerEdgeKey(key); key[41U] = 1U; Reject([&] { ValidateCommitOwnerEdgeKey(key); });
    key = EncodeCommitOwnerEdgeKey(id.source); key[33U] = 1U; Reject([&] { ValidateCommitOwnerEdgeKey(key); });
    key = EncodeCommitOwnerEdgeKey(rollback.source); key[25U] = 1U; Reject([&] { ValidateCommitOwnerEdgeKey(key); });
    key = EncodeCommitOwnerEdgeKey(producer.source); key[42U] = 1U; Reject([&] { ValidateCommitOwnerEdgeKey(key); });

    auto target = Group(); const auto member = Member(Key()); Journal(target).bytes = 500U;
    target.members = One(CommitOwnerTreeKind::kMembers, EncodeCommitOwnerMemberKey({member.group_id, 0U}), Ref(CommitOwnerObjectKind::kMemberValue, SerializeCommitOwnerMember(member)));
    for (const auto& edge : edges) {
        ValidateCommitOwnerEdgeBinding(edge, target);
        auto wrong = edge; wrong.scope.owner_id[0U] ^= 1U; Reject([&] { ValidateCommitOwnerEdgeBinding(wrong, target); });
        wrong = edge; wrong.generation = 3U; Reject([&] { ValidateCommitOwnerEdgeBinding(wrong, target); });
        wrong = edge; wrong.group_id[0U] ^= 1U;
        if (edge.source.index() == 2U) As<CommitOwnerRollbackEdge>(wrong).source_group = {6U, 1U};
        Reject([&] { ValidateCommitOwnerEdgeBinding(wrong, target); });
        wrong = edge; std::visit([](auto& source) {
            if constexpr (requires { source.target_ordinal; }) source.target_ordinal = 1U;
        }, wrong.source);
        if (edge.source.index() <= 2U) Reject([&] { ValidateCommitOwnerEdgeBinding(wrong, target); }); else ValidateCommitOwnerEdgeBinding(wrong, target);
    }

    auto coverage = ImageCoverage(Key()); const auto measured = [](const CommitOwnerCoverageValue& value) {
        return Ref(CommitOwnerObjectKind::kCoverageValue, Encode(value));
    };
    auto current = producer; As<CommitOwnerProducerEdge>(current).value = measured(coverage); ValidateCommitOwnerProducerSource(current, coverage);
    const auto producer_refusal = [&](auto change, const Where& where = Where::current()) {
        auto edge = current; auto value = coverage; change(edge, value);
        if (std::holds_alternative<CommitOwnerProducerEdge>(edge.source) && As<CommitOwnerProducerEdge>(edge).value == measured(coverage))
            As<CommitOwnerProducerEdge>(edge).value = measured(value);
        Reject([&] { ValidateCommitOwnerProducerSource(edge, value); }, where);
    };
    producer_refusal([&](auto& edge, auto&) { edge.source = CommitOwnerCoveredEdge{Key(), measured(coverage), 0U}; });
    producer_refusal([](auto& edge, auto&) { edge.scope.owner_id[0U] ^= 1U; });
    producer_refusal([](auto&, auto& value) { value.generation = 3U; value.producer->generation = 3U; });
    producer_refusal([](auto& edge, auto&) { ++As<CommitOwnerProducerEdge>(edge).coverage.coord.x; });
    producer_refusal([](auto&, auto& value) { value.producer.reset(); });
    producer_refusal([](auto&, auto& value) { value.producer->group_id[0U] ^= 1U; });
    producer_refusal([](auto&, auto& value) { value.producer->decision_sha256[0U] ^= 1U; });
    producer_refusal([](auto& edge, auto&) { As<CommitOwnerProducerEdge>(edge).target_ordinal = 1U; });

    auto holder = covered; As<CommitOwnerCoveredEdge>(holder).value = measured(coverage); ValidateCommitOwnerCoveredSource(holder, coverage, member);
    const auto covered_refusal = [&](auto change, const Where& where = Where::current()) {
        auto edge = holder; auto value = coverage; auto owner = member; change(edge, value, owner);
        if (std::holds_alternative<CommitOwnerCoveredEdge>(edge.source) && As<CommitOwnerCoveredEdge>(edge).value == measured(coverage))
            As<CommitOwnerCoveredEdge>(edge).value = measured(value);
        Reject([&] { ValidateCommitOwnerCoveredSource(edge, value, owner); }, where);
    };
    covered_refusal([&](auto& edge, auto&, auto&) { edge.source = CommitOwnerProducerEdge{Key(), measured(coverage), 0U}; });
    covered_refusal([](auto& edge, auto&, auto&) { edge.scope.owner_id[0U] ^= 1U; });
    covered_refusal([](auto&, auto& value, auto&) { value.generation = 3U; value.producer->generation = 3U; });
    covered_refusal([](auto& edge, auto&, auto&) { ++As<CommitOwnerCoveredEdge>(edge).coverage.coord.x; });
    auto different = coverage; different.image->sha256[0U] ^= 1U; Reject([&] { ValidateCommitOwnerCoveredSource(holder, different, member); });
    covered_refusal([](auto&, auto&, auto& owner) { owner.group_id[0U] ^= 1U; });
    covered_refusal([](auto&, auto&, auto& owner) { owner.decision_sha256[0U] ^= 1U; });
    covered_refusal([](auto&, auto&, auto& owner) { owner.ordinal = 1U; owner.member_count = 2U; });
    covered_refusal([](auto&, auto&, auto& owner) { ++owner.coverage.coord.x; });
    covered_refusal([](auto&, auto&, auto& owner) { owner.generation = 3U; });
    auto never = Coverage(Key()); auto zero = Member(Key(), CommitGroupMemberKind::kObserved); zero.revision = zero.commit_time_ms = zero.schema_version = 0U;
    auto zero_edge = covered; As<CommitOwnerCoveredEdge>(zero_edge).value = measured(never); ValidateCommitOwnerCoveredSource(zero_edge, never, zero);
    auto nonzero = Member(Key(), CommitGroupMemberKind::kObserved); Reject([&] { ValidateCommitOwnerCoveredSource(zero_edge, never, nonzero); });

    const std::array<std::uint8_t, 3U> principal{'a', 'b', 'c'};
    CommitOwnerIdKeyHasher hasher(3U, 2U); hasher.Principal(principal); hasher.Id(std::array<std::uint8_t, 2U>{0U, 255U});
    auto named = id; entry(named).key_sha256 = hasher.Finish(); entry(named).entry_bytes = 117U;
    const CommitIdEntry record{"abc", {0U, 255U}, named.group_id, named.decision_sha256, {8U}, 20U};
    ValidateCommitOwnerIdSource(named, record, entry(named).inventory, 1U, entry(named).entry_sha256);
    Reject([&] { ValidateCommitOwnerIdSource(producer, record, entry(named).inventory, 1U, entry(named).entry_sha256); });
    auto other_sha = entry(named).entry_sha256; other_sha[0U] ^= 1U;
    Reject([&] { ValidateCommitOwnerIdSource(named, record, entry(named).inventory, 1U, other_sha); });
    auto other_record = record; other_record.group_id[0U] ^= 1U;
    Reject([&] { ValidateCommitOwnerIdSource(named, other_record, entry(named).inventory, 1U, entry(named).entry_sha256); });
    other_record = record; other_record.decision_sha256[0U] ^= 1U;
    Reject([&] { ValidateCommitOwnerIdSource(named, other_record, entry(named).inventory, 1U, entry(named).entry_sha256); });
    auto longer = named; ++entry(longer).entry_bytes;
    Reject([&] { ValidateCommitOwnerIdSource(longer, record, entry(longer).inventory, 1U, entry(longer).entry_sha256); });
}

void IdKeyAndCounterBounds() {
    const auto largest = kMaxCommitIdInventoryBytes - 112U - 64U;
    { CommitOwnerIdKeyHasher bound(largest, 64U); (void)bound; }
    Reject([&] { CommitOwnerIdKeyHasher over(largest + 1U, 64U); });
    const std::array<std::uint8_t, 4U> bytes{'a', 'b', 'c', 'd'};
    CommitOwnerIdKeyHasher principal(3U, 1U); Reject([&] { principal.Principal(bytes); });
    principal.Principal(std::span(bytes).first(3U)); principal.Id(std::span(bytes).first(1U));
    Reject([&] { principal.Principal({}); }); Reject([&] { principal.Id(std::span(bytes).first(1U)); });
    (void)principal.Finish(); Reject([&] { principal.Id({}); }); Reject([&] { principal.Principal({}); });

    const CommitCounterRecord counter{kScope, {6U}, 3U, 3U}; (void)Encode(counter);
    RefuseValue(counter, [](auto& v) { v.exclusive_ceiling = 2U; });
    RefuseValue(counter, [](auto& v) { v.generation = 1U; v.exclusive_ceiling = 2U; });
    RefuseValue(counter, [](auto& v) { v.generation = 0U; v.exclusive_ceiling = 0U; });
    RefuseValue(counter, [](auto& v) { v.counter_owner = {}; });
    RefuseValue(counter, [](auto& v) { v.scope.directory_id = {}; });
    const auto raw = SerializeCommitCounter(counter);
    auto other = kScope; other.role = CommitOwnerRole::kStandalone; Reject([&] { (void)ParseCommitCounter(raw, other); });
    other = kScope; other.owner_id[0U] ^= 1U; Reject([&] { (void)ParseCommitCounter(raw, other); });
    Bytes longer(raw.begin(), raw.end()); longer.push_back(0U); Reject([&] { (void)ParseCommitCounter(longer, kScope); });
    for (const auto at : {7U, 80U, 91U}) { auto damaged = raw; damaged[at] = 1U; Crc(damaged); Reject([&] { (void)ParseCommitCounter(damaged, kScope); }); }
    auto damaged = raw; damaged.back() ^= 1U; Reject([&] { (void)ParseCommitCounter(damaged, kScope); });
    auto next = counter; next.generation = 4U; next.exclusive_ceiling = 4U; ValidateCommitCounterSuccessor(counter, next);
    auto moved = next; moved.scope.owner_id[0U] ^= 1U; Reject([&] { ValidateCommitCounterSuccessor(counter, moved); });
    CommitOwnerRoot root; root.scope = kScope; root.state = CommitOwnerState::kActive; root.generation = 2U;
    root.coverage_tree = Empty(CommitOwnerTreeKind::kCoverage).root; root.group_tree = Empty(CommitOwnerTreeKind::kGroups).root;
    root.journal_inventory = {CommitOwnerObjectKind::kJournalInventory, 128U, {1U}}; root.id_inventory = {CommitOwnerObjectKind::kIdInventory, 164U, {2U}};
    root.reference_context = {kScope.directory_id, {3U}, 1U}; root.reference_replay = {64U, {}, CommitRecordKind::kNone, {4U}};
    root.counter_owner = counter.counter_owner; root.counter_ceiling = 3U; ValidateCommitCounterBinding(root, counter);
    auto foreign = counter; foreign.scope = other; Reject([&] { ValidateCommitCounterBinding(root, foreign); });
}

void ExtremeRoundTrips() {
    constexpr auto low = std::numeric_limits<std::int64_t>::min(), high = std::numeric_limits<std::int64_t>::max();
    for (const bool three : {false, true}) {
        CommitOwnerCoverageKey key{{}, static_cast<std::uint8_t>(three ? 3U : 2U), {low, high, three ? low : 0}}; key.table_epoch.fill(0xFFU);
        const auto encoded = EncodeCommitOwnerCoverageKey(key); assert(DecodeCommitOwnerCoverageKey(encoded) == key);
        auto lower = key; lower.coord.y = high - 1; assert(EncodeCommitOwnerCoverageKey(lower) < encoded);
        auto value = Coverage(key); value.generation = kU64; value.state = CommitOwnerCoverageState::kImage; value.identity = {kU64, kU64, kU64};
        value.image = CommitOwnerNativeRef{CommitOwnerNativeKind::kImage, kU64 / 8U, {}}; value.image->sha256.fill(0xFFU);
        auto producer = Member(key); producer.generation = kU64; producer.group_id.fill(0xFFU); producer.decision_sha256.fill(0xFFU);
        producer.ordinal = kU64 - 1U; producer.member_count = kU64; producer.revision = producer.commit_time_ms = producer.schema_version = kU64;
        value.producer = producer;
        const auto raw = Encode(value); const auto parsed = ParseCommitOwnerCoverage(raw, kScope, kU64, key);
        assert(parsed == value && Encode(parsed) == raw);

        auto observed = producer; observed.kind = CommitGroupMemberKind::kObserved;
        CommitOwnerObservation observation{kScope, kU64, observed, CommitOwnerObservationWal{CommitOwnerImageBase{{kU64, kU64, kU64}, *value.image},
            {CommitOwnerNativeKind::kWalPrefix, kU64 / 8U, {1U}}}};
        const auto observation_raw = Encode(observation);
        const auto observation_parsed = ParseCommitOwnerObservation(observation_raw, kScope, kU64, {observed.group_id, observed.ordinal});
        assert(observation_parsed == observation && Encode(observation_parsed) == observation_raw);
    }
    auto group = Group(); group.generation = kU64; group.group_id.fill(0xFFU); group.decision_sha256.fill(0xFFU);
    group.source = CommitOwnerJournalRecord{{kScope.directory_id, {6U}, kU64}, CommitRecordKind::kDecision, kU64 - kMaxCommitDecisionBytes,
        kMaxCommitDecisionBytes, group.decision_sha256};
    group.durability = CommitOwnerDurabilityProof::kReceipt;
    group.receipt = CommitOwnerJournalRecord{{kScope.directory_id, {6U}, kU64}, CommitRecordKind::kReceipt, kU64 - kCommitReceiptBytes, kCommitReceiptBytes, {8U}};
    const auto group_raw = Encode(group); const auto group_parsed = ParseCommitOwnerGroup(group_raw, kScope, kU64, group.group_id);
    assert(group_parsed == group && Encode(group_parsed) == group_raw);

    auto edges = Edges();
    As<CommitOwnerRollbackEdge>(edges[2U]).source_ordinal = kU64; As<CommitOwnerRollbackEdge>(edges[2U]).target_ordinal = kU64;
    auto& feed = As<CommitOwnerFeedEdge>(edges[3U]); feed.source_ordinal = kU64; feed.archive.bytes = kU64 / 8U;
    feed.frame_offset = kU64 / 8U - 1U; feed.frame_bytes = 1U;
    auto& id = As<CommitOwnerIdEdge>(edges[4U]); id.inventory.bytes = kMaxCommitIdInventoryBytes; id.generation = kU64;
    id.entry_offset = kMaxCommitIdInventoryBytes - 4U - 112U; id.entry_bytes = 112U;
    As<CommitOwnerTransitionEdge>(edges[5U]).participant_ordinal = kU64; As<CommitOwnerTransitionEdge>(edges[5U]).source_generation = kU64 - 1U;
    for (auto& edge : edges) {
        edge.generation = kU64; const auto key = EncodeCommitOwnerEdgeKey(edge.source); const auto raw = Encode(edge);
        const auto parsed = ParseCommitOwnerEdge(raw, kScope, kU64, key); assert(parsed == edge && Encode(parsed) == raw);
    }
    const auto rollback = EncodeCommitOwnerEdgeKey(edges[2U].source);
    assert(std::all_of(rollback.begin() + 17, rollback.begin() + 25, [](auto byte) { return byte == 0xFFU; }));
    const auto transition = EncodeCommitOwnerEdgeKey(edges[5U].source);
    assert(std::all_of(transition.begin() + 33, transition.begin() + 41, [](auto byte) { return byte == 0xFFU; }));
}

// Any accepted single-byte change must re-encode to exactly the accepted bytes.
template <class Parse> void Canonical(const Bytes& raw, Parse parse, bool envelope = true) {
    for (std::size_t at = 0U; at + 4U < raw.size(); ++at) {
        if (envelope && at >= 60U && at < 64U) continue;
        for (const std::uint8_t mask : {std::uint8_t{0x01U}, std::uint8_t{0x80U}, std::uint8_t{0xFFU}}) {
            auto changed = raw; changed[at] ^= mask; Crc(changed, envelope);
            try {
                const auto value = parse(changed); const auto again = Encode(value);
                if (again != changed) std::fprintf(stderr, "non-canonical bytes accepted at %zu\n", at);
                assert(again == changed);
            } catch (const std::invalid_argument&) {}
        }
    }
}
void CanonicalEncodings() {
    for (const bool three : {false, true}) {
        const auto key = Key(three);
        const auto parse = [&](std::span<const std::uint8_t> bytes) { return ParseCommitOwnerCoverage(bytes, kScope, 2U, key); };
        for (const auto& value : {Coverage(key), ImageCoverage(key), RetainedCoverage(key), ForgottenCoverage(key)}) Canonical(Encode(value), parse);
        const auto member = Member(key, CommitGroupMemberKind::kObserved);
        const auto observation_parse = [&](std::span<const std::uint8_t> bytes) { return ParseCommitOwnerObservation(bytes, kScope, 2U, {member.group_id, 0U}); };
        CommitOwnerObservation observation{kScope, 2U, member, CommitOwnerNativeRef{CommitOwnerNativeKind::kImage, 1024U, {9U}}};
        Canonical(Encode(observation), observation_parse);
        observation.source = Retained(key); Canonical(Encode(observation), observation_parse);
        observation.source = CommitOwnerObservationWal{CommitOwnerImageBase{{6U, 10U, 1U}, {CommitOwnerNativeKind::kImage, 1024U, {9U}}},
            {CommitOwnerNativeKind::kWalPrefix, 200U, {8U}}};
        Canonical(Encode(observation), observation_parse);
    }
    auto group = Group();
    const auto group_parse = [&](std::span<const std::uint8_t> bytes) { return ParseCommitOwnerGroup(bytes, kScope, 2U, group.group_id); };
    Canonical(Encode(group), group_parse);
    group.source = CommitOwnerSealSource{608U, {7U}}; group.durability = CommitOwnerDurabilityProof::kReceipt;
    group.receipt = CommitOwnerJournalRecord{{kScope.directory_id, {6U}, 1U}, CommitRecordKind::kReceipt, 220U, kCommitReceiptBytes, {8U}};
    Canonical(Encode(group), group_parse);
    for (const auto& edge : Edges()) {
        const auto key = EncodeCommitOwnerEdgeKey(edge.source);
        Canonical(Encode(edge), [&](std::span<const std::uint8_t> bytes) { return ParseCommitOwnerEdge(bytes, kScope, 2U, key); });
    }
    Canonical(Encode(CommitCounterRecord{kScope, {6U}, 2U, 128U}), [](std::span<const std::uint8_t> bytes) { return ParseCommitCounter(bytes, kScope); }, false);
}
}  // namespace

int main() {
    owner_artifact_allocation::CalibrateAligned();
    CoverageAndOwnedProofs(); GroupAndObservationBindings(); EdgeSetsAndExactSources();
    IdKeyStreamingAndIncarnation(); CounterOwnershipAndExhaustion(); SparseHeaderRefusal();
    CoverageRefusalsAndBounds(); SetCommitmentBounds(); GroupRefusalsAndBounds(); ObservationRefusalsAndBounds();
    EdgeRefusalsAndBounds(); IdKeyAndCounterBounds(); ExtremeRoundTrips(); CanonicalEncodings();
}
