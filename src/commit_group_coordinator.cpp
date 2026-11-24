#include "commit_group_coordinator.hpp"

#include <stdexcept>
#include <string>

#include "process_lock.hpp"
#include "store_manifest.hpp"

namespace chunkdb {
namespace {

[[noreturn]] void Fail(const std::string& reason) { throw std::runtime_error("commit group coordinator: " + reason); }

CommitOwnerRoot Inactive(const CommitOwnerScope& scope) {
    CommitOwnerRoot root; root.scope = scope; return root;
}
CommitOwnerObjectRef InactiveReference(const CommitOwnerScope& scope) {
    const auto bytes = SerializeCommitOwnerRoot(Inactive(scope));
    return {CommitOwnerObjectKind::kRoot, bytes.size(), crypto::Sha256Of(bytes)};
}
CommitOwnerPage EmptyPage(const CommitOwnerScope& scope, CommitOwnerTreeKind tree, std::uint64_t generation) {
    CommitOwnerPage page; page.scope = scope; page.generation = generation; page.tree = tree; return page;
}

}  // namespace

CommitGroupCoordinator::CommitGroupCoordinator(const std::filesystem::path& directory, const CommitOwnerScope& scope)
    : scope_(scope), writable_(false), io_(directory, scope) {}

CommitGroupCoordinator::CommitGroupCoordinator(const std::filesystem::path& directory, const CommitOwnerScope& scope,
    const ProcessLock& lock, bool allow_multiple_processes)
    : scope_(scope), writable_(true), io_(directory, scope, lock, allow_multiple_processes) {}

std::optional<CommitOwnerRootArtifact> CommitGroupCoordinator::ReadOwner() const {
    auto owner = io_.ReadCanonicalRoot();
    // Only bootstrap creates INACTIVE, and no successor transition yields it.
    if (owner && owner->value.state == CommitOwnerState::kInactive && owner->value.generation != 1U)
        Fail("INACTIVE owner root generation is not one");
    return owner;
}

CommitGroupSelection CommitGroupCoordinator::AdmitSelection(const CommitOwnerRoot& root) const {
    if (root.scope != scope_) Fail("owner root names another scope");
    if (root.state == CommitOwnerState::kPending) Fail("pending owner replacement is not admitted by this build");
    if (root.state != CommitOwnerState::kActive) Fail("owner root has no selections");
    // The child, value and plan codecs are missing, so only empty trees are admitted.
    for (const auto tree : {CommitOwnerTreeKind::kCoverage, CommitOwnerTreeKind::kGroups}) {
        const auto& reference = tree == CommitOwnerTreeKind::kCoverage ? root.coverage_tree : root.group_tree;
        const auto page = io_.ReadPage(reference, tree, root.generation);
        ValidateCommitOwnerRootPage(root, tree, page.value, page.reference);
        if (page.value.node != CommitOwnerNodeKind::kEmpty) Fail("nonempty owner trees are not admitted by this build");
    }
    CommitGroupSelection selection{io_.ReadJournalInventory(root.journal_inventory), {}, {}};
    const auto& context = selection.journal.value.context;
    // The named generation stays pinned while both cursors are admitted.
    auto journal = io_.OpenJournal(context);
    selection.ids = io_.ReadIdInventory(root.id_inventory, context);
    // ID entries are bound to decisions only by the ID work, so none is admitted yet.
    if (selection.ids.header.entry_count != 0U) Fail("retained ID entries are not admitted by this build");
    const std::uint64_t cursors[] = {selection.ids.header.replay.bytes, root.reference_replay.bytes};
    selection.scan = journal.Scan(selection.journal.value, cursors);
    ValidateCommitOwnerSelections(root, {selection.journal.reference, selection.journal.value, selection.scan.certified,
        selection.ids.reference, selection.ids.header, selection.scan.cursors[0], selection.scan.cursors[1]});
    // No native counter format exists yet, so only a zero counter owner is admitted.
    ValidateCommitOwnerCounter(root, {}, 0U);
    return selection;
}

CommitOwnerStagedArtifact CommitGroupCoordinator::InstallInactiveOwner() {
    if (!writable_) Fail("read-only coordinator cannot install an owner");
    return io_.InstallCanonicalInactive();
}

CommitGroupActivation CommitGroupCoordinator::StageActivation() {
    if (!writable_) Fail("read-only coordinator cannot stage an activation");
    const auto owner = ReadOwner();
    if (!owner) Fail("canonical owner is missing");
    if (owner->value != Inactive(scope_)) Fail("canonical owner is not INACTIVE");
    // A visible INACTIVE root may be left by an installer that failed before its syncs.
    (void)io_.SettleCanonicalInactive();

    const CommitJournalContext context{scope_.directory_id, NewStoreId(), 1U};
    const auto header = SerializeCommitJournalHeader(context);
    const CommitJournalCut empty{kCommitJournalHeaderBytes, {}, CommitRecordKind::kNone, crypto::Sha256Of(header)};
    CommitOwnerRoot value;
    value.scope = scope_;
    value.generation = 2U;
    value.state = CommitOwnerState::kActive;
    // The created file is released at once, so no writable journal handle outlives staging.
    (void)io_.CreateJournal(context);
    value.coverage_tree = io_.StagePage(EmptyPage(scope_, CommitOwnerTreeKind::kCoverage, value.generation)).reference;
    value.group_tree = io_.StagePage(EmptyPage(scope_, CommitOwnerTreeKind::kGroups, value.generation)).reference;
    value.journal_inventory = io_.StageJournalInventory({context, 1U, empty}).reference;
    value.id_inventory = io_.StageIdInventory({context, 1U, empty, {}}).reference;
    value.reference_context = context;
    value.reference_replay = empty;
    // Every selected object and its parent names are durable before this root names them.
    const CommitGroupActivation candidate{io_.StageRoot(value).reference, value};
    ValidateActivation(candidate);
    return candidate;
}

void CommitGroupCoordinator::ValidateActivation(const CommitGroupActivation& candidate) const {
    const auto owner = ReadOwner();
    if (!owner || owner->reference != InactiveReference(scope_)) Fail("canonical owner is not INACTIVE");
    const auto staged = io_.ReadRoot(candidate.root, 2U);
    if (staged.value != candidate.value) Fail("activation root object does not match its candidate");
    ValidateCommitOwnerRootSuccessor(owner->value, candidate.value);
    const auto selection = AdmitSelection(candidate.value);
    const auto& journal = selection.journal.value;
    if (journal.generation != 1U || journal.certified.bytes != kCommitJournalHeaderBytes ||
        !selection.scan.uncertified.empty() || selection.scan.measured_bytes != kCommitJournalHeaderBytes)
        Fail("activation journal is not empty");
    const auto& ids = selection.ids.header;
    if (ids.generation != 1U || ids.entry_count != 0U || ids.replay != journal.certified)
        Fail("activation ID inventory is not empty");
}

}  // namespace chunkdb
