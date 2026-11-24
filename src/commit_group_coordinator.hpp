#pragma once

#include <filesystem>
#include <optional>

#include "commit_group_journal.hpp"
#include "commit_owner_artifact_io.hpp"

namespace chunkdb {

class ProcessLock;

// A staged ACTIVE root object that names an empty journal generation; never canonical.
struct CommitGroupActivation {
    CommitOwnerObjectRef root;
    CommitOwnerRoot value;
};

// Facts admitted for one ACTIVE root; scan.cursors holds the ID cursor, then the reference cursor.
struct CommitGroupSelection {
    CommitJournalInventoryArtifact journal;
    CommitIdInventoryArtifact ids;
    CommitJournalScan scan;
};

// The lazy owner of one data directory's or standalone store's commit group
// state. It is serialized by its caller and nothing in the server constructs it
// in this build: commit groups stay inactive, and no path makes a canonical
// ACTIVE root. The owner-transition permit arrives with the first ACTIVE
// mutation.
class CommitGroupCoordinator {
  public:
    // Read-only view.
    CommitGroupCoordinator(const std::filesystem::path& directory, const CommitOwnerScope& scope);
    // Writer; the exclusive lock must outlive the coordinator.
    CommitGroupCoordinator(const std::filesystem::path& directory, const CommitOwnerScope& scope,
        const ProcessLock& lock, bool allow_multiple_processes);

    [[nodiscard]] const CommitOwnerScope& scope() const noexcept { return scope_; }
    [[nodiscard]] bool writable() const noexcept { return writable_; }

    // The canonical root as facts; an INACTIVE root must be generation one.
    [[nodiscard]] std::optional<CommitOwnerRootArtifact> ReadOwner() const;
    // Admits the selections of an ACTIVE root without changing files; refuses
    // PENDING roots, nonempty trees and retained ID entries.
    [[nodiscard]] CommitGroupSelection AdmitSelection(const CommitOwnerRoot& root) const;

    // Installs or re-syncs the fixed generation-one INACTIVE root. Only
    // creation and ENABLE FEATURE may call this, before their manifest claims
    // the feature; late initialization of an existing directory is forbidden.
    // This build has no production caller.
    CommitOwnerStagedArtifact InstallInactiveOwner();
    // Re-syncs the canonical INACTIVE root, then stages and syncs a fresh empty
    // journal generation, empty trees, both inventories and a generation-two
    // ACTIVE root object. The canonical root is never changed.
    [[nodiscard]] CommitGroupActivation StageActivation();
    // The INACTIVE-to-ACTIVE handover check: the canonical root is still the
    // generation-one INACTIVE root, and the candidate is its admitted empty
    // successor. It proves admission, not durability.
    void ValidateActivation(const CommitGroupActivation& candidate) const;

  private:
    CommitOwnerScope scope_;
    bool writable_ = false;
    CommitOwnerArtifactIo io_;
};

}  // namespace chunkdb
