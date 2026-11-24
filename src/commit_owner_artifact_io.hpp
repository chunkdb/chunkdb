#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <span>

#include "commit_group_journal.hpp"
#include "commit_group_owner_format.hpp"

namespace chunkdb {

class ProcessLock;

struct CommitOwnerRootArtifact {
    CommitOwnerObjectRef reference;
    CommitOwnerRoot value;
};
struct CommitOwnerPageArtifact {
    CommitOwnerObjectRef reference;
    CommitOwnerPage value;
};
struct CommitJournalInventoryArtifact {
    CommitOwnerObjectRef reference;
    CommitJournalInventory value;
};
struct CommitIdInventoryArtifact {
    CommitOwnerObjectRef reference;
    CommitIdInventoryHeader header;
    CommitIdInventory value;
};
// A durable private object, not permission to select a root or authorize a group.
struct CommitOwnerStagedArtifact {
    CommitOwnerObjectRef reference;
    bool installed = false;
};

// One pinned journal generation below chunkdb.groups.journal. A writable file
// appends only at an end it admitted itself, through a tail-free Scan or its
// own appends; the exclusive writer lock must outlive it. A failed write or
// sync fences this object; only a fresh scan from disk may admit a new end.
class CommitGroupJournalFile {
  public:
    ~CommitGroupJournalFile();
    CommitGroupJournalFile(CommitGroupJournalFile&&) noexcept;
    CommitGroupJournalFile& operator=(CommitGroupJournalFile&&) noexcept;

    [[nodiscard]] const CommitJournalContext& context() const noexcept;
    [[nodiscard]] bool writable() const noexcept;
    [[nodiscard]] std::uint64_t MeasuredBytes() const;
    // Refuses an extent change during the scan; a tail-free result admits its end.
    [[nodiscard]] CommitJournalScan Scan(const CommitJournalInventory& certified,
        std::span<const std::uint64_t> cursors = {});
    // Appends one completely admitted record at the admitted end without
    // syncing it, and returns the new end.
    std::uint64_t Append(std::span<const std::uint8_t> record);
    // Syncs this pinned generation.
    void Sync();

  private:
    friend class CommitOwnerArtifactIo;
    struct Impl;
    explicit CommitGroupJournalFile(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

class CommitOwnerArtifactIo {
  public:
    CommitOwnerArtifactIo(const std::filesystem::path& directory, const CommitOwnerScope& scope);
    // The exclusive lock must outlive this object; shared-writer mode refuses.
    CommitOwnerArtifactIo(const std::filesystem::path& directory, const CommitOwnerScope& scope,
        const ProcessLock& exclusive_lock, bool allow_multiple_processes);
    ~CommitOwnerArtifactIo();
    CommitOwnerArtifactIo(const CommitOwnerArtifactIo&) = delete;
    CommitOwnerArtifactIo& operator=(const CommitOwnerArtifactIo&) = delete;

    [[nodiscard]] std::optional<CommitOwnerRootArtifact> ReadCanonicalRoot() const;
    [[nodiscard]] CommitOwnerRootArtifact ReadRoot(const CommitOwnerObjectRef& reference,
        std::optional<std::uint64_t> expected_generation = {}) const;
    [[nodiscard]] CommitOwnerPageArtifact ReadPage(const CommitOwnerObjectRef& reference,
        CommitOwnerTreeKind tree, std::uint64_t selecting_generation) const;
    [[nodiscard]] CommitJournalInventoryArtifact ReadJournalInventory(const CommitOwnerObjectRef& reference) const;
    [[nodiscard]] CommitIdInventoryArtifact ReadIdInventory(const CommitOwnerObjectRef& reference,
        const CommitJournalContext& expected) const;
    [[nodiscard]] CommitOwnerStagedArtifact StageInactive();
    [[nodiscard]] CommitOwnerStagedArtifact StageRoot(const CommitOwnerRoot& value);
    [[nodiscard]] CommitOwnerStagedArtifact StagePage(const CommitOwnerPage& value);
    [[nodiscard]] CommitOwnerStagedArtifact StageJournalInventory(const CommitJournalInventory& value);
    [[nodiscard]] CommitOwnerStagedArtifact StageIdInventory(const CommitIdInventory& value);

    // Writer only: creates, syncs and pins a new journal generation holding
    // only its header; an existing generation is refused, never adopted.
    [[nodiscard]] CommitGroupJournalFile CreateJournal(const CommitJournalContext& context);
    // Writable exactly when this is a writer view.
    [[nodiscard]] CommitGroupJournalFile OpenJournal(const CommitJournalContext& context) const;

    // Writer only: publishes the fixed generation-one INACTIVE root without
    // replacement, or admits an identical existing root, and syncs it and the
    // root directory. It never accepts another root. Creation and ENABLE FEATURE
    // are its only intended callers; this build has none.
    [[nodiscard]] CommitOwnerStagedArtifact InstallCanonicalInactive();
    // Writer only: the same admission and syncs for an existing root; refuses when absent.
    [[nodiscard]] CommitOwnerStagedArtifact SettleCanonicalInactive();

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace chunkdb
