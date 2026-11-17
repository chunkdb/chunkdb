#pragma once

#include <filesystem>
#include <memory>
#include <optional>

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
// A durable private object, not permission to select a root or authorize a group.
struct CommitOwnerStagedArtifact {
    CommitOwnerObjectRef reference;
    bool installed = false;
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
    [[nodiscard]] CommitOwnerStagedArtifact StageInactive();
    [[nodiscard]] CommitOwnerStagedArtifact StageRoot(const CommitOwnerRoot& value);
    [[nodiscard]] CommitOwnerStagedArtifact StagePage(const CommitOwnerPage& value);

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace chunkdb
