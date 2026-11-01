#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "commit_group_owner_format.hpp"

namespace chunkdb {

class ProcessLock;
class CommitOwnerContext;
class CommitOwnerDirectoryContext;
class CommitOwnerBootstrap;
class CommitOwnerStoreAdmission;

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
    friend class CommitOwnerContext;
    friend class CommitOwnerDirectoryContext;
    friend class CommitOwnerBootstrap;
    friend class CommitOwnerStoreAdmission;
    struct Impl;
    explicit CommitOwnerArtifactIo(std::unique_ptr<Impl> impl);
    [[nodiscard]] std::shared_ptr<CommitOwnerArtifactIo> PinDirectories(std::span<const std::string> names) const;
    [[nodiscard]] bool MatchesDirectory(const std::filesystem::path& directory) const;
    [[nodiscard]] std::vector<std::string> CatalogTableNames() const;
    [[nodiscard]] std::optional<std::vector<std::uint8_t>> ReadManifestBytes(bool directory) const;
    [[nodiscard]] static std::shared_ptr<CommitOwnerArtifactIo> OpenBootstrapDirectory(const std::filesystem::path& directory);
    [[nodiscard]] std::optional<std::vector<std::uint8_t>> ReadBootstrapBytes() const;
    void PublishBootstrapBytes(std::span<const std::uint8_t> bytes);
    void SyncFeatureDecision(std::span<const std::uint8_t> bytes);
    void PublishInactive();
    void RequireRestoreGuard() const;
    void RewriteRestoredInactive(const CommitOwnerScope& source_scope, const CommitOwnerScope& destination_scope);
    [[nodiscard]] std::optional<std::vector<std::uint8_t>> ReadEmptyInventoryBytes() const;
    void RequireNoPendingMetadata() const;
    std::unique_ptr<Impl> impl_;
};

}  // namespace chunkdb
