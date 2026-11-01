#pragma once

#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "commit_owner_artifact_io.hpp"
#include "store_manifest.hpp"

namespace chunkdb {

class TableCatalog;
class CommitOwnerStoreAdmission;
class CommitOwnerDirectoryContext;
struct BackupRecord;

class CommitOwnerRestoreOwnershipError final : public std::runtime_error {
  private:
    friend class CommitOwnerArtifactIo;
    friend class CommitOwnerBootstrap;
    explicit CommitOwnerRestoreOwnershipError(const char* reason)
        : std::runtime_error(std::string("restore private directory ownership is uncertain: ") + reason) {}
};

inline constexpr std::string_view kCommitOwnerBootstrapName = "chunkdb.bootstrap.pending";
inline constexpr std::size_t kCommitOwnerBootstrapHeaderBytes = 64U;
inline constexpr std::size_t kMaxCommitOwnerBootstrapBytes = kStoreManifestMaxSize + 68U;
enum class CommitOwnerBootstrapKind : std::uint8_t { kDirectory = 1U, kStandalone = 2U };
struct CommitOwnerBootstrapHeader {
    CommitOwnerBootstrapKind kind = CommitOwnerBootstrapKind::kDirectory;
    std::uint64_t bytes = 0U;
    std::uint64_t manifest_bytes = 0U;
    crypto::Sha256Digest manifest_sha256{};
};
struct CommitOwnerBootstrapDecision {
    CommitOwnerBootstrapKind kind = CommitOwnerBootstrapKind::kDirectory;
    std::vector<std::uint8_t> manifest;
    friend bool operator==(const CommitOwnerBootstrapDecision&, const CommitOwnerBootstrapDecision&) = default;
};
[[nodiscard]] CommitOwnerBootstrapHeader InspectCommitOwnerBootstrapHeader(std::span<const std::uint8_t> header);
[[nodiscard]] std::vector<std::uint8_t> SerializeCommitOwnerBootstrapDecision(const CommitOwnerBootstrapDecision& decision);
[[nodiscard]] CommitOwnerBootstrapDecision ParseCommitOwnerBootstrapDecision(std::span<const std::uint8_t> bytes);

// A single constructor admission, not permission for a later grouped write.
class CommitOwnerContext {
  private:
    friend class CommitOwnerBootstrap;
    friend class CommitOwnerDirectoryContext;
    CommitOwnerContext(std::shared_ptr<const CommitOwnerDirectoryContext> owner,
        std::shared_ptr<CommitOwnerArtifactIo> table, StoreId epoch);
    [[nodiscard]] std::unique_ptr<CommitOwnerStoreAdmission> Consume(const std::filesystem::path& directory) const;
    std::shared_ptr<const CommitOwnerDirectoryContext> owner_;
    StoreId epoch_{};
    mutable std::mutex mutex_;
    mutable std::shared_ptr<CommitOwnerArtifactIo> table_;
    mutable bool consumed_ = false;
};

class CommitOwnerStoreAdmission {
  public:
    [[nodiscard]] const StoreManifest& manifest() const noexcept { return manifest_; }
    [[nodiscard]] StoreManifest ReadCurrentManifest() const;
    [[nodiscard]] const std::shared_ptr<const CommitOwnerDirectoryContext>& owner() const noexcept { return owner_; }
  private:
    friend class CommitOwnerContext;
    friend class CommitOwnerBootstrap;
    CommitOwnerStoreAdmission(std::shared_ptr<const CommitOwnerDirectoryContext> owner, StoreId epoch);
    void Validate(const std::filesystem::path& directory);
    std::shared_ptr<const CommitOwnerDirectoryContext> owner_;
    std::shared_ptr<CommitOwnerArtifactIo> table_;
    StoreId epoch_{};
    StoreManifest manifest_;
};

class CommitOwnerDirectoryContext : public std::enable_shared_from_this<CommitOwnerDirectoryContext> {
  public:
    [[nodiscard]] const CommitOwnerScope& scope() const noexcept { return scope_; }
    void RequireInactive() const;
    [[nodiscard]] std::shared_ptr<const CommitOwnerContext> ForTable(std::string_view name) const;
  private:
    friend class CommitOwnerBootstrap;
    friend class CommitOwnerStoreAdmission;
    friend class TableCatalog;
    CommitOwnerDirectoryContext(CommitOwnerScope scope, std::shared_ptr<CommitOwnerArtifactIo> root);
    [[nodiscard]] std::shared_ptr<const CommitOwnerContext> ForStagedTable(std::string_view operation,
        const StoreId& expected_epoch) const;
    CommitOwnerScope scope_;
    std::shared_ptr<CommitOwnerArtifactIo> root_;
};

class CommitOwnerBootstrap {
  public:
    [[nodiscard]] static std::optional<CommitOwnerBootstrapDecision> ReadPending(const std::filesystem::path& directory);
    [[nodiscard]] static std::optional<StoreManifest> PendingStandaloneManifest(const std::filesystem::path& directory,
        AccessMode access_mode);
    [[nodiscard]] static std::shared_ptr<const CommitOwnerDirectoryContext> AdmitDirectory(
        const std::filesystem::path& directory, AccessMode access_mode = AccessMode::kReadOnly);
    [[nodiscard]] static std::unique_ptr<CommitOwnerStoreAdmission> BeginStoreAdmission(const StoreConfig& config);
    [[nodiscard]] static StoreManifest InitializeStandalone(const std::filesystem::path& directory,
        StoreManifest proposal, const Geometry& geometry, const ProcessLock& lock);
    [[nodiscard]] static DataDirManifest InitializeDirectory(const std::filesystem::path& directory,
        const ProcessLock& lock);
    static void CompleteFeatureOwner(const std::filesystem::path& directory, const ProcessLock& lock);
    // Read-only facts for metadata preflight; never returns a serving ticket.
    static void ValidateDirectoryScopes(const std::filesystem::path& directory,
        AccessMode access_mode = AccessMode::kReadOnly);
    static void ValidateManagedDirectories(const std::filesystem::path& directory);
    [[nodiscard]] static std::vector<std::uint8_t> ReadInactiveRoot(const std::filesystem::path& directory,
        const CommitOwnerScope& scope);
    [[nodiscard]] static DataDirManifest RewriteInactiveRestore(const std::filesystem::path& private_copy,
        const BackupRecord& admitted_backup);
    [[nodiscard]] static std::optional<StoreId> UnpublishedEmptyIdentity(const std::filesystem::path& directory,
        std::uint8_t dims);
};

}  // namespace chunkdb
