#include "commit_owner_bootstrap.hpp"

#include <algorithm>
#include <cstdlib>
#include <sstream>
#include <stdexcept>

#include "checkpoint.hpp"
#include "backup.hpp"
#include "chunk_coordinate_index.hpp"
#include "chunk_store_internal.hpp"
#include "feature_activation_records.hpp"
#include "process_lock.hpp"

namespace chunkdb {
namespace {
[[noreturn]] void Bad(const char* reason) {
    throw std::runtime_error(std::string("commit owner bootstrap: ") + reason);
}
void Crash(const char* name) { if (ConsumeFailpointEnv(name)) std::_Exit(86); }

void CheckCreationNamespace(const std::filesystem::path& directory, CommitOwnerBootstrapKind kind,
    bool pending, bool manifested) {
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        const auto name = entry.path().filename().string();
        bool temporary = false;
        for (const auto* family : {"chunkdb.manifest.tmp.", "table.manifest.tmp.", "chunkdb.index.tmp."})
            temporary = temporary || name.starts_with(family);
        if (temporary) {
            if (!pending) Bad("creation temporary has no checked decision");
            continue;  // The pinned native validator checks target, spelling and bytes below.
        }
        if (pending && (name == kCommitOwnerBootstrapName || name == "chunkdb.groups.owner")) continue;
        if (manifested && name == (kind == CommitOwnerBootstrapKind::kDirectory ? kDataDirManifestFileName : kStoreManifestFileName)) continue;
        if (kind == CommitOwnerBootstrapKind::kStandalone && name == kChunkIndexFileName) continue;
        if (name == "tables" || name == ".chunkdb.staging" || name == ".chunkdb.dropped" || IsStoreEntryName(name))
            Bad("fresh owner namespace contains other owned state");
    }
}

void CheckEmptyInventory(const std::optional<std::vector<std::uint8_t>>& bytes,
    const Geometry& geometry, const StoreId& id, bool required) {
    if (!bytes) {
        if (required) Bad("committed creation lost its coordinate inventory");
        return;
    }
    std::istringstream input(std::string(bytes->begin(), bytes->end()));
    const auto header = ReadIndexInventory(input, bytes->size(), geometry, id);
    if (header.count != 0U || header.generation != 1U || header.retained_deletion_floor != 0U ||
        header.compaction_pending || header.compaction_through != 0U)
        Bad("creation coordinate inventory is not the original empty inventory");
}

void FinishDecision(const std::filesystem::path& directory) {
    Crash("CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_BEFORE_PENDING_REMOVE_ONCE");
    if (!std::filesystem::remove(directory / kCommitOwnerBootstrapName)) Bad("creation decision disappeared");
    SyncDirectoryPath(directory);
    Crash("CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_AFTER_PENDING_REMOVE_ONCE");
}

void RequireInitialRoot(const std::optional<CommitOwnerRootArtifact>& selected, const CommitOwnerScope& scope,
    bool claimed) {
    if (!selected) {
        if (claimed) Bad("committed creation lost its canonical owner");
        return;
    }
    CommitOwnerRoot expected; expected.scope = scope;
    if (selected->value != expected) Bad("canonical owner is not initial INACTIVE generation one");
}
}  // namespace

std::optional<CommitOwnerBootstrapDecision> CommitOwnerBootstrap::ReadPending(const std::filesystem::path& directory) {
    const auto path = std::filesystem::weakly_canonical(directory);
    const auto status = std::filesystem::symlink_status(path);
    if (status.type() == std::filesystem::file_type::not_found) return {};
    const auto view = CommitOwnerArtifactIo::OpenBootstrapDirectory(path);
    const auto bytes = view->ReadBootstrapBytes();
    if (!bytes) return {};
    return ParseCommitOwnerBootstrapDecision(*bytes);
}

std::optional<StoreManifest> CommitOwnerBootstrap::PendingStandaloneManifest(const std::filesystem::path& directory,
    AccessMode access_mode) {
    const auto pending = ReadPending(directory);
    if (!pending) return {};
    if (pending->kind != CommitOwnerBootstrapKind::kStandalone) Bad("directory creation decision used as a standalone store");
    if (access_mode == AccessMode::kReadOnly) Bad("pending creation requires a read-write open");
    const auto existing = ReadStoreManifest(directory);
    if (existing && SerializeStoreManifest(*existing) != pending->manifest) Bad("manifest differs from creation decision");
    return ParseStoreManifest(pending->manifest);
}

StoreManifest CommitOwnerBootstrap::InitializeStandalone(const std::filesystem::path& directory,
    StoreManifest proposal, const Geometry& geometry, const ProcessLock& lock) {
    const auto pending = ReadPending(directory);
    const auto existing = ReadStoreManifest(directory);
    if (existing && !pending) Bad("standalone creation already has a manifest");
    CommitOwnerBootstrapDecision decision;
    if (pending) {
        if (pending->kind != CommitOwnerBootstrapKind::kStandalone) Bad("creation kind differs");
        decision = *pending;
    } else {
        proposal.commit_owner = CommitOwnerScope{proposal.store_id, NewStoreId(), CommitOwnerRole::kStandalone};
        decision = {CommitOwnerBootstrapKind::kStandalone, SerializeStoreManifest(proposal)};
    }
    const auto target = ParseStoreManifest(decision.manifest);
    RequireOpenableFeatures(target.features, AccessMode::kReadWrite);
    if (!SameGeometry(target.geometry, geometry.config()) || target.schema != geometry.layout().schema() ||
        *target.chunk_schema != geometry.chunk_schema()) Bad("creation geometry or schemas differ");
    if (existing && SerializeStoreManifest(*existing) != decision.manifest) Bad("manifest differs from creation decision");
    CheckCreationNamespace(directory, decision.kind, pending.has_value(), existing.has_value());
    CommitOwnerArtifactIo writer(directory, *target.commit_owner, lock, false);
    if (pending) writer.ValidateBootstrapTemporaries(decision);
    CheckEmptyInventory(writer.ReadEmptyInventoryBytes(), geometry, target.store_id, existing.has_value());
    RequireInitialRoot(writer.ReadCanonicalRoot(), *target.commit_owner, existing.has_value());
    if (!pending) {
        Crash("CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_BEFORE_DECISION_ONCE");
    }
    writer.PublishBootstrapBytes(SerializeCommitOwnerBootstrapDecision(decision));
    if (!pending) Crash("CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_AFTER_DECISION_ONCE");
    if (!std::filesystem::exists(directory / kChunkIndexFileName) &&
        !ChunkCoordinateIndex::InitializeEmpty(directory, geometry, target.store_id)) Bad("creation inventory appeared");
    writer.PublishInactive();
    Crash("CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_AFTER_OWNER_ONCE");
    if (!existing && !PublishNewFile(StoreManifestPath(directory), decision.manifest,
            "CHUNKDB_FAILPOINT_CRASH_MANIFEST_BEFORE_PUBLISH_ONCE",
            "CHUNKDB_FAILPOINT_CRASH_MANIFEST_AFTER_PUBLISH_ONCE")) Bad("creation manifest appeared");
    // Also pay any previous uncertain manifest-file or parent-name durability.
    SyncFilePath(StoreManifestPath(directory)); SyncDirectoryPath(directory);
    FinishDecision(directory);
    return target;
}

std::optional<StoreId> CommitOwnerBootstrap::UnpublishedEmptyIdentity(const std::filesystem::path& directory,
    std::uint8_t dims) {
    const auto path = std::filesystem::weakly_canonical(directory);
    if (std::filesystem::symlink_status(path).type() == std::filesystem::file_type::not_found) return {};
    auto view = CommitOwnerArtifactIo::OpenBootstrapDirectory(path);
    const auto bytes = view->ReadEmptyInventoryBytes();
    if (!bytes) return {};
    if (bytes->size() < 24U) Bad("empty coordinate inventory lacks its identity");
    StoreId id; std::copy_n(bytes->begin() + 8, id.size(), id.begin());
    if (id == StoreId{}) Bad("empty coordinate inventory has a zero table identity");
    GeometryConfig config{1U, 1U, 1U, 1U, 1U}; config.dims = dims;
    CheckEmptyInventory(bytes, Geometry(config), id, true);
    CheckCreationNamespace(path, CommitOwnerBootstrapKind::kStandalone, false, false);
    return id;
}

DataDirManifest CommitOwnerBootstrap::InitializeDirectory(const std::filesystem::path& directory,
    const ProcessLock& lock) {
    const auto pending = ReadPending(directory);
    const auto existing = ReadDataDirManifest(directory);
    if (existing && !pending) Bad("directory creation already has a manifest");
    CommitOwnerBootstrapDecision decision;
    if (pending) {
        if (pending->kind != CommitOwnerBootstrapKind::kDirectory) Bad("creation kind differs");
        decision = *pending;
    } else {
        DataDirManifest target{.features = kKnownFeatures, .data_dir_id = NewStoreId(), .options = {}};
        target.commit_owner = CommitOwnerScope{target.data_dir_id, NewStoreId(), CommitOwnerRole::kCatalog};
        decision = {CommitOwnerBootstrapKind::kDirectory, SerializeDataDirManifest(target)};
    }
    const auto target = ParseDataDirManifest(decision.manifest);
    RequireOpenableFeatures(target.features, AccessMode::kReadWrite);
    if (existing && SerializeDataDirManifest(*existing) != decision.manifest) Bad("manifest differs from creation decision");
    CheckCreationNamespace(directory, decision.kind, pending.has_value(), existing.has_value());
    CommitOwnerArtifactIo writer(directory, *target.commit_owner, lock, false);
    if (pending) writer.ValidateBootstrapTemporaries(decision);
    RequireInitialRoot(writer.ReadCanonicalRoot(), *target.commit_owner, existing.has_value());
    if (!pending) {
        Crash("CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_BEFORE_DECISION_ONCE");
    }
    writer.PublishBootstrapBytes(SerializeCommitOwnerBootstrapDecision(decision));
    if (!pending) Crash("CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_AFTER_DECISION_ONCE");
    writer.PublishInactive();
    Crash("CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_AFTER_OWNER_ONCE");
    if (!existing && !PublishNewFile(DataDirManifestPath(directory), decision.manifest,
            "CHUNKDB_FAILPOINT_CRASH_DATA_DIR_MANIFEST_BEFORE_PUBLISH_ONCE",
            "CHUNKDB_FAILPOINT_CRASH_DATA_DIR_MANIFEST_AFTER_PUBLISH_ONCE")) Bad("creation manifest appeared");
    SyncFilePath(DataDirManifestPath(directory)); SyncDirectoryPath(directory);
    FinishDecision(directory);
    return target;
}

void CommitOwnerBootstrap::CompleteFeatureOwner(const std::filesystem::path& directory, const ProcessLock& lock) {
    const auto decision = ReadFeatureActivationJournal(directory);
    if (!decision || decision->feature != kFeatureCommitGroups || !decision->commit_owner)
        Bad("canonical feature owner requires its persisted group-activation decision");
    ValidateFeatureActivationJournal(directory, *decision);
    CommitOwnerArtifactIo writer(directory, *decision->commit_owner, lock, false);
    const auto manifest = ReadDataDirManifest(directory);
    if (!manifest) Bad("feature activation directory manifest disappeared");
    if ((manifest->features.incompat & kFeatureCommitGroups) != 0U && !writer.ReadCanonicalRoot())
        Bad("feature-visible manifest lost its canonical owner");
    writer.SyncFeatureDecision(EncodeFeatureActivationJournal(*decision));
    writer.PublishInactive();
    Crash("CHUNKDB_FAILPOINT_CRASH_OWNER_FEATURE_AFTER_OWNER_ONCE");
}

DataDirManifest CommitOwnerBootstrap::RewriteInactiveRestore(const std::filesystem::path& private_copy,
    const BackupRecord& admitted_backup) {
    std::shared_ptr<CommitOwnerArtifactIo> pinned;
    try {
        const auto name = private_copy.filename().string();
        constexpr std::string_view prefix = ".chunkdb.restore.";
        if (!name.starts_with(prefix) || name.size() != prefix.size() + 32U ||
            !std::all_of(name.begin() + static_cast<std::ptrdiff_t>(prefix.size()), name.end(), [](char byte) {
                return (byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f');
            })) Bad("restore copy has no private operation name");
        pinned = CommitOwnerArtifactIo::OpenBootstrapDirectory(private_copy);
    } catch (const std::exception& error) { throw CommitOwnerRestoreOwnershipError(error.what()); }
    pinned->RequireRestoreGuard();
    ValidateBackupContents(private_copy, admitted_backup);
    const auto bytes = pinned->ReadManifestBytes(true);
    if (!bytes) Bad("copied source catalog manifest is missing");
    auto manifest = ParseDataDirManifest(*bytes);
    if (!manifest.commit_owner || manifest.commit_owner->role != CommitOwnerRole::kCatalog)
        Bad("inactive owner restore requires an admitted catalog scope");
    const auto source_scope = *manifest.commit_owner;
    (void)ReadInactiveRoot(private_copy, source_scope);
    pinned->RequireRestoreDirectory();
    manifest.data_dir_id = NewStoreId();
    if (manifest.data_dir_id == source_scope.directory_id) Bad("restored directory identity collided");
    manifest.commit_owner = CommitOwnerScope{manifest.data_dir_id, NewStoreId(), CommitOwnerRole::kCatalog};
    if (manifest.commit_owner->owner_id == source_scope.owner_id) Bad("restored owner identity collided");
    pinned->RewriteRestoredInactive(source_scope, *manifest.commit_owner);
    pinned->RequireRestoreDirectory();
    Crash("CHUNKDB_FAILPOINT_CRASH_OWNER_RESTORE_AFTER_PRIVATE_ROOT_ONCE");
    return manifest;
}
}  // namespace chunkdb
