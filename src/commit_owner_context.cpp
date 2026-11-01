#include "commit_owner_bootstrap.hpp"

#include <array>
#include <set>
#include <stdexcept>
#include <utility>

#include "chunkdb/table_catalog.hpp"

namespace chunkdb {
namespace {
[[noreturn]] void Bad(const char* reason) {
    throw std::runtime_error(std::string("commit owner admission: ") + reason);
}
bool Grouped(const FeatureFlags& features) noexcept { return (features.incompat & kFeatureCommitGroups) != 0U; }
void RequireTableScope(const StoreManifest& manifest, const CommitOwnerScope& scope, const StoreId& epoch) {
    if (!Grouped(manifest.features) || !manifest.commit_owner || *manifest.commit_owner != scope || manifest.store_id != epoch)
        Bad("table scope or epoch changed");
}
}  // namespace

CommitOwnerDirectoryContext::CommitOwnerDirectoryContext(CommitOwnerScope scope, std::shared_ptr<CommitOwnerArtifactIo> root)
    : scope_(std::move(scope)), root_(std::move(root)) {}

void CommitOwnerDirectoryContext::RequireInactive() const {
    root_->RequireNoPendingMetadata();
    const auto selected = root_->ReadCanonicalRoot();
    if (!selected) Bad("claimed canonical owner is missing");
    CommitOwnerRoot expected; expected.scope = scope_;
    if (selected->value != expected) Bad("owner requires complete grouped recovery; initial admission accepts only INACTIVE generation one");
}

std::shared_ptr<const CommitOwnerContext> CommitOwnerDirectoryContext::ForTable(std::string_view name) const {
    if (scope_.role != CommitOwnerRole::kCatalog || !IsValidTableName(name)) Bad("invalid managed table label");
    RequireInactive();
    const std::array<std::string, 2U> components{"tables", std::string(name)};
    auto table = root_->PinDirectories(components);
    const auto bytes = table->ReadManifestBytes(false);
    if (!bytes) Bad("managed table manifest is missing");
    const auto manifest = ParseStoreManifest(*bytes);
    RequireTableScope(manifest, scope_, manifest.store_id);
    return std::shared_ptr<const CommitOwnerContext>(new CommitOwnerContext(shared_from_this(), std::move(table), manifest.store_id));
}

std::shared_ptr<const CommitOwnerContext> CommitOwnerDirectoryContext::ForStagedTable(std::string_view operation,
    const StoreId& expected_epoch) const {
    if (scope_.role != CommitOwnerRole::kCatalog || operation.empty() || operation == "." || operation == ".." ||
        operation.find_first_of("/\\") != std::string_view::npos) Bad("invalid staged table label");
    RequireInactive();
    const std::array<std::string, 2U> components{".chunkdb.staging", std::string(operation)};
    auto table = root_->PinDirectories(components);
    const auto bytes = table->ReadManifestBytes(false);
    if (!bytes) Bad("staged table manifest is missing");
    const auto manifest = ParseStoreManifest(*bytes);
    RequireTableScope(manifest, scope_, expected_epoch);
    return std::shared_ptr<const CommitOwnerContext>(new CommitOwnerContext(shared_from_this(), std::move(table), expected_epoch));
}

CommitOwnerContext::CommitOwnerContext(std::shared_ptr<const CommitOwnerDirectoryContext> owner,
    std::shared_ptr<CommitOwnerArtifactIo> table, StoreId epoch)
    : owner_(std::move(owner)), epoch_(epoch), table_(std::move(table)) {}

std::unique_ptr<CommitOwnerStoreAdmission> CommitOwnerContext::Consume(const std::filesystem::path& directory) const {
    auto admitted = std::unique_ptr<CommitOwnerStoreAdmission>(new CommitOwnerStoreAdmission(owner_, epoch_));
    {
        std::lock_guard lock(mutex_);
        if (consumed_ || !table_) Bad("store admission ticket has already been consumed");
        admitted->table_ = std::move(table_); consumed_ = true;
    }
    admitted->Validate(directory);
    return admitted;
}

CommitOwnerStoreAdmission::CommitOwnerStoreAdmission(std::shared_ptr<const CommitOwnerDirectoryContext> owner, StoreId epoch)
    : owner_(std::move(owner)), epoch_(epoch) {}

StoreManifest CommitOwnerStoreAdmission::ReadCurrentManifest() const {
    owner_->RequireInactive();
    const auto bytes = table_->ReadManifestBytes(false);
    if (!bytes) Bad("admitted table manifest disappeared");
    auto manifest = ParseStoreManifest(*bytes);
    RequireTableScope(manifest, owner_->scope(), epoch_);
    return manifest;
}

void CommitOwnerStoreAdmission::Validate(const std::filesystem::path& directory) {
    if (!table_->MatchesDirectory(std::filesystem::weakly_canonical(directory))) Bad("store directory generation differs from its admission");
    manifest_ = ReadCurrentManifest();
}

std::shared_ptr<const CommitOwnerDirectoryContext> CommitOwnerBootstrap::AdmitDirectory(const std::filesystem::path& directory,
    AccessMode access_mode) {
    const auto path = std::filesystem::weakly_canonical(directory);
    const auto initial = ReadDataDirManifest(path);
    if (!initial) Bad("catalog manifest is missing");
    RequireOpenableFeatures(initial->features, access_mode);
    if (!Grouped(initial->features)) {
        if (std::filesystem::symlink_status(path / "chunkdb.groups.owner").type() != std::filesystem::file_type::not_found)
            Bad("unclaimed canonical owner exists");
        const auto tables = path / "tables";
        if (std::filesystem::is_directory(tables)) {
            for (const auto& entry : std::filesystem::directory_iterator(tables)) {
                if (!IsValidTableName(entry.path().filename().string()) || !entry.is_directory()) continue;
                const auto manifest = ReadStoreManifest(entry.path());
                if (!manifest || Grouped(manifest->features)) Bad("table claims an owner absent from its directory");
                RequireOpenableFeatures(manifest->features, access_mode);
            }
        }
        return {};
    }
    if (!initial->commit_owner || initial->commit_owner->role != CommitOwnerRole::kCatalog)
        Bad("catalog manifest lacks its owner scope");
    auto root = std::make_shared<CommitOwnerArtifactIo>(path, *initial->commit_owner);
    const auto bytes = root->ReadManifestBytes(true);
    if (!bytes) Bad("catalog manifest disappeared");
    const auto manifest = ParseDataDirManifest(*bytes);
    if (manifest.commit_owner != initial->commit_owner || manifest.data_dir_id != initial->data_dir_id)
        Bad("catalog scope changed during admission");
    auto context = std::shared_ptr<CommitOwnerDirectoryContext>(new CommitOwnerDirectoryContext(*manifest.commit_owner, std::move(root)));
    context->RequireInactive();
    std::set<StoreId> epochs;
    for (const auto& name : context->root_->CatalogTableNames()) {
        auto ticket = context->ForTable(name);
        const auto bytes = ticket->table_->ReadManifestBytes(false);
        if (!bytes) Bad("managed table manifest disappeared during inventory admission");
        RequireOpenableFeatures(ParseStoreManifest(*bytes).features, access_mode);
        if (!epochs.insert(ticket->epoch_).second) Bad("duplicate table epoch in catalog inventory");
    }
    context->RequireInactive();
    return context;
}

void CommitOwnerBootstrap::ValidateDirectoryScopes(const std::filesystem::path& directory, AccessMode access_mode) {
    const auto initial = ReadDataDirManifest(directory);
    if (!initial) Bad("catalog manifest is missing");
    RequireOpenableFeatures(initial->features, access_mode);
    if (!Grouped(initial->features)) {
        (void)AdmitDirectory(directory, access_mode);
        return;
    }
    auto root = std::make_shared<CommitOwnerArtifactIo>(directory, *initial->commit_owner);
    const auto selected = root->ReadCanonicalRoot();
    CommitOwnerRoot expected; expected.scope = *initial->commit_owner;
    if (!selected || selected->value != expected) Bad("catalog requires its complete initial INACTIVE owner");
    std::set<StoreId> epochs;
    for (const auto& name : root->CatalogTableNames()) {
        const std::array<std::string, 2U> components{"tables", name};
        const auto table = root->PinDirectories(components);
        const auto bytes = table->ReadManifestBytes(false);
        if (!bytes) Bad("managed table manifest is missing");
        const auto manifest = ParseStoreManifest(*bytes);
        RequireOpenableFeatures(manifest.features, access_mode);
        RequireTableScope(manifest, *initial->commit_owner, manifest.store_id);
        if (!epochs.insert(manifest.store_id).second) Bad("duplicate table epoch in catalog inventory");
    }
}

std::vector<std::uint8_t> CommitOwnerBootstrap::ReadInactiveRoot(const std::filesystem::path& directory,
    const CommitOwnerScope& scope) {
    CommitOwnerArtifactIo view(directory, scope);
    const auto selected = view.ReadCanonicalRoot();
    CommitOwnerRoot expected; expected.scope = scope;
    if (!selected || selected->value != expected) Bad("claimed owner is not initial INACTIVE generation one");
    const auto bytes = SerializeCommitOwnerRoot(expected);
    return {bytes.begin(), bytes.end()};
}

std::unique_ptr<CommitOwnerStoreAdmission> CommitOwnerBootstrap::BeginStoreAdmission(const StoreConfig& config) {
    if (config.data_dir.empty()) throw std::invalid_argument("data_dir must not be empty");
    const auto path = std::filesystem::weakly_canonical(config.data_dir);
    if (config.commit_owner_context) return config.commit_owner_context->Consume(path);
    const auto initial = ReadStoreManifest(path);
    if (!initial) return {};
    if (!Grouped(initial->features)) {
        if (std::filesystem::symlink_status(path / "chunkdb.groups.owner").type() != std::filesystem::file_type::not_found)
            Bad("unclaimed canonical owner exists");
        return {};
    }
    if (!initial->commit_owner || initial->commit_owner->role != CommitOwnerRole::kStandalone)
        Bad("catalog-owned table requires its checked directory admission");
    if (ReadPending(path)) {
        (void)PendingStandaloneManifest(path, config.access_mode);
        return {};  // Creation redo runs only after acquiring its real writer lock.
    }
    if (ReadDataDirManifest(path)) Bad("standalone root also contains a catalog manifest");
    auto root = std::make_shared<CommitOwnerArtifactIo>(path, *initial->commit_owner);
    auto owner = std::shared_ptr<CommitOwnerDirectoryContext>(new CommitOwnerDirectoryContext(*initial->commit_owner, root));
    owner->RequireInactive();
    auto ticket = std::shared_ptr<const CommitOwnerContext>(new CommitOwnerContext(owner, std::move(root), initial->store_id));
    return ticket->Consume(path);
}
}  // namespace chunkdb
