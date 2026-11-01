#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <type_traits>

#include "chunkdb/crc32.hpp"
#include "chunkdb/table_catalog.hpp"
#include "backup.hpp"
#include "checkpoint.hpp"
#include "commit_owner_bootstrap.hpp"
#include "feature_activation_records.hpp"
#include "feed_prefix.hpp"
#include "process_lock.hpp"
#include "test_utils.hpp"
#include "verify.hpp"
#include "owner_artifact_allocation_guard.hpp"
#include "txn_test_utils.hpp"

#ifdef _WIN32
#include <windows.h>
#include <winioctl.h>
#else
#include <sys/wait.h>
#endif

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
void Save(const std::filesystem::path& path, std::span<const std::uint8_t> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close(); assert(output.good());
}
template <typename F> void Reject(F action) {
    bool rejected = false;
    try { action(); } catch (const std::exception&) { rejected = true; }
    assert(rejected);
}
Bytes Crc(Bytes bytes) {
    const auto crc = Crc32(bytes.data(), bytes.size() - 4U);
    for (unsigned i = 0; i < 4U; ++i) bytes[bytes.size() - 4U + i] = static_cast<std::uint8_t>(crc >> (8U * i));
    return bytes;
}
StoreManifest StandaloneManifest() {
    StoreManifest manifest;
    manifest.geometry = {1U, 1U, 2U, 2U, 8U}; manifest.store_id = {11U};
    manifest.schema = SingleBitsColumnSchema(8U);
    manifest.commit_owner = CommitOwnerScope{manifest.store_id, {12U}, CommitOwnerRole::kStandalone};
    return manifest;
}
void DecisionAndScopeAdmission() {
    const auto manifest = StandaloneManifest();
    const auto raw = SerializeStoreManifest(manifest);
    assert(ParseStoreManifest(raw).commit_owner == manifest.commit_owner);
    auto missing = manifest; missing.commit_owner.reset(); Reject([&] { (void)SerializeStoreManifest(missing); });
    auto unclaimed = manifest; unclaimed.features.incompat &= ~kFeatureCommitGroups;
    Reject([&] { (void)SerializeStoreManifest(unclaimed); });
    auto wrong_role = manifest; wrong_role.commit_owner->directory_id = {99U};
    Reject([&] { (void)SerializeStoreManifest(wrong_role); });
    CommitOwnerBootstrapDecision decision{CommitOwnerBootstrapKind::kStandalone, raw};
    const auto bytes = SerializeCommitOwnerBootstrapDecision(decision);
    assert(ParseCommitOwnerBootstrapDecision(bytes) == decision);
    assert(InspectCommitOwnerBootstrapHeader(std::span(bytes).first(64U)).bytes == bytes.size());
    for (std::size_t i = 0U; i < bytes.size(); ++i)
        Reject([&] { (void)ParseCommitOwnerBootstrapDecision(std::span(bytes).first(i)); });
    auto corrupt = bytes; corrupt[64U] ^= 1U; corrupt = Crc(std::move(corrupt));
    Reject([&] { (void)ParseCommitOwnerBootstrapDecision(corrupt); });
    auto oversized = bytes;
    for (unsigned i = 0U; i < 8U; ++i) oversized[8U + i] = 0xFFU;
    const auto header_crc = Crc32(oversized.data(), 60U);
    for (unsigned i = 0U; i < 4U; ++i) oversized[60U + i] = static_cast<std::uint8_t>(header_crc >> (8U * i));
    Reject([&] { (void)InspectCommitOwnerBootstrapHeader(std::span(oversized).first(64U)); });
}

void StandaloneCreationAndMissingOwner() {
    test::ScopedTempDir temporary("chunkdb-owner-store-bootstrap");
    StoreConfig config; config.data_dir = temporary.path(); config.background_maintenance = false;
    {
        ChunkStore store(config);
        const auto manifest = *ReadStoreManifest(temporary.path());
        assert(manifest.commit_owner && manifest.commit_owner->role == CommitOwnerRole::kStandalone);
        assert(manifest.commit_owner->directory_id == store.store_id());
        assert(CommitOwnerBootstrap::ReadInactiveRoot(temporary.path(), *manifest.commit_owner).size() == 512U);
        assert(!std::filesystem::exists(temporary.path() / kCommitOwnerBootstrapName));
        assert(!std::filesystem::exists(temporary.path() / "chunkdb.groups.objects"));
    }
    config.access_mode = AccessMode::kReadOnly;
    { ChunkStore reopened(config); }
    const auto manifest_bytes = LoadFile(StoreManifestPath(temporary.path()));
    const auto index_bytes = LoadFile(temporary.path() / "chunkdb.index");
    std::filesystem::remove(temporary.path() / "chunkdb.groups.owner");
    Reject([&] { ChunkStore refused(config); });
    config.access_mode = AccessMode::kReadWrite;
    Reject([&] { ChunkStore refused(config); });
    assert(LoadFile(StoreManifestPath(temporary.path())) == manifest_bytes);
    assert(LoadFile(temporary.path() / "chunkdb.index") == index_bytes);
    assert(!std::filesystem::exists(temporary.path() / "chunkdb.groups.owner"));
}

void BorrowedAndShortBootstrapOwnership() {
    test::ScopedTempDir temporary("chunkdb-owner-bootstrap-locks");
    const auto borrowed_root = temporary.path() / "borrowed";
    std::filesystem::create_directory(borrowed_root);
    ProcessLock owner(borrowed_root);
    StoreConfig borrowed; borrowed.data_dir = borrowed_root; borrowed.acquire_process_lock = false;
    borrowed.bootstrap_process_lock = &owner; borrowed.background_maintenance = false;
    { ChunkStore store(borrowed); assert(ReadStoreManifest(borrowed_root)->commit_owner); }
    const auto other = temporary.path() / "wrong-root";
    std::filesystem::create_directory(other);
    borrowed.data_dir = other;
    Reject([&] { ChunkStore wrong_native_owner(borrowed); });
    for (const auto* name : {"table.manifest", "chunkdb.groups.owner", "chunkdb.bootstrap.pending", "chunkdb.index"})
        assert(!std::filesystem::exists(other / name));
    const auto shared_root = temporary.path() / "short-shared-bootstrap";
    StoreConfig shared; shared.data_dir = shared_root; shared.allow_multiple_processes = true;
    shared.background_maintenance = false;
    { ChunkStore store(shared); assert(ReadStoreManifest(shared_root)->commit_owner); }
    { ProcessLock available_after_constructor(shared_root); }
}

void CatalogContextsAndSingleUse() {
    test::ScopedTempDir temporary("chunkdb-owner-catalog-bootstrap");
    CatalogConfig config; config.data_dir = temporary.path(); config.background_maintenance = false;
    {
        TableCatalog catalog(config);
        (void)catalog.Create("alpha", {1U, 1U, 2U, 2U, 8U}, config.default_options);
        (void)catalog.Create("beta", {1U, 1U, 2U, 2U, 8U}, config.default_options);
    }
    const auto root = *ReadDataDirManifest(temporary.path());
    assert(root.commit_owner && root.commit_owner->role == CommitOwnerRole::kCatalog);
    const auto directory = CommitOwnerBootstrap::AdmitDirectory(temporary.path());
    const auto table = temporary.path() / "tables/alpha";
    StoreConfig isolated; isolated.data_dir = table; isolated.access_mode = AccessMode::kReadOnly;
    isolated.geometry_fields = 0U;
    Reject([&] { ChunkStore refused(isolated); });
    auto ticket = directory->ForTable("alpha");
    isolated.commit_owner_context = ticket;
    { ChunkStore admitted(isolated); }
    Reject([&] { ChunkStore reused(isolated); });
    auto mismatched = isolated;
    mismatched.commit_owner_context = directory->ForTable("alpha");
    mismatched.data_dir = temporary.path() / "tables/beta";
    Reject([&] { ChunkStore wrong_generation(mismatched); });
    Reject([&] { ChunkStore consumed_after_failure(mismatched); });
    const auto copy = temporary.path() / "copied-table";
    std::filesystem::copy(table, copy, std::filesystem::copy_options::recursive);
    auto copied = isolated;
    copied.data_dir = copy; copied.commit_owner_context = directory->ForTable("alpha");
    // Every persisted identity and byte matches; only the native directory differs.
    Reject([&] { ChunkStore outside_managed_generation(copied); });
    const auto before = LoadFile(StoreManifestPath(table));
    auto beta = *ReadStoreManifest(temporary.path() / "tables/beta");
    beta.commit_owner->owner_id[0U] ^= 1U;
    Save(StoreManifestPath(temporary.path() / "tables/beta"), SerializeStoreManifest(beta));
    Reject([&] { TableCatalog refused(config); });
    assert(LoadFile(StoreManifestPath(table)) == before);
    std::ostringstream output;
    assert(VerifyDataDirectory(temporary.path(), output).errors != 0U);
}

void PendingCreationKeepsIdentity() {
    test::ScopedTempDir temporary("chunkdb-owner-pending-bootstrap");
    const auto manifest = StandaloneManifest();
    const auto raw = SerializeStoreManifest(manifest);
    Save(temporary.path() / kCommitOwnerBootstrapName,
        SerializeCommitOwnerBootstrapDecision({CommitOwnerBootstrapKind::kStandalone, raw}));
    StoreConfig config; config.data_dir = temporary.path(); config.background_maintenance = false;
    config.geometry_fields = 0U;
    config.access_mode = AccessMode::kReadOnly;
    Reject([&] { ChunkStore refused(config); });
    assert(!std::filesystem::exists(temporary.path() / "chunkdb.groups.owner"));
    config.access_mode = AccessMode::kReadWrite;
    { ChunkStore recovered(config); assert(recovered.store_id() == manifest.store_id); }
    assert(LoadFile(StoreManifestPath(temporary.path())) == raw);
    assert(!std::filesystem::exists(temporary.path() / kCommitOwnerBootstrapName));
    Save(temporary.path() / kCommitOwnerBootstrapName,
        SerializeCommitOwnerBootstrapDecision({CommitOwnerBootstrapKind::kStandalone, raw}));
    std::filesystem::remove(temporary.path() / "chunkdb.groups.owner");
    Reject([&] { ChunkStore must_not_repair_committed_loss(config); });
    assert(!std::filesystem::exists(temporary.path() / "chunkdb.groups.owner"));
    assert(LoadFile(StoreManifestPath(temporary.path())) == raw);
}

void HistoricalActivationRecoversWithoutOwner() {
    test::ScopedTempDir temporary("chunkdb-owner-historical-activation");
    CatalogConfig config; config.data_dir = temporary.path(); config.background_maintenance = false;
    {
        TableCatalog catalog(config);
        (void)catalog.Create("alpha", {1U, 1U, 2U, 2U, 8U}, config.default_options);
    }
    auto root = *ReadDataDirManifest(temporary.path());
    root.features.incompat = kFeatureActivation | kFeatureChunkModel; root.commit_owner.reset();
    Save(DataDirManifestPath(temporary.path()), SerializeDataDirManifest(root));
    assert(std::filesystem::remove(temporary.path() / "chunkdb.groups.owner"));
    auto table = *ReadStoreManifest(temporary.path() / "tables/alpha");
    table.features.incompat = kFeatureChunkModel; table.commit_owner.reset();
    Save(StoreManifestPath(temporary.path() / "tables/alpha"), SerializeStoreManifest(table));
    const FeatureActivationJournal decision{root.data_dir_id, kFeatureFeedSlots, {{"alpha", table.store_id}}};
    auto legacy = EncodeFeatureActivationJournal(decision);
    legacy.erase(legacy.begin() + 32, legacy.begin() + 72); legacy[4U] = 1U; legacy = Crc(std::move(legacy));
    assert(DecodeFeatureActivationJournal(legacy) == decision);
    Save(temporary.path() / kFeaturePendingFileName, legacy);
    const auto before = LoadFile(StoreManifestPath(temporary.path() / "tables/alpha"));
    config.access_mode = AccessMode::kReadOnly;
    Reject([&] { TableCatalog cannot_redo_read_only(config); });
    assert(LoadFile(StoreManifestPath(temporary.path() / "tables/alpha")) == before);
    std::ostringstream output;
    assert(VerifyDataDirectory(temporary.path(), output).errors == 0U);
    config.access_mode = AccessMode::kReadWrite;
    { TableCatalog recovered(config); }
    assert(!ReadFeatureActivationJournal(temporary.path()));
    assert(!ReadDataDirManifest(temporary.path())->commit_owner);
    assert(!ReadStoreManifest(temporary.path() / "tables/alpha")->commit_owner);
    assert(!std::filesystem::exists(temporary.path() / "chunkdb.groups.owner"));
    config.access_mode = AccessMode::kReadOnly;
    { TableCatalog reopened(config); }
    auto impossible = legacy;
    impossible[24U] = static_cast<std::uint8_t>(kFeatureCommitGroups); impossible = Crc(std::move(impossible));
    Reject([&] { (void)DecodeFeatureActivationJournal(impossible); });
}

void SparseDecisionAdmission() {
    test::ScopedTempDir temporary("chunkdb-owner-sparse-decision");
    const auto bytes = SerializeCommitOwnerBootstrapDecision(
        {CommitOwnerBootstrapKind::kStandalone, SerializeStoreManifest(StandaloneManifest())});
    const auto path = temporary.path() / kCommitOwnerBootstrapName;
    Save(path, bytes);
#ifdef _WIN32
    const auto file = CreateFileW(path.c_str(), GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
    assert(file != INVALID_HANDLE_VALUE);
    DWORD returned = 0U;
    assert(DeviceIoControl(file, FSCTL_SET_SPARSE, nullptr, 0U, nullptr, 0U, &returned, nullptr) != 0);
    assert(CloseHandle(file) != 0);
#endif
    std::filesystem::resize_file(path, std::uint64_t{1U} << 30U);
    {
        owner_artifact_allocation::Guard allocation(128U * 1024U);
        const auto denied = owner_artifact_allocation::denied.load();
        Reject([&] { (void)CommitOwnerBootstrap::ReadPending(temporary.path()); });
        assert(owner_artifact_allocation::denied.load() == denied);
    }
    assert(std::filesystem::file_size(path) == (std::uint64_t{1U} << 30U));
}

void ReusedOwnerMustBeDurableBeforeManifest() {
    for (const bool fail_directory : {false, true}) {
        test::ScopedTempDir temporary("chunkdb-owner-reuse-durability");
        const auto directory = std::filesystem::canonical(temporary.path());
        const auto target = StandaloneManifest();
        Save(directory / kCommitOwnerBootstrapName,
            SerializeCommitOwnerBootstrapDecision({CommitOwnerBootstrapKind::kStandalone, SerializeStoreManifest(target)}));
        CommitOwnerRoot inactive; inactive.scope = *target.commit_owner;
        Save(directory / "chunkdb.groups.owner", SerializeCommitOwnerRoot(inactive));
        bool file_synced = false, failed = false;
        struct Observers {
            ~Observers() { SetFileSyncObserverForTests({}); SetDirectorySyncObserverForTests({}); }
        } observers;
        SetFileSyncObserverForTests([&](const auto& path) {
            if (path != directory / "chunkdb.groups.owner") return;
            file_synced = true;
            if (!fail_directory) { failed = true; throw std::runtime_error("injected owner file-sync failure"); }
        });
        SetDirectorySyncObserverForTests([&](const auto& path) {
            if (fail_directory && file_synced && path == directory) {
                failed = true; throw std::runtime_error("injected owner parent-sync failure");
            }
        });
        StoreConfig config; config.data_dir = directory; config.background_maintenance = false;
        config.geometry_fields = 0U;
        Reject([&] { ChunkStore refused(config); });
        assert(file_synced && failed);
        assert(!std::filesystem::exists(StoreManifestPath(directory)));
        assert(CommitOwnerBootstrap::ReadPending(directory)->manifest == SerializeStoreManifest(target));
        SetFileSyncObserverForTests({}); SetDirectorySyncObserverForTests({});
        { ChunkStore recovered(config); assert(recovered.store_id() == target.store_id); }
    }
}

void WrongPendingRootRefusesBeforeInventory() {
    test::ScopedTempDir temporary("chunkdb-owner-pending-wrong-root");
    const auto target = StandaloneManifest();
    const auto decision = SerializeCommitOwnerBootstrapDecision(
        {CommitOwnerBootstrapKind::kStandalone, SerializeStoreManifest(target)});
    Save(temporary.path() / kCommitOwnerBootstrapName, decision);
    CommitOwnerRoot later; later.scope = *target.commit_owner; later.generation = 2U;
    const auto bytes = SerializeCommitOwnerRoot(later);
    Save(temporary.path() / "chunkdb.groups.owner", bytes);
    StoreConfig config; config.data_dir = temporary.path(); config.background_maintenance = false;
    config.geometry_fields = 0U;
    Reject([&] { ChunkStore refused(config); });
    assert(!std::filesystem::exists(temporary.path() / "chunkdb.index"));
    assert(!std::filesystem::exists(StoreManifestPath(temporary.path())));
    assert(LoadFile(temporary.path() / "chunkdb.groups.owner") == Bytes(bytes.begin(), bytes.end()));
    assert(LoadFile(temporary.path() / kCommitOwnerBootstrapName) == decision);
}

void OwnerAdmissionPrecedesChunkReads() {
    test::ScopedTempDir temporary("chunkdb-owner-verify-read-admission");
    CatalogConfig config; config.data_dir = temporary.path(); config.background_maintenance = false;
    {
        TableCatalog catalog(config);
        auto table = catalog.Create("alpha", {1U, 1U, 2U, 2U, 8U}, config.default_options);
        auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, "10000000");
        lease->store().CheckpointForTests(0, 0);
    }
    struct Reads : FeedWalPrefixTestHook {
        std::size_t images = 0U;
        void Run(Point point, ChunkCoord) override { if (point == Point::kImageRead) ++images; }
        ~Reads() override { FeedWalPrefixTestAccess::SetHook(nullptr); }
    } reads;
    FeedWalPrefixTestAccess::SetHook(&reads);
    std::ostringstream findings;
    assert(VerifyDataDirectory(temporary.path(), findings).errors == 0U && reads.images > 0U);
    const auto source = LoadFile(temporary.path() / "chunkdb.groups.owner");
    const auto scope = *ReadDataDirManifest(temporary.path())->commit_owner;
    for (const auto* damage : {"missing", "generation", "active", "pending"}) {
        if (std::string_view(damage) == "missing") {
            std::filesystem::remove(temporary.path() / "chunkdb.groups.owner");
        } else {
            CommitOwnerRoot root; root.scope = scope; root.generation = 2U;
            if (std::string_view(damage) != "generation") {
                root.state = CommitOwnerState::kActive;
                root.coverage_tree = {CommitOwnerObjectKind::kPage, 92U, {1U}};
                root.group_tree = {CommitOwnerObjectKind::kPage, 92U, {2U}};
                root.journal_inventory = {CommitOwnerObjectKind::kJournalInventory, 128U, {3U}};
                root.id_inventory = {CommitOwnerObjectKind::kIdInventory, 164U, {4U}};
                root.reference_context = {scope.directory_id, {7U}, 1U};
                root.reference_replay.bytes = 64U; root.reference_replay.sha256 = {8U};
                if (std::string_view(damage) == "pending") {
                    root.state = CommitOwnerState::kPending; root.generation = 3U;
                    root.replacement_plan = {CommitOwnerObjectKind::kReplacementPlan, 68U, {5U}};
                    root.previous_root = {CommitOwnerObjectKind::kRoot, 512U, {6U}};
                }
            }
            Save(temporary.path() / "chunkdb.groups.owner", SerializeCommitOwnerRoot(root));
        }
        reads.images = 0U; findings.str(""); findings.clear();
        assert(VerifyDataDirectory(temporary.path(), findings).errors > 0U);
        assert(reads.images == 0U);
        Save(temporary.path() / "chunkdb.groups.owner", source);
    }
}

void UnclaimedOwnersRefuseEverySurface() {
    test::ScopedTempDir temporary("chunkdb-owner-unclaimed-surfaces");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto source = root / "source", backup = root / "backup";
    std::filesystem::create_directories(source / "tables");
    DataDirManifest manifest{.features = {.incompat = kFeatureChunkModel}, .data_dir_id = NewStoreId(), .options = {}};
    Save(DataDirManifestPath(source), SerializeDataDirManifest(manifest));
    CommitOwnerRoot unclaimed; unclaimed.scope = {manifest.data_dir_id, NewStoreId(), CommitOwnerRole::kCatalog};
    const auto root_bytes = SerializeCommitOwnerRoot(unclaimed);
    {
        CatalogConfig config; config.data_dir = source; config.background_maintenance = false;
        TableCatalog catalog(config);
        (void)catalog.BackupTo(backup, {});
        Save(source / "chunkdb.groups.owner", root_bytes);
        std::ostringstream findings;
        assert(VerifyDataDirectory(source, findings).errors > 0U);
        assert(findings.str().find("unclaimed canonical commit owner") != std::string::npos);
        Reject([&] { (void)CommitOwnerBootstrap::AdmitDirectory(source); });
        const auto refused = root / "refused-backup";
        Reject([&] { (void)catalog.BackupTo(refused, {}); });
        assert(!std::filesystem::exists(refused / kBackupMarkerName));
        assert(LoadFile(source / "chunkdb.groups.owner") == Bytes(root_bytes.begin(), root_bytes.end()));
    }
    Save(backup / "chunkdb.groups.owner", root_bytes);
    auto record = ReadBackupRecord(backup);
    record.files.push_back(InspectBackupFile(backup, "chunkdb.groups.owner"));
    Save(backup / kBackupMarkerName, SerializeBackupRecord(record));
    const auto reject_unclaimed_owner = [](auto action) {
        bool rejected = false;
        try { action(); } catch (const std::exception& error) {
            assert(std::string_view(error.what()) == "unclaimed canonical commit owner exists");
            rejected = true;
        }
        assert(rejected);
    };
    reject_unclaimed_owner([&] { ValidateBackupInventory(backup, record); });
    const auto destination = root / "refused-restore";
    reject_unclaimed_owner([&] { RestoreBackup(backup, destination); });
    assert(!std::filesystem::exists(destination));
    assert(LoadFile(backup / "chunkdb.groups.owner") == Bytes(root_bytes.begin(), root_bytes.end()));
}

int RunChild(const std::string& executable, const char* mode, const std::filesystem::path& directory, const char* point) {
    const auto command = "\"" + executable + "\" \"" + mode + "\" \"" + directory.string() + "\" \"" + point + "\"";
#ifdef _WIN32
    return std::system(("\"" + command + "\"").c_str());
#else
    const auto result = std::system(command.c_str());
    return WIFEXITED(result) ? WEXITSTATUS(result) : -1;
#endif
}

void CreationCrashBoundaries(const std::string& executable) {
    for (const auto* mode : {"--create-store", "--create-catalog"}) {
        const bool standalone = std::string_view(mode) == "--create-store";
        const std::vector<const char*> points{"CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_BEFORE_DECISION_ONCE",
                "CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_AFTER_DECISION_ONCE",
                "CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_AFTER_OWNER_ONCE",
                "CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_BEFORE_PENDING_REMOVE_ONCE",
                "CHUNKDB_FAILPOINT_CRASH_OWNER_BOOTSTRAP_AFTER_PENDING_REMOVE_ONCE",
                standalone ? "CHUNKDB_FAILPOINT_CRASH_MANIFEST_BEFORE_PUBLISH_ONCE" :
                    "CHUNKDB_FAILPOINT_CRASH_DATA_DIR_MANIFEST_BEFORE_PUBLISH_ONCE",
                standalone ? "CHUNKDB_FAILPOINT_CRASH_MANIFEST_AFTER_PUBLISH_ONCE" :
                    "CHUNKDB_FAILPOINT_CRASH_DATA_DIR_MANIFEST_AFTER_PUBLISH_ONCE"};
        for (const auto* point : points) {
            test::ScopedTempDir temporary("chunkdb-owner-bootstrap-crash");
            assert(RunChild(executable, mode, temporary.path(), point) == 86);
            const auto decision = CommitOwnerBootstrap::ReadPending(temporary.path());
            const auto selected_path = temporary.path() / "chunkdb.groups.owner";
            const auto selected = std::filesystem::exists(selected_path) ? LoadFile(selected_path) : Bytes{};
            if (decision) {
                if (standalone) {
                    StoreConfig read_only; read_only.data_dir = temporary.path(); read_only.access_mode = AccessMode::kReadOnly;
                    Reject([&] { ChunkStore pending(read_only); });
                } else {
                    CatalogConfig read_only; read_only.data_dir = temporary.path(); read_only.access_mode = AccessMode::kReadOnly;
                    Reject([&] { TableCatalog pending(read_only); });
                }
                assert(CommitOwnerBootstrap::ReadPending(temporary.path()) == decision);
            }
            if (standalone) {
                StoreConfig config; config.data_dir = temporary.path(); config.background_maintenance = false;
                ChunkStore reopened(config);
                if (decision) assert(LoadFile(StoreManifestPath(temporary.path())) == decision->manifest);
            } else {
                CatalogConfig config; config.data_dir = temporary.path(); config.background_maintenance = false;
                TableCatalog reopened(config);
                if (decision) assert(LoadFile(DataDirManifestPath(temporary.path())) == decision->manifest);
            }
            assert(!CommitOwnerBootstrap::ReadPending(temporary.path()));
            if (!selected.empty()) assert(LoadFile(selected_path) == selected);
        }
    }
}

std::map<std::filesystem::path, Bytes> BootstrapFiles(const std::filesystem::path& root) {
    std::map<std::filesystem::path, Bytes> files;
    for (std::filesystem::recursive_directory_iterator entry(root), end; entry != end; ++entry) {
        const auto relative = entry->path().lexically_relative(root);
        if (relative.begin()->string() == ".chunkdb.lock") { entry.disable_recursion_pending(); continue; }
        const auto type = entry->symlink_status().type();
        Bytes value;
        if (type == std::filesystem::file_type::regular) { value = LoadFile(entry->path()); value.push_back('f'); }
        else if (type == std::filesystem::file_type::directory) value = {'d'};
        else if (type == std::filesystem::file_type::symlink) {
            const auto target = std::filesystem::read_symlink(entry->path()).generic_string();
            value.assign(target.begin(), target.end()); value.push_back('l');
        } else value = {'?'};
        files.emplace(relative, std::move(value));
    }
    return files;
}

void TemporaryCreationRecovery(const std::string& executable) {
    for (const auto* family : {"directory", "standalone", "index"}) {
        const bool directory = std::string_view(family) == "directory";
        const bool index = std::string_view(family) == "index";
        for (const auto* damage : {"zero", "prefix", "exact", "wrong", "wrong-prefix", "other-family", "spelling", "directory", "later-root"
#ifndef _WIN32
                , "symlink"
#endif
                }) {
            test::ScopedTempDir temporary("chunkdb-owner-temporary-recovery");
            const auto root = std::filesystem::canonical(temporary.path());
            const auto mode = directory ? "--create-catalog" : index ? "--create-index-temp" : "--create-store";
            const auto point = directory ? "CHUNKDB_FAILPOINT_CRASH_DATA_DIR_MANIFEST_BEFORE_PUBLISH_ONCE" :
                index ? "CHUNKDB_TEST_INDEX_TEMP_SYNC" : "CHUNKDB_FAILPOINT_CRASH_MANIFEST_BEFORE_PUBLISH_ONCE";
            assert(RunChild(executable, mode, root, point) == 86);
            const auto decision = *CommitOwnerBootstrap::ReadPending(root);
            const auto target = directory ? "chunkdb.manifest" : index ? "chunkdb.index" : "table.manifest";
            std::filesystem::path staged;
            for (const auto& entry : std::filesystem::directory_iterator(root))
                if (entry.path().filename().string().starts_with(std::string(target) + ".tmp.")) {
                    assert(staged.empty()); staged = entry.path();
                }
            assert(!staged.empty());
            const auto complete = LoadFile(staged);
            const auto selected = directory ? *ParseDataDirManifest(decision.manifest).commit_owner :
                *ParseStoreManifest(decision.manifest).commit_owner;
            const auto change = std::string_view(damage);
            if (change == "zero") Save(staged, {});
            else if (change == "prefix") Save(staged, std::span(complete).first(complete.size() / 2U));
            else if (change == "wrong" || change == "wrong-prefix") {
                auto bytes = complete;
                if (change == "wrong-prefix") bytes.resize(bytes.size() / 2U);
                bytes.back() ^= 1U; Save(staged, bytes);
            } else if (change == "other-family") {
                const auto wrong = directory ? "table.manifest" : "chunkdb.manifest";
                Save(root / (std::string(wrong) + staged.filename().string().substr(std::string(target).size())), complete);
            } else if (change == "spelling") {
                std::filesystem::rename(staged, root / (std::string(target) + ".tmp.1.2.bad.4"));
            } else if (change == "directory") {
                std::filesystem::remove(staged); std::filesystem::create_directory(staged);
                Save(staged / "unrelated", Bytes{'k', 'e', 'e', 'p'});
            } else if (change == "later-root") {
                CommitOwnerRoot later; later.scope = selected; later.generation = 2U;
                Save(root / "chunkdb.groups.owner", SerializeCommitOwnerRoot(later));
#ifndef _WIN32
            } else if (change == "symlink") {
                const auto sentinel = root / "unrelated";
                Save(sentinel, complete); std::filesystem::remove(staged);
                std::filesystem::create_symlink(sentinel, staged);
#endif
            }
            const auto open = [&] {
                if (directory) {
                    CatalogConfig config; config.data_dir = root; config.background_maintenance = false;
                    TableCatalog store(config);
                } else {
                    StoreConfig config; config.data_dir = root; config.background_maintenance = false;
                    config.geometry_fields = 0U; ChunkStore store(config);
                }
            };
            if (change == "zero" || change == "prefix" || change == "exact") {
                open();
                assert(!CommitOwnerBootstrap::ReadPending(root));
                assert(CommitOwnerBootstrap::ReadInactiveRoot(root, selected).size() == 512U);
                assert(LoadFile(directory ? DataDirManifestPath(root) : StoreManifestPath(root)) == decision.manifest);
            } else {
                const auto before = BootstrapFiles(root);
                Reject(open);
                assert(BootstrapFiles(root) == before);
            }
        }
    }
}

void InactiveBackupRestore(const std::string& executable) {
    test::ScopedTempDir temporary("chunkdb-owner-inactive-transfer");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto source = root / "source", backup = root / "backup";
    StoreId source_epoch;
    std::uint64_t revision = 0U;
    {
        CatalogConfig config; config.data_dir = source; config.background_maintenance = false;
        TableCatalog catalog(config);
        auto table = catalog.Create("alpha", {1U, 1U, 2U, 2U, 8U}, config.default_options);
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, "10100000");
        revision = lease->store().GetChunkVersion(0, 0); source_epoch = table->store_id();
        lease.reset();
        (void)catalog.BackupTo(backup, {});
    }
    const auto source_manifest = *ReadDataDirManifest(backup);
    const auto source_root = LoadFile(backup / "chunkdb.groups.owner");
    const auto record = ReadBackupRecord(backup);
    assert(std::any_of(record.files.begin(), record.files.end(), [](const auto& file) {
        return file.relative_path == "chunkdb.groups.owner" && file.size == kCommitOwnerRootBytes;
    }));
    const auto restored = root / "restored";
    RestoreBackup(backup, restored);
    const auto destination = *ReadDataDirManifest(restored);
    assert(destination.commit_owner && destination.commit_owner != source_manifest.commit_owner);
    assert(destination.data_dir_id != source_manifest.data_dir_id);
    assert(ReadStoreManifest(restored / "tables/alpha")->commit_owner == destination.commit_owner);
    assert(ReadStoreManifest(restored / "tables/alpha")->store_id != source_epoch);
    {
        CatalogConfig config; config.data_dir = restored; config.background_maintenance = false;
        TableCatalog catalog(config); auto lease = catalog.Find("alpha")->Acquire();
        assert(lease->store().GetChunkVersion(0, 0) == revision);
        assert(lease->store().GetBlockBits(0, 0) == "10100000");
    }
    for (const auto* damage : {"scope", "extent", "copy"}) {
        const auto bad_copy = root / (".chunkdb.restore." + StoreIdHex(NewStoreId()));
        std::filesystem::copy(backup, bad_copy, std::filesystem::copy_options::recursive);
        Save(bad_copy / kRestoreIncompleteName, Bytes{'C', 'K', 'R', 'I'});
        if (std::string_view(damage) == "scope") {
            auto scope = *source_manifest.commit_owner; scope.owner_id[0U] ^= 1U;
            CommitOwnerRoot foreign; foreign.scope = scope;
            Save(bad_copy / "chunkdb.groups.owner", SerializeCommitOwnerRoot(foreign));
        } else if (std::string_view(damage) == "extent") {
            std::filesystem::resize_file(bad_copy / "chunkdb.groups.owner", 513U);
        } else {
            auto manifest = LoadFile(StoreManifestPath(bad_copy / "tables/alpha")); manifest.back() ^= 1U;
            Save(StoreManifestPath(bad_copy / "tables/alpha"), manifest);
        }
        const auto before = LoadFile(bad_copy / "chunkdb.groups.owner");
        Reject([&] { (void)CommitOwnerBootstrap::RewriteInactiveRestore(bad_copy, record); });
        assert(LoadFile(bad_copy / "chunkdb.groups.owner") == before);
        assert(LoadFile(backup / "chunkdb.groups.owner") == source_root);
        std::filesystem::remove_all(bad_copy);
    }
    {
        const auto named = root / (".chunkdb.restore." + StoreIdHex(NewStoreId()));
        const auto held = root / "held-private-generation";
        std::filesystem::copy(backup, named, std::filesystem::copy_options::recursive);
        Save(named / kRestoreIncompleteName, Bytes{'C', 'K', 'R', 'I'});
        bool swapped = false;
        struct Observer { ~Observer() { SetDirectorySyncObserverForTests({}); } } observer;
        SetDirectorySyncObserverForTests([&](const auto& path) {
            if (path != named || swapped) return;
            std::filesystem::rename(named, held); std::filesystem::create_directory(named);
            Save(named / "unrelated", Bytes{'k', 'e', 'e', 'p'}); swapped = true;
        });
        Reject([&] { (void)CommitOwnerBootstrap::RewriteInactiveRestore(named, record); });
        SetDirectorySyncObserverForTests({});
        assert(swapped && LoadFile(named / "unrelated") == (Bytes{'k', 'e', 'e', 'p'}));
        assert(!std::filesystem::exists(named / "chunkdb.groups.owner"));
        assert(LoadFile(held / "chunkdb.groups.owner") == source_root);
        assert(LoadFile(backup / "chunkdb.groups.owner") == source_root);
        std::filesystem::remove_all(named); std::filesystem::remove_all(held);
    }
    {
        const auto parent = std::filesystem::canonical(temporary.path());
        const auto held = parent / "held-restore-caller-generation", target = parent / "uncertain-target";
        std::filesystem::path substituted;
        struct Observer { ~Observer() { SetFileSyncObserverForTests({}); } } observer;
        SetFileSyncObserverForTests([&](const auto& path) {
            if (!substituted.empty() || !path.filename().string().starts_with(".owner-restore-") ||
                path.parent_path().parent_path() != parent) return;
            substituted = path.parent_path();
            std::filesystem::rename(substituted, held); std::filesystem::create_directory(substituted);
            Save(substituted / "unrelated", Bytes{'k', 'e', 'e', 'p'});
        });
        bool refused = false;
        try { RestoreBackup(backup, target); }
        catch (const CommitOwnerRestoreOwnershipError& error) {
            refused = std::string_view(error.what()).find("native directory generation changed") != std::string_view::npos;
        }
        SetFileSyncObserverForTests({});
        assert(refused && !substituted.empty() && !std::filesystem::exists(target));
        assert(LoadFile(substituted / "unrelated") == (Bytes{'k', 'e', 'e', 'p'}));
        assert(LoadFile(held / kRestoreIncompleteName) == (Bytes{'C', 'K', 'R', 'I'}));
        assert(LoadFile(backup / "chunkdb.groups.owner") == source_root);
        std::filesystem::remove_all(substituted); std::filesystem::remove_all(held);
    }
    for (const bool deny_diagnostics : {false, true}) {
        const auto parent = std::filesystem::canonical(temporary.path());
        const auto held = parent / (deny_diagnostics ? "held-reopen-allocation" : "held-reopen-failure");
        const auto target = parent / (deny_diagnostics ? "reopen-allocation-target" : "reopen-failure-target");
        std::filesystem::path substituted;
        std::optional<owner_artifact_allocation::Guard> allocation;
        const auto denied = owner_artifact_allocation::denied.load();
        struct Observer { ~Observer() { SetFileSyncObserverForTests({}); } } observer;
        SetFileSyncObserverForTests([&](const auto& path) {
            if (!substituted.empty() || path.filename() != "chunkdb.groups.owner" ||
                !path.parent_path().filename().string().starts_with(".chunkdb.restore.") ||
                path.parent_path().parent_path() != parent) return;
            substituted = path.parent_path();
            std::filesystem::rename(substituted, held);
            Save(substituted, Bytes{'k', 'e', 'e', 'p'});  // A regular leaf, not another openable directory.
            if (deny_diagnostics) allocation.emplace(0U);
        });
        bool classified = false, escaped = false;
        try { RestoreBackup(backup, target); }
        catch (const CommitOwnerRestoreOwnershipError& error) {
            static_assert(std::is_nothrow_copy_constructible_v<CommitOwnerRestoreOwnershipError>);
            static_assert(noexcept(error.what()));
            const auto copied = error;
            classified = std::string_view(copied.what()) == error.what() &&
                std::string_view(copied.what()).find("restore private directory ownership is uncertain:") == 0U;
        } catch (const std::exception&) { escaped = true; }
        allocation.reset(); SetFileSyncObserverForTests({});
        assert(classified && !escaped && !substituted.empty());
        if (deny_diagnostics) assert(owner_artifact_allocation::denied.load() > denied);
        assert(!std::filesystem::exists(target));
        assert(std::filesystem::is_regular_file(substituted));
        assert(LoadFile(substituted) == (Bytes{'k', 'e', 'e', 'p'}));
        assert(LoadFile(held / kRestoreIncompleteName) == (Bytes{'C', 'K', 'R', 'I'}));
        assert(LoadFile(backup / "chunkdb.groups.owner") == source_root);
        std::filesystem::remove(substituted); std::filesystem::remove_all(held);
    }
    const auto private_copy = root / (".chunkdb.restore." + StoreIdHex(NewStoreId()));
    std::filesystem::copy(backup, private_copy, std::filesystem::copy_options::recursive);
    Save(private_copy / kRestoreIncompleteName, Bytes{'C', 'K', 'R', 'I'});
    Save(private_copy / kRestoreIncompleteName, Bytes{'b', 'a', 'd', '!'});
    Reject([&] { (void)CommitOwnerBootstrap::RewriteInactiveRestore(private_copy, record); });
    assert(LoadFile(private_copy / "chunkdb.groups.owner") == source_root);
    Save(private_copy / kRestoreIncompleteName, Bytes{'C', 'K', 'R', 'I'});
    const auto rewritten = CommitOwnerBootstrap::RewriteInactiveRestore(private_copy, record);
    assert(rewritten.commit_owner != source_manifest.commit_owner);
    assert(!std::filesystem::exists(private_copy / ".chunkdb.lock"));
    const auto private_root = LoadFile(private_copy / "chunkdb.groups.owner");
    Reject([&] { (void)CommitOwnerBootstrap::RewriteInactiveRestore(private_copy, record); });
    assert(LoadFile(private_copy / "chunkdb.groups.owner") == private_root);
    assert(LoadFile(backup / "chunkdb.groups.owner") == source_root);
    assert(ReadDataDirManifest(backup)->commit_owner == source_manifest.commit_owner);
    // Remove this deliberately incomplete test copy before exercising normal restore's stale-copy refusal.
    std::filesystem::remove_all(private_copy);
    const auto crash_target = root / "crash-target";
    const auto command = "\"" + executable + "\" --restore \"" + backup.string() + "\" \"" + crash_target.string() +
        "\" CHUNKDB_FAILPOINT_CRASH_OWNER_RESTORE_AFTER_PRIVATE_ROOT_ONCE";
#ifdef _WIN32
    assert(std::system(("\"" + command + "\"").c_str()) == 86);
#else
    const auto status = std::system(command.c_str()); assert(WIFEXITED(status) && WEXITSTATUS(status) == 86);
#endif
    assert(!std::filesystem::exists(crash_target));
    bool incomplete = false;
    for (const auto& entry : std::filesystem::directory_iterator(temporary.path())) {
        if (!entry.path().filename().string().starts_with(".chunkdb.restore.")) continue;
        incomplete = true;
        assert(LoadFile(entry.path() / kRestoreIncompleteName) == (Bytes{'C', 'K', 'R', 'I'}));
        CatalogConfig read_only; read_only.data_dir = entry.path(); read_only.access_mode = AccessMode::kReadOnly;
        Reject([&] { TableCatalog cannot_open_private_copy(read_only); });
    }
    assert(incomplete);
    Reject([&] { RestoreBackup(backup, crash_target); });
    assert(!std::filesystem::exists(crash_target));
    assert(LoadFile(backup / "chunkdb.groups.owner") == source_root);
    ValidateBackupInventory(backup, record);
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 5 && std::string_view(argv[1]) == "--restore") {
        txn_test::ScopedEnv failpoint(argv[4], "1");
        RestoreBackup(argv[2], argv[3]); return 3;
    }
    if (argc == 4) {
        txn_test::ScopedEnv failpoint(argv[3], "1");
        if (std::string_view(argv[1]) == "--create-store" || std::string_view(argv[1]) == "--create-index-temp") {
            if (std::string_view(argv[1]) == "--create-index-temp")
                SetFileSyncObserverForTests([](const auto& path) {
                    if (path.filename().string().starts_with("chunkdb.index.tmp.")) std::_Exit(86);
                });
            StoreConfig config; config.data_dir = argv[2]; config.background_maintenance = false;
            ChunkStore store(config);
        } else if (std::string_view(argv[1]) == "--create-catalog") {
            CatalogConfig config; config.data_dir = argv[2]; config.background_maintenance = false;
            TableCatalog catalog(config);
        } else return 2;
        return 3;
    }
    DecisionAndScopeAdmission(); StandaloneCreationAndMissingOwner();
    BorrowedAndShortBootstrapOwnership();
    CatalogContextsAndSingleUse(); PendingCreationKeepsIdentity();
    HistoricalActivationRecoversWithoutOwner(); SparseDecisionAdmission();
    ReusedOwnerMustBeDurableBeforeManifest();
    WrongPendingRootRefusesBeforeInventory();
    OwnerAdmissionPrecedesChunkReads(); UnclaimedOwnersRefuseEverySurface();
    CreationCrashBoundaries(argv[0]);
    TemporaryCreationRecovery(argv[0]);
    InactiveBackupRestore(argv[0]);
}
