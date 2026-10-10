#include "migrations_records.hpp"

#include <cstdlib>
#include <stdexcept>

#include "checkpoint.hpp"
#include "durability_io.hpp"
#include "store_manifest.hpp"

namespace chunkdb {
namespace {

void Crash(const char* key) {
    if (ConsumeFailpointEnv(key)) std::_Exit(86);
}

void ApplyFile(const std::filesystem::path& root, const MigrationFileChange& file, std::size_t index) {
    const auto prefix = "CHUNKDB_FAILPOINT_CRASH_MIGRATION_FILE_" + std::to_string(index);
    Crash((prefix + "_BEFORE_PUBLISH_ONCE").c_str());
    // Each replacement is atomic and synced. No filesystem mutation occurs
    // before CompleteMigrationJournal has validated every participant.
    const auto sync_failure = "CHUNKDB_FAILPOINT_MIGRATION_FILE_" + std::to_string(index) + "_SYNC_FAIL_ONCE";
    AtomicWrite(root / file.path, file.after, true, true, nullptr, sync_failure.c_str(), false);
    Crash((prefix + "_AFTER_PUBLISH_ONCE").c_str());
}

}  // namespace

void CompleteMigrationJournal(const std::filesystem::path& root, const MigrationJournal& journal) {
    ValidateMigrationJournal(root, journal);
    std::size_t first = 0;
    // DROP's version floor must be durable before removing the live name.
    if (journal.files.front().path == kDataDirManifestFileName) {
        ApplyFile(root, journal.files.front(), 0U);
        first = 1;
    }
    if (journal.directory_action != MigrationDirectoryAction::kNone) {
        const auto live = root / "tables" / journal.table;
        const auto operation_parent = root / (journal.directory_action == MigrationDirectoryAction::kCreate ?
                                               ".chunkdb.staging" : ".chunkdb.dropped");
        const auto operation = operation_parent / journal.operation_name;
        const bool create = journal.directory_action == MigrationDirectoryAction::kCreate;
        const auto& source = create ? operation : live;
        const auto& target = create ? live : operation;
        Crash("CHUNKDB_FAILPOINT_CRASH_MIGRATION_DIRECTORY_BEFORE_PUBLISH_ONCE");
        if (std::filesystem::exists(source)) {
            const auto error = MoveDirectoryNoReplace(source, target);
            if (error) throw std::runtime_error("cannot publish migration directory: " + error.message());
        }
        Crash("CHUNKDB_FAILPOINT_CRASH_MIGRATION_DIRECTORY_AFTER_PUBLISH_ONCE");
        SyncDirectoryPath(root / "tables");
        SyncDirectoryPath(operation_parent);
    }
    for (std::size_t i = first; i < journal.files.size(); ++i) ApplyFile(root, journal.files[i], i);
    Crash("CHUNKDB_FAILPOINT_CRASH_MIGRATION_BEFORE_PENDING_REMOVE_ONCE");
    if (!std::filesystem::remove(root / kMigrationPendingFileName))
        throw std::runtime_error("migration decision disappeared before completion");
    SyncDirectoryPath(root);
    Crash("CHUNKDB_FAILPOINT_CRASH_MIGRATION_AFTER_PENDING_REMOVE_ONCE");
}

void RecoverMigrations(const std::filesystem::path& root, AccessMode access_mode) {
    (void)ReadMigrationRecords(root);
    if (const auto journal = ReadMigrationJournal(root)) {
        ValidateMigrationJournal(root, *journal);
        if (access_mode == AccessMode::kReadOnly)
            throw std::runtime_error("migration recovery requires a read-write open");
        CompleteMigrationJournal(root, *journal);
    }
    if (access_mode == AccessMode::kReadWrite) {
        CleanupAtomicTmpArtifacts(root / kMigrationPendingFileName);
        CleanupAtomicTmpArtifacts(root / kMigrationsFileName);
    }
}

}  // namespace chunkdb
