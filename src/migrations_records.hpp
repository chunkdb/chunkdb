#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb {

inline constexpr std::string_view kMigrationsFileName = "chunkdb.migrations";
inline constexpr std::string_view kMigrationPendingFileName = "chunkdb.migration.pending";
inline constexpr std::size_t kMaxMigrationRecordsBytes = 16U * 1024U * 1024U;
inline constexpr std::size_t kMaxMigrationJournalBytes = 64U * 1024U * 1024U;

struct MigrationRecord {
    std::string name;
    std::uint64_t applied_ms = 0;
    std::string user;
    std::string statement;
    friend bool operator==(const MigrationRecord&, const MigrationRecord&) = default;
};

struct MigrationFileChange {
    std::string path;
    std::optional<std::vector<std::uint8_t>> before;
    std::vector<std::uint8_t> after;
};

// kRename moves `tables/<table>` to `tables/<operation_name>`; only a catalog
// operation uses it.
enum class MigrationDirectoryAction : std::uint8_t { kNone, kCreate, kDrop, kRename };

struct MigrationJournal {
    StoreId data_dir_id{};
    // A DROP TABLE or ALTER TABLE ... RENAME TO outside MIGRATE (format
    // version 2): no record and no ledger; its directory move is its commit
    // point, and recovery completes it only once the move happened.
    bool catalog_operation = false;
    MigrationRecord record;
    std::vector<MigrationFileChange> files;
    MigrationDirectoryAction directory_action = MigrationDirectoryAction::kNone;
    std::string table;
    StoreId table_id{};
    std::string operation_name;
};

void RequireValidMigrationName(std::string_view name);
[[nodiscard]] std::vector<std::uint8_t> EncodeMigrationRecords(
    const StoreId& data_dir_id, const std::vector<MigrationRecord>& records);
[[nodiscard]] std::vector<MigrationRecord> DecodeMigrationRecords(
    const std::vector<std::uint8_t>& bytes, const StoreId& data_dir_id);
// Missing ledger is an empty list; malformed or foreign state is an error.
[[nodiscard]] std::vector<MigrationRecord> ReadMigrationRecords(const std::filesystem::path& root);
[[nodiscard]] std::vector<std::uint8_t> EncodeMigrationJournal(const MigrationJournal& journal);
[[nodiscard]] MigrationJournal DecodeMigrationJournal(const std::vector<std::uint8_t>& bytes);
[[nodiscard]] std::optional<MigrationJournal> ReadMigrationJournal(const std::filesystem::path& root);
void WriteMigrationJournal(const std::filesystem::path& root, const MigrationJournal& journal, bool* published = nullptr);
// Checks all participants and identities before any recovery mutation.
void ValidateMigrationJournal(const std::filesystem::path& root, const MigrationJournal& journal);
void CompleteMigrationJournal(const std::filesystem::path& root, const MigrationJournal& journal);
// For a catalog operation: whether its directory move happened, that is,
// whether `tables/<table>` is gone.
[[nodiscard]] bool CatalogOperationMoved(const std::filesystem::path& root, const MigrationJournal& journal);
// Removes the journal of a catalog operation whose move did not happen.
void DiscardCatalogOperation(const std::filesystem::path& root);
void RecoverMigrations(const std::filesystem::path& root, AccessMode access_mode);

}  // namespace chunkdb
