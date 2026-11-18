#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "users.hpp"

namespace chunkdb {

inline constexpr std::string_view kBackupMarkerName = "chunkdb.backup";
inline constexpr std::string_view kBackupIncompleteName = ".chunkdb.backup.incomplete";
inline constexpr std::string_view kRestoreIncompleteName = ".chunkdb.restore.incomplete";
inline constexpr std::string_view kBackupStagingName = ".chunkdb.backups";
inline constexpr std::string_view kBackupStagingOwnerName = ".chunkdb.backup.owner";

struct BackupTableCut {
    std::string name;
    StoreId epoch{};
    std::uint64_t revision = 0;
};
struct BackupFileRecord {
    std::filesystem::path relative_path;
    std::uint64_t size = 0;
    std::uint32_t crc32 = 0;
};
struct BackupRecord {
    std::uint64_t created_at_ms = 0;
    std::vector<BackupTableCut> tables;
    std::vector<BackupFileRecord> files;
};
struct BackupResult {
    std::vector<BackupTableCut> tables;
    std::uint64_t files_count = 0;
    std::uint64_t bytes = 0;
};
// Deterministic pin/copy boundaries, following the storage test-hook pattern.
struct BackupTestHook {
    enum class Point { kBeforeTargetCreate, kBeforeCopyCreate, kBeforeStagingCreate, kAfterTargetGuard, kBeforeTablePin, kBeforeLargeChunkPin, kBeforeMaintenanceWait, kAfterCut, kWaitingForCompletion, kAfterFlush, kAfterPin, kBeforeCopy, kAfterCopy, kBeforePublish, kBeforeWalReplayRead, kAfterTargetValidation, kBeforeSourceDirectoryOpen };
    virtual ~BackupTestHook() = default;
    virtual void Run(Point point, std::string_view table, std::uint64_t revision) = 0;
};
using BackupCancel = std::stop_token;
struct BackupOptions {
    BackupCancel cancelled{};
    // Prepare the caller's reply and do its last fallible work before the
    // durable completion point; no callback runs after publication commits.
    std::function<void(const BackupResult&)> before_publish{};
    // The configured --backup-dir the target lies under; see PrepareBackupTarget.
    std::filesystem::path backup_directory{};
};
class BackupBusyError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};
class BackupPublicationUnknownError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Only staging children bearing this catalog identity and their exact name
// may be removed as orphans, including under a symlinked staging root.
void WriteBackupStagingOwner(const std::filesystem::path& staging, const StoreId& data_dir_id);
[[nodiscard]] bool IsOwnedBackupStaging(const std::filesystem::path& staging, const StoreId& data_dir_id);
// A failed removal is logged as a warning and leaves the directory for the
// next backup or writer start.
void RemoveBackupStaging(const std::filesystem::path& staging) noexcept;
// Removes a private temporary tree without following descendant aliases.
[[nodiscard]] std::error_code RemoveBackupTemporaryTree(const std::filesystem::path& root);
// Removes staging left by failed or interrupted backups of this catalog,
// following a configured staging root alias. The caller holds the writer lock
// and either the backup guard or a catalog open that no backup can enter yet,
// so no backup that could own an entry is running. The root, unowned entries
// and backup or restore destinations are kept and logged as warnings.
void RemoveOrphanedBackupStaging(const std::filesystem::path& data_dir, const StoreId& data_dir_id);

// Validate an absent/empty destination outside the source, then durably
// establish its incomplete guard before any backup contents are copied.
// Resolve configured aliases at the boundary; descendant walks stay no-follow.
[[nodiscard]] std::filesystem::path CanonicalBackupPath(const std::filesystem::path& path);
[[nodiscard]] std::filesystem::path ResolveBackupTarget(const std::filesystem::path& backup_directory,
    const std::filesystem::path& requested, BackupTestHook* hook = nullptr);
void RequireBackupTarget(const std::filesystem::path& source, const std::filesystem::path& target);
// With `backup_directory`, resolved as ResolveBackupTarget resolves it, the
// target must lie under it. On Linux and macOS, it and every existing directory
// from it down to the target must be owned by the server user or root and not
// writable by group or others; the target itself is always checked there.
void PrepareBackupTarget(const std::filesystem::path& source, const std::filesystem::path& target, BackupTestHook* hook = nullptr,
    const std::filesystem::path& backup_directory = {});
void EnsureBackupDirectory(const std::filesystem::path& path);
// Also checks the enclosing catalog when opening a table directory directly.
// This must run before a store/catalog creates directories or lock artifacts.
// Read-only opening accepts a completed backup; incomplete guards and a
// marker that is not a regular file are refused in every mode.
void RequireNotBackupDirectory(const std::filesystem::path& path, AccessMode access_mode = AccessMode::kReadWrite);
[[nodiscard]] BackupFileRecord InspectBackupFile(const std::filesystem::path& root,
    const std::filesystem::path& relative_path, const BackupCancel& cancelled = {});
// Copies exactly size bytes in bounded blocks and returns their inventory.
// The source may grow afterwards; shortening its required prefix is damage.
[[nodiscard]] BackupFileRecord CopyBackupFile(const std::filesystem::path& source,
    const std::filesystem::path& root, const std::filesystem::path& relative_path,
    std::uint64_t size, const BackupCancel& cancelled = {}, BackupTestHook* hook = nullptr);
// Reads only a pinned prefix, permitting replacement of its live name.
[[nodiscard]] std::vector<std::uint8_t> ReadBackupFile(const std::filesystem::path& source,
    std::uint64_t size, const BackupCancel& cancelled = {});
[[nodiscard]] std::vector<std::uint8_t> SerializeBackupRecord(const BackupRecord& record);
[[nodiscard]] BackupRecord ParseBackupRecord(const std::vector<std::uint8_t>& bytes);
[[nodiscard]] BackupRecord ReadBackupRecord(const std::filesystem::path& root);
// Checks an exact, safe inventory plus every advertised table cut and its
// metadata. Throws on incomplete guards, unexpected entries or damage.
// Internal check while the publisher still owns its incomplete guard.
void ValidateBackupContents(const std::filesystem::path& root, const BackupRecord& record);
void ValidateBackupInventory(const std::filesystem::path& root, const BackupRecord& record);
// Syncs the contents/marker and removes the guard last. A failed completion
// sync durably restores the guard, or reports an unknown publication outcome.
void CompleteBackup(const std::filesystem::path& root, const BackupRecord& record,
    const BackupCancel& cancelled = {});
// Internal publication helpers shared by backup creation and restore.
void SyncBackupTree(const std::filesystem::path& root, const BackupCancel& cancelled = {});
void CompleteBackupGuard(const std::filesystem::path& root, std::string_view guard, std::string_view phase);
void RestoreBackup(const std::filesystem::path& backup, const std::filesystem::path& target);

// The completed backup a RESTORE TABLE statement names, resolved under the
// configured --backup-dir with the BACKUP TO path rules.
[[nodiscard]] std::filesystem::path ResolveBackupSource(const std::filesystem::path& backup_directory,
    const std::filesystem::path& requested);
// One table of a completed backup, copied into a private directory.
struct BackupTableCopy {
    BackupTableCut cut;
    // Relative to the copied table directory.
    std::vector<std::filesystem::path> files;
    std::uint64_t bytes = 0;
};
using BackupCopyProgress = std::function<void(std::uint64_t copied_files, std::uint64_t copied_bytes,
    std::uint64_t total_files, std::uint64_t total_bytes)>;
// Copies table `table` of the completed backup `backup` into the existing
// empty `directory`. The backup's table directory must hold exactly its
// inventory files, all regular; every copied length and CRC must match the
// inventory, and the copy must pass the inventory check of its cut. Throws
// std::invalid_argument when the backup has no such table.
[[nodiscard]] BackupTableCopy CopyBackupTable(const std::filesystem::path& backup, std::string_view table,
    const std::filesystem::path& directory, const BackupCancel& cancelled = {}, const BackupCopyProgress& progress = {});
// Gives a copied table directory a new epoch whose lineage is `cut`: WAL
// crash tails are normalized, every image, WAL, coordinate log and slot is
// rebound, the clock restarts at the cut's successor, and the manifest is
// written last. `files` are the table's inventory files relative to it.
// Returns the new epoch.
[[nodiscard]] StoreId RebindRestoredTable(const std::filesystem::path& directory, const BackupTableCut& cut,
    const std::vector<std::filesystem::path>& files, const BackupCancel& cancelled = {});

} // namespace chunkdb
