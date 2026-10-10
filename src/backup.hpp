#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
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
    enum class Point { kBeforeTargetCreate, kBeforeCopyCreate, kBeforeStagingCreate, kAfterTargetGuard, kBeforeTablePin, kBeforeLargeChunkPin, kBeforeMaintenanceWait, kAfterCut, kWaitingForCompletion, kAfterFlush, kAfterPin, kBeforeCopy, kAfterCopy, kBeforePublish };
    virtual ~BackupTestHook() = default;
    virtual void Run(Point point, std::string_view table, std::uint64_t revision) = 0;
};
using BackupCancel = std::stop_token;
struct BackupOptions {
    BackupCancel cancelled{};
    std::optional<Users> users{};
    // Prepare the caller's reply and do its last fallible work before the
    // durable completion point; no callback runs after publication commits.
    std::function<void(const BackupResult&)> before_publish{};
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
// may be removed at startup, including under a symlinked staging root.
void WriteBackupStagingOwner(const std::filesystem::path& staging, const StoreId& data_dir_id);
[[nodiscard]] bool IsOwnedBackupStaging(const std::filesystem::path& staging, const StoreId& data_dir_id);

// Validate an absent/empty destination outside the source, then durably
// establish its incomplete guard before any backup contents are copied.
[[nodiscard]] std::filesystem::path ResolveBackupTarget(const std::filesystem::path& backup_directory,
    const std::filesystem::path& requested);
void RequireBackupTarget(const std::filesystem::path& source, const std::filesystem::path& target);
void PrepareBackupTarget(const std::filesystem::path& source, const std::filesystem::path& target, BackupTestHook* hook = nullptr);
void EnsureBackupDirectory(const std::filesystem::path& path);
// Also checks the enclosing catalog when opening a table directory directly.
// This must run before a store/catalog creates directories or lock artifacts.
void RequireNotBackupDirectory(const std::filesystem::path& path);
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

} // namespace chunkdb
