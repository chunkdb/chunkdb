#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/types.hpp"
#include "users.hpp"

namespace chunkdb {

inline constexpr std::string_view kBackupMarkerName = "chunkdb.backup";
inline constexpr std::string_view kBackupIncompleteName = ".chunkdb.backup.incomplete";
inline constexpr std::string_view kRestoreIncompleteName = ".chunkdb.restore.incomplete";
inline constexpr std::string_view kBackupStagingName = ".chunkdb.backups";

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
using BackupCancel = std::function<bool()>;
struct BackupOptions {
    BackupCancel cancelled;
    std::optional<Users> users;
    // Prepare the caller's reply and do its last fallible work before the
    // durable completion point; no callback runs after publication commits.
    std::function<void(const BackupResult&)> before_publish;
};
class BackupBusyError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};
class BackupPublicationUnknownError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Validate an absent/empty destination outside the source, then durably
// establish its incomplete guard before any backup contents are copied.
void PrepareBackupTarget(const std::filesystem::path& source, const std::filesystem::path& target);
// Also checks the enclosing catalog when opening a table directory directly.
// This must run before a store/catalog creates directories or lock artifacts.
void RequireNotBackupDirectory(const std::filesystem::path& path);
[[nodiscard]] BackupFileRecord InspectBackupFile(const std::filesystem::path& root,
    const std::filesystem::path& relative_path, const BackupCancel& cancelled = {});
// Copies exactly size bytes in bounded blocks and returns their inventory.
// The source may grow afterwards; shortening its required prefix is damage.
[[nodiscard]] BackupFileRecord CopyBackupFile(const std::filesystem::path& source,
    const std::filesystem::path& root, const std::filesystem::path& relative_path,
    std::uint64_t size, const BackupCancel& cancelled = {});
[[nodiscard]] std::vector<std::uint8_t> SerializeBackupRecord(const BackupRecord& record);
[[nodiscard]] BackupRecord ParseBackupRecord(const std::vector<std::uint8_t>& bytes);
[[nodiscard]] BackupRecord ReadBackupRecord(const std::filesystem::path& root);
// Checks an exact, safe inventory plus every advertised table cut and its
// metadata. Throws on incomplete guards, unexpected entries or damage.
void ValidateBackupInventory(const std::filesystem::path& root, const BackupRecord& record);
// Syncs the contents/marker and removes the guard last. A failed completion
// sync durably restores the guard, or reports an unknown publication outcome.
void CompleteBackup(const std::filesystem::path& root, const BackupRecord& record,
    const BackupCancel& cancelled = {});
void RestoreBackup(const std::filesystem::path& backup, const std::filesystem::path& target);

} // namespace chunkdb
