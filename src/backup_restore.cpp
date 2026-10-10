#include "backup.hpp"

#include <sstream>
#include <system_error>

#include "checkpoint.hpp"
#include "chunkdb/file_layout.hpp"
#include "feature_flags.hpp"
#include "feed_slot_records.hpp"
#include "store_manifest.hpp"
#include "verify.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#if defined(__linux__)
#include <sys/syscall.h>
#endif
#endif

namespace chunkdb {
namespace {
void Crash(const char* point) noexcept { if (ConsumeFailpointEnv(point)) std::_Exit(86); }
void ExclusiveMove(const std::filesystem::path& from, const std::filesystem::path& to) {
#ifdef _WIN32
    if (MoveFileExW(from.wstring().c_str(), to.wstring().c_str(), MOVEFILE_WRITE_THROUGH) == 0)
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "restore exclusive directory rename failed");
#elif defined(__APPLE__)
    if (::renamex_np(from.c_str(), to.c_str(), RENAME_EXCL) != 0)
        throw std::system_error(errno, std::generic_category(), "restore exclusive directory rename failed");
#elif defined(__linux__) && defined(SYS_renameat2)
    if (::syscall(SYS_renameat2, AT_FDCWD, from.c_str(), AT_FDCWD, to.c_str(), 1U) != 0)
        throw std::system_error(errno, std::generic_category(), "restore exclusive directory rename failed");
#else
    (void)from; (void)to;
    throw std::runtime_error("restore requires atomic exclusive directory rename support");
#endif
}
ChunkCoord Coordinate(const std::filesystem::path& path) {
    const auto stem = path.stem().string(); const auto separator = stem.find('_', 2U);
    ChunkCoord coord{};
    if (stem.rfind("C_", 0U) != 0U || separator == std::string::npos ||
        !TryParseInt64(stem.substr(2U, separator - 2U), &coord.x) || !TryParseInt64(stem.substr(separator + 1U), &coord.y))
        throw std::runtime_error("invalid restored chunk name");
    return coord;
}
} // namespace
void RestoreBackup(const std::filesystem::path& source, const std::filesystem::path& destination) {
    const auto backup = std::filesystem::absolute(source).lexically_normal();
    const auto target = std::filesystem::absolute(destination).lexically_normal();
    RequireBackupTarget(backup, target);
    auto record = ReadBackupRecord(backup);
    ValidateBackupInventory(backup, record);
    std::ostringstream findings;
    const auto verified = VerifyDataDirectory(backup, findings);
    if (verified.errors != 0U) throw std::runtime_error("backup storage verification failed: " + findings.str());
    Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_VERIFY_ONCE");
    EnsureDirectoryPathExists(target.parent_path(), true);
    const auto temporary = target.parent_path() / (".chunkdb.restore." + StoreIdHex(NewStoreId()));
    if (!std::filesystem::create_directory(temporary)) throw std::runtime_error("restore temporary directory already exists");
    // Keep the guard inside the renamed tree, so every visible partial target
    // is refused independently of the name of its temporary sibling.
    if (!PublishNewFile(temporary / kRestoreIncompleteName, std::vector<std::uint8_t>{'C', 'K', 'R', 'I'}))
        throw std::runtime_error("restore temporary directory is already owned");
    SyncDirectoryPath(target.parent_path());
    Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_GUARD_ONCE");
    EnsureDirectoryPathExists(temporary / "tables", true);
    for (const auto& file : record.files) {
        const auto copied = CopyBackupFile(backup / file.relative_path, temporary, file.relative_path, file.size);
        if (copied.crc32 != file.crc32) throw std::runtime_error("backup changed while being restored");
        Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_COPY_FILE_ONCE");
    }
    Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_COPY_ONCE");
    for (auto& cut : record.tables) {
        const auto directory = temporary / "tables" / cut.name;
        auto manifest = *ReadStoreManifest(directory);
        RequireOpenableFeatures(manifest.features, AccessMode::kReadWrite);
        Geometry geometry(manifest.geometry, manifest.schema);
        const auto old_epoch = cut.epoch; const auto new_epoch = NewStoreId();
        if (new_epoch == old_epoch) throw std::runtime_error("restore generated an existing epoch");
        const auto prefix = std::filesystem::path("tables") / cut.name;
        for (const auto& file : record.files) {
            const auto relative = file.relative_path.lexically_relative(prefix);
            if (std::distance(relative.begin(), relative.end()) != 2 || relative.begin()->string() == "..") continue;
            const auto path = temporary / file.relative_path;
            if (relative.extension() == ".chk") {
                auto bytes = RewriteChunkImageStoreId(LoadFile(path), geometry, Coordinate(relative), old_epoch, new_epoch, manifest.features);
                AtomicWrite(path, bytes, true, true);
            } else if (relative.extension() == ".wal") {
                auto bytes = RewriteWalStoreId(LoadFile(path), Coordinate(relative), old_epoch, new_epoch, manifest.features);
                AtomicWrite(path, bytes, true, true);
            }
            Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_FILE_ONCE");
        }
        if (auto slots = ReadFeedSlotRecords(directory, old_epoch)) {
            slots->epoch = new_epoch; slots->durable_watermark = cut.revision;
            for (auto& slot : slots->slots) { slot.written = cut.revision; slot.lost = false; }
            WriteFeedSlotRecords(directory, *slots);
        }
        manifest.store_id = new_epoch;
        AtomicWrite(StoreManifestPath(directory), SerializeStoreManifest(manifest), true, true);
        cut.epoch = new_epoch;
        Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_TABLE_ONCE");
    }
    for (auto& file : record.files) file = InspectBackupFile(temporary, file.relative_path);
    ValidateBackupContents(temporary, record);
    SyncBackupTree(temporary);
    Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_SYNC_ONCE");
    // An initially empty target may still be used, but only if it is empty
    // now. The final exclusive rename never overwrites a competing entry.
    RequireBackupTarget(backup, target);
    if (std::filesystem::exists(target) && !std::filesystem::remove(target))
        throw std::runtime_error("restore destination ceased to be empty");
    ExclusiveMove(temporary, target);
    Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_RENAME_ONCE");
    SyncDirectoryPath(target.parent_path());
    Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_PARENT_SYNC_ONCE");
    CompleteBackupGuard(target, kRestoreIncompleteName, "RESTORE");
    // No cleanup or fallible callback follows the completion point.
}
} // namespace chunkdb
