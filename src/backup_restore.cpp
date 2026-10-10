#include "backup.hpp"

#include <algorithm>
#include <sstream>
#include <system_error>

#include "checkpoint.hpp"
#include "chunkdb/file_layout.hpp"
#include "feature_flags.hpp"
#include "feed_slot_records.hpp"
#include "store_manifest.hpp"
#include "verify.hpp"
#include "migrations_records.hpp"
#include "wal_replay.hpp"

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
std::vector<std::uint8_t> RecoverWal(const std::filesystem::path& path, const Geometry& geometry,
    ChunkCoord coord, const StoreId& epoch, FeatureFlags features) {
    std::vector<std::uint8_t> payload(geometry.ChunkPayloadBytes(), 0U), presence(ChunkPresenceBitmapBytes(geometry), 0U);
    ChunkVars vars; std::uint64_t revision = 0U, schema = 0U;
    auto image_path = path; image_path.replace_extension(".chk");
    if (std::filesystem::exists(image_path)) {
        auto image = ParseChunkImage(LoadFile(image_path), geometry, coord, epoch, features);
        revision = image.revision; schema = image.schema_version;
        payload = std::move(image.payload); presence = std::move(image.presence_bitmap); vars = std::move(image.vars);
    }
    auto bytes = LoadFile(path);
    const auto replay = ReplayWal(bytes, geometry, coord, epoch, features, revision, schema, &payload, &presence, &vars);
    if (replay.torn_creation) return {};
    if (!replay.replayable || (replay.tail_truncated_or_corrupt && !replay.stopped_at_crash_tail) || !replay.vars_problem.empty())
        throw std::runtime_error("restored WAL is damaged: " + path.string());
    if (replay.tail_truncated_or_corrupt) bytes.resize(replay.valid_end);
    return bytes;
}
} // namespace
void RestoreBackup(const std::filesystem::path& source, const std::filesystem::path& destination) {
    RequireBackupTarget(source, destination);
    const auto backup = std::filesystem::canonical(source);
    const auto target = std::filesystem::weakly_canonical(destination);
    auto record = ReadBackupRecord(backup);
    ValidateBackupInventory(backup, record);
    std::ostringstream findings;
    const auto verified = VerifyDataDirectory(backup, findings);
    if (verified.errors != 0U) throw std::runtime_error("backup storage verification failed: " + findings.str());
    Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_VERIFY_ONCE");
    EnsureDirectoryPathExists(target.parent_path(), true);
    for (const auto& entry : std::filesystem::directory_iterator(target.parent_path()))
        if (entry.path().filename().string().rfind(".chunkdb.restore.", 0U) == 0U)
            throw std::runtime_error("an interrupted restore temporary copy remains: " + entry.path().string());
    const auto temporary = target.parent_path() / (".chunkdb.restore." + StoreIdHex(NewStoreId()));
    if (!std::filesystem::create_directory(temporary)) throw std::runtime_error("restore temporary directory already exists");
    bool moved = false;
    try {
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
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_RESTORE_COPY_FAIL_ONCE"))
                throw std::runtime_error("injected restore copy failure");
            Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_COPY_FILE_ONCE");
        }
        Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_COPY_ONCE");
        auto directory_manifest = *ReadDataDirManifest(temporary);
        RequireOpenableFeatures(directory_manifest.features, AccessMode::kReadWrite);
        const auto old_directory_id = directory_manifest.data_dir_id;
        const auto migrations = ReadMigrationRecords(temporary);
        directory_manifest.data_dir_id = NewStoreId();
        if (directory_manifest.data_dir_id == old_directory_id) throw std::runtime_error("restore generated an existing data-directory id");
        AtomicWrite(DataDirManifestPath(temporary), SerializeDataDirManifest(directory_manifest), true, true);
        if (std::filesystem::exists(temporary / kMigrationsFileName))
            AtomicWrite(temporary / kMigrationsFileName, EncodeMigrationRecords(directory_manifest.data_dir_id, migrations), true, true);
        for (auto& cut : record.tables) {
            const auto directory = temporary / "tables" / cut.name;
            auto manifest = *ReadStoreManifest(directory);
            RequireOpenableFeatures(manifest.features, AccessMode::kReadWrite);
            Geometry geometry(manifest.geometry, manifest.schema);
            const auto old_epoch = cut.epoch; const auto new_epoch = NewStoreId();
            if (new_epoch == old_epoch) throw std::runtime_error("restore generated an existing epoch");
            const auto prefix = std::filesystem::path("tables") / cut.name;
            // Normalize crash-consistent WAL tails while the checkpoint still has
            // its old epoch, then rewrite the identity without changing history.
            for (const auto& file : record.files) {
                const auto relative = file.relative_path.lexically_relative(prefix);
                if (std::distance(relative.begin(), relative.end()) != 2 || relative.begin()->string() == "..") continue;
                const auto path = temporary / file.relative_path;
                if (relative.extension() == ".wal") {
                    const auto coord = Coordinate(relative);
                    auto bytes = RecoverWal(path, geometry, coord, old_epoch, manifest.features);
                    if (bytes.empty()) {
                        std::filesystem::remove(path);
                        if (std::filesystem::is_empty(path.parent_path())) std::filesystem::remove(path.parent_path());
                    } else {
                        bytes = RewriteWalStoreId(std::move(bytes), coord, old_epoch, new_epoch, manifest.features);
                        AtomicWrite(path, bytes, true, true);
                    }
                }
            }
            for (const auto& file : record.files) {
                const auto relative = file.relative_path.lexically_relative(prefix);
                if (std::distance(relative.begin(), relative.end()) != 2 || relative.begin()->string() == "..") continue;
                const auto path = temporary / file.relative_path;
                if (relative.extension() == ".chk") {
                    auto bytes = RewriteChunkImageStoreId(LoadFile(path), geometry, Coordinate(relative), old_epoch, new_epoch, manifest.features);
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
        std::erase_if(record.files, [&](const auto& file) { return !std::filesystem::exists(temporary / file.relative_path); });
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
        moved = true;
        Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_RENAME_ONCE");
        SyncDirectoryPath(target.parent_path());
        Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_PARENT_SYNC_ONCE");
        CompleteBackupGuard(target, kRestoreIncompleteName, "RESTORE");
        // No cleanup or fallible callback follows the completion point.
    } catch (const std::exception& failure) {
        if (!moved) {
            std::error_code error;
            std::filesystem::remove_all(temporary, error);
            if (error) throw std::runtime_error(std::string(failure.what()) + "; restore temporary cleanup failed: " + temporary.string() + ": " + error.message());
        }
        throw;
    }
}
} // namespace chunkdb
