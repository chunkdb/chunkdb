#include "backup.hpp"

#include <algorithm>
#include <array>
#include <fstream>
#include <limits>
#include <sstream>
#include <system_error>

#include "checkpoint.hpp"
#include "file_permissions.hpp"
#include "chunkdb/file_layout.hpp"
#include "feature_flags.hpp"
#include "feed_slot_records.hpp"
#include "store_manifest.hpp"
#include "commit_owner_bootstrap.hpp"
#include "verify.hpp"
#include "migrations_records.hpp"
#include "wal_replay.hpp"
#include "chunk_coordinate_index.hpp"
#include "shared_file_input.hpp"

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
ChunkCoord Coordinate(const std::filesystem::path& path, const Geometry& geometry) {
    ChunkCoord coord{};
    if (!ParseChunkFilePath(path, geometry, &coord))
        throw std::runtime_error("invalid restored chunk name");
    return coord;
}
std::vector<std::uint8_t> RecoverWal(const std::filesystem::path& path, const Geometry& geometry,
    ChunkCoord coord, const StoreId& epoch, FeatureFlags features, const ChunkCoordinateIndex& retained) {
    std::vector<std::uint8_t> payload(geometry.ChunkPayloadBytes(), 0U), presence(ChunkPresenceBitmapBytes(geometry), 0U);
    ChunkVars vars; std::uint64_t revision = 0U, schema = 0U, time = 0U;
    std::shared_ptr<const ChunkColumnState> chunk_columns;
    auto image_path = path; image_path.replace_extension(".chk");
    if (std::filesystem::exists(image_path)) {
        auto image = ParseChunkImage(LoadChunkImageFile(image_path, geometry, coord, epoch, features),
            geometry, coord, epoch, features);
        revision = image.revision; schema = image.schema_version; time = image.commit_time_ms;
        payload = std::move(image.payload); presence = std::move(image.presence_bitmap); vars = std::move(image.vars);
        chunk_columns = std::move(image.chunk_columns);
    }
    const auto boundary = retained.EmptyBoundary(coord);
    if (boundary)
        (void)ApplyRetainedEmptyBase(geometry, *boundary, revision, time, schema,
            payload, presence, vars, chunk_columns);
    auto bytes = LoadFile(path);
    const auto replay = ReplayWal(bytes, geometry, coord, epoch, features, revision, schema,
        &payload, &presence, &vars, nullptr, &chunk_columns, time,
        boundary ? std::optional{WalKnownIdentity{boundary->revision, boundary->commit_time_ms}} : std::nullopt);
    if (replay.torn_creation) return {};
    if (!replay.replayable || (replay.tail_truncated_or_corrupt && !replay.stopped_at_crash_tail) || !replay.vars_problem.empty())
        throw std::runtime_error("restored WAL is damaged: " + path.string());
    if (replay.tail_truncated_or_corrupt) bytes.resize(replay.valid_end);
    return bytes;
}

class SinkBuffer final : public std::streambuf {
  public:
    explicit SinkBuffer(const AtomicFileSink& sink) : sink_(sink) {}
  private:
    std::streamsize xsputn(const char* data, std::streamsize count) override {
        sink_(std::span(reinterpret_cast<const std::uint8_t*>(data), static_cast<std::size_t>(count)));
        return count;
    }
    int_type overflow(int_type value) override {
        if (traits_type::eq_int_type(value, traits_type::eof())) return traits_type::not_eof(value);
        const auto byte = static_cast<std::uint8_t>(traits_type::to_char_type(value));
        sink_(std::span(&byte, 1U));
        return value;
    }
    const AtomicFileSink& sink_;
};

void RewriteRetainedIndex(const std::filesystem::path& directory, const Geometry& geometry,
    const StoreId& old_epoch, const StoreId& new_epoch) {
    const auto inventory_path = directory / kChunkIndexFileName;
    SharedFileInput inventory(inventory_path);
    if (!inventory) throw std::runtime_error("restored coordinate inventory is missing");
    std::vector<IndexInventoryEntry> entries;
    auto header = ReadIndexInventory(inventory, std::filesystem::file_size(inventory_path), geometry, old_epoch,
        [&](const auto& entry) { entries.push_back(entry); });
    inventory.close();
    if (header.compaction_pending) throw std::runtime_error("restored coordinate compaction is pending");
    for (const auto& entry : entries) {
        const auto path = LargeChunkDirectory(directory, entry.coord, geometry.dims()) / kChunkIndexFileName;
        {
            SharedFileInput input(path);
            if (!input) throw std::runtime_error("restored coordinate log is missing");
            (void)ReadChunkIndexLog(input, std::filesystem::file_size(path), geometry, old_epoch,
                entry.coord, entry.committed_bytes);
        }
        const auto log_header = SerializeChunkIndexHeader({new_epoch, geometry.dims(), entry.coord});
        AtomicWriteStream(path, [&](const AtomicFileSink& sink) {
            SharedFileInput input(path);
            if (!input) throw std::runtime_error("failed to reopen restored coordinate log");
            input.seekg(static_cast<std::streamoff>(log_header.size()));
            if (!input) throw std::runtime_error("failed to seek restored coordinate log");
            sink(log_header);
            std::array<std::uint8_t, 65536> bytes;
            auto remaining = entry.committed_bytes - log_header.size();
            while (remaining != 0U) {
                const auto count = static_cast<std::size_t>(std::min<std::uint64_t>(remaining, bytes.size()));
                input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(count));
                if (input.gcount() != static_cast<std::streamsize>(count))
                    throw std::runtime_error("restored coordinate log changed during identity rewrite");
                sink(std::span(bytes.data(), count));
                remaining -= count;
            }
        }, true, true);
        Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_INDEX_LOG_ONCE");
    }
    header.table_id = new_epoch;
    AtomicWriteStream(inventory_path, [&](const AtomicFileSink& sink) {
        SinkBuffer buffer(sink);
        std::ostream output(&buffer);
        WriteIndexInventory(output, header, entries);
        if (!output) throw std::runtime_error("failed to rewrite restored coordinate inventory");
    }, true, true);
    Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_INDEX_ONCE");
}
} // namespace
void RestoreBackup(const std::filesystem::path& source, const std::filesystem::path& destination) {
    RequireBackupTarget(source, destination);
    const auto backup = std::filesystem::canonical(source);
    const auto target = std::filesystem::weakly_canonical(destination);
    auto record = ReadBackupRecord(backup);
    ValidateBackupInventory(backup, record);
    for (const auto& cut : record.tables) {
        const auto manifest = ReadStoreManifest(backup / "tables" / cut.name);
        if (!manifest || (manifest->features.incompat & kFeatureTableLineage) == 0U)
            throw FeatureAdmissionError("feature_disabled", "table_lineage",
                "restore requires table_lineage on backed-up tables; enable it on the source and create another backup");
        if (cut.revision == std::numeric_limits<std::uint64_t>::max())
            throw std::overflow_error("restore fork revision has no successor");
    }
    std::ostringstream findings;
    const auto verified = VerifyDataDirectory(backup, findings);
    if (verified.errors != 0U) throw std::runtime_error("backup storage verification failed: " + findings.str());
    Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_VERIFY_ONCE");
    EnsureDirectoryPathExists(target.parent_path(), true);
    for (const auto& entry : std::filesystem::directory_iterator(target.parent_path()))
        if (entry.path().filename().string().rfind(".chunkdb.restore.", 0U) == 0U)
            throw std::runtime_error("an interrupted restore temporary copy remains: " + entry.path().string());
    const auto temporary = target.parent_path() / (".chunkdb.restore." + StoreIdHex(NewStoreId()));
    if (!CreateOwnerOnlyDirectory(temporary)) throw std::runtime_error("restore temporary directory already exists");
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
        if (directory_manifest.commit_owner)
            directory_manifest = CommitOwnerBootstrap::RewriteInactiveRestore(temporary, record);
        else directory_manifest.data_dir_id = NewStoreId();
        if (directory_manifest.data_dir_id == old_directory_id) throw std::runtime_error("restore generated an existing data-directory id");
        AtomicWrite(DataDirManifestPath(temporary), SerializeDataDirManifest(directory_manifest), true, true);
        if (std::filesystem::exists(temporary / kMigrationsFileName))
            AtomicWrite(temporary / kMigrationsFileName, EncodeMigrationRecords(directory_manifest.data_dir_id, migrations), true, true);
        for (auto& cut : record.tables) {
            const auto directory = temporary / "tables" / cut.name;
            auto manifest = *ReadStoreManifest(directory);
            RequireOpenableFeatures(manifest.features, AccessMode::kReadWrite);
            Geometry geometry(manifest.geometry, manifest.schema, *manifest.chunk_schema);
            const auto old_epoch = cut.epoch; const auto new_epoch = NewStoreId();
            if (new_epoch == old_epoch) throw std::runtime_error("restore generated an existing epoch");
            const auto prefix = std::filesystem::path("tables") / cut.name;
            ChunkCoordinateIndex retained(directory, geometry, old_epoch, true);
            // Normalize crash-consistent WAL tails while the checkpoint still has
            // its old epoch, then rewrite the identity without changing history.
            for (const auto& file : record.files) {
                const auto relative = file.relative_path.lexically_relative(prefix);
                if (std::distance(relative.begin(), relative.end()) != 2 || relative.begin()->string() == "..") continue;
                const auto path = temporary / file.relative_path;
                if (relative.extension() == ".wal") {
                    const auto coord = Coordinate(relative, geometry);
                    auto bytes = RecoverWal(path, geometry, coord, old_epoch, manifest.features, retained);
                    if (bytes.empty()) {
                        std::filesystem::remove(path);
                        if (std::filesystem::is_empty(path.parent_path())) std::filesystem::remove(path.parent_path());
                    } else {
                        bytes = RewriteWalStoreId(std::move(bytes), coord, old_epoch, new_epoch,
                            manifest.features, geometry.dims());
                        AtomicWrite(path, bytes, true, true);
                    }
                }
            }
            for (const auto& file : record.files) {
                const auto relative = file.relative_path.lexically_relative(prefix);
                if (std::distance(relative.begin(), relative.end()) != 2 || relative.begin()->string() == "..") continue;
                const auto path = temporary / file.relative_path;
                if (relative.extension() == ".chk") {
                    auto bytes = RewriteChunkImageStoreId(LoadFile(path), geometry, Coordinate(relative, geometry), old_epoch, new_epoch, manifest.features);
                    AtomicWrite(path, bytes, true, true);
                }
                Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_FILE_ONCE");
            }
            RewriteRetainedIndex(directory, geometry, old_epoch, new_epoch);
            if (auto slots = ReadFeedSlotRecords(directory, old_epoch, geometry.dims())) {
                slots->epoch = new_epoch; slots->durable_watermark = cut.revision;
                for (auto& slot : slots->slots) { slot.written = cut.revision; slot.lost = slot.schema_filter_lost; }
                WriteFeedSlotRecords(directory, *slots);
            }
            // The verified backup contains every committed image/WAL/slot at
            // or below this cut. Reservations above it belonged to the old
            // epoch, so publish its checked successor before the new identity.
            AtomicWrite(directory / "chunkdb.version", SerializeVersionClockRecord(cut.revision + 1U), true, true);
            Crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_FORK_CLOCK_ONCE");
            manifest.store_id = new_epoch;
            manifest.commit_owner = directory_manifest.commit_owner;
            manifest.lineage = TableLineage{old_epoch, cut.revision};
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
