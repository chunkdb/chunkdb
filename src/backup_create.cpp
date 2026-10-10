#include "backup.hpp"

#include <chrono>
#include <fstream>
#include <limits>
#include <regex>
#include <set>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feed_slots.hpp"
#include "snapshot_generation.hpp"
#include "store_manifest.hpp"
#include "wal_replay.hpp"

namespace chunkdb {
namespace {
void CheckCancelled(const BackupCancel& cancelled) {
    if (cancelled && cancelled()) throw std::runtime_error("backup cancelled");
}
template <typename Mutex>
std::unique_lock<Mutex> CancellableLock(Mutex& mutex, const BackupCancel& cancelled) {
    std::unique_lock<Mutex> lock(mutex, std::defer_lock);
    // Timed acquisition only provides cancellation while existing work drains.
    // Successful acquisition, rather than elapsed time, establishes the hold.
    while (!lock.try_lock_for(std::chrono::milliseconds(50))) CheckCancelled(cancelled);
    CheckCancelled(cancelled);
    return lock;
}
struct PinnedFile {
    std::filesystem::path relative;
    std::uint64_t required, observed;
};
struct StagingCleanup {
    std::filesystem::path path;
    ~StagingCleanup() {
        if (path.empty()) return;
        std::error_code error;
        std::filesystem::remove_all(path, error);
    }
};
void Save(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    file.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    file.close();
    if (!file) throw std::runtime_error("cannot write backup staging metadata: " + path.string());
}
void Crash(const char* point) { if (ConsumeFailpointEnv(point)) std::_Exit(86); }
}

BackupResult TableCatalog::BackupTo(const std::filesystem::path& target, const BackupOptions& options) {
    RequireWritable("backup");
    if (config_.allow_multiple_processes)
        throw std::invalid_argument("backup requires a single-process catalog");
    std::unique_lock backup_lock(backup_mutex_, std::try_to_lock);
    if (!backup_lock.owns_lock()) throw BackupBusyError("another backup is running");
    CheckCancelled(options.cancelled);
    PrepareBackupTarget(config_.data_dir, target);
    std::filesystem::create_directory(target / "tables");
    const auto hook = [&](BackupTestHook::Point point, std::string_view table = {}, std::uint64_t revision = 0) {
        if (auto* current = backup_hook_.load(std::memory_order_acquire)) current->Run(point, table, revision);
        CheckCancelled(options.cancelled);
    };
    hook(BackupTestHook::Point::kAfterTargetGuard);
    Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_TARGET_GUARD_ONCE");
    const auto stage_parent = config_.data_dir / kBackupStagingName;
    if (std::filesystem::is_symlink(std::filesystem::symlink_status(stage_parent)))
        throw std::runtime_error("backup staging directory is a symlink");
    std::filesystem::create_directories(stage_parent);
    StagingCleanup staging{stage_parent / StoreIdHex(NewStoreId())};
    if (!std::filesystem::create_directory(staging.path))
        throw std::runtime_error("backup staging directory already exists");
    std::vector<PinnedFile> files;
    BackupRecord record;
    record.created_at_ms = UnixMillisNow();
    const auto save = [&](const std::filesystem::path& relative, const std::vector<std::uint8_t>& bytes) {
        Save(staging.path / relative, bytes);
        files.push_back({relative, bytes.size(), bytes.size()});
    };
    const auto link = [&](const std::filesystem::path& source, const std::filesystem::path& relative,
                          std::uint64_t required, std::uint64_t observed) {
        if (!std::filesystem::is_regular_file(std::filesystem::symlink_status(source)))
            throw std::runtime_error("backup source is not a regular file: " + source.string());
        std::filesystem::create_directories((staging.path / relative).parent_path());
        std::filesystem::create_hard_link(source, staging.path / relative);
        files.push_back({relative, required, observed});
    };
    {
        auto operations = CancellableLock(operations_mutex_, options.cancelled);
        std::vector<std::shared_ptr<Table>> tables;
        {
            std::shared_lock lock(tables_mutex_);
            for (const auto& [_, table] : tables_) tables.push_back(table);
        }
        for (const auto& table : tables) {
            CheckCancelled(options.cancelled);
            auto pin = table->PinForBackup(options.cancelled);
            auto& store = pin.store();
            auto maintenance = CancellableLock(store.backup_maintenance_mutex_, options.cancelled);
            const auto require_healthy = [&] {
                store.ThrowIfDurabilityPoisoned();
                std::lock_guard lock(store.snapshot_generation_mutex_);
                if (store.snapshot_generation_epoch_failed_ ||
                    ((store.snapshot_generation_ & 1U) != 0U &&
                     store.snapshot_generation_active_writers_ == 0U &&
                     !store.snapshot_generation_linger_pending_))
                    throw std::runtime_error("backup refuses a failed snapshot generation");
            };
            require_healthy();
            std::uint64_t cut;
            {
                std::lock_guard lock(store.write_completion_mutex_);
                cut = store.version_clock_.load(std::memory_order_seq_cst) - 1U;
            }
            hook(BackupTestHook::Point::kAfterCut, table->name(), cut);
            {
                std::unique_lock lock(store.write_completion_mutex_);
                const auto completed = [&] {
                    for (auto* write = store.write_completions_; write != nullptr; write = write->next)
                        if (write->bound <= cut) return false;
                    return true;
                };
                while (!completed()) {
                    CheckCancelled(options.cancelled);
                    // Completion notifications drive this wait; timeout checks disconnect.
                    store.write_completion_cv_.wait_for(lock, std::chrono::milliseconds(50));
                }
            }
            Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_CUT_ONCE");
            require_healthy();
            std::vector<std::shared_ptr<ChunkStore::LargeChunk>> large_chunks;
            {
                std::lock_guard lock(store.large_chunks_mutex_);
                for (const auto& [_, large] : store.large_chunks_) large_chunks.push_back(large);
            }
            std::vector<std::pair<ChunkCoord, std::shared_ptr<ChunkStore::RegularChunk>>> chunks;
            for (const auto& large : large_chunks) {
                std::lock_guard lock(large->mutex);
                for (const auto& item : large->chunks) chunks.push_back(item);
            }
            std::set<std::pair<std::int64_t, std::int64_t>> coords;
            for (const auto& [coord, chunk] : chunks) {
                CheckCancelled(options.cancelled);
                std::unique_lock lock(chunk->mutex);
                if (chunk->wal_repair_failed) throw std::runtime_error("backup refuses an unrepaired WAL");
                store.FlushWalBatch(coord, chunk, false);
                coords.emplace(coord.x, coord.y);
            }
            hook(BackupTestHook::Point::kAfterFlush, table->name(), cut);
            Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_FLUSH_ONCE");
            // Cache registry locks are released before directory and file I/O.
            static const std::regex large_name(R"(^L_-?[0-9]+_-?[0-9]+$)");
            static const std::regex chunk_name(R"(^C_(-?[0-9]+)_(-?[0-9]+)\.(chk|wal)$)");
            for (const auto& large : std::filesystem::directory_iterator(store.data_dir_)) {
                if (!std::regex_match(large.path().filename().string(), large_name)) continue;
                if (!std::filesystem::is_directory(large.symlink_status()))
                    throw std::runtime_error("unsafe backup chunk directory: " + large.path().string());
                for (const auto& entry : std::filesystem::directory_iterator(large.path())) {
                    std::smatch match;
                    const auto name = entry.path().filename().string();
                    if (!std::regex_match(name, match, chunk_name)) continue;
                    if (!std::filesystem::is_regular_file(entry.symlink_status()))
                        throw std::runtime_error("unsafe backup chunk file: " + entry.path().string());
                    const ChunkCoord coord{std::stoll(match[1].str()), std::stoll(match[2].str())};
                    auto expected = ChunkWalPath(store.data_dir_, store.geometry_, coord);
                    expected.replace_extension(entry.path().extension());
                    if (entry.path() != expected)
                        throw std::runtime_error("backup chunk path is not canonical");
                    coords.emplace(coord.x, coord.y);
                }
            }
            const auto table_root = std::filesystem::path("tables") / table->name();
            for (const auto& [x, y] : coords) {
                CheckCancelled(options.cancelled);
                const ChunkCoord coord{x, y};
                const auto chunk = store.GetOrLoadRegularChunk(coord);
                std::unique_lock lock(chunk->mutex);
                if (chunk->wal_repair_failed) throw std::runtime_error("backup refuses an unrepaired WAL");
                store.FlushWalBatch(coord, chunk, false);
                auto payload = std::vector<std::uint8_t>(store.geometry_.ChunkPayloadBytes(), 0U);
                auto presence = std::vector<std::uint8_t>(ChunkPresenceBitmapBytes(store.geometry_), 0U);
                ChunkVars vars;
                std::uint64_t base = 0, schema = 0;
                const auto image_path = ChunkDataPath(store.data_dir_, store.geometry_, coord);
                if (std::filesystem::exists(image_path)) {
                    const auto bytes = LoadFile(image_path);
                    auto image = ParseChunkImage(bytes, store.geometry_, coord, store.store_id_, store.features_);
                    base = image.revision; schema = image.schema_version;
                    if (base > cut) throw std::runtime_error("backup image exceeds its revision cut");
                    payload = std::move(image.payload); presence = std::move(image.presence_bitmap); vars = std::move(image.vars);
                    link(image_path, table_root / std::filesystem::relative(image_path, store.data_dir_), bytes.size(), bytes.size());
                }
                const auto wal_path = ChunkWalPath(store.data_dir_, store.geometry_, coord);
                if (std::filesystem::exists(wal_path)) {
                    const auto bytes = LoadFile(wal_path);
                    std::vector<WalFrameBoundary> boundaries;
                    const auto replay = ReplayWal(bytes, store.geometry_, coord, store.store_id_, store.features_,
                        base, schema, &payload, &presence, &vars, &boundaries);
                    if (!replay.torn_creation && (!replay.replayable ||
                            (replay.tail_truncated_or_corrupt && !replay.stopped_at_crash_tail) || !replay.vars_problem.empty()))
                        throw std::runtime_error("backup WAL cannot be replayed: " + replay.stop_reason + " " + replay.vars_problem);
                    std::uint64_t required = 0;
                    for (const auto& boundary : boundaries) if (boundary.revision <= cut) required = boundary.end;
                    if (required != 0U)
                        link(wal_path, table_root / std::filesystem::relative(wal_path, store.data_dir_), required, bytes.size());
                }
            }
            require_healthy();
            save(table_root / "table.manifest", LoadFile(StoreManifestPath(store.data_dir_)));
            const auto ceiling = store.version_clock_ceiling_.load(std::memory_order_acquire);
            if (ceiling <= cut) throw std::runtime_error("backup clock does not exceed its revision cut");
            save(table_root / "chunkdb.version", SerializeVersionClockRecord(ceiling));
            save(table_root / "chunkdb.snapshot", SerializeSnapshotGenerationRecord(2U));
            const auto initialized = LoadFile(store.data_dir_ / ".chunkdb.initialized");
            if (!IsValidInitializedStoreMarker(initialized)) throw std::runtime_error("backup initialized marker is damaged");
            save(table_root / ".chunkdb.initialized", initialized);
            if (auto slots = ReadFeedSlotRecords(store.data_dir_, store.store_id_)) {
                slots->durable_watermark = std::min(slots->durable_watermark, cut);
                for (auto& slot : slots->slots) slot.written = std::min(slot.written, slots->durable_watermark);
                save(table_root / kFeedSlotsFileName, SerializeFeedSlotRecords(*slots));
            }
            record.tables.push_back({table->name(), store.store_id_, cut});
            hook(BackupTestHook::Point::kAfterPin, table->name(), cut);
            Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_PIN_ONCE");
        }
        save("chunkdb.manifest", LoadFile(DataDirManifestPath(config_.data_dir)));
        if (options.users) save(kUsersFileName, EncodeUsers(*options.users));
        else if (const auto users = ReadUsersFile(config_.data_dir)) save(kUsersFileName, EncodeUsers(*users));
    }
    hook(BackupTestHook::Point::kBeforeCopy);
    Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_BEFORE_COPY_ONCE");
    for (const auto& file : files) {
        CheckCancelled(options.cancelled);
        if (file.required > file.observed) throw std::logic_error("backup prefix exceeds its captured bound");
        record.files.push_back(CopyBackupFile(staging.path / file.relative, target, file.relative, file.required, options.cancelled));
    }
    hook(BackupTestHook::Point::kAfterCopy);
    Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_COPY_ONCE");
    std::filesystem::remove_all(staging.path);
    staging.path.clear();
    BackupResult result{record.tables, record.files.size(), 0U};
    for (const auto& file : record.files) {
        if (file.size > std::numeric_limits<std::uint64_t>::max() - result.bytes)
            throw std::overflow_error("backup byte count overflows");
        result.bytes += file.size;
    }
    hook(BackupTestHook::Point::kBeforePublish);
    if (options.before_publish) options.before_publish(result);
    CheckCancelled(options.cancelled);
    CompleteBackup(target, record, options.cancelled);
    return result;
}
} // namespace chunkdb
