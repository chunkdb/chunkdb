#include "backup.hpp"

#include <chrono>
#include <fstream>
#include <limits>
#include <map>
#include <regex>
#include <set>

#include "checkpoint.hpp"
#include "file_permissions.hpp"
#include "change_feed.hpp"
#include "chunkdb/logging.hpp"
#include "chunk_store_internal.hpp"
#include "chunk_coordinate_index.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feed_slots.hpp"
#include "migrations_records.hpp"
#include "snapshot_generation.hpp"
#include "store_manifest.hpp"
#include "wal_replay.hpp"
#include "shared_file_input.hpp"

namespace chunkdb {
namespace {
void CheckCancelled(const BackupCancel& cancelled) {
    if (cancelled.stop_requested()) throw std::runtime_error("backup cancelled");
}
std::unique_lock<BackupMaintenanceGate> CancellableLock(BackupMaintenanceGate& gate, const BackupCancel& cancelled) {
    if (!gate.lock(cancelled)) throw std::runtime_error("backup cancelled");
    return std::unique_lock<BackupMaintenanceGate>(gate, std::adopt_lock);
}
struct PinnedFile {
    std::filesystem::path relative;
    std::uint64_t required, observed;
};
struct StagingCleanup {
    std::filesystem::path path;
    ~StagingCleanup() {
        if (!path.empty()) RemoveBackupStaging(path);
    }
};
void Save(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    AtomicWrite(path, bytes, false, false, nullptr, nullptr, false);
}
void Crash(const char* point) { if (ConsumeFailpointEnv(point)) std::_Exit(86); }

struct RetainedBackupIndex {
    Geometry geometry;
    ChunkCoordinateIndex index;
    RetainedBackupIndex(const std::filesystem::path& root, const StoreManifest& manifest)
        : geometry(manifest.geometry, manifest.schema, *manifest.chunk_schema),
          index(root, geometry, manifest.store_id, true) {}
};
}

BackupResult TableCatalog::BackupTo(const std::filesystem::path& target, const BackupOptions& options) {
    RequireWritable("backup");
    if (!feature_metadata_gate_.lock_shared(options.cancelled)) throw std::runtime_error("backup cancelled");
    std::shared_lock feature_metadata(feature_metadata_gate_, std::adopt_lock);
    migration_health_->Check();
    if (config_.allow_multiple_processes)
        throw std::invalid_argument("backup requires a single-process catalog");
    std::unique_lock backup_lock(backup_mutex_, std::try_to_lock);
    if (!backup_lock.owns_lock()) throw BackupBusyError("another backup is running");
    CheckCancelled(options.cancelled);
    PrepareBackupTarget(config_.data_dir, target, backup_hook_.load(std::memory_order_acquire));
    EnsureBackupDirectory(target / "tables");
    const auto hook = [&](BackupTestHook::Point point, std::string_view table = {}, std::uint64_t revision = 0) {
        if (auto* current = backup_hook_.load(std::memory_order_acquire)) current->Run(point, table, revision);
        CheckCancelled(options.cancelled);
    };
    hook(BackupTestHook::Point::kAfterTargetGuard);
    Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_TARGET_GUARD_ONCE");
    const auto source_manifest = ReadDataDirManifest(config_.data_dir);
    if (!source_manifest) throw std::runtime_error("backup source catalog manifest disappeared");
    // The writer lock and backup guard exclude every other backup of this
    // catalog, so staging left by earlier ones is removed before this one stages.
    RemoveOrphanedBackupStaging(config_.data_dir, source_manifest->data_dir_id);
    const auto stage_alias = config_.data_dir / kBackupStagingName;
    // Staging roots intentionally allow a configured directory alias.
    const auto stage_parent = std::filesystem::weakly_canonical(stage_alias);
    EnsureDirectoryPathExists(stage_parent, false);
    const auto stage_path = stage_parent / (StoreIdHex(source_manifest->data_dir_id) + "." + StoreIdHex(NewStoreId()));
    hook(BackupTestHook::Point::kBeforeStagingCreate, stage_path.filename().string());
    if (!CreateOwnerOnlyDirectory(stage_path))
        throw std::runtime_error("backup staging directory already exists");
    // Arm cleanup only after this invocation successfully creates the directory.
    StagingCleanup staging{stage_path};
    SyncDirectoryPath(stage_parent);
    Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_STAGING_CREATE_ONCE");
    WriteBackupStagingOwner(staging.path, source_manifest->data_dir_id);
    Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_STAGING_OWNER_ONCE");
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
        EnsureDirectoryPathExists((staging.path / relative).parent_path(), false);
        std::filesystem::create_hard_link(source, staging.path / relative);
        files.push_back({relative, required, observed});
    };
    {
        auto metadata = CancellableLock(backup_metadata_gate_, options.cancelled);
        migration_health_->Check();
        if (ReadMigrationJournal(config_.data_dir)) throw MigrationRecoveryRequiredError("backup requires completed migrations");
        std::vector<std::shared_ptr<Table>> tables;
        std::string fenced;
        {
            std::shared_lock lock(tables_mutex_);
            for (const auto& [_, table] : tables_) tables.push_back(table);
            if (!fenced_.empty()) fenced = fenced_.begin()->first;
        }
        // A fenced table is out of service, not absent: the copy would lack it.
        if (!fenced.empty()) ThrowIfFenced(fenced);
        for (const auto& table : tables) {
            CheckCancelled(options.cancelled);
            hook(BackupTestHook::Point::kBeforeTablePin, table->name());
            std::optional<Table::BackupPin> pin;
            try { pin.emplace(table->PinForBackup(options.cancelled)); }
            catch (const TableNotFoundError&) { ThrowIfFenced(table->name()); continue; }
            auto& store = pin->store();
            hook(BackupTestHook::Point::kBeforeMaintenanceWait, table->name());
            auto maintenance = CancellableLock(store.backup_maintenance_mutex_, options.cancelled);
            const auto require_healthy = [&] {
                store.ThrowIfDurabilityPoisoned();
                if (store.coordinate_index_->failed())
                    throw std::runtime_error("backup refuses a failed coordinate index");
                if (store.snapshot_generation_mapping_->IsFailed())
                    throw std::runtime_error("backup refuses a failed snapshot generation");
            };
            require_healthy();
            const auto cut = store.version_clock_.load(std::memory_order_seq_cst) - 1U;
            hook(BackupTestHook::Point::kAfterCut, table->name(), cut);
            while (!store.write_producers_->Completed(store.version_clock_, cut)) {
                hook(BackupTestHook::Point::kWaitingForCompletion, table->name(), cut);
                // Waiting work stays on this side: mutations only publish and
                // clear their own cache-line bound, without locks or wakeups.
                std::this_thread::yield();
            }
            Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_CUT_ONCE");
            require_healthy();
            // Snapshot resident large names first: an eviction may move a
            // batched-only chunk to disk and retire its registry entry. Its
            // name remains in this union even if no directory existed yet.
            std::set<LargeChunkCoord> names;
            {
                std::lock_guard lock(store.large_chunks_mutex_);
                for (const auto& [lc, _] : store.large_chunks_) names.insert(lc);
            }
            for (const auto& large : std::filesystem::directory_iterator(store.data_dir_)) {
                const auto name = large.path().filename().string();
                if (!name.starts_with("L_")) continue;
                LargeChunkCoord lc;
                if (!ParseLargeChunkName(name, store.geometry_.dims(), &lc))
                    throw std::runtime_error("backup large chunk name is not canonical");
                if (!std::filesystem::is_directory(large.symlink_status()))
                    throw std::runtime_error("unsafe backup chunk directory: " + large.path().string());
                if (large.path() != LargeChunkDirectory(store.data_dir_, lc, store.geometry_.dims()))
                    throw std::runtime_error("backup large chunk path is not canonical");
                names.insert(lc);
            }
            const auto table_root = std::filesystem::path("tables") / table->name();
            const auto inventory_path = store.data_dir_ / kChunkIndexFileName;
            const auto inventory_bytes = std::filesystem::file_size(inventory_path);
            link(inventory_path, table_root / kChunkIndexFileName, inventory_bytes, inventory_bytes);
            SharedFileInput inventory(staging.path / table_root / kChunkIndexFileName);
            if (!inventory) throw std::runtime_error("failed to read pinned coordinate inventory");
            const auto index_header = ReadIndexInventory(inventory, inventory_bytes, store.geometry_, store.store_id_,
                [&](const IndexInventoryEntry& entry) {
                    const auto relative = LargeChunkDirectory({}, entry.coord, store.geometry_.dims()) / kChunkIndexFileName;
                    const auto source = store.data_dir_ / relative;
                    link(source, table_root / relative, entry.committed_bytes, std::filesystem::file_size(source));
                });
            if (index_header.compaction_pending || index_header.retained_deletion_floor > cut)
                throw std::runtime_error("backup coordinate inventory is not admitted at its revision cut");
            for (const auto& lc : names) {
                CheckCancelled(options.cancelled);
                hook(BackupTestHook::Point::kBeforeLargeChunkPin, table->name(), cut);
                std::unique_lock registry_lock(store.large_chunks_mutex_);
                const auto registered = store.large_chunks_.find(lc);
                std::shared_ptr<ChunkStore::LargeChunk> large;
                std::unique_lock<std::mutex> large_lock;
                if (registered != store.large_chunks_.end()) {
                    large = registered->second;
                    large_lock = std::unique_lock(large->mutex);
                    if (large->retired) throw std::logic_error("retired backup chunk container is registered");
                    registry_lock.unlock();
                }
                // With no resident container, keep registry admission closed
                // only while listing/linking this cold directory. Do not
                // populate the registry or eviction ring for cold files.
                std::set<ChunkCoord> coords;
                const auto directory = LargeChunkDirectory(store.data_dir_, lc, store.geometry_.dims());
                if (std::filesystem::exists(directory)) {
                    if (!std::filesystem::is_directory(std::filesystem::symlink_status(directory)))
                        throw std::runtime_error("unsafe backup chunk directory: " + directory.string());
                    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
                        if (entry.path().extension() != ".chk" && entry.path().extension() != ".wal") continue;
                        if (!std::filesystem::is_regular_file(entry.symlink_status()))
                            throw std::runtime_error("unsafe backup chunk file: " + entry.path().string());
                        ChunkCoord coord;
                        if (!ParseChunkFilePath(entry.path().lexically_relative(store.data_dir_), store.geometry_, &coord))
                            throw std::runtime_error("backup chunk path is not canonical");
                        coords.insert(coord);
                    }
                }
                if (large) for (const auto& [coord, _] : large->chunks) coords.insert(coord);
                for (const auto& coord : coords) {
                    CheckCancelled(options.cancelled);
                    std::unique_lock<RegularChunkMutex> chunk_lock;
                    if (large) {
                        const auto resident = large->chunks.find(coord);
                        if (resident != large->chunks.end()) {
                            chunk_lock = std::unique_lock(resident->second->mutex);
                            if (resident->second->wal_repair_failed) throw std::runtime_error("backup refuses an unrepaired WAL");
                            store.FlushWalBatch(coord, resident->second, false);
                        }
                    }
                    // Cold chunks remain cold; WALs, including a crash-shaped
                    // tail, are validated only during the private copy phase.
                    for (const auto& path : {ChunkDataPath(store.data_dir_, store.geometry_, coord),
                                            ChunkWalPath(store.data_dir_, store.geometry_, coord)}) {
                        if (!std::filesystem::exists(path)) continue;
                        const auto bytes = std::filesystem::file_size(path);
                        link(path, table_root / std::filesystem::relative(path, store.data_dir_), bytes, bytes);
                    }
                }
            }
            hook(BackupTestHook::Point::kAfterFlush, table->name(), cut);
            Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_FLUSH_ONCE");
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
        migration_health_->Check();
        save("chunkdb.manifest", LoadFile(DataDirManifestPath(config_.data_dir)));
        (void)ReadMigrationRecords(config_.data_dir);
        if (std::filesystem::exists(config_.data_dir / kMigrationsFileName))
            save(std::string(kMigrationsFileName), LoadFile(config_.data_dir / kMigrationsFileName));
        if (const auto users = ReadUsersFile(config_.data_dir)) save(kUsersFileName, EncodeUsers(*users));
    }
    hook(BackupTestHook::Point::kBeforeCopy);
    Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_BEFORE_COPY_ONCE");
    std::map<std::string, std::unique_ptr<RetainedBackupIndex>> retained_indexes;
    for (auto& file : files) {
        CheckCancelled(options.cancelled);
        if (file.relative.extension() == ".wal") {
            const auto table_name = std::next(file.relative.begin())->string();
            const auto table_cut = std::find_if(record.tables.begin(), record.tables.end(), [&](const auto& cut) { return cut.name == table_name; });
            const auto table_dir = staging.path / "tables" / table_name;
            const auto manifest = *ReadStoreManifest(table_dir);
            const Geometry geometry(manifest.geometry, manifest.schema, *manifest.chunk_schema);
            ChunkCoord coord;
            if (!ParseChunkFilePath(file.relative.lexically_relative(std::filesystem::path("tables") / table_name), geometry, &coord))
                throw std::logic_error("invalid staged WAL name");
            auto payload = std::vector<std::uint8_t>(geometry.ChunkPayloadBytes(), 0U);
            auto presence = std::vector<std::uint8_t>(ChunkPresenceBitmapBytes(geometry), 0U);
            ChunkVars vars;
            std::shared_ptr<const ChunkColumnState> chunk_columns;
            std::uint64_t base = 0, schema = 0, time = 0;
            const auto image_path = ChunkDataPath(table_dir, geometry, coord);
            if (std::filesystem::exists(image_path)) {
                auto image = ParseChunkImage(LoadChunkImageFile(image_path, geometry, coord,
                    manifest.store_id, manifest.features), geometry, coord, manifest.store_id, manifest.features);
                base = image.revision; schema = image.schema_version; time = image.commit_time_ms;
                if (base > table_cut->revision) throw std::runtime_error("backup image exceeds its revision cut");
                payload = std::move(image.payload); presence = std::move(image.presence_bitmap); vars = std::move(image.vars);
                chunk_columns = std::move(image.chunk_columns);
            }
            auto& retained = retained_indexes[table_name];
            if (!retained) retained = std::make_unique<RetainedBackupIndex>(table_dir, manifest);
            if (const auto boundary = retained->index.EmptyBoundary(coord))
                (void)ApplyRetainedEmptyBase(geometry, *boundary, base, time, schema,
                    payload, presence, vars, chunk_columns);
            SharedFileInput input(staging.path / file.relative);
            if (!input) throw std::runtime_error("failed to read pinned backup WAL");
            std::vector<WalFrameBoundary> boundaries;
            const auto replay = ReplayWalStream(input, file.observed, geometry, coord, manifest.store_id, manifest.features,
                base, schema, &payload, &presence, &vars, &boundaries, &chunk_columns,
                [&] { hook(BackupTestHook::Point::kBeforeWalReplayRead, table_name, table_cut->revision); }, time);
            if (!replay.torn_creation && (!replay.replayable ||
                (replay.tail_truncated_or_corrupt && !replay.stopped_at_crash_tail) || !replay.vars_problem.empty()))
                throw std::runtime_error("backup WAL cannot be replayed: " + replay.stop_reason + " " + replay.vars_problem);
            // A cold WAL remains a whole crash-consistent file, including
            // a recoverable tail. Only accepted changes after S require a
            // cropped prefix; restore performs normal crash recovery.
            const bool after_cut = std::any_of(boundaries.begin(), boundaries.end(),
                [&](const auto& boundary) { return boundary.revision > table_cut->revision; });
            if (after_cut) {
                file.required = 0;
                for (const auto& boundary : boundaries) if (boundary.revision <= table_cut->revision) file.required = boundary.end;
                if (file.required == 0U) continue;
            }
        }
        if (file.required > file.observed) throw std::logic_error("backup prefix exceeds its captured bound");
        record.files.push_back(CopyBackupFile(staging.path / file.relative, target, file.relative, file.required, options.cancelled, backup_hook_.load(std::memory_order_acquire)));
    }
    hook(BackupTestHook::Point::kAfterCopy);
    Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_COPY_ONCE");
    RemoveBackupStaging(staging.path);
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
