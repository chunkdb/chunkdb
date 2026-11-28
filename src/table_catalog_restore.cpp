#include "chunkdb/table_catalog.hpp"

#include <array>
#include <cstdlib>
#include <sstream>
#include <utility>

#include "backup.hpp"
#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/logging.hpp"
#include "durability_io.hpp"
#include "feature_flags.hpp"
#include "feed_slot_records.hpp"
#include "file_permissions.hpp"
#include "statement_control.hpp"
#include "store_manifest.hpp"
#include "user_registry.hpp"
#include "verify.hpp"

namespace chunkdb {
namespace {

constexpr const char* kFenceRestore = "a failed restore could not remove it";

// Runs `fn` when the scope ends, unless dismissed. `fn` must not throw.
template <typename Fn>
class ScopeExit {
  public:
    explicit ScopeExit(Fn fn) : fn_(std::move(fn)) {}
    ScopeExit(const ScopeExit&) = delete;
    ScopeExit& operator=(const ScopeExit&) = delete;
    ~ScopeExit() {
        if (active_) fn_();
    }
    void Dismiss() noexcept { active_ = false; }

  private:
    Fn fn_;
    bool active_ = true;
};

void CrashAtFailpoint(const char* key) {
    if (ConsumeFailpointEnv(key)) ExitAbruptly(86);
}

std::string OperationName(const std::string& table) {
    return table + "." + StoreIdHex(NewStoreId()).substr(0, 16);
}

// Logs the start, each tenth of the bytes and the end of a table copy.
class RestoreProgressLog {
  public:
    explicit RestoreProgressLog(const TableRestoreRequest& request) : request_(request) {}
    void operator()(std::uint64_t files, std::uint64_t bytes, std::uint64_t total_files, std::uint64_t total_bytes) {
        if (request_.progress) request_.progress(files, bytes, total_files, total_bytes);
        const std::uint64_t tenth = bytes / std::max<std::uint64_t>(1U, total_bytes / 10U);
        if (files != total_files && tenth <= logged_tenth_) return;
        logged_tenth_ = tenth;
        LogMessage(LogLevel::kInfo, LogComponent::kStore, "table restore copied",
            {{"table", request_.name}, {"files", std::to_string(files) + "/" + std::to_string(total_files)},
             {"bytes", std::to_string(bytes) + "/" + std::to_string(total_bytes)}});
    }

  private:
    const TableRestoreRequest& request_;
    std::uint64_t logged_tenth_ = 0U;
};

}  // namespace

TableRestoreResult TableCatalog::RestoreTable(const TableRestoreRequest& request) {
    RequireWritable("RESTORE TABLE");
    RequireNewTableName(request.name);
    const auto require_name = [&] {
        migration_health_->Check();
        ThrowIfFenced(request.name);
        if (Find(request.name) != nullptr) throw TableExistsError("table '" + request.name + "' already exists");
    };
    // Build the table under a private name, as Create does: a crash leaves
    // the staging directory, which the next writer start removes. While the
    // restore runs, RECOVER TABLE of the name leaves it alone.
    const auto operation = OperationName(request.name);
    {
        std::lock_guard operations(operations_mutex_);
        require_name();
        restore_staging_.insert(operation);
    }
    // Runs last, after the operations lock taken below is released.
    ScopeExit registration([&] {
        std::lock_guard operations(operations_mutex_);
        restore_staging_.erase(operation);
    });
    LogMessage(LogLevel::kInfo, LogComponent::kStore, "table restore started",
        {{"table", request.name}, {"backup", request.backup.string()}, {"backed_up_table", request.table}});

    EnsureDirectoryPathExists(StagingDir(), /*durable_sync=*/true);
    const auto staging = StagingDir() / operation;
    if (!CreateOwnerOnlyDirectory(staging)) throw std::runtime_error("table staging directory already exists");
    const auto target = TablesDir() / request.name;
    TableRestoreResult result;
    result.name = request.name;
    StoreManifest manifest;
    // The table must have the table-owned features the directory gives its tables.
    const auto require_features = [&] {
        const auto directory_manifest = ReadDataDirManifest(config_.data_dir);
        if (!directory_manifest) throw std::runtime_error("data directory manifest disappeared");
        const auto served = TableScopedFeatures(directory_manifest->features);
        if (TableScopedFeatures(manifest.features).incompat != served.incompat)
            throw std::invalid_argument("the backed-up table has features " + DescribeFeatures(TableScopedFeatures(manifest.features)) +
                " but this data directory gives its tables " + DescribeFeatures(served));
    };
    std::shared_lock<BackupMaintenanceGate> metadata;
    std::unique_lock operations(operations_mutex_, std::defer_lock);
    TableOptions options;
    FenceNode undo_fence;
    ServedNode entry;
    try {
        RestoreProgressLog progress(request);
        const auto copy = CopyBackupTable(request.backup, request.table, staging, request.cancelled, std::ref(progress));
        CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_RESTORE_TABLE_AFTER_COPY_ONCE");
        const auto copied = ReadStoreManifest(staging);
        if (!copied) throw std::runtime_error("restored table manifest is missing");
        manifest = *copied;
        if ((manifest.features.incompat & kFeatureTableLineage) == 0U)
            throw FeatureAdmissionError("feature_disabled", "table_lineage",
                "restore requires table_lineage on the backed-up table; enable it on the source and create another backup");
        require_features();
        options = DecodeTableOptions(manifest.options);
        RequireValidTableOptions(options);
        RequireRecoverableOptions(request.name, manifest);
        result.parent_epoch = copy.cut.epoch;
        result.fork_revision = copy.cut.revision;
        result.files = copy.files.size();
        result.bytes = copy.bytes;
        result.epoch = RebindRestoredTable(staging, copy.cut, copy.files, request.cancelled);
        manifest.store_id = result.epoch;
        manifest.lineage = TableLineage{result.parent_epoch, result.fork_revision};
        manifest.follower.reset();
        std::ostringstream findings;
        if (VerifyTableDirectory(staging, findings).errors != 0U)
            throw std::runtime_error("restored table failed verification: " + findings.str());
        SyncBackupTree(staging, request.cancelled);
        if (request.cancelled.stop_requested()) throw std::runtime_error("backup cancelled");
        CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_RESTORE_TABLE_BEFORE_PUBLISH_ONCE");
        // ENABLE FEATURE cannot change the directory's features from this
        // check until the table serves.
        if (!feature_metadata_gate_.lock_shared(request.cancelled)) throw std::runtime_error("backup cancelled");
        metadata = std::shared_lock(feature_metadata_gate_, std::adopt_lock);
        require_features();
        operations.lock();
        require_name();
        RequireUnusedEpoch(result.epoch);
        undo_fence = PrepareFence(nullptr, request.name, kFenceRestore, result.epoch);
        entry = PrepareServed(request.name);
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_RESTORE_TABLE_PUBLISH_FAIL_ONCE"))
            throw std::runtime_error("injected failure before publishing a restored table");
        // A stop accepted until here leaves nothing restored; after it the
        // statement reports its real outcome.
        EnterStatementDecision();
        const auto moved = MoveDirectoryNoReplace(staging, target);
        if (moved == std::errc::file_exists)
            throw TableExistsError("cannot restore table '" + request.name + "': " + target.string() +
                " exists but is not a table this server opened");
        if (moved) throw std::runtime_error("cannot publish table " + target.string() + ": " + moved.message());
    } catch (const std::exception& error) {
        std::error_code cleanup_ec;
        std::filesystem::remove_all(staging, cleanup_ec);
        LogMessage(LogLevel::kWarn, LogComponent::kStore, "table restore failed; nothing was published",
            {{"table", request.name}, {"error", error.what()}});
        if (request.cancelled.stop_requested()) throw std::runtime_error("RESTORE TABLE cancelled; nothing was restored");
        throw;
    }
    CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_RESTORE_TABLE_AFTER_PUBLISH_ONCE");

    // From here the table is in tables/. If it cannot be made durable or
    // opened, take it back out, as Create does, so the name is not left with
    // a table this server does not serve.
    std::shared_ptr<ChunkStore> store;
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_RESTORE_TABLE_OPEN_FAIL_ONCE"))
            throw std::runtime_error("injected failure opening a restored table");
        SyncDirectoryPath(TablesDir());
        SyncDirectoryPath(StagingDir());
        store = OpenStore(request.name, target, manifest.geometry, 0U, options);
        Geometry geometry = store->geometry();
        entry.mapped() = std::shared_ptr<Table>(new Table(request.name, target, result.epoch, std::move(geometry), options,
            std::move(store), config_.feed_buffer_bytes, migration_health_, config_.feed_linger));
    } catch (const std::exception& e) {
        store.reset();
        try {
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_RESTORE_TABLE_UNDO_FAIL_ONCE"))
                throw std::runtime_error("injected failure removing a restored table");
            MoveToDropped(target, request.name);
        } catch (const std::exception& undo_error) {
            {
                std::unique_lock lock(tables_mutex_);
                fenced_.insert(std::move(undo_fence));
            }
            LogMessage(LogLevel::kError, LogComponent::kStore,
                "restored table could not be removed again; it is fenced until RECOVER TABLE or a restart opens it",
                {{"table", request.name}, {"error", undo_error.what()}});
        }
        LogMessage(LogLevel::kError, LogComponent::kStore, "restored table could not be made durable or opened",
            {{"table", request.name}, {"error", e.what()}});
        throw;
    }
    {
        std::unique_lock lock(tables_mutex_);
        tables_.insert(std::move(entry));
    }
    LogMessage(LogLevel::kInfo, LogComponent::kStore, "table restored",
        {{"table", request.name}, {"store_id", StoreIdHex(result.epoch)}, {"parent_epoch", StoreIdHex(result.parent_epoch)},
         {"fork_revision", std::to_string(result.fork_revision)}, {"files", std::to_string(result.files)},
         {"bytes", std::to_string(result.bytes)}});
    return result;
}

}  // namespace chunkdb

namespace chunkdb {
namespace {

constexpr const char* kFenceSwapUnknown = "a failed swap could not make its exchange durable";
constexpr const char* kFenceSwapReopen = "a swap could not open it under its new name";
constexpr const char* kFenceSwapRefusedReopen = "a refused swap could not reopen it";
constexpr const char* kWatchSwapped = "watched table was swapped; watch it again";

// `path` below `from`, moved below `to`; any other path unchanged.
std::string MovedPath(const std::string& path, const std::string& from, const std::string& to) {
    if (path.rfind(from, 0U) != 0U) return path;
    if (path.size() != from.size() && path[from.size()] != '/' &&
        path[from.size()] != static_cast<char>(std::filesystem::path::preferred_separator))
        return path;
    return to + path.substr(from.size());
}

// Marks every slot recorded in `dir` so that its next watch starts with
// resync, durably. Returns the records it replaced, if it replaced any.
std::optional<FeedSlotRecords> RequireSlotResync(const std::filesystem::path& dir, const StoreId& epoch, std::uint8_t dims) {
    auto records = ReadFeedSlotRecords(dir, epoch, dims);
    if (!records) return std::nullopt;
    auto marked = *records;
    bool changed = false;
    for (auto& slot : marked.slots) {
        if (slot.lost || slot.resync) continue;
        slot.resync = true;
        changed = true;
    }
    if (!changed) return std::nullopt;
    WriteFeedSlotRecords(dir, marked);
    return records;
}

}  // namespace

void TableCatalog::SwapTables(std::string_view first, std::string_view second, std::stop_token cancelled) {
    RequireWritable("SWAP TABLES");
    RequireValidTableName(first);
    RequireValidTableName(second);
    if (first == second) throw std::invalid_argument("SWAP TABLES needs two different tables");
#ifdef _WIN32
    // Windows has no atomic directory exchange: refuse before closing anything.
    throw std::runtime_error("SWAP TABLES needs an atomic directory exchange, which Windows does not have");
#endif
    // As RecoverTable: feature activation and backup metadata capture and
    // pinning exclude a swap, so a backup copies both names before or after.
    if (!feature_metadata_gate_.lock_shared(cancelled)) throw std::runtime_error("SWAP TABLES cancelled");
    std::shared_lock feature_metadata(feature_metadata_gate_, std::adopt_lock);
    if (!backup_metadata_gate_.lock_shared(cancelled)) throw std::runtime_error("SWAP TABLES cancelled");
    std::shared_lock backup_metadata(backup_metadata_gate_, std::adopt_lock);
    // Name order, so two swaps of overlapping tables admit in the same order.
    const std::array<std::string, 2U> names{std::string(std::min(first, second)), std::string(std::max(first, second))};
    const std::array<std::shared_ptr<Table>, 2U> tables{Require(names[0]), Require(names[1])};
    std::unique_lock ddl_first(tables[0]->ddl_mutex_);
    std::unique_lock ddl_second(tables[1]->ddl_mutex_);
    const std::array<TableOptions, 2U> options{tables[0]->Info().options, tables[1]->Info().options};
    std::array<std::shared_ptr<ChunkStore>, 2U> stores;
    ScopeExit serving([&] {
        for (std::size_t i = 0; i < stores.size(); ++i)
            if (stores[i]) tables[i]->EndExclusive(std::move(stores[i]), options[i]);
    });
    for (std::size_t i = 0; i < stores.size(); ++i) {
        stores[i] = tables[i]->BeginExclusive(/*closing=*/false, cancelled);
        if (stores[i]) continue;
        if (cancelled.stop_requested()) throw std::runtime_error("SWAP TABLES cancelled");
        throw TableNotFoundError("table '" + names[i] + "' does not exist");
    }
    std::lock_guard operations(operations_mutex_);
    migration_health_->Check();
    for (std::size_t i = 0; i < stores.size(); ++i) {
        if (Find(names[i]) != tables[i]) throw TableNotFoundError("table '" + names[i] + "' does not exist");
        if (const char* reason = Table::StoreFenceReason(*stores[i]))
            throw TableFencedError("table '" + names[i] + "' is fenced: " + reason + "; run RECOVER TABLE " + names[i]);
    }
    // Everything acknowledged becomes durable under the old names first.
    for (auto& store : stores) store->WalBarrier();
    const std::array<std::filesystem::path, 2U> dirs{tables[0]->dir_, tables[1]->dir_};
    // Made now, so that fencing does not allocate.
    std::array<FenceNode, 2U> fences{PrepareFence(tables[0], names[0], kFenceSwapUnknown),
        PrepareFence(tables[1], names[1], kFenceSwapUnknown)};
    std::array<std::shared_ptr<ChunkStore::UnsyncedArtifacts>, 2U> unsynced{
        std::make_shared<ChunkStore::UnsyncedArtifacts>(), std::make_shared<ChunkStore::UnsyncedArtifacts>()};
    // Unsynced paths are spelled below each store's own directory.
    const std::array<std::string, 2U> store_dirs{stores[0]->data_dir().string(), stores[1]->data_dir().string()};
    // A stop accepted until here leaves both tables serving as they were;
    // after it the statement reports its real outcome.
    EnterStatementDecision();
    serving.Dismiss();
    for (std::size_t i = 0; i < stores.size(); ++i) {
        stores[i]->HandOverUnsyncedOnClose(unsynced[i]);
        stores[i].reset();
    }
    // Opens directory `dir` as table `name` with what store `from` still owed.
    const auto open = [&](std::size_t name, std::size_t from) {
        const auto manifest = ReadOpenableManifest(dirs[name]);
        const auto table_options = DecodeTableOptions(manifest.options);
        auto store = OpenStore(names[name], dirs[name], manifest.geometry, 0U, table_options);
        ChunkStore::UnsyncedArtifacts moved;
        moved.overflow = unsynced[from]->overflow;
        const auto& old_dir = store_dirs[from];
        const auto new_dir = store->data_dir().string();
        for (const auto& path : unsynced[from]->files) moved.files.insert(MovedPath(path, old_dir, new_dir));
        for (const auto& path : unsynced[from]->dirs) moved.dirs.insert(MovedPath(path, old_dir, new_dir));
        for (const auto& path : unsynced[from]->swept_dirs) moved.swept_dirs.insert(MovedPath(path, old_dir, new_dir));
        store->AdoptUnsynced(moved);
        const StoreId store_id = store->store_id();
        Geometry geometry = store->geometry();
        return std::shared_ptr<Table>(new Table(names[name], dirs[name], store_id, std::move(geometry), table_options,
            std::move(store), config_.feed_buffer_bytes, migration_health_, config_.feed_linger));
    };
    // After the exchange each slot is reached by another name, so its
    // consumer must resync. The marks are durable before the exchange, so no
    // crash loses them; a crash before it leaves only an extra resync.
    std::array<std::optional<FeedSlotRecords>, 2U> slot_records;
    std::error_code exchanged;
    std::exception_ptr failure;
    try {
        for (std::size_t i = 0; i < dirs.size(); ++i)
            slot_records[i] = RequireSlotResync(dirs[i], tables[i]->store_id(), tables[i]->geometry().dims());
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_SWAP_TABLES_SLOT_MARK_FAIL_ONCE"))
            throw std::runtime_error("injected failure marking swapped slots");
        CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_BEFORE_EXCHANGE_ONCE");
        exchanged = ConsumeFailpointEnv("CHUNKDB_FAILPOINT_SWAP_TABLES_EXCHANGE_UNSUPPORTED_ONCE")
            ? std::make_error_code(std::errc::function_not_supported) : ExchangeDirectories(dirs[0], dirs[1]);
    } catch (const std::exception&) {
        failure = std::current_exception();
    }
    if (exchanged || failure) {
        // Nothing moved: both tables serve again as they were.
        for (std::size_t i = 0; i < stores.size(); ++i) {
            if (slot_records[i]) {
                try {
                    WriteFeedSlotRecords(dirs[i], *slot_records[i]);
                } catch (const std::exception& error) {
                    LogMessage(LogLevel::kWarn, LogComponent::kStore,
                        "slots of a table whose swap failed could not be restored; their next watch resyncs",
                        {{"table", names[i]}, {"error", error.what()}});
                }
            }
            try {
                auto store = OpenStore(names[i], dirs[i], tables[i]->geometry().config(), 0U, options[i]);
                store->AdoptUnsynced(*unsynced[i]);
                tables[i]->EndExclusive(std::move(store), options[i]);
            } catch (const std::exception& error) {
                LogMessage(LogLevel::kError, LogComponent::kStore,
                    "table could not be reopened after a refused swap; it is fenced until RECOVER TABLE or a restart",
                    {{"table", names[i]}, {"error", error.what()}});
                fences[i].mapped().reason = kFenceSwapRefusedReopen;
                FenceTable(*tables[i], options[i], std::move(fences[i]));
            }
        }
        if (failure) std::rethrow_exception(failure);
        throw std::runtime_error("SWAP TABLES needs an atomic directory exchange, which was refused: " + exchanged.message());
    }
    // Each fence reserves the epoch now on disk under its name.
    std::swap(fences[0].mapped().epoch, fences[1].mapped().epoch);
    CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_EXCHANGE_ONCE");
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_SWAP_TABLES_SYNC_FAIL_ONCE"))
            throw std::runtime_error("injected directory sync failure after exchanging tables");
        SyncDirectoryPath(TablesDir());
    } catch (const std::exception& error) {
        // Exchanged now, but a crash could still undo it: neither name serves
        // until a recovery opens what its directory holds.
        LogMessage(LogLevel::kError, LogComponent::kStore,
            "swapped tables could not be made durable; both are fenced until RECOVER TABLE or a restart",
            {{"tables", names[0] + "," + names[1]}, {"error", error.what()}});
        for (std::size_t i = 0; i < stores.size(); ++i) {
            tables[i]->ended_.store(kWatchSwapped, std::memory_order_release);
            FenceTable(*tables[i], options[i], std::move(fences[i]));
        }
        throw;
    }
    CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_SYNC_ONCE");
    std::array<std::shared_ptr<Table>, 2U> swapped;
    for (std::size_t i = 0; i < swapped.size(); ++i) {
        try {
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_SWAP_TABLES_OPEN_FAIL_ONCE"))
                throw std::runtime_error("injected failure opening a swapped table");
            swapped[i] = open(i, 1U - i);
        } catch (const std::exception& error) {
            LogMessage(LogLevel::kError, LogComponent::kStore,
                "swapped table could not be opened under its new name; it is fenced until RECOVER TABLE or a restart",
                {{"table", names[i]}, {"error", error.what()}});
        }
    }
    // Connections holding the old tables find the swapped ones; their
    // watches end, and a watch by name resumes by lineage or resyncs.
    for (std::size_t i = 0; i < swapped.size(); ++i) {
        tables[i]->ended_.store(kWatchSwapped, std::memory_order_release);
        if (swapped[i]) {
            {
                std::unique_lock lock(tables_mutex_);
                tables_[names[i]] = swapped[i];
            }
            tables[i]->EndExclusive(nullptr, options[i]);
        } else {
            fences[i].mapped().reason = kFenceSwapReopen;
            FenceTable(*tables[i], options[i], std::move(fences[i]));
        }
    }
    LogMessage(LogLevel::kInfo, LogComponent::kStore, "tables swapped",
        {{"tables", names[0] + "," + names[1]}, {"store_ids", StoreIdHex(tables[1]->store_id()) + "," + StoreIdHex(tables[0]->store_id())}});
    if (!swapped[0] || !swapped[1]) throw std::runtime_error("swapped tables could not all be opened under their new names");
}

}  // namespace chunkdb

namespace chunkdb {
namespace {

constexpr const char* kFenceRenameUnknown = "a rename could not make it durable";
constexpr const char* kFenceRenameReopen = "a rename could not open it under its new name";
constexpr const char* kFenceRenameRefusedReopen = "a refused rename could not reopen it";
constexpr const char* kWatchRenamed = "watched table was renamed; watch it again";

}  // namespace

void TableCatalog::RenameTable(std::string_view name, std::string_view new_name, UserRegistry* users, std::stop_token cancelled) {
    RequireWritable("ALTER TABLE ... RENAME TO");
    RequireValidTableName(name);
    RequireNewTableName(new_name);
    const std::string from(name), to(new_name);
    if (from == to) throw std::invalid_argument("table '" + from + "' already has the name " + to);
    // As SwapTables: feature activation and backup metadata capture and
    // pinning exclude a rename, so a backup copies the table under one name.
    if (!feature_metadata_gate_.lock_shared(cancelled)) throw std::runtime_error("ALTER TABLE ... RENAME TO cancelled");
    std::shared_lock feature_metadata(feature_metadata_gate_, std::adopt_lock);
    if (!backup_metadata_gate_.lock_shared(cancelled)) throw std::runtime_error("ALTER TABLE ... RENAME TO cancelled");
    std::shared_lock backup_metadata(backup_metadata_gate_, std::adopt_lock);
    const auto table = Require(from);
    std::lock_guard ddl(table->ddl_mutex_);
    const TableOptions options = table->Info().options;
    auto store = table->BeginExclusive(/*closing=*/false, cancelled);
    if (!store) {
        if (cancelled.stop_requested()) throw std::runtime_error("ALTER TABLE ... RENAME TO cancelled");
        throw TableNotFoundError("table '" + from + "' does not exist");
    }
    ScopeExit serving([&] { table->EndExclusive(std::move(store), options); });
    std::lock_guard operations(operations_mutex_);
    migration_health_->Check();
    if (Find(from) != table) throw TableNotFoundError("table '" + from + "' does not exist");
    if (const char* reason = Table::StoreFenceReason(*store))
        throw TableFencedError("table '" + from + "' is fenced: " + reason + "; run RECOVER TABLE " + from);
    ThrowIfFenced(to);
    if (Find(to) != nullptr)
        throw TableExistsError("table '" + to + "' already exists; use another name, or DROP TABLE " + to + " first");
    // Everything acknowledged becomes durable under the old name first.
    store->WalBarrier();
    const std::filesystem::path old_dir = table->dir_;
    const std::filesystem::path new_dir = TablesDir() / to;
    // Made now, so that fencing and publishing do not allocate.
    auto fence = PrepareFence(table, to, kFenceRenameUnknown);
    auto old_fence = PrepareFence(table, from, kFenceRenameRefusedReopen);
    auto entry = PrepareServed(to);
    const auto unsynced = std::make_shared<ChunkStore::UnsyncedArtifacts>();
    const std::string store_dir = store->data_dir().string();
    // A stop accepted until here leaves the table serving under its name;
    // after it the statement reports its real outcome.
    EnterStatementDecision();
    // The grants move first: a crash from here leaves them on both names,
    // never on neither. A failure here still serves the table unchanged.
    UserRegistry::TableGrants copied;
    // Takes back the grants given to the new name, which no table then has.
    const auto take_back_grants = [&] {
        try {
            users->RestoreTableGrants(to, copied);
        } catch (const std::exception& error) {
            LogMessage(LogLevel::kWarn, LogComponent::kStore,
                "grants given to the new name of a table whose rename failed could not be taken back; REVOKE them",
                {{"table", from}, {"name", to}, {"error", error.what()}});
        }
    };
    if (users) {
        try {
            users->CopyTableGrants(from, to, copied);
        } catch (const UsersOutcomeUnknownError&) {
            take_back_grants();
            throw;
        }
    }
    serving.Dismiss();
    store->HandOverUnsyncedOnClose(unsynced);
    store.reset();
    CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_RENAME_TABLE_BEFORE_RENAME_ONCE");
    const auto moved = ConsumeFailpointEnv("CHUNKDB_FAILPOINT_RENAME_TABLE_MOVE_FAIL_ONCE")
        ? std::make_error_code(std::errc::io_error) : MoveDirectoryNoReplace(old_dir, new_dir);
    if (moved) {
        // Nothing moved: the table serves again under its name.
        if (users) take_back_grants();
        try {
            auto reopened = OpenStore(from, old_dir, table->geometry().config(), 0U, options);
            reopened->AdoptUnsynced(*unsynced);
            table->EndExclusive(std::move(reopened), options);
        } catch (const std::exception& error) {
            LogMessage(LogLevel::kError, LogComponent::kStore,
                "table could not be reopened after a refused rename; it is fenced until RECOVER TABLE or a restart",
                {{"table", from}, {"error", error.what()}});
            FenceTable(*table, options, std::move(old_fence));
        }
        if (moved == std::errc::file_exists)
            throw TableExistsError("cannot rename table '" + from + "': " + new_dir.string() +
                " exists but is not a table this server opened");
        throw std::runtime_error("cannot rename table '" + from + "' to '" + to + "': " + moved.message());
    }
    CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_RENAME_TABLE_AFTER_RENAME_ONCE");
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_RENAME_TABLE_SYNC_FAIL_ONCE"))
            throw std::runtime_error("injected directory sync failure after renaming a table");
        SyncDirectoryPath(TablesDir());
    } catch (const std::exception& error) {
        // Renamed now, but a crash could still undo it: as after a failed
        // swap, neither name serves until a recovery opens what its
        // directory holds, and the grants stay on both names.
        LogMessage(LogLevel::kError, LogComponent::kStore,
            "renamed table could not be made durable; both names are fenced until RECOVER TABLE or a restart",
            {{"table", from}, {"name", to}, {"error", error.what()}});
        table->ended_.store(kWatchRenamed, std::memory_order_release);
        old_fence.mapped().reason = kFenceRenameUnknown;
        old_fence.mapped().epoch.reset();
        {
            std::unique_lock lock(tables_mutex_);
            // A node insert does not allocate.
            fenced_.insert(std::move(old_fence));
        }
        FenceTable(*table, options, std::move(fence));
        throw;
    }
    CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_RENAME_TABLE_AFTER_SYNC_ONCE");
    std::shared_ptr<Table> renamed;
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_RENAME_TABLE_OPEN_FAIL_ONCE"))
            throw std::runtime_error("injected failure opening a renamed table");
        auto opened = OpenStore(to, new_dir, table->geometry().config(), 0U, options);
        // What the closed store still owed, spelled below the new directory.
        ChunkStore::UnsyncedArtifacts owed;
        owed.overflow = unsynced->overflow;
        const auto opened_dir = opened->data_dir().string();
        for (const auto& path : unsynced->files) owed.files.insert(MovedPath(path, store_dir, opened_dir));
        for (const auto& path : unsynced->dirs) owed.dirs.insert(MovedPath(path, store_dir, opened_dir));
        for (const auto& path : unsynced->swept_dirs) owed.swept_dirs.insert(MovedPath(path, store_dir, opened_dir));
        opened->AdoptUnsynced(owed);
        const StoreId store_id = opened->store_id();
        Geometry geometry = opened->geometry();
        renamed = std::shared_ptr<Table>(new Table(to, new_dir, store_id, std::move(geometry), options, std::move(opened),
            config_.feed_buffer_bytes, migration_health_, config_.feed_linger));
    } catch (const std::exception& error) {
        LogMessage(LogLevel::kError, LogComponent::kStore,
            "renamed table could not be opened under its new name; it is fenced until RECOVER TABLE or a restart",
            {{"table", from}, {"name", to}, {"error", error.what()}});
        fence.mapped().reason = kFenceRenameReopen;
        table->ended_.store(kWatchRenamed, std::memory_order_release);
        FenceTable(*table, options, std::move(fence));
        // The rename is durable, so the old name is free and keeps no grants.
        if (users) {
            try {
                users->ForgetTable(from);
            } catch (const std::exception& forget_error) {
                LogMessage(LogLevel::kWarn, LogComponent::kStore,
                    "grants on the old name of a renamed table could not be removed; REVOKE them",
                    {{"table", from}, {"name", to}, {"error", forget_error.what()}});
            }
        }
        throw;
    }
    // Connections holding the old table find no table of its name; its
    // watches end, and a watch of the new name resumes in the same epoch.
    {
        std::unique_lock lock(tables_mutex_);
        if (const auto it = tables_.find(from); it != tables_.end() && it->second == table) tables_.erase(it);
        entry.mapped() = std::move(renamed);
        tables_.insert(std::move(entry));
    }
    table->ended_.store(kWatchRenamed, std::memory_order_release);
    table->EndExclusive(nullptr, options);
    LogMessage(LogLevel::kInfo, LogComponent::kStore, "table renamed",
        {{"table", from}, {"name", to}, {"store_id", StoreIdHex(table->store_id())}});
    // Inside catalog admission, as DROP TABLE forgets grants: a statement
    // creating or granting the old name follows the whole rename.
    if (users) {
        try {
            users->ForgetTable(from);
        } catch (const std::exception& error) {
            throw std::runtime_error("table '" + from + "' was renamed to '" + to +
                "', but its grants on the old name could not be removed: " + error.what());
        }
    }
}

}  // namespace chunkdb
