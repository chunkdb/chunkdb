#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/change_feed.hpp"
#include "chunkdb/feed_slots.hpp"
#include "chunkdb/migration_health.hpp"
#include "chunkdb/geometry.hpp"

namespace chunkdb {

struct StoreManifest;
struct BackupOptions;
struct BackupResult;
struct BackupTestHook;
struct MigrationRequest;
struct MigrationRecord;
class UserRegistry;

class ProcessLock;
class SlotWatch;
struct FeedSlotClaim;
struct FeedSlotAckState;

// Conventional table name for callers that explicitly create it.
inline constexpr std::string_view kDefaultTableName = "default";
inline constexpr std::size_t kMaxTableNameLength = 64;

// Table names become directory names: `[a-z0-9][a-z0-9_-]{0,63}`, and not a
// Windows device name (con, prn, aux, nul, com0-com9, lpt0-lpt9).
[[nodiscard]] bool IsValidTableName(std::string_view name) noexcept;
// Throws std::invalid_argument stating the rule when `name` breaks it.
void RequireValidTableName(std::string_view name);
// Throws std::invalid_argument for an option a table cannot use.
void RequireValidTableOptions(const TableOptions& options);

// The named table does not exist (or was dropped).
class TableNotFoundError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// A table of that name already exists.
class TableExistsError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// The named table is fenced (TableCatalog::Fences) and out of service until
// TableCatalog::RecoverTable or a restart.
class TableFencedError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Bits naming TableOptions fields, for CatalogConfig::default_option_fields.
enum TableOptionField : std::uint32_t {
    kOptionFieldDurabilityMode = 1U << 0U,
    kOptionFieldCheckpointUpdates = 1U << 1U,
    kOptionFieldCheckpointWalBytes = 1U << 2U,
    kOptionFieldWalGroupCommitUpdates = 1U << 3U,
    kOptionFieldCheckpointCompression = 1U << 4U,
};

struct CatalogConfig {
    std::filesystem::path data_dir;
    AccessMode access_mode = AccessMode::kReadWrite;
    bool allow_multiple_processes = false;

    // Defaults the server offers for new tables. Existing tables keep their stored
    // options; a field named in default_option_fields must equal every
    // existing table's stored value, or the catalog refuses to open.
    TableOptions default_options;
    std::uint32_t default_option_fields = 0;

    // Shared by all tables (StoreResources).
    std::size_t max_loaded_chunks = kDefaultMaxLoadedChunks;
    std::size_t max_open_wal_streams = 1024;
    bool background_maintenance = false;
    std::size_t background_checkpoint_queue_limit = 4096;
    // Per table: the chunk states kept for open transactions.
    std::size_t txn_history_bytes = kDefaultTxnHistoryBytes;
    std::size_t slot_max_bytes = kDefaultSlotMaxBytes;
    std::chrono::milliseconds slot_sync_interval = kDefaultSlotSyncInterval;
    std::size_t feed_buffer_bytes = kDefaultFeedBufferBytes;
    std::chrono::milliseconds feed_linger{30000};
};

// A catalog configuration whose new-table options and shared budgets come
// from a store configuration. Geometry belongs to each table. `option_fields`
// names the options given explicitly (TableOptionField bits).
[[nodiscard]] CatalogConfig CatalogConfigFromStoreConfig(
    const StoreConfig& config,
    std::uint32_t option_fields = 0);

// A change to some options of a table; unset fields keep their value.
struct TableOptionsUpdate {
    std::optional<DurabilityMode> durability_mode;
    std::optional<std::size_t> checkpoint_update_interval;
    std::optional<std::size_t> checkpoint_wal_bytes;
    std::optional<std::size_t> wal_group_commit_updates;
    std::optional<CheckpointCompression> checkpoint_compression;
    std::optional<std::size_t> var_max_chunk_bytes;
    std::optional<std::size_t> feed_buffer_bytes;
    std::optional<std::size_t> slot_max_bytes;

    // Every field set from `options`.
    [[nodiscard]] static TableOptionsUpdate From(const TableOptions& options);
    [[nodiscard]] TableOptions ApplyTo(TableOptions options) const;
    [[nodiscard]] bool empty() const noexcept;
};

struct TableInfo {
    std::string name;
    StoreId store_id{};
    GeometryConfig geometry;
    TableSchema schema;
    TableOptions options;
    // Effective limits; options preserves whether each override was set.
    std::size_t feed_buffer_bytes = kDefaultFeedBufferBytes;
    std::size_t slot_max_bytes = kDefaultSlotMaxBytes;
    TableSchema chunk_schema{};
};

struct CatalogFeatureStatus {
    FeatureFlags directory;
    std::vector<std::pair<std::string, FeatureFlags>> tables;
    bool activation_pending = false;
};

struct TableDefinition {
    GeometryConfig geometry;
    TableOptions options;
    std::optional<TableSchema> schema;
    std::optional<TableSchema> chunk_schema{};
};

// A table that refuses its commands until TableCatalog::RecoverTable or a
// restart recovers it: after a failure that left it out of service, or while
// its store is fail-closed after a durability failure or its feed cleanup
// failed.
struct TableFence {
    std::string table;
    std::string reason;
    // What it served last; std::nullopt when it never served.
    std::optional<TableInfo> info;
};

// One table of a catalog. A connection keeps a shared_ptr<Table> for the
// table it selected; every command on it runs under a Lease.
class Table : public std::enable_shared_from_this<Table> {
  public:
    // Keeps the table's store open (not dropped, not being reopened) while a
    // command runs on it. Holds plain pointers: the table cannot leave its
    // catalog, and its store cannot close, while any lease is active.
    class Lease {
      public:
        Lease(Lease&& other) noexcept;
        Lease& operator=(Lease&&) = delete;
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        ~Lease();

        [[nodiscard]] ChunkStore& store() const noexcept { return *store_; }

      private:
        friend class Table;
        Lease(Table* table, ChunkStore* store) noexcept;

        Table* table_ = nullptr;
        ChunkStore* store_ = nullptr;
    };

    ~Table();
    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;

    [[nodiscard]] const std::string& name() const noexcept { return name_; }
    [[nodiscard]] const StoreId& store_id() const noexcept { return store_id_; }
    [[nodiscard]] const std::optional<TableLineage>& lineage() const noexcept { return lineage_; }
    // Changes when the table's columns change.
    [[nodiscard]] Geometry geometry() const;
    [[nodiscard]] TableInfo Info() const;
    // Waits while the table is being reopened. std::nullopt once it was
    // dropped: a table of the same name created later is another table.
    [[nodiscard]] std::optional<Lease> Acquire();

    [[nodiscard]] std::unique_ptr<FeedSubscription> SubscribeFeed(const FeedOptions& options = {});
    [[nodiscard]] std::size_t FeedBufferBytes() const noexcept { return effective_feed_buffer_bytes_.load(std::memory_order_acquire); }
    // Ends existing subscriptions; another subscription can start a fresh feed.
    void StopFeed();

    // Create, drop, advance and archive-reader creation require exclusive table
    // access; callers must not hold a Lease. Listing may hold a Lease.
    // Positions are persisted atomically and synced.
    [[nodiscard]] FeedSlot CreateFeedSlot(std::string_view name, bool if_not_exists = false, const FeedSlotFilters& filters = {});
    void DropFeedSlot(std::string_view name, bool if_exists = false);
    [[nodiscard]] std::vector<FeedSlot> ListFeedSlots(bool include_lost = false);
    [[nodiscard]] std::vector<std::uint32_t> ResolveFeedColumns(const std::vector<std::string>& names);
    // Monotonic, in this epoch, and no higher than the durable frontier. The
    // streaming layer must additionally check its last revision sent.
    void AdvanceFeedSlot(std::string_view name, FeedPosition position);
    [[nodiscard]] FeedArchiveReader ReadFeedArchive(FeedPosition after);

  private:
    friend class SlotWatch;
    friend class TableCatalog;
    friend struct FeedSlotTestAccess;
    friend class FeedSubscription;
    friend struct FeedTestAccess;
    friend struct BackupTestAccess;
    friend struct FeatureActivationTestAccess;
    friend struct CatalogTxnTestAccess;
    void ReleaseFeed(const std::shared_ptr<ChangeFeed>& feed);
    [[nodiscard]] std::pair<FeedSlot, std::shared_ptr<FeedSlotClaim>> ClaimFeedSlot(std::string_view name, const FeedOptions& options,
        bool takeover, std::string user, const std::function<void(std::string_view)>& authorize_owner);
    [[nodiscard]] FeedSlot ReadClaimedFeedSlot(const std::shared_ptr<FeedSlotClaim>& claim);
    void StageClaimedFeedSlotAck(const std::shared_ptr<FeedSlotClaim>& claim, FeedPosition position);
    void FlushClaimedFeedSlotAcks(const std::shared_ptr<FeedSlotClaim>& claim, bool force);
    void SyncClaimedFeedSlot(const std::shared_ptr<FeedSlotClaim>& claim);
    [[nodiscard]] FeedArchiveReader ReadClaimedFeedArchive(const std::shared_ptr<FeedSlotClaim>& claim, FeedPosition after);
    enum class State { kOpen, kBusy, kFailed, kGone };

    Table(
        std::string name,
        std::filesystem::path dir,
        StoreId store_id,
        Geometry geometry,
        TableOptions options,
        std::shared_ptr<ChunkStore> store,
        std::size_t feed_buffer_bytes,
        std::shared_ptr<MigrationHealth> migration_health,
        std::chrono::milliseconds feed_linger = std::chrono::milliseconds(30000));
    // Blocks new leases, waits for running ones and hands out the store.
    [[nodiscard]] std::shared_ptr<ChunkStore> BeginExclusive(bool closing = false, std::stop_token cancelled = {}, bool* admitted = nullptr);
    // Ends BeginExclusive: serving again with `store`, or gone when null.
    void EndExclusive(std::shared_ptr<ChunkStore> store, const TableOptions& options,
                      bool check_feed_error = false);
    void ReleaseLease() noexcept;
    // Snapshot capture drains ordinary admission without detaching the feed,
    // closing the store or changing its maintenance owner.
    class AdmissionPause {
      public:
        AdmissionPause(AdmissionPause&& other) noexcept;
        AdmissionPause(const AdmissionPause&) = delete;
        AdmissionPause& operator=(const AdmissionPause&) = delete;
        ~AdmissionPause();
        [[nodiscard]] ChunkStore& store() const noexcept { return *store_; }
      private:
        friend class Table;
        AdmissionPause(std::shared_ptr<Table> table, std::shared_ptr<ChunkStore> store) noexcept;
        std::shared_ptr<Table> table_;
        std::shared_ptr<ChunkStore> store_;
    };
    [[nodiscard]] AdmissionPause PauseAdmission(
        std::chrono::steady_clock::time_point deadline, std::stop_token cancelled);
    void ResumeAdmission() noexcept;
    void FailFeed(std::exception_ptr error);
    class BackupPin {
      public:
        BackupPin(BackupPin&& other) noexcept;
        BackupPin(const BackupPin&) = delete;
        BackupPin& operator=(const BackupPin&) = delete;
        ~BackupPin();
        ChunkStore& store() const noexcept { return *store_; }
      private:
        friend class Table;
        BackupPin(std::shared_ptr<Table> table, std::shared_ptr<ChunkStore> store)
            : table_(std::move(table)), store_(std::move(store)) {}
        std::shared_ptr<Table> table_;
        std::shared_ptr<ChunkStore> store_;
    };
    [[nodiscard]] BackupPin PinForBackup(std::stop_token cancelled);
    void ReleaseBackupPin() noexcept;
    void PrepareFeedSlotBaseline(ChunkStore& store);
    [[nodiscard]] bool RetainIdleFeed() const;
    void StopFeedLingerTimer();
    void StartFeedLingerTimer();
    [[nodiscard]] bool ExpireFeedLinger(std::stop_token cancelled);
    void QuarantineFeedLinger(const std::exception& error, std::shared_ptr<ChunkStore> store, bool admitted) noexcept;
    // Why a table in its catalog refuses commands; nullptr when it does not.
    [[nodiscard]] const char* FenceReason() const;
    // Open with a store that is neither fail-closed nor without its feed.
    [[nodiscard]] bool ServesUnfenced() const;
    // Why `store` refuses changes until it is reopened; nullptr when it does not.
    [[nodiscard]] static const char* StoreFenceReason(const ChunkStore& store);

    const std::string name_;
    const std::filesystem::path dir_;
    const StoreId store_id_;
    const std::optional<TableLineage> lineage_;
    const std::size_t feed_buffer_bytes_;
    std::atomic<std::size_t> effective_feed_buffer_bytes_;
    const std::chrono::milliseconds feed_linger_;
    // Timer control precedes exclusive admission. The worker never takes it.
    std::mutex feed_timer_control_mutex_;
    mutable std::mutex feed_timer_mutex_;
    std::condition_variable_any feed_timer_cv_;
    std::optional<std::chrono::steady_clock::time_point> feed_linger_deadline_;
    std::jthread feed_linger_timer_;
    std::atomic<bool> feed_timer_finished_{true};
    std::atomic<bool> feed_cleanup_failed_{false};
    // Why watches of a table that left service end, when not dropped: a static
    // text, set before the exclusive section that ends them.
    std::atomic<const char*> ended_{nullptr};
    const std::shared_ptr<MigrationHealth> migration_health_;
    Geometry geometry_;

    // Leases take no lock: an acquirer counts itself in active_leases_ and
    // then checks state_; an exclusive operation sets state_ to kBusy and
    // then waits for the count to drain. Both sides use sequentially
    // consistent operations, so one of them always sees the other.
    std::atomic<State> state_{State::kOpen};
    std::atomic<std::size_t> active_leases_{0};
    // Waiting only: exclusive operations for leases to drain, acquirers for
    // a reopen to finish. Also guards options_.
    mutable std::mutex mutex_;
    std::condition_variable_any cv_;
    std::mutex ddl_mutex_;
    std::size_t backup_pins_ = 0;
    std::size_t backup_pin_waiters_ = 0;
    // Written only while state_ is kBusy and no lease is active.
    std::shared_ptr<ChunkStore> store_;
    TableOptions options_;
    // Protected with options_ by mutex_.
    std::size_t slot_max_bytes_ = kDefaultSlotMaxBytes;
    std::shared_ptr<ChangeFeed> feed_;
    std::size_t feed_subscriptions_ = 0;
    std::map<std::string, std::weak_ptr<FeedSlotClaim>> slot_claims_;
    std::shared_ptr<FeedSlotAckState> slot_ack_state_;
    std::shared_ptr<std::atomic<std::size_t>> slot_output_bytes_ = std::make_shared<std::atomic<std::size_t>>(0U);
};

struct CatalogTxnSnapshot {
    std::shared_ptr<Table> table;
    std::unique_ptr<TxnSnapshot> snapshot;
};

// The tables of one data directory (docs/STORAGE_FORMAT.md Section 1):
// `chunkdb.manifest`, the writer lock, and one store per `tables/<name>/`.
// Tables share one StoreResources, so the cache and WAL-stream budgets cover
// all of them.
class TableCatalog {
    friend struct CatalogTxnTestAccess;
  public:
    // Opens (a writer creates when new) the data directory and every table
    // in it; a writer that finds no table creates `default`.
    explicit TableCatalog(CatalogConfig config);
    ~TableCatalog();

    TableCatalog(const TableCatalog&) = delete;
    TableCatalog& operator=(const TableCatalog&) = delete;

    [[nodiscard]] const std::filesystem::path& data_dir() const noexcept {
        return config_.data_dir;
    }
    [[nodiscard]] AccessMode access_mode() const noexcept { return config_.access_mode; }
    [[nodiscard]] const TableOptions& default_options() const noexcept {
        return config_.default_options;
    }
    [[nodiscard]] const std::shared_ptr<StoreResources>& resources() const noexcept {
        return resources_;
    }

    // nullptr when there is no such table.
    [[nodiscard]] std::shared_ptr<Table> Find(std::string_view name) const;
    // Captures one common directory boundary before the first transaction
    // statement. Callers hold no table lease; later reads use these original
    // snapshots even when they first touch a table in a later statement.
    [[nodiscard]] std::vector<CatalogTxnSnapshot> CaptureTxnSnapshots(
        std::chrono::milliseconds max_duration, std::stop_token cancelled = {});
    // Sorted by name.
    [[nodiscard]] std::vector<TableInfo> List() const;
    [[nodiscard]] std::size_t TableCount() const;
    // Find, but throws TableFencedError when the table is fenced and out of
    // service, and TableNotFoundError when there is no such table.
    [[nodiscard]] std::shared_ptr<Table> Require(std::string_view name) const;
    // Fenced tables, sorted by name: those out of service, which Find does
    // not return, and those it returns that refuse changes.
    [[nodiscard]] std::vector<TableFence> Fences() const;
    [[nodiscard]] std::optional<TableFence> Fence(std::string_view name) const;
    // Runs for one fenced table what opening the catalog runs for each table,
    // while other tables keep serving: waits for the commands running on it
    // and closes its store, removes its interrupted creation and drop
    // directories, drops an interrupted narrowing and opens it again. A table
    // whose directory is gone is gone afterwards. Throws std::invalid_argument
    // for a table that is not fenced, and MigrationRecoveryRequiredError or
    // FeatureRecoveryRequiredError while the catalog is fenced, which only a
    // restart recovers. After a failure the table stays fenced.
    // `cancelled` ends the wait for a running feature activation or backup.
    void RecoverTable(std::string_view name, std::stop_token cancelled = {});
    // Drops collected deletion identities older than before and reopens the table.
    [[nodiscard]] std::uint64_t CompactTombstones(std::string_view name, std::uint64_t before);

    // Each is crash-atomic: after a crash the table exists completely, with
    // the old or the new options, or not at all. Throws TableExistsError,
    // TableNotFoundError, std::invalid_argument (bad name, geometry or
    // options, read-only catalog) or std::runtime_error (I/O).
    // Without `schema` the table has one column bits(geometry.block_bits);
    // with it, geometry.block_bits must be its FixedBitsPerBlock.
    std::shared_ptr<Table> Create(
        std::string_view name,
        const GeometryConfig& geometry,
        const TableOptions& options,
        const std::optional<TableSchema>& schema = std::nullopt,
        bool if_not_exists = false);
    // Evaluates the definition only when creation is needed, under admission.
    std::shared_ptr<Table> Create(
        std::string_view name, const std::function<TableDefinition()>& definition,
        bool if_not_exists = false);
    // Waits for running commands on the table (so the calling thread must
    // not hold a Lease on it). Irreversible.
    void Drop(std::string_view name);
    void Drop(std::string_view name, bool if_exists);
    void Drop(std::string_view name, UserRegistry* users, bool if_exists = false);
    // Applies `update` to the table's current options (under the same lock
    // as other table operations, so concurrent changes do not undo each
    // other), persists them and reopens the table; its chunks leave the
    // cache. Waits for running commands like Drop.
    void SetOptions(std::string_view name, const TableOptionsUpdate& update);
    // Replaces every option.
    void SetOptions(std::string_view name, const TableOptions& options);
    // Changes the table's columns (docs/design/COLUMNS_DESIGN.md): writes the next
    // schema version, which `change` makes from the current one
    // (AddColumn, DropColumn, RenameColumn of chunkdb/schema.hpp), into the
    // manifest atomically and reopens the table like SetOptions. Nothing else
    // is rewritten: chunks written by earlier versions are translated when
    // they load. Throws std::invalid_argument for a change the schema rules
    // refuse; the table then stays as it was.
    void ChangeColumns(std::string_view name, const std::function<TableSchema(const TableSchema&)>& change);
    // A null result leaves the existing schema/store/feed untouched. The
    // decision is serialized with column changes and table deletion.
    [[nodiscard]] bool ChangeColumnsIfNeeded(
        std::string_view name,
        const std::function<std::optional<TableSchema>(const TableSchema&)>& change);
    // Chunk-column changes share the block schema revision and the same
    // exclusive manifest publication and recovery rules as block columns.
    void ChangeChunkColumns(std::string_view name, const std::function<TableSchema(const TableSchema&)>& change);
    [[nodiscard]] bool ChangeChunkColumnsIfNeeded(
        std::string_view name,
        const std::function<std::optional<TableSchema>(const TableSchema&)>& change);
    // Narrows column `column` to `type`, a type of its family that does not
    // hold every value of its own, after checking every stored value
    // (docs/design/COLUMNS_DESIGN.md): the manifest first records the narrowing in
    // progress, so writes to the column must fit `type` too while every
    // populated chunk is read; then the next schema version is written, or,
    // when a value does not fit, the narrowing is dropped and
    // std::invalid_argument names that block and value. A crash before the
    // end leaves the schema as it was: the next open drops the narrowing.
    // Holds the catalog's table operations for the whole check.
    void NarrowColumn(std::string_view name, std::string_view column, ColumnType type);

    void SetMigrationTestHook(MigrationTestHook* hook) noexcept { migration_health_->hook.store(hook, std::memory_order_release); }
    [[nodiscard]] bool Migrate(const MigrationRequest& request, UserRegistry* users);
    [[nodiscard]] std::vector<MigrationRecord> Migrations() const;
    [[nodiscard]] const std::shared_ptr<MigrationHealth>& migration_health() const noexcept { return migration_health_; }

    // WalBarrier on every table. Every table is attempted; the first
    // failure is rethrown afterwards.
    [[nodiscard]] bool EnableFeature(std::string_view name);
    [[nodiscard]] CatalogFeatureStatus FeatureStatus();
    void WalBarrier();
    [[nodiscard]] BackupResult BackupTo(const std::filesystem::path& target, const BackupOptions& options);
    void SetBackupHookForTests(BackupTestHook* hook) noexcept { backup_hook_.store(hook, std::memory_order_release); }

  private:
    [[nodiscard]] std::filesystem::path TablesDir() const;
    friend struct BackupTestAccess;
    friend struct FeatureActivationTestAccess;
    [[nodiscard]] std::filesystem::path StagingDir() const;
    [[nodiscard]] std::filesystem::path DroppedDir() const;
    void OpenDataDirManifest();
    // Raises the data directory's version floor (chunkdb.manifest) durably.
    void RaiseVersionFloor(std::uint64_t floor);
    // Every interrupted operation, or only those of `table`.
    void RemoveInterruptedOperations(std::string_view table = {});
    void OpenExistingTables();
    struct FoundTable;
    // Reads a table directory to open; a writer drops an interrupted narrowing.
    [[nodiscard]] FoundTable ReadTableToOpen(const std::string& name, const std::filesystem::path& dir);
    [[nodiscard]] std::shared_ptr<Table> OpenFoundTable(FoundTable& table);
    // Throws as opening does for a table directory without its manifest.
    [[nodiscard]] StoreManifest ReadOpenableManifest(const std::filesystem::path& dir) const;
    // Throws std::invalid_argument when opening would refuse the stored options.
    void RequireRecoverableOptions(const std::string& name, const StoreManifest& manifest) const;
    [[nodiscard]] std::shared_ptr<ChunkStore> OpenStore(
        const std::string& name,
        const std::filesystem::path& dir,
        const GeometryConfig& geometry,
        std::uint32_t geometry_fields,
        const TableOptions& options);
    // " --flag value (table 'name' stores value)" for every option named in
    // default_option_fields that differs from `stored`; empty when none.
    [[nodiscard]] std::string OptionFlagMismatches(
        const std::string& name,
        const TableOptions& stored) const;
    void RequireWritable(const char* operation) const;
    // Waits for the table's commands, writes its batched writes to their WALs,
    // replaces its manifest with `change` applied (atomically, synced) and
    // reopens it with `options`. Before the manifest is replaced a failure
    // leaves the table as it was; after it, the table serves the new manifest
    // or, if it cannot reopen, is fenced until RecoverTable or a restart.
    static void RequireCompatibleColumnAdditions(
        const TableSchema& before, const TableSchema& after, ChunkStore& store);
    void RewriteManifest(
        Table& table,
        const TableOptions& options,
        const std::function<void(StoreManifest*)>& change,
        const char* dir_sync_failpoint,
        const std::optional<TableSchema>& chunk_schema = std::nullopt);
    // Moves `dir` out of `tables/` into `.chunkdb.dropped/` and syncs both.
    void MoveToDropped(const std::filesystem::path& dir, const std::string& name);
    void RemoveDroppedTree(const std::filesystem::path& dropped) noexcept;
    // Ends an exclusive section with the table out of service: removed from
    // the catalog and gone for every connection holding it.
    void RetireTable(Table& table, const TableOptions& options);
    struct Fenced {
        // nullptr for a table that never served.
        std::shared_ptr<Table> table;
        const char* reason = nullptr;
    };
    using FenceNode = std::map<std::string, Fenced, std::less<>>::node_type;
    // Made before the step that may fail, so that fencing does not allocate.
    [[nodiscard]] static FenceNode PrepareFence(std::shared_ptr<Table> table, const std::string& name, const char* reason);
    // RetireTable, with the table's name fenced until RecoverTable.
    void FenceTable(Table& table, const TableOptions& options, FenceNode fence);
    void ThrowIfFenced(std::string_view name) const;
    using ServedNode = std::map<std::string, std::shared_ptr<Table>, std::less<>>::node_type;
    [[nodiscard]] static ServedNode PrepareServed(const std::string& name);

    CatalogConfig config_;
    std::shared_ptr<StoreResources> resources_;
    std::unique_ptr<ProcessLock> process_lock_;
    // Serializes Create, Drop and SetOptions.
    mutable std::mutex operations_mutex_;
    std::mutex backup_mutex_;
    // Slow metadata admission only; ordinary read/write leases do not take it.
    BackupMaintenanceGate feature_metadata_gate_;
    // Named schema operations cannot straddle backup metadata capture.
    // Ordinary table DDL retains its per-table admission. Copying holds no gate.
    BackupMaintenanceGate backup_metadata_gate_;
    std::atomic<BackupTestHook*> backup_hook_{nullptr};
    std::shared_ptr<MigrationHealth> migration_health_ = std::make_shared<MigrationHealth>();
    // The data directory's version floor (see DataDirVersionFloor); changed
    // under operations_mutex_.
    std::uint64_t version_floor_ = 0;
    mutable std::shared_mutex tables_mutex_;
    std::map<std::string, std::shared_ptr<Table>, std::less<>> tables_;
    // Out of service until RecoverTable; disjoint from tables_, which
    // tables_mutex_ guards with it.
    std::map<std::string, Fenced, std::less<>> fenced_;
};

}  // namespace chunkdb
