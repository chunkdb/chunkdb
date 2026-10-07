#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/geometry.hpp"

namespace chunkdb {

class ProcessLock;

// The table a connection starts on, created when a writer finds no tables.
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

    // Geometry of `default` when the catalog creates it. When `default`
    // exists, every field named in default_geometry_fields must match it.
    GeometryConfig default_geometry;
    std::uint32_t default_geometry_fields = kAllGeometryFields;

    // Options of `default` when the catalog creates it, and the defaults the
    // server offers for new tables. Existing tables keep their stored
    // options; a field named in default_option_fields must equal every
    // existing table's stored value, or the catalog refuses to open.
    TableOptions default_options;
    std::uint32_t default_option_fields = 0;

    // Shared by all tables (StoreResources).
    std::size_t max_loaded_chunks = kDefaultMaxLoadedChunks;
    std::size_t max_open_wal_streams = 1024;
    bool background_maintenance = false;
    std::size_t background_checkpoint_queue_limit = 4096;
};

// A catalog configuration whose `default` table and new-table defaults come
// from a store configuration (geometry, options, budgets). `option_fields`
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

    // Every field set from `options`.
    [[nodiscard]] static TableOptionsUpdate From(const TableOptions& options);
    [[nodiscard]] TableOptions ApplyTo(TableOptions options) const;
    [[nodiscard]] bool empty() const noexcept;
};

struct TableInfo {
    std::string name;
    StoreId store_id{};
    GeometryConfig geometry;
    TableOptions options;
};

// One table of a catalog. A connection keeps a shared_ptr<Table> for the
// table it selected; every command on it runs under a Lease.
class Table {
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

    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;

    [[nodiscard]] const std::string& name() const noexcept { return name_; }
    [[nodiscard]] const StoreId& store_id() const noexcept { return store_id_; }
    [[nodiscard]] const GeometryConfig& geometry() const noexcept { return geometry_; }
    [[nodiscard]] TableInfo Info() const;
    // Waits while the table is being reopened. std::nullopt once it was
    // dropped: a table of the same name created later is another table.
    [[nodiscard]] std::optional<Lease> Acquire();

  private:
    friend class TableCatalog;
    enum class State { kOpen, kBusy, kGone };

    Table(
        std::string name,
        std::filesystem::path dir,
        StoreId store_id,
        GeometryConfig geometry,
        TableOptions options,
        std::shared_ptr<ChunkStore> store);
    // Blocks new leases, waits for running ones and hands out the store.
    [[nodiscard]] std::shared_ptr<ChunkStore> BeginExclusive();
    // Ends BeginExclusive: serving again with `store`, or gone when null.
    void EndExclusive(std::shared_ptr<ChunkStore> store, const TableOptions& options);
    void ReleaseLease() noexcept;

    const std::string name_;
    const std::filesystem::path dir_;
    const StoreId store_id_;
    const GeometryConfig geometry_;

    // Leases take no lock: an acquirer counts itself in active_leases_ and
    // then checks state_; an exclusive operation sets state_ to kBusy and
    // then waits for the count to drain. Both sides use sequentially
    // consistent operations, so one of them always sees the other.
    std::atomic<State> state_{State::kOpen};
    std::atomic<std::size_t> active_leases_{0};
    // Waiting only: exclusive operations for leases to drain, acquirers for
    // a reopen to finish. Also guards options_.
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    // Written only while state_ is kBusy and no lease is active.
    std::shared_ptr<ChunkStore> store_;
    TableOptions options_;
};

// The tables of one data directory (docs/STORAGE_FORMAT.md Section 1):
// `chunkdb.manifest`, the writer lock, and one store per `tables/<name>/`.
// Tables share one StoreResources, so the cache and WAL-stream budgets cover
// all of them.
class TableCatalog {
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
    // Sorted by name.
    [[nodiscard]] std::vector<TableInfo> List() const;
    [[nodiscard]] std::size_t TableCount() const;

    // Each is crash-atomic: after a crash the table exists completely, with
    // the old or the new options, or not at all. Throws TableExistsError,
    // TableNotFoundError, std::invalid_argument (bad name, geometry or
    // options, read-only catalog) or std::runtime_error (I/O).
    std::shared_ptr<Table> Create(
        std::string_view name,
        const GeometryConfig& geometry,
        const TableOptions& options);
    // Waits for running commands on the table (so the calling thread must
    // not hold a Lease on it). Irreversible.
    void Drop(std::string_view name);
    // Applies `update` to the table's current options (under the same lock
    // as other table operations, so concurrent changes do not undo each
    // other), persists them and reopens the table; its chunks leave the
    // cache. Waits for running commands like Drop.
    void SetOptions(std::string_view name, const TableOptionsUpdate& update);
    // Replaces every option.
    void SetOptions(std::string_view name, const TableOptions& options);

    // WalBarrier on every table. Every table is attempted; the first
    // failure is rethrown afterwards.
    void WalBarrier();

  private:
    [[nodiscard]] std::filesystem::path TablesDir() const;
    [[nodiscard]] std::filesystem::path StagingDir() const;
    [[nodiscard]] std::filesystem::path DroppedDir() const;
    void OpenDataDirManifest();
    void RemoveInterruptedOperations();
    void OpenExistingTables();
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
    // Moves `dir` out of `tables/` into `.chunkdb.dropped/` and syncs both.
    void MoveToDropped(const std::filesystem::path& dir, const std::string& name);
    void RemoveDroppedTree(const std::filesystem::path& dropped) noexcept;
    // Ends an exclusive section with the table out of service: removed from
    // the catalog (until restart) and gone for every connection holding it.
    void RetireTable(Table& table, const TableOptions& options);

    CatalogConfig config_;
    std::shared_ptr<StoreResources> resources_;
    std::unique_ptr<ProcessLock> process_lock_;
    // Serializes Create, Drop and SetOptions.
    std::mutex operations_mutex_;
    mutable std::shared_mutex tables_mutex_;
    std::map<std::string, std::shared_ptr<Table>, std::less<>> tables_;
};

}  // namespace chunkdb
