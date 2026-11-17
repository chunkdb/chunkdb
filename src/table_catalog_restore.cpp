#include "chunkdb/table_catalog.hpp"

#include <cstdlib>
#include <sstream>

#include "backup.hpp"
#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/logging.hpp"
#include "durability_io.hpp"
#include "feature_flags.hpp"
#include "file_permissions.hpp"
#include "store_manifest.hpp"
#include "verify.hpp"

namespace chunkdb {
namespace {

constexpr const char* kFenceRestore = "a failed restore could not remove it";

void CrashAtFailpoint(const char* key) {
    if (ConsumeFailpointEnv(key)) std::_Exit(86);
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
    RequireValidTableName(request.name);
    // ENABLE FEATURE cannot change the directory's features between the
    // feature check below and publication.
    std::shared_lock metadata(feature_metadata_gate_);
    const auto require_name = [&] {
        migration_health_->Check();
        ThrowIfFenced(request.name);
        if (Find(request.name) != nullptr) throw TableExistsError("table '" + request.name + "' already exists");
    };
    {
        std::lock_guard operations(operations_mutex_);
        require_name();
    }
    const auto directory_manifest = ReadDataDirManifest(config_.data_dir);
    if (!directory_manifest) throw std::runtime_error("data directory manifest disappeared");
    LogMessage(LogLevel::kInfo, LogComponent::kStore, "table restore started",
        {{"table", request.name}, {"backup", request.backup.string()}, {"backed_up_table", request.table}});

    // Build the table under a private name, as Create does: a crash leaves
    // the staging directory, which the next writer start removes.
    EnsureDirectoryPathExists(StagingDir(), /*durable_sync=*/true);
    const auto staging = StagingDir() / OperationName(request.name);
    if (!CreateOwnerOnlyDirectory(staging)) throw std::runtime_error("table staging directory already exists");
    const auto target = TablesDir() / request.name;
    TableRestoreResult result;
    result.name = request.name;
    StoreManifest manifest;
    std::unique_lock operations(operations_mutex_, std::defer_lock);
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
        const auto served = TableScopedFeatures(directory_manifest->features);
        if (TableScopedFeatures(manifest.features).incompat != served.incompat)
            throw std::invalid_argument("the backed-up table has features " + DescribeFeatures(TableScopedFeatures(manifest.features)) +
                " but this data directory gives its tables " + DescribeFeatures(served));
        RequireValidTableOptions(DecodeTableOptions(manifest.options));
        RequireRecoverableOptions(request.name, manifest);
        result.parent_epoch = copy.cut.epoch;
        result.fork_revision = copy.cut.revision;
        result.files = copy.files.size();
        result.bytes = copy.bytes;
        result.epoch = RebindRestoredTable(staging, copy.cut, copy.files, request.cancelled);
        manifest.store_id = result.epoch;
        manifest.lineage = TableLineage{result.parent_epoch, result.fork_revision};
        std::ostringstream findings;
        if (VerifyTableDirectory(staging, findings).errors != 0U)
            throw std::runtime_error("restored table failed verification: " + findings.str());
        SyncBackupTree(staging, request.cancelled);
        if (request.cancelled.stop_requested()) throw std::runtime_error("backup cancelled");
        CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_RESTORE_TABLE_BEFORE_PUBLISH_ONCE");
        operations.lock();
        require_name();
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_RESTORE_TABLE_PUBLISH_FAIL_ONCE"))
            throw std::runtime_error("injected failure before publishing a restored table");
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
    const auto options = DecodeTableOptions(manifest.options);
    std::shared_ptr<ChunkStore> store;
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_RESTORE_TABLE_OPEN_FAIL_ONCE"))
            throw std::runtime_error("injected failure opening a restored table");
        SyncDirectoryPath(TablesDir());
        SyncDirectoryPath(StagingDir());
        store = OpenStore(request.name, target, manifest.geometry, 0U, options);
    } catch (const std::exception& e) {
        LogMessage(LogLevel::kError, LogComponent::kStore, "restored table cannot be made durable or opened; removing it",
            {{"table", request.name}, {"error", e.what()}});
        try {
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_RESTORE_TABLE_UNDO_FAIL_ONCE"))
                throw std::runtime_error("injected failure removing a restored table");
            MoveToDropped(target, request.name);
        } catch (const std::exception& undo_error) {
            LogMessage(LogLevel::kError, LogComponent::kStore,
                "restored table could not be removed again; it is fenced until RECOVER TABLE or a restart opens it",
                {{"table", request.name}, {"error", undo_error.what()}});
            std::unique_lock lock(tables_mutex_);
            fenced_.insert(PrepareFence(nullptr, request.name, kFenceRestore));
        }
        throw;
    }
    Geometry geometry = store->geometry();
    auto table = std::shared_ptr<Table>(new Table(request.name, target, result.epoch, std::move(geometry), options,
        std::move(store), config_.feed_buffer_bytes, migration_health_, config_.feed_linger));
    {
        std::unique_lock lock(tables_mutex_);
        tables_.emplace(request.name, std::move(table));
    }
    LogMessage(LogLevel::kInfo, LogComponent::kStore, "table restored",
        {{"table", request.name}, {"store_id", StoreIdHex(result.epoch)}, {"parent_epoch", StoreIdHex(result.parent_epoch)},
         {"fork_revision", std::to_string(result.fork_revision)}, {"files", std::to_string(result.files)},
         {"bytes", std::to_string(result.bytes)}});
    return result;
}

}  // namespace chunkdb
