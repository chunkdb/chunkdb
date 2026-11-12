#include "chunkdb/table_catalog.hpp"

#include "chunkdb/logging.hpp"
#include "store_manifest.hpp"

namespace chunkdb {

void TableCatalog::ChangeChunkColumns(
    std::string_view name, const std::function<TableSchema(const TableSchema&)>& change) {
    (void)ChangeChunkColumnsIfNeeded(name, change);
}

bool TableCatalog::ChangeChunkColumnsIfNeeded(
    std::string_view name,
    const std::function<std::optional<TableSchema>(const TableSchema&)>& change) {
    RequireWritable("ALTER TABLE");
    std::shared_lock metadata(feature_metadata_gate_);
    const auto table = Require(name);
    std::lock_guard ddl(table->ddl_mutex_);
    {
        auto lease = table->Acquire();
        if (!lease) throw TableNotFoundError("table was dropped");
        lease->store().ThrowIfDurabilityPoisoned();
    }
    std::optional<TableSchema> changed;
    {
        std::lock_guard operations(operations_mutex_);
        migration_health_->Check();
        if (Find(name) != table) throw TableNotFoundError("table was dropped");
        changed = change(table->Info().chunk_schema);
        if (!changed) return false;
        ValidateChunkSchema(*changed);
    }
    RewriteManifest(*table, table->Info().options, [](StoreManifest*) {},
        "CHUNKDB_FAILPOINT_ALTER_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", changed);
    LogMessage(LogLevel::kInfo, LogComponent::kStore, "table chunk columns changed",
        {{"table", table->name_}, {"schema_version", std::to_string(changed->version)}});
    return true;
}

}  // namespace chunkdb
