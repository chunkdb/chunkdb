#include "chunkdb/table_catalog.hpp"

#include "chunk_coordinate_index.hpp"

namespace chunkdb {
namespace {
template <typename Fn>
class ScopeExit {
  public:
    explicit ScopeExit(Fn fn) : fn_(std::move(fn)) {}
    ScopeExit(const ScopeExit&) = delete;
    ScopeExit& operator=(const ScopeExit&) = delete;
    ~ScopeExit() { if (active_) fn_(); }
    void Dismiss() noexcept { active_ = false; }
  private:
    Fn fn_;
    bool active_ = true;
};
}

std::uint64_t TableCatalog::CompactTombstones(std::string_view name, std::uint64_t before) {
    RequireWritable("COMPACT TOMBSTONES");
    std::shared_lock metadata(feature_metadata_gate_);
    const auto table = Require(name);
    std::lock_guard ddl(table->ddl_mutex_);
    const auto options = table->Info().options;
    auto store = table->BeginExclusive();
    if (!store) throw TableNotFoundError("table was dropped");
    ScopeExit restore([&] { table->EndExclusive(std::move(store), options); });
    std::lock_guard operations(operations_mutex_);
    migration_health_->Check();
    store->ThrowIfDurabilityPoisoned();
    const auto completed = store->CompletedRevision();
    if (!completed) throw std::invalid_argument("tombstone compaction requires a writer frontier");
    if (before <= 1U || *completed == 0U) return store->RetainedDeletionFloor();
    store->FlushWalBatchesForReopen();
    auto fence = PrepareFence(table, table->name_, "tombstone compaction could not reopen it");
    const auto unsynced = std::make_shared<ChunkStore::UnsyncedArtifacts>();
    store->HandOverUnsyncedOnClose(unsynced);
    restore.Dismiss();
    ScopeExit retire([&] { store.reset(); FenceTable(*table, options, std::move(fence)); });
    {
        // Collection and its retained-log publication cannot overlap the
        // staged compaction decision or its snapshot-generation bracket.
        std::unique_lock maintenance(store->backup_maintenance_mutex_);
        std::unique_lock publication(store->checkpoint_publish_mutex_);
        ChunkStore::SnapshotGenerationWriteGuard snapshot(store.get());
        store->coordinate_index_->Compact(before, *completed);
        snapshot.Finish();
    }
    store.reset();
    auto reopened = OpenStore(table->name_, table->dir_, table->geometry().config(), 0U, options);
    reopened->AdoptUnsynced(*unsynced);
    const auto floor = reopened->RetainedDeletionFloor();
    retire.Dismiss();
    table->EndExclusive(std::move(reopened), options);
    return floor;
}

}  // namespace chunkdb
