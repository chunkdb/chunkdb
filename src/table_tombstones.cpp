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
    auto fence = PrepareFence(table, table->name_, "tombstone compaction could not reopen it");
    auto store = table->BeginExclusive();
    if (!store) throw TableNotFoundError("table was dropped");
    ScopeExit retire([&] { FenceTable(*table, options, std::move(fence)); store.reset(); });
    bool publication_started = false;
    try {
        std::lock_guard operations(operations_mutex_);
        migration_health_->Check();
        store->ThrowIfDurabilityPoisoned();
        const auto completed = store->CompletedRevision();
        if (!completed) throw std::invalid_argument("tombstone compaction requires a writer frontier");
        if (before <= 1U || *completed == 0U) {
            const auto floor = store->RetainedDeletionFloor();
            table->EndExclusive(store, options);
            retire.Dismiss();
            return floor;
        }
        store->FlushWalBatchesForReopen();
        const auto unsynced = std::make_shared<ChunkStore::UnsyncedArtifacts>();
        store->HandOverUnsyncedOnClose(unsynced);
        publication_started = true;
        {
            // Collection cannot overlap staged decisions or their bracket.
            std::unique_lock maintenance(store->backup_maintenance_mutex_);
            std::unique_lock publication(store->checkpoint_publish_mutex_);
            ChunkStore::SnapshotGenerationWriteGuard snapshot(store.get());
            store->coordinate_index_->Compact(before, *completed);
            snapshot.Finish();
        }
        store.reset();
        store = OpenStore(table->name_, table->dir_, table->geometry().config(), 0U, options);
        store->AdoptUnsynced(*unsynced);
        const auto floor = store->RetainedDeletionFloor();
        table->EndExclusive(store, options);
        retire.Dismiss();
        return floor;
    } catch (const std::exception&) {
        // Resume the old owner explicitly while its artifacts are unchanged.
        // If resume itself fails, the prepared fence still closes admission.
        if (!publication_started && store) {
            table->EndExclusive(store, options);
            retire.Dismiss();
        }
        throw;
    }
}

}  // namespace chunkdb
