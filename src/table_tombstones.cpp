#include "chunkdb/table_catalog.hpp"

#include "chunk_coordinate_index.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/file_layout.hpp"

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
        if (store->allow_multiple_processes_)
            throw std::invalid_argument("tombstone compaction requires a single-process table");
        const auto completed = store->CompletedRevision();
        if (!completed) throw std::invalid_argument("tombstone compaction requires a writer frontier");
        if (before <= 1U || *completed == 0U) {
            const auto floor = store->RetainedDeletionFloor();
            table->EndExclusive(store, options);
            retire.Dismiss();
            return floor;
        }
        const auto unsynced = std::make_shared<ChunkStore::UnsyncedArtifacts>();
        store->HandOverUnsyncedOnClose(unsynced);
        publication_started = true;
        // A current WAL may omit the REQUIRED metadata initialized by an old
        // retained base. Publish complete durable current states before that
        // base can disappear, while each checkpoint owns its normal locks.
        store->WalBarrier();
        bool have_cursor = false;
        ChunkCoord cursor{};
        for (;;) {
            const auto page = store->coordinate_index_->CurrentPage(have_cursor, cursor, kMaxChunkScanLimit);
            for (const auto& coord : page.coords) {
                auto chunk = store->TryGetLoadedChunk(coord);
                const bool artifact = std::filesystem::exists(ChunkDataPath(store->data_dir_, store->geometry_, coord)) ||
                    std::filesystem::exists(ChunkWalPath(store->data_dir_, store->geometry_, coord));
                if (!artifact && !chunk) continue;
                if (!chunk) chunk = store->GetOrLoadRegularChunk(coord);
                std::unique_lock lock(chunk->mutex);
                if (!chunk->written) continue;
                if (!artifact) {
                    const auto retained = store->coordinate_index_->EmptyBoundary(coord);
                    if (retained && retained->revision == chunk->version &&
                        !ChunkPopulated(store->geometry_, chunk->presence_bitmap, chunk->chunk_columns.get())) continue;
                }
                if (!store->CheckpointChunk(coord, chunk))
                    throw std::runtime_error("tombstone compaction could not checkpoint current state");
            }
            if (!page.more) break;
            have_cursor = true;
            cursor = page.coords.back();
        }
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
