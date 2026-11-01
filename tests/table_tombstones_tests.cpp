#include <cassert>
#include <cstdlib>
#include <new>

#include "checkpoint.hpp"
#include "change_feed.hpp"
#include "chunkdb/table_catalog.hpp"
#include "test_utils.hpp"

namespace allocation { thread_local bool fail_next = false; }
void* operator new(std::size_t bytes) {
    if (allocation::fail_next) { allocation::fail_next = false; throw std::bad_alloc(); }
    if (auto* memory = std::malloc(bytes == 0U ? 1U : bytes)) return memory;
    throw std::bad_alloc();
}
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void operator delete(void* pointer) noexcept { std::free(pointer); }
void* operator new[](std::size_t bytes) { return ::operator new(bytes); }
void operator delete[](void* pointer) noexcept { ::operator delete(pointer); }
void operator delete(void* pointer, std::size_t) noexcept { ::operator delete(pointer); }
void operator delete[](void* pointer, std::size_t) noexcept { ::operator delete(pointer); }

namespace chunkdb {
struct FeatureActivationTestAccess {
    static void ReleaseGeometryCopyStorage(Table& table) {
        // No lease can observe this exclusive-owner storage. A later schema
        // copy must allocate just as it would with exhausted vector capacity.
        table.geometry_ = Geometry(table.geometry_.config());
    }
};
}

namespace {
using namespace chunkdb;
CatalogConfig Config(const std::filesystem::path& path) {
    CatalogConfig config; config.data_dir = path; config.background_maintenance = false;
    config.default_options.checkpoint_update_interval = 1000U;
    config.default_options.wal_group_commit_updates = 1000U;
    return config;
}
std::uint64_t Populate(TableCatalog& catalog) {
    GeometryConfig geometry; geometry.chunk_width_blocks = 2U; geometry.chunk_height_blocks = 2U; geometry.block_bits = 8U;
    const auto table = catalog.Create("t", geometry, catalog.default_options());
    auto lease = table->Acquire();
    auto& store = lease->store();
    store.SetBlockBits(0, 0, "10000000");
    store.UnsetBlock(0, 0);
    const auto deleted = store.GetChunkVersion(0, 0);
    store.CheckpointForTests(0, 0);
    assert(store.RetainedTombstoneCount() == 1U);
    store.SetBlockBits(2, 0, "10000000");
    store.SetBlockBits(2, 0, "01000000");
    return deleted;
}
void CompactionReopensAndReportsHistoryFloor() {
    test::ScopedTempDir directory("chunkdb-table-tombstones");
    const auto config = Config(directory.path());
    std::uint64_t deleted = 0U;
    {
        TableCatalog catalog(config);
        deleted = Populate(catalog);
        assert(catalog.CompactTombstones("t", 0U) == 0U);
        assert(catalog.CompactTombstones("t", deleted + 1U) == deleted);
        auto lease = catalog.Require("t")->Acquire();
        auto& store = lease->store();
        assert(!store.ReadChunkState(0, 0).written);
        assert(store.GetBlockBits(2, 0) == "01000000");
        assert(store.RetainedTombstoneCount() == 0U && store.RetainedDeletionFloor() == deleted);
        bool refused = false;
        try { (void)store.ScanChunkIdentities(false, {}, 10U, 0U, true); }
        catch (const TombstoneHistoryLostError& error) {
            refused = error.retained_deletion_floor() == deleted && error.position().has_value();
        }
        assert(refused);
        const auto page = store.ScanChunkIdentities(false, {}, 10U, deleted, true);
        assert(page.entries.size() == 1U && page.entries[0].data);
        store.WalBarrier();
    }
    auto readonly = config; readonly.access_mode = AccessMode::kReadOnly;
    TableCatalog reader(readonly);
    bool refused = false;
    try { (void)reader.CompactTombstones("t", deleted + 1U); }
    catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
    auto lease = reader.Require("t")->Acquire();
    assert(lease->store().RetainedDeletionFloor() == deleted && lease->store().RetainedTombstoneCount() == 0U);
    assert(lease->store().GetBlockBits(2, 0) == "01000000");
}
void CompactionFailureFencesUntilRecovery() {
    for (const auto fail_at : {1U, 2U}) {
        test::ScopedTempDir directory("chunkdb-table-tombstone-failure");
        TableCatalog catalog(Config(directory.path()));
        const auto deleted = Populate(catalog);
        const auto shard = std::filesystem::canonical(directory.path()) / "tables/t/L_0_0";
        std::size_t calls = 0U;
        SetDirectorySyncObserverForTests([&](const std::filesystem::path& path) {
            if (path == shard && ++calls == fail_at) throw std::runtime_error("injected compaction directory sync failure");
        });
        bool failed = false;
        try { (void)catalog.CompactTombstones("t", deleted + 1U); }
        catch (const std::runtime_error& error) {
            failed = std::string_view(error.what()).find("injected compaction") != std::string_view::npos;
        }
        SetDirectorySyncObserverForTests({});
        assert(failed && !catalog.Find("t") && catalog.Fence("t"));
        catalog.RecoverTable("t");
        auto lease = catalog.Require("t")->Acquire();
        assert(lease->store().GetBlockBits(2, 0) == "01000000");
        assert(lease->store().ReadChunkState(0, 0).written == (fail_at == 1U));
        assert(lease->store().RetainedDeletionFloor() == (fail_at == 1U ? 0U : deleted));
    }
}
void ResumeAllocationFailureDoesNotEscapeCleanup() {
    struct Hook final : FeedTestHook {
        Table* table = nullptr;
        bool armed = true;
        void Run(Point point, std::uint64_t) override {
            if (point != Point::kAfterResume || !armed) return;
            armed = false;
            FeatureActivationTestAccess::ReleaseGeometryCopyStorage(*table);
            allocation::fail_next = true;
        }
    } hook;
    for (const bool compact : {false, true}) {
        test::ScopedTempDir directory("chunkdb-compaction-resume-allocation");
        TableCatalog catalog(Config(directory.path()));
        const auto table = catalog.Create("t", [&] {
            return TableDefinition{.geometry = {2U, 2U, 2U, 2U, 8U}, .options = catalog.default_options(),
                .schema = SingleBitsColumnSchema(8U), .chunk_schema = TableSchema{
                    .next_column_id = 2U, .columns = {{.id = 1U, .name = "owner",
                        .type = {ColumnKind::kUnsigned, 8U}, .default_value = {}}}}};
        });
        std::uint64_t deleted = 0U;
        {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(0, 0, "10000000");
            lease->store().UnsetBlock(0, 0);
            deleted = lease->store().GetChunkVersion(0, 0);
            lease->store().CheckpointForTests(0, 0);
        }
        auto subscription = table->SubscribeFeed();
        hook.table = table.get(); hook.armed = true;
        FeedTestAccess::SetHook(*table, &hook);
        bool failed = false;
        try { (void)catalog.CompactTombstones("t", compact ? deleted + 1U : 0U); }
        catch (const std::bad_alloc&) { failed = true; }
        assert(failed && !allocation::fail_next);
        FeedTestAccess::SetHook(*table, nullptr);
        if (compact) assert(!catalog.Find("t") && catalog.Fence("t"));
        else {
            auto lease = catalog.Require("t")->Acquire();
            assert(lease && lease->store().ReadChunkState(0, 0).written);
        }
    }
}
}
int main(int argc, char**) {
    if (argc == 1) { CompactionReopensAndReportsHistoryFloor(); CompactionFailureFencesUntilRecovery(); }
    ResumeAllocationFailureDoesNotEscapeCleanup();
}
