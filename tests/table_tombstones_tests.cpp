#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <future>
#include <iostream>
#include <new>
#include <sstream>
#include <tuple>

#include "checkpoint.hpp"
#include "change_feed.hpp"
#include "chunk_coordinate_index.hpp"
#include "chunkdb/file_layout.hpp"
#include "feed_slots.hpp"
#include "chunkdb/table_catalog.hpp"
#include "test_utils.hpp"
#include "verify.hpp"
#ifndef _WIN32
#include <sys/wait.h>
#endif

namespace allocation { thread_local bool fail_next = false; }
#if defined(__GNUC__)
__attribute__((noinline))
#endif
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
        const auto info = catalog.Require("t")->Info();
        const Geometry geometry(info.geometry, info.schema, info.chunk_schema);
        const auto inventory = std::filesystem::canonical(directory.path()) / "tables/t" / kChunkIndexFileName;
        SetDirectorySyncObserverForTests([&](const std::filesystem::path& path) {
            if (path != shard) return;
            std::ifstream input(inventory, std::ios::binary);
            const auto header = ReadIndexInventoryHeader(input, std::filesystem::file_size(inventory), geometry, info.store_id);
            if (header.compaction_pending == (fail_at == 2U))
                throw std::runtime_error("injected compaction directory sync failure");
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
void RepeatedCompactionRetainsCurrentState() {
    test::ScopedTempDir directory("chunkdb-repeated-tombstone-compaction");
    const auto config = Config(directory.path());
    std::uint64_t later = 0U;
    {
        TableCatalog catalog(config);
        const auto deleted = Populate(catalog);
        assert(catalog.CompactTombstones("t", deleted + 1U) == deleted);
        assert(catalog.CompactTombstones("t", deleted + 1U) == deleted);
        {
            auto lease = catalog.Require("t")->Acquire();
            assert(lease->store().GetBlockBits(2, 0) == "01000000");
            lease->store().UnsetBlock(2, 0);
            later = lease->store().GetChunkVersion(1, 0);
            lease->store().CheckpointForTests(1, 0);
        }
        assert(later > deleted);
        assert(catalog.CompactTombstones("t", later + 1U) == later);
        assert(catalog.CompactTombstones("t", later + 1U) == later);
    }
    TableCatalog reopened(config);
    auto lease = reopened.Require("t")->Acquire();
    assert(lease->store().RetainedDeletionFloor() == later && lease->store().RetainedTombstoneCount() == 0U);
    assert(!lease->store().ReadChunkState(0, 0).written && !lease->store().ReadChunkState(1, 0).written);
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
std::pair<std::uint64_t, std::uint64_t> PrepareRequiredResurrection(TableCatalog& catalog, bool slots) {
    const auto table = catalog.Create("t", [&] {
        return TableDefinition{.geometry = {2U, 2U, 2U, 2U, 8U}, .options = catalog.default_options(),
            .schema = SingleBitsColumnSchema(8U), .chunk_schema = TableSchema{
                .next_column_id = 2U, .columns = {{.id = 1U, .name = "owner", .type = {ColumnKind::kUnsigned, 8U},
                    .required = true, .default_value = {}}}}};
    });
    std::uint64_t deleted = 0U;
    {
        auto lease = table->Acquire();
        auto& store = lease->store();
        assert(store.TransformChunkState({0, 0}, {}, false, [&](ChunkState& state) {
            state.chunk_columns = std::make_shared<const ChunkColumnState>(AssignChunkColumns(
                store.geometry().chunk_schema(), nullptr, {{"owner", std::uint64_t{0U}}}, true));
        }).ok);
        deleted = store.GetChunkVersion(0, 0);
        store.CheckpointForTests(0, 0);
    }
    if (slots) (void)table->CreateFeedSlot("consumer");
    auto lease = table->Acquire();
    // The ordinary resurrection frame has no metadata record; its initialized
    // REQUIRED row comes only from the old retained empty boundary.
    lease->store().SetBlockBits(0, 0, "10000000");
    const auto produced = lease->store().GetChunkVersion(0, 0);
    assert(!lease->store().ReadChunkState(9, 9).written);
    return {deleted, produced};
}
void RequiredResurrectionSurvivesCompaction() {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) for (const bool slots : {false, true}) {
        test::ScopedTempDir directory("chunkdb-compaction-required-replay");
        auto config = Config(directory.path()); config.default_options.durability_mode = mode;
        config.slot_sync_interval = std::chrono::hours(1);
        std::uint64_t produced = 0U, deleted = 0U;
        {
            TableCatalog catalog(config);
            std::tie(deleted, produced) = PrepareRequiredResurrection(catalog, slots);
            assert(catalog.CompactTombstones("t", deleted + 1U) == deleted);
            auto lease = catalog.Require("t")->Acquire();
            assert(lease->store().GetBlockBits(0, 0) == "10000000");
            assert(lease->store().GetChunkVersion(0, 0) == produced);
            assert(lease->store().RetainedDeletionFloor() == deleted);
            assert(!lease->store().ReadChunkState(9, 9).written);
            assert(!std::filesystem::exists(ChunkDataPath(directory.path() / "tables/t", lease->store().geometry(), {9, 9})));
        }
        TableCatalog reopened(config);
        auto lease = reopened.Require("t")->Acquire();
        assert(lease->store().GetChunkVersion(0, 0) == produced && lease->store().GetBlockBits(0, 0) == "10000000");
        assert(lease->store().RetainedDeletionFloor() == deleted);
    }
}
std::string Quote(const std::string& text) {
#ifdef _WIN32
    return '"' + text + '"';
#else
    std::string result = "'";
    for (const char c : text) result += c == '\'' ? "'\\''" : std::string(1U, c);
    return result + "'";
#endif
}
void CrashRequiredResurrection(const char* executable) {
    if (!kFailpointsCompiled) return;
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) for (const bool slots : {false, true})
    for (const auto* suffix : {"AFTER_STAGE_DIRECTORY_SYNC", "AFTER_DECISION"}) {
        test::ScopedTempDir directory("chunkdb-compaction-required-crash");
        const auto point = "CHUNKDB_FAILPOINT_CRASH_INDEX_COMPACTION_" + std::string(suffix) + "_ONCE";
        const auto command = Quote(std::filesystem::absolute(executable).string()) + " --compaction-crash " +
            Quote(directory.path().string()) + " " + Quote(point) + " " +
            (mode == DurabilityMode::kRelaxed ? "relaxed" : "fsync") + (slots ? " slots" : " plain");
#ifdef _WIN32
        assert(std::system(('"' + command + '"').c_str()) == 86);
#else
        const auto result = std::system(command.c_str());
        assert(result != -1 && WIFEXITED(result) && WEXITSTATUS(result) == 86);
#endif
        std::uint64_t deleted = 0U, produced = 0U;
        std::ifstream expected(directory.path() / "expected"); expected >> deleted >> produced;
        assert(expected && deleted != 0U && produced > deleted);
        auto config = Config(directory.path() / "data"); config.default_options.durability_mode = mode;
        config.slot_sync_interval = std::chrono::hours(1);
        TableCatalog catalog(config);
        const auto table = catalog.Require("t");
        {
            auto lease = table->Acquire();
            assert(lease->store().GetChunkVersion(0, 0) == produced && lease->store().GetBlockBits(0, 0) == "10000000");
            const auto decided = std::string_view(suffix) == "AFTER_DECISION";
            assert(lease->store().RetainedDeletionFloor() == (decided ? deleted : 0U));
            assert(!lease->store().ReadChunkState(9, 9).written);
        }
        if (slots) {
            FeedSlotTestAccess::Sync(*table);
            auto archive = table->ReadFeedArchive({table->Info().store_id, deleted});
            const auto entry = archive.Next();
            assert(entry && entry->position.revision == produced && entry->blocks.size() == 1U && !archive.Next());
        }
    }
}
// Collection of an emptied chunk publishes its tombstone, then removes the
// image and WAL. A crash at each step keeps every acknowledged write and the
// emptied chunk's version; the old image never resurrects its data.
constexpr std::array<const char*, 3U> kCollectionPoints{
    "CHUNKDB_FAILPOINT_CRASH_INDEX_AFTER_LOG_SYNC_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_INDEX_AFTER_INVENTORY_PUBLISH_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_EMPTY_GC_AFTER_TOMBSTONE_ONCE"};
CatalogConfig CollectionConfig(const std::filesystem::path& root, std::string_view mode) {
    auto config = Config(root / "data");
    config.default_options.durability_mode = mode == "relaxed" ? DurabilityMode::kRelaxed : DurabilityMode::kFsyncWal;
    return config;
}
// Chunk (0,0) is emptied after a checkpoint gave it an image with data. With
// `append`, chunk (1,0) of the same large chunk already has a tombstone, so
// the target's record appends to an existing log instead of creating one.
void PrepareCollection(const std::filesystem::path& root, std::string_view mode, bool append, const char* point) {
    TableCatalog catalog(CollectionConfig(root, mode));
    GeometryConfig geometry; geometry.large_chunk_width_chunks = 2U; geometry.large_chunk_height_chunks = 2U;
    geometry.chunk_width_blocks = 2U; geometry.chunk_height_blocks = 2U; geometry.block_bits = 8U;
    auto lease = catalog.Create("t", geometry, catalog.default_options())->Acquire();
    auto& store = lease->store();
    store.SetBlockBits(8, 0, "00000011");
    const auto live = store.GetChunkVersion(4, 0);
    std::uint64_t prior = 0U;
    if (append) {
        store.SetBlockBits(2, 0, "10000000"); store.UnsetBlock(2, 0);
        prior = store.GetChunkVersion(1, 0); store.CheckpointForTests(1, 0);
        assert(store.RetainedTombstoneCount() == 1U);
    }
    store.SetBlockBits(0, 0, "10000000"); store.CheckpointForTests(0, 0);
    assert(std::filesystem::exists(ChunkDataPath(store.data_dir(), store.geometry(), {0, 0})));
    store.UnsetBlock(0, 0);
    const auto deleted = store.GetChunkVersion(0, 0);
    // Relaxed acknowledgements become durable at a barrier.
    if (mode == "relaxed") store.WalBarrier();
    { std::ofstream expected(root / "expected"); expected << live << ' ' << deleted << ' ' << prior; expected.close(); assert(expected); }
#ifdef _WIN32
    _putenv_s(point, "1");
#else
    setenv(point, "1", 1);
#endif
    store.CheckpointForTests(0, 0);
}
void RequireCollectionState(TableCatalog& catalog, std::uint64_t live, std::uint64_t deleted, std::uint64_t prior) {
    auto lease = catalog.Require("t")->Acquire();
    auto& store = lease->store();
    assert(store.GetBlockBits(8, 0) == "00000011" && store.GetChunkVersion(4, 0) == live);
    const auto emptied = store.ReadChunkState(0, 0);
    assert(emptied.written && emptied.version == deleted && emptied.commit_time_ms != 0U);
    assert(std::all_of(emptied.presence_bitmap.begin(), emptied.presence_bitmap.end(), [](std::uint8_t byte) { return byte == 0U; }));
    assert((store.ScanPopulatedChunks(false, {}, 10U).coords == std::vector<ChunkCoord>{{4, 0}}));
    const auto page = store.ScanChunkIdentities(false, {}, 10U, 0U, true);
    std::vector<std::pair<ChunkCoord, std::uint64_t>> identities;
    for (const auto& entry : page.entries) {
        assert(entry.data.has_value() == (entry.coord == ChunkCoord{4, 0}));
        identities.emplace_back(entry.coord, entry.version);
    }
    std::vector<std::pair<ChunkCoord, std::uint64_t>> expected{{{0, 0}, deleted}};
    if (prior != 0U) expected.push_back({{1, 0}, prior});
    expected.push_back({{4, 0}, live});
    assert(identities == expected);
}
void CrashCollection(const char* executable) {
    if (!kFailpointsCompiled) return;
    for (const auto* mode : {"relaxed", "fsync"}) for (const bool append : {false, true}) for (const auto* point : kCollectionPoints) {
        test::ScopedTempDir directory("chunkdb-collection-crash");
        const auto root = std::filesystem::canonical(directory.path());
        const auto command = Quote(std::filesystem::absolute(executable).string()) + " --collection-crash " +
            Quote(root.string()) + " " + Quote(point) + " " + mode + (append ? " append" : " create");
#ifdef _WIN32
        assert(std::system(('"' + command + '"').c_str()) == 86);
#else
        const auto result = std::system(command.c_str());
        assert(result != -1 && WIFEXITED(result) && WEXITSTATUS(result) == 86);
#endif
        std::uint64_t live = 0U, deleted = 0U, prior = 0U;
        std::ifstream expected(root / "expected"); expected >> live >> deleted >> prior;
        assert(expected && live != 0U && deleted > live && (prior != 0U) == append);
        const auto config = CollectionConfig(root, mode);
        const auto table = root / "data/tables/t";
        // A log appended past its inventory boundary is unpublished, and
        // recovery removes it; the WAL still holds the emptying commit.
        const bool published = std::string_view(point) != kCollectionPoints[0] || !append;
        {
            TableCatalog catalog(config);
            RequireCollectionState(catalog, live, deleted, prior);
            auto lease = catalog.Require("t")->Acquire();
            auto& store = lease->store();
            assert(store.RetainedTombstoneCount() == (published ? 1U : 0U) + (append ? 1U : 0U));
            store.CheckpointForTests(0, 0);
            assert(store.RetainedTombstoneCount() == 1U + (append ? 1U : 0U));
            assert(!std::filesystem::exists(ChunkDataPath(table, store.geometry(), {0, 0})));
            assert(!std::filesystem::exists(ChunkWalPath(table, store.geometry(), {0, 0})));
        }
        std::ostringstream findings;
        const auto verified = VerifyDataDirectory(root / "data", findings);
        if (verified.errors != 0U) std::cerr << findings.str();
        assert(verified.errors == 0U);
        TableCatalog reopened(config);
        RequireCollectionState(reopened, live, deleted, prior);
    }
}
}
// Compaction checkpoints every current chunk after its barrier. A background
// checkpoint that starts after the barrier visited a chunk may still be
// writing that chunk's files without its lock when compaction reaches it;
// compaction waits for it and keeps the write it carries.
void CompactionWaitsForBackgroundCheckpointFiles() {
#ifndef _WIN32
    // On Windows the barrier holds the publish lock through its syncs, so no
    // checkpoint publishes inside them.
    test::ScopedTempDir directory("chunkdb-table-tombstone-checkpoint-files");
    auto config = Config(directory.path());
    config.default_options.checkpoint_update_interval = 1U;
    std::uint64_t deleted = 0U;
    {
        TableCatalog catalog(config);
        deleted = Populate(catalog);
    }
    config.background_maintenance = true;
    {
        TableCatalog catalog(config);
        ChunkStore* store = nullptr;
        {
            auto lease = catalog.Require("t")->Acquire();
            store = &lease->store();
            store->ArmBarrierBeforeSyncPauseForTests();
        }
        auto compaction = std::async(std::launch::async, [&] { return catalog.CompactTombstones("t", deleted + 1U); });
        assert(store->WaitForBarrierBeforeSyncForTests());
        // Compaction is paused inside its own barrier, after it flushed every
        // chunk, so this write races nothing it has started.
        store->ArmCheckpointBeforeWalRemovalPauseForTests();
        store->SetBlockBits(2, 0, "00100000");
        assert(store->WaitForCheckpointBeforeWalRemovalForTests());
        store->ResumeBarrierBeforeSyncForTests();
        const bool waited = compaction.wait_for(std::chrono::milliseconds(200)) != std::future_status::ready;
        store->ResumeCheckpointBeforeWalRemovalForTests();
        assert(compaction.get() == deleted);
        assert(waited);
        auto lease = catalog.Require("t")->Acquire();
        assert(lease->store().GetBlockBits(2, 0) == "00100000");
        assert(lease->store().RetainedTombstoneCount() == 0U && lease->store().RetainedDeletionFloor() == deleted);
        lease->store().WalBarrier();
    }
    config.background_maintenance = false;
    TableCatalog reopened(config);
    auto lease = reopened.Require("t")->Acquire();
    assert(lease->store().GetBlockBits(2, 0) == "00100000");
    assert(!lease->store().ReadChunkState(0, 0).written);
    assert(lease->store().RetainedTombstoneCount() == 0U && lease->store().RetainedDeletionFloor() == deleted);
#endif
}
int main(int argc, char** argv) {
    if (argc > 1 && std::string_view(argv[1]) == "--repeat") { RepeatedCompactionRetainsCurrentState(); return 0; }
    if (argc == 6 && std::string_view(argv[1]) == "--compaction-crash") {
        const auto root = std::filesystem::path(argv[2]);
        auto config = Config(root / "data");
        config.default_options.durability_mode = std::string_view(argv[4]) == "relaxed" ? DurabilityMode::kRelaxed : DurabilityMode::kFsyncWal;
        config.slot_sync_interval = std::chrono::hours(1);
        TableCatalog catalog(config);
        const auto [deleted, produced] = PrepareRequiredResurrection(catalog, std::string_view(argv[5]) == "slots");
        { std::ofstream expected(root / "expected"); expected << deleted << ' ' << produced; expected.close(); assert(expected); }
#ifdef _WIN32
        _putenv_s(argv[3], "1");
#else
        setenv(argv[3], "1", 1);
#endif
        (void)catalog.CompactTombstones("t", deleted + 1U);
        return 1;
    }
    if (argc == 6 && std::string_view(argv[1]) == "--collection-crash") {
        PrepareCollection(argv[2], argv[4], std::string_view(argv[5]) == "append", argv[3]);
        return 1;
    }
    if (argc == 2 && std::string_view(argv[1]) == "--crash-children") {
        CrashRequiredResurrection(argv[0]); CrashCollection(argv[0]); return 0;
    }
    if (argc > 1 && std::string_view(argv[1]) == "--required-replay") { RequiredResurrectionSurvivesCompaction(); return 0; }
    if (argc == 1) { CompactionReopensAndReportsHistoryFloor(); CompactionFailureFencesUntilRecovery(); }
    ResumeAllocationFailureDoesNotEscapeCleanup();
    if (argc == 1) RequiredResurrectionSurvivesCompaction();
    if (argc == 1) RepeatedCompactionRetainsCurrentState();
    if (argc == 1) CompactionWaitsForBackgroundCheckpointFiles();
}
