#include <cassert>
#include <condition_variable>
#include <fstream>
#include <thread>

#include "feed_slots.hpp"
#include "chunk_store_internal.hpp"
#include "feed_slot_records.hpp"
#include "feed_test_utils.hpp"
#include "chunkdb/file_layout.hpp"
#include "wal_writer.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::feed_test;
using chunkdb::test::ScopedTempDir;

CatalogConfig SlotsConfig(const std::filesystem::path& path) {
    auto config = Config(path);
    config.slot_sync_interval = std::chrono::hours(1);
    config.default_options.wal_group_commit_updates = 1000U;
    return config;
}
std::size_t Archives(const std::filesystem::path& root) {
    const auto dir = root / "tables" / "default" / kFeedArchiveDirName;
    if (!std::filesystem::exists(dir)) return 0U;
    std::size_t count = 0U;
    for (const auto& entry : std::filesystem::directory_iterator(dir))
        if (entry.path().extension() == ".wal") ++count;
    return count;
}
void EqualEntry(const FeedEntry& expected, const FeedEntry& actual) {
    assert(expected.kind == actual.kind && expected.position == actual.position);
    assert(expected.commit_time_ms == actual.commit_time_ms && expected.schema_version == actual.schema_version);
    assert(expected.user == actual.user);
    EqualBlocks(expected.blocks, actual.blocks);
}
void AllMutationsAndRelease() {
    ScopedTempDir dir("chunkdb-feed-slots-mutations");
    TableCatalog catalog(SlotsConfig(dir.path()));
    auto table = catalog.Find("default");
    const auto slot = table->CreateFeedSlot("slow");
    const auto fast = table->CreateFeedSlot("fast");
    auto feed = table->SubscribeFeed();
    std::vector<std::shared_ptr<const FeedEntry>> expected;
    {
        auto lease = table->Acquire();
        auto& store = lease->store();
        for (std::uint32_t i = 0U; i < 16U; ++i) {
            ScopedWriteUser user("alice");
            const auto before = store.ReadChunkState(0, 0);
            const auto value = i + 1U;
            if (i % 8U == 0U) store.SetBlockBits(0, 0, Bits(value));
            else if (i % 8U == 1U) {
                auto state = before;
                txn_test::SetCounter(&state, value);
                (void)store.SetChunkStateBytes(0, 0, state.payload, state.presence_bitmap);
            } else if (i % 8U == 2U) {
                auto state = before;
                txn_test::SetCounter(&state, value);
                assert(store.WriteChunkState(0, 0, std::move(state), before.version).ok);
            } else if (i % 8U == 3U) {
                auto snapshot = store.BeginTxnSnapshot(txn_test::kTxnDuration);
                auto a = store.ReadChunkStateAt(*snapshot, 0, 0);
                auto b = store.ReadChunkStateAt(*snapshot, 1, 0);
                txn_test::SetCounter(&a, value);
                txn_test::SetCounter(&b, value + 100U);
                (void)store.CommitTransaction(*snapshot, {}, {{{0, 0}, std::move(a)}, {{1, 0}, std::move(b)}});
            } else if (i % 8U == 4U) {
                assert(store.ApplyChunkBatch(0, 0, true, before.version,
                    {{true, 0, 0, Bits(value)}, {true, 1, 0, Bits(value + 100U)}}).ok);
            } else if (i % 8U == 5U) {
                auto state = before;
                txn_test::SetCounter(&state, value);
                assert(store.CasChunkStateBytes(0, 0, before.version, state.payload, state.presence_bitmap).ok);
            } else if (i % 8U == 6U) store.SetBlock(0, 0, {{"bits", BitsValue{Bits(value)}}});
            else assert(store.UnsetBlock(0, 0, before.version).ok);
            expected.push_back(Next(*feed));
            assert(!store.CasChunkStateBytes(0, 0, before.version, before.payload, before.presence_bitmap).ok);
            if (i % 2U == 1U) {
                store.CheckpointForTests(0, 0);
                store.CheckpointForTests(1, 0);
            }
        }
        auto empty = store.ReadChunkState(0, 0);
        std::fill(empty.payload.begin(), empty.payload.end(), 0U);
        std::fill(empty.presence_bitmap.begin(), empty.presence_bitmap.end(), 0U);
        (void)store.SetChunkStateBytes(0, 0, empty.payload, empty.presence_bitmap);
        expected.push_back(Next(*feed));
        store.CheckpointForTests(0, 0);
    }
    assert(Archives(dir.path()) != 0U);
    FeedSlotTestAccess::Sync(*table);
    {
        auto reader = table->ReadFeedArchive(slot.position);
        for (const auto& change : expected) {
            auto entry = reader.Next();
            assert(entry);
            EqualEntry(*change, *entry);
        }
        assert(!reader.Next());
    }
    const auto frontier = table->ListFeedSlots().front().durable_watermark;
    const auto count = Archives(dir.path());
    table->AdvanceFeedSlot("fast", {fast.position.epoch, frontier});
    assert(Archives(dir.path()) == count);
    assert(txn_test::Throws([&] { table->AdvanceFeedSlot("slow", {slot.position.epoch, frontier + 1U}); }));
    auto foreign = slot.position;
    foreign.epoch[0] ^= 1U;
    assert(txn_test::Throws([&] { table->AdvanceFeedSlot("slow", foreign); }));
    table->AdvanceFeedSlot("slow", {slot.position.epoch, frontier});
    assert(Archives(dir.path()) == 0U);
    assert(txn_test::Throws([&] { (void)table->ReadFeedArchive(slot.position); }));
    assert(txn_test::Throws([&] { table->AdvanceFeedSlot("slow", slot.position); }));
    feed.reset();
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, Bits(999U));
    }
    FeedSlotTestAccess::Sync(*table);
    {
        auto reader = table->ReadFeedArchive({slot.position.epoch, frontier});
        const auto entry = reader.Next();
        assert(entry && !entry->user && entry->blocks.size() == 1U);
        assert(!reader.Next());
    }
    table->DropFeedSlot("slow");
    table->DropFeedSlot("fast");
    assert(table->ListFeedSlots().empty());
}

void RestartAndPins() {
    ScopedTempDir dir("chunkdb-feed-slots-restart");
    auto config = SlotsConfig(dir.path());
    FeedPosition start;
    std::uint64_t final = 0U;
    {
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        start = table->CreateFeedSlot("consumer").position;
        {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(0, 0, Bits(123U));
        }
        FeedSlotTestAccess::Sync(*table);
        auto reader = table->ReadFeedArchive(start);
        table->DropFeedSlot("consumer");
        {
            auto lease = table->Acquire();
            lease->store().CheckpointForTests(0, 0);
        }
        const auto entry = reader.Next();
        assert(entry && entry->blocks[0].after == std::vector<ColumnValue>{BitsValue{Bits(123U)}});
        assert(!reader.Next());
        const auto recreated = table->CreateFeedSlot("consumer");
        start = recreated.position;
        {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(0, 0, Bits(456U));
            lease->store().CheckpointForTests(0, 0);
        }
        FeedSlotTestAccess::Sync(*table);
        final = table->ListFeedSlots()[0].durable_watermark;
    }
    {
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        const auto slots = table->ListFeedSlots();
        assert(slots.size() == 1U && slots[0].position == start && slots[0].durable_watermark == final);
        auto reader = table->ReadFeedArchive(start);
        const auto entry = reader.Next();
        assert(entry && entry->blocks[0].before == std::vector<ColumnValue>{BitsValue{Bits(123U)}});
        assert(entry->blocks[0].after == std::vector<ColumnValue>{BitsValue{Bits(456U)}});
        assert(!reader.Next());
    }
    config.access_mode = AccessMode::kReadOnly;
    {
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        assert(table->ListFeedSlots()[0].position == start);
        assert(txn_test::Throws([&] { (void)table->CreateFeedSlot("other"); }));
        assert(txn_test::Throws([&] { table->DropFeedSlot("consumer"); }));
        assert(txn_test::Throws([&] { table->AdvanceFeedSlot("consumer", start); }));
        auto reader = table->ReadFeedArchive(start);
        assert(reader.Next() && !reader.Next());
    }
}

void ActivationBaseline() {
    ScopedTempDir dir("chunkdb-feed-slots-baseline");
    TableCatalog catalog(SlotsConfig(dir.path()));
    auto table = catalog.Find("default");
    {
        auto lease = table->Acquire();
        auto& store = lease->store();
        store.SetBlockBits(0, 0, Bits(1U));
        store.SetBlockBits(1, 0, Bits(2U));
        store.CheckpointForTests(0, 0);
        store.SetBlockBits(0, 0, Bits(3U));
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_CHECKPOINT_AFTER_IMAGE_REPLACE_ONCE", "1");
        assert(txn_test::Throws([&] { store.CheckpointForTests(0, 0); }));
    }
    const auto slot = table->CreateFeedSlot("consumer");
    auto feed = table->SubscribeFeed();
    {
        auto lease = table->Acquire();
        auto state = lease->store().ReadChunkState(0, 0);
        txn_test::SetCounter(&state, 4U);
        state.payload[4] = 5U;
        (void)lease->store().SetChunkStateBytes(0, 0, state.payload, state.presence_bitmap);
        lease->store().CheckpointForTests(0, 0);
    }
    const auto expected = Next(*feed);
    FeedSlotTestAccess::Sync(*table);
    auto reader = table->ReadFeedArchive(slot.position);
    const auto entry = reader.Next();
    assert(entry);
    EqualEntry(*expected, *entry);
    assert(!reader.Next());
}

void UnloadedActivation() {
    ScopedTempDir dir("chunkdb-feed-slots-unloaded");
    auto config = SlotsConfig(dir.path());
    {
        TableCatalog catalog(config);
        auto lease = catalog.Find("default")->Acquire();
        for (std::int64_t i = 0; i < 8; ++i) lease->store().SetBlockBits(i * 4, 0, Bits(static_cast<std::uint32_t>(i + 1)));
    }
    config.max_loaded_chunks = 1U;
    config.background_maintenance = false;
    TableCatalog catalog(config);
    auto table = catalog.Find("default");
    const auto slot = table->CreateFeedSlot("consumer");
    {
        auto lease = table->Acquire();
        for (std::int64_t i = 0; i < 8; ++i)
            assert(lease->store().GetBlock(4 * i, 0) == std::vector<ColumnValue>{BitsValue{Bits(static_cast<std::uint32_t>(i + 1))}});
    }
    assert(slot.position.revision != 0U && catalog.resources()->LoadedChunkCount() <= 1U);
}

void AliasRecovery() {
    ScopedTempDir dir("chunkdb-feed-slots-alias");
    const auto config = SlotsConfig(dir.path());
    FeedPosition start;
    std::filesystem::path archive;
    std::vector<std::uint8_t> original;
    {
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        start = table->CreateFeedSlot("consumer").position;
        {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(0, 0, Bits(1U));
            lease->store().CheckpointForTests(0, 0);
        }
        FeedSlotTestAccess::Sync(*table);
        const auto root = dir.path() / "tables" / "default";
        for (const auto& file : std::filesystem::directory_iterator(root / kFeedArchiveDirName))
            if (file.path().extension() == ".wal") archive = file.path();
        original = LoadFile(archive);
        auto lease = table->Acquire();
        std::filesystem::create_hard_link(archive, ChunkWalPath(root, lease->store().geometry(), {0, 0}));
    }
    {
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        {
            auto lease = table->Acquire();
            const auto live = ChunkWalPath(dir.path() / "tables" / "default", lease->store().geometry(), {0, 0});
            assert(!std::filesystem::exists(live));
            lease->store().SetBlockBits(0, 0, Bits(2U));
            lease->store().CheckpointForTests(0, 0);
        }
        assert(LoadFile(archive) == original);
        FeedSlotTestAccess::Sync(*table);
        auto reader = table->ReadFeedArchive(start);
        assert(reader.Next()->blocks[0].after == std::vector<ColumnValue>{BitsValue{Bits(1U)}});
        assert(reader.Next()->blocks[0].after == std::vector<ColumnValue>{BitsValue{Bits(2U)}});
        assert(!reader.Next());
    }
}

void PinnedAcrossReopen() {
    ScopedTempDir dir("chunkdb-feed-slots-reopen-pin");
    TableCatalog catalog(SlotsConfig(dir.path()));
    auto table = catalog.Find("default");
    const auto start = table->CreateFeedSlot("consumer").position;
    { auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, Bits(1U)); }
    FeedSlotTestAccess::Sync(*table);
    auto old_reader = table->ReadFeedArchive(start);
    catalog.ChangeColumns("default", [](const auto& schema) { return RenameColumn(schema, "bits", "renamed"); });
    TableOptionsUpdate update;
    update.wal_group_commit_updates = 2000U;
    catalog.SetOptions("default", update);
    {
        auto lease = table->Acquire();
        lease->store().SetBlock(0, 0, {{"renamed", BitsValue{Bits(2U)}}});
        lease->store().CheckpointForTests(0, 0);
    }
    const auto old = old_reader.Next();
    assert(old && old->schema_version == 1U && old->blocks[0].after == std::vector<ColumnValue>{BitsValue{Bits(1U)}});
    assert(!old_reader.Next());
    FeedSlotTestAccess::Sync(*table);
    auto new_reader = table->ReadFeedArchive(start);
    assert(new_reader.Next()->schema_version == 1U);
    const auto newer = new_reader.Next();
    assert(newer && newer->schema_version == 2U);
    assert(newer->blocks[0].before == std::vector<ColumnValue>{BitsValue{Bits(1U)}});
    assert(newer->blocks[0].after == std::vector<ColumnValue>{BitsValue{Bits(2U)}});
    assert(!new_reader.Next());
}

void FinalPinReleasesHistory() {
    ScopedTempDir dir("chunkdb-feed-slots-final-pin");
    auto config = SlotsConfig(dir.path());
    config.slot_sync_interval = std::chrono::milliseconds(10);
    TableCatalog catalog(config);
    auto table = catalog.Find("default");
    const auto start = table->CreateFeedSlot("consumer").position;
    { auto lease = table->Acquire();
      lease->store().SetBlockBits(0, 0, Bits(1U));
      lease->store().CheckpointForTests(0, 0); }
    FeedSlotTestAccess::Sync(*table);
    struct Retained : FeedSlotTestHook {
        std::mutex mutex;
        std::condition_variable cv;
        bool done = false;
        void Run(Point point, std::uint64_t) override {
            if (point != Point::kAfterRetention) return;
            std::lock_guard lock(mutex);
            done = true;
            cv.notify_all();
        }
    } hook;
    {
        auto reader = table->ReadFeedArchive(start);
        table->DropFeedSlot("consumer");
        FeedSlotTestAccess::SetHook(*table, &hook);
        assert(Archives(dir.path()) != 0U);
    }
    {
        std::unique_lock lock(hook.mutex);
        assert(hook.cv.wait_for(lock, 10s, [&] { return hook.done; }));
    }
    FeedSlotTestAccess::SetHook(*table, nullptr);
    assert(Archives(dir.path()) == 0U);
}

void LostLastSlotRequiresFreshWatch() {
    ScopedTempDir dir("chunkdb-feed-slots-lost-watch");
    auto config = SlotsConfig(dir.path());
    config.slot_sync_interval = std::chrono::milliseconds(10);
    config.slot_max_bytes = 1U;
    TableCatalog catalog(config);
    auto table = catalog.Find("default");
    (void)table->CreateFeedSlot("consumer");
    auto feed = table->SubscribeFeed();
    { auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, Bits(1U)); }
    const auto captured = Next(*feed)->position;
    feed.reset();
    struct Disabled : FeedSlotTestHook {
        std::mutex mutex;
        std::condition_variable cv;
        bool done = false;
        void Run(Point point, std::uint64_t) override {
            if (point != Point::kFeedDisabled) return;
            std::lock_guard lock(mutex);
            done = true;
            cv.notify_all();
        }
    } hook;
    FeedSlotTestAccess::SetHook(*table, &hook);
    { auto lease = table->Acquire(); lease->store().CheckpointForTests(0, 0); }
    {
        std::unique_lock lock(hook.mutex);
        assert(hook.cv.wait_for(lock, 10s, [&] { return hook.done; }));
    }
    assert(table->ListFeedSlots().empty());
    { auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, Bits(2U)); }
    FeedOptions options;
    options.after = captured;
    auto resumed = table->SubscribeFeed(options);
    assert(Next(*resumed)->kind == FeedEntry::Kind::kResync);
    FeedSlotTestAccess::SetHook(*table, nullptr);
}

void InterruptedReleaseResumesAtOpen() {
    ScopedTempDir dir("chunkdb-feed-slots-release-restart");
    const auto config = SlotsConfig(dir.path());
    std::filesystem::path base;
    {
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        { auto lease = table->Acquire();
          lease->store().SetBlockBits(0, 0, Bits(1U));
          lease->store().CheckpointForTests(0, 0); }
        (void)table->CreateFeedSlot("consumer");
        { auto lease = table->Acquire();
          lease->store().SetBlockBits(0, 0, Bits(2U));
          lease->store().CheckpointForTests(0, 0); }
        FeedSlotTestAccess::Sync(*table);
        const auto root = dir.path() / "tables" / "default";
        auto records = *ReadFeedSlotRecords(root, table->Info().store_id);
        records.slots[0].written = records.durable_watermark;
        WriteFeedSlotRecords(root, records);
        // A crash between WAL deletion and base deletion: its release decision
        // is already durable, but only the first namespace change survived.
        for (const auto& entry : std::filesystem::directory_iterator(root / kFeedArchiveDirName)) {
            if (entry.path().extension() == ".wal") std::filesystem::remove(entry.path());
            else if (entry.path().extension() == ".chk") base = entry.path();
        }
        assert(!base.empty() && std::filesystem::exists(base));
    }
    {
        TableCatalog catalog(config);
        assert(!std::filesystem::exists(base));
    }
    {
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        { auto lease = table->Acquire();
          lease->store().SetBlockBits(0, 0, Bits(3U));
          lease->store().CheckpointForTests(0, 0); }
        FeedSlotTestAccess::Sync(*table);
        assert(Archives(dir.path()) != 0U);
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
        assert(txn_test::Throws([&] { table->DropFeedSlot("consumer"); }));
        assert(Archives(dir.path()) != 0U);
    }
    {
        TableCatalog catalog(config);
        assert(catalog.Find("default")->ListFeedSlots().empty());
        assert(Archives(dir.path()) == 0U);
    }
}

void StartupCollisionAndTornFirstFrame() {
    for (unsigned shape = 0U; shape < 4U; ++shape) {
        ScopedTempDir dir("chunkdb-feed-slots-startup-collision");
        const auto config = SlotsConfig(dir.path());
        const auto root = dir.path() / "tables" / "default";
        FeedPosition start;
        std::filesystem::path archive, live;
        FeatureFlags features;
        {
            TableCatalog catalog(config);
            auto table = catalog.Find("default");
            start = table->CreateFeedSlot("consumer").position;
            auto lease = table->Acquire();
            auto& store = lease->store();
            store.SetBlockBits(0, 0, Bits(1U));
            store.CheckpointForTests(0, 0);
            live = ChunkWalPath(root, store.geometry(), {0, 0});
            features = store.features();
            for (const auto& entry : std::filesystem::directory_iterator(root / kFeedArchiveDirName))
                if (entry.path().extension() == ".wal") archive = entry.path();
        }
        assert(!archive.empty());
        const auto original = LoadFile(archive);
        if (shape == 0U) {
            std::filesystem::copy_file(archive, live);
            assert(!std::filesystem::equivalent(archive, live));
            assert(txn_test::Throws([&] { TableCatalog refused(config); }));
            assert(LoadFile(live) == original && LoadFile(archive) == original);
            continue;
        }
        auto bytes = BuildWalHeader({0, 0}, start.epoch, features);
        if (shape == 1U) bytes.resize(bytes.size() + kWalFrameFixedHeaderSize, 0U);
        else {
            std::vector<std::uint8_t> frame;
            WalFrameBuilder builder(&frame, 1U);
            const std::vector<std::uint8_t> payload{2U, 0U, 0U, 0U};
            builder.AppendSpan(0U, payload.data(), payload.size());
            (void)builder.Finish(999U, 1000U);
            frame.resize(shape == 2U ? kWalFrameFixedHeaderSize - 1U : kWalFrameFixedHeaderSize + kWalFrameHeaderCrcSize);
            bytes.insert(bytes.end(), frame.begin(), frame.end());
        }
        {
            std::ofstream file(live, std::ios::binary);
            file.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
            assert(file.good());
        }
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        {
            auto lease = table->Acquire();
            assert(lease->store().GetBlock(0, 0) == std::vector<ColumnValue>{BitsValue{Bits(1U)}});
            lease->store().SetBlockBits(0, 0, Bits(2U));
            lease->store().CheckpointForTests(0, 0);
        }
        assert(LoadFile(archive) == original);
        FeedSlotTestAccess::Sync(*table);
        auto reader = table->ReadFeedArchive(start);
        assert(reader.Next()->blocks[0].after == std::vector<ColumnValue>{BitsValue{Bits(1U)}});
        assert(reader.Next()->blocks[0].after == std::vector<ColumnValue>{BitsValue{Bits(2U)}});
        assert(!reader.Next());
    }
}
}  // namespace

int main() {
    AllMutationsAndRelease();
    RestartAndPins();
    ActivationBaseline();
    UnloadedActivation();
    AliasRecovery();
    PinnedAcrossReopen();
    FinalPinReleasesHistory();
    LostLastSlotRequiresFreshWatch();
    InterruptedReleaseResumesAtOpen();
    StartupCollisionAndTornFirstFrame();
}
