#include <cassert>
#include <condition_variable>
#include <fstream>
#include <mutex>
#include <optional>
#include <thread>

#include "feed_slots.hpp"
#include "chunk_store_internal.hpp"
#include "feed_test_utils.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::feed_test;
using chunkdb::test::ScopedTempDir;

CatalogConfig Configuration(const std::filesystem::path& path) {
    auto config = Config(path);
    config.slot_sync_interval = std::chrono::hours(1);
    config.default_options.wal_group_commit_updates = 1000U;
    return config;
}
std::filesystem::path TablePath(const std::filesystem::path& root) { return root / "tables" / "default"; }
std::size_t Archives(const std::filesystem::path& root) {
    const auto path = TablePath(root) / kFeedArchiveDirName;
    if (!std::filesystem::exists(path)) return 0U;
    std::size_t result = 0U;
    for (const auto& file : std::filesystem::directory_iterator(path))
        if (file.path().extension() == ".wal") ++result;
    return result;
}
class SyncPause : public FeedSlotTestHook {
  public:
    explicit SyncPause(Point point) : point_(point) {}
    void Run(Point point, std::uint64_t captured) override {
        if (point != point_) return;
        std::unique_lock lock(mutex_);
        if (entered_) return;
        entered_ = true;
        captured_ = captured;
        cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
    }
    std::uint64_t Wait() {
        std::unique_lock lock(mutex_);
        assert(cv_.wait_for(lock, 10s, [&] { return entered_; }));
        return captured_;
    }
    void Release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }
  private:
    Point point_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_ = false, released_ = false;
    std::uint64_t captured_ = 0U;
};

void UnfinishedWriterHoldsDurableFrontier() {
    ScopedTempDir directory("chunkdb-feed-slots-pending-writer");
    TableCatalog catalog(Configuration(directory.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    const auto start = table->CreateFeedSlot("consumer").position;
    { auto lease = table->Acquire();
      (void)lease->store().ReadChunkState(0, 0);
      (void)lease->store().ReadChunkState(1, 0); }
    Pause writer_pause(FeedTestHook::Point::kAfterVersion);
    FeedTestAccess::SetHook(*table, &writer_pause);
    std::thread low([&] { auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, Bits(1U)); });
    const auto reserved = writer_pause.Wait();
    std::uint64_t high_revision;
    { auto lease = table->Acquire();
      lease->store().SetBlockBits(4, 0, Bits(2U));
      high_revision = lease->store().GetChunkVersion(1, 0); }
    assert(high_revision > reserved);
    SyncPause sync_pause(FeedSlotTestHook::Point::kBeforeFlush);
    FeedSlotTestAccess::SetHook(*table, &sync_pause);
    std::thread sync([&] { FeedSlotTestAccess::Sync(*table); });
    const auto captured = sync_pause.Wait();
    assert(captured < reserved);
    writer_pause.Release();
    low.join();
    sync_pause.Release();
    sync.join();
    FeedTestAccess::SetHook(*table, nullptr);
    FeedSlotTestAccess::SetHook(*table, nullptr);
    const auto records = ReadFeedSlotRecords(TablePath(directory.path()), start.epoch);
    assert(records->durable_watermark == captured && records->durable_watermark < high_revision);
    { auto reader = table->ReadFeedArchive(start); assert(!reader.Next()); }
    FeedSlotTestAccess::Sync(*table);
    assert(table->ListFeedSlots()[0].durable_watermark >= high_revision);
    auto reader = table->ReadFeedArchive(start);
    assert(reader.Next()->position.revision == reserved);
    assert(reader.Next()->position.revision == high_revision);
    assert(!reader.Next());
}

void WritesDuringSyncWaitForNextPass() {
    ScopedTempDir directory("chunkdb-feed-slots-captured-frontier");
    TableCatalog catalog(Configuration(directory.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    const auto start = table->CreateFeedSlot("consumer").position;
    std::uint64_t first;
    { auto lease = table->Acquire();
      (void)lease->store().ReadChunkState(1, 0);
      lease->store().SetBlockBits(0, 0, Bits(1U));
      first = lease->store().GetChunkVersion(0, 0); }
    SyncPause pause(FeedSlotTestHook::Point::kBeforeSync);
    FeedSlotTestAccess::SetHook(*table, &pause);
    std::thread sync([&] { FeedSlotTestAccess::Sync(*table); });
    const auto captured = pause.Wait();
    assert(captured >= first);
    std::uint64_t later;
    { auto lease = table->Acquire();
      lease->store().SetBlockBits(4, 0, Bits(2U));
      later = lease->store().GetChunkVersion(1, 0); }
    assert(later > captured);
    pause.Release();
    sync.join();
    FeedSlotTestAccess::SetHook(*table, nullptr);
    assert(ReadFeedSlotRecords(TablePath(directory.path()), start.epoch)->durable_watermark == captured);
    { auto reader = table->ReadFeedArchive(start);
      assert(reader.Next()->position.revision == first && !reader.Next()); }
    FeedSlotTestAccess::Sync(*table);
    assert(table->ListFeedSlots()[0].durable_watermark >= later);
    auto reader = table->ReadFeedArchive({start.epoch, first});
    assert(reader.Next()->position.revision == later && !reader.Next());
}

void AmbiguousAcknowledgementPreservesArchives() {
    ScopedTempDir directory("chunkdb-feed-slots-ack-failure");
    const auto config = Configuration(directory.path());
    FeedPosition start;
    std::uint64_t frontier;
    {
        TableCatalog catalog(config);
        (void)feed_test::CreateDefault(catalog);
        auto table = catalog.Find("default");
        start = table->CreateFeedSlot("consumer").position;
        { auto lease = table->Acquire();
          lease->store().SetBlockBits(0, 0, Bits(1U));
          lease->store().CheckpointForTests(0, 0); }
        FeedSlotTestAccess::Sync(*table);
        frontier = table->ListFeedSlots()[0].durable_watermark;
        const auto count = Archives(directory.path());
        assert(count > 0U);
        { txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
          assert(txn_test::Throws([&] { table->AdvanceFeedSlot("consumer", {start.epoch, frontier}); })); }
        assert(table->ListFeedSlots()[0].position == start);
        assert(Archives(directory.path()) == count);
        assert(txn_test::Throws([&] { FeedSlotTestAccess::Retain(*table); }));
        assert(txn_test::Throws([&] { FeedSlotTestAccess::Sync(*table); }));
        { auto lease = table->Acquire();
          assert(txn_test::Throws([&] { lease->store().SetBlockBits(0, 0, Bits(2U)); })); }
    }
    TableCatalog reopened(config);
    auto table = reopened.Find("default");
    assert(table->ListFeedSlots()[0].position.revision == frontier);
    FeedSlotTestAccess::Retain(*table);
    assert(Archives(directory.path()) == 0U);
}

void LimitLossKeepsFastSlotAndSurvivesRestart() {
    ScopedTempDir directory("chunkdb-feed-slots-limit");
    auto config = Configuration(directory.path());
    config.slot_max_bytes = 1U;
    FeedPosition start;
    {
        TableCatalog catalog(config);
        (void)feed_test::CreateDefault(catalog);
        auto table = catalog.Find("default");
        start = table->CreateFeedSlot("slow").position;
        (void)table->CreateFeedSlot("fast");
        { auto lease = table->Acquire();
          lease->store().SetBlockBits(0, 0, Bits(1U));
          lease->store().CheckpointForTests(0, 0); }
        FeedSlotTestAccess::Sync(*table);
        const auto frontier = table->ListFeedSlots()[0].durable_watermark;
        auto reader = table->ReadFeedArchive(start);
        std::string log;
        SetLogSinkForTests([&](const std::string& line) { log += line; });
        table->AdvanceFeedSlot("fast", {start.epoch, frontier});
        ResetLogSinkForTests();
        const auto slots = table->ListFeedSlots();
        assert(slots.size() == 1U && slots[0].name == "fast" && slots[0].position.revision == frontier);
        assert(log.find("feed slot lost") != std::string::npos && log.find("slow") != std::string::npos);
        const auto records = ReadFeedSlotRecords(TablePath(directory.path()), start.epoch);
        assert(records->slots.size() == 2U && records->slots[0].lost && !records->slots[1].lost);
        bool lost = false;
        try { table->AdvanceFeedSlot("slow", {start.epoch, frontier}); }
        catch (const FeedSlotLostError&) { lost = true; }
        assert(lost && Archives(directory.path()) > 0U);
        assert(reader.Next() && !reader.Next());
    }
    TableCatalog reopened(config);
    auto table = reopened.Find("default");
    assert(table->ListFeedSlots().size() == 1U);
    bool lost = false;
    try { table->AdvanceFeedSlot("slow", start); }
    catch (const FeedSlotLostError&) { lost = true; }
    assert(lost);
    FeedSlotTestAccess::Retain(*table);
    assert(Archives(directory.path()) == 0U);
}

void SlotRecordSyncFailureFreezesFrontier() {
    ScopedTempDir directory("chunkdb-feed-slots-record-sync-failure");
    TableCatalog catalog(Configuration(directory.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    const auto start = table->CreateFeedSlot("consumer").position;
    { auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, Bits(1U)); }
    const auto before = LoadFile(TablePath(directory.path()) / kFeedSlotsFileName);
    struct FailRecordSync : FeedSlotTestHook {
        std::optional<txn_test::ScopedEnv> failure;
        void Run(Point point, std::uint64_t) override {
            if (point == Point::kBeforePersist)
                failure.emplace("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_SYNC_FAIL_ONCE", "1");
        }
    } hook;
    FeedSlotTestAccess::SetHook(*table, &hook);
    assert(txn_test::Throws([&] { FeedSlotTestAccess::Sync(*table); }));
    FeedSlotTestAccess::SetHook(*table, nullptr);
    hook.failure.reset();
    assert(LoadFile(TablePath(directory.path()) / kFeedSlotsFileName) == before);
    assert(table->ListFeedSlots()[0].durable_watermark == start.revision);
    assert(txn_test::Throws([&] { FeedSlotTestAccess::Sync(*table); }));
    assert(txn_test::Throws([&] { FeedSlotTestAccess::Retain(*table); }));
    { auto lease = table->Acquire();
      assert(txn_test::Throws([&] { lease->store().SetBlockBits(0, 0, Bits(2U)); })); }
}

void SlotRecordsStartupAndTemporaryCleanup() {
    ScopedTempDir directory("chunkdb-feed-slots-startup-records");
    auto config = Configuration(directory.path());
    FeedPosition start;
    { TableCatalog catalog(config); (void)CreateDefault(catalog); start = catalog.Find("default")->CreateFeedSlot("consumer").position; }
    const auto record = TablePath(directory.path()) / kFeedSlotsFileName;
    const auto temporary = TablePath(directory.path()) / "chunkdb.slots.tmp.interrupted";
    const auto save = [](const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        assert(out.good());
    };
    const auto valid = LoadFile(record);
    save(temporary, {1U, 2U});
    config.access_mode = AccessMode::kReadOnly;
    { TableCatalog catalog(config);
      assert(catalog.Find("default")->ListFeedSlots()[0].position == start);
      assert(std::filesystem::exists(temporary) && LoadFile(temporary) == std::vector<std::uint8_t>({1U, 2U})); }
    config.access_mode = AccessMode::kReadWrite;
    { TableCatalog catalog(config); assert(!std::filesystem::exists(temporary)); }
    auto corrupt = valid;
    corrupt.back() ^= 1U;
    save(record, corrupt);
    assert(txn_test::Throws([&] { TableCatalog catalog(config); }));
    assert(LoadFile(record) == corrupt);
    config.access_mode = AccessMode::kReadOnly;
    assert(txn_test::Throws([&] { TableCatalog catalog(config); }));
    assert(LoadFile(record) == corrupt);
    save(record, valid);
    config.access_mode = AccessMode::kReadWrite;
    config.allow_multiple_processes = true;
    assert(txn_test::Throws([&] { TableCatalog catalog(config); }));
}

#if defined(__APPLE__)
void SyncFailureFreezesFrontier() {
    ScopedTempDir directory("chunkdb-feed-slots-sync-failure");
    TableCatalog catalog(Configuration(directory.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    const auto start = table->CreateFeedSlot("consumer").position;
    { auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, Bits(1U)); }
    const auto before = LoadFile(TablePath(directory.path()) / kFeedSlotsFileName);
    { txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_FULL_SYNC_FILE_FAIL_ONCE", "1");
      assert(txn_test::Throws([&] { FeedSlotTestAccess::Sync(*table); })); }
    assert(LoadFile(TablePath(directory.path()) / kFeedSlotsFileName) == before);
    assert(table->ListFeedSlots()[0].durable_watermark == start.revision);
    assert(txn_test::Throws([&] { FeedSlotTestAccess::Sync(*table); }));
    assert(txn_test::Throws([&] { table->AdvanceFeedSlot("consumer", start); }));
    { auto lease = table->Acquire();
      assert(txn_test::Throws([&] { lease->store().SetBlockBits(0, 0, Bits(2U)); })); }
}
#endif
}  // namespace

int main() {
    UnfinishedWriterHoldsDurableFrontier();
    WritesDuringSyncWaitForNextPass();
    AmbiguousAcknowledgementPreservesArchives();
    LimitLossKeepsFastSlotAndSurvivesRestart();
    SlotRecordSyncFailureFreezesFrontier();
    SlotRecordsStartupAndTemporaryCleanup();
#if defined(__APPLE__)
    SyncFailureFreezesFrontier();
#endif
}
