#include <algorithm>
#include <cassert>
#include <condition_variable>
#include <fstream>
#include <future>
#include <ios>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>
#include <utility>

#include "feed_slots.hpp"
#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "feed_test_utils.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::feed_test;
using chunkdb::test::ScopedTempDir;

class DiagnosticSink {
  public:
    explicit DiagnosticSink(LogSink sink) { SetLogSinkForTests(std::move(sink)); }
    ~DiagnosticSink() { ResetLogSinkForTests(); }
    DiagnosticSink(const DiagnosticSink&) = delete;
    DiagnosticSink& operator=(const DiagnosticSink&) = delete;
};

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

class SlotDiagnosticClock : public FeedSlotTestHook {
  public:
    void Run(Point point, std::uint64_t) override {
        if (point == Point::kBeforeFlush || point == Point::kBeforeSync) time_ += 300ms;
        if (point == Point::kBeforePersist) time_ += persist;
    }
    std::chrono::steady_clock::time_point SyncTime(std::chrono::steady_clock::time_point) const noexcept override {
        return time_;
    }
    std::chrono::milliseconds persist{400};
  private:
    std::chrono::steady_clock::time_point time_{};
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

// Records every file and directory synced while it exists.
class SyncLog {
  public:
    SyncLog() {
        SetFileSyncObserverForTests([this](const std::filesystem::path& path) {
            std::lock_guard lock(mutex_);
            paths_.push_back(path);
            ++files_;
        });
        SetDirectorySyncObserverForTests([this](const std::filesystem::path& path) { Add(path); });
    }
    ~SyncLog() {
        SetFileSyncObserverForTests({});
        SetDirectorySyncObserverForTests({});
    }
    SyncLog(const SyncLog&) = delete;
    SyncLog& operator=(const SyncLog&) = delete;
    [[nodiscard]] bool Contains(const std::filesystem::path& path) const {
        const auto wanted = std::filesystem::weakly_canonical(path);
        std::lock_guard lock(mutex_);
        return std::any_of(paths_.begin(), paths_.end(),
            [&](const auto& synced) { return std::filesystem::weakly_canonical(synced) == wanted; });
    }
    [[nodiscard]] std::size_t FileCount() const {
        std::lock_guard lock(mutex_);
        return files_;
    }
  private:
    void Add(const std::filesystem::path& path) { std::lock_guard lock(mutex_); paths_.push_back(path); }
    mutable std::mutex mutex_;
    std::vector<std::filesystem::path> paths_;
    std::size_t files_ = 0U;
};
// Every large-chunk directory and WAL of the table, and the archive directory.
std::vector<std::filesystem::path> StoreArtifacts(const std::filesystem::path& root) {
    std::vector<std::filesystem::path> result{TablePath(root) / kFeedArchiveDirName};
    assert(std::filesystem::is_directory(result.front()));
    std::size_t wals = 0U;
    for (const auto& directory : std::filesystem::directory_iterator(TablePath(root))) {
        if (!directory.is_directory() || !directory.path().filename().string().starts_with("L_")) continue;
        result.push_back(directory.path());
        for (const auto& file : std::filesystem::directory_iterator(directory.path()))
            if (file.path().extension() == ".wal") { result.push_back(file.path()); ++wals; }
    }
    assert(result.size() >= 4U && wals >= 2U);
    return result;
}
void SetBlocks(Table& table, std::initializer_list<std::pair<std::int64_t, std::int64_t>> blocks, std::uint32_t value) {
    auto lease = table.Acquire();
    for (const auto& [x, y] : blocks) lease->store().SetBlockBits(x, y, Bits(value));
}
std::filesystem::path WalOf(Table& table, const std::filesystem::path& root, std::int64_t x, std::int64_t y) {
    auto lease = table.Acquire();
    const auto& geometry = lease->store().geometry();
    return ChunkWalPath(TablePath(root), geometry, geometry.BlockToChunk(x, y));
}

// A store's first pass syncs every WAL, large-chunk directory and the archive
// directory: files written before this process opened the table included.
void FirstPassSyncsEveryFile() {
    ScopedTempDir directory("chunkdb-feed-slots-first-pass");
    {
        TableCatalog catalog(Configuration(directory.path()));
        (void)feed_test::CreateDefault(catalog);
        auto table = catalog.Find("default");
        (void)table->CreateFeedSlot("consumer");
        SetBlocks(*table, {{0, 0}, {16, 0}}, 1U);
        { auto lease = table->Acquire(); lease->store().CheckpointForTests(0, 0); }
        SetBlocks(*table, {{0, 0}, {40, 40}}, 2U);
    }
    SlotDiagnosticClock clock;
    TableCatalog catalog(Configuration(directory.path()));
    auto table = catalog.Find("default");
    FeedSlotTestAccess::SetHook(*table, &clock);
    const SyncLog synced;
    std::string warning;
    const auto caller = std::this_thread::get_id();
    const DiagnosticSink sink([&](const std::string& line) {
        if (std::this_thread::get_id() == caller && line.find("feed slot pass was slow") != std::string::npos) warning = line;
    });
    FeedSlotTestAccess::Sync(*table);
    const auto artifacts = StoreArtifacts(directory.path());
    for (const auto& path : artifacts) assert(synced.Contains(path));
    const auto files = [](const std::string& line) {
        const auto at = line.find(" files=");
        assert(at != std::string::npos);
        return std::stoull(line.substr(at + 7U));
    };
    assert(warning.find("first_pass=true") != std::string::npos);
    const auto wals = std::count_if(artifacts.begin(), artifacts.end(), [](const auto& path) { return path.extension() == ".wal"; });
    assert(files(warning) >= static_cast<std::size_t>(wals));

    // No frontier advance or pending writes: only the overflow sweep syncs files.
    { auto lease = table->Acquire(); lease->store().ForceUnsyncedOverflowForTests(); }
    const auto before = synced.FileCount();
    warning.clear();
    FeedSlotTestAccess::Sync(*table);
    assert(warning.find("first_pass=false") != std::string::npos && warning.find("whole_directory=true") != std::string::npos);
    assert(synced.FileCount() > before && files(warning) == synced.FileCount() - before);
    FeedSlotTestAccess::SetHook(*table, nullptr);
}

void SlowSlotPassWarnsAfterPublishing() {
    ScopedTempDir directory("chunkdb-feed-slots-warning");
    SlotDiagnosticClock clock;
    TableCatalog catalog(Configuration(directory.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    (void)table->CreateFeedSlot("consumer");
    bool notified = false;
    const auto caller = std::this_thread::get_id();
    auto witness = table->SubscribeFeed({.notify = [&] {
        if (std::this_thread::get_id() == caller) notified = true;
    }});
    FeedSlotTestAccess::SetHook(*table, &clock);
    std::string warning;
    std::future<std::uint64_t> durable;
    bool notified_before_warning = false, could_take_locks = false;
    const DiagnosticSink sink([&](const std::string& line) {
        if (std::this_thread::get_id() != caller || line.find("feed slot pass was slow") == std::string::npos) return;
        warning = line;
        notified_before_warning = notified;
        durable = std::async(std::launch::async, [&] {
            const auto watermark = table->ListFeedSlots().front().durable_watermark;
            auto lease = table->Acquire();
            lease->store().WalBarrier();
            return watermark;
        });
        could_take_locks = durable.wait_for(10s) == std::future_status::ready;
        throw std::ios_base::failure("injected slot diagnostic output failure");
    });
    SetBlocks(*table, {{0, 0}}, 1U);
    clock.persist = 399ms;
    FeedSlotTestAccess::Sync(*table);
    assert(warning.empty());

    SetBlocks(*table, {{0, 0}}, 2U);
    std::uint64_t revision;
    { auto lease = table->Acquire(); revision = lease->store().GetChunkVersion(0, 0); }
    clock.persist = 400ms;
    notified = false;
    FeedSlotTestAccess::Sync(*table);
    FeedSlotTestAccess::SetHook(*table, nullptr);
    assert(warning.find(" WARN store ") != std::string::npos);
    assert(warning.find("total_ms=1000 flush_ms=300 sync_ms=300 persist_ms=400") != std::string::npos);
    assert(warning.find(" chunks=1 ") != std::string::npos);
    assert(notified_before_warning && could_take_locks && durable.get() == revision);
    SetBlocks(*table, {{0, 0}}, 3U); // The diagnostic failure did not fence the store.
}

void SlowSlotWorkerWarnsAfterRetention() {
    struct SlowAck : FeedSlotTestHook {
        void Run(Point point, std::uint64_t) override {
            if (point == Point::kAfterAckPersist) persisted = true;
            if (point == Point::kAfterRetention) retained = std::exchange(persisted, false);
        }
        std::chrono::steady_clock::duration AckRetentionElapsed(std::chrono::steady_clock::duration) noexcept override {
            return std::exchange(retained, false) ? 1s : 0s;
        }
        bool persisted = false, retained = false;
    } hook;
    ScopedTempDir directory("chunkdb-feed-slots-ack-warning");
    TableCatalog catalog(Configuration(directory.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    const auto start = table->CreateFeedSlot("consumer").position;
    SetBlocks(*table, {{0, 0}}, 1U);
    FeedSlotTestAccess::Sync(*table);
    const auto frontier = table->ListFeedSlots().front().durable_watermark;
    assert(frontier > start.revision);
    std::promise<std::string> logged;
    auto warning = logged.get_future();
    const DiagnosticSink sink([&](const std::string& line) {
        if (line.find("feed slot acknowledgement and retention step was slow") != std::string::npos) logged.set_value(line);
    });
    FeedSlotTestAccess::SetHook(*table, &hook);
    FeedSlotTestAccess::StageAck(*table, "consumer", {start.epoch, frontier});
    assert(warning.wait_for(10s) == std::future_status::ready);
    const auto line = warning.get();
    assert(line.find(" WARN store ") != std::string::npos && line.ends_with(" total_ms=1000 slots=1"));
    assert(table->ListFeedSlots().front().position.revision == frontier);
    FeedSlotTestAccess::SetHook(*table, nullptr);
}

// After the first pass, a pass syncs what was written since the previous one:
// a new WAL and its large-chunk directory, but not a WAL left unchanged.
void LaterPassSyncsNewWal() {
    ScopedTempDir directory("chunkdb-feed-slots-later-pass");
    TableCatalog catalog(Configuration(directory.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    const auto start = table->CreateFeedSlot("consumer").position;
    SetBlocks(*table, {{0, 0}}, 1U);
    FeedSlotTestAccess::Sync(*table);
    const auto old_wal = WalOf(*table, directory.path(), 0, 0);
    const auto new_wal = WalOf(*table, directory.path(), 40, 40);
    assert(!std::filesystem::exists(new_wal.parent_path()));
    SetBlocks(*table, {{40, 40}}, 2U);
    {
        const SyncLog synced;
        FeedSlotTestAccess::Sync(*table);
        assert(synced.Contains(new_wal) && synced.Contains(new_wal.parent_path()));
        assert(!synced.Contains(old_wal));
    }
    auto reader = table->ReadFeedArchive(start);
    assert(reader.Next() && reader.Next() && !reader.Next());
}

// A pass that takes the unsynced set while a rollback boundary has taken its
// chunk's WAL out of it, but not synced it yet, syncs that WAL before it
// persists the durable frontier.
class FrontierWitness : public FeedSlotTestHook {
  public:
    FrontierWitness(const SyncLog& synced, std::filesystem::path wal) : synced_(synced), wal_(std::move(wal)) {}
    void Run(Point point, std::uint64_t) override {
        std::unique_lock lock(mutex_);
        if (point == Point::kBeforePersist) { persisted_after_sync_ = synced_.Contains(wal_); return; }
        if (point != Point::kBeforeSync || entered_) return;
        entered_ = true;
        cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
    }
    void Wait() { std::unique_lock lock(mutex_); assert(cv_.wait_for(lock, 10s, [&] { return entered_; })); }
    void Release() { std::lock_guard lock(mutex_); released_ = true; cv_.notify_all(); }
    [[nodiscard]] bool PersistedAfterSync() { std::lock_guard lock(mutex_); return persisted_after_sync_; }
  private:
    const SyncLog& synced_;
    const std::filesystem::path wal_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_ = false, released_ = false, persisted_after_sync_ = false;
};
void PassCoversPausedBoundary() {
    ScopedTempDir directory("chunkdb-feed-slots-boundary");
    TableCatalog catalog(Configuration(directory.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    (void)table->CreateFeedSlot("consumer");
    SetBlocks(*table, {{0, 0}}, 1U); // Pending in the group-commit batch.
    const auto wal = WalOf(*table, directory.path(), 0, 0);
    const SyncLog synced;
    FrontierWitness witness(synced, wal);
    FeedSlotTestAccess::SetHook(*table, &witness);
    // The pass writes the batch, records the WAL and stops before its take.
    std::thread pass([&] { FeedSlotTestAccess::Sync(*table); });
    witness.Wait();
    auto lease = table->Acquire();
    auto& store = lease->store();
    store.ArmWalBoundarySyncPauseForTests();
    std::exception_ptr boundary_error;
    std::thread boundary([&] {
        try {
            const auto state = store.ReadChunkState(0, 0);
            auto payload = state.payload;
            payload[0] ^= 1U;
            assert(store.CasChunkStateBytes(0, 0, state.version, payload, state.presence_bitmap).ok);
        } catch (const std::exception&) { boundary_error = std::current_exception(); }
    });
    assert(store.WaitForWalBoundarySyncPauseForTests());
    witness.Release();
    pass.join();
    assert(witness.PersistedAfterSync());
    store.ResumeWalBoundarySyncForTests();
    boundary.join();
    if (boundary_error) std::rethrow_exception(boundary_error);
    lease.reset();
    FeedSlotTestAccess::SetHook(*table, nullptr);
}

// A pass syncs what it drained without the publish lock, so a write and the
// checkpoint it triggers finish while the pass still waits for its syncs. That
// checkpoint is strict: the archive it publishes is synced, and the frontier
// the pass persists covers only what the pass captured. On Windows the pass
// keeps the publish lock through its syncs, so the checkpoint waits for them
// and publishes after the pass with the same results.
void CheckpointDoesNotWaitForPassSyncs() {
#ifdef _WIN32
    constexpr bool kPublishesDuringSyncs = false;
#else
    constexpr bool kPublishesDuringSyncs = true;
#endif
    ScopedTempDir directory("chunkdb-feed-slots-unlocked-syncs");
    TableCatalog catalog(Configuration(directory.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    const auto start = table->CreateFeedSlot("consumer").position;
    SetBlocks(*table, {{0, 0}}, 1U);
    std::uint64_t first;
    { auto lease = table->Acquire(); first = lease->store().GetChunkVersion(0, 0); }
    const SyncLog synced;
    SyncPause pause(FeedSlotTestHook::Point::kAfterDrain);
    FeedSlotTestAccess::SetHook(*table, &pause);
    std::thread pass([&] { FeedSlotTestAccess::Sync(*table); });
    const auto captured = pause.Wait();
    assert(captured >= first);
    const auto wal = WalOf(*table, directory.path(), 0, 0);
    std::uint64_t later, publishes;
    bool archived_synced;
    {
        auto lease = table->Acquire();
        auto& store = lease->store();
        const auto before = store.CheckpointPublishesDuringBarrierSyncsForTests();
        if constexpr (!kPublishesDuringSyncs) store.ArmCheckpointPublishAttemptForTests();
        auto checkpoint = std::async(std::launch::async, [&] {
            store.SetBlockBits(0, 0, Bits(2U));
            store.CheckpointForTests(0, 0);
        });
        if constexpr (!kPublishesDuringSyncs) {
            // The checkpoint reaches publication and waits for the paused pass.
            assert(store.WaitForCheckpointPublishAttemptForTests());
            assert(checkpoint.wait_for(0s) == std::future_status::timeout);
            pause.Release();
        }
        // Outside Windows the pass stays paused before its syncs until the checkpoint ends.
        assert(checkpoint.wait_for(10s) == std::future_status::ready);
        checkpoint.get();
        later = store.GetChunkVersion(0, 0);
        publishes = store.CheckpointPublishesDuringBarrierSyncsForTests() - before;
        // The WAL was synced before the checkpoint archived it: by the
        // checkpoint outside Windows, where the pass has not synced yet.
        archived_synced = synced.Contains(wal);
    }
    assert(later > captured && publishes == (kPublishesDuringSyncs ? 1U : 0U) && archived_synced);
    if constexpr (kPublishesDuringSyncs) pause.Release();
    pass.join();
    FeedSlotTestAccess::SetHook(*table, nullptr);
    assert(Archives(directory.path()) == 1U && !std::filesystem::exists(wal));
    assert(ReadFeedSlotRecords(TablePath(directory.path()), start.epoch)->durable_watermark == captured);
    { auto reader = table->ReadFeedArchive(start);
      assert(reader.Next()->position.revision == first && !reader.Next()); }
    FeedSlotTestAccess::Sync(*table);
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

// An archive directory made by a preparation that failed before syncing its
// parent is in the parent only once a sync makes its entry durable. The next
// archive into it, in the same process or after a restart, must sync the
// parent before the archive is relied on: the checkpoint then removes the
// WAL the slot still needs, and a crash could lose the directory with it.
// The checked archive belongs to a chunk whose directory was prepared before
// the failure, so preparing a chunk directory does not sync the same parent
// by coincidence in the same process; after a restart that preparation syncs
// it as well.
void ArchiveAfterFailedDirectoryPreparationSyncsParent() {
    for (const bool restart : {false, true}) {
        ScopedTempDir directory("chunkdb-feed-slots-archive-dir");
        const auto config = Configuration(directory.path());
        const auto table_dir = TablePath(directory.path());
        const auto archive_dir = table_dir / kFeedArchiveDirName;
        // Directories synced while recording, with whether the archive held
        // a WAL when each was synced.
        std::mutex mutex;
        std::vector<std::pair<std::filesystem::path, bool>> synced;
        bool recording = false;
        const auto archived = [&] { return Archives(directory.path()) > 0U; };
        SetDirectorySyncObserverForTests([&](const std::filesystem::path& path) {
            std::lock_guard lock(mutex);
            if (recording) synced.emplace_back(path, archived());
        });
        const auto same = [](const std::filesystem::path& a, const std::filesystem::path& b) {
            std::error_code ec;
            return std::filesystem::equivalent(a, b, ec) && !ec;
        };
        {
            std::optional<TableCatalog> catalog(std::in_place, config);
            (void)feed_test::CreateDefault(*catalog);
            auto table = catalog->Find("default");
            // Chunk (0, 0) is in large chunk (0, 0) and chunk (2, 0), block
            // (8, 0), in large chunk (1, 0).
            { auto lease = table->Acquire();
              lease->store().SetBlockBits(0, 0, Bits(1U));
              lease->store().SetBlockBits(8, 0, Bits(1U));
              lease->store().CheckpointForTests(0, 0);
              lease->store().CheckpointForTests(2, 0); }
            (void)table->CreateFeedSlot("consumer");
            // With a slot every checkpoint syncs like a synced mode: this
            // one prepares chunk (2, 0)'s directory and, having no WAL to
            // archive, does not make the archive directory.
            { auto lease = table->Acquire();
              lease->store().CheckpointForTests(2, 0); }
            assert(!std::filesystem::exists(archive_dir));
            { auto lease = table->Acquire();
              lease->store().SetBlockBits(0, 0, Bits(2U));
              txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_DIR_CREATE_BEFORE_PARENT_SYNC_ONCE", "1");
              assert(txn_test::Throws([&] { lease->store().CheckpointForTests(0, 0); })); }
            assert(std::filesystem::exists(archive_dir));
            assert(!archived());
            if (restart) {
                table.reset();
                catalog.reset();
                catalog.emplace(config);
                table = catalog->Find("default");
            }
            { auto lease = table->Acquire();
              lease->store().SetBlockBits(8, 0, Bits(3U));
              {
                  std::lock_guard lock(mutex);
                  recording = true;
              }
              lease->store().CheckpointForTests(2, 0); }
            assert(archived());
        }
        SetDirectorySyncObserverForTests({});

        // The table directory was synced before the archive directory was
        // synced holding the archived WAL.
        bool parent_synced = false;
        bool relied_on = false;
        for (const auto& [path, holds_wal] : synced) {
            if (same(path, table_dir)) parent_synced = true;
            if (same(path, archive_dir) && holds_wal) {
                relied_on = true;
                break;
            }
        }
        assert(relied_on);
        assert(parent_synced);
    }
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
    FirstPassSyncsEveryFile();
    SlowSlotPassWarnsAfterPublishing();
    SlowSlotWorkerWarnsAfterRetention();
    LaterPassSyncsNewWal();
    PassCoversPausedBoundary();
    CheckpointDoesNotWaitForPassSyncs();
    AmbiguousAcknowledgementPreservesArchives();
    ArchiveAfterFailedDirectoryPreparationSyncsParent();
    LimitLossKeepsFastSlotAndSurvivesRestart();
    SlotRecordSyncFailureFreezesFrontier();
    SlotRecordsStartupAndTemporaryCleanup();
#if defined(__APPLE__)
    SyncFailureFreezesFrontier();
#endif
}
