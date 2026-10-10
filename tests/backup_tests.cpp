#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <future>
#include <mutex>
#include <sstream>
#include <map>
#include <set>
#include "wal_writer.hpp"
#include "txn_history.hpp"
#include <thread>

#include "backup.hpp"
#include "checkpoint.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "feed_test_utils.hpp"
#include "feed_slot_records.hpp"
#include "snapshot_generation.hpp"
#include "store_manifest.hpp"
#include "wal_replay.hpp"
#include "verify.hpp"

namespace chunkdb {
struct BackupTestAccess {
    static auto Operations(TableCatalog& catalog) { return std::unique_lock(catalog.operations_mutex_); }
    static auto Maintenance(ChunkStore& store) { return std::unique_lock(store.backup_maintenance_mutex_); }
    static auto Exclusive(Table& table) { return table.BeginExclusive(); }
    static void EndExclusive(Table& table, std::shared_ptr<ChunkStore> store) { table.EndExclusive(std::move(store), table.options_); }
    static auto Pin(Table& table) { return table.PinForBackup({}); }
};
}
namespace {
using namespace chunkdb;
using namespace std::chrono_literals;
using txn_test::CounterBits;
using txn_test::ReadCounter;
using txn_test::WriteCounter;
using txn_test::ScopedEnv;
using test::ScopedTempDir;

CatalogConfig Config(const std::filesystem::path& path, DurabilityMode mode = DurabilityMode::kRelaxed) {
    auto config = feed_test::Config(path);
    config.default_options.durability_mode = mode;
    config.default_options.wal_group_commit_updates = 64;
    return config;
}
template <typename F> std::string Error(F&& f) {
    try { f(); } catch (const std::exception& e) { return e.what(); }
    return {};
}
class Pause final : public BackupTestHook {
  public:
    explicit Pause(Point point) : point_(point) {}
    void Run(Point point, std::string_view, std::uint64_t revision) override {
        if (point != point_) return;
        std::unique_lock lock(mutex_);
        if (entered_) return;
        entered_ = true; revision_ = revision; cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
    }
    std::uint64_t Wait() {
        std::unique_lock lock(mutex_);
        assert(cv_.wait_for(lock, 10s, [&] { return entered_; }));
        return revision_;
    }
    void Release() { std::lock_guard lock(mutex_); released_ = true; cv_.notify_all(); }
  private:
    Point point_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_ = false, released_ = false;
    std::uint64_t revision_ = 0;
};
ChunkState ReadBackup(const std::filesystem::path& root, std::string_view table, ChunkCoord coord) {
    const auto dir = root / "tables" / table;
    const auto manifest = *ReadStoreManifest(dir);
    const Geometry geometry(manifest.geometry, manifest.schema);
    ChunkState state{.version = 0, .payload = std::vector<std::uint8_t>(geometry.ChunkPayloadBytes()),
        .presence_bitmap = std::vector<std::uint8_t>(ChunkPresenceBitmapBytes(geometry)), .vars = {}};
    std::uint64_t schema = 0;
    const auto image = ChunkDataPath(dir, geometry, coord);
    if (std::filesystem::exists(image)) {
        auto parsed = ParseChunkImage(LoadFile(image), geometry, coord, manifest.store_id, manifest.features);
        state.version = parsed.revision; schema = parsed.schema_version;
        state.payload = std::move(parsed.payload); state.presence_bitmap = std::move(parsed.presence_bitmap); state.vars = std::move(parsed.vars);
    }
    const auto wal = ChunkWalPath(dir, geometry, coord);
    if (std::filesystem::exists(wal)) {
        const auto replay = ReplayWal(LoadFile(wal), geometry, coord, manifest.store_id, manifest.features,
            state.version, schema, &state.payload, &state.presence_bitmap, &state.vars);
        assert(replay.replayable && !replay.tail_truncated_or_corrupt && replay.vars_problem.empty());
        state.version = std::max(state.version, replay.revision);
    }
    return state;
}
void CrcChunks() {
    const std::string text = "123456789";
    const auto* data = reinterpret_cast<const std::uint8_t*>(text.data());
    assert(Crc32(data, text.size()) == 0xcbf43926U);
    assert(Crc32Extend(0, nullptr, 0) == 0U);
    for (std::size_t split = 0; split <= text.size(); ++split) {
        const auto first = Crc32Extend(0, data, split);
        assert(Crc32Extend(first, nullptr, 0) == first);
        assert(Crc32Extend(first, data + split, text.size() - split) == Crc32(data, text.size()));
    }
}
void CutAndProgress(DurabilityMode mode) {
    ScopedTempDir temp("chunkdb-backup-cut");
    const auto root = std::filesystem::canonical(temp.path());
    const auto source = root / "source", target = root / "backup";
    TableCatalog catalog(Config(source, mode));
    auto table = catalog.Find("default");
    const auto slot = table->CreateFeedSlot("reader");
    auto lease = table->Acquire();
    auto& store = lease->store();
    WriteCounter(store, {0, 0}, 11); store.CheckpointForTests(0, 0);
    WriteCounter(store, {1, 0}, 22);  // cached batch must be flushed before linking
    const auto before = store.GetChunkVersion(1, 0);
    Pause pause(BackupTestHook::Point::kAfterCut);
    catalog.SetBackupHookForTests(&pause);
    auto backup = std::async(std::launch::async, [&] { return catalog.BackupTo(target, {}); });
    const auto cut = pause.Wait();
    assert(cut >= before);
    auto writer = std::async(std::launch::async, [&] { WriteCounter(store, {0, 0}, 33); WriteCounter(store, {2, 0}, 44); });
    assert(writer.wait_for(10s) == std::future_status::ready); writer.get();
    store.CheckpointForTests(0, 0);  // deferred while the backup owns maintenance
    const auto image = ChunkDataPath(store.data_dir(), store.geometry(), {0, 0});
    assert(ParseChunkImage(LoadFile(image), store.geometry(), {0, 0}, slot.position.epoch, ReadStoreManifest(store.data_dir())->features).revision <= cut);
    pause.Release();
    const auto result = backup.get(); catalog.SetBackupHookForTests(nullptr);
    assert(result.tables.size() == 1U && result.tables[0].revision == cut && result.files_count > 4U && result.bytes > 0U);
    const auto record = ReadBackupRecord(target); ValidateBackupInventory(target, record);
    assert(txn_test::CounterOf(ReadBackup(target, "default", {0, 0})) == 11U);
    assert(txn_test::CounterOf(ReadBackup(target, "default", {1, 0})) == 22U);
    assert(txn_test::CounterOf(ReadBackup(target, "default", {2, 0})) == 0U);
    const auto slots = *ReadFeedSlotRecords(target / "tables/default", slot.position.epoch);
    assert(slots.slots.size() == 1U && slots.slots[0].name == "reader" && slots.durable_watermark <= cut);
    assert(!std::filesystem::exists(target / "tables/default" / kFeedArchiveDirName));
    // A later checkpoint retries the deferred image replacement and preserves the live write.
    store.CheckpointForTests(0, 0);
    assert(ParseChunkImage(LoadFile(image), store.geometry(), {0, 0}, slot.position.epoch, ReadStoreManifest(store.data_dir())->features).revision > cut);
    assert(ReadCounter(store, {0, 0}) == 33U);
    assert(!Error([&] { TableCatalog refused(Config(target)); }).empty());
    auto direct = txn_test::Config(target / "tables/default", mode);
    assert(!Error([&] { ChunkStore refused(direct); }).empty());
    RestoreBackup(target, root / "restored");
    std::ostringstream verification;
    assert(VerifyDataDirectory(root / "restored", verification).errors == 0U);
    TableCatalog restored(Config(root / "restored", mode));
    auto restored_table = restored.Find("default");
    assert(restored_table->Info().store_id != slot.position.epoch);
    auto restored_lease = restored_table->Acquire();
    assert(ReadCounter(restored_lease->store(), {0, 0}) == 11U);
    assert(ReadCounter(restored_lease->store(), {1, 0}) == 22U);
    assert(ReadCounter(restored_lease->store(), {2, 0}) == 0U);
}
void AcknowledgedLoad(DurabilityMode mode) {
    ScopedTempDir temp("chunkdb-backup-load");
    const auto root = std::filesystem::canonical(temp.path());
    auto config = Config(root / "source", mode);
    config.default_options.checkpoint_update_interval = 2;
    config.default_options.checkpoint_wal_bytes = 1024;
    TableCatalog catalog(config);
    (void)catalog.Create("a", config.default_geometry, config.default_options);
    (void)catalog.Create("b", config.default_geometry, config.default_options);
    class LoadHook final : public BackupTestHook {
      public:
        void Run(Point point, std::string_view table, std::uint64_t revision) override {
            if (point == Point::kAfterCut) {
                std::lock_guard lock(mutex); cuts[std::string(table)] = revision; cv.notify_all();
            }
            copy.Run(point, table, revision);
        }
        void AwaitCut(const std::string& table) {
            std::unique_lock lock(mutex); assert(cv.wait_for(lock, 20s, [&] { return cuts.contains(table); }));
        }
        std::mutex mutex; std::condition_variable cv;
        std::map<std::string, std::uint64_t> cuts;
        Pause copy{Point::kBeforeCopy};
    } hook;
    catalog.SetBackupHookForTests(&hook);
    struct Ack { ChunkCoord coord; ChunkState state; };
    struct Writer { std::string name; std::vector<Ack> acknowledged; std::promise<void> started; std::future<void> done; };
    std::array<Writer, 3> writers;
    const std::array<std::string, 3> names{"default", "a", "b"};
    for (std::size_t i = 0; i < writers.size(); ++i) {
        auto& writer = writers[i]; writer.name = names[i];
        writer.done = std::async(std::launch::async, [&writer, &catalog, &hook] {
            auto lease = catalog.Find(writer.name)->Acquire(); auto& store = lease->store();
            const auto note = [&](ChunkCoord coord) { writer.acknowledged.push_back({coord, store.ReadChunkState(coord.x, coord.y)}); };
            for (std::uint32_t value = 1; value <= 20; ++value) {
                if (value == 6) { writer.started.set_value(); hook.AwaitCut(writer.name); }
                WriteCounter(store, {0, 0}, value); note({0, 0});
                const auto column = store.geometry().layout().schema().columns[0].name;
                const auto conditional = store.SetBlock(4, 0, {{column, BitsValue{CounterBits(value + 100)}}}, store.GetChunkVersion(1, 0));
                assert(conditional.ok); note({1, 0});
                auto whole = store.ReadChunkState(2, 0); txn_test::SetCounter(&whole, value + 200);
                (void)store.SetChunkStateBytes(2, 0, whole.payload, whole.presence_bitmap); note({2, 0});
                auto snapshot = store.BeginTxnSnapshot(txn_test::kTxnDuration);
                std::vector<TxnChunkWrite> writes;
                for (const ChunkCoord coord : {ChunkCoord{0, 0}, ChunkCoord{1, 0}}) {
                    auto state = store.ReadChunkStateAt(*snapshot, coord.x, coord.y);
                    txn_test::SetCounter(&state, value + 300);
                    writes.push_back({.coord = coord, .state = std::move(state)});
                }
                (void)store.CommitTransaction(*snapshot, {}, std::move(writes)); note({0, 0}); note({1, 0});
                WriteCounter(store, {3, 0}, value); note({3, 0});
                store.UnsetBlock(12, 0); note({3, 0}); store.CheckpointForTests(3, 0);
                if (value >= 6) { WriteCounter(store, {4, 0}, value + 400); note({4, 0}); }
            }
            assert(store.RuntimeStats().checkpoints > 0U);
            assert(store.RuntimeStats().empty_chunk_gcs > 0U);
        });
    }
    for (auto& writer : writers) writer.started.get_future().wait();
    auto backup = std::async(std::launch::async, [&] { return catalog.BackupTo(root / "backup", {}); });
    hook.copy.Wait();
    for (auto& writer : writers) { assert(writer.done.wait_for(20s) == std::future_status::ready); writer.done.get(); }
    hook.copy.Release(); const auto result = backup.get(); catalog.SetBackupHookForTests(nullptr);
    assert(result.tables.size() == writers.size());
    RestoreBackup(root / "backup", root / "restored");
    std::ostringstream report; assert(VerifyDataDirectory(root / "restored", report).errors == 0U);
    TableCatalog restored(Config(root / "restored", mode));
    for (const auto& writer : writers) {
        const auto cut = std::find_if(result.tables.begin(), result.tables.end(), [&](const auto& t) { return t.name == writer.name; });
        assert(cut != result.tables.end());
        std::map<std::pair<std::int64_t, std::int64_t>, ChunkState> expected;
        bool later = false;
        for (const auto& ack : writer.acknowledged) {
            if (ack.state.version <= cut->revision) expected[{ack.coord.x, ack.coord.y}] = ack.state;
            else later = true;
        }
        assert(later && expected.size() == 4U);
        std::printf("backup load mode=%s table=%s acknowledged=%zu cut=%llu verified_chunks=5\n",
            DurabilityModeName(mode), writer.name.c_str(), writer.acknowledged.size(), static_cast<unsigned long long>(cut->revision));
        auto restored_table = restored.Find(writer.name);
        assert(restored_table->Info().store_id != cut->epoch);
        auto lease = restored_table->Acquire();
        for (std::int64_t x = 0; x <= 4; ++x) {
            const auto actual = lease->store().ReadChunkState(x, 0);
            const auto state = expected.find({x, 0});
            if (state == expected.end()) assert(!ChunkPresent(actual.presence_bitmap));
            else assert(actual.payload == state->second.payload && actual.presence_bitmap == state->second.presence_bitmap && actual.vars == state->second.vars);
        }
    }
}
void CopyReleasesHoldsAndBusy() {
    ScopedTempDir temp("chunkdb-backup-copy");
    const auto root = std::filesystem::canonical(temp.path());
    TableCatalog catalog(Config(root / "source"));
    auto table = catalog.Find("default");
    { auto lease = table->Acquire(); WriteCounter(lease->store(), {0, 0}, 7); }
    Pause pause(BackupTestHook::Point::kBeforeCopy); catalog.SetBackupHookForTests(&pause);
    auto backup = std::async(std::launch::async, [&] { return catalog.BackupTo(root / "backup", {}); });
    pause.Wait();
    bool busy = false;
    try { (void)catalog.BackupTo(root / "other", {}); } catch (const BackupBusyError&) { busy = true; }
    assert(busy && !std::filesystem::exists(root / "other"));
    auto ddl = std::async(std::launch::async, [&] {
        TableOptionsUpdate options;
        options.checkpoint_update_interval = 1U;
        catalog.SetOptions("default", options);
        auto reopened = catalog.Find("default");
        (void)reopened->CreateFeedSlot("after");
        auto lease = reopened->Acquire(); WriteCounter(lease->store(), {0, 0}, 9); lease->store().CheckpointForTests(0, 0);
    });
    assert(ddl.wait_for(10s) == std::future_status::ready); ddl.get();
    pause.Release(); (void)backup.get(); catalog.SetBackupHookForTests(nullptr);
    assert(txn_test::CounterOf(ReadBackup(root / "backup", "default", {0, 0})) == 7U);
}
void TargetAndCancellation() {
    ScopedTempDir temp("chunkdb-backup-target");
    const auto root = std::filesystem::canonical(temp.path());
    const auto source = root / "source";
    TableCatalog catalog(Config(source));
    assert(!Error([&] { (void)catalog.BackupTo(source / "nested", {}); }).empty());
    assert(!std::filesystem::exists(source / "nested"));
    const auto occupied = root / "occupied";
    std::filesystem::create_directory(occupied); std::ofstream(occupied / "keep") << "foreign";
    assert(!Error([&] { (void)catalog.BackupTo(occupied, {}); }).empty());
    assert(LoadFile(occupied / "keep") == std::vector<std::uint8_t>({'f','o','r','e','i','g','n'}));
    assert(!Error([&] { (void)catalog.BackupTo(root / "early", {.cancelled = [] { return true; }}); }).empty());
    assert(!std::filesystem::exists(root / "early"));
    Pause pause(BackupTestHook::Point::kBeforeCopy); catalog.SetBackupHookForTests(&pause);
    std::atomic<bool> cancel = false;
    auto backup = std::async(std::launch::async, [&] { return Error([&] { (void)catalog.BackupTo(root / "cancelled", {.cancelled = [&] { return cancel.load(); }}); }); });
    pause.Wait(); cancel = true; pause.Release(); assert(backup.get().find("cancelled") != std::string::npos);
    catalog.SetBackupHookForTests(nullptr);
    assert(std::filesystem::exists(root / "cancelled" / kBackupIncompleteName));
    assert(!std::filesystem::exists(root / "cancelled" / kBackupMarkerName));
    (void)catalog.BackupTo(root / "later", {});
}
void CompletionAndPoison() {
    ScopedTempDir temp("chunkdb-backup-completion");
    const auto root = std::filesystem::canonical(temp.path());
    TableCatalog catalog(Config(root / "source", DurabilityMode::kFsyncWal));
    auto lease = catalog.Find("default")->Acquire(); auto& store = lease->store();
    WriteCounter(store, {0, 0}, 1);
    auto snapshot = store.BeginTxnSnapshot(txn_test::kTxnDuration);
    auto state = store.ReadChunkStateAt(*snapshot, 0, 0); txn_test::SetCounter(&state, 2);
    std::vector<TxnChunkWrite> writes{{.coord = {0, 0}, .state = std::move(state)}};
    store.ArmTxnPauseForTests(TxnPausePoint::kBeforePostCommitOutcome);
    ScopedEnv rename_failure("CHUNKDB_FAILPOINT_TXN_COMMIT_INTENT_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
    ScopedEnv failure("CHUNKDB_FAILPOINT_TXN_COMMIT_INTENT_COMPLETION_SYNC_FAIL_ONCE", "1");
    auto transaction = std::async(std::launch::async, [&] { return Error([&] { (void)store.CommitTransaction(*snapshot, {}, std::move(writes)); }); });
    assert(store.WaitForTxnPauseForTests(TxnPausePoint::kBeforePostCommitOutcome));
    // Chunk locks are released, but completion (and poisoning) has not happened.
    assert(ReadCounter(store, {0, 0}) == 2U);
    Pause pause(BackupTestHook::Point::kWaitingForCompletion); catalog.SetBackupHookForTests(&pause);
    std::atomic<bool> cancel = false;
    auto backup = std::async(std::launch::async, [&] { return Error([&] { (void)catalog.BackupTo(root / "cancelled", {.cancelled = [&] { return cancel.load(); }}); }); });
    pause.Wait(); pause.Release(); cancel = true;
    assert(backup.wait_for(10s) == std::future_status::ready);
    assert(backup.get().find("cancelled") != std::string::npos);
    catalog.SetBackupHookForTests(nullptr);
    Pause waited(BackupTestHook::Point::kWaitingForCompletion); catalog.SetBackupHookForTests(&waited);
    auto refused = std::async(std::launch::async, [&] { return Error([&] { (void)catalog.BackupTo(root / "poisoned", {}); }); });
    const auto cut = waited.Wait();
    auto later = std::async(std::launch::async, [&] { WriteCounter(store, {1, 0}, 99); });
    assert(later.wait_for(10s) == std::future_status::ready); later.get();
    assert(store.GetChunkVersion(1, 0) > cut);
    waited.Release();
    store.ResumeTxnForTests(TxnPausePoint::kBeforePostCommitOutcome);
    assert(!transaction.get().empty());
    assert(refused.get().find("fail-closed") != std::string::npos);
    catalog.SetBackupHookForTests(nullptr);
    assert(!std::filesystem::exists(root / "poisoned" / kBackupMarkerName));
}
void FailedGenerationAndEmptyCatalog() {
    ScopedTempDir temp("chunkdb-backup-generation");
    const auto root = std::filesystem::canonical(temp.path());
    TableCatalog catalog(Config(root / "source"));
    {
        auto lease = catalog.Find("default")->Acquire(); auto& store = lease->store();
        store.SetSnapshotGenerationLingerForTests(0, 0);
        ScopedEnv failure("CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_END_FAIL_ONCE", "1");
        WriteCounter(store, {0, 0}, 1);
        (void)Error([&] { store.WalBarrier(); });
        assert((store.SnapshotGenerationForTests() & 1U) != 0U);
        assert(Error([&] { (void)catalog.BackupTo(root / "failed", {}); }).find("snapshot") != std::string::npos);
    }
    catalog.Drop("default");
    const auto result = catalog.BackupTo(root / "empty", {});
    assert(result.tables.empty()); ValidateBackupInventory(root / "empty", ReadBackupRecord(root / "empty"));
}
void SaveBytes(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    out.close(); assert(out);
}
void ColdRecovery(unsigned defect) {
    ScopedTempDir temp("chunkdb-backup-cold");
    const auto root = std::filesystem::canonical(temp.path());
    const auto source = root / "source";
    std::filesystem::path wal;
    std::uint64_t boundary = 0, version = 0;
    {
        TableCatalog catalog(Config(source, DurabilityMode::kFsyncWal));
        auto lease = catalog.Find("default")->Acquire(); auto& store = lease->store();
        WriteCounter(store, {0, 0}, 11); store.WalBarrier();
        wal = ChunkWalPath(store.data_dir(), store.geometry(), {0, 0}); boundary = std::filesystem::file_size(wal);
        WriteCounter(store, {0, 0}, 22); store.WalBarrier(); version = store.GetChunkVersion(0, 0);
    }
    auto bytes = LoadFile(wal);
    if (defect == 0) { bytes.pop_back(); SaveBytes(wal, bytes); }
    if (defect == 1) { bytes.resize(kWalHeaderSize / 2U); SaveBytes(wal, bytes); }
    if (defect == 2) { bytes[static_cast<std::size_t>(boundary) - 5U] ^= 1U; SaveBytes(wal, bytes); }
    if (defect == 3) {
        const TxnIntent intent{.state = TxnIntentState::kRollback, .version = version,
            .entries = {{.coord = {0, 0}, .wal_boundary = boundary}}};
        SaveBytes(TxnIntentPath(source / "tables/default", version), SerializeTxnIntent(intent));
    }
    if (defect == 4) std::filesystem::rename(wal, wal.parent_path() / "C_00_0.wal");
    TableCatalog catalog(Config(source, DurabilityMode::kFsyncWal));
    auto lease = catalog.Find("default")->Acquire();
    assert(!lease->store().IsChunkLoadedForTests(0, 0));
    if (defect == 2 || defect == 4) {
        assert(!Error([&] { (void)catalog.BackupTo(root / "backup", {}); }).empty());
        assert(!std::filesystem::exists(root / "backup" / kBackupMarkerName));
    } else {
        (void)catalog.BackupTo(root / "backup", {});
        assert(txn_test::CounterOf(ReadBackup(root / "backup", "default", {0, 0})) == (defect == 1 ? 0U : 11U));
        assert(!std::filesystem::exists(root / "backup" / "tables/default/.chunkdb.intents"));
        RestoreBackup(root / "backup", root / "restored");
        std::ostringstream report; assert(VerifyDataDirectory(root / "restored", report).errors == 0U);
    }
}
void CancelCatalogWaitAndRestrictions() {
    ScopedTempDir temp("chunkdb-backup-waits");
    const auto root = std::filesystem::canonical(temp.path());
    const auto source = root / "source";
    {
        TableCatalog catalog(Config(source));
        auto table = catalog.Find("default");
        // Each threshold identifies the first callback after an actual failed
        // timed lock acquisition or Busy-table predicate observation.
        const auto run_wait = [&](const char* name, unsigned reached) {
            std::promise<void> waiting;
            auto observed = waiting.get_future();
            std::atomic<unsigned> checks = 0;
            std::atomic<bool> cancel = false;
            auto backup = std::async(std::launch::async, [&] {
                return Error([&] { (void)catalog.BackupTo(root / name, {.cancelled = [&] {
                    if (++checks == reached) waiting.set_value();
                    return cancel.load();
                }}); });
            });
            assert(observed.wait_for(10s) == std::future_status::ready);
            cancel = true;
            assert(backup.wait_for(10s) == std::future_status::ready);
            assert(backup.get().find("cancelled") != std::string::npos);
            assert(std::filesystem::exists(root / name / kBackupIncompleteName));
        };
        { auto operations = BackupTestAccess::Operations(catalog); run_wait("operations", 3); }
        {
            auto exclusive = BackupTestAccess::Exclusive(*table);
            run_wait("pin", 5);
            BackupTestAccess::EndExclusive(*table, std::move(exclusive));
        }
        {
            auto lease = table->Acquire();
            auto maintenance = BackupTestAccess::Maintenance(lease->store());
            run_wait("maintenance", 6);
        }
        std::future<void> exclusive;
        {
            auto pin = BackupTestAccess::Pin(*table);
            exclusive = std::async(std::launch::async, [&] {
                auto store = BackupTestAccess::Exclusive(*table);
                BackupTestAccess::EndExclusive(*table, std::move(store));
            });
            auto lease = table->Acquire(); // pins prevent the transition to Busy
            WriteCounter(lease->store(), {0, 0}, 77);
        }
        exclusive.get();
    }
    auto readonly = Config(source); readonly.access_mode = AccessMode::kReadOnly;
    { TableCatalog catalog(readonly); assert(!Error([&] { (void)catalog.BackupTo(root / "ro", {}); }).empty()); }
    auto multi = Config(source); multi.allow_multiple_processes = true;
    { TableCatalog catalog(multi); assert(!Error([&] { (void)catalog.BackupTo(root / "multi", {}); }).empty()); }
}
}
int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--completion") { CompletionAndPoison(); return 0; }
    if (argc == 2 && std::string_view(argv[1]) == "--generation") { FailedGenerationAndEmptyCatalog(); return 0; }
    CrcChunks();
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal, DurabilityMode::kFsyncCheckpoint}) { CutAndProgress(mode); AcknowledgedLoad(mode); }
    CopyReleasesHoldsAndBusy(); TargetAndCancellation(); CompletionAndPoison(); FailedGenerationAndEmptyCatalog(); CancelCatalogWaitAndRestrictions();
    for (unsigned defect = 0; defect < 5U; ++defect) ColdRecovery(defect);
    std::puts("backup core passed: 3 cut modes, 3 load modes x3 tables x155 acknowledgements, 5 cold cases, 6 focused groups");
}
