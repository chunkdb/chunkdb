#include "statement_control.hpp"
#include "chunkdb/chunk_store.hpp"
#include "user_registry.hpp"
#include "test_utils.hpp"
#include "catalog_test_utils.hpp"
#include "backup.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <exception>
#include <future>
#include <thread>

namespace chunkdb {
struct FeatureActivationTestAccess {
    static auto Metadata(TableCatalog& catalog) { return std::unique_lock(catalog.feature_metadata_gate_); }
    static auto Operations(TableCatalog& catalog) { return std::unique_lock(catalog.operations_mutex_); }
    static auto Resident(ChunkStore& store) { return store.GetOrLoadRegularChunk({0, 0}); }
};
struct StatementChunkLockTestAccess {
    static void WaitWriter(RegularChunkMutex& mutex) {
        std::unique_lock lock(mutex.mutex_);
        ++mutex.waiters_; // Counted like the lock's own waits, so a writer's arrival wakes it.
        const bool arrived = mutex.changed_.wait_for(lock, std::chrono::seconds(5), [&] { return mutex.writers_ != 0U; });
        --mutex.waiters_;
        assert(arrived);
    }
    // Until a thread blocks inside one of the lock's waits.
    static void WaitBlocked(RegularChunkMutex& mutex, std::size_t count) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        std::unique_lock lock(mutex.mutex_);
        while (mutex.waiters_ < count) {
            assert(std::chrono::steady_clock::now() < deadline);
            lock.unlock();
            std::this_thread::yield();
            lock.lock();
        }
    }
};
struct UserRegistryTestAccess {
    static auto Admission(UserRegistry& users) { return std::unique_lock(users.mutex_); }
};
}

namespace {
using namespace chunkdb;
using namespace std::chrono_literals;
StoreConfig Config(const std::filesystem::path& dir, DurabilityMode mode) {
    StoreConfig config;
    config.data_dir = dir;
    config.geometry = {.large_chunk_width_chunks = 2U, .large_chunk_height_chunks = 2U,
        .chunk_width_blocks = 4U, .chunk_height_blocks = 4U, .block_bits = 4U};
    config.durability_mode = mode;
    config.wal_group_commit_updates = 1U;
    return config;
}
void OrdinaryCancellationRollsBack() {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal, DurabilityMode::kFsyncCheckpoint}) {
        test::ScopedTempDir dir("chunkdb-cancel-ordinary");
        auto config = Config(dir.path(), mode);
        {
            ChunkStore store(config);
            store.SetBlockBits(0, 0, "1000");
            const auto version = store.GetChunkVersion(0, 0);
            StatementControl control(StatementControl::Clock::now(), 0U);
            assert(control.Start() && control.RequestStop(StatementStopReason::kCancelled));
            bool cancelled = false;
            {
                ScopedStatementControl scope(&control);
                try { store.SetBlockBits(0, 0, "0100"); }
                catch (const StatementStoppedError&) { cancelled = true; }
            }
            assert(cancelled && store.GetBlockBits(0, 0) == "1000" && store.GetChunkVersion(0, 0) == version);
        }
        ChunkStore reopened(config);
        assert(reopened.GetBlockBits(0, 0) == "1000");
    }
}
void ConditionalDecisionArbitrates() {
    for (const auto phase : {ConditionalMutationPausePoint::kAfterWalAppend,
                             ConditionalMutationPausePoint::kAfterCommitIntentPublish}) {
        test::ScopedTempDir dir("chunkdb-cancel-conditional");
        const auto config = Config(dir.path(), DurabilityMode::kFsyncWal);
        const bool before = phase == ConditionalMutationPausePoint::kAfterWalAppend;
        {
            ChunkStore store(config);
            store.SetBlockBits(0, 0, "1000");
            auto next = store.ReadChunkState(0, 0);
            const auto version = next.version;
            next.payload[0] = 2U;
            StatementControl control(StatementControl::Clock::now(), 0U);
            assert(control.Start());
            store.ArmConditionalMutationPauseForTests(phase);
            bool cancelled = false;
            std::exception_ptr failure;
            std::thread writer([&] {
                ScopedStatementControl scope(&control);
                try { (void)store.WriteChunkState(0, 0, std::move(next), version); }
                catch (const StatementStoppedError&) { cancelled = true; }
                catch (const std::exception&) { failure = std::current_exception(); }
            });
            assert(store.WaitForConditionalMutationPauseForTests());
            assert(control.RequestStop(StatementStopReason::kCancelled) == before);
            store.ResumeConditionalMutationForTests();
            writer.join();
            if (failure) std::rethrow_exception(failure);
            assert(cancelled == before);
            assert(store.GetBlockBits(0, 0) == (before ? "1000" : "0100"));
            assert(control.Complete().has_value() == before);
        }
        ChunkStore reopened(config);
        assert(reopened.GetBlockBits(0, 0) == (before ? "1000" : "0100"));
    }
}
void TransactionCancellationRollsBack() {
    test::ScopedTempDir dir("chunkdb-cancel-transaction");
    const auto config = Config(dir.path(), DurabilityMode::kFsyncWal);
    {
        ChunkStore store(config);
        store.SetBlockBits(0, 0, "1000");
        auto snapshot = store.BeginTxnSnapshot(10s);
        auto next = store.ReadChunkStateAt(*snapshot, 0, 0);
        next.payload[0] = 2U;
        StatementControl control(StatementControl::Clock::now(), 0U);
        assert(control.Start() && control.RequestStop(StatementStopReason::kCancelled));
        bool cancelled = false;
        {
            ScopedStatementControl scope(&control);
            try { (void)store.CommitTransaction(*snapshot, {}, {{{0, 0}, std::move(next)}}); }
            catch (const StatementStoppedError&) { cancelled = true; }
        }
        assert(cancelled && store.GetBlockBits(0, 0) == "1000");
    }
    ChunkStore reopened(config);
    assert(reopened.GetBlockBits(0, 0) == "1000");
}
void UserReplacementCancellationIsNotPublished() {
    test::ScopedTempDir dir("chunkdb-cancel-user-replacement");
    const auto verifier = scram::MakeVerifier("secret", std::vector<std::uint8_t>(16U, 7U), scram::kMinIterations);
    UserRegistry users(dir.path(), std::make_pair(std::string("admin"), verifier), {});
    const auto before = users.Snapshot();
    StatementControl control(StatementControl::Clock::now(), 0U);
    assert(control.Start() && control.RequestStop(StatementStopReason::kCancelled));
    bool cancelled = false;
    {
        ScopedStatementControl scope(&control);
        try { users.SetLimits("admin", 1U, 3U); }
        catch (const StatementStoppedError&) { cancelled = true; }
    }
    assert(cancelled && users.Snapshot() == before && ReadUsersFile(dir.path()) == before);
}
class DdlAdmissionGate final : public MigrationTestHook {
  public:
    void Run(Point point, std::string_view) override {
        if (point != Point::kBeforeTableExclusive) return;
        std::unique_lock lock(mutex_);
        entered_ = true;
        changed_.notify_all();
        changed_.wait(lock, [&] { return released_; });
    }
    void Wait() {
        std::unique_lock lock(mutex_);
        assert(changed_.wait_for(lock, 5s, [&] { return entered_; }));
    }
    void Release() { { std::lock_guard lock(mutex_); released_ = true; } changed_.notify_all(); }
  private:
    std::mutex mutex_;
    std::condition_variable changed_;
    bool entered_ = false, released_ = false;
};
void CancellationInterruptsDdlAdmission() {
    test::ScopedTempDir dir("chunkdb-cancel-ddl-admission");
    const auto config = Config(dir.path(), DurabilityMode::kFsyncWal);
    TableCatalog catalog(CatalogConfigFromStoreConfig(config));
    auto table = test::CreateBitsTable(catalog, config.geometry);
    auto lease = table->Acquire();
    DdlAdmissionGate gate;
    catalog.SetMigrationTestHook(&gate);
    auto first = std::async(std::launch::async, [&] { catalog.SetOptions("default", catalog.default_options()); });
    gate.Wait();
    StatementControl control(StatementControl::Clock::now(), 0U);
    assert(control.Start());
    std::promise<void> entering;
    auto second = std::async(std::launch::async, [&] {
        ScopedStatementControl scope(&control);
        entering.set_value();
        try { catalog.SetOptions("default", catalog.default_options()); }
        catch (const StatementStoppedError&) { return true; }
        return false;
    });
    entering.get_future().get();
    assert(control.RequestStop(StatementStopReason::kCancelled));
    const bool interrupted = second.wait_for(2s) == std::future_status::ready;
    gate.Release();
    lease.reset();
    first.get();
    assert(interrupted && second.get());
    catalog.SetMigrationTestHook(nullptr);
    assert(table->Acquire().has_value());
}
void CancellationInterruptsCatalogAdmission() {
    for (const bool feature : {false, true}) {
        test::ScopedTempDir dir("chunkdb-cancel-catalog-admission");
        const auto config = Config(dir.path(), DurabilityMode::kFsyncWal);
        TableCatalog catalog(CatalogConfigFromStoreConfig(config));
        StatementControl control(StatementControl::Clock::now(), 0U);
        assert(control.Start());
        std::promise<void> entering;
        const auto exercise = [&](auto& admission) {
            auto command = std::async(std::launch::async, [&] {
                ScopedStatementControl scope(&control);
                entering.set_value();
                try { (void)test::CreateBitsTable(catalog, config.geometry); }
                catch (const StatementStoppedError&) { return true; }
                return false;
            });
            entering.get_future().get();
            assert(control.RequestStop(StatementStopReason::kCancelled));
            const bool interrupted = command.wait_for(2s) == std::future_status::ready;
            admission.unlock();
            assert(interrupted && command.get());
        };
        if (feature) { auto admission = FeatureActivationTestAccess::Metadata(catalog); exercise(admission); }
        else { auto admission = FeatureActivationTestAccess::Operations(catalog); exercise(admission); }
        assert(!catalog.Find("default"));
    }
}
// Each release wakes the waiters its change admits, with no stop requested:
// the last reader admits a writer, a writer admits a reader, and a reader
// queued behind a waiting writer follows that writer.
void ReleaseAdmitsBlockedLockers() {
    RegularChunkMutex mutex;
    const auto admitted = [](std::future<bool>& locker) {
        return locker.wait_for(std::chrono::seconds(5)) == std::future_status::ready && locker.get();
    };
    mutex.lock_shared();
    auto writer = std::async(std::launch::async, [&] { return mutex.lock(std::stop_token{}); });
    StatementChunkLockTestAccess::WaitBlocked(mutex, 1U);
    mutex.unlock_shared();
    assert(admitted(writer));
    auto reader = std::async(std::launch::async, [&] { return mutex.lock_shared(std::stop_token{}); });
    StatementChunkLockTestAccess::WaitBlocked(mutex, 1U);
    mutex.unlock();
    assert(admitted(reader));
    auto queued_writer = std::async(std::launch::async, [&] { return mutex.lock(std::stop_token{}); });
    StatementChunkLockTestAccess::WaitBlocked(mutex, 1U);
    auto queued_reader = std::async(std::launch::async, [&] { return mutex.lock_shared(std::stop_token{}); });
    StatementChunkLockTestAccess::WaitBlocked(mutex, 2U);
    mutex.unlock_shared();
    assert(admitted(queued_writer));
    mutex.unlock();
    assert(admitted(queued_reader));
    mutex.unlock_shared();
    assert(mutex.try_lock()); mutex.unlock();
}
void CancelledWriterReopensReadAdmission() {
    RegularChunkMutex mutex;
    mutex.lock_shared();
    std::stop_source cancelled;
    auto writer = std::async(std::launch::async, [&] { return mutex.lock(cancelled.get_token()); });
    StatementChunkLockTestAccess::WaitWriter(mutex);
    assert(!mutex.try_lock_shared());
    cancelled.request_stop();
    assert(!writer.get());
    const bool shared = mutex.try_lock_shared();
    assert(shared == !kSerialChunkReaders);
    if (shared) mutex.unlock_shared();
    mutex.unlock_shared();
    assert(mutex.try_lock_shared()); mutex.unlock_shared();
    assert(mutex.try_lock()); mutex.unlock();
}
void CancellationInterruptsUserReplacementAdmission() {
    test::ScopedTempDir dir("chunkdb-cancel-user-admission");
    const auto verifier = scram::MakeVerifier("secret", std::vector<std::uint8_t>(16U, 7U), scram::kMinIterations);
    UserRegistry users(dir.path(), std::make_pair(std::string("admin"), verifier), {});
    const auto before = users.Snapshot();
    auto holder = UserRegistryTestAccess::Admission(users);
    StatementControl control(StatementControl::Clock::now(), 0U);
    assert(control.Start());
    std::promise<void> entering;
    auto command = std::async(std::launch::async, [&] {
        ScopedStatementControl context(&control);
        entering.set_value();
        try { users.SetLimits("admin", 1U, 1U); }
        catch (const StatementStoppedError&) { return true; }
        return false;
    });
    entering.get_future().get();
    assert(control.RequestStop(StatementStopReason::kCancelled));
    const bool interrupted = command.wait_for(2s) == std::future_status::ready;
    holder.unlock();
    assert(interrupted && command.get());
    assert(users.Snapshot() == before && ReadUsersFile(dir.path()) == before);
}
void CancellationInterruptsContendedChunkAdmission() {
    for (const bool writing : {false, true}) {
        test::ScopedTempDir dir("chunkdb-cancel-chunk-admission");
        ChunkStore store(Config(dir.path(), DurabilityMode::kFsyncWal));
        store.SetBlockBits(0, 0, "1000");
        auto next = store.ReadChunkState(0, 0);
        const auto version = next.version;
        next.payload[0] = 2U;
        store.ArmConditionalMutationPauseForTests(ConditionalMutationPausePoint::kAfterWalAppend);
        auto holder = std::async(std::launch::async, [&] {
            return store.WriteChunkState(0, 0, std::move(next), version);
        });
        assert(store.WaitForConditionalMutationPauseForTests());
        StatementControl control(StatementControl::Clock::now(), 0U);
        assert(control.Start());
        std::promise<void> entering;
        auto command = std::async(std::launch::async, [&] {
            ScopedStatementControl context(&control);
            entering.set_value();
            try {
                if (writing) store.SetBlockBits(0, 0, "0010");
                else (void)store.GetBlockBits(0, 0);
            } catch (const StatementStoppedError&) { return true; }
            return false;
        });
        entering.get_future().get();
        assert(control.RequestStop(StatementStopReason::kCancelled));
        const bool interrupted = command.wait_for(2s) == std::future_status::ready;
        store.ResumeConditionalMutationForTests();
        assert(holder.get().ok);
        assert(interrupted && command.get());
        assert(store.GetBlockBits(0, 0) == "0100");
    }
}
void CancellationRacesChunkAdmissionRelease() {
    for (std::size_t i = 0U; i < 32U; ++i) {
        RegularChunkMutex mutex;
        mutex.lock();
        StatementControl control(StatementControl::Clock::now(), 0U);
        assert(control.Start());
        std::promise<void> entering;
        auto waiter = std::async(std::launch::async, [&] {
            ScopedStatementControl context(&control);
            entering.set_value();
            try { auto lock = LockStatementChunkShared(mutex); }
            catch (const StatementStoppedError&) { return true; }
            return false;
        });
        entering.get_future().get();
        assert(control.RequestStop(StatementStopReason::kCancelled));
        mutex.unlock();
        assert(waiter.get());
        assert(mutex.try_lock()); mutex.unlock();
    }
}
class BackupAdmissionGate final : public BackupTestHook {
  public:
    explicit BackupAdmissionGate(Point point) : point_(point) {}
    void Run(Point point, std::string_view, std::uint64_t) override {
        if (point != point_) return;
        std::unique_lock lock(mutex_);
        if (entered_) return;
        entered_ = true; changed_.notify_all();
        changed_.wait(lock, [&] { return released_; });
    }
    void Wait(std::future<bool>& command) {
        std::unique_lock lock(mutex_);
        const bool entered = changed_.wait_for(lock, 5s, [&] { return entered_; });
        if (!entered && command.wait_for(0s) == std::future_status::ready) (void)command.get();
        assert(entered);
    }
    void Release() { { std::lock_guard lock(mutex_); released_ = true; } changed_.notify_all(); }
  private:
    const Point point_;
    std::mutex mutex_;
    std::condition_variable changed_;
    bool entered_ = false, released_ = false;
};
void CancellationInterruptsBackupAdmission() {
    for (const bool resident_wait : {false, true}) {
        test::ScopedTempDir dir("chunkdb-cancel-backup-admission");
        const auto root = std::filesystem::canonical(dir.path());
        const auto config = Config(root / "source", DurabilityMode::kFsyncWal);
        TableCatalog catalog(CatalogConfigFromStoreConfig(config));
        auto table = test::CreateBitsTable(catalog, config.geometry);
        auto lease = table->Acquire();
        auto& store = lease->store();
        store.SetBlockBits(0, 0, "1000");
        const auto resident = FeatureActivationTestAccess::Resident(store);
        auto next = store.ReadChunkState(0, 0);
        const auto version = next.version;
        next.payload[0] = 2U;
        BackupAdmissionGate gate(resident_wait ? BackupTestHook::Point::kAfterCut : BackupTestHook::Point::kWaitingForCompletion);
        catalog.SetBackupHookForTests(&gate);
        StatementControl control(StatementControl::Clock::now(), 0U);
        assert(control.Start());
        const auto backup = [&] {
            return std::async(std::launch::async, [&] {
                ScopedStatementControl context(&control);
                BackupOptions options{.cancelled = StatementStopToken()};
                try { (void)catalog.BackupTo(root / "backup", options); }
                catch (const std::exception&) {
                    if (control.Complete() == StatementStopReason::kCancelled) return true;
                    throw;
                }
                return false;
            });
        };
        std::future<bool> command;
        if (resident_wait) { command = backup(); gate.Wait(command); }
        store.ArmConditionalMutationPauseForTests(ConditionalMutationPausePoint::kAfterWalAppend);
        auto writer = std::async(std::launch::async, [&] { return store.WriteChunkState(0, 0, std::move(next), version); });
        struct ResumeWriter {
            ChunkStore& store;
            bool armed = true;
            ~ResumeWriter() { if (armed) store.ResumeConditionalMutationForTests(); }
        } resume{store};
        assert(store.WaitForConditionalMutationPauseForTests());
        if (resident_wait) {
            gate.Release();
            StatementChunkLockTestAccess::WaitWriter(resident->mutex);
        } else { command = backup(); gate.Wait(command); }
        assert(control.RequestStop(StatementStopReason::kCancelled));
        gate.Release();
        const bool interrupted = command.wait_for(2s) == std::future_status::ready;
        store.ResumeConditionalMutationForTests();
        resume.armed = false;
        assert(writer.get().ok);
        assert(interrupted && command.get());
        assert(!std::filesystem::exists(root / "backup" / kBackupMarkerName));
        assert(store.GetBlockBits(0, 0) == "0100");
        catalog.SetBackupHookForTests(nullptr);
    }
}
std::atomic<std::int64_t> fake_clock_ms{0};
StatementControl::Clock::time_point FakeNow() noexcept {
    return StatementControl::Clock::time_point(std::chrono::milliseconds(fake_clock_ms.load()));
}
// Leaves chunk (0, 0) of a relaxed store with background maintenance in a
// checkpoint that writes and syncs its files without the chunk lock, so
// its mark stays set until ResumeCheckpointBeforeWalRemovalForTests.
StoreConfig MarkedCheckpointConfig(const std::filesystem::path& dir) {
    auto config = Config(dir, DurabilityMode::kRelaxed);
    config.checkpoint_update_interval = 1U;
    config.background_maintenance = true;
    return config;
}
void HoldCheckpointMark(ChunkStore& store) {
    // The barrier's floor makes the next checkpoint sync its files.
    store.WalBarrier();
    store.ArmCheckpointBeforeWalRemovalPauseForTests();
    store.SetBlockBits(0, 0, "1000");
    assert(store.WaitForCheckpointBeforeWalRemovalForTests());
}
// A statement that waits for a background checkpoint's files stops with its
// cancellation or deadline while the checkpoint still owns them, and its
// write is not applied.
void CheckpointFilesWaitStops() {
    enum class Operation { kConditional, kTransform, kCommit };
    for (const auto operation : {Operation::kConditional, Operation::kTransform, Operation::kCommit}) {
        for (const auto reason : {StatementStopReason::kCancelled, StatementStopReason::kTimeout}) {
            test::ScopedTempDir dir("chunkdb-cancel-checkpoint-files");
            const auto config = MarkedCheckpointConfig(dir.path());
            {
                ChunkStore store(config);
                HoldCheckpointMark(store);
                auto snapshot = store.BeginTxnSnapshot(10s);
                struct Resume {
                    ChunkStore& store;
                    ~Resume() { store.ResumeCheckpointBeforeWalRemovalForTests(); }
                } resume{store};
                auto next = store.ReadChunkStateAt(*snapshot, 0, 0);
                const auto version = next.version;
                next.payload[0] = 2U;
                StatementControl control(FakeNow(), reason == StatementStopReason::kTimeout ? 1000U : 0U, &FakeNow);
                assert(control.Start());
                auto write = std::async(std::launch::async, [&] {
                    ScopedStatementControl scope(&control);
                    try {
                        if (operation == Operation::kConditional) (void)store.WriteChunkState(0, 0, std::move(next), version);
                        else if (operation == Operation::kTransform)
                            (void)store.TransformChunkState({0, 0}, std::nullopt, false, [](ChunkState& state) { state.payload[0] = 2U; });
                        else (void)store.CommitTransaction(*snapshot, {}, {{{0, 0}, std::move(next)}});
                    } catch (const StatementStoppedError& stopped) { return stopped.reason(); }
                    return StatementStopReason::kNone;
                });
                assert(write.wait_for(200ms) == std::future_status::timeout);
                if (reason == StatementStopReason::kTimeout) fake_clock_ms += 1000;
                assert(control.RequestStop(reason));
                assert(write.wait_for(5s) == std::future_status::ready && write.get() == reason);
                assert(store.GetBlockBits(0, 0) == "1000");
            }
            ChunkStore reopened(config);
            assert(reopened.GetBlockBits(0, 0) == "1000");
        }
    }
}
// Ordinary writes past the deferred-checkpoint hard bound wait for the mark:
// a statement's stop ends its wait without the write, and the checkpoint's
// end wakes every other waiter, with or without a statement.
void CheckpointBacklogWaitStopsOrResumes() {
    for (const bool stop : {true, false}) {
        test::ScopedTempDir dir("chunkdb-cancel-checkpoint-backlog");
        const auto config = MarkedCheckpointConfig(dir.path());
        {
            ChunkStore store(config);
            HoldCheckpointMark(store);
            struct Resume {
                ChunkStore& store;
                ~Resume() { store.ResumeCheckpointBeforeWalRemovalForTests(); }
            } resume{store};
            // The hard bound is four checkpoint intervals since the image.
            for (const auto x : {1, 2, 3, 0}) store.SetBlockBits(x, 1, "0001");
            StatementControl control(StatementControl::Clock::now(), 0U);
            assert(control.Start());
            auto bounded = std::async(std::launch::async, [&] {
                ScopedStatementControl scope(&control);
                try { store.SetBlockBits(0, 2, "0011"); }
                catch (const StatementStoppedError&) { return false; }
                return true;
            });
            auto plain = std::async(std::launch::async, [&] { store.SetBlockBits(1, 2, "0110"); });
            assert(bounded.wait_for(200ms) == std::future_status::timeout);
            assert(plain.wait_for(0ms) == std::future_status::timeout);
            if (stop) {
                assert(control.RequestStop(StatementStopReason::kCancelled));
                assert(bounded.wait_for(5s) == std::future_status::ready && !bounded.get());
                assert(plain.wait_for(0ms) == std::future_status::timeout);
            }
            store.ResumeCheckpointBeforeWalRemovalForTests();
            assert(plain.wait_for(5s) == std::future_status::ready);
            plain.get();
            if (!stop) assert(bounded.wait_for(5s) == std::future_status::ready && bounded.get());
            assert(store.BlockExists(0, 2) == !stop && store.GetBlockBits(1, 2) == "0110");
            store.WalBarrier();
        }
        auto reopened_config = config;
        reopened_config.background_maintenance = false;
        ChunkStore reopened(reopened_config);
        assert(reopened.BlockExists(0, 2) == !stop && reopened.GetBlockBits(1, 2) == "0110");
        assert(reopened.GetBlockBits(0, 1) == "0001" && reopened.GetBlockBits(0, 0) == "1000");
    }
}
StoreConfig ThreeAxisConfig(const std::filesystem::path& dir) {
    auto config = Config(dir, DurabilityMode::kFsyncWal);
    config.geometry.dims = 3U;
    config.geometry.chunk_depth_blocks = 2U;
    config.geometry.large_chunk_depth_chunks = 2U;
    return config;
}
void StartStopped(StatementControl& control) {
    assert(control.Start() && control.RequestStop(StatementStopReason::kCancelled));
}
template <typename Operation> bool Stops(Operation operation) {
    try { operation(); }
    catch (const StatementStoppedError&) { return true; }
    return false;
}
// Chunk model statements wait for a contended chunk lock with their stop, and
// stop at each chunk of a three-axis area, an identity scan and a read-only
// coordinate index rebuild, even where no chunk lock is taken.
void ChunkModelStatementsStop() {
    test::ScopedTempDir dir("chunkdb-cancel-chunk-model");
    auto config = ThreeAxisConfig(dir.path());
    {
        ChunkStore store(config);
        store.SetBlockBits3D(0, 0, 0, "1000");
        auto snapshot = store.BeginTxnSnapshot(10s);
        const auto resident = FeatureActivationTestAccess::Resident(store);
        resident->mutex.lock();
        const auto stops = [&](auto operation) {
            StatementControl control(StatementControl::Clock::now(), 0U);
            assert(control.Start());
            auto command = std::async(std::launch::async, [&] {
                ScopedStatementControl scope(&control);
                return Stops(operation);
            });
            StatementChunkLockTestAccess::WaitBlocked(resident->mutex, 1U);
            assert(control.RequestStop(StatementStopReason::kCancelled));
            const bool interrupted = command.wait_for(5s) == std::future_status::ready;
            if (!interrupted) resident->mutex.unlock();
            assert(interrupted && command.get());
        };
        stops([&] { (void)store.TransformChunkState({0, 0, 0}, std::nullopt, false, [](ChunkState& state) { state.payload[0] = 2U; }); });
        stops([&] { (void)store.GetBlockWithVersion3DAt(*snapshot, 0, 0, 0); });
        resident->mutex.unlock();
        assert(store.GetBlockBits3D(0, 0, 0) == "1000");
        StatementControl control(StatementControl::Clock::now(), 0U);
        StartStopped(control);
        ScopedStatementControl scope(&control);
        assert(Stops([&] { (void)store.ReadChunkRange3D(10, 10, 10, 11, 11, 11, true); }));
        assert(Stops([&] { (void)store.ReadChunkRadius3D(10, 10, 10, 1, true); }));
    }
    {
        // Nothing is loaded, so the scan reads the chunk from its files.
        ChunkStore store(config);
        StatementControl control(StatementControl::Clock::now(), 0U);
        StartStopped(control);
        ScopedStatementControl scope(&control);
        assert(Stops([&] { (void)store.ScanChunkIdentities(false, {}, 8U, std::nullopt, false); }));
    }
    config.access_mode = AccessMode::kReadOnly;
    ChunkStore read_only(config);
    {
        StatementControl control(StatementControl::Clock::now(), 0U);
        StartStopped(control);
        ScopedStatementControl scope(&control);
        assert(Stops([&] { (void)read_only.RetainedTombstoneCount(); }));
    }
    assert(read_only.RetainedTombstoneCount() == 0U);
}
// ALTER TABLE on chunk columns and COMPACT TOMBSTONES wait for the catalog's
// feature metadata admission with their stop.
void ChunkModelCatalogAdmissionStops() {
    test::ScopedTempDir dir("chunkdb-cancel-chunk-columns");
    const auto config = Config(dir.path(), DurabilityMode::kFsyncWal);
    TableCatalog catalog(CatalogConfigFromStoreConfig(config));
    (void)test::CreateBitsTable(catalog, config.geometry);
    const auto add = [](const TableSchema& current) {
        return AddChunkColumn(current, Column{.id = current.next_column_id, .name = "owner",
            .type = {ColumnKind::kUnsigned, 8U}, .nullable = true, .default_value = {}});
    };
    for (const bool compact : {false, true}) {
        StatementControl control(StatementControl::Clock::now(), 0U);
        assert(control.Start());
        auto admission = FeatureActivationTestAccess::Metadata(catalog);
        std::promise<void> entering;
        auto command = std::async(std::launch::async, [&] {
            ScopedStatementControl scope(&control);
            entering.set_value();
            return Stops([&] {
                if (compact) (void)catalog.CompactTombstones("default", 1U);
                else catalog.ChangeChunkColumns("default", add);
            });
        });
        entering.get_future().get();
        assert(control.RequestStop(StatementStopReason::kCancelled));
        const bool interrupted = command.wait_for(2s) == std::future_status::ready;
        admission.unlock();
        assert(interrupted && command.get());
    }
    assert(catalog.Require("default")->Info().chunk_schema.columns.empty());
}
}
int main(int argc, char** argv) {
    CancelledWriterReopensReadAdmission();
    ReleaseAdmitsBlockedLockers();
    if (argc == 2 && std::string_view(argv[1]) == "--chunk-mutex-only") return 0;
    OrdinaryCancellationRollsBack();
    ConditionalDecisionArbitrates();
    TransactionCancellationRollsBack();
    UserReplacementCancellationIsNotPublished();
    CancellationInterruptsDdlAdmission();
    CancellationInterruptsCatalogAdmission();
    CancellationInterruptsUserReplacementAdmission();
    CancellationInterruptsContendedChunkAdmission();
    CancellationRacesChunkAdmissionRelease();
    CancellationInterruptsBackupAdmission();
    CheckpointFilesWaitStops();
    CheckpointBacklogWaitStopsOrResumes();
    ChunkModelStatementsStop();
    ChunkModelCatalogAdmissionStops();
}
