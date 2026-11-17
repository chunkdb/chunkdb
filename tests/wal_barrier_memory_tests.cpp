#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <new>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "checkpoint.hpp"
#include "chunkdb/chunk_store.hpp"
#include "test_utils.hpp"

// Fails the allocation that follows `fail_after` successful ones on this
// thread; negative leaves allocation alone.
namespace allocation { thread_local long fail_after = -1; }
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void* operator new(std::size_t bytes) {
    if (allocation::fail_after == 0) { allocation::fail_after = -1; throw std::bad_alloc(); }
    if (allocation::fail_after > 0) --allocation::fail_after;
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
struct UnsyncedTestAccess {
    using Artifacts = ChunkStore::UnsyncedArtifacts;
    struct State {
        std::unordered_set<std::string> files;
        std::unordered_set<std::string> dirs;
        std::unordered_set<std::string> swept_dirs;
        std::unordered_multiset<std::string> files_in_flight;
        std::unordered_multiset<std::string> dirs_in_flight;
        bool overflow = false;
        bool operator==(const State&) const = default;
    };
    static State Snapshot(ChunkStore& store) {
        std::lock_guard lock(store.unsynced_mutex_);
        return State{store.unsynced_files_, store.unsynced_dirs_, store.unsynced_swept_dirs_,
                     store.unsynced_files_in_flight_, store.unsynced_dirs_in_flight_,
                     store.unsynced_overflow_};
    }
    // What a rollback boundary does before it syncs its chunk's artifacts.
    static ChunkStore::UnsyncedArtifacts Forget(ChunkStore& store, const std::string& file,
                                                const std::string& dir = {}) {
        std::vector<std::filesystem::path> dirs;
        if (!dir.empty()) dirs.emplace_back(dir);
        return store.ForgetUnsynced({std::filesystem::path(file)}, dirs);
    }
    static void Finish(ChunkStore& store, const ChunkStore::UnsyncedArtifacts& forgotten, bool synced) {
        store.FinishForgottenUnsynced(forgotten, synced);
    }
    static ChunkStore::UnsyncedArtifacts Take(ChunkStore& store) { return store.TakeUnsynced(); }
    static void Restore(ChunkStore& store, ChunkStore::UnsyncedArtifacts drained) {
        store.RestoreUnsynced(std::move(drained));
    }
};
}  // namespace chunkdb

namespace {
using namespace chunkdb;
using Access = UnsyncedTestAccess;

StoreConfig Config(const std::filesystem::path& data_dir) {
    return StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 5,
        },
        .data_dir = data_dir,
        .durability_mode = DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 1000,
        .checkpoint_wal_bytes = 1024 * 1024,
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 128,
    };
}

// Records the files synced through SyncFilePath while it exists.
class SyncedFiles {
  public:
    SyncedFiles() {
        SetFileSyncObserverForTests([this](const std::filesystem::path& path) {
            std::lock_guard lock(mutex_);
            paths_.insert(path.lexically_normal().string());
        });
    }
    ~SyncedFiles() { SetFileSyncObserverForTests({}); }
    SyncedFiles(const SyncedFiles&) = delete;
    SyncedFiles& operator=(const SyncedFiles&) = delete;
    [[nodiscard]] bool Contains(const std::string& path) const {
        std::lock_guard lock(mutex_);
        return paths_.contains(std::filesystem::path(path).lexically_normal().string());
    }

  private:
    mutable std::mutex mutex_;
    std::unordered_set<std::string> paths_;
};

// Writes WALs in two chunks and hands one of them to a rollback boundary
// whose sync is still in flight.
Access::Artifacts PrepareInFlight(ChunkStore& store) {
    store.SetBlockBits(0, 0, "10101");
    store.SetBlockBits(4, 0, "01010");
    const auto tracked = Access::Snapshot(store);
    assert(!tracked.overflow && tracked.files.size() >= 2U);
    auto forgotten = Access::Forget(store, *tracked.files.begin());
    assert(forgotten.files.size() == 1U);
    return forgotten;
}

Access::Artifacts TakeFailingAllocation(ChunkStore& store, long fail_after, bool& failed) {
    Access::Artifacts drained;
    failed = false;
    allocation::fail_after = fail_after;
    try { drained = Access::Take(store); }
    catch (const std::bad_alloc&) { failed = true; }
    allocation::fail_after = -1;
    return drained;
}

// A drain that runs out of memory keeps every tracked file, and the next
// barrier syncs them all.
void TestFailedDrainKeepsTrackedFiles() {
    test::ScopedTempDir dir("chunkdb-barrier-memory-files");
    ChunkStore store(Config(dir.path()));
    const auto forgotten = PrepareInFlight(store);
    const auto before = Access::Snapshot(store);
    assert(!before.files.empty());

    bool failed = false;
    (void)TakeFailingAllocation(store, 0, failed);
    assert(failed);
    assert(Access::Snapshot(store) == before);

    {
        const SyncedFiles synced;
        store.WalBarrier();
        for (const auto& file : before.files) assert(synced.Contains(file));
        for (const auto& file : forgotten.files) assert(synced.Contains(file));
    }
    Access::Finish(store, forgotten, true);
    assert(store.UnsyncedTrackedCountForTests() == 0U);
}

// A drain that runs out of memory keeps the overflow flag, so the next
// barrier still syncs the whole data directory.
void TestFailedDrainKeepsOverflow() {
    test::ScopedTempDir dir("chunkdb-barrier-memory-overflow");
    ChunkStore store(Config(dir.path()));
    const auto forgotten = PrepareInFlight(store);
    store.ForceUnsyncedOverflowForTests();
    const auto before = Access::Snapshot(store);
    assert(before.overflow);

    bool failed = false;
    (void)TakeFailingAllocation(store, 0, failed);
    assert(failed);
    assert(Access::Snapshot(store) == before);

    const auto full_syncs = store.RuntimeStats().wal_barrier_full_syncs;
    store.WalBarrier();
    assert(store.RuntimeStats().wal_barrier_full_syncs == full_syncs + 1U);
    assert(!store.UnsyncedOverflowFlagForTests());
    Access::Finish(store, forgotten, true);
}

// Whichever allocation of the drain fails, the set stays as it was; the
// drain that succeeds holds every tracked and in-flight artifact.
void TestEveryDrainAllocationFailure() {
    test::ScopedTempDir dir("chunkdb-barrier-memory-each");
    ChunkStore store(Config(dir.path()));
    const auto forgotten = PrepareInFlight(store);
    const auto before = Access::Snapshot(store);

    long failures = 0;
    for (long fail_after = 0;; ++fail_after) {
        bool failed = false;
        auto drained = TakeFailingAllocation(store, fail_after, failed);
        if (failed) {
            ++failures;
            assert(Access::Snapshot(store) == before);
            continue;
        }
        for (const auto& file : before.files) assert(drained.files.contains(file));
        for (const auto& file : forgotten.files) assert(drained.files.contains(file));
        for (const auto& dir_path : before.dirs) assert(drained.dirs.contains(dir_path));
        const auto after = Access::Snapshot(store);
        assert(after.files.empty() && after.dirs.empty() && after.swept_dirs.empty() && !after.overflow);
        Access::Restore(store, std::move(drained));
        break;
    }
    assert(failures > 0);
    Access::Finish(store, forgotten, true);
    store.WalBarrier();
    assert(store.UnsyncedTrackedCountForTests() == 0U);
}

// A barrier leaves the tracked sets empty, without room for an entry. With a
// file and a directory in flight, whichever allocation of the next drain
// fails, the overflow flag stays set; the drain that succeeds holds both.
void TestDrainIntoEmptySetKeepsOverflow() {
    test::ScopedTempDir dir("chunkdb-barrier-memory-empty");
    ChunkStore store(Config(dir.path()));
    store.SetBlockBits(0, 0, "10101");
    store.SetBlockBits(4, 0, "01010");
    const auto tracked = Access::Snapshot(store);
    assert(!tracked.overflow && !tracked.files.empty() && !tracked.dirs.empty());
    const auto forgotten = Access::Forget(store, *tracked.files.begin(), *tracked.dirs.begin());
    assert(forgotten.files.size() == 1U && forgotten.dirs.size() == 1U);
    store.WalBarrier();
    store.ForceUnsyncedOverflowForTests();
    const auto before = Access::Snapshot(store);
    assert(before.overflow && before.files.empty() && before.dirs.empty() && before.swept_dirs.empty());
    assert(!before.files_in_flight.empty() && !before.dirs_in_flight.empty());

    long failures = 0;
    for (long fail_after = 0;; ++fail_after) {
        bool failed = false;
        auto drained = TakeFailingAllocation(store, fail_after, failed);
        if (failed) {
            ++failures;
            assert(Access::Snapshot(store) == before);
            continue;
        }
        assert(drained.overflow);
        for (const auto& file : forgotten.files) assert(drained.files.contains(file));
        for (const auto& dir_path : forgotten.dirs) assert(drained.dirs.contains(dir_path));
        Access::Restore(store, std::move(drained));
        break;
    }
    assert(failures > 0);
    Access::Finish(store, forgotten, true);
    const auto full_syncs = store.RuntimeStats().wal_barrier_full_syncs;
    store.WalBarrier();
    assert(store.RuntimeStats().wal_barrier_full_syncs == full_syncs + 1U);
    assert(!store.UnsyncedOverflowFlagForTests());
}

// Whichever allocation of the restore after a failed sync fails, the set
// is back as it was or the overflow flag is set, and the next barrier syncs
// every drained file or the whole data directory.
void TestEveryRestoreAllocationFailure() {
    long failures = 0;
    for (long fail_after = 0;; ++fail_after) {
        test::ScopedTempDir dir("chunkdb-barrier-memory-restore");
        ChunkStore store(Config(dir.path()));
        store.SetBlockBits(0, 0, "10101");
        store.SetBlockBits(4, 0, "01010");
        const auto before = Access::Snapshot(store);
        assert(!before.overflow && !before.files.empty() && !before.dirs.empty());

        auto drained = Access::Take(store);
        allocation::fail_after = fail_after;
        try { Access::Restore(store, std::move(drained)); }
        catch (const std::bad_alloc&) {}
        const bool failed = allocation::fail_after == -1;
        allocation::fail_after = -1;

        const auto after = Access::Snapshot(store);
        if (!failed) {
            assert(after == before);
            break;
        }
        ++failures;
        assert(after == before || (after.overflow && after.files.empty() && after.dirs.empty()));
        const SyncedFiles synced;
        const auto full_syncs = store.RuntimeStats().wal_barrier_full_syncs;
        store.WalBarrier();
        if (after.overflow) {
            assert(store.RuntimeStats().wal_barrier_full_syncs == full_syncs + 1U);
        } else {
            for (const auto& file : before.files) assert(synced.Contains(file));
        }
        assert(store.UnsyncedTrackedCountForTests() == 0U && !store.UnsyncedOverflowFlagForTests());
    }
    assert(failures > 0);
}
}  // namespace

int main() {
    TestFailedDrainKeepsTrackedFiles();
    TestFailedDrainKeepsOverflow();
    TestEveryDrainAllocationFailure();
    TestDrainIntoEmptySetKeepsOverflow();
    TestEveryRestoreAllocationFailure();
    std::cout << "wal_barrier_memory tests passed\n";
    return 0;
}
