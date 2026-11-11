#include <algorithm>
#include <cassert>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_columns.hpp"
#include "chunkdb/file_layout.hpp"
#include "test_utils.hpp"
#include "txn_test_utils.hpp"

namespace chunkdb {
struct FeatureActivationTestAccess {
    static bool MutationOwnsLock(ChunkStore& store, ChunkCoord coord) {
        const auto chunk = store.GetOrLoadRegularChunk(coord);
        bool acquired = false;
        std::thread probe([&] {
            acquired = chunk->mutex.try_lock();
            if (acquired) chunk->mutex.unlock();
        });
        probe.join();
        return !acquired;
    }
};
}

namespace {
using namespace chunkdb;
void Reject(const std::function<void()>& action) {
    bool refused = false;
    try { action(); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}
StoreConfig Config(const std::filesystem::path& path) {
    auto config = txn_test::Config(path, DurabilityMode::kFsyncWal, 8U);
    config.background_maintenance = false;
    config.checkpoint_update_interval = 1000U;
    return config;
}
void AtomicTransformOwnsWholeState() {
    test::ScopedTempDir directory("chunkdb-atomic-transform");
    const auto config = Config(directory.path());
    std::uint64_t revision = 0U;
    {
        ChunkStore store(config);
        const auto first = store.TransformChunkState({0, 0}, {}, true, [&](ChunkState& state) {
            assert(FeatureActivationTestAccess::MutationOwnsLock(store, {0, 0}));
            assert(!state.written && state.version == 0U);
            state.payload[0] = 7U;
            state.presence_bitmap[0] = 1U;
            state.version = 999999U;
            state.commit_time_ms = 1U;
        });
        assert(first.ok && first.version != 0U && first.version != 999999U);
        const auto before = store.ReadChunkState(0, 0);
        assert(before.payload[0] == 7U && before.commit_time_ms != 1U);
        bool called = false;
        assert(!store.TransformChunkState({0, 0}, {}, true, [&](auto&) { called = true; }).ok);
        assert(!store.TransformChunkState({0, 0}, first.version + 1U, false, [&](auto&) { called = true; }).ok);
        assert(!called);
        Reject([&] { (void)store.TransformChunkState({0, 0}, {}, false, [&](ChunkState& state) {
            state.payload[0] = 99U;
            throw std::invalid_argument("invalid patch");
        }); });
        assert(store.ReadChunkState(0, 0).version == first.version);
        assert(store.ReadChunkState(0, 0).payload[0] == 7U);
        const auto next = store.TransformChunkState({0, 0}, first.version, false, [&](ChunkState& state) {
            state.payload[1] = 11U;
            state.presence_bitmap[0] |= 2U;
        });
        assert(next.ok && next.version > first.version);
        revision = next.version;
    }
    ChunkStore reopened(config);
    const auto state = reopened.ReadChunkState(0, 0);
    assert(state.version == revision && state.payload[0] == 7U && state.payload[1] == 11U);
}
void RequiredMetadataMustBeExplicit() {
    test::ScopedTempDir directory("chunkdb-transform-required");
    auto config = Config(directory.path());
    config.chunk_schema = TableSchema{.next_column_id = 2U, .columns = {
        {.id = 1U, .name = "owner", .type = {ColumnKind::kUnsigned, 8U}, .required = true, .default_value = {}}}};
    ChunkStore store(config);
    Reject([&] { (void)store.TransformChunkState({0, 0}, {}, false, [](ChunkState& state) {
        state.payload[0] = 1U; state.presence_bitmap[0] = 1U;
    }); });
    const auto result = store.TransformChunkState({0, 0}, {}, false, [&](ChunkState& state) {
        state.chunk_columns = std::make_shared<const ChunkColumnState>(AssignChunkColumns(
            store.geometry().chunk_schema(), nullptr, {{"owner", std::uint64_t{0U}}}, true));
    });
    assert(result.ok && result.version != 0U);
    assert(store.TransformChunkState({0, 0}, {}, true, [](auto&) {}).ok);
}
void NeverWrittenDefaultIsNotPriorQuota() {
    test::ScopedTempDir directory("chunkdb-transform-first-quota");
    auto config = Config(directory.path());
    config.var_max_chunk_bytes = 20U;
    config.chunk_schema = TableSchema{.next_column_id = 2U, .columns = {
        {.id = 1U, .name = "label", .type = {ColumnKind::kText, 128U}, .nullable = true,
         .has_default = true, .default_value = std::vector<std::uint8_t>(16U, 'x')}}};
    ChunkStore store(config);
    auto state = store.ReadChunkState(0, 0);
    state.chunk_columns = store.geometry().default_chunk_columns();
    assert(!state.written && state.chunk_columns->variable_bytes > config.var_max_chunk_bytes);
    state.payload[0] = 1U; state.presence_bitmap[0] = 1U;
    Reject([&] { (void)store.WriteChunkState({0, 0}, state, {}); });
    Reject([&] { (void)store.TransformChunkState({0, 0}, {}, false, [&](auto& target) { target = state; }); });
    auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(30));
    Reject([&] { (void)store.CommitTransaction(*snapshot, {}, {{{0, 0}, state}}); });
    store.EndTxnSnapshot(*snapshot);
    assert(!store.ReadChunkState(0, 0).written);
}
// Counts the file and directory syncs made while it exists.
class SyncCount {
  public:
    SyncCount() {
        SetFileSyncObserverForTests([this](const std::filesystem::path& path) { Note(path); });
        SetDirectorySyncObserverForTests([this](const std::filesystem::path& path) { Note(path); });
    }
    ~SyncCount() {
        SetFileSyncObserverForTests({});
        SetDirectorySyncObserverForTests({});
    }
    SyncCount(const SyncCount&) = delete;
    SyncCount& operator=(const SyncCount&) = delete;
    [[nodiscard]] std::size_t Take() {
        std::lock_guard lock(mutex_);
        const auto count = paths_.size();
        paths_.clear();
        return count;
    }
    [[nodiscard]] bool Synced(const std::filesystem::path& path) {
        std::lock_guard lock(mutex_);
        return std::any_of(paths_.begin(), paths_.end(), [&](const std::filesystem::path& synced) {
            std::error_code ec;
            return synced == path || (std::filesystem::equivalent(synced, path, ec) && !ec);
        });
    }
    // Whether `path` was synced since the last Take, forgetting every sync.
    [[nodiscard]] bool TakeSynced(const std::filesystem::path& path) {
        const bool synced = Synced(path);
        (void)Take();
        return synced;
    }

  private:
    void Note(const std::filesystem::path& path) {
        std::lock_guard lock(mutex_);
        paths_.push_back(path);
    }
    std::mutex mutex_;
    std::vector<std::filesystem::path> paths_;
};
TableSchema OwnerColumns() {
    return TableSchema{.next_column_id = 2U, .columns = {
        {.id = 1U, .name = "owner", .type = {ColumnKind::kUnsigned, 8U}, .nullable = true, .default_value = {}}}};
}
std::shared_ptr<const ChunkColumnState> Owner(const ChunkStore& store, std::uint64_t owner) {
    return std::make_shared<const ChunkColumnState>(AssignChunkColumns(
        store.geometry().chunk_schema(), nullptr, {{"owner", owner}}, true));
}
// A relaxed write without a condition is applied in memory, a never-written
// chunk's first write included (docs/DURABILITY_CONTRACT.md); only IF
// VERSION and IF ABSENT publish a durable decision.
void RelaxedUnconditionalWritesSyncNothing() {
    test::ScopedTempDir directory("chunkdb-relaxed-first-write");
    auto config = Config(directory.path());
    config.durability_mode = DurabilityMode::kRelaxed;
    config.chunk_schema = OwnerColumns();
    std::uint64_t first = 0U, empty = 0U, second = 0U;
    {
        ChunkStore store(config);
        // The store reserves its first range of versions durably on its
        // first write; that clock bookkeeping is not the write path.
        auto warm = store.ReadChunkState(9, 9);
        warm.presence_bitmap[0] = 1U;
        assert(store.WriteChunkState({9, 9}, warm, {}).ok);
        SyncCount syncs;
        auto state = store.ReadChunkState(0, 0);
        assert(!state.written);
        state.payload[0] = 7U;
        state.presence_bitmap[0] = 1U;
        state.chunk_columns = Owner(store, 5U);
        const auto written = store.WriteChunkState({0, 0}, state, {});
        assert(written.ok && written.version != 0U);
        first = written.version;
        // An unchanged first write still gives the chunk its identity.
        const auto unchanged = store.WriteChunkState({1, 0}, store.ReadChunkState(1, 0), {});
        assert(unchanged.ok && unchanged.version != 0U && store.ReadChunkState(1, 0).written);
        empty = unchanged.version;
        // Without chunk columns a first write takes their defaults.
        auto plain = store.ReadChunkState(2, 0);
        plain.payload[0] = 9U;
        plain.presence_bitmap[0] = 1U;
        const auto defaults = store.WriteChunkState({2, 0}, plain, {});
        assert(defaults.ok && defaults.version != 0U);
        second = defaults.version;
        state.payload[0] = 8U;
        state.chunk_columns = Owner(store, 7U);
        const auto again = store.WriteChunkState({0, 0}, state, {});
        assert(again.ok && again.version > first);
        first = again.version;
        assert(syncs.Take() == 0U);

        // The conditional writes, and transformations (metadata and
        // projected SET CHUNK), keep their durable decision: they publish an
        // intent, and its directory is synced.
        const auto intents = directory.path() / kConditionalIntentDirName;
        assert(store.TransformChunkState({4, 0}, {}, false, [&](ChunkState& target) {
            target.chunk_columns = Owner(store, 3U);
        }).ok);
        assert(syncs.TakeSynced(intents));
        assert(store.WriteChunkState({3, 0}, state, {}, /*if_absent=*/true).ok);
        assert(syncs.TakeSynced(intents));
        const auto expected = store.WriteChunkState({0, 0}, state, first);
        assert(expected.ok && expected.version == first);
        state.payload[0] = 10U;
        const auto versioned = store.WriteChunkState({0, 0}, state, first);
        assert(versioned.ok && versioned.version > first && syncs.TakeSynced(intents));
        first = versioned.version;

        // A barrier makes the first writes durable.
        store.WalBarrier();
        assert(syncs.Synced(ChunkWalPath(directory.path(), store.geometry(), {1, 0})));
        assert(syncs.Synced(ChunkWalPath(directory.path(), store.geometry(), {2, 0})));
    }
    ChunkStore reopened(config);
    const auto zero = reopened.ReadChunkState(0, 0);
    assert(zero.written && zero.version == first && zero.payload[0] == 10U);
    assert(zero.chunk_columns && zero.chunk_columns->bytes == Owner(reopened, 7U)->bytes);
    const auto one = reopened.ReadChunkState(1, 0);
    assert(one.written && one.version == empty);
    const auto two = reopened.ReadChunkState(2, 0);
    assert(two.written && two.version == second && two.payload[0] == 9U);
    assert(two.chunk_columns && two.chunk_columns->bytes == reopened.geometry().default_chunk_columns()->bytes);
}
// A first write that fails before its commit leaves the chunk never
// written: no state, no chunk columns, no coordinate, no WAL, and a restart
// finds nothing either.
void FailedFirstWriteStaysNeverWritten() {
    test::ScopedTempDir directory("chunkdb-failed-first-write");
    auto config = Config(directory.path());
    config.chunk_schema = OwnerColumns();
    const auto refused = [](ChunkStore& store, const std::function<void()>& write) {
        bool failed = false;
        {
            txn_test::ScopedEnv failpoint("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
            try { write(); } catch (const std::runtime_error&) { failed = true; }
        }
        assert(failed);
        const auto after = store.ReadChunkState(0, 0);
        assert(!after.written && after.version == 0U && after.payload[0] == 0U);
        assert(after.presence_bitmap[0] == 0U && after.chunk_columns == nullptr);
        assert(store.ScanChunkIdentities(false, {}, 16U).entries.empty());
        assert(store.ScanPopulatedChunks(false, {}, 16U).coords.empty());
        // The refused frame left nothing a restart could replay.
        assert(!std::filesystem::exists(ChunkWalPath(store.data_dir(), store.geometry(), {0, 0})));
    };
    {
        ChunkStore store(config);
        auto state = store.ReadChunkState(0, 0);
        state.payload[0] = 7U;
        state.presence_bitmap[0] = 1U;
        state.chunk_columns = Owner(store, 5U);
        refused(store, [&] { (void)store.WriteChunkState({0, 0}, state, {}); });
        refused(store, [&] { (void)store.TransformChunkState({0, 0}, {}, false, [&](ChunkState& target) { target = state; }); });
    }
    std::uint64_t version = 0U;
    {
        ChunkStore reopened(config);
        assert(!reopened.ReadChunkState(0, 0).written);
        assert(reopened.ScanChunkIdentities(false, {}, 16U).entries.empty());
        // A different state, so a refused frame still on disk would show.
        auto state = reopened.ReadChunkState(0, 0);
        state.payload[0] = 8U;
        state.presence_bitmap[0] = 3U;
        state.chunk_columns = Owner(reopened, 6U);
        const auto written = reopened.WriteChunkState({0, 0}, state, {});
        assert(written.ok && written.version != 0U);
        version = written.version;
        const auto page = reopened.ScanChunkIdentities(false, {}, 16U);
        assert(page.entries.size() == 1U && page.entries[0].version == version);
    }
    ChunkStore reopened(config);
    const auto state = reopened.ReadChunkState(0, 0);
    assert(state.written && state.version == version && state.payload[0] == 8U && state.presence_bitmap[0] == 3U);
    assert(state.chunk_columns && state.chunk_columns->bytes == Owner(reopened, 6U)->bytes);
}
}

int main() {
    AtomicTransformOwnsWholeState();
    RequiredMetadataMustBeExplicit();
    NeverWrittenDefaultIsNotPriorQuota();
    RelaxedUnconditionalWritesSyncNothing();
    FailedFirstWriteStaysNeverWritten();
}
