#include <cassert>
#include <functional>
#include <thread>

#include "chunk_store_internal.hpp"
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
}

int main() {
    AtomicTransformOwnsWholeState();
    RequiredMetadataMustBeExplicit();
    NeverWrittenDefaultIsNotPriorQuota();
}
