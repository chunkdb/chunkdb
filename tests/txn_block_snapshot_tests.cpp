#include <cassert>
#include <chrono>
#include <string>

#include "chunkdb/chunk_store.hpp"
#include "txn_block_memory.hpp"
#include "txn_block_rows.hpp"
#include "txn_block_allocation_guard.hpp"
#include "txn_test_utils.hpp"

namespace {
using namespace chunkdb;
template <class F> void Refuse(F&& call) {
    bool refused = false;
    try { call(); } catch (const std::invalid_argument&) { refused = true; }
    catch (const TransactionConflictError&) { refused = true; }
    assert(refused);
}
void SnapshotRowsAndHistory() {
    for (const bool three : {false, true}) {
        test::ScopedTempDir directory("chunkdb-snapshot-one-row");
        auto config = txn_test::Config(directory.path(), DurabilityMode::kRelaxed, 8U);
        config.background_maintenance = false;
        config.geometry.dims = three ? 3U : 2U;
        config.geometry.chunk_depth_blocks = three ? 2U : 1U;
        config.schema = SingleBitsColumnSchema(8U);
        config.schema->columns.push_back({.id = 2U, .name = "text", .type = {ColumnKind::kText, kMaxVariableValueBytes},
            .nullable = true, .default_value = {}});
        config.schema->next_column_id = 3U;
        config.var_max_chunk_bytes = kVarMaxChunkBytesLimit;
        ChunkStore store(config);
        const ChunkCoord coord{0, 0, three ? -1 : 0};
        auto before = store.BeginTxnSnapshot(txn_test::kTxnDuration);
        const auto empty = store.ReadBlockAtSnapshot(*before, coord, 0U);
        assert(!empty.row.present && !empty.written && empty.version == 0U);
        // The whole-state API is only fixture setup; the row API must select
        // the small historical row before trying to copy the large current one.
        auto state = store.ReadChunkState(coord);
        state.presence_bitmap[0] = 1U; state.payload[0] = 9U;
        state.vars.Assign({2U, 0U}, std::vector<std::uint8_t>{'a'});
        (void)store.CommitTransaction(*before, {}, {{coord, std::move(state)}});
        auto snapshot = store.BeginTxnSnapshot(txn_test::kTxnDuration);
        const auto expected = store.ReadBlockAtSnapshot(*snapshot, coord, 0U);
        auto writer = store.BeginTxnSnapshot(txn_test::kTxnDuration);
        auto large = store.ReadChunkStateAt(*writer, coord);
        large.payload[0] = 10U;
        large.vars.Assign({2U, 0U}, std::vector<std::uint8_t>(2U * 1024U * 1024U, 'b'));
        (void)store.CommitTransaction(*writer, {}, {{coord, std::move(large)}});
        std::size_t local = 0U; std::atomic<std::size_t> global{0U};
        TxnBlockMemoryResource memory(local, global, 4096U, 4096U);
        {
            txn_block_allocation::Guard allocations(16U * 1024U);
            const auto actual = store.ReadBlockAtSnapshot(*snapshot, coord, 0U, &memory);
            assert(actual.row == expected.row && actual.version == expected.version && actual.written == expected.written);
            assert(actual.commit_time_ms == expected.commit_time_ms && local < 4096U);
        }
        assert(local == 0U && global == 0U);
        Refuse([&] { (void)store.ReadBlockAtSnapshot(*snapshot, coord, store.geometry().ChunkBlockCount()); });
        if (!three) Refuse([&] { (void)store.ReadBlockAtSnapshot(*snapshot, {0, 0, 1}, 0U); });
        test::ScopedTempDir foreign_directory("chunkdb-foreign-row");
        auto foreign_config = config; foreign_config.data_dir = foreign_directory.path(); ChunkStore foreign(foreign_config);
        Refuse([&] { (void)foreign.ReadBlockAtSnapshot(*snapshot, coord, 0U); });
        store.EndTxnSnapshot(*snapshot);
        Refuse([&] { (void)store.ReadBlockAtSnapshot(*snapshot, coord, 0U); });
    }
}
void LargeFixedChunkDoesNotCopy() {
    test::ScopedTempDir directory("chunkdb-large-fixed-row");
    auto config = txn_test::Config(directory.path(), DurabilityMode::kRelaxed, 65535U);
    config.geometry.chunk_width_blocks = 256U; config.geometry.chunk_height_blocks = 32U;
    config.background_maintenance = false; config.checkpoint_update_interval = 100000U;
    ChunkStore store(config);
    std::string bits(65535U, '0'); bits[0] = '1'; store.SetBlockBits(0, 0, bits);
    auto snapshot = store.BeginTxnSnapshot(txn_test::kTxnDuration);
    std::size_t local = 0U; std::atomic<std::size_t> global{0U};
    TxnBlockMemoryResource memory(local, global, 16384U, 16384U);
    {
        txn_block_allocation::Guard allocations(16384U);
        const auto row = store.ReadBlockAtSnapshot(*snapshot, {}, 0U, &memory);
        assert(row.row.present && row.row.cells.size() == 1U && row.row.cells[0].bytes.size() == 8192U);
        assert(row.row.cells[0].bytes[0] == 1U && local < 16384U);
    }
    assert(local == 0U && global == 0U);
    store.EndTxnSnapshot(*snapshot);
}
void EvictedHistoryRefuses() {
    test::ScopedTempDir directory("chunkdb-row-history-eviction");
    auto config = txn_test::Config(directory.path(), DurabilityMode::kRelaxed);
    config.background_maintenance = false; config.txn_history_bytes = 1U;
    ChunkStore store(config);
    txn_test::WriteCounter(store, {}, 1U);
    auto snapshot = store.BeginTxnSnapshot(txn_test::kTxnDuration);
    txn_test::WriteCounter(store, {}, 2U);
    Refuse([&] { (void)store.ReadBlockAtSnapshot(*snapshot, {}, 0U); });
}
}  // namespace
int main() { SnapshotRowsAndHistory(); LargeFixedChunkDoesNotCopy(); EvictedHistoryRefuses(); }
