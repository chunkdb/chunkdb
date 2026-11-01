#include <array>
#include <atomic>
#include <barrier>
#include <bit>
#include <cassert>
#include <cstdio>
#include <limits>
#include <memory_resource>
#include <memory>
#include <source_location>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "txn_block_deltas.hpp"
#include "txn_block_memory.hpp"
#include "txn_block_allocation_guard.hpp"

namespace {
using namespace chunkdb;
static_assert(!std::is_move_assignable_v<TxnBlockDelta>);
static_assert(!std::is_move_assignable_v<TxnBlockPlannedRow>);
static_assert(!std::is_move_assignable_v<TxnBlockPlannedChunk>);
static_assert(!std::is_move_assignable_v<TxnBlockRebasePlan>);
template <class F> void Reject(F&& call, const std::source_location& where = std::source_location::current()) {
    bool rejected = false;
    try { call(); } catch (const std::invalid_argument&) { rejected = true; }
    if (!rejected) std::fprintf(stderr, "missing admission refusal at %s:%u (%s)\n", where.file_name(), where.line(), where.function_name());
    assert(rejected);
}
template <class F> void Conflict(F&& call, TxnConflictReason expected, const std::source_location& where = std::source_location::current()) {
    bool rejected = false;
    try { call(); } catch (const TransactionConflictError& error) { rejected = error.reason() == expected; }
    if (!rejected) std::fprintf(stderr, "missing %s conflict at %s:%u (%s)\n", TxnConflictReasonName(expected), where.file_name(), where.line(), where.function_name());
    assert(rejected);
}
TableSchema Schema() {
    TableSchema schema;
    schema.next_column_id = 5U;
    schema.columns = {
        {.id = 1U, .name = "n", .type = {ColumnKind::kUnsigned, 8U}, .default_value = {}},
        {.id = 2U, .name = "f", .type = {ColumnKind::kFloat32, 32U}, .nullable = true, .default_value = {}},
        {.id = 3U, .name = "text", .type = {ColumnKind::kText, kMaxVariableValueBytes}, .nullable = true, .default_value = {}},
        {.id = 4U, .name = "bytes", .type = {ColumnKind::kBytes, kMaxVariableValueBytes}, .default_value = {}},
    };
    return schema;
}
Geometry MakeGeometry(bool three = false, TableSchema metadata = {}) {
    return Geometry({1U, 1U, 2U, 2U, 40U, static_cast<std::uint8_t>(three ? 3U : 2U), three ? 2U : 1U, 1U}, Schema(), metadata);
}
struct State {
    std::vector<std::uint8_t> payload, presence;
    ChunkVars vars;
    std::uint64_t revision = 7U;
    bool written = true;
    std::shared_ptr<const ChunkColumnState> metadata;
    explicit State(const Geometry& geometry)
        : payload(geometry.ChunkPayloadBytes(), 0U), presence((geometry.ChunkBlockCount() + 7U) / 8U, 0U) {}
    void Put(const Geometry& geometry, std::size_t index, const TxnBlockRow& row) {
        assert(row.present);
        presence[index / 8U] |= static_cast<std::uint8_t>(1U << (index % 8U));
        for (std::size_t i = 0U; i < row.cells.size(); ++i) {
            const auto& cell = row.cells[i]; const auto& column = geometry.layout().schema().columns[i];
            if (const auto* fixed = geometry.layout().FixedColumnAt(i)) {
                if (fixed->validity != ChunkLayout::kNoValidity) {
                    const auto byte = fixed->validity + index / 8U;
                    const auto mask = static_cast<std::uint8_t>(1U << (index % 8U));
                    if (cell.null) payload[byte] &= static_cast<std::uint8_t>(~mask);
                    else payload[byte] |= mask;
                }
                if (!cell.null) WriteValueBits(payload.data(), fixed->values * 8U + index * fixed->width, cell.bytes.data(), fixed->width);
            } else {
                const VarKey key{column.id, static_cast<std::uint32_t>(index)};
                if (cell.null || (!column.nullable && cell.bytes.empty())) (void)vars.Remove(key);
                else vars.Assign(key, cell.bytes);
            }
        }
    }
    TxnBlockCurrentChunk View(ChunkCoord coord = {}) const {
        return {coord, revision, written, payload, presence, &vars, metadata.get()};
    }
};
TxnBlockRow Row(const Geometry& geometry, std::uint64_t value,
    std::pmr::memory_resource* memory = std::pmr::get_default_resource()) {
    TxnBlockRow absent(memory); absent.schema_version = geometry.layout().schema().version;
    const std::array<ColumnAssignment, 1U> assignments{{{"n", value}}};
    return AssignTxnBlockRow(geometry.layout(), absent, assignments, memory);
}
TxnBlockSnapshot Capture(const Geometry& geometry, const State& state, std::size_t index,
    std::pmr::memory_resource* memory = std::pmr::get_default_resource()) {
    TxnBlockSnapshot snapshot(memory);
    snapshot.row = CaptureTxnBlockRow(geometry.layout(), state.payload, state.presence, state.vars, index, memory);
    snapshot.version = state.revision; snapshot.commit_time_ms = 100U; snapshot.written = state.written;
    return snapshot;
}
void RowsPreserveRawBits() {
    const auto geometry = MakeGeometry(); State state(geometry);
    auto row = Row(geometry, 9U);
    const auto assign = [&](ColumnValue value) {
        const std::array<ColumnAssignment, 1U> assignments{{{"f", std::move(value)}}};
        return AssignTxnBlockRow(geometry.layout(), row, assignments, row.resource());
    };
    auto negative_zero = assign(-0.0F); state.Put(geometry, 0U, negative_zero);
    assert(MatchesTxnBlockRow(geometry.layout(), state.payload, state.presence, state.vars, 0U, negative_zero));
    auto positive_zero = assign(0.0F);
    assert(!MatchesTxnBlockRow(geometry.layout(), state.payload, state.presence, state.vars, 0U, positive_zero));
    const float nan = std::bit_cast<float>(std::uint32_t{0x7FC01234U});
    auto payload_nan = assign(nan); state.Put(geometry, 0U, payload_nan);
    auto captured = Capture(geometry, state, 0U);
    assert(captured.row == payload_nan);
    auto other_nan = assign(std::bit_cast<float>(std::uint32_t{0x7FC01235U}));
    assert(!MatchesTxnBlockRow(geometry.layout(), state.payload, state.presence, state.vars, 0U, other_nan));
    const std::array<ColumnAssignment, 1U> empty{{{"text", std::string{}}}};
    auto empty_text = AssignTxnBlockRow(geometry.layout(), row, empty, row.resource());
    state.Put(geometry, 1U, empty_text);
    assert(!MatchesTxnBlockRow(geometry.layout(), state.payload, state.presence, state.vars, 1U, row));
    assert(Capture(geometry, state, 1U).row == empty_text);
    auto invalid = CloneTxnBlockRow(row, row.resource()); invalid.cells[0].bytes.push_back(0U);
    Reject([&] { ValidateTxnBlockRow(geometry.layout(), invalid); });
}
void JournalAndRebase() {
    for (const bool three : {false, true}) {
        const auto geometry = MakeGeometry(three); State current(geometry);
        auto original = Row(geometry, 1U); current.Put(geometry, 0U, original);
        auto other = Row(geometry, 2U); current.Put(geometry, 1U, other);
        const auto snapshot = Capture(geometry, current, 0U);
        std::size_t private_bytes = 0U; std::atomic<std::size_t> global{0U};
        TxnBlockMemoryResource memory(private_bytes, global, 128U * 1024U, 128U * 1024U);
        {
            TxnBlockDeltas journal(StoreId{1U}, geometry.layout(), &memory);
            const TxnBlockKey key{{-1, 2, three ? -3 : 0}, 0U};
            journal.Publish(journal.Prepare(key, snapshot, TxnBlockAction::kObserve));
            auto replacement = Row(geometry, 8U);
            journal.Publish(journal.Prepare(key, snapshot, TxnBlockAction::kReplace, &replacement));
            TxnBlockSnapshot private_observation;
            private_observation.row = CloneTxnBlockRow(replacement, private_observation.row.resource());
            private_observation.version = snapshot.version; private_observation.commit_time_ms = snapshot.commit_time_ms;
            private_observation.written = snapshot.written;
            journal.Publish(journal.Prepare(key, private_observation, TxnBlockAction::kReplace, &replacement));
            journal.Publish(journal.Prepare(key, private_observation, TxnBlockAction::kObserve));
            assert(journal.rows().begin()->second.before.row == original);
            assert(*journal.Read(key) == replacement);
            auto late = journal.Prepare(key, snapshot, TxnBlockAction::kDelete);
            journal.Publish(journal.Prepare(key, snapshot, TxnBlockAction::kReplace, &replacement));
            Reject([&] { journal.Publish(std::move(late)); });
            auto concurrent_other = Row(geometry, 99U); current.Put(geometry, 1U, concurrent_other); ++current.revision;
            const auto payload = current.payload, presence = current.presence;
            const auto variables = current.vars.Encode();
            const auto view = current.View(key.chunk);
            auto plan = PlanTxnBlockRebase(journal, StoreId{1U}, geometry, std::span(&view, 1U), kDefaultVarMaxChunkBytes);
            assert(plan.rows.size() == 1U && plan.rows[0].after == replacement);
            assert(plan.chunks.size() == 1U && !plan.chunks[0].physical_staging_bytes);
            assert(current.payload == payload && current.presence == presence && current.vars.Encode() == variables);
            auto conflict = Row(geometry, 3U); current.Put(geometry, 0U, conflict);
            Conflict([&] { (void)PlanTxnBlockRebase(journal, StoreId{1U}, geometry, std::array{current.View(key.chunk)}, kDefaultVarMaxChunkBytes); }, TxnConflictReason::kChunkChanged);
            current.Put(geometry, 0U, original); ++current.revision;
            auto aba = PlanTxnBlockRebase(journal, StoreId{1U}, geometry, std::array{current.View(key.chunk)}, kDefaultVarMaxChunkBytes);
            assert(aba.rows.size() == 1U);
            journal.Publish(journal.PreparePromotion(key.chunk, snapshot.version));
            assert(journal.rows().empty() && journal.promotions().size() == 1U && journal.Read(key) == nullptr);
            Conflict([&] { (void)PlanTxnBlockRebase(journal, StoreId{1U}, geometry, std::array{current.View(key.chunk)}, kDefaultVarMaxChunkBytes); }, TxnConflictReason::kChunkChanged);
            Reject([&] { (void)journal.Prepare(key, snapshot, TxnBlockAction::kObserve); });
        }
        assert(private_bytes == 0U && global == 0U && memory.allocated_bytes() == 0U);
    }
}
class FailingResource final : public std::pmr::memory_resource {
    void* do_allocate(std::size_t, std::size_t) override { throw std::bad_alloc(); }
    void do_deallocate(void*, std::size_t, std::size_t) override { assert(false); }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
};
void Accounting() {
    std::size_t local = 0U; std::atomic<std::size_t> global{0U};
    TxnBlockMemoryResource memory(local, global, 100U, 200U);
    auto* allocation = memory.allocate(100U);
    assert(local == 100U && global == 100U);
    Reject([&] { (void)memory.allocate(1U); });
    assert(local == 100U && global == 100U);
    memory.deallocate(allocation, 100U);
    FailingResource failure; TxnBlockMemoryResource failed(local, global, 100U, 100U, &failure);
    bool bad_alloc = false;
    try { (void)failed.allocate(20U); } catch (const std::bad_alloc&) { bad_alloc = true; }
    assert(bad_alloc && local == 0U && global == 0U);
    global.store(std::numeric_limits<std::size_t>::max() - 1U);
    TxnBlockMemoryResource overflow(local, global, 100U, std::numeric_limits<std::size_t>::max());
    Reject([&] { (void)overflow.allocate(2U); });
    assert(global == std::numeric_limits<std::size_t>::max() - 1U && local == 0U);
    global = 0U;
    std::barrier start(3), held(3), release(3);
    std::atomic<unsigned> admitted{0U};
    const auto contender = [&] {
        std::size_t bytes = 0U; TxnBlockMemoryResource budget(bytes, global, 80U, 100U);
        void* owned = nullptr;
        start.arrive_and_wait();
        try { owned = budget.allocate(80U); ++admitted; } catch (const std::invalid_argument&) {}
        held.arrive_and_wait(); release.arrive_and_wait();
        if (owned) budget.deallocate(owned, 80U);
        assert(bytes == 0U);
    };
    std::thread first(contender), second(contender);
    start.arrive_and_wait(); held.arrive_and_wait();
    assert(admitted == 1U && global == 80U);
    release.arrive_and_wait(); first.join(); second.join(); assert(global == 0U);
}
void FailedEditsAndLateConflict() {
    const auto geometry = MakeGeometry(); State state(geometry); auto row = Row(geometry, 1U); state.Put(geometry, 0U, row);
    auto snapshot = Capture(geometry, state, 0U);
    std::size_t private_bytes = 0U; std::atomic<std::size_t> global{0U};
    TxnBlockMemoryResource memory(private_bytes, global, 4096U, 4096U);
    {
        TxnBlockDeltas journal(StoreId{1U}, geometry.layout(), &memory);
        journal.Publish(journal.Prepare({{0, 0}, 0U}, snapshot, TxnBlockAction::kObserve));
        journal.Publish(journal.Prepare({{1, 0}, 0U}, snapshot, TxnBlockAction::kObserve));
        const auto charged = private_bytes;
        const auto remaining = 4096U - charged;
        auto* occupied = memory.allocate(remaining);
        Reject([&] { (void)journal.PreparePromotion({0, 0}, snapshot.version); });
        assert(journal.promotions().empty() && journal.rows().size() == 2U && private_bytes == 4096U);
        memory.deallocate(occupied, remaining);
        assert(private_bytes == charged && global == charged);
        auto large = CloneTxnBlockRow(row, std::pmr::get_default_resource()); large.cells[2].null = false; large.cells[2].bytes.resize(8192U, 'a');
        Reject([&] { (void)journal.Prepare({{0, 0}, 0U}, snapshot, TxnBlockAction::kReplace, &large); });
        assert(private_bytes == charged && global == charged && journal.Read({{0, 0}, 0U})->cells[0].bytes[0] == 1U);
        auto different = Row(geometry, 2U); State changed(geometry); changed.Put(geometry, 0U, different);
        const std::array views{state.View({0, 0}), changed.View({1, 0})};
        Conflict([&] { (void)PlanTxnBlockRebase(journal, StoreId{1U}, geometry, views, kDefaultVarMaxChunkBytes); }, TxnConflictReason::kChunkChanged);
        assert(private_bytes == charged && global == charged && state.payload[0] == 1U);
        Conflict([&] { (void)PlanTxnBlockRebase(journal, StoreId{2U}, geometry, views, kDefaultVarMaxChunkBytes); }, TxnConflictReason::kTableChanged);
        auto changed_schema = Schema(); changed_schema.columns[0].name = "changed";
        Geometry changed_geometry({1U, 1U, 2U, 2U, 40U}, changed_schema);
        Conflict([&] { (void)PlanTxnBlockRebase(journal, StoreId{1U}, changed_geometry, views, kDefaultVarMaxChunkBytes); }, TxnConflictReason::kTableChanged);
    }
    assert(private_bytes == 0U && global == 0U);
}
void WideVariablesStayBorrowed() {
    const auto geometry = MakeGeometry(); State state(geometry); auto row = Row(geometry, 1U); state.Put(geometry, 0U, row);
    // Three unrelated large values approach the whole-section ceiling; two tiny
    // edits must not materialize that section or claim a known physical peak.
    const std::vector<std::uint8_t> wide(16U * 1024U * 1024U - kVarEntryHeaderBytes, 'a');
    for (std::uint32_t i = 1U; i < 4U; ++i) { state.Put(geometry, i, row); state.vars.Assign({3U, i}, wide); }
    auto snapshot = Capture(geometry, state, 0U);
    std::size_t local = 0U; std::atomic<std::size_t> global{0U};
    TxnBlockMemoryResource memory(local, global, 16384U, 16384U);
    {
        TxnBlockDeltas journal(StoreId{1U}, geometry.layout(), &memory);
        const std::array<ColumnAssignment, 2U> assignments{{{"text", std::string("x")}, {"bytes", BytesValue{{1U}}}}};
        auto after = AssignTxnBlockRow(geometry.layout(), snapshot.row, assignments, &memory);
        journal.Publish(journal.Prepare({{}, 0U}, snapshot, TxnBlockAction::kReplace, &after));
        const auto* address = state.vars.Encode().data(); const auto size = state.vars.encoded_size();
        txn_block_allocation::Guard bounded(1024U * 1024U);
        auto plan = PlanTxnBlockRebase(journal, StoreId{1U}, geometry, std::array{state.View()}, kVarMaxChunkBytesLimit);
        assert(plan.rows.size() == 1U && plan.chunks[0].variables.size() == 2U);
        assert(plan.chunks[0].current_variable_data_capacity == state.vars.Encode().capacity());
        assert(!plan.chunks[0].physical_staging_bytes && plan.chunks[0].resulting_variable_bytes == size + 26U);
        assert(state.vars.Encode().data() == address && state.vars.encoded_size() == size && local < 16384U);
    }
    assert(local == 0U && global == 0U);
}
void InitializationAndDelete() {
    TableSchema metadata;
    metadata.next_column_id = 2U;
    metadata.columns = {{.id = 1U, .name = "owner", .type = {ColumnKind::kText, 1024U},
        .has_default = true, .default_value = std::vector<std::uint8_t>(100U, 'd')}};
    const auto geometry = MakeGeometry(false, metadata); State state(geometry); state.written = false; state.revision = 0U;
    auto snapshot = Capture(geometry, state, 0U); snapshot.commit_time_ms = 0U;
    TxnBlockDeltas journal(StoreId{1U}, geometry.layout(), std::pmr::get_default_resource());
    auto row = Row(geometry, 3U);
    journal.Publish(journal.Prepare({{}, 0U}, snapshot, TxnBlockAction::kReplace, &row));
    Reject([&] { (void)PlanTxnBlockRebase(journal, StoreId{1U}, geometry, std::array{state.View()}, 50U); });
    state.metadata = geometry.default_chunk_columns();
    Reject([&] { (void)PlanTxnBlockRebase(journal, StoreId{1U}, geometry, std::array{state.View()}, 50U); });
    auto plan = PlanTxnBlockRebase(journal, StoreId{1U}, geometry, std::array{state.View()}, 1024U);
    assert(plan.rows.size() == 1U && plan.chunks[0].resulting_metadata_variable_bytes == geometry.default_chunk_columns()->variable_bytes);
    auto required_metadata = metadata; required_metadata.columns[0].required = true;
    required_metadata.columns[0].has_default = false; required_metadata.columns[0].default_value.clear();
    const auto required = MakeGeometry(false, required_metadata);
    Reject([&] { (void)PlanTxnBlockRebase(journal, StoreId{1U}, required, std::array{state.View()}, 1024U); });
    state.Put(geometry, 0U, row); state.revision = 9U; state.written = true;
    state.metadata = std::make_shared<const ChunkColumnState>(AssignChunkColumns(metadata, nullptr, {{"owner", std::string("current")}}, true));
    auto present = Capture(geometry, state, 0U);
    TxnBlockDeltas deletion(StoreId{1U}, geometry.layout(), std::pmr::get_default_resource());
    deletion.Publish(deletion.Prepare({{}, 0U}, present, TxnBlockAction::kDelete));
    assert(!deletion.Read({{}, 0U})->present && deletion.rows().begin()->second.before.row == row);
    const auto metadata_bytes = state.metadata->bytes;
    auto removed = PlanTxnBlockRebase(deletion, StoreId{1U}, geometry, std::array{state.View()}, 1024U);
    assert(removed.rows.size() == 1U && !removed.rows[0].after.present && state.metadata->bytes == metadata_bytes);
    assert(removed.chunks[0].resulting_metadata_variable_bytes == state.metadata->variable_bytes);
    auto required_row_schema = Schema(); required_row_schema.columns[0].required = true;
    ChunkLayout required_layout(required_row_schema, 4U, 1024U);
    TxnBlockRow absent; absent.schema_version = 1U;
    Reject([&] { (void)AssignTxnBlockRow(required_layout, absent, {}, absent.resource()); });
    const std::array<ColumnAssignment, 1U> supplied{{{"n", std::uint64_t{3U}}}};
    assert(AssignTxnBlockRow(required_layout, absent, supplied, absent.resource()).present);
}
void PreparedOwnershipMoves() {
    const auto geometry = MakeGeometry(); State state(geometry); auto row = Row(geometry, 1U); state.Put(geometry, 0U, row);
    auto snapshot = Capture(geometry, state, 0U);
    TxnBlockDeltas journal(StoreId{1U}, geometry.layout(), std::pmr::get_default_resource());
    auto prepared = journal.Prepare({{}, 0U}, snapshot, TxnBlockAction::kObserve);
    auto transferred = std::move(prepared);
    Reject([&] { journal.Publish(std::move(prepared)); });
    assert(journal.rows().empty());
    journal.Publish(std::move(transferred)); assert(journal.rows().size() == 1U);
    auto promotion = journal.PreparePromotion({}, snapshot.version);
    auto moved_promotion = std::move(promotion);
    Reject([&] { journal.Publish(std::move(promotion)); });
    assert(journal.rows().size() == 1U && journal.promotions().empty());
    journal.Publish(std::move(moved_promotion)); assert(journal.rows().empty() && journal.promotions().size() == 1U);
}
void ResourceOwnershipCannotBeBorrowed() {
    const auto geometry = MakeGeometry();
    std::size_t first_bytes = 0U, second_bytes = 0U; std::atomic<std::size_t> global{0U};
    TxnBlockMemoryResource first(first_bytes, global, 4096U, 8192U), second(second_bytes, global, 4096U, 8192U);
    {
        auto left = Row(geometry, 1U, &first), right = Row(geometry, 2U, &second);
        const auto before_left = CloneTxnBlockRow(left, std::pmr::get_default_resource());
        const auto before_right = CloneTxnBlockRow(right, std::pmr::get_default_resource());
        const auto charged_left = first_bytes, charged_right = second_bytes;
        TxnBlockRow absent(&first); absent.schema_version = 1U;
        Reject([&] { absent = std::move(right); });
        assert(!absent.present && absent.cells.empty() && right == before_right && first_bytes == charged_left && second_bytes == charged_right);
        Reject([&] { left = std::move(right); });
        assert(left == before_left && right == before_right && first_bytes == charged_left && second_bytes == charged_right);
        Reject([&] { left.cells[0] = std::move(right.cells[0]); });
        assert(left == before_left && right == before_right && first_bytes == charged_left && second_bytes == charged_right);
        assert(global == charged_left + charged_right);
        TxnBlockSnapshot first_snapshot(&first), second_snapshot(&second);
        first_snapshot.row = Row(geometry, 3U, &first); second_snapshot.row = Row(geometry, 4U, &second);
        first_snapshot.version = 10U; second_snapshot.version = 20U;
        first_snapshot.commit_time_ms = 30U; second_snapshot.commit_time_ms = 40U;
        first_snapshot.written = true;
        const auto first_snapshot_row = CloneTxnBlockRow(first_snapshot.row, std::pmr::get_default_resource());
        const auto second_snapshot_row = CloneTxnBlockRow(second_snapshot.row, std::pmr::get_default_resource());
        const auto snapshot_first_bytes = first_bytes, snapshot_second_bytes = second_bytes;
        Reject([&] { first_snapshot = std::move(second_snapshot); });
        assert(first_snapshot.row == first_snapshot_row && second_snapshot.row == second_snapshot_row);
        assert(first_snapshot.version == 10U && second_snapshot.version == 20U);
        assert(first_snapshot.commit_time_ms == 30U && second_snapshot.commit_time_ms == 40U);
        assert(first_snapshot.written && !second_snapshot.written && first_bytes == snapshot_first_bytes && second_bytes == snapshot_second_bytes);
        TxnBlockPlannedVariable first_variable(&first), second_variable(&second);
        first_variable.key = {1U, 2U}; second_variable.key = {3U, 4U};
        first_variable.value.push_back(9U); second_variable.value.push_back(8U); second_variable.removed = true;
        const auto variable_first_bytes = first_bytes, variable_second_bytes = second_bytes;
        Reject([&] { first_variable = std::move(second_variable); });
        assert((first_variable.key == VarKey{1U, 2U}) && (second_variable.key == VarKey{3U, 4U}));
        assert(!first_variable.removed && second_variable.removed && first_variable.value[0] == 9U && second_variable.value[0] == 8U);
        assert(first_bytes == variable_first_bytes && second_bytes == variable_second_bytes && global == first_bytes + second_bytes);
    }
    assert(first_bytes == 0U && second_bytes == 0U && global == 0U);
}
}  // namespace

int main() {
    RowsPreserveRawBits(); JournalAndRebase(); Accounting(); FailedEditsAndLateConflict(); WideVariablesStayBorrowed(); InitializationAndDelete(); PreparedOwnershipMoves(); ResourceOwnershipCannotBeBorrowed();
}
