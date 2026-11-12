#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>

#include "txn_block_stage.hpp"
#include "txn_block_stage_allocation_guard.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::detail;

struct GlobalCount {
    std::atomic<std::size_t> bytes{0U};
    std::size_t limit = 64U * 1024U * 1024U;
};
// The explicit account owns both counters independently of a session/engine.
// SessionTransaction integration is deliberately not simulated by a raw pointer.
struct Account final : ChunkStorageAccount {
    std::shared_ptr<GlobalCount> global = std::make_shared<GlobalCount>();
    std::size_t bytes = 0U, calls = 0U, fail_at = 0U;
    std::size_t limit = 64U * 1024U * 1024U;
    std::size_t peak = 0U;
    const std::atomic<std::size_t>* transfer_counter = nullptr;
    std::size_t transfer_expected = 0U, transfer_releases = 0U;
    void Reserve(std::size_t amount) override {
        ++calls;
        if (fail_at == calls) throw std::bad_alloc();
        if (bytes > limit || amount > limit - bytes) throw std::invalid_argument("stage private quota");
        auto current = global->bytes.load();
        for (;;) {
            if (current > global->limit || amount > global->limit - current) throw std::invalid_argument("stage shared quota");
            if (global->bytes.compare_exchange_weak(current, current + amount)) break;
        }
        bytes += amount;
        peak = std::max(peak, bytes);
    }
    void Release(std::size_t amount) noexcept override {
        if (transfer_counter) {
            // Only an already captured stable atomic is read from this callback.
            assert(transfer_counter->load() == transfer_expected);
            ++transfer_releases;
        }
        assert(amount <= bytes);
        bytes -= amount;
        const auto before = global->bytes.fetch_sub(amount);
        assert(amount <= before);
        (void)before;
    }
    ~Account() override { assert(bytes == 0U); }
};
struct ForeignUpstream final : std::pmr::memory_resource {
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        return std::pmr::new_delete_resource()->allocate(bytes, alignment);
    }
    void do_deallocate(void* memory, std::size_t bytes, std::size_t alignment) override {
        std::pmr::new_delete_resource()->deallocate(memory, bytes, alignment);
    }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
};

TableSchema BlockSchema() {
    TableSchema schema;
    schema.next_column_id = 5U;
    schema.columns = {
        {.id = 1U, .name = "n", .type = {ColumnKind::kUnsigned, 3U}, .default_value = {}},
        {.id = 2U, .name = "f", .type = {ColumnKind::kFloat32, 32U}, .nullable = true, .default_value = {}},
        {.id = 3U, .name = "text", .type = {ColumnKind::kText, 16384U}, .nullable = true, .default_value = {}},
        {.id = 4U, .name = "bytes", .type = {ColumnKind::kBytes, 16384U}, .default_value = {}},
    };
    return schema;
}
TableSchema Metadata(bool required = false) {
    TableSchema schema;
    schema.next_column_id = 2U;
    schema.columns = {{.id = 1U, .name = "owner", .type = {ColumnKind::kText, 16384U},
        .required = required, .has_default = !required,
        .default_value = required ? std::vector<std::uint8_t>{} : std::vector<std::uint8_t>{'b', 'a', 's', 'e'}}};
    return schema;
}
Geometry Shape(bool three = false, TableSchema blocks = BlockSchema(), TableSchema metadata = Metadata()) {
    return Geometry({1U, 1U, 2U, 2U, 35U, static_cast<std::uint8_t>(three ? 3U : 2U), three ? 2U : 1U, 1U},
        std::move(blocks), std::move(metadata));
}
TxnBlockRow Row(const Geometry& geometry, std::uint64_t n, std::string text, bool full = false) {
    TxnBlockRow absent;
    absent.schema_version = geometry.layout().schema().version;
    std::vector<ColumnAssignment> values{{"n", n}, {"text", std::move(text)}};
    if (full) {
        values.push_back({"f", std::bit_cast<float>(std::uint32_t{0x7FC01234U})});
        values.push_back({"bytes", BytesValue{{1U, 2U, 3U}}});
    }
    return AssignTxnBlockRow(geometry.layout(), absent, values, absent.resource());
}
struct State {
    ChunkCoord coord;
    ChunkState state;
    std::size_t present = 0U;
    explicit State(const Geometry& geometry, ChunkCoord point = {}, bool written = true) : coord(point) {
        state.payload.resize(geometry.ChunkPayloadBytes(), 0U);
        state.presence_bitmap.resize((geometry.ChunkBlockCount() + 7U) / 8U, 0U);
        state.chunk_columns = geometry.default_chunk_columns();
        state.version = written ? 7U : 0U; state.commit_time_ms = written ? 100U : 0U; state.written = written;
    }
    void Put(const Geometry& geometry, std::size_t index, const TxnBlockRow& row) {
        assert(row.present);
        const auto mask = static_cast<std::uint8_t>(1U << (index % 8U));
        if ((state.presence_bitmap[index / 8U] & mask) == 0U) ++present;
        state.presence_bitmap[index / 8U] |= mask;
        for (std::size_t i = 0U; i < row.cells.size(); ++i) {
            const auto& cell = row.cells[i]; const auto& column = geometry.layout().schema().columns[i];
            if (const auto* fixed = geometry.layout().FixedColumnAt(i)) {
                if (fixed->validity != ChunkLayout::kNoValidity) {
                    auto& byte = state.payload[fixed->validity + index / 8U];
                    if (cell.null) byte &= static_cast<std::uint8_t>(~mask); else byte |= mask;
                }
                const std::vector<std::uint8_t> zero((fixed->width + 7U) / 8U, 0U);
                WriteValueBits(state.payload.data(), fixed->values * 8U + index * fixed->width,
                    cell.null ? zero.data() : cell.bytes.data(), fixed->width);
            } else {
                const VarKey key{column.id, static_cast<std::uint32_t>(index)};
                if (cell.null || (!column.nullable && cell.bytes.empty())) (void)state.vars.Remove(key);
                else state.vars.Assign(key, cell.bytes);
            }
        }
    }
    TxnBlockMutableChunk View() {
        return {coord, state.payload, state.presence_bitmap, &state.vars, &state.chunk_columns,
            &state.version, &state.commit_time_ms, &state.written, &present};
    }
    TxnBlockSnapshot Snapshot(const Geometry& geometry, std::size_t index = 0U) const {
        TxnBlockSnapshot snapshot;
        snapshot.row = CaptureTxnBlockRow(geometry.layout(), state.payload, state.presence_bitmap,
            state.vars, index, snapshot.row.resource());
        snapshot.version = state.version; snapshot.commit_time_ms = state.commit_time_ms; snapshot.written = state.written;
        return snapshot;
    }
};
struct Saved {
    ChunkState state;
    std::size_t present;
    std::array<void*, 2U> buffers;
    std::array<ChunkStoragePolicy*, 2U> policies;
    explicit Saved(State& source) : state(source.state), present(source.present), buffers(ChunkVarsStorageAccess::Buffers(source.state.vars)) {
        for (std::size_t i = 0U; i < buffers.size(); ++i) policies[i] = ChunkStoragePolicyAccess::Inspect(buffers[i]).policy;
    }
    void Same(State& source) const {
        assert(source.state.version == state.version && source.state.commit_time_ms == state.commit_time_ms && source.state.written == state.written);
        assert(source.state.payload == state.payload && source.state.presence_bitmap == state.presence_bitmap && source.state.vars == state.vars);
        assert(source.state.chunk_columns == state.chunk_columns && source.present == present);
        const auto actual = ChunkVarsStorageAccess::Buffers(source.state.vars);
        assert(actual == buffers);
        for (std::size_t i = 0U; i < buffers.size(); ++i)
            assert(ChunkStoragePolicyAccess::Inspect(actual[i]).policy == policies[i]);
    }
};
struct Scenario {
    Geometry geometry;
    State first, second;
    TxnBlockDeltas journal;
    TxnBlockRow after_first, after_second, unrelated;
    explicit Scenario(bool three = false)
        : geometry(Shape(three)), first(geometry), second(geometry, {1, 0, three ? -1 : 0}),
          journal(StoreId{1U}, geometry.layout(), std::pmr::get_default_resource()),
          after_first(Row(geometry, 3U, std::string(4096U, 'x'), true)),
          after_second(Row(geometry, 6U, std::string(3072U, 'y'), true)), unrelated(Row(geometry, 5U, "side")) {
        first.Put(geometry, 0U, Row(geometry, 1U, "a")); second.Put(geometry, 0U, Row(geometry, 2U, "b"));
        auto a = first.Snapshot(geometry), b = second.Snapshot(geometry);
        journal.Publish(journal.Prepare({first.coord, 0U}, a, TxnBlockAction::kReplace, &after_first));
        journal.Publish(journal.Prepare({second.coord, 0U}, b, TxnBlockAction::kReplace, &after_second));
        first.Put(geometry, 1U, unrelated);
        first.state.chunk_columns = std::make_shared<const ChunkColumnState>(
            AssignChunkColumns(geometry.chunk_schema(), first.state.chunk_columns.get(), {{"owner", std::string("current")}}, false));
    }
    std::array<TxnBlockMutableChunk, 2U> Views() { return {first.View(), second.View()}; }
    PreparedTxnBlockStage Prepare(const std::shared_ptr<Account>& account, const std::shared_ptr<TxnHistory>& history) {
        return PreparedTxnBlockStage::Prepare(journal, StoreId{1U}, geometry, Views(), history, account, kVarMaxChunkBytesLimit);
    }
};

void ZeroExternalCreditUpstreamOrdering() {
    const auto geometry = Shape(); State state(geometry);
    TxnBlockRow absent; absent.schema_version = geometry.layout().schema().version;
    const std::array<ColumnAssignment, 1U> assignments{{{"n", std::uint64_t{1U}}}};
    const auto original = AssignTxnBlockRow(geometry.layout(), absent, assignments, absent.resource());
    state.Put(geometry, 0U, original);
    assert(state.state.vars.encoded_size() == 0U && ChunkVarsStorageAccess::BufferAllocationBytes(state.state.vars) == 0U);
    assert((ChunkVarsStorageAccess::Policies(state.state.vars) == std::array<ChunkStoragePolicy*, 2U>{}));
    Saved saved(state);
    const auto before = state.Snapshot(geometry);
    const auto after = Row(geometry, 2U, "later", true);
    TxnBlockDeltas journal(StoreId{1U}, geometry.layout(), std::pmr::get_default_resource());
    journal.Publish(journal.Prepare({{}, 0U}, before, TxnBlockAction::kReplace, &after));
    auto account = std::make_shared<Account>(); auto history = std::make_shared<TxnHistory>(1024U * 1024U);
    const std::array views{state.View()};
    const auto prepare = [&] {
        return PreparedTxnBlockStage::Prepare(journal, StoreId{1U}, geometry, views, history, account, kVarMaxChunkBytesLimit);
    };
    auto& requests = txn_block_allocation::upstream;
    requests.Start(account->bytes, account->global->bytes, 0U, true);
    { auto stage = prepare(); assert(!stage.empty() && account->bytes == requests.live); }
    const auto calls = requests.count;
    assert(calls > 10U && account->bytes == 0U && requests.live == 0U);
    requests.Pause(); saved.Same(state);
    for (std::size_t ordinal = 1U; ordinal <= calls; ++ordinal) {
        requests.Start(account->bytes, account->global->bytes, ordinal, true);
        bool failed = false;
        try { (void)prepare(); } catch (const std::bad_alloc&) { failed = true; }
        assert(failed && account->bytes == 0U && account->global->bytes == 0U && requests.live == 0U);
        requests.Pause(); saved.Same(state);
    }
}

void ActualUpstreamAdmissionAndFailures() {
    Scenario scenario;
    auto account = std::make_shared<Account>();
    auto original_policy = ChunkStoragePolicy::Create(account);
    scenario.first.state.vars = ChunkVarsStorageAccess::Clone(scenario.first.state.vars, original_policy);
    original_policy->TransferToRegularStorage();
    Saved first(scenario.first), second(scenario.second);
    const auto external = original_policy->allocation_bytes() + ChunkVarsStorageAccess::BufferAllocationBytes(scenario.second.state.vars);
    auto history = std::make_shared<TxnHistory>(1024U * 1024U);
    auto& requests = txn_block_allocation::upstream;
    requests.Start(account->bytes, account->global->bytes);
    {
        auto stage = scenario.Prepare(account, history);
        // Requests come from replacement new, not Reserve call ordinals or vector capacity.
        assert(requests.count > 10U && account->bytes == requests.live + external);
    }
    const auto actual_calls = requests.count;
    assert(requests.live == 0U && account->bytes == 0U && account->global->bytes == 0U);
    requests.Pause(); first.Same(scenario.first); second.Same(scenario.second);
    for (std::size_t ordinal = 1U; ordinal <= actual_calls; ++ordinal) {
        requests.Start(account->bytes, account->global->bytes, ordinal);
        bool failed = false;
        try { (void)scenario.Prepare(account, history); } catch (const std::bad_alloc&) { failed = true; }
        assert(failed && requests.live == 0U && account->bytes == 0U && account->global->bytes == 0U);
        requests.Pause(); first.Same(scenario.first); second.Same(scenario.second);
    }
    std::atomic<std::uint64_t> clock{8U};
    auto snapshot = history->Register(history, clock, std::chrono::steady_clock::now() + std::chrono::hours(1));
    requests.Start(account->bytes, account->global->bytes);
    {
        auto stage = scenario.Prepare(account, history);
        assert(account->bytes == requests.live + external);
        // Preserve the preparation ledger while ownership changes to history.
        requests.Pause(); stage.BindHistoryTag(8U);
        txn_block_allocation::Guard disabled(0U); stage.Apply(); stage.Commit();
        std::size_t retained = 0U;
        for (const auto coord : {scenario.first.coord, scenario.second.coord}) {
            assert(history->VisitAt(*snapshot, coord, [&](const TxnKeptState& kept) {
                const auto view = kept.View();
                const std::array ids{requests.Contains(&kept), requests.Contains(view.payload.data()),
                    requests.Contains(view.presence_bitmap.data()), requests.Contains(view.metadata_bytes.data()),
                    requests.Contains(kept.physical->policy.get())};
                std::size_t actual = 0U;
                for (std::size_t i = 0U; i < ids.size(); ++i) {
                    for (std::size_t j = 0U; j < i; ++j) assert(ids[i] != ids[j]);
                    actual += requests.requests[ids[i]].bytes;
                }
                const auto buffers = ChunkVarsStorageAccess::BufferAllocationBytes(view.vars);
                auto* original = ChunkStoragePolicyAccess::Inspect(view.vars.Encode().data()).policy;
                const auto control = coord == scenario.first.coord ? ChunkStoragePolicyAccess::ControlBytes() :
                    requests.requests[requests.Contains(original)].bytes;
                assert(control == ChunkStoragePolicyAccess::ControlBytes());
                actual += buffers + control;
                assert(kept.bytes == actual);
                retained += actual;
            }));
        }
        assert(history->KeptBytes() == retained);
    }
    assert(account->bytes == 0U && account->global->bytes == 0U);
    history->End(*snapshot);
    assert(history->KeptBytes() == ChunkStoragePolicyAccess::ControlBytes());
    original_policy = {};
    assert(history->KeptBytes() == 0U);
    // End this observation; installed regular buffers retain normal ownership.
    requests.live = 0U;
}

std::size_t ObserveHistoryTransfer(std::size_t expected) {
    const auto geometry = Shape(); State state(geometry);
    state.Put(geometry, 0U, Row(geometry, 1U, "original", true));
    const auto original_buffers = ChunkVarsStorageAccess::BufferAllocationBytes(state.state.vars);
    const auto before = state.Snapshot(geometry);
    const auto after = Row(geometry, 2U, "replacement", true);
    TxnBlockDeltas journal(StoreId{1U}, geometry.layout(), std::pmr::get_default_resource());
    journal.Publish(journal.Prepare({{}, 0U}, before, TxnBlockAction::kReplace, &after));
    auto account = std::make_shared<Account>(); auto history = std::make_shared<TxnHistory>(1024U * 1024U);
    // Shared history ownership spans every callback using this captured atomic.
    const auto& destination = history->KeptBytesCounter();
    std::atomic<std::uint64_t> clock{8U};
    auto snapshot = history->Register(history, clock, std::chrono::steady_clock::now() + std::chrono::hours(1));
    auto& requests = txn_block_allocation::upstream;
    std::size_t actual = 0U;
    requests.Start(account->bytes, account->global->bytes);
    {
        const std::array views{state.View()};
        auto stage = PreparedTxnBlockStage::Prepare(journal, StoreId{1U}, geometry, views, history, account, kVarMaxChunkBytesLimit);
        assert(account->bytes == requests.live + original_buffers);
        requests.Pause(); stage.BindHistoryTag(8U);
        txn_block_allocation::Guard disabled(0U); stage.Apply();
        if (expected != 0U) {
            assert(destination.load() == 0U);
            account->transfer_expected = expected;
            account->transfer_counter = &destination;
        }
        stage.Commit();
        assert(history->VisitAt(*snapshot, {}, [&](const TxnKeptState& kept) {
            const auto view = kept.View();
            const auto* original_policy = ChunkStoragePolicyAccess::Inspect(view.vars.Encode().data()).policy;
            const std::array ids{requests.Contains(&kept), requests.Contains(view.payload.data()),
                requests.Contains(view.presence_bitmap.data()), requests.Contains(view.metadata_bytes.data()),
                requests.Contains(kept.physical->policy.get()), requests.Contains(original_policy)};
            for (std::size_t i = 0U; i < ids.size(); ++i) {
                for (std::size_t j = 0U; j < i; ++j) assert(ids[i] != ids[j]);
                actual += requests.requests[ids[i]].bytes;
            }
            actual += original_buffers;
            assert(kept.bytes == actual && destination.load() == actual);
            if (expected != 0U) assert(actual == expected && account->transfer_releases >= 3U);
        }));
    }
    account->transfer_counter = nullptr;
    assert(account->bytes == 0U && account->global->bytes == 0U);
    history->End(*snapshot); assert(destination.load() == 0U);
    requests.live = 0U;
    return actual;
}

void HistoryAdmissionPrecedesSourceRelease() {
    // The first pass measures actual node/buffer/control requests and original
    // recorded extents; the second asserts that COMPLETE credit at first Release.
    const auto extent = ObserveHistoryTransfer(0U);
    assert(extent != 0U && ObserveHistoryTransfer(extent) == extent);
}

void SharedGlobalExactAdmission() {
    Scenario first_scenario, second_scenario;
    Saved first(first_scenario.first), first_other(first_scenario.second);
    Saved second(second_scenario.first), second_other(second_scenario.second);
    auto first_history = std::make_shared<TxnHistory>(1024U * 1024U);
    auto second_history = std::make_shared<TxnHistory>(1024U * 1024U);
    auto calibration = std::make_shared<Account>();
    { auto stage = second_scenario.Prepare(calibration, second_history); assert(!stage.empty()); }
    const auto second_peak = calibration->peak;
    assert(second_peak != 0U && calibration->bytes == 0U);
    auto shared = std::make_shared<GlobalCount>();
    auto first_account = std::make_shared<Account>(), second_account = std::make_shared<Account>();
    first_account->global = shared; second_account->global = shared;
    first_account->limit = second_account->limit = std::numeric_limits<std::size_t>::max();
    {
        auto first_stage = first_scenario.Prepare(first_account, first_history);
        const auto held = first_account->bytes;
        shared->limit = held + second_peak - 1U;
        bool refused = false;
        try { (void)second_scenario.Prepare(second_account, second_history); }
        catch (const std::invalid_argument&) { refused = true; }
        assert(refused && first_account->bytes == held && second_account->bytes == 0U && shared->bytes == held);
        first.Same(first_scenario.first); first_other.Same(first_scenario.second);
        second.Same(second_scenario.first); second_other.Same(second_scenario.second);
        assert(first_stage.changed_chunks() == 2U);
        shared->limit = held + second_peak;
        second_account->peak = 0U;
        {
            auto second_stage = second_scenario.Prepare(second_account, second_history);
            assert(second_stage.changed_chunks() == 2U && second_account->peak == second_peak);
            assert(first_account->bytes == held && shared->bytes == held + second_account->bytes);
        }
        assert(first_account->bytes == held && second_account->bytes == 0U && shared->bytes == held);
    }
    assert(first_account->bytes == 0U && second_account->bytes == 0U && shared->bytes == 0U);
    first.Same(first_scenario.first); first_other.Same(first_scenario.second);
    second.Same(second_scenario.first); second_other.Same(second_scenario.second);
}

void PreparationFailureMatrix() {
    Scenario scenario;
    Saved first(scenario.first), second(scenario.second);
    auto history = std::make_shared<TxnHistory>(1024U * 1024U);
    auto calibration = std::make_shared<Account>();
    { auto prepared = scenario.Prepare(calibration, history); assert(prepared.changed_chunks() == 2U); }
    const auto calls = calibration->calls;
    assert(calls > 10U && calibration->bytes == 0U && calibration->global->bytes == 0U);
    first.Same(scenario.first); second.Same(scenario.second);
    // Every actual admission ordinal is denied once; this is not a retry policy.
    for (std::size_t ordinal = 1U; ordinal <= calls; ++ordinal) {
        auto account = std::make_shared<Account>(); account->fail_at = ordinal;
        bool failed = false;
        try { (void)scenario.Prepare(account, history); } catch (const std::bad_alloc&) { failed = true; }
        assert(failed && account->bytes == 0U && account->global->bytes == 0U);
        first.Same(scenario.first); second.Same(scenario.second);
    }
    auto quota = std::make_shared<Account>(); quota->limit = 128U;
    bool refused = false;
    try { (void)scenario.Prepare(quota, history); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused && quota->bytes == 0U && quota->global->bytes == 0U);
    first.Same(scenario.first); second.Same(scenario.second);
}

struct LateFailure {};
void NonallocatingApplyAndReverseUndo() {
    for (const bool three : {false, true}) {
        Scenario scenario(three);
        Saved first(scenario.first), second(scenario.second);
        const auto old_data = scenario.first.state.vars.Encode().capacity();
        const auto old_offsets = ChunkVarsStorageAccess::OffsetCapacityBytes(scenario.first.state.vars);
        auto account = std::make_shared<Account>();
        auto history = std::make_shared<TxnHistory>(1024U * 1024U);
        auto prepared = scenario.Prepare(account, history);
        prepared.BindHistoryTag(8U);
        const auto calls = account->calls;
        bool failed = false;
        try {
            txn_block_allocation::Guard disabled(0U);
            auto applied = std::move(prepared);
            applied.Apply();
            assert(MatchesTxnBlockRow(scenario.geometry.layout(), scenario.first.state.payload,
                scenario.first.state.presence_bitmap, scenario.first.state.vars, 0U, scenario.after_first));
            assert(MatchesTxnBlockRow(scenario.geometry.layout(), scenario.first.state.payload,
                scenario.first.state.presence_bitmap, scenario.first.state.vars, 1U, scenario.unrelated));
            assert(MatchesTxnBlockRow(scenario.geometry.layout(), scenario.second.state.payload,
                scenario.second.state.presence_bitmap, scenario.second.state.vars, 0U, scenario.after_second));
            assert(scenario.first.state.vars.encoded_size() > old_data &&
                scenario.first.state.vars.size() * sizeof(std::uint32_t) > old_offsets);
            assert(scenario.first.state.chunk_columns == first.state.chunk_columns);
            // The future coordinator may have staged producing metadata too.
            scenario.first.state.version = 8U; scenario.first.state.commit_time_ms = 101U;
            scenario.second.state.version = 8U; scenario.second.state.commit_time_ms = 102U;
            throw LateFailure{};
        } catch (const LateFailure&) { failed = true; }
        assert(failed && account->calls == calls && account->bytes == 0U && account->global->bytes == 0U);
        first.Same(scenario.first); second.Same(scenario.second);
    }
}

void HistoryOutlivesTheTransactionAccount(bool retain) {
    Scenario scenario;
    const auto original = scenario.first.Snapshot(scenario.geometry);
    const auto metadata = scenario.first.state.chunk_columns->bytes;
    auto account = std::make_shared<Account>();
    std::weak_ptr<Account> account_lifetime = account;
    std::weak_ptr<GlobalCount> counter_lifetime = account->global;
    auto history = std::make_shared<TxnHistory>(retain ? 1024U * 1024U : 1U);
    std::atomic<std::uint64_t> clock{8U};
    std::unique_ptr<TxnSnapshot> snapshot;
    {
        auto stage = scenario.Prepare(account, history);
        // Registration occurs after physical preparation and before revision reservation.
        snapshot = history->Register(history, clock, std::chrono::steady_clock::now() + std::chrono::minutes(1));
        const auto tag = clock.fetch_add(1U);
        stage.BindHistoryTag(tag);
        txn_block_allocation::Guard disabled(0U);
        stage.Apply();
        scenario.first.state.version = tag; scenario.second.state.version = tag;
        const auto before = account->bytes;
        const auto live_bytes = scenario.first.state.vars.Encode().get_allocator().policy()->allocation_bytes() +
            scenario.second.state.vars.Encode().get_allocator().policy()->allocation_bytes();
        stage.Commit();
        if (!retain) assert(account->bytes == before - live_bytes && history->KeptBytes() == 0U);
    }
    assert(account->bytes == 0U && account->global->bytes == 0U);
    account.reset();
    assert(account_lifetime.expired() && counter_lifetime.expired());
    if (retain) {
        assert(history->KeptBytes() != 0U && history->KeptCount() == 2U);
        txn_block_allocation::Guard disabled(0U);
        const auto visited = history->VisitAt(*snapshot, scenario.first.coord, [&](const TxnKeptState& kept) {
            const auto view = kept.View();
            assert(view.version == original.version && view.commit_time_ms == original.commit_time_ms && view.written == original.written);
            assert(MatchesTxnBlockRow(scenario.geometry.layout(), view.payload, view.presence_bitmap, view.vars, 0U, original.row));
            assert(view.metadata_bytes.size() == metadata.size() && std::equal(view.metadata_bytes.begin(), view.metadata_bytes.end(), metadata.begin()));
        });
        assert(visited);
        history->End(*snapshot);
        assert(history->KeptBytes() == 0U && history->KeptCount() == 0U);
    } else {
        bool refused = false;
        try { (void)history->ChangedSince(*snapshot, {}); }
        catch (const TransactionConflictError& error) { refused = error.reason() == TxnConflictReason::kHistoryLimit; }
        assert(refused && history->KeptCount() == 0U);
    }
    // Installed regular storage must not retain or call the ended account.
    scenario.first.state.vars.Assign({4U, 2U}, std::array<std::uint8_t, 1U>{9U});
}

enum class MixedHistoryMode { kMeasure, kExact, kOneByteShort };
std::array<std::size_t, 3U> MixedHistory(std::size_t limit, MixedHistoryMode mode) {
    const auto geometry = Shape(); State state(geometry);
    const auto row7 = Row(geometry, 1U, "0007", true), row8 = Row(geometry, 2U, "0008", true);
    const auto row9 = Row(geometry, 3U, "0009", true), row10 = Row(geometry, 4U, "0010", true);
    state.Put(geometry, 0U, row7);
    auto history = std::make_shared<TxnHistory>(limit); auto account = std::make_shared<Account>();
    std::atomic<std::uint64_t> clock{8U};
    const auto register_at = [&](std::uint64_t next) {
        clock.store(next);
        return history->Register(history, clock, std::chrono::steady_clock::now() + std::chrono::hours(1));
    };
    const auto physical = [&](std::uint64_t tag, const TxnBlockRow& after) {
        const auto before = state.Snapshot(geometry);
        TxnBlockDeltas journal(StoreId{1U}, geometry.layout(), std::pmr::get_default_resource());
        journal.Publish(journal.Prepare({{}, 0U}, before, TxnBlockAction::kReplace, &after));
        {
            const std::array views{state.View()};
            auto stage = PreparedTxnBlockStage::Prepare(journal, StoreId{1U}, geometry, views, history, account, kVarMaxChunkBytesLimit);
            stage.BindHistoryTag(tag);
            txn_block_allocation::Guard disabled(0U); stage.Apply();
            state.state.version = tag; state.state.commit_time_ms = tag; stage.Commit();
        }
        assert(account->bytes == 0U && account->global->bytes == 0U);
    };
    const auto legacy = [&](std::uint64_t tag, const TxnBlockRow& after) {
        auto reservation = TxnHistory::Reserve({}, tag, state.state);
        const auto bytes = reservation.mapped().bytes;
        state.Put(geometry, 0U, after); state.state.version = tag; state.state.commit_time_ms = tag;
        txn_block_allocation::Guard disabled(0U); history->Publish(reservation);
        return bytes;
    };
    const auto lookup = [&](const TxnSnapshot& snapshot, std::uint64_t tag, bool is_physical, const TxnBlockRow& row) {
        std::size_t bytes = 0U;
        txn_block_allocation::Guard disabled(0U);
        assert(history->VisitAt(snapshot, {}, [&](const TxnKeptState& kept) {
            assert(kept.tag == tag && kept.physical.has_value() == is_physical);
            const auto view = kept.View();
            assert(MatchesTxnBlockRow(geometry.layout(), view.payload, view.presence_bitmap, view.vars, 0U, row));
            bytes = kept.bytes;
        }));
        return bytes;
    };
    const auto history_conflict = [&](const TxnSnapshot& snapshot) {
        bool refused = false;
        try { (void)history->ChangedSince(snapshot, {}); }
        catch (const TransactionConflictError& error) { refused = error.reason() == TxnConflictReason::kHistoryLimit; }
        assert(refused);
    };
    auto s7 = register_at(8U);
    physical(8U, row8);
    const auto first = lookup(*s7, 8U, true, row7);
    auto s8 = register_at(9U);
    const auto second = legacy(9U, row9);
    assert(lookup(*s8, 9U, false, row8) == second);
    auto s9 = register_at(10U);
    physical(10U, row10);
    const auto third = lookup(*s9, 10U, true, row9);
    if (mode == MixedHistoryMode::kOneByteShort) {
        assert(limit + 1U == first + second + third);
        history_conflict(*s7);
        assert(history->KeptCount() == 2U && history->KeptBytes() == second + third);
    } else {
        assert(history->KeptCount() == 3U && history->KeptBytes() == first + second + third);
        assert(lookup(*s7, 8U, true, row7) == first);
        if (mode == MixedHistoryMode::kExact) {
            assert(limit == first + second + third);
            auto s10 = register_at(11U);
            const auto fourth = legacy(11U, row7);
            assert(first >= fourth && fourth == second);
            history_conflict(*s7);
            assert(history->KeptCount() == 3U && history->KeptBytes() == second + third + fourth);
            assert(lookup(*s8, 9U, false, row8) == second && lookup(*s9, 10U, true, row9) == third);
            assert(lookup(*s10, 11U, false, row10) == fourth);
            history->End(*s8); assert(history->KeptBytes() == third + fourth && history->KeptCount() == 2U);
            history->End(*s9); assert(history->KeptBytes() == fourth && history->KeptCount() == 1U);
            history->End(*s10); assert(history->KeptBytes() == 0U && history->KeptCount() == 0U);
            return {first, second, third};
        }
    }
    history->End(*s7);
    assert(lookup(*s8, 9U, false, row8) == second && lookup(*s9, 10U, true, row9) == third);
    assert(history->KeptBytes() == second + third);
    history->End(*s8); assert(history->KeptBytes() == third && history->KeptCount() == 1U);
    history->End(*s9); assert(history->KeptBytes() == 0U && history->KeptCount() == 0U);
    return {first, second, third};
}

void MixedLegacyAndPhysicalHistory() {
    const auto charges = MixedHistory(1024U * 1024U, MixedHistoryMode::kMeasure);
    const auto exact = charges[0] + charges[1] + charges[2];
    assert(MixedHistory(exact, MixedHistoryMode::kExact) == charges);
    assert(MixedHistory(exact - 1U, MixedHistoryMode::kOneByteShort) == charges);
}

void InitializationAndConflicts() {
    auto geometry = Shape(); State state(geometry, {}, false);
    state.state.chunk_columns.reset();
    auto before = state.Snapshot(geometry);
    auto replacement = Row(geometry, 3U, "first", true);
    TxnBlockDeltas journal(StoreId{1U}, geometry.layout(), std::pmr::get_default_resource());
    journal.Publish(journal.Prepare({{}, 0U}, before, TxnBlockAction::kReplace, &replacement));
    auto account = std::make_shared<Account>(); auto history = std::make_shared<TxnHistory>(1024U * 1024U);
    Saved original(state);
    {
        const std::array views{state.View()};
        auto stage = PreparedTxnBlockStage::Prepare(journal, StoreId{1U}, geometry, views, history, account, kVarMaxChunkBytesLimit);
        stage.BindHistoryTag(1U);
        txn_block_allocation::Guard disabled(0U); stage.Apply();
        assert(state.state.written && state.present == 1U && state.state.chunk_columns == geometry.default_chunk_columns());
        stage.Undo();
    }
    original.Same(state); assert(account->bytes == 0U);
    auto required = Shape(false, BlockSchema(), Metadata(true)); State missing(required, {}, false);
    bool refused = false;
    try {
        const std::array views{missing.View()};
        (void)PreparedTxnBlockStage::Prepare(journal, StoreId{1U}, required, views, history, account, kVarMaxChunkBytesLimit);
    } catch (const std::invalid_argument&) { refused = true; }
    assert(refused && account->bytes == 0U && !missing.state.written);

    Scenario aba;
    aba.first.Put(aba.geometry, 0U, Row(aba.geometry, 4U, "changed"));
    const auto conflict = [&](const Geometry& current_geometry, StoreId epoch, TxnConflictReason expected) {
        bool caught = false;
        try { (void)PreparedTxnBlockStage::Prepare(aba.journal, epoch, current_geometry, aba.Views(), history, account, kVarMaxChunkBytesLimit); }
        catch (const TransactionConflictError& error) { caught = error.reason() == expected; }
        assert(caught && account->bytes == 0U && account->global->bytes == 0U);
    };
    conflict(aba.geometry, StoreId{1U}, TxnConflictReason::kChunkChanged);
    aba.first.Put(aba.geometry, 0U, Row(aba.geometry, 1U, "a")); aba.first.state.version = 9U;
    { auto accepted = aba.Prepare(account, history); accepted.BindHistoryTag(10U); }
    assert(account->bytes == 0U);
    conflict(aba.geometry, StoreId{2U}, TxnConflictReason::kTableChanged);
    auto schema = BlockSchema(); schema.columns[0].name = "renamed";
    auto altered = Shape(false, schema);
    conflict(altered, StoreId{1U}, TxnConflictReason::kTableChanged);
}

void SparseDeleteAndCoalescedNulls() {
    auto geometry = Shape(); State state(geometry);
    const auto initial = Row(geometry, 1U, "initial", true);
    const auto unrelated = Row(geometry, 7U, "unrelated", true);
    state.Put(geometry, 0U, initial); state.Put(geometry, 1U, initial); state.Put(geometry, 2U, unrelated);
    const auto first = state.Snapshot(geometry), second = state.Snapshot(geometry, 1U);
    const auto after = Row(geometry, 4U, "after");
    TxnBlockDeltas journal(StoreId{1U}, geometry.layout(), std::pmr::get_default_resource());
    journal.Publish(journal.Prepare({{}, 0U}, first, TxnBlockAction::kReplace, &after));
    journal.Publish(journal.Prepare({{}, 1U}, second, TxnBlockAction::kDelete));
    auto account = std::make_shared<Account>(); auto history = std::make_shared<TxnHistory>(1024U * 1024U);
    Saved original(state);
    {
        const std::array views{state.View()};
        auto stage = PreparedTxnBlockStage::Prepare(journal, StoreId{1U}, geometry, views, history, account, kVarMaxChunkBytesLimit);
        stage.BindHistoryTag(8U);
        txn_block_allocation::Guard disabled(0U); stage.Apply();
        TxnBlockRow absent; absent.schema_version = geometry.layout().schema().version;
        assert(state.present == 2U);
        assert(MatchesTxnBlockRow(geometry.layout(), state.state.payload, state.state.presence_bitmap, state.state.vars, 0U, after));
        assert(MatchesTxnBlockRow(geometry.layout(), state.state.payload, state.state.presence_bitmap, state.state.vars, 1U, absent));
        assert(MatchesTxnBlockRow(geometry.layout(), state.state.payload, state.state.presence_bitmap, state.state.vars, 2U, unrelated));
        stage.Undo(); stage.Undo();
    }
    original.Same(state); assert(account->bytes == 0U);
    for (const auto action : {TxnBlockAction::kObserve, TxnBlockAction::kReplace}) {
        TxnBlockDeltas unchanged(StoreId{1U}, geometry.layout(), std::pmr::get_default_resource());
        unchanged.Publish(unchanged.Prepare({{}, 0U}, first, action, action == TxnBlockAction::kReplace ? &initial : nullptr));
        {
            const std::array views{state.View()};
            auto stage = PreparedTxnBlockStage::Prepare(unchanged, StoreId{1U}, geometry, views, history, account, kVarMaxChunkBytesLimit);
            assert(stage.empty()); stage.BindHistoryTag(8U);
            txn_block_allocation::Guard disabled(0U); stage.Apply(); stage.Commit();
        }
        original.Same(state); assert(account->bytes == 0U && history->KeptCount() == 0U);
    }
}

void HistoryTagAndOwnerBinding() {
    Scenario scenario;
    Saved first(scenario.first), second(scenario.second);
    auto account = std::make_shared<Account>(); auto history = std::make_shared<TxnHistory>(1024U * 1024U);
    {
        auto stage = scenario.Prepare(account, history);
        const auto reject = [&](std::uint64_t tag) {
            bool refused = false;
            try { stage.BindHistoryTag(tag); } catch (const std::invalid_argument&) { refused = true; }
            assert(refused);
        };
        reject(0U); reject(7U);
        ++scenario.second.state.commit_time_ms;
        reject(8U);
        --scenario.second.state.commit_time_ms;
        stage.BindHistoryTag(8U); reject(9U);
    }
    first.Same(scenario.first); second.Same(scenario.second);
    assert(account->bytes == 0U && account->global->bytes == 0U);
}

void OriginalPolicyAdmission() {
    auto geometry = Shape(); State state(geometry);
    state.Put(geometry, 0U, Row(geometry, 1U, "a"));
    auto account = std::make_shared<Account>();
    {
        ForeignUpstream foreign;
        auto foreign_policy = ChunkStoragePolicy::Create(account, &foreign);
        auto foreign_vars = ChunkVarsStorageAccess::Clone(state.state.vars, foreign_policy);
        foreign_policy->TransferToRegularStorage();
        bool rejected = false;
        try { TxnOriginalVariables::Validate(foreign_vars); } catch (const std::invalid_argument&) { rejected = true; }
        assert(rejected && account->bytes == 0U);
    }
    {
        auto empty_policy = ChunkStoragePolicy::Create(account);
        auto empty = ChunkVarsStorageAccess::Clone(ChunkVars{}, empty_policy);
        bool rejected = false;
        try { TxnOriginalVariables::Validate(empty); } catch (const std::invalid_argument&) { rejected = true; }
        assert(rejected);
        empty_policy->TransferToRegularStorage();
        const auto control = empty_policy->allocation_bytes();
        { auto admission = TxnOriginalVariables::Prepare(empty, account); assert(account->bytes == control); }
        assert(account->bytes == 0U && empty_policy->transaction_bytes() == 0U);
    }
    auto policy = ChunkStoragePolicy::Create(account);
    state.state.vars = ChunkVarsStorageAccess::Clone(state.state.vars, policy);
    bool refused = false;
    try { TxnOriginalVariables::Validate(state.state.vars); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
    policy->TransferToRegularStorage();
    auto outside = ChunkVarsStorageAccess::Clone(state.state.vars, policy);
    refused = false;
    try { TxnOriginalVariables::Validate(state.state.vars); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
    outside = {};
    Saved original(state);
    auto history = std::make_shared<TxnHistory>(1024U * 1024U);
    auto snapshot = state.Snapshot(geometry); auto after = Row(geometry, 2U, "after");
    TxnBlockDeltas journal(StoreId{1U}, geometry.layout(), std::pmr::get_default_resource());
    journal.Publish(journal.Prepare({{}, 0U}, snapshot, TxnBlockAction::kReplace, &after));
    {
        const std::array views{state.View()};
        auto stage = PreparedTxnBlockStage::Prepare(journal, StoreId{1U}, geometry, views, history, account, kVarMaxChunkBytesLimit);
        stage.BindHistoryTag(8U);
        txn_block_allocation::Guard disabled(0U); stage.Apply(); stage.Undo();
    }
    original.Same(state); assert(account->bytes == 0U && policy->transaction_bytes() == 0U);
}
}  // namespace

int main() {
    txn_block_allocation::CalibrateAligned();
    ZeroExternalCreditUpstreamOrdering();
    HistoryAdmissionPrecedesSourceRelease();
    ActualUpstreamAdmissionAndFailures(); SharedGlobalExactAdmission();
    PreparationFailureMatrix(); NonallocatingApplyAndReverseUndo();
    HistoryOutlivesTheTransactionAccount(true); HistoryOutlivesTheTransactionAccount(false);
    MixedLegacyAndPhysicalHistory();
    InitializationAndConflicts(); SparseDeleteAndCoalescedNulls(); HistoryTagAndOwnerBinding(); OriginalPolicyAdmission();
}
