// Reads at a transaction snapshot (docs/design/TRANSACTIONS_DESIGN.md): every plain
// write path keeps what an open snapshot needs, a snapshot reads chunks as
// they were when it was taken (GET AREA's no-cache path too), and a writer
// racing a registering snapshot is kept.

#include <cassert>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "txn_test_utils.hpp"

namespace {

using chunkdb::ChunkCoord;
using chunkdb::ChunkState;
using chunkdb::ChunkStore;
using chunkdb::TransactionConflictError;
using chunkdb::TxnConflictReason;
using chunkdb::TxnPausePoint;
using chunkdb::test::ScopedTempDir;
using chunkdb::txn_test::Config;
using chunkdb::txn_test::CounterBits;
using chunkdb::txn_test::CounterOf;
using chunkdb::txn_test::kTxnDuration;
using chunkdb::txn_test::ReadCounter;
using chunkdb::txn_test::SetCounter;
using chunkdb::txn_test::WriteCounter;

constexpr ChunkCoord kA{0, 0};
constexpr ChunkCoord kB{1, 0};
constexpr ChunkCoord kC{2, 0};
constexpr ChunkCoord kD{3, 0};

std::uint32_t CounterAt(ChunkStore& store, const chunkdb::TxnSnapshot& snapshot, const ChunkCoord& coord) {
    return CounterOf(store.ReadChunkStateAt(snapshot, coord.x, coord.y));
}

TxnConflictReason ConflictReasonOf(const std::function<void()>& action) {
    try {
        action();
    } catch (const TransactionConflictError& error) {
        return error.reason();
    }
    assert(false && "expected a transaction conflict");
    return TxnConflictReason::kChunkChanged;
}

// A snapshot reads A, plain writes change A and B, and B at the snapshot is
// still the old B.
void TestSnapshotReadsOldStates() {
    ScopedTempDir dir("chunkdb-txn-snapshot-old");
    ChunkStore store(Config(dir.path()));
    WriteCounter(store, kA, 1);
    WriteCounter(store, kB, 2);
    const auto version_a = store.GetChunkVersion(kA.x, kA.y);

    auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
    assert(store.TxnRegisteredCountForTests() == 1U);
    assert(CounterAt(store, *snapshot, kA) == 1U);
    WriteCounter(store, kA, 10);
    WriteCounter(store, kB, 20);
    assert(ReadCounter(store, kA) == 10U);
    assert(ReadCounter(store, kB) == 20U);
    assert(CounterAt(store, *snapshot, kB) == 2U);
    assert(CounterAt(store, *snapshot, kA) == 1U);
    // The version as of the snapshot.
    assert(store.ReadChunkStateAt(*snapshot, kA.x, kA.y).version == version_a);
    assert(store.ChunkChangedSince(*snapshot, kA.x, kA.y));
    assert(store.ChunkChangedSince(*snapshot, kB.x, kB.y));
    assert(!store.ChunkChangedSince(*snapshot, kC.x, kC.y));
    // GET BLOCK at the snapshot.
    const auto block = store.GetBlockAt(*snapshot, kA.x * 4, kA.y * 4);
    assert(block.has_value() && std::get<chunkdb::BitsValue>((*block)[0]).digits == CounterBits(1));
    assert(!store.GetBlockAt(*snapshot, kA.x * 4 + 1, kA.y * 4).has_value());

    // A second write to A is not kept again: S reads the first kept state.
    const auto kept = store.TxnKeptStateCountForTests();
    WriteCounter(store, kA, 11);
    assert(store.TxnKeptStateCountForTests() == kept);
    assert(CounterAt(store, *snapshot, kA) == 1U);

    // Ending the snapshot drops what only it needed.
    store.EndTxnSnapshot(*snapshot);
    assert(store.TxnKeptStateCountForTests() == 0U);
    assert(store.TxnRegisteredCountForTests() == 0U);
    bool ended = false;
    try {
        (void)store.ReadChunkStateAt(*snapshot, kA.x, kA.y);
    } catch (const std::invalid_argument&) {
        ended = true;
    }
    assert(ended);
    // Without an open transaction nothing is kept.
    WriteCounter(store, kB, 21);
    assert(store.TxnKeptStateCountForTests() == 0U);
}

// Every path that changes a chunk keeps the state it replaces.
void TestEveryWritePathKeeps() {
    ScopedTempDir dir("chunkdb-txn-snapshot-paths");
    ChunkStore store(Config(dir.path(), chunkdb::DurabilityMode::kRelaxed));
    for (std::int64_t x = 0; x < 8; ++x) {
        WriteCounter(store, ChunkCoord{x, 1}, static_cast<std::uint32_t>(100 + x));
    }
    auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
    const auto replace = [&](std::int64_t x, std::uint32_t value) {
        auto state = store.ReadChunkState(x, 1);
        SetCounter(&state, value);
        return state;
    };

    // SetBlockBits and UnsetBlock (bit strings).
    WriteCounter(store, ChunkCoord{0, 1}, 1);
    store.UnsetBlock(1 * 4, 4);
    // SetChunkStateBytes (a whole chunk without a condition).
    {
        auto state = replace(2, 2);
        (void)store.SetChunkStateBytes(2, 1, state.payload, state.presence_bitmap);
    }
    // WriteChunkState without and with a condition.
    (void)store.WriteChunkState(3, 1, replace(3, 3), std::nullopt);
    {
        const auto version = store.GetChunkVersion(4, 1);
        const auto result = store.WriteChunkState(4, 1, replace(4, 4), version);
        assert(result.ok);
    }
    // CasChunkStateBytes and ApplyChunkBatch (conditional).
    {
        auto state = replace(5, 5);
        const auto result =
            store.CasChunkStateBytes(5, 1, store.GetChunkVersion(5, 1), state.payload, state.presence_bitmap);
        assert(result.ok);
    }
    {
        const auto result = store.ApplyChunkBatch(
            6, 1, false, 0, {chunkdb::ChunkBatchOp{.set = true, .x = 6 * 4, .y = 4, .bits = CounterBits(6)}});
        assert(result.ok);
    }
    // SetBlock with columns (the typed path, here the one bits column).
    store.SetBlock(7 * 4, 4, {chunkdb::ColumnAssignment{.column = "bits", .value = chunkdb::BitsValue{CounterBits(7)}}});

    for (std::int64_t x = 0; x < 8; ++x) {
        const ChunkCoord coord{x, 1};
        assert(CounterAt(store, *snapshot, coord) == static_cast<std::uint32_t>(100 + x));
        assert(store.ChunkChangedSince(*snapshot, coord.x, coord.y));
    }
    assert(ReadCounter(store, ChunkCoord{1, 1}) == 0U);
    assert(ReadCounter(store, ChunkCoord{7, 1}) == 7U);
    assert(store.TxnKeptStateCountForTests() == 8U);
}

// GET AREA at a snapshot, for a chunk in the cache and for chunks only on
// disk (the no-cache path reads the files first, then the history).
void TestAreaAtSnapshot() {
    ScopedTempDir dir("chunkdb-txn-snapshot-area");
    auto config = Config(dir.path());
    config.max_loaded_chunks = 4;
    ChunkStore store(config);
    WriteCounter(store, kA, 1);
    WriteCounter(store, kB, 2);
    WriteCounter(store, kD, 4);

    auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
    WriteCounter(store, kA, 10);
    WriteCounter(store, kB, 20);
    WriteCounter(store, kC, 30);       // absent at the snapshot
    store.UnsetBlock(kD.x * 4, kD.y * 4);  // present at the snapshot, gone now

    // Push B, C and D out of the cache.
    for (std::int64_t y = 10; y < 30; ++y) {
        (void)store.GetChunkVersion(0, y);
    }
    (void)store.GetChunkVersion(kA.x, kA.y);
    assert(!store.IsChunkLoadedForTests(kB.x, kB.y));
    assert(!store.IsChunkLoadedForTests(kD.x, kD.y));

    const auto counter_of = [](const chunkdb::ChunkRangeEntry& entry) {
        return CounterOf(ChunkState{
            .version = entry.version,
            .payload = entry.payload,
            .presence_bitmap = entry.presence_bitmap,
            .vars = {},
        });
    };
    const auto entries = store.ReadChunkRangeAt(*snapshot, 0, 0, 3, 0);
    assert(entries.size() == 3U);
    assert(entries[0].coord == kA && counter_of(entries[0]) == 1U);
    assert(entries[1].coord == kB && counter_of(entries[1]) == 2U);
    assert(entries[2].coord == kD && counter_of(entries[2]) == 4U);
    // The chunks stayed out of the cache.
    assert(!store.IsChunkLoadedForTests(kB.x, kB.y));
    // Without the snapshot the area is current.
    const auto current = store.ReadChunkRange(0, 0, 3, 0);
    assert(current.size() == 3U);
    assert(counter_of(current[0]) == 10U && counter_of(current[1]) == 20U && current[2].coord == kC);
    // The radius form.
    const auto around = store.ReadChunkRadiusAt(*snapshot, 1, 0, 1);
    assert(around.size() == 2U);
    assert(around[0].coord == kA && counter_of(around[0]) == 1U);
    assert(around[1].coord == kB && counter_of(around[1]) == 2U);
}

// Text values at a snapshot.
chunkdb::TableSchema TextSchema() {
    chunkdb::Column id;
    id.id = 1;
    id.name = "id";
    id.type = chunkdb::ColumnType{.kind = chunkdb::ColumnKind::kUnsigned, .size = 8};
    chunkdb::Column note;
    note.id = 2;
    note.name = "note";
    note.type = chunkdb::ColumnType{.kind = chunkdb::ColumnKind::kText, .size = 16};
    note.nullable = true;
    return chunkdb::TableSchema{.version = 1, .next_column_id = 3, .columns = {id, note}};
}

void TestTextValuesAtSnapshot() {
    ScopedTempDir dir("chunkdb-txn-snapshot-text");
    auto config = Config(dir.path(), chunkdb::DurabilityMode::kFsyncWal, 8);
    config.schema = TextSchema();
    ChunkStore store(config);
    const auto row = [](std::uint64_t id, std::string note) {
        return std::vector<chunkdb::ColumnAssignment>{
            {.column = "id", .value = id},
            {.column = "note", .value = std::move(note)},
        };
    };
    store.SetBlock(1, 1, row(1, "before"));
    auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
    store.SetBlock(1, 1, row(2, "after"));
    store.SetBlock(2, 1, row(3, "new"));

    const auto old_block = store.GetBlockAt(*snapshot, 1, 1);
    assert(old_block.has_value());
    assert(std::get<std::uint64_t>((*old_block)[0]) == 1U);
    assert(std::get<std::string>((*old_block)[1]) == "before");
    assert(!store.GetBlockAt(*snapshot, 2, 1).has_value());
    const auto state = store.ReadChunkStateAt(*snapshot, 0, 0);
    assert(state.vars.size() == 1U);
    const auto area = store.ReadChunkRangeAt(*snapshot, 0, 0, 0, 0, true);
    assert(area.size() == 1U && area[0].vars == state.vars);
    const auto now = store.GetBlock(1, 1);
    assert(now.has_value() && std::get<std::string>((*now)[1]) == "after");
}

// A write that finds an open transaction while another one registers: the
// registering snapshot read its version before the write took its own, so
// the write must keep the state it replaces, and does once the registration
// is published.
void TestWriterRacingRegistrationIsKept() {
    ScopedTempDir dir("chunkdb-txn-snapshot-race");
    ChunkStore store(Config(dir.path()));
    WriteCounter(store, kA, 1);

    store.ArmTxnPauseForTests(TxnPausePoint::kRegisterBeforePublish);
    store.ArmTxnPauseForTests(TxnPausePoint::kWriteAfterOpenCount);
    std::unique_ptr<chunkdb::TxnSnapshot> snapshot;
    std::thread registering([&] { snapshot = store.BeginTxnSnapshot(kTxnDuration); });
    assert(store.WaitForTxnPauseForTests(TxnPausePoint::kRegisterBeforePublish));

    std::thread writer([&] { WriteCounter(store, kA, 2); });
    // The writer took a version above the snapshot's and saw it counted.
    assert(store.WaitForTxnPauseForTests(TxnPausePoint::kWriteAfterOpenCount));
    store.ResumeTxnForTests(TxnPausePoint::kWriteAfterOpenCount);
    store.ResumeTxnForTests(TxnPausePoint::kRegisterBeforePublish);
    registering.join();
    writer.join();

    assert(snapshot->version() < store.GetChunkVersion(kA.x, kA.y));
    assert(ReadCounter(store, kA) == 2U);
    assert(store.TxnKeptStateCountForTests() == 1U);
    assert(CounterAt(store, *snapshot, kA) == 1U);
    assert(store.ChunkChangedSince(*snapshot, kA.x, kA.y));
}

// The other order: the write took its version before the snapshot read the
// clock, so the snapshot includes the write and reads it once the write is
// done (it holds the chunk's lock until then).
void TestSnapshotAfterWriterVersionSeesTheWrite() {
    ScopedTempDir dir("chunkdb-txn-snapshot-race-after");
    ChunkStore store(Config(dir.path()));
    WriteCounter(store, kA, 1);
    auto older = store.BeginTxnSnapshot(kTxnDuration);

    store.ArmTxnPauseForTests(TxnPausePoint::kWriteAfterOpenCount);
    std::thread writer([&] { WriteCounter(store, kA, 2); });
    assert(store.WaitForTxnPauseForTests(TxnPausePoint::kWriteAfterOpenCount));
    auto newer = store.BeginTxnSnapshot(kTxnDuration);
    std::uint32_t seen = 0;
    std::thread reader([&] { seen = CounterAt(store, *newer, kA); });
    store.ResumeTxnForTests(TxnPausePoint::kWriteAfterOpenCount);
    writer.join();
    reader.join();

    assert(store.GetChunkVersion(kA.x, kA.y) <= newer->version());
    assert(seen == 2U);
    assert(!store.ChunkChangedSince(*newer, kA.x, kA.y));
    assert(CounterAt(store, *older, kA) == 1U);
}

void TestRefusedTables() {
    ScopedTempDir dir("chunkdb-txn-snapshot-refused");
    {
        ChunkStore writer(Config(dir.path()));
        WriteCounter(writer, kA, 1);
    }
    {
        auto config = Config(dir.path());
        config.access_mode = chunkdb::AccessMode::kReadOnly;
        ChunkStore reader(config);
        bool refused = false;
        try {
            (void)reader.BeginTxnSnapshot(kTxnDuration);
        } catch (const std::invalid_argument&) {
            refused = true;
        }
        assert(refused);
    }
    {
        auto config = Config(dir.path());
        config.allow_multiple_processes = true;
        ChunkStore shared(config);
        bool refused = false;
        try {
            (void)shared.BeginTxnSnapshot(kTxnDuration);
        } catch (const std::invalid_argument&) {
            refused = true;
        }
        assert(refused);
    }
    // A snapshot of one table cannot read another.
    ScopedTempDir other_dir("chunkdb-txn-snapshot-other");
    ChunkStore first(Config(dir.path()));
    ChunkStore second(Config(other_dir.path()));
    auto snapshot = first.BeginTxnSnapshot(kTxnDuration);
    bool refused = false;
    try {
        (void)second.ReadChunkStateAt(*snapshot, 0, 0);
    } catch (const std::invalid_argument&) {
        refused = true;
    }
    assert(refused);
}

// Past the history limit the oldest transactions end with CONFLICT; plain
// writes go on.
void TestHistoryLimit() {
    ScopedTempDir dir("chunkdb-txn-snapshot-limit");
    auto config = Config(dir.path());
    config.txn_history_bytes = 600;  // about two kept states of this geometry
    ChunkStore store(config);
    auto oldest = store.BeginTxnSnapshot(kTxnDuration);
    for (std::int64_t x = 0; x < 6; ++x) {
        WriteCounter(store, ChunkCoord{x, 0}, static_cast<std::uint32_t>(x + 1));
    }
    assert(ConflictReasonOf([&] { (void)store.ReadChunkStateAt(*oldest, 0, 0); }) == TxnConflictReason::kHistoryLimit);
    assert(store.TxnRegisteredCountForTests() == 0U);
    assert(store.TxnKeptStateCountForTests() == 0U);
    for (std::int64_t x = 0; x < 6; ++x) {
        assert(ReadCounter(store, ChunkCoord{x, 0}) == static_cast<std::uint32_t>(x + 1));
    }
}

// A write in flight whose version is at or below the newest snapshot, past
// the history limit: only the older snapshot, which needs the state, ends.
void TestHistoryLimitSparesNewerSnapshots() {
    ScopedTempDir dir("chunkdb-txn-snapshot-limit-newer");
    auto config = Config(dir.path());
    config.txn_history_bytes = 64;  // less than one kept state
    ChunkStore store(config);
    WriteCounter(store, kA, 1);
    auto older = store.BeginTxnSnapshot(kTxnDuration);

    store.ArmTxnPauseForTests(TxnPausePoint::kWriteAfterOpenCount);
    std::thread writer([&] { WriteCounter(store, kA, 2); });
    assert(store.WaitForTxnPauseForTests(TxnPausePoint::kWriteAfterOpenCount));
    // Registered after the write took its version: it includes the write.
    auto newer = store.BeginTxnSnapshot(kTxnDuration);
    store.ResumeTxnForTests(TxnPausePoint::kWriteAfterOpenCount);
    writer.join();

    assert(store.GetChunkVersion(kA.x, kA.y) <= newer->version());
    assert(store.TxnRegisteredCountForTests() == 1U);
    assert(ConflictReasonOf([&] { (void)store.ReadChunkStateAt(*older, kA.x, kA.y); }) ==
           TxnConflictReason::kHistoryLimit);
    assert(CounterAt(store, *newer, kA) == 2U);
    assert(store.TxnKeptStateCountForTests() == 0U);
}

void TestDurationLimit() {
    ScopedTempDir dir("chunkdb-txn-snapshot-duration");
    ChunkStore store(Config(dir.path()));
    auto snapshot = store.BeginTxnSnapshot(std::chrono::milliseconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    assert(ConflictReasonOf([&] { (void)store.ReadChunkStateAt(*snapshot, 0, 0); }) == TxnConflictReason::kDuration);
    assert(store.TxnRegisteredCountForTests() == 0U);
}

}  // namespace

int main() {
    TestSnapshotReadsOldStates();
    TestEveryWritePathKeeps();
    TestAreaAtSnapshot();
    TestTextValuesAtSnapshot();
    TestWriterRacingRegistrationIsKept();
    TestSnapshotAfterWriterVersionSeesTheWrite();
    TestRefusedTables();
    TestHistoryLimit();
    TestHistoryLimitSparesNewerSnapshots();
    TestDurationLimit();
    return 0;
}
