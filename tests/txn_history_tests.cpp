// Unit tests of the transaction history (src/txn_history.hpp): snapshot
// registration, the keep rule, pruning, reads at a snapshot, "changed since",
// the byte and duration limits, and the transaction intent record.

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "txn_history.hpp"

namespace {

using chunkdb::ChunkCoord;
using chunkdb::ChunkState;
using chunkdb::TransactionConflictError;
using chunkdb::TxnConflictReason;
using chunkdb::TxnHistory;
using chunkdb::TxnKeptState;
using chunkdb::TxnSnapshot;

constexpr ChunkCoord kA{0, 0};
constexpr ChunkCoord kB{1, 0};

const auto kFarDeadline = std::chrono::steady_clock::now() + std::chrono::hours(1);

ChunkState StateOf(std::uint8_t value, std::uint64_t version, std::size_t bytes = 4) {
    return ChunkState{
        .version = version,
        .payload = std::vector<std::uint8_t>(bytes, value),
        .presence_bitmap = {1},
        .vars = {},
    };
}

// What a write does: keep the state it replaces, tagged with its version.
void Write(TxnHistory& history, const ChunkCoord& coord, std::uint64_t tag, std::uint8_t replaced_value) {
    auto reservation = TxnHistory::Reserve(coord, tag, StateOf(replaced_value, tag - 1U));
    history.Publish(reservation);
}

// The value a read at the snapshot sees from the history; -1 for none.
int KeptValueAt(TxnHistory& history, const TxnSnapshot& snapshot, const ChunkCoord& coord) {
    int value = -1;
    (void)history.VisitAt(snapshot, coord, [&](const TxnKeptState& kept) { value = kept.state.payload[0]; });
    return value;
}

template <class F>
TxnConflictReason ConflictOf(F&& f) {
    try {
        f();
    } catch (const TransactionConflictError& error) {
        return error.reason();
    }
    assert(false && "expected a transaction conflict");
    return TxnConflictReason::kChunkChanged;
}

void TestRegistrationTakesTheLastIssuedVersion() {
    auto history = std::make_shared<TxnHistory>(1U << 20U);
    std::atomic<std::uint64_t> clock{42};
    assert(history->OpenCount() == 0U);
    auto first = history->Register(history, clock, kFarDeadline);
    // The clock holds the next version, so the last one issued is 41.
    assert(first->version() == 41U);
    assert(history->OpenCount() == 1U);
    clock.store(50);
    auto second = history->Register(history, clock, kFarDeadline);
    assert(second->version() == 49U);
    assert(history->OpenCount() == 2U);
    assert(history->RegisteredCount() == 2U);
    history->End(*first);
    assert(history->OpenCount() == 1U);
    // An ended snapshot can no longer read.
    bool refused = false;
    try {
        (void)KeptValueAt(*history, *first, kA);
    } catch (const std::invalid_argument&) {
        refused = true;
    }
    assert(refused);
    second.reset();
    assert(history->OpenCount() == 0U);
    assert(history->RegisteredCount() == 0U);
}

void TestKeepRuleReadAtSnapshotAndPrune() {
    auto history = std::make_shared<TxnHistory>(1U << 20U);
    std::atomic<std::uint64_t> clock{11};
    auto s10 = history->Register(history, clock, kFarDeadline);
    assert(s10->version() == 10U);

    // No snapshot is older than a write at 10 or below: nothing kept.
    Write(*history, kA, 10, 1);
    assert(history->KeptCount() == 0U);

    // A write at 12 replaced value 2: S=10 needs it.
    Write(*history, kA, 12, 2);
    assert(history->KeptCount() == 1U);
    assert(KeptValueAt(*history, *s10, kA) == 2);
    assert(history->ChangedSince(*s10, kA));
    assert(!history->ChangedSince(*s10, kB));
    assert(KeptValueAt(*history, *s10, kB) == -1);

    // A second write to A: its kept state 12 is already above the newest
    // snapshot (10), and S=10 reads that one, so 14 is not kept.
    Write(*history, kA, 14, 3);
    assert(history->KeptCount() == 1U);
    assert(KeptValueAt(*history, *s10, kA) == 2);

    // A newer snapshot at 15 needs the state 14 replaced... but 14 is not
    // above 15, so a write at 16 is what it sees changed.
    clock.store(16);
    auto s15 = history->Register(history, clock, kFarDeadline);
    assert(s15->version() == 15U);
    assert(KeptValueAt(*history, *s15, kA) == -1);
    assert(!history->ChangedSince(*s15, kA));
    Write(*history, kA, 16, 4);
    assert(history->KeptCount() == 2U);
    // S=10 still reads the oldest state above it, S=15 the one above 15.
    assert(KeptValueAt(*history, *s10, kA) == 2);
    assert(KeptValueAt(*history, *s15, kA) == 4);

    // A write to B at 17 is needed by both.
    Write(*history, kB, 17, 9);
    assert(history->KeptCount() == 3U);
    assert(KeptValueAt(*history, *s10, kB) == 9);
    assert(KeptValueAt(*history, *s15, kB) == 9);

    // Ending S=10 drops what only it needed: the state tagged 12 (at or
    // below the oldest remaining snapshot, 15).
    history->End(*s10);
    assert(history->KeptCount() == 2U);
    assert(KeptValueAt(*history, *s15, kA) == 4);
    // Ending the last one drops everything.
    history->End(*s15);
    assert(history->KeptCount() == 0U);
    assert(history->KeptBytes() == 0U);
}

void TestValidateAndEnd() {
    auto history = std::make_shared<TxnHistory>(1U << 20U);
    std::atomic<std::uint64_t> clock{21};
    auto unchanged = history->Register(history, clock, kFarDeadline);
    auto changed = history->Register(history, clock, kFarDeadline);
    Write(*history, kB, 25, 1);
    // A transaction that touched only A commits; it ends either way.
    history->ValidateAndEnd(*unchanged, {kA});
    assert(history->RegisteredCount() == 1U);
    assert(ConflictOf([&] { history->ValidateAndEnd(*changed, {kA, kB}); }) == TxnConflictReason::kChunkChanged);
    assert(history->RegisteredCount() == 0U);
    assert(history->KeptCount() == 0U);
}

void TestByteLimitUnregistersTheOldest() {
    // Room for about two kept states of 1000 bytes.
    const std::size_t state_bytes = 1000;
    auto history = std::make_shared<TxnHistory>(2U * (state_bytes + sizeof(TxnKeptState) + 64U + 1U) + 100U);
    std::atomic<std::uint64_t> clock{101};
    auto oldest = history->Register(history, clock, kFarDeadline);
    clock.store(111);
    auto newest = history->Register(history, clock, kFarDeadline);
    const auto write = [&](const ChunkCoord& coord, std::uint64_t tag) {
        auto reservation = TxnHistory::Reserve(coord, tag, StateOf(7, tag - 1U, state_bytes));
        history->Publish(reservation);
    };
    write(ChunkCoord{0, 0}, 105);  // only the oldest needs it
    write(ChunkCoord{1, 0}, 112);
    assert(history->KeptCount() == 2U);
    // Past the limit: the oldest snapshot goes, and with it what only it
    // needed; the write itself never fails.
    write(ChunkCoord{2, 0}, 113);
    assert(history->RegisteredCount() == 1U);
    assert(history->KeptCount() == 2U);
    assert(ConflictOf([&] { (void)KeptValueAt(*history, *oldest, kA); }) == TxnConflictReason::kHistoryLimit);
    assert(KeptValueAt(*history, *newest, ChunkCoord{2, 0}) == 7);
    // One more past it unregisters the last one; nothing is kept then.
    write(ChunkCoord{3, 0}, 114);
    assert(history->RegisteredCount() == 0U);
    assert(history->KeptCount() == 0U);
    assert(history->OpenCount() == 0U);
    assert(ConflictOf([&] { (void)history->ChangedSince(*newest, kA); }) == TxnConflictReason::kHistoryLimit);
}

// Past the limit only snapshots that need the state go: once none older
// than the write is left, a newer one stays registered.
void TestByteLimitSparesSnapshotsThatDoNotNeedTheState() {
    auto history = std::make_shared<TxnHistory>(200U);
    std::atomic<std::uint64_t> clock{101};
    auto older = history->Register(history, clock, kFarDeadline);  // S=100
    clock.store(201);
    auto newer = history->Register(history, clock, kFarDeadline);  // S=200
    // A write whose version 150 is below the newer snapshot, with a state
    // larger than the whole limit.
    auto reservation = TxnHistory::Reserve(kA, 150, StateOf(7, 149, 1000));
    history->Publish(reservation);
    assert(!reservation.empty());
    assert(history->KeptCount() == 0U);
    assert(history->RegisteredCount() == 1U);
    assert(ConflictOf([&] { (void)KeptValueAt(*history, *older, kA); }) == TxnConflictReason::kHistoryLimit);
    assert(KeptValueAt(*history, *newer, kA) == -1);
    assert(history->OpenCount() == 1U);
}

void TestDurationLimit() {
    auto history = std::make_shared<TxnHistory>(1U << 20U);
    std::atomic<std::uint64_t> clock{5};
    auto expired = history->Register(history, clock, std::chrono::steady_clock::now() - std::chrono::milliseconds(1));
    auto live = history->Register(history, clock, kFarDeadline);
    // The next history operation unregisters it.
    Write(*history, kA, 7, 1);
    assert(history->RegisteredCount() == 1U);
    assert(ConflictOf([&] { (void)KeptValueAt(*history, *expired, kA); }) == TxnConflictReason::kDuration);
    assert(KeptValueAt(*history, *live, kA) == 1);
}

void TestIntentRecord() {
    chunkdb::TxnIntent intent{
        .state = chunkdb::TxnIntentState::kRollback,
        .version = 123456789,
        .entries = {
            {.coord = ChunkCoord{-5, 7}, .wal_boundary = 0},
            {.coord = ChunkCoord{1LL << 40, -(1LL << 40)}, .wal_boundary = 4242},
        },
    };
    const auto bytes = chunkdb::SerializeTxnIntent(intent);
    assert(bytes.size() == 20U + 24U * 2U);
    assert(bytes[0] == 'C' && bytes[1] == 'K' && bytes[2] == 'T' && bytes[3] == 'B');
    chunkdb::TxnIntent parsed;
    assert(chunkdb::TryParseTxnIntent(bytes, &parsed));
    assert(parsed.state == chunkdb::TxnIntentState::kRollback);
    assert(parsed.version == intent.version);
    assert(parsed.entries.size() == 2U);
    assert(parsed.entries[0].coord == intent.entries[0].coord && parsed.entries[0].wal_boundary == 0U);
    assert(parsed.entries[1].coord == intent.entries[1].coord && parsed.entries[1].wal_boundary == 4242U);

    intent.state = chunkdb::TxnIntentState::kCommitted;
    const auto committed = chunkdb::SerializeTxnIntent(intent);
    assert(committed[3] == 'C');
    assert(chunkdb::TryParseTxnIntent(committed, &parsed) && parsed.state == chunkdb::TxnIntentState::kCommitted);

    // Any damage is refused: a flipped byte, a cut, a wrong count.
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        auto damaged = bytes;
        damaged[i] ^= 0x01U;
        assert(!chunkdb::TryParseTxnIntent(damaged, &parsed));
    }
    auto cut = bytes;
    cut.pop_back();
    assert(!chunkdb::TryParseTxnIntent(cut, &parsed));
    assert(!chunkdb::TryParseTxnIntent({}, &parsed));

    assert(chunkdb::IsTxnIntentFileName("txn-17.rollback"));
    assert(!chunkdb::IsTxnIntentFileName("txn-.rollback"));
    assert(!chunkdb::IsTxnIntentFileName("txn-1x.rollback"));
    assert(!chunkdb::IsTxnIntentFileName("txn-17.rollback.tmp.1.2.3.4"));
    assert(!chunkdb::IsTxnIntentFileName("L_0_0__C_0_0.wal.rollback"));
    assert(chunkdb::IsTxnIntentArtifactName("txn-17.rollback.tmp.1.2.3.4"));
    assert(chunkdb::TxnIntentPath("/d", 17).filename() == "txn-17.rollback");

    // A read-only load's boundary: the smallest a pending CKTB sets.
    chunkdb::TxnIntent other{
        .state = chunkdb::TxnIntentState::kRollback,
        .version = 9,
        .entries = {{.coord = ChunkCoord{-5, 7}, .wal_boundary = 60}},
    };
    const auto boundary = chunkdb::TxnRollbackBoundaryForChunk(
        {chunkdb::SerializeTxnIntent(other), bytes, committed}, ChunkCoord{1LL << 40, -(1LL << 40)});
    assert(boundary.has_value() && *boundary == 4242U);
    const auto both = chunkdb::TxnRollbackBoundaryForChunk(
        {chunkdb::SerializeTxnIntent(other), bytes}, ChunkCoord{-5, 7});
    assert(both.has_value() && *both == 0U);
    assert(!chunkdb::TxnRollbackBoundaryForChunk({committed}, ChunkCoord{-5, 7}).has_value());
}

}  // namespace

int main() {
    TestRegistrationTakesTheLastIssuedVersion();
    TestKeepRuleReadAtSnapshotAndPrune();
    TestValidateAndEnd();
    TestByteLimitUnregistersTheOldest();
    TestByteLimitSparesSnapshotsThatDoNotNeedTheState();
    TestDurationLimit();
    TestIntentRecord();
    return 0;
}
