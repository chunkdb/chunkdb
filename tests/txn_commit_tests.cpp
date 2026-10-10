// Transaction commits (docs/design/TRANSACTIONS_DESIGN.md): all chunks or none,
// conflicts, serializability, limits, failures before the commit point,
// repair failure, the commit record, recovery of intents and how read-only
// processes and chunkdb_verify see them.

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "txn_history.hpp"
#include "txn_test_utils.hpp"
#include "verify.hpp"

namespace {

using chunkdb::ChunkCoord;
using chunkdb::ChunkState;
using chunkdb::ChunkStore;
using chunkdb::TransactionConflictError;
using chunkdb::TxnChunkWrite;
using chunkdb::TxnConflictReason;
using chunkdb::test::ScopedTempDir;
using chunkdb::txn_test::Config;
using chunkdb::txn_test::CounterOf;
using chunkdb::txn_test::HasTxnIntent;
using chunkdb::txn_test::kTxnDuration;
using chunkdb::txn_test::ReadCounter;
using chunkdb::txn_test::ScopedEnv;
using chunkdb::txn_test::SetCounter;
using chunkdb::txn_test::Throws;
using chunkdb::txn_test::ThrowsAs;
using chunkdb::txn_test::Transfer;
using chunkdb::txn_test::WriteCounter;

constexpr ChunkCoord kA{0, 0};
constexpr ChunkCoord kB{1, 0};
constexpr ChunkCoord kC{2, 0};
constexpr ChunkCoord kD{3, 0};

std::uintmax_t FileSizeOr(const std::filesystem::path& path, std::uintmax_t absent) {
    std::error_code ec;
    const auto size = std::filesystem::file_size(path, ec);
    return ec ? absent : size;
}

std::filesystem::path WalOf(const ChunkStore& store, const ChunkCoord& coord) {
    return chunkdb::ChunkWalPath(store.data_dir(), store.geometry(), coord);
}

// The counters to write: each chunk's state as of the snapshot with a new
// counter.
std::vector<TxnChunkWrite> Writes(
    ChunkStore& store,
    const chunkdb::TxnSnapshot& snapshot,
    const std::vector<std::pair<ChunkCoord, std::uint32_t>>& counters) {
    std::vector<TxnChunkWrite> writes;
    for (const auto& [coord, value] : counters) {
        auto state = store.ReadChunkStateAt(snapshot, coord.x, coord.y);
        SetCounter(&state, value);
        writes.push_back(TxnChunkWrite{.coord = coord, .state = std::move(state)});
    }
    return writes;
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

bool Ended(ChunkStore& store, const chunkdb::TxnSnapshot& snapshot) {
    return ThrowsAs<std::invalid_argument>([&] { (void)store.ReadChunkStateAt(snapshot, 0, 0); });
}

void TestAtomicCommit(chunkdb::DurabilityMode mode) {
    ScopedTempDir dir("chunkdb-txn-commit-atomic");
    auto config = Config(dir.path(), mode);
    std::uint64_t version = 0;
    std::uint64_t version_d = 0;
    {
        ChunkStore store(config);
        WriteCounter(store, kA, 10);
        WriteCounter(store, kB, 20);
        WriteCounter(store, kD, 40);
        version_d = store.GetChunkVersion(kD.x, kD.y);
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        // D is written unchanged, so it is skipped.
        auto writes = Writes(store, *snapshot, {{kA, 5}, {kB, 25}, {kC, 7}, {kD, 40}});
        version = store.CommitTransaction(*snapshot, {kA, kB}, std::move(writes));
        assert(version > 0U);
        assert(store.GetChunkVersion(kA.x, kA.y) == version);
        assert(store.GetChunkVersion(kB.x, kB.y) == version);
        assert(store.GetChunkVersion(kC.x, kC.y) == version);
        assert(store.GetChunkVersion(kD.x, kD.y) == version_d);
        assert(ReadCounter(store, kA) == 5U && ReadCounter(store, kB) == 25U && ReadCounter(store, kC) == 7U);
        assert(Ended(store, *snapshot));
        assert(store.TxnRegisteredCountForTests() == 0U);
        assert(!HasTxnIntent(dir.path()));
    }
    // Durable and with the same versions after a restart.
    ChunkStore reopened(config);
    assert(ReadCounter(reopened, kA) == 5U && ReadCounter(reopened, kB) == 25U && ReadCounter(reopened, kC) == 7U);
    assert(ReadCounter(reopened, kD) == 40U);
    assert(reopened.GetChunkVersion(kA.x, kA.y) == version);
    assert(reopened.GetChunkVersion(kC.x, kC.y) == version);
    assert(reopened.GetChunkVersion(kD.x, kD.y) == version_d);

    // Nothing written, or nothing changed: version 0 and the transaction
    // ends.
    auto empty = reopened.BeginTxnSnapshot(kTxnDuration);
    assert(reopened.CommitTransaction(*empty, {kA}, {}) == 0U);
    assert(Ended(reopened, *empty));
    auto same = reopened.BeginTxnSnapshot(kTxnDuration);
    assert(reopened.CommitTransaction(*same, {}, Writes(reopened, *same, {{kA, 5}})) == 0U);
    assert(reopened.GetChunkVersion(kA.x, kA.y) == version);
}

// Text values change through VAR_PUT and VAR_DEL records and replay.
void TestCommitTextValues() {
    ScopedTempDir dir("chunkdb-txn-commit-text");
    auto config = Config(dir.path(), chunkdb::DurabilityMode::kRelaxed, 8);
    chunkdb::Column id;
    id.id = 1;
    id.name = "id";
    id.type = chunkdb::ColumnType{.kind = chunkdb::ColumnKind::kUnsigned, .size = 8};
    chunkdb::Column note;
    note.id = 2;
    note.name = "note";
    note.type = chunkdb::ColumnType{.kind = chunkdb::ColumnKind::kText, .size = 16};
    note.nullable = true;
    config.schema = chunkdb::TableSchema{.version = 1, .next_column_id = 3, .columns = {id, note}};
    const auto row = [](std::uint64_t value, std::string text) {
        return std::vector<chunkdb::ColumnAssignment>{{.column = "id", .value = value}, {.column = "note", .value = std::move(text)}};
    };
    const auto note_of = [](ChunkStore& store, std::int64_t x) -> std::optional<std::string> {
        const auto block = store.GetBlock(x, 0);
        if (!block.has_value()) {
            return std::nullopt;
        }
        return std::get<std::string>((*block)[1]);
    };
    {
        ChunkStore store(config);
        store.SetBlock(0, 0, row(1, "keep"));
        store.SetBlock(1, 0, row(2, "change"));
        store.SetBlock(2, 0, row(3, "drop"));

        // The transaction's private copy: change block 1, remove block 2,
        // add block 3.
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        auto state = store.ReadChunkStateAt(*snapshot, 0, 0);
        ChunkState copy;
        {
            // Built through a store of the same columns.
            auto scratch_config = config;
            scratch_config.data_dir = dir.path() / "copy";
            ChunkStore copy_store(scratch_config);
            (void)copy_store.WriteChunkState(0, 0, state, std::nullopt);
            copy_store.SetBlock(1, 0, row(20, "changed"));
            copy_store.UnsetBlock(2, 0);
            copy_store.SetBlock(3, 0, row(4, "added"));
            copy = copy_store.ReadChunkState(0, 0);
        }
        const auto version =
            store.CommitTransaction(*snapshot, {}, {TxnChunkWrite{.coord = kA, .state = std::move(copy)}});
        assert(version > 0U);
        assert(note_of(store, 0) == "keep");
        assert(note_of(store, 1) == "changed");
        assert(!note_of(store, 2).has_value());
        assert(note_of(store, 3) == "added");
    }
    ChunkStore reopened(config);
    assert(note_of(reopened, 0) == "keep");
    assert(note_of(reopened, 1) == "changed");
    assert(!note_of(reopened, 2).has_value());
    assert(note_of(reopened, 3) == "added");
}

// Snapshot readers never see half of a commit.
void TestCommitVisibleAsOne() {
    ScopedTempDir dir("chunkdb-txn-commit-visible");
    ChunkStore store(Config(dir.path(), chunkdb::DurabilityMode::kRelaxed));
    WriteCounter(store, kA, 1000);
    WriteCounter(store, kB, 1000);
    std::atomic<bool> done{false};
    std::atomic<std::uint64_t> checks{0};
    std::thread reader([&] {
        while (!done.load()) {
            auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
            const auto a = CounterOf(store.ReadChunkStateAt(*snapshot, kA.x, kA.y));
            const auto b = CounterOf(store.ReadChunkStateAt(*snapshot, kB.x, kB.y));
            assert(a + b == 2000U);
            const auto area = store.ReadChunkRangeAt(*snapshot, kA.x, kA.y, kB.x, kB.y);
            assert(area.size() == 2U);
            std::uint32_t sum = 0;
            for (const auto& entry : area) {
                sum += CounterOf(ChunkState{
                    .version = entry.version,
                    .payload = entry.payload,
                    .presence_bitmap = entry.presence_bitmap,
                    .vars = {},
                });
            }
            assert(sum == 2000U);
            checks.fetch_add(1);
        }
    });
    for (std::uint32_t i = 0; i < 100; ++i) {
        if (i % 2 == 0) {
            (void)Transfer(store, kA, kB, 1 + i % 7);
        } else {
            (void)Transfer(store, kB, kA, 1 + i % 5);
        }
    }
    done.store(true);
    reader.join();
    assert(checks.load() > 0U);
    assert(ReadCounter(store, kA) + ReadCounter(store, kB) == 2000U);
}

void TestConflicts() {
    ScopedTempDir dir("chunkdb-txn-commit-conflicts");
    ChunkStore store(Config(dir.path()));
    WriteCounter(store, kA, 1);
    WriteCounter(store, kB, 2);
    const auto version_b = store.GetChunkVersion(kB.x, kB.y);

    // A chunk the transaction read changed.
    {
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        auto writes = Writes(store, *snapshot, {{kB, 3}});
        (void)store.ReadChunkStateAt(*snapshot, kA.x, kA.y);
        WriteCounter(store, kA, 10);
        assert(ConflictReasonOf([&] { (void)store.CommitTransaction(*snapshot, {kA}, std::move(writes)); }) ==
               TxnConflictReason::kChunkChanged);
        assert(ReadCounter(store, kB) == 2U && store.GetChunkVersion(kB.x, kB.y) == version_b);
        assert(Ended(store, *snapshot));
        assert(!HasTxnIntent(dir.path()));
    }
    // A chunk it wrote changed (a blind write).
    {
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        auto writes = Writes(store, *snapshot, {{kB, 3}});
        WriteCounter(store, kB, 20);
        assert(ConflictReasonOf([&] { (void)store.CommitTransaction(*snapshot, {}, std::move(writes)); }) ==
               TxnConflictReason::kChunkChanged);
        assert(ReadCounter(store, kB) == 20U);
    }
    // A chunk it did not touch changed: no conflict.
    {
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        auto writes = Writes(store, *snapshot, {{kB, 21}});
        WriteCounter(store, kC, 30);
        assert(store.CommitTransaction(*snapshot, {kB}, std::move(writes)) > 0U);
        assert(ReadCounter(store, kB) == 21U);
    }
    // A commit keeps what other open transactions need.
    {
        auto reader = store.BeginTxnSnapshot(kTxnDuration);
        auto writer = store.BeginTxnSnapshot(kTxnDuration);
        assert(store.CommitTransaction(*writer, {kB}, Writes(store, *writer, {{kB, 22}})) > 0U);
        assert(CounterOf(store.ReadChunkStateAt(*reader, kB.x, kB.y)) == 21U);
        assert(store.ChunkChangedSince(*reader, kB.x, kB.y));
        // ... and conflicts with them.
        assert(ConflictReasonOf([&] {
                   (void)store.CommitTransaction(*reader, {kB}, Writes(store, *reader, {{kA, 99}}));
               }) == TxnConflictReason::kChunkChanged);
    }
    // A snapshot past its duration limit cannot commit.
    {
        auto snapshot = store.BeginTxnSnapshot(std::chrono::milliseconds(1));
        auto writes = Writes(store, *snapshot, {{kA, 50}});
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        assert(ConflictReasonOf([&] { (void)store.CommitTransaction(*snapshot, {}, std::move(writes)); }) ==
               TxnConflictReason::kDuration);
        assert(ReadCounter(store, kA) == 10U);
    }
}

// Two transactions that each read what the other writes: not both commit.
void TestWriteSkewRefused() {
    ScopedTempDir dir("chunkdb-txn-commit-skew");
    ChunkStore store(Config(dir.path(), chunkdb::DurabilityMode::kRelaxed));
    constexpr int kRounds = 100;
    for (int round = 0; round < kRounds; ++round) {
        WriteCounter(store, kA, 0);
        WriteCounter(store, kB, 0);
        std::atomic<int> ready{0};
        std::atomic<int> committed{0};
        const auto run = [&](const ChunkCoord& write_to) {
            auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
            const auto a = CounterOf(store.ReadChunkStateAt(*snapshot, kA.x, kA.y));
            const auto b = CounterOf(store.ReadChunkStateAt(*snapshot, kB.x, kB.y));
            // Write only when neither is set yet: as one serial order, only
            // one of the two can.
            assert(a == 0U && b == 0U);
            auto writes = Writes(store, *snapshot, {{write_to, 1}});
            ready.fetch_add(1);
            while (ready.load() < 2) {
                std::this_thread::yield();
            }
            try {
                (void)store.CommitTransaction(*snapshot, {kA, kB}, std::move(writes));
                committed.fetch_add(1);
            } catch (const TransactionConflictError&) {
            }
        };
        std::thread first(run, kA);
        std::thread second(run, kB);
        first.join();
        second.join();
        assert(committed.load() == 1);
        assert(ReadCounter(store, kA) + ReadCounter(store, kB) == 1U);
    }
}

void TestLimits() {
    ScopedTempDir dir("chunkdb-txn-commit-limits");
    ChunkStore store(Config(dir.path()));
    auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
    std::vector<std::pair<ChunkCoord, std::uint32_t>> many;
    for (std::int64_t x = 0; x <= static_cast<std::int64_t>(chunkdb::kMaxTxnWrittenChunks); ++x) {
        many.emplace_back(ChunkCoord{x, 5}, 1);
    }
    assert(ThrowsAs<std::invalid_argument>(
        [&] { (void)store.CommitTransaction(*snapshot, {}, Writes(store, *snapshot, many)); }));
    std::vector<ChunkCoord> reads;
    for (std::int64_t x = 0; x <= static_cast<std::int64_t>(chunkdb::kMaxTxnReadChunks); ++x) {
        reads.push_back(ChunkCoord{x, 7});
    }
    assert(ThrowsAs<std::invalid_argument>([&] { (void)store.CommitTransaction(*snapshot, reads, {}); }));
    // The same chunk twice, and a state of the wrong size.
    assert(ThrowsAs<std::invalid_argument>([&] {
        (void)store.CommitTransaction(*snapshot, {}, Writes(store, *snapshot, {{kA, 1}, {kA, 2}}));
    }));
    assert(ThrowsAs<std::invalid_argument>([&] {
        auto writes = Writes(store, *snapshot, {{kA, 1}});
        writes[0].state.payload.pop_back();
        (void)store.CommitTransaction(*snapshot, {}, std::move(writes));
    }));
    // Each left the transaction open and nothing changed.
    assert(store.TxnRegisteredCountForTests() == 1U);
    assert(ReadCounter(store, ChunkCoord{0, 5}) == 0U);
    reads.pop_back();
    if constexpr (!chunkdb::txn_test::kThreadSanitizer) {
        many.pop_back();
        // A commit with writes locks both sets: exercise the full write
        // limit together with the maximum, disjoint read set.
        const auto version = store.CommitTransaction(*snapshot, reads, Writes(store, *snapshot, many));
        assert(version > 0U);
        for (const auto& [coord, value] : many) {
            assert(ReadCounter(store, coord) == value);
            assert(store.GetChunkVersion(coord.x, coord.y) == version);
        }
        assert(Ended(store, *snapshot));
        assert(store.TxnRegisteredCountForTests() == 0U);
        assert(!HasTxnIntent(dir.path()));
        std::printf(
            "txn store full-size commit passed (%zu written, %zu read chunks)\n", many.size(), reads.size());
        return;
    }
    // Invalid arguments leave the snapshot usable for a valid commit.
    // Keep the commit within TSan's detector's lock capacity.
    assert(store.CommitTransaction(*snapshot, {kA}, Writes(store, *snapshot, {{kA, 1}})) > 0U);
    assert(ReadCounter(store, kA) == 1U);
    assert(Ended(store, *snapshot));
    assert(ReadCounter(store, ChunkCoord{0, 5}) == 0U);

    // The maximum read set is accepted by a read-only commit, which does
    // not need to hold all those chunks' locks at once.
    auto reader = store.BeginTxnSnapshot(kTxnDuration);
    assert(store.CommitTransaction(*reader, reads, {}) == 0U);
    assert(Ended(store, *reader));
    std::puts("txn store TSan limit cases passed");
}

// A store that is fail-closed refuses commits.
void TestPoisonedStoreRefused() {
    ScopedTempDir dir("chunkdb-txn-commit-poisoned");
    ChunkStore store(Config(dir.path()));
    WriteCounter(store, kA, 1);
    {
        ScopedEnv sync_fail("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
        ScopedEnv resize_fail("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1");
        assert(ThrowsAs<chunkdb::WriteOutcomeUnknownError>([&] { WriteCounter(store, kA, 2); }));
    }
    auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
    auto writes = Writes(store, *snapshot, {{kB, 5}});
    bool refused = false;
    try {
        (void)store.CommitTransaction(*snapshot, {}, std::move(writes));
    } catch (const TransactionConflictError&) {
        assert(false);
    } catch (const std::runtime_error& error) {
        refused = std::string(error.what()).find("fail-closed") != std::string::npos;
    }
    assert(refused);
    assert(Ended(store, *snapshot));
    assert(ReadCounter(store, kB) == 0U);
}

// A failure while the WAL boundaries are set changes nothing.
void TestBoundaryFailureChangesNothing() {
    ScopedTempDir dir("chunkdb-txn-commit-boundary");
    auto config = Config(dir.path(), chunkdb::DurabilityMode::kRelaxed);
    {
        ChunkStore store(config);
        WriteCounter(store, kA, 1);
        WriteCounter(store, kB, 2);
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        auto writes = Writes(store, *snapshot, {{kA, 10}, {kB, 20}});
        {
            ScopedEnv fail("CHUNKDB_FAILPOINT_TXN_BOUNDARY_FAIL_ONCE", "1");
            assert(Throws([&] { (void)store.CommitTransaction(*snapshot, {}, std::move(writes)); }));
        }
        assert(ReadCounter(store, kA) == 1U && ReadCounter(store, kB) == 2U);
        assert(!HasTxnIntent(dir.path()));
        assert(Ended(store, *snapshot));
        (void)Transfer(store, kA, kB, 1);
        assert(ReadCounter(store, kA) == 0U && ReadCounter(store, kB) == 3U);
    }
    ChunkStore reopened(config);
    assert(ReadCounter(reopened, kA) == 0U && ReadCounter(reopened, kB) == 3U);
}

// A failure while the frames are appended goes back to the boundaries.
void TestAppendFailureChangesNothing() {
    for (const auto mode : {chunkdb::DurabilityMode::kRelaxed, chunkdb::DurabilityMode::kFsyncWal,
                            chunkdb::DurabilityMode::kFsyncCheckpoint}) {
        for (std::size_t fail_at = 0; fail_at <= 2; ++fail_at) {
            ScopedTempDir dir("chunkdb-txn-commit-append");
            auto config = Config(dir.path(), mode);
            config.checkpoint_update_interval = 1000;
            config.checkpoint_wal_bytes = 1U << 20U;
            {
                ChunkStore store(config);
                WriteCounter(store, kA, 1);
                store.WalBarrier();
                const auto wal_a = FileSizeOr(WalOf(store, kA), 0);
                const auto version_a = store.GetChunkVersion(kA.x, kA.y);
                auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
                // A has a WAL, C gets a new one.
                auto writes = Writes(store, *snapshot, {{kA, 10}, {kC, 30}});
                {
                    ScopedEnv fail("CHUNKDB_FAILPOINT_TXN_APPEND_FAIL_AT", std::to_string(fail_at));
                    assert(Throws([&] { (void)store.CommitTransaction(*snapshot, {}, std::move(writes)); }));
                }
                assert(ReadCounter(store, kA) == 1U && ReadCounter(store, kC) == 0U);
                assert(store.GetChunkVersion(kA.x, kA.y) == version_a);
                assert(FileSizeOr(WalOf(store, kA), 0) == wal_a);
                assert(!std::filesystem::exists(WalOf(store, kC)));
                assert(!HasTxnIntent(dir.path()));
                // The store goes on.
                auto next = store.BeginTxnSnapshot(kTxnDuration);
                assert(store.CommitTransaction(*next, {}, Writes(store, *next, {{kC, 31}})) > 0U);
                WriteCounter(store, kA, 2);
            }
            ChunkStore reopened(config);
            assert(ReadCounter(reopened, kA) == 2U && ReadCounter(reopened, kC) == 31U);
        }
    }
}

// When the WALs cannot be repaired the chunks stay cached and the store
// fail-closed; the next start removes the transaction.
void TestRepairFailureFailsClosed() {
    ScopedTempDir dir("chunkdb-txn-commit-repair");
    auto config = Config(dir.path());
    config.checkpoint_update_interval = 1000;
    config.checkpoint_wal_bytes = 1U << 20U;
    config.max_loaded_chunks = 4;
    {
        ChunkStore store(config);
        WriteCounter(store, kA, 1);
        WriteCounter(store, kB, 2);
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        auto writes = Writes(store, *snapshot, {{kA, 10}, {kB, 20}});
        bool fail_closed = false;
        {
            ScopedEnv fail("CHUNKDB_FAILPOINT_TXN_APPEND_FAIL_AT", "2");
            ScopedEnv repair_fail("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1");
            try {
                (void)store.CommitTransaction(*snapshot, {}, std::move(writes));
            } catch (const chunkdb::WriteOutcomeUnknownError&) {
                assert(false);
            } catch (const std::runtime_error& error) {
                fail_closed = std::string(error.what()).find("fail-closed") != std::string::npos;
            }
        }
        assert(fail_closed);
        // Memory never took the transaction; the intent stays for the next
        // start.
        assert(ReadCounter(store, kA) == 1U && ReadCounter(store, kB) == 2U);
        assert(HasTxnIntent(dir.path()));
        assert(Throws([&] { WriteCounter(store, kC, 1); }));
        auto next = store.BeginTxnSnapshot(kTxnDuration);
        assert(Throws([&] { (void)store.CommitTransaction(*next, {}, Writes(store, *next, {{kC, 3}})); }));
        // The chunks are not evicted under cache pressure.
        for (std::int64_t y = 10; y < 30; ++y) {
            (void)store.GetChunkVersion(0, y);
        }
        assert(store.IsChunkLoadedForTests(kA.x, kA.y));
        assert(store.IsChunkLoadedForTests(kB.x, kB.y));
    }
    {
        ChunkStore reopened(config);
        assert(!HasTxnIntent(dir.path()));
        assert(ReadCounter(reopened, kA) == 1U && ReadCounter(reopened, kB) == 2U);
        (void)Transfer(reopened, kA, kB, 1);
    }
    ChunkStore again(config);
    assert(ReadCounter(again, kA) == 0U && ReadCounter(again, kB) == 3U);
}

// A commit record that is visible but cannot be made durable: the outcome
// is unknown and the store fail-closed; once visible it is never undone.
void TestCommitRecordNotDurable() {
    ScopedTempDir dir("chunkdb-txn-commit-record");
    auto config = Config(dir.path());
    {
        ChunkStore store(config);
        WriteCounter(store, kA, 1);
        // A failed directory sync that the retry completes is a commit.
        {
            auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
            ScopedEnv fail("CHUNKDB_FAILPOINT_TXN_COMMIT_INTENT_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
            assert(store.CommitTransaction(*snapshot, {}, Writes(store, *snapshot, {{kA, 2}})) > 0U);
            assert(!HasTxnIntent(dir.path()));
        }
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        ScopedEnv fail("CHUNKDB_FAILPOINT_TXN_COMMIT_INTENT_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
        ScopedEnv retry_fail("CHUNKDB_FAILPOINT_TXN_COMMIT_INTENT_COMPLETION_SYNC_FAIL_ONCE", "1");
        assert(ThrowsAs<chunkdb::WriteOutcomeUnknownError>(
            [&] { (void)store.CommitTransaction(*snapshot, {}, Writes(store, *snapshot, {{kA, 3}, {kB, 4}})); }));
        assert(ReadCounter(store, kA) == 3U && ReadCounter(store, kB) == 4U);
        assert(Throws([&] { WriteCounter(store, kC, 1); }));
    }
    ChunkStore reopened(config);
    assert(ReadCounter(reopened, kA) == 3U && ReadCounter(reopened, kB) == 4U);
    assert(!HasTxnIntent(dir.path()));
}

// Relaxed mode before any FLUSH WAL: a checkpoint writes the image without a
// sync and removes the WAL without one. A rollback boundary (a transaction
// or a conditional write) makes the image, the WAL and their directories
// durable first, so the frame never lands on an image a power loss can lose.
void TestRelaxedBoundaryMakesImageDurable() {
    ScopedTempDir dir("chunkdb-txn-commit-relaxed-image");
    auto config = Config(dir.path(), chunkdb::DurabilityMode::kRelaxed);
    config.checkpoint_update_interval = 1000;
    config.checkpoint_wal_bytes = 1U << 20U;
    config.wal_group_commit_updates = 1;
    {
        ChunkStore store(config);
        WriteCounter(store, kA, 1);
        store.CheckpointForTests(kA.x, kA.y);  // unsynced image, WAL removed
        WriteCounter(store, kA, 2);            // a new WAL, unsynced
        assert(store.UnsyncedTrackedCountForTests() > 0U);
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        assert(store.CommitTransaction(*snapshot, {kA}, Writes(store, *snapshot, {{kA, 3}})) > 0U);
        assert(store.UnsyncedTrackedCountForTests() == 0U);

        // The conditional write path, on another chunk.
        WriteCounter(store, kB, 1);
        store.CheckpointForTests(kB.x, kB.y);
        WriteCounter(store, kB, 2);
        assert(store.UnsyncedTrackedCountForTests() > 0U);
        auto state = store.ReadChunkState(kB.x, kB.y);
        SetCounter(&state, 3);
        assert(store.CasChunkStateBytes(kB.x, kB.y, state.version, state.payload, state.presence_bitmap).ok);
        // Only the conditional frame itself, which relaxed mode appends
        // without a sync, is left for a barrier: the image and the
        // directories went before its intent.
        assert(store.UnsyncedTrackedCountForTests() == 1U);
    }
    ChunkStore reopened(config);
    assert(ReadCounter(reopened, kA) == 3U && ReadCounter(reopened, kB) == 3U);
}

// Few WAL streams, all held by chunks the commit only reads: the commit
// writes its batch and frames through streams of its own instead of waiting
// for the pool, while other threads that need streams skip the chunks it
// holds.
void TestFewWalStreams() {
    ScopedTempDir dir("chunkdb-txn-commit-streams");
    auto config = Config(dir.path(), chunkdb::DurabilityMode::kRelaxed);
    config.max_open_wal_streams = 2;
    config.wal_group_commit_updates = 2;
    config.checkpoint_update_interval = 1000;
    config.checkpoint_wal_bytes = 1U << 20U;
    ChunkStore store(config);
    // Two flushes each: R1 and R2 keep open streams; W only stages.
    const ChunkCoord r1{0, 2};
    const ChunkCoord r2{1, 2};
    const ChunkCoord w{2, 2};
    WriteCounter(store, r1, 1);
    WriteCounter(store, r1, 11);
    WriteCounter(store, r2, 2);
    WriteCounter(store, r2, 12);
    WriteCounter(store, w, 3);
    assert(store.OpenWalStreamCountForTests() == 2U);

    std::atomic<bool> stop{false};
    std::thread other([&] {
        std::uint32_t value = 0;
        while (!stop.load()) {
            try {
                WriteCounter(store, ChunkCoord{5 + static_cast<std::int64_t>(value % 3), 5}, value);
            } catch (const std::runtime_error& error) {
                // A clean refusal while every stream belongs to locked
                // chunks; nothing else is expected.
                assert(std::string(error.what()).find("WAL stream capacity") != std::string::npos);
            }
            ++value;
        }
    });
    for (std::uint32_t round = 0; round < 20; ++round) {
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        const auto a = CounterOf(store.ReadChunkStateAt(*snapshot, r1.x, r1.y));
        const auto b = CounterOf(store.ReadChunkStateAt(*snapshot, r2.x, r2.y));
        const auto started = std::chrono::steady_clock::now();
        assert(store.CommitTransaction(*snapshot, {r1, r2}, Writes(store, *snapshot, {{w, a + b + round}})) > 0U);
        // Never the pool's capacity wait.
        assert(std::chrono::steady_clock::now() - started < std::chrono::milliseconds(900));
        WriteCounter(store, r1, 11 + round);  // a stream for R1 again
    }
    stop.store(true);
    other.join();
    // The last round read R1 = 11 + 18 and R2 = 12.
    assert(ReadCounter(store, w) == 11U + 18U + 12U + 19U);
}

// Failures after the commit point never surface as an error: the commit
// stands and reports its version.
void TestFailureAfterCommitPoint() {
    ScopedTempDir dir("chunkdb-txn-commit-after-point");
    auto config = Config(dir.path());
    {
        ChunkStore store(config);
        {
            auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
            ScopedEnv fail("CHUNKDB_FAILPOINT_TXN_INTENT_UNLINK_FAIL_ONCE", "1");
            assert(store.CommitTransaction(*snapshot, {}, Writes(store, *snapshot, {{kA, 1}, {kB, 2}})) > 0U);
            // The commit form stays; it keeps the frames at the next start.
            assert(HasTxnIntent(dir.path()));
        }
        {
            auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
            ScopedEnv fail("CHUNKDB_FAILPOINT_TXN_AFTER_COMMIT_THROW_ONCE", "1");
            assert(store.CommitTransaction(*snapshot, {}, Writes(store, *snapshot, {{kA, 3}})) > 0U);
        }
        {
            // Eleven updates of C: the commit's twelfth reaches the
            // checkpoint trigger (8 plus its hysteresis), and the checkpoint
            // fails.
            for (std::uint32_t i = 0; i < 11; ++i) {
                WriteCounter(store, kC, 100 + i);
            }
            auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
            auto writes = Writes(store, *snapshot, {{kC, 9}});
            ScopedEnv fail("CHUNKDB_FAILPOINT_CHECKPOINT_BEFORE_IMAGE_REPLACE_ONCE", "1");
            assert(store.CommitTransaction(*snapshot, {}, std::move(writes)) > 0U);
            assert(std::getenv("CHUNKDB_FAILPOINT_CHECKPOINT_BEFORE_IMAGE_REPLACE_ONCE") == nullptr ||
                   std::string(std::getenv("CHUNKDB_FAILPOINT_CHECKPOINT_BEFORE_IMAGE_REPLACE_ONCE")).empty());
        }
        assert(ReadCounter(store, kA) == 3U && ReadCounter(store, kB) == 2U && ReadCounter(store, kC) == 9U);
        WriteCounter(store, kD, 4);
    }
    ChunkStore reopened(config);
    assert(!HasTxnIntent(dir.path()));
    assert(ReadCounter(reopened, kA) == 3U && ReadCounter(reopened, kB) == 2U && ReadCounter(reopened, kC) == 9U);
    assert(ReadCounter(reopened, kD) == 4U);
}

void WriteFile(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(out.good());
}

std::string VerifyOutput(const std::filesystem::path& data_dir) {
    std::ostringstream out;
    (void)chunkdb::VerifyDataDirectory(data_dir, out);
    return out.str();
}

// Intents left on disk: a read-only process reads as of their decision
// without changing a file, chunkdb_verify reports them, and a writer's open
// resolves them.
void TestIntentsOnDisk() {
    // A data directory, so chunkdb_verify can check it; the store is its
    // default table.
    ScopedTempDir root("chunkdb-txn-commit-intents");
    const auto table_dir = root.path() / "tables" / "default";
    auto config = Config(table_dir);
    config.checkpoint_update_interval = 1000;
    config.checkpoint_wal_bytes = 1U << 20U;
    {
        auto catalog_config = chunkdb::CatalogConfigFromStoreConfig(config);
        catalog_config.data_dir = root.path();
        chunkdb::TableCatalog catalog(catalog_config);
    }
    std::uintmax_t boundary_a = 0;
    std::filesystem::path wal_a;
    std::filesystem::path wal_c;
    {
        ChunkStore store(config);
        WriteCounter(store, kA, 1);
        wal_a = WalOf(store, kA);
        wal_c = WalOf(store, kC);
        boundary_a = FileSizeOr(wal_a, 0);
    }
    {
        // What a commit that crashed before its commit point leaves.
        ChunkStore store(config);
        WriteCounter(store, kA, 10);
        WriteCounter(store, kC, 30);
    }
    const auto intent_path = chunkdb::TxnIntentPath(table_dir, 999);
    chunkdb::TxnIntent intent{
        .state = chunkdb::TxnIntentState::kRollback,
        .version = 999,
        .entries = {
            {.coord = kA, .wal_boundary = boundary_a},
            {.coord = kC, .wal_boundary = 0},
        },
    };
    const auto read_only = [&] {
        auto read_config = config;
        read_config.access_mode = chunkdb::AccessMode::kReadOnly;
        return read_config;
    };
    // The commit form: the frames count, and a listed chunk without a WAL is
    // fine.
    intent.state = chunkdb::TxnIntentState::kCommitted;
    intent.entries.push_back({.coord = kD, .wal_boundary = 77});
    WriteFile(intent_path, chunkdb::SerializeTxnIntent(intent));
    {
        ChunkStore reader(read_only());
        assert(ReadCounter(reader, kA) == 10U && ReadCounter(reader, kC) == 30U);
        const auto area = reader.ReadChunkRange(0, 0, 3, 0);
        assert(area.size() == 2U);
    }
    assert(VerifyOutput(root.path()).find("txn_commit_cleanup_pending") != std::string::npos);
    // The rollback form: the WALs end at their boundaries.
    intent.state = chunkdb::TxnIntentState::kRollback;
    intent.entries.pop_back();
    WriteFile(intent_path, chunkdb::SerializeTxnIntent(intent));
    const auto wal_bytes = FileSizeOr(wal_a, 0);
    {
        ChunkStore reader(read_only());
        assert(ReadCounter(reader, kA) == 1U && ReadCounter(reader, kC) == 0U);
        assert(reader.ReadChunkRange(0, 0, 3, 0).size() == 1U);
    }
    // Read-only changed nothing.
    assert(std::filesystem::exists(intent_path));
    assert(FileSizeOr(wal_a, 0) == wal_bytes);
    assert(VerifyOutput(root.path()).find("txn_rollback_pending") != std::string::npos);
    // A damaged intent is an error for chunkdb_verify and for a writer.
    {
        auto damaged = chunkdb::SerializeTxnIntent(intent);
        damaged[10] ^= 1U;
        const auto damaged_path = chunkdb::TxnIntentPath(table_dir, 1000);
        WriteFile(damaged_path, damaged);
        assert(VerifyOutput(root.path()).find("txn_intent_invalid") != std::string::npos);
        assert(Throws([&] { ChunkStore writer(config); }));
        std::filesystem::remove(damaged_path);
    }
    // A writer's open truncates, removes the intent and the temporary files
    // of interrupted intents, and goes on.
    const auto stale = table_dir / ".chunkdb.intents" / "txn-1001.rollback.tmp.2147483000.1.2.3";
    WriteFile(stale, {1, 2, 3});
    {
        ChunkStore writer(config);
        assert(!std::filesystem::exists(intent_path));
        assert(!std::filesystem::exists(stale));
        assert(ReadCounter(writer, kA) == 1U && ReadCounter(writer, kC) == 0U);
        assert(!std::filesystem::exists(wal_c));
        assert(FileSizeOr(wal_a, 0) == boundary_a);
        (void)Transfer(writer, kA, kB, 1);
    }
    ChunkStore again(config);
    assert(ReadCounter(again, kA) == 0U && ReadCounter(again, kB) == 1U);
    const auto output = VerifyOutput(root.path());
    assert(output.find("txn_") == std::string::npos);
}

// Every real point owns an independent slot, including the last enum value.
// Repeat to check that rearming resets reached/resumed state as well.
void TestPauseSlots() {
    chunkdb::TxnHistory history(1U << 20U);
    constexpr std::array points{
        chunkdb::TxnPausePoint::kRegisterBeforePublish,
        chunkdb::TxnPausePoint::kWriteAfterOpenCount,
        chunkdb::TxnPausePoint::kBeforePostCommitOutcome};
    static_assert(points.size() + 1U == static_cast<std::size_t>(chunkdb::TxnPausePoint::kCount));
    for (unsigned round = 0; round < 2U; ++round) {
        std::array<std::atomic<bool>, points.size()> completed{};
        std::array<std::thread, points.size()> workers;
        for (std::size_t i = 0; i < points.size(); ++i) {
            history.ArmPauseForTests(points[i]);
            workers[i] = std::thread([&, i] {
                history.PauseForTests(points[i]);
                completed[i].store(true);
            });
        }
        for (std::size_t i = 0; i < points.size(); ++i) {
            assert(history.WaitForPauseForTests(points[i]));
            assert(!completed[i].load());
        }
        for (std::size_t i = 0; i < points.size(); ++i) {
            history.ResumeForTests(points[i]);
            workers[i].join();
            assert(completed[i].load());
            for (std::size_t j = i + 1U; j < points.size(); ++j) assert(!completed[j].load());
        }
    }
}

void TestPostCommitPause() {
    ScopedTempDir dir("chunkdb-txn-commit-post-pause");
    const auto config = Config(dir.path());
    {
        ChunkStore store(config);
        WriteCounter(store, kA, 1U);
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        auto writes = Writes(store, *snapshot, {{kA, 2U}, {kB, 3U}});
        constexpr auto point = chunkdb::TxnPausePoint::kBeforePostCommitOutcome;
        store.ArmTxnPauseForTests(point);
        auto commit = std::async(std::launch::async, [&] {
            return store.CommitTransaction(*snapshot, {}, std::move(writes));
        });
        assert(store.WaitForTxnPauseForTests(point));
        assert(commit.wait_for(std::chrono::seconds(0)) == std::future_status::timeout);
        // Publication has happened and chunk locks have been released,
        // although the committing caller has not completed its bookkeeping.
        assert(ReadCounter(store, kA) == 2U && ReadCounter(store, kB) == 3U);
        const auto committed = store.GetChunkVersion(kA.x, kA.y);
        assert(committed > 0U && store.GetChunkVersion(kB.x, kB.y) == committed);
        WriteCounter(store, kA, 4U);
        assert(store.GetChunkVersion(kA.x, kA.y) > committed);
        store.ResumeTxnForTests(point);
        assert(commit.get() == committed);
        assert(ReadCounter(store, kA) == 4U && ReadCounter(store, kB) == 3U);
    }
    ChunkStore reopened(config);
    assert(ReadCounter(reopened, kA) == 4U && ReadCounter(reopened, kB) == 3U);
}

}  // namespace

int main(int argc, char** argv) {
    TestPauseSlots();
    TestPostCommitPause();
    if (argc == 2 && std::string_view(argv[1]) == "--pause-regressions") return 0;
    assert(argc == 1);
    for (const auto mode : {chunkdb::DurabilityMode::kRelaxed, chunkdb::DurabilityMode::kFsyncWal,
                            chunkdb::DurabilityMode::kFsyncCheckpoint}) {
        TestAtomicCommit(mode);
    }
    TestCommitTextValues();
    TestCommitVisibleAsOne();
    TestConflicts();
    TestWriteSkewRefused();
    TestLimits();
    TestPoisonedStoreRefused();
    TestBoundaryFailureChangesNothing();
    TestAppendFailureChangesNothing();
    TestRepairFailureFailsClosed();
    TestCommitRecordNotDurable();
    TestIntentsOnDisk();
    TestRelaxedBoundaryMakesImageDurable();
    TestFewWalStreams();
    TestFailureAfterCommitPoint();
    return 0;
}
