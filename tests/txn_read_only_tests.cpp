// BEGIN READ ONLY through the engine (docs/reference/TRANSACTION_STATEMENTS.md):
// reads, SCAN CHUNKS and AGGREGATE at one snapshot, refused writes, no read
// set, the duration and history limits, the writers' copy-cost metrics, a
// snapshot that waits for the writes it includes, and readers beside
// writers.

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "statement_control.hpp"
#include "txn_test_utils.hpp"

namespace {

using chunkdb::CommandEngine;
using chunkdb::SessionState;
using Parameters = std::vector<std::optional<std::string>>;

bool Contains(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

void ExpectReply(const std::string& reply, const std::string& want) {
    if (reply != want) {
        std::fprintf(stderr, "expected '%s', got '%s'\n", want.c_str(), reply.c_str());
        assert(false);
    }
}

void ExpectError(const std::string& reply, const std::string& part) {
    if (!reply.starts_with("-ERR ") || !Contains(reply, part)) {
        std::fprintf(stderr, "expected an error with '%s', got '%s'\n", part.c_str(), reply.c_str());
        assert(false);
    }
}

void ExpectContains(const std::string& reply, const std::string& part) {
    if (!Contains(reply, part)) {
        std::fprintf(stderr, "expected '%s' in '%s'\n", part.c_str(), reply.c_str());
        assert(false);
    }
}

std::uint64_t VersionOf(const std::string& reply) {
    if (reply.size() <= 3 || reply[0] != ':') {
        std::fprintf(stderr, "expected a version, got '%s'\n", reply.c_str());
        assert(false);
    }
    return std::stoull(reply.substr(1));
}

std::string Value(std::int64_t n) {
    return "*1\r\n:" + std::to_string(n) + "\r\n";
}

// One SCAN CHUNKS row of a 2D table without data.
std::string Row(std::int64_t x, std::int64_t y, std::uint64_t version) {
    return "*3\r\n:" + std::to_string(x) + "\r\n:" + std::to_string(y) + "\r\n:" + std::to_string(version) + "\r\n";
}

// A metric's value in SHOW METRICS.
std::uint64_t Metric(const std::string& metrics, const std::string& name) {
    const auto at = metrics.find("\n" + name + " ");
    assert(at != std::string::npos);
    return std::stoull(metrics.substr(at + name.size() + 2U));
}

struct Fixture {
    chunkdb::test::ScopedTempDir dir{"chunkdb-txn-read-only"};
    std::shared_ptr<chunkdb::TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    SessionState a;
    SessionState b;

    explicit Fixture(chunkdb::QueryLimits limits = {}, std::size_t history_bytes = chunkdb::kDefaultTxnHistoryBytes,
        std::chrono::milliseconds duration = chunkdb::EngineConfig{}.txn_max_duration) {
        chunkdb::CatalogConfig catalog_config;
        catalog_config.data_dir = dir.path();
        catalog_config.background_maintenance = false;
        catalog_config.default_options.durability_mode = chunkdb::DurabilityMode::kRelaxed;
        catalog_config.query_limits = limits;
        catalog_config.txn_history_bytes = history_bytes;
        catalog = std::make_shared<chunkdb::TableCatalog>(catalog_config);
        chunkdb::EngineConfig config;
        config.require_auth = false;
        config.server_version = "test";
        config.txn_max_duration = duration;
        engine = std::make_unique<CommandEngine>(config, catalog);
        Hello(a);
        Hello(b);
        ExpectReply(Run(a, "CREATE TABLE t (n u16 REQUIRED) CHUNK 4 x 4"), "+OK\r\n");
        ExpectReply(Run(a, "CREATE TABLE other (n u16 REQUIRED) CHUNK 4 x 4"), "+OK\r\n");
    }

    void Hello(SessionState& session) {
        assert(engine->Execute(session, "HELLO 3\r\n").starts_with("%"));
    }

    std::string Run(SessionState& session, const std::string& line, const Parameters& parameters = {}) {
        return engine->Execute(session, line + "\r\n", parameters);
    }

    std::size_t RegisteredSnapshots() {
        const auto lease = catalog->Find("t")->Acquire();
        return lease->store().TxnRegisteredCountForTests();
    }

    std::size_t KeptBytes() {
        const auto lease = catalog->Find("t")->Acquire();
        return lease->store().TxnKeptBytesForTests();
    }

    std::string Metrics() {
        const auto reply = Run(b, "SHOW METRICS");
        assert(reply.starts_with("$"));
        return reply;
    }
};

// Every read of the transaction sees the table as of its first statement:
// chunks changed, emptied or created later, on every SCAN page, in AGGREGATE
// and in point reads. COMMIT checks nothing and ends it.
void TestSnapshotScanAndAggregate() {
    Fixture f;
    const auto v0 = VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 1"));
    const auto v1 = VersionOf(f.Run(f.b, "SET BLOCK 4 0 IN t n = 2"));
    const auto data_before = f.Run(f.b, "SCAN CHUNKS FROM t WITH DATA");
    const auto sum_before = f.Run(f.b, "AGGREGATE AREA 0 0 TO 3 3 FROM t COUNT(*), SUM(n)");

    ExpectReply(f.Run(f.a, "BEGIN READ ONLY"), "+OK\r\n");
    const auto first = f.Run(f.a, "SCAN CHUNKS FROM t");
    ExpectContains(first, "$6\r\nchunks\r\n*2\r\n" + Row(0, 0, v0) + Row(1, 0, v1) + "$4\r\nmore\r\n#f\r\n");
    // The page is the snapshot's revision, so WATCH AFTER it follows on.
    ExpectContains(first, "$8\r\nposition\r\n:" + std::to_string(v1) + "\r\n");
    assert(f.RegisteredSnapshots() == 1U);

    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 5"));
    (void)VersionOf(f.Run(f.b, "DELETE BLOCK 4 0 FROM t"));
    (void)VersionOf(f.Run(f.b, "SET BLOCK 8 0 IN t n = 3"));
    assert(!Contains(f.Run(f.b, "SCAN CHUNKS FROM t"), Row(1, 0, v1)));

    ExpectReply(f.Run(f.a, "SCAN CHUNKS FROM t"), first);
    ExpectReply(f.Run(f.a, "SCAN CHUNKS FROM t WITH DATA"), data_before);
    ExpectReply(f.Run(f.a, "AGGREGATE AREA 0 0 TO 3 3 FROM t COUNT(*), SUM(n)"), sum_before);
    const auto page = f.Run(f.a, "SCAN CHUNKS FROM t LIMIT 1");
    ExpectContains(page, "*1\r\n" + Row(0, 0, v0) + "$4\r\nmore\r\n#t\r\n");
    ExpectContains(f.Run(f.a, "SCAN CHUNKS FROM t AFTER 0 0"), "*1\r\n" + Row(1, 0, v1) + "$4\r\nmore\r\n#f\r\n");
    ExpectContains(f.Run(f.a, "SCAN CHUNKS FROM t WHERE n = 2"), "*1\r\n" + Row(1, 0, v1));
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(1));
    ExpectReply(f.Run(f.a, "GET BLOCK 4 0 FROM t"), Value(2));
    ExpectReply(f.Run(f.a, "GET BLOCK 8 0 FROM t"), "_\r\n");
    ExpectReply(f.Run(f.a, "GET CHUNK 2 0 FROM t"), "_\r\n");
    assert(f.a.transaction->read_set.empty());

    ExpectReply(f.Run(f.a, "COMMIT"), "_\r\n");
    assert(f.a.transaction == nullptr);
    assert(f.RegisteredSnapshots() == 0U && f.KeptBytes() == 0U);
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(5));
}

// Writes and statements outside the read-only list are refused and leave
// the transaction open; an ordinary transaction still refuses SCAN.
void TestRefusedStatements() {
    Fixture f;
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 1"));
    const std::string form = f.Run(f.b, "GET CHUNK 0 0 FROM t");
    const auto body = form.find("\r\n") + 2U;
    const std::string bytes = form.substr(body, form.size() - body - 2U);

    ExpectError(f.Run(f.a, "BEGIN READ"), "SYNTAX");
    ExpectError(f.Run(f.a, "BEGIN READ WRITE"), "SYNTAX");
    ExpectError(f.Run(f.a, "BEGIN ONLY"), "SYNTAX");
    assert(f.a.transaction == nullptr);
    // Without a statement COMMIT has nothing to end but the transaction.
    ExpectReply(f.Run(f.a, "begin read only"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "COMMIT"), "_\r\n");

    ExpectReply(f.Run(f.a, "BEGIN READ ONLY"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(1));
    const std::string refused = "inside a READ ONLY transaction only GET statements";
    ExpectError(f.Run(f.a, "SET BLOCK 0 0 IN t n = 2"), refused);
    ExpectError(f.Run(f.a, "DELETE BLOCK 0 0 FROM t"), refused);
    ExpectError(f.Run(f.a, "SET CHUNK 1 1 IN t $1", Parameters{bytes}), refused);
    ExpectError(f.Run(f.a, "CREATE TABLE x (n u8) CHUNK 4 x 4"), refused);
    ExpectError(f.Run(f.a, "FLUSH WAL"), refused);
    ExpectError(f.Run(f.a, "SCAN CHUNKS FROM t SINCE 0"), "SINCE does not run inside a transaction");
    ExpectError(f.Run(f.a, "SCAN CHUNKS FROM other"), "a transaction covers one table");
    ExpectError(f.Run(f.a, "BEGIN READ ONLY"), "a transaction is already open");
    ExpectReply(f.Run(f.a, "PING"), "+PONG\r\n");
    assert(f.Run(f.a, "DESCRIBE t").starts_with("%"));
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(1));
    ExpectReply(f.Run(f.a, "ROLLBACK"), "+OK\r\n");
    assert(f.RegisteredSnapshots() == 0U);
    ExpectReply(f.Run(f.b, "GET BLOCK 0 0 FROM t"), Value(1));

    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectError(f.Run(f.a, "SCAN CHUNKS FROM t"), "inside a transaction only GET, SET and DELETE");
    ExpectReply(f.Run(f.a, "ROLLBACK"), "+OK\r\n");
}

// A read-only transaction keeps no read set, so --txn-max-read-chunks does
// not bound it.
void TestNoReadSetLimit() {
    Fixture f(chunkdb::QueryLimits{.txn_max_read_chunks = 1U});
    for (const auto* write : {"SET BLOCK 0 0 IN t n = 1", "SET BLOCK 4 0 IN t n = 2", "SET BLOCK 8 8 IN t n = 3"})
        (void)VersionOf(f.Run(f.b, write));
    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectError(f.Run(f.a, "AGGREGATE AREA 0 0 TO 3 3 FROM t SUM(n)"), "reads at most 1 chunks");
    ExpectReply(f.Run(f.a, "ROLLBACK"), "+OK\r\n");

    ExpectReply(f.Run(f.a, "BEGIN READ ONLY"), "+OK\r\n");
    ExpectContains(f.Run(f.a, "AGGREGATE AREA 0 0 TO 3 3 FROM t SUM(n)"), "$6\r\nsum(n)\r\n:6\r\n");
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(1));
    ExpectReply(f.Run(f.a, "GET BLOCK 4 0 FROM t"), Value(2));
    ExpectContains(f.Run(f.a, "GET AREA 0 0 TO 3 3 FROM t"), "*3\r\n");
    ExpectContains(f.Run(f.a, "SCAN CHUNKS FROM t"), "$6\r\nchunks\r\n*3\r\n");
    assert(f.a.transaction->read_set.empty());
    // Later changes to every chunk it read do not fail COMMIT.
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 9"));
    ExpectReply(f.Run(f.a, "COMMIT"), "_\r\n");
}

// Past --txn-max-duration-ms the next statement ends the transaction with
// CONFLICT reason=duration and frees the states writers kept for it.
// The transaction's start is moved back instead of waiting.
void TestDurationLimit() {
    Fixture f;
    const auto version = VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 1"));
    ExpectReply(f.Run(f.a, "BEGIN READ ONLY"), "+OK\r\n");
    ExpectContains(f.Run(f.a, "SCAN CHUNKS FROM t"), Row(0, 0, version));
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 2"));
    assert(f.KeptBytes() > 0U);

    f.a.transaction->started -= chunkdb::EngineConfig{}.txn_max_duration;
    ExpectError(f.Run(f.a, "SCAN CHUNKS FROM t"), "CONFLICT reason=duration");
    assert(f.RegisteredSnapshots() == 0U && f.KeptBytes() == 0U);
    ExpectError(f.Run(f.a, "PING"), "CONFLICT reason=duration");
    ExpectError(f.Run(f.a, "COMMIT"), "CONFLICT reason=duration");
    assert(f.a.transaction == nullptr);

    // The limit counts from BEGIN, also before the first statement.
    ExpectReply(f.Run(f.a, "BEGIN READ ONLY"), "+OK\r\n");
    f.a.transaction->started -= chunkdb::EngineConfig{}.txn_max_duration;
    ExpectError(f.Run(f.a, "AGGREGATE CHUNK 0 0 FROM t COUNT(*)"), "CONFLICT reason=duration");
    ExpectReply(f.Run(f.a, "ROLLBACK"), "+OK\r\n");
    assert(f.RegisteredSnapshots() == 0U);

    ExpectReply(f.Run(f.a, "BEGIN READ ONLY"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(2));
    ExpectReply(f.Run(f.a, "COMMIT"), "_\r\n");
}

// Past --txn-history-bytes writers end the oldest snapshot: the read-only
// transaction answers CONFLICT reason=history_limit and keeps nothing.
void TestHistoryLimit() {
    Fixture f({}, 1U);
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 1"));
    ExpectReply(f.Run(f.a, "BEGIN READ ONLY"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(1));
    // The write does not fail and does not wait for the reader.
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 2"));
    assert(f.RegisteredSnapshots() == 0U && f.KeptBytes() == 0U);
    ExpectError(f.Run(f.a, "SCAN CHUNKS FROM t"), "CONFLICT reason=history_limit");
    ExpectError(f.Run(f.a, "COMMIT"), "CONFLICT reason=history_limit");
    const auto metrics = f.Metrics();
    assert(Metric(metrics, "chunkdb_txn_history_copies_total") == 1U);
    assert(Metric(metrics, "chunkdb_txn_history_bytes") == 0U);
}

// Writers copy a chunk's state only while a snapshot is registered; the
// metrics count those copies and their bytes, and the gauge drops when the
// transaction ends.
void TestCopyCostMetrics() {
    Fixture f;
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 1"));
    (void)VersionOf(f.Run(f.b, "SET BLOCK 4 0 IN t n = 1"));
    auto metrics = f.Metrics();
    assert(Metric(metrics, "chunkdb_txn_history_copies_total") == 0U);
    assert(Metric(metrics, "chunkdb_txn_history_copied_bytes_total") == 0U);

    ExpectReply(f.Run(f.a, "BEGIN READ ONLY"), "+OK\r\n");
    // BEGIN alone registers nothing, so writes still copy nothing.
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 2"));
    assert(Metric(f.Metrics(), "chunkdb_txn_history_copies_total") == 0U);
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(2));
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 3"));
    (void)VersionOf(f.Run(f.b, "SET BLOCK 4 0 IN t n = 3"));
    // A second write to a chunk already kept for the snapshot copies again.
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 4"));
    const auto kept = f.KeptBytes();
    metrics = f.Metrics();
    assert(Metric(metrics, "chunkdb_txn_history_copies_total") == 3U);
    assert(Metric(metrics, "chunkdb_txn_history_bytes") == kept);
    assert(Metric(metrics, "chunkdb_txn_history_copied_bytes_total") > kept && kept > 0U);
    const auto copied = Metric(metrics, "chunkdb_txn_history_copied_bytes_total");

    ExpectReply(f.Run(f.a, "COMMIT"), "_\r\n");
    metrics = f.Metrics();
    assert(Metric(metrics, "chunkdb_txn_history_bytes") == 0U);
    assert(Metric(metrics, "chunkdb_txn_history_copies_total") == 3U);
    assert(Metric(metrics, "chunkdb_txn_history_copied_bytes_total") == copied);
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 5"));
    assert(Metric(f.Metrics(), "chunkdb_txn_history_copies_total") == 3U);
}

// A write of chunk (10, 10) held after it took its version and before it
// published the chunk, while an ordinary snapshot is open.
struct PausedWrite {
    Fixture& f;
    SessionState holder;
    std::string written;
    std::thread writer;

    explicit PausedWrite(Fixture& fixture) : f(fixture) {
        f.Hello(holder);
        ExpectReply(f.Run(holder, "BEGIN"), "+OK\r\n");
        ExpectReply(f.Run(holder, "GET BLOCK 0 0 FROM t"), Value(1));
        Store().ArmTxnPauseForTests(chunkdb::TxnPausePoint::kWriteAfterOpenCount);
        // A new value each time: a write that changes nothing keeps nothing.
        static int value = 0;
        writer = std::thread([this, n = ++value] { written = f.Run(f.b, "SET BLOCK 40 40 IN t n = " + std::to_string(n)); });
        assert(Store().WaitForTxnPauseForTests(chunkdb::TxnPausePoint::kWriteAfterOpenCount));
    }
    chunkdb::ChunkStore& Store() { return f.catalog->Find("t")->Acquire()->store(); }
    // Waits until `count` snapshots are registered.
    void AwaitSnapshots(std::size_t count) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (f.RegisteredSnapshots() != count) {
            assert(std::chrono::steady_clock::now() < deadline);
            std::this_thread::yield();
        }
    }
    std::uint64_t Finish() {
        Store().ResumeTxnForTests(chunkdb::TxnPausePoint::kWriteAfterOpenCount);
        writer.join();
        (void)f.Run(holder, "ROLLBACK");
        return VersionOf(written);
    }
};

// Point and area reads do not wait for a write the snapshot includes; SCAN
// CHUNKS waits for it, so it lists the chunk.
void TestScanWaitsForIncludedWrites() {
    Fixture f;
    const auto first = VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 1"));
    PausedWrite paused(f);
    SessionState reader;
    f.Hello(reader);
    ExpectReply(f.Run(reader, "BEGIN READ ONLY"), "+OK\r\n");
    ExpectReply(f.Run(reader, "GET BLOCK 0 0 FROM t"), Value(1));
    ExpectContains(f.Run(reader, "AGGREGATE AREA 0 0 TO 1 1 FROM t SUM(n)"), "$6\r\nsum(n)\r\n:1\r\n");
    std::string scanned;
    std::thread scanner([&] { scanned = f.Run(reader, "SCAN CHUNKS FROM t"); });
    // A scan cannot answer before the write completes.
    paused.AwaitSnapshots(2U);
    assert(scanned.empty());
    const auto version = paused.Finish();
    scanner.join();
    ExpectContains(scanned, "*2\r\n" + Row(0, 0, first) + Row(10, 10, version));
    ExpectContains(scanned, "$8\r\nposition\r\n:" + std::to_string(version) + "\r\n");
    ExpectReply(f.Run(reader, "COMMIT"), "_\r\n");
}

// The transaction's duration and a stopped statement end the scan's wait.
void TestScanWaitEnds() {
    Fixture f({}, chunkdb::kDefaultTxnHistoryBytes, std::chrono::milliseconds(300));
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 1"));
    {
        PausedWrite paused(f);
        SessionState reader;
        f.Hello(reader);
        ExpectReply(f.Run(reader, "BEGIN READ ONLY"), "+OK\r\n");
        ExpectError(f.Run(reader, "SCAN CHUNKS FROM t"), "CONFLICT reason=duration");
        ExpectReply(f.Run(reader, "ROLLBACK"), "+OK\r\n");
        (void)paused.Finish();
    }
    PausedWrite paused(f);
    auto& store = paused.Store();
    const auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(60));
    chunkdb::StatementControl control(std::chrono::steady_clock::now(), 0U);
    assert(control.Start());
    bool stopped = false;
    std::thread scanner([&] {
        chunkdb::ScopedStatementControl scope(&control);
        try {
            (void)store.ScanChunkIdentitiesAt(*snapshot, false, {}, 10U);
        } catch (const chunkdb::StatementStoppedError& error) {
            stopped = error.reason() == chunkdb::StatementStopReason::kCancelled;
        }
    });
    assert(control.RequestStop(chunkdb::StatementStopReason::kCancelled));
    scanner.join();
    assert(stopped);
    (void)paused.Finish();
    // Once the write completed, the same snapshot scans without waiting.
    assert(store.ScanChunkIdentitiesAt(*snapshot, false, {}, 10U).entries.size() == 2U);
}

// Ordinary transactions move units between chunks while plain writes add
// and empty another chunk; every read-only transaction sees the same total
// in AGGREGATE, SCAN WITH DATA pages and point reads.
void TestReadersBesideWriters() {
    Fixture f;
    constexpr int kChunks = 4;
    constexpr int kUnits = 1000;
    constexpr int kTransfers = 150;
    constexpr int kReads = 60;
    for (int c = 0; c < kChunks; ++c)
        (void)VersionOf(f.Run(f.b, "SET BLOCK " + std::to_string(c * 4) + " 0 IN t n = " + std::to_string(kUnits)));
    std::atomic<int> running_writers{2};
    std::vector<std::thread> threads;
    for (int w = 0; w < 2; ++w) {
        threads.emplace_back([&, w] {
            SessionState session;
            f.Hello(session);
            std::mt19937 random(static_cast<unsigned>(w + 1));
            for (int i = 0; i < kTransfers;) {
                const int from = static_cast<int>(random() % kChunks);
                const int to = (from + 1 + static_cast<int>(random() % (kChunks - 1))) % kChunks;
                const auto block = [](int c) { return std::to_string(c * 4) + " 0"; };
                ExpectReply(f.Run(session, "BEGIN"), "+OK\r\n");
                const auto source = f.Run(session, "GET BLOCK " + block(from) + " FROM t");
                const auto target = f.Run(session, "GET BLOCK " + block(to) + " FROM t");
                if (source.starts_with("-") || target.starts_with("-")) {
                    ExpectReply(f.Run(session, "ROLLBACK"), "+OK\r\n");
                    continue;
                }
                const auto value = [](const std::string& reply) { return std::stoll(reply.substr(5)); };
                (void)f.Run(session, "SET BLOCK " + block(from) + " IN t n = " + std::to_string(value(source) - 1));
                (void)f.Run(session, "SET BLOCK " + block(to) + " IN t n = " + std::to_string(value(target) + 1));
                const auto committed = f.Run(session, "COMMIT");
                if (committed.starts_with(":")) ++i;
                else ExpectError(committed, "CONFLICT");
            }
            running_writers.fetch_sub(1);
        });
    }
    threads.emplace_back([&] {
        SessionState session;
        f.Hello(session);
        for (int i = 0; running_writers.load() > 0; ++i) {
            (void)VersionOf(f.Run(session, i % 2 == 0 ? "SET BLOCK 40 0 IN t n = 1" : "DELETE BLOCK 40 0 FROM t"));
        }
    });
    const std::string total = "$6\r\nsum(n)\r\n:" + std::to_string(kChunks * kUnits) + "\r\n";
    for (int r = 0; r < 2; ++r) {
        threads.emplace_back([&] {
            SessionState session;
            f.Hello(session);
            for (int i = 0; i < kReads; ++i) {
                ExpectReply(f.Run(session, "BEGIN READ ONLY"), "+OK\r\n");
                const auto scan = f.Run(session, "SCAN CHUNKS FROM t WITH DATA");
                assert(scan.starts_with("%5\r\n"));
                ExpectContains(f.Run(session, "AGGREGATE AREA 0 0 TO 3 0 FROM t SUM(n)"), total);
                std::string pages;
                std::string after;
                for (;;) {
                    const auto page = f.Run(session, "SCAN CHUNKS FROM t" + after + " LIMIT 1 WITH DATA");
                    const auto rows = page.find("$6\r\nchunks\r\n*1\r\n");
                    if (rows == std::string::npos) break;
                    pages += page.substr(rows + 16U, page.find("$4\r\nmore\r\n") - rows - 16U);
                    if (Contains(page, "$4\r\nmore\r\n#f\r\n")) break;
                    const auto row = page.substr(rows + 16U);
                    const auto x = std::stoll(row.substr(5U));
                    const auto y_at = row.find(':', 5U);
                    after = " AFTER " + std::to_string(x) + " " + std::to_string(std::stoll(row.substr(y_at + 1U)));
                }
                // The pages read separately join into the whole scan.
                ExpectContains(scan, "$6\r\nchunks\r\n");
                assert(scan.find(pages) != std::string::npos && !pages.empty());
                ExpectReply(f.Run(session, "COMMIT"), "_\r\n");
            }
        });
    }
    for (auto& thread : threads) thread.join();
    assert(f.RegisteredSnapshots() == 0U && f.KeptBytes() == 0U);
    ExpectContains(f.Run(f.a, "AGGREGATE AREA 0 0 TO 3 0 FROM t SUM(n)"), total);
}

}  // namespace

int main() {
    TestSnapshotScanAndAggregate();
    TestRefusedStatements();
    TestNoReadSetLimit();
    TestDurationLimit();
    TestHistoryLimit();
    TestCopyCostMetrics();
    TestScanWaitsForIncludedWrites();
    TestScanWaitEnds();
    TestReadersBesideWriters();
    std::puts("txn read-only tests passed");
    return 0;
}
