// Transactions through the engine (docs/TRANSACTIONS_DESIGN.md): BEGIN,
// reads and writes on private copies, COMMIT and ROLLBACK, the statements a
// transaction refuses, conflicts and limits.

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "txn_test_utils.hpp"

namespace {

using chunkdb::CommandEngine;
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
    if (reply.rfind("-ERR ", 0) != 0 || !Contains(reply, part)) {
        std::fprintf(stderr, "expected an error with '%s', got '%s'\n", part.c_str(), reply.c_str());
        assert(false);
    }
}

// ":<n>\r\n" -> n.
std::uint64_t VersionOf(const std::string& reply) {
    if (reply.size() <= 3 || reply[0] != ':') {
        std::fprintf(stderr, "expected a version, got '%s'\n", reply.c_str());
        assert(false);
    }
    return std::stoull(reply.substr(1));
}

// The value of GET BLOCK n FROM t for a one-column integer table.
std::string Value(std::int64_t n) {
    return "*1\r\n:" + std::to_string(n) + "\r\n";
}

// The version in a GET CHUNK reply: the form's first eight bytes.
std::uint64_t ChunkVersionOf(const std::string& reply) {
    const auto body = reply.find("\r\n");
    assert(reply[0] == '$' && body != std::string::npos);
    std::uint64_t version = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        version |= static_cast<std::uint64_t>(static_cast<unsigned char>(reply[body + 2 + i])) << (8U * i);
    }
    return version;
}

struct Fixture {
    chunkdb::test::ScopedTempDir dir{"chunkdb-txn-engine"};
    std::shared_ptr<chunkdb::TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    chunkdb::SessionState a;
    chunkdb::SessionState b;

    explicit Fixture(chunkdb::EngineConfig config = {}) {
        chunkdb::CatalogConfig catalog_config;
        catalog_config.data_dir = dir.path();
        catalog = std::make_shared<chunkdb::TableCatalog>(catalog_config);
        config.require_auth = false;
        config.server_version = "test";
        engine = std::make_unique<CommandEngine>(config, catalog);
        for (auto* session : {&a, &b}) {
            assert(engine->Execute(*session, "HELLO 3\r\n").rfind("%8\r\n", 0) == 0);
        }
        ExpectReply(Run(a, "CREATE TABLE t (n u16 REQUIRED) CHUNK 4 x 4"), "+OK\r\n");
        ExpectReply(Run(a, "CREATE TABLE other (n u16 REQUIRED) CHUNK 4 x 4"), "+OK\r\n");
    }

    std::string Run(chunkdb::SessionState& session, const std::string& line, const Parameters& parameters = {}) {
        return engine->Execute(session, line + "\r\n", parameters);
    }

    std::size_t RegisteredSnapshots() {
        const auto lease = catalog->Find("t")->Acquire();
        return lease->store().TxnRegisteredCountForTests();
    }
};

// Writes in a transaction are seen by the transaction only, until COMMIT
// applies them together with one version.
void TestCommitAppliesTogether() {
    Fixture f;
    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "SET BLOCK 0 0 IN t n = 1"), "_\r\n");
    ExpectReply(f.Run(f.a, "SET BLOCK 9 9 IN t n = 2"), "_\r\n");
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(1));
    ExpectReply(f.Run(f.b, "GET BLOCK 0 0 FROM t"), "_\r\n");
    const std::uint64_t version = VersionOf(f.Run(f.a, "COMMIT"));
    ExpectReply(f.Run(f.b, "GET BLOCK 0 0 FROM t"), Value(1));
    ExpectReply(f.Run(f.b, "GET BLOCK 9 9 FROM t"), Value(2));
    assert(ChunkVersionOf(f.Run(f.b, "GET CHUNK 0 0 FROM t")) == version);
    assert(ChunkVersionOf(f.Run(f.b, "GET CHUNK 2 2 FROM t")) == version);
    // The session is out of the transaction: a write answers its version.
    (void)VersionOf(f.Run(f.a, "SET BLOCK 0 0 IN t n = 3"));
    assert(f.RegisteredSnapshots() == 0);
}

// Reads see the table as of the first statement, also chunks first read
// after another session changed them.
void TestSnapshotReads() {
    Fixture f;
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 1"));
    (void)VersionOf(f.Run(f.b, "SET BLOCK 9 9 IN t n = 1"));
    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(1));
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 2"));
    (void)VersionOf(f.Run(f.b, "SET BLOCK 9 9 IN t n = 2"));
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(1));
    ExpectReply(f.Run(f.a, "GET BLOCK 9 9 FROM t"), Value(1));
    // GET AREA: the snapshot, with the transaction's own write on top.
    ExpectReply(f.Run(f.a, "SET BLOCK 5 5 IN t n = 7"), "_\r\n");
    const std::string area = f.Run(f.a, "GET AREA 0 0 TO 2 2 FROM t");
    assert(area.rfind("*3\r\n", 0) == 0);
    // It read chunks another session changed since, so its write cannot
    // commit.
    ExpectError(f.Run(f.a, "COMMIT"), "CONFLICT chunk_changed");
    ExpectReply(f.Run(f.b, "GET BLOCK 5 5 FROM t"), "_\r\n");

    // A transaction that only read commits with nothing to answer.
    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(2));
    ExpectReply(f.Run(f.a, "COMMIT"), "_\r\n");
}

// A chunk read or written that another session changed after the snapshot
// makes COMMIT answer CONFLICT and change nothing.
void TestConflict() {
    Fixture f;
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 1"));
    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(1));
    ExpectReply(f.Run(f.a, "SET BLOCK 9 9 IN t n = 5"), "_\r\n");
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 2"));
    ExpectError(f.Run(f.a, "COMMIT"), "CONFLICT chunk_changed");
    ExpectReply(f.Run(f.b, "GET BLOCK 9 9 FROM t"), "_\r\n");
    // COMMIT ended the transaction.
    ExpectReply(f.Run(f.a, "ROLLBACK"), "+OK\r\n");
    (void)VersionOf(f.Run(f.a, "SET BLOCK 9 9 IN t n = 5"));

    // Write against write: the first to commit wins.
    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectReply(f.Run(f.b, "BEGIN"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "SET BLOCK 1 1 IN t n = 10"), "_\r\n");
    ExpectReply(f.Run(f.b, "SET BLOCK 1 1 IN t n = 20"), "_\r\n");
    (void)VersionOf(f.Run(f.a, "COMMIT"));
    ExpectError(f.Run(f.b, "COMMIT"), "CONFLICT chunk_changed");
    ExpectReply(f.Run(f.b, "GET BLOCK 1 1 FROM t"), Value(10));
    assert(f.RegisteredSnapshots() == 0);
}

void TestRollbackAndStatements() {
    Fixture f;
    ExpectReply(f.Run(f.a, "ROLLBACK"), "+OK\r\n");
    ExpectError(f.Run(f.a, "COMMIT"), "INVALID_ARGUMENT no transaction is open");
    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectError(f.Run(f.a, "BEGIN"), "INVALID_ARGUMENT a transaction is already open");
    ExpectReply(f.Run(f.a, "SET BLOCK 0 0 IN t n = 1"), "_\r\n");
    ExpectReply(f.Run(f.a, "DELETE BLOCK 0 0 FROM t"), "_\r\n");
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), "_\r\n");
    ExpectReply(f.Run(f.a, "SET BLOCK 0 0 IN t n = 4"), "_\r\n");

    // Refused statements leave the transaction open.
    ExpectError(f.Run(f.a, "CREATE TABLE x (n u8) CHUNK 4 x 4"), "INVALID_ARGUMENT inside a transaction");
    ExpectError(f.Run(f.a, "SCAN CHUNKS FROM t"), "INVALID_ARGUMENT inside a transaction");
    ExpectError(f.Run(f.a, "FLUSH WAL"), "INVALID_ARGUMENT inside a transaction");
    ExpectError(f.Run(f.a, "GET BLOCK 0 0 FROM other"), "INVALID_ARGUMENT a transaction covers one table");
    ExpectError(f.Run(f.a, "SET BLOCK 0 0 IN t n = 1 IF VERSION 3"), "INVALID_ARGUMENT IF VERSION is not used");
    ExpectError(f.Run(f.a, "SET BLOCK 1 1 IN t n = 70000"), "INVALID_ARGUMENT");
    ExpectReply(f.Run(f.a, "PING"), "+PONG\r\n");
    assert(f.Run(f.a, "DESCRIBE t").rfind("%6\r\n", 0) == 0);
    assert(f.Run(f.a, "DESCRIBE other").rfind("%6\r\n", 0) == 0);
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(4));

    ExpectReply(f.Run(f.a, "ROLLBACK"), "+OK\r\n");
    ExpectReply(f.Run(f.b, "GET BLOCK 0 0 FROM t"), "_\r\n");
    assert(f.RegisteredSnapshots() == 0);

    // A closed connection rolls its transaction back.
    {
        chunkdb::SessionState c;
        assert(f.engine->Execute(c, "HELLO 3\r\n").rfind("%8\r\n", 0) == 0);
        ExpectReply(f.Run(c, "BEGIN"), "+OK\r\n");
        ExpectReply(f.Run(c, "SET BLOCK 0 0 IN t n = 9"), "_\r\n");
        assert(f.RegisteredSnapshots() == 1);
    }
    assert(f.RegisteredSnapshots() == 0);
    ExpectReply(f.Run(f.b, "GET BLOCK 0 0 FROM t"), "_\r\n");
}

// SET CHUNK in a transaction replaces its copy; GET CHUNK reads it.
void TestChunkStatements() {
    Fixture f;
    (void)VersionOf(f.Run(f.b, "SET BLOCK 0 0 IN t n = 1"));
    const std::string form = f.Run(f.b, "GET CHUNK 0 0 FROM t");
    const auto body = form.find("\r\n") + 2;
    const std::string bytes = form.substr(body, form.size() - body - 2);
    (void)VersionOf(f.Run(f.b, "SET BLOCK 1 0 IN t n = 2"));

    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "SET CHUNK 0 0 IN t $1", Parameters{bytes}), "_\r\n");
    ExpectReply(f.Run(f.a, "GET BLOCK 1 0 FROM t"), "_\r\n");
    ExpectReply(f.Run(f.a, "GET BLOCK 0 0 FROM t"), Value(1));
    ExpectError(f.Run(f.a, "SET CHUNK 0 0 IN t $1", Parameters{std::string("short")}), "");
    (void)VersionOf(f.Run(f.a, "COMMIT"));
    ExpectReply(f.Run(f.b, "GET BLOCK 1 0 FROM t"), "_\r\n");
}

void TestDurationLimit() {
    Fixture f(chunkdb::EngineConfig{.txn_max_duration = std::chrono::milliseconds(1000)});
    // The limit counts from BEGIN.
    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    ExpectError(f.Run(f.a, "GET BLOCK 0 0 FROM t"), "CONFLICT duration");
    // Ended: every statement answers the conflict, and none runs outside
    // the transaction, until ROLLBACK or COMMIT closes it.
    ExpectError(f.Run(f.a, "SET BLOCK 0 0 IN t n = 9"), "CONFLICT duration");
    ExpectError(f.Run(f.a, "PING"), "CONFLICT duration");
    ExpectError(f.Run(f.a, "BEGIN"), "CONFLICT duration");
    ExpectReply(f.Run(f.b, "GET BLOCK 0 0 FROM t"), "_\r\n");
    assert(f.RegisteredSnapshots() == 0);
    ExpectReply(f.Run(f.a, "ROLLBACK"), "+OK\r\n");
    (void)VersionOf(f.Run(f.a, "SET BLOCK 0 0 IN t n = 2"));

    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "SET BLOCK 0 0 IN t n = 3"), "_\r\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    ExpectError(f.Run(f.a, "COMMIT"), "CONFLICT duration");
    ExpectReply(f.Run(f.b, "GET BLOCK 0 0 FROM t"), Value(2));
    assert(f.RegisteredSnapshots() == 0);
}

void TestByteAndChunkLimits() {
    // A 4x4 chunk of u16 is 32 bytes of values, 2 of validity and 2 of
    // presence: two copies fit in 100 bytes, three do not.
    Fixture f(chunkdb::EngineConfig{.txn_max_bytes = 100});
    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "SET BLOCK 0 0 IN t n = 1"), "_\r\n");
    ExpectReply(f.Run(f.a, "SET BLOCK 4 0 IN t n = 1"), "_\r\n");
    ExpectError(f.Run(f.a, "SET BLOCK 8 0 IN t n = 1"), "INVALID_ARGUMENT the transaction's changes would take");
    // The transaction stays open, without the refused write.
    ExpectReply(f.Run(f.a, "GET BLOCK 8 0 FROM t"), "_\r\n");
    (void)VersionOf(f.Run(f.a, "COMMIT"));
    ExpectReply(f.Run(f.b, "GET BLOCK 4 0 FROM t"), Value(1));
    ExpectReply(f.Run(f.b, "GET BLOCK 8 0 FROM t"), "_\r\n");

    Fixture g;
    ExpectReply(g.Run(g.a, "BEGIN"), "+OK\r\n");
    for (std::size_t i = 0; i < chunkdb::kMaxTxnWrittenChunks; ++i) {
        ExpectReply(g.Run(g.a, "SET BLOCK " + std::to_string(i * 4) + " 0 IN t n = 1"), "_\r\n");
    }
    ExpectError(
        g.Run(g.a, "SET BLOCK " + std::to_string(chunkdb::kMaxTxnWrittenChunks * 4) + " 0 IN t n = 1"),
        "INVALID_ARGUMENT a transaction writes at most");
    ExpectReply(g.Run(g.a, "SET BLOCK 1 1 IN t n = 2"), "_\r\n");
    if constexpr (!chunkdb::txn_test::kThreadSanitizer) {
        const auto version = VersionOf(g.Run(g.a, "COMMIT"));
        for (std::size_t i = 0; i < chunkdb::kMaxTxnWrittenChunks; ++i) {
            ExpectReply(g.Run(g.b, "GET BLOCK " + std::to_string(i * 4) + " 0 FROM t"), Value(1));
            assert(ChunkVersionOf(g.Run(g.b, "GET CHUNK " + std::to_string(i) + " 0 FROM t")) == version);
        }
        ExpectReply(g.Run(g.b, "GET BLOCK 1 1 FROM t"), Value(2));
        ExpectReply(
            g.Run(g.b, "GET BLOCK " + std::to_string(chunkdb::kMaxTxnWrittenChunks * 4) + " 0 FROM t"), "_\r\n");
        assert(g.RegisteredSnapshots() == 0);
        std::printf("txn engine full-size commit passed (%zu written chunks)\n", chunkdb::kMaxTxnWrittenChunks);
        return;
    }
    // Check the saturated write set through its private copies, then roll
    // it back under TSan; keep commits within its detector's lock capacity.
    for (std::size_t i = 0; i < chunkdb::kMaxTxnWrittenChunks; ++i) {
        ExpectReply(g.Run(g.a, "GET BLOCK " + std::to_string(i * 4) + " 0 FROM t"), Value(1));
        ExpectReply(g.Run(g.b, "GET BLOCK " + std::to_string(i * 4) + " 0 FROM t"), "_\r\n");
    }
    ExpectReply(g.Run(g.a, "GET BLOCK 1 1 FROM t"), Value(2));
    ExpectReply(
        g.Run(g.a, "GET BLOCK " + std::to_string(chunkdb::kMaxTxnWrittenChunks * 4) + " 0 FROM t"), "_\r\n");
    ExpectReply(g.Run(g.a, "ROLLBACK"), "+OK\r\n");
    assert(g.RegisteredSnapshots() == 0);
    ExpectReply(g.Run(g.b, "GET BLOCK 1 1 FROM t"), "_\r\n");
    // The refused statement and rollback did not retain the write budget.
    ExpectReply(g.Run(g.a, "BEGIN"), "+OK\r\n");
    ExpectReply(g.Run(g.a, "SET BLOCK 1 1 IN t n = 2"), "_\r\n");
    (void)VersionOf(g.Run(g.a, "COMMIT"));
    ExpectReply(g.Run(g.b, "GET BLOCK 1 1 FROM t"), Value(2));
    std::puts("txn engine TSan limit cases passed");
}

// ALTER TABLE or DROP TABLE during a transaction ends it with CONFLICT.
void TestTableChanged() {
    Fixture f;
    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "SET BLOCK 0 0 IN t n = 1"), "_\r\n");
    ExpectReply(f.Run(f.b, "ALTER TABLE t ADD COLUMN m u8 NULL"), "+OK\r\n");
    ExpectError(f.Run(f.a, "GET BLOCK 0 0 FROM t"), "CONFLICT table_changed");
    ExpectReply(f.Run(f.b, "GET BLOCK 0 0 FROM t"), "_\r\n");
    // COMMIT of an ended transaction answers the conflict and closes it.
    ExpectError(f.Run(f.a, "COMMIT"), "CONFLICT table_changed");

    ExpectReply(f.Run(f.a, "BEGIN"), "+OK\r\n");
    ExpectReply(f.Run(f.a, "SET BLOCK 0 0 IN t n = 1"), "_\r\n");
    ExpectReply(f.Run(f.b, "DROP TABLE t"), "+OK\r\n");
    ExpectError(f.Run(f.a, "COMMIT"), "CONFLICT table_changed");
}

}  // namespace

int main() {
    TestCommitAppliesTogether();
    TestSnapshotReads();
    TestConflict();
    TestRollbackAndStatements();
    TestChunkStatements();
    TestDurationLimit();
    TestByteAndChunkLimits();
    TestTableChanged();
    std::puts("txn engine tests passed");
    return 0;
}
