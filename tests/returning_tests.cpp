// RETURNING [OLD | NEW] of SET BLOCK and DELETE BLOCK (#145,
// docs/DESIGN-2.0.md E4): rows made under the write's lock or from the
// transaction's view, refused writes, rights, the max_returning_rows and
// response bounds, and replies encoded row by row through a continuation.

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/reply.hpp"
#include "chunkdb/table_catalog.hpp"
#include "login_helpers.hpp"
#include "returning.hpp"
#include "test_utils.hpp"

namespace {

using chunkdb::CommandEngine;
using chunkdb::ReplyContinuation;
using chunkdb::ReturningLimitError;
using chunkdb::ReturningRows;
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

// `part` is a code, optionally followed by text the reply contains.
void ExpectError(const std::string& reply, const std::string& part) {
    const auto space = part.find(' ');
    const auto code = part.substr(0, space);
    if (!reply.starts_with("-ERR " + code + " ") ||
        (space != std::string::npos && !Contains(reply, part.substr(space + 1U)))) {
        std::fprintf(stderr, "expected an error with '%s', got '%s'\n", part.c_str(), reply.c_str());
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

std::string Le(std::uint64_t value) {
    std::string out;
    for (std::size_t i = 0; i < 8U; ++i) out.push_back(static_cast<char>((value >> (8U * i)) & 0xffU));
    return out;
}

// {applied, version, rows} with `rows` the encoded rows array.
std::string Answer(bool applied, std::optional<std::uint64_t> version, const std::string& rows) {
    return std::string("%3\r\n$7\r\napplied\r\n") + (applied ? "#t\r\n" : "#f\r\n") + "$7\r\nversion\r\n" +
           (version ? ":" + std::to_string(*version) + "\r\n" : "_\r\n") + "$4\r\nrows\r\n" + rows;
}

// The version an {applied, version, rows} reply holds.
std::uint64_t AnswerVersion(const std::string& reply) {
    const std::string key = "$7\r\nversion\r\n:";
    const auto at = reply.find(key);
    if (!reply.starts_with("%3\r\n") || at == std::string::npos) {
        std::fprintf(stderr, "expected a RETURNING answer, got '%s'\n", reply.c_str());
        assert(false);
    }
    return std::stoull(reply.substr(at + key.size()));
}

// One row: coordinates, then `values` as GET BLOCK encodes them (its reply
// without the array header).
std::string Row(std::size_t columns, const std::vector<std::int64_t>& coordinates, const std::string& values) {
    std::string row = "*" + std::to_string(coordinates.size() + columns) + "\r\n";
    for (const auto c : coordinates) row += ":" + std::to_string(c) + "\r\n";
    return row + values;
}

// GET BLOCK's values without its array header.
std::string Values(const std::string& get) {
    assert(get.starts_with('*'));
    return get.substr(get.find("\r\n") + 2U);
}

struct Fixture {
    chunkdb::test::ScopedTempDir dir{"chunkdb-returning"};
    std::shared_ptr<chunkdb::TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    SessionState session;

    explicit Fixture(chunkdb::QueryLimits limits = {}) {
        chunkdb::CatalogConfig config;
        config.data_dir = dir.path();
        config.query_limits = limits;
        catalog = std::make_shared<chunkdb::TableCatalog>(config);
        engine = std::make_unique<CommandEngine>(chunkdb::EngineConfig{.require_auth = false, .server_version = "test"}, catalog);
        Hello(session);
        ExpectReply(Run("CREATE TABLE bank (coins u16, bonus i8 NULL, rate f32 NULL, name text(8) NULL, a u8, b u8) "
                        "CHUNK 4 x 4 CHUNK COLUMNS (status u8 DEFAULT 0)"),
                    "+OK\r\n");
    }

    void Hello(SessionState& state) const {
        assert(engine->Execute(state, "HELLO 3\r\n").rfind("%", 0) == 0);
    }

    std::string Run(const std::string& line, const Parameters& parameters = {}) {
        return engine->Execute(session, line + "\r\n", parameters);
    }
    std::string Run(SessionState& state, const std::string& line, const Parameters& parameters = {}) const {
        return engine->Execute(state, line + "\r\n", parameters);
    }
    // The committed version of the chunk of block (x, y).
    std::uint64_t ChunkVersion(std::int64_t x, std::int64_t y) {
        const auto reply = Run("GET BLOCK " + std::to_string(x) + " " + std::to_string(y) + " FROM bank WITH VERSION");
        assert(reply.starts_with("*2\r\n:"));
        return std::stoull(reply.substr(5));
    }
};

// SET BLOCK: NEW is the block as a later read decodes it, OLD the block
// before, and an absent block has no OLD row.
void TestSet() {
    Fixture f;
    // A write without RETURNING answers its version, as before.
    (void)VersionOf(f.Run("SET BLOCK 3 3 IN bank coins = 1"));

    auto reply = f.Run("SET BLOCK 1 2 IN bank coins = 10, a = 1 RETURNING *");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(6, {1, 2}, ":10\r\n_\r\n_\r\n_\r\n:1\r\n:0\r\n")));
    ExpectReply(f.Run("GET BLOCK 1 2 FROM bank"), "*6\r\n:10\r\n_\r\n_\r\n_\r\n:1\r\n:0\r\n");
    reply = f.Run("SET BLOCK 1 3 IN bank coins = 1 RETURNING OLD *");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*0\r\n"));

    reply = f.Run("SET BLOCK 1 2 IN bank coins = coins + 5, rate = 0.1 RETURNING OLD coins, rate");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(2, {1, 2}, ":10\r\n_\r\n")));
    // NEW in the order named; an f32 value as stored.
    reply = f.Run("SET BLOCK 1 2 IN bank name = $1 RETURNING NEW rate, name, coins", Parameters{std::string("ab")});
    ExpectReply(reply, Answer(true, AnswerVersion(reply),
        "*1\r\n" + Row(3, {1, 2}, Values(f.Run("GET BLOCK 1 2 FROM bank COLUMNS rate, name, coins")))));
    assert(Contains(reply, "$2\r\nab\r\n:15\r\n"));
    reply = f.Run("SET BLOCK 1 2 IN bank a = b, b = a, bonus = -3 RETURNING NEW *");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(6, {1, 2}, Values(f.Run("GET BLOCK 1 2 FROM bank")))));
    assert(Contains(reply, ":-3\r\n"));
    // The answer's version is the chunk's version after the write.
    assert(AnswerVersion(reply) == f.ChunkVersion(1, 2));

    // A refused statement changes nothing.
    const auto before = f.Run("GET BLOCK 1 2 FROM bank");
    ExpectError(f.Run("SET BLOCK 1 2 IN bank coins = 1 RETURNING nope"), "INVALID_ARGUMENT the table has no column nope");
    ExpectError(f.Run("SET BLOCK 1 2 IN bank coins = NULL RETURNING *"), "INVALID_ARGUMENT");
    ExpectError(f.Run("SET BLOCK 1 2 IN bank coins = coins - 100 RETURNING OLD *"), "OUT_OF_RANGE");
    ExpectReply(f.Run("GET BLOCK 1 2 FROM bank"), before);
}

// A new block's NEW row holds the values it takes: defaults, NULL, zero or
// empty.
void TestNewBlockDefaults() {
    Fixture f;
    ExpectReply(f.Run("CREATE TABLE d (n u8 DEFAULT 7, t text(4) DEFAULT 'x', e text(4) NULL DEFAULT '', u text(4), "
                      "m i8 NULL, k bytes(2) NULL DEFAULT x'0102', z i16, f f64 NULL DEFAULT 1.5) CHUNK 2 x 2"),
        "+OK\r\n");
    auto reply = f.Run("SET BLOCK 0 1 IN d m = 1 RETURNING *");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(8, {0, 1}, Values(f.Run("GET BLOCK 0 1 FROM d")))));
    ExpectReply(reply, Answer(true, AnswerVersion(reply),
        "*1\r\n*10\r\n:0\r\n:1\r\n:7\r\n$1\r\nx\r\n$0\r\n\r\n$0\r\n\r\n:1\r\n$2\r\n\x01\x02\r\n:0\r\n,1.5\r\n"));
}

// IF <expr> and IF VERSION: a refused write answers applied false with the
// chunk's version and no rows, and changes nothing.
void TestConditional() {
    Fixture f;
    (void)VersionOf(f.Run("SET BLOCK 1 2 IN bank coins = 10"));
    const auto version = f.ChunkVersion(1, 2);
    ExpectReply(f.Run("SET BLOCK 1 2 IN bank coins = 0 IF coins > 100 RETURNING *"), Answer(false, version, "*0\r\n"));
    ExpectReply(f.Run("SET BLOCK 1 2 IN bank coins = 0 IF VERSION 999 RETURNING OLD *"), Answer(false, version, "*0\r\n"));
    ExpectReply(f.Run("SET BLOCK 1 2 IN bank coins = 0 IF VERSION $1 RETURNING OLD *", Parameters{Le(998)}),
        Answer(false, version, "*0\r\n"));
    ExpectReply(f.Run("DELETE BLOCK 1 2 FROM bank IF coins > 100 RETURNING *"), Answer(false, version, "*0\r\n"));
    assert(f.ChunkVersion(1, 2) == version);

    auto reply = f.Run("SET BLOCK 1 2 IN bank coins = coins - 1 IF coins > 0 RETURNING OLD coins");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(1, {1, 2}, ":10\r\n")));
    reply = f.Run("SET BLOCK 1 2 IN bank coins = 1 IF VERSION " + std::to_string(AnswerVersion(reply)) + " RETURNING NEW coins");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(1, {1, 2}, ":1\r\n")));
    reply = f.Run("SET BLOCK 2 2 IN bank coins = 4 IF NOT EXISTS RETURNING OLD *");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*0\r\n"));
}

// DELETE BLOCK returns the block it removed; an absent block has no row.
void TestDelete() {
    Fixture f;
    (void)VersionOf(f.Run("SET BLOCK 1 2 IN bank coins = 10, name = 'gold'"));
    auto reply = f.Run("DELETE BLOCK 1 2 FROM bank RETURNING name, coins");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(2, {1, 2}, "$4\r\ngold\r\n:10\r\n")));
    ExpectReply(f.Run("GET BLOCK 1 2 FROM bank"), "_\r\n");
    reply = f.Run("DELETE BLOCK 1 2 FROM bank RETURNING OLD *");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*0\r\n"));
    ExpectError(f.Run("DELETE BLOCK 1 2 FROM bank RETURNING NEW *"), "SYNTAX");
}

// A column named twice is returned twice, NEW and OLD alike.
void TestRepeatedColumn() {
    Fixture f;
    auto reply = f.Run("SET BLOCK 0 0 IN bank coins = 4, name = 'ab' RETURNING NEW name, coins, name");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(3, {0, 0}, "$2\r\nab\r\n:4\r\n$2\r\nab\r\n")));
    reply = f.Run("DELETE BLOCK 0 0 FROM bank RETURNING coins, coins");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(2, {0, 0}, ":4\r\n:4\r\n")));
}

// A refused write on a never-written chunk has no version, as GET BLOCK
// WITH VERSION shows, in and out of a transaction.
void TestNeverWrittenChunk() {
    Fixture f;
    ExpectReply(f.Run("GET BLOCK 40 40 FROM bank WITH VERSION"), "*2\r\n_\r\n_\r\n");
    ExpectReply(f.Run("SET BLOCK 40 40 IN bank coins = 1 IF EXISTS RETURNING *"), Answer(false, std::nullopt, "*0\r\n"));
    ExpectReply(f.Run("SET BLOCK 40 40 IN bank coins = 1 IF VERSION 5 RETURNING *"), Answer(false, std::nullopt, "*0\r\n"));
    ExpectReply(f.Run("DELETE BLOCK 40 40 FROM bank IF EXISTS RETURNING *"), Answer(false, std::nullopt, "*0\r\n"));
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectReply(f.Run("DELETE BLOCK 40 40 FROM bank IF EXISTS RETURNING *"), Answer(false, std::nullopt, "*0\r\n"));
    ExpectReply(f.Run("ROLLBACK"), "+OK\r\n");
    ExpectReply(f.Run("GET BLOCK 40 40 FROM bank WITH VERSION"), "*2\r\n_\r\n_\r\n");
}

// The counter `name{labels}` in SHOW METRICS.
std::uint64_t Metric(Fixture& f, const std::string& series) {
    const auto text = f.Run("SHOW METRICS");
    const auto at = text.find("\n" + series + " ");
    assert(at != std::string::npos);
    return std::stoull(text.substr(at + series.size() + 2U));
}

// Metrics count a refusal answered as applied false as the error it stands
// for, as without RETURNING.
void TestRefusalMetrics() {
    Fixture f;
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 1"));
    const std::string mismatches = "chunkdb_errors_total{code=\"version_mismatch\"}";
    const std::string other = "chunkdb_errors_total{code=\"other\"}";
    const std::string refused = "chunkdb_commands_total{class=\"conditional\",outcome=\"error\"}";
    const std::string applied = "chunkdb_commands_total{class=\"conditional\",outcome=\"ok\"}";
    const auto count = [&] {
        return std::vector<std::uint64_t>{Metric(f, mismatches), Metric(f, other), Metric(f, refused), Metric(f, applied)};
    };
    auto before = count();
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = 2 IF VERSION 999"), "VERSION_MISMATCH");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = 2 IF coins > 5"), "CONDITION_FAILED");
    const auto plain = count();
    assert(plain[0] == before[0] + 1U && plain[1] == before[1] + 1U && plain[2] == before[2] + 2U && plain[3] == before[3]);
    before = plain;
    (void)f.Run("SET BLOCK 0 0 IN bank coins = 2 IF VERSION 999 RETURNING *");
    (void)f.Run("SET BLOCK 0 0 IN bank coins = 2 IF coins > 5 RETURNING *");
    const auto returning = count();
    assert(returning[0] == before[0] + 1U && returning[1] == before[1] + 1U && returning[2] == before[2] + 2U &&
           returning[3] == before[3]);
    // An applied write and a following statement count as ok.
    (void)f.Run("SET BLOCK 0 0 IN bank coins = 2 IF coins = 1 RETURNING *");
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 3 IF coins = 2"));
    const auto after = count();
    assert(after[2] == returning[2] && after[3] == returning[3] + 2U);
}

// A table of one bits column and a three-dimensional table.
void TestBitsAndThreeDimensions() {
    Fixture f;
    ExpectReply(f.Run("CREATE TABLE bitsy (bits bits(4)) CHUNK 2 x 2"), "+OK\r\n");
    auto reply = f.Run("SET BLOCK 0 0 IN bitsy bits = b'1010' RETURNING *");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(1, {0, 0}, "$1\r\n\x05\r\n")));
    reply = f.Run("DELETE BLOCK 0 0 FROM bitsy RETURNING *");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(1, {0, 0}, "$1\r\n\x05\r\n")));
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bitsy"), "_\r\n");

    ExpectReply(f.Run("CREATE TABLE cube (n u8) CHUNK 2 x 2 x 2"), "+OK\r\n");
    reply = f.Run("SET BLOCK 1 -2 3 IN cube n = 4 RETURNING NEW n");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(1, {1, -2, 3}, ":4\r\n")));
    ExpectError(f.Run("SET BLOCK 1 2 IN cube n = 4 RETURNING *"), "INVALID_ARGUMENT coordinate dimensions");
}

// In a transaction, rows come from the transaction's view, the version is
// null until COMMIT, and the returned block counts as a read.
void TestTransactions() {
    Fixture f;
    SessionState other;
    f.Hello(other);
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 1"));
    const auto committed = f.ChunkVersion(0, 0);
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectReply(f.Run("SET BLOCK 0 0 IN bank coins = coins + 1 RETURNING NEW coins"),
        Answer(true, std::nullopt, "*1\r\n" + Row(1, {0, 0}, ":2\r\n")));
    ExpectReply(f.Run("SET BLOCK 0 0 IN bank a = 3 RETURNING OLD coins, a"),
        Answer(true, std::nullopt, "*1\r\n" + Row(2, {0, 0}, ":2\r\n:0\r\n")));
    // A refused write in the transaction's own chunk, and in a chunk at the snapshot.
    ExpectReply(f.Run("SET BLOCK 0 0 IN bank coins = 0 IF coins > 100 RETURNING *"), Answer(false, std::nullopt, "*0\r\n"));
    ExpectReply(f.Run("DELETE BLOCK 9 9 FROM bank IF EXISTS RETURNING *"), Answer(false, std::nullopt, "*0\r\n"));
    ExpectReply(f.Run("DELETE BLOCK 0 0 FROM bank RETURNING a"), Answer(true, std::nullopt, "*1\r\n" + Row(1, {0, 0}, ":3\r\n")));
    ExpectReply(f.Run("SET BLOCK 0 0 IN bank coins = 6 RETURNING OLD *"), Answer(true, std::nullopt, "*0\r\n"));
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = 1 IF VERSION 1 RETURNING *"), "INVALID_ARGUMENT IF VERSION");
    // Others see the committed block until COMMIT.
    ExpectReply(f.Run(other, "GET BLOCK 0 0 FROM bank COLUMNS coins"), "*1\r\n:1\r\n");
    (void)VersionOf(f.Run("COMMIT"));
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bank COLUMNS coins, a"), "*2\r\n:6\r\n:0\r\n");
    assert(f.ChunkVersion(0, 0) > committed);

    // RETURNING reads its block: a write to the chunk before COMMIT conflicts.
    const chunkdb::ChunkCoord coord{2, 2, 0};
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectReply(f.Run("SET BLOCK 8 8 IN bank coins = 1 RETURNING OLD coins"), Answer(true, std::nullopt, "*0\r\n"));
    assert(f.session.transaction->read_set.contains(coord));
    (void)VersionOf(f.Run(other, "SET BLOCK 9 9 IN bank coins = 7"));
    ExpectError(f.Run("COMMIT"), "CONFLICT");
    ExpectReply(f.Run("GET BLOCK 8 8 FROM bank"), "_\r\n");
}

// A DEFAULT COMMIT_TIME column: NEW holds the commit time the write stamps,
// OLD the value before it; a transaction refuses NEW of it.
void TestCommitTime() {
    Fixture f;
    ExpectReply(f.Run("CREATE TABLE stamps (n u8, t text(4) NULL, at u64 DEFAULT COMMIT_TIME) CHUNK 2 x 2"), "+OK\r\n");
    const auto at = [&f] { return Values(f.Run("GET BLOCK 0 0 FROM stamps COLUMNS at")); };
    // The reply of an applied write of block (0, 0) returning `values`.
    const auto expect_row = [&](const std::string& reply, std::size_t columns, const std::string& values) {
        ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(columns, {0, 0}, values)));
    };
    // A new block.
    auto reply = f.Run("SET BLOCK 0 0 IN stamps n = 1 RETURNING NEW *");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(3, {0, 0}, Values(f.Run("GET BLOCK 0 0 FROM stamps")))));
    auto stamped = at();
    assert(stamped != ":0\r\n");
    // Another value changes: OLD is the stamp before, NEW the new stamp, with
    // single values, expressions and a text value.
    expect_row(f.Run("SET BLOCK 0 0 IN stamps n = 2 RETURNING OLD at"), 1U, stamped);
    reply = f.Run("SET BLOCK 0 0 IN stamps n = n + 1 RETURNING NEW at");
    expect_row(reply, 1U, at());
    reply = f.Run("SET BLOCK 0 0 IN stamps t = 'ab' RETURNING NEW at, n");
    expect_row(reply, 2U, at() + ":3\r\n");
    // Nothing changes: the stamp stays.
    stamped = at();
    expect_row(f.Run("SET BLOCK 0 0 IN stamps n = 3, t = 'ab' RETURNING NEW at"), 1U, stamped);
    assert(at() == stamped);
    // A value the write gives the column itself stays; the same value as
    // before does not count as given.
    expect_row(f.Run("SET BLOCK 0 0 IN stamps n = 4, at = 5 RETURNING NEW at"), 1U, ":5\r\n");
    assert(at() == ":5\r\n");
    reply = f.Run("SET BLOCK 0 0 IN stamps n = 5, at = 5 RETURNING NEW at");
    expect_row(reply, 1U, at());
    assert(at() != ":5\r\n");
    // A new block given a zero stamp takes the commit time.
    reply = f.Run("SET BLOCK 1 1 IN stamps at = 0 RETURNING NEW at");
    const auto new_stamp = Values(f.Run("GET BLOCK 1 1 FROM stamps COLUMNS at"));
    assert(new_stamp != ":0\r\n");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(1, {1, 1}, new_stamp)));

    // In a transaction the commit time is known only at COMMIT.
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectError(f.Run("SET BLOCK 0 0 IN stamps n = 6 RETURNING NEW at"), "INVALID_ARGUMENT COMMIT_TIME");
    ExpectError(f.Run("SET BLOCK 0 0 IN stamps n = 6 RETURNING NEW *"), "INVALID_ARGUMENT COMMIT_TIME");
    ExpectReply(f.Run("SET BLOCK 0 0 IN stamps n = 6 RETURNING NEW n"), Answer(true, std::nullopt, "*1\r\n" + Row(1, {0, 0}, ":6\r\n")));
    stamped = at();
    ExpectReply(f.Run("SET BLOCK 0 0 IN stamps n = 7 RETURNING OLD at"),
        Answer(true, std::nullopt, "*1\r\n" + Row(1, {0, 0}, stamped)));
    ExpectReply(f.Run("ROLLBACK"), "+OK\r\n");
}

// RETURNING needs READ and WRITE (WRITE includes READ).
void TestRights() {
    chunkdb::test::ScopedTempDir dir{"chunkdb-returning-rights"};
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    const auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
    const auto users = chunkdb::test::MakeUsers(dir.path(), "admin", "admin-password");
    CommandEngine engine(chunkdb::EngineConfig{.require_auth = true, .users = users, .server_version = "test"}, catalog);
    SessionState admin;
    SessionState bot;
    assert(chunkdb::test::LoginOnEngine(engine, admin, "admin", std::string("admin-password")).rfind("%", 0) == 0);
    const auto run = [&engine](SessionState& session, const std::string& line, const Parameters& parameters = {}) {
        return engine.Execute(session, line + "\r\n", parameters);
    };
    const auto verifier = chunkdb::scram::FormatVerifier(chunkdb::test::PrepareLoginVerifier(
        "bot-password", chunkdb::crypto::RandomBytes(16), chunkdb::scram::kMinIterations));
    ExpectReply(run(admin, "CREATE TABLE t (n u8) CHUNK 4 x 4"), "+OK\r\n");
    ExpectReply(run(admin, "CREATE USER bot VERIFIER $1", Parameters{verifier}), "+OK\r\n");
    ExpectReply(run(admin, "GRANT READ ON t TO bot"), "+OK\r\n");
    assert(chunkdb::test::LoginOnEngine(engine, bot, "bot", std::string("bot-password")).rfind("%", 0) == 0);
    ExpectError(run(bot, "SET BLOCK 0 0 IN t n = 1 RETURNING *"), "PERMISSION_DENIED WRITE");
    ExpectError(run(bot, "DELETE BLOCK 0 0 FROM t RETURNING *"), "PERMISSION_DENIED WRITE");
    ExpectReply(run(admin, "GRANT WRITE ON t TO bot"), "+OK\r\n");
    const auto reply = run(bot, "SET BLOCK 0 0 IN t n = 1 RETURNING *");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(1, {0, 0}, ":1\r\n")));
}

// Rows past the response bound refuse the statement before the write, in
// and out of a transaction; fewer columns fit.
void TestResponseBound() {
    chunkdb::QueryLimits limits;
    // 67 bytes of answer framing, 4 of rows array header: 29 for the row.
    limits.max_area_response_bytes = 100U;
    Fixture f(limits);
    const auto refused = f.Run("SET BLOCK 0 0 IN bank coins = 10, name = 'abcdefgh' RETURNING NEW coins, name");
    ExpectError(refused, "OUT_OF_RANGE");
    assert(Contains(refused, " reason=returning_limit") && Contains(refused, " bound=max_response_bytes") && Contains(refused, " limit=100 "));
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bank"), "_\r\n");
    auto reply = f.Run("SET BLOCK 0 0 IN bank coins = 10, name = 'abcdefgh' RETURNING NEW name");
    ExpectReply(reply, Answer(true, AnswerVersion(reply), "*1\r\n" + Row(1, {0, 0}, "$8\r\nabcdefgh\r\n")));
    const auto version = f.ChunkVersion(0, 0);
    ExpectError(f.Run("DELETE BLOCK 0 0 FROM bank RETURNING coins, name"), "OUT_OF_RANGE");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = coins + 1 IF EXISTS RETURNING OLD coins, name"), "OUT_OF_RANGE");
    assert(f.ChunkVersion(0, 0) == version);
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bank COLUMNS coins"), "*1\r\n:10\r\n");

    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectError(f.Run("DELETE BLOCK 0 0 FROM bank RETURNING coins, name"), "OUT_OF_RANGE");
    assert(f.session.transaction != nullptr && f.session.transaction->writes.empty());
    ExpectReply(f.Run("ROLLBACK"), "+OK\r\n");
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bank COLUMNS coins"), "*1\r\n:10\r\n");
}

// The row and byte bounds of ReturningRows, exact at the edge.
void TestRowsBounds() {
    ReturningRows rows(2U, 2U, 1U << 20U, 0U);
    rows.Add(0, 0, 0, {std::uint64_t{1}});
    rows.Add(1, 0, 0, {std::uint64_t{2}});
    bool refused = false;
    try {
        rows.Add(2, 0, 0, {std::uint64_t{3}});
    } catch (const ReturningLimitError& error) {
        refused = error.limit_name() == "max_returning_rows" && error.limit() == 2U;
    }
    assert(refused && rows.size() == 2U);

    // "*1\r\n" + "*3\r\n:0\r\n:0\r\n$3\r\nabc\r\n": 25 bytes with 5 of framing.
    ReturningRows fits(2U, 10U, 30U, 5U);
    fits.Add(0, 0, 0, {std::string("abc")});
    assert(fits.encoded_bytes() == 25U);
    ReturningRows over(2U, 10U, 29U, 5U);
    refused = false;
    try {
        over.Add(0, 0, 0, {std::string("abc")});
    } catch (const ReturningLimitError& error) {
        refused = error.limit_name() == "max_response_bytes";
    }
    assert(refused && over.size() == 0U);
    assert(chunkdb::kMaxReturningRows == 65536U);
}

std::string Drain(ReplyContinuation& reply, std::size_t credit) {
    std::string out;
    while (!reply.done()) {
        const auto piece = reply.Next(credit);
        assert(!piece.empty() && piece.size() <= credit);
        out += piece;
    }
    return out;
}

// A continuation writes the same bytes as the joined reply in pieces of any
// size, and is done exactly at its last byte.
void TestContinuation() {
    const auto build = [] {
        ReturningRows rows(3U, 16U, 1U << 20U, 0U);
        rows.Add(-1, 2, std::numeric_limits<std::int64_t>::min(),
            {std::monostate{}, true, std::uint64_t{std::numeric_limits<std::uint64_t>::max()}, std::int64_t{-7}});
        rows.Add(0, 0, 0, {1.5F, 2.25, std::string("text"), chunkdb::BytesValue{.bytes = {0x00, 0xff}}});
        rows.Add(1, 1, 1, {chunkdb::BitsValue{.digits = "101"}, std::string(70000U, 'x'), std::monostate{}, false});
        return rows;
    };
    const std::string prefix = "%1\r\n$4\r\nrows\r\n";
    const std::string joined = build().Reply(prefix, "+tail\r\n", nullptr);
    assert(joined.starts_with(prefix + "*3\r\n*7\r\n:-1\r\n:2\r\n:-9223372036854775808\r\n_\r\n#t\r\n"));
    assert(joined.ends_with("_\r\n#f\r\n+tail\r\n"));
    assert(build().encoded_bytes() == joined.size() - prefix.size() - 7U);
    for (const std::size_t credit : {1U, 3U, 7U, 64U, 4096U, 65536U, 1U << 20U}) {
        std::unique_ptr<ReplyContinuation> continuation;
        ExpectReply(build().Reply(prefix, "+tail\r\n", &continuation), "");
        assert(continuation && continuation->encoded_size() == joined.size());
        ExpectReply(Drain(*continuation, credit), joined);
    }
    // No rows: done after one piece that holds the whole reply.
    std::unique_ptr<ReplyContinuation> empty;
    ExpectReply(ReturningRows(2U, 1U, 100U, 0U).Reply("+", "", &empty), "");
    ExpectReply(empty->Next(4U), "+*0\r");
    ExpectReply(empty->Next(4U), "\n");
    assert(empty->done());
}

// Through the engine: the reply is a continuation when the caller writes
// one, and its capture is admitted before the write.
void TestEngineContinuation() {
    Fixture f;
    std::unique_ptr<ReplyContinuation> continuation;
    ExpectReply(f.engine->Execute(f.session, "SET BLOCK 1 1 IN bank coins = 3, name = 'n' RETURNING NEW *\r\n", {}, &continuation), "");
    assert(continuation);
    const auto streamed = Drain(*continuation, 5U);
    ExpectReply(streamed, Answer(true, AnswerVersion(streamed), "*1\r\n" + Row(6, {1, 1}, Values(f.Run("GET BLOCK 1 1 FROM bank")))));
    // A write without RETURNING still answers in one piece.
    ExpectReply(f.engine->Execute(f.session, "SET BLOCK 1 1 IN bank coins = 4\r\n", {}, &continuation).substr(0U, 1U), ":");
    assert(!continuation);

    const auto version = f.ChunkVersion(1, 1);
    std::size_t asked = 0U;
    const std::function<void(std::size_t)> refuse = [&asked](std::size_t bytes) {
        asked = bytes;
        throw chunkdb::ReplyAdmissionError("reply capture budget is full");
    };
    ExpectError(f.engine->Execute(f.session, "DELETE BLOCK 1 1 FROM bank RETURNING *\r\n", {}, &continuation, &refuse),
        "BUSY reason=reply_budget");
    assert(asked != 0U && f.ChunkVersion(1, 1) == version);
    ExpectReply(f.Run("GET BLOCK 1 1 FROM bank COLUMNS coins"), "*1\r\n:4\r\n");
}

}  // namespace

int main() {
    TestSet();
    TestNewBlockDefaults();
    TestConditional();
    TestDelete();
    TestBitsAndThreeDimensions();
    TestTransactions();
    TestCommitTime();
    TestRights();
    TestResponseBound();
    TestRowsBounds();
    TestContinuation();
    TestEngineContinuation();
    TestRepeatedColumn();
    TestNeverWrittenChunk();
    TestRefusalMetrics();
    return 0;
}
