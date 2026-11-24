// SET BLOCK and DELETE BLOCK with IF <expr> and computed values (#145,
// docs/DESIGN-2.0.md E2 and E3): evaluation under the write's chunk lock,
// CONDITION_FAILED, OUT_OF_RANGE and NULL rules, transactions, rights and
// concurrent writers.

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "login_helpers.hpp"
#include "test_utils.hpp"
#include "../src/sectioned_chunk_form.hpp"

namespace {

using chunkdb::BlockWriteView;
using chunkdb::ColumnAssignment;
using chunkdb::ColumnValue;
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

// ":<n>\r\n" -> n.
std::uint64_t VersionOf(const std::string& reply) {
    if (reply.size() <= 3 || reply[0] != ':') {
        std::fprintf(stderr, "expected a version, got '%s'\n", reply.c_str());
        assert(false);
    }
    return std::stoull(reply.substr(1));
}

std::string Le(std::uint64_t value, std::size_t bytes = 8) {
    std::string out;
    for (std::size_t i = 0; i < bytes; ++i) out.push_back(static_cast<char>((value >> (8U * i)) & 0xffU));
    return out;
}

std::string Failed(std::uint64_t current) {
    return "CONDITION_FAILED current=" + std::to_string(current) + " reason=condition_failed the condition does not hold";
}

struct Fixture {
    chunkdb::test::ScopedTempDir dir{"chunkdb-conditional-writes"};
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

    std::string Coins(std::int64_t x = 0, std::int64_t y = 0) {
        return Run("GET BLOCK " + std::to_string(x) + " " + std::to_string(y) + " FROM bank COLUMNS coins");
    }
};

std::string Int(std::uint64_t value) {
    return "*1\r\n:" + std::to_string(value) + "\r\n";
}

// E2: values computed from the block as it was, refused when they leave
// the column's type.
void TestComputedValues() {
    Fixture f;
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 10, a = 1, b = 2"));
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = coins + $1", Parameters{Le(5)}));
    ExpectReply(f.Coins(), Int(15));
    // All values read the block before the statement.
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank a = b, b = a"));
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bank COLUMNS a, b"), "*2\r\n:2\r\n:1\r\n");
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = GREATEST(coins * 2, 7) - POS_X, a = 3"));
    ExpectReply(f.Coins(), Int(30));
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bank COLUMNS a"), "*1\r\n:3\r\n");

    // No wrap and no clamp: the block keeps its values.
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = coins + 65530"), "OUT_OF_RANGE");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = coins - 31"), "OUT_OF_RANGE");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank rate = 1e38 * 10"), "OUT_OF_RANGE");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank name = COALESCE(name, 'abcdefghi')"), "OUT_OF_RANGE");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = coins / 0"), "INVALID_ARGUMENT");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = name + 1"), "INVALID_ARGUMENT");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = missing + 1"), "INVALID_ARGUMENT");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank missing = coins + 1"), "INVALID_ARGUMENT");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = coins + $1", Parameters{Le(1, 4)}),
                "INVALID_ARGUMENT $1 for a u16 value must be 8 bytes, got 4");
    ExpectReply(f.Coins(), Int(30));

    // NULL: arithmetic gives NULL, which a NULL column takes and a column
    // without NULL refuses. An absent block reads as NULL.
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank bonus = bonus + 1"));
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bank COLUMNS bonus"), "*1\r\n_\r\n");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = coins + bonus"), "INVALID_ARGUMENT");
    ExpectError(f.Run("SET BLOCK 1 1 IN bank coins = coins + 1"), "INVALID_ARGUMENT");
    ExpectReply(f.Run("GET BLOCK 1 1 FROM bank"), "_\r\n");
    // A counter starts at the absent block; other columns take defaults.
    (void)VersionOf(f.Run("SET BLOCK 1 1 IN bank coins = COALESCE(coins, 0) + 1"));
    (void)VersionOf(f.Run("SET BLOCK 1 1 IN bank coins = COALESCE(coins, 0) + 1"));
    ExpectReply(f.Run("GET BLOCK 1 1 FROM bank COLUMNS coins, bonus, a"), "*3\r\n:2\r\n_\r\n:0\r\n");

    // Floats round to f32 when assigned.
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank rate = 0.1 + 0.2"));
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bank COLUMNS rate"), "*1\r\n,0.3\r\n");

    // IF VERSION combines with computed values.
    const auto version = VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 1"));
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = coins + 1 IF VERSION " + std::to_string(version + 100)),
                "VERSION_MISMATCH current=" + std::to_string(version));
    ExpectReply(f.Coins(), Int(1));
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = coins + 1 IF VERSION " + std::to_string(version)));
    ExpectReply(f.Coins(), Int(2));

    // NOW() is the write's commit time.
    ExpectReply(f.Run("CREATE TABLE clock (t u64) CHUNK 4 x 4"), "+OK\r\n");
    (void)VersionOf(f.Run("SET BLOCK 1 2 IN clock t = NOW()"));
    const auto lease = f.catalog->Find("clock")->Acquire();
    ExpectReply(f.Run("GET BLOCK 1 2 FROM clock"), Int(lease->store().ChunkCommitTimeForTests(0, 0)));
}

// E3: IF <expr> is read with the write; when it is false or unknown the
// statement answers CONDITION_FAILED and changes nothing.
void TestConditions() {
    Fixture f;
    auto version = VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 15"));
    version = VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 1 IF coins > 5"));
    const std::string describe = f.Run("DESCRIBE bank");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = 2 IF coins > 5"), Failed(version));
    // Unknown is false.
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = 2 IF bonus > 0"), Failed(version));
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = 2 IF NOT (bonus > 0)"), Failed(version));
    ExpectError(f.Run("DELETE BLOCK 0 0 FROM bank IF coins <> 1"), Failed(version));
    // No change: no revision, so no feed event.
    ExpectReply(f.Coins(), Int(1));
    ExpectReply(f.Run("DESCRIBE bank"), describe);

    // Parameters take the type of the operand they meet.
    version = VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = $1 IF coins >= $2 AND name IS NULL",
                              Parameters{Le(9), Le(1)}));
    ExpectReply(f.Coins(), Int(9));
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = coins - $1 IF coins >= $1 + 0", Parameters{Le(10)}), "SYNTAX");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = coins - $1 IF coins >= $2", Parameters{Le(10), Le(10)}),
                Failed(version));

    // Insert if absent, delete if present.
    (void)VersionOf(f.Run("SET BLOCK 2 2 IN bank coins = 4 IF NOT EXISTS"));
    const auto created = VersionOf(f.Run("GET BLOCK 2 2 FROM bank WITH VERSION").substr(4));
    ExpectError(f.Run("SET BLOCK 2 2 IN bank coins = 5 IF NOT EXISTS"), Failed(created));
    ExpectReply(f.Coins(2, 2), Int(4));
    (void)VersionOf(f.Run("DELETE BLOCK 2 2 FROM bank IF EXISTS AND coins = 4"));
    ExpectReply(f.Run("GET BLOCK 2 2 FROM bank"), "_\r\n");
    ExpectError(f.Run("DELETE BLOCK 2 2 FROM bank IF EXISTS"), "CONDITION_FAILED");
    // A never-written chunk has version 0.
    ExpectError(f.Run("DELETE BLOCK 100 100 FROM bank IF EXISTS"), Failed(0));

    // Chunk columns and positions.
    (void)VersionOf(f.Run("SET CHUNK 0 0 IN bank META status = 3"));
    (void)VersionOf(f.Run("SET BLOCK 1 0 IN bank coins = 7 IF chunk.status = 3 AND POS_X = 1"));
    ExpectError(f.Run("SET BLOCK 1 0 IN bank coins = 8 IF chunk.status = 4"), "CONDITION_FAILED");
    ExpectReply(f.Coins(1, 0), Int(7));

    // Errors in the condition change nothing either.
    ExpectError(f.Run("SET BLOCK 1 0 IN bank coins = 8 IF coins / 0 = 1"), "INVALID_ARGUMENT");
    ExpectError(f.Run("SET BLOCK 1 0 IN bank coins = 8 IF missing = 1"), "INVALID_ARGUMENT");
    ExpectError(f.Run("SET BLOCK 1 0 IN bank coins = 8 IF coins = 'x'"), "INVALID_ARGUMENT");
    ExpectReply(f.Coins(1, 0), Int(7));

    // Coordinates follow the table's dims.
    ExpectError(f.Run("SET BLOCK 1 0 0 IN bank coins = 8 IF EXISTS"), "INVALID_ARGUMENT coordinate dimensions differ");

    // The frames of the expressions' parameters are bounded by their types.
    using Plan = CommandEngine::PayloadPlan;
    auto plan = f.engine->PlanPayload(f.session, "SET BLOCK 0 0 IN bank coins = coins + $1 IF name = $2 AND a = $3\r\n");
    assert(plan.plan == Plan::kParameters && (plan.parameter_limits == std::vector<std::size_t>{8, 8, 8}));
    plan = f.engine->PlanPayload(f.session, "SET BLOCK 0 0 IN bank name = $2, coins = $1 IF rate < $3\r\n");
    assert(plan.plan == Plan::kParameters && (plan.parameter_limits == std::vector<std::size_t>{8, 8, 4}));
    plan = f.engine->PlanPayload(f.session, "DELETE BLOCK 0 0 FROM bank IF bonus = $1\r\n");
    assert(plan.plan == Plan::kParameters && (plan.parameter_limits == std::vector<std::size_t>{8}));
    plan = f.engine->PlanPayload(f.session, "DELETE BLOCK 0 0 FROM bank IF missing = $1\r\n");
    assert(plan.plan == Plan::kDiscard && Contains(plan.reject_response, "INVALID_ARGUMENT"));
    plan = f.engine->PlanPayload(f.session, "DELETE BLOCK 0 0 FROM nowhere IF a = $1\r\n");
    assert(plan.plan == Plan::kDiscard && Contains(plan.reject_response, "NO_TABLE"));
}

// E3: in a transaction a condition reads the transaction's view and is a
// read of the chunk at COMMIT; values read the view too.
void TestTransactions() {
    Fixture f;
    SessionState other;
    f.Hello(other);
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 1"));
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectReply(f.Run("SET BLOCK 0 0 IN bank coins = coins + 1"), "_\r\n");
    ExpectReply(f.Run("SET BLOCK 0 0 IN bank coins = coins + 1 IF coins = 2"), "_\r\n");
    ExpectReply(f.Coins(), Int(3));
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = 10 IF coins = 2"), "CONDITION_FAILED");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = 10 IF coins = 2 AND NOW() > 0"), "INVALID_ARGUMENT NOW()");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = coins + 1 IF VERSION 1"), "INVALID_ARGUMENT IF VERSION");
    ExpectReply(f.Coins(), Int(3));
    // Other sessions see the block as committed.
    ExpectReply(f.Run(other, "GET BLOCK 0 0 FROM bank COLUMNS coins"), Int(1));
    (void)VersionOf(f.Run("COMMIT"));
    ExpectReply(f.Coins(), Int(3));

    // A failed condition keeps the transaction open and nothing of it is written.
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectError(f.Run("DELETE BLOCK 0 0 FROM bank IF coins > 100"), "CONDITION_FAILED");
    ExpectReply(f.Run("DELETE BLOCK 0 0 FROM bank IF coins = 3"), "_\r\n");
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bank"), "_\r\n");
    ExpectReply(f.Run("ROLLBACK"), "+OK\r\n");
    ExpectReply(f.Coins(), Int(3));

    // The chunk a failed condition read changes before COMMIT: CONFLICT.
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = 0 IF coins > 100"), "CONDITION_FAILED");
    ExpectReply(f.Run("SET BLOCK 8 8 IN bank coins = 1"), "_\r\n");
    (void)VersionOf(f.Run(other, "SET BLOCK 1 1 IN bank coins = 200"));
    ExpectError(f.Run("COMMIT"), "CONFLICT");
    ExpectReply(f.Run("GET BLOCK 8 8 FROM bank"), "_\r\n");

    // Computed values without a condition rely on the write's own check.
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectReply(f.Run("SET BLOCK 0 0 IN bank coins = coins * 2"), "_\r\n");
    (void)VersionOf(f.Run(other, "SET BLOCK 0 0 IN bank coins = 50"));
    ExpectError(f.Run("COMMIT"), "CONFLICT");
    ExpectReply(f.Coins(), Int(50));
}

// E3: a condition needs READ and WRITE (WRITE includes READ), as computed
// values do.
void TestRights() {
    chunkdb::test::ScopedTempDir dir{"chunkdb-conditional-rights"};
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
    ExpectError(run(bot, "SET BLOCK 0 0 IN t n = COALESCE(n, 0) + 1"), "PERMISSION_DENIED WRITE");
    ExpectError(run(bot, "SET BLOCK 0 0 IN t n = 5 IF n IS NULL"), "PERMISSION_DENIED WRITE");
    ExpectError(run(bot, "DELETE BLOCK 0 0 FROM t IF EXISTS"), "PERMISSION_DENIED WRITE");
    ExpectReply(run(admin, "GRANT WRITE ON t TO bot"), "+OK\r\n");
    (void)VersionOf(run(bot, "SET BLOCK 0 0 IN t n = COALESCE(n, 0) + 1"));
    (void)VersionOf(run(bot, "SET BLOCK 0 0 IN t n = 5 IF n = 1"));
    ExpectReply(run(admin, "GET BLOCK 0 0 FROM t"), Int(5));
}

// The store runs the decision under the chunk's lock: a writer that starts
// while it runs writes after it.
void TestDecisionHoldsTheLock() {
    using namespace std::chrono_literals;
    Fixture f;
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 10"));
    const auto lease = f.catalog->Find("bank")->Acquire();
    auto& store = lease->store();
    std::atomic<bool> started{false};
    std::uint64_t other_version = 0;
    std::thread other;
    const auto result = store.WriteBlockDecided(0, 0, 0, false, std::vector<std::size_t>{0}, false,
        [&](const BlockWriteView& block, std::vector<ColumnAssignment>& values) {
            assert(block.values != nullptr && block.commit_time_ms != 0U);
            const auto coins = std::get<std::uint64_t>((*block.values)[0]);
            other = std::thread([&] {
                started = true;
                other_version = store.SetBlock(0, 0, {ColumnAssignment{"coins", std::uint64_t{500}}}, std::nullopt).version;
            });
            while (!started) std::this_thread::yield();
            // Time for the other writer to reach the lock; it must wait.
            std::this_thread::sleep_for(50ms);
            values.push_back(ColumnAssignment{"coins", coins + 1});
            return true;
        });
    other.join();
    assert(result.ok && other_version > result.version);
    ExpectReply(f.Coins(), Int(500));

    // A refusal and a throw change nothing.
    const auto before = f.Run("GET BLOCK 0 0 FROM bank WITH VERSION");
    const auto refused = store.WriteBlockDecided(0, 0, 0, true, {}, true, [](const BlockWriteView& block, auto&) {
        assert(block.chunk_columns.size() == 1U && std::get<std::uint64_t>(block.chunk_columns[0]) == 0U);
        return false;
    });
    assert(!refused.ok && refused.version == other_version);
    bool thrown = false;
    try {
        (void)store.WriteBlockDecided(0, 0, 0, true, {}, false, [](const BlockWriteView&, auto&) -> bool {
            throw std::out_of_range("refused");
        });
    } catch (const std::out_of_range&) {
        thrown = true;
    }
    assert(thrown);
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bank WITH VERSION"), before);
}

// Concurrent writers: counters lose no update and a guarded decrement never
// goes below zero.
void TestConcurrentWriters() {
    Fixture f;
    constexpr int kThreads = 8;
    constexpr int kWrites = 100;
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 0, a = 100"));
    std::atomic<int> applied{0};
    std::atomic<int> refused{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            SessionState session;
            f.Hello(session);
            for (int i = 0; i < kWrites; ++i) {
                (void)VersionOf(f.Run(session, "SET BLOCK 0 0 IN bank coins = coins + 1"));
                const auto reply = f.Run(session, "SET BLOCK 0 0 IN bank a = a - 1 IF a > 0");
                if (reply.starts_with(":")) ++applied;
                else if (reply.starts_with("-ERR CONDITION_FAILED ")) ++refused;
                else assert(false);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bank COLUMNS coins, a"), "*2\r\n:800\r\n:0\r\n");
    assert(applied == 100 && refused == kThreads * kWrites - 100);
}


// In a transaction a value that reads the block is a read of its chunk, as
// a condition is; a value that reads nothing of the chunk is not.
void TestTransactionReadSet() {
    Fixture f;
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 1"));
    const chunkdb::ChunkCoord first{0, 0, 0};
    const chunkdb::ChunkCoord second{2, 2, 0};
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectReply(f.Run("SET BLOCK 9 9 IN bank coins = 2 * 3"), "_\r\n");
    assert(!f.session.transaction->read_set.contains(second));
    ExpectReply(f.Run("SET BLOCK 0 0 IN bank coins = coins + 1"), "_\r\n");
    assert(f.session.transaction->read_set.contains(first));
    ExpectReply(f.Run("ROLLBACK"), "+OK\r\n");
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectReply(f.Run("SET BLOCK 9 9 IN bank a = chunk.status"), "_\r\n");
    assert(f.session.transaction->read_set.contains(second));
    ExpectReply(f.Run("ROLLBACK"), "+OK\r\n");
}

// A refused condition in a transaction answers CONDITION_FAILED even when
// the transaction may write no more chunks, and adds no write.
void TestTransactionRefusalTakesNoWriteRoom() {
    chunkdb::QueryLimits limits;
    limits.txn_max_written_chunks = 1U;
    Fixture f(limits);
    (void)VersionOf(f.Run("SET BLOCK 8 8 IN bank coins = 4"));
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectReply(f.Run("SET BLOCK 0 0 IN bank coins = 1"), "_\r\n");
    ExpectError(f.Run("SET BLOCK 8 8 IN bank coins = 5 IF coins > 4"), "CONDITION_FAILED");
    ExpectError(f.Run("DELETE BLOCK 100 100 FROM bank IF EXISTS"), "CONDITION_FAILED current=0 ");
    assert(f.session.transaction->writes.size() == 1U);
    ExpectError(f.Run("SET BLOCK 8 8 IN bank coins = 5 IF coins = 4"), "INVALID_ARGUMENT a transaction writes at most 1 chunks");
    (void)VersionOf(f.Run("COMMIT"));
    ExpectReply(f.Coins(8, 8), Int(4));
}

// IF VERSION $n: the version as a u64 parameter (docs/DESIGN-2.0.md E1).
void TestVersionParameter() {
    Fixture f;
    const auto version = VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 1"));
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = 2 IF VERSION $1", Parameters{Le(version + 1)}),
                "VERSION_MISMATCH current=" + std::to_string(version));
    const auto next = VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = $2 IF VERSION $1", Parameters{Le(version), Le(2)}));
    ExpectReply(f.Coins(), Int(2));
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = coins + 1 IF VERSION $1", Parameters{Le(version)}), "VERSION_MISMATCH");
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = coins + 1 IF VERSION $1", Parameters{Le(next)}));
    ExpectReply(f.Coins(), Int(3));
    ExpectError(f.Run("DELETE BLOCK 0 0 FROM bank IF VERSION $1", Parameters{std::nullopt}), "INVALID_ARGUMENT IF VERSION cannot be NULL");
    ExpectError(f.Run("DELETE BLOCK 0 0 FROM bank IF VERSION $1", Parameters{Le(1, 4)}), "INVALID_ARGUMENT $1 for a u64 value must be 8 bytes");
    const auto current = VersionOf(f.Run("GET BLOCK 0 0 FROM bank WITH VERSION").substr(4));
    (void)VersionOf(f.Run("DELETE BLOCK 0 0 FROM bank IF VERSION $1", Parameters{Le(current)}));
    ExpectReply(f.Run("GET BLOCK 0 0 FROM bank"), "_\r\n");
    using Plan = CommandEngine::PayloadPlan;
    const auto plan = f.engine->PlanPayload(f.session, "SET BLOCK 0 0 IN bank name = $2 IF VERSION $1\r\n");
    assert(plan.plan == Plan::kParameters && (plan.parameter_limits == std::vector<std::size_t>{8, 8}));
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = 1 IF VERSION $1", Parameters{Le(1)}), "INVALID_ARGUMENT IF VERSION");
    ExpectReply(f.Run("ROLLBACK"), "+OK\r\n");
}

// The first bulk string of `reply`: the chunk form of GET CHUNK, of a GET AREA
// row or of a SCAN CHUNKS ... WITH DATA row.
std::string FirstForm(const std::string& reply) {
    const auto at = reply.find('$');
    assert(at != std::string::npos);
    const auto end = reply.find("\r\n", at);
    const auto length = std::stoull(reply.substr(at + 1U, end - at - 1U));
    return reply.substr(end + 2U, length);
}

// The version the chunk form header carries.
std::uint64_t FormVersion(const std::string& form) {
    const std::span<const std::uint8_t> bytes(reinterpret_cast<const std::uint8_t*>(form.data()), form.size());
    return chunkdb::ParseChunkFormSections(bytes, bytes.size(), chunkdb::ChunkFormReadMode::kRead).header.chunk_version;
}

// RENAME TO onto the name of a dropped table: every chunk of the renamed
// table had the version its chunk of the dropped table had, with other data.
// Each surface reports the name floor for such a chunk, refuses a write
// conditioned on the version a client read from the dropped table, and
// takes one conditioned on the floor a fresh read shows (#306).
void TestNameFloorVersions() {
    Fixture f;
    ExpectReply(f.Run("CREATE TABLE other (coins u16, bonus i8 NULL, rate f32 NULL, name text(8) NULL, a u8, b u8) "
                      "CHUNK 4 x 4 CHUNK COLUMNS (status u8 DEFAULT 0)"),
                "+OK\r\n");
    constexpr int kChunks = 8;
    std::vector<std::uint64_t> stale;
    for (int k = 0; k < kChunks; ++k) {
        const auto x = std::to_string(4 * k);
        stale.push_back(VersionOf(f.Run("SET BLOCK " + x + " 0 IN bank coins = 1")));
        assert(VersionOf(f.Run("SET BLOCK " + x + " 0 IN other coins = 2")) == stale.back());
    }
    ExpectReply(f.Run("DROP TABLE bank"), "+OK\r\n");
    ExpectReply(f.Run("ALTER TABLE other RENAME TO bank"), "+OK\r\n");
    const auto floor = f.catalog->Find("bank")->Acquire()->store().name_floor();
    assert(floor > stale.back());
    const auto s = [](std::uint64_t value) { return std::to_string(value); };
    const auto mismatch = "VERSION_MISMATCH current=" + s(floor);

    // GET BLOCK ... WITH VERSION, and SET BLOCK ... IF VERSION.
    assert(VersionOf(f.Run("GET BLOCK 0 0 FROM bank WITH VERSION").substr(4)) == floor);
    ExpectError(f.Run("SET BLOCK 0 0 IN bank coins = 3 IF VERSION " + s(stale[0])), mismatch);
    assert(VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 3 IF VERSION " + s(floor))) > floor);
    // DELETE BLOCK ... IF VERSION.
    ExpectError(f.Run("DELETE BLOCK 4 0 FROM bank IF VERSION " + s(stale[1])), mismatch);
    assert(VersionOf(f.Run("DELETE BLOCK 4 0 FROM bank IF VERSION " + s(floor))) > floor);
    // A write with computed values decides IF VERSION under the chunk lock.
    ExpectError(f.Run("SET BLOCK 8 0 IN bank coins = coins + 1 IF VERSION " + s(stale[2])), mismatch);
    assert(VersionOf(f.Run("SET BLOCK 8 0 IN bank coins = coins + 1 IF VERSION " + s(floor))) > floor);
    // RETURNING answers the version a refused write saw.
    auto reply = f.Run("SET BLOCK 12 0 IN bank coins = 4 IF VERSION " + s(stale[3]) + " RETURNING coins");
    assert(reply.starts_with("%3\r\n$7\r\napplied\r\n#f\r\n") && Contains(reply, "$7\r\nversion\r\n:" + s(floor) + "\r\n"));
    reply = f.Run("SET BLOCK 12 0 IN bank coins = 4 IF VERSION " + s(floor) + " RETURNING coins");
    assert(reply.starts_with("%3\r\n$7\r\napplied\r\n#t\r\n"));
    // GET CHUNK's form carries the floor; SET CHUNK ... IF VERSION, with the
    // form of chunk 0, which the writes above changed, so the write changes
    // chunk 4 and takes a new version.
    assert(FormVersion(FirstForm(f.Run("GET CHUNK 4 0 FROM bank"))) == floor);
    const auto form = FirstForm(f.Run("GET CHUNK 0 0 FROM bank"));
    ExpectError(f.Run("SET CHUNK 4 0 IN bank $1 IF VERSION " + s(stale[4]), Parameters{form}), mismatch);
    assert(VersionOf(f.Run("SET CHUNK 4 0 IN bank $1 IF VERSION " + s(floor), Parameters{form})) > floor);
    // SET CHUNK ... META ... IF VERSION.
    ExpectError(f.Run("SET CHUNK 5 0 IN bank META status = 1 IF VERSION " + s(stale[5])), mismatch);
    assert(VersionOf(f.Run("SET CHUNK 5 0 IN bank META status = 1 IF VERSION " + s(floor))) > floor);
    // A GET AREA row's form carries the floor.
    assert(FormVersion(FirstForm(f.Run("GET AREA 6 0 TO 6 0 FROM bank"))) == floor);
    // SCAN CHUNKS rows and their WITH DATA forms carry the floor.
    assert(Contains(f.Run("SCAN CHUNKS FROM bank AFTER 6 0 LIMIT 1"), "*3\r\n:7\r\n:0\r\n:" + s(floor) + "\r\n"));
    const auto scan = f.Run("SCAN CHUNKS FROM bank AFTER 6 0 LIMIT 1 WITH DATA");
    const auto row = "*4\r\n:7\r\n:0\r\n:" + s(floor) + "\r\n";
    assert(scan.find(row) != std::string::npos);
    assert(FormVersion(FirstForm(scan.substr(scan.find(row) + row.size()))) == floor);
    // Inside a transaction: GET BLOCK ... WITH VERSION at the snapshot, and the
    // version a refused condition answers.
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    assert(VersionOf(f.Run("GET BLOCK 28 0 FROM bank WITH VERSION").substr(4)) == floor);
    ExpectError(f.Run("SET BLOCK 28 0 IN bank coins = 5 IF coins > 100"), Failed(floor));
    ExpectReply(f.Run("ROLLBACK"), "+OK\r\n");
    // A chunk written after the rename shows its own version.
    const auto written = VersionOf(f.Run("SET BLOCK 28 0 IN bank coins = 6"));
    assert(written > floor && VersionOf(f.Run("GET BLOCK 28 0 FROM bank WITH VERSION").substr(4)) == written);
}

// Under the chunk's lock the store decodes only the columns the decision
// reads.
void TestDecisionDecodesItsColumns() {
    Fixture f;
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN bank coins = 7, name = 'abc', a = 3"));
    const auto lease = f.catalog->Find("bank")->Acquire();
    const std::vector<std::size_t> slots{0, 3};
    const auto result = lease->store().WriteBlockDecided(0, 0, 0, false, slots, false,
        [](const BlockWriteView& block, std::vector<ColumnAssignment>&) {
            const auto& values = *block.values;
            assert(values.size() == 6U && block.chunk_columns.empty());
            assert(std::get<std::uint64_t>(values[0]) == 7U && std::get<std::string>(values[3]) == "abc");
            for (const std::size_t unread : {1U, 2U, 4U, 5U}) assert(std::holds_alternative<std::monostate>(values[unread]));
            return false;
        });
    assert(!result.ok);
}
}  // namespace

int main() {
    TestComputedValues();
    TestConditions();
    TestTransactions();
    TestRights();
    TestDecisionHoldsTheLock();
    TestConcurrentWriters();
    TestTransactionReadSet();
    TestTransactionRefusalTakesNoWriteRoom();
    TestVersionParameter();
    TestDecisionDecodesItsColumns();
    TestNameFloorVersions();
    std::puts("conditional writes tests passed");
    return 0;
}
