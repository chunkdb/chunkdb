// Protocol 3 (#62, docs/CQL_DESIGN.md) through the engine: HELLO 3, the
// block statements with literals and parameters, typed replies, IF VERSION,
// and how parameter frames are planned.

#include <bit>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/table_catalog.hpp"
#include "test_utils.hpp"

namespace {

using chunkdb::Column;
using chunkdb::ColumnKind;
using chunkdb::ColumnType;
using chunkdb::CommandEngine;
using chunkdb::TableSchema;
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

Column MakeColumn(std::uint32_t id, std::string name, ColumnKind kind, std::uint32_t size, bool nullable = false) {
    Column column;
    column.id = id;
    column.name = std::move(name);
    column.type = ColumnType{.kind = kind, .size = size};
    column.nullable = nullable;
    return column;
}

TableSchema World() {
    auto id = MakeColumn(1, "id", ColumnKind::kUnsigned, 10);
    id.required = true;
    return TableSchema{
        .version = 1,
        .next_column_id = 9,
        .columns =
            {id,
             MakeColumn(2, "temp", ColumnKind::kSigned, 8, true),
             MakeColumn(3, "solid", ColumnKind::kBool, 1),
             MakeColumn(4, "h", ColumnKind::kFloat32, 32),
             MakeColumn(5, "d", ColumnKind::kFloat64, 64),
             MakeColumn(6, "mask", ColumnKind::kBits, 3, true),
             MakeColumn(7, "name", ColumnKind::kText, 16, true),
             MakeColumn(8, "blob", ColumnKind::kBytes, 4)},
    };
}

constexpr chunkdb::GeometryConfig kWorldGeometry{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 4,
    .chunk_height_blocks = 4,
    .block_bits = 10 + 8 + 1 + 32 + 64 + 3,
};

constexpr chunkdb::GeometryConfig kPlainGeometry{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 4,
    .chunk_height_blocks = 4,
    .block_bits = 12,
};

std::string LittleEndian(std::uint64_t value, std::size_t bytes) {
    std::string out;
    for (std::size_t i = 0; i < bytes; ++i) {
        out.push_back(static_cast<char>((value >> (8U * i)) & 0xffU));
    }
    return out;
}

// ":<n>\r\n" -> n.
std::uint64_t VersionOf(const std::string& reply) {
    assert(reply.size() > 3 && reply[0] == ':');
    return std::stoull(reply.substr(1));
}

struct Fixture {
    chunkdb::test::ScopedTempDir dir{"chunkdb-cql-engine"};
    std::shared_ptr<chunkdb::TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    chunkdb::SessionState session;

    Fixture() {
        chunkdb::CatalogConfig config;
        config.data_dir = dir.path();
        catalog = std::make_shared<chunkdb::TableCatalog>(config);
        (void)catalog->Create("world", kWorldGeometry, chunkdb::TableOptions{}, World());
        (void)catalog->Create("plain", kPlainGeometry, chunkdb::TableOptions{});
        engine = std::make_unique<CommandEngine>(
            chunkdb::EngineConfig{.auth_token = "", .require_auth = false, .server_version = "test"},
            catalog);
        const std::string hello = engine->Execute(session, "HELLO 3\r\n");
        assert(hello.rfind("%6\r\n", 0) == 0);
    }

    std::string Run(const std::string& line, const Parameters& parameters = {}) {
        return engine->Execute(session, line + "\r\n", {}, parameters);
    }
};

void TestHello() {
    chunkdb::test::ScopedTempDir dir("chunkdb-cql-hello");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
    CommandEngine engine(
        chunkdb::EngineConfig{.auth_token = "secret", .require_auth = true, .server_version = "test"},
        catalog);

    chunkdb::SessionState before;
    ExpectError(engine.Execute(before, "GET BLOCK 0 0 FROM world\r\n"), "PROTOCOL expected HELLO 2");
    assert(before.close_after_reply);

    chunkdb::SessionState other;
    ExpectError(engine.Execute(other, "HELLO 4\r\n"), "PROTOCOL expected HELLO 2");
    assert(other.close_after_reply);

    chunkdb::SessionState anonymous;
    ExpectError(engine.Execute(anonymous, "HELLO 3\r\n"), "AUTH_REQUIRED use HELLO 3 AUTH <token>");

    chunkdb::SessionState with_table;
    ExpectError(engine.Execute(with_table, "HELLO 3 AUTH secret TABLE world\r\n"), "options are AUTH <token>");

    chunkdb::SessionState session;
    const std::string hello = engine.Execute(session, "HELLO 3 AUTH secret\r\n");
    assert(hello.rfind("%6\r\n$8\r\nprotocol\r\n:3\r\n$14\r\nserver_version\r\n$4\r\ntest\r\n", 0) == 0);
    assert(Contains(hello, "$14\r\nmax_parameters\r\n:65535\r\n"));
    assert(session.greeted && session.protocol == chunkdb::kCqlProtocolVersion);
    ExpectError(engine.Execute(session, "HELLO 3 AUTH secret\r\n"), "PROTOCOL HELLO was already sent");
    // Protocol 2 commands are not statements.
    ExpectError(engine.Execute(session, "PING\r\n"), "SYNTAX column 1: unknown statement 'PING'");
}

void TestLiteralsAndTypedReplies() {
    Fixture f;
    const std::string set = f.Run(
        "SET BLOCK 1 2 IN world id = 5, temp = -3, solid = TRUE, h = 1.5, d = 2e-3, mask = b'110', "
        "name = 'it''s', blob = x'00ff'");
    const std::uint64_t version = VersionOf(set);
    ExpectReply(
        f.Run("GET BLOCK 1 2 FROM world"),
        std::string("*8\r\n:5\r\n:-3\r\n#t\r\n,1.5\r\n,0.002\r\n$1\r\n\x03\r\n$4\r\nit's\r\n$2\r\n") +
            std::string("\x00\xff", 2) + "\r\n");
    ExpectReply(f.Run("get block 1 2 from world columns name, id"), "*2\r\n$4\r\nit's\r\n:5\r\n");
    ExpectReply(f.Run("GET BLOCK 1 3 FROM world"), "_\r\n");

    // A new block takes defaults: NULL for NULL columns, zero and empty
    // otherwise.
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN world id = 1"));
    ExpectReply(f.Run("GET BLOCK 0 0 FROM world"), "*8\r\n:1\r\n_\r\n#f\r\n,0\r\n,0\r\n_\r\n_\r\n$0\r\n\r\n");
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN world h = -inf, d = nan, temp = NULL"));
    ExpectReply(f.Run("GET BLOCK 0 0 FROM world COLUMNS h, d, temp"), "*3\r\n,-inf\r\n,nan\r\n_\r\n");
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN world h = 3, d = -7"));
    ExpectReply(f.Run("GET BLOCK 0 0 FROM world COLUMNS h, d"), "*2\r\n,3\r\n,-7\r\n");

    // IF VERSION: the chunk version, as in every conditional write.
    ExpectError(
        f.Run("SET BLOCK 1 2 IN world id = 6 IF VERSION 1"),
        "VERSION_MISMATCH current=" + std::to_string(VersionOf(f.Run("SET BLOCK 1 2 IN world id = 5"))));
    const std::uint64_t current = VersionOf(f.Run("SET BLOCK 1 2 IN world id = 5"));
    assert(current > version);
    const std::uint64_t next = VersionOf(f.Run("SET BLOCK 1 2 IN world id = 6 IF VERSION " + std::to_string(current)));
    assert(next > current);
    ExpectReply(f.Run("GET BLOCK 1 2 FROM world COLUMNS id"), "*1\r\n:6\r\n");

    ExpectError(f.Run("DELETE BLOCK 1 2 FROM world IF VERSION " + std::to_string(current)), "VERSION_MISMATCH");
    ExpectReply(f.Run("GET BLOCK 1 2 FROM world COLUMNS id"), "*1\r\n:6\r\n");
    (void)VersionOf(f.Run("DELETE BLOCK 1 2 FROM world IF VERSION " + std::to_string(next)));
    ExpectReply(f.Run("GET BLOCK 1 2 FROM world"), "_\r\n");
    // Deleting an absent block changes nothing and still answers the version.
    (void)VersionOf(f.Run("DELETE BLOCK 1 2 FROM world"));
}

void TestParameters() {
    Fixture f;
    const double quarter = 0.25;
    const std::string set = f.Run(
        "SET BLOCK 3 3 IN world id = $1, d = $2, name = $3, temp = $4, h = $5, solid = $6, mask = $7, blob = $8",
        Parameters{
            LittleEndian(7, 8),
            LittleEndian(std::bit_cast<std::uint64_t>(quarter), 8),
            std::string("h\ri"),
            std::nullopt,
            LittleEndian(std::bit_cast<std::uint32_t>(2.5F), 4),
            std::string(1, '\x01'),
            std::string(1, '\x05'),
            std::string(),
        });
    (void)VersionOf(set);
    ExpectReply(
        f.Run("GET BLOCK 3 3 FROM world"),
        "*8\r\n:7\r\n_\r\n#t\r\n,2.5\r\n,0.25\r\n$1\r\n\x05\r\n$3\r\nh\ri\r\n$0\r\n\r\n");
    (void)VersionOf(f.Run("SET BLOCK 3 3 IN world temp = $1", Parameters{LittleEndian(static_cast<std::uint64_t>(-100), 8)}));
    ExpectReply(f.Run("GET BLOCK 3 3 FROM world COLUMNS temp"), "*1\r\n:-100\r\n");

    ExpectError(f.Run("SET BLOCK 3 3 IN world id = $1", Parameters{LittleEndian(7, 4)}), "$1 for column id (u10) must be 8 bytes, got 4");
    ExpectError(f.Run("SET BLOCK 3 3 IN world solid = $1", Parameters{std::string(1, '\x02')}), "must be 0 or 1");
    ExpectError(f.Run("SET BLOCK 3 3 IN world mask = $1", Parameters{std::string(1, '\x08')}), "has bits past its width");
    ExpectError(f.Run("SET BLOCK 3 3 IN world id = $1", Parameters{LittleEndian(1024, 8)}), "id");
    ExpectError(f.Run("SET BLOCK 3 3 IN world id = $1"), "PROTOCOL the statement takes 1 parameters, got 0");
    ExpectReply(f.Run("GET BLOCK 3 3 FROM world COLUMNS id, solid"), "*2\r\n:7\r\n#t\r\n");
}

void TestErrors() {
    Fixture f;
    ExpectError(f.Run("GET BLOCK 0 0 FROM nowhere"), "NO_TABLE table 'nowhere' does not exist");
    ExpectError(f.Run("GET BLOCK 0 0 FROM world COLUMNS nope"), "INVALID_ARGUMENT the table has no column nope");
    ExpectError(f.Run("SET BLOCK 0 0 IN world nope = 1"), "INVALID_ARGUMENT the table has no column nope");
    ExpectError(f.Run("SET BLOCK 0 0 IN world id = 'x'"), "column id (u10) does not take text");
    ExpectError(f.Run("SET BLOCK 0 0 IN world id = -1"), "does not hold a negative value");
    ExpectError(f.Run("SET BLOCK 0 0 IN world id = 1.5"), "does not take a number with a fraction");
    ExpectError(f.Run("SET BLOCK 0 0 IN world id = 1, solid = 1"), "column solid (bool) does not take an integer");
    ExpectError(f.Run("SET BLOCK 0 0 IN world id = 1, h = 1e39"), "column h (f32) does not hold this value");
    ExpectError(f.Run("SET BLOCK 0 0 IN world id = 2000"), "INVALID_ARGUMENT");
    ExpectError(f.Run("SET BLOCK 0 0 IN world id = 1, id = 2"), "given twice");
    ExpectError(f.Run("SET BLOCK 0 0 IN world temp = 1"), "REQUIRED");
    ExpectError(f.Run("SET BLOCK 0 0 IN world id = 1, name = '01234567890123456'"), "INVALID_ARGUMENT");
    ExpectError(f.Run("SET BLOCK 0 0 IN world id = 1, mask = b'1'"), "INVALID_ARGUMENT");
    ExpectError(f.Run("GET BLOCK 0 0 FROM world;"), "SYNTAX column 25: unexpected ';'");
    ExpectError(f.Run("SET BLOCK 0 0 IN world id = 1\x01"), "SYNTAX column 30: unexpected '\\x01'");
    ExpectError(f.Run("CREATE TABLE t (a u8) CHUNK 4 x 4"), "UNKNOWN_COMMAND this statement is not available yet");
    ExpectReply(f.Run("GET BLOCK 0 0 FROM world"), "_\r\n");
}

// A table of one bits(N) column, as protocol 2 tables are.
void TestBitStringTable() {
    Fixture f;
    const std::uint64_t version = VersionOf(f.Run("SET BLOCK 5 5 IN plain bits = b'101000000001'"));
    ExpectReply(f.Run("GET BLOCK 5 5 FROM plain"), "*1\r\n$2\r\n\x05\x08\r\n");
    ExpectError(f.Run("DELETE BLOCK 5 5 FROM plain IF VERSION " + std::to_string(version + 1)), "VERSION_MISMATCH");
    (void)VersionOf(f.Run("DELETE BLOCK 5 5 FROM plain IF VERSION " + std::to_string(version)));
    ExpectReply(f.Run("GET BLOCK 5 5 FROM plain"), "_\r\n");
}

void TestPlanParameters() {
    Fixture f;
    using Plan = CommandEngine::PayloadPlan;
    auto plan = f.engine->PlanPayload(f.session, "SET BLOCK 0 0 IN world name = $2, id = $1, mask = $3\r\n");
    assert(plan.plan == Plan::kParameters);
    assert((plan.parameter_limits == std::vector<std::size_t>{8, 16, 1}));
    assert(f.engine->PlanPayload(f.session, "GET BLOCK 0 0 FROM world\r\n").plan == Plan::kNone);
    assert(f.engine->PlanPayload(f.session, "SET BLOCK 0 0 IN world name = 'costs $1'\r\n").plan == Plan::kNone);

    plan = f.engine->PlanPayload(f.session, "SET BLOCK 0 0 IN nowhere id = $1\r\n");
    assert(plan.plan == Plan::kReject && Contains(plan.reject_response, "NO_TABLE"));
    plan = f.engine->PlanPayload(f.session, "SET BLOCK 0 0 IN world nope = $1\r\n");
    assert(plan.plan == Plan::kReject && Contains(plan.reject_response, "no column nope"));
    plan = f.engine->PlanPayload(f.session, "SET BLOCK $1\r\n");
    assert(plan.plan == Plan::kReject && Contains(plan.reject_response, "SYNTAX"));
    plan = f.engine->PlanPayload(f.session, "SET BLOCK 0 0 IN world id = $1, d = $3\r\n");
    assert(plan.plan == Plan::kReject && Contains(plan.reject_response, "without gaps"));
}

}  // namespace

int main() {
    TestHello();
    TestLiteralsAndTypedReplies();
    TestParameters();
    TestErrors();
    TestBitStringTable();
    TestPlanParameters();
    return 0;
}
