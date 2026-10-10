// CQL (docs/CQL.md) through the engine: HELLO 3, the
// block statements with literals and parameters, typed replies, IF VERSION,
// and how parameter frames are planned.

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/table_catalog.hpp"
#include "login_helpers.hpp"
#include "../src/store_manifest.hpp"
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
            chunkdb::EngineConfig{.require_auth = false, .server_version = "test"},
            catalog);
        const std::string hello = engine->Execute(session, "HELLO 3\r\n");
        assert(hello.rfind("%8\r\n", 0) == 0);
    }

    std::string Run(const std::string& line, const Parameters& parameters = {}) {
        return engine->Execute(session, line + "\r\n", parameters);
    }
};

void TestHello() {
    chunkdb::test::ScopedTempDir dir("chunkdb-cql-hello");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
    CommandEngine engine(
        chunkdb::EngineConfig{
            .require_auth = true,
            .users = chunkdb::test::MakeUsers(dir.path(), "admin", "secret"),
            .server_version = "test",
        },
        catalog);

    chunkdb::SessionState before;
    ExpectError(engine.Execute(before, "GET BLOCK 0 0 FROM world\r\n"), "PROTOCOL expected HELLO 3");
    assert(before.close_after_reply);

    chunkdb::SessionState other;
    ExpectError(engine.Execute(other, "HELLO 4\r\n"), "PROTOCOL expected HELLO 3");
    assert(other.close_after_reply);

    chunkdb::SessionState anonymous;
    ExpectError(
        engine.Execute(anonymous, "HELLO 3\r\n"),
        "AUTH_REQUIRED use HELLO 3 USER <name> $1 with a SCRAM-SHA-256 client-first message");

    chunkdb::SessionState with_table;
    ExpectError(
        engine.Execute(with_table, "HELLO 3 USER admin $1 TABLE world\r\n"),
        "INVALID_ARGUMENT HELLO is HELLO 3, or HELLO 3 USER <name> $1");
    chunkdb::SessionState with_token;
    ExpectError(
        engine.Execute(with_token, "HELLO 3 AUTH secret\r\n"),
        "INVALID_ARGUMENT HELLO is HELLO 3, or HELLO 3 USER <name> $1");

    chunkdb::SessionState wrong;
    ExpectError(
        chunkdb::test::LoginOnEngine(engine, wrong, "admin", "wrong"), "AUTH_FAILED invalid user or password");
    assert(!wrong.greeted);

    chunkdb::SessionState session;
    const std::string hello = chunkdb::test::LoginOnEngine(engine, session, "admin", "secret");
    assert(hello.rfind("%8\r\n$8\r\nprotocol\r\n:3\r\n$14\r\nserver_version\r\n$4\r\ntest\r\n", 0) == 0);
    assert(Contains(hello, "$14\r\nmax_parameters\r\n:65535\r\n"));
    assert(session.greeted);
    assert(Contains(hello, "$14\r\nmax_scan_limit\r\n:1024\r\n"));
    assert(Contains(hello, "$16\r\nserver_signature\r\n$"));
    ExpectError(engine.Execute(session, "HELLO 3\r\n"), "PROTOCOL HELLO was already sent");
    ExpectReply(engine.Execute(session, "PING\r\n"), "+PONG\r\n");
    // Commands of the retired protocol 2 are not statements.
    ExpectError(engine.Execute(session, "GET 0 0\r\n"), "SYNTAX column 5: expected BLOCK, CHUNK or AREA, got '0'");
    ExpectError(engine.Execute(session, "CHUNKGET 0 0\r\n"), "SYNTAX column 1: unknown statement 'CHUNKGET'");
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

// Reads a bulk reply: its bytes.
std::string BulkOf(const std::string& reply) {
    assert(reply.size() >= 4 && reply[0] == '$');
    const std::size_t header_end = reply.find("\r\n");
    const std::size_t length = std::stoull(reply.substr(1, header_end - 1));
    assert(reply.size() == header_end + 2 + length + 2);
    return reply.substr(header_end + 2, length);
}

std::uint64_t LoadLittleEndian(const std::string& bytes, std::size_t offset, std::size_t size) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < size; ++i) {
        value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(bytes[offset + i])) << (8U * i);
    }
    return value;
}

void TestChunkStatements() {
    Fixture f;
    const auto geometry = f.catalog->Find("world")->geometry();
    const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
    const std::size_t presence_bytes = 2;
    // An absent chunk answers its empty form and version, so a write can
    // create it only while it is still absent.
    const std::string empty = BulkOf(f.Run("GET CHUNK 2 2 FROM world"));
    assert(empty.size() == 16 + presence_bytes + payload_bytes);
    assert(LoadLittleEndian(empty, 8, 8) == 1U);
    assert(empty[16] == 0 && empty[17] == 0);
    const std::uint64_t empty_version = LoadLittleEndian(empty, 0, 8);
    std::string one_block = empty;
    one_block[16] = '\x01';
    const std::uint64_t created = VersionOf(
        f.Run("SET CHUNK 2 2 IN world $1 IF VERSION " + std::to_string(empty_version), Parameters{one_block}));
    assert(created != empty_version);
    ExpectError(
        f.Run("SET CHUNK 2 2 IN world $1 IF VERSION " + std::to_string(empty_version), Parameters{one_block}),
        "VERSION_MISMATCH current=" + std::to_string(created));

    (void)VersionOf(f.Run("SET BLOCK 0 0 IN world id = 3, name = 'ab', blob = x'01'"));
    const std::uint64_t version = VersionOf(f.Run("SET BLOCK 1 0 IN world id = 4, temp = -1, mask = b'111'"));
    const std::string form = BulkOf(f.Run("GET CHUNK 0 0 FROM world"));
    // version, schema version, presence (blocks 0 and 1), payload, then the
    // VARS entries:
    // name and blob of block 0, 12 bytes of header each.
    assert(form.size() == 16 + presence_bytes + payload_bytes + (12 + 2) + (12 + 1));
    assert(LoadLittleEndian(form, 0, 8) == version);
    assert(static_cast<std::uint8_t>(form[16]) == 0x03 && form[17] == 0);

    // The same state into another chunk; the version is not read.
    std::string copy = form;
    copy[0] = '\x7f';
    const std::uint64_t written = VersionOf(f.Run("SET CHUNK 1 1 IN world $1", Parameters{copy}));
    ExpectReply(f.Run("GET BLOCK 4 4 FROM world COLUMNS id, name, blob"), "*3\r\n:3\r\n$2\r\nab\r\n$1\r\n\x01\r\n");
    ExpectReply(f.Run("GET BLOCK 5 4 FROM world COLUMNS id, temp, mask, name"), "*4\r\n:4\r\n:-1\r\n$1\r\n\x07\r\n_\r\n");
    ExpectError(
        f.Run("SET CHUNK 1 1 IN world $1 IF VERSION " + std::to_string(written + 1), Parameters{copy}),
        "VERSION_MISMATCH current=" + std::to_string(written));
    // A state with no values for text and bytes removes them.
    const std::string bare = form.substr(0, 16 + presence_bytes + payload_bytes);
    (void)VersionOf(f.Run("SET CHUNK 1 1 IN world $1 IF VERSION " + std::to_string(written), Parameters{bare}));
    ExpectReply(f.Run("GET BLOCK 4 4 FROM world COLUMNS id, name, blob"), "*3\r\n:3\r\n_\r\n$0\r\n\r\n");

    // COLUMNS: the sections of the named columns, then their values.
    const std::string id_only = BulkOf(f.Run("GET CHUNK 0 0 FROM world COLUMNS id"));
    assert(id_only.size() == 16 + presence_bytes + 20);
    assert(id_only.substr(18) == form.substr(18, 20));
    const std::string name_only = BulkOf(f.Run("GET CHUNK 0 0 FROM world COLUMNS name"));
    assert(name_only.size() == 16 + presence_bytes + 12 + 2);
    assert(name_only.substr(18 + 12) == "ab");

    ExpectError(f.Run("SET CHUNK 1 1 IN world $1", Parameters{form.substr(0, 20)}), "INVALID_ARGUMENT the chunk takes at least");
    ExpectError(f.Run("SET CHUNK 1 1 IN world $1", Parameters{std::nullopt}), "the chunk cannot be NULL");
    std::string absent = form;
    absent[16] = '\x02';  // block 0, which has values, absent
    ExpectError(f.Run("SET CHUNK 1 1 IN world $1", Parameters{absent}), "a value of an absent block");
    ExpectReply(f.Run("GET BLOCK 4 4 FROM world COLUMNS id"), "*1\r\n:3\r\n");

    auto plan = f.engine->PlanPayload(f.session, "SET CHUNK 0 0 IN world $1\r\n");
    assert(plan.plan == CommandEngine::PayloadPlan::kParameters);
    assert((plan.parameter_limits ==
            std::vector<std::size_t>{16 + presence_bytes + payload_bytes + chunkdb::kDefaultVarMaxChunkBytes}));

    // A form encoded for another schema version is refused: its bytes would
    // land in the wrong columns.
    ExpectReply(f.Run("ALTER TABLE world ADD COLUMN extra u8 NULL"), "+OK\r\n");
    ExpectError(
        f.Run("SET CHUNK 1 1 IN world $1", Parameters{form}),
        "SCHEMA_MISMATCH current=2 the chunk was encoded for schema version 1");
    ExpectReply(f.Run("GET BLOCK 4 4 FROM world COLUMNS id"), "*1\r\n:3\r\n");
    const std::string current = BulkOf(f.Run("GET CHUNK 1 1 FROM world"));
    assert(LoadLittleEndian(current, 8, 8) == 2U);
    (void)VersionOf(f.Run("SET CHUNK 1 1 IN world $1", Parameters{current}));
}

void TestAreaStatements() {
    Fixture f;
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN world id = 1, name = 'x'"));
    const std::uint64_t far = VersionOf(f.Run("SET BLOCK 9 -1 IN world id = 2"));
    const std::string box = f.Run("GET AREA -1 -1 TO 2 0 FROM world COLUMNS name");
    // Chunks (0, 0) and (2, -1), in ascending x then y.
    assert(box.rfind("*2\r\n*3\r\n:0\r\n:0\r\n$", 0) == 0);
    const std::string second = "*3\r\n:2\r\n:-1\r\n$18\r\n";
    assert(box.find(second) != std::string::npos);
    const std::size_t at = box.find(second) + second.size();
    assert(LoadLittleEndian(box, at, 8) == far);
    ExpectReply(f.Run("GET AREA 5 5 TO 6 6 FROM world"), "*0\r\n");
    const std::string around = f.Run("GET AREA AROUND 1 0 RADIUS 1 FROM world COLUMNS id");
    assert(around.rfind("*1\r\n*3\r\n:0\r\n:0\r\n$", 0) == 0);
    ExpectError(f.Run("GET AREA 0 0 TO 300 0 FROM world"), "INVALID_ARGUMENT");
}

// After a restart, area reads take chunks from the files without loading
// them: the same form, version and values as a chunk read.
void TestAreaFromFiles() {
    chunkdb::test::ScopedTempDir dir("chunkdb-cql-area-files");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    const chunkdb::EngineConfig engine_config{.require_auth = false, .server_version = "test"};
    {
        auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
        (void)catalog->Create("world", kWorldGeometry, chunkdb::TableOptions{}, World());
        CommandEngine engine(engine_config, catalog);
        chunkdb::SessionState session;
        (void)engine.Execute(session, "HELLO 3\r\n");
        (void)VersionOf(engine.Execute(session, "SET BLOCK 1 1 IN world id = 9, name = 'files', blob = x'0102'\r\n"));
        (void)VersionOf(engine.Execute(session, "SET BLOCK 2 1 IN world id = 8\r\n"));
    }
    auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
    CommandEngine engine(engine_config, catalog);
    chunkdb::SessionState session;
    (void)engine.Execute(session, "HELLO 3\r\n");
    const std::string area = engine.Execute(session, "GET AREA 0 0 TO 0 0 FROM world\r\n");
    const std::string chunk = engine.Execute(session, "GET CHUNK 0 0 FROM world\r\n");
    assert(area == "*1\r\n*3\r\n:0\r\n:0\r\n" + chunk);
    assert(Contains(chunk, "files"));
}

void TestTableStatements() {
    Fixture f;
    ExpectReply(
        f.Run("CREATE TABLE land (id u10 REQUIRED, light u4 DEFAULT 15, sign text(8) NULL, h f32 DEFAULT 1.5) "
              "CHUNK 4 x 4 LARGE 2 x 2 WITH var_max_chunk_bytes = 4096, durability_mode = 'fsync-wal'"),
        "+OK\r\n");
    ExpectReply(f.Run("SHOW TABLES"), "*4\r\n$7\r\ndefault\r\n$4\r\nland\r\n$5\r\nplain\r\n$5\r\nworld\r\n");
    const std::string described = f.Run("DESCRIBE land");
    const std::string columns =
        "%6\r\n$5\r\ntable\r\n$4\r\nland\r\n$7\r\nversion\r\n:1\r\n$7\r\ncolumns\r\n*4\r\n"
        "%6\r\n$2\r\nid\r\n:1\r\n$4\r\nname\r\n$2\r\nid\r\n$4\r\ntype\r\n$3\r\nu10\r\n$4\r\nnull\r\n#f\r\n$8\r\nrequired\r\n#t\r\n$7\r\ndefault\r\n_\r\n"
        "%6\r\n$2\r\nid\r\n:2\r\n$4\r\nname\r\n$5\r\nlight\r\n$4\r\ntype\r\n$2\r\nu4\r\n$4\r\nnull\r\n#f\r\n$8\r\nrequired\r\n#f\r\n$7\r\ndefault\r\n:15\r\n"
        "%6\r\n$2\r\nid\r\n:3\r\n$4\r\nname\r\n$4\r\nsign\r\n$4\r\ntype\r\n$7\r\ntext(8)\r\n$4\r\nnull\r\n#t\r\n$8\r\nrequired\r\n#f\r\n$7\r\ndefault\r\n_\r\n"
        "%6\r\n$2\r\nid\r\n:4\r\n$4\r\nname\r\n$1\r\nh\r\n$4\r\ntype\r\n$3\r\nf32\r\n$4\r\nnull\r\n#f\r\n$8\r\nrequired\r\n#f\r\n$7\r\ndefault\r\n,1.5\r\n"
        "$5\r\nchunk\r\n*2\r\n:4\r\n:4\r\n$5\r\nlarge\r\n*2\r\n:2\r\n:2\r\n$7\r\noptions\r\n%6\r\n"
        "$15\r\ndurability_mode\r\n$9\r\nfsync-wal\r\n";
    if (described.rfind(columns, 0) != 0) {
        std::fprintf(stderr, "DESCRIBE: %s\n", described.c_str());
        assert(false);
    }
    assert(Contains(described, "$19\r\nvar_max_chunk_bytes\r\n:4096\r\n"));

    (void)VersionOf(f.Run("SET BLOCK 0 0 IN land id = 7, sign = 'hi'"));
    ExpectReply(f.Run("GET BLOCK 0 0 FROM land"), "*4\r\n:7\r\n:15\r\n$2\r\nhi\r\n,1.5\r\n");

    ExpectReply(f.Run("ALTER TABLE land ADD COLUMN depth i8 NULL"), "+OK\r\n");
    ExpectReply(f.Run("GET BLOCK 0 0 FROM land COLUMNS depth, id"), "*2\r\n_\r\n:7\r\n");
    ExpectError(f.Run("ALTER TABLE land ADD COLUMN must u8 REQUIRED"), "needs a DEFAULT");

    // Wider at once; narrower after checking every value, or with USING.
    ExpectReply(f.Run("ALTER TABLE land ALTER COLUMN light TYPE u8"), "+OK\r\n");
    ExpectError(f.Run("ALTER TABLE land ALTER COLUMN light TYPE u2"), "its DEFAULT does not fit u2");
    (void)VersionOf(f.Run("SET BLOCK 0 0 IN land depth = 100"));
    ExpectError(f.Run("ALTER TABLE land ALTER COLUMN depth TYPE i4"), "block (0, 0) holds 100");
    ExpectReply(f.Run("GET BLOCK 0 0 FROM land COLUMNS depth"), "*1\r\n:100\r\n");
    ExpectReply(f.Run("ALTER TABLE land ALTER COLUMN light TYPE u2 USING CLAMP"), "+OK\r\n");
    ExpectReply(f.Run("GET BLOCK 0 0 FROM land COLUMNS light"), "*1\r\n:3\r\n");
    ExpectReply(f.Run("ALTER TABLE land ALTER COLUMN id TYPE u9"), "+OK\r\n");
    ExpectError(f.Run("ALTER TABLE land ALTER COLUMN id TYPE text(4)"), "INVALID_ARGUMENT");

    ExpectReply(f.Run("ALTER TABLE land RENAME COLUMN sign TO label"), "+OK\r\n");
    ExpectReply(f.Run("GET BLOCK 0 0 FROM land COLUMNS label"), "*1\r\n$2\r\nhi\r\n");
    ExpectReply(f.Run("ALTER TABLE land DROP COLUMN label"), "+OK\r\n");
    ExpectError(f.Run("GET BLOCK 0 0 FROM land COLUMNS label"), "no column label");
    ExpectReply(f.Run("ALTER TABLE land SET checkpoint_updates = 64"), "+OK\r\n");
    assert(Contains(f.Run("DESCRIBE land"), "$18\r\ncheckpoint_updates\r\n:64\r\n"));
    assert(Contains(f.Run("DESCRIBE land"), "$7\r\nversion\r\n:7\r\n"));

    ExpectError(f.Run("CREATE TABLE land (a u8) CHUNK 4 x 4"), "TABLE_EXISTS");
    ExpectError(f.Run("CREATE TABLE t (a u8) CHUNK 4 x 4 WITH colour = 1"), "unknown table option 'colour'");
    ExpectError(f.Run("CREATE TABLE t (a u8) CHUNK 4 x 4 WITH checkpoint_updates = 1, checkpoint_updates = 2"), "given twice");
    ExpectError(f.Run("CREATE TABLE t (a u8) CHUNK 4 x 4 WITH checkpoint_updates = TRUE"), "takes a number or a quoted value");
    ExpectError(f.Run("CREATE TABLE t (a u8 DEFAULT NULL) CHUNK 4 x 4"), "cannot be NULL");
    ExpectError(f.Run("CREATE TABLE t (a u8 DEFAULT 300) CHUNK 4 x 4"), "INVALID_ARGUMENT");
    ExpectError(f.Run("CREATE TABLE t (a text(4)) CHUNK 4 x 4"), "INVALID_ARGUMENT");
    ExpectError(f.Run("ALTER TABLE nowhere DROP COLUMN a"), "NO_TABLE");
    ExpectError(f.Run("DESCRIBE nowhere"), "NO_TABLE");
    ExpectReply(f.Run("SHOW TABLES"), "*4\r\n$7\r\ndefault\r\n$4\r\nland\r\n$5\r\nplain\r\n$5\r\nworld\r\n");

    ExpectReply(f.Run("FLUSH WAL"), "+OK\r\n");
    assert(f.Run("SHOW METRICS").rfind("$", 0) == 0);
    ExpectReply(f.Run("DROP TABLE land"), "+OK\r\n");
    ExpectError(f.Run("GET BLOCK 0 0 FROM land"), "NO_TABLE table 'land' does not exist");
    // A table of the same name later is another table.
    ExpectReply(f.Run("CREATE TABLE land (b bool) CHUNK 2 x 2"), "+OK\r\n");
    ExpectReply(f.Run("GET BLOCK 0 0 FROM land"), "_\r\n");
    // Frames are bounded by the table as it is now, not as this connection
    // last used it.
    auto plan = f.engine->PlanPayload(f.session, "SET CHUNK 0 0 IN land $1\r\n");
    const std::size_t small = plan.parameter_limits.at(0);
    ExpectReply(f.Run("DROP TABLE land"), "+OK\r\n");
    ExpectReply(f.Run("CREATE TABLE land (b bool) CHUNK 8 x 8"), "+OK\r\n");
    plan = f.engine->PlanPayload(f.session, "SET CHUNK 0 0 IN land $1\r\n");
    // Presence and payload of a bool table: 1 + 1 bytes for 2 x 2 blocks,
    // 8 + 8 for 8 x 8.
    assert(plan.parameter_limits.at(0) == small + 14U);
}

// The chunk coordinates of a SCAN CHUNKS reply, and whether more follow.
std::pair<std::vector<std::pair<std::int64_t, std::int64_t>>, bool> ScanOf(const std::string& reply) {
    const std::string head = "%2\r\n$6\r\nchunks\r\n*";
    assert(reply.rfind(head, 0) == 0);
    std::size_t at = head.size();
    const auto line = [&reply, &at] {
        const std::size_t end = reply.find("\r\n", at);
        std::string text = reply.substr(at, end - at);
        at = end + 2;
        return text;
    };
    const std::size_t count = std::stoull(line());
    std::vector<std::pair<std::int64_t, std::int64_t>> chunks;
    for (std::size_t i = 0; i < count; ++i) {
        assert(line() == "*2");
        const std::int64_t x = std::stoll(line().substr(1));
        chunks.emplace_back(x, std::stoll(line().substr(1)));
    }
    assert(line() == "$4" && line() == "more");
    const std::string more = line();
    assert(more == "#t" || more == "#f");
    assert(at == reply.size());
    return {chunks, more == "#t"};
}

void TestScanChunks() {
    Fixture f;
    const auto [none, none_more] = ScanOf(f.Run("SCAN CHUNKS FROM world"));
    assert(none.empty() && !none_more);
    for (const auto& [x, y] : std::vector<std::pair<int, int>>{{0, 0}, {5, 0}, {-1, 9}}) {
        (void)VersionOf(f.Run("SET BLOCK " + std::to_string(x) + " " + std::to_string(y) + " IN world id = 1"));
    }
    // Chunks (0, 0), (1, 0) and (-1, 2); pages continue AFTER the last chunk
    // of the previous one and together list each chunk once.
    const auto [all, all_more] = ScanOf(f.Run("SCAN CHUNKS FROM world"));
    assert(all.size() == 3 && !all_more);
    auto sorted = all;
    std::sort(sorted.begin(), sorted.end());
    assert((sorted == std::vector<std::pair<std::int64_t, std::int64_t>>{{-1, 2}, {0, 0}, {1, 0}}));
    const auto [first, first_more] = ScanOf(f.Run("SCAN CHUNKS FROM world LIMIT 2"));
    assert(first.size() == 2 && first_more);
    assert(first[0] == all[0] && first[1] == all[1]);
    const auto [rest, rest_more] = ScanOf(f.Run(
        "SCAN CHUNKS FROM world AFTER " + std::to_string(first[1].first) + " " + std::to_string(first[1].second) +
        " LIMIT 2"));
    assert(rest.size() == 1 && rest[0] == all[2] && !rest_more);
    ExpectError(f.Run("SCAN CHUNKS FROM world LIMIT 0"), "LIMIT must be between 1 and 1024");
    ExpectError(f.Run("SCAN CHUNKS FROM world LIMIT 1025"), "LIMIT must be between 1 and 1024");
    ExpectError(f.Run("SCAN CHUNKS FROM nowhere"), "NO_TABLE");
}

void TestSlotStatements() {
    Fixture f;
    ExpectReply(f.Run("SHOW SLOTS"), "*0\r\n");
    ExpectReply(f.Run("CREATE SLOT 'consumer' ON world"), "+OK\r\n");
    ExpectReply(f.Run("CREATE SLOT 'other' ON plain"), "+OK\r\n");
    const auto world = f.catalog->Find("world")->ListFeedSlots().front();
    std::string expected = "*1\r\n%6\r\n$5\r\ntable\r\n$5\r\nworld\r\n$4\r\nname\r\n$8\r\nconsumer\r\n$5\r\nepoch\r\n$32\r\n";
    expected += chunkdb::StoreIdHex(world.position.epoch);
    expected += "\r\n$5\r\nacked\r\n:" + std::to_string(world.position.revision);
    expected += "\r\n$14\r\nretained_bytes\r\n:0\r\n$4\r\nlost\r\n#f\r\n";
    ExpectReply(f.Run("SHOW SLOTS ON world"), expected);
    assert(f.Run("SHOW SLOTS").starts_with("*2\r\n"));
    ExpectError(f.Run("CREATE SLOT 'consumer' ON world"), "INVALID_ARGUMENT");
    ExpectError(f.Run("DROP SLOT 'missing' ON world"), "INVALID_ARGUMENT");
    ExpectError(f.Run("ACK 0"), "INVALID_ARGUMENT no slot watch is open");
    ExpectReply(f.Run("BEGIN"), "+OK\r\n");
    ExpectError(f.Run("CREATE SLOT 'transaction' ON world"), "INVALID_ARGUMENT inside a transaction");
    ExpectReply(f.Run("ROLLBACK"), "+OK\r\n");
    ExpectReply(f.Run("DROP SLOT 'consumer' ON world"), "+OK\r\n");
    ExpectReply(f.Run("SHOW SLOTS ON world"), "*0\r\n");
    ExpectReply(f.Run("DROP TABLE plain"), "+OK\r\n");
    ExpectReply(f.Run("SHOW SLOTS"), "*0\r\n");
    ExpectError(f.Run("SHOW SLOTS ON absent"), "NO_TABLE");
}

void TestSlotListingDuringDrop() {
    for (const bool after_find : {false, true}) {
        Fixture f;
        ExpectReply(f.Run("CREATE SLOT 'consumer' ON world"), "+OK\r\n");
        struct DropHook final : chunkdb::CommandEngineTestHook {
            std::shared_ptr<chunkdb::TableCatalog> catalog;
            bool after_find = false, dropped = false;
            void Run(Point point, std::string_view table) override {
                if (dropped || (after_find ? point != Point::kBeforeSlotTableList || table != "world"
                                          : point != Point::kAfterSlotTablesListed)) return;
                // Complete DROP on another thread at the exact lookup gap.
                std::async(std::launch::async, [&] { catalog->Drop("world"); }).get();
                dropped = true;
            }
        } hook;
        hook.catalog = f.catalog;
        hook.after_find = after_find;
        f.engine->SetHookForTests(&hook);
        ExpectReply(f.Run("SHOW SLOTS"), "*0\r\n");
        assert(hook.dropped);
        f.engine->SetHookForTests(nullptr);
        ExpectError(f.Run("SHOW SLOTS ON world"), "NO_TABLE");
    }
    Fixture f;
    struct DropScopedHook final : chunkdb::CommandEngineTestHook {
        std::shared_ptr<chunkdb::TableCatalog> catalog;
        void Run(Point point, std::string_view table) override {
            if (point == Point::kBeforeSlotTableList && table == "world")
                std::async(std::launch::async, [&] { catalog->Drop("world"); }).get();
        }
    } hook;
    hook.catalog = f.catalog;
    f.engine->SetHookForTests(&hook);
    ExpectError(f.Run("SHOW SLOTS ON world"), "NO_TABLE");
    f.engine->SetHookForTests(nullptr);
}

}  // namespace

int main() {
    TestHello();
    TestLiteralsAndTypedReplies();
    TestParameters();
    TestErrors();
    TestBitStringTable();
    TestPlanParameters();
    TestChunkStatements();
    TestAreaStatements();
    TestAreaFromFiles();
    TestTableStatements();
    TestScanChunks();
    TestSlotStatements();
    TestSlotListingDuringDrop();
    return 0;
}
