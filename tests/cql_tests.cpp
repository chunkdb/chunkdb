// The CQL parser (#62, docs/CQL.md): every statement, literals,
// parameters, and the errors that name where a statement goes wrong.

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <string>
#include <variant>
#include <vector>

#include "cql.hpp"

namespace {

namespace cql = chunkdb::cql;
using chunkdb::ColumnKind;
using chunkdb::ColumnType;
using chunkdb::Conversion;

void ExpectError(const std::string& line, const std::string& part) {
    try {
        (void)cql::Parse(line);
    } catch (const cql::ParseError& e) {
        if (std::string(e.what()).find(part) == std::string::npos) {
            std::fprintf(stderr, "%s: expected '%s', got '%s'\n", line.c_str(), part.c_str(), e.what());
            assert(false);
        }
        return;
    }
    std::fprintf(stderr, "%s: expected an error with '%s'\n", line.c_str(), part.c_str());
    assert(false);
}

template <typename T>
T Get(const std::string& line, std::size_t parameters = 0) {
    const cql::Parsed parsed = cql::Parse(line);
    assert(parsed.parameters == parameters);
    assert(std::holds_alternative<T>(parsed.statement));
    return std::get<T>(parsed.statement);
}

cql::Literal ValueOf(const std::string& literal) {
    const auto set = Get<cql::SetBlock>("SET BLOCK 0 0 IN t a = " + literal);
    assert(set.values.size() == 1);
    return set.values[0].value;
}

void TestBlockStatements() {
    const auto get = Get<cql::GetBlock>("GET BLOCK -3 9223372036854775807 FROM world");
    assert(get.x == -3 && get.y == std::numeric_limits<std::int64_t>::max());
    assert(get.table == "world" && get.columns.empty());

    const auto columns = Get<cql::GetBlock>("get block 1 2 from t columns b, a");
    assert((columns.columns == std::vector<std::string>{"b", "a"}));

    const auto set = Get<cql::SetBlock>("SET BLOCK -9223372036854775808 0 IN t a = 5, b = $1, c = NULL IF VERSION 7", 1);
    assert(set.x == std::numeric_limits<std::int64_t>::min());
    assert(set.values.size() == 3);
    assert(set.values[0].column == "a" && set.values[0].value == cql::Literal(cql::Integer{.negative = false, .magnitude = 5}));
    assert(set.values[1].value == cql::Literal(cql::Parameter{.index = 1}));
    assert(std::holds_alternative<cql::Null>(set.values[2].value));
    assert(set.if_version == 7U);
    assert(!Get<cql::SetBlock>("SET BLOCK 0 0 IN t a = 1").if_version.has_value());

    const auto del = Get<cql::DeleteBlock>("DELETE BLOCK 4 5 FROM t IF VERSION 0");
    assert(del.x == 4 && del.y == 5 && del.table == "t" && del.if_version == 0U);
}

void TestChunkAndAreaStatements() {
    const auto get = Get<cql::GetChunk>("GET CHUNK -1 2 FROM t COLUMNS a");
    assert(get.chunk_x == -1 && get.chunk_y == 2 && get.columns == std::vector<std::string>{"a"});

    const auto set = Get<cql::SetChunk>("SET CHUNK 3 4 IN t $1 IF VERSION 12", 1);
    assert(set.chunk_x == 3 && set.state.index == 1 && set.if_version == 12U);
    ExpectError("SET CHUNK 3 4 IN t x'00'", "the chunk is sent as a parameter");

    const auto box = Get<cql::GetArea>("GET AREA -2 -2 TO 5 6 FROM t");
    assert(!box.around && box.x0 == -2 && box.y0 == -2 && box.x1 == 5 && box.y1 == 6);

    const auto around = Get<cql::GetArea>("GET AREA AROUND 10 -10 RADIUS 3 FROM t COLUMNS a, b");
    assert(around.around && around.x0 == 10 && around.y0 == -10 && around.radius == 3);
    assert(around.columns.size() == 2);
    ExpectError("GET AREA AROUND 0 0 RADIUS -1 FROM t", "a radius must be between 0");
}

void TestLiterals() {
    assert(ValueOf("-12") == cql::Literal(cql::Integer{.negative = true, .magnitude = 12}));
    assert(ValueOf("-0") == cql::Literal(cql::Integer{.negative = false, .magnitude = 0}));
    assert(ValueOf("18446744073709551615") ==
           cql::Literal(cql::Integer{.negative = false, .magnitude = std::numeric_limits<std::uint64_t>::max()}));
    ExpectError("SET BLOCK 0 0 IN t a = 18446744073709551616", "is not a 64-bit integer");

    assert(ValueOf("1.5") == cql::Literal(1.5));
    assert(ValueOf("2e-3") == cql::Literal(2e-3));
    assert(ValueOf("-4.25E+2") == cql::Literal(-425.0));
    assert(ValueOf("inf") == cql::Literal(std::numeric_limits<double>::infinity()));
    assert(ValueOf("-INF") == cql::Literal(-std::numeric_limits<double>::infinity()));
    assert(std::isnan(std::get<double>(ValueOf("NaN"))));
    ExpectError("SET BLOCK 0 0 IN t a = 1.2.3", "is not a number");
    ExpectError("SET BLOCK 0 0 IN t a = 1e999", "is not a number");
    ExpectError("SET BLOCK 0 0 IN t a = -abc", "expected a number after -");

    assert(ValueOf("TRUE") == cql::Literal(true));
    assert(ValueOf("false") == cql::Literal(false));
    assert(std::holds_alternative<cql::Null>(ValueOf("null")));

    assert(ValueOf("'it''s'") == cql::Literal(cql::Text{.value = "it's"}));
    assert(ValueOf("''") == cql::Literal(cql::Text{.value = ""}));
    assert(ValueOf("'a, b = c'") == cql::Literal(cql::Text{.value = "a, b = c"}));
    ExpectError("SET BLOCK 0 0 IN t a = 'open", "no closing quote");

    assert(ValueOf("x'0a0BfF'") == cql::Literal(cql::Bytes{.value = {0x0a, 0x0b, 0xff}}));
    assert(ValueOf("X''") == cql::Literal(cql::Bytes{}));
    ExpectError("SET BLOCK 0 0 IN t a = x'abc'", "pairs of hexadecimal digits");
    ExpectError("SET BLOCK 0 0 IN t a = x'zz'", "pairs of hexadecimal digits");

    assert(ValueOf("b'1010'") == cql::Literal(cql::Bits{.digits = "1010"}));
    ExpectError("SET BLOCK 0 0 IN t a = b''", "the digits 0 and 1");
    ExpectError("SET BLOCK 0 0 IN t a = b'102'", "the digits 0 and 1");
}

void TestParameters() {
    const auto set = Get<cql::SetBlock>("SET BLOCK 0 0 IN t a = $2, b = $1", 2);
    assert(set.values[0].value == cql::Literal(cql::Parameter{.index = 2}));
    ExpectError("SET BLOCK 0 0 IN t a = $1, b = $3", "numbered $1 to $3 without gaps");
    ExpectError("SET BLOCK 0 0 IN t a = $2", "numbered $1 to $2 without gaps");
    ExpectError("SET BLOCK 0 0 IN t a = $1, b = $1", "$1 is used twice");
    ExpectError("SET BLOCK 0 0 IN t a = $0", "numbered from $1");
    ExpectError("SET BLOCK 0 0 IN t a = $65536", "numbered from $1");
    ExpectError("SET BLOCK 0 0 IN t a = $", "followed by its number");
    ExpectError("CREATE TABLE t (a u8 DEFAULT $1) CHUNK 4 x 4", "parameters are values of SET BLOCK");
    ExpectError("ALTER TABLE t SET wal = $1", "parameters are values of SET BLOCK");
}

void TestCreateTable() {
    const auto create = Get<cql::CreateTable>(
        "CREATE TABLE world (a u10 REQUIRED, b u4 DEFAULT 15, c text(256) NULL, d i64 NULL DEFAULT -1 REQUIRED, "
        "e bool, f f32, g F64, h bits(9), i bytes(16)) CHUNK 16 x 32 LARGE 8 x 8 WITH zrle = true, cache_chunks = 100");
    assert(create.table == "world" && create.columns.size() == 9);
    assert(create.columns[0].name == "a" && create.columns[0].type == (ColumnType{.kind = ColumnKind::kUnsigned, .size = 10}));
    assert(create.columns[0].required && !create.columns[0].nullable && !create.columns[0].default_value.has_value());
    assert(create.columns[1].default_value == cql::Literal(cql::Integer{.negative = false, .magnitude = 15}));
    assert(create.columns[2].type == (ColumnType{.kind = ColumnKind::kText, .size = 256}) && create.columns[2].nullable);
    assert(create.columns[3].type == (ColumnType{.kind = ColumnKind::kSigned, .size = 64}));
    assert(create.columns[3].nullable && create.columns[3].required);
    assert(create.columns[3].default_value == cql::Literal(cql::Integer{.negative = true, .magnitude = 1}));
    assert(create.columns[4].type == (ColumnType{.kind = ColumnKind::kBool, .size = 1}));
    assert(create.columns[5].type == (ColumnType{.kind = ColumnKind::kFloat32, .size = 32}));
    assert(create.columns[6].type == (ColumnType{.kind = ColumnKind::kFloat64, .size = 64}));
    assert(create.columns[7].type == (ColumnType{.kind = ColumnKind::kBits, .size = 9}));
    assert(create.columns[8].type == (ColumnType{.kind = ColumnKind::kBytes, .size = 16}));
    assert(create.chunk_width == 16 && create.chunk_height == 32);
    assert(create.large.has_value() && create.large->first == 8 && create.large->second == 8);
    assert(create.options.size() == 2);
    assert(create.options[0].name == "zrle" && create.options[0].value == cql::Literal(true));
    assert(create.options[1].name == "cache_chunks");

    const auto compact = Get<cql::CreateTable>("create table t (a u8) chunk 16x16");
    assert(compact.chunk_width == 16 && compact.chunk_height == 16 && !compact.large.has_value());
    assert(compact.options.empty());

    ExpectError("CREATE TABLE t (a u8 NULL NULL) CHUNK 4 x 4", "NULL is given twice");
    ExpectError("CREATE TABLE t (a u8 DEFAULT 1 DEFAULT 2) CHUNK 4 x 4", "DEFAULT is given twice");
    ExpectError("CREATE TABLE t (a u8 REQUIRED REQUIRED) CHUNK 4 x 4", "REQUIRED is given twice");
    ExpectError("CREATE TABLE t (a int) CHUNK 4 x 4", "int is not a column type");
    ExpectError("CREATE TABLE t (a text) CHUNK 4 x 4", "expected (");
    ExpectError("CREATE TABLE t (a u8) CHUNK 4 by 4", "expected X, got 'by'");
    ExpectError("CREATE TABLE t (a u8)", "expected CHUNK, got the end of the statement");
    ExpectError("CREATE TABLE t () CHUNK 4 x 4", "expected a column name, got ')'");
    ExpectError("CREATE TABLE T (a u8) CHUNK 4 x 4", "names are lowercase: 'T'");
}

void TestAlterAndOtherStatements() {
    const auto add = Get<cql::AlterTable>("ALTER TABLE t ADD COLUMN n u8 NULL");
    const auto& column = std::get<cql::AddColumn>(add.change).column;
    assert(add.table == "t" && column.name == "n" && column.nullable);

    assert(std::get<cql::DropColumn>(Get<cql::AlterTable>("ALTER TABLE t DROP COLUMN a").change).column == "a");

    const auto rename = std::get<cql::RenameColumn>(Get<cql::AlterTable>("ALTER TABLE t RENAME COLUMN a TO b").change);
    assert(rename.column == "a" && rename.new_name == "b");

    const auto widen = std::get<cql::AlterColumnType>(Get<cql::AlterTable>("ALTER TABLE t ALTER COLUMN a TYPE u16").change);
    assert(widen.column == "a" && widen.type == (ColumnType{.kind = ColumnKind::kUnsigned, .size = 16}));
    assert(!widen.conversion.has_value());
    const auto clamp = std::get<cql::AlterColumnType>(Get<cql::AlterTable>("ALTER TABLE t ALTER COLUMN a TYPE u4 USING CLAMP").change);
    assert(clamp.conversion == Conversion::kClamp);
    const auto def = std::get<cql::AlterColumnType>(Get<cql::AlterTable>("ALTER TABLE t ALTER COLUMN a TYPE u4 USING default").change);
    assert(def.conversion == Conversion::kDefault);
    const auto cut = std::get<cql::AlterColumnType>(Get<cql::AlterTable>("ALTER TABLE t ALTER COLUMN s TYPE text(4) USING TRUNCATE").change);
    assert(cut.conversion == Conversion::kTruncate);
    ExpectError("ALTER TABLE t ALTER COLUMN a TYPE u4 USING ROUND", "expected CLAMP, DEFAULT or TRUNCATE");

    const auto option = std::get<cql::SetOption>(Get<cql::AlterTable>("ALTER TABLE t SET cache_chunks = 64").change).option;
    assert(option.name == "cache_chunks" && option.value == cql::Literal(cql::Integer{.negative = false, .magnitude = 64}));
    ExpectError("ALTER TABLE t TRUNCATE", "expected ADD, DROP, RENAME, ALTER or SET");

    assert(Get<cql::DropTable>("DROP TABLE old").table == "old");
    (void)Get<cql::ShowTables>("SHOW TABLES");
    (void)Get<cql::ShowMetrics>("show metrics");
    assert(Get<cql::Describe>("DESCRIBE world").table == "world");
    (void)Get<cql::FlushWal>("  FLUSH\tWAL  ");
    (void)Get<cql::Ping>("ping");
    (void)Get<cql::Begin>("BEGIN");
    (void)Get<cql::Commit>("commit");
    (void)Get<cql::Rollback>("ROLLBACK");
    const auto create = Get<cql::CreateUser>("CREATE USER bot VERIFIER $1 MANAGES USERS", 1);
    assert(create.user == "bot" && create.manages_users && create.verifier == cql::Literal(cql::Parameter{.index = 1}));
    const auto quoted = Get<cql::CreateUser>("create user bot verifier 'SCRAM-SHA-256$4096:a$b:c'");
    assert(!quoted.manages_users && quoted.verifier == cql::Literal(cql::Text{.value = "SCRAM-SHA-256$4096:a$b:c"}));
    ExpectError("CREATE USER bot PASSWORD 'pencil'", "expected VERIFIER");
    ExpectError("CREATE USER bot VERIFIER 12", "never a password");
    const auto rekey = Get<cql::AlterUser>("ALTER USER bot VERIFIER $1", 1);
    assert(rekey.verifier.has_value() && !rekey.manages_users.has_value());
    assert(Get<cql::AlterUser>("ALTER USER bot NO MANAGES USERS").manages_users == false);
    assert(Get<cql::AlterUser>("ALTER USER bot MANAGES USERS").manages_users == true);
    assert(Get<cql::DropUser>("DROP USER bot").user == "bot");
    const auto grant = Get<cql::GrantRight>("GRANT WRITE ON world TO bot");
    assert(!grant.revoke && grant.right == chunkdb::Right::kWrite && grant.table == "world" && grant.user == "bot");
    const auto every = Get<cql::GrantRight>("grant admin on * to bot");
    assert(every.right == chunkdb::Right::kAdmin && every.table == "*");
    const auto revoke = Get<cql::GrantRight>("REVOKE READ ON * FROM bot");
    assert(revoke.revoke && revoke.right == chunkdb::Right::kRead && revoke.user == "bot");
    ExpectError("GRANT DELETE ON world TO bot", "expected READ, WRITE or ADMIN");
    ExpectError("REVOKE READ ON world TO bot", "expected FROM");
    (void)Get<cql::ShowUsers>("SHOW USERS");
    const auto scan = Get<cql::ScanChunks>("SCAN CHUNKS FROM world AFTER -3 4 LIMIT 10");
    assert(scan.table == "world" && scan.after == std::make_pair(std::int64_t{-3}, std::int64_t{4}) && scan.limit == 10U);
    const auto whole = Get<cql::ScanChunks>("scan chunks from world");
    assert(!whole.after.has_value() && !whole.limit.has_value());
    ExpectError("SCAN BLOCKS FROM world", "expected CHUNKS");
    ExpectError("SCAN CHUNKS FROM world AFTER 1", "expected a chunk y");
}

void TestWatch() {
    const auto watch = Get<cql::Watch>("WATCH world AREA -2 3 TO 4 5 AFTER 1234567890abcdefABCDEF0123456789 18446744073709551615");
    assert(watch.table == "world" && watch.area->first.x == -2 && watch.area->last.y == 5);
    assert(watch.after->epoch[0] == 0x12 && watch.after->epoch[15] == 0x89);
    assert(watch.after->revision == UINT64_MAX);
    assert(Get<cql::Watch>("watch world after 00000000000000000000000000000000 0").after->revision == 0);
    assert(!Get<cql::Watch>("WATCH world").after);
    (void)Get<cql::Unwatch>("UNWATCH");
    ExpectError("WATCH world AREA 2 0 TO 1 0", "bounds are reversed");
    ExpectError("WATCH world AFTER abc 0", "32 hex digits");
    ExpectError("WATCH world AFTER z0000000000000000000000000000000 0", "32 hex digits");
    ExpectError("WATCH world AFTER 00000000000000000000000000000000 -1", "revision must be between 0");
    const auto slot = Get<cql::Watch>("WATCH world SLOT 'consumer_1' AREA 0 0 TO 1 1 AFTER 00000000000000000000000000000000 4");
    assert(slot.slot == "consumer_1" && slot.after->revision == 4 && slot.area->last.x == 1);
    assert(Get<cql::Ack>("ACK 18446744073709551615").revision == UINT64_MAX);
    ExpectError("ACK -1", "revision must be between 0");
    ExpectError("ACK 18446744073709551616", "not a 64-bit integer");
    ExpectError("ACK 1 extra", "expected the end");
    const auto create = Get<cql::CreateSlot>("CREATE SLOT '_consumer1' ON world");
    assert(create.name == "_consumer1" && create.table == "world");
    const auto drop = Get<cql::DropSlot>("drop slot 'consumer' on world");
    assert(drop.name == "consumer" && drop.table == "world");
    assert(!Get<cql::ShowSlots>("SHOW SLOTS").table);
    assert(Get<cql::ShowSlots>("SHOW SLOTS ON world").table == "world");
    for (const auto& name : std::vector<std::string>{"", "Upper", "1start", "hy-phen", "quote'", std::string(64, 'a')}) {
        std::string quoted;
        for (const char c : name) { quoted += c; if (c == '\'') quoted += c; }
        for (const auto& prefix : {"CREATE SLOT ", "DROP SLOT ", "WATCH world SLOT "}) {
            const std::string suffix = std::string(prefix).starts_with("WATCH") ? "" : " ON world";
            ExpectError(std::string(prefix) + "'" + quoted + "'" + suffix, "slot names must match");
        }
    }
    assert(Get<cql::CreateSlot>("CREATE SLOT '" + std::string(63, 'a') + "' ON world").name.size() == 63);
    ExpectError("CREATE SLOT consumer ON world", "quoted slot name");
    ExpectError("DROP SLOT $1 ON world", "quoted slot name");
    ExpectError("SHOW SLOTS ON world extra", "expected the end");
    ExpectError("UNWATCH extra", "expected the end");
}
void TestErrors() {
    ExpectError("", "column 1: unknown statement the end of the statement");
    ExpectError("SELECT * FROM t", "column 1: unknown statement 'SELECT'");
    ExpectError("GET ROW 1 2 FROM t", "column 5: expected BLOCK, CHUNK or AREA, got 'ROW'");
    ExpectError("GET BLOCK 1 FROM t", "column 13: expected y, got 'FROM'");
    ExpectError("GET BLOCK 1 2 FROM t extra", "column 22: expected the end of the statement, got 'extra'");
    ExpectError("GET BLOCK 1 2 FROM t COLUMNS", "expected a column name, got the end of the statement");
    ExpectError("GET BLOCK 1 2 FROM t COLUMNS a,", "expected a column name");
    ExpectError("GET BLOCK 9223372036854775808 0 FROM t", "x is out of range");
    ExpectError("GET BLOCK 1 2 FROM t; DROP TABLE t", "column 21: unexpected ';'");
    ExpectError("SET BLOCK 0 0 IN t", "expected a column name");
    ExpectError("SET BLOCK 0 0 IN t a 1", "expected =, got '1'");
    ExpectError("SET BLOCK 0 0 IN t a = b", "expected a value, got 'b'");
    ExpectError("SET BLOCK 0 0 IN t a = 1 IF VERSION -1", "a version must be between 0");
    ExpectError("SET BLOCK 0 0 IN t a = 1 IF 3", "expected VERSION");
    ExpectError("DROP t", "expected TABLE");
    ExpectError("FLUSH", "expected WAL");
    ExpectError("SHOW GRANTS", "expected TABLES, METRICS, USERS or SLOTS");
}

}  // namespace

int main() {
    TestBlockStatements();
    TestChunkAndAreaStatements();
    TestLiterals();
    TestParameters();
    TestCreateTable();
    TestAlterAndOtherStatements();
    TestWatch();
    TestErrors();
    return 0;
}
