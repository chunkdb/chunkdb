// The CQL parser (#62, docs/CQL.md): every statement, literals,
// parameters, and the errors that name where a statement goes wrong.

#include <algorithm>
#include <cassert>
#include <clocale>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <string>
#include <variant>
#include <vector>

#include "cql.hpp"
#include "expr.hpp"

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
void TestStatementSuggestions() {
    ExpectError("CRETE TABLE t (n u8) CHUNK 4 x 4", "unknown statement 'CRETE'; did you mean CREATE?");
    ExpectError("DESCRBE t", "did you mean DESCRIBE?");
    ExpectError("MIGRTE 'm' DROP TABLE t", "did you mean MIGRATE?");
    ExpectError("RECOVR TABLE t", "did you mean RECOVER?");
    ExpectError("SCA CHUNKS FROM t", "did you mean SCAN?");
    for (const auto* word : {"xxxxxxxx", "et", "verylongunknownstatement"}) {
        try { (void)cql::Parse(word); assert(false); }
        catch (const cql::ParseError& error) { assert(std::string(error.what()).find("did you mean") == std::string::npos); }
    }
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

void TestChunkModelCoordinates() {
    const auto read = Get<cql::GetBlock>("GET BLOCK -1 2 -3 FROM t COLUMNS a WITH VERSION WITH SCHEMA");
    assert(read.dims == 3U && read.x == -1 && read.y == 2 && read.z == -3 && read.with_version && read.with_schema);
    const auto write = Get<cql::SetBlock>("SET BLOCK 1 2 3 IN t a=$1 IF VERSION 8", 1);
    assert(write.dims == 3U && write.z == 3 && write.if_version == 8U);
    const auto remove = Get<cql::DeleteBlock>("DELETE BLOCK 1 2 -9223372036854775808 FROM t");
    assert(remove.dims == 3U && remove.z == std::numeric_limits<std::int64_t>::min());
    const auto chunk = Get<cql::GetChunk>("GET CHUNK 1 2 0 FROM t");
    assert(chunk.dims == 3U && chunk.chunk_z == 0);
    assert(Get<cql::GetChunk>("GET CHUNK 1 2 FROM t").dims == 2U);
    const auto box = Get<cql::GetArea>("GET AREA -1 -2 -3 TO 4 5 6 FROM t WITH SCHEMA");
    assert(box.dims == 3U && box.z0 == -3 && box.z1 == 6 && box.with_schema);
    const auto ball = Get<cql::GetArea>("GET AREA AROUND 1 2 3 RADIUS 4 FROM t");
    assert(ball.dims == 3U && ball.around && ball.z0 == 3 && ball.radius == 4);
    const auto watch = Get<cql::Watch>("WATCH t AREA -1 -2 -3 TO 4 5 6");
    assert(watch.areas.size() == 1U && watch.areas[0].dims == 3U && watch.areas[0].first.z == -3 && watch.areas[0].last.z == 6);
    const auto slot = Get<cql::CreateSlot>("CREATE SLOT 's' ON t AREA 1 2 0 TO 3 4 0");
    assert(slot.area && slot.area->dims == 3U && slot.area->first.z == 0);
    ExpectError("GET BLOCK 1 2 9223372036854775808 FROM t", "z is out of range");
    ExpectError("GET CHUNK 1 2 3 4 FROM t", "expected FROM");
    ExpectError("GET AREA 1 2 TO 3 4 5 FROM t", "different dimensions");
    ExpectError("GET AREA 1 2 3 TO 4 5 FROM t", "different dimensions");
    ExpectError("WATCH t AREA 1 2 3 TO 4 5", "different dimensions");
    ExpectError("CREATE SLOT 's' ON t AREA 1 2 4 TO 3 4 3", "bounds are reversed");
}

void TestChunkModelSchemaAndWrites() {
    for (const auto* geometry : {"2 x 3 x 4", "2x3x4", "2x3 x4", "2 X 3X4"}) {
        const auto create = Get<cql::CreateTable>(std::string("CREATE TABLE t (v u8) CHUNK ") + geometry +
            " CHUNK COLUMNS (title text(8) NULL DEFAULT 'base') LARGE 5 x 6 x 7");
        assert(create.dims == 3U && create.chunk_width == 2 && create.chunk_height == 3 && create.chunk_depth == 4);
        assert(create.large && create.large->first == 5 && create.large->second == 6 && create.large_depth == 7);
        assert(create.chunk_columns.size() == 1U && create.chunk_columns[0].name == "title" && create.chunk_columns[0].nullable);
    }
    const auto reordered = Get<cql::CreateTable>("CREATE TABLE t (v u8) LARGE 5x6x7 CHUNK COLUMNS (v u10) CHUNK 2x3x4");
    assert(reordered.dims == 3U && reordered.chunk_columns[0].name == "v");
    const auto add = std::get<cql::AddColumn>(Get<cql::AlterTable>("ALTER TABLE t ADD CHUNK COLUMN IF NOT EXISTS a u8").change);
    const auto drop = std::get<cql::DropColumn>(Get<cql::AlterTable>("ALTER TABLE t DROP CHUNK COLUMN IF EXISTS a").change);
    const auto rename = std::get<cql::RenameColumn>(Get<cql::AlterTable>("ALTER TABLE t RENAME CHUNK COLUMN a TO b").change);
    assert(add.chunk_column && add.if_not_exists && drop.chunk_column && drop.if_exists && rename.chunk_column && rename.new_name == "b");
    const auto meta = Get<cql::SetChunk>("SET CHUNK 1 2 3 IN t META title=$1,v=8 IF ABSENT", 1);
    assert(meta.dims == 3U && meta.chunk_z == 3 && meta.metadata && meta.metadata->size() == 2U);
    assert(meta.state.index == 0U && meta.if_absent && !meta.if_version && meta.columns.empty());
    const auto projected = Get<cql::SetChunk>("SET CHUNK 1 2 IN t COLUMNS b,a $1 IF VERSION 18446744073709551615", 1);
    assert(projected.dims == 2U && projected.columns == (std::vector<std::string>{"b", "a"}));
    assert(projected.state.index == 1U && !projected.metadata && projected.if_version == std::numeric_limits<std::uint64_t>::max());
    assert(Get<cql::SetChunk>("SET CHUNK 1 2 IN t $1 IF ABSENT", 1).if_absent);
    const auto migration = Get<cql::Migrate>("MIGRATE 'm' ALTER TABLE t ADD CHUNK COLUMN a u8");
    assert(std::get<cql::AddColumn>(std::get<cql::AlterTable>(migration.statement).change).chunk_column);
    ExpectError("CREATE TABLE t (v u8) CHUNK 2x3x4 LARGE 5x6", "different dimensions");
    ExpectError("CREATE TABLE t (v u8) LARGE 5x6x7", "different dimensions");
    ExpectError("CREATE TABLE t (v u8) CHUNK 2x3x4x5", "more than three dimensions");
    ExpectError("CREATE TABLE t (v u8) CHUNK 2x3x4294967296", "out of range");
    ExpectError("CREATE TABLE t (v u8) CHUNK COLUMNS (a u8) CHUNK COLUMNS (b u8)", "given twice");
    ExpectError("SET CHUNK 1 2 IN t META a=$1 $2", "expected the end");
    ExpectError("SET CHUNK 1 2 IN t COLUMNS a META a=$1", "expected a value");
    ExpectError("SET CHUNK 1 2 IN t $1 IF ABSENT IF VERSION 1", "expected the end");
    ExpectError("SET CHUNK 1 2 IN t META a=$1 IF VERSION 1 IF ABSENT", "expected the end");
}

void TestChunkModelScan() {
    const auto scan = Get<cql::ScanChunks>("SCAN CHUNKS FROM t AFTER -1 2 -3 SINCE 18446744073709551615 LIMIT 9 WITH DATA COLUMNS (b,a)");
    assert(scan.after && scan.after->first == -1 && scan.after->second == 2 && scan.after_z == -3 && scan.dims == 3U);
    assert(scan.since == std::numeric_limits<std::uint64_t>::max() && scan.limit == 9U && scan.with_data);
    assert(scan.columns == (std::vector<std::string>{"b", "a"}));
    assert(Get<cql::ScanChunks>("SCAN CHUNKS FROM t SINCE 0").since == 0U);
    ExpectError("SCAN CHUNKS FROM t SINCE 0 SINCE 1", "given twice");
    ExpectError("SCAN CHUNKS FROM t WITH DATA WITH DATA", "given twice");
    ExpectError("SCAN CHUNKS FROM t COLUMNS a", "requires WITH DATA");
    ExpectError("SCAN CHUNKS FROM t SINCE -1", "a revision must be between 0");
    // WHERE ends before the next clause; its parameters are the statement's.
    const auto where = Get<cql::ScanChunks>("SCAN CHUNKS FROM t WHERE n = $1 AND chunk.k IS NULL LIMIT 3 SINCE 0 WITH DATA", 1);
    assert(where.where && where.where->nodes[where.where->root].op == chunkdb::expr::Op::kAnd);
    assert(where.limit == 3U && where.since == 0U && where.with_data);
    assert(!Get<cql::ScanChunks>("SCAN CHUNKS FROM t").where);
    assert(Get<cql::ScanChunks>("SCAN CHUNKS FROM t AFTER 1 2 WHERE pos_x < 4").after_z == 0);
    ExpectError("SCAN CHUNKS FROM t SINCE 0 WHERE n = 1", "WHERE with SINCE requires WITH DATA");
    ExpectError("SCAN CHUNKS FROM t WHERE n = 1 WHERE n = 2", "column 32: WHERE is given twice");
    ExpectError("SCAN CHUNKS FROM t WHERE", "column 25: expected a value, got the end of the statement");
    ExpectError("SCAN CHUNKS FROM t WHERE n = $2", "parameters must be numbered $1 to $2 without gaps");
    ExpectError("SCAN CHUNKS FROM t WHERE n = 1 n", "expected the end of the statement, got 'n'");
    const auto compact = Get<cql::CompactTombstones>("COMPACT TOMBSTONES t BEFORE 18446744073709551615");
    assert(compact.table == "t" && compact.before == std::numeric_limits<std::uint64_t>::max());
    assert(Get<cql::CompactTombstones>("COMPACT TOMBSTONES t BEFORE 0").before == 0U);
    assert(Get<cql::ShowTableStats>("SHOW TABLE STATS t").table == "t");
    ExpectError("COMPACT TOMBSTONES t BEFORE -1", "a revision must be between 0");
    ExpectError("MIGRATE 'compact' COMPACT TOMBSTONES t BEFORE 1", "one schema statement");
}

void TestConditionalObjects() {
    assert(Get<cql::CreateTable>("CREATE TABLE IF NOT EXISTS t (a u8) CHUNK 4 x 4").if_not_exists);
    assert(Get<cql::DropTable>("DROP TABLE IF EXISTS t").if_exists);
    const auto add = Get<cql::AlterTable>("ALTER TABLE t ADD COLUMN IF NOT EXISTS a u8 DEFAULT 0");
    assert(std::get<cql::AddColumn>(add.change).if_not_exists);
    const auto drop = Get<cql::AlterTable>("ALTER TABLE t DROP COLUMN IF EXISTS a");
    assert(std::get<cql::DropColumn>(drop.change).if_exists);
    assert(Get<cql::CreateSlot>("CREATE SLOT IF NOT EXISTS 's' ON t").if_not_exists);
    assert(Get<cql::DropSlot>("DROP SLOT IF EXISTS 's' ON t").if_exists);
    assert(Get<cql::CreateUser>("CREATE USER IF NOT EXISTS u VERIFIER $1", 1).if_not_exists);
    assert(Get<cql::DropUser>("DROP USER IF EXISTS u").if_exists);
    assert(!Get<cql::CreateTable>("CREATE TABLE t (a u8) CHUNK 4 x 4").if_not_exists);
    assert(!Get<cql::DropTable>("DROP TABLE t").if_exists);
    assert(!Get<cql::CreateUser>("CREATE USER u VERIFIER $1", 1).if_not_exists);
    assert(!Get<cql::DropUser>("DROP USER u").if_exists);
    assert(!Get<cql::CreateSlot>("CREATE SLOT 's' ON t").if_not_exists);
    assert(!Get<cql::DropSlot>("DROP SLOT 's' ON t").if_exists);
    assert(!std::get<cql::AddColumn>(Get<cql::AlterTable>("ALTER TABLE t ADD COLUMN a u8").change).if_not_exists);
    assert(!std::get<cql::DropColumn>(Get<cql::AlterTable>("ALTER TABLE t DROP COLUMN a").change).if_exists);
    for (const auto* text : {
             "CREATE TABLE IF UNKNOWN t (a u8) CHUNK 4 x 4",
             "CREATE SLOT IF UNKNOWN 's' ON t", "CREATE USER IF UNKNOWN u VERIFIER $1",
             "DROP TABLE IF UNKNOWN t", "DROP SLOT IF UNKNOWN 's' ON t", "DROP USER IF UNKNOWN u",
             "ALTER TABLE t ADD COLUMN IF UNKNOWN a u8", "ALTER TABLE t DROP COLUMN IF UNKNOWN a",
             "CREATE TABLE t IF NOT EXISTS (a u8) CHUNK 4 x 4", "DROP TABLE t IF EXISTS",
             "CREATE SLOT 's' IF NOT EXISTS ON t", "DROP SLOT 's' IF EXISTS ON t",
             "CREATE USER u IF NOT EXISTS VERIFIER $1", "DROP USER u IF EXISTS",
             "ALTER TABLE t ADD IF NOT EXISTS COLUMN a u8", "ALTER TABLE t DROP IF EXISTS COLUMN a"}) {
        try { (void)cql::Parse(text); assert(false); }
        catch (const cql::ParseError&) {}
    }
    ExpectError("CREATE TABLE IF NOT UNKNOWN t (a u8) CHUNK 4 x 4", "expected EXISTS");
    ExpectError("DROP TABLE IF NOT EXISTS t", "expected EXISTS");
    const auto migration = Get<cql::Migrate>("MIGRATE 'init' CREATE TABLE IF NOT EXISTS t (a u8) CHUNK 4 x 4");
    assert(std::get<cql::CreateTable>(migration.statement).if_not_exists);
}

void TestDefaultChunkGeometry() {
    const auto create = Get<cql::CreateTable>("CREATE TABLE t (a u8)");
    assert(create.chunk_width == 16 && create.chunk_height == 16);
    const auto migration = Get<cql::Migrate>("MIGRATE 'initial' CREATE TABLE t (a u8)");
    assert(std::get<cql::CreateTable>(migration.statement).chunk_width == 16);
}

void TestRightsLists() {
    // Rights are hierarchical: one grant of the highest requested right, or
    // revoke of the lowest, represents the entire list in one publication.
    for (const auto* list : {"READ, WRITE", "WRITE, READ", "READ, WRITE, READ"}) {
        assert(Get<cql::GrantRight>(std::string("GRANT ") + list + " ON t TO bot").right == chunkdb::Right::kWrite);
        assert(Get<cql::GrantRight>(std::string("REVOKE ") + list + " ON t FROM bot").right == chunkdb::Right::kRead);
    }
    assert(Get<cql::GrantRight>("GRANT READ, ADMIN, WRITE ON * TO bot").right == chunkdb::Right::kAdmin);
    assert(Get<cql::GrantRight>("REVOKE ADMIN, WRITE ON * FROM bot").right == chunkdb::Right::kWrite);
    ExpectError("GRANT READ, DELETE ON t TO bot", "expected READ, WRITE, ADMIN, MONITOR, BACKUP or REPLICATION");
    ExpectError("GRANT READ, ON t TO bot", "expected READ, WRITE, ADMIN, MONITOR, BACKUP or REPLICATION");
    const auto migration = Get<cql::Migrate>("MIGRATE 'grant' GRANT WRITE, READ ON t TO bot");
    assert(std::get<cql::GrantRight>(migration.statement).right == chunkdb::Right::kWrite);
}

void TestUnorderedOptions() {
    std::vector<std::string> options{"AFTER 0123456789abcdef0123456789abcdef 42", "AREA -1 0 TO 1 2", "SLOT 'consumer'"};
    do {
        const auto watch = Get<cql::Watch>("WATCH t " + options[0] + " " + options[1] + " " + options[2]);
        assert(watch.slot == "consumer" && watch.areas.at(0).first.x == -1 && watch.areas.at(0).last.y == 2);
        assert(watch.after->revision == 42 && watch.after->epoch[0] == 1);
    } while (std::next_permutation(options.begin(), options.end()));
    for (const auto* option : {"AREA 0 0 TO 1 1", "SLOT 'consumer'", "AFTER 00000000000000000000000000000000 0"})
        ExpectError(std::string("WATCH t ") + option + " " + option, "given twice");
    const auto scan = Get<cql::ScanChunks>("SCAN CHUNKS FROM t LIMIT 5 AFTER 1 2");
    assert(scan.limit == 5 && scan.after->first == 1 && scan.after->second == 2);
    ExpectError("SCAN CHUNKS FROM t LIMIT 5 LIMIT 6", "given twice");
    ExpectError("SCAN CHUNKS FROM t AFTER 1 2 AFTER 3 4", "given twice");
    options = {"CHUNK 4 x 8", "LARGE 2 x 3", "WITH checkpoint_updates = 64"};
    do {
        const auto create = Get<cql::CreateTable>("CREATE TABLE t (a u8) " + options[0] + " " + options[1] + " " + options[2]);
        assert(create.chunk_width == 4 && create.chunk_height == 8 && create.large->first == 2 && create.large->second == 3);
        assert(create.options.size() == 1 && create.options.front().name == "checkpoint_updates");
    } while (std::next_permutation(options.begin(), options.end()));
    for (const auto* option : {"CHUNK 4 x 4", "LARGE 2 x 2", "WITH checkpoint_updates = 64"})
        ExpectError(std::string("CREATE TABLE t (a u8) ") + option + " " + option, "given twice");
}

void TestBlockStatements() {
    const auto get = Get<cql::GetBlock>("GET BLOCK -3 9223372036854775807 FROM world");
    assert(get.x == -3 && get.y == std::numeric_limits<std::int64_t>::max());
    assert(get.table == "world" && get.columns.empty());

    const auto columns = Get<cql::GetBlock>("get block 1 2 from t columns b, a");
    assert((columns.columns == std::vector<std::string>{"b", "a"}));

    for (const auto* clauses : {"WITH VERSION WITH SCHEMA", "WITH SCHEMA WITH VERSION"}) {
        const auto metadata = Get<cql::GetBlock>(std::string("GET BLOCK 0 0 FROM t COLUMNS b, a ") + clauses);
        assert(metadata.with_version && metadata.with_schema);
        assert((metadata.columns == std::vector<std::string>{"b", "a"}));
    }
    assert(Get<cql::GetBlock>("GET BLOCK 0 0 FROM t WITH VERSION").with_version);
    assert(Get<cql::GetBlock>("GET BLOCK 0 0 FROM t WITH SCHEMA").with_schema);
    ExpectError("GET BLOCK 0 0 FROM t WITH VERSION WITH VERSION", "given twice");
    ExpectError("GET BLOCK 0 0 FROM t WITH SCHEMA WITH SCHEMA", "given twice");

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
    assert(Get<cql::GetArea>("GET AREA 0 0 TO 2 2 FROM t WITH SCHEMA").with_schema);
    assert(Get<cql::GetArea>("GET AREA AROUND 0 0 RADIUS 1 FROM t COLUMNS a WITH SCHEMA").with_schema);
    ExpectError("GET AREA 0 0 TO 2 2 FROM t WITH SCHEMA WITH SCHEMA", "WITH SCHEMA is given twice");
    ExpectError("GET AREA 0 0 TO 2 2 FROM t WITH VERSION", "expected SCHEMA");
    ExpectError("GET AREA AROUND 0 0 RADIUS -1 FROM t", "a radius must be between 0");

    const auto page = Get<cql::GetArea>("GET AREA -2 -2 TO 5 6 FROM after COLUMNS limit, after LIMIT 7 AFTER -1 3 WITH SCHEMA");
    assert(page.limit == 7U && page.after == (chunkdb::ChunkCoord{-1, 3}) && page.with_schema);
    assert(page.table == "after" && page.columns == (std::vector<std::string>{"limit", "after"}));
    const auto page3 = Get<cql::GetArea>("GET AREA AROUND -1 2 0 RADIUS 8 FROM t AFTER 0 2 0 LIMIT 18446744073709551615");
    assert(page3.dims == 3U && page3.after == (chunkdb::ChunkCoord{0, 2, 0}));
    assert(page3.limit == std::numeric_limits<std::uint64_t>::max());
    assert(Get<cql::GetArea>("GET AREA 0 0 TO 2 2 FROM t AFTER 0 0").after.has_value());
    assert(Get<cql::GetArea>("GET AREA 0 0 TO 2 2 FROM t LIMIT 0").limit == 0U);
    ExpectError("GET AREA 0 0 TO 2 2 FROM t LIMIT 1 LIMIT 2", "LIMIT is given twice");
    ExpectError("GET AREA 0 0 TO 2 2 FROM t AFTER 0 0 AFTER 1 1", "AFTER is given twice");
    ExpectError("GET AREA 0 0 TO 2 2 FROM t AFTER 0 0 0", "cursor has different dimensions");
    ExpectError("GET AREA 0 0 0 TO 2 2 0 FROM t AFTER 0 0", "cursor has different dimensions");
    ExpectError("GET AREA 0 0 TO 2 2 FROM t LIMIT -1", "a limit must be between 0");
}

// Float literals read '.' whatever the process locale.
void TestFloatLocale() {
    const char* previous = std::setlocale(LC_NUMERIC, nullptr);
    const std::string saved = previous != nullptr ? previous : "C";
    for (const char* name : {"de_DE.UTF-8", "de_DE.utf8", "fr_FR.UTF-8", "ru_RU.UTF-8"}) {
        if (std::setlocale(LC_NUMERIC, name) != nullptr) break;
    }
    const cql::Literal half = ValueOf("1.5");
    const cql::Literal small = ValueOf("-2.5e-310");
    std::setlocale(LC_NUMERIC, saved.c_str());
    assert(half == cql::Literal(1.5));
    assert(small == cql::Literal(-2.5e-310));
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
    ExpectError("SET BLOCK 0 0 IN t a = 1e999", "1e999 is beyond the f64 range");
    ExpectError("SET BLOCK 0 0 IN t a = -1e999", "column 25: 1e999 is beyond the f64 range");
    // The nearest f64: subnormal values are kept, and smaller ones are zero.
    assert(ValueOf("1e-310") == cql::Literal(1e-310));
    assert(ValueOf("4.9406564584124654e-324") == cql::Literal(std::numeric_limits<double>::denorm_min()));
    assert(ValueOf("2.2250738585072014e-308") == cql::Literal(std::numeric_limits<double>::min()));
    assert(ValueOf("1.7976931348623157e308") == cql::Literal(std::numeric_limits<double>::max()));
    assert(ValueOf("1e-400") == cql::Literal(0.0));
    assert(ValueOf("-1e-400") == cql::Literal(0.0));

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
    assert(cql::CountParameterTokens("BOGUS $1 $1 $0 $999999999999999999999") == 4U);
    assert(cql::CountParameterTokens("? $3 x'$1' b'$2' '$4' \"$5\" $2") == 2U);
    assert(cql::CountParameterTokens("'doubled '' $1' $2 \"escaped \"\" $3\" $4") == 2U);
    assert(cql::CountParameterTokens("$ $bad 'unterminated $1") == 0U);
    assert(cql::CountParameterTokens("$2 $1") == 2U);
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
    ExpectError("CREATE TABLE t (a u8) CHUNK", "expected a width");
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
    using Change = cql::AlterColumnAttribute::Change;
    const auto attribute = [](const std::string& line) { return std::get<cql::AlterColumnAttribute>(Get<cql::AlterTable>(line).change); };
    const auto set_default = attribute("ALTER TABLE t ALTER COLUMN a SET DEFAULT 7");
    assert(set_default.column == "a" && set_default.change == Change::kSetDefault &&
           set_default.value == cql::Literal(cql::Integer{.negative = false, .magnitude = 7}));
    assert(attribute("alter table t alter column s set default 'x'").value == cql::Literal(cql::Text{.value = "x"}));
    assert(attribute("ALTER TABLE t ALTER COLUMN a DROP DEFAULT").change == Change::kDropDefault);
    assert(!attribute("ALTER TABLE t ALTER COLUMN a DROP DEFAULT").value.has_value());
    assert(attribute("ALTER TABLE t ALTER COLUMN a SET NULL").change == Change::kSetNull);
    assert(attribute("ALTER TABLE t ALTER COLUMN a DROP NULL").change == Change::kDropNull);
    assert(attribute("ALTER TABLE t ALTER COLUMN a SET REQUIRED").change == Change::kSetRequired);
    assert(attribute("ALTER TABLE t ALTER COLUMN a DROP REQUIRED").change == Change::kDropRequired);
    ExpectError("ALTER TABLE t ALTER COLUMN a SET DEFAULT $1", "parameters are values of SET BLOCK and SET CHUNK");
    ExpectError("ALTER TABLE t ALTER COLUMN a SET UNIQUE", "expected DEFAULT, NULL or REQUIRED, got 'UNIQUE'");
    ExpectError("ALTER TABLE t ALTER COLUMN a DROP NULL NOW", "expected the end of the statement");
    ExpectError("ALTER TABLE t ALTER COLUMN a WIDEN", "expected TYPE, SET or DROP, got 'WIDEN'");
    assert(std::get<cql::RenameTable>(Get<cql::AlterTable>("ALTER TABLE t RENAME TO t_old").change).new_name == "t_old");
    ExpectError("ALTER TABLE t RENAME TO", "expected a table name");
    ExpectError("ALTER TABLE t RENAME TO T", "names are lowercase: 'T'");
    ExpectError("MIGRATE 'm1' ALTER TABLE t RENAME TO u", "MIGRATE cannot rename a table");
    const auto migrated = Get<cql::Migrate>("MIGRATE 'm1' ALTER TABLE t ALTER COLUMN a DROP NULL");
    assert(std::holds_alternative<cql::AlterColumnAttribute>(std::get<cql::AlterTable>(migrated.statement).change));

    const auto option = std::get<cql::SetOption>(Get<cql::AlterTable>("ALTER TABLE t SET cache_chunks = 64").change).option;
    assert(option.name == "cache_chunks" && option.value == cql::Literal(cql::Integer{.negative = false, .magnitude = 64}));
    ExpectError("ALTER TABLE t TRUNCATE", "expected ADD, DROP, RENAME, ALTER or SET");

    assert(Get<cql::DropTable>("DROP TABLE old").table == "old");
    (void)Get<cql::ShowTables>("SHOW TABLES");
    (void)Get<cql::ShowMetrics>("show metrics");
    (void)Get<cql::ShowStatus>("SHOW STATUS");
    assert(Get<cql::EnableFeature>("ENABLE FEATURE 'feed_slots'").name == "feed_slots");
    assert(Get<cql::Describe>("DESCRIBE world").table == "world");
    assert(Get<cql::RecoverTable>("recover table world").table == "world");
    ExpectError("RECOVER world", "expected TABLE");
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
    const auto limits = Get<cql::AlterUser>("ALTER USER bot LIMIT max_connections = 18446744073709551615, statements_per_sec = 65536");
    assert(limits.max_connections == std::numeric_limits<std::uint64_t>::max() && limits.statements_per_sec == 65536U);
    assert(Get<cql::AlterUser>("ALTER USER bot LIMIT statements_per_sec = 0").statements_per_sec == 0U);
    for (const auto line : {"ALTER USER bot LIMIT", "ALTER USER bot LIMIT max_connections = -1",
        "ALTER USER bot LIMIT statements_per_sec = 65537", "ALTER USER bot LIMIT max_connections = 18446744073709551616",
        "ALTER USER bot LIMIT max_connections = 0, max_connections = 1",
        "ALTER USER bot LIMIT statements_per_sec = 0, statements_per_sec = 1", "ALTER USER bot LIMIT unknown = 1"})
        ExpectError(line, "");
    assert(Get<cql::CreateUser>("CREATE USER bot VERIFIER $1 REQUIRE CHANNEL BINDING", 1).requires_channel_binding);
    const auto strict = Get<cql::CreateUser>("CREATE USER bot VERIFIER $1 MANAGES USERS REQUIRE CHANNEL BINDING", 1);
    assert(strict.manages_users && strict.requires_channel_binding && !create.requires_channel_binding);
    const auto require = Get<cql::AlterUser>("ALTER USER bot REQUIRE CHANNEL BINDING");
    assert(require.requires_channel_binding == true && !require.manages_users.has_value() && !require.verifier.has_value());
    assert(Get<cql::AlterUser>("ALTER USER bot NO REQUIRE CHANNEL BINDING").requires_channel_binding == false);
    assert(!rekey.requires_channel_binding.has_value());
    const auto add_verifier = Get<cql::AlterUser>("ALTER USER bot ADD VERIFIER $1", 1);
    assert(add_verifier.added_verifier == cql::Literal(cql::Parameter{.index = 1}) && !add_verifier.verifier &&
        !add_verifier.drop_old_verifier);
    const auto drop_old = Get<cql::AlterUser>("alter user bot drop old verifier");
    assert(drop_old.drop_old_verifier && !drop_old.verifier && !drop_old.added_verifier && !rekey.drop_old_verifier);
    for (const auto line : {"ALTER USER bot ADD VERIFIER", "ALTER USER bot ADD VERIFIER 12", "ALTER USER bot ADD $1",
        "ALTER USER bot DROP VERIFIER", "ALTER USER bot DROP OLD", "ALTER USER bot DROP OLD VERIFIER $1"})
        ExpectError(line, "");
    assert(Get<cql::DropUser>("DROP USER bot").user == "bot");
    const auto grant = Get<cql::GrantRight>("GRANT WRITE ON world TO bot");
    assert(!grant.revoke && grant.right == chunkdb::Right::kWrite && grant.table == "world" && grant.user == "bot");
    const auto every = Get<cql::GrantRight>("grant admin on * to bot");
    assert(every.right == chunkdb::Right::kAdmin && every.table == "*");
    const auto revoke = Get<cql::GrantRight>("REVOKE READ ON * FROM bot");
    assert(revoke.revoke && revoke.right == chunkdb::Right::kRead && revoke.user == "bot");
    ExpectError("GRANT DELETE ON world TO bot", "expected READ, WRITE, ADMIN, MONITOR, BACKUP or REPLICATION");
    ExpectError("REVOKE READ ON world TO bot", "expected FROM");
    const auto prefix = Get<cql::GrantRight>("GRANT ADMIN ON tenant1_* TO t1");
    assert(prefix.right == chunkdb::Right::kAdmin && prefix.table == "tenant1_*" && prefix.server_rights == 0U);
    assert(Get<cql::GrantRight>("REVOKE READ ON t* FROM t1").table == "t*");
    ExpectError("GRANT ADMIN ON tenant1_ * TO t1", "expected TO, got '*'");
    ExpectError("GRANT ADMIN ON tenant1_** TO t1", "expected TO, got '*'");
    ExpectError("GRANT ADMIN ON Tenant* TO t1", "names are lowercase");
    const auto server = Get<cql::GrantRight>("GRANT monitor, BACKUP, replication ON * TO ops");
    assert(!server.right.has_value() && server.server_rights == chunkdb::kKnownServerRights && server.table == "*");
    const auto mixed = Get<cql::GrantRight>("REVOKE WRITE, MONITOR, READ ON * FROM ops");
    assert(mixed.revoke && mixed.right == chunkdb::Right::kRead &&
           mixed.server_rights == static_cast<std::uint8_t>(chunkdb::ServerRight::kMonitor));
    ExpectError("GRANT MONITOR ON world TO ops", "column 18: MONITOR, BACKUP and REPLICATION are granted ON *");
    ExpectError("GRANT READ, BACKUP ON t* TO ops", "are granted ON *");
    (void)Get<cql::ShowUsers>("SHOW USERS");
    const auto scan = Get<cql::ScanChunks>("SCAN CHUNKS FROM world AFTER -3 4 LIMIT 10");
    assert(scan.table == "world" && scan.after == std::make_pair(std::int64_t{-3}, std::int64_t{4}) && scan.limit == 10U);
    const auto whole = Get<cql::ScanChunks>("scan chunks from world");
    assert(!whole.after.has_value() && !whole.limit.has_value());
    ExpectError("SCAN BLOCKS FROM world", "expected CHUNKS");
    ExpectError("SCAN CHUNKS FROM world AFTER 1", "expected a chunk y");
}

void TestWatch() {
    const auto filtered_create = Get<cql::CreateSlot>("CREATE SLOT IF NOT EXISTS 's' ON t CHUNKS COLUMNS (b,a) NO BEFORE AREA -4 -3 TO 2 9");
    assert(filtered_create.if_not_exists && filtered_create.chunks && filtered_create.no_before && filtered_create.columns->at(0) == "b" && filtered_create.area->first.x == -4);
    assert(!filtered_create.owner);
    const auto owned = Get<cql::CreateSlot>("CREATE SLOT 's' ON t CHUNKS OWNER bot NO BEFORE");
    assert(owned.owner == "bot" && owned.chunks && owned.no_before);
    assert(Get<cql::CreateSlot>("CREATE SLOT 's' ON t OWNER bot").owner == "bot");
    const auto migrate = cql::Parse("MIGRATE 'm' CREATE SLOT 's' ON t OWNER bot");
    assert(std::get<cql::CreateSlot>(std::get<cql::Migrate>(migrate.statement).statement).owner == "bot");
    const auto filters = Get<cql::Watch>("WATCH t COLUMNS b,a NO BEFORE AREA 0 0 TO 1 1 SLOT 's' CHUNKS ACK TIMEOUT 9");
    assert(filters.columns->size() == 2U && filters.no_before && filters.chunks && filters.ack_timeout_ms == 9U);
    for (const auto suffix : {"AREA 0 0 TO 1 1 AREA 0 0 TO 1 1", "COLUMNS a COLUMNS b", "NO BEFORE NO BEFORE", "CHUNKS CHUNKS", "COLUMNS ()", "AREA 1 0 TO 0 1", "OWNER bot OWNER bot", "OWNER", "OWNER Bot", "OWNER 'bot'"}) {
        const auto statement = std::string("CREATE SLOT 's' ON t ") + suffix;
        bool rejected = false; try { (void)cql::Parse(statement); } catch (const cql::ParseError&) { rejected = true; }
        assert(rejected);
    }

    const auto watch = Get<cql::Watch>("WATCH world AREA -2 3 TO 4 5 AFTER 1234567890abcdefABCDEF0123456789 18446744073709551615");
    assert(watch.table == "world" && watch.areas.at(0).first.x == -2 && watch.areas.at(0).last.y == 5);
    assert(watch.after->epoch[0] == 0x12 && watch.after->epoch[15] == 0x89);
    assert(watch.after->revision == UINT64_MAX);
    assert(Get<cql::Watch>("watch world after 00000000000000000000000000000000 0").after->revision == 0);
    assert(!Get<cql::Watch>("WATCH world").after);
    assert(Get<cql::Watch>("WATCH world ACK TIMEOUT 18446744073709551615 SLOT 's'").ack_timeout_ms == UINT64_MAX);
    assert(Get<cql::Watch>("WATCH world TAKEOVER SLOT 's'").takeover);
    assert(Get<cql::Watch>("WATCH world SLOT 's' TAKEOVER ACK TIMEOUT 10").takeover);
    ExpectError("WATCH world TAKEOVER", "requires SLOT");
    ExpectError("WATCH world SLOT 's' TAKEOVER TAKEOVER", "given twice");
    ExpectError("WATCH world ACK TIMEOUT 10", "requires SLOT");
    ExpectError("WATCH world SLOT 's' ACK TIMEOUT 0", "must be positive");
    ExpectError("WATCH world SLOT 's' ACK TIMEOUT -1", "between 0");
    ExpectError("WATCH world SLOT 's' ACK TIMEOUT 10 ACK TIMEOUT 20", "given twice");
    (void)Get<cql::Unwatch>("UNWATCH");
    ExpectError("WATCH world AREA 2 0 TO 1 0", "bounds are reversed");
    ExpectError("WATCH world AFTER abc 0", "32 hex digits");
    ExpectError("WATCH world AFTER z0000000000000000000000000000000 0", "32 hex digits");
    ExpectError("WATCH world AFTER 00000000000000000000000000000000 -1", "revision must be between 0");
    const auto slot = Get<cql::Watch>("WATCH world SLOT 'consumer_1' AREA 0 0 TO 1 1 AFTER 00000000000000000000000000000000 4");
    assert(slot.slot == "consumer_1" && slot.after->revision == 4 && slot.areas.at(0).last.x == 1);
    const auto several = Get<cql::Watch>("WATCH world AREA 0 0 TO 1 1, AREA -4 -4 TO -3 -3,area 9 9 TO 9 9 COLUMNS a");
    assert(several.areas.size() == 3U && several.areas[1].first.x == -4 && several.areas[2].last.y == 9 && several.columns->size() == 1U);
    ExpectError("WATCH world AREA 0 0 TO 1 1 AREA 2 2 TO 3 3", "AREA is given twice");
    ExpectError("WATCH world AREA 0 0 TO 1 1, COLUMNS a", "expected AREA");
    ExpectError("WATCH world AREA 0 0 TO 1 1, AREA 0 0 0 TO 1 1 1", "AREA dimensions differ");
    ExpectError("CREATE SLOT 's' ON world AREA 0 0 TO 1 1, AREA 2 2 TO 3 3", "a slot takes one AREA");
    std::string many = "WATCH world AREA 0 0 TO 0 0";
    for (std::size_t i = 1U; i < chunkdb::kMaxFeedAreas; ++i) many += ", AREA 0 0 TO 0 0";
    assert(Get<cql::Watch>(many).areas.size() == chunkdb::kMaxFeedAreas);
    ExpectError(many + ", AREA 0 0 TO 0 0", "at most 64 areas");
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
    const auto prefixed = Get<cql::ShowSlots>("SHOW SLOTS ON world PREFIX 'con'");
    assert(prefixed.table == "world" && prefixed.prefix == "con");
    assert(!Get<cql::ShowSlots>("SHOW SLOTS PREFIX '_'").table && Get<cql::ShowSlots>("show slots prefix '_'").prefix == "_");
    assert(!Get<cql::ShowSlots>("SHOW SLOTS ON world").prefix);
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
    ExpectError("SHOW SLOTS PREFIX con", "quoted slot name");
    ExpectError("SHOW SLOTS PREFIX ''", "slot names must match");
    ExpectError("SHOW SLOTS PREFIX 'con' ON world", "expected the end");
    ExpectError("UNWATCH extra", "expected the end");
}
// CHECK in CREATE TABLE (a column's and the table's), ADD CHECK and DROP
// CHECK; the condition's text is kept as written.
void TestChecks() {
    const auto create = Get<cql::CreateTable>(
        "CREATE TABLE t (hp i32 CHECK (hp >= 0) CHECK hp_max ( hp <= max_hp ), max_hp i32 NULL) CHUNK 4 x 4 "
        "CHECK (pos_z = 0 OR hp > 1) CHECK named (\"max_hp\" IS NULL)");
    assert(create.columns[0].checks.size() == 2U && create.columns[1].checks.empty());
    assert(!create.columns[0].checks[0].name && create.columns[0].checks[0].expression == "hp >= 0");
    assert(create.columns[0].checks[1].name == "hp_max" && create.columns[0].checks[1].expression == "hp <= max_hp");
    assert(create.chunk_width == 4U && create.checks.size() == 2U);
    assert(create.checks[0].expression == "pos_z = 0 OR hp > 1" && create.checks[1].name == "named");
    assert(create.checks[1].expression == "\"max_hp\" IS NULL");
    // A column named check is still a column.
    assert(Get<cql::CreateTable>("CREATE TABLE t (check u8, b bits(4) CHECK (b <> b'0000'))").columns.size() == 2U);
    const auto add = std::get<cql::AddCheck>(Get<cql::AlterTable>("ALTER TABLE t ADD CHECK (a + b < 10)").change);
    assert(!add.check.name && add.check.expression == "a + b < 10");
    assert(std::get<cql::AddCheck>(Get<cql::AlterTable>("alter table t add check ab ((a) < b)").change).check.name == "ab");
    const auto drop = std::get<cql::DropCheck>(Get<cql::AlterTable>("ALTER TABLE t DROP CHECK IF EXISTS ab").change);
    assert(drop.name == "ab" && drop.if_exists);
    assert(!std::get<cql::DropCheck>(Get<cql::AlterTable>("ALTER TABLE t DROP CHECK ab").change).if_exists);
    const auto migrated = Get<cql::Migrate>("MIGRATE 'm1' ALTER TABLE t ADD CHECK (a > 0)");
    assert(std::holds_alternative<cql::AddCheck>(std::get<cql::AlterTable>(migrated.statement).change));
    ExpectError("ALTER TABLE t ADD CHECK (a > $1)", "CHECK takes no parameters");
    ExpectError("ALTER TABLE t ADD CHECK a = 0", "expected (, got '='");
    ExpectError("ALTER TABLE t ADD CHECK (a > 0", "expected )");
    ExpectError("ALTER TABLE t ADD CHECK (a >)", "expected a value");
    ExpectError("ALTER TABLE t ADD CHECK Named (a > 0)", "names are lowercase");
    ExpectError("ALTER TABLE t ADD COLUMN c u8 CHECK (c > 0)", "ADD COLUMN takes no CHECK; add the column, then ALTER TABLE ... ADD CHECK");
    ExpectError("CREATE TABLE t (a u8) CHUNK COLUMNS (b u8 CHECK (b > 0))", "CHUNK COLUMNS takes no CHECK");
    ExpectError("ALTER TABLE t DROP CHECK", "expected a check name");
}

void TestBackup() {
    assert(Get<cql::Backup>("BACKUP TO '/backups/snapshot'").path == "/backups/snapshot");
    assert(Get<cql::Backup>("backup to '/backups/it''s'").path == "/backups/it's");
    ExpectError("BACKUP '/path'", "expected TO");
    ExpectError("BACKUP TO path", "expected a quoted backup path");
    ExpectError("BACKUP TO $1", "expected a quoted backup path");
    ExpectError("BACKUP TO '/path' extra", "expected the end of the statement");
    const auto restore = Get<cql::RestoreTable>("RESTORE TABLE world FROM 'snap/it''s' AS world_restored");
    assert(restore.table == "world" && restore.path == "snap/it's" && restore.name == "world_restored");
    const auto same = Get<cql::RestoreTable>("restore table world from 'snap'");
    assert(same.table == "world" && same.path == "snap" && same.name == "world");
    ExpectError("RESTORE world FROM 'snap'", "expected TABLE");
    ExpectError("RESTORE TABLE world 'snap'", "expected FROM");
    ExpectError("RESTORE TABLE world FROM snap", "expected a quoted backup path");
    ExpectError("RESTORE TABLE world FROM $1", "expected a quoted backup path");
    ExpectError("RESTORE TABLE world FROM 'snap' AS", "expected a table name");
    ExpectError("RESTORE TABLE world FROM 'snap' INTO other", "expected the end of the statement");
    const auto swap = Get<cql::SwapTables>("SWAP TABLES world, world_restored");
    assert(swap.first == "world" && swap.second == "world_restored");
    ExpectError("SWAP TABLE world, other", "expected TABLES");
    ExpectError("SWAP TABLES world other", "expected a comma");
    ExpectError("SWAP TABLES world,", "expected a table name");
    ExpectError("SWAP TABLES world, other, third", "expected the end of the statement");
}

void TestAggregate() {
    using Kind = cql::AggregateFunction::Kind;
    const auto area = Get<cql::Aggregate>("AGGREGATE AREA -1 -2 TO 3 4 FROM t COUNT(*), SUM(a), MIN(a), MAX(b), AVG(a)");
    assert(!area.around && area.dims == 2U && area.x0 == -1 && area.y0 == -2 && area.x1 == 3 && area.y1 == 4);
    assert(area.table == "t" && !area.by_chunk);
    assert((area.functions == std::vector<cql::AggregateFunction>{{Kind::kCount, ""}, {Kind::kSum, "a"},
        {Kind::kMin, "a"}, {Kind::kMax, "b"}, {Kind::kAvg, "a"}}));
    std::vector<std::string> names;
    for (const auto& function : area.functions) names.push_back(cql::AggregateFunctionName(function));
    assert((names == std::vector<std::string>{"count(*)", "sum(a)", "min(a)", "max(b)", "avg(a)"}));
    const auto ball = Get<cql::Aggregate>("aggregate area around 1 2 3 radius 4 from t count ( * ) by chunk");
    assert(ball.around && ball.dims == 3U && ball.z0 == 3 && ball.radius == 4 && ball.by_chunk);
    const auto chunk = Get<cql::Aggregate>("AGGREGATE CHUNK 5 -6 7 FROM t SUM(a) BY CHUNK");
    assert(!chunk.around && chunk.dims == 3U && chunk.by_chunk && chunk.functions.size() == 1U);
    assert(chunk.x0 == 5 && chunk.x1 == 5 && chunk.y0 == -6 && chunk.y1 == -6 && chunk.z0 == 7 && chunk.z1 == 7);
    ExpectError("AGGREGATE AREA 0 0 TO 1 1 FROM t", "expected COUNT, SUM, MIN, MAX or AVG, got the end of the statement");
    ExpectError("AGGREGATE AREA 0 0 TO 1 1 FROM t COUNT(a)", "COUNT takes *, got 'a'");
    ExpectError("AGGREGATE AREA 0 0 TO 1 1 FROM t SUM(*)", "expected a column name, got '*'");
    ExpectError("AGGREGATE AREA 0 0 TO 1 1 FROM t SUM a", "expected (, got 'a'");
    ExpectError("AGGREGATE AREA 0 0 TO 1 1 FROM t MEDIAN(a)", "expected COUNT, SUM, MIN, MAX or AVG, got 'MEDIAN'");
    ExpectError("AGGREGATE AREA 0 0 TO 1 1 FROM t SUM(a), sum(a)", "column 42: sum(a) is given twice");
    ExpectError("AGGREGATE AREA 0 0 TO 1 1 FROM t COUNT(*) BY CHUNK BY CHUNK", "BY CHUNK is given twice");
    ExpectError("AGGREGATE AREA 0 0 TO 1 1 FROM t COUNT(*) BY BLOCK", "expected CHUNK, got 'BLOCK'");
    const auto where = Get<cql::Aggregate>("AGGREGATE CHUNK 0 0 FROM t COUNT(*) WHERE a > $1 OR b IN ($2, 3) BY CHUNK", 2);
    assert(where.where && where.by_chunk && where.where->nodes[where.where->root].op == chunkdb::expr::Op::kOr);
    assert(Get<cql::Aggregate>("AGGREGATE CHUNK 0 0 FROM t COUNT(*) BY CHUNK WHERE EXISTS").where->nodes.size() == 1U);
    assert(!chunk.where);
    ExpectError("AGGREGATE AREA 0 0 TO 1 1 FROM t COUNT(*) WHERE a > 1 WHERE a < 3", "column 55: WHERE is given twice");
    ExpectError("AGGREGATE AREA 0 0 TO 1 1 FROM t COUNT(*) WHERE a >", "expected a value, got the end of the statement");
    ExpectError("AGGREGATE CHUNK 0 0 FROM t COUNT(*) WHERE a = $1 AND b = $1", "$1 is used twice");
    ExpectError("AGGREGATE CHUNK 0 0 FROM t COUNT(*) WHERE (a = 1) + 1 > 0", "column 51: unexpected '+'");
    std::string long_where = "AGGREGATE CHUNK 0 0 FROM t COUNT(*) WHERE a = 0";
    for (int i = 1; i < 90; ++i) long_where += " OR a = " + std::to_string(i);
    ExpectError(long_where, "the expression is too long: at most 256 nodes");
    ExpectError("AGGREGATE AREA 0 0 TO 1 1 0 FROM t COUNT(*)", "AREA endpoints have different dimensions");
    ExpectError("AGGREGATE AREA AROUND 0 0 RADIUS -1 FROM t COUNT(*)", "a radius must be between 0");
    ExpectError("AGGREGATE BLOCK 0 0 FROM t COUNT(*)", "expected CHUNK or AREA, got 'BLOCK'");
    ExpectError("AGGREGATE CHUNK 0 0 FROM t COUNT(*) LIMIT 1", "expected the end of the statement, got 'LIMIT'");
    ExpectError("AGGREGAT CHUNK 0 0 FROM t COUNT(*)", "did you mean AGGREGATE?");
}

void TestErrors() {
    ExpectError("", "column 1: unknown statement the end of the statement");
    ExpectError("SELECT * FROM t", "column 1: unknown statement 'SELECT'; read with GET BLOCK, GET AREA or GET CHUNK");
    ExpectError("BOGUS", "column 1: unknown statement 'BOGUS'; the CQL reference lists every statement");
    ExpectError("GET ROW 1 2 FROM t", "column 5: expected BLOCK, CHUNK or AREA, got 'ROW'");
    ExpectError("GET BLOCK 1 FROM t", "column 13: expected y, got 'FROM'");
    ExpectError("GET BLOCK 1 2 FROM t extra", "column 22: expected the end of the statement, got 'extra'");
    ExpectError("GET BLOCK 1 2 FROM t COLUMNS", "expected a column name, got the end of the statement");
    ExpectError("GET BLOCK 1 2 FROM t COLUMNS a,", "expected a column name");
    ExpectError("GET BLOCK 9223372036854775808 0 FROM t", "x is out of range");
    ExpectError("GET BLOCK 1 2 FROM t; DROP TABLE t", "column 21: unexpected ';'");
    ExpectError("SET BLOCK 0 0 IN t", "expected a column name");
    ExpectError("SET BLOCK 0 0 IN t a 1", "expected =, got '1'");
    ExpectError("SET BLOCK 0 0 IN t a = 1 IF VERSION -1", "a version must be between 0");
    ExpectError("DROP t", "expected TABLE");
    ExpectError("FLUSH", "expected WAL");
    ExpectError("SHOW GRANTS", "expected TABLES, METRICS, STATUS, CONNECTIONS, MIGRATIONS, USERS or SLOTS");
}

// IF <expr> and computed values of SET BLOCK and DELETE BLOCK (#145): a
// statement with single values and no condition parses as before.
void TestBlockExpressions() {
    const auto plain = Get<cql::SetBlock>("SET BLOCK 0 0 IN t a = 1, b = $1", 1);
    assert(plain.values.size() == 2U && !plain.expressions && !plain.if_version);
    const auto versioned = Get<cql::SetBlock>("SET BLOCK 0 0 IN t a = 1 IF VERSION 3");
    assert(versioned.if_version == 3U && !versioned.expressions);

    const auto computed = Get<cql::SetBlock>("SET BLOCK 1 2 IN t coins = coins + $1", 1);
    assert(computed.values.empty() && computed.expressions && !computed.expressions->condition);
    assert(computed.expressions->assignments.size() == 1U && computed.expressions->assignments[0].column == "coins");
    // One computed value makes every value an expression.
    const auto mixed = Get<cql::SetBlock>("SET BLOCK 1 2 IN t a = $1, b = a * 2, c = (3)", 1);
    assert(mixed.values.empty() && mixed.expressions->assignments.size() == 3U);
    assert(Get<cql::SetBlock>("SET BLOCK 1 2 IN t a = 1 * 2").expressions->assignments.size() == 1U);
    assert(Get<cql::SetBlock>("SET BLOCK 1 2 IN t a = -b").expressions);

    const auto conditional = Get<cql::SetBlock>("SET BLOCK 1 2 3 IN t a = 1, b = $1 IF a < $2 AND EXISTS", 2);
    assert(conditional.dims == 3U && conditional.values.size() == 2U && !conditional.if_version);
    assert(conditional.expressions->condition && conditional.expressions->assignments.empty());
    const auto both = Get<cql::SetBlock>("SET BLOCK 1 2 IN t hp = LEAST(hp + 5, max_hp) if hp > 0", 0);
    assert(both.expressions->condition && both.expressions->assignments.size() == 1U);
    // `version` is a column unless VERSION is followed by a number.
    const auto column = Get<cql::SetBlock>("SET BLOCK 1 2 IN t a = 1 IF version = 3");
    assert(!column.if_version && column.expressions->condition);
    const auto counted = Get<cql::SetBlock>("SET BLOCK 1 2 IN t n = COALESCE(n, 0) + 1 IF VERSION 7");
    assert(counted.if_version == 7U && counted.expressions && !counted.expressions->condition);

    const auto removed = Get<cql::DeleteBlock>("DELETE BLOCK 1 2 FROM t IF EXISTS AND hp <= $1", 1);
    assert(removed.expressions && removed.expressions->condition && !removed.if_version);
    const auto removed3 = Get<cql::DeleteBlock>("delete block 1 2 3 from t if chunk.status = 2");
    assert(removed3.dims == 3U && removed3.expressions->condition);
    const auto unconditional = Get<cql::DeleteBlock>("DELETE BLOCK 1 2 FROM t IF VERSION 4");
    assert(unconditional.if_version == 4U && !unconditional.expressions);
    // IF VERSION $n reads the version from a parameter frame.
    const auto versioned_parameter = Get<cql::SetBlock>("SET BLOCK 1 2 IN t a = $2 IF VERSION $1", 2);
    assert(!versioned_parameter.if_version && versioned_parameter.values.size() == 1U);
    assert(versioned_parameter.expressions->version_parameter == 1U && !versioned_parameter.expressions->condition);
    const auto removed_parameter = Get<cql::DeleteBlock>("DELETE BLOCK 1 2 FROM t IF VERSION $1", 1);
    assert(!removed_parameter.if_version && removed_parameter.expressions->version_parameter == 1U);
    ExpectError("DELETE BLOCK 1 2 FROM t IF VERSION $1 AND a = 1", "expected the end of the statement");

    ExpectError("SET BLOCK 0 0 IN t a = 1 IF VERSION 3 AND a = 1", "expected the end of the statement, got 'AND'");
    ExpectError("SET BLOCK 0 0 IN t a = coins + $1 IF a > $1", "$1 is used twice");
    ExpectError("SET BLOCK 0 0 IN t a = b + $2", "numbered $1 to $2 without gaps");
    ExpectError("SET BLOCK 0 0 IN t a = b +", "column 27");
    ExpectError("SET BLOCK 0 0 IN t a = 1 IF", "column 28");
    ExpectError("SET BLOCK 0 0 IN t a = 1 IF a > 1 extra", "expected the end of the statement, got 'extra'");
    ExpectError("DELETE BLOCK 0 0 FROM t IF a +", "column 31");
    ExpectError("DELETE BLOCK 0 0 FROM t IF VERSION 1.5", "expected a version");
}

// RETURNING [OLD | NEW] (* | column, ...) of SET BLOCK and DELETE BLOCK
// (#145): NEW is SET's default, OLD DELETE's and its only form; a write
// without it parses as before.
void TestBlockReturning() {
    assert(!Get<cql::SetBlock>("SET BLOCK 0 0 IN t a = 1").returning);
    assert(!Get<cql::DeleteBlock>("DELETE BLOCK 0 0 FROM t").returning);

    const auto all = Get<cql::SetBlock>("SET BLOCK 0 0 IN t a = 1, b = $1 RETURNING *", 1);
    assert(all.values.size() == 2U && !all.expressions && all.returning);
    assert(all.returning->new_values && all.returning->columns.empty());
    const auto old = Get<cql::SetBlock>("set block 0 0 in t a = 1 returning old a, b");
    assert(!old.expressions && !old.returning->new_values);
    assert((old.returning->columns == std::vector<std::string>{"a", "b"}));
    const auto computed = Get<cql::SetBlock>("SET BLOCK 1 2 3 IN t coins = coins + $1 IF coins > 0 RETURNING NEW coins", 1);
    assert(computed.dims == 3U && computed.expressions->condition && computed.expressions->assignments.size() == 1U);
    assert(computed.returning->new_values && computed.returning->columns == std::vector<std::string>{"coins"});
    const auto versioned = Get<cql::SetBlock>("SET BLOCK 0 0 IN t a = 1 IF VERSION 3 RETURNING OLD *");
    assert(versioned.if_version == 3U && !versioned.returning->new_values);
    // OLD and NEW right after RETURNING are keywords; a column named old
    // follows one of them.
    const auto named = Get<cql::SetBlock>("SET BLOCK 0 0 IN t old = 1 RETURNING NEW old, new");
    assert((named.returning->columns == std::vector<std::string>{"old", "new"}));

    const auto removed = Get<cql::DeleteBlock>("DELETE BLOCK 0 0 FROM t RETURNING *");
    assert(removed.returning && !removed.returning->new_values && !removed.expressions);
    const auto conditional = Get<cql::DeleteBlock>("DELETE BLOCK 0 0 FROM t IF hp <= $1 RETURNING OLD hp", 1);
    assert(conditional.expressions->condition && conditional.returning->columns == std::vector<std::string>{"hp"});

    ExpectError("DELETE BLOCK 0 0 FROM t RETURNING NEW *", "column 35: DELETE BLOCK returns only OLD values");
    ExpectError("SET BLOCK 0 0 IN t a = 1 RETURNING", "expected a column name or *");
    ExpectError("SET BLOCK 0 0 IN t a = 1 RETURNING OLD", "expected a column name or *");
    ExpectError("SET BLOCK 0 0 IN t a = 1 RETURNING a,", "expected a column name or *");
    ExpectError("SET BLOCK 0 0 IN t a = 1 RETURNING *, a", "expected the end of the statement, got ','");
    ExpectError("SET BLOCK 0 0 IN t a = 1 RETURNING A", "names are lowercase");
    ExpectError("SET BLOCK 0 0 IN t a = 1 RETURNING * IF a > 1", "expected the end of the statement, got 'IF'");
}

}  // namespace

void TestHelloOptions() {
    using namespace chunkdb::cql;
    const auto hello = ParseHello("HELLO 3 USER admin $1 WITH CLIENT_NAME='a b',statement_timeout_ms=0,default_tag=$2,future=$3");
    assert(hello.protocol == 3U && hello.user == "admin" && hello.parameters == 3U);
    assert(hello.options.size() == 4U && hello.options[0].name == "client_name");
    assert(std::get<Text>(hello.options[0].value).value == "a b");
    assert(std::get<Integer>(hello.options[1].value).magnitude == 0U);
    for (const auto line : {"HELLO 3 WITH x=$2", "HELLO 3 WITH x=$1,y=$1", "HELLO 3 USER admin $2", "HELLO 3 WITH x="}) {
        bool rejected = false;
        try { (void)ParseHello(line); } catch (const ParseError&) { rejected = true; }
        assert(rejected);
    }
}

int main() {
    assert(Get<cql::Cancel>("CANCEL 18446744073709551615").id == UINT64_MAX);
    assert(Get<cql::KillConnection>("KILL CONNECTION 0").id == 0U);
    (void)Get<cql::ShowConnections>("SHOW CONNECTIONS");
    for (const auto* line : {"CANCEL -1", "CANCEL 18446744073709551616", "CANCEL $1", "CANCEL 1 extra",
                             "KILL 1", "KILL CONNECTION -1", "SHOW CONNECTIONS extra"}) {
        bool refused = false;
        try { (void)cql::Parse(line); } catch (const cql::ParseError&) { refused = true; }
        assert(refused);
    }
    TestChunkModelCoordinates();
    TestChunkModelSchemaAndWrites();
    TestChunkModelScan();
    TestHelloOptions();
    TestDefaultChunkGeometry();
    TestRightsLists();
    TestUnorderedOptions();
    TestStatementSuggestions();
    TestConditionalObjects();
    TestBlockStatements();
    TestBlockExpressions();
    TestBlockReturning();
    TestChunkAndAreaStatements();
    TestAggregate();
    TestLiterals();
    TestFloatLocale();
    TestParameters();
    TestCreateTable();
    TestAlterAndOtherStatements();
    TestWatch();
    TestBackup();
    TestChecks();
    TestErrors();
    return 0;
}
