// AGGREGATE (docs/reference/AGGREGATE.md): COUNT(*), SUM, MIN, MAX and AVG
// over the present blocks of a chunk, area or ball, with BY CHUNK, NULL and
// float rules, exact integer sums, bounds and transaction snapshots.

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "aggregate.hpp"
#include "block_filter.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/table_catalog.hpp"
#include "statement_control.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-aggregate"};
    std::shared_ptr<TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    SessionState session;
    SessionState other;
    explicit Fixture(QueryLimits limits = {}) {
        CatalogConfig config;
        config.data_dir = directory.path();
        config.background_maintenance = false;
        config.default_options.durability_mode = DurabilityMode::kRelaxed;
        config.query_limits = limits;
        catalog = std::make_shared<TableCatalog>(config);
        engine = std::make_unique<CommandEngine>(EngineConfig{.require_auth = false}, catalog);
        assert(Run("HELLO 3").starts_with("%"));
        assert(engine->Execute(other, "HELLO 3\r\n").starts_with("%"));
    }
    std::string Run(std::string_view command) { return engine->Execute(session, std::string(command) + "\r\n"); }
    std::string RunOther(std::string_view command) { return engine->Execute(other, std::string(command) + "\r\n"); }
    void Ok(std::string_view command) {
        const auto reply = Run(command);
        if (reply.starts_with("-")) {
            std::fprintf(stderr, "%s: %s", std::string(command).c_str(), reply.c_str());
            assert(false);
        }
    }
};

void Error(const std::string& reply, std::string_view code, std::string_view part = {}) {
    if (!reply.starts_with("-ERR " + std::string(code) + " ") || reply.find(part) == std::string::npos) {
        std::fprintf(stderr, "expected %s with '%s', got %s", std::string(code).c_str(), std::string(part).c_str(), reply.c_str());
        assert(false);
    }
}

std::string Int(std::int64_t value) { std::string out; Protocol::AppendInteger(out, value); return out; }
std::string UInt(std::uint64_t value) { std::string out; Protocol::AppendInteger(out, value); return out; }
std::string Double(double value) { std::string out; Protocol::AppendDouble(out, value); return out; }
std::string Float(float value) { std::string out; Protocol::AppendFloat(out, value); return out; }
std::string Null() { return "_\r\n"; }

std::string Map(const std::vector<std::pair<std::string, std::string>>& entries) {
    std::string out;
    Protocol::AppendMapHeader(out, entries.size());
    for (const auto& [name, value] : entries) {
        Protocol::AppendBulk(out, name);
        out += value;
    }
    return out;
}

std::string Rows(const std::vector<std::vector<std::string>>& rows) {
    std::string out;
    Protocol::AppendArrayHeader(out, rows.size());
    for (const auto& row : rows) {
        Protocol::AppendArrayHeader(out, row.size());
        for (const auto& value : row) out += value;
    }
    return out;
}

void Same(const std::string& actual, const std::string& expected) {
    if (actual != expected) {
        std::fprintf(stderr, "expected %s\ngot %s\n", expected.c_str(), actual.c_str());
        assert(false);
    }
}

constexpr auto kU64Max = std::numeric_limits<std::uint64_t>::max();
constexpr auto kI64Min = std::numeric_limits<std::int64_t>::min();

void CreateIntegers(Fixture& f) {
    f.Ok("CREATE TABLE t (v u8 DEFAULT 0, s i16 NULL, big u64 NULL, neg i64 NULL, f f32 NULL, name text(8) NULL, "
         "flag bool NULL) CHUNK 2 x 2 CHUNK COLUMNS (tag u8 DEFAULT 7)");
    f.Ok("SET BLOCK 0 0 IN t v = 1, s = -5, big = 18446744073709551615, neg = -9223372036854775808");
    f.Ok("SET BLOCK 1 0 IN t v = 2, big = 18446744073709551615, neg = -9223372036854775808");
    f.Ok("SET BLOCK 2 0 IN t v = 4, s = 7, big = 18446744073709551615, neg = -1");
}

// Integer SUM is exact past 64 bits; NULL values are skipped.
void IntegersAndNulls() {
    Fixture f;
    CreateIntegers(f);
    Same(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*), SUM(v), MIN(s), MAX(s), AVG(s), SUM(big), SUM(neg), "
               "MAX(big), MIN(neg), AVG(v), SUM(f)"),
        Map({{"count(*)", UInt(3U)}, {"sum(v)", Int(7)}, {"min(s)", Int(-5)}, {"max(s)", Int(7)},
            {"avg(s)", Double(1.0)}, {"sum(big)", "(55340232221128654845\r\n"},
            {"sum(neg)", "(-18446744073709551617\r\n"}, {"max(big)", UInt(kU64Max)}, {"min(neg)", Int(kI64Min)},
            {"avg(v)", Double(7.0 / 3.0)}, {"sum(f)", Null()}}));
    // An area without blocks counts zero and has no values.
    Same(f.Run("AGGREGATE AREA 5 5 TO 6 6 FROM t COUNT(*), SUM(v), MIN(f), AVG(s)"),
        Map({{"count(*)", UInt(0U)}, {"sum(v)", Null()}, {"min(f)", Null()}, {"avg(s)", Null()}}));
    Same(f.Run("AGGREGATE CHUNK 1 0 FROM t SUM(v), COUNT(*)"), Map({{"sum(v)", Int(4)}, {"count(*)", UInt(1U)}}));
    Same(f.Run("AGGREGATE AREA AROUND 0 0 RADIUS 0 FROM t SUM(v)"), Map({{"sum(v)", Int(3)}}));
    // A sum within int64 stays an integer.
    Same(f.Run("AGGREGATE CHUNK 1 0 FROM t SUM(big), SUM(neg)"), Map({{"sum(big)", "(18446744073709551615\r\n"},
        {"sum(neg)", Int(-1)}}));
}

// f32 and f64 sum in f64; MIN and MAX order NaN above every value.
void Floats() {
    Fixture f;
    f.Ok("CREATE TABLE fl (f f32 NULL, d f64 NULL) CHUNK 2 x 2");
    f.Ok("SET BLOCK 0 0 IN fl f = 1.5, d = 1.5");
    f.Ok("SET BLOCK 1 0 IN fl f = nan, d = nan");
    f.Ok("SET BLOCK 0 1 IN fl f = -inf, d = -inf");
    f.Ok("SET BLOCK 1 1 IN fl f = 0.25");
    const auto nan = std::numeric_limits<double>::quiet_NaN();
    const auto inf = std::numeric_limits<double>::infinity();
    Same(f.Run("AGGREGATE CHUNK 0 0 FROM fl MIN(f), MAX(f), SUM(f), AVG(f), MIN(d), MAX(d), SUM(d), COUNT(*)"),
        Map({{"min(f)", Float(-std::numeric_limits<float>::infinity())}, {"max(f)", Float(std::numeric_limits<float>::quiet_NaN())},
            {"sum(f)", Double(nan)}, {"avg(f)", Double(nan)}, {"min(d)", Double(-inf)}, {"max(d)", Double(nan)},
            {"sum(d)", Double(nan)}, {"count(*)", UInt(4U)}}));
    f.Ok("SET BLOCK 2 0 IN fl f = 0.1, d = 0.1");
    f.Ok("SET BLOCK 3 0 IN fl f = 0.2, d = 0.2");
    Same(f.Run("AGGREGATE CHUNK 1 0 FROM fl MIN(f), MAX(f), SUM(f), AVG(d), MAX(d)"),
        Map({{"min(f)", Float(0.1F)}, {"max(f)", Float(0.2F)},
            {"sum(f)", Double(static_cast<double>(0.1F) + static_cast<double>(0.2F))},
            {"avg(d)", Double((0.1 + 0.2) / 2.0)}, {"max(d)", Double(0.2)}}));
    assert(Float(0.1F) == ",0.1\r\n");
}

// BY CHUNK answers one row per chunk with a present block, in coordinate
// order; chunks with only metadata or no blocks left are omitted.
void ByChunk() {
    Fixture f;
    CreateIntegers(f);
    f.Ok("SET BLOCK 2 1 IN t v = 8");
    f.Ok("SET BLOCK -1 -1 IN t v = 9");
    f.Ok("SET CHUNK 5 5 IN t META tag = 1");
    f.Ok("SET BLOCK 6 0 IN t v = 3");
    f.Ok("DELETE BLOCK 6 0 FROM t");
    Same(f.Run("AGGREGATE AREA -1 -1 TO 5 5 FROM t COUNT(*), SUM(v), MIN(s) BY CHUNK"),
        Rows({{Int(-1), Int(-1), UInt(1U), Int(9), Null()}, {Int(0), Int(0), UInt(2U), Int(3), Int(-5)},
            {Int(1), Int(0), UInt(2U), Int(12), Int(7)}}));
    Same(f.Run("AGGREGATE AREA -1 -1 TO 5 5 FROM t COUNT(*), SUM(v)"), Map({{"count(*)", UInt(5U)}, {"sum(v)", Int(24)}}));
    Same(f.Run("AGGREGATE AREA 4 4 TO 5 5 FROM t COUNT(*) BY CHUNK"), Rows({}));

    f.Ok("CREATE TABLE cube (v i8) CHUNK 2 x 2 x 2");
    f.Ok("SET BLOCK 0 0 0 IN cube v = -3");
    f.Ok("SET BLOCK 3 0 1 IN cube v = 5");
    f.Ok("SET BLOCK 0 0 2 IN cube v = 1");
    Same(f.Run("AGGREGATE AREA AROUND 0 0 0 RADIUS 1 FROM cube SUM(v), COUNT(*) BY CHUNK"),
        Rows({{Int(0), Int(0), Int(0), Int(-3), UInt(1U)}, {Int(0), Int(0), Int(1), Int(1), UInt(1U)},
            {Int(1), Int(0), Int(0), Int(5), UInt(1U)}}));
    Same(f.Run("AGGREGATE CHUNK 1 0 0 FROM cube MAX(v)"), Map({{"max(v)", Int(5)}}));
    Error(f.Run("AGGREGATE CHUNK 0 0 FROM cube COUNT(*)"), "INVALID_ARGUMENT", "coordinate dimensions differ");
}

void Refusals() {
    Fixture f;
    CreateIntegers(f);
    Error(f.Run("AGGREGATE CHUNK 0 0 FROM t SUM(name)"), "INVALID_ARGUMENT", "sum(name) takes an integer or float column");
    Error(f.Run("AGGREGATE CHUNK 0 0 FROM t MIN(flag)"), "INVALID_ARGUMENT", "min(flag) takes an integer or float column");
    const auto missing = f.Run("AGGREGATE CHUNK 0 0 FROM t MAX(tag)");
    Error(missing, "INVALID_ARGUMENT", "the table has no column tag; use DESCRIBE <table> to check column names\r\n");
    assert(missing.find("DESCRIBE") == missing.rfind("DESCRIBE"));
    Error(f.Run("AGGREGATE CHUNK 0 0 FROM missing COUNT(*)"), "NO_TABLE");
    Error(f.Run("AGGREGATE AREA 1 0 TO 0 0 FROM t COUNT(*)"), "INVALID_ARGUMENT");
}

void Bounds() {
    {
        Fixture f(QueryLimits{.max_area_chunks = 4U});
        CreateIntegers(f);
        Same(f.Run("AGGREGATE AREA 0 0 TO 1 1 FROM t COUNT(*)"), Map({{"count(*)", UInt(3U)}}));
        Error(f.Run("AGGREGATE AREA 0 0 TO 2 1 FROM t COUNT(*)"), "INVALID_ARGUMENT", "AGGREGATE covers at most 4 chunks");
        Error(f.Run("AGGREGATE AREA AROUND 0 0 RADIUS 2 FROM t COUNT(*)"), "INVALID_ARGUMENT", "covers at most 4 chunks");
    }
    const auto one_row = Rows({{Int(0), Int(0), UInt(2U)}}).size();
    Fixture f(QueryLimits{.max_area_response_bytes = one_row});
    CreateIntegers(f);
    Same(f.Run("AGGREGATE CHUNK 0 0 FROM t COUNT(*) BY CHUNK"), Rows({{Int(0), Int(0), UInt(2U)}}));
    Error(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*) BY CHUNK"), "OUT_OF_RANGE", "aggregate response exceeds");
    Error(f.Run("AGGREGATE CHUNK 0 0 FROM t COUNT(*), SUM(v), MIN(v), MAX(v)"), "OUT_OF_RANGE", "aggregate response exceeds");
}

// Inside a transaction AGGREGATE reads its snapshot and private writes and
// adds every requested cell to the read set; a refused read adds none.
void Transactions() {
    {
        Fixture f;
        CreateIntegers(f);
        f.Ok("BEGIN");
        const auto before = f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*), SUM(v)");
        Same(before, Map({{"count(*)", UInt(3U)}, {"sum(v)", Int(7)}}));
        assert(f.session.transaction->read_set.size() == 2U);
        assert(f.RunOther("SET BLOCK 3 1 IN t v = 100").starts_with(":"));
        Same(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*), SUM(v)"), before);
        assert(f.Run("SET BLOCK 1 1 IN t v = 10") == "_\r\n");
        Same(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*), SUM(v) BY CHUNK"),
            Rows({{Int(0), Int(0), UInt(3U), Int(13)}, {Int(1), Int(0), UInt(1U), Int(4)}}));
        Same(f.Run("AGGREGATE AREA 0 0 TO 2 0 FROM t COUNT(*)"), Map({{"count(*)", UInt(4U)}}));
        assert(f.session.transaction->read_set.size() == 3U);
        f.Ok("ROLLBACK");
        Same(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*), SUM(v)"), Map({{"count(*)", UInt(4U)}, {"sum(v)", Int(107)}}));
    }
    {
        Fixture f(QueryLimits{.txn_max_read_chunks = 1U});
        CreateIntegers(f);
        f.Ok("BEGIN");
        Error(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*)"), "INVALID_ARGUMENT", "reads at most 1 chunks");
        assert(f.session.transaction->read_set.empty());
        Same(f.Run("AGGREGATE CHUNK 0 0 FROM t COUNT(*)"), Map({{"count(*)", UInt(2U)}}));
        f.Ok("ROLLBACK");
    }
    // A reply over the size limit is refused after the visit and adds no
    // cell, so a later write to those cells does not fail the COMMIT.
    Fixture f(QueryLimits{.max_area_response_bytes = Rows({{Int(0), Int(0), UInt(2U)}}).size()});
    CreateIntegers(f);
    f.Ok("BEGIN");
    Error(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*), SUM(v), MIN(v)"), "OUT_OF_RANGE", "aggregate response exceeds");
    Error(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*) BY CHUNK"), "OUT_OF_RANGE", "aggregate response exceeds");
    assert(f.session.transaction->read_set.empty());
    assert(f.RunOther("SET BLOCK 0 0 IN t v = 9").starts_with(":"));
    assert(f.Run("SET BLOCK 5 5 IN t v = 2") == "_\r\n");
    f.Ok("COMMIT");
}

// A chunk with only chunk-column values has no blocks, committed or written
// in the open transaction.
void MetaOnlyChunk() {
    Fixture f;
    CreateIntegers(f);
    f.Ok("SET CHUNK 5 5 IN t META tag = 1");
    const auto empty = Map({{"count(*)", UInt(0U)}, {"sum(v)", Null()}});
    Same(f.Run("AGGREGATE CHUNK 5 5 FROM t COUNT(*), SUM(v)"), empty);
    Same(f.Run("AGGREGATE CHUNK 5 5 FROM t COUNT(*), SUM(v) BY CHUNK"), Rows({}));
    f.Ok("BEGIN");
    f.Ok("SET CHUNK 6 6 IN t META tag = 2");
    Same(f.Run("AGGREGATE AREA 5 5 TO 6 6 FROM t COUNT(*), SUM(v)"), empty);
    Same(f.Run("AGGREGATE AREA 5 5 TO 6 6 FROM t COUNT(*), SUM(v) BY CHUNK"), Rows({}));
    f.Ok("COMMIT");
    Same(f.Run("AGGREGATE AREA 5 5 TO 6 6 FROM t COUNT(*), SUM(v)"), empty);
    Same(f.Run("AGGREGATE AREA 5 5 TO 6 6 FROM t COUNT(*), SUM(v) BY CHUNK"), Rows({}));
}

// The block loop of one chunk stops a cancelled statement.
void Cancellation() {
    TableSchema schema;
    schema.columns = {Column{.id = 1U, .name = "v", .type = {ColumnKind::kSigned, 4U}, .nullable = true,
        .default_value = {}}};
    schema.next_column_id = 2U;
    const ChunkLayout layout(schema, 4U, 1024U);
    const std::vector<cql::AggregateFunction> functions{
        {cql::AggregateFunction::Kind::kSum, "v"}, {cql::AggregateFunction::Kind::kCount, ""}};
    Aggregator aggregator(layout, functions);
    // Values -1, 3 and NULL in blocks 0, 1 and 3; block 2 is absent.
    std::vector<std::uint8_t> payload(layout.payload_bytes());
    payload[0] = 0x3F;
    payload[layout.fixed_columns()[0].validity] = 0x03;
    const std::vector<std::uint8_t> presence{0x0B};
    aggregator.Add(payload, presence);
    std::string out;
    aggregator.AppendResults(out);
    Same(out, Int(2) + UInt(3U));
    StatementControl control(StatementControl::Clock::now(), 0U);
    assert(control.Start());
    ScopedStatementControl context(&control);
    assert(control.RequestStop(StatementStopReason::kCancelled));
    bool stopped = false;
    try { aggregator.Add(payload, presence); } catch (const StatementStoppedError&) { stopped = true; }
    assert(stopped && aggregator.blocks() == 3U);
}

// Values are read in place as DecodeColumnValue reads them: widths that cross
// byte boundaries, sign extension, 33 and 64 bits, f32 and f64, NULL skipped.
void ValuesInPlace() {
    const auto column = [](std::uint32_t id, std::string name, ColumnKind kind, std::uint32_t width, bool nullable) {
        return Column{.id = id, .name = std::move(name), .type = {kind, width}, .nullable = nullable, .default_value = {}};
    };
    TableSchema schema;
    schema.columns = {column(1U, "a", ColumnKind::kUnsigned, 5U, true), column(2U, "b", ColumnKind::kSigned, 7U, false),
        column(3U, "c", ColumnKind::kSigned, 3U, true), column(4U, "d", ColumnKind::kUnsigned, 64U, false),
        column(5U, "e", ColumnKind::kSigned, 64U, false), column(6U, "g", ColumnKind::kSigned, 33U, false),
        column(7U, "h", ColumnKind::kFloat32, 32U, true), column(8U, "k", ColumnKind::kFloat64, 64U, false)};
    schema.next_column_id = 9U;
    constexpr std::size_t kBlocks = 13U;
    const ChunkLayout layout(schema, kBlocks, 4096U);
    std::vector<std::uint8_t> payload(layout.payload_bytes(), 0U);
    std::vector<std::uint8_t> presence((kBlocks + 7U) / 8U, 0U);
    std::uint64_t seed = 0x9E3779B97F4A7C15ULL;
    for (std::size_t block = 0U; block < kBlocks; ++block) {
        if (block == 6U) continue;
        presence[block / 8U] |= static_cast<std::uint8_t>(1U << (block % 8U));
        for (const auto& fixed : layout.fixed_columns()) {
            seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
            auto raw = seed;
            const auto kind = schema.columns[fixed.column].type.kind;
            if (kind == ColumnKind::kFloat32)
                raw = std::bit_cast<std::uint32_t>(static_cast<float>(static_cast<std::int64_t>(seed >> 40U) - 8000) / 7.0F);
            if (kind == ColumnKind::kFloat64)
                raw = std::bit_cast<std::uint64_t>(static_cast<double>(static_cast<std::int64_t>(seed >> 20U)) / -3.0);
            std::array<std::uint8_t, 8U> bytes{};
            for (std::size_t i = 0U; i < bytes.size(); ++i) bytes[i] = static_cast<std::uint8_t>(raw >> (8U * i));
            WriteValueBits(payload.data() + fixed.values, block * fixed.width, bytes.data(), fixed.width);
            if (fixed.validity != ChunkLayout::kNoValidity && block % 4U != 1U)
                payload[fixed.validity + block / 8U] |= static_cast<std::uint8_t>(1U << (block % 8U));
        }
    }
    using Kind = cql::AggregateFunction::Kind;
    std::vector<cql::AggregateFunction> functions;
    std::string expected;
    for (const auto& fixed : layout.fixed_columns()) {
        const auto& schema_column = schema.columns[fixed.column];
        const auto kind = schema_column.type.kind;
        const bool floating = kind == ColumnKind::kFloat32 || kind == ColumnKind::kFloat64;
        __int128 low = 0, high = 0, sum = 0;
        double low_number = 0.0, high_number = 0.0;
        bool first = true;
        for (std::size_t block = 0U; block < kBlocks; ++block) {
            if ((presence[block / 8U] & (1U << (block % 8U))) == 0U) continue;
            if (fixed.validity != ChunkLayout::kNoValidity && (payload[fixed.validity + block / 8U] & (1U << (block % 8U))) == 0U)
                continue;
            std::array<std::uint8_t, 8U> bytes{};
            ReadValueBits(payload.data(), fixed.values * 8U + block * fixed.width, bytes.data(), fixed.width);
            const auto value = DecodeColumnValue(schema_column, bytes.data());
            if (floating) {
                const double number = kind == ColumnKind::kFloat32 ? static_cast<double>(std::get<float>(value)) : std::get<double>(value);
                low_number = first ? number : std::min(low_number, number);
                high_number = first ? number : std::max(high_number, number);
            } else {
                const __int128 integer = kind == ColumnKind::kUnsigned ? static_cast<__int128>(std::get<std::uint64_t>(value))
                                                                       : static_cast<__int128>(std::get<std::int64_t>(value));
                low = first ? integer : std::min(low, integer);
                high = first ? integer : std::max(high, integer);
                sum += integer;
            }
            first = false;
        }
        functions.push_back({Kind::kMin, schema_column.name});
        functions.push_back({Kind::kMax, schema_column.name});
        if (floating && kind == ColumnKind::kFloat32) {
            Protocol::AppendFloat(expected, static_cast<float>(low_number));
            Protocol::AppendFloat(expected, static_cast<float>(high_number));
        } else if (floating) {
            Protocol::AppendDouble(expected, low_number);
            Protocol::AppendDouble(expected, high_number);
        } else if (kind == ColumnKind::kUnsigned) {
            Protocol::AppendInteger(expected, static_cast<std::uint64_t>(low));
            Protocol::AppendInteger(expected, static_cast<std::uint64_t>(high));
        } else {
            Protocol::AppendInteger(expected, static_cast<std::int64_t>(low));
            Protocol::AppendInteger(expected, static_cast<std::int64_t>(high));
        }
        // Sums of the narrow integer columns stay in the i64 range.
        if (!floating && fixed.width <= 33U) {
            functions.push_back({Kind::kSum, schema_column.name});
            Protocol::AppendInteger(expected, static_cast<std::int64_t>(sum));
        }
    }
    Aggregator aggregator(layout, functions);
    aggregator.Add(payload, presence);
    assert(aggregator.blocks() == kBlocks - 1U);
    std::string out;
    aggregator.AppendResults(out);
    Same(out, expected);
}

std::string U64Frame(std::uint64_t value) {
    std::string frame(8U, '\0');
    for (std::size_t i = 0U; i < 8U; ++i) frame[i] = static_cast<char>((value >> (8U * i)) & 0xFFU);
    return frame;
}

// WHERE aggregates only the present blocks for which it is TRUE; NULL and
// false skip a block. It reads block and chunk columns, positions, EXISTS
// and NOW().
void Where() {
    Fixture f;
    CreateIntegers(f);
    f.Ok("SET BLOCK 1 0 IN t name = 'b'");
    f.Ok("SET BLOCK 2 1 IN t v = 8, name = 'c'");
    f.Ok("SET CHUNK 1 0 IN t META tag = 3");
    const std::string area = "AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*), SUM(v)";
    Same(f.Run(area + " WHERE v >= 2"), Map({{"count(*)", UInt(3U)}, {"sum(v)", Int(14)}}));
    Same(f.Run(area + " WHERE v >= 2 BY CHUNK"), Rows({{Int(0), Int(0), UInt(1U), Int(2)}, {Int(1), Int(0), UInt(2U), Int(12)}}));
    // s is NULL in two blocks: s > 0 is unknown there, NOT (s > 0) too.
    Same(f.Run(area + " WHERE s > 0"), Map({{"count(*)", UInt(1U)}, {"sum(v)", Int(4)}}));
    Same(f.Run(area + " WHERE NOT (s > 0)"), Map({{"count(*)", UInt(1U)}, {"sum(v)", Int(1)}}));
    Same(f.Run(area + " WHERE s IS NULL"), Map({{"count(*)", UInt(2U)}, {"sum(v)", Int(10)}}));
    Same(f.Run(area + " WHERE s > 0 OR s IS NULL"), Map({{"count(*)", UInt(3U)}, {"sum(v)", Int(14)}}));
    // Text values come with the chunk only when WHERE reads them.
    Same(f.Run(area + " WHERE name >= 'b'"), Map({{"count(*)", UInt(2U)}, {"sum(v)", Int(10)}}));
    Same(f.Run(area + " WHERE chunk.tag = 3"), Map({{"count(*)", UInt(2U)}, {"sum(v)", Int(12)}}));
    Same(f.Run(area + " WHERE chunk.tag = 7 AND POS_X = 1 AND POS_Y = 0"), Map({{"count(*)", UInt(1U)}, {"sum(v)", Int(2)}}));
    Same(f.Run(area + " WHERE EXISTS AND NOW() > 0"), Map({{"count(*)", UInt(4U)}, {"sum(v)", Int(15)}}));
    // Chunks without a matching block have no row and no value.
    Same(f.Run(area + " WHERE v > 100 BY CHUNK"), Rows({}));
    Same(f.Run(area + " WHERE v > 100"), Map({{"count(*)", UInt(0U)}, {"sum(v)", Null()}}));
    Same(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t MIN(s), AVG(v) WHERE v / 2 = 1"),
        Map({{"min(s)", Null()}, {"avg(v)", Double(2.0)}}));
    // A parameter takes the type of the column it meets, as an 8-byte
    // integer; a NULL frame makes every comparison unknown.
    const std::vector<std::optional<std::string>> two{U64Frame(2U)};
    Same(f.engine->Execute(f.session, area + " WHERE v > $1\r\n", two), Map({{"count(*)", UInt(2U)}, {"sum(v)", Int(12)}}));
    const std::vector<std::optional<std::string>> null{std::nullopt};
    Same(f.engine->Execute(f.session, area + " WHERE v > $1\r\n", null), Map({{"count(*)", UInt(0U)}, {"sum(v)", Null()}}));
    const std::vector<std::optional<std::string>> text{std::string("c")};
    Same(f.engine->Execute(f.session, area + " WHERE name = $1\r\n", text), Map({{"count(*)", UInt(1U)}, {"sum(v)", Int(8)}}));
    const auto plan = f.engine->PlanPayload(f.session, area + " WHERE v > $1 AND name < $2\r\n");
    assert(plan.plan == CommandEngine::PayloadPlan::kParameters);
    assert((plan.parameter_limits == std::vector<std::size_t>{8U, 8U}));

    f.Ok("CREATE TABLE cube (v i8) CHUNK 2 x 2 x 2");
    f.Ok("SET BLOCK 0 0 0 IN cube v = -3");
    f.Ok("SET BLOCK 3 0 1 IN cube v = 5");
    Same(f.Run("AGGREGATE AREA 0 0 0 TO 1 0 0 FROM cube SUM(v) WHERE POS_Z = 1 AND POS_X = 3"), Map({{"sum(v)", Int(5)}}));
}

// Each refusal of WHERE, before any chunk is read or as it is evaluated.
void WhereRefusals() {
    Fixture f;
    CreateIntegers(f);
    const std::string head = "AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*) WHERE ";
    const auto missing = f.Run(head + "w > 1");
    Error(missing, "INVALID_ARGUMENT", "the table has no column w");
    assert(missing.find("DESCRIBE") != std::string::npos);
    Error(f.Run(head + "chunk.v = 1"), "INVALID_ARGUMENT", "the table has no chunk column v");
    Error(f.Run(head + "name > 1"), "INVALID_ARGUMENT");
    Error(f.Run(head + "v + 1"), "INVALID_ARGUMENT");
    Error(f.Run(head + "v > 1 WHERE v < 3"), "SYNTAX", "WHERE is given twice");
    Error(f.Run(head + "v >"), "SYNTAX");
    // Evaluation: division by zero and integers beyond 128 bits.
    Error(f.Run(head + "v / 0 = 1"), "INVALID_ARGUMENT");
    Error(f.Run(head + "big * big * big > 0"), "OUT_OF_RANGE");
    // A parameter needs a column to take its type from, and its frame must
    // have that type's size.
    const auto untyped = f.engine->PlanPayload(f.session, head + "$1 < $2\r\n");
    assert(untyped.plan == CommandEngine::PayloadPlan::kDiscard);
    assert(untyped.reject_response.starts_with("-ERR INVALID_ARGUMENT "));
    const std::vector<std::optional<std::string>> frames{std::string("a"), std::string("b")};
    Error(f.engine->Execute(f.session, head + "$1 < $2\r\n", frames), "INVALID_ARGUMENT");
    const std::vector<std::optional<std::string>> short_frame{std::string("ab")};
    Error(f.engine->Execute(f.session, head + "v = $1\r\n", short_frame), "INVALID_ARGUMENT", "must be 8 bytes");
    const auto unknown = f.engine->PlanPayload(f.session, head + "w = $1\r\n");
    assert(unknown.plan == CommandEngine::PayloadPlan::kDiscard);
    Error(unknown.reject_response, "INVALID_ARGUMENT", "the table has no column w");
    const auto absent = f.engine->PlanPayload(f.session, "AGGREGATE CHUNK 0 0 FROM missing COUNT(*) WHERE v = $1\r\n");
    assert(absent.plan == CommandEngine::PayloadPlan::kDiscard && absent.reject_response.starts_with("-ERR NO_TABLE "));
}

// In a transaction WHERE reads the snapshot and private writes; every
// requested cell joins the read set, also when no block matches, since a
// write to any block could change the result.
void WhereTransactions() {
    Fixture f;
    CreateIntegers(f);
    f.Ok("BEGIN");
    Same(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*) WHERE v > 100"), Map({{"count(*)", UInt(0U)}}));
    assert(f.session.transaction->read_set.size() == 2U);
    assert(f.Run("SET BLOCK 0 1 IN t v = 200") == "_\r\n");
    Same(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*), SUM(v) WHERE v > 100"), Map({{"count(*)", UInt(1U)}, {"sum(v)", Int(200)}}));
    assert(f.RunOther("SET BLOCK 3 1 IN t v = 150").starts_with(":"));
    Same(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*), SUM(v) WHERE v > 100"), Map({{"count(*)", UInt(1U)}, {"sum(v)", Int(200)}}));
    Error(f.Run("COMMIT"), "CONFLICT");
    // A refused WHERE adds nothing to the read set.
    f.Ok("BEGIN");
    Error(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*) WHERE v / 0 = 1"), "INVALID_ARGUMENT");
    Error(f.Run("AGGREGATE AREA 0 0 TO 1 0 FROM t COUNT(*) WHERE w = 1"), "INVALID_ARGUMENT");
    assert(f.session.transaction->read_set.empty());
    f.Ok("ROLLBACK");
}

// The block loop of WHERE stops a cancelled statement.
void WhereCancellation() {
    TableSchema schema;
    schema.columns = {Column{.id = 1U, .name = "v", .type = {ColumnKind::kSigned, 8U}, .nullable = false,
        .default_value = {}}};
    schema.next_column_id = 2U;
    GeometryConfig config;
    config.chunk_width_blocks = config.chunk_height_blocks = 2U;
    config.block_bits = 8U;
    const Geometry geometry(config, schema);
    const auto where = expr::ParseCondition("v >= 0").expression;
    BlockFilter filter(geometry, where);
    filter.Bind({}, 0U);
    std::vector<std::uint8_t> payload(geometry.layout().payload_bytes(), 0U);
    payload[1] = 0xFF;
    std::vector<std::uint8_t> presence{0x0F};
    assert(filter.Apply(ChunkCoord{}, payload, presence, ChunkVars{}, nullptr) == 3U && presence[0] == 0x0D);
    StatementControl control(StatementControl::Clock::now(), 0U);
    assert(control.Start());
    ScopedStatementControl context(&control);
    assert(control.RequestStop(StatementStopReason::kCancelled));
    bool stopped = false;
    try { (void)filter.Apply(ChunkCoord{}, payload, presence, ChunkVars{}, nullptr); }
    catch (const StatementStoppedError&) { stopped = true; }
    assert(stopped);
}

}  // namespace

int main() {
    IntegersAndNulls();
    Floats();
    ByChunk();
    Refusals();
    Bounds();
    Transactions();
    MetaOnlyChunk();
    Cancellation();
    ValuesInPlace();
    Where();
    WhereRefusals();
    WhereTransactions();
    WhereCancellation();
    return 0;
}
