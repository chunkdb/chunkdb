// The BLOCKS selection of GET, AGGREGATE and SCAN CHUNKS, WHERE in GET AREA
// and GET BLOCKS, and AREA, AROUND and BLOCKS in SCAN CHUNKS (#145,
// docs/DESIGN-2.0.md E4 and E5): forms hold only the blocks that stay, and
// chunks where none stays are left out.

#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "block_selection.hpp"
#include "chunk_store_internal.hpp"
#include "statement_control.hpp"
#include "typed_chunk_form.hpp"
#include "test_utils.hpp"

namespace {

using namespace chunkdb;
using Parameters = std::vector<std::optional<std::string>>;

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-block-selection"};
    std::shared_ptr<TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    SessionState session;
    Fixture() {
        CatalogConfig config;
        config.data_dir = directory.path();
        config.background_maintenance = false;
        catalog = std::make_shared<TableCatalog>(config);
        engine = std::make_unique<CommandEngine>(EngineConfig{.require_auth = false}, catalog);
        assert(engine->Execute(session, "HELLO 3\r\n").starts_with("%"));
    }
    std::string Run(std::string_view statement, const Parameters& parameters = {}) {
        return engine->Execute(session, std::string(statement) + "\r\n", parameters);
    }
    void Ok(std::string_view statement) {
        const auto reply = Run(statement);
        if (reply.starts_with("-")) {
            std::fprintf(stderr, "%s: %s", std::string(statement).c_str(), reply.c_str());
            assert(false);
        }
    }
};

// An integer parameter: eight bytes, little-endian, whatever the column width.
std::string Le(std::uint64_t value) {
    std::string out;
    for (std::size_t i = 0; i < 8U; ++i) out.push_back(static_cast<char>((value >> (8U * i)) & 0xffU));
    return out;
}

void Error(const std::string& reply, std::string_view code, std::string_view message = {}) {
    const auto prefix = "-ERR " + std::string(code) + ' ';
    if (!reply.starts_with(prefix) || reply.find(message, prefix.size()) == std::string::npos) {
        std::fprintf(stderr, "expected error %s with '%s', got '%s'\n",
            std::string(code).c_str(), std::string(message).c_str(), reply.c_str());
        assert(false);
    }
}

// One row of an unpaged GET AREA reply: the chunk, its present blocks (local
// indexes) and the text and bytes values its form holds.
struct Row {
    ChunkCoord coord;
    std::set<std::size_t> blocks;
    std::size_t vars = 0;
};

std::size_t Number(const std::string& reply, std::size_t* at) {
    std::size_t value = 0;
    const auto parsed = std::from_chars(reply.data() + *at, reply.data() + reply.size(), value);
    assert(parsed.ec == std::errc{});
    *at = static_cast<std::size_t>(parsed.ptr - reply.data()) + 2U;
    return value;
}

std::int64_t Integer(const std::string& reply, std::size_t* at) {
    assert(reply[*at] == ':');
    std::int64_t value = 0;
    const auto parsed = std::from_chars(reply.data() + *at + 1U, reply.data() + reply.size(), value);
    assert(parsed.ec == std::errc{});
    *at = static_cast<std::size_t>(parsed.ptr - reply.data()) + 2U;
    return value;
}

std::vector<Row> Rows(Fixture& f, const std::string& reply, std::string_view table) {
    if (!reply.starts_with("*")) {
        std::fprintf(stderr, "expected area rows, got '%s'\n", reply.c_str());
        assert(false);
    }
    const auto geometry = f.catalog->Require(table)->geometry();
    std::size_t at = 1U;
    const std::size_t count = Number(reply, &at);
    std::vector<Row> rows;
    for (std::size_t i = 0; i < count; ++i) {
        assert(reply[at] == '*');
        ++at;
        (void)Number(reply, &at);
        Row row;
        row.coord.x = Integer(reply, &at);
        row.coord.y = Integer(reply, &at);
        if (geometry.dims() == 3U) row.coord.z = Integer(reply, &at);
        assert(reply[at] == '$');
        ++at;
        const std::size_t length = Number(reply, &at);
        const auto state = DecodeSectionedChunkState(geometry,
            std::span(reinterpret_cast<const std::uint8_t*>(reply.data() + at), length), kVarMaxChunkBytesLimit);
        at += length + 2U;
        for (std::size_t block = 0; block < geometry.layout().block_count(); ++block)
            if (BlockPresent(state.presence_bitmap, block)) row.blocks.insert(block);
        row.vars = state.vars.size();
        rows.push_back(std::move(row));
    }
    assert(at == reply.size());
    return rows;
}

// A 4 x 4 chunk table: blocks x 0..7, y 0..3 with n = x and a name, one
// block at (12, 0) and one at (-1, -1).
void Fill(Fixture& f) {
    f.Ok("CREATE TABLE t (n u8, name text(4) NULL) CHUNK 4 x 4");
    for (int x = 0; x < 8; ++x)
        for (int y = 0; y < 4; ++y)
            f.Ok("SET BLOCK " + std::to_string(x) + " " + std::to_string(y) + " IN t n = " + std::to_string(x) +
                 ", name = 'b'");
    f.Ok("SET BLOCK 12 0 IN t n = 12, name = 'c'");
    f.Ok("SET BLOCK -1 -1 IN t n = 1");
}

// Local index of block (x, y) of a 4 x 4 chunk.
std::size_t At(std::size_t x, std::size_t y) { return x + 4U * y; }

void TestGetBlocks() {
    Fixture f;
    Fill(f);
    auto rows = Rows(f, f.Run("GET BLOCKS 2 1 TO 5 2 FROM t"), "t");
    assert(rows.size() == 2U);
    assert(rows[0].coord == (ChunkCoord{0, 0, 0}) && rows[1].coord == (ChunkCoord{1, 0, 0}));
    assert((rows[0].blocks == std::set<std::size_t>{At(2, 1), At(3, 1), At(2, 2), At(3, 2)}));
    assert((rows[1].blocks == std::set<std::size_t>{At(0, 1), At(1, 1), At(0, 2), At(1, 2)}));
    // Only the kept blocks' text values stay in the form.
    assert(rows[0].vars == 4U && rows[1].vars == 4U);

    // A chunk with no block in the box is left out; negative coordinates.
    rows = Rows(f, f.Run("GET BLOCKS -1 -1 TO 0 0 FROM t"), "t");
    assert(rows.size() == 2U && rows[0].coord == (ChunkCoord{-1, -1, 0}));
    assert((rows[0].blocks == std::set<std::size_t>{At(3, 3)}) && (rows[1].blocks == std::set<std::size_t>{At(0, 0)}));
    assert(Rows(f, f.Run("GET BLOCKS 8 0 TO 11 3 FROM t"), "t").empty());
    assert(Rows(f, f.Run("GET BLOCKS 13 0 TO 15 3 FROM t"), "t").empty());

    // WHERE, with BLOCKS and with AREA; a parameter takes the column's type.
    rows = Rows(f, f.Run("GET BLOCKS 0 0 TO 12 0 FROM t WHERE n >= $1", {Le(6U)}), "t");
    assert(rows.size() == 2U && (rows[0].blocks == std::set<std::size_t>{At(2, 0), At(3, 0)}));
    assert(rows[1].coord == (ChunkCoord{3, 0, 0}) && (rows[1].blocks == std::set<std::size_t>{At(0, 0)}));
    rows = Rows(f, f.Run("GET AREA 0 0 TO 3 0 FROM t WHERE n = 5 OR name = 'c'"), "t");
    assert(rows.size() == 2U && rows[0].coord == (ChunkCoord{1, 0, 0}) && rows[0].blocks.size() == 4U);
    assert(rows[0].vars == 4U && rows[1].coord == (ChunkCoord{3, 0, 0}));
    assert(Rows(f, f.Run("GET AREA AROUND 0 0 RADIUS 5 FROM t WHERE n > 100"), "t").empty());
    // Without WHERE GET AREA keeps every block.
    rows = Rows(f, f.Run("GET AREA 0 0 TO 0 0 FROM t"), "t");
    assert(rows.size() == 1U && rows[0].blocks.size() == 16U);

    Error(f.Run("GET BLOCKS 5 5 TO 0 0 FROM t"), "INVALID_ARGUMENT", "block range corners are reversed");
    Error(f.Run("GET BLOCKS 0 0 0 TO 1 1 1 FROM t"), "INVALID_ARGUMENT");
    Error(f.Run("GET BLOCKS 0 0 TO 1 1 FROM t WHERE nope = 1"), "INVALID_ARGUMENT");
}

void TestAggregateBlocks() {
    Fixture f;
    Fill(f);
    auto reply = f.Run("AGGREGATE BLOCKS 2 1 TO 5 2 FROM t COUNT(*), SUM(n)");
    assert(reply == "%2\r\n$8\r\ncount(*)\r\n:8\r\n$6\r\nsum(n)\r\n:28\r\n");
    reply = f.Run("AGGREGATE BLOCKS 2 1 TO 12 2 FROM t COUNT(*) WHERE n > 4 BY CHUNK");
    assert(reply == "*1\r\n*3\r\n:1\r\n:0\r\n:6\r\n");
    reply = f.Run("AGGREGATE BLOCKS 8 0 TO 11 3 FROM t COUNT(*)");
    assert(reply == "%1\r\n$8\r\ncount(*)\r\n:0\r\n");
}

struct ScanPage {
    std::vector<ChunkCoord> coords{};
    std::vector<std::optional<std::string>> forms{};
    std::string after{};
};

// The rows, optional data fields and continuation of a selected 2D scan.
ScanPage Scan(const std::string& reply) {
    assert(reply.starts_with("%6\r\n$6\r\nchunks\r\n*"));
    std::size_t at = std::string_view("%6\r\n$6\r\nchunks\r\n*").size();
    const std::size_t count = Number(reply, &at);
    ScanPage page;
    for (std::size_t i = 0; i < count; ++i) {
        assert(reply[at] == '*');
        ++at;
        const std::size_t items = Number(reply, &at);
        assert(items == 3U || items == 4U);
        ChunkCoord coord{};
        coord.x = Integer(reply, &at);
        coord.y = Integer(reply, &at);
        (void)Integer(reply, &at);
        if (items == 4U) {
            if (std::string_view(reply).substr(at).starts_with("_\r\n")) {
                page.forms.push_back(std::nullopt);
                at += 3U;
            } else {
                assert(reply[at] == '$');
                ++at;
                const auto length = Number(reply, &at);
                page.forms.emplace_back(reply.substr(at, length));
                at += length + 2U;
            }
        }
        page.coords.push_back(coord);
    }
    constexpr std::string_view more_key = "$4\r\nmore\r\n";
    assert(std::string_view(reply).substr(at).starts_with(more_key));
    at += more_key.size();
    const auto more = std::string_view(reply).substr(at, 4U);
    assert(more == "#t\r\n" || more == "#f\r\n");
    at += more.size();
    constexpr std::string_view after_key = "$5\r\nafter\r\n";
    assert(std::string_view(reply).substr(at).starts_with(after_key));
    at += after_key.size();
    const auto epoch = reply.find("$5\r\nepoch\r\n", at);
    assert(epoch != std::string::npos);
    page.after = reply.substr(at, epoch - at);
    assert((page.after == "_\r\n") == (more == "#f\r\n"));
    return page;
}

void TestScanSelections() {
    Fixture f;
    Fill(f);
    auto page = Scan(f.Run("SCAN CHUNKS FROM t AREA 1 0 TO 3 0"));
    assert((page.coords == std::vector<ChunkCoord>{{1, 0, 0}, {3, 0, 0}}) && page.after == "_\r\n");
    page = Scan(f.Run("SCAN CHUNKS FROM t AREA AROUND 0 0 RADIUS 1"));
    assert((page.coords == std::vector<ChunkCoord>{{0, 0, 0}, {1, 0, 0}}));
    page = Scan(f.Run("SCAN CHUNKS FROM t BLOCKS 6 0 TO 13 0"));
    assert((page.coords == std::vector<ChunkCoord>{{1, 0, 0}, {3, 0, 0}}));
    page = Scan(f.Run("SCAN CHUNKS FROM t BLOCKS 8 0 TO 11 3"));
    assert(page.coords.empty());
    // Pages: LIMIT bounds the identities visited; continue AFTER `after`.
    page = Scan(f.Run("SCAN CHUNKS FROM t AREA -1 -1 TO 3 0 LIMIT 1"));
    assert((page.coords == std::vector<ChunkCoord>{{-1, -1, 0}}) && page.after == "*2\r\n:-1\r\n:-1\r\n");
    page = Scan(f.Run("SCAN CHUNKS FROM t AREA -1 -1 TO 3 0 AFTER -1 -1 LIMIT 2"));
    assert((page.coords == std::vector<ChunkCoord>{{0, 0, 0}, {1, 0, 0}}) && page.after == "*2\r\n:1\r\n:0\r\n");
    page = Scan(f.Run("SCAN CHUNKS FROM t AREA -1 -1 TO 3 0 AFTER 1 0 LIMIT 2"));
    assert((page.coords == std::vector<ChunkCoord>{{3, 0, 0}}) && page.after == "_\r\n");
    // The first candidate's x is in range, but its y is outside the area.
    page = Scan(f.Run("SCAN CHUNKS FROM t AREA -1 0 TO 3 0 LIMIT 1"));
    assert(page.coords.empty() && page.after == "*2\r\n:-1\r\n:-1\r\n");
    page = Scan(f.Run("SCAN CHUNKS FROM t AREA -1 0 TO 3 0 AFTER -1 -1 LIMIT 1"));
    assert((page.coords == std::vector<ChunkCoord>{{0, 0, 0}}) && page.after == "*2\r\n:0\r\n:0\r\n");
    // Without a selection the reply keeps its five keys.
    assert(f.Run("SCAN CHUNKS FROM t LIMIT 1").starts_with("%5\r\n"));
    Error(f.Run("SCAN CHUNKS FROM t AREA 3 0 TO 1 0"), "INVALID_ARGUMENT");
    Error(f.Run("SCAN CHUNKS FROM t BLOCKS 0 0 0 TO 1 1 1"), "INVALID_ARGUMENT");
}

void TestScanBlockData() {
    Fixture f;
    Fill(f);
    const auto write = f.Run("SET BLOCK 6 0 IN t n = 6, name = 'keep'");
    std::size_t at = 0U;
    const auto revision = Integer(write, &at);
    const std::string selection = "SCAN CHUNKS FROM t BLOCKS 6 0 TO 6 0";
    auto page = Scan(f.Run("SCAN CHUNKS FROM t BLOCKS 6 0 TO 7 0 WITH DATA WHERE n = 6"));
    assert((page.coords == std::vector<ChunkCoord>{{1, 0, 0}}));
    assert(page.forms.size() == 1U && page.forms.front());
    const auto& form = *page.forms.front();
    const auto geometry = f.catalog->Require("t")->geometry();
    const auto state = DecodeSectionedChunkState(geometry,
        std::span(reinterpret_cast<const std::uint8_t*>(form.data()), form.size()), kVarMaxChunkBytesLimit);
    constexpr std::uint32_t block = 2U; // Local coordinates (2, 0).
    assert(PresentBlockCount(state.presence_bitmap) == 1U && BlockPresent(state.presence_bitmap, block));
    assert(state.vars.size() == 1U);
    const auto name = state.vars.Find({geometry.layout().schema().columns[1].id, block});
    assert(name && std::string_view(reinterpret_cast<const char*>(name->data()), name->size()) == "keep");

    f.Ok("DELETE BLOCK 6 0 FROM t");
    assert(f.Run("GET BLOCK 7 0 FROM t").starts_with("*"));
    assert(Scan(f.Run(selection + " WITH DATA")).coords.empty());
    page = Scan(f.Run(selection + " SINCE " + std::to_string(revision) + " WITH DATA"));
    assert((page.coords == std::vector<ChunkCoord>{{1, 0, 0}}));
    assert(page.forms.size() == 1U && !page.forms.front());
}

std::size_t clip_clock_reads = 0U;

StatementControl::Clock::time_point ClipNow() noexcept {
    // Start and the first poll succeed. The next poll crosses the deadline.
    return StatementControl::Clock::time_point(std::chrono::milliseconds(++clip_clock_reads < 3U ? 0 : 1));
}

void TestClipCancellation() {
    const Geometry geometry(GeometryConfig{.chunk_width_blocks = 512U, .chunk_height_blocks = 512U});
    const BlockBox box{.last = {510, 511, 0}};
    std::vector<std::uint8_t> presence((geometry.ChunkBlockCount() + 7U) / 8U, 0U);
    SetBlockPresent(&presence, geometry.ChunkBlockCount() - 1U, true);
    clip_clock_reads = 0U;
    StatementControl control(StatementControl::Clock::time_point{}, 1U, &ClipNow);
    assert(control.Start());
    const ScopedStatementControl scope(&control);
    try {
        (void)ClipToBox(geometry, box, {}, presence);
        assert(false);
    } catch (const StatementStoppedError& error) {
        assert(error.reason() == StatementStopReason::kTimeout);
    }
}

void TestThreeDimensions() {
    Fixture f;
    f.Ok("CREATE TABLE c (n u8) CHUNK 2 x 2 x 2");
    f.Ok("SET BLOCK 0 0 0 IN c n = 1");
    f.Ok("SET BLOCK 0 0 1 IN c n = 2");
    f.Ok("SET BLOCK 1 1 1 IN c n = 3");
    f.Ok("SET BLOCK 0 0 2 IN c n = 4");
    auto rows = Rows(f, f.Run("GET BLOCKS 0 0 0 TO 0 0 1 FROM c"), "c");
    assert(rows.size() == 1U && (rows[0].blocks == std::set<std::size_t>{0U, 4U}));
    rows = Rows(f, f.Run("GET BLOCKS 0 0 1 TO 1 1 2 FROM c WHERE n > 2"), "c");
    assert(rows.size() == 2U && (rows[0].blocks == std::set<std::size_t>{7U}) && rows[1].coord == (ChunkCoord{0, 0, 1}));
    assert(f.Run("AGGREGATE BLOCKS 0 0 0 TO 1 1 2 FROM c SUM(n)") == "%1\r\n$6\r\nsum(n)\r\n:10\r\n");
    Error(f.Run("GET BLOCKS 0 0 TO 1 1 FROM c"), "INVALID_ARGUMENT");
}

}  // namespace

int main() {
    TestGetBlocks();
    TestAggregateBlocks();
    TestScanSelections();
    TestScanBlockData();
    TestClipCancellation();
    TestThreeDimensions();
    return 0;
}
