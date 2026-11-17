#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/table_catalog.hpp"
#include "sectioned_chunk_form.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-area-paging"};
    std::shared_ptr<TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    SessionState session;
    explicit Fixture(QueryLimits limits = {}) {
        CatalogConfig config;
        config.data_dir = directory.path();
        config.background_maintenance = false;
        config.default_options.durability_mode = DurabilityMode::kRelaxed;
        config.query_limits = limits;
        catalog = std::make_shared<TableCatalog>(config);
        engine = std::make_unique<CommandEngine>(EngineConfig{.require_auth = false}, catalog);
        assert(Run("HELLO 3").starts_with("%"));
    }
    std::string Run(std::string_view command) {
        return engine->Execute(session, std::string(command) + "\r\n");
    }
    void Create(bool three = false, std::string_view columns = "v u8 DEFAULT 0") {
        assert(Run("CREATE TABLE t (" + std::string(columns) + ") CHUNK 1 x 1" +
            (three ? " x 1" : "") + " CHUNK COLUMNS (tag u8 DEFAULT 7)") == "+OK\r\n");
    }
    void Write(std::string_view coord, unsigned value = 3U) {
        assert(Run("SET BLOCK " + std::string(coord) + " IN t v=" + std::to_string(value)).starts_with(":"));
    }
};

using Row = std::pair<ChunkCoord, std::string>;
std::string Expected(const std::vector<Row>& rows, std::optional<ChunkCoord> after,
    std::uint8_t dims = 2U, bool paged = true, bool schema = false) {
    std::string result;
    if (schema) { Protocol::AppendArrayHeader(result, 2U); Protocol::AppendInteger(result, UINT64_C(1)); }
    if (paged) { Protocol::AppendMapHeader(result, 3U); Protocol::AppendBulk(result, "chunks"); }
    Protocol::AppendArrayHeader(result, rows.size());
    for (const auto& [coord, form] : rows) {
        Protocol::AppendArrayHeader(result, static_cast<std::size_t>(dims) + 1U);
        Protocol::AppendInteger(result, coord.x); Protocol::AppendInteger(result, coord.y);
        if (dims == 3U) Protocol::AppendInteger(result, coord.z);
        result += form;
    }
    if (paged) {
        Protocol::AppendBulk(result, "more"); Protocol::AppendBoolean(result, after.has_value());
        Protocol::AppendBulk(result, "after");
        if (!after) Protocol::AppendNull(result);
        else {
            Protocol::AppendArrayHeader(result, dims);
            Protocol::AppendInteger(result, after->x); Protocol::AppendInteger(result, after->y);
            if (dims == 3U) Protocol::AppendInteger(result, after->z);
        }
    }
    return result;
}

void Error(const std::string& reply, std::string_view code) {
    assert(reply.starts_with("-ERR " + std::string(code) + " "));
}

void EmptyProgressAndCompatibility() {
    Fixture f;
    f.Create(); f.Write("5 0");
    const auto form = f.Run("GET CHUNK 5 0 FROM t");
    assert(f.Run("GET AREA 0 0 TO 5 0 FROM t LIMIT 2") == Expected({}, ChunkCoord{1, 0}));
    assert(f.Run("GET AREA 0 0 TO 5 0 FROM t AFTER 1 0 LIMIT 2") == Expected({}, ChunkCoord{3, 0}));
    assert(f.Run("GET AREA 0 0 TO 5 0 FROM t LIMIT 2 AFTER 3 0") == Expected({{{5, 0}, form}}, {}));
    assert(f.Run("GET AREA 0 0 TO 5 0 FROM t AFTER 5 0") == Expected({}, {}));
    assert(f.Run("GET AREA 0 0 TO 5 0 FROM t") == Expected({{{5, 0}, form}}, {}, 2U, false));
    assert(f.Run("GET AREA 0 0 TO 5 0 FROM t WITH SCHEMA LIMIT 6") == Expected({{{5, 0}, form}}, {}, 2U, true, true));
    Error(f.Run("GET AREA 0 0 TO 5 0 FROM t AFTER 6 0"), "INVALID_ARGUMENT");
    Error(f.Run("GET AREA 0 0 TO 5 0 FROM t LIMIT 0"), "INVALID_ARGUMENT");
    Error(f.Run("GET AREA 0 0 TO 5 0 FROM t LIMIT 4097"), "INVALID_ARGUMENT");
    Error(f.Run("GET AREA 0 0 TO 4096 0 FROM t"), "INVALID_ARGUMENT");
    assert(f.Run("GET AREA 0 0 TO 4096 0 FROM t LIMIT 2") == Expected({}, ChunkCoord{1, 0}));
}

void ThreeDimensionsAndProjection() {
    Fixture f;
    f.Create(true, "v u8 DEFAULT 0, other u16 DEFAULT 9");
    f.Write("-1 0 0"); f.Write("0 0 0");
    const auto first = f.Run("GET CHUNK -1 0 0 FROM t COLUMNS v");
    const auto last = f.Run("GET CHUNK 0 0 0 FROM t COLUMNS v");
    assert(first.find("CKCF") != std::string::npos);
    assert(f.Run("GET AREA -1 0 0 TO 0 0 1 FROM t COLUMNS v LIMIT 2") ==
        Expected({{{-1, 0, 0}, first}}, ChunkCoord{-1, 0, 1}, 3U));
    assert(f.Run("GET AREA -1 0 0 TO 0 0 1 FROM t COLUMNS v AFTER -1 0 1 LIMIT 2") ==
        Expected({{{0, 0, 0}, last}}, {}, 3U));
    assert(f.Run("GET AREA AROUND 0 0 0 RADIUS 1 FROM t COLUMNS v LIMIT 1") ==
        Expected({{{-1, 0, 0}, first}}, ChunkCoord{-1, 0, 0}, 3U));
    Error(f.Run("GET AREA AROUND 0 0 0 RADIUS 1 FROM t AFTER 1 1 0 LIMIT 1"), "INVALID_ARGUMENT");
    const auto body = first.find("\r\n") + 2U;
    const auto bytes = std::span(reinterpret_cast<const std::uint8_t*>(first.data() + body), first.size() - body - 2U);
    const auto form = ParseChunkFormSections(bytes, bytes.size(), ChunkFormReadMode::kRead);
    assert(form.header.flags == kChunkFormProjected);
    const auto metadata = ParseChunkFormChunkColumns(form);
    assert(metadata.size() == 1U && !metadata[0].is_null && metadata[0].value.size() == 1U && metadata[0].value[0] == 7U);
}

void ExactResponseBounds() {
    Fixture wide;
    wide.Create();
    for (int x = 0; x < 10; ++x) wide.Write(std::to_string(x) + " 0");
    for (const bool schema : {false, true}) {
        const std::string command = "GET AREA 0 0 TO 9 0 FROM t" + std::string(schema ? " WITH SCHEMA" : "");
        const auto response = wide.Run(command);
        assert(response.starts_with(schema ? "*2\r\n:1\r\n*10\r\n" : "*10\r\n"));
        for (const bool exact : {true, false}) {
            QueryLimits limits;
            limits.max_area_response_bytes = response.size() - static_cast<std::size_t>(!exact);
            Fixture f(limits); f.Create();
            std::vector<Row> rows;
            for (int x = 0; x < 10; ++x) {
                f.Write(std::to_string(x) + " 0");
                rows.push_back({{x, 0}, f.Run("GET CHUNK " + std::to_string(x) + " 0 FROM t")});
            }
            const auto reply = f.Run(command);
            if (exact) assert(reply == Expected(rows, {}, 2U, false, schema));
            else {
                Error(reply, "OUT_OF_RANGE");
                assert(reply.find("response exceeds the " + std::to_string(limits.max_area_response_bytes) +
                    "-byte limit") != std::string::npos);
            }
        }
    }
    const auto one_page_bytes = Expected({{{0, 0}, wide.Run("GET CHUNK 0 0 FROM t")}}, ChunkCoord{0, 0}).size();
    Fixture f(QueryLimits{.max_area_response_bytes = one_page_bytes}); f.Create(); f.Write("0 0"); f.Write("1 0");
    const auto first = f.Run("GET CHUNK 0 0 FROM t");
    const auto last = f.Run("GET CHUNK 1 0 FROM t");
    assert(f.Run("GET AREA 0 0 TO 1 0 FROM t LIMIT 2") == Expected({{{0, 0}, first}}, ChunkCoord{0, 0}));
    assert(f.Run("GET AREA 0 0 TO 1 0 FROM t LIMIT 2 AFTER 0 0") == Expected({{{1, 0}, last}}, {}));
    for (const auto read_limit : {std::size_t{1}, std::size_t{2}}) {
        Fixture transaction(QueryLimits{.max_area_response_bytes = one_page_bytes, .txn_max_read_chunks = read_limit});
        transaction.Create(); transaction.Write("0 0"); transaction.Write("1 0");
        assert(transaction.Run("BEGIN") == "+OK\r\n");
        assert(transaction.Run("SET BLOCK 0 0 IN t v=9") == "_\r\n");
        const auto own = transaction.Run("GET CHUNK 0 0 FROM t");
        const auto reads = transaction.session.transaction->read_set;
        const auto bytes = transaction.session.transaction->bytes;
        const auto page = transaction.Run("GET AREA 0 0 TO 1 0 FROM t LIMIT 2");
        if (read_limit == 1U) {
            Error(page, "INVALID_ARGUMENT");
            assert(transaction.session.transaction->read_set == reads);
        } else {
            assert(page == Expected({{{0, 0}, own}}, ChunkCoord{0, 0}));
            assert(transaction.session.transaction->read_set.size() == 2U &&
                transaction.session.transaction->read_set.contains(ChunkCoord{1, 0}));
        }
        assert(transaction.session.transaction->bytes == bytes && transaction.session.transaction->writes.size() == 1U);
        assert(transaction.Run("GET CHUNK 0 0 FROM t") == own);
        assert(transaction.Run("COMMIT").starts_with(":"));
    }
    const auto empty_bytes = Expected({}, {}).size();
    Fixture empty(QueryLimits{.max_area_response_bytes = empty_bytes - 1U}); empty.Create();
    Error(empty.Run("GET AREA 0 0 TO 0 0 FROM t LIMIT 1"), "OUT_OF_RANGE");
    const auto cursor_bytes = Expected({}, ChunkCoord{9, 0}).size();
    Fixture cursor(QueryLimits{.max_area_response_bytes = cursor_bytes}); cursor.Create();
    Error(cursor.Run("GET AREA 0 0 TO 20 0 FROM t LIMIT 21"), "OUT_OF_RANGE");
    Error(cursor.Run("GET AREA 0 0 TO 20 0 FROM t AFTER 9 0 LIMIT 21"), "OUT_OF_RANGE");
    assert(cursor.Run("GET AREA 0 0 TO 10 0 FROM t LIMIT 11") == Expected({}, {}));
}

void ExtremeEmptySelectors() {
    constexpr auto minimum = std::numeric_limits<std::int64_t>::min();
    constexpr auto maximum = std::numeric_limits<std::int64_t>::max();
    for (const bool three : {false, true}) {
        Fixture f; f.Create(three);
        const std::string third = three ? " 0" : "";
        const auto dims = static_cast<std::uint8_t>(three ? 3U : 2U);
        const auto around = "GET AREA AROUND 0 0" + third + " RADIUS " + std::to_string(maximum) + " FROM t";
        assert(f.Run(around + " LIMIT 1") == Expected({}, ChunkCoord{-maximum, 0, 0}, dims));
        assert(f.Run(around + " AFTER " + std::to_string(maximum) + " 0" + third) == Expected({}, {}, dims));
        Error(f.Run(around), "INVALID_ARGUMENT");
        const auto range = "GET AREA " + std::to_string(minimum) + " " + std::to_string(minimum) + third +
            " TO " + std::to_string(maximum) + " " + std::to_string(maximum) + third + " FROM t LIMIT 1";
        assert(f.Run(range) == Expected({}, ChunkCoord{minimum, minimum, 0}, dims));
        assert(f.Run(range + " AFTER " + std::to_string(maximum) + " " + std::to_string(maximum) + third) == Expected({}, {}, dims));
    }
}

void TransactionRefusalsAreAtomic() {
    QueryLimits limits;
    limits.txn_max_written_chunks = 1U;
    limits.txn_max_read_chunks = 2U;
    Fixture f(limits); f.Create();
    assert(f.Run("BEGIN") == "+OK\r\n");
    assert(f.Run("SET BLOCK 0 0 IN t v=5") == "_\r\n");
    const auto bytes = f.session.transaction->bytes;
    const auto private_form = f.Run("GET CHUNK 0 0 FROM t");
    Error(f.Run("SET BLOCK 1 0 IN t v=6"), "INVALID_ARGUMENT");
    assert(f.session.transaction->bytes == bytes && f.session.transaction->writes.size() == 1U);
    assert(f.Run("GET CHUNK 0 0 FROM t") == private_form);
    const auto read_set = f.session.transaction->read_set;
    Error(f.Run("GET AREA 0 0 TO 2 0 FROM t LIMIT 3"), "INVALID_ARGUMENT");
    assert(f.session.transaction->read_set == read_set && f.session.transaction->writes.size() == 1U);
    assert(f.Run("GET AREA 0 0 TO 1 0 FROM t LIMIT 2") == Expected({{{0, 0}, private_form}}, {}));
    assert(f.session.transaction->read_set.size() == 2U && f.session.transaction->bytes == bytes);
    assert(f.Run("COMMIT").starts_with(":"));
    assert(f.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:5\r\n");
    assert(f.Run("GET BLOCK 1 0 FROM t") == "_\r\n");

    Fixture small(QueryLimits{.max_area_response_bytes = 80U}); small.Create();
    assert(small.Run("BEGIN") == "+OK\r\n");
    assert(small.Run("SET BLOCK 0 0 IN t v=8") == "_\r\n");
    const auto small_form = small.Run("GET CHUNK 0 0 FROM t");
    const auto small_reads = small.session.transaction->read_set;
    const auto small_bytes = small.session.transaction->bytes;
    Error(small.Run("GET AREA 0 0 TO 1 0 FROM t LIMIT 2"), "OUT_OF_RANGE");
    assert(small.session.transaction->read_set == small_reads && small.session.transaction->bytes == small_bytes &&
        small.session.transaction->writes.size() == 1U);
    assert(small.Run("GET CHUNK 0 0 FROM t") == small_form);
    assert(small.Run("COMMIT").starts_with(":"));
    assert(small.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:8\r\n");
}

// SET CHUNK in a transaction meets the configured written-chunk limit before
// its form is decoded: a form that decoding refuses gets the limit's error.
void SetChunkMeetsConfiguredWrittenLimitBeforeDecoding() {
    QueryLimits limits;
    limits.txn_max_written_chunks = 1U;
    Fixture f(limits); f.Create();
    f.Write("2 0", 6U);
    const auto reply = f.Run("GET CHUNK 2 0 FROM t");
    const auto body = reply.find("\r\n") + 2U;
    auto form = reply.substr(body, reply.size() - body - 2U);
    // A projected form without COLUMNS: its sections parse, decoding refuses it.
    form[6] = static_cast<char>(form[6] | static_cast<char>(kChunkFormProjected));
    const std::vector<std::optional<std::string>> parameters{form};
    const auto set = [&] { return f.engine->Execute(f.session, "SET CHUNK 2 0 IN t $1\r\n", parameters); };
    const auto malformed = set();
    Error(malformed, "INVALID_ARGUMENT");
    assert(malformed.find("projection") != std::string::npos);
    assert(f.Run("BEGIN") == "+OK\r\n");
    assert(f.Run("SET BLOCK 0 0 IN t v=5") == "_\r\n");
    const auto bytes = f.session.transaction->bytes;
    const auto over = set();
    Error(over, "INVALID_ARGUMENT");
    assert(over.find("a transaction writes at most 1 chunks") != std::string::npos);
    assert(f.session.transaction->bytes == bytes && f.session.transaction->writes.size() == 1U);
    assert(f.Run("COMMIT").starts_with(":"));
    assert(f.Run("GET BLOCK 0 0 FROM t") == "*1\r\n:5\r\n");
    assert(f.Run("GET BLOCK 2 0 FROM t") == "*1\r\n:6\r\n");
}

std::string Cell(std::int64_t x, bool three) { return std::to_string(x) + " 0" + (three ? " 0" : ""); }
ChunkCoord At(std::int64_t x) { return {x, 0, 0}; }
std::string Refusal(std::int64_t x, bool three, std::size_t required, std::size_t limit) {
    return "-ERR OUT_OF_RANGE reason=area_entry_too_large chunk=" + std::to_string(x) + (three ? ",0,0" : ",0") +
        " required=" + std::to_string(required) + " limit=" + std::to_string(limit) + " ";
}

// Visits a page at the store, refusing entries that `fits` rejects.
template <typename Fits>
ChunkAreaPage Visit(ChunkStore& store, bool three, std::int64_t last, std::optional<ChunkCoord> after,
    std::size_t limit, Fits fits, std::vector<ChunkCoord>* populated) {
    const ChunkAreaSelector selector{.dims = static_cast<std::uint8_t>(three ? 3U : 2U), .first = {0, 0, 0},
        .last = {last, 0, 0}};
    return store.VisitChunkArea(selector, after, limit,
        [&](const ChunkCoord& coord, std::optional<ChunkRangeEntry>&& entry, bool) {
            if (entry && !fits(*entry)) return false;
            if (entry) populated->push_back(coord);
            return true;
        });
}

template <typename Fits>
void VisitRefuses(ChunkStore& store, bool three, std::int64_t last, ChunkCoord after, std::size_t limit,
    Fits fits, ChunkCoord refused) {
    std::vector<ChunkCoord> populated;
    bool thrown = false;
    try { (void)Visit(store, three, last, after, limit, fits, &populated); }
    catch (const AreaEntryTooLargeError& error) {
        thrown = error.coord() == refused && error.dims() == (three ? 3U : 2U) &&
            error.limit() == store.query_limits().max_area_response_bytes && !error.required();
    }
    assert(thrown && populated.empty());
}

// A chunk with large variable values cannot fit an otherwise empty page. A
// page ends before it, the next page is refused with its coordinate and size,
// and the client continues AFTER it.
void OversizeEntryInPage(bool three) {
    const auto dims = static_cast<std::uint8_t>(three ? 3U : 2U);
    const std::string columns = "v u8 DEFAULT 0, s text(8192) NULL";
    std::size_t small = 0U;
    { Fixture wide; wide.Create(three, columns); wide.Write(Cell(0, three)); small = wide.Run("GET CHUNK " + Cell(0, three) + " FROM t").size(); }
    const auto limit = Expected({{At(0), std::string(small, 'x')}, {At(1), std::string(small, 'x')}}, At(1), dims).size() + 64U;
    const std::string big(4096, 'b');
    assert(limit < big.size());
    Fixture f(QueryLimits{.max_area_response_bytes = limit}); f.Create(three, columns);
    for (const int x : {0, 1, 3}) f.Write(Cell(x, three));
    assert(f.Run("SET BLOCK " + Cell(2, three) + " IN t v=3, s='" + big + "'").starts_with(":"));
    const auto form = [&](int x, std::string_view projection = "") {
        return f.Run("GET CHUNK " + Cell(x, three) + " FROM t" + std::string(projection));
    };
    const auto first = form(0), second = form(1), oversize = form(2), last = form(3);
    assert(oversize.find(big) != std::string::npos);
    const auto area = "GET AREA " + Cell(0, three) + " TO " + Cell(4, three) + " FROM t";
    const auto refusal = Refusal(2, three, Expected({{At(2), oversize}}, At(2), dims).size(), limit);

    assert(f.Run(area + " LIMIT 5") == Expected({{At(0), first}, {At(1), second}}, At(1), dims));
    const auto refused = f.Run(area + " AFTER " + Cell(1, three) + " LIMIT 5");
    assert(refused.starts_with(refusal) && refused.find("GET CHUNK") != std::string::npos);
    assert(f.Run(area + " AFTER " + Cell(1, three) + " LIMIT 1") == refused);
    assert(f.Run(area + " LIMIT 1") == Expected({{At(0), first}}, At(0), dims));
    assert(f.Run(area + " AFTER " + Cell(0, three) + " LIMIT 1") == Expected({{At(1), second}}, At(1), dims));
    assert(f.Run(area + " AFTER " + Cell(2, three) + " LIMIT 1") == Expected({{At(3), last}}, At(3), dims));
    assert(f.Run(area + " AFTER " + Cell(2, three) + " LIMIT 5") == Expected({{At(3), last}}, {}, dims));
    assert(f.Run(area + " COLUMNS v AFTER " + Cell(1, three) + " LIMIT 5") ==
        Expected({{At(2), form(2, " COLUMNS v")}, {At(3), form(3, " COLUMNS v")}}, {}, dims));
    assert(f.Run(area).starts_with(Refusal(2, three, Expected({{At(2), oversize}}, {}, dims, false).size(), limit)));

    assert(f.Run("BEGIN") == "+OK\r\n");
    assert(f.Run(area + " LIMIT 5") == Expected({{At(0), first}, {At(1), second}}, At(1), dims));
    const auto reads = f.session.transaction->read_set;
    assert(f.Run(area + " AFTER " + Cell(1, three) + " LIMIT 5") == refused);
    assert(f.session.transaction->read_set == reads);
    assert(f.Run("ROLLBACK") == "+OK\r\n");

    auto lease = f.catalog->Require("t")->Acquire();
    auto& store = lease->store();
    const auto fits = [](const ChunkRangeEntry& entry) { return entry.vars.encoded_size() < 4096U; };
    std::vector<ChunkCoord> populated;
    auto page = Visit(store, three, 4, std::nullopt, 5U, fits, &populated);
    assert(page.has_more && page.after == At(1) && populated == (std::vector{At(0), At(1)}));
    VisitRefuses(store, three, 4, At(1), 5U, fits, At(2));
    VisitRefuses(store, three, 4, At(1), 1U, fits, At(2));
    populated.clear();
    page = Visit(store, three, 4, At(2), 5U, fits, &populated);
    assert(!page.has_more && !page.after && populated == std::vector{At(3)});
}

// At the largest chunk geometry one complete chunk exceeds the default
// response bound: the page before it advances, the chunk is refused with
// its coordinate, and GET CHUNK or a projection reads it.
void LargestGeometry(bool three) {
    const auto dims = static_cast<std::uint8_t>(three ? 3U : 2U);
    Fixture f;
    assert(f.Run(std::string("CREATE TABLE t (v u8, padding bits(504)) CHUNK ") +
        (three ? "128 x 128 x 64" : "1024 x 1024")) == "+OK\r\n");
    f.Write(three ? "128 0 0" : "1024 0", 11U);
    const auto budget = QueryLimits{}.max_area_response_bytes;
    const auto full = f.Run("GET CHUNK " + Cell(1, three) + " FROM t");
    assert(full.size() > budget);
    const auto form = f.Run("GET CHUNK " + Cell(1, three) + " FROM t COLUMNS v");
    const auto area = "GET AREA " + Cell(0, three) + " TO " + Cell(2, three) + " FROM t";
    assert(f.Run(area).starts_with(Refusal(1, three, Expected({{At(1), full}}, {}, dims, false).size(), budget)));
    assert(f.Run(area + " LIMIT 3") == Expected({}, At(0), dims));
    const auto refused = f.Run(area + " AFTER " + Cell(0, three) + " LIMIT 3");
    assert(refused.starts_with(Refusal(1, three, Expected({{At(1), full}}, At(1), dims).size(), budget)));
    assert(f.Run(area + " AFTER " + Cell(0, three) + " LIMIT 1") == refused);
    assert(f.Run(area + " COLUMNS v AFTER " + Cell(0, three) + " LIMIT 1") == Expected({{At(1), form}}, At(1), dims));
    assert(f.Run(area + " COLUMNS v") == Expected({{At(1), form}}, {}, dims, false));
    assert(f.Run(area + " AFTER " + Cell(1, three) + " LIMIT 3") == Expected({}, {}, dims));
    assert(f.Run(std::string("GET BLOCK ") + (three ? "128 0 0" : "1024 0") + " FROM t COLUMNS v") == "*1\r\n:11\r\n");

    auto lease = f.catalog->Require("t")->Acquire();
    auto& store = lease->store();
    const auto bound = store.query_limits().max_area_response_bytes;
    const auto fits = [bound](const ChunkRangeEntry& entry) { return entry.payload.size() < bound; };
    std::vector<ChunkCoord> populated;
    auto page = Visit(store, three, 2, std::nullopt, 3U, fits, &populated);
    assert(page.has_more && page.after == At(0) && populated.empty());
    VisitRefuses(store, three, 2, At(0), 3U, fits, At(1));
    VisitRefuses(store, three, 2, At(0), 1U, fits, At(1));
    page = Visit(store, three, 2, At(1), 3U, fits, &populated);
    assert(!page.has_more && !page.after && populated.empty());
}
}  // namespace

int main() {
    EmptyProgressAndCompatibility();
    ThreeDimensionsAndProjection();
    ExactResponseBounds();
    ExtremeEmptySelectors();
    TransactionRefusalsAreAtomic();
    SetChunkMeetsConfiguredWrittenLimitBeforeDecoding();
    for (const bool three : {false, true}) {
        OversizeEntryInPage(three);
        LargestGeometry(three);
    }
}
