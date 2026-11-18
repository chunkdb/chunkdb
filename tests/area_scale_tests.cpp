#include <algorithm>
#include <bit>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
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
    test::ScopedTempDir directory{"chunkdb-area-scale"};
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
    void Ok(std::string_view command) {
        const auto reply = Run(command);
        assert(reply == "+OK\r\n" || reply.starts_with(":") || reply == "_\r\n");
    }
};

void Error(const std::string& reply, std::string_view code, std::string_view text = {}) {
    assert(reply.starts_with("-ERR " + std::string(code) + " "));
    assert(reply.find(text) != std::string::npos);
}

struct Entry {
    std::vector<std::int64_t> coord;
    std::string form;
};

// The entries of a plain GET AREA reply.
std::vector<Entry> Entries(const std::string& reply, std::size_t dims) {
    std::size_t at = 0U;
    const auto line = [&] {
        const auto end = reply.find("\r\n", at);
        assert(end != std::string::npos);
        auto text = reply.substr(at, end - at);
        at = end + 2U;
        return text;
    };
    const auto header = line();
    assert(header.starts_with("*"));
    std::vector<Entry> entries(std::stoull(header.substr(1)));
    for (auto& entry : entries) {
        assert(line() == "*" + std::to_string(dims + 1U));
        for (std::size_t axis = 0U; axis < dims; ++axis) {
            const auto value = line();
            assert(value.starts_with(":"));
            entry.coord.push_back(std::stoll(value.substr(1)));
        }
        const auto bulk = line();
        assert(bulk.starts_with("$"));
        const auto size = static_cast<std::size_t>(std::stoull(bulk.substr(1)));
        entry.form = reply.substr(at, size);
        at += size + 2U;
    }
    assert(at == reply.size());
    return entries;
}

std::span<const std::uint8_t> Bytes(const std::string& text) {
    return {reinterpret_cast<const std::uint8_t*>(text.data()), text.size()};
}

std::uint32_t Le32(std::span<const std::uint8_t> bytes, std::size_t at) {
    std::uint32_t value = 0U;
    for (unsigned i = 0U; i < 4U; ++i) value |= static_cast<std::uint32_t>(bytes[at + i]) << (8U * i);
    return value;
}

// An independent reader of a downsampled form; `bytes` must outlive it.
class Cells {
  public:
    Cells(const std::string& bytes, std::size_t count)
        : count_(count), form_(ParseChunkFormSections(Bytes(bytes), bytes.size(), ChunkFormReadMode::kRead)),
          columns_(ParseChunkFormBlockColumns(form_, count)) {}
    [[nodiscard]] const ChunkFormSections& form() const { return form_; }
    [[nodiscard]] std::span<const std::uint8_t> Section(ChunkFormSectionKind kind) const {
        for (const auto& section : form_.sections)
            if (section.kind == static_cast<std::uint16_t>(kind)) return section.body;
        return {};
    }
    [[nodiscard]] bool Present(std::size_t cell) const {
        return (Section(ChunkFormSectionKind::kPresence)[cell / 8U] & (1U << (cell % 8U))) != 0U;
    }
    [[nodiscard]] std::optional<std::uint64_t> Fixed(std::uint32_t id, std::size_t cell) const {
        for (const auto& column : columns_) {
            if (column.column_id != id) continue;
            if (!column.validity.empty() && (column.validity[cell / 8U] & (1U << (cell % 8U))) == 0U) return std::nullopt;
            const auto raw = ChunkFormRawBlockValue(column, count_, cell);
            std::uint64_t value = 0U;
            for (std::size_t i = 0U; i < raw.size(); ++i) value |= static_cast<std::uint64_t>(raw[i]) << (8U * i);
            return value;
        }
        assert(false && "column is not in the form");
        return std::nullopt;
    }
    [[nodiscard]] std::optional<std::string> Var(std::uint32_t id, std::size_t cell) const {
        const auto body = Section(ChunkFormSectionKind::kVariableValues);
        for (std::size_t at = 0U; at < body.size();) {
            const auto length = Le32(body, at + 8U);
            if (Le32(body, at) == id && Le32(body, at + 4U) == cell)
                return std::string(reinterpret_cast<const char*>(body.data() + at + 12U), length);
            at += 12U + length;
        }
        return std::nullopt;
    }

  private:
    std::size_t count_;
    ChunkFormSections form_;
    std::vector<ChunkFormBlockColumn> columns_;
};

std::optional<std::uint64_t> Signed(std::int64_t value, unsigned width) {
    return static_cast<std::uint64_t>(value) & ((std::uint64_t{1} << width) - 1U);
}
std::optional<std::uint64_t> Double(double value) { return std::bit_cast<std::uint64_t>(value); }
bool IsNaN(std::optional<std::uint64_t> raw) { return raw && std::isnan(std::bit_cast<double>(*raw)); }

void CreateTerrain(Fixture& f) {
    // IDs: v 1, h 2, f 3, b 4, label 5.
    f.Ok("CREATE TABLE t (v u8, h i16 NULL, f f64 NULL, b bits(3), label text(8) NULL) CHUNK 4 x 4"
         " CHUNK COLUMNS (tag u8 DEFAULT 7)");
    // Cell 0 holds blocks 1, 4 and 5; cell 1 is empty; cell 2 holds block 13;
    // cell 3 holds blocks 10, 11, 14 and 15.
    f.Ok("SET BLOCK 1 0 IN t v = 5, f = nan, b = b'101'");
    f.Ok("SET BLOCK 0 1 IN t v = 9, h = 2, f = 2.5, b = b'011'");
    f.Ok("SET BLOCK 1 1 IN t v = 5, h = -7, f = -1, b = b'011', label = 'c'");
    f.Ok("SET BLOCK 1 3 IN t v = 1");
    f.Ok("SET BLOCK 2 2 IN t v = 6, f = nan");
    f.Ok("SET BLOCK 3 2 IN t v = 4");
    f.Ok("SET BLOCK 2 3 IN t v = 4");
    f.Ok("SET BLOCK 3 3 IN t v = 6");
}

Entry One(Fixture& f, const std::string& tail) {
    const auto entries = Entries(f.Run("GET AREA 0 0 TO 0 0 FROM t " + tail), 2U);
    assert(entries.size() == 1U && entries[0].coord == (std::vector<std::int64_t>{0, 0}));
    return entries[0];
}

void Reductions() {
    Fixture f;
    CreateTerrain(f);
    const auto stored = One(f, "");
    const auto version = ParseChunkFormSections(Bytes(stored.form), stored.form.size(), ChunkFormReadMode::kRead).header.chunk_version;
    assert(version != 0U);
    const auto check_shape = [version](const Cells& cells, ChunkFormReduction reduction, bool projected) {
        assert(cells.form().header.flags == (projected ? kChunkFormProjected : 0U));
        assert(cells.form().header.chunk_version == version && cells.form().header.schema_version == 1U);
        const auto scale = cells.Section(ChunkFormSectionKind::kScale);
        assert(scale.size() == 8U && Le32(scale, 0U) == 2U && Le32(scale, 4U) == static_cast<std::uint32_t>(reduction));
        const auto metadata = ParseChunkFormChunkColumns(cells.form());
        assert(metadata.size() == 1U && metadata[0].value.size() == 1U && metadata[0].value[0] == 7U);
        assert(cells.Present(0U) && !cells.Present(1U) && cells.Present(2U) && cells.Present(3U));
        assert(cells.form().sections.back().kind == static_cast<std::uint16_t>(ChunkFormSectionKind::kScale));
    };

    const auto first = One(f, "SCALE 2 USING FIRST");
    {
        const Cells cells(first.form, 4U);
        check_shape(cells, ChunkFormReduction::kFirst, false);
        assert(cells.Fixed(1, 0) == 5U && !cells.Fixed(2, 0) && IsNaN(cells.Fixed(3, 0)) && cells.Fixed(4, 0) == 5U);
        assert(!cells.Var(5, 0));
        assert(cells.Fixed(1, 2) == 1U && !cells.Fixed(2, 2) && !cells.Fixed(3, 2) && cells.Fixed(4, 2) == 0U && !cells.Var(5, 2));
        assert(cells.Fixed(1, 3) == 6U && IsNaN(cells.Fixed(3, 3)));
    }
    const auto any = One(f, "SCALE 2 USING ANY");
    {
        const Cells cells(any.form, 4U);
        check_shape(cells, ChunkFormReduction::kAny, false);
        assert(cells.Fixed(1, 0) == 5U && cells.Fixed(2, 0) == Signed(2, 16) && IsNaN(cells.Fixed(3, 0)));
        assert(cells.Var(5, 0) == "c" && !cells.Var(5, 2) && !cells.Fixed(2, 3));
    }
    const auto max = One(f, "COLUMNS v, h, f SCALE 2 USING MAX");
    {
        const Cells cells(max.form, 4U);
        check_shape(cells, ChunkFormReduction::kMax, true);
        // NaN is skipped beside other values; NULL is skipped; signed order.
        assert(cells.Fixed(1, 0) == 9U && cells.Fixed(2, 0) == Signed(2, 16) && cells.Fixed(3, 0) == Double(2.5));
        assert(cells.Fixed(1, 2) == 1U && !cells.Fixed(2, 2) && !cells.Fixed(3, 2));
        // Only NaN among the values gives NaN.
        assert(cells.Fixed(1, 3) == 6U && IsNaN(cells.Fixed(3, 3)) && !cells.Fixed(2, 3));
    }
    const auto min = One(f, "COLUMNS f, v, h SCALE 2 USING MIN");
    {
        const Cells cells(min.form, 4U);
        check_shape(cells, ChunkFormReduction::kMin, true);
        assert(cells.Fixed(1, 0) == 5U && cells.Fixed(2, 0) == Signed(-7, 16) && cells.Fixed(3, 0) == Double(-1.0));
        assert(cells.Fixed(1, 3) == 4U && IsNaN(cells.Fixed(3, 3)));
    }
    const auto mode = One(f, "COLUMNS v, h, b SCALE 2 USING MODE");
    {
        const Cells cells(mode.form, 4U);
        check_shape(cells, ChunkFormReduction::kMode, true);
        // h ties 2 and -7, v ties 6 and 4 in cell 3: the value seen first wins.
        assert(cells.Fixed(1, 0) == 5U && cells.Fixed(2, 0) == Signed(2, 16) && cells.Fixed(4, 0) == 6U);
        assert(cells.Fixed(1, 3) == 6U && !cells.Fixed(2, 3) && cells.Fixed(4, 3) == 0U);
    }

    // A factor of the whole side gives one cell; 1 keeps every block.
    {
        const auto whole = One(f, "COLUMNS v SCALE 4 USING MAX");
        const Cells cells(whole.form, 1U);
        assert(cells.Present(0U) && cells.Fixed(1, 0) == 9U);
        const auto same = One(f, "COLUMNS v SCALE 1 USING MIN");
        const Cells blocks(same.form, 16U);
        const std::vector<std::size_t> present{1U, 4U, 5U, 10U, 11U, 13U, 14U, 15U};
        for (std::size_t block = 0U; block < 16U; ++block)
            assert(blocks.Present(block) == (std::find(present.begin(), present.end(), block) != present.end()));
        assert(blocks.Fixed(1, 4) == 9U && blocks.Fixed(1, 13) == 1U);
    }

    Error(f.Run("GET AREA 0 0 TO 0 0 FROM t SCALE 2 USING MAX"), "INVALID_ARGUMENT", "MAX does not accept column b");
    Error(f.Run("GET AREA 0 0 TO 0 0 FROM t COLUMNS label SCALE 2 USING MIN"), "INVALID_ARGUMENT", "column label");
    Error(f.Run("GET AREA 0 0 TO 0 0 FROM t COLUMNS f SCALE 2 USING MODE"), "INVALID_ARGUMENT", "MODE does not accept column f");
    Error(f.Run("GET AREA 0 0 TO 0 0 FROM t COLUMNS v SCALE 3 USING MAX"), "INVALID_ARGUMENT", "must divide every chunk side");
    Error(f.Run("GET AREA 0 0 TO 0 0 FROM t COLUMNS v SCALE 8 USING MAX"), "INVALID_ARGUMENT", "must divide every chunk side");
    Error(f.Run("GET AREA 0 0 TO 0 0 FROM t SCALE 0 USING MAX"), "SYNTAX");
    Error(f.Run("GET AREA 0 0 TO 0 0 FROM t SCALE 2 USING AVG"), "SYNTAX");
    Error(f.Run("GET AREA 0 0 TO 0 0 FROM t SCALE 2"), "SYNTAX");
    Error(f.Run("GET AREA 0 0 TO 0 0 FROM t SCALE 2 USING MAX SCALE 2 USING MIN"), "SYNTAX");

    // A downsampled form cannot be written back.
    const std::vector<std::optional<std::string>> parameters{first.form};
    Error(f.engine->Execute(f.session, "SET CHUNK 0 0 IN t $1\r\n", parameters), "INVALID_ARGUMENT", "downsampled");
}

void ThreeDimensionsAndPartialAreas() {
    Fixture f;
    f.Ok("CREATE TABLE t (v u8) CHUNK 2 x 2 x 2");
    f.Ok("SET BLOCK 1 1 0 IN t v = 8");
    f.Ok("SET BLOCK 0 0 1 IN t v = 3");
    f.Ok("SET BLOCK 4 0 0 IN t v = 1");
    // Three chunks with a factor of 2: each chunk is reduced by itself.
    for (const auto& [reduction, value] : std::vector<std::pair<std::string, std::uint64_t>>{
             {"FIRST", 8U}, {"ANY", 8U}, {"MAX", 8U}, {"MIN", 3U}, {"MODE", 8U}}) {
        const auto entries = Entries(f.Run("GET AREA 0 0 0 TO 2 0 0 FROM t SCALE 2 USING " + reduction), 3U);
        assert(entries.size() == 2U);
        assert(entries[0].coord == (std::vector<std::int64_t>{0, 0, 0}) && entries[1].coord == (std::vector<std::int64_t>{2, 0, 0}));
        const Cells first(entries[0].form, 1U), last(entries[1].form, 1U);
        assert(first.Present(0U) && first.Fixed(1, 0) == value && last.Fixed(1, 0) == 1U);
    }
    const auto around = Entries(f.Run("GET AREA AROUND 0 0 0 RADIUS 1 FROM t SCALE 2 USING MIN"), 3U);
    assert(around.size() == 1U && Cells(around[0].form, 1U).Fixed(1, 0) == 3U);

    f.Ok("CREATE TABLE cube (v u8) CHUNK 4 x 4 x 4");
    // Cells are 2 x 2 x 2 and indexed x, then y, then z.
    f.Ok("SET BLOCK 3 1 1 IN cube v = 11");
    f.Ok("SET BLOCK 0 2 0 IN cube v = 12");
    f.Ok("SET BLOCK 1 1 3 IN cube v = 14");
    const auto cube = Entries(f.Run("GET AREA -1 0 0 TO 0 0 0 FROM cube SCALE 2 USING FIRST"), 3U);
    assert(cube.size() == 1U);
    const Cells cells(cube[0].form, 8U);
    for (std::size_t cell = 0U; cell < 8U; ++cell) assert(cells.Present(cell) == (cell == 1U || cell == 2U || cell == 4U));
    assert(cells.Fixed(1, 1) == 11U && cells.Fixed(1, 2) == 12U && cells.Fixed(1, 4) == 14U);

    f.Ok("CREATE TABLE slab (v u8) CHUNK 4 x 4 x 2");
    f.Ok("SET BLOCK 0 0 0 IN slab v = 1");
    Error(f.Run("GET AREA 0 0 0 TO 0 0 0 FROM slab SCALE 4 USING MAX"), "INVALID_ARGUMENT", "(4 x 4 x 2)");
    assert(Entries(f.Run("GET AREA 0 0 0 TO 0 0 0 FROM slab SCALE 2 USING MAX"), 3U).size() == 1U);
    f.Ok("CREATE TABLE strip (v u8) CHUNK 6 x 4");
    Error(f.Run("GET AREA 0 0 TO 0 0 FROM strip SCALE 4 USING MAX"), "INVALID_ARGUMENT", "(6 x 4)");
}

std::string PageReply(const std::vector<Entry>& rows, std::optional<std::pair<std::int64_t, std::int64_t>> after) {
    std::string result;
    Protocol::AppendMapHeader(result, 3U);
    Protocol::AppendBulk(result, "chunks");
    Protocol::AppendArrayHeader(result, rows.size());
    for (const auto& row : rows) {
        Protocol::AppendArrayHeader(result, 3U);
        Protocol::AppendInteger(result, row.coord[0]);
        Protocol::AppendInteger(result, row.coord[1]);
        Protocol::AppendBulk(result, row.form);
    }
    Protocol::AppendBulk(result, "more");
    Protocol::AppendBoolean(result, after.has_value());
    Protocol::AppendBulk(result, "after");
    if (!after) Protocol::AppendNull(result);
    else {
        Protocol::AppendArrayHeader(result, 2U);
        Protocol::AppendInteger(result, after->first);
        Protocol::AppendInteger(result, after->second);
    }
    return result;
}

void Fill(Fixture& f) {
    f.Ok("CREATE TABLE t (v u8) CHUNK 8 x 8");
    for (int x = 0; x < 24; ++x)
        for (const int y : {0, 5})
            f.Ok("SET BLOCK " + std::to_string(x) + " " + std::to_string(y) + " IN t v = " + std::to_string(x * 4 + y));
}

void BoundsPagesAndTransactions() {
    const std::string scaled = "GET AREA 0 0 TO 2 0 FROM t SCALE 4 USING MAX";
    std::string reply;
    {
        Fixture f;
        Fill(f);
        reply = f.Run(scaled);
        const auto entries = Entries(reply, 2U);
        assert(entries.size() == 3U);
        // Chunk 1 covers x 8 to 15: cell 3 holds x 12..15, y 4..7, the largest 15 * 4 + 5.
        const Cells second(entries[1].form, 4U);
        assert(second.Fixed(1, 3) == 65U && second.Fixed(1, 0) == 11U * 4U);
        assert(f.Run(scaled + " LIMIT 2") == PageReply({entries[0], entries[1]}, std::pair{1, 0}));
        assert(f.Run(scaled + " LIMIT 2 AFTER 1 0") == PageReply({entries[2]}, std::nullopt));
        const auto schema = f.Run(scaled + " WITH SCHEMA");
        assert(schema == "*2\r\n:1\r\n" + reply);

        // Inside a transaction the private state is reduced and marked private.
        assert(f.Run("BEGIN") == "+OK\r\n");
        f.Ok("SET BLOCK 0 0 IN t v = 200");
        const auto own = Entries(f.Run(scaled), 2U);
        const Cells cells(own[0].form, 4U);
        assert(cells.form().header.flags == kChunkFormPrivate && cells.form().header.chunk_version == 0U);
        assert(cells.Fixed(1, 0) == 200U);
        assert(f.Run("ROLLBACK") == "+OK\r\n");
    }
    // The response bound counts the downsampled forms, not the stored chunks.
    for (const bool exact : {true, false}) {
        QueryLimits limits;
        limits.max_area_response_bytes = reply.size() - static_cast<std::size_t>(!exact);
        Fixture f(limits);
        Fill(f);
        Error(f.Run("GET AREA 0 0 TO 2 0 FROM t"), "OUT_OF_RANGE");
        const auto bounded = f.Run(scaled);
        // Commit times differ from the first fixture, so compare the shape.
        if (exact) assert(bounded.size() == reply.size() && Entries(bounded, 2U).size() == 3U);
        else Error(bounded, "OUT_OF_RANGE", "response exceeds");
        if (!exact) {
            const auto page = f.Run(scaled + " LIMIT 3");
            assert(page.starts_with("%3\r\n$6\r\nchunks\r\n*2\r\n"));
        }
    }
}

}  // namespace

int main() {
    Reductions();
    ThreeDimensionsAndPartialAreas();
    BoundsPagesAndTransactions();
}
