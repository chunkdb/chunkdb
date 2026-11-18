#include <algorithm>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/chunk_columns.hpp"
#include "chunkdb/file_layout.hpp"
#include "expr.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;

// A block coordinate, kept apart from ChunkCoord.
struct BlockPoint {
    std::int64_t x;
    std::int64_t y;
    std::int64_t z;
};

struct Example {
    std::string name;
    ColumnType type;
    ColumnValue value;
};
const std::vector<Example> kExamples{
    {"u", {ColumnKind::kUnsigned, 64}, std::numeric_limits<std::uint64_t>::max()},
    {"s", {ColumnKind::kSigned, 64}, std::numeric_limits<std::int64_t>::min()},
    {"u8v", {ColumnKind::kUnsigned, 8}, std::uint64_t{255}},
    {"i8v", {ColumnKind::kSigned, 8}, std::int64_t{-128}},
    {"f",{ColumnKind::kFloat32, 32}, -1.5F},
    {"d", {ColumnKind::kFloat64, 64}, 1.25},
    {"flag", {ColumnKind::kBool, 1}, true},
    {"mask", {ColumnKind::kBits, 9}, BitsValue{"100000001"}},
    {"text_value", {ColumnKind::kText, 32}, std::string("a\0\xe9\x9b\xaa", 5)},
    {"bytes_value", {ColumnKind::kBytes, 32}, BytesValue{{0, 255, 13, 10}}},
};

TableSchema Schema() {
    TableSchema schema;
    for (const bool nullable : {false, true}) {
        for (const auto& example : kExamples)
            schema.columns.push_back({.id = schema.next_column_id++,
                .name = (nullable ? "n_" : "") + example.name, .type = example.type,
                .nullable = nullable, .default_value = {}});
    }
    return schema;
}

ColumnValue ExampleValue(const Column& column, bool nulls) {
    if (column.nullable && nulls) return std::monostate{};
    for (const auto& example : kExamples)
        if (column.name == (column.nullable ? "n_" : "") + example.name) return example.value;
    throw std::logic_error("unknown example column");
}

std::vector<ColumnValue> Values(const TableSchema& schema, bool nulls) {
    std::vector<ColumnValue> values;
    for (const auto& column : schema.columns) values.push_back(ExampleValue(column, nulls));
    return values;
}

std::vector<ColumnValue> DefaultValues(const TableSchema& schema) {
    std::vector<ColumnValue> values;
    for (const auto& column : schema.columns) {
        if (column.nullable) { values.emplace_back(); continue; }
        switch (column.type.kind) {
            case ColumnKind::kUnsigned: values.emplace_back(std::uint64_t{0}); break;
            case ColumnKind::kSigned: values.emplace_back(std::int64_t{0}); break;
            case ColumnKind::kFloat32: values.emplace_back(0.0F); break;
            case ColumnKind::kFloat64: values.emplace_back(0.0); break;
            case ColumnKind::kBool: values.emplace_back(false); break;
            case ColumnKind::kBits: values.emplace_back(BitsValue{std::string(column.type.size, '0')}); break;
            case ColumnKind::kText: values.emplace_back(std::string{}); break;
            case ColumnKind::kBytes: values.emplace_back(BytesValue{}); break;
        }
    }
    return values;
}

StoreConfig Config(const std::filesystem::path& directory, std::uint8_t dims) {
    StoreConfig config;
    config.data_dir = directory;
    config.schema = Schema();
    config.chunk_schema = Schema();
    // Same names and IDs, different slots: the two domains must stay separate.
    std::reverse(config.chunk_schema->columns.begin(), config.chunk_schema->columns.end());
    config.geometry.dims = dims;
    config.geometry.chunk_width_blocks = config.geometry.chunk_height_blocks = 2;
    config.geometry.chunk_depth_blocks = dims == 3 ? 2 : 1;
    config.geometry.large_chunk_depth_chunks = dims == 3 ? 2 : 1;
    config.geometry.block_bits = FixedBitsPerBlock(*config.schema);
    config.background_maintenance = false;
    config.checkpoint_update_interval = 1;
    return config;
}

// Expressions are not wired into CQL statements yet. Exercise their existing
// input contract with real stored rows, without adding statement semantics.
void CheckRow(ChunkStore& store, const ChunkState& state, const BlockPoint& point,
    const std::vector<ColumnValue>& expected, bool present) {
    const auto& geometry = store.geometry();
    const auto& chunks = geometry.chunk_schema();
    const expr::ColumnList schema(geometry.layout().schema().columns, chunks.columns);
    const auto block = geometry.dims() == 3
        ? store.BlockInState3D(state, point.x, point.y, point.z)
        : store.BlockInState(state, point.x, point.y);
    assert(block.has_value() == present);
    // The evaluator takes a complete row; an empty span is a malformed input.
    // How CQL resolves chunk.<column> for a chunk without metadata is not
    // decided yet; until expressions are wired into CQL, this test chooses
    // the defaults that a first write would start from.
    const auto& stored = state.chunk_columns ? state.chunk_columns : geometry.default_chunk_columns();
    const auto metadata = DecodeChunkColumns(chunks, stored->bytes);
    assert(metadata == expected);
    expr::Inputs inputs{.exists = present, .block = block ? std::span<const ColumnValue>(*block) : std::span<const ColumnValue>{},
        .chunk = metadata, .x = point.x, .y = point.y, .z = point.z};
    const auto condition = [&](const std::string& text) {
        return expr::EvaluateCondition(expr::PlanCondition(expr::ParseCondition(text).expression, schema), inputs);
    };
    assert(condition(present ? "EXISTS" : "NOT EXISTS"));
    assert(condition("POS_X = -1 AND POS_Y = 3 AND POS_Z = " + std::to_string(point.z)));
    for (std::size_t i = 0; i < chunks.columns.size(); ++i) {
        const auto& column = chunks.columns[i];
        const auto name = "chunk." + column.name;
        const auto null = std::holds_alternative<std::monostate>(expected[i]);
        assert(condition(name + (null ? " IS NULL" : " IS NOT NULL")));
        if (!present) assert(condition(column.name + " IS NULL"));
        const auto plan = expr::PlanCondition(expr::ParseCondition(name + " = $1").expression, schema);
        assert(plan.parameters().size() == 1 && plan.parameters()[0].type == column.type);
        const std::vector<ColumnValue> parameters{expected[i]};
        inputs.parameters = parameters;
        assert(expr::EvaluateCondition(plan, inputs) == !null);
        assert(!condition("NOT (" + name + " = $1)"));
        // Copying every kind also checks representation and assignment typing.
        // COALESCE supplies a typed value for NULL in a nonnullable target.
        const std::vector<ColumnValue> fallback{ExampleValue(column, false)};
        inputs.parameters = fallback;
        const auto expression = expr::ParseAssignments(column.name + " = COALESCE(" + name + ", $1)");
        const auto copied = expr::EvaluateAssignments(expr::PlanAssignments(expression.values, schema), inputs);
        assert(copied.size() == 1 && copied[0] == (null ? fallback[0] : expected[i]));
        if (column.nullable) {
            const auto direct = expr::ParseAssignments(column.name + " = " + name);
            assert(expr::EvaluateAssignments(expr::PlanAssignments(direct.values, schema), inputs) == parameters);
            // NULL from a chunk column cannot enter a block column without NULL.
            const auto strict = expr::PlanAssignments(
                expr::ParseAssignments(column.name.substr(2) + " = " + name).values, schema);
            bool rejected = false;
            try { (void)expr::EvaluateAssignments(strict, inputs); }
            catch (const std::invalid_argument&) { rejected = true; }
            assert(rejected == null);
        }
        inputs.parameters = {};
    }
    if (std::find(expected.begin(), expected.end(), ColumnValue(std::numeric_limits<std::uint64_t>::max())) != expected.end()) {
        assert(condition("chunk.u = 18446744073709551615 AND chunk.s = -9223372036854775808"));
        assert(condition("chunk.f + chunk.d = -0.25 AND chunk.flag AND chunk.mask = b'100000001'"));
        assert(condition("chunk.u8v + 1 = 256 AND chunk.i8v - 1 = -129 AND chunk.i8v < chunk.u8v"));
        if (present) assert(condition("u = 0 AND chunk.u <> u"));
    }
    const auto planned = expr::PlanCondition(expr::ParseCondition("chunk.u IS NULL").expression, schema);
    inputs.chunk = {};
    bool refused = false;
    try { (void)expr::EvaluateCondition(planned, inputs); }
    catch (const std::logic_error&) { refused = true; }
    assert(refused);
}

void Lifecycle(std::uint8_t dims) {
    test::ScopedTempDir directory("chunkdb-chunk-expression");
    const auto config = Config(directory.path(), dims);
    const BlockPoint point{-1, 3, dims == 3 ? -5 : 0};
    const ChunkCoord coord{-1, 1, dims == 3 ? -3 : 0};
    const auto full = Values(*config.chunk_schema, false);
    const auto nullable = Values(*config.chunk_schema, true);
    const auto defaults = DefaultValues(*config.chunk_schema);
    std::uint64_t deleted = 0;
    {
        ChunkStore store(config);
        const auto absent = store.ReadChunkState(coord);
        assert(!absent.written && absent.version == 0 && !absent.chunk_columns);
        CheckRow(store, absent, point, defaults, false);
        auto state = absent;
        state.chunk_columns = std::make_shared<const ChunkColumnState>(EncodeChunkColumns(*config.chunk_schema, full));
        assert(store.WriteChunkState(coord, state, {}).ok);
        CheckRow(store, store.ReadChunkState(coord), point, full, false);
        state = store.ReadChunkState(coord);
        if (dims == 3) store.SetBlockInState3D(state, point.x, point.y, point.z, {{"u", std::uint64_t{0}}});
        else store.SetBlockInState(state, point.x, point.y, {{"u", std::uint64_t{0}}});
        state.chunk_columns = std::make_shared<const ChunkColumnState>(EncodeChunkColumns(*config.chunk_schema, nullable));
        assert(store.WriteChunkState(coord, state, {}).ok);
        CheckRow(store, store.ReadChunkState(coord), point, nullable, true);
    }
    {
        ChunkStore store(config);
        CheckRow(store, store.ReadChunkState(coord), point, nullable, true);
        auto state = store.ReadChunkState(coord);
        if (dims == 3) store.UnsetBlockInState3D(state, point.x, point.y, point.z);
        else store.UnsetBlockInState(state, point.x, point.y);
        assert(store.WriteChunkState(coord, state, {}).ok);
        CheckRow(store, store.ReadChunkState(coord), point, nullable, false);
        state = store.ReadChunkState(coord);
        state.chunk_columns = std::make_shared<const ChunkColumnState>(DefaultChunkColumns(*config.chunk_schema));
        const auto result = store.WriteChunkState(coord, state, {});
        assert(result.ok);
        deleted = result.version;
        assert(store.RetainedTombstoneCount() == 1);
        assert(!std::filesystem::exists(ChunkDataPath(directory.path(), store.geometry(), coord)));
        assert(!std::filesystem::exists(ChunkWalPath(directory.path(), store.geometry(), coord)));
        CheckRow(store, store.ReadChunkState(coord), point, defaults, false);
    }
    {
        ChunkStore store(config);
        const auto tombstone = store.ReadChunkState(coord);
        assert(tombstone.written && tombstone.version == deleted && tombstone.commit_time_ms != 0);
        CheckRow(store, tombstone, point, defaults, false);
    }
}
}  // namespace

int main() {
    Lifecycle(2);
    Lifecycle(3);
    std::cout << "2/2 chunk expression lifecycle groups passed (8 column kinds, nullable and nonnullable); 0 skipped\n";
}
