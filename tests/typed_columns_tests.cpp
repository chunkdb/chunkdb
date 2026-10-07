#include <bit>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/chunk_layout.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/table_catalog.hpp"
#include "test_utils.hpp"

namespace {

using chunkdb::BitsValue;
using chunkdb::ChunkLayout;
using chunkdb::Column;
using chunkdb::ColumnAssignment;
using chunkdb::ColumnKind;
using chunkdb::ColumnType;
using chunkdb::ColumnValue;
using chunkdb::TableSchema;

void SetEnvVar(const char* key, const char* value) {
#ifdef _WIN32
    const int rc = _putenv_s(key, value);
#else
    const int rc = setenv(key, value, 1);
#endif
    assert(rc == 0);
    (void)rc;
}

void UnsetEnvVar(const char* key) {
#ifdef _WIN32
    const int rc = _putenv_s(key, "");
#else
    const int rc = unsetenv(key);
#endif
    assert(rc == 0);
    (void)rc;
}

bool Contains(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

std::string ErrorOf(const std::function<void()>& action) {
    try {
        action();
    } catch (const std::exception& e) {
        return e.what();
    }
    return {};
}

void ExpectError(const std::function<void()>& action, const std::string& part) {
    const auto error = ErrorOf(action);
    if (error.empty() || !Contains(error, part)) {
        std::fprintf(stderr, "expected an error with '%s', got '%s'\n", part.c_str(), error.c_str());
        assert(false);
    }
}

Column Fixed(std::uint32_t id, std::string name, ColumnKind kind, std::uint32_t size) {
    Column column;
    column.id = id;
    column.name = std::move(name);
    column.type = ColumnType{.kind = kind, .size = size};
    return column;
}

// id u10 REQUIRED, light u4 DEFAULT 15, temp i8 NULL, solid bool,
// height f32, mask bits(3) NULL DEFAULT 101, big u64.
TableSchema World() {
    auto id = Fixed(1, "id", ColumnKind::kUnsigned, 10);
    id.required = true;
    auto light = Fixed(2, "light", ColumnKind::kUnsigned, 4);
    light.has_default = true;
    light.default_value = {15};
    auto temp = Fixed(3, "temp", ColumnKind::kSigned, 8);
    temp.nullable = true;
    auto mask = Fixed(6, "mask", ColumnKind::kBits, 3);
    mask.nullable = true;
    mask.has_default = true;
    mask.default_value = {0b101};
    return TableSchema{
        .version = 1,
        .next_column_id = 8,
        .columns =
            {id, light, temp, Fixed(4, "solid", ColumnKind::kBool, 1), Fixed(5, "height", ColumnKind::kFloat32, 32),
             mask, Fixed(7, "big", ColumnKind::kUnsigned, 64)},
    };
}

constexpr chunkdb::GeometryConfig kGeometry{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 4,
    .chunk_height_blocks = 3,
    .block_bits = 10 + 4 + 8 + 1 + 32 + 3 + 64,
};

chunkdb::StoreConfig Config(const std::filesystem::path& dir, std::size_t checkpoint_update_interval = 256) {
    chunkdb::StoreConfig config;
    config.geometry = kGeometry;
    config.schema = World();
    config.data_dir = dir;
    config.checkpoint_update_interval = checkpoint_update_interval;
    return config;
}

std::vector<ColumnValue> Row(
    std::uint64_t id,
    std::uint64_t light,
    ColumnValue temp,
    bool solid,
    float height,
    ColumnValue mask,
    std::uint64_t big) {
    return {id, light, std::move(temp), solid, height, std::move(mask), big};
}

void ExpectRow(chunkdb::ChunkStore& store, std::int64_t x, std::int64_t y, const std::optional<std::vector<ColumnValue>>& want) {
    if (store.GetBlock(x, y) != want) {
        std::fprintf(stderr, "block (%lld,%lld) does not hold the expected values\n", static_cast<long long>(x),
                     static_cast<long long>(y));
        assert(false);
    }
}

void TestLayout() {
    // 12 blocks: id 120 bits -> 15 bytes, light 48 -> 6, temp 96 -> 12 plus
    // 2 validity bytes, solid 12 -> 2, height 384 -> 48, mask 36 -> 5 plus 2,
    // big 768 -> 96.
    const ChunkLayout layout(World(), 12, 1U << 20U);
    const auto& fixed = layout.fixed_columns();
    assert(fixed.size() == 7U);
    assert(fixed[0].values == 0U && fixed[0].validity == ChunkLayout::kNoValidity);
    assert(fixed[1].values == 15U);
    assert(fixed[2].values == 21U && fixed[2].validity == 33U);
    assert(fixed[3].values == 35U);
    assert(fixed[4].values == 37U);
    assert(fixed[5].values == 85U && fixed[5].validity == 90U);
    assert(fixed[6].values == 92U);
    assert(layout.payload_bytes() == 188U);
    assert(!layout.bit_string_blocks());
    assert(layout.FindColumn("height") == 4U);
    assert(layout.FindColumn("nope") == std::string_view::npos);

    // One bits(N) column is the old bit string per block.
    const ChunkLayout bits(chunkdb::SingleBitsColumnSchema(9), 12, 1U << 20U);
    assert(bits.bit_string_blocks());
    assert(bits.payload_bytes() == (12U * 9U + 7U) / 8U);
    assert(chunkdb::Geometry(kGeometry, World()).ChunkPayloadBytes() == 188U);

    ExpectError([] { (void)ChunkLayout(World(), 12, 100); }, "chunk payload must be <= 100 bytes");
    auto narrow = kGeometry;
    narrow.block_bits = 16;
    ExpectError([&] { (void)chunkdb::Geometry(narrow, World()); }, "does not match the columns' 122 fixed bits");
}

void TestValueEncoding() {
    const auto schema = World();
    const auto& id = schema.columns[0];
    const auto& temp = schema.columns[2];
    const auto& solid = schema.columns[3];
    const auto& height = schema.columns[4];
    const auto& mask = schema.columns[5];
    const auto& big = schema.columns[6];
    const auto round_trip = [](const Column& column, const ColumnValue& value) {
        const auto bytes = chunkdb::EncodeColumnValue(column, value);
        assert(bytes.size() == (chunkdb::FixedWidthBits(column.type) + 7U) / 8U);
        return chunkdb::DecodeColumnValue(column, bytes.data());
    };
    assert(round_trip(id, std::uint64_t{1023}) == ColumnValue{std::uint64_t{1023}});
    assert(chunkdb::EncodeColumnValue(id, std::uint64_t{0x2A5}) == (std::vector<std::uint8_t>{0xA5, 0x02}));
    assert(round_trip(temp, std::int64_t{-128}) == ColumnValue{std::int64_t{-128}});
    assert(round_trip(temp, std::int64_t{127}) == ColumnValue{std::int64_t{127}});
    assert(chunkdb::EncodeColumnValue(temp, std::int64_t{-1}) == std::vector<std::uint8_t>{0xFF});
    assert(round_trip(solid, true) == ColumnValue{true});
    assert(round_trip(height, -2.5F) == ColumnValue{-2.5F});
    assert(std::isnan(std::get<float>(round_trip(height, std::numeric_limits<float>::quiet_NaN()))));
    assert(round_trip(mask, BitsValue{"110"}) == ColumnValue{BitsValue{"110"}});
    assert(chunkdb::EncodeColumnValue(mask, BitsValue{"110"}) == std::vector<std::uint8_t>{0b011});
    const auto max64 = std::numeric_limits<std::uint64_t>::max();
    assert(round_trip(big, max64) == ColumnValue{max64});
    auto wide = Fixed(9, "wide", ColumnKind::kSigned, 64);
    assert(round_trip(wide, std::numeric_limits<std::int64_t>::min()) ==
           ColumnValue{std::numeric_limits<std::int64_t>::min()});
    auto f64 = Fixed(10, "f", ColumnKind::kFloat64, 64);
    assert(round_trip(f64, 1e300) == ColumnValue{1e300});

    ExpectError([&] { (void)chunkdb::EncodeColumnValue(id, std::uint64_t{1024}); }, "1024 is out of range 0..1023");
    ExpectError([&] { (void)chunkdb::EncodeColumnValue(temp, std::int64_t{128}); }, "128 is out of range -128..127");
    ExpectError([&] { (void)chunkdb::EncodeColumnValue(temp, std::int64_t{-129}); }, "out of range");
    ExpectError([&] { (void)chunkdb::EncodeColumnValue(id, std::int64_t{1}); }, "column id is u10, not a signed integer");
    ExpectError([&] { (void)chunkdb::EncodeColumnValue(height, 1.0); }, "is f32, not an f64");
    ExpectError([&] { (void)chunkdb::EncodeColumnValue(mask, BitsValue{"1"}); }, "is bits(3), not bits(1)");
    ExpectError([&] { (void)chunkdb::EncodeColumnValue(mask, BitsValue{"1x0"}); }, "holds only 0 and 1");
    ExpectError([&] { (void)chunkdb::EncodeColumnValue(temp, std::monostate{}); }, "cannot be NULL");
}

void TestBlockValues() {
    chunkdb::test::ScopedTempDir dir("chunkdb-typed-columns");
    chunkdb::ChunkStore store(Config(dir.path()));
    assert(store.GetBlock(10, 4) == std::nullopt);

    // A new block needs its REQUIRED column; nothing is written without it.
    ExpectError([&] { store.SetBlock(10, 4, {{"light", std::uint64_t{3}}}); }, "column id is REQUIRED");
    assert(!store.ChunkExists(2, 1));

    // The other columns take their DEFAULT, NULL, or zero.
    store.SetBlock(10, 4, {{"id", std::uint64_t{23}}});
    ExpectRow(store, 10, 4, Row(23, 15, std::monostate{}, false, 0.0F, BitsValue{"101"}, 0));

    // Later writes change only the given columns.
    store.SetBlock(10, 4, {{"light", std::uint64_t{3}}, {"temp", std::int64_t{-7}}, {"height", 1.5F}});
    store.SetBlock(11, 4, {{"id", std::uint64_t{1023}}, {"solid", true}, {"mask", std::monostate{}}, {"big", ~std::uint64_t{0}}});
    ExpectRow(store, 10, 4, Row(23, 3, std::int64_t{-7}, false, 1.5F, BitsValue{"101"}, 0));
    ExpectRow(store, 11, 4, Row(1023, 15, std::monostate{}, true, 0.0F, std::monostate{}, ~std::uint64_t{0}));
    store.SetBlock(10, 4, {{"temp", std::monostate{}}});
    ExpectRow(store, 10, 4, Row(23, 3, std::monostate{}, false, 1.5F, BitsValue{"101"}, 0));

    // Bad writes are refused before anything changes.
    const auto version = store.GetChunkVersion(2, 1);
    ExpectError([&] { store.SetBlock(10, 4, {{"nope", std::uint64_t{1}}}); }, "the table has no column nope");
    ExpectError([&] { store.SetBlock(10, 4, {{"id", std::uint64_t{1}}, {"id", std::uint64_t{2}}}); }, "given twice");
    ExpectError([&] { store.SetBlock(10, 4, {{"light", std::monostate{}}}); }, "column light cannot be NULL");
    ExpectError([&] { store.SetBlock(10, 4, {{"light", std::uint64_t{1}}, {"id", std::uint64_t{5000}}}); }, "out of range");
    ExpectRow(store, 10, 4, Row(23, 3, std::monostate{}, false, 1.5F, BitsValue{"101"}, 0));
    assert(store.GetChunkVersion(2, 1) == version);

    // Writing the values a block already has is not a write.
    store.SetBlock(10, 4, {{"light", std::uint64_t{3}}});
    assert(store.GetChunkVersion(2, 1) == version);

    // The bit-string commands need one bits(N) column.
    ExpectError([&] { (void)store.GetBlockBits(10, 4); }, "bit strings need a table with one bits(N) column");
    ExpectError([&] { store.SetBlockBits(10, 4, std::string(kGeometry.block_bits, '0')); }, "one bits(N) column");

    store.UnsetBlock(10, 4);
    assert(store.GetBlock(10, 4) == std::nullopt);
    ExpectRow(store, 11, 4, Row(1023, 15, std::monostate{}, true, 0.0F, std::monostate{}, ~std::uint64_t{0}));
    // A block created again starts from its defaults, not from old values.
    store.SetBlock(10, 4, {{"id", std::uint64_t{1}}});
    ExpectRow(store, 10, 4, Row(1, 15, std::monostate{}, false, 0.0F, BitsValue{"101"}, 0));
}

// Values survive a restart from WAL frames alone and from chunk images.
void TestValuesSurviveRestart() {
    for (const std::size_t checkpoint_update_interval : {std::size_t{1}, std::size_t{100000}}) {
        chunkdb::test::ScopedTempDir dir("chunkdb-typed-columns-restart");
        {
            chunkdb::ChunkStore store(Config(dir.path(), checkpoint_update_interval));
            for (std::int64_t x = 0; x < 8; ++x) {
                store.SetBlock(x, -1, {{"id", static_cast<std::uint64_t>(x)}, {"temp", std::int64_t{-x}}, {"height", 0.25F * static_cast<float>(x)}});
            }
            store.SetBlock(3, -1, {{"temp", std::monostate{}}, {"mask", BitsValue{"011"}}});
            store.UnsetBlock(5, -1);
        }
        auto config = Config(dir.path(), checkpoint_update_interval);
        config.schema.reset();
        config.geometry_fields = 0;
        chunkdb::ChunkStore store(config);
        assert(store.geometry().layout().schema() == World());
        for (std::int64_t x = 0; x < 8; ++x) {
            if (x == 5) {
                assert(store.GetBlock(x, -1) == std::nullopt);
                continue;
            }
            const ColumnValue temp = x == 3 ? ColumnValue{std::monostate{}} : ColumnValue{std::int64_t{-x}};
            const ColumnValue mask = x == 3 ? ColumnValue{BitsValue{"011"}} : ColumnValue{BitsValue{"101"}};
            ExpectRow(store, x, -1, Row(static_cast<std::uint64_t>(x), 15, temp, false, 0.25F * static_cast<float>(x), mask, 0));
        }
    }
}

// A write whose WAL sync fails leaves memory and the version as they were.
void TestFailedWriteRollsBack() {
    chunkdb::test::ScopedTempDir dir("chunkdb-typed-columns-rollback");
    auto config = Config(dir.path());
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    chunkdb::ChunkStore store(config);
    store.SetBlock(0, 0, {{"id", std::uint64_t{7}}, {"temp", std::int64_t{5}}});
    const auto before = store.GetChunkStateBytes(0, 0);
    const auto version = store.GetChunkVersion(0, 0);
    const std::vector<std::function<void()>> writes = {
        [&] { store.SetBlock(0, 0, {{"light", std::uint64_t{1}}, {"temp", std::monostate{}}, {"big", std::uint64_t{9}}}); },
        [&] { store.SetBlock(1, 0, {{"id", std::uint64_t{8}}}); },
        [&] { store.UnsetBlock(0, 0); },
    };
    for (const auto& write : writes) {
        SetEnvVar("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
        assert(!ErrorOf(write).empty());
        UnsetEnvVar("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE");
        assert(store.GetChunkStateBytes(0, 0) == before);
        assert(store.GetChunkVersion(0, 0) == version);
    }
}

// Whole-chunk writes keep the canonical form: nothing in absent blocks,
// padding, or NULL values.
void TestChunkStateIsCanonical() {
    chunkdb::test::ScopedTempDir dir("chunkdb-typed-columns-state");
    chunkdb::ChunkStore store(Config(dir.path()));
    const auto& layout = store.geometry().layout();
    std::vector<std::uint8_t> payload(layout.payload_bytes(), 0xFFU);
    // Blocks 0 and 1 present; temp of block 1 NULL.
    std::vector<std::uint8_t> presence = {0b11, 0};
    const auto& temp = layout.fixed_columns()[2];
    payload[temp.validity] = 0b01;
    payload[temp.validity + 1] = 0;
    (void)store.SetChunkStateBytes(0, 0, payload, presence);
    const auto state = store.GetChunkStateBytes(0, 0);
    // Every value of block 0 is all ones (with temp valid), so reading it
    // back gives the widest values; block 1 has a NULL temp.
    const auto block0 = store.GetBlock(0, 0);
    assert(block0.has_value());
    assert((*block0)[0] == ColumnValue{std::uint64_t{1023}});
    assert((*block0)[2] == ColumnValue{std::int64_t{-1}});
    const auto block1 = store.GetBlock(1, 0);
    assert((*block1)[2] == ColumnValue{std::monostate{}});
    assert(store.GetBlock(2, 0) == std::nullopt);
    // The stored state equals the one built by typed writes.
    chunkdb::test::ScopedTempDir other_dir("chunkdb-typed-columns-state-typed");
    chunkdb::ChunkStore other(Config(other_dir.path()));
    const std::vector<ColumnAssignment> all_ones = {
        {"id", std::uint64_t{1023}}, {"light", std::uint64_t{15}}, {"temp", std::int64_t{-1}}, {"solid", true},
        {"height", std::bit_cast<float>(0xFFFFFFFFU)}, {"mask", BitsValue{"111"}}, {"big", ~std::uint64_t{0}}};
    other.SetBlock(0, 0, all_ones);
    auto second = all_ones;
    second[2].value = std::monostate{};
    other.SetBlock(1, 0, second);
    assert(other.GetChunkStateBytes(0, 0) == state);
}

void TestCatalogTables() {
    chunkdb::test::ScopedTempDir dir("chunkdb-typed-columns-catalog");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    {
        chunkdb::TableCatalog catalog(config);
        ExpectError(
            [&] { (void)catalog.Create("world", chunkdb::GeometryConfig{}, chunkdb::TableOptions{}, World()); },
            "does not match the columns' 122 fixed bits");
        chunkdb::TableOptions with_extra;
        with_extra.extra_max_block_bits = 64;
        ExpectError([&] { (void)catalog.Create("world", kGeometry, with_extra, World()); },
                    "extra data needs a table with one bits(N) column");
        auto text = World();
        auto sign = Fixed(8, "sign", ColumnKind::kText, 256);
        sign.nullable = true;
        text.columns.push_back(sign);
        text.next_column_id = 9;
        ExpectError([&] { (void)catalog.Create("world", kGeometry, chunkdb::TableOptions{}, text); },
                    "text and bytes columns are not supported by this build yet");
        assert(catalog.Find("world") == nullptr);

        const auto table = catalog.Create("world", kGeometry, chunkdb::TableOptions{}, World());
        assert(table->Info().schema == World());
        auto lease = *table->Acquire();
        lease.store().SetBlock(-1, -1, {{"id", std::uint64_t{99}}});
        chunkdb::TableOptionsUpdate update;
        update.extra_max_block_bits = 64;
        ExpectError([&] { catalog.SetOptions("world", update); }, "extra data needs a table with one bits(N) column");
    }
    chunkdb::TableCatalog catalog(config);
    const auto table = catalog.Find("world");
    assert(table != nullptr && table->Info().schema == World());
    assert(table->geometry().ChunkPayloadBytes() == 188U);
    auto lease = *table->Acquire();
    ExpectRow(lease.store(), -1, -1, Row(99, 15, std::monostate{}, false, 0.0F, BitsValue{"101"}, 0));
}

// The bit-string protocol commands refuse a table with columns and leave it
// and the connection usable.
void TestProtocolOnTypedTable() {
    chunkdb::test::ScopedTempDir dir("chunkdb-typed-columns-protocol");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
    (void)catalog->Create("world", kGeometry, chunkdb::TableOptions{}, World());
    chunkdb::CommandEngine engine(chunkdb::EngineConfig{.auth_token = "", .require_auth = false}, catalog);
    chunkdb::SessionState session;
    assert(engine.Execute(session, "HELLO 2\r\n")[0] == '$');
    assert(engine.Execute(session, "USE world\r\n")[0] == '$');
    const std::string bits(kGeometry.block_bits, '1');
    assert(engine.Execute(session, "SET 0 0 " + bits + "\r\n").rfind("-ERR", 0) == 0);
    assert(engine.Execute(session, "GET 0 0\r\n").rfind("-ERR", 0) == 0);
    auto lease = *catalog->Find("world")->Acquire();
    assert(lease.store().GetBlock(0, 0) == std::nullopt);
    lease.store().SetBlock(0, 0, {{"id", std::uint64_t{5}}});
    assert(engine.Execute(session, "UNSET 0 0\r\n") == "+OK\r\n");
    assert(lease.store().GetBlock(0, 0) == std::nullopt);
}

void TestStoreConfigSchema() {
    chunkdb::test::ScopedTempDir dir("chunkdb-typed-columns-config");
    {
        chunkdb::ChunkStore store(Config(dir.path()));
    }
    auto other = World();
    other.columns[1].default_value = {14};
    auto config = Config(dir.path());
    config.schema = other;
    ExpectError([&] { chunkdb::ChunkStore store(config); }, "was created with different columns");
}

}  // namespace

int main() {
    TestLayout();
    TestValueEncoding();
    TestBlockValues();
    TestValuesSurviveRestart();
    TestFailedWriteRollsBack();
    TestChunkStateIsCanonical();
    TestCatalogTables();
    TestProtocolOnTypedTable();
    TestStoreConfigSchema();
    return 0;
}
