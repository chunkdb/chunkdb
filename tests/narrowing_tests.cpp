// Narrowing a column after checking every stored value (#61): the pending
// narrowing and its rules, writes refused while it lasts, the check over
// images and WALs, a value that does not fit, an interrupted check, and a
// read failure during it.

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/table_catalog.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"

namespace {

using chunkdb::BitsValue;
using chunkdb::Column;
using chunkdb::ColumnKind;
using chunkdb::ColumnType;
using chunkdb::ColumnValue;
using chunkdb::TableSchema;
using chunkdb::test::ScopedTempDir;

bool Contains(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

template <typename Exception = std::exception, typename Fn>
void ExpectThrow(Fn fn, const std::string& part) {
    try {
        fn();
    } catch (const Exception& e) {
        if (!Contains(e.what(), part)) {
            std::fprintf(stderr, "expected '%s', got '%s'\n", part.c_str(), e.what());
            assert(false);
        }
        return;
    }
    std::fprintf(stderr, "expected an exception with '%s'\n", part.c_str());
    assert(false);
}

void SetEnv(const char* key, const char* value) {
#ifdef _WIN32
    (void)_putenv_s(key, value);
#else
    if (value[0] == '\0') {
        (void)unsetenv(key);
    } else {
        (void)setenv(key, value, 1);
    }
#endif
}

Column MakeColumn(std::uint32_t id, std::string name, ColumnKind kind, std::uint32_t size) {
    Column column;
    column.id = id;
    column.name = std::move(name);
    column.type = ColumnType{.kind = kind, .size = size};
    return column;
}

// id u10, light u4 DEFAULT 15, sign text(8) NULL.
TableSchema Initial() {
    auto light = MakeColumn(2, "light", ColumnKind::kUnsigned, 4);
    light.has_default = true;
    light.default_value = {15};
    auto sign = MakeColumn(3, "sign", ColumnKind::kText, 8);
    sign.nullable = true;
    return TableSchema{
        .version = 1,
        .next_column_id = 4,
        .columns = {MakeColumn(1, "id", ColumnKind::kUnsigned, 10), light, sign},
        .history = {},
        .pending = std::nullopt,
    };
}

chunkdb::GeometryConfig GeometryFor(const TableSchema& schema) {
    return chunkdb::GeometryConfig{
        .large_chunk_width_chunks = 2,
        .large_chunk_height_chunks = 2,
        .chunk_width_blocks = 4,
        .chunk_height_blocks = 4,
        .block_bits = chunkdb::FixedBitsPerBlock(schema),
    };
}

ColumnType Type(ColumnKind kind, std::uint32_t size) {
    return ColumnType{.kind = kind, .size = size};
}

void TestRules() {
    const auto v1 = Initial();
    const auto pending = chunkdb::WithPendingNarrowing(v1, "id", Type(ColumnKind::kUnsigned, 8));
    assert(pending.version == 1U && pending.pending.has_value() && pending.pending->column_id == 1U);
    // Through the manifest's schema area.
    const auto bytes = chunkdb::EncodeTableSchema(pending);
    assert(chunkdb::DecodeTableSchema(bytes.data(), bytes.size()) == pending);
    // A widening needs no check; the DEFAULT must fit; one at a time; no
    // other change meanwhile.
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::WithPendingNarrowing(v1, "id", Type(ColumnKind::kUnsigned, 12)); }, "cannot be narrowed");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::WithPendingNarrowing(v1, "light", Type(ColumnKind::kUnsigned, 3)); }, "its DEFAULT does not fit");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::WithPendingNarrowing(pending, "sign", Type(ColumnKind::kText, 3)); }, "another narrowing");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::RenameColumn(pending, "sign", "label"); }, "a narrowing of this table is in progress");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::NarrowColumnType(v1, "id", Type(ColumnKind::kUnsigned, 8)); }, "no narrowing to u8 in progress");
    // The end of a check: the next version with an exact conversion.
    const auto narrowed = chunkdb::NarrowColumnType(pending, "id", Type(ColumnKind::kUnsigned, 8));
    assert(narrowed.version == 2U && !narrowed.pending.has_value());
    assert(narrowed.history.back().changes[0].conversion == chunkdb::Conversion::kExact);
    assert(chunkdb::SchemaAtVersion(narrowed, 1) == v1);
    assert(chunkdb::WithoutPendingNarrowing(pending) == v1);
}

using Row = std::vector<ColumnValue>;

void ExpectRow(chunkdb::ChunkStore& store, std::int64_t x, std::int64_t y, const Row& want) {
    if (store.GetBlock(x, y) != std::optional<Row>(want)) {
        std::fprintf(stderr, "block (%lld,%lld) does not hold the expected values\n", static_cast<long long>(x),
                     static_cast<long long>(y));
        assert(false);
    }
}

// While a narrowing is in progress every write path refuses values the
// narrower type does not hold.
void TestWritesWhilePending() {
    ScopedTempDir dir("chunkdb-narrowing-pending");
    chunkdb::StoreConfig config;
    config.data_dir = dir.path();
    const auto schema = chunkdb::WithPendingNarrowing(Initial(), "id", Type(ColumnKind::kUnsigned, 8));
    config.schema = schema;
    config.geometry = GeometryFor(schema);
    chunkdb::ChunkStore store(config);
    store.SetBlock(0, 0, {{"id", std::uint64_t{255}}});
    ExpectThrow<std::invalid_argument>(
        [&] { store.SetBlock(1, 0, {{"id", std::uint64_t{256}}}); }, "column id is being narrowed to u8");
    assert(!store.GetBlock(1, 0).has_value());
    // A whole-chunk write holding a value over the narrower type.
    auto payload = store.GetChunkPayloadBytes(0, 0);
    payload[0] = 0xFF;
    payload[1] = 0x03;  // id of block 0 = 1023
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.SetChunkStateBytes(0, 0, payload, std::vector<std::uint8_t>{0x01, 0x00}); },
        "does not hold the value of block index 0");
    ExpectThrow<std::invalid_argument>(
        [&] {
            (void)store.CasChunkStateBytes(
                0, 0, store.GetChunkVersion(0, 0), payload, std::vector<std::uint8_t>{0x01, 0x00});
        },
        "does not hold the value of block index 0");
    ExpectRow(store, 0, 0, Row{std::uint64_t{255}, std::uint64_t{15}, std::monostate{}});

    // A whole-chunk write with text values, while the text column narrows.
    {
        ScopedTempDir text_dir("chunkdb-narrowing-pending-text");
        chunkdb::StoreConfig text_config;
        text_config.data_dir = text_dir.path();
        const auto text_schema = chunkdb::WithPendingNarrowing(Initial(), "sign", Type(ColumnKind::kText, 3));
        text_config.schema = text_schema;
        text_config.geometry = GeometryFor(text_schema);
        chunkdb::ChunkStore text_store(text_config);
        text_store.SetBlock(0, 0, {{"id", std::uint64_t{1}}, {"sign", std::string("abc")}});
        auto state = *text_store.ReadChunkState(0, 0);
        const std::vector<chunkdb::VarChange> longer{{
            .key = chunkdb::VarKey{.column_id = 3, .block_index = 0},
            .value = std::vector<std::uint8_t>{'a', 'b', 'c', 'd'},
        }};
        state.vars = chunkdb::ChunkVars::Merge(state.vars, longer);
        ExpectThrow<std::invalid_argument>(
            [&] { (void)text_store.WriteChunkState(0, 0, state, std::nullopt); },
            "column sign is being narrowed to text(3), which does not hold the value of block index 0");
        ExpectRow(text_store, 0, 0, Row{std::uint64_t{1}, std::uint64_t{15}, std::string("abc")});
    }

    // A table of bit strings narrowing its one column.
    ScopedTempDir bits_dir("chunkdb-narrowing-bits");
    chunkdb::StoreConfig bits_config;
    bits_config.data_dir = bits_dir.path();
    bits_config.geometry = GeometryFor(chunkdb::SingleBitsColumnSchema(4));
    bits_config.schema = chunkdb::WithPendingNarrowing(chunkdb::SingleBitsColumnSchema(4), "bits", Type(ColumnKind::kBits, 2));
    chunkdb::ChunkStore bits(bits_config);
    bits.SetBlockBits(0, 0, "1100");
    ExpectThrow<std::invalid_argument>([&] { bits.SetBlockBits(1, 0, "1110"); }, "being narrowed to bits(2)");
}

void TestNarrowTable() {
    for (const std::size_t checkpoint_updates : {std::size_t{1}, std::size_t{100000}}) {
        ScopedTempDir dir("chunkdb-narrowing");
        chunkdb::CatalogConfig config;
        config.data_dir = dir.path();
        {
            chunkdb::TableCatalog catalog(config);
            chunkdb::TableOptions options;
            options.checkpoint_update_interval = checkpoint_updates;
            (void)catalog.Create("world", GeometryFor(Initial()), options, Initial());
            {
                auto lease = *catalog.Find("world")->Acquire();
                lease.store().SetBlock(0, 0, {{"id", std::uint64_t{200}}, {"sign", std::string("abc")}});
                lease.store().SetBlock(9, 9, {{"id", std::uint64_t{700}}, {"sign", std::string("abcdef")}});
            }
            // A value that does not fit: refused, the schema as it was.
            ExpectThrow<std::invalid_argument>(
                [&] { catalog.NarrowColumn("world", "id", Type(ColumnKind::kUnsigned, 8)); },
                "column id cannot be narrowed to u8: block (9, 9) holds 700");
            assert(catalog.Find("world")->Info().schema == Initial());
            ExpectThrow<std::invalid_argument>(
                [&] { catalog.NarrowColumn("world", "sign", Type(ColumnKind::kText, 4)); }, "holds 'abcdef'");
            // Once it fits, the column narrows.
            {
                auto lease = *catalog.Find("world")->Acquire();
                lease.store().SetBlock(9, 9, {{"id", std::uint64_t{70}}, {"sign", std::string("ab")}});
            }
            catalog.NarrowColumn("world", "id", Type(ColumnKind::kUnsigned, 8));
            catalog.NarrowColumn("world", "sign", Type(ColumnKind::kText, 4));
            const auto info = catalog.Find("world")->Info();
            assert(info.schema.version == 3U && !info.schema.pending.has_value());
            assert(info.schema.columns[0].type == Type(ColumnKind::kUnsigned, 8));
            auto lease = *catalog.Find("world")->Acquire();
            ExpectRow(lease.store(), 0, 0, Row{std::uint64_t{200}, std::uint64_t{15}, std::string("abc")});
            ExpectThrow<std::invalid_argument>(
                [&] { lease.store().SetBlock(0, 0, {{"id", std::uint64_t{256}}}); }, "out of range 0..255");
        }
        chunkdb::TableCatalog catalog(config);
        auto lease = *catalog.Find("world")->Acquire();
        ExpectRow(lease.store(), 0, 0, Row{std::uint64_t{200}, std::uint64_t{15}, std::string("abc")});
        ExpectRow(lease.store(), 9, 9, Row{std::uint64_t{70}, std::uint64_t{15}, std::string("ab")});
    }
}

// A crash leaves the narrowing in the manifest; the next open drops it. A
// failure while reading drops it at once.
void TestInterruptedNarrowing() {
    ScopedTempDir dir("chunkdb-narrowing-interrupted");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    {
        chunkdb::TableCatalog catalog(config);
        (void)catalog.Create("world", GeometryFor(Initial()), chunkdb::TableOptions{}, Initial());
        auto lease = *catalog.Find("world")->Acquire();
        lease.store().SetBlock(0, 0, {{"id", std::uint64_t{5}}});
        SetEnv("CHUNKDB_FAILPOINT_NARROW_SCAN_FAIL_ONCE", "1");
    }
    {
        chunkdb::TableCatalog catalog(config);
        ExpectThrow<std::runtime_error>(
            [&] { catalog.NarrowColumn("world", "id", Type(ColumnKind::kUnsigned, 8)); }, "injected failure reading");
        SetEnv("CHUNKDB_FAILPOINT_NARROW_SCAN_FAIL_ONCE", "");
        assert(catalog.Find("world")->Info().schema == Initial());
    }
    // What a crash between the first and last manifest write leaves.
    const auto table_dir = dir.path() / "tables" / "world";
    auto manifest = *chunkdb::ReadStoreManifest(table_dir);
    manifest.schema = chunkdb::WithPendingNarrowing(manifest.schema, "id", Type(ColumnKind::kUnsigned, 8));
    {
        const auto bytes = chunkdb::SerializeStoreManifest(manifest);
        std::ofstream out(chunkdb::StoreManifestPath(table_dir), std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        assert(out);
    }
    chunkdb::TableCatalog catalog(config);
    assert(catalog.Find("world")->Info().schema == Initial());
    assert(!chunkdb::ReadStoreManifest(table_dir)->schema.pending.has_value());
    auto lease = *catalog.Find("world")->Acquire();
    lease.store().SetBlock(1, 0, {{"id", std::uint64_t{900}}});
}

}  // namespace

int main() {
    TestRules();
    TestWritesWhilePending();
    TestNarrowTable();
    TestInterruptedNarrowing();
    return 0;
}
