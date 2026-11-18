// ALTER COLUMN SET/DROP DEFAULT, SET/DROP NULL and SET/DROP REQUIRED (#145):
// the schema rules, translation of older chunks, the DROP NULL check over
// stored values, writes refused while it lasts, an interrupted check, and
// the statements, also inside MIGRATE.

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/table_catalog.hpp"
#include "migrations_records.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"

namespace {

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
            std::abort();
        }
        return;
    }
    std::fprintf(stderr, "expected an exception with '%s'\n", part.c_str());
    std::abort();
}

Column MakeColumn(std::uint32_t id, std::string name, ColumnKind kind, std::uint32_t size) {
    Column column;
    column.id = id;
    column.name = std::move(name);
    column.type = ColumnType{.kind = kind, .size = size};
    return column;
}

// id u10, light u4 DEFAULT 15, sign text(8) NULL, level u8 NULL, note text(8).
TableSchema Initial() {
    auto light = MakeColumn(2, "light", ColumnKind::kUnsigned, 4);
    light.has_default = true;
    light.default_value = {15};
    auto sign = MakeColumn(3, "sign", ColumnKind::kText, 8);
    sign.nullable = true;
    auto level = MakeColumn(4, "level", ColumnKind::kUnsigned, 8);
    level.nullable = true;
    return TableSchema{
        .version = 1,
        .next_column_id = 6,
        .columns = {MakeColumn(1, "id", ColumnKind::kUnsigned, 10), light, sign, level,
                    MakeColumn(5, "note", ColumnKind::kText, 8)},
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

Column Attributes(const TableSchema& schema, std::size_t at) {
    return schema.columns.at(at);
}

void ExpectRoundTrip(const TableSchema& schema) {
    const auto bytes = chunkdb::EncodeTableSchema(schema);
    assert(chunkdb::DecodeTableSchema(bytes.data(), bytes.size()) == schema);
}

void TestRules() {
    const auto v1 = Initial();
    // SET DEFAULT, DROP DEFAULT, SET NULL, SET and DROP REQUIRED: one
    // version each, undone exactly.
    auto light = Attributes(v1, 1);
    light.default_value = {3};
    const auto v2 = chunkdb::ChangeColumnAttributes(v1, "light", light);
    assert(v2.version == 2U && v2.columns[1].default_value == std::vector<std::uint8_t>{3});
    assert(v2.history.back().changes.at(0).kind == chunkdb::SchemaChange::Kind::kChangeAttributes);
    assert(chunkdb::SchemaAtVersion(v2, 1) == v1);
    ExpectRoundTrip(v2);
    light.has_default = false;
    light.default_value.clear();
    const auto v3 = chunkdb::ChangeColumnAttributes(v2, "light", light);
    assert(!v3.columns[1].has_default && chunkdb::SchemaAtVersion(v3, 2) == v2);
    auto id = Attributes(v3, 0);
    id.nullable = true;
    const auto v4 = chunkdb::ChangeColumnAttributes(v3, "id", id);
    auto note = Attributes(v4, 4);
    note.required = true;
    const auto v5 = chunkdb::ChangeColumnAttributes(v4, "note", note);
    note.required = false;
    const auto v6 = chunkdb::ChangeColumnAttributes(v5, "note", note);
    assert(v6.version == 6U && v6.columns[0].nullable && !v6.columns[4].required);
    assert(chunkdb::SchemaAtVersion(v6, 1) == v1);
    ExpectRoundTrip(v6);

    // Refused: no change, NULL with REQUIRED, and DROP NULL without its check.
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnAttributes(v1, "light", Attributes(v1, 1)); }, "already has these attributes");
    auto sign = Attributes(v1, 2);
    sign.required = true;
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnAttributes(v1, "sign", sign); }, "cannot be both NULL and REQUIRED; DROP NULL first");
    auto required = Attributes(v5, 4);
    required.nullable = true;
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnAttributes(v5, "note", required); }, "DROP REQUIRED first");
    sign = Attributes(v1, 2);
    sign.nullable = false;
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnAttributes(v1, "sign", sign); }, "DROP NULL checks every stored value first");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnAttributes(v1, "absent", sign); }, "the table has no column absent");
    // A DEFAULT the type does not hold.
    light = Attributes(v1, 1);
    light.default_value = {16};
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnAttributes(v1, "light", light); }, "bits past the type's width");

    // DROP NULL: a recorded check, then the next version.
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::WithPendingNotNull(v1, "id"); }, "column id is not NULL");
    const auto pending = chunkdb::WithPendingNotNull(v1, "sign");
    assert(pending.version == 1U && pending.pending && pending.pending->not_null && pending.pending->column_id == 3U);
    ExpectRoundTrip(pending);
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::WithPendingNotNull(pending, "level"); }, "another narrowing");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::NarrowColumnType(pending, "sign", ColumnType{.kind = ColumnKind::kText, .size = 8}); },
        "no narrowing to text(8) in progress");
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::DropColumnNull(v1, "sign"); }, "no DROP NULL check in progress");
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::DropColumnNull(pending, "level"); }, "no DROP NULL check in progress");
    const auto dropped = chunkdb::DropColumnNull(pending, "sign");
    assert(dropped.version == 2U && !dropped.pending && !dropped.columns[2].nullable);
    assert(chunkdb::SchemaAtVersion(dropped, 1) == v1);
    assert(chunkdb::WithoutPendingNarrowing(pending) == v1);
    ExpectRoundTrip(dropped);

    // Chunk columns record no attribute change.
    auto chunks = chunkdb::EmptyChunkSchemaFor(v2);
    chunks.history.back() = v2.history.back();
    ExpectThrow<std::invalid_argument>([&] { chunkdb::ValidateChunkSchema(chunks); }, "chunk columns cannot record an attribute change");
}

using Row = std::vector<ColumnValue>;

void ExpectRow(chunkdb::ChunkStore& store, std::int64_t x, std::int64_t y, const Row& want) {
    if (store.GetBlock(x, y) != std::optional<Row>(want)) {
        std::fprintf(stderr, "block (%lld,%lld) does not hold the expected values\n", static_cast<long long>(x),
                     static_cast<long long>(y));
        std::abort();
    }
}

// Chunks written before SET NULL and DROP NULL read as they were: an empty
// text stays empty, a value stays a value; images and WALs alike.
void TestTranslation() {
    for (const std::size_t checkpoint_updates : {std::size_t{1}, std::size_t{100000}}) {
        ScopedTempDir dir("chunkdb-alter-column-translation");
        chunkdb::CatalogConfig config;
        config.data_dir = dir.path();
        {
            chunkdb::TableCatalog catalog(config);
            chunkdb::TableOptions options;
            options.checkpoint_update_interval = checkpoint_updates;
            (void)catalog.Create("world", GeometryFor(Initial()), options, Initial());
            auto lease = *catalog.Find("world")->Acquire();
            lease.store().SetBlock(0, 0, {{"id", std::uint64_t{1}}, {"sign", std::string("")}, {"note", std::string("")}});
            lease.store().SetBlock(1, 0, {{"id", std::uint64_t{2}}, {"sign", std::string("ab")}, {"level", std::uint64_t{9}},
                                          {"note", std::string("cd")}});
        }
        {
            chunkdb::TableCatalog catalog(config);
            // SET NULL on a text and a fixed column, then DROP NULL of both.
            catalog.ChangeColumns("world", [](const TableSchema& schema) {
                auto note = schema.columns[4];
                note.nullable = true;
                return chunkdb::ChangeColumnAttributes(schema, "note", note);
            });
            catalog.ChangeColumns("world", [](const TableSchema& schema) {
                auto id = schema.columns[0];
                id.nullable = true;
                return chunkdb::ChangeColumnAttributes(schema, "id", id);
            });
            auto lease = *catalog.Find("world")->Acquire();
            ExpectRow(lease.store(), 0, 0, Row{std::uint64_t{1}, std::uint64_t{15}, std::string(""), std::monostate{}, std::string("")});
            ExpectRow(lease.store(), 1, 0, Row{std::uint64_t{2}, std::uint64_t{15}, std::string("ab"), std::uint64_t{9}, std::string("cd")});
            lease.store().SetBlock(2, 0, {{"note", std::monostate{}}});
        }
        chunkdb::TableCatalog catalog(config);
        {
            auto lease = *catalog.Find("world")->Acquire();
            ExpectRow(lease.store(), 0, 0, Row{std::uint64_t{1}, std::uint64_t{15}, std::string(""), std::monostate{}, std::string("")});
            ExpectRow(lease.store(), 2, 0, Row{std::monostate{}, std::uint64_t{15}, std::monostate{}, std::monostate{}, std::monostate{}});
            lease.store().UnsetBlock(2, 0);
        }
        catalog.DropColumnNull("world", "sign");
        catalog.DropColumnNull("world", "note");
        catalog.DropColumnNull("world", "id");
        const auto schema = catalog.Find("world")->Info().schema;
        assert(schema.version == 6U && !schema.columns[0].nullable && !schema.columns[2].nullable && !schema.columns[4].nullable);
        auto lease = *catalog.Find("world")->Acquire();
        ExpectRow(lease.store(), 0, 0, Row{std::uint64_t{1}, std::uint64_t{15}, std::string(""), std::monostate{}, std::string("")});
        ExpectRow(lease.store(), 1, 0, Row{std::uint64_t{2}, std::uint64_t{15}, std::string("ab"), std::uint64_t{9}, std::string("cd")});
    }
}

// While a DROP NULL check lasts, every write path refuses a NULL of the
// column, also one a new block would take by omitting it.
void TestWritesWhilePending() {
    for (const char* column : {"level", "sign"}) {
        ScopedTempDir dir("chunkdb-drop-null-pending");
        chunkdb::StoreConfig config;
        config.data_dir = dir.path();
        const auto schema = chunkdb::WithPendingNotNull(Initial(), column);
        config.schema = schema;
        config.geometry = GeometryFor(schema);
        chunkdb::ChunkStore store(config);
        const bool text = std::string(column) == "sign";
        const ColumnValue value = text ? ColumnValue(std::string("x")) : ColumnValue(std::uint64_t{3});
        store.SetBlock(0, 0, {{"id", std::uint64_t{1}}, {column, value}});
        ExpectThrow<std::invalid_argument>(
            [&] { store.SetBlock(1, 0, {{"id", std::uint64_t{2}}}); }, "a new block must give it while DROP NULL checks its values");
        ExpectThrow<std::invalid_argument>(
            [&] { store.SetBlock(0, 0, {{column, std::monostate{}}}); }, "cannot be NULL while DROP NULL checks its values");
        // An existing block keeps its value when a write omits the column.
        store.SetBlock(0, 0, {{"id", std::uint64_t{5}}});
        assert(!store.GetBlock(1, 0).has_value());
        // A whole-chunk write that makes a block present without the value.
        const auto payload = store.GetChunkPayloadBytes(0, 0);
        ExpectThrow<std::invalid_argument>(
            [&] { (void)store.SetChunkStateBytes(0, 0, payload, std::vector<std::uint8_t>{0x03, 0x00}); },
            "as block index 1 is");
        ExpectThrow<std::invalid_argument>(
            [&] {
                (void)store.CasChunkStateBytes(0, 0, store.GetChunkVersion(0, 0), payload, std::vector<std::uint8_t>{0x03, 0x00});
            },
            "as block index 1 is");
        (void)store.SetChunkStateBytes(0, 0, payload, std::vector<std::uint8_t>{0x01, 0x00});
        ExpectRow(store, 0, 0, Row{std::uint64_t{5}, std::uint64_t{15}, text ? value : ColumnValue(std::monostate{}),
                                   text ? ColumnValue(std::monostate{}) : value, std::string("")});
    }
}

void TestDropNullTable() {
    for (const std::size_t checkpoint_updates : {std::size_t{1}, std::size_t{100000}}) {
        ScopedTempDir dir("chunkdb-drop-null");
        chunkdb::CatalogConfig config;
        config.data_dir = dir.path();
        {
            chunkdb::TableCatalog catalog(config);
            chunkdb::TableOptions options;
            options.checkpoint_update_interval = checkpoint_updates;
            (void)catalog.Create("world", GeometryFor(Initial()), options, Initial());
            {
                auto lease = *catalog.Find("world")->Acquire();
                lease.store().SetBlock(0, 0, {{"id", std::uint64_t{1}}, {"sign", std::string("abc")}, {"level", std::uint64_t{4}}});
                lease.store().SetBlock(9, 9, {{"id", std::uint64_t{2}}, {"level", std::uint64_t{5}}});
                lease.store().SetBlock(6, 1, {{"id", std::uint64_t{3}}, {"sign", std::string("")}});
            }
            // A NULL: refused with its block, the schema as it was.
            ExpectThrow<std::invalid_argument>(
                [&] { catalog.DropColumnNull("world", "sign"); },
                "column sign cannot drop NULL: block (9, 9) holds NULL; set a value there first");
            ExpectThrow<std::invalid_argument>(
                [&] { catalog.DropColumnNull("world", "level"); }, "block (6, 1) holds NULL");
            assert(catalog.Find("world")->Info().schema == Initial());
            ExpectThrow<std::invalid_argument>([&] { catalog.DropColumnNull("world", "id"); }, "column id is not NULL");
            {
                auto lease = *catalog.Find("world")->Acquire();
                lease.store().SetBlock(9, 9, {{"sign", std::string("x")}});
                lease.store().SetBlock(6, 1, {{"level", std::uint64_t{6}}});
            }
            catalog.DropColumnNull("world", "sign");
            catalog.DropColumnNull("world", "level");
            const auto info = catalog.Find("world")->Info();
            assert(info.schema.version == 3U && !info.schema.pending);
            assert(!info.schema.columns[2].nullable && !info.schema.columns[3].nullable);
            auto lease = *catalog.Find("world")->Acquire();
            ExpectThrow<std::invalid_argument>(
                [&] { lease.store().SetBlock(0, 0, {{"sign", std::monostate{}}}); }, "column sign cannot be NULL");
        }
        chunkdb::TableCatalog catalog(config);
        auto lease = *catalog.Find("world")->Acquire();
        ExpectRow(lease.store(), 0, 0, Row{std::uint64_t{1}, std::uint64_t{15}, std::string("abc"), std::uint64_t{4}, std::string("")});
        ExpectRow(lease.store(), 9, 9, Row{std::uint64_t{2}, std::uint64_t{15}, std::string("x"), std::uint64_t{5}, std::string("")});
        ExpectRow(lease.store(), 6, 1, Row{std::uint64_t{3}, std::uint64_t{15}, std::string(""), std::uint64_t{6}, std::string("")});
        // A new block without the column takes zero or empty now.
        lease.store().SetBlock(2, 2, {{"id", std::uint64_t{7}}});
        ExpectRow(lease.store(), 2, 2, Row{std::uint64_t{7}, std::uint64_t{15}, std::string(""), std::uint64_t{0}, std::string("")});
    }
}

// What a crash during the check leaves: the next start drops the check and
// the column still takes NULL.
void TestInterruptedDropNull() {
    ScopedTempDir dir("chunkdb-drop-null-interrupted");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    {
        chunkdb::TableCatalog catalog(config);
        (void)catalog.Create("world", GeometryFor(Initial()), chunkdb::TableOptions{}, Initial());
    }
    const auto table_dir = dir.path() / "tables" / "world";
    auto manifest = *chunkdb::ReadStoreManifest(table_dir);
    manifest.schema = chunkdb::WithPendingNotNull(manifest.schema, "sign");
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
    lease.store().SetBlock(1, 0, {{"id", std::uint64_t{9}}});
    ExpectRow(lease.store(), 1, 0, Row{std::uint64_t{9}, std::uint64_t{15}, std::monostate{}, std::monostate{}, std::string("")});
}

// The statements, directly and as named migrations.
void TestStatements() {
    for (const bool migrate : {false, true}) {
        ScopedTempDir dir("chunkdb-alter-column-engine");
        chunkdb::CatalogConfig config;
        config.data_dir = dir.path();
        auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
        chunkdb::EngineConfig engine_config;
        engine_config.require_auth = false;
        chunkdb::CommandEngine engine(engine_config, catalog);
        chunkdb::SessionState session;
        assert(engine.Execute(session, "HELLO 3").front() == '%');
        assert(engine.Execute(session, "CREATE TABLE world (id u16, light u8 DEFAULT 7, sign text(8) NULL) CHUNK 4 x 4") == "+OK\r\n");
        assert(engine.Execute(session, "SET BLOCK 0 0 IN world id=1").front() == ':');
        std::size_t step = 0U;
        const auto run = [&](const std::string& statement) {
            const auto command = (migrate ? "MIGRATE 'step_" + std::to_string(step++) + "' " : std::string{}) + statement;
            return engine.Execute(session, command);
        };
        const std::string ok = migrate ? "+applied\r\n" : "+OK\r\n";
        assert(run("ALTER TABLE world ALTER COLUMN light SET DEFAULT 9") == ok);
        assert(engine.Execute(session, "SET BLOCK 1 0 IN world id=2").front() == ':');
        assert(engine.Execute(session, "GET BLOCK 1 0 FROM world COLUMNS light") == "*1\r\n:9\r\n");
        // Blocks that had the old DEFAULT keep it.
        assert(engine.Execute(session, "GET BLOCK 0 0 FROM world COLUMNS light") == "*1\r\n:7\r\n");
        assert(run("ALTER TABLE world ALTER COLUMN light DROP DEFAULT") == ok);
        assert(engine.Execute(session, "SET BLOCK 2 0 IN world id=3").front() == ':');
        assert(engine.Execute(session, "GET BLOCK 2 0 FROM world COLUMNS light") == "*1\r\n:0\r\n");
        assert(run("ALTER TABLE world ALTER COLUMN light SET REQUIRED") == ok);
        assert(Contains(engine.Execute(session, "SET BLOCK 3 0 IN world id=4"), "column light is REQUIRED"));
        assert(run("ALTER TABLE world ALTER COLUMN light DROP REQUIRED") == ok);
        assert(run("ALTER TABLE world ALTER COLUMN light SET NULL") == ok);
        assert(engine.Execute(session, "SET BLOCK 3 0 IN world id=4").front() == ':');
        assert(engine.Execute(session, "GET BLOCK 3 0 FROM world COLUMNS light") == "*1\r\n_\r\n");
        // A NULL refuses DROP NULL and leaves no migration record.
        const auto refused = run("ALTER TABLE world ALTER COLUMN light DROP NULL");
        assert(refused.starts_with("-ERR INVALID_ARGUMENT ") && Contains(refused, "block (3, 0) holds NULL"));
        assert(chunkdb::ReadMigrationRecords(dir.path()).size() == (migrate ? 5U : 0U));
        assert(engine.Execute(session, "SET BLOCK 3 0 IN world light=5").front() == ':');
        assert(run("ALTER TABLE world ALTER COLUMN light DROP NULL") == ok);
        assert(Contains(engine.Execute(session, "SET BLOCK 3 0 IN world light=NULL"), "cannot be NULL"));
        const auto schema = catalog->Find("world")->Info().schema;
        assert(schema.version == 7U && !schema.columns[1].nullable && !schema.pending);
        // Refusals.
        assert(Contains(run("ALTER TABLE world ALTER COLUMN light DROP DEFAULT"), "column light (u8) has no DEFAULT"));
        assert(Contains(run("ALTER TABLE world ALTER COLUMN light DROP REQUIRED"), "already has these attributes"));
        assert(Contains(run("ALTER TABLE world ALTER COLUMN light SET DEFAULT NULL"), "cannot be NULL, so its DEFAULT"));
        assert(Contains(run("ALTER TABLE world ALTER COLUMN light SET DEFAULT 300"), "out of range"));
        assert(Contains(run("ALTER TABLE world ALTER COLUMN sign SET REQUIRED"), "DROP NULL first"));
        assert(Contains(run("ALTER TABLE world ALTER COLUMN gone SET NULL"), "the table has no column gone"));
        assert(catalog->Find("world")->Info().schema == schema);
        // SET DEFAULT NULL on a NULL column drops its DEFAULT.
        assert(run("ALTER TABLE world ALTER COLUMN sign SET DEFAULT 'hi'") == ok);
        assert(run("ALTER TABLE world ALTER COLUMN sign SET DEFAULT NULL") == ok);
        assert(!catalog->Find("world")->Info().schema.columns[2].has_default);
    }
}

}  // namespace

int main() {
    TestRules();
    TestTranslation();
    TestWritesWhilePending();
    TestDropNullTable();
    TestInterruptedDropNull();
    TestStatements();
    std::printf("alter column tests passed\n");
    return 0;
}
