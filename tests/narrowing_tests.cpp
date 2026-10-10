// Narrowing a column after checking every stored value (#61): the pending
// narrowing and its rules, writes refused while it lasts, the check over
// images and WALs, a value that does not fit, an interrupted check, and a
// read failure during it.

#include <cassert>
#include <condition_variable>
#include <future>
#include <mutex>
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
#include "chunkdb/engine.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/table_catalog.hpp"
#include "store_manifest.hpp"
#include "chunk_store_internal.hpp"
#include "migrations_records.hpp"
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
        auto state = text_store.ReadChunkState(0, 0);
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
                "column id cannot be narrowed to u8: block (9, 9) holds 700; u8 holds 0..255");
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

void TestRequiredOnEmptyEngine(bool migrate) {
    ScopedTempDir dir("chunkdb-required-empty");
    chunkdb::CatalogConfig config; config.data_dir = dir.path();
    auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
    chunkdb::EngineConfig engine_config; engine_config.require_auth = false;
    chunkdb::CommandEngine engine(engine_config, catalog);
    chunkdb::SessionState session;
    assert(engine.Execute(session, "HELLO 3").front() == '%');
    assert(engine.Execute(session, "CREATE TABLE world (id u16) CHUNK 4 x 4") == "+OK\r\n");
    const auto command = migrate ? "MIGRATE 'required' ALTER TABLE world ADD COLUMN must u8 REQUIRED" :
                                   "ALTER TABLE world ADD COLUMN must u8 REQUIRED";
    const auto reply = engine.Execute(session, command);
    const auto expected = migrate ? "+applied\r\n" : "+OK\r\n";
    if (reply != expected) { std::fprintf(stderr, "empty REQUIRED expected [%s], got [%s]\n", expected, reply.c_str()); std::abort(); }
    assert(catalog->Find("world")->Info().schema.columns.back().required);
}

void TestRequiredColumnDataGate(bool migrate) {
    for (const auto* kind : {"cached_empty", "dirty", "wal", "image", "deleted_memory", "deleted_disk"}) {
        ScopedTempDir dir("chunkdb-required-data-gate");
        chunkdb::CatalogConfig config; config.data_dir = dir.path();
        config.default_options.checkpoint_update_interval = std::string(kind) == "image" ? 1U : 100000U;
        config.max_loaded_chunks = 1U;
        std::shared_ptr<chunkdb::TableCatalog> catalog;
        std::unique_ptr<chunkdb::CommandEngine> engine;
        std::unique_ptr<chunkdb::SessionState> session;
        const auto open = [&] {
            catalog = std::make_shared<chunkdb::TableCatalog>(config);
            chunkdb::EngineConfig options; options.require_auth = false;
            engine = std::make_unique<chunkdb::CommandEngine>(options, catalog);
            session = std::make_unique<chunkdb::SessionState>();
            assert(engine->Execute(*session, "HELLO 3").front() == '%');
        };
        open();
        assert(engine->Execute(*session, "CREATE TABLE world (id u16) CHUNK 4 x 4") == "+OK\r\n");
        if (std::string(kind) == "cached_empty") {
            assert(engine->Execute(*session, "GET BLOCK 0 0 FROM world") == "_\r\n");
        } else {
            assert(engine->Execute(*session, "SET BLOCK 0 0 IN world id=1").front() == ':');
            if (std::string(kind).starts_with("deleted"))
                assert(engine->Execute(*session, "DELETE BLOCK 0 0 FROM world").front() == ':');
        }
        if (std::string(kind) == "wal" || std::string(kind) == "image" || std::string(kind) == "deleted_disk") {
            engine.reset(); catalog.reset(); open();
            auto lease = catalog->Find("world")->Acquire();
            assert(lease->store().ApproxLoadedChunkCount() == 0U);
        }
        const bool populated = std::string(kind) == "dirty" || std::string(kind) == "wal" || std::string(kind) == "image";
        const auto manifest = chunkdb::LoadFile(dir.path() / "tables/world/table.manifest");
        const auto command = std::string(migrate ? "MIGRATE 'required' " : "") + "ALTER TABLE world ADD COLUMN must u8 REQUIRED";
        const auto reply = engine->Execute(*session, command);
        if (populated) {
            assert(reply.starts_with("-ERR INVALID_ARGUMENT ") && Contains(reply, "needs a DEFAULT"));
            assert(catalog->Find("world")->Info().schema.version == 1U);
            assert(chunkdb::LoadFile(dir.path() / "tables/world/table.manifest") == manifest);
            assert(chunkdb::ReadMigrationRecords(dir.path()).empty() && !chunkdb::ReadMigrationJournal(dir.path()));
            // The failed named attempt records nothing and can succeed after
            // the last block is deleted under the same migration name.
            assert(engine->Execute(*session, "DELETE BLOCK 0 0 FROM world").front() == ':');
            assert(engine->Execute(*session, command) == (migrate ? "+applied\r\n" : "+OK\r\n"));
        } else {
            assert(reply == (migrate ? "+applied\r\n" : "+OK\r\n"));
        }
        assert(catalog->Find("world")->Info().schema.version == 2U);
        assert(engine->Execute(*session, "SET BLOCK 0 0 IN world id=2").starts_with("-ERR INVALID_ARGUMENT "));
        assert(engine->Execute(*session, "SET BLOCK 0 0 IN world id=2,must=7").front() == ':');
        assert(engine->Execute(*session, "GET BLOCK 0 0 FROM world COLUMNS must") == "*1\r\n:7\r\n");
    }
}

struct PauseNarrowing final : chunkdb::MigrationTestHook {
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false, released = false;
    void Run(Point point, std::string_view) override {
        if (point != Point::kBeforeNarrowingScan) return;
        std::unique_lock lock(mutex); entered = true; cv.notify_all();
        cv.wait(lock, [&] { return released; });
    }
    void Wait() { std::unique_lock lock(mutex); assert(cv.wait_for(lock, std::chrono::seconds(10), [&] { return entered; })); }
    void Release() { std::lock_guard lock(mutex); released = true; cv.notify_all(); }
};

void TestConcurrentNarrowingWriter() {
    ScopedTempDir dir("chunkdb-narrowing-concurrent");
    chunkdb::CatalogConfig config; config.data_dir = dir.path();
    auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
    chunkdb::EngineConfig engine_config; engine_config.require_auth = false;
    chunkdb::CommandEngine engine(engine_config, catalog);
    chunkdb::SessionState ddl_session, write_session;
    assert(engine.Execute(ddl_session, "HELLO 3").front() == '%');
    assert(engine.Execute(write_session, "HELLO 3").front() == '%');
    assert(engine.Execute(ddl_session, "CREATE TABLE world (id u16) CHUNK 4 x 4") == "+OK\r\n");
    assert(engine.Execute(ddl_session, "SET BLOCK 0 0 IN world id=100").front() == ':');
    PauseNarrowing hook; catalog->SetMigrationTestHook(&hook);
    auto narrow = std::async(std::launch::async, [&] { return engine.Execute(ddl_session, "ALTER TABLE world ALTER COLUMN id TYPE u8"); });
    hook.Wait();
    assert(catalog->Find("world")->Info().schema.pending);
    auto writer = std::async(std::launch::async, [&] {
        const auto refused = engine.Execute(write_session, "SET BLOCK 0 0 IN world id=700");
        assert(refused.starts_with("-ERR INVALID_ARGUMENT ") && Contains(refused, "being narrowed to u8"));
        return engine.Execute(write_session, "SET BLOCK 1 0 IN world id=255");
    });
    assert(writer.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    assert(writer.get().front() == ':');
    hook.Release();
    assert(narrow.get() == "+OK\r\n"); catalog->SetMigrationTestHook(nullptr);
    assert(engine.Execute(write_session, "GET BLOCK 0 0 FROM world COLUMNS id") == "*1\r\n:100\r\n");
    assert(engine.Execute(write_session, "GET BLOCK 1 0 FROM world COLUMNS id") == "*1\r\n:255\r\n");
    assert(catalog->Find("world")->Info().schema.columns[0].type == Type(ColumnKind::kUnsigned, 8));
}

void TestNarrowingRangeFamilies() {
    const std::vector<std::vector<std::string>> cases{
        {"i16", "i8", "-129", "i8 holds -128..127"},
        {"f64", "f32", "1e40", "f32 holds finite values in -3.4028234663852886e+38..3.4028234663852886e+38"},
        {"text(8)", "text(3)", "'four'", "text(3) holds 0..3 UTF-8 bytes"},
        {"bytes(8)", "bytes(3)", "x'01020304'", "bytes(3) holds 0..3 bytes"}};
    for (const auto& values : cases) {
        for (const bool migrate : {false, true}) {
            ScopedTempDir dir("chunkdb-narrowing-range");
            chunkdb::CatalogConfig config; config.data_dir = dir.path();
            auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
            chunkdb::EngineConfig engine_config; engine_config.require_auth = false;
            chunkdb::CommandEngine engine(engine_config, catalog); chunkdb::SessionState session;
            assert(engine.Execute(session, "HELLO 3").front() == '%');
            assert(engine.Execute(session, "CREATE TABLE world (id u8, v " + values[0] + ") CHUNK 4 x 4") == "+OK\r\n");
            assert(engine.Execute(session, "SET BLOCK 0 0 IN world id=1,v=" + values[2]).front() == ':');
            const auto text = std::string(migrate ? "MIGRATE 'narrow' " : "") + "ALTER TABLE world ALTER COLUMN v TYPE " + values[1];
            const auto reply = engine.Execute(session, text);
            assert(reply.starts_with("-ERR INVALID_ARGUMENT ") && Contains(reply, values[3]));
            assert(!catalog->Find("world")->Info().schema.pending);
            assert(catalog->Find("world")->Info().schema.version == 1U);
        }
    }
    assert(chunkdb::ColumnTypeRange(Type(ColumnKind::kUnsigned, 64)) == "u64 holds 0..18446744073709551615");
    assert(chunkdb::ColumnTypeRange(Type(ColumnKind::kSigned, 64)) == "i64 holds -9223372036854775808..9223372036854775807");
}

void TestEngineDefaultAndTruncate() {
    for (const bool migrate : {false, true}) {
        ScopedTempDir dir("chunkdb-engine-conversions");
        chunkdb::CatalogConfig config; config.data_dir = dir.path();
        auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
        chunkdb::EngineConfig engine_config; engine_config.require_auth = false;
        chunkdb::CommandEngine engine(engine_config, catalog); chunkdb::SessionState session;
        assert(engine.Execute(session, "HELLO 3").front() == '%');
        assert(engine.Execute(session, "CREATE TABLE world (id u16, light u8 DEFAULT 7, sign text(8) DEFAULT 'hi', blob bytes(8), flags bits(8)) CHUNK 4 x 4") == "+OK\r\n");
        assert(engine.Execute(session, "SET BLOCK 0 0 IN world id=700,light=200,sign='😀abc',blob=x'01020304',flags=b'10111111'").front() == ':');
        std::size_t step = 0U;
        for (const auto* statement : {"ALTER TABLE world ALTER COLUMN light TYPE u4 USING DEFAULT",
                                      "ALTER TABLE world ALTER COLUMN sign TYPE text(5) USING TRUNCATE",
                                      "ALTER TABLE world ALTER COLUMN blob TYPE bytes(2) USING TRUNCATE",
                                      "ALTER TABLE world ALTER COLUMN flags TYPE bits(2) USING TRUNCATE"}) {
            const auto command = std::string(migrate ? "MIGRATE 'convert_" + std::to_string(step++) + "' " : "") + statement;
            assert(engine.Execute(session, command) == (migrate ? "+applied\r\n" : "+OK\r\n"));
        }
        assert(engine.Execute(session, "GET BLOCK 0 0 FROM world COLUMNS light") == "*1\r\n:7\r\n");
        assert(engine.Execute(session, "GET BLOCK 0 0 FROM world COLUMNS sign") == "*1\r\n$5\r\n😀a\r\n");
        auto lease = catalog->Find("world")->Acquire();
        const auto row = *lease->store().GetBlock(0, 0);
        assert(std::get<chunkdb::BytesValue>(row[3]).bytes == std::vector<std::uint8_t>({1U, 2U}));
        assert(std::get<BitsValue>(row[4]).digits == "10");
        lease.reset();
        const auto geometry = catalog->Find("world")->Info().geometry;
        for (const auto* statement : {"ALTER TABLE world SET chunk_width_blocks = 8", "ALTER TABLE world SET chunk_height_blocks = 8",
                                      "ALTER TABLE world CHUNK 8 x 8"}) {
            assert(engine.Execute(session, statement).starts_with("-ERR "));
            assert(chunkdb::SameGeometry(catalog->Find("world")->Info().geometry, geometry));
        }
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

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--required-empty") { TestRequiredOnEmptyEngine(false); return 0; }
    if (argc == 2 && std::string(argv[1]) == "--required-migration") { TestRequiredOnEmptyEngine(true); return 0; }
    if (argc == 2 && std::string(argv[1]) == "--range") { TestNarrowTable(); return 0; }
    TestRequiredOnEmptyEngine(false); TestRequiredOnEmptyEngine(true);
    TestRequiredColumnDataGate(false); TestRequiredColumnDataGate(true);
    TestConcurrentNarrowingWriter(); TestNarrowingRangeFamilies(); TestEngineDefaultAndTruncate();
    TestRules();
    TestWritesWhilePending();
    TestNarrowTable();
    TestInterruptedNarrowing();
    return 0;
}
