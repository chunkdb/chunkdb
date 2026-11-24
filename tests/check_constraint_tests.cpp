// CHECK constraints (#145): the schema rules and history, every write path,
// the ADD CHECK scan over stored blocks, writes refused while it lasts, an
// interrupted scan, persistence across a restart, and the statements, also
// inside MIGRATE and transactions.

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
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

using chunkdb::CheckConstraint;
using chunkdb::CheckFailedError;
using chunkdb::Column;
using chunkdb::ColumnKind;
using chunkdb::ColumnType;
using chunkdb::ColumnValue;
using chunkdb::TableSchema;
using chunkdb::test::ScopedTempDir;
using Row = std::vector<ColumnValue>;

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

void Expect(bool condition, const char* what) {
    if (!condition) {
        std::fprintf(stderr, "failed: %s\n", what);
        std::abort();
    }
}

Column MakeColumn(std::uint32_t id, std::string name, ColumnKind kind, std::uint32_t size) {
    Column column;
    column.id = id;
    column.name = std::move(name);
    column.type = ColumnType{.kind = kind, .size = size};
    return column;
}

// hp i16 DEFAULT 10, max_hp i16 NULL, tag text(8) NULL; no checks.
TableSchema Plain() {
    auto hp = MakeColumn(1, "hp", ColumnKind::kSigned, 16);
    hp.has_default = true;
    hp.default_value = chunkdb::EncodeColumnValue(hp, std::int64_t{10});
    auto max_hp = MakeColumn(2, "max_hp", ColumnKind::kSigned, 16);
    max_hp.nullable = true;
    auto tag = MakeColumn(3, "tag", ColumnKind::kText, 8);
    tag.nullable = true;
    return TableSchema{.version = 1, .next_column_id = 4, .columns = {hp, max_hp, tag}, .history = {}, .pending = std::nullopt};
}

// Plain() with hp >= 0, hp <= max_hp (NULL passes) and tag <> 'bad'.
TableSchema Checked() {
    auto schema = Plain();
    schema.checks = {
        CheckConstraint{.name = "hp_check", .expression = "hp >= 0"},
        CheckConstraint{.name = "hp_max", .expression = "hp <= max_hp"},
        CheckConstraint{.name = "tag_check", .expression = "tag <> 'bad'"},
    };
    chunkdb::ValidateTableSchema(schema);
    return schema;
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

void ExpectRoundTrip(const TableSchema& schema) {
    const auto bytes = chunkdb::EncodeTableSchema(schema);
    Expect(chunkdb::DecodeTableSchema(bytes.data(), bytes.size()) == schema, "schema round trip");
}

void ExpectRow(chunkdb::ChunkStore& store, std::int64_t x, std::int64_t y, const std::optional<Row>& want) {
    if (store.GetBlock(x, y) != want) {
        std::fprintf(stderr, "block (%lld,%lld) does not hold the expected values\n", static_cast<long long>(x),
                     static_cast<long long>(y));
        std::abort();
    }
}

void TestRules() {
    const auto columns = Plain().columns;
    const auto refused = [&](const std::string& expression, const std::string& part) {
        ExpectThrow<std::invalid_argument>(
            [&] { chunkdb::ValidateCheck(CheckConstraint{.name = "c", .expression = expression}, columns); }, part);
    };
    refused("hp > $1", "cannot use parameters");
    refused("chunk.biome = 1", "not chunk columns");
    refused("hp < NOW()", "not NOW()");
    refused("EXISTS", "not EXISTS");
    refused("gone > 1", "CHECK c:");
    refused("hp + 1", "CHECK c:");
    refused("hp >", "CHECK c:");
    ExpectThrow<std::invalid_argument>(
        [&] { chunkdb::ValidateCheck(CheckConstraint{.name = "Bad", .expression = "hp > 0"}, columns); }, "CHECK name 'Bad'");
    chunkdb::ValidateCheck(CheckConstraint{.name = "pos", .expression = "pos_x >= 0 AND pos_y < 100"}, columns);

    // Names: a column's, the first column read, then numbered.
    auto schema = Checked();
    Expect(chunkdb::NewCheckName(schema, "hp > 1") == "hp_check_2", "numbered name");
    Expect(chunkdb::NewCheckName(schema, "pos_x > 1") == "check", "name without columns");
    Expect(chunkdb::NewCheckName(schema, "hp > 1", "max_hp") == "max_hp_check", "column name");
    Expect(chunkdb::CheckColumnNames(schema.checks[1]) == std::vector<std::string>{"hp", "max_hp"}, "columns read");

    // ADD CHECK as a pending step, then version 2; DROP CHECK as version 3.
    ExpectRoundTrip(schema);
    auto pending = chunkdb::WithPendingCheck(schema, CheckConstraint{.name = "low", .expression = "hp < 1000"});
    ExpectRoundTrip(pending);
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::WithPendingCheck(pending, CheckConstraint{.name = "x", .expression = "hp < 9"}); },
        "another narrowing or CHECK");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::WithPendingCheck(schema, CheckConstraint{.name = "hp_max", .expression = "hp < 9"}); },
        "already has a CHECK named hp_max");
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::AddColumn(pending, MakeColumn(0, "z", ColumnKind::kBool, 1)); },
        "in progress");
    const auto added = chunkdb::AddPendingCheck(pending);
    Expect(added.version == 2U && !added.pending && added.checks.size() == 4U && added.checks[3].name == "low", "added");
    ExpectRoundTrip(added);
    const auto dropped = chunkdb::DropCheck(added, "hp_max");
    Expect(dropped.version == 3U && dropped.checks.size() == 3U && !chunkdb::HasCheck(dropped, "hp_max"), "dropped");
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::DropCheck(dropped, "hp_max"); }, "has no CHECK hp_max");
    ExpectRoundTrip(dropped);
    // Earlier versions keep their own checks.
    Expect(chunkdb::SchemaAtVersion(dropped, 1U).checks == schema.checks, "version 1 checks");
    Expect(chunkdb::SchemaAtVersion(dropped, 2U).checks == added.checks, "version 2 checks");

    // RENAME COLUMN rewrites the checks; earlier versions read the old name.
    const auto renamed = chunkdb::RenameColumn(dropped, "hp", "health");
    Expect(renamed.checks[0].expression == "health >= 0" && renamed.checks[2].expression == "health < 1000", "rewritten");
    const auto keyword = chunkdb::RenameColumn(renamed, "tag", "exists");
    Expect(keyword.checks[1].expression == "\"exists\" <> 'bad'", "keyword quoted");
    ExpectRoundTrip(keyword);
    Expect(chunkdb::SchemaAtVersion(renamed, 3U) == dropped, "rename undone");
    // A name quoted for the rename stays quoted when it is undone.
    Expect(chunkdb::SchemaAtVersion(keyword, 4U).checks[1].expression == "\"tag\" <> 'bad'", "quoted undone");
    // DROP COLUMN and value conversions of a column a check reads.
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::DropColumn(keyword, "health"); },
        "column health is read by CHECK hp_check; drop the check first");
    ExpectThrow<std::invalid_argument>(
        [&] {
            (void)chunkdb::ChangeColumnType(keyword, "health", ColumnType{.kind = ColumnKind::kSigned, .size = 8},
                chunkdb::Conversion::kClamp);
        },
        "which converted values could fail");
    const auto wider = chunkdb::ChangeColumnType(
        keyword, "health", ColumnType{.kind = ColumnKind::kSigned, .size = 32}, chunkdb::Conversion::kExact);
    Expect(wider.checks == keyword.checks, "widening keeps checks");
    // Narrowing too; a check that no longer plans refuses it first.
    (void)chunkdb::WithPendingNarrowing(wider, "health", ColumnType{.kind = ColumnKind::kSigned, .size = 16});
    auto bits = TableSchema{.version = 1, .next_column_id = 2,
        .columns = {MakeColumn(1, "b", ColumnKind::kBits, 8)}, .history = {}, .pending = std::nullopt};
    bits.checks = {CheckConstraint{.name = "b_check", .expression = "b <> b'00000000'"}};
    chunkdb::ValidateTableSchema(bits);
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::WithPendingNarrowing(bits, "b", ColumnType{.kind = ColumnKind::kBits, .size = 4}); },
        "column b cannot become bits(4)");
    // Chunk columns have none.
    auto chunk_schema = chunkdb::EmptyChunkSchemaFor(Plain());
    chunk_schema.checks = {CheckConstraint{.name = "c", .expression = "x > 0"}};
    ExpectThrow<std::invalid_argument>([&] { chunkdb::ValidateChunkSchema(chunk_schema); }, "chunk columns cannot have CHECK");
    // A table without checks encodes as before.
    const auto plain = chunkdb::EncodeTableSchema(Plain());
    auto with = chunkdb::EncodeTableSchema(Checked());
    Expect(with.size() > plain.size() && std::equal(plain.begin(), plain.end(), with.begin()), "suffix after history");
}

chunkdb::StoreConfig StoreConfigFor(const std::filesystem::path& dir, const TableSchema& schema) {
    chunkdb::StoreConfig config;
    config.data_dir = dir;
    config.schema = schema;
    config.geometry = GeometryFor(schema);
    return config;
}

// Every ChunkStore write path: the block after the write must pass, NULL
// passes, a refused write changes nothing.
void TestStoreWrites() {
    ScopedTempDir dir("chunkdb-check-store");
    chunkdb::ChunkStore store(StoreConfigFor(dir.path(), Checked()));
    const auto failed = [](const std::function<void()>& write, const std::string& part) {
        ExpectThrow<CheckFailedError>(write, part);
    };
    // A new block: its defaults count (hp 10, max_hp NULL passes hp_max).
    store.SetBlock(0, 0, {});
    ExpectRow(store, 0, 0, Row{std::int64_t{10}, std::monostate{}, std::monostate{}});
    failed([&] { store.SetBlock(1, 0, {{"hp", std::int64_t{-1}}}); }, "block (1, 0) fails CHECK hp_check (hp >= 0)");
    ExpectRow(store, 1, 0, std::nullopt);
    // An update is checked with the columns it keeps.
    store.SetBlock(0, 0, {{"max_hp", std::int64_t{20}}});
    failed([&] { store.SetBlock(0, 0, {{"hp", std::int64_t{21}}}); }, "fails CHECK hp_max");
    failed([&] { store.SetBlock(0, 0, {{"tag", std::string("bad")}}); }, "fails CHECK tag_check");
    ExpectRow(store, 0, 0, Row{std::int64_t{10}, std::int64_t{20}, std::monostate{}});
    // Conditional writes.
    const auto version = store.GetChunkVersion(0, 0);
    failed([&] { (void)store.SetBlock(0, 0, {{"hp", std::int64_t{-5}}}, version); }, "hp_check");
    Expect(store.SetBlock(0, 0, {{"hp", std::int64_t{15}}}, version).ok, "conditional write");
    // Whole-chunk writes: every present block of the result.
    auto state = store.ReadChunkState(0, 0);
    auto good = state;
    store.SetBlockInState(good, 1, 1, {{"hp", std::int64_t{3}}});
    auto bad = good;
    failed([&] { store.SetBlockInState(bad, 2, 1, {{"hp", std::int64_t{-3}}}); }, "block (2, 1) fails CHECK hp_check");
    Expect(bad.payload == good.payload && bad.presence_bitmap == good.presence_bitmap, "state unchanged");
    // A state made by hand that holds a failing block.
    bad.payload[0] = 0xFFU;  // hp of block 0 becomes negative
    bad.payload[1] = 0xFFU;
    failed([&] { (void)store.WriteChunkState(0, 0, bad, std::nullopt); }, "block (0, 0) fails CHECK hp_check");
    failed([&] { (void)store.WriteChunkState(0, 0, bad, store.GetChunkVersion(0, 0)); }, "hp_check");
    failed([&] { (void)store.SetChunkStateBytes(0, 0, bad.payload, bad.presence_bitmap); }, "hp_check");
    failed([&] { (void)store.CasChunkStateBytes(0, 0, store.GetChunkVersion(0, 0), bad.payload, bad.presence_bitmap); },
        "hp_check");
    failed(
        [&] {
            (void)store.TransformChunkState(chunkdb::ChunkCoord{0, 0}, std::nullopt, false,
                [&](chunkdb::ChunkState& next) { next.payload = bad.payload; });
        },
        "hp_check");
    ExpectRow(store, 0, 0, Row{std::int64_t{15}, std::int64_t{20}, std::monostate{}});
    Expect(store.WriteChunkState(0, 0, good, std::nullopt).ok, "whole chunk");
    ExpectRow(store, 1, 1, Row{std::int64_t{3}, std::monostate{}, std::monostate{}});
    // A transaction's states are checked at commit.
    {
        auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(30));
        auto committed = store.ReadChunkStateAt(*snapshot, chunkdb::ChunkCoord{0, 0});
        committed.payload[0] = 0xFFU;
        committed.payload[1] = 0xFFU;
        failed([&] { (void)store.CommitTransaction(*snapshot, {}, {{{0, 0}, committed}}); }, "hp_check");
    }
    ExpectRow(store, 0, 0, Row{std::int64_t{15}, std::int64_t{20}, std::monostate{}});
    // Deletes pass.
    store.UnsetBlock(1, 1);
    ExpectRow(store, 1, 1, std::nullopt);
}

// Positions and a table of bit strings.
void TestPositionsAndBits() {
    ScopedTempDir dir("chunkdb-check-bits");
    auto schema = TableSchema{.version = 1, .next_column_id = 2,
        .columns = {MakeColumn(1, "bits", ColumnKind::kBits, 4)}, .history = {}, .pending = std::nullopt};
    schema.checks = {CheckConstraint{.name = "nonzero", .expression = "bits <> b'0000'"},
                     CheckConstraint{.name = "east", .expression = "pos_x >= 0"}};
    chunkdb::ChunkStore store(StoreConfigFor(dir.path(), schema));
    store.SetBlockBits(1, 1, "1000");
    ExpectThrow<CheckFailedError>([&] { store.SetBlockBits(2, 1, "0000"); }, "block (2, 1) fails CHECK nonzero");
    ExpectThrow<CheckFailedError>([&] { store.SetBlockBits(-1, 1, "1000"); }, "block (-1, 1) fails CHECK east");
    ExpectThrow<CheckFailedError>(
        [&] {
            (void)store.ApplyChunkBatch(0, 0, false, 0, {chunkdb::ChunkBatchOp{.set = true, .x = 2, .y = 2, .bits = "0000"}});
        },
        "fails CHECK nonzero");
    ExpectThrow<CheckFailedError>([&] { store.SetChunkBits(0, 0, std::string(64, '0')); }, "fails CHECK nonzero");
    Expect(store.GetBlockBits(1, 1) == "1000" && !store.ReadBlockBits(2, 1).has_value(), "bits unchanged");
}

struct Engine {
    explicit Engine(const std::filesystem::path& dir) {
        config.data_dir = dir;
        catalog = std::make_shared<chunkdb::TableCatalog>(config);
        chunkdb::EngineConfig engine_config;
        engine_config.require_auth = false;
        engine = std::make_unique<chunkdb::CommandEngine>(engine_config, catalog);
        Expect(engine->Execute(session, "HELLO 3").front() == '%', "hello");
    }
    std::string Run(const std::string& line, const std::vector<std::optional<std::string>>& parameters = {}) {
        return engine->Execute(session, line + "\r\n", parameters);
    }
    // The form of a GET CHUNK reply.
    std::string Form(const std::string& line) {
        const auto reply = Run(line);
        const auto body = reply.find("\r\n") + 2U;
        return reply.substr(body, reply.size() - body - 2U);
    }

    chunkdb::CatalogConfig config;
    std::shared_ptr<chunkdb::TableCatalog> catalog;
    std::unique_ptr<chunkdb::CommandEngine> engine;
    chunkdb::SessionState session;
};

void ExpectFailed(const std::string& reply, const std::string& part) {
    if (!reply.starts_with("-ERR CHECK_FAILED ") || !Contains(reply, part)) {
        std::fprintf(stderr, "expected CHECK_FAILED with '%s', got '%s'\n", part.c_str(), reply.c_str());
        std::abort();
    }
}

// CREATE TABLE with checks, the write statements, DESCRIBE and a restart.
void TestStatements() {
    ScopedTempDir dir("chunkdb-check-statements");
    {
        Engine e(dir.path());
        const auto refused = e.Run("CREATE TABLE bad (a u8 CHECK (b > 0)) CHUNK 4 x 4");
        Expect(refused.starts_with("-ERR INVALID_ARGUMENT ") && Contains(refused, "CHECK a_check:"), "bad check");
        Expect(Contains(e.Run("DESCRIBE bad"), "NO_TABLE"), "no table");
        Expect(e.Run("CREATE TABLE w (hp i16 CHECK (hp >= 0), max_hp i16 NULL, tag text(8) NULL) CHUNK 4 x 4 "
                     "CHECK (hp <= max_hp) CHECK named (tag <> 'bad')") == "+OK\r\n", "create");
        const auto describe = e.Run("DESCRIBE w");
        Expect(Contains(describe, "$6\r\nchecks\r\n*3\r\n") && Contains(describe, "$8\r\nhp_check\r\n") &&
               Contains(describe, "$7\r\nhp >= 0\r\n") && Contains(describe, "$5\r\nnamed\r\n"), "describe");
        Expect(e.Run("SET BLOCK 0 0 IN w hp=1").front() == ':', "set");
        ExpectFailed(e.Run("SET BLOCK 0 0 IN w hp=-1"), "check=hp_check");
        ExpectFailed(e.Run("SET BLOCK 1 0 IN w hp=5, max_hp=4"), "fails CHECK hp_check_2 (hp <= max_hp)");
        ExpectFailed(e.Run("SET BLOCK 0 0 IN w tag='bad' IF VERSION " + std::to_string(
            e.catalog->Find("w")->Acquire()->store().GetChunkVersion(0, 0))), "named");
        Expect(e.Run("GET BLOCK 0 0 FROM w") == "*3\r\n:1\r\n_\r\n_\r\n", "unchanged");
        // A transaction: a failing statement is refused and the transaction
        // goes on.
        Expect(e.Run("BEGIN") == "+OK\r\n", "begin");
        ExpectFailed(e.Run("SET BLOCK 0 0 IN w hp=-2"), "hp_check");
        Expect(e.Run("SET BLOCK 1 1 IN w hp=7") == "_\r\n", "txn set");
        Expect(e.Run("COMMIT").front() == ':', "commit");
        Expect(e.Run("GET BLOCK 1 1 FROM w COLUMNS hp") == "*1\r\n:7\r\n", "committed");

        // SET CHUNK, whole and projected, with a form of a table without
        // the checks.
        Expect(e.Run("CREATE TABLE raw (hp i16, max_hp i16 NULL, tag text(8) NULL) CHUNK 4 x 4") == "+OK\r\n", "raw");
        Expect(e.Run("SET BLOCK 2 2 IN raw hp=-9").front() == ':', "raw set");
        const auto whole = e.Form("GET CHUNK 0 0 FROM raw");
        ExpectFailed(e.Run("SET CHUNK 0 0 IN w $1", {whole}), "block (2, 2) fails CHECK hp_check");
        Expect(e.Run("SET BLOCK 1 1 IN raw hp=-8").front() == ':', "raw set");
        const auto projected = e.Form("GET CHUNK 0 0 FROM raw COLUMNS hp");
        ExpectFailed(e.Run("SET CHUNK 0 0 IN w COLUMNS hp $1", {projected}), "block (1, 1) fails CHECK hp_check");
        Expect(e.Run("BEGIN") == "+OK\r\n", "begin");
        ExpectFailed(e.Run("SET CHUNK 0 0 IN w $1", {whole}), "hp_check");
        ExpectFailed(e.Run("SET CHUNK 0 0 IN w COLUMNS hp $1", {projected}), "hp_check");
        Expect(e.Run("COMMIT") == "_\r\n", "empty commit");
        Expect(e.Run("GET BLOCK 1 1 FROM w COLUMNS hp") == "*1\r\n:7\r\n", "chunk unchanged");

        // Schema changes around the checks.
        Expect(Contains(e.Run("ALTER TABLE w DROP COLUMN max_hp"), "is read by CHECK hp_check_2"), "drop column");
        Expect(Contains(e.Run("ALTER TABLE w ALTER COLUMN hp TYPE i8 USING CLAMP"), "converted values could fail"), "clamp");
        Expect(e.Run("ALTER TABLE w RENAME COLUMN hp TO health") == "+OK\r\n", "rename");
        Expect(Contains(e.Run("DESCRIBE w"), "$11\r\nhealth >= 0\r\n"), "renamed check");
        ExpectFailed(e.Run("SET BLOCK 0 0 IN w health=-1"), "health >= 0");
        Expect(e.Run("ALTER TABLE w DROP CHECK named") == "+OK\r\n", "drop check");
        Expect(Contains(e.Run("ALTER TABLE w DROP CHECK named"), "the table has no CHECK named"), "drop again");
        Expect(e.Run("ALTER TABLE w DROP CHECK IF EXISTS named") == "+OK\r\n", "drop if exists");
        Expect(e.Run("SET BLOCK 0 0 IN w tag='bad'").front() == ':', "dropped check");
    }
    // After a restart the checks hold as they were.
    Engine e(dir.path());
    const auto schema = e.catalog->Find("w")->Info().schema;
    Expect(schema.checks.size() == 2U && schema.checks[0].expression == "health >= 0", "persisted");
    ExpectFailed(e.Run("SET BLOCK 0 0 IN w health=-1"), "hp_check");
    Expect(e.Run("SET BLOCK 0 0 IN w health=2").front() == ':', "after restart");
}

// ADD CHECK scans the stored blocks: the first failing one refuses it,
// directly and as a named migration.
void TestAddCheck() {
    for (const bool migrate : {false, true}) {
        ScopedTempDir dir("chunkdb-add-check");
        Engine e(dir.path());
        std::size_t step = 0U;
        const auto run = [&](const std::string& statement) {
            return e.Run((migrate ? "MIGRATE 'step_" + std::to_string(step++) + "' " : std::string{}) + statement);
        };
        const std::string ok = migrate ? "+applied\r\n" : "+OK\r\n";
        Expect(e.Run("CREATE TABLE w (hp i16, max_hp i16 NULL) CHUNK 4 x 4") == "+OK\r\n", "create");
        Expect(e.Run("SET BLOCK 0 0 IN w hp=1, max_hp=2").front() == ':', "set");
        Expect(e.Run("SET BLOCK 9 9 IN w hp=-1").front() == ':', "set");
        Expect(e.Run("SET BLOCK 5 1 IN w hp=3").front() == ':', "set");
        const auto before = e.catalog->Find("w")->Info().schema;
        ExpectFailed(run("ALTER TABLE w ADD CHECK (hp >= 0)"),
            "cannot add CHECK hp_check: block (9, 9) fails it; change or delete that block first");
        Expect(e.catalog->Find("w")->Info().schema == before, "schema as it was");
        Expect(chunkdb::ReadMigrationRecords(dir.path()).empty(), "no record");
        // NULL passes.
        Expect(run("ALTER TABLE w ADD CHECK (hp <= max_hp)") == ok, "null passes");
        ExpectFailed(run("ALTER TABLE w ADD CHECK (hp / (max_hp - 2) > 0)"), "cannot be evaluated for block (0, 0)");
        Expect(e.Run("SET BLOCK 9 9 IN w hp=0").front() == ':', "fix");
        Expect(run("ALTER TABLE w ADD CHECK positive (hp >= 0)") == ok, "added");
        Expect(Contains(run("ALTER TABLE w ADD CHECK positive (hp > 0)"), "already has a CHECK named positive"), "dup");
        const auto schema = e.catalog->Find("w")->Info().schema;
        Expect(schema.version == 3U && schema.checks.size() == 2U && schema.checks[1].name == "positive", "version");
        ExpectFailed(e.Run("SET BLOCK 9 9 IN w hp=-1"), "check=positive");
        Expect(run("ALTER TABLE w DROP CHECK positive") == ok, "drop");
        Expect(run("ALTER TABLE w DROP CHECK IF EXISTS positive") == ok, "drop if exists");
        Expect(e.Run("SET BLOCK 9 9 IN w hp=-1").front() == ':', "dropped");
        Expect(chunkdb::ReadMigrationRecords(dir.path()).size() == (migrate ? 4U : 0U), "records");
    }
}

// Runs `run` once, when ADD CHECK is about to read the stored blocks.
struct AtScan final : chunkdb::MigrationTestHook {
    std::function<void()> run;
    void Run(Point point, std::string_view) override {
        if (point == Point::kBeforeNarrowingScan && run) std::exchange(run, nullptr)();
    }
};

// While the scan lasts, writes must pass the check being added.
void TestWritesDuringScan() {
    ScopedTempDir dir("chunkdb-add-check-scan");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    chunkdb::TableCatalog catalog(config);
    (void)catalog.Create("w", GeometryFor(Plain()), chunkdb::TableOptions{}, Plain());
    (*catalog.Find("w")->Acquire()).store().SetBlock(0, 0, {{"hp", std::int64_t{1}}});
    AtScan hook;
    hook.run = [&] {
        auto lease = *catalog.Find("w")->Acquire();
        ExpectThrow<CheckFailedError>([&] { lease.store().SetBlock(1, 0, {{"hp", std::int64_t{-1}}}); },
            "fails CHECK hp_check (hp >= 0), which ADD CHECK is checking");
        lease.store().SetBlock(1, 0, {{"hp", std::int64_t{4}}});
    };
    catalog.SetMigrationTestHook(&hook);
    catalog.AddCheck("w", [](const TableSchema& schema) {
        return CheckConstraint{.name = chunkdb::NewCheckName(schema, "hp >= 0"), .expression = "hp >= 0"};
    });
    catalog.SetMigrationTestHook(nullptr);
    Expect(!hook.run, "hook ran");
    const auto schema = catalog.Find("w")->Info().schema;
    Expect(schema.version == 2U && !schema.pending && schema.checks.size() == 1U, "added");
    ExpectRow((*catalog.Find("w")->Acquire()).store(), 1, 0, Row{std::int64_t{4}, std::monostate{}, std::monostate{}});
}

// What a crash during the scan leaves: the next start drops the check in
// progress and the table keeps the checks it had.
void TestInterruptedAddCheck() {
    ScopedTempDir dir("chunkdb-add-check-interrupted");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    {
        chunkdb::TableCatalog catalog(config);
        (void)catalog.Create("w", GeometryFor(Checked()), chunkdb::TableOptions{}, Checked());
    }
    const auto table_dir = dir.path() / "tables" / "w";
    auto manifest = *chunkdb::ReadStoreManifest(table_dir);
    manifest.schema = chunkdb::WithPendingCheck(manifest.schema, CheckConstraint{.name = "low", .expression = "hp < 5"});
    {
        const auto bytes = chunkdb::SerializeStoreManifest(manifest);
        std::ofstream out(chunkdb::StoreManifestPath(table_dir), std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        Expect(static_cast<bool>(out), "manifest written");
    }
    chunkdb::TableCatalog catalog(config);
    Expect(catalog.Find("w")->Info().schema == Checked(), "dropped");
    Expect(!chunkdb::ReadStoreManifest(table_dir)->schema.pending.has_value(), "manifest without it");
    auto lease = *catalog.Find("w")->Acquire();
    lease.store().SetBlock(1, 0, {{"hp", std::int64_t{9}}});
    ExpectThrow<CheckFailedError>([&] { lease.store().SetBlock(1, 0, {{"hp", std::int64_t{-9}}}); }, "hp_check");
}

}  // namespace

int main() {
    TestRules();
    TestStoreWrites();
    TestPositionsAndBits();
    TestStatements();
    TestAddCheck();
    TestWritesDuringScan();
    TestInterruptedAddCheck();
    return 0;
}
