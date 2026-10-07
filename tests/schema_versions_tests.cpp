// Schema versions (#61): adding, dropping and renaming columns as new
// versions, the history that rebuilds earlier versions, translation of chunk
// state between versions, and tables whose images and WALs were written by
// earlier versions: read at once after a change, after more writes, after a
// restart, by verify, and refused when damaged.

#include <cassert>
#include <cstdlib>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_layout.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/table_catalog.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "verify.hpp"
#include "wal_replay.hpp"
#include "wal_writer.hpp"

namespace {

using Bytes = std::vector<std::uint8_t>;
using chunkdb::BitsValue;
using chunkdb::BytesValue;
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

Bytes ReadFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

Column MakeColumn(std::string name, ColumnKind kind, std::uint32_t size) {
    Column column;
    column.name = std::move(name);
    column.type = ColumnType{.kind = kind, .size = size};
    return column;
}

// Version 1: id u10 REQUIRED, light u4 DEFAULT 15, sign text(8) NULL.
TableSchema Initial() {
    auto id = MakeColumn("id", ColumnKind::kUnsigned, 10);
    id.id = 1;
    id.required = true;
    auto light = MakeColumn("light", ColumnKind::kUnsigned, 4);
    light.id = 2;
    light.has_default = true;
    light.default_value = {15};
    auto sign = MakeColumn("sign", ColumnKind::kText, 8);
    sign.id = 3;
    sign.nullable = true;
    return TableSchema{.version = 1, .next_column_id = 4, .columns = {id, light, sign}, .history = {}};
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

// temp i8 NULL DEFAULT -3 and note text(5) DEFAULT 'hi', both added.
TableSchema AddTwo(const TableSchema& schema) {
    auto temp = MakeColumn("temp", ColumnKind::kSigned, 8);
    temp.nullable = true;
    temp.has_default = true;
    temp.default_value = {0xFD};
    auto note = MakeColumn("note", ColumnKind::kText, 5);
    note.has_default = true;
    note.default_value = {'h', 'i'};
    return chunkdb::AddColumn(chunkdb::AddColumn(schema, temp), note);
}

void TestChangesAndHistory() {
    const auto v1 = Initial();
    const auto v3 = AddTwo(v1);
    assert(v3.version == 3U && v3.history.size() == 2U && v3.next_column_id == 6U);
    assert(v3.columns.size() == 5U && v3.columns[3].id == 4U && v3.columns[4].id == 5U);
    const auto v4 = chunkdb::DropColumn(v3, "light");
    const auto v5 = chunkdb::RenameColumn(v4, "sign", "label");
    assert(v5.version == 5U && v5.columns[1].name == "label" && v5.columns[1].id == 3U);

    // Every version comes back from the history, with the history up to it.
    assert(chunkdb::SchemaAtVersion(v5, 1) == v1);
    assert(chunkdb::SchemaAtVersion(v5, 3) == v3);
    assert(chunkdb::SchemaAtVersion(v5, 4) == v4);
    assert(chunkdb::SchemaAtVersion(v5, 5) == v5);
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::SchemaAtVersion(v5, 6); }, "no schema version 6");
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::SchemaAtVersion(v5, 0); }, "no schema version 0");

    // The history survives the manifest's schema area.
    const auto bytes = chunkdb::EncodeTableSchema(v5);
    assert(chunkdb::DecodeTableSchema(bytes.data(), bytes.size()) == v5);
    for (std::size_t size = 0; size < bytes.size(); ++size) {
        bool threw = false;
        try {
            (void)chunkdb::DecodeTableSchema(bytes.data(), size);
        } catch (const std::exception&) {
            threw = true;
        }
        assert(threw);
    }

    // Changes the rules refuse.
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::DropColumn(v1, "nope"); }, "no column nope");
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::RenameColumn(v1, "id", "light"); }, "appears twice");
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::RenameColumn(v1, "id", "Bad"); }, "must be 1 to 63");
    auto required = MakeColumn("must", ColumnKind::kBool, 1);
    required.required = true;
    ExpectThrow<std::invalid_argument>([&] { (void)chunkdb::AddColumn(v1, required); }, "needs a DEFAULT");
    const auto without_id = chunkdb::DropColumn(v1, "id");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::DropColumn(without_id, "light"); }, "the last fixed-width column");
    // A damaged history is refused like any damaged schema.
    auto damaged = v5;
    damaged.history[1].changes[0].position = 9;
    ExpectThrow<std::invalid_argument>([&] { chunkdb::ValidateTableSchema(damaged); }, "does not match its columns");
    auto short_history = v5;
    short_history.history.pop_back();
    ExpectThrow<std::invalid_argument>([&] { chunkdb::ValidateTableSchema(short_history); }, "3 steps for version 5");
}

// Translation keeps the values of kept columns, gives added columns what a
// new block would get, and drops what dropped columns held.
void TestTranslateChunk() {
    const auto v1 = Initial();
    const auto v5 = chunkdb::RenameColumn(chunkdb::DropColumn(AddTwo(v1), "light"), "sign", "label");
    const chunkdb::Geometry geometry(GeometryFor(v5), v5);
    const auto& from = geometry.LayoutAt(1);
    const auto& to = geometry.layout();
    // Blocks 0 and 2 present at version 1.
    const Bytes presence = {0x05, 0x00};
    Bytes payload(from.payload_bytes(), 0U);
    const auto& id = *from.FixedColumnAt(0);
    const auto& light = *from.FixedColumnAt(1);
    const Bytes id0 = chunkdb::EncodeColumnValue(v1.columns[0], std::uint64_t{700});
    const Bytes id2 = chunkdb::EncodeColumnValue(v1.columns[0], std::uint64_t{5});
    chunkdb::WriteValueBits(payload.data(), id.values * 8U + 0U * id.width, id0.data(), id.width);
    chunkdb::WriteValueBits(payload.data(), id.values * 8U + 2U * id.width, id2.data(), id.width);
    const Bytes nine = {9};
    chunkdb::WriteValueBits(payload.data(), light.values * 8U + 0U * light.width, nine.data(), light.width);
    chunkdb::ChunkVars vars;
    vars.Assign({.column_id = 3, .block_index = 2}, Bytes{'o', 'k'});

    chunkdb::TranslateChunk(from, to, presence, &payload, &vars);
    assert(payload.size() == to.payload_bytes());
    const auto value = [&](std::size_t index, std::size_t block) -> ColumnValue {
        const auto& column = to.schema().columns[index];
        const auto* fixed = to.FixedColumnAt(index);
        if (fixed == nullptr) {
            const auto found = vars.Find({.column_id = column.id, .block_index = static_cast<std::uint32_t>(block)});
            if (!found.has_value()) {
                return column.nullable ? ColumnValue{std::monostate{}} : chunkdb::DecodeVarValue(column, {});
            }
            return chunkdb::DecodeVarValue(column, *found);
        }
        if (fixed->validity != chunkdb::ChunkLayout::kNoValidity &&
            ((payload[fixed->validity + block / 8U] >> (block % 8U)) & 1U) == 0U) {
            return std::monostate{};
        }
        Bytes bytes((fixed->width + 7U) / 8U);
        chunkdb::ReadValueBits(payload.data(), fixed->values * 8U + block * fixed->width, bytes.data(), fixed->width);
        return chunkdb::DecodeColumnValue(column, bytes.data());
    };
    // Version 5 columns: id, label (was sign), temp, note.
    assert(value(0, 0) == ColumnValue{std::uint64_t{700}} && value(0, 2) == ColumnValue{std::uint64_t{5}});
    assert(value(1, 0) == ColumnValue{std::monostate{}} && value(1, 2) == ColumnValue{std::string("ok")});
    assert(value(2, 0) == ColumnValue{std::int64_t{-3}} && value(2, 2) == ColumnValue{std::int64_t{-3}});
    assert(value(3, 0) == ColumnValue{std::string("hi")});
    // Absent blocks stay empty: no values and no validity.
    assert(value(2, 1) == ColumnValue{std::monostate{}} && !vars.Find({.column_id = 5, .block_index = 1}).has_value());
    to.RequireValidVars(vars, presence);

    // Dropping a text column drops its values.
    const auto without_sign = chunkdb::DropColumn(v1, "sign");
    const chunkdb::Geometry dropped(GeometryFor(without_sign), without_sign);
    Bytes v1_payload(dropped.LayoutAt(1).payload_bytes(), 0U);
    chunkdb::ChunkVars v1_vars;
    v1_vars.Assign({.column_id = 3, .block_index = 2}, Bytes{'x'});
    chunkdb::TranslateChunk(dropped.LayoutAt(1), dropped.layout(), presence, &v1_payload, &v1_vars);
    assert(v1_vars.empty());
}

using Row = std::vector<ColumnValue>;

void ExpectRow(chunkdb::ChunkStore& store, std::int64_t x, std::int64_t y, const std::optional<Row>& want) {
    if (store.GetBlock(x, y) != want) {
        std::fprintf(stderr, "block (%lld,%lld) does not hold the expected values\n", static_cast<long long>(x),
                     static_cast<long long>(y));
        assert(false);
    }
}

// Data written at version 1 into images (checkpoint interval 1) or into the
// WAL only, then columns change: values read right after the change, after
// writes at the new version, and after a restart that replays both.
void TestTableAcrossVersions() {
    for (const std::size_t checkpoint_updates : {std::size_t{1}, std::size_t{100000}}) {
        ScopedTempDir dir("chunkdb-schema-versions");
        chunkdb::CatalogConfig config;
        config.data_dir = dir.path();
        {
            chunkdb::TableCatalog catalog(config);
            chunkdb::TableOptions options;
            options.checkpoint_update_interval = checkpoint_updates;
            (void)catalog.Create("world", GeometryFor(Initial()), options, Initial());
            {
                auto lease = *catalog.Find("world")->Acquire();
                lease.store().SetBlock(0, 0, {{"id", std::uint64_t{1}}, {"light", std::uint64_t{3}}, {"sign", std::string("a")}});
                lease.store().SetBlock(5, 0, {{"id", std::uint64_t{2}}});
            }
            catalog.ChangeColumns("world", [](const TableSchema& schema) { return AddTwo(schema); });
            catalog.ChangeColumns("world", [](const TableSchema& schema) { return chunkdb::DropColumn(schema, "light"); });
            catalog.ChangeColumns(
                "world", [](const TableSchema& schema) { return chunkdb::RenameColumn(schema, "sign", "label"); });
            const auto table = catalog.Find("world");
            assert(table->Info().schema.version == 5U);
            auto lease = *table->Acquire();
            auto& store = lease.store();
            // id, label, temp, note
            ExpectRow(store, 0, 0, Row{std::uint64_t{1}, std::string("a"), std::int64_t{-3}, std::string("hi")});
            ExpectRow(store, 5, 0, Row{std::uint64_t{2}, std::monostate{}, std::int64_t{-3}, std::string("hi")});
            // Writes at the new version, in the same chunk and a new one.
            store.SetBlock(0, 0, {{"temp", std::int64_t{7}}, {"label", std::string("b")}});
            store.SetBlock(1, 0, {{"id", std::uint64_t{3}}, {"note", std::string("yo")}});
            ExpectThrow<std::invalid_argument>([&] { store.SetBlock(1, 0, {{"light", std::uint64_t{1}}}); }, "no column light");
        }
        // A restart replays images and WALs of both versions.
        chunkdb::TableCatalog catalog(config);
        auto lease = *catalog.Find("world")->Acquire();
        auto& store = lease.store();
        ExpectRow(store, 0, 0, Row{std::uint64_t{1}, std::string("b"), std::int64_t{7}, std::string("hi")});
        ExpectRow(store, 1, 0, Row{std::uint64_t{3}, std::monostate{}, std::int64_t{-3}, std::string("yo")});
        ExpectRow(store, 5, 0, Row{std::uint64_t{2}, std::monostate{}, std::int64_t{-3}, std::string("hi")});
        assert(store.GetBlock(2, 0) == std::nullopt);
    }
}

// Images and frames of later versions name their version; verify reads them,
// and a version the table never had is damage.
void TestVersionsInFiles() {
    ScopedTempDir dir("chunkdb-schema-versions-files");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    {
        chunkdb::TableCatalog catalog(config);
        chunkdb::TableOptions options;
        options.checkpoint_update_interval = 1;
        (void)catalog.Create("world", GeometryFor(Initial()), options, Initial());
        catalog.ChangeColumns("world", [](const TableSchema& schema) { return AddTwo(schema); });
        auto lease = *catalog.Find("world")->Acquire();
        lease.store().SetBlock(0, 0, {{"id", std::uint64_t{1}}});
    }
    const auto run_verify = [&] {
        std::ostringstream out;
        const auto counters = chunkdb::VerifyDataDirectory(dir.path(), out);
        return std::make_pair(counters, out.str());
    };
    {
        const auto [counters, out] = run_verify();
        if (counters.errors != 0U || counters.warnings != 0U) {
            std::fprintf(stderr, "%s", out.c_str());
            assert(false);
        }
    }
    const auto table_dir = dir.path() / "tables" / "world";
    const auto manifest = chunkdb::ReadStoreManifest(table_dir);
    const chunkdb::Geometry geometry(manifest->geometry, manifest->schema);
    const auto image_path = chunkdb::ChunkDataPath(table_dir, geometry, {0, 0});
    auto image_bytes = ReadFile(image_path);
    const auto image =
        chunkdb::ParseChunkImage(image_bytes, geometry, {0, 0}, manifest->store_id, manifest->features);
    assert(image.schema_version == 3U);
    // An image of a version above the table's is refused.
    const auto earlier = chunkdb::Geometry(GeometryFor(Initial()), Initial());
    ExpectThrow<std::runtime_error>(
        [&] { (void)chunkdb::ParseChunkImage(image_bytes, earlier, {0, 0}, manifest->store_id, manifest->features); },
        "schema version 3, the table is at 1");
}

void SetFailpoint(const char* key, const char* value) {
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

// Replay refuses a frame of a version the table never had, and a frame older
// than the state it follows.
void TestFrameVersions() {
    const auto v3 = AddTwo(Initial());
    const chunkdb::Geometry geometry(GeometryFor(v3), v3);
    const chunkdb::StoreId store_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    const chunkdb::ChunkCoord coord{0, 0};
    const auto frame = [&](std::uint64_t revision, std::uint64_t version) {
        Bytes batch;
        chunkdb::WalFrameBuilder builder(&batch, version);
        const Bytes presence = {0x01, 0x00};
        builder.AppendSpan(
            static_cast<std::uint32_t>(geometry.LayoutAt(version).payload_bytes()), presence.data(), presence.size());
        (void)builder.Finish(revision, 1000U + revision);
        return batch;
    };
    const auto replay = [&](const std::vector<Bytes>& frames) {
        auto wal = chunkdb::BuildWalHeader(coord, store_id, {});
        for (const auto& f : frames) {
            wal.insert(wal.end(), f.begin(), f.end());
        }
        Bytes payload;
        Bytes presence(2, 0U);
        chunkdb::ChunkVars vars;
        return chunkdb::ReplayWal(wal, geometry, coord, store_id, {}, 0, 0, &payload, &presence, &vars);
    };
    {
        const auto r = replay({frame(1, 1), frame(2, 3)});
        assert(r.applied_frames == 2U && !r.tail_truncated_or_corrupt);
    }
    {
        const auto r = replay({frame(1, 3), frame(2, 2)});
        assert(r.applied_frames == 1U && r.stop_reason == "frame_schema_version_order" && !r.stopped_at_crash_tail);
    }
    {
        // Written by hand: the writer cannot name a version the table lacks.
        Bytes batch;
        chunkdb::WalFrameBuilder builder(&batch, 9);
        const Bytes presence = {0x01, 0x00};
        builder.AppendSpan(static_cast<std::uint32_t>(geometry.layout().payload_bytes()), presence.data(), presence.size());
        (void)builder.Finish(1, 1001);
        const auto r = replay({batch});
        assert(r.applied_frames == 0U && r.stop_reason == "frame_schema_version");
    }
}

// A change the rules refuse leaves the table as it was; a change whose
// directory sync fails is reported but served, as TABLESET does.
void TestCatalogChangeFailures() {
    ScopedTempDir dir("chunkdb-schema-versions-failures");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    chunkdb::TableCatalog catalog(config);
    (void)catalog.Create("world", GeometryFor(Initial()), chunkdb::TableOptions{}, Initial());
    {
        auto lease = *catalog.Find("world")->Acquire();
        lease.store().SetBlock(0, 0, {{"id", std::uint64_t{9}}});
    }
    ExpectThrow<std::invalid_argument>(
        [&] { catalog.ChangeColumns("world", [](const TableSchema& schema) { return chunkdb::DropColumn(schema, "nope"); }); },
        "no column nope");
    assert(catalog.Find("world")->Info().schema == Initial());
    {
        auto lease = *catalog.Find("world")->Acquire();
        ExpectRow(lease.store(), 0, 0, Row{std::uint64_t{9}, std::uint64_t{15}, std::monostate{}});
    }
    SetFailpoint("CHUNKDB_FAILPOINT_ALTER_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
    ExpectThrow<std::runtime_error>(
        [&] {
            catalog.ChangeColumns(
                "world", [](const TableSchema& schema) { return chunkdb::RenameColumn(schema, "light", "lamp"); });
        },
        "");
    SetFailpoint("CHUNKDB_FAILPOINT_ALTER_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "");
    const auto table = catalog.Find("world");
    assert(table->Info().schema.version == 2U && table->Info().schema.columns[1].name == "lamp");
    auto lease = *table->Acquire();
    ExpectRow(lease.store(), 0, 0, Row{std::uint64_t{9}, std::uint64_t{15}, std::monostate{}});
}

}  // namespace

int main() {
    TestChangesAndHistory();
    TestTranslateChunk();
    TestTableAcrossVersions();
    TestVersionsInFiles();
    TestCatalogChangeFailures();
    TestFrameVersions();
    return 0;
}
