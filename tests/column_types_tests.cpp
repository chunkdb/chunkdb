// Changing column types (#61): which changes are allowed, how values convert
// (widening, CLAMP, DEFAULT, TRUNCATE), the column's DEFAULT following them,
// and tables whose files were written before one or more type changes.

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/table_catalog.hpp"
#include "test_utils.hpp"

namespace {

using chunkdb::BitsValue;
using chunkdb::BytesValue;
using chunkdb::Column;
using chunkdb::ColumnKind;
using chunkdb::ColumnType;
using chunkdb::ColumnValue;
using chunkdb::Conversion;
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

Column MakeColumn(std::uint32_t id, std::string name, ColumnKind kind, std::uint32_t size) {
    Column column;
    column.id = id;
    column.name = std::move(name);
    column.type = ColumnType{.kind = kind, .size = size};
    return column;
}

// id u10 REQUIRED, light u4 DEFAULT 15, temp i8 NULL, height f64,
// sign text(8) NULL, mask bits(3).
TableSchema Initial() {
    auto id = MakeColumn(1, "id", ColumnKind::kUnsigned, 10);
    id.required = true;
    auto light = MakeColumn(2, "light", ColumnKind::kUnsigned, 4);
    light.has_default = true;
    light.default_value = {15};
    auto temp = MakeColumn(3, "temp", ColumnKind::kSigned, 8);
    temp.nullable = true;
    auto sign = MakeColumn(5, "sign", ColumnKind::kText, 8);
    sign.nullable = true;
    return TableSchema{
        .version = 1,
        .next_column_id = 7,
        .columns = {id, light, temp, MakeColumn(4, "height", ColumnKind::kFloat64, 64), sign,
                    MakeColumn(6, "mask", ColumnKind::kBits, 3)},
        .history = {},
    };
}

ColumnType Type(ColumnKind kind, std::uint32_t size) {
    return ColumnType{.kind = kind, .size = size};
}

void TestAllowedChanges() {
    const auto v1 = Initial();
    // Widening needs no conversion.
    for (const auto& [name, type] : std::vector<std::pair<std::string, ColumnType>>{
             {"light", Type(ColumnKind::kUnsigned, 8)},
             {"light", Type(ColumnKind::kSigned, 5)},
             {"temp", Type(ColumnKind::kSigned, 16)},
             {"sign", Type(ColumnKind::kText, 16)},
             {"mask", Type(ColumnKind::kBits, 5)},
         }) {
        const auto changed = chunkdb::ChangeColumnType(v1, name, type, Conversion::kExact);
        assert(changed.version == 2U && changed.history.back().changes[0].kind == chunkdb::SchemaChange::Kind::kChangeType);
    }
    // f64 to f32 narrows; f32 to f64 widens.
    const auto f32 = chunkdb::ChangeColumnType(v1, "height", Type(ColumnKind::kFloat32, 32), Conversion::kClamp);
    (void)chunkdb::ChangeColumnType(f32, "height", Type(ColumnKind::kFloat64, 64), Conversion::kExact);

    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnType(v1, "light", Type(ColumnKind::kSigned, 4), Conversion::kExact); },
        "does not hold every u4 value");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnType(v1, "temp", Type(ColumnKind::kUnsigned, 16), Conversion::kExact); },
        "does not hold every i8 value");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnType(v1, "id", Type(ColumnKind::kUnsigned, 8), Conversion::kExact); },
        "CLAMP, DEFAULT or TRUNCATE");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnType(v1, "id", Type(ColumnKind::kText, 8), Conversion::kDefault); },
        "add a column of the new type");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnType(v1, "sign", Type(ColumnKind::kText, 4), Conversion::kClamp); },
        "cannot be clamped");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnType(v1, "id", Type(ColumnKind::kUnsigned, 8), Conversion::kTruncate); },
        "cannot be truncated");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnType(v1, "light", Type(ColumnKind::kUnsigned, 4), Conversion::kExact); },
        "already u4");

    // The history keeps the column before and the conversion, through the
    // manifest's schema area, and undoes them.
    const auto v3 = chunkdb::ChangeColumnType(
        chunkdb::ChangeColumnType(v1, "id", Type(ColumnKind::kUnsigned, 8), Conversion::kClamp), "sign",
        Type(ColumnKind::kText, 3), Conversion::kTruncate);
    const auto bytes = chunkdb::EncodeTableSchema(v3);
    assert(chunkdb::DecodeTableSchema(bytes.data(), bytes.size()) == v3);
    assert(chunkdb::SchemaAtVersion(v3, 1) == v1);
}

void TestConversions() {
    const auto v1 = Initial();
    const auto column = [&](const char* name) {
        for (const auto& c : v1.columns) {
            if (c.name == name) {
                return c;
            }
        }
        assert(false);
        return Column{};
    };
    const auto convert = [&](const char* name, ColumnType type, Conversion conversion, const ColumnValue& value) {
        const Column before = column(name);
        Column after = before;
        after.type = type;
        if (after.has_default) {
            after.has_default = false;
            after.default_value.clear();
        }
        return chunkdb::ConvertValue(before, after, conversion, value);
    };
    // Values that fit convert exactly under every conversion.
    assert(convert("id", Type(ColumnKind::kUnsigned, 8), Conversion::kClamp, std::uint64_t{200}) == ColumnValue{std::uint64_t{200}});
    assert(convert("light", Type(ColumnKind::kSigned, 5), Conversion::kExact, std::uint64_t{15}) == ColumnValue{std::int64_t{15}});
    // CLAMP: the nearest value in range.
    assert(convert("id", Type(ColumnKind::kUnsigned, 8), Conversion::kClamp, std::uint64_t{700}) == ColumnValue{std::uint64_t{255}});
    assert(convert("temp", Type(ColumnKind::kSigned, 4), Conversion::kClamp, std::int64_t{-100}) == ColumnValue{std::int64_t{-8}});
    assert(convert("height", Type(ColumnKind::kFloat32, 32), Conversion::kClamp, 1e300) ==
           ColumnValue{std::numeric_limits<float>::max()});
    assert(std::isnan(std::get<float>(
        convert("height", Type(ColumnKind::kFloat32, 32), Conversion::kClamp, std::numeric_limits<double>::quiet_NaN()))));
    // DEFAULT: the column's DEFAULT, else NULL, else zero.
    assert(convert("temp", Type(ColumnKind::kSigned, 4), Conversion::kDefault, std::int64_t{100}) == ColumnValue{std::monostate{}});
    assert(convert("id", Type(ColumnKind::kUnsigned, 8), Conversion::kDefault, std::uint64_t{700}) == ColumnValue{std::uint64_t{0}});
    // TRUNCATE: the first bytes or bits, text at a character boundary.
    assert(convert("sign", Type(ColumnKind::kText, 2), Conversion::kTruncate, std::string("h\xC3\xA9llo")) ==
           ColumnValue{std::string("h")});
    assert(convert("sign", Type(ColumnKind::kText, 3), Conversion::kTruncate, std::string("h\xC3\xA9llo")) ==
           ColumnValue{std::string("h\xC3\xA9")});
    assert(convert("mask", Type(ColumnKind::kBits, 2), Conversion::kTruncate, BitsValue{"101"}) == ColumnValue{BitsValue{"10"}});
    assert(convert("mask", Type(ColumnKind::kBits, 2), Conversion::kExact, BitsValue{"100"}) == ColumnValue{BitsValue{"10"}});
    assert(convert("mask", Type(ColumnKind::kBits, 5), Conversion::kExact, BitsValue{"101"}) == ColumnValue{BitsValue{"10100"}});
    // NULL stays NULL; kExact with a value that does not fit is a bug.
    assert(convert("temp", Type(ColumnKind::kSigned, 4), Conversion::kClamp, std::monostate{}) == ColumnValue{std::monostate{}});
    ExpectThrow<std::logic_error>(
        [&] { (void)convert("id", Type(ColumnKind::kUnsigned, 8), Conversion::kExact, std::uint64_t{700}); }, "does not fit u8");

    // The DEFAULT follows the values.
    auto with_default = Initial();
    with_default.columns[0].has_default = true;
    with_default.columns[0].required = false;
    with_default.columns[0].default_value = chunkdb::EncodeColumnValue(with_default.columns[0], std::uint64_t{700});
    const auto clamped = chunkdb::ChangeColumnType(with_default, "id", Type(ColumnKind::kUnsigned, 8), Conversion::kClamp);
    assert(chunkdb::DecodeColumnValue(clamped.columns[0], clamped.columns[0].default_value.data()) ==
           ColumnValue{std::uint64_t{255}});
    ExpectThrow<std::invalid_argument>(
        [&] { (void)chunkdb::ChangeColumnType(with_default, "id", Type(ColumnKind::kUnsigned, 8), Conversion::kDefault); },
        "its DEFAULT does not fit u8");
}

using Row = std::vector<ColumnValue>;

void ExpectRow(chunkdb::ChunkStore& store, std::int64_t x, std::int64_t y, const Row& want) {
    if (store.GetBlock(x, y) != std::optional<Row>(want)) {
        std::fprintf(stderr, "block (%lld,%lld) does not hold the expected values\n", static_cast<long long>(x),
                     static_cast<long long>(y));
        assert(false);
    }
}

// Files written before type changes, including two changes of one column,
// read right after the changes and after a restart.
void TestTableAcrossTypeChanges() {
    for (const std::size_t checkpoint_updates : {std::size_t{1}, std::size_t{100000}}) {
        ScopedTempDir dir("chunkdb-column-types");
        chunkdb::CatalogConfig config;
        config.data_dir = dir.path();
        const auto v1 = Initial();
        const chunkdb::GeometryConfig geometry{
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = chunkdb::FixedBitsPerBlock(v1),
        };
        // id, light, temp, height, sign, mask after the changes.
        const Row first{std::uint64_t{255}, std::uint64_t{3}, std::monostate{}, 0.5, std::string("abc"), BitsValue{"10"}};
        const Row second{std::uint64_t{20}, std::uint64_t{15}, std::int64_t{-5}, 0.0, std::monostate{}, BitsValue{"00"}};
        {
            chunkdb::TableCatalog catalog(config);
            chunkdb::TableOptions options;
            options.checkpoint_update_interval = checkpoint_updates;
            (void)catalog.Create("world", geometry, options, v1);
            {
                auto lease = *catalog.Find("world")->Acquire();
                lease.store().SetBlock(
                    0, 0,
                    {{"id", std::uint64_t{700}}, {"light", std::uint64_t{3}}, {"temp", std::int64_t{100}},
                     {"height", 0.5}, {"sign", std::string("abcdefgh")}, {"mask", BitsValue{"101"}}});
                lease.store().SetBlock(1, 0, {{"id", std::uint64_t{20}}, {"temp", std::int64_t{-5}}});
            }
            const auto change = [&](const char* name, ColumnType type, Conversion conversion) {
                catalog.ChangeColumns("world", [&](const TableSchema& schema) {
                    return chunkdb::ChangeColumnType(schema, name, type, conversion);
                });
            };
            change("id", Type(ColumnKind::kUnsigned, 8), Conversion::kClamp);
            change("light", Type(ColumnKind::kUnsigned, 8), Conversion::kExact);
            change("temp", Type(ColumnKind::kSigned, 4), Conversion::kDefault);
            change("sign", Type(ColumnKind::kText, 5), Conversion::kTruncate);
            change("sign", Type(ColumnKind::kText, 3), Conversion::kTruncate);
            change("mask", Type(ColumnKind::kBits, 2), Conversion::kTruncate);
            auto lease = *catalog.Find("world")->Acquire();
            auto& store = lease.store();
            ExpectRow(store, 0, 0, first);
            ExpectRow(store, 1, 0, second);
            // The widened column takes values the old type could not hold.
            store.SetBlock(2, 0, {{"id", std::uint64_t{1}}, {"light", std::uint64_t{200}}});
        }
        chunkdb::TableCatalog catalog(config);
        auto lease = *catalog.Find("world")->Acquire();
        auto& store = lease.store();
        ExpectRow(store, 0, 0, first);
        ExpectRow(store, 1, 0, second);
        ExpectRow(store, 2, 0, Row{std::uint64_t{1}, std::uint64_t{200}, std::monostate{}, 0.0, std::monostate{}, BitsValue{"00"}});
    }
}

}  // namespace

int main() {
    TestAllowedChanges();
    TestConversions();
    TestTableAcrossTypeChanges();
    return 0;
}
