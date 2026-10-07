#include <cassert>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/schema.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"

namespace {

using chunkdb::Column;
using chunkdb::ColumnKind;
using chunkdb::ColumnType;
using chunkdb::TableSchema;

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

Column Fixed(std::uint32_t id, std::string name, ColumnKind kind, std::uint32_t size) {
    return Column{.id = id, .name = std::move(name), .type = ColumnType{.kind = kind, .size = size}};
}

TableSchema World() {
    auto light = Fixed(2, "light", ColumnKind::kUnsigned, 4);
    light.has_default = true;
    light.default_value = {15};
    auto temp = Fixed(3, "temp", ColumnKind::kSigned, 8);
    temp.nullable = true;
    auto sign = Fixed(5, "sign", ColumnKind::kText, 256);
    sign.nullable = true;
    sign.has_default = true;
    sign.default_value = {'h', 'i'};
    auto id = Fixed(1, "id", ColumnKind::kUnsigned, 10);
    id.required = true;
    return TableSchema{
        .version = 4,
        .next_column_id = 7,
        .columns = {id, light, temp, Fixed(4, "solid", ColumnKind::kBool, 1), sign,
                    Fixed(6, "chest", ColumnKind::kBytes, 4096)},
    };
}

void TestRoundTrip() {
    const auto schema = World();
    chunkdb::ValidateTableSchema(schema);
    const auto bytes = chunkdb::EncodeTableSchema(schema);
    assert(chunkdb::DecodeTableSchema(bytes.data(), bytes.size()) == schema);
    assert(chunkdb::FixedBitsPerBlock(schema) == 10U + 4U + 8U + 1U);

    const auto single = chunkdb::SingleBitsColumnSchema(16);
    const auto single_bytes = chunkdb::EncodeTableSchema(single);
    assert(chunkdb::DecodeTableSchema(single_bytes.data(), single_bytes.size()) == single);
    assert(chunkdb::FixedBitsPerBlock(single) == 16U);
    assert(chunkdb::ColumnTypeName(single.columns.front().type) == "bits(16)");
}

void TestTypeNames() {
    assert(chunkdb::ColumnTypeName({ColumnKind::kUnsigned, 10}) == "u10");
    assert(chunkdb::ColumnTypeName({ColumnKind::kSigned, 8}) == "i8");
    assert(chunkdb::ColumnTypeName({ColumnKind::kBool, 1}) == "bool");
    assert(chunkdb::ColumnTypeName({ColumnKind::kFloat32, 32}) == "f32");
    assert(chunkdb::ColumnTypeName({ColumnKind::kFloat64, 64}) == "f64");
    assert(chunkdb::ColumnTypeName({ColumnKind::kText, 256}) == "text(256)");
    assert(chunkdb::ColumnTypeName({ColumnKind::kBytes, 9}) == "bytes(9)");
}

void TestValidationRules() {
    struct Case {
        const char* reason;
        std::function<void(TableSchema*)> damage;
    };
    const std::vector<Case> cases = {
        {"version must be at least 1", [](TableSchema* s) { s->version = 0; }},
        {"at least one column", [](TableSchema* s) { s->columns.clear(); }},
        {"must be 1 to 63 characters", [](TableSchema* s) { s->columns[0].name = "Id"; }},
        {"must be 1 to 63 characters", [](TableSchema* s) { s->columns[0].name = "1id"; }},
        {"must be 1 to 63 characters", [](TableSchema* s) { s->columns[0].name = std::string(64, 'a'); }},
        {"appears twice", [](TableSchema* s) { s->columns[1].name = "id"; }},
        {"invalid id", [](TableSchema* s) { s->columns[1].id = 1; }},
        {"invalid id", [](TableSchema* s) { s->columns[1].id = 7; }},
        {"invalid id", [](TableSchema* s) { s->columns[1].id = 0; }},
        {"uN takes N from 1 to 64", [](TableSchema* s) { s->columns[0].type.size = 65; }},
        {"iN takes N from 2 to 64", [](TableSchema* s) { s->columns[2].type.size = 1; }},
        {"bool is 1 bit", [](TableSchema* s) { s->columns[3].type.size = 2; }},
        {"text(max) takes max", [](TableSchema* s) { s->columns[4].type.size = 0; }},
        {"bytes(max) takes max", [](TableSchema* s) { s->columns[5].type.size = (16U << 20U) + 1U; }},
        {"both NULL and REQUIRED", [](TableSchema* s) { s->columns[0].nullable = true; }},
        {"takes 1 bytes", [](TableSchema* s) { s->columns[1].default_value = {1, 0}; }},
        {"bits past the type's width", [](TableSchema* s) { s->columns[1].default_value = {16}; }},
        {"longer than text(256)", [](TableSchema* s) { s->columns[4].default_value.assign(257, 'a'); }},
        {"not UTF-8", [](TableSchema* s) { s->columns[4].default_value = {0xC0, 0x80}; }},
        {"not UTF-8", [](TableSchema* s) { s->columns[4].default_value = {0xED, 0xA0, 0x80}; }},
        {"a default value without DEFAULT", [](TableSchema* s) { s->columns[2].default_value = {1}; }},
        {"at most 65535",
         [](TableSchema* s) {
             s->columns.push_back(Fixed(7, "wide", ColumnKind::kBits, 65535));
             s->next_column_id = 8;
         }},
    };
    for (const auto& c : cases) {
        auto schema = World();
        c.damage(&schema);
        const auto error = ErrorOf([&] { chunkdb::ValidateTableSchema(schema); });
        if (!Contains(error, c.reason)) {
            std::fprintf(stderr, "expected '%s', got '%s'\n", c.reason, error.c_str());
        }
        assert(Contains(error, c.reason));
        assert(!ErrorOf([&] { (void)chunkdb::EncodeTableSchema(schema); }).empty());
    }
    // Multi-byte UTF-8 is accepted.
    auto schema = World();
    schema.columns[4].default_value = {0xD0, 0x9F, 0xE2, 0x82, 0xAC, 0xF0, 0x9F, 0x98, 0x80};
    chunkdb::ValidateTableSchema(schema);
}

void TestMalformedBytes() {
    const auto good = chunkdb::EncodeTableSchema(World());
    // Every proper prefix is truncated.
    for (std::size_t size = 0; size < good.size(); ++size) {
        assert(!ErrorOf([&] { (void)chunkdb::DecodeTableSchema(good.data(), size); }).empty());
    }
    auto trailing = good;
    trailing.push_back(0);
    assert(Contains(ErrorOf([&] { (void)chunkdb::DecodeTableSchema(trailing.data(), trailing.size()); }),
                    "trailing bytes"));
    // The first column's kind follows version (8), next id (4), count (4)
    // and its id (4); its flags follow the kind and size (4).
    auto kind = good;
    kind[20] = 99;
    assert(Contains(ErrorOf([&] { (void)chunkdb::DecodeTableSchema(kind.data(), kind.size()); }),
                    "unknown column type 99"));
    auto flags = good;
    flags[25] = 0x80;
    assert(Contains(ErrorOf([&] { (void)chunkdb::DecodeTableSchema(flags.data(), flags.size()); }),
                    "unknown column flags"));
}

// This step stores one fixed-width column that cannot be null; a manifest
// that records anything else is refused before the table is touched.
void TestUnsupportedSchemaIsRefused() {
    chunkdb::test::ScopedTempDir dir("chunkdb-schema-unsupported");
    const chunkdb::GeometryConfig geometry{
        .large_chunk_width_chunks = 2,
        .large_chunk_height_chunks = 2,
        .chunk_width_blocks = 4,
        .chunk_height_blocks = 4,
        .block_bits = 8,
    };
    auto schema = TableSchema{
        .version = 1,
        .next_column_id = 3,
        .columns = {Fixed(1, "id", ColumnKind::kUnsigned, 4), Fixed(2, "light", ColumnKind::kUnsigned, 4)},
    };
    assert(!chunkdb::UnsupportedSchemaReason(schema).empty());
    assert(chunkdb::UnsupportedSchemaReason(chunkdb::SingleBitsColumnSchema(8)).empty());
    std::filesystem::create_directories(dir.path());
    const auto manifest = chunkdb::SerializeStoreManifest(chunkdb::StoreManifest{
        .features = {},
        .geometry = geometry,
        .store_id = chunkdb::NewStoreId(),
        .options = {},
        .schema = schema,
    });
    {
        std::ofstream out(chunkdb::StoreManifestPath(dir.path()), std::ios::binary);
        out.write(reinterpret_cast<const char*>(manifest.data()), static_cast<std::streamsize>(manifest.size()));
        assert(out.good());
    }
    chunkdb::StoreConfig config;
    config.data_dir = dir.path();
    config.geometry = geometry;
    config.geometry_fields = 0;
    const auto error = ErrorOf([&] { chunkdb::ChunkStore store(config); });
    assert(Contains(error, "not supported by this build yet"));
    std::vector<std::string> names;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path())) {
        names.push_back(entry.path().filename().string());
    }
    assert(names == std::vector<std::string>{"table.manifest"});
}

}  // namespace

int main() {
    TestRoundTrip();
    TestTypeNames();
    TestValidationRules();
    TestMalformedBytes();
    TestUnsupportedSchemaIsRefused();
    return 0;
}
