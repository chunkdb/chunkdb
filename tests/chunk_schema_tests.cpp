#include <cassert>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/chunk_layout.hpp"
#include "chunkdb/geometry.hpp"
#include "chunkdb/schema.hpp"

namespace {

using chunkdb::ColumnDomain;
using chunkdb::SchemaChange;
using chunkdb::TableSchema;
using Bytes = std::vector<std::uint8_t>;

void Reject(const std::function<void()>& call) {
    bool rejected = false;
    try { call(); } catch (const std::invalid_argument&) { rejected = true; }
    catch (const std::runtime_error&) { rejected = true; }
    assert(rejected);
}

chunkdb::Column Column(std::string name, chunkdb::ColumnKind kind, std::uint32_t size) {
    chunkdb::Column column;
    column.name = std::move(name);
    column.type = {kind, size};
    return column;
}

void RoundTrip(const TableSchema& schema, ColumnDomain domain) {
    const auto bytes = domain == ColumnDomain::kBlock ? chunkdb::EncodeTableSchema(schema) : chunkdb::EncodeChunkSchema(schema);
    const auto parsed = domain == ColumnDomain::kBlock ? chunkdb::DecodeTableSchema(bytes.data(), bytes.size()) :
                                                       chunkdb::DecodeChunkSchema(bytes.data(), bytes.size());
    assert(parsed == schema);
    for (std::size_t cut = 0; cut < bytes.size(); ++cut) {
        Reject([&] {
            if (domain == ColumnDomain::kBlock) (void)chunkdb::DecodeTableSchema(bytes.data(), cut);
            else (void)chunkdb::DecodeChunkSchema(bytes.data(), cut);
        });
    }
    auto trailing = bytes;
    trailing.push_back(2);
    Reject([&] {
        if (domain == ColumnDomain::kBlock) (void)chunkdb::DecodeTableSchema(trailing.data(), trailing.size());
        else (void)chunkdb::DecodeChunkSchema(trailing.data(), trailing.size());
    });
}

void TestEmptyAndVariableChunkSchema() {
    const TableSchema empty;
    chunkdb::ValidateChunkSchema(empty);
    Reject([&] { chunkdb::ValidateTableSchema(empty); });
    const Bytes expected{1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};
    assert(chunkdb::EncodeChunkSchema(empty) == expected);
    RoundTrip(empty, ColumnDomain::kChunk);
    const chunkdb::ChunkLayout empty_row(empty, 1, 1024, ColumnDomain::kChunk);
    assert(empty_row.payload_bytes() == 0 && empty_row.schema().columns.empty());
    Reject([&] { (void)chunkdb::ChunkLayout(empty, 1, 1024); });
    Reject([&] { (void)chunkdb::ChunkLayout(empty, 2, 1024, ColumnDomain::kChunk); });
    auto invalid = empty;
    invalid.next_column_id = 0;
    Reject([&] { chunkdb::ValidateChunkSchema(invalid); });

    auto entities = Column("entities", chunkdb::ColumnKind::kBytes, 1024);
    entities.nullable = true;
    const auto chunks = chunkdb::AddChunkColumn(empty, entities);
    assert(chunks.version == 2 && chunks.columns.size() == 1 && chunks.columns.front().id == 1);
    assert(chunkdb::SchemaAtVersion(chunks, 1) == empty);
    RoundTrip(chunks, ColumnDomain::kChunk);
    const chunkdb::ChunkLayout variable_row(chunks, 1, 1024, ColumnDomain::kChunk);
    assert(variable_row.payload_bytes() == 0 && variable_row.has_var_columns());
    variable_row.RequireValidVars({}, {1});
    const auto dropped = chunkdb::DropChunkColumn(chunks, "entities");
    assert(dropped.version == 3 && dropped.columns.empty() && dropped.next_column_id == 2);
    assert(chunkdb::SchemaAtVersion(dropped, 2) == chunks);
    RoundTrip(dropped, ColumnDomain::kChunk);
    (void)chunkdb::ChunkLayout(dropped, 1, 1024, ColumnDomain::kChunk);
    auto replacement = chunkdb::AddChunkColumn(dropped, entities);
    assert(replacement.columns.front().id == 2);
    auto required = Column("owner", chunkdb::ColumnKind::kUnsigned, 8);
    required.required = true;
    Reject([&] { (void)chunkdb::AddChunkColumn(empty, required); });
    const auto declared_empty = chunkdb::AddChunkColumn(empty, required, true);
    assert(declared_empty.columns.front().required);
}

void TestSharedSchemaRevisions() {
    auto blocks = chunkdb::SingleBitsColumnSchema(8);
    TableSchema chunks;
    chunkdb::ValidateSchemaDomains(blocks, chunks);
    blocks = chunkdb::AddColumn(blocks, Column("light", chunkdb::ColumnKind::kUnsigned, 4));
    chunks = chunkdb::AdvanceOtherDomain(chunks, ColumnDomain::kChunk);
    chunkdb::ValidateSchemaDomains(blocks, chunks);
    assert(chunks.version == 2 && chunks.columns.empty());
    const Bytes expected_marker{2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 5};
    assert(chunkdb::EncodeChunkSchema(chunks) == expected_marker);
    const auto block_before = blocks;
    chunks = chunkdb::AddChunkColumn(chunks, Column("status", chunkdb::ColumnKind::kUnsigned, 8));
    blocks = chunkdb::AdvanceOtherDomain(blocks, ColumnDomain::kBlock);
    chunkdb::ValidateSchemaDomains(blocks, chunks);
    assert(blocks.version == 3 && chunks.version == 3);
    assert(chunkdb::SchemaAtVersion(blocks, 2) == block_before);
    assert(chunkdb::SchemaAtVersion(chunks, 1).columns.empty());
    assert(chunkdb::SchemaAtVersion(chunks, 2).columns.empty());
    RoundTrip(blocks, ColumnDomain::kBlock);
    RoundTrip(chunks, ColumnDomain::kChunk);
    (void)chunkdb::ChunkLayout(blocks, 4, 1024);
    const auto id = chunks.columns.front().id;
    chunks = chunkdb::RenameChunkColumn(chunks, "status", "state");
    blocks = chunkdb::AdvanceOtherDomain(blocks, ColumnDomain::kBlock);
    assert(chunks.columns.front().id == id);
    chunkdb::ValidateSchemaDomains(blocks, chunks);
    chunks = chunkdb::DropChunkColumn(chunks, "state");
    blocks = chunkdb::AdvanceOtherDomain(blocks, ColumnDomain::kBlock);
    chunkdb::ValidateSchemaDomains(blocks, chunks);
    assert(chunks.columns.empty() && blocks.version == 5);
    RoundTrip(blocks, ColumnDomain::kBlock);
    RoundTrip(chunks, ColumnDomain::kChunk);
    chunkdb::GeometryConfig config;
    config.block_bits = chunkdb::FixedBitsPerBlock(blocks);
    const chunkdb::Geometry geometry(config, blocks, chunks);
    assert(geometry.has_chunk_column_history() && geometry.chunk_columns_layout() == nullptr);
    assert(geometry.ChunkColumnsLayoutAt(1).schema().columns.empty());
    assert(geometry.ChunkColumnsLayoutAt(3).schema().columns.front().name == "status");
    assert(geometry.ChunkColumnsLayoutAt(4).schema().columns.front().name == "state");
    assert(geometry.ChunkColumnsLayoutAt(5).schema().columns.empty());
    Reject([&] { (void)geometry.ChunkColumnsLayoutAt(6); });
    const auto block_only = chunkdb::SingleBitsColumnSchema(8);
    config.block_bits = 8;
    const chunkdb::Geometry unused(config, block_only);
    assert(!unused.has_chunk_column_history() && unused.chunk_columns_layout() == nullptr);
    assert(unused.chunk_schema() == TableSchema{});
}

void TestMalformedDomainHistories() {
    const auto initial = chunkdb::SingleBitsColumnSchema(8);
    const TableSchema empty;
    const auto block_marker = chunkdb::AdvanceOtherDomain(initial, ColumnDomain::kBlock);
    const auto chunk_marker = chunkdb::AdvanceOtherDomain(empty, ColumnDomain::kChunk);
    Reject([&] { chunkdb::ValidateSchemaDomains(block_marker, chunk_marker); });
    const auto block_change = chunkdb::AddColumn(initial, Column("light", chunkdb::ColumnKind::kUnsigned, 4));
    const auto chunk_change = chunkdb::AddChunkColumn(empty, Column("status", chunkdb::ColumnKind::kUnsigned, 8));
    Reject([&] { chunkdb::ValidateSchemaDomains(block_change, chunk_change); });
    Reject([&] { chunkdb::ValidateSchemaDomains(initial, chunk_marker); });
    Reject([&] { (void)chunkdb::AdvanceOtherDomain(initial, static_cast<ColumnDomain>(99)); });
    Reject([&] { (void)chunkdb::ChunkLayout(initial, 1, 1024, static_cast<ColumnDomain>(99)); });
    auto invalid = block_marker;
    invalid.history.back().changes.front().position = 1;
    Reject([&] { chunkdb::ValidateTableSchema(invalid); });
    invalid = block_marker;
    invalid.history.back().changes.front().column.id = 1;
    Reject([&] { (void)chunkdb::EncodeTableSchema(invalid); });
    invalid = block_marker;
    invalid.history.back().changes.push_back(invalid.history.back().changes.front());
    Reject([&] { chunkdb::ValidateTableSchema(invalid); });
    invalid = block_marker;
    invalid.history.back().changes.front().kind = static_cast<SchemaChange::Kind>(255);
    Reject([&] { chunkdb::ValidateTableSchema(invalid); });
    Reject([&] { (void)chunkdb::SchemaAtVersion(invalid, 1); });
    auto bytes = chunkdb::EncodeChunkSchema(chunk_marker);
    bytes.back() = 255;
    Reject([&] { (void)chunkdb::DecodeChunkSchema(bytes.data(), bytes.size()); });
    bytes = chunkdb::EncodeChunkSchema(chunk_marker);
    bytes[16] = 0;  // Empty steps do not stand in for an explicit marker.
    Reject([&] { (void)chunkdb::DecodeChunkSchema(bytes.data(), bytes.size()); });
}

void TestMetadataRowDefaultsAndHistory() {
    const TableSchema empty;
    auto status = Column("status", chunkdb::ColumnKind::kUnsigned, 8);
    status.has_default = true;
    status.default_value = {13};
    const auto chunks = chunkdb::AddChunkColumn(empty, status);
    const chunkdb::ChunkLayout from(empty, 1, 1024, ColumnDomain::kChunk);
    const chunkdb::ChunkLayout to(chunks, 1, 1024, ColumnDomain::kChunk);
    Bytes row;
    chunkdb::ChunkVars vars;
    chunkdb::TranslateChunk(from, to, {1}, &row, &vars);
    assert(row == Bytes{13} && vars.empty());
    const auto renamed = chunkdb::RenameChunkColumn(chunks, "status", "state");
    const chunkdb::ChunkLayout after_rename(renamed, 1, 1024, ColumnDomain::kChunk);
    chunkdb::TranslateChunk(to, after_rename, {1}, &row, &vars);
    assert(row == Bytes{13});
    const auto dropped = chunkdb::DropChunkColumn(renamed, "state");
    const chunkdb::ChunkLayout after_drop(dropped, 1, 1024, ColumnDomain::kChunk);
    chunkdb::TranslateChunk(after_rename, after_drop, {1}, &row, &vars);
    assert(row.empty() && vars.empty());
}

void TestBlockConversionAcrossChunkChanges() {
    TableSchema blocks;
    auto value = Column("value", chunkdb::ColumnKind::kUnsigned, 8);
    value.id = 1;
    blocks.columns = {value};
    blocks.next_column_id = 2;
    TableSchema chunks;
    chunks = chunkdb::AddChunkColumn(chunks, Column("status", chunkdb::ColumnKind::kUnsigned, 8));
    blocks = chunkdb::AdvanceOtherDomain(blocks, ColumnDomain::kBlock);
    const auto before = blocks;
    blocks = chunkdb::ChangeColumnType(blocks, "value", {chunkdb::ColumnKind::kUnsigned, 16},
        chunkdb::Conversion::kExact);
    chunks = chunkdb::AdvanceOtherDomain(chunks, ColumnDomain::kChunk);
    chunkdb::ValidateSchemaDomains(blocks, chunks);
    RoundTrip(blocks, ColumnDomain::kBlock);
    RoundTrip(chunks, ColumnDomain::kChunk);
    Bytes payload{255};
    chunkdb::ChunkVars vars;
    const chunkdb::ChunkLayout from(before, 1, 1024), to(blocks, 1, 1024);
    chunkdb::TranslateChunk(from, to, {1}, &payload, &vars);
    assert((payload == Bytes{255, 0}));
    auto changed = chunks;
    changed.columns.front().name = "different";
    Reject([&] { chunkdb::ValidateSchemaDomains(blocks, changed); });
}

}  // namespace

int main() {
    TestEmptyAndVariableChunkSchema();
    TestSharedSchemaRevisions();
    TestMalformedDomainHistories();
    TestMetadataRowDefaultsAndHistory();
    TestBlockConversionAcrossChunkChanges();
}
