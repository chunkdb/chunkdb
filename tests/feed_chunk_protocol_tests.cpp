#include <cassert>
#include <iostream>
#include <limits>

#include "chunkdb/chunk_columns.hpp"
#include "feed_protocol.hpp"
#include "sectioned_chunk_form.hpp"
#include "store_manifest.hpp"
#include "typed_chunk_form.hpp"

namespace {
using namespace chunkdb;
using Row = std::vector<ColumnValue>;
const StoreId kEpoch{1U, 2U, 3U, 4U};

template <typename Fn> void Refuse(Fn&& fn) {
    bool refused = false;
    try { fn(); } catch (const std::exception&) { refused = true; }
    assert(refused);
}

Column Scalar(std::uint32_t id, std::string name) {
    return {.id = id, .name = std::move(name), .type = {ColumnKind::kUnsigned, 8U},
        .has_default = true, .default_value = {0U}};
}

void DimensionalFrames() {
    FeedEntry entry;
    entry.position = {kEpoch, std::numeric_limits<std::uint64_t>::max()};
    entry.commit_time_ms = 123U; entry.schema_version = 7U; entry.dims = 3U;
    entry.blocks.push_back({.chunk = {-1, 2, 3}, .x = -4, .y = 5,
        .before = Row{std::uint64_t{1U}}, .after = Row{std::uint64_t{2U}}, .z = 0});
    entry.chunk_changes.push_back({{-1, 2, 3}, Row{std::uint64_t{10U}, std::string("old")},
        Row{std::uint64_t{11U}, std::string("new")}});
    const auto expected = ">10\r\n$6\r\nchange\r\n$32\r\n" + StoreIdHex(kEpoch) +
        "\r\n(18446744073709551615\r\n:123\r\n_\r\n:7\r\n*1\r\n*5\r\n:-4\r\n:5\r\n:0\r\n"
        "*1\r\n:1\r\n*1\r\n:2\r\n_\r\n_\r\n*1\r\n*5\r\n:-1\r\n:2\r\n:3\r\n"
        "*2\r\n:10\r\n$3\r\nold\r\n*2\r\n:11\r\n$3\r\nnew\r\n";
    assert(EncodeFeedEntry(entry) == expected);
    entry.kind = FeedEntry::Kind::kChunks;
    entry.chunk_forms = {{{-1, 2, 0}, 4U, nullptr}};
    assert(EncodeFeedEntry(entry).find("*1\r\n*5\r\n:-1\r\n:2\r\n:0\r\n:4\r\n_\r\n") != std::string::npos);
    entry.kind = FeedEntry::Kind::kSchema;
    entry.columns = {Scalar(1U, "block_value")};
    entry.chunk_columns = {Scalar(1U, "chunk_value")};
    const auto schema = EncodeFeedEntry(entry);
    assert(schema.find("$4\r\ndims\r\n:3\r\n") != std::string::npos);
    assert(schema.find("$13\r\nchunk_columns\r\n*1\r\n") != std::string::npos);
    assert(schema.find("chunk_value") != std::string::npos);
    entry.kind = FeedEntry::Kind::kChange;
    entry.dims = 2U;
    Refuse([&] { (void)EncodeFeedEntry(entry); });
    entry.dims = 0U;
    Refuse([&] { (void)EncodeFeedEntry(entry); });
    entry.dims = 3U; entry.kind = static_cast<FeedEntry::Kind>(255U);
    Refuse([&] { (void)EncodeFeedEntry(entry); });
}

void OverflowCoordinates() {
    FeedEntry entry;
    entry.dims = 3U; entry.position = {kEpoch, 1U};
    entry.blocks.push_back({.chunk = {INT64_MIN, 0, INT64_MAX}, .y = 0, .before = {},
        .after = Row{std::uint64_t{1U}}, .local_x = 3U, .local_z = 2U});
    const auto bytes = EncodeFeedEntry(entry);
    assert(bytes.find("*5\r\n*2\r\n:-9223372036854775808\r\n:3\r\n:0\r\n*2\r\n:9223372036854775807\r\n:2\r\n") != std::string::npos);
}

void FiltersRetainMetadata() {
    auto entry = std::make_shared<FeedEntry>();
    entry->dims = 3U;
    for (const auto z : {-1, 1}) {
        entry->blocks.push_back({.chunk = {0, 0, z}, .x = 0, .y = 0,
            .before = Row{std::uint64_t{1U}, std::uint64_t{2U}},
            .after = Row{std::uint64_t{3U}, std::uint64_t{4U}}, .z = z});
        entry->chunk_changes.push_back({{0, 0, z}, Row{std::uint64_t{5U}, std::uint64_t{6U}},
            Row{std::uint64_t{7U}, std::uint64_t{8U}}});
    }
    entry->protocol_frame = std::make_shared<const std::string>("cached whole entry");
    const std::vector<Column> columns{Scalar(1U, "a"), Scalar(2U, "b")};
    const FeedArea area{{0, 0, 1}, {0, 0, 1}, 3U};
    auto filtered = FilterFeedEntry(entry, area, {}, false, columns);
    assert(filtered && filtered->blocks.size() == 1U && filtered->chunk_changes.size() == 1U);
    assert(filtered->blocks[0].chunk.z == 1 && filtered->chunk_changes[0].chunk.z == 1);
    filtered = FilterFeedEntry(entry, area, std::vector<std::uint32_t>{2U}, true, columns);
    assert(filtered && filtered->dims == 3U && !filtered->protocol_frame);
    assert(!filtered->blocks[0].before && filtered->blocks[0].after == std::optional<Row>(Row{std::uint64_t{4U}}));
    assert(!filtered->chunk_changes[0].before && filtered->chunk_changes[0].after->size() == 2U);
    assert(entry->blocks[1].before && entry->chunk_changes[1].before && entry->protocol_frame);
    auto metadata_only = std::make_shared<FeedEntry>(*entry);
    metadata_only->blocks.clear();
    metadata_only->chunk_changes.erase(metadata_only->chunk_changes.begin());
    assert(FilterFeedEntry(metadata_only, area, {}, false, columns));
    assert(!FilterFeedEntry(metadata_only, FeedArea{{0, 0, -1}, {0, 0, -1}, 3U}, {}, false, columns));
    Refuse([&] { (void)FilterFeedEntry(entry, FeedArea{{0, 0}, {0, 0}}, {}, false, columns); });
    Refuse([&] { (void)FilterFeedEntry(entry, {}, std::vector<std::uint32_t>{}, false, columns); });
    entry->kind = FeedEntry::Kind::kSchema; entry->columns = columns;
    entry->chunk_columns = {Scalar(1U, "metadata")};
    filtered = FilterFeedEntry(entry, {}, std::vector<std::uint32_t>{2U}, true, columns);
    assert(filtered->columns.size() == 1U && filtered->columns[0].id == 2U && filtered->chunk_columns.size() == 1U);
}

void CheckedHistoricalProjection() {
    const TableSchema blocks{.version = 1U, .next_column_id = 3U, .columns = {Scalar(1U, "a"), Scalar(2U, "b")}};
    const TableSchema metadata{.version = 1U, .next_column_id = 2U, .columns = {Scalar(1U, "owner")}};
    GeometryConfig config;
    config.dims = 3U; config.chunk_width_blocks = 2U; config.chunk_height_blocks = 2U;
    config.chunk_depth_blocks = 2U; config.block_bits = 16U;
    const Geometry original(config, blocks, metadata);
    ChunkState state{.version = 5U, .payload = std::vector<std::uint8_t>(original.ChunkPayloadBytes(), 0U),
        .presence_bitmap = {1U}, .written = true, .commit_time_ms = 50U};
    state.payload[original.layout().FixedColumnAt(0U)->values] = 11U;
    state.payload[original.layout().FixedColumnAt(1U)->values] = 22U;
    state.chunk_columns = std::make_shared<const ChunkColumnState>(AssignChunkColumns(metadata, nullptr,
        {{"owner", std::uint64_t{7U}}}, true));
    const auto body = EncodeSectionedChunkState(original, state, kVarMaxChunkBytesLimit);
    const auto form = std::make_shared<const std::string>(reinterpret_cast<const char*>(body.data()), body.size());
    auto entry = std::make_shared<FeedEntry>();
    entry->dims = 3U; entry->position = {kEpoch, 10U}; entry->commit_time_ms = 100U;
    entry->schema_version = 1U; entry->chunk_forms_complete = true;
    entry->chunk_forms = {{{0, 0, 0}, 5U, form}, {{0, 0, 2}, 5U, form}};
    const auto newer_blocks = AddColumn(blocks, Scalar(3U, "later"));
    const auto newer_metadata = AdvanceOtherDomain(metadata, ColumnDomain::kChunk);
    config.block_bits = FixedBitsPerBlock(newer_blocks);
    const Geometry current(config, newer_blocks, newer_metadata);
    const auto projected = ChunkFeedEntry(entry, current, FeedArea{{0, 0, 0}, {0, 0, 0}, 3U}, std::vector<std::uint32_t>{2U});
    assert(projected && projected->dims == 3U && projected->chunk_forms.size() == 1U);
    const auto& value = *projected->chunk_forms[0].form;
    const std::span<const std::uint8_t> bytes(reinterpret_cast<const std::uint8_t*>(value.data()), value.size());
    const auto parsed = ParseChunkFormSections(bytes, bytes.size(), ChunkFormReadMode::kRead);
    assert(parsed.header.flags == kChunkFormProjected && parsed.header.chunk_version == 5U && parsed.header.commit_time_ms == 50U);
    assert(parsed.header.schema_version == 1U);
    const std::vector<std::size_t> selected{1U};
    const auto decoded = DecodeSectionedChunkState(original, bytes, kVarMaxChunkBytesLimit, selected);
    assert(decoded.payload[original.layout().FixedColumnAt(1U)->values] == 22U);
    assert(std::get<std::uint64_t>(DecodeChunkColumns(metadata, decoded.chunk_columns->bytes)[0]) == 7U);
    Refuse([&] { (void)ChunkFeedEntry(entry, current, {}, std::vector<std::uint32_t>{3U}); });
    entry->chunk_forms[0].version = 6U;
    Refuse([&] { (void)ChunkFeedEntry(entry, current, {}, std::vector<std::uint32_t>{2U}); });
    entry->chunk_forms[0].version = 5U;
    entry->chunk_forms[0].form = std::make_shared<const std::string>("truncated");
    Refuse([&] { (void)ChunkFeedEntry(entry, current, {}, std::vector<std::uint32_t>{2U}); });
}

void RetainedMetadataBudget() {
    FeedEntry entry;
    constexpr std::size_t row_bytes = 128U * 1024U;
    entry.chunk_changes.push_back({{}, Row{std::string(row_bytes, 'a')}, Row{BytesValue{std::vector<std::uint8_t>(row_bytes, 7U)}}});
    assert(FeedRowRetainedBytes(entry.chunk_changes[0].before) >= row_bytes);
    assert(FeedEntryRetainedBytes(entry) > 2U * row_bytes);
    const auto before_schema = FeedEntryRetainedBytes(entry);
    auto column = Scalar(1U, "metadata");
    column.type = {ColumnKind::kBytes, static_cast<std::uint32_t>(row_bytes)};
    column.default_value.assign(row_bytes, 0U);
    entry.chunk_columns.push_back(std::move(column));
    assert(FeedEntryRetainedBytes(entry) >= before_schema + row_bytes);
    assert(FeedRowRetainedBytes({}) == 0U);
}

void EmptyMetadataProjection() {
    const TableSchema blocks{.version = 1U, .next_column_id = 2U, .columns = {Scalar(1U, "a")}};
    GeometryConfig config;
    config.chunk_width_blocks = 2U; config.chunk_height_blocks = 2U; config.block_bits = 8U;
    const Geometry original(config, blocks);
    ChunkState state{.version = 5U, .payload = {7U, 0U, 0U, 0U}, .presence_bitmap = {1U},
        .written = true, .commit_time_ms = 50U};
    const auto body = EncodeSectionedChunkState(original, state, kVarMaxChunkBytesLimit);
    auto entry = std::make_shared<FeedEntry>();
    entry->schema_version = 1U; entry->chunk_forms_complete = true; entry->position = {kEpoch, 5U};
    entry->chunk_forms.push_back({{}, 5U, std::make_shared<const std::string>(
        reinterpret_cast<const char*>(body.data()), body.size())});
    const auto newer_blocks = AddColumn(blocks, Scalar(2U, "later"));
    config.block_bits = FixedBitsPerBlock(newer_blocks);
    const Geometry newer(config, newer_blocks);
    for (const auto* geometry : {&original, &newer}) {
        const auto projected = ChunkFeedEntry(entry, *geometry, {}, std::vector<std::uint32_t>{1U});
        assert(projected && projected->chunk_forms.size() == 1U && projected->chunk_forms[0].form);
        const auto& form = *projected->chunk_forms[0].form;
        const std::span<const std::uint8_t> bytes(reinterpret_cast<const std::uint8_t*>(form.data()), form.size());
        const auto parsed = ParseChunkFormSections(bytes, bytes.size(), ChunkFormReadMode::kRead);
        assert(parsed.header.schema_version == 1U && parsed.header.chunk_version == 5U && parsed.header.commit_time_ms == 50U);
        const std::vector<std::size_t> selected{0U};
        const auto decoded = DecodeSectionedChunkState(original, bytes, kVarMaxChunkBytesLimit, selected);
        assert(decoded.payload[0] == 7U && !decoded.chunk_columns);
    }
}
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--empty-domain") { EmptyMetadataProjection(); return 0; }
    assert(argc == 1);
    DimensionalFrames(); OverflowCoordinates(); FiltersRetainMetadata(); CheckedHistoricalProjection(); RetainedMetadataBudget();
    EmptyMetadataProjection();
}
