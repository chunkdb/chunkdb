#include <cassert>

#include "feed_archive.hpp"
#include "feed_slots.hpp"
#include "feed_protocol.hpp"
#include "feed_test_utils.hpp"
#include "sectioned_chunk_form.hpp"
#include "typed_chunk_form.hpp"

namespace {
using namespace chunkdb;
std::shared_ptr<Table> Create(TableCatalog& catalog, std::uint8_t dims = 2U) {
    auto geometry = txn_test::Config({}).geometry;
    geometry.block_bits = 8U; geometry.dims = dims;
    geometry.chunk_depth_blocks = dims == 3U ? 2U : 1U;
    geometry.large_chunk_depth_chunks = dims == 3U ? 2U : 1U;
    return catalog.Create("t", [&] {
        return TableDefinition{.geometry = geometry, .options = catalog.default_options(),
            .schema = SingleBitsColumnSchema(8U), .chunk_schema = TableSchema{
                .next_column_id = 2U, .columns = {{.id = 1U, .name = "owner",
                    .type = {ColumnKind::kUnsigned, 8U}, .default_value = {}}}}};
    });
}
std::uint64_t Metadata(Table& table, ChunkCoord coord, std::uint64_t owner) {
    auto lease = table.Acquire();
    return lease->store().TransformChunkState(coord, {}, false, [&](ChunkState& state) {
        state.chunk_columns = std::make_shared<const ChunkColumnState>(AssignChunkColumns(
            lease->store().geometry().chunk_schema(), state.chunk_columns.get(), {{"owner", owner}}, !state.written));
    }).version;
}
void Row(const std::optional<std::vector<ColumnValue>>& row, std::uint64_t value) {
    assert(row && row->size() == 1U && std::get<std::uint64_t>(row->front()) == value);
}
void CheckedForm(Table& table, const FeedEntry& event, std::uint64_t owner) {
    assert(event.chunk_forms.size() == 1U && event.chunk_forms.front().form);
    const auto& text = *event.chunk_forms.front().form;
    const auto bytes = std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(text.data()), text.size());
    auto lease = table.Acquire();
    const auto decoded = DecodeSectionedChunkState(lease->store().geometry(), bytes, kVarMaxChunkBytesLimit);
    assert(decoded.chunk_columns);
    const auto row = DecodeChunkColumns(lease->store().geometry().chunk_schema(), decoded.chunk_columns->bytes);
    assert(std::get<std::uint64_t>(row.front()) == owner);
    const auto envelope = ParseChunkFormSections(bytes, kDefaultFeedBufferBytes, ChunkFormReadMode::kRead);
    assert(envelope.header.chunk_version == event.chunk_forms.front().version);
    assert(envelope.header.commit_time_ms == event.commit_time_ms);
}
void LiveMetadataAndCheckedForms() {
    test::ScopedTempDir directory("chunkdb-live-chunk-model");
    TableCatalog catalog(feed_test::Config(directory.path()));
    const auto table = Create(catalog);
    auto ordinary = table->SubscribeFeed();
    auto chunks = table->SubscribeFeed(FeedOptions{.chunks = true});
    const auto first = Metadata(*table, {0, 0}, 7U);
    const auto change = feed_test::Next(*ordinary);
    assert(change->dims == 2U && change->blocks.empty() && change->position.revision == first);
    assert(change->chunk_changes.size() == 1U && !change->chunk_changes[0].before);
    Row(change->chunk_changes[0].after, 7U);
    CheckedForm(*table, *feed_test::Next(*chunks), 7U);
    {
        auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, "10000000");
    }
    const auto block = feed_test::Next(*ordinary);
    assert(block->blocks.size() == 1U && block->chunk_changes.empty());
    CheckedForm(*table, *feed_test::Next(*chunks), 7U);
    (void)Metadata(*table, {0, 0}, 0U);
    const auto reset = feed_test::Next(*ordinary);
    assert(reset->blocks.empty() && reset->chunk_changes.size() == 1U);
    Row(reset->chunk_changes[0].before, 7U); Row(reset->chunk_changes[0].after, 0U);
    CheckedForm(*table, *feed_test::Next(*chunks), 0U);
    {
        auto lease = table->Acquire(); lease->store().UnsetBlock(0, 0);
    }
    assert(feed_test::Next(*ordinary)->chunk_changes.empty());
    const auto empty = feed_test::Next(*chunks);
    assert(empty->chunk_forms.size() == 1U && !empty->chunk_forms[0].form);
}
void ArchivePreservesMetadataRowsAndForms() {
    test::ScopedTempDir directory("chunkdb-archive-chunk-model");
    auto config = feed_test::Config(directory.path()); config.slot_sync_interval = std::chrono::hours(1);
    TableCatalog catalog(config);
    const auto table = Create(catalog);
    const auto slot = table->CreateFeedSlot("consumer");
    const auto first = Metadata(*table, {0, 0}, 9U);
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, "10000000");
        lease->store().CheckpointForTests(0, 0);
    }
    FeedSlotTestAccess::Sync(*table);
    auto reader = table->ReadFeedArchive(slot.position);
    const auto metadata = reader.Next();
    assert(metadata && metadata->position.revision == first && metadata->blocks.empty());
    assert(metadata->chunk_changes.size() == 1U && !metadata->chunk_changes[0].before);
    Row(metadata->chunk_changes[0].after, 9U);
    const auto block = reader.Next();
    assert(block && block->blocks.size() == 1U && block->chunk_changes.empty() && !reader.Next());
    auto forms = table->ReadFeedArchive(slot.position);
    FeedSlotTestAccess::EnableChunkForms(forms, kDefaultFeedBufferBytes);
    const auto first_form = forms.Next(); assert(first_form);
    CheckedForm(*table, *first_form, 9U);
    const auto second_form = forms.Next(); assert(second_form);
    CheckedForm(*table, *second_form, 9U);
    assert(!forms.Next());
}
void BaselineInitializationAndSchemaRows() {
    test::ScopedTempDir directory("chunkdb-feed-baseline-row");
    TableCatalog catalog(feed_test::Config(directory.path()));
    const auto table = Create(catalog);
    auto ordinary = table->SubscribeFeed();
    auto chunks = table->SubscribeFeed(FeedOptions{.chunks = true});
    (void)Metadata(*table, {0, 0}, 0U);
    const auto baseline = feed_test::Next(*ordinary);
    assert(baseline->blocks.empty() && baseline->chunk_changes.size() == 1U);
    assert(!baseline->chunk_changes[0].before); Row(baseline->chunk_changes[0].after, 0U);
    const auto empty = feed_test::Next(*chunks);
    assert(empty->chunk_forms.size() == 1U && !empty->chunk_forms[0].form);
    catalog.ChangeChunkColumns("t", [](const auto& schema) {
        return AddChunkColumn(schema, Column{.name = "label", .type = {ColumnKind::kText, 128U},
            .nullable = true, .default_value = {}});
    });
    const auto changed = feed_test::Next(*ordinary);
    assert(changed->kind == FeedEntry::Kind::kSchema && changed->dims == 2U);
    assert(changed->columns.size() == 1U && changed->chunk_columns.size() == 2U);
    assert(feed_test::Next(*chunks)->kind == FeedEntry::Kind::kSchema);
    const auto plain = catalog.Create("plain", txn_test::Config({}).geometry, catalog.default_options());
    auto plain_watch = plain->SubscribeFeed();
    {
        auto lease = plain->Acquire(); lease->store().SetBlockBits(0, 0, feed_test::Bits(1U));
    }
    assert(feed_test::Next(*plain_watch)->chunk_changes.empty());
}
void ArchiveEnforcesDecodedEntryBudget() {
    test::ScopedTempDir directory("chunkdb-archive-row-budget");
    TableCatalog catalog(feed_test::Config(directory.path()));
    const auto table = Create(catalog);
    catalog.ChangeChunkColumns("t", [](const auto& schema) {
        return AddChunkColumn(schema, Column{.name = "label", .type = {ColumnKind::kText, 262144U},
            .nullable = true, .default_value = {}});
    });
    const auto slot = table->CreateFeedSlot("budget");
    {
        auto lease = table->Acquire();
        (void)lease->store().TransformChunkState({0, 0}, {}, false, [&](ChunkState& state) {
            state.chunk_columns = std::make_shared<const ChunkColumnState>(AssignChunkColumns(
                lease->store().geometry().chunk_schema(), state.chunk_columns.get(),
                {{"label", std::string(131072U, 'x')}}, true));
        });
        lease->store().CheckpointForTests(0, 0);
    }
    FeedSlotTestAccess::Sync(*table);
    for (const bool forms : {false, true}) {
        auto reader = table->ReadFeedArchive(slot.position);
        FeedSlotTestAccess::SetEntryBudget(reader, 16384U);
        if (forms) FeedSlotTestAccess::EnableChunkForms(reader, 16384U);
        bool rejected = false;
        try { (void)reader.Next(); } catch (const std::length_error&) { rejected = true; }
        assert(rejected);
    }
    auto admitted = table->ReadFeedArchive(slot.position);
    FeedSlotTestAccess::SetEntryBudget(admitted, 1048576U);
    const auto entry = admitted.Next();
    assert(entry && entry->chunk_changes.size() == 1U);
    assert(FeedEntryRetainedBytes(*entry) > 131072U && FeedEntryRetainedBytes(*entry) <= 1048576U);
}
void ThreeDimensionalBlockCoordinates() {
    test::ScopedTempDir directory("chunkdb-feed-three-dimensional");
    TableCatalog catalog(feed_test::Config(directory.path()));
    const auto table = Create(catalog, 3U);
    auto watch = table->SubscribeFeed();
    {
        auto lease = table->Acquire(); lease->store().SetBlockBits3D(-1, 8, -5, "10000000");
    }
    const auto entry = feed_test::Next(*watch);
    assert(entry->dims == 3U && entry->blocks.size() == 1U);
    const auto& block = entry->blocks[0];
    assert((block.chunk == ChunkCoord{-1, 2, -3}));
    assert(block.x == -1 && block.y == 8 && block.z == -5);
    assert(block.local_x == 3U && block.local_y == 0U && block.local_z == 1U);
}
}
int main(int argc, char** argv) {
    if (argc > 1) {
        if (std::string_view(argv[1]) == "--archive-budget") ArchiveEnforcesDecodedEntryBudget();
        else ThreeDimensionalBlockCoordinates();
        return 0;
    }
    LiveMetadataAndCheckedForms();
    ArchivePreservesMetadataRowsAndForms();
    BaselineInitializationAndSchemaRows();
    ArchiveEnforcesDecodedEntryBudget();
}
