#include "reply_stream.hpp"
#include "feed_protocol.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/engine.hpp"
#include "catalog_test_utils.hpp"
#include "test_utils.hpp"

#include <cassert>
#include <limits>

namespace {
using namespace chunkdb;
ChunkLayout Layout() {
    TableSchema schema;
    schema.next_column_id = 5U;
    schema.columns = {
        {.id = 1U, .name = "count", .type = {ColumnKind::kUnsigned, 8U}, .required = true},
        {.id = 2U, .name = "data", .type = {ColumnKind::kBytes, 200000U}, .nullable = true},
        {.id = 3U, .name = "other", .type = {ColumnKind::kBytes, 100U}, .nullable = true},
        {.id = 4U, .name = "optional", .type = {ColumnKind::kUnsigned, 3U}, .nullable = true},
    };
    return ChunkLayout(std::move(schema), 16U, 1024U);
}
std::string Collect(ReplyContinuation& reply, std::size_t credit, std::size_t consumed = 0U) {
    std::string bytes;
    while (!reply.done()) {
        const auto next = reply.Next(credit);
        assert(next.size() <= credit);
        assert(!next.empty() || reply.done());
        bytes += next;
    }
    assert(bytes.size() + consumed == reply.encoded_size());
    assert(reply.Next(credit).empty());
    return bytes;
}
void ProgressiveSnapshotsAndBounds() {
    auto layout = Layout();
    std::vector<ChunkRangeEntry> entries;
    ChunkRangeEntry first{.coord = {-4, 5}, .payload = std::vector<std::uint8_t>(layout.payload_bytes(), 7U),
        .presence_bitmap = {255U, 255U}, .version = 42U};
    first.vars.Assign({2U, 0U}, std::vector<std::uint8_t>(131073U, 9U));
    first.vars.Assign({3U, 15U}, std::vector<std::uint8_t>{0U, 3U});
    entries.push_back(first);
    entries.push_back(first);
    entries.back().coord = {9, -10};
    for (const auto& columns : {std::vector<std::size_t>{}, std::vector<std::size_t>{0U, 1U},
                              std::vector<std::size_t>{2U}, std::vector<std::size_t>{1U, 0U, 2U},
                              std::vector<std::size_t>{3U}, std::vector<std::size_t>{0U, 3U, 1U}}) {
        std::string expected;
        Protocol::AppendArrayHeader(expected, 2U);
        Protocol::AppendInteger(expected, layout.schema().version);
        Protocol::AppendArrayHeader(expected, entries.size());
        for (const auto& entry : entries) {
            Protocol::AppendArrayHeader(expected, 3U);
            Protocol::AppendInteger(expected, entry.coord.x);
            Protocol::AppendInteger(expected, entry.coord.y);
            Protocol::AppendBulk(expected, EncodeChunkForm(layout, entry.version, entry.payload,
                entry.presence_bitmap, entry.vars, columns));
        }
        for (const auto credit : {1U, 17U, 65536U}) {
            auto reply = ChunkReply(layout, entries, columns, true, true, expected.size());
            assert(Collect(*reply, credit) == expected);
        }
        bool rejected = false;
        try { (void)ChunkReply(layout, entries, columns, true, true, expected.size() - 1U); }
        catch (const std::out_of_range&) { rejected = true; }
        assert(rejected);
    }
    auto reply = ChunkReply(layout, {first}, {}, false, false, std::numeric_limits<std::size_t>::max());
    std::string expected;
    Protocol::AppendBulk(expected, EncodeChunkForm(layout, first.version, first.payload, first.presence_bitmap, first.vars, {}));
    first.payload.assign(first.payload.size(), 99U);
    first.vars.Assign({2U, 0U}, std::vector<std::uint8_t>{1U});
    assert(Collect(*reply, 65536U) == expected);
    auto empty = ChunkReply(layout, {}, {}, true, false, 4U);
    assert(Collect(*empty, 1U) == "*0\r\n");
}
void CaptureCreditPrecedesLoadingAndOwnsSnapshot() {
    test::ScopedTempDir dir("chunkdb-reply-capture");
    StoreConfig config;
    config.data_dir = dir.path();
    config.geometry = {.large_chunk_width_chunks = 2U, .large_chunk_height_chunks = 2U,
        .chunk_width_blocks = 4U, .chunk_height_blocks = 4U, .block_bits = 4U};
    config.durability_mode = DurabilityMode::kFsyncWal;
    {
        TableCatalog catalog(CatalogConfigFromStoreConfig(config));
        const auto table = test::CreateBitsTable(catalog, config.geometry);
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, "1000");
    }
    auto catalog = std::make_shared<TableCatalog>(CatalogConfigFromStoreConfig(config));
    CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
    SessionState session;
    (void)engine.Execute(session, "HELLO 3");
    const auto loaded = [&] {
        auto lease = catalog->Require("default")->Acquire();
        return lease->store().RuntimeStats().unique_loaded_chunks;
    };
    assert(loaded() == 0U);
    std::size_t reservations = 0U;
    const std::function<void(std::size_t)> refuse = [&](std::size_t bytes) {
        ++reservations;
        assert(bytes != 0U && loaded() == 0U);
        throw ReplyAdmissionError("capture credit refused");
    };
    for (const auto line : {"GET BLOCK 0 0 FROM default", "GET CHUNK 0 0 FROM default",
                           "GET AREA 0 0 TO 0 0 FROM default"}) {
        std::unique_ptr<ReplyContinuation> reply;
        assert(engine.Execute(session, line, {}, &reply, &refuse).starts_with("-ERR BUSY reason=reply_budget"));
        assert(!reply && loaded() == 0U);
    }
    assert(reservations == 3U);
    std::unique_ptr<ReplyContinuation> reply;
    const std::function<void(std::size_t)> reserve = [&](std::size_t bytes) {
        assert(bytes > 65536U && loaded() == 0U);
    };
    assert(engine.Execute(session, "GET CHUNK 0 0 FROM default", {}, &reply, &reserve).empty());
    assert(reply && loaded() == 1U);
    const auto expected = engine.Execute(session, "GET CHUNK 0 0 FROM default");
    const auto first = reply->Next(1U);
    // The continuation must retain no table lease while encoding or writing.
    SessionState writer;
    (void)engine.Execute(writer, "HELLO 3");
    assert(!engine.Execute(writer, "SET BLOCK 0 0 IN default bits = b'0100'").starts_with('-'));
    assert(engine.Execute(writer, "ALTER TABLE default ADD COLUMN light u8") == "+OK\r\n");
    const auto rest = Collect(*reply, 17U, first.size());
    assert(first + rest == expected);
}
void WideNullableSchemaHasAnObtainableCaptureBound() {
    TableSchema schema;
    schema.columns.push_back({.id = 1U, .name = "fixed", .type = {ColumnKind::kUnsigned, 8U}, .required = true});
    for (std::uint32_t id = 2U; id <= 33U; ++id)
        schema.columns.push_back({.id = id, .name = "v" + std::to_string(id),
            .type = {ColumnKind::kBytes, kMaxVariableValueBytes}, .nullable = true});
    schema.next_column_id = 34U;
    const ChunkLayout layout(std::move(schema), 1U, 1024U);
    const std::vector<std::size_t> fixed{0U};
    assert(BlockReplyCaptureBound(layout, fixed) <= 2147483648ULL);
    std::vector<std::size_t> all;
    for (std::size_t i = 0U; i < layout.schema().columns.size(); ++i) all.push_back(i);
    assert(BlockReplyCaptureBound(layout, all) <= 2147483648ULL);
}
}
int main() {
    ProgressiveSnapshotsAndBounds();
    CaptureCreditPrecedesLoadingAndOwnsSnapshot();
    WideNullableSchemaHasAnObtainableCaptureBound();
}
