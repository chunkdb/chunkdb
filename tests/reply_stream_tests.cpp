#include "reply_stream.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/engine.hpp"
#include "catalog_test_utils.hpp"
#include "test_utils.hpp"

#include <cassert>
#include <limits>

namespace {
using namespace chunkdb;
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
    std::vector<std::uint8_t> first(131073U, 9U), second{0U, 3U, 255U};
    ReplyParts parts;
    Protocol::AppendArrayHeader(parts.text(), 2U);
    Protocol::AppendInteger(parts.text(), std::uint64_t{7U});
    Protocol::AppendArrayHeader(parts.text(), 2U);
    Protocol::AppendInteger(parts.text(), std::int64_t{-4});
    parts.AppendBulk(first);
    Protocol::AppendInteger(parts.text(), std::int64_t{9});
    parts.AppendBulk(second);
    std::string expected;
    Protocol::AppendArrayHeader(expected, 2U);
    Protocol::AppendInteger(expected, std::uint64_t{7U});
    Protocol::AppendArrayHeader(expected, 2U);
    Protocol::AppendInteger(expected, std::int64_t{-4});
    Protocol::AppendBulk(expected, {reinterpret_cast<const char*>(first.data()), first.size()});
    Protocol::AppendInteger(expected, std::int64_t{9});
    Protocol::AppendBulk(expected, {reinterpret_cast<const char*>(second.data()), second.size()});
    assert(parts.size() == expected.size());
    for (const auto credit : {1U, 17U, 65536U}) {
        auto copy = parts;
        auto reply = std::move(copy).Continuation();
        assert(Collect(*reply, credit) == expected);
    }
    // The parts own their bodies: the sources may change afterwards.
    first.assign(first.size(), 1U);
    assert(std::move(parts).Join() == expected);
    ReplyParts empty;
    Protocol::AppendArrayHeader(empty.text(), 0U);
    auto reply = std::move(empty).Continuation();
    assert(Collect(*reply, 1U) == "*0\r\n");
}
// A continuation writes exactly the reply of the statement: checked sectioned
// forms with chunk columns and projections, three-axis areas with and
// without schema, private transaction states and identity pages with data.
void ChunkModelContinuationsMatchReplies() {
    test::ScopedTempDir dir("chunkdb-reply-chunk-model");
    CatalogConfig config;
    config.data_dir = dir.path();
    config.background_maintenance = false;
    auto catalog = std::make_shared<TableCatalog>(config);
    CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
    SessionState session;
    assert(engine.Execute(session, "HELLO 3\r\n").starts_with("%"));
    const auto run = [&](const std::string& line, const std::vector<std::optional<std::string>>& parameters = {}) {
        return engine.Execute(session, line + "\r\n", parameters);
    };
    assert(run("CREATE TABLE t (a u8 DEFAULT 0, note text(64) NULL) CHUNK 2 x 2 x 2 LARGE 2 x 2 x 2 "
               "CHUNK COLUMNS (owner u8 DEFAULT 0, tag bytes(128) NULL)") == "+OK\r\n");
    assert(run("SET BLOCK -1 -1 -1 IN t a=7, note='x'").starts_with(":"));
    assert(run("SET BLOCK 0 0 0 IN t a=9").starts_with(":"));
    assert(run("SET CHUNK -1 -1 -1 IN t META owner=3, tag=$1", {"abc"}).starts_with(":"));
    const auto same = [&](const std::string& line) {
        const auto expected = run(line);
        assert(expected.size() > 64U && !expected.starts_with("-"));
        for (const auto credit : {1U, 17U, 65536U}) {
            std::unique_ptr<ReplyContinuation> reply;
            assert(engine.Execute(session, line + "\r\n", {}, &reply).empty() && reply);
            assert(Collect(*reply, credit) == expected);
        }
    };
    for (const auto* line : {"GET CHUNK -1 -1 -1 FROM t", "GET CHUNK -1 -1 -1 FROM t COLUMNS note",
             "GET AREA -1 -1 -1 TO 0 0 0 FROM t COLUMNS a WITH SCHEMA", "GET AREA AROUND -1 -1 -1 RADIUS 1 FROM t",
             "SCAN CHUNKS FROM t WITH DATA", "SCAN CHUNKS FROM t LIMIT 1 WITH DATA COLUMNS (a)", "SCAN CHUNKS FROM t"})
        same(line);
    assert(run("BEGIN") == "+OK\r\n");
    assert(run("SET BLOCK -1 -1 -1 IN t a=11") == "_\r\n");
    same("GET CHUNK -1 -1 -1 FROM t");
    same("GET AREA -1 -1 -1 TO 0 0 0 FROM t");
    assert(run("ROLLBACK") == "+OK\r\n");
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
    schema.columns.push_back({.id = 1U, .name = "fixed", .type = {ColumnKind::kUnsigned, 8U}, .required = true, .default_value = {}});
    for (std::uint32_t id = 2U; id <= 33U; ++id)
        schema.columns.push_back({.id = id, .name = "v" + std::to_string(id),
            .type = {ColumnKind::kBytes, kMaxVariableValueBytes}, .nullable = true, .default_value = {}});
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
    ChunkModelContinuationsMatchReplies();
    CaptureCreditPrecedesLoadingAndOwnsSnapshot();
    WideNullableSchemaHasAnObtainableCaptureBound();
}
