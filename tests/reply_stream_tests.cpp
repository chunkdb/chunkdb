#include "reply_stream.hpp"
#include "feed_protocol.hpp"
#include "chunkdb/protocol.hpp"

#include <cassert>
#include <limits>

namespace {
using namespace chunkdb;
ChunkLayout Layout() {
    TableSchema schema;
    schema.next_column_id = 4U;
    schema.columns = {
        {.id = 1U, .name = "count", .type = {ColumnKind::kUnsigned, 8U}, .required = true},
        {.id = 2U, .name = "data", .type = {ColumnKind::kBytes, 200000U}, .nullable = true},
        {.id = 3U, .name = "other", .type = {ColumnKind::kBytes, 100U}, .nullable = true},
    };
    return ChunkLayout(std::move(schema), 16U, 1024U);
}
std::string Collect(ReplyContinuation& reply, std::size_t credit) {
    std::string bytes;
    while (!reply.done()) {
        const auto next = reply.Next(credit);
        assert(next.size() <= credit);
        assert(!next.empty() || reply.done());
        bytes += next;
    }
    assert(bytes.size() == reply.encoded_size());
    assert(reply.Next(credit).empty());
    return bytes;
}
void ProgressiveSnapshotsAndBounds() {
    auto layout = Layout();
    std::vector<ChunkRangeEntry> entries;
    ChunkRangeEntry first{.coord = {-4, 5}, .payload = std::vector<std::uint8_t>(16U, 7U),
        .presence_bitmap = {255U, 255U}, .version = 42U};
    first.vars.Assign({2U, 0U}, std::vector<std::uint8_t>(131073U, 9U));
    first.vars.Assign({3U, 15U}, std::vector<std::uint8_t>{0U, 3U});
    entries.push_back(first);
    entries.push_back(first);
    entries.back().coord = {9, -10};
    for (const auto& columns : {std::vector<std::size_t>{}, std::vector<std::size_t>{0U, 1U},
                              std::vector<std::size_t>{2U}, std::vector<std::size_t>{1U, 0U, 2U}}) {
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
}
int main() { ProgressiveSnapshotsAndBounds(); }
