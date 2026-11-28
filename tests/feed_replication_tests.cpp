#include <algorithm>
#include <array>
#include <cassert>
#include <charconv>
#include <chrono>
#include <future>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "chunkdb/engine.hpp"
#include "feed_test_utils.hpp"
#include "store_manifest.hpp"

namespace {
using namespace chunkdb;

struct Resp {
    char type = '\0';
    std::string_view text{};
    std::vector<Resp> items{};
};

template <typename Integer>
Integer Number(std::string_view text) {
    Integer value{};
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    assert(error == std::errc{} && end == text.data() + text.size());
    return value;
}

Resp Read(std::string_view& input) {
    const auto end = input.find("\r\n");
    assert(end != std::string_view::npos && end >= 1U);
    Resp value{.type = input.front(), .text = input.substr(1U, end - 1U)};
    input.remove_prefix(end + 2U);
    if (value.type == '$') {
        const auto size = Number<std::size_t>(value.text);
        assert(size <= input.size() && input.substr(size, 2U) == "\r\n");
        value.text = input.substr(0U, size);
        input.remove_prefix(size + 2U);
    } else if (value.type == '>' || value.type == '*' || value.type == '%') {
        const auto size = Number<std::size_t>(value.text) * (value.type == '%' ? 2U : 1U);
        assert(size <= input.size() / 3U);
        for (std::size_t i = 0; i < size; ++i) value.items.push_back(Read(input));
    }
    return value;
}

Resp Parse(std::string_view input) {
    auto value = Read(input);
    assert(input.empty());
    return value;
}

const Resp& Field(const Resp& map, std::string_view key) {
    assert(map.type == '%');
    for (std::size_t i = 0; i < map.items.size(); i += 2U) {
        if (map.items[i].text == key) return map.items[i + 1U];
    }
    throw std::logic_error("missing RESP map field");
}

std::uint64_t Unsigned(const Resp& value) {
    assert(value.type == ':' || value.type == '(');
    return Number<std::uint64_t>(value.text);
}

void ReadRow(const Resp& row) {
    if (row.type == '_') return;
    assert(row.type == '*');
    for (const auto& field : row.items) assert(field.type == '$' || field.type == '_');
}

// APPLY's single-table commit decoder, expressed against the documented wire
// contract. Decode the sender's actual bytes independently of the feed types.
Resp ReadCommit(const FeedEntry& entry, std::size_t dims, std::string_view table = "t") {
    assert(entry.kind == FeedEntry::Kind::kReplication && entry.protocol_frame);
    auto push = Parse(*entry.protocol_frame);
    const auto& fields = push.items;
    assert((push.type == '>' || push.type == '*') && (fields.size() == 9U || fields.size() == 10U));
    assert((fields[0].type == '$' || fields[0].type == '+') && fields[0].text == "replication");
    assert(fields[1].type == '$' && fields[1].text == StoreIdHex(entry.position.epoch));
    assert(Unsigned(fields[2]) == entry.position.revision && Unsigned(fields[3]) == entry.commit_time_ms);
    assert(fields[4].type == '_' || fields[4].type == '$' || fields[4].type == '+');
    assert(Unsigned(fields[5]) == entry.schema_version);
    assert(fields[6].type == '*' && fields[7].type == '_' && fields[8].type == '_');
    if (fields.size() == 10U && fields[9].type != '_') {
        assert(fields[9].type == '*' && fields[9].items.size() == 1U);
        const auto& member = fields[9].items.front();
        assert((member.type == '$' || member.type == '+') && member.text == table);
    }
    for (const auto& chunk : fields[6].items) {
        assert(chunk.type == '*' && chunk.items.size() == dims + 3U);
        const auto coordinates = [dims](const Resp& position) {
            for (std::size_t i = 0; i < dims; ++i) {
                assert(position.items[i].type == ':');
                (void)Number<std::int64_t>(position.items[i].text);
            }
        };
        coordinates(chunk);
        assert(Unsigned(chunk.items[dims]) == entry.position.revision);
        assert(chunk.items[dims + 1U].type == '$' || chunk.items[dims + 1U].type == '+');
        const auto kind = chunk.items[dims + 1U].text;
        const auto& body = chunk.items[dims + 2U];
        if (kind == "form") {
            assert(body.type == '$' || body.type == '_');
        } else {
            assert(kind == "rows" && body.type == '*' && body.items.size() == 2U);
            assert(body.items[0].type == '*');
            for (const auto& block : body.items[0].items) {
                assert(block.type == '*' && block.items.size() == dims + 1U);
                coordinates(block);
                ReadRow(block.items[dims]);
            }
            ReadRow(body.items[1]);
        }
    }
    return push;
}

struct Fixture {
    explicit Fixture(std::string name, std::chrono::milliseconds linger = {}) : dir(std::move(name)),
        catalog([&] {
            auto config = feed_test::Config(dir.path());
            config.feed_linger = linger;
            return std::make_shared<TableCatalog>(config);
        }()),
        engine({.require_auth = false}, catalog) {
        assert(Run("HELLO 3").find("replication_watch") != std::string::npos);
    }
    std::string Run(std::string_view line, std::span<const std::optional<std::string>> arguments = {}) {
        return engine.Execute(session, std::string(line) + "\r\n", arguments);
    }
    void Ok(std::string_view line) { assert(Run(line) == "+OK\r\n"); }

    test::ScopedTempDir dir;
    std::shared_ptr<TableCatalog> catalog;
    CommandEngine engine;
    SessionState session;
};

FeedOptions Replication(std::optional<FeedPosition> after = {}) {
    return FeedOptions{.after = after, .replication = true};
}

void ExactRowsAndForms() {
    Fixture f("chunkdb-replication-wire");
    f.Ok("CREATE TABLE t (n i8, bits bits(9), f f64, label text(16) NULL, bin bytes(16), flag bool) "
        "CHUNK 4 x 4 CHUNK COLUMNS (owner u8 DEFAULT 3)");
    auto table = f.catalog->Require("t");
    auto replica = table->SubscribeFeed(Replication(FeedPosition{table->Info().store_id, 0U}));
    auto second = table->SubscribeFeed(Replication(replica->position()));
    const std::string nan("\x42\0\0\0\0\0\xf8\xff", 8U);
    const std::string binary("\0\r\n", 3U);
    const std::array<std::optional<std::string>, 2> parameters{nan, binary};
    const auto write = f.Run("SET BLOCK -1 2 IN t n = -2, bits = b'100000001', f = $1, label = NULL, bin = $2, flag = true",
        parameters);
    const auto row = feed_test::Next(*replica);
    assert(Unsigned(Parse(write)) == row->position.revision);
    assert(feed_test::Next(*second)->protocol_frame == row->protocol_frame);
    assert(FeedTestAccess::BufferedBytes(*table) <= table->FeedBufferBytes());
    const auto decoded = ReadCommit(*row, 2U);
    assert(decoded.items.size() == 10U && decoded.items[9].type == '_');
    const auto& chunk = decoded.items[6].items.front().items;
    assert(chunk[0].text == "-1" && chunk[1].text == "0" && chunk[3].text == "rows");
    const auto& block = chunk[4].items[0].items.front().items;
    assert(block[0].text == "-1" && block[1].text == "2");
    assert(block[2].items[0].text == std::string_view("\xfe\xff\xff\xff\xff\xff\xff\xff", 8U));
    assert(block[2].items[1].text == std::string_view("\1\1", 2U));
    assert(block[2].items[2].text == nan && block[2].items[3].type == '_');
    assert(block[2].items[4].text == binary && block[2].items[5].text == std::string_view("\1", 1U));
    assert(chunk[4].items[1].items.front().text == std::string_view("\3\0\0\0\0\0\0\0", 8U));
    assert(row->protocol_frame->starts_with(">10\r\n$11\r\nreplication\r\n$32\r\n"));
    assert(row->protocol_frame->ends_with("_\r\n_\r\n_\r\n"));

    const auto original = f.Run("GET CHUNK -1 0 FROM t");
    const std::array<std::optional<std::string>, 1> form{std::string(Parse(original).text)};
    const auto version = Unsigned(Parse(f.Run("SET CHUNK 1 0 IN t $1", form)));
    const auto whole = feed_test::Next(*replica);
    const auto whole_push = ReadCommit(*whole, 2U);
    const auto& whole_chunk = whole_push.items[6].items.front().items;
    const auto copied = f.Run("GET CHUNK 1 0 FROM t");
    assert(whole->position.revision == version && whole_chunk[3].text == "form");
    assert(whole_chunk[4].type == '$' && whole_chunk[4].text == Parse(copied).text);

    (void)Unsigned(Parse(f.Run("DELETE BLOCK -1 2 FROM t")));
    const auto erased = feed_test::Next(*replica);
    const auto erased_push = ReadCommit(*erased, 2U);
    const auto& deleted = erased_push.items[6].items.front().items[4].items;
    assert(deleted[0].items.front().items[2].type == '_' && deleted[1].type == '_');

    // Writing a previously unwritten empty chunk can take a revision with
    // no changed block or non-default metadata value.
    const auto empty_form = f.Run("GET CHUNK -1 0 FROM t");
    const std::array<std::optional<std::string>, 1> empty{std::string(Parse(empty_form).text)};
    const auto empty_version = Unsigned(Parse(f.Run("SET CHUNK 2 0 IN t $1", empty)));
    const auto empty_write = feed_test::Next(*replica);
    const auto empty_push = ReadCommit(*empty_write, 2U);
    assert(empty_write->position.revision == empty_version);
    assert(empty_push.items[6].items.front().items[3].text == "form");
    assert(empty_push.items[6].items.front().items[4].type == '_');

    // The current expiry producer uses this identity and the chunk path.
    {
        auto lease = table->Acquire();
        const ScopedWriteUser expiry("@expire");
        (void)lease->store().TransformChunkState({1, 0}, {}, false, [](ChunkState& state) {
            std::fill(state.payload.begin(), state.payload.end(), 0U);
            std::fill(state.presence_bitmap.begin(), state.presence_bitmap.end(), 0U);
            state.vars = ChunkVars{};
        });
    }
    const auto expired = feed_test::Next(*replica);
    const auto expired_push = ReadCommit(*expired, 2U);
    assert(expired_push.items[4].text == "@expire");
    assert(expired_push.items[6].items.front().items[3].text == "form");
    assert(expired_push.items[6].items.front().items[4].type == '_');
}

void TransactionsAndSchema() {
    Fixture f("chunkdb-replication-ddl");
    f.Ok("CREATE TABLE t (n u8) CHUNK 2 x 2 x 2 LARGE 2 x 2 x 2");
    auto table = f.catalog->Require("t");
    auto replica = table->SubscribeFeed(Replication());
    auto ordinary = table->SubscribeFeed();
    f.Ok("BEGIN");
    (void)f.Run("SET BLOCK 0 0 -1 IN t n = 1");
    (void)f.Run("SET BLOCK 0 0 2 IN t n = 2");
    assert(f.Run("COMMIT").front() != '-');
    const auto commit = feed_test::Next(*replica);
    const auto decoded = ReadCommit(*commit, 3U);
    assert(decoded.items[6].items.size() == 2U);
    const auto changed = feed_test::Next(*ordinary);
    assert(changed->kind == FeedEntry::Kind::kChange && changed->position == commit->position);

    f.Ok("ALTER TABLE t ADD CHUNK COLUMN owner u8 DEFAULT 4");
    const auto schema = feed_test::Next(*replica);
    const auto schema_push = Parse(*schema->protocol_frame);
    assert(schema->kind == FeedEntry::Kind::kSchema && schema->position.revision > commit->position.revision);
    const auto& map = schema_push.items[1];
    assert(Field(map, "epoch").text == StoreIdHex(commit->position.epoch));
    assert(Unsigned(Field(map, "schema_version")) == 2U && Field(map, "checks").items.empty());
    const auto& step = Field(map, "step").items.front();
    assert(Field(step, "kind").text == "chunk_add" && Unsigned(Field(step, "column_id")) == 1U);
    assert(Field(step, "default").text == std::string_view("\4\0\0\0\0\0\0\0", 8U));
    const auto ordinary_schema = feed_test::Next(*ordinary);
    assert(ordinary_schema->kind == FeedEntry::Kind::kSchema && ordinary_schema->position == schema->position);
    assert(ordinary_schema->protocol_frame->find("step") == std::string::npos);

    f.Ok("ALTER TABLE t ADD CHECK nonnegative (n >= 0)");
    const auto checked = feed_test::Next(*replica);
    const auto checked_push = Parse(*checked->protocol_frame);
    assert(Field(Field(checked_push.items[1], "checks").items.front(), "name").text == "nonnegative");
    assert(Field(Field(checked_push.items[1], "step").items.front(), "kind").text == "add_check");

    const auto revision = Unsigned(Parse(f.Run("SET BLOCK 0 0 -1 IN t n = 3")));
    const auto rows = feed_test::Next(*replica);
    const auto rows_push = ReadCommit(*rows, 3U);
    assert(rows->position.revision == revision && rows->schema_version == 3U && revision > checked->position.revision);
    const auto& row = rows_push.items[6].items.front().items[5].items[0].items.front().items;
    assert(row[2].text == "-1");
}

void CollectionAndCatchUp() {
    Fixture f("chunkdb-replication-collection", std::chrono::hours(1));
    f.Ok("CREATE TABLE old (n u8) CHUNK 4 x 4");
    (void)Unsigned(Parse(f.Run("SET BLOCK 0 0 IN old n = 1")));
    f.Ok("DROP TABLE old"); // Raise the directory floor before making the empty table.
    f.Ok("CREATE TABLE t (n u8) CHUNK 4 x 4");
    auto table = f.catalog->Require("t");
    const auto epoch = table->Info().store_id;
    auto replica = table->SubscribeFeed(Replication(FeedPosition{epoch, 0U}));
    const auto start = replica->position();
    assert(start.revision > 0U);
    const auto set = Unsigned(Parse(f.Run("SET BLOCK 0 0 IN t n = 1")));
    const auto erased = Unsigned(Parse(f.Run("DELETE BLOCK 0 0 FROM t")));
    assert(feed_test::Next(*replica)->position.revision == set);
    const auto deletion = feed_test::Next(*replica);
    assert(deletion->position.revision == erased);
    replica.reset();
    {
        auto lease = table->Acquire();
        lease->store().CheckpointForTests(0, 0);
        lease->store().CheckpointForTests(0, 0);
    }
    auto resumed = table->SubscribeFeed(Replication(deletion->position));
    const auto collect = feed_test::Next(*resumed);
    assert(collect->kind == FeedEntry::Kind::kCollect && collect->position.revision > erased);
    const auto collect_push = Parse(*collect->protocol_frame);
    const auto& tombstone = collect_push.items[3].items.front().items;
    assert(collect_push.items[0].text == "collect" && collect_push.items[1].text == StoreIdHex(epoch));
    assert(tombstone.size() == 4U && tombstone[0].text == "0" && tombstone[1].text == "0");
    assert(Unsigned(tombstone[2]) == erased && Unsigned(tombstone[3]) == deletion->commit_time_ms);

    const auto before = erased + 1U;
    assert(Unsigned(Parse(f.Run("COMPACT TOMBSTONES t BEFORE " + std::to_string(before)))) == erased);
    const auto compact = feed_test::Next(*resumed);
    const auto compact_push = Parse(*compact->protocol_frame);
    assert(compact->kind == FeedEntry::Kind::kCompact && compact->position.revision > collect->position.revision);
    assert(compact_push.items[0].text == "compact" && Unsigned(compact_push.items[3]) == before);

    auto historical = table->SubscribeFeed(Replication(start));
    assert(feed_test::Next(*historical)->position.revision == set);
    assert(feed_test::Next(*historical)->position == deletion->position);
    assert(feed_test::Next(*historical)->protocol_frame == collect->protocol_frame);
    assert(feed_test::Next(*historical)->protocol_frame == compact->protocol_frame);
    auto wrong_epoch = table->SubscribeFeed(Replication(FeedPosition{}));
    assert(feed_test::Next(*wrong_epoch)->kind == FeedEntry::Kind::kResync);

    historical.reset(); resumed.reset(); wrong_epoch.reset();
    table->StopFeed();
    auto lost = table->SubscribeFeed(Replication(deletion->position));
    assert(feed_test::Next(*lost)->kind == FeedEntry::Kind::kResync);
}

void OrdersConcurrentCommits() {
    Fixture f("chunkdb-replication-order");
    f.Ok("CREATE TABLE t (bits bits(4)) CHUNK 4 x 4");
    auto table = f.catalog->Require("t");
    auto replica = table->SubscribeFeed(Replication());
    feed_test::Pause pause(FeedTestHook::Point::kAfterVersion);
    FeedTestAccess::SetHook(*table, &pause);
    auto writer = std::async(std::launch::async, [&] {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, "1000");
    });
    const auto first = pause.Wait();
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(4, 0, "0100");
    }
    assert(!replica->Next());
    pause.Release();
    writer.get();
    assert(feed_test::Next(*replica)->position.revision == first);
    assert(feed_test::Next(*replica)->position.revision > first);
    FeedTestAccess::SetHook(*table, nullptr);
}

void OrdersInlineCollectionAfterItsWrite() {
    Fixture f("chunkdb-replication-inline-collection");
    f.Ok("CREATE TABLE t (n u8) CHUNK 4 x 4 WITH checkpoint_updates = 1");
    auto table = f.catalog->Require("t");
    auto replica = table->SubscribeFeed(Replication());
    (void)Unsigned(Parse(f.Run("SET BLOCK 0 0 IN t n = 1")));
    assert(feed_test::Next(*replica)->kind == FeedEntry::Kind::kReplication);
    const auto erased = Unsigned(Parse(f.Run("DELETE BLOCK 0 0 FROM t")));
    const auto commit = feed_test::Next(*replica);
    const auto collect = feed_test::Next(*replica);
    assert(commit->kind == FeedEntry::Kind::kReplication && commit->position.revision == erased);
    assert(collect->kind == FeedEntry::Kind::kCollect && collect->position.revision > erased);
}

void RefusesFiltersAndSlots() {
    Fixture f("chunkdb-replication-options");
    f.Ok("CREATE TABLE t (n u8) CHUNK 4 x 4");
    for (const auto* options : {"SLOT 'consumer'", "CHUNKS", "NO BEFORE", "COLUMNS n", "AREA 0 0 TO 1 1",
            "TAKEOVER", "ACK TIMEOUT 1", "REPLICATION"}) {
        assert(f.Run("WATCH t REPLICATION " + std::string(options)).starts_with("-ERR SYNTAX"));
    }
    const auto epoch = f.catalog->Require("t")->store_id();
    assert(f.Run("WATCH t REPLICATION AFTER " + StoreIdHex(epoch) + " 0").starts_with("%2\r\n") && f.session.watch);
}

void ResyncsAcrossOrdinaryOnlyHistory() {
    Fixture f("chunkdb-replication-history-gap");
    f.Ok("CREATE TABLE t (n u8) CHUNK 4 x 4");
    auto table = f.catalog->Require("t");
    auto ordinary = table->SubscribeFeed();
    const auto old = ordinary->position();
    (void)Unsigned(Parse(f.Run("SET BLOCK 0 0 IN t n = 1")));
    const auto seen = feed_test::Next(*ordinary);
    auto replica = table->SubscribeFeed(Replication(old));
    assert(feed_test::Next(*replica)->kind == FeedEntry::Kind::kResync);
    auto at_old_frontier = table->SubscribeFeed(Replication(seen->position));
    const auto resync = feed_test::Next(*at_old_frontier);
    assert(resync->kind == FeedEntry::Kind::kResync && resync->position.revision > seen->position.revision);
    auto at_cut = table->SubscribeFeed(Replication(resync->position));
    assert(!at_cut->Next());
    (void)Unsigned(Parse(f.Run("SET BLOCK 0 0 IN t n = 2")));
    const auto next = feed_test::Next(*at_cut);
    assert(next->position.revision > seen->position.revision && next->kind == FeedEntry::Kind::kReplication);
}

void BoundedHistory() {
    Fixture f("chunkdb-replication-budget");
    f.Ok("CREATE TABLE t (data bytes(2048)) CHUNK 4 x 4");
    auto table = f.catalog->Require("t");
    constexpr std::size_t budget = 16U * 1024U;
    auto options = Replication();
    options.buffer_bytes = budget;
    auto lagging = table->SubscribeFeed(options);
    auto current = table->SubscribeFeed(options);
    for (char digit = 'a'; digit < 'i'; ++digit) {
        const std::array<std::optional<std::string>, 1> args{std::string(1024U, digit)};
        const auto written = f.Run("SET BLOCK 0 0 IN t data = $1", args);
        const auto next = feed_test::Next(*current);
        assert(next->kind == FeedEntry::Kind::kReplication && next->position.revision == Unsigned(Parse(written)));
        assert(FeedTestAccess::BufferedBytes(*table) <= budget);
    }
    assert(feed_test::Next(*lagging)->kind == FeedEntry::Kind::kResync);
}
}  // namespace

int main() {
    ExactRowsAndForms();
    TransactionsAndSchema();
    CollectionAndCatchUp();
    OrdersConcurrentCommits();
    OrdersInlineCollectionAfterItsWrite();
    RefusesFiltersAndSlots();
    ResyncsAcrossOrdinaryOnlyHistory();
    BoundedHistory();
}
