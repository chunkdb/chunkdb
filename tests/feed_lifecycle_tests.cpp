#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <cmath>
#include <limits>
#include <thread>

#include "crypto.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/protocol.hpp"
#include "scram.hpp"
#include "feed_test_utils.hpp"
#include "feed_protocol.hpp"
#include "user_registry.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::feed_test;
using chunkdb::test::ScopedTempDir;

void RowDecoderScratch() {
    ScopedTempDir dir("chunkdb-feed-row-scratch");
    auto catalog = std::make_shared<TableCatalog>(Config(dir.path()));
    CommandEngine engine({.require_auth = false}, catalog);
    SessionState session;
    (void)engine.Execute(session, "HELLO 3\r\n");
    assert(engine.Execute(session, "CREATE TABLE rows (n u3 NULL, i i17, flag bool, f f32, g f64, b bits(9), wide u64) CHUNK 2 x 2\r\n") == "+OK\r\n");
    auto table = catalog->Find("rows");
    auto feed = table->SubscribeFeed();
    auto lease = table->Acquire();
    auto& store = lease->store();
    store.SetBlock(1, 0, {{"n", std::monostate{}}, {"i", std::int64_t{-65536}}, {"flag", true},
        {"f", float{1.25}}, {"g", double{-2.5}}, {"b", BitsValue{"101010101"}},
        {"wide", std::numeric_limits<std::uint64_t>::max()}});
    const auto before = store.GetBlock(1, 0);
    const auto first = Next(*feed);
    assert(!first->blocks[0].before && first->blocks[0].after == before);
    store.SetBlock(1, 0, {{"n", std::uint64_t{7}}, {"i", std::int64_t{65535}}, {"flag", false}});
    const auto second = Next(*feed);
    assert(second->blocks[0].before == before && second->blocks[0].after == store.GetBlock(1, 0));
}

void WireNumberLimits() {
    FeedEntry entry;
    entry.position.revision = std::numeric_limits<std::uint64_t>::max();
    entry.commit_time_ms = entry.position.revision;
    entry.schema_version = entry.position.revision;
    entry.blocks.push_back({{0, std::numeric_limits<std::int64_t>::max()},
        std::numeric_limits<std::int64_t>::min(), std::nullopt, std::nullopt,
        std::vector<ColumnValue>{entry.position.revision, BitsValue{"101010101"}}, 0U, std::numeric_limits<std::uint32_t>::max()});
    const auto expected = std::string(">7\r\n$6\r\nchange\r\n$32\r\n") + std::string(32, '0') +
        "\r\n:18446744073709551615\r\n:18446744073709551615\r\n_\r\n:18446744073709551615\r\n"
        "*1\r\n*4\r\n:-9223372036854775808\r\n*2\r\n:9223372036854775807\r\n:4294967295\r\n"
        "_\r\n*2\r\n:18446744073709551615\r\n$2\r\n\x55\x01\r\n";
    assert(EncodeFeedEntry(entry) == expected);
}

void PackedRows() {
    ScopedTempDir dir("chunkdb-feed-packed-rows");
    auto catalog = std::make_shared<TableCatalog>(Config(dir.path()));
    CommandEngine engine({.require_auth = false}, catalog);
    SessionState session;
    (void)engine.Execute(session, "HELLO 3\r\n");
    assert(engine.Execute(session, "CREATE TABLE packed (tail bits(9), aligned bits(16)) CHUNK 16 x 16\r\n") == "+OK\r\n");
    auto table = catalog->Find("packed");
    auto feed = table->SubscribeFeed();
    auto lease = table->Acquire();
    auto& store = lease->store();
    auto state = store.ReadChunkState(0, 0);
    for (unsigned byte = 0U; byte < 256U; ++byte) {
        std::string digits(16, '0');
        for (unsigned bit = 0U; bit < 16U; ++bit)
            digits[bit] = (((bit < 8U ? byte : byte ^ 255U) >> (bit % 8U)) & 1U) != 0U ? '1' : '0';
        const BitsValue tail{digits.substr(0U, 9U)}, aligned{digits};
        store.SetBlockInState(state, byte % 16U, byte / 16U, {{"tail", tail}, {"aligned", aligned}});
        FeedEntry encoded;
        encoded.blocks.push_back({{0, 0}, 0, 0, std::nullopt,
            std::vector<ColumnValue>{tail, aligned}, 0U, 0U});
        std::string expected = "*2\r\n";
        Protocol::AppendValue(expected, tail);
        Protocol::AppendValue(expected, aligned);
        assert(EncodeFeedEntry(encoded).ends_with(expected));
    }
    const auto revision = state.version;
    assert(store.WriteChunkState(0, 0, std::move(state), revision).ok);
    const auto entry = Next(*feed);
    assert(entry->blocks.size() == 256U);
    for (const auto& block : entry->blocks) {
        assert(!block.before);
        assert(block.after == store.GetBlock(*block.x, *block.y));
    }
}

void RepeatedFixedRows() {
    ScopedTempDir dir("chunkdb-feed-repeated-fixed");
    auto catalog = std::make_shared<TableCatalog>(Config(dir.path()));
    CommandEngine engine({.require_auth = false}, catalog);
    SessionState session;
    (void)engine.Execute(session, "HELLO 3\r\n");
    assert(engine.Execute(session, "CREATE TABLE cached (n u64 NULL, i i16, f f32, g f64, b bits(16)) CHUNK 16 x 16\r\n") == "+OK\r\n");
    auto table = catalog->Find("cached");
    auto feed = table->SubscribeFeed();
    auto lease = table->Acquire();
    auto& store = lease->store();
    std::array<std::vector<ColumnValue>, 256U> previous;
    std::shared_ptr<const FeedEntry> first;
    for (unsigned phase = 0U; phase < 2U; ++phase) {
        auto state = store.ReadChunkState(0, 0);
        const auto version = state.version;
        for (unsigned b = 0U; b < 256U; ++b) {
            ColumnValue nullable = std::monostate{};
            if ((b / 16U + phase) % 2U != 0U) nullable = std::numeric_limits<std::uint64_t>::max() - phase;
            const auto nan = std::bit_cast<double>((b / 16U + phase) % 2U != 0U ?
                std::uint64_t{0x7ff8000000000001} : std::uint64_t{0xfff8000000000001});
            store.SetBlockInState(state, b % 16U, b / 16U, {{"n", nullable}, {"i", std::int64_t{-23} + phase},
                {"f", phase == 0U ? float{-0.0} : float{0.0}}, {"g", nan}, {"b", BitsValue{std::string(16U, phase == 0U ? '0' : '1')}}});
        }
        assert(store.WriteChunkState(0, 0, std::move(state), version).ok);
        const auto entry = Next(*feed);
        assert(entry->blocks.size() == 256U);
        for (unsigned b = 0U; b < 256U; ++b) {
            const auto& block = entry->blocks[b];
            const auto current = store.GetBlock(b % 16U, b / 16U);
            assert(current && block.after && SameFeedValues(*block.after, *current));
            if (phase == 0U) assert(!block.before);
            else {
                assert(block.before && SameFeedValues(*block.before, previous[b]));
                assert(SameFeedValues(*first->blocks[b].after, previous[b]));
            }
            previous[b] = *current;
        }
        if (phase == 0U) first = entry;
    }
}

void RepeatedWireRows() {
    const std::vector<ColumnValue> row{std::numeric_limits<std::uint64_t>::max(), std::int64_t{-1},
        float{-0.0}, double{-0.0}, true, BitsValue{"101010101"}, std::string("repeated\r\ntext"),
        BytesValue{{0, 13, 10, 255}}, std::monostate{}};
    auto positive_zero = row;
    positive_zero[2] = float{0.0};
    assert(!SameFeedValues(row, positive_zero));
    auto nan = row;
    nan[3] = std::bit_cast<double>(std::uint64_t{0x7ff8000000000001});
    auto other_nan = nan;
    other_nan[3] = std::bit_cast<double>(std::uint64_t{0xfff8000000000001});
    assert(SameFeedValues(nan, nan) && !SameFeedValues(nan, other_nan));
    FeedEntry entry;
    entry.blocks.reserve(256U);
    for (unsigned i = 0U; i < 256U; ++i) {
        auto after = i / 16U % 4U == 0U ? row : i / 16U % 4U == 1U ? positive_zero :
                     i / 16U % 4U == 2U ? nan : other_nan;
        std::optional<std::vector<ColumnValue>> before;
        if (i % 5U != 0U) before = row;
        if (i % 17U == 0U) before = std::vector<ColumnValue>{};
        entry.blocks.push_back({{std::numeric_limits<std::int64_t>::max(), std::numeric_limits<std::int64_t>::min()},
            i < 128U ? std::optional<std::int64_t>{i % 16U} : std::nullopt,
            i < 128U ? std::optional<std::int64_t>{i / 16U} : std::nullopt,
            std::move(before), std::move(after), i % 16U, i / 16U});
    }
    // The regular GET BLOCK serializer is an independent reference for each row.
    std::string expected = ">7\r\n";
    Protocol::AppendBulk(expected, "change");
    Protocol::AppendBulk(expected, std::string(32U, '0'));
    Protocol::AppendInteger(expected, std::uint64_t{0});
    Protocol::AppendInteger(expected, std::uint64_t{0});
    Protocol::AppendNull(expected);
    Protocol::AppendInteger(expected, std::uint64_t{0});
    Protocol::AppendArrayHeader(expected, entry.blocks.size());
    for (const auto& block : entry.blocks) {
        Protocol::AppendArrayHeader(expected, 4U);
        const auto coordinate = [&](std::optional<std::int64_t> absolute, std::int64_t chunk, std::uint32_t local) {
            if (absolute) Protocol::AppendInteger(expected, *absolute);
            else {
                Protocol::AppendArrayHeader(expected, 2U);
                Protocol::AppendInteger(expected, chunk);
                Protocol::AppendInteger(expected, std::uint64_t{local});
            }
        };
        coordinate(block.x, block.chunk.x, block.local_x);
        coordinate(block.y, block.chunk.y, block.local_y);
        for (const auto* values : {&block.before, &block.after}) {
            if (!*values) Protocol::AppendNull(expected);
            else {
                Protocol::AppendArrayHeader(expected, (*values)->size());
                for (const auto& value : **values) Protocol::AppendValue(expected, value);
            }
        }
    }
    assert(EncodeFeedEntry(entry) == expected);
}

void SchemaAndVars() {
    ScopedTempDir dir("chunkdb-feed-schema");
    auto catalog = std::make_shared<TableCatalog>(Config(dir.path()));
    CommandEngine engine({.require_auth = false}, catalog);
    SessionState session;
    assert(engine.Execute(session, "HELLO 3\r\n").front() == '%');
    assert(engine.Execute(session, "CREATE TABLE t (n u16, note text(64) NULL, data bytes(64)) CHUNK 2 x 2\r\n") == "+OK\r\n");
    auto table = catalog->Find("t");
    auto feed = table->SubscribeFeed();
    {
        auto lease = table->Acquire();
        lease->store().SetBlock(0, 0, {{"n", std::uint64_t{7}}, {"note", std::string("old")}, {"data", BytesValue{{1, 0, 2}}}});
    }
    auto first = Next(*feed);
    assert(first->blocks.size() == 1 && !first->blocks[0].before);
    assert(first->blocks[0].after == (std::vector<ColumnValue>{std::uint64_t{7}, std::string("old"), BytesValue{{1, 0, 2}}}));
    {
        auto lease = table->Acquire();
        lease->store().SetBlock(0, 0, {{"note", std::monostate{}}});
    }
    auto second = Next(*feed);
    assert(second->blocks[0].before == first->blocks[0].after);
    assert(second->blocks[0].after == (std::vector<ColumnValue>{std::uint64_t{7}, std::monostate{}, BytesValue{{1, 0, 2}}}));
    {
        auto lease = table->Acquire();
        auto& store = lease->store();
        auto snapshot = store.BeginTxnSnapshot(txn_test::kTxnDuration);
        auto a = store.ReadChunkStateAt(*snapshot, 0, 0);
        auto b = store.ReadChunkStateAt(*snapshot, 1, 0);
        store.SetBlockInState(a, 0, 0, {{"note", std::string("txn")}});
        store.SetBlockInState(b, 2, 0, {{"n", std::uint64_t{8}}, {"note", std::string("whole")}});
        (void)store.CommitTransaction(*snapshot, {}, {{{0, 0}, std::move(a)}, {{1, 0}, std::move(b)}});
    }
    auto transaction = Next(*feed);
    assert(transaction->blocks.size() == 2 && transaction->blocks[0].before == second->blocks[0].after);
    assert(std::get<std::string>((*transaction->blocks[0].after)[1]) == "txn");
    assert(std::get<std::string>((*transaction->blocks[1].after)[1]) == "whole");
    catalog->ChangeColumns("t", [](const auto& schema) {
        return AddColumn(schema, Column{.name = "extra", .type = {ColumnKind::kUnsigned, 8}, .nullable = true, .default_value = {}});
    });
    auto schema = Next(*feed);
    assert(schema->kind == FeedEntry::Kind::kSchema && schema->schema_version == 2U);
    assert(schema->position.revision > transaction->position.revision && schema->columns.size() == 4);
    assert(schema->columns.back().name == "extra");
    {
        auto lease = table->Acquire();
        lease->store().SetBlock(0, 0, {{"extra", std::uint64_t{17}}});
    }
    auto change = Next(*feed);
    assert(change->kind == FeedEntry::Kind::kChange && change->schema_version == 2U);
    assert(change->position.revision > schema->position.revision);
    assert(change->blocks[0].before->size() == 4U && std::holds_alternative<std::monostate>(change->blocks[0].before->back()));
    assert(std::get<std::uint64_t>(change->blocks[0].after->back()) == 17U);
    // An options-only reopen keeps the feed without a schema entry.
    TableOptionsUpdate options;
    options.wal_group_commit_updates = 2U;
    catalog->SetOptions("t", options);
    {
        auto lease = table->Acquire();
        lease->store().SetBlock(0, 0, {{"n", std::uint64_t{9}}});
    }
    assert(Next(*feed)->kind == FeedEntry::Kind::kChange);
    catalog->ChangeColumns("t", [](const auto& current) { return RenameColumn(current, "note", "text"); });
    auto renamed = Next(*feed);
    assert(renamed->kind == FeedEntry::Kind::kSchema && renamed->schema_version == 3U && renamed->columns[1].name == "text");
    catalog->Drop("t");
    assert(Next(*feed)->kind == FeedEntry::Kind::kEnd);
    assert(!feed->Next() && !table->Acquire());
}

void WritingUser(bool authenticated) {
    ScopedTempDir dir("chunkdb-feed-user");
    auto catalog = std::make_shared<TableCatalog>(Config(dir.path()));
    std::shared_ptr<UserRegistry> users;
    if (authenticated) users = std::make_shared<UserRegistry>(dir.path(),
        std::make_pair(std::string("admin"), scram::MakeVerifier("secret", crypto::RandomBytes(16), scram::kMinIterations)),
        std::array<std::uint8_t, 32>{7});
    CommandEngine engine({.require_auth = authenticated, .users = users}, catalog);
    SessionState session;
    if (authenticated) {
        const auto login = scram::StartClientLogin("admin", scram::NewNonce());
        const std::vector<std::optional<std::string>> params{login.first};
        const auto first = engine.Execute(session, "HELLO 3 USER admin $1\r\n", params);
        assert(first.rfind("+SCRAM ", 0) == 0);
        const auto finish = scram::FinishClientLogin(login, "secret", first.substr(7, first.size() - 9));
        const std::vector<std::optional<std::string>> final{finish.message};
        assert(engine.Execute(session, "AUTH $1\r\n", final).front() == '%');
    } else assert(engine.Execute(session, "HELLO 3\r\n").front() == '%');
    auto feed = catalog->Find("default")->SubscribeFeed();
    const auto write = "SET BLOCK 0 0 IN default bits = b'" + Bits(1) + "'\r\n";
    assert(engine.Execute(session, write).front() != '-');
    auto entry = Next(*feed);
    assert(entry->user == (authenticated ? std::optional<std::string>("admin") : std::nullopt));
    assert(engine.Execute(session, "BEGIN\r\n") == "+OK\r\n");
    assert(engine.Execute(session, "SET BLOCK 4 0 IN default bits = b'" + Bits(2) + "'\r\n").front() != '-');
    assert(engine.Execute(session, "COMMIT\r\n").front() != '-');
    assert(Next(*feed)->user == entry->user);
    // The engine restores the surrounding C++ identity when it returns.
    {
        auto lease = catalog->Find("default")->Acquire();
        lease->store().SetBlockBits(8, 0, Bits(3));
    }
    assert(!Next(*feed)->user);
}

void PositionsAndLag() {
    ScopedTempDir dir("chunkdb-feed-lag");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto prototype = table->SubscribeFeed();
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, Bits(1));
    }
    auto old = Next(*prototype);
    prototype.reset();
    auto active = table->SubscribeFeed({.buffer_bytes = 4096U});
    auto slow = table->SubscribeFeed();
    const auto start = active->position();
    for (std::uint32_t i = 3; i < 25; ++i) {
        {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(0, 0, Bits(i));
        }
        auto entry = Next(*active);
        assert(entry->kind == FeedEntry::Kind::kChange);
    }
    auto resync = Next(*slow);
    assert(resync->kind == FeedEntry::Kind::kResync && resync->position == active->position());
    auto resumed = table->SubscribeFeed({.after = active->position()});
    assert(!resumed->Next());
    auto stale = table->SubscribeFeed({.after = old->position});
    assert(Next(*stale)->kind == FeedEntry::Kind::kResync);
    auto wrong = start;
    wrong.epoch[0] ^= 1U;
    auto epoch = table->SubscribeFeed({.after = wrong});
    assert(Next(*epoch)->kind == FeedEntry::Kind::kResync);
    auto future = table->SubscribeFeed({.after = FeedPosition{table->store_id(), std::numeric_limits<std::uint64_t>::max()}});
    assert(Next(*future)->kind == FeedEntry::Kind::kResync);
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, Bits(25));
    }
    assert(Next(*slow)->position == Next(*active)->position);
    assert(Next(*resumed)->kind == FeedEntry::Kind::kChange);
}

void ToggleUnderLoad() {
    ScopedTempDir dir("chunkdb-feed-toggle");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    std::atomic<bool> stop{false};
    std::atomic<unsigned> ready{0};
    std::vector<std::thread> writers;
    for (unsigned t = 0; t < 3; ++t) writers.emplace_back([&, t] {
        ready.fetch_add(1);
        ready.notify_all();
        std::uint32_t value = 0;
        while (!stop.load()) {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(static_cast<std::int64_t>(t) * 4, 0, Bits(++value));
        }
    });
    auto seen = ready.load();
    while (seen < 3U) { ready.wait(seen); seen = ready.load(); }
    for (unsigned i = 0; i < 12; ++i) {
        auto subscription = table->SubscribeFeed();
        const auto start = subscription->position();
        auto first = Next(*subscription);
        assert(first->kind == FeedEntry::Kind::kChange && first->position.revision > start.revision);
        table->StopFeed();
        assert(Next(*subscription)->kind == FeedEntry::Kind::kEnd);
    }
    stop.store(true);
    for (auto& thread : writers) thread.join();
    // No subscriber leaves no feed pointer or sender behind.
    { auto last = table->SubscribeFeed(); }
    auto after = table->SubscribeFeed();
    assert(!after->Next());
}

void FailedSchemaPublicationEndsFeedWithoutBlockingTable() {
    ScopedTempDir dir("chunkdb-feed-schema-failure");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto feed = table->SubscribeFeed();
    {
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_VERSION_RESERVE_FAIL_ONCE", "1");
        catalog.ChangeColumns("default", [](const auto& schema) {
            return AddColumn(schema, Column{.name = "extra", .type = {ColumnKind::kUnsigned, 8}, .default_value = {}});
        });
    }
    assert(table->Info().schema.version == 2U);
    assert(txn_test::ThrowsAs<std::runtime_error>([&] { (void)feed->Next(); }));
    {
        auto lease = table->Acquire();
        lease->store().SetBlock(0, 0, {{"bits", BitsValue{Bits(1)}}, {"extra", std::uint64_t{1}}});
        assert(std::get<std::uint64_t>(lease->store().GetBlock(0, 0)->back()) == 1U);
    }
    feed.reset();
    auto retry = table->SubscribeFeed();
    {
        auto lease = table->Acquire();
        lease->store().SetBlock(0, 0, {{"extra", std::uint64_t{2}}});
    }
    const auto event = Next(*retry);
    assert(event->schema_version == 2U && std::get<std::uint64_t>(event->blocks[0].before->back()) == 1U);
}

void UnchangedNaNIsNotAnotherBlockChange() {
    ScopedTempDir dir("chunkdb-feed-nan");
    auto catalog = std::make_shared<TableCatalog>(Config(dir.path()));
    CommandEngine engine({.require_auth = false}, catalog);
    SessionState session;
    (void)engine.Execute(session, "HELLO 3\r\n");
    assert(engine.Execute(session, "CREATE TABLE floats (f f32, n u8) CHUNK 4 x 4\r\n") == "+OK\r\n");
    auto table = catalog->Find("floats");
    {
        auto lease = table->Acquire();
        for (std::int64_t x = 0; x < 2; ++x)
            lease->store().SetBlock(x, 0, {{"f", std::numeric_limits<float>::quiet_NaN()}, {"n", std::uint64_t{1}}});
    }
    auto feed = table->SubscribeFeed();
    {
        auto lease = table->Acquire();
        lease->store().SetBlock(0, 0, {{"n", std::uint64_t{2}}});
    }
    const auto entry = Next(*feed);
    assert(entry->blocks.size() == 1U && entry->blocks[0].x == 0);
    assert(std::isnan(std::get<float>(entry->blocks[0].before->front())));
    assert(std::isnan(std::get<float>(entry->blocks[0].after->front())));
}

void DecodedEntryOverBudget() {
    ScopedTempDir dir("chunkdb-feed-large-change");
    auto config = Config(dir.path());
    auto geometry = txn_test::Config({}).geometry;
    geometry.chunk_width_blocks = 8;
    geometry.chunk_height_blocks = 8;
    geometry.block_bits = 1;
    TableCatalog catalog(config);
    (void)test::CreateBitsTable(catalog, geometry);
    auto table = catalog.Find("default");
    auto feed = table->SubscribeFeed({.buffer_bytes = 2048U});
    std::uint64_t revision = 0;
    {
        auto lease = table->Acquire();
        auto state = lease->store().ReadChunkState(0, 0);
        std::fill(state.payload.begin(), state.payload.end(), 0xffU);
        std::fill(state.presence_bitmap.begin(), state.presence_bitmap.end(), 0xffU);
        revision = lease->store().SetChunkStateBytes(0, 0, state.payload, state.presence_bitmap);
    }
    const auto event = Next(*feed);
    assert(event->kind == FeedEntry::Kind::kResync && event->position.revision == revision);
    assert(FeedTestAccess::BufferedBytes(*table) <= 2048U);
}

void ExtremeBlockCoordinates() {
    ScopedTempDir dir("chunkdb-feed-coordinates");
    auto config = Config(dir.path());
    auto geometry = txn_test::Config({}).geometry;
    geometry.chunk_width_blocks = 3;
    geometry.chunk_height_blocks = 3;
    TableCatalog catalog(config);
    (void)test::CreateBitsTable(catalog, geometry);
    auto table = catalog.Find("default");
    auto feed = table->SubscribeFeed();
    for (const auto coordinate : {std::numeric_limits<std::int64_t>::min(), std::numeric_limits<std::int64_t>::max()}) {
        {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(coordinate, coordinate, Bits(1));
        }
        const auto change = Next(*feed);
        assert(change->blocks.size() == 1U && change->blocks[0].x == coordinate && change->blocks[0].y == coordinate);
    }
}

void ChunkCoordinatesBeyondAbsoluteBlockDomain() {
    ScopedTempDir dir("chunkdb-feed-chunk-coordinates");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    const ChunkCoord coordinate{std::numeric_limits<std::int64_t>::max(), std::numeric_limits<std::int64_t>::min()};
    auto feed = table->SubscribeFeed();
    auto inside = table->SubscribeFeed({.area = FeedArea{coordinate, coordinate}});
    auto outside = table->SubscribeFeed({.area = FeedArea{{0, 0}, {0, 0}}});
    {
        auto lease = table->Acquire();
        auto state = lease->store().ReadChunkState(coordinate.x, coordinate.y);
        std::fill(state.payload.begin(), state.payload.end(), 0x5aU);
        std::fill(state.presence_bitmap.begin(), state.presence_bitmap.end(), 0xffU);
        (void)lease->store().SetChunkStateBytes(coordinate.x, coordinate.y, state.payload, state.presence_bitmap);
    }
    const auto entry = Next(*feed);
    assert(entry->kind == FeedEntry::Kind::kChange && entry->blocks.size() == 16U);
    for (std::size_t b = 0; b < entry->blocks.size(); ++b) {
        const auto& block = entry->blocks[b];
        assert(block.chunk == coordinate && !block.x && !block.y);
        assert(block.local_x == b % 4U && block.local_y == b / 4U && block.after);
    }
    EqualBlocks(entry->blocks, Next(*inside)->blocks);
    assert(!outside->Next());
    assert(outside->position() == entry->position);
}

void AbsentBlockBytesAreCanonicalizedBeforeCommit() {
    ScopedTempDir dir("chunkdb-feed-empty-rows");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto feed = table->SubscribeFeed();
    auto area = table->SubscribeFeed({.area = FeedArea{{0, 0}, {0, 0}}});
    {
        auto lease = table->Acquire();
        auto state = lease->store().ReadChunkState(0, 0);
        state.payload[0] = 1U;
        // The store clears absent block bytes, so this is a no-op.
        assert(lease->store().SetChunkStateBytes(0, 0, state.payload, state.presence_bitmap) == state.version);
    }
    assert(!feed->Next() && !area->Next());
}

void RefuseUnsupported() {
    ScopedTempDir dir("chunkdb-feed-refuse");
    { TableCatalog writable(Config(dir.path())); (void)CreateDefault(writable); }
    {
        auto config = Config(dir.path());
        config.access_mode = AccessMode::kReadOnly;
        TableCatalog readonly(config);
        assert(txn_test::ThrowsAs<std::invalid_argument>([&] { (void)readonly.Find("default")->SubscribeFeed(); }));
        assert(readonly.Find("default")->Acquire());
    }
    {
        auto config = Config(dir.path());
        config.allow_multiple_processes = true;
        TableCatalog multi(config);
        assert(txn_test::ThrowsAs<std::invalid_argument>([&] { (void)multi.Find("default")->SubscribeFeed(); }));
    }
}
}  // namespace
int main() {
    RowDecoderScratch();
    WireNumberLimits();
    PackedRows();
    RepeatedFixedRows();
    RepeatedWireRows();
    SchemaAndVars();
    WritingUser(false);
    WritingUser(true);
    PositionsAndLag();
    ToggleUnderLoad();
    RefuseUnsupported();
    ExtremeBlockCoordinates();
    DecodedEntryOverBudget();
    UnchangedNaNIsNotAnotherBlockChange();
    FailedSchemaPublicationEndsFeedWithoutBlockingTable();
    ChunkCoordinatesBeyondAbsoluteBlockDomain();
    AbsentBlockBytesAreCanonicalizedBeforeCommit();
}
