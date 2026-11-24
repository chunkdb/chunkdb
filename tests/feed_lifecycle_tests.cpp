#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <cmath>
#include <future>
#include <limits>
#include <string_view>
#include <thread>

#include "crypto.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/protocol.hpp"
#include "scram.hpp"
#include "feed_test_utils.hpp"
#include "feed_protocol.hpp"
#include "feature_flags.hpp"
#include "wal_replay.hpp"
#include "wal_writer.hpp"
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
    const auto expected = std::string(">10\r\n$6\r\nchange\r\n$32\r\n") + std::string(32, '0') +
        "\r\n(18446744073709551615\r\n(18446744073709551615\r\n_\r\n(18446744073709551615\r\n"
        "*1\r\n*4\r\n:-9223372036854775808\r\n*2\r\n:9223372036854775807\r\n:4294967295\r\n"
        "_\r\n*2\r\n(18446744073709551615\r\n$2\r\n\x55\x01\r\n_\r\n_\r\n*0\r\n";
    assert(EncodeFeedEntry(entry) == expected);
    entry.kind = FeedEntry::Kind::kChunks;
    entry.chunk_forms.push_back({{std::numeric_limits<std::int64_t>::min(), std::numeric_limits<std::int64_t>::max()}, entry.position.revision, {}});
    const auto chunks = std::string(">9\r\n$6\r\nchunks\r\n$32\r\n") + std::string(32, '0') +
        "\r\n(18446744073709551615\r\n(18446744073709551615\r\n_\r\n(18446744073709551615\r\n"
        "*1\r\n*4\r\n:-9223372036854775808\r\n:9223372036854775807\r\n(18446744073709551615\r\n_\r\n_\r\n_\r\n";
    assert(EncodeFeedEntry(entry) == chunks);
    entry.kind = FeedEntry::Kind::kProgress;
    assert(EncodeFeedEntry(entry) == std::string(">3\r\n$8\r\nprogress\r\n$32\r\n") +
        std::string(32, '0') + "\r\n(18446744073709551615\r\n");
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
        expected += "_\r\n_\r\n*0\r\n";
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
    std::string expected = ">10\r\n";
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
    expected += "_\r\n_\r\n*0\r\n";
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
    (void)CreateDefault(*catalog);
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

std::uint64_t VersionReply(const std::string& reply) {
    assert(reply.size() > 3U && reply.front() == ':');
    return std::stoull(reply.substr(1U));
}

// A message rides the commit of its write or transaction, with that commit's
// data (docs/design/MESSAGES.md).
void MessagesRideTheirCommit() {
    ScopedTempDir dir("chunkdb-feed-messages");
    auto catalog = std::make_shared<TableCatalog>(Config(dir.path()));
    CommandEngine engine({.require_auth = false}, catalog);
    SessionState session;
    (void)engine.Execute(session, "HELLO 3\r\n");
    assert(engine.Execute(session, "CREATE TABLE notes (kind u8) CHUNK 2 x 2\r\n") == "+OK\r\n");
    auto feed = catalog->Find("notes")->SubscribeFeed();

    const auto first = VersionReply(engine.Execute(session, "SET BLOCK 0 0 IN notes kind = 1 PUBLISH 'orders.paid' 'order 7'\r\n"));
    auto entry = Next(*feed);
    assert(entry->position.revision == first && entry->blocks.size() == 1U);
    assert(entry->messages == (std::vector<FeedMessage>{{"orders.paid", "order 7"}}));
    assert(entry->protocol_frame->starts_with(">11\r\n$6\r\nchange\r\n"));
    assert(entry->protocol_frame->ends_with("*1\r\n*2\r\n$11\r\norders.paid\r\n$7\r\norder 7\r\n"));

    // A write that publishes commits even when it changes nothing.
    const auto second = VersionReply(engine.Execute(session, "SET BLOCK 0 0 IN notes kind = 1 PUBLISH 'orders' x'00ff'\r\n"));
    assert(second > first);
    entry = Next(*feed);
    assert(entry->position.revision == second && entry->blocks.empty());
    assert(entry->messages == (std::vector<FeedMessage>{{"orders", std::string("\0\xff", 2U)}}));
    // Without a message it commits nothing; the next change is the DELETE.
    assert(VersionReply(engine.Execute(session, "SET BLOCK 0 0 IN notes kind = 1\r\n")) == second);
    const std::vector<std::optional<std::string>> payload{std::string("\0\1", 2U)};
    const auto third = VersionReply(engine.Execute(session, "DELETE BLOCK 0 0 FROM notes PUBLISH 'orders' $1\r\n", payload));
    entry = Next(*feed);
    assert(entry->position.revision == third && entry->blocks.size() == 1U && !entry->blocks[0].after);
    assert(entry->messages.size() == 1U && entry->messages[0].payload == *payload[0]);
    const std::vector<std::optional<std::string>> null{std::nullopt};
    assert(engine.Execute(session, "SET BLOCK 1 1 IN notes kind = 4 PUBLISH 'orders' $1\r\n", null).starts_with("-ERR INVALID_ARGUMENT"));
    assert(engine.Execute(session, "SET BLOCK 1 1 IN notes kind = 4 IF kind = 9 PUBLISH 'orders' 'no'\r\n").starts_with("-ERR CONDITION_FAILED"));
    const auto conditional = VersionReply(engine.Execute(session, "SET BLOCK 1 1 IN notes kind = 4 IF kind IS NULL PUBLISH 'orders' 'expr'\r\n"));
    entry = Next(*feed);
    assert(entry->position.revision == conditional && entry->messages.size() == 1U && entry->messages[0].payload == "expr");

    // One transaction is one change with its message, from any statement.
    assert(engine.Execute(session, "BEGIN\r\n") == "+OK\r\n");
    assert(engine.Execute(session, "SET BLOCK 0 0 IN notes kind = 2\r\n") == "_\r\n");
    assert(engine.Execute(session, "SET BLOCK 2 2 IN notes kind = 3 PUBLISH 'orders' 'both'\r\n") == "_\r\n");
    assert(engine.Execute(session, "SET BLOCK 3 3 IN notes kind = 3 PUBLISH 'orders' 'again'\r\n").starts_with("-ERR INVALID_ARGUMENT"));
    const auto committed = VersionReply(engine.Execute(session, "COMMIT\r\n"));
    entry = Next(*feed);
    assert(entry->position.revision == committed && entry->blocks.size() == 2U);
    assert(entry->messages == (std::vector<FeedMessage>{{"orders", "both"}}));
    // A transaction that publishes commits even when it changes nothing.
    assert(engine.Execute(session, "BEGIN\r\n") == "+OK\r\n");
    assert(engine.Execute(session, "SET BLOCK 2 2 IN notes kind = 3 PUBLISH 'audit' 'same'\r\n") == "_\r\n");
    const auto unchanged = VersionReply(engine.Execute(session, "COMMIT\r\n"));
    entry = Next(*feed);
    assert(entry->position.revision == unchanged && entry->blocks.empty());
    assert(entry->messages == (std::vector<FeedMessage>{{"audit", "same"}}));
}

// Only AREA drops a change, with its messages; the wire form adds the
// messages element only to a change that carries one.
void MessagesFollowAreaFilters() {
    auto entry = std::make_shared<FeedEntry>();
    entry->messages.push_back({"orders", std::string("\0\r\n", 3U)});
    const std::vector<FeedArea> elsewhere{FeedArea{.first = {5, 5, 0}, .last = {6, 6, 0}, .dims = 2U}};
    assert(!FilterFeedEntry(entry, elsewhere, std::nullopt, false, {}));
    const auto kept = FilterFeedEntry(entry, {}, std::nullopt, true, {});
    assert(kept && kept->messages == entry->messages);
    const auto tail = std::string("_\r\n_\r\n*0\r\n*1\r\n*2\r\n$6\r\norders\r\n$3\r\n") + std::string("\0\r\n", 3U) + "\r\n";
    assert(kept->protocol_frame->starts_with(">11\r\n$6\r\nchange\r\n") && kept->protocol_frame->ends_with(tail));
    FeedEntry chunks = *entry;
    chunks.kind = FeedEntry::Kind::kChunks;
    const auto encoded = EncodeFeedEntry(chunks);
    assert(encoded.starts_with(">10\r\n$6\r\nchunks\r\n"));
    assert(encoded.ends_with(std::string("*0\r\n_\r\n_\r\n*1\r\n*2\r\n$6\r\norders\r\n$3\r\n") + std::string("\0\r\n", 3U) + "\r\n"));
}

// The MESSAGE TLV belongs to the `messages` feature and is bounded.
void MessageFrames() {
    ScopedTempDir dir("chunkdb-feed-message-frames");
    TableCatalog catalog(Config(dir.path()));
    const auto geometry = CreateDefault(catalog)->geometry();
    const FeedMessage message{"orders", std::string(kMaxFeedMessageBytes, 'm')};
    std::vector<std::uint8_t> frame;
    WalFrameBuilder builder(&frame, 1U, {}, std::nullopt, false, &message);
    const std::uint8_t presence = 0U;
    builder.AppendSpan(static_cast<std::uint32_t>(geometry.ChunkPayloadBytes()), &presence, 1U);
    (void)builder.Finish(3U, 30U);
    assert(InspectFeedFrame(frame, geometry, {.incompat = kFeatureMessages}).message == message);
    bool refused = false;
    try {
        (void)InspectFeedFrame(frame, geometry, {});
    } catch (const std::runtime_error& error) {
        refused = std::string_view(error.what()).find("tlv_message_without_feature") != std::string_view::npos;
    }
    assert(refused);
    for (const auto& invalid : {FeedMessage{"Orders", ""}, FeedMessage{"", "x"}, FeedMessage{"9th", ""},
             FeedMessage{std::string(kMaxFeedChannelBytes + 1U, 'c'), ""},
             FeedMessage{"orders", std::string(kMaxFeedMessageBytes + 1U, 'm')}}) {
        std::vector<std::uint8_t> bytes;
        bool threw = false;
        try {
            WalFrameBuilder invalid_builder(&bytes, 1U, {}, std::nullopt, false, &invalid);
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        assert(threw && bytes.empty());
    }
}

void PositionsAndLag() {
    ScopedTempDir dir("chunkdb-feed-lag");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    // Linger retains this ring after the last reader leaves, including its
    // budget. Choose the small budget before publishing the first entry.
    auto prototype = table->SubscribeFeed({.buffer_bytes = 4096U});
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, Bits(1));
    }
    auto old = Next(*prototype);
    const auto buffered = FeedTestAccess::BufferedBytes(*table);
    assert(buffered != 0U && buffered <= 4096U);
    prototype.reset();
    assert(FeedTestAccess::Capturing(*table));
    assert(FeedTestAccess::BufferedBytes(*table) == buffered);
    // A different budget cannot silently replace resumable idle history.
    assert(txn_test::ThrowsAs<std::invalid_argument>([&] {
        (void)table->SubscribeFeed({.buffer_bytes = 8192U});
    }));
    assert(FeedTestAccess::BufferedBytes(*table) == buffered);
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, Bits(2));
    }
    auto replay = table->SubscribeFeed({.after = old->position, .buffer_bytes = 4096U});
    const auto disconnected = Next(*replay);
    assert(disconnected->kind == FeedEntry::Kind::kChange);
    assert(std::get<BitsValue>(disconnected->blocks[0].before->front()).digits == Bits(1));
    assert(std::get<BitsValue>(disconnected->blocks[0].after->front()).digits == Bits(2));
    replay.reset();
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

void FailedSchemaPublicationEndsFeedWithoutBlockingTable(bool slot_backed = false) {
    ScopedTempDir dir("chunkdb-feed-schema-failure");
    auto config = Config(dir.path());
    // Keep the slot worker's durable sync outside this admission regression.
    config.slot_sync_interval = std::chrono::hours(1);
    TableCatalog catalog(config);
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    std::optional<FeedSlot> slot;
    if (slot_backed) slot = table->CreateFeedSlot("history");
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
    if (slot_backed) {
        // A slot still owns the failed ring. Closing the ordinary reader must
        // not recreate capture or bypass the slot worker's failure policy.
        assert(txn_test::ThrowsAs<std::runtime_error>([&] { (void)table->SubscribeFeed(); }));
        const auto slots = table->ListFeedSlots(true);
        assert(slots.size() == 1U && slots[0].name == slot->name && slots[0].position == slot->position);
        assert(!slots[0].lost && slots[0].durable_watermark == slot->durable_watermark);
        return;
    }
    // An errored ring cannot resume: release it immediately, even with linger
    // enabled, while retaining the healthy table and its successful writes.
    assert(!FeedTestAccess::Capturing(*table));
    auto retry = table->SubscribeFeed();
    assert(FeedTestAccess::Capturing(*table));
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
    auto inside = table->SubscribeFeed({.areas = {FeedArea{coordinate, coordinate}}});
    auto outside = table->SubscribeFeed({.areas = {FeedArea{{0, 0}, {0, 0}}}});
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
    auto area = table->SubscribeFeed({.areas = {FeedArea{{0, 0}, {0, 0}}}});
    {
        auto lease = table->Acquire();
        auto state = lease->store().ReadChunkState(0, 0);
        state.payload[0] = 1U;
        // The store clears absent block bytes, so this is a no-op.
        assert(lease->store().SetChunkStateBytes(0, 0, state.payload, state.presence_bitmap) == state.version);
    }
    assert(!feed->Next() && !area->Next());
}

// A chunk in any area is kept; watches with the same filters receive one
// projected entry and frame, which nothing keeps once all of them read it.
void SeveralAreasShareOneProjection() {
    ScopedTempDir dir("chunkdb-feed-areas");
    auto catalog = std::make_shared<TableCatalog>(Config(dir.path()));
    CommandEngine engine({.require_auth = false}, catalog);
    SessionState session;
    (void)engine.Execute(session, "HELLO 3\r\n");
    const auto run = [&](const std::string& statement) { assert(engine.Execute(session, statement + "\r\n")[0] != '-'); };
    run("CREATE TABLE t (n u8, m u8) CHUNK 2 x 2");
    auto table = catalog->Find("t");
    const FeedArea low{{0, 0}, {0, 0}}, high{{2, 2}, {2, 2}};
    const auto m = table->geometry().layout().schema().columns[1].id;
    auto first = table->SubscribeFeed({.areas = {low, high}});
    auto second = table->SubscribeFeed({.areas = {high, low, low}});
    auto projected = table->SubscribeFeed({.areas = {low, high}, .column_ids = std::vector<std::uint32_t>{m}});
    auto alone = table->SubscribeFeed({.areas = {low}});
    SessionState watching;
    (void)engine.Execute(watching, "HELLO 3\r\n");
    assert(engine.Execute(watching, "WATCH t AREA 2 2 TO 2 2, AREA 0 0 TO 0 0\r\n")[0] == '%');
    // A second WATCH leaves the open one in place.
    const auto* open = watching.watch.get();
    assert(engine.Execute(watching, "WATCH t\r\n").starts_with("-ERR INVALID_ARGUMENT"));
    assert(watching.watch.get() == open);
    run("BEGIN");
    run("SET BLOCK 0 0 IN t n = 1, m = 4");
    run("SET BLOCK 2 2 IN t n = 2, m = 5");
    run("SET BLOCK 4 4 IN t n = 3, m = 6");
    run("COMMIT");
    auto a = Next(*first);
    assert(a->blocks.size() == 2U && a->protocol_frame && *a->protocol_frame == EncodeFeedEntry(*a));
    for (const auto& block : a->blocks) assert(block.chunk == low.first || block.chunk == high.first);
    auto b = Next(*second);
    assert(b == a);
    assert(Next(*watching.watch) == a);
    const auto c = Next(*projected);
    assert(c != a && c->blocks.size() == 2U && c->blocks[0].after->size() == 1U && *c->protocol_frame == EncodeFeedEntry(*c));
    const auto d = Next(*alone);
    assert(d->blocks.size() == 1U && d->blocks[0].chunk == low.first);
    const std::weak_ptr<const FeedEntry> shared = a;
    a.reset();
    b.reset();
    assert(shared.expired());
    assert(txn_test::ThrowsAs<std::invalid_argument>([&] {
        (void)table->SubscribeFeed({.areas = std::vector<FeedArea>(kMaxFeedAreas + 1U, low)});
    }));
    assert(txn_test::ThrowsAs<std::invalid_argument>([&] { (void)table->SubscribeFeed({.areas = {low, FeedArea{{0, 0, 0}, {0, 0, 0}, 3U}}}); }));
}

// A projection group keeps an item only while another member must still read
// it, within its share of the feed budget and above the ring floor; an item
// that is the ring entry itself does not outlive the ring.
void ProjectionGroupBounds() {
    ScopedTempDir dir("chunkdb-feed-group-bounds");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    // Ungrouped; each read waits until the sender has placed that write in the ring.
    auto plain = table->SubscribeFeed({.buffer_bytes = 4096U});
    std::uint32_t value = 0U;
    const auto write = [&] {
        { auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, Bits(++value)); }
        assert(Next(*plain)->kind == FeedEntry::Kind::kChange);
    };
    const auto evict = [&] { for (int i = 0; i < 64; ++i) write(); };
    // NO BEFORE copies every change, so each read either shares or projects its own.
    auto fast = table->SubscribeFeed({.no_before = true});
    auto slow = table->SubscribeFeed({.no_before = true});
    std::vector<std::unique_ptr<FeedSubscription>> others;
    for (std::int64_t i = 1; i < 64; ++i) others.push_back(table->SubscribeFeed({.areas = {FeedArea{{i, i}, {i, i}}}}));
    // 64 groups leave each a share of 64 bytes, less than one projection.
    write();
    auto a = Next(*fast);
    const std::weak_ptr<const FeedEntry> unshared = a;
    a.reset();
    assert(unshared.expired());
    others.clear();
    // The last member to read a change keeps nothing for the others.
    auto b = Next(*slow);
    assert(b->kind == FeedEntry::Kind::kChange && b->position == fast->position());
    const std::weak_ptr<const FeedEntry> last = b;
    b.reset();
    assert(last.expired());
    write();
    a = Next(*fast);
    const std::weak_ptr<const FeedEntry> shared = a;
    a.reset();
    assert(Next(*slow) == shared.lock());
    assert(shared.expired());
    // The ring floor passes an item that the slow member never read.
    write();
    a = Next(*fast);
    const std::weak_ptr<const FeedEntry> held = a;
    a.reset();
    assert(!held.expired());
    evict();
    assert(Next(*fast)->kind == FeedEntry::Kind::kResync && !held.expired());
    write();
    assert(Next(*fast)->kind == FeedEntry::Kind::kChange && held.expired());
    assert(Next(*slow)->kind == FeedEntry::Kind::kResync);
    // An area that holds every change shares the ring entry without holding it.
    auto reader = table->SubscribeFeed({.areas = {FeedArea{{0, 0}, {0, 0}}}});
    auto lagging = table->SubscribeFeed({.areas = {FeedArea{{0, 0}, {0, 0}}}});
    { auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, Bits(++value)); }
    auto change = Next(*plain);
    assert(Next(*reader) == change);
    const std::weak_ptr<const FeedEntry> ring_entry = change;
    change.reset();
    evict();
    assert(ring_entry.expired());
    assert(Next(*lagging)->kind == FeedEntry::Kind::kResync);
}

// A new projection group shrinks every share, so the groups together keep at
// most the feed budget, and nothing the ring floor passed stays.
void ProjectionGroupsShareTheBudget() {
    ScopedTempDir dir("chunkdb-feed-group-budget");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto plain = table->SubscribeFeed({.buffer_bytes = 4096U});
    std::uint32_t value = 0U;
    const auto write = [&] {
        { auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, Bits(++value)); }
        assert(Next(*plain)->kind == FeedEntry::Kind::kChange);
    };
    auto fast = table->SubscribeFeed({.no_before = true});
    auto slow = table->SubscribeFeed({.no_before = true});
    // Alone, the group may keep the whole budget for the slow member.
    write();
    auto a = Next(*fast);
    const std::weak_ptr<const FeedEntry> alone = a;
    a.reset();
    assert(!alone.expired());
    // 63 more groups leave it 64 bytes, less than one projection.
    std::vector<std::unique_ptr<FeedSubscription>> others;
    for (std::int64_t i = 1; i < 64; ++i) others.push_back(table->SubscribeFeed({.areas = {FeedArea{{i, i}, {i, i}}}}));
    assert(alone.expired());
    others.clear();
    // An item that still fits the share goes once the ring floor has passed it.
    write();
    a = Next(*fast);
    const std::weak_ptr<const FeedEntry> passed = a;
    a.reset();
    for (int i = 0; i < 64; ++i) write();
    assert(!passed.expired());
    auto other = table->SubscribeFeed({.areas = {FeedArea{{1, 1}, {1, 1}}}});
    assert(passed.expired());
}

// Records whether `entry` was freed while its feed's ring mutex was held:
// 0 not yet freed, 1 freed outside it, 2 freed under it.
std::shared_ptr<std::atomic<int>> ObserveFree(Table& table, const std::shared_ptr<const FeedEntry>& entry) {
    auto state = std::make_shared<std::atomic<int>>(0);
    auto& ring = FeedTestAccess::RingMutex(table);
    // The projection was built mutable; only this test changes it.
    auto& target = const_cast<FeedEntry&>(*entry);
    target.protocol_frame = std::shared_ptr<const std::string>(new std::string(*entry->protocol_frame),
        [&ring, state](const std::string* frame) {
            delete frame;
            // The freeing thread may hold the mutex, so another one tests it.
            const bool unlocked = std::async(std::launch::async, [&ring] {
                const auto until = std::chrono::steady_clock::now() + 2s;
                do {
                    if (ring.try_lock()) { ring.unlock(); return true; }
                    std::this_thread::yield();
                } while (std::chrono::steady_clock::now() < until);
                return false;
            }).get();
            state->store(unlocked ? 1 : 2);
        });
    return state;
}

// Readers free the group items they prune after releasing the ring mutex,
// which writers take at a full ring.
void ProjectionGroupFreesOutsideRing() {
    ScopedTempDir dir("chunkdb-feed-group-free");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto plain = table->SubscribeFeed({.buffer_bytes = 4096U});
    std::uint32_t value = 0U;
    const auto write = [&] {
        { auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, Bits(++value)); }
        assert(Next(*plain)->kind == FeedEntry::Kind::kChange);
    };
    auto fast = table->SubscribeFeed({.no_before = true});
    auto slow = table->SubscribeFeed({.no_before = true});
    write();
    auto a = Next(*fast);
    const auto floor_pruned = ObserveFree(*table, a);
    a.reset();
    for (int i = 0; i < 64; ++i) write();
    assert(Next(*fast)->kind == FeedEntry::Kind::kResync && floor_pruned->load() == 0);
    // The next read finds its entry after pruning the item the floor passed.
    write();
    a = Next(*fast);
    assert(a->kind == FeedEntry::Kind::kChange && floor_pruned->load() == 1);
    const auto read_past = ObserveFree(*table, a);
    a.reset();
    // The slow member's resync moves it past the item fast kept for it.
    assert(Next(*slow)->kind == FeedEntry::Kind::kResync && read_past->load() == 1);
    // So does an explicit resync.
    write();
    a = Next(*fast);
    const auto skipped = ObserveFree(*table, a);
    a.reset();
    (void)slow->Resync();
    assert(skipped->load() == 1);
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
int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--failed-schema-publication") {
        FailedSchemaPublicationEndsFeedWithoutBlockingTable();
        FailedSchemaPublicationEndsFeedWithoutBlockingTable(true);
        return 0;
    }
    assert(argc == 1);
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
    MessagesRideTheirCommit();
    MessagesFollowAreaFilters();
    MessageFrames();
    FailedSchemaPublicationEndsFeedWithoutBlockingTable();
    FailedSchemaPublicationEndsFeedWithoutBlockingTable(true);
    ChunkCoordinatesBeyondAbsoluteBlockDomain();
    AbsentBlockBytesAreCanonicalizedBeforeCommit();
    SeveralAreasShareOneProjection();
    ProjectionGroupBounds();
    ProjectionGroupsShareTheBudget();
    ProjectionGroupFreesOutsideRing();
}
