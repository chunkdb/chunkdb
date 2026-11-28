// CatalogFeed (WATCH CATALOG): whole-catalog pushes, changes by scope,
// visibility without MANAGES USERS, history overflow and failure.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <cstddef>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "catalog_feed.hpp"
#include "change_feed.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "statement_control.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;

struct Resp {
    char type = 0;
    std::string value;
    std::vector<Resp> items;
};

Resp Parse(const std::string& bytes, std::size_t& at) {
    const auto end = bytes.find("\r\n", at);
    assert(end != std::string::npos);
    Resp reply{bytes[at], bytes.substr(at + 1U, end - at - 1U), {}};
    at = end + 2U;
    if (reply.type == '$') {
        const auto size = std::stoull(reply.value);
        reply.value = bytes.substr(at, size);
        at += size + 2U;
    } else if (reply.type == '*' || reply.type == '>' || reply.type == '%') {
        const auto count = std::stoull(reply.value) * (reply.type == '%' ? 2U : 1U);
        for (std::size_t i = 0; i < count; ++i) reply.items.push_back(Parse(bytes, at));
    }
    return reply;
}

Resp Parse(const std::string& bytes) {
    std::size_t at = 0U;
    auto reply = Parse(bytes, at);
    assert(at == bytes.size());
    return reply;
}

bool Has(const Resp& map, std::string_view key) {
    for (std::size_t i = 0; i < map.items.size(); i += 2U)
        if (map.items[i].value == key) return true;
    return false;
}

const Resp& Field(const Resp& map, std::string_view key) {
    assert(map.type == '%');
    for (std::size_t i = 0; i < map.items.size(); i += 2U)
        if (map.items[i].value == key) return map.items[i + 1U];
    throw std::logic_error("missing map key");
}

struct Event {
    std::string kind, action, name;
    std::string table;  // "" for null
    std::string data;   // "" for null
    bool operator==(const Event&) const = default;
};

struct Push {
    std::string epoch;
    std::uint64_t revision = 0U;
    bool user_null = true;
    std::string user;
    std::vector<Event> events;
};

Push CatalogPush(const std::string& bytes) {
    const auto reply = Parse(bytes);
    assert(reply.type == '>' && reply.items.size() == 7U && reply.items[0].value == "catalog");
    assert(reply.items[1].type == '$' && reply.items[1].value.size() == 32U);
    assert(reply.items[3].type == ':');
    assert(reply.items[6].type == '_');
    Push push{reply.items[1].value, std::stoull(reply.items[2].value), reply.items[4].type == '_', reply.items[4].value, {}};
    for (const auto& item : reply.items[5].items) {
        const auto& table = Field(item, "table");
        const auto& data = Field(item, "data");
        push.events.push_back(Event{Field(item, "kind").value, Field(item, "action").value, Field(item, "name").value,
            table.type == '_' ? std::string{} : table.value,
            // A server record is shown as "*"; a test record by its text.
            data.type == '_' ? std::string{} : Has(data, "v") ? Field(data, "v").value : std::string("*")});
    }
    return push;
}

// Events as "kind action name table data|...", "." for an empty field: a
// string to compare, without temporaries GCC 15 and 16 misreport as dangling.
std::string Render(const std::vector<Event>& events) {
    std::string out;
    const auto field = [&out](const std::string& value) { out += value.empty() ? std::string(".") : value; };
    for (const auto& event : events) {
        if (!out.empty()) out += "|";
        field(event.kind); out += " "; field(event.action); out += " "; field(event.name);
        out += " "; field(event.table); out += " "; field(event.data);
    }
    return out;
}

std::string Value(const std::string& text) {
    return "%1\r\n$1\r\nv\r\n$" + std::to_string(text.size()) + "\r\n" + text + "\r\n";
}

CatalogKey TableKey(const std::string& name) { return CatalogKey{.kind = CatalogKind::kTable, .table = name, .name = {}}; }
CatalogKey UserKey(const std::string& name) { return CatalogKey{.kind = CatalogKind::kUser, .table = {}, .name = name}; }
CatalogKey SlotKey(const std::string& table, const std::string& name) {
    return CatalogKey{.kind = CatalogKind::kSlot, .table = table, .name = name};
}

// The catalog a feed reads, as a test changes it.
struct FakeCatalog {
    std::map<CatalogKey, CatalogValue> records;
    std::set<std::string, std::less<>> unknown;
    std::size_t reads = 0U;
    bool fail = false;
    // Runs inside a whole-catalog read, as a statement running meanwhile would.
    std::function<void()> during_whole_read;

    CatalogState Read(const CatalogScope& scope) {
        ++reads;
        if (fail) throw std::runtime_error("catalog read failed");
        if (scope.all_tables && during_whole_read) during_whole_read();
        CatalogState state;
        state.unknown_tables = unknown;
        state.unknown_slots = unknown;
        for (const auto& [key, value] : records) {
            if (key.kind != CatalogKind::kUser && !scope.all_tables &&
                std::find(scope.tables.begin(), scope.tables.end(), key.table) == scope.tables.end()) continue;
            if (key.kind != CatalogKind::kUser && unknown.contains(key.table)) continue;
            state.records.emplace(key, value);
        }
        return state;
    }
    void SetTable(const std::string& name, const std::string& text) {
        records.insert_or_assign(TableKey(name), CatalogValue{.full = Value(text), .limited = Value(text)});
    }
    void SetUser(const std::string& name, const std::string& text) {
        records.insert_or_assign(UserKey(name), CatalogValue{.full = Value(text), .limited = std::nullopt});
    }
    void SetSlot(const std::string& table, const std::string& name, const std::string& owner) {
        records.insert_or_assign(SlotKey(table, name), CatalogValue{.full = Value("owner " + owner), .limited = Value("owner")});
    }
};

std::shared_ptr<CatalogFeed> MakeFeed(FakeCatalog& catalog, std::size_t buffer_bytes = 1U << 20U) {
    return std::make_shared<CatalogFeed>([&catalog](const CatalogScope& scope) { return catalog.Read(scope); }, buffer_bytes,
        std::make_unique<CatalogClock>(std::filesystem::path{}, CatalogClock::Mode::kMemory));
}

std::string NextBytes(CatalogSubscription& subscription) {
    auto frame = subscription.Next();
    assert(frame && frame->bytes && !frame->close);
    return *frame->bytes;
}

// A statement that can change the catalog: marked, then refreshed.
void Change(CatalogFeed& feed, const CatalogScope& scope, std::string_view user) {
    feed.Begin();
    feed.Refresh(scope, user);
}

// The revision of a progress push.
std::uint64_t ProgressRevision(const std::string& bytes) {
    const auto reply = Parse(bytes);
    assert(reply.type == '>' && reply.items.size() == 3U && reply.items[0].value == "progress");
    return std::stoull(reply.items[2].value);
}

// The next catalog push, after any progress.
std::string NextPushBytes(CatalogSubscription& subscription) {
    for (;;) {
        auto bytes = NextBytes(subscription);
        if (Parse(bytes).items.at(0).value != "progress") return bytes;
    }
}

void TestNoSubscriberReadsNothing() {
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    auto feed = MakeFeed(catalog);
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    assert(catalog.reads == 0U);
    {
        auto watch = feed->Subscribe(true, {});
        assert(catalog.reads == 1U);
    }
    // The last watch left: nothing is read again until the next one.
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    assert(catalog.reads == 1U);
    auto watch = feed->Subscribe(true, {});
    assert(catalog.reads == 2U);
    // Every statement took a revision, watched or not.
    assert(watch->revision() == 2U);
    const auto whole = CatalogPush(NextBytes(*watch));
    assert(whole.revision == 2U);
}

void TestWholeCatalogThenChanges() {
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    catalog.SetTable("x", "x1");
    catalog.SetUser("u", "user");
    catalog.SetSlot("t", "s", "u");
    auto feed = MakeFeed(catalog);
    std::size_t notified = 0U;
    auto watch = feed->Subscribe(true, [&] { ++notified; });
    assert(watch->revision() == 0U);
    const auto whole = CatalogPush(NextBytes(*watch));
    assert(whole.revision == 0U && whole.user_null);
    assert(Render(whole.events) == "table create t . one|table create x . x1|user create u . user|slot create s t owner u");
    assert(!watch->Next());

    catalog.SetTable("t", "two");
    catalog.SetSlot("t", "s2", "u");
    catalog.records.erase(UserKey("u"));
    // Outside the scope: published only by a statement on x.
    catalog.SetTable("x", "x2");
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    assert(notified == 1U);
    const auto change = CatalogPush(NextBytes(*watch));
    assert(change.epoch == whole.epoch && change.revision == 1U && !change.user_null && change.user == "admin");
    assert(Render(change.events) == "user drop u . .|table alter t . two|slot create s2 t owner u");
    assert(!watch->Next());

    // Nothing changed: the revision reaches the watch as progress.
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    assert(notified == 2U);
    assert(ProgressRevision(NextBytes(*watch)) == 2U);
    assert(!watch->Next());

    catalog.records.erase(TableKey("t"));
    catalog.records.erase(SlotKey("t", "s"));
    catalog.records.erase(SlotKey("t", "s2"));
    Change(*feed, CatalogScope{.tables = {}, .all_tables = true}, {});
    const auto dropped = CatalogPush(NextBytes(*watch));
    assert(dropped.revision == 3U && dropped.user_null);
    assert(Render(dropped.events) == "table drop t . .|slot drop s t .|slot drop s2 t .|table alter x . x2");

    // A new watch starts from the whole catalog at the current revision.
    auto late = feed->Subscribe(true, {});
    assert(late->revision() == 3U);
    const auto now = CatalogPush(NextBytes(*late));
    assert(now.revision == 3U);
    assert(Render(now.events) == "table create x . x2");
}

void TestUsersHiddenWithoutManagesUsers() {
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    catalog.SetUser("u", "user");
    catalog.SetSlot("t", "s", "u");
    auto feed = MakeFeed(catalog);
    auto full = feed->Subscribe(true, {});
    auto limited = feed->Subscribe(false, {});
    (void)NextBytes(*full);
    const auto whole = CatalogPush(NextBytes(*limited));
    assert(Render(whole.events) == "table create t . one|slot create s t owner");

    // A user change and a new slot owner: only the full watch sees them.
    catalog.SetUser("u", "user2");
    catalog.SetSlot("t", "s", "v");
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    const auto seen = CatalogPush(NextBytes(*full));
    assert(!seen.user_null && seen.user == "admin");
    assert(Render(seen.events) == "user alter u . user2|slot alter s t owner v");
    // The limited watch only learns that its catalog stays as it was.
    assert(ProgressRevision(NextBytes(*limited)) == seen.revision);
    assert(!limited->Next());

    catalog.SetTable("t", "two");
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    const auto table = CatalogPush(NextBytes(*limited));
    // Who ran the statement is user information too.
    assert(table.revision == 2U && table.user_null);
    assert(Render(table.events) == "table alter t . two");
    assert(CatalogPush(NextBytes(*full)).revision == 2U);
}

void TestUnknownTableKeepsRecords() {
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    catalog.SetSlot("t", "s", "u");
    auto feed = MakeFeed(catalog);
    auto watch = feed->Subscribe(true, {});
    (void)NextBytes(*watch);
    // A fenced table that never served cannot be read: it is not dropped.
    catalog.unknown.insert("t");
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    assert(ProgressRevision(NextBytes(*watch)) == 1U);
    assert(!watch->Next());
    // The next refresh reads it again, whatever it is for.
    catalog.unknown.clear();
    catalog.SetTable("t", "two");
    catalog.SetUser("v", "user");
    Change(*feed, CatalogScope{.tables = {}, .all_tables = false}, "admin");
    const auto retried = CatalogPush(NextBytes(*watch));
    assert(Render(retried.events) == "table alter t . two|user create v . user");
    catalog.records.erase(TableKey("t"));
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    const auto dropped = CatalogPush(NextBytes(*watch));
    assert(Render(dropped.events) == "table drop t . .");
}

void TestOverflowSendsResyncAndWholeCatalog() {
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    // Every push is larger than this history.
    auto feed = MakeFeed(catalog, 1U);
    auto watch = feed->Subscribe(true, {});
    const auto whole = CatalogPush(NextBytes(*watch));
    catalog.SetTable("t", "two");
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    catalog.SetTable("u", "three");
    Change(*feed, CatalogScope{.tables = {"u"}, .all_tables = false}, "admin");
    const auto resync = Parse(NextBytes(*watch));
    assert(resync.type == '>' && resync.items.size() == 5U && resync.items[0].value == "resync");
    assert(resync.items[1].value == whole.epoch && resync.items[2].value == "2");
    assert(resync.items[3].type == '_' && resync.items[4].type == '_');
    const auto again = CatalogPush(NextBytes(*watch));
    assert(again.revision == 2U && again.user_null);
    assert(Render(again.events) == "table create t . two|table create u . three");
    assert(!watch->Next());
}

void TestFailureEndsWatches() {
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    auto feed = MakeFeed(catalog);
    std::size_t notified = 0U;
    auto watch = feed->Subscribe(true, [&] { ++notified; });
    (void)NextBytes(*watch);
    catalog.fail = true;
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    assert(notified == 1U);
    const auto end = watch->Next();
    assert(end && end->close && end->bytes->starts_with("-ERR INTERNAL "));
    assert(!watch->Next());
    // A new watch reads the whole catalog again, while the failed one stays ended.
    catalog.fail = false;
    catalog.SetTable("t", "two");
    const auto reads = catalog.reads;
    auto fresh = feed->Subscribe(true, {});
    assert(catalog.reads == reads + 1U);
    const auto whole = CatalogPush(NextBytes(*fresh));
    assert(Render(whole.events) == "table create t . two");
    catalog.SetTable("t", "three");
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    assert(CatalogPush(NextBytes(*fresh)).events.size() == 1U);
    assert(!watch->Next());
}

// WATCH CATALOG through the engine, without users.
struct EngineFixture {
    test::ScopedTempDir dir;
    std::shared_ptr<TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    explicit EngineFixture(std::size_t feed_buffer_bytes = kDefaultFeedBufferBytes) : dir("chunkdb-catalog-watch") {
        CatalogConfig config;
        config.data_dir = dir.path();
        config.feed_buffer_bytes = feed_buffer_bytes;
        catalog = std::make_shared<TableCatalog>(config);
        engine = std::make_unique<CommandEngine>(EngineConfig{.require_auth = false}, catalog);
        // A catalog opened this way starts without tables.
        auto session = Session();
        assert(Run(*session, "CREATE TABLE default (n u8) CHUNK 4 x 4") == "+OK\r\n");
    }
    std::unique_ptr<SessionState> Session() {
        auto session = std::make_unique<SessionState>();
        assert(engine->Execute(*session, "HELLO 3\r\n").front() == '%');
        return session;
    }
    std::string Run(SessionState& session, const std::string& line) { return engine->Execute(session, line + "\r\n"); }
    std::unique_ptr<SessionState> Watcher() {
        auto session = Session();
        assert(Run(*session, "WATCH CATALOG").starts_with("%2\r\n$5\r\nepoch\r\n"));
        return session;
    }
};

bool HasEvent(const Push& push, std::string_view kind, std::string_view action, std::string_view name) {
    return std::any_of(push.events.begin(), push.events.end(), [&](const Event& event) {
        return event.kind == kind && event.action == action && event.name == name;
    });
}

void TestRefusedStatementTakesNoRevision() {
    EngineFixture fixture;
    auto watcher = fixture.Watcher();
    const auto whole = CatalogPush(NextBytes(*watcher->catalog_watch));
    assert(HasEvent(whole, "table", "create", "default"));
    const auto recorded = *ReadCatalogRevision(fixture.dir.path());
    auto ddl = fixture.Session();
    // Refused before its decision: no mark, no revision, nothing to watch.
    assert(fixture.Run(*ddl, "ALTER TABLE default DROP COLUMN nosuch").starts_with("-ERR "));
    assert(!watcher->catalog_watch->Next());
    assert(*ReadCatalogRevision(fixture.dir.path()) == recorded);
    assert(fixture.Run(*ddl, "CREATE TABLE after_refusal (n u8) CHUNK 4 x 4") == "+OK\r\n");
    const auto created = CatalogPush(NextBytes(*watcher->catalog_watch));
    assert(created.revision == whole.revision + 1U);
    assert(HasEvent(created, "table", "create", "after_refusal"));
}

void TestCancelAfterCatalogMark() {
    EngineFixture fixture;
    auto watcher = fixture.Watcher();
    const auto whole = CatalogPush(NextBytes(*watcher->catalog_watch));
    const auto recorded = *ReadCatalogRevision(fixture.dir.path());
    auto ddl = fixture.Session();
    StatementControl control(StatementControl::Clock::time_point{}, 0U);
    assert(control.Start());
    struct CancelAtMark final : CommandEngineTestHook {
        explicit CancelAtMark(StatementControl& control) : control(control) {}
        void Run(Point point, std::string_view) override {
            if (point == Point::kAfterCatalogMark) assert(control.RequestStop(StatementStopReason::kCancelled));
        }
        StatementControl& control;
    } hook(control);
    fixture.engine->SetHookForTests(&hook);
    {
        const ScopedStatementControl statement(&control);
        assert(fixture.Run(*ddl, "CREATE TABLE cancelled_t (n u8) CHUNK 4 x 4").starts_with("-ERR CANCELLED "));
    }
    fixture.engine->SetHookForTests(nullptr);
    assert(!fixture.catalog->Find("cancelled_t"));
    assert(!watcher->catalog_watch->Next());
    assert(*ReadCatalogRevision(fixture.dir.path()) == recorded);
    assert(fixture.Run(*ddl, "CREATE TABLE next_t (n u8) CHUNK 4 x 4") == "+OK\r\n");
    const auto created = CatalogPush(NextBytes(*watcher->catalog_watch));
    assert(created.revision == whole.revision + 1U && HasEvent(created, "table", "create", "next_t"));
}

struct FeedCleanupFailure final : FeedTestHook {
    std::atomic<bool> armed{false};
    void Run(Point point, std::uint64_t) override {
        if (point == Point::kBeforeLingerPause && armed.exchange(false, std::memory_order_acq_rel))
            throw std::runtime_error("injected feed cleanup failure");
    }
};
// Tables keep a pointer to it after they are closed.
FeedCleanupFailure feed_failure;

void TestTableNeedingRecoveryKeepsWatch() {
    EngineFixture fixture;
    auto session = fixture.Session();
    assert(fixture.Run(*session, "CREATE TABLE realm (n u8) CHUNK 2 x 2") == "+OK\r\n");
    auto watcher = fixture.Watcher();
    (void)NextBytes(*watcher->catalog_watch);
    const auto table = fixture.catalog->Find("realm");
    {
        auto subscription = table->SubscribeFeed();
        FeedTestAccess::SetHook(*table, &feed_failure);
    }
    feed_failure.armed.store(true, std::memory_order_release);
    FeedTestAccess::ExpireLinger(*table);
    assert(FeedTestAccess::WaitLingerExpired(*table, std::chrono::seconds(10)));
    assert(fixture.catalog->Fence("realm"));
    // Statements on it fail; the watch keeps its record and goes on.
    assert(fixture.Run(*session, "ALTER TABLE realm ADD COLUMN extra u8 NULL").starts_with("-ERR "));
    assert(!watcher->catalog_watch->Next());
    // A new watch reads the whole catalog, the table included.
    auto late = fixture.Watcher();
    const auto whole = CatalogPush(NextPushBytes(*late->catalog_watch));
    assert(HasEvent(whole, "table", "create", "realm") && HasEvent(whole, "table", "create", "default"));
    assert(fixture.Run(*session, "CREATE TABLE later (n u8) CHUNK 2 x 2") == "+OK\r\n");
    assert(HasEvent(CatalogPush(NextPushBytes(*watcher->catalog_watch)), "table", "create", "later"));
    assert(HasEvent(CatalogPush(NextPushBytes(*late->catalog_watch)), "table", "create", "later"));
}

void TestConcurrentStatements() {
    EngineFixture fixture;
    auto watcher = fixture.Watcher();
    auto first = CatalogPush(NextBytes(*watcher->catalog_watch));
    std::map<std::string, std::string> tables;
    std::uint64_t revision = first.revision;
    const auto apply = [&](const Push& push) {
        // Every event moves from the state of the previous one, in revision order.
        assert(push.revision > revision);
        revision = push.revision;
        for (const auto& event : push.events) {
            assert(event.kind == "table");
            const bool present = tables.contains(event.name);
            if (event.action == "create") { assert(!present); tables[event.name] = event.data; }
            else if (event.action == "alter") assert(present);
            else { assert(event.action == "drop" && present); tables.erase(event.name); }
        }
    };
    for (const auto& event : first.events) tables[event.name] = event.data;
    constexpr std::size_t kThreads = 4U, kRounds = 6U;
    std::vector<std::thread> threads;
    for (std::size_t t = 0; t < kThreads; ++t) {
        threads.emplace_back([&fixture, t] {
            auto session = fixture.Session();
            for (std::size_t round = 0; round < kRounds; ++round) {
                const auto name = "c" + std::to_string(t) + "_" + std::to_string(round);
                assert(fixture.Run(*session, "CREATE TABLE " + name + " (n u8) CHUNK 2 x 2") == "+OK\r\n");
                assert(fixture.Run(*session, "ALTER TABLE " + name + " ADD COLUMN m u8 NULL") == "+OK\r\n");
                if (round % 2U == 0U) assert(fixture.Run(*session, "DROP TABLE " + name) == "+OK\r\n");
            }
        });
    }
    for (auto& thread : threads) thread.join();
    while (auto frame = watcher->catalog_watch->Next()) {
        assert(!frame->close);
        // A statement whose change an earlier event carried sends progress.
        if (Parse(*frame->bytes).items.at(0).value == "progress") continue;
        apply(CatalogPush(*frame->bytes));
    }
    std::set<std::string> listed;
    for (const auto& info : fixture.catalog->List()) listed.insert(info.name);
    std::set<std::string> seen;
    for (const auto& [name, _] : tables) seen.insert(name);
    assert(seen == listed && listed.size() == 1U + kThreads * kRounds / 2U);
}

void TestRenameAndSwap() {
    EngineFixture fixture;
    auto session = fixture.Session();
    assert(fixture.Run(*session, "CREATE TABLE a (n u8) CHUNK 2 x 2") == "+OK\r\n");
    assert(fixture.Run(*session, "CREATE TABLE b (n u8) CHUNK 2 x 2") == "+OK\r\n");
    auto watcher = fixture.Watcher();
    (void)NextBytes(*watcher->catalog_watch);
    assert(fixture.Run(*session, "ALTER TABLE a RENAME TO c") == "+OK\r\n");
    const auto renamed = CatalogPush(NextPushBytes(*watcher->catalog_watch));
    assert(renamed.events.size() == 2U && HasEvent(renamed, "table", "drop", "a") && HasEvent(renamed, "table", "create", "c"));
#ifndef _WIN32
    assert(fixture.Run(*session, "SWAP TABLES b, c") == "+OK\r\n");
    const auto swapped = CatalogPush(NextPushBytes(*watcher->catalog_watch));
    assert(swapped.events.size() == 2U && HasEvent(swapped, "table", "alter", "b") && HasEvent(swapped, "table", "alter", "c"));
#endif
}

void TestResyncWithSmallBudget() {
    EngineFixture fixture(2048U);
    auto session = fixture.Session();
    auto watcher = fixture.Watcher();
    const auto whole = CatalogPush(NextBytes(*watcher->catalog_watch));
    for (std::size_t i = 0; i < 8U; ++i)
        assert(fixture.Run(*session, "CREATE TABLE r" + std::to_string(i) + " (n u8) CHUNK 2 x 2") == "+OK\r\n");
    const auto resync = Parse(NextBytes(*watcher->catalog_watch));
    assert(resync.type == '>' && resync.items[0].value == "resync" && resync.items[1].value == whole.epoch);
    const auto again = CatalogPush(NextBytes(*watcher->catalog_watch));
    assert(again.revision == whole.revision + 8U && again.events.size() == 9U && HasEvent(again, "table", "create", "r7"));
    assert(!watcher->catalog_watch->Next());
}

void TestSecondWatchRefused() {
    EngineFixture fixture;
    auto watcher = fixture.Watcher();
    const auto* open = watcher->catalog_watch.get();
    assert(fixture.Run(*watcher, "WATCH default").starts_with("-ERR INVALID_ARGUMENT"));
    assert(fixture.Run(*watcher, "WATCH CATALOG").starts_with("-ERR INVALID_ARGUMENT"));
    assert(watcher->catalog_watch.get() == open && !watcher->watch);
    auto table = fixture.Session();
    assert(fixture.Run(*table, "WATCH default").starts_with("%2\r\n"));
    assert(fixture.Run(*table, "WATCH CATALOG").starts_with("-ERR INVALID_ARGUMENT") && !table->catalog_watch);
}

FeedPosition At(const StoreId& epoch, std::uint64_t revision) { return FeedPosition{.epoch = epoch, .revision = revision}; }

bool IsResync(const std::string& bytes) { return Parse(bytes).items.at(0).value == "resync"; }

void TestResumeAfter() {
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    auto feed = MakeFeed(catalog);
    // Keeps the history: it reads nothing after the whole catalog.
    auto keeper = feed->Subscribe(true, {});
    (void)NextBytes(*keeper);
    catalog.SetTable("t", "two");
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    const auto epoch = feed->epoch();
    // At a revision the history covers: only the later changes.
    auto behind = feed->Subscribe(true, {}, At(epoch, 0U));
    assert(behind->revision() == 0U);
    const auto change = CatalogPush(NextBytes(*behind));
    assert(change.revision == 1U);
    assert(Render(change.events) == "table alter t . two");
    assert(!behind->Next());
    // At the current revision: nothing until the next change.
    auto current = feed->Subscribe(true, {}, At(epoch, 1U));
    assert(current->revision() == 1U);
    assert(!current->Next());
    // Another epoch, or a revision ahead: resync, then the whole catalog.
    auto other = feed->Subscribe(true, {}, At(StoreId{1}, 1U));
    assert(IsResync(NextBytes(*other)));
    assert(CatalogPush(NextBytes(*other)).revision == 1U);
    auto ahead = feed->Subscribe(true, {}, At(epoch, 9U));
    assert(IsResync(NextBytes(*ahead)));
    assert(Render(CatalogPush(NextBytes(*ahead)).events) == "table create t . two");
}

void TestProgressCoalesces() {
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    auto feed = MakeFeed(catalog);
    auto watch = feed->Subscribe(true, {});
    (void)NextBytes(*watch);
    const CatalogScope scope{.tables = {"t"}, .all_tables = false};
    Change(*feed, scope, "admin");
    Change(*feed, scope, "admin");
    assert(ProgressRevision(NextBytes(*watch)) == 2U);
    assert(!watch->Next());

    Change(*feed, scope, "admin");
    catalog.SetTable("t", "two");
    Change(*feed, scope, "admin");
    const auto changed = CatalogPush(NextBytes(*watch));
    assert(changed.revision == 4U && Render(changed.events) == "table alter t . two");
    assert(!watch->Next());
}

void TestDurableClock() {
    test::ScopedTempDir dir("chunkdb-catalog-clock");
    StoreId epoch{};
    {
        CatalogClock clock(dir.path(), CatalogClock::Mode::kDurable);
        epoch = clock.epoch();
        assert(clock.revision() == 0U);
        // A start writes nothing: the first mark creates the record.
        assert(!ReadCatalogRevision(dir.path()).has_value());
        clock.Begin();
        assert(ReadCatalogRevision(dir.path())->in_progress);
        assert(clock.End() == 1U);
        assert((*ReadCatalogRevision(dir.path()) == CatalogRevisionRecord{.epoch = epoch, .revision = 1U}));
        // A read while a statement runs takes a new revision, still marked.
        clock.Begin();
        assert(clock.Snapshot() == 2U);
        assert(ReadCatalogRevision(dir.path())->revision == 2U);
        clock.Begin();
        assert(clock.End() == 3U);
        assert(ReadCatalogRevision(dir.path())->in_progress);
        // Stopped while a statement runs, as a crash stops it.
    }
    {
        // The catalog may have changed after revision 3: the start takes 4.
        CatalogClock clock(dir.path(), CatalogClock::Mode::kDurable);
        assert(clock.epoch() == epoch && clock.revision() == 4U);
        // The record is still marked: a read takes a new revision and records it.
        assert(clock.Snapshot() == 5U);
        assert((*ReadCatalogRevision(dir.path()) == CatalogRevisionRecord{.epoch = epoch, .revision = 5U}));
        // Unmarked now: a read keeps it.
        assert(clock.Snapshot() == 5U);
    }
    {
        // Unchanged after a clean stop.
        CatalogClock clock(dir.path(), CatalogClock::Mode::kDurable);
        assert(clock.epoch() == epoch && clock.revision() == 5U);
    }
    {
        // Several writers: the record stays marked, and the next start moves on.
        CatalogClock shared(dir.path(), CatalogClock::Mode::kShared);
        shared.Begin();
        (void)shared.End();
        assert((*ReadCatalogRevision(dir.path()) == CatalogRevisionRecord{.epoch = epoch, .revision = 5U, .in_progress = true}));
    }
    {
        CatalogClock clock(dir.path(), CatalogClock::Mode::kDurable);
        assert(clock.epoch() == epoch && clock.revision() == 6U);
    }
    // A damaged record starts a new epoch.
    auto bytes = EncodeCatalogRevision(CatalogRevisionRecord{.epoch = epoch, .revision = 5U});
    bytes[30] ^= 1U;
    {
        std::ofstream out(dir.path() / std::string(kCatalogRevisionFileName), std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    }
    bool refused = false;
    try { (void)DecodeCatalogRevision(bytes); } catch (const std::runtime_error&) { refused = true; }
    assert(refused);
    {
        CatalogClock clock(dir.path(), CatalogClock::Mode::kDurable);
        assert(clock.epoch() != epoch && clock.revision() == 0U);
    }
    // A missing record starts a new epoch as well.
    std::filesystem::remove(dir.path() / std::string(kCatalogRevisionFileName));
    {
        CatalogClock clock(dir.path(), CatalogClock::Mode::kDurable);
        assert(clock.epoch() != epoch);
        (void)clock.Snapshot();
        assert(ReadCatalogRevision(dir.path())->epoch == clock.epoch());
    }
}

void TestCancelKeepsNeededMark() {
    test::ScopedTempDir dir("chunkdb-catalog-cancel-mark");
    CatalogClock clock(dir.path(), CatalogClock::Mode::kDurable);
    clock.Begin();
    clock.Begin();
    assert(clock.End() == 1U);
    assert(ReadCatalogRevision(dir.path())->in_progress);
    // The remaining statement was stopped before applying, so the committed
    // revision stands and its mark clears.
    assert(clock.End(false) == 1U);
    const auto settled = *ReadCatalogRevision(dir.path());
    assert(settled.revision == 1U && !settled.in_progress);

    clock.Begin();
    clock.Begin();
    assert(clock.End(false) == 1U);
    assert(ReadCatalogRevision(dir.path())->in_progress);
    clock.Hold(true);
    assert(clock.End(false) == 1U);
    assert(ReadCatalogRevision(dir.path())->in_progress);
}

void TestDurableResume() {
    test::ScopedTempDir dir("chunkdb-catalog-resume");
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    const auto feed_at = [&] {
        return std::make_shared<CatalogFeed>([&catalog](const CatalogScope& scope) { return catalog.Read(scope); }, 1U << 20U,
            std::make_unique<CatalogClock>(dir.path(), CatalogClock::Mode::kDurable));
    };
    FeedPosition seen;
    {
        auto feed = feed_at();
        auto watch = feed->Subscribe(true, {});
        (void)NextBytes(*watch);
        catalog.SetTable("t", "two");
        Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
        seen = At(feed->epoch(), CatalogPush(NextBytes(*watch)).revision);
        // A statement that changes nothing moves the watch on with progress.
        Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
        seen.revision = ProgressRevision(NextBytes(*watch));
    }
    {
        // After a restart, a consumer at the last revision continues without
        // the whole catalog.
        auto feed = feed_at();
        auto watch = feed->Subscribe(true, {}, seen);
        assert(feed->epoch() == seen.epoch && watch->revision() == seen.revision);
        assert(!watch->Next());
        catalog.SetTable("t", "three");
        Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
        assert(Render(CatalogPush(NextBytes(*watch)).events) == "table alter t . three");
        // One step behind: the history after the restart does not hold it.
        auto behind = feed->Subscribe(true, {}, At(seen.epoch, seen.revision - 1U));
        assert(IsResync(NextBytes(*behind)));
    }
}

void TestCatalogDefaultsAcrossStarts() {
    test::ScopedTempDir dir("chunkdb-catalog-defaults");
    CatalogConfig config;
    config.data_dir = dir.path();
    const auto watch = [&](const std::string& after, bool resync) {
        auto catalog = std::make_shared<TableCatalog>(config);
        CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
        SessionState session;
        assert(engine.Execute(session, "HELLO 3").starts_with("%"));
        if (after.empty()) assert(engine.Execute(session, "CREATE TABLE inherited (n u8) CHUNK 4 x 4") == "+OK\r\n");
        const auto reply = engine.Execute(session, after.empty() ? "WATCH CATALOG" : "WATCH CATALOG AFTER " + after);
        const auto start = Parse(reply);
        const auto position = Field(start, "epoch").value + " " + Field(start, "revision").value;
        if (resync) {
            assert(position != after);
            assert(IsResync(NextBytes(*session.catalog_watch)));
        } else if (!after.empty()) {
            assert(position == after);
            assert(!session.catalog_watch->Next());
            return position;
        }
        const auto whole = Parse(NextBytes(*session.catalog_watch));
        assert(whole.items.at(0).value == "catalog");
        const auto& events = whole.items.at(5).items;
        const auto table = std::find_if(events.begin(), events.end(), [](const Resp& event) {
            return Field(event, "kind").value == "table" && Field(event, "name").value == "inherited";
        });
        assert(table != events.end());
        const auto& options = Field(Field(*table, "data"), "options");
        assert(Field(options, "feed_buffer_bytes").value == std::to_string(config.feed_buffer_bytes));
        assert(Field(options, "slot_max_bytes").value == std::to_string(config.slot_max_bytes));
        return position;
    };
    auto position = watch({}, false);
    config.feed_buffer_bytes /= 2U;
    position = watch(position, true);
    config.slot_max_bytes /= 2U;
    position = watch(position, true);
    (void)watch(position, false);
}

void ArmFailpoint(const char* name) {
#ifdef _WIN32
    (void)_putenv_s(name, "1");
#else
    (void)setenv(name, "1", 1);
#endif
}

template <typename Fn> bool Throws(Fn&& fn) {
    try {
        fn();
    } catch (const std::runtime_error&) {
        return true;
    }
    return false;
}

void TestStatementDuringSubscribeRead() {
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    auto feed = MakeFeed(catalog);
    Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    const auto epoch = feed->epoch();
    // A CREATE TABLE begins while the first watch reads the catalog, and its
    // table is already there when the read reaches it.
    bool begun = false;
    catalog.during_whole_read = [&] {
        if (begun) return;
        begun = true;
        feed->Begin();
        catalog.SetTable("t2", "new");
    };
    auto watch = feed->Subscribe(true, {}, At(epoch, 1U));
    // The read is not the catalog of revision 1: the consumer rebuilds.
    assert(watch->revision() == 2U);
    assert(IsResync(NextBytes(*watch)));
    const auto whole = CatalogPush(NextBytes(*watch));
    assert(whole.revision == 2U);
    assert(Render(whole.events) == "table create t . one|table create t2 . new");
    // The statement ends: the read already showed its change.
    feed->Refresh(CatalogScope{.tables = {"t2"}, .all_tables = false}, "admin");
    assert(ProgressRevision(NextBytes(*watch)) == 3U);
    assert(!watch->Next());
}

void TestFailedRecordWrites() {
    test::ScopedTempDir dir("chunkdb-catalog-write-fail");
    StoreId epoch{};
    {
        CatalogClock clock(dir.path(), CatalogClock::Mode::kDurable);
        epoch = clock.epoch();
        // A mark that cannot be written refuses the statement.
        ArmFailpoint("CHUNKDB_FAILPOINT_CATALOG_REVISION_WRITE_FAIL_ONCE");
        assert(Throws([&] { clock.Begin(); }));
        assert(!ReadCatalogRevision(dir.path()).has_value());
        clock.Begin();
        clock.Begin();
        ArmFailpoint("CHUNKDB_FAILPOINT_CATALOG_REVISION_WRITE_FAIL_ONCE");
        assert(Throws([&] { (void)clock.End(); }));
        // The record is unknown after the failure: the next mark is written again.
        clock.Begin();
        const auto marked = *ReadCatalogRevision(dir.path());
        assert(marked.in_progress && marked.revision == 1U);
        // Stopped with statements running, as a crash stops them.
    }
    {
        CatalogClock clock(dir.path(), CatalogClock::Mode::kDurable);
        assert(clock.epoch() == epoch && clock.revision() == 2U);
        clock.Begin();
        ArmFailpoint("CHUNKDB_FAILPOINT_CATALOG_REVISION_WRITE_FAIL_ONCE");
        assert(Throws([&] { (void)clock.End(); }));
        // Revision 3 was never handed out; a read records it first.
        assert(clock.Snapshot() == 3U);
        assert((*ReadCatalogRevision(dir.path()) == CatalogRevisionRecord{.epoch = epoch, .revision = 3U}));
        // Held: marked even with nothing running, and a read takes a new revision.
        clock.Hold(true);
        assert(ReadCatalogRevision(dir.path())->in_progress);
        assert(clock.Snapshot() == 4U);
        assert(ReadCatalogRevision(dir.path())->in_progress);
        // Released, a settled refresh at the current revision clears the mark.
        clock.Hold(false);
        clock.Settle(3U);
        assert(ReadCatalogRevision(dir.path())->in_progress);
        clock.Settle(4U);
        assert((*ReadCatalogRevision(dir.path()) == CatalogRevisionRecord{.epoch = epoch, .revision = 4U}));
    }
    {
        CatalogClock clock(dir.path(), CatalogClock::Mode::kDurable);
        assert(clock.revision() == 4U);
    }
}

void TestUnreadTableKeepsMark() {
    test::ScopedTempDir dir("chunkdb-catalog-unread");
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    const auto feed_at = [&] {
        return std::make_shared<CatalogFeed>([&catalog](const CatalogScope& scope) { return catalog.Read(scope); }, 1U << 20U,
            std::make_unique<CatalogClock>(dir.path(), CatalogClock::Mode::kDurable));
    };
    FeedPosition seen;
    {
        auto feed = feed_at();
        auto watch = feed->Subscribe(true, {});
        (void)NextBytes(*watch);
        catalog.unknown.insert("t");
        Change(*feed, CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
        seen = At(feed->epoch(), ProgressRevision(NextBytes(*watch)));
        // Its slots may differ from the published records: the record stays marked.
        assert(ReadCatalogRevision(dir.path())->in_progress);
    }
    catalog.unknown.clear();
    {
        auto feed = feed_at();
        auto watch = feed->Subscribe(true, {}, seen);
        assert(IsResync(NextBytes(*watch)));
    }
}

}  // namespace

int main() {
    TestNoSubscriberReadsNothing();
    TestWholeCatalogThenChanges();
    TestUsersHiddenWithoutManagesUsers();
    TestUnknownTableKeepsRecords();
    TestOverflowSendsResyncAndWholeCatalog();
    TestFailureEndsWatches();
    TestRefusedStatementTakesNoRevision();
    TestCancelAfterCatalogMark();
    TestTableNeedingRecoveryKeepsWatch();
    TestConcurrentStatements();
    TestRenameAndSwap();
    TestResyncWithSmallBudget();
    TestSecondWatchRefused();
    TestResumeAfter();
    TestProgressCoalesces();
    TestDurableClock();
    TestCancelKeepsNeededMark();
    TestDurableResume();
    TestCatalogDefaultsAcrossStarts();
    TestStatementDuringSubscribeRead();
    TestFailedRecordWrites();
    TestUnreadTableKeepsMark();
}
