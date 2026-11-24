// CatalogFeed (WATCH CATALOG): whole-catalog pushes, changes by scope,
// visibility without MANAGES USERS, history overflow and failure.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <cassert>
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

// A named parameter, not a temporary inside the assert condition.
bool EventsAre(const std::vector<Event>& actual, const std::vector<Event>& expected) { return actual == expected; }

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

    CatalogState Read(const CatalogScope& scope) {
        ++reads;
        if (fail) throw std::runtime_error("catalog read failed");
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
    return std::make_shared<CatalogFeed>([&catalog](const CatalogScope& scope) { return catalog.Read(scope); }, buffer_bytes);
}

std::string NextBytes(CatalogSubscription& subscription) {
    auto frame = subscription.Next();
    assert(frame && frame->bytes && !frame->close);
    return *frame->bytes;
}

void TestNoSubscriberReadsNothing() {
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    auto feed = MakeFeed(catalog);
    feed->Refresh(CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    assert(catalog.reads == 0U);
    {
        auto watch = feed->Subscribe(true, {});
        assert(catalog.reads == 1U);
    }
    // The last watch left: nothing is read again until the next one.
    feed->Refresh(CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    assert(catalog.reads == 1U);
    auto watch = feed->Subscribe(true, {});
    assert(catalog.reads == 2U);
    // Each read of the whole catalog takes a new revision.
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
    assert(watch->revision() == 1U);
    const auto whole = CatalogPush(NextBytes(*watch));
    assert(whole.revision == 1U && whole.user_null);
    assert(EventsAre(whole.events, {
        {"table", "create", "t", "", "one"}, {"table", "create", "x", "", "x1"},
        {"user", "create", "u", "", "user"}, {"slot", "create", "s", "t", "owner u"}}));
    assert(!watch->Next());

    catalog.SetTable("t", "two");
    catalog.SetSlot("t", "s2", "u");
    catalog.records.erase(UserKey("u"));
    // Outside the scope: published only by a statement on x.
    catalog.SetTable("x", "x2");
    feed->Refresh(CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    assert(notified == 1U);
    const auto change = CatalogPush(NextBytes(*watch));
    assert(change.epoch == whole.epoch && change.revision == 2U && !change.user_null && change.user == "admin");
    assert(EventsAre(change.events, {
        {"user", "drop", "u", "", ""}, {"table", "alter", "t", "", "two"}, {"slot", "create", "s2", "t", "owner u"}}));
    assert(!watch->Next());

    // Nothing changed: no push and no revision.
    feed->Refresh(CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    assert(!watch->Next() && notified == 1U);

    catalog.records.erase(TableKey("t"));
    catalog.records.erase(SlotKey("t", "s"));
    catalog.records.erase(SlotKey("t", "s2"));
    feed->Refresh(CatalogScope{.tables = {}, .all_tables = true}, {});
    const auto dropped = CatalogPush(NextBytes(*watch));
    assert(dropped.revision == 3U && dropped.user_null);
    assert(EventsAre(dropped.events, {
        {"table", "drop", "t", "", ""}, {"slot", "drop", "s", "t", ""}, {"slot", "drop", "s2", "t", ""},
        {"table", "alter", "x", "", "x2"}}));

    // A new watch starts from the whole catalog at the current revision.
    auto late = feed->Subscribe(true, {});
    assert(late->revision() == 3U);
    const auto now = CatalogPush(NextBytes(*late));
    assert(now.revision == 3U);
    assert(EventsAre(now.events, {{"table", "create", "x", "", "x2"}}));
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
    assert(EventsAre(whole.events, {{"table", "create", "t", "", "one"}, {"slot", "create", "s", "t", "owner"}}));

    // A user change and a new slot owner: only the full watch sees them.
    catalog.SetUser("u", "user2");
    catalog.SetSlot("t", "s", "v");
    feed->Refresh(CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    const auto seen = CatalogPush(NextBytes(*full));
    assert(!seen.user_null && seen.user == "admin");
    assert(EventsAre(seen.events, {{"user", "alter", "u", "", "user2"}, {"slot", "alter", "s", "t", "owner v"}}));
    assert(!limited->Next());

    catalog.SetTable("t", "two");
    feed->Refresh(CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    const auto table = CatalogPush(NextBytes(*limited));
    // Who ran the statement is user information too.
    assert(table.revision == 3U && table.user_null);
    assert(EventsAre(table.events, {{"table", "alter", "t", "", "two"}}));
    assert(CatalogPush(NextBytes(*full)).revision == 3U);
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
    feed->Refresh(CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    assert(!watch->Next());
    // The next refresh reads it again, whatever it is for.
    catalog.unknown.clear();
    catalog.SetTable("t", "two");
    catalog.SetUser("v", "user");
    feed->Refresh(CatalogScope{.tables = {}, .all_tables = false}, "admin");
    const auto retried = CatalogPush(NextBytes(*watch));
    assert(EventsAre(retried.events, {{"table", "alter", "t", "", "two"}, {"user", "create", "v", "", "user"}}));
    catalog.records.erase(TableKey("t"));
    feed->Refresh(CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    const auto dropped = CatalogPush(NextBytes(*watch));
    assert(EventsAre(dropped.events, {{"table", "drop", "t", "", ""}}));
}

void TestOverflowSendsResyncAndWholeCatalog() {
    FakeCatalog catalog;
    catalog.SetTable("t", "one");
    // Every push is larger than this history.
    auto feed = MakeFeed(catalog, 1U);
    auto watch = feed->Subscribe(true, {});
    const auto whole = CatalogPush(NextBytes(*watch));
    catalog.SetTable("t", "two");
    feed->Refresh(CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
    catalog.SetTable("u", "three");
    feed->Refresh(CatalogScope{.tables = {"u"}, .all_tables = false}, "admin");
    const auto resync = Parse(NextBytes(*watch));
    assert(resync.type == '>' && resync.items.size() == 5U && resync.items[0].value == "resync");
    assert(resync.items[1].value == whole.epoch && resync.items[2].value == "3");
    assert(resync.items[3].type == '_' && resync.items[4].type == '_');
    const auto again = CatalogPush(NextBytes(*watch));
    assert(again.revision == 3U && again.user_null);
    assert(EventsAre(again.events, {{"table", "create", "t", "", "two"}, {"table", "create", "u", "", "three"}}));
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
    feed->Refresh(CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
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
    assert(EventsAre(whole.events, {{"table", "create", "t", "", "two"}}));
    catalog.SetTable("t", "three");
    feed->Refresh(CatalogScope{.tables = {"t"}, .all_tables = false}, "admin");
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

bool HasEvent(const Push& push, const std::string& kind, const std::string& action, const std::string& name) {
    return std::any_of(push.events.begin(), push.events.end(), [&](const Event& event) {
        return event.kind == kind && event.action == action && event.name == name;
    });
}

// Stops the statement that runs as the catalog refresh starts.
struct StopBeforeRefresh final : CommandEngineTestHook {
    StatementControl* control = nullptr;
    void Run(Point point, std::string_view) override {
        if (point == Point::kBeforeCatalogRefresh && control != nullptr) (void)control->RequestStop(StatementStopReason::kCancelled);
    }
};

void TestCancelledStatementKeepsWatch() {
    EngineFixture fixture;
    auto watcher = fixture.Watcher();
    const auto whole = CatalogPush(NextBytes(*watcher->catalog_watch));
    assert(HasEvent(whole, "table", "create", "default"));
    auto ddl = fixture.Session();
    StopBeforeRefresh hook;
    fixture.engine->SetHookForTests(&hook);
    StatementControl control(StatementControl::Clock::now(), 0U);
    assert(control.Start());
    hook.control = &control;
    {
        // A statement that fails before its decision, then is cancelled
        // while the refresh reads the table it named.
        const ScopedStatementControl scope(&control);
        assert(fixture.Run(*ddl, "ALTER TABLE default DROP COLUMN nosuch").starts_with("-ERR "));
    }
    hook.control = nullptr;
    fixture.engine->SetHookForTests(nullptr);
    assert(!watcher->catalog_watch->Next());
    assert(fixture.Run(*ddl, "CREATE TABLE after_stop (n u8) CHUNK 4 x 4") == "+OK\r\n");
    const auto created = CatalogPush(NextBytes(*watcher->catalog_watch));
    assert(created.revision == whole.revision + 1U && HasEvent(created, "table", "create", "after_stop"));
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
    const auto whole = CatalogPush(NextBytes(*late->catalog_watch));
    assert(HasEvent(whole, "table", "create", "realm") && HasEvent(whole, "table", "create", "default"));
    assert(fixture.Run(*session, "CREATE TABLE later (n u8) CHUNK 2 x 2") == "+OK\r\n");
    assert(HasEvent(CatalogPush(NextBytes(*watcher->catalog_watch)), "table", "create", "later"));
    assert(HasEvent(CatalogPush(NextBytes(*late->catalog_watch)), "table", "create", "later"));
}

void TestConcurrentStatements() {
    EngineFixture fixture;
    auto watcher = fixture.Watcher();
    auto first = CatalogPush(NextBytes(*watcher->catalog_watch));
    std::map<std::string, std::string> tables;
    std::uint64_t revision = first.revision;
    const auto apply = [&](const Push& push) {
        // Every event moves from the state of the previous one.
        assert(push.revision == revision + 1U);
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
    const auto renamed = CatalogPush(NextBytes(*watcher->catalog_watch));
    assert(renamed.events.size() == 2U && HasEvent(renamed, "table", "drop", "a") && HasEvent(renamed, "table", "create", "c"));
#ifndef _WIN32
    assert(fixture.Run(*session, "SWAP TABLES b, c") == "+OK\r\n");
    const auto swapped = CatalogPush(NextBytes(*watcher->catalog_watch));
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

}  // namespace

int main() {
    TestNoSubscriberReadsNothing();
    TestWholeCatalogThenChanges();
    TestUsersHiddenWithoutManagesUsers();
    TestUnknownTableKeepsRecords();
    TestOverflowSendsResyncAndWholeCatalog();
    TestFailureEndsWatches();
    TestCancelledStatementKeepsWatch();
    TestTableNeedingRecoveryKeepsWatch();
    TestConcurrentStatements();
    TestRenameAndSwap();
    TestResyncWithSmallBudget();
    TestSecondWatchRefused();
}
