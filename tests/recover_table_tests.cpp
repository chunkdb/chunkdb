// RECOVER TABLE: every way a table is fenced, recovered online, after a
// crash, after a clean restart, and after a crash during online recovery,
// with the same result.
#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <fstream>
#include <future>
#include <initializer_list>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>
#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "migrations_test_utils.hpp"
#include "../src/audit_log.hpp"
#include "../src/backup.hpp"
#include "../src/change_feed.hpp"
#include "../src/chunk_store_internal.hpp"
#include "../src/checkpoint.hpp"
#include "../src/slot_watch.hpp"
#include "../src/store_manifest.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::migration_test;
using namespace std::chrono_literals;

// Table changes that fail to reopen the table, failed drops and creates,
// a fail-closed store, a failed snapshot generation and a failed feed cleanup.
const char* const kStates[] = {"reopen", "narrow", "drop", "create", "durability", "snapshot", "feed"};

// Out of service, rather than serving reads.
bool Retired(const std::string& state) { return state != "durability" && state != "snapshot" && state != "feed"; }

constexpr const char* kFenced = "-ERR INTERNAL table 'realm' is fenced: ";

class FeedCleanupFailure final : public FeedTestHook {
  public:
    void Arm() { armed_.store(true, std::memory_order_release); }
    void Run(Point point, std::uint64_t) override {
        if (point == Point::kBeforeLingerPause && armed_.exchange(false, std::memory_order_acq_rel))
            throw std::runtime_error("injected feed cleanup failure");
    }
  private:
    std::atomic<bool> armed_{false};
};
// Tables keep a pointer to it after they are closed.
FeedCleanupFailure feed_failure;

int RunChild(const std::string& executable, const std::vector<std::string>& arguments) {
    std::string command = "\"" + executable + "\"";
    for (const auto& argument : arguments) command += " \"" + argument + "\"";
#ifdef _WIN32
    command = "\"" + command + "\"";
    return std::system(command.c_str());
#else
    const auto result = std::system(command.c_str());
    return WIFEXITED(result) ? WEXITSTATUS(result) : -1;
#endif
}

void Seed(Engine& e, const std::string& state) {
    Reply(e.Run("CREATE TABLE other (v u16) CHUNK 2 x 2 WITH durability_mode = 'fsync-wal'"), "+OK\r\n");
    assert(e.Run("SET BLOCK 0 0 IN other v=5").front() == ':');
    if (state == "create") return;
    Reply(e.Run("CREATE TABLE realm (v u16) CHUNK 2 x 2 WITH durability_mode = 'fsync-wal'"), "+OK\r\n");
    assert(e.Run("SET BLOCK 0 0 IN realm v=7").front() == ':');
    assert(e.Run("SET BLOCK 1 0 IN realm v=300").front() == ':');
}

void Fence(Engine& e, const std::string& state) {
    if (state == "reopen") {
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_TABLESET_REOPEN_FAIL_ONCE", "1");
        Error(e.Run("ALTER TABLE realm ADD COLUMN extra u8 NULL"), "INTERNAL");
    } else if (state == "narrow") {
        // The manifest keeps the narrowing in progress: opening drops it.
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_TABLESET_REOPEN_FAIL_ONCE", "1");
        Error(e.Run("ALTER TABLE realm ALTER COLUMN v TYPE u8"), "INTERNAL");
        assert(ReadStoreManifest(e.catalog->data_dir() / "tables" / "realm")->schema.pending);
    } else if (state == "drop") {
        txn_test::ScopedEnv rename("CHUNKDB_FAILPOINT_TABLE_DROP_RENAME_FAIL_ONCE", "1");
        txn_test::ScopedEnv reopen("CHUNKDB_FAILPOINT_TABLE_DROP_REOPEN_FAIL_ONCE", "1");
        Error(e.Run("DROP TABLE realm"), "INTERNAL");
    } else if (state == "create") {
        txn_test::ScopedEnv sync("CHUNKDB_FAILPOINT_TABLE_CREATE_SYNC_FAIL_ONCE", "1");
        txn_test::ScopedEnv undo("CHUNKDB_FAILPOINT_TABLE_CREATE_UNDO_FAIL_ONCE", "1");
        Error(e.Run("CREATE TABLE realm (v u16) CHUNK 2 x 2 WITH durability_mode = 'fsync-wal'"), "INTERNAL");
    } else if (state == "durability") {
        txn_test::ScopedEnv sync("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
        txn_test::ScopedEnv rollback("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1");
        Error(e.Run("SET BLOCK 0 0 IN realm v=2"), "INTERNAL");
    } else if (state == "snapshot") {
        e.catalog->Find("realm")->Acquire()->store().SetSnapshotGenerationLingerForTests(0, 0);
        txn_test::ScopedEnv end("CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_END_FAIL_ONCE", "1");
        (void)e.Run("SET BLOCK 0 0 IN realm v=2");
        (void)e.Run("FLUSH WAL");
        assert(e.catalog->Fence("realm"));
    } else {
        const auto table = e.catalog->Find("realm");
        auto subscription = table->SubscribeFeed();
        FeedTestAccess::SetHook(*table, &feed_failure);
        subscription.reset();
        feed_failure.Arm();
        FeedTestAccess::ExpireLinger(*table);
        assert(FeedTestAccess::WaitLingerExpired(*table, 10s));
    }
}

// Operation directories an interrupted create or drop leaves.
void Leftovers(const std::filesystem::path& root, std::initializer_list<const char*> names) {
    for (const auto* dir : {".chunkdb.staging", ".chunkdb.dropped"}) {
        for (const auto* name : names) {
            std::filesystem::create_directories(root / dir / name);
            std::ofstream(root / dir / name / "table.manifest") << "leftover";
        }
    }
}

// What every path observes while the table is fenced; it also writes to the
// other table, which keeps serving.
void WhileFenced(Engine& e, const std::string& state) {
    const auto fenced = kFenced;
    Leftovers(e.catalog->data_dir(), {"realm.00000000000000aa"});
    assert(e.Run("SET BLOCK 1 1 IN other v=6").front() == ':');
    assert(e.Run("GET BLOCK 0 0 FROM other").find(":5\r\n") != std::string::npos);
    assert(e.Run("SHOW TABLES").find("realm") != std::string::npos);
    const auto describe = e.Run("DESCRIBE realm");
    if (state == "create") assert(describe.rfind(fenced, 0U) == 0U);
    else assert(describe.find("fenced") != std::string::npos);
    const auto read = e.Run("GET BLOCK 0 0 FROM realm");
    if (Retired(state)) {
        assert(read.rfind(fenced, 0U) == 0U && read.find("; run RECOVER TABLE realm\r\n") != std::string::npos);
        assert(e.Run("CREATE TABLE realm (v u8) CHUNK 2 x 2").rfind(fenced, 0U) == 0U);
        assert(e.Run("DROP TABLE IF EXISTS realm").rfind(fenced, 0U) == 0U);
        assert(e.Run("MIGRATE 'gone' DROP TABLE IF EXISTS realm").rfind(fenced, 0U) == 0U);
        assert(e.Run("SHOW SLOTS ON realm").rfind(fenced, 0U) == 0U);
        assert(e.Run("SHOW SLOTS").front() == '*');
        bool refused = false;
        test::ScopedTempDir target("chunkdb-recover-table-backup");
        try { (void)e.catalog->BackupTo(std::filesystem::canonical(target.path()), {}); }
        catch (const TableFencedError&) { refused = true; }
        assert(refused);
    } else if (state == "durability" || state == "snapshot") {
        assert(read.find(":2\r\n") != std::string::npos || read.find(":7\r\n") != std::string::npos);
        Error(e.Run("SET BLOCK 0 0 IN realm v=3"), "INTERNAL");
    } else {
        Reply(read, "-ERR INTERNAL " + std::string(kFeedRecoveryRequiredMessage) + "\r\n");
    }
    assert(e.catalog->Migrations().empty());
}

// The recovered or reopened table, the other table and the operation
// directories, which recovery leaves alike.
std::string Snapshot(Engine& e) {
    std::string out = e.Run("SHOW TABLES");
    for (const auto* name : {"realm", "other"}) {
        if (!e.catalog->Find(name)) { out += std::string(name) + " absent\n"; continue; }
        const auto describe = e.Run(std::string("DESCRIBE ") + name);
        assert(describe.find("fenced") == std::string::npos);
        out += describe;
        for (const auto* block : {"0 0", "1 0", "1 1"}) out += e.Run(std::string("GET BLOCK ") + block + " FROM " + name);
    }
    const auto manifest = ReadStoreManifest(e.catalog->data_dir() / "tables" / "realm");
    out += manifest && manifest->schema.pending ? "pending narrowing\n" : "no narrowing\n";
    for (const auto* dir : {".chunkdb.staging", ".chunkdb.dropped"}) {
        const auto path = e.catalog->data_dir() / dir;
        if (!std::filesystem::exists(path)) continue;
        for (const auto& entry : std::filesystem::directory_iterator(path)) out += entry.path().filename().string() + "\n";
    }
    return out;
}

void Same(const std::string& actual, const std::string& expected) {
    if (actual == expected) return;
    std::fprintf(stderr, "expected [%s], got [%s]\n", expected.c_str(), actual.c_str());
    std::abort();
}

std::string Observe(const std::filesystem::path& root) {
    Engine e(root, true);
    return Snapshot(e);
}

void Matrix(const std::string& executable) {
    for (const std::string state : kStates) {
        std::cout << "fenced state " << state << '\n' << std::flush;
        test::ScopedTempDir online("chunkdb-recover-table-online"), crashed("chunkdb-recover-table-crash"),
            restarted("chunkdb-recover-table-restart"), interrupted("chunkdb-recover-table-interrupted");
        std::string recovered;
        {
            Engine e(online.path(), true);
            Seed(e, state); Fence(e, state); WhileFenced(e, state);
            Reply(e.Run("RECOVER TABLE realm"), "+OK\r\n");
            recovered = Snapshot(e);
            Error(e.Run("RECOVER TABLE realm"), "INVALID_ARGUMENT");
        }
        Same(Observe(online.path()), recovered);
        assert(RunChild(executable, {"--crash", crashed.path().string(), state}) == 86);
        Same(Observe(crashed.path()), recovered);
        {
            Engine e(restarted.path(), true);
            Seed(e, state); Fence(e, state); WhileFenced(e, state);
        }
        Same(Observe(restarted.path()), recovered);
        assert(RunChild(executable, {"--crash-recovering", interrupted.path().string(), state}) == 86);
        Same(Observe(interrupted.path()), recovered);
    }
}

// Recovery waits for the commands running on the table, and other tables
// serve meanwhile.
void ExcludesCommands() {
    test::ScopedTempDir dir("chunkdb-recover-table-exclusive");
    Engine e(dir.path(), true);
    Seed(e, "durability"); Fence(e, "durability");
    const auto table = e.catalog->Find("realm");
    auto lease = table->Acquire();
    assert(lease);
    auto recovery = std::async(std::launch::async, [&] { e.catalog->RecoverTable("realm"); });
    assert(recovery.wait_for(100ms) == std::future_status::timeout);
    SessionState other;
    assert(test::LoginOnEngine(*e.engine, other, "admin", "admin-password").front() == '%');
    assert(e.engine->Execute(other, "GET BLOCK 0 0 FROM other").find(":5\r\n") != std::string::npos);
    lease.reset();
    assert(recovery.wait_for(10s) == std::future_status::ready);
    recovery.get();
    assert(!table->Acquire() && e.catalog->Find("realm") != table);
    assert(e.Run("SET BLOCK 0 0 IN realm v=9").front() == ':');
}

// A table that serves answers at once; a second recovery waiting for the
// first learns that the table is no longer fenced.
void ConcurrentRecovery() {
    test::ScopedTempDir dir("chunkdb-recover-table-concurrent");
    Engine e(dir.path(), true);
    Seed(e, "durability");
    {
        auto lease = e.catalog->Find("other")->Acquire();
        auto healthy = std::async(std::launch::async, [&] {
            try { e.catalog->RecoverTable("other"); } catch (const std::invalid_argument&) { return true; }
            return false;
        });
        assert(healthy.wait_for(5s) == std::future_status::ready && healthy.get());
    }
    Fence(e, "durability");
    const auto table = e.catalog->Find("realm");
    auto lease = table->Acquire();
    auto first = std::async(std::launch::async, [&] { e.catalog->RecoverTable("realm"); });
    assert(FeedTestAccess::WaitLingerDraining(*table, 5s));
    auto second = std::async(std::launch::async, [&] {
        try { e.catalog->RecoverTable("realm"); }
        catch (const std::invalid_argument& error) { return std::string(error.what()); }
        catch (const std::exception& error) { return std::string("unexpected: ") + error.what(); }
        return std::string("recovered twice");
    });
    assert(second.wait_for(100ms) == std::future_status::timeout);
    lease.reset();
    first.get();
    Same(second.get(), "table 'realm' is not fenced");
}

// A recovery that opening would refuse leaves a fail-closed store serving
// reads, and says that only a restart with other flags opens it.
void OptionMismatchKeepsStore() {
    test::ScopedTempDir dir("chunkdb-recover-table-options");
    auto config = Config(dir.path());
    config.default_option_fields = kOptionFieldCheckpointUpdates;
    auto catalog = std::make_shared<TableCatalog>(config);
    EngineConfig engine_config;
    engine_config.require_auth = false;
    CommandEngine engine(engine_config, catalog);
    SessionState session;
    assert(engine.Execute(session, "HELLO 3").front() == '%');
    const auto run = [&](const std::string& line) { return engine.Execute(session, line); };
    Reply(run("CREATE TABLE realm (v u16) CHUNK 2 x 2 WITH durability_mode = 'fsync-wal'"), "+OK\r\n");
    assert(run("SET BLOCK 0 0 IN realm v=7").front() == ':');
    {
        txn_test::ScopedEnv sync("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
        txn_test::ScopedEnv rollback("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1");
        Error(run("SET BLOCK 0 0 IN realm v=2"), "INTERNAL");
    }
    const auto table_dir = dir.path() / "tables" / "realm";
    auto manifest = *ReadStoreManifest(table_dir);
    auto options = DecodeTableOptions(manifest.options);
    options.checkpoint_update_interval += 1U;
    manifest.options = EncodeTableOptions(options);
    AtomicWrite(StoreManifestPath(table_dir), SerializeStoreManifest(manifest), true, true);
    const auto refused = run("RECOVER TABLE realm");
    assert(refused.rfind("-ERR INVALID_ARGUMENT server option flags differ", 0U) == 0U);
    assert(refused.find("--checkpoint-updates") != std::string::npos && refused.find("Restart the server") != std::string::npos);
    assert(catalog->Find("realm") && catalog->Fence("realm"));
    assert(run("GET BLOCK 0 0 FROM realm").front() == '*');
}

// A backup and a recovery exclude each other: the backup copies the
// recovered table or refuses, never leaves it out.
void BackupDuringRecovery() {
    class PausePin final : public BackupTestHook {
      public:
        void Run(Point point, std::string_view table, std::uint64_t) override {
            if (point != Point::kBeforeTablePin || table != "realm") return;
            std::unique_lock lock(mutex_);
            reached_ = true; cv_.notify_all();
            cv_.wait(lock, [&] { return released_; });
        }
        void WaitReached() { std::unique_lock lock(mutex_); assert(cv_.wait_for(lock, 10s, [&] { return reached_; })); }
        void Release() { std::lock_guard lock(mutex_); released_ = true; cv_.notify_all(); }
      private:
        std::mutex mutex_;
        std::condition_variable cv_;
        bool reached_ = false, released_ = false;
    } pause;
    for (const std::string state : {"durability", "snapshot"}) {
        test::ScopedTempDir dir("chunkdb-recover-table-backup-race"), target("chunkdb-recover-table-backup-target");
        Engine e(dir.path(), true);
        Seed(e, state); Fence(e, state);
        PausePin hook;
        e.catalog->SetBackupHookForTests(&hook);
        auto backup = std::async(std::launch::async, [&] {
            try {
                const auto result = e.catalog->BackupTo(std::filesystem::canonical(target.path()), {});
                const bool copied = std::any_of(result.tables.begin(), result.tables.end(), [](const auto& cut) { return cut.name == "realm"; });
                return std::string(copied ? "copied" : "missing");
            } catch (const std::exception&) { return std::string("refused"); }
        });
        hook.WaitReached();
        auto recovery = std::async(std::launch::async, [&] { e.catalog->RecoverTable("realm"); });
        assert(recovery.wait_for(200ms) == std::future_status::timeout);
        hook.Release();
        assert(backup.get() != "missing");
        recovery.get();
        e.catalog->SetBackupHookForTests(nullptr);
        assert(e.Run("SET BLOCK 0 0 IN realm v=9").front() == ':');
    }
    (void)pause;
}

// Watches of a fenced table end saying it is fenced; those of a table
// recovered while it served reads end saying it was recovered.
void WatchesEnd() {
    const auto end_reply = [](FeedSubscription& feed) {
        const auto entry = feed.Next(10s);
        assert(entry && entry->kind == FeedEntry::Kind::kEnd && entry->protocol_frame);
        return *entry->protocol_frame;
    };
    {
        test::ScopedTempDir dir("chunkdb-recover-table-watch-fenced");
        Engine e(dir.path(), true);
        Seed(e, "reopen");
        auto feed = e.catalog->Find("realm")->SubscribeFeed();
        Fence(e, "reopen");
        Same(end_reply(*feed), "-ERR INTERNAL watched table is fenced; watch it again after RECOVER TABLE\r\n");
    }
    test::ScopedTempDir dir("chunkdb-recover-table-watch-recovered");
    Engine e(dir.path(), true);
    Seed(e, "snapshot");
    Reply(e.Run("CREATE SLOT 'consumer' ON realm"), "+OK\r\n");
    const auto table = e.catalog->Find("realm");
    auto feed = table->SubscribeFeed();
    auto slot = SlotWatch::Create(table, "consumer", FeedOptions{});
    slot->SetQuota(1U << 20U);
    slot->Activate();
    Fence(e, "snapshot");
    Reply(e.Run("RECOVER TABLE realm"), "+OK\r\n");
    const std::string recovered = "-ERR INTERNAL watched table was recovered; watch it again to resume\r\n";
    Same(end_reply(*feed), recovered);
    for (int i = 0; i < 1000 && !slot->Finished(); ++i) slot->WorkStep();
    std::string last;
    while (auto output = slot->Take(1U << 20U)) last = *output->bytes;
    Same(last, recovered);
    slot.reset();
    // The slot keeps its position on the recovered table.
    assert(e.catalog->Find("realm")->ListFeedSlots().front().name == "consumer");
}

// Recovery removes only the table's own interrupted operations; opening the
// catalog removes all of them.
void LeftoverFilter() {
    test::ScopedTempDir dir("chunkdb-recover-table-leftovers");
    const auto exists = [&](const char* name) {
        return std::filesystem::exists(dir.path() / ".chunkdb.staging" / name) ||
               std::filesystem::exists(dir.path() / ".chunkdb.dropped" / name);
    };
    {
        Engine e(dir.path(), true);
        Seed(e, "reopen"); Fence(e, "reopen");
        Leftovers(dir.path(), {"realm.00000000000000aa", "realm2.00000000000000bb", "other.00000000000000cc"});
        Reply(e.Run("RECOVER TABLE realm"), "+OK\r\n");
        assert(!exists("realm.00000000000000aa"));
        assert(exists("realm2.00000000000000bb") && exists("other.00000000000000cc"));
    }
    Engine e(dir.path(), true);
    assert(!exists("realm2.00000000000000bb") && !exists("other.00000000000000cc"));
}

// An applied record still answers while its table is fenced.
void AppliedMigrationWhileFenced() {
    test::ScopedTempDir dir("chunkdb-recover-table-ledger");
    Engine e(dir.path(), true);
    Seed(e, "reopen");
    Reply(e.Run("MIGRATE 'later' ALTER TABLE realm ADD COLUMN later u8 NULL"), "+applied\r\n");
    Fence(e, "reopen");
    Reply(e.Run("MIGRATE 'later' ALTER TABLE realm ADD COLUMN later u8 NULL"), "+skipped\r\n");
    Error(e.Run("MIGRATE 'later' ALTER TABLE realm ADD COLUMN other u8 NULL"), "CONFLICT");
    assert(e.Run("MIGRATE 'next' ALTER TABLE realm ADD COLUMN next u8 NULL").rfind(kFenced, 0U) == 0U);
}

void FailedRecoveryStaysFenced() {
    test::ScopedTempDir dir("chunkdb-recover-table-failure");
    Engine e(dir.path(), true);
    Seed(e, "durability"); Fence(e, "durability");
    {
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_RECOVER_TABLE_OPEN_FAIL_ONCE", "1");
        Error(e.Run("RECOVER TABLE realm"), "INTERNAL");
    }
    // Its store closed, the table is now out of service, and says so.
    assert(!e.catalog->Find("realm"));
    Same(e.Run("GET BLOCK 0 0 FROM realm"), std::string(kFenced) + "a recovery could not reopen it; run RECOVER TABLE realm\r\n");
    assert(e.Run("DESCRIBE realm").find("a recovery could not reopen it") != std::string::npos);
    Reply(e.Run("RECOVER TABLE realm"), "+OK\r\n");
    assert(e.Run("SET BLOCK 0 0 IN realm v=9").front() == ':');
}

void RefusedRecovery() {
    test::ScopedTempDir dir("chunkdb-recover-table-refused");
    Engine e(dir.path(), true);
    Seed(e, "reopen");
    Error(e.Run("RECOVER TABLE realm"), "INVALID_ARGUMENT");
    Error(e.Run("RECOVER TABLE absent"), "NO_TABLE");
    Error(e.Run("RECOVER TABLE"), "SYNTAX");
    Error(e.Run("RECOVER realm"), "SYNTAX");
    Fence(e, "reopen");
    Reply(e.Run("BEGIN"), "+OK\r\n");
    Error(e.Run("RECOVER TABLE realm"), "INVALID_ARGUMENT");
    Reply(e.Run("ROLLBACK"), "+OK\r\n");
    assert(e.catalog->Fence("realm"));
    e.users->Create("limited", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
    e.users->Grant("limited", "realm", Right::kWrite);
    e.users->Create("outsider", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
    e.users->Grant("outsider", "other", Right::kAdmin);
    SessionState limited, outsider;
    assert(test::LoginOnEngine(*e.engine, limited, "limited", "pw").front() == '%');
    assert(test::LoginOnEngine(*e.engine, outsider, "outsider", "pw").front() == '%');
    Error(e.engine->Execute(limited, "RECOVER TABLE realm"), "PERMISSION_DENIED");
    Error(e.engine->Execute(limited, "RECOVER TABLE other"), "NO_TABLE");
    // A user without rights on a fenced table learns nothing about it.
    for (const auto* statement : {"RECOVER TABLE %", "DESCRIBE %", "GET BLOCK 0 0 FROM %", "SHOW SLOTS ON %", "WATCH %"}) {
        auto fenced = std::string(statement), absent = fenced;
        fenced.replace(fenced.find('%'), 1U, "realm");
        absent.replace(absent.find('%'), 1U, "absent");
        auto expected = e.engine->Execute(outsider, absent);
        expected.replace(expected.find("absent"), 6U, "realm");
        Same(e.engine->Execute(outsider, fenced), expected);
    }
    assert(e.engine->Execute(outsider, "SHOW TABLES").find("realm") == std::string::npos);
    Reply(e.Run("RECOVER TABLE realm"), "+OK\r\n");
    // An unknown migration outcome fences the catalog: only a restart
    // completes it.
    {
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_MIGRATION_AFTER_DECISION_FAIL_ONCE", "1");
        Error(e.Run("MIGRATE 'pending' ALTER TABLE realm ADD COLUMN later u8 NULL"), "INTERNAL");
    }
    Reply(e.Run("RECOVER TABLE realm"), "-ERR INTERNAL " + std::string(kMigrationRecoveryRequiredMessage) + "\r\n");
    bool refused = false;
    try { e.catalog->RecoverTable("realm"); } catch (const MigrationRecoveryRequiredError&) { refused = true; }
    assert(refused);
}

// RECOVER TABLE is audited as recover_table with its table.
void Audited() {
    test::ScopedTempDir dir("chunkdb-recover-table-audit"), logs("chunkdb-recover-table-audit-log");
    Engine e(dir.path(), true);
    Seed(e, "reopen"); Fence(e, "reopen");
    EngineConfig config;
    config.require_auth = true;
    config.users = e.users;
    const auto path = logs.path() / "audit.log";
    config.audit = std::make_shared<AuditLog>(path);
    e.engine = std::make_unique<CommandEngine>(config, e.catalog);
    e.session = SessionState{};
    assert(test::LoginOnEngine(*e.engine, e.session, "admin", "admin-password").front() == '%');
    Reply(e.Run("RECOVER TABLE realm"), "+OK\r\n");
    Error(e.Run("RECOVER TABLE realm"), "INVALID_ARGUMENT");
    std::ifstream in(path);
    std::vector<std::string> lines;
    for (std::string line; std::getline(in, line);)
        if (line.find("\"event\":\"recover_table\"") != std::string::npos) lines.push_back(line);
    assert(lines.size() == 2U);
    assert(lines[0].find("\"ok\":true") != std::string::npos && lines[0].find("\"table\":\"realm\"") != std::string::npos);
    assert(lines[1].find("\"ok\":false") != std::string::npos && lines[1].find("\"error_code\":\"INVALID_ARGUMENT\"") != std::string::npos);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && (std::string(argv[1]) == "--crash" || std::string(argv[1]) == "--crash-recovering")) {
        Engine e(argv[2], true);
        Seed(e, argv[3]); Fence(e, argv[3]); WhileFenced(e, argv[3]);
        if (std::string(argv[1]) == "--crash") std::_Exit(86);
        txn_test::ScopedEnv crash("CHUNKDB_FAILPOINT_CRASH_RECOVER_TABLE_AFTER_CLOSE_ONCE", "1");
        (void)e.Run("RECOVER TABLE realm");
        return 3;  // The failpoint must have ended the process.
    }
    const std::string only = argc == 3 && std::string(argv[1]) == "--only" ? argv[2] : "";
    if (only.empty() || only == "matrix") Matrix(argv[0]);
    for (const auto& [name, run] : std::initializer_list<std::pair<const char*, void (*)()>>{
             {"excludes commands", ExcludesCommands}, {"concurrent recovery", ConcurrentRecovery},
             {"option mismatch", OptionMismatchKeepsStore}, {"backup during recovery", BackupDuringRecovery},
             {"watches end", WatchesEnd}, {"leftover filter", LeftoverFilter},
             {"applied migration", AppliedMigrationWhileFenced}, {"failed recovery", FailedRecoveryStaysFenced},
             {"refused recovery", RefusedRecovery}, {"audited", Audited}}) {
        if (!only.empty() && only != name) continue;
        std::cout << name << '\n' << std::flush;
        run();
    }
}
