// RECOVER TABLE: every way a table is fenced, recovered online, after a
// crash, after a clean restart, and after a crash during online recovery,
// with the same result.
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <string>
#include <vector>
#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "migrations_test_utils.hpp"
#include "../src/backup.hpp"
#include "../src/change_feed.hpp"
#include "../src/chunk_store_internal.hpp"
#include "../src/store_manifest.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::migration_test;
using namespace std::chrono_literals;

// Table changes that fail to reopen the table, failed drops and creates,
// a fail-closed store and a failed feed cleanup.
const char* const kStates[] = {"reopen", "narrow", "drop", "create", "durability", "feed"};

bool Retired(const std::string& state) { return state != "durability" && state != "feed"; }

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

// What every path observes while the table is fenced; it also writes to the
// other table, which keeps serving.
void WhileFenced(Engine& e, const std::string& state) {
    const auto fenced = "-ERR INTERNAL table 'realm' is fenced: ";
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
        bool refused = false;
        test::ScopedTempDir target("chunkdb-recover-table-backup");
        try { (void)e.catalog->BackupTo(std::filesystem::canonical(target.path()), {}); }
        catch (const TableFencedError&) { refused = true; }
        assert(refused);
    } else if (state == "durability") {
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

void FailedRecoveryStaysFenced() {
    test::ScopedTempDir dir("chunkdb-recover-table-failure");
    Engine e(dir.path(), true);
    Seed(e, "durability"); Fence(e, "durability");
    {
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_RECOVER_TABLE_OPEN_FAIL_ONCE", "1");
        Error(e.Run("RECOVER TABLE realm"), "INTERNAL");
    }
    // Its store closed, the table is now out of service.
    assert(!e.catalog->Find("realm"));
    assert(e.Run("GET BLOCK 0 0 FROM realm").rfind("-ERR INTERNAL table 'realm' is fenced: ", 0U) == 0U);
    assert(e.Run("DESCRIBE realm").find("fenced") != std::string::npos);
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
    Reply(e.Run("BEGIN"), "+OK\r\n");
    Error(e.Run("RECOVER TABLE realm"), "INVALID_ARGUMENT");
    Reply(e.Run("ROLLBACK"), "+OK\r\n");
    Fence(e, "reopen");
    e.users->Create("limited", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
    e.users->Grant("limited", "realm", Right::kWrite);
    SessionState limited;
    assert(test::LoginOnEngine(*e.engine, limited, "limited", "pw").front() == '%');
    Error(e.engine->Execute(limited, "RECOVER TABLE realm"), "PERMISSION_DENIED");
    Error(e.engine->Execute(limited, "RECOVER TABLE other"), "NO_TABLE");
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
    Matrix(argv[0]); ExcludesCommands(); FailedRecoveryStaysFenced(); RefusedRecovery();
}
