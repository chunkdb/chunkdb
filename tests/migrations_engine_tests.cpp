#include <cassert>
#include <condition_variable>
#include <future>
#include <fstream>
#include <mutex>
#include <thread>

#include "migrations_test_utils.hpp"
#include "../src/cql.hpp"
#include "../src/feed_slot_records.hpp"
#include "../src/feed_slots.hpp"
#include "../src/scram.hpp"
#include "../src/store_manifest.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::migration_test;
using namespace std::chrono_literals;

void GrammarAndRecords() {
    for (const auto* bad : {"MIGRATE x DROP TABLE t", "MIGRATE '' DROP TABLE t", "MIGRATE '../bad' DROP TABLE t",
                           "MIGRATE 'x' SET BLOCK 0 0 IN t a=1", "MIGRATE 'x' CREATE USER a VERIFIER $1",
                           "MIGRATE 'x' MIGRATE 'y' DROP TABLE t", "MIGRATE 'x' DROP TABLE t DROP TABLE u"}) {
        bool refused = false;
        try { (void)cql::Parse(bad); } catch (const cql::ParseError&) { refused = true; }
        assert(refused);
    }
    test::ScopedTempDir dir("chunkdb-migrations-engine");
    StoreId table_id;
    {
        Engine e(dir.path());
        Reply(e.Run("SHOW MIGRATIONS"), "*0\r\n");
        Reply(e.Run(std::string("MIGRATE 'create_realm' ") + kCreate + "  \r\n"), "+applied\r\n");
        table_id = e.catalog->Find("realm")->store_id();
        Reply(e.Run(std::string("MIGRATE 'create_realm' ") + kCreate), "+skipped\r\n");
        Error(e.Run("MIGRATE 'create_realm' create TABLE realm (v u16 REQUIRED) CHUNK 2 x 2 LARGE 1 x 1"), "CONFLICT");
        Reply(e.Run("MIGRATE 'add' ALTER TABLE realm ADD COLUMN extra u8 NULL"), "+applied\r\n");
        Reply(e.Run("MIGRATE 'add' ALTER TABLE realm ADD COLUMN extra u8 NULL"), "+skipped\r\n");
        assert(e.catalog->Find("realm")->Info().schema.version == 2U);
        Error(e.Run(std::string("MIGRATE 'retry' ") + kCreate), "TABLE_EXISTS");
        Reply(e.Run("MIGRATE 'retry' ALTER TABLE realm RENAME COLUMN extra TO other"), "+applied\r\n");
        const auto records = e.catalog->Migrations();
        assert(records.size() == 3U && records[0].name == "create_realm" && records[1].name == "add" && records[2].name == "retry");
        assert(records[0].statement == kCreate && records[0].user.empty() && records[0].applied_ms != 0U);
        Reply(e.Run("BEGIN"), "+OK\r\n");
        Error(e.Run("MIGRATE 'txn' DROP TABLE realm"), "INVALID_ARGUMENT");
        Reply(e.Run("ROLLBACK"), "+OK\r\n");
    }
    Engine e(dir.path());
    assert(e.catalog->Find("realm")->store_id() == table_id);
    assert(e.catalog->Find("realm")->Info().schema.version == 3U);
    Reply(e.Run("MIGRATE 'retry' ALTER TABLE realm RENAME COLUMN extra TO other"), "+skipped\r\n");
}

void NarrowingAndSlots() {
    test::ScopedTempDir dir("chunkdb-migrations-narrow-slot");
    Engine e(dir.path());
    Reply(e.Run(std::string("MIGRATE 'create_realm' ") + kCreate), "+applied\r\n");
    assert(e.Run("SET BLOCK 0 0 IN realm v=300").front() == ':');
    Error(e.Run("MIGRATE 'narrow' ALTER TABLE realm ALTER COLUMN v TYPE u8"), "INVALID_ARGUMENT");
    assert(e.catalog->Migrations().size() == 1U && e.catalog->Find("realm")->Info().schema.version == 1U);
    assert(e.Run("SET BLOCK 0 0 IN realm v=3").front() == ':');
    Reply(e.Run("MIGRATE 'narrow' ALTER TABLE realm ALTER COLUMN v TYPE u8"), "+applied\r\n");
    Reply(e.Run("MIGRATE 'narrow' ALTER TABLE realm ALTER COLUMN v TYPE u8"), "+skipped\r\n");
    assert(e.catalog->Find("realm")->Info().schema.version == 2U);
    Reply(e.Run("CREATE SLOT 'first' ON realm"), "+OK\r\n");
    auto table = e.catalog->Find("realm");
    const auto first = table->ListFeedSlots().front();
    Reply(e.Run("MIGRATE 'slot' CREATE SLOT 'second' ON realm"), "+applied\r\n");
    Reply(e.Run("MIGRATE 'slot' CREATE SLOT 'second' ON realm"), "+skipped\r\n");
    const auto slots = table->ListFeedSlots();
    assert(slots.size() == 2U && slots[0].name == "first" && slots[0].position == first.position);
    Reply(e.Run("MIGRATE 'drop_slot' DROP SLOT 'second' ON realm"), "+applied\r\n");
    Reply(e.Run("MIGRATE 'drop_slot' DROP SLOT 'second' ON realm"), "+skipped\r\n");
    assert(table->ListFeedSlots().size() == 1U);
}

void RightsAndDrop() {
    test::ScopedTempDir dir("chunkdb-migrations-rights");
    {
        Engine e(dir.path(), true);
        auto verifier = scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations);
        e.users->Create("limited", verifier, false);
        e.users->Grant("limited", "realm", Right::kAdmin);
        Reply(e.Run(std::string("MIGRATE 'create_realm' ") + kCreate), "+applied\r\n");
        SessionState limited;
        assert(test::LoginOnEngine(*e.engine, limited, "limited", "pw").front() == '%');
        const auto run = [&](const std::string& text) { return e.engine->Execute(limited, text); };
        Reply(run("MIGRATE 'add' ALTER TABLE realm ADD COLUMN extra u8 NULL"), "+applied\r\n");
        e.users->Revoke("limited", "realm", Right::kAdmin);
        Error(run("MIGRATE 'add' ALTER TABLE realm ADD COLUMN extra u8 NULL"), "PERMISSION_DENIED");
        Error(run("MIGRATE 'add' ALTER TABLE realm DROP COLUMN v"), "PERMISSION_DENIED");
        Error(run("SHOW MIGRATIONS"), "PERMISSION_DENIED");
        Reply(e.Run("MIGRATE 'grant' GRANT READ ON realm TO limited"), "+applied\r\n");
        Reply(e.Run("MIGRATE 'revoke' REVOKE READ ON realm FROM limited"), "+applied\r\n");
        e.users->Grant("limited", "realm", Right::kRead);
        Reply(e.Run("MIGRATE 'drop' DROP TABLE realm"), "+applied\r\n");
        assert(!e.users->Find("limited")->grants.contains("realm"));
        assert(e.catalog->Migrations()[1].user == "limited");
    }
    Engine e(dir.path(), true);
    Reply(e.Run("MIGRATE 'drop' DROP TABLE realm"), "+skipped\r\n");
    assert(!e.catalog->Find("realm") && !e.users->Find("limited")->grants.contains("realm"));
}

struct Pause : MigrationTestHook {
    std::mutex mutex;
    std::condition_variable cv;
    bool prepared = false, release = false;
    Point pause_at = Point::kPrepared;
    std::size_t arrivals = 0U, user_entries = 0U, table_entries = 0U;
    void Run(Point point, std::string_view) override {
        std::unique_lock lock(mutex);
        if (point == Point::kBeforeAdmission) { ++arrivals; cv.notify_all(); }
        if (point == Point::kBeforeUserUpdate) { ++user_entries; cv.notify_all(); }
        if (point == Point::kBeforeTableExclusive) { ++table_entries; cv.notify_all(); }
        if (point == pause_at && !prepared) {
            prepared = true; cv.notify_all(); cv.wait(lock, [&] { return release; });
        }
    }
    void WaitPrepared() { std::unique_lock lock(mutex); cv.wait(lock, [&] { return prepared; }); }
    void WaitSecond() { std::unique_lock lock(mutex); cv.wait(lock, [&] { return arrivals >= 2U; }); }
    void WaitAdmission() { std::unique_lock lock(mutex); cv.wait(lock, [&] { return arrivals >= 1U; }); }
    void WaitUser() { std::unique_lock lock(mutex); cv.wait(lock, [&] { return user_entries >= 1U; }); }
    void WaitTableSecond() { std::unique_lock lock(mutex); cv.wait(lock, [&] { return table_entries >= 2U; }); }
    void Release() { std::lock_guard lock(mutex); release = true; cv.notify_all(); }
};

void ConcurrentNameAndDdl() {
    test::ScopedTempDir dir("chunkdb-migrations-concurrent");
    Engine e(dir.path());
    Reply(e.Run(kCreate), "+OK\r\n");
    Pause pause;
    e.catalog->SetMigrationTestHook(&pause);
    SessionState second;
    assert(e.engine->Execute(second, "HELLO 3").front() == '%');
    const std::string text = "MIGRATE 'one' ALTER TABLE realm ADD COLUMN extra u8 NULL";
    auto a = std::async(std::launch::async, [&] { return e.Run(text); });
    pause.WaitPrepared();
    auto b = std::async(std::launch::async, [&] { return e.engine->Execute(second, text); });
    pause.WaitSecond();
    assert(b.wait_for(30ms) == std::future_status::timeout);
    auto ordinary = std::async(std::launch::async, [&] { e.catalog->ChangeColumns("realm", [](const TableSchema& schema) {
        return RenameColumn(schema, "extra", "renamed");
    }); });
    assert(ordinary.wait_for(30ms) == std::future_status::timeout);
    pause.Release();
    Reply(a.get(), "+applied\r\n"); Reply(b.get(), "+skipped\r\n"); ordinary.get();
    e.catalog->SetMigrationTestHook(nullptr);
    assert(e.catalog->Migrations().size() == 1U && e.catalog->Find("realm")->Info().schema.version == 3U);
}

void UserAndSlotFencing() {
    test::ScopedTempDir dir("chunkdb-migrations-fences");
    Engine e(dir.path(), true);
    Reply(e.Run(kCreate), "+OK\r\n");
    e.users->Create("limited", scram::MakeVerifier("old", crypto::RandomBytes(16), scram::kMinIterations), false);
    {
        Pause pause;
        e.catalog->SetMigrationTestHook(&pause);
        auto migration = std::async(std::launch::async, [&] { return e.Run("MIGRATE 'grant' GRANT WRITE ON realm TO limited"); });
        pause.WaitPrepared();
        const auto verifier = scram::MakeVerifier("new", crypto::RandomBytes(16), scram::kMinIterations);
        e.users->SetMigrationTestHook(&pause);
        auto password = std::async(std::launch::async, [&] { e.users->SetVerifier("limited", verifier); });
        pause.WaitUser();
        assert(password.wait_for(30ms) == std::future_status::timeout);
        auto show = std::async(std::launch::async, [&] { return e.catalog->Migrations(); });
        assert(show.wait_for(30ms) == std::future_status::timeout);
        pause.Release();
        Reply(migration.get(), "+applied\r\n"); password.get();
        assert(show.get().size() == 1U);
        e.users->SetMigrationTestHook(nullptr);
        const auto user = e.users->Find("limited");
        assert(user->verifier == verifier && user->grants.at("realm") == Right::kWrite);
        e.catalog->SetMigrationTestHook(nullptr);
    }
    auto table = e.catalog->Find("realm");
    const auto first = table->CreateFeedSlot("first").position;
    assert(e.Run("SET BLOCK 0 0 IN realm v=10").front() == ':');
    FeedPosition latest{first.epoch, 0U};
    { auto lease = table->Acquire(); latest.revision = lease->store().GetChunkVersion(0, 0); }
    FeedSlotTestAccess::Sync(*table);
    assert(latest.revision > first.revision);
    {
        Pause pause;
        e.catalog->SetMigrationTestHook(&pause);
        auto migration = std::async(std::launch::async, [&] { return e.Run("MIGRATE 'slot' CREATE SLOT 'second' ON realm"); });
        pause.WaitPrepared();
        auto ack = std::async(std::launch::async, [&] { table->AdvanceFeedSlot("first", latest); });
        pause.WaitTableSecond();
        assert(ack.wait_for(30ms) == std::future_status::timeout);
        pause.Release();
        Reply(migration.get(), "+applied\r\n"); ack.get();
        assert(table->ListFeedSlots()[0].position == latest && table->ListFeedSlots().size() == 2U);
        e.catalog->SetMigrationTestHook(nullptr);
    }
}

void OrdinaryDropGrantFencing() {
    test::ScopedTempDir dir("chunkdb-migrations-normal-drop");
    Engine e(dir.path(), true);
    Reply(e.Run(kCreate), "+OK\r\n");
    e.users->Create("limited", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
    e.users->Grant("limited", "realm", Right::kRead);
    SessionState second;
    assert(test::LoginOnEngine(*e.engine, second, "admin", "admin-password").front() == '%');
    Pause pause;
    pause.pause_at = MigrationTestHook::Point::kBeforeUserUpdate;
    e.catalog->SetMigrationTestHook(&pause);
    e.users->SetMigrationTestHook(&pause);
    auto drop = std::async(std::launch::async, [&] { return e.Run("DROP TABLE realm"); });
    pause.WaitPrepared();
    auto grant = std::async(std::launch::async, [&] { return e.engine->Execute(second, "MIGRATE 'grant' GRANT WRITE ON realm TO limited"); });
    pause.WaitAdmission();
    assert(grant.wait_for(30ms) == std::future_status::timeout);
    pause.Release();
    Reply(drop.get(), "+OK\r\n"); Reply(grant.get(), "+applied\r\n");
    e.users->SetMigrationTestHook(nullptr); e.catalog->SetMigrationTestHook(nullptr);
    assert(e.users->Find("limited")->grants.at("realm") == Right::kWrite);
}

void RebindRecoveredUsers() {
    test::ScopedTempDir dir("chunkdb-migrations-users-rebind");
    std::shared_ptr<UserRegistry> reused;
    {
        Engine e(dir.path(), true);
        reused = e.users;
        reused->Create("limited", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_MIGRATION_AFTER_DECISION_FAIL_ONCE", "1");
        Error(e.Run("MIGRATE 'grant' GRANT WRITE ON realm TO limited"), "INTERNAL");
    }
    // Both an existing registry and one read before catalog recovery hold the
    // previous users file. Binding must reload the committed recovery image.
    const auto before = test::MakeUsers(dir.path(), "admin", "admin-password");
    assert(!before->Find("limited")->grants.contains("realm"));
    auto catalog = std::make_shared<TableCatalog>(Config(dir.path()));
    for (const auto& users : {reused, before}) {
        EngineConfig config; config.users = users;
        CommandEngine engine(config, catalog);
        assert(users->Find("limited")->grants.at("realm") == Right::kWrite);
        users->SetVerifier("limited", scram::MakeVerifier("new", crypto::RandomBytes(16), scram::kMinIterations));
        assert(ReadUsersFile(dir.path())->users.at("limited").grants.at("realm") == Right::kWrite);
    }
}

void PreparationRestoreFailure() {
    test::ScopedTempDir dir("chunkdb-migrations-restore-failure");
    {
        Engine e(dir.path());
        Reply(e.Run(kCreate), "+OK\r\n");
        const auto table = e.catalog->Find("realm");
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_MIGRATION_RESUME_FAIL_ONCE", "1");
        Error(e.Run("MIGRATE 'bad' ALTER TABLE realm DROP COLUMN missing"), "INTERNAL");
        assert(!ReadMigrationJournal(dir.path()) && ReadMigrationRecords(dir.path()).empty());
        auto command = std::async(std::launch::async, [&] { try { (void)table->Acquire(); } catch (const std::runtime_error&) { return true; } return false; });
        assert(command.wait_for(1s) == std::future_status::ready && command.get());
    }
    Engine e(dir.path());
    assert(e.catalog->Find("realm")->Info().schema.version == 1U && e.catalog->Migrations().empty());
}

void DropUnderNoAuthAndDefaultRecreation() {
    test::ScopedTempDir dir("chunkdb-migrations-drop-noauth");
    {
        Engine e(dir.path(), true);
        Reply(e.Run(kCreate), "+OK\r\n");
        e.users->Create("limited", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
        e.users->Grant("limited", "realm", Right::kRead);
    }
    {
        Engine e(dir.path());
        Reply(e.Run("MIGRATE 'drop' DROP TABLE realm"), "+applied\r\n");
        assert(!ReadUsersFile(dir.path())->users.at("limited").grants.contains("realm"));
        const auto original = e.catalog->Find("default")->store_id();
        Reply(e.Run("MIGRATE 'default_drop' DROP TABLE default"), "+applied\r\n");
        assert(!e.catalog->Find("default"));
        // Auto-creation on an empty catalog belongs to the next open. The
        // migration names the old table identity and cannot drop its successor.
        e.engine.reset(); e.catalog.reset();
        auto catalog = std::make_shared<TableCatalog>(Config(dir.path()));
        assert(catalog->Find("default")->store_id() != original);
    }
    Engine e(dir.path());
    const auto replacement = e.catalog->Find("default")->store_id();
    Reply(e.Run("MIGRATE 'default_drop' DROP TABLE default"), "+skipped\r\n");
    assert(e.catalog->Find("default")->store_id() == replacement);
}

void UnboundMetadataRefused() {
    for (const auto filename : {kMigrationsFileName, kMigrationPendingFileName}) {
        test::ScopedTempDir dir("chunkdb-migrations-unbound");
        const auto path = dir.path() / filename;
        { std::ofstream file(path); file << "foreign metadata"; }
        bool refused = false;
        try { TableCatalog catalog(Config(dir.path())); } catch (const std::runtime_error&) { refused = true; }
        assert(refused && !std::filesystem::exists(DataDirManifestPath(dir.path())));
        assert(std::filesystem::file_size(path) == 16U);
    }
}

void FailedDecisionAndRecovery() {
    test::ScopedTempDir dir("chunkdb-migrations-failure");
    {
        Engine e(dir.path(), true);
        Reply(e.Run(kCreate), "+OK\r\n");
        const auto table = e.catalog->Find("realm");
        Pause pause;
        pause.pause_at = MigrationTestHook::Point::kAfterDecision;
        e.catalog->SetMigrationTestHook(&pause);
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_MIGRATION_AFTER_DECISION_FAIL_ONCE", "1");
        auto migration = std::async(std::launch::async, [&] { return e.Run("MIGRATE 'one' ALTER TABLE realm ADD COLUMN extra u8 NULL"); });
        pause.WaitPrepared();
        auto blocked = std::async(std::launch::async, [&] { try { (void)table->Acquire(); } catch (const std::runtime_error&) { return true; } return false; });
        assert(blocked.wait_for(30ms) == std::future_status::timeout);
        pause.Release();
        Reply(migration.get(), "-ERR INTERNAL " + std::string(kMigrationRecoveryRequiredMessage) + "\r\n");
        assert(blocked.wait_for(1s) == std::future_status::ready && blocked.get());
        e.catalog->SetMigrationTestHook(nullptr);
        assert(ReadMigrationJournal(dir.path()));
        Reply(e.Run("SHOW MIGRATIONS"), "-ERR INTERNAL " + std::string(kMigrationRecoveryRequiredMessage) + "\r\n");
        Error(e.Run("CREATE TABLE other (v u8) CHUNK 2 x 2"), "INTERNAL");
        bool refused = false;
        try { e.users->Grant("admin", "realm", Right::kRead); } catch (const std::runtime_error&) { refused = true; }
        assert(refused);
        // Destruction must work even though admission has been poisoned.
    }
    Engine e(dir.path(), true);
    assert(!ReadMigrationJournal(dir.path()));
    assert(e.catalog->Find("realm")->Info().schema.version == 2U);
    Reply(e.Run("MIGRATE 'one' ALTER TABLE realm ADD COLUMN extra u8 NULL"), "+skipped\r\n");
}

}  // namespace
int main() {
    GrammarAndRecords(); NarrowingAndSlots(); RightsAndDrop(); ConcurrentNameAndDdl(); UserAndSlotFencing(); OrdinaryDropGrantFencing(); RebindRecoveredUsers(); PreparationRestoreFailure(); DropUnderNoAuthAndDefaultRecreation(); UnboundMetadataRefused(); FailedDecisionAndRecovery();
}
