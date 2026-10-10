#include <cassert>
#include <condition_variable>
#include <future>
#include <mutex>
#include <sstream>

#include "migrations_test_utils.hpp"
#include "../src/backup.hpp"
#include "../src/checkpoint.hpp"
#include "../src/migrations.hpp"
#include "../src/store_manifest.hpp"
#include "../src/verify.hpp"

namespace chunkdb {
struct BackupTestAccess {
    static void WaitMetadata(TableCatalog& catalog, bool shared) {
        auto& gate = catalog.backup_metadata_gate_;
        std::unique_lock lock(gate.mutex_);
        assert(gate.cv_.wait_for(lock, std::chrono::seconds(10), [&] {
            return shared ? gate.shared_waiters_ != 0U : gate.exclusive_waiters_ != 0U;
        }));
    }
};
}
namespace {
using namespace chunkdb;
using namespace chunkdb::migration_test;
using namespace std::chrono_literals;

class BackupPause final : public BackupTestHook {
  public:
    explicit BackupPause(Point point, std::string table = {}) : point_(point), table_(std::move(table)) {}
    void Run(Point point, std::string_view table, std::uint64_t) override {
        if (point != point_ || (!table_.empty() && table != table_)) return;
        std::unique_lock lock(mutex_);
        if (entered_) return;
        entered_ = true; cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
    }
    void Wait() { std::unique_lock lock(mutex_); assert(cv_.wait_for(lock, 10s, [&] { return entered_; })); }
    void Release() { std::lock_guard lock(mutex_); released_ = true; cv_.notify_all(); }
  private:
    Point point_;
    std::string table_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_ = false, released_ = false;
};
class MigrationPause final : public MigrationTestHook {
  public:
    MigrationPause(Point point, std::string name) : point_(point), name_(std::move(name)) {}
    void Run(Point point, std::string_view name) override {
        if (point != point_ || name != name_) return;
        std::unique_lock lock(mutex_);
        entered_ = true; cv_.notify_all();
        if (point == Point::kPrepared) cv_.wait(lock, [&] { return released_; });
    }
    void Wait() { std::unique_lock lock(mutex_); assert(cv_.wait_for(lock, 10s, [&] { return entered_; })); }
    void Release() { std::lock_guard lock(mutex_); released_ = true; cv_.notify_all(); }
  private:
    Point point_;
    std::string name_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_ = false, released_ = false;
};
MigrationRequest Add(std::string name, std::string column) {
    MigrationRequest request;
    request.kind = MigrationRequest::Kind::kAlter;
    request.table = "realm";
    request.record.name = std::move(name);
    request.record.statement = "ALTER TABLE realm ADD COLUMN " + column + " u8 NULL";
    request.alter = [column = std::move(column)](StoreManifest& manifest) {
        manifest.schema = AddColumn(manifest.schema, Column{.name = column, .type = {ColumnKind::kUnsigned, 8U}, .nullable = true, .default_value = {}});
    };
    return request;
}
template <typename T> void Ready(std::future<T>& future) { assert(future.wait_for(10s) == std::future_status::ready); }
void Validate(const std::filesystem::path& path) {
    ValidateBackupInventory(path, ReadBackupRecord(path));
    std::ostringstream findings;
    const auto verified = VerifyDataDirectory(path, findings);
    if (verified.errors) { std::fprintf(stderr, "%s", findings.str().c_str()); std::abort(); }
}

void RestoreHistory() {
    test::ScopedTempDir source("chunkdb-backup-migrations-history"), target("chunkdb-backup-migrations-copy"), restored("chunkdb-backup-migrations-restored");
    StoreId source_id;
    std::vector<MigrationRecord> records;
    {
        Engine e(source.path());
        Reply(e.Run(std::string("MIGRATE 'create_realm' ") + kCreate), "+applied\r\n");
        Reply(e.Run("MIGRATE 'add' ALTER TABLE realm ADD COLUMN extra u8 NULL"), "+applied\r\n");
        Reply(e.Run("MIGRATE 'slot' CREATE SLOT 'resume' ON realm"), "+applied\r\n");
        source_id = ReadDataDirManifest(source.path())->data_dir_id;
        records = e.catalog->Migrations();
        (void)e.catalog->BackupTo(target.path(), {});
    }
    Validate(target.path());
    assert(ReadMigrationRecords(target.path()) == records);
    RestoreBackup(target.path(), restored.path());
    assert(ReadDataDirManifest(restored.path())->data_dir_id != source_id);
    assert(ReadMigrationRecords(restored.path()) == records);
    Engine e(restored.path());
    Reply(e.Run(std::string("MIGRATE 'create_realm' ") + kCreate), "+skipped\r\n");
    Reply(e.Run("MIGRATE 'add' ALTER TABLE realm ADD COLUMN extra u8 NULL"), "+skipped\r\n");
    Reply(e.Run("MIGRATE 'slot' CREATE SLOT 'resume' ON realm"), "+skipped\r\n");
    assert(e.catalog->Find("realm")->Info().schema.version == 2U);
    assert(e.catalog->Find("realm")->ListFeedSlots().size() == 1U);
}

void MetadataCutAndCopyProgress() {
    test::ScopedTempDir source("chunkdb-backup-migrations-cut"), target("chunkdb-backup-migrations-cut-copy");
    Engine e(source.path()); Reply(e.Run(std::string("MIGRATE 'create_realm' ") + kCreate), "+applied\r\n");
    BackupPause pin(BackupTestHook::Point::kAfterPin, "realm");
    e.catalog->SetBackupHookForTests(&pin);
    auto backup = std::async(std::launch::async, [&] { return e.catalog->BackupTo(target.path(), {}); }); pin.Wait();
    auto migration = std::async(std::launch::async, [&] { return e.catalog->Migrate(Add("later", "extra"), nullptr); });
    BackupTestAccess::WaitMetadata(*e.catalog, true);
    pin.Release(); Ready(backup); (void)backup.get(); Ready(migration); assert(migration.get());
    e.catalog->SetBackupHookForTests(nullptr);
    Validate(target.path());
    assert(ReadMigrationRecords(target.path()).size() == 1U);
    assert(ReadStoreManifest(target.path()/"tables/realm")->schema.version == 1U);

    test::ScopedTempDir second("chunkdb-backup-migrations-copy-progress");
    BackupPause copy(BackupTestHook::Point::kBeforeCopy); e.catalog->SetBackupHookForTests(&copy);
    auto copying = std::async(std::launch::async, [&] { return e.catalog->BackupTo(second.path(), {}); }); copy.Wait();
    auto during_copy = std::async(std::launch::async, [&] { return e.catalog->Migrate(Add("during_copy", "more"), nullptr); });
    Ready(during_copy); assert(during_copy.get()); copy.Release(); Ready(copying); (void)copying.get();
    e.catalog->SetBackupHookForTests(nullptr);
    Validate(second.path());
    assert(ReadMigrationRecords(second.path()).size() == 2U);
    assert(ReadStoreManifest(second.path()/"tables/realm")->schema.version == 2U);
}

void GrantCoherence() {
    test::ScopedTempDir source("chunkdb-backup-migrations-grant"), target("chunkdb-backup-migrations-grant-copy");
    Engine e(source.path(), true);
    MigrationRequest grant; grant.kind = MigrationRequest::Kind::kGrant; grant.record = {"grant", 0, "admin", "GRANT READ ON realm TO admin"};
    grant.table = "realm"; grant.user = "admin"; grant.right = Right::kRead;
    MigrationPause prepared(MigrationTestHook::Point::kPrepared, "grant"); e.catalog->SetMigrationTestHook(&prepared);
    auto migration = std::async(std::launch::async, [&] { return e.catalog->Migrate(grant, e.users.get()); }); prepared.Wait();
    auto backup = std::async(std::launch::async, [&] { return e.catalog->BackupTo(target.path(), {}); });
    BackupTestAccess::WaitMetadata(*e.catalog, false);
    prepared.Release(); Ready(migration); assert(migration.get()); Ready(backup); (void)backup.get();
    e.catalog->SetMigrationTestHook(nullptr); Validate(target.path());
    assert(ReadMigrationRecords(target.path()).size() == 1U);
    assert(ReadUsersFile(target.path())->users.at("admin").grants.at("realm") == Right::kRead);
}

void CancelAdmission() {
    test::ScopedTempDir source("chunkdb-backup-migrations-cancel"), target("chunkdb-backup-migrations-cancel-copy");
    Engine e(source.path()); Reply(e.Run(kCreate), "+OK\r\n");
    BackupPause pin(BackupTestHook::Point::kAfterPin, "realm"); e.catalog->SetBackupHookForTests(&pin);
    auto backup = std::async(std::launch::async, [&] { return e.catalog->BackupTo(target.path(), {}); }); pin.Wait();
    std::stop_source stop; auto request = Add("cancelled", "extra"); request.cancelled = stop.get_token();
    auto migration = std::async(std::launch::async, [&] {
        try { (void)e.catalog->Migrate(request, nullptr); } catch (const std::exception& error) { return std::string(error.what()); }
        return std::string{};
    });
    BackupTestAccess::WaitMetadata(*e.catalog, true); stop.request_stop(); Ready(migration);
    assert(migration.get() == "migration cancelled");
    assert(e.catalog->Migrations().empty());
    pin.Release(); Ready(backup); (void)backup.get(); e.catalog->SetBackupHookForTests(nullptr); Validate(target.path());
}

void InvalidLedgerAndFencedBackup() {
    test::ScopedTempDir source("chunkdb-backup-migrations-validation"), target("chunkdb-backup-migrations-invalid"), failed("chunkdb-backup-migrations-fenced");
    Engine e(source.path()); Reply(e.Run(std::string("MIGRATE 'create_realm' ") + kCreate), "+applied\r\n");
    (void)e.catalog->BackupTo(target.path(), {});
    auto record = ReadBackupRecord(target.path());
    const auto records = ReadMigrationRecords(target.path());
    AtomicWrite(target.path()/kMigrationsFileName, EncodeMigrationRecords(NewStoreId(), records), true, true);
    for (auto& file : record.files) if (file.relative_path == kMigrationsFileName) file = InspectBackupFile(target.path(), file.relative_path);
    bool rejected = false;
    try { ValidateBackupContents(target.path(), record); } catch (const std::exception&) { rejected = true; }
    assert(rejected);
    {
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_MIGRATION_AFTER_DECISION_FAIL_ONCE", "1");
        Error(e.Run("MIGRATE 'pending' ALTER TABLE realm ADD COLUMN extra u8 NULL"), "INTERNAL");
    }
    assert(ReadMigrationJournal(source.path()));
    rejected = false;
    try { (void)e.catalog->BackupTo(failed.path(), {}); } catch (const MigrationRecoveryRequiredError&) { rejected = true; }
    assert(rejected && std::filesystem::is_empty(failed.path()));
}

class PoisonAdmission final : public MigrationTestHook {
  public:
    void Run(Point point, std::string_view name) override {
        std::unique_lock lock(mutex_);
        if (point == Point::kPrepared && name == "poison") {
            prepared_ = true; cv_.notify_all();
            cv_.wait(lock, [&] { return released_; });
        } else if (point == Point::kBeforeCatalogAdmission && name == "default") {
            admitted_ = true; cv_.notify_all();
        }
    }
    void Wait(bool prepared) {
        std::unique_lock lock(mutex_);
        assert(cv_.wait_for(lock, 10s, [&] { return prepared ? prepared_ : admitted_; }));
    }
    void Release() { std::lock_guard lock(mutex_); released_ = true; cv_.notify_all(); }
  private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool prepared_ = false, admitted_ = false, released_ = false;
};
void PoisonWhileOrdinaryDdlWaits() {
    test::ScopedTempDir source("chunkdb-backup-migrations-poison-admission");
    {
        Engine e(source.path()); Reply(e.Run(kCreate), "+OK\r\n");
        auto original = e.catalog->Find("default");
        PoisonAdmission hook; e.catalog->SetMigrationTestHook(&hook);
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_MIGRATION_AFTER_DECISION_FAIL_ONCE", "1");
        auto migration = std::async(std::launch::async, [&] {
            try { (void)e.catalog->Migrate(Add("poison", "extra"), nullptr); }
            catch (const MigrationRecoveryRequiredError&) { return true; }
            return false;
        }); hook.Wait(true);
        auto ddl = std::async(std::launch::async, [&] {
            try { e.catalog->SetOptions("default", e.catalog->default_options()); }
            catch (const MigrationRecoveryRequiredError&) { return true; }
            return false;
        }); hook.Wait(false); hook.Release();
        Ready(migration); assert(migration.get()); Ready(ddl); assert(ddl.get());
        assert(!e.catalog->Find("default"));
        bool refused = false;
        try { (void)original->Acquire(); } catch (const MigrationRecoveryRequiredError&) { refused = true; }
        assert(refused);
        e.catalog->SetMigrationTestHook(nullptr);
    }
    Engine reopened(source.path());
    assert(reopened.catalog->Find("default") && reopened.catalog->Find("default")->Info().schema.version == 1U);
    assert(reopened.catalog->Find("realm")->Info().schema.version == 2U);
    assert(reopened.catalog->Migrations().size() == 1U);
}

void OrdinaryDdlAndMigration() {
    test::ScopedTempDir source("chunkdb-backup-migrations-ddl"), target("chunkdb-backup-migrations-ddl-copy");
    Engine e(source.path()); Reply(e.Run(kCreate), "+OK\r\n");
    BackupPause pin(BackupTestHook::Point::kAfterPin, "realm"); e.catalog->SetBackupHookForTests(&pin);
    auto backup = std::async(std::launch::async, [&] { return e.catalog->BackupTo(target.path(), {}); }); pin.Wait();
    MigrationPause entering(MigrationTestHook::Point::kBeforeTableExclusive, "realm"); e.catalog->SetMigrationTestHook(&entering);
    auto ddl = std::async(std::launch::async, [&] {
        e.catalog->ChangeColumns("realm", [](const TableSchema& schema) { return AddColumn(schema, Column{.name = "ordinary", .type = {ColumnKind::kUnsigned, 8U}, .nullable = true, .default_value = {}}); });
    }); entering.Wait();
    auto migration = std::async(std::launch::async, [&] { return e.catalog->Migrate(Add("named", "named"), nullptr); });
    BackupTestAccess::WaitMetadata(*e.catalog, true);
    auto unrelated = std::async(std::launch::async, [&] { return e.catalog->Create("unrelated", e.catalog->Find("realm")->geometry().config(), e.catalog->default_options(), e.catalog->Find("realm")->Info().schema); });
    Ready(unrelated); assert(unrelated.get());
    pin.Release(); Ready(backup); (void)backup.get(); Ready(ddl); ddl.get(); Ready(migration); assert(migration.get());
    e.catalog->SetMigrationTestHook(nullptr); e.catalog->SetBackupHookForTests(nullptr);
    assert(e.catalog->Find("realm")->Info().schema.version == 3U); Validate(target.path());
}
}
int main() {
    RestoreHistory(); MetadataCutAndCopyProgress(); GrantCoherence(); CancelAdmission(); InvalidLedgerAndFencedBackup(); PoisonWhileOrdinaryDdlWaits(); OrdinaryDdlAndMigration();
    std::puts("backup migrations: 7 groups passed");
}
