#include <cassert>
#include <condition_variable>
#include <future>
#include <fstream>
#include <mutex>
#include <thread>

#include "migrations_test_utils.hpp"
#include "catalog_test_utils.hpp"
#include "../src/chunk_store_internal.hpp"
#include "../src/cql.hpp"
#include "../src/feed_slot_records.hpp"
#include "../src/feed_slots.hpp"
#include "../src/feature_flags.hpp"
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
        Error(e.Run("MIGRATE 'create_realm' create TABLE realm (v u16 REQUIRED) CHUNK 2 x 2 LARGE 1 x 1"), "MIGRATION_MISMATCH");
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

void ConditionalDdl() {
    test::ScopedTempDir dir("chunkdb-migrations-conditional");
    Engine e(dir.path());
    const std::string create = "CREATE TABLE IF NOT EXISTS realm (v u16 REQUIRED) CHUNK 2 x 2 LARGE 1 x 1";
    Reply(e.Run("MIGRATE 'create' " + create), "+applied\r\n");
    Reply(e.Run("MIGRATE 'create' " + create), "+skipped\r\n");
    Reply(e.Run("MIGRATE 'add' ALTER TABLE realm ADD COLUMN IF NOT EXISTS extra u8 NULL"), "+applied\r\n");
    Reply(e.Run("MIGRATE 'add' ALTER TABLE realm ADD COLUMN IF NOT EXISTS extra u8 NULL"), "+skipped\r\n");
    Reply(e.Run("MIGRATE 'drop_column' ALTER TABLE realm DROP COLUMN IF EXISTS extra"), "+applied\r\n");
    Reply(e.Run("MIGRATE 'drop_column' ALTER TABLE realm DROP COLUMN IF EXISTS extra"), "+skipped\r\n");
    Reply(e.Run("MIGRATE 'slot' CREATE SLOT IF NOT EXISTS 'first' ON realm"), "+applied\r\n");
    Reply(e.Run("MIGRATE 'slot' CREATE SLOT IF NOT EXISTS 'first' ON realm"), "+skipped\r\n");
    assert(e.Run("SET BLOCK 0 0 IN realm v=3").front() == ':');
    const auto table = e.catalog->Find("realm");
    const auto schema = table->Info().schema;
    const auto manifest = LoadFile(dir.path() / "tables/realm/table.manifest");
    const auto slots = LoadFile(dir.path() / "tables/realm/chunkdb.slots");
    const auto chunk = e.Run("GET CHUNK 0 0 FROM realm");
    auto watch = table->SubscribeFeed();
    const auto position = watch->position();
    auto held = table->Acquire();
    const auto* store = &held->store();
    const std::vector<std::string> no_ops{
        "CREATE TABLE IF NOT EXISTS realm (bad i8 DEFAULT 1000) CHUNK 8 x 8 WITH unknown = 1",
        "ALTER TABLE realm ADD COLUMN IF NOT EXISTS v i8 DEFAULT 1000",
        "ALTER TABLE realm DROP COLUMN IF EXISTS absent",
        "CREATE SLOT IF NOT EXISTS 'first' ON realm",
        "DROP SLOT IF EXISTS 'absent' ON realm",
        "DROP TABLE IF EXISTS absent"};
    for (std::size_t i = 0; i < no_ops.size(); ++i) {
        const auto text = "MIGRATE 'noop_" + std::to_string(i) + "' " + no_ops[i];
        auto call = std::async(std::launch::async, [&] { return e.Run(text); });
        assert(call.wait_for(10s) == std::future_status::ready);
        Reply(call.get(), "+applied\r\n");
        Reply(e.Run(text), "+skipped\r\n");
    }
    assert(&held->store() == store);
    assert(table->Info().schema == schema && table == e.catalog->Find("realm"));
    assert(LoadFile(dir.path() / "tables/realm/table.manifest") == manifest);
    assert(LoadFile(dir.path() / "tables/realm/chunkdb.slots") == slots);
    assert(watch->position() == position && !watch->Next());
    held.reset();
    assert(e.Run("GET CHUNK 0 0 FROM realm") == chunk);
    assert(e.catalog->Migrations().size() == 10U);
    Error(e.Run("MIGRATE 'noop_0' DROP TABLE IF EXISTS absent"), "MIGRATION_MISMATCH");
    assert(e.Run("SET BLOCK 0 0 IN realm v=4").front() == ':');
    // SET queues publication; the feed sender appends the event asynchronously.
    const auto change = watch->Next(10s);
    assert(change && change->kind == FeedEntry::Kind::kChange && change->schema_version == schema.version);
    watch.reset();
    Reply(e.Run("MIGRATE 'drop_slot' DROP SLOT IF EXISTS 'first' ON realm"), "+applied\r\n");
    Reply(e.Run("MIGRATE 'drop_slot' DROP SLOT IF EXISTS 'first' ON realm"), "+skipped\r\n");
    Reply(e.Run("MIGRATE 'drop' DROP TABLE IF EXISTS realm"), "+applied\r\n");
    Reply(e.Run("MIGRATE 'drop' DROP TABLE IF EXISTS realm"), "+skipped\r\n");
    assert(!e.catalog->Find("realm"));
}

void ConditionalColumnsRefusePoisonedStore() {
    test::ScopedTempDir dir("chunkdb-migrations-conditional-poison");
    Engine e(dir.path());
    Reply(e.Run(std::string(kCreate) + " WITH durability_mode = 'fsync-wal'"), "+OK\r\n");
    assert(e.Run("SET BLOCK 0 0 IN realm v=1").front() == ':');
    {
        txn_test::ScopedEnv sync_failure("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
        txn_test::ScopedEnv rollback_failure("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1");
        Error(e.Run("SET BLOCK 0 0 IN realm v=2"), "OUTCOME_UNKNOWN");
    }
    const auto table = e.catalog->Find("realm");
    const auto refused_write = e.Run("SET BLOCK 0 0 IN realm v=3");
    Error(refused_write, "INTERNAL");
    // Internal durability details stay in the server log, outside the reply.
    const auto schema = table->Info().schema;
    const auto manifest = LoadFile(dir.path() / "tables/realm/table.manifest");
    const auto root = LoadFile(DataDirManifestPath(dir.path()));
    for (const auto* command : {
             "ALTER TABLE realm ADD COLUMN IF NOT EXISTS v i8 DEFAULT 1000",
             "ALTER TABLE realm DROP COLUMN IF EXISTS absent",
             "MIGRATE 'poison_add' ALTER TABLE realm ADD COLUMN IF NOT EXISTS v i8 DEFAULT 1000",
             "MIGRATE 'poison_drop' ALTER TABLE realm DROP COLUMN IF EXISTS absent"}) {
        Error(e.Run(command), "INTERNAL");
        assert(table->Info().schema == schema && table == e.catalog->Find("realm"));
        assert(LoadFile(dir.path() / "tables/realm/table.manifest") == manifest);
        assert(LoadFile(DataDirManifestPath(dir.path())) == root);
        assert(ReadMigrationRecords(dir.path()).empty() && !ReadMigrationJournal(dir.path()));
        assert(!std::filesystem::exists(dir.path() / kMigrationsFileName));
    }
}

void ConditionalJournalValidationAndRecovery() {
    const std::vector<std::string> statements{
        "CREATE TABLE IF NOT EXISTS realm (ignored i8 DEFAULT 1000) CHUNK 8 x 8",
        "ALTER TABLE realm ADD COLUMN IF NOT EXISTS v u8",
        "ALTER TABLE realm DROP COLUMN IF EXISTS absent",
        "CREATE SLOT IF NOT EXISTS 'first' ON realm",
        "DROP SLOT IF EXISTS 'absent' ON realm",
        "DROP TABLE IF EXISTS absent"};
    for (const auto& statement : statements) {
        test::ScopedTempDir dir("chunkdb-migrations-conditional-recovery");
        StoreId id{};
        std::vector<std::uint8_t> manifest, slots;
        {
            Engine e(dir.path());
            Reply(e.Run(kCreate), "+OK\r\n");
            Reply(e.Run("CREATE SLOT 'first' ON realm"), "+OK\r\n");
            id = e.catalog->Find("realm")->store_id();
            manifest = LoadFile(dir.path() / "tables/realm/table.manifest");
            slots = LoadFile(dir.path() / "tables/realm/chunkdb.slots");
            txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_MIGRATION_AFTER_DECISION_FAIL_ONCE", "1");
            Error(e.Run("MIGRATE 'once' " + statement), "OUTCOME_UNKNOWN");
            const auto journal = *ReadMigrationJournal(dir.path());
            assert(journal.directory_action == MigrationDirectoryAction::kNone && journal.files.size() == 1U);
            assert(journal.files[0].path == kMigrationsFileName && ReadMigrationRecords(dir.path()).empty());
            ValidateMigrationJournal(dir.path(), journal);
            const auto reject = [&](MigrationJournal bad) {
                bool refused = false;
                try { ValidateMigrationJournal(dir.path(), bad); } catch (const std::exception&) { refused = true; }
                assert(refused);
                assert(ReadMigrationRecords(dir.path()).empty());
                assert(LoadFile(dir.path() / "tables/realm/table.manifest") == manifest);
                assert(LoadFile(dir.path() / "tables/realm/chunkdb.slots") == slots);
            };
            auto bad = journal; bad.data_dir_id[0] ^= 1U; reject(bad);
            bad = journal; bad.table_id[0] ^= 1U; reject(bad);
            bad = journal; bad.files.front().after.back() ^= 1U; reject(bad);
            bad = journal; bad.files.insert(bad.files.begin(), {"chunkdb.users", std::nullopt, {}}); reject(bad);
            // Reencode the exact append so these cases reach pending-statement
            // and existence validation, rather than failing a ledger mismatch.
            const auto changed_statement = [&](const std::string& text) {
                auto altered = journal; altered.record.statement = text;
                altered.files.back().after = EncodeMigrationRecords(altered.data_dir_id, {altered.record});
                reject(std::move(altered));
            };
            changed_statement("CREATE TABLE realm (v u16) CHUNK 2 x 2");
            changed_statement("ALTER TABLE realm ADD COLUMN IF NOT EXISTS absent u8");
            changed_statement("ALTER TABLE realm DROP COLUMN IF EXISTS v");
            changed_statement("CREATE SLOT IF NOT EXISTS 'absent' ON realm");
            changed_statement("DROP SLOT IF EXISTS 'first' ON realm");
            changed_statement("DROP TABLE IF EXISTS realm");
#ifndef _WIN32
            if (statement.find("SLOT") != std::string::npos) {
                const auto real = dir.path() / "tables/realm/chunkdb.slots";
                const auto saved = dir.path() / "saved.slots";
                std::filesystem::rename(real, saved);
                std::filesystem::create_symlink(saved, real);
                bool refused = false;
                try { ValidateMigrationJournal(dir.path(), journal); } catch (const std::exception&) { refused = true; }
                assert(refused && ReadMigrationRecords(dir.path()).empty());
                std::filesystem::remove(real); std::filesystem::rename(saved, real);
                ValidateMigrationJournal(dir.path(), journal);
            }
#endif
            Error(e.Run("CREATE TABLE IF NOT EXISTS realm (v u8) CHUNK 2 x 2"), "INTERNAL");
            Error(e.Run("SHOW MIGRATIONS"), "INTERNAL");
        }
        Engine recovered(dir.path());
        assert(!ReadMigrationJournal(dir.path()) && recovered.catalog->Migrations().size() == 1U);
        assert(recovered.catalog->Find("realm")->store_id() == id);
        assert(LoadFile(dir.path() / "tables/realm/table.manifest") == manifest);
        assert(LoadFile(dir.path() / "tables/realm/chunkdb.slots") == slots);
        Reply(recovered.Run("MIGRATE 'once' " + statement), "+skipped\r\n");
    }
}

void LedgerLimit(bool bytes_limit) {
    test::ScopedTempDir dir("chunkdb-migrations-ledger-limit");
    { Engine e(dir.path()); (void)test::CreateBitsTable(*e.catalog, txn_test::Config({}).geometry); }
    auto manifest = *ReadDataDirManifest(dir.path());
    manifest.features.incompat |= kFeatureMigrations;
    const auto save = [&](const std::filesystem::path& file, const std::vector<std::uint8_t>& bytes) {
        std::ofstream output(file, std::ios::binary);
        output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        assert(output.good());
    };
    save(DataDirManifestPath(dir.path()), SerializeDataDirManifest(manifest));
    const std::string statement = "ALTER TABLE default SET checkpoint_updates = 512";
    std::vector<MigrationRecord> records;
    if (bytes_limit) {
        // Exact 16 MiB ledger: 32-byte header/checksum and 256 records, whose
        // names are all five bytes and whose other metadata takes 20 bytes.
        for (std::size_t i = 0; i < 256U; ++i) {
            const auto name = "b" + std::to_string(1000U + i);
            std::string text = statement;
            text.insert(text.find(" SET"), 65511U - text.size(), ' ');
            if (i == 0U) text.erase(text.find(" SET") - 32U, 32U);
            records.push_back({name, 1U, "", std::move(text)});
        }
    } else {
        for (std::size_t i = 0; i < 16384U; ++i)
            records.push_back({"r" + std::to_string(i), 1U, "", statement});
    }
    const auto encoded = EncodeMigrationRecords(manifest.data_dir_id, records);
    if (bytes_limit) assert(encoded.size() == kMaxMigrationRecordsBytes);
    save(dir.path() / kMigrationsFileName, encoded);
    Engine e(dir.path());
    const auto before = e.catalog->Find("default")->Info();
    const auto table_image = LoadFile(dir.path() / "tables/default/table.manifest");
    Reply(e.Run("MIGRATE '" + records.front().name + "' " + records.front().statement), "+skipped\r\n");
    Error(e.Run("MIGRATE '" + records.front().name + "' ALTER TABLE default SET checkpoint_updates = 513"), "MIGRATION_MISMATCH");
    const auto reply = e.Run("MIGRATE 'overflow' ALTER TABLE default ADD COLUMN extra u8 NULL");
    Error(reply, "OUT_OF_RANGE");
    assert(reply.find(bytes_limit ? "16777216" : "16384") != std::string::npos);
    assert(reply.find(bytes_limit ? "byte limit" : "record limit") != std::string::npos);
    const auto after = e.catalog->Find("default")->Info();
    assert(after.schema.version == before.schema.version && after.schema.columns == before.schema.columns);
    assert(LoadFile(dir.path() / "tables/default/table.manifest") == table_image);
    assert(LoadFile(dir.path() / kMigrationsFileName) == encoded);
    assert(e.catalog->Migrations() == records && !ReadMigrationJournal(dir.path()));
    Reply(e.Run("MIGRATE '" + records.front().name + "' " + records.front().statement), "+skipped\r\n");
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

// CREATE SLOT in a migration records its caller, or OWNER; the same
// migration run again by another user is skipped and keeps that owner.
void SlotOwnersInMigrations() {
    test::ScopedTempDir dir("chunkdb-migrations-slot-owner");
    {
        Engine e(dir.path(), true);
        e.users->Create("limited", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
        e.users->Grant("limited", "realm", Right::kAdmin);
        Reply(e.Run(std::string("MIGRATE 'create_realm' ") + kCreate), "+applied\r\n");
        SessionState limited;
        assert(test::LoginOnEngine(*e.engine, limited, "limited", "pw").front() == '%');
        const auto run = [&](const std::string& text) { return e.engine->Execute(limited, text); };
        Reply(run("MIGRATE 'caller' CREATE SLOT 'caller' ON realm"), "+applied\r\n");
        Reply(run("MIGRATE 'named' CREATE SLOT 'named' ON realm OWNER bot"), "+applied\r\n");
        Reply(e.Run("MIGRATE 'caller' CREATE SLOT 'caller' ON realm"), "+skipped\r\n");
        const auto slots = e.catalog->Find("realm")->ListFeedSlots();
        assert(slots.size() == 2U && slots[0].owner == "limited" && slots[1].owner == "bot");
    }
    Engine e(dir.path(), true);
    Reply(e.Run("MIGRATE 'named' CREATE SLOT 'named' ON realm OWNER bot"), "+skipped\r\n");
    const auto slots = e.catalog->Find("realm")->ListFeedSlots();
    assert(slots.size() == 2U && slots[0].owner == "limited" && slots[1].owner == "bot");
    // IF NOT EXISTS with OWNER is a no-op only for that owner, and is checked
    // before a no-op is recorded.
    const auto records = e.catalog->Migrations().size();
    Error(e.Run("MIGRATE 'other' CREATE SLOT IF NOT EXISTS 'named' ON realm OWNER limited"), "INVALID_ARGUMENT reason=slot_filters");
    const auto long_owner = e.Run("MIGRATE 'long' CREATE SLOT IF NOT EXISTS 'named' ON realm OWNER " + std::string(64U, 'a'));
    Error(long_owner, "INVALID_ARGUMENT"); assert(long_owner.find("slot owners are user names") != std::string::npos);
    assert(e.catalog->Migrations().size() == records);
    Reply(e.Run("MIGRATE 'same' CREATE SLOT IF NOT EXISTS 'named' ON realm OWNER bot"), "+applied\r\n");
    Reply(e.Run("MIGRATE 'implicit' CREATE SLOT IF NOT EXISTS 'named' ON realm"), "+applied\r\n");
    assert(e.catalog->Migrations().size() == records + 2U && e.catalog->Find("realm")->ListFeedSlots()[1].owner == "bot");
}

struct PauseNarrowingScan : MigrationTestHook {
    std::mutex mutex;
    std::condition_variable cv;
    bool scanning = false, release = false, rewrite = false, rewritten = false;
    std::filesystem::path table_dir;
    void Run(Point point, std::string_view) override {
        std::unique_lock lock(mutex);
        if (point == Point::kBeforeNarrowingScan && !rewrite) {
            scanning = true; cv.notify_all(); cv.wait(lock, [&] { return release; });
        }
        if (point == Point::kBeforeNarrowingScan) scanning = true;
        // Between the scan and the migration's exclusive admission of the
        // table, the manifest changes under it.
        if (point == Point::kBeforeTableExclusive && rewrite && scanning && !rewritten) {
            auto manifest = *ReadStoreManifest(table_dir);
            const auto before = manifest;
            const auto pending = manifest.schema.pending;
            manifest.schema = RenameColumn(WithoutPendingNarrowing(manifest.schema), "w", "renamed");
            manifest.schema.pending = pending;
            AlignManifestSchemaDomains(&manifest, before.schema, *before.chunk_schema);
            const auto bytes = SerializeStoreManifest(manifest);
            std::ofstream out(StoreManifestPath(table_dir), std::ios::binary | std::ios::trunc);
            out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
            assert(out);
            rewritten = true;
        }
    }
    void WaitScanning() { std::unique_lock lock(mutex); assert(cv.wait_for(lock, 10s, [&] { return scanning; })); }
    void Release() { std::lock_guard lock(mutex); release = true; cv.notify_all(); }
};

void NarrowingScanOutsideCatalogAdmission() {
    test::ScopedTempDir dir("chunkdb-migrations-narrow-scan");
    Engine e(dir.path());
    Reply(e.Run(kCreate), "+OK\r\n");
    Reply(e.Run("CREATE TABLE other (v u16) CHUNK 2 x 2"), "+OK\r\n");
    assert(e.Run("SET BLOCK 0 0 IN realm v=3").front() == ':');
    PauseNarrowingScan pause;
    e.catalog->SetMigrationTestHook(&pause);
    auto narrow = std::async(std::launch::async, [&] { return e.Run("MIGRATE 'narrow' ALTER TABLE realm ALTER COLUMN v TYPE u8"); });
    pause.WaitScanning();
    assert(e.catalog->Find("realm")->Info().schema.pending);
    SessionState second;
    assert(e.engine->Execute(second, "HELLO 3").front() == '%');
    // Other tables' DDL, migrations and SHOW MIGRATIONS do not wait for the scan.
    auto others = std::async(std::launch::async, [&] {
        Reply(e.engine->Execute(second, "CREATE TABLE fresh (v u8) CHUNK 2 x 2"), "+OK\r\n");
        Reply(e.engine->Execute(second, "ALTER TABLE other ADD COLUMN extra u8 NULL"), "+OK\r\n");
        Reply(e.engine->Execute(second, "MIGRATE 'drop_fresh' DROP TABLE fresh"), "+applied\r\n");
        Reply(e.engine->Execute(second, "DROP TABLE other"), "+OK\r\n");
        assert(e.catalog->Migrations().size() == 1U);
        // Writes to the column must fit the narrower type while it is checked.
        const auto refused = e.engine->Execute(second, "SET BLOCK 0 0 IN realm v=300");
        assert(refused.starts_with("-ERR INVALID_ARGUMENT ") && refused.find("being narrowed to u8") != std::string::npos);
        assert(e.engine->Execute(second, "SET BLOCK 1 0 IN realm v=200").front() == ':');
    });
    assert(others.wait_for(10s) == std::future_status::ready);
    others.get();
    pause.Release();
    Reply(narrow.get(), "+applied\r\n");
    e.catalog->SetMigrationTestHook(nullptr);
    const auto records = e.catalog->Migrations();
    assert(records.size() == 2U && records[0].name == "drop_fresh" && records[1].name == "narrow");
    const auto schema = e.catalog->Find("realm")->Info().schema;
    assert(schema.version == 2U && !schema.pending && schema.columns[0].type.size == 8U);
    assert(!ReadMigrationJournal(dir.path()) && !ReadStoreManifest(dir.path() / "tables" / "realm")->schema.pending);
    Reply(e.Run("GET BLOCK 0 0 FROM realm COLUMNS v"), "*1\r\n:3\r\n");
    Reply(e.Run("GET BLOCK 1 0 FROM realm COLUMNS v"), "*1\r\n:200\r\n");
}

void NarrowingRefusalLeavesNoTrace() {
    test::ScopedTempDir dir("chunkdb-migrations-narrow-refused");
    Engine e(dir.path());
    Reply(e.Run("CREATE TABLE world (v u16, w u8) CHUNK 2 x 2"), "+OK\r\n");
    assert(e.Run("SET BLOCK 0 0 IN world v=3").front() == ':');
    const auto table_dir = dir.path() / "tables" / "world";
    const auto untouched = [&](std::uint64_t version) {
        const auto schema = e.catalog->Find("world")->Info().schema;
        assert(schema.version == version && !schema.pending && schema.columns[0].type.size == 16U);
        assert(ReadStoreManifest(table_dir)->schema == schema);
        assert(e.catalog->Migrations().empty() && !ReadMigrationJournal(dir.path()));
    };
    const std::string text = "MIGRATE 'narrow' ALTER TABLE world ALTER COLUMN v TYPE u8";
    assert(e.Run("SET BLOCK 1 0 IN world v=300").front() == ':');
    Error(e.Run(text), "INVALID_ARGUMENT");
    untouched(1U);
    assert(e.Run("DELETE BLOCK 1 0 FROM world").front() == ':');
    {
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_NARROW_SCAN_FAIL_ONCE", "1");
        Error(e.Run(text), "INTERNAL");
    }
    untouched(1U);
    // The schema the scan checked changes before the decision.
    PauseNarrowingScan changed;
    changed.rewrite = true;
    changed.table_dir = table_dir;
    e.catalog->SetMigrationTestHook(&changed);
    Error(e.Run(text), "INTERNAL");
    e.catalog->SetMigrationTestHook(nullptr);
    assert(changed.rewritten);
    untouched(2U);
    assert(e.catalog->Find("world")->Info().schema.columns[1].name == "renamed");
    Reply(e.Run(text), "+applied\r\n");
    assert(e.catalog->Find("world")->Info().schema.columns[0].type.size == 8U);
}

void NarrowingEndFailureFences() {
    for (const bool migrate : {false, true}) {
        test::ScopedTempDir dir("chunkdb-migrations-narrow-end");
        Engine e(dir.path());
        Reply(e.Run("CREATE TABLE world (v u16) CHUNK 2 x 2"), "+OK\r\n");
        assert(e.Run("SET BLOCK 0 0 IN world v=300").front() == ':');
        const std::string text = std::string(migrate ? "MIGRATE 'narrow' " : "") + "ALTER TABLE world ALTER COLUMN v TYPE u8";
        {
            // Removing the narrowing fails before its manifest is replaced.
            txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_NARROW_END_FAIL_ONCE", "1");
            const auto refused = e.Run(text);
            Error(refused, "INVALID_ARGUMENT");
            assert(refused.find("holds 300") != std::string::npos);
        }
        assert(!e.catalog->Find("world") && ReadStoreManifest(dir.path() / "tables" / "world")->schema.pending);
        const auto fenced = e.Run("GET BLOCK 0 0 FROM world COLUMNS v");
        Error(fenced, "INTERNAL");
        assert(fenced.find("is fenced: a column narrowing could not be ended") != std::string::npos);
        assert(e.catalog->Migrations().empty() && !ReadMigrationJournal(dir.path()));
        Reply(e.Run("RECOVER TABLE world"), "+OK\r\n");
        const auto schema = e.catalog->Find("world")->Info().schema;
        assert(schema.version == 1U && !schema.pending && schema.columns[0].type.size == 16U);
        assert(!ReadStoreManifest(dir.path() / "tables" / "world")->schema.pending);
        Reply(e.Run("GET BLOCK 0 0 FROM world COLUMNS v"), "*1\r\n:300\r\n");
        assert(e.Run("DELETE BLOCK 0 0 FROM world").front() == ':');
        Reply(e.Run(text), migrate ? "+applied\r\n" : "+OK\r\n");
        assert(e.catalog->Find("world")->Info().schema.columns[0].type.size == 8U);
    }
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
        Reply(run("SHOW MIGRATIONS"), "*0\r\n");
        Reply(e.Run("MIGRATE 'grant' GRANT READ ON realm TO limited"), "+applied\r\n");
        Reply(e.Run("MIGRATE 'revoke' REVOKE READ ON realm FROM limited"), "+applied\r\n");
        // A grant names a valid table name, so the users file it writes reads back.
        Error(e.Run("MIGRATE 'grant_con' GRANT READ ON con TO limited"), "INVALID_ARGUMENT");
        assert(e.catalog->Migrations().size() == 4U);
        e.users->Grant("limited", "realm", Right::kRead);
        Reply(e.Run("MIGRATE 'drop' DROP TABLE realm"), "+applied\r\n");
        assert(!e.users->Find("limited")->grants.contains("realm"));
        assert(e.catalog->Migrations()[1].user == "limited");
    }
    Engine e(dir.path(), true);
    Reply(e.Run("MIGRATE 'drop' DROP TABLE realm"), "+skipped\r\n");
    assert(!e.catalog->Find("realm") && !e.users->Find("limited")->grants.contains("realm"));
}

// A recorded name belongs to the table its statement names (to MANAGES USERS
// for GRANT and REVOKE): a caller without ADMIN there gets the same refusal
// whatever statement it submits, and sees only the records it could replay.
void RecordedNamesFollowRights() {
    test::ScopedTempDir dir("chunkdb-migrations-name-rights");
    Engine e(dir.path(), true);
    const auto verifier = scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations);
    e.users->Create("owner", verifier, false);
    e.users->Create("keeper", verifier, true);
    e.users->Grant("owner", "own", Right::kAdmin);
    Reply(e.Run(std::string("MIGRATE 'create_realm' ") + kCreate), "+applied\r\n");
    Reply(e.Run("MIGRATE 'create_own' CREATE TABLE own (v u8) CHUNK 2 x 2"), "+applied\r\n");
    Reply(e.Run("MIGRATE 'add_flag' ALTER TABLE realm ADD COLUMN flag u8 NULL"), "+applied\r\n");
    Reply(e.Run("MIGRATE 'grant_own' GRANT READ ON own TO keeper"), "+applied\r\n");
    SessionState owner, keeper;
    assert(test::LoginOnEngine(*e.engine, owner, "owner", "pw").front() == '%');
    assert(test::LoginOnEngine(*e.engine, keeper, "keeper", "pw").front() == '%');
    const auto as = [&](SessionState& session, const std::string& text) { return e.engine->Execute(session, text); };
    const auto refused = as(owner, "MIGRATE 'add_flag' ALTER TABLE own ADD COLUMN flag u8 NULL");
    Error(refused, "PERMISSION_DENIED");
    assert(refused.find("ALTER") == std::string::npos && refused.find("realm") == std::string::npos);
    Reply(as(owner, "MIGRATE 'add_flag' ALTER TABLE own ADD COLUMN other u8 NULL"), refused);
    Reply(as(owner, "MIGRATE 'create_realm' DROP TABLE own"), "-ERR PERMISSION_DENIED reason=permission_denied migration name 'create_realm' is recorded for a change outside your rights; ask an administrator to grant this right\r\n");
    assert(e.catalog->Find("own")->Info().schema.columns.size() == 1U && e.catalog->Migrations().size() == 4U);
    Reply(as(owner, "MIGRATE 'own_flag' ALTER TABLE own ADD COLUMN flag u8 NULL"), "+applied\r\n");
    Reply(as(owner, "MIGRATE 'own_flag' ALTER TABLE own ADD COLUMN flag u8 NULL"), "+skipped\r\n");
    Error(as(owner, "MIGRATE 'own_flag' ALTER TABLE own ADD COLUMN extra u8 NULL"), "MIGRATION_MISMATCH");
    Error(as(owner, "MIGRATE 'create_own' ALTER TABLE own ADD COLUMN extra u8 NULL"), "MIGRATION_MISMATCH");
    Reply(e.Run("MIGRATE 'add_flag' ALTER TABLE realm ADD COLUMN flag u8 NULL"), "+skipped\r\n");
    Reply(e.Run("MIGRATE 'add_more' ALTER TABLE realm ADD COLUMN more u8 NULL"), "+applied\r\n");
    Error(e.Run("MIGRATE 'own_flag' ALTER TABLE realm ADD COLUMN extra u8 NULL"), "MIGRATION_MISMATCH");
    // GRANT records answer to MANAGES USERS, not to ADMIN on the granted table.
    Error(as(owner, "MIGRATE 'grant_own' ALTER TABLE own ADD COLUMN extra u8 NULL"), "PERMISSION_DENIED");
    Reply(as(keeper, "MIGRATE 'grant_own' GRANT READ ON own TO keeper"), "+skipped\r\n");
    Error(as(keeper, "MIGRATE 'add_flag' GRANT READ ON own TO keeper"), "PERMISSION_DENIED");
    const auto names = [](const std::string& reply) {
        std::string found;
        for (std::size_t at = reply.find("$4\r\nname\r\n"); at != std::string::npos; at = reply.find("$4\r\nname\r\n", at + 1U)) {
            const auto start = reply.find("\r\n", at + 10U) + 2U;
            found += reply.substr(start, reply.find("\r\n", start) - start) + " ";
        }
        return found;
    };
    const auto owner_listing = as(owner, "SHOW MIGRATIONS");
    assert(names(owner_listing) == "create_own own_flag ");
    // The applying user shows only with MANAGES USERS.
    assert(owner_listing.find("$4\r\nuser\r\n_\r\n") != std::string::npos && owner_listing.find("owner") == std::string::npos);
    assert(e.Run("SHOW MIGRATIONS").find("$4\r\nuser\r\n$5\r\nowner\r\n") != std::string::npos);
    assert(names(as(keeper, "SHOW MIGRATIONS")) == "grant_own ");
    assert(names(e.Run("SHOW MIGRATIONS")) == "create_realm create_own add_flag grant_own own_flag add_more ");
    e.users->Revoke("owner", "own", Right::kAdmin);
    Reply(as(owner, "SHOW MIGRATIONS"), "*0\r\n");
}


// A recorded statement that no longer parses is listed and replayed only with
// ADMIN on `*`; other callers see neither it nor its text.
void UnparseableRecordNeedsEveryTable() {
    test::ScopedTempDir dir("chunkdb-migrations-unparseable");
    Engine e(dir.path(), true);
    e.users->Create("owner", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
    e.users->Grant("owner", "own", Right::kAdmin);
    Reply(e.Run("MIGRATE 'create_own' CREATE TABLE own (v u8) CHUNK 2 x 2"), "+applied\r\n");
    auto records = ReadMigrationRecords(dir.path());
    records.push_back({"hidden", records.front().applied_ms, "admin", "ALTER TABLE own SECRET own_text"});
    const auto bytes = EncodeMigrationRecords(ReadDataDirManifest(dir.path())->data_dir_id, records);
    {
        std::ofstream out(dir.path() / kMigrationsFileName, std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    }
    SessionState owner;
    assert(test::LoginOnEngine(*e.engine, owner, "owner", "pw").front() == '%');
    const auto shown = e.engine->Execute(owner, "SHOW MIGRATIONS");
    assert(shown.rfind("*1\r\n", 0) == 0 && shown.find("own_text") == std::string::npos);
    const auto refused = e.engine->Execute(owner, "MIGRATE 'hidden' ALTER TABLE own ADD COLUMN flag u8 NULL");
    Error(refused, "PERMISSION_DENIED");
    assert(refused.find("own_text") == std::string::npos);
    assert(e.Run("SHOW MIGRATIONS").find("own_text") != std::string::npos);
    Error(e.Run("MIGRATE 'hidden' ALTER TABLE own ADD COLUMN flag u8 NULL"), "MIGRATION_MISMATCH");
}


// A caller with neither MANAGES USERS nor an ADMIN grant lists nothing
// without reading the ledger.
void ListingWithoutAdminSkipsLedger() {
    test::ScopedTempDir dir("chunkdb-migrations-listing");
    Engine e(dir.path(), true);
    e.users->Create("reader", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
    e.users->Grant("reader", "own", Right::kWrite);
    Reply(e.Run("MIGRATE 'create_own' CREATE TABLE own (v u8) CHUNK 2 x 2"), "+applied\r\n");
    {
        std::ofstream out(dir.path() / kMigrationsFileName, std::ios::binary | std::ios::trunc);
        out << "not a ledger";
    }
    SessionState reader;
    assert(test::LoginOnEngine(*e.engine, reader, "reader", "pw").front() == '%');
    Reply(e.engine->Execute(reader, "SHOW MIGRATIONS"), "*0\r\n");
    assert(e.Run("SHOW MIGRATIONS").rfind("-ERR ", 0) == 0);
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

// A watch rechecks READ from the published rights view: a migration holding
// the registry lock across its journal writes does not hold up the check.
void RightCheckDuringUserMigration() {
    test::ScopedTempDir dir("chunkdb-migrations-right-check");
    Engine e(dir.path(), true);
    Reply(e.Run(kCreate), "+OK\r\n");
    e.users->Create("limited", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
    e.users->Grant("limited", "realm", Right::kRead);
    ReadRightCheck check(e.users, "limited", e.users->FindIncarnation("limited")->incarnation, "realm",
        e.users->Generation());
    e.users->Revoke("limited", "realm", Right::kRead);
    Pause pause;
    e.catalog->SetMigrationTestHook(&pause);
    auto migration = std::async(std::launch::async, [&] { return e.Run("MIGRATE 'grant' GRANT WRITE ON realm TO admin"); });
    pause.WaitPrepared();
    auto checked = std::async(std::launch::async, [&] { return check.Check(); });
    const bool prompt = checked.wait_for(10s) == std::future_status::ready;
    pause.Release();
    Reply(migration.get(), "+applied\r\n");
    e.catalog->SetMigrationTestHook(nullptr);
    assert(prompt && checked.get() == ReadRightCheck::State::kNoRight);
}

// Every statement checks that the session's user still exists from the
// published rights view: a PING completes while a migration holds the
// registry lock across its journal writes.
void PingDuringUserMigration() {
    test::ScopedTempDir dir("chunkdb-migrations-ping");
    Engine e(dir.path(), true);
    Reply(e.Run(kCreate), "+OK\r\n");
    e.users->Create("limited", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
    SessionState limited;
    assert(test::LoginOnEngine(*e.engine, limited, "limited", "pw").front() == '%');
    Reply(e.engine->Execute(limited, "PING\r\n"), "+PONG\r\n");
    e.users->Grant("limited", "realm", Right::kRead);
    Pause pause;
    e.catalog->SetMigrationTestHook(&pause);
    auto migration = std::async(std::launch::async, [&] { return e.Run("MIGRATE 'grant' GRANT WRITE ON realm TO admin"); });
    pause.WaitPrepared();
    auto ping = std::async(std::launch::async, [&] { return e.engine->Execute(limited, "PING\r\n"); });
    const bool prompt = ping.wait_for(10s) == std::future_status::ready;
    pause.Release();
    Reply(migration.get(), "+applied\r\n");
    e.catalog->SetMigrationTestHook(nullptr);
    assert(prompt);
    Reply(ping.get(), "+PONG\r\n");
}

void RebindRecoveredUsers() {
    test::ScopedTempDir dir("chunkdb-migrations-users-rebind");
    std::shared_ptr<UserRegistry> reused;
    {
        Engine e(dir.path(), true);
        reused = e.users;
        reused->Create("limited", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_MIGRATION_AFTER_DECISION_FAIL_ONCE", "1");
        Error(e.Run("MIGRATE 'grant' GRANT WRITE ON realm TO limited"), "OUTCOME_UNKNOWN");
        // Decided before completion failed: memory already holds the change.
        assert(reused->Find("limited")->grants.at("realm") == Right::kWrite);
    }
    // A registry read before catalog recovery holds the previous users file.
    // Binding must reload the committed recovery image.
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

void DropUnderNoAuthAndExplicitReplacement() {
    test::ScopedTempDir dir("chunkdb-migrations-drop-noauth");
    {
        Engine e(dir.path(), true);
        (void)test::CreateBitsTable(*e.catalog, txn_test::Config({}).geometry);
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
        // Reopening leaves the catalog empty. An explicit successor remains
        // intact when the old named migration is retried.
        e.engine.reset(); e.catalog.reset();
        auto catalog = std::make_shared<TableCatalog>(Config(dir.path()));
        assert(catalog->List().empty());
        (void)test::CreateBitsTable(*catalog, txn_test::Config({}).geometry);
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
        Reply(migration.get(), "-ERR OUTCOME_UNKNOWN stage=migration_completion reason=outcome_unknown " + std::string(kMigrationRecoveryRequiredMessage) + "\r\n");
        assert(blocked.wait_for(1s) == std::future_status::ready && blocked.get());
        e.catalog->SetMigrationTestHook(nullptr);
        assert(ReadMigrationJournal(dir.path()));
        Reply(e.Run("SHOW MIGRATIONS"), "-ERR INTERNAL reason=internal " + std::string(kMigrationRecoveryRequiredMessage) + "\r\n");
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

class LeaseDrainHook final : public MigrationTestHook {
  public:
    void Run(Point point, std::string_view name) override {
        if (point != Point::kBeforeTableExclusive || name != "realm") return;
        std::lock_guard lock(mutex_); entered_ = true; cv_.notify_all();
    }
    void Wait() { std::unique_lock lock(mutex_); assert(cv_.wait_for(lock, 10s, [&] { return entered_; })); }
  private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_ = false;
};
void UsersProgressDuringDropLeaseDrain() {
    test::ScopedTempDir dir("chunkdb-migrations-drop-lease-users");
    Engine e(dir.path(), true); Reply(e.Run(kCreate), "+OK\r\n");
    e.users->Create("limited", scram::MakeVerifier("old", crypto::RandomBytes(16), scram::kMinIterations), false);
    e.users->Grant("limited", "realm", Right::kRead);
    auto held = e.catalog->Find("realm")->Acquire(); assert(held);
    LeaseDrainHook hook; e.catalog->SetMigrationTestHook(&hook);
    auto drop = std::async(std::launch::async, [&] { return e.Run("MIGRATE 'drop' DROP TABLE realm"); }); hook.Wait();
    const auto verifier = scram::MakeVerifier("new", crypto::RandomBytes(16), scram::kMinIterations);
    auto update = std::async(std::launch::async, [&] {
        e.users->SetVerifier("limited", verifier);
        return e.users->Find("limited");
    });
    assert(update.wait_for(10s) == std::future_status::ready);
    assert(update.get()->verifier == verifier);
    // The held command may consult its user's current rights before releasing
    // its lease; the waiting migration must not hold that registry mutex.
    assert(e.users->Find("limited")->grants.at("realm") == Right::kRead);
    held.reset(); assert(drop.wait_for(10s) == std::future_status::ready); Reply(drop.get(), "+applied\r\n");
    e.catalog->SetMigrationTestHook(nullptr);
    assert(!e.users->Find("limited")->grants.contains("realm"));
    assert(e.users->Find("limited")->verifier == verifier);
    assert(e.catalog->Migrations().size() == 1U);
}

}  // namespace
int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--ledger-limit-records") { LedgerLimit(false); return 0; }
    if (argc == 2 && std::string(argv[1]) == "--ledger-limit-bytes") { LedgerLimit(true); return 0; }
    ConditionalDdl(); ConditionalColumnsRefusePoisonedStore(); ConditionalJournalValidationAndRecovery();
    UsersProgressDuringDropLeaseDrain(); GrammarAndRecords(); NarrowingAndSlots(); SlotOwnersInMigrations(); NarrowingScanOutsideCatalogAdmission(); NarrowingRefusalLeavesNoTrace(); NarrowingEndFailureFences(); RightsAndDrop(); RecordedNamesFollowRights(); UnparseableRecordNeedsEveryTable(); ListingWithoutAdminSkipsLedger(); ConcurrentNameAndDdl(); UserAndSlotFencing(); OrdinaryDropGrantFencing(); RightCheckDuringUserMigration(); PingDuringUserMigration(); RebindRecoveredUsers(); PreparationRestoreFailure(); DropUnderNoAuthAndExplicitReplacement(); UnboundMetadataRefused(); FailedDecisionAndRecovery();
    LedgerLimit(false); LedgerLimit(true);
}
