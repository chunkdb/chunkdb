#include <condition_variable>
#include <future>
#include <iostream>
#include "server_slots_test_utils.hpp"
#include "backup.hpp"
#include "store_manifest.hpp"
#include "verify.hpp"
#include "slot_watch.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::slot_socket_test;

std::string Backup(const std::filesystem::path& path) {
    std::string quoted;
    for (const char c : path.string()) { quoted += c; if (c == '\'') quoted += c; }
    return "BACKUP TO '" + quoted + "'";
}
class CopyPause : public BackupTestHook {
  public:
    explicit CopyPause(Point point = Point::kBeforeCopy) : point_(point) {}
    void Run(Point point, std::string_view, std::uint64_t) override {
        if (point != point_) return;
        std::unique_lock lock(mutex_);
        if (entered_) return;
        entered_ = true; cv_.notify_all();
        if (!cv_.wait_for(lock, 10s, [&] { return released_; }))
            throw std::runtime_error("copy pause was not released");
    }
    void Wait() {
        std::unique_lock lock(mutex_);
        assert(cv_.wait_for(lock, 10s, [&] { return entered_; }));
    }
    void Release() { std::lock_guard lock(mutex_); released_ = true; cv_.notify_all(); }
  private:
    Point point_;
    std::mutex mutex_; std::condition_variable cv_;
    bool entered_ = false, released_ = false;
};
class AbortNotice : public CommandEngineTestHook {
  public:
    void Run(Point point, std::string_view) override {
        if (point != Point::kAfterBackupAborted) return;
        std::lock_guard lock(mutex_); aborted_ = true; cv_.notify_all();
    }
    void Wait() {
        std::unique_lock lock(mutex_);
        assert(cv_.wait_for(lock, 10s, [&] { return aborted_; }));
    }
  private:
    std::mutex mutex_; std::condition_variable cv_; bool aborted_ = false;
};
// Holds RESTORE TABLE after each progress report until the test releases it.
class RestoreProgressPause : public CommandEngineTestHook {
  public:
    void Run(Point point, std::string_view) override {
        if (point != Point::kAfterRestoreProgress) return;
        std::unique_lock lock(mutex_);
        const auto report = ++reports_;
        cv_.notify_all();
        if (!cv_.wait_for(lock, 10s, [&] { return released_ >= report; }))
            throw std::runtime_error("restore progress pause was not released");
    }
    void Wait(std::uint64_t report) {
        std::unique_lock lock(mutex_);
        assert(cv_.wait_for(lock, 10s, [&] { return reports_ >= report; }) && reports_ == report);
    }
    void Release(std::uint64_t report) { std::lock_guard lock(mutex_); released_ = report; cv_.notify_all(); }
  private:
    std::mutex mutex_; std::condition_variable cv_;
    std::uint64_t reports_ = 0U, released_ = 0U;
};
void Verify(const std::filesystem::path& root) {
    std::ostringstream output;
    const auto counters = VerifyDataDirectory(root, output);
    if (counters.errors || counters.warnings) std::cerr << output.str();
    assert(counters.errors == 0 && counters.warnings == 0);
}
void CopyAndResync(bool tls) {
    CopyPause pause;
    test::ScopedTempDir destinations("chunkdb-backup-protocol");
    Harness harness(tls, false, kDefaultSlotMaxBytes, 100ms, kDefaultFeedBufferBytes, destinations.path());
    const auto backup = std::filesystem::canonical(destinations.path()) / "it\'s-backup", restored = std::filesystem::canonical(destinations.path()) / "restored";
    auto writer = harness.Connect();
    writer->Ok("CREATE TABLE t (n u8) CHUNK 2 x 2");
    writer->Ok("CREATE SLOT 'consumer' ON t");
    const auto before = Number(writer->Command("SET BLOCK 0 0 IN t n = 7"));
    const auto old_epoch = StoreIdHex(harness.catalog->Find("t")->Info().store_id);
    auto client = harness.Connect();
    harness.catalog->SetBackupHookForTests(&pause);
    client->Line(Backup(backup.filename()));
    pause.Wait();
    // Copy is held after the pin phase, and both data writes and DDL finish.
    const auto after = Number(writer->Command("SET BLOCK 0 0 IN t n = 8"));
    writer->Ok("ALTER TABLE t ADD COLUMN extra u16 DEFAULT 9");
    Error(writer->Command(Backup("busy")), "BUSY");
    assert(!std::filesystem::exists(std::filesystem::canonical(destinations.path()) / "busy"));
    pause.Release();
    const auto response = client->Read();
    harness.catalog->SetBackupHookForTests(nullptr);
    const auto record = ReadBackupRecord(backup);
    assert(Number(Field(response, "tables")) == record.tables.size());
    assert(Number(Field(response, "files")) == record.files.size());
    const auto& cuts = Field(response, "cuts");
    assert(cuts.type == '*' && cuts.items.size() == record.tables.size());
    std::uint64_t cut = 0;
    for (const auto& item : cuts.items) if (Field(item, "table").value == "t") {
        cut = Number(Field(item, "revision"));
        assert(Field(item, "epoch").value == old_epoch);
    }
    assert(cut >= before && after > cut);
    Verify(backup); RestoreBackup(backup, restored); Verify(restored);
    {
        auto config = feed_test::Config(restored);
        TableCatalog catalog(config);
        auto table = catalog.Find("t");
        assert(table->Info().schema.version == 1);
        { auto lease = table->Acquire(); assert(lease->store().GetBlock(0, 0) == std::vector<ColumnValue>{std::uint64_t{7U}}); }
        const auto slots = table->ListFeedSlots();
        assert(slots.size() == 1 && slots[0].position.revision == cut && StoreIdHex(slots[0].position.epoch) != old_epoch);
        auto shared_catalog = std::shared_ptr<TableCatalog>(&catalog, [](TableCatalog*) {});
        EngineConfig engine_config; engine_config.require_auth = false;
        CommandEngine engine(engine_config, shared_catalog); SessionState session;
        (void)engine.Execute(session, "HELLO 3");
        assert(cut > 0U);
        const auto start = engine.Execute(session, "WATCH t SLOT 'consumer' AFTER " + old_epoch + " " + std::to_string(cut - 1U));
        assert(start == "%2\r\n$5\r\nepoch\r\n$32\r\n" + StoreIdHex(slots[0].position.epoch) +
            "\r\n$8\r\nrevision\r\n:" + std::to_string(cut) + "\r\n");
        session.slot_watch->Activate(); session.slot_watch->WorkStep();
        const auto resync = session.slot_watch->Take(SIZE_MAX);
        assert(resync && resync->bytes->find("resync") != std::string::npos);
        assert(resync->bytes->find("$32\r\n" + old_epoch + "\r\n:" + std::to_string(cut) + "\r\n") != std::string::npos);
        session.slot_watch->Cancel(); session.slot_watch->WorkStep(); session.slot_watch.reset();
        // The restored slot is at the cut, not at its consumer's position.
        SessionState saved; (void)engine.Execute(saved, "HELLO 3");
        assert(engine.Execute(saved, "WATCH t SLOT 'consumer'") == start);
        saved.slot_watch->Activate(); saved.slot_watch->WorkStep();
        const auto moved = saved.slot_watch->Take(SIZE_MAX);
        assert(moved && moved->bytes->find("resync") != std::string::npos);
        saved.slot_watch->Cancel(); saved.slot_watch->WorkStep(); saved.slot_watch.reset();
        SessionState exact; (void)engine.Execute(exact, "HELLO 3");
        assert(engine.Execute(exact, "WATCH t SLOT 'consumer' AFTER " + old_epoch + " " + std::to_string(cut)) == start);
        exact.slot_watch->Activate(); exact.slot_watch->WorkStep();
        assert(!exact.slot_watch->Take(SIZE_MAX));
        assert(exact.slot_watch->position() == slots[0].position);
        exact.slot_watch->Cancel(); exact.slot_watch->WorkStep(); exact.slot_watch.reset();
    }
    assert(writer->Command("PING").value == "PONG");
}
void Rights(bool tls) {
    test::ScopedTempDir destinations("chunkdb-backup-rights");
    Harness harness(tls, true, kDefaultSlotMaxBytes, 100ms, kDefaultFeedBufferBytes, destinations.path());
    auto admin = harness.Connect();
    admin->Ok("CREATE TABLE hidden (n u8) CHUNK 2 x 2");
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(test::PrepareLoginVerifier("pw", salt, scram::kMinIterations));
    admin->Ok("CREATE USER reader VERIFIER '" + verifier + "'");
    admin->Ok("GRANT ADMIN ON * TO reader");
    Client reader(harness.port, tls); reader.Login("reader", "pw");
    Error(reader.Command(Backup("denied")), "PERMISSION_DENIED");
    assert(!std::filesystem::exists(std::filesystem::canonical(destinations.path()) / "denied"));
    admin->Ok("ALTER USER reader MANAGES USERS");
    const auto reply = reader.Command(Backup("allowed"));
    assert(reply.type == '%' && Number(Field(reply, "tables")) == harness.catalog->TableCount());
    Verify(std::filesystem::canonical(destinations.path()) / "allowed");
    reader.Ok("BEGIN");
    Error(reader.Command(Backup("txn")), "INVALID_ARGUMENT");
    reader.Ok("ROLLBACK");
}
void Disconnect(bool tls, bool close_notify = false, bool half_close = false) {
    CopyPause pause;
    test::ScopedTempDir destinations("chunkdb-backup-disconnect");
    Harness harness(tls, false, kDefaultSlotMaxBytes, 100ms, kDefaultFeedBufferBytes, destinations.path());
    auto writer = harness.Connect(); writer->Ok("CREATE TABLE t (n u8) CHUNK 2 x 2");
    (void)writer->Command("SET BLOCK 0 0 IN t n = 7");
    auto client = harness.Connect();
    harness.catalog->SetBackupHookForTests(&pause);
    const auto target = std::filesystem::canonical(destinations.path()) / "completed";
    client->Line(Backup("completed")); pause.Wait();
#ifdef CHUNKDB_WITH_OPENSSL
    if (close_notify) client->CloseTlsWrite();
    else
#else
    assert(!close_notify);
#endif
    if (half_close) client->CloseWrite();
    else {
        client->Line("PING");
        client.reset();
    }
    pause.Release();
    if (half_close) assert(client->Read().type == '%');
    const auto deadline = Clock::now() + 10s;
    while (!std::filesystem::exists(target / kBackupMarkerName) ||
           std::filesystem::exists(target / kBackupIncompleteName)) {
        assert(Clock::now() < deadline);
        std::this_thread::yield();
    }
    // Releasing the hook after server shutdown also waits for the worker's
    // final reply path, including a peer that has already disconnected.
    harness.server().Stop();
    harness.catalog->SetBackupHookForTests(nullptr);
    Verify(target);
}
void Shutdown(bool tls) {
    CopyPause pause; AbortNotice notice;
    test::ScopedTempDir destinations("chunkdb-backup-shutdown");
    Harness harness(tls, false, kDefaultSlotMaxBytes, 100ms, kDefaultFeedBufferBytes, destinations.path());
    auto client = harness.Connect();
    harness.catalog->SetBackupHookForTests(&pause); harness.engine().SetHookForTests(&notice);
    client->Line(Backup("aborted")); pause.Wait();
    harness.server().Stop(); pause.Release(); notice.Wait();
    harness.catalog->SetBackupHookForTests(nullptr); harness.engine().SetHookForTests(nullptr);
    assert(std::filesystem::exists(destinations.path() / "aborted" / kBackupIncompleteName));
    assert(!std::filesystem::exists(destinations.path() / "aborted" / kBackupMarkerName));
}
void TargetPolicy(bool tls) {
    test::ScopedTempDir destinations("chunkdb-backup-policy");
    Harness harness(tls, false, kDefaultSlotMaxBytes, 100ms, kDefaultFeedBufferBytes, destinations.path());
    auto client = harness.Connect();
    for (const auto& path : {std::string("../escape"), std::string("nested/../../escape"),
                            (destinations.path() / "absolute").string()})
        Error(client->Command(Backup(path)), "INVALID_ARGUMENT");
#ifndef _WIN32
    std::filesystem::create_directory_symlink(harness.directory.path(), destinations.path() / "alias");
    Error(client->Command(Backup("alias/escape")), "INVALID_ARGUMENT");
    assert(!std::filesystem::exists(harness.directory.path() / "escape"));
#endif
    const auto reply = client->Command(Backup("nested/snapshot")); assert(reply.type == '%');
    Verify(destinations.path() / "nested/snapshot");
    client.reset(); harness.Restart(); client = harness.Connect();
    assert(client->Command(Backup("restarted")).type == '%');
    Verify(destinations.path() / "restarted");
    EngineConfig config; config.require_auth = false;
    CommandEngine disabled(config, harness.catalog); SessionState session;
    (void)disabled.Execute(session, "HELLO 3");
    const auto error = disabled.Execute(session, Backup("disabled"));
    assert(error.starts_with("-ERR INVALID_ARGUMENT") && error.find("--backup-dir") != std::string::npos);
}
void DroppedBetweenListingAndPin(bool tls) {
    CopyPause pause(BackupTestHook::Point::kBeforeTablePin);
    test::ScopedTempDir destinations("chunkdb-backup-dropped-protocol");
    Harness harness(tls, false, kDefaultSlotMaxBytes, 100ms, kDefaultFeedBufferBytes, destinations.path());
    auto writer = harness.Connect(); writer->Ok("CREATE TABLE a (n u8) CHUNK 2 x 2");
    auto backup = harness.Connect(); harness.catalog->SetBackupHookForTests(&pause);
    backup->Line(Backup("snapshot")); pause.Wait();
    writer->Ok("DROP TABLE a"); pause.Release();
    const auto response = backup->Read(); harness.catalog->SetBackupHookForTests(nullptr);
    const auto record = ReadBackupRecord(destinations.path() / "snapshot");
    assert(Number(Field(response, "tables")) == 1U && record.tables.size() == 1U);
    const auto& cuts = Field(response, "cuts"); assert(cuts.items.size() == 1U);
    assert(Field(cuts.items[0], "table").value == "default" && record.tables[0].name == "default");
    assert(!std::filesystem::exists(destinations.path() / "snapshot/tables/a"));
    Verify(destinations.path() / "snapshot");
}
std::string Quoted(const std::string& text) {
    std::string quoted;
    for (const char c : text) { quoted += c; if (c == '\'') quoted += c; }
    return "'" + quoted + "'";
}
// RESTORE TABLE publishes the backup cut as a new table beside the live one.
void RestoreStatement() {
    test::ScopedTempDir destinations("chunkdb-restore-table-protocol");
    Harness harness(false, true, kDefaultSlotMaxBytes, 100ms, kDefaultFeedBufferBytes, destinations.path());
    auto admin = harness.Connect();
    admin->Ok("CREATE TABLE t (n u8) CHUNK 2 x 2");
    admin->Ok("CREATE SLOT 'consumer' ON t");
    const auto written = Number(admin->Command("SET BLOCK 0 0 IN t n = 7"));
    const auto backup = admin->Command(Backup("it's-snap"));
    std::uint64_t cut = 0U;
    for (const auto& item : Field(backup, "cuts").items) if (Field(item, "table").value == "t") cut = Number(Field(item, "revision"));
    assert(cut >= written);
    (void)admin->Command("SET BLOCK 0 0 IN t n = 8");
    const auto old_epoch = StoreIdHex(harness.catalog->Find("t")->store_id());

    const auto restore = "RESTORE TABLE t FROM " + Quoted("it's-snap");
    const auto reply = admin->Command(restore + " AS t_restored");
    assert(reply.type == '%' && Field(reply, "table").value == "t_restored");
    assert(Field(reply, "parent_epoch").value == old_epoch && Number(Field(reply, "fork_revision")) == cut);
    assert(Field(reply, "epoch").value == StoreIdHex(harness.catalog->Find("t_restored")->store_id()));
    assert(Field(reply, "epoch").value != old_epoch);
    assert(Number(Field(reply, "files")) > 0U && Number(Field(reply, "bytes")) > 0U);
    assert(Number(admin->Command("GET BLOCK 0 0 FROM t_restored").items.at(0)) == 7U);
    assert(Number(admin->Command("GET BLOCK 0 0 FROM t").items.at(0)) == 8U);
    const auto described = admin->Command("DESCRIBE t_restored");
    assert(Field(described, "epoch").value == Field(reply, "epoch").value);
    const auto slots = harness.catalog->Find("t_restored")->ListFeedSlots();
    assert(slots.size() == 1U && slots[0].name == "consumer" && slots[0].position.revision == cut);
    // The restored slot is at the cut, not at its consumer's position: a
    // watch without AFTER starts with resync.
    {
        auto watcher = harness.Connect();
        assert(Start(watcher->Command("WATCH t_restored SLOT 'consumer'")) == std::make_pair(Field(reply, "epoch").value, cut));
        const auto resync = watcher->Read();
        assert(resync.type == '>' && resync.items[0].value == "resync" && resync.items[1].value == Field(reply, "epoch").value);
        Unwatch(*watcher);
    }

    Error(admin->Command(restore + " AS t_restored"), "TABLE_EXISTS");
    Error(admin->Command(restore), "TABLE_EXISTS");
    Error(admin->Command("RESTORE TABLE absent FROM " + Quoted("it's-snap") + " AS other"), "INVALID_ARGUMENT");
    Error(admin->Command("RESTORE TABLE t FROM '../escape' AS other"), "INVALID_ARGUMENT");
    Error(admin->Command("RESTORE TABLE t FROM 'missing' AS other"), "INTERNAL");
    admin->Ok("BEGIN");
    Error(admin->Command(restore + " AS other"), "INVALID_ARGUMENT");
    admin->Ok("ROLLBACK");
    assert(harness.catalog->Find("other") == nullptr);

    // ADMIN on `*`, or MANAGES USERS, may restore; ADMIN on one table may not.
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(test::PrepareLoginVerifier("pw", salt, scram::kMinIterations));
    admin->Ok("CREATE USER operator VERIFIER '" + verifier + "'");
    admin->Ok("GRANT ADMIN ON t TO operator");
    Client user(harness.port, false); user.Login("operator", "pw");
    Error(user.Command(restore + " AS by_table_admin"), "PERMISSION_DENIED");
    admin->Ok("GRANT ADMIN ON * TO operator");
    assert(user.Command(restore + " AS by_admin").type == '%');
    admin->Ok("REVOKE ADMIN ON * FROM operator");
    admin->Ok("ALTER USER operator MANAGES USERS");
    assert(user.Command(restore + " AS by_manager").type == '%');
    assert(harness.catalog->Find("by_table_admin") == nullptr);
    assert(harness.catalog->Find("by_admin") && harness.catalog->Find("by_manager"));

    // Without --backup-dir, and on a read-only server, it is refused.
    EngineConfig config; config.require_auth = false;
    CommandEngine disabled(config, harness.catalog); SessionState session;
    (void)disabled.Execute(session, "HELLO 3");
    const auto error = disabled.Execute(session, restore + " AS other");
    assert(error.starts_with("-ERR INVALID_ARGUMENT") && error.find("--backup-dir") != std::string::npos);
    auto read_only_config = feed_test::Config(std::filesystem::canonical(destinations.path()) / "it's-snap");
    read_only_config.access_mode = AccessMode::kReadOnly;
    auto read_only = std::make_shared<TableCatalog>(read_only_config);
    config.backup_dir = destinations.path();
    CommandEngine reader(config, read_only); SessionState reading;
    (void)reader.Execute(reading, "HELLO 3");
    const auto refused = reader.Execute(reading, restore + " AS other");
    assert(refused.starts_with("-ERR INVALID_ARGUMENT") && refused.find("read-only") != std::string::npos);
    Verify(std::filesystem::canonical(destinations.path()) / "it's-snap");
}
// A running RESTORE TABLE lists the files and bytes it copied in SHOW CONNECTIONS.
void RestoreProgress() {
    RestoreProgressPause pause;
    test::ScopedTempDir destinations("chunkdb-restore-progress-protocol");
    Harness harness(false, false, kDefaultSlotMaxBytes, 100ms, kDefaultFeedBufferBytes, destinations.path());
    auto admin = harness.Connect();
    admin->Ok("CREATE TABLE t (n u8) CHUNK 2 x 2");
    (void)admin->Command("SET BLOCK 0 0 IN t n = 7");
    (void)admin->Command(Backup("snap"));
    auto restorer = harness.Connect();
    // The progress of the one connection running RESTORE.
    const auto restoring = [&] {
        const auto listing = admin->Command("SHOW CONNECTIONS");
        ExpectType(listing, '*');
        const Reply* found = nullptr;
        for (const auto& item : listing.items) {
            if (Field(item, "statement").value != "RESTORE") continue;
            assert(found == nullptr);
            found = &item;
        }
        assert(found != nullptr && Field(*found, "state").value == "running");
        return Field(*found, "progress");
    };
    harness.engine().SetHookForTests(&pause);
    restorer->Line("RESTORE TABLE t FROM 'snap' AS t_progress");
    // The totals come before the first file is copied.
    pause.Wait(1U);
    auto progress = restoring();
    assert(progress.type == '%' && progress.items.size() == 8U);
    assert(Number(Field(progress, "files_done")) == 0U && Number(Field(progress, "bytes_done")) == 0U);
    const auto files = Number(Field(progress, "files_total"));
    const auto bytes = Number(Field(progress, "bytes_total"));
    assert(files > 0U && bytes > 0U);
    std::uint64_t copied = 0U;
    for (std::uint64_t file = 1U; file <= files; ++file) {
        assert(harness.catalog->Find("t_progress") == nullptr);
        pause.Release(file);
        pause.Wait(file + 1U);
        progress = restoring();
        assert(Number(Field(progress, "files_done")) == file && Number(Field(progress, "files_total")) == files);
        const auto done = Number(Field(progress, "bytes_done"));
        assert(done >= copied && done <= bytes && Number(Field(progress, "bytes_total")) == bytes);
        copied = done;
    }
    assert(copied == bytes);
    pause.Release(files + 1U);
    const auto reply = restorer->Read();
    harness.engine().SetHookForTests(nullptr);
    assert(reply.type == '%' && Number(Field(reply, "files")) == files && Number(Field(reply, "bytes")) == bytes);
    assert(harness.catalog->Find("t_progress") != nullptr);
    // The next statement on the connection reports no progress.
    assert(restorer->Command("PING").value == "PONG");
    for (const auto& item : admin->Command("SHOW CONNECTIONS").items) assert(Field(item, "progress").type == '_');
}
// SWAP TABLES exchanges the names; open transactions on either end with CONFLICT.
void SwapStatement() {
    test::ScopedTempDir destinations("chunkdb-swap-tables-protocol");
    Harness harness(false, true, kDefaultSlotMaxBytes, 100ms, kDefaultFeedBufferBytes, destinations.path());
    auto admin = harness.Connect();
    admin->Ok("CREATE TABLE t (n u8) CHUNK 2 x 2");
    admin->Ok("CREATE SLOT 'consumer' ON t");
    (void)admin->Command("SET BLOCK 0 0 IN t n = 7");
    (void)admin->Command(Backup("snap"));
    (void)admin->Command("SET BLOCK 0 0 IN t n = 8");
    const auto restored = Field(admin->Command("RESTORE TABLE t FROM 'snap' AS t_restored"), "epoch").value;
    const auto live = StoreIdHex(harness.catalog->Find("t")->store_id());
#ifdef _WIN32
    // Windows has no atomic directory exchange; both tables keep their names.
    Error(admin->Command("SWAP TABLES t, t_restored"), "INTERNAL");
    assert(Field(admin->Command("DESCRIBE t"), "epoch").value == live);
    return;
#endif
    // The live table's slot is consumed and acknowledged under its own name.
    auto watcher = harness.Connect();
    (void)Start(watcher->Command("WATCH t SLOT 'consumer'"));
    const auto acked = Number(admin->Command("SET BLOCK 1 1 IN t n = 3"));
    while (Change(NextChange(*watcher)) < acked) {}
    watcher->Line("ACK " + std::to_string(acked));
    Unwatch(*watcher);
    WaitAck(*admin, "t", "consumer", acked);

    auto txn = harness.Connect();
    txn->Ok("BEGIN");
    assert(Number(txn->Command("GET BLOCK 0 0 FROM t").items.at(0)) == 8U);
    admin->Ok("SWAP TABLES t, t_restored");
    Error(txn->Command("SET BLOCK 0 0 IN t n = 9"), "CONFLICT");
    txn->Ok("ROLLBACK");
    assert(Field(admin->Command("DESCRIBE t"), "epoch").value == restored);
    assert(Field(admin->Command("DESCRIBE t_restored"), "epoch").value == live);
    assert(Number(admin->Command("GET BLOCK 0 0 FROM t").items.at(0)) == 7U);
    assert(Number(admin->Command("GET BLOCK 0 0 FROM t_restored").items.at(0)) == 8U);

    // Each name now reaches another table's slot, so a watch without AFTER
    // resyncs, until the consumer acknowledges a change after the resync.
    for (const auto& [table, epoch] : {std::pair{std::string("t"), restored}, std::pair{std::string("t_restored"), live}}) {
        assert(Start(watcher->Command("WATCH " + table + " SLOT 'consumer'")).first == epoch);
        const auto resync = watcher->Read();
        assert(resync.type == '>' && resync.items[0].value == "resync" && resync.items[1].value == epoch);
        Unwatch(*watcher);
    }
    assert(Start(watcher->Command("WATCH t_restored SLOT 'consumer'")) == std::make_pair(live, acked));
    assert(watcher->Read().items.at(0).value == "resync");
    const auto after_swap = Number(admin->Command("SET BLOCK 1 1 IN t_restored n = 4"));
    assert(Change(NextChange(*watcher)) == after_swap);
    watcher->Line("ACK " + std::to_string(after_swap));
    Unwatch(*watcher);
    WaitAck(*admin, "t_restored", "consumer", after_swap);
    assert(Start(watcher->Command("WATCH t_restored SLOT 'consumer'")) == std::make_pair(live, after_swap));
    const auto resumed = Number(admin->Command("SET BLOCK 1 1 IN t_restored n = 5"));
    assert(Change(NextChange(*watcher)) == resumed);
    Unwatch(*watcher);
    // A consumer naming its own position keeps it: the exact fork continues.
    const auto fork = Start(watcher->Command("WATCH t SLOT 'consumer' AFTER " + live + " " +
        std::to_string(harness.catalog->Find("t")->lineage()->fork_revision)));
    assert(fork.first == restored);
    const auto on_restored = Number(admin->Command("SET BLOCK 1 1 IN t n = 6"));
    assert(Change(NextChange(*watcher)) == on_restored);
    Unwatch(*watcher);

    Error(admin->Command("SWAP TABLES t, t"), "INVALID_ARGUMENT");
    Error(admin->Command("SWAP TABLES t, absent"), "NO_TABLE");
    admin->Ok("BEGIN");
    Error(admin->Command("SWAP TABLES t, t_restored"), "INVALID_ARGUMENT");
    admin->Ok("ROLLBACK");

    // Grants stay with names, so the user needs ADMIN on both.
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(test::PrepareLoginVerifier("pw", salt, scram::kMinIterations));
    admin->Ok("CREATE USER operator VERIFIER '" + verifier + "'");
    admin->Ok("GRANT ADMIN ON t TO operator");
    admin->Ok("GRANT READ ON t_restored TO operator");
    Client user(harness.port, false); user.Login("operator", "pw");
    Error(user.Command("SWAP TABLES t, t_restored"), "PERMISSION_DENIED");
    admin->Ok("GRANT ADMIN ON t_restored TO operator");
    user.Ok("SWAP TABLES t_restored, t");
    assert(Field(admin->Command("DESCRIBE t"), "epoch").value == live);
}
// ALTER TABLE ... RENAME TO keeps the epoch and the slot position under the
// new name; open transactions end with CONFLICT and the old name is gone.
void RenameStatement() {
    test::ScopedTempDir destinations("chunkdb-rename-table-protocol");
    Harness harness(false, true, kDefaultSlotMaxBytes, 100ms, kDefaultFeedBufferBytes, destinations.path());
    auto admin = harness.Connect();
    admin->Ok("CREATE TABLE r (n u8) CHUNK 2 x 2");
    admin->Ok("CREATE SLOT 'consumer' ON r");
    const auto epoch = StoreIdHex(harness.catalog->Find("r")->store_id());
    auto watcher = harness.Connect();
    (void)Start(watcher->Command("WATCH r SLOT 'consumer'"));
    const auto acked = Number(admin->Command("SET BLOCK 1 1 IN r n = 3"));
    while (Change(NextChange(*watcher)) < acked) {}
    watcher->Line("ACK " + std::to_string(acked));
    Unwatch(*watcher);
    WaitAck(*admin, "r", "consumer", acked);

    auto txn = harness.Connect();
    txn->Ok("BEGIN");
    assert(Number(txn->Command("GET BLOCK 1 1 FROM r").items.at(0)) == 3U);
    admin->Ok("ALTER TABLE r RENAME TO r_old");
    Error(txn->Command("SET BLOCK 0 0 IN r n = 9"), "CONFLICT");
    txn->Ok("ROLLBACK");
    Error(admin->Command("DESCRIBE r"), "NO_TABLE");
    assert(Field(admin->Command("DESCRIBE r_old"), "epoch").value == epoch);
    assert(Number(admin->Command("GET BLOCK 1 1 FROM r_old").items.at(0)) == 3U);
    // The slot is reached by its new name at its saved position, without resync.
    assert(Start(watcher->Command("WATCH r_old SLOT 'consumer'")) == std::make_pair(epoch, acked));
    const auto after = Number(admin->Command("SET BLOCK 1 1 IN r_old n = 4"));
    assert(Change(NextChange(*watcher)) == after);
    Unwatch(*watcher);

    Error(admin->Command("ALTER TABLE r_old RENAME TO r_old"), "INVALID_ARGUMENT");
    Error(admin->Command("ALTER TABLE r RENAME TO r_new"), "NO_TABLE");
    admin->Ok("CREATE TABLE r (n u8) CHUNK 2 x 2");
    Error(admin->Command("ALTER TABLE r_old RENAME TO r"), "TABLE_EXISTS");
    admin->Ok("BEGIN");
    Error(admin->Command("ALTER TABLE r_old RENAME TO r_new"), "INVALID_ARGUMENT");
    admin->Ok("ROLLBACK");
}
} // namespace
int main() {
    RestoreStatement();
    RestoreProgress();
    SwapStatement();
    RenameStatement();
    std::cout << "restore, swap and rename protocol groups passed\n";
    for (const bool tls : {false, true}) {
#ifndef CHUNKDB_WITH_OPENSSL
        if (tls) continue;
#endif
        CopyAndResync(tls); Rights(tls); Disconnect(tls); Shutdown(tls); TargetPolicy(tls); DroppedBetweenListingAndPin(tls);
        if (!tls) Disconnect(false, false, true);
#ifdef CHUNKDB_WITH_OPENSSL
        if (tls) Disconnect(true, true);
#endif
        std::cout << (tls ? "TLS: 7" : "plain: 7") << " backup protocol groups passed\n";
    }
}
