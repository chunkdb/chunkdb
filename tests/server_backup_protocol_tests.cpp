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
    void Run(Point point, std::string_view, std::uint64_t) override {
        if (point != Point::kBeforeCopy) return;
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
        const auto start = engine.Execute(session, "WATCH t SLOT 'consumer' AFTER " + old_epoch + " " + std::to_string(before));
        assert(start.starts_with("+OK "));
        session.slot_watch->Activate(); session.slot_watch->WorkStep();
        const auto resync = session.slot_watch->Take(SIZE_MAX);
        assert(resync && resync->bytes->find("resync") != std::string::npos);
        session.slot_watch->Cancel(); session.slot_watch.reset();
    }
    assert(writer->Command("PING").value == "PONG");
}
void Rights(bool tls) {
    test::ScopedTempDir destinations("chunkdb-backup-rights");
    Harness harness(tls, true, kDefaultSlotMaxBytes, 100ms, kDefaultFeedBufferBytes, destinations.path());
    auto admin = harness.Connect();
    admin->Ok("CREATE TABLE hidden (n u8) CHUNK 2 x 2");
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(scram::MakeVerifier("pw", salt, scram::kMinIterations));
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
} // namespace
int main() {
    for (const bool tls : {false, true}) {
#ifndef CHUNKDB_WITH_OPENSSL
        if (tls) continue;
#endif
        CopyAndResync(tls); Rights(tls); Disconnect(tls); Shutdown(tls); TargetPolicy(tls);
        if (!tls) Disconnect(false, false, true);
#ifdef CHUNKDB_WITH_OPENSSL
        if (tls) Disconnect(true, true);
#endif
        std::cout << (tls ? "TLS: 6" : "plain: 6") << " backup protocol groups passed\n";
    }
}
