#include <condition_variable>
#include <future>
#include <iostream>
#include "server_slots_test_utils.hpp"
#include "backup.hpp"
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
    Harness harness(tls);
    test::ScopedTempDir destinations("chunkdb-backup-protocol");
    const auto backup = std::filesystem::canonical(destinations.path()) / "it\'s-backup", restored = std::filesystem::canonical(destinations.path()) / "restored";
    auto writer = harness.Connect();
    writer->Ok("CREATE TABLE t (n u8) CHUNK 2 x 2");
    writer->Ok("CREATE SLOT 'consumer' ON t");
    const auto before = Number(writer->Command("SET BLOCK 0 0 IN t n = 7"));
    const auto old_epoch = StoreIdHex(harness.catalog->Find("t")->Info().store_id);
    auto client = harness.Connect();
    harness.catalog->SetBackupHookForTests(&pause);
    client->Line(Backup(backup));
    pause.Wait();
    // Copy is held after the pin phase, and both data writes and DDL finish.
    const auto after = Number(writer->Command("SET BLOCK 0 0 IN t n = 8"));
    writer->Ok("ALTER TABLE t ADD COLUMN extra u16 DEFAULT 9");
    Error(writer->Command(Backup(std::filesystem::canonical(destinations.path()) / "busy")), "BUSY");
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
    Harness harness(tls, true);
    test::ScopedTempDir destinations("chunkdb-backup-rights");
    auto admin = harness.Connect();
    admin->Ok("CREATE TABLE hidden (n u8) CHUNK 2 x 2");
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(scram::MakeVerifier("pw", salt, scram::kMinIterations));
    admin->Ok("CREATE USER reader VERIFIER '" + verifier + "'");
    admin->Ok("GRANT ADMIN ON * TO reader");
    Client reader(harness.port, tls); reader.Login("reader", "pw");
    Error(reader.Command(Backup(std::filesystem::canonical(destinations.path()) / "denied")), "PERMISSION_DENIED");
    assert(!std::filesystem::exists(std::filesystem::canonical(destinations.path()) / "denied"));
    admin->Ok("ALTER USER reader MANAGES USERS");
    const auto reply = reader.Command(Backup(std::filesystem::canonical(destinations.path()) / "allowed"));
    assert(reply.type == '%' && Number(Field(reply, "tables")) == harness.catalog->TableCount());
    Verify(std::filesystem::canonical(destinations.path()) / "allowed");
    reader.Ok("BEGIN");
    Error(reader.Command(Backup(std::filesystem::canonical(destinations.path()) / "txn")), "INVALID_ARGUMENT");
    reader.Ok("ROLLBACK");
}
void Disconnect(bool tls) {
    CopyPause pause; AbortNotice notice;
    Harness harness(tls);
    test::ScopedTempDir destinations("chunkdb-backup-disconnect");
    auto writer = harness.Connect(); writer->Ok("CREATE TABLE t (n u8) CHUNK 2 x 2");
    (void)writer->Command("SET BLOCK 0 0 IN t n = 7");
    auto client = harness.Connect();
    harness.catalog->SetBackupHookForTests(&pause); harness.engine().SetHookForTests(&notice);
    const auto target = std::filesystem::canonical(destinations.path()) / "aborted";
    client->Line(Backup(target)); pause.Wait();
    client->Line("PING"); // Unread pipelined bytes must not hide the peer's FIN.
    client.reset(); pause.Release(); notice.Wait();
    harness.catalog->SetBackupHookForTests(nullptr); harness.engine().SetHookForTests(nullptr);
    assert(std::filesystem::exists(target / kBackupIncompleteName));
    bool refused = false;
    try { RestoreBackup(target, std::filesystem::canonical(destinations.path()) / "refused"); } catch (const std::exception&) { refused = true; }
    assert(refused);
    const auto next = writer->Command(Backup(std::filesystem::canonical(destinations.path()) / "next"));
    assert(next.type == '%'); Verify(std::filesystem::canonical(destinations.path()) / "next");
}
} // namespace
int main() {
    for (const bool tls : {false, true}) {
#ifndef CHUNKDB_WITH_OPENSSL
        if (tls) continue;
#endif
        CopyAndResync(tls); Rights(tls); Disconnect(tls);
        std::cout << (tls ? "TLS" : "plain") << ": 3 backup protocol groups passed\n";
    }
}
