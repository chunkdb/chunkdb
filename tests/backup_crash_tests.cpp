#include <cassert>
#include <cstdlib>
#include <sstream>
#include <string>

#ifndef _WIN32
#include <sys/wait.h>
#endif
#include "backup.hpp"
#include "feed_test_utils.hpp"
#include "verify.hpp"
#include "wal_writer.hpp"
#include "checkpoint.hpp"
#include "chunkdb/file_layout.hpp"

namespace {
using namespace chunkdb;
CatalogConfig Config(const std::filesystem::path& path, DurabilityMode mode) {
    auto config = feed_test::Config(path);
    config.default_options.durability_mode = mode;
    return config;
}
std::string Quote(const std::string& text) {
#ifdef _WIN32
    return '"' + text + '"';
#else
    std::string result = "'";
    for (const char c : text) result += c == '\'' ? "'\\''" : std::string(1, c);
    return result + "'";
#endif
}
int Child(const char* executable, const std::filesystem::path& root, DurabilityMode mode, const char* point) {
    const auto command = Quote(std::filesystem::absolute(executable).string()) + " --child " + Quote(root.string()) +
        " " + std::to_string(static_cast<int>(mode)) + " " + Quote(point);
    const auto result = std::system(command.c_str());
#ifdef _WIN32
    return result;
#else
    assert(result != -1 && WIFEXITED(result)); return WEXITSTATUS(result);
#endif
}
void CrashCase(const char* executable, DurabilityMode mode, const char* step) {
    test::ScopedTempDir temp("chunkdb-backup-crash");
    const auto root = std::filesystem::canonical(temp.path());
    const auto point = "CHUNKDB_FAILPOINT_CRASH_BACKUP_" + std::string(step) + "_ONCE";
    assert(Child(executable, root, mode, point.c_str()) == 86);
    // Restart repairs any interrupted local staging. Accepted source writes remain.
    {
        TableCatalog source(Config(root / "source", mode));
        auto lease = source.Find("default")->Acquire();
        assert(txn_test::ReadCounter(lease->store(), {0, 0}) == 11U);
        assert(txn_test::ReadCounter(lease->store(), {1, 0}) == 22U);
        assert(!std::filesystem::exists(root / "source" / kBackupStagingName) ||
            std::filesystem::is_empty(root / "source" / kBackupStagingName));
    }
    const bool complete = std::string_view(step) == "AFTER_GUARD_REMOVE" || std::string_view(step) == "AFTER_COMPLETE";
    bool restored = false;
    try { RestoreBackup(root / "backup", root / "restored"); restored = true; }
    catch (const std::exception&) { assert(!complete); }
    assert(restored == complete);
    if (complete) {
        std::ostringstream report; assert(VerifyDataDirectory(root / "restored", report).errors == 0U);
        TableCatalog restored_catalog(Config(root / "restored", mode));
        auto lease = restored_catalog.Find("default")->Acquire();
        assert(txn_test::ReadCounter(lease->store(), {0, 0}) == 11U);
        assert(txn_test::ReadCounter(lease->store(), {1, 0}) == 22U);
    } else {
        assert(std::filesystem::exists(root / "backup" / kBackupIncompleteName));
        assert(!std::filesystem::exists(root / "restored"));
    }
}
void LinkedWalSyncFailurePreservesLivePrefix() {
#ifdef __APPLE__
    test::ScopedTempDir temp("chunkdb-linked-wal-sync");
    const auto wal = temp.path() / "live.wal";
    const auto link = temp.path() / "pinned.wal";
    const std::vector<std::uint8_t> bytes{'d', 'u', 'r', 'a', 'b', 'l', 'e', '!'};
    AtomicWrite(wal, bytes, true, true);
    std::filesystem::create_hard_link(wal, link);
    txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_FULL_SYNC_FILE_FAIL_ONCE", "1");
    bool failed = false;
    try { (void)ResizeWalPreservingLinks(wal, bytes.size() - 1U, false); }
    catch (const std::exception& error) {
        failed = std::string(error.what()).find("F_FULLFSYNC") != std::string::npos;
    }
    assert(failed);
    assert(LoadFile(wal) == bytes && LoadFile(link) == bytes);
    assert(std::filesystem::equivalent(wal, link));
#endif
}

void LinkedWalCleanupWithOpenStream() {
    test::ScopedTempDir temp("chunkdb-linked-wal-open");
    const auto wal = temp.path() / "live.wal";
    const auto link = temp.path() / "pinned.wal";
    WalAppendStream stream(wal, std::ios::binary | std::ios::app);
    assert(stream.is_open());
    stream.write("first", 5); stream.flush(); assert(stream.good());
    std::filesystem::create_hard_link(wal, link);
    assert(std::filesystem::hard_link_count(wal) == 2U);
    assert(std::filesystem::remove(link));
    assert(std::filesystem::hard_link_count(wal) == 1U && stream.is_open());
    stream.write("second", 6); stream.flush(); assert(stream.good());
    assert(LoadFile(wal) == std::vector<std::uint8_t>({'f','i','r','s','t','s','e','c','o','n','d'}));
    stream.close(); assert(!stream.fail() && !stream.is_open());
    SyncFilePath(wal);
    assert(LoadFile(wal) == std::vector<std::uint8_t>({'f','i','r','s','t','s','e','c','o','n','d'}));

    TableCatalog catalog(Config(temp.path() / "source", DurabilityMode::kRelaxed));
    auto lease = catalog.Find("default")->Acquire();
    auto& store = lease->store();
    txn_test::WriteCounter(store, {0, 0}, 17U); store.WalBarrier();
    const auto pooled = ChunkWalPath(store.data_dir(), store.geometry(), {0, 0});
    const auto pooled_link = temp.path() / "pooled-pin.wal";
    const auto opened = store.RuntimeStats().open_wal_streams;
    assert(opened > 0U);
    std::filesystem::create_hard_link(pooled, pooled_link);
    assert(std::filesystem::remove(pooled_link));
    assert(std::filesystem::hard_link_count(pooled) == 1U);
    assert(store.RuntimeStats().open_wal_streams == opened);
    txn_test::WriteCounter(store, {0, 0}, 29U); store.WalBarrier();
    assert(txn_test::ReadCounter(store, {0, 0}) == 29U);
    assert(store.RuntimeStats().open_wal_streams == opened);
}

void WalReplacementCrash(const char* executable, DurabilityMode mode) {
    test::ScopedTempDir temp("chunkdb-linked-wal-crash");
    const auto root = std::filesystem::canonical(temp.path());
    std::filesystem::path wal;
    {
        TableCatalog source(Config(root / "source", mode));
        auto lease = source.Find("default")->Acquire();
        txn_test::WriteCounter(lease->store(), {0, 0}, 47U);
        lease->store().WalBarrier();
        wal = ChunkWalPath(lease->store().data_dir(), lease->store().geometry(), {0, 0});
    }
    const auto durable = LoadFile(wal);
    auto torn = durable; torn.push_back('F');
    AtomicWrite(wal, torn, true, true);
    std::filesystem::create_hard_link(wal, root / "pinned.wal");
    assert(Child(executable, root, mode, "wal-replace") == 86);
    assert(LoadFile(wal) == durable);
    assert(LoadFile(root / "pinned.wal") == torn);
    assert(!std::filesystem::equivalent(wal, root / "pinned.wal"));
    {
        TableCatalog recovered(Config(root / "source", mode));
        auto lease = recovered.Find("default")->Acquire();
        assert(txn_test::ReadCounter(lease->store(), {0, 0}) == 47U);
        (void)recovered.BackupTo(root / "backup", {});
    }
    RestoreBackup(root / "backup", root / "restored");
    std::ostringstream report; assert(VerifyDataDirectory(root / "restored", report).errors == 0U);
    TableCatalog restored(Config(root / "restored", mode));
    auto lease = restored.Find("default")->Acquire();
    assert(txn_test::ReadCounter(lease->store(), {0, 0}) == 47U);
}

void CompletionFailure(bool reinstate) {
    test::ScopedTempDir temp("chunkdb-backup-sync");
    const auto root = std::filesystem::canonical(temp.path());
    TableCatalog catalog(Config(root / "source", DurabilityMode::kFsyncWal));
    { auto lease = catalog.Find("default")->Acquire(); txn_test::WriteCounter(lease->store(), {0, 0}, 31); }
    txn_test::ScopedEnv sync_fail("CHUNKDB_FAILPOINT_BACKUP_COMPLETE_SYNC_FAIL_ONCE", "1");
    std::unique_ptr<txn_test::ScopedEnv> guard_fail;
    if (reinstate) guard_fail = std::make_unique<txn_test::ScopedEnv>("CHUNKDB_FAILPOINT_BACKUP_GUARD_REINSTATE_FAIL_ONCE", "1");
    bool failed = false;
    try { (void)catalog.BackupTo(root / "backup", {}); }
    catch (const BackupPublicationUnknownError&) { assert(reinstate); failed = true; }
    catch (const std::exception&) { assert(!reinstate); failed = true; }
    assert(failed);
    assert(std::filesystem::exists(root / "backup" / kBackupIncompleteName) != reinstate);
    assert(std::filesystem::exists(root / "backup" / kBackupMarkerName));
    // An unknown result may have completed. A reinstated guard must refuse restore.
    bool restored = false;
    try { RestoreBackup(root / "backup", root / "restored"); restored = true; } catch (const std::exception&) { assert(!reinstate); }
    assert(restored == reinstate);
    auto lease = catalog.Find("default")->Acquire(); assert(txn_test::ReadCounter(lease->store(), {0, 0}) == 31U);
}
}
int main(int argc, char** argv) {
    if (argc == 5 && std::string_view(argv[1]) == "--child") {
        const std::filesystem::path root(argv[2]);
        const auto mode = static_cast<chunkdb::DurabilityMode>(std::stoi(argv[3]));
        chunkdb::TableCatalog catalog(Config(root / "source", mode));
        if (std::string_view(argv[4]) == "wal-replace") {
            chunkdb::txn_test::ScopedEnv crash("CHUNKDB_FAILPOINT_CRASH_WAL_REPLACE_AFTER_RENAME_ONCE", "1");
            auto lease = catalog.Find("default")->Acquire();
            (void)chunkdb::txn_test::ReadCounter(lease->store(), {0, 0});
            return 2;
        }
        {
            auto lease = catalog.Find("default")->Acquire();
            chunkdb::txn_test::WriteCounter(lease->store(), {0, 0}, 11);
            lease->store().CheckpointForTests(0, 0);
            chunkdb::txn_test::WriteCounter(lease->store(), {1, 0}, 22);
            lease->store().WalBarrier();
        }
        chunkdb::txn_test::ScopedEnv crash(argv[4], "1");
        (void)catalog.BackupTo(root / "backup", {});
        return 2;
    }
    if (argc == 2 && std::string_view(argv[1]) == "--wal-replacement") {
        LinkedWalSyncFailurePreservesLivePrefix(); LinkedWalCleanupWithOpenStream();
        for (const auto mode : {chunkdb::DurabilityMode::kRelaxed, chunkdb::DurabilityMode::kFsyncWal, chunkdb::DurabilityMode::kFsyncCheckpoint})
            WalReplacementCrash(argv[0], mode);
        std::puts("WAL replacement passed: sync-before-rename failure, open-stream link cleanup, 3 crash/restore modes");
        return 0;
    }
    LinkedWalSyncFailurePreservesLivePrefix(); LinkedWalCleanupWithOpenStream();
    for (const auto mode : {chunkdb::DurabilityMode::kRelaxed, chunkdb::DurabilityMode::kFsyncWal, chunkdb::DurabilityMode::kFsyncCheckpoint})
        WalReplacementCrash(argv[0], mode);
    for (const auto mode : {chunkdb::DurabilityMode::kRelaxed, chunkdb::DurabilityMode::kFsyncWal, chunkdb::DurabilityMode::kFsyncCheckpoint})
        for (const auto* step : {"AFTER_TARGET_GUARD", "AFTER_CUT", "AFTER_FLUSH", "AFTER_PIN", "BEFORE_COPY", "AFTER_COPY",
                "BEFORE_MARKER", "AFTER_MARKER", "AFTER_GUARD_REMOVE", "AFTER_COMPLETE"}) CrashCase(argv[0], mode, step);
    CompletionFailure(false); CompletionFailure(true);
    std::puts("backup crash passed: 10 stages x3 modes =30 child exits, 3 WAL replacement crash/restore modes, 2 completion failures, linked-WAL sync and cleanup checks");
}
