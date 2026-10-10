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
    for (const auto mode : {chunkdb::DurabilityMode::kRelaxed, chunkdb::DurabilityMode::kFsyncWal, chunkdb::DurabilityMode::kFsyncCheckpoint})
        for (const auto* step : {"AFTER_TARGET_GUARD", "AFTER_CUT", "AFTER_FLUSH", "AFTER_PIN", "BEFORE_COPY", "AFTER_COPY",
                "BEFORE_MARKER", "AFTER_MARKER", "AFTER_GUARD_REMOVE", "AFTER_COMPLETE"}) CrashCase(argv[0], mode, step);
    CompletionFailure(false); CompletionFailure(true);
    std::puts("backup crash passed: 10 stages x3 modes =30 child exits, 2 completion failures");
}
