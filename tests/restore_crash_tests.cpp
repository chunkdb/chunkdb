#include <array>
#include <cstdlib>
#include <iostream>
#ifndef _WIN32
#include <sys/wait.h>
#endif
#include "backup_restore_test_utils.hpp"
#include "txn_test_utils.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::backup_test;
constexpr std::array<const char*, 11U> kRestorePoints{
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_VERIFY_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_GUARD_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_COPY_FILE_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_COPY_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_FILE_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_TABLE_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_SYNC_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_RENAME_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_PARENT_SYNC_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_GUARD_REMOVE_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_COMPLETE_ONCE"};
constexpr std::array<const char*, 4U> kBackupPoints{
    "CHUNKDB_FAILPOINT_CRASH_BACKUP_BEFORE_MARKER_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_MARKER_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_GUARD_REMOVE_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_COMPLETE_ONCE"};
std::string Quote(const std::string& argument) {
#ifdef _WIN32
    assert(argument.find('"') == std::string::npos); return "\"" + argument + "\"";
#else
    std::string result = "'";
    for (char ch : argument) result += ch == '\'' ? "'\\''" : std::string(1U, ch);
    return result + "'";
#endif
}
int Child(const std::string& executable, const std::vector<std::string>& arguments) {
    std::string command = Quote(executable); for (const auto& argument : arguments) command += " " + Quote(argument);
#ifdef _WIN32
    return std::system(("\"" + command + "\"").c_str());
#else
    const auto status = std::system(command.c_str()); return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
#endif
}
void RestoreCrashes(const std::string& executable) {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        for (const auto* point : kRestorePoints) {
            Fixture fixture(mode); const auto before = InspectBackupFile(fixture.backup, kBackupMarkerName);
            const auto target = fixture.root / "restored";
            assert(Child(executable, {"--restore-child", fixture.backup.string(), target.string(), point}) == 86);
            assert(InspectBackupFile(fixture.backup, kBackupMarkerName).crc32 == before.crc32 && Verify(fixture.backup).errors == 0U);
            const std::string name(point);
            if (std::filesystem::exists(target)) {
                const bool complete = name.find("AFTER_GUARD_REMOVE") != std::string::npos || name.find("AFTER_COMPLETE") != std::string::npos;
                if (complete) { assert(Verify(target).errors == 0U); RequireNotBackupDirectory(target); }
                else { assert(Verify(target).errors > 0U); Throws([&] { RequireNotBackupDirectory(target); }); }
            }
            for (const auto& entry : std::filesystem::directory_iterator(fixture.root))
                if (entry.path().filename().string().rfind(".chunkdb.restore.", 0U) == 0U) {
                    assert(std::filesystem::exists(entry.path() / kRestoreIncompleteName));
                    Throws([&] { RequireNotBackupDirectory(entry.path()); });
                }
        }
    }
}
void BackupCrashes(const std::string& executable) {
    for (const auto* point : kBackupPoints) {
        Fixture fixture; Save(fixture.backup / kBackupIncompleteName, {'C', 'K', 'B', 'I'});
        assert(Child(executable, {"--backup-child", fixture.backup.string(), point}) == 86);
        const std::string name(point);
        const bool complete = name.find("AFTER_GUARD_REMOVE") != std::string::npos || name.find("AFTER_COMPLETE") != std::string::npos;
        if (complete) assert(Verify(fixture.backup).errors == 0U);
        else {
            assert(Verify(fixture.backup).errors > 0U);
            Throws([&] { RestoreBackup(fixture.backup, fixture.root / "refused"); });
        }
        Throws([&] { RequireNotBackupDirectory(fixture.backup); });
    }
}
void RestoreTool(const std::string& executable) {
    auto tool = std::filesystem::absolute(executable).parent_path() / "chunkdb_restore";
#ifdef _WIN32
    tool += ".exe";
#endif
    assert(Child(tool.string(), {"--help"}) == 0);
    assert(Child(tool.string(), {}) == 2);
    Fixture fixture; const auto target = fixture.root / "tool-restored";
    assert(Child(tool.string(), {fixture.backup.string(), target.string()}) == 0);
    assert(Verify(target).errors == 0U);
    assert(Child(tool.string(), {fixture.backup.string(), target.string()}) == 1);
    const auto uncertain = fixture.root / "tool-uncertain";
    {
        txn_test::ScopedEnv sync("CHUNKDB_FAILPOINT_RESTORE_COMPLETE_SYNC_FAIL_ONCE", "1");
        txn_test::ScopedEnv reinstate("CHUNKDB_FAILPOINT_RESTORE_GUARD_REINSTATE_FAIL_ONCE", "1");
        assert(Child(tool.string(), {fixture.backup.string(), uncertain.string()}) == 3);
    }
    assert(Verify(uncertain).errors == 0U);
    Save(fixture.backup / kBackupIncompleteName, {'C', 'K', 'B', 'I'});
    const auto refused = fixture.root / "tool-refused";
    assert(Child(tool.string(), {fixture.backup.string(), refused.string()}) == 1);
    assert(!std::filesystem::exists(refused));
}
} // namespace
int main(int argc, char** argv) {
    if (argc == 5 && std::string_view(argv[1]) == "--restore-child") {
        txn_test::ScopedEnv point(argv[4], "1"); RestoreBackup(argv[2], argv[3]); return 3;
    }
    if (argc == 4 && std::string_view(argv[1]) == "--backup-child") {
        auto record = ReadBackupRecord(argv[2]); txn_test::ScopedEnv point(argv[3], "1"); CompleteBackup(argv[2], record); return 3;
    }
    RestoreCrashes(argv[0]); BackupCrashes(argv[0]); RestoreTool(argv[0]);
    std::cout << "26 backup/restore crash cases and 6 restore tool cases passed\n";
}
