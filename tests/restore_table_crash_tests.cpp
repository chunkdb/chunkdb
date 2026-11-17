// A RESTORE TABLE child process stops at each step; the next writer start
// serves the restored table completely or not at all.
#include <cstdlib>
#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "restore_table_test_utils.hpp"
#include "txn_test_utils.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::restore_table_test;

// In the order a restore reaches them; the last one follows publication.
constexpr std::array<const char*, 8U> kPoints{
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_TABLE_AFTER_COPY_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_FILE_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_INDEX_LOG_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_INDEX_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_FORK_CLOCK_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_TABLE_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_TABLE_BEFORE_PUBLISH_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_TABLE_AFTER_PUBLISH_ONCE"};

std::string Quote(const std::string& argument) {
#ifdef _WIN32
    assert(argument.find('"') == std::string::npos);
    return "\"" + argument + "\"";
#else
    std::string result = "'";
    for (char ch : argument) result += ch == '\'' ? "'\\''" : std::string(1U, ch);
    return result + "'";
#endif
}

int Child(const std::string& executable, const std::vector<std::string>& arguments) {
    std::string command = Quote(executable);
    for (const auto& argument : arguments) command += " " + Quote(argument);
#ifdef _WIN32
    return std::system(("\"" + command + "\"").c_str());
#else
    const auto status = std::system(command.c_str());
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
#endif
}

CatalogConfig ChildConfig(const std::filesystem::path& data, std::string_view mode) {
    CatalogConfig config;
    config.data_dir = data;
    config.background_maintenance = false;
    config.default_options.durability_mode = mode == "fsync-wal" ? DurabilityMode::kFsyncWal : DurabilityMode::kRelaxed;
    return config;
}

void Crashes(const std::string& executable) {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        for (std::size_t at = 0U; at < kPoints.size(); ++at) {
            const std::string point = kPoints[at];
            Source source(mode);
            const auto marker = InspectBackupFile(source.backup, kBackupMarkerName);
            assert(Child(executable, {"--restore-table-child", source.data.string(), source.backup.string(), point,
                       mode == DurabilityMode::kFsyncWal ? "fsync-wal" : "relaxed"}) == 86);
            // The backup is only read.
            assert(InspectBackupFile(source.backup, kBackupMarkerName).crc32 == marker.crc32);
            Verified(source.backup);
            const bool published = at + 1U == kPoints.size();
            {
                // The writer start removes an unpublished staging copy.
                TableCatalog catalog(source.Config());
                assert(Entries(source.data / ".chunkdb.staging") == 0U);
                if (published) {
                    const auto table = catalog.Find("t_restored");
                    assert(table != nullptr);
                    source.CheckRestored(catalog, "t_restored", table->store_id());
                } else {
                    assert(catalog.Find("t_restored") == nullptr && catalog.TableCount() == 1U);
                    assert(!std::filesystem::exists(source.data / "tables/t_restored"));
                    const auto result = catalog.RestoreTable(source.Request());
                    source.CheckRestored(catalog, "t_restored", result.epoch);
                }
            }
            Verified(source.data);
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 6 && std::string_view(argv[1]) == "--restore-table-child") {
        TableCatalog catalog(ChildConfig(argv[2], argv[5]));
        txn_test::ScopedEnv point(argv[4], "1");
        (void)catalog.RestoreTable(TableRestoreRequest{.backup = argv[3], .table = "t", .name = "t_restored"});
        return 3;
    }
    Crashes(argv[0]);
    std::cout << "16 restore table crash cases passed\n";
}
