// RESTORE TABLE, ALTER TABLE ... RENAME TO and SWAP TABLES child processes stop
// at each step; the next writer start serves each table completely, under its
// old or new name.
#include <algorithm>
#include <cstdlib>
#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "admin.hpp"
#include "chunkdb/crc32.hpp"
#include "feature_activation_records.hpp"
#include "restore_table_test_utils.hpp"
#include "table_swap_guard.hpp"
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


// A SWAP TABLES child process stops before the exchange, after it, and after
// its sync; the next start serves both names exchanged or neither.
#ifndef _WIN32
void SwapCrashes(const std::string& executable) {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        for (const std::string point : {"CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_BEFORE_EXCHANGE_ONCE",
                 "CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_EXCHANGE_ONCE",
                 "CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_SYNC_ONCE"}) {
            Source source(mode);
            StoreId restored{};
            {
                TableCatalog catalog(source.Config());
                restored = catalog.RestoreTable(source.Request()).epoch;
            }
            assert(Child(executable, {"--swap-child", source.data.string(), point,
                       mode == DurabilityMode::kFsyncWal ? "fsync-wal" : "relaxed"}) == 86);
            const bool exchanged = point.find("BEFORE_EXCHANGE") == std::string::npos;
            {
                TableCatalog catalog(source.Config());
                assert(catalog.TableCount() == 2U && catalog.Fences().empty());
                const auto copy = catalog.Require(exchanged ? "t" : "t_restored");
                const auto live = catalog.Require(exchanged ? "t_restored" : "t");
                assert(copy->store_id() == restored && live->store_id() == source.epoch);
                assert(copy->Acquire()->store().GetBlockBits(8, 0) == "00000011");
                assert(live->Acquire()->store().GetBlockBits(8, 0) == "11111111");
                // The slots were marked before the exchange: every crash point keeps the marks.
                assert(copy->ListFeedSlots().at(0).resync && live->ListFeedSlots().at(0).resync);
                // The swap can run again after the restart.
                catalog.SwapTables("t", "t_restored");
                assert(catalog.Require(exchanged ? "t_restored" : "t")->store_id() == restored);
            }
            Verified(source.data);
        }
    }
}
#else
constexpr std::array<const char*, 3U> kGuardedRenames{
    "CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_PARK_FIRST_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_MOVE_SECOND_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_MOVE_PARKED_ONCE"};

constexpr const char* kAfterGuardSync = "CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_GUARD_SYNC_ONCE";

const char* ModeName(DurabilityMode mode) {
    return mode == DurabilityMode::kFsyncWal ? "fsync-wal" : "relaxed";
}

StoreId CrashGuardedSwap(const std::string& executable, const Source& source, const char* point) {
    StoreId restored{};
    {
        TableCatalog catalog(source.Config());
        restored = catalog.RestoreTable(source.Request()).epoch;
    }
    assert(Child(executable, {"--swap-child", source.data.string(), point, ModeName(source.mode)}) == 86);
    return restored;
}

void CheckGuardedSwapRestart(const Source& source, const StoreId& restored, bool exchanged) {
    {
        TableCatalog catalog(source.Config());
        assert(catalog.TableCount() == 2U && catalog.Fences().empty());
        assert(!TableSwapPending(source.data) && Entries(source.data / "tables") == 2U);
        for (const auto& entry : std::filesystem::directory_iterator(source.data))
            assert(!entry.path().filename().string().starts_with("chunkdb.swap.pending.tmp."));
        const auto copy = catalog.Require(exchanged ? "t" : "t_restored");
        const auto live = catalog.Require(exchanged ? "t_restored" : "t");
        assert(copy->store_id() == restored && live->store_id() == source.epoch);
        assert(copy->lineage() && *copy->lineage() == (TableLineage{source.epoch, source.cut}));
        assert(copy->Acquire()->store().GetBlockBits(8, 0) == "00000011");
        assert(live->Acquire()->store().GetBlockBits(8, 0) == "11111111");
        assert(live->Acquire()->store().GetBlockBits(0, 1) == "00010000");
        assert(copy->ListFeedSlots().at(0).resync && live->ListFeedSlots().at(0).resync);
        catalog.SwapTables("t", "t_restored");
        assert(catalog.Require(exchanged ? "t_restored" : "t")->store_id() == restored);
    }
    Verified(source.data);
}

void SwapCrashes(const std::string& executable) {
    struct Point {
        const char* key = nullptr;
        bool exchanged = true;
        bool pending = true;
    };
    constexpr std::array points{
        Point{"CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_BEFORE_EXCHANGE_ONCE", false, false},
        Point{"CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_BEFORE_GUARD_PUBLISH_ONCE", false, false},
        Point{"CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_GUARD_PUBLISH_ONCE"},
        Point{kAfterGuardSync},
        Point{kGuardedRenames[0]}, Point{kGuardedRenames[1]}, Point{kGuardedRenames[2]},
        Point{"CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_TABLES_SYNC_ONCE"},
        Point{"CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_GUARD_UNLINK_ONCE", true, false},
        Point{"CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_GUARD_REMOVE_ONCE", true, false},
        Point{"CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_REOPEN_ONCE", true, false}};
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        for (const auto& point : points) {
            Source source(mode);
            const auto restored = CrashGuardedSwap(executable, source, point.key);
            assert(TableSwapPending(source.data) == point.pending);
            CheckGuardedSwapRestart(source, restored, point.exchanged);
        }
        // Interrupt three successive writer starts, once after each remaining
        // move. Every start resumes from the identities left by its predecessor.
        Source source(mode);
        const auto restored = CrashGuardedSwap(executable, source, kAfterGuardSync);
        for (const auto* point : kGuardedRenames) {
            assert(Child(executable, {"--swap-recovery-child", source.data.string(), point, ModeName(mode)}) == 86);
            assert(ValidatePendingTableSwap(source.data));
        }
        CheckGuardedSwapRestart(source, restored, true);
    }
}

void SwapGuardAdmissionAndDamage(const std::string& executable) {
    Source source;
    const auto restored = CrashGuardedSwap(executable, source, kGuardedRenames[0]);
    const auto pending = source.data / kTableSwapPendingFileName;
    assert(!std::filesystem::exists(source.data / "tables/t"));
    const auto expect_pending = [&](CatalogConfig config) {
        assert(Throws([&] { TableCatalog refused(std::move(config)); }).find(pending.filename().string()) != std::string::npos);
    };
    auto read_only = source.Config();
    read_only.access_mode = AccessMode::kReadOnly;
    expect_pending(read_only);
    auto multi_process = source.Config();
    multi_process.allow_multiple_processes = true;
    expect_pending(multi_process);
    {
        std::ostringstream findings;
        assert(VerifyDataDirectory(source.data, findings).errors == 0U);
        assert(findings.str().find("swap_recovery_pending") != std::string::npos);
        assert(Throws([&] { (void)RebuildVersionClock(source.data, findings); })
                   .find(pending.filename().string()) != std::string::npos);
    }
    const auto root = ReadDataDirManifest(source.data);
    assert(root);
    assert(Throws([&] { ValidateFeatureInventory(source.data, root->data_dir_id, {{"t_restored", restored}}); })
               .find(pending.filename().string()) != std::string::npos);

    const auto original = LoadFile(pending);
    const auto reject = [&] {
        const auto before = LoadFile(pending);
        expect_pending(source.Config());
        assert(LoadFile(pending) == before && !std::filesystem::exists(source.data / "tables/t"));
        std::ostringstream findings;
        assert(VerifyDataDirectory(source.data, findings).errors != 0U);
        assert(findings.str().find("swap_pending_invalid") != std::string::npos);
    };
    const auto damaged = [&](auto corrupt, bool fix_checksum = true) {
        auto bytes = original;
        bytes.resize(bytes.size() - 4U);
        corrupt(bytes);
        WriteLe32(bytes, Crc32(bytes) ^ (fix_checksum ? 0U : 1U));
        AtomicWrite(pending, bytes, true, true);
        reject();
    };
    constexpr std::size_t kFirstNameLength = 24U;
    const std::size_t first_id = kFirstNameLength + 1U + original[kFirstNameLength];
    const std::size_t second_length = first_id + StoreId{}.size();
    const std::size_t second_id = second_length + 1U + original[second_length];
    damaged([](auto& bytes) { bytes[0] ^= 1U; }); // magic
    damaged([](auto& bytes) { bytes[4] = 2U; }); // version
    damaged([](auto& bytes) { bytes[6] = 1U; }); // reserved
    damaged([](auto&) {}, false); // CRC
    damaged([](auto& bytes) { bytes[kFirstNameLength] = 65U; }); // name length
    damaged([](auto& bytes) { bytes[kFirstNameLength + 1U] = '.'; }); // unsafe name
    damaged([](auto& bytes) { bytes[kFirstNameLength + 1U] = 'z'; }); // name order
    damaged([&](auto& bytes) { std::fill_n(bytes.data() + first_id, StoreId{}.size(), 0U); });
    damaged([&](auto& bytes) { std::copy_n(bytes.data() + first_id, StoreId{}.size(), bytes.data() + second_id); });
    damaged([](auto& bytes) { bytes[8] ^= 1U; }); // different directory
    damaged([&](auto& bytes) { bytes[second_id] ^= 1U; }); // different table
    damaged([](auto& bytes) { bytes.pop_back(); }); // truncated suffix
    damaged([](auto& bytes) { bytes.push_back(0U); }); // trailing data

    AtomicWrite(pending, original, true, true);
    const auto manifest = StoreManifestPath(source.data / "tables/t_restored");
    const auto renamed_manifest = manifest.parent_path() / "saved.manifest";
    std::filesystem::rename(manifest, renamed_manifest);
    reject();
    std::filesystem::rename(renamed_manifest, manifest);
    // Both live names are absent, a layout no step can leave.
    const auto misplaced = source.data / "tables/extra";
    std::filesystem::rename(manifest.parent_path(), misplaced);
    reject();
    std::filesystem::rename(misplaced, manifest.parent_path());
    CheckGuardedSwapRestart(source, restored, true);
}
#endif

// An ALTER TABLE ... RENAME TO child process stops before the rename, after
// it, and after its sync; the next start serves the table, with its epoch,
// data and slot, under exactly one of the names.
void RenameCrashes(const std::string& executable) {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        for (const std::string point : {"CHUNKDB_FAILPOINT_CRASH_RENAME_TABLE_BEFORE_RENAME_ONCE",
                 "CHUNKDB_FAILPOINT_CRASH_RENAME_TABLE_AFTER_RENAME_ONCE",
                 "CHUNKDB_FAILPOINT_CRASH_RENAME_TABLE_AFTER_SYNC_ONCE"}) {
            Source source(mode);
            assert(Child(executable, {"--rename-child", source.data.string(), point,
                       mode == DurabilityMode::kFsyncWal ? "fsync-wal" : "relaxed"}) == 86);
            const bool renamed = point.find("BEFORE_RENAME") == std::string::npos;
            {
                TableCatalog catalog(source.Config());
                assert(catalog.TableCount() == 1U && catalog.Fences().empty());
                assert(catalog.Find(renamed ? "t" : "t_old") == nullptr);
                const auto table = catalog.Require(renamed ? "t_old" : "t");
                assert(table->store_id() == source.epoch);
                {
                    auto lease = table->Acquire();
                    assert(lease->store().GetBlockBits(8, 0) == "11111111");
                    assert(lease->store().GetBlockBits(0, 1) == "00010000");
                    assert(lease->store().GetBlockBits(9, 3) == "00000101");
                }
                const auto slots = table->ListFeedSlots();
                assert(slots.size() == 1U && !slots[0].resync && slots[0].position.epoch == source.epoch);
                // The rename can run again after the restart.
                if (renamed) catalog.RenameTable("t_old", "t", nullptr);
                else catalog.RenameTable("t", "t_old", nullptr);
                assert(catalog.Require(renamed ? "t" : "t_old")->store_id() == source.epoch);
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
    if (argc == 5 && std::string_view(argv[1]) == "--rename-child") {
        TableCatalog catalog(ChildConfig(argv[2], argv[4]));
        // Acknowledged before the rename, so every crash point keeps it.
        catalog.Require("t")->Acquire()->store().SetBlockBits(0, 1, "00010000");
        txn_test::ScopedEnv point(argv[3], "1");
        catalog.RenameTable("t", "t_old", nullptr);
        return 3;
    }
    if (argc == 5 && std::string_view(argv[1]) == "--swap-child") {
        TableCatalog catalog(ChildConfig(argv[2], argv[4]));
#ifdef _WIN32
        // In relaxed mode this acknowledged write relies on the swap's barrier.
        catalog.Require("t")->Acquire()->store().SetBlockBits(0, 1, "00010000");
#endif
        txn_test::ScopedEnv point(argv[3], "1");
        catalog.SwapTables("t", "t_restored");
        return 3;
    }
#ifdef _WIN32
    if (argc == 5 && std::string_view(argv[1]) == "--swap-recovery-child") {
        txn_test::ScopedEnv point(argv[3], "1");
        TableCatalog catalog(ChildConfig(argv[2], argv[4]));
        return 3;
    }
#endif
    Crashes(argv[0]);
    RenameCrashes(argv[0]);
    SwapCrashes(argv[0]);
#ifdef _WIN32
    SwapGuardAdmissionAndDamage(argv[0]);
#endif
    std::cout << "restore table, rename table and swap tables crash cases passed\n";
}
