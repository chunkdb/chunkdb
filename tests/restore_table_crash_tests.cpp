// RESTORE TABLE, ALTER TABLE ... RENAME TO and SWAP TABLES child processes stop
// at each step; the next writer start serves each table completely, under its
// old or new name.
#include <algorithm>
#include <cstdlib>
#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "migrations_records.hpp"
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


// A SWAP TABLES child process stops before the exchange, after it, and after
// its sync; the next start serves both names exchanged or neither.
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



// Repeat one maximum-verifier-size user record under distinct names, using
// the normal users encoder for the record and the seed's admin entry.
void WriteLargeGrantUsers(const std::filesystem::path& path) {
    Users users;
    auto& admin = users.users["admin"];
    admin.verifier.iterations = scram::kMinIterations;
    admin.verifier.salt.resize(scram::kMaxSaltBytes, 1U);
    admin.manages_users = true;
    admin.grants = {{kEveryTable, Right::kAdmin}, {"t", Right::kWrite}};
    auto bytes = EncodeUsers(users);
    Users padding;
    constexpr std::size_t name_bytes = 63U;
    auto& filler = padding.users[std::string(name_bytes, 'z')];
    filler.verifier = admin.verifier;
    filler.added_verifier = filler.verifier;
    filler.added_verifier->stored_key[0] = 1U;
    auto record = EncodeUsers(padding);
    constexpr std::size_t count_at = 4U + 4U + 32U;
    constexpr std::size_t header_bytes = count_at + 4U;
    constexpr std::size_t checksum_bytes = 4U;
    record.resize(record.size() - checksum_bytes);
    record.erase(record.begin(), record.begin() + static_cast<std::ptrdiff_t>(header_bytes));
    bytes.resize(bytes.size() - checksum_bytes);
    constexpr auto limit = static_cast<std::size_t>(kMaxUsersFileBytes);
    constexpr std::size_t headroom = 16U; // RENAME to t_old adds four bytes.
    const auto fillers = static_cast<std::uint32_t>((limit - headroom - checksum_bytes - bytes.size()) / record.size());
    bytes.reserve(limit);
    const auto count = fillers + 1U;
    for (std::size_t i = 0; i < 4U; ++i) bytes[count_at + i] = static_cast<std::uint8_t>(count >> (i * 8U));
    for (std::uint32_t i = 0; i < fillers; ++i) {
        auto digits = i;
        for (std::size_t end = 2U + name_bytes; end > 2U + name_bytes - 8U; --end) {
            record[end - 1U] = static_cast<std::uint8_t>("0123456789abcdef"[digits & 15U]);
            digits >>= 4U;
        }
        bytes.insert(bytes.end(), record.begin(), record.end());
    }
    WriteLe32(bytes, Crc32(bytes));
    assert(bytes.size() <= limit - headroom && bytes.size() > limit - headroom - record.size());
    ValidateUsersImage(bytes);
    AtomicWrite(path, bytes, true, true);
}

// DROP TABLE and RENAME TO with grants on the table stop after the grant
// journal, after the directory move and around the users write; the next
// start serves the table with its grants, or the change whole: no table and
// no grants after a drop, both under the new name after a rename.
void GrantCrashes(const std::string& executable, bool large) {
    test::ScopedTempDir fixtures("chunkdb-grant-crash-fixtures");
    const auto seed = fixtures.path() / kUsersFileName;
    if (large) WriteLargeGrantUsers(seed);
    const std::array<std::string, 3U> common{"CHUNKDB_FAILPOINT_CRASH_MIGRATION_FILE_0_BEFORE_PUBLISH_ONCE",
        "CHUNKDB_FAILPOINT_CRASH_MIGRATION_FILE_0_AFTER_PUBLISH_ONCE",
        "CHUNKDB_FAILPOINT_CRASH_MIGRATION_BEFORE_PENDING_REMOVE_ONCE"};
    for (const bool rename : {false, true}) {
        std::vector<std::string> points{rename ? "CHUNKDB_FAILPOINT_CRASH_RENAME_TABLE_AFTER_JOURNAL_ONCE"
                                               : "CHUNKDB_FAILPOINT_CRASH_TABLE_DROP_AFTER_JOURNAL_ONCE",
            rename ? "CHUNKDB_FAILPOINT_CRASH_RENAME_TABLE_AFTER_RENAME_ONCE" : "CHUNKDB_FAILPOINT_CRASH_TABLE_DROP_AFTER_RENAME_ONCE"};
        if (rename) points.push_back("CHUNKDB_FAILPOINT_CRASH_RENAME_TABLE_AFTER_SYNC_ONCE");
        points.insert(points.end(), common.begin(), common.end());
        if (large) points.emplace_back(); // Complete the statement without a crash as well.
        for (const auto& point : points) {
            Source source;
            if (large) {
                assert(std::filesystem::copy_file(seed, source.data / kUsersFileName));
            } else {
                TableCatalog catalog(source.Config());
                OpenUsers(source.data)->Grant("admin", "t", Right::kWrite);
            }
            std::vector<std::string> args{rename ? "--rename-grants-child" : "--drop-grants-child", source.data.string(),
                point.empty() ? "complete" : point};
            if (large) args.emplace_back("--without-registry");
            assert(Child(executable, args) == (point.empty() ? 0 : 86));
            if (point.empty()) assert(!std::filesystem::exists(source.data / kMigrationPendingFileName));
            const bool before_move = point.find("AFTER_JOURNAL") != std::string::npos;
            {
                TableCatalog catalog(source.Config());
                assert(!ReadMigrationJournal(source.data));
                const auto users = ReadUsersFile(source.data);
                assert(users);
                const auto& grants = users->users.at("admin").grants;
                const std::string name = before_move ? "t" : rename ? "t_old" : "";
                if (name.empty()) {
                    assert(catalog.Find("t") == nullptr && catalog.TableCount() == 0U);
                } else {
                    assert(catalog.Require(name)->store_id() == source.epoch && catalog.TableCount() == 1U);
                    assert(grants.at(name) == Right::kWrite);
                }
                // Nothing names a table that is not there.
                for (const auto* other : {"t", "t_old"})
                    if (other != name) assert(!grants.contains(other));
                if (large) assert(std::filesystem::file_size(source.data / kUsersFileName) > kMaxUsersFileBytes - 512U);
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
    if ((argc == 4 || (argc == 5 && std::string_view(argv[4]) == "--without-registry")) &&
        (std::string_view(argv[1]) == "--drop-grants-child" || std::string_view(argv[1]) == "--rename-grants-child")) {
        TableCatalog catalog(ChildConfig(argv[2], "relaxed"));
        const auto users = argc == 4 ? OpenUsers(argv[2]) : nullptr;
        std::optional<txn_test::ScopedEnv> point;
        if (std::string_view(argv[3]) != "complete") point.emplace(argv[3], "1");
        if (std::string_view(argv[1]) == "--drop-grants-child") catalog.Drop("t", users.get(), false);
        else catalog.RenameTable("t", "t_old", users.get());
        return point ? 3 : 0;
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
        txn_test::ScopedEnv point(argv[3], "1");
        catalog.SwapTables("t", "t_restored");
        return 3;
    }
    Crashes(argv[0]);
    GrantCrashes(argv[0], false);
    GrantCrashes(argv[0], true);
    RenameCrashes(argv[0]);
#ifdef _WIN32
    // Windows refuses SWAP TABLES before it closes anything.
    constexpr bool kAtomicExchange = false;
#else
    constexpr bool kAtomicExchange = true;
#endif
    if (!kAtomicExchange) {
        std::cout << "16 restore table, 6 rename table and 24 grant cases passed\n";
        return 0;
    }
    SwapCrashes(argv[0]);
    std::cout << "16 restore table, 6 rename table, 24 grant and 6 swap tables cases passed\n";
}
