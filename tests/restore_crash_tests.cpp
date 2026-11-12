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
constexpr std::array<const char*, 12U> kRestorePoints{
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_VERIFY_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_GUARD_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_COPY_FILE_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_COPY_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_FILE_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_FORK_CLOCK_ONCE",
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
                else { assert(Verify(target).errors > 0U); Throws([&] { RequireNotBackupDirectory(target); }); RefusesOpen(target); }
            }
            for (const auto& entry : std::filesystem::directory_iterator(fixture.root))
                if (entry.path().filename().string().rfind(".chunkdb.restore.", 0U) == 0U) {
                    assert(std::filesystem::exists(entry.path() / kRestoreIncompleteName));
                    Throws([&] { RequireNotBackupDirectory(entry.path()); });
                    RefusesOpen(entry.path());
                    try {
                        RestoreBackup(fixture.backup, fixture.root / "retry");
                        assert(false && "crashed temporary copy must be identified");
                    } catch (const std::exception& error) {
                        assert(std::string(error.what()).find(entry.path().string()) != std::string::npos);
                    }
                }
        }
    }
}
// Restore gives every coordinate log and then the inventory the new table
// identity. A crash between them leaves only a refused temporary copy; the
// backup stays intact, and a restore after removing that copy returns every
// acknowledged write, version and tombstone.
void RestoreIndexCrashes(const std::string& executable) {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        for (const auto* point : {"CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_INDEX_LOG_ONCE",
                 "CHUNKDB_FAILPOINT_CRASH_RESTORE_AFTER_REWRITE_INDEX_ONCE"}) {
            test::ScopedTempDir temporary("chunkdb-restore-index-crash");
            const auto root = std::filesystem::canonical(temporary.path());
            CatalogConfig config; config.data_dir = root / "source"; config.background_maintenance = false;
            config.default_options.durability_mode = mode;
            StoreId epoch{};
            std::uint64_t live = 0U, cut = 0U;
            std::array<std::uint64_t, 2U> deleted{};
            {
                TableCatalog catalog(config);
                GeometryConfig geometry; geometry.large_chunk_width_chunks = 2U; geometry.large_chunk_height_chunks = 2U;
                geometry.chunk_width_blocks = 2U; geometry.chunk_height_blocks = 2U; geometry.block_bits = 8U;
                const auto table = catalog.Create("default", geometry, catalog.default_options());
                epoch = table->store_id();
                auto lease = table->Acquire();
                auto& store = lease->store();
                store.SetBlockBits(8, 0, "00000011"); live = store.GetChunkVersion(4, 0);
                // Tombstones in two large chunks give the inventory two logs.
                for (std::size_t i = 0; i < deleted.size(); ++i) {
                    const auto x = static_cast<std::int64_t>(4U * i);
                    store.SetBlockBits(x, 0, "10000000"); store.UnsetBlock(x, 0);
                    deleted[i] = store.GetChunkVersion(2 * static_cast<std::int64_t>(i), 0);
                    store.CheckpointForTests(2 * static_cast<std::int64_t>(i), 0);
                }
                assert(store.RetainedTombstoneCount() == 2U);
                store.WalBarrier();
                lease.reset();
                const auto result = catalog.BackupTo(root / "backup", {});
                assert(result.tables.size() == 1U && result.tables.front().epoch == epoch);
                cut = result.tables.front().revision;
            }
            const auto backup = root / "backup", target = root / "restored";
            const auto marker = InspectBackupFile(backup, kBackupMarkerName);
            assert(Verify(backup).errors == 0U);
            assert(Child(executable, {"--restore-child", backup.string(), target.string(), point}) == 86);
            assert(InspectBackupFile(backup, kBackupMarkerName).crc32 == marker.crc32 && Verify(backup).errors == 0U);
            assert(!std::filesystem::exists(target));
            std::vector<std::filesystem::path> copies;
            for (const auto& entry : std::filesystem::directory_iterator(root))
                if (entry.path().filename().string().rfind(".chunkdb.restore.", 0U) == 0U) copies.push_back(entry.path());
            assert(copies.size() == 1U && std::filesystem::exists(copies.front() / kRestoreIncompleteName));
            assert(Verify(copies.front()).errors > 0U);
            RefusesOpen(copies.front());
            assert(!RemoveBackupTemporaryTree(copies.front()));
            RestoreBackup(backup, target);
            assert(Verify(target).errors == 0U);
            auto restored = config; restored.data_dir = target;
            TableCatalog catalog(restored);
            const auto table = catalog.Require("default");
            assert(table->store_id() != epoch);
            auto lease = table->Acquire();
            auto& store = lease->store();
            assert(store.GetBlockBits(8, 0) == "00000011" && store.GetChunkVersion(4, 0) == live);
            assert(store.RetainedTombstoneCount() == 2U);
            const auto page = store.ScanChunkIdentities(false, {}, 10U, 0U, false);
            assert(page.entries.size() == 3U && page.position && *page.position >= cut);
            assert((page.entries[0].coord == ChunkCoord{0, 0}) && page.entries[0].version == deleted[0]);
            assert((page.entries[1].coord == ChunkCoord{2, 0}) && page.entries[1].version == deleted[1]);
            assert((page.entries[2].coord == ChunkCoord{4, 0}) && page.entries[2].version == live);
            for (std::size_t i = 0; i < deleted.size(); ++i) {
                const auto state = store.ReadChunkState(2 * static_cast<std::int64_t>(i), 0);
                assert(state.written && state.version == deleted[i]);
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
        RefusesOpen(fixture.backup, complete);
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
void LostPostForkMutation(const std::string& executable) {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        Fixture fixture(mode); const auto target = fixture.root / "lost-post-fork";
        RestoreBackup(fixture.backup, target);
        const auto manifest = *ReadStoreManifest(target / "tables/default");
        WriteFeedSlotRecords(target / "tables/default", {manifest.store_id, 3U, {}});
        assert(Child(executable, {"--lineage-lost-child", target.string()}) == 86);
        CatalogConfig config; config.data_dir = target; auto catalog = std::make_shared<TableCatalog>(config);
        auto table = catalog->Find("default"); FeedOptions options; options.after = FeedPosition{fixture.manifest.store_id, 3U};
        auto watch = table->SubscribeFeed(options); const auto resync = watch->Next(std::chrono::seconds(5));
        assert(resync && resync->kind == FeedEntry::Kind::kResync);
        assert(resync->protocol_frame && resync->protocol_frame->find("$32\r\n" + StoreIdHex(fixture.manifest.store_id) + "\r\n:3\r\n") != std::string::npos);
        assert(!table->Acquire()->store().GetBlock(0, 0));
    }
}
} // namespace
int main(int argc, char** argv) {
    if (argc == 3 && std::string_view(argv[1]) == "--lineage-lost-child") {
        CatalogConfig config; config.data_dir = argv[2]; TableCatalog catalog(config);
        auto table = catalog.Find("default"); auto lease = table->Acquire();
        lease->store().SetBlock(0, 0, {{"extra", std::uint64_t{10U}}});
        lease->store().UnsetBlock(0, 0); lease->store().CheckpointForTests(0, 0);
        lease->store().FlushSnapshotGenerationLingerForTests();
        assert(!std::filesystem::exists(ChunkDataPath(lease->store().data_dir(), lease->store().geometry(), {0, 0})));
        assert(!std::filesystem::exists(ChunkWalPath(lease->store().data_dir(), lease->store().geometry(), {0, 0})));
        std::_Exit(86);
    }
    if (argc == 5 && std::string_view(argv[1]) == "--restore-child") {
        txn_test::ScopedEnv point(argv[4], "1"); RestoreBackup(argv[2], argv[3]); return 3;
    }
    if (argc == 4 && std::string_view(argv[1]) == "--backup-child") {
        auto record = ReadBackupRecord(argv[2]); txn_test::ScopedEnv point(argv[3], "1"); CompleteBackup(argv[2], record); return 3;
    }
    RestoreCrashes(argv[0]); RestoreIndexCrashes(argv[0]); BackupCrashes(argv[0]); RestoreTool(argv[0]); LostPostForkMutation(argv[0]);
    std::cout << "34 backup/restore crash cases and 6 restore tool cases passed\n";
}
