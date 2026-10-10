#include <iostream>
#include <stop_token>
#include "backup_restore_test_utils.hpp"
#include "slot_watch.hpp"
#include "txn_test_utils.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::backup_test;
void Records() {
    Fixture fixture;
    const auto bytes = SerializeBackupRecord(fixture.record);
    const auto record = ParseBackupRecord(bytes);
    assert(record.tables.size() == 1U && record.tables[0].epoch == fixture.manifest.store_id && record.tables[0].revision == 3U);
    assert(record.files.size() == fixture.record.files.size());
    auto bad = bytes; bad.back() ^= 1U; Throws([&] { (void)ParseBackupRecord(bad); });
    auto duplicate = fixture.record; duplicate.files.push_back(duplicate.files.front()); Throws([&] { (void)SerializeBackupRecord(duplicate); });
    auto traversal = fixture.record; traversal.files[0].relative_path = "../outside"; Throws([&] { (void)SerializeBackupRecord(traversal); });
    auto zero = fixture.record; zero.tables[0].epoch = {}; Throws([&] { (void)SerializeBackupRecord(zero); });
    assert(Verify(fixture.backup).errors == 0U);
    std::ostringstream findings; (void)VerifyDataDirectory(fixture.backup, findings);
    assert(findings.str().find("unexpected_entry") == std::string::npos);
}
void EpochAndHistory() {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        Fixture fixture(mode); const auto source_marker = LoadFile(fixture.backup / kBackupMarkerName);
        const auto source_image = LoadFile(ChunkDataPath(fixture.Table(), fixture.old_geometry, {0, 0}));
        const auto source_wal = LoadFile(ChunkWalPath(fixture.Table(), fixture.old_geometry, {0, 0}));
        const auto first = fixture.root / "first", second = fixture.root / "second";
        std::filesystem::create_directory(second); // Empty destinations are accepted.
        RestoreBackup(fixture.backup, first); RestoreBackup(fixture.backup, second);
        const auto original_directory = *ReadDataDirManifest(fixture.backup);
        const auto first_directory = *ReadDataDirManifest(first), second_directory = *ReadDataDirManifest(second);
        assert(first_directory.data_dir_id != original_directory.data_dir_id &&
            second_directory.data_dir_id != original_directory.data_dir_id &&
            first_directory.data_dir_id != second_directory.data_dir_id);
        assert(first_directory.options == original_directory.options);
        const auto one = *ReadStoreManifest(first / "tables/default"), two = *ReadStoreManifest(second / "tables/default");
        assert(one.store_id != two.store_id && one.store_id != fixture.manifest.store_id && two.store_id != fixture.manifest.store_id);
        assert(!std::filesystem::exists(first / kBackupMarkerName) && !std::filesystem::exists(first / kRestoreIncompleteName));
        assert(Verify(first).errors == 0U && Verify(second).errors == 0U);
        auto image = LoadFile(ChunkDataPath(first / "tables/default", fixture.old_geometry, {0, 0}));
        const auto crc_at = kImageFixedHeaderSize + static_cast<std::size_t>(ReadLe16(image, 10U)) * kImageSectionEntrySize;
        std::copy(source_image.begin() + 24U, source_image.begin() + 40U, image.begin() + 24U);
        std::copy(source_image.begin() + static_cast<std::ptrdiff_t>(crc_at), source_image.begin() + static_cast<std::ptrdiff_t>(crc_at + 4U), image.begin() + static_cast<std::ptrdiff_t>(crc_at));
        assert(image == source_image); // Compression, old layout and optional section preserved.
        auto wal = LoadFile(ChunkWalPath(first / "tables/default", fixture.old_geometry, {0, 0}));
        assert(ReadLe32(wal, 12U) == 0U && ReadLe32(wal, 20U) == 0U);
        assert(std::equal(wal.begin() + kWalHeaderSize, wal.end(), source_wal.begin() + kWalHeaderSize));
        auto slots = *ReadFeedSlotRecords(first / "tables/default", one.store_id);
        assert(slots.durable_watermark == 3U && slots.slots.size() == 2U);
        for (const auto& slot : slots.slots) assert(slot.written == 3U && !slot.lost);
        CatalogConfig config; config.data_dir = first; config.default_geometry = one.geometry; config.slot_sync_interval = std::chrono::hours(1);
        auto catalog = std::make_shared<TableCatalog>(config); auto table = catalog->Find("default");
        { auto lease = table->Acquire(); const auto values = lease->store().GetBlock(0, 0);
          assert(values && values->size() == 2U && std::get<std::uint64_t>((*values)[1]) == 9U);
          assert(std::get<BitsValue>((*values)[0]).digits == "00010000"); }
        FeedOptions options; options.after = FeedPosition{fixture.manifest.store_id, 3U};
        auto watch = SlotWatch::Create(table, "consumer", options); watch->Activate(); watch->WorkStep();
        const auto output = watch->Take(4096U); assert(output && output->bytes->find("resync") != std::string::npos && output->bytes->find(StoreIdHex(one.store_id)) != std::string::npos);
        watch->Consumed(output->bytes->size()); watch->Cancel(); watch->WorkStep(); watch.reset();
        assert(LoadFile(fixture.backup / kBackupMarkerName) == source_marker && LoadFile(ChunkDataPath(fixture.Table(), fixture.old_geometry, {0, 0})) == source_image);
    }
}
void StrictInventory() {
    const auto check = [](auto mutation) {
        Fixture fixture; mutation(fixture); fixture.Remark();
        Throws([&] { ValidateBackupInventory(fixture.backup, fixture.record); });
        assert(Verify(fixture.backup).errors > 0U);
        Throws([&] { RestoreBackup(fixture.backup, fixture.root / "refused"); });
        assert(!std::filesystem::exists(fixture.root / "refused"));
    };
    check([](auto& f) { f.record.tables[0].revision = 2U; });
    check([](auto& f) { Save(f.Table() / "chunkdb.version", SerializeVersionClockRecord(3U)); });
    check([](auto& f) { Save(f.Table() / "chunkdb.snapshot", SerializeSnapshotGenerationRecord(1U)); });
    check([](auto& f) { Save(f.backup / kUsersFileName, Bytes{1U, 2U}); });
    check([](auto& f) { WriteFeedSlotRecords(f.Table(), {f.manifest.store_id, 4U, {{"consumer", 4U, false}}}); });
    check([](auto& f) { auto wal = BuildWalHeader({0, 0}, f.manifest.store_id, {}); auto frame = f.Frame(4U); wal.insert(wal.end(), frame.begin(), frame.end()); Save(ChunkWalPath(f.Table(), f.old_geometry, {0, 0}), wal); });
    check([](auto& f) { std::filesystem::rename(ChunkDataPath(f.Table(), f.old_geometry, {0, 0}), f.Table() / "L_0_0/C_-0_0.chk"); });
    check([](auto& f) { Save(f.Table() / ".chunkdb.intents/txn-3.rollback", Bytes{1U}); });
    check([](auto& f) { Save(f.Table() / ".chunkdb.feed/C_0_0.1-3.wal", Bytes{1U}); });
    Fixture missing; std::filesystem::remove(missing.backup / kUsersFileName); assert(Verify(missing.backup).errors > 0U);
    Fixture damaged; Save(damaged.backup / "chunkdb.users", Bytes{9U}); assert(Verify(damaged.backup).errors > 0U);
    Fixture extra; Save(extra.backup / "foreign", Bytes{1U}); assert(Verify(extra.backup).errors > 0U);
    Fixture directory_marker; std::filesystem::remove(directory_marker.backup / kBackupMarkerName);
    std::filesystem::create_directory(directory_marker.backup / kBackupMarkerName);
    assert(Verify(directory_marker.backup).errors > 0U); RefusesOpen(directory_marker.backup);
    Fixture ordinary; std::filesystem::remove(ordinary.backup / kBackupMarkerName);
    assert(Verify(ordinary.backup).errors == 0U);
    Save(ordinary.backup / kUsersFileName, Bytes{0U}); assert(Verify(ordinary.backup).errors > 0U);
#ifndef _WIN32
    Fixture unsafe; std::filesystem::create_symlink(unsafe.backup / kUsersFileName, unsafe.backup / "link");
    assert(Verify(unsafe.backup).errors > 0U);
    for (const auto name : {kBackupMarkerName, kBackupIncompleteName, kRestoreIncompleteName}) {
        Fixture dangling; std::filesystem::remove(dangling.backup / kBackupMarkerName);
        std::filesystem::create_symlink(dangling.root / "missing", dangling.backup / name);
        assert(Verify(dangling.backup).errors > 0U); RefusesOpen(dangling.backup);
    }
#endif
}
void TargetsAndCopies() {
    Fixture fixture;
    const auto occupied = fixture.root / "occupied"; Save(occupied / "keep", {4U});
    Throws([&] { RestoreBackup(fixture.backup, occupied); }); assert(LoadFile(occupied / "keep") == Bytes{4U});
    Throws([&] { RestoreBackup(fixture.backup, fixture.backup / "child"); });
    Throws([&] { RequireBackupTarget(fixture.backup, std::filesystem::path(std::string("bad\0suffix", 10U))); });
    const auto copy = fixture.root / "copy"; std::filesystem::create_directory(copy);
    Bytes large(150000U, 0x5aU); Save(fixture.source / "large", large);
    auto record = CopyBackupFile(fixture.source / "large", copy, "large", large.size());
    assert(record.size == large.size() && record.crc32 == Crc32(large));
    assert(ReadBackupFile(fixture.source / "large", 77U) == Bytes(77U, 0x5aU));
    Throws([&] { (void)ReadBackupFile(fixture.source / "large", large.size() + 1U); });
    Throws([&] { (void)CopyBackupFile(fixture.source / "large", copy, "large", large.size()); });
    assert(LoadFile(copy / "large") == large);
    Throws([&] { (void)CopyBackupFile(fixture.source / "large", copy, "short", large.size() + 1U); });
#ifndef _WIN32
    std::filesystem::create_symlink(fixture.source / "large", copy / "linked");
    Throws([&] { (void)CopyBackupFile(fixture.source / "large", copy, "linked", 1U); }); assert(LoadFile(fixture.source / "large") == large);
#endif
    Throws([&] { RequireNotBackupDirectory(fixture.backup); });
    Throws([&] { RequireNotBackupDirectory(fixture.Table()); });
    RefusesOpen(fixture.backup);
    SyncBackupTree(fixture.backup.lexically_relative(std::filesystem::current_path()));
}
void CompletionFailures() {
    for (const std::string phase : {"BACKUP", "RESTORE"}) {
        for (const std::string failure : {"GUARD_REMOVE_FAIL", "COMPLETE_SYNC_FAIL", "UNKNOWN"}) {
            Fixture fixture; const auto target = fixture.root / "restore";
            const auto key = "CHUNKDB_FAILPOINT_" + phase + "_";
            const auto failure_key = key + (failure == "UNKNOWN" ? "COMPLETE_SYNC_FAIL" : failure) + "_ONCE";
            txn_test::ScopedEnv first(failure_key.c_str(), "1");
            const auto invoke = [&] {
                if (phase == "RESTORE") RestoreBackup(fixture.backup, target);
                else { Save(fixture.backup / kBackupIncompleteName, {'C', 'K', 'B', 'I'}); CompleteBackup(fixture.backup, fixture.record); }
            };
            if (failure == "UNKNOWN") {
                const auto reinstate_key = key + "GUARD_REINSTATE_FAIL_ONCE";
                txn_test::ScopedEnv second(reinstate_key.c_str(), "1");
                Throws<BackupPublicationUnknownError>(invoke);
                const auto published = phase == "RESTORE" ? target : fixture.backup;
                assert(Verify(published).errors == 0U); // Explicit unknown outcome may be fully committed.
            } else {
                Throws(invoke); const auto guarded = phase == "RESTORE" ? target : fixture.backup;
                assert(Verify(guarded).errors > 0U); Throws([&] { RequireNotBackupDirectory(guarded); });
                RefusesOpen(guarded);
            }
            if (phase == "RESTORE") assert(Verify(fixture.backup).errors == 0U);
        }
    }
    Fixture cancelled; Save(cancelled.backup / kBackupIncompleteName, {'C', 'K', 'B', 'I'});
    std::stop_source cancellation; cancellation.request_stop();
    Throws([&] { CompleteBackup(cancelled.backup, cancelled.record, cancellation.get_token()); });
    assert(Verify(cancelled.backup).errors > 0U);
}
void RestoreTemporaryCleanup() {
    for (unsigned failure = 0; failure != 3U; ++failure) {
        Fixture fixture;
        const auto target = fixture.root / "restore";
        if (failure == 0U) {
            // Verification permits read-only features, but restore must refuse
            // to rewrite their unknown state after copying it privately.
            fixture.manifest.features.ro_compat = 4U;
            Save(StoreManifestPath(fixture.Table()), SerializeStoreManifest(fixture.manifest));
            fixture.Remark();
            ValidateBackupInventory(fixture.backup, fixture.record);
            Throws([&] { RestoreBackup(fixture.backup, target); });
            fixture.manifest.features.ro_compat = 0U;
            Save(StoreManifestPath(fixture.Table()), SerializeStoreManifest(fixture.manifest));
            fixture.Remark();
        } else if (failure == 1U) {
            auto manifest = *ReadDataDirManifest(fixture.backup);
            manifest.features.ro_compat = 4U;
            Save(DataDirManifestPath(fixture.backup), SerializeDataDirManifest(manifest)); fixture.Remark();
            ValidateBackupInventory(fixture.backup, fixture.record);
            Throws([&] { RestoreBackup(fixture.backup, target); });
            manifest.features.ro_compat = 0U;
            Save(DataDirManifestPath(fixture.backup), SerializeDataDirManifest(manifest)); fixture.Remark();
        } else {
            txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_RESTORE_COPY_FAIL_ONCE", "1");
            Throws([&] { RestoreBackup(fixture.backup, target); });
        }
        assert(!std::filesystem::exists(target));
        for (const auto& entry : std::filesystem::directory_iterator(fixture.root))
            assert(entry.path().filename().string().rfind(".chunkdb.restore.", 0U) != 0U);
        RestoreBackup(fixture.backup, target);
        assert(Verify(target).errors == 0U);
    }
}
void CrashConsistentWal() {
    for (unsigned defect = 0; defect != 3U; ++defect) {
        Fixture fixture;
        const auto path = ChunkWalPath(fixture.Table(), fixture.old_geometry, {0, 0});
        auto wal = LoadFile(path);
        if (defect == 0U) {
            auto tail = fixture.Frame(4U); tail.pop_back(); wal.insert(wal.end(), tail.begin(), tail.end());
        } else if (defect == 1U) wal.resize(kWalHeaderSize / 2U);
        else { wal.back() ^= 1U; auto later = fixture.Frame(4U); wal.insert(wal.end(), later.begin(), later.end()); }
        Save(path, wal); fixture.Remark();
        const auto target = fixture.root / "restored";
        if (defect == 2U) {
            Throws([&] { RestoreBackup(fixture.backup, target); });
            assert(!std::filesystem::exists(target));
        } else {
            ValidateBackupInventory(fixture.backup, fixture.record);
            RestoreBackup(fixture.backup, target);
            std::ostringstream findings;
            assert(VerifyDataDirectory(target, findings).errors == 0U);
            assert(findings.str().find("wal_tail_truncated") == std::string::npos && findings.str().find("wal_torn_creation") == std::string::npos);
            if (defect == 1U) assert(!std::filesystem::exists(ChunkWalPath(target / "tables/default", fixture.old_geometry, {0, 0})));
            else assert(std::filesystem::file_size(ChunkWalPath(target / "tables/default", fixture.old_geometry, {0, 0})) == wal.size() - fixture.Frame(4U).size() + 1U);
        }
    }
}
void SymlinkedRootsAndStaging() {
#ifndef _WIN32
    Fixture fixture;
    const auto alias = fixture.root / "alias";
    std::filesystem::create_directory_symlink(fixture.backup, alias);
    assert(Verify(alias).errors == 0U);
    RestoreBackup(alias, fixture.root / "restored");
    const auto source_alias = fixture.root / "source-alias";
    std::filesystem::create_directory_symlink(fixture.source, source_alias);
    PrepareBackupTarget(source_alias, fixture.root / "second-backup");
    const auto parent_alias = fixture.root / "parent-alias";
    std::filesystem::create_directory_symlink(fixture.root, parent_alias);
    RestoreBackup(alias, parent_alias / "another-restored");
#endif
    Fixture staging;
    std::filesystem::remove(staging.backup / kBackupMarkerName);
    const auto prior = Verify(staging.backup);
    Save(staging.backup / kBackupStagingName / "interrupted" / "link", {1U});
    std::ostringstream findings;
    const auto verified = VerifyDataDirectory(staging.backup, findings);
    assert(verified.errors == 0U && verified.warnings == prior.warnings + 1U);
    assert(findings.str().find("interrupted_backup") != std::string::npos);
}
void RootedTargets() {
    Fixture fixture;
    const auto root = fixture.root / "destinations";
    std::filesystem::create_directory(root);
    assert(ResolveBackupTarget(root, "daily/nested") == root / "daily/nested");
    assert(ResolveBackupTarget(root, "./daily") == root / "daily");
    Throws<std::invalid_argument>([&] { (void)ResolveBackupTarget({}, "daily"); });
    Throws<std::invalid_argument>([&] { (void)ResolveBackupTarget(root, {}); });
    Throws<std::invalid_argument>([&] { (void)ResolveBackupTarget(root, root / "absolute"); });
    for (const auto* name : {"..", "daily/../escaped", ".", "back\\slash"})
        Throws<std::invalid_argument>([&] { (void)ResolveBackupTarget(root, name); });
#ifndef _WIN32
    std::filesystem::create_directory(root / "inside");
    std::filesystem::create_directory_symlink(root / "inside", root / "linked-inside");
    std::filesystem::create_directory_symlink(fixture.source, root / "linked-outside");
    for (const auto* name : {"linked-inside", "linked-inside/child", "linked-outside/child"})
        Throws<std::invalid_argument>([&] { (void)ResolveBackupTarget(root, name); });
    const auto alias = fixture.root / "destinations-alias";
    std::filesystem::create_directory_symlink(root, alias);
    assert(ResolveBackupTarget(alias, "daily") == root / "daily");
#endif
}
} // namespace
int main(int argc, char** argv) {
    if (argc == 2) {
        const std::string_view name(argv[1]);
        if (name == "identity") EpochAndHistory();
        else if (name == "cleanup") RestoreTemporaryCleanup();
        else if (name == "cold") CrashConsistentWal();
        else if (name == "symlinks") SymlinkedRootsAndStaging();
        else return 2;
        return 0;
    }
    Records(); EpochAndHistory(); StrictInventory(); TargetsAndCopies(); CompletionFailures();
    RestoreTemporaryCleanup(); CrashConsistentWal(); SymlinkedRootsAndStaging(); RootedTargets();
    std::cout << "9 backup restore groups passed\n";
}
