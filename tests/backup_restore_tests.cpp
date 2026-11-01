#include <iostream>
#include <stop_token>
#include "feed_phase_watchdog.hpp"
#include "backup_restore_test_utils.hpp"

#include <cstdlib>
#include "slot_watch.hpp"
#include "txn_test_utils.hpp"
#include "directory_alias_test_utils.hpp"
#include "backup_directory.hpp"

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
void FilteredSlots() {
    Fixture fixture;
    fixture.manifest.features.incompat |= kFeatureSlotFilters;
    Save(StoreManifestPath(fixture.Table()), SerializeStoreManifest(fixture.manifest));
    const FeedSlotFilters healthy{.area = FeedArea{{-1, -2}, {4, 5}}, .column_ids = std::vector<std::uint32_t>{1U}, .no_before = true};
    const FeedSlotFilters missing{.column_ids = std::vector<std::uint32_t>{UINT32_MAX}};
    WriteFeedSlotRecords(fixture.Table(), {fixture.manifest.store_id, 3U,
        {{"healthy", 1U, false, healthy}, {"schema_lost", 2U, true, missing, true}}});
    fixture.Remark();
    const auto source = LoadFile(fixture.Table() / kFeedSlotsFileName);
    const auto restored = fixture.root / "filtered";
    RestoreBackup(fixture.backup, restored);
    const auto manifest = *ReadStoreManifest(restored / "tables/default");
    const auto slots = *ReadFeedSlotRecords(restored / "tables/default", manifest.store_id);
    assert(slots.filter_format && slots.slots[0].filters == healthy && !slots.slots[0].lost);
    assert(slots.slots[1].filters == missing && slots.slots[1].lost && slots.slots[1].schema_filter_lost);
    assert(slots.slots[0].written == 3U && slots.slots[1].written == 3U);
    assert(LoadFile(fixture.Table() / kFeedSlotsFileName) == source);
    assert(Verify(restored).errors == 0U);
    fixture.manifest.features.incompat &= ~kFeatureSlotFilters;
    Save(StoreManifestPath(fixture.Table()), SerializeStoreManifest(fixture.manifest)); fixture.Remark();
    Throws([&] { RestoreBackup(fixture.backup, fixture.root / "unowned_filters"); });
    assert(!std::filesystem::exists(fixture.root / "unowned_filters"));
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
        assert(one.lineage == (TableLineage{fixture.manifest.store_id, 3U}) && two.lineage == one.lineage);
        std::uint64_t next = 0U;
        assert(TryParseVersionClockRecord(LoadFile(first / "tables/default/chunkdb.version"), &next) && next == 4U);
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
        CatalogConfig config; config.data_dir = first; config.slot_sync_interval = std::chrono::hours(1);
        auto catalog = std::make_shared<TableCatalog>(config); auto table = catalog->Find("default");
        { auto lease = table->Acquire(); const auto values = lease->store().GetBlock(0, 0);
          assert(values && values->size() == 2U && std::get<std::uint64_t>((*values)[1]) == 9U);
          assert(std::get<BitsValue>((*values)[0]).digits == "00010000"); }
        FeedOptions options; options.after = FeedPosition{fixture.manifest.store_id, 3U};
        auto watch = SlotWatch::Create(table, "consumer", options); watch->Activate(); watch->WorkStep();
        assert(!watch->Take(4096U)); // An exact parent fork needs no missing prefix.
        watch->Cancel(); watch->WorkStep(); watch.reset();
        assert(LoadFile(fixture.backup / kBackupMarkerName) == source_marker && LoadFile(ChunkDataPath(fixture.Table(), fixture.old_geometry, {0, 0})) == source_image);
    }
}
void LineageContinuation() {
    Fixture fixture;
    for (const bool restart : {false, true}) {
        const auto target = fixture.root / (restart ? "cold-restarted" : "cold-first");
        RestoreBackup(fixture.backup, target);
        const auto manifest = *ReadStoreManifest(target / "tables/default");
        // No durable slot starts a feed: this exercises the first ordinary AFTER.
        WriteFeedSlotRecords(target / "tables/default", {manifest.store_id, 3U, {}});
        CatalogConfig config; config.data_dir = target;
        if (restart) { TableCatalog unused(config); }
        auto catalog = std::make_shared<TableCatalog>(config); auto table = catalog->Find("default");
        FeedOptions options; options.after = FeedPosition{fixture.manifest.store_id, 3U};
        auto ordinary = table->SubscribeFeed(options);
        assert(ordinary->position() == (FeedPosition{manifest.store_id, 3U}));
        assert(!ordinary->Next(std::chrono::milliseconds(0)));
        { auto lease = table->Acquire(); lease->store().SetBlock(0, 0, {{"extra", std::uint64_t{10U}}}); }
        const auto change = ordinary->Next(std::chrono::seconds(5));
        assert(change && change->kind == FeedEntry::Kind::kChange && change->position.revision == 4U);
        assert(std::get<std::uint64_t>((*change->blocks[0].after)[1]) == 10U);
    }
    for (const unsigned scenario : {0U, 1U, 2U}) {
        const auto target = fixture.root / ("lineage-missing-" + std::to_string(scenario)); RestoreBackup(fixture.backup, target);
        CatalogConfig config; config.data_dir = target; auto catalog = std::make_shared<TableCatalog>(config);
        auto table = catalog->Find("default"); FeedOptions options;
        options.after = FeedPosition{scenario == 2U ? NewStoreId() : fixture.manifest.store_id, scenario == 0U ? 2U : 4U};
        auto watch = table->SubscribeFeed(options); const auto resync = watch->Next(std::chrono::seconds(5));
        assert(resync && resync->kind == FeedEntry::Kind::kResync);
        assert(*resync->protocol_frame == ">5\r\n$6\r\nresync\r\n$32\r\n" + StoreIdHex(table->store_id()) +
            "\r\n:3\r\n$32\r\n" + StoreIdHex(fixture.manifest.store_id) + "\r\n:3\r\n");
        assert(watch->ResyncFrame() == *resync->protocol_frame);
    }
    const auto first = fixture.root / "lineage-first"; RestoreBackup(fixture.backup, first);
    const auto next_backup = fixture.root / "lineage-next-backup";
    {
        CatalogConfig config; config.data_dir = first; TableCatalog catalog(config);
        (void)catalog.BackupTo(next_backup, {});
    }
    const auto second = fixture.root / "lineage-second"; RestoreBackup(next_backup, second);
    const auto parent = *ReadStoreManifest(first / "tables/default"), child = *ReadStoreManifest(second / "tables/default");
    assert(child.lineage && child.lineage->parent_epoch == parent.store_id && child.lineage->parent_epoch != fixture.manifest.store_id);
    assert(child.lineage->fork_revision == ReadBackupRecord(next_backup).tables[0].revision);
    const auto empty_source = fixture.root / "empty-source", empty_backup = fixture.root / "empty-backup";
    {
        CatalogConfig config; config.data_dir = empty_source; TableCatalog catalog(config);
        auto table = catalog.Create("empty", {1U, 1U, 2U, 2U, 8U}, config.default_options);
        {
            auto lease = table->Acquire(); lease->store().SetBlockBits(0, 0, "10000000");
            lease->store().UnsetBlock(0, 0); lease->store().CheckpointForTests(0, 0);
            lease->store().FlushSnapshotGenerationLingerForTests();
            assert(!std::filesystem::exists(ChunkDataPath(lease->store().data_dir(), lease->store().geometry(), {0, 0})));
        }
        (void)catalog.BackupTo(empty_backup, {});
    }
    const auto empty_cut = ReadBackupRecord(empty_backup).tables[0];
    const auto empty_restore = fixture.root / "empty-restore"; RestoreBackup(empty_backup, empty_restore);
    {
        CatalogConfig config; config.data_dir = empty_restore; auto catalog = std::make_shared<TableCatalog>(config);
        FeedOptions options; options.after = FeedPosition{empty_cut.epoch, empty_cut.revision};
        auto table = catalog->Find("empty"); auto watch = table->SubscribeFeed(options);
        assert(watch->position() == (FeedPosition{table->store_id(), empty_cut.revision}));
        assert(!watch->Next(std::chrono::milliseconds(0)));
        assert(!table->Acquire()->store().GetBlock(0, 0));
    }
    fixture.manifest.features.incompat &= ~kFeatureTableLineage;
    Save(StoreManifestPath(fixture.Table()), SerializeStoreManifest(fixture.manifest)); fixture.Remark();
    const auto refused = fixture.root / "lineage-disabled";
    Throws<FeatureAdmissionError>([&] { RestoreBackup(fixture.backup, refused); });
    assert(!std::filesystem::exists(refused));
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
    RefusesOpen(fixture.backup, true);
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
        test::FeedPhaseWatchdog::Phase("restore temporary cleanup: prepare refusal " + std::to_string(failure));
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
        test::FeedPhaseWatchdog::Phase("restore temporary cleanup: check private staging removal");
        assert(!std::filesystem::exists(target));
        for (const auto& entry : std::filesystem::directory_iterator(fixture.root))
            assert(entry.path().filename().string().rfind(".chunkdb.restore.", 0U) != 0U);
        test::FeedPhaseWatchdog::Phase("restore temporary cleanup: restore after refusal");
        RestoreBackup(fixture.backup, target);
        assert(Verify(target).errors == 0U);
    }
}
void RestoreCleanupRefusalPreservesFailure() {
    if (!kFailpointsCompiled) return;
    {
        test::ScopedTempDir missing("chunkdb-missing-restore-cleanup");
        assert(!RemoveBackupTemporaryTree(missing.path() / "missing"));
        assert(!RemoveBackupTemporaryTree(missing.path() / "missing-parent/missing"));
    }
    for (const auto* point : {"CHUNKDB_FAILPOINT_BACKUP_TEMPORARY_CLEANUP_INVALID_PATH_ONCE",
             "CHUNKDB_FAILPOINT_BACKUP_TEMPORARY_CLEANUP_OPEN_FAIL_ONCE"}) {
        Fixture fixture;
        const auto target = fixture.root / "restore";
        txn_test::ScopedEnv copy_failure("CHUNKDB_FAILPOINT_RESTORE_COPY_FAIL_ONCE", "1");
        txn_test::ScopedEnv cleanup_failure(point, "1");
        std::string message;
        try { RestoreBackup(fixture.backup, target); }
        catch (const std::exception& failure) { message = failure.what(); }
        assert(message.find("injected restore copy failure") != std::string::npos);
        assert(message.find("restore temporary cleanup failed") != std::string::npos);
        assert(!std::filesystem::exists(target));
        std::size_t remaining = 0U;
        for (const auto& entry : backup_detail::DirectoryEntries(fixture.root)) {
            if (!entry.path().filename().string().starts_with(".chunkdb.restore.")) continue;
            assert(std::filesystem::exists(entry.path() / kRestoreIncompleteName));
            ++remaining;
            assert(!RemoveBackupTemporaryTree(entry.path()));
        }
        assert(remaining == 1U);
        assert(Verify(fixture.backup).errors == 0U);
    }
}
void RestoreCleanupRefusesReplacedParent() {
#ifdef _WIN32
    Fixture fixture;
    const auto parent = fixture.root / "restore-parent", moved = fixture.root / "moved-parent";
    const auto outside = fixture.root / "outside";
    std::filesystem::create_directory(parent);
    std::filesystem::create_directory(outside);
    Save(outside / "sentinel", Bytes{'s'});
    std::unique_ptr<test::ScopedDirectoryAlias> alias;
    std::string message;
    {
        struct ResetObserver { ~ResetObserver() { SetDirectorySyncObserverForTests({}); } } reset;
        SetDirectorySyncObserverForTests([&](const auto& directory) {
            if (directory != parent || alias) return;
            std::filesystem::rename(parent, moved);
            alias = std::make_unique<test::ScopedDirectoryAlias>(outside, parent);
            throw std::runtime_error("injected restore parent replacement");
        });
        try { RestoreBackup(fixture.backup, parent / "restored"); }
        catch (const std::exception& failure) { message = failure.what(); }
    }
    assert(alias);
    assert(message.find("injected restore parent replacement") != std::string::npos);
    assert(message.find("restore temporary cleanup failed") != std::string::npos);
    assert(LoadFile(outside / "sentinel") == Bytes{'s'});
    for (const auto& entry : backup_detail::DirectoryEntries(outside)) assert(entry.path().filename() == "sentinel");
    assert(!alias->Remove());
    std::filesystem::rename(moved, parent);
    std::size_t remaining = 0U;
    for (const auto& entry : backup_detail::DirectoryEntries(parent)) {
        assert(entry.path().filename().string().starts_with(".chunkdb.restore."));
        assert(std::filesystem::exists(entry.path() / kRestoreIncompleteName));
        ++remaining;
        assert(!RemoveBackupTemporaryTree(entry.path()));
    }
    assert(remaining == 1U);
#endif
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
    Fixture fixture;
    const auto alias = fixture.root / "alias";
    test::ScopedDirectoryAlias backup_alias(fixture.backup, alias);
    assert(Verify(alias).errors == 0U);
    RestoreBackup(alias, fixture.root / "restored");
    const auto source_alias = fixture.root / "source-alias";
    test::ScopedDirectoryAlias source_link(fixture.source, source_alias);
    PrepareBackupTarget(source_alias, fixture.root / "second-backup");
    const auto parent_alias = fixture.root / "parent-alias";
    test::ScopedDirectoryAlias parent_link(fixture.root, parent_alias);
    RestoreBackup(alias, parent_alias / "another-restored");
    Fixture staging;
    std::filesystem::remove(staging.backup / kBackupMarkerName);
    const auto prior = Verify(staging.backup);
    Save(staging.backup / kBackupStagingName / "interrupted" / "link", {1U});
    std::ostringstream findings;
    const auto verified = VerifyDataDirectory(staging.backup, findings);
    assert(verified.errors == 0U && verified.warnings == prior.warnings + 1U);
    assert(findings.str().find("interrupted_backup") != std::string::npos);
}
void InventoryPathSpellings() {
    BackupRecord record;
    record.files.push_back({"chunkdb.users", 0U, 0U});
    const auto canonical = SerializeBackupRecord(record);
    assert(ParseBackupRecord(canonical).files[0].relative_path.generic_string() == "chunkdb.users");
    const auto rejected = [&](std::string_view label, const std::string& spelling) {
        // One-file/no-table marker: preserve the fixed header through file count,
        // then write the raw spelling without std::filesystem normalization.
        Bytes bytes(canonical.begin(), canonical.begin() + 24U);
        WriteLe32(bytes, static_cast<std::uint32_t>(spelling.size()));
        bytes.insert(bytes.end(), spelling.begin(), spelling.end());
        WriteLe64(bytes, 0U); WriteLe32(bytes, 0U); WriteLe32(bytes, Crc32(bytes));
        try { (void)ParseBackupRecord(bytes); }
        catch (const std::runtime_error& error) {
            assert(std::string_view(error.what()).find("invalid backup inventory path") != std::string_view::npos);
            return;
        }
        std::cerr << "inventory path case accepted: " << label << '\n';
        assert(false && "invalid inventory spelling must be refused before path access");
    };
    rejected("native separator", "tables\\default/table.manifest");
    rejected("drive-less root", "/chunkdb.users");
    rejected("drive relative", "C:chunkdb.users");
    rejected("drive absolute", "C:/chunkdb.users");
    rejected("UNC", "//server/share/chunkdb.users");
    rejected("repeated separator", "tables//default/table.manifest");
    rejected("dot component", "tables/./default/table.manifest");
    rejected("parent component", "tables/../chunkdb.users");
    rejected("NUL", std::string("chunkdb.users\0suffix", 20U));
    Throws([&] { auto rooted = record; rooted.files[0].relative_path = "/chunkdb.users"; (void)SerializeBackupRecord(rooted); });
    Throws([&] { auto drive = record; drive.files[0].relative_path = "C:chunkdb.users"; (void)SerializeBackupRecord(drive); });
#ifdef _WIN32
    assert(std::filesystem::path("/chunkdb.users").has_root_directory());
    assert(!std::filesystem::path("/chunkdb.users").is_absolute());
    auto generated = record; generated.files[0].relative_path = std::filesystem::path("tables") / "default" / "table.manifest";
    const auto parsed = ParseBackupRecord(SerializeBackupRecord(generated));
    assert(parsed.files[0].relative_path.generic_string() == "tables/default/table.manifest");
#endif
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
    const auto rejected = [&](std::string_view name) {
        try { (void)ResolveBackupTarget(root, std::string(name)); }
        catch (const std::invalid_argument&) { return; }
        std::cerr << "rooted target case accepted: " << name << '\n';
        assert(false && "invalid backup target spelling must be refused");
    };
    for (const auto* name : {"..", "daily/../escaped", ".", "back\\slash", "back\\slash/nested", "back\\..\\escaped"})
        rejected(name);
    rejected(std::string("bad\0suffix", 10U));
#ifdef _WIN32
    for (const auto* name : {"C:daily", "C:/daily", "/daily", "\\daily", "\\\\server\\share\\daily"}) rejected(name);
#endif
    std::filesystem::create_directory(root / "inside");
    test::ScopedDirectoryAlias inside_link(root / "inside", root / "linked-inside");
    test::ScopedDirectoryAlias outside_link(fixture.source, root / "linked-outside");
    for (const auto* name : {"linked-inside", "linked-inside/child", "linked-outside/child"})
        Throws<std::invalid_argument>([&] { (void)ResolveBackupTarget(root, name); });
    const auto alias = fixture.root / "destinations-alias";
    test::ScopedDirectoryAlias root_link(root, alias);
    assert(ResolveBackupTarget(alias, "daily") == root / "daily");
}
void ReparseInventoryAndRestore() {
    Fixture fixture;
    const auto original = fixture.root / "original-table";
    const auto marker = LoadFile(fixture.backup / kBackupMarkerName);
    std::filesystem::rename(fixture.Table(), original);
    test::ScopedDirectoryAlias inventory_link(original, fixture.Table());
    // A canonicalized root alias is permitted, but an inventory descendant
    // must never redirect verification or restore outside the backup.
    assert(Verify(fixture.backup).errors > 0U);
    const auto target = fixture.root / "refused-restore";
    Throws([&] { RestoreBackup(fixture.backup, target); });
    assert(!std::filesystem::exists(target));
    assert(LoadFile(fixture.backup / kBackupMarkerName) == marker);
    assert(ReadStoreManifest(original)->store_id == fixture.manifest.store_id);
    assert(!inventory_link.Remove());
    std::filesystem::rename(original, fixture.Table());
    assert(Verify(fixture.backup).errors == 0U);
}
void ExtendedRootPaths() {
#ifdef _WIN32
    Fixture fixture;
    const auto extended = std::filesystem::path(L"\\\\?\\" + fixture.root.wstring());
    const auto top = extended / "extended-root";
    auto root = top;
    for (unsigned i = 0U; i < 3U; ++i) root /= std::string(100U, 'x') + std::to_string(i);
    std::filesystem::create_directories(root);
    assert(root.native().size() > 260U);
    assert(CanonicalBackupPath(root).native().starts_with(L"\\\\?\\"));
    const auto target = ResolveBackupTarget(root, "daily");
    test::FeedPhaseWatchdog::Phase("extended roots: prepare target " + target.string());
    PrepareBackupTarget(fixture.source, target);
    assert(std::filesystem::exists(target / kBackupIncompleteName));
    const auto restored = root / "restored";
    test::FeedPhaseWatchdog::Phase("extended roots: validate restore target");
    RequireBackupTarget(fixture.backup, restored);
    test::FeedPhaseWatchdog::Phase("extended roots: resolve restore target");
    assert(CanonicalBackupPath(restored).native().starts_with(L"\\\\?\\"));
    test::FeedPhaseWatchdog::Phase("extended roots: enumerate restore parent");
    std::size_t children = 0U;
    for (const auto& entry : backup_detail::DirectoryEntries(root)) {
        assert(entry.path().filename() == "daily");
        ++children;
    }
    assert(children == 1U);
    struct SyncTrace {
        SyncTrace() {
            SetDirectorySyncObserverForTests([](const auto& directory) {
                test::FeedPhaseWatchdog::Phase("extended roots: sync directory " + directory.string());
            });
            SetFileSyncObserverForTests([](const auto& file) {
                test::FeedPhaseWatchdog::Phase("extended roots: sync file " + file.string());
            });
        }
        ~SyncTrace() {
            SetFileSyncObserverForTests({});
            SetDirectorySyncObserverForTests({});
        }
    } sync_trace;
    test::FeedPhaseWatchdog::Phase("extended roots: restore " + restored.string());
    RestoreBackup(fixture.backup, restored);
    test::FeedPhaseWatchdog::Phase("extended roots: verify");
    assert(Verify(restored).errors == 0U);
    auto overlap = std::filesystem::path(L"\\\\?\\" + fixture.source.wstring());
    overlap /= std::string(200U, 'y');
    Throws<std::invalid_argument>([&] { RequireBackupTarget(fixture.source, overlap); });
    test::FeedPhaseWatchdog::Phase("extended roots: cleanup");
    RemoveBackupStaging(top);
    assert(!std::filesystem::exists(top));
#endif
}
} // namespace
int main(int argc, char** argv) {
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    _set_error_mode(_OUT_TO_STDERR);
#endif
    if (argc == 2) {
        const std::string_view name(argv[1]);
        test::FeedPhaseWatchdog watchdog("backup restore " + std::string(name));
        if (name == "filters") FilteredSlots();
        else if (name == "identity") EpochAndHistory();
        else if (name == "lineage") LineageContinuation();
        else if (name == "cleanup") RestoreTemporaryCleanup();
        else if (name == "cleanup-refusal") { RestoreCleanupRefusalPreservesFailure(); RestoreCleanupRefusesReplacedParent(); }
        else if (name == "cold") CrashConsistentWal();
        else if (name == "symlinks") SymlinkedRootsAndStaging();
        else if (name == "roots") { RootedTargets(); InventoryPathSpellings(); }
        else if (name == "reparse") ReparseInventoryAndRestore();
        else if (name == "extended") ExtendedRootPaths();
        else return 2;
        return 0;
    }
    const auto run = [](std::string_view name, const auto& group) {
        test::FeedPhaseWatchdog watchdog("backup restore " + std::string(name));
        group();
    };
    run("filtered slots", FilteredSlots); run("records", Records); run("identity", EpochAndHistory);
    run("lineage", LineageContinuation); run("inventory", StrictInventory); run("targets and copies", TargetsAndCopies);
    run("cleanup refusal", RestoreCleanupRefusalPreservesFailure); run("cleanup parent replacement", RestoreCleanupRefusesReplacedParent);
    run("completion failures", CompletionFailures); run("temporary cleanup", RestoreTemporaryCleanup);
    run("cold WAL", CrashConsistentWal); run("root aliases", SymlinkedRootsAndStaging);
    run("rooted targets", RootedTargets); run("inventory paths", InventoryPathSpellings);
    run("inventory reparse", ReparseInventoryAndRestore); run("extended roots", ExtendedRootPaths);
    std::cout << "14 backup restore groups passed\n";
}
