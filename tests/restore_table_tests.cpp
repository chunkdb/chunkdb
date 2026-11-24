// RESTORE TABLE into a running catalog, SWAP TABLES and ALTER TABLE ... RENAME
// TO (docs/design/RESTORE_TABLE.md).
#include <stop_token>

#include "feed_slots.hpp"
#include "restore_table_test_utils.hpp"
#include "statement_control.hpp"
#include "txn_test_utils.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::restore_table_test;

void RestoresIntoNewName() {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        Source source(mode);
        StoreId restored{};
        {
            TableCatalog catalog(source.Config());
            std::uint64_t reported_files = 0U, reported_bytes = 0U, total_files = 0U, total_bytes = 0U, calls = 0U;
            auto request = source.Request();
            request.progress = [&](std::uint64_t files, std::uint64_t bytes, std::uint64_t all_files, std::uint64_t all_bytes) {
                // The totals come first, before any file is copied.
                if (calls++ == 0U) assert(files == 0U && bytes == 0U && all_files > 0U && all_bytes > 0U);
                else assert(files > reported_files && bytes >= reported_bytes);
                assert(files <= all_files && bytes <= all_bytes);
                // Not visible until the restore publishes it.
                assert(catalog.Find("t_restored") == nullptr && !std::filesystem::exists(source.data / "tables/t_restored"));
                reported_files = files; reported_bytes = bytes; total_files = all_files; total_bytes = all_bytes;
            };
            const auto result = catalog.RestoreTable(request);
            assert(result.name == "t_restored" && result.parent_epoch == source.epoch && result.fork_revision == source.cut);
            assert(result.files == total_files && reported_files == total_files && calls == total_files + 1U);
            assert(result.bytes == total_bytes && reported_bytes == total_bytes && total_bytes > 0U);
            restored = result.epoch;
            source.CheckRestored(catalog, "t_restored", restored);
            const auto names = catalog.List();
            assert(names.size() == 2U && names[0].name == "t" && names[1].name == "t_restored");
            // The restored table serves writes after its cut.
            auto lease = catalog.Require("t_restored")->Acquire();
            lease->store().SetBlockBits(0, 1, "01000000");
            assert(lease->store().GetChunkVersion(0, 0) > source.cut);
            assert(Entries(source.data / ".chunkdb.staging") == 0U);
        }
        Verified(source.data);
        Verified(source.backup);
        TableCatalog reopened(source.Config());
        assert(reopened.Require("t_restored")->store_id() == restored);
        assert(reopened.Require("t_restored")->Acquire()->store().GetBlockBits(0, 1) == "01000000");
        // A second restore of the same cut gets another identity.
        const auto again = reopened.RestoreTable(source.Request("t_again"));
        assert(again.epoch != restored && again.epoch != source.epoch);
        source.CheckRestored(reopened, "t_again", again.epoch);
    }
}

void KeepsTheBackedUpName() {
    Source source;
    TableCatalog catalog(source.Config());
    // Read from the dropped table after the backup cut.
    const auto stale = catalog.Require("t")->Acquire()->store().GetChunkVersion(4, 0);
    assert(stale > source.cut);
    catalog.Drop("t");
    auto request = source.Request("t");
    const auto result = catalog.RestoreTable(request);
    const auto table = catalog.Require("t");
    assert(table->store_id() == result.epoch && result.epoch != source.epoch);
    assert(table->Acquire()->store().GetBlockBits(8, 0) == "00000011");
    // The restored clock starts above the dropped table's, not at the cut's successor.
    RefusesStaleVersion(catalog, "t", stale);
}

// After RESTORE ... AS t_restored and SWAP TABLES, `t` reaches the restored
// table, whose clock the restore started at the cut's successor; a version a
// client read from the live `t` after the cut never matches it.
void SwapRefusesStaleVersions() {
    Source source;
    std::uint64_t stale = 0U;
    {
        TableCatalog catalog(source.Config());
        (void)catalog.RestoreTable(source.Request());
        stale = catalog.Require("t")->Acquire()->store().GetChunkVersion(4, 0);
        assert(stale > source.cut);
        catalog.SwapTables("t", "t_restored");
        RefusesStaleVersion(catalog, "t", stale);
    }
    // The floor that keeps them apart is durable.
    TableCatalog catalog(source.Config());
    RefusesStaleVersion(catalog, "t", stale);
}

// RENAME TO onto the name of a dropped table starts the renamed table's clock
// above the dropped table's, and a table created later under the old name
// starts above the renamed table's.
void RenameRefusesStaleVersions() {
    Source source;
    TableCatalog catalog(source.Config());
    (void)catalog.Create("u", SourceGeometry(), catalog.default_options());
    std::uint64_t old_name_stale = 0U;
    {
        auto lease = catalog.Require("u")->Acquire();
        lease->store().SetBlockBits(8, 0, "00001111");
        old_name_stale = lease->store().GetChunkVersion(4, 0);
    }
    // A version of `t` at or above every version `u` has reserved.
    std::uint64_t stale = 0U;
    {
        auto lease = catalog.Require("t")->Acquire();
        lease->store().SetBlockBits(8, 0, "00011111");
        stale = lease->store().GetChunkVersion(4, 0);
    }
    assert(stale > old_name_stale);
    catalog.Drop("t");
    catalog.RenameTable("u", "t", nullptr);
    RefusesStaleVersion(catalog, "t", stale);
    (void)catalog.Create("u", SourceGeometry(), catalog.default_options());
    RefusesStaleVersion(catalog, "u", old_name_stale);
}

// Writes block (8, 0) of `name` until chunk (4, 0) has `version`; the clock
// gives one version per write.
void WriteToVersion(TableCatalog& catalog, std::string_view name, std::uint64_t version) {
    auto lease = catalog.Require(name)->Acquire();
    auto& store = lease->store();
    while (store.GetChunkVersion(4, 0) < version) store.SetBlockBits(8, 0, "01010101");
    assert(store.GetChunkVersion(4, 0) == version);
}

// Creates `u`, an unrelated table whose chunk (4, 0) has the version that a
// client read from chunk (4, 0) of `t`, with other data; returns that version.
std::uint64_t SameVersionOtherData(TableCatalog& catalog) {
    const auto version = catalog.Require("t")->Acquire()->store().GetChunkVersion(4, 0);
    (void)catalog.Create("u", SourceGeometry(), catalog.default_options());
    WriteToVersion(catalog, "u", version);
    assert(catalog.Require("u")->Acquire()->store().GetBlockBits(8, 0) !=
           catalog.Require("t")->Acquire()->store().GetBlockBits(8, 0));
    return version;
}

// A write conditioned on `stale`, a version a client read under `name` from
// the table that held the name before, is refused by the table that holds it
// now, also where one of its existing chunks carries the same number.
void RefusesExistingStaleVersion(TableCatalog& catalog, std::string_view name, std::uint64_t stale) {
    auto lease = catalog.Require(name)->Acquire();
    auto& store = lease->store();
    const auto bits = store.GetBlockBits(8, 0);
    assert(!store.UnsetBlock(8, 0, stale).ok);
    assert(store.GetBlockBits(8, 0) == bits);
}

// The backup of `u` at `source.root / "other"`, restored under `name`.
TableRestoreRequest OtherRequest(const Source& source, std::string name) {
    return TableRestoreRequest{.backup = source.root / "other", .table = "u", .name = std::move(name)};
}

// RESTORE ... AS u_restored and SWAP TABLES t, u_restored: `t` then reaches a
// table whose chunk (4, 0) has the version a client read from `t`.
void SwapRefusesExistingStaleVersion() {
    Source source;
    TableCatalog catalog(source.Config());
    const auto stale = SameVersionOtherData(catalog);
    (void)catalog.BackupTo(source.root / "other", {});
    (void)catalog.RestoreTable(OtherRequest(source, "u_restored"));
    catalog.SwapTables("t", "u_restored");
    RefusesExistingStaleVersion(catalog, "t", stale);
}

// DROP TABLE t and RESTORE ... AS t from a backup of another table.
void RestoreAfterDropRefusesExistingStaleVersion() {
    Source source;
    TableCatalog catalog(source.Config());
    const auto stale = SameVersionOtherData(catalog);
    (void)catalog.BackupTo(source.root / "other", {});
    catalog.Drop("t");
    (void)catalog.RestoreTable(OtherRequest(source, "t"));
    RefusesExistingStaleVersion(catalog, "t", stale);
}

// DROP TABLE t and ALTER TABLE u RENAME TO t.
void RenameOntoDroppedRefusesExistingStaleVersion() {
    Source source;
    TableCatalog catalog(source.Config());
    const auto stale = SameVersionOtherData(catalog);
    catalog.Drop("t");
    catalog.RenameTable("u", "t", nullptr);
    RefusesExistingStaleVersion(catalog, "t", stale);
}

void LeavesNothingAfterRefusal(const Source& source, TableCatalog& catalog) {
    assert(Entries(source.data / ".chunkdb.staging") == 0U);
    assert(Entries(source.data / "tables") == 1U && catalog.TableCount() == 1U);
    assert(catalog.Find("t_restored") == nullptr && catalog.Fences().empty());
}

void Refusals() {
    Source source;
    {
        TableCatalog catalog(source.Config());
        Throws<TableExistsError>([&] { (void)catalog.RestoreTable(source.Request("t")); });
        Throws<std::invalid_argument>([&] { (void)catalog.RestoreTable(source.Request("Bad.Name")); });
        auto missing = source.Request();
        missing.table = "absent";
        assert(Throws<std::invalid_argument>([&] { (void)catalog.RestoreTable(missing); }).find("no table 'absent'") != std::string::npos);
        LeavesNothingAfterRefusal(source, catalog);
    }
    {
        // A stored option that differs from an explicit server flag would
        // refuse the next start, so it is refused before publication.
        auto config = source.Config();
        config.default_options.durability_mode = DurabilityMode::kFsyncWal;
        config.default_option_fields = kOptionFieldDurabilityMode;
        TableOptions fsync = config.default_options;
        {
            TableCatalog catalog(source.Config());
            catalog.SetOptions("t", fsync);
        }
        TableCatalog catalog(config);
        assert(Throws<std::invalid_argument>([&] { (void)catalog.RestoreTable(source.Request()); })
                   .find("server option flags differ") != std::string::npos);
        LeavesNothingAfterRefusal(source, catalog);
    }
    {
        auto config = source.Config();
        config.access_mode = AccessMode::kReadOnly;
        TableCatalog catalog(config);
        Throws<std::invalid_argument>([&] { (void)catalog.RestoreTable(source.Request()); });
        assert(catalog.Find("t_restored") == nullptr);
        assert(Throws<std::invalid_argument>([&] { catalog.SwapTables("t", "default"); }).find("read-only") != std::string::npos);
    }
}

// Copy a backup into `copy` and damage it with `damage`; the restore is
// refused and leaves nothing behind.
template <typename Damage>
void RefusesDamagedBackup(const Damage& damage, std::string_view expected) {
    Source source;
    const auto copy = source.root / "damaged";
    std::filesystem::copy(source.backup, copy, std::filesystem::copy_options::recursive);
    damage(copy);
    TableCatalog catalog(source.Config());
    auto request = source.Request();
    request.backup = copy;
    const auto error = Throws([&] { (void)catalog.RestoreTable(request); });
    if (error.find(expected) == std::string::npos) std::cerr << error << '\n';
    assert(error.find(expected) != std::string::npos);
    LeavesNothingAfterRefusal(source, catalog);
}

std::filesystem::path FirstChunkFile(const std::filesystem::path& table, std::string_view extension) {
    for (const auto& entry : std::filesystem::recursive_directory_iterator(table))
        if (entry.path().extension() == extension) return entry.path();
    assert(false && "no chunk file");
    return {};
}

void DamagedBackups() {
    RefusesDamagedBackup([](const auto& copy) {
        const auto image = FirstChunkFile(copy / "tables/t", ".chk");
        auto bytes = LoadFile(image);
        bytes.back() ^= 0x5AU;
        AtomicWrite(image, bytes, true, true);
    }, "checksum");
    RefusesDamagedBackup([](const auto& copy) {
        const auto wal = FirstChunkFile(copy / "tables/t", ".wal");
        auto bytes = LoadFile(wal);
        bytes.push_back(0U);
        AtomicWrite(wal, bytes, true, true);
    }, "length mismatch");
    RefusesDamagedBackup([](const auto& copy) {
        AtomicWrite(copy / "tables/t/extra.bin", std::vector<std::uint8_t>{1U}, true, true);
    }, "unexpected backup file");
    RefusesDamagedBackup([](const auto& copy) {
        AtomicWrite(copy / kBackupIncompleteName, std::vector<std::uint8_t>{'C', 'K', 'B', 'I'}, true, true);
    }, "incomplete");
#ifndef _WIN32
    RefusesDamagedBackup([](const auto& copy) {
        const auto wal = FirstChunkFile(copy / "tables/t", ".wal");
        const auto moved = copy / "elsewhere.wal";
        std::filesystem::rename(wal, moved);
        std::filesystem::create_symlink(moved, wal);
    }, "unsafe backup entry");
#endif
}

void InjectedFailures() {
    Source source;
    {
        TableCatalog catalog(source.Config());
        {
            txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_RESTORE_TABLE_PUBLISH_FAIL_ONCE", "1");
            Throws([&] { (void)catalog.RestoreTable(source.Request()); });
        }
        LeavesNothingAfterRefusal(source, catalog);
        {
            // After the rename, a table that cannot open is moved out again.
            txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_RESTORE_TABLE_OPEN_FAIL_ONCE", "1");
            Throws([&] { (void)catalog.RestoreTable(source.Request()); });
        }
        LeavesNothingAfterRefusal(source, catalog);
        assert(Entries(source.data / ".chunkdb.dropped") == 0U);
        {
            // When it cannot be moved out either, its name is fenced.
            txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_RESTORE_TABLE_OPEN_FAIL_ONCE", "1");
            txn_test::ScopedEnv undo("CHUNKDB_FAILPOINT_RESTORE_TABLE_UNDO_FAIL_ONCE", "1");
            Throws([&] { (void)catalog.RestoreTable(source.Request()); });
        }
        assert(catalog.Find("t_restored") == nullptr && catalog.Fence("t_restored").has_value());
        Throws<TableFencedError>([&] { (void)catalog.RestoreTable(source.Request()); });
        catalog.RecoverTable("t_restored");
        const auto table = catalog.Require("t_restored");
        source.CheckRestored(catalog, "t_restored", table->store_id());
    }
    Verified(source.data);
}

void Cancellation() {
    Source source;
    TableCatalog catalog(source.Config());
    std::stop_source stop;
    auto request = source.Request();
    request.cancelled = stop.get_token();
    // A stop after the first file leaves copied files in staging, which the refusal removes.
    request.progress = [&](std::uint64_t files, std::uint64_t, std::uint64_t all_files, std::uint64_t) {
        if (files == 0U) return;
        assert(files == 1U && all_files > 1U && Entries(source.data / ".chunkdb.staging") > 0U);
        stop.request_stop();
    };
    assert(Throws([&] { (void)catalog.RestoreTable(request); }).find("cancelled") != std::string::npos);
    LeavesNothingAfterRefusal(source, catalog);
    // A stop at the totals, before any file is copied.
    std::stop_source first;
    request.cancelled = first.get_token();
    request.progress = [&](std::uint64_t, std::uint64_t, std::uint64_t, std::uint64_t) { first.request_stop(); };
    assert(Throws([&] { (void)catalog.RestoreTable(request); }).find("cancelled") != std::string::npos);
    LeavesNothingAfterRefusal(source, catalog);
    std::stop_source before;
    before.request_stop();
    request.cancelled = before.get_token();
    request.progress = {};
    assert(Throws([&] { (void)catalog.RestoreTable(request); }).find("cancelled") != std::string::npos);
    LeavesNothingAfterRefusal(source, catalog);
}


// A restored table and the live one, with their epochs.
struct Swapped {
    Source source;
    TableCatalog catalog{source.Config()};
    StoreId restored = catalog.RestoreTable(source.Request()).epoch;

    // Each name reaches the other table's data, epoch and slots.
    void Check(bool exchanged) {
        const auto t = catalog.Require("t"), other = catalog.Require("t_restored");
        const auto& live = exchanged ? other : t;
        const auto& copy = exchanged ? t : other;
        assert(live->store_id() == source.epoch && copy->store_id() == restored);
        assert(copy->lineage() && *copy->lineage() == (TableLineage{source.epoch, source.cut}));
        assert(live->Acquire()->store().GetBlockBits(8, 0) == "11111111");
        assert(copy->Acquire()->store().GetBlockBits(8, 0) == "00000011");
        const auto slots = copy->ListFeedSlots();
        assert(slots.size() == 1U && slots[0].position == (FeedPosition{restored, source.cut}));
        assert(live->ListFeedSlots().at(0).position.epoch == source.epoch);
    }
};

void SwapsNames() {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        Swapped swapped{Source(mode)};
        const auto old_t = swapped.catalog.Require("t");
        auto watch = old_t->SubscribeFeed();
        swapped.catalog.SwapTables("t", "t_restored");
        swapped.Check(true);
        // The old table objects are gone; their watches end.
        assert(!old_t->Acquire().has_value());
        const auto ended = watch->Next(std::chrono::seconds(5));
        assert(ended && ended->kind == FeedEntry::Kind::kEnd && ended->protocol_frame &&
               ended->protocol_frame->find("swapped") != std::string::npos);
        watch.reset();
        // Writes under the new names are acknowledged and stay there.
        swapped.catalog.Require("t")->Acquire()->store().SetBlockBits(0, 1, "00010000");
        swapped.catalog.Require("t_restored")->Acquire()->store().SetBlockBits(0, 1, "00100000");
        // Either order names the same exchange; swapping again restores both.
        swapped.catalog.SwapTables("t_restored", "t");
        swapped.Check(false);
        swapped.catalog.SwapTables("t", "t_restored");
        assert(Entries(swapped.source.data / "tables") == 2U);
    }
}

void SwapSurvivesRestart() {
    Source source;
    StoreId restored{};
    {
        TableCatalog catalog(source.Config());
        restored = catalog.RestoreTable(source.Request()).epoch;
        catalog.SwapTables("t", "t_restored");
        catalog.Require("t")->Acquire()->store().SetBlockBits(0, 1, "00010000");
    }
    Verified(source.data);
    TableCatalog catalog(source.Config());
    assert(catalog.Require("t")->store_id() == restored && catalog.Require("t_restored")->store_id() == source.epoch);
    assert(catalog.Require("t")->Acquire()->store().GetBlockBits(0, 1) == "00010000");
}

// A consumer of `t` resumes by name: the exact fork position continues on
// the restored table, a later position of the old epoch resyncs.
void FeedAfterSwap() {
    Swapped swapped{Source()};
    swapped.catalog.SwapTables("t", "t_restored");
    const auto t = swapped.catalog.Require("t");
    FeedOptions exact;
    exact.after = FeedPosition{swapped.source.epoch, swapped.source.cut};
    auto continuing = t->SubscribeFeed(exact);
    FeedOptions later;
    later.after = FeedPosition{swapped.source.epoch, swapped.source.cut + 1U};
    auto resyncing = t->SubscribeFeed(later);
    const auto resync = resyncing->Next(std::chrono::seconds(5));
    assert(resync && resync->kind == FeedEntry::Kind::kResync);
    assert(resync->protocol_frame && resync->protocol_frame->find(StoreIdHex(swapped.source.epoch)) != std::string::npos);
    std::uint64_t written = 0U;
    {
        auto lease = t->Acquire();
        lease->store().SetBlockBits(0, 1, "00010000");
        written = lease->store().GetChunkVersion(0, 0);
    }
    const auto change = continuing->Next(std::chrono::seconds(5));
    assert(change && change->kind == FeedEntry::Kind::kChange);
    assert(change->position == (FeedPosition{swapped.restored, written}));
}

void SwapRefusals() {
    Swapped swapped{Source()};
    auto& catalog = swapped.catalog;
    Throws<std::invalid_argument>([&] { catalog.SwapTables("t", "t"); });
    Throws<TableNotFoundError>([&] { catalog.SwapTables("t", "absent"); });
    Throws<std::invalid_argument>([&] { catalog.SwapTables("t", "Bad.Name"); });
    {
        // Without a version floor above both clocks nothing moves and both
        // tables serve.
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_VERSION_FLOOR_WRITE_FAIL_ONCE", "1");
        assert(Throws([&] { catalog.SwapTables("t", "t_restored"); }).find("version floor") != std::string::npos);
    }
    assert(catalog.Fences().empty());
    swapped.Check(false);
    {
        // Without an atomic exchange nothing moves and both tables serve.
        txn_test::ScopedEnv refuse("CHUNKDB_FAILPOINT_SWAP_TABLES_EXCHANGE_UNSUPPORTED_ONCE", "1");
        assert(Throws([&] { catalog.SwapTables("t", "t_restored"); }).find("atomic directory exchange") != std::string::npos);
    }
    assert(catalog.Fences().empty());
    swapped.Check(false);
    {
        // An exchange that cannot be synced leaves both names fenced; a
        // recovery opens what each directory holds.
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_SWAP_TABLES_SYNC_FAIL_ONCE", "1");
        Throws([&] { catalog.SwapTables("t", "t_restored"); });
    }
    assert(catalog.Fence("t") && catalog.Fence("t_restored") && catalog.TableCount() == 0U);
    catalog.RecoverTable("t");
    catalog.RecoverTable("t_restored");
    swapped.Check(true);
    {
        // A name whose table cannot open is fenced; the other serves.
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_SWAP_TABLES_OPEN_FAIL_ONCE", "1");
        Throws([&] { catalog.SwapTables("t", "t_restored"); });
    }
    assert(catalog.Fence("t") && catalog.Find("t_restored") != nullptr);
    catalog.RecoverTable("t");
    swapped.Check(false);
    Verified(swapped.source.data);
}

#ifdef _WIN32
constexpr bool kAtomicExchange = false;
#else
constexpr bool kAtomicExchange = true;
#endif

// Whether the one slot of `table` starts its next watch without AFTER with resync.
bool Resyncs(TableCatalog& catalog, std::string_view table) {
    const auto slots = catalog.Require(table)->ListFeedSlots();
    assert(slots.size() == 1U);
    return slots[0].resync;
}

// A restored slot and both tables' slots after a swap start their next watch
// without AFTER with resync; a refused swap leaves the slots as they were.
void SwapMarksSlots() {
    Swapped swapped{Source()};
    auto& catalog = swapped.catalog;
    assert(!Resyncs(catalog, "t") && Resyncs(catalog, "t_restored"));
    for (const auto* point : {"CHUNKDB_FAILPOINT_SWAP_TABLES_EXCHANGE_UNSUPPORTED_ONCE", "CHUNKDB_FAILPOINT_SWAP_TABLES_SLOT_MARK_FAIL_ONCE"}) {
        txn_test::ScopedEnv refuse(point, "1");
        Throws([&] { catalog.SwapTables("t", "t_restored"); });
        assert(catalog.Fences().empty());
        swapped.Check(false);
        assert(!Resyncs(catalog, "t") && Resyncs(catalog, "t_restored"));
    }
    catalog.SwapTables("t", "t_restored");
    swapped.Check(true);
    assert(Resyncs(catalog, "t") && Resyncs(catalog, "t_restored"));
}

// The marks are durable: a restart keeps them.
void SwapMarksSurviveRestart() {
    Source source;
    {
        TableCatalog catalog(source.Config());
        (void)catalog.RestoreTable(source.Request());
        catalog.SwapTables("t", "t_restored");
    }
    TableCatalog catalog(source.Config());
    assert(Resyncs(catalog, "t") && Resyncs(catalog, "t_restored"));
}

// A marked slot that outgrows its retention limit becomes lost and loses its
// mark; its records stay valid across a restart.
void MarkedSlotBecomesLost() {
    Source source;
    {
        TableCatalog catalog(source.Config());
        (void)catalog.RestoreTable(source.Request());
        auto update = TableOptionsUpdate::From(catalog.Require("t_restored")->Info().options);
        update.slot_max_bytes = 1U;
        catalog.SetOptions("t_restored", update);
        const auto table = catalog.Require("t_restored");
        {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(0, 1, "00010000");
            lease->store().WalBarrier();
            lease->store().CheckpointForTests(0, 0);
        }
        FeedSlotTestAccess::Sync(*table);
        FeedSlotTestAccess::Retain(*table);
        const auto slots = table->ListFeedSlots(true);
        assert(slots.size() == 1U && slots[0].lost && !slots[0].resync);
        table->Acquire()->store().SetBlockBits(0, 1, "00100000");
    }
    TableCatalog catalog(source.Config());
    const auto slots = catalog.Require("t_restored")->ListFeedSlots(true);
    assert(slots.size() == 1U && slots[0].lost && !slots[0].resync);
    Verified(source.data);
}

// Windows has no atomic directory exchange: SWAP TABLES is refused before
// anything closes, and both tables keep serving.
void SwapRefusedOnWindows() {
    Swapped swapped{Source()};
    assert(Throws([&] { swapped.catalog.SwapTables("t", "t_restored"); }).find("Windows") != std::string::npos);
    assert(swapped.catalog.Fences().empty());
    swapped.Check(false);
    assert(!Resyncs(swapped.catalog, "t"));
}

// CANCEL, KILL or a statement timeout accepted before the decision leaves
// nothing restored or swapped; after it, a stop is refused and the statement
// reports its real outcome.
void StatementStops() {
    const auto running = [](StatementControl& control) { assert(control.Start()); };
    {
        Source source;
        TableCatalog catalog(source.Config());
        StatementControl control(StatementControl::Clock::now(), 0U);
        running(control);
        ScopedStatementControl context(&control);
        assert(control.RequestStop(StatementStopReason::kCancelled));
        (void)Throws<StatementStoppedError>([&] { (void)catalog.RestoreTable(source.Request()); });
        LeavesNothingAfterRefusal(source, catalog);
    }
    {
        Source source;
        TableCatalog catalog(source.Config());
        StatementControl control(StatementControl::Clock::now(), 0U);
        running(control);
        ScopedStatementControl context(&control);
        (void)catalog.RestoreTable(source.Request());
        assert(control.decision_entered() && !control.RequestStop(StatementStopReason::kCancelled));
        assert(catalog.Find("t_restored") != nullptr);
    }
    if (!kAtomicExchange) return;
    Swapped swapped{Source()};
    {
        StatementControl control(StatementControl::Clock::now(), 0U);
        running(control);
        ScopedStatementControl context(&control);
        assert(control.RequestStop(StatementStopReason::kKilled));
        (void)Throws<StatementStoppedError>([&] { swapped.catalog.SwapTables("t", "t_restored"); });
    }
    assert(swapped.catalog.Fences().empty());
    swapped.Check(false);
    {
        StatementControl control(StatementControl::Clock::now(), 0U);
        running(control);
        ScopedStatementControl context(&control);
        swapped.catalog.SwapTables("t", "t_restored");
        assert(control.decision_entered() && !control.RequestStop(StatementStopReason::kCancelled));
    }
    swapped.Check(true);
}

// The table `name` holds the live table of `source` with its epoch, data,
// tombstones and slot, and its slot does not resync.
void CheckLive(const Source& source, TableCatalog& catalog, std::string_view name) {
    const auto table = catalog.Require(name);
    assert(table->store_id() == source.epoch && !table->lineage());
    {
        auto lease = table->Acquire();
        auto& store = lease->store();
        assert(store.GetBlockBits(8, 0) == "11111111");
        assert(store.GetBlockBits(9, 3) == "00000101" && store.GetChunkVersion(4, 1) == source.wal_only);
        assert(store.RetainedTombstoneCount() == 2U);
    }
    const auto slots = table->ListFeedSlots();
    assert(slots.size() == 1U && slots[0].name == "consumer" && !slots[0].resync && slots[0].position.epoch == source.epoch);
}

// RENAME TO keeps the table's epoch, data and slots under the new name; the
// old name is free, its table object is gone and its watches end.
void RenamesTable() {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        Source source(mode);
        {
            TableCatalog catalog(source.Config());
            const auto old_t = catalog.Require("t");
            const auto position = old_t->ListFeedSlots().at(0).position;
            auto watch = old_t->SubscribeFeed();
            catalog.RenameTable("t", "t_old", nullptr);
            assert(catalog.Find("t") == nullptr && catalog.TableCount() == 1U && catalog.Fences().empty());
            CheckLive(source, catalog, "t_old");
            assert(catalog.Require("t_old")->ListFeedSlots().at(0).position == position);
            assert(!old_t->Acquire().has_value());
            const auto ended = watch->Next(std::chrono::seconds(5));
            assert(ended && ended->kind == FeedEntry::Kind::kEnd && ended->protocol_frame &&
                   ended->protocol_frame->find("renamed") != std::string::npos);
            watch.reset();
            // A watch of the new name continues from the last revision in the same epoch.
            FeedOptions after;
            after.after = FeedPosition{source.epoch, *catalog.Require("t_old")->Acquire()->store().CompletedRevision()};
            auto continuing = catalog.Require("t_old")->SubscribeFeed(after);
            std::uint64_t written = 0U;
            {
                auto lease = catalog.Require("t_old")->Acquire();
                lease->store().SetBlockBits(1, 3, "00010000");
                written = lease->store().GetChunkVersion(0, 1);
            }
            bool seen = false;
            for (int i = 0; i < 64 && !seen; ++i) {
                const auto entry = continuing->Next(std::chrono::seconds(5));
                assert(entry && entry->kind != FeedEntry::Kind::kResync && entry->kind != FeedEntry::Kind::kEnd);
                seen = entry->kind == FeedEntry::Kind::kChange && entry->position == (FeedPosition{source.epoch, written});
            }
            assert(seen);
            continuing.reset();
            // A new table can take the old name.
            GeometryConfig geometry;
            geometry.block_bits = 8U;
            (void)catalog.Create("t", geometry, catalog.default_options());
        }
        Verified(source.data);
        TableCatalog catalog(source.Config());
        CheckLive(source, catalog, "t_old");
        assert(catalog.Require("t_old")->Acquire()->store().GetBlockBits(1, 3) == "00010000");
        assert(catalog.Require("t")->store_id() != source.epoch);
        catalog.Drop("t");
        catalog.RenameTable("t_old", "t", nullptr);
        CheckLive(source, catalog, "t");
        assert(Entries(source.data / "tables") == 1U);
    }
}

void RenameRefusals() {
    Source source;
    TableCatalog catalog(source.Config());
    Throws<std::invalid_argument>([&] { catalog.RenameTable("t", "t", nullptr); });
    Throws<TableNotFoundError>([&] { catalog.RenameTable("absent", "other", nullptr); });
    Throws<std::invalid_argument>([&] { catalog.RenameTable("t", "Bad.Name", nullptr); });
    GeometryConfig geometry;
    geometry.block_bits = 8U;
    (void)catalog.Create("taken", geometry, catalog.default_options());
    Throws<TableExistsError>([&] { catalog.RenameTable("t", "taken", nullptr); });
    {
        // Without a version floor above its clock the table serves unchanged.
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_VERSION_FLOOR_WRITE_FAIL_ONCE", "1");
        assert(Throws([&] { catalog.RenameTable("t", "t_new", nullptr); }).find("version floor") != std::string::npos);
    }
    assert(catalog.Fences().empty() && catalog.Find("t_new") == nullptr);
    CheckLive(source, catalog, "t");
    // A directory no table of this server owns is not replaced.
    std::filesystem::create_directory(source.data / "tables" / "squatted");
    assert(Throws<TableExistsError>([&] { catalog.RenameTable("t", "squatted", nullptr); }).find("not a table this server opened") !=
           std::string::npos);
    std::filesystem::remove(source.data / "tables" / "squatted");
    {
        // A rename that fails serves the table again under its name.
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_RENAME_TABLE_MOVE_FAIL_ONCE", "1");
        Throws([&] { catalog.RenameTable("t", "t_new", nullptr); });
    }
    assert(catalog.Fences().empty() && catalog.Find("t_new") == nullptr);
    CheckLive(source, catalog, "t");
    {
        // A rename that cannot be synced fences both names, since a crash
        // could still undo it; a recovery opens what each directory holds.
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_RENAME_TABLE_SYNC_FAIL_ONCE", "1");
        Throws([&] { catalog.RenameTable("t", "t_new", nullptr); });
    }
    assert(catalog.Fence("t_new") && catalog.Fence("t") && catalog.Find("t") == nullptr && catalog.Find("t_new") == nullptr);
    Throws<TableFencedError>([&] { (void)catalog.Create("t", geometry, catalog.default_options()); });
    catalog.RecoverTable("t_new");
    CheckLive(source, catalog, "t_new");
    assert(catalog.Fence("t"));
    catalog.RecoverTable("t");
    assert(catalog.Fences().empty() && catalog.Find("t") == nullptr && catalog.TableCount() == 2U);
    {
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_RENAME_TABLE_OPEN_FAIL_ONCE", "1");
        Throws([&] { catalog.RenameTable("t_new", "t", nullptr); });
    }
    assert(catalog.Fence("t") && catalog.Find("t_new") == nullptr);
    catalog.RecoverTable("t");
    CheckLive(source, catalog, "t");
    Verified(source.data);
}

// A stop accepted before the decision leaves the name as it was; after it,
// the rename reports its real outcome.
void RenameStatementStops() {
    Source source;
    TableCatalog catalog(source.Config());
    {
        StatementControl control(StatementControl::Clock::now(), 0U);
        assert(control.Start());
        ScopedStatementControl context(&control);
        assert(control.RequestStop(StatementStopReason::kCancelled));
        (void)Throws<StatementStoppedError>([&] { catalog.RenameTable("t", "t_old", nullptr); });
    }
    assert(catalog.Fences().empty());
    CheckLive(source, catalog, "t");
    {
        StatementControl control(StatementControl::Clock::now(), 0U);
        assert(control.Start());
        ScopedStatementControl context(&control);
        catalog.RenameTable("t", "t_old", nullptr);
        assert(control.decision_entered() && !control.RequestStop(StatementStopReason::kCancelled));
    }
    CheckLive(source, catalog, "t_old");
}

}  // namespace

int main() {
    RestoresIntoNewName();
    KeepsTheBackedUpName();
    Refusals();
    DamagedBackups();
    InjectedFailures();
    Cancellation();
    StatementStops();
    MarkedSlotBecomesLost();
    RenamesTable();
    RenameRefusals();
    RenameStatementStops();
    RenameRefusesStaleVersions();
    if (kAtomicExchange) {
        SwapRefusesStaleVersions();
        SwapsNames();
        SwapSurvivesRestart();
        FeedAfterSwap();
        SwapRefusals();
        SwapMarksSlots();
        SwapMarksSurviveRestart();
    } else {
        SwapRefusedOnWindows();
    }
    // Existing chunks of the table that takes a name keep their versions.
    RestoreAfterDropRefusesExistingStaleVersion();
    RenameOntoDroppedRefusesExistingStaleVersion();
    if (kAtomicExchange) SwapRefusesExistingStaleVersion();
    std::cout << "restore table tests passed\n";
}
