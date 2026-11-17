// RESTORE TABLE into a running catalog (docs/design/RESTORE_TABLE.md).
#include <stop_token>

#include "restore_table_test_utils.hpp"
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
            std::uint64_t reported_files = 0U, reported_bytes = 0U, total_files = 0U, total_bytes = 0U;
            auto request = source.Request();
            request.progress = [&](std::uint64_t files, std::uint64_t bytes, std::uint64_t all_files, std::uint64_t all_bytes) {
                assert(files > reported_files && bytes >= reported_bytes && files <= all_files && bytes <= all_bytes);
                // Not visible until the restore publishes it.
                assert(catalog.Find("t_restored") == nullptr && !std::filesystem::exists(source.data / "tables/t_restored"));
                reported_files = files; reported_bytes = bytes; total_files = all_files; total_bytes = all_bytes;
            };
            const auto result = catalog.RestoreTable(request);
            assert(result.name == "t_restored" && result.parent_epoch == source.epoch && result.fork_revision == source.cut);
            assert(result.files == total_files && reported_files == total_files);
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
    catalog.Drop("t");
    auto request = source.Request("t");
    const auto result = catalog.RestoreTable(request);
    const auto table = catalog.Require("t");
    assert(table->store_id() == result.epoch && result.epoch != source.epoch);
    assert(table->Acquire()->store().GetBlockBits(8, 0) == "00000011");
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
    request.progress = [&](std::uint64_t, std::uint64_t, std::uint64_t, std::uint64_t) { stop.request_stop(); };
    assert(Throws([&] { (void)catalog.RestoreTable(request); }).find("cancelled") != std::string::npos);
    LeavesNothingAfterRefusal(source, catalog);
    std::stop_source before;
    before.request_stop();
    request.cancelled = before.get_token();
    request.progress = {};
    assert(Throws([&] { (void)catalog.RestoreTable(request); }).find("cancelled") != std::string::npos);
    LeavesNothingAfterRefusal(source, catalog);
}

}  // namespace

int main() {
    RestoresIntoNewName();
    KeepsTheBackedUpName();
    Refusals();
    DamagedBackups();
    InjectedFailures();
    Cancellation();
    std::cout << "restore table tests passed\n";
}
