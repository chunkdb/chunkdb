#include <algorithm>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "backup.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "feed_test_utils.hpp"
#include "login_helpers.hpp"
#include "migrations_records.hpp"
#include "store_manifest.hpp"
#include "feed_slot_records.hpp"
#include "users.hpp"
#include "verify.hpp"
#include "wal_archive.hpp"

using namespace chunkdb;
using namespace chunkdb::feed_test;

namespace chunkdb {
struct WalArchiveTestAccess {
    static void AgeSeals(TableCatalog& catalog, std::chrono::milliseconds age) {
        auto& archiver = *catalog.archiver_;
        std::lock_guard lock(archiver.pass_mutex_);
        archiver.last_attempt_ms_ += static_cast<std::uint64_t>(age.count());
    }
};
}  // namespace chunkdb

namespace {

template <typename Function>
void Throws(Function&& function) {
    bool thrown = false;
    try { function(); } catch (const std::exception&) { thrown = true; }
    assert(thrown);
}

CatalogConfig ArchiveConfig(const std::filesystem::path& data, const std::filesystem::path& archive) {
    auto config = Config(data);
    config.wal_archive.directory = archive;
    config.wal_archive.interval = std::chrono::hours(24);
    return config;
}

std::vector<std::filesystem::path> Files(const std::filesystem::path& directory, std::string_view suffix) {
    std::vector<std::filesystem::path> files;
    if (!std::filesystem::exists(directory)) return files;
    for (const auto& entry : std::filesystem::directory_iterator(directory))
        if (entry.path().filename().string().ends_with(suffix)) files.push_back(entry.path());
    std::sort(files.begin(), files.end());
    return files;
}

std::vector<WalArchiveRecord> Records(const std::filesystem::path& archive, const StoreId& directory) {
    std::vector<WalArchiveRecord> records;
    for (const auto& path : Files(archive / WalArchiveDirectoryDirName(directory), ""))
        if (ParseWalArchiveRecordName(path.filename().string())) records.push_back(ParseWalArchiveRecord(LoadFile(path)));
    return records;
}

const WalArchiveTableEntry* Entry(const WalArchiveRecord& record, const StoreId& epoch) {
    for (const auto& table : record.tables) if (table.epoch == epoch) return &table;
    return nullptr;
}

void Write(Table& table, std::int64_t x, std::int64_t y, std::uint32_t value) {
    auto lease = table.Acquire();
    assert(lease);
    lease->store().SetBlockBits(x, y, Bits(value));
}

// The worker's first pass runs when the catalog opens; tests that count
// records start after it.
void AwaitFirstPass(const TableCatalog& catalog) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    for (;;) {
        const auto status = catalog.ArchiveStatus();
        assert(status);
        if (status->last_seal_ms != 0U || status->error) return;
        assert(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

std::uint64_t Completed(Table& table) {
    auto lease = table.Acquire();
    assert(lease);
    return *lease->store().CompletedRevision();
}

void RecordFormat() {
    WalArchiveRecord record{{1U, 2U}, 42U, 41U, WalArchiveContentName("users", {1U}), "", {}};
    const auto manifest = WalArchiveContentName("table.2", {2U}) + ".manifest";
    record.tables.push_back({"world", {3U}, 9U, 40U, 2U, manifest,
        {{"C_0_0.5-9.wal", 100U, 7U}, {"C_0_0.5.base", 80U, 8U}, {manifest, 10U, 9U}},
        WalArchiveTableState::kSealed});
    record.tables.push_back({"", {4U}, std::nullopt, 0U, 0U, "", {{"C_1_0.3-4.wal", 10U, 9U}},
        WalArchiveTableState::kDetached});
    record.tables.push_back({"frozen", {5U}, 5U, 32U, 1U, WalArchiveContentName("table.1", {3U}) + ".manifest", {},
        WalArchiveTableState::kFenced, "lost durability"});
    record.tables.push_back({"busy", {6U}, std::nullopt, 0U, 0U, "", {}, WalArchiveTableState::kBusy});
    const auto bytes = SerializeWalArchiveRecord(record);
    assert(ParseWalArchiveRecord(bytes) == record);
    auto unknown_state = bytes;
    const auto first_state = 40U + 1U + record.users.size() + 1U + record.ledger.size() + 4U;
    unknown_state.at(first_state) = 255U;
    unknown_state.resize(unknown_state.size() - 4U);
    WriteLe32(unknown_state, Crc32(unknown_state));
    Throws([&] { (void)ParseWalArchiveRecord(unknown_state); });
    auto damaged = bytes;
    damaged[damaged.size() / 2U] ^= 1U;
    Throws([&] { (void)ParseWalArchiveRecord(damaged); });
    Throws([&] { (void)ParseWalArchiveRecord({bytes.begin(), bytes.end() - 1}); });
    auto repeated = record;
    repeated.tables[1].epoch = repeated.tables[0].epoch;
    Throws([&] { (void)ParseWalArchiveRecord(SerializeWalArchiveRecord(repeated)); });
    auto path = record;
    path.tables[0].files[0].name = "../outside";
    Throws([&] { (void)SerializeWalArchiveRecord(path); });
    auto unordered = record;
    unordered.predecessor_ms = unordered.captured_ms;
    Throws([&] { (void)SerializeWalArchiveRecord(unordered); });
    // A position must be explicitly present to carry its time bound.
    auto bounded = record;
    bounded.tables[1].time_bound_ms = 5U;
    Throws([&] { (void)SerializeWalArchiveRecord(bounded); });

    const StoreId directory{7U, 8U};
    const auto chain = SerializeWalArchiveChain(directory, 42U);
    assert(chain.size() == 36U && ParseWalArchiveChain(chain, directory) == 42U);
    Throws([&] { (void)ParseWalArchiveChain(chain, StoreId{9U}); });
    auto damaged_chain = chain;
    damaged_chain[24] ^= 1U;
    Throws([&] { (void)ParseWalArchiveChain(damaged_chain, directory); });
    assert(WalArchiveContentName("users", {1U, 2U}).starts_with("users.") && WalArchiveContentName("users", {1U, 2U}).size() == 70U);

    assert(WalArchiveRecordName(42U) == "seal.00000000000000000042");
    assert(ParseWalArchiveRecordName("seal.00000000000000000042") == 42U);
    assert(!ParseWalArchiveRecordName("seal.42") && !ParseWalArchiveRecordName(".seal.00000000000000000042.copy"));
    const StoreId epoch{0xabU, 0x01U};
    assert(ParseWalArchiveTableDirName(WalArchiveTableDirName(epoch)) == epoch);
    assert(!ParseWalArchiveTableDirName("table.AB010000000000000000000000000000"));
    assert(WalArchiveSegmentLast("C_-1_2.5-9.wal") == 9U && WalArchiveSegmentLast("C_-1_-2_3.15-27.wal") == 27U);
    assert(!WalArchiveSegmentLast("C_0_0.5.base") && !WalArchiveSegmentLast("table.1.0000abcd.manifest"));
    assert(ExpandWalArchiveCommand("cp %p dest/%f 100%%", "a b", "t/c") == "cp a b dest/t/c 100%");
}

using ArchiveTree = std::map<std::filesystem::path, std::pair<std::filesystem::file_type, std::vector<std::uint8_t>>>;

ArchiveTree SnapshotTree(const std::filesystem::path& root) {
    ArchiveTree result;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        const auto type = entry.symlink_status().type();
        std::vector<std::uint8_t> bytes;
        if (type == std::filesystem::file_type::regular) bytes = LoadFile(entry.path());
        if (type == std::filesystem::file_type::symlink) {
            const auto target = std::filesystem::read_symlink(entry.path()).generic_string();
            bytes.assign(target.begin(), target.end());
        }
        result.emplace(entry.path().lexically_relative(root), std::pair{type, std::move(bytes)});
    }
    return result;
}

std::vector<std::uint8_t> SealField(std::string_view value, std::size_t width) {
    assert(value.size() <= 255U && (width == 1U || width == 2U));
    std::vector<std::uint8_t> bytes{static_cast<std::uint8_t>(value.size())};
    if (width == 2U) bytes.push_back(0U);
    bytes.insert(bytes.end(), value.begin(), value.end());
    return bytes;
}

std::size_t FindSealField(const std::vector<std::uint8_t>& bytes, std::string_view value, std::size_t width) {
    const auto field = SealField(value, width);
    const auto found = std::search(bytes.begin(), bytes.end(), field.begin(), field.end());
    assert(found != bytes.end());
    return static_cast<std::size_t>(found - bytes.begin());
}

void ReplaceSealField(std::vector<std::uint8_t>& bytes, std::string_view before, std::string_view after, std::size_t width = 1U) {
    const auto offset = FindSealField(bytes, before, width);
    const auto start = bytes.begin() + static_cast<std::ptrdiff_t>(offset);
    const auto field = SealField(after, width);
    const auto at = bytes.erase(start, start + static_cast<std::ptrdiff_t>(width + before.size()));
    bytes.insert(at, field.begin(), field.end());
}

void RejectMalformedArchiveSources() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-input");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto data = root / "data", archive = root / "archive", backup = root / "backup", target = root / "restored";
    const auto outside = root / "outside";
    EnsureDirectoryPathExists(outside, true);
    const std::vector<std::uint8_t> sentinel{1U, 2U, 3U};
    AtomicWrite(outside / "sentinel", sentinel, true, true);
    StoreId directory{};
    std::uint64_t capture = 0U;
    Users users;
    users.secret.fill(7U);
    {
        TableCatalog catalog(ArchiveConfig(data, archive));
        AwaitFirstPass(catalog);
        const auto table = CreateDefault(catalog);
        (void)catalog.Create("another", table->geometry().config(), catalog.default_options());
        directory = ReadDataDirManifest(data)->data_dir_id;
        WriteUsersFile(data, users);
        AtomicWrite(data / kMigrationsFileName, EncodeMigrationRecords(directory, {{"example", 1U, "", "CREATE TABLE default"}}), true, true);
        catalog.RunArchivePass();
        (void)catalog.BackupTo(backup, {});
        WalArchiveTestAccess::AgeSeals(catalog, std::chrono::seconds(1));
        Write(*table, 0, 0, 8U);
        catalog.RunArchivePass();
        capture = catalog.ArchiveStatus()->last_seal_ms;
    }
    const auto records = archive / WalArchiveDirectoryDirName(directory);
    const auto seal_path = records / WalArchiveRecordName(capture);
    const auto valid = LoadFile(seal_path);
    const auto seal = ParseWalArchiveRecord(valid);
    assert(seal.tables.size() == 2U && !seal.users.empty() && !seal.ledger.empty());
    const auto& table = seal.tables.back();
    const auto reject = [&](std::string_view message) {
        const auto before = SnapshotTree(root);
        std::string refusal;
        try { (void)RestoreArchiveSeal(backup, target, archive, capture); }
        catch (const std::exception& error) { refusal = error.what(); }
        assert(refusal.find(message) != std::string::npos);
        std::ostringstream findings;
        assert(VerifyWalArchive(archive, findings).errors != 0U);
        assert(findings.str().find(message) != std::string::npos);
        assert(!std::filesystem::exists(target) && LoadFile(outside / "sentinel") == sentinel);
        assert(SnapshotTree(root) == before);
    };
    const auto reject_record = [&](std::string_view message, const auto& change) {
        auto forged = valid;
        change(forged);
        forged.resize(forged.size() - 4U);
        WriteLe32(forged, Crc32(forged));
        AtomicWrite(seal_path, forged, true, true);
        reject(message);
        AtomicWrite(seal_path, valid, true, true);
    };

    reject_record("invalid WAL archive table name", [&](auto& bytes) { ReplaceSealField(bytes, table.name, "../x"); });
    reject_record("invalid WAL archive file name", [&](auto& bytes) { ReplaceSealField(bytes, table.manifest, "/absolute"); });
    reject_record("invalid WAL archive file name", [&](auto& bytes) { ReplaceSealField(bytes, seal.users, "outside/users"); });
    reject_record("invalid WAL archive file name", [&](auto& bytes) { ReplaceSealField(bytes, seal.ledger, "outside\\ledger"); });
    reject_record("invalid WAL archive metadata name", [&](auto& bytes) { ReplaceSealField(bytes, table.manifest, "table.01." + table.manifest.substr(8U)); });
    reject_record("repeats a table name", [&](auto& bytes) { ReplaceSealField(bytes, table.name, seal.tables.front().name); });
    reject_record("repeats a table epoch", [&](auto& bytes) {
        const auto offset = FindSealField(bytes, table.name, 1U) + 1U + table.name.size();
        std::copy(seal.tables.front().epoch.begin(), seal.tables.front().epoch.end(), bytes.begin() + static_cast<std::ptrdiff_t>(offset));
    });
    const auto segment = std::find_if(table.files.begin(), table.files.end(), [](const auto& file) { return file.name.ends_with(".wal"); });
    const auto descriptor = std::find_if(table.files.begin(), table.files.end(), [](const auto& file) { return file.name.ends_with(".base"); });
    assert(segment != table.files.end() && descriptor != table.files.end());
    reject_record("unknown or noncanonical WAL archive inventory name", [&](auto& bytes) {
        ReplaceSealField(bytes, segment->name, "unrecognized.txt", 2U);
    });
    reject_record("unknown or noncanonical WAL archive inventory name", [&](auto& bytes) {
        ReplaceSealField(bytes, descriptor->name, "C_00" + descriptor->name.substr(3U), 2U);
    });
    reject_record("table count exceeds remaining record bytes", [&](auto& bytes) {
        bytes.at(40U + 1U + seal.users.size() + 1U + seal.ledger.size()) = 255U;
    });
    reject_record("file count exceeds remaining record bytes", [&](auto& bytes) {
        const auto offset = FindSealField(bytes, table.name, 1U) + 1U + table.name.size() + 16U + 24U
            + 1U + table.manifest.size() + 1U + table.reason.size();
        bytes.at(offset) = 255U;
    });

    const auto users_path = records / seal.users;
    const auto encoded = EncodeUsers(users);
    users.secret.fill(8U);
    AtomicWrite(users_path, EncodeUsers(users), true, true);
    reject("WAL archive metadata digest does not match");
    AtomicWrite(users_path, encoded, true, true);

#ifndef _WIN32
    const auto moved_users = outside / "users";
    std::filesystem::rename(users_path, moved_users);
    std::filesystem::create_symlink(moved_users, users_path);
    reject("invalid backup source file");
    std::filesystem::remove(users_path);
    std::filesystem::rename(moved_users, users_path);

    const auto table_dir = archive / WalArchiveTableDirName(table.epoch);
    const auto wal_path = table_dir / segment->name;
    const auto moved_wal = outside / "wal";
    std::filesystem::rename(wal_path, moved_wal);
    std::filesystem::create_symlink(moved_wal, wal_path);
    reject("invalid backup source file");
    std::filesystem::remove(wal_path);
    std::filesystem::rename(moved_wal, wal_path);

    const auto moved_table = outside / "table";
    std::filesystem::rename(table_dir, moved_table);
    std::filesystem::create_directory_symlink(moved_table, table_dir);
    reject("not a safe directory");
    std::filesystem::remove(table_dir);
    std::filesystem::rename(moved_table, table_dir);

    const auto moved_records = outside / "records";
    std::filesystem::rename(records, moved_records);
    std::filesystem::create_directory_symlink(moved_records, records);
    reject("not a safe directory");
    std::filesystem::remove(records);
    std::filesystem::rename(moved_records, records);

    const auto alias = root / "archive-alias";
    std::filesystem::create_directory_symlink(archive, alias);
    std::ostringstream findings;
    assert(VerifyWalArchive(alias, findings).errors == 0U);
    assert(RestoreArchiveSeal(backup, target, alias, capture).empty());
#endif
}

void Configuration() {
    WalArchiveConfig both{.directory = "a", .command = "cp %p b"};
    Throws([&] { RequireValidWalArchiveConfig(both); });
    Throws([&] { RequireValidWalArchiveConfig({.command = "cp file b/%f"}); });
    Throws([&] { RequireValidWalArchiveConfig({.command = "cp %p b/%x"}); });
    Throws([&] { RequireValidWalArchiveConfig({.command = "cp %p b/%f", .retention = std::chrono::hours(1)}); });
    Throws([&] { RequireValidWalArchiveConfig({.directory = "a", .interval = std::chrono::milliseconds(50)}); });
    RequireValidWalArchiveConfig({.directory = "a", .retention = std::chrono::hours(1)});
    RequireValidWalArchiveConfig({.command = "cp %p b/%f"});

    test::ScopedTempDir temporary("chunkdb-wal-archive-config");
    const auto root = std::filesystem::canonical(temporary.path());
    { TableCatalog catalog(Config(root / "data")); }
    auto read_only = ArchiveConfig(root / "data", root / "archive");
    read_only.access_mode = AccessMode::kReadOnly;
    Throws([&] { TableCatalog catalog(read_only); });
    Throws([&] { TableCatalog catalog(ArchiveConfig(root / "data", root / "data" / "archive")); });
}

void DirectoryArchive() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-dir");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto data = root / "data", archive = root / "archive";
    StoreId epoch{}, directory{};
    {
        TableCatalog catalog(ArchiveConfig(data, archive));
        AwaitFirstPass(catalog);
        const auto table = CreateDefault(catalog);
        epoch = table->store_id();
        directory = ReadDataDirManifest(data)->data_dir_id;
        Write(*table, 0, 0, 1U);
        Write(*table, 40, 40, 2U);
        const auto first = Completed(*table);
        catalog.RunArchivePass();
        const auto records = Records(archive, directory);
        assert(!records.empty());
        const auto* entry = Entry(records.back(), epoch);
        assert(entry && entry->name == "default" && entry->revision == first && entry->schema_version == 1U);
        const auto files = archive / WalArchiveTableDirName(epoch);
        for (const auto& record : records) {
            if (const auto* listed = Entry(record, epoch)) {
                for (const auto& file : listed->files) {
                    const auto bytes = LoadFile(files / file.name);
                    assert(bytes.size() == file.size && Crc32(bytes) == file.crc32);
                }
            }
        }
        // Both chunks are sealed and shipped; nothing stays staged or archived locally.
        assert(Files(files, ".wal").size() == 2U && Files(files, ".base").size() == 2U);
        assert(Files(files, ".manifest").size() == 1U);
        assert(Files(data / "tables/default" / kFeedArchiveDirName, ".wal").empty());
        assert(!std::filesystem::exists(data / std::string(kWalArchiveStagingName) / WalArchiveTableDirName(epoch)));

        catalog.ChangeColumns("default", [](const TableSchema& schema) {
            return AddColumn(schema, {.id = 0U, .name = "note", .type = {ColumnKind::kBytes, 16U},
                .nullable = true, .default_value = {}});
        });
        {
            // Bit-string writes need a table with one bits column; this one now has two.
            auto lease = table->Acquire();
            assert(lease);
            lease->store().SetBlock(0, 0, {{"note", BytesValue{{1U}}}});
        }
        const auto second = Completed(*table);
        assert(second > first);
        catalog.RunArchivePass();
        const auto latest = Records(archive, directory);
        entry = Entry(latest.back(), epoch);
        assert(entry && entry->revision == second && entry->schema_version == 2U);
        assert(Files(files, ".manifest").size() == 2U);
        // The chunk's two segments follow each other without overlap.
        std::vector<std::uint64_t> lasts;
        for (const auto& segment : Files(files, ".wal")) {
            const auto name = segment.filename().string();
            if (name.starts_with("C_0_0.")) lasts.push_back(*WalArchiveSegmentLast(name));
        }
        assert(lasts.size() == 2U && lasts[0] < lasts[1] && lasts[1] == second);
        const auto status = catalog.ArchiveStatus();
        assert(status && status->mode == "dir" && !status->error && status->tables.size() == 1U &&
            status->tables[0].name == "default" && status->tables[0].revision == second);
    }
    const auto before = Records(archive, directory).size();
    {
        // A restart stages the manifest again; equal bytes ship under the same name.
        TableCatalog catalog(ArchiveConfig(data, archive));
        catalog.RunArchivePass();
        assert(Files(archive / WalArchiveTableDirName(epoch), ".manifest").size() >= 2U);
        assert(!catalog.ArchiveStatus()->error);
    }
    assert(Records(archive, directory).size() > before);
}

void ChangedMetadata() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-metadata");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto data = root / "data", archive = root / "archive";
    TableCatalog catalog(ArchiveConfig(data, archive));
    AwaitFirstPass(catalog);
    const auto table = CreateDefault(catalog);
    const auto directory = ReadDataDirManifest(data)->data_dir_id;
    const auto records_dir = archive / WalArchiveDirectoryDirName(directory);
    const auto table_dir = archive / WalArchiveTableDirName(table->store_id());
    Users users;
    users.secret.fill(7U);
    WriteUsersFile(data, users);
    const auto ledger_path = data / std::string(kMigrationsFileName);
    std::vector<MigrationRecord> migrations{{"create_default", 1U, "", "CREATE TABLE default"}};
    AtomicWrite(ledger_path, EncodeMigrationRecords(directory, migrations), true, true);
    catalog.RunArchivePass();
    const auto first = Records(archive, directory).back();

    users.secret.fill(8U);
    WriteUsersFile(data, users);
    migrations[0].applied_ms = 2U;
    const auto ledger = EncodeMigrationRecords(directory, migrations);
    AtomicWrite(ledger_path, ledger, true, true);
    TableOptionsUpdate options;
    options.checkpoint_update_interval = 200U;
    catalog.SetOptions("default", options);
    catalog.RunArchivePass();
    const auto second = Records(archive, directory).back();
    assert(first.users != second.users && first.ledger != second.ledger);
    assert(LoadFile(records_dir / second.users) == EncodeUsers(users));
    assert(LoadFile(records_dir / second.ledger) == ledger);
    const auto* before = Entry(first, table->store_id());
    const auto* after = Entry(second, table->store_id());
    assert(before && after && before->schema_version == after->schema_version && before->manifest != after->manifest);
    assert(LoadFile(table_dir / after->manifest) == LoadFile(StoreManifestPath(data / "tables/default")));
}

void ExistingArchiveFileMustMatch() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-existing-file");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto data = root / "data", archive = root / "archive";
    StoreId directory{};
    Users users;
    users.secret.fill(7U);
    {
        TableCatalog catalog(Config(data));
        directory = ReadDataDirManifest(data)->data_dir_id;
        WriteUsersFile(data, users);
    }
    const auto expected = EncodeUsers(users);
    users.secret.fill(8U);
    const auto different = EncodeUsers(users);
    assert(expected != different && expected.size() == different.size() && Crc32(expected) == Crc32(different));
    const auto target = archive / WalArchiveDirectoryDirName(directory) / WalArchiveContentName("users", expected);
    AtomicWrite(target, different, true, true);
    TableCatalog catalog(ArchiveConfig(data, archive));
    AwaitFirstPass(catalog);
    Throws([&] { catalog.RunArchivePass(); });
    assert(Records(archive, directory).empty() && LoadFile(target) == different);
}

void RetrySyncsExistingArchiveFiles() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-retry-sync");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto data = root / "data", archive = root / "archive";
    TableCatalog catalog(ArchiveConfig(data, archive));
    AwaitFirstPass(catalog);
    const auto table = CreateDefault(catalog);
    catalog.RunArchivePass();
    const auto last = catalog.ArchiveStatus()->last_seal_ms;
    const auto target = archive / WalArchiveTableDirName(table->store_id());
    Write(*table, 0, 0, 1U);
    bool interrupted = false;
    bool resynced = false;
    SetDirectorySyncObserverForTests([&](const auto& directory) {
        if (directory != target || Files(target, ".wal").empty() || Files(target, ".base").empty()) return;
        if (!interrupted) {
            interrupted = true;
            throw std::runtime_error("interrupt archive publication after its final file rename");
        }
        resynced = true;
    });
    Throws([&] { catalog.RunArchivePass(); });
    assert(interrupted && catalog.ArchiveStatus()->last_seal_ms == last);
    catalog.RunArchivePass();
    SetDirectorySyncObserverForTests({});
    assert(resynced && catalog.ArchiveStatus()->last_seal_ms > last && !catalog.ArchiveStatus()->error);
}

void CheckRestoredHealthy(const std::filesystem::path& target, std::uint32_t value) {
    TableCatalog restored(Config(target));
    assert(!restored.Find("default"));
    const auto table = restored.Find("healthy");
    assert(table);
    const auto lease = table->Acquire();
    assert(lease && lease->store().GetBlockBits(0, 0) == Bits(value));
}

void BusyTableLeavesHealthySeals() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-busy-table");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto backup = root / "backup", archive = root / "archive", target = root / "restored";
    const auto data = root / "data";
    TableCatalog catalog(ArchiveConfig(data, archive));
    AwaitFirstPass(catalog);
    const auto table = CreateDefault(catalog);
    const auto healthy = catalog.Create("healthy", table->geometry().config(), catalog.default_options());
    const auto directory = ReadDataDirManifest(data)->data_dir_id;
    Write(*healthy, 0, 0, 7U);
    (void)catalog.BackupTo(backup, {});
    struct CaptureWhileBusy : MigrationTestHook {
        CaptureWhileBusy(TableCatalog& catalog_in, Table& healthy_in)
            : catalog(catalog_in), healthy(healthy_in) {}
        void Run(Point point, std::string_view name) override {
            if (point != Point::kBeforeCatalogAdmission) return;
            assert(name == "default");
            for (const auto value : {1U, 2U}) {
                const auto last = catalog.ArchiveStatus()->last_seal_ms;
                Write(healthy, 0, 0, value);
                catalog.RunArchivePass();
                const auto status = catalog.ArchiveStatus();
                assert(!status->error && status->last_seal_ms > last);
                assert(status->tables.size() == 1U && status->tables.front().name == "healthy");
            }
            ran = true;
        }
        TableCatalog& catalog;
        Table& healthy;
        bool ran = false;
    } hook(catalog, *healthy);
    catalog.SetMigrationTestHook(&hook);
    TableOptionsUpdate options;
    options.checkpoint_update_interval = 200U;
    catalog.SetOptions("default", options);
    catalog.SetMigrationTestHook(nullptr);
    assert(hook.ran);
    const auto seal = Records(archive, directory).back();
    const auto* busy = Entry(seal, table->store_id());
    assert(busy && busy->name == "default" && busy->state == WalArchiveTableState::kBusy);
    assert(!busy->revision && busy->manifest.empty());
    const auto skipped = RestoreArchiveSeal(backup, target, archive, seal.predecessor_ms);
    assert(skipped == std::vector<std::string>{"skipped table 'default': busy; no previous seal"});
    CheckRestoredHealthy(target, 1U);
    catalog.RunArchivePass();
    const auto available = Records(archive, directory).back();
    const auto* prior = Entry(available, table->store_id());
    assert(prior && prior->state == WalArchiveTableState::kSealed);

    catalog.SetMigrationTestHook(&hook);
    options.checkpoint_update_interval = 300U;
    catalog.SetOptions("default", options);
    catalog.SetMigrationTestHook(nullptr);
    const auto again = Records(archive, directory).back();
    const auto* stale = Entry(again, table->store_id());
    assert(stale && stale->state == WalArchiveTableState::kBusy && stale->revision == prior->revision &&
        stale->manifest == prior->manifest && stale->time_bound_ms == prior->time_bound_ms);
}

void FencedTableLeavesHealthySealsAndRetention() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-fenced-table");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto data = root / "data", archive = root / "archive", backup = root / "backup", target = root / "restored";
    auto config = ArchiveConfig(data, archive);
    config.wal_archive.retention = std::chrono::hours(1);
    TableCatalog catalog(config);
    AwaitFirstPass(catalog);
    const auto table = CreateDefault(catalog);
    const auto healthy = catalog.Create("healthy", table->geometry().config(), catalog.default_options());
    const auto directory = ReadDataDirManifest(data)->data_dir_id;
    (void)catalog.BackupTo(backup, {});
    Write(*table, 0, 0, 9U);
    Write(*healthy, 0, 0, 1U);
    catalog.RunArchivePass();
    const auto good = Records(archive, directory).back();
    const auto* prior = Entry(good, table->store_id());
    assert(prior && prior->revision && !prior->manifest.empty());
    const auto blocked_files = archive / WalArchiveTableDirName(table->store_id());
    const auto healthy_files = archive / WalArchiveTableDirName(healthy->store_id());
    const auto old_healthy = Files(healthy_files, ".wal").front();
    const auto old_blocked = Files(blocked_files, ".wal").front();
    assert(old_healthy != old_blocked);

    {
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_TABLESET_REOPEN_FAIL_ONCE", "1");
        TableOptionsUpdate options;
        options.checkpoint_update_interval = 200U;
        Throws([&] { catalog.SetOptions("default", options); });
    }
    assert(!catalog.Find("default") && catalog.Fence("default"));
    for (const auto value : {2U, 3U}) {
        const auto last = catalog.ArchiveStatus()->last_seal_ms;
        Write(*healthy, 0, 0, value);
        catalog.RunArchivePass();
        assert(catalog.ArchiveStatus()->last_seal_ms > last && !catalog.ArchiveStatus()->error);
    }
    const auto seal = Records(archive, directory).back();
    const auto* fenced = Entry(seal, table->store_id());
    assert(fenced && fenced->state == WalArchiveTableState::kFenced && !fenced->reason.empty());
    assert(fenced->revision == prior->revision && fenced->time_bound_ms == prior->time_bound_ms &&
        fenced->manifest == prior->manifest);
    const auto skipped = RestoreArchiveSeal(backup, target, archive, seal.captured_ms);
    assert(skipped.size() == 1U && skipped.front().starts_with("skipped table 'default': fenced") &&
        skipped.front().ends_with(std::to_string(*prior->revision)));
    CheckRestoredHealthy(target, 3U);

    WalArchiveTestAccess::AgeSeals(catalog, std::chrono::hours(2));
    Write(*healthy, 0, 0, 4U);
    catalog.RunArchivePass();
    assert(!std::filesystem::exists(old_healthy) && std::filesystem::exists(old_blocked));
    const auto kept = Records(archive, directory);
    assert(kept.size() == 2U && kept.front().captured_ms == seal.captured_ms);
    const auto* pinned = Entry(kept.back(), table->store_id());
    assert(pinned && pinned->state == WalArchiveTableState::kFenced && pinned->revision == prior->revision);
    assert(std::any_of(pinned->files.begin(), pinned->files.end(),
        [&](const auto& file) { return file.name == old_blocked.filename().string(); }));
    const auto saved = ReadWalArchiveRecord(data / kWalArchiveStagingName / "last");
    const auto* remembered = Entry(saved, table->store_id());
    assert(remembered && remembered->revision == prior->revision && remembered->files.empty());
    std::ostringstream findings;
    assert(VerifyWalArchive(archive, findings).errors == 0U);

    catalog.RecoverTable("default");
    const auto recovered = catalog.Find("default");
    assert(recovered);
    Write(*recovered, 0, 0, 10U);
    catalog.RunArchivePass();
    const auto next = Records(archive, directory).back();
    const auto* normal = Entry(next, recovered->store_id());
    assert(normal && normal->state == WalArchiveTableState::kSealed && normal->revision == Completed(*recovered));
}

void ArchiveShowStatus() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-show-status");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto catalog = std::make_shared<TableCatalog>(ArchiveConfig(root / "data", root / "archive"));
    CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
    SessionState session;
    (void)engine.Execute(session, "HELLO 3\r\n");
    AwaitFirstPass(*catalog);
    const auto table = CreateDefault(*catalog);
    Write(*table, 0, 0, 1U);
    catalog->RunArchivePass();
    const auto status = catalog->ArchiveStatus();
    const auto reply = engine.Execute(session, "SHOW STATUS\r\n");
    assert(reply.starts_with("%8\r\n"));
    assert(reply.ends_with("$7\r\narchive\r\n%5\r\n$4\r\nmode\r\n$3\r\ndir\r\n$12\r\nlast_seal_ms\r\n:" +
        std::to_string(status->last_seal_ms) + "\r\n$5\r\nerror\r\n_\r\n$12\r\nstaged_bytes\r\n:0\r\n" +
        "$6\r\ntables\r\n*1\r\n%2\r\n$4\r\nname\r\n$7\r\ndefault\r\n$8\r\nrevision\r\n:" +
        std::to_string(Completed(*table)) + "\r\n"));
}

void PendingGrantStopsArchiveAndWrites() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-pending-grant");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto data = root / "data";
    auto config = ArchiveConfig(data, root / "archive");
    std::uint64_t before = 0U;
    {
        const auto catalog = std::make_shared<TableCatalog>(config);
        AwaitFirstPass(*catalog);
        const auto table = CreateDefault(*catalog);
        (void)catalog->Create("healthy", table->geometry().config(), catalog->default_options());
        (void)catalog->Create("other", table->geometry().config(), catalog->default_options());
        const auto users = test::MakeUsers(data, "admin", "admin-password");
        CommandEngine engine(EngineConfig{.require_auth = false, .users = users}, catalog);
        SessionState session;
        assert(engine.Execute(session, "HELLO 3\r\n").starts_with('%'));
        catalog->RunArchivePass();
        before = catalog->ArchiveStatus()->last_seal_ms;
        {
            txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_MIGRATION_AFTER_DECISION_FAIL_ONCE", "1");
            assert(engine.Execute(session, "MIGRATE 'grant' GRANT WRITE ON healthy TO admin\r\n").starts_with("-ERR OUTCOME_UNKNOWN "));
        }
        assert(ReadMigrationJournal(data));
        const auto write = engine.Execute(session, "SET BLOCK 0 0 IN other bits = b'00000000000000000000000000000001'\r\n");
        assert(write.starts_with("-ERR INTERNAL ") && write.find(kMigrationRecoveryRequiredMessage) != std::string::npos);
        Throws([&] { catalog->RunArchivePass(); });
        assert(catalog->ArchiveStatus()->last_seal_ms == before);
    }
    TableCatalog recovered(config);
    AwaitFirstPass(recovered);
    assert(!ReadMigrationJournal(data) && recovered.ArchiveStatus()->last_seal_ms > before);
    Write(*recovered.Find("other"), 0, 0, 1U);
}

// Records of one chain, oldest first: each names the previous one, and only
// `starts` of them start a chain.
void CheckChain(const std::vector<WalArchiveRecord>& records, std::size_t starts) {
    std::size_t seen = 0U;
    for (std::size_t i = 0U; i < records.size(); ++i) {
        if (records[i].predecessor_ms == 0U) { ++seen; continue; }
        assert(i > 0U && records[i].predecessor_ms == records[i - 1U].captured_ms);
    }
    assert(seen == starts);
}

void ChainAndTimeBound() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-chain");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto data = root / "data", archive = root / "archive";
    {
        // A users file is captured and shipped by content.
        Users users;
        users.secret.fill(7U);
        std::filesystem::create_directories(data);
        TableCatalog catalog(Config(data));
        (void)CreateDefault(catalog);
        WriteUsersFile(data, users);
    }
    StoreId directory{};
    {
        TableCatalog catalog(ArchiveConfig(data, archive));
        AwaitFirstPass(catalog);
        const auto table = catalog.Find("default");
        directory = ReadDataDirManifest(data)->data_dir_id;
        Write(*table, 0, 0, 1U);
        catalog.RunArchivePass();
        const auto record = Records(archive, directory).back();
        const auto* entry = Entry(record, table->store_id());
        assert(entry && entry->revision && entry->time_bound_ms != 0U && !entry->manifest.empty());
        assert(std::filesystem::exists(archive / WalArchiveTableDirName(table->store_id()) / entry->manifest));
        assert(!record.users.empty() && record.ledger.empty());
        assert(std::filesystem::exists(archive / WalArchiveDirectoryDirName(directory) / record.users));
        {
            auto lease = table->Acquire();
            const auto sealed = lease->store().ReadChunkState(0, 0);
            // Frames at or below the seal point took their times before the bound.
            assert(sealed.version <= *entry->revision && sealed.commit_time_ms <= entry->time_bound_ms);
        }
        Write(*table, 0, 0, 2U);
        {
            auto lease = table->Acquire();
            const auto later = lease->store().ReadChunkState(0, 0);
            // A frame above the seal point takes a time at or above the bound.
            assert(later.version > *entry->revision && later.commit_time_ms >= entry->time_bound_ms);
        }
        catalog.RunArchivePass();
    }
    {
        // A restart with archiving continues the chain.
        TableCatalog catalog(ArchiveConfig(data, archive));
        catalog.RunArchivePass();
    }
    auto records = Records(archive, directory);
    assert(records.size() >= 4U);
    CheckChain(records, 1U);
    // Unchanged users bytes ship once per process under the same name.
    assert(records.back().users == records.front().users);
    {
        // A start without archiving ends the chain.
        TableCatalog catalog(Config(data));
        assert(!std::filesystem::exists(data / std::string(kWalArchiveStagingName)));
    }
    {
        TableCatalog catalog(ArchiveConfig(data, archive));
        catalog.RunArchivePass();
    }
    records = Records(archive, directory);
    CheckChain(records, 2U);
}

void Retention() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-retention");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto data = root / "data", archive = root / "archive";
    auto config = ArchiveConfig(data, archive);
    config.wal_archive.retention = std::chrono::milliseconds(1);
    TableCatalog catalog(config);
    AwaitFirstPass(catalog);
    const auto table = CreateDefault(catalog);
    const auto directory = ReadDataDirManifest(data)->data_dir_id;
    for (std::uint32_t value = 1U; value <= 3U; ++value) {
        Write(*table, 0, 0, value);
        catalog.RunArchivePass();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    catalog.RunArchivePass();
    // The newest record older than the bound and every later one stay.
    const auto records = Records(archive, directory);
    assert(records.size() == 2U);
    const auto files = archive / WalArchiveTableDirName(table->store_id());
    for (const auto& segment : Files(files, ".wal")) {
        const auto name = segment.filename().string();
        assert(std::any_of(records.begin(), records.end(), [&](const auto& record) {
            const auto* entry = Entry(record, table->store_id());
            return entry && std::any_of(entry->files.begin(), entry->files.end(), [&](const auto& file) { return file.name == name; });
        }));
    }
    assert(Files(files, ".wal").size() == 1U);
}

void RetentionPreservesRepeatedFiles() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-repeated-files");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto data = root / "data", archive = root / "archive";
    StoreId epoch{}, directory{};
    {
        TableCatalog catalog(Config(data));
        epoch = CreateDefault(catalog)->store_id();
        directory = ReadDataDirManifest(data)->data_dir_id;
    }
    const auto records_dir = archive / WalArchiveDirectoryDirName(directory);
    const auto table_dir = archive / WalArchiveTableDirName(epoch);
    const std::vector<std::uint8_t> bytes{1U};
    const WalArchiveFile duplicate{"C_0_0.1-1.wal", 1U, Crc32(bytes)};
    const WalArchiveFile descriptor{"C_0_0.1.base", 1U, Crc32(bytes)};
    const WalArchiveFile obsolete{"C_1_0.2-2.wal", 1U, Crc32(bytes)};
    for (const auto& file : {duplicate, descriptor, obsolete}) AtomicWrite(table_dir / file.name, bytes, true, true);
    // Age the records deterministically. The kept record re-lists the same
    // segment and descriptor, as staging can do after a restart.
    const auto manifest = WalArchiveContentName("table.1", bytes) + ".manifest";
    const WalArchiveRecord first{directory, 1U, 0U, "", "",
        {{"default", epoch, 2U, 1U, 1U, manifest, {duplicate, descriptor, obsolete}, WalArchiveTableState::kSealed}}};
    const WalArchiveRecord kept{directory, 2U, 1U, "", "",
        {{"default", epoch, 2U, 2U, 1U, manifest, {duplicate, descriptor}, WalArchiveTableState::kSealed}}};
    for (const auto& record : {first, kept})
        AtomicWrite(records_dir / WalArchiveRecordName(record.captured_ms), SerializeWalArchiveRecord(record), true, true);
    AtomicWrite(data / kWalArchiveStagingName / kWalArchiveChainName, SerializeWalArchiveChain(directory, kept.captured_ms), true, true);
    auto config = ArchiveConfig(data, archive);
    config.wal_archive.retention = std::chrono::hours(1);
    TableCatalog catalog(config);
    AwaitFirstPass(catalog);
    assert(!catalog.ArchiveStatus()->error);
    assert(!std::filesystem::exists(records_dir / WalArchiveRecordName(first.captured_ms)));
    assert(std::filesystem::exists(records_dir / WalArchiveRecordName(kept.captured_ms)));
    assert(LoadFile(table_dir / duplicate.name) == bytes && LoadFile(table_dir / descriptor.name) == bytes);
    assert(!std::filesystem::exists(table_dir / obsolete.name));
}

void SlotsKeepTheirSegments() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-slot");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto data = root / "data", archive = root / "archive";
    TableCatalog catalog(ArchiveConfig(data, archive));
    const auto table = CreateDefault(catalog);
    (void)table->CreateFeedSlot("consumer");
    Write(*table, 0, 0, 1U);
    catalog.RunArchivePass();
    const auto local = data / "tables/default" / kFeedArchiveDirName;
    assert(Files(archive / WalArchiveTableDirName(table->store_id()), ".wal").size() == 1U);
    assert(Files(local, ".wal").size() == 1U);
    table->DropFeedSlot("consumer");
    catalog.RunArchivePass();
    assert(Files(local, ".wal").empty());
}

#ifndef _WIN32
void CommandArchive() {
    test::ScopedTempDir temporary("chunkdb-wal-archive-command");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto data = root / "data with spaces", copies = root / "copies";
    auto config = Config(data);
    config.wal_archive.interval = std::chrono::hours(24);
    config.wal_archive.command = "exit 7 # %p";
    StoreId epoch{};
    {
        TableCatalog catalog(config);
        const auto table = CreateDefault(catalog);
        epoch = table->store_id();
        Write(*table, 0, 0, 1U);
        Throws([&] { catalog.RunArchivePass(); });
        const auto status = catalog.ArchiveStatus();
        assert(status && status->mode == "command" && status->error);
        // The sealed segment waits staged for the next pass.
        assert(Files(data / std::string(kWalArchiveStagingName) / WalArchiveTableDirName(epoch), ".wal").size() == 1U);
        assert(Files(data / "tables/default" / kFeedArchiveDirName, ".wal").empty());
    }
    config.wal_archive.command = "mkdir -p \"" + copies.string() + "/$(dirname '%f')\" && cp \"%p\" \"" +
        copies.string() + "/%f\"";
    {
        TableCatalog catalog(config);
        catalog.RunArchivePass();
        assert(!catalog.ArchiveStatus()->error);
    }
    assert(!std::filesystem::exists(data / std::string(kWalArchiveStagingName) / WalArchiveTableDirName(epoch)));
    assert(Files(copies / WalArchiveTableDirName(epoch), ".wal").size() == 1U);
    const auto directory = ReadDataDirManifest(data)->data_dir_id;
    const auto records = Records(copies, directory);
    assert(!records.empty());
    assert(std::any_of(records.begin(), records.end(), [&](const auto& record) {
        const auto* entry = Entry(record, epoch);
        return entry && std::any_of(entry->files.begin(), entry->files.end(),
            [](const auto& file) { return file.name.ends_with(".wal"); });
    }));
}
#endif

}  // namespace

int main() {
    RecordFormat();
    RejectMalformedArchiveSources();
    Configuration();
    DirectoryArchive();
    ChangedMetadata();
    ExistingArchiveFileMustMatch();
    RetrySyncsExistingArchiveFiles();
    BusyTableLeavesHealthySeals();
    FencedTableLeavesHealthySealsAndRetention();
    ArchiveShowStatus();
    PendingGrantStopsArchiveAndWrites();
    ChainAndTimeBound();
    Retention();
    RetentionPreservesRepeatedFiles();
    SlotsKeepTheirSegments();
#ifndef _WIN32
    CommandArchive();
#endif
    std::cout << "wal archive tests passed\n";
    return 0;
}
