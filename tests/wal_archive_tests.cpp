#include <algorithm>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "chunk_store_internal.hpp"
#include "feed_test_utils.hpp"
#include "store_manifest.hpp"
#include "wal_archive.hpp"

using namespace chunkdb;
using namespace chunkdb::feed_test;

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
    WalArchiveRecord record{{1U, 2U}, 42U, {}};
    record.tables.push_back({"world", {3U}, 9U, 2U, {{"C_0_0.5-9.wal", 100U, 7U}, {"C_0_0.5.base", 80U, 8U}}});
    record.tables.push_back({"", {4U}, std::nullopt, 1U, {{"table.1.0000abcd.manifest", 10U, 9U}}});
    const auto bytes = SerializeWalArchiveRecord(record);
    assert(ParseWalArchiveRecord(bytes) == record);
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
        Write(*table, 0, 0, 3U);
        const auto second = Completed(*table);
        catalog.RunArchivePass();
        entry = Entry(Records(archive, directory).back(), epoch);
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
    const auto data = root / "data", copies = root / "copies";
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
    config.wal_archive.command = "mkdir -p \"" + copies.string() + "/$(dirname '%f')\" && cp '%p' \"" +
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
    Configuration();
    DirectoryArchive();
    Retention();
    SlotsKeepTheirSegments();
#ifndef _WIN32
    CommandArchive();
#endif
    std::cout << "wal archive tests passed\n";
    return 0;
}
