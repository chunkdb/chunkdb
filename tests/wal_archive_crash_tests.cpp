// Abrupt exits at each WAL archive step: a restart ships again, the chain of
// seal records has no fork, and every acknowledged write ends up archived.
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "chunkdb/crc32.hpp"
#include "chunk_store_internal.hpp"
#include "feed_test_utils.hpp"
#include "store_manifest.hpp"
#include "wal_archive.hpp"

namespace {
using namespace chunkdb;

CatalogConfig ArchiveConfig(const std::filesystem::path& root, bool retention) {
    auto config = feed_test::Config(root / "data");
    config.wal_archive.directory = root / "archive";
    config.wal_archive.interval = std::chrono::hours(24);
    if (retention) config.wal_archive.retention = std::chrono::milliseconds(1);
    return config;
}

std::string Quote(const std::string& text) {
#ifdef _WIN32
    return '"' + text + '"';
#else
    std::string result = "'";
    for (const char c : text) result += c == '\'' ? "'\\''" : std::string(1, c);
    return result + "'";
#endif
}

int Child(const char* executable, const std::filesystem::path& root, const std::string& point) {
    const auto command = Quote(std::filesystem::absolute(executable).string()) + " --child " + Quote(root.string()) +
        " " + Quote(point);
#ifdef _WIN32
    return std::system(("\"" + command + "\"").c_str());
#else
    const auto result = std::system(command.c_str());
    assert(result != -1 && WIFEXITED(result));
    return WEXITSTATUS(result);
#endif
}

void Write(Table& table, std::int64_t x, std::int64_t y, std::uint32_t value) {
    auto lease = table.Acquire();
    assert(lease);
    lease->store().SetBlockBits(x, y, feed_test::Bits(value));
}

std::uint64_t Version(Table& table, std::int64_t chunk_x, std::int64_t chunk_y) {
    auto lease = table.Acquire();
    assert(lease);
    return lease->store().ReadChunkState(chunk_x, chunk_y).version;
}

void AwaitFirstPass(const TableCatalog& catalog) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (catalog.ArchiveStatus()->last_seal_ms == 0U && !catalog.ArchiveStatus()->error) {
        assert(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

std::vector<WalArchiveRecord> Records(const std::filesystem::path& archive, const StoreId& directory) {
    std::vector<std::filesystem::path> paths;
    const auto records_dir = archive / WalArchiveDirectoryDirName(directory);
    for (const auto& entry : std::filesystem::directory_iterator(records_dir))
        if (ParseWalArchiveRecordName(entry.path().filename().string())) paths.push_back(entry.path());
    std::sort(paths.begin(), paths.end());
    std::vector<WalArchiveRecord> records;
    for (const auto& path : paths) records.push_back(ParseWalArchiveRecord(LoadFile(path)));
    return records;
}

// The child: two writes, a first pass, a third write, and a pass that stops
// at `point`.
int RunChild(const std::filesystem::path& root, const std::string& point) {
    const bool retention = point.find("RETENTION") != std::string::npos;
    TableCatalog catalog(ArchiveConfig(root, retention));
    AwaitFirstPass(catalog);
    const auto table = feed_test::CreateDefault(catalog);
    Write(*table, 0, 0, 11U);
    Write(*table, 4, 0, 22U);
    catalog.RunArchivePass();
    if (retention) {
        for (std::uint32_t value = 1U; value <= 2U; ++value) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            Write(*table, 0, 0, value);
            catalog.RunArchivePass();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    Write(*table, 0, 0, 33U);
    txn_test::ScopedEnv crash(point.c_str(), "1");
    catalog.RunArchivePass();
    return 2;
}

void CrashCase(const char* executable, const char* step) {
    test::ScopedTempDir temporary("chunkdb-wal-archive-crash");
    const auto root = std::filesystem::canonical(temporary.path());
    const std::string point = "CHUNKDB_FAILPOINT_CRASH_ARCHIVE_" + std::string(step) + "_ONCE";
    const bool retention = point.find("RETENTION") != std::string::npos;
    assert(Child(executable, root, point) == 86);
    const auto directory = ReadDataDirManifest(root / "data")->data_dir_id;
    StoreId epoch{};
    std::uint64_t first = 0U, second = 0U;
    {
        TableCatalog catalog(ArchiveConfig(root, retention));
        AwaitFirstPass(catalog);
        const auto table = catalog.Find("default");
        assert(table);
        epoch = table->store_id();
        Write(*table, 0, 0, 44U);
        catalog.RunArchivePass();
        first = Version(*table, 0, 0);
        second = Version(*table, 1, 0);
        assert(!catalog.ArchiveStatus()->error);
    }
    // The start finished any staged record; none is left behind.
    const auto staged_root = root / "data" / std::string(kWalArchiveStagingName) / WalArchiveDirectoryDirName(directory);
    if (std::filesystem::exists(staged_root))
        for (const auto& entry : std::filesystem::directory_iterator(staged_root))
            assert(!ParseWalArchiveRecordName(entry.path().filename().string()));
    const auto records = Records(root / "archive", directory);
    assert(!records.empty());
    // One chain: every record names the one before it; retention may have
    // removed the first records, never one in the middle.
    for (std::size_t i = 1U; i < records.size(); ++i) assert(records[i].predecessor_ms == records[i - 1U].captured_ms);
    assert(retention || records.front().predecessor_ms == 0U);
    // Every listed file is in the archive with its length and CRC, and the
    // segments of each chunk reach its last acknowledged version.
    const auto files = root / "archive" / WalArchiveTableDirName(epoch);
    std::uint64_t last_00 = 0U, last_10 = 0U;
    for (const auto& record : records) {
        for (const auto& table : record.tables) {
            if (table.epoch != epoch) continue;
            for (const auto& file : table.files) {
                const auto bytes = LoadFile(files / file.name);
                assert(bytes.size() == file.size && Crc32(bytes) == file.crc32);
                if (const auto last = WalArchiveSegmentLast(file.name)) {
                    if (file.name.starts_with("C_0_0.")) last_00 = std::max(last_00, *last);
                    if (file.name.starts_with("C_1_0.")) last_10 = std::max(last_10, *last);
                }
            }
        }
    }
    // Retention removes the only segment of chunk (1, 0) once a later record seals past it.
    assert(last_00 == first && (retention || last_10 == second));
    const auto* newest = &records.back().tables.front();
    for (const auto& table : records.back().tables) if (table.epoch == epoch) newest = &table;
    assert(newest->epoch == epoch && newest->revision && *newest->revision >= first);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::string_view(argv[1]) == "--child") return RunChild(argv[2], argv[3]);
    const char* steps[] = {"AFTER_STAGE", "AFTER_SHIP", "AFTER_RECORD_STAGE", "AFTER_RECORD_SHIP", "AFTER_CHAIN",
        "RETENTION_AFTER_RECORD"};
    for (const auto* step : steps) CrashCase(argv[0], step);
    std::cout << "wal archive crash passed: 6 steps\n";
    return 0;
}
