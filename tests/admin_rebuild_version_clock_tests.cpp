// chunkdb_admin rebuild-version-clock (#149): a data directory whose clock
// files are lost gets clocks above every revision on disk, offline.

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "admin.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/table_catalog.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"

namespace {

using chunkdb::CatalogConfig;
using chunkdb::GeometryConfig;
using chunkdb::TableCatalog;

constexpr GeometryConfig kGeometry{
    .large_chunk_width_chunks = 4,
    .large_chunk_height_chunks = 4,
    .chunk_width_blocks = 8,
    .chunk_height_blocks = 8,
    .block_bits = 4,
};

CatalogConfig Config(const std::filesystem::path& data_dir) {
    CatalogConfig config;
    config.data_dir = data_dir;
    return config;
}

bool Contains(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

template <typename Fn>
std::string ErrorOf(Fn&& fn) {
    try {
        fn();
    } catch (const std::exception& e) {
        return e.what();
    }
    return {};
}

std::vector<std::uint8_t> ReadBytes(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void WriteBytes(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(out.good());
}

std::uint64_t Ceiling(const std::filesystem::path& table_dir) {
    std::uint64_t ceiling = 0;
    assert(chunkdb::TryParseVersionClockRecord(ReadBytes(table_dir / "chunkdb.version"), &ceiling));
    return ceiling;
}

void WriteBits(TableCatalog& catalog, const std::string& table, std::int64_t x, std::int64_t y, const std::string& bits) {
    auto lease = catalog.Find(table)->Acquire();
    assert(lease.has_value());
    lease->store().SetBlockBits(x, y, bits);
}

std::uint64_t ChunkVersion(TableCatalog& catalog, const std::string& table, std::int64_t cx, std::int64_t cy) {
    auto lease = catalog.Find(table)->Acquire();
    assert(lease.has_value());
    return lease->store().GetChunkVersion(cx, cy);
}

// Writes `count` updates to two chunks of `table`, checkpointing every second
// update, and returns the highest chunk version.
std::uint64_t Fill(TableCatalog& catalog, const std::string& table, int count) {
    chunkdb::TableOptions options;
    options.checkpoint_update_interval = 2;
    (void)catalog.Create(table, kGeometry, options);
    for (int i = 0; i < count; ++i) {
        WriteBits(catalog, table, 0, 0, (i % 2) == 0 ? "0001" : "0010");
        WriteBits(catalog, table, 100, 100, (i % 2) == 0 ? "0011" : "0100");
    }
    return std::max(ChunkVersion(catalog, table, 0, 0), ChunkVersion(catalog, table, 12, 12));
}

bool HasFileWithExtension(const std::filesystem::path& dir, const std::string& extension) {
    for (const auto& entry : std::filesystem::recursive_directory_iterator(dir)) {
        if (entry.is_regular_file() && entry.path().extension() == extension) return true;
    }
    return false;
}

// Rebuilds a deleted clock, a malformed clock and a deleted initialized
// marker; the reopened tables then issue revisions above every old one.
void TestRebuildAfterLostClock() {
    chunkdb::test::ScopedTempDir dir("chunkdb-admin-rebuild-clock");
    const auto world = dir.path() / "tables" / "world";
    const auto other = dir.path() / "tables" / "other";
    std::uint64_t world_highest = 0;
    std::uint64_t other_highest = 0;
    {
        TableCatalog catalog(Config(dir.path()));
        world_highest = Fill(catalog, "world", 7);
        other_highest = Fill(catalog, "other", 1);
    }
    assert(HasFileWithExtension(world, ".chk") && HasFileWithExtension(world, ".wal"));
    std::filesystem::remove(world / "chunkdb.version");
    WriteBytes(other / "chunkdb.version", {'n', 'o', 't', ' ', 'a', ' ', 'c', 'l', 'o', 'c', 'k'});
    std::filesystem::remove(other / ".chunkdb.initialized");
    const auto refused = ErrorOf([&] {
        TableCatalog catalog(Config(dir.path()));
        WriteBits(catalog, "world", 0, 0, "0101");
    });
    assert(Contains(refused, "rebuild-version-clock"));

    std::ostringstream findings;
    const auto rebuilt = chunkdb::RebuildVersionClock(dir.path(), findings);
    assert(rebuilt.size() == 2U);
    assert(rebuilt[0].table == "other" && rebuilt[1].table == "world");
    for (const auto& table : rebuilt) {
        assert(table.rewritten && !table.previous_ceiling.has_value());
        assert(table.ceiling == table.highest_revision + chunkdb::kRebuiltVersionClockMargin);
    }
    assert(rebuilt[0].highest_revision >= other_highest && rebuilt[1].highest_revision >= world_highest);
    assert(Ceiling(world) == rebuilt[1].ceiling && Ceiling(other) == rebuilt[0].ceiling);
    assert(chunkdb::IsValidInitializedStoreMarker(ReadBytes(other / ".chunkdb.initialized")));

    {
        TableCatalog catalog(Config(dir.path()));
        assert(ChunkVersion(catalog, "world", 0, 0) <= world_highest);
        WriteBits(catalog, "world", 0, 0, "0101");
        WriteBits(catalog, "other", 3, 3, "0110");
        assert(ChunkVersion(catalog, "world", 0, 0) >= rebuilt[1].ceiling);
        assert(ChunkVersion(catalog, "other", 0, 0) >= rebuilt[0].ceiling);
    }
    // Valid clocks above everything on disk are kept as they are.
    const auto kept_bytes = ReadBytes(world / "chunkdb.version");
    std::ostringstream again_findings;
    for (const auto& table : chunkdb::RebuildVersionClock(dir.path(), again_findings)) {
        assert(!table.rewritten && table.previous_ceiling.has_value() && table.ceiling == *table.previous_ceiling);
    }
    assert(ReadBytes(world / "chunkdb.version") == kept_bytes);
}

// A clock above every revision is never lowered; one at or below a revision
// on disk is raised above it and above the directory's version floor.
void TestNeverLowers() {
    chunkdb::test::ScopedTempDir dir("chunkdb-admin-rebuild-never-lower");
    const auto world = dir.path() / "tables" / "world";
    {
        TableCatalog catalog(Config(dir.path()));
        (void)Fill(catalog, "world", 3);
        // A dropped table raises the directory's version floor.
        (void)Fill(catalog, "gone", 3);
        catalog.Drop("gone");
    }
    const auto root = chunkdb::ReadDataDirManifest(dir.path());
    assert(root.has_value());
    const auto floor = chunkdb::DataDirVersionFloor(*root);
    assert(floor > 0U);

    const std::uint64_t high = std::uint64_t{1} << 60U;
    WriteBytes(world / "chunkdb.version", chunkdb::SerializeVersionClockRecord(high));
    std::ostringstream findings;
    auto rebuilt = chunkdb::RebuildVersionClock(dir.path(), findings);
    assert(rebuilt.size() == 1U && !rebuilt[0].rewritten && rebuilt[0].ceiling == high && Ceiling(world) == high);

    WriteBytes(world / "chunkdb.version", chunkdb::SerializeVersionClockRecord(1U));
    rebuilt = chunkdb::RebuildVersionClock(dir.path(), findings);
    assert(rebuilt.size() == 1U && rebuilt[0].rewritten && rebuilt[0].previous_ceiling == 1U);
    assert(rebuilt[0].highest_revision > 1U);
    assert(rebuilt[0].ceiling == std::max(rebuilt[0].highest_revision, floor) + chunkdb::kRebuiltVersionClockMargin);
    assert(Ceiling(world) == rebuilt[0].ceiling);
}

// A running writer, damage the rebuild cannot read, and a directory that is
// not a data directory are refused before any file changes.
void TestRefusals() {
    chunkdb::test::ScopedTempDir dir("chunkdb-admin-rebuild-refusals");
    const auto world = dir.path() / "tables" / "world";
    {
        TableCatalog catalog(Config(dir.path()));
        (void)Fill(catalog, "world", 4);
        const auto clock = ReadBytes(world / "chunkdb.version");
        std::ostringstream findings;
        assert(!ErrorOf([&] { (void)chunkdb::RebuildVersionClock(dir.path(), findings); }).empty());
        assert(ReadBytes(world / "chunkdb.version") == clock);
    }

    std::filesystem::remove(world / "chunkdb.version");
    std::filesystem::path wal;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(world)) {
        if (entry.path().extension() == ".wal") wal = entry.path();
    }
    assert(!wal.empty());
    auto bytes = ReadBytes(wal);
    assert(!bytes.empty());
    bytes[0] = static_cast<std::uint8_t>(bytes[0] ^ 0xFFU);
    WriteBytes(wal, bytes);
    std::ostringstream findings;
    const auto error = ErrorOf([&] { (void)chunkdb::RebuildVersionClock(dir.path(), findings); });
    assert(Contains(error, "errors other than a lost clock") && Contains(error, "no clock was changed"));
    assert(Contains(findings.str(), "VERIFY error"));
    assert(!std::filesystem::exists(world / "chunkdb.version"));

    chunkdb::test::ScopedTempDir empty("chunkdb-admin-rebuild-empty");
    assert(Contains(ErrorOf([&] { (void)chunkdb::RebuildVersionClock(empty.path(), findings); }),
        "is not a chunkdb data directory"));
    assert(std::filesystem::is_empty(empty.path()));
}

int ExitCode(int status) {
#ifdef _WIN32
    return status;
#else
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
#endif
}

int Run(const std::string& executable, const std::string& args, const std::filesystem::path& output_path,
    std::string* output) {
    std::string command = "\"" + executable + "\" " + args + " > \"" + output_path.string() + "\" 2>&1";
#ifdef _WIN32
    // cmd strips the outermost quote pair of the whole line.
    command = "\"" + command + "\"";
#endif
    const int status = std::system(command.c_str());
    const auto bytes = ReadBytes(output_path);
    output->assign(bytes.begin(), bytes.end());
    return ExitCode(status);
}

// The command line runs the rebuild and reports each table.
void TestCommandLine(const std::string& admin) {
    chunkdb::test::ScopedTempDir dir("chunkdb-admin-rebuild-cli");
    const auto data_dir = dir.path() / "data";
    {
        TableCatalog catalog(Config(data_dir));
        (void)Fill(catalog, "world", 2);
    }
    std::filesystem::remove(data_dir / "tables" / "world" / "chunkdb.version");
    const auto log = dir.path() / "admin.log";
    std::string output;
    assert(Run(admin, "rebuild-version-clock", log, &output) == 2 && Contains(output, "rebuild-version-clock"));
    assert(Run(admin, "--data-dir \"" + data_dir.string() + "\" rebuild-version-clock", log, &output) == 0);
    assert(Contains(output, "table world: rebuilt, next revision "));
    assert(Run(admin, "--data-dir \"" + data_dir.string() + "\" rebuild-version-clock", log, &output) == 0);
    assert(Contains(output, "table world: kept, next revision "));
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <chunkdb_admin>\n";
        return 2;
    }
    TestRebuildAfterLostClock();
    TestNeverLowers();
    TestRefusals();
    TestCommandLine(argv[1]);
    std::cout << "admin rebuild-version-clock tests passed\n";
    return 0;
}
