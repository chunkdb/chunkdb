// chunkdb_admin rebuild-version-clock (#149): a data directory whose clock
// files are lost gets clocks above every revision on disk, offline.

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
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
#include "feed_slot_records.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "txn_history.hpp"

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

void SetEnvVar(const char* key, const char* value) {
#ifdef _WIN32
    (void)_putenv_s(key, value);
#else
    (void)setenv(key, value, 1);
#endif
}

void UnsetEnvVar(const char* key) {
#ifdef _WIN32
    (void)_putenv_s(key, "");
#else
    (void)unsetenv(key);
#endif
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
    assert(rebuilt[0].marker_rewritten && !rebuilt[1].marker_rewritten);
    assert(rebuilt[0].highest_revision == other_highest && rebuilt[1].highest_revision == world_highest);
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
        assert(!table.rewritten && !table.marker_rewritten && table.previous_ceiling.has_value() &&
            table.ceiling == *table.previous_ceiling);
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
        assert(Contains(ErrorOf([&] { (void)chunkdb::RebuildVersionClock(dir.path(), findings); }), "already has an active writer"));
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

void RewriteTableManifest(const std::filesystem::path& table_dir, const std::function<void(chunkdb::StoreManifest&)>& edit) {
    const auto path = chunkdb::StoreManifestPath(table_dir);
    auto manifest = chunkdb::ParseStoreManifest(ReadBytes(path));
    edit(manifest);
    WriteBytes(path, chunkdb::SerializeStoreManifest(manifest));
}

// A revision held only by the restore lineage is the table's highest one:
// the survey reads it exactly, below the margin that would hide a miss.
void TestLineageRevision() {
    chunkdb::test::ScopedTempDir dir("chunkdb-admin-rebuild-lineage");
    const auto world = dir.path() / "tables" / "world";
    std::uint64_t highest = 0;
    {
        TableCatalog catalog(Config(dir.path()));
        highest = Fill(catalog, "world", 3);
    }
    const std::uint64_t fork = highest + 1000U;
    RewriteTableManifest(world, [&](chunkdb::StoreManifest& manifest) {
        manifest.features.incompat |= chunkdb::kFeatureTableLineage;
        manifest.lineage = chunkdb::TableLineage{.parent_epoch = chunkdb::NewStoreId(), .fork_revision = fork};
    });
    std::filesystem::remove(world / "chunkdb.version");
    std::ostringstream findings;
    const auto rebuilt = chunkdb::RebuildVersionClock(dir.path(), findings);
    assert(rebuilt.size() == 1U && rebuilt[0].rewritten && !rebuilt[0].marker_rewritten);
    assert(rebuilt[0].highest_revision == fork);
    assert(rebuilt[0].ceiling == fork + chunkdb::kRebuiltVersionClockMargin);
}

// A revision held only by the slot records' durable watermark, or only by a
// transaction intent, is the table's highest one.
void TestSlotAndIntentRevisions() {
    chunkdb::test::ScopedTempDir dir("chunkdb-admin-rebuild-slots-intents");
    const auto world = dir.path() / "tables" / "world";
    std::uint64_t highest = 0;
    {
        TableCatalog catalog(Config(dir.path()));
        highest = Fill(catalog, "world", 3);
    }
    const auto manifest = chunkdb::ParseStoreManifest(ReadBytes(chunkdb::StoreManifestPath(world)));
    const std::uint64_t watermark = highest + 500U;
    RewriteTableManifest(world, [](chunkdb::StoreManifest& edited) {
        edited.features.incompat |= chunkdb::kFeatureFeedSlots;
    });
    chunkdb::WriteFeedSlotRecords(world, chunkdb::FeedSlotRecords{
        .epoch = manifest.store_id,
        .durable_watermark = watermark,
        .slots = {chunkdb::FeedSlotRecord{.name = "reader", .written = highest}},
        .filter_format = false,
        .dims = manifest.geometry.dims,
    });
    std::filesystem::remove(world / "chunkdb.version");
    std::ostringstream findings;
    auto rebuilt = chunkdb::RebuildVersionClock(dir.path(), findings);
    assert(rebuilt.size() == 1U && rebuilt[0].rewritten);
    assert(rebuilt[0].highest_revision == watermark);

    const std::uint64_t intent_version = watermark + 700U;
    const auto intent_path = chunkdb::TxnIntentPath(world, intent_version);
    std::filesystem::create_directories(intent_path.parent_path());
    WriteBytes(intent_path, chunkdb::SerializeTxnIntent(chunkdb::TxnIntent{
        .state = chunkdb::TxnIntentState::kCommitted,
        .version = intent_version,
        .entries = {chunkdb::TxnIntentEntry{.coord = chunkdb::ChunkCoord{}, .wal_boundary = 0U}},
    }, manifest.geometry.dims, manifest.store_id));
    std::filesystem::remove(world / "chunkdb.version");
    rebuilt = chunkdb::RebuildVersionClock(dir.path(), findings);
    assert(rebuilt.size() == 1U && rebuilt[0].rewritten);
    assert(rebuilt[0].highest_revision == intent_version);
}

// Like a read-write open, the rebuild refuses a table or directory with a
// feature this build can only read, whose records may hold revisions.
void TestReadOnlyFeatureRefused() {
    constexpr std::uint32_t kUnknownRoCompat = 1U << 3U;
    chunkdb::test::ScopedTempDir dir("chunkdb-admin-rebuild-features");
    const auto world = dir.path() / "tables" / "world";
    {
        TableCatalog catalog(Config(dir.path()));
        (void)Fill(catalog, "world", 2);
    }
    std::filesystem::remove(world / "chunkdb.version");
    RewriteTableManifest(world, [](chunkdb::StoreManifest& manifest) { manifest.features.ro_compat |= kUnknownRoCompat; });
    std::ostringstream findings;
    auto error = ErrorOf([&] { (void)chunkdb::RebuildVersionClock(dir.path(), findings); });
    assert(Contains(error, "can only be opened read-only"));
    assert(!std::filesystem::exists(world / "chunkdb.version"));
    RewriteTableManifest(world, [](chunkdb::StoreManifest& manifest) { manifest.features.ro_compat &= ~kUnknownRoCompat; });

    const auto root_path = chunkdb::DataDirManifestPath(dir.path());
    auto root = chunkdb::ParseDataDirManifest(ReadBytes(root_path));
    root.features.ro_compat |= kUnknownRoCompat;
    WriteBytes(root_path, chunkdb::SerializeDataDirManifest(root));
    error = ErrorOf([&] { (void)chunkdb::RebuildVersionClock(dir.path(), findings); });
    assert(Contains(error, "can only be opened read-only"));
    assert(!std::filesystem::exists(world / "chunkdb.version"));
}

// The server names the rebuild only for a clock the rebuild replaces, not
// for one it cannot read.
void TestServerAdvice() {
    chunkdb::test::ScopedTempDir dir("chunkdb-admin-rebuild-advice");
    const auto world = dir.path() / "tables" / "world";
    {
        TableCatalog catalog(Config(dir.path()));
        (void)Fill(catalog, "world", 2);
    }
    const auto clock = ReadBytes(world / "chunkdb.version");
    WriteBytes(world / "chunkdb.version", std::vector<std::uint8_t>(clock.begin(), clock.end() - 1));
    const auto open = [&] {
        TableCatalog catalog(Config(dir.path()));
        WriteBits(catalog, "world", 0, 0, "0101");
    };
    auto refused = ErrorOf(open);
    assert(Contains(refused, "record size is 15 bytes") && Contains(refused, "rebuild-version-clock"));

    WriteBytes(world / "chunkdb.version", clock);
    SetEnvVar("CHUNKDB_FAILPOINT_VERSION_READ_FAIL_ONCE", "1");
    refused = ErrorOf(open);
    UnsetEnvVar("CHUNKDB_FAILPOINT_VERSION_READ_FAIL_ONCE");
    assert(Contains(refused, "injected version bookkeeping read failure"));
    assert(!Contains(refused, "rebuild-version-clock"));
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
    assert(!Contains(output, "marker rewritten"));
    // A malformed marker beside a kept clock is replaced; the clock is not.
    const auto world = data_dir / "tables" / "world";
    const auto clock = ReadBytes(world / "chunkdb.version");
    WriteBytes(world / ".chunkdb.initialized", {'b', 'a', 'd'});
    assert(Run(admin, "--data-dir \"" + data_dir.string() + "\" rebuild-version-clock", log, &output) == 0);
    assert(Contains(output, "table world: kept, next revision ") && Contains(output, ", marker rewritten"));
    assert(ReadBytes(world / "chunkdb.version") == clock);
    assert(chunkdb::IsValidInitializedStoreMarker(ReadBytes(world / ".chunkdb.initialized")));
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
    TestLineageRevision();
    TestSlotAndIntentRevisions();
    TestReadOnlyFeatureRefused();
    TestServerAdvice();
    TestCommandLine(argv[1]);
    std::cout << "admin rebuild-version-clock tests passed\n";
    return 0;
}
