// chunkdb_migrate: conversion of data directories written by real old servers
// (tests/fixtures/migrate, made by scripts/test/make_migrate_fixtures.py),
// and the refusals for damaged or unconvertible sources.
//
// Usage: chunkdb_migrate_test <tests/fixtures/migrate>

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "chunkdb/crc32.hpp"
#include "chunkdb/table_catalog.hpp"
#include "legacy_format.hpp"
#include "migrate.hpp"
#include "process_lock.hpp"
#include "test_utils.hpp"

namespace {

namespace fs = std::filesystem;
using chunkdb::migrate::MigrateOptions;
using chunkdb::migrate::MigrateRefused;
using chunkdb::migrate::MigrateSummary;

fs::path g_fixtures;

// The geometry make_migrate_fixtures.py runs the old servers with.
constexpr chunkdb::GeometryConfig kGeometry{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 4,
    .chunk_height_blocks = 4,
    .block_bits = 4,
};

struct Expected {
    std::string payload;   // bit text
    std::string presence;  // bit text
    std::string revision;  // a number, or "-" for none
};

std::map<std::pair<std::int64_t, std::int64_t>, Expected> ReadExpected(const std::string& name) {
    std::ifstream in(g_fixtures / (name + ".expected"));
    assert(in.good());
    std::map<std::pair<std::int64_t, std::int64_t>, Expected> expected;
    std::int64_t cx = 0;
    std::int64_t cy = 0;
    std::string state;
    std::string revision;
    while (in >> cx >> cy >> state >> revision) {
        const auto bar = state.find('|');
        expected[{cx, cy}] = Expected{state.substr(0, bar), state.substr(bar + 1), revision};
    }
    assert(!expected.empty());
    return expected;
}

// The old server's text with the payload of absent blocks as zero, which is
// how chunkdb 2.0 stores and returns it.
std::string Canonical(const Expected& e) {
    std::string payload = e.payload;
    const std::size_t bits = static_cast<std::size_t>(kGeometry.block_bits);
    for (std::size_t block = 0; block < e.presence.size(); ++block) {
        if (e.presence[block] == '0') {
            payload.replace(block * bits, bits, std::string(bits, '0'));
        }
    }
    return payload + "|" + e.presence;
}

std::string BitText(const std::vector<std::uint8_t>& bytes, std::size_t count) {
    std::string text;
    for (std::size_t i = 0; i < count; ++i) {
        text.push_back(((bytes[i / 8] >> (i % 8)) & 1U) != 0U ? '1' : '0');
    }
    return text;
}

// Every file under `root` with its bytes: a source must stay byte for byte
// the same.
std::map<std::string, std::string> Tree(const fs::path& root) {
    std::map<std::string, std::string> tree;
    for (const auto& entry : fs::recursive_directory_iterator(root)) {
        const auto relative = entry.path().lexically_relative(root).generic_string();
        if (entry.is_directory()) {
            tree[relative + "/"] = "";
            continue;
        }
        std::ifstream in(entry.path(), std::ios::binary);
        std::ostringstream bytes;
        bytes << in.rdbuf();
        tree[relative] = bytes.str();
    }
    return tree;
}

fs::path CopyFixture(const std::string& name, const fs::path& into) {
    const auto copy = into / name;
    fs::create_directories(into);
    fs::copy(g_fixtures / name, copy, fs::copy_options::recursive);
    return copy;
}

MigrateOptions Options(const fs::path& from, const fs::path& to) {
    MigrateOptions options;
    options.from = from;
    options.to = to;
    options.geometry = kGeometry;
    return options;
}

// Runs a conversion that must be refused; returns all problems as one text.
std::string Refusal(const MigrateOptions& options) {
    // A refused conversion leaves the destination as it found it.
    const bool existed = fs::exists(options.to);
    const auto before = existed ? Tree(options.to) : std::map<std::string, std::string>{};
    try {
        (void)chunkdb::migrate::Migrate(options);
    } catch (const MigrateRefused& e) {
        std::string text = e.what();
        for (const auto& problem : e.problems()) {
            text += "\n" + problem;
        }
        assert(fs::exists(options.to) == existed);
        assert(!existed || Tree(options.to) == before);
        return text;
    }
    assert(false && "expected the conversion to be refused");
    return {};
}

bool Contains(const std::string& text, const std::string& part) {
    if (text.find(part) != std::string::npos) {
        return true;
    }
    std::cerr << "missing '" << part << "' in:\n" << text << "\n";
    return false;
}

// Reads the converted `default` table and compares it with what the old
// server read; returns the store's revisions.
void CheckConverted(
    const fs::path& to,
    const std::string& fixture,
    const MigrateSummary& summary) {
    const auto expected = ReadExpected(fixture);
    chunkdb::CatalogConfig config;
    config.data_dir = to;
    config.access_mode = chunkdb::AccessMode::kReadOnly;
        config.default_geometry_fields = 0;
    chunkdb::TableCatalog catalog(config);
    assert(catalog.TableCount() == 1U);
    const auto table = catalog.Find("default");
    assert(table->geometry().block_bits == kGeometry.block_bits &&
           table->geometry().large_chunk_width_chunks == kGeometry.large_chunk_width_chunks);
    auto lease = table->Acquire();
    auto& store = lease->store();
    const chunkdb::Geometry geometry(kGeometry);

    const auto page = store.ScanPopulatedChunks(false, {}, 1024);
    assert(page.coords.size() == expected.size());
    assert(summary.chunks == expected.size());
    for (const auto& [coord, want] : expected) {
        const auto state = store.GetChunkStateBytes(coord.first, coord.second);
        const std::vector<std::uint8_t> payload(
            state.begin(), state.begin() + static_cast<std::ptrdiff_t>(geometry.ChunkPayloadBytes()));
        const std::vector<std::uint8_t> presence(
            state.begin() + static_cast<std::ptrdiff_t>(geometry.ChunkPayloadBytes()), state.end());
        const auto got = BitText(payload, geometry.ChunkPayloadBits()) + "|" +
                         BitText(presence, geometry.ChunkBlockCount());
        if (got != Canonical(want)) {
            std::cerr << fixture << " chunk " << coord.first << " " << coord.second << ": got "
                      << got << ", want " << Canonical(want) << "\n";
            assert(false);
        }
        const std::uint64_t revision = store.GetChunkVersion(coord.first, coord.second);
        if (want.revision == "-") {
            // Fresh, and above every token the old server issued.
            assert(revision >= summary.source_clock_ceiling);
        } else {
            assert(revision == std::stoull(want.revision));
        }
    }
}

void TestConvertsV1xDirectory() {
    chunkdb::test::ScopedTempDir dir("chunkdb-migrate-v1x");
    const auto source = CopyFixture("v1x", dir.path());
    const auto before = Tree(source);
    const auto to = dir.path() / "converted";
    const auto summary = chunkdb::migrate::Migrate(Options(source, to));
    assert(Tree(source) == before);

    CheckConverted(to, "v1x", summary);
    assert(summary.images_by_version[1] == 1U && summary.images_by_version[2] == 1U &&
           summary.images_by_version[3] == 1U);
    assert(summary.wals_v2 >= 1U && summary.wals_v3 >= 1U && summary.wals_v4 == 0U);
    // The record appended half-way by make_migrate_fixtures.py.
    assert(summary.torn_tails == 1U);
    assert(summary.losses.empty());
    assert(summary.persisted_revisions == 0U && summary.assigned_revisions == summary.chunks);
    assert(summary.verify.errors == 0U && summary.verify.warnings == 0U);
    // The source had no writer lock file; that is noted, not an error.
    bool lock_note = false;
    for (const auto& note : summary.notes) {
        lock_note = lock_note || note.find("has no writer lock file") != std::string::npos;
    }
    assert(lock_note);
}

void TestConvertsDevelopmentFormat() {
    chunkdb::test::ScopedTempDir dir("chunkdb-migrate-dev");
    const auto source = CopyFixture("dev", dir.path());
    const auto before = Tree(source);
    const auto to = dir.path() / "converted";
    const auto summary = chunkdb::migrate::Migrate(Options(source, to));
    assert(Tree(source) == before);

    CheckConverted(to, "dev", summary);
    for (int version = 1; version <= 5; ++version) {
        assert(summary.images_by_version[static_cast<std::size_t>(version)] == 1U);
    }
    // 1.x records continued by v4 frames after a mid-stream header.
    assert(summary.wals_mixed >= 1U && summary.wals_v4 >= 1U);
    // The crash inside CHUNKBATCH left a rollback intent; its write is not
    // in the result (the .expected file is what the old server read).
    assert(summary.rollback_intents == 1U);
    assert(summary.persisted_revisions == 4U);
    assert(summary.losses.empty());
}

// The engine refuses an unconverted directory without changing it.
void TestEngineRefusesUnconvertedSource() {
    chunkdb::test::ScopedTempDir dir("chunkdb-migrate-engine");
    for (const char* name : {"v1x", "dev"}) {
        const auto source = CopyFixture(name, dir.path());
        const auto before = Tree(source);
        chunkdb::CatalogConfig config;
        config.data_dir = source;
        config.default_geometry = kGeometry;
        bool refused = false;
        try {
            chunkdb::TableCatalog catalog(config);
        } catch (const std::exception& e) {
            refused = Contains(e.what(), "chunkdb_migrate");
        }
        assert(refused);
        assert(Tree(source) == before);
    }
}

void Overwrite(const fs::path& path, std::size_t offset, std::uint8_t value) {
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    file.seekp(static_cast<std::streamoff>(offset));
    file.put(static_cast<char>(value));
    assert(file.good());
}

void TestDamageIsRefusedUnlessAccepted() {
    chunkdb::test::ScopedTempDir dir("chunkdb-migrate-damage");
    {
        // A WAL with a damaged header: the old server dropped it whole.
        const auto source = CopyFixture("v1x", dir.path() / "wal-header");
        Overwrite(source / "L_0_0" / "C_0_0.wal", 8, 9);  // version 9
        const auto to = dir.path() / "wal-header-out";
        assert(Contains(Refusal(Options(source, to)), "C_0_0.wal: not replayable (invalid_header)"));
        auto options = Options(source, to);
        options.accept_loss = true;
        const auto summary = chunkdb::migrate::Migrate(options);
        assert(summary.losses.size() == 1U);
    }
    {
        // A damaged record followed by valid ones: the old server stopped
        // there and lost the rest.
        const auto source = CopyFixture("v1x", dir.path() / "wal-middle");
        const auto wal = source / "L_2_-3" / "C_4_-5.wal";
        Overwrite(wal, 36U + 14U, 0xFF);  // the first record's body
        const auto to = dir.path() / "wal-middle-out";
        assert(Contains(Refusal(Options(source, to)), "C_4_-5.wal: replay stopped at byte 36"));
        auto options = Options(source, to);
        options.accept_loss = true;
        const auto summary = chunkdb::migrate::Migrate(options);
        assert(summary.losses.size() == 1U);
        // Nothing of that WAL applied, and it had no image: the chunk is gone.
        chunkdb::CatalogConfig config;
        config.data_dir = to;
        config.access_mode = chunkdb::AccessMode::kReadOnly;
        config.default_geometry_fields = 0;
        chunkdb::TableCatalog catalog(config);
        auto lease = catalog.Find("default")->Acquire();
        assert(!lease->store().ChunkExists(4, -5));
    }
    {
        // An unreadable image: the old server could not load the chunk.
        const auto source = CopyFixture("v1x", dir.path() / "image");
        const auto image = source / "L_-1_-1" / "C_-1_-1.chk";
        Overwrite(image, fs::file_size(image) - 1U, 0xEE);
        const auto to = dir.path() / "image-out";
        assert(Contains(Refusal(Options(source, to)), "C_-1_-1.chk: unreadable image"));
        auto options = Options(source, to);
        options.accept_loss = true;
        const auto summary = chunkdb::migrate::Migrate(options);
        assert(summary.losses.size() == 1U);
        assert(summary.chunks == ReadExpected("v1x").size() - 1U);
    }
}

void TestWrongGeometryIsRefused() {
    chunkdb::test::ScopedTempDir dir("chunkdb-migrate-geometry");
    const auto source = CopyFixture("v1x", dir.path());
    auto options = Options(source, dir.path() / "out");
    options.geometry.block_bits = 5;
    assert(Contains(Refusal(options), "written with another geometry than the given one"));
    options = Options(source, dir.path() / "out");
    options.geometry.large_chunk_width_chunks = 8;
    options.geometry.large_chunk_height_chunks = 8;
    assert(Contains(Refusal(options), "belongs in L_0_0 with the given --large-chunk-width"));
}

std::vector<std::uint8_t> IntentRecord(const char* magic, std::uint64_t boundary) {
    std::vector<std::uint8_t> bytes(magic, magic + 4);
    for (int i = 0; i < 8; ++i) {
        bytes.push_back(static_cast<std::uint8_t>(boundary >> (8 * i)));
    }
    const std::uint32_t crc = chunkdb::Crc32(bytes.data(), bytes.size());
    for (int i = 0; i < 4; ++i) {
        bytes.push_back(static_cast<std::uint8_t>(crc >> (8 * i)));
    }
    return bytes;
}

void WriteBytes(const fs::path& path, const std::vector<std::uint8_t>& bytes) {
    fs::create_directories(path.parent_path());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(out.good());
}

void TestIntents() {
    chunkdb::test::ScopedTempDir dir("chunkdb-migrate-intents");
    const auto intent_dir = fs::path(".chunkdb.intents");
    {
        // A boundary past the end of the WAL: the old server refused to start.
        const auto source = CopyFixture("v1x", dir.path() / "short");
        WriteBytes(source / intent_dir / "L_0_0__C_0_0.wal.rollback", IntentRecord("CKRB", 100000));
        assert(Contains(Refusal(Options(source, dir.path() / "short-out")),
                        "shorter than its rollback intent's boundary"));
    }
    {
        const auto source = CopyFixture("v1x", dir.path() / "damaged");
        auto bytes = IntentRecord("CKRB", 36);
        bytes[13] ^= 0x01U;
        WriteBytes(source / intent_dir / "L_0_0__C_0_0.wal.rollback", bytes);
        assert(Contains(Refusal(Options(source, dir.path() / "damaged-out")),
                        "damaged conditional intent"));
    }
    {
        // A rollback to the bare header drops every record; a committed
        // intent keeps the WAL whole.
        const auto source = CopyFixture("v1x", dir.path() / "applied");
        WriteBytes(source / intent_dir / "L_0_0__C_0_0.wal.rollback", IntentRecord("CKRB", 36));
        WriteBytes(source / intent_dir / "L_0_0__C_1_0.wal.rollback", IntentRecord("CKRC", 0));
        const auto summary = chunkdb::migrate::Migrate(Options(source, dir.path() / "applied-out"));
        assert(summary.rollback_intents == 1U && summary.committed_intents == 1U);
        chunkdb::CatalogConfig config;
        config.data_dir = dir.path() / "applied-out";
        config.access_mode = chunkdb::AccessMode::kReadOnly;
        config.default_geometry_fields = 0;
        chunkdb::TableCatalog catalog(config);
        auto lease = catalog.Find("default")->Acquire();
        // Chunk (0,0) is its v1 image alone.
        const chunkdb::Geometry geometry(kGeometry);
        std::ifstream in(source / "L_0_0" / "C_0_0.chk", std::ios::binary);
        const std::vector<std::uint8_t> bytes(
            (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        const auto image = chunkdb::legacy::ParseLegacyChunkImage(bytes, geometry, chunkdb::ChunkCoord{0, 0});
        auto want = image.payload;
        want.insert(want.end(), image.presence_bitmap.begin(), image.presence_bitmap.end());
        assert(lease->store().GetChunkStateBytes(0, 0) == want);
        assert(BitText(want, 64) != Canonical(ReadExpected("v1x").at({0, 0})).substr(0, 64));
        const auto expected = ReadExpected("v1x");
        const auto c10 = lease->store().GetChunkStateBytes(1, 0);
        assert(BitText(c10, 64) == Canonical(expected.at({1, 0})).substr(0, 64));
    }
}

void TestDestinationAndSourceChecks() {
    chunkdb::test::ScopedTempDir dir("chunkdb-migrate-paths");
    const auto source = CopyFixture("v1x", dir.path());
    {
        const auto to = dir.path() / "occupied";
        fs::create_directories(to);
        WriteBytes(to / "file", {1});
        assert(Contains(Refusal(Options(source, to)), "exists and is not an empty directory"));
        assert(fs::exists(to / "file"));
    }
    assert(Contains(Refusal(Options(source, source / "inside")), "outside the source"));
    {
        // An empty destination directory is fine.
        const auto to = dir.path() / "empty";
        fs::create_directories(to);
        (void)chunkdb::migrate::Migrate(Options(source, to));
        assert(fs::exists(to / "chunkdb.manifest"));
    }
    {
        // A running server holds the source's writer lock.
        const auto locked = CopyFixture("dev", dir.path() / "locked");
        chunkdb::ProcessLock lock(locked);
        assert(Contains(Refusal(Options(locked, dir.path() / "locked-out")), "a server is using the source"));
    }
    {
        // A 2.0 data directory needs no conversion.
        const auto current = dir.path() / "current";
        {
            chunkdb::CatalogConfig config;
            config.data_dir = current;
            config.default_geometry = kGeometry;
            chunkdb::TableCatalog catalog(config);
        }
        assert(Contains(Refusal(Options(current, dir.path() / "current-out")), "already a 2.0 data directory"));
    }
    {
        // The experimental region layout was never converted.
        const auto region = CopyFixture("v1x", dir.path() / "region");
        WriteBytes(region / "R_0_0.rgn", {0});
        assert(Contains(Refusal(Options(region, dir.path() / "region-out")), "fs_region_v1"));
    }
    // No half-written destination or staging directory is left behind.
    for (const auto& entry : fs::directory_iterator(dir.path())) {
        assert(entry.path().filename().string().find(".migrate-") == std::string::npos);
    }
}

void AppendLe(std::vector<std::uint8_t>* out, std::uint64_t value, int bytes) {
    for (int i = 0; i < bytes; ++i) {
        out->push_back(static_cast<std::uint8_t>(value >> (8 * i)));
    }
}

// A 1.x record: "DLT1", offset, size, CRC of the data, data.
std::vector<std::uint8_t> DltRecord(std::uint32_t offset, const std::vector<std::uint8_t>& data) {
    std::vector<std::uint8_t> out = {'D', 'L', 'T', '1'};
    AppendLe(&out, offset, 4);
    AppendLe(&out, data.size(), 2);
    AppendLe(&out, chunkdb::Crc32(data.data(), data.size()), 4);
    out.insert(out.end(), data.begin(), data.end());
    return out;
}

// A frame of the 2.0 development format: header with its CRC, records with
// theirs, frame CRC over the records.
std::vector<std::uint8_t> Frame(
    std::uint64_t revision,
    const std::vector<std::pair<std::uint32_t, std::vector<std::uint8_t>>>& records) {
    std::vector<std::uint8_t> body;
    for (const auto& [offset, data] : records) {
        std::vector<std::uint8_t> record;
        AppendLe(&record, offset, 4);
        AppendLe(&record, data.size(), 2);
        record.insert(record.end(), data.begin(), data.end());
        AppendLe(&record, chunkdb::Crc32(record.data(), record.size()), 4);
        body.insert(body.end(), record.begin(), record.end());
    }
    std::vector<std::uint8_t> out = {'F', 'R', 'M', '1'};
    AppendLe(&out, revision, 8);
    AppendLe(&out, records.size(), 2);
    AppendLe(&out, body.size(), 4);
    AppendLe(&out, chunkdb::Crc32(out.data() + 4, 14), 4);
    out.insert(out.end(), body.begin(), body.end());
    AppendLe(&out, chunkdb::Crc32(body.data(), body.size()), 4);
    return out;
}

std::vector<std::uint8_t> ReadBytes(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void Append(const fs::path& path, const std::vector<std::uint8_t>& bytes) {
    auto all = ReadBytes(path);
    all.insert(all.end(), bytes.begin(), bytes.end());
    WriteBytes(path, all);
}

std::vector<std::uint8_t> StateOf(const fs::path& converted, std::int64_t cx, std::int64_t cy) {
    chunkdb::CatalogConfig config;
    config.data_dir = converted;
    config.access_mode = chunkdb::AccessMode::kReadOnly;
    config.default_geometry_fields = 0;
    chunkdb::TableCatalog catalog(config);
    auto lease = catalog.Find("default")->Acquire();
    return lease->store().GetChunkStateBytes(cx, cy);
}

// A new write after the conversion never gets a token of the old clock,
// also when every converted chunk kept its own revision.
void TestClockStartsAtOldCeiling() {
    chunkdb::test::ScopedTempDir dir("chunkdb-migrate-clock");
    const auto source = CopyFixture("dev", dir.path());
    for (const char* file : {"L_0_0/C_1_0.wal", "L_1_1/C_3_2.chk", "L_1_1/C_3_2.wal", "L_2_-3/C_4_-5.wal",
                             "L_3_3/C_6_6.wal", ".chunkdb.intents/L_1_1__C_3_2.wal.rollback"}) {
        fs::remove(source / file);
    }
    const auto to = dir.path() / "out";
    const auto summary = chunkdb::migrate::Migrate(Options(source, to));
    assert(summary.assigned_revisions == 0U && summary.persisted_revisions == 4U);
    assert(summary.source_clock_ceiling > 32774U);
    chunkdb::CatalogConfig config;
    config.data_dir = to;
    config.default_geometry_fields = 0;
    chunkdb::TableCatalog catalog(config);
    auto lease = catalog.Find("default")->Acquire();
    const std::vector<std::uint8_t> payload(8, 0x11);
    const std::vector<std::uint8_t> presence = {0xFF, 0xFF};
    assert(lease->store().SetChunkStateBytes(9, 9, payload, presence) >= summary.source_clock_ceiling);
}

void TestIntentsWithoutTheirWal() {
    chunkdb::test::ScopedTempDir dir("chunkdb-migrate-intent-wal");
    const fs::path intents(".chunkdb.intents");
    const auto source = CopyFixture("v1x", dir.path());
    // A rollback that keeps 500 bytes of a WAL that is gone: for a chunk
    // without other files, and for a path no chunk uses.
    WriteBytes(source / intents / "L_0_0__C_1_1.wal.rollback", IntentRecord("CKRB", 500));
    WriteBytes(source / intents / "L_9_9__C_1_1.wal.rollback", IntentRecord("CKRB", 500));
    WriteBytes(source / intents / "..__C_0_0.wal.rollback", IntentRecord("CKRB", 0));
    const auto refusal = Refusal(Options(source, dir.path() / "out"));
    // Paths are reported with the platform's separator.
    const auto wal_name = [](const char* large, const char* chunk) { return (fs::path(large) / chunk).string(); };
    assert(Contains(refusal, wal_name("L_0_0", "C_1_1.wal") + ": missing although a rollback intent keeps 500 bytes"));
    assert(Contains(refusal, wal_name("L_9_9", "C_1_1.wal") + ": missing although"));
    assert(Contains(refusal, "malformed conditional intent name"));
    // Boundary 0 means the WAL held nothing yet: nothing depends on it.
    fs::remove(source / intents / "L_0_0__C_1_1.wal.rollback");
    fs::remove(source / intents / "..__C_0_0.wal.rollback");
    WriteBytes(source / intents / "L_9_9__C_1_1.wal.rollback", IntentRecord("CKRB", 0));
    (void)chunkdb::migrate::Migrate(Options(source, dir.path() / "out"));
}

void TestWalShapes() {
    chunkdb::test::ScopedTempDir dir("chunkdb-migrate-wal-shapes");
    const std::vector<std::uint8_t> payload = {1, 2, 3, 4, 5, 6, 7, 8};
    const std::vector<std::uint8_t> presence = {0xFF, 0xFF};
    {
        // Streams without a WAL header, as a writer could leave them.
        const auto source = CopyFixture("dev", dir.path() / "headerless");
        auto records = DltRecord(0, payload);
        const auto presence_record = DltRecord(8, presence);
        records.insert(records.end(), presence_record.begin(), presence_record.end());
        WriteBytes(source / "L_0_0" / "C_0_1.wal", records);
        WriteBytes(source / "L_0_0" / "C_1_1.wal", Frame(40000, {{0, payload}, {8, presence}}));
        const auto to = dir.path() / "headerless-out";
        const auto summary = chunkdb::migrate::Migrate(Options(source, to));
        assert(summary.wals_headerless == 2U);
        auto want = payload;
        want.insert(want.end(), presence.begin(), presence.end());
        assert(StateOf(to, 0, 1) == want && StateOf(to, 1, 1) == want);
        chunkdb::CatalogConfig config;
        config.data_dir = to;
        config.access_mode = chunkdb::AccessMode::kReadOnly;
        config.default_geometry_fields = 0;
        chunkdb::TableCatalog catalog(config);
        auto lease = catalog.Find("default")->Acquire();
        assert(lease->store().GetChunkVersion(1, 1) == 40000U);
    }
    {
        // A torn v4 frame at the end is a crash, not a loss; so are trailing
        // zeros and an all-zero WAL.
        const auto source = CopyFixture("dev", dir.path() / "torn");
        auto torn = Frame(50000, {{0, payload}});
        torn.resize(torn.size() - 3);
        Append(source / "L_3_-4" / "C_7_-8.wal", torn);
        Append(source / "L_0_0" / "C_1_0.wal", std::vector<std::uint8_t>(40, 0));
        WriteBytes(source / "L_0_0" / "C_1_1.wal", std::vector<std::uint8_t>(64, 0));
        WriteBytes(source / "L_0_0" / "C_0_1.wal", {});
        const auto to = dir.path() / "torn-out";
        const auto summary = chunkdb::migrate::Migrate(Options(source, to));
        assert(summary.losses.empty());
        assert(summary.torn_tails == 3U);
        CheckConverted(to, "dev", summary);
    }
    {
        // A header for another chunk appended after the records, then more
        // records: the old server stopped at the header and lost them.
        const auto source = CopyFixture("v1x", dir.path() / "midstream");
        const auto wal = source / "L_3_3" / "C_6_6.wal";
        auto header = ReadBytes(wal);
        header.resize(36);
        header[20] = 5;  // chunk_x
        Append(wal, header);
        Append(wal, DltRecord(0, payload));
        assert(Contains(Refusal(Options(source, dir.path() / "midstream-out")),
                        "C_6_6.wal: replay stopped at byte"));
    }
    {
        // A WAL header naming another chunk: the old server dropped the WAL.
        const auto source = CopyFixture("v1x", dir.path() / "coord");
        Overwrite(source / "L_3_3" / "C_6_6.wal", 20, 7);
        assert(Contains(Refusal(Options(source, dir.path() / "coord-out")),
                        "C_6_6.wal: not replayable (invalid_header)"));
    }
    {
        // One damaged geometry field while the other headers agree with the
        // flags: damage, which --accept-loss converts without that chunk.
        const auto source = CopyFixture("v1x", dir.path() / "header-field");
        Overwrite(source / "L_-1_-1" / "C_-1_-1.chk", 10, 5);  // block_bits
        auto options = Options(source, dir.path() / "header-field-out");
        assert(Contains(Refusal(options), "C_-1_-1.chk: unreadable image (geometry mismatch)"));
        options.accept_loss = true;
        const auto summary = chunkdb::migrate::Migrate(options);
        assert(summary.losses.size() == 1U && summary.chunks == ReadExpected("v1x").size() - 1U);
    }
    {
        // "out/" is the directory "out".
        const auto source = CopyFixture("v1x", dir.path() / "slash");
        const auto to = fs::path((dir.path() / "slash-out").string() + "/");
        (void)chunkdb::migrate::Migrate(Options(source, to));
        assert(fs::exists(dir.path() / "slash-out" / "chunkdb.manifest"));
        for (const auto& entry : fs::directory_iterator(dir.path() / "slash-out")) {
            assert(entry.path().filename().string().find("migrate") == std::string::npos);
        }
    }
}

// The engine call chunkdb_migrate writes chunks with.
void TestImportChunk() {
    chunkdb::test::ScopedTempDir dir("chunkdb-migrate-import");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    config.default_geometry = kGeometry;
    chunkdb::TableCatalog catalog(config);
    auto lease = catalog.Find("default")->Acquire();
    auto& store = lease->store();
    const std::vector<std::uint8_t> payload = {0xFF, 0x01, 0, 0, 0, 0, 0, 0xF0};
    const std::vector<std::uint8_t> presence = {0x01, 0x80};
    const auto error_of = [&](auto fn) {
        try {
            fn();
        } catch (const std::invalid_argument& e) {
            return std::string(e.what());
        }
        return std::string();
    };
    assert(Contains(error_of([&] { store.ImportChunk(0, 0, payload, presence, 0); }), "nonzero"));
    assert(Contains(error_of([&] { store.ImportChunk(0, 0, payload, {0, 0}, 7); }), "present block"));

    store.ImportChunk(0, 0, payload, presence, 1000000);
    // Absent blocks' payload is stored as zero: only blocks 0 and 15.
    const auto state = store.GetChunkStateBytes(0, 0);
    assert((state == std::vector<std::uint8_t>{0x0F, 0, 0, 0, 0, 0, 0, 0xF0, 0x01, 0x80}));
    assert(store.GetChunkVersion(0, 0) == 1000000U);
    assert(Contains(error_of([&] { store.ImportChunk(0, 0, payload, presence, 5); }), "already has stored state"));
    // The clock is past the imported revision: a new write never reuses it.
    assert(store.SetChunkStateBytes(1, 0, payload, presence) > 1000000U);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: chunkdb_migrate_test <tests/fixtures/migrate>\n";
        return 2;
    }
    g_fixtures = argv[1];
    TestConvertsV1xDirectory();
    TestConvertsDevelopmentFormat();
    TestEngineRefusesUnconvertedSource();
    TestDamageIsRefusedUnlessAccepted();
    TestWrongGeometryIsRefused();
    TestIntents();
    TestDestinationAndSourceChecks();
    TestImportChunk();
    TestClockStartsAtOldCeiling();
    TestIntentsWithoutTheirWal();
    TestWalShapes();
    return 0;
}
