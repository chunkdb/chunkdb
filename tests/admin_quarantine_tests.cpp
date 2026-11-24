// chunkdb_admin quarantine (docs/BACKUP.md): a damaged chunk's files move
// into the table's .chunkdb.quarantine directory with a record, so the table
// opens without the chunk; crashes at each step leave a table a rerun
// finishes.

#include <algorithm>
#include <cassert>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feed_slot_records.hpp"
#include "quarantine_record.hpp"
#include "test_utils.hpp"
#include "txn_test_utils.hpp"
#include "verify.hpp"

namespace {
using namespace chunkdb;

bool Contains(const std::string& text, std::string_view part) { return text.find(part) != std::string::npos; }

std::vector<char> Load(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

// Flips one byte of an image, or of the first frame header of a WAL with a
// later frame, so neither is a crash tail.
void Damage(const std::filesystem::path& path) {
    auto bytes = Load(path);
    assert(!bytes.empty());
    std::size_t at = bytes.size() / 2U;
    if (path.extension() == ".wal") {
        const std::string magic = "FRM2";
        const auto frame = std::search(bytes.begin(), bytes.end(), magic.begin(), magic.end());
        assert(frame != bytes.end());
        at = static_cast<std::size_t>(frame - bytes.begin()) + 6U;
    }
    bytes[at] = static_cast<char>(bytes[at] ^ 0x5A);
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    assert(output);
}

int ExitCode(int status) {
#ifdef _WIN32
    return status;
#else
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
#endif
}

struct Fixture {
    std::string admin;
    test::ScopedTempDir directory{"chunkdb-admin-quarantine"};
    std::filesystem::path data = std::filesystem::canonical(directory.path()) / "data";
    std::filesystem::path table_dir = data / "tables" / "t";
    std::optional<Geometry> geometry;

    explicit Fixture(std::string executable) : admin(std::move(executable)) {
        TableCatalog catalog(Config());
        GeometryConfig config;
        config.chunk_width_blocks = 2U; config.chunk_height_blocks = 2U; config.block_bits = 8U;
        const auto table = catalog.Create("t", config, catalog.default_options());
        (void)table->CreateFeedSlot("consumer");
        auto lease = table->Acquire();
        auto& store = lease->store();
        // Chunk (0,0): an image and a WAL of two frames; chunk (1,0): a WAL.
        store.SetBlockBits(0, 0, "00000001");
        store.CheckpointForTests(0, 0);
        store.SetBlockBits(1, 0, "00000010");
        store.SetBlockBits(0, 1, "00000011");
        store.SetBlockBits(2, 0, "00000100");
        store.WalBarrier();
        geometry.emplace(store.geometry());
    }
    [[nodiscard]] CatalogConfig Config() const {
        CatalogConfig config;
        config.data_dir = data;
        return config;
    }
    [[nodiscard]] std::filesystem::path File(std::int64_t x, std::int64_t y, bool wal) const {
        return wal ? ChunkWalPath(table_dir, *geometry, {x, y}) : ChunkDataPath(table_dir, *geometry, {x, y});
    }
    int Run(const std::string& args, std::string* output) const {
        const auto log = directory.path() / "admin.log";
        std::string command = "\"" + admin + "\" --data-dir \"" + data.string() + "\" " + args + " > \"" + log.string() + "\" 2>&1";
#ifdef _WIN32
        command = "\"" + command + "\"";
#endif
        const int status = std::system(command.c_str());
        const auto bytes = Load(log);
        output->assign(bytes.begin(), bytes.end());
        return ExitCode(status);
    }
    [[nodiscard]] std::vector<std::filesystem::path> Entries() const {
        std::vector<std::filesystem::path> entries;
        const auto root = table_dir / kQuarantineDirName;
        if (!std::filesystem::exists(root)) return entries;
        for (const auto& entry : std::filesystem::directory_iterator(root)) entries.push_back(entry.path());
        return entries;
    }
    [[nodiscard]] bool Opens() const {
        try { TableCatalog catalog(Config()); return true; }
        catch (const std::exception&) { return false; }
    }
    [[nodiscard]] std::string Block(std::int64_t x, std::int64_t y) const {
        TableCatalog catalog(Config());
        auto lease = catalog.Require("t")->Acquire();
        return lease->store().ReadBlockBits(x, y).value_or("");
    }
};

// A damaged image keeps the table from opening; quarantine sets the chunk
// aside, records it and marks the slot, and the table serves without it.
void QuarantinesDamagedImage(const std::string& admin) {
    Fixture f(admin);
    Damage(f.File(0, 0, false));
    assert(!f.Opens());
    std::string output;
    assert(f.Run("quarantine --table t --chunk 0 0", &output) == 0);
    assert(Contains(output, "quarantined chunk 0 0 of table t in tables/t/.chunkdb.quarantine/C_0_0."));
    assert(Contains(output, "C_0_0.wal C_0_0.chk (chunk_image_invalid)") && Contains(output, "1 slots start their next watch"));
    assert(!std::filesystem::exists(f.File(0, 0, false)) && !std::filesystem::exists(f.File(0, 0, true)));
    const auto entries = f.Entries();
    assert(entries.size() == 1U);
    const auto record = ParseQuarantineRecord(LoadFile(entries[0] / std::string(kQuarantineRecordName)));
    assert(record.coord.x == 0 && record.coord.y == 0 && record.files.size() == 2U);
    for (const auto& file : record.files) {
        const auto kept = LoadFile(entries[0] / file.name);
        assert(kept.size() == file.size && Crc32(kept) == file.crc32);
    }
    std::ostringstream findings;
    assert(VerifyDataDirectory(f.data, findings).errors == 0U);
    assert(Contains(findings.str(), "chunk_quarantined"));
    const auto slots = ReadFeedSlotRecords(f.table_dir, record.table_epoch, 2U);
    assert(slots && slots->slots.size() == 1U && slots->slots[0].resync);
    // The table opens: the quarantined chunk is absent, the others serve.
    assert(f.Block(0, 0).empty() && f.Block(0, 1).empty());
    assert(f.Block(2, 0) == "00000100");
    // The chunk takes new writes again.
    {
        TableCatalog catalog(f.Config());
        auto lease = catalog.Require("t")->Acquire();
        lease->store().SetBlockBits(0, 0, "00001000");
    }
    assert(f.Block(0, 0) == "00001000");
}

// Refusals: sound files without --force, a missing chunk, table or
// dimension, a running server, and invalid usage.
void Refusals(const std::string& admin) {
    Fixture f(admin);
    std::string output;
    assert(f.Run("quarantine --table t --chunk 1 0", &output) == 1 && Contains(output, "give --force"));
    assert(std::filesystem::exists(f.File(1, 0, true)) && f.Entries().empty());
    assert(f.Run("quarantine --table t --chunk 5 5", &output) == 1 && Contains(output, "has no image or WAL"));
    assert(f.Run("quarantine --table u --chunk 1 0", &output) == 1 && Contains(output, "has no table u"));
    assert(f.Run("quarantine --table t --chunk 1 0 0", &output) == 1 && Contains(output, "has 2 dimensions"));
    assert(f.Run("quarantine --table t --chunk 1", &output) == 2 && Contains(output, "Usage"));
    assert(f.Run("quarantine --chunk 1 0", &output) == 2);
    assert(f.Run("rebuild-version-clock --table t", &output) == 2);
    {
        TableCatalog running(f.Config());
        assert(f.Run("quarantine --table t --chunk 1 0 --force", &output) == 1);
        assert(std::filesystem::exists(f.File(1, 0, true)));
    }
    assert(f.Run("quarantine --table t --chunk 1 0 --force", &output) == 0 && Contains(output, "quarantined chunk 1 0"));
    assert(!Contains(output, "(") && f.Entries().size() == 1U);
    assert(f.Block(2, 0).empty() && f.Block(0, 0) == "00000001");
}

// A crash at each step leaves a table that opens or keeps the chunk whole,
// and running the command again finishes the quarantine.
void Crashes(const std::string& admin) {
    for (const auto* point : {"BEFORE_RECORD", "AFTER_RECORD", "AFTER_REMOVE"}) {
        Fixture f(admin);
        // A damaged WAL over a sound image: without its WAL the chunk is the
        // image's older state.
        Damage(f.File(0, 0, true));
        assert(!f.Opens());
        std::string output;
        const auto failpoint = "CHUNKDB_FAILPOINT_CRASH_QUARANTINE_" + std::string(point) + "_ONCE";
        {
            txn_test::ScopedEnv crash(failpoint.c_str(), "1");
            assert(f.Run("quarantine --table t --chunk 0 0", &output) == 86);
        }
        const bool removed_wal = std::string_view(point) == "AFTER_REMOVE";
        assert(std::filesystem::exists(f.File(0, 0, true)) != removed_wal);
        assert(std::filesystem::exists(f.File(0, 0, false)));
        assert(f.Opens() == removed_wal);
        std::ostringstream interrupted;
        (void)VerifyDataDirectory(f.data, interrupted);
        assert(Contains(interrupted.str(), "quarantine_interrupted"));
        if (removed_wal) assert(f.Block(0, 0) == "00000001");
        const int rerun = f.Run("quarantine --table t --chunk 0 0", &output);
        if (rerun != 0) std::cerr << point << " exit " << rerun << ": " << output << "\n";
        assert(rerun == 0);
        assert(Contains(output, std::string_view(point) == "BEFORE_RECORD" ? "quarantined chunk" : "finished quarantine of chunk"));
        assert(f.Entries().size() == 1U);
        assert(!std::filesystem::exists(f.File(0, 0, true)) && !std::filesystem::exists(f.File(0, 0, false)));
        assert(f.Opens() && f.Block(0, 0).empty() && f.Block(2, 0) == "00000100");
        std::ostringstream finished;
        (void)VerifyDataDirectory(f.data, finished);
        assert(!Contains(finished.str(), "quarantine_interrupted"));
        // A run after a finished one changes nothing.
        assert(f.Run("quarantine --table t --chunk 0 0", &output) == 0 && Contains(output, "is already quarantined in"));
        assert(f.Entries().size() == 1U);
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: chunkdb_admin_quarantine_test <chunkdb_admin>\n";
        return 2;
    }
    const std::string admin = std::filesystem::absolute(argv[1]).string();
    QuarantinesDamagedImage(admin);
    Refusals(admin);
    Crashes(admin);
    std::cout << "admin_quarantine tests passed\n";
    return 0;
}
