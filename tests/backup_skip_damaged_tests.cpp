// BACKUP TO ... SKIP DAMAGED (docs/BACKUP.md): chunks whose image or WAL
// fails the chunkdb_verify checks are left out, listed in the reply and in
// version 2 of the completion record, and the copy restores without them.

#include <algorithm>
#include <cassert>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "backup.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "test_utils.hpp"
#include "verify.hpp"

namespace {
using namespace chunkdb;

template <typename Function>
std::string Throws(Function&& function) {
    try { function(); } catch (const std::exception& error) { return error.what(); }
    assert(false && "expected failure");
    return {};
}

void Reply(const std::string& actual, std::string_view expected) {
    if (actual != expected) {
        std::cerr << "expected " << expected << ", received " << actual << '\n';
        assert(false);
    }
}

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

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-backup-skip-damaged"};
    std::filesystem::path root = std::filesystem::canonical(directory.path());
    std::filesystem::path data = root / "data", backups = root / "backups";
    std::shared_ptr<TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    SessionState session;

    Fixture() {
        std::filesystem::create_directories(backups);
        std::filesystem::permissions(backups, std::filesystem::perms::owner_all);
        Open();
    }
    void Open() {
        engine.reset();
        catalog.reset();
        CatalogConfig config;
        config.data_dir = data;
        catalog = std::make_shared<TableCatalog>(config);
        engine = std::make_unique<CommandEngine>(EngineConfig{.require_auth = false, .backup_dir = backups}, catalog);
        session = SessionState{};
        assert(engine->Execute(session, "HELLO 3\r\n").starts_with("%"));
    }
    std::string Run(std::string_view statement) { return engine->Execute(session, std::string(statement) + "\r\n"); }
    std::filesystem::path File(std::int64_t x, std::int64_t y, bool wal) {
        const auto lease = catalog->Require("t")->Acquire();
        const auto& store = lease->store();
        return wal ? ChunkWalPath(store.data_dir(), store.geometry(), {x, y})
                   : ChunkDataPath(store.data_dir(), store.geometry(), {x, y});
    }
};

// Chunks (0,0) sound, (1,0) with a damaged image, (2,0) with a damaged WAL.
void DamagedTable(Fixture& f) {
    Reply(f.Run("CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2"), "+OK\r\n");
    for (const auto* statement : {"SET BLOCK 0 0 IN t a = 1", "SET BLOCK 2 0 IN t a = 2", "SET BLOCK 4 0 IN t a = 3",
             "SET BLOCK 5 0 IN t a = 4"})
        assert(f.Run(statement).starts_with(":"));
    {
        auto lease = f.catalog->Require("t")->Acquire();
        lease->store().CheckpointForTests(1, 0);
    }
    Reply(f.Run("FLUSH WAL"), "+OK\r\n");
    f.Open();
    Damage(f.File(1, 0, false));
    Damage(f.File(2, 0, true));
}

void SkipsAndListsDamagedChunks() {
    Fixture f;
    DamagedTable(f);
    // Without SKIP DAMAGED the damage refuses the backup.
    assert(f.Run("BACKUP TO 'plain'").starts_with("-ERR "));
    const auto reply = f.Run("BACKUP TO 'skipped' SKIP DAMAGED");
    assert(reply.starts_with("%5\r\n$6\r\ntables\r\n:1\r\n"));
    assert(Contains(reply, "$7\r\nskipped\r\n*2\r\n"));
    assert(Contains(reply, "$5\r\nchunk\r\n*2\r\n:1\r\n:0\r\n") && Contains(reply, "chunk_image_invalid"));
    assert(Contains(reply, "$5\r\nchunk\r\n*2\r\n:2\r\n:0\r\n") && Contains(reply, "C_2_0.wal"));
    const auto backup = f.backups / "skipped";
    const auto marker = Load(backup / std::string(kBackupMarkerName));
    assert(marker.size() > 6U && marker[4] == 2 && marker[5] == 0);
    const auto record = ReadBackupRecord(backup);
    assert(record.skipped.size() == 2U);
    assert(record.skipped[0].table == "t" && record.skipped[0].coord.x == 1 && record.skipped[0].code == "chunk_image_invalid");
    assert(record.skipped[1].coord.x == 2 && record.skipped[1].dims == 2U);
    for (const auto& file : record.files) {
        const auto name = file.relative_path.filename().string();
        assert(!name.starts_with("C_1_0.") && !name.starts_with("C_2_0."));
    }
    std::ostringstream findings;
    const auto verified = VerifyDataDirectory(backup, findings);
    assert(verified.errors == 0U && verified.warnings == 2U);
    assert(Contains(findings.str(), "backup_chunk_skipped") && Contains(findings.str(), "chunk 2,0 was left out"));
    // The restored copy serves without the skipped chunks.
    const auto restored = f.root / "restored";
    RestoreBackup(backup, restored);
    {
        CatalogConfig config;
        config.data_dir = restored;
        auto catalog = std::make_shared<TableCatalog>(config);
        CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
        SessionState session;
        assert(engine.Execute(session, "HELLO 3\r\n").starts_with("%"));
        Reply(engine.Execute(session, "GET BLOCK 0 0 FROM t\r\n"), "*1\r\n:1\r\n");
        Reply(engine.Execute(session, "GET BLOCK 5 0 FROM t\r\n"), "_\r\n");
        Reply(engine.Execute(session, "GET BLOCK 2 0 FROM t\r\n"), "_\r\n");
        Reply(engine.Execute(session, "CHECK TABLE t\r\n"), "%2\r\n$6\r\nchunks\r\n:1\r\n$7\r\ndamaged\r\n*0\r\n");
    }
    // A record whose skipped chunk does not fit its table is refused.
    const auto marker_path = backup / std::string(kBackupMarkerName);
    std::filesystem::permissions(marker_path, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);
    const auto rewrite = [&](const BackupRecord& changed) {
        const auto bytes = SerializeBackupRecord(changed);
        std::ofstream output(marker_path, std::ios::binary | std::ios::trunc);
        output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        assert(output);
    };
    auto wrong = record;
    wrong.skipped[0].dims = 3U;
    rewrite(wrong);
    assert(Contains(Throws([&] { ValidateBackupInventory(backup, ReadBackupRecord(backup)); }), "other dimensions"));
    wrong = record;
    wrong.skipped[0].coord = ChunkCoord{0, 0};
    rewrite(wrong);
    assert(Contains(Throws([&] { ValidateBackupInventory(backup, ReadBackupRecord(backup)); }), "is in the inventory"));
    rewrite(record);
    ValidateBackupInventory(backup, ReadBackupRecord(backup));
}

// Without damage, SKIP DAMAGED lists nothing and writes a version 1 record.
void NothingToSkip() {
    Fixture f;
    Reply(f.Run("CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2"), "+OK\r\n");
    assert(f.Run("SET BLOCK 0 0 IN t a = 1").starts_with(":"));
    const auto reply = f.Run("BACKUP TO 'clean' SKIP DAMAGED");
    assert(reply.starts_with("%5\r\n") && reply.ends_with("$7\r\nskipped\r\n*0\r\n"));
    assert(Load(f.backups / "clean" / std::string(kBackupMarkerName))[4] == 1);
    assert(f.Run("BACKUP TO 'plain'").starts_with("%4\r\n"));
}

// Version 2 round trip, three-dimensional coordinates and refused records.
void RecordFormat() {
    BackupRecord record;
    record.created_at_ms = 7U;
    record.tables.push_back({"t", StoreId{1U}, 5U});
    record.files.push_back({"chunkdb.manifest", 3U, 4U});
    record.skipped.push_back({"t", 3U, ChunkCoord{-1, 2, -3}, "wal_damaged"});
    const auto bytes = SerializeBackupRecord(record);
    const auto parsed = ParseBackupRecord(bytes);
    assert(parsed.skipped.size() == 1U && parsed.skipped[0].dims == 3U);
    assert(parsed.skipped[0].coord.x == -1 && parsed.skipped[0].coord.y == 2 && parsed.skipped[0].coord.z == -3);
    assert(parsed.skipped[0].code == "wal_damaged" && parsed.files.size() == 1U);
    auto repeated = record;
    repeated.skipped.push_back(repeated.skipped.front());
    assert(Contains(Throws([&] { (void)SerializeBackupRecord(repeated); }), "skipped backup chunk"));
    auto unknown = record;
    unknown.skipped.front().table = "u";
    assert(Contains(Throws([&] { (void)SerializeBackupRecord(unknown); }), "skipped backup chunk"));
    auto truncated = bytes;
    truncated.erase(truncated.end() - 6, truncated.end() - 4);
    assert(!Throws([&] { (void)ParseBackupRecord(truncated); }).empty());
}

}  // namespace

int main() {
    SkipsAndListsDamagedChunks();
    NothingToSkip();
    RecordFormat();
    std::cout << "backup_skip_damaged tests passed\n";
    return 0;
}
