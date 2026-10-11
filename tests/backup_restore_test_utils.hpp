#pragma once
#include <algorithm>
#include <cassert>
#include <iostream>
#include <sstream>
#include "backup.hpp"
#include "checkpoint.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feature_flags.hpp"
#include "feed_slot_records.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "verify.hpp"
#include "wal_writer.hpp"

namespace chunkdb::backup_test {
using Bytes = std::vector<std::uint8_t>;
inline void Save(const std::filesystem::path& path, const Bytes& bytes) {
    EnsureDirectoryPathExists(path.parent_path(), true);
    AtomicWrite(path, bytes, true, true);
}
template <typename Exception = std::exception, typename Function>
void Throws(Function&& function) {
    try { function(); } catch (const Exception&) { return; }
    assert(false && "expected failure");
}
inline VerifyCounters Verify(const std::filesystem::path& path) {
    std::ostringstream out; auto result = VerifyDataDirectory(path, out);
    if (result.errors != 0U) std::cerr << out.str();
    return result;
}
inline void RefusesOpen(const std::filesystem::path& catalog_directory) {
    const auto refused = [](auto&& open) {
        try { open(); }
        catch (const std::exception& error) {
            assert(std::string_view(error.what()).find("backup or incomplete restore directory cannot be opened") != std::string_view::npos);
            return;
        }
        assert(false && "backup open must be refused");
    };
    std::vector<std::filesystem::path> before;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(catalog_directory)) before.push_back(entry.path());
    std::sort(before.begin(), before.end());
    for (const auto mode : {AccessMode::kReadWrite, AccessMode::kReadOnly}) {
        CatalogConfig catalog; catalog.data_dir = catalog_directory; catalog.access_mode = mode;
        refused([&] { TableCatalog opened(catalog); });
        for (const auto& directory : {catalog_directory, catalog_directory / "tables/default"}) {
            StoreConfig store; store.data_dir = directory; store.access_mode = mode; store.geometry_fields = 0U;
            refused([&] { ChunkStore opened(store); });
        }
    }
    std::vector<std::filesystem::path> after;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(catalog_directory)) after.push_back(entry.path());
    std::sort(after.begin(), after.end()); assert(before == after);
}
// A frozen cut with a compressed old-schema checkpoint, a later old-schema
// WAL, and a current manifest adding a column. Its optional image section
// must survive epoch rewriting byte for byte.
struct Fixture {
    test::ScopedTempDir temporary{"chunkdb-backup-restore"};
    std::filesystem::path root = std::filesystem::canonical(temporary.path());
    std::filesystem::path source = root / "source", backup = root / "backup";
    StoreManifest manifest;
    Geometry old_geometry{{2U, 2U, 2U, 1U, 8U}};
    BackupRecord record;
    explicit Fixture(DurabilityMode mode = DurabilityMode::kRelaxed) {
        std::filesystem::create_directory(source);
        PrepareBackupTarget(source, backup);
        Save(backup / kDataDirManifestFileName, SerializeDataDirManifest({{}, NewStoreId(), {}}));
        manifest.geometry = old_geometry.config(); manifest.store_id = NewStoreId();
        manifest.features = {kFeatureFeedSlots, 0U, 1U};
        TableOptions options; options.durability_mode = mode;
        manifest.options = EncodeTableOptions(options);
        Column extra; extra.name = "extra"; extra.type = {ColumnKind::kUnsigned, 8U}; extra.has_default = true; extra.default_value = {9U};
        manifest.schema = AddColumn(old_geometry.layout().schema(), extra);
        manifest.geometry.block_bits = FixedBitsPerBlock(manifest.schema);
        const auto table = backup / "tables/default";
        Save(StoreManifestPath(table), SerializeStoreManifest(manifest));
        Save(table / "chunkdb.version", SerializeVersionClockRecord(100U));
        Save(table / "chunkdb.snapshot", SerializeSnapshotGenerationRecord(0U));
        Bytes initialized{'C', 'K', 'I', 'D'}; WriteLe64(initialized, 1U); WriteLe32(initialized, Crc32(initialized));
        Save(table / ".chunkdb.initialized", initialized);
        WriteFeedSlotRecords(table, {manifest.store_id, 3U, {{"consumer", 1U, false}, {"lost", 2U, true}}});
        auto image = SerializeChunkImage(old_geometry, {0, 0}, {7U, 0U}, {1U}, CheckpointCompression::kZrle, 2U, 20U, manifest.store_id);
        const auto old_crc = kImageFixedHeaderSize + 2U * kImageSectionEntrySize;
        Bytes extended(image.begin(), image.begin() + static_cast<std::ptrdiff_t>(old_crc));
        extended[10U] = 3U; extended[20U] = 1U;
        const Bytes optional{'o', 'p', 'q'};
        WriteLe16(extended, 99U); WriteLe16(extended, 0U); WriteLe32(extended, optional.size()); WriteLe32(extended, optional.size()); WriteLe32(extended, Crc32(optional));
        WriteLe32(extended, Crc32(extended));
        extended.insert(extended.end(), image.begin() + static_cast<std::ptrdiff_t>(old_crc + 4U), image.end());
        extended.insert(extended.end(), optional.begin(), optional.end());
        Save(ChunkDataPath(table, old_geometry, {0, 0}), extended);
        auto wal = BuildWalHeader({0, 0}, manifest.store_id, {});
        const auto frame = Frame(3U);
        wal.insert(wal.end(), frame.begin(), frame.end());
        Save(ChunkWalPath(table, old_geometry, {0, 0}), wal);
        Users users; users.secret.fill(7U); Save(backup / kUsersFileName, EncodeUsers(users));
        record.created_at_ms = 123U; record.tables.push_back({"default", manifest.store_id, 3U});
        Refresh(); CompleteBackup(backup, record);
    }
    Bytes Frame(std::uint64_t revision) const {
        Bytes bytes; WalFrameBuilder frame(&bytes, 1U); const std::uint8_t value = 8U;
        frame.AppendSpan(0U, &value, 1U); (void)frame.Finish(revision, revision * 10U); return bytes;
    }
    std::filesystem::path Table() const { return backup / "tables/default"; }
    void Refresh() {
        record.files.clear();
        for (const auto& entry : std::filesystem::recursive_directory_iterator(backup)) {
            if (!entry.is_regular_file()) continue;
            const auto name = entry.path().filename().string();
            if (name == kBackupMarkerName || name == kBackupIncompleteName || name == kRestoreIncompleteName) continue;
            record.files.push_back(InspectBackupFile(backup, entry.path().lexically_relative(backup)));
        }
    }
    void Remark() { Refresh(); Save(backup / kBackupMarkerName, SerializeBackupRecord(record)); }
};
} // namespace chunkdb::backup_test
