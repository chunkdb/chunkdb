#include <cassert>
#include <fstream>
#include <sstream>

#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feature_flags.hpp"
#include "feed_slot_records.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "catalog_test_utils.hpp"
#include "verify.hpp"
#include "wal_writer.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;

void Save(const std::filesystem::path& path, const Bytes& bytes) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(output.good());
}

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-feed-slots-verify"};
    std::filesystem::path table = directory.path() / "tables" / "default";
    StoreManifest manifest;
    Geometry geometry{{2U, 2U, 2U, 1U, 8U}};
    Fixture() {
        StoreConfig config;
        config.data_dir = table;
        config.geometry = geometry.config();
        auto catalog_config = CatalogConfigFromStoreConfig(config);
        catalog_config.data_dir = directory.path();
        { TableCatalog catalog(catalog_config); (void)test::CreateBitsTable(catalog, config.geometry); }
        manifest = *ReadStoreManifest(table);
        manifest.features.incompat |= kFeatureFeedSlots;
        Save(StoreManifestPath(table), SerializeStoreManifest(manifest));
        WriteFeedSlotRecords(table, {manifest.store_id, 4U, {{"consumer", 1U, false}}});
    }
    std::pair<VerifyCounters, std::string> Verify() const {
        std::ostringstream output;
        auto counters = VerifyDataDirectory(directory.path(), output);
        return {counters, output.str()};
    }
    Bytes Frame(std::uint64_t revision, std::uint8_t value, bool gc = false, bool present = true) const {
        Bytes result;
        WalFrameBuilder frame(&result, 1U, {}, gc ? std::nullopt : std::optional<std::string_view>("alice"), gc);
        const Bytes payload{value, 0U};
        const std::uint8_t presence = (gc || !present) ? 0U : 1U;
        frame.AppendSpan(0U, payload.data(), payload.size());
        frame.AppendSpan(2U, &presence, 1U);
        (void)frame.Finish(revision, revision * 10U);
        return result;
    }
    std::filesystem::path Wal(std::string_view name = "C_0_0.2-4.wal") const {
        return table / kFeedArchiveDirName / name;
    }
    std::filesystem::path Base() const { return table / kFeedArchiveDirName / "C_0_0.2.chk"; }
    Bytes Archive() const {
        auto wal = BuildWalHeader({0, 0}, manifest.store_id, manifest.features);
        for (const auto& frame : {Frame(2U, 8U), Frame(3U, 0U, false, false), Frame(4U, 0U, true)})
            wal.insert(wal.end(), frame.begin(), frame.end());
        return wal;
    }
};

void RecordsAndTemporaries() {
    Fixture fixture;
    assert(fixture.Verify().first.errors == 0U);
    const auto path = fixture.table / kFeedSlotsFileName;
    const auto valid = LoadFile(path);
    auto corrupt = valid;
    corrupt.back() ^= 1U;
    Save(path, corrupt);
    auto [counts, output] = fixture.Verify();
    assert(counts.errors > 0U && output.find("feed_slots_invalid") != std::string::npos);
    assert(LoadFile(path) == corrupt);
    Save(path, valid);
    auto foreign = fixture.manifest.store_id;
    foreign[0] ^= 1U;
    WriteFeedSlotRecords(fixture.table, {foreign, 4U, {{"consumer", 1U, false}}});
    assert(fixture.Verify().second.find("feed_slots_invalid") != std::string::npos);
    Save(path, valid);
    Save(fixture.table / "chunkdb.slots.tmp.interrupted", {1U, 2U});
    const auto temporary = fixture.table / "chunkdb.slots.tmp.interrupted";
    auto temporary_check = fixture.Verify();
    assert(temporary_check.first.errors == 0U && temporary_check.second.find("tmp_artifact") != std::string::npos);
    assert(LoadFile(temporary) == Bytes({1U, 2U}));
    fixture.manifest.features.incompat &= ~kFeatureFeedSlots;
    Save(StoreManifestPath(fixture.table), SerializeStoreManifest(fixture.manifest));
    assert(fixture.Verify().second.find("feed_slots_without_feature") != std::string::npos);
}

void ArchiveValidation() {
    Fixture fixture;
    const auto bytes = fixture.Archive();
    Save(fixture.Wal(), bytes);
    auto [counts, output] = fixture.Verify();
    assert(counts.errors == 0U && output.find("feed_archive_wal_invalid") == std::string::npos);
    assert(LoadFile(fixture.Wal()) == bytes);
    std::filesystem::rename(fixture.Wal(), fixture.Wal("C_0_0.2-5.wal"));
    assert(fixture.Verify().second.find("feed_archive_wal_invalid") != std::string::npos);
    std::filesystem::rename(fixture.Wal("C_0_0.2-5.wal"), fixture.Wal());
    auto partial = bytes;
    partial.pop_back();
    Save(fixture.Wal(), partial);
    assert(fixture.Verify().second.find("feed_archive_wal_invalid") != std::string::npos);
    assert(LoadFile(fixture.Wal()) == partial);
    auto damaged = bytes;
    damaged.back() ^= 1U;
    Save(fixture.Wal(), damaged);
    assert(fixture.Verify().second.find("feed_archive_wal_invalid") != std::string::npos);
    Save(fixture.Wal(), bytes);
    std::filesystem::rename(fixture.Wal(), fixture.Wal("C_0_0.0-4.wal"));
    assert(fixture.Verify().second.find("feed_archive_name_invalid") != std::string::npos);
    std::filesystem::remove(fixture.Wal("C_0_0.0-4.wal"));
    // A linked base before WAL rename is an accepted checkpoint crash state.
    const auto image = SerializeChunkImage(fixture.geometry, {0, 0}, {7U, 0U}, {1U},
        CheckpointCompression::kNone, 1U, 10U, fixture.manifest.store_id);
    Save(fixture.Base(), image);
    assert(fixture.Verify().first.errors == 0U);
    auto bad_base = image;
    bad_base.back() ^= 1U;
    Save(fixture.Base(), bad_base);
    assert(fixture.Verify().second.find("feed_archive_base_invalid") != std::string::npos);
    assert(LoadFile(fixture.Base()) == bad_base);
    Save(fixture.Base(), SerializeChunkImage(fixture.geometry, {0, 0}, {7U, 0U}, {1U},
        CheckpointCompression::kNone, 2U, 20U, fixture.manifest.store_id));
    assert(fixture.Verify().second.find("feed_archive_base_invalid") != std::string::npos);
}
}  // namespace

int main() { RecordsAndTemporaries(); ArchiveValidation(); }
