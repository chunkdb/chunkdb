#include <cassert>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>

#include "verify.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunk_coordinate_index.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "wal_writer.hpp"
#include "archive_base_format.hpp"
#include "feed_slot_records.hpp"
#include "txn_intent_format.hpp"
#include "directory_alias_test_utils.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;

void Save(const std::filesystem::path& path, const Bytes& bytes) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close(); assert(output);
}
std::map<std::filesystem::path, Bytes> Snapshot(const std::filesystem::path& root) {
    std::map<std::filesystem::path, Bytes> files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root))
        if (entry.is_regular_file()) files.emplace(entry.path().lexically_relative(root), LoadFile(entry.path()));
    return files;
}

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-verify-index"};
    std::filesystem::path root = directory.path() / "tables/t";
    GeometryConfig config;
    StoreManifest manifest;
    Geometry geometry;
    explicit Fixture(std::uint8_t dims = 2U, bool required = false)
        : config(Shape(dims)), manifest(Manifest(config, required)),
          geometry(config, manifest.schema, *manifest.chunk_schema) {
        Save(DataDirManifestPath(directory.path()), SerializeDataDirManifest({.data_dir_id = StoreId{9U}, .options = {}}));
        Save(StoreManifestPath(root), SerializeStoreManifest(manifest));
        Save(root / "chunkdb.version", SerializeVersionClockRecord(1000U));
        Inventory({});
    }
    static GeometryConfig Shape(std::uint8_t dims) {
        GeometryConfig shape{2U, 2U, 2U, 2U, 8U};
        shape.dims = dims;
        shape.chunk_depth_blocks = dims == 3U ? 2U : 1U;
        shape.large_chunk_depth_chunks = dims == 3U ? 2U : 1U;
        return shape;
    }
    static StoreManifest Manifest(const GeometryConfig& shape, bool required) {
        StoreManifest value{.geometry = shape, .store_id = StoreId{1U},
            .options = EncodeTableOptions(TableOptions{}), .schema = SingleBitsColumnSchema(8U),
            .chunk_schema = TableSchema{}};
        if (required) {
            value.chunk_schema->next_column_id = 2U;
            value.chunk_schema->columns.push_back({.id = 1U, .name = "owner",
                .type = {ColumnKind::kUnsigned, 8U}, .required = true, .default_value = {}});
        }
        return value;
    }
    ChunkCoord Coord() const { return {-1, 2, geometry.dims() == 3U ? -3 : 0}; }
    std::filesystem::path Log(ChunkCoord coord) const {
        return LargeChunkDirectory(root, geometry.ChunkToLarge(coord), geometry.dims()) / kChunkIndexFileName;
    }
    Bytes LogBytes(ChunkCoord coord, ChunkIndexKind kind = ChunkIndexKind::kDeleted,
        std::uint64_t revision = 10U, std::uint64_t schema = 1U) const {
        auto bytes = SerializeChunkIndexHeader({manifest.store_id, geometry.dims(), geometry.ChunkToLarge(coord)});
        if (kind == ChunkIndexKind::kForget) {
            const auto deleted = SerializeChunkIndexRecord({ChunkIndexKind::kDeleted, coord, revision, revision * 10U, schema}, geometry, geometry.ChunkToLarge(coord));
            bytes.insert(bytes.end(), deleted.begin(), deleted.end());
        }
        const auto record = SerializeChunkIndexRecord({kind, coord, revision, revision * 10U, schema}, geometry, geometry.ChunkToLarge(coord));
        bytes.insert(bytes.end(), record.begin(), record.end());
        return bytes;
    }
    void Inventory(std::vector<IndexInventoryEntry> entries, bool pending = false, std::uint64_t floor = 0U) {
        std::ostringstream output(std::ios::binary);
        WriteIndexInventory(output, {.table_id = manifest.store_id, .dims = geometry.dims(), .compaction_pending = pending,
            .retained_deletion_floor = floor, .compaction_through = pending ? floor : 0U, .count = entries.size()}, entries);
        const auto bytes = output.str();
        Save(root / kChunkIndexFileName, Bytes(bytes.begin(), bytes.end()));
    }
    void Retained(ChunkCoord coord, ChunkIndexKind kind = ChunkIndexKind::kDeleted, std::uint64_t floor = 0U) {
        const auto bytes = LogBytes(coord, kind);
        Save(Log(coord), bytes);
        Inventory({{.coord = geometry.ChunkToLarge(coord), .committed_bytes = bytes.size()}}, false, floor);
    }
    void Image(ChunkCoord coord, std::uint64_t revision, bool populated, std::uint64_t time = 0U) {
        Bytes payload(geometry.ChunkPayloadBytes(), 0U), presence(ChunkPresenceBitmapBytes(geometry), 0U);
        if (populated) { payload[0] = 7U; presence[0] = 1U; }
        const auto columns = DefaultChunkColumnsAt(geometry, 1U);
        Save(ChunkDataPath(root, geometry, coord), SerializeChunkImage(geometry, coord, payload, presence,
            CheckpointCompression::kNone, revision, time == 0U ? revision * 10U : time, manifest.store_id,
            nullptr, columns.get()));
    }
    void Wal(ChunkCoord coord, std::uint64_t revision, bool populated) {
        auto bytes = BuildWalHeader(coord, manifest.store_id, manifest.features, geometry.dims());
        WalFrameBuilder frame(&bytes, 1U);
        Bytes state(geometry.ChunkPayloadBytes() + ChunkPresenceBitmapBytes(geometry), 0U);
        if (populated) { state[0] = 7U; state[geometry.ChunkPayloadBytes()] = 1U; }
        frame.AppendSpan(0U, state.data(), state.size());
        (void)frame.Finish(revision, revision * 10U);
        Save(ChunkWalPath(root, geometry, coord), bytes);
    }
    std::string Check(bool error = false, std::string_view code = {}) const {
        const auto before = Snapshot(directory.path());
        std::ostringstream output;
        const auto result = VerifyDataDirectory(directory.path(), output);
        if ((result.errors != 0U) != error || (!code.empty() && output.str().find(code) == std::string::npos))
            std::cerr << output.str();
        assert((result.errors != 0U) == error);
        assert(code.empty() || output.str().find(code) != std::string::npos);
        assert(Snapshot(directory.path()) == before);
        return output.str();
    }
};

void HeaderOnlyWalMetadata() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        for (const bool required : {false, true}) {
            Fixture fixture(dims, true);
            if (!required) {
                auto& owner = fixture.manifest.chunk_schema->columns.front();
                owner.has_default = true;
                owner.default_value = {7U};
                fixture.geometry = Geometry(fixture.config, fixture.manifest.schema, *fixture.manifest.chunk_schema);
                Save(StoreManifestPath(fixture.root), SerializeStoreManifest(fixture.manifest));
            }
            Save(ChunkWalPath(fixture.root, fixture.geometry, fixture.Coord()),
                BuildWalHeader(fixture.Coord(), fixture.manifest.store_id, fixture.manifest.features, dims));
            fixture.Check();
        }
    }
}

void CanonicalPaths() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        Fixture fixture(dims);
        const auto coord = fixture.Coord();
        fixture.Image(coord, 10U, true); fixture.Wal(coord, 11U, false);
        fixture.Check();
        const auto source = ChunkDataPath(fixture.root, fixture.geometry, coord);
        const auto wrong = LargeChunkDirectory(fixture.root, {0, 0, 0}, dims) / source.filename();
        Save(wrong, LoadFile(source));
        fixture.Check(true, "chunk_misplaced");
        std::filesystem::remove(wrong);
        auto malformed = source.parent_path() / (dims == 3U ? "C_-1_2.chk" : "C_-01_2.chk");
        Save(malformed, LoadFile(source));
        fixture.Check(true, "chunk_name_invalid");
        std::filesystem::remove(malformed);
        const auto bad_large = fixture.root / (dims == 3U ? "L_0_0" : "L_00_0");
        std::filesystem::create_directory(bad_large);
        fixture.Check(true, "large_chunk_name_invalid");
    }
}

void ManagedAliases() {
    Fixture fixture;
    fixture.Image(fixture.Coord(), 10U, true);
    fixture.Check();
    test::ScopedTempDir outside("chunkdb-verify-alias");
    const auto root_alias_path = outside.path() / "root";
    test::ScopedDirectoryAlias root_alias(fixture.directory.path(), root_alias_path);
    std::ostringstream findings;
    assert(VerifyDataDirectory(root_alias_path, findings).errors == 0U);
    const auto managed = LargeChunkDirectory(fixture.root, fixture.geometry.ChunkToLarge(fixture.Coord()));
    const auto target = outside.path() / "shard";
    std::filesystem::rename(managed, target);
    test::ScopedDirectoryAlias alias(target, managed);
    const auto before = Snapshot(target);
    fixture.Check(true, "large_chunk_path_invalid");
    assert(Snapshot(target) == before);
}
void InventoryAndCommittedCuts() {
    Fixture fixture;
    const auto coord = fixture.Coord();
    fixture.Retained(coord); fixture.Check();
    const auto path = fixture.root / kChunkIndexFileName;
    const auto inventory = LoadFile(path);
    std::filesystem::remove(path); fixture.Check(true, "coordinate_index_invalid"); Save(path, inventory);
    auto damaged = inventory; damaged.back() ^= 1U; Save(path, damaged);
    fixture.Check(true, "coordinate_index_invalid"); Save(path, inventory);
    const auto log = fixture.Log(coord);
    const auto bytes = LoadFile(log);
    std::filesystem::remove(log); fixture.Check(true, "coordinate_index_invalid"); Save(log, bytes);
    damaged = bytes; damaged.back() ^= 1U; Save(log, damaged);
    fixture.Check(true, "coordinate_index_invalid"); Save(log, bytes);
    auto suffix = bytes; suffix.push_back(255U); Save(log, suffix);
    fixture.Check(false, "index_log_unpublished_suffix"); Save(log, bytes);
    const auto header = SerializeChunkIndexHeader({fixture.manifest.store_id, fixture.geometry.dims(), fixture.geometry.ChunkToLarge(coord)});
    fixture.Inventory({{.coord = fixture.geometry.ChunkToLarge(coord), .committed_bytes = 2U*bytes.size()-header.size()}});
    fixture.Check(true, "coordinate_index_invalid");
}
void RetainedOverlapAndResurrection() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        Fixture fixture(dims, true);
        const auto coord = fixture.Coord(); fixture.Retained(coord);
        fixture.Wal(coord, 11U, true); fixture.Check();
        std::filesystem::remove(ChunkWalPath(fixture.root, fixture.geometry, coord));
        fixture.Image(coord, 10U, true); fixture.Check(true, "chunk_state_invalid");
        fixture.Image(coord, 10U, false, 101U); fixture.Check(true, "commit time differs");
        fixture.Image(coord, 10U, false); fixture.Check();
        fixture.Image(coord, 9U, true); fixture.Check();
        std::filesystem::remove(ChunkDataPath(fixture.root, fixture.geometry, coord));
        fixture.Wal(coord, 10U, false);
        const auto path = ChunkWalPath(fixture.root, fixture.geometry, coord);
        auto wrong_time = BuildWalHeader(coord, fixture.manifest.store_id, fixture.manifest.features, dims);
        WalFrameBuilder frame(&wrong_time, 1U);
        Bytes state(fixture.geometry.ChunkPayloadBytes()+ChunkPresenceBitmapBytes(fixture.geometry), 0U);
        frame.AppendSpan(0U, state.data(), state.size()); (void)frame.Finish(10U, 101U);
        Save(path, wrong_time); fixture.Check(true, "frame_base_commit_time_mismatch");
        fixture.Image(coord, 20U, true);
        fixture.Check(true, "frame_retained_commit_time_mismatch");
        std::filesystem::remove(ChunkDataPath(fixture.root, fixture.geometry, coord));
        fixture.Wal(coord, 10U, false); fixture.Check();
    }
}

void ImageBaseTimeAndConflictingVisibilityCuts() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        Fixture fixture(dims);
        const auto coord = fixture.Coord();
        fixture.Image(coord, 10U, true);
        auto bytes = BuildWalHeader(coord, fixture.manifest.store_id, fixture.manifest.features, dims);
        WalFrameBuilder frame(&bytes, 1U);
        Bytes state(fixture.geometry.ChunkPayloadBytes() + ChunkPresenceBitmapBytes(fixture.geometry), 0U);
        state[0] = 7U; state[fixture.geometry.ChunkPayloadBytes()] = 1U;
        frame.AppendSpan(0U, state.data(), state.size()); (void)frame.Finish(10U, 101U);
        const auto wal = ChunkWalPath(fixture.root, fixture.geometry, coord);
        Save(wal, bytes); fixture.Check(true, "frame_base_commit_time_mismatch");
        fixture.Wal(coord, 10U, true); fixture.Check();
        fixture.Image(coord, 9U, true); fixture.Wal(coord, 10U, false);
        const auto full = std::filesystem::file_size(wal);
        const auto header = ChunkWalHeaderBytes(dims);
        const auto conditional = ConditionalIntentDirectory(fixture.root) / CanonicalConditionalIntentFileName(fixture.geometry, coord);
        const auto put_conditional = [&](bool committed, std::uint64_t cut) {
            Bytes marker{'C', 'K', 'R', static_cast<std::uint8_t>(committed ? 'C' : 'B')};
            WriteLe64(marker, cut); WriteLe32(marker, Crc32(marker)); Save(conditional, marker);
        };
        const auto put_transaction = [&](std::uint64_t revision, std::uint64_t cut) {
            Save(TxnIntentPath(fixture.root, revision), SerializeTxnIntent({TxnIntentState::kRollback,
                revision, {{coord, cut}}}, dims, fixture.manifest.store_id));
        };
        put_conditional(false, header); put_transaction(11U, full);
        fixture.Check(true, "conflicting pending rollback boundaries");
        put_conditional(false, full); fixture.Check();
        put_conditional(true, header); fixture.Check();
        std::filesystem::remove(conditional);
        put_transaction(12U, header); fixture.Check(true, "conflicting pending rollback boundaries");
        put_transaction(12U, full); fixture.Check();
    }
}

void RollbackVisibility() {
    for (const bool transaction : {false, true}) {
        Fixture fixture(3U);
        const auto coord = fixture.Coord();
        Save(fixture.Log(coord), fixture.LogBytes(coord));
        fixture.Image(coord, 9U, true); fixture.Wal(coord, 10U, false);
        const auto cut = ChunkWalHeaderBytes(fixture.geometry.dims());
        auto marker = ConditionalIntentDirectory(fixture.root) / CanonicalConditionalIntentFileName(fixture.geometry, coord);
        const auto write_marker = [&](bool rollback, std::uint64_t boundary) {
            Bytes bytes;
            if (transaction) {
                marker = TxnIntentPath(fixture.root, 10U);
                bytes = SerializeTxnIntent({rollback ? TxnIntentState::kRollback : TxnIntentState::kCommitted,
                    10U, {{coord, boundary}}}, fixture.geometry.dims(), fixture.manifest.store_id);
            } else {
                bytes = {'C', 'K', 'R', static_cast<std::uint8_t>(rollback ? 'B' : 'C')};
                WriteLe64(bytes, boundary); WriteLe32(bytes, Crc32(bytes));
            }
            Save(marker, bytes);
        };
        write_marker(true, cut); fixture.Check(true, "index_unpublished_boundary_without_artifact");
        write_marker(false, cut); fixture.Check();
        write_marker(true, cut + 1U); fixture.Check(true, "wal_damaged");
        write_marker(true, cut);
        std::filesystem::remove(ChunkWalPath(fixture.root, fixture.geometry, coord));
        fixture.Check(true, "rollback_visibility_invalid");
    }
}
void UnlistedAndForgetBindings() {
    Fixture fixture;
    const auto coord = fixture.Coord();
    Save(fixture.Log(coord), fixture.LogBytes(coord));
    fixture.Check(true, "index_unpublished_boundary_without_artifact");
    fixture.Image(coord, 10U, true); fixture.Check(true, "index_unpublished_boundary_without_artifact");
    fixture.Image(coord, 10U, false); fixture.Check(false, "index_log_unlisted");
    fixture.Image(coord, 11U, true); fixture.Check();
    Save(fixture.Log(coord), fixture.LogBytes(coord, ChunkIndexKind::kForget));
    fixture.Check(true, "coordinate_index_invalid");
    fixture.Retained(coord, ChunkIndexKind::kForget, 9U); fixture.Check(true, "coordinate_index_invalid");
    fixture.Retained(coord, ChunkIndexKind::kForget, 10U); fixture.Check();
}
void CompactionDecisions() {
    Fixture fixture;
    const auto coord = fixture.Coord();
    const auto original = fixture.LogBytes(coord);
    const auto replacement = SerializeChunkIndexHeader({fixture.manifest.store_id, fixture.geometry.dims(), fixture.geometry.ChunkToLarge(coord)});
    Save(fixture.Log(coord), original);
    auto stage = fixture.Log(coord); stage += ".compact";
    Save(stage, replacement);
    fixture.Inventory({{.coord = fixture.geometry.ChunkToLarge(coord), .committed_bytes = original.size(),
        .replacement_bytes = replacement.size(), .replacement_sha256 = crypto::Sha256Of(replacement)}}, true, 10U);
    fixture.Check(false, "index_compaction_pending");
    auto damaged = replacement; damaged.back() ^= 1U; Save(stage, damaged);
    fixture.Check(true, "coordinate_index_invalid");
    std::filesystem::remove(stage); fixture.Check(true, "coordinate_index_invalid");
    Save(fixture.Log(coord), replacement); fixture.Check(false, "index_compaction_pending");
    auto longer = original;
    const auto live = SerializeChunkIndexRecord({ChunkIndexKind::kLive, coord, 11U, 110U, 1U}, fixture.geometry,
        fixture.geometry.ChunkToLarge(coord));
    longer.insert(longer.end(), live.begin(), live.end());
    const auto retained_live = fixture.LogBytes(coord, ChunkIndexKind::kLive, 11U);
    Save(fixture.Log(coord), longer); Save(stage, retained_live);
    fixture.Inventory({{.coord = fixture.geometry.ChunkToLarge(coord), .committed_bytes = longer.size(),
        .replacement_bytes = retained_live.size(), .replacement_sha256 = crypto::Sha256Of(retained_live)}}, true, 10U);
    fixture.Check(false, "index_compaction_pending");
    Save(stage, fixture.LogBytes(coord, ChunkIndexKind::kLive, 12U));
    fixture.Check(true, "coordinate_index_invalid");
}

void CheckedBindings() {
    Fixture fixture(3U);
    const auto coord = fixture.Coord();
    fixture.Retained(coord);
    const auto log = fixture.Log(coord);
    const auto original = LoadFile(log);
    auto wrong = SerializeChunkIndexHeader({StoreId{2U}, fixture.geometry.dims(), fixture.geometry.ChunkToLarge(coord)});
    const auto header_size = wrong.size();
    wrong.insert(wrong.end(), original.begin() + static_cast<std::ptrdiff_t>(header_size), original.end());
    Save(log, wrong); fixture.Check(true, "coordinate_index_invalid"); Save(log, original);
    const auto root_id = fixture.manifest.store_id;
    fixture.manifest.store_id = StoreId{2U};
    fixture.Inventory({{.coord = fixture.geometry.ChunkToLarge(coord), .committed_bytes = original.size()}});
    fixture.manifest.store_id = root_id;
    fixture.Check(true, "coordinate_index_invalid");
    fixture.Inventory({{.coord = fixture.geometry.ChunkToLarge(coord), .committed_bytes = original.size()}});
    fixture.Image(coord, 11U, true);
    const auto image_path = ChunkDataPath(fixture.root, fixture.geometry, coord);
    const auto image = LoadFile(image_path);
    Save(image_path, RewriteChunkImageStoreId(image, fixture.geometry, coord, root_id, StoreId{2U}, fixture.manifest.features));
    fixture.Check(true, "chunk_image_invalid"); Save(image_path, image);
    fixture.Wal(coord, 12U, true);
    const auto wal_path = ChunkWalPath(fixture.root, fixture.geometry, coord);
    Save(wal_path, RewriteWalStoreId(LoadFile(wal_path), coord, root_id, StoreId{2U}, fixture.manifest.features, 3U));
    fixture.Check(true, "wal_not_replayable");
}

void ArchiveDescriptors() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        Fixture fixture(dims);
        const auto coord = fixture.Coord();
        fixture.manifest.features.incompat |= kFeatureFeedSlots;
        Save(StoreManifestPath(fixture.root), SerializeStoreManifest(fixture.manifest));
        fixture.Wal(coord, 11U, true);
        const auto directory = fixture.root / kFeedArchiveDirName;
        const auto wal = directory / (ChunkFileStem(coord, dims) + ".11-11.wal");
        Save(wal, LoadFile(ChunkWalPath(fixture.root, fixture.geometry, coord)));
        std::filesystem::remove(ChunkWalPath(fixture.root, fixture.geometry, coord));
        const auto descriptor_path = directory / ArchiveBaseFileName(coord, dims, 11U);
        auto descriptor = ArchiveBaseDescriptor{.dims = dims, .kind = ArchiveBaseKind::kRetainedEmpty,
            .epoch = fixture.manifest.store_id, .coord = coord, .first = 11U,
            .base_revision = 10U, .base_time_ms = 100U, .base_schema = 1U};
        auto descriptor_bytes = SerializeArchiveBaseDescriptor(descriptor, kMaxChunkImageFileBytes);
        Save(descriptor_path, descriptor_bytes); fixture.Check();
        std::filesystem::remove(descriptor_path); fixture.Check(true, "feed_archive_wal_invalid");
        Save(descriptor_path, descriptor_bytes);
        auto corrupt = descriptor_bytes; corrupt.back() ^= 1U;
        Save(descriptor_path, corrupt); fixture.Check(true, "feed_archive_base_invalid");
        Save(descriptor_path, descriptor_bytes);
        fixture.Image(coord, 10U, true);
        const auto image = LoadFile(ChunkDataPath(fixture.root, fixture.geometry, coord));
        const auto pinned = directory / (ChunkFileStem(coord, dims) + ".11.chk");
        Save(pinned, image); fixture.Check(true, "non-image archive base");
        descriptor.kind = ArchiveBaseKind::kImage;
        descriptor.image_length = image.size(); descriptor.image_sha256 = crypto::Sha256Of(image);
        descriptor_bytes = SerializeArchiveBaseDescriptor(descriptor, kMaxChunkImageFileBytes);
        Save(descriptor_path, descriptor_bytes); fixture.Check();
        auto changed = ParseChunkImage(image, fixture.geometry, coord, fixture.manifest.store_id, fixture.manifest.features);
        changed.payload[0] ^= 1U;
        Save(pinned, SerializeChunkImage(fixture.geometry, coord, changed.payload, changed.presence_bitmap,
            CheckpointCompression::kNone, changed.revision, changed.commit_time_ms, fixture.manifest.store_id));
        fixture.Check(true, "digest mismatch");
    }
}
void HistoricalIntentCounts() {
    constexpr std::size_t count = 16385U;
    for (const auto dims : {std::uint8_t{2U}, std::uint8_t{3U}}) {
        for (const auto state : {TxnIntentState::kRollback, TxnIntentState::kCommitted}) {
            test::ScopedTempDir directory("chunkdb-historical-intent-count");
            Save(DataDirManifestPath(directory.path()), SerializeDataDirManifest({.data_dir_id = StoreId{9U}, .options = {}}));
            StoreConfig config;
            config.data_dir = directory.path() / "tables/t";
            config.geometry = {1U, 1U, 1U, 1U, 8U};
            config.geometry.dims = dims;
            config.max_loaded_chunks = 1U;
            config.background_maintenance = false;
            StoreId id{};
            { ChunkStore initialized(config); id = initialized.store_id(); }
            TxnIntent intent{.state = state, .version = 100U, .entries = {}};
            for (std::size_t i = 0U; i < count; ++i)
                intent.entries.push_back({{static_cast<std::int64_t>(i), 0, dims == 3U ? -1 : 0}, 0U});
            const auto bytes = SerializeTxnIntent(intent, dims, id, count);
            bool lower_write_limit_refused = false;
            try { (void)SerializeTxnIntent(intent, dims, id, 1U); }
            catch (const std::invalid_argument&) { lower_write_limit_refused = true; }
            assert(lower_write_limit_refused);
            const auto path = TxnIntentPath(config.data_dir, intent.version);
            Save(path, bytes);
            assert(LoadRecoveryTxnIntent(path, dims, id) == bytes);
            TxnIntent decoded;
            assert(TryParseTxnIntent(bytes, &decoded, dims, id));
            assert(decoded.entries.size() == count && decoded.state == state);
            {
                auto read_only = config; read_only.access_mode = AccessMode::kReadOnly;
                ChunkStore reader(read_only);
                const auto unwritten = reader.ReadChunkState(intent.entries.back().coord);
                assert(!unwritten.written && unwritten.version == 0U);
                assert(LoadFile(path) == bytes);
            }
            const auto before = Snapshot(directory.path());
            std::ostringstream findings;
            const auto checked = VerifyDataDirectory(directory.path(), findings);
            if (checked.errors != 0U) std::cerr << findings.str();
            assert(checked.errors == 0U);
            assert(Snapshot(directory.path()) == before);
            {
                ChunkStore recovered(config);
                assert(!std::filesystem::exists(path));
                const auto unwritten = recovered.ReadChunkState(intent.entries.back().coord);
                assert(!unwritten.written && unwritten.version == 0U);
            }
        }
    }
}
} // namespace

int main(int argc, char** argv) {
    const auto run = [&](std::string_view name, const auto& test) {
        if (argc == 1 || (argc == 2 && name == argv[1])) test();
    };
    run("paths", CanonicalPaths); run("index", InventoryAndCommittedCuts);
    run("header-only", HeaderOnlyWalMetadata);
    run("retained", RetainedOverlapAndResurrection); run("unlisted", UnlistedAndForgetBindings);
    run("compaction", CompactionDecisions); run("bindings", CheckedBindings); run("archive", ArchiveDescriptors);
    run("rollback", RollbackVisibility);
    run("aliases", ManagedAliases);
    run("identity-cuts", ImageBaseTimeAndConflictingVisibilityCuts);
    run("intent-counts", HistoricalIntentCounts);
}
