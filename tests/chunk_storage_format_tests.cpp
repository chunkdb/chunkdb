#include <cassert>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "wal_replay.hpp"
#include "wal_writer.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;

void Reject(const std::function<void()>& call, std::string_view reason = {}) {
    try { call(); }
    catch (const std::runtime_error& error) {
        if (std::string_view(error.what()).find(reason) == std::string_view::npos)
            std::fprintf(stderr, "expected '%.*s', got '%s'\n", static_cast<int>(reason.size()), reason.data(), error.what());
        assert(std::string_view(error.what()).find(reason) != std::string_view::npos);
        return;
    } catch (const std::invalid_argument& error) {
        if (std::string_view(error.what()).find(reason) == std::string_view::npos)
            std::fprintf(stderr, "expected '%.*s', got '%s'\n", static_cast<int>(reason.size()), reason.data(), error.what());
        assert(std::string_view(error.what()).find(reason) != std::string_view::npos);
        return;
    }
    assert(false && "expected format refusal");
}

void At(Bytes* bytes, std::size_t at, std::uint64_t value, std::size_t width) {
    for (std::size_t i = 0U; i < width; ++i)
        (*bytes)[at + i] = static_cast<std::uint8_t>(value >> (8U * i));
}
void Save(const std::filesystem::path& path, const Bytes& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(output.good());
}
Geometry Shape(std::uint8_t dims) {
    return Geometry(GeometryConfig{.large_chunk_width_chunks = 5, .large_chunk_height_chunks = 6,
        .chunk_width_blocks = 2, .chunk_height_blocks = 3, .block_bits = 8,
        .dims = dims, .chunk_depth_blocks = dims == 3 ? 4U : 1U,
        .large_chunk_depth_chunks = dims == 3 ? 7U : 1U});
}
const StoreId kEpoch{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
constexpr FeatureFlags kFeatures{.incompat = kChunkModelFormatBit};

void ManifestDomainsAndAdmission() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        const auto geometry = Shape(dims);
        auto blocks = geometry.layout().schema();
        TableSchema chunks;
        Column status;
        status.name = "status";
        status.type = {ColumnKind::kUnsigned, 8};
        chunks = AddChunkColumn(chunks, status);
        blocks = AdvanceOtherDomain(blocks, ColumnDomain::kBlock);
        const StoreManifest manifest{.geometry = geometry.config(), .store_id = kEpoch, .options = {},
            .schema = blocks, .chunk_schema = chunks};
        const auto bytes = SerializeStoreManifest(manifest);
        assert(ReadLe16(bytes, 4U) == 5 && bytes[20] == dims);
        assert(ReadLe32(bytes, 24U) == 5 && ReadLe32(bytes, 28U) == 6);
        assert(ReadLe32(bytes, 32U) == (dims == 3 ? 7U : 1U));
        assert(ReadLe32(bytes, 36U) == 2 && ReadLe32(bytes, 40U) == 3);
        assert(ReadLe32(bytes, 44U) == (dims == 3 ? 4U : 1U));
        assert(std::equal(kEpoch.begin(), kEpoch.end(), bytes.begin() + 48U));
        assert(ReadLe32(bytes, 64U) == 0);
        const auto parsed = ParseStoreManifest(bytes);
        assert(parsed.geometry.dims == dims && parsed.schema == blocks && *parsed.chunk_schema == chunks);
        for (std::size_t cut = 0U; cut < bytes.size(); ++cut)
            Reject([&] { (void)ParseStoreManifest(Bytes(bytes.begin(), bytes.begin() + cut)); });
        for (std::size_t at = 0U; at < bytes.size(); ++at) {
            auto changed = bytes;
            changed[at] ^= 0x80U;
            Reject([&] { (void)ParseStoreManifest(changed); });
        }
        auto changed = bytes;
        changed[21] = 1U;
        At(&changed, changed.size() - 4U, Crc32(changed.data(), changed.size() - 4U), 4U);
        Reject([&] { (void)ParseStoreManifest(changed); }, "reserved");
        for (std::uint16_t old = 1U; old < 5U; ++old) {
            changed = bytes;
            At(&changed, 4U, old, 2U);
            Reject([&] { (void)ParseStoreManifest(changed); }, "earlier development build");
        }
        auto misaligned = manifest;
        misaligned.chunk_schema = TableSchema{};
        Reject([&] { (void)SerializeStoreManifest(misaligned); }, "revisions differ");
    }
    const DataDirManifest directory{.data_dir_id = kEpoch, .options = {}};
    auto bytes = SerializeDataDirManifest(directory);
    assert(ReadLe16(bytes, 4U) == 2U && ReadLe32(bytes, 8U) == kChunkModelFormatBit);
    assert(ParseDataDirManifest(bytes).data_dir_id == kEpoch);
    At(&bytes, 4U, 1U, 2U);
    Reject([&] { (void)ParseDataDirManifest(bytes); }, "earlier development build");
}

void ImageAndWalRuntimeReaders() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        const auto geometry = Shape(dims);
        const ChunkCoord coord{-3, 4, dims == 3 ? -5 : 0};
        Bytes payload(geometry.ChunkPayloadBytes(), 13U);
        const auto presence = FullPresenceBitmap(geometry);
        for (const auto compression : {CheckpointCompression::kNone, CheckpointCompression::kZrle}) {
            const auto bytes = SerializeChunkImage(geometry, coord, payload, presence, compression, 7U, 700U, kEpoch);
            const auto image = ParseChunkImage(bytes, geometry, coord, kEpoch, kFeatures);
            assert(image.payload == payload && image.presence_bitmap == presence);
            assert(image.revision == 7 && image.commit_time_ms == 700);
            test::ScopedTempDir directory("chunkdb-image-bounds");
            const auto path = directory.path() / "image.chk";
            Save(path, bytes);
            assert(LoadChunkImageFile(path, geometry, coord, kEpoch, kFeatures) == bytes);
            const StoreId replacement{9};
            const auto rewritten = RewriteChunkImageStoreId(bytes, geometry, coord, kEpoch, replacement, kFeatures);
            assert(ParseChunkImage(rewritten, geometry, coord, replacement, kFeatures).payload == payload);
            Reject([&] { (void)ParseChunkImage(bytes, geometry, {coord.x, coord.y + 1, coord.z}, kEpoch, kFeatures); }, "chunk");
            std::filesystem::resize_file(path, kMaxChunkImageFileBytes + 1U);
            Reject([&] { (void)LoadChunkImageFile(path, geometry, coord, kEpoch, kFeatures); }, "limit");
        }
        auto wal = BuildWalHeader(coord, kEpoch, kFeatures, dims);
        const auto header_size = wal.size();
        assert(header_size == 48U + 8U * dims);
        for (std::size_t cut = 0U; cut < header_size; ++cut) {
            Bytes recovered_payload(geometry.ChunkPayloadBytes(), 0U), recovered_presence(presence.size(), 0U);
            ChunkVars vars;
            const auto replay = ReplayWal(Bytes(wal.begin(), wal.begin() + cut), geometry, coord, kEpoch,
                kFeatures, 0, 0, &recovered_payload, &recovered_presence, &vars);
            assert(replay.torn_creation && !replay.replayable && replay.applied_frames == 0U);
        }
        WalFrameBuilder frame(&wal, geometry.layout().schema().version);
        frame.AppendSpan(0U, payload.data(), payload.size());
        frame.AppendSpan(static_cast<std::uint32_t>(payload.size()), presence.data(), presence.size());
        (void)frame.Finish(8U, 800U);
        Bytes recovered_payload(geometry.ChunkPayloadBytes(), 0U), recovered_presence(presence.size(), 0U);
        ChunkVars vars;
        const auto replay = ReplayWal(wal, geometry, coord, kEpoch, kFeatures, 0, 0,
            &recovered_payload, &recovered_presence, &vars);
        assert(replay.replayable && replay.valid_end == wal.size() && replay.applied_frames == 1U);
        assert(replay.revision == 8 && replay.commit_time_ms == 800);
        assert(recovered_payload == payload && recovered_presence == presence);
        const StoreId replacement{9};
        const auto rewritten = RewriteWalStoreId(wal, coord, kEpoch, replacement, kFeatures, dims);
        ValidateWalHeader(rewritten, coord, replacement, kFeatures, dims);
        assert(std::equal(wal.begin() + header_size, wal.end(), rewritten.begin() + header_size));
        auto old = wal;
        At(&old, 8U, 1U, 2U);
        Reject([&] { (void)ReplayWal(old, geometry, coord, kEpoch, kFeatures, 0, 0,
            &recovered_payload, &recovered_presence, &vars); }, "earlier development build");
    }
}

void EarlierManifestLeavesDirectoryUntouched() {
    test::ScopedTempDir directory("chunkdb-earlier-manifest");
    const auto geometry = Shape(2);
    auto bytes = SerializeStoreManifest(StoreManifest{.geometry = geometry.config(), .store_id = kEpoch, .options = {},
        .schema = geometry.layout().schema()});
    At(&bytes, 4U, 4U, 2U);
    Save(StoreManifestPath(directory.path()), bytes);
    StoreConfig config;
    config.data_dir = directory.path();
    config.geometry = geometry.config();
    Reject([&] { ChunkStore store(config); }, "earlier development build");
    assert(LoadFile(StoreManifestPath(directory.path())) == bytes);
    std::size_t count = 0U;
    for (const auto& item : std::filesystem::directory_iterator(directory.path())) {
        assert(item.path().filename() == kStoreManifestFileName);
        ++count;
    }
    assert(count == 1U);
}

void MetadataImageWalAndSchemaHistory() {
    const auto shape = Shape(3);
    TableSchema chunks;
    chunks.next_column_id = 3;
    chunks.columns = {
        {.id = 1, .name = "owner", .type = {ColumnKind::kUnsigned, 8}, .required = true, .default_value = {}},
        {.id = 2, .name = "label", .type = {ColumnKind::kText, 32}, .nullable = true, .default_value = {}},
    };
    const Geometry geometry(shape.config(), shape.layout().schema(), chunks);
    assert(geometry.chunk_columns_require_initialization() && geometry.default_chunk_columns());
    const auto metadata = AssignChunkColumns(chunks, nullptr,
        {{"owner", std::uint64_t{9}}, {"label", std::string("stored")}}, true);
    const ChunkCoord coord{-3, 4, -5};
    Bytes payload(geometry.ChunkPayloadBytes(), 0U), presence(ChunkPresenceBitmapBytes(geometry), 0U);
    ChunkVars vars;
    const auto image_bytes = SerializeChunkImage(geometry, coord, payload, presence,
        CheckpointCompression::kZrle, 7U, 700U, kEpoch, &vars, &metadata);
    const auto image = ParseChunkImage(image_bytes, geometry, coord, kEpoch, kFeatures);
    assert(image.chunk_columns && image.chunk_columns->bytes == metadata.bytes);
    assert(image.chunk_columns->variable_bytes == 18U);
    assert(ChunkPopulated(geometry, image.presence_bitmap, image.chunk_columns.get()));
    assert(!ChunkPopulated(geometry, presence, geometry.default_chunk_columns().get()));
    Reject([&] { (void)SerializeChunkImage(geometry, coord, payload, presence,
        CheckpointCompression::kNone, 7U, 700U, kEpoch); }, "missing its chunk columns");
    auto later_chunks = RenameChunkColumn(chunks, "label", "note");
    auto later_blocks = AdvanceOtherDomain(geometry.layout().schema(), ColumnDomain::kBlock);
    const Geometry later(geometry.config(), later_blocks, later_chunks);
    auto historical = ParseChunkImage(image_bytes, later, coord, kEpoch, kFeatures);
    BringChunkColumnsToSchema(later, historical.schema_version, later.chunk_schema().version, &historical.chunk_columns);
    assert(historical.chunk_columns->bytes == metadata.bytes);
    assert(std::get<std::string>(DecodeChunkColumns(later_chunks, historical.chunk_columns->bytes)[1]) == "stored");

    auto wal = BuildWalHeader(coord, kEpoch, kFeatures, 3);
    {
        WalFrameBuilder frame(&wal, 1U);
        frame.AppendChunkColumns(metadata);
        bool duplicate_rejected = false;
        try { frame.AppendChunkColumns(metadata); }
        catch (const std::logic_error&) { duplicate_rejected = true; }
        assert(duplicate_rejected);
        (void)frame.Finish(8U, 800U);
    }
    std::shared_ptr<const ChunkColumnState> recovered;
    const auto replay = ReplayWal(wal, geometry, coord, kEpoch, kFeatures, 0U, 0U,
        &payload, &presence, &vars, nullptr, &recovered);
    assert(replay.applied_frames == 1U && !replay.tail_truncated_or_corrupt);
    assert(recovered && recovered->bytes == metadata.bytes && replay.commit_time_ms == 800U);
    assert(ChunkPopulated(geometry, presence, recovered.get()));

    auto invalid = BuildWalHeader(coord, kEpoch, kFeatures, 3);
    {
        WalFrameBuilder frame(&invalid, 1U);
        const Bytes value{1U};
        frame.AppendSpan(0U, value.data(), value.size());
        (void)frame.Finish(8U, 800U);
    }
    recovered.reset();
    const auto refused = ReplayWal(invalid, geometry, coord, kEpoch, kFeatures, 0U, 0U,
        &payload, &presence, &vars, nullptr, &recovered);
    assert(refused.applied_frames == 0U && refused.tail_truncated_or_corrupt && !refused.stopped_at_crash_tail);
    assert(refused.stop_reason == "record_chunk_columns_required" && !recovered);
}
}

int main() {
    ManifestDomainsAndAdmission();
    ImageAndWalRuntimeReaders();
    EarlierManifestLeavesDirectoryUntouched();
    MetadataImageWalAndSchemaHistory();
}
