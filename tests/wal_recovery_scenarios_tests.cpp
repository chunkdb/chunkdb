#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "wal_replay.hpp"

namespace {

constexpr std::size_t kWalHeaderSize = 8U + 2U + 2U + 4U + 4U + 8U + 8U;

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-wal-recovery-" + suffix + "-" + std::to_string(tick));
}

chunkdb::StoreConfig BuildConfig(const std::filesystem::path& data_dir) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 8,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 1'000'000,
        .checkpoint_wal_bytes = 1'000'000,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
    };
}

void AppendBytes(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    assert(out.is_open());
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.flush();
}

void WriteBytes(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    assert(out.is_open());
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.flush();
}

std::string ReadBytes(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in.is_open());

    in.seekg(0, std::ios::end);
    const std::streamoff size = in.tellg();
    assert(size >= 0);

    in.seekg(0, std::ios::beg);
    std::string out(static_cast<std::size_t>(size), '\0');
    if (!out.empty()) {
        in.read(out.data(), static_cast<std::streamsize>(out.size()));
    }
    return out;
}

// ---- v4 frame builders -----------------------------------------------------
//
// The production writer only ever emits well-formed frames, so the format-v2
// replay guards (frame header CRC, record CRC over byte_offset || data_size ||
// body, frame trailer CRC, torn-frame handling, and the legacy record stream)
// are exercised against hand-built byte streams.

void PushLe16(std::vector<std::uint8_t>& out, std::uint16_t value) {
    for (int i = 0; i < 2; ++i) {
        out.push_back(static_cast<std::uint8_t>(value >> (8 * i)));
    }
}

void PushLe32(std::vector<std::uint8_t>& out, std::uint32_t value) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::uint8_t>(value >> (8 * i)));
    }
}

void PushLe64(std::vector<std::uint8_t>& out, std::uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::uint8_t>(value >> (8 * i)));
    }
}

std::vector<std::uint8_t> BuildWalFileHeader(
    const chunkdb::Geometry& geometry,
    const chunkdb::ChunkCoord& coord,
    std::uint16_t version) {
    std::vector<std::uint8_t> out;
    const std::string magic = "CHKWAL02";
    out.insert(out.end(), magic.begin(), magic.end());
    PushLe16(out, version);
    PushLe16(out, static_cast<std::uint16_t>(geometry.config().block_bits));
    PushLe32(out, geometry.config().chunk_width_blocks);
    PushLe32(out, geometry.config().chunk_height_blocks);
    PushLe64(out, static_cast<std::uint64_t>(coord.x));
    PushLe64(out, static_cast<std::uint64_t>(coord.y));
    return out;
}

using Span = std::pair<std::uint32_t, std::vector<std::uint8_t>>;

std::vector<std::uint8_t> BuildFrame(std::uint64_t revision, const std::vector<Span>& spans) {
    std::vector<std::uint8_t> body;
    for (const auto& [offset, bytes] : spans) {
        const std::size_t begin = body.size();
        PushLe32(body, offset);
        PushLe16(body, static_cast<std::uint16_t>(bytes.size()));
        body.insert(body.end(), bytes.begin(), bytes.end());
        PushLe32(body, chunkdb::Crc32(body.data() + begin, body.size() - begin));
    }

    std::vector<std::uint8_t> out;
    const std::string magic = "FRM1";
    out.insert(out.end(), magic.begin(), magic.end());
    PushLe64(out, revision);
    PushLe16(out, static_cast<std::uint16_t>(spans.size()));
    PushLe32(out, static_cast<std::uint32_t>(body.size()));
    PushLe32(out, chunkdb::Crc32(out.data() + 4U, out.size() - 4U));
    out.insert(out.end(), body.begin(), body.end());
    PushLe32(out, chunkdb::Crc32(body.data(), body.size()));
    return out;
}

std::vector<std::uint8_t> BuildLegacyRecord(std::uint32_t offset, const std::vector<std::uint8_t>& body) {
    std::vector<std::uint8_t> out;
    const std::string magic = "DLT1";
    out.insert(out.end(), magic.begin(), magic.end());
    PushLe32(out, offset);
    PushLe16(out, static_cast<std::uint16_t>(body.size()));
    PushLe32(out, chunkdb::Crc32(body));
    out.insert(out.end(), body.begin(), body.end());
    return out;
}

void Append(std::vector<std::uint8_t>& out, const std::vector<std::uint8_t>& more) {
    out.insert(out.end(), more.begin(), more.end());
}

}  // namespace

int main() {
    // Scenario 1: trailing truncated WAL record should be ignored during replay.
    {
        const auto data_dir = TempDataDir("truncated-record");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
            store.SetBlockBits(1, 0, "00001111");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        assert(std::filesystem::exists(wal_path));

        // Add a deliberately incomplete record tail: magic + partial header bytes.
        AppendBytes(wal_path, std::string("DLT1", 4) + std::string("\x01\x02\x03", 3));

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "11110000");
            assert(recovered.GetBlockBits(1, 0) == "00001111");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 2: truncated WAL header should be ignored when a checkpoint image exists.
    {
        const auto data_dir = TempDataDir("truncated-header");
        auto config = BuildConfig(data_dir);
        config.checkpoint_update_interval = 1;

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "10101010");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();

            const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
            assert(std::filesystem::exists(data_path));
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        WriteBytes(wal_path, "CHKW");

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "10101010");
            recovered.SetBlockBits(0, 0, "01010101");
            assert(recovered.GetBlockBits(0, 0) == "01010101");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 3: headerless WAL (record stream starts with DLT1) should replay.
    {
        const auto data_dir = TempDataDir("headerless");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "00001111");
            store.SetBlockBits(1, 0, "11110000");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        const std::string wal = ReadBytes(wal_path);
        assert(wal.size() > kWalHeaderSize);
        WriteBytes(wal_path, wal.substr(kWalHeaderSize));

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "00001111");
            assert(recovered.GetBlockBits(1, 0) == "11110000");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 4: repeated WAL header mid-stream should be skipped during replay.
    {
        const auto data_dir = TempDataDir("repeated-header");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "10101010");
            store.SetBlockBits(0, 0, "01010101");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        const std::string wal = ReadBytes(wal_path);
        assert(wal.size() > kWalHeaderSize);

        // Duplicate the valid header at the beginning of the record stream.
        std::string duplicated;
        duplicated.reserve(wal.size() + kWalHeaderSize);
        duplicated.append(wal.data(), static_cast<std::ptrdiff_t>(kWalHeaderSize));
        duplicated.append(wal.data(), static_cast<std::ptrdiff_t>(kWalHeaderSize));
        duplicated.append(
            wal.data() + static_cast<std::ptrdiff_t>(kWalHeaderSize),
            static_cast<std::ptrdiff_t>(wal.size() - kWalHeaderSize));
        WriteBytes(wal_path, duplicated);

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "01010101");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 5: read-only replay must not mutate on-disk state.
    {
        const auto data_dir = TempDataDir("read-only-non-mutating");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11001100");
            store.SetBlockBits(1, 0, "00110011");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        assert(!std::filesystem::exists(data_path));
        assert(std::filesystem::exists(wal_path));

        const std::string wal_before = ReadBytes(wal_path);
        const auto tmp_artifact =
            data_path.parent_path() /
            (data_path.filename().string() + ".tmp.999999.stale-artifact");
        WriteBytes(tmp_artifact, "orphan-temp");
        assert(std::filesystem::exists(tmp_artifact));

        auto read_only = config;
        read_only.access_mode = chunkdb::AccessMode::kReadOnly;

        {
            chunkdb::ChunkStore store(read_only);
            assert(store.GetBlockBits(0, 0) == "11001100");
            assert(store.GetBlockBits(1, 0) == "00110011");
        }

        assert(!std::filesystem::exists(data_path));
        assert(std::filesystem::exists(wal_path));
        assert(ReadBytes(wal_path) == wal_before);
        assert(std::filesystem::exists(tmp_artifact));

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 6: writable replay must not compact WAL-backed state on load.
    {
        const auto data_dir = TempDataDir("writable-deferred-compaction");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "10101010");
            store.SetBlockBits(1, 0, "01010101");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        assert(!std::filesystem::exists(data_path));
        assert(std::filesystem::exists(wal_path));
        const std::string wal_before = ReadBytes(wal_path);

        {
            chunkdb::ChunkStore store(config);
            assert(store.GetBlockBits(0, 0) == "10101010");
            assert(store.GetBlockBits(1, 0) == "01010101");
        }

        assert(!std::filesystem::exists(data_path));
        assert(std::filesystem::exists(wal_path));
        assert(ReadBytes(wal_path) == wal_before);

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 7: low-value deferred compaction does not force a checkpoint on eviction.
    {
        const auto data_dir = TempDataDir("writable-eviction-no-compaction");
        auto config = BuildConfig(data_dir);
        config.max_loaded_chunks = 1;

        chunkdb::ChunkCoord coord_a;
        chunkdb::ChunkCoord coord_b;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
            coord_a = store.geometry().BlockToChunk(0, 0);
            coord_b = store.geometry().BlockToChunk(
                static_cast<std::int64_t>(store.geometry().config().chunk_width_blocks),
                0);
            geometry = store.geometry();
        }

        const auto data_path_a = chunkdb::ChunkDataPath(data_dir, geometry, coord_a);
        const auto wal_path_a = chunkdb::ChunkWalPath(data_dir, geometry, coord_a);
        assert(!std::filesystem::exists(data_path_a));
        assert(std::filesystem::exists(wal_path_a));

        {
            chunkdb::ChunkStore store(config);
            assert(store.GetBlockBits(0, 0) == "11110000");
            assert(store.GetBlockBits(
                       static_cast<std::int64_t>(geometry.config().chunk_width_blocks),
                       0) == "00000000");
        }

        assert(!std::filesystem::exists(data_path_a));
        assert(std::filesystem::exists(wal_path_a));

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 8: deferred compaction still happens on eviction when checkpoint thresholds require it.
    {
        const auto data_dir = TempDataDir("writable-eviction-threshold-compaction");
        const auto initial_config = BuildConfig(data_dir);
        auto eviction_config = BuildConfig(data_dir);
        eviction_config.max_loaded_chunks = 1;
        eviction_config.checkpoint_wal_bytes = 1;

        chunkdb::ChunkCoord coord_a;
        chunkdb::ChunkCoord coord_b;
        chunkdb::Geometry geometry(initial_config.geometry);

        {
            chunkdb::ChunkStore store(initial_config);
            store.SetBlockBits(0, 0, "11110000");
            coord_a = store.geometry().BlockToChunk(0, 0);
            coord_b = store.geometry().BlockToChunk(
                static_cast<std::int64_t>(store.geometry().config().chunk_width_blocks),
                0);
            geometry = store.geometry();
        }

        const auto data_path_a = chunkdb::ChunkDataPath(data_dir, geometry, coord_a);
        const auto wal_path_a = chunkdb::ChunkWalPath(data_dir, geometry, coord_a);
        assert(!std::filesystem::exists(data_path_a));
        assert(std::filesystem::exists(wal_path_a));

        {
            chunkdb::ChunkStore store(eviction_config);
            assert(store.GetBlockBits(0, 0) == "11110000");
            assert(store.GetBlockBits(
                       static_cast<std::int64_t>(geometry.config().chunk_width_blocks),
                       0) == "00000000");
        }

        assert(std::filesystem::exists(data_path_a));
        assert(!std::filesystem::exists(wal_path_a));

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 9: explicit presence survives WAL replay and unset remains distinct from zero bits.
    {
        const auto data_dir = TempDataDir("exists-vs-zero");
        const auto config = BuildConfig(data_dir);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "00000000");
            assert(store.BlockExists(0, 0));
            assert(store.GetBlockBits(0, 0) == "00000000");
        }

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.BlockExists(0, 0));
            assert(recovered.GetBlockBits(0, 0) == "00000000");
            recovered.UnsetBlock(0, 0);
            assert(!recovered.BlockExists(0, 0));
            assert(recovered.GetBlockBits(0, 0) == "00000000");
        }

        {
            chunkdb::ChunkStore recovered(config);
            assert(!recovered.BlockExists(0, 0));
            assert(recovered.GetBlockBits(0, 0) == "00000000");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 10: chunk-level state survives WAL replay and explicit zero chunks remain distinct from absence.
    {
        const auto data_dir = TempDataDir("chunk-exists-vs-zero");
        const auto config = BuildConfig(data_dir);

        {
            chunkdb::ChunkStore store(config);
            const std::string zero_chunk(store.geometry().ChunkPayloadBits(), '0');
            const std::string sparse_presence = "1000000000000001";
            const std::string sparse_payload = "11111111" + std::string(112, '0') + "00000000";

            store.SetChunkBits(0, 0, zero_chunk);
            assert(store.ChunkExists(0, 0));
            assert(store.BlockExists(0, 0));
            assert(store.GetBlockBits(0, 0) == "00000000");

            store.SetChunkStateBits(1, 0, sparse_payload, sparse_presence);
            assert(store.ChunkExists(1, 0));
            assert(store.BlockExists(4, 0));
            assert(!store.BlockExists(5, 0));
            assert(store.GetBlockBits(4, 0) == "11111111");
            assert(store.GetBlockBits(5, 0) == "00000000");
        }

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.ChunkExists(0, 0));
            assert(recovered.BlockExists(0, 0));
            assert(recovered.GetChunkBits(0, 0) == std::string(recovered.geometry().ChunkPayloadBits(), '0'));
            assert(recovered.GetChunkStateBits(0, 0) ==
                   std::string(recovered.geometry().ChunkPayloadBits(), '0') + "|" +
                   std::string(recovered.geometry().ChunkBlockCount(), '1'));

            assert(recovered.ChunkExists(1, 0));
            assert(recovered.GetChunkStateBits(1, 0) ==
                   "11111111" + std::string(112, '0') + "00000000|1000000000000001");
            assert(recovered.BlockExists(4, 0));
            assert(!recovered.BlockExists(5, 0));
            assert(recovered.GetBlockBits(4, 0) == "11111111");
            assert(recovered.GetBlockBits(5, 0) == "00000000");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario: the partial header-CRC mitigation (CDB-DEF-1). A delta record
    // whose corrupted byte_offset makes it straddle the payload/presence
    // region boundary must be rejected (replay stops) rather than applying a
    // CRC-valid body across the boundary. Geometry here: 4x4 blocks x 8 bits
    // = 16 payload bytes, 2 presence bytes; boundary at offset 16.
    {
        const auto data_dir = TempDataDir("record-region-straddle");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            // Write the last block so the payload delta lands near the
            // boundary (block 15 → byte 15, data_size 1).
            store.SetBlockBits(3, 3, "11110000");
            coord = store.geometry().BlockToChunk(3, 3);
            geometry = store.geometry();
        }
        const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
        assert(payload_bytes == 16U);

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        auto wal = ReadBytes(wal_path);
        constexpr std::size_t kFrameHeaderSize = 4U + 8U + 2U + 4U + 4U;
        assert(wal.size() > kWalHeaderSize + kFrameHeaderSize + 6U);
        // Corrupt the first record's byte_offset (3 instead of its real
        // offset 15) inside the first v4 frame, leaving the body intact. The
        // 1.x body-only CRC could not see this; the v4 record CRC covers the
        // offset, so replay rejects the frame instead of mis-applying it.
        const std::size_t offset_field = kWalHeaderSize + kFrameHeaderSize;  // byte_offset u32
        wal[offset_field] = static_cast<char>(3);
        wal[offset_field + 1] = 0;
        wal[offset_field + 2] = 0;
        wal[offset_field + 3] = 0;
        WriteBytes(wal_path, wal);

        {
            chunkdb::ChunkStore recovered(config);
            // Straddling record rejected → its write is absent, not
            // mis-applied across the boundary.
            assert(!recovered.BlockExists(3, 3));
            assert(recovered.GetBlockBits(3, 3) == "00000000");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 8: format-v2 frame guards, exercised directly against ReplayWal
    // so a multi-record frame can be torn at *every* byte boundary cheaply.
    {
        // No store and no data directory: this scenario drives replay directly.
        const chunkdb::Geometry geometry(BuildConfig("").geometry);
        const chunkdb::ChunkCoord coord{0, 0};
        const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
        const std::size_t presence_bytes = (geometry.ChunkBlockCount() + 7U) / 8U;

        // Frame A: one record. Frame B: three records, one of them in the
        // presence region — a multi-record mutation, which 1.x could only
        // replay as a prefix.
        const auto frame_a = BuildFrame(7U, {{0U, {0xAA}}});
        const auto frame_b = BuildFrame(
            9U,
            {{1U, {0xBB, 0xBB}},
             {3U, {0xCC}},
             {static_cast<std::uint32_t>(payload_bytes), {0x0F}}});

        const auto header = BuildWalFileHeader(geometry, coord, chunkdb::kWalFileVersion);
        std::vector<std::uint8_t> wal = header;
        Append(wal, frame_a);
        const std::size_t frame_b_begin = wal.size();
        Append(wal, frame_b);

        const auto replay_into = [&](const std::vector<std::uint8_t>& bytes,
                                     std::vector<std::uint8_t>* payload,
                                     std::vector<std::uint8_t>* presence) {
            payload->assign(payload_bytes, 0U);
            presence->assign(presence_bytes, 0U);
            return chunkdb::ReplayWal(bytes, geometry, coord, payload, presence);
        };

        std::vector<std::uint8_t> payload;
        std::vector<std::uint8_t> presence;

        // Intact stream: both frames apply, and the revision is the last one.
        {
            const auto result = replay_into(wal, &payload, &presence);
            assert(result.replayable);
            assert(!result.tail_truncated_or_corrupt);
            assert(result.applied_frames == 2);
            assert(result.applied_records == 4);
            assert(result.revision == 9U);
            assert(!result.legacy_records);
            assert(result.wal_version == chunkdb::kWalFileVersion);
            assert(payload[0] == 0xAA && payload[1] == 0xBB && payload[2] == 0xBB);
            assert(payload[3] == 0xCC && presence[0] == 0x0F);
        }

        // The state frame A alone produces; every torn prefix must match it.
        std::vector<std::uint8_t> after_a_payload;
        std::vector<std::uint8_t> after_a_presence;
        {
            std::vector<std::uint8_t> only_a = header;
            Append(only_a, frame_a);
            const auto result = replay_into(only_a, &after_a_payload, &after_a_presence);
            assert(result.applied_frames == 1);
            assert(result.revision == 7U);
        }

        // Torn at every byte boundary inside frame B: nothing of B is applied.
        for (std::size_t cut = frame_b_begin + 1U; cut < wal.size(); ++cut) {
            const std::vector<std::uint8_t> torn(wal.begin(), wal.begin() + static_cast<std::ptrdiff_t>(cut));
            const auto result = replay_into(torn, &payload, &presence);
            assert(result.replayable);
            assert(result.tail_truncated_or_corrupt);
            assert(result.applied_frames == 1);
            assert(result.applied_records == 1);
            assert(result.revision == 7U);
            assert(payload == after_a_payload);
            assert(presence == after_a_presence);
        }

        // Field offsets inside a frame: magic 0..3, revision 4..11,
        // record_count 12..13, body_size 14..17, header CRC 18..21, then the
        // records (byte_offset, data_size, body, record CRC), then the 4-byte
        // frame CRC.
        constexpr std::size_t kFrameHeader = 22U;

        // Re-signs the frame CRC over the (possibly corrupted) record bytes of
        // frame B, so a record-level corruption reaches the per-record CRC
        // instead of stopping at the frame CRC.
        const auto resign_frame_crc = [&](std::vector<std::uint8_t>& bytes) {
            const std::size_t body_begin = frame_b_begin + kFrameHeader;
            const std::size_t body_size = frame_b.size() - kFrameHeader - 4U;
            const std::uint32_t crc = chunkdb::Crc32(bytes.data() + body_begin, body_size);
            for (int i = 0; i < 4; ++i) {
                bytes[body_begin + body_size + static_cast<std::size_t>(i)] =
                    static_cast<std::uint8_t>(crc >> (8 * i));
            }
        };

        const auto expect_frame_b_rejected = [&](std::vector<std::uint8_t> bytes,
                                                 const char* reason) {
            const auto result = replay_into(bytes, &payload, &presence);
            assert(result.replayable);
            assert(result.tail_truncated_or_corrupt);
            assert(result.stop_reason == reason);
            assert(result.applied_frames == 1);
            assert(result.applied_records == 1);
            assert(result.revision == 7U);
            assert(payload == after_a_payload);
            assert(presence == after_a_presence);
        };

        // Frame magic and every header field are covered by the header CRC.
        for (const auto& [offset, reason] :
             std::vector<std::pair<std::size_t, const char*>>{
                 {0U, "frame_magic_mismatch"},        // magic
                 {5U, "frame_header_crc_mismatch"},   // revision
                 {12U, "frame_header_crc_mismatch"},  // record_count
                 {14U, "frame_header_crc_mismatch"},  // body_size
                 {18U, "frame_header_crc_mismatch"},  // the header CRC itself
             }) {
            auto corrupt = wal;
            corrupt[frame_b_begin + offset] ^= 0x01U;
            expect_frame_b_rejected(corrupt, reason);
        }

        // Any corruption of the record bytes trips the frame CRC first, and
        // the frame trailer itself is the last field the frame CRC guards.
        for (const std::size_t offset :
             {kFrameHeader, kFrameHeader + 4U, kFrameHeader + 6U, kFrameHeader + 7U,
              frame_b.size() - 1U}) {
            auto corrupt = wal;
            corrupt[frame_b_begin + offset] ^= 0x01U;
            expect_frame_b_rejected(corrupt, "frame_crc_mismatch");
        }

        // CDB-DEF-1: with the frame CRC re-signed, the per-record CRC is the
        // only thing left, and it covers byte_offset and data_size as well as
        // the body. The 1.x body-only CRC applied such a record at the wrong
        // offset; every one of these must now be rejected.
        for (const std::size_t offset :
             {kFrameHeader,       // byte_offset
              kFrameHeader + 4U,  // data_size
              kFrameHeader + 6U,  // body
              kFrameHeader + 8U}) {  // the record CRC itself
            auto corrupt = wal;
            corrupt[frame_b_begin + offset] ^= 0x01U;
            resign_frame_crc(corrupt);
            expect_frame_b_rejected(corrupt, "record_crc_mismatch");
        }

        // A record re-pointed across the payload/presence boundary with both
        // CRCs re-signed is still stopped by the shape guard.
        {
            auto straddle = wal;
            const std::size_t record_at = frame_b_begin + kFrameHeader;
            straddle[record_at] = static_cast<std::uint8_t>(payload_bytes - 1U);
            straddle[record_at + 1] = 0U;
            straddle[record_at + 2] = 0U;
            straddle[record_at + 3] = 0U;
            const std::uint32_t record_crc = chunkdb::Crc32(straddle.data() + record_at, 8U);
            for (int i = 0; i < 4; ++i) {
                straddle[record_at + 8U + static_cast<std::size_t>(i)] =
                    static_cast<std::uint8_t>(record_crc >> (8 * i));
            }
            resign_frame_crc(straddle);
            expect_frame_b_rejected(straddle, "record_region_straddle");
        }

        // A 1.x record stream still replays, and is reported as legacy so the
        // writer knows it must emit a fresh v4 header before appending frames.
        {
            std::vector<std::uint8_t> legacy =
                BuildWalFileHeader(geometry, coord, chunkdb::kWalFileVersionV3);
            Append(legacy, BuildLegacyRecord(0U, {0x11}));
            Append(legacy, BuildLegacyRecord(static_cast<std::uint32_t>(payload_bytes), {0x01}));
            const auto result = replay_into(legacy, &payload, &presence);
            assert(result.replayable);
            assert(!result.tail_truncated_or_corrupt);
            assert(result.legacy_records);
            assert(result.applied_frames == 0);
            assert(result.applied_records == 2);
            assert(result.revision == 0U);
            assert(payload[0] == 0x11 && presence[0] == 0x01);

            // Mixed WAL: the legacy records above, then a mid-stream v4 header
            // and frames. This is exactly what lazy migration writes.
            std::vector<std::uint8_t> mixed = legacy;
            Append(mixed, BuildWalFileHeader(geometry, coord, chunkdb::kWalFileVersion));
            Append(mixed, frame_a);
            Append(mixed, frame_b);
            const auto mixed_result = replay_into(mixed, &payload, &presence);
            assert(mixed_result.replayable);
            assert(!mixed_result.tail_truncated_or_corrupt);
            assert(!mixed_result.legacy_records);
            assert(mixed_result.applied_records == 6);
            assert(mixed_result.applied_frames == 2);
            assert(mixed_result.revision == 9U);
            assert(payload[0] == 0xAA && payload[1] == 0xBB && presence[0] == 0x0F);

            // A frame torn at the very end of a mixed stream keeps the legacy
            // prefix and frame A, and drops frame B entirely.
            std::vector<std::uint8_t> mixed_torn(mixed.begin(), mixed.end() - 1);
            const auto torn_result = replay_into(mixed_torn, &payload, &presence);
            assert(torn_result.tail_truncated_or_corrupt);
            assert(torn_result.applied_frames == 1);
            assert(torn_result.applied_records == 3);
            assert(torn_result.revision == 7U);
            assert(payload[1] == 0x00 && presence[0] == 0x01);
        }
    }

    return 0;
}
