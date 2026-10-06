// Readers for pre-2.0 storage artifacts (src/legacy_format.hpp), kept for the
// offline converter. Every fixture is built byte by byte here, so these tests
// do not depend on what the engine writes today.

#include <cassert>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/bit_codec.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/zrle.hpp"
#include "legacy_format.hpp"

namespace {

using Bytes = std::vector<std::uint8_t>;

// 4x4 blocks of 5 bits: 10 payload bytes, 2 presence bytes.
const chunkdb::Geometry kGeometry(chunkdb::GeometryConfig{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 4,
    .chunk_height_blocks = 4,
    .block_bits = 5,
});
constexpr std::size_t kPayloadBytes = 10;
constexpr std::size_t kPresenceBytes = 2;

void Le16(Bytes* out, std::uint16_t v) {
    for (int i = 0; i < 2; ++i) out->push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}
void Le32(Bytes* out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out->push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}
void Le64(Bytes* out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) out->push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}
void Append(Bytes* out, const Bytes& more) { out->insert(out->end(), more.begin(), more.end()); }
void Append(Bytes* out, const std::string& text) { out->insert(out->end(), text.begin(), text.end()); }

// Block (x, y) of the chunk at (cx, cy) with `bits`, as payload + presence.
std::pair<Bytes, Bytes> StateWithBlock(std::uint32_t local_x, std::uint32_t local_y, const std::string& bits) {
    Bytes payload(kPayloadBytes, 0U);
    Bytes presence(kPresenceBytes, 0U);
    const std::size_t index = kGeometry.LocalBlockIndex(local_x, local_y);
    chunkdb::BitCodec::WriteBits(payload, index * kGeometry.config().block_bits, bits);
    chunkdb::BitCodec::WriteBits(presence, index, "1");
    return {payload, presence};
}

// The 52-byte header of image versions 1-3, plus the v4/v5 revision and
// header CRC when `revision` is given.
Bytes ImageHeader(
    std::uint16_t version,
    const chunkdb::ChunkCoord& coord,
    std::uint32_t crc,
    const std::uint64_t* revision) {
    Bytes bytes;
    Append(&bytes, std::string("CHKDATA1"));
    Le16(&bytes, version);
    Le16(&bytes, static_cast<std::uint16_t>(kGeometry.config().block_bits));
    Le32(&bytes, kGeometry.config().chunk_width_blocks);
    Le32(&bytes, kGeometry.config().chunk_height_blocks);
    Le64(&bytes, static_cast<std::uint64_t>(coord.x));
    Le64(&bytes, static_cast<std::uint64_t>(coord.y));
    Le32(&bytes, static_cast<std::uint32_t>(kPayloadBytes));
    Le32(&bytes, crc);
    Le64(&bytes, 1727786400123ULL);  // write_timestamp_ms
    if (revision != nullptr) {
        Le64(&bytes, *revision);
        Le32(&bytes, chunkdb::Crc32(bytes.data(), bytes.size()));
    }
    return bytes;
}

Bytes WalHeader(std::uint16_t version, const chunkdb::ChunkCoord& coord) {
    Bytes bytes;
    Append(&bytes, std::string("CHKWAL02"));
    Le16(&bytes, version);
    Le16(&bytes, static_cast<std::uint16_t>(kGeometry.config().block_bits));
    Le32(&bytes, kGeometry.config().chunk_width_blocks);
    Le32(&bytes, kGeometry.config().chunk_height_blocks);
    Le64(&bytes, static_cast<std::uint64_t>(coord.x));
    Le64(&bytes, static_cast<std::uint64_t>(coord.y));
    return bytes;
}

// A 1.x record: magic, offset, size, CRC over the body only, body.
Bytes DltRecord(std::uint32_t offset, const Bytes& body) {
    Bytes bytes;
    Append(&bytes, std::string("DLT1"));
    Le32(&bytes, offset);
    Le16(&bytes, static_cast<std::uint16_t>(body.size()));
    Le32(&bytes, chunkdb::Crc32(body));
    Append(&bytes, body);
    return bytes;
}

// A 2.0 development (WAL v4) frame of span records.
Bytes Frame1(std::uint64_t revision, const std::vector<std::pair<std::uint32_t, Bytes>>& spans) {
    Bytes records;
    for (const auto& [offset, body] : spans) {
        const std::size_t begin = records.size();
        Le32(&records, offset);
        Le16(&records, static_cast<std::uint16_t>(body.size()));
        Append(&records, body);
        Le32(&records, chunkdb::Crc32(records.data() + begin, records.size() - begin));
    }
    Bytes frame;
    Append(&frame, std::string("FRM1"));
    Le64(&frame, revision);
    Le16(&frame, static_cast<std::uint16_t>(spans.size()));
    Le32(&frame, static_cast<std::uint32_t>(records.size()));
    Le32(&frame, chunkdb::Crc32(frame.data() + 4, 14));
    Append(&frame, records);
    Le32(&frame, chunkdb::Crc32(records));
    return frame;
}

chunkdb::legacy::LegacyWalReplay Replay(const Bytes& wal, Bytes* payload, Bytes* presence) {
    payload->assign(kPayloadBytes, 0U);
    presence->assign(kPresenceBytes, 0U);
    return chunkdb::legacy::ReplayLegacyWal(wal, kGeometry, {0, 0}, payload, presence);
}

std::string BlockBits(const Bytes& payload, std::uint32_t local_x, std::uint32_t local_y) {
    const std::size_t index = kGeometry.LocalBlockIndex(local_x, local_y);
    return chunkdb::BitCodec::ExtractBits(
        payload, index * kGeometry.config().block_bits, kGeometry.config().block_bits);
}

void TestImagesOfEveryVersion() {
    const chunkdb::ChunkCoord coord{-1, -2};
    const auto [payload, presence] = StateWithBlock(3, 1, "11100");
    Bytes state = payload;
    Append(&state, presence);

    // v1: payload only, every block present.
    {
        Bytes image = ImageHeader(1, coord, chunkdb::Crc32(payload), nullptr);
        Append(&image, payload);
        const auto parsed = chunkdb::legacy::ParseLegacyChunkImage(image, kGeometry, coord);
        assert(parsed.version == 1U && parsed.revision == 0U);
        assert(parsed.payload == payload);
        assert(parsed.presence_bitmap == chunkdb::FullPresenceBitmap(kGeometry));
    }
    // v2: payload and presence; v3: the same state zrle-compressed.
    {
        Bytes image = ImageHeader(2, coord, chunkdb::Crc32(state), nullptr);
        Append(&image, state);
        const auto parsed = chunkdb::legacy::ParseLegacyChunkImage(image, kGeometry, coord);
        assert(parsed.version == 2U && parsed.revision == 0U);
        assert(parsed.payload == payload && parsed.presence_bitmap == presence);

        Bytes compressed = ImageHeader(3, coord, chunkdb::Crc32(state), nullptr);
        Append(&compressed, chunkdb::ZrleCompress(state));
        const auto parsed3 = chunkdb::legacy::ParseLegacyChunkImage(compressed, kGeometry, coord);
        assert(parsed3.version == 3U);
        assert(parsed3.payload == payload && parsed3.presence_bitmap == presence);
    }
    // v4 and v5 (2.0 development builds): revision and header CRC.
    {
        const std::uint64_t revision = 16742;
        Bytes image = ImageHeader(4, coord, chunkdb::Crc32(state), &revision);
        Append(&image, state);
        const auto parsed = chunkdb::legacy::ParseLegacyChunkImage(image, kGeometry, coord);
        assert(parsed.version == 4U && parsed.revision == revision);
        assert(parsed.payload == payload && parsed.presence_bitmap == presence);

        Bytes compressed = ImageHeader(5, coord, chunkdb::Crc32(state), &revision);
        Append(&compressed, chunkdb::ZrleCompress(state));
        const auto parsed5 = chunkdb::legacy::ParseLegacyChunkImage(compressed, kGeometry, coord);
        assert(parsed5.version == 5U && parsed5.revision == revision);
        assert(parsed5.payload == payload && parsed5.presence_bitmap == presence);

        // The header CRC covers the revision.
        Bytes damaged = image;
        damaged[52] ^= 0x01U;
        bool rejected = false;
        try {
            (void)chunkdb::legacy::ParseLegacyChunkImage(damaged, kGeometry, coord);
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        assert(rejected);
    }
}

void TestWalStreams() {
    const chunkdb::ChunkCoord coord{0, 0};
    Bytes payload;
    Bytes presence;
    // Block (0,0) = "10101": payload byte 0 holds bits 0..4 LSB-first.
    const Bytes set_00_payload = {0x15};
    const Bytes set_00_presence = {0x01};

    // A v3 record stream (1.x).
    Bytes v3 = WalHeader(3, coord);
    Append(&v3, DltRecord(0, set_00_payload));
    Append(&v3, DltRecord(kPayloadBytes, set_00_presence));
    {
        const auto result = Replay(v3, &payload, &presence);
        assert(result.replayable && !result.tail_truncated_or_corrupt);
        assert(result.legacy_records && result.wal_version == 3U);
        assert(result.applied_records == 2U && result.applied_frames == 0U);
        assert(BlockBits(payload, 0, 0) == "10101" && presence[0] == 0x01);
    }
    // A headerless record stream.
    {
        const Bytes headerless(v3.begin() + 36, v3.end());
        const auto result = Replay(headerless, &payload, &presence);
        assert(result.replayable && result.wal_version == 0U);
        assert(BlockBits(payload, 0, 0) == "10101");
    }
    // Lazy migration: a v4 header after the records, then frames.
    Bytes mixed = v3;
    Append(&mixed, WalHeader(4, coord));
    Append(&mixed, Frame1(7, {{1, {0xE0}}, {kPayloadBytes, {0x03}}}));
    Append(&mixed, Frame1(9, {{0, {0x0A}}}));
    {
        const auto result = Replay(mixed, &payload, &presence);
        assert(result.replayable && !result.tail_truncated_or_corrupt);
        assert(!result.legacy_records);
        assert(result.applied_frames == 2U && result.applied_records == 5U);
        assert(result.revision == 9U);
        assert(payload[0] == 0x0A && payload[1] == 0xE0 && presence[0] == 0x03);
    }
    // A frame torn at the end is dropped as a whole.
    {
        const Bytes torn(mixed.begin(), mixed.end() - 1);
        const auto result = Replay(torn, &payload, &presence);
        assert(result.tail_truncated_or_corrupt);
        assert(result.applied_frames == 1U && result.revision == 7U);
        assert(payload[0] == 0x15 && payload[1] == 0xE0);
    }
    // A v4 stream with its header repeated is replayed as written.
    {
        Bytes repeated = WalHeader(4, coord);
        Append(&repeated, WalHeader(4, coord));
        Append(&repeated, Frame1(3, {{0, {0x15}}, {kPayloadBytes, {0x01}}}));
        const auto result = Replay(repeated, &payload, &presence);
        assert(result.replayable && !result.tail_truncated_or_corrupt);
        assert(result.applied_frames == 1U && result.revision == 3U);
        assert(BlockBits(payload, 0, 0) == "10101");
    }
    // A record body CRC mismatch stops 1.x replay at that record.
    {
        Bytes damaged = v3;
        damaged.back() ^= 0x01U;
        const auto result = Replay(damaged, &payload, &presence);
        assert(result.tail_truncated_or_corrupt);
        assert(result.stop_reason == "record_crc_mismatch");
        assert(result.applied_records == 1U);
    }
}

void TestIntermediateVersionClockRecord() {
    std::uint64_t ceiling = 0;
    Bytes record;
    Le64(&record, 50000);
    assert(chunkdb::legacy::TryParseIntermediateVersionClockRecord(record, &ceiling));
    assert(ceiling == 50000U);
    assert(!chunkdb::legacy::TryParseIntermediateVersionClockRecord(Bytes(8, 0U), &ceiling));
    assert(!chunkdb::legacy::TryParseIntermediateVersionClockRecord(Bytes(16, 1U), &ceiling));
}

}  // namespace

int main() {
    TestImagesOfEveryVersion();
    TestWalStreams();
    TestIntermediateVersionClockRecord();
    return 0;
}
