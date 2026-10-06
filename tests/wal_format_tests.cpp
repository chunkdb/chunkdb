// The 2.0 WAL (docs/STORAGE_FORMAT.md Section 4): a checksummed file header
// and frames with a commit time, optional TLV fields (TAG) and typed records.
// Covers the frame guards byte by byte, and the loader rules that keep a
// writer from appending where replay never reaches.

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "test_utils.hpp"
#include "wal_replay.hpp"
#include "wal_writer.hpp"

namespace {

using Bytes = std::vector<std::uint8_t>;
using chunkdb::test::ScopedTempDir;

// 4x4 blocks of 8 bits: 16 payload bytes, 2 presence bytes.
const chunkdb::GeometryConfig kGeometryConfig{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 4,
    .chunk_height_blocks = 4,
    .block_bits = 8,
};
const chunkdb::Geometry kGeometry(kGeometryConfig);
constexpr std::size_t kPayloadBytes = 16;
constexpr std::size_t kPresenceBytes = 2;
const chunkdb::StoreId kStoreId = {9, 8, 7, 6, 5, 4, 3, 2, 1, 1, 2, 3, 4, 5, 6, 7};
const chunkdb::FeatureFlags kNoFeatures{.incompat = 0, .ro_compat = 0, .compat = 0};
const chunkdb::ChunkCoord kCoord{0, 0};
constexpr std::size_t kFrameHeader = chunkdb::kWalFrameFixedHeaderSize + chunkdb::kWalFrameHeaderCrcSize;

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
void PutLe32(Bytes* bytes, std::size_t at, std::uint32_t v) {
    for (std::size_t i = 0; i < 4; ++i) (*bytes)[at + i] = static_cast<std::uint8_t>(v >> (8 * i));
}

struct Record {
    std::uint8_t type;
    Bytes body;
};

Record Span(std::uint32_t offset, const Bytes& data) {
    Bytes body;
    Le32(&body, offset);
    Append(&body, data);
    return {chunkdb::kWalRecordSpan, body};
}

// A frame built by hand, so every field can be set to anything.
Bytes BuildFrame(
    std::uint64_t revision,
    std::uint64_t commit_time_ms,
    const Bytes& tlv,
    const std::vector<Record>& records,
    std::uint16_t frame_flags = 0) {
    Bytes body;
    for (const auto& record : records) {
        body.push_back(record.type);
        Le32(&body, static_cast<std::uint32_t>(record.body.size()));
        Append(&body, record.body);
    }
    Bytes frame = {'F', 'R', 'M', '2'};
    Le64(&frame, revision);
    Le64(&frame, commit_time_ms);
    Le16(&frame, frame_flags);
    Le16(&frame, static_cast<std::uint16_t>(tlv.size()));
    Le32(&frame, static_cast<std::uint32_t>(records.size()));
    Le32(&frame, static_cast<std::uint32_t>(body.size()));
    Append(&frame, tlv);
    Le32(&frame, chunkdb::Crc32(frame.data() + 4, frame.size() - 4));
    Append(&frame, body);
    Le32(&frame, chunkdb::Crc32(body));
    return frame;
}

Bytes Tlv(std::uint16_t type, const Bytes& value) {
    Bytes tlv;
    Le16(&tlv, type);
    Le16(&tlv, static_cast<std::uint16_t>(value.size()));
    Append(&tlv, value);
    return tlv;
}

Bytes Header(const chunkdb::FeatureFlags& features = kNoFeatures) {
    return chunkdb::BuildWalHeader(kCoord, kStoreId, features);
}

chunkdb::WalReplayResult Replay(
    const Bytes& wal,
    Bytes* payload,
    Bytes* presence,
    const chunkdb::FeatureFlags& store_features = kNoFeatures) {
    payload->assign(kPayloadBytes, 0U);
    presence->assign(kPresenceBytes, 0U);
    return chunkdb::ReplayWal(wal, kGeometry, kCoord, kStoreId, store_features, payload, presence);
}

// Re-signs the header CRC of the frame at `at` (with `tlv_size` TLV bytes) so
// a header-field corruption reaches the field check instead.
void ResignHeader(Bytes* wal, std::size_t at, std::size_t tlv_size) {
    const std::size_t crc_at = at + chunkdb::kWalFrameFixedHeaderSize + tlv_size;
    PutLe32(wal, crc_at, chunkdb::Crc32(wal->data() + at + 4, crc_at - at - 4));
}

// Re-signs the frame CRC of the frame at `at` over its records.
void ResignBody(Bytes* wal, std::size_t at, std::size_t frame_size, std::size_t tlv_size) {
    const std::size_t begin = at + kFrameHeader + tlv_size;
    const std::size_t end = at + frame_size - 4;
    PutLe32(wal, end, chunkdb::Crc32(wal->data() + begin, end - begin));
}

void TestFrameRoundTripAndTornCuts() {
    const Bytes tag = {'j', 'o', 'b', '-', '7'};
    // Frame A: one record. Frame B: a tag and three records, one of them in
    // the presence region.
    const auto frame_a = BuildFrame(7, 1727786400000ULL, {}, {Span(0, {0xAA})});
    const auto frame_b = BuildFrame(
        9, 1727786400500ULL, Tlv(chunkdb::kWalTlvTag, tag),
        {Span(1, {0xBB, 0xBB}), Span(3, {0xCC}), Span(kPayloadBytes, {0x0F})});

    // The writer produces exactly this layout.
    {
        Bytes batch;
        chunkdb::WalFrameBuilder builder(&batch, tag);
        const Bytes b1 = {0xBB, 0xBB};
        const Bytes b2 = {0xCC};
        const Bytes b3 = {0x0F};
        builder.AppendSpan(1, b1.data(), b1.size());
        builder.AppendSpan(3, b2.data(), b2.size());
        builder.AppendSpan(kPayloadBytes, b3.data(), b3.size());
        assert(builder.Finish(9, 1727786400500ULL) == frame_b.size());
        assert(batch == frame_b);
        Bytes untagged;
        chunkdb::WalFrameBuilder plain(&untagged);
        const Bytes a = {0xAA};
        plain.AppendSpan(0, a.data(), a.size());
        (void)plain.Finish(7, 1727786400000ULL);
        assert(untagged == frame_a);
    }

    Bytes wal = Header();
    Append(&wal, frame_a);
    const std::size_t frame_b_at = wal.size();
    Append(&wal, frame_b);

    Bytes payload;
    Bytes presence;
    {
        const auto result = Replay(wal, &payload, &presence);
        assert(result.replayable && !result.tail_truncated_or_corrupt);
        assert(result.applied_frames == 2U && result.applied_records == 4U);
        assert(result.revision == 9U && result.commit_time_ms == 1727786400500ULL);
        assert(result.valid_end == wal.size());
        assert(payload[0] == 0xAA && payload[1] == 0xBB && payload[2] == 0xBB);
        assert(payload[3] == 0xCC && presence[0] == 0x0F);
    }

    Bytes after_a_payload;
    Bytes after_a_presence;
    {
        Bytes only_a(wal.begin(), wal.begin() + static_cast<std::ptrdiff_t>(frame_b_at));
        const auto result = Replay(only_a, &after_a_payload, &after_a_presence);
        assert(result.applied_frames == 1U && result.revision == 7U);
    }
    // Torn anywhere inside frame B (TLV, records, trailer): nothing of B.
    for (std::size_t cut = frame_b_at + 1; cut < wal.size(); ++cut) {
        const Bytes torn(wal.begin(), wal.begin() + static_cast<std::ptrdiff_t>(cut));
        const auto result = Replay(torn, &payload, &presence);
        assert(result.replayable && result.tail_truncated_or_corrupt);
        assert(result.stopped_at_crash_tail);
        assert(result.applied_frames == 1U && result.revision == 7U);
        assert(result.commit_time_ms == 1727786400000ULL);
        assert(result.valid_end == frame_b_at);
        assert(payload == after_a_payload && presence == after_a_presence);
    }
}

void TestFrameFieldGuards() {
    const Bytes tag = {'t'};
    const std::size_t tlv_size = chunkdb::kWalTlvHeaderSize + tag.size();
    const auto frame_a = BuildFrame(7, 100, {}, {Span(0, {0xAA})});
    const auto frame_b = BuildFrame(
        9, 200, Tlv(chunkdb::kWalTlvTag, tag), {Span(1, {0xBB}), Span(kPayloadBytes, {0x01})});
    Bytes wal = Header();
    Append(&wal, frame_a);
    const std::size_t at = wal.size();
    Append(&wal, frame_b);

    Bytes payload;
    Bytes presence;
    const auto expect_b_rejected = [&](const Bytes& bytes, const std::string& reason) {
        const auto result = Replay(bytes, &payload, &presence);
        if (result.stop_reason != reason) {
            std::fprintf(stderr, "expected %s, got %s\n", reason.c_str(), result.stop_reason.c_str());
        }
        assert(result.replayable && result.tail_truncated_or_corrupt);
        assert(result.stop_reason == reason);
        assert(result.applied_frames == 1U && result.revision == 7U);
        assert(result.valid_end == at);
    };

    // Frame header: magic 0..4, revision 4..12, commit time 12..20, flags
    // 20..22, tlv_size 22..24, record_count 24..28, body_size 28..32, TLV,
    // header CRC. Everything but the magic is covered by the header CRC.
    for (const auto& [offset, reason] : std::vector<std::pair<std::size_t, const char*>>{
             {0, "frame_magic_mismatch"},
             {5, "frame_header_crc_mismatch"},
             {13, "frame_header_crc_mismatch"},
             {20, "frame_header_crc_mismatch"},
             {24, "frame_header_crc_mismatch"},
             {28, "frame_header_crc_mismatch"},
             {chunkdb::kWalFrameFixedHeaderSize + 4, "frame_header_crc_mismatch"},  // the tag
             {chunkdb::kWalFrameFixedHeaderSize + tlv_size, "frame_header_crc_mismatch"},
         }) {
        auto corrupt = wal;
        corrupt[at + offset] ^= 0x01U;
        expect_b_rejected(corrupt, reason);
    }
    // Every record byte and the trailer are covered by the frame CRC.
    for (std::size_t offset = kFrameHeader + tlv_size; offset < frame_b.size(); ++offset) {
        auto corrupt = wal;
        corrupt[at + offset] ^= 0x01U;
        expect_b_rejected(corrupt, "frame_crc_mismatch");
    }

    // With the CRCs re-signed, the structural checks still hold.
    const std::size_t records_at = at + kFrameHeader + tlv_size;
    {
        auto corrupt = wal;
        corrupt[at + 20] = 1;  // frame flags
        ResignHeader(&corrupt, at, tlv_size);
        expect_b_rejected(corrupt, "frame_flags_unknown");
    }
    {
        auto corrupt = wal;
        corrupt[records_at] = 0x7F;  // record type
        ResignBody(&corrupt, at, frame_b.size(), tlv_size);
        expect_b_rejected(corrupt, "record_unknown_type");
    }
    {
        auto corrupt = wal;
        PutLe32(&corrupt, records_at + 1, 4);  // a span with no bytes
        ResignBody(&corrupt, at, frame_b.size(), tlv_size);
        expect_b_rejected(corrupt, "record_span_empty");
    }
    {
        auto corrupt = wal;
        PutLe32(&corrupt, records_at + 1, 1000);  // past the frame
        ResignBody(&corrupt, at, frame_b.size(), tlv_size);
        expect_b_rejected(corrupt, "record_out_of_frame");
    }
    {
        auto corrupt = wal;
        PutLe32(&corrupt, records_at + 5, kPayloadBytes + kPresenceBytes);  // offset out of state
        ResignBody(&corrupt, at, frame_b.size(), tlv_size);
        expect_b_rejected(corrupt, "record_out_of_range");
    }
    {
        // Re-pointed across the payload/presence boundary.
        auto corrupt = BuildFrame(9, 200, {}, {Span(kPayloadBytes - 1, {0xBB, 0x01})});
        Bytes stream = Header();
        Append(&stream, frame_a);
        Append(&stream, corrupt);
        expect_b_rejected(stream, "record_region_straddle");
    }
    for (const auto& [tlv, reason] : std::vector<std::pair<Bytes, const char*>>{
             {Tlv(77, {1}), "tlv_unknown_type"},
             {Tlv(chunkdb::kWalTlvTag, {}), "tlv_empty_tag"},
             {[] {
                  Bytes two = Tlv(chunkdb::kWalTlvTag, {1});
                  Append(&two, Tlv(chunkdb::kWalTlvTag, {2}));
                  return two;
              }(),
              "tlv_duplicate_tag"},
             {Bytes{1, 0, 9, 0, 1}, "tlv_out_of_frame"},
         }) {
        Bytes stream = Header();
        Append(&stream, frame_a);
        Append(&stream, BuildFrame(9, 200, tlv, {Span(1, {0xBB})}));
        expect_b_rejected(stream, reason);
    }
    {
        Bytes stream = Header();
        Append(&stream, frame_a);
        Append(&stream, BuildFrame(9, 200, {}, {}));
        expect_b_rejected(stream, "frame_empty");
    }
}

void TestUnknownTypesOwnedByUnknownFeatures() {
    const chunkdb::FeatureFlags compat{.incompat = 0, .ro_compat = 0, .compat = 1U << 6U};
    Bytes wal = Header(compat);
    Append(&wal, BuildFrame(
                     5, 50, Tlv(42, {1, 2}),
                     {Span(0, {0x5A}), Record{0x40, {1, 2, 3}}, Span(kPayloadBytes, {0x01})}));
    Bytes payload;
    Bytes presence;
    // The store must record the feature the file uses.
    {
        const auto result = Replay(wal, &payload, &presence);
        assert(!result.replayable);
        assert(result.stop_reason.find("uses features the store does not") != std::string::npos);
    }
    // With it, the TLV field and the record it owns are skipped.
    const auto result = Replay(wal, &payload, &presence, compat);
    assert(result.replayable && !result.tail_truncated_or_corrupt);
    assert(result.applied_frames == 1U && result.applied_records == 3U);
    assert(payload[0] == 0x5A && presence[0] == 0x01);
}

void TestHeaderChecks() {
    Bytes payload;
    Bytes presence;
    const auto frame = BuildFrame(3, 30, {}, {Span(0, {0x11})});
    const auto expect_invalid = [&](Bytes wal, const std::string& reason) {
        Append(&wal, frame);
        const auto result = Replay(wal, &payload, &presence);
        assert(!result.replayable && !result.torn_creation);
        if (result.stop_reason.find(reason) == std::string::npos) {
            std::fprintf(stderr, "expected %s, got %s\n", reason.c_str(), result.stop_reason.c_str());
            assert(false);
        }
        assert(result.applied_frames == 0U);
    };
    {
        // A 2.0 development WAL (version 4) starts with another magic.
        Bytes old = {'C', 'H', 'K', 'W', 'A', 'L', '0', '2'};
        old.resize(chunkdb::kWalHeaderSize, 0);
        expect_invalid(old, "not a 2.0 WAL");
    }
    for (const auto& [offset, reason] : std::vector<std::pair<std::size_t, const char*>>{
             {8, "checksum mismatch"},    // version
             {24, "checksum mismatch"},   // store id
             {40, "checksum mismatch"},   // chunk_x
             {56, "checksum mismatch"}}) {  // the CRC itself
        auto header = Header();
        header[offset] ^= 0x01U;
        expect_invalid(header, reason);
    }
    {
        auto other = kStoreId;
        other[3] ^= 0xFFU;
        expect_invalid(chunkdb::BuildWalHeader(kCoord, other, kNoFeatures), "belongs to another store");
        expect_invalid(chunkdb::BuildWalHeader({1, 0}, kStoreId, kNoFeatures), "coordinate mismatch");
    }
    // Frames without a header are not replayed.
    {
        const auto result = Replay(frame, &payload, &presence);
        assert(!result.replayable && !result.torn_creation);
    }
    // A prefix of this chunk's header (including empty) is an interrupted
    // creation; anything else that short is damage.
    for (const std::size_t length : {std::size_t{0}, std::size_t{1}, std::size_t{30},
                                     chunkdb::kWalHeaderSize - 1}) {
        const auto header = Header();
        const Bytes prefix(header.begin(), header.begin() + static_cast<std::ptrdiff_t>(length));
        const auto result = Replay(prefix, &payload, &presence);
        assert(result.torn_creation && !result.replayable);
    }
    {
        Bytes short_garbage = {'X', 'Y'};
        const auto result = Replay(short_garbage, &payload, &presence);
        assert(!result.torn_creation && !result.replayable);
    }
}

Bytes ReadFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void WriteFile(const std::filesystem::path& path, const Bytes& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    assert(out);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
}

chunkdb::StoreConfig StoreConfig(const std::filesystem::path& dir, chunkdb::DurabilityMode mode) {
    chunkdb::StoreConfig config;
    config.geometry = kGeometryConfig;
    config.data_dir = dir;
    config.durability_mode = mode;
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000'000;
    config.wal_group_commit_updates = 1;
    return config;
}

// Regression: a WAL whose tail a crash tore used to keep the torn bytes, the
// next writes were appended after them, and replay (which stops at the torn
// bytes) lost those acknowledged writes on the following restart.
void TestWritesAfterATornTailSurviveRestart() {
    for (const auto mode : {chunkdb::DurabilityMode::kFsyncWal, chunkdb::DurabilityMode::kRelaxed}) {
        ScopedTempDir dir("chunkdb-wal-torn-tail");
        const auto config = StoreConfig(dir.path(), mode);
        const auto wal_path = chunkdb::ChunkWalPath(dir.path(), kGeometry, {0, 0});
        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
            store.WalBarrier();
        }
        const auto committed = ReadFile(wal_path);
        // The start of a frame a crash cut short.
        auto torn = committed;
        Append(&torn, BuildFrame(99, 1, {}, {Span(1, {0x77})}));
        torn.resize(committed.size() + 11);
        WriteFile(wal_path, torn);
        {
            chunkdb::ChunkStore store(config);
            assert(store.GetBlockBits(0, 0) == "11110000");
            // The torn bytes are gone before anything is appended.
            assert(ReadFile(wal_path) == committed);
            store.SetBlockBits(1, 0, "00001111");
            store.WalBarrier();
        }
        chunkdb::ChunkStore store(config);
        assert(store.GetBlockBits(0, 0) == "11110000");
        assert(store.GetBlockBits(1, 0) == "00001111");
    }
}

// Which stops a crash can leave (trimmed before appending) and which mean
// damage to acknowledged frames (the load fails).
void TestCrashTailClassification() {
    const auto frame_a = BuildFrame(7, 100, {}, {Span(0, {0xAA})});
    const auto frame_b = BuildFrame(9, 200, {}, {Span(1, {0xBB})});
    Bytes wal = Header();
    Append(&wal, frame_a);
    const std::size_t b_at = wal.size();
    Append(&wal, frame_b);
    Bytes payload;
    Bytes presence;
    const auto stop = [&](const Bytes& bytes, bool crash_shaped) {
        const auto result = Replay(bytes, &payload, &presence);
        assert(result.replayable && result.tail_truncated_or_corrupt);
        assert(result.stopped_at_crash_tail == crash_shaped);
    };
    {
        auto last_trailer = wal;  // the last frame complete but failing its CRC
        last_trailer.back() ^= 0x01U;
        stop(last_trailer, true);
    }
    {
        auto zero_tail = wal;  // unwritten blocks after the last frame
        zero_tail.resize(wal.size() + 50, 0);
        stop(zero_tail, true);
        auto short_garbage = wal;  // too short to be a frame header
        Append(&short_garbage, {0x13, 0x37, 0x42});
        stop(short_garbage, true);
    }
    {
        auto first_body = wal;  // frame A damaged, frame B after it
        first_body[b_at - 5] ^= 0x01U;
        stop(first_body, false);
        auto first_header = wal;
        first_header[chunkdb::kWalHeaderSize + 6] ^= 0x01U;
        stop(first_header, false);
        // Garbage with an acknowledged frame after it.
        auto garbage_then_frame = wal;
        Append(&garbage_then_frame, Bytes(64, 0x5A));
        Append(&garbage_then_frame, BuildFrame(11, 300, {}, {Span(2, {0xDD})}));
        stop(garbage_then_frame, false);
    }
    {
        // No CRC-valid frame header after the stop: what a crash leaves on a
        // filesystem that exposes unwritten or stale blocks.
        auto garbage_tail = wal;
        Append(&garbage_tail, Bytes(64, 0x5A));
        stop(garbage_tail, true);
        const auto frame_c = BuildFrame(11, 300, {}, {Span(2, {0xDD, 0xEE, 0xFF})});
        auto partial_header = wal;  // frame C's header only partly written
        Append(&partial_header, Bytes(frame_c.begin(), frame_c.begin() + 20));
        partial_header.resize(partial_header.size() + 40, 0);
        stop(partial_header, true);
        auto unwritten_start = wal;  // frame C's first bytes never written
        Append(&unwritten_start, frame_c);
        std::fill(unwritten_start.begin() + static_cast<std::ptrdiff_t>(wal.size()),
                  unwritten_start.begin() + static_cast<std::ptrdiff_t>(wal.size() + 8), 0);
        stop(unwritten_start, true);
    }
    {
        const auto result = Replay(Bytes(100, 0), &payload, &presence);
        assert(result.torn_creation && !result.replayable);
    }
    {
        // Part of the header (up to and into its CRC), then unwritten blocks.
        const auto header = Header();
        for (const std::size_t written : {std::size_t{30}, std::size_t{58}}) {
            Bytes torn(header.begin(), header.begin() + static_cast<std::ptrdiff_t>(written));
            torn.resize(100, 0);
            const auto result = Replay(torn, &payload, &presence);
            assert(result.torn_creation);
        }
        // A complete header with a damaged CRC followed by frames is damage.
        auto damaged = header;
        damaged[57] ^= 0x01U;
        Append(&damaged, BuildFrame(3, 30, {}, {Span(0, {0x11})}));
        const auto result = Replay(damaged, &payload, &presence);
        assert(!result.torn_creation && !result.replayable);
    }
}

int ExitCode(int status) {
#ifdef _WIN32
    return status;
#else
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
#endif
}

int RunVerify(const std::string& verify, const std::filesystem::path& data_dir, std::string* output) {
    const auto log = data_dir.string() + "-verify.log";
    std::string command = "\"" + verify + "\" --data-dir \"" + data_dir.string() + "\" > \"" + log + "\" 2>&1";
#ifdef _WIN32
    command = "\"" + command + "\"";
#endif
    const int status = std::system(command.c_str());
    const auto bytes = [&] {
        std::ifstream in(log, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }();
    *output = bytes;
    std::filesystem::remove(log);
    return ExitCode(status);
}

// A damaged frame with an acknowledged frame after it: every read path
// refuses the chunk and the file is left exactly as it was.
void TestDamageBeforeTheEndFailsClosed(const std::string& verify) {
    ScopedTempDir dir("chunkdb-wal-damage-before-end");
    const auto config = StoreConfig(dir.path(), chunkdb::DurabilityMode::kFsyncWal);
    const auto wal_path = chunkdb::ChunkWalPath(dir.path(), kGeometry, {0, 0});
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "11110000");
        store.SetBlockBits(1, 0, "00001111");
    }
    auto wal = ReadFile(wal_path);
    wal[chunkdb::kWalHeaderSize + kFrameHeader + 9] ^= 0x01U;  // first frame's payload byte
    WriteFile(wal_path, wal);

    const auto expect_refused = [&](const std::function<void()>& read, const char* text) {
        bool refused = false;
        try {
            read();
        } catch (const std::exception& e) {
            refused = std::string(e.what()).find(text) != std::string::npos;
        }
        assert(refused);
        assert(ReadFile(wal_path) == wal);
    };
    expect_refused([&] {
        chunkdb::ChunkStore store(config);
        (void)store.GetBlockBits(1, 0);
    }, "damaged before its end");
    expect_refused([&] {
        chunkdb::ChunkStore store(config);
        (void)store.ScanPopulatedChunks(false, {}, 10);
    }, "cannot be replayed");
    expect_refused([&] {
        auto read_only = config;
        read_only.access_mode = chunkdb::AccessMode::kReadOnly;
        chunkdb::ChunkStore store(read_only);
        (void)store.GetBlockBits(1, 0);
    }, "rejected WAL");

    std::string output;
    assert(RunVerify(verify, dir.path(), &output) == 1);
    assert(output.find("VERIFY error wal_damaged") != std::string::npos);
}

// Stops a crash can leave are trimmed, and writes after them survive.
void TestCrashShapedTailsAreTrimmed() {
    for (const int variant : {0, 1}) {
        ScopedTempDir dir("chunkdb-wal-crash-tail");
        const auto config = StoreConfig(dir.path(), chunkdb::DurabilityMode::kFsyncWal);
        const auto wal_path = chunkdb::ChunkWalPath(dir.path(), kGeometry, {0, 0});
        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
        }
        const auto committed = ReadFile(wal_path);
        auto damaged = committed;
        if (variant == 0) {
            // A complete last frame whose bytes did not all reach the disk.
            auto frame = BuildFrame(99, 1, {}, {Span(1, {0x77})});
            frame.back() ^= 0xFFU;
            Append(&damaged, frame);
        } else {
            damaged.resize(committed.size() + 50, 0);  // unwritten blocks
        }
        WriteFile(wal_path, damaged);
        {
            chunkdb::ChunkStore store(config);
            assert(store.GetBlockBits(0, 0) == "11110000");
            assert(ReadFile(wal_path) == committed);
            store.SetBlockBits(1, 0, "00001111");
        }
        chunkdb::ChunkStore store(config);
        assert(store.GetBlockBits(1, 0) == "00001111");
    }
}

// A WAL left by an interrupted creation (empty, or part of its header) holds
// no mutation: it is replaced, never appended to.
void TestInterruptedWalCreationIsReplaced() {
    for (const std::size_t kept : {std::size_t{0}, std::size_t{20}, std::size_t{100}}) {
        ScopedTempDir dir("chunkdb-wal-torn-creation");
        const auto config = StoreConfig(dir.path(), chunkdb::DurabilityMode::kFsyncWal);
        const auto wal_path = chunkdb::ChunkWalPath(dir.path(), kGeometry, {0, 0});
        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
        }
        auto header = ReadFile(wal_path);
        if (kept == 100) {
            header.assign(100, 0);  // unwritten blocks, longer than a header
        } else {
            header.resize(kept);
        }
        WriteFile(wal_path, header);
        {
            // Read-only: treated as holding nothing, and left alone.
            auto read_only = config;
            read_only.access_mode = chunkdb::AccessMode::kReadOnly;
            chunkdb::ChunkStore reader(read_only);
            assert(!reader.BlockExists(0, 0));
            assert(ReadFile(wal_path) == header);
        }
        {
            chunkdb::ChunkStore store(config);
            assert(!store.BlockExists(0, 0));
            store.SetBlockBits(1, 0, "00001111");
        }
        const auto rewritten = ReadFile(wal_path);
        assert(rewritten.size() > chunkdb::kWalHeaderSize);
        chunkdb::ChunkStore store(config);
        assert(store.GetBlockBits(1, 0) == "00001111");
    }
}

// A WAL that is not a crash artifact (damaged header, frames without a
// header) fails the chunk load and is left as it is.
void TestDamagedWalFailsTheLoad() {
    for (const bool strip_header : {false, true}) {
        ScopedTempDir dir("chunkdb-wal-damaged-header");
        const auto config = StoreConfig(dir.path(), chunkdb::DurabilityMode::kFsyncWal);
        const auto wal_path = chunkdb::ChunkWalPath(dir.path(), kGeometry, {0, 0});
        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
        }
        auto wal = ReadFile(wal_path);
        if (strip_header) {
            wal.erase(wal.begin(), wal.begin() + static_cast<std::ptrdiff_t>(chunkdb::kWalHeaderSize));
        } else {
            wal[30] ^= 0x01U;  // inside the store id
        }
        WriteFile(wal_path, wal);
        bool refused = false;
        try {
            chunkdb::ChunkStore store(config);
            (void)store.GetBlockBits(0, 0);
        } catch (const std::exception& e) {
            refused = std::string(e.what()).find("cannot be replayed") != std::string::npos;
        }
        assert(refused);
        assert(ReadFile(wal_path) == wal);
    }
}

std::uint64_t NowMs() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

// Each frame carries its commit time, and a reload takes it from the WAL.
void TestCommitTimeSurvivesReload() {
    ScopedTempDir dir("chunkdb-wal-commit-time");
    const auto config = StoreConfig(dir.path(), chunkdb::DurabilityMode::kFsyncWal);
    const auto before = NowMs();
    std::uint64_t committed = 0;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "11110000");
        committed = store.ChunkCommitTimeForTests(0, 0);
        assert(committed >= before && committed <= NowMs());
        store.SetBlockBits(1, 0, "00001111");
        assert(store.ChunkCommitTimeForTests(0, 0) >= committed);
        committed = store.ChunkCommitTimeForTests(0, 0);
    }
    chunkdb::ChunkStore store(config);
    assert(store.ChunkCommitTimeForTests(0, 0) == committed);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        throw std::invalid_argument("usage: chunkdb_wal_format_test <chunkdb_verify>");
    }
    TestFrameRoundTripAndTornCuts();
    TestCrashTailClassification();
    TestFrameFieldGuards();
    TestUnknownTypesOwnedByUnknownFeatures();
    TestHeaderChecks();
    TestWritesAfterATornTailSurviveRestart();
    TestInterruptedWalCreationIsReplaced();
    TestDamagedWalFailsTheLoad();
    TestDamageBeforeTheEndFailsClosed(argv[1]);
    TestCrashShapedTailsAreTrimmed();
    TestCommitTimeSurvivesReload();
    return 0;
}
