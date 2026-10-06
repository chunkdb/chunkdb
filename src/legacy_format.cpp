// Readers for every chunkdb storage artifact written before the 2.0 storage
// format (docs/EXTENSIBLE_FORMAT_DESIGN.md): chunk images v1-v5, WAL v2/v3
// record streams and v4 frames (including a v4 header written mid-stream
// after 1.x records), and the intermediate 8-byte version-clock record.
//
// The engine does not call this code; it is kept, with its tests, for the
// offline converter (chunkdb_migrate). It is a frozen copy of the engine's
// readers at the time the 2.0 format replaced them, so its constants are its
// own and engine changes never reach it.

#include "legacy_format.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/crc32.hpp"
#include "chunkdb/zrle.hpp"

namespace chunkdb::legacy {

namespace {

constexpr std::size_t kImageMagicSize = 8;
constexpr std::uint8_t kImageMagic[kImageMagicSize] = {'C', 'H', 'K', 'D', 'A', 'T', 'A', '1'};
constexpr std::uint16_t kImageV1 = 1;
constexpr std::uint16_t kImageV2 = 2;
constexpr std::uint16_t kImageV3Compressed = 3;
constexpr std::uint16_t kImageV4 = 4;
constexpr std::uint16_t kImageV5Compressed = 5;
constexpr std::size_t kImageHeaderSize = kImageMagicSize + 2U + 2U + 4U + 4U + 8U + 8U + 4U + 4U + 8U;
constexpr std::size_t kImageHeaderSizeV4 = kImageHeaderSize + 8U + 4U;

constexpr std::size_t kWalMagicSize_ = 8;
constexpr std::uint8_t kWalMagic_[kWalMagicSize_] = {'C', 'H', 'K', 'W', 'A', 'L', '0', '2'};
constexpr std::uint16_t kWalV2 = 2;
constexpr std::uint16_t kWalV3 = 3;
constexpr std::uint16_t kWalV4 = 4;
constexpr std::size_t kWalHeaderSize_ = kWalMagicSize_ + 2U + 2U + 4U + 4U + 8U + 8U;

constexpr std::size_t kDltMagicSize = 4;
constexpr std::uint8_t kDltMagic[kDltMagicSize] = {'D', 'L', 'T', '1'};
constexpr std::size_t kDltRecordHeaderSize = kDltMagicSize + 4U + 2U + 4U;

constexpr std::size_t kFrameMagicSize = 4;
constexpr std::uint8_t kFrameMagic[kFrameMagicSize] = {'F', 'R', 'M', '1'};
constexpr std::size_t kFrameHeaderSize = kFrameMagicSize + 8U + 2U + 4U + 4U;
constexpr std::size_t kFrameTrailerSize = 4U;
constexpr std::size_t kFrameRecordOverhead = 4U + 2U + 4U;

}  // namespace

LegacyChunkImage ParseLegacyChunkImage(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord) {
    if (bytes.size() < kImageHeaderSize) {
        throw std::runtime_error("chunk file too small");
    }

    if (std::memcmp(bytes.data(), kImageMagic, kImageMagicSize) != 0) {
        throw std::runtime_error("invalid chunk magic");
    }

    const std::uint16_t version = ReadLe16(bytes, 8U);
    const bool revisioned =
        version == kImageV4 || version == kImageV5Compressed;
    const bool legacy_v1 = version == kImageV1;
    const bool compressed =
        version == kImageV5Compressed || version == kImageV3Compressed;
    if (!revisioned && !legacy_v1 && version != kImageV2 &&
        version != kImageV3Compressed) {
        throw std::runtime_error("unsupported chunk file version");
    }

    const std::uint16_t block_bits = ReadLe16(bytes, 10U);
    const std::uint32_t chunk_width = ReadLe32(bytes, 12U);
    const std::uint32_t chunk_height = ReadLe32(bytes, 16U);

    if (block_bits != geometry.config().block_bits ||
        chunk_width != geometry.config().chunk_width_blocks ||
        chunk_height != geometry.config().chunk_height_blocks) {
        throw std::runtime_error("geometry mismatch");
    }

    const auto chunk_x = static_cast<std::int64_t>(ReadLe64(bytes, 20U));
    const auto chunk_y = static_cast<std::int64_t>(ReadLe64(bytes, 28U));
    if (chunk_x != expected_chunk_coord.x || chunk_y != expected_chunk_coord.y) {
        throw std::runtime_error("chunk coordinate mismatch");
    }

    const std::uint32_t payload_size = ReadLe32(bytes, 36U);
    const std::uint32_t payload_crc = ReadLe32(bytes, 40U);

    if (payload_size != geometry.ChunkPayloadBytes()) {
        throw std::runtime_error("payload size mismatch");
    }

    LegacyChunkImage image;
    image.version = version;
    std::size_t header_size = kImageHeaderSize;
    if (revisioned) {
        if (bytes.size() < kImageHeaderSizeV4) {
            throw std::runtime_error("chunk file too small");
        }
        // The revision drives CHUNKVER / CAS decisions, so the header that
        // carries it is checksummed on its own.
        const std::uint32_t header_crc = ReadLe32(bytes, kImageHeaderSizeV4 - 4U);
        if (Crc32(bytes.data(), kImageHeaderSizeV4 - 4U) != header_crc) {
            throw std::runtime_error("header checksum mismatch");
        }
        image.revision = ReadLe64(bytes, kImageHeaderSize);
        header_size = kImageHeaderSizeV4;
    }

    if (legacy_v1) {
        if (bytes.size() != header_size + payload_size) {
            throw std::runtime_error("incomplete payload");
        }

        image.payload.assign(
            bytes.begin() + static_cast<std::ptrdiff_t>(header_size),
            bytes.end());
        if (Crc32(image.payload) != payload_crc) {
            throw std::runtime_error("payload checksum mismatch");
        }
        image.presence_bitmap = FullPresenceBitmap(geometry);
        return image;
    }

    const std::size_t presence_bytes = ChunkPresenceBitmapBytes(geometry);

    if (compressed) {
        // The CRC covers the canonical uncompressed state, so corruption in
        // the compressed blob is caught either by the bounded decoder or by
        // the checksum of its output.
        const auto state = ZrleDecompress(
            bytes.data() + header_size,
            bytes.size() - header_size,
            payload_size + presence_bytes);
        if (Crc32(state) != payload_crc) {
            throw std::runtime_error("payload checksum mismatch");
        }
        SplitChunkStateBytes(geometry, state, &image.payload, &image.presence_bitmap);
        return image;
    }

    if (bytes.size() != header_size + payload_size + presence_bytes) {
        throw std::runtime_error("incomplete payload");
    }

    std::vector<std::uint8_t> state(
        bytes.begin() + static_cast<std::ptrdiff_t>(header_size),
        bytes.end());
    if (Crc32(state) != payload_crc) {
        throw std::runtime_error("payload checksum mismatch");
    }

    SplitChunkStateBytes(geometry, state, &image.payload, &image.presence_bitmap);
    return image;
}

void ValidateLegacyWalHeader(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord) {
    if (bytes.size() < kWalHeaderSize_) {
        throw std::runtime_error("WAL file too small");
    }
    if (std::memcmp(bytes.data(), kWalMagic_, kWalMagicSize_) != 0) {
        throw std::runtime_error("invalid WAL magic");
    }

    const std::uint16_t version = ReadLe16(bytes, 8U);
    if (version != kWalV4 && version != kWalV3 && version != kWalV2) {
        throw std::runtime_error("unsupported WAL version");
    }

    const std::uint16_t block_bits = ReadLe16(bytes, 10U);
    const std::uint32_t chunk_width = ReadLe32(bytes, 12U);
    const std::uint32_t chunk_height = ReadLe32(bytes, 16U);
    if (block_bits != geometry.config().block_bits ||
        chunk_width != geometry.config().chunk_width_blocks ||
        chunk_height != geometry.config().chunk_height_blocks) {
        throw std::runtime_error("WAL geometry mismatch");
    }

    const auto chunk_x = static_cast<std::int64_t>(ReadLe64(bytes, 20U));
    const auto chunk_y = static_cast<std::int64_t>(ReadLe64(bytes, 28U));
    if (chunk_x != expected_chunk_coord.x || chunk_y != expected_chunk_coord.y) {
        throw std::runtime_error("WAL chunk coordinate mismatch");
    }
}
namespace {

[[nodiscard]] bool IsKnownWalVersion(std::uint16_t version) noexcept {
    return version == kWalV4 || version == kWalV3 ||
           version == kWalV2;
}

// A span is legitimate only when it lies wholly in the payload region,
// wholly in the presence region, or covers the full state (conditional
// full-state writes). Anything else straddles the boundary.
[[nodiscard]] bool SpanShapeValid(
    std::size_t byte_offset,
    std::size_t end,
    std::size_t payload_boundary,
    std::size_t state_size) noexcept {
    const bool wholly_in_payload = end <= payload_boundary;
    const bool wholly_in_presence = byte_offset >= payload_boundary;
    const bool full_state_span = byte_offset == 0U && end == state_size;
    return wholly_in_payload || wholly_in_presence || full_state_span;
}

struct PendingRecord {
    std::size_t offset;
    std::size_t source;
    std::size_t size;
};

// Parses and validates one whole v4 frame at `cursor` without applying it.
// Returns false with `stop_reason` set when the frame is torn or invalid.
[[nodiscard]] bool ParseFrame(
    const std::vector<std::uint8_t>& wal_bytes,
    std::size_t cursor,
    std::size_t payload_boundary,
    std::size_t state_size,
    std::vector<PendingRecord>* records,
    std::uint64_t* revision,
    std::size_t* frame_size,
    std::string* stop_reason) {
    const std::size_t remaining = wal_bytes.size() - cursor;
    if (remaining < kFrameHeaderSize) {
        *stop_reason = "partial_frame_header";
        return false;
    }
    if (std::memcmp(wal_bytes.data() + cursor, kFrameMagic, kFrameMagicSize) != 0) {
        *stop_reason = "frame_magic_mismatch";
        return false;
    }
    const std::uint64_t frame_revision = ReadLe64(wal_bytes, cursor + 4U);
    const std::uint16_t record_count = ReadLe16(wal_bytes, cursor + 12U);
    const std::uint32_t body_size = ReadLe32(wal_bytes, cursor + 14U);
    const std::uint32_t header_crc = ReadLe32(wal_bytes, cursor + 18U);
    if (Crc32(wal_bytes.data() + cursor + kFrameMagicSize, 14U) != header_crc) {
        *stop_reason = "frame_header_crc_mismatch";
        return false;
    }
    if (record_count == 0) {
        *stop_reason = "frame_empty";
        return false;
    }
    const std::size_t total = kFrameHeaderSize + static_cast<std::size_t>(body_size) + kFrameTrailerSize;
    if (remaining < total) {
        // A crash inside one mutation's append: the frame is torn and is
        // ignored as a whole, which is what makes multi-record mutations
        // all-or-nothing.
        *stop_reason = "partial_frame_body";
        return false;
    }
    const std::size_t records_begin = cursor + kFrameHeaderSize;
    const std::uint32_t frame_crc = ReadLe32(wal_bytes, records_begin + body_size);
    if (Crc32(wal_bytes.data() + records_begin, body_size) != frame_crc) {
        *stop_reason = "frame_crc_mismatch";
        return false;
    }

    records->clear();
    std::size_t at = records_begin;
    const std::size_t records_end = records_begin + body_size;
    for (std::uint16_t i = 0; i < record_count; ++i) {
        if (records_end - at < kFrameRecordOverhead) {
            *stop_reason = "record_out_of_frame";
            return false;
        }
        const std::uint32_t byte_offset = ReadLe32(wal_bytes, at);
        const std::uint16_t data_size = ReadLe16(wal_bytes, at + 4U);
        if (data_size == 0 || records_end - at < kFrameRecordOverhead + data_size) {
            *stop_reason = "record_out_of_frame";
            return false;
        }
        const std::size_t body_at = at + 6U;
        const std::uint32_t record_crc = ReadLe32(wal_bytes, body_at + data_size);
        if (Crc32(wal_bytes.data() + at, 6U + data_size) != record_crc) {
            *stop_reason = "record_crc_mismatch";
            return false;
        }
        const std::size_t end = static_cast<std::size_t>(byte_offset) + data_size;
        if (end > state_size) {
            *stop_reason = "record_out_of_range";
            return false;
        }
        if (!SpanShapeValid(byte_offset, end, payload_boundary, state_size)) {
            *stop_reason = "record_region_straddle";
            return false;
        }
        records->push_back({byte_offset, body_at, data_size});
        at = body_at + data_size + 4U;
    }
    if (at != records_end) {
        *stop_reason = "frame_body_size_mismatch";
        return false;
    }
    *revision = frame_revision;
    *frame_size = total;
    return true;
}

}  // namespace

LegacyWalReplay ReplayLegacyWal(
    const std::vector<std::uint8_t>& wal_bytes,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    std::vector<std::uint8_t>* payload,
    std::vector<std::uint8_t>* presence_bitmap) {
    LegacyWalReplay result;
    if (payload == nullptr || presence_bitmap == nullptr) {
        throw std::invalid_argument("chunk state outputs must not be null");
    }

    auto state = BuildChunkStateBytes(geometry, *payload, *presence_bitmap);

    if (wal_bytes.empty()) {
        return result;
    }

    // Region boundary inside `state`: bytes [0, payload_bytes) are payload,
    // [payload_bytes, state.size()) are the presence bitmap.
    const std::size_t payload_boundary = geometry.ChunkPayloadBytes();

    std::size_t cursor = 0;
    // Frames (v4) or 1.x records (v2/v3); decided by the header, or by the
    // leading magic for a headerless stream.
    bool framed = false;
    if (wal_bytes.size() >= kWalHeaderSize_ &&
        std::memcmp(wal_bytes.data(), kWalMagic_, kWalMagicSize_) == 0) {
        try {
            ValidateLegacyWalHeader(wal_bytes, geometry, chunk_coord);
            cursor = kWalHeaderSize_;
            result.replayable = true;
            result.wal_version = ReadLe16(wal_bytes, 8U);
            framed = result.wal_version == kWalV4;
        } catch (...) {
            result.stop_reason = "invalid_header";
            return result;
        }
    } else if (
        wal_bytes.size() >= kFrameHeaderSize &&
        std::memcmp(wal_bytes.data(), kFrameMagic, kFrameMagicSize) == 0) {
        // Headerless WAL can appear if a writer recreated WAL and appended
        // frames across a file replacement race; replay from the stream start.
        result.replayable = true;
        framed = true;
    } else if (
        wal_bytes.size() >= kDltRecordHeaderSize &&
        std::memcmp(wal_bytes.data(), kDltMagic, kDltMagicSize) == 0) {
        result.replayable = true;
    } else {
        result.stop_reason = "unknown_prefix";
        return result;
    }

    std::vector<PendingRecord> records;
    while (cursor < wal_bytes.size()) {
        const std::size_t remaining = wal_bytes.size() - cursor;

        // If a repeated WAL header is encountered mid-stream, skip it and continue.
        if (remaining >= kWalHeaderSize_ &&
            std::memcmp(wal_bytes.data() + cursor, kWalMagic_, kWalMagicSize_) == 0) {
            const std::uint16_t version = ReadLe16(wal_bytes, cursor + 8U);
            const std::uint16_t block_bits = ReadLe16(wal_bytes, cursor + 10U);
            const std::uint32_t chunk_width = ReadLe32(wal_bytes, cursor + 12U);
            const std::uint32_t chunk_height = ReadLe32(wal_bytes, cursor + 16U);
            const auto header_chunk_x = static_cast<std::int64_t>(ReadLe64(wal_bytes, cursor + 20U));
            const auto header_chunk_y = static_cast<std::int64_t>(ReadLe64(wal_bytes, cursor + 28U));

            if (IsKnownWalVersion(version) &&
                block_bits == geometry.config().block_bits &&
                chunk_width == geometry.config().chunk_width_blocks &&
                chunk_height == geometry.config().chunk_height_blocks &&
                header_chunk_x == chunk_coord.x &&
                header_chunk_y == chunk_coord.y) {
                cursor += kWalHeaderSize_;
                framed = version == kWalV4;
                continue;
            }

            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "header_mismatch_midstream";
            break;
        }

        if (framed) {
            std::uint64_t revision = 0;
            std::size_t frame_size = 0;
            std::string stop_reason;
            if (!ParseFrame(
                    wal_bytes, cursor, payload_boundary, state.size(),
                    &records, &revision, &frame_size, &stop_reason)) {
                result.tail_truncated_or_corrupt = true;
                result.stop_reason = stop_reason;
                break;
            }
            for (const auto& record : records) {
                std::copy(
                    wal_bytes.data() + record.source,
                    wal_bytes.data() + record.source + record.size,
                    state.begin() + static_cast<std::ptrdiff_t>(record.offset));
            }
            result.applied_records += records.size();
            result.applied_frames += 1;
            result.revision = revision;
            cursor += frame_size;
            continue;
        }

        // 1.x record stream (v2/v3): body-only CRC plus the structural
        // straddle guard, exactly as the 1.x readers applied it.
        if (remaining < kDltRecordHeaderSize) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "partial_record_header";
            break;
        }
        if (std::memcmp(wal_bytes.data() + cursor, kDltMagic, kDltMagicSize) != 0) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "record_magic_mismatch";
            break;
        }

        const std::uint32_t byte_offset = ReadLe32(wal_bytes, cursor + 4U);
        const std::uint16_t data_size = ReadLe16(wal_bytes, cursor + 8U);
        const std::uint32_t record_crc = ReadLe32(wal_bytes, cursor + 10U);

        const std::size_t full_record_size = kDltRecordHeaderSize + data_size;
        if (remaining < full_record_size) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "partial_record_payload";
            break;
        }

        const std::size_t payload_end = static_cast<std::size_t>(byte_offset) + data_size;
        if (payload_end > state.size()) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "record_out_of_range";
            break;
        }
        if (!SpanShapeValid(byte_offset, payload_end, payload_boundary, state.size())) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "record_region_straddle";
            break;
        }

        const std::uint8_t* record_data = wal_bytes.data() + cursor + kDltRecordHeaderSize;
        if (Crc32(record_data, data_size) != record_crc) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "record_crc_mismatch";
            break;
        }

        std::copy(
            record_data,
            record_data + data_size,
            state.begin() + static_cast<std::ptrdiff_t>(byte_offset));

        cursor += full_record_size;
        result.applied_records += 1;
    }

    result.stop_offset = cursor;
    result.legacy_records = result.replayable && !framed;
    SplitChunkStateBytes(geometry, state, payload, presence_bitmap);

    return result;
}

bool TryParseIntermediateVersionClockRecord(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* out_ceiling) {
    if (bytes.size() != sizeof(std::uint64_t)) {
        return false;
    }
    const std::uint64_t ceiling = ReadLe64(bytes, 0U);
    if (ceiling == 0U) {
        return false;
    }
    *out_ceiling = ceiling;
    return true;
}

namespace {

constexpr std::size_t kRecordSize16 = 16;

[[nodiscard]] bool HasMagic(const std::vector<std::uint8_t>& bytes, const char (&magic)[5]) {
    return bytes.size() >= 4U && std::memcmp(bytes.data(), magic, 4U) == 0;
}

[[nodiscard]] bool ChecksummedRecord16(const std::vector<std::uint8_t>& bytes) {
    return bytes.size() == kRecordSize16 && ReadLe32(bytes, 12U) == Crc32(bytes.data(), 12U);
}

}  // namespace

bool TryParseLegacyVersionClockRecord(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* out_ceiling) {
    if (!ChecksummedRecord16(bytes) || !HasMagic(bytes, "CKVR")) {
        return false;
    }
    const std::uint64_t ceiling = ReadLe64(bytes, 4U);
    if (ceiling == 0U) {
        return false;
    }
    *out_ceiling = ceiling;
    return true;
}

bool IsValidLegacyInitializedMarker(const std::vector<std::uint8_t>& bytes) {
    return ChecksummedRecord16(bytes) && HasMagic(bytes, "CKID") && ReadLe64(bytes, 4U) == 1U;
}

bool TryParseLegacyConditionalIntent(
    const std::vector<std::uint8_t>& bytes,
    LegacyConditionalIntent* out) {
    if (!ChecksummedRecord16(bytes)) {
        return false;
    }
    if (HasMagic(bytes, "CKRB")) {
        out->rollback = true;
    } else if (HasMagic(bytes, "CKRC")) {
        out->rollback = false;
    } else {
        return false;
    }
    out->boundary = ReadLe64(bytes, 4U);
    return true;
}

}  // namespace chunkdb::legacy
