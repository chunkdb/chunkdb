#include "wal_replay.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/crc32.hpp"

namespace chunkdb {

void ValidateWalHeader(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord) {
    if (bytes.size() < kWalHeaderSize) {
        throw std::runtime_error("WAL file too small");
    }
    if (std::memcmp(bytes.data(), kWalMagic, kWalMagicSize) != 0) {
        throw std::runtime_error("invalid WAL magic");
    }

    const std::uint16_t version = ReadLe16(bytes, 8U);
    if (version != kWalFileVersion && version != kWalFileVersionV3 && version != kWalFileVersionLegacy) {
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
    return version == kWalFileVersion || version == kWalFileVersionV3 ||
           version == kWalFileVersionLegacy;
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
    if (remaining < kWalFrameHeaderSize) {
        *stop_reason = "partial_frame_header";
        return false;
    }
    if (std::memcmp(wal_bytes.data() + cursor, kWalFrameMagic, kWalFrameMagicSize) != 0) {
        *stop_reason = "frame_magic_mismatch";
        return false;
    }
    const std::uint64_t frame_revision = ReadLe64(wal_bytes, cursor + 4U);
    const std::uint16_t record_count = ReadLe16(wal_bytes, cursor + 12U);
    const std::uint32_t body_size = ReadLe32(wal_bytes, cursor + 14U);
    const std::uint32_t header_crc = ReadLe32(wal_bytes, cursor + 18U);
    if (Crc32(wal_bytes.data() + cursor + kWalFrameMagicSize, 14U) != header_crc) {
        *stop_reason = "frame_header_crc_mismatch";
        return false;
    }
    if (record_count == 0) {
        *stop_reason = "frame_empty";
        return false;
    }
    const std::size_t total = kWalFrameHeaderSize + static_cast<std::size_t>(body_size) + kWalFrameTrailerSize;
    if (remaining < total) {
        // A crash inside one mutation's append: the frame is torn and is
        // ignored as a whole, which is what makes multi-record mutations
        // all-or-nothing.
        *stop_reason = "partial_frame_body";
        return false;
    }
    const std::size_t records_begin = cursor + kWalFrameHeaderSize;
    const std::uint32_t frame_crc = ReadLe32(wal_bytes, records_begin + body_size);
    if (Crc32(wal_bytes.data() + records_begin, body_size) != frame_crc) {
        *stop_reason = "frame_crc_mismatch";
        return false;
    }

    records->clear();
    std::size_t at = records_begin;
    const std::size_t records_end = records_begin + body_size;
    for (std::uint16_t i = 0; i < record_count; ++i) {
        if (records_end - at < kWalFrameRecordOverhead) {
            *stop_reason = "record_out_of_frame";
            return false;
        }
        const std::uint32_t byte_offset = ReadLe32(wal_bytes, at);
        const std::uint16_t data_size = ReadLe16(wal_bytes, at + 4U);
        if (data_size == 0 || records_end - at < kWalFrameRecordOverhead + data_size) {
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

WalReplayResult ReplayWal(
    const std::vector<std::uint8_t>& wal_bytes,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    std::vector<std::uint8_t>* payload,
    std::vector<std::uint8_t>* presence_bitmap) {
    WalReplayResult result;
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
    if (wal_bytes.size() >= kWalHeaderSize &&
        std::memcmp(wal_bytes.data(), kWalMagic, kWalMagicSize) == 0) {
        try {
            ValidateWalHeader(wal_bytes, geometry, chunk_coord);
            cursor = kWalHeaderSize;
            result.replayable = true;
            result.wal_version = ReadLe16(wal_bytes, 8U);
            framed = result.wal_version == kWalFileVersion;
        } catch (...) {
            result.stop_reason = "invalid_header";
            return result;
        }
    } else if (
        wal_bytes.size() >= kWalFrameHeaderSize &&
        std::memcmp(wal_bytes.data(), kWalFrameMagic, kWalFrameMagicSize) == 0) {
        // Headerless WAL can appear if a writer recreated WAL and appended
        // frames across a file replacement race; replay from the stream start.
        result.replayable = true;
        framed = true;
    } else if (
        wal_bytes.size() >= kWalRecordHeaderSize &&
        std::memcmp(wal_bytes.data(), kWalRecordMagic, kWalRecordMagicSize) == 0) {
        result.replayable = true;
    } else {
        result.stop_reason = "unknown_prefix";
        return result;
    }

    std::vector<PendingRecord> records;
    while (cursor < wal_bytes.size()) {
        const std::size_t remaining = wal_bytes.size() - cursor;

        // If a repeated WAL header is encountered mid-stream, skip it and continue.
        if (remaining >= kWalHeaderSize &&
            std::memcmp(wal_bytes.data() + cursor, kWalMagic, kWalMagicSize) == 0) {
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
                cursor += kWalHeaderSize;
                framed = version == kWalFileVersion;
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
        if (remaining < kWalRecordHeaderSize) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "partial_record_header";
            break;
        }
        if (std::memcmp(wal_bytes.data() + cursor, kWalRecordMagic, kWalRecordMagicSize) != 0) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "record_magic_mismatch";
            break;
        }

        const std::uint32_t byte_offset = ReadLe32(wal_bytes, cursor + 4U);
        const std::uint16_t data_size = ReadLe16(wal_bytes, cursor + 8U);
        const std::uint32_t record_crc = ReadLe32(wal_bytes, cursor + 10U);

        const std::size_t full_record_size = kWalRecordHeaderSize + data_size;
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

        const std::uint8_t* record_data = wal_bytes.data() + cursor + kWalRecordHeaderSize;
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

    result.legacy_records = result.replayable && !framed;
    SplitChunkStateBytes(geometry, state, payload, presence_bitmap);

    return result;
}

}  // namespace chunkdb
