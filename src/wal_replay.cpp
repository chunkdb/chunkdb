#include "wal_replay.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
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
    if (version != kWalFileVersion && version != kWalFileVersionLegacy) {
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
    // [payload_bytes, state.size()) are the presence bitmap. A legitimate
    // writer never emits a delta record that straddles this boundary (payload
    // spans and presence spans are logged separately), so a record crossing
    // it indicates a corrupted byte_offset/data_size header — which the v3
    // record CRC (payload only) cannot itself detect. This structural check
    // is a partial, format-compatible mitigation for the header-CRC gap; see
    // docs/KNOWN_LIMITATIONS.md.
    const std::size_t payload_boundary = geometry.ChunkPayloadBytes();

    std::size_t cursor = 0;
    if (wal_bytes.size() >= kWalHeaderSize &&
        std::memcmp(wal_bytes.data(), kWalMagic, kWalMagicSize) == 0) {
        try {
            ValidateWalHeader(wal_bytes, geometry, chunk_coord);
            cursor = kWalHeaderSize;
            result.replayable = true;
        } catch (...) {
            result.stop_reason = "invalid_header";
            return result;
        }
    } else if (
        wal_bytes.size() >= kWalRecordHeaderSize &&
        std::memcmp(wal_bytes.data(), kWalRecordMagic, kWalRecordMagicSize) == 0) {
        // Headerless WAL can appear if a writer recreated WAL and appended records
        // across a file replacement race; replay from record stream start.
        cursor = 0;
        result.replayable = true;
    } else {
        // Unknown leading bytes: treat as non-replayable WAL content.
        result.stop_reason = "unknown_prefix";
        return result;
    }

    while (cursor < wal_bytes.size()) {
        const std::size_t remaining = wal_bytes.size() - cursor;
        if (remaining < kWalRecordHeaderSize) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "partial_record_header";
            break;
        }

        // If a repeated WAL header is encountered mid-stream, skip it and continue.
        if (remaining >= kWalHeaderSize &&
            std::memcmp(wal_bytes.data() + cursor, kWalMagic, kWalMagicSize) == 0) {
            const std::uint16_t version = ReadLe16(wal_bytes, cursor + 8U);
            const std::uint16_t block_bits = ReadLe16(wal_bytes, cursor + 10U);
            const std::uint32_t chunk_width = ReadLe32(wal_bytes, cursor + 12U);
            const std::uint32_t chunk_height = ReadLe32(wal_bytes, cursor + 16U);
            const auto header_chunk_x = static_cast<std::int64_t>(ReadLe64(wal_bytes, cursor + 20U));
            const auto header_chunk_y = static_cast<std::int64_t>(ReadLe64(wal_bytes, cursor + 28U));

            if ((version == kWalFileVersion || version == kWalFileVersionLegacy) &&
                block_bits == geometry.config().block_bits &&
                chunk_width == geometry.config().chunk_width_blocks &&
                chunk_height == geometry.config().chunk_height_blocks &&
                header_chunk_x == chunk_coord.x &&
                header_chunk_y == chunk_coord.y) {
                cursor += kWalHeaderSize;
                continue;
            }

            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "header_mismatch_midstream";
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

        // Partial mitigation for the header-CRC gap (CDB-DEF-1): the record
        // CRC covers only the body, so a corrupted byte_offset that stays in
        // range would otherwise apply a CRC-valid body to the wrong location.
        // Legitimate writers emit exactly three record shapes: a span wholly
        // inside the payload region [0, payload_boundary), a span wholly
        // inside the presence region [payload_boundary, state.size()), or a
        // single full-state span [0, state.size()) (conditional CAS/BATCH).
        // Any other span that crosses the payload/presence boundary indicates
        // a corrupted header, so stop replay rather than mis-applying it. This
        // does not catch a flip that stays within one region.
        const bool wholly_in_payload = payload_end <= payload_boundary;
        const bool wholly_in_presence = byte_offset >= payload_boundary;
        const bool full_state_span = byte_offset == 0U && payload_end == state.size();
        if (!wholly_in_payload && !wholly_in_presence && !full_state_span) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "record_region_straddle";
            break;
        }

        const std::uint8_t* record_data = wal_bytes.data() + cursor + kWalRecordHeaderSize;
        const std::uint32_t computed_crc = Crc32(record_data, data_size);
        if (computed_crc != record_crc) {
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

    SplitChunkStateBytes(geometry, state, payload, presence_bitmap);

    return result;
}

}  // namespace chunkdb
