#include "wal_replay.hpp"

#include <algorithm>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/crc32.hpp"
#include "feature_flags.hpp"

namespace chunkdb {

void ValidateWalHeader(
    const std::vector<std::uint8_t>& bytes,
    const ChunkCoord& expected_chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& store_features) {
    if (bytes.size() < kWalHeaderSize) {
        throw std::runtime_error("WAL file too small");
    }
    if (std::memcmp(bytes.data(), kWalMagic, kWalMagicSize) != 0) {
        throw std::runtime_error("not a 2.0 WAL (bad magic)");
    }
    if (ReadLe32(bytes, kWalHeaderSize - 4U) != Crc32(bytes.data(), kWalHeaderSize - 4U)) {
        throw std::runtime_error("WAL header checksum mismatch");
    }
    const std::uint16_t version = ReadLe16(bytes, 8U);
    if (version != kWalFormatVersion) {
        throw std::runtime_error("unsupported WAL version " + std::to_string(version));
    }
    if (ReadLe16(bytes, 10U) != 0U) {
        throw std::runtime_error("WAL header reserved field is not zero");
    }
    const FeatureFlags features{
        .incompat = ReadLe32(bytes, 12U),
        .ro_compat = ReadLe32(bytes, 16U),
        .compat = ReadLe32(bytes, 20U),
    };
    if (!IsSubsetOf(features, store_features)) {
        throw std::runtime_error(
            "WAL uses features the store does not (" + DescribeFeatures(features) + ")");
    }
    if (!std::equal(store_id.begin(), store_id.end(), bytes.begin() + 24)) {
        throw std::runtime_error("WAL belongs to another store");
    }
    const auto chunk_x = static_cast<std::int64_t>(ReadLe64(bytes, 40U));
    const auto chunk_y = static_cast<std::int64_t>(ReadLe64(bytes, 48U));
    if (chunk_x != expected_chunk_coord.x || chunk_y != expected_chunk_coord.y) {
        throw std::runtime_error("WAL chunk coordinate mismatch");
    }
}

namespace {

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

struct PendingSpan {
    std::size_t offset;
    std::size_t source;
    std::size_t size;
};

// An EXTRA_PUT (with its value at `source`) or EXTRA_DEL record.
struct PendingExtra {
    bool put = false;
    std::uint32_t block_index = 0;
    std::uint32_t bit_length = 0;
    std::size_t source = 0;
};

struct ParsedFrame {
    std::uint64_t revision = 0;
    std::uint64_t commit_time_ms = 0;
    std::size_t size = 0;
    std::vector<PendingSpan> spans;
    // Ascending block order; empty when extra_replace is set.
    std::vector<PendingExtra> extra_ops;
    std::optional<ChunkExtra> extra_replace;
    std::size_t record_count = 0;
};

struct FrameShape {
    // Bytes [0, payload_boundary) of the state are payload, the rest presence.
    std::size_t payload_boundary = 0;
    std::size_t state_size = 0;
    std::size_t block_count = 0;
    bool extra_enabled = false;
    bool may_skip_unknown = false;
};

// Parses an extra-data record into `frame`; returns the stop reason, or
// empty when the record is valid.
[[nodiscard]] std::string ParseExtraRecord(
    const std::vector<std::uint8_t>& wal,
    std::uint8_t type,
    std::size_t body_at,
    std::uint32_t size,
    const FrameShape& shape,
    ParsedFrame* frame) {
    if (!shape.extra_enabled) {
        return "record_extra_disabled";
    }
    if (frame->extra_replace.has_value()) {
        return "record_extra_order";
    }
    if (type == kWalRecordExtraReplace) {
        if (!frame->extra_ops.empty()) {
            return "record_extra_order";
        }
        try {
            frame->extra_replace = ChunkExtra::Decode(
                wal.data() + body_at, size, shape.block_count, ExtraPadding::kReject);
        } catch (const std::invalid_argument&) {
            return "record_extra_invalid";
        }
        return {};
    }
    const bool put = type == kWalRecordExtraPut;
    if (put ? size < kExtraEntryHeaderBytes : size != 4U) {
        return "record_extra_invalid";
    }
    PendingExtra op{.put = put, .block_index = ReadLe32(wal, body_at)};
    if (op.block_index >= shape.block_count) {
        return "record_out_of_range";
    }
    if (!frame->extra_ops.empty() && op.block_index <= frame->extra_ops.back().block_index) {
        return "record_extra_order";
    }
    if (put) {
        op.bit_length = ReadLe32(wal, body_at + 4U);
        op.source = body_at + kExtraEntryHeaderBytes;
        if (op.bit_length == 0U || op.bit_length > kExtraMaxBlockBitsLimit ||
            size != kExtraEntryHeaderBytes + ExtraValueBytes(op.bit_length)) {
            return "record_extra_invalid";
        }
        const unsigned used = op.bit_length % 8U;
        const std::uint8_t last = wal[body_at + size - 1U];
        if (used != 0U && (last >> used) != 0U) {
            return "record_extra_invalid";
        }
    }
    frame->extra_ops.push_back(op);
    return {};
}

// Parses and validates one whole frame at `cursor` without applying it.
// Returns false with `stop_reason` set when the frame is torn or invalid.
[[nodiscard]] bool ParseFrame(
    const std::vector<std::uint8_t>& wal,
    std::size_t cursor,
    const FrameShape& shape,
    ParsedFrame* frame,
    std::string* stop_reason,
    bool* reaches_end,
    bool* intact) {
    const std::size_t payload_boundary = shape.payload_boundary;
    const std::size_t state_size = shape.state_size;
    const bool may_skip_unknown = shape.may_skip_unknown;
    // Set when the failing frame provably extends to the end of the file.
    *reaches_end = false;
    // Set once the whole frame is present and both CRCs match: a frame a
    // crash cannot have cut, so any later failure is damage.
    *intact = false;
    const std::size_t remaining = wal.size() - cursor;
    if (remaining < kWalFrameFixedHeaderSize) {
        *stop_reason = "partial_frame_header";
        *reaches_end = true;
        return false;
    }
    if (std::memcmp(wal.data() + cursor, kWalFrameMagic, kWalFrameMagicSize) != 0) {
        *stop_reason = "frame_magic_mismatch";
        return false;
    }
    const std::uint64_t revision = ReadLe64(wal, cursor + 4U);
    const std::uint64_t commit_time_ms = ReadLe64(wal, cursor + 12U);
    const std::uint16_t frame_flags = ReadLe16(wal, cursor + 20U);
    const std::uint16_t tlv_size = ReadLe16(wal, cursor + 22U);
    const std::uint32_t record_count = ReadLe32(wal, cursor + 24U);
    const std::uint32_t body_size = ReadLe32(wal, cursor + 28U);
    const std::size_t header_size = kWalFrameFixedHeaderSize + tlv_size + kWalFrameHeaderCrcSize;
    if (remaining < header_size) {
        *stop_reason = "partial_frame_header";
        *reaches_end = true;
        return false;
    }
    const std::size_t crc_at = cursor + kWalFrameFixedHeaderSize + tlv_size;
    if (Crc32(wal.data() + cursor + kWalFrameMagicSize, crc_at - cursor - kWalFrameMagicSize) !=
        ReadLe32(wal, crc_at)) {
        *stop_reason = "frame_header_crc_mismatch";
        return false;
    }
    // In 64 bits, so no platform can wrap the declared frame size.
    const std::uint64_t total64 =
        static_cast<std::uint64_t>(header_size) + body_size + kWalFrameTrailerSize;
    // The header is CRC-valid, so its declared extent can be trusted from
    // here on: any later failure is in a frame that ends at or past EOF
    // exactly when this holds.
    *reaches_end = total64 >= remaining;
    if (total64 > remaining) {
        // A crash inside one mutation's append: the frame is torn and is
        // ignored as a whole, which is what makes mutations all-or-nothing.
        *stop_reason = "partial_frame_body";
        return false;
    }
    const auto total = static_cast<std::size_t>(total64);
    const std::size_t records_begin = cursor + header_size;
    const std::size_t records_end = records_begin + body_size;
    if (Crc32(wal.data() + records_begin, body_size) != ReadLe32(wal, records_end)) {
        *stop_reason = "frame_crc_mismatch";
        return false;
    }
    *intact = true;
    if (frame_flags != 0U) {
        *stop_reason = "frame_flags_unknown";
        return false;
    }
    if (record_count == 0U) {
        *stop_reason = "frame_empty";
        return false;
    }

    // TLV fields, covered by the header CRC.
    bool have_tag = false;
    for (std::size_t at = cursor + kWalFrameFixedHeaderSize; at < crc_at;) {
        if (crc_at - at < kWalTlvHeaderSize) {
            *stop_reason = "tlv_out_of_frame";
            return false;
        }
        const std::uint16_t type = ReadLe16(wal, at);
        const std::uint16_t length = ReadLe16(wal, at + 2U);
        if (crc_at - at - kWalTlvHeaderSize < length) {
            *stop_reason = "tlv_out_of_frame";
            return false;
        }
        if (type == kWalTlvTag) {
            if (have_tag || length == 0U) {
                *stop_reason = have_tag ? "tlv_duplicate_tag" : "tlv_empty_tag";
                return false;
            }
            have_tag = true;
        } else if (!may_skip_unknown) {
            *stop_reason = "tlv_unknown_type";
            return false;
        }
        at += kWalTlvHeaderSize + length;
    }

    frame->spans.clear();
    frame->extra_ops.clear();
    frame->extra_replace.reset();
    std::size_t at = records_begin;
    for (std::uint32_t i = 0; i < record_count; ++i) {
        if (records_end - at < kWalRecordHeaderSize) {
            *stop_reason = "record_out_of_frame";
            return false;
        }
        const std::uint8_t type = wal[at];
        const std::uint32_t size = ReadLe32(wal, at + 1U);
        const std::size_t body_at = at + kWalRecordHeaderSize;
        if (records_end - body_at < size) {
            *stop_reason = "record_out_of_frame";
            return false;
        }
        if (type == kWalRecordSpan) {
            if (size <= kWalSpanOffsetSize) {
                *stop_reason = "record_span_empty";
                return false;
            }
            const std::size_t offset = ReadLe32(wal, body_at);
            const std::size_t data_size = size - kWalSpanOffsetSize;
            if (offset > state_size || data_size > state_size - offset) {
                *stop_reason = "record_out_of_range";
                return false;
            }
            if (!SpanShapeValid(offset, offset + data_size, payload_boundary, state_size)) {
                *stop_reason = "record_region_straddle";
                return false;
            }
            frame->spans.push_back({offset, body_at + kWalSpanOffsetSize, data_size});
        } else if (
            type == kWalRecordExtraPut || type == kWalRecordExtraDel ||
            type == kWalRecordExtraReplace) {
            auto reason = ParseExtraRecord(wal, type, body_at, size, shape, frame);
            if (!reason.empty()) {
                *stop_reason = std::move(reason);
                return false;
            }
        } else if (!may_skip_unknown) {
            *stop_reason = "record_unknown_type";
            return false;
        }
        at = body_at + size;
    }
    if (at != records_end) {
        *stop_reason = "frame_body_size_mismatch";
        return false;
    }
    frame->revision = revision;
    frame->commit_time_ms = commit_time_ms;
    frame->size = total;
    frame->record_count = record_count;
    return true;
}

// Applies a frame's extra-data records the way spans apply: blindly, so a
// WAL replayed over a newer image (a crash between a checkpoint's image
// publish and its WAL removal) or over no image (empty-chunk collection)
// converges to the same state. EXTRA_PUT sets the value, EXTRA_DEL removes
// it if present, EXTRA_REPLACE replaces all of it. The invariants hold for
// the state after the last frame, which ReplayWal checks.
void ApplyFrameExtra(
    const std::vector<std::uint8_t>& wal,
    ParsedFrame* frame,
    ChunkExtra* extra) {
    if (frame->extra_replace.has_value()) {
        *extra = std::move(*frame->extra_replace);
        frame->extra_replace.reset();
        return;
    }
    const auto value_of = [&wal](const PendingExtra& op) {
        const auto begin = wal.begin() + static_cast<std::ptrdiff_t>(op.source);
        return ExtraValue{
            .bit_length = op.bit_length,
            .bytes = std::vector<std::uint8_t>(
                begin, begin + static_cast<std::ptrdiff_t>(ExtraValueBytes(op.bit_length))),
        };
    };
    if (frame->extra_ops.size() == 1U) {
        const auto& op = frame->extra_ops.front();
        if (op.put) {
            extra->Assign(op.block_index, value_of(op));
        } else {
            (void)extra->Remove(op.block_index);
        }
        return;
    }
    // Many records: one pass over the chunk's values.
    std::vector<ExtraChange> changes;
    changes.reserve(frame->extra_ops.size());
    for (const auto& op : frame->extra_ops) {
        changes.push_back(ExtraChange{
            .block_index = op.block_index,
            .value = op.put ? std::optional<ExtraValue>(value_of(op)) : std::nullopt,
        });
    }
    *extra = ChunkExtra::Merge(*extra, changes);
}

// A crash while the file was created (its header is written in the first
// append) leaves a prefix of the header and, on filesystems that expose
// unwritten blocks, zeros after it. The prefix is compared outside the
// feature flags (bytes 12..24), which depend on the writer's build; the
// header CRC (56..60) may be partly written; everything after must be zero.
[[nodiscard]] bool IsInterruptedCreation(
    const std::vector<std::uint8_t>& wal,
    const ChunkCoord& chunk_coord,
    const StoreId& store_id) {
    const auto expected = BuildWalHeader(chunk_coord, store_id, FeatureFlags{});
    constexpr std::size_t kComparedEnd = kWalHeaderSize - 4U;
    std::size_t matched = 0;
    while (matched < wal.size() && matched < kComparedEnd &&
           ((matched >= 12U && matched < 24U) || wal[matched] == expected[matched])) {
        ++matched;
    }
    if (matched == kComparedEnd) {
        // Everything but the CRC matched: whatever part of the CRC was
        // written, no frame can follow a header whose CRC never completed.
        matched = std::min(wal.size(), kWalHeaderSize);
    }
    return std::all_of(
        wal.begin() + static_cast<std::ptrdiff_t>(matched), wal.end(),
        [](std::uint8_t byte) { return byte == 0U; });
}

// Whether a frame header with a valid CRC starts anywhere after `cursor`.
// Acknowledged frames always have one, so a stop with none after it can only
// have lost the frame it stopped at; that is the shape a crash leaves.
[[nodiscard]] bool HasValidFrameHeaderAfter(
    const std::vector<std::uint8_t>& wal,
    std::size_t cursor) {
    for (std::size_t at = cursor + 1U; at + kWalFrameFixedHeaderSize <= wal.size(); ++at) {
        if (std::memcmp(wal.data() + at, kWalFrameMagic, kWalFrameMagicSize) != 0) {
            continue;
        }
        const std::uint16_t tlv_size = ReadLe16(wal, at + 22U);
        const std::size_t crc_at = at + kWalFrameFixedHeaderSize + tlv_size;
        if (crc_at + kWalFrameHeaderCrcSize > wal.size()) {
            continue;
        }
        if (Crc32(wal.data() + at + kWalFrameMagicSize, crc_at - at - kWalFrameMagicSize) ==
            ReadLe32(wal, crc_at)) {
            return true;
        }
    }
    return false;
}

}  // namespace

WalReplayResult ReplayWal(
    const std::vector<std::uint8_t>& wal_bytes,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& store_features,
    std::uint64_t base_revision,
    std::vector<std::uint8_t>* payload,
    std::vector<std::uint8_t>* presence_bitmap,
    ChunkExtra* extra) {
    WalReplayResult result;
    if (payload == nullptr || presence_bitmap == nullptr) {
        throw std::invalid_argument("chunk state outputs must not be null");
    }
    ChunkExtra discarded_extra;
    if (extra == nullptr) {
        extra = &discarded_extra;
    }

    std::string header_error;
    if (wal_bytes.size() < kWalHeaderSize) {
        header_error = "WAL file too small";
    } else {
        try {
            ValidateWalHeader(wal_bytes, chunk_coord, store_id, store_features);
        } catch (const std::exception& e) {
            header_error = e.what();
        }
    }
    if (!header_error.empty()) {
        if (IsInterruptedCreation(wal_bytes, chunk_coord, store_id)) {
            result.torn_creation = true;
            result.stop_reason = "torn_creation";
        } else {
            result.stop_reason = "invalid_header: " + header_error;
        }
        return result;
    }
    result.replayable = true;
    const FeatureFlags file_features{
        .incompat = ReadLe32(wal_bytes, 12U),
        .ro_compat = ReadLe32(wal_bytes, 16U),
        .compat = ReadLe32(wal_bytes, 20U),
    };
    auto state = BuildChunkStateBytes(geometry, *payload, *presence_bitmap);
    const FrameShape shape{
        .payload_boundary = geometry.ChunkPayloadBytes(),
        .state_size = state.size(),
        .block_count = geometry.ChunkBlockCount(),
        .extra_enabled = HasExtraData(store_features),
        .may_skip_unknown = MaySkipUnknownTypes(UnionFeatures(file_features, store_features)),
    };

    std::size_t cursor = kWalHeaderSize;
    ParsedFrame frame;
    std::uint64_t previous_revision = 0;
    while (cursor < wal_bytes.size()) {
        std::string stop_reason;
        bool reaches_end = false;
        bool intact = false;
        bool parsed = ParseFrame(wal_bytes, cursor, shape, &frame, &stop_reason, &reaches_end, &intact);
        // Every mutation reserves a higher revision than the one before it
        // (under the chunk lock), so a frame that does not is damage.
        if (parsed && frame.revision <= previous_revision) {
            parsed = false;
            stop_reason = "frame_revision_order";
            reaches_end = frame.size >= wal_bytes.size() - cursor;
        }
        if (!parsed) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = stop_reason;
            // A whole, checksum-valid frame was written completely, so its
            // failure is damage (a writer bug, or a file from elsewhere) even
            // as the last frame; trimming it would drop a mutation silently.
            result.stopped_at_crash_tail =
                !intact && (reaches_end || !HasValidFrameHeaderAfter(wal_bytes, cursor));
            break;
        }
        previous_revision = frame.revision;
        if (frame.revision <= base_revision) {
            result.skipped_frames += 1;
            cursor += frame.size;
            continue;
        }
        if (!frame.extra_ops.empty() || frame.extra_replace.has_value()) {
            ApplyFrameExtra(wal_bytes, &frame, extra);
        }
        for (const auto& span : frame.spans) {
            std::copy(
                wal_bytes.data() + span.source,
                wal_bytes.data() + span.source + span.size,
                state.begin() + static_cast<std::ptrdiff_t>(span.offset));
        }
        result.applied_records += frame.record_count;
        result.applied_frames += 1;
        result.revision = frame.revision;
        result.commit_time_ms = frame.commit_time_ms;
        cursor += frame.size;
    }
    result.valid_end = cursor;

    SplitChunkStateBytes(geometry, state, payload, presence_bitmap);
    // Every committed state keeps the extra-data invariants, and replay ends
    // in a committed state; anything else is damage.
    if (extra->encoded_size() > kExtraMaxChunkBytesLimit) {
        result.extra_problem = "extra data takes " + std::to_string(extra->encoded_size()) +
                               " bytes, more than " + std::to_string(kExtraMaxChunkBytesLimit);
    }
    for (const auto entry : *extra) {
        if (!BlockPresent(*presence_bitmap, entry.block_index)) {
            result.extra_problem =
                "extra data for absent block index " + std::to_string(entry.block_index);
            break;
        }
    }
    return result;
}

}  // namespace chunkdb
