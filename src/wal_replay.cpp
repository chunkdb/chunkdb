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

// A VAR_PUT (with its value at `source`) or VAR_DEL record.
struct PendingVar {
    bool put = false;
    VarKey key{};
    std::uint32_t length = 0;
    std::size_t source = 0;
};

struct ParsedFrame {
    std::uint64_t revision = 0;
    // The schema version its records are laid out by (TLV SCHEMA; 1 without).
    std::uint64_t schema_version = 1;
    std::uint64_t commit_time_ms = 0;
    std::optional<std::string> user;
    bool gc = false;
    std::size_t size = 0;
    std::vector<PendingSpan> spans;
    // Ascending key order; empty when var_replace is set.
    std::vector<PendingVar> var_ops;
    std::optional<ChunkVars> var_replace;
    std::size_t record_count = 0;
};

struct FrameShape {
    // Bytes [0, payload_boundary) of the state are payload, the rest presence.
    std::size_t payload_boundary = 0;
    std::size_t state_size = 0;
    std::size_t block_count = 0;
    // The table has text or bytes columns.
    bool vars_enabled = false;
    bool may_skip_unknown = false;
};

// Parses a value record into `frame`; returns the stop reason, or empty
// when the record is valid.
[[nodiscard]] std::string ParseVarRecord(
    const std::vector<std::uint8_t>& wal,
    std::uint8_t type,
    std::size_t body_at,
    std::uint32_t size,
    const FrameShape& shape,
    ParsedFrame* frame) {
    if (!shape.vars_enabled) {
        return "record_vars_without_columns";
    }
    if (frame->var_replace.has_value()) {
        return "record_vars_order";
    }
    if (type == kWalRecordVarReplace) {
        if (!frame->var_ops.empty()) {
            return "record_vars_order";
        }
        try {
            frame->var_replace = ChunkVars::Decode(wal.data() + body_at, size, shape.block_count);
        } catch (const std::invalid_argument&) {
            return "record_vars_invalid";
        }
        return {};
    }
    const bool put = type == kWalRecordVarPut;
    if (put ? size < kVarEntryHeaderBytes : size != 8U) {
        return "record_vars_invalid";
    }
    PendingVar op{
        .put = put,
        .key = VarKey{.column_id = ReadLe32(wal, body_at), .block_index = ReadLe32(wal, body_at + 4U)},
    };
    if (op.key.block_index >= shape.block_count) {
        return "record_out_of_range";
    }
    if (!frame->var_ops.empty() && op.key <= frame->var_ops.back().key) {
        return "record_vars_order";
    }
    if (put) {
        op.length = ReadLe32(wal, body_at + 8U);
        op.source = body_at + kVarEntryHeaderBytes;
        if (size != kVarEntryHeaderBytes + op.length) {
            return "record_vars_invalid";
        }
    }
    frame->var_ops.push_back(op);
    return {};
}

// Parses and validates one whole frame at `cursor` without applying it.
// Returns false with `stop_reason` set when the frame is torn or invalid.
[[nodiscard]] bool ParseFrame(
    const std::vector<std::uint8_t>& wal,
    std::size_t cursor,
    const Geometry& geometry,
    bool may_skip_unknown,
    bool slots_enabled,
    ParsedFrame* frame,
    std::string* stop_reason,
    bool* reaches_end,
    bool* intact) {
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
    if ((frame_flags & ~kWalFrameGc) != 0U || (frame_flags != 0U && !slots_enabled)) {
        *stop_reason = "frame_flags_unknown";
        return false;
    }
    if (record_count == 0U) {
        *stop_reason = "frame_empty";
        return false;
    }

    // TLV fields, covered by the header CRC.
    bool have_tag = false;
    bool have_schema = false;
    bool have_user = false;
    frame->user.reset();
    frame->gc = (frame_flags & kWalFrameGc) != 0U;
    frame->schema_version = 1;
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
        } else if (type == kWalTlvSchema) {
            if (have_schema || length != 8U) {
                *stop_reason = "tlv_schema_invalid";
                return false;
            }
            have_schema = true;
            frame->schema_version = ReadLe64(wal, at + kWalTlvHeaderSize);
        } else if (type == kWalTlvUser) {
            if (!slots_enabled || have_user || length == 0U) {
                *stop_reason = !slots_enabled ? "tlv_user_without_slots" :
                    (have_user ? "tlv_duplicate_user" : "tlv_empty_user");
                return false;
            }
            have_user = true;
            frame->user.emplace(reinterpret_cast<const char*>(wal.data() + at + kWalTlvHeaderSize), length);
        } else if (!may_skip_unknown) {
            *stop_reason = "tlv_unknown_type";
            return false;
        }
        at += kWalTlvHeaderSize + length;
    }

    // Records are laid out by the frame's schema version, which the table must
    // have reached.
    if (frame->schema_version == 0U || frame->schema_version > geometry.layout().schema().version) {
        *stop_reason = "frame_schema_version";
        return false;
    }
    const ChunkLayout& layout = geometry.LayoutAt(frame->schema_version);
    const FrameShape shape{
        .payload_boundary = layout.payload_bytes(),
        .state_size = layout.payload_bytes() + ChunkPresenceBitmapBytes(geometry),
        .block_count = layout.block_count(),
        .vars_enabled = layout.has_var_columns(),
        .may_skip_unknown = may_skip_unknown,
    };
    const std::size_t payload_boundary = shape.payload_boundary;
    const std::size_t state_size = shape.state_size;

    frame->spans.clear();
    frame->var_ops.clear();
    frame->var_replace.reset();
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
            type == kWalRecordVarPut || type == kWalRecordVarDel || type == kWalRecordVarReplace) {
            auto reason = ParseVarRecord(wal, type, body_at, size, shape, frame);
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
    if (frame->gc) {
        // A collection marker may only reset the complete state to empty.
        // Otherwise suppressing it in catch-up would hide a user mutation.
        bool empty_reset = !have_tag && frame->spans.size() == 2U && frame->var_ops.empty() &&
            record_count == (shape.vars_enabled ? 3U : 2U) &&
            (shape.vars_enabled ? frame->var_replace.has_value() && frame->var_replace->empty()
                                : !frame->var_replace.has_value());
        if (empty_reset) {
            const auto& payload = frame->spans[0];
            const auto& presence = frame->spans[1];
            empty_reset = payload.offset == 0U && payload.size == payload_boundary &&
                presence.offset == payload_boundary && presence.size == state_size - payload_boundary;
            for (const auto& span : frame->spans) {
                empty_reset = empty_reset && std::all_of(wal.data() + span.source,
                    wal.data() + span.source + span.size, [](std::uint8_t byte) { return byte == 0U; });
            }
        }
        if (!empty_reset) {
            *stop_reason = "frame_gc_invalid";
            return false;
        }
    }
    frame->revision = revision;
    frame->commit_time_ms = commit_time_ms;
    frame->size = total;
    frame->record_count = record_count;
    return true;
}

// Applies a frame's value records the way spans apply: blindly, so a WAL
// replayed over a newer image (a crash between a checkpoint's image publish
// and its WAL removal) or over no image (empty-chunk collection) converges
// to the same state. VAR_PUT sets the value, VAR_DEL removes it if present,
// VAR_REPLACE replaces all of them. The invariants hold for the state after
// the last frame, which ReplayWal checks.
void ApplyFrameVars(
    const std::vector<std::uint8_t>& wal,
    ParsedFrame* frame,
    ChunkVars* vars) {
    if (frame->var_replace.has_value()) {
        *vars = std::move(*frame->var_replace);
        frame->var_replace.reset();
        return;
    }
    const auto value_of = [&wal](const PendingVar& op) {
        return std::span<const std::uint8_t>(wal.data() + op.source, op.length);
    };
    if (frame->var_ops.size() == 1U) {
        const auto& op = frame->var_ops.front();
        if (op.put) {
            vars->Assign(op.key, value_of(op));
        } else {
            (void)vars->Remove(op.key);
        }
        return;
    }
    // Many records: one pass over the chunk's values.
    std::vector<VarChange> changes;
    changes.reserve(frame->var_ops.size());
    for (const auto& op : frame->var_ops) {
        const auto value = value_of(op);
        changes.push_back(VarChange{
            .key = op.key,
            .value = op.put ? std::optional<std::vector<std::uint8_t>>(std::vector<std::uint8_t>(value.begin(), value.end()))
                            : std::nullopt,
        });
    }
    *vars = ChunkVars::Merge(*vars, changes);
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

FeedFrameInfo ReplayFeedFrame(
    const std::vector<std::uint8_t>& bytes, const Geometry& geometry, ChunkState* state,
    FeatureFlags features) {
    ParsedFrame frame;
    std::string reason;
    bool reaches_end = false;
    bool intact = false;
    if (!ParseFrame(bytes, 0U, geometry, MaySkipUnknownTypes(features),
                    (features.incompat & kFeatureFeedSlots) != 0U, &frame, &reason, &reaches_end, &intact) ||
        frame.size != bytes.size()) {
        throw std::runtime_error("invalid captured feed frame: " + reason);
    }
    const auto& layout = geometry.LayoutAt(frame.schema_version);
    if (state->payload.size() != layout.payload_bytes() || state->presence_bitmap.size() != ChunkPresenceBitmapBytes(geometry)) {
        throw std::logic_error("captured feed state has another layout");
    }
    for (const auto& span : frame.spans) {
        const auto payload_end = state->payload.size();
        const auto first = std::min(span.offset, payload_end);
        const auto last = std::min(span.offset + span.size, payload_end);
        std::copy_n(bytes.data() + span.source, last - first, state->payload.data() + first);
        if (span.offset + span.size > payload_end) {
            const auto presence_first = std::max(span.offset, payload_end);
            std::copy_n(bytes.data() + span.source + presence_first - span.offset,
                        span.offset + span.size - presence_first,
                        state->presence_bitmap.data() + presence_first - payload_end);
        }
    }
    ApplyFrameVars(bytes, &frame, &state->vars);
    layout.RequireValidVars(state->vars, state->presence_bitmap);
    state->version = frame.revision;
    return {frame.revision, frame.commit_time_ms, frame.schema_version, std::move(frame.user), frame.gc};
}

FeedFrameInfo InspectFeedFrame(
    const std::vector<std::uint8_t>& bytes, const Geometry& geometry, FeatureFlags features) {
    ParsedFrame frame;
    std::string reason;
    bool reaches_end = false;
    bool intact = false;
    if (!ParseFrame(bytes, 0U, geometry, MaySkipUnknownTypes(features),
                    (features.incompat & kFeatureFeedSlots) != 0U, &frame, &reason, &reaches_end, &intact) ||
        frame.size != bytes.size()) {
        throw std::runtime_error("invalid feed archive frame: " + reason);
    }
    return {frame.revision, frame.commit_time_ms, frame.schema_version, std::move(frame.user), frame.gc};
}

WalReplayResult ReplayWal(
    const std::vector<std::uint8_t>& wal_bytes,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& store_features,
    std::uint64_t base_revision,
    std::uint64_t base_schema_version,
    std::vector<std::uint8_t>* payload,
    std::vector<std::uint8_t>* presence_bitmap,
    ChunkVars* vars) {
    WalReplayResult result;
    if (payload == nullptr || presence_bitmap == nullptr) {
        throw std::invalid_argument("chunk state outputs must not be null");
    }
    ChunkVars discarded_vars;
    if (vars == nullptr) {
        vars = &discarded_vars;
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
        // Nothing applies; the state still ends at the current version.
        BringToCurrentSchema(geometry, base_schema_version, *presence_bitmap, payload, vars);
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
    const bool may_skip_unknown = MaySkipUnknownTypes(UnionFeatures(file_features, store_features));
    const std::uint64_t current_version = geometry.layout().schema().version;
    if (base_schema_version > current_version) {
        throw std::invalid_argument("chunk state is of a schema version the table does not have");
    }
    // The state and the schema version it is laid out by; 0 is the empty
    // state of no image, which takes the version of the first frame applied.
    std::uint64_t state_version = base_schema_version;
    std::vector<std::uint8_t> state;
    const auto set_state = [&](const std::vector<std::uint8_t>& state_payload, const std::vector<std::uint8_t>& presence) {
        state = state_payload;
        state.insert(state.end(), presence.begin(), presence.end());
    };
    // Moves the state to a later version (or gives the empty state one).
    const auto move_state_to = [&](std::uint64_t version) {
        const ChunkLayout& to = geometry.LayoutAt(version);
        if (state_version == 0U) {
            set_state(std::vector<std::uint8_t>(to.payload_bytes(), 0U), *presence_bitmap);
        } else {
            const ChunkLayout& from = geometry.LayoutAt(state_version);
            std::vector<std::uint8_t> state_payload(state.begin(), state.begin() + static_cast<std::ptrdiff_t>(from.payload_bytes()));
            const std::vector<std::uint8_t> presence(state.begin() + static_cast<std::ptrdiff_t>(from.payload_bytes()), state.end());
            // One version at a time: each step converts as it recorded.
            for (std::uint64_t step = state_version + 1U; step <= version; ++step) {
                TranslateChunk(geometry.LayoutAt(step - 1U), geometry.LayoutAt(step), presence, &state_payload, vars);
            }
            set_state(state_payload, presence);
        }
        state_version = version;
    };
    if (state_version != 0U) {
        if (payload->size() != geometry.LayoutAt(state_version).payload_bytes() ||
            presence_bitmap->size() != ChunkPresenceBitmapBytes(geometry)) {
            throw std::invalid_argument("chunk state size does not match its schema version");
        }
        set_state(*payload, *presence_bitmap);
    }

    std::size_t cursor = kWalHeaderSize;
    ParsedFrame frame;
    std::uint64_t previous_revision = 0;
    while (cursor < wal_bytes.size()) {
        std::string stop_reason;
        bool reaches_end = false;
        bool intact = false;
        bool parsed =
            ParseFrame(wal_bytes, cursor, geometry, may_skip_unknown,
                       (store_features.incompat & kFeatureFeedSlots) != 0U,
                       &frame, &stop_reason, &reaches_end, &intact);
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
        // Schema versions only grow: a frame written after the state it
        // follows is of that state's version or a later one.
        if (frame.schema_version < state_version) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "frame_schema_version_order";
            result.stopped_at_crash_tail = false;
            break;
        }
        if (frame.schema_version != state_version) {
            move_state_to(frame.schema_version);
        }
        if (!frame.var_ops.empty() || frame.var_replace.has_value()) {
            ApplyFrameVars(wal_bytes, &frame, vars);
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

    if (state_version != current_version) {
        move_state_to(current_version);
    }
    SplitChunkStateBytes(geometry, state, payload, presence_bitmap);
    // Every committed state keeps the value invariants, and replay ends in a
    // committed state; anything else is damage.
    if (vars->encoded_size() > kVarMaxChunkBytesLimit) {
        result.vars_problem = "values take " + std::to_string(vars->encoded_size()) + " bytes, more than " +
                              std::to_string(kVarMaxChunkBytesLimit);
    } else {
        try {
            geometry.layout().RequireValidVars(*vars, *presence_bitmap);
        } catch (const std::invalid_argument& e) {
            result.vars_problem = e.what();
        }
    }
    return result;
}

}  // namespace chunkdb
