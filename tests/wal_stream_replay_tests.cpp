#include <algorithm>
#include <cassert>
#include <cstring>
#include <sstream>
#include <tuple>

#include "chunkdb/crc32.hpp"
#include "feature_flags.hpp"
#include "wal_replay.hpp"
#include "wal_writer.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
const StoreId kId{1U, 2U, 3U, 4U};
const FeatureFlags kFeatures{.incompat = kFeatureChunkModel};

Geometry MakeGeometry() {
    GeometryConfig config;
    config.chunk_width_blocks = 2U;
    config.chunk_height_blocks = 1U;
    config.block_bits = 8U;
    return Geometry(config);
}

Bytes Header() { return BuildWalHeader({0, 0}, kId, kFeatures); }

Bytes Frame(std::uint64_t revision, std::uint64_t schema = 1U) {
    Bytes bytes;
    WalFrameBuilder frame(&bytes, schema);
    const Bytes state{static_cast<std::uint8_t>(revision), 42U, 3U};
    frame.AppendSpan(0U, state.data(), state.size());
    (void)frame.Finish(revision, revision * 10U);
    return bytes;
}

void Append(Bytes& target, const Bytes& bytes) { target.insert(target.end(), bytes.begin(), bytes.end()); }

struct Outcome {
    WalReplayResult result;
    ChunkState state;
    std::vector<WalFrameBoundary> boundaries;
    std::string error;
};

Outcome Run(const Bytes& bytes, const Geometry& geometry, bool stream,
    std::uint64_t base = 0U, std::uint64_t schema = 0U, ChunkState seed = {}) {
    Outcome outcome;
    outcome.state = std::move(seed);
    if (outcome.state.presence_bitmap.empty())
        outcome.state.presence_bitmap.assign(ChunkPresenceBitmapBytes(geometry), 0U);
    try {
        auto& state = outcome.state;
        if (stream) {
            std::istringstream input(std::string(bytes.begin(), bytes.end()), std::ios::binary);
            outcome.result = ReplayWalStream(input, bytes.size(), geometry, {0, 0}, kId, kFeatures,
                base, schema, &state.payload, &state.presence_bitmap, &state.vars, &outcome.boundaries, &state.chunk_columns);
        } else {
            outcome.result = ReplayWal(bytes, geometry, {0, 0}, kId, kFeatures,
                base, schema, &state.payload, &state.presence_bitmap, &state.vars, &outcome.boundaries, &state.chunk_columns);
        }
    } catch (const std::exception& error) { outcome.error = error.what(); }
    return outcome;
}

void Same(const Outcome& left, const Outcome& right) {
    const auto fields = [](const auto& value) {
        return std::tie(value.applied_records, value.applied_frames, value.skipped_frames, value.revision,
            value.commit_time_ms, value.replayable, value.torn_creation, value.tail_truncated_or_corrupt,
            value.stopped_at_crash_tail, value.valid_end, value.stop_reason, value.vars_problem);
    };
    assert(left.error == right.error);
    assert(fields(left.result) == fields(right.result));
    assert(left.state.payload == right.state.payload && left.state.presence_bitmap == right.state.presence_bitmap);
    assert(left.state.vars == right.state.vars);
    assert(static_cast<bool>(left.state.chunk_columns) == static_cast<bool>(right.state.chunk_columns));
    if (left.state.chunk_columns) assert(left.state.chunk_columns->bytes == right.state.chunk_columns->bytes);
    assert(left.boundaries.size() == right.boundaries.size());
    for (std::size_t i = 0U; i < left.boundaries.size(); ++i)
        assert(left.boundaries[i].revision == right.boundaries[i].revision && left.boundaries[i].end == right.boundaries[i].end);
}

void Check(const Bytes& bytes, const Geometry& geometry) { Same(Run(bytes, geometry, false), Run(bytes, geometry, true)); }

void CutsAndDamage() {
    const auto geometry = MakeGeometry();
    auto bytes = Header();
    Append(bytes, Frame(2U));
    Append(bytes, Frame(7U));
    const auto valid = Run(bytes, geometry, true);
    assert(valid.error.empty() && valid.result.applied_frames == 2U && valid.result.revision == 7U);
    assert(valid.result.valid_end == bytes.size() && valid.boundaries.size() == 2U);
    for (std::size_t end = 0U; end <= bytes.size(); ++end)
        Check(Bytes(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(end)), geometry);
    for (std::size_t at = 0U; at < bytes.size(); ++at) {
        auto corrupt = bytes;
        corrupt[at] ^= 1U;
        Check(corrupt, geometry);
    }
    for (const auto revision : {0U, 2U, 1U}) {
        auto order = Header();
        Append(order, Frame(2U));
        Append(order, Frame(revision));
        Check(order, geometry);
        assert(Run(order, geometry, true).result.stop_reason == "frame_revision_order");
    }
    ChunkState image;
    image.payload = {9U, 9U};
    image.presence_bitmap = {3U};
    const auto memory = Run(bytes, geometry, false, 7U, 1U, image);
    const auto stream = Run(bytes, geometry, true, 7U, 1U, image);
    Same(memory, stream);
    assert(stream.result.skipped_frames == 2U && stream.state.payload == image.payload);
}

void InterruptedCreationAndInteriorSearch() {
    const auto geometry = MakeGeometry();
    for (const auto size : {0U, 1U, 65U, 65536U, 131073U}) {
        const Bytes zero(size, 0U);
        Check(zero, geometry);
        assert(Run(zero, geometry, true).result.torn_creation);
        assert(IsInterruptedWalCreation(zero, {0, 0}, kId));
    }
    auto interrupted = Bytes(Header().size() + 131073U, 0U);
    const auto expected = Header();
    std::copy_n(expected.begin(), 20U, interrupted.begin());
    Check(interrupted, geometry);
    assert(Run(interrupted, geometry, true).result.torn_creation);
    interrupted.back() = 1U;
    Check(interrupted, geometry);
    assert(!Run(interrupted, geometry, true).result.torn_creation);
    auto prefix = Header();
    prefix.resize(prefix.size() + 131073U, 0U);
    prefix[32U] ^= 1U;
    Check(prefix, geometry);
    auto first = Frame(2U);
    first.back() ^= 1U;
    for (const auto offset : {65533U, 65534U, 65535U, 65536U, 65537U}) {
        auto interior = Header();
        const auto stopped = interior.size();
        Append(interior, first);
        interior.resize(stopped + offset, 0U);
        Append(interior, Frame(7U));
        Check(interior, geometry);
        assert(!Run(interior, geometry, true).result.stopped_at_crash_tail);
    }
    auto tail = Header();
    Append(tail, first);
    tail.resize(tail.size() + 131073U, 0U);
    Check(tail, geometry);
    assert(Run(tail, geometry, true).result.stopped_at_crash_tail);
    // A later header counts even if its TLV region spans an entire scan block.
    auto later = Frame(7U);
    const auto fixed = kWalFrameFixedHeaderSize;
    Bytes large(later.begin(), later.begin() + static_cast<std::ptrdiff_t>(fixed));
    large[22U] = 255U; large[23U] = 255U;
    large.resize(fixed + 65535U, 0U);
    WriteLe32(large, Crc32(large.data() + kWalFrameMagicSize, large.size() - kWalFrameMagicSize));
    Append(tail, large);
    Check(tail, geometry);
    assert(!Run(tail, geometry, true).result.stopped_at_crash_tail);
}

void SchemaMetadataAndFinalInvariants() {
    const auto initial = MakeGeometry();
    auto chunks = AddChunkColumn(TableSchema{}, {.id = 0U, .name = "owner",
        .type = {ColumnKind::kUnsigned, 8U}, .has_default = true, .default_value = {7U}});
    auto blocks = AdvanceOtherDomain(initial.layout().schema(), ColumnDomain::kBlock);
    blocks = AddColumn(blocks, {.id = 0U, .name = "extra", .type = {ColumnKind::kUnsigned, 8U},
        .has_default = true, .default_value = {9U}});
    chunks = AdvanceOtherDomain(chunks, ColumnDomain::kChunk);
    auto config = initial.config();
    config.block_bits = FixedBitsPerBlock(blocks);
    const Geometry geometry(config, blocks, chunks);
    auto bytes = Header();
    Append(bytes, Frame(1U));
    {
        WalFrameBuilder frame(&bytes, 2U);
        frame.AppendChunkColumns(EncodeChunkColumns(geometry.ChunkColumnsLayoutAt(2U).schema(), {std::uint64_t{11U}}));
        (void)frame.Finish(2U, 20U);
    }
    {
        WalFrameBuilder frame(&bytes, 3U);
        const std::uint8_t value = 5U;
        frame.AppendSpan(0U, &value, 1U);
        (void)frame.Finish(3U, 30U);
    }
    Check(bytes, geometry);
    const auto state = Run(bytes, geometry, true);
    assert(state.error.empty() && state.result.vars_problem.empty() && state.state.payload.size() == 4U);
    assert(state.state.chunk_columns &&
        std::get<std::uint64_t>(DecodeChunkColumns(chunks, state.state.chunk_columns->bytes).front()) == 11U);
    assert(state.state.payload.back() == 9U);
    // Replaying a higher revision in an older schema is damage, including at EOF.
    Append(bytes, Frame(4U, 1U));
    Check(bytes, geometry);
    assert(Run(bytes, geometry, true).result.stop_reason == "frame_schema_version_order");

    auto variables = AddColumn(initial.layout().schema(), {.id = 0U, .name = "text",
        .type = {ColumnKind::kText, 8U}, .nullable = true, .default_value = {}});
    config.block_bits = FixedBitsPerBlock(variables);
    const Geometry variable_geometry(config, variables);
    bytes = Header();
    {
        WalFrameBuilder frame(&bytes, 2U);
        const Bytes text{'x'};
        frame.AppendVarPut({2U, 0U}, text);
        (void)frame.Finish(1U, 10U);
    }
    Check(bytes, variable_geometry);
    assert(!Run(bytes, variable_geometry, true).result.vars_problem.empty());
    {
        WalFrameBuilder frame(&bytes, 2U);
        const std::uint8_t present = 1U;
        frame.AppendSpan(2U, &present, 1U);
        (void)frame.Finish(2U, 20U);
    }
    Check(bytes, variable_geometry);
    assert(Run(bytes, variable_geometry, true).result.vars_problem.empty());

    config = initial.config();
    config.dims = 3U;
    config.chunk_width_blocks = 1U;
    config.chunk_depth_blocks = 2U;
    const Geometry three(config);
    bytes = BuildWalHeader({0, 0, 0}, kId, kFeatures, 3U);
    Append(bytes, Frame(1U));
    Check(bytes, three);
    assert(Run(bytes, three, true).result.applied_frames == 1U);
}

class LimitedBuffer final : public std::stringbuf {
  public:
    LimitedBuffer(const Bytes& bytes, std::streamsize maximum) :
        std::stringbuf(std::string(bytes.begin(), bytes.end()), std::ios::in | std::ios::binary), maximum_(maximum) {}
    std::streamsize xsgetn(char* target, std::streamsize count) override {
        if (count > maximum_) throw std::runtime_error("unbounded WAL stream read");
        largest = std::max(largest, count);
        return std::stringbuf::xsgetn(target, count);
    }
    std::streamsize largest = 0;
  private:
    std::streamsize maximum_;
};

void BoundedReadsAndExtent() {
    const auto geometry = MakeGeometry();
    auto bytes = Header();
    for (std::uint64_t revision = 1U; revision <= 4096U; ++revision) Append(bytes, Frame(revision));
    LimitedBuffer buffer(bytes, 1024);
    std::istream input(&buffer);
    Bytes payload, presence(1U, 0U);
    ChunkVars vars;
    const auto result = ReplayWalStream(input, bytes.size(), geometry, {0, 0}, kId, kFeatures,
        0U, 0U, &payload, &presence, &vars);
    assert(result.applied_frames == 4096U && result.revision == 4096U && buffer.largest <= 1024);
    const auto prefix = Header().size() + Frame(1U).size();
    std::istringstream with_suffix(std::string(bytes.begin(), bytes.end()), std::ios::binary);
    payload.clear(); presence.assign(1U, 0U);
    const auto one = ReplayWalStream(with_suffix, prefix, geometry, {0, 0}, kId, kFeatures,
        0U, 0U, &payload, &presence, &vars);
    assert(one.applied_frames == 1U && one.valid_end == prefix);
    std::istringstream short_file(std::string(bytes.begin(), bytes.end() - 1), std::ios::binary);
    bool failed = false;
    try { (void)ReplayWalStream(short_file, bytes.size(), geometry, {0, 0}, kId, kFeatures,
        0U, 0U, &payload, &presence, &vars); }
    catch (const std::runtime_error&) { failed = true; }
    assert(failed);
    struct NoSeek final : std::stringbuf {
        pos_type seekoff(off_type, std::ios::seekdir, std::ios::openmode) override { return pos_type(off_type(-1)); }
        pos_type seekpos(pos_type, std::ios::openmode) override { return pos_type(off_type(-1)); }
    } no_seek;
    std::istream unseekable(&no_seek);
    failed = false;
    try { (void)ReplayWalStream(unseekable, 0U, geometry, {0, 0}, kId, kFeatures,
        0U, 0U, &payload, &presence, &vars); }
    catch (const std::runtime_error&) { failed = true; }
    assert(failed);
    auto oversized = Header();
    auto large = Frame(1U);
    for (unsigned i = 0U; i < 4U; ++i)
        large[28U + i] = static_cast<std::uint8_t>(kMaxWalFrameBytes >> (8U * i));
    const auto crc = Crc32(large.data() + kWalFrameMagicSize, kWalFrameFixedHeaderSize - kWalFrameMagicSize);
    for (unsigned i = 0U; i < 4U; ++i)
        large[kWalFrameFixedHeaderSize + i] = static_cast<std::uint8_t>(crc >> (8U * i));
    large.resize(kWalFrameFixedHeaderSize + kWalFrameHeaderCrcSize);
    Append(oversized, large);
    Check(oversized, geometry);
    assert(Run(oversized, geometry, true).error == "WAL frame exceeds its byte limit");
}
}  // namespace

int main() {
    CutsAndDamage();
    InterruptedCreationAndInteriorSearch();
    SchemaMetadataAndFinalInvariants();
    BoundedReadsAndExtent();
}
