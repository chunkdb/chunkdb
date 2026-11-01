#include <algorithm>
#include <cassert>
#include <functional>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "chunkdb/crc32.hpp"
#include "wal_replay.hpp"
#include "wal_writer.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
const StoreId kTable{1U};
const FeatureFlags kFeatures{.incompat = kChunkModelFormatBit};

void Reject(const std::function<void()>& action) {
    bool refused = false;
    try { action(); } catch (const std::exception&) { refused = true; }
    assert(refused);
}
void Append(Bytes& bytes, const Bytes& next) { bytes.insert(bytes.end(), next.begin(), next.end()); }
Bytes Frame(const Geometry& geometry, std::uint64_t revision) {
    Bytes bytes;
    WalFrameBuilder frame(&bytes, 1U);
    const std::uint8_t value = static_cast<std::uint8_t>(revision);
    frame.AppendSpan(0U, &value, 1U);
    const std::uint8_t present = 1U;
    frame.AppendSpan(static_cast<std::uint32_t>(geometry.ChunkPayloadBytes()), &present, 1U);
    (void)frame.Finish(revision, revision * 10U);
    return bytes;
}
void Validate(const Bytes& bytes, std::uint64_t cut, const Geometry& geometry, ChunkCoord coord = {}) {
    std::istringstream input(std::string(bytes.begin(), bytes.end()));
    ValidateWalPrefix(input, cut, geometry, coord, kTable, kFeatures);
}

void OneFrameAtATimeAndExactCuts() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        GeometryConfig config{2U, 2U, 2U, 2U, 8U};
        config.dims = dims;
        config.chunk_depth_blocks = dims == 3U ? 2U : 1U;
        const Geometry geometry(config);
        const ChunkCoord coord{-1, 2, dims == 3U ? -3 : 0};
        auto bytes = BuildWalHeader(coord, kTable, kFeatures, dims);
        const auto header_size = bytes.size();
        std::vector<std::size_t> cuts{header_size};
        for (std::uint64_t revision = 1U; revision <= 200U; ++revision) {
            Append(bytes, Frame(geometry, revision));
            cuts.push_back(bytes.size());
        }
        struct BoundedReadBuffer : std::stringbuf {
            explicit BoundedReadBuffer(const std::string& text) : std::stringbuf(text) {}
            std::streamsize largest = 0;
            std::uint64_t consumed = 0;
            std::streamsize xsgetn(char* at, std::streamsize count) override {
                if (count > 512) throw std::length_error("whole WAL read is forbidden");
                largest = std::max(largest, count);
                const auto read = std::stringbuf::xsgetn(at, count);
                consumed += static_cast<std::uint64_t>(read);
                return read;
            }
        } buffer(std::string(bytes.begin(), bytes.end()));
        std::istream stream(&buffer);
        ValidateWalPrefix(stream, cuts[199], geometry, coord, kTable, kFeatures);
        assert(buffer.largest <= 512 && buffer.consumed == cuts[199]);
        assert(bytes.size() > static_cast<std::size_t>(buffer.largest));
        for (std::size_t cut = 0U; cut <= cuts[3]; ++cut) {
            if (std::find(cuts.begin(), cuts.end(), cut) != cuts.end()) Validate(bytes, cut, geometry, coord);
            else Reject([&] { Validate(bytes, cut, geometry, coord); });
        }
        auto duplicate = BuildWalHeader(coord, kTable, kFeatures, dims);
        Append(duplicate, Frame(geometry, 1U)); Append(duplicate, Frame(geometry, 1U));
        Reject([&] { Validate(duplicate, duplicate.size(), geometry, coord); });
        const auto one_end = cuts[1];
        for (std::size_t at = 0U; at < one_end; ++at) {
            auto damaged = bytes;
            damaged[at] ^= 1U;
            Reject([&] { Validate(damaged, one_end, geometry, coord); });
        }
    }
}

void SharedSchemaHistoryIsAppliedBeforeEachFrame() {
    auto blocks = SingleBitsColumnSchema(8U);
    TableSchema chunks;
    chunks.next_column_id = 2U;
    chunks.columns = {{.id = 1U, .name = "owner", .type = {ColumnKind::kUnsigned, 8U}, .default_value = {}}};
    const auto first_chunks = chunks;
    blocks = ChangeColumnType(blocks, "bits", {ColumnKind::kBits, 16U}, Conversion::kExact);
    chunks = AdvanceOtherDomain(chunks, ColumnDomain::kChunk);
    chunks = RenameChunkColumn(chunks, "owner", "renamed");
    blocks = AdvanceOtherDomain(blocks, ColumnDomain::kBlock);
    const Geometry geometry({2U, 2U, 2U, 2U, 16U}, blocks, chunks);
    auto bytes = BuildWalHeader({}, kTable, kFeatures);
    for (std::uint64_t version = 1U; version <= 3U; ++version) {
        Bytes frame_bytes;
        WalFrameBuilder frame(&frame_bytes, version);
        auto payload = Bytes(geometry.LayoutAt(version).payload_bytes(), 0U);
        payload[0] = static_cast<std::uint8_t>(version);
        payload.push_back(1U);
        frame.AppendSpan(0U, payload.data(), payload.size());
        if (version != 2U)
            frame.AppendChunkColumns(EncodeChunkColumns(version == 1U ? first_chunks : chunks,
                {ColumnValue{std::uint64_t{7U}}}));
        (void)frame.Finish(version, version * 10U);
        Append(bytes, frame_bytes);
    }
    Validate(bytes, bytes.size(), geometry);
    auto bad = bytes;
    bad.back() ^= 1U;
    Reject([&] { Validate(bad, bad.size(), geometry); });
}

void WriterAndReaderApplyTheSameFrameCeiling() {
    // Old deletions and new empty nullable entries maximize record overhead.
    constexpr auto old_deletes = (kVarMaxChunkBytesLimit / kVarEntryHeaderBytes) * (kWalRecordHeaderSize + 8ULL);
    constexpr auto new_values = kVarMaxChunkBytesLimit + (kVarMaxChunkBytesLimit / kVarEntryHeaderBytes) * kWalRecordHeaderSize;
    constexpr auto fixed_row = kMaxChunkColumnsSectionBytes - kVarMaxChunkBytesLimit;
    constexpr auto envelope = 65535ULL + kWalFrameFixedHeaderSize + kWalFrameHeaderCrcSize + kWalFrameTrailerSize;
    constexpr auto worst = old_deletes + new_values + fixed_row + 64ULL * 1024ULL * 1024ULL +
        1048576ULL / 8ULL + kMaxColumnsPerTable * (kWalRecordHeaderSize + kWalSpanOffsetSize) + envelope;
    static_assert(worst < kMaxWalFrameBytes);
    Bytes oversized(static_cast<std::size_t>(kMaxWalFrameBytes), 0U), batch;
    WalFrameBuilder writer(&batch, 1U);
    const auto before = batch;
    Reject([&] { writer.AppendSpan(0U, oversized.data(), oversized.size()); });
    assert(batch == before);
    Reject([&] { writer.AppendVarPut({1U, 0U}, oversized); });
    assert(batch == before);
    oversized.clear(); oversized.shrink_to_fit();

    const Geometry geometry({2U, 2U, 2U, 2U, 8U});
    auto frame = Frame(geometry, 1U);
    const auto header = kWalFrameFixedHeaderSize + kWalFrameHeaderCrcSize;
    const auto put32 = [&](std::size_t at, std::uint32_t value) {
        for (unsigned i = 0U; i < 4U; ++i) frame[at + i] = static_cast<std::uint8_t>(value >> (8U * i));
    };
    put32(28U, static_cast<std::uint32_t>(kMaxWalFrameBytes));
    put32(header - 4U, Crc32(frame.data() + kWalFrameMagicSize,
        header - kWalFrameMagicSize - kWalFrameHeaderCrcSize));
    auto bytes = BuildWalHeader({}, kTable, kFeatures);
    Append(bytes, frame);
    Reject([&] { Validate(bytes, bytes.size(), geometry); });
    bool frame_limit = false;
    try { (void)InspectFeedFrame(frame, geometry, kFeatures); }
    catch (const std::runtime_error& error) {
        frame_limit = std::string(error.what()).find("byte limit") != std::string::npos;
    }
    assert(frame_limit);
    // The checked declaration is refused before trying to allocate/read its body.
    std::istringstream input(std::string(bytes.begin(), bytes.end()));
    bool extent_limit = false;
    try { ValidateWalPrefix(input, bytes.size() + kMaxWalFrameBytes, geometry, {}, kTable, kFeatures); }
    catch (const std::runtime_error& error) {
        extent_limit = std::string(error.what()).find("invalid frame extent") != std::string::npos;
    }
    assert(extent_limit);
}
}

int main() {
    OneFrameAtATimeAndExactCuts();
    SharedSchemaHistoryIsAppliedBeforeEachFrame();
    WriterAndReaderApplyTheSameFrameCeiling();
}
