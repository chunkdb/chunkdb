#include "wal_replay.hpp"

#include <cstring>
#include <stdexcept>

#include "chunkdb/crc32.hpp"

namespace chunkdb {

void ValidateWalPrefix(std::istream& input, std::uint64_t prefix_bytes, const Geometry& geometry,
    const ChunkCoord& coord, const StoreId& table_id, FeatureFlags features,
    ChunkState state, std::uint64_t schema_version) {
    const auto read_exact = [&](std::uint8_t* at, std::size_t count) {
        input.read(reinterpret_cast<char*>(at), static_cast<std::streamsize>(count));
        if (input.gcount() != static_cast<std::streamsize>(count))
            throw std::runtime_error("failed to read complete rollback WAL prefix");
    };
    const auto header_size = ChunkWalHeaderBytes(geometry.dims());
    if (prefix_bytes < header_size)
        throw std::runtime_error("rollback WAL boundary ends inside its header");
    std::vector<std::uint8_t> header(header_size);
    read_exact(header.data(), header.size());
    ValidateWalHeader(header, coord, table_id, features, geometry.dims());
    if (state.presence_bitmap.empty())
        state.presence_bitmap.assign(ChunkPresenceBitmapBytes(geometry), 0U);
    const auto base = state.version;
    std::uint64_t position = header_size, previous = 0U;
    while (position != prefix_bytes) {
        const auto remaining = prefix_bytes - position;
        if (remaining < kWalFrameFixedHeaderSize)
            throw std::runtime_error("rollback WAL boundary ends inside a frame header");
        std::vector<std::uint8_t> frame(kWalFrameFixedHeaderSize);
        read_exact(frame.data(), frame.size());
        if (std::memcmp(frame.data(), kWalFrameMagic, kWalFrameMagicSize) != 0)
            throw std::runtime_error("rollback WAL has invalid frame magic");
        const auto frame_header = kWalFrameFixedHeaderSize + ReadLe16(frame, 22U) + kWalFrameHeaderCrcSize;
        if (frame_header > remaining)
            throw std::runtime_error("rollback WAL boundary ends inside frame metadata");
        frame.resize(frame_header);
        read_exact(frame.data() + kWalFrameFixedHeaderSize, frame_header - kWalFrameFixedHeaderSize);
        if (ReadLe32(frame, frame_header - 4U) != Crc32(frame.data() + kWalFrameMagicSize,
            frame_header - kWalFrameMagicSize - kWalFrameHeaderCrcSize))
            throw std::runtime_error("rollback WAL frame header checksum mismatch");
        const auto frame_bytes = static_cast<std::uint64_t>(frame_header) + ReadLe32(frame, 28U) + kWalFrameTrailerSize;
        if (frame_bytes > kMaxWalFrameBytes || frame_bytes > remaining)
            throw std::runtime_error("rollback WAL boundary has an invalid frame extent");
        frame.resize(static_cast<std::size_t>(frame_bytes));
        read_exact(frame.data() + frame_header, frame.size() - frame_header);
        const auto info = InspectFeedFrame(frame, geometry, features);
        if (info.revision <= previous)
            throw std::runtime_error("rollback WAL frame revisions do not increase");
        previous = info.revision;
        position += frame_bytes;
        if (info.revision == base && state.written && info.commit_time_ms != state.commit_time_ms)
            throw std::runtime_error("rollback WAL producing time conflicts with its replay base");
        if (info.revision <= base) continue;
        if (info.schema_version < schema_version)
            throw std::runtime_error("rollback WAL frame schemas move backwards");
        if (schema_version == 0U) {
            state.payload.assign(geometry.LayoutAt(info.schema_version).payload_bytes(), 0U);
            schema_version = info.schema_version;
        }
        while (schema_version < info.schema_version) {
            BringChunkColumnsToSchema(geometry, state.written ? schema_version : 0U,
                schema_version + 1U, &state.chunk_columns);
            TranslateChunk(geometry.LayoutAt(schema_version), geometry.LayoutAt(schema_version + 1U),
                state.presence_bitmap, &state.payload, &state.vars);
            ++schema_version;
        }
        (void)ReplayFeedFrame(frame, geometry, &state, features);
    }
}

}  // namespace chunkdb
