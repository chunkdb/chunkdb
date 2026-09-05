#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "chunk_store_internal.hpp"

namespace chunkdb {

struct WalReplayResult {
    std::size_t applied_records = 0;
    // v4 frames applied (zero for 1.x streams).
    std::size_t applied_frames = 0;
    // Revision carried by the last applied frame; zero when none applied.
    std::uint64_t revision = 0;
    // WAL file version from the header (zero for a headerless stream).
    std::uint16_t wal_version = 0;
    bool replayable = false;
    // True when the stream was (or ended) in the 1.x record layout; a writer
    // must then emit a fresh v4 header before appending frames.
    bool legacy_records = false;
    bool tail_truncated_or_corrupt = false;
    std::string stop_reason;
};

[[nodiscard]] WalReplayResult ReplayWal(
    const std::vector<std::uint8_t>& wal_bytes,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    std::vector<std::uint8_t>* payload,
    std::vector<std::uint8_t>* presence_bitmap);

}  // namespace chunkdb
