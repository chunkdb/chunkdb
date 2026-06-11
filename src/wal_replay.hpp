#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "chunk_store_internal.hpp"

namespace chunkdb {

struct WalReplayResult {
    std::size_t applied_records = 0;
    bool replayable = false;
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
