#pragma once

#include <cstdint>

#include "chunkdb/types.hpp"

namespace chunkdb {

// Chunk-coordinate corners, or a center and Euclidean radius when around.
struct ChunkAreaSelector {
    std::uint8_t dims = 2U;
    ChunkCoord first{};
    ChunkCoord last{};
    bool around = false;
    std::int64_t radius = 0;
};

}  // namespace chunkdb
