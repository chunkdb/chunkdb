#pragma once

#include <cstdint>
#include <functional>

namespace chunkdb {

struct ChunkCoord {
    std::int64_t x = 0;
    std::int64_t y = 0;

    bool operator==(const ChunkCoord& other) const noexcept {
        return x == other.x && y == other.y;
    }
};

struct LargeChunkCoord {
    std::int64_t x = 0;
    std::int64_t y = 0;

    bool operator==(const LargeChunkCoord& other) const noexcept {
        return x == other.x && y == other.y;
    }
};

struct ChunkCoordHash {
    std::size_t operator()(const ChunkCoord& c) const noexcept {
        const std::size_t h1 = std::hash<std::int64_t>{}(c.x);
        const std::size_t h2 = std::hash<std::int64_t>{}(c.y);
        return h1 ^ (h2 << 1U);
    }
};

struct LargeChunkCoordHash {
    std::size_t operator()(const LargeChunkCoord& c) const noexcept {
        const std::size_t h1 = std::hash<std::int64_t>{}(c.x);
        const std::size_t h2 = std::hash<std::int64_t>{}(c.y);
        return h1 ^ (h2 << 1U);
    }
};

}  // namespace chunkdb
