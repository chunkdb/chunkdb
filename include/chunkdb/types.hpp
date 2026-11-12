#pragma once

#include <cstdint>
#include <functional>

namespace chunkdb {

struct ChunkCoord {
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::int64_t z = 0;

    bool operator==(const ChunkCoord& other) const noexcept {
        return x == other.x && y == other.y && z == other.z;
    }

    bool operator<(const ChunkCoord& other) const noexcept {
        if (x != other.x) return x < other.x;
        if (y != other.y) return y < other.y;
        return z < other.z;
    }
};

struct LargeChunkCoord {
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::int64_t z = 0;

    bool operator==(const LargeChunkCoord& other) const noexcept {
        return x == other.x && y == other.y && z == other.z;
    }

    bool operator<(const LargeChunkCoord& other) const noexcept {
        if (x != other.x) return x < other.x;
        if (y != other.y) return y < other.y;
        return z < other.z;
    }
};

struct ChunkCoordHash {
    std::size_t operator()(const ChunkCoord& c) const noexcept {
        const std::size_t h1 = std::hash<std::int64_t>{}(c.x);
        const std::size_t h2 = std::hash<std::int64_t>{}(c.y);
        const std::size_t hash = h1 ^ (h2 << 1U);
        return c.z == 0 ? hash : hash ^ (std::hash<std::int64_t>{}(c.z) << 2U);
    }
};

struct LargeChunkCoordHash {
    std::size_t operator()(const LargeChunkCoord& c) const noexcept {
        const std::size_t h1 = std::hash<std::int64_t>{}(c.x);
        const std::size_t h2 = std::hash<std::int64_t>{}(c.y);
        const std::size_t hash = h1 ^ (h2 << 1U);
        return c.z == 0 ? hash : hash ^ (std::hash<std::int64_t>{}(c.z) << 2U);
    }
};

}  // namespace chunkdb
