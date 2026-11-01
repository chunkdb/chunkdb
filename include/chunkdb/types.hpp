#pragma once

#include <cstdint>
#include <cstddef>

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

namespace detail {
[[nodiscard]] std::uint64_t CoordinateHashSeed();

[[nodiscard]] constexpr std::uint64_t MixCoordinateWord(std::uint64_t value) noexcept {
    value = (value ^ (value >> 30U)) * UINT64_C(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27U)) * UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31U);
}

[[nodiscard]] constexpr std::size_t HashCoordinates(
    std::int64_t x, std::int64_t y, std::int64_t z, std::uint64_t seed) noexcept {
    auto mixed = MixCoordinateWord(static_cast<std::uint64_t>(x) + seed);
    mixed = MixCoordinateWord(mixed ^ (static_cast<std::uint64_t>(y) + UINT64_C(0x9e3779b97f4a7c15)));
    mixed = MixCoordinateWord(mixed ^ (static_cast<std::uint64_t>(z) + UINT64_C(0xd1b54a32d192ed03)));
    if constexpr (sizeof(std::size_t) < sizeof(mixed)) return static_cast<std::size_t>(mixed ^ (mixed >> 32U));
    return static_cast<std::size_t>(mixed);
}
}  // namespace detail

struct ChunkCoordHash {
    ChunkCoordHash() : seed_(detail::CoordinateHashSeed()) {}
    explicit constexpr ChunkCoordHash(std::uint64_t seed) noexcept : seed_(seed) {}

    [[nodiscard]] std::size_t operator()(const ChunkCoord& c) const noexcept {
        return detail::HashCoordinates(c.x, c.y, c.z, seed_);
    }

  private:
    std::uint64_t seed_;
};

struct LargeChunkCoordHash {
    LargeChunkCoordHash() : seed_(detail::CoordinateHashSeed()) {}
    explicit constexpr LargeChunkCoordHash(std::uint64_t seed) noexcept : seed_(seed) {}

    [[nodiscard]] std::size_t operator()(const LargeChunkCoord& c) const noexcept {
        return detail::HashCoordinates(c.x, c.y, c.z, seed_);
    }

  private:
    std::uint64_t seed_;
};

}  // namespace chunkdb
