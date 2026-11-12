#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <limits>
#include <unordered_set>

#include "chunkdb/types.hpp"

namespace {
template <typename Coord, typename Hash>
void Distribution(std::uint64_t seed, bool three_axes) {
    constexpr std::size_t count = 8192U;
    const Hash hash(seed);
    std::unordered_set<Coord, Hash> keys(0U, hash);
    keys.reserve(count);
    for (std::int64_t i = 0; i < static_cast<std::int64_t>(count); ++i) {
        const auto z = three_axes ? (i * 17 + 3) : 0;
        // All these keys shared one old XOR/shift hash bucket.
        const Coord coord{(i << 1U) ^ (z << 2U), i, z};
        assert(keys.insert(coord).second);
    }
    std::size_t maximum = 0U;
    for (std::size_t i = 0; i < keys.bucket_count(); ++i) maximum = std::max(maximum, keys.bucket_size(i));
    assert(keys.size() == count && maximum <= 16U);
    assert(hash(Coord{2, 0}) != hash(Coord{0, 1}));
    for (const auto x : {std::numeric_limits<std::int64_t>::min(), std::int64_t{-1}, std::int64_t{0},
                        std::numeric_limits<std::int64_t>::max()}) {
        const Coord coord{x, x, three_axes ? x : 0};
        assert(hash(coord) == hash(coord));
        assert(hash(coord) != hash(Coord{x, x, three_axes ? x ^ 1 : 1}));
        assert(hash(coord) != Hash(seed ^ UINT64_C(0x9e3779b97f4a7c15))(coord));
    }
}
}

int main() {
    for (const auto seed : {UINT64_C(0), UINT64_C(1), UINT64_C(0x123456789abcdef0), UINT64_MAX}) {
        for (const bool three_axes : {false, true}) {
            Distribution<chunkdb::ChunkCoord, chunkdb::ChunkCoordHash>(seed, three_axes);
            Distribution<chunkdb::LargeChunkCoord, chunkdb::LargeChunkCoordHash>(seed, three_axes);
        }
    }
    assert(chunkdb::ChunkCoordHash{}({-1, 2, 3}) == chunkdb::ChunkCoordHash{}({-1, 2, 3}));
    assert(chunkdb::ChunkCoordHash{}({-1, 2, 3}) == chunkdb::LargeChunkCoordHash{}({-1, 2, 3}));
}
