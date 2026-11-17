#include <cassert>
#include <cstdint>
#include <limits>
#include <vector>

#include "area_cursor.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::detail;
constexpr auto minimum = std::numeric_limits<std::int64_t>::min();
constexpr auto maximum = std::numeric_limits<std::int64_t>::max();
constexpr auto umaximum = std::numeric_limits<std::uint64_t>::max();

static_assert(AreaSquare(umaximum).high == umaximum - 1U && AreaSquare(umaximum).low == 1U);
static_assert(AreaSquare(UINT64_C(1) << 32U).high == 1U && AreaSquare(UINT64_C(1) << 32U).low == 0U);
static_assert(AreaSquareRoot(AreaSquare(umaximum), umaximum) == umaximum);
static_assert(AreaSquareRoot({umaximum, umaximum}, umaximum) == umaximum);

std::vector<ChunkCoord> Collect(const AreaCursor& cursor) {
    std::vector<ChunkCoord> values;
    for (auto next = std::optional{cursor.First()}; next; next = cursor.Next(*next)) {
        assert(cursor.Contains(*next));
        assert(values.empty() || values.back() < *next);
        assert(values.size() < 100000U);
        values.push_back(*next);
    }
    return values;
}

bool Add(std::int64_t center, std::int64_t offset, std::int64_t* result) {
    if ((offset < 0 && center < minimum - offset) || (offset > 0 && center > maximum - offset)) return false;
    *result = center + offset;
    return true;
}

std::vector<ChunkCoord> BallOracle(std::uint8_t dims, ChunkCoord center, std::int64_t radius) {
    std::vector<ChunkCoord> expected;
    for (auto dx = -radius; dx <= radius; ++dx) {
        for (auto dy = -radius; dy <= radius; ++dy) {
            for (auto dz = dims == 3U ? -radius : 0; dz <= (dims == 3U ? radius : 0); ++dz) {
                if (dx * dx + dy * dy + dz * dz > radius * radius) continue;
                ChunkCoord coord;
                if (Add(center.x, dx, &coord.x) && Add(center.y, dy, &coord.y) &&
                    (dims == 2U || Add(center.z, dz, &coord.z))) expected.push_back(coord);
            }
        }
    }
    return expected;
}

void ExactShapes() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        ChunkAreaSelector range{.dims = dims, .first = {-2, -1, dims == 3U ? -1 : 0}, .last = {1, 1, dims == 3U ? 2 : 0}};
        std::vector<ChunkCoord> expected;
        for (auto x = -2; x <= 1; ++x) for (auto y = -1; y <= 1; ++y)
            for (auto z = dims == 3U ? -1 : 0; z <= (dims == 3U ? 2 : 0); ++z) expected.push_back({x, y, z});
        assert(Collect(AreaCursor(range)) == expected);
        for (std::int64_t radius = 0; radius <= 9; ++radius) {
            ChunkAreaSelector ball{.dims = dims, .first = {-2, 3, dims == 3U ? -1 : 0}, .around = true, .radius = radius};
            assert(Collect(AreaCursor(ball)) == BallOracle(dims, ball.first, radius));
        }
        for (auto center : {ChunkCoord{minimum, minimum, minimum}, ChunkCoord{maximum, maximum, maximum},
                            ChunkCoord{minimum, maximum, minimum}}) {
            if (dims == 2U) center.z = 0;
            ChunkAreaSelector ball{.dims = dims, .first = center, .around = true, .radius = 2};
            assert(Collect(AreaCursor(ball)) == BallOracle(dims, center, 2));
        }
    }
}

void ExtremeProgress() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        const AreaCursor range({.dims = dims, .first = {minimum, minimum, dims == 3U ? minimum : 0},
            .last = {maximum, maximum, dims == 3U ? maximum : 0}});
        const auto next = range.Next({minimum, maximum, dims == 3U ? maximum : 0});
        assert(next && *next == (ChunkCoord{minimum + 1, minimum, dims == 3U ? minimum : 0}));
        assert(!range.Next({maximum, maximum, dims == 3U ? maximum : 0}));
        for (auto center : {ChunkCoord{}, ChunkCoord{minimum, minimum, minimum}, ChunkCoord{maximum, maximum, maximum}}) {
            if (dims == 2U) center.z = 0;
            const AreaCursor ball({.dims = dims, .first = center, .around = true, .radius = maximum});
            auto current = ball.First();
            for (unsigned count = 0U; count < 128U; ++count) {
                assert(ball.Contains(current));
                const auto following = ball.Next(current);
                assert(following && current < *following);
                current = *following;
            }
            const ChunkCoord last = center.x == 0 ? ChunkCoord{maximum, 0, 0} : center.x == minimum
                ? ChunkCoord{-1, minimum, dims == 3U ? minimum : 0}
                : ChunkCoord{maximum, maximum, dims == 3U ? maximum : 0};
            assert(ball.Contains(last) && !ball.Next(last));
        }
    }
}

void InvalidSelectors() {
    const auto reject = [](ChunkAreaSelector selector) {
        bool refused = false;
        try { (void)AreaCursor(selector); }
        catch (const std::invalid_argument&) { refused = true; }
        assert(refused);
    };
    reject({.dims = 1U});
    reject({.dims = 2U, .first = {0, 0, 1}});
    reject({.dims = 3U, .first = {0, 0, 1}, .last = {0, 0, 0}});
    reject({.around = true, .radius = -1});
    const AreaCursor ball({.around = true, .radius = 1});
    bool refused = false;
    try { (void)ball.Next({1, 1}); }
    catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
    const AreaCursor ball3({.dims = 3U, .around = true, .radius = 1});
    assert(ball3.Contains({0, 0, 1}) && ball3.Contains({-1, 0, 0}));
    for (const auto coord : {ChunkCoord{1, 0, 1}, ChunkCoord{0, 1, 1}}) {
        assert(!ball3.Contains(coord));
        refused = false;
        try { (void)ball3.Next(coord); }
        catch (const std::invalid_argument&) { refused = true; }
        assert(refused);
    }
    const AreaCursor wide({.dims = 3U, .around = true, .radius = maximum});
    assert(wide.Contains({maximum, 0, 0}) && wide.Contains({0, 0, maximum}));
    assert(!wide.Contains({maximum, 1, 0}) && !wide.Contains({0, maximum, 1}) &&
        !wide.Contains({maximum, maximum, maximum}));
    const AreaCursor clipped({.dims = 3U, .first = {minimum, minimum, minimum}, .around = true, .radius = maximum});
    assert(clipped.Contains({-1, minimum, minimum}));
    assert(!clipped.Contains({-1, minimum + 1, minimum}));
}

void ExactCountAdmission() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        const ChunkAreaSelector range{.dims = dims, .first = {-2, -1, dims == 3U ? -1 : 0},
            .last = {1, 1, dims == 3U ? 2 : 0}};
        const auto count = dims == 3U ? 48U : 12U;
        assert(!AreaRequestedExceeds(range, count));
        assert(AreaRequestedExceeds(range, count - 1U));
        assert(AreaRequestedExceeds(range, 0U));
        for (std::int64_t radius = 0; radius <= 9; ++radius) {
            const ChunkAreaSelector ball{.dims = dims, .first = {}, .around = true, .radius = radius};
            const auto cells = BallOracle(dims, {}, radius).size();
            assert(!AreaRequestedExceeds(ball, cells));
            assert(AreaRequestedExceeds(ball, cells - 1U));
            auto clipped = ball;
            clipped.first = {minimum, maximum, dims == 3U ? minimum : 0};
            assert(!AreaRequestedExceeds(clipped, cells));
            assert(AreaRequestedExceeds(clipped, cells - 1U));
        }
        assert(AreaRequestedExceeds({.dims = dims, .first = {}, .around = true, .radius = maximum}, 4096U));
        assert(AreaRequestedExceeds({.dims = dims, .first = {minimum, minimum, dims == 3U ? minimum : 0},
            .last = {maximum, maximum, dims == 3U ? maximum : 0}}, umaximum));
    }
    assert(!AreaRequestedExceeds({.first = {minimum, 0}, .last = {maximum - 1, 0}}, umaximum));
    assert(AreaRequestedExceeds({.first = {minimum, 0}, .last = {maximum, 0}}, umaximum));
}
}  // namespace

int main() {
    ExactShapes();
    ExtremeProgress();
    InvalidSelectors();
    ExactCountAdmission();
}
