#pragma once

#include <bit>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>

#include "chunkdb/area_selector.hpp"

namespace chunkdb::detail {

struct AreaWide {
    std::uint64_t high = 0U;
    std::uint64_t low = 0U;
};

inline constexpr bool AreaLessEqual(AreaWide first, AreaWide second) noexcept {
    return first.high < second.high || (first.high == second.high && first.low <= second.low);
}

inline constexpr AreaWide AreaSquare(std::uint64_t value) noexcept {
    const auto low = value & UINT64_C(0xffffffff);
    const auto high = value >> 32U;
    const auto middle = low * high;
    AreaWide result{high * high, low * low};
    for (unsigned i = 0U; i < 2U; ++i) {
        const auto previous = result.low;
        result.low += middle << 32U;
        result.high += (middle >> 32U) + (result.low < previous ? 1U : 0U);
    }
    return result;
}

inline constexpr AreaWide AreaAdd(AreaWide first, AreaWide second) noexcept {
    const auto low = first.low + second.low;
    return {first.high + second.high + (low < first.low ? 1U : 0U), low};
}

inline constexpr AreaWide AreaSubtract(AreaWide first, AreaWide second) noexcept {
    return {first.high - second.high - (first.low < second.low ? 1U : 0U), first.low - second.low};
}

inline constexpr std::uint64_t AreaSquareRoot(AreaWide value, std::uint64_t upper) noexcept {
    std::uint64_t lower = 0U;
    while (lower < upper) {
        const auto difference = upper - lower;
        const auto middle = lower + difference / 2U + difference % 2U;
        if (AreaLessEqual(AreaSquare(middle), value)) lower = middle;
        else upper = middle - 1U;
    }
    return lower;
}

inline constexpr std::uint64_t AreaDistance(std::int64_t first, std::int64_t second) noexcept {
    return first < second ? static_cast<std::uint64_t>(second) - static_cast<std::uint64_t>(first)
                          : static_cast<std::uint64_t>(first) - static_cast<std::uint64_t>(second);
}

inline constexpr std::int64_t AreaAddClipped(std::int64_t center, std::uint64_t distance) noexcept {
    constexpr auto maximum = std::numeric_limits<std::int64_t>::max();
    if (distance > static_cast<std::uint64_t>(maximum) - static_cast<std::uint64_t>(center)) return maximum;
    return std::bit_cast<std::int64_t>(static_cast<std::uint64_t>(center) + distance);
}

inline constexpr std::int64_t AreaSubtractClipped(std::int64_t center, std::uint64_t distance) noexcept {
    constexpr auto minimum = std::numeric_limits<std::int64_t>::min();
    if (distance > static_cast<std::uint64_t>(center) - static_cast<std::uint64_t>(minimum)) return minimum;
    return std::bit_cast<std::int64_t>(static_cast<std::uint64_t>(center) - distance);
}

// Every next cell is computed directly from row bounds, including at int64 edges.
class AreaCursor {
  public:
    explicit AreaCursor(ChunkAreaSelector selector) : selector_(selector) {
        if (selector_.dims != 2U && selector_.dims != 3U)
            throw std::invalid_argument("area dimensions must be two or three");
        if (selector_.dims == 2U && (selector_.first.z != 0 || (!selector_.around && selector_.last.z != 0)))
            throw std::invalid_argument("two-dimensional area has an inactive coordinate");
        if (selector_.around) {
            if (selector_.radius < 0) throw std::invalid_argument("chunk radius must be >= 0");
            radius_ = static_cast<std::uint64_t>(selector_.radius);
            radius_squared_ = AreaSquare(radius_);
            min_x_ = AreaSubtractClipped(selector_.first.x, radius_);
            max_x_ = AreaAddClipped(selector_.first.x, radius_);
        } else {
            if (selector_.first.x > selector_.last.x || selector_.first.y > selector_.last.y ||
                (selector_.dims == 3U && selector_.first.z > selector_.last.z))
                throw std::invalid_argument("chunk range corners are reversed");
            min_x_ = selector_.first.x;
            max_x_ = selector_.last.x;
        }
    }

    [[nodiscard]] bool Contains(const ChunkCoord& coord) const noexcept {
        if (selector_.dims == 2U && coord.z != 0) return false;
        if (!selector_.around)
            return coord.x >= min_x_ && coord.x <= max_x_ &&
                coord.y >= selector_.first.y && coord.y <= selector_.last.y &&
                (selector_.dims == 2U || (coord.z >= selector_.first.z && coord.z <= selector_.last.z));
        const auto dx = AreaDistance(coord.x, selector_.first.x);
        const auto dy = AreaDistance(coord.y, selector_.first.y);
        const auto dz = selector_.dims == 3U ? AreaDistance(coord.z, selector_.first.z) : 0U;
        if (dx > radius_ || dy > radius_ || dz > radius_) return false;
        return AreaLessEqual(AreaAdd(AreaAdd(AreaSquare(dx), AreaSquare(dy)), AreaSquare(dz)), radius_squared_);
    }

    [[nodiscard]] ChunkCoord First() const noexcept {
        const auto y = YBounds(min_x_).first;
        return {min_x_, y, selector_.dims == 3U ? ZBounds(min_x_, y).first : 0};
    }

    [[nodiscard]] std::optional<ChunkCoord> Next(ChunkCoord current) const {
        if (!Contains(current)) throw std::invalid_argument("area cursor is outside its selector");
        if (selector_.dims == 3U && current.z < ZBounds(current.x, current.y).second) {
            ++current.z;
            return current;
        }
        if (current.y < YBounds(current.x).second) {
            ++current.y;
            current.z = selector_.dims == 3U ? ZBounds(current.x, current.y).first : 0;
            return current;
        }
        if (current.x == max_x_) return std::nullopt;
        ++current.x;
        current.y = YBounds(current.x).first;
        current.z = selector_.dims == 3U ? ZBounds(current.x, current.y).first : 0;
        return current;
    }

  private:
    [[nodiscard]] std::pair<std::int64_t, std::int64_t> YBounds(std::int64_t x) const noexcept {
        if (!selector_.around) return {selector_.first.y, selector_.last.y};
        const auto remaining = AreaSubtract(radius_squared_, AreaSquare(AreaDistance(x, selector_.first.x)));
        const auto radius = AreaSquareRoot(remaining, radius_);
        return {AreaSubtractClipped(selector_.first.y, radius), AreaAddClipped(selector_.first.y, radius)};
    }

    [[nodiscard]] std::pair<std::int64_t, std::int64_t> ZBounds(std::int64_t x, std::int64_t y) const noexcept {
        if (!selector_.around) return {selector_.first.z, selector_.last.z};
        const auto used = AreaAdd(AreaSquare(AreaDistance(x, selector_.first.x)),
            AreaSquare(AreaDistance(y, selector_.first.y)));
        const auto radius = AreaSquareRoot(AreaSubtract(radius_squared_, used), radius_);
        return {AreaSubtractClipped(selector_.first.z, radius), AreaAddClipped(selector_.first.z, radius)};
    }

    ChunkAreaSelector selector_;
    std::uint64_t radius_ = 0U;
    AreaWide radius_squared_{};
    std::int64_t min_x_ = 0;
    std::int64_t max_x_ = 0;
};

inline bool AreaRequestedExceeds(ChunkAreaSelector selector, std::uint64_t limit) {
    (void)AreaCursor(selector);
    if (!selector.around) {
        const std::uint64_t spans[]{AreaDistance(selector.first.x, selector.last.x),
            AreaDistance(selector.first.y, selector.last.y), AreaDistance(selector.first.z, selector.last.z)};
        std::uint64_t count = 1U;
        for (std::uint8_t axis = 0U; axis < selector.dims; ++axis) {
            if (spans[axis] >= limit || count > limit / (spans[axis] + 1U)) return true;
            count *= spans[axis] + 1U;
        }
        return false;
    }
    // An inscribed cube rejects enormous balls before visiting any cell.
    const auto side = 2U * (static_cast<std::uint64_t>(selector.radius) / selector.dims) + 1U;
    std::uint64_t cube = 1U;
    for (std::uint8_t axis = 0U; axis < selector.dims; ++axis) {
        if (cube > limit / side) return true;
        cube *= side;
    }
    // Unpaged admission retains the full selector count before domain clipping.
    selector.first = {};
    const AreaCursor entire(selector);
    std::uint64_t count = 0U;
    for (auto current = std::optional{entire.First()}; current; current = entire.Next(*current)) {
        if (count == limit) return true;
        ++count;
    }
    return false;
}

}  // namespace chunkdb::detail
