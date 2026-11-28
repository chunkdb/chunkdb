#pragma once

#include <optional>

namespace chunkdb::server_bench::detail {

// A missing or regressed counter cannot describe CPU use during an interval.
template <typename Counter>
Counter CpuCounterDelta(std::optional<Counter> start, std::optional<Counter> end) {
    if (!start || !end || *end < *start) return Counter{};
    return *end - *start;
}

}  // namespace chunkdb::server_bench::detail
