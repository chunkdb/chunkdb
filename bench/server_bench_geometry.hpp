#pragma once

#include "chunkdb/geometry.hpp"
#include "chunkdb/server_bench.hpp"

namespace chunkdb::server_bench {

[[nodiscard]] GeometryConfig SpawnGeometry(const Args& args);

}  // namespace chunkdb::server_bench
