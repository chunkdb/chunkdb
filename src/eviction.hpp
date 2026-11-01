#pragma once

#include "chunk_store_internal.hpp"

namespace chunkdb {
[[nodiscard]] std::size_t EvictionLowerWatermark(std::size_t max_loaded_chunks);
}

