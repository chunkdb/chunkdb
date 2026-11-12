#pragma once

#include <cstdint>
#include <string_view>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb {
// Internal all-column SET CHUNK entrypoints shared with the fuzz harness.
// Decode validates the checked form against the current shared schema.
[[nodiscard]] std::uint64_t ChunkFormSchemaVersion(std::string_view form);
[[nodiscard]] ChunkState DecodeChunkForm(const Geometry& geometry, std::string_view form);
}  // namespace chunkdb
