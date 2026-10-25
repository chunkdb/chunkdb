#pragma once

#include <cstdint>
#include <string_view>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb {
// Internal all-column SET CHUNK form decoder. The caller chooses the matching
// schema layout and validates its values exactly as in the statement path.
[[nodiscard]] std::uint64_t ChunkFormSchemaVersion(std::string_view form);
[[nodiscard]] ChunkState DecodeChunkForm(const Geometry& geometry, std::string_view form);
}  // namespace chunkdb
