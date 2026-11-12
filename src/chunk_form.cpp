#include "chunk_form.hpp"

#include "sectioned_chunk_form.hpp"
#include "typed_chunk_form.hpp"

namespace chunkdb {
// The schema version a chunk form of every column was encoded for.
[[nodiscard]] std::uint64_t ChunkFormSchemaVersion(std::string_view form) {
    return ParseChunkFormSections({reinterpret_cast<const std::uint8_t*>(form.data()), form.size()},
        UINT32_MAX, ChunkFormReadMode::kMutation).header.schema_version;
}

// Decode a complete current-schema mutation; its producing identity is ignored.
[[nodiscard]] ChunkState DecodeChunkForm(const Geometry& geometry, std::string_view form) {
    return DecodeSectionedChunkState(geometry,
        {reinterpret_cast<const std::uint8_t*>(form.data()), form.size()}, kVarMaxChunkBytesLimit);
}

}  // namespace chunkdb
