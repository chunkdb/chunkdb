#include "chunk_form.hpp"

#include <stdexcept>
#include <string>

#include "chunk_store_internal.hpp"

namespace chunkdb {
namespace {
constexpr std::size_t kChunkFormHeaderBytes = 16;
[[nodiscard]] std::uint64_t LoadLittleEndian(std::string_view bytes) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(bytes[i])) << (8U * i);
    }
    return value;
}

}  // namespace

// The schema version a chunk form of every column was encoded for.
[[nodiscard]] std::uint64_t ChunkFormSchemaVersion(std::string_view form) {
    if (form.size() < kChunkFormHeaderBytes) {
        throw std::invalid_argument(
            "the chunk starts with its version and schema version (16 bytes), got " + std::to_string(form.size()));
    }
    return LoadLittleEndian(form.substr(8, 8));
}

// The state a chunk form of every column sends, for a layout of the schema
// version it was encoded for; its version is not used.
[[nodiscard]] ChunkState DecodeChunkForm(const Geometry& geometry, std::string_view form) {
    const std::size_t presence_bytes = ChunkPresenceBitmapBytes(geometry);
    const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
    const std::size_t header = kChunkFormHeaderBytes;
    if (form.size() < header + presence_bytes + payload_bytes) {
        throw std::invalid_argument(
            "the chunk takes at least " + std::to_string(header + presence_bytes + payload_bytes) +
            " bytes (version, schema version, presence, payload), got " + std::to_string(form.size()));
    }
    const auto* data = reinterpret_cast<const std::uint8_t*>(form.data());
    ChunkState state;
    state.presence_bitmap.assign(data + header, data + header + presence_bytes);
    state.payload.assign(data + header + presence_bytes, data + header + presence_bytes + payload_bytes);
    const std::size_t vars_offset = header + presence_bytes + payload_bytes;
    state.vars = ChunkVars::Decode(data + vars_offset, form.size() - vars_offset, geometry.ChunkBlockCount());
    return state;
}

}  // namespace chunkdb
