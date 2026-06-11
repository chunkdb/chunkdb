#pragma once

#include <filesystem>
#include <stdexcept>
#include <vector>

#include "chunk_store_internal.hpp"

namespace chunkdb {

[[nodiscard]] std::runtime_error BuildWalOpenError(
    const std::filesystem::path& path,
    int err);
[[nodiscard]] std::vector<std::uint8_t> BuildWalHeader(
    const Geometry& geometry,
    const ChunkCoord& chunk_coord);
void AppendWalDeltaSpanToBatch(
    std::vector<std::uint8_t>* batch,
    std::uint32_t byte_offset,
    const std::uint8_t* payload_bytes,
    std::size_t payload_size,
    std::size_t* appended_record_bytes,
    std::size_t* appended_record_count);

}  // namespace chunkdb
