#pragma once

#include <filesystem>

#include "chunkdb/geometry.hpp"
#include "chunkdb/types.hpp"

namespace chunkdb {

[[nodiscard]] std::filesystem::path LargeChunkDirectory(
    const std::filesystem::path& data_dir,
    const LargeChunkCoord& large_coord);
[[nodiscard]] std::filesystem::path ChunkDataPath(
    const std::filesystem::path& data_dir,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord);
[[nodiscard]] std::filesystem::path ChunkWalPath(
    const std::filesystem::path& data_dir,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord);

}  // namespace chunkdb
