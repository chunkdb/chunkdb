#include "chunkdb/file_layout.hpp"

#include <string>

namespace chunkdb {

std::filesystem::path LargeChunkDirectory(
    const std::filesystem::path& data_dir,
    const LargeChunkCoord& large_coord) {
    return data_dir / ("L_" + std::to_string(large_coord.x) + "_" + std::to_string(large_coord.y));
}

std::filesystem::path ChunkDataPath(
    const std::filesystem::path& data_dir,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord) {
    const LargeChunkCoord large_coord = geometry.ChunkToLarge(chunk_coord);
    return LargeChunkDirectory(data_dir, large_coord) /
           ("C_" + std::to_string(chunk_coord.x) + "_" + std::to_string(chunk_coord.y) + ".chk");
}

std::filesystem::path ChunkWalPath(
    const std::filesystem::path& data_dir,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord) {
    const LargeChunkCoord large_coord = geometry.ChunkToLarge(chunk_coord);
    return LargeChunkDirectory(data_dir, large_coord) /
           ("C_" + std::to_string(chunk_coord.x) + "_" + std::to_string(chunk_coord.y) + ".wal");
}

}  // namespace chunkdb
