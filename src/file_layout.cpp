#include "chunkdb/file_layout.hpp"

#include <string>
#include <algorithm>
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "wal_replay.hpp"

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

std::vector<std::uint8_t> RewriteChunkImageStoreId(
    std::vector<std::uint8_t> bytes, const Geometry& geometry, ChunkCoord coord,
    const StoreId& old_id, const StoreId& new_id, FeatureFlags features) {
    (void)ParseChunkImage(bytes, geometry, coord, old_id, features);
    if (std::all_of(new_id.begin(), new_id.end(), [](auto b) { return b == 0U; }))
        throw std::invalid_argument("new store id must not be zero");
    std::copy(new_id.begin(), new_id.end(), bytes.begin() + 24U);
    const auto crc_at = kImageFixedHeaderSize + static_cast<std::size_t>(ReadLe16(bytes, 10U)) * kImageSectionEntrySize;
    const auto crc = Crc32(bytes.data(), crc_at);
    for (unsigned i = 0U; i < 4U; ++i) bytes[crc_at + i] = static_cast<std::uint8_t>(crc >> (i * 8U));
    return bytes;
}
std::vector<std::uint8_t> RewriteWalStoreId(
    std::vector<std::uint8_t> bytes, ChunkCoord coord,
    const StoreId& old_id, const StoreId& new_id, FeatureFlags features) {
    ValidateWalHeader(bytes, coord, old_id, features);
    if (std::all_of(new_id.begin(), new_id.end(), [](auto b) { return b == 0U; }))
        throw std::invalid_argument("new store id must not be zero");
    const FeatureFlags original{ReadLe32(bytes, 12U), ReadLe32(bytes, 16U), ReadLe32(bytes, 20U)};
    const auto header = BuildWalHeader(coord, new_id, original);
    std::copy(header.begin(), header.end(), bytes.begin());
    return bytes;
}

}  // namespace chunkdb
