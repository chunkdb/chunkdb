#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <vector>
#include "chunkdb/chunk_store.hpp"

#include "chunkdb/geometry.hpp"
#include "chunkdb/types.hpp"

namespace chunkdb {

[[nodiscard]] std::filesystem::path LargeChunkDirectory(
    const std::filesystem::path& data_dir,
    const LargeChunkCoord& large_coord);
[[nodiscard]] std::filesystem::path LargeChunkDirectory(
    const std::filesystem::path& data_dir,
    const LargeChunkCoord& large_coord, std::uint8_t dims);
[[nodiscard]] std::string ChunkFileStem(const ChunkCoord& coord, std::uint8_t dims);
// Canonical coordinate names only; failure never changes the output coordinate.
[[nodiscard]] bool ParseChunkFileStem(std::string_view name, std::uint8_t dims, ChunkCoord* out);
[[nodiscard]] bool ParseLargeChunkName(std::string_view name, std::uint8_t dims, LargeChunkCoord* out);
// No filesystem access: require exact relative placement, dimension and suffix.
[[nodiscard]] bool ParseChunkFilePath(
    const std::filesystem::path& relative, const Geometry& geometry, ChunkCoord* out);
[[nodiscard]] std::filesystem::path ChunkDataPath(
    const std::filesystem::path& data_dir,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord);
[[nodiscard]] std::filesystem::path ChunkWalPath(
    const std::filesystem::path& data_dir,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord);

// Validate before changing identity; preserve section bytes, historical schema,
// compression and file feature flags while rebuilding only the header CRC.
[[nodiscard]] std::vector<std::uint8_t> RewriteChunkImageStoreId(
    std::vector<std::uint8_t> bytes, const Geometry& geometry, ChunkCoord coord,
    const StoreId& old_id, const StoreId& new_id, FeatureFlags features);
[[nodiscard]] std::vector<std::uint8_t> RewriteWalStoreId(
    std::vector<std::uint8_t> bytes, ChunkCoord coord,
    const StoreId& old_id, const StoreId& new_id, FeatureFlags features);

}  // namespace chunkdb
