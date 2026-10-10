#pragma once

#include <filesystem>
#include <vector>
#include "chunkdb/chunk_store.hpp"

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

// Validate before changing identity; preserve section bytes, historical schema,
// compression and file feature flags while rebuilding only the header CRC.
[[nodiscard]] std::vector<std::uint8_t> RewriteChunkImageStoreId(
    std::vector<std::uint8_t> bytes, const Geometry& geometry, ChunkCoord coord,
    const StoreId& old_id, const StoreId& new_id, FeatureFlags features);
[[nodiscard]] std::vector<std::uint8_t> RewriteWalStoreId(
    std::vector<std::uint8_t> bytes, ChunkCoord coord,
    const StoreId& old_id, const StoreId& new_id, FeatureFlags features);

}  // namespace chunkdb
