#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <vector>

#include "chunk_store_internal.hpp"

namespace chunkdb {

struct ReadOnlyArtifactSnapshot {
    bool present = false;
    std::vector<std::uint8_t> bytes;

    bool operator==(const ReadOnlyArtifactSnapshot&) const = default;
};

struct ReadOnlyChunkDiskSnapshot {
    ReadOnlyArtifactSnapshot image;
    ReadOnlyArtifactSnapshot wal;
    ReadOnlyArtifactSnapshot intent;

    bool operator==(const ReadOnlyChunkDiskSnapshot&) const = default;
};

[[nodiscard]] ReadOnlyChunkDiskSnapshot LoadStableReadOnlyChunkDiskSnapshot(
    const std::filesystem::path& data_path,
    const std::filesystem::path& wal_path,
    const std::filesystem::path& intent_path,
    const std::filesystem::path& generation_path,
    const ChunkCoord& chunk_coord,
    const std::function<void(
        std::size_t,
        ReadOnlySnapshotArtifact)>& observation);

}  // namespace chunkdb
