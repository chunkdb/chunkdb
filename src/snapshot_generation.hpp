#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <vector>

#include "chunk_store_internal.hpp"

namespace chunkdb {

// `record_required`: the store saw a generation record when it opened. A
// writer never removes it, so its absence then means the store directory was
// removed (a dropped table) and the read fails instead of seeing an empty
// store.
[[nodiscard]] std::uint64_t ReadSnapshotGenerationForScan(
    const std::filesystem::path& path,
    bool record_required);

struct ReadOnlyArtifactSnapshot {
    bool present = false;
    std::vector<std::uint8_t> bytes;

    bool operator==(const ReadOnlyArtifactSnapshot&) const = default;
};

struct ReadOnlyChunkDiskSnapshot {
    ReadOnlyArtifactSnapshot image;
    ReadOnlyArtifactSnapshot wal;
    ReadOnlyArtifactSnapshot intent;
    // Every pending transaction intent of the table, by name.
    std::vector<ReadOnlyArtifactSnapshot> txn_intents;

    bool operator==(const ReadOnlyChunkDiskSnapshot&) const = default;
};

[[nodiscard]] ReadOnlyChunkDiskSnapshot LoadStableReadOnlyChunkDiskSnapshot(
    const std::filesystem::path& data_path,
    const std::filesystem::path& wal_path,
    const std::filesystem::path& intent_path,
    const std::filesystem::path& txn_intent_dir,
    const std::filesystem::path& generation_path,
    bool generation_record_required,
    const ChunkCoord& chunk_coord,
    const std::function<void(
        std::size_t,
        ReadOnlySnapshotArtifact)>& observation);

}  // namespace chunkdb
