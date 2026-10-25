#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"

namespace chunkdb {

// A durable writer session plus its volatile mapped bracket. Both parts
// must match around an artifact collection to exclude writer-restart ABA.
using SnapshotGenerationObservation = std::pair<std::uint64_t, std::uint64_t>;

class SnapshotGenerationMapping {
  public:
    explicit SnapshotGenerationMapping(const std::filesystem::path& directory);
    ~SnapshotGenerationMapping();
    void Initialize(std::uint64_t session);
    [[nodiscard]] std::uint64_t Load() const;
    [[nodiscard]] bool IsFailed() const;
    bool CompareExchange(std::uint64_t& expected, std::uint64_t desired);
  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// `record_required`: the store saw a generation record when it opened. A
// writer never removes it, so its absence then means the store directory was
// removed (a dropped table) and the read fails instead of seeing an empty
// store.
[[nodiscard]] SnapshotGenerationObservation ReadSnapshotGenerationForScan(
    const std::filesystem::path& path,
    bool record_required);

// The generation now when it is stable (even, no publication in progress),
// or nullopt while a writer is publishing or its session cannot be read
// consistently. Damage still throws.
[[nodiscard]] std::optional<SnapshotGenerationObservation> StableSnapshotGeneration(
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
    // The stable generation observed before and after the collection.
    SnapshotGenerationObservation generation{};

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
