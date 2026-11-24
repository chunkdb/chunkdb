#pragma once

// Secondary indexes (docs/design/INDEXES.md): keys, the run and manifest
// files, and the layers of one index. An index is derived from chunk state
// and rebuilt from it whenever its files cannot be trusted.

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <span>
#include <string>
#include <vector>

#include "chunkdb/chunk_layout.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/geometry.hpp"
#include "chunkdb/types.hpp"
#include "crypto.hpp"

namespace chunkdb {

inline constexpr std::string_view kIndexesDirectoryName = "indexes";
// The most bytes of a value a key holds: a text or bytes column with a
// larger max needs a PREFIX, a wider bits(N) column cannot be indexed.
inline constexpr std::uint32_t kMaxIndexKeyBytes = 256U;
// Runs above this many are merged into one.
inline constexpr std::size_t kMaxIndexRuns = 8U;

// Throws std::invalid_argument unless `column` can be indexed with `prefix`
// (0 for none): a fixed-width column of at most kMaxIndexKeyBytes, or a text
// or bytes column whose max is at most kMaxIndexKeyBytes or which has a
// prefix of 1 to kMaxIndexKeyBytes bytes.
void RequireIndexableColumn(const Column& column, std::uint32_t prefix);

// One block of one chunk under its key. `removed` is zero for a current
// entry, else the revision that removed it while a snapshot may still read
// it.
struct IndexEntry {
    std::vector<std::uint8_t> key{};
    ChunkCoord coord{};
    std::uint32_t block = 0U;
    std::uint64_t removed = 0U;
};
// Key bytes, then coordinates ascending by x, y, z, then block index.
[[nodiscard]] bool IndexEntryLess(const IndexEntry& first, const IndexEntry& second) noexcept;

// Appends the order-preserving key of a fixed-width value, as
// EncodeColumnValue writes it; returns false, appending nothing, for NaN,
// which has no entry.
[[nodiscard]] bool AppendFixedIndexKey(const Column& column, const std::uint8_t* value, std::vector<std::uint8_t>* key);
// Appends the key of a text or bytes value, cut to `prefix` bytes when it is
// not zero: each 0x00 becomes 0x00 0xFF and 0x00 0x00 ends the value, so a
// shorter value sorts first.
void AppendVarIndexKey(std::span<const std::uint8_t> value, std::uint32_t prefix, std::vector<std::uint8_t>* key);
// The entries of one chunk state for the column at `column_index` of
// `layout`, ordered: one per present block with a value, none for NULL, NaN
// or an absent block. A text or bytes column that cannot be NULL holds the
// empty value where VARS has no entry.
[[nodiscard]] std::vector<IndexEntry> ChunkIndexEntries(const ChunkLayout& layout, std::size_t column_index,
    std::uint32_t prefix, const ChunkCoord& coord, std::span<const std::uint8_t> presence,
    std::span<const std::uint8_t> payload, const ChunkVars& vars);

// What the files of an index are bound to.
struct IndexIdentity {
    StoreId epoch{};
    std::uint8_t dims = 2U;
    std::uint32_t index_id = 0U;
    std::uint32_t column_id = 0U;
    std::uint32_t prefix = 0U;
    std::uint64_t schema_version = 1U;
    friend bool operator==(const IndexIdentity&, const IndexIdentity&) = default;
};
enum class IndexState : std::uint8_t { kBuilding = 1, kReady = 2 };
struct IndexRunRef {
    std::uint64_t generation = 0U;
    std::uint64_t bytes = 0U;
    crypto::Sha256Digest sha256{};
    friend bool operator==(const IndexRunRef&, const IndexRunRef&) = default;
};
// The manifest lists the runs, newest first; its replacement commits a
// flush, a merge or a state change.
struct IndexManifest {
    IndexIdentity identity{};
    IndexState state = IndexState::kBuilding;
    std::uint64_t generation = 0U;
    std::vector<IndexRunRef> runs{};
};
[[nodiscard]] std::vector<std::uint8_t> SerializeIndexManifest(const IndexManifest& manifest);
// Throws std::runtime_error for any damage.
[[nodiscard]] IndexManifest ParseIndexManifest(std::span<const std::uint8_t> bytes);

// A chunk a run covers: the revision whose state produced its entries, or
// zero for a chunk that has no populated state.
struct IndexRunChunk {
    ChunkCoord coord{};
    std::uint64_t revision = 0U;
    std::uint32_t entries = 0U;
};

// Where an index reads chunk state from: every coordinate the coordinate
// index lists, with its revision and whether it is populated, and the
// current state of a chunk.
struct IndexChunkSource {
    std::function<void(const std::function<void(const ChunkCoord&, std::uint64_t, bool)>&)> for_each{};
    std::function<ChunkState(const ChunkCoord&)> read{};
};

// The layers of one index: an in-memory delta and up to kMaxIndexRuns runs
// on disk. A chunk's entries come from the newest layer that covers it.
// Not thread-safe; its owner serializes calls.
class SecondaryIndex {
  public:
    // `directory` is the index's own directory; `column_index` the place of
    // the indexed column in the layout.
    SecondaryIndex(std::filesystem::path directory, const ChunkLayout& layout, std::size_t column_index,
        IndexIdentity identity);

    struct OpenReport {
        // Why the index was rebuilt from chunk state; empty when it was not.
        std::string rebuilt;
        // Chunks recomputed because their covering layer was stale.
        std::size_t recomputed = 0U;
    };
    // Loads the manifest and the runs' chunk tables, removes files the
    // manifest does not list, and brings every chunk of `source` up to date
    // in the delta. A missing, damaged or foreign manifest or run, or an
    // index still building, is rebuilt from `source` and flushed.
    OpenReport Open(const IndexChunkSource& source);
    // Recomputes every chunk of `source` and replaces the runs with one.
    void Rebuild(const IndexChunkSource& source);

    // The delta covers `coord` at `revision` with `entries`, ordered.
    void Replace(const ChunkCoord& coord, std::uint64_t revision, std::vector<IndexEntry> entries);
    // The delta covers `coord` with no populated state.
    void Forget(const ChunkCoord& coord);
    // Writes the delta as the newest run and publishes a manifest that lists
    // it; merges every run into one once there are more than kMaxIndexRuns,
    // or when the delta reached a quarter of the oldest run's entries.
    void Flush();
    // Writes every effective entry as one run and publishes it.
    void Merge();
    // Publishes `state` in a new manifest.
    void SetState(IndexState state);

    // The effective entries in key order: from each chunk's covering layer,
    // removal-marked ones included. Stops when `visit` returns false.
    void ForEach(const std::function<bool(const IndexEntry&)>& visit) const;
    // The revision that covers `coord`, and whether any layer does.
    [[nodiscard]] bool Covered(const ChunkCoord& coord, std::uint64_t* revision) const;

    // Checks every run's digest, block checksums and order, and that each
    // chunk of `source` has exactly the entries its state gives. Returns one
    // line per problem.
    [[nodiscard]] std::vector<std::string> Verify(const IndexChunkSource& source) const;

    [[nodiscard]] IndexState state() const noexcept { return manifest_.state; }
    [[nodiscard]] std::size_t runs() const noexcept { return manifest_.runs.size(); }
    [[nodiscard]] std::size_t delta_chunks() const noexcept { return delta_.size(); }
    [[nodiscard]] std::uint64_t manifest_generation() const noexcept { return manifest_.generation; }
    [[nodiscard]] const std::filesystem::path& directory() const noexcept { return directory_; }
    [[nodiscard]] static std::filesystem::path ManifestPath(const std::filesystem::path& directory);
    [[nodiscard]] static std::filesystem::path RunPath(const std::filesystem::path& directory, std::uint64_t generation);

  private:
    struct DeltaChunk {
        std::uint64_t revision = 0U;
        std::vector<IndexEntry> entries;
    };
    struct Cover {
        // Index in manifest_.runs, or kDelta.
        std::size_t layer = 0U;
        std::uint64_t revision = 0U;
    };
    static constexpr std::size_t kDelta = static_cast<std::size_t>(-1);

    void LoadOrThrow();
    void RemoveUnlisted() const;
    void Clear() noexcept;
    void PublishManifest(IndexManifest next);
    [[nodiscard]] IndexRunRef WriteRun(std::uint64_t generation, const std::vector<IndexRunChunk>& chunks,
        const std::function<void(const std::function<void(const IndexEntry&)>&)>& entries) const;
    void RefreshCovers(const IndexChunkSource& source, OpenReport* report);
    [[nodiscard]] std::vector<IndexEntry> EntriesOf(const ChunkState& state, const ChunkCoord& coord) const;
    [[nodiscard]] std::uint64_t DeltaEntries() const noexcept;

    std::filesystem::path directory_;
    const ChunkLayout& layout_;
    std::size_t column_index_;
    IndexIdentity identity_;
    IndexManifest manifest_{};
    std::vector<std::uint64_t> run_entries_;
    std::map<ChunkCoord, DeltaChunk> delta_;
    std::map<ChunkCoord, Cover> covers_;
};

}  // namespace chunkdb
