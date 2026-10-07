#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "chunk_store_internal.hpp"
#include "history_format.hpp"

// Block history on disk (docs/STORAGE_FORMAT.md Section 9): the segments of
// each chunk under the store's `history` directory, and the events a chunk's
// image and WAL hold.
namespace chunkdb::history {

inline constexpr std::string_view kHistoryDirName = "history";
// A chunk's newest segment takes appends until its records pass this size.
inline constexpr std::size_t kSegmentTargetRecordBytes = 64U * 1024U;
// A new segment gets a keyframe once the records since the newest keyframe
// (or since the chunk's history began) take this many times its size.
inline constexpr std::uint64_t kKeyframeRatio = 8;

// History that cannot be read as written: a segment that is damaged,
// missing from the chain, or foreign. The chunk's history fails closed.
class HistoryDamagedError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

struct SegmentInfo {
    std::filesystem::path path;
    std::uint64_t base_revision = 0;
    std::uint64_t base_time_ms = 0;
    bool keyframe = false;
    bool cut = false;
    bool first = false;
    // Bytes of the header with its keyframe, and of the whole valid file.
    std::size_t header_size = 0;
    std::size_t size = 0;
    std::uint64_t first_revision = 0;
    std::uint64_t last_revision = 0;
    std::uint64_t first_time_ms = 0;
    std::uint64_t last_time_ms = 0;
};

// What a chunk's history holds on disk.
struct ChunkHistory {
    // Oldest first; consecutive (each starts where the one before ends).
    std::vector<SegmentInfo> segments;
    // Record bytes since the newest keyframe, or since the history began.
    std::uint64_t bytes_since_keyframe = 0;

    // Zero when there is none.
    [[nodiscard]] std::uint64_t last_revision() const noexcept;
    [[nodiscard]] std::uint64_t last_time_ms() const noexcept;
    // Where the retained events begin after a trim: the base of the oldest
    // segment when it is a cut, else 0 (nothing was trimmed).
    [[nodiscard]] std::uint64_t trimmed_before() const noexcept;
};

// The events a chunk's image and WAL hold above some revision.
struct Derivation {
    // Mutations at or above the history start and above `after_revision`,
    // in revision order; each with at least one change.
    std::vector<Mutation> mutations;
    // The state before the first of them, its revision and commit time
    // (when there are none: the final state).
    ChunkState base;
    std::uint64_t base_revision = 0;
    std::uint64_t base_time_ms = 0;
    // The revision of the image (0 without one).
    std::uint64_t image_revision = 0;
    // The state after every frame.
    ChunkState final_state;
    std::uint64_t final_revision = 0;
    std::uint64_t final_time_ms = 0;
};

// Replays `wal` (null: no WAL) over `image` (null: no image) of one chunk
// of a store with history from `history_start`, collecting the mutations
// above `after_revision`. A WAL that stops before its end is damage unless
// `allow_crash_tail` and the stop has the shape a crash leaves. Throws
// HistoryDamagedError for anything that cannot be replayed.
[[nodiscard]] Derivation DeriveHistory(
    const Geometry& geometry,
    const ChunkCoord& chunk,
    const StoreId& store_id,
    const FeatureFlags& store_features,
    const std::vector<std::uint8_t>* image,
    const std::vector<std::uint8_t>* wal,
    std::uint64_t history_start,
    std::uint64_t after_revision,
    bool allow_crash_tail);

// The history files of one store.
class HistoryFiles {
  public:
    HistoryFiles(
        std::filesystem::path data_dir,
        const Geometry& geometry,
        const StoreId& store_id,
        std::uint64_t history_start);

    [[nodiscard]] const std::filesystem::path& directory() const noexcept { return dir_; }
    [[nodiscard]] std::filesystem::path ChunkDirectory(const ChunkCoord& chunk) const;
    [[nodiscard]] std::filesystem::path SegmentPath(const ChunkCoord& chunk, std::uint64_t base_revision) const;

    // Reads and checks the chunk's segments. `writable` also repairs what an
    // interrupted writer left: temp files, segments older than a cut, and
    // a torn tail of the newest segment (synced before this returns); a
    // reader ignores them. Throws HistoryDamagedError.
    [[nodiscard]] ChunkHistory Load(const ChunkCoord& chunk, bool writable) const;

    // Appends `mutations` (above history->last_revision(), in order) to the
    // chunk's history, durable when this returns: to the newest segment, or
    // to a new one that starts from `base`, the state at
    // history->last_revision() (when there is no segment yet: at
    // base_revision and base_time_ms). On failure the files may hold a
    // partial append: Load the history again before the next Append.
    void Append(
        const ChunkCoord& chunk,
        ChunkHistory* history,
        std::span<const Mutation> mutations,
        const ChunkState& base,
        std::uint64_t base_revision,
        std::uint64_t base_time_ms) const;

    struct SegmentContents {
        SegmentHeader header;
        std::vector<std::uint8_t> bytes;
    };
    // The segment's header and its valid bytes. Throws HistoryDamagedError.
    [[nodiscard]] SegmentContents ReadSegment(const ChunkCoord& chunk, const SegmentInfo& segment) const;

  private:
    std::filesystem::path data_dir_;
    std::filesystem::path dir_;
    Geometry geometry_;
    StoreId store_id_;
    std::uint64_t history_start_ = 0;
};

}  // namespace chunkdb::history
