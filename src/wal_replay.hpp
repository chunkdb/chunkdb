#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "chunk_store_internal.hpp"

namespace chunkdb {

struct WalReplayResult {
    std::size_t applied_records = 0;
    std::size_t applied_frames = 0;
    // Valid frames at or below the base revision: the image already holds
    // them.
    std::size_t skipped_frames = 0;
    // Revision and commit time of the last applied frame; zero when none.
    std::uint64_t revision = 0;
    std::uint64_t commit_time_ms = 0;
    // The header is valid for this store and chunk; frames were replayed.
    bool replayable = false;
    // A crash while the file was being created, before any frame: shorter
    // than a WAL header and a prefix of the header this chunk's WAL starts
    // with (feature flags aside), or empty, or nothing but zero bytes (a
    // filesystem that exposes unwritten blocks after a crash). It holds no
    // mutation.
    bool torn_creation = false;
    // Replay stopped before the end of the file (torn or invalid frame).
    bool tail_truncated_or_corrupt = false;
    // The stop has a shape a crash can leave: the failing frame reaches the
    // end of the file (cut short, or failing a checksum as the last bytes),
    // or no frame header with a valid checksum follows the stop. A frame that
    // is complete with both checksums valid was written whole, so its failure
    // is damage even as the last frame, like bytes after a failing frame.
    bool stopped_at_crash_tail = false;
    // Bytes from the start of the file through the last applied frame (the
    // header alone when none applied); what a writer keeps before appending.
    std::size_t valid_end = 0;
    std::string stop_reason;
    // Set when the values after the last applied frame break an invariant
    // (ChunkLayout::RequireValidVars, more than kVarMaxChunkBytesLimit): the
    // files are damaged, the state must not be used.
    std::string vars_problem;
};

struct FeedFrameInfo {
    std::uint64_t revision;
    std::uint64_t commit_time_ms;
    std::uint64_t schema_version;
    std::optional<std::string> user{};
    bool gc = false;
};
// Validates the whole frame's framing and records without applying state.
[[nodiscard]] FeedFrameInfo InspectFeedFrame(
    const std::vector<std::uint8_t>& frame, const Geometry& geometry, FeatureFlags features = {});
// One captured, headerless frame; validated with the same parser as recovery.
[[nodiscard]] FeedFrameInfo ReplayFeedFrame(
    const std::vector<std::uint8_t>& frame, const Geometry& geometry, ChunkState* state,
    FeatureFlags features = {});

// Validates a WAL file header for this store and chunk; throws
// std::runtime_error naming the defect.
void ValidateWalHeader(
    const std::vector<std::uint8_t>& bytes,
    const ChunkCoord& expected_chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& store_features);

// Replays the frames of a WAL onto `payload`, `presence_bitmap` and `vars`
// (the chunk's state from its image, whose revision is `base_revision`; 0
// without an image). Every frame is validated completely before it is
// applied, and frame revisions must increase; the first frame that fails
// stops replay with nothing of it applied. Frames at or below
// `base_revision` are validated and skipped: the image already holds them,
// and a WAL that outlived its checkpoint (a crash between publishing the
// image and removing the WAL) may lack frames the image holds, so applying
// them would mix old values into the newer state. Null `vars` stands for an
// image without values; the result is then discarded. The state given is
// laid out by schema version `base_schema_version` (0: the empty state of no
// image); each frame applies in its own version, the state moving forward to
// it, and the result is laid out by the current version.
[[nodiscard]] WalReplayResult ReplayWal(
    const std::vector<std::uint8_t>& wal_bytes,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& store_features,
    std::uint64_t base_revision,
    std::uint64_t base_schema_version,
    std::vector<std::uint8_t>* payload,
    std::vector<std::uint8_t>* presence_bitmap,
    ChunkVars* vars);

}  // namespace chunkdb
