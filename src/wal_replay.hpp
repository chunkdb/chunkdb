#pragma once

#include <cstddef>
#include <span>
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
    // Set when the extra data after the last applied frame breaks an
    // invariant (a value on an absent block, more than
    // kExtraMaxChunkBytesLimit): the files are damaged, the state must not
    // be used.
    std::string extra_problem;
};

// What replay tells an observer about a frame it applies.
struct WalFrameInfo {
    std::uint64_t revision = 0;
    std::uint64_t commit_time_ms = 0;
    // Empty when the frame has none.
    std::span<const std::uint8_t> tag{};
    // The blocks whose state (bits, presence, extra data) the frame can
    // change, ascending and unique.
    const std::vector<std::uint32_t>* touched_blocks = nullptr;
};

// Watches the frames replay applies (not the ones it skips): BeforeFrame
// sees the state the frame starts from, AfterFrame the state it leaves.
// `state` is the packed chunk state (payload then presence). An exception
// stops replay and propagates.
class WalReplayObserver {
  public:
    virtual ~WalReplayObserver() = default;
    virtual void BeforeFrame(
        const WalFrameInfo& frame,
        const std::vector<std::uint8_t>& state,
        const ChunkExtra& extra) = 0;
    virtual void AfterFrame(
        const WalFrameInfo& frame,
        const std::vector<std::uint8_t>& state,
        const ChunkExtra& extra) = 0;
};

// Validates a WAL file header for this store and chunk; throws
// std::runtime_error naming the defect.
void ValidateWalHeader(
    const std::vector<std::uint8_t>& bytes,
    const ChunkCoord& expected_chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& store_features);

// Replays the frames of a WAL onto `payload`, `presence_bitmap` and `extra`
// (the chunk's state from its image, whose revision is `base_revision`; 0
// without an image). Every frame is validated completely before it is
// applied, and frame revisions must increase; the first frame that fails
// stops replay with nothing of it applied. Frames at or below
// `base_revision` are validated and skipped: the image already holds them,
// and a WAL that outlived its checkpoint (a crash between publishing the
// image and removing the WAL) may lack frames the image holds, so applying
// them would mix old values into the newer state. Null `extra` stands for an
// image without extra data; the result is then discarded. `observer`, when
// given, sees every applied frame.
[[nodiscard]] WalReplayResult ReplayWal(
    const std::vector<std::uint8_t>& wal_bytes,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& store_features,
    std::uint64_t base_revision,
    std::vector<std::uint8_t>* payload,
    std::vector<std::uint8_t>* presence_bitmap,
    ChunkExtra* extra,
    WalReplayObserver* observer = nullptr);

}  // namespace chunkdb
