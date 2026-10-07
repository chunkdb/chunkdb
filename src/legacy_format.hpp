#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "chunk_store_internal.hpp"

// Readers for storage artifacts written before the 2.0 storage format. The
// engine does not use them; they exist for offline conversion
// (chunkdb_migrate). See legacy_format.cpp.
namespace chunkdb::legacy {

struct LegacyChunkImage {
    std::vector<std::uint8_t> payload;
    std::vector<std::uint8_t> presence_bitmap;
    // Image version 1-5.
    std::uint16_t version = 0;
    // Persisted revision (versions 4-5); zero for 1.x images.
    std::uint64_t revision = 0;
};

// Parses a chunk image of version 1-5 (magic CHKDATA1) for `geometry` and the
// expected coordinate. Throws std::runtime_error on any mismatch or damage.
[[nodiscard]] LegacyChunkImage ParseLegacyChunkImage(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord);

void ValidateLegacyWalHeader(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord);

struct LegacyWalReplay {
    std::size_t applied_records = 0;
    // v4 frames applied (zero for 1.x streams).
    std::size_t applied_frames = 0;
    // Revision carried by the last applied frame; zero when none applied.
    std::uint64_t revision = 0;
    // WAL file version from the header (zero for a headerless stream).
    std::uint16_t wal_version = 0;
    bool replayable = false;
    // True when the stream was (or ended) in the 1.x record layout.
    bool legacy_records = false;
    bool tail_truncated_or_corrupt = false;
    std::string stop_reason;
    // Bytes of the WAL that replay consumed: where it stopped, or the WAL
    // size when it read everything. Zero for an unreplayable WAL.
    std::size_t stop_offset = 0;
};

// Replays a WAL of version 2-4 (magic CHKWAL02, DLT1 records or FRM1 frames,
// including a v4 header written after 1.x records, or a headerless stream)
// onto `payload` and `presence_bitmap`, exactly as the 1.x and 2.0
// development servers did.
[[nodiscard]] LegacyWalReplay ReplayLegacyWal(
    const std::vector<std::uint8_t>& wal_bytes,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    std::vector<std::uint8_t>* payload,
    std::vector<std::uint8_t>* presence_bitmap);

// The 8-byte little-endian version-clock ceiling that development builds
// before the checked `chunkdb.version` record wrote. Returns false unless it
// is exactly 8 bytes and non-zero.
[[nodiscard]] bool TryParseIntermediateVersionClockRecord(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* out_ceiling);

// The checked version-clock record (`chunkdb.version`): "CKVR", u64 ceiling,
// CRC32 of the first 12 bytes. Returns false unless valid with a nonzero
// ceiling.
[[nodiscard]] bool TryParseLegacyVersionClockRecord(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* out_ceiling);

// The initialized-store marker (`.chunkdb.initialized`): "CKID", u64 1,
// CRC32 of the first 12 bytes.
[[nodiscard]] bool IsValidLegacyInitializedMarker(const std::vector<std::uint8_t>& bytes);

struct LegacyConditionalIntent {
    // Rollback ("CKRB"): the WAL is cut to `boundary`. Committed ("CKRC"):
    // the WAL is kept whole.
    bool rollback = false;
    std::uint64_t boundary = 0;
};

// Parses a conditional intent (`.chunkdb.intents/<wal path, '/' as "__">.rollback`):
// "CKRB" or "CKRC", u64 WAL size before the mutation, CRC32 of the first 12
// bytes. Returns false for anything else.
[[nodiscard]] bool TryParseLegacyConditionalIntent(
    const std::vector<std::uint8_t>& bytes,
    LegacyConditionalIntent* out);

}  // namespace chunkdb::legacy
