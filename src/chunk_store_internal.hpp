#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/extra_data.hpp"
#include "chunkdb/geometry.hpp"
#include "chunkdb/types.hpp"

namespace chunkdb {

inline constexpr std::size_t kWalMagicSize = 8;

// Chunk image (docs/STORAGE_FORMAT.md Section 3): a fixed header, a directory
// of sections, a header CRC over both, then the section bodies.
inline constexpr std::size_t kImageMagicSize = 8;
inline constexpr std::uint8_t kImageMagic[kImageMagicSize] = {'C', 'H', 'K', 'I', 'M', 'A', 'G', 'E'};
inline constexpr std::uint16_t kImageFormatVersion = 1;
// magic, version u16, section_count u16, three u32 feature sets, store id,
// chunk_x, chunk_y, revision, commit_time_ms.
inline constexpr std::size_t kImageFixedHeaderSize = kImageMagicSize + 2U + 2U + 12U + 16U + 8U * 4U;
// type u16, flags u16, stored_size u32, raw_size u32, crc32 u32.
inline constexpr std::size_t kImageSectionEntrySize = 16U;
inline constexpr std::uint16_t kImageMaxSections = 64U;
inline constexpr std::uint16_t kImageSectionPayload = 1U;
inline constexpr std::uint16_t kImageSectionPresence = 2U;
// Per-block extra data (ChunkExtra::Encode); only in images of chunks that
// have some, with the image's kFeatureExtraData bit set.
inline constexpr std::uint16_t kImageSectionExtra = 3U;
inline constexpr std::uint16_t kImageSectionFlagZrle = 1U;
// WAL (docs/STORAGE_FORMAT.md Section 4): a checksummed file header, then an
// append-only sequence of frames, one per mutation.
inline constexpr std::uint8_t kWalMagic[kWalMagicSize] = {'C', 'H', 'K', 'W', 'A', 'L', 'O', 'G'};
inline constexpr std::uint16_t kWalFormatVersion = 1;
// magic, version u16, reserved u16, three u32 feature sets, store id,
// chunk_x, chunk_y, header CRC.
inline constexpr std::size_t kWalHeaderSize = kWalMagicSize + 2U + 2U + 12U + 16U + 8U + 8U + 4U;
inline constexpr std::size_t kWalFrameMagicSize = 4;
inline constexpr std::uint8_t kWalFrameMagic[kWalFrameMagicSize] = {'F', 'R', 'M', '2'};
// magic, revision u64, commit_time_ms u64, frame_flags u16, tlv_size u16,
// record_count u32, body_size u32; the TLV area and a header CRC follow.
inline constexpr std::size_t kWalFrameFixedHeaderSize = kWalFrameMagicSize + 8U + 8U + 2U + 2U + 4U + 4U;
inline constexpr std::size_t kWalFrameHeaderCrcSize = 4U;
inline constexpr std::size_t kWalFrameTrailerSize = 4U;
// Record: type u8, size u32, then `size` body bytes.
inline constexpr std::size_t kWalRecordHeaderSize = 1U + 4U;
inline constexpr std::uint8_t kWalRecordSpan = 1U;
// A span body: byte_offset u32, then the bytes to write there.
inline constexpr std::size_t kWalSpanOffsetSize = 4U;
// Extra data (kFeatureExtraData). EXTRA_PUT: block_index u32, bit_length u32,
// value bytes. EXTRA_DEL: block_index u32. EXTRA_REPLACE: a whole EXTRA
// section. A frame holds EXTRA_PUT/EXTRA_DEL records in strictly ascending
// block order, or one EXTRA_REPLACE and neither of them.
inline constexpr std::uint8_t kWalRecordExtraPut = 2U;
inline constexpr std::uint8_t kWalRecordExtraDel = 3U;
inline constexpr std::uint8_t kWalRecordExtraReplace = 4U;
// TLV entry: type u16, length u16, value.
inline constexpr std::size_t kWalTlvHeaderSize = 4U;
inline constexpr std::uint16_t kWalTlvTag = 1U;
inline constexpr std::uint64_t kWriterHeartbeatIntervalMs = 250;
inline constexpr std::uint64_t kWriterStaleThresholdMs = 5000;
inline constexpr std::uint64_t kAtomicTmpCurrentPidCleanupMinAgeMs = 500;
inline constexpr int kAtomicWriteRetryCount = 6;
inline constexpr auto kAtomicWriteRetryBaseDelay = std::chrono::milliseconds(2);
inline constexpr std::size_t kWalOpenStreamsFdReserve = 32;
inline constexpr auto kWalStreamCapacityWaitTimeout = std::chrono::milliseconds(1000);
inline constexpr auto kWalStreamCapacityRetryInterval = std::chrono::milliseconds(10);
inline constexpr std::size_t kEvictionRefillLargeChunkBudget = 16;

inline constexpr std::string_view kExtraDataDisabled =
    "extra data is not enabled on this table (set its extra_max_block_bits option)";

struct ChunkStateImage {
    std::vector<std::uint8_t> payload;
    std::vector<std::uint8_t> presence_bitmap;
    // Revision and commit time of the last mutation the image captures.
    std::uint64_t revision = 0;
    std::uint64_t commit_time_ms = 0;
    // Feature flags of the features this image uses.
    FeatureFlags features;
    // Every entry belongs to a present block.
    ChunkExtra extra;
};

void WriteLe16(std::vector<std::uint8_t>& out, std::uint16_t value);
void WriteLe32(std::vector<std::uint8_t>& out, std::uint32_t value);
void WriteLe64(std::vector<std::uint8_t>& out, std::uint64_t value);
[[nodiscard]] std::uint16_t ReadLe16(const std::vector<std::uint8_t>& data, std::size_t offset);
[[nodiscard]] std::uint32_t ReadLe32(const std::vector<std::uint8_t>& data, std::size_t offset);
[[nodiscard]] std::uint64_t ReadLe64(const std::vector<std::uint8_t>& data, std::size_t offset);

[[nodiscard]] std::size_t ChunkPresenceBitmapBytes(const Geometry& geometry) noexcept;
[[nodiscard]] std::size_t ChunkStateBytes(const Geometry& geometry) noexcept;
void MaskUnusedPresenceBits(const Geometry& geometry, std::vector<std::uint8_t>* presence_bitmap);
void MaskUnusedPayloadBits(const Geometry& geometry, std::vector<std::uint8_t>* payload);
[[nodiscard]] std::vector<std::uint8_t> FullPresenceBitmap(const Geometry& geometry);
[[nodiscard]] bool BlockPresent(const std::vector<std::uint8_t>& presence_bitmap, std::size_t block_index);
void SetBlockPresent(std::vector<std::uint8_t>* presence_bitmap, std::size_t block_index, bool present);
[[nodiscard]] bool ChunkPresent(const std::vector<std::uint8_t>& presence_bitmap) noexcept;
[[nodiscard]] std::string PresenceBitsText(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& presence_bitmap);
void CanonicalizeAbsentBlocks(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& presence_bitmap,
    std::vector<std::uint8_t>* payload);
[[nodiscard]] std::vector<std::uint8_t> BuildChunkStateBytes(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& payload,
    const std::vector<std::uint8_t>& presence_bitmap);
void SplitChunkStateBytes(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& state,
    std::vector<std::uint8_t>* payload,
    std::vector<std::uint8_t>* presence_bitmap);

[[nodiscard]] std::string CanonicalPathKey(const std::filesystem::path& path);
[[nodiscard]] std::uint64_t CurrentProcessIdValue();
[[nodiscard]] std::uint64_t UnixMillisNow();
[[nodiscard]] std::int64_t FloorDiv(std::int64_t value, std::int64_t divisor);
[[nodiscard]] bool ConsumeFailpointEnv(const char* key);
[[nodiscard]] std::chrono::milliseconds ConsumeFailpointDelayMs(const char* key);
[[nodiscard]] bool TryParseInt64(const std::string& text, std::int64_t* out);
[[nodiscard]] bool TryParseUint64(const std::string& text, std::uint64_t* out);
[[nodiscard]] bool IsProcessAlive(std::int64_t pid);

[[nodiscard]] std::vector<std::uint8_t> LoadFile(const std::filesystem::path& path);
// `chunkdb.version`: magic "CKVR", little-endian u64 exclusive ceiling,
// then CRC32 over the preceding 12 bytes (16 bytes total).
[[nodiscard]] std::vector<std::uint8_t> SerializeVersionClockRecord(
    std::uint64_t ceiling);
[[nodiscard]] bool TryParseVersionClockRecord(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* out_ceiling);
// `chunkdb.snapshot`: magic "CKSG", little-endian u64 generation,
// then CRC32 over the preceding 12 bytes (16 bytes total).
[[nodiscard]] std::vector<std::uint8_t> SerializeSnapshotGenerationRecord(
    std::uint64_t generation);
[[nodiscard]] bool TryParseSnapshotGenerationRecord(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* out_generation);
enum class ConditionalIntentState {
    kRollback,
    kCommitted,
};
[[nodiscard]] bool TryParseConditionalIntent(
    const std::vector<std::uint8_t>& bytes,
    ConditionalIntentState* out_state,
    std::uint64_t* out_committed_wal_size);
[[nodiscard]] bool TryParseConditionalRollbackIntent(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* out_committed_wal_size);
[[nodiscard]] bool IsValidInitializedStoreMarker(
    const std::vector<std::uint8_t>& bytes);

// Conditional-intent artifacts live in one dedicated shallow directory so
// startup recovery scans O(pending intents) files instead of recursively
// walking the whole data tree. The intent file name embeds the WAL path
// relative to the data directory with `__` as the separator, which is
// unambiguous for the layout grammar (`L_<i>_<j>/C_<x>_<y>.wal`).
inline constexpr std::string_view kConditionalIntentDirName = ".chunkdb.intents";
// Process-lock control directory (holds writer.lock / writer.meta). It is not
// storage state and its lock file is held with exclusive open semantics on
// Windows, so data-directory walks that sync or read storage artifacts must
// skip it.
inline constexpr std::string_view kProcessLockDirName = ".chunkdb.lock";
[[nodiscard]] std::filesystem::path ConditionalIntentDirectory(
    const std::filesystem::path& data_dir);
[[nodiscard]] std::filesystem::path ConditionalIntentPathForWal(
    const std::filesystem::path& data_dir,
    const std::filesystem::path& wal_path);
[[nodiscard]] std::filesystem::path WalPathForConditionalIntent(
    const std::filesystem::path& data_dir,
    const std::filesystem::path& intent_path);

// Serializes a chunk image. With zrle compression each section is stored
// compressed; the section CRCs cover the raw bytes either way. Non-empty
// `extra` adds an EXTRA section and sets the image's kFeatureExtraData bit.
[[nodiscard]] std::vector<std::uint8_t> SerializeChunkImage(
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    const std::vector<std::uint8_t>& payload,
    const std::vector<std::uint8_t>& presence_bitmap,
    CheckpointCompression compression,
    std::uint64_t revision,
    std::uint64_t commit_time_ms,
    const StoreId& store_id,
    const ChunkExtra* extra = nullptr);
// The WAL file header for a chunk of this store.
[[nodiscard]] std::vector<std::uint8_t> BuildWalHeader(
    const ChunkCoord& chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& features);
// Parses and fully validates a chunk image of this store: header CRC, store
// id, coordinate, a non-zero revision, feature flags within the store's,
// the section directory, every section's size and CRC, and the extra data
// (well formed, only on present blocks, only with the image's
// kFeatureExtraData bit). Throws std::runtime_error naming the defect.
[[nodiscard]] ChunkStateImage ParseChunkImage(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& store_features);

// Read-only stores cannot persist the deterministic clock and use an opaque
// process-local random token instead. Read-write stores use NextChunkVersion.
[[nodiscard]] std::uint64_t NewChunkVersionToken();

}  // namespace chunkdb
