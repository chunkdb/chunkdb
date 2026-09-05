#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/geometry.hpp"
#include "chunkdb/types.hpp"

namespace chunkdb {

inline constexpr std::size_t kChunkMagicSize = 8;
inline constexpr std::size_t kWalMagicSize = 8;
inline constexpr std::size_t kWalRecordMagicSize = 4;
inline constexpr std::size_t kRegionMagicSize = 8;

// Chunk image versions. 4/5 (format v2) append the chunk revision and a
// header CRC to the 1.x header; 4 is raw state, 5 is a zrle-compressed
// state blob. 1, 2 and 3 are the 1.x layouts, accepted on read only.
inline constexpr std::uint16_t kChunkFileVersion = 4;
inline constexpr std::uint16_t kChunkFileVersionCompressed = 5;
inline constexpr std::uint16_t kChunkFileVersionLegacy = 1;
inline constexpr std::uint16_t kChunkFileVersionV2 = 2;
inline constexpr std::uint16_t kChunkFileVersionV3Compressed = 3;
// WAL versions. 4 (format v2) frames one mutation per frame with a record
// CRC over header and body and a frame CRC; 2 and 3 are the 1.x record
// streams, accepted on read only.
inline constexpr std::uint16_t kWalFileVersion = 4;
inline constexpr std::uint16_t kWalFileVersionV3 = 3;
inline constexpr std::uint16_t kWalFileVersionLegacy = 2;
inline constexpr std::uint16_t kRegionFileVersion = 2;
inline constexpr std::uint16_t kRegionFileVersionLegacy = 1;

inline constexpr std::uint8_t kChunkMagic[kChunkMagicSize] = {'C', 'H', 'K', 'D', 'A', 'T', 'A', '1'};
inline constexpr std::uint8_t kWalMagic[kWalMagicSize] = {'C', 'H', 'K', 'W', 'A', 'L', '0', '2'};
inline constexpr std::uint8_t kWalRecordMagic[kWalRecordMagicSize] = {'D', 'L', 'T', '1'};
inline constexpr std::size_t kWalFrameMagicSize = 4;
inline constexpr std::uint8_t kWalFrameMagic[kWalFrameMagicSize] = {'F', 'R', 'M', '1'};
inline constexpr std::uint8_t kRegionMagic[kRegionMagicSize] = {'C', 'H', 'K', 'R', 'G', 'N', '1', '0'};

inline constexpr std::size_t kChunkHeaderSize =
    kChunkMagicSize + 2U + 2U + 4U + 4U + 8U + 8U + 4U + 4U + 8U;
// v4/v5 image header = the 1.x header + revision (u64) + header CRC.
inline constexpr std::size_t kChunkHeaderSizeV4 = kChunkHeaderSize + 8U + 4U;
inline constexpr std::size_t kWalHeaderSize = kWalMagicSize + 2U + 2U + 4U + 4U + 8U + 8U;
// 1.x record: magic, byte_offset, data_size, body CRC.
inline constexpr std::size_t kWalRecordHeaderSize = kWalRecordMagicSize + 4U + 2U + 4U;
// v4 frame: magic, revision, record_count, body_size, header CRC; then
// records of byte_offset, data_size, body, record CRC (over the three);
// then a frame CRC over all record bytes.
inline constexpr std::size_t kWalFrameHeaderSize = kWalFrameMagicSize + 8U + 2U + 4U + 4U;
inline constexpr std::size_t kWalFrameTrailerSize = 4U;
inline constexpr std::size_t kWalFrameRecordOverhead = 4U + 2U + 4U;
inline constexpr std::size_t kRegionHeaderSize =
    kRegionMagicSize + 2U + 2U + 4U + 4U + 4U + 8U + 8U + 4U + 4U;
inline constexpr std::uint64_t kWriterHeartbeatIntervalMs = 250;
inline constexpr std::uint64_t kWriterStaleThresholdMs = 5000;
inline constexpr std::uint64_t kAtomicTmpCurrentPidCleanupMinAgeMs = 500;
inline constexpr int kAtomicWriteRetryCount = 6;
inline constexpr auto kAtomicWriteRetryBaseDelay = std::chrono::milliseconds(2);
inline constexpr std::size_t kWalOpenStreamsFdReserve = 32;
inline constexpr auto kWalStreamCapacityWaitTimeout = std::chrono::milliseconds(1000);
inline constexpr auto kWalStreamCapacityRetryInterval = std::chrono::milliseconds(10);
inline constexpr std::size_t kEvictionRefillLargeChunkBudget = 16;

struct ChunkStateImage {
    std::vector<std::uint8_t> payload;
    std::vector<std::uint8_t> presence_bitmap;
    std::uint16_t version = 0;
    // Persisted chunk revision (format v2); zero for 1.x images.
    std::uint64_t revision = 0;
};

struct RegionChunkAddress {
    std::int64_t region_x = 0;
    std::int64_t region_y = 0;
    std::uint32_t local_x = 0;
    std::uint32_t local_y = 0;
    std::uint32_t slot_index = 0;
};

struct RegionFileImage {
    std::uint32_t span_chunks = 0;
    std::int64_t region_x = 0;
    std::int64_t region_y = 0;
    std::uint32_t slot_count = 0;
    std::uint32_t payload_bytes = 0;
    std::vector<std::uint8_t> present_bitmap;
    std::vector<std::uint32_t> slot_crc;
    std::vector<std::uint8_t> slot_payloads;
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

[[nodiscard]] RegionChunkAddress ComputeRegionChunkAddress(
    const ChunkCoord& chunk_coord,
    std::size_t span_chunks);
[[nodiscard]] std::filesystem::path RegionDataPath(
    const std::filesystem::path& data_dir,
    const ChunkCoord& chunk_coord,
    std::size_t span_chunks);
[[nodiscard]] std::filesystem::path LayoutWalPath(
    const std::filesystem::path& data_dir,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    StorageLayoutMode mode);
[[nodiscard]] RegionFileImage BuildEmptyRegionFileImage(
    const Geometry& geometry,
    const RegionChunkAddress& addr,
    std::size_t span_chunks);
[[nodiscard]] bool RegionSlotPresent(const RegionFileImage& image, std::uint32_t slot_index);
void SetRegionSlotPresent(RegionFileImage* image, std::uint32_t slot_index, bool present);
[[nodiscard]] std::vector<std::uint8_t> SerializeRegionFileImage(
    const Geometry& geometry,
    const RegionFileImage& image);
[[nodiscard]] RegionFileImage ParseRegionFileImage(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const RegionChunkAddress& expected_addr,
    std::size_t expected_span_chunks);
[[nodiscard]] std::vector<std::uint8_t> ExtractRegionSlotState(
    const RegionFileImage& image,
    std::uint32_t slot_index);
void WriteRegionSlotState(
    RegionFileImage* image,
    std::uint32_t slot_index,
    const std::vector<std::uint8_t>& payload);
[[nodiscard]] std::mutex& RegionIoMutex();

[[nodiscard]] std::vector<std::uint8_t> LoadFile(const std::filesystem::path& path);
// `chunkdb.version`: magic "CKVR", little-endian u64 exclusive ceiling,
// then CRC32 over the preceding 12 bytes (16 bytes total).
[[nodiscard]] std::vector<std::uint8_t> SerializeVersionClockRecord(
    std::uint64_t ceiling);
[[nodiscard]] bool TryParseVersionClockRecord(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* out_ceiling);
// Intermediate development builds stored only the little-endian u64 ceiling.
// It remains safe to upgrade because the value is still an exclusive bound
// over every token those builds could have issued.
[[nodiscard]] bool TryParseIntermediateVersionClockRecord(
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

[[nodiscard]] ChunkStateImage ParseChunkImage(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord);

// Read-only stores cannot persist the deterministic clock and use an opaque
// process-local random token instead. Read-write stores use NextChunkVersion.
[[nodiscard]] std::uint64_t NewChunkVersionToken();

}  // namespace chunkdb
