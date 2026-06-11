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

inline constexpr std::uint16_t kChunkFileVersion = 2;
inline constexpr std::uint16_t kChunkFileVersionLegacy = 1;
inline constexpr std::uint16_t kWalFileVersion = 3;
inline constexpr std::uint16_t kWalFileVersionLegacy = 2;
inline constexpr std::uint16_t kRegionFileVersion = 2;
inline constexpr std::uint16_t kRegionFileVersionLegacy = 1;

inline constexpr std::uint8_t kChunkMagic[kChunkMagicSize] = {'C', 'H', 'K', 'D', 'A', 'T', 'A', '1'};
inline constexpr std::uint8_t kWalMagic[kWalMagicSize] = {'C', 'H', 'K', 'W', 'A', 'L', '0', '2'};
inline constexpr std::uint8_t kWalRecordMagic[kWalRecordMagicSize] = {'D', 'L', 'T', '1'};
inline constexpr std::uint8_t kRegionMagic[kRegionMagicSize] = {'C', 'H', 'K', 'R', 'G', 'N', '1', '0'};

inline constexpr std::size_t kChunkHeaderSize =
    kChunkMagicSize + 2U + 2U + 4U + 4U + 8U + 8U + 4U + 4U + 8U;
inline constexpr std::size_t kWalHeaderSize = kWalMagicSize + 2U + 2U + 4U + 4U + 8U + 8U;
inline constexpr std::size_t kWalRecordHeaderSize = kWalRecordMagicSize + 4U + 2U + 4U;
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

}  // namespace chunkdb
