#include "chunkdb/chunk_store.hpp"

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "eviction.hpp"
#include "process_lock.hpp"
#include "wal_replay.hpp"
#include "wal_stream_pool.hpp"
#include "wal_writer.hpp"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <vector>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <stdio.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <unistd.h>
#endif

namespace chunkdb {





[[nodiscard]] std::string CanonicalPathKey(const std::filesystem::path& path) {
    std::error_code ec;
    const auto canonical = std::filesystem::weakly_canonical(path, ec);
    if (!ec) {
        return canonical.lexically_normal().string();
    }
    ec.clear();
    const auto absolute = std::filesystem::absolute(path, ec);
    if (!ec) {
        return absolute.lexically_normal().string();
    }
    return path.lexically_normal().string();
}


[[nodiscard]] std::size_t ChunkPresenceBitmapBytes(const Geometry& geometry) noexcept {
    return (geometry.ChunkBlockCount() + 7U) / 8U;
}

[[nodiscard]] std::size_t ChunkStateBytes(const Geometry& geometry) noexcept {
    return geometry.ChunkPayloadBytes() + ChunkPresenceBitmapBytes(geometry);
}

void MaskUnusedPresenceBits(const Geometry& geometry, std::vector<std::uint8_t>* presence_bitmap) {
    if (presence_bitmap == nullptr || presence_bitmap->empty()) {
        return;
    }

    const std::size_t used_bits = geometry.ChunkBlockCount();
    const std::size_t trailing_bits = presence_bitmap->size() * 8U - used_bits;
    if (trailing_bits == 0) {
        return;
    }

    const std::uint8_t mask = static_cast<std::uint8_t>(0xFFU >> trailing_bits);
    presence_bitmap->back() &= mask;
}

[[nodiscard]] std::vector<std::uint8_t> FullPresenceBitmap(const Geometry& geometry) {
    std::vector<std::uint8_t> presence_bitmap(ChunkPresenceBitmapBytes(geometry), 0xFFU);
    MaskUnusedPresenceBits(geometry, &presence_bitmap);
    return presence_bitmap;
}

[[nodiscard]] bool BlockPresent(
    const std::vector<std::uint8_t>& presence_bitmap,
    std::size_t block_index) {
    const std::size_t byte_index = block_index / 8U;
    const std::uint8_t bit_mask = static_cast<std::uint8_t>(1U << (block_index % 8U));
    return (presence_bitmap[byte_index] & bit_mask) != 0U;
}

void SetBlockPresent(
    std::vector<std::uint8_t>* presence_bitmap,
    std::size_t block_index,
    bool present) {
    const std::size_t byte_index = block_index / 8U;
    const std::uint8_t bit_mask = static_cast<std::uint8_t>(1U << (block_index % 8U));
    if (present) {
        (*presence_bitmap)[byte_index] |= bit_mask;
    } else {
        (*presence_bitmap)[byte_index] &= static_cast<std::uint8_t>(~bit_mask);
    }
}

[[nodiscard]] bool ChunkPresent(const std::vector<std::uint8_t>& presence_bitmap) noexcept {
    return std::any_of(
        presence_bitmap.begin(),
        presence_bitmap.end(),
        [](std::uint8_t byte) { return byte != 0U; });
}

[[nodiscard]] std::string PresenceBitsText(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& presence_bitmap) {
    return BitCodec::ExtractBits(presence_bitmap, 0, geometry.ChunkBlockCount());
}

void CanonicalizeAbsentBlocks(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& presence_bitmap,
    std::vector<std::uint8_t>* payload) {
    if (payload == nullptr) {
        throw std::invalid_argument("payload must not be null");
    }

    const std::size_t block_bits = geometry.config().block_bits;
    const std::size_t block_count = geometry.ChunkBlockCount();
    const std::string zero_bits(block_bits, '0');
    for (std::size_t block_index = 0; block_index < block_count; ++block_index) {
        if (!BlockPresent(presence_bitmap, block_index)) {
            BitCodec::WriteBits(*payload, block_index * block_bits, zero_bits);
        }
    }
}

[[nodiscard]] std::vector<std::uint8_t> BuildChunkStateBytes(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& payload,
    const std::vector<std::uint8_t>& presence_bitmap) {
    if (payload.size() != geometry.ChunkPayloadBytes()) {
        throw std::invalid_argument("payload size does not match geometry");
    }
    if (presence_bitmap.size() != ChunkPresenceBitmapBytes(geometry)) {
        throw std::invalid_argument("presence bitmap size does not match geometry");
    }

    std::vector<std::uint8_t> state;
    state.reserve(ChunkStateBytes(geometry));
    state.insert(state.end(), payload.begin(), payload.end());
    state.insert(state.end(), presence_bitmap.begin(), presence_bitmap.end());
    return state;
}


void SplitChunkStateBytes(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& state,
    std::vector<std::uint8_t>* payload,
    std::vector<std::uint8_t>* presence_bitmap) {
    const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
    const std::size_t presence_bytes = ChunkPresenceBitmapBytes(geometry);
    if (state.size() != payload_bytes + presence_bytes) {
        throw std::invalid_argument("chunk state size does not match geometry");
    }

    payload->assign(state.begin(), state.begin() + static_cast<std::ptrdiff_t>(payload_bytes));
    presence_bitmap->assign(
        state.begin() + static_cast<std::ptrdiff_t>(payload_bytes),
        state.end());
    MaskUnusedPresenceBits(geometry, presence_bitmap);
}


// Maximum number of files examined during the startup scan. The scan is
// informational only (results appear in the startup log message); it does not
// affect correctness. Capping it keeps startup latency bounded on large worlds
// that contain hundreds of thousands of chunk files.
constexpr std::uint64_t kStartupScanFileLimit = 100'000;

struct StartupRecoveryScan {
    std::uint64_t wal_files = 0;
    std::uint64_t checkpoint_files = 0;
    std::uint64_t region_files = 0;
    bool scan_capped = false;
};

StartupRecoveryScan ScanStartupRecovery(const std::filesystem::path& data_dir) {
    StartupRecoveryScan result;
    std::error_code exists_ec;
    if (!std::filesystem::exists(data_dir, exists_ec) || exists_ec) {
        return result;
    }

    std::uint64_t examined = 0;
    const std::filesystem::recursive_directory_iterator end;
    std::error_code it_ec;
    for (std::filesystem::recursive_directory_iterator it(data_dir, it_ec);
         it != end && !it_ec;
         it.increment(it_ec)) {
        if (!it->is_regular_file()) {
            continue;
        }
        if (examined >= kStartupScanFileLimit) {
            result.scan_capped = true;
            break;
        }
        ++examined;
        const auto ext = it->path().extension();
        if (ext == ".wal") {
            ++result.wal_files;
        } else if (ext == ".chk") {
            ++result.checkpoint_files;
        } else if (ext == ".rgn") {
            ++result.region_files;
        }
    }
    return result;
}

std::uint64_t CurrentProcessIdValue() {
#ifdef _WIN32
    return static_cast<std::uint64_t>(GetCurrentProcessId());
#else
    return static_cast<std::uint64_t>(::getpid());
#endif
}

std::uint64_t UnixMillisNow() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

std::int64_t FloorDiv(std::int64_t value, std::int64_t divisor) {
    if (divisor <= 0) {
        throw std::invalid_argument("divisor must be > 0");
    }
    std::int64_t q = value / divisor;
    const std::int64_t r = value % divisor;
    if (r != 0 && ((r < 0) != (divisor < 0))) {
        --q;
    }
    return q;
}

bool ConsumeFailpointEnv(const char* key) {
    const char* value = std::getenv(key);
    if (value == nullptr || value[0] == '\0') {
        return false;
    }
#ifdef _WIN32
    (void)_putenv_s(key, "");
#else
    (void)unsetenv(key);
#endif
    return true;
}

[[nodiscard]] std::chrono::milliseconds ConsumeFailpointDelayMs(const char* key) {
    const char* value = std::getenv(key);
    if (value == nullptr || value[0] == '\0') {
        return std::chrono::milliseconds(0);
    }

    std::uint64_t delay_ms = 0;
    try {
        std::size_t consumed = 0;
        delay_ms = static_cast<std::uint64_t>(std::stoull(value, &consumed, 10));
        if (consumed != std::strlen(value)) {
            delay_ms = 0;
        }
    } catch (...) {
        delay_ms = 0;
    }
#ifdef _WIN32
    (void)_putenv_s(key, "");
#else
    (void)unsetenv(key);
#endif
    return std::chrono::milliseconds(delay_ms);
}


void WriteLe16(std::vector<std::uint8_t>& out, std::uint16_t value) {
    out.push_back(static_cast<std::uint8_t>(value & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
}

void WriteLe32(std::vector<std::uint8_t>& out, std::uint32_t value) {
    out.push_back(static_cast<std::uint8_t>(value & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 16U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 24U) & 0xFFU));
}

void WriteLe64(std::vector<std::uint8_t>& out, std::uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFU));
    }
}

std::uint16_t ReadLe16(const std::vector<std::uint8_t>& data, std::size_t offset) {
    return static_cast<std::uint16_t>(
        static_cast<std::uint16_t>(data[offset]) |
        static_cast<std::uint16_t>(data[offset + 1] << 8U));
}

std::uint32_t ReadLe32(const std::vector<std::uint8_t>& data, std::size_t offset) {
    return static_cast<std::uint32_t>(data[offset]) |
           (static_cast<std::uint32_t>(data[offset + 1]) << 8U) |
           (static_cast<std::uint32_t>(data[offset + 2]) << 16U) |
           (static_cast<std::uint32_t>(data[offset + 3]) << 24U);
}

std::uint64_t ReadLe64(const std::vector<std::uint8_t>& data, std::size_t offset) {
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value |= static_cast<std::uint64_t>(data[offset + i]) << (8U * i);
    }
    return value;
}

RegionChunkAddress ComputeRegionChunkAddress(const ChunkCoord& chunk_coord, std::size_t span_chunks) {
    if (span_chunks == 0) {
        throw std::invalid_argument("experimental_region_span_chunks must be > 0");
    }
    const auto span = static_cast<std::int64_t>(span_chunks);
    const auto rx = FloorDiv(chunk_coord.x, span);
    const auto ry = FloorDiv(chunk_coord.y, span);
    const auto lx = static_cast<std::uint32_t>(chunk_coord.x - rx * span);
    const auto ly = static_cast<std::uint32_t>(chunk_coord.y - ry * span);
    const auto span_u32 = static_cast<std::uint32_t>(span_chunks);
    return RegionChunkAddress{
        .region_x = rx,
        .region_y = ry,
        .local_x = lx,
        .local_y = ly,
        .slot_index = ly * span_u32 + lx,
    };
}

std::filesystem::path RegionDataPath(
    const std::filesystem::path& data_dir,
    const ChunkCoord& chunk_coord,
    std::size_t span_chunks) {
    const auto addr = ComputeRegionChunkAddress(chunk_coord, span_chunks);
    return data_dir /
           ("R_" + std::to_string(addr.region_x) + "_" + std::to_string(addr.region_y) + ".rgn");
}

std::filesystem::path LayoutWalPath(
    const std::filesystem::path& data_dir,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    StorageLayoutMode mode) {
    switch (mode) {
        case StorageLayoutMode::kFsSplitV1:
        case StorageLayoutMode::kFsRegionV1Experimental:
            return ChunkWalPath(data_dir, geometry, chunk_coord);
    }
    return ChunkWalPath(data_dir, geometry, chunk_coord);
}

std::size_t RegionPresentBitmapBytes(std::uint32_t slot_count) {
    return (static_cast<std::size_t>(slot_count) + 7U) / 8U;
}

RegionFileImage BuildEmptyRegionFileImage(
    const Geometry& geometry,
    const RegionChunkAddress& addr,
    std::size_t span_chunks) {
    const auto span_u32 = static_cast<std::uint32_t>(span_chunks);
    const std::uint32_t slot_count = span_u32 * span_u32;
    const std::uint32_t payload_bytes = static_cast<std::uint32_t>(ChunkStateBytes(geometry));

    RegionFileImage image;
    image.span_chunks = span_u32;
    image.region_x = addr.region_x;
    image.region_y = addr.region_y;
    image.slot_count = slot_count;
    image.payload_bytes = payload_bytes;
    image.present_bitmap.assign(RegionPresentBitmapBytes(slot_count), 0U);
    image.slot_crc.assign(slot_count, 0U);
    image.slot_payloads.assign(static_cast<std::size_t>(slot_count) * payload_bytes, 0U);
    return image;
}

bool RegionSlotPresent(const RegionFileImage& image, std::uint32_t slot_index) {
    if (slot_index >= image.slot_count) {
        throw std::runtime_error("region slot index out of range");
    }
    const std::size_t byte_index = slot_index / 8U;
    const std::uint8_t bit_mask = static_cast<std::uint8_t>(1U << (slot_index % 8U));
    return (image.present_bitmap[byte_index] & bit_mask) != 0U;
}

void SetRegionSlotPresent(RegionFileImage* image, std::uint32_t slot_index, bool present) {
    if (image == nullptr || slot_index >= image->slot_count) {
        throw std::runtime_error("region slot index out of range");
    }
    const std::size_t byte_index = slot_index / 8U;
    const std::uint8_t bit_mask = static_cast<std::uint8_t>(1U << (slot_index % 8U));
    if (present) {
        image->present_bitmap[byte_index] |= bit_mask;
    } else {
        image->present_bitmap[byte_index] &= static_cast<std::uint8_t>(~bit_mask);
    }
}

std::vector<std::uint8_t> SerializeRegionFileImage(
    const Geometry& geometry,
    const RegionFileImage& image) {
    const std::uint32_t expected_payload_bytes = static_cast<std::uint32_t>(ChunkStateBytes(geometry));
    if (image.payload_bytes != expected_payload_bytes) {
        throw std::runtime_error("region payload bytes mismatch");
    }
    if (image.slot_crc.size() != image.slot_count) {
        throw std::runtime_error("region crc table size mismatch");
    }
    if (image.present_bitmap.size() != RegionPresentBitmapBytes(image.slot_count)) {
        throw std::runtime_error("region presence bitmap size mismatch");
    }
    if (image.slot_payloads.size() != static_cast<std::size_t>(image.slot_count) * image.payload_bytes) {
        throw std::runtime_error("region payload area size mismatch");
    }

    std::vector<std::uint8_t> out;
    out.reserve(
        kRegionHeaderSize +
        image.present_bitmap.size() +
        image.slot_crc.size() * 4U +
        image.slot_payloads.size());

    out.insert(out.end(), kRegionMagic, kRegionMagic + kRegionMagicSize);
    WriteLe16(out, kRegionFileVersion);
    WriteLe16(out, static_cast<std::uint16_t>(geometry.config().block_bits));
    WriteLe32(out, geometry.config().chunk_width_blocks);
    WriteLe32(out, geometry.config().chunk_height_blocks);
    WriteLe32(out, image.span_chunks);
    WriteLe64(out, static_cast<std::uint64_t>(image.region_x));
    WriteLe64(out, static_cast<std::uint64_t>(image.region_y));
    WriteLe32(out, image.payload_bytes);
    WriteLe32(out, image.slot_count);

    out.insert(out.end(), image.present_bitmap.begin(), image.present_bitmap.end());
    for (const auto crc : image.slot_crc) {
        WriteLe32(out, crc);
    }
    out.insert(out.end(), image.slot_payloads.begin(), image.slot_payloads.end());
    return out;
}

RegionFileImage ParseRegionFileImage(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const RegionChunkAddress& expected_addr,
    std::size_t expected_span_chunks) {
    if (bytes.size() < kRegionHeaderSize) {
        throw std::runtime_error("region file too small");
    }
    if (std::memcmp(bytes.data(), kRegionMagic, kRegionMagicSize) != 0) {
        throw std::runtime_error("invalid region file magic");
    }

    const std::uint16_t version = ReadLe16(bytes, 8U);
    if (version != kRegionFileVersion && version != kRegionFileVersionLegacy) {
        throw std::runtime_error("unsupported region file version");
    }
    const std::uint16_t block_bits = ReadLe16(bytes, 10U);
    const std::uint32_t chunk_width = ReadLe32(bytes, 12U);
    const std::uint32_t chunk_height = ReadLe32(bytes, 16U);
    if (block_bits != geometry.config().block_bits ||
        chunk_width != geometry.config().chunk_width_blocks ||
        chunk_height != geometry.config().chunk_height_blocks) {
        throw std::runtime_error("region geometry mismatch");
    }

    const std::uint32_t span_chunks = ReadLe32(bytes, 20U);
    const auto region_x = static_cast<std::int64_t>(ReadLe64(bytes, 24U));
    const auto region_y = static_cast<std::int64_t>(ReadLe64(bytes, 32U));
    const std::uint32_t payload_bytes = ReadLe32(bytes, 40U);
    const std::uint32_t slot_count = ReadLe32(bytes, 44U);

    const auto expected_span_u32 = static_cast<std::uint32_t>(expected_span_chunks);
    if (span_chunks != expected_span_u32) {
        throw std::runtime_error("region span mismatch");
    }
    if (region_x != expected_addr.region_x || region_y != expected_addr.region_y) {
        throw std::runtime_error("region coordinate mismatch");
    }
    if (slot_count != expected_span_u32 * expected_span_u32) {
        throw std::runtime_error("region slot count mismatch");
    }

    const std::size_t bitmap_bytes = RegionPresentBitmapBytes(slot_count);
    const std::size_t crc_bytes = static_cast<std::size_t>(slot_count) * 4U;
    const std::size_t payload_area_bytes = static_cast<std::size_t>(slot_count) * payload_bytes;
    const std::size_t expected_size = kRegionHeaderSize + bitmap_bytes + crc_bytes + payload_area_bytes;
    if (bytes.size() != expected_size) {
        throw std::runtime_error("region file size mismatch");
    }

    RegionFileImage image;
    image.span_chunks = span_chunks;
    image.region_x = region_x;
    image.region_y = region_y;
    image.slot_count = slot_count;
    image.present_bitmap.assign(
        bytes.begin() + static_cast<std::ptrdiff_t>(kRegionHeaderSize),
        bytes.begin() + static_cast<std::ptrdiff_t>(kRegionHeaderSize + bitmap_bytes));
    std::vector<std::uint32_t> raw_crc(slot_count);
    std::size_t crc_cursor = kRegionHeaderSize + bitmap_bytes;
    for (std::size_t i = 0; i < slot_count; ++i) {
        raw_crc[i] = ReadLe32(bytes, crc_cursor);
        crc_cursor += 4U;
    }

    const std::vector<std::uint8_t> raw_slot_payloads(
        bytes.begin() + static_cast<std::ptrdiff_t>(kRegionHeaderSize + bitmap_bytes + crc_bytes),
        bytes.end());

    if (version == kRegionFileVersionLegacy) {
        const std::uint32_t legacy_payload_bytes = static_cast<std::uint32_t>(geometry.ChunkPayloadBytes());
        if (payload_bytes != legacy_payload_bytes) {
            throw std::runtime_error("region payload bytes mismatch");
        }

        const auto full_presence = FullPresenceBitmap(geometry);
        image.payload_bytes = static_cast<std::uint32_t>(ChunkStateBytes(geometry));
        image.slot_crc.assign(slot_count, 0U);
        image.slot_payloads.assign(
            static_cast<std::size_t>(slot_count) * image.payload_bytes,
            0U);

        for (std::size_t slot_index = 0; slot_index < slot_count; ++slot_index) {
            const std::size_t raw_offset = slot_index * static_cast<std::size_t>(payload_bytes);
            std::vector<std::uint8_t> legacy_payload(
                raw_slot_payloads.begin() + static_cast<std::ptrdiff_t>(raw_offset),
                raw_slot_payloads.begin() + static_cast<std::ptrdiff_t>(raw_offset + payload_bytes));
            if (RegionSlotPresent(image, static_cast<std::uint32_t>(slot_index)) &&
                Crc32(legacy_payload) != raw_crc[slot_index]) {
                throw std::runtime_error("region slot payload checksum mismatch");
            }

            const auto state = BuildChunkStateBytes(geometry, legacy_payload, full_presence);
            const std::size_t state_offset = slot_index * static_cast<std::size_t>(image.payload_bytes);
            std::copy(
                state.begin(),
                state.end(),
                image.slot_payloads.begin() + static_cast<std::ptrdiff_t>(state_offset));
            if (RegionSlotPresent(image, static_cast<std::uint32_t>(slot_index))) {
                image.slot_crc[slot_index] = Crc32(state);
            }
        }

        return image;
    }

    const std::uint32_t expected_payload_bytes = static_cast<std::uint32_t>(ChunkStateBytes(geometry));
    if (payload_bytes != expected_payload_bytes) {
        throw std::runtime_error("region payload bytes mismatch");
    }

    image.payload_bytes = payload_bytes;
    image.slot_crc = std::move(raw_crc);
    image.slot_payloads = std::move(raw_slot_payloads);
    return image;
}

std::vector<std::uint8_t> ExtractRegionSlotState(
    const RegionFileImage& image,
    std::uint32_t slot_index) {
    if (!RegionSlotPresent(image, slot_index)) {
        return {};
    }

    const std::size_t slot_offset = static_cast<std::size_t>(slot_index) * image.payload_bytes;
    std::vector<std::uint8_t> payload(
        image.slot_payloads.begin() + static_cast<std::ptrdiff_t>(slot_offset),
        image.slot_payloads.begin() + static_cast<std::ptrdiff_t>(slot_offset + image.payload_bytes));
    if (Crc32(payload) != image.slot_crc[slot_index]) {
        throw std::runtime_error("region slot payload checksum mismatch");
    }
    return payload;
}

void WriteRegionSlotState(
    RegionFileImage* image,
    std::uint32_t slot_index,
    const std::vector<std::uint8_t>& payload) {
    if (image == nullptr || payload.size() != image->payload_bytes) {
        throw std::runtime_error("region slot payload size mismatch");
    }
    const std::size_t slot_offset = static_cast<std::size_t>(slot_index) * image->payload_bytes;
    std::copy(payload.begin(), payload.end(), image->slot_payloads.begin() + static_cast<std::ptrdiff_t>(slot_offset));
    image->slot_crc[slot_index] = Crc32(payload);
    SetRegionSlotPresent(image, slot_index, true);
}

std::mutex& RegionIoMutex() {
    static std::mutex mutex;
    return mutex;
}




ChunkStateImage ParseChunkImage(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord) {
    if (bytes.size() < kChunkHeaderSize) {
        throw std::runtime_error("chunk file too small");
    }

    if (std::memcmp(bytes.data(), kChunkMagic, kChunkMagicSize) != 0) {
        throw std::runtime_error("invalid chunk magic");
    }

    const std::uint16_t version = ReadLe16(bytes, 8U);
    if (version != kChunkFileVersion && version != kChunkFileVersionLegacy) {
        throw std::runtime_error("unsupported chunk file version");
    }

    const std::uint16_t block_bits = ReadLe16(bytes, 10U);
    const std::uint32_t chunk_width = ReadLe32(bytes, 12U);
    const std::uint32_t chunk_height = ReadLe32(bytes, 16U);

    if (block_bits != geometry.config().block_bits ||
        chunk_width != geometry.config().chunk_width_blocks ||
        chunk_height != geometry.config().chunk_height_blocks) {
        throw std::runtime_error("geometry mismatch");
    }

    const auto chunk_x = static_cast<std::int64_t>(ReadLe64(bytes, 20U));
    const auto chunk_y = static_cast<std::int64_t>(ReadLe64(bytes, 28U));
    if (chunk_x != expected_chunk_coord.x || chunk_y != expected_chunk_coord.y) {
        throw std::runtime_error("chunk coordinate mismatch");
    }

    const std::uint32_t payload_size = ReadLe32(bytes, 36U);
    const std::uint32_t payload_crc = ReadLe32(bytes, 40U);

    if (payload_size != geometry.ChunkPayloadBytes()) {
        throw std::runtime_error("payload size mismatch");
    }

    ChunkStateImage image;
    if (version == kChunkFileVersionLegacy) {
        if (bytes.size() != kChunkHeaderSize + payload_size) {
            throw std::runtime_error("incomplete payload");
        }

        image.payload.assign(
            bytes.begin() + static_cast<std::ptrdiff_t>(kChunkHeaderSize),
            bytes.end());
        if (Crc32(image.payload) != payload_crc) {
            throw std::runtime_error("payload checksum mismatch");
        }
        image.presence_bitmap = FullPresenceBitmap(geometry);
        return image;
    }

    const std::size_t presence_bytes = ChunkPresenceBitmapBytes(geometry);
    if (bytes.size() != kChunkHeaderSize + payload_size + presence_bytes) {
        throw std::runtime_error("incomplete payload");
    }

    std::vector<std::uint8_t> state(
        bytes.begin() + static_cast<std::ptrdiff_t>(kChunkHeaderSize),
        bytes.end());
    if (Crc32(state) != payload_crc) {
        throw std::runtime_error("payload checksum mismatch");
    }

    SplitChunkStateBytes(geometry, state, &image.payload, &image.presence_bitmap);
    return image;
}


std::vector<std::uint8_t> LoadFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open file: " + path.string());
    }

    input.seekg(0, std::ios::end);
    const std::streamoff size = input.tellg();
    if (size < 0) {
        throw std::runtime_error("failed to read file size: " + path.string());
    }

    input.seekg(0, std::ios::beg);
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(size));
    if (!bytes.empty()) {
        input.read(reinterpret_cast<char*>(bytes.data()), size);
    }
    if (!input.good() && !input.eof()) {
        throw std::runtime_error("failed to read file: " + path.string());
    }

    return bytes;
}


DurabilityMode ParseDurabilityMode(std::string_view text) {
    if (text == "relaxed") {
        return DurabilityMode::kRelaxed;
    }
    if (text == "fsync-wal") {
        return DurabilityMode::kFsyncWal;
    }
    if (text == "fsync-checkpoint") {
        return DurabilityMode::kFsyncCheckpoint;
    }
    throw std::invalid_argument(
        "invalid durability mode: " + std::string(text) +
        " (expected relaxed|fsync-wal|fsync-checkpoint)");
}

const char* DurabilityModeName(DurabilityMode mode) noexcept {
    switch (mode) {
        case DurabilityMode::kRelaxed:
            return "relaxed";
        case DurabilityMode::kFsyncWal:
            return "fsync-wal";
        case DurabilityMode::kFsyncCheckpoint:
            return "fsync-checkpoint";
    }
    return "unknown";
}

const char* AccessModeName(AccessMode mode) noexcept {
    switch (mode) {
        case AccessMode::kReadWrite:
            return "read-write";
        case AccessMode::kReadOnly:
            return "read-only";
    }
    return "unknown";
}

StorageLayoutMode ParseStorageLayoutMode(std::string_view text) {
    if (text == "fs_split_v1") {
        return StorageLayoutMode::kFsSplitV1;
    }
    if (text == "fs_region_v1") {
        return StorageLayoutMode::kFsRegionV1Experimental;
    }
    throw std::invalid_argument(
        "invalid storage layout mode: " + std::string(text) +
        " (expected fs_split_v1|fs_region_v1)");
}

const char* StorageLayoutModeName(StorageLayoutMode mode) noexcept {
    switch (mode) {
        case StorageLayoutMode::kFsSplitV1:
            return "fs_split_v1";
        case StorageLayoutMode::kFsRegionV1Experimental:
            return "fs_region_v1";
    }
    return "unknown";
}

ChunkStore::ChunkStore(StoreConfig config)
    : geometry_(config.geometry),
      data_dir_(std::move(config.data_dir)),
      durability_mode_(config.durability_mode),
      access_mode_(config.access_mode),
      storage_layout_mode_(config.storage_layout_mode),
      experimental_region_span_chunks_(config.experimental_region_span_chunks),
      checkpoint_update_interval_(config.checkpoint_update_interval),
      checkpoint_wal_bytes_(config.checkpoint_wal_bytes),
      wal_group_commit_updates_(config.wal_group_commit_updates),
      max_loaded_chunks_(config.max_loaded_chunks),
      max_open_wal_streams_(config.max_open_wal_streams) {
    if (data_dir_.empty()) {
        throw std::invalid_argument("data_dir must not be empty");
    }
    if (checkpoint_update_interval_ == 0) {
        throw std::invalid_argument("checkpoint_update_interval must be > 0");
    }
    if (checkpoint_wal_bytes_ == 0) {
        throw std::invalid_argument("checkpoint_wal_bytes must be > 0");
    }
    if (wal_group_commit_updates_ == 0) {
        throw std::invalid_argument("wal_group_commit_updates must be > 0");
    }
    if (max_loaded_chunks_ == 0) {
        throw std::invalid_argument("max_loaded_chunks must be > 0");
    }
    if (max_open_wal_streams_ == 0) {
        throw std::invalid_argument("max_open_wal_streams must be > 0");
    }
    if (experimental_region_span_chunks_ == 0) {
        throw std::invalid_argument("experimental_region_span_chunks must be > 0");
    }
    if (experimental_region_span_chunks_ > 64) {
        throw std::invalid_argument("experimental_region_span_chunks must be <= 64");
    }

#ifndef _WIN32
    {
        struct rlimit limit {};
        if (getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur != RLIM_INFINITY) {
            const std::size_t soft_limit = static_cast<std::size_t>(limit.rlim_cur);
            const std::size_t clamped =
                soft_limit > kWalOpenStreamsFdReserve
                    ? (soft_limit - kWalOpenStreamsFdReserve)
                    : 1U;
            if (max_open_wal_streams_ > clamped) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kStore,
                    "max_open_wal_streams clamped by RLIMIT_NOFILE reserve",
                    {
                        {"configured", std::to_string(max_open_wal_streams_)},
                        {"effective", std::to_string(clamped)},
                        {"rlimit_nofile_soft", std::to_string(soft_limit)},
                        {"reserve", std::to_string(kWalOpenStreamsFdReserve)},
                    });
                max_open_wal_streams_ = clamped;
            }
        }
    }
#else
    {
        // Windows has no RLIMIT_NOFILE, but the C runtime caps the number of
        // simultaneously open stdio streams (_getmaxstdio, default 512). The
        // WAL stream pool can keep up to max_open_wal_streams files open, so
        // without this an open-heavy workload hits EMFILE ("Too many open
        // files"). Raise the CRT limit toward its maximum, then clamp the WAL
        // pool to the effective limit minus a reserve for other handles.
        constexpr int kWindowsStdioTarget = 8192;  // CRT hard maximum
        if (_getmaxstdio() < kWindowsStdioTarget) {
            (void)_setmaxstdio(kWindowsStdioTarget);
        }
        const int effective_stdio = _getmaxstdio();
        if (effective_stdio > 0) {
            const std::size_t budget = static_cast<std::size_t>(effective_stdio);
            const std::size_t clamped =
                budget > kWalOpenStreamsFdReserve
                    ? (budget - kWalOpenStreamsFdReserve)
                    : 1U;
            if (max_open_wal_streams_ > clamped) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kStore,
                    "max_open_wal_streams clamped by Windows CRT stdio limit",
                    {
                        {"configured", std::to_string(max_open_wal_streams_)},
                        {"effective", std::to_string(clamped)},
                        {"crt_maxstdio", std::to_string(effective_stdio)},
                        {"reserve", std::to_string(kWalOpenStreamsFdReserve)},
                    });
                max_open_wal_streams_ = clamped;
            }
        }
    }
#endif

    const auto recovery_start = std::chrono::steady_clock::now();
    const auto startup_scan = ScanStartupRecovery(data_dir_);

    std::filesystem::create_directories(data_dir_);
    AcquireProcessLock(config.allow_multiple_processes);

    const auto recovery_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - recovery_start);
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kRecovery,
        "startup recovery summary",
        {
            {"checkpoint_files", std::to_string(startup_scan.checkpoint_files)},
            {"region_files", std::to_string(startup_scan.region_files)},
            {"wal_files", std::to_string(startup_scan.wal_files)},
            {"scan_capped", startup_scan.scan_capped ? "true" : "false"},
            {"replay_mode", "lazy-on-load"},
            {"elapsed_ms", std::to_string(recovery_elapsed_ms.count())},
        });
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kStore,
        "store initialized",
        {
            {"data_dir", data_dir_.string()},
            {"durability_mode", DurabilityModeName(durability_mode_)},
            {"access_mode", AccessModeName(access_mode_)},
            {"storage_layout_mode", StorageLayoutModeName(storage_layout_mode_)},
            {"max_loaded_chunks", std::to_string(max_loaded_chunks_)},
            {"max_open_wal_streams", std::to_string(max_open_wal_streams_)},
        });
}

ChunkStore::~ChunkStore() {
    FlushAllPendingWalBatches();
    ReleaseProcessLock();
}

bool ChunkStore::BlockExists(std::int64_t block_x, std::int64_t block_y) {
    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return BlockPresent(regular_chunk->presence_bitmap, block_index);
}

std::string ChunkStore::GetBlockBits(std::int64_t block_x, std::int64_t block_y) {
    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const std::size_t bit_offset = block_index * geometry_.config().block_bits;

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    if (!BlockPresent(regular_chunk->presence_bitmap, block_index)) {
        return std::string(geometry_.config().block_bits, '0');
    }
    return BitCodec::ExtractBits(regular_chunk->payload, bit_offset, geometry_.config().block_bits);
}

void ChunkStore::SetBlockBits(std::int64_t block_x, std::int64_t block_y, std::string_view bits) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
    if (bits.size() != geometry_.config().block_bits) {
        throw std::invalid_argument("bit string length does not match configured block_bits");
    }
    if (!BitCodec::IsBitString(bits)) {
        throw std::invalid_argument("bit string must contain only 0 and 1");
    }

    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const std::size_t bit_offset = block_index * geometry_.config().block_bits;

    const std::size_t begin_byte = bit_offset / 8U;
    const std::size_t end_byte = (bit_offset + bits.size() - 1U) / 8U;
    const std::size_t touched_bytes = end_byte - begin_byte + 1U;
    const std::size_t presence_byte_index = block_index / 8U;

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    auto& previous_bytes = regular_chunk->scratch_before;
    previous_bytes.resize(touched_bytes);
    std::copy_n(
        regular_chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin_byte),
        static_cast<std::ptrdiff_t>(touched_bytes),
        previous_bytes.begin());
    const std::uint8_t previous_presence_byte = regular_chunk->presence_bitmap[presence_byte_index];

    BitCodec::WriteBits(regular_chunk->payload, bit_offset, bits);
    SetBlockPresent(&regular_chunk->presence_bitmap, block_index, true);

    bool payload_changed = false;
    for (std::size_t i = 0; i < touched_bytes; ++i) {
        if (previous_bytes[i] != regular_chunk->payload[begin_byte + i]) {
            payload_changed = true;
            break;
        }
    }
    const bool presence_changed =
        previous_presence_byte != regular_chunk->presence_bitmap[presence_byte_index];

    if (!payload_changed && !presence_changed) {
        return;
    }

    try {
        std::size_t appended_bytes = 0;
        if (payload_changed) {
            AppendWalDelta(
                chunk_coord,
                regular_chunk,
                static_cast<std::uint32_t>(begin_byte),
                regular_chunk->payload.data() + begin_byte,
                touched_bytes,
                &appended_bytes);
        }
        if (presence_changed) {
            std::size_t presence_record_bytes = 0;
            AppendWalDelta(
                chunk_coord,
                regular_chunk,
                static_cast<std::uint32_t>(geometry_.ChunkPayloadBytes() + presence_byte_index),
                &regular_chunk->presence_bitmap[presence_byte_index],
                1U,
                &presence_record_bytes);
            appended_bytes += presence_record_bytes;
        }

        regular_chunk->pending_updates += 1;
        regular_chunk->wal_bytes += appended_bytes;
        MaybeCheckpointChunk(chunk_coord, regular_chunk);
    } catch (...) {
        std::copy(
            previous_bytes.begin(),
            previous_bytes.end(),
            regular_chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin_byte));
        regular_chunk->presence_bitmap[presence_byte_index] = previous_presence_byte;
        throw;
    }
}

void ChunkStore::UnsetBlock(std::int64_t block_x, std::int64_t block_y) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }

    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const std::size_t bit_offset = block_index * geometry_.config().block_bits;
    const std::size_t begin_byte = bit_offset / 8U;
    const std::size_t end_byte = (bit_offset + geometry_.config().block_bits - 1U) / 8U;
    const std::size_t touched_bytes = end_byte - begin_byte + 1U;
    const std::size_t presence_byte_index = block_index / 8U;

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    auto& previous_bytes = regular_chunk->scratch_before;
    previous_bytes.resize(touched_bytes);
    std::copy_n(
        regular_chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin_byte),
        static_cast<std::ptrdiff_t>(touched_bytes),
        previous_bytes.begin());
    const std::uint8_t previous_presence_byte = regular_chunk->presence_bitmap[presence_byte_index];

    BitCodec::WriteBits(
        regular_chunk->payload,
        bit_offset,
        std::string(geometry_.config().block_bits, '0'));
    SetBlockPresent(&regular_chunk->presence_bitmap, block_index, false);

    bool payload_changed = false;
    for (std::size_t i = 0; i < touched_bytes; ++i) {
        if (previous_bytes[i] != regular_chunk->payload[begin_byte + i]) {
            payload_changed = true;
            break;
        }
    }
    const bool presence_changed =
        previous_presence_byte != regular_chunk->presence_bitmap[presence_byte_index];

    if (!payload_changed && !presence_changed) {
        return;
    }

    try {
        std::size_t appended_bytes = 0;
        if (payload_changed) {
            AppendWalDelta(
                chunk_coord,
                regular_chunk,
                static_cast<std::uint32_t>(begin_byte),
                regular_chunk->payload.data() + begin_byte,
                touched_bytes,
                &appended_bytes);
        }
        if (presence_changed) {
            std::size_t presence_record_bytes = 0;
            AppendWalDelta(
                chunk_coord,
                regular_chunk,
                static_cast<std::uint32_t>(geometry_.ChunkPayloadBytes() + presence_byte_index),
                &regular_chunk->presence_bitmap[presence_byte_index],
                1U,
                &presence_record_bytes);
            appended_bytes += presence_record_bytes;
        }

        regular_chunk->pending_updates += 1;
        regular_chunk->wal_bytes += appended_bytes;
        MaybeCheckpointChunk(chunk_coord, regular_chunk);
    } catch (...) {
        std::copy(
            previous_bytes.begin(),
            previous_bytes.end(),
            regular_chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin_byte));
        regular_chunk->presence_bitmap[presence_byte_index] = previous_presence_byte;
        throw;
    }
}

bool ChunkStore::ChunkExists(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return ChunkPresent(regular_chunk->presence_bitmap);
}

void ChunkStore::SetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y, std::string_view bits) {
    SetChunkStateBits(
        chunk_x,
        chunk_y,
        bits,
        std::string(geometry_.ChunkBlockCount(), '1'));
}

void ChunkStore::SetChunkStateBits(
    std::int64_t chunk_x,
    std::int64_t chunk_y,
    std::string_view payload_bits,
    std::string_view presence_bits) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
    if (payload_bits.size() != geometry_.ChunkPayloadBits()) {
        throw std::invalid_argument("payload bit string length does not match configured chunk size");
    }
    if (presence_bits.size() != geometry_.ChunkBlockCount()) {
        throw std::invalid_argument("presence bit string length does not match configured chunk block count");
    }
    if (!BitCodec::IsBitString(payload_bits)) {
        throw std::invalid_argument("payload bit string must contain only 0 and 1");
    }
    if (!BitCodec::IsBitString(presence_bits)) {
        throw std::invalid_argument("presence bit string must contain only 0 and 1");
    }

    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    auto previous_payload = regular_chunk->payload;
    auto previous_presence = regular_chunk->presence_bitmap;

    regular_chunk->payload = EmptyPayload();
    BitCodec::WriteBits(regular_chunk->payload, 0, payload_bits);
    regular_chunk->presence_bitmap = EmptyPresenceBitmap();
    BitCodec::WriteBits(regular_chunk->presence_bitmap, 0, presence_bits);
    CanonicalizeAbsentBlocks(geometry_, regular_chunk->presence_bitmap, &regular_chunk->payload);

    const bool payload_changed = regular_chunk->payload != previous_payload;
    const bool presence_changed = regular_chunk->presence_bitmap != previous_presence;
    if (!payload_changed && !presence_changed) {
        return;
    }

    try {
        std::size_t appended_bytes = 0;
        std::size_t appended_record_count = 0;
        if (payload_changed) {
            std::size_t payload_record_bytes = 0;
            std::size_t payload_record_count = 0;
            AppendWalDeltaSpanToBatch(
                &regular_chunk->wal_batch,
                0U,
                regular_chunk->payload.data(),
                regular_chunk->payload.size(),
                &payload_record_bytes,
                &payload_record_count);
            appended_bytes += payload_record_bytes;
            appended_record_count += payload_record_count;
        }
        if (presence_changed) {
            std::size_t presence_record_bytes = 0;
            std::size_t presence_record_count = 0;
            AppendWalDeltaSpanToBatch(
                &regular_chunk->wal_batch,
                static_cast<std::uint32_t>(geometry_.ChunkPayloadBytes()),
                regular_chunk->presence_bitmap.data(),
                regular_chunk->presence_bitmap.size(),
                &presence_record_bytes,
                &presence_record_count);
            appended_bytes += presence_record_bytes;
            appended_record_count += presence_record_count;
        }

        regular_chunk->pending_wal_flush_updates += appended_record_count;
        const bool sync_required = durability_mode_ != DurabilityMode::kRelaxed;
        if (sync_required || regular_chunk->pending_wal_flush_updates >= wal_group_commit_updates_) {
            FlushWalBatch(chunk_coord, regular_chunk, sync_required);
        }

        regular_chunk->pending_updates += 1;
        regular_chunk->wal_bytes += appended_bytes;
        MaybeCheckpointChunk(chunk_coord, regular_chunk);
    } catch (...) {
        regular_chunk->payload = std::move(previous_payload);
        regular_chunk->presence_bitmap = std::move(previous_presence);
        throw;
    }
}

std::string ChunkStore::GetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return BitCodec::ExtractBits(regular_chunk->payload, 0, geometry_.ChunkPayloadBits());
}

std::vector<std::uint8_t> ChunkStore::GetChunkPayloadBytes(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return regular_chunk->payload;
}

std::string ChunkStore::GetChunkStateBits(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return BitCodec::ExtractBits(regular_chunk->payload, 0, geometry_.ChunkPayloadBits()) + "|" +
           PresenceBitsText(geometry_, regular_chunk->presence_bitmap);
}

std::vector<std::uint8_t> ChunkStore::GetChunkStateBytes(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return BuildChunkStateBytes(geometry_, regular_chunk->payload, regular_chunk->presence_bitmap);
}

std::size_t ChunkStore::ApproxLoadedChunkCount() const {
    std::size_t loaded = 0;
    std::lock_guard global_lock(large_chunks_mutex_);
    for (const auto& [_, large_chunk] : large_chunks_) {
        std::lock_guard chunk_lock(large_chunk->mutex);
        loaded += large_chunk->chunks.size();
    }
    return loaded;
}

StoreRuntimeStats ChunkStore::RuntimeStats() const noexcept {
    const auto forced_with_data = stats_eviction_forced_wal_flushes_with_data_.load(std::memory_order_relaxed);
    const auto forced_empty = stats_eviction_forced_wal_flushes_empty_batch_.load(std::memory_order_relaxed);
    return StoreRuntimeStats{
        .evictions = stats_evictions_.load(std::memory_order_relaxed),
        .checkpoints = stats_checkpoints_.load(std::memory_order_relaxed),
        .wal_batch_flushes = stats_wal_batch_flushes_.load(std::memory_order_relaxed),
        .unique_loaded_chunks = stats_unique_loaded_chunks_.load(std::memory_order_relaxed),
        .open_wal_streams = stats_open_wal_streams_current_.load(std::memory_order_relaxed),
        .eviction_snapshot_builds = stats_eviction_snapshot_builds_.load(std::memory_order_relaxed),
        .eviction_probes = stats_eviction_probes_.load(std::memory_order_relaxed),
        .eviction_no_progress_cycles = stats_eviction_no_progress_cycles_.load(std::memory_order_relaxed),
        .eviction_forced_wal_flushes = forced_with_data + forced_empty,
        .eviction_forced_wal_flushes_with_data = forced_with_data,
        .eviction_forced_wal_flushes_empty_batch = forced_empty,
    };
}

std::uint64_t ChunkStore::WalOpenCountForTests() const noexcept {
    return stats_wal_open_count_.load(std::memory_order_relaxed);
}

std::uint64_t ChunkStore::WalParentPrepareCountForTests() const noexcept {
    return stats_wal_parent_prepare_calls_.load(std::memory_order_relaxed);
}

std::uint64_t ChunkStore::OpenWalStreamCountForTests() const noexcept {
    return stats_open_wal_streams_current_.load(std::memory_order_relaxed);
}

std::shared_ptr<ChunkStore::LargeChunk> ChunkStore::GetOrCreateLargeChunk(const LargeChunkCoord& large_coord) {
    std::lock_guard lock(large_chunks_mutex_);
    auto it = large_chunks_.find(large_coord);
    if (it != large_chunks_.end()) {
        return it->second;
    }

    auto created = std::make_shared<LargeChunk>();
    large_chunks_.emplace(large_coord, created);
    eviction_large_chunk_ring_.push_back(large_coord);
    return created;
}

std::shared_ptr<ChunkStore::RegularChunk> ChunkStore::GetOrLoadRegularChunk(const ChunkCoord& chunk_coord) {
    const LargeChunkCoord large_coord = geometry_.ChunkToLarge(chunk_coord);
    const auto large_chunk = GetOrCreateLargeChunk(large_coord);

    bool inserted = false;
    std::shared_ptr<RegularChunk> selected;
    {
        std::lock_guard lock(large_chunk->mutex);
        auto it = large_chunk->chunks.find(chunk_coord);
        if (it != large_chunk->chunks.end()) {
            selected = it->second;
        } else {
            const auto loaded = LoadChunkPayload(chunk_coord);
            selected = std::make_shared<RegularChunk>(loaded.payload, loaded.presence_bitmap);
            selected->wal_bytes = loaded.wal_bytes;
            selected->checkpoint_due_armed = loaded.wal_bytes >= checkpoint_wal_bytes_;
            selected->deferred_wal_compaction = loaded.deferred_wal_compaction;
            selected->wal_header_written = loaded.wal_header_written;
            selected->wal_path = loaded.wal_path;
            large_chunk->chunks.emplace(chunk_coord, selected);
            inserted = true;
        }
    }

    TouchChunk(selected);
    if (inserted) {
        RegisterEvictionCandidate(large_coord, chunk_coord);
        const auto loaded_now = loaded_chunk_count_.fetch_add(1, std::memory_order_relaxed) + 1;
        stats_unique_loaded_chunks_.fetch_add(1, std::memory_order_relaxed);
        if (loaded_now > max_loaded_chunks_) {
            MaybeEvictChunks();
        }
    }
    return selected;
}

std::vector<std::uint8_t> ChunkStore::EmptyPayload() const {
    return std::vector<std::uint8_t>(geometry_.ChunkPayloadBytes(), 0U);
}

std::vector<std::uint8_t> ChunkStore::EmptyPresenceBitmap() const {
    return std::vector<std::uint8_t>(ChunkPresenceBitmapBytes(geometry_), 0U);
}

ChunkStore::LoadedChunkPayload ChunkStore::LoadChunkPayload(const ChunkCoord& chunk_coord) {
    const auto wal_path = LayoutWalPath(data_dir_, geometry_, chunk_coord, storage_layout_mode_);
    const auto data_path =
        (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1)
            ? ChunkDataPath(data_dir_, geometry_, chunk_coord)
            : RegionDataPath(data_dir_, chunk_coord, experimental_region_span_chunks_);
    const bool writable = access_mode_ != AccessMode::kReadOnly;
    LoadedChunkPayload loaded{
        .payload = EmptyPayload(),
        .presence_bitmap = EmptyPresenceBitmap(),
        .wal_bytes = 0,
        .deferred_wal_compaction = false,
        .wal_header_written = false,
        .wal_path = {},
    };
    if (writable) {
        CleanupAtomicTmpArtifacts(data_path);
    }
    if (std::filesystem::exists(data_path)) {
        try {
            if (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1) {
                const auto data_bytes = LoadFile(data_path);
                auto image = ParseChunkImage(data_bytes, geometry_, chunk_coord);
                loaded.payload = std::move(image.payload);
                loaded.presence_bitmap = std::move(image.presence_bitmap);
            } else {
                const auto addr = ComputeRegionChunkAddress(chunk_coord, experimental_region_span_chunks_);
                std::lock_guard region_lock(RegionIoMutex());
                const auto region_bytes = LoadFile(data_path);
                const auto region = ParseRegionFileImage(region_bytes, geometry_, addr, experimental_region_span_chunks_);
                const auto slot_state = ExtractRegionSlotState(region, addr.slot_index);
                if (!slot_state.empty()) {
                    SplitChunkStateBytes(
                        geometry_,
                        slot_state,
                        &loaded.payload,
                        &loaded.presence_bitmap);
                }
            }
        } catch (...) {
            // The image can be replaced concurrently by atomic checkpoint rename.
            // If it disappeared during open, fall back to empty payload.
            if (std::filesystem::exists(data_path)) {
                throw;
            }
            loaded.payload = EmptyPayload();
            loaded.presence_bitmap = EmptyPresenceBitmap();
        }
    }

    if (std::filesystem::exists(wal_path)) {
        std::vector<std::uint8_t> wal_bytes;
        try {
            wal_bytes = LoadFile(wal_path);
        } catch (...) {
            // WAL can be removed concurrently by checkpoint cleanup.
            // If it no longer exists, treat as already checkpointed.
            if (std::filesystem::exists(wal_path)) {
                throw;
            }
            return loaded;
        }

        const auto replay = ReplayWal(
            wal_bytes,
            geometry_,
            chunk_coord,
            &loaded.payload,
            &loaded.presence_bitmap);
        if (!replay.replayable) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kRecovery,
                "WAL skipped during chunk load",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"reason", replay.stop_reason.empty() ? "non_replayable" : replay.stop_reason},
                });
        } else if (replay.tail_truncated_or_corrupt) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kRecovery,
                "WAL replay stopped on tail corruption/truncation",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"reason", replay.stop_reason.empty() ? "tail_corruption" : replay.stop_reason},
                    {"applied_records", std::to_string(replay.applied_records)},
                });
        }
        if (writable) {
            loaded.deferred_wal_compaction = true;
            loaded.wal_bytes = wal_bytes.size();
            loaded.wal_header_written = true;
            loaded.wal_path = wal_path;
        }
    }

    return loaded;
}

void ChunkStore::TouchChunk(const std::shared_ptr<RegularChunk>& chunk) noexcept {
    const std::uint64_t tick = access_clock_.fetch_add(1, std::memory_order_relaxed) + 1U;
    chunk->last_access_tick.store(tick, std::memory_order_relaxed);
}




}  // namespace chunkdb
