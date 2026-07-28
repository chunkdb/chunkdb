#include "chunkdb/chunk_store.hpp"

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "eviction.hpp"
#include "process_lock.hpp"
#include "wal_replay.hpp"
#include "wal_stream_pool.hpp"
#include "wal_writer.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
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
#include "chunkdb/zrle.hpp"

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

namespace {
std::size_t RegionPresentBitmapBytes(std::uint32_t slot_count) {
    return (static_cast<std::size_t>(slot_count) + 7U) / 8U;
}
}  // namespace

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
    // Enforce the slot bound here rather than relying on the trailing
    // SetRegionSlotPresent call: that check runs after the heap writes below, so
    // an out-of-range index would corrupt memory before ever being rejected.
    if (slot_index >= image->slot_count) {
        throw std::runtime_error("region slot index out of range");
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
    if (version != kChunkFileVersion && version != kChunkFileVersionLegacy &&
        version != kChunkFileVersionCompressed) {
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

    if (version == kChunkFileVersionCompressed) {
        // The CRC covers the canonical uncompressed state, so corruption in
        // the compressed blob is caught either by the bounded decoder or by
        // the checksum of its output.
        const auto state = ZrleDecompress(
            bytes.data() + kChunkHeaderSize,
            bytes.size() - kChunkHeaderSize,
            payload_size + presence_bytes);
        if (Crc32(state) != payload_crc) {
            throw std::runtime_error("payload checksum mismatch");
        }
        SplitChunkStateBytes(geometry, state, &image.payload, &image.presence_bitmap);
        return image;
    }

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

}  // namespace chunkdb
