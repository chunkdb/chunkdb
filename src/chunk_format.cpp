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
#include "feature_flags.hpp"

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

void MaskUnusedPayloadBits(const Geometry& geometry, std::vector<std::uint8_t>* payload) {
    if (payload == nullptr || payload->empty()) {
        return;
    }

    const std::size_t used_bits = geometry.ChunkPayloadBits();
    const std::size_t trailing_bits = payload->size() * 8U - used_bits;
    if (trailing_bits == 0) {
        return;
    }

    const std::uint8_t mask = static_cast<std::uint8_t>(0xFFU >> trailing_bits);
    payload->back() &= mask;
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

namespace {

struct ImageSectionEntry {
    std::uint16_t type = 0;
    std::uint16_t flags = 0;
    std::uint32_t stored_size = 0;
    std::uint32_t raw_size = 0;
    std::uint32_t crc = 0;
};

[[nodiscard]] std::uint32_t KnownSectionRawSize(const Geometry& geometry, std::uint16_t type) {
    switch (type) {
        case kImageSectionPayload:
            return static_cast<std::uint32_t>(geometry.ChunkPayloadBytes());
        case kImageSectionPresence:
            return static_cast<std::uint32_t>(ChunkPresenceBitmapBytes(geometry));
        default:
            return 0;
    }
}

void AppendSection(
    std::vector<std::uint8_t>* directory,
    std::vector<std::uint8_t>* bodies,
    std::uint16_t type,
    const std::vector<std::uint8_t>& raw,
    CheckpointCompression compression) {
    const bool compressed = compression == CheckpointCompression::kZrle;
    const auto stored = compressed ? ZrleCompress(raw) : raw;
    WriteLe16(*directory, type);
    WriteLe16(*directory, compressed ? kImageSectionFlagZrle : 0U);
    WriteLe32(*directory, static_cast<std::uint32_t>(stored.size()));
    WriteLe32(*directory, static_cast<std::uint32_t>(raw.size()));
    WriteLe32(*directory, Crc32(raw));
    bodies->insert(bodies->end(), stored.begin(), stored.end());
}

}  // namespace

std::vector<std::uint8_t> SerializeChunkImage(
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    const std::vector<std::uint8_t>& payload,
    const std::vector<std::uint8_t>& presence_bitmap,
    CheckpointCompression compression,
    std::uint64_t revision,
    std::uint64_t commit_time_ms,
    const StoreId& store_id,
    const ChunkExtra* extra) {
    if (payload.size() != geometry.ChunkPayloadBytes() ||
        presence_bitmap.size() != ChunkPresenceBitmapBytes(geometry)) {
        throw std::invalid_argument("chunk state size does not match geometry");
    }
    if (revision == 0U) {
        throw std::invalid_argument("chunk image revision must not be zero");
    }
    const bool has_extra = extra != nullptr && !extra->empty();
    std::vector<std::uint8_t> directory;
    std::vector<std::uint8_t> bodies;
    AppendSection(&directory, &bodies, kImageSectionPayload, payload, compression);
    AppendSection(&directory, &bodies, kImageSectionPresence, presence_bitmap, compression);
    if (has_extra) {
        AppendSection(&directory, &bodies, kImageSectionExtra, extra->Encode(), compression);
    }

    std::vector<std::uint8_t> bytes;
    bytes.reserve(kImageFixedHeaderSize + directory.size() + 4U + bodies.size());
    bytes.insert(bytes.end(), kImageMagic, kImageMagic + kImageMagicSize);
    WriteLe16(bytes, kImageFormatVersion);
    WriteLe16(bytes, has_extra ? 3U : 2U);
    WriteLe32(bytes, 0U);
    WriteLe32(bytes, has_extra ? kFeatureExtraData : 0U);
    WriteLe32(bytes, 0U);
    bytes.insert(bytes.end(), store_id.begin(), store_id.end());
    WriteLe64(bytes, static_cast<std::uint64_t>(chunk_coord.x));
    WriteLe64(bytes, static_cast<std::uint64_t>(chunk_coord.y));
    WriteLe64(bytes, revision);
    WriteLe64(bytes, commit_time_ms);
    bytes.insert(bytes.end(), directory.begin(), directory.end());
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    bytes.insert(bytes.end(), bodies.begin(), bodies.end());
    return bytes;
}

std::vector<std::uint8_t> BuildWalHeader(
    const ChunkCoord& chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& features) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kWalHeaderSize);
    bytes.insert(bytes.end(), kWalMagic, kWalMagic + kWalMagicSize);
    WriteLe16(bytes, kWalFormatVersion);
    WriteLe16(bytes, 0U);
    WriteLe32(bytes, features.incompat);
    WriteLe32(bytes, features.ro_compat);
    WriteLe32(bytes, features.compat);
    bytes.insert(bytes.end(), store_id.begin(), store_id.end());
    WriteLe64(bytes, static_cast<std::uint64_t>(chunk_coord.x));
    WriteLe64(bytes, static_cast<std::uint64_t>(chunk_coord.y));
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    return bytes;
}

ChunkStateImage ParseChunkImage(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& store_features) {
    if (bytes.size() < kImageFixedHeaderSize + 4U) {
        throw std::runtime_error("chunk image too small");
    }
    if (std::memcmp(bytes.data(), kImageMagic, kImageMagicSize) != 0) {
        throw std::runtime_error("not a 2.0 chunk image (bad magic)");
    }
    const std::uint16_t version = ReadLe16(bytes, 8U);
    if (version != kImageFormatVersion) {
        throw std::runtime_error("unsupported chunk image version " + std::to_string(version));
    }
    const std::uint16_t section_count = ReadLe16(bytes, 10U);
    if (section_count > kImageMaxSections) {
        throw std::runtime_error("chunk image has too many sections");
    }
    const std::size_t directory_end =
        kImageFixedHeaderSize + static_cast<std::size_t>(section_count) * kImageSectionEntrySize;
    if (bytes.size() < directory_end + 4U) {
        throw std::runtime_error("chunk image too small for its section directory");
    }
    if (ReadLe32(bytes, directory_end) != Crc32(bytes.data(), directory_end)) {
        throw std::runtime_error("chunk image header checksum mismatch");
    }

    ChunkStateImage image;
    image.features = FeatureFlags{
        .incompat = ReadLe32(bytes, 12U),
        .ro_compat = ReadLe32(bytes, 16U),
        .compat = ReadLe32(bytes, 20U),
    };
    if (!IsSubsetOf(image.features, store_features)) {
        throw std::runtime_error(
            "chunk image uses features the store does not (" +
            DescribeFeatures(image.features) + ")");
    }
    if (!std::equal(store_id.begin(), store_id.end(), bytes.begin() + 24)) {
        throw std::runtime_error("chunk image belongs to another store");
    }
    const auto chunk_x = static_cast<std::int64_t>(ReadLe64(bytes, 40U));
    const auto chunk_y = static_cast<std::int64_t>(ReadLe64(bytes, 48U));
    if (chunk_x != expected_chunk_coord.x || chunk_y != expected_chunk_coord.y) {
        throw std::runtime_error("chunk coordinate mismatch");
    }
    image.revision = ReadLe64(bytes, 56U);
    if (image.revision == 0U) {
        throw std::runtime_error("chunk image revision is zero");
    }
    image.commit_time_ms = ReadLe64(bytes, 64U);

    // An image is rewritten whole at every checkpoint with the features it
    // uses, so its own flags name every feature its sections belong to.
    const bool may_skip_unknown = MaySkipUnknownTypes(image.features);
    std::size_t body_at = directory_end + 4U;
    std::uint16_t previous_type = 0;
    bool have_payload = false;
    bool have_presence = false;
    std::vector<std::uint8_t> extra_section;
    bool have_extra = false;
    for (std::uint16_t i = 0; i < section_count; ++i) {
        const std::size_t at = kImageFixedHeaderSize + static_cast<std::size_t>(i) * kImageSectionEntrySize;
        const ImageSectionEntry entry{
            .type = ReadLe16(bytes, at),
            .flags = ReadLe16(bytes, at + 2U),
            .stored_size = ReadLe32(bytes, at + 4U),
            .raw_size = ReadLe32(bytes, at + 8U),
            .crc = ReadLe32(bytes, at + 12U),
        };
        const std::string name = "chunk image section " + std::to_string(entry.type);
        if (i > 0 && entry.type <= previous_type) {
            throw std::runtime_error("chunk image sections are not in ascending type order");
        }
        previous_type = entry.type;
        if ((entry.flags & ~kImageSectionFlagZrle) != 0U) {
            throw std::runtime_error(name + " has unknown flags");
        }
        const bool compressed = (entry.flags & kImageSectionFlagZrle) != 0U;
        if (!compressed && entry.stored_size != entry.raw_size) {
            throw std::runtime_error(name + " stored and raw sizes differ");
        }
        if (bytes.size() - body_at < entry.stored_size) {
            throw std::runtime_error(name + " extends past the end of the image");
        }
        const bool fixed_size =
            entry.type == kImageSectionPayload || entry.type == kImageSectionPresence;
        const bool known = fixed_size || entry.type == kImageSectionExtra;
        if (!known && !may_skip_unknown) {
            throw std::runtime_error("unknown " + name);
        }
        if (fixed_size && entry.raw_size != KnownSectionRawSize(geometry, entry.type)) {
            throw std::runtime_error(name + " has the wrong size for the store geometry");
        }
        if (entry.type == kImageSectionExtra) {
            if (!HasExtraData(image.features)) {
                throw std::runtime_error(name + " (extra data) without the image's extra-data feature");
            }
            // Bounded before decompression allocates it.
            if (entry.raw_size == 0U || entry.raw_size > kExtraMaxChunkBytesLimit) {
                throw std::runtime_error(
                    name + " (extra data) has size " + std::to_string(entry.raw_size));
            }
        }
        std::vector<std::uint8_t> raw;
        if (compressed) {
            raw = ZrleDecompress(bytes.data() + body_at, entry.stored_size, entry.raw_size);
        } else {
            raw.assign(
                bytes.begin() + static_cast<std::ptrdiff_t>(body_at),
                bytes.begin() + static_cast<std::ptrdiff_t>(body_at + entry.stored_size));
        }
        if (Crc32(raw) != entry.crc) {
            throw std::runtime_error(name + " checksum mismatch");
        }
        if (entry.type == kImageSectionPayload) {
            image.payload = std::move(raw);
            have_payload = true;
        } else if (entry.type == kImageSectionPresence) {
            image.presence_bitmap = std::move(raw);
            MaskUnusedPresenceBits(geometry, &image.presence_bitmap);
            have_presence = true;
        } else if (entry.type == kImageSectionExtra) {
            extra_section = std::move(raw);
            have_extra = true;
        }
        body_at += entry.stored_size;
    }
    if (body_at != bytes.size()) {
        throw std::runtime_error("chunk image has bytes after its last section");
    }
    if (!have_payload || !have_presence) {
        throw std::runtime_error("chunk image lacks its payload or presence section");
    }
    if (have_extra) {
        try {
            image.extra = ChunkExtra::Decode(
                extra_section.data(), extra_section.size(), geometry.ChunkBlockCount(),
                ExtraPadding::kReject);
        } catch (const std::invalid_argument& e) {
            throw std::runtime_error(std::string("chunk image extra data is damaged: ") + e.what());
        }
        for (const auto entry : image.extra) {
            if (!BlockPresent(image.presence_bitmap, entry.block_index)) {
                throw std::runtime_error(
                    "chunk image has extra data for absent block index " +
                    std::to_string(entry.block_index));
            }
        }
    }
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
