#include "chunkdb/chunk_store.hpp"

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "shared_file_input.hpp"
#include "eviction.hpp"
#include "process_lock.hpp"
#include "populated_index_format.hpp"
#include "store_manifest.hpp"
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

static_assert(kFeatureChunkModel == kChunkModelFormatBit);

[[nodiscard]] std::size_t ChunkPresenceBitmapBytes(const Geometry& geometry) noexcept {
    return (geometry.ChunkBlockCount() + 7U) / 8U;
}

[[nodiscard]] std::size_t ChunkStateBytes(const Geometry& geometry) noexcept {
    return geometry.ChunkPayloadBytes() + ChunkPresenceBitmapBytes(geometry);
}

void MaskUnusedPayloadBits(const Geometry& geometry, std::vector<std::uint8_t>* payload) {
    geometry.layout().MaskPadding(payload);
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

std::size_t PresentBlockCount(const std::vector<std::uint8_t>& presence_bitmap) noexcept {
    std::size_t count = 0U;
    for (const auto byte : presence_bitmap) count += std::popcount(byte);
    return count;
}

bool ChunkPopulated(const Geometry& geometry, const std::vector<std::uint8_t>& presence_bitmap,
    const ChunkColumnState* chunk_columns) noexcept {
    return ChunkPresent(presence_bitmap) || ChunkColumnsPopulated(geometry, chunk_columns);
}

bool ChunkColumnsPopulated(const Geometry& geometry, const ChunkColumnState* chunk_columns) noexcept {
    const auto& baseline = geometry.default_chunk_columns();
    return chunk_columns != nullptr && (baseline == nullptr || chunk_columns->bytes != baseline->bytes);
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
    geometry.layout().ClearEmptyValues(presence_bitmap, payload);
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

// The most raw bytes one section of a chunk image holds: the largest a
// payload or a VARS section can ever be.
constexpr std::uint32_t kMaxImageSectionRawBytes = 64U * 1024U * 1024U;

ChunkImageHeaderLimits ImageHeaderLimits() noexcept {
    // A metadata row adds its scalar directory to the shared variable-byte
    // budget. Unknown sections remain bounded again by the typed body reader.
    constexpr std::uint64_t section_limit = kMaxChunkColumnsSectionBytes;
    return {.max_file_bytes = kMaxChunkImageFileBytes,
            .max_section_stored_bytes = section_limit + kZrleMaxOverheadBytes,
            .max_section_raw_bytes = section_limit,
            .max_total_raw_bytes = kMaxChunkImageRawBytes};
}

struct ImageSectionEntry {
    std::uint16_t type = 0;
    std::uint16_t flags = 0;
    std::uint32_t stored_size = 0;
    std::uint32_t raw_size = 0;
    std::uint32_t crc = 0;
};

[[nodiscard]] std::uint32_t KnownSectionRawSize(
    const Geometry& geometry,
    const ChunkLayout& layout,
    std::uint16_t type) {
    switch (type) {
        case kImageSectionPayload:
            return static_cast<std::uint32_t>(layout.payload_bytes());
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
    const ChunkVars* vars,
    const ChunkColumnState* chunk_columns) {
    if (payload.size() != geometry.ChunkPayloadBytes() ||
        presence_bitmap.size() != ChunkPresenceBitmapBytes(geometry)) {
        throw std::invalid_argument("chunk state size does not match geometry");
    }
    if (revision == 0U) {
        throw std::invalid_argument("chunk image revision must not be zero");
    }
    const bool has_vars = vars != nullptr && !vars->empty();
    if (vars != nullptr && vars->encoded_size() > kVarMaxChunkBytesLimit)
        throw std::invalid_argument("chunk image VARS exceeds byte limit");
    if (geometry.chunk_schema().columns.empty()) {
        if (chunk_columns != nullptr && !chunk_columns->bytes.empty())
            throw std::invalid_argument("chunk image has metadata without chunk columns");
    } else {
        if (chunk_columns == nullptr) throw std::invalid_argument("chunk image is missing its chunk columns");
        const auto variable_bytes = ValidateChunkColumns(geometry.chunk_schema(), chunk_columns->bytes);
        if (variable_bytes > kVarMaxChunkBytesLimit - (vars == nullptr ? 0U : vars->encoded_size()))
            throw std::invalid_argument("chunk image exceeds the combined variable-byte limit");
    }
    if (has_vars) {
        geometry.layout().RequireValidVars(*vars, presence_bitmap);
    }
    std::vector<std::uint8_t> directory;
    std::vector<std::uint8_t> bodies;
    AppendSection(&directory, &bodies, kImageSectionPayload, payload, compression);
    AppendSection(&directory, &bodies, kImageSectionPresence, presence_bitmap, compression);
    if (has_vars) {
        AppendSection(&directory, &bodies, kImageSectionVars, vars->Encode(), compression);
    }
    // Images of a table that never changed its columns stay as they were.
    const std::uint64_t schema_version = geometry.layout().schema().version;
    const bool has_schema = schema_version > 1U;
    if (has_schema) {
        std::vector<std::uint8_t> version_bytes;
        WriteLe64(version_bytes, schema_version);
        AppendSection(&directory, &bodies, kImageSectionSchema, version_bytes, CheckpointCompression::kNone);
    }
    if (chunk_columns != nullptr && !chunk_columns->bytes.empty())
        AppendSection(&directory, &bodies, kImageSectionChunkColumns, chunk_columns->bytes, compression);

    std::vector<ChunkImageSectionDescriptor> sections;
    sections.reserve(directory.size() / kImageSectionEntrySize);
    for (std::size_t at = 0; at < directory.size(); at += kImageSectionEntrySize)
        sections.push_back({.kind = ReadLe16(directory, at), .flags = ReadLe16(directory, at + 2U),
            .stored_size = ReadLe32(directory, at + 4U), .raw_size = ReadLe32(directory, at + 8U),
            .raw_crc32 = ReadLe32(directory, at + 12U)});
    const ChunkFileContext context{.dims = geometry.dims(), .epoch = store_id, .coord = chunk_coord};
    auto bytes = SerializeChunkImageHeader(context,
        {.features = context.admitted_features, .revision = revision, .commit_time_ms = commit_time_ms},
        sections, ImageHeaderLimits());
    bytes.insert(bytes.end(), bodies.begin(), bodies.end());
    return bytes;
}

std::vector<std::uint8_t> BuildWalHeader(
    const ChunkCoord& chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& features, std::uint8_t dims) {
    return SerializeChunkWalHeader(
        {.dims = dims, .epoch = store_id, .coord = chunk_coord, .admitted_features = features}, features);
}

ChunkStateImage ParseChunkImage(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord,
    const StoreId& store_id,
    const FeatureFlags& store_features) {
    if (bytes.size() < 12U) {
        throw std::runtime_error("chunk image too small");
    }
    if (std::memcmp(bytes.data(), kImageMagic, kImageMagicSize) != 0) {
        throw std::runtime_error("not a 2.0 chunk image (bad magic)");
    }
    const std::uint16_t version = ReadLe16(bytes, 8U);
    if (version == 1U) throw EarlierDevelopmentFormatError();
    if (version != kImageFormatVersion) {
        throw std::runtime_error("unsupported chunk image version " + std::to_string(version));
    }
    const std::uint16_t section_count = ReadLe16(bytes, 10U);
    if (section_count > kImageMaxSections) {
        throw std::runtime_error("chunk image has too many sections");
    }
    const auto fixed_header = ChunkImageFixedHeaderBytes(geometry.dims());
    const std::size_t directory_end = fixed_header + static_cast<std::size_t>(section_count) * kImageSectionEntrySize;
    if (bytes.size() < directory_end + 4U) {
        throw std::runtime_error("chunk image too small for its section directory");
    }
    CheckedChunkImageHeader checked;
    try {
        checked = ParseChunkImageHeader(std::span(bytes).first(directory_end + 4U), bytes.size(),
            {.dims = geometry.dims(), .epoch = store_id, .coord = expected_chunk_coord, .admitted_features = store_features},
            ImageHeaderLimits());
    } catch (const std::invalid_argument& error) { throw std::runtime_error(error.what()); }
    ChunkStateImage image;
    image.features = checked.info.features;
    image.revision = checked.info.revision;
    image.commit_time_ms = checked.info.commit_time_ms;

    // An image is rewritten whole at every checkpoint with the features it
    // uses, so its own flags name every feature its sections belong to.
    const bool may_skip_unknown = MaySkipUnknownTypes(image.features);

    // The schema version sizes the other sections, so it is read first.
    std::uint64_t schema_version = 1;
    for (std::size_t i = 0, offset = directory_end + 4U; i < section_count; ++i) {
        const std::size_t at = fixed_header + i * kImageSectionEntrySize;
        const std::uint32_t stored_size = ReadLe32(bytes, at + 4U);
        if (bytes.size() - offset < stored_size) {
            break;  // the loop below reports it
        }
        if (ReadLe16(bytes, at) == kImageSectionSchema) {
            if (ReadLe16(bytes, at + 2U) != 0U || stored_size != 8U || ReadLe32(bytes, at + 8U) != 8U) {
                throw std::runtime_error("chunk image schema section is malformed");
            }
            if (Crc32(bytes.data() + offset, 8U) != ReadLe32(bytes, at + 12U)) {
                throw std::runtime_error("chunk image section 4 checksum mismatch");
            }
            schema_version = ReadLe64(bytes, offset);
            break;
        }
        offset += stored_size;
    }
    if (schema_version == 0U || schema_version > geometry.layout().schema().version) {
        throw std::runtime_error(
            "chunk image is of schema version " + std::to_string(schema_version) + ", the table is at " +
            std::to_string(geometry.layout().schema().version));
    }
    const ChunkLayout& layout = geometry.LayoutAt(schema_version);
    image.schema_version = schema_version;

    std::size_t body_at = directory_end + 4U;
    std::uint16_t previous_type = 0;
    bool have_payload = false;
    bool have_presence = false;
    std::vector<std::uint8_t> vars_section;
    bool have_vars = false;
    for (std::uint16_t i = 0; i < section_count; ++i) {
        const std::size_t at = fixed_header + static_cast<std::size_t>(i) * kImageSectionEntrySize;
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
        const bool known =
            fixed_size || entry.type == kImageSectionVars || entry.type == kImageSectionSchema ||
            entry.type == kImageSectionChunkColumns;
        if (!known && !may_skip_unknown) {
            throw std::runtime_error("unknown " + name);
        }
        // Bounded before decompression allocates it. A compressed section
        // expands without a fixed ratio, so a section whose raw size no
        // known type pins down -- one owned by a feature this build does not
        // know -- could otherwise ask for gigabytes from a few stored bytes.
        // No section a known type owns reaches this size.
        const auto section_limit = entry.type == kImageSectionChunkColumns ?
            kMaxChunkColumnsSectionBytes : kMaxImageSectionRawBytes;
        if (entry.raw_size > section_limit) {
            throw std::runtime_error(
                name + " declares " + std::to_string(entry.raw_size) + " raw bytes, more than " +
                std::to_string(section_limit));
        }
        if (fixed_size && entry.raw_size != KnownSectionRawSize(geometry, layout, entry.type)) {
            throw std::runtime_error(name + " has the wrong size for the store geometry");
        }
        // Bounded before decompression allocates it.
        if (entry.type == kImageSectionVars && (entry.raw_size == 0U || entry.raw_size > kVarMaxChunkBytesLimit)) {
            throw std::runtime_error(name + " (VARS) has size " + std::to_string(entry.raw_size));
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
        } else if (entry.type == kImageSectionVars) {
            vars_section = std::move(raw);
            have_vars = true;
        } else if (entry.type == kImageSectionChunkColumns) {
            if (!geometry.has_chunk_column_history())
                throw std::runtime_error("chunk image has metadata without chunk columns");
            try {
                const auto& schema = geometry.ChunkColumnsLayoutAt(schema_version).schema();
                if (schema.columns.empty()) throw std::invalid_argument("metadata schema is empty");
                const auto variable_bytes = ValidateChunkColumns(schema, raw);
                image.chunk_columns = std::make_shared<const ChunkColumnState>(
                    ChunkColumnState{.bytes = std::move(raw), .variable_bytes = variable_bytes});
            } catch (const std::invalid_argument& error) {
                throw std::runtime_error("chunk image metadata is damaged: " + std::string(error.what()));
            }
        }
        body_at += entry.stored_size;
    }
    if (body_at != bytes.size()) {
        throw std::runtime_error("chunk image has bytes after its last section");
    }
    if (!have_payload || !have_presence) {
        throw std::runtime_error("chunk image lacks its payload or presence section");
    }
    if (have_vars) {
        try {
            image.vars = ChunkVars::Decode(vars_section.data(), vars_section.size(), geometry.ChunkBlockCount());
            layout.RequireValidVars(image.vars, image.presence_bitmap);
        } catch (const std::invalid_argument& e) {
            throw std::runtime_error(std::string("chunk image VARS section is damaged: ") + e.what());
        }
    }
    if (geometry.has_chunk_column_history() &&
        !geometry.ChunkColumnsLayoutAt(schema_version).schema().columns.empty() && !image.chunk_columns)
        throw std::runtime_error("chunk image is missing its chunk columns");
    if (image.chunk_columns && image.chunk_columns->variable_bytes > kVarMaxChunkBytesLimit - image.vars.encoded_size())
        throw std::runtime_error("chunk image exceeds the combined variable-byte limit");
    return image;
}


std::vector<std::uint8_t> LoadChunkImageFile(
    const std::filesystem::path& path, const Geometry& geometry, ChunkCoord coord,
    const StoreId& store_id, FeatureFlags store_features) {
    SharedFileInput input(path);
    if (!input) throw std::runtime_error("failed to open chunk image: " + path.string());
    input.seekg(0, std::ios::end);
    const auto end = input.tellg();
    if (end < 0) throw std::runtime_error("failed to measure chunk image: " + path.string());
    const auto file_size = static_cast<std::uint64_t>(end);
    if (file_size < 12U) throw std::runtime_error("chunk image too small: " + path.string());
    input.seekg(0, std::ios::beg);
    const auto read_exact = [&](std::uint8_t* bytes, std::size_t count) {
        input.read(reinterpret_cast<char*>(bytes), static_cast<std::streamsize>(count));
        if (input.gcount() != static_cast<std::streamsize>(count))
            throw std::runtime_error("short read of chunk image: " + path.string());
    };
    std::vector<std::uint8_t> bytes(12U);
    read_exact(bytes.data(), bytes.size());
    if (std::memcmp(bytes.data(), kImageMagic, kImageMagicSize) != 0)
        throw std::runtime_error("chunk image magic mismatch: " + path.string());
    if (ReadLe16(bytes, 8U) == 1U) throw EarlierDevelopmentFormatError();
    if (ReadLe16(bytes, 8U) != kImageFormatVersion)
        throw std::runtime_error("unsupported chunk image version: " + path.string());
    if (file_size > kMaxChunkImageFileBytes)
        throw std::runtime_error("chunk image exceeds file byte limit: " + path.string());
    const auto count = ReadLe16(bytes, 10U);
    if (count > kImageMaxSections) throw std::runtime_error("chunk image has too many sections");
    const auto header_size = ChunkImageHeaderBytes(geometry.dims(), count);
    if (file_size < header_size) throw std::runtime_error("chunk image directory is truncated");
    bytes.resize(header_size);
    read_exact(bytes.data() + 12U, header_size - 12U);
    try {
        (void)ParseChunkImageHeader(bytes, file_size,
            {.dims = geometry.dims(), .epoch = store_id, .coord = coord, .admitted_features = store_features},
            ImageHeaderLimits());
    } catch (const std::invalid_argument& error) {
        throw std::runtime_error(error.what());
    }
    bytes.resize(static_cast<std::size_t>(file_size));
    read_exact(bytes.data() + header_size, bytes.size() - header_size);
    return bytes;
}

std::shared_ptr<const ChunkColumnState> DefaultChunkColumnsAt(const Geometry& geometry, std::uint64_t version) {
    if (version == 0U || !geometry.has_chunk_column_history()) return {};
    if (version == geometry.chunk_schema().version) return geometry.default_chunk_columns();
    const auto& schema = geometry.ChunkColumnsLayoutAt(version).schema();
    if (schema.columns.empty()) return {};
    return std::make_shared<const ChunkColumnState>(DefaultChunkColumns(schema));
}

void BringChunkColumnsToSchema(const Geometry& geometry, std::uint64_t from_version, std::uint64_t to_version,
    std::shared_ptr<const ChunkColumnState>* state) {
    if (from_version == 0U || !geometry.has_chunk_column_history()) { state->reset(); return; }
    if (to_version < from_version || to_version > geometry.chunk_schema().version)
        throw std::invalid_argument("chunk column schema version is out of range");
    const auto& from = geometry.ChunkColumnsLayoutAt(from_version).schema();
    (void)ValidateChunkColumns(from, *state ? std::span<const std::uint8_t>((*state)->bytes) :
        std::span<const std::uint8_t>{});
    auto next = *state;
    for (auto version = from_version + 1U; version <= to_version; ++version) {
        const auto& before = geometry.ChunkColumnsLayoutAt(version - 1U).schema();
        const auto& after = geometry.ChunkColumnsLayoutAt(version).schema();
        if (before.columns == after.columns) continue;
        auto changed = TranslateChunkColumns(before, after, next ? *next : ChunkColumnState{});
        if (after.columns.empty()) next.reset();
        else next = std::make_shared<const ChunkColumnState>(std::move(changed));
    }
    *state = std::move(next);
}

bool ApplyRetainedEmptyBase(const Geometry& geometry, const ChunkIndexRecord& boundary,
    std::uint64_t& revision, std::uint64_t& commit_time_ms, std::uint64_t& schema_version,
    std::vector<std::uint8_t>& payload, std::vector<std::uint8_t>& presence, ChunkVars& vars,
    std::shared_ptr<const ChunkColumnState>& chunk_columns) {
    if (boundary.kind != ChunkIndexKind::kDeleted && boundary.kind != ChunkIndexKind::kForget)
        throw std::invalid_argument("retained replay base is not an empty boundary");
    if (revision > boundary.revision) return false;
    if (revision == boundary.revision) {
        if (commit_time_ms != boundary.commit_time_ms)
            throw std::runtime_error("retained empty boundary conflicts with an equal-version producing time");
        auto current_columns = chunk_columns;
        BringChunkColumnsToSchema(geometry, schema_version, geometry.chunk_schema().version, &current_columns);
        if (ChunkPopulated(geometry, presence, current_columns.get()))
            throw std::runtime_error("retained empty boundary conflicts with a populated equal-version image");
    }
    payload.assign(geometry.LayoutAt(boundary.schema_version).payload_bytes(), 0U);
    presence.assign(ChunkPresenceBitmapBytes(geometry), 0U);
    vars = ChunkVars{};
    chunk_columns = DefaultChunkColumnsAt(geometry, boundary.schema_version);
    schema_version = boundary.schema_version;
    revision = boundary.revision;
    commit_time_ms = boundary.commit_time_ms;
    return true;
}

void BringToCurrentSchema(
    const Geometry& geometry,
    std::uint64_t version,
    const std::vector<std::uint8_t>& presence,
    std::vector<std::uint8_t>* payload,
    ChunkVars* vars) {
    const ChunkLayout& current = geometry.layout();
    if (version == 0U) {
        // The empty state of no image.
        payload->assign(current.payload_bytes(), 0U);
        *vars = ChunkVars{};
        return;
    }
    // One version at a time: each step converts as it recorded.
    for (std::uint64_t step = version + 1U; step <= current.schema().version; ++step) {
        TranslateChunk(geometry.LayoutAt(step - 1U), geometry.LayoutAt(step), presence, payload, vars);
    }
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
