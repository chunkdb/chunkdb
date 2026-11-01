#include "chunk_file_header.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <stdexcept>

#include "chunkdb/crc32.hpp"

namespace chunkdb {
namespace {

constexpr std::array<std::uint8_t, 8> kImageMagic{'C', 'H', 'K', 'I', 'M', 'A', 'G', 'E'};
constexpr std::array<std::uint8_t, 8> kWalMagic{'C', 'H', 'K', 'W', 'A', 'L', 'O', 'G'};
constexpr std::uint16_t kMaxSections = 64;

void RequireDims(std::uint8_t dims) {
    if (dims != 2 && dims != 3) throw std::invalid_argument("chunk file dimensions must be 2 or 3");
}

void RequireContext(const ChunkFileContext& context) {
    RequireDims(context.dims);
    if (context.dims == 2 && context.coord.z != 0)
        throw std::invalid_argument("inactive chunk file z coordinate must be 0");
    if ((context.admitted_features.incompat & kChunkModelFormatBit) == 0)
        throw std::invalid_argument("table envelope lacks chunk model feature");
}

void RequireFeatures(const FeatureFlags& used, const ChunkFileContext& context) {
    if ((used.incompat & kChunkModelFormatBit) == 0)
        throw std::invalid_argument("chunk file lacks chunk model feature");
    const auto& admitted = context.admitted_features;
    if ((used.incompat & ~admitted.incompat) != 0 || (used.ro_compat & ~admitted.ro_compat) != 0 ||
        (used.compat & ~admitted.compat) != 0)
        throw std::invalid_argument("chunk file features exceed table envelope");
}

std::uint64_t ReadLe(const std::uint8_t* at, unsigned width) noexcept {
    std::uint64_t value = 0;
    for (unsigned i = 0; i < width; ++i) value |= static_cast<std::uint64_t>(at[i]) << (8U * i);
    return value;
}

void WriteLe(std::vector<std::uint8_t>* bytes, std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) bytes->push_back(static_cast<std::uint8_t>(value >> (8U * i)));
}

void RequireVersion(std::span<const std::uint8_t> bytes, const std::array<std::uint8_t, 8>& magic) {
    if (bytes.size() < 10U) throw std::invalid_argument("chunk file header too short");
    if (!std::equal(magic.begin(), magic.end(), bytes.begin()))
        throw std::invalid_argument("chunk file header magic mismatch");
    const auto version = ReadLe(bytes.data() + 8, 2);
    if (version == 1)
        throw std::invalid_argument("created by an earlier development build of 2.0; recreate it");
    if (version != 2) throw std::invalid_argument("unsupported chunk file header version");
}

void RequireChecksum(std::span<const std::uint8_t> bytes) {
    if (ReadLe(bytes.data() + bytes.size() - 4U, 4) != Crc32(bytes.data(), bytes.size() - 4U))
        throw std::invalid_argument("chunk file header checksum mismatch");
}

FeatureFlags ParseIdentity(std::span<const std::uint8_t> bytes, const ChunkFileContext& context) {
    if (bytes[12] != context.dims) throw std::invalid_argument("chunk file dimensions differ from table");
    if (bytes[13] != 0 || bytes[14] != 0 || bytes[15] != 0)
        throw std::invalid_argument("chunk file header reserved bytes are nonzero");
    const FeatureFlags features{
        .incompat = static_cast<std::uint32_t>(ReadLe(bytes.data() + 16, 4)),
        .ro_compat = static_cast<std::uint32_t>(ReadLe(bytes.data() + 20, 4)),
        .compat = static_cast<std::uint32_t>(ReadLe(bytes.data() + 24, 4)),
    };
    RequireFeatures(features, context);
    if (!std::equal(context.epoch.begin(), context.epoch.end(), bytes.begin() + 28))
        throw std::invalid_argument("chunk file belongs to another epoch");
    const ChunkCoord coord{
        std::bit_cast<std::int64_t>(ReadLe(bytes.data() + 44, 8)),
        std::bit_cast<std::int64_t>(ReadLe(bytes.data() + 52, 8)),
        context.dims == 3 ? std::bit_cast<std::int64_t>(ReadLe(bytes.data() + 60, 8)) : 0,
    };
    if (coord != context.coord) throw std::invalid_argument("chunk file belongs to another chunk");
    return features;
}

void WriteIdentity(std::vector<std::uint8_t>* bytes, const ChunkFileContext& context, const FeatureFlags& features) {
    bytes->push_back(context.dims);
    bytes->insert(bytes->end(), 3, 0);
    WriteLe(bytes, features.incompat, 4);
    WriteLe(bytes, features.ro_compat, 4);
    WriteLe(bytes, features.compat, 4);
    bytes->insert(bytes->end(), context.epoch.begin(), context.epoch.end());
    WriteLe(bytes, std::bit_cast<std::uint64_t>(context.coord.x), 8);
    WriteLe(bytes, std::bit_cast<std::uint64_t>(context.coord.y), 8);
    if (context.dims == 3) WriteLe(bytes, std::bit_cast<std::uint64_t>(context.coord.z), 8);
}

ChunkImageSectionDescriptor ReadSection(const std::uint8_t* at) noexcept {
    return {
        .kind = static_cast<std::uint16_t>(ReadLe(at, 2)),
        .flags = static_cast<std::uint16_t>(ReadLe(at + 2, 2)),
        .stored_size = static_cast<std::uint32_t>(ReadLe(at + 4, 4)),
        .raw_size = static_cast<std::uint32_t>(ReadLe(at + 8, 4)),
        .raw_crc32 = static_cast<std::uint32_t>(ReadLe(at + 12, 4)),
    };
}

class DirectoryBounds {
public:
    DirectoryBounds(std::uint64_t header_bytes, const ChunkImageHeaderLimits& limits) : limits_(limits), file_(header_bytes) {
        if (file_ > limits_.max_file_bytes) throw std::invalid_argument("chunk image exceeds file byte limit");
    }
    void Add(const ChunkImageSectionDescriptor& entry) {
        if (have_previous_ && entry.kind <= previous_)
            throw std::invalid_argument("chunk image section kinds must be ascending and unique");
        if ((entry.flags & ~1U) != 0) throw std::invalid_argument("chunk image section flags are invalid");
        if (entry.flags == 0 && entry.stored_size != entry.raw_size)
            throw std::invalid_argument("uncompressed chunk image section sizes differ");
        if (entry.stored_size > limits_.max_section_stored_bytes || entry.raw_size > limits_.max_section_raw_bytes)
            throw std::invalid_argument("chunk image exceeds section byte limit");
        if (entry.stored_size > limits_.max_file_bytes - file_)
            throw std::invalid_argument("chunk image exceeds file byte limit");
        if (entry.raw_size > limits_.max_total_raw_bytes - raw_)
            throw std::invalid_argument("chunk image exceeds total raw byte limit");
        file_ += entry.stored_size;
        raw_ += entry.raw_size;
        previous_ = entry.kind;
        have_previous_ = true;
    }
    [[nodiscard]] std::uint64_t FileBytes() const noexcept { return file_; }
private:
    const ChunkImageHeaderLimits& limits_;
    std::uint64_t file_;
    std::uint64_t raw_ = 0;
    std::uint16_t previous_ = 0;
    bool have_previous_ = false;
};

}  // namespace

std::size_t ChunkImageFixedHeaderBytes(std::uint8_t dims) {
    RequireDims(dims);
    return 60U + 8U * dims;
}

std::size_t ChunkImageHeaderBytes(std::uint8_t dims, std::uint16_t section_count) {
    if (section_count > kMaxSections) throw std::invalid_argument("chunk image has too many sections");
    return ChunkImageFixedHeaderBytes(dims) + 16U * section_count + 4U;
}

std::size_t ChunkWalHeaderBytes(std::uint8_t dims) {
    RequireDims(dims);
    return 48U + 8U * dims;
}

CheckedChunkImageHeader ParseChunkImageHeader(
    std::span<const std::uint8_t> header, std::uint64_t complete_file_bytes,
    const ChunkFileContext& context, const ChunkImageHeaderLimits& limits) {
    RequireContext(context);
    RequireVersion(header, kImageMagic);
    if (header.size() < ChunkImageFixedHeaderBytes(context.dims))
        throw std::invalid_argument("chunk image header too short");
    const auto count = static_cast<std::uint16_t>(ReadLe(header.data() + 10, 2));
    const auto header_bytes = ChunkImageHeaderBytes(context.dims, count);
    if (header.size() != header_bytes) throw std::invalid_argument("chunk image header has wrong length");
    RequireChecksum(header);
    const ChunkImageHeaderInfo info{
        .features = ParseIdentity(header, context),
        .revision = ReadLe(header.data() + 44U + 8U * context.dims, 8),
        .commit_time_ms = ReadLe(header.data() + 52U + 8U * context.dims, 8),
    };
    if (info.revision == 0) throw std::invalid_argument("chunk image revision is zero");
    DirectoryBounds bounds(header_bytes, limits);
    const auto directory = ChunkImageFixedHeaderBytes(context.dims);
    for (std::uint16_t i = 0; i < count; ++i) bounds.Add(ReadSection(header.data() + directory + 16U * i));
    if (bounds.FileBytes() != complete_file_bytes)
        throw std::invalid_argument("chunk image sections do not fill complete file");
    CheckedChunkImageHeader result{.info = info, .sections = {}, .header_bytes = header_bytes, .file_bytes = complete_file_bytes};
    result.sections.reserve(count);
    for (std::uint16_t i = 0; i < count; ++i) result.sections.push_back(ReadSection(header.data() + directory + 16U * i));
    return result;
}

std::vector<std::uint8_t> SerializeChunkImageHeader(
    const ChunkFileContext& context, const ChunkImageHeaderInfo& info,
    std::span<const ChunkImageSectionDescriptor> sections, const ChunkImageHeaderLimits& limits) {
    RequireContext(context);
    RequireFeatures(info.features, context);
    if (info.revision == 0) throw std::invalid_argument("chunk image revision is zero");
    if (sections.size() > kMaxSections) throw std::invalid_argument("chunk image has too many sections");
    const auto header_bytes = ChunkImageHeaderBytes(context.dims, static_cast<std::uint16_t>(sections.size()));
    DirectoryBounds bounds(header_bytes, limits);
    for (const auto& entry : sections) bounds.Add(entry);
    std::vector<std::uint8_t> bytes;
    bytes.reserve(header_bytes);
    bytes.insert(bytes.end(), kImageMagic.begin(), kImageMagic.end());
    WriteLe(&bytes, 2, 2);
    WriteLe(&bytes, sections.size(), 2);
    WriteIdentity(&bytes, context, info.features);
    WriteLe(&bytes, info.revision, 8);
    WriteLe(&bytes, info.commit_time_ms, 8);
    for (const auto& entry : sections) {
        WriteLe(&bytes, entry.kind, 2); WriteLe(&bytes, entry.flags, 2);
        WriteLe(&bytes, entry.stored_size, 4); WriteLe(&bytes, entry.raw_size, 4);
        WriteLe(&bytes, entry.raw_crc32, 4);
    }
    WriteLe(&bytes, Crc32(bytes), 4);
    return bytes;
}

FeatureFlags ParseChunkWalHeader(std::span<const std::uint8_t> header, const ChunkFileContext& context) {
    RequireContext(context);
    RequireVersion(header, kWalMagic);
    if (header.size() != ChunkWalHeaderBytes(context.dims)) throw std::invalid_argument("chunk WAL header has wrong length");
    RequireChecksum(header);
    if (ReadLe(header.data() + 10, 2) != 0) throw std::invalid_argument("chunk WAL reserved field is nonzero");
    return ParseIdentity(header, context);
}

std::vector<std::uint8_t> SerializeChunkWalHeader(const ChunkFileContext& context, const FeatureFlags& features) {
    RequireContext(context);
    RequireFeatures(features, context);
    std::vector<std::uint8_t> bytes;
    bytes.reserve(ChunkWalHeaderBytes(context.dims));
    bytes.insert(bytes.end(), kWalMagic.begin(), kWalMagic.end());
    WriteLe(&bytes, 2, 2);
    WriteLe(&bytes, 0, 2);
    WriteIdentity(&bytes, context, features);
    WriteLe(&bytes, Crc32(bytes), 4);
    return bytes;
}

}  // namespace chunkdb
