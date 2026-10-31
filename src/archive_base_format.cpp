#include "archive_base_format.hpp"

#include <algorithm>
#include <bit>
#include <charconv>
#include <stdexcept>

#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"

namespace chunkdb {
namespace {

void RequireCoord(std::uint8_t dims, const ChunkCoord& coord) {
    if (dims != 2 && dims != 3) throw std::invalid_argument("archive base dimensions must be 2 or 3");
    if (dims == 2 && coord.z != 0) throw std::invalid_argument("inactive archive base z coordinate must be 0");
}

void RequireDescriptor(const ArchiveBaseDescriptor& descriptor, std::uint64_t max_image_bytes) {
    RequireCoord(descriptor.dims, descriptor.coord);
    if (descriptor.first == 0 || descriptor.base_revision >= descriptor.first)
        throw std::invalid_argument("archive base revision must be strictly below segment first");
    if (descriptor.base_schema == 0) throw std::invalid_argument("archive base schema version is zero");
    switch (descriptor.kind) {
        case ArchiveBaseKind::kNeverWritten:
            if (descriptor.base_revision != 0 || descriptor.base_time_ms != 0)
                throw std::invalid_argument("never-written archive base has a producing commit");
            break;
        case ArchiveBaseKind::kImage:
            if (descriptor.base_revision == 0 || descriptor.image_length == 0 ||
                descriptor.image_length > max_image_bytes)
                throw std::invalid_argument("archive base image revision or length is invalid");
            return;
        case ArchiveBaseKind::kRetainedEmpty:
            if (descriptor.base_revision == 0)
                throw std::invalid_argument("retained-empty archive base has no producing commit");
            break;
        default:
            throw std::invalid_argument("unknown archive base kind");
    }
    if (descriptor.image_length != 0 || descriptor.image_crc32 != 0)
        throw std::invalid_argument("non-image archive base contains image fields");
}

std::uint64_t ReadLe(const std::uint8_t* at, unsigned width) noexcept {
    std::uint64_t value = 0;
    for (unsigned i = 0; i < width; ++i) value |= static_cast<std::uint64_t>(at[i]) << (8U * i);
    return value;
}

void WriteLe(std::vector<std::uint8_t>* bytes, std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) bytes->push_back(static_cast<std::uint8_t>(value >> (8U * i)));
}

}  // namespace

std::vector<std::uint8_t> SerializeArchiveBaseDescriptor(
    const ArchiveBaseDescriptor& descriptor, std::uint64_t max_image_bytes) {
    RequireDescriptor(descriptor, max_image_bytes);
    std::vector<std::uint8_t> bytes;
    bytes.reserve(72U + 8U * descriptor.dims);
    bytes.insert(bytes.end(), {'C', 'K', 'F', 'B'});
    WriteLe(&bytes, 1, 2);
    bytes.push_back(descriptor.dims);
    bytes.push_back(static_cast<std::uint8_t>(descriptor.kind));
    bytes.insert(bytes.end(), descriptor.epoch.begin(), descriptor.epoch.end());
    WriteLe(&bytes, std::bit_cast<std::uint64_t>(descriptor.coord.x), 8);
    WriteLe(&bytes, std::bit_cast<std::uint64_t>(descriptor.coord.y), 8);
    if (descriptor.dims == 3) WriteLe(&bytes, std::bit_cast<std::uint64_t>(descriptor.coord.z), 8);
    WriteLe(&bytes, descriptor.first, 8);
    WriteLe(&bytes, descriptor.base_revision, 8);
    WriteLe(&bytes, descriptor.base_time_ms, 8);
    WriteLe(&bytes, descriptor.base_schema, 8);
    WriteLe(&bytes, descriptor.image_length, 8);
    WriteLe(&bytes, descriptor.image_crc32, 4);
    WriteLe(&bytes, Crc32(bytes), 4);
    return bytes;
}

ArchiveBaseDescriptor ParseArchiveBaseDescriptor(
    std::span<const std::uint8_t> bytes, std::uint8_t dims, const StoreId& epoch,
    const ChunkCoord& coord, std::uint64_t first, std::uint64_t max_image_bytes) {
    RequireCoord(dims, coord);
    if (bytes.size() != 72U + 8U * dims) throw std::invalid_argument("archive base descriptor has wrong length");
    if (bytes[0] != 'C' || bytes[1] != 'K' || bytes[2] != 'F' || bytes[3] != 'B')
        throw std::invalid_argument("not a CKFB archive base");
    if (ReadLe(bytes.data() + 4, 2) != 1) throw std::invalid_argument("unsupported archive base version");
    if (bytes[6] != dims) throw std::invalid_argument("archive base dimensions differ from table");
    if (ReadLe(bytes.data() + bytes.size() - 4U, 4) != Crc32(bytes.data(), bytes.size() - 4U))
        throw std::invalid_argument("archive base checksum mismatch");
    if (!std::equal(epoch.begin(), epoch.end(), bytes.begin() + 8))
        throw std::invalid_argument("archive base belongs to another epoch");
    const ChunkCoord stored_coord{
        std::bit_cast<std::int64_t>(ReadLe(bytes.data() + 24, 8)),
        std::bit_cast<std::int64_t>(ReadLe(bytes.data() + 32, 8)),
        dims == 3 ? std::bit_cast<std::int64_t>(ReadLe(bytes.data() + 40, 8)) : 0,
    };
    if (stored_coord != coord) throw std::invalid_argument("archive base belongs to another chunk");
    const auto fields = 24U + 8U * dims;
    ArchiveBaseDescriptor descriptor{
        .dims = dims, .kind = static_cast<ArchiveBaseKind>(bytes[7]), .epoch = epoch, .coord = coord,
        .first = ReadLe(bytes.data() + fields, 8), .base_revision = ReadLe(bytes.data() + fields + 8U, 8),
        .base_time_ms = ReadLe(bytes.data() + fields + 16U, 8),
        .base_schema = ReadLe(bytes.data() + fields + 24U, 8),
        .image_length = ReadLe(bytes.data() + fields + 32U, 8),
        .image_crc32 = static_cast<std::uint32_t>(ReadLe(bytes.data() + fields + 40U, 4)),
    };
    if (descriptor.first != first) throw std::invalid_argument("archive base belongs to another segment");
    RequireDescriptor(descriptor, max_image_bytes);
    return descriptor;
}

void ValidatePinnedArchiveBaseImage(
    const ArchiveBaseDescriptor& descriptor, const PinnedBaseImageIdentity& image,
    std::span<const std::uint8_t> bytes, std::uint64_t max_image_bytes) {
    RequireDescriptor(descriptor, max_image_bytes);
    if (descriptor.kind != ArchiveBaseKind::kImage) throw std::invalid_argument("archive base does not require an image");
    if (image.dims != descriptor.dims || image.epoch != descriptor.epoch || image.coord != descriptor.coord ||
        image.revision != descriptor.base_revision || image.commit_time_ms != descriptor.base_time_ms ||
        image.schema_version != descriptor.base_schema)
        throw std::invalid_argument("pinned archive base image identity mismatch");
    if (bytes.size() != descriptor.image_length || Crc32(bytes.data(), bytes.size()) != descriptor.image_crc32)
        throw std::invalid_argument("pinned archive base image length or checksum mismatch");
}

std::string ArchiveBaseFileName(const ChunkCoord& coord, std::uint8_t dims, std::uint64_t first) {
    RequireCoord(dims, coord);
    if (first == 0) throw std::invalid_argument("archive segment first revision is zero");
    return ChunkFileStem(coord, dims) + "." + std::to_string(first) + ".base";
}

bool ParseArchiveBaseFileName(
    std::string_view name, std::uint8_t dims, ChunkCoord* coord, std::uint64_t* first) {
    if (coord == nullptr || first == nullptr || (dims != 2 && dims != 3) ||
        name.size() > 27U + 21U * dims || !name.ends_with(".base"))
        return false;
    name.remove_suffix(5);
    const auto dot = name.find('.');
    if (dot == std::string_view::npos) return false;
    ChunkCoord parsed_coord;
    if (!ParseChunkFileStem(name.substr(0, dot), dims, &parsed_coord)) return false;
    const auto digits = name.substr(dot + 1U);
    if (digits.empty() || digits.front() == '0' || digits.size() > 20U ||
        !std::all_of(digits.begin(), digits.end(), [](char c) { return c >= '0' && c <= '9'; }))
        return false;
    std::uint64_t revision = 0;
    const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), revision);
    if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size() || revision == 0) return false;
    *coord = parsed_coord;
    *first = revision;
    return true;
}

}  // namespace chunkdb
