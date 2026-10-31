#include "chunkdb/file_layout.hpp"

#include <string>
#include <array>
#include <charconv>
#include <algorithm>
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "wal_replay.hpp"

namespace chunkdb {

namespace {

void RequireDimensions(std::uint8_t dims, std::int64_t z) {
    if (dims != 2 && dims != 3) throw std::invalid_argument("coordinate dimensions must be 2 or 3");
    if (dims == 2 && z != 0) throw std::invalid_argument("inactive z coordinate must be 0");
}

bool ParseCoordinateName(
    std::string_view name, std::string_view prefix, std::uint8_t dims,
    std::array<std::int64_t, 3>* axes) {
    if ((dims != 2 && dims != 3) || !name.starts_with(prefix)) return false;
    name.remove_prefix(prefix.size());
    for (std::uint8_t axis = 0; axis < dims; ++axis) {
        const auto separator = name.find('_');
        if ((axis + 1U < dims) != (separator != std::string_view::npos)) return false;
        const auto text = name.substr(0, separator);
        if (text.empty() || text.size() > 20U) return false;
        const auto digits = text.front() == '-' ? text.substr(1) : text;
        if (digits.empty() || (digits.front() == '0' && (digits.size() != 1U || text.front() == '-')))
            return false;
        if (!std::all_of(digits.begin(), digits.end(), [](char c) { return c >= '0' && c <= '9'; }))
            return false;
        const auto parsed = std::from_chars(text.data(), text.data() + text.size(), (*axes)[axis]);
        if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) return false;
        if (separator != std::string_view::npos) name.remove_prefix(separator + 1U);
    }
    return true;
}

}  // namespace

std::filesystem::path LargeChunkDirectory(
    const std::filesystem::path& data_dir,
    const LargeChunkCoord& large_coord) {
    return LargeChunkDirectory(data_dir, large_coord, 2);
}

std::filesystem::path LargeChunkDirectory(
    const std::filesystem::path& data_dir, const LargeChunkCoord& coord, std::uint8_t dims) {
    RequireDimensions(dims, coord.z);
    auto name = "L_" + std::to_string(coord.x) + "_" + std::to_string(coord.y);
    if (dims == 3) name += "_" + std::to_string(coord.z);
    return data_dir / name;
}

std::string ChunkFileStem(const ChunkCoord& coord, std::uint8_t dims) {
    RequireDimensions(dims, coord.z);
    auto name = "C_" + std::to_string(coord.x) + "_" + std::to_string(coord.y);
    if (dims == 3) name += "_" + std::to_string(coord.z);
    return name;
}

bool ParseChunkFileStem(std::string_view name, std::uint8_t dims, ChunkCoord* out) {
    std::array<std::int64_t, 3> axes{};
    if (out == nullptr || !ParseCoordinateName(name, "C_", dims, &axes)) return false;
    *out = {axes[0], axes[1], axes[2]};
    return true;
}

bool ParseLargeChunkName(std::string_view name, std::uint8_t dims, LargeChunkCoord* out) {
    std::array<std::int64_t, 3> axes{};
    if (out == nullptr || !ParseCoordinateName(name, "L_", dims, &axes)) return false;
    *out = {axes[0], axes[1], axes[2]};
    return true;
}

bool ParseChunkFilePath(const std::filesystem::path& relative, const Geometry& geometry, ChunkCoord* out) {
    if (out == nullptr || relative.is_absolute()) return false;
    const auto extension = relative.extension();
    if (extension != ".chk" && extension != ".wal") return false;
    ChunkCoord coord;
    if (!ParseChunkFileStem(relative.stem().string(), geometry.dims(), &coord)) return false;
    auto expected = ChunkDataPath({}, geometry, coord);
    expected.replace_extension(extension);
    if (relative != expected) return false;
    *out = coord;
    return true;
}

std::filesystem::path ChunkDataPath(
    const std::filesystem::path& data_dir,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord) {
    const LargeChunkCoord large_coord = geometry.ChunkToLarge(chunk_coord);
    return LargeChunkDirectory(data_dir, large_coord, geometry.dims()) /
           (ChunkFileStem(chunk_coord, geometry.dims()) + ".chk");
}

std::filesystem::path ChunkWalPath(
    const std::filesystem::path& data_dir,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord) {
    const LargeChunkCoord large_coord = geometry.ChunkToLarge(chunk_coord);
    return LargeChunkDirectory(data_dir, large_coord, geometry.dims()) /
           (ChunkFileStem(chunk_coord, geometry.dims()) + ".wal");
}

std::vector<std::uint8_t> RewriteChunkImageStoreId(
    std::vector<std::uint8_t> bytes, const Geometry& geometry, ChunkCoord coord,
    const StoreId& old_id, const StoreId& new_id, FeatureFlags features) {
    (void)ParseChunkImage(bytes, geometry, coord, old_id, features);
    if (std::all_of(new_id.begin(), new_id.end(), [](auto b) { return b == 0U; }))
        throw std::invalid_argument("new store id must not be zero");
    std::copy(new_id.begin(), new_id.end(), bytes.begin() + 24U);
    const auto crc_at = kImageFixedHeaderSize + static_cast<std::size_t>(ReadLe16(bytes, 10U)) * kImageSectionEntrySize;
    const auto crc = Crc32(bytes.data(), crc_at);
    for (unsigned i = 0U; i < 4U; ++i) bytes[crc_at + i] = static_cast<std::uint8_t>(crc >> (i * 8U));
    return bytes;
}
std::vector<std::uint8_t> RewriteWalStoreId(
    std::vector<std::uint8_t> bytes, ChunkCoord coord,
    const StoreId& old_id, const StoreId& new_id, FeatureFlags features) {
    ValidateWalHeader(bytes, coord, old_id, features);
    if (std::all_of(new_id.begin(), new_id.end(), [](auto b) { return b == 0U; }))
        throw std::invalid_argument("new store id must not be zero");
    const FeatureFlags original{ReadLe32(bytes, 12U), ReadLe32(bytes, 16U), ReadLe32(bytes, 20U)};
    const auto header = BuildWalHeader(coord, new_id, original);
    std::copy(header.begin(), header.end(), bytes.begin());
    return bytes;
}

}  // namespace chunkdb
