#include "store_manifest.hpp"

#include <algorithm>
#include <random>
#include <stdexcept>
#include <system_error>

#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"

namespace chunkdb {

namespace {

constexpr std::array<std::uint8_t, 4> kStoreManifestMagic = {'C', 'K', 'M', 'F'};
constexpr std::size_t kFlagsOffset = 8U;
constexpr std::size_t kGeometryOffset = 20U;
constexpr std::size_t kStoreIdOffset = 40U;
constexpr std::size_t kOptionsSizeOffset = 56U;
constexpr std::size_t kOptionsOffset = 60U;
constexpr std::size_t kOptionEntryHeaderSize = 4U;

// 2.0.0 defines no option types; each later option belongs to a feature.
[[nodiscard]] bool IsKnownOptionType(std::uint16_t /*type*/) noexcept {
    return false;
}

void ValidateOptions(const std::vector<std::uint8_t>& options, const FeatureFlags& features) {
    std::size_t at = 0;
    while (at < options.size()) {
        if (options.size() - at < kOptionEntryHeaderSize) {
            throw std::runtime_error("truncated option entry");
        }
        const std::uint16_t type = ReadLe16(options, at);
        const std::uint16_t length = ReadLe16(options, at + 2U);
        if (options.size() - at - kOptionEntryHeaderSize < length) {
            throw std::runtime_error("option " + std::to_string(type) + " overruns the options area");
        }
        if (!IsKnownOptionType(type) && !MaySkipUnknownTypes(features)) {
            throw std::runtime_error("unknown option type " + std::to_string(type));
        }
        at += kOptionEntryHeaderSize + length;
    }
}

[[nodiscard]] bool StartsWith(const std::string& text, std::string_view prefix) {
    return text.rfind(prefix, 0) == 0;
}

}  // namespace

bool IsStoreEntryName(const std::string& name) {
    if (StartsWith(name, ".chunkdb.lock") ||
        StartsWith(name, std::string(kStoreManifestFileName) + ".tmp.")) {
        return false;
    }
    if (StartsWith(name, "chunkdb.") || StartsWith(name, ".chunkdb.")) {
        return true;
    }
    if (!StartsWith(name, "L_")) {
        return false;
    }
    const std::string rest = name.substr(2);
    const std::size_t separator = rest.find('_');
    std::int64_t x = 0;
    std::int64_t y = 0;
    return separator != std::string::npos &&
           TryParseInt64(rest.substr(0, separator), &x) &&
           TryParseInt64(rest.substr(separator + 1), &y);
}

std::filesystem::path StoreManifestPath(const std::filesystem::path& data_dir) {
    return data_dir / std::string(kStoreManifestFileName);
}

std::vector<std::uint8_t> SerializeStoreManifest(const StoreManifest& manifest) {
    const std::size_t size = kStoreManifestMinSize + manifest.options.size();
    if (size > kStoreManifestMaxSize) {
        throw std::invalid_argument("store manifest options are too large");
    }
    std::vector<std::uint8_t> bytes;
    bytes.reserve(size);
    bytes.insert(bytes.end(), kStoreManifestMagic.begin(), kStoreManifestMagic.end());
    WriteLe16(bytes, kStoreManifestVersion);
    WriteLe16(bytes, 0U);
    WriteLe32(bytes, manifest.features.incompat);
    WriteLe32(bytes, manifest.features.ro_compat);
    WriteLe32(bytes, manifest.features.compat);
    WriteLe32(bytes, manifest.geometry.large_chunk_width_chunks);
    WriteLe32(bytes, manifest.geometry.large_chunk_height_chunks);
    WriteLe32(bytes, manifest.geometry.chunk_width_blocks);
    WriteLe32(bytes, manifest.geometry.chunk_height_blocks);
    WriteLe32(bytes, manifest.geometry.block_bits);
    bytes.insert(bytes.end(), manifest.store_id.begin(), manifest.store_id.end());
    WriteLe32(bytes, static_cast<std::uint32_t>(manifest.options.size()));
    bytes.insert(bytes.end(), manifest.options.begin(), manifest.options.end());
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    return bytes;
}

StoreManifest ParseStoreManifest(const std::vector<std::uint8_t>& bytes) {
    if (bytes.size() < 6U ||
        !std::equal(kStoreManifestMagic.begin(), kStoreManifestMagic.end(), bytes.begin())) {
        throw std::runtime_error("bad magic");
    }
    const std::uint16_t version = ReadLe16(bytes, 4U);
    if (version == 1U) {
        throw std::runtime_error(
            "manifest version 1 was written by a 2.0 development build that predates the "
            "extensible storage format; this build does not open it");
    }
    if (version != kStoreManifestVersion) {
        throw std::runtime_error(
            "unsupported manifest version " + std::to_string(version) + ", expected " +
            std::to_string(kStoreManifestVersion));
    }
    if (bytes.size() < kStoreManifestMinSize || bytes.size() > kStoreManifestMaxSize) {
        throw std::runtime_error(
            "size is " + std::to_string(bytes.size()) + " bytes, expected " +
            std::to_string(kStoreManifestMinSize) + ".." + std::to_string(kStoreManifestMaxSize));
    }
    const std::size_t crc_offset = bytes.size() - 4U;
    if (ReadLe32(bytes, crc_offset) != Crc32(bytes.data(), crc_offset)) {
        throw std::runtime_error("checksum mismatch");
    }
    if (ReadLe16(bytes, 6U) != 0U) {
        throw std::runtime_error("reserved field is not zero");
    }
    const std::uint32_t options_size = ReadLe32(bytes, kOptionsSizeOffset);
    if (options_size != crc_offset - kOptionsOffset) {
        throw std::runtime_error(
            "options size " + std::to_string(options_size) + " does not match the file size");
    }

    StoreManifest manifest;
    manifest.features = FeatureFlags{
        .incompat = ReadLe32(bytes, kFlagsOffset),
        .ro_compat = ReadLe32(bytes, kFlagsOffset + 4U),
        .compat = ReadLe32(bytes, kFlagsOffset + 8U),
    };
    manifest.geometry = GeometryConfig{
        .large_chunk_width_chunks = ReadLe32(bytes, kGeometryOffset),
        .large_chunk_height_chunks = ReadLe32(bytes, kGeometryOffset + 4U),
        .chunk_width_blocks = ReadLe32(bytes, kGeometryOffset + 8U),
        .chunk_height_blocks = ReadLe32(bytes, kGeometryOffset + 12U),
        .block_bits = ReadLe32(bytes, kGeometryOffset + 16U),
    };
    try {
        (void)Geometry(manifest.geometry);
    } catch (const std::invalid_argument& e) {
        throw std::runtime_error(std::string("invalid geometry: ") + e.what());
    }
    std::copy_n(
        bytes.begin() + static_cast<std::ptrdiff_t>(kStoreIdOffset),
        manifest.store_id.size(),
        manifest.store_id.begin());
    if (std::all_of(manifest.store_id.begin(), manifest.store_id.end(),
                    [](std::uint8_t byte) { return byte == 0U; })) {
        throw std::runtime_error("store id is zero");
    }
    manifest.options.assign(
        bytes.begin() + static_cast<std::ptrdiff_t>(kOptionsOffset),
        bytes.begin() + static_cast<std::ptrdiff_t>(crc_offset));
    ValidateOptions(manifest.options, manifest.features);
    return manifest;
}

std::optional<StoreManifest> ReadStoreManifest(const std::filesystem::path& data_dir) {
    const auto path = StoreManifestPath(data_dir);
    std::error_code status_ec;
    const auto status = std::filesystem::symlink_status(path, status_ec);
    if (status_ec == std::errc::no_such_file_or_directory ||
        (!status_ec && status.type() == std::filesystem::file_type::not_found)) {
        return std::nullopt;
    }
    if (status_ec) {
        throw std::runtime_error(
            "cannot inspect store manifest " + path.string() + ": " + status_ec.message());
    }
    if (status.type() != std::filesystem::file_type::regular) {
        throw std::runtime_error("store manifest " + path.string() + " is not a regular file");
    }
    std::error_code size_ec;
    const auto size = std::filesystem::file_size(path, size_ec);
    if (size_ec) {
        throw std::runtime_error(
            "cannot inspect store manifest " + path.string() + ": " + size_ec.message());
    }
    if (size > kStoreManifestMaxSize) {
        throw std::runtime_error(
            "store manifest " + path.string() + " is damaged (size is " + std::to_string(size) +
            " bytes, more than " + std::to_string(kStoreManifestMaxSize) +
            "); restore it from a backup of this data directory");
    }

    std::vector<std::uint8_t> bytes;
    try {
        bytes = LoadFile(path);
    } catch (const std::exception& e) {
        throw std::runtime_error(
            "cannot read store manifest " + path.string() + ": " + e.what());
    }
    try {
        return ParseStoreManifest(bytes);
    } catch (const std::exception& e) {
        throw std::runtime_error(
            "store manifest " + path.string() + " is damaged (" + e.what() +
            "); restore it from a backup of this data directory");
    }
}

std::optional<std::string> FindStoreEntry(const std::filesystem::path& data_dir) {
    std::error_code status_ec;
    const auto status = std::filesystem::status(data_dir, status_ec);
    if (status_ec == std::errc::no_such_file_or_directory ||
        (!status_ec && status.type() == std::filesystem::file_type::not_found)) {
        return std::nullopt;
    }
    if (status_ec) {
        throw std::runtime_error(
            "cannot inspect data directory " + data_dir.string() + ": " + status_ec.message());
    }
    if (status.type() != std::filesystem::file_type::directory) {
        throw std::runtime_error("data directory path is not a directory: " + data_dir.string());
    }

    std::error_code ec;
    std::filesystem::directory_iterator it(data_dir, ec);
    const std::filesystem::directory_iterator end;
    for (; !ec && it != end; it.increment(ec)) {
        const auto name = it->path().filename().string();
        if (IsStoreEntryName(name)) {
            return name;
        }
    }
    if (ec) {
        throw std::runtime_error(
            "cannot list data directory " + data_dir.string() + ": " + ec.message());
    }
    return std::nullopt;
}

StoreId NewStoreId() {
    std::random_device device;
    StoreId id{};
    while (std::all_of(id.begin(), id.end(), [](std::uint8_t byte) { return byte == 0U; })) {
        for (std::size_t i = 0; i < id.size(); i += 4U) {
            const std::uint32_t word = device();
            for (std::size_t j = 0; j < 4U; ++j) {
                id[i + j] = static_cast<std::uint8_t>((word >> (8U * j)) & 0xFFU);
            }
        }
    }
    return id;
}

std::string StoreIdHex(const StoreId& store_id) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string text;
    text.reserve(store_id.size() * 2U);
    for (const auto byte : store_id) {
        text.push_back(kDigits[byte >> 4U]);
        text.push_back(kDigits[byte & 0x0FU]);
    }
    return text;
}

std::string DescribeGeometry(const GeometryConfig& geometry) {
    return "large_chunk_width=" + std::to_string(geometry.large_chunk_width_chunks) +
           " large_chunk_height=" + std::to_string(geometry.large_chunk_height_chunks) +
           " chunk_width=" + std::to_string(geometry.chunk_width_blocks) +
           " chunk_height=" + std::to_string(geometry.chunk_height_blocks) +
           " block_bits=" + std::to_string(geometry.block_bits);
}

bool SameGeometry(const GeometryConfig& lhs, const GeometryConfig& rhs) noexcept {
    return lhs.large_chunk_width_chunks == rhs.large_chunk_width_chunks &&
           lhs.large_chunk_height_chunks == rhs.large_chunk_height_chunks &&
           lhs.chunk_width_blocks == rhs.chunk_width_blocks &&
           lhs.chunk_height_blocks == rhs.chunk_height_blocks &&
           lhs.block_bits == rhs.block_bits;
}

}  // namespace chunkdb
