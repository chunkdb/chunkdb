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
constexpr std::size_t kStoreManifestCrcOffset = kStoreManifestSize - 4U;
constexpr std::size_t kStoreIdOffset = 26U;

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
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kStoreManifestSize);
    bytes.insert(bytes.end(), kStoreManifestMagic.begin(), kStoreManifestMagic.end());
    WriteLe16(bytes, kStoreManifestVersion);
    WriteLe32(bytes, manifest.geometry.large_chunk_width_chunks);
    WriteLe32(bytes, manifest.geometry.large_chunk_height_chunks);
    WriteLe32(bytes, manifest.geometry.chunk_width_blocks);
    WriteLe32(bytes, manifest.geometry.chunk_height_blocks);
    WriteLe32(bytes, manifest.geometry.block_bits);
    bytes.insert(bytes.end(), manifest.store_id.begin(), manifest.store_id.end());
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    return bytes;
}

StoreManifest ParseStoreManifest(const std::vector<std::uint8_t>& bytes) {
    if (bytes.size() != kStoreManifestSize) {
        throw std::runtime_error(
            "size is " + std::to_string(bytes.size()) + " bytes, expected " +
            std::to_string(kStoreManifestSize));
    }
    if (!std::equal(kStoreManifestMagic.begin(), kStoreManifestMagic.end(), bytes.begin())) {
        throw std::runtime_error("bad magic");
    }
    if (ReadLe32(bytes, kStoreManifestCrcOffset) !=
        Crc32(bytes.data(), kStoreManifestCrcOffset)) {
        throw std::runtime_error("checksum mismatch");
    }
    const std::uint16_t version = ReadLe16(bytes, 4U);
    if (version != kStoreManifestVersion) {
        throw std::runtime_error(
            "unsupported manifest version " + std::to_string(version) + ", expected " +
            std::to_string(kStoreManifestVersion));
    }

    StoreManifest manifest;
    manifest.geometry = GeometryConfig{
        .large_chunk_width_chunks = ReadLe32(bytes, 6U),
        .large_chunk_height_chunks = ReadLe32(bytes, 10U),
        .chunk_width_blocks = ReadLe32(bytes, 14U),
        .chunk_height_blocks = ReadLe32(bytes, 18U),
        .block_bits = ReadLe32(bytes, 22U),
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
