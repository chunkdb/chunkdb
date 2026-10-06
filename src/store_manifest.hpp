#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/geometry.hpp"

namespace chunkdb {

// `chunkdb.manifest` records what a data directory is: its geometry and a
// random store id. It is written once, before any other artifact, and never
// changed. Little-endian, exactly kStoreManifestSize bytes:
//   [0, 4)   magic "CKMF"
//   [4, 6)   u16 manifest version (1)
//   [6, 26)  u32 large_chunk_width, large_chunk_height, chunk_width,
//            chunk_height, block_bits
//   [26, 42) store id (16 random bytes, not all zero)
//   [42, 46) u32 CRC32 over [0, 42)
inline constexpr std::string_view kStoreManifestFileName = "chunkdb.manifest";
inline constexpr std::uint16_t kStoreManifestVersion = 1;
inline constexpr std::size_t kStoreManifestSize = 46;

struct StoreManifest {
    GeometryConfig geometry;
    StoreId store_id{};
};

[[nodiscard]] std::filesystem::path StoreManifestPath(const std::filesystem::path& data_dir);
[[nodiscard]] std::vector<std::uint8_t> SerializeStoreManifest(const StoreManifest& manifest);
// Throws std::runtime_error saying what is wrong unless `bytes` is a complete,
// checksum-valid manifest of a supported version with a valid geometry and a
// non-zero store id.
[[nodiscard]] StoreManifest ParseStoreManifest(const std::vector<std::uint8_t>& bytes);
// Returns std::nullopt only when the manifest does not exist. A manifest that
// cannot be inspected, read, or parsed is an error.
[[nodiscard]] std::optional<StoreManifest> ReadStoreManifest(const std::filesystem::path& data_dir);
// Whether a top-level data-directory entry is chunkdb state: chunk
// directories (`L_<x>_<y>`) and bookkeeping (`chunkdb.*`, `.chunkdb.*`). The
// writer lock and unpublished manifest temp files are not, since a crashed or
// concurrent initialization leaves them before the store exists. Anything
// else, such as `lost+found` on a volume root, is not chunkdb's: store code
// neither reads nor descends into it.
[[nodiscard]] bool IsStoreEntryName(const std::string& name);
// Name of the first entry of `data_dir` that is chunkdb state (chunk
// directories, bookkeeping files), or std::nullopt when there is none or the
// directory is absent. The writer lock and unpublished manifest temp files do
// not count, and neither do entries chunkdb never creates. Throws on listing
// errors.
[[nodiscard]] std::optional<std::string> FindStoreEntry(const std::filesystem::path& data_dir);
[[nodiscard]] StoreId NewStoreId();
[[nodiscard]] std::string StoreIdHex(const StoreId& store_id);
[[nodiscard]] std::string DescribeGeometry(const GeometryConfig& geometry);
[[nodiscard]] bool SameGeometry(const GeometryConfig& lhs, const GeometryConfig& rhs) noexcept;

}  // namespace chunkdb
