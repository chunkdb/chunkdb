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
#include "feature_flags.hpp"

namespace chunkdb {

// `chunkdb.manifest` records what a data directory is: its feature flags,
// geometry, a random store id and options. It is written once, before any
// other artifact. Little-endian (docs/STORAGE_FORMAT.md Section 1.1):
//   [0, 4)    magic "CKMF"
//   [4, 6)    u16 manifest version (2)
//   [6, 8)    u16 reserved, zero
//   [8, 20)   u32 incompat, ro_compat, compat feature flags
//   [20, 40)  u32 large_chunk_width, large_chunk_height, chunk_width,
//             chunk_height, block_bits
//   [40, 56)  store id (16 random bytes, not all zero)
//   [56, 60)  u32 options_size
//   [60, 60 + options_size)  options: TLV entries (u16 type, u16 length, value)
//   then      u32 CRC32 over every preceding byte
inline constexpr std::string_view kStoreManifestFileName = "chunkdb.manifest";
inline constexpr std::uint16_t kStoreManifestVersion = 2;
inline constexpr std::size_t kStoreManifestMinSize = 64;
inline constexpr std::size_t kStoreManifestMaxSize = 64U * 1024U;

struct StoreManifest {
    FeatureFlags features;
    GeometryConfig geometry;
    StoreId store_id{};
    // Raw TLV option entries. 2.0.0 defines no option types.
    std::vector<std::uint8_t> options;
};

[[nodiscard]] std::filesystem::path StoreManifestPath(const std::filesystem::path& data_dir);
[[nodiscard]] std::vector<std::uint8_t> SerializeStoreManifest(const StoreManifest& manifest);
// Throws std::runtime_error saying what is wrong unless `bytes` is a complete,
// checksum-valid manifest of this version with well-formed options (no
// unknown option type unless an unknown non-incompat feature owns it), a
// valid geometry and a non-zero store id. Feature flags are returned as they
// are; RequireOpenableFeatures decides whether the store may be opened.
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
