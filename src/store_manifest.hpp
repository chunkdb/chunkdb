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

// `table.manifest` records what a table directory (a store) is: its feature
// flags, geometry, a random store id and options. It is written once, before
// any other artifact, and replaced atomically only to change options.
// Little-endian (docs/STORAGE_FORMAT.md Section 1.2):
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
inline constexpr std::string_view kStoreManifestFileName = "table.manifest";
inline constexpr std::uint16_t kStoreManifestVersion = 2;
inline constexpr std::size_t kStoreManifestMinSize = 64;
inline constexpr std::size_t kStoreManifestMaxSize = 64U * 1024U;

// Option TLV types of a table manifest. Each appears at most once; an absent
// one takes its TableOptions default.
inline constexpr std::uint16_t kOptionDurabilityMode = 1;          // u8
inline constexpr std::uint16_t kOptionCheckpointUpdates = 2;       // u64, > 0
inline constexpr std::uint16_t kOptionCheckpointWalBytes = 3;      // u64, > 0
inline constexpr std::uint16_t kOptionWalGroupCommitUpdates = 4;   // u64, > 0
inline constexpr std::uint16_t kOptionCheckpointCompression = 5;   // u8
// Present exactly when the table has the extra-data feature.
inline constexpr std::uint16_t kOptionExtraMaxBlockBits = 6;       // u64, > 0
inline constexpr std::uint16_t kOptionExtraMaxChunkBytes = 7;      // u64, > 0
// Present exactly when the table has the history feature.
inline constexpr std::uint16_t kOptionHistoryStart = 8;            // u64, > 0
// Only with the history feature; an absent one takes its default.
inline constexpr std::uint16_t kOptionHistoryMaxAgeMs = 9;         // u64, > 0
inline constexpr std::uint16_t kOptionHistoryMaxChunkBytes = 10;   // u64, > 0
inline constexpr std::uint16_t kOptionHistoryMaxTagBytes = 11;     // u64, 1 to 255

struct StoreManifest {
    FeatureFlags features;
    GeometryConfig geometry;
    StoreId store_id{};
    // Raw TLV option entries (EncodeTableOptions).
    std::vector<std::uint8_t> options;
};

// Options area holding every option of `options` (the extra-data limits
// only when extra data is enabled, the history options only with history,
// and of those the limits only when they differ from their defaults).
[[nodiscard]] std::vector<std::uint8_t> EncodeTableOptions(const TableOptions& options);
// Throws std::runtime_error for a malformed or repeated known option,
// unusable extra-data limits, or history options without history_start. Entries of unknown types are skipped;
// ParseStoreManifest has already checked that the manifest's features allow
// them.
[[nodiscard]] TableOptions DecodeTableOptions(const std::vector<std::uint8_t>& options);

// The features a table with `options` has: kFeatureExtraData when extra data
// is enabled, kFeatureHistory with history.
[[nodiscard]] FeatureFlags TableFeatures(const TableOptions& options) noexcept;

[[nodiscard]] std::filesystem::path StoreManifestPath(const std::filesystem::path& data_dir);
[[nodiscard]] std::vector<std::uint8_t> SerializeStoreManifest(const StoreManifest& manifest);
// Throws std::runtime_error saying what is wrong unless `bytes` is a complete,
// checksum-valid manifest of this version with well-formed options (no
// unknown option type unless an unknown non-incompat feature owns it,
// extra-data limits exactly when it has the extra-data feature, and
// history_start exactly when it has the history feature), a valid
// geometry and a non-zero store id. Feature flags are returned as they
// are; RequireOpenableFeatures decides whether the store may be opened.
[[nodiscard]] StoreManifest ParseStoreManifest(const std::vector<std::uint8_t>& bytes);
// Returns std::nullopt only when the manifest does not exist. A manifest that
// cannot be inspected, read, or parsed is an error.
[[nodiscard]] std::optional<StoreManifest> ReadStoreManifest(const std::filesystem::path& data_dir);
// Whether a top-level entry of a store directory is chunkdb state: chunk
// directories (`L_<x>_<y>`), the manifest and bookkeeping (`chunkdb.*`,
// `.chunkdb.*`). The writer lock and unpublished manifest temp files are not,
// since a crashed or concurrent initialization leaves them before the store
// exists. Anything else, such as `lost+found` on a volume root, is not
// chunkdb's: store code neither reads nor descends into it.
[[nodiscard]] bool IsStoreEntryName(const std::string& name);
// Name of the first entry of `data_dir` that is chunkdb state (chunk
// directories, bookkeeping files), or std::nullopt when there is none or the
// directory is absent. The writer lock and unpublished manifest temp files do
// not count, and neither do entries chunkdb never creates. Throws on listing
// errors.
[[nodiscard]] std::optional<std::string> FindStoreEntry(const std::filesystem::path& data_dir);

// `chunkdb.manifest` records that a directory is a chunkdb data directory
// holding tables under `tables/`, with feature flags for the directory
// itself (docs/STORAGE_FORMAT.md Section 1.1). Little-endian:
//   [0, 4)    magic "CKDM"
//   [4, 6)    u16 version (1)
//   [6, 8)    u16 reserved, zero
//   [8, 20)   u32 incompat, ro_compat, compat feature flags
//   [20, 36)  data-directory id (16 random bytes, not all zero)
//   [36, 40)  u32 options_size
//   [40, 40 + options_size)  options: TLV entries (u16 type, u16 length):
//                            1 version_floor (u64, see below)
//   then      u32 CRC32 over every preceding byte
inline constexpr std::string_view kDataDirManifestFileName = "chunkdb.manifest";
inline constexpr std::uint16_t kDataDirManifestVersion = 1;
inline constexpr std::size_t kDataDirManifestMinSize = 44;
inline constexpr std::size_t kDataDirManifestMaxSize = 64U * 1024U;

struct DataDirManifest {
    FeatureFlags features;
    StoreId data_dir_id{};
    std::vector<std::uint8_t> options;
};

// Every version token a table of the directory issued is below the
// version_floor option (raised when a table is dropped); a table's version
// clock starts there, so a table dropped and created again under the same
// name never reuses a token. 0 when the option is absent.
inline constexpr std::uint16_t kDataDirOptionVersionFloor = 1;
[[nodiscard]] std::uint64_t DataDirVersionFloor(const DataDirManifest& manifest);
void SetDataDirVersionFloor(DataDirManifest* manifest, std::uint64_t floor);

[[nodiscard]] std::filesystem::path DataDirManifestPath(const std::filesystem::path& data_dir);
[[nodiscard]] std::vector<std::uint8_t> SerializeDataDirManifest(const DataDirManifest& manifest);
[[nodiscard]] DataDirManifest ParseDataDirManifest(const std::vector<std::uint8_t>& bytes);
// std::nullopt only when the manifest does not exist.
[[nodiscard]] std::optional<DataDirManifest> ReadDataDirManifest(
    const std::filesystem::path& data_dir);

[[nodiscard]] StoreId NewStoreId();
[[nodiscard]] std::string StoreIdHex(const StoreId& store_id);
[[nodiscard]] std::string DescribeGeometry(const GeometryConfig& geometry);
[[nodiscard]] bool SameGeometry(const GeometryConfig& lhs, const GeometryConfig& rhs) noexcept;

}  // namespace chunkdb
