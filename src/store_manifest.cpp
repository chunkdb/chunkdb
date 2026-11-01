#include "store_manifest.hpp"

#include <algorithm>
#include <limits>
#include <random>
#include <stdexcept>
#include <system_error>

#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"

namespace chunkdb {

namespace {

constexpr std::array<std::uint8_t, 4> kStoreManifestMagic = {'C', 'K', 'M', 'F'};
constexpr std::size_t kFlagsOffset = 8U;
constexpr std::size_t kDimensionsOffset = 20U;
constexpr std::size_t kGeometryOffset = 24U;
constexpr std::size_t kStoreIdOffset = 48U;
constexpr std::size_t kOptionsSizeOffset = 64U;
constexpr std::size_t kOptionsOffset = 68U;
constexpr std::size_t kOptionEntryHeaderSize = 4U;

constexpr std::array<std::uint8_t, 4> kDataDirManifestMagic = {'C', 'K', 'D', 'M'};
constexpr std::size_t kDataDirIdOffset = 20U;
constexpr std::size_t kDataDirOptionsSizeOffset = 36U;
constexpr std::size_t kDataDirOptionsOffset = 40U;

[[nodiscard]] bool IsKnownTableOptionType(std::uint16_t type) noexcept {
    return (type >= kOptionDurabilityMode && type <= kOptionVarMaxChunkBytes) ||
           type == kOptionFeedBufferBytes || type == kOptionSlotMaxBytes;
}

[[nodiscard]] bool IsKnownTableManifestType(std::uint16_t type) noexcept {
    return IsKnownTableOptionType(type) || type == kOptionParentEpoch ||
           type == kOptionForkRevision;
}

void RequireValidLineage(const StoreManifest& manifest) {
    if (!manifest.lineage) return;
    if ((manifest.features.incompat & kFeatureTableLineage) == 0U)
        throw std::invalid_argument("lineage metadata requires table_lineage feature");
    const auto& parent = manifest.lineage->parent_epoch;
    if (parent == StoreId{}) throw std::invalid_argument("lineage parent epoch is zero");
    if (parent == manifest.store_id)
        throw std::invalid_argument("lineage parent epoch equals the table epoch");
}

// Remove only owned lineage TLVs. All mutable and unknown options survive.
void DecodeLineage(StoreManifest* manifest) {
    std::optional<StoreId> parent;
    std::optional<std::uint64_t> fork;
    for (std::size_t at = 0; at < manifest->options.size();) {
        const auto type = ReadLe16(manifest->options, at);
        const auto length = ReadLe16(manifest->options, at + 2U);
        const auto next = at + kOptionEntryHeaderSize + length;
        if (type == kOptionParentEpoch) {
            if (parent) throw std::runtime_error("lineage parent epoch appears twice");
            if (length != StoreId{}.size()) throw std::runtime_error("lineage parent epoch has invalid length");
            parent.emplace();
            std::copy_n(manifest->options.begin() + static_cast<std::ptrdiff_t>(at + 4U),
                parent->size(), parent->begin());
        } else if (type == kOptionForkRevision) {
            if (fork) throw std::runtime_error("lineage fork revision appears twice");
            if (length != 8U) throw std::runtime_error("lineage fork revision has invalid length");
            fork = ReadLe64(manifest->options, at + 4U);
        }
        at = next;
    }
    if (parent.has_value() != fork.has_value())
        throw std::runtime_error("lineage parent epoch and fork revision must both be present");
    if (!parent) return;
    manifest->lineage = TableLineage{*parent, *fork};
    try { RequireValidLineage(*manifest); }
    catch (const std::invalid_argument& e) { throw std::runtime_error(e.what()); }
    std::vector<std::uint8_t> options;
    options.reserve(manifest->options.size() - 32U);
    for (std::size_t at = 0; at < manifest->options.size();) {
        const auto type = ReadLe16(manifest->options, at);
        const auto next = at + kOptionEntryHeaderSize + ReadLe16(manifest->options, at + 2U);
        if (type != kOptionParentEpoch && type != kOptionForkRevision)
            options.insert(options.end(), manifest->options.begin() + static_cast<std::ptrdiff_t>(at),
                manifest->options.begin() + static_cast<std::ptrdiff_t>(next));
        at = next;
    }
    manifest->options = std::move(options);
}

// Walks a TLV options area: every entry must lie inside it, and an entry of a
// type `is_known` rejects is allowed only when the manifest carries a feature
// this build does not know (which may own that type).
template <typename IsKnown>
void ValidateOptions(
    const std::vector<std::uint8_t>& options,
    const FeatureFlags& features,
    IsKnown is_known) {
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
        if (!is_known(type) && !MaySkipUnknownTypes(features)) {
            throw std::runtime_error("unknown option type " + std::to_string(type));
        }
        at += kOptionEntryHeaderSize + length;
    }
}

void AppendOption(std::vector<std::uint8_t>& out, std::uint16_t type, std::uint8_t value) {
    WriteLe16(out, type);
    WriteLe16(out, 1U);
    out.push_back(value);
}

void AppendOption(std::vector<std::uint8_t>& out, std::uint16_t type, std::uint64_t value) {
    WriteLe16(out, type);
    WriteLe16(out, 8U);
    WriteLe64(out, value);
}

// Returns the file's bytes, or std::nullopt when the file does not exist.
std::optional<std::vector<std::uint8_t>> ReadManifestFile(
    const std::filesystem::path& path,
    std::size_t max_size,
    const char* what) {
    std::error_code status_ec;
    const auto status = std::filesystem::symlink_status(path, status_ec);
    if (status_ec == std::errc::no_such_file_or_directory ||
        (!status_ec && status.type() == std::filesystem::file_type::not_found)) {
        return std::nullopt;
    }
    if (status_ec) {
        throw std::runtime_error(
            std::string("cannot inspect ") + what + " " + path.string() + ": " +
            status_ec.message());
    }
    if (status.type() != std::filesystem::file_type::regular) {
        throw std::runtime_error(std::string(what) + " " + path.string() + " is not a regular file");
    }
    std::error_code size_ec;
    const auto size = std::filesystem::file_size(path, size_ec);
    if (size_ec) {
        throw std::runtime_error(
            std::string("cannot inspect ") + what + " " + path.string() + ": " +
            size_ec.message());
    }
    if (size > max_size) {
        throw std::runtime_error(
            std::string(what) + " " + path.string() + " is damaged (size is " +
            std::to_string(size) + " bytes, more than " + std::to_string(max_size) +
            "); restore it from a backup of this data directory");
    }
    try {
        return LoadFile(path);
    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("cannot read ") + what + " " + path.string() + ": " + e.what());
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
    if (name == kStoreManifestFileName || StartsWith(name, "chunkdb.") ||
        StartsWith(name, ".chunkdb.")) {
        return true;
    }
    LargeChunkCoord three_axes;
    if (ParseLargeChunkName(name, 3, &three_axes)) return true;
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

void AlignManifestSchemaDomains(
    StoreManifest* manifest, const TableSchema& previous_blocks, const TableSchema& previous_chunks) {
    if (!manifest->chunk_schema) throw std::invalid_argument("chunk schema domain is missing");
    auto& blocks = manifest->schema;
    auto& chunks = *manifest->chunk_schema;
    if (blocks.version < previous_blocks.version || chunks.version < previous_chunks.version)
        throw std::invalid_argument("schema revisions cannot move backwards");
    if (blocks.version > chunks.version) {
        if (chunks != previous_chunks) throw std::invalid_argument("both column domains changed at different revisions");
        while (chunks.version < blocks.version) chunks = AdvanceOtherDomain(chunks, ColumnDomain::kChunk);
    } else if (chunks.version > blocks.version) {
        if (blocks != previous_blocks) throw std::invalid_argument("both column domains changed at different revisions");
        while (blocks.version < chunks.version) blocks = AdvanceOtherDomain(blocks, ColumnDomain::kBlock);
    }
    ValidateSchemaDomains(blocks, chunks);
}

std::vector<std::uint8_t> SerializeStoreManifest(const StoreManifest& manifest) {
    RequireValidLineage(manifest);
    if ((manifest.features.incompat & kFeatureChunkModel) == 0)
        throw std::invalid_argument("table manifest lacks chunk_model feature");
    const auto chunk_schema = manifest.chunk_schema ? *manifest.chunk_schema : EmptyChunkSchemaFor(manifest.schema);
    ValidateSchemaDomains(manifest.schema, chunk_schema);
    if (manifest.geometry.block_bits != FixedBitsPerBlock(manifest.schema)) {
        throw std::invalid_argument(
            "store manifest geometry block_bits " + std::to_string(manifest.geometry.block_bits) +
            " does not match the schema's " + std::to_string(FixedBitsPerBlock(manifest.schema)) +
            " fixed bits per block");
    }
    (void)Geometry(manifest.geometry, manifest.schema, chunk_schema);
    const auto schema = EncodeTableSchema(manifest.schema);
    const auto encoded_chunk_schema = EncodeChunkSchema(chunk_schema);
    const auto lineage_bytes = manifest.lineage ? 32U : 0U;
    if (manifest.options.size() > kStoreManifestMaxSize - lineage_bytes)
        throw std::invalid_argument("store manifest options and schema are too large");
    const auto options_size = manifest.options.size() + lineage_bytes;
    if (options_size > kStoreManifestMaxSize - kStoreManifestMinSize ||
        schema.size() > kStoreManifestMaxSize - kStoreManifestMinSize - options_size ||
        encoded_chunk_schema.size() > kStoreManifestMaxSize - kStoreManifestMinSize - options_size - schema.size()) {
        throw std::invalid_argument("store manifest options and schema are too large");
    }
    const std::size_t size = kStoreManifestMinSize + options_size + schema.size() + encoded_chunk_schema.size();
    std::vector<std::uint8_t> bytes;
    bytes.reserve(size);
    bytes.insert(bytes.end(), kStoreManifestMagic.begin(), kStoreManifestMagic.end());
    WriteLe16(bytes, kStoreManifestVersion);
    WriteLe16(bytes, 0U);
    WriteLe32(bytes, manifest.features.incompat);
    WriteLe32(bytes, manifest.features.ro_compat);
    WriteLe32(bytes, manifest.features.compat);
    bytes.push_back(manifest.geometry.dims);
    bytes.insert(bytes.end(), 3, 0);
    WriteLe32(bytes, manifest.geometry.large_chunk_width_chunks);
    WriteLe32(bytes, manifest.geometry.large_chunk_height_chunks);
    WriteLe32(bytes, manifest.geometry.large_chunk_depth_chunks);
    WriteLe32(bytes, manifest.geometry.chunk_width_blocks);
    WriteLe32(bytes, manifest.geometry.chunk_height_blocks);
    WriteLe32(bytes, manifest.geometry.chunk_depth_blocks);
    bytes.insert(bytes.end(), manifest.store_id.begin(), manifest.store_id.end());
    WriteLe32(bytes, static_cast<std::uint32_t>(options_size));
    bytes.insert(bytes.end(), manifest.options.begin(), manifest.options.end());
    if (manifest.lineage) {
        WriteLe16(bytes, kOptionParentEpoch);
        WriteLe16(bytes, static_cast<std::uint16_t>(manifest.lineage->parent_epoch.size()));
        bytes.insert(bytes.end(), manifest.lineage->parent_epoch.begin(), manifest.lineage->parent_epoch.end());
        AppendOption(bytes, kOptionForkRevision, manifest.lineage->fork_revision);
    }
    WriteLe32(bytes, static_cast<std::uint32_t>(schema.size()));
    bytes.insert(bytes.end(), schema.begin(), schema.end());
    WriteLe32(bytes, static_cast<std::uint32_t>(encoded_chunk_schema.size()));
    bytes.insert(bytes.end(), encoded_chunk_schema.begin(), encoded_chunk_schema.end());
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    return bytes;
}

StoreManifest ParseStoreManifest(const std::vector<std::uint8_t>& bytes) {
    if (bytes.size() < 6U ||
        !std::equal(kStoreManifestMagic.begin(), kStoreManifestMagic.end(), bytes.begin())) {
        throw std::runtime_error("bad magic");
    }
    const std::uint16_t version = ReadLe16(bytes, 4U);
    if (version >= 1U && version < kStoreManifestVersion) {
        throw EarlierDevelopmentFormatError();
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
    if (options_size > crc_offset - kOptionsOffset - 8U) {
        throw std::runtime_error(
            "options size " + std::to_string(options_size) + " overruns the file");
    }
    const std::size_t schema_size_offset = kOptionsOffset + options_size;
    const std::uint32_t schema_size = ReadLe32(bytes, schema_size_offset);
    if (schema_size > crc_offset - schema_size_offset - 8U) {
        throw std::runtime_error(
            "schema size " + std::to_string(schema_size) + " overruns the file");
    }
    const auto chunk_schema_size_offset = schema_size_offset + 4U + schema_size;
    const auto chunk_schema_size = ReadLe32(bytes, chunk_schema_size_offset);
    if (chunk_schema_size != crc_offset - chunk_schema_size_offset - 4U)
        throw std::runtime_error("chunk schema size does not match the file size");

    StoreManifest manifest;
    manifest.features = FeatureFlags{
        .incompat = ReadLe32(bytes, kFlagsOffset),
        .ro_compat = ReadLe32(bytes, kFlagsOffset + 4U),
        .compat = ReadLe32(bytes, kFlagsOffset + 8U),
    };
    if ((manifest.features.incompat & kFeatureChunkModel) == 0)
        throw std::runtime_error("table manifest lacks chunk_model feature");
    if (bytes[kDimensionsOffset + 1U] != 0 || bytes[kDimensionsOffset + 2U] != 0 ||
        bytes[kDimensionsOffset + 3U] != 0)
        throw std::runtime_error("geometry reserved fields are nonzero");
    manifest.geometry = GeometryConfig{
        .large_chunk_width_chunks = ReadLe32(bytes, kGeometryOffset),
        .large_chunk_height_chunks = ReadLe32(bytes, kGeometryOffset + 4U),
        .chunk_width_blocks = ReadLe32(bytes, kGeometryOffset + 12U),
        .chunk_height_blocks = ReadLe32(bytes, kGeometryOffset + 16U),
        .dims = bytes[kDimensionsOffset],
        .chunk_depth_blocks = ReadLe32(bytes, kGeometryOffset + 20U),
        .large_chunk_depth_chunks = ReadLe32(bytes, kGeometryOffset + 8U),
    };
    try {
        manifest.schema = DecodeTableSchema(bytes.data() + schema_size_offset + 4U, schema_size);
        manifest.chunk_schema = DecodeChunkSchema(bytes.data() + chunk_schema_size_offset + 4U, chunk_schema_size);
        ValidateSchemaDomains(manifest.schema, *manifest.chunk_schema);
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("invalid schema: ") + e.what());
    }
    if (const auto reason = UnsupportedSchemaReason(manifest.schema); !reason.empty()) {
        throw std::runtime_error("unsupported schema: " + reason);
    }
    manifest.geometry.block_bits = FixedBitsPerBlock(manifest.schema);
    try {
        (void)Geometry(manifest.geometry, manifest.schema, *manifest.chunk_schema);
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
        bytes.begin() + static_cast<std::ptrdiff_t>(schema_size_offset));
    ValidateOptions(manifest.options, manifest.features, IsKnownTableManifestType);
    DecodeLineage(&manifest);
    (void)DecodeTableOptions(manifest.options);
    return manifest;
}

std::optional<StoreManifest> ReadStoreManifest(const std::filesystem::path& data_dir) {
    const auto path = StoreManifestPath(data_dir);
    const auto bytes = ReadManifestFile(path, kStoreManifestMaxSize, "table manifest");
    if (!bytes.has_value()) {
        return std::nullopt;
    }
    try {
        return ParseStoreManifest(*bytes);
    } catch (const EarlierDevelopmentFormatError&) {
        throw;
    } catch (const std::exception& e) {
        throw std::runtime_error(
            "table manifest " + path.string() + " is damaged (" + e.what() +
            "); restore it from a backup of this data directory");
    }
}

std::vector<std::uint8_t> EncodeTableOptions(const TableOptions& options) {
    std::vector<std::uint8_t> out;
    AppendOption(out, kOptionDurabilityMode, static_cast<std::uint8_t>(options.durability_mode));
    AppendOption(
        out, kOptionCheckpointUpdates,
        static_cast<std::uint64_t>(options.checkpoint_update_interval));
    AppendOption(
        out, kOptionCheckpointWalBytes, static_cast<std::uint64_t>(options.checkpoint_wal_bytes));
    AppendOption(
        out, kOptionWalGroupCommitUpdates,
        static_cast<std::uint64_t>(options.wal_group_commit_updates));
    AppendOption(
        out, kOptionCheckpointCompression,
        static_cast<std::uint8_t>(options.checkpoint_compression));
    AppendOption(out, kOptionVarMaxChunkBytes, static_cast<std::uint64_t>(options.var_max_chunk_bytes));
    if (options.feed_buffer_bytes) AppendOption(out, kOptionFeedBufferBytes, static_cast<std::uint64_t>(*options.feed_buffer_bytes));
    if (options.slot_max_bytes) AppendOption(out, kOptionSlotMaxBytes, static_cast<std::uint64_t>(*options.slot_max_bytes));
    return out;
}

void ReplaceTableOptions(StoreManifest* manifest, const TableOptions& options) {
    ValidateOptions(manifest->options, manifest->features, IsKnownTableOptionType);
    auto replacement = EncodeTableOptions(options);
    for (std::size_t at = 0; at < manifest->options.size();) {
        const auto type = ReadLe16(manifest->options, at);
        const auto next = at + kOptionEntryHeaderSize + ReadLe16(manifest->options, at + 2U);
        if (!IsKnownTableOptionType(type))
            replacement.insert(replacement.end(), manifest->options.begin() + static_cast<std::ptrdiff_t>(at),
                manifest->options.begin() + static_cast<std::ptrdiff_t>(next));
        at = next;
    }
    manifest->options = std::move(replacement);
}

TableOptions DecodeTableOptions(const std::vector<std::uint8_t>& options) {
    TableOptions decoded;
    std::uint32_t seen = 0;
    std::size_t at = 0;
    while (at < options.size()) {
        if (options.size() - at < kOptionEntryHeaderSize) {
            throw std::runtime_error("truncated option entry");
        }
        const std::uint16_t type = ReadLe16(options, at);
        const std::uint16_t length = ReadLe16(options, at + 2U);
        const std::size_t value_at = at + kOptionEntryHeaderSize;
        if (options.size() - value_at < length) {
            throw std::runtime_error("option " + std::to_string(type) + " overruns the options area");
        }
        at = value_at + length;
        if (!IsKnownTableOptionType(type)) {
            continue;
        }
        const std::uint32_t bit = 1U << type;
        if ((seen & bit) != 0U) {
            throw std::runtime_error("option " + std::to_string(type) + " appears twice");
        }
        seen |= bit;
        const bool is_u8 =
            type == kOptionDurabilityMode || type == kOptionCheckpointCompression;
        if (length != (is_u8 ? 1U : 8U)) {
            throw std::runtime_error(
                "option " + std::to_string(type) + " has length " + std::to_string(length));
        }
        if (is_u8) {
            const std::uint8_t value = options[value_at];
            if (type == kOptionDurabilityMode) {
                if (value > static_cast<std::uint8_t>(DurabilityMode::kFsyncCheckpoint)) {
                    throw std::runtime_error("unknown durability mode " + std::to_string(value));
                }
                decoded.durability_mode = static_cast<DurabilityMode>(value);
            } else {
                if (value > static_cast<std::uint8_t>(CheckpointCompression::kZrle)) {
                    throw std::runtime_error(
                        "unknown checkpoint compression " + std::to_string(value));
                }
                decoded.checkpoint_compression = static_cast<CheckpointCompression>(value);
            }
            continue;
        }
        const std::uint64_t value = ReadLe64(options, value_at);
        if (value == 0U || value > std::numeric_limits<std::size_t>::max()) {
            throw std::runtime_error(
                "option " + std::to_string(type) + " has value " + std::to_string(value));
        }
        const auto size_value = static_cast<std::size_t>(value);
        if (type == kOptionCheckpointUpdates) {
            decoded.checkpoint_update_interval = size_value;
        } else if (type == kOptionCheckpointWalBytes) {
            decoded.checkpoint_wal_bytes = size_value;
        } else if (type == kOptionWalGroupCommitUpdates) {
            decoded.wal_group_commit_updates = size_value;
        } else if (type == kOptionFeedBufferBytes) {
            decoded.feed_buffer_bytes = size_value;
        } else if (type == kOptionSlotMaxBytes) {
            decoded.slot_max_bytes = size_value;
        } else {
            try {
                RequireValidVarLimit(size_value);
            } catch (const std::invalid_argument& e) {
                throw std::runtime_error(e.what());
            }
            decoded.var_max_chunk_bytes = size_value;
        }
    }
    return decoded;
}

std::filesystem::path DataDirManifestPath(const std::filesystem::path& data_dir) {
    return data_dir / std::string(kDataDirManifestFileName);
}

std::vector<std::uint8_t> SerializeDataDirManifest(const DataDirManifest& manifest) {
    if ((manifest.features.incompat & kFeatureChunkModel) == 0)
        throw std::invalid_argument("data directory manifest lacks chunk_model feature");
    if (manifest.options.size() > kDataDirManifestMaxSize - kDataDirManifestMinSize) {
        throw std::invalid_argument("data directory manifest options are too large");
    }
    const std::size_t size = kDataDirManifestMinSize + manifest.options.size();
    std::vector<std::uint8_t> bytes;
    bytes.reserve(size);
    bytes.insert(bytes.end(), kDataDirManifestMagic.begin(), kDataDirManifestMagic.end());
    WriteLe16(bytes, kDataDirManifestVersion);
    WriteLe16(bytes, 0U);
    WriteLe32(bytes, manifest.features.incompat);
    WriteLe32(bytes, manifest.features.ro_compat);
    WriteLe32(bytes, manifest.features.compat);
    bytes.insert(bytes.end(), manifest.data_dir_id.begin(), manifest.data_dir_id.end());
    WriteLe32(bytes, static_cast<std::uint32_t>(manifest.options.size()));
    bytes.insert(bytes.end(), manifest.options.begin(), manifest.options.end());
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    return bytes;
}

DataDirManifest ParseDataDirManifest(const std::vector<std::uint8_t>& bytes) {
    if (bytes.size() >= 4U &&
        std::equal(kStoreManifestMagic.begin(), kStoreManifestMagic.end(), bytes.begin())) {
        throw EarlierDevelopmentFormatError();
    }
    if (bytes.size() < 6U ||
        !std::equal(kDataDirManifestMagic.begin(), kDataDirManifestMagic.end(), bytes.begin())) {
        throw std::runtime_error("bad magic");
    }
    const std::uint16_t version = ReadLe16(bytes, 4U);
    if (version != 0 && version < kDataDirManifestVersion) throw EarlierDevelopmentFormatError();
    if (version != kDataDirManifestVersion) {
        throw std::runtime_error(
            "unsupported version " + std::to_string(version) + ", expected " +
            std::to_string(kDataDirManifestVersion));
    }
    if (bytes.size() < kDataDirManifestMinSize || bytes.size() > kDataDirManifestMaxSize) {
        throw std::runtime_error(
            "size is " + std::to_string(bytes.size()) + " bytes, expected " +
            std::to_string(kDataDirManifestMinSize) + ".." +
            std::to_string(kDataDirManifestMaxSize));
    }
    const std::size_t crc_offset = bytes.size() - 4U;
    if (ReadLe32(bytes, crc_offset) != Crc32(bytes.data(), crc_offset)) {
        throw std::runtime_error("checksum mismatch");
    }
    if (ReadLe16(bytes, 6U) != 0U) {
        throw std::runtime_error("reserved field is not zero");
    }
    const std::uint32_t options_size = ReadLe32(bytes, kDataDirOptionsSizeOffset);
    if (options_size != crc_offset - kDataDirOptionsOffset) {
        throw std::runtime_error(
            "options size " + std::to_string(options_size) + " does not match the file size");
    }
    DataDirManifest manifest;
    manifest.features = FeatureFlags{
        .incompat = ReadLe32(bytes, kFlagsOffset),
        .ro_compat = ReadLe32(bytes, kFlagsOffset + 4U),
        .compat = ReadLe32(bytes, kFlagsOffset + 8U),
    };
    if ((manifest.features.incompat & kFeatureChunkModel) == 0)
        throw std::runtime_error("data directory manifest lacks chunk_model feature");
    std::copy_n(
        bytes.begin() + static_cast<std::ptrdiff_t>(kDataDirIdOffset),
        manifest.data_dir_id.size(),
        manifest.data_dir_id.begin());
    if (std::all_of(manifest.data_dir_id.begin(), manifest.data_dir_id.end(),
                    [](std::uint8_t byte) { return byte == 0U; })) {
        throw std::runtime_error("data directory id is zero");
    }
    manifest.options.assign(
        bytes.begin() + static_cast<std::ptrdiff_t>(kDataDirOptionsOffset),
        bytes.begin() + static_cast<std::ptrdiff_t>(crc_offset));
    ValidateOptions(manifest.options, manifest.features, [](std::uint16_t type) {
        return type == kDataDirOptionVersionFloor;
    });
    (void)DataDirVersionFloor(manifest);  // checks its length and that it appears once
    return manifest;
}

std::uint64_t DataDirVersionFloor(const DataDirManifest& manifest) {
    const auto& options = manifest.options;
    std::optional<std::uint64_t> floor;
    for (std::size_t at = 0; at + kOptionEntryHeaderSize <= options.size();) {
        const std::uint16_t type = ReadLe16(options, at);
        const std::uint16_t length = ReadLe16(options, at + 2U);
        const std::size_t value_at = at + kOptionEntryHeaderSize;
        at = value_at + length;
        if (type != kDataDirOptionVersionFloor) {
            continue;
        }
        if (length != 8U || at > options.size()) {
            throw std::runtime_error("version_floor option has length " + std::to_string(length));
        }
        if (floor.has_value()) {
            throw std::runtime_error("version_floor option appears twice");
        }
        floor = ReadLe64(options, value_at);
    }
    return floor.value_or(0U);
}

void SetDataDirVersionFloor(DataDirManifest* manifest, std::uint64_t floor) {
    std::vector<std::uint8_t> kept;
    const auto& options = manifest->options;
    for (std::size_t at = 0; at + kOptionEntryHeaderSize <= options.size();) {
        const std::uint16_t type = ReadLe16(options, at);
        const std::size_t end = at + kOptionEntryHeaderSize + ReadLe16(options, at + 2U);
        if (end > options.size()) {
            throw std::runtime_error("version_floor rewrite found a truncated option entry");
        }
        if (type != kDataDirOptionVersionFloor) {
            kept.insert(kept.end(), options.begin() + static_cast<std::ptrdiff_t>(at),
                        options.begin() + static_cast<std::ptrdiff_t>(end));
        }
        at = end;
    }
    AppendOption(kept, kDataDirOptionVersionFloor, floor);
    manifest->options = std::move(kept);
}

std::optional<DataDirManifest> ReadDataDirManifest(const std::filesystem::path& data_dir) {
    const auto path = DataDirManifestPath(data_dir);
    const auto bytes =
        ReadManifestFile(path, kDataDirManifestMaxSize, "data directory manifest");
    if (!bytes.has_value()) {
        return std::nullopt;
    }
    try {
        return ParseDataDirManifest(*bytes);
    } catch (const EarlierDevelopmentFormatError&) {
        throw;
    } catch (const std::exception& e) {
        throw std::runtime_error(
            "data directory manifest " + path.string() + " is not usable (" + e.what() + ")");
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
    return lhs.dims == rhs.dims &&
           lhs.large_chunk_depth_chunks == rhs.large_chunk_depth_chunks &&
           lhs.chunk_depth_blocks == rhs.chunk_depth_blocks &&
           lhs.large_chunk_width_chunks == rhs.large_chunk_width_chunks &&
           lhs.large_chunk_height_chunks == rhs.large_chunk_height_chunks &&
           lhs.chunk_width_blocks == rhs.chunk_width_blocks &&
           lhs.chunk_height_blocks == rhs.chunk_height_blocks &&
           lhs.block_bits == rhs.block_bits;
}

}  // namespace chunkdb
