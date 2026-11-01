#include "chunk_store_internal.hpp"

#include <stdexcept>

#include "chunk_coordinate_index.hpp"
#include "chunkdb/file_layout.hpp"
#include "shared_file_input.hpp"
#include "wal_replay.hpp"

namespace chunkdb {

std::vector<std::uint8_t> LoadRecoveryTxnIntent(const std::filesystem::path& path) {
    SharedFileInput input(path);
    input.seekg(0, std::ios::end);
    const auto extent = input.tellg();
    constexpr auto maximum = 40U + kMaxTxnWrittenChunks * 32U;
    if (extent < 0 || static_cast<std::uint64_t>(extent) > maximum)
        throw std::runtime_error("transaction intent exceeds its bounded record size: " + path.string());
    input.seekg(0);
    if (!input) throw std::runtime_error("failed to rewind transaction intent: " + path.string());
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(extent));
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (input.gcount() != static_cast<std::streamsize>(bytes.size()) || input.peek() != std::char_traits<char>::eof())
        throw std::runtime_error("transaction intent changed while being read: " + path.string());
    return bytes;
}

void ValidateRollbackWalBoundary(const std::filesystem::path& data_dir, const Geometry& geometry,
    const StoreId& table_id, FeatureFlags features, const ChunkCoord& coord, std::uint64_t boundary,
    const ChunkIndexRecord* retained) {
    const auto wal_path = ChunkWalPath(data_dir, geometry, coord);
    std::error_code error;
    const auto status = std::filesystem::symlink_status(wal_path, error);
    if (error && error != std::errc::no_such_file_or_directory)
        throw std::runtime_error("failed to inspect rollback WAL " + wal_path.string() + ": " + error.message());
    if (!std::filesystem::exists(status)) {
        if (boundary != 0U) throw std::runtime_error("missing rollback WAL " + wal_path.string());
        return;
    }
    if (!std::filesystem::is_regular_file(status))
        throw std::runtime_error("rollback WAL is not a regular file: " + wal_path.string());
    const auto size = std::filesystem::file_size(wal_path, error);
    if (error || size < boundary)
        throw std::runtime_error("rollback WAL is shorter than its committed boundary: " + wal_path.string());
    if (boundary == 0U) return;
    SharedFileInput input(wal_path);
    ChunkState state;
    std::uint64_t schema = 0U;
    const auto image_path = ChunkDataPath(data_dir, geometry, coord);
    if (std::filesystem::exists(image_path)) {
        auto image = ParseChunkImage(LoadChunkImageFile(image_path, geometry, coord, table_id, features),
            geometry, coord, table_id, features);
        schema = image.schema_version;
        state.version = image.revision;
        state.written = image.revision != 0U;
        state.commit_time_ms = image.commit_time_ms;
        state.payload = std::move(image.payload);
        state.presence_bitmap = std::move(image.presence_bitmap);
        state.vars = std::move(image.vars);
        state.chunk_columns = std::move(image.chunk_columns);
    }
    if (retained) {
        (void)ApplyRetainedEmptyBase(geometry, *retained, state.version, state.commit_time_ms, schema,
            state.payload, state.presence_bitmap, state.vars, state.chunk_columns);
        state.written = state.version != 0U;
    }
    ValidateWalPrefix(input, boundary, geometry, coord, table_id, features, std::move(state), schema);
}

}  // namespace chunkdb
