// CHECK TABLE: the chunkdb_verify checks of one chunk's files while the
// table serves.

#include <algorithm>
#include <set>
#include <system_error>

#include "chunk_check.hpp"
#include "chunk_coordinate_index.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"
#include "statement_control.hpp"

namespace chunkdb {
namespace {
// Reads of a chunk that is not cached before CHECK TABLE checks it in the cache.
constexpr int kCheckReadAttempts = 3;
}  // namespace

std::vector<ChunkCoord> ChunkFileCoordinates(const std::filesystem::path& table_dir, const Geometry& geometry) {
    std::set<ChunkCoord> coordinates;
    std::error_code error;
    for (std::filesystem::directory_iterator large(table_dir, error), end; !error && large != end; large.increment(error)) {
        LargeChunkCoord large_coord;
        if (!ParseLargeChunkName(large->path().filename().string(), geometry.dims(), &large_coord)) continue;
        // A directory collection removes while it is listed holds no chunk.
        std::error_code inner;
        for (std::filesystem::directory_iterator file(large->path(), inner), file_end; !inner && file != file_end;
             file.increment(inner)) {
            const auto extension = file->path().extension();
            if (extension != ".chk" && extension != ".wal") continue;
            ChunkCoord coord;
            if (ParseChunkFilePath(file->path().lexically_relative(table_dir), geometry, &coord)) coordinates.insert(coord);
        }
    }
    if (error) throw std::system_error(error, "cannot list the chunks of " + table_dir.string());
    return {coordinates.begin(), coordinates.end()};
}

std::optional<ChunkStore::ChunkFileDamage> ChunkStore::CheckChunkFiles(const ChunkCoord& coord) {
    if (access_mode_ == AccessMode::kReadOnly) throw std::invalid_argument("CHECK TABLE needs a writer");
    const auto check = [&] {
        return ::chunkdb::CheckChunkFiles(data_dir_, geometry_, store_id_, features_, coord,
            coordinate_index_->EmptyBoundary(coord));
    };
    const auto damage = [&](const ChunkFileCheck& result) -> std::optional<ChunkFileDamage> {
        const auto* found = result.damage();
        if (found == nullptr) return std::nullopt;
        return ChunkFileDamage{found->code, found->path.lexically_relative(data_dir_), found->detail};
    };
    const auto check_cached = [&](const std::shared_ptr<RegularChunk>& loaded) {
        auto lock = LockStatementChunk(loaded->mutex);
        AwaitStatementChunkFilesLocked(lock, *loaded);
        return damage(check());
    };
    for (int attempt = 0; attempt < kCheckReadAttempts; ++attempt) {
        CheckStatementCancellation();
        // Holding the chunk keeps it cached: eviction removes only a chunk
        // nothing else holds.
        if (const auto loaded = TryGetLoadedChunk(coord); loaded != nullptr) return check_cached(loaded);
        // As ReadPopulatedChunkStateNoCache: files of a chunk that is not
        // cached change only after a load, and an eviction bumps the removal
        // count after the last change to them.
        const auto removals = eviction_removals_.load(std::memory_order_acquire);
        auto result = check();
        if (TryGetLoadedChunk(coord) == nullptr && eviction_removals_.load(std::memory_order_acquire) == removals)
            return damage(result);
    }
    // Evictions kept racing the read: check the chunk in the cache. A load
    // refuses damaged files with the error a read of the chunk gets.
    return check_cached(GetOrLoadRegularChunk(coord));
}

}  // namespace chunkdb
