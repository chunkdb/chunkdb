// CHECK TABLE: the chunkdb_verify checks of one chunk's files while the
// table serves.

#include <algorithm>
#include <exception>
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

std::vector<ChunkCoord> ChunkFileCoordinates(const std::filesystem::path& table_dir, const Geometry& geometry,
    const std::function<void()>& checkpoint) {
    std::set<ChunkCoord> coordinates;
    checkpoint();
    std::error_code error;
    for (std::filesystem::directory_iterator large(table_dir, error), end; !error && large != end; large.increment(error)) {
        checkpoint();
        LargeChunkCoord large_coord;
        if (!ParseLargeChunkName(large->path().filename().string(), geometry.dims(), &large_coord)) continue;
        std::error_code inner;
        for (std::filesystem::directory_iterator file(large->path(), inner), file_end; !inner && file != file_end;
             file.increment(inner)) {
            checkpoint();
            const auto extension = file->path().extension();
            if (extension != ".chk" && extension != ".wal") continue;
            ChunkCoord coord;
            if (ParseChunkFilePath(file->path().lexically_relative(table_dir), geometry, &coord)) coordinates.insert(coord);
        }
        // A directory collection removes while it is listed holds no chunk;
        // any other listing error would hide chunks from the check.
        if (inner && inner != std::errc::no_such_file_or_directory)
            throw std::system_error(inner, "cannot list the chunks of " + large->path().string());
    }
    if (error) throw std::system_error(error, "cannot list the chunks of " + table_dir.string());
    return {coordinates.begin(), coordinates.end()};
}

std::optional<ChunkStore::ChunkFileDamage> ChunkStore::CheckChunkFiles(const ChunkCoord& coord,
    const std::function<void()>& read_checkpoint) {
    if (access_mode_ == AccessMode::kReadOnly) throw std::invalid_argument("CHECK TABLE needs a writer");
    const auto check = [&] {
        return ::chunkdb::CheckChunkFiles(data_dir_, geometry_, store_id_, features_, coord,
            coordinate_index_->EmptyBoundary(coord), std::nullopt, true, std::nullopt, read_checkpoint);
    };
    const auto damage = [&](const ChunkFileCheck& result) -> std::optional<ChunkFileDamage> {
        const auto* found = result.damage();
        if (found == nullptr) return std::nullopt;
        return ChunkFileDamage{found->code, found->path.lexically_relative(data_dir_), found->detail};
    };
    // The exclusive lock: a background checkpoint owns the files without the
    // chunk lock, and only an exclusive holder can wait for it to finish. So
    // reads of this chunk wait for the check too.
    const auto check_cached = [&](const std::shared_ptr<RegularChunk>& loaded) {
        auto lock = LockStatementChunk(loaded->mutex);
        AwaitStatementChunkFilesLocked(lock, *loaded);
        return damage(check());
    };
    std::optional<ChunkFileDamage> last;
    for (int attempt = 0; attempt < kCheckReadAttempts; ++attempt) {
        CheckStatementCancellation();
        // Holding the chunk keeps it cached: eviction removes only a chunk
        // nothing else holds.
        if (const auto loaded = TryGetLoadedChunk(coord); loaded != nullptr) return check_cached(loaded);
        // As ReadPopulatedChunkStateNoCache: files of a chunk that is not
        // cached change only after a load, and an eviction bumps the removal
        // count after the last change to them.
        const auto removals = eviction_removals_.load(std::memory_order_acquire);
        PauseCacheForTests(CachePausePoint::kCheckTableBeforeRead, coord);
        last = damage(check());
        PauseCacheForTests(CachePausePoint::kCheckTableAfterRead, coord);
        if (TryGetLoadedChunk(coord) == nullptr && eviction_removals_.load(std::memory_order_acquire) == removals)
            return last;
    }
    // Evictions of any chunk kept racing the reads: check this one in the
    // cache. A load refuses damaged files; the damage the last read found is
    // then the report.
    if (read_checkpoint) read_checkpoint();
    std::shared_ptr<RegularChunk> loaded;
    try {
        loaded = GetOrLoadRegularChunk(coord, read_checkpoint);
    } catch (const StatementStoppedError&) {
        throw;
    } catch (const CheckTableShutdownError&) {
        throw;
    } catch (const std::exception&) {
        if (last) return last;
        throw;
    }
    std::optional<ChunkFileDamage> found;
    std::exception_ptr stopped;
    try {
        found = check_cached(loaded);
    } catch (const StatementStoppedError&) {
        stopped = std::current_exception();
    } catch (const CheckTableShutdownError&) {
        stopped = std::current_exception();
    }
    // Loaded only to read its files: evict it again unless a statement holds
    // it by now.
    loaded.reset();
    std::size_t removed = 0U;
    std::vector<LargeChunkCoord> maybe_empty;
    (void)TryEvictCandidate({geometry_.ChunkToLarge(coord), coord, 0U}, false, &removed, &maybe_empty);
    FinishEvictedChunks(removed, std::move(maybe_empty));
    if (stopped) std::rethrow_exception(stopped);
    return found;
}

}  // namespace chunkdb
