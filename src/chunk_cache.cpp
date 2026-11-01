#include "chunkdb/chunk_store.hpp"

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunk_coordinate_index.hpp"
#include "eviction.hpp"
#include "feed_slots.hpp"
#include "process_lock.hpp"
#include "wal_replay.hpp"
#include "wal_stream_pool.hpp"
#include "wal_writer.hpp"
#include "shared_file_input.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <vector>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/zrle.hpp"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <stdio.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <unistd.h>
#endif

#include "snapshot_generation.hpp"
#include "txn_history.hpp"

namespace chunkdb {

std::shared_ptr<ChunkStore::LargeChunk> ChunkStore::GetOrCreateLargeChunk(const LargeChunkCoord& large_coord) {
    std::lock_guard lock(large_chunks_mutex_);
    auto it = large_chunks_.find(large_coord);
    if (it != large_chunks_.end()) {
        return it->second;
    }

    if (scan_catalog_ready_) {
        scan_catalog_.try_emplace(
            std::make_pair(large_coord.x, large_coord.y),
            LargeChunkDirectory(data_dir_, large_coord));
    }
    auto created = std::make_shared<LargeChunk>();
    large_chunks_.emplace(large_coord, created);
    eviction_large_chunk_ring_.push_back(large_coord);
    return created;
}

std::shared_ptr<ChunkStore::RegularChunk> ChunkStore::GetOrLoadRegularChunk(const ChunkCoord& chunk_coord) {
    const LargeChunkCoord large_coord = geometry_.ChunkToLarge(chunk_coord);

    bool inserted = false;
    std::shared_ptr<RegularChunk> selected;
    std::shared_ptr<LargeChunk> large_chunk;
    // A read-only store loads a missing chunk with the large chunk unlocked:
    // the load can wait for a writer in another process, and this process
    // changes nothing, so a copy loaded meanwhile by another read is as good
    // as this one, and RefreshReadOnlyChunk checks whichever is kept.
    std::optional<LoadedChunkPayload> read_only_load;
    while (true) {
        large_chunk = GetOrCreateLargeChunk(large_coord);
        std::unique_lock lock(large_chunk->mutex);
        if (large_chunk->retired) {
            // The empty-container cleanup dropped this object from
            // `large_chunks_` while we were waiting for its mutex. Inserting
            // here would strand the chunk in an unreachable container and let
            // a later lookup create a second live instance of it.
            continue;
        }
        auto it = large_chunk->chunks.find(chunk_coord);
        if (it != large_chunk->chunks.end()) {
            selected = it->second;
        } else {
            if (access_mode_ == AccessMode::kReadOnly && !read_only_load.has_value()) {
                lock.unlock();
                read_only_load = LoadChunkPayload(chunk_coord, true);
                continue;
            }
            auto loaded = read_only_load.has_value() ? std::move(*read_only_load) : LoadChunkPayload(chunk_coord, true);
            // An image without a WAL may be of an earlier schema version.
            BringChunkColumnsToSchema(geometry_, loaded.schema_version, geometry_.chunk_schema().version, &loaded.chunk_columns);
            BringToCurrentSchema(geometry_, loaded.schema_version, loaded.presence_bitmap, &loaded.payload, &loaded.vars);
            selected = std::make_shared<RegularChunk>(
                std::move(loaded.payload), std::move(loaded.presence_bitmap));
            selected->vars = std::move(loaded.vars);
            selected->chunk_columns = std::move(loaded.chunk_columns);
            // The persisted revision survives eviction and restart, so
            // versions do not change on reload. A chunk without a producing
            // commit keeps revision zero and consumes no write reservation.
            if (loaded.revision != 0) {
                RaiseVersionClockAbove(loaded.revision);
            }
            selected->version = loaded.revision;
            // A surviving header or interrupted creation is not a committed mutation.
            selected->written = loaded.revision != 0;
            selected->commit_time_ms = loaded.commit_time_ms;
            selected->wal_bytes = loaded.wal_bytes;
            selected->checkpoint_due_armed = loaded.wal_bytes >= checkpoint_wal_bytes_;
            selected->deferred_wal_compaction = loaded.deferred_wal_compaction;
            selected->wal_header_written = loaded.wal_header_written;
            selected->wal_path = loaded.wal_path;
            selected->read_only_generation = loaded.read_only_generation;
            large_chunk->chunks.emplace(chunk_coord, selected);
            inserted = true;
            // Two live objects for one chunk mean writes made through the
            // older one can be discarded by whoever checkpoints from the
            // newer, silently losing acknowledged writes. It is cheap to
            // notice here (one map operation per load, next to disk I/O) and
            // otherwise only shows up as a rare wrong value under eviction
            // pressure, so it is checked in every build rather than left to
            // a stress test to stumble over.
            {
                std::lock_guard registry(live_chunk_instances_mutex_);
                auto& slot = live_chunk_instances_[chunk_coord];
                if (const auto previous = slot.lock(); previous != nullptr) {
                    stats_duplicate_chunk_instances_.fetch_add(
                        1, std::memory_order_relaxed);
                    LogMessage(
                        LogLevel::kError,
                        LogComponent::kStore,
                        "duplicate live chunk instance; writes through the "
                        "previous object can be lost",
                        {
                            {"chunk_x", std::to_string(chunk_coord.x)},
                            {"chunk_y", std::to_string(chunk_coord.y)},
                            {"previous_pending_wal_batch",
                             std::to_string(previous->wal_batch.size())},
                        });
                }
                slot = selected;
            }
        }
        break;
    }

    if (access_mode_ == AccessMode::kReadOnly && !inserted) {
        RefreshReadOnlyChunk(chunk_coord, *selected);
    }
    TouchChunk(selected);
    if (inserted) {
        RegisterEvictionCandidate(
            large_coord,
            chunk_coord,
            selected->last_access_tick.load(std::memory_order_relaxed));
        loaded_chunk_count_.fetch_add(1, std::memory_order_relaxed);
        const auto loaded_now =
            resources_->loaded_chunks_.fetch_add(1, std::memory_order_relaxed) + 1U;
        stats_unique_loaded_chunks_.fetch_add(1, std::memory_order_relaxed);
        if (loaded_now > resources_->max_loaded_chunks_) {
            RequestEviction();
        }
    }
    return selected;
}

// A read-only store keeps the chunks it loads and checks a copy on every
// read against the table's snapshot generation. A writer changes a chunk's
// files only inside a generation guard: the mapped counter is odd from before
// the first change until the last guard ends, and the epoch it then
// publishes is higher; a writer that starts again has a later session. So a
// stable generation equal to the one the copy was read at proves that no
// file of the table changed since, and the copy is what the files hold. The
// manifest check catches a table dropped and created again under the same
// name, whose new generation could repeat the old one. Anything else loads
// the chunk again through the snapshot bracket.
void ChunkStore::RefreshReadOnlyChunk(const ChunkCoord& chunk_coord, RegularChunk& chunk) {
    if (const auto now = StableSnapshotGeneration(snapshot_generation_path_, snapshot_generation_record_seen_)) {
        bool unchanged = false;
        {
            std::shared_lock lock(chunk.mutex);
            unchanged = *now == chunk.read_only_generation;
        }
        if (unchanged) {
            RequireStoreStillOnDisk();
            return;
        }
    }
    auto loaded = LoadChunkPayload(chunk_coord);
    BringChunkColumnsToSchema(geometry_, loaded.schema_version, geometry_.chunk_schema().version, &loaded.chunk_columns);
    BringToCurrentSchema(geometry_, loaded.schema_version, loaded.presence_bitmap, &loaded.payload, &loaded.vars);
    std::unique_lock lock(chunk.mutex);
    // Generations only grow, so a copy another read loaded later is kept.
    if (loaded.read_only_generation <= chunk.read_only_generation) {
        return;
    }
    chunk.payload = std::move(loaded.payload);
    chunk.presence_bitmap = std::move(loaded.presence_bitmap);
    chunk.present_blocks = PresentBlockCount(chunk.presence_bitmap);
    chunk.vars = std::move(loaded.vars);
    chunk.chunk_columns = std::move(loaded.chunk_columns);
    chunk.version = loaded.revision;
    chunk.written = loaded.revision != 0;
    chunk.commit_time_ms = loaded.commit_time_ms;
    chunk.wal_bytes = loaded.wal_bytes;
    chunk.wal_header_written = loaded.wal_header_written;
    chunk.wal_path = loaded.wal_path;
    chunk.read_only_generation = loaded.read_only_generation;
}

std::vector<std::uint8_t> ChunkStore::EmptyPayload() const {
    return std::vector<std::uint8_t>(geometry_.ChunkPayloadBytes(), 0U);
}

std::vector<std::uint8_t> ChunkStore::EmptyPresenceBitmap() const {
    return std::vector<std::uint8_t>(ChunkPresenceBitmapBytes(geometry_), 0U);
}

ChunkStore::LoadedChunkPayload ChunkStore::LoadChunkPayload(const ChunkCoord& chunk_coord, bool authoritative_prefix,
    const std::shared_ptr<ChunkCoordinateIndex>& recovery_index) {
    std::vector<WalFrameBoundary> boundaries;
    // A read-only store archives nothing, so it keeps no WAL prefixes.
    const bool track_prefix =
        access_mode_ == AccessMode::kReadWrite && feed_slots_ && feed_slots_->ArchiveRequired();
    const auto token = track_prefix ? feed_slots_->prefix_index().BeginSeed(chunk_coord) : FeedWalPrefixIndex::SeedToken{};
    bool prefix_seeded = false;
    const auto seed = [&] {
        if (!track_prefix || prefix_seeded) return;
        feed_slots_->prefix_index().SeedReplay(chunk_coord, boundaries, authoritative_prefix ? nullptr : &token,
            {}, &checkpoint_publish_mutex_);
        prefix_seeded = true;
    };
    const auto wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);
    const auto data_path = ChunkDataPath(data_dir_, geometry_, chunk_coord);
    const bool writable = access_mode_ != AccessMode::kReadOnly;
    LoadedChunkPayload loaded{
        .payload = EmptyPayload(),
        .presence_bitmap = EmptyPresenceBitmap(),
        .wal_bytes = 0,
        .deferred_wal_compaction = false,
        .wal_header_written = false,
        .wal_path = {},
    };
    auto retained_index = recovery_index ? recovery_index : (writable ? coordinate_index_ : nullptr);
    std::optional<ChunkIndexRecord> retained_boundary;
    const auto apply_boundary = [&] {
        if (retained_index) retained_boundary = retained_index->EmptyBoundary(chunk_coord);
        if (retained_boundary)
            (void)ApplyRetainedEmptyBase(geometry_, *retained_boundary, loaded.revision, loaded.commit_time_ms,
                loaded.schema_version, loaded.payload, loaded.presence_bitmap, loaded.vars, loaded.chunk_columns);
    };
    const auto finish = [&] {
        if (retained_boundary && retained_boundary->kind == ChunkIndexKind::kForget &&
            loaded.revision == retained_boundary->revision) {
            loaded.revision = 0U;
            loaded.commit_time_ms = 0U;
        }
        seed();
    };
    if (!writable) {
        const auto snapshot =
            LoadStableReadOnlyChunkDiskSnapshot(
                data_path,
                wal_path,
                ConditionalIntentPathForWal(data_dir_, wal_path),
                ConditionalIntentDirectory(data_dir_),
                snapshot_generation_path_,
                snapshot_generation_record_seen_,
                chunk_coord,
                [this, chunk_coord](const std::filesystem::path& path) {
                    return LoadChunkImageFile(path, geometry_, chunk_coord, store_id_, features_);
                },
                [this](
                    std::size_t collection,
                    ReadOnlySnapshotArtifact artifact) {
                    PauseReadOnlySnapshotForTests(
                        collection, artifact);
                },
                [&] { if (!recovery_index) retained_index = RetainedIndexForSnapshot(); });
        loaded.read_only_generation = snapshot.generation;
        if (!recovery_index && retained_index) {
            std::lock_guard lock(coordinate_index_refresh_mutex_);
            if (!read_only_retained_index_ || read_only_retained_index_->inventory_generation() < retained_index->inventory_generation())
                read_only_retained_index_ = retained_index;
        }

        // An image or a WAL names its store; an absent chunk does not, so a
        // table dropped and created again would read as empty.
        if (!snapshot.image.present && !snapshot.wal.present) {
            RequireStoreStillOnDisk();
        }
        if (snapshot.image.present) {
            auto image = ParseChunkImage(
                snapshot.image.bytes, geometry_, chunk_coord, store_id_, features_);
            loaded.payload = std::move(image.payload);
            loaded.presence_bitmap = std::move(image.presence_bitmap);
            loaded.vars = std::move(image.vars);
            loaded.chunk_columns = std::move(image.chunk_columns);
            loaded.schema_version = image.schema_version;
            loaded.revision = image.revision;
            loaded.commit_time_ms = image.commit_time_ms;
        }
        apply_boundary();

        std::vector<std::uint8_t> replay_bytes;
        ConditionalIntentState intent_state =
            ConditionalIntentState::kCommitted;
        std::uint64_t committed_wal_size = 0;
        if (snapshot.intent.present) {
            if (!TryParseConditionalIntent(
                    snapshot.intent.bytes,
                    &intent_state,
                    &committed_wal_size)) {
                throw std::runtime_error(
                    "read-only chunk snapshot contains a malformed "
                    "conditional intent for chunk (" +
                    std::to_string(chunk_coord.x) + "," +
                    std::to_string(chunk_coord.y) + ")");
            }
        }

        // A transaction intent that has not reached its commit form limits
        // the WAL to its boundary as a CKRB does.
        std::vector<std::vector<std::uint8_t>> txn_intents;
        txn_intents.reserve(snapshot.txn_intents.size());
        for (const auto& txn_intent : snapshot.txn_intents) {
            txn_intents.push_back(txn_intent.bytes);
        }
        const auto txn_boundary = TxnRollbackBoundaryForChunk(txn_intents, chunk_coord, geometry_, store_id_);
        bool rollback_boundary = snapshot.intent.present && intent_state == ConditionalIntentState::kRollback;
        if (txn_boundary.has_value()) {
            if (rollback_boundary && committed_wal_size != *txn_boundary)
                throw std::runtime_error("conflicting pending rollback boundaries for one chunk");
            committed_wal_size = *txn_boundary;
            rollback_boundary = true;
        }

        if (rollback_boundary) {
            if (!snapshot.wal.present) {
                if (committed_wal_size != 0U) {
                    throw std::runtime_error(
                        "read-only chunk snapshot is missing the WAL required "
                        "by CKRB boundary " +
                        std::to_string(committed_wal_size) + " for chunk (" +
                        std::to_string(chunk_coord.x) + "," +
                        std::to_string(chunk_coord.y) + ")");
                }
            } else {
                if (snapshot.wal.bytes.size() < committed_wal_size) {
                    throw std::runtime_error(
                        "read-only chunk snapshot WAL is shorter than CKRB "
                        "boundary " +
                        std::to_string(committed_wal_size) + " for chunk (" +
                        std::to_string(chunk_coord.x) + "," +
                        std::to_string(chunk_coord.y) + ")");
                }
                replay_bytes.assign(
                    snapshot.wal.bytes.begin(),
                    snapshot.wal.bytes.begin() +
                        static_cast<std::ptrdiff_t>(committed_wal_size));
            }
        } else if (snapshot.wal.present) {
            replay_bytes = snapshot.wal.bytes;
        }

        if (!replay_bytes.empty()) {
            const auto replay = ReplayWal(
                replay_bytes,
                geometry_,
                chunk_coord,
                store_id_,
                features_,
                loaded.revision,
                loaded.schema_version,
                &loaded.payload,
                &loaded.presence_bitmap,
                &loaded.vars,
                track_prefix ? &boundaries : nullptr,
                &loaded.chunk_columns, loaded.commit_time_ms, retained_boundary ?
                    std::optional{WalKnownIdentity{retained_boundary->revision, retained_boundary->commit_time_ms}} : std::nullopt);
            loaded.schema_version = geometry_.layout().schema().version;
            if (replay.torn_creation) {
                // An interrupted creation holds no mutation, and names no
                // store either.
                if (!snapshot.image.present) {
                    RequireStoreStillOnDisk();
                }
                finish();
                return loaded;
            }
            // A crash-shaped tail ends the WAL here as it does for a
            // writer's load. Bytes before an intent's boundary were whole
            // when the intent was written, so a failure there is damage.
            const bool crash_tail =
                replay.stopped_at_crash_tail && replay.valid_end >= committed_wal_size;
            if (!replay.replayable || (replay.tail_truncated_or_corrupt && !crash_tail)) {
                throw std::runtime_error(
                    "read-only chunk snapshot rejected WAL for chunk (" +
                    std::to_string(chunk_coord.x) + "," +
                    std::to_string(chunk_coord.y) + "): " +
                    (replay.stop_reason.empty()
                         ? std::string("non-replayable or corrupt WAL")
                         : replay.stop_reason));
            }
            if (!replay.vars_problem.empty()) {
                throw std::runtime_error(
                    "read-only chunk snapshot of chunk (" + std::to_string(chunk_coord.x) + "," +
                    std::to_string(chunk_coord.y) + ") has inconsistent text and bytes values: " +
                    replay.vars_problem);
            }
            if (replay.applied_frames > 0) {
                loaded.revision = replay.revision;
                loaded.commit_time_ms = replay.commit_time_ms;
            }
        }
        finish();
        return loaded;
    }

    if (writable) {
        CleanupAtomicTmpArtifacts(data_path);
    }
    if (std::filesystem::exists(data_path)) {
        try {
            FeedWalPrefixTestAccess::Run(FeedWalPrefixTestHook::Point::kImageRead, chunk_coord);
            const auto data_bytes = LoadChunkImageFile(data_path, geometry_, chunk_coord, store_id_, features_);
            auto image =
                ParseChunkImage(data_bytes, geometry_, chunk_coord, store_id_, features_);
            loaded.payload = std::move(image.payload);
            loaded.presence_bitmap = std::move(image.presence_bitmap);
            loaded.vars = std::move(image.vars);
            loaded.chunk_columns = std::move(image.chunk_columns);
            loaded.schema_version = image.schema_version;
            loaded.revision = image.revision;
            loaded.commit_time_ms = image.commit_time_ms;
        } catch (...) {
            // The image can be replaced concurrently by atomic checkpoint rename.
            // If it disappeared during open, fall back to empty payload.
            if (std::filesystem::exists(data_path)) {
                throw;
            }
            loaded.payload = EmptyPayload();
            loaded.presence_bitmap = EmptyPresenceBitmap();
            loaded.vars = ChunkVars{};
            loaded.chunk_columns.reset();
            loaded.schema_version = 0;
            loaded.revision = 0;
            loaded.commit_time_ms = 0;
        }
    }

    apply_boundary();
    if (std::filesystem::exists(wal_path)) {
        std::optional<SharedFileInput> opened;
        try {
            opened.emplace(wal_path);
        } catch (const std::system_error&) {
            if (std::filesystem::exists(wal_path)) {
                throw;
            }
            finish();
            return loaded;
        }
        auto& wal_input = *opened;
        wal_input.seekg(0, std::ios::end);
        const auto measured = wal_input.tellg();
        if (measured < 0) throw std::runtime_error("failed to measure WAL " + wal_path.string());
        const auto wal_bytes = static_cast<std::uint64_t>(measured);
        wal_input.seekg(0);
        if (!wal_input) throw std::runtime_error("failed to rewind WAL " + wal_path.string());
        const auto replay = ReplayWalStream(
            wal_input,
            wal_bytes,
            geometry_,
            chunk_coord,
            store_id_,
            features_,
            loaded.revision,
            loaded.schema_version,
            &loaded.payload,
            &loaded.presence_bitmap,
            &loaded.vars,
            track_prefix ? &boundaries : nullptr,
            &loaded.chunk_columns, {}, loaded.commit_time_ms, retained_boundary ?
                std::optional{WalKnownIdentity{retained_boundary->revision, retained_boundary->commit_time_ms}} : std::nullopt);
        wal_input.close();
        loaded.schema_version = geometry_.layout().schema().version;
        if (replay.torn_creation) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kRecovery,
                "WAL left by an interrupted creation holds no mutation",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"bytes", std::to_string(wal_bytes)},
                });
            if (writable) {
                TrimWalForAppend(chunk_coord, wal_path, 0U);
            }
            finish();
            return loaded;
        }
        if (!replay.replayable) {
            // Not a crash artifact: the header is damaged or names another
            // store or chunk. Loading without it would hide its mutations and
            // later appends would be lost with it, so the load fails.
            throw std::runtime_error(
                "WAL " + wal_path.string() + " cannot be replayed (" + replay.stop_reason +
                "); refusing to load chunk (" + std::to_string(chunk_coord.x) + "," +
                std::to_string(chunk_coord.y) + ")");
        }
        if (replay.tail_truncated_or_corrupt && !replay.stopped_at_crash_tail) {
            // Valid-looking bytes follow the failing frame: a crash cannot
            // leave that, so frames acknowledged after it may be there.
            // Truncating would destroy them; refuse the load and leave the
            // file for inspection instead.
            throw std::runtime_error(
                "WAL " + wal_path.string() + " is damaged before its end (" + replay.stop_reason +
                " at byte " + std::to_string(replay.valid_end) + " of " +
                std::to_string(wal_bytes) + "); refusing to load chunk (" +
                std::to_string(chunk_coord.x) + "," + std::to_string(chunk_coord.y) + ")");
        }
        if (!replay.vars_problem.empty()) {
            // Every committed state is consistent, so this is damage; serving
            // or rewriting it would spread it.
            throw std::runtime_error(
                "WAL " + wal_path.string() + " leaves inconsistent text and bytes values (" +
                replay.vars_problem + "); refusing to load chunk (" +
                std::to_string(chunk_coord.x) + "," + std::to_string(chunk_coord.y) + ")");
        }
        if (replay.tail_truncated_or_corrupt) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kRecovery,
                "WAL replay stopped on tail corruption/truncation",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"reason", replay.stop_reason.empty() ? "tail_corruption" : replay.stop_reason},
                    {"applied_records", std::to_string(replay.applied_records)},
                });
        }
        if (replay.applied_frames > 0) {
            loaded.revision = replay.revision;
            loaded.commit_time_ms = replay.commit_time_ms;
        }
        if (writable) {
            if (replay.tail_truncated_or_corrupt) {
                // Publish the fully validated prefix before trim invalidates
                // a concurrent cold seed. Catch-up must not see an unknown
                // entry between repair and this load's return.
                seed();
                TrimWalForAppend(chunk_coord, wal_path, replay.valid_end);
                FeedWalPrefixTestAccess::Run(FeedWalPrefixTestHook::Point::kAfterRecoveryTrim, chunk_coord);
            }
            loaded.deferred_wal_compaction = true;
            loaded.wal_bytes = replay.valid_end;
            loaded.wal_header_written = true;
            loaded.wal_path = wal_path;
        }
    }

    finish();
    return loaded;
}

void ChunkStore::TrimWalForAppend(const ChunkCoord& chunk_coord, const std::filesystem::path& wal_path, std::size_t keep_bytes) {
    SnapshotGenerationWriteGuard snapshot_write(this);
    std::lock_guard publish_lock(checkpoint_publish_mutex_);
    const bool strict =
        durability_mode_ != DurabilityMode::kRelaxed ||
        barrier_durability_floor_.load(std::memory_order_acquire);
    std::error_code ec;
    if (keep_bytes == 0U) {
        std::filesystem::remove(wal_path, ec);
    } else {
        if (ResizeWalPreservingLinks(wal_path, keep_bytes, strict) && !strict)
            NoteUnsyncedDir(wal_path.parent_path());
    }
    if (ec) {
        throw std::runtime_error(
            "failed to trim WAL before appending: " + wal_path.string() +
            " (ec=" + std::to_string(ec.value()) + ", msg='" + ec.message() + "')");
    }
    if (feed_slots_) feed_slots_->prefix_index().Truncate(chunk_coord, keep_bytes);
    if (keep_bytes == 0U) {
        if (strict) {
            SyncDirectoryPath(wal_path.parent_path());
        } else {
            NoteUnsyncedDir(wal_path.parent_path());
        }
    } else if (strict) {
        SyncFilePath(wal_path);
    } else {
        NoteUnsyncedFile(wal_path);
    }
    snapshot_write.Finish();
}

void ChunkStore::TouchChunk(const std::shared_ptr<RegularChunk>& chunk) noexcept {
    chunk->last_access_tick.store(resources_->NextAccessTick(), std::memory_order_relaxed);
}

}  // namespace chunkdb
