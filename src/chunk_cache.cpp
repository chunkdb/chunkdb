#include "chunkdb/chunk_store.hpp"

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "eviction.hpp"
#include "process_lock.hpp"
#include "wal_replay.hpp"
#include "wal_stream_pool.hpp"
#include "wal_writer.hpp"

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
    while (true) {
        large_chunk = GetOrCreateLargeChunk(large_coord);
        std::lock_guard lock(large_chunk->mutex);
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
            auto loaded = LoadChunkPayload(chunk_coord);
            selected = std::make_shared<RegularChunk>(
                std::move(loaded.payload), std::move(loaded.presence_bitmap));
            selected->extra = std::move(loaded.extra);
            // The persisted revision survives eviction and restart, so
            // CHUNKVER tokens do not change on reload. A chunk with no
            // artifact (revision zero) takes a fresh token for this load.
            if (loaded.revision != 0) {
                RaiseVersionClockAbove(loaded.revision);
            }
            selected->version = loaded.revision != 0 ? loaded.revision : NextChunkVersion();
            selected->commit_time_ms = loaded.commit_time_ms;
            selected->wal_bytes = loaded.wal_bytes;
            selected->checkpoint_due_armed = loaded.wal_bytes >= checkpoint_wal_bytes_;
            selected->deferred_wal_compaction = loaded.deferred_wal_compaction;
            selected->wal_header_written = loaded.wal_header_written;
            selected->wal_path = loaded.wal_path;
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

std::vector<std::uint8_t> ChunkStore::EmptyPayload() const {
    return std::vector<std::uint8_t>(geometry_.ChunkPayloadBytes(), 0U);
}

std::vector<std::uint8_t> ChunkStore::EmptyPresenceBitmap() const {
    return std::vector<std::uint8_t>(ChunkPresenceBitmapBytes(geometry_), 0U);
}

ChunkStore::ReadOnlyChunkFiles ChunkStore::ReadOnlyChunkFilesFor(const ChunkCoord& chunk_coord) {
    const auto wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);
    const auto data_path = ChunkDataPath(data_dir_, geometry_, chunk_coord);
    const auto snapshot =
        LoadStableReadOnlyChunkDiskSnapshot(
            data_path,
            wal_path,
            ConditionalIntentPathForWal(data_dir_, wal_path),
            snapshot_generation_path_,
            snapshot_generation_record_seen_,
            chunk_coord,
            [this](
                std::size_t collection,
                ReadOnlySnapshotArtifact artifact) {
                PauseReadOnlySnapshotForTests(
                    collection, artifact);
            });

    // An image or a WAL names its store; an absent chunk does not, so a
    // table dropped and created again would read as empty.
    if (!snapshot.image.present && !snapshot.wal.present) {
        RequireStoreStillOnDisk();
    }
    ReadOnlyChunkFiles files;
    if (snapshot.image.present) {
        files.image = snapshot.image.bytes;
    }

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
    files.whole_bytes = committed_wal_size;

    if (snapshot.intent.present &&
        intent_state == ConditionalIntentState::kRollback) {
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
            files.replay_bytes.assign(
                snapshot.wal.bytes.begin(),
                snapshot.wal.bytes.begin() +
                    static_cast<std::ptrdiff_t>(committed_wal_size));
        }
    } else if (snapshot.wal.present) {
        files.replay_bytes = snapshot.wal.bytes;
    }
    return files;
}

ChunkStore::LoadedChunkPayload ChunkStore::LoadChunkPayload(const ChunkCoord& chunk_coord) {
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
    if (!writable) {
        const auto files = ReadOnlyChunkFilesFor(chunk_coord);
        if (files.image.has_value()) {
            auto image = ParseChunkImage(
                *files.image, geometry_, chunk_coord, store_id_, features_);
            loaded.payload = std::move(image.payload);
            loaded.presence_bitmap = std::move(image.presence_bitmap);
            loaded.extra = std::move(image.extra);
            loaded.revision = image.revision;
            loaded.commit_time_ms = image.commit_time_ms;
        }
        const auto& replay_bytes = files.replay_bytes;
        const std::uint64_t committed_wal_size = files.whole_bytes;

        if (!replay_bytes.empty()) {
            const auto replay = ReplayWal(
                replay_bytes,
                geometry_,
                chunk_coord,
                store_id_,
                features_,
                loaded.revision,
                &loaded.payload,
                &loaded.presence_bitmap,
                &loaded.extra);
            if (replay.torn_creation) {
                // An interrupted creation holds no mutation, and names no
                // store either.
                if (!files.image.has_value()) {
                    RequireStoreStillOnDisk();
                }
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
            if (!replay.extra_problem.empty()) {
                throw std::runtime_error(
                    "read-only chunk snapshot of chunk (" + std::to_string(chunk_coord.x) + "," +
                    std::to_string(chunk_coord.y) + ") has inconsistent extra data: " +
                    replay.extra_problem);
            }
            if (replay.applied_frames > 0) {
                loaded.revision = replay.revision;
                loaded.commit_time_ms = replay.commit_time_ms;
            }
        }
        return loaded;
    }

    if (writable) {
        CleanupAtomicTmpArtifacts(data_path);
    }
    if (std::filesystem::exists(data_path)) {
        try {
            const auto data_bytes = LoadFile(data_path);
            auto image =
                ParseChunkImage(data_bytes, geometry_, chunk_coord, store_id_, features_);
            loaded.payload = std::move(image.payload);
            loaded.presence_bitmap = std::move(image.presence_bitmap);
            loaded.extra = std::move(image.extra);
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
            loaded.extra = ChunkExtra{};
            loaded.revision = 0;
            loaded.commit_time_ms = 0;
        }
    }

    if (std::filesystem::exists(wal_path)) {
        std::vector<std::uint8_t> wal_bytes;
        try {
            wal_bytes = LoadFile(wal_path);
        } catch (...) {
            // WAL can be removed concurrently by checkpoint cleanup.
            // If it no longer exists, treat as already checkpointed.
            if (std::filesystem::exists(wal_path)) {
                throw;
            }
            return loaded;
        }

        const auto replay = ReplayWal(
            wal_bytes,
            geometry_,
            chunk_coord,
            store_id_,
            features_,
            loaded.revision,
            &loaded.payload,
            &loaded.presence_bitmap,
            &loaded.extra);
        if (replay.torn_creation) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kRecovery,
                "WAL left by an interrupted creation holds no mutation",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"bytes", std::to_string(wal_bytes.size())},
                });
            if (writable) {
                TrimWalForAppend(wal_path, 0U);
            }
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
                std::to_string(wal_bytes.size()) + "); refusing to load chunk (" +
                std::to_string(chunk_coord.x) + "," + std::to_string(chunk_coord.y) + ")");
        }
        if (!replay.extra_problem.empty()) {
            // Every committed state is consistent, so this is damage; serving
            // or rewriting it would spread it.
            throw std::runtime_error(
                "WAL " + wal_path.string() + " leaves inconsistent extra data (" +
                replay.extra_problem + "); refusing to load chunk (" +
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
                TrimWalForAppend(wal_path, replay.valid_end);
            }
            loaded.deferred_wal_compaction = true;
            loaded.wal_bytes = replay.valid_end;
            loaded.wal_header_written = true;
            loaded.wal_path = wal_path;
        }
    }

    return loaded;
}

void ChunkStore::TrimWalForAppend(const std::filesystem::path& wal_path, std::size_t keep_bytes) {
    SnapshotGenerationWriteGuard snapshot_write(this);
    const bool strict =
        durability_mode_ != DurabilityMode::kRelaxed ||
        barrier_durability_floor_.load(std::memory_order_acquire);
    std::error_code ec;
    if (keep_bytes == 0U) {
        std::filesystem::remove(wal_path, ec);
    } else {
        std::filesystem::resize_file(wal_path, keep_bytes, ec);
    }
    if (ec) {
        throw std::runtime_error(
            "failed to trim WAL before appending: " + wal_path.string() +
            " (ec=" + std::to_string(ec.value()) + ", msg='" + ec.message() + "')");
    }
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
