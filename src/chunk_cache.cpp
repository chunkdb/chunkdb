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

    auto created = std::make_shared<LargeChunk>();
    large_chunks_.emplace(large_coord, created);
    eviction_large_chunk_ring_.push_back(large_coord);
    return created;
}

std::shared_ptr<ChunkStore::RegularChunk> ChunkStore::GetOrLoadRegularChunk(const ChunkCoord& chunk_coord) {
    const LargeChunkCoord large_coord = geometry_.ChunkToLarge(chunk_coord);
    const auto large_chunk = GetOrCreateLargeChunk(large_coord);

    bool inserted = false;
    std::shared_ptr<RegularChunk> selected;
    {
        std::lock_guard lock(large_chunk->mutex);
        auto it = large_chunk->chunks.find(chunk_coord);
        if (it != large_chunk->chunks.end()) {
            selected = it->second;
        } else {
            const auto loaded = LoadChunkPayload(chunk_coord);
            selected = std::make_shared<RegularChunk>(loaded.payload, loaded.presence_bitmap);
            // Format v2: the persisted revision survives eviction and restart,
            // so CHUNKVER tokens no longer change on reload. Legacy chunks
            // (no v2 artifact yet) keep the 1.x behavior of a fresh token per
            // load until their first mutation or checkpoint persists one.
            if (loaded.revision != 0) {
                RaiseVersionClockAbove(loaded.revision);
            }
            selected->version = loaded.revision != 0 ? loaded.revision : NextChunkVersion();
            selected->wal_bytes = loaded.wal_bytes;
            selected->checkpoint_due_armed = loaded.wal_bytes >= checkpoint_wal_bytes_;
            selected->deferred_wal_compaction = loaded.deferred_wal_compaction;
            selected->wal_header_written = loaded.wal_header_written;
            selected->wal_needs_v4_header = loaded.wal_needs_v4_header;
            selected->wal_path = loaded.wal_path;
            large_chunk->chunks.emplace(chunk_coord, selected);
            inserted = true;
        }
    }

    TouchChunk(selected);
    if (inserted) {
        RegisterEvictionCandidate(
            large_coord,
            chunk_coord,
            selected->last_access_tick.load(std::memory_order_relaxed));
        const auto loaded_now = loaded_chunk_count_.fetch_add(1, std::memory_order_relaxed) + 1;
        stats_unique_loaded_chunks_.fetch_add(1, std::memory_order_relaxed);
        if (loaded_now > max_loaded_chunks_) {
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

ChunkStore::LoadedChunkPayload ChunkStore::LoadChunkPayload(const ChunkCoord& chunk_coord) {
    const auto wal_path = LayoutWalPath(data_dir_, geometry_, chunk_coord, storage_layout_mode_);
    const auto data_path =
        (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1)
            ? ChunkDataPath(data_dir_, geometry_, chunk_coord)
            : RegionDataPath(data_dir_, chunk_coord, experimental_region_span_chunks_);
    const bool writable = access_mode_ != AccessMode::kReadOnly;
    LoadedChunkPayload loaded{
        .payload = EmptyPayload(),
        .presence_bitmap = EmptyPresenceBitmap(),
        .wal_bytes = 0,
        .deferred_wal_compaction = false,
        .wal_header_written = false,
        .wal_needs_v4_header = false,
        .wal_path = {},
    };
    if (!writable) {
        const auto snapshot =
            LoadStableReadOnlyChunkDiskSnapshot(
                data_path,
                wal_path,
                ConditionalIntentPathForWal(data_dir_, wal_path),
                snapshot_generation_path_,
                chunk_coord,
                [this](
                    std::size_t collection,
                    ReadOnlySnapshotArtifact artifact) {
                    PauseReadOnlySnapshotForTests(
                        collection, artifact);
                });

        if (snapshot.image.present) {
            if (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1) {
                auto image =
                    ParseChunkImage(snapshot.image.bytes, geometry_, chunk_coord);
                loaded.payload = std::move(image.payload);
                loaded.presence_bitmap = std::move(image.presence_bitmap);
                loaded.revision = image.revision;
            } else {
                const auto addr = ComputeRegionChunkAddress(
                    chunk_coord, experimental_region_span_chunks_);
                const auto region = ParseRegionFileImage(
                    snapshot.image.bytes,
                    geometry_,
                    addr,
                    experimental_region_span_chunks_);
                const auto slot_state =
                    ExtractRegionSlotState(region, addr.slot_index);
                if (!slot_state.empty()) {
                    SplitChunkStateBytes(
                        geometry_,
                        slot_state,
                        &loaded.payload,
                        &loaded.presence_bitmap);
                }
            }
        }

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
                &loaded.payload,
                &loaded.presence_bitmap);
            if (!replay.replayable ||
                replay.tail_truncated_or_corrupt) {
                throw std::runtime_error(
                    "read-only chunk snapshot rejected WAL for chunk (" +
                    std::to_string(chunk_coord.x) + "," +
                    std::to_string(chunk_coord.y) + "): " +
                    (replay.stop_reason.empty()
                         ? std::string("non-replayable or corrupt WAL")
                         : replay.stop_reason));
            }
            if (replay.applied_frames > 0) {
                loaded.revision = replay.revision;
            }
        }
        return loaded;
    }

    if (writable) {
        CleanupAtomicTmpArtifacts(data_path);
    }
    if (std::filesystem::exists(data_path)) {
        try {
            if (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1) {
                const auto data_bytes = LoadFile(data_path);
                auto image = ParseChunkImage(data_bytes, geometry_, chunk_coord);
                loaded.payload = std::move(image.payload);
                loaded.presence_bitmap = std::move(image.presence_bitmap);
                loaded.revision = image.revision;
            } else {
                const auto addr = ComputeRegionChunkAddress(chunk_coord, experimental_region_span_chunks_);
                std::lock_guard region_lock(RegionIoMutex());
                const auto region_bytes = LoadFile(data_path);
                const auto region = ParseRegionFileImage(region_bytes, geometry_, addr, experimental_region_span_chunks_);
                const auto slot_state = ExtractRegionSlotState(region, addr.slot_index);
                if (!slot_state.empty()) {
                    SplitChunkStateBytes(
                        geometry_,
                        slot_state,
                        &loaded.payload,
                        &loaded.presence_bitmap);
                }
            }
        } catch (...) {
            // The image can be replaced concurrently by atomic checkpoint rename.
            // If it disappeared during open, fall back to empty payload.
            if (std::filesystem::exists(data_path)) {
                throw;
            }
            loaded.payload = EmptyPayload();
            loaded.presence_bitmap = EmptyPresenceBitmap();
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
            &loaded.payload,
            &loaded.presence_bitmap);
        if (!replay.replayable) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kRecovery,
                "WAL skipped during chunk load",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"reason", replay.stop_reason.empty() ? "non_replayable" : replay.stop_reason},
                });
        } else if (replay.tail_truncated_or_corrupt) {
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
        }
        if (writable) {
            loaded.deferred_wal_compaction = true;
            loaded.wal_bytes = wal_bytes.size();
            loaded.wal_header_written = true;
            // A legacy (v2/v3) stream cannot take v4 frames directly; the
            // first append writes a v4 header mid-stream first.
            loaded.wal_needs_v4_header = replay.replayable && replay.legacy_records;
            loaded.wal_path = wal_path;
        }
    }

    return loaded;
}

void ChunkStore::TouchChunk(const std::shared_ptr<RegularChunk>& chunk) noexcept {
    const std::uint64_t tick = access_clock_.fetch_add(1, std::memory_order_relaxed) + 1U;
    chunk->last_access_tick.store(tick, std::memory_order_relaxed);
}

}  // namespace chunkdb
