#include "wal_writer.hpp"

#include <chrono>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "checkpoint.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "wal_stream_pool.hpp"

namespace chunkdb {

[[nodiscard]] const char* ErrnoName(int err) {
    switch (err) {
#ifdef EACCES
        case EACCES:
            return "EACCES";
#endif
#ifdef EMFILE
        case EMFILE:
            return "EMFILE";
#endif
#ifdef ENFILE
        case ENFILE:
            return "ENFILE";
#endif
#ifdef ENOSPC
        case ENOSPC:
            return "ENOSPC";
#endif
#ifdef EROFS
        case EROFS:
            return "EROFS";
#endif
        default:
            return "UNKNOWN";
    }
}

[[nodiscard]] std::runtime_error BuildWalOpenError(
    const std::filesystem::path& path,
    int err) {
    return std::runtime_error(
        "failed to open WAL file for append: " + path.string() +
        " (errno=" + std::to_string(err) +
        ", code=" + ErrnoName(err) +
        ", msg='" + std::strerror(err) + "')");
}
std::size_t AppendWalDeltaRecordToBatch(
    std::vector<std::uint8_t>* batch,
    std::uint32_t byte_offset,
    const std::uint8_t* payload_bytes,
    std::size_t payload_size) {
    if (batch == nullptr || payload_bytes == nullptr || payload_size == 0) {
        throw std::invalid_argument("WAL delta payload must not be empty");
    }
    if (payload_size > std::numeric_limits<std::uint16_t>::max()) {
        throw std::invalid_argument("WAL delta payload too large");
    }

    const std::size_t record_size = kWalRecordHeaderSize + payload_size;
    batch->reserve(batch->size() + record_size);
    batch->insert(batch->end(), kWalRecordMagic, kWalRecordMagic + kWalRecordMagicSize);
    WriteLe32(*batch, byte_offset);
    WriteLe16(*batch, static_cast<std::uint16_t>(payload_size));
    WriteLe32(*batch, Crc32(payload_bytes, payload_size));
    batch->insert(batch->end(), payload_bytes, payload_bytes + payload_size);
    return record_size;
}

void AppendWalDeltaSpanToBatch(
    std::vector<std::uint8_t>* batch,
    std::uint32_t byte_offset,
    const std::uint8_t* payload_bytes,
    std::size_t payload_size,
    std::size_t* appended_record_bytes,
    std::size_t* appended_record_count) {
    if (payload_bytes == nullptr || payload_size == 0) {
        throw std::invalid_argument("WAL delta payload must not be empty");
    }

    constexpr std::size_t kMaxPayloadSize = std::numeric_limits<std::uint16_t>::max();
    std::size_t total_record_bytes = 0;
    std::size_t total_record_count = 0;
    std::size_t cursor = 0;
    while (cursor < payload_size) {
        const std::size_t chunk_size = std::min(kMaxPayloadSize, payload_size - cursor);
        if (cursor > std::numeric_limits<std::uint32_t>::max() - byte_offset) {
            throw std::invalid_argument("WAL delta byte offset overflow");
        }
        total_record_bytes += AppendWalDeltaRecordToBatch(
            batch,
            static_cast<std::uint32_t>(byte_offset + cursor),
            payload_bytes + cursor,
            chunk_size);
        cursor += chunk_size;
        total_record_count += 1;
    }

    if (appended_record_bytes != nullptr) {
        *appended_record_bytes = total_record_bytes;
    }
    if (appended_record_count != nullptr) {
        *appended_record_count = total_record_count;
    }
}
std::vector<std::uint8_t> BuildWalHeader(const Geometry& geometry, const ChunkCoord& chunk_coord) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kWalHeaderSize);

    bytes.insert(bytes.end(), kWalMagic, kWalMagic + kWalMagicSize);
    WriteLe16(bytes, kWalFileVersion);
    WriteLe16(bytes, static_cast<std::uint16_t>(geometry.config().block_bits));
    WriteLe32(bytes, geometry.config().chunk_width_blocks);
    WriteLe32(bytes, geometry.config().chunk_height_blocks);
    WriteLe64(bytes, static_cast<std::uint64_t>(chunk_coord.x));
    WriteLe64(bytes, static_cast<std::uint64_t>(chunk_coord.y));

    return bytes;
}
void ChunkStore::AppendWalDelta(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    std::uint32_t byte_offset,
    const std::uint8_t* payload_bytes,
    std::size_t payload_size,
    std::size_t* appended_record_bytes) {
    if (payload_bytes == nullptr || payload_size == 0) {
        throw std::invalid_argument("WAL delta payload must not be empty");
    }
    std::size_t appended_record_count = 0;
    AppendWalDeltaSpanToBatch(
        &chunk->wal_batch,
        byte_offset,
        payload_bytes,
        payload_size,
        appended_record_bytes,
        &appended_record_count);

    chunk->pending_wal_flush_updates += appended_record_count;

    const bool sync_required = durability_mode_ != DurabilityMode::kRelaxed;
    if (sync_required || chunk->pending_wal_flush_updates >= wal_group_commit_updates_) {
        FlushWalBatch(chunk_coord, chunk, sync_required);
    }
}

void ChunkStore::FlushWalBatch(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool force_sync) {
    if (chunk->wal_batch.empty()) {
        return;
    }

    bool first_create = false;
    try {
        EnsureWalAppendStream(chunk_coord, chunk, &first_create);

        if (const auto hold = ConsumeFailpointDelayMs("CHUNKDB_FAILPOINT_WAL_APPEND_HOLD_MS_ONCE");
            hold.count() > 0) {
            std::this_thread::sleep_for(hold);
        }

        auto& output = chunk->wal_append_stream;
        output.write(
            reinterpret_cast<const char*>(chunk->wal_batch.data()),
            static_cast<std::streamsize>(chunk->wal_batch.size()));
        output.flush();
        if (!output.good()) {
            throw std::runtime_error("failed to append WAL record batch: " + chunk->wal_path.string());
        }

        if (force_sync) {
            SyncFilePath(chunk->wal_path);
            if (first_create) {
                if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_AFTER_FILE_SYNC_BEFORE_DIR_SYNC_ONCE")) {
                    throw std::runtime_error(
                        "injected WAL sync failure after file sync before directory sync: " + chunk->wal_path.string());
                }
                SyncDirectoryPath(chunk->wal_path.parent_path());
            }
        }
    } catch (...) {
        // Reset append stream on any failure so the next write can reopen cleanly.
        CloseWalAppendStream(chunk);
        throw;
    }

    stats_wal_batch_flushes_.fetch_add(1, std::memory_order_relaxed);
    chunk->wal_batch.clear();
    chunk->pending_wal_flush_updates = 0;
}

void ChunkStore::FlushWalBatchForEviction(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool force_sync) {
    if (chunk->wal_batch.empty()) {
        return;
    }

    // On eviction, bypass WAL stream-cache tracking when no stream is currently open.
    // This avoids extra map/mutex churn for streams that will be closed immediately.
    if (!chunk->wal_stream_initialized || !chunk->wal_append_stream.is_open()) {
        if (chunk->wal_path.empty()) {
            chunk->wal_path = LayoutWalPath(data_dir_, geometry_, chunk_coord, storage_layout_mode_);
        }
        const auto wal_parent_path = chunk->wal_path.parent_path();
        EnsureWalParentDirectoryCached(
            wal_parent_path,
            false,
            durability_mode_ != DurabilityMode::kRelaxed);

        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_OPEN_ONCE")) {
            throw std::runtime_error("injected WAL open failure: " + chunk->wal_path.string());
        }

        const bool needs_header = !chunk->wal_header_written;
        std::ofstream out(chunk->wal_path, std::ios::binary | std::ios::app);
        if (!out.is_open()) {
            int open_err = errno;
            InvalidateWalParentDirectoryCache(wal_parent_path);
            EnsureWalParentDirectoryCached(
                wal_parent_path,
                true,
                durability_mode_ != DurabilityMode::kRelaxed);
            out.clear();
            out.open(chunk->wal_path, std::ios::binary | std::ios::app);
            if (!out.is_open()) {
                open_err = errno;
                throw BuildWalOpenError(chunk->wal_path, open_err);
            }
        }

        if (needs_header) {
            const auto wal_header = BuildWalHeader(geometry_, chunk_coord);
            out.write(
                reinterpret_cast<const char*>(wal_header.data()),
                static_cast<std::streamsize>(wal_header.size()));
            if (!out.good()) {
                throw std::runtime_error("failed to append WAL header: " + chunk->wal_path.string());
            }
            chunk->wal_header_written = true;
        }

        out.write(
            reinterpret_cast<const char*>(chunk->wal_batch.data()),
            static_cast<std::streamsize>(chunk->wal_batch.size()));
        out.flush();
        if (!out.good()) {
            throw std::runtime_error("failed to append WAL record batch: " + chunk->wal_path.string());
        }

        if (force_sync) {
            SyncFilePath(chunk->wal_path);
            if (needs_header) {
                if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_AFTER_FILE_SYNC_BEFORE_DIR_SYNC_ONCE")) {
                    throw std::runtime_error(
                        "injected WAL sync failure after file sync before directory sync: " +
                        chunk->wal_path.string());
                }
                SyncDirectoryPath(wal_parent_path);
            }
        }

        stats_wal_batch_flushes_.fetch_add(1, std::memory_order_relaxed);
        chunk->wal_batch.clear();
        chunk->pending_wal_flush_updates = 0;
        return;
    }

    FlushWalBatch(chunk_coord, chunk, force_sync);
}
void ChunkStore::FlushAllPendingWalBatches() noexcept {
    std::vector<std::shared_ptr<LargeChunk>> large_chunks;
    {
        std::lock_guard lock(large_chunks_mutex_);
        large_chunks.reserve(large_chunks_.size());
        for (const auto& [_, large_chunk] : large_chunks_) {
            large_chunks.push_back(large_chunk);
        }
    }

    for (const auto& large_chunk : large_chunks) {
        std::vector<std::pair<ChunkCoord, std::shared_ptr<RegularChunk>>> chunks;
        {
            std::lock_guard lock(large_chunk->mutex);
            chunks.reserve(large_chunk->chunks.size());
            for (const auto& [coord, chunk] : large_chunk->chunks) {
                chunks.emplace_back(coord, chunk);
            }
        }

        for (const auto& [coord, chunk] : chunks) {
            std::unique_lock chunk_lock(chunk->mutex);
            try {
                FlushWalBatch(
                    coord,
                    chunk,
                    durability_mode_ != DurabilityMode::kRelaxed);
            } catch (const std::exception& e) {
                try {
                    LogMessage(
                        LogLevel::kError,
                        LogComponent::kStore,
                        "shutdown WAL flush failed; durability may be violated",
                        {
                            {"chunk_x", std::to_string(coord.x)},
                            {"chunk_y", std::to_string(coord.y)},
                            {"wal_path", chunk->wal_path.empty() ? "unset" : chunk->wal_path.string()},
                            {"pending_batch_bytes", std::to_string(chunk->wal_batch.size())},
                            {"durability_mode", DurabilityModeName(durability_mode_)},
                            {"error", e.what()},
                        });
                } catch (...) {
                }
            } catch (...) {
                try {
                    LogMessage(
                        LogLevel::kError,
                        LogComponent::kStore,
                        "shutdown WAL flush failed; durability may be violated",
                        {
                            {"chunk_x", std::to_string(coord.x)},
                            {"chunk_y", std::to_string(coord.y)},
                            {"wal_path", chunk->wal_path.empty() ? "unset" : chunk->wal_path.string()},
                            {"pending_batch_bytes", std::to_string(chunk->wal_batch.size())},
                            {"durability_mode", DurabilityModeName(durability_mode_)},
                            {"error", "unknown"},
                        });
                } catch (...) {
                }
            }
            CloseWalAppendStream(chunk);
        }
    }
}

}  // namespace chunkdb
