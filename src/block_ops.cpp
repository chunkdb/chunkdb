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

#include <shared_mutex>

namespace chunkdb {

bool ChunkStore::BlockExists(std::int64_t block_x, std::int64_t block_y) {
    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return BlockPresent(regular_chunk->presence_bitmap, block_index);
}

std::string ChunkStore::GetBlockBits(std::int64_t block_x, std::int64_t block_y) {
    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const std::size_t bit_offset = block_index * geometry_.config().block_bits;

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    if (!BlockPresent(regular_chunk->presence_bitmap, block_index)) {
        return std::string(geometry_.config().block_bits, '0');
    }
    return BitCodec::ExtractBits(regular_chunk->payload, bit_offset, geometry_.config().block_bits);
}

void ChunkStore::FinishOrdinaryMutationLocked(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    std::size_t appended_bytes,
    std::size_t appended_record_count,
    std::uint64_t reserved_version) {
    chunk->pending_wal_flush_updates += appended_record_count;
    chunk->pending_updates += 1;
    chunk->wal_bytes += appended_bytes;

    const bool sync_required = durability_mode_ != DurabilityMode::kRelaxed;
    if (sync_required || chunk->pending_wal_flush_updates >= wal_group_commit_updates_) {
        // Throws with the WAL file already neutralized on failure; the
        // caller rolls back memory, batch, and counters.
        FlushWalBatch(chunk_coord, chunk, sync_required);
    }

    // Commit point passed: in synced modes the records are durable, in
    // relaxed mode they are accepted into the group-commit batch. From here
    // on NO failure may escape this function — a throw would reach the
    // caller's rollback (which restores memory and truncates the staged
    // batch) and contradict an already-committed write. The inline checkpoint
    // failure is logged and retained for retry; even the logging itself must
    // not throw out (e.g. bad_alloc), so it is fully contained.
    chunk->version = reserved_version;
    try {
        MaybeCheckpointChunk(chunk_coord, chunk);
    } catch (...) {
        try {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kStore,
                "ordinary mutation committed in WAL but inline checkpoint failed; retaining WAL for retry",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                });
        } catch (...) {
            // Logging is best-effort; the committed mutation stands.
        }
    }
}

void ChunkStore::SetBlockBits(std::int64_t block_x, std::int64_t block_y, std::string_view bits) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
    ThrowIfDurabilityPoisoned();
    if (bits.size() != geometry_.config().block_bits) {
        throw std::invalid_argument("bit string length does not match configured block_bits");
    }
    if (!BitCodec::IsBitString(bits)) {
        throw std::invalid_argument("bit string must contain only 0 and 1");
    }

    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const std::size_t bit_offset = block_index * geometry_.config().block_bits;

    const std::size_t begin_byte = bit_offset / 8U;
    const std::size_t end_byte = (bit_offset + bits.size() - 1U) / 8U;
    const std::size_t touched_bytes = end_byte - begin_byte + 1U;
    const std::size_t presence_byte_index = block_index / 8U;

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    auto& previous_bytes = regular_chunk->scratch_before;
    previous_bytes.resize(touched_bytes);
    std::copy_n(
        regular_chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin_byte),
        static_cast<std::ptrdiff_t>(touched_bytes),
        previous_bytes.begin());
    const std::uint8_t previous_presence_byte = regular_chunk->presence_bitmap[presence_byte_index];

    BitCodec::WriteBits(regular_chunk->payload, bit_offset, bits);
    SetBlockPresent(&regular_chunk->presence_bitmap, block_index, true);

    bool payload_changed = false;
    for (std::size_t i = 0; i < touched_bytes; ++i) {
        if (previous_bytes[i] != regular_chunk->payload[begin_byte + i]) {
            payload_changed = true;
            break;
        }
    }
    const bool presence_changed =
        previous_presence_byte != regular_chunk->presence_bitmap[presence_byte_index];

    if (!payload_changed && !presence_changed) {
        return;
    }

    // Snapshot every component needed for a full rollback, mirroring the
    // conditional path: a rejected mutation must leave memory, the staged
    // batch, the counters, and the WAL file exactly as before the command.
    const std::size_t saved_wal_batch_size = regular_chunk->wal_batch.size();
    const auto saved_pending_updates = regular_chunk->pending_updates;
    const auto saved_wal_bytes = regular_chunk->wal_bytes;
    const auto saved_pending_wal_flush_updates = regular_chunk->pending_wal_flush_updates;
    try {
        // Reserve the version token before any WAL staging so a
        // version-clock failure is a clean pre-WAL error.
        const std::uint64_t reserved_version = NextChunkVersion();
        // One mutation is one WAL frame, applied all-or-nothing on replay.
        WalFrameBuilder frame(&regular_chunk->wal_batch);
        if (payload_changed) {
            frame.AppendSpan(
                static_cast<std::uint32_t>(begin_byte),
                regular_chunk->payload.data() + begin_byte,
                touched_bytes);
        }
        if (presence_changed) {
            frame.AppendSpan(
                static_cast<std::uint32_t>(geometry_.ChunkPayloadBytes() + presence_byte_index),
                &regular_chunk->presence_bitmap[presence_byte_index],
                1U);
        }
        const std::size_t appended_bytes = frame.Finish(reserved_version);
        const std::size_t appended_record_count = frame.record_count();

        FinishOrdinaryMutationLocked(
            chunk_coord,
            regular_chunk,
            appended_bytes,
            appended_record_count,
            reserved_version);
    } catch (...) {
        std::copy(
            previous_bytes.begin(),
            previous_bytes.end(),
            regular_chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin_byte));
        regular_chunk->presence_bitmap[presence_byte_index] = previous_presence_byte;
        // The batch is only ever appended to within this op (a successful
        // flush clears it but then never reaches this catch), so truncating
        // back to the pre-op length restores the exact saved content without
        // an O(batch) copy on every write.
        regular_chunk->wal_batch.resize(saved_wal_batch_size);
        regular_chunk->pending_updates = saved_pending_updates;
        regular_chunk->wal_bytes = saved_wal_bytes;
        regular_chunk->pending_wal_flush_updates = saved_pending_wal_flush_updates;
        throw;
    }
}

void ChunkStore::UnsetBlock(std::int64_t block_x, std::int64_t block_y) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
    ThrowIfDurabilityPoisoned();

    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const std::size_t bit_offset = block_index * geometry_.config().block_bits;
    const std::size_t begin_byte = bit_offset / 8U;
    const std::size_t end_byte = (bit_offset + geometry_.config().block_bits - 1U) / 8U;
    const std::size_t touched_bytes = end_byte - begin_byte + 1U;
    const std::size_t presence_byte_index = block_index / 8U;

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    auto& previous_bytes = regular_chunk->scratch_before;
    previous_bytes.resize(touched_bytes);
    std::copy_n(
        regular_chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin_byte),
        static_cast<std::ptrdiff_t>(touched_bytes),
        previous_bytes.begin());
    const std::uint8_t previous_presence_byte = regular_chunk->presence_bitmap[presence_byte_index];

    BitCodec::WriteBits(
        regular_chunk->payload,
        bit_offset,
        std::string(geometry_.config().block_bits, '0'));
    SetBlockPresent(&regular_chunk->presence_bitmap, block_index, false);

    bool payload_changed = false;
    for (std::size_t i = 0; i < touched_bytes; ++i) {
        if (previous_bytes[i] != regular_chunk->payload[begin_byte + i]) {
            payload_changed = true;
            break;
        }
    }
    const bool presence_changed =
        previous_presence_byte != regular_chunk->presence_bitmap[presence_byte_index];

    if (!payload_changed && !presence_changed) {
        return;
    }

    // Snapshot every component needed for a full rollback, mirroring the
    // conditional path: a rejected mutation must leave memory, the staged
    // batch, the counters, and the WAL file exactly as before the command.
    const std::size_t saved_wal_batch_size = regular_chunk->wal_batch.size();
    const auto saved_pending_updates = regular_chunk->pending_updates;
    const auto saved_wal_bytes = regular_chunk->wal_bytes;
    const auto saved_pending_wal_flush_updates = regular_chunk->pending_wal_flush_updates;
    try {
        // Reserve the version token before any WAL staging so a
        // version-clock failure is a clean pre-WAL error.
        const std::uint64_t reserved_version = NextChunkVersion();
        // One mutation is one WAL frame, applied all-or-nothing on replay.
        WalFrameBuilder frame(&regular_chunk->wal_batch);
        if (payload_changed) {
            frame.AppendSpan(
                static_cast<std::uint32_t>(begin_byte),
                regular_chunk->payload.data() + begin_byte,
                touched_bytes);
        }
        if (presence_changed) {
            frame.AppendSpan(
                static_cast<std::uint32_t>(geometry_.ChunkPayloadBytes() + presence_byte_index),
                &regular_chunk->presence_bitmap[presence_byte_index],
                1U);
        }
        const std::size_t appended_bytes = frame.Finish(reserved_version);
        const std::size_t appended_record_count = frame.record_count();

        FinishOrdinaryMutationLocked(
            chunk_coord,
            regular_chunk,
            appended_bytes,
            appended_record_count,
            reserved_version);
    } catch (...) {
        std::copy(
            previous_bytes.begin(),
            previous_bytes.end(),
            regular_chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin_byte));
        regular_chunk->presence_bitmap[presence_byte_index] = previous_presence_byte;
        // The batch is only ever appended to within this op (a successful
        // flush clears it but then never reaches this catch), so truncating
        // back to the pre-op length restores the exact saved content without
        // an O(batch) copy on every write.
        regular_chunk->wal_batch.resize(saved_wal_batch_size);
        regular_chunk->pending_updates = saved_pending_updates;
        regular_chunk->wal_bytes = saved_wal_bytes;
        regular_chunk->pending_wal_flush_updates = saved_pending_wal_flush_updates;
        throw;
    }
}

bool ChunkStore::ChunkExists(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return ChunkPresent(regular_chunk->presence_bitmap);
}

void ChunkStore::SetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y, std::string_view bits) {
    SetChunkStateBits(
        chunk_x,
        chunk_y,
        bits,
        std::string(geometry_.ChunkBlockCount(), '1'));
}

void ChunkStore::SetChunkStateBits(
    std::int64_t chunk_x,
    std::int64_t chunk_y,
    std::string_view payload_bits,
    std::string_view presence_bits) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
    ThrowIfDurabilityPoisoned();
    if (payload_bits.size() != geometry_.ChunkPayloadBits()) {
        throw std::invalid_argument("payload bit string length does not match configured chunk size");
    }
    if (presence_bits.size() != geometry_.ChunkBlockCount()) {
        throw std::invalid_argument("presence bit string length does not match configured chunk block count");
    }
    if (!BitCodec::IsBitString(payload_bits)) {
        throw std::invalid_argument("payload bit string must contain only 0 and 1");
    }
    if (!BitCodec::IsBitString(presence_bits)) {
        throw std::invalid_argument("presence bit string must contain only 0 and 1");
    }

    auto payload = EmptyPayload();
    BitCodec::WriteBits(payload, 0, payload_bits);
    auto presence_bitmap = EmptyPresenceBitmap();
    BitCodec::WriteBits(presence_bitmap, 0, presence_bits);
    ApplyChunkState({chunk_x, chunk_y}, std::move(payload), std::move(presence_bitmap));
}

void ChunkStore::SetChunkPayloadBytes(
    std::int64_t chunk_x,
    std::int64_t chunk_y,
    const std::vector<std::uint8_t>& payload) {
    SetChunkStateBytes(chunk_x, chunk_y, payload, FullPresenceBitmap(geometry_));
}

void ChunkStore::SetChunkStateBytes(
    std::int64_t chunk_x,
    std::int64_t chunk_y,
    const std::vector<std::uint8_t>& payload,
    const std::vector<std::uint8_t>& presence_bitmap) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
    ThrowIfDurabilityPoisoned();
    if (payload.size() != geometry_.ChunkPayloadBytes()) {
        throw std::invalid_argument("payload byte length does not match configured chunk size");
    }
    if (presence_bitmap.size() != ChunkPresenceBitmapBytes(geometry_)) {
        throw std::invalid_argument("presence byte length does not match configured chunk block count");
    }

    auto canonical_payload = payload;
    MaskUnusedPayloadBits(geometry_, &canonical_payload);
    auto canonical_presence = presence_bitmap;
    MaskUnusedPresenceBits(geometry_, &canonical_presence);
    ApplyChunkState({chunk_x, chunk_y}, std::move(canonical_payload), std::move(canonical_presence));
}

void ChunkStore::ApplyChunkState(
    const ChunkCoord& chunk_coord,
    std::vector<std::uint8_t> payload,
    std::vector<std::uint8_t> presence_bitmap) {
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    auto previous_payload = regular_chunk->payload;
    auto previous_presence = regular_chunk->presence_bitmap;

    regular_chunk->payload = std::move(payload);
    regular_chunk->presence_bitmap = std::move(presence_bitmap);
    CanonicalizeAbsentBlocks(geometry_, regular_chunk->presence_bitmap, &regular_chunk->payload);

    const bool payload_changed = regular_chunk->payload != previous_payload;
    const bool presence_changed = regular_chunk->presence_bitmap != previous_presence;
    if (!payload_changed && !presence_changed) {
        return;
    }

    // Snapshot every component needed for a full rollback, mirroring the
    // conditional path.
    const std::size_t saved_wal_batch_size = regular_chunk->wal_batch.size();
    const auto saved_pending_updates = regular_chunk->pending_updates;
    const auto saved_wal_bytes = regular_chunk->wal_bytes;
    const auto saved_pending_wal_flush_updates = regular_chunk->pending_wal_flush_updates;
    try {
        // Reserve the version token before any WAL staging so a
        // version-clock failure is a clean pre-WAL error.
        const std::uint64_t reserved_version = NextChunkVersion();
        // A full-chunk replace can span several records; the frame makes the
        // whole replace atomic across crash recovery.
        WalFrameBuilder frame(&regular_chunk->wal_batch);
        if (payload_changed) {
            frame.AppendSpan(0U, regular_chunk->payload.data(), regular_chunk->payload.size());
        }
        if (presence_changed) {
            frame.AppendSpan(
                static_cast<std::uint32_t>(geometry_.ChunkPayloadBytes()),
                regular_chunk->presence_bitmap.data(),
                regular_chunk->presence_bitmap.size());
        }
        const std::size_t appended_bytes = frame.Finish(reserved_version);
        const std::size_t appended_record_count = frame.record_count();

        FinishOrdinaryMutationLocked(
            chunk_coord,
            regular_chunk,
            appended_bytes,
            appended_record_count,
            reserved_version);
    } catch (...) {
        regular_chunk->payload = std::move(previous_payload);
        regular_chunk->presence_bitmap = std::move(previous_presence);
        // The batch is only ever appended to within this op (a successful
        // flush clears it but then never reaches this catch), so truncating
        // back to the pre-op length restores the exact saved content without
        // an O(batch) copy on every write.
        regular_chunk->wal_batch.resize(saved_wal_batch_size);
        regular_chunk->pending_updates = saved_pending_updates;
        regular_chunk->wal_bytes = saved_wal_bytes;
        regular_chunk->pending_wal_flush_updates = saved_pending_wal_flush_updates;
        throw;
    }
}

std::string ChunkStore::GetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return BitCodec::ExtractBits(regular_chunk->payload, 0, geometry_.ChunkPayloadBits());
}

std::vector<std::uint8_t> ChunkStore::GetChunkPayloadBytes(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return regular_chunk->payload;
}

std::string ChunkStore::GetChunkStateBits(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return BitCodec::ExtractBits(regular_chunk->payload, 0, geometry_.ChunkPayloadBits()) + "|" +
           PresenceBitsText(geometry_, regular_chunk->presence_bitmap);
}

std::vector<std::uint8_t> ChunkStore::GetChunkStateBytes(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return BuildChunkStateBytes(geometry_, regular_chunk->payload, regular_chunk->presence_bitmap);
}

}  // namespace chunkdb
