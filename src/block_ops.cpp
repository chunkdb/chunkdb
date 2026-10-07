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
    RequireBitStringBlocks();
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

std::optional<std::string> ChunkStore::ReadBlockBits(
    std::int64_t block_x,
    std::int64_t block_y) {
    RequireBitStringBlocks();
    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const std::size_t bit_offset = block_index * geometry_.config().block_bits;

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    if (!BlockPresent(regular_chunk->presence_bitmap, block_index)) {
        return std::nullopt;
    }
    return BitCodec::ExtractBits(regular_chunk->payload, bit_offset, geometry_.config().block_bits);
}

void ChunkStore::FinishOrdinaryMutationLocked(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    std::size_t appended_bytes,
    std::uint64_t reserved_version,
    std::uint64_t commit_time_ms) {
    // Group commit counts mutations, whatever number of records each one
    // appended (docs/STORAGE_FORMAT.md Section 5).
    chunk->pending_wal_flush_updates += 1;
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
    chunk->commit_time_ms = commit_time_ms;
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
    RequireBitStringBlocks();
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
        const std::uint64_t commit_time_ms = NextCommitTimeMs(*regular_chunk);
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
        const std::size_t appended_bytes = frame.Finish(reserved_version, commit_time_ms);

        FinishOrdinaryMutationLocked(
            chunk_coord,
            regular_chunk,
            appended_bytes,
            reserved_version,
            commit_time_ms);
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

struct ChunkStore::BlockWrite {
    struct Edit {
        const ChunkLayout::FixedColumn* fixed = nullptr;
        bool null = false;
        // Offset of the value (as EncodeColumnValue writes it) in `values`.
        std::size_t value = 0;
    };
    // A byte range of PAYLOAD the write touches, and where its previous
    // bytes are kept in RegularChunk::scratch_before.
    struct Range {
        std::size_t begin = 0;
        std::size_t size = 0;
        std::size_t saved = 0;
    };

    std::vector<Edit> edits;
    std::vector<std::uint8_t> values;
    // Per schema column: given by the caller.
    std::vector<std::uint8_t> given;
    std::vector<Range> ranges;

    // An edit whose value starts as zero bytes; the pointer is valid until
    // the next Add.
    std::uint8_t* Add(const ChunkLayout::FixedColumn* fixed, bool null) {
        const std::size_t offset = values.size();
        values.resize(offset + (fixed->width + 7U) / 8U, 0U);
        edits.push_back(Edit{.fixed = fixed, .null = null, .value = offset});
        return values.data() + offset;
    }
};

ChunkStore::BlockWrite& ChunkStore::ThreadBlockWrite() {
    // One per thread and reused, so a block write allocates nothing once its
    // buffers have grown.
    thread_local BlockWrite write;
    write.edits.clear();
    write.values.clear();
    write.given.clear();
    write.ranges.clear();
    return write;
}

void ChunkStore::UnsetBlock(std::int64_t block_x, std::int64_t block_y) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
    ThrowIfDurabilityPoisoned();
    if (!geometry_.layout().bit_string_blocks()) {
        const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
        const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
        const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
        auto& write = ThreadBlockWrite();
        for (const auto& fixed : geometry_.layout().fixed_columns()) {
            (void)write.Add(&fixed, fixed.validity != ChunkLayout::kNoValidity);
        }
        const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
        std::unique_lock lock(regular_chunk->mutex);
        WriteBlockColumnsLocked(chunk_coord, regular_chunk, block_index, write, false);
        return;
    }

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
    ExtraUndo extra_undo;
    try {
        // Extra data belongs to a present block: it goes with the block.
        if (presence_changed &&
            regular_chunk->extra.Find(static_cast<std::uint32_t>(block_index)).has_value()) {
            ExtraUpdate removal;
            removal.changes.push_back(
                ExtraChange{.block_index = static_cast<std::uint32_t>(block_index)});
            extra_undo = ApplyExtraUpdate(&regular_chunk->extra, std::move(removal));
        }
        // Reserve the version token before any WAL staging so a
        // version-clock failure is a clean pre-WAL error.
        const std::uint64_t reserved_version = NextChunkVersion();
        const std::uint64_t commit_time_ms = NextCommitTimeMs(*regular_chunk);
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
        frame.AppendExtraUpdate(regular_chunk->extra, extra_undo);
        const std::size_t appended_bytes = frame.Finish(reserved_version, commit_time_ms);

        FinishOrdinaryMutationLocked(
            chunk_coord,
            regular_chunk,
            appended_bytes,
            reserved_version,
            commit_time_ms);
    } catch (...) {
        std::copy(
            previous_bytes.begin(),
            previous_bytes.end(),
            regular_chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin_byte));
        regular_chunk->presence_bitmap[presence_byte_index] = previous_presence_byte;
        UndoExtraUpdate(&regular_chunk->extra, std::move(extra_undo));
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

void ChunkStore::RequireBitStringBlocks() const {
    if (!geometry_.layout().bit_string_blocks()) {
        throw std::invalid_argument("bit strings need a table with one bits(N) column; this table has columns");
    }
}

void ChunkStore::SetBlock(
    std::int64_t block_x,
    std::int64_t block_y,
    const std::vector<ColumnAssignment>& values) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
    ThrowIfDurabilityPoisoned();
    const auto& layout = geometry_.layout();
    const auto& columns = layout.schema().columns;

    // Every value is checked before the chunk is touched.
    auto& write = ThreadBlockWrite();
    write.given.assign(columns.size(), 0U);
    for (const auto& assignment : values) {
        const std::size_t index = layout.FindColumn(assignment.column);
        if (index == std::string_view::npos) {
            throw std::invalid_argument("the table has no column " + assignment.column);
        }
        if (write.given[index] != 0U) {
            throw std::invalid_argument("column " + assignment.column + " is given twice");
        }
        write.given[index] = 1U;
        const auto& column = columns[index];
        const auto* fixed = layout.FixedColumnAt(index);
        if (std::holds_alternative<std::monostate>(assignment.value)) {
            if (!column.nullable) {
                throw std::invalid_argument("column " + column.name + " cannot be NULL");
            }
            (void)write.Add(fixed, true);
            continue;
        }
        EncodeColumnValue(column, assignment.value, write.Add(fixed, false));
    }

    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);
    if (!BlockPresent(regular_chunk->presence_bitmap, block_index)) {
        // A new block: every column not given takes its DEFAULT, NULL, or
        // zero; a REQUIRED one must be given.
        for (std::size_t index = 0; index < columns.size(); ++index) {
            if (write.given[index] != 0U) {
                continue;
            }
            const auto& column = columns[index];
            if (column.required) {
                throw std::invalid_argument("column " + column.name + " is REQUIRED: a new block must give it");
            }
            const auto* fixed = layout.FixedColumnAt(index);
            if (column.has_default) {
                std::copy(column.default_value.begin(), column.default_value.end(), write.Add(fixed, false));
            } else {
                (void)write.Add(fixed, column.nullable);
            }
        }
    }
    WriteBlockColumnsLocked(chunk_coord, regular_chunk, block_index, write, true);
}

std::optional<std::vector<ColumnValue>> ChunkStore::GetBlock(std::int64_t block_x, std::int64_t block_y) {
    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const auto& layout = geometry_.layout();

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    if (!BlockPresent(regular_chunk->presence_bitmap, block_index)) {
        return std::nullopt;
    }
    std::vector<ColumnValue> values;
    values.reserve(layout.schema().columns.size());
    std::vector<std::uint8_t> bytes;
    const std::uint8_t* payload = regular_chunk->payload.data();
    for (std::size_t index = 0; index < layout.schema().columns.size(); ++index) {
        const auto* fixed = layout.FixedColumnAt(index);
        if (fixed->validity != ChunkLayout::kNoValidity &&
            ((payload[fixed->validity + block_index / 8U] >> (block_index % 8U)) & 1U) == 0U) {
            values.emplace_back(std::monostate{});
            continue;
        }
        bytes.resize((fixed->width + 7U) / 8U);
        ReadValueBits(payload, fixed->values * 8U + block_index * fixed->width, bytes.data(), fixed->width);
        values.push_back(DecodeColumnValue(layout.schema().columns[index], bytes.data()));
    }
    return values;
}

void ChunkStore::WriteBlockColumnsLocked(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    std::size_t block_index,
    BlockWrite& write,
    bool present) {
    // Per column its value's bytes and the byte of its validity bit. Columns
    // are byte-aligned arrays, so the ranges never overlap.
    auto& ranges = write.ranges;
    auto& saved = chunk->scratch_before;
    saved.clear();
    const auto keep = [&](std::size_t begin, std::size_t size) {
        ranges.push_back(BlockWrite::Range{.begin = begin, .size = size, .saved = saved.size()});
        saved.insert(
            saved.end(),
            chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin),
            chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin + size));
    };
    for (const auto& edit : write.edits) {
        const std::size_t bit = edit.fixed->values * 8U + block_index * edit.fixed->width;
        keep(bit / 8U, (bit + edit.fixed->width - 1U) / 8U - bit / 8U + 1U);
        if (edit.fixed->validity != ChunkLayout::kNoValidity) {
            keep(edit.fixed->validity + block_index / 8U, 1U);
        }
    }
    const std::size_t presence_byte_index = block_index / 8U;
    const std::uint8_t previous_presence_byte = chunk->presence_bitmap[presence_byte_index];
    const auto restore = [&] {
        for (const auto& range : ranges) {
            std::copy_n(
                saved.begin() + static_cast<std::ptrdiff_t>(range.saved),
                static_cast<std::ptrdiff_t>(range.size),
                chunk->payload.begin() + static_cast<std::ptrdiff_t>(range.begin));
        }
        chunk->presence_bitmap[presence_byte_index] = previous_presence_byte;
    };

    for (const auto& edit : write.edits) {
        WriteValueBits(
            chunk->payload.data(),
            edit.fixed->values * 8U + block_index * edit.fixed->width,
            write.values.data() + edit.value,
            edit.fixed->width);
        if (edit.fixed->validity != ChunkLayout::kNoValidity) {
            auto& byte = chunk->payload[edit.fixed->validity + block_index / 8U];
            const auto mask = static_cast<std::uint8_t>(1U << (block_index % 8U));
            byte = edit.null ? static_cast<std::uint8_t>(byte & ~mask) : static_cast<std::uint8_t>(byte | mask);
        }
    }
    SetBlockPresent(&chunk->presence_bitmap, block_index, present);

    const auto changed = [&](const BlockWrite::Range& range) {
        return !std::equal(
            saved.begin() + static_cast<std::ptrdiff_t>(range.saved),
            saved.begin() + static_cast<std::ptrdiff_t>(range.saved + range.size),
            chunk->payload.begin() + static_cast<std::ptrdiff_t>(range.begin));
    };
    const bool presence_changed = previous_presence_byte != chunk->presence_bitmap[presence_byte_index];
    if (!presence_changed && std::none_of(ranges.begin(), ranges.end(), changed)) {
        return;
    }

    // Snapshot every component needed for a full rollback, as SetBlockBits
    // does: a rejected mutation must leave memory, the staged batch, the
    // counters, and the WAL file exactly as before the command.
    const std::size_t saved_wal_batch_size = chunk->wal_batch.size();
    const auto saved_pending_updates = chunk->pending_updates;
    const auto saved_wal_bytes = chunk->wal_bytes;
    const auto saved_pending_wal_flush_updates = chunk->pending_wal_flush_updates;
    try {
        const std::uint64_t reserved_version = NextChunkVersion();
        const std::uint64_t commit_time_ms = NextCommitTimeMs(*chunk);
        // One mutation is one WAL frame, applied all-or-nothing on replay.
        WalFrameBuilder frame(&chunk->wal_batch);
        for (const auto& range : ranges) {
            if (changed(range)) {
                frame.AppendSpan(
                    static_cast<std::uint32_t>(range.begin), chunk->payload.data() + range.begin, range.size);
            }
        }
        if (presence_changed) {
            frame.AppendSpan(
                static_cast<std::uint32_t>(geometry_.ChunkPayloadBytes() + presence_byte_index),
                &chunk->presence_bitmap[presence_byte_index],
                1U);
        }
        const std::size_t appended_bytes = frame.Finish(reserved_version, commit_time_ms);

        FinishOrdinaryMutationLocked(chunk_coord, chunk, appended_bytes, reserved_version, commit_time_ms);
    } catch (...) {
        restore();
        chunk->wal_batch.resize(saved_wal_batch_size);
        chunk->pending_updates = saved_pending_updates;
        chunk->wal_bytes = saved_wal_bytes;
        chunk->pending_wal_flush_updates = saved_pending_wal_flush_updates;
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
    RequireBitStringBlocks();
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
    RequireBitStringBlocks();
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
    ApplyChunkState({chunk_x, chunk_y}, std::move(payload), std::move(presence_bitmap), nullptr);
}

std::uint64_t ChunkStore::SetChunkPayloadBytes(
    std::int64_t chunk_x,
    std::int64_t chunk_y,
    const std::vector<std::uint8_t>& payload) {
    return SetChunkStateBytes(chunk_x, chunk_y, payload, FullPresenceBitmap(geometry_));
}

std::uint64_t ChunkStore::SetChunkStateBytes(
    std::int64_t chunk_x,
    std::int64_t chunk_y,
    const std::vector<std::uint8_t>& payload,
    const std::vector<std::uint8_t>& presence_bitmap,
    const ChunkExtra& extra) {
    if (extra_max_block_bits_ == 0U) {
        throw std::invalid_argument(std::string(kExtraDataDisabled));
    }
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
    return ApplyChunkState(
        {chunk_x, chunk_y}, std::move(canonical_payload), std::move(canonical_presence), &extra);
}

std::uint64_t ChunkStore::SetChunkStateBytes(
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
    return ApplyChunkState(
        {chunk_x, chunk_y}, std::move(canonical_payload), std::move(canonical_presence), nullptr);
}

std::uint64_t ChunkStore::ApplyChunkState(
    const ChunkCoord& chunk_coord,
    std::vector<std::uint8_t> payload,
    std::vector<std::uint8_t> presence_bitmap,
    const ChunkExtra* extra) {
    CanonicalizeAbsentBlocks(geometry_, presence_bitmap, &payload);

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    // Rejected before anything changes.
    auto extra_update = ExtraUpdateForState(regular_chunk->extra, presence_bitmap, extra);
    RequireExtraWrite(regular_chunk->extra, extra_update);

    const bool payload_changed = payload != regular_chunk->payload;
    const bool presence_changed = presence_bitmap != regular_chunk->presence_bitmap;
    if (!payload_changed && !presence_changed && extra_update.empty()) {
        return regular_chunk->version;
    }

    auto previous_payload = std::exchange(regular_chunk->payload, std::move(payload));
    auto previous_presence = std::exchange(regular_chunk->presence_bitmap, std::move(presence_bitmap));

    // Snapshot every component needed for a full rollback, mirroring the
    // conditional path.
    const std::size_t saved_wal_batch_size = regular_chunk->wal_batch.size();
    const auto saved_pending_updates = regular_chunk->pending_updates;
    const auto saved_wal_bytes = regular_chunk->wal_bytes;
    const auto saved_pending_wal_flush_updates = regular_chunk->pending_wal_flush_updates;
    ExtraUndo extra_undo;
    try {
        extra_undo = ApplyExtraUpdate(&regular_chunk->extra, std::move(extra_update));
        // Reserve the version token before any WAL staging so a
        // version-clock failure is a clean pre-WAL error.
        const std::uint64_t reserved_version = NextChunkVersion();
        const std::uint64_t commit_time_ms = NextCommitTimeMs(*regular_chunk);
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
        frame.AppendExtraUpdate(regular_chunk->extra, extra_undo);
        const std::size_t appended_bytes = frame.Finish(reserved_version, commit_time_ms);

        FinishOrdinaryMutationLocked(
            chunk_coord,
            regular_chunk,
            appended_bytes,
            reserved_version,
            commit_time_ms);
    } catch (...) {
        regular_chunk->payload = std::move(previous_payload);
        regular_chunk->presence_bitmap = std::move(previous_presence);
        UndoExtraUpdate(&regular_chunk->extra, std::move(extra_undo));
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
    return regular_chunk->version;
}

ExtraUpdate ChunkStore::ExtraUpdateForState(
    const ChunkExtra& current,
    const std::vector<std::uint8_t>& presence,
    const ChunkExtra* extra) const {
    ExtraUpdate update;
    if (extra != nullptr) {
        const std::size_t block_count = geometry_.ChunkBlockCount();
        for (const auto entry : *extra) {
            if (entry.block_index >= block_count || !BlockPresent(presence, entry.block_index)) {
                throw std::invalid_argument(
                    "extra data for block index " + std::to_string(entry.block_index) +
                    ", which the chunk state leaves absent");
            }
        }
        if (*extra != current) {
            update.replace = *extra;
        }
        return update;
    }
    for (const auto entry : current) {
        if (!BlockPresent(presence, entry.block_index)) {
            update.changes.push_back(ExtraChange{.block_index = entry.block_index});
        }
    }
    return update;
}

void ChunkStore::RequireExtraWrite(const ChunkExtra& current, const ExtraUpdate& update) const {
    if (update.empty()) {
        return;
    }
    const auto check_value = [&](std::uint32_t block_index, const auto& value) {
        if (const auto existing = current.Find(block_index);
            existing.has_value() && *existing == value) {
            return;
        }
        if (extra_max_block_bits_ == 0U) {
            throw std::invalid_argument(std::string(kExtraDataDisabled));
        }
        if (value.bit_length > extra_max_block_bits_) {
            throw std::invalid_argument(
                "extra data of " + std::to_string(value.bit_length) +
                " bits exceeds extra_max_block_bits (" + std::to_string(extra_max_block_bits_) +
                ")");
        }
    };
    if (update.replace.has_value()) {
        for (const auto entry : *update.replace) {
            check_value(entry.block_index, entry.value);
        }
    } else {
        for (const auto& change : update.changes) {
            if (change.value.has_value()) {
                check_value(change.block_index, *change.value);
            }
        }
    }
    // A chunk over a lowered limit may still shrink.
    const std::size_t after = ExtraSizeAfter(current, update);
    if (after > extra_max_chunk_bytes_ && after > current.encoded_size()) {
        throw std::invalid_argument(
            "extra data of the chunk would take " + std::to_string(after) +
            " bytes, more than extra_max_chunk_bytes (" + std::to_string(extra_max_chunk_bytes_) +
            ")");
    }
}

std::optional<ExtraValue> ChunkStore::GetBlockExtra(std::int64_t block_x, std::int64_t block_y) {
    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const auto block_index =
        static_cast<std::uint32_t>(geometry_.LocalBlockIndex(local_x, local_y));

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    const auto value = regular_chunk->extra.Find(block_index);
    if (!value.has_value()) {
        return std::nullopt;
    }
    return value->ToValue();
}

std::uint64_t ChunkStore::PutBlockExtra(std::int64_t block_x, std::int64_t block_y, ExtraValue value) {
    return ChangeBlockExtra(block_x, block_y, std::move(value));
}

std::uint64_t ChunkStore::DeleteBlockExtra(std::int64_t block_x, std::int64_t block_y) {
    return ChangeBlockExtra(block_x, block_y, std::nullopt);
}

std::uint64_t ChunkStore::ChangeBlockExtra(
    std::int64_t block_x,
    std::int64_t block_y,
    std::optional<ExtraValue> value) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
    ThrowIfDurabilityPoisoned();
    if (extra_max_block_bits_ == 0U) {
        throw std::invalid_argument(std::string(kExtraDataDisabled));
    }
    if (value.has_value()) {
        // Validates the length and that the padding bits are clear.
        value = MakeExtraValue(value->bit_length, std::move(value->bytes), ExtraPadding::kReject);
    }

    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const auto block_index =
        static_cast<std::uint32_t>(geometry_.LocalBlockIndex(local_x, local_y));

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    const auto current = regular_chunk->extra.Find(block_index);
    if (value.has_value()) {
        if (!BlockPresent(regular_chunk->presence_bitmap, block_index)) {
            throw std::invalid_argument(
                "block (" + std::to_string(block_x) + "," + std::to_string(block_y) +
                ") is not set; extra data belongs to a present block");
        }
        if (current.has_value() && *current == *value) {
            return regular_chunk->version;
        }
    } else if (!current.has_value()) {
        return regular_chunk->version;
    }
    ExtraUpdate update;
    update.changes.push_back(ExtraChange{.block_index = block_index, .value = std::move(value)});
    RequireExtraWrite(regular_chunk->extra, update);

    // Snapshot every component needed for a full rollback, as for SET.
    const std::size_t saved_wal_batch_size = regular_chunk->wal_batch.size();
    const auto saved_pending_updates = regular_chunk->pending_updates;
    const auto saved_wal_bytes = regular_chunk->wal_bytes;
    const auto saved_pending_wal_flush_updates = regular_chunk->pending_wal_flush_updates;
    ExtraUndo extra_undo;
    try {
        extra_undo = ApplyExtraUpdate(&regular_chunk->extra, std::move(update));
        const std::uint64_t reserved_version = NextChunkVersion();
        const std::uint64_t commit_time_ms = NextCommitTimeMs(*regular_chunk);
        WalFrameBuilder frame(&regular_chunk->wal_batch);
        frame.AppendExtraUpdate(regular_chunk->extra, extra_undo);
        const std::size_t appended_bytes = frame.Finish(reserved_version, commit_time_ms);

        FinishOrdinaryMutationLocked(
            chunk_coord,
            regular_chunk,
            appended_bytes,
            reserved_version,
            commit_time_ms);
    } catch (...) {
        UndoExtraUpdate(&regular_chunk->extra, std::move(extra_undo));
        regular_chunk->wal_batch.resize(saved_wal_batch_size);
        regular_chunk->pending_updates = saved_pending_updates;
        regular_chunk->wal_bytes = saved_wal_bytes;
        regular_chunk->pending_wal_flush_updates = saved_pending_wal_flush_updates;
        throw;
    }
    return regular_chunk->version;
}

std::string ChunkStore::GetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y) {
    RequireBitStringBlocks();
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
    RequireBitStringBlocks();
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

std::vector<std::uint8_t> ChunkStore::GetChunkStateExtraBytes(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    auto bytes =
        BuildChunkStateBytes(geometry_, regular_chunk->payload, regular_chunk->presence_bitmap);
    regular_chunk->extra.EncodeTo(&bytes);
    return bytes;
}

}  // namespace chunkdb
