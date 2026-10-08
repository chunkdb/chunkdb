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
#include <type_traits>
#include <variant>
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
    if (const auto& pending = geometry_.layout().schema().pending;
        pending.has_value() && !ValueFits(pending->type, BitsValue{.digits = std::string(bits)})) {
        throw std::invalid_argument(
            "the block's bits are being narrowed to " + ColumnTypeName(pending->type) + ", which does not hold this value");
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
        WalFrameBuilder frame(&regular_chunk->wal_batch, geometry_.layout().schema().version);
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
    // Text and bytes values: what each should be afterwards, any order;
    // values already so are dropped when the write is applied.
    std::vector<VarChange> var_changes;
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
    write.var_changes.clear();
    write.given.clear();
    write.ranges.clear();
    return write;
}

void ChunkStore::UnsetBlock(std::int64_t block_x, std::int64_t block_y) {
    (void)UnsetBlock(block_x, block_y, std::nullopt);
}

ChunkMutationResult ChunkStore::UnsetBlock(
    std::int64_t block_x,
    std::int64_t block_y,
    std::optional<std::uint64_t> expected_version) {
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
        for (const auto& column : geometry_.layout().schema().columns) {
            if (!IsFixedWidth(column.type.kind)) {
                write.var_changes.push_back(VarChange{
                    .key = VarKey{.column_id = column.id, .block_index = static_cast<std::uint32_t>(block_index)},
                    .value = std::nullopt,
                });
            }
        }
        const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
        std::unique_lock lock(regular_chunk->mutex);
        if (expected_version.has_value() && regular_chunk->version != *expected_version) {
            return ChunkMutationResult{.ok = false, .version = regular_chunk->version};
        }
        WriteBlockColumnsLocked(chunk_coord, regular_chunk, block_index, write, false);
        return ChunkMutationResult{.ok = true, .version = regular_chunk->version};
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
    if (expected_version.has_value() && regular_chunk->version != *expected_version) {
        return ChunkMutationResult{.ok = false, .version = regular_chunk->version};
    }

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
        return ChunkMutationResult{.ok = true, .version = regular_chunk->version};
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
        WalFrameBuilder frame(&regular_chunk->wal_batch, geometry_.layout().schema().version);
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
    return ChunkMutationResult{.ok = true, .version = regular_chunk->version};
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
    (void)SetBlock(block_x, block_y, values, std::nullopt);
}

ChunkMutationResult ChunkStore::SetBlock(
    std::int64_t block_x,
    std::int64_t block_y,
    const std::vector<ColumnAssignment>& values,
    std::optional<std::uint64_t> expected_version) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
    ThrowIfDurabilityPoisoned();
    const auto& layout = geometry_.layout();
    const auto& columns = layout.schema().columns;

    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const auto key_of = [block_index](const Column& column) {
        return VarKey{.column_id = column.id, .block_index = static_cast<std::uint32_t>(block_index)};
    };

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
        const bool null = std::holds_alternative<std::monostate>(assignment.value);
        if (const auto& pending = layout.schema().pending;
            pending.has_value() && pending->column_id == column.id && !ValueFits(pending->type, assignment.value)) {
            throw std::invalid_argument(
                "column " + column.name + " is being narrowed to " + ColumnTypeName(pending->type) +
                ", which does not hold this value");
        }
        if (null && !column.nullable) {
            throw std::invalid_argument("column " + column.name + " cannot be NULL");
        }
        if (fixed == nullptr) {
            // No entry is NULL, or the empty value of a column that cannot be
            // NULL.
            auto bytes = null ? std::vector<std::uint8_t>{} : EncodeVarValue(column, assignment.value);
            write.var_changes.push_back(VarChange{
                .key = key_of(column),
                .value = null || (bytes.empty() && !column.nullable)
                             ? std::nullopt
                             : std::optional<std::vector<std::uint8_t>>(std::move(bytes)),
            });
            continue;
        }
        if (null) {
            (void)write.Add(fixed, true);
            continue;
        }
        EncodeColumnValue(column, assignment.value, write.Add(fixed, false));
    }

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);
    if (expected_version.has_value() && regular_chunk->version != *expected_version) {
        return ChunkMutationResult{.ok = false, .version = regular_chunk->version};
    }
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
            if (fixed == nullptr) {
                // A new block has no values yet: only a default that is not
                // stored as no entry is written.
                if (column.has_default && (!column.default_value.empty() || column.nullable)) {
                    write.var_changes.push_back(VarChange{.key = key_of(column), .value = column.default_value});
                }
            } else if (column.has_default) {
                std::copy(column.default_value.begin(), column.default_value.end(), write.Add(fixed, false));
            } else {
                (void)write.Add(fixed, column.nullable);
            }
        }
    }
    WriteBlockColumnsLocked(chunk_coord, regular_chunk, block_index, write, true);
    return ChunkMutationResult{.ok = true, .version = regular_chunk->version};
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
        const auto& column = layout.schema().columns[index];
        const auto* fixed = layout.FixedColumnAt(index);
        if (fixed == nullptr) {
            const auto value = regular_chunk->vars.Find(
                VarKey{.column_id = column.id, .block_index = static_cast<std::uint32_t>(block_index)});
            if (value.has_value()) {
                values.push_back(DecodeVarValue(column, *value));
            } else if (column.nullable) {
                values.emplace_back(std::monostate{});
            } else {
                values.push_back(DecodeVarValue(column, {}));
            }
            continue;
        }
        if (fixed->validity != ChunkLayout::kNoValidity &&
            ((payload[fixed->validity + block_index / 8U] >> (block_index % 8U)) & 1U) == 0U) {
            values.emplace_back(std::monostate{});
            continue;
        }
        bytes.resize((fixed->width + 7U) / 8U);
        ReadValueBits(payload, fixed->values * 8U + block_index * fixed->width, bytes.data(), fixed->width);
        values.push_back(DecodeColumnValue(column, bytes.data()));
    }
    return values;
}

void ChunkStore::WriteBlockColumnsLocked(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    std::size_t block_index,
    BlockWrite& write,
    bool present) {
    // The text and bytes values that change, ascending, checked against the
    // table's limit before anything changes.
    VarUpdate var_update;
    if (!write.var_changes.empty()) {
        std::sort(write.var_changes.begin(), write.var_changes.end(), [](const VarChange& lhs, const VarChange& rhs) {
            return lhs.key < rhs.key;
        });
        for (auto& change : write.var_changes) {
            const auto current = chunk->vars.Find(change.key);
            const bool same = change.value.has_value()
                                  ? current.has_value() && std::equal(current->begin(), current->end(),
                                                                      change.value->begin(), change.value->end())
                                  : !current.has_value();
            if (!same) {
                var_update.changes.push_back(std::move(change));
            }
        }
        RequireVarWrite(chunk->vars, var_update);
    }

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
    if (!presence_changed && var_update.empty() && std::none_of(ranges.begin(), ranges.end(), changed)) {
        return;
    }

    // Snapshot every component needed for a full rollback, as SetBlockBits
    // does: a rejected mutation must leave memory, the staged batch, the
    // counters, and the WAL file exactly as before the command.
    const std::size_t saved_wal_batch_size = chunk->wal_batch.size();
    const auto saved_pending_updates = chunk->pending_updates;
    const auto saved_wal_bytes = chunk->wal_bytes;
    const auto saved_pending_wal_flush_updates = chunk->pending_wal_flush_updates;
    VarUndo var_undo;
    try {
        if (!var_update.empty()) {
            var_undo = ApplyVarUpdate(&chunk->vars, std::move(var_update));
        }
        const std::uint64_t reserved_version = NextChunkVersion();
        const std::uint64_t commit_time_ms = NextCommitTimeMs(*chunk);
        // One mutation is one WAL frame, applied all-or-nothing on replay.
        WalFrameBuilder frame(&chunk->wal_batch, geometry_.layout().schema().version);
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
        frame.AppendVarUpdate(chunk->vars, var_undo);
        const std::size_t appended_bytes = frame.Finish(reserved_version, commit_time_ms);

        FinishOrdinaryMutationLocked(chunk_coord, chunk, appended_bytes, reserved_version, commit_time_ms);
    } catch (...) {
        restore();
        UndoVarUpdate(&chunk->vars, std::move(var_undo));
        chunk->wal_batch.resize(saved_wal_batch_size);
        chunk->pending_updates = saved_pending_updates;
        chunk->wal_bytes = saved_wal_bytes;
        chunk->pending_wal_flush_updates = saved_pending_wal_flush_updates;
        throw;
    }
}

void ChunkStore::RequirePendingFits(
    const std::vector<std::uint8_t>& payload,
    const std::vector<std::uint8_t>& presence) const {
    const auto& layout = geometry_.layout();
    const auto& pending = layout.schema().pending;
    if (!pending.has_value()) {
        return;
    }
    const std::size_t index = layout.IndexOfId(pending->column_id);
    const auto* fixed = layout.FixedColumnAt(index);
    if (fixed == nullptr) {
        return;  // whole-chunk writes carry no text or bytes values
    }
    const Column& column = layout.schema().columns[index];
    std::vector<std::uint8_t> bytes((fixed->width + 7U) / 8U);
    for (std::size_t block = 0; block < layout.block_count(); ++block) {
        if (!BlockPresent(presence, block) ||
            (fixed->validity != ChunkLayout::kNoValidity && ((payload[fixed->validity + block / 8U] >> (block % 8U)) & 1U) == 0U)) {
            continue;
        }
        ReadValueBits(payload.data(), fixed->values * 8U + block * fixed->width, bytes.data(), fixed->width);
        if (!ValueFits(pending->type, DecodeColumnValue(column, bytes.data()))) {
            throw std::invalid_argument(
                "column " + column.name + " is being narrowed to " + ColumnTypeName(pending->type) +
                ", which does not hold the value of block index " + std::to_string(block));
        }
    }
}

namespace {

[[nodiscard]] std::string DescribeValue(const ColumnValue& value) {
    return std::visit(
        [](const auto& v) -> std::string {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
                return "NULL";
            } else if constexpr (std::is_same_v<T, bool>) {
                return v ? "true" : "false";
            } else if constexpr (std::is_same_v<T, BitsValue>) {
                return v.digits;
            } else if constexpr (std::is_same_v<T, std::string>) {
                return "'" + v + "'";
            } else if constexpr (std::is_same_v<T, BytesValue>) {
                return std::to_string(v.bytes.size()) + " bytes";
            } else {
                std::ostringstream out;
                out << v;
                return out.str();
            }
        },
        value);
}

}  // namespace

std::optional<std::string> ChunkStore::FindValueNotFitting(std::uint32_t column_id, const ColumnType& type) {
    const auto& layout = geometry_.layout();
    const std::size_t index = layout.IndexOfId(column_id);
    if (index == std::string_view::npos) {
        throw std::invalid_argument("the table has no column with id " + std::to_string(column_id));
    }
    const Column& column = layout.schema().columns[index];
    const auto* fixed = layout.FixedColumnAt(index);
    const auto width = static_cast<std::int64_t>(geometry_.config().chunk_width_blocks);
    const auto height = static_cast<std::int64_t>(geometry_.config().chunk_height_blocks);
    const auto describe = [&](const ChunkCoord& coord, std::size_t block, const ColumnValue& value) {
        const auto local_x = static_cast<std::int64_t>(block) % width;
        const auto local_y = static_cast<std::int64_t>(block) / width;
        return "block (" + std::to_string(coord.x * width + local_x) + ", " + std::to_string(coord.y * height + local_y) +
               ") holds " + DescribeValue(value);
    };
    std::vector<std::uint8_t> bytes(fixed != nullptr ? (fixed->width + 7U) / 8U : 0U);
    bool has_cursor = false;
    ChunkCoord cursor{};
    while (true) {
        const auto page = ScanPopulatedChunks(has_cursor, cursor, kMaxChunkScanLimit);
        for (const auto& coord : page.coords) {
            const auto chunk = GetOrLoadRegularChunk(coord);
            std::shared_lock lock(chunk->mutex);
            if (fixed == nullptr) {
                for (const auto entry : chunk->vars) {
                    if (entry.key.column_id != column_id) {
                        continue;
                    }
                    const ColumnValue value = DecodeVarValue(column, entry.value);
                    if (!ValueFits(type, value)) {
                        return describe(coord, entry.key.block_index, value);
                    }
                }
                continue;
            }
            for (std::size_t block = 0; block < layout.block_count(); ++block) {
                if (!BlockPresent(chunk->presence_bitmap, block) ||
                    (fixed->validity != ChunkLayout::kNoValidity &&
                     ((chunk->payload[fixed->validity + block / 8U] >> (block % 8U)) & 1U) == 0U)) {
                    continue;
                }
                ReadValueBits(chunk->payload.data(), fixed->values * 8U + block * fixed->width, bytes.data(), fixed->width);
                const ColumnValue value = DecodeColumnValue(column, bytes.data());
                if (!ValueFits(type, value)) {
                    return describe(coord, block, value);
                }
            }
        }
        if (!page.has_more || page.coords.empty()) {
            return std::nullopt;
        }
        cursor = page.coords.back();
        has_cursor = true;
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
    ApplyChunkState({chunk_x, chunk_y}, std::move(payload), std::move(presence_bitmap));
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
    return ApplyChunkState({chunk_x, chunk_y}, std::move(canonical_payload), std::move(canonical_presence));
}

std::uint64_t ChunkStore::ApplyChunkState(
    const ChunkCoord& chunk_coord,
    std::vector<std::uint8_t> payload,
    std::vector<std::uint8_t> presence_bitmap) {
    CanonicalizeAbsentBlocks(geometry_, presence_bitmap, &payload);
    RequirePendingFits(payload, presence_bitmap);

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    // Rejected before anything changes.
    auto var_update = VarUpdateForState(regular_chunk->vars, presence_bitmap);

    const bool payload_changed = payload != regular_chunk->payload;
    const bool presence_changed = presence_bitmap != regular_chunk->presence_bitmap;
    if (!payload_changed && !presence_changed && var_update.empty()) {
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
    VarUndo var_undo;
    try {
        if (!var_update.empty()) {
            var_undo = ApplyVarUpdate(&regular_chunk->vars, std::move(var_update));
        }
        // Reserve the version token before any WAL staging so a
        // version-clock failure is a clean pre-WAL error.
        const std::uint64_t reserved_version = NextChunkVersion();
        const std::uint64_t commit_time_ms = NextCommitTimeMs(*regular_chunk);
        // A full-chunk replace can span several records; the frame makes the
        // whole replace atomic across crash recovery.
        WalFrameBuilder frame(&regular_chunk->wal_batch, geometry_.layout().schema().version);
        if (payload_changed) {
            frame.AppendSpan(0U, regular_chunk->payload.data(), regular_chunk->payload.size());
        }
        if (presence_changed) {
            frame.AppendSpan(
                static_cast<std::uint32_t>(geometry_.ChunkPayloadBytes()),
                regular_chunk->presence_bitmap.data(),
                regular_chunk->presence_bitmap.size());
        }
        frame.AppendVarUpdate(regular_chunk->vars, var_undo);
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
        UndoVarUpdate(&regular_chunk->vars, std::move(var_undo));
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

VarUpdate ChunkStore::VarUpdateForState(const ChunkVars& current, const std::vector<std::uint8_t>& presence) {
    VarUpdate update;
    for (const auto entry : current) {
        if (!BlockPresent(presence, entry.key.block_index)) {
            update.changes.push_back(VarChange{.key = entry.key, .value = std::nullopt});
        }
    }
    return update;
}

void ChunkStore::RequireVarWrite(const ChunkVars& current, const VarUpdate& update) const {
    if (update.empty()) {
        return;
    }
    // A chunk over a lowered limit may still shrink.
    const std::size_t after = VarsSizeAfter(current, update);
    if (after > var_max_chunk_bytes_ && after > current.encoded_size()) {
        throw std::invalid_argument(
            "the text and bytes values of the chunk would take " + std::to_string(after) +
            " bytes, more than var_max_chunk_bytes (" + std::to_string(var_max_chunk_bytes_) + ")");
    }
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

}  // namespace chunkdb
