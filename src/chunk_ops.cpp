#include <algorithm>
#include <array>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunk_store_internal.hpp"
#include "checkpoint.hpp"
#include "chunkdb/bit_codec.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "txn_history.hpp"
#include "wal_writer.hpp"
#include "change_feed.hpp"

namespace chunkdb {

bool ChunkStore::ApplyFullChunkStateLocked(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    std::vector<std::uint8_t> new_payload,
    std::vector<std::uint8_t> new_presence,
    VarUpdate var_update) {
    if (new_payload == chunk->payload && new_presence == chunk->presence_bitmap &&
        var_update.empty()) {
        return false;
    }
    RequirePendingFits(new_payload, new_presence);
    // A WAL whose failed write or transaction could not be repaired takes
    // nothing more until a restart repairs it.
    if (chunk->wal_repair_failed) {
        throw std::runtime_error(
            "chunk (" + std::to_string(chunk_coord.x) + "," + std::to_string(chunk_coord.y) +
            ") is fail-closed: its WAL could not be repaired; restart the store");
    }

    // The whole canonical state is logged as one WAL frame starting at
    // offset zero. Replay applies a frame completely or not at all, so the
    // mutation is atomic across crash recovery for every geometry.

    // The rollback boundary below must cover every acknowledged frame and be
    // durable before an intent names it: a rollback to it would cut off
    // frames still in the batch, and a boundary past the synced end of the
    // WAL can be gone after a power loss, leaving an intent that cannot be
    // applied. A relaxed write, or a process that crashed, may also have
    // left the image this frame's value records apply over without a sync.
    MakeWalBoundaryDurableLocked(chunk_coord, chunk, /*own_stream=*/false);

    // Reserve the version token before anything about this mutation can become
    // visible. Reserving (and, if needed, persisting a higher ceiling) up front
    // means a version-allocation failure happens before the WAL append, so a
    // conditional mutation can never be committed on disk and then fail while
    // trying to obtain a token. A reserved-but-unused token is simply skipped,
    // which is harmless: the clock only has to stay monotonic.
    FeedWriteGuard feed(*this);
    feed.Before(chunk_coord, chunk->payload, chunk->presence_bitmap, chunk->vars);
    const std::uint64_t reserved_version = NextChunkVersion();
    feed.Version(reserved_version);
    const std::uint64_t commit_time_ms = NextCommitTimeMs(*chunk);
    // The state this write replaces, while a transaction is open.
    TxnKeep keep = PrepareTxnKeepLocked(chunk_coord, *chunk, reserved_version);

    // Snapshot every component needed for a full rollback. CurrentWalFileSize
    // throws on an inspection error rather than reporting an empty WAL, so a
    // failed rollback can never silently drop the truncation baseline.
    const std::uint64_t committed_wal_size = CurrentWalFileSize(chunk);
    const bool sync_required = durability_mode_ != DurabilityMode::kRelaxed;
    const auto previous_payload = chunk->payload;
    const auto previous_presence = chunk->presence_bitmap;
    const auto saved_wal_batch = chunk->wal_batch;
    const auto saved_pending_updates = chunk->pending_updates;
    const auto saved_wal_bytes = chunk->wal_bytes;
    const auto saved_pending_wal_flush_updates = chunk->pending_wal_flush_updates;
    SnapshotGenerationWriteGuard snapshot_write(this);
    std::filesystem::path rollback_intent_path;
    try {
        rollback_intent_path =
            WriteConditionalRollbackIntent(
                chunk_coord, chunk, committed_wal_size);
    } catch (...) {
        snapshot_write.Finish();
        throw;
    }

    // Intent establishment completed. Only now may the live vectors change.
    chunk->payload = std::move(new_payload);
    chunk->presence_bitmap = std::move(new_presence);

    bool commit_record_durable = false;
    VarUndo var_undo;
    try {
        if (!var_update.empty()) {
            var_undo = ApplyVarUpdate(&chunk->vars, std::move(var_update));
        }
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_INTENT_PUBLISH_ONCE")) {
            throw std::runtime_error(
                "injected conditional failure after rollback-intent publication");
        }
        // Log the payload and the presence bitmap as separate spans: a span
        // never straddles the payload/presence boundary, so replay's shape
        // guard stays strict even when a large state splits into several
        // records. The frame keeps the whole replace atomic.
        WalFrameBuilder frame(&chunk->wal_batch, geometry_.layout().schema().version, {}, SlotWriteUser());
        frame.AppendSpan(0U, chunk->payload.data(), chunk->payload.size());
        frame.AppendSpan(
            static_cast<std::uint32_t>(geometry_.ChunkPayloadBytes()),
            chunk->presence_bitmap.data(),
            chunk->presence_bitmap.size());
        frame.AppendVarUpdate(chunk->vars, var_undo);
        const std::size_t appended_bytes = frame.Finish(reserved_version, commit_time_ms);
        feed.Capture(chunk->wal_batch, appended_bytes);

        // One mutation, whatever its record count.
        chunk->pending_wal_flush_updates += 1;
        if (sync_required || chunk->pending_wal_flush_updates >= wal_group_commit_updates_) {
            FlushWalBatch(chunk_coord, chunk, sync_required);
        }

        chunk->pending_updates += 1;
        chunk->wal_bytes += appended_bytes;

        PauseConditionalMutationForTests(
            ConditionalMutationPausePoint::kAfterWalAppend);

        // Deterministic hook simulating a post-append failure (e.g. a later
        // durability step) that forces the rollback path with the rejected
        // record already on disk.
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE")) {
            throw std::runtime_error("injected conditional-mutation failure after WAL append");
        }

        // Publishing and syncing CKRC is the commit point. Recovery preserves
        // the WAL whether the committed marker or its later unlink survives.
        commit_record_durable = PublishConditionalCommitIntent(
            rollback_intent_path, committed_wal_size);
    } catch (...) {
        // CKRB still owns the prior boundary, so fully roll back memory and
        // the WAL tail. If local repair cannot finish, startup repeats the
        // repair before replay and the live store is poisoned.
        chunk->payload = previous_payload;
        chunk->presence_bitmap = previous_presence;
        UndoVarUpdate(&chunk->vars, std::move(var_undo));
        chunk->wal_batch = saved_wal_batch;
        chunk->pending_updates = saved_pending_updates;
        chunk->wal_bytes = saved_wal_bytes;
        chunk->pending_wal_flush_updates = saved_pending_wal_flush_updates;
        try {
            TruncateWalTail(chunk_coord, chunk, committed_wal_size, sync_required);
            std::error_code remove_intent_ec;
            std::filesystem::remove(rollback_intent_path, remove_intent_ec);
            if (remove_intent_ec) {
                throw std::runtime_error(
                    "failed to clear rollback intent after WAL repair: " +
                    rollback_intent_path.string() + " (" +
                    remove_intent_ec.message() + ")");
            }
            SyncDirectoryPath(rollback_intent_path.parent_path());
        } catch (const std::exception& truncate_error) {
            // The freshly appended WAL record could not be neutralized, so on a
            // later crash recovery repairs it from the retained durable intent.
            // Memory has already been rolled back, so continuing to serve would
            // leave memory and disk incoherent. Poison the store until restart.
            const std::string reason =
                "WAL rollback after a rejected conditional mutation on chunk (" +
                std::to_string(chunk_coord.x) + "," + std::to_string(chunk_coord.y) +
                ") failed: " + truncate_error.what();
            LogMessage(
                LogLevel::kError,
                LogComponent::kRecovery,
                "failed to neutralize WAL after rejected conditional mutation; poisoning store",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"error", truncate_error.what()},
                });
            PoisonDurability(reason);
            chunk->wal_repair_failed = true;
        }
        snapshot_write.Finish();
        throw;
    }

    chunk->version = reserved_version;
    chunk->commit_time_ms = commit_time_ms;
    // Still under the chunk's lock, so no read sees the new state without
    // the kept one.
    txn_history_->Publish(keep.node);
    feed.Commit();
    try {
        ClearCommittedConditionalIntent(rollback_intent_path);
    } catch (const std::exception& cleanup_error) {
        if (!commit_record_durable) {
            // The CKRC replace was visible but its first directory sync did
            // not complete. A successful sync now makes either the retained
            // CKRC or its already-visible unlink durable; both recover as
            // committed. Never enter the rollback path after CKRC is visible.
            try {
                if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CONDITIONAL_COMMIT_RETRY_SYNC_FAIL_ONCE")) {
                    throw std::runtime_error("injected commit intent retry sync failure");
                }
                SyncDirectoryPath(rollback_intent_path.parent_path());
                commit_record_durable = true;
            } catch (const std::exception& completion_error) {
                const std::string reason =
                    "visible commit intent could not be made durable after "
                    "cleanup failure on chunk (" +
                    std::to_string(chunk_coord.x) + "," +
                    std::to_string(chunk_coord.y) + "): " +
                    completion_error.what();
                PoisonDurability(reason);
                // Past the commit point: the write is applied in memory and
                // a restart may keep it or not, so the error must not read
                // as "not applied".
                throw WriteOutcomeUnknownError(
                    "the conditional write on chunk (" + std::to_string(chunk_coord.x) + "," +
                    std::to_string(chunk_coord.y) +
                    ") may or may not be applied: its commit record could not be made durable (" +
                    completion_error.what() + "); the store is fail-closed until restart");
            }
        }
        // The mutation is committed. CKRC is safe if retained: startup removes
        // it without truncating any WAL bytes. If the unlink survives, normal
        // replay reaches the same committed state.
        LogMessage(
            LogLevel::kWarn,
            LogComponent::kRecovery,
            "conditional mutation committed but intent cleanup was incomplete",
            {
                {"chunk_x", std::to_string(chunk_coord.x)},
                {"chunk_y", std::to_string(chunk_coord.y)},
                {"error", cleanup_error.what()},
            });
    }
    (void)commit_record_durable;

    try {
        MaybeCheckpointChunk(chunk_coord, chunk);
    } catch (const std::exception& checkpoint_error) {
        LogMessage(
            LogLevel::kWarn,
            LogComponent::kRecovery,
            "conditional mutation committed in WAL but inline checkpoint failed; "
            "retaining committed mutation for retry",
            {
                {"chunk_x", std::to_string(chunk_coord.x)},
                {"chunk_y", std::to_string(chunk_coord.y)},
                {"error", checkpoint_error.what()},
            });
    }
    try {
        snapshot_write.Finish();
    } catch (const std::exception& finish_error) {
        // The mutation is already committed; a generation-publication
        // failure here must not surface as a command error. The epoch stays
        // failed, so subsequent transitions fail closed until restart.
        LogMessage(
            LogLevel::kWarn,
            LogComponent::kRecovery,
            "conditional mutation committed but snapshot generation could not be republished",
            {
                {"chunk_x", std::to_string(chunk_coord.x)},
                {"chunk_y", std::to_string(chunk_coord.y)},
                {"error", finish_error.what()},
            });
    }
    return true;
}

ChunkMutationResult ChunkStore::CasChunkState(
    std::int64_t chunk_x,
    std::int64_t chunk_y,
    std::uint64_t expected_version,
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
    if (!BitCodec::IsBitString(payload_bits) || !BitCodec::IsBitString(presence_bits)) {
        throw std::invalid_argument("bit strings must contain only 0 and 1");
    }
    auto payload = EmptyPayload();
    BitCodec::WriteBits(payload, 0, payload_bits);
    auto presence = EmptyPresenceBitmap();
    BitCodec::WriteBits(presence, 0, presence_bits);
    return CasChunkStateBytes(chunk_x, chunk_y, expected_version, payload, presence);
}

ChunkMutationResult ChunkStore::CasChunkStateBytes(
    std::int64_t chunk_x,
    std::int64_t chunk_y,
    std::uint64_t expected_version,
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
    auto new_payload = payload;
    MaskUnusedPayloadBits(geometry_, &new_payload);
    auto new_presence = presence_bitmap;
    MaskUnusedPresenceBits(geometry_, &new_presence);
    CanonicalizeAbsentBlocks(geometry_, new_presence, &new_payload);

    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    if (regular_chunk->version != expected_version) {
        return ChunkMutationResult{.ok = false, .version = regular_chunk->version};
    }
    auto var_update = VarUpdateForState(regular_chunk->vars, new_presence);

    (void)ApplyFullChunkStateLocked(
        chunk_coord,
        regular_chunk,
        std::move(new_payload),
        std::move(new_presence),
        std::move(var_update));
    return ChunkMutationResult{.ok = true, .version = regular_chunk->version};
}

ChunkMutationResult ChunkStore::ApplyChunkBatch(
    std::int64_t chunk_x,
    std::int64_t chunk_y,
    bool has_expected_version,
    std::uint64_t expected_version,
    const std::vector<ChunkBatchOp>& ops) {
    RequireBitStringBlocks();
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
    ThrowIfDurabilityPoisoned();
    if (ops.empty() || ops.size() > kMaxChunkBatchOps) {
        throw std::invalid_argument(
            "batch must contain between 1 and " + std::to_string(kMaxChunkBatchOps) + " operations");
    }

    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const std::size_t block_bits = geometry_.config().block_bits;
    for (const auto& op : ops) {
        const ChunkCoord op_chunk = geometry_.BlockToChunk(op.x, op.y);
        if (!(op_chunk == chunk_coord)) {
            throw std::invalid_argument(
                "batch block (" + std::to_string(op.x) + "," + std::to_string(op.y) +
                ") is outside the target chunk");
        }
        if (op.set) {
            if (op.bits.size() != block_bits) {
                throw std::invalid_argument("bit string length does not match configured block_bits");
            }
            if (!BitCodec::IsBitString(op.bits)) {
                throw std::invalid_argument("bit string must contain only 0 and 1");
            }
        }
    }

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    if (has_expected_version && regular_chunk->version != expected_version) {
        return ChunkMutationResult{.ok = false, .version = regular_chunk->version};
    }

    auto new_payload = regular_chunk->payload;
    auto new_presence = regular_chunk->presence_bitmap;
    const std::string zero_bits(block_bits, '0');
    for (const auto& op : ops) {
        const auto [local_x, local_y] = geometry_.BlockToLocal(op.x, op.y);
        const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
        BitCodec::WriteBits(new_payload, block_index * block_bits, op.set ? op.bits : zero_bits);
        SetBlockPresent(&new_presence, block_index, op.set);
    }

    // A table of bit strings has no text or bytes columns.
    (void)ApplyFullChunkStateLocked(
        chunk_coord, regular_chunk, std::move(new_payload), std::move(new_presence), VarUpdate{});
    return ChunkMutationResult{.ok = true, .version = regular_chunk->version};
}

ChunkState ChunkStore::ReadChunkState(std::int64_t chunk_x, std::int64_t chunk_y) {
    const auto regular_chunk = GetOrLoadRegularChunk(ChunkCoord{chunk_x, chunk_y});
    std::shared_lock lock(regular_chunk->mutex);
    return ChunkState{
        .version = regular_chunk->version,
        .payload = regular_chunk->payload,
        .presence_bitmap = regular_chunk->presence_bitmap,
        .vars = regular_chunk->vars,
    };
}

ChunkMutationResult ChunkStore::WriteChunkState(
    std::int64_t chunk_x,
    std::int64_t chunk_y,
    ChunkState state,
    std::optional<std::uint64_t> expected_version) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
    ThrowIfDurabilityPoisoned();
    if (state.payload.size() != geometry_.ChunkPayloadBytes()) {
        throw std::invalid_argument("payload byte length does not match configured chunk size");
    }
    if (state.presence_bitmap.size() != ChunkPresenceBitmapBytes(geometry_)) {
        throw std::invalid_argument("presence byte length does not match configured chunk block count");
    }
    MaskUnusedPayloadBits(geometry_, &state.payload);
    MaskUnusedPresenceBits(geometry_, &state.presence_bitmap);
    CanonicalizeAbsentBlocks(geometry_, state.presence_bitmap, &state.payload);
    // Every value is checked before the chunk is touched.
    geometry_.layout().RequireValidVars(state.vars, state.presence_bitmap);
    RequirePendingFits(state.payload, state.presence_bitmap, &state.vars);

    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);
    if (expected_version.has_value() && regular_chunk->version != *expected_version) {
        return ChunkMutationResult{.ok = false, .version = regular_chunk->version};
    }
    VarUpdate var_update;
    if (state.vars.Encode() != regular_chunk->vars.Encode()) {
        var_update.replace = std::move(state.vars);
    }
    RequireVarWrite(regular_chunk->vars, var_update);
    if (expected_version.has_value()) {
        // As CHUNKPUT IF: the conditional write path (docs/DURABILITY_CONTRACT.md).
        (void)ApplyFullChunkStateLocked(
            chunk_coord,
            regular_chunk,
            std::move(state.payload),
            std::move(state.presence_bitmap),
            std::move(var_update));
        return ChunkMutationResult{.ok = true, .version = regular_chunk->version};
    }
    const std::uint64_t version = ApplyChunkStateLocked(
        chunk_coord,
        regular_chunk,
        std::move(state.payload),
        std::move(state.presence_bitmap),
        std::move(var_update));
    return ChunkMutationResult{.ok = true, .version = version};
}

}  // namespace chunkdb
