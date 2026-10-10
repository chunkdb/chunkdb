#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "txn_history.hpp"
#include "wal_writer.hpp"
#include "change_feed.hpp"
#include "feed_prefix.hpp"
#include "feed_slots.hpp"

namespace chunkdb {

namespace {

// Spans of a commit frame closer than this many bytes are logged as one: a
// span record costs nine bytes before its data.
constexpr std::size_t kTxnSpanMergeGap = 16;

// A transaction may stay open at most this long whatever the caller asks,
// so its deadline never overflows the clock.
constexpr auto kTxnMaxDurationCap = std::chrono::hours(24 * 365);

void CrashAtTxnFailpoint(const char* key) {
    if (ConsumeFailpointEnv(key)) {
        std::_Exit(86);
    }
}

// True, once, when the failpoint `key` holds `value`.
[[nodiscard]] bool ConsumeFailpointAt(const char* key, std::size_t value) {
    const char* text = std::getenv(key);
    if (text == nullptr || text[0] == '\0') {
        return false;
    }
    std::uint64_t parsed = 0;
    if (!TryParseUint64(text, &parsed) || parsed != value) {
        return false;
    }
    return ConsumeFailpointEnv(key);
}

[[nodiscard]] std::string CurrentExceptionMessage() {
    try {
        throw;
    } catch (const std::exception& error) {
        return error.what();
    } catch (...) {
        return "unknown error";
    }
}

[[nodiscard]] std::string CoordText(const ChunkCoord& coord) {
    return "(" + std::to_string(coord.x) + "," + std::to_string(coord.y) + ")";
}

// SPAN records that turn `before` into `after`, at `base` in the chunk state.
void AppendDiffSpans(
    WalFrameBuilder* frame,
    std::size_t base,
    const std::vector<std::uint8_t>& before,
    const std::vector<std::uint8_t>& after) {
    bool open = false;
    std::size_t begin = 0;
    std::size_t end = 0;
    for (std::size_t i = 0; i < after.size(); ++i) {
        if (before[i] == after[i]) {
            continue;
        }
        if (open && i - end <= kTxnSpanMergeGap) {
            end = i + 1U;
            continue;
        }
        if (open) {
            frame->AppendSpan(static_cast<std::uint32_t>(base + begin), after.data() + begin, end - begin);
        }
        open = true;
        begin = i;
        end = i + 1U;
    }
    if (open) {
        frame->AppendSpan(static_cast<std::uint32_t>(base + begin), after.data() + begin, end - begin);
    }
}

// VAR_PUT and VAR_DEL records, ascending, that turn `before` into `after`.
void AppendVarDiff(WalFrameBuilder* frame, const ChunkVars& before, const ChunkVars& after) {
    auto old_it = before.begin();
    auto new_it = after.begin();
    while (old_it != before.end() || new_it != after.end()) {
        if (new_it == after.end() || (old_it != before.end() && (*old_it).key < (*new_it).key)) {
            frame->AppendVarDel((*old_it).key);
            ++old_it;
        } else if (old_it == before.end() || (*new_it).key < (*old_it).key) {
            frame->AppendVarPut((*new_it).key, (*new_it).value);
            ++new_it;
        } else {
            const auto old_value = (*old_it).value;
            const auto new_value = (*new_it).value;
            if (!std::equal(old_value.begin(), old_value.end(), new_value.begin(), new_value.end())) {
                frame->AppendVarPut((*new_it).key, new_value);
            }
            ++old_it;
            ++new_it;
        }
    }
}

}  // namespace

std::unique_ptr<TxnSnapshot> ChunkStore::BeginTxnSnapshot(std::chrono::milliseconds max_duration) {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("transactions need a table opened read-write");
    }
    if (allow_multiple_processes_) {
        throw std::invalid_argument(
            "transactions are refused on a table shared by several writer processes: its history would not see "
            "every write");
    }
    if (max_duration.count() <= 0) {
        throw std::invalid_argument("a transaction's duration limit must be positive");
    }
    const auto duration =
        std::min(max_duration, std::chrono::duration_cast<std::chrono::milliseconds>(kTxnMaxDurationCap));
    return txn_history_->Register(txn_history_, version_clock_, std::chrono::steady_clock::now() + duration);
}

void ChunkStore::EndTxnSnapshot(TxnSnapshot& snapshot) noexcept {
    if (snapshot.history_ != nullptr) {
        snapshot.history_->End(snapshot);
    }
}

bool ChunkStore::OwnsTxnSnapshot(const TxnSnapshot& snapshot) const noexcept {
    return snapshot.history_ == txn_history_;
}

void ChunkStore::RequireOwnTxnSnapshot(const TxnSnapshot& snapshot) const {
    if (snapshot.history_ != txn_history_) {
        throw std::invalid_argument("the transaction belongs to another table");
    }
}

void ChunkStore::ApplyTxnHistory(
    const TxnSnapshot& snapshot,
    const ChunkCoord& chunk_coord,
    bool with_vars,
    ChunkRangeEntry* entry) {
    (void)txn_history_->VisitAt(snapshot, chunk_coord, [&](const TxnKeptState& kept) {
        entry->payload = kept.state.payload;
        entry->presence_bitmap = kept.state.presence_bitmap;
        entry->version = kept.state.version;
        if (with_vars) {
            entry->vars = kept.state.vars;
        }
    });
}

ChunkState ChunkStore::ReadChunkStateAt(const TxnSnapshot& snapshot, std::int64_t chunk_x, std::int64_t chunk_y) {
    RequireOwnTxnSnapshot(snapshot);
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    ChunkRangeEntry entry;
    entry.coord = chunk_coord;
    {
        std::shared_lock lock(regular_chunk->mutex);
        entry.payload = regular_chunk->payload;
        entry.presence_bitmap = regular_chunk->presence_bitmap;
        entry.version = regular_chunk->version;
        entry.vars = regular_chunk->vars;
    }
    ApplyTxnHistory(snapshot, chunk_coord, true, &entry);
    return ChunkState{
        .version = entry.version,
        .payload = std::move(entry.payload),
        .presence_bitmap = std::move(entry.presence_bitmap),
        .vars = std::move(entry.vars),
    };
}

std::optional<std::vector<ColumnValue>> ChunkStore::GetBlockAt(
    const TxnSnapshot& snapshot,
    std::int64_t block_x,
    std::int64_t block_y) {
    RequireOwnTxnSnapshot(snapshot);
    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const auto decode = [&](const std::vector<std::uint8_t>& payload,
                            const std::vector<std::uint8_t>& presence,
                            const ChunkVars& vars) -> std::optional<std::vector<ColumnValue>> {
        if (!BlockPresent(presence, block_index)) {
            return std::nullopt;
        }
        return DecodeBlockColumns(geometry_.layout(), payload, vars, block_index);
    };

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::optional<std::vector<ColumnValue>> values;
    {
        std::shared_lock lock(regular_chunk->mutex);
        values = decode(regular_chunk->payload, regular_chunk->presence_bitmap, regular_chunk->vars);
    }
    (void)txn_history_->VisitAt(snapshot, chunk_coord, [&](const TxnKeptState& kept) {
        values = decode(kept.state.payload, kept.state.presence_bitmap, kept.state.vars);
    });
    return values;
}

bool ChunkStore::ChunkChangedSince(const TxnSnapshot& snapshot, std::int64_t chunk_x, std::int64_t chunk_y) {
    RequireOwnTxnSnapshot(snapshot);
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    // A write keeps its chunk's state before it releases the chunk's lock.
    std::shared_lock lock(regular_chunk->mutex);
    return txn_history_->ChangedSince(snapshot, chunk_coord);
}

std::size_t ChunkStore::TxnKeptStateCountForTests() const {
    return txn_history_->KeptCount();
}

std::size_t ChunkStore::TxnKeptBytesForTests() const {
    return txn_history_->KeptBytes();
}

std::size_t ChunkStore::TxnRegisteredCountForTests() const {
    return txn_history_->RegisteredCount();
}

void ChunkStore::ArmTxnPauseForTests(TxnPausePoint point) {
    txn_history_->ArmPauseForTests(point);
}

bool ChunkStore::WaitForTxnPauseForTests(TxnPausePoint point) {
    return txn_history_->WaitForPauseForTests(point);
}

void ChunkStore::ResumeTxnForTests(TxnPausePoint point) {
    txn_history_->ResumeForTests(point);
}

void ChunkStore::AppendTxnFrameLocked(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    const std::vector<std::uint8_t>& bytes,
    bool new_wal) {
    if (chunk->wal_path.empty()) {
        chunk->wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);
    }
    const auto& wal_path = chunk->wal_path;
    const auto parent = wal_path.parent_path();
    if (new_wal) {
        EnsureDirectoryPathExists(parent, /*durable_sync=*/true);
    }
    std::ofstream out(wal_path, std::ios::binary | std::ios::app);
    if (!out.is_open()) {
        throw BuildWalOpenError(wal_path, errno);
    }
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    out.flush();
    if (!out.good()) {
        throw std::runtime_error("failed to append a transaction frame to WAL " + wal_path.string());
    }
    out.close();
    if (out.fail()) {
        throw std::runtime_error("failed to close WAL " + wal_path.string() + " after a transaction frame");
    }
    SyncFilePath(wal_path);
    if (new_wal) {
        // The large-chunk directory's own entry was synced with the boundary,
        // or when it was made just above.
        SyncDirectoryPath(parent);
        chunk->wal_header_written = true;
    }
}

std::uint64_t ChunkStore::CommitTransaction(
    TxnSnapshot& snapshot,
    const std::vector<ChunkCoord>& read_set,
    std::vector<TxnChunkWrite> writes) {
    RequireOwnTxnSnapshot(snapshot);
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }

    // Everything the caller passed is checked before any chunk is touched.
    if (writes.size() > kMaxTxnWrittenChunks) {
        throw std::invalid_argument(
            "a transaction writes at most " + std::to_string(kMaxTxnWrittenChunks) + " chunks");
    }
    std::vector<ChunkCoord> reads = read_set;
    std::sort(reads.begin(), reads.end(), TxnCoordLess{});
    reads.erase(std::unique(reads.begin(), reads.end()), reads.end());
    if (reads.size() > kMaxTxnReadChunks) {
        throw std::invalid_argument(
            "a transaction reads at most " + std::to_string(kMaxTxnReadChunks) + " chunks");
    }
    std::sort(writes.begin(), writes.end(), [](const TxnChunkWrite& lhs, const TxnChunkWrite& rhs) {
        return TxnCoordLess{}(lhs.coord, rhs.coord);
    });
    for (std::size_t i = 0; i < writes.size(); ++i) {
        if (i > 0 && writes[i].coord == writes[i - 1].coord) {
            throw std::invalid_argument("chunk " + CoordText(writes[i].coord) + " is written twice");
        }
        auto& state = writes[i].state;
        if (state.payload.size() != geometry_.ChunkPayloadBytes()) {
            throw std::invalid_argument("payload byte length does not match configured chunk size");
        }
        if (state.presence_bitmap.size() != ChunkPresenceBitmapBytes(geometry_)) {
            throw std::invalid_argument("presence byte length does not match configured chunk block count");
        }
        MaskUnusedPayloadBits(geometry_, &state.payload);
        MaskUnusedPresenceBits(geometry_, &state.presence_bitmap);
        CanonicalizeAbsentBlocks(geometry_, state.presence_bitmap, &state.payload);
        geometry_.layout().RequireValidVars(state.vars, state.presence_bitmap);
        RequirePendingFits(state.payload, state.presence_bitmap, &state.vars);
    }

    // From here the transaction ends whatever happens, except for an invalid
    // value found under the locks.
    class EndSnapshotOnExit {
      public:
        EndSnapshotOnExit(ChunkStore* store, TxnSnapshot* snapshot) : store_(store), snapshot_(snapshot) {}
        EndSnapshotOnExit(const EndSnapshotOnExit&) = delete;
        EndSnapshotOnExit& operator=(const EndSnapshotOnExit&) = delete;
        ~EndSnapshotOnExit() {
            if (armed_) {
                store_->EndTxnSnapshot(*snapshot_);
            }
        }
        void Disarm() noexcept { armed_ = false; }

      private:
        ChunkStore* store_;
        TxnSnapshot* snapshot_;
        bool armed_ = true;
    } end_snapshot(this, &snapshot);
    if (writes.empty()) {
        // A transaction that wrote nothing read one consistent snapshot; it
        // only has to still be registered.
        txn_history_->ValidateAndEnd(snapshot, {});
        return 0;
    }
    ThrowIfDurabilityPoisoned();

    // The chunks in coordinate order, each once: written ones locked
    // exclusively, read ones shared. Plain operations lock one chunk at a
    // time, so this order cannot deadlock with them or with other commits.
    struct Target {
        ChunkCoord coord;
        // Index into `writes`, or kNoWrite.
        std::size_t write;
        std::shared_ptr<RegularChunk> chunk;
    };
    constexpr std::size_t kNoWrite = static_cast<std::size_t>(-1);
    std::vector<Target> targets;
    targets.reserve(writes.size() + reads.size());
    for (std::size_t i = 0; i < writes.size(); ++i) {
        targets.push_back(Target{.coord = writes[i].coord, .write = i, .chunk = nullptr});
    }
    for (const auto& coord : reads) {
        if (!std::binary_search(
                writes.begin(), writes.end(), TxnChunkWrite{.coord = coord, .state = {}},
                [](const TxnChunkWrite& lhs, const TxnChunkWrite& rhs) {
                    return TxnCoordLess{}(lhs.coord, rhs.coord);
                })) {
            targets.push_back(Target{.coord = coord, .write = kNoWrite, .chunk = nullptr});
        }
    }
    std::sort(targets.begin(), targets.end(), [](const Target& lhs, const Target& rhs) {
        return TxnCoordLess{}(lhs.coord, rhs.coord);
    });
    // Loaded and pinned before any is locked: a load may evict, and a
    // pinned chunk is never evicted.
    for (auto& target : targets) {
        target.chunk = GetOrLoadRegularChunk(target.coord);
    }

    struct HeldLocks {
        std::vector<std::pair<RegularChunk*, bool>> held;
        HeldLocks() = default;
        HeldLocks(const HeldLocks&) = delete;
        HeldLocks& operator=(const HeldLocks&) = delete;
        void Release() noexcept {
            for (auto it = held.rbegin(); it != held.rend(); ++it) {
                if (it->second) {
                    it->first->mutex.unlock();
                } else {
                    it->first->mutex.unlock_shared();
                }
                NoteThreadReleasedChunkLock(it->first);
            }
            held.clear();
        }
        ~HeldLocks() { Release(); }
    } locks;
    locks.held.reserve(targets.size());
    ReserveThreadChunkLocks(targets.size());
    for (const auto& target : targets) {
        const bool exclusive = target.write != kNoWrite;
        if (exclusive) {
            target.chunk->mutex.lock();
        } else {
            target.chunk->mutex.lock_shared();
        }
        locks.held.emplace_back(target.chunk.get(), exclusive);
        NoteThreadHoldsChunkLock(target.chunk.get());
    }

    ThrowIfDurabilityPoisoned();
    for (const auto& target : targets) {
        if (target.chunk->wal_repair_failed) {
            throw std::runtime_error(
                "chunk " + CoordText(target.coord) +
                " is fail-closed: its WAL could not be repaired; restart the store");
        }
    }
    for (const auto& target : targets) {
        if (target.write == kNoWrite) {
            continue;
        }
        const auto& vars = writes[target.write].state.vars;
        const std::size_t after = vars.encoded_size();
        // A chunk already over a lowered limit may still shrink.
        if (after > var_max_chunk_bytes_ && after > target.chunk->vars.encoded_size()) {
            end_snapshot.Disarm();
            throw std::invalid_argument(
                "the text and bytes values of chunk " + CoordText(target.coord) + " would take " +
                std::to_string(after) + " bytes, more than var_max_chunk_bytes (" +
                std::to_string(var_max_chunk_bytes_) + ")");
        }
    }

    // With every touched chunk locked no write can come between this check
    // and the commit, so two transactions that read what the other writes
    // cannot both commit.
    {
        std::vector<ChunkCoord> coords;
        coords.reserve(targets.size());
        for (const auto& target : targets) {
            coords.push_back(target.coord);
        }
        txn_history_->ValidateAndEnd(snapshot, coords);
    }

    struct Change {
        const Target* target;
        TxnChunkWrite* write;
        std::uint64_t boundary = 0;
        bool new_wal = false;
        // The WAL header when new, then the frame.
        std::vector<std::uint8_t> bytes{};
        std::optional<FeedWalPrefixIndex::Prepared> prefix{};
        bool append_started = false;
        TxnKeep keep{};
    };
    std::vector<Change> changes;
    for (const auto& target : targets) {
        if (target.write == kNoWrite) {
            continue;
        }
        auto& write = writes[target.write];
        const auto& chunk = *target.chunk;
        if (write.state.payload == chunk.payload && write.state.presence_bitmap == chunk.presence_bitmap &&
            write.state.vars == chunk.vars) {
            continue;
        }
        changes.push_back(Change{.target = &target, .write = &write});
    }
    if (changes.empty()) {
        return 0;
    }

    // Each WAL's size becomes a durable boundary the transaction can go back
    // to.
    for (std::size_t i = 0; i < changes.size(); ++i) {
        auto& change = changes[i];
        const auto& chunk = change.target->chunk;
        try {
            // Without a stream from the shared pool, which may be full of
            // streams of chunks this commit holds.
            MakeWalBoundaryDurableLocked(change.target->coord, chunk, /*own_stream=*/true);
        } catch (const WriteOutcomeUnknownError& error) {
            // About the batch's own writes; the transaction has not touched
            // the WAL yet.
            throw std::runtime_error(std::string("the transaction was not applied: ") + error.what());
        }
        // The commit appends through a stream of its own, and the pool gets
        // the slot back.
        CloseWalAppendStream(chunk);
        change.boundary = CurrentWalFileSize(chunk);
        change.new_wal = change.boundary == 0U || !chunk->wal_header_written;
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_TXN_BOUNDARY_FAIL_ONCE")) {
            throw std::runtime_error("injected failure while setting a transaction's WAL boundaries");
        }
    }

    // One version and one commit time for every chunk. Taken after the
    // chunks are locked and flushed, so it is above every frame in their WALs.
    FeedWriteGuard feed(*this);
    const std::uint64_t version = NextChunkVersion();
    feed.Version(version);
    std::uint64_t commit_time_ms = 0;
    for (const auto& change : changes) {
        commit_time_ms = NextCommitTimeMs(*change.target->chunk);
    }
    // Loaded after the version was taken, as a plain write does.
    const bool keep_states = txn_history_->OpenCount() > 0U;

    // Everything that allocates happens before the intent.
    TxnIntent intent{.state = TxnIntentState::kRollback, .version = version, .entries = {}};
    intent.entries.reserve(changes.size());
    for (auto& change : changes) {
        const auto& chunk = *change.target->chunk;
        const auto& state = change.write->state;
        if (change.new_wal) {
            change.bytes = BuildWalHeader(change.target->coord, store_id_, features_);
        }
        feed.Before(change.target->coord, chunk.payload, chunk.presence_bitmap, chunk.vars);
        WalFrameBuilder frame(&change.bytes, geometry_.layout().schema().version, {}, SlotWriteUser());
        AppendDiffSpans(&frame, 0U, chunk.payload, state.payload);
        AppendDiffSpans(&frame, geometry_.ChunkPayloadBytes(), chunk.presence_bitmap, state.presence_bitmap);
        AppendVarDiff(&frame, chunk.vars, state.vars);
        const auto frame_bytes = frame.Finish(version, commit_time_ms);
        if (feed_slots_ && feed_slots_->ArchiveRequired())
            change.prefix.emplace(feed_slots_->prefix_index().Prepare(change.target->coord, change.boundary, change.bytes));
        feed.Capture(change.bytes, frame_bytes);
        if (keep_states) {
            change.keep.node = TxnHistory::Reserve(change.target->coord, version, ChunkState{});
        }
        intent.entries.push_back(TxnIntentEntry{.coord = change.target->coord, .wal_boundary = change.boundary});
    }
    const auto intent_path = TxnIntentPath(data_dir_, version);
    const auto intent_dir = intent_path.parent_path();
    const auto rollback_record = SerializeTxnIntent(intent);
    intent.state = TxnIntentState::kCommitted;
    const auto commit_record = SerializeTxnIntent(intent);

    // From the intent to its commit form, read-only processes do not take
    // these chunks.
    SnapshotGenerationWriteGuard snapshot_write(this);
    bool intent_visible = false;
    try {
        EnsureDirectoryPathExists(intent_dir, /*durable_sync=*/true);
        AtomicWrite(intent_path, rollback_record, /*fsync_file=*/true, /*fsync_directory=*/true, &intent_visible);
    } catch (...) {
        bool cleaned = true;
        if (intent_visible) {
            // Published but not synced: no WAL changed yet, so the intent
            // just has to go, durably, before any of them takes a write.
            try {
                std::error_code remove_ec;
                if (!std::filesystem::remove(intent_path, remove_ec) || remove_ec) {
                    throw std::runtime_error("failed to remove " + intent_path.string());
                }
                SyncDirectoryPath(intent_dir);
            } catch (const std::exception& cleanup_error) {
                cleaned = false;
                for (const auto& change : changes) {
                    change.target->chunk->wal_repair_failed = true;
                }
                PoisonDurability(
                    "a transaction intent was published but neither synced nor removed: " +
                    std::string(cleanup_error.what()));
            }
        }
        if (cleaned) {
            snapshot_write.Finish();
        }
        throw;
    }
    CrashAtTxnFailpoint("CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_INTENT_PUBLISH_ONCE");

    // Made before the commit point, so reporting an unknown outcome after it
    // cannot fail (copying an exception does not allocate).
    const WriteOutcomeUnknownError unknown_outcome(
        "the transaction may or may not be applied: its commit record " + intent_path.string() +
        " could not be made durable; the table is fail-closed until restart");

    // Before the commit point: a failure goes back to the boundaries.
    bool commit_durable = true;
    std::string commit_sync_error;
    try {
        for (std::size_t i = 0; i <= changes.size(); ++i) {
            if (ConsumeFailpointAt("CHUNKDB_FAILPOINT_TXN_APPEND_FAIL_AT", i)) {
                throw std::runtime_error(
                    "injected failure after " + std::to_string(i) + " of " + std::to_string(changes.size()) +
                    " transaction frames");
            }
            if (ConsumeFailpointAt("CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", i)) {
                std::_Exit(86);
            }
            if (i == changes.size()) {
                break;
            }
            auto& change = changes[i];
            change.append_started = true;
            AppendTxnFrameLocked(change.target->coord, change.target->chunk, change.bytes, change.new_wal);
            if (change.prefix) feed_slots_->prefix_index().Commit(std::move(*change.prefix));
        }
        CrashAtTxnFailpoint("CHUNKDB_FAILPOINT_CRASH_TXN_BEFORE_COMMIT_PUBLISH_ONCE");
        // Replacing the intent with its commit form is the commit point.
        bool replaced = false;
        try {
            AtomicWrite(
                intent_path,
                commit_record,
                /*fsync_file=*/true,
                /*fsync_directory=*/true,
                &replaced,
                "CHUNKDB_FAILPOINT_TXN_COMMIT_INTENT_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
        } catch (...) {
            if (!replaced) {
                throw;
            }
            // The commit form is visible: never go back from here. Complete
            // the directory sync it is missing.
            try {
                if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_TXN_COMMIT_INTENT_COMPLETION_SYNC_FAIL_ONCE")) {
                    throw std::runtime_error("injected transaction commit-intent completion sync failure");
                }
                SyncDirectoryPath(intent_dir);
            } catch (...) {
                // Nothing here may throw into the rollback below: the commit
                // form is visible.
                commit_durable = false;
                try {
                    commit_sync_error = CurrentExceptionMessage();
                } catch (...) {
                    commit_sync_error.clear();
                }
            }
        }
    } catch (...) {
        const std::string error = CurrentExceptionMessage();
        // Nothing is committed: the WALs go back to their boundaries, then
        // the intent goes. While the intent stays, the next start truncates
        // every WAL it lists, so none of them may take another write.
        std::string repair_error;
        for (auto& change : changes) {
            if (!change.append_started) {
                continue;
            }
            try {
                TruncateWalTail(change.target->coord, change.target->chunk, change.boundary, /*force_sync=*/true);
            } catch (const std::exception& truncate_error) {
                repair_error = truncate_error.what();
                break;
            }
        }
        if (repair_error.empty()) {
            try {
                if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_TXN_INTENT_REMOVE_FAIL_ONCE")) {
                    throw std::runtime_error("injected transaction intent removal failure");
                }
                std::error_code remove_ec;
                if (!std::filesystem::remove(intent_path, remove_ec) || remove_ec) {
                    throw std::runtime_error(
                        "failed to remove transaction intent " + intent_path.string() +
                        (remove_ec ? " (" + remove_ec.message() + ")" : std::string()));
                }
                SyncDirectoryPath(intent_dir);
            } catch (const std::exception& remove_error) {
                repair_error = remove_error.what();
            }
        }
        if (!repair_error.empty()) {
            for (const auto& change : changes) {
                change.target->chunk->wal_repair_failed = true;
            }
            const std::string reason =
                "repairing the WALs of a failed transaction failed: " + repair_error + " (after: " + error + ")";
            LogMessage(
                LogLevel::kError,
                LogComponent::kRecovery,
                "failed to repair the WALs of a failed transaction; poisoning store",
                {{"version", std::to_string(version)}, {"error", error}, {"repair_error", repair_error}});
            PoisonDurability(reason);
            // The generation stays odd until a restart repairs the WALs.
            throw std::runtime_error(
                "the transaction was not applied: " + error + "; its WALs could not be repaired (" + repair_error +
                "), the table is fail-closed until restart, whose recovery removes the transaction");
        }
        snapshot_write.Finish();
        throw;
    }
    CrashAtTxnFailpoint("CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_COMMIT_PUBLISH_ONCE");

    // Committed. From here no failure may escape: an error would read as
    // "not applied", and a retry would apply the transaction twice. What
    // follows is either non-throwing or contained, as after an ordinary
    // write's commit point (FinishOrdinaryMutationLocked).

    // Memory takes the new states; the replaced ones go to the history (into
    // nodes made before the intent) for older snapshots. Moves only.
    for (auto& change : changes) {
        auto& chunk = *change.target->chunk;
        ChunkState previous{
            .version = chunk.version,
            .payload = std::move(chunk.payload),
            .presence_bitmap = std::move(chunk.presence_bitmap),
            .vars = std::move(chunk.vars),
        };
        chunk.payload = std::move(change.write->state.payload);
        chunk.presence_bitmap = std::move(change.write->state.presence_bitmap);
        chunk.vars = std::move(change.write->state.vars);
        chunk.version = version;
        chunk.commit_time_ms = commit_time_ms;
        chunk.pending_updates += 1;
        chunk.wal_bytes += change.bytes.size();
        if (!change.keep.node.empty()) {
            TxnHistory::Fill(change.keep.node, std::move(previous));
            txn_history_->Publish(change.keep.node);
        }
    }
    feed.Commit();
    locks.Release();
    txn_history_->PauseForTests(TxnPausePoint::kBeforePostCommitOutcome);

    // Called inside a handler: names the exception being handled.
    const auto log_contained = [](const char* message, std::uint64_t commit_version) noexcept {
        try {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kRecovery,
                message,
                {{"version", std::to_string(commit_version)}, {"error", CurrentExceptionMessage()}});
        } catch (...) {
            // Logging is best-effort; the commit stands.
        }
    };

    if (!commit_durable) {
        // The commit form is visible but its directory entry may not be
        // durable: a crash may keep or drop the transaction. Fail closed
        // first, with a reason that cannot fail to build, then name it.
        PoisonDurability(std::string());
        try {
            PoisonDurability(
                "transaction commit record " + intent_path.string() +
                " could not be made durable: " + commit_sync_error);
        } catch (...) {
            // The store is fail-closed already; only the reason is missing.
        }
        try {
            throw WriteOutcomeUnknownError(
                "the transaction may or may not be applied: its commit record could not be made durable (" +
                commit_sync_error + "); the table is fail-closed until restart");
        } catch (const WriteOutcomeUnknownError&) {
            throw;
        } catch (...) {
            throw unknown_outcome;
        }
    }

    CrashAtTxnFailpoint("CHUNKDB_FAILPOINT_CRASH_TXN_BEFORE_INTENT_UNLINK_ONCE");
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_TXN_INTENT_UNLINK_FAIL_ONCE")) {
            throw std::runtime_error("injected transaction intent unlink failure");
        }
        std::error_code remove_ec;
        if (!std::filesystem::remove(intent_path, remove_ec) || remove_ec) {
            throw std::runtime_error(
                "failed to remove " + intent_path.string() +
                (remove_ec ? " (" + remove_ec.message() + ")" : std::string()));
        }
        SyncDirectoryPath(intent_dir);
    } catch (...) {
        // A retained commit form keeps the frames at the next start, as an
        // absent intent does.
        log_contained("transaction committed but its intent could not be removed", version);
    }
    try {
        snapshot_write.Finish();
    } catch (...) {
        // Committed; the epoch stays failed, so later transitions fail
        // closed until restart.
        log_contained("transaction committed but snapshot generation could not be republished", version);
    }

    // The usual checkpoint checks, one chunk at a time; a failure is retried
    // by a later write, as after an ordinary write.
    for (const auto& change : changes) {
        try {
            const auto& chunk = change.target->chunk;
            std::unique_lock lock(chunk->mutex);
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_TXN_AFTER_COMMIT_THROW_ONCE")) {
                // Not a std::exception: whatever is thrown is contained.
                throw 86;
            }
            MaybeCheckpointChunk(change.target->coord, chunk);
        } catch (...) {
            log_contained("transaction committed in WAL but inline checkpoint failed; retaining WAL for retry", version);
        }
    }
    return version;
}

}  // namespace chunkdb
