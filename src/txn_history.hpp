#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/types.hpp"

namespace chunkdb {

struct TxnCoordLess {
    bool operator()(const ChunkCoord& lhs, const ChunkCoord& rhs) const noexcept {
        return lhs.x != rhs.x ? lhs.x < rhs.x : lhs.y < rhs.y;
    }
};

// A chunk state kept for open transactions: the state a write replaced,
// tagged with the version of that write (docs/TRANSACTIONS_DESIGN.md).
struct TxnKeptState {
    std::uint64_t tag = 0;
    ChunkCoord coord{};
    ChunkState state{};
    // What the state counts toward the history's byte limit.
    std::size_t bytes = 0;
    // Every kept state of the history in ascending tag order, linked through
    // the states themselves so that linking allocates nothing.
    TxnKeptState* older = nullptr;
    TxnKeptState* newer = nullptr;
};

// The snapshots of a store's open transactions and the chunk states they
// still need.
//
// A snapshot registers under the history lock: it counts itself open, takes
// S, the last version the store's clock issued, and publishes itself. A write
// takes its version T from the same clock before it changes its chunk and
// then loads the open count; clock and count are sequentially consistent, so
// either the write sees the count (and finds the snapshot registered once it
// takes the history lock) or the snapshot's S is at or above T. A write that
// sees open transactions copies its chunk's state before changing it and,
// once committed, keeps the copy tagged T unless no registered snapshot is
// older than T, or a kept state of that chunk is already tagged above the
// newest registered snapshot. A read at S takes the oldest kept state of the
// chunk tagged above S, when there is one, instead of the current state.
//
// The history lock is the innermost lock: nothing takes another lock or
// loads a chunk while holding it.
class TxnHistory {
  public:
    using KeptByChunk = std::multimap<ChunkCoord, TxnKeptState, TxnCoordLess>;
    // A kept state built before a write's commit point, so publishing it
    // afterwards allocates nothing.
    using Reservation = KeptByChunk::node_type;

    explicit TxnHistory(std::size_t limit_bytes);

    TxnHistory(const TxnHistory&) = delete;
    TxnHistory& operator=(const TxnHistory&) = delete;
    ~TxnHistory();

    // Registered snapshots; loaded by writes after they took their version.
    [[nodiscard]] std::uint64_t OpenCount() const noexcept { return open_count_.load(std::memory_order_seq_cst); }

    // A state to keep for `coord`, tagged `tag`. Allocates.
    [[nodiscard]] static Reservation Reserve(const ChunkCoord& coord, std::uint64_t tag, ChunkState state);
    // Moves `state` into a reservation made with an empty one. Never
    // allocates.
    static void Fill(Reservation& reservation, ChunkState&& state) noexcept;
    // Keeps the reserved state when a registered snapshot needs it, under the
    // keep rule above; past the byte limit unregisters the oldest snapshots
    // until it fits. Never allocates and never fails; a state not kept stays
    // in `reservation`.
    void Publish(Reservation& reservation) noexcept;

    // Registers a snapshot at the last version `clock` issued (the clock
    // holds the next one).
    [[nodiscard]] std::unique_ptr<TxnSnapshot> Register(
        const std::shared_ptr<TxnHistory>& self,
        const std::atomic<std::uint64_t>& clock,
        std::chrono::steady_clock::time_point deadline);
    void End(TxnSnapshot& snapshot) noexcept;

    // Under the history lock: throws as RequireRegisteredLocked, then calls
    // `visit` with the oldest kept state of `coord` tagged above the snapshot,
    // when there is one. Returns whether it did.
    template <class Visit>
    bool VisitAt(const TxnSnapshot& snapshot, const ChunkCoord& coord, Visit&& visit) {
        std::lock_guard lock(mutex_);
        ExpireLocked(std::chrono::steady_clock::now());
        RequireRegisteredLocked(snapshot);
        const TxnKeptState* kept = OldestAboveLocked(coord, snapshot.version_);
        if (kept == nullptr) {
            return false;
        }
        visit(*kept);
        return true;
    }

    // Whether a kept state of `coord` is tagged above the snapshot; throws as
    // RequireRegisteredLocked.
    [[nodiscard]] bool ChangedSince(const TxnSnapshot& snapshot, const ChunkCoord& coord);
    // Commit validation, with every chunk in `coords` locked: throws
    // TransactionConflictError when the snapshot is not registered or a
    // kept state of one of them is tagged above it. The snapshot ends either
    // way.
    void ValidateAndEnd(TxnSnapshot& snapshot, const std::vector<ChunkCoord>& coords);

    [[nodiscard]] std::size_t KeptCount() const;
    [[nodiscard]] std::size_t KeptBytes() const;
    [[nodiscard]] std::size_t RegisteredCount() const;

    void ArmPauseForTests(TxnPausePoint point);
    [[nodiscard]] bool WaitForPauseForTests(TxnPausePoint point);
    void ResumeForTests(TxnPausePoint point);
    // Waits at `point` when it is armed.
    void PauseForTests(TxnPausePoint point);

  private:
    // Throws TransactionConflictError when a limit unregistered the snapshot
    // and std::invalid_argument when it ended or was never registered.
    void RequireRegisteredLocked(const TxnSnapshot& snapshot) const;
    [[nodiscard]] const TxnKeptState* OldestAboveLocked(const ChunkCoord& coord, std::uint64_t version) const;
    void UnregisterLocked(TxnSnapshot& snapshot, std::optional<TxnConflictReason> reason) noexcept;
    // Unregisters the snapshots past their deadline.
    void ExpireLocked(std::chrono::steady_clock::time_point now) noexcept;
    // Drops the kept states no registered snapshot needs any more: those
    // tagged at or below the oldest one, or all when none is registered.
    void PruneLocked() noexcept;
    void EraseLocked(TxnKeptState* kept) noexcept;

    const std::size_t limit_bytes_;
    std::atomic<std::uint64_t> open_count_{0};

    mutable std::mutex mutex_;
    std::multimap<std::uint64_t, TxnSnapshot*> by_version_;
    std::multimap<std::chrono::steady_clock::time_point, TxnSnapshot*> by_deadline_;
    KeptByChunk kept_;
    TxnKeptState* oldest_ = nullptr;
    TxnKeptState* newest_ = nullptr;
    std::size_t kept_bytes_ = 0;

    // Test pause points; a leaf lock of their own. Once a point was armed,
    // reaching any point takes the pause lock.
    struct PauseState {
        bool armed = false;
        bool reached = false;
        bool resumed = false;
    };
    std::atomic<int> pause_armed_{0};
    std::mutex pause_mutex_;
    std::condition_variable pause_cv_;
    std::array<PauseState, 3> pauses_{};
};

// What a write keeps for open transactions (ChunkStore::PrepareTxnKeepLocked).
struct TxnKeep {
    TxnHistory::Reservation node;
};

// The chunk locks this thread holds beyond the one a plain operation holds:
// a commit locks several chunks, and the WAL stream pool must not try_lock a
// chunk mutex the thread already owns.
void NoteThreadHoldsChunkLock(const void* chunk);
void NoteThreadReleasedChunkLock(const void* chunk) noexcept;
[[nodiscard]] bool ThreadHoldsChunkLock(const void* chunk) noexcept;
// Room for `count` more entries, so noting a lock cannot fail.
void ReserveThreadChunkLocks(std::size_t count);

// Transaction intents (docs/STORAGE_FORMAT.md): `.chunkdb.intents/txn-<T>.rollback`.
enum class TxnIntentState {
    kRollback,
    kCommitted,
};

struct TxnIntentEntry {
    ChunkCoord coord{};
    // The WAL's size before the transaction; zero means it had no WAL.
    std::uint64_t wal_boundary = 0;
};

struct TxnIntent {
    TxnIntentState state = TxnIntentState::kRollback;
    std::uint64_t version = 0;
    std::vector<TxnIntentEntry> entries{};
};

inline constexpr std::string_view kTxnIntentPrefix = "txn-";
inline constexpr std::string_view kTxnIntentSuffix = ".rollback";

[[nodiscard]] std::vector<std::uint8_t> SerializeTxnIntent(const TxnIntent& intent);
// False for anything but a well-formed record.
[[nodiscard]] bool TryParseTxnIntent(const std::vector<std::uint8_t>& bytes, TxnIntent* out);
[[nodiscard]] std::filesystem::path TxnIntentPath(const std::filesystem::path& data_dir, std::uint64_t version);
// A transaction intent's file name (not one of its temporary files).
[[nodiscard]] bool IsTxnIntentFileName(std::string_view name) noexcept;
// A transaction intent's name or one of its temporary files.
[[nodiscard]] bool IsTxnIntentArtifactName(std::string_view name) noexcept;
// For a read-only load: the WAL boundary that pending CKTB intents set for
// `coord` (the smallest when several list it), from the intents' bytes.
// Throws std::runtime_error for a malformed intent.
[[nodiscard]] std::optional<std::uint64_t> TxnRollbackBoundaryForChunk(
    const std::vector<std::vector<std::uint8_t>>& intents,
    const ChunkCoord& coord);

}  // namespace chunkdb
