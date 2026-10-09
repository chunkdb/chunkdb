#include "txn_history.hpp"

#include <algorithm>
#include <iterator>
#include <stdexcept>
#include <string>
#include <utility>

#include "chunk_store_internal.hpp"

namespace chunkdb {

namespace {

// A kept state's own bookkeeping, counted toward the byte limit with its
// state's bytes.
constexpr std::size_t kKeptStateOverheadBytes = sizeof(TxnKeptState) + 64U;

thread_local std::vector<const void*> g_thread_chunk_locks;

[[nodiscard]] std::string ConflictMessage(TxnConflictReason reason) {
    switch (reason) {
        case TxnConflictReason::kChunkChanged:
            return "a chunk the transaction read or wrote changed after its snapshot";
        case TxnConflictReason::kDuration:
            return "the transaction was open longer than its duration limit";
        case TxnConflictReason::kHistoryLimit:
            return "the table's kept states for open transactions reached their byte limit";
        case TxnConflictReason::kTableChanged:
            return "the table was altered or dropped during the transaction";
    }
    return "transaction conflict";
}

}  // namespace

const char* TxnConflictReasonName(TxnConflictReason reason) noexcept {
    switch (reason) {
        case TxnConflictReason::kChunkChanged:
            return "chunk_changed";
        case TxnConflictReason::kDuration:
            return "duration";
        case TxnConflictReason::kHistoryLimit:
            return "history_limit";
        case TxnConflictReason::kTableChanged:
            return "table_changed";
    }
    return "unknown";
}

TxnSnapshot::~TxnSnapshot() {
    if (history_ != nullptr) {
        history_->End(*this);
    }
}

TxnHistory::TxnHistory(std::size_t limit_bytes) : limit_bytes_(limit_bytes) {}

TxnHistory::~TxnHistory() {
    // Snapshots hold the history, so none is registered any more.
    std::lock_guard lock(mutex_);
    PruneLocked();
}

TxnHistory::Reservation TxnHistory::Reserve(const ChunkCoord& coord, std::uint64_t tag, ChunkState state) {
    const std::size_t bytes =
        state.payload.size() + state.presence_bitmap.size() + state.vars.encoded_size() + kKeptStateOverheadBytes;
    KeptByChunk staging;
    const auto it = staging.emplace(
        coord,
        TxnKeptState{
            .tag = tag,
            .coord = coord,
            .state = std::move(state),
            .bytes = bytes,
            .older = nullptr,
            .newer = nullptr,
        });
    return staging.extract(it);
}

void TxnHistory::Fill(Reservation& reservation, ChunkState&& state) noexcept {
    auto& kept = reservation.mapped();
    kept.state = std::move(state);
    kept.bytes = kept.state.payload.size() + kept.state.presence_bitmap.size() + kept.state.vars.encoded_size() +
                 kKeptStateOverheadBytes;
}

void TxnHistory::Publish(Reservation& reservation) noexcept {
    if (reservation.empty()) {
        return;
    }
    std::lock_guard lock(mutex_);
    ExpireLocked(std::chrono::steady_clock::now());
    TxnKeptState& state = reservation.mapped();
    const auto needed = [&]() {
        // Some registered snapshot is older than the write, and no kept
        // state of the chunk is already tagged above the newest snapshot
        // (that one is what every older snapshot reads instead).
        if (by_version_.empty() || by_version_.begin()->first >= state.tag) {
            return false;
        }
        const auto range = kept_.equal_range(state.coord);
        if (range.first != range.second &&
            std::prev(range.second)->second.tag > std::prev(by_version_.end())->first) {
            return false;
        }
        return true;
    };
    if (!needed()) {
        return;
    }
    // Plain writes never fail for the limit: the oldest snapshots go first,
    // and only while one of them still needs the state.
    while (needed() && kept_bytes_ + state.bytes > limit_bytes_) {
        UnregisterLocked(*by_version_.begin()->second, TxnConflictReason::kHistoryLimit);
        PruneLocked();
    }
    if (!needed()) {
        return;
    }
    const auto inserted = kept_.insert(std::move(reservation));
    TxnKeptState* kept = &inserted->second;
    // Writes reach the history nearly in tag order: walk back from the
    // newest.
    TxnKeptState* before = newest_;
    while (before != nullptr && before->tag > kept->tag) {
        before = before->older;
    }
    kept->older = before;
    kept->newer = before != nullptr ? before->newer : oldest_;
    if (kept->newer != nullptr) {
        kept->newer->older = kept;
    } else {
        newest_ = kept;
    }
    if (before != nullptr) {
        before->newer = kept;
    } else {
        oldest_ = kept;
    }
    kept_bytes_ += kept->bytes;
}

std::unique_ptr<TxnSnapshot> TxnHistory::Register(
    const std::shared_ptr<TxnHistory>& self,
    const std::atomic<std::uint64_t>& clock,
    std::chrono::steady_clock::time_point deadline) {
    std::unique_ptr<TxnSnapshot> snapshot(new TxnSnapshot());
    std::lock_guard lock(mutex_);
    ExpireLocked(std::chrono::steady_clock::now());
    // Counted open before S is read: a write whose version is above S then
    // sees the count and keeps what this snapshot needs.
    open_count_.fetch_add(1, std::memory_order_seq_cst);
    try {
        const std::uint64_t next = clock.load(std::memory_order_seq_cst);
        snapshot->version_ = next == 0U ? 0U : next - 1U;
        PauseForTests(TxnPausePoint::kRegisterBeforePublish);
        snapshot->by_version_ = by_version_.emplace(snapshot->version_, snapshot.get());
        try {
            snapshot->by_deadline_ = by_deadline_.emplace(deadline, snapshot.get());
        } catch (...) {
            by_version_.erase(snapshot->by_version_);
            throw;
        }
    } catch (...) {
        open_count_.fetch_sub(1, std::memory_order_seq_cst);
        throw;
    }
    snapshot->registered_ = true;
    snapshot->history_ = self;
    return snapshot;
}

void TxnHistory::End(TxnSnapshot& snapshot) noexcept {
    std::lock_guard lock(mutex_);
    if (snapshot.registered_) {
        UnregisterLocked(snapshot, std::nullopt);
        PruneLocked();
    }
}

bool TxnHistory::ChangedSince(const TxnSnapshot& snapshot, const ChunkCoord& coord) {
    std::lock_guard lock(mutex_);
    ExpireLocked(std::chrono::steady_clock::now());
    RequireRegisteredLocked(snapshot);
    return OldestAboveLocked(coord, snapshot.version_) != nullptr;
}

void TxnHistory::ValidateAndEnd(TxnSnapshot& snapshot, const std::vector<ChunkCoord>& coords) {
    std::lock_guard lock(mutex_);
    ExpireLocked(std::chrono::steady_clock::now());
    RequireRegisteredLocked(snapshot);
    const bool changed = std::any_of(coords.begin(), coords.end(), [&](const ChunkCoord& coord) {
        return OldestAboveLocked(coord, snapshot.version_) != nullptr;
    });
    UnregisterLocked(snapshot, std::nullopt);
    PruneLocked();
    if (changed) {
        throw TransactionConflictError(
            TxnConflictReason::kChunkChanged, ConflictMessage(TxnConflictReason::kChunkChanged));
    }
}

std::size_t TxnHistory::KeptCount() const {
    std::lock_guard lock(mutex_);
    return kept_.size();
}

std::size_t TxnHistory::KeptBytes() const {
    std::lock_guard lock(mutex_);
    return kept_bytes_;
}

std::size_t TxnHistory::RegisteredCount() const {
    std::lock_guard lock(mutex_);
    return by_version_.size();
}

void TxnHistory::RequireRegisteredLocked(const TxnSnapshot& snapshot) const {
    if (snapshot.registered_) {
        return;
    }
    if (snapshot.end_reason_.has_value()) {
        throw TransactionConflictError(*snapshot.end_reason_, ConflictMessage(*snapshot.end_reason_));
    }
    throw std::invalid_argument("the transaction has ended");
}

const TxnKeptState* TxnHistory::OldestAboveLocked(const ChunkCoord& coord, std::uint64_t version) const {
    // A chunk's kept states are in ascending tag order: its writes take
    // their versions under its lock.
    const auto range = kept_.equal_range(coord);
    for (auto it = range.first; it != range.second; ++it) {
        if (it->second.tag > version) {
            return &it->second;
        }
    }
    return nullptr;
}

void TxnHistory::UnregisterLocked(TxnSnapshot& snapshot, std::optional<TxnConflictReason> reason) noexcept {
    by_version_.erase(snapshot.by_version_);
    by_deadline_.erase(snapshot.by_deadline_);
    snapshot.registered_ = false;
    snapshot.end_reason_ = reason;
    open_count_.fetch_sub(1, std::memory_order_seq_cst);
}

void TxnHistory::ExpireLocked(std::chrono::steady_clock::time_point now) noexcept {
    bool expired = false;
    while (!by_deadline_.empty() && by_deadline_.begin()->first <= now) {
        UnregisterLocked(*by_deadline_.begin()->second, TxnConflictReason::kDuration);
        expired = true;
    }
    if (expired) {
        PruneLocked();
    }
}

void TxnHistory::PruneLocked() noexcept {
    while (oldest_ != nullptr && (by_version_.empty() || oldest_->tag <= by_version_.begin()->first)) {
        EraseLocked(oldest_);
    }
}

void TxnHistory::EraseLocked(TxnKeptState* kept) noexcept {
    if (kept->older != nullptr) {
        kept->older->newer = kept->newer;
    } else {
        oldest_ = kept->newer;
    }
    if (kept->newer != nullptr) {
        kept->newer->older = kept->older;
    } else {
        newest_ = kept->older;
    }
    kept_bytes_ -= kept->bytes;
    const auto range = kept_.equal_range(kept->coord);
    for (auto it = range.first; it != range.second; ++it) {
        if (&it->second == kept) {
            kept_.erase(it);
            return;
        }
    }
}

void TxnHistory::ArmPauseForTests(TxnPausePoint point) {
    std::lock_guard lock(pause_mutex_);
    auto& pause = pauses_[static_cast<std::size_t>(point)];
    pause = PauseState{.armed = point != TxnPausePoint::kNone, .reached = false, .resumed = false};
    pause_armed_.fetch_add(1, std::memory_order_release);
}

bool TxnHistory::WaitForPauseForTests(TxnPausePoint point) {
    std::unique_lock lock(pause_mutex_);
    return pause_cv_.wait_for(lock, std::chrono::seconds(30), [&] {
        return pauses_[static_cast<std::size_t>(point)].reached;
    });
}

void TxnHistory::ResumeForTests(TxnPausePoint point) {
    std::lock_guard lock(pause_mutex_);
    pauses_[static_cast<std::size_t>(point)].resumed = true;
    pause_cv_.notify_all();
}

void TxnHistory::PauseForTests(TxnPausePoint point) {
    if (pause_armed_.load(std::memory_order_acquire) == 0) {
        return;
    }
    std::unique_lock lock(pause_mutex_);
    auto& pause = pauses_[static_cast<std::size_t>(point)];
    if (!pause.armed || pause.reached) {
        return;
    }
    pause.reached = true;
    pause_cv_.notify_all();
    pause_cv_.wait(lock, [&] { return pause.resumed; });
    pause.armed = false;
}

void NoteThreadHoldsChunkLock(const void* chunk) {
    g_thread_chunk_locks.push_back(chunk);
}

void NoteThreadReleasedChunkLock(const void* chunk) noexcept {
    const auto it = std::find(g_thread_chunk_locks.rbegin(), g_thread_chunk_locks.rend(), chunk);
    if (it != g_thread_chunk_locks.rend()) {
        g_thread_chunk_locks.erase(std::next(it).base());
    }
}

bool ThreadHoldsChunkLock(const void* chunk) noexcept {
    return std::find(g_thread_chunk_locks.begin(), g_thread_chunk_locks.end(), chunk) != g_thread_chunk_locks.end();
}

void ReserveThreadChunkLocks(std::size_t count) {
    g_thread_chunk_locks.reserve(g_thread_chunk_locks.size() + count);
}

}  // namespace chunkdb
