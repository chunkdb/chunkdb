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

#include "snapshot_generation.hpp"
#include "txn_history.hpp"

namespace chunkdb {

namespace {

// Immediate (sleep-free) attempts. A writer bracket that is merely being
// raced normally resolves inside these.
constexpr std::size_t kReadOnlySnapshotSpinAttempts = 8;
// After the spins, keep retrying with exponential backoff until this much
// wall time has been spent sleeping. Only active artifact publications hold
// an odd epoch; completed transitions publish even on their own thread.
constexpr std::uint64_t kReadOnlySnapshotBackoffBudgetMs = 250;
constexpr std::uint64_t kReadOnlySnapshotBackoffStartUs = 250;
constexpr std::uint64_t kReadOnlySnapshotBackoffMaxUs = 20'000;
constexpr std::string_view kSnapshotGenerationFile = "chunkdb.snapshot";
constexpr std::array<std::uint8_t, 4> kSnapshotGenerationMagic = {
    'C', 'K', 'S', 'G'};
constexpr std::size_t kSnapshotGenerationRecordSize = 16;
thread_local std::vector<ChunkStore*> g_snapshot_write_stack;

// A snapshot artifact that exists but cannot be read *right now*. On Windows
// an atomic replace makes the target briefly unopenable (sharing violation)
// even though nothing is damaged, so this is namespace instability like a
// vanished path, not proof of corruption. The bracket protocol retries it
// within its bounded budget and still fails closed once the budget is spent.
struct SnapshotArtifactUnstable : std::runtime_error {
    using std::runtime_error::runtime_error;
};

void CrashAtSnapshotFailpoint(const char* key) {
    if (ConsumeFailpointEnv(key)) {
        std::_Exit(86);
    }
}

[[nodiscard]] ReadOnlyArtifactSnapshot ReadArtifactForSnapshot(
    const std::filesystem::path& path) {
    std::error_code status_ec;
    const auto status = std::filesystem::symlink_status(path, status_ec);
    if (status_ec == std::errc::no_such_file_or_directory ||
        (!status_ec && !std::filesystem::exists(status))) {
        return {};
    }
    if (status_ec) {
        throw std::runtime_error(
            "read-only snapshot cannot inspect artifact " + path.string() +
            ": " + status_ec.message());
    }
    if (!std::filesystem::is_regular_file(status)) {
        throw std::runtime_error(
            "read-only snapshot expected a regular file at " + path.string());
    }

    try {
        return {
            .present = true,
            .bytes = LoadFile(path),
        };
    } catch (const std::exception& load_error) {
        // An atomic replacement/removal between status and open is namespace
        // instability, not proof of damage. If the path is now absent, record
        // that observation and let the generation bracket decide whether it
        // is stable. A still-present unreadable artifact fails closed.
        std::error_code restat_ec;
        const auto restat = std::filesystem::symlink_status(path, restat_ec);
        if (restat_ec == std::errc::no_such_file_or_directory ||
            (!restat_ec && !std::filesystem::exists(restat))) {
            return {};
        }
        if (restat_ec) {
            throw std::runtime_error(
                "read-only snapshot cannot re-inspect artifact " +
                path.string() + " after a read failure: " +
                restat_ec.message());
        }
        throw SnapshotArtifactUnstable(
            "read-only snapshot cannot read artifact " + path.string() +
            ": " + load_error.what());
    }
}

// The table's pending transaction intents, in name order. They are rare
// and short-lived, so listing them costs little.
[[nodiscard]] std::vector<ReadOnlyArtifactSnapshot> ReadTxnIntentsForSnapshot(
    const std::filesystem::path& intent_dir) {
    std::error_code list_ec;
    std::filesystem::directory_iterator iterator(intent_dir, list_ec);
    if (list_ec == std::errc::no_such_file_or_directory) {
        return {};
    }
    if (list_ec) {
        throw SnapshotArtifactUnstable(
            "read-only snapshot cannot list intents under " + intent_dir.string() + ": " + list_ec.message());
    }
    std::vector<std::string> names;
    for (const std::filesystem::directory_iterator end; iterator != end;) {
        auto name = iterator->path().filename().string();
        if (IsTxnIntentFileName(name)) {
            names.push_back(std::move(name));
        }
        iterator.increment(list_ec);
        if (list_ec) {
            throw SnapshotArtifactUnstable(
                "read-only snapshot cannot list intents under " + intent_dir.string() + ": " + list_ec.message());
        }
    }
    std::sort(names.begin(), names.end());
    std::vector<ReadOnlyArtifactSnapshot> intents;
    for (const auto& name : names) {
        auto intent = ReadArtifactForSnapshot(intent_dir / name);
        if (intent.present) {
            intents.push_back(std::move(intent));
        }
    }
    return intents;
}

[[nodiscard]] ReadOnlyChunkDiskSnapshot CollectReadOnlyChunkDiskSnapshot(
    const std::filesystem::path& data_path,
    const std::filesystem::path& wal_path,
    const std::filesystem::path& intent_path,
    const std::filesystem::path& txn_intent_dir,
    std::size_t collection,
    const std::function<void(
        std::size_t,
        ReadOnlySnapshotArtifact)>& observation) {
    ReadOnlyChunkDiskSnapshot snapshot;
    snapshot.image = ReadArtifactForSnapshot(data_path);
    observation(collection, ReadOnlySnapshotArtifact::kImage);
    snapshot.wal = ReadArtifactForSnapshot(wal_path);
    observation(collection, ReadOnlySnapshotArtifact::kWal);
    snapshot.intent = ReadArtifactForSnapshot(intent_path);
    snapshot.txn_intents = ReadTxnIntentsForSnapshot(txn_intent_dir);
    observation(collection, ReadOnlySnapshotArtifact::kIntent);
    return snapshot;
}

[[nodiscard]] std::uint64_t ReadSnapshotGeneration(
    const std::filesystem::path& path,
    bool record_required) {
    const auto artifact = ReadArtifactForSnapshot(path);
    if (!artifact.present) {
        // Generation zero is the implicit stable epoch of a store no writer
        // has opened yet. A writer durably creates an odd record before it
        // changes any snapshot artifact, and never removes the record: once
        // seen, its absence means the store directory itself went away.
        if (record_required) {
            throw std::runtime_error(
                "snapshot generation record " + path.string() +
                " disappeared after the store was opened: the table was dropped or its "
                "directory removed");
        }
        return 0;
    }
    std::uint64_t generation = 0;
    if (!TryParseSnapshotGenerationRecord(
            artifact.bytes, &generation)) {
        throw std::runtime_error(
            "read-only snapshot generation metadata is malformed: " +
            path.string());
    }
    return generation;
}

[[nodiscard]] bool ForceReadOnlySnapshotInstabilityForTests(
    const ChunkCoord& chunk_coord) {
    const char* value =
        std::getenv("CHUNKDB_FAILPOINT_READ_ONLY_SNAPSHOT_RETRY_EXHAUST");
    if (value == nullptr || value[0] == '\0') {
        return false;
    }
    return std::string_view(value) ==
           (std::to_string(chunk_coord.x) + "," +
            std::to_string(chunk_coord.y));
}

}  // namespace

std::uint64_t ReadSnapshotGenerationForScan(
    const std::filesystem::path& path,
    bool record_required) {
    return ReadSnapshotGeneration(path, record_required);
}

[[nodiscard]] ReadOnlyChunkDiskSnapshot LoadStableReadOnlyChunkDiskSnapshot(
    const std::filesystem::path& data_path,
    const std::filesystem::path& wal_path,
    const std::filesystem::path& intent_path,
    const std::filesystem::path& txn_intent_dir,
    const std::filesystem::path& generation_path,
    bool generation_record_required,
    const ChunkCoord& chunk_coord,
    const std::function<void(
        std::size_t,
        ReadOnlySnapshotArtifact)>& observation) {
    // Bounded backoff, not an unbounded wait: the reader still fails closed,
    // it just stops treating a normal-length writer bracket as damage.
    const auto backoff_budget =
        std::chrono::microseconds(kReadOnlySnapshotBackoffBudgetMs * 1000U);
    std::chrono::microseconds slept{0};
    std::chrono::microseconds next_sleep{kReadOnlySnapshotBackoffStartUs};
    std::size_t attempt = 0;
    std::string last_unstable;
    while (true) {
        ++attempt;
        try {
            const std::uint64_t before =
                ReadSnapshotGeneration(generation_path, generation_record_required);
            const auto snapshot = CollectReadOnlyChunkDiskSnapshot(
                data_path, wal_path, intent_path, txn_intent_dir, attempt, observation);
            const std::uint64_t after =
                ReadSnapshotGeneration(generation_path, generation_record_required);
            if ((before & 1U) == 0U && before == after &&
                !ForceReadOnlySnapshotInstabilityForTests(chunk_coord)) {
                return snapshot;
            }
        } catch (const SnapshotArtifactUnstable& unstable) {
            // Retry it like any other unstable observation. A genuinely
            // damaged artifact stays unreadable for the whole budget and
            // still fails closed below, carrying this message with it.
            last_unstable = unstable.what();
            observation(attempt, ReadOnlySnapshotArtifact::kUnstableObservation);
        }
        if (attempt < kReadOnlySnapshotSpinAttempts) {
            continue;
        }
        if (slept >= backoff_budget) {
            break;
        }
        const auto sleep_for = std::min(next_sleep, backoff_budget - slept);
        std::this_thread::sleep_for(sleep_for);
        slept += sleep_for;
        next_sleep = std::min(
            next_sleep * 2,
            std::chrono::microseconds(kReadOnlySnapshotBackoffMaxUs));
    }

    throw std::runtime_error(
        "read-only chunk snapshot remained unstable after " +
        std::to_string(attempt) + " bounded attempts spanning " +
        std::to_string(kReadOnlySnapshotBackoffBudgetMs) +
        "ms for chunk (" +
        std::to_string(chunk_coord.x) + "," +
        std::to_string(chunk_coord.y) + ")" +
        (last_unstable.empty() ? "" : "; last read failure: " + last_unstable));
}

std::vector<std::uint8_t> SerializeSnapshotGenerationRecord(
    std::uint64_t generation) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kSnapshotGenerationRecordSize);
    bytes.insert(
        bytes.end(),
        kSnapshotGenerationMagic.begin(),
        kSnapshotGenerationMagic.end());
    WriteLe64(bytes, generation);
    WriteLe32(bytes, Crc32(bytes));
    return bytes;
}

bool TryParseSnapshotGenerationRecord(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* generation) {
    if (generation == nullptr ||
        bytes.size() != kSnapshotGenerationRecordSize ||
        !std::equal(
            kSnapshotGenerationMagic.begin(),
            kSnapshotGenerationMagic.end(),
            bytes.begin())) {
        return false;
    }
    const std::uint32_t expected_crc = ReadLe32(bytes, 12U);
    if (Crc32(bytes.data(), 12U) != expected_crc) {
        return false;
    }
    *generation = ReadLe64(bytes, 4U);
    return true;
}

ChunkStore::SnapshotGenerationWriteGuard::SnapshotGenerationWriteGuard(
    ChunkStore* store)
    : store_(store),
      nested_on_same_thread_(
          std::find(
              g_snapshot_write_stack.begin(),
              g_snapshot_write_stack.end(),
              store_) != g_snapshot_write_stack.end()) {
    g_snapshot_write_stack.push_back(store_);
    try {
        std::lock_guard lock(store_->snapshot_generation_mutex_);
        store_->BeginSnapshotGenerationWriteLocked();
    } catch (...) {
        g_snapshot_write_stack.pop_back();
        throw;
    }
}

ChunkStore::SnapshotGenerationWriteGuard::~SnapshotGenerationWriteGuard() {
    if (!finished_) {
        std::lock_guard lock(store_->snapshot_generation_mutex_);
        store_->AbandonSnapshotGenerationWriteLocked(
            !nested_on_same_thread_);
        g_snapshot_write_stack.pop_back();
    }
}

void ChunkStore::SnapshotGenerationWriteGuard::Finish() {
    if (finished_) {
        return;
    }
    finished_ = true;
    g_snapshot_write_stack.pop_back();
    std::lock_guard lock(store_->snapshot_generation_mutex_);
    store_->FinishSnapshotGenerationWriteLocked();
}

void ChunkStore::InitializeSnapshotGeneration(bool store_preexisting) {
    (void)store_preexisting;
    snapshot_generation_path_ =
        data_dir_ / std::string(kSnapshotGenerationFile);
    if (access_mode_ != AccessMode::kReadOnly) {
        // A crash while the record was being replaced leaves its temp file.
        CleanupAtomicTmpArtifacts(snapshot_generation_path_);
    }

    const auto artifact =
        ReadArtifactForSnapshot(snapshot_generation_path_);
    std::uint64_t persisted = 0;
    if (artifact.present &&
        !TryParseSnapshotGenerationRecord(
            artifact.bytes, &persisted)) {
        throw std::runtime_error(
            "snapshot generation metadata is malformed: " +
            snapshot_generation_path_.string());
    }

    snapshot_generation_ = persisted;
    snapshot_generation_record_seen_ = artifact.present;
    if (access_mode_ == AccessMode::kReadOnly) {
        return;
    }
    if (persisted >
        std::numeric_limits<std::uint64_t>::max() - 2U) {
        throw std::overflow_error(
            "snapshot generation exhausted; refusing to wrap");
    }

    // Startup itself is a writer transition because rollback-intent recovery
    // can truncate WALs and remove intent files. If the previous process
    // crashed while odd, skip to a new odd value rather than reusing its
    // epoch. The durable odd publication precedes every recovery mutation.
    const std::uint64_t recovery_generation =
        (persisted & 1U) == 0U ? persisted + 1U : persisted + 2U;
    if (ConsumeFailpointEnv(
            "CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_BEGIN_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected snapshot generation begin failure");
    }
    bool recovery_generation_replaced = false;
    try {
        AtomicWrite(
            snapshot_generation_path_,
            SerializeSnapshotGenerationRecord(recovery_generation),
            /*fsync_file=*/true,
            /*fsync_directory=*/true,
            &recovery_generation_replaced,
            /*after_rename_failpoint=*/nullptr,
            /*enable_generic_failpoints=*/false);
    } catch (...) {
        if (recovery_generation_replaced) {
            snapshot_generation_ = recovery_generation;
        }
        throw;
    }
    snapshot_generation_ = recovery_generation;
    snapshot_generation_record_seen_ = true;
    CrashAtSnapshotFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_AFTER_BEGIN_ONCE");
}

void ChunkStore::FinishSnapshotGenerationRecovery() {
    if (access_mode_ == AccessMode::kReadOnly) {
        return;
    }
    if ((snapshot_generation_ & 1U) == 0U ||
        snapshot_generation_ ==
            std::numeric_limits<std::uint64_t>::max()) {
        throw std::runtime_error(
            "invalid snapshot generation at recovery completion");
    }
    if (ConsumeFailpointEnv(
            "CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_END_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected snapshot generation end failure");
    }
    CrashAtSnapshotFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_BEFORE_END_ONCE");
    const std::uint64_t stable_generation = snapshot_generation_ + 1U;
    bool stable_generation_replaced = false;
    try {
        AtomicWrite(
            snapshot_generation_path_,
            SerializeSnapshotGenerationRecord(stable_generation),
            /*fsync_file=*/true,
            /*fsync_directory=*/true,
            &stable_generation_replaced,
            /*after_rename_failpoint=*/nullptr,
            /*enable_generic_failpoints=*/false);
    } catch (...) {
        if (stable_generation_replaced) {
            snapshot_generation_ = stable_generation;
        }
        throw;
    }
    snapshot_generation_ = stable_generation;
}

void ChunkStore::BeginSnapshotGenerationWriteLocked() {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument(
            "read-only store cannot begin a snapshot generation write");
    }
    if (snapshot_generation_active_writers_ > 0U) {
        ++snapshot_generation_active_writers_;
        return;
    }
    if ((snapshot_generation_ & 1U) != 0U) {
        throw std::runtime_error(
            "snapshot generation is unresolved; restart the writer to "
            "complete recovery");
    }
    if (snapshot_generation_ >
        std::numeric_limits<std::uint64_t>::max() - 2U) {
        throw std::overflow_error(
            "snapshot generation exhausted; refusing to wrap");
    }
    if (ConsumeFailpointEnv(
            "CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_BEGIN_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected snapshot generation begin failure");
    }
    const std::uint64_t write_generation = snapshot_generation_ + 1U;
    bool write_generation_replaced = false;
    try {
        AtomicWrite(
            snapshot_generation_path_,
            SerializeSnapshotGenerationRecord(write_generation),
            /*fsync_file=*/true,
            /*fsync_directory=*/true,
            &write_generation_replaced,
            /*after_rename_failpoint=*/nullptr,
            /*enable_generic_failpoints=*/false);
    } catch (...) {
        if (write_generation_replaced) {
            snapshot_generation_ = write_generation;
        }
        throw;
    }
    snapshot_generation_ = write_generation;
    stats_snapshot_generation_odd_publications_.fetch_add(
        1, std::memory_order_relaxed);
    snapshot_generation_active_writers_ = 1U;
    snapshot_generation_epoch_failed_ = false;
    CrashAtSnapshotFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_AFTER_BEGIN_ONCE");
}

void ChunkStore::PublishStableSnapshotGenerationLocked() {
    if ((snapshot_generation_ & 1U) == 0U ||
        snapshot_generation_ ==
            std::numeric_limits<std::uint64_t>::max()) {
        throw std::logic_error(
            "snapshot generation write did not hold an odd epoch");
    }
    if (ConsumeFailpointEnv(
            "CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_END_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected snapshot generation end failure");
    }
    CrashAtSnapshotFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_BEFORE_END_ONCE");
    const std::uint64_t stable_generation = snapshot_generation_ + 1U;
    bool stable_generation_replaced = false;
    try {
        // The even (stable) record keeps its file-data sync (so a crash can
        // never expose a torn record through the rename) but skips the
        // directory sync: losing the rename to a crash only re-exposes the
        // previous record, which is always the durable odd published by
        // BeginSnapshotGenerationWriteLocked — readers then fail closed
        // until writer recovery, strictly more conservative. The odd record
        // keeps both syncs because it is what fail-closes post-crash
        // readers before the bracketed artifacts are known coherent. Rename
        // visibility is immediate for live same-host readers, which is all
        // the running bracket protocol needs.
        AtomicWrite(
            snapshot_generation_path_,
            SerializeSnapshotGenerationRecord(stable_generation),
            /*fsync_file=*/true,
            /*fsync_directory=*/false,
            &stable_generation_replaced,
            /*after_rename_failpoint=*/nullptr,
            /*enable_generic_failpoints=*/false);
    } catch (...) {
        if (stable_generation_replaced) {
            snapshot_generation_ = stable_generation;
        }
        throw;
    }
    snapshot_generation_ = stable_generation;
    stats_snapshot_generation_even_publications_.fetch_add(
        1, std::memory_order_relaxed);
}

void ChunkStore::FinishSnapshotGenerationWriteLocked() {
    if (snapshot_generation_active_writers_ == 0U) {
        throw std::logic_error(
            "snapshot generation active-writer underflow");
    }
    --snapshot_generation_active_writers_;
    if (snapshot_generation_active_writers_ != 0U) {
        return;
    }
    if (snapshot_generation_epoch_failed_) {
        throw std::runtime_error(
            "a concurrent snapshot transition failed; snapshot generation "
            "remains odd until writer restart");
    }
    if ((snapshot_generation_ & 1U) == 0U ||
        snapshot_generation_ ==
            std::numeric_limits<std::uint64_t>::max()) {
        throw std::logic_error(
            "snapshot generation write did not hold an odd epoch");
    }
    // The last active publication closes the bracket before returning.
    // Delegating this to an idle timer can leave coherent artifacts unreadable
    // for an arbitrarily delayed scheduler wake-up. Concurrent and nested
    // transitions still share the epoch until the last one finishes.
    PublishStableSnapshotGenerationLocked();
}

void ChunkStore::AbandonSnapshotGenerationWriteLocked(
    bool fail_epoch) noexcept {
    if (fail_epoch) {
        snapshot_generation_epoch_failed_ = true;
    }
    if (snapshot_generation_active_writers_ > 0U) {
        --snapshot_generation_active_writers_;
    }
}

void ChunkStore::SetSnapshotGenerationLingerForTests(
    std::uint64_t window_ms,
    std::size_t max_brackets) {
    // Retained for source compatibility with older test harnesses. Completed
    // publications cannot defer their stable generation, regardless of policy.
    (void)window_ms;
    (void)max_brackets;
}

bool ChunkStore::SnapshotGenerationLingerPendingForTests() const {
    return false;
}

std::uint64_t ChunkStore::SnapshotGenerationForTests() const {
    std::lock_guard lock(snapshot_generation_mutex_);
    return snapshot_generation_;
}

std::uint64_t ChunkStore::SnapshotGenerationOddPublicationsForTests()
    const noexcept {
    return stats_snapshot_generation_odd_publications_.load(
        std::memory_order_relaxed);
}

std::uint64_t ChunkStore::SnapshotGenerationEvenPublicationsForTests()
    const noexcept {
    return stats_snapshot_generation_even_publications_.load(
        std::memory_order_relaxed);
}

void ChunkStore::FlushSnapshotGenerationLingerForTests() {
    // Compatibility no-op: the last active publication already closes itself.
}

}  // namespace chunkdb
