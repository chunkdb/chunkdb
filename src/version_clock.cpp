#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <limits>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "checkpoint.hpp"
#include "change_feed.hpp"
#include "chunk_coordinate_index.hpp"
#include "chunkdb/bit_codec.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "wal_writer.hpp"

namespace chunkdb {

namespace {

[[nodiscard]] std::uint64_t MicrosecondsSince(std::chrono::steady_clock::time_point started) {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - started).count());
}

// Number of version tokens reserved (and durably persisted) per extension of
// the version clock. Larger values amortize the persistence fsync over more
// mutations at the cost of skipping more tokens per restart.
constexpr std::uint64_t kVersionClockReservationBatch = 65536;
// When half a range of tokens is left below the ceiling, the next range is
// written on the extension thread, so a writer waits for a clock write only
// when it issues this many tokens faster than the record syncs. A restart
// skips at most one and a half ranges.
constexpr std::uint64_t kVersionClockExtensionLead = kVersionClockReservationBatch / 2U;

// Persisted version-clock bookkeeping format (data_dir/chunkdb.version). The
// record is exactly kVersionClockRecordSize bytes:
//   [0, 4)   magic "CKVR"
//   [4, 12)  little-endian u64 reserved ceiling (exclusive upper bound over
//            every version token this store may already have issued)
//   [12, 16) little-endian u32 CRC32 of bytes [0, 12)
// Any deviation (short, long, wrong magic, bad CRC, or a zero ceiling) is
// treated as damage, never as a fresh clock, so a lost or corrupted record can
// never silently reset the clock and reissue an already-exposed token.
constexpr std::array<std::uint8_t, 4> kVersionClockMagic = {'C', 'K', 'V', 'R'};
constexpr std::size_t kVersionClockRecordSize = 16;
constexpr std::array<std::uint8_t, 4> kInitializedMarkerMagic = {'C', 'K', 'I', 'D'};
constexpr std::size_t kInitializedMarkerRecordSize = 16;

[[nodiscard]] std::vector<std::uint8_t> SerializeInitializedMarker() {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kInitializedMarkerRecordSize);
    bytes.insert(bytes.end(), kInitializedMarkerMagic.begin(), kInitializedMarkerMagic.end());
    WriteLe64(bytes, 1U);
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    return bytes;
}

[[nodiscard]] bool IsValidInitializedMarker(const std::vector<std::uint8_t>& bytes) {
    return bytes.size() == kInitializedMarkerRecordSize &&
           std::equal(kInitializedMarkerMagic.begin(), kInitializedMarkerMagic.end(), bytes.begin()) &&
           ReadLe64(bytes, 4U) == 1U &&
           ReadLe32(bytes, 12U) == Crc32(bytes.data(), 12U);
}

}  // namespace

std::vector<std::uint8_t> SerializeVersionClockRecord(std::uint64_t ceiling) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kVersionClockRecordSize);
    bytes.insert(bytes.end(), kVersionClockMagic.begin(), kVersionClockMagic.end());
    WriteLe64(bytes, ceiling);
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    return bytes;
}

// Parses a version-clock record, returning true only for a structurally and
// checksum-valid record with a non-zero ceiling.
bool TryParseVersionClockRecord(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* out_ceiling) {
    if (bytes.size() != kVersionClockRecordSize) {
        return false;
    }
    if (!std::equal(kVersionClockMagic.begin(), kVersionClockMagic.end(), bytes.begin())) {
        return false;
    }
    if (ReadLe32(bytes, 12U) != Crc32(bytes.data(), 12U)) {
        return false;
    }
    const std::uint64_t ceiling = ReadLe64(bytes, 4U);
    if (ceiling == 0U) {
        return false;
    }
    *out_ceiling = ceiling;
    return true;
}

bool IsValidInitializedStoreMarker(const std::vector<std::uint8_t>& bytes) {
    return IsValidInitializedMarker(bytes);
}

std::vector<std::uint8_t> SerializeInitializedStoreMarker() {
    return SerializeInitializedMarker();
}

std::uint64_t NewChunkVersionToken() {
    static std::mutex rng_mutex;
    static std::mt19937_64 rng([] {
        std::random_device device;
        return (static_cast<std::uint64_t>(device()) << 32U) ^ device();
    }());

    std::uint64_t epoch = 0;
    {
        std::lock_guard lock(rng_mutex);
        epoch = rng();
    }
    return ((epoch & 0xFFFFFFFFULL) << 32U) | 1ULL;
}

std::optional<std::uint64_t> ChunkStore::CompletedRevision() const {
    if (access_mode_ == AccessMode::kReadOnly) return std::nullopt;
    return write_producers_->CompletedFrontier(version_clock_);
}

void ChunkStore::InitializeVersionClock(bool store_preexisting, const std::error_code& inventory_error) {
    if (access_mode_ == AccessMode::kReadOnly) {
        return;
    }
    version_clock_path_ = data_dir_ / "chunkdb.version";
    const auto initialized_marker_path = data_dir_ / ".chunkdb.initialized";
    // A crash while either record was being replaced leaves its temp file.
    CleanupAtomicTmpArtifacts(version_clock_path_);
    CleanupAtomicTmpArtifacts(initialized_marker_path);

    std::error_code initialized_exists_ec;
    const bool initialized_marker_present =
        std::filesystem::exists(initialized_marker_path, initialized_exists_ec);
    if (initialized_exists_ec) {
        throw std::runtime_error(
            "cannot inspect initialized-store marker " +
            initialized_marker_path.string() + ": " +
            initialized_exists_ec.message());
    }
    if (initialized_marker_present) {
        const auto marker_bytes = LoadFile(initialized_marker_path);
        if (!IsValidInitializedMarker(marker_bytes)) {
            throw std::runtime_error(
                "initialized-store marker is invalid: " +
                initialized_marker_path.string() +
                "; run chunkdb_admin rebuild-version-clock to replace it");
        }
    }

    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_VERSION_STAT_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected version bookkeeping inspection failure: " +
            version_clock_path_.string());
    }
    std::error_code exists_ec;
    const bool present = std::filesystem::exists(version_clock_path_, exists_ec);
    if (exists_ec) {
        // The bookkeeping cannot even be inspected. Refusing is the only way to
        // keep the deterministic guarantee: a stat failure is not proof of a
        // fresh store, and resetting the clock here could reissue a token a
        // previous instance already exposed.
        throw std::runtime_error(
            "cannot inspect version bookkeeping " + version_clock_path_.string() +
            " (ec=" + std::to_string(exists_ec.value()) + ", msg='" + exists_ec.message() +
            "'); refusing to open so a stale chunk version can never be reissued");
    }

    if (!present) {
        if (initialized_marker_present) {
            // The checked marker proves that deterministic tokens were exposed
            // from this store. Starting a fresh clock could reissue one.
            throw std::runtime_error(
                "version bookkeeping " + version_clock_path_.string() +
                " is missing but the valid initialized-store marker proves "
                "that persisted version tokens were previously issued; the "
                "deterministic chunk-version guarantee cannot be honored. "
                "Restore the file from backup, run chunkdb_admin "
                "rebuild-version-clock, or intentionally reinitialize the "
                "whole store; refusing to open with a reset clock");
        }
        if (inventory_error) {
            throw std::runtime_error(
                "both version bookkeeping records are missing: " + version_clock_path_.string() +
                " and " + initialized_marker_path.string() +
                "; startup inventory is unavailable (ec=" + std::to_string(inventory_error.value()) +
                ", msg='" + inventory_error.message() + "'), so this cannot be proved a new store. "
                "Restore a consistent backup, run chunkdb_admin rebuild-version-clock, or intentionally "
                "reinitialize the whole store; refusing to open with a reset clock");
        }
        if (store_preexisting) {
            throw std::runtime_error(
                "both version bookkeeping records are missing: " + version_clock_path_.string() +
                " and " + initialized_marker_path.string() +
                "; chunk artifacts or a capped startup scan prevent proving this is a new store. "
                "Restore a consistent backup, run chunkdb_admin rebuild-version-clock, or intentionally "
                "reinitialize the whole store; refusing to open with a reset clock");
        }
        // Nothing proves that deterministic tokens were ever exposed: a new
        // store, or one whose initialization stopped after the manifest.
        // Persist the clock first and the marker second; a crash between them
        // is recognized by the valid clock on the next startup.
        const std::uint64_t first =
            std::max<std::uint64_t>(1U, std::min(initial_version_floor_, std::numeric_limits<std::uint64_t>::max() - 1U));
        version_clock_.store(first, std::memory_order_relaxed);
        version_clock_ceiling_.store(first, std::memory_order_relaxed);
        AtomicWrite(
            version_clock_path_,
            SerializeVersionClockRecord(first),
            /*fsync_file=*/true,
            /*fsync_directory=*/true,
            /*out_replaced=*/nullptr,
            /*after_rename_failpoint=*/nullptr,
            // Clock bookkeeping must not consume the generic ATOMICWRITE
            // failpoints a test armed for a data-path write; the clock has
            // its own VERSION_RESERVE_FAIL_ONCE hook.
            /*enable_generic_failpoints=*/false);
        AtomicWrite(
            initialized_marker_path,
            SerializeInitializedMarker(),
            /*fsync_file=*/true,
            /*fsync_directory=*/true,
            /*out_replaced=*/nullptr,
            /*after_rename_failpoint=*/nullptr,
            // Clock bookkeeping must not consume the generic ATOMICWRITE
            // failpoints a test armed for a data-path write; the clock has
            // its own VERSION_RESERVE_FAIL_ONCE hook.
            /*enable_generic_failpoints=*/false);
        return;
    }

    std::vector<std::uint8_t> bytes;
    std::uintmax_t record_size = kVersionClockRecordSize;
    try {
        std::error_code size_ec;
        record_size = std::filesystem::file_size(version_clock_path_, size_ec);
        if (size_ec) {
            throw std::runtime_error(
                "failed to inspect record size: " + size_ec.message());
        }
        if (record_size == kVersionClockRecordSize) {
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_VERSION_READ_FAIL_ONCE")) {
                throw std::runtime_error("injected version bookkeeping read failure");
            }
            bytes = LoadFile(version_clock_path_);
        }
    } catch (const std::exception& e) {
        // Present but unreadable: treat as damage, never as a reset
        // opportunity. The offline rebuild refuses an unreadable clock too, so
        // the file's access must be fixed first.
        throw std::runtime_error(
            "version bookkeeping " + version_clock_path_.string() +
            " exists but could not be read (" + e.what() +
            "); refusing to open so a stale chunk version can never be reissued");
    }
    if (record_size != kVersionClockRecordSize) {
        throw std::runtime_error(
            "version bookkeeping " + version_clock_path_.string() +
            " exists but could not be read (record size is " + std::to_string(record_size) +
            " bytes, expected " + std::to_string(kVersionClockRecordSize) +
            "); refusing to open so a stale chunk version can never be reissued; "
            "run chunkdb_admin rebuild-version-clock to replace it");
    }

    std::uint64_t persisted_ceiling = 0;
    if (!TryParseVersionClockRecord(bytes, &persisted_ceiling)) {
        throw std::runtime_error(
            "version bookkeeping " + version_clock_path_.string() +
            " is malformed, truncated, oversized, or corrupt (size=" +
            std::to_string(bytes.size()) + " bytes, expected " +
            std::to_string(kVersionClockRecordSize) +
            "); refusing to open so a stale chunk version can never be reissued; run chunkdb_admin rebuild-version-clock to replace it");
    }

    // Every token ever issued before this start-up was strictly below the last
    // persisted ceiling. Starting the clock there guarantees new tokens are at
    // or above it, so a stale version can never match after a restart. Token
    // zero is reserved as the "no version" sentinel.
    version_clock_.store(persisted_ceiling, std::memory_order_relaxed);
    version_clock_ceiling_.store(persisted_ceiling, std::memory_order_relaxed);
    if (!initialized_marker_present) {
        // Initialization stopped between the clock and the marker. The valid
        // clock record already proves initialization; persist the marker
        // before this instance can expose another token.
        AtomicWrite(
            initialized_marker_path,
            SerializeInitializedMarker(),
            /*fsync_file=*/true,
            /*fsync_directory=*/true,
            /*out_replaced=*/nullptr,
            /*after_rename_failpoint=*/nullptr,
            // Clock bookkeeping must not consume the generic ATOMICWRITE
            // failpoints a test armed for a data-path write; the clock has
            // its own VERSION_RESERVE_FAIL_ONCE hook.
            /*enable_generic_failpoints=*/false);
    }
}

void ChunkStore::WriteVersionClockCeiling(std::uint64_t ceiling) {
    // Persists the new ceiling before any token at or above the old one is
    // issued, so a crash can never leave issued tokens above the durable
    // ceiling.
    if (version_clock_pause_armed_.load(std::memory_order_acquire)) PauseVersionClockExtensionIfArmed();
    AtomicWrite(
        version_clock_path_,
        SerializeVersionClockRecord(ceiling),
        /*fsync_file=*/true,
        /*fsync_directory=*/true,
        /*out_replaced=*/nullptr,
        /*after_rename_failpoint=*/nullptr,
        // Clock bookkeeping must not consume the generic ATOMICWRITE
        // failpoints a test armed for a data-path write; the clock has
        // its own VERSION_RESERVE_FAIL_ONCE hook.
        /*enable_generic_failpoints=*/false,
        /*before_replace=*/[] {
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CRASH_VERSION_EXTENSION_BEFORE_REPLACE_ONCE")) ExitAbruptly(86);
        });
}

void ChunkStore::AdvanceVersionClockCeilingLocked(std::unique_lock<std::mutex>& lock, std::uint64_t minimum) {
    if (version_clock_extension_in_flight_) {
        // One writer of the record at a time: wait for the one in flight.
        version_clock_extended_.wait(lock, [this] { return !version_clock_extension_in_flight_; });
        return;
    }
    if (minimum > std::numeric_limits<std::uint64_t>::max() - kVersionClockReservationBatch) {
        throw std::overflow_error("chunk version clock exhausted");
    }
    const std::uint64_t ceiling = minimum + kVersionClockReservationBatch;
    version_clock_extension_in_flight_ = true;
    lock.unlock();
    const auto started = std::chrono::steady_clock::now();
    try {
        WriteVersionClockCeiling(ceiling);
    } catch (...) {
        // The ceiling stays; the next writer at it writes the record again.
        lock.lock();
        version_clock_extension_in_flight_ = false;
        version_clock_extended_.notify_all();
        throw;
    }
    const auto elapsed_us = MicrosecondsSince(started);
    lock.lock();
    // Counted before the ceiling is published, so a reader of the ceiling
    // sees the count.
    stats_version_clock_extensions_.fetch_add(1U, std::memory_order_relaxed);
    stats_version_clock_extension_us_.fetch_add(elapsed_us, std::memory_order_relaxed);
    version_clock_ceiling_.store(ceiling, std::memory_order_release);
    version_clock_extension_in_flight_ = false;
    version_clock_extended_.notify_all();
}

void ChunkStore::StartVersionClockExtension(std::uint64_t ceiling) {
    std::lock_guard lock(version_clock_mutex_);
    StartVersionClockExtensionLocked(ceiling);
}

void ChunkStore::StartVersionClockExtensionLocked(std::uint64_t ceiling) {
    if (!version_clock_early_extensions_ || version_clock_extension_in_flight_ ||
        version_clock_ceiling_.load(std::memory_order_acquire) != ceiling ||
        ceiling > std::numeric_limits<std::uint64_t>::max() - kVersionClockReservationBatch) {
        return;
    }
    // The previous extension thread cleared the in-flight flag under this
    // mutex as its last locked step, so joining it does not wait on the
    // mutex held here.
    if (version_clock_extender_.joinable()) version_clock_extender_.join();
    version_clock_extension_in_flight_ = true;
    try {
        version_clock_extender_ = std::thread([this, ceiling] { RunVersionClockExtension(ceiling); });
    } catch (const std::system_error&) {
        // No thread: the writer that reaches the ceiling writes the record.
        version_clock_extension_in_flight_ = false;
    }
}

void ChunkStore::RunVersionClockExtension(std::uint64_t ceiling) noexcept {
    const std::uint64_t next = ceiling + kVersionClockReservationBatch;
    const auto started = std::chrono::steady_clock::now();
    bool written = false;
    try {
        WriteVersionClockCeiling(next);
        written = true;
    } catch (const std::exception& error) {
        // Nothing was issued from the next range. The writer that reaches
        // the ceiling writes the record itself and gets its own error.
        LogMessage(
            LogLevel::kWarn, LogComponent::kStore, "version clock extension failed; writers at the ceiling retry it",
            {{"data_dir", data_dir_.string()}, {"error", error.what()}});
    }
    const auto elapsed_us = MicrosecondsSince(started);
    {
        std::lock_guard lock(version_clock_mutex_);
        if (written) {
            stats_version_clock_extensions_.fetch_add(1U, std::memory_order_relaxed);
            stats_version_clock_extension_us_.fetch_add(elapsed_us, std::memory_order_relaxed);
            version_clock_ceiling_.store(next, std::memory_order_release);
        }
        version_clock_extension_in_flight_ = false;
    }
    version_clock_extended_.notify_all();
}

void ChunkStore::PauseVersionClockExtensionIfArmed() {
    std::unique_lock lock(durability_hook_mutex_);
    if (!version_clock_pause_armed_.exchange(false, std::memory_order_acq_rel)) return;
    version_clock_pause_reached_ = true;
    durability_hook_cv_.notify_all();
    durability_hook_cv_.wait(lock, [this] { return version_clock_pause_resumed_; });
    if (version_clock_pause_fail_) throw std::runtime_error("injected version clock extension failure");
}

void ChunkStore::ArmVersionClockExtensionPauseForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    version_clock_pause_reached_ = false;
    version_clock_pause_resumed_ = false;
    version_clock_pause_fail_ = false;
    version_clock_pause_armed_.store(true, std::memory_order_release);
}

bool ChunkStore::WaitForVersionClockExtensionPauseForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    return durability_hook_cv_.wait_for(lock, std::chrono::seconds(10), [this] { return version_clock_pause_reached_; });
}

void ChunkStore::ResumeVersionClockExtensionForTests(bool fail) {
    std::lock_guard lock(durability_hook_mutex_);
    version_clock_pause_fail_ = fail;
    version_clock_pause_resumed_ = true;
    durability_hook_cv_.notify_all();
}

void ChunkStore::StopVersionClockExtension() noexcept {
    std::thread extender;
    {
        std::lock_guard lock(version_clock_mutex_);
        version_clock_early_extensions_ = false;
        extender = std::move(version_clock_extender_);
    }
    {
        // A held test pause must not keep shutdown waiting.
        std::lock_guard lock(durability_hook_mutex_);
        version_clock_pause_armed_.store(false, std::memory_order_release);
        version_clock_pause_resumed_ = true;
        durability_hook_cv_.notify_all();
    }
    // The write in flight finishes: the record holds its ceiling or the one
    // before, both above every issued token.
    if (extender.joinable()) extender.join();
}

std::uint64_t ChunkStore::NextChunkVersion() {
    if (access_mode_ == AccessMode::kReadOnly) {
        return NewChunkVersionToken();
    }

    // Deterministic hook for the "version reservation fails before the mutation
    // is visible" test. Because conditional mutations reserve the token before
    // any WAL append, throwing here leaves nothing to roll back.
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_VERSION_RESERVE_FAIL_ONCE")) {
        throw std::runtime_error("injected version reservation failure");
    }

    // The clock only advances below the durable ceiling, so it never passes
    // the ceiling and can never wrap: an exhausted clock refuses every later
    // reservation instead of issuing token zero or a reused token.
    std::uint64_t candidate = version_clock_.load(std::memory_order_seq_cst);
    while (true) {
        const std::uint64_t ceiling = version_clock_ceiling_.load(std::memory_order_acquire);
        if (candidate >= ceiling) {
            // Reserved range exhausted: wait for a higher durable ceiling
            // before issuing any token at or above it. Throws, leaving the
            // clock unchanged, when the write fails or no higher ceiling
            // exists. The wait counts from here, so a writer blocked behind
            // another writer's extension counts too.
            const auto waiting = std::chrono::steady_clock::now();
            {
                std::unique_lock lock(version_clock_mutex_);
                candidate = version_clock_.load(std::memory_order_seq_cst);
                if (candidate >= version_clock_ceiling_.load(std::memory_order_acquire)) {
                    AdvanceVersionClockCeilingLocked(lock, candidate);
                    candidate = version_clock_.load(std::memory_order_seq_cst);
                }
            }
            stats_version_clock_wait_us_.fetch_add(MicrosecondsSince(waiting), std::memory_order_relaxed);
            continue;
        }
        // Sequentially consistent: a write loads the open-transaction count
        // after taking its version, and a transaction snapshot reads the
        // clock after counting itself open (txn_history.hpp), so one of the
        // two always sees the other.
        if (version_clock_.compare_exchange_weak(
                candidate, candidate + 1U, std::memory_order_seq_cst, std::memory_order_seq_cst)) {
            // Exactly one writer takes the token half a range below this
            // ceiling, and it starts writing the next range while the rest
            // of this one is issued.
            if (ceiling - candidate == kVersionClockExtensionLead) StartVersionClockExtension(ceiling);
            return candidate;
        }
    }
}

std::uint64_t ChunkStore::NextCommitTimeMs(const RegularChunk& chunk) {
#ifdef CHUNKDB_BENCH_CONSTANT_COMMIT_TIME
    // Benchmark builds only: a fixed time per chunk, without the clock read
    // and the shared monotonic step, so a paired run measures their cost.
    return std::max<std::uint64_t>(chunk.commit_time_ms, 1U);
#else
    const std::uint64_t floor = std::max(UnixMillisNow(), chunk.commit_time_ms);
    std::uint64_t last = last_commit_time_ms_.load(std::memory_order_relaxed);
    while (true) {
        const std::uint64_t issued = std::max(floor, last);
        if (last_commit_time_ms_.compare_exchange_weak(
                last, issued, std::memory_order_relaxed, std::memory_order_relaxed)) {
            return issued;
        }
    }
#endif
}

void ChunkStore::RaiseVersionClockAbove(std::uint64_t revision) {
    if (access_mode_ == AccessMode::kReadOnly || revision == 0 ||
        revision == std::numeric_limits<std::uint64_t>::max()) {
        return;
    }
    if (version_clock_.load(std::memory_order_relaxed) > revision) {
        return;
    }
    std::unique_lock lock(version_clock_mutex_);
    const std::uint64_t next = revision + 1;
    while (version_clock_.load(std::memory_order_relaxed) <= revision &&
           next >= version_clock_ceiling_.load(std::memory_order_acquire)) {
        AdvanceVersionClockCeilingLocked(lock, next);
    }
    if (version_clock_.load(std::memory_order_relaxed) > revision) {
        return;
    }
    // NextChunkVersion takes tokens without this mutex, so the clock may have
    // moved past `next` since the check: only ever raise it, or tokens
    // already issued would be issued again.
    std::uint64_t current = version_clock_.load(std::memory_order_relaxed);
    while (current < next &&
           !version_clock_.compare_exchange_weak(current, next, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
    // The raise can pass the token that starts the next range's write.
    const std::uint64_t ceiling = version_clock_ceiling_.load(std::memory_order_acquire);
    if (ceiling - next <= kVersionClockExtensionLead) StartVersionClockExtensionLocked(ceiling);
}

std::uint64_t ChunkStore::ChunkCommitTimeForTests(std::int64_t chunk_x, std::int64_t chunk_y) {
    const auto regular_chunk = GetOrLoadRegularChunk(ChunkCoord{chunk_x, chunk_y});
    std::shared_lock lock(regular_chunk->mutex);
    return regular_chunk->commit_time_ms;
}

std::uint64_t ChunkStore::GetChunkVersion(std::int64_t chunk_x, std::int64_t chunk_y) {
    if (geometry_.dims() != 2U) throw std::invalid_argument("chunk coordinates must match the table dimensions");
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return regular_chunk->version;
}

bool ChunkStore::HasWrittenChunkIdentities() const {
    const auto index = const_cast<ChunkStore*>(this)->CoordinateIndexForScan();
    if (!index) throw std::runtime_error("coordinate index is not initialized");
    return index->has_written_identities();
}

}  // namespace chunkdb
