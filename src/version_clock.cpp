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
#include "wal_writer.hpp"

namespace chunkdb {

namespace {

// Number of version tokens reserved (and durably persisted) per extension of
// the version clock. Larger values amortize the persistence fsync over more
// mutations at the cost of skipping at most this many tokens per restart.
constexpr std::uint64_t kVersionClockReservationBatch = 16384;

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

bool TryParseIntermediateVersionClockRecord(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* out_ceiling) {
    if (bytes.size() != sizeof(std::uint64_t)) {
        return false;
    }
    const std::uint64_t ceiling = ReadLe64(bytes, 0U);
    if (ceiling == 0U) {
        return false;
    }
    *out_ceiling = ceiling;
    return true;
}

bool IsValidInitializedStoreMarker(const std::vector<std::uint8_t>& bytes) {
    return IsValidInitializedMarker(bytes);
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

void ChunkStore::InitializeVersionClock(bool store_preexisting) {
    if (access_mode_ == AccessMode::kReadOnly) {
        return;
    }
    version_clock_path_ = data_dir_ / "chunkdb.version";
    const auto initialized_marker_path = data_dir_ / ".chunkdb.initialized";

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
                initialized_marker_path.string());
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
                "Restore the file from backup or intentionally reinitialize "
                "the whole store; refusing to open with a reset clock");
        }
        // No new-format invariant proves that deterministic tokens were ever
        // exposed. This is either a genuinely new store or a stable-v1 legacy
        // store, whose format predates both bookkeeping files. Persist the
        // clock first and the marker second; a crash between them is safely
        // recognized by the valid clock on the next startup.
        version_clock_.store(1U, std::memory_order_relaxed);
        version_clock_ceiling_.store(1U, std::memory_order_relaxed);
        AtomicWrite(
            version_clock_path_,
            SerializeVersionClockRecord(1U),
            /*fsync_file=*/true,
            /*fsync_directory=*/true);
        AtomicWrite(
            initialized_marker_path,
            SerializeInitializedMarker(),
            /*fsync_file=*/true,
            /*fsync_directory=*/true);
        if (store_preexisting) {
            LogMessage(
                LogLevel::kInfo,
                LogComponent::kRecovery,
                "migrated legacy store to checked version-clock bookkeeping",
                {{"version_ceiling", "1"}});
        }
        return;
    }

    std::vector<std::uint8_t> bytes;
    try {
        std::error_code size_ec;
        const auto record_size =
            std::filesystem::file_size(version_clock_path_, size_ec);
        if (size_ec) {
            throw std::runtime_error(
                "failed to inspect record size: " + size_ec.message());
        }
        if (record_size != kVersionClockRecordSize &&
            record_size != sizeof(std::uint64_t)) {
            throw std::runtime_error(
                "record size is " + std::to_string(record_size) +
                " bytes, expected " + std::to_string(kVersionClockRecordSize) +
                " (checked) or 8 (intermediate ceiling)");
        }
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_VERSION_READ_FAIL_ONCE")) {
            throw std::runtime_error("injected version bookkeeping read failure");
        }
        bytes = LoadFile(version_clock_path_);
    } catch (const std::exception& e) {
        // Present but unreadable: treat as damage, never as a reset opportunity.
        throw std::runtime_error(
            "version bookkeeping " + version_clock_path_.string() +
            " exists but could not be read (" + e.what() +
            "); refusing to open so a stale chunk version can never be reissued");
    }

    std::uint64_t persisted_ceiling = 0;
    const bool checked_record =
        TryParseVersionClockRecord(bytes, &persisted_ceiling);
    const bool intermediate_record =
        !checked_record &&
        TryParseIntermediateVersionClockRecord(bytes, &persisted_ceiling);
    if (!checked_record && !intermediate_record) {
        throw std::runtime_error(
            "version bookkeeping " + version_clock_path_.string() +
            " is malformed, truncated, oversized, or corrupt (size=" +
            std::to_string(bytes.size()) + " bytes, expected " +
            std::to_string(kVersionClockRecordSize) +
            " checked bytes or a valid 8-byte intermediate ceiling" +
            "); refusing to open so a stale chunk version can never be reissued");
    }

    if (intermediate_record) {
        // Preserve the known exclusive ceiling exactly while adding format
        // validation. Never restart or lower an intermediate clock.
        AtomicWrite(
            version_clock_path_,
            SerializeVersionClockRecord(persisted_ceiling),
            /*fsync_file=*/true,
            /*fsync_directory=*/true);
        LogMessage(
            LogLevel::kInfo,
            LogComponent::kRecovery,
            "upgraded intermediate version-clock bookkeeping",
            {{"version_ceiling", std::to_string(persisted_ceiling)}});
    }

    // Every token ever issued before this start-up was strictly below the last
    // persisted ceiling. Starting the clock there guarantees new tokens are at
    // or above it, so a stale version can never match after a restart. Token
    // zero is reserved as the "no version" sentinel.
    version_clock_.store(persisted_ceiling, std::memory_order_relaxed);
    version_clock_ceiling_.store(persisted_ceiling, std::memory_order_relaxed);
    if (!initialized_marker_present) {
        // Upgrade a store created before the explicit marker. The valid clock
        // record already proves initialization; persist the marker before this
        // instance can expose another token.
        AtomicWrite(
            initialized_marker_path,
            SerializeInitializedMarker(),
            /*fsync_file=*/true,
            /*fsync_directory=*/true);
    }
}

void ChunkStore::ExtendVersionClockCeilingLocked(std::uint64_t minimum_exclusive) {
    // Called under version_clock_mutex_. Persists a new ceiling above
    // minimum_exclusive before it is used, so a crash can never leave issued
    // tokens above the durable ceiling.
    std::uint64_t new_ceiling = version_clock_ceiling_.load(std::memory_order_relaxed);
    if (new_ceiling <= minimum_exclusive) {
        if (minimum_exclusive >
            std::numeric_limits<std::uint64_t>::max() - kVersionClockReservationBatch) {
            throw std::overflow_error("chunk version clock exhausted");
        }
        new_ceiling = minimum_exclusive + kVersionClockReservationBatch;
    }

    AtomicWrite(
        version_clock_path_,
        SerializeVersionClockRecord(new_ceiling),
        /*fsync_file=*/true,
        /*fsync_directory=*/true);
    version_clock_ceiling_.store(new_ceiling, std::memory_order_release);
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

    while (true) {
        const std::uint64_t candidate = version_clock_.fetch_add(1, std::memory_order_relaxed);
        if (candidate < version_clock_ceiling_.load(std::memory_order_acquire)) {
            return candidate;
        }
        // Reserved range exhausted: persist a higher ceiling before issuing
        // any token at or above it.
        std::lock_guard lock(version_clock_mutex_);
        if (candidate >= version_clock_ceiling_.load(std::memory_order_acquire)) {
            ExtendVersionClockCeilingLocked(candidate);
        }
        return candidate;
    }
}

std::uint64_t ChunkStore::GetChunkVersion(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return regular_chunk->version;
}

}  // namespace chunkdb
