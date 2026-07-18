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
constexpr std::array<std::uint8_t, 4> kRollbackIntentMagic = {'C', 'K', 'R', 'B'};
constexpr std::array<std::uint8_t, 4> kCommittedIntentMagic = {'C', 'K', 'R', 'C'};
constexpr std::size_t kRollbackIntentRecordSize = 16;
constexpr std::string_view kRollbackIntentSuffix = ".rollback";

[[nodiscard]] std::vector<std::uint8_t> SerializeConditionalIntent(
    ConditionalIntentState state,
    std::uint64_t committed_wal_size) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kRollbackIntentRecordSize);
    const auto& magic =
        state == ConditionalIntentState::kRollback
            ? kRollbackIntentMagic
            : kCommittedIntentMagic;
    bytes.insert(bytes.end(), magic.begin(), magic.end());
    WriteLe64(bytes, committed_wal_size);
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    return bytes;
}

[[nodiscard]] bool TryParseIntent(
    const std::vector<std::uint8_t>& bytes,
    ConditionalIntentState* out_state,
    std::uint64_t* out_committed_wal_size) {
    if (bytes.size() != kRollbackIntentRecordSize ||
        ReadLe32(bytes, 12U) != Crc32(bytes.data(), 12U)) {
        return false;
    }
    if (std::equal(kRollbackIntentMagic.begin(), kRollbackIntentMagic.end(), bytes.begin())) {
        *out_state = ConditionalIntentState::kRollback;
    } else if (
        std::equal(kCommittedIntentMagic.begin(), kCommittedIntentMagic.end(), bytes.begin())) {
        *out_state = ConditionalIntentState::kCommitted;
    } else {
        return false;
    }
    *out_committed_wal_size = ReadLe64(bytes, 4U);
    return true;
}

}  // namespace

std::filesystem::path ConditionalIntentDirectory(
    const std::filesystem::path& data_dir) {
    return data_dir / std::string(kConditionalIntentDirName);
}

std::filesystem::path ConditionalIntentPathForWal(
    const std::filesystem::path& data_dir,
    const std::filesystem::path& wal_path) {
    auto relative = wal_path.lexically_relative(data_dir).generic_string();
    if (relative.empty() || relative.rfind("..", 0) == 0) {
        // A WAL outside the data directory cannot occur through the layout
        // helpers; fail loudly rather than fabricate an ambiguous name.
        throw std::invalid_argument(
            "WAL path is not inside the data directory: " + wal_path.string());
    }
    std::string flat;
    flat.reserve(relative.size());
    for (const char c : relative) {
        if (c == '/') {
            flat += "__";
        } else {
            flat += c;
        }
    }
    return ConditionalIntentDirectory(data_dir) /
           (flat + std::string(kRollbackIntentSuffix));
}

std::filesystem::path WalPathForConditionalIntent(
    const std::filesystem::path& data_dir,
    const std::filesystem::path& intent_path) {
    const std::string name = intent_path.filename().string();
    if (name.size() <= kRollbackIntentSuffix.size() ||
        name.substr(name.size() - kRollbackIntentSuffix.size()) !=
            kRollbackIntentSuffix) {
        throw std::invalid_argument(
            "not a conditional intent file name: " + intent_path.string());
    }
    const std::string flat =
        name.substr(0, name.size() - kRollbackIntentSuffix.size());
    std::filesystem::path wal_path = data_dir;
    std::size_t start = 0;
    while (true) {
        const std::size_t separator = flat.find("__", start);
        if (separator == std::string::npos) {
            wal_path /= flat.substr(start);
            break;
        }
        wal_path /= flat.substr(start, separator - start);
        start = separator + 2U;
    }
    return wal_path;
}

namespace {

void CrashAtFailpoint(const char* key) {
    if (ConsumeFailpointEnv(key)) {
        std::_Exit(86);
    }
}

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

bool TryParseConditionalRollbackIntent(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* out_committed_wal_size) {
    ConditionalIntentState state = ConditionalIntentState::kRollback;
    return TryParseIntent(bytes, &state, out_committed_wal_size) &&
           state == ConditionalIntentState::kRollback;
}

bool TryParseConditionalIntent(
    const std::vector<std::uint8_t>& bytes,
    ConditionalIntentState* out_state,
    std::uint64_t* out_committed_wal_size) {
    return TryParseIntent(bytes, out_state, out_committed_wal_size);
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

void ChunkStore::RecoverConditionalRollbackIntents() {
    if (access_mode_ == AccessMode::kReadOnly) {
        return;
    }

    // Intents live in one dedicated shallow directory, so recovery cost is
    // proportional to the number of pending intents, not to world size.
    const auto intent_dir = ConditionalIntentDirectory(data_dir_);
    std::error_code dir_exists_ec;
    const bool intent_dir_present =
        std::filesystem::exists(intent_dir, dir_exists_ec);
    if (dir_exists_ec) {
        throw std::runtime_error(
            "failed to inspect conditional intent directory " +
            intent_dir.string() + ": " + dir_exists_ec.message());
    }
    if (!intent_dir_present) {
        return;
    }

    std::error_code iterator_ec;
    std::filesystem::directory_iterator iterator(
        intent_dir,
        std::filesystem::directory_options::none,
        iterator_ec);
    if (iterator_ec) {
        throw std::runtime_error(
            "failed to inspect conditional rollback intents under " +
            intent_dir.string() + ": " + iterator_ec.message());
    }

    const std::filesystem::directory_iterator end;
    while (iterator != end) {
        std::error_code type_ec;
        const bool regular = iterator->is_regular_file(type_ec);
        if (type_ec) {
            throw std::runtime_error(
                "failed to inspect conditional rollback artifact " +
                iterator->path().string() + ": " + type_ec.message());
        }
        const auto intent_path = iterator->path();
        iterator.increment(iterator_ec);
        if (iterator_ec) {
            throw std::runtime_error(
                "failed while scanning conditional rollback intents under " +
                intent_dir.string() + ": " + iterator_ec.message());
        }
        if (!regular ||
            intent_path.string().size() <= kRollbackIntentSuffix.size() ||
            intent_path.string().substr(
                intent_path.string().size() - kRollbackIntentSuffix.size()) !=
                kRollbackIntentSuffix) {
            continue;
        }

        std::uint64_t committed_size = 0;
        ConditionalIntentState intent_state = ConditionalIntentState::kRollback;
        const auto bytes = LoadFile(intent_path);
        if (!TryParseIntent(bytes, &intent_state, &committed_size)) {
            throw std::runtime_error(
                "invalid conditional rollback intent: " + intent_path.string());
        }
        const std::filesystem::path wal_path =
            WalPathForConditionalIntent(data_dir_, intent_path);

        if (intent_state == ConditionalIntentState::kRollback) {
            std::error_code exists_ec;
            const bool wal_present = std::filesystem::exists(wal_path, exists_ec);
            if (exists_ec) {
                throw std::runtime_error(
                    "failed to inspect WAL while recovering rollback intent " +
                    intent_path.string() + ": " + exists_ec.message());
            }
            if (wal_present) {
                std::error_code size_ec;
                const auto actual_size = std::filesystem::file_size(wal_path, size_ec);
                if (size_ec) {
                    throw std::runtime_error(
                        "failed to inspect WAL size while recovering rollback intent " +
                        intent_path.string() + ": " + size_ec.message());
                }
                if (actual_size < committed_size) {
                    throw std::runtime_error(
                        "WAL is shorter than the committed rollback boundary for " +
                        intent_path.string());
                }
                if (committed_size == 0U) {
                    std::error_code remove_ec;
                    std::filesystem::remove(wal_path, remove_ec);
                    if (remove_ec) {
                        throw std::runtime_error(
                            "failed to remove rejected WAL while recovering " +
                            intent_path.string() + ": " + remove_ec.message());
                    }
                    SyncDirectoryPath(wal_path.parent_path());
                } else {
                    std::error_code resize_ec;
                    std::filesystem::resize_file(wal_path, committed_size, resize_ec);
                    if (resize_ec) {
                        throw std::runtime_error(
                            "failed to truncate rejected WAL while recovering " +
                            intent_path.string() + ": " + resize_ec.message());
                    }
                    SyncFilePath(wal_path);
                }
            } else if (committed_size != 0U) {
                throw std::runtime_error(
                    "WAL required by conditional rollback intent is missing: " +
                    intent_path.string());
            }
        }

        std::error_code remove_intent_ec;
        std::filesystem::remove(intent_path, remove_intent_ec);
        if (remove_intent_ec) {
            throw std::runtime_error(
                "failed to clear recovered conditional rollback intent " +
                intent_path.string() + ": " + remove_intent_ec.message());
        }
        SyncDirectoryPath(intent_path.parent_path());
    }
}

std::filesystem::path ChunkStore::WriteConditionalRollbackIntent(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    std::uint64_t committed_size) {
    const auto wal_path =
        chunk->wal_path.empty()
            ? LayoutWalPath(data_dir_, geometry_, chunk_coord, storage_layout_mode_)
            : chunk->wal_path;
    const auto intent_path = ConditionalIntentPathForWal(data_dir_, wal_path);
    // Intent publication is durable in every mode, so its directory must be
    // durably present as well.
    EnsureDirectoryPathExists(intent_path.parent_path(), /*durable_sync=*/true);

    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ROLLBACK_INTENT_INSPECT_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected conditional intent inspection failure: " +
            intent_path.string());
    }
    std::error_code exists_ec;
    const bool existing = std::filesystem::exists(intent_path, exists_ec);
    if (exists_ec) {
        throw std::runtime_error(
            "failed to inspect conditional intent " + intent_path.string() +
            ": " + exists_ec.message());
    }
    if (existing) {
        ConditionalIntentState existing_state = ConditionalIntentState::kRollback;
        std::uint64_t existing_boundary = 0;
        if (!TryParseIntent(
                LoadFile(intent_path), &existing_state, &existing_boundary)) {
            PoisonDurability(
                "invalid pre-existing conditional intent " +
                intent_path.string());
            throw std::runtime_error(
                "invalid pre-existing conditional intent: " +
                intent_path.string());
        }
        (void)existing_boundary;
        if (existing_state == ConditionalIntentState::kRollback) {
            PoisonDurability(
                "unresolved rollback intent appeared while the store was live: " +
                intent_path.string());
            throw std::runtime_error(
                "unresolved rollback intent appeared while the store was live: " +
                intent_path.string());
        }
        // A committed marker retained after a prior unlink failure is safe,
        // but it must be durably removed before publishing a new rollback
        // boundary for this chunk.
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ROLLBACK_INTENT_REPLACE_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected failure replacing pre-existing committed intent: " +
                intent_path.string());
        }
        ClearCommittedConditionalIntent(intent_path);
    }

    PauseConditionalMutationForTests(
        ConditionalMutationPausePoint::kBeforeRollbackIntentPublish);
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CONDITIONAL_BEFORE_INTENT_PUBLISH_ONCE")) {
        throw std::runtime_error(
            "injected conditional failure before rollback-intent publication");
    }
    CrashAtFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_INTENT_PUBLISH_ONCE");
    bool published = false;
    try {
        AtomicWrite(
            intent_path,
            SerializeConditionalIntent(
                ConditionalIntentState::kRollback, committed_size),
            /*fsync_file=*/true,
            /*fsync_directory=*/true,
            &published);
    } catch (...) {
        if (published) {
            // The WAL has not been touched. The visible but not yet
            // directory-synced intent must be removed durably before another
            // operation can append to this WAL.
            try {
                std::error_code remove_ec;
                const bool removed =
                    std::filesystem::remove(intent_path, remove_ec);
                if (remove_ec || !removed) {
                    throw std::runtime_error(
                        "failed to remove ambiguously published rollback intent");
                }
                SyncDirectoryPath(intent_path.parent_path());
            } catch (const std::exception& cleanup_error) {
                PoisonDurability(
                    "rollback-intent publication failed and cleanup was not "
                    "durable: " +
                    std::string(cleanup_error.what()));
            }
        }
        throw;
    }
    PauseConditionalMutationForTests(
        ConditionalMutationPausePoint::kAfterRollbackIntentPublish);
    CrashAtFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_AFTER_INTENT_PUBLISH_ONCE");
    return intent_path;
}

bool ChunkStore::PublishConditionalCommitIntent(
    const std::filesystem::path& intent_path,
    std::uint64_t committed_size) {
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CONDITIONAL_BEFORE_COMMIT_PUBLISH_ONCE")) {
        throw std::runtime_error(
            "injected conditional failure before commit-intent publication");
    }
    CrashAtFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_COMMIT_PUBLISH_ONCE");
    bool replaced = false;
    try {
        AtomicWrite(
            intent_path,
            SerializeConditionalIntent(
                ConditionalIntentState::kCommitted, committed_size),
            /*fsync_file=*/true,
            /*fsync_directory=*/true,
            &replaced,
            "CHUNKDB_FAILPOINT_COMMIT_INTENT_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
    } catch (...) {
        if (!replaced) {
            throw;
        }
        // The committed state is visible. Complete the known missing
        // directory durability step without repeating the replace.
        try {
            if (ConsumeFailpointEnv(
                    "CHUNKDB_FAILPOINT_COMMIT_INTENT_COMPLETION_SYNC_FAIL_ONCE")) {
                throw std::runtime_error(
                    "injected commit-intent completion sync failure");
            }
            SyncDirectoryPath(intent_path.parent_path());
        } catch (const std::exception& sync_error) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kRecovery,
                "commit intent is visible but requires a later directory sync",
                {
                    {"path", intent_path.string()},
                    {"error", sync_error.what()},
                });
            return false;
        }
    }
    PauseConditionalMutationForTests(
        ConditionalMutationPausePoint::kAfterCommitIntentPublish);
    CrashAtFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_AFTER_COMMIT_PUBLISH_ONCE");
    return true;
}

void ChunkStore::ClearCommittedConditionalIntent(
    const std::filesystem::path& intent_path) {
    CrashAtFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_INTENT_CLEAR_ONCE");
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CONDITIONAL_INTENT_UNLINK_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected conditional commit-intent unlink failure: " +
            intent_path.string());
    }
    std::error_code remove_ec;
    const bool removed = std::filesystem::remove(intent_path, remove_ec);
    if (remove_ec) {
        throw std::runtime_error(
            "failed to clear conditional rollback intent " +
            intent_path.string() + ": " + remove_ec.message());
    }
    if (!removed) {
        throw std::runtime_error(
            "conditional committed intent disappeared before cleanup: " +
            intent_path.string());
    }
    PauseConditionalMutationForTests(
        ConditionalMutationPausePoint::kAfterCommitIntentUnlink);
    if (ConsumeFailpointEnv(
            "CHUNKDB_FAILPOINT_CONDITIONAL_INTENT_AFTER_UNLINK_BEFORE_DIR_SYNC_ONCE")) {
        throw std::runtime_error(
            "injected conditional intent cleanup failure after unlink before "
            "directory sync: " +
            intent_path.string());
    }
    CrashAtFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_AFTER_INTENT_UNLINK_ONCE");
    SyncDirectoryPath(intent_path.parent_path());
}

void ChunkStore::PoisonDurability(std::string reason) noexcept {
    try {
        std::lock_guard lock(poison_mutex_);
        if (poison_reason_.empty()) {
            poison_reason_ = std::move(reason);
        }
    } catch (...) {
        // Setting the reason string is best-effort; the atomic flag below is
        // the authoritative fail-closed signal.
    }
    durability_poisoned_.store(true, std::memory_order_release);
}

void ChunkStore::ThrowIfDurabilityPoisoned() const {
    if (!durability_poisoned_.load(std::memory_order_acquire)) {
        return;
    }
    std::string reason;
    {
        std::lock_guard lock(poison_mutex_);
        reason = poison_reason_;
    }
    throw std::runtime_error(
        "store is fail-closed after an unrecoverable durability error: " +
        (reason.empty() ? std::string("durability rollback could not be completed")
                        : reason) +
        "; restart the store to run crash recovery");
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

bool ChunkStore::ApplyFullChunkStateLocked(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    std::vector<std::uint8_t> new_payload,
    std::vector<std::uint8_t> new_presence) {
    if (new_payload == chunk->payload && new_presence == chunk->presence_bitmap) {
        return false;
    }

    // The whole canonical state is logged as one span starting at offset
    // zero, which makes the mutation atomic across crash recovery: replay
    // applies the single record completely or not at all. Geometries whose
    // state cannot fit in one record are rejected before any mutation so we
    // never accept prefix-replay behavior while claiming atomicity.
    const auto state = BuildChunkStateBytes(geometry_, new_payload, new_presence);
    if (state.size() > kMaxAtomicChunkStateBytes) {
        throw std::invalid_argument(
            "chunk state (" + std::to_string(state.size()) +
            " bytes) exceeds the single-record atomicity limit of " +
            std::to_string(kMaxAtomicChunkStateBytes) +
            " bytes; conditional mutations are not supported for this geometry");
    }

    // Reserve the version token before anything about this mutation can become
    // visible. Reserving (and, if needed, persisting a higher ceiling) up front
    // means a version-allocation failure happens before the WAL append, so a
    // conditional mutation can never be committed on disk and then fail while
    // trying to obtain a token. A reserved-but-unused token is simply skipped,
    // which is harmless: the clock only has to stay monotonic.
    const std::uint64_t reserved_version = NextChunkVersion();

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
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_INTENT_PUBLISH_ONCE")) {
            throw std::runtime_error(
                "injected conditional failure after rollback-intent publication");
        }
        std::size_t appended_bytes = 0;
        std::size_t appended_record_count = 0;
        AppendWalDeltaSpanToBatch(
            &chunk->wal_batch,
            0U,
            state.data(),
            state.size(),
            &appended_bytes,
            &appended_record_count);

        chunk->pending_wal_flush_updates += appended_record_count;
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
        }
        snapshot_write.Finish();
        throw;
    }

    chunk->version = reserved_version;
    try {
        ClearCommittedConditionalIntent(rollback_intent_path);
    } catch (const std::exception& cleanup_error) {
        if (!commit_record_durable) {
            // The CKRC replace was visible but its first directory sync did
            // not complete. A successful sync now makes either the retained
            // CKRC or its already-visible unlink durable; both recover as
            // committed. Never enter the rollback path after CKRC is visible.
            try {
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
                throw;
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

    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    if (regular_chunk->version != expected_version) {
        return ChunkMutationResult{.ok = false, .version = regular_chunk->version};
    }

    auto new_payload = EmptyPayload();
    BitCodec::WriteBits(new_payload, 0, payload_bits);
    auto new_presence = EmptyPresenceBitmap();
    BitCodec::WriteBits(new_presence, 0, presence_bits);
    CanonicalizeAbsentBlocks(geometry_, new_presence, &new_payload);

    (void)ApplyFullChunkStateLocked(
        chunk_coord,
        regular_chunk,
        std::move(new_payload),
        std::move(new_presence));
    return ChunkMutationResult{.ok = true, .version = regular_chunk->version};
}

ChunkMutationResult ChunkStore::ApplyChunkBatch(
    std::int64_t chunk_x,
    std::int64_t chunk_y,
    bool has_expected_version,
    std::uint64_t expected_version,
    const std::vector<ChunkBatchOp>& ops) {
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

    (void)ApplyFullChunkStateLocked(
        chunk_coord,
        regular_chunk,
        std::move(new_payload),
        std::move(new_presence));
    return ChunkMutationResult{.ok = true, .version = regular_chunk->version};
}

}  // namespace chunkdb
