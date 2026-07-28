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

namespace chunkdb {

namespace {

constexpr std::size_t kReadOnlySnapshotMaxAttempts = 8;
constexpr std::string_view kSnapshotGenerationFile = "chunkdb.snapshot";
constexpr std::array<std::uint8_t, 4> kSnapshotGenerationMagic = {
    'C', 'K', 'S', 'G'};
constexpr std::size_t kSnapshotGenerationRecordSize = 16;
thread_local std::vector<ChunkStore*> g_snapshot_write_stack;

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
        throw std::runtime_error(
            "read-only snapshot cannot read artifact " + path.string() +
            ": " + load_error.what());
    }
}

[[nodiscard]] ReadOnlyChunkDiskSnapshot CollectReadOnlyChunkDiskSnapshot(
    const std::filesystem::path& data_path,
    const std::filesystem::path& wal_path,
    const std::filesystem::path& intent_path,
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
    observation(collection, ReadOnlySnapshotArtifact::kIntent);
    return snapshot;
}

[[nodiscard]] std::uint64_t ReadSnapshotGeneration(
    const std::filesystem::path& path) {
    const auto artifact = ReadArtifactForSnapshot(path);
    if (!artifact.present) {
        // Generation zero is the implicit stable epoch for a legacy or empty
        // store. A current writer durably creates an odd record before it
        // changes any snapshot artifact, and never removes the record.
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

[[nodiscard]] ReadOnlyChunkDiskSnapshot LoadStableReadOnlyChunkDiskSnapshot(
    const std::filesystem::path& data_path,
    const std::filesystem::path& wal_path,
    const std::filesystem::path& intent_path,
    const std::filesystem::path& generation_path,
    const ChunkCoord& chunk_coord,
    const std::function<void(
        std::size_t,
        ReadOnlySnapshotArtifact)>& observation) {
    for (std::size_t attempt = 0;
         attempt < kReadOnlySnapshotMaxAttempts;
         ++attempt) {
        const std::uint64_t before =
            ReadSnapshotGeneration(generation_path);
        const auto snapshot = CollectReadOnlyChunkDiskSnapshot(
            data_path, wal_path, intent_path, attempt + 1U, observation);
        const std::uint64_t after =
            ReadSnapshotGeneration(generation_path);
        if ((before & 1U) == 0U && before == after &&
            !ForceReadOnlySnapshotInstabilityForTests(chunk_coord)) {
            return snapshot;
        }
    }

    throw std::runtime_error(
        "read-only chunk snapshot remained unstable after " +
        std::to_string(kReadOnlySnapshotMaxAttempts) +
        " bounded attempts for chunk (" +
        std::to_string(chunk_coord.x) + "," +
        std::to_string(chunk_coord.y) + ")");
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
    snapshot_generation_active_writers_ = 1U;
    snapshot_generation_epoch_failed_ = false;
    CrashAtSnapshotFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_AFTER_BEGIN_ONCE");
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

}  // namespace chunkdb
