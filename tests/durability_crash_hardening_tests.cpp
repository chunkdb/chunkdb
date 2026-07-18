#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace {

std::uint64_t CurrentPid() {
#ifdef _WIN32
    return static_cast<std::uint64_t>(::GetCurrentProcessId());
#else
    return static_cast<std::uint64_t>(::getpid());
#endif
}

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    static std::atomic<std::uint64_t> counter{0};
    const std::uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);
    return base / (
        "chunkdb-crash-hardening-" + suffix + "-" + std::to_string(tick) + "-" +
        std::to_string(CurrentPid()) + "-" + std::to_string(seq));
}

void RemoveAllWithRetry(const std::filesystem::path& path) {
    constexpr int kAttempts = 80;
    constexpr auto kSleep = std::chrono::milliseconds(25);

    for (int attempt = 0; attempt < kAttempts; ++attempt) {
        std::error_code remove_ec;
        std::filesystem::remove_all(path, remove_ec);
        std::error_code exists_ec;
        if (!std::filesystem::exists(path, exists_ec) && !exists_ec) {
            return;
        }
        std::this_thread::sleep_for(kSleep);
    }

    std::error_code remove_ec;
    std::filesystem::remove_all(path, remove_ec);
    std::error_code exists_ec;
    if (std::filesystem::exists(path, exists_ec) || exists_ec) {
        throw std::runtime_error("failed to remove temp dir: " + path.string());
    }
}

chunkdb::StoreConfig BuildConfig(
    const std::filesystem::path& data_dir,
    chunkdb::DurabilityMode mode) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 8,
        },
        .data_dir = data_dir,
        .durability_mode = mode,
        .checkpoint_update_interval = 1,
        .checkpoint_wal_bytes = 128,
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 64,
        .allow_multiple_processes = true,
    };
}

void SetEnvVar(const char* key, const char* value) {
#ifdef _WIN32
    const int rc = _putenv_s(key, value);
    assert(rc == 0);
#else
    const int rc = setenv(key, value, 1);
    assert(rc == 0);
#endif
}

void UnsetEnvVar(const char* key) {
#ifdef _WIN32
    const int rc = _putenv_s(key, "");
    assert(rc == 0);
#else
    const int rc = unsetenv(key);
    assert(rc == 0);
#endif
}

class ScopedEnv {
  public:
    ScopedEnv(const char* key, const char* value) : key_(key) {
        const char* previous = std::getenv(key_);
        if (previous != nullptr) {
            had_previous_ = true;
            previous_ = previous;
        }
        SetEnvVar(key_, value);
    }

    ~ScopedEnv() {
        if (had_previous_) {
            SetEnvVar(key_, previous_.c_str());
        } else {
            UnsetEnvVar(key_);
        }
    }

  private:
    const char* key_;
    bool had_previous_ = false;
    std::string previous_;
};

class ScopedLogCapture {
  public:
    explicit ScopedLogCapture(chunkdb::LogLevel level)
        : previous_level_(chunkdb::GetLogLevel()) {
        chunkdb::SetLogLevel(level);
        chunkdb::SetLogSinkForTests([this](const std::string& line) {
            std::lock_guard lock(mutex_);
            lines_.push_back(line);
        });
    }

    ~ScopedLogCapture() {
        chunkdb::ResetLogSinkForTests();
        chunkdb::SetLogLevel(previous_level_);
    }

    [[nodiscard]] bool Contains(std::string_view needle) const {
        std::lock_guard lock(mutex_);
        for (const auto& line : lines_) {
            if (line.find(needle) != std::string::npos) {
                return true;
            }
        }
        return false;
    }

  private:
    chunkdb::LogLevel previous_level_;
    mutable std::mutex mutex_;
    std::vector<std::string> lines_;
};

std::vector<std::uint8_t> ReadBytes(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in.is_open());

    in.seekg(0, std::ios::end);
    const std::streamoff size = in.tellg();
    assert(size >= 0);
    in.seekg(0, std::ios::beg);

    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(size));
    if (!bytes.empty()) {
        in.read(reinterpret_cast<char*>(bytes.data()), size);
    }
    assert(in.good() || in.eof());
    return bytes;
}

void AppendRaw(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    assert(out.is_open());
    if (!bytes.empty()) {
        out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    }
    out.flush();
    assert(out.good());
}

void WriteLe16(std::vector<std::uint8_t>* out, std::uint16_t v) {
    out->push_back(static_cast<std::uint8_t>(v & 0xFFU));
    out->push_back(static_cast<std::uint8_t>((v >> 8U) & 0xFFU));
}

void WriteLe32(std::vector<std::uint8_t>* out, std::uint32_t v) {
    out->push_back(static_cast<std::uint8_t>(v & 0xFFU));
    out->push_back(static_cast<std::uint8_t>((v >> 8U) & 0xFFU));
    out->push_back(static_cast<std::uint8_t>((v >> 16U) & 0xFFU));
    out->push_back(static_cast<std::uint8_t>((v >> 24U) & 0xFFU));
}

std::size_t CountTmpArtifactsForTarget(const std::filesystem::path& target_path) {
    const auto parent = target_path.parent_path();
    if (!std::filesystem::exists(parent)) {
        return 0;
    }

    const std::string prefix = target_path.filename().string() + ".tmp.";
    std::size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(parent)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto name = entry.path().filename().string();
        if (name.rfind(prefix, 0) == 0) {
            ++count;
        }
    }
    return count;
}

void TestCrashPointAfterTempFlushBeforeRename() {
    const auto data_dir = TempDataDir("failpoint-after-temp-flush");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncCheckpoint);

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(0, 0);
    const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);

    std::vector<std::uint8_t> baseline;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "11110000");
        assert(std::filesystem::exists(data_path));
        baseline = ReadBytes(data_path);

        {
            ScopedEnv fp("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_TEMP_FLUSH_ONCE", "1");
            // The WAL append is the commit point: an inline-checkpoint
            // failure after it is logged and retried, never returned as a
            // command error, so the acknowledgement can never contradict
            // the durable outcome.
            store.SetBlockBits(0, 0, "00001111");
        }

        assert(store.GetBlockBits(0, 0) == "00001111");
        assert(std::filesystem::exists(data_path));
        const auto after_fail = ReadBytes(data_path);
        assert(after_fail == baseline);
        assert(CountTmpArtifactsForTarget(data_path) >= 1);
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "00001111");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "00001111");
    }
    assert(CountTmpArtifactsForTarget(data_path) == 0);

    RemoveAllWithRetry(data_dir);
}

void TestOrphanTempArtifactCleanup() {
    const auto data_dir = TempDataDir("orphan-cleanup");
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kRelaxed);
    config.checkpoint_update_interval = 1;

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(1, 1);
    const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
    const auto orphan_path = std::filesystem::path(data_path.string() + ".tmp.orphan-manual");

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(1, 1, "10101010");
    }

    {
        std::ofstream out(orphan_path, std::ios::binary | std::ios::trunc);
        assert(out.is_open());
        out << "orphan";
    }
    assert(std::filesystem::exists(orphan_path));

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(1, 1) == "10101010");
    }

    assert(!std::filesystem::exists(orphan_path));
    RemoveAllWithRetry(data_dir);
}

void TestFlushFailureInjectionFailsLoudly() {
    const auto data_dir = TempDataDir("sync-fail");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncCheckpoint);

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(0, 0);
    const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "00110011");
        const auto baseline = ReadBytes(data_path);

        {
            ScopedEnv fp("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_SYNC_FAIL_ONCE", "1");
            // Checkpoint temp-sync failure is post-commit (the WAL append
            // succeeded), so the command succeeds and the image is retried.
            store.SetBlockBits(0, 0, "11001100");
        }
        assert(store.GetBlockBits(0, 0) == "11001100");
        assert(ReadBytes(data_path) == baseline);
    }

    RemoveAllWithRetry(data_dir);
}

void TestCrashPointAfterRenameBeforeDirSync() {
    const auto data_dir = TempDataDir("failpoint-after-rename-before-dirsync");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncCheckpoint);

    {
        chunkdb::ChunkStore seed(config);
        seed.SetBlockBits(0, 0, "01010101");
    }

    {
        chunkdb::ChunkStore store(config);
        assert(store.GetBlockBits(0, 0) == "01010101");
        ScopedEnv fp("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
        // Post-replace directory-sync failure is post-commit: the command
        // succeeds and the retained WAL covers the image until retry.
        store.SetBlockBits(0, 0, "10101010");
        assert(store.GetBlockBits(0, 0) == "10101010");
    }

    {
        chunkdb::ChunkStore recovered(config);
        const std::string bits = recovered.GetBlockBits(0, 0);
        // Replace boundary fault must never leave a torn final state.
        // The persisted value must be old-or-new only.
        assert(bits == "01010101" || bits == "10101010");
    }

    RemoveAllWithRetry(data_dir);
}

void TestReplaceBoundaryOldOrNewInvariantRepeated() {
    const auto data_dir = TempDataDir("replace-old-or-new-repeated");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncCheckpoint);
    std::string expected = "00000000";

    {
        chunkdb::ChunkStore seed(config);
        seed.SetBlockBits(0, 0, expected);
    }

    for (int i = 0; i < 8; ++i) {
        const std::string next = (i % 2 == 0) ? "11110000" : "00001111";
        {
            chunkdb::ChunkStore store(config);
            assert(store.GetBlockBits(0, 0) == expected);
            ScopedEnv fp("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
            // Post-replace boundary failures are post-commit successes.
            store.SetBlockBits(0, 0, next);
        }

        {
            chunkdb::ChunkStore recovered(config);
            const std::string persisted = recovered.GetBlockBits(0, 0);
            assert(persisted == expected || persisted == next);
            expected = persisted;
        }
    }

    RemoveAllWithRetry(data_dir);
}

void TestWalFirstCreateAfterFileSyncBeforeDirSync() {
    const auto data_dir = TempDataDir("wal-first-create-after-file-sync-before-dirsync");
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncWal);
    config.checkpoint_update_interval = 1;
    config.checkpoint_wal_bytes = 1'000'000;

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(0, 0);
    const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);

    {
        chunkdb::ChunkStore seed(config);
        seed.SetBlockBits(0, 0, "01010101");
    }

    if (std::filesystem::exists(wal_path)) {
        std::filesystem::remove(wal_path);
    }
    assert(!std::filesystem::exists(wal_path));

    bool threw = false;
    {
        chunkdb::ChunkStore store(config);
        assert(store.GetBlockBits(0, 0) == "01010101");
        ScopedEnv fp("CHUNKDB_FAILPOINT_WAL_AFTER_FILE_SYNC_BEFORE_DIR_SYNC_ONCE", "1");
        try {
            store.SetBlockBits(0, 0, "10101010");
        } catch (const std::exception&) {
            threw = true;
        }
    }
    assert(threw);

    {
        chunkdb::ChunkStore recovered(config);
        const std::string bits = recovered.GetBlockBits(0, 0);
        // On first WAL create boundary fault (file sync complete, dir sync missing),
        // persisted state after restart must remain old-or-new only.
        assert(bits == "01010101" || bits == "10101010");
    }

    RemoveAllWithRetry(data_dir);
}

void TestCheckpointFirstCreateAfterDirectoryCreateBeforeParentSync() {
    const auto data_dir = TempDataDir("checkpoint-dir-create-before-parent-sync");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncCheckpoint);

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(0, 0);
    const chunkdb::LargeChunkCoord large_coord = geometry.ChunkToLarge(coord);
    const auto large_dir = chunkdb::LargeChunkDirectory(data_dir, large_coord);
    const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);

    bool threw = false;
    {
        chunkdb::ChunkStore store(config);
        ScopedEnv fp("CHUNKDB_FAILPOINT_DIR_CREATE_BEFORE_PARENT_SYNC_ONCE", "1");
        try {
            store.SetBlockBits(0, 0, "11110000");
        } catch (const std::exception&) {
            threw = true;
        }
        assert(std::filesystem::exists(large_dir));
        assert(!std::filesystem::exists(data_path));
    }
    assert(threw);

    {
        chunkdb::ChunkStore recovered(config);
        recovered.SetBlockBits(0, 0, "11110000");
        assert(recovered.GetBlockBits(0, 0) == "11110000");
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "11110000");
    }

    RemoveAllWithRetry(data_dir);
}

void TestWalFirstCreateAfterDirectoryCreateBeforeParentSync() {
    const auto data_dir = TempDataDir("wal-dir-create-before-parent-sync");
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncWal);
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(0, 0);
    const chunkdb::LargeChunkCoord large_coord = geometry.ChunkToLarge(coord);
    const auto large_dir = chunkdb::LargeChunkDirectory(data_dir, large_coord);
    const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
    const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);

    bool threw = false;
    {
        chunkdb::ChunkStore store(config);
        ScopedEnv fp("CHUNKDB_FAILPOINT_DIR_CREATE_BEFORE_PARENT_SYNC_ONCE", "1");
        try {
            store.SetBlockBits(0, 0, "00001111");
        } catch (const std::exception&) {
            threw = true;
        }
        assert(std::filesystem::exists(large_dir));
        assert(!std::filesystem::exists(wal_path));
        assert(!std::filesystem::exists(data_path));
    }
    assert(threw);

    {
        chunkdb::ChunkStore recovered(config);
        recovered.SetBlockBits(0, 0, "00001111");
        assert(recovered.GetBlockBits(0, 0) == "00001111");
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "00001111");
    }

    RemoveAllWithRetry(data_dir);
}

// A WAL flush failure is a pre-commit failure: the command must error, the
// in-memory value must roll back, the appended bytes must be neutralized so
// the rejected value can never resurrect after restart, and the store must
// keep serving.
void TestRejectedWriteAfterWalSyncFailureDoesNotResurrect() {
    const auto data_dir = TempDataDir("wal-sync-fail-rollback");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncWal);

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "11110000");

        bool threw = false;
        {
            ScopedEnv fp("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
            try {
                store.SetBlockBits(0, 0, "00001111");
            } catch (const std::exception&) {
                threw = true;
            }
        }
        assert(threw);
        // Memory rolled back and the store still serves reads and writes.
        assert(store.GetBlockBits(0, 0) == "11110000");
        store.SetBlockBits(1, 0, "10101010");
        assert(store.GetBlockBits(1, 0) == "10101010");
    }

    {
        chunkdb::ChunkStore recovered(config);
        // The rejected value must not resurrect from the WAL.
        assert(recovered.GetBlockBits(0, 0) == "11110000");
        assert(recovered.GetBlockBits(1, 0) == "10101010");
    }

    RemoveAllWithRetry(data_dir);
}

// Same rejection contract in relaxed group-commit mode: earlier acknowledged
// batched writes survive, the rejected write does not, in memory or on disk.
void TestRelaxedGroupCommitFlushFailureRollsBackOnlyRejectedWrite() {
    const auto data_dir = TempDataDir("relaxed-group-commit-rollback");
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kRelaxed);
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;
    config.wal_group_commit_updates = 3;

    {
        chunkdb::ChunkStore store(config);
        // One acknowledged write stays in the in-memory batch (3 records
        // needed to trigger the group flush: payload+presence per SET).
        store.SetBlockBits(0, 0, "11110000");

        bool threw = false;
        {
            ScopedEnv fp("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
            try {
                // Second write reaches the group limit and triggers the
                // failing flush.
                store.SetBlockBits(1, 0, "00001111");
            } catch (const std::exception&) {
                threw = true;
            }
        }
        assert(threw);
        assert(store.GetBlockBits(0, 0) == "11110000");
        assert(!store.BlockExists(1, 0));
    }

    {
        chunkdb::ChunkStore recovered(config);
        // Clean shutdown flushed the restored batch: the acknowledged write
        // survives, the rejected one does not.
        assert(recovered.GetBlockBits(0, 0) == "11110000");
        assert(!recovered.BlockExists(1, 0));
    }

    RemoveAllWithRetry(data_dir);
}

// The audit's CDB-DEF-7 reproduction, inverted to the fixed contract: a
// checkpoint failure after the image was atomically replaced is post-commit,
// so the ordinary SET succeeds and the value is durable — the client reply
// and the durable outcome agree.
void TestOrdinaryWriteCheckpointFailureAfterImageReplaceIsCommitted() {
    const auto data_dir = TempDataDir("ordinary-after-image-replace");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncCheckpoint);

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "11110000");
        {
            ScopedEnv fp("CHUNKDB_FAILPOINT_CHECKPOINT_AFTER_IMAGE_REPLACE_ONCE", "1");
            store.SetBlockBits(0, 0, "00001111");
        }
        assert(store.GetBlockBits(0, 0) == "00001111");
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "00001111");
        assert(recovered.BlockExists(0, 0));
    }

    RemoveAllWithRetry(data_dir);
}

// A failure publishing the even snapshot generation happens AFTER the WAL
// flush has committed the records, so it is post-commit: an ordinary SET must
// still succeed (not return -ERR for a durable write), the value must persist,
// and no duplicate records may resurface on restart. Regression test for the
// audit finding that the ordinary path surfaced this post-commit failure as a
// command error and rolled back a durable write.
void TestOrdinaryWriteGenerationPublishFailureIsCommitted() {
    const auto data_dir = TempDataDir("ordinary-generation-publish-fail");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncWal);

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "11110000");
        {
            // Fires inside FinishSnapshotGenerationWriteLocked, i.e. the
            // even-generation publication after the WAL append committed.
            ScopedEnv fp("CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_END_FAIL_ONCE", "1");
            store.SetBlockBits(0, 0, "00001111");
        }
        assert(store.GetBlockBits(0, 0) == "00001111");
    }

    {
        chunkdb::ChunkStore recovered(config);
        // Committed value present, and exactly it — no duplicate/older record
        // resurrected from a rolled-back batch.
        assert(recovered.GetBlockBits(0, 0) == "00001111");
        assert(recovered.BlockExists(0, 0));
    }

    RemoveAllWithRetry(data_dir);
}

// MSET's documented applied-prefix contract: a mid-command failure leaves
// earlier items applied (in memory and, per the mode, later durable) while
// the failing item is fully rolled back.
void TestMSetMidFailureLeavesAppliedPrefixOnly() {
    const auto data_dir = TempDataDir("mset-prefix");
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kRelaxed);
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;
    config.wal_group_commit_updates = 3;

    {
        auto store = std::make_shared<chunkdb::ChunkStore>(config);
        chunkdb::CommandEngine engine(
            chunkdb::EngineConfig{
                .auth_token = "",
                .require_auth = false,
            },
            store);
        chunkdb::SessionState session;

        // The first item stages two records without flushing; the second
        // item reaches the group-commit limit and triggers the failing
        // flush, so it rolls back while the first item stays applied.
        std::string reply;
        {
            ScopedEnv fp("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
            reply = engine.Execute(session, "MSET 0 0 11110000 1 0 00001111\r\n");
        }
        assert(reply.rfind("-ERR", 0) == 0);
        assert(store->GetBlockBits(0, 0) == "11110000");
        assert(!store->BlockExists(1, 0));
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "11110000");
        assert(!recovered.BlockExists(1, 0));
    }

    RemoveAllWithRetry(data_dir);
}

void TestTornWalTailIgnored() {
    const auto data_dir = TempDataDir("torn-wal-tail");
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kRelaxed);
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(0, 0);
    const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "11110000");
        store.SetBlockBits(1, 0, "00001111");
    }

    assert(std::filesystem::exists(wal_path));

    // Append one intentionally corrupted tail record:
    // DLT1 + byte_offset + data_size + bad_crc + one byte data.
    std::vector<std::uint8_t> corrupt;
    corrupt.push_back('D');
    corrupt.push_back('L');
    corrupt.push_back('T');
    corrupt.push_back('1');
    WriteLe32(&corrupt, 0U);
    WriteLe16(&corrupt, 1U);
    WriteLe32(&corrupt, 0xDEADBEEFU);
    corrupt.push_back(0xAAU);
    AppendRaw(wal_path, corrupt);

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "11110000");
        assert(recovered.GetBlockBits(1, 0) == "00001111");
    }

    RemoveAllWithRetry(data_dir);
}

void TestWindowsDirectorySyncCapabilityUnavailableFailsClosed() {
#ifdef _WIN32
    const auto data_dir = TempDataDir("windows-dir-sync-unavailable");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncWal);
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    {
        ScopedEnv fp("CHUNKDB_FAILPOINT_WINDOWS_DIRECTORY_SYNC_CAPABILITY_ERROR_ONCE", "1");
        bool threw = false;
        try {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
        } catch (const std::runtime_error& error) {
            threw = true;
            const std::string message = error.what();
            assert(message.find("strict durability requires Windows directory sync capability") !=
                   std::string::npos);
        }
        assert(threw);
    }

    assert(logs.Contains("ERROR store pid="));
    assert(logs.Contains(
        "strict durability unavailable on Windows; directory sync capability missing"));
    assert(logs.Contains("step=directory_sync"));
    assert(logs.Contains(
        "impact=\"strict durability mode cannot be honored on this filesystem/runtime\""));

    RemoveAllWithRetry(data_dir);
#endif
}

void ApplyConditionalForCrash(
    chunkdb::ChunkStore* store,
    const std::string& kind,
    std::uint64_t expected_version) {
    if (kind == "cas") {
        (void)store->CasChunkState(
            0,
            0,
            expected_version,
            std::string(store->geometry().ChunkPayloadBits(), '1'),
            std::string(store->geometry().ChunkBlockCount(), '1'));
        return;
    }
    assert(kind == "batch");
    const std::vector<chunkdb::ChunkBatchOp> ops = {
        {.set = true, .x = 0, .y = 0, .bits = "01010101"}};
    (void)store->ApplyChunkBatch(
        0, 0, true, expected_version, ops);
}

int RunConditionalCrashChild(
    const std::filesystem::path& data_dir,
    const std::string& kind,
    const char* failpoint) {
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncWal);
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;
    chunkdb::ChunkStore store(config);
    const auto expected = store.GetChunkVersion(0, 0);
    SetEnvVar(failpoint, "1");
    ApplyConditionalForCrash(&store, kind, expected);
    UnsetEnvVar(failpoint);
    return 3;
}

int RunPostRecoveryWriteCrashChild(
    const std::filesystem::path& data_dir) {
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncWal);
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;
    chunkdb::ChunkStore store(config);
    store.SetBlockBits(1, 1, "00110011");
    store.WalBarrier();
    std::_Exit(87);
}

bool HasConditionalIntent(const std::filesystem::path& data_dir) {
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(data_dir)) {
        if (entry.is_regular_file() &&
            entry.path().extension() == ".rollback") {
            return true;
        }
    }
    return false;
}

void RunConditionalCrashBoundaryCase(
    const std::string& executable,
    const std::string& kind,
    const char* failpoint,
    bool expect_committed) {
    const auto data_dir = TempDataDir("conditional-intent-crash");
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncWal);
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;

    {
        chunkdb::ChunkStore seed(config);
        seed.SetBlockBits(0, 0, "10101010");
        seed.WalBarrier();
    }

    const std::string command =
        "\"" + executable + "\" --conditional-crash-child \"" +
        data_dir.string() + "\" " + kind + " " + failpoint;
    const int status = std::system(command.c_str());
    assert(status != 0);

    // TEMP DIAGNOSTIC (Windows): read the raw chunkdb.snapshot file left by the
    // crashed child and report its parity. This runs in the PARENT (stderr is
    // captured), unlike a child-side print. Record layout: magic[4]="CKSG",
    // u64 LE generation, u32 CRC = 16 bytes.
    {
        const auto snap_path = data_dir / "chunkdb.snapshot";
        std::error_code snap_ec;
        if (std::filesystem::exists(snap_path, snap_ec) && !snap_ec) {
            std::ifstream in(snap_path, std::ios::binary);
            std::vector<unsigned char> b((std::istreambuf_iterator<char>(in)),
                                         std::istreambuf_iterator<char>());
            if (b.size() >= 12 && b[0] == 'C' && b[1] == 'K' && b[2] == 'S' && b[3] == 'G') {
                std::uint64_t gen = 0;
                for (int i = 0; i < 8; ++i) {
                    gen |= static_cast<std::uint64_t>(b[4 + i]) << (8 * i);
                }
                std::fprintf(stderr,
                    "=== SNAP-ON-DISK DIAG: kind=%s failpoint=%s status=%d gen=%llu parity=%s ===\n",
                    kind.c_str(), failpoint, status,
                    static_cast<unsigned long long>(gen), (gen & 1ULL) ? "ODD" : "EVEN");
            } else {
                std::fprintf(stderr,
                    "=== SNAP-ON-DISK DIAG: kind=%s failpoint=%s status=%d snapshot present but unparseable (size=%zu) ===\n",
                    kind.c_str(), failpoint, status, b.size());
            }
        } else {
            std::fprintf(stderr,
                "=== SNAP-ON-DISK DIAG: kind=%s failpoint=%s status=%d NO snapshot file ===\n",
                kind.c_str(), failpoint, status);
        }
        std::fflush(stderr);
    }

    {
        auto read_only_config = config;
        read_only_config.access_mode = chunkdb::AccessMode::kReadOnly;
        bool failed_closed = false;
        std::string diag_what = "<no throw>";
        std::string diag_value = "<none>";
        try {
            chunkdb::ChunkStore reader(read_only_config);
            diag_value = reader.GetBlockBits(0, 0);
        } catch (const std::exception& error) {
            diag_what = error.what();
            failed_closed =
                std::string(error.what()).find(
                    "remained unstable") !=
                    std::string::npos ||
                std::string(error.what()).find(
                    "snapshot generation") !=
                    std::string::npos;
        }
        if (!failed_closed) {
            std::fprintf(stderr,
                "\n=== FAILED_CLOSED DIAG: kind=%s failpoint=%s what='%s' value='%s' ===\n",
                kind.c_str(), failpoint, diag_what.c_str(), diag_value.c_str());
            std::fflush(stderr);
        }
        assert(failed_closed);
    }

    const std::string expected =
        expect_committed
            ? (kind == "cas" ? "11111111" : "01010101")
            : "10101010";
    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == expected);
        assert(!HasConditionalIntent(data_dir));
    }

    const std::string post_recovery_command =
        "\"" + executable + "\" --post-recovery-write-crash-child \"" +
        data_dir.string() + "\"";
    const int post_recovery_status =
        std::system(post_recovery_command.c_str());
    assert(post_recovery_status != 0);

    {
        chunkdb::ChunkStore restarted(config);
        assert(restarted.GetBlockBits(0, 0) == expected);
        assert(restarted.GetBlockBits(1, 1) == "00110011");
        assert(!HasConditionalIntent(data_dir));
    }

    RemoveAllWithRetry(data_dir);
}

void TestConditionalIntentCrashBoundaries(const std::string& executable) {
    struct Boundary {
        const char* failpoint;
        bool committed;
    };
    const std::vector<Boundary> boundaries = {
        {"CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_AFTER_BEGIN_ONCE", false},
        {"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_INTENT_PUBLISH_ONCE", false},
        {"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_AFTER_INTENT_PUBLISH_ONCE", false},
        {"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_COMMIT_PUBLISH_ONCE", false},
        {"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_AFTER_COMMIT_PUBLISH_ONCE", true},
        {"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_INTENT_CLEAR_ONCE", true},
        {"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_AFTER_INTENT_UNLINK_ONCE", true},
        {"CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_BEFORE_END_ONCE", true},
    };
    for (const auto& kind : {"cas", "batch"}) {
        for (const auto& boundary : boundaries) {
            RunConditionalCrashBoundaryCase(
                executable, kind, boundary.failpoint, boundary.committed);
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 5 && std::string(argv[1]) == "--conditional-crash-child") {
        return RunConditionalCrashChild(argv[2], argv[3], argv[4]);
    }
    if (argc == 3 && std::string(argv[1]) == "--post-recovery-write-crash-child") {
        return RunPostRecoveryWriteCrashChild(argv[2]);
    }
    TestCrashPointAfterTempFlushBeforeRename();
    TestCrashPointAfterRenameBeforeDirSync();
    TestReplaceBoundaryOldOrNewInvariantRepeated();
    TestCheckpointFirstCreateAfterDirectoryCreateBeforeParentSync();
    TestWalFirstCreateAfterFileSyncBeforeDirSync();
    TestWalFirstCreateAfterDirectoryCreateBeforeParentSync();
    TestOrphanTempArtifactCleanup();
    TestFlushFailureInjectionFailsLoudly();
    TestRejectedWriteAfterWalSyncFailureDoesNotResurrect();
    TestRelaxedGroupCommitFlushFailureRollsBackOnlyRejectedWrite();
    TestOrdinaryWriteCheckpointFailureAfterImageReplaceIsCommitted();
    TestOrdinaryWriteGenerationPublishFailureIsCommitted();
    TestMSetMidFailureLeavesAppliedPrefixOnly();
    TestTornWalTailIgnored();
    TestWindowsDirectorySyncCapabilityUnavailableFailsClosed();
    TestConditionalIntentCrashBoundaries(argv[0]);
    return 0;
}
