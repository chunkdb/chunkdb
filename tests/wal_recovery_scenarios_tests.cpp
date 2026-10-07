#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "wal_replay.hpp"

namespace {

using chunkdb::kWalHeaderSize;

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-wal-recovery-" + suffix + "-" + std::to_string(tick));
}

chunkdb::StoreConfig BuildConfig(const std::filesystem::path& data_dir) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 8,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 1'000'000,
        .checkpoint_wal_bytes = 1'000'000,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
    };
}

void AppendBytes(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    assert(out.is_open());
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.flush();
}

void WriteBytes(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    assert(out.is_open());
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.flush();
}

std::string ReadBytes(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in.is_open());

    in.seekg(0, std::ios::end);
    const std::streamoff size = in.tellg();
    assert(size >= 0);

    in.seekg(0, std::ios::beg);
    std::string out(static_cast<std::size_t>(size), '\0');
    if (!out.empty()) {
        in.read(out.data(), static_cast<std::streamsize>(out.size()));
    }
    return out;
}

// ---- v4 frame builders -----------------------------------------------------
//
// The production writer only ever emits well-formed frames, so the format-v2
// replay guards (frame header CRC, record CRC over byte_offset || data_size ||
// body, frame trailer CRC, torn-frame handling, and the legacy record stream)
// are exercised against hand-built byte streams.

void SetEnvVar(const char* key, const char* value) {
#ifdef _WIN32
    const int rc = _putenv_s(key, value);
#else
    const int rc = setenv(key, value, 1);
#endif
    assert(rc == 0);
    (void)rc;
}

void UnsetEnvVar(const char* key) {
#ifdef _WIN32
    const int rc = _putenv_s(key, "");
#else
    const int rc = unsetenv(key);
#endif
    assert(rc == 0);
    (void)rc;
}

// The files of a data directory as a crash would leave them, without the
// writer lock (its heartbeat files change while the store is open).
void CopyCrashImage(const std::filesystem::path& from, const std::filesystem::path& to) {
    std::filesystem::create_directories(to);
    for (const auto& entry : std::filesystem::directory_iterator(from)) {
        if (entry.path().filename() == ".chunkdb.lock") {
            continue;
        }
        std::filesystem::copy(
            entry.path(), to / entry.path().filename(), std::filesystem::copy_options::recursive);
    }
}

// Relaxed group commit: a checkpoint writes its image from memory, which
// includes frames still in the batch, while the WAL file holds only the
// flushed ones. A crash between publishing the image and removing the WAL
// must recover the image, not the image with the older WAL frames replayed
// over it (a state that never existed, at an old revision).
void TestCrashAfterRelaxedCheckpointPublish() {
    const auto live = TempDataDir("ckpt-publish-live");
    const auto crashed = TempDataDir("ckpt-publish-crashed");
    auto config = BuildConfig(live);
    config.checkpoint_update_interval = 2;
    config.wal_group_commit_updates = 100;
    std::string a_live;
    std::string b_live;
    std::uint64_t version_live = 0;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "00000001");  // A=1, flushed to the WAL file
        store.WalBarrier();
        store.SetBlockBits(0, 0, "00000010");  // A=2, batch only
        store.ArmCheckpointBeforeWalRemovalPauseForTests();
        std::thread writer([&] { store.SetBlockBits(0, 2, "00000001"); });  // B=1, checkpoints
        assert(store.WaitForCheckpointBeforeWalRemovalForTests());
        CopyCrashImage(live, crashed);
        store.ResumeCheckpointBeforeWalRemovalForTests();
        writer.join();
        a_live = store.GetBlockBits(0, 0);
        b_live = store.GetBlockBits(0, 2);
        version_live = store.GetChunkVersion(0, 0);
    }
    assert(a_live == "00000010" && b_live == "00000001");
    for (int reopen = 0; reopen < 2; ++reopen) {
        auto recovered_config = config;
        recovered_config.data_dir = crashed;
        chunkdb::ChunkStore recovered(recovered_config);
        assert(recovered.GetBlockBits(0, 0) == a_live);
        assert(recovered.GetBlockBits(0, 2) == b_live);
        if (reopen == 0) {
            assert(recovered.GetChunkVersion(0, 0) == version_live);
        } else {
            assert(recovered.GetChunkVersion(0, 0) > version_live);
        }
        // Writes after the recovery replay after the stale frames.
        recovered.SetBlockBits(0, 1, "00000011");
        assert(recovered.GetBlockBits(0, 1) == "00000011");
        if (reopen == 0) {
            recovered.WalBarrier();
        }
    }
    {
        auto recovered_config = config;
        recovered_config.data_dir = crashed;
        chunkdb::ChunkStore recovered(recovered_config);
        assert(recovered.GetBlockBits(0, 0) == a_live && recovered.GetBlockBits(0, 1) == "00000011");
    }
    std::filesystem::remove_all(live);
    std::filesystem::remove_all(crashed);
}

// Empty-chunk collection removes the image before the WAL. A crash between
// the two replays the WAL over no image, which ends empty only when the WAL
// holds every frame, including those still in the batch.
void TestCrashDuringEmptyChunkCollection() {
    const auto live = TempDataDir("gc-live");
    const auto crashed = TempDataDir("gc-crashed");
    auto config = BuildConfig(live);
    config.checkpoint_update_interval = 2;
    config.wal_group_commit_updates = 100;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "00000001");
        store.SetBlockBits(1, 0, "00000001");
        store.SetBlockBits(2, 0, "00000001");  // checkpoints: image holds all three
        store.UnsetBlock(2, 0);
        store.WalBarrier();
        store.UnsetBlock(1, 0);  // batch only
        store.ArmCheckpointBeforeWalRemovalPauseForTests();
        std::thread writer([&] { store.UnsetBlock(0, 0); });  // empties the chunk: collection
        assert(store.WaitForCheckpointBeforeWalRemovalForTests());
        CopyCrashImage(live, crashed);
        store.ResumeCheckpointBeforeWalRemovalForTests();
        writer.join();
        assert(!store.ChunkExists(0, 0));
    }
    {
        auto recovered_config = config;
        recovered_config.data_dir = crashed;
        chunkdb::ChunkStore recovered(recovered_config);
        assert(!recovered.ChunkExists(0, 0));
        assert(!recovered.BlockExists(0, 0) && !recovered.BlockExists(1, 0) && !recovered.BlockExists(2, 0));
    }
    // Closed first: Windows cannot remove the files of an open store.
    std::filesystem::remove_all(live);
    std::filesystem::remove_all(crashed);
}

// After a rejected conditional write whose WAL repair failed, the store is
// poisoned and a rollback intent still needs the WAL at the next start.
// Eviction and checkpoints must leave that WAL alone, so the next start
// repairs it and opens.
void TestPoisonedStoreKeepsItsWal() {
    const auto dir = TempDataDir("poison");
    auto config = BuildConfig(dir);
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "00000001");
    }
    {
        auto tight = config;
        tight.checkpoint_wal_bytes = 64;  // the loaded chunk is due for a checkpoint
        tight.max_loaded_chunks = 1;
        chunkdb::ChunkStore store(tight);
        assert(store.GetBlockBits(0, 0) == "00000001");
        const auto version = store.GetChunkVersion(0, 0);
        const std::vector<std::uint8_t> payload(16, 0x07);
        const std::vector<std::uint8_t> presence(2, 0xFF);
        SetEnvVar("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE", "1");
        SetEnvVar("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1");
        bool failed = false;
        try {
            (void)store.CasChunkStateBytes(0, 0, version, payload, presence);
        } catch (const std::exception&) {
            failed = true;
        }
        UnsetEnvVar("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE");
        UnsetEnvVar("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE");
        assert(failed);
        bool refused = false;
        try {
            store.SetBlockBits(0, 0, "00000011");
        } catch (const std::exception&) {
            refused = true;
        }
        assert(refused);
        // A checkpoint is refused while the chunk is still cached ...
        bool checkpoint_refused = false;
        try {
            store.CheckpointForTests(0, 0);
        } catch (const std::exception& e) {
            checkpoint_refused = std::string(e.what()).find("fail-closed") != std::string::npos;
        }
        assert(checkpoint_refused);
        // ... and another chunk does not push (0,0) out of the cache: its
        // WAL holds the rejected frame, so memory is the only right copy.
        // Reads keep serving the state before the rejected write.
        (void)store.GetBlockBits(100, 100);
        assert(store.IsChunkLoadedForTests(0, 0));
        assert(store.GetBlockBits(0, 0) == "00000001");
        const auto range = store.ReadChunkRange(0, 0, 0, 0);
        assert(range.size() == 1U && (range[0].presence_bitmap == std::vector<std::uint8_t>{0x01, 0x00}));
        assert(std::filesystem::exists(chunkdb::ChunkWalPath(dir, chunkdb::Geometry(config.geometry), {0, 0})));
        assert(!std::filesystem::exists(chunkdb::ChunkDataPath(dir, chunkdb::Geometry(config.geometry), {0, 0})));
    }
    {
        chunkdb::ChunkStore reopened(config);
        assert(reopened.GetBlockBits(0, 0) == "00000001");
    }
    std::filesystem::remove_all(dir);
}

// A checkpoint that fails after removing the WAL (here at its directory
// sync) leaves no WAL behind. The next write must start a new WAL with its
// header, which later writes and a rolled-back conditional write extend.
void TestCheckpointFailureAfterWalRemoval() {
    const auto dir = TempDataDir("ckpt-after-wal-remove");
    auto config = BuildConfig(dir);
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "00000001");
        store.SetBlockBits(1, 0, "00000001");
        SetEnvVar("CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_WAL_REMOVE_ONCE", "1");
        bool failed = false;
        try {
            store.CheckpointForTests(0, 0);
        } catch (const std::exception&) {
            failed = true;
        }
        UnsetEnvVar("CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_WAL_REMOVE_ONCE");
        assert(failed);
        store.SetBlockBits(2, 0, "00000011");
        const std::vector<std::uint8_t> payload(16, 0x07);
        const std::vector<std::uint8_t> presence(2, 0xFF);
        SetEnvVar("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE", "1");
        bool rejected = false;
        try {
            (void)store.CasChunkStateBytes(0, 0, store.GetChunkVersion(0, 0), payload, presence);
        } catch (const std::exception&) {
            rejected = true;
        }
        UnsetEnvVar("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE");
        assert(rejected);
        store.SetBlockBits(3, 0, "00000111");
    }
    {
        chunkdb::ChunkStore reopened(config);
        assert(reopened.GetBlockBits(0, 0) == "00000001");
        assert(reopened.GetBlockBits(1, 0) == "00000001");
        assert(reopened.GetBlockBits(2, 0) == "00000011");
        assert(reopened.GetBlockBits(3, 0) == "00000111");
        assert(!reopened.BlockExists(0, 1));
    }
    std::filesystem::remove_all(dir);
}

// A crash can tear the last frame of a WAL. A read-only store reads the
// chunk up to the last whole frame, as a writer's load does, instead of
// failing until a writer trims the tail. Damage followed by a valid frame
// still fails the load.
void TestReadOnlyLoadOfCrashTornTail() {
    const auto dir = TempDataDir("ro-torn-tail");
    const auto config = BuildConfig(dir);
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "11110000");
        store.SetBlockBits(1, 0, "00001111");
    }
    const auto wal_path = chunkdb::ChunkWalPath(dir, chunkdb::Geometry(config.geometry), {0, 0});
    const auto whole = ReadBytes(wal_path);
    auto read_only = config;
    read_only.access_mode = chunkdb::AccessMode::kReadOnly;

    std::filesystem::resize_file(wal_path, whole.size() - 3U);
    {
        chunkdb::ChunkStore reader(read_only);
        assert(reader.GetBlockBits(0, 0) == "11110000");
        assert(!reader.BlockExists(1, 0));
    }

    std::string damaged = whole;
    damaged[kWalHeaderSize + 4U] = static_cast<char>(damaged[kWalHeaderSize + 4U] ^ 0x5A);
    {
        std::ofstream out(wal_path, std::ios::binary | std::ios::trunc);
        out.write(damaged.data(), static_cast<std::streamsize>(damaged.size()));
    }
    {
        chunkdb::ChunkStore reader(read_only);
        bool refused = false;
        try {
            (void)reader.GetBlockBits(1, 0);
        } catch (const std::exception&) {
            refused = true;
        }
        assert(refused);
    }
    std::filesystem::remove_all(dir);
}

// A read-only store's area reads follow the same rules as its chunk loads:
// the frame of a rejected conditional write that a rollback intent still
// covers is not part of the chunk.
void TestReadOnlyAreaReadHonoursRollbackIntent() {
    const auto dir = TempDataDir("ro-area-rollback");
    auto config = BuildConfig(dir);
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    {
        chunkdb::ChunkStore writer(config);
        writer.SetBlockBits(0, 0, "00000001");
        const std::vector<std::uint8_t> payload(16, 0x07);
        const std::vector<std::uint8_t> presence(2, 0xFF);
        SetEnvVar("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE", "1");
        SetEnvVar("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1");
        bool failed = false;
        try {
            (void)writer.CasChunkStateBytes(0, 0, writer.GetChunkVersion(0, 0), payload, presence);
        } catch (const std::exception&) {
            failed = true;
        }
        UnsetEnvVar("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE");
        UnsetEnvVar("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE");
        assert(failed);

        auto read_only = config;
        read_only.access_mode = chunkdb::AccessMode::kReadOnly;
        chunkdb::ChunkStore reader(read_only);
        const auto range = reader.ReadChunkRange(0, 0, 0, 0);
        assert(range.size() == 1U);
        assert((range[0].presence_bitmap == std::vector<std::uint8_t>{0x01, 0x00}));
        assert(reader.ReadChunkRadius(0, 0, 0).size() == 1U);
        assert(reader.ScanPopulatedChunks(false, {}, 10).coords.size() == 1U);
        assert(reader.GetBlockBits(0, 0) == "00000001" && !reader.BlockExists(1, 0));
    }
    std::filesystem::remove_all(dir);
}

// A WAL that outlived its checkpoint (a crash between the image publish and
// the WAL removal, with frames still in the group-commit batch) is only right
// over that image. Empty-chunk collection that stops after removing the image
// must not replay that WAL into blocks that were deleted.
void TestCollectionAfterGappedWal() {
    const auto live = TempDataDir("gap-live");
    const auto crashed = TempDataDir("gap-crashed");
    const auto after = TempDataDir("gap-after");
    auto config = BuildConfig(live);
    config.wal_group_commit_updates = 100;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "00000001");  // A, flushed to the WAL file
        store.WalBarrier();
        store.UnsetBlock(0, 0);                // batch only
        store.SetBlockBits(0, 2, "00000001");  // B, batch only (another presence byte)
        store.ArmCheckpointBeforeWalRemovalPauseForTests();
        std::thread checkpoint([&] { store.CheckpointForTests(0, 0); });
        assert(store.WaitForCheckpointBeforeWalRemovalForTests());
        CopyCrashImage(live, crashed);  // image {B}, WAL [A]
        store.ResumeCheckpointBeforeWalRemovalForTests();
        checkpoint.join();
    }
    auto crashed_config = config;
    crashed_config.data_dir = crashed;
    const auto image = chunkdb::ChunkDataPath(crashed, chunkdb::Geometry(config.geometry), {0, 0});
    {
        chunkdb::ChunkStore store(crashed_config);
        assert(!store.BlockExists(0, 0) && store.BlockExists(0, 2));
        store.UnsetBlock(0, 2);
        store.WalBarrier();
        assert(std::filesystem::exists(image));
        SetEnvVar("CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_IMAGE_REMOVE_ONCE", "1");
        bool failed = false;
        try {
            store.CheckpointForTests(0, 0);
        } catch (const std::exception&) {
            failed = true;
        }
        UnsetEnvVar("CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_IMAGE_REMOVE_ONCE");
        assert(failed && !std::filesystem::exists(image));
        CopyCrashImage(crashed, after);
    }
    // A writer recovers the crash first (its odd snapshot generation keeps
    // read-only stores out until then), then a read-only store reads it.
    for (const bool read_only : {false, true}) {
        auto after_config = config;
        after_config.data_dir = after;
        if (read_only) {
            after_config.access_mode = chunkdb::AccessMode::kReadOnly;
        }
        chunkdb::ChunkStore store(after_config);
        assert(!store.BlockExists(0, 0) && !store.BlockExists(0, 2));
        assert(store.ReadChunkRange(0, 0, 0, 0).empty());
    }
    std::filesystem::remove_all(live);
    std::filesystem::remove_all(crashed);
    std::filesystem::remove_all(after);
}

// Relaxed group commit: a conditional write takes its rollback boundary
// after flushing the batch, so a write acknowledged before it survives a
// rollback that the next start has to finish.
void TestRollbackBoundaryCoversTheBatch() {
    const auto dir = TempDataDir("boundary-batch");
    auto config = BuildConfig(dir);
    // The earlier write's frame (two records) stays in the batch; the
    // conditional write's frame fills it, so both are flushed together.
    config.wal_group_commit_updates = 3;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "00000001");  // acknowledged, batch only
        const std::vector<std::uint8_t> payload(16, 0x07);
        const std::vector<std::uint8_t> presence(2, 0xFF);
        SetEnvVar("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE", "1");
        SetEnvVar("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1");
        SetEnvVar("CHUNKDB_FAILPOINT_WAL_REMOVE_FAIL_ONCE", "1");
        bool failed = false;
        try {
            (void)store.CasChunkStateBytes(0, 0, store.GetChunkVersion(0, 0), payload, presence);
        } catch (const std::exception&) {
            failed = true;
        }
        UnsetEnvVar("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE");
        UnsetEnvVar("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE");
        UnsetEnvVar("CHUNKDB_FAILPOINT_WAL_REMOVE_FAIL_ONCE");
        assert(failed);
        assert(store.GetBlockBits(0, 0) == "00000001");
    }
    {
        chunkdb::ChunkStore reopened(config);
        assert(reopened.GetBlockBits(0, 0) == "00000001");
        assert(!reopened.BlockExists(1, 0));
    }
    std::filesystem::remove_all(dir);
}

// The flush a conditional write starts with can fail, here with a repair
// that fails too: the conditional write fails with nothing applied, and the
// chunk still loads after a restart.
void TestFailedFlushBeforeConditionalWrite() {
    const auto dir = TempDataDir("repair-failed");
    auto config = BuildConfig(dir);
    config.wal_group_commit_updates = 100;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "00000001");  // acknowledged, batch only
        const std::vector<std::uint8_t> payload(16, 0x07);
        const std::vector<std::uint8_t> presence(2, 0xFF);
        // The flush before the conditional write fails at its sync, and the
        // repair of the written bytes fails too.
        SetEnvVar("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
        SetEnvVar("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1");
        SetEnvVar("CHUNKDB_FAILPOINT_WAL_REMOVE_FAIL_ONCE", "1");
        bool failed = false;
        try {
            (void)store.CasChunkStateBytes(0, 0, store.GetChunkVersion(0, 0), payload, presence);
        } catch (const std::exception&) {
            failed = true;
        }
        UnsetEnvVar("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE");
        UnsetEnvVar("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE");
        UnsetEnvVar("CHUNKDB_FAILPOINT_WAL_REMOVE_FAIL_ONCE");
        assert(failed);
        bool refused = false;
        try {
            store.SetBlockBits(1, 0, "00000011");
        } catch (const std::exception&) {
            refused = true;
        }
        assert(refused);
        assert(store.GetBlockBits(0, 0) == "00000001");
    }
    {
        chunkdb::ChunkStore reopened(config);
        assert(reopened.GetBlockBits(0, 0) == "00000001");
        assert(!reopened.BlockExists(1, 0));
    }
    std::filesystem::remove_all(dir);
}

// A write whose WAL append fails and cannot be repaired leaves its frame in
// the WAL while memory is rolled back. Eviction must not drop that memory:
// a reload would serve the write that failed.
void TestFailedOrdinaryRepairKeepsChunkCached() {
    const auto dir = TempDataDir("ordinary-repair");
    auto config = BuildConfig(dir);
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.max_loaded_chunks = 1;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "00000001");
        SetEnvVar("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
        SetEnvVar("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1");
        bool failed = false;
        try {
            store.SetBlockBits(0, 0, "00000010");
        } catch (const std::exception&) {
            failed = true;
        }
        UnsetEnvVar("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE");
        UnsetEnvVar("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE");
        assert(failed);
        assert(store.GetBlockBits(0, 0) == "00000001");
        (void)store.GetBlockBits(100, 100);
        assert(store.IsChunkLoadedForTests(0, 0));
        assert(store.GetBlockBits(0, 0) == "00000001");
    }
    std::filesystem::remove_all(dir);
}

// A conditional write whose commit record is visible but cannot be made
// durable is past its commit point: the error must say the outcome is
// unknown, not read as "not applied".
void TestUndurableCommitReportsUnknownOutcome() {
    const auto dir = TempDataDir("commit-unknown");
    auto config = BuildConfig(dir);
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    const std::vector<std::uint8_t> payload(16, 0x07);
    const std::vector<std::uint8_t> presence(2, 0xFF);
    const std::vector<const char*> failpoints = {
        "CHUNKDB_FAILPOINT_COMMIT_INTENT_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE",
        "CHUNKDB_FAILPOINT_COMMIT_INTENT_COMPLETION_SYNC_FAIL_ONCE",
        "CHUNKDB_FAILPOINT_CONDITIONAL_INTENT_UNLINK_FAIL_ONCE",
        "CHUNKDB_FAILPOINT_CONDITIONAL_COMMIT_RETRY_SYNC_FAIL_ONCE",
    };
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "00000001");
        for (const auto* name : failpoints) {
            SetEnvVar(name, "1");
        }
        std::string error;
        try {
            (void)store.CasChunkStateBytes(0, 0, store.GetChunkVersion(0, 0), payload, presence);
        } catch (const std::exception& e) {
            error = e.what();
        }
        for (const auto* name : failpoints) {
            UnsetEnvVar(name);
        }
        assert(error.find("may or may not be applied") != std::string::npos);
        assert(store.BlockExists(1, 0));  // applied in memory
    }
    {
        chunkdb::ChunkStore reopened(config);
        assert(reopened.BlockExists(1, 0));  // the visible commit record survived
    }
    std::filesystem::remove_all(dir);
}

#if defined(__APPLE__)
// On macOS a durability promise also flushes the drive's cache
// (F_FULLFSYNC): a WAL acknowledgement in fsync-wal, WALFLUSH in relaxed
// mode, and the directory entry a new part of the world needs.
void TestMacosDurabilityUsesFullSync() {
    const auto strict_dir = TempDataDir("full-sync-strict");
    const auto relaxed_dir = TempDataDir("full-sync-relaxed");
    auto config = BuildConfig(strict_dir);
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    const auto fails = [](const char* failpoint, const std::function<void()>& action) {
        SetEnvVar(failpoint, "1");
        bool failed = false;
        try {
            action();
        } catch (const std::exception&) {
            failed = true;
        }
        UnsetEnvVar(failpoint);
        return failed;
    };
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "00000001");  // the WAL exists from here on
        assert(fails("CHUNKDB_FAILPOINT_FULL_SYNC_FILE_FAIL_ONCE", [&] { store.SetBlockBits(1, 0, "00000001"); }));
        assert(!store.BlockExists(1, 0));
        store.SetBlockBits(1, 0, "00000001");
        // Another large chunk: its directory is created and synced first. A
        // failure there leaves the store fail-closed, so it comes last.
        assert(fails("CHUNKDB_FAILPOINT_FULL_SYNC_DIRECTORY_FAIL_ONCE", [&] { store.SetBlockBits(100, 100, "00000001"); }));
    }
    auto relaxed = BuildConfig(relaxed_dir);
    {
        chunkdb::ChunkStore store(relaxed);
        store.SetBlockBits(0, 0, "00000011");
        assert(fails("CHUNKDB_FAILPOINT_FULL_SYNC_FILE_FAIL_ONCE", [&] { store.WalBarrier(); }));
        store.WalBarrier();
    }
    std::filesystem::remove_all(strict_dir);
    std::filesystem::remove_all(relaxed_dir);
}
#endif

}  // namespace

int main() {
    TestCrashAfterRelaxedCheckpointPublish();
    TestCrashDuringEmptyChunkCollection();
    TestPoisonedStoreKeepsItsWal();
    TestCheckpointFailureAfterWalRemoval();
    TestReadOnlyLoadOfCrashTornTail();
    TestReadOnlyAreaReadHonoursRollbackIntent();
    TestCollectionAfterGappedWal();
    TestRollbackBoundaryCoversTheBatch();
    TestFailedFlushBeforeConditionalWrite();
    TestFailedOrdinaryRepairKeepsChunkCached();
    TestUndurableCommitReportsUnknownOutcome();
#if defined(__APPLE__)
    TestMacosDurabilityUsesFullSync();
#endif
    // Scenario 1: trailing truncated WAL record should be ignored during replay.
    {
        const auto data_dir = TempDataDir("truncated-record");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
            store.SetBlockBits(1, 0, "00001111");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        assert(std::filesystem::exists(wal_path));

        // Add a deliberately incomplete tail: bytes that do not start a frame.
        AppendBytes(wal_path, std::string("DLT1", 4) + std::string("\x01\x02\x03", 3));

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "11110000");
            assert(recovered.GetBlockBits(1, 0) == "00001111");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 2: a WAL cut inside its header (a crash while it was being
    // created) holds no mutation; it is replaced and the image is used.
    {
        const auto data_dir = TempDataDir("truncated-header");
        auto config = BuildConfig(data_dir);
        config.checkpoint_update_interval = 1;

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "10101010");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();

            const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
            assert(std::filesystem::exists(data_path));
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        WriteBytes(wal_path, "CHKW");

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "10101010");
            recovered.SetBlockBits(0, 0, "01010101");
            assert(recovered.GetBlockBits(0, 0) == "01010101");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 3: frames without a WAL header are damage, not a crash
    // artifact (a writer creates the header in the file's first append), so
    // the chunk load fails instead of guessing.
    {
        const auto data_dir = TempDataDir("headerless");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "00001111");
            store.SetBlockBits(1, 0, "11110000");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        const std::string wal = ReadBytes(wal_path);
        assert(wal.size() > kWalHeaderSize);
        WriteBytes(wal_path, wal.substr(kWalHeaderSize));

        {
            bool refused = false;
            try {
                chunkdb::ChunkStore recovered(config);
                (void)recovered.GetBlockBits(0, 0);
            } catch (const std::exception& e) {
                refused = std::string(e.what()).find("cannot be replayed") != std::string::npos;
            }
            assert(refused);
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 4: a WAL header in the middle of the stream is damage. Only
    // the 1.x lazy migration wrote one; replay now stops there.
    {
        const auto data_dir = TempDataDir("repeated-header");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "10101010");
            store.SetBlockBits(0, 0, "01010101");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        const std::string wal = ReadBytes(wal_path);
        assert(wal.size() > kWalHeaderSize);

        // Duplicate the valid header at the beginning of the record stream.
        std::string duplicated;
        duplicated.reserve(wal.size() + kWalHeaderSize);
        duplicated.append(wal.data(), static_cast<std::ptrdiff_t>(kWalHeaderSize));
        duplicated.append(wal.data(), static_cast<std::ptrdiff_t>(kWalHeaderSize));
        duplicated.append(
            wal.data() + static_cast<std::ptrdiff_t>(kWalHeaderSize),
            static_cast<std::ptrdiff_t>(wal.size() - kWalHeaderSize));
        const std::vector<std::uint8_t> duplicated_bytes(duplicated.begin(), duplicated.end());
        std::vector<std::uint8_t> payload(geometry.ChunkPayloadBytes(), 0U);
        std::vector<std::uint8_t> presence((geometry.ChunkBlockCount() + 7U) / 8U, 0U);
        chunkdb::StoreId store_id{};
        {
            chunkdb::ChunkStore store(config);
            store_id = store.store_id();
        }
        const auto replay = chunkdb::ReplayWal(
            duplicated_bytes, geometry, coord, store_id, chunkdb::FeatureFlags{}, 0, &payload,
            &presence, nullptr);
        assert(replay.replayable);
        assert(replay.tail_truncated_or_corrupt);
        assert(replay.stop_reason == "frame_magic_mismatch");
        assert(replay.applied_frames == 0U);

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 5: read-only replay must not mutate on-disk state.
    {
        const auto data_dir = TempDataDir("read-only-non-mutating");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11001100");
            store.SetBlockBits(1, 0, "00110011");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        assert(!std::filesystem::exists(data_path));
        assert(std::filesystem::exists(wal_path));

        const std::string wal_before = ReadBytes(wal_path);
        const auto tmp_artifact =
            data_path.parent_path() /
            (data_path.filename().string() + ".tmp.999999.stale-artifact");
        WriteBytes(tmp_artifact, "orphan-temp");
        assert(std::filesystem::exists(tmp_artifact));

        auto read_only = config;
        read_only.access_mode = chunkdb::AccessMode::kReadOnly;

        {
            chunkdb::ChunkStore store(read_only);
            assert(store.GetBlockBits(0, 0) == "11001100");
            assert(store.GetBlockBits(1, 0) == "00110011");
        }

        assert(!std::filesystem::exists(data_path));
        assert(std::filesystem::exists(wal_path));
        assert(ReadBytes(wal_path) == wal_before);
        assert(std::filesystem::exists(tmp_artifact));

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 6: writable replay must not compact WAL-backed state on load.
    {
        const auto data_dir = TempDataDir("writable-deferred-compaction");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "10101010");
            store.SetBlockBits(1, 0, "01010101");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        assert(!std::filesystem::exists(data_path));
        assert(std::filesystem::exists(wal_path));
        const std::string wal_before = ReadBytes(wal_path);

        {
            chunkdb::ChunkStore store(config);
            assert(store.GetBlockBits(0, 0) == "10101010");
            assert(store.GetBlockBits(1, 0) == "01010101");
        }

        assert(!std::filesystem::exists(data_path));
        assert(std::filesystem::exists(wal_path));
        assert(ReadBytes(wal_path) == wal_before);

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 7: low-value deferred compaction does not force a checkpoint on eviction.
    {
        const auto data_dir = TempDataDir("writable-eviction-no-compaction");
        auto config = BuildConfig(data_dir);
        config.max_loaded_chunks = 1;

        chunkdb::ChunkCoord coord_a;
        chunkdb::ChunkCoord coord_b;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
            coord_a = store.geometry().BlockToChunk(0, 0);
            coord_b = store.geometry().BlockToChunk(
                static_cast<std::int64_t>(store.geometry().config().chunk_width_blocks),
                0);
            geometry = store.geometry();
        }

        const auto data_path_a = chunkdb::ChunkDataPath(data_dir, geometry, coord_a);
        const auto wal_path_a = chunkdb::ChunkWalPath(data_dir, geometry, coord_a);
        assert(!std::filesystem::exists(data_path_a));
        assert(std::filesystem::exists(wal_path_a));

        {
            chunkdb::ChunkStore store(config);
            assert(store.GetBlockBits(0, 0) == "11110000");
            assert(store.GetBlockBits(
                       static_cast<std::int64_t>(geometry.config().chunk_width_blocks),
                       0) == "00000000");
        }

        assert(!std::filesystem::exists(data_path_a));
        assert(std::filesystem::exists(wal_path_a));

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 8: deferred compaction still happens on eviction when checkpoint thresholds require it.
    {
        const auto data_dir = TempDataDir("writable-eviction-threshold-compaction");
        const auto initial_config = BuildConfig(data_dir);
        auto eviction_config = BuildConfig(data_dir);
        eviction_config.max_loaded_chunks = 1;
        eviction_config.checkpoint_wal_bytes = 1;

        chunkdb::ChunkCoord coord_a;
        chunkdb::ChunkCoord coord_b;
        chunkdb::Geometry geometry(initial_config.geometry);

        {
            chunkdb::ChunkStore store(initial_config);
            store.SetBlockBits(0, 0, "11110000");
            coord_a = store.geometry().BlockToChunk(0, 0);
            coord_b = store.geometry().BlockToChunk(
                static_cast<std::int64_t>(store.geometry().config().chunk_width_blocks),
                0);
            geometry = store.geometry();
        }

        const auto data_path_a = chunkdb::ChunkDataPath(data_dir, geometry, coord_a);
        const auto wal_path_a = chunkdb::ChunkWalPath(data_dir, geometry, coord_a);
        assert(!std::filesystem::exists(data_path_a));
        assert(std::filesystem::exists(wal_path_a));

        {
            chunkdb::ChunkStore store(eviction_config);
            assert(store.GetBlockBits(0, 0) == "11110000");
            assert(store.GetBlockBits(
                       static_cast<std::int64_t>(geometry.config().chunk_width_blocks),
                       0) == "00000000");
        }

        assert(std::filesystem::exists(data_path_a));
        assert(!std::filesystem::exists(wal_path_a));

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 9: explicit presence survives WAL replay and unset remains distinct from zero bits.
    {
        const auto data_dir = TempDataDir("exists-vs-zero");
        const auto config = BuildConfig(data_dir);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "00000000");
            assert(store.BlockExists(0, 0));
            assert(store.GetBlockBits(0, 0) == "00000000");
        }

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.BlockExists(0, 0));
            assert(recovered.GetBlockBits(0, 0) == "00000000");
            recovered.UnsetBlock(0, 0);
            assert(!recovered.BlockExists(0, 0));
            assert(recovered.GetBlockBits(0, 0) == "00000000");
        }

        {
            chunkdb::ChunkStore recovered(config);
            assert(!recovered.BlockExists(0, 0));
            assert(recovered.GetBlockBits(0, 0) == "00000000");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 10: chunk-level state survives WAL replay and explicit zero chunks remain distinct from absence.
    {
        const auto data_dir = TempDataDir("chunk-exists-vs-zero");
        const auto config = BuildConfig(data_dir);

        {
            chunkdb::ChunkStore store(config);
            const std::string zero_chunk(store.geometry().ChunkPayloadBits(), '0');
            const std::string sparse_presence = "1000000000000001";
            const std::string sparse_payload = "11111111" + std::string(112, '0') + "00000000";

            store.SetChunkBits(0, 0, zero_chunk);
            assert(store.ChunkExists(0, 0));
            assert(store.BlockExists(0, 0));
            assert(store.GetBlockBits(0, 0) == "00000000");

            store.SetChunkStateBits(1, 0, sparse_payload, sparse_presence);
            assert(store.ChunkExists(1, 0));
            assert(store.BlockExists(4, 0));
            assert(!store.BlockExists(5, 0));
            assert(store.GetBlockBits(4, 0) == "11111111");
            assert(store.GetBlockBits(5, 0) == "00000000");
        }

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.ChunkExists(0, 0));
            assert(recovered.BlockExists(0, 0));
            assert(recovered.GetChunkBits(0, 0) == std::string(recovered.geometry().ChunkPayloadBits(), '0'));
            assert(recovered.GetChunkStateBits(0, 0) ==
                   std::string(recovered.geometry().ChunkPayloadBits(), '0') + "|" +
                   std::string(recovered.geometry().ChunkBlockCount(), '1'));

            assert(recovered.ChunkExists(1, 0));
            assert(recovered.GetChunkStateBits(1, 0) ==
                   "11111111" + std::string(112, '0') + "00000000|1000000000000001");
            assert(recovered.BlockExists(4, 0));
            assert(!recovered.BlockExists(5, 0));
            assert(recovered.GetBlockBits(4, 0) == "11111111");
            assert(recovered.GetBlockBits(5, 0) == "00000000");
        }

        std::filesystem::remove_all(data_dir);
    }

    return 0;
}
