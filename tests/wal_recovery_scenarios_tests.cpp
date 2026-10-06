#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
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
    auto recovered_config = config;
    recovered_config.data_dir = crashed;
    chunkdb::ChunkStore recovered(recovered_config);
    assert(!recovered.ChunkExists(0, 0));
    assert(!recovered.BlockExists(0, 0) && !recovered.BlockExists(1, 0) && !recovered.BlockExists(2, 0));
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
        // ... and another chunk pushes (0,0) out of the cache: evicted
        // without a checkpoint, its WAL kept.
        (void)store.GetBlockBits(100, 100);
        assert(!store.IsChunkLoadedForTests(0, 0));
        assert(std::filesystem::exists(chunkdb::ChunkWalPath(dir, chunkdb::Geometry(config.geometry), {0, 0})));
        assert(!std::filesystem::exists(chunkdb::ChunkDataPath(dir, chunkdb::Geometry(config.geometry), {0, 0})));
    }
    chunkdb::ChunkStore reopened(config);
    assert(reopened.GetBlockBits(0, 0) == "00000001");
    std::filesystem::remove_all(dir);
}

}  // namespace

int main() {
    TestCrashAfterRelaxedCheckpointPublish();
    TestCrashDuringEmptyChunkCollection();
    TestPoisonedStoreKeepsItsWal();
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
