#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "chunk_store_internal.hpp"
#include "checkpoint.hpp"
#include "chunk_coordinate_index.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"
#include "wal_writer.hpp"
#include "test_utils.hpp"

namespace {

chunkdb::StoreConfig Config(const std::filesystem::path& path) {
    chunkdb::StoreConfig config;
    config.data_dir = path;
    config.geometry.chunk_width_blocks = 4;
    config.geometry.chunk_height_blocks = 4;
    config.geometry.block_bits = 5;
    config.background_maintenance = false;
    config.max_loaded_chunks = 1;
    config.checkpoint_update_interval = 1000;
    config.wal_group_commit_updates = 1;
    return config;
}

std::vector<std::uint8_t> Read(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input.good());
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void TestNeverWrittenIdentity() {
    chunkdb::test::ScopedTempDir dir("chunkdb-never-written-identity");
    const auto config = Config(dir.path());
    for (int restart = 0; restart < 2; ++restart) {
        chunkdb::ChunkStore store(config);
        const auto clock = Read(dir.path() / "chunkdb.version");
        const auto frontier = store.CompletedRevision();
        for (std::int64_t coordinate = 0; coordinate < 64; ++coordinate) {
            assert(store.GetChunkVersion(coordinate, -coordinate) == 0);
            const auto state = store.ReadChunkState(coordinate, -coordinate);
            assert(!state.written && state.version == 0);
        }
        assert(store.RuntimeStats().evictions != 0);
        assert(store.GetChunkVersion(0, 0) == 0);
        assert(store.CompletedRevision() == frontier);
        assert(Read(dir.path() / "chunkdb.version") == clock);
    }
}

void TestReadDoesNotReserveRevision() {
    chunkdb::test::ScopedTempDir dir("chunkdb-read-without-version-reservation");
    chunkdb::ChunkStore store(Config(dir.path()));
#ifdef _WIN32
    _putenv_s("CHUNKDB_FAILPOINT_VERSION_RESERVE_FAIL_ONCE", "1");
#else
    setenv("CHUNKDB_FAILPOINT_VERSION_RESERVE_FAIL_ONCE", "1", 1);
#endif
    // The write failpoint remains armed across every missing-chunk read.
    for (std::int64_t coordinate = 0; coordinate < 8; ++coordinate) {
        assert(!store.ReadChunkState(coordinate, coordinate).written);
        assert(!store.GetBlockWithVersion(coordinate * 4, coordinate * 4).version);
    }
    bool failed_write = false;
    try {
        store.SetBlockBits(0, 0, "10101");
    } catch (const std::runtime_error& error) {
        failed_write = std::string(error.what()).find("version reservation failure") != std::string::npos;
    }
    assert(failed_write);
    assert(!store.ReadChunkState(0, 0).written);
    store.SetBlockBits(0, 0, "10101");
    assert(store.GetChunkVersion(0, 0) != 0);
}

void TestReadOnlyRefreshDoesNotInventIdentity() {
    chunkdb::test::ScopedTempDir dir("chunkdb-read-only-empty-identity");
    auto config = Config(dir.path());
    chunkdb::ChunkStore writer(config);
    const chunkdb::ChunkCoord missing{-4, -3};
    const auto wal_path = chunkdb::ChunkWalPath(dir.path(), writer.geometry(), missing);
    std::filesystem::create_directories(wal_path.parent_path());
    const auto header = chunkdb::BuildWalHeader(missing, writer.store_id(), writer.features());
    {
        std::ofstream output(wal_path, std::ios::binary);
        output.write(reinterpret_cast<const char*>(header.data()), static_cast<std::streamsize>(header.size()));
        output.close();
        assert(output.good());
    }
    config.access_mode = chunkdb::AccessMode::kReadOnly;
    chunkdb::ChunkStore reader(config);
    assert(!reader.ReadChunkState(missing.x, missing.y).written);
    writer.SetBlockBits(0, 0, "10101");
    writer.WalBarrier();
    const auto refreshed = reader.ReadChunkState(missing.x, missing.y);
    assert(!refreshed.written && refreshed.version == 0);
    assert(!reader.GetBlockWithVersion(missing.x * 4, missing.y * 4).version);
    assert(Read(wal_path) == header);
}

void TestCollectedIdentitySurvivesRestart() {
    chunkdb::test::ScopedTempDir dir("chunkdb-collected-producing-identity");
    const auto config = Config(dir.path());
    const chunkdb::ChunkCoord coord{-3, 7};
    std::uint64_t revision = 0U, time = 0U;
    {
        chunkdb::ChunkStore writer(config);
        writer.SetBlockBits(-12, 28, "10101");
        writer.CheckpointForTests(coord.x, coord.y);
        writer.UnsetBlock(-12, 28);
        const auto empty = writer.ReadChunkState(coord);
        revision = empty.version;
        time = empty.commit_time_ms;
        assert(revision != 0U && time != 0U && empty.written);
        const auto frontier = writer.CompletedRevision();
        auto read_config = config;
        read_config.access_mode = chunkdb::AccessMode::kReadOnly;
        chunkdb::ChunkStore reader(read_config);
        assert(reader.ReadChunkState(coord).version == revision);
        writer.CheckpointForTests(coord.x, coord.y);
        assert(writer.CompletedRevision() == frontier);
        assert(!std::filesystem::exists(chunkdb::ChunkDataPath(dir.path(), writer.geometry(), coord)));
        assert(!std::filesystem::exists(chunkdb::ChunkWalPath(dir.path(), writer.geometry(), coord)));
        const auto refreshed = reader.ReadChunkState(coord);
        assert(refreshed.written && refreshed.version == revision && refreshed.commit_time_ms == time);
        assert(reader.ScanPopulatedChunks(false, {}, 1024U).coords.empty());
        assert(writer.ScanPopulatedChunks(false, {}, 1024U).coords.empty());
        assert(writer.ScanCatalogBuildsForTests() == 0U);
        assert(writer.ScanCachedLargeChunksMergedForTests() == 0U);
    }
    {
        chunkdb::ChunkStore store(config);
        const auto empty = store.ReadChunkState(coord);
        assert(empty.written && empty.version == revision && empty.commit_time_ms == time);
        assert(store.HasWrittenChunkIdentities());
        store.SetBlockBits(-12, 28, "11001");
        assert(store.GetBlockBits(-12, 28) == "11001");
        assert(store.ReadChunkState(coord).version > revision);
        assert(store.ScanPopulatedChunks(false, {}, 1024U).coords == std::vector{coord});
        assert(store.ScanCatalogBuildsForTests() == 0U);
    }
}

void TestTornHeaderAdmissionChecksCompleteSuffix() {
    chunkdb::test::ScopedTempDir dir("chunkdb-index-torn-header-admission");
    const auto config = Config(dir.path());
    std::vector<std::uint8_t> bytes;
    std::filesystem::path wal;
    {
        chunkdb::ChunkStore store(config);
        wal = chunkdb::ChunkWalPath(dir.path(), store.geometry(), {0, 0});
        bytes = chunkdb::BuildWalHeader({0, 0}, store.store_id(), store.features());
    }
    const auto generation = Read(dir.path() / "chunkdb.snapshot");
    const auto version = Read(dir.path() / "chunkdb.version");
    bytes.back() ^= 1U;
    bytes.resize(128U * 1024U, 0U);
    bytes.back() = 1U;
    std::filesystem::create_directories(wal.parent_path());
    {
        std::ofstream output(wal, std::ios::binary);
        output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        output.close();
        assert(output.good());
    }
    bool refused = false;
    try { chunkdb::ChunkStore store(config); }
    catch (const std::runtime_error& error) {
        refused = std::string(error.what()).find("nonzero suffix") != std::string::npos;
    }
    assert(refused);
    assert(Read(wal) == bytes);
    assert(Read(dir.path() / "chunkdb.snapshot") == generation);
    assert(Read(dir.path() / "chunkdb.version") == version);
}

void TestStartupTailRepairRetainsSnapshotOwner() {
    chunkdb::test::ScopedTempDir dir("chunkdb-index-startup-tail-repair");
    const auto config = Config(dir.path());
    std::filesystem::path wal;
    std::uint64_t revision = 0U, size = 0U;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        revision = store.GetChunkVersion(0, 0);
        wal = chunkdb::ChunkWalPath(dir.path(), store.geometry(), {0, 0});
        size = std::filesystem::file_size(wal);
    }
    {
        std::ofstream output(wal, std::ios::binary | std::ios::app);
        output.write("partial", 7);
        output.close();
        assert(output.good());
    }
    chunkdb::ChunkStore writer(config);
    assert(std::filesystem::file_size(wal) == size);
    assert(writer.GetChunkVersion(0, 0) == revision);
    assert((writer.ScanPopulatedChunks(false, {}, 10U).coords == std::vector{chunkdb::ChunkCoord{0, 0}}));
    auto read_config = config;
    read_config.access_mode = chunkdb::AccessMode::kReadOnly;
    chunkdb::ChunkStore reader(read_config);
    assert(reader.GetBlockBits(0, 0) == "10101");
}

void TestReadOnlyStartupDoesNotWalkLiveArtifacts() {
    chunkdb::test::ScopedTempDir dir("chunkdb-index-read-only-artifact-race");
    auto config = Config(dir.path());
    chunkdb::ChunkStore writer(config);
    writer.SetBlockBits(0, 0, "10101");
    writer.CheckpointForTests(0, 0);
    writer.UnsetBlock(0, 0);
    class Collect final : public chunkdb::CoordinateArtifactTestHook {
      public:
        explicit Collect(chunkdb::ChunkStore& writer) : writer_(writer) {}
        void BeforeStatus(const std::filesystem::path&) override {
            if (observed++) return;
            writer_.CheckpointForTests(0, 0);
        }
        std::size_t observed = 0U;
      private:
        chunkdb::ChunkStore& writer_;
    } collect(writer);
    chunkdb::SetCoordinateArtifactTestHook(&collect);
    config.access_mode = chunkdb::AccessMode::kReadOnly;
    chunkdb::ChunkStore reader(config);
    assert(collect.observed == 0U);
    bool unstable = false;
    try { (void)reader.ScanPopulatedChunks(false, {}, 10U); }
    catch (const std::runtime_error& error) {
        unstable = std::string(error.what()).find("changed during artifact admission") != std::string::npos;
    }
    chunkdb::SetCoordinateArtifactTestHook(nullptr);
    assert(unstable && collect.observed != 0U);
    assert(reader.ScanPopulatedChunks(false, {}, 10U).coords.empty());
    assert(reader.ReadChunkState(0, 0).written);
}

void TestAreaSkipsMissingCoordinatesBeforeDiskReads() {
    chunkdb::test::ScopedTempDir dir("chunkdb-area-population-index");
    chunkdb::ChunkStore store(Config(dir.path()));
    store.ArmCachePauseForTests(chunkdb::CachePausePoint::kNoCacheReadBeforeDisk, {0, 0});
    auto area = std::async(std::launch::async, [&] { return store.ReadChunkRange(0, 0, 1, 1); });
    const auto status = area.wait_for(std::chrono::seconds(2));
    store.ResumeCachePauseForTests(chunkdb::CachePausePoint::kNoCacheReadBeforeDisk);
    assert(area.get().empty());
    assert(status == std::future_status::ready);
}

void TestRetainedBoundaryRejectsEqualWalTimeConflict() {
    chunkdb::test::ScopedTempDir directory("chunkdb-retained-wal-time");
    const auto config = Config(directory.path());
    std::filesystem::path wal;
    std::vector<std::uint8_t> bytes;
    {
        chunkdb::ChunkStore writer(config);
        writer.SetBlockBits(0, 0, "10000");
        writer.UnsetBlock(0, 0);
        const auto empty = writer.ReadChunkState(0, 0);
        writer.CheckpointForTests(0, 0);
        (void)writer.ReadChunkState(2, 0);
        assert(!writer.IsChunkLoadedForTests(0, 0));
        wal = chunkdb::ChunkWalPath(directory.path(), writer.geometry(), {0, 0});
        bytes = chunkdb::BuildWalHeader({0, 0}, writer.store_id(), writer.features());
        chunkdb::WalFrameBuilder frame(&bytes, writer.geometry().layout().schema().version);
        const std::vector<std::uint8_t> zeros(writer.geometry().ChunkPayloadBytes() +
            chunkdb::ChunkPresenceBitmapBytes(writer.geometry()), 0U);
        frame.AppendSpan(0U, zeros.data(), zeros.size());
        (void)frame.Finish(empty.version, empty.commit_time_ms + 1U);
        chunkdb::AtomicWrite(wal, bytes, true, true);
        bool refused = false;
        try { (void)writer.ScanChunkIdentities(false, {}, 10U, 0U, true); }
        catch (const std::runtime_error& error) {
            refused = std::string(error.what()).find("frame_base_commit_time_mismatch") != std::string::npos;
        }
        assert(refused && Read(wal) == bytes);
        auto readonly = config; readonly.access_mode = chunkdb::AccessMode::kReadOnly;
        chunkdb::ChunkStore reader(readonly);
        refused = false;
        try { (void)reader.ReadChunkState(0, 0); }
        catch (const std::runtime_error& error) {
            refused = std::string(error.what()).find("frame_base_commit_time_mismatch") != std::string::npos;
        }
        assert(refused && Read(wal) == bytes);
    }
    bool refused = false;
    try { chunkdb::ChunkStore writer(config); }
    catch (const std::runtime_error& error) {
        refused = std::string(error.what()).find("frame_base_commit_time_mismatch") != std::string::npos;
    }
    assert(refused && Read(wal) == bytes);
}

}  // namespace

int main(int argc, char** argv) {
    const std::string_view selected = argc > 1 ? argv[1] : "all";
    if (selected == "all" || selected == "identity") TestNeverWrittenIdentity();
    if (selected == "all" || selected == "reservation") TestReadDoesNotReserveRevision();
    if (selected == "all" || selected == "refresh") TestReadOnlyRefreshDoesNotInventIdentity();
    if (selected == "all" || selected == "collection") TestCollectedIdentitySurvivesRestart();
    if (selected == "all" || selected == "admission") TestTornHeaderAdmissionChecksCompleteSuffix();
    if (selected == "all" || selected == "repair") TestStartupTailRepairRetainsSnapshotOwner();
    if (selected == "all" || selected == "readonly-admission") TestReadOnlyStartupDoesNotWalkLiveArtifacts();
    if (selected == "all" || selected == "area-index") TestAreaSkipsMissingCoordinatesBeforeDiskReads();
    if (selected == "all" || selected == "retained-time") TestRetainedBoundaryRejectsEqualWalTimeConflict();
}
