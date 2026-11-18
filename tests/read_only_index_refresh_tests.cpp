#include <cassert>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <set>
#include <string>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "chunk_coordinate_index.hpp"
#include "checkpoint.hpp"
#include "chunkdb/file_layout.hpp"
#include "test_utils.hpp"
#include "txn_test_utils.hpp"

namespace {
using namespace chunkdb;

StoreConfig Config(const std::filesystem::path& path, std::uint8_t dims = 2U) {
    auto config = txn_test::Config(path, DurabilityMode::kRelaxed);
    config.geometry.dims = dims;
    config.geometry.chunk_depth_blocks = dims == 3U ? 2U : 1U;
    config.geometry.large_chunk_depth_chunks = dims == 3U ? 2U : 1U;
    config.background_maintenance = false;
    config.checkpoint_update_interval = 1'000'000U;
    config.checkpoint_wal_bytes = 1'000'000'000U;
    config.wal_group_commit_updates = 1U;
    return config;
}
void Write(ChunkStore& store, const ChunkCoord& coord, std::uint32_t value) {
    if (store.geometry().dims() == 3U)
        store.SetBlockBits3D(coord.x * 4, coord.y * 4, coord.z * 2, txn_test::CounterBits(value));
    else txn_test::WriteCounter(store, coord, value);
}
void Empty(ChunkStore& store, const ChunkCoord& coord) {
    if (store.geometry().dims() == 3U) store.UnsetBlock3D(coord.x * 4, coord.y * 4, coord.z * 2);
    else store.UnsetBlock(coord.x * 4, coord.y * 4);
}
class Walk final : public CoordinateArtifactTestHook {
  public:
    Walk() { SetCoordinateArtifactTestHook(this); }
    ~Walk() { SetCoordinateArtifactTestHook(nullptr); }
    void BeforeStatus(const std::filesystem::path& path) override {
        paths.push_back(path);
        if (on_artifact) std::exchange(on_artifact, {})();
    }
    void Only(const std::filesystem::path& root, const Geometry& geometry, const std::set<ChunkCoord>& changed) const {
        for (const auto& path : paths) {
            ChunkCoord coord;
            assert(ParseChunkFilePath(path.lexically_relative(std::filesystem::canonical(root)), geometry, &coord));
            assert(changed.contains(coord));
        }
    }
    std::vector<std::filesystem::path> paths;
    std::function<void()> on_artifact;
};
void Equal(ChunkStore& reader, ChunkStore& writer, std::uint64_t after = 0U) {
    for (const auto since : {std::optional<std::uint64_t>{}, std::optional<std::uint64_t>{after}}) {
        const auto actual = reader.ScanChunkIdentities(false, {}, 128U, since, true);
        const auto expected = writer.ScanChunkIdentities(false, {}, 128U, since, true);
        assert(actual.entries.size() == expected.entries.size());
        for (std::size_t i = 0; i != expected.entries.size(); ++i) {
            const auto& a = actual.entries[i]; const auto& b = expected.entries[i];
            assert(a.coord == b.coord && a.version == b.version && a.commit_time_ms == b.commit_time_ms);
            assert(a.data.has_value() == b.data.has_value());
            if (a.data) assert(a.data->payload == b.data->payload && a.data->presence_bitmap == b.data->presence_bitmap);
        }
    }
}
void ChangedArtifactsOnly(std::uint8_t dims) {
    test::ScopedTempDir directory("chunkdb-read-only-index-delta");
    const auto config = Config(directory.path(), dims);
    ChunkStore writer(config);
    const auto z = dims == 3U ? -2 : 0;
    for (int x = 0; x != 32; ++x) Write(writer, {x, 0, z}, static_cast<std::uint32_t>(x + 1));
    auto readonly = config; readonly.access_mode = AccessMode::kReadOnly;
    ChunkStore reader(readonly);
    Walk walk;
    Equal(reader, writer);
    assert(walk.paths.size() >= 32U);
    walk.paths.clear();
    const auto before = *writer.CompletedRevision();
    Write(writer, {0, 0, z}, 42U);
    Write(writer, {64, -1, z}, 43U);
    Empty(writer, {1, 0, z});
    Equal(reader, writer, before);
    assert(!walk.paths.empty());
    walk.Only(directory.path(), writer.geometry(), {{0, 0, z}, {1, 0, z}, {64, -1, z}});
    walk.paths.clear();
    if (dims == 2U) writer.CheckpointForTests(1, 0);
    Equal(reader, writer);
    assert(walk.paths.empty());
    assert(reader.RetainedTombstoneCount() == writer.RetainedTombstoneCount());
    assert(reader.RetainedTombstoneCount() == (dims == 2U ? 1U : 0U));
    Write(writer, {1, 0, z}, 44U);
    Equal(reader, writer);
    walk.Only(directory.path(), writer.geometry(), {{1, 0, z}});
    assert(reader.RetainedTombstoneCount() == 0U && writer.RetainedTombstoneCount() == 0U);
    if (dims == 2U) {
        walk.paths.clear();
        const auto before_transaction = writer.ReadChunkState(0, 0).version;
        txn_test::Transfer(writer, {0, 0}, {2, 0}, 5U);
        Equal(reader, writer);
        assert(writer.ReadChunkState(0, 0).version > before_transaction);
        walk.Only(directory.path(), writer.geometry(), {{0, 0}, {2, 0}});
        const auto area = reader.ReadChunkRange(0, 0, 31, 0, true);
        assert(area.size() == 32U && txn_test::CounterOf(ChunkState{.payload = area[0].payload,
            .presence_bitmap = area[0].presence_bitmap}) == 37U);
    }
}
void RacedRefreshPublishesNothing() {
    test::ScopedTempDir directory("chunkdb-read-only-index-race");
    const auto config = Config(directory.path());
    ChunkStore writer(config);
    Write(writer, {0, 0}, 1U); Write(writer, {1, 0}, 2U);
    auto readonly = config; readonly.access_mode = AccessMode::kReadOnly;
    ChunkStore reader(readonly);
    Equal(reader, writer);
    Write(writer, {0, 0}, 3U);
    Walk walk;
    walk.on_artifact = [&] { Write(writer, {2, 0}, 4U); Empty(writer, {1, 0}); };
    bool refused = false;
    try { (void)reader.ScanPopulatedChunks(false, {}, 10U); }
    catch (const std::runtime_error& error) { refused = std::string(error.what()).find("changed during") != std::string::npos; }
    assert(refused);
    Equal(reader, writer);
    walk.Only(directory.path(), writer.geometry(), {{0, 0}, {1, 0}, {2, 0}});
}
void RestartAndLostHistoryRebuild() {
    test::ScopedTempDir directory("chunkdb-read-only-index-history");
    const auto config = Config(directory.path());
    auto writer = std::make_unique<ChunkStore>(config);
    for (int x = 0; x != 8; ++x) Write(*writer, {x, 0}, 1U);
    auto readonly = config; readonly.access_mode = AccessMode::kReadOnly;
    ChunkStore reader(readonly);
    Equal(reader, *writer);
    Walk walk;
    for (unsigned i = 0; i != 4100U; ++i) Write(*writer, {0, 0}, i + 2U);
    Equal(reader, *writer);
    assert(walk.paths.size() >= 8U);
    walk.paths.clear();
    Write(*writer, {0, 0}, 42U);
    Equal(reader, *writer);
    walk.Only(directory.path(), writer->geometry(), {{0, 0}});
    writer.reset();
    writer = std::make_unique<ChunkStore>(config);
    Write(*writer, {9, 0}, 9U);
    walk.paths.clear();
    Equal(reader, *writer);
    assert(walk.paths.size() >= 9U);
    // A writer without change history remains readable through full admission.
    std::filesystem::remove(directory.path() / "chunkdb.snapshot.changes");
    walk.paths.clear();
    Write(*writer, {10, 0}, 10U);
    Equal(reader, *writer);
    assert(walk.paths.size() >= 10U);
}
std::string Quote(const std::string& text) {
#ifdef _WIN32
    return '"' + text + '"';
#else
    std::string result = "'";
    for (const char c : text) result += c == '\'' ? "'\\''" : std::string(1, c);
    return result + "'";
#endif
}
void WriterCrashDuringPublication(const char* executable) {
    if (!kFailpointsCompiled) { std::cout << "writer crash: skipped (failpoints disabled)\n"; return; }
    test::ScopedTempDir directory("chunkdb-read-only-index-crash");
    const auto config = Config(directory.path());
    {
        ChunkStore writer(config);
        Write(writer, {0, 0}, 10U); Write(writer, {1, 0}, 20U);
    }
    auto readonly = config; readonly.access_mode = AccessMode::kReadOnly;
    ChunkStore reader(readonly);
    assert(reader.ScanPopulatedChunks(false, {}, 10U).coords.size() == 2U);
    auto command = Quote(std::filesystem::absolute(executable).string()) + " --crash " + Quote(directory.path().string());
#ifdef _WIN32
    command = '"' + command + '"';
    assert(std::system(command.c_str()) == 86);
#else
    const auto result = std::system(command.c_str());
    assert(result != -1 && WIFEXITED(result) && WEXITSTATUS(result) == 86);
#endif
    Walk walk;
    bool refused = false;
    try { (void)reader.ScanPopulatedChunks(false, {}, 10U); }
    catch (const std::runtime_error& error) { refused = std::string(error.what()).find("unstable during writer publication") != std::string::npos; }
    assert(refused && walk.paths.empty());
    ChunkStore recovered(config);
    assert(txn_test::ReadCounter(recovered, {0, 0}) == 10U && txn_test::ReadCounter(recovered, {1, 0}) == 20U);
    Equal(reader, recovered);
    walk.paths.clear();
    Write(recovered, {2, 0}, 30U);
    Equal(reader, recovered);
    walk.Only(directory.path(), recovered.geometry(), {{2, 0}});
}
}

int main(int argc, char** argv) {
    if (argc == 3 && std::string_view(argv[1]) == "--crash") {
        ChunkStore writer(Config(argv[2]));
        txn_test::ScopedEnv crash("CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "1");
        txn_test::Transfer(writer, {0, 0}, {1, 0}, 5U);
        return 1;
    }
    ChangedArtifactsOnly(2U);
    ChangedArtifactsOnly(3U);
    RacedRefreshPublishesNothing();
    RestartAndLostHistoryRebuild();
    WriterCrashDuringPublication(argv[0]);
    std::cout << "read-only index refresh: 5 cases passed\n";
}
