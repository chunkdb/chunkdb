#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;

StoreConfig Config(const std::filesystem::path& directory, bool three = true) {
    StoreConfig config;
    config.data_dir = directory;
    config.geometry.dims = three ? 3U : 2U;
    config.geometry.chunk_width_blocks = 1U;
    config.geometry.chunk_height_blocks = 1U;
    config.geometry.chunk_depth_blocks = 1U;
    config.geometry.large_chunk_width_chunks = 2U;
    config.geometry.large_chunk_height_chunks = 2U;
    config.geometry.large_chunk_depth_chunks = three ? 2U : 1U;
    config.geometry.block_bits = 8U;
    config.background_maintenance = false;
    config.checkpoint_update_interval = 1000U;
    config.max_loaded_chunks = 1024U;
    config.wal_group_commit_updates = 1U;
    return config;
}

template <typename Action> void Refuses(Action action) {
    bool refused = false;
    try { action(); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}

std::vector<ChunkCoord> Coordinates(const std::vector<ChunkRangeEntry>& entries) {
    std::vector<ChunkCoord> result;
    for (const auto& entry : entries) result.push_back(entry.coord);
    assert(std::is_sorted(result.begin(), result.end()));
    return result;
}

void RejectsTwoDimensionalTables() {
    test::ScopedTempDir directory("chunkdb-world-3d-arity");
    ChunkStore store(Config(directory.path(), false));
    store.SetBlockBits(0, 0, "10000000");
    auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(30));
    for (const auto z : {0, 1, -1}) {
        Refuses([&] { (void)store.ReadChunkRange3D(0, 0, z, 0, 0, z); });
        Refuses([&] { (void)store.ReadChunkRadius3D(0, 0, z, 0); });
        Refuses([&] { (void)store.ReadChunkRange3DAt(*snapshot, 0, 0, z, 0, 0, z); });
        Refuses([&] { (void)store.ReadChunkRadius3DAt(*snapshot, 0, 0, z, 0); });
    }
    assert(store.ReadBlockBits(0, 0) == "10000000");
}

void ExactBallsAndRangeAdmission() {
    test::ScopedTempDir directory("chunkdb-world-3d-balls");
    auto config = Config(directory.path());
    config.query_limits.max_area_chunks = 256U;
    ChunkStore store(config);
    std::vector<ChunkCoord> expected;
    for (std::int64_t x = -3; x <= 3; ++x) {
        for (std::int64_t y = -3; y <= 3; ++y) {
            for (std::int64_t z = -3; z <= 3; ++z) {
                store.SetBlockBits3D(x, y, z, "10000000");
                if (x * x + y * y + z * z <= 9) expected.push_back({x, y, z});
            }
        }
    }
    assert(expected.size() == 123U);
    assert(Coordinates(store.ReadChunkRadius3D(0, 0, 0, 3)) == expected);
    assert((Coordinates(store.ReadChunkRadius3D(0, 0, 0, 0)) == std::vector<ChunkCoord>{{0, 0, 0}}));
    assert((Coordinates(store.ReadChunkRange3D(-1, -1, -1, -1, -1, 0)) ==
        std::vector<ChunkCoord>{{-1, -1, -1}, {-1, -1, 0}}));
    auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(30));
    std::vector<ChunkCoord> covered;
    const TxnAreaRead read{.overlay = {}, .covered = &covered};
    Refuses([&] { (void)store.ReadChunkRadius3DAt(*snapshot, 0, 0, 0, 4, false, read); });
    Refuses([&] { (void)store.ReadChunkRadius3DAt(*snapshot, 0, 0, 0,
        std::numeric_limits<std::int64_t>::max(), false, read); });
    Refuses([&] { (void)store.ReadChunkRadius3DAt(*snapshot, 0, 0, 0, -1, false, read); });
    Refuses([&] { (void)store.ReadChunkRange3DAt(*snapshot, -128, 0, 0, 128, 0, 0, false, read); });
    Refuses([&] { (void)store.ReadChunkRange3DAt(*snapshot, 0, 0, 1, 0, 0, 0, false, read); });
    Refuses([&] { (void)store.ReadChunkRange3DAt(*snapshot,
        std::numeric_limits<std::int64_t>::min(), 0, 0,
        std::numeric_limits<std::int64_t>::max(), 0, 0, false, read); });
    assert(covered.empty());
    const auto boundary = store.ReadChunkRange3DAt(*snapshot, -128, 0, 0, 127, 0, 0, false, read);
    assert(boundary.size() == 7U);
    assert(covered.size() == config.query_limits.max_area_chunks && std::is_sorted(covered.begin(), covered.end()));
    assert((covered.front() == ChunkCoord{-128, 0, 0} && covered.back() == ChunkCoord{127, 0, 0}));
}

void ClippedIntegerEdges() {
    test::ScopedTempDir directory("chunkdb-world-3d-edges");
    ChunkStore store(Config(directory.path()));
    const auto low = std::numeric_limits<std::int64_t>::min();
    const auto high = std::numeric_limits<std::int64_t>::max();
    store.SetBlockBits3D(high, high, high, "10000000");
    store.SetBlockBits3D(low, low, low, "01000000");
    assert((Coordinates(store.ReadChunkRange3D(high, high, high, high, high, high)) ==
        std::vector<ChunkCoord>{{high, high, high}}));
    assert((Coordinates(store.ReadChunkRange3D(low, low, low, low, low, low)) ==
        std::vector<ChunkCoord>{{low, low, low}}));
    assert((Coordinates(store.ReadChunkRadius3D(high, high, high, 1)) ==
        std::vector<ChunkCoord>{{high, high, high}}));
    assert((Coordinates(store.ReadChunkRadius3D(low, low, low, 1)) ==
        std::vector<ChunkCoord>{{low, low, low}}));
}

void SnapshotAndOwnOverlay() {
    test::ScopedTempDir directory("chunkdb-world-3d-snapshot");
    auto config = Config(directory.path());
    config.geometry.chunk_width_blocks = 2U;
    config.geometry.chunk_height_blocks = 2U;
    config.geometry.chunk_depth_blocks = 2U;
    ChunkStore store(config);
    store.SetBlockBits3D(-1, -1, -1, "10000000");
    const auto before = store.ReadChunkState(ChunkCoord{-1, -1, -1});
    auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(30));
    store.SetBlockBits3D(-1, -1, -1, "01000000");
    auto own = store.ReadChunkState(ChunkCoord{0, 0, 0});
    store.SetBlockInState3D(own, 0, 0, 0, {{"bits", BitsValue{"00100000"}}});
    std::vector<ChunkCoord> covered;
    const TxnAreaRead read{.overlay = [&](const auto& coord) -> const ChunkState* {
        return coord == ChunkCoord{0, 0, 0} ? &own : nullptr;
    }, .covered = &covered};
    const auto entries = store.ReadChunkRange3DAt(*snapshot, -1, -1, -1, 0, 0, 0, true, read);
    assert((Coordinates(entries) == std::vector<ChunkCoord>{{-1, -1, -1}, {0, 0, 0}}));
    assert(entries[0].payload == before.payload && entries[0].version == before.version);
    assert(entries[0].commit_time_ms == before.commit_time_ms);
    assert(entries[1].payload == own.payload);
    assert(covered.size() == 8U && std::is_sorted(covered.begin(), covered.end()));
    covered.clear();
    assert(store.ReadChunkRadius3DAt(*snapshot, 0, 0, 0, 1, true, read).size() == 1U);
    assert(covered.size() == 7U);
    test::ScopedTempDir other_directory("chunkdb-world-3d-other-snapshot");
    ChunkStore other(Config(other_directory.path()));
    auto foreign = other.BeginTxnSnapshot(std::chrono::seconds(30));
    Refuses([&] { (void)store.ReadChunkRadius3DAt(*foreign, 0, 0, 0, 0); });
    Refuses([&] { (void)store.ReadChunkRange3DAt(*foreign, 0, 0, 0, 0, 0, 0); });
}

void MetadataOnlyAndByteBudget() {
    test::ScopedTempDir directory("chunkdb-world-3d-metadata");
    auto config = Config(directory.path());
    config.chunk_schema = SingleBitsColumnSchema(8U);
    ChunkStore store(config);
    auto state = store.ReadChunkState(ChunkCoord{0, 0, -1});
    state.chunk_columns = std::make_shared<const ChunkColumnState>(AssignChunkColumns(
        store.geometry().chunk_schema(), nullptr, {{"bits", BitsValue{"10000000"}}}, true));
    assert(store.WriteChunkState({0, 0, -1}, state, {}).ok);
    const auto entries = store.ReadChunkRange3D(0, 0, -1, 0, 0, 0);
    assert(entries.size() == 1U && entries[0].chunk_columns && entries[0].commit_time_ms != 0U);
    assert(entries[0].chunk_columns->bytes == state.chunk_columns->bytes);
    auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(30));
    auto changed = store.ReadChunkState(ChunkCoord{0, 0, -1});
    changed.chunk_columns = std::make_shared<const ChunkColumnState>(AssignChunkColumns(
        store.geometry().chunk_schema(), changed.chunk_columns.get(), {{"bits", BitsValue{"01000000"}}}, false));
    assert(store.WriteChunkState({0, 0, -1}, changed, {}).ok);
    const auto old_range = store.ReadChunkRange3DAt(*snapshot, 0, 0, -1, 0, 0, -1);
    const auto old_ball = store.ReadChunkRadius3DAt(*snapshot, 0, 0, -1, 0);
    for (const auto* old : {&old_range, &old_ball}) {
        assert(old->size() == 1U && old->front().version == entries[0].version);
        assert(old->front().commit_time_ms == entries[0].commit_time_ms);
        assert(old->front().chunk_columns->bytes == state.chunk_columns->bytes);
    }

    test::ScopedTempDir large_directory("chunkdb-world-3d-byte-budget");
    auto large_config = Config(large_directory.path());
    large_config.geometry.chunk_width_blocks = 1024U;
    large_config.geometry.chunk_height_blocks = 512U;
    large_config.geometry.chunk_depth_blocks = 2U;
    large_config.geometry.block_bits = 256U;
    ChunkStore large(large_config);
    const std::string bits(256U, '1');
    large.SetBlockBits3D(0, 0, 0, bits);
    large.SetBlockBits3D(1024, 0, 0, bits);
    bool refused = false;
    try { (void)large.ReadChunkRange3D(0, 0, 0, 1, 0, 0); }
    catch (const std::out_of_range&) { refused = true; }
    assert(refused);
}
}  // namespace

int main(int argc, char** argv) {
    RejectsTwoDimensionalTables();
    if (argc == 2 && std::string_view(argv[1]) == "--two-dimensional-only") return 0;
    ExactBallsAndRangeAdmission();
    ClippedIntegerEdges();
    SnapshotAndOwnOverlay();
    MetadataOnlyAndByteBudget();
}
