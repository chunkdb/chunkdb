#include <cassert>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string_view>

#include "chunkdb/chunk_store.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;

StoreConfig Config(const std::filesystem::path& directory, bool three_dimensional = false) {
    StoreConfig config;
    config.data_dir = directory;
    config.geometry.chunk_width_blocks = 2;
    config.geometry.chunk_height_blocks = 2;
    config.geometry.block_bits = 8;
    config.geometry.dims = three_dimensional ? 3U : 2U;
    config.geometry.chunk_depth_blocks = three_dimensional ? 2U : 1U;
    config.geometry.large_chunk_depth_chunks = three_dimensional ? 2U : 1U;
    config.durability_mode = DurabilityMode::kFsyncWal;
    return config;
}

template <typename F> void RefusesCoordinateArity(F action) {
    bool refused = false;
    try { action(); }
    catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}

void TwoDimensionalArity() {
    test::ScopedTempDir directory("chunkdb-block-coordinate-arity");
    ChunkStore store(Config(directory.path()));
    store.SetBlockBits(-1, -1, "00000101");
    const auto before = store.GetBlockWithVersion(-1, -1);
    assert(before.version);
    // An integral third argument still denotes the expected version.
    const auto mismatch = store.UnsetBlock(-1, -1, *before.version + 1U);
    assert(!mismatch.ok && mismatch.version == *before.version);
    auto state = store.ReadChunkState(-1, -1);
    auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(30));
    for (const auto z : {0, 1, -1}) {
        RefusesCoordinateArity([&] { (void)store.BlockExists3D(-1, -1, z); });
        RefusesCoordinateArity([&] { (void)store.GetBlockBits3D(-1, -1, z); });
        RefusesCoordinateArity([&] { (void)store.ReadBlockBits3D(-1, -1, z); });
        RefusesCoordinateArity([&] { store.SetBlockBits3D(-1, -1, z, "11111111"); });
        RefusesCoordinateArity([&] { store.UnsetBlock3D(-1, -1, z); });
        RefusesCoordinateArity([&] { (void)store.UnsetBlock3D(-1, -1, z, before.version); });
        RefusesCoordinateArity([&] { store.SetBlock3D(-1, -1, z, {}); });
        RefusesCoordinateArity([&] { (void)store.SetBlock3D(-1, -1, z, {}, before.version); });
        RefusesCoordinateArity([&] { (void)store.GetBlock3D(-1, -1, z); });
        RefusesCoordinateArity([&] { (void)store.GetBlockWithVersion3D(-1, -1, z); });
        RefusesCoordinateArity([&] { (void)store.GetBlock3DAt(*snapshot, -1, -1, z); });
        RefusesCoordinateArity([&] { (void)store.GetBlockWithVersion3DAt(*snapshot, -1, -1, z); });
        RefusesCoordinateArity([&] { store.SetBlockInState3D(state, -1, -1, z, {}); });
        RefusesCoordinateArity([&] { store.UnsetBlockInState3D(state, -1, -1, z); });
        RefusesCoordinateArity([&] { (void)store.BlockInState3D(state, -1, -1, z); });
    }
    assert(store.ReadBlockBits(-1, -1) == "00000101");
    assert(store.GetBlockWithVersion(-1, -1).version == before.version);
    assert(state.payload == store.ReadChunkState(-1, -1).payload);
    assert(state.presence_bitmap == store.ReadChunkState(-1, -1).presence_bitmap);
}

void ThreeDimensionalPoints() {
    test::ScopedTempDir directory("chunkdb-block-coordinate-depth");
    auto config = Config(directory.path(), true);
    Column number;
    number.id = 1U;
    number.name = "n";
    number.type = {ColumnKind::kUnsigned, 8U};
    config.schema = TableSchema{.version = 1U, .next_column_id = 2U,
        .columns = {number}};
    const std::vector<ColumnAssignment> seven{{"n", std::uint64_t{7}}};
    const std::vector<ColumnAssignment> eight{{"n", std::uint64_t{8}}};
    {
        ChunkStore store(config);
        store.SetBlock3D(-1, -1, -1, seven);
        store.SetBlock3D(-1, -1, 0, eight);
        store.SetBlock3D(0, 0, 1, seven);
        assert(store.GetBlock3D(-1, -1, -1)->front() == ColumnValue(std::uint64_t{7}));
        assert(store.GetBlock3D(-1, -1, 0)->front() == ColumnValue(std::uint64_t{8}));
        assert(!store.BlockExists3D(0, 0, 0) && store.BlockExists3D(0, 0, 1));
        assert(store.ReadChunkState(ChunkCoord{0, 0, 0}).presence_bitmap.front() == 16U);
        const auto before = store.GetBlockWithVersion3D(-1, -1, -1);
        auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(30));
        const auto mismatch = store.SetBlock3D(-1, -1, -1, eight, *before.version + 1U);
        assert(!mismatch.ok && mismatch.version == *before.version);
        const auto changed = store.SetBlock3D(-1, -1, -1, eight, before.version);
        assert(changed.ok && changed.version > *before.version);
        assert(store.GetBlock3DAt(*snapshot, -1, -1, -1) == before.values);
        assert(store.GetBlockWithVersion3DAt(*snapshot, -1, -1, -1).version == before.version);
        auto state = store.ReadChunkState(ChunkCoord{-1, -1, -1});
        store.UnsetBlockInState3D(state, -1, -1, -1);
        assert(!store.BlockInState3D(state, -1, -1, -1));
        store.SetBlockInState3D(state, -1, -1, -1, seven);
        assert(store.BlockInState3D(state, -1, -1, -1) == before.values);
        store.UnsetBlock3D(0, 0, 1);
        assert(!store.GetBlock3D(0, 0, 1));
        RefusesCoordinateArity([&] { (void)store.BlockExists(-1, -1); });
        RefusesCoordinateArity([&] { (void)store.ChunkExists(-1, -1); });
        RefusesCoordinateArity([&] { (void)store.GetChunkPayloadBytes(-1, -1); });
        RefusesCoordinateArity([&] { (void)store.GetChunkStateBytes(-1, -1); });
    }
    {
        ChunkStore store(config);
        assert(store.GetBlock3D(-1, -1, -1)->front() == ColumnValue(std::uint64_t{8}));
        assert(store.GetBlock3D(-1, -1, 0)->front() == ColumnValue(std::uint64_t{8}));
        assert(!store.BlockExists3D(0, 0, 1));
    }
    auto bits_config = Config(directory.path() / "bits", true);
    ChunkStore bits(bits_config);
    bits.SetBlockBits3D(-1, -1, -1, "00000111");
    bits.SetBlockBits3D(-1, -1, 0, "00001000");
    assert(bits.ReadBlockBits3D(-1, -1, -1) == "00000111");
    assert(bits.GetBlockBits3D(-1, -1, 0) == "00001000");
    bits.UnsetBlock3D(-1, -1, -1);
    assert(!bits.ReadBlockBits3D(-1, -1, -1));
    assert(bits.GetBlockBits3D(-1, -1, -1) == "00000000");
}
}  // namespace

int main(int argc, char** argv) {
    TwoDimensionalArity();
    if (argc == 2 && std::string_view(argv[1]) == "--two-dimensional-only") return 0;
    ThreeDimensionalPoints();
}
