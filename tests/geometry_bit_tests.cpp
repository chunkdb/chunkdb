#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>
#include <unordered_set>
#include <tuple>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/geometry.hpp"
#include "chunkdb/file_layout.hpp"
#include "test_utils.hpp"
#include "chunk_store_internal.hpp"
#include "store_manifest.hpp"

namespace {

void ExpectInvalidGeometry(chunkdb::GeometryConfig config) {
    bool rejected = false;
    try {
        chunkdb::Geometry geometry(config);
        (void)geometry;
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    assert(rejected);
}

template <typename Exception, typename Action>
void ExpectThrows(Action action) {
    bool rejected = false;
    try { action(); } catch (const Exception&) { rejected = true; }
    assert(rejected);
}

auto SnapshotFiles(const std::filesystem::path& root) {
    using State = std::tuple<std::string, std::filesystem::perms, std::filesystem::file_time_type>;
    std::map<std::filesystem::path, State> files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        std::string bytes;
        if (entry.is_regular_file()) {
            std::ifstream input(entry.path(), std::ios::binary);
            assert(input.good());
            bytes.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
        }
        files.emplace(entry.path().lexically_relative(root),
                      State{bytes, entry.status().permissions(), entry.last_write_time()});
    }
    return files;
}

}  // namespace

int main() {
    {
        chunkdb::GeometryConfig config;
        config.dims = 3;
        config.chunk_width_blocks = 3;
        config.chunk_height_blocks = 5;
        config.chunk_depth_blocks = 7;
        config.large_chunk_width_chunks = 2;
        config.large_chunk_height_chunks = 3;
        config.large_chunk_depth_chunks = 4;
        const chunkdb::Geometry geometry(config);
        assert(geometry.dims() == 3);
        assert(geometry.ChunkBlockCount() == 105);
        assert(geometry.ChunkPayloadBytes() == 210);
        const std::vector<std::uint8_t> payload(geometry.ChunkPayloadBytes());
        const std::vector<std::uint8_t> presence(chunkdb::ChunkPresenceBitmapBytes(geometry));
        chunkdb::StoreId id{}; id[0] = 1;
        const auto image = chunkdb::SerializeChunkImage(geometry, {0, 0, 0}, payload, presence,
            chunkdb::CheckpointCompression::kNone, 1U, 1U, id);
        const auto decoded = chunkdb::ParseChunkImage(image, geometry, {0, 0, 0}, id,
            {.incompat = chunkdb::kFeatureChunkModel});
        assert(decoded.payload == payload && decoded.presence_bitmap == presence && decoded.revision == 1U);
        ExpectThrows<std::invalid_argument>([&] { (void)chunkdb::BuildWalHeader({0, 0, 1}, id, {}); });
        chunkdb::StoreManifest manifest;
        manifest.features = {.incompat = chunkdb::kFeatureChunkModel};
        manifest.geometry = config;
        manifest.schema = chunkdb::SingleBitsColumnSchema(config.block_bits);
        manifest.store_id = id;
        const auto manifest_bytes = chunkdb::SerializeStoreManifest(manifest);
        const auto decoded_manifest = chunkdb::ParseStoreManifest(manifest_bytes);
        assert(chunkdb::SameGeometry(decoded_manifest.geometry, config));
        manifest.features = {};
        ExpectThrows<std::invalid_argument>([&] { (void)chunkdb::SerializeStoreManifest(manifest); });
        auto different_depth = config;
        ++different_depth.chunk_depth_blocks;
        assert(!chunkdb::SameGeometry(config, different_depth));
        different_depth = config; ++different_depth.large_chunk_depth_chunks;
        assert(!chunkdb::SameGeometry(config, different_depth));
        different_depth = config; different_depth.dims = 2;
        assert(!chunkdb::SameGeometry(config, different_depth));
        assert((geometry.BlockToChunk(-1, -5, -8) == chunkdb::ChunkCoord{-1, -1, -2}));
        assert((geometry.BlockToLocal(-1, -5, -8) == std::array<std::uint32_t, 3>{2, 0, 6}));
        assert((geometry.ChunkToLarge({-3, -4, -5}) == chunkdb::LargeChunkCoord{-2, -2, -2}));
        std::size_t index = 0;
        for (std::uint32_t z = 0; z < 7; ++z)
            for (std::uint32_t y = 0; y < 5; ++y)
                for (std::uint32_t x = 0; x < 3; ++x)
                    assert(geometry.LocalBlockIndex(x, y, z) == index++);
        assert(index == geometry.ChunkBlockCount());
        ExpectThrows<std::invalid_argument>([&] { (void)geometry.BlockToChunk(1, 2); });
        ExpectThrows<std::invalid_argument>([&] { (void)geometry.BlockToLocal(1, 2); });
        ExpectThrows<std::invalid_argument>([&] { (void)geometry.LocalBlockIndex(1, 2); });
        ExpectThrows<std::out_of_range>([&] { (void)geometry.LocalBlockIndex(3, 0, 0); });
        ExpectThrows<std::out_of_range>([&] { (void)geometry.LocalBlockIndex(0, 5, 0); });
        ExpectThrows<std::out_of_range>([&] { (void)geometry.LocalBlockIndex(0, 0, 7); });
        for (const auto value : {std::numeric_limits<std::int64_t>::min(),
                                 std::int64_t{-8}, std::int64_t{-1}, std::int64_t{0},
                                 std::int64_t{8}, std::numeric_limits<std::int64_t>::max()}) {
            const auto chunk = geometry.BlockToChunk(value, value, value);
            const auto local = geometry.BlockToLocal(value, value, value);
            const std::array<std::int64_t, 3> axes{chunk.x, chunk.y, chunk.z};
            const std::array<std::uint32_t, 3> sides{3, 5, 7};
            for (std::size_t axis = 0; axis < 3; ++axis) {
                assert(local[axis] < sides[axis]);
                assert(static_cast<__int128>(axes[axis]) * sides[axis] + local[axis] == value);
            }
        }

        auto invalid = config;
        invalid.dims = 1; ExpectInvalidGeometry(invalid);
        invalid.dims = 4; ExpectInvalidGeometry(invalid);
        invalid = config; invalid.chunk_depth_blocks = 0; ExpectInvalidGeometry(invalid);
        invalid = config; invalid.large_chunk_depth_chunks = 0; ExpectInvalidGeometry(invalid);
        invalid = config; invalid.chunk_depth_blocks = 4097; ExpectInvalidGeometry(invalid);
        invalid = config; invalid.large_chunk_depth_chunks = 1'000'001; ExpectInvalidGeometry(invalid);
        invalid = config; invalid.chunk_width_blocks = 4096; invalid.chunk_height_blocks = 4096;
        invalid.chunk_depth_blocks = 4096; ExpectInvalidGeometry(invalid);
        invalid = config; invalid.chunk_width_blocks = 128; invalid.chunk_height_blocks = 128;
        invalid.chunk_depth_blocks = 64; invalid.block_bits = 1024; ExpectInvalidGeometry(invalid);
        invalid = config; invalid.dims = 2; ExpectInvalidGeometry(invalid);
        auto boundary = config;
        boundary.chunk_width_blocks = 128; boundary.chunk_height_blocks = 128;
        boundary.chunk_depth_blocks = 64; boundary.block_bits = 1;
        assert(chunkdb::Geometry(boundary).ChunkBlockCount() == 1'048'576);

        const chunkdb::ChunkCoord coord{-3, -4, -5};
        assert(chunkdb::ChunkDataPath("root", geometry, coord) == "root/L_-2_-2_-2/C_-3_-4_-5.chk");
        assert(chunkdb::ChunkWalPath("root", geometry, coord) == "root/L_-2_-2_-2/C_-3_-4_-5.wal");
        chunkdb::ChunkCoord parsed{77, 88, 99};
        assert(chunkdb::ParseChunkFilePath("L_-2_-2_-2/C_-3_-4_-5.chk", geometry, &parsed));
        assert(parsed == coord);
        assert(!chunkdb::ParseChunkFilePath("L_-1_-2_-2/C_-3_-4_-5.chk", geometry, &parsed));
        assert(parsed == coord);
        assert(!chunkdb::ParseChunkFilePath("L_-2_-2/C_-3_-4_-5.chk", geometry, &parsed));
        assert(!chunkdb::ParseChunkFilePath("../L_-2_-2_-2/C_-3_-4_-5.chk", geometry, &parsed));
        assert(!chunkdb::ParseChunkFilePath("L_-2_-2_-2/C_-3_-4_-5.other", geometry, &parsed));
    }
    {
        const chunkdb::Geometry geometry(chunkdb::GeometryConfig{});
        ExpectThrows<std::invalid_argument>([&] { (void)geometry.BlockToChunk(0, 0, 0); });
        ExpectThrows<std::invalid_argument>([&] { (void)geometry.BlockToLocal(0, 0, 0); });
        ExpectThrows<std::invalid_argument>([&] { (void)geometry.LocalBlockIndex(0, 0, 0); });
        ExpectThrows<std::invalid_argument>([&] { (void)geometry.ChunkToLarge({0, 0, 1}); });
        ExpectThrows<std::invalid_argument>([&] { (void)chunkdb::ChunkFileStem({0, 0, 1}, 2); });
        ExpectThrows<std::invalid_argument>([&] { (void)chunkdb::LargeChunkDirectory("root", {0, 0, 1}); });
        assert(chunkdb::ChunkDataPath("root", geometry, {-1, 0}) == "root/L_-1_0/C_-1_0.chk");
        chunkdb::ChunkCoord parsed{4, 5, 6};
        for (const auto name : {"C_1", "C_1_2_3", "C_+1_2", "C_01_2", "C_-0_2", "C_1_",
                                "C_1_2x", "C_9223372036854775808_2", "C_-9223372036854775809_2"}) {
            assert(!chunkdb::ParseChunkFileStem(name, 2, &parsed));
            assert((parsed == chunkdb::ChunkCoord{4, 5, 6}));
        }
        assert(chunkdb::ParseChunkFileStem("C_-9223372036854775808_9223372036854775807", 2, &parsed));
        assert(parsed.x == std::numeric_limits<std::int64_t>::min());
        assert(parsed.y == std::numeric_limits<std::int64_t>::max());
        assert(parsed.z == 0);
        assert(!chunkdb::ParseChunkFileStem("C_0_0", 1, &parsed));
        assert(!chunkdb::ParseChunkFileStem("C_0_0", 2, nullptr));
        chunkdb::LargeChunkCoord large;
        assert(chunkdb::ParseLargeChunkName("L_-1_0_3", 3, &large));
        assert((large == chunkdb::LargeChunkCoord{-1, 0, 3}));
        assert(!chunkdb::ParseLargeChunkName("L_-1_0_3", 2, &large));
        std::unordered_set<chunkdb::ChunkCoord, chunkdb::ChunkCoordHash> keys;
        keys.insert({1, 2, 0}); keys.insert({1, 2, 1}); keys.insert({1, 2, -1});
        assert(keys.size() == 3);
        assert((chunkdb::ChunkCoord{1, 2, -1} < chunkdb::ChunkCoord{1, 2, 0}));
        assert((chunkdb::LargeChunkCoord{1, 2, 0} < chunkdb::LargeChunkCoord{1, 2, 1}));
        assert(chunkdb::ChunkCoordHash{}({1, 2}) ==
               (std::hash<std::int64_t>{}(1) ^ (std::hash<std::int64_t>{}(2) << 1U)));
    }
    {
        chunkdb::test::ScopedTempDir temp("geometry-admission");
        chunkdb::StoreConfig config;
        config.data_dir = temp.path() / "new";
        config.geometry.dims = 4;
        ExpectThrows<std::invalid_argument>([&] { chunkdb::ChunkStore store(config); });
        assert(!std::filesystem::exists(config.data_dir));
        config.geometry = {};
        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "1010101010101010");
            store.WalBarrier();
        }
        const auto before = SnapshotFiles(config.data_dir);
        assert(!before.empty());
        config.geometry.dims = 3;
        config.geometry_fields = chunkdb::kGeometryDimensions;
        ExpectThrows<std::runtime_error>([&] { chunkdb::ChunkStore store(config); });
        assert(SnapshotFiles(config.data_dir) == before);
    }
    {
        chunkdb::Geometry geometry({
            .large_chunk_width_chunks = 4,
            .large_chunk_height_chunks = 4,
            .chunk_width_blocks = 8,
            .chunk_height_blocks = 8,
            .block_bits = 5,
        });

        auto c1 = geometry.BlockToChunk(15, 16);
        assert(c1.x == 1);
        assert(c1.y == 2);

        auto c2 = geometry.BlockToChunk(-1, -1);
        assert(c2.x == -1);
        assert(c2.y == -1);

        auto l2 = geometry.BlockToLocal(-1, -1);
        assert(l2.first == 7);
        assert(l2.second == 7);

        auto lg = geometry.ChunkToLarge({-5, 9});
        assert(lg.x == -2);
        assert(lg.y == 2);

        assert(geometry.ChunkBlockCount() == 64);
        assert(geometry.ChunkPayloadBits() == 320);
        assert(geometry.ChunkPayloadBytes() == 40);
    }

    {
        chunkdb::Geometry max_reasonable({
            .large_chunk_width_chunks = 1'000'000,
            .large_chunk_height_chunks = 1'000'000,
            .chunk_width_blocks = 512,
            .chunk_height_blocks = 512,
            .block_bits = 32,
        });
        assert(max_reasonable.ChunkPayloadBytes() == 1024U * 1024U);

        ExpectInvalidGeometry({
            .large_chunk_width_chunks = 1'000'001,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = 16,
            .chunk_height_blocks = 16,
            .block_bits = 16,
        });
        ExpectInvalidGeometry({
            .large_chunk_width_chunks = 8,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = 4097,
            .chunk_height_blocks = 16,
            .block_bits = 16,
        });
        ExpectInvalidGeometry({
            .large_chunk_width_chunks = 8,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = 4096,
            .chunk_height_blocks = 4096,
            .block_bits = 1,
        });
        ExpectInvalidGeometry({
            .large_chunk_width_chunks = 8,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = 1024,
            .chunk_height_blocks = 1024,
            .block_bits = 1024,
        });
        ExpectInvalidGeometry({
            .large_chunk_width_chunks = 8,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = 16,
            .chunk_height_blocks = 16,
            .block_bits = std::numeric_limits<std::uint32_t>::max(),
        });

        // block_bits is a u16 in the chunk image and WAL headers, so the
        // largest representable width is accepted and the next one is not,
        // even when the payload would fit the size bound.
        chunkdb::Geometry widest_block({
            .large_chunk_width_chunks = 1,
            .large_chunk_height_chunks = 1,
            .chunk_width_blocks = 1,
            .chunk_height_blocks = 1,
            .block_bits = 65'535,
        });
        assert(widest_block.ChunkPayloadBytes() == 8192U);
        ExpectInvalidGeometry({
            .large_chunk_width_chunks = 1,
            .large_chunk_height_chunks = 1,
            .chunk_width_blocks = 1,
            .chunk_height_blocks = 1,
            .block_bits = 65'536,
        });
    }

    {
        std::vector<std::uint8_t> bytes(3, 0U);
        chunkdb::BitCodec::WriteBits(bytes, 0, "1011");
        chunkdb::BitCodec::WriteBits(bytes, 7, "1110001");

        assert(chunkdb::BitCodec::ExtractBits(bytes, 0, 4) == "1011");
        assert(chunkdb::BitCodec::ExtractBits(bytes, 7, 7) == "1110001");
        assert(chunkdb::BitCodec::IsBitString("010101"));
        assert(!chunkdb::BitCodec::IsBitString("10A1"));
    }

    {
        std::mt19937_64 rng(1234567);
        std::vector<std::uint8_t> bytes(256, 0U);
        std::vector<std::uint8_t> mirror(bytes.size(), 0U);

        auto write_slow = [&](std::size_t bit_offset, std::string_view bits) {
            for (std::size_t i = 0; i < bits.size(); ++i) {
                const std::size_t absolute = bit_offset + i;
                const std::size_t byte_index = absolute / 8U;
                const std::size_t bit_index = absolute % 8U;
                const std::uint8_t mask = static_cast<std::uint8_t>(1U << bit_index);
                if (bits[i] == '1') {
                    mirror[byte_index] = static_cast<std::uint8_t>(mirror[byte_index] | mask);
                } else {
                    mirror[byte_index] = static_cast<std::uint8_t>(mirror[byte_index] & ~mask);
                }
            }
        };

        for (int i = 0; i < 500; ++i) {
            const bool aligned = (i % 2) == 0;
            const std::size_t bit_offset = aligned
                                               ? static_cast<std::size_t>(rng() % (bytes.size() * 8U - 64U)) & ~std::size_t(7U)
                                               : static_cast<std::size_t>(rng() % (bytes.size() * 8U - 64U));
            const std::size_t bit_count = aligned ? 64U : (1U + (rng() % 63U));

            std::string bits(bit_count, '0');
            for (std::size_t b = 0; b < bit_count; ++b) {
                bits[b] = ((rng() >> (b % 16U)) & 1U) != 0U ? '1' : '0';
            }

            chunkdb::BitCodec::WriteBits(bytes, bit_offset, bits);
            write_slow(bit_offset, bits);
            assert(chunkdb::BitCodec::ExtractBits(bytes, bit_offset, bit_count) == bits);
        }

        assert(bytes == mirror);
    }

    return 0;
}
