#include <cassert>
#include <cstdint>
#include <vector>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/geometry.hpp"

int main() {
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
        std::vector<std::uint8_t> bytes(3, 0U);
        chunkdb::BitCodec::WriteBits(bytes, 0, "1011");
        chunkdb::BitCodec::WriteBits(bytes, 7, "1110001");

        assert(chunkdb::BitCodec::ExtractBits(bytes, 0, 4) == "1011");
        assert(chunkdb::BitCodec::ExtractBits(bytes, 7, 7) == "1110001");
        assert(chunkdb::BitCodec::IsBitString("010101"));
        assert(!chunkdb::BitCodec::IsBitString("10A1"));
    }

    return 0;
}
