#include <cassert>
#include <cstdint>
#include <random>
#include <string>
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
