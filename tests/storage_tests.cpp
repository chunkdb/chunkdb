#include <cassert>
#include <filesystem>
#include <string>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"

namespace {

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-storage-test-" + std::to_string(tick));
}

}  // namespace

void TestPackedSettersMaskPaddingBits() {
    const auto data_dir = TempDataDir();
    chunkdb::StoreConfig config{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 3,
            .chunk_height_blocks = 3,
            .block_bits = 3,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 2,
        .checkpoint_wal_bytes = 1024,
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
    };
    std::vector<std::uint8_t> expected_state;
    std::vector<std::uint8_t> expected_payload;
    {
    chunkdb::ChunkStore store(config);
    assert(store.geometry().ChunkPayloadBits() == 27);
    assert(store.geometry().ChunkPayloadBytes() == 4);

    // Bit i lives in byte i/8 as (1 << (i % 8)), so the padding bits are the
    // high bits of the last byte. Set every padding bit: they must be dropped,
    // not stored or echoed.
    const std::vector<std::uint8_t> payload{0xA5, 0x3C, 0xFF, 0xFF};
    const std::vector<std::uint8_t> presence{0x80, 0xFF};
    store.SetChunkStateBytes(0, 0, payload, presence);
    const auto state = store.GetChunkStateBytes(0, 0);
    // Presence 0x80,0x01 = blocks 7 and 8; their payload bits are 21..26, so
    // bytes 0..1 are canonicalized to zero, byte 2 keeps bits 5..7 and byte 3
    // keeps bits 0..2.
    assert((state == std::vector<std::uint8_t>{0x00, 0x00, 0xE0, 0x07, 0x80, 0x01}));
    assert(store.GetChunkStateBits(0, 0) == std::string(21, '0') + "111111" + "|" + "000000011");

    store.SetChunkPayloadBytes(1, 0, payload);
    assert((store.GetChunkPayloadBytes(1, 0) == std::vector<std::uint8_t>{0xA5, 0x3C, 0xFF, 0x07}));
    assert(store.GetChunkBits(1, 0) == "10100101" "00111100" "11111111" "111");
    assert(store.GetChunkStateBits(1, 0).substr(28) == "111111111");
    expected_state = state;
    expected_payload = store.GetChunkPayloadBytes(1, 0);
    }

    // Packed writes are durable across a clean restart.
    {
        chunkdb::ChunkStore reopened(config);
        assert(reopened.GetChunkStateBytes(0, 0) == expected_state);
        assert(reopened.GetChunkPayloadBytes(1, 0) == expected_payload);
    }

    std::filesystem::remove_all(data_dir);
}

int main() {
    TestPackedSettersMaskPaddingBits();
    const auto data_dir = TempDataDir();

    chunkdb::StoreConfig config{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 5,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 2,
        .checkpoint_wal_bytes = 1024,
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
    };

    {
        chunkdb::ChunkStore store(config);
        const std::string zero_chunk(store.geometry().ChunkPayloadBits(), '0');
        const std::string empty_presence(store.geometry().ChunkBlockCount(), '0');
        const std::string full_presence(store.geometry().ChunkBlockCount(), '1');
        const std::string sparse_presence = "1000000000000001";
        const std::string sparse_payload = "11111" + std::string(70, '0') + "00000";
        const std::size_t presence_bytes = (store.geometry().ChunkBlockCount() + 7U) / 8U;
        store.SetBlockBits(0, 0, "10101");
        store.SetBlockBits(3, 3, "11111");
        store.SetBlockBits(-1, -1, "00011");
        store.SetBlockBits(2, 2, "00000");
        store.SetChunkBits(1, 0, zero_chunk);
        store.SetChunkStateBits(2, 0, sparse_payload, sparse_presence);
        store.SetChunkStateBits(3, 0, sparse_payload, empty_presence);

        assert(store.BlockExists(0, 0));
        assert(store.GetBlockBits(0, 0) == "10101");
        assert(store.BlockExists(3, 3));
        assert(store.GetBlockBits(3, 3) == "11111");
        assert(store.BlockExists(-1, -1));
        assert(store.GetBlockBits(-1, -1) == "00011");
        assert(store.BlockExists(2, 2));
        assert(store.GetBlockBits(2, 2) == "00000");
        assert(store.ChunkExists(1, 0));
        assert(store.GetChunkBits(1, 0) == zero_chunk);
        assert(store.BlockExists(4, 0));
        assert(store.GetBlockBits(4, 0) == "00000");
        assert(store.ChunkExists(2, 0));
        assert(store.GetChunkStateBits(2, 0) == sparse_payload + "|" + sparse_presence);
        assert(store.GetChunkBits(2, 0) == sparse_payload);
        assert(store.GetChunkStateBytes(2, 0).size() == store.geometry().ChunkPayloadBytes() + presence_bytes);

        // Packed-byte setters round-trip through the byte getters and match
        // the bit-string forms exactly.
        {
            const auto state = store.GetChunkStateBytes(2, 0);
            const std::vector<std::uint8_t> payload(
                state.begin(), state.begin() + static_cast<std::ptrdiff_t>(store.geometry().ChunkPayloadBytes()));
            const std::vector<std::uint8_t> presence(
                state.begin() + static_cast<std::ptrdiff_t>(store.geometry().ChunkPayloadBytes()), state.end());
            store.SetChunkStateBytes(4, 0, payload, presence);
            assert(store.GetChunkStateBits(4, 0) == sparse_payload + "|" + sparse_presence);
            assert(store.GetChunkStateBytes(4, 0) == state);

            store.SetChunkPayloadBytes(5, 0, payload);
            assert(store.GetChunkBits(5, 0) == sparse_payload);
            assert(store.GetChunkStateBits(5, 0) == sparse_payload + "|" + full_presence);
            assert(store.GetChunkPayloadBytes(5, 0) == payload);

            bool rejected = false;
            try {
                store.SetChunkPayloadBytes(6, 0, std::vector<std::uint8_t>(payload.size() + 1, 0));
            } catch (const std::invalid_argument&) {
                rejected = true;
            }
            assert(rejected);
            rejected = false;
            try {
                store.SetChunkStateBytes(6, 0, payload, std::vector<std::uint8_t>(presence.size() - 1, 0));
            } catch (const std::invalid_argument&) {
                rejected = true;
            }
            assert(rejected);
            assert(!store.ChunkExists(6, 0));
        }
        assert(!store.ChunkExists(3, 0));
        assert(store.GetChunkStateBits(3, 0) == zero_chunk + "|" + empty_presence);
        assert(store.GetChunkBits(3, 0) == zero_chunk);
        assert(store.GetChunkStateBits(1, 0) == zero_chunk + "|" + full_presence);

        store.UnsetBlock(2, 2);
        assert(!store.BlockExists(2, 2));
        assert(store.GetBlockBits(2, 2) == "00000");

        const std::string chunk_bits = store.GetChunkBits(0, 0);
        assert(chunk_bits.size() == 80);
    }

    {
        chunkdb::ChunkStore store(config);
        assert(store.BlockExists(0, 0));
        assert(store.GetBlockBits(0, 0) == "10101");
        assert(store.BlockExists(3, 3));
        assert(store.GetBlockBits(3, 3) == "11111");
        assert(store.BlockExists(-1, -1));
        assert(store.GetBlockBits(-1, -1) == "00011");
        assert(!store.BlockExists(2, 2));
        assert(store.GetBlockBits(2, 2) == "00000");
        assert(store.ChunkExists(1, 0));
        assert(store.GetBlockBits(4, 0) == "00000");
        assert(store.ChunkExists(2, 0));
        assert(store.GetChunkStateBits(2, 0) == "11111" + std::string(70, '0') + "00000|1000000000000001");
        assert(!store.ChunkExists(3, 0));
        assert(store.GetChunkStateBits(3, 0) == std::string(store.geometry().ChunkPayloadBits(), '0') + "|" +
               std::string(store.geometry().ChunkBlockCount(), '0'));
    }

    {
        chunkdb::ChunkStore store(config);
        const chunkdb::ChunkCoord coord = store.geometry().BlockToChunk(5, 1);
        const auto wal_path = chunkdb::ChunkWalPath(data_dir, store.geometry(), coord);
        const auto chk_path = chunkdb::ChunkDataPath(data_dir, store.geometry(), coord);

        store.SetBlockBits(5, 1, "11001");
        assert(std::filesystem::exists(wal_path));
        assert(!std::filesystem::exists(chk_path));

        store.SetBlockBits(6, 1, "00110");
        assert(std::filesystem::exists(wal_path));
        assert(!std::filesystem::exists(chk_path));

        store.SetBlockBits(5, 1, "11100");
        assert(std::filesystem::exists(chk_path));
        assert(!std::filesystem::exists(wal_path));
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(5, 1) == "11100");
        assert(recovered.GetBlockBits(6, 1) == "00110");
    }

    std::filesystem::remove_all(data_dir);
    return 0;
}
