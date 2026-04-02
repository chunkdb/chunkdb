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

int main() {
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
