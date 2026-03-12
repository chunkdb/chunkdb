#include <cassert>
#include <filesystem>
#include <string>

#include "chunkdb/chunk_store.hpp"

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
    };

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        store.SetBlockBits(3, 3, "11111");
        store.SetBlockBits(-1, -1, "00011");

        assert(store.GetBlockBits(0, 0) == "10101");
        assert(store.GetBlockBits(3, 3) == "11111");
        assert(store.GetBlockBits(-1, -1) == "00011");

        const std::string chunk_bits = store.GetChunkBits(0, 0);
        assert(chunk_bits.size() == 80);
    }

    {
        chunkdb::ChunkStore store(config);
        assert(store.GetBlockBits(0, 0) == "10101");
        assert(store.GetBlockBits(3, 3) == "11111");
        assert(store.GetBlockBits(-1, -1) == "00011");
    }

    std::filesystem::remove_all(data_dir);
    return 0;
}
