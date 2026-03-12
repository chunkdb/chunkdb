#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"

namespace {

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-storage-test-" + std::to_string(tick));
}

std::vector<std::uint8_t> ReadBytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input.good());
    input.seekg(0, std::ios::end);
    const std::streamoff size = input.tellg();
    input.seekg(0, std::ios::beg);
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(size));
    if (!bytes.empty()) {
        input.read(reinterpret_cast<char*>(bytes.data()), size);
    }
    return bytes;
}

void WriteBytes(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    assert(output.good());
    if (!bytes.empty()) {
        output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    }
    output.flush();
    assert(output.good());
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

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(1, 1, "11001");

        const chunkdb::ChunkCoord coord = store.geometry().BlockToChunk(1, 1);
        const auto data_path = chunkdb::ChunkDataPath(data_dir, store.geometry(), coord);
        const auto wal_path = chunkdb::ChunkWalPath(data_dir, store.geometry(), coord);

        auto data_bytes = ReadBytes(data_path);
        assert(data_bytes.size() > 8);

        const std::uint8_t wal_magic[8] = {'C', 'H', 'K', 'W', 'A', 'L', '0', '1'};
        for (std::size_t i = 0; i < 8; ++i) {
            data_bytes[i] = wal_magic[i];
        }
        WriteBytes(wal_path, data_bytes);

        const std::vector<std::uint8_t> corrupt_payload = {'B', 'A', 'D'};
        WriteBytes(data_path, corrupt_payload);
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(1, 1) == "11001");
    }

    std::filesystem::remove_all(data_dir);
    return 0;
}
