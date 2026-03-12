#include <cassert>
#include <filesystem>
#include <memory>

#include "chunkdb/chunk_store.hpp"

namespace {

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-lock-test-" + std::to_string(tick));
}

chunkdb::StoreConfig BuildConfig(const std::filesystem::path& path, bool allow_multi) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 4,
        },
        .data_dir = path,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 8,
        .checkpoint_wal_bytes = 256,
        .max_loaded_chunks = 64,
        .allow_multiple_processes = allow_multi,
    };
}

}  // namespace

int main() {
    const auto data_dir = TempDataDir();

    auto first = std::make_unique<chunkdb::ChunkStore>(BuildConfig(data_dir, false));

    bool blocked = false;
    try {
        auto second = std::make_unique<chunkdb::ChunkStore>(BuildConfig(data_dir, false));
        (void)second;
    } catch (const std::exception&) {
        blocked = true;
    }
    assert(blocked);

    auto allowed = std::make_unique<chunkdb::ChunkStore>(BuildConfig(data_dir, true));
    allowed->SetBlockBits(0, 0, "1111");
    assert(allowed->GetBlockBits(0, 0) == "1111");

    first.reset();
    allowed.reset();

    std::filesystem::remove_all(data_dir);
    return 0;
}
