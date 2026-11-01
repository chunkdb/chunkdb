#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"
#include "test_utils.hpp"

namespace {

chunkdb::StoreConfig Config(const std::filesystem::path& path) {
    chunkdb::StoreConfig config;
    config.data_dir = path;
    config.geometry.chunk_width_blocks = 4;
    config.geometry.chunk_height_blocks = 4;
    config.geometry.block_bits = 5;
    config.background_maintenance = false;
    config.max_loaded_chunks = 1;
    config.checkpoint_update_interval = 1000;
    config.wal_group_commit_updates = 1;
    return config;
}

std::vector<std::uint8_t> Read(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input.good());
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void TestNeverWrittenIdentity() {
    chunkdb::test::ScopedTempDir dir("chunkdb-never-written-identity");
    const auto config = Config(dir.path());
    for (int restart = 0; restart < 2; ++restart) {
        chunkdb::ChunkStore store(config);
        const auto clock = Read(dir.path() / "chunkdb.version");
        const auto frontier = store.CompletedRevision();
        for (std::int64_t coordinate = 0; coordinate < 64; ++coordinate) {
            assert(store.GetChunkVersion(coordinate, -coordinate) == 0);
            const auto state = store.ReadChunkState(coordinate, -coordinate);
            assert(!state.written && state.version == 0);
        }
        assert(store.RuntimeStats().evictions != 0);
        assert(store.GetChunkVersion(0, 0) == 0);
        assert(store.CompletedRevision() == frontier);
        assert(Read(dir.path() / "chunkdb.version") == clock);
    }
}

void TestReadDoesNotReserveRevision() {
    chunkdb::test::ScopedTempDir dir("chunkdb-read-without-version-reservation");
    chunkdb::ChunkStore store(Config(dir.path()));
#ifdef _WIN32
    _putenv_s("CHUNKDB_FAILPOINT_VERSION_RESERVE_FAIL_ONCE", "1");
#else
    setenv("CHUNKDB_FAILPOINT_VERSION_RESERVE_FAIL_ONCE", "1", 1);
#endif
    // The write failpoint remains armed across every missing-chunk read.
    for (std::int64_t coordinate = 0; coordinate < 8; ++coordinate) {
        assert(!store.ReadChunkState(coordinate, coordinate).written);
        assert(!store.GetBlockWithVersion(coordinate * 4, coordinate * 4).version);
    }
    bool failed_write = false;
    try {
        store.SetBlockBits(0, 0, "10101");
    } catch (const std::runtime_error& error) {
        failed_write = std::string(error.what()).find("version reservation failure") != std::string::npos;
    }
    assert(failed_write);
    assert(!store.ReadChunkState(0, 0).written);
    store.SetBlockBits(0, 0, "10101");
    assert(store.GetChunkVersion(0, 0) != 0);
}

void TestReadOnlyRefreshDoesNotInventIdentity() {
    chunkdb::test::ScopedTempDir dir("chunkdb-read-only-empty-identity");
    auto config = Config(dir.path());
    chunkdb::ChunkStore writer(config);
    const chunkdb::ChunkCoord missing{-4, -3};
    const auto wal_path = chunkdb::ChunkWalPath(dir.path(), writer.geometry(), missing);
    std::filesystem::create_directories(wal_path.parent_path());
    const auto header = chunkdb::BuildWalHeader(missing, writer.store_id(), {});
    {
        std::ofstream output(wal_path, std::ios::binary);
        output.write(reinterpret_cast<const char*>(header.data()), static_cast<std::streamsize>(header.size()));
        output.close();
        assert(output.good());
    }
    config.access_mode = chunkdb::AccessMode::kReadOnly;
    chunkdb::ChunkStore reader(config);
    assert(!reader.ReadChunkState(missing.x, missing.y).written);
    writer.SetBlockBits(0, 0, "10101");
    writer.WalBarrier();
    const auto refreshed = reader.ReadChunkState(missing.x, missing.y);
    assert(!refreshed.written && refreshed.version == 0);
    assert(!reader.GetBlockWithVersion(missing.x * 4, missing.y * 4).version);
    assert(Read(wal_path) == header);
}

}  // namespace

int main(int argc, char** argv) {
    const std::string_view selected = argc > 1 ? argv[1] : "all";
    if (selected == "all" || selected == "identity") TestNeverWrittenIdentity();
    if (selected == "all" || selected == "reservation") TestReadDoesNotReserveRevision();
    if (selected == "all" || selected == "refresh") TestReadOnlyRefreshDoesNotInventIdentity();
}
