#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"

namespace {

constexpr std::size_t kWalHeaderSize = 8U + 2U + 2U + 4U + 4U + 8U + 8U;

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-wal-recovery-" + suffix + "-" + std::to_string(tick));
}

chunkdb::StoreConfig BuildConfig(const std::filesystem::path& data_dir) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 8,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 1'000'000,
        .checkpoint_wal_bytes = 1'000'000,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
    };
}

void AppendBytes(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    assert(out.is_open());
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.flush();
}

void WriteBytes(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    assert(out.is_open());
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.flush();
}

std::string ReadBytes(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in.is_open());

    in.seekg(0, std::ios::end);
    const std::streamoff size = in.tellg();
    assert(size >= 0);

    in.seekg(0, std::ios::beg);
    std::string out(static_cast<std::size_t>(size), '\0');
    if (!out.empty()) {
        in.read(out.data(), static_cast<std::streamsize>(out.size()));
    }
    return out;
}

}  // namespace

int main() {
    // Scenario 1: trailing truncated WAL record should be ignored during replay.
    {
        const auto data_dir = TempDataDir("truncated-record");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
            store.SetBlockBits(1, 0, "00001111");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        assert(std::filesystem::exists(wal_path));

        // Add a deliberately incomplete record tail: magic + partial header bytes.
        AppendBytes(wal_path, std::string("DLT1", 4) + std::string("\x01\x02\x03", 3));

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "11110000");
            assert(recovered.GetBlockBits(1, 0) == "00001111");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 2: truncated WAL header should be ignored when a checkpoint image exists.
    {
        const auto data_dir = TempDataDir("truncated-header");
        auto config = BuildConfig(data_dir);
        config.checkpoint_update_interval = 1;

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "10101010");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();

            const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
            assert(std::filesystem::exists(data_path));
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        WriteBytes(wal_path, "CHKW");

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "10101010");
            recovered.SetBlockBits(0, 0, "01010101");
            assert(recovered.GetBlockBits(0, 0) == "01010101");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 3: headerless WAL (record stream starts with DLT1) should replay.
    {
        const auto data_dir = TempDataDir("headerless");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "00001111");
            store.SetBlockBits(1, 0, "11110000");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        const std::string wal = ReadBytes(wal_path);
        assert(wal.size() > kWalHeaderSize);
        WriteBytes(wal_path, wal.substr(kWalHeaderSize));

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "00001111");
            assert(recovered.GetBlockBits(1, 0) == "11110000");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 4: repeated WAL header mid-stream should be skipped during replay.
    {
        const auto data_dir = TempDataDir("repeated-header");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "10101010");
            store.SetBlockBits(0, 0, "01010101");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        const std::string wal = ReadBytes(wal_path);
        assert(wal.size() > kWalHeaderSize);

        // Duplicate the valid header at the beginning of the record stream.
        std::string duplicated;
        duplicated.reserve(wal.size() + kWalHeaderSize);
        duplicated.append(wal.data(), static_cast<std::ptrdiff_t>(kWalHeaderSize));
        duplicated.append(wal.data(), static_cast<std::ptrdiff_t>(kWalHeaderSize));
        duplicated.append(
            wal.data() + static_cast<std::ptrdiff_t>(kWalHeaderSize),
            static_cast<std::ptrdiff_t>(wal.size() - kWalHeaderSize));
        WriteBytes(wal_path, duplicated);

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "01010101");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 5: read-only replay must not mutate on-disk state.
    {
        const auto data_dir = TempDataDir("read-only-non-mutating");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11001100");
            store.SetBlockBits(1, 0, "00110011");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        assert(!std::filesystem::exists(data_path));
        assert(std::filesystem::exists(wal_path));

        const std::string wal_before = ReadBytes(wal_path);
        const auto tmp_artifact =
            data_path.parent_path() /
            (data_path.filename().string() + ".tmp.999999.stale-artifact");
        WriteBytes(tmp_artifact, "orphan-temp");
        assert(std::filesystem::exists(tmp_artifact));

        auto read_only = config;
        read_only.access_mode = chunkdb::AccessMode::kReadOnly;

        {
            chunkdb::ChunkStore store(read_only);
            assert(store.GetBlockBits(0, 0) == "11001100");
            assert(store.GetBlockBits(1, 0) == "00110011");
        }

        assert(!std::filesystem::exists(data_path));
        assert(std::filesystem::exists(wal_path));
        assert(ReadBytes(wal_path) == wal_before);
        assert(std::filesystem::exists(tmp_artifact));

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 6: writable replay must not compact WAL-backed state on load.
    {
        const auto data_dir = TempDataDir("writable-deferred-compaction");
        const auto config = BuildConfig(data_dir);

        chunkdb::ChunkCoord coord;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "10101010");
            store.SetBlockBits(1, 0, "01010101");
            coord = store.geometry().BlockToChunk(0, 0);
            geometry = store.geometry();
        }

        const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
        assert(!std::filesystem::exists(data_path));
        assert(std::filesystem::exists(wal_path));
        const std::string wal_before = ReadBytes(wal_path);

        {
            chunkdb::ChunkStore store(config);
            assert(store.GetBlockBits(0, 0) == "10101010");
            assert(store.GetBlockBits(1, 0) == "01010101");
        }

        assert(!std::filesystem::exists(data_path));
        assert(std::filesystem::exists(wal_path));
        assert(ReadBytes(wal_path) == wal_before);

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 7: low-value deferred compaction does not force a checkpoint on eviction.
    {
        const auto data_dir = TempDataDir("writable-eviction-no-compaction");
        auto config = BuildConfig(data_dir);
        config.max_loaded_chunks = 1;

        chunkdb::ChunkCoord coord_a;
        chunkdb::ChunkCoord coord_b;
        chunkdb::Geometry geometry(config.geometry);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
            coord_a = store.geometry().BlockToChunk(0, 0);
            coord_b = store.geometry().BlockToChunk(
                static_cast<std::int64_t>(store.geometry().config().chunk_width_blocks),
                0);
            geometry = store.geometry();
        }

        const auto data_path_a = chunkdb::ChunkDataPath(data_dir, geometry, coord_a);
        const auto wal_path_a = chunkdb::ChunkWalPath(data_dir, geometry, coord_a);
        assert(!std::filesystem::exists(data_path_a));
        assert(std::filesystem::exists(wal_path_a));

        {
            chunkdb::ChunkStore store(config);
            assert(store.GetBlockBits(0, 0) == "11110000");
            assert(store.GetBlockBits(
                       static_cast<std::int64_t>(geometry.config().chunk_width_blocks),
                       0) == "00000000");
        }

        assert(!std::filesystem::exists(data_path_a));
        assert(std::filesystem::exists(wal_path_a));

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 8: deferred compaction still happens on eviction when checkpoint thresholds require it.
    {
        const auto data_dir = TempDataDir("writable-eviction-threshold-compaction");
        const auto initial_config = BuildConfig(data_dir);
        auto eviction_config = BuildConfig(data_dir);
        eviction_config.max_loaded_chunks = 1;
        eviction_config.checkpoint_wal_bytes = 1;

        chunkdb::ChunkCoord coord_a;
        chunkdb::ChunkCoord coord_b;
        chunkdb::Geometry geometry(initial_config.geometry);

        {
            chunkdb::ChunkStore store(initial_config);
            store.SetBlockBits(0, 0, "11110000");
            coord_a = store.geometry().BlockToChunk(0, 0);
            coord_b = store.geometry().BlockToChunk(
                static_cast<std::int64_t>(store.geometry().config().chunk_width_blocks),
                0);
            geometry = store.geometry();
        }

        const auto data_path_a = chunkdb::ChunkDataPath(data_dir, geometry, coord_a);
        const auto wal_path_a = chunkdb::ChunkWalPath(data_dir, geometry, coord_a);
        assert(!std::filesystem::exists(data_path_a));
        assert(std::filesystem::exists(wal_path_a));

        {
            chunkdb::ChunkStore store(eviction_config);
            assert(store.GetBlockBits(0, 0) == "11110000");
            assert(store.GetBlockBits(
                       static_cast<std::int64_t>(geometry.config().chunk_width_blocks),
                       0) == "00000000");
        }

        assert(std::filesystem::exists(data_path_a));
        assert(!std::filesystem::exists(wal_path_a));

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 9: explicit presence survives WAL replay and unset remains distinct from zero bits.
    {
        const auto data_dir = TempDataDir("exists-vs-zero");
        const auto config = BuildConfig(data_dir);

        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "00000000");
            assert(store.BlockExists(0, 0));
            assert(store.GetBlockBits(0, 0) == "00000000");
        }

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.BlockExists(0, 0));
            assert(recovered.GetBlockBits(0, 0) == "00000000");
            recovered.UnsetBlock(0, 0);
            assert(!recovered.BlockExists(0, 0));
            assert(recovered.GetBlockBits(0, 0) == "00000000");
        }

        {
            chunkdb::ChunkStore recovered(config);
            assert(!recovered.BlockExists(0, 0));
            assert(recovered.GetBlockBits(0, 0) == "00000000");
        }

        std::filesystem::remove_all(data_dir);
    }

    // Scenario 10: chunk-level presence survives WAL replay and explicit zero chunks remain present.
    {
        const auto data_dir = TempDataDir("chunk-exists-vs-zero");
        const auto config = BuildConfig(data_dir);

        {
            chunkdb::ChunkStore store(config);
            store.SetChunkBits(0, 0, std::string(store.geometry().ChunkPayloadBits(), '0'));
            assert(store.ChunkExists(0, 0));
            assert(store.BlockExists(0, 0));
            assert(store.GetBlockBits(0, 0) == "00000000");
        }

        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.ChunkExists(0, 0));
            assert(recovered.BlockExists(0, 0));
            assert(recovered.GetChunkBits(0, 0) == std::string(recovered.geometry().ChunkPayloadBits(), '0'));
        }

        std::filesystem::remove_all(data_dir);
    }

    return 0;
}
