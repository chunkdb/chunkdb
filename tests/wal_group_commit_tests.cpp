#include <cassert>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"

#ifndef _WIN32
#include <sys/resource.h>
#endif

namespace {

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-wal-group-commit-" + suffix + "-" + std::to_string(tick));
}

chunkdb::StoreConfig BaseConfig(const std::filesystem::path& data_dir) {
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
        .checkpoint_update_interval = 10'000,
        .checkpoint_wal_bytes = 10'000'000,
        .wal_group_commit_updates = 4,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
    };
}

std::string MakeBits(std::uint32_t v) {
    std::string bits(8, '0');
    for (int i = 0; i < 8; ++i) {
        bits[i] = ((v >> i) & 1U) != 0U ? '1' : '0';
    }
    return bits;
}

void TestRelaxedGroupCommitThreshold() {
    const auto data_dir = TempDataDir("threshold");
    auto config = BaseConfig(data_dir);

    chunkdb::ChunkCoord coord;
    chunkdb::Geometry geometry(config.geometry);

    {
        chunkdb::ChunkStore store(config);
        coord = store.geometry().BlockToChunk(0, 0);
        geometry = store.geometry();

        const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);

        store.SetBlockBits(0, 0, MakeBits(1));
        store.SetBlockBits(0, 0, MakeBits(2));
        store.SetBlockBits(0, 0, MakeBits(3));

        // Before threshold flush, relaxed-mode group commit should keep WAL unsynced in memory.
        assert(!std::filesystem::exists(wal_path));

        store.SetBlockBits(0, 0, MakeBits(4));
        assert(std::filesystem::exists(wal_path));
        assert(std::filesystem::file_size(wal_path) > 0);
    }

    std::filesystem::remove_all(data_dir);
}

void TestGroupCommitFlushOnCleanShutdown() {
    const auto data_dir = TempDataDir("shutdown");
    auto config = BaseConfig(data_dir);
    config.wal_group_commit_updates = 64;

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101010");
        store.SetBlockBits(1, 0, "01010101");
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "10101010");
        assert(recovered.GetBlockBits(1, 0) == "01010101");
    }

    std::filesystem::remove_all(data_dir);
}

void TestWalFlushReusesAppendHandle() {
    const auto data_dir = TempDataDir("reuse-append-handle");
    auto config = BaseConfig(data_dir);
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.wal_group_commit_updates = 1;
    config.checkpoint_update_interval = 10'000;
    config.checkpoint_wal_bytes = 10'000'000;

    {
        chunkdb::ChunkStore store(config);
        for (std::uint32_t i = 0; i < 12; ++i) {
            store.SetBlockBits(0, 0, MakeBits(i));
        }

        // With a persistent WAL append stream for the loaded chunk, repeated flushes
        // should not reopen the file every time.
        assert(store.WalOpenCountForTests() == 1);
    }

    std::filesystem::remove_all(data_dir);
}

void TestWalOpenHandleCap() {
    const auto data_dir = TempDataDir("open-handle-cap");
    auto config = BaseConfig(data_dir);
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.wal_group_commit_updates = 1;
    config.max_loaded_chunks = 512;
    config.max_open_wal_streams = 8;
    config.checkpoint_update_interval = 10'000;
    config.checkpoint_wal_bytes = 10'000'000;

    {
        chunkdb::ChunkStore store(config);
        for (int i = 0; i < 128; ++i) {
            const int x = i * 4;
            store.SetBlockBits(x, 0, MakeBits(static_cast<std::uint32_t>(i)));
            assert(store.OpenWalStreamCountForTests() <= config.max_open_wal_streams);
        }
    }

    std::filesystem::remove_all(data_dir);
}

void TestWalOpenHandleCapAutoClampNearRlimit() {
#ifdef _WIN32
    return;
#else
    struct rlimit original {};
    if (getrlimit(RLIMIT_NOFILE, &original) != 0) {
        return;
    }
    if (original.rlim_cur == RLIM_INFINITY) {
        return;
    }

    const rlim_t desired_soft = std::min<rlim_t>(original.rlim_cur, static_cast<rlim_t>(64));
    if (desired_soft < static_cast<rlim_t>(48)) {
        return;
    }

    struct rlimit limited = original;
    limited.rlim_cur = desired_soft;
    if (setrlimit(RLIMIT_NOFILE, &limited) != 0) {
        return;
    }
    struct ScopedRestore {
        struct rlimit previous {};
        ~ScopedRestore() { (void)setrlimit(RLIMIT_NOFILE, &previous); }
    } restore{original};

    const auto data_dir = TempDataDir("open-handle-auto-clamp");
    auto config = BaseConfig(data_dir);
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.wal_group_commit_updates = 1;
    config.max_loaded_chunks = 512;
    config.max_open_wal_streams = static_cast<std::size_t>(desired_soft);
    config.checkpoint_update_interval = 10'000;
    config.checkpoint_wal_bytes = 10'000'000;

    const std::size_t expected_cap =
        desired_soft > static_cast<rlim_t>(32)
            ? static_cast<std::size_t>(desired_soft - static_cast<rlim_t>(32))
            : 1U;

    {
        chunkdb::ChunkStore store(config);
        assert(store.MaxOpenWalStreamsForTests() == expected_cap);
        for (int i = 0; i < 128; ++i) {
            const int x = i * 4;
            store.SetBlockBits(x, 0, MakeBits(static_cast<std::uint32_t>(i)));
            assert(store.OpenWalStreamCountForTests() <= expected_cap);
        }
    }

    std::filesystem::remove_all(data_dir);
#endif
}

void TestWalParentDirectoryPrepareIsCachedPerParent() {
    const auto data_dir = TempDataDir("parent-dir-cache");
    auto config = BaseConfig(data_dir);
    config.geometry.large_chunk_width_chunks = 128;
    config.geometry.large_chunk_height_chunks = 1;
    config.geometry.chunk_width_blocks = 4;
    config.geometry.chunk_height_blocks = 4;
    config.geometry.block_bits = 8;
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.wal_group_commit_updates = 1;
    config.max_loaded_chunks = 1024;
    config.checkpoint_update_interval = 10'000;
    config.checkpoint_wal_bytes = 10'000'000;

    {
        chunkdb::ChunkStore store(config);
        for (int i = 0; i < 32; ++i) {
            const int x = i * static_cast<int>(config.geometry.chunk_width_blocks);
            store.SetBlockBits(x, 0, MakeBits(static_cast<std::uint32_t>(i)));
        }

        // All writes map into different chunks under the same WAL parent directory.
        assert(store.WalParentPrepareCountForTests() == 1);
    }

    std::filesystem::remove_all(data_dir);
}

}  // namespace

int main() {
    TestRelaxedGroupCommitThreshold();
    TestGroupCommitFlushOnCleanShutdown();
    TestWalFlushReusesAppendHandle();
    TestWalOpenHandleCap();
    TestWalOpenHandleCapAutoClampNearRlimit();
    TestWalParentDirectoryPrepareIsCachedPerParent();
    return 0;
}
