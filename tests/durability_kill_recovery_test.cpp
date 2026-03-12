#include <cassert>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

#include "chunkdb/chunk_store.hpp"

#ifndef _WIN32
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-durability-kill-test-" + std::to_string(tick));
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
        .durability_mode = chunkdb::DurabilityMode::kFsyncWal,
        .checkpoint_update_interval = 1'000'000,
        .checkpoint_wal_bytes = 1'000'000,
        .max_loaded_chunks = 64,
        .allow_multiple_processes = false,
    };
}

#ifndef _WIN32
[[noreturn]] void ChildLoop(const std::filesystem::path& data_dir) {
    chunkdb::ChunkStore store(BuildConfig(data_dir));
    std::uint64_t i = 0;
    while (true) {
        const std::string bits = (i % 2 == 0) ? "11110000" : "00001111";
        store.SetBlockBits(0, 0, bits);
        ++i;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}
#endif

}  // namespace

int main(int argc, char** argv) {
#ifdef _WIN32
    (void)argc;
    (void)argv;
    return 0;
#else
    if (argc == 3 && std::string(argv[1]) == "--child") {
        ChildLoop(argv[2]);
    }

    const auto data_dir = TempDataDir();

    const pid_t child = fork();
    assert(child >= 0);

    if (child == 0) {
        execl(argv[0], argv[0], "--child", data_dir.c_str(), nullptr);
        _exit(127);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    kill(child, SIGKILL);

    int status = 0;
    waitpid(child, &status, 0);
    assert(WIFSIGNALED(status));

    {
        chunkdb::ChunkStore recovered(BuildConfig(data_dir));
        const std::string observed = recovered.GetBlockBits(0, 0);
        assert(observed == "11110000" || observed == "00001111" || observed == "00000000");

        recovered.SetBlockBits(0, 0, "10101010");
        assert(recovered.GetBlockBits(0, 0) == "10101010");
    }

    std::filesystem::remove_all(data_dir);
    return 0;
#endif
}
