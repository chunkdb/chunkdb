#include <array>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <stdexcept>

#include "chunkdb/chunk_store.hpp"

#ifndef _WIN32
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-durability-kill-test-" + suffix + "-" + std::to_string(tick));
}

const char* ModeName(chunkdb::DurabilityMode mode) {
    switch (mode) {
        case chunkdb::DurabilityMode::kFsyncWal:
            return "fsync-wal";
        case chunkdb::DurabilityMode::kFsyncCheckpoint:
            return "fsync-checkpoint";
        default:
            return "relaxed";
    }
}

chunkdb::DurabilityMode ParseModeArg(const std::string& mode_text) {
    if (mode_text == "fsync-wal") {
        return chunkdb::DurabilityMode::kFsyncWal;
    }
    if (mode_text == "fsync-checkpoint") {
        return chunkdb::DurabilityMode::kFsyncCheckpoint;
    }
    throw std::invalid_argument("unsupported mode arg: " + mode_text);
}

chunkdb::StoreConfig BuildConfig(
    const std::filesystem::path& data_dir,
    chunkdb::DurabilityMode mode) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 8,
        },
        .data_dir = data_dir,
        .durability_mode = mode,
        .checkpoint_update_interval = 64,
        .checkpoint_wal_bytes = 512,
        .max_loaded_chunks = 64,
        .allow_multiple_processes = false,
    };
}

#ifndef _WIN32
[[noreturn]] void ChildLoop(
    const std::filesystem::path& data_dir,
    chunkdb::DurabilityMode mode) {
    chunkdb::ChunkStore store(BuildConfig(data_dir, mode));

    constexpr std::array<std::pair<int, int>, 4> coords{{
        {0, 0},
        {1, 0},
        {0, 1},
        {1, 1},
    }};
    constexpr std::array<const char*, 4> bits{{
        "11110000",
        "00001111",
        "10101010",
        "01010101",
    }};

    std::uint64_t i = 0;
    while (true) {
        const auto coord = coords[static_cast<std::size_t>(i % coords.size())];
        const auto value = bits[static_cast<std::size_t>(i % bits.size())];
        store.SetBlockBits(coord.first, coord.second, value);
        ++i;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}
#endif

void ValidateRecoveredWritable(
    const std::filesystem::path& data_dir,
    chunkdb::DurabilityMode mode) {
    chunkdb::ChunkStore recovered(BuildConfig(data_dir, mode));

    constexpr std::array<const char*, 5> allowed{{
        "00000000",
        "11110000",
        "00001111",
        "10101010",
        "01010101",
    }};

    for (const auto& [x, y] : std::array<std::pair<int, int>, 4>{{
             {0, 0},
             {1, 0},
             {0, 1},
             {1, 1},
         }}) {
        const std::string observed = recovered.GetBlockBits(x, y);
        bool accepted = false;
        for (const auto* bits : allowed) {
            if (observed == bits) {
                accepted = true;
                break;
            }
        }
        assert(accepted);
    }

    recovered.SetBlockBits(0, 0, "11001100");
    recovered.SetBlockBits(1, 1, "00110011");
    assert(recovered.GetBlockBits(0, 0) == "11001100");
    assert(recovered.GetBlockBits(1, 1) == "00110011");
}

}  // namespace

int main(int argc, char** argv) {
#ifdef _WIN32
    (void)argc;
    (void)argv;
    return 0;
#else
    if (argc == 4 && std::string(argv[1]) == "--child") {
        ChildLoop(argv[2], ParseModeArg(argv[3]));
    }

    for (const auto mode : {chunkdb::DurabilityMode::kFsyncWal, chunkdb::DurabilityMode::kFsyncCheckpoint}) {
        const auto data_dir = TempDataDir(ModeName(mode));

        const pid_t child = fork();
        assert(child >= 0);

        if (child == 0) {
            execl(argv[0], argv[0], "--child", data_dir.c_str(), ModeName(mode), nullptr);
            _exit(127);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        kill(child, SIGKILL);

        int status = 0;
        waitpid(child, &status, 0);
        assert(WIFSIGNALED(status));

        ValidateRecoveredWritable(data_dir, mode);
        std::filesystem::remove_all(data_dir);
    }

    return 0;
#endif
}
