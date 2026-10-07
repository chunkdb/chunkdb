#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <stdexcept>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/table_catalog.hpp"

#ifndef _WIN32
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

// Everything here serves the fork/SIGKILL scenario, which has no Windows
// equivalent; main() returns early there.
#ifndef _WIN32
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

// Two tables with different geometry share a cache budget far below what
// the writes touch, so chunks of both are evicted for each other, while a
// third table is created and dropped over and over.
const chunkdb::GeometryConfig kTerrainGeometry{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 8,
    .chunk_height_blocks = 2,
    .block_bits = 5,
};
constexpr std::array<const char*, 3> kDefaultValues{{"00000000", "11110000", "00001111"}};
constexpr std::array<const char*, 3> kTerrainValues{{"00000", "10101", "01010"}};
constexpr std::int64_t kTableWriteSpan = 120;  // chunks touched per table

// `default` is relaxed (fast, so the writes go far past the cache budget);
// the other tables are fsync-wal.
chunkdb::CatalogConfig BuildCatalogConfig(const std::filesystem::path& data_dir) {
    auto config = chunkdb::CatalogConfigFromStoreConfig(
        BuildConfig(data_dir, chunkdb::DurabilityMode::kRelaxed));
    config.max_loaded_chunks = 64;
    return config;
}

[[noreturn]] void ChildTablesLoop(const std::filesystem::path& data_dir) {
    chunkdb::TableCatalog catalog(BuildCatalogConfig(data_dir));
    auto options = catalog.default_options();
    options.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    const auto terrain = catalog.Create("terrain", kTerrainGeometry, options);
    const auto table = catalog.Find("default");
    // Tells the parent that both tables exist; the kill comes after this.
    std::ofstream(data_dir.string() + ".ready") << "ready";

    std::thread churn([&catalog, options]() {
        for (std::uint64_t i = 0;; ++i) {
            const auto scratch = catalog.Create("scratch", kTerrainGeometry, options);
            {
                auto lease = scratch->Acquire();
                lease->store().SetBlockBits(static_cast<std::int64_t>(i % 7), 0, "11111");
            }
            catalog.Drop("scratch");
        }
    });
    churn.detach();

    for (std::uint64_t i = 0;; ++i) {
        const auto k = static_cast<std::int64_t>(i % kTableWriteSpan);
        {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(k * 4, 0, kDefaultValues[i % kDefaultValues.size()]);
        }
        if (i % 8 == 0) {
            auto lease = terrain->Acquire();
            const auto j = static_cast<std::int64_t>((i / 8) % kTableWriteSpan);
            lease->store().SetBlockBits(j * 8, 1, kTerrainValues[(i / 8) % kTerrainValues.size()]);
        }
    }
}

bool OneOf(const std::string& value, const std::array<const char*, 3>& allowed) {
    for (const auto* candidate : allowed) {
        if (value == candidate) {
            return true;
        }
    }
    return false;
}

void ValidateRecoveredTables(const std::filesystem::path& data_dir) {
    chunkdb::TableCatalog catalog(BuildCatalogConfig(data_dir));
    // Interrupted creates and drops are gone; `scratch` exists completely
    // or not at all.
    for (const char* leftover : {".chunkdb.staging", ".chunkdb.dropped"}) {
        const auto dir = data_dir / leftover;
        assert(!std::filesystem::exists(dir) || std::filesystem::is_empty(dir));
    }
    if (const auto scratch = catalog.Find("scratch")) {
        {
            auto lease = scratch->Acquire();
            for (std::int64_t x = 0; x < 7; ++x) {
                const auto bits = lease->store().GetBlockBits(x, 0);
                assert(bits == "00000" || bits == "11111");
            }
        }
        catalog.Drop("scratch");
    }
    assert(catalog.Find("terrain")->Info().options.durability_mode ==
           chunkdb::DurabilityMode::kFsyncWal);
    auto table = *catalog.Find("default")->Acquire();
    auto terrain = *catalog.Find("terrain")->Acquire();
    const auto& geometry = catalog.Find("terrain")->geometry().config();
    assert(geometry.block_bits == kTerrainGeometry.block_bits);
    assert(geometry.chunk_width_blocks == kTerrainGeometry.chunk_width_blocks);
    for (std::int64_t k = 0; k < kTableWriteSpan; ++k) {
        assert(OneOf(table.store().GetBlockBits(k * 4, 0), kDefaultValues));
        assert(OneOf(terrain.store().GetBlockBits(k * 8, 1), kTerrainValues));
    }
    // Reading both tables went through the shared budget of 64 chunks.
    assert(table.store().RuntimeStats().evictions > 0U);
    assert(terrain.store().RuntimeStats().evictions > 0U);
    assert(catalog.resources()->LoadedChunkCount() <= 64U);
    table.store().SetBlockBits(0, 0, "11001100");
    terrain.store().SetBlockBits(0, 1, "11011");
    assert(table.store().GetBlockBits(0, 0) == "11001100");
    assert(terrain.store().GetBlockBits(0, 1) == "11011");
}

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
#endif  // !_WIN32

int main(int argc, char** argv) {
#ifdef _WIN32
    (void)argc;
    (void)argv;
    return 0;
#else
    if (argc == 4 && std::string(argv[1]) == "--child") {
        ChildLoop(argv[2], ParseModeArg(argv[3]));
    }
    if (argc == 3 && std::string(argv[1]) == "--tables-child") {
        ChildTablesLoop(argv[2]);
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

    // Several kills at different points of the table workload, timed from
    // the moment the child has created both tables.
    for (const int run_ms : {150, 400, 900}) {
        const auto data_dir = TempDataDir("tables-" + std::to_string(run_ms));
        const std::filesystem::path ready = data_dir.string() + ".ready";
        const pid_t child = fork();
        assert(child >= 0);
        if (child == 0) {
            execl(argv[0], argv[0], "--tables-child", data_dir.c_str(), nullptr);
            _exit(127);
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (!std::filesystem::exists(ready)) {
            assert(std::chrono::steady_clock::now() < deadline);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(run_ms));
        kill(child, SIGKILL);
        int status = 0;
        waitpid(child, &status, 0);
        assert(WIFSIGNALED(status));
        ValidateRecoveredTables(data_dir);
        std::filesystem::remove_all(data_dir);
        std::filesystem::remove(ready);
    }

    return 0;
#endif
}
