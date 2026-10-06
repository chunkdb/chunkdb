#include <atomic>
#include <cassert>
#include <filesystem>
#include <exception>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/table_catalog.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace {

struct BlockCoord {
    int x = 0;
    int y = 0;

    bool operator==(const BlockCoord& other) const noexcept {
        return x == other.x && y == other.y;
    }
};

struct BlockCoordHash {
    std::size_t operator()(const BlockCoord& c) const noexcept {
        const std::size_t h1 = std::hash<int>{}(c.x);
        const std::size_t h2 = std::hash<int>{}(c.y);
        return h1 ^ (h2 << 1U);
    }
};

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    static std::atomic<std::uint64_t> counter{0};
    const std::uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);
#ifdef _WIN32
    const std::uint64_t pid = static_cast<std::uint64_t>(::GetCurrentProcessId());
#else
    const std::uint64_t pid = static_cast<std::uint64_t>(::getpid());
#endif
    return base / (
        "chunkdb-concurrency-evict-stress-" + std::to_string(tick) + "-" + std::to_string(pid) +
        "-" + std::to_string(seq));
}

void RemoveAllWithRetry(const std::filesystem::path& path) {
    constexpr int kAttempts = 20;
    constexpr auto kSleep = std::chrono::milliseconds(25);

    for (int attempt = 0; attempt < kAttempts; ++attempt) {
        std::error_code remove_ec;
        std::filesystem::remove_all(path, remove_ec);
        std::error_code exists_ec;
        if (!std::filesystem::exists(path, exists_ec) && !exists_ec) {
            return;
        }
        std::this_thread::sleep_for(kSleep);
    }

    std::error_code remove_ec;
    std::filesystem::remove_all(path, remove_ec);
    std::error_code exists_ec;
    if (std::filesystem::exists(path, exists_ec) || exists_ec) {
        throw std::runtime_error("failed to remove temp dir: " + path.string());
    }
}

std::string MakeBits(std::uint32_t v) {
    std::string bits(8, '0');
    for (int i = 0; i < 8; ++i) {
        bits[i] = ((v >> i) & 1U) != 0U ? '1' : '0';
    }
    return bits;
}

// Three tables share a budget of 16 chunks while writers and readers load
// chunks of all of them, one table's options change under load (each change
// reopens it), and a fourth table is created and dropped over and over.
void RunTablesSharingOneBudget() {
    const auto data_dir = TempDataDir();
    constexpr int kThreadCount = 9;
    constexpr int kOpsPerThread = 1500;
    constexpr std::size_t kMaxLoadedChunks = 16;
    const std::vector<std::string> tables = {"default", "wide", "tall"};

    chunkdb::CatalogConfig config;
    config.data_dir = data_dir;
    config.default_geometry = {
        .large_chunk_width_chunks = 4,
        .large_chunk_height_chunks = 4,
        .chunk_width_blocks = 8,
        .chunk_height_blocks = 8,
        .block_bits = 8,
    };
    config.default_options.checkpoint_update_interval = 128;
    config.default_options.checkpoint_wal_bytes = 16 * 1024;
    config.default_options.wal_group_commit_updates = 1;
    config.max_loaded_chunks = kMaxLoadedChunks;

    std::vector<BlockCoord> block_pool;
    for (int i = 0; i < 600; ++i) {
        block_pool.push_back(BlockCoord{(i - 300) * 8, ((i % 37) - 18) * 8});
    }
    std::map<std::pair<std::size_t, std::pair<int, int>>, std::string> expected;
    std::mutex expected_mutex;

    {
        chunkdb::TableCatalog catalog(config);
        (void)catalog.Create(
            "wide",
            {.large_chunk_width_chunks = 2, .large_chunk_height_chunks = 2,
             .chunk_width_blocks = 16, .chunk_height_blocks = 4, .block_bits = 8},
            config.default_options);
        (void)catalog.Create(
            "tall",
            {.large_chunk_width_chunks = 8, .large_chunk_height_chunks = 1,
             .chunk_width_blocks = 4, .chunk_height_blocks = 16, .block_bits = 8},
            config.default_options);

        std::atomic<bool> start{false};
        std::atomic<bool> done{false};
        std::vector<std::thread> workers;
        for (int tid = 0; tid < kThreadCount; ++tid) {
            workers.emplace_back([&, tid]() {
                std::mt19937 rng(static_cast<std::uint32_t>(0xBADC0DE + tid * 17));
                std::uniform_int_distribution<int> read_pick(
                    0, static_cast<int>(block_pool.size() - 1));
                std::uniform_int_distribution<std::size_t> table_pick(0, tables.size() - 1);
                const std::size_t own_table = static_cast<std::size_t>(tid) % tables.size();
                // Threads that write to one table write disjoint coordinates.
                const std::size_t writers_per_table = kThreadCount / tables.size();
                const std::size_t slot = static_cast<std::size_t>(tid) / tables.size();
                const std::size_t shard = block_pool.size() / writers_per_table;
                std::uniform_int_distribution<int> write_pick(
                    static_cast<int>(slot * shard), static_cast<int>((slot + 1) * shard - 1));
                const auto writer = catalog.Find(tables[own_table]);
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (int i = 0; i < kOpsPerThread; ++i) {
                    if ((i % 4) == 0) {
                        const auto reader = catalog.Find(tables[table_pick(rng)]);
                        auto lease = reader->Acquire();
                        const auto coord = block_pool[static_cast<std::size_t>(read_pick(rng))];
                        (void)lease->store().GetBlockBits(coord.x, coord.y);
                        continue;
                    }
                    const auto coord = block_pool[static_cast<std::size_t>(write_pick(rng))];
                    const auto bits = MakeBits(static_cast<std::uint32_t>(tid * 2654435761U + i));
                    {
                        auto lease = writer->Acquire();
                        lease->store().SetBlockBits(coord.x, coord.y, bits);
                    }
                    std::lock_guard lock(expected_mutex);
                    expected[{own_table, {coord.x, coord.y}}] = bits;
                }
            });
        }
        std::thread options_changer([&]() {
            std::size_t round = 0;
            while (!done.load(std::memory_order_acquire)) {
                auto options = config.default_options;
                options.checkpoint_update_interval = (round++ % 2 == 0) ? 64 : 128;
                catalog.SetOptions("tall", options);
                std::this_thread::sleep_for(std::chrono::milliseconds(15));
            }
        });
        std::thread churn([&]() {
            while (!done.load(std::memory_order_acquire)) {
                const auto scratch =
                    catalog.Create("scratch", config.default_geometry, config.default_options);
                {
                    auto lease = scratch->Acquire();
                    lease->store().SetBlockBits(0, 0, "11111111");
                }
                catalog.Drop("scratch");
            }
        });

        start.store(true, std::memory_order_release);
        for (auto& worker : workers) {
            worker.join();
        }
        done.store(true, std::memory_order_release);
        options_changer.join();
        churn.join();

        for (const auto& [key, bits] : expected) {
            auto lease = catalog.Find(tables[key.first])->Acquire();
            assert(lease->store().GetBlockBits(key.second.first, key.second.second) == bits);
        }
        assert(catalog.resources()->LoadedChunkCount() <= kMaxLoadedChunks + 8);
        assert(catalog.Find("scratch") == nullptr);
        // The shared count is exactly the tables' counts: no drift from
        // evictions, reopens or drops.
        std::uint64_t per_table = 0;
        for (const auto& name : tables) {
            auto lease = catalog.Find(name)->Acquire();
            per_table += lease->store().ApproxLoadedChunkCount();
        }
        assert(per_table == catalog.resources()->LoadedChunkCount());
    }

    {
        chunkdb::TableCatalog reloaded(config);
        assert(reloaded.TableCount() == tables.size());
        for (const auto& [key, bits] : expected) {
            auto lease = reloaded.Find(tables[key.first])->Acquire();
            assert(lease->store().GetBlockBits(key.second.first, key.second.second) == bits);
        }
    }
    RemoveAllWithRetry(data_dir);
}

}  // namespace

int main() {
    try {
    RunTablesSharingOneBudget();
    const auto data_dir = TempDataDir();

    constexpr int kThreadCount = 12;
    constexpr int kOpsPerThread = 2500;
    constexpr std::size_t kMaxLoadedChunks = 16;

    chunkdb::StoreConfig config{
        .geometry = {
            .large_chunk_width_chunks = 4,
            .large_chunk_height_chunks = 4,
            .chunk_width_blocks = 8,
            .chunk_height_blocks = 8,
            .block_bits = 8,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 128,
        .checkpoint_wal_bytes = 16 * 1024,
        // This stress target isolates concurrent eviction/load correctness; keep WAL batching
        // covered by the dedicated wal_group_commit suite rather than mixing both stressors here.
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = kMaxLoadedChunks,
        .allow_multiple_processes = false,
    };

    std::vector<BlockCoord> block_pool;
    block_pool.reserve(600);
    for (int i = 0; i < 600; ++i) {
        const int chunk_x = i - 300;
        const int chunk_y = (i % 37) - 18;
        block_pool.push_back(BlockCoord{chunk_x * 8, chunk_y * 8});
    }

    std::unordered_map<BlockCoord, std::string, BlockCoordHash> expected;
    std::mutex expected_mutex;
    std::atomic<bool> start{false};

    {
        chunkdb::ChunkStore store(config);

        std::vector<std::thread> workers;
        workers.reserve(kThreadCount);

        for (int tid = 0; tid < kThreadCount; ++tid) {
            workers.emplace_back([&, tid]() {
                std::mt19937 rng(static_cast<std::uint32_t>(0xC001D00D + tid * 31));
                std::uniform_int_distribution<int> read_pick(0, static_cast<int>(block_pool.size() - 1));

                const std::size_t shard_size = block_pool.size() / static_cast<std::size_t>(kThreadCount);
                const std::size_t shard_begin = static_cast<std::size_t>(tid) * shard_size;
                const std::size_t shard_end =
                    (tid == kThreadCount - 1) ? block_pool.size() : (shard_begin + shard_size);
                std::uniform_int_distribution<int> write_pick(
                    static_cast<int>(shard_begin),
                    static_cast<int>(shard_end - 1));

                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }

                for (int i = 0; i < kOpsPerThread; ++i) {
                    if ((i % 4) == 0) {
                        const BlockCoord read_coord = block_pool[static_cast<std::size_t>(read_pick(rng))];
                        (void)store.GetBlockBits(read_coord.x, read_coord.y);
                        continue;
                    }

                    const BlockCoord coord = block_pool[static_cast<std::size_t>(write_pick(rng))];
                    const std::string bits = MakeBits(static_cast<std::uint32_t>(tid * 1315423911U + i));
                    store.SetBlockBits(coord.x, coord.y, bits);

                    std::lock_guard lock(expected_mutex);
                    expected[coord] = bits;
                }
            });
        }

        start.store(true, std::memory_order_release);
        for (auto& worker : workers) {
            worker.join();
        }

        for (const auto& [coord, bits] : expected) {
            assert(store.GetBlockBits(coord.x, coord.y) == bits);
        }

        assert(store.ApproxLoadedChunkCount() <= kMaxLoadedChunks + 8);
    }

    {
        chunkdb::ChunkStore reloaded(config);
        std::lock_guard lock(expected_mutex);
        for (const auto& [coord, bits] : expected) {
            assert(reloaded.GetBlockBits(coord.x, coord.y) == bits);
        }
    }

    RemoveAllWithRetry(data_dir);
    return 0;
    } catch (const std::exception& e) {
        std::cerr << "stress test failed: " << e.what() << std::endl;
        return 1;
    }
}
