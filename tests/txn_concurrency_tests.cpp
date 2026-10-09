// Transactions from many threads (docs/TRANSACTIONS_DESIGN.md), meant to run
// under TSan too: increments with retry on conflict end at exactly the
// number of acknowledged commits, transfers keep their total for every
// snapshot reader, and plain writes to the same chunks are never lost to a
// transaction that read the chunk before them.

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "txn_test_utils.hpp"

namespace {

using chunkdb::ChunkCoord;
using chunkdb::ChunkState;
using chunkdb::ChunkStore;
using chunkdb::test::ScopedTempDir;
using chunkdb::txn_test::Config;
using chunkdb::txn_test::CounterBits;
using chunkdb::txn_test::CounterOf;
using chunkdb::txn_test::Increment;
using chunkdb::txn_test::kTxnDuration;
using chunkdb::txn_test::ReadCounter;
using chunkdb::txn_test::Transfer;
using chunkdb::txn_test::WriteCounter;

constexpr int kIncrementThreads = 8;
constexpr int kIncrementsPerThread = 40;
constexpr std::size_t kCounterChunks = 3;
constexpr ChunkCoord kTransferA{5, 5};
constexpr ChunkCoord kTransferB{6, 5};
constexpr std::uint32_t kTransferTotal = 10'000;

void TestConcurrentTransactions(chunkdb::DurabilityMode mode) {
    ScopedTempDir dir("chunkdb-txn-concurrency");
    auto config = Config(dir.path(), mode);
    config.max_loaded_chunks = 8;  // evictions while transactions run
    std::vector<std::atomic<std::uint32_t>> acknowledged(kCounterChunks);
    std::atomic<std::uint32_t> last_plain{0};
    {
        ChunkStore store(config);
        WriteCounter(store, kTransferA, kTransferTotal);
        WriteCounter(store, kTransferB, 0);

        std::atomic<bool> stop{false};
        std::vector<std::thread> threads;
        for (int t = 0; t < kIncrementThreads; ++t) {
            threads.emplace_back([&, t] {
                std::mt19937 random(static_cast<unsigned>(t));
                for (int i = 0; i < kIncrementsPerThread; ++i) {
                    const std::size_t counter = random() % kCounterChunks;
                    if (Increment(store, ChunkCoord{static_cast<std::int64_t>(counter), 0}) > 0U) {
                        acknowledged[counter].fetch_add(1);
                    }
                }
            });
        }
        // Transfers between two other chunks, and snapshot readers that
        // must always see their total.
        std::vector<std::thread> background;
        for (int t = 0; t < 2; ++t) {
            background.emplace_back([&, t] {
                std::mt19937 random(static_cast<unsigned>(100 + t));
                while (!stop.load()) {
                    if (random() % 2 == 0) {
                        (void)Transfer(store, kTransferA, kTransferB, 1 + random() % 20);
                    } else {
                        (void)Transfer(store, kTransferB, kTransferA, 1 + random() % 20);
                    }
                }
            });
        }
        background.emplace_back([&] {
            while (!stop.load()) {
                auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
                const auto area = store.ReadChunkRangeAt(*snapshot, kTransferA.x, kTransferA.y, kTransferB.x, kTransferB.y);
                std::uint32_t sum = 0;
                for (const auto& entry : area) {
                    sum += CounterOf(ChunkState{
                        .version = entry.version,
                        .payload = entry.payload,
                        .presence_bitmap = entry.presence_bitmap,
                        .vars = {},
                    });
                }
                assert(sum == kTransferTotal);
            }
        });
        // Plain writes to another block of the counter chunks: a
        // transaction that read a chunk before such a write conflicts
        // instead of writing its old copy back.
        background.emplace_back([&] {
            std::uint32_t value = 0;
            while (!stop.load()) {
                ++value;
                for (std::size_t counter = 0; counter < kCounterChunks; ++counter) {
                    store.SetBlockBits(static_cast<std::int64_t>(counter) * 4 + 1, 0, CounterBits(value));
                }
                last_plain.store(value);
            }
        });
        // The plain-written block only ever grows: a transaction writing back
        // a copy older than a plain write would take it back.
        background.emplace_back([&] {
            std::vector<std::string> highest(kCounterChunks, CounterBits(0));
            const auto value_of = [](const std::string& bits) {
                std::uint32_t value = 0;
                for (std::size_t i = 0; i < bits.size(); ++i) {
                    value |= static_cast<std::uint32_t>(bits[i] == '1') << i;
                }
                return value;
            };
            while (!stop.load()) {
                for (std::size_t counter = 0; counter < kCounterChunks; ++counter) {
                    const auto bits = store.GetBlockBits(static_cast<std::int64_t>(counter) * 4 + 1, 0);
                    assert(value_of(bits) >= value_of(highest[counter]));
                    highest[counter] = bits;
                }
            }
        });
        // Unrelated chunks loaded and written, pushing others out of the
        // cache.
        background.emplace_back([&] {
            std::uint32_t value = 0;
            while (!stop.load()) {
                WriteCounter(store, ChunkCoord{20 + static_cast<std::int64_t>(value % 12), 3}, value);
                ++value;
            }
        });
        for (auto& thread : threads) {
            thread.join();
        }
        stop.store(true);
        for (auto& thread : background) {
            thread.join();
        }

        for (std::size_t counter = 0; counter < kCounterChunks; ++counter) {
            const ChunkCoord coord{static_cast<std::int64_t>(counter), 0};
            const auto value = ReadCounter(store, coord);
            if (value != acknowledged[counter].load()) {
                std::fprintf(stderr, "counter %zu is %u after %u acknowledged increments\n", counter, value,
                             acknowledged[counter].load());
                assert(false);
            }
            assert(store.GetBlockBits(coord.x * 4 + 1, 0) == CounterBits(last_plain.load()));
        }
        std::uint32_t total = 0;
        for (const auto& coord : {kTransferA, kTransferB}) {
            total += ReadCounter(store, coord);
        }
        assert(total == kTransferTotal);
        assert(store.TxnRegisteredCountForTests() == 0U);
        assert(store.TxnKeptStateCountForTests() == 0U);
    }
    ChunkStore reopened(config);
    std::uint32_t sum = 0;
    for (std::size_t counter = 0; counter < kCounterChunks; ++counter) {
        sum += ReadCounter(reopened, ChunkCoord{static_cast<std::int64_t>(counter), 0});
    }
    assert(sum == static_cast<std::uint32_t>(kIncrementThreads * kIncrementsPerThread));
}

}  // namespace

int main() {
    TestConcurrentTransactions(chunkdb::DurabilityMode::kRelaxed);
    TestConcurrentTransactions(chunkdb::DurabilityMode::kFsyncWal);
    TestConcurrentTransactions(chunkdb::DurabilityMode::kFsyncCheckpoint);
    return 0;
}
