// Transactions under SIGKILL (docs/design/TRANSACTIONS_DESIGN.md): a child process
// moves amounts between counters in random chunks inside transactions from
// several threads and is killed; after a restart the total is unchanged and
// every commit the child acknowledged is present. POSIX only.

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "txn_test_utils.hpp"

#ifndef _WIN32
#include <csignal>
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#ifndef _WIN32
namespace {

using chunkdb::ChunkCoord;
using chunkdb::ChunkStore;
using chunkdb::DurabilityMode;
using chunkdb::txn_test::Config;
using chunkdb::txn_test::HasTxnIntent;
using chunkdb::txn_test::ReadCounter;
using chunkdb::txn_test::Transfer;
using chunkdb::txn_test::WriteCounter;

constexpr std::int64_t kSide = 4;  // 16 counters in a 4x4 square of chunks
constexpr std::uint32_t kStart = 1000;
constexpr int kThreads = 4;

std::vector<ChunkCoord> Counters() {
    std::vector<ChunkCoord> coords;
    for (std::int64_t x = 0; x < kSide; ++x) {
        for (std::int64_t y = 0; y < kSide; ++y) {
            coords.push_back(ChunkCoord{x, y});
        }
    }
    return coords;
}

const char* ModeArg(DurabilityMode mode) {
    return chunkdb::DurabilityModeName(mode);
}

DurabilityMode ParseMode(const std::string& text) {
    return chunkdb::ParseDurabilityMode(text);
}

// Transfers until killed. After each commit returns, its version and chunks
// go to the log: what the child acknowledged.
[[noreturn]] void ChildLoop(const std::filesystem::path& dir, DurabilityMode mode, const std::filesystem::path& log) {
    ChunkStore store(Config(dir, mode));
    const int fd = ::open(log.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    assert(fd >= 0);
    std::mutex log_mutex;
    const auto coords = Counters();
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            std::mt19937 random(static_cast<unsigned>(1234 + t));
            std::uniform_int_distribution<std::size_t> pick(0, coords.size() - 1U);
            std::uniform_int_distribution<std::uint32_t> amount(1, 50);
            while (true) {
                const auto from = coords[pick(random)];
                auto to = coords[pick(random)];
                if (to == from) {
                    continue;
                }
                const auto version = Transfer(store, from, to, amount(random));
                if (version == 0U) {
                    continue;
                }
                const std::string line = std::to_string(version) + " " + std::to_string(from.x) + " " +
                                         std::to_string(from.y) + " " + std::to_string(to.x) + " " +
                                         std::to_string(to.y) + "\n";
                std::lock_guard lock(log_mutex);
                const auto written = ::write(fd, line.data(), line.size());
                assert(written == static_cast<ssize_t>(line.size()));
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    std::_Exit(0);
}

void Validate(const std::filesystem::path& dir, DurabilityMode mode, const std::filesystem::path& log) {
    ChunkStore store(Config(dir, mode));
    assert(!HasTxnIntent(dir));
    std::uint64_t total = 0;
    for (const auto& coord : Counters()) {
        total += ReadCounter(store, coord);
    }
    if (total != kStart * Counters().size()) {
        std::fprintf(stderr, "total %llu after the kill\n", static_cast<unsigned long long>(total));
        assert(false);
    }
    // Every acknowledged commit is there: its chunks are at its version or
    // past it.
    std::ifstream in(log);
    std::uint64_t version = 0;
    ChunkCoord from;
    ChunkCoord to;
    std::size_t acknowledged = 0;
    while (in >> version >> from.x >> from.y >> to.x >> to.y) {
        assert(store.GetChunkVersion(from.x, from.y) >= version);
        assert(store.GetChunkVersion(to.x, to.y) >= version);
        ++acknowledged;
    }
    assert(acknowledged > 0U);
    // The store goes on.
    (void)Transfer(store, ChunkCoord{0, 0}, ChunkCoord{1, 1}, 5);
}

void RunKill(const char* executable, DurabilityMode mode, int run_ms) {
    chunkdb::test::ScopedTempDir dir("chunkdb-txn-kill");
    const auto data_dir = dir.path() / "data";
    const auto log = dir.path() / "acknowledged.log";
    {
        ChunkStore store(Config(data_dir, mode));
        for (const auto& coord : Counters()) {
            WriteCounter(store, coord, kStart);
        }
        store.WalBarrier();
    }
    const pid_t child = fork();
    assert(child >= 0);
    if (child == 0) {
        execl(executable, executable, "--child", data_dir.c_str(), ModeArg(mode), log.c_str(), nullptr);
        _exit(127);
    }
    // Wait until it acknowledged something, then let it run a while.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (!std::filesystem::exists(log) || std::filesystem::file_size(log) == 0U) {
        assert(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(run_ms));
    kill(child, SIGKILL);
    int status = 0;
    waitpid(child, &status, 0);
    assert(WIFSIGNALED(status));
    Validate(data_dir, mode, log);
    Validate(data_dir, mode, log);
}

}  // namespace
#endif  // !_WIN32

int main(int argc, char** argv) {
#ifdef _WIN32
    (void)argc;
    (void)argv;
    return 0;
#else
    if (argc == 5 && std::string(argv[1]) == "--child") {
        ChildLoop(argv[2], ParseMode(argv[3]), argv[4]);
    }
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        for (const int run_ms : {100, 400}) {
            RunKill(argv[0], mode, run_ms);
        }
    }
    RunKill(argv[0], DurabilityMode::kFsyncCheckpoint, 250);
    return 0;
#endif
}
