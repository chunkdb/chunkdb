// A process that crashes inside a commit (docs/TRANSACTIONS_DESIGN.md): at
// every failpoint a child process ends abruptly, and after the next start
// every written chunk shows all of the transaction or none of it. A crash
// during that recovery is resolved by the start after it.

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "chunkdb/chunk_store.hpp"
#include "txn_test_utils.hpp"

namespace {

using chunkdb::ChunkCoord;
using chunkdb::ChunkStore;
using chunkdb::DurabilityMode;
using chunkdb::TxnChunkWrite;
using chunkdb::test::ScopedTempDir;
using chunkdb::txn_test::Config;
using chunkdb::txn_test::HasTxnIntent;
using chunkdb::txn_test::kTxnDuration;
using chunkdb::txn_test::ReadCounter;
using chunkdb::txn_test::SetCounter;
using chunkdb::txn_test::Throws;
using chunkdb::txn_test::Transfer;
using chunkdb::txn_test::WriteCounter;

constexpr ChunkCoord kA{0, 0};
constexpr ChunkCoord kB{1, 0};
// No WAL before the transaction.
constexpr ChunkCoord kC{0, 1};
constexpr int kCrashExit = 86;

chunkdb::StoreConfig CrashConfig(const std::filesystem::path& dir, DurabilityMode mode) {
    auto config = Config(dir, mode);
    // No checkpoint between the seed and the crash: the WALs stay.
    config.checkpoint_update_interval = 1000;
    config.checkpoint_wal_bytes = 1U << 20U;
    return config;
}

const char* ModeArg(DurabilityMode mode) {
    return chunkdb::DurabilityModeName(mode);
}

DurabilityMode ParseMode(const std::string& text) {
    return chunkdb::ParseDurabilityMode(text);
}

constexpr DurabilityMode kModes[] = {
    DurabilityMode::kRelaxed,
    DurabilityMode::kFsyncWal,
    DurabilityMode::kFsyncCheckpoint,
};
// A relaxed write the commit has to flush from its group-commit batch at its
// boundary: A holds it instead of the seeded 1.
constexpr std::uint32_t kStagedA = 5;

int ExitCodeOf(int status) {
#ifdef _WIN32
    return status;
#else
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
#endif
}

int RunChild(const std::string& executable, const std::vector<std::string>& args) {
    std::string command = "\"" + executable + "\"";
    for (const auto& arg : args) {
        command += " \"" + arg + "\"";
    }
#ifdef _WIN32
    // cmd strips the outermost quotes of the whole line.
    command = "\"" + command + "\"";
#endif
    return ExitCodeOf(std::system(command.c_str()));
}

// Commits A=10, B=20, C=30 with the failpoint armed; the commit never
// returns. With `staged`, a relaxed write of A waits in its batch first.
int RunCommitChild(
    const std::filesystem::path& dir,
    DurabilityMode mode,
    const char* failpoint,
    const char* value,
    bool staged) {
    ChunkStore store(CrashConfig(dir, mode));
    if (staged) {
        WriteCounter(store, kA, kStagedA);
    }
    auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
    std::vector<TxnChunkWrite> writes;
    for (const auto& [coord, counter] : std::vector<std::pair<ChunkCoord, std::uint32_t>>{{kA, 10}, {kB, 20}, {kC, 30}}) {
        auto state = store.ReadChunkStateAt(*snapshot, coord.x, coord.y);
        SetCounter(&state, counter);
        writes.push_back(TxnChunkWrite{.coord = coord, .state = std::move(state)});
    }
    chunkdb::txn_test::ScopedEnv armed(failpoint, value);
    (void)store.CommitTransaction(*snapshot, {kA, kB, kC}, std::move(writes));
    return 3;
}

// Opens the store with the recovery failpoint armed.
int RunRecoveryChild(const std::filesystem::path& dir, DurabilityMode mode) {
    chunkdb::txn_test::ScopedEnv armed("CHUNKDB_FAILPOINT_CRASH_TXN_RECOVERY_AFTER_TRUNCATE_ONCE", "1");
    ChunkStore store(CrashConfig(dir, mode));
    return 3;
}

void Seed(const std::filesystem::path& dir, DurabilityMode mode) {
    ChunkStore store(CrashConfig(dir, mode));
    WriteCounter(store, kA, 1);
    WriteCounter(store, kB, 2);
    store.WalBarrier();
}

// All of the transaction or none of it, then the store goes on.
void ExpectAllOrNone(const std::filesystem::path& dir, DurabilityMode mode, bool committed, std::uint32_t base_a = 1) {
    {
        ChunkStore store(CrashConfig(dir, mode));
        assert(!HasTxnIntent(dir));
        const auto a = ReadCounter(store, kA);
        const auto b = ReadCounter(store, kB);
        const auto c = ReadCounter(store, kC);
        if (committed) {
            assert(a == 10U && b == 20U && c == 30U);
            const auto version = store.GetChunkVersion(kA.x, kA.y);
            assert(store.GetChunkVersion(kB.x, kB.y) == version);
            assert(store.GetChunkVersion(kC.x, kC.y) == version);
        } else {
            assert(a == base_a && b == 2U && c == 0U);
        }
        (void)Transfer(store, kA, kC, 1);
        WriteCounter(store, kB, 77);
    }
    ChunkStore again(CrashConfig(dir, mode));
    assert(ReadCounter(again, kA) + ReadCounter(again, kC) == (committed ? 40U : base_a));
    assert(ReadCounter(again, kB) == 77U);
    assert(!HasTxnIntent(dir));
}

void ExpectReadOnlyFailsClosed(const std::filesystem::path& dir, DurabilityMode mode) {
    auto config = CrashConfig(dir, mode);
    config.access_mode = chunkdb::AccessMode::kReadOnly;
    assert(Throws([&] {
        ChunkStore reader(config);
        (void)ReadCounter(reader, kA);
    }));
}

void TestCrashAtEveryFailpoint(const std::string& executable) {
    struct Case {
        const char* failpoint;
        const char* value;
        bool committed;
    };
    const std::vector<Case> cases = {
        {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_INTENT_PUBLISH_ONCE", "1", false},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "0", false},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "1", false},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "2", false},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "3", false},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_BEFORE_COMMIT_PUBLISH_ONCE", "1", false},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_COMMIT_PUBLISH_ONCE", "1", true},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_BEFORE_INTENT_UNLINK_ONCE", "1", true},
    };
    const auto run = [&](DurabilityMode mode, const Case& item, bool staged) {
        ScopedTempDir dir("chunkdb-txn-crash");
        Seed(dir.path(), mode);
        const int code = RunChild(
            executable,
            {"--commit-crash-child", dir.path().string(), ModeArg(mode), item.failpoint, item.value,
             staged ? "staged" : "seeded"});
        if (code != kCrashExit) {
            std::fprintf(stderr, "child for %s=%s exited with %d\n", item.failpoint, item.value, code);
            assert(false);
        }
        // The crash left the generation odd: readers fail closed until a
        // writer recovers.
        ExpectReadOnlyFailsClosed(dir.path(), mode);
        ExpectAllOrNone(dir.path(), mode, item.committed, staged ? kStagedA : 1U);
    };
    for (const auto mode : kModes) {
        for (const auto& item : cases) {
            run(mode, item, false);
        }
    }
    // The staged write reaches the WAL before the boundary, so it stays
    // whether the transaction does or not.
    for (const auto& item : cases) {
        if (std::string(item.failpoint) == "CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES" ||
            std::string(item.failpoint) == "CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_COMMIT_PUBLISH_ONCE") {
            run(DurabilityMode::kRelaxed, item, true);
        }
    }
}

// A crash while recovery truncates the WALs of an uncommitted transaction:
// the intent is still there, and the next start finishes the job.
void TestCrashDuringRecovery(const std::string& executable) {
    for (const auto mode : kModes) {
        ScopedTempDir dir("chunkdb-txn-crash-recovery");
        Seed(dir.path(), mode);
        assert(RunChild(
                   executable,
                   {"--commit-crash-child", dir.path().string(), ModeArg(mode),
                    "CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "3", "seeded"}) == kCrashExit);
        assert(HasTxnIntent(dir.path()));
        assert(RunChild(executable, {"--recovery-crash-child", dir.path().string(), ModeArg(mode)}) == kCrashExit);
        assert(HasTxnIntent(dir.path()));
        ExpectAllOrNone(dir.path(), mode, false);
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 7 && std::string(argv[1]) == "--commit-crash-child") {
        return RunCommitChild(argv[2], ParseMode(argv[3]), argv[4], argv[5], std::string(argv[6]) == "staged");
    }
    if (argc == 4 && std::string(argv[1]) == "--recovery-crash-child") {
        return RunRecoveryChild(argv[2], ParseMode(argv[3]));
    }
    TestCrashAtEveryFailpoint(argv[0]);
    TestCrashDuringRecovery(argv[0]);
    return 0;
}
