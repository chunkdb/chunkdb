// A process that crashes inside a commit (docs/design/TRANSACTIONS_DESIGN.md): at
// every failpoint a child process ends abruptly, and after the next start
// every written chunk shows all of the transaction or none of it. A crash
// during that recovery is resolved by the start after it.

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "chunkdb/chunk_columns.hpp"
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

// A three-dimensional table with a chunk column: the transaction intent
// names three-axis chunks, and each commit frame carries metadata as well as
// blocks. A crash leaves both all-or-none.
constexpr ChunkCoord kA3{0, 0, 1};
constexpr ChunkCoord kB3{1, 0, -1};
// No WAL before the transaction.
constexpr ChunkCoord kC3{0, 1, 2};

chunkdb::StoreConfig Crash3DConfig(const std::filesystem::path& dir, DurabilityMode mode) {
    auto config = CrashConfig(dir, mode);
    config.geometry.dims = 3U;
    config.geometry.chunk_depth_blocks = 2U;
    config.geometry.large_chunk_depth_chunks = 2U;
    chunkdb::TableSchema metadata;
    metadata.next_column_id = 2;
    metadata.columns = {{.id = 1, .name = "tag", .type = {chunkdb::ColumnKind::kUnsigned, 8}, .nullable = true,
        .default_value = {}}};
    config.chunk_schema = metadata;
    return config;
}

std::uint32_t Counter3D(ChunkStore& store, const ChunkCoord& coord) {
    return chunkdb::txn_test::CounterOf(store.ReadChunkState(coord));
}

std::optional<std::uint64_t> Tag(ChunkStore& store, const ChunkCoord& coord) {
    const auto state = store.ReadChunkState(coord);
    if (!state.chunk_columns) return std::nullopt;
    const auto values = chunkdb::DecodeChunkColumns(store.geometry().chunk_schema(), state.chunk_columns->bytes);
    if (std::holds_alternative<std::monostate>(values[0])) return std::nullopt;
    return std::get<std::uint64_t>(values[0]);
}

void Seed3D(const std::filesystem::path& dir, DurabilityMode mode) {
    ChunkStore store(Crash3DConfig(dir, mode));
    store.SetBlockBits3D(kA3.x * 4, kA3.y * 4, kA3.z * 2, chunkdb::txn_test::CounterBits(1));
    store.SetBlockBits3D(kB3.x * 4, kB3.y * 4, kB3.z * 2, chunkdb::txn_test::CounterBits(2));
    store.WalBarrier();
}

// Commits A=10, B=20, C=30 with tags 11, 21 and 31 under the armed failpoint.
int RunCommit3DChild(const std::filesystem::path& dir, DurabilityMode mode, const char* failpoint, const char* value) {
    ChunkStore store(Crash3DConfig(dir, mode));
    auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
    std::vector<TxnChunkWrite> writes;
    for (const auto& [coord, counter] : std::vector<std::pair<ChunkCoord, std::uint32_t>>{{kA3, 10}, {kB3, 20}, {kC3, 30}}) {
        auto state = store.ReadChunkStateAt(*snapshot, coord);
        SetCounter(&state, counter);
        state.chunk_columns = std::make_shared<const chunkdb::ChunkColumnState>(chunkdb::AssignChunkColumns(
            store.geometry().chunk_schema(), state.chunk_columns.get(),
            {{"tag", static_cast<std::uint64_t>(counter + 1U)}}, !state.written));
        writes.push_back(TxnChunkWrite{.coord = coord, .state = std::move(state)});
    }
    chunkdb::txn_test::ScopedEnv armed(failpoint, value);
    (void)store.CommitTransaction(*snapshot, {kA3, kB3, kC3}, std::move(writes));
    return 3;
}

void ExpectAllOrNone3D(const std::filesystem::path& dir, DurabilityMode mode, bool committed) {
    ChunkStore store(Crash3DConfig(dir, mode));
    assert(!HasTxnIntent(dir));
    if (committed) {
        assert(Counter3D(store, kA3) == 10U && Counter3D(store, kB3) == 20U && Counter3D(store, kC3) == 30U);
        assert(Tag(store, kA3) == 11U && Tag(store, kB3) == 21U && Tag(store, kC3) == 31U);
        const auto version = store.ReadChunkState(kA3).version;
        assert(store.ReadChunkState(kB3).version == version && store.ReadChunkState(kC3).version == version);
    } else {
        assert(Counter3D(store, kA3) == 1U && Counter3D(store, kB3) == 2U && Counter3D(store, kC3) == 0U);
        assert(!Tag(store, kA3) && !Tag(store, kB3) && !Tag(store, kC3));
        assert(!store.ReadChunkState(kC3).written);
    }
    store.SetBlockBits3D(kB3.x * 4, kB3.y * 4, kB3.z * 2, chunkdb::txn_test::CounterBits(77));
    assert(Counter3D(store, kB3) == 77U);
}

void TestDimensionalMetadataCrash(const std::string& executable) {
    const std::vector<std::pair<const char*, const char*>> before = {
        {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_INTENT_PUBLISH_ONCE", "1"},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "0"},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "1"},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "2"},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "3"},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_BEFORE_COMMIT_PUBLISH_ONCE", "1"},
    };
    const std::vector<std::pair<const char*, const char*>> after = {
        {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_COMMIT_PUBLISH_ONCE", "1"},
        {"CHUNKDB_FAILPOINT_CRASH_TXN_BEFORE_INTENT_UNLINK_ONCE", "1"},
    };
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) {
        for (const bool committed : {false, true}) {
            for (const auto& [failpoint, value] : committed ? after : before) {
                ScopedTempDir dir("chunkdb-txn-crash-3d");
                Seed3D(dir.path(), mode);
                const int code = RunChild(executable,
                    {"--commit-crash-3d-child", dir.path().string(), ModeArg(mode), failpoint, value});
                if (code != kCrashExit) {
                    std::fprintf(stderr, "3D child for %s=%s exited with %d\n", failpoint, value, code);
                    assert(false);
                }
                ExpectAllOrNone3D(dir.path(), mode, committed);
            }
        }
    }
}

// One write outside a transaction changes blocks and metadata of a 3D chunk under its rollback intent.
int RunAutocommit3DChild(const std::filesystem::path& dir, DurabilityMode mode, const char* failpoint) {
    ChunkStore store(Crash3DConfig(dir, mode));
    chunkdb::txn_test::ScopedEnv armed(failpoint, "1");
    (void)store.TransformChunkState(kA3, std::nullopt, false, [&](chunkdb::ChunkState& state) {
        SetCounter(&state, 10);
        state.chunk_columns = std::make_shared<const chunkdb::ChunkColumnState>(chunkdb::AssignChunkColumns(
            store.geometry().chunk_schema(), state.chunk_columns.get(), {{"tag", std::uint64_t{11U}}}, !state.written));
    });
    return 3;
}

bool HasRollbackIntent(const std::filesystem::path& dir) {
    for (const auto& entry : std::filesystem::recursive_directory_iterator(dir))
        if (entry.is_regular_file() && entry.path().extension() == ".rollback") return true;
    return false;
}

void TestDimensionalAutocommitMetadataCrash(const std::string& executable) {
    const std::vector<std::pair<const char*, bool>> boundaries = {
        {"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_INTENT_PUBLISH_ONCE", false},
        {"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_AFTER_INTENT_PUBLISH_ONCE", false},
        {"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_COMMIT_PUBLISH_ONCE", false},
        {"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_AFTER_COMMIT_PUBLISH_ONCE", true},
        {"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_INTENT_CLEAR_ONCE", true},
        {"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_AFTER_INTENT_UNLINK_ONCE", true},
    };
    for (const auto& [failpoint, committed] : boundaries) {
        ScopedTempDir dir("chunkdb-autocommit-crash-3d");
        Seed3D(dir.path(), DurabilityMode::kFsyncWal);
        const int code = RunChild(executable,
            {"--autocommit-crash-3d-child", dir.path().string(), ModeArg(DurabilityMode::kFsyncWal), failpoint});
        if (code != kCrashExit) {
            std::fprintf(stderr, "3D autocommit child for %s exited with %d\n", failpoint, code);
            assert(false);
        }
        ChunkStore store(Crash3DConfig(dir.path(), DurabilityMode::kFsyncWal));
        assert(!HasRollbackIntent(dir.path()));
        assert(Counter3D(store, kA3) == (committed ? 10U : 1U));
        assert(Tag(store, kA3) == (committed ? std::optional<std::uint64_t>{11U} : std::nullopt));
        assert(Counter3D(store, kB3) == 2U);
        store.SetBlockBits3D(kA3.x * 4, kA3.y * 4, kA3.z * 2, chunkdb::txn_test::CounterBits(77));
        assert(Counter3D(store, kA3) == 77U && Tag(store, kA3) == (committed ? std::optional<std::uint64_t>{11U} : std::nullopt));
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 7 && std::string(argv[1]) == "--commit-crash-child") {
        return RunCommitChild(argv[2], ParseMode(argv[3]), argv[4], argv[5], std::string(argv[6]) == "staged");
    }
    if (argc == 6 && std::string(argv[1]) == "--commit-crash-3d-child") {
        return RunCommit3DChild(argv[2], ParseMode(argv[3]), argv[4], argv[5]);
    }
    if (argc == 5 && std::string(argv[1]) == "--autocommit-crash-3d-child") {
        return RunAutocommit3DChild(argv[2], ParseMode(argv[3]), argv[4]);
    }
    if (argc == 4 && std::string(argv[1]) == "--recovery-crash-child") {
        return RunRecoveryChild(argv[2], ParseMode(argv[3]));
    }
    TestCrashAtEveryFailpoint(argv[0]);
    TestCrashDuringRecovery(argv[0]);
    TestDimensionalMetadataCrash(argv[0]);
    TestDimensionalAutocommitMetadataCrash(argv[0]);
    return 0;
}
