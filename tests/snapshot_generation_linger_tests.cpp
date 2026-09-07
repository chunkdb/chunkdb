// Coverage for the snapshot-generation bracket "linger": the even (stable)
// record is published lazily so consecutive transitions - above all a cache
// eviction pass - run inside one already-open odd epoch instead of paying a
// three-sync bracket each.
//
// The properties under test are the ones the contract depends on:
//   * consecutive transitions coalesce into one odd publication
//   * the epoch is bounded in both bracket count and wall time
//   * WalBarrier and store close leave a stable (even) generation behind
//   * a read-only reader rides out a long odd epoch instead of failing
//   * a crash while lingering costs nothing beyond ordinary recovery

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace {

constexpr std::uint64_t kForeverMs = 3'600'000;
constexpr std::size_t kUnboundedBrackets = 1'000'000;

std::uint64_t CurrentPid() {
#ifdef _WIN32
    return static_cast<std::uint64_t>(::GetCurrentProcessId());
#else
    return static_cast<std::uint64_t>(::getpid());
#endif
}

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    static std::atomic<std::uint64_t> counter{0};
    const std::uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);
    return base / ("chunkdb-linger-" + suffix + "-" + std::to_string(tick) + "-" +
                   std::to_string(CurrentPid()) + "-" + std::to_string(seq));
}

void RemoveAllWithRetry(const std::filesystem::path& path) {
    for (int attempt = 0; attempt < 40; ++attempt) {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
        std::error_code exists_ec;
        if (!std::filesystem::exists(path, exists_ec) && !exists_ec) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    throw std::runtime_error("failed to remove temp dir: " + path.string());
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
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 8,
        .allow_multiple_processes = true,
    };
}

chunkdb::StoreConfig ReadOnlyConfig(chunkdb::StoreConfig config) {
    config.access_mode = chunkdb::AccessMode::kReadOnly;
    return config;
}

// Reads the persisted record rather than the in-memory value, so "the store
// left a stable generation behind" is checked the way a separate process
// would see it.
std::uint64_t PersistedGeneration(const std::filesystem::path& data_dir) {
    const auto path = data_dir / "chunkdb.snapshot";
    std::ifstream in(path, std::ios::binary);
    assert(in.is_open());
    const std::vector<std::uint8_t> bytes(
        (std::istreambuf_iterator<char>(in)),
        std::istreambuf_iterator<char>());
    // magic "CKSG" (4) + little-endian generation (8) + CRC32 (4).
    assert(bytes.size() == 16);
    assert(bytes[0] == 'C' && bytes[1] == 'K' && bytes[2] == 'S' &&
           bytes[3] == 'G');
    std::uint64_t generation = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        generation |= static_cast<std::uint64_t>(bytes[4 + i]) << (8U * i);
    }
    return generation;
}

// Writes distinct chunks well past max_loaded_chunks so the eviction path -
// the transition issue #24 is about - runs many times.
void WriteEvictingChunks(chunkdb::ChunkStore* store, int count) {
    for (int i = 0; i < count; ++i) {
        store->SetBlockBits(i, 0, "11110000");
    }
}

// Consecutive transitions must share one durable odd publication. With an
// effectively unbounded window the whole run is a single epoch: exactly one
// odd record for the entire workload, and no even record until something
// closes the bracket.
void TestConsecutiveTransitionsShareOneBracket() {
    const auto data_dir = TempDataDir("coalesce");
    const auto config = BuildConfig(data_dir);
    {
        chunkdb::ChunkStore store(config);
        store.SetSnapshotGenerationLingerForTests(
            kForeverMs, kUnboundedBrackets);

        WriteEvictingChunks(&store, 64);

        const auto stats = store.RuntimeStats();
        assert(stats.evictions > 0);
        assert(store.SnapshotGenerationOddPublicationsForTests() == 1);
        assert(store.SnapshotGenerationEvenPublicationsForTests() == 0);
        assert(store.SnapshotGenerationLingerPendingForTests());
        // The pre-fix cost was one bracket (three durable syncs) per
        // eviction; anything near that ratio is the regression.
        assert(
            store.SnapshotGenerationOddPublicationsForTests() <
            stats.evictions);
    }
    RemoveAllWithRetry(data_dir);
}

// The epoch is bounded by bracket count, so a writer that never pauses cannot
// hold readers off on an unbounded odd generation.
void TestBracketBudgetClosesTheEpoch() {
    // Baseline: one bracket per transition, i.e. what the workload costs
    // without any coalescing at all.
    const auto baseline_dir = TempDataDir("bracket-budget-baseline");
    std::uint64_t baseline_odd = 0;
    {
        chunkdb::ChunkStore store(BuildConfig(baseline_dir));
        store.SetSnapshotGenerationLingerForTests(0, 0);
        WriteEvictingChunks(&store, 64);
        baseline_odd = store.SnapshotGenerationOddPublicationsForTests();
        assert(baseline_odd > 8);
    }
    RemoveAllWithRetry(baseline_dir);

    const auto data_dir = TempDataDir("bracket-budget");
    {
        chunkdb::ChunkStore store(BuildConfig(data_dir));
        store.SetSnapshotGenerationLingerForTests(kForeverMs, 4);
        WriteEvictingChunks(&store, 64);

        const auto odd = store.SnapshotGenerationOddPublicationsForTests();
        const auto even = store.SnapshotGenerationEvenPublicationsForTests();
        // Strictly more than one epoch: the count budget really did force
        // the epoch closed and reopened, so a non-stop writer cannot hold
        // readers off forever on bracket count alone.
        assert(odd > 1);
        // Every closed epoch published exactly one even record. The final
        // epoch is either still lingering (even == odd - 1) or was itself
        // closed by the budget (even == odd).
        assert(even + 1 == odd || even == odd);
        // ...and still far cheaper than a bracket per transition.
        assert(odd * 3 <= baseline_odd);
    }
    RemoveAllWithRetry(data_dir);
}

// The epoch is also bounded in wall time: an idle store must settle on an
// even generation on its own, with no further writes and no close.
void TestLingerWindowClosesTheEpochWhileIdle() {
    const auto data_dir = TempDataDir("window");
    const auto config = BuildConfig(data_dir);
    {
        chunkdb::ChunkStore store(config);
        store.SetSnapshotGenerationLingerForTests(20, kUnboundedBrackets);
        store.SetBlockBits(0, 0, "11110000");

        bool settled = false;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            if (!store.SnapshotGenerationLingerPendingForTests() &&
                (store.SnapshotGenerationForTests() & 1U) == 0U) {
                settled = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        assert(settled);
        assert(store.SnapshotGenerationEvenPublicationsForTests() >= 1);

        // And an ordinary read-only load succeeds against the settled store.
        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        assert(reader.GetBlockBits(0, 0) == "11110000");
    }
    RemoveAllWithRetry(data_dir);
}

// WALFLUSH coalesces every per-chunk flush into one bracket and then closes
// it, so a barrier leaves readers a stable generation instead of an odd epoch
// that only a timer would resolve.
void TestWalBarrierClosesTheBracket() {
    const auto data_dir = TempDataDir("barrier");
    const auto config = BuildConfig(data_dir);
    {
        chunkdb::ChunkStore store(config);
        store.SetSnapshotGenerationLingerForTests(
            kForeverMs, kUnboundedBrackets);

        WriteEvictingChunks(&store, 32);
        assert(store.SnapshotGenerationLingerPendingForTests());

        const auto odd_before =
            store.SnapshotGenerationOddPublicationsForTests();
        store.WalBarrier();

        assert(!store.SnapshotGenerationLingerPendingForTests());
        assert((store.SnapshotGenerationForTests() & 1U) == 0U);
        assert((PersistedGeneration(data_dir) & 1U) == 0U);
        // The barrier's own per-chunk flushes rode the same epoch.
        assert(
            store.SnapshotGenerationOddPublicationsForTests() == odd_before);

        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        assert(reader.GetBlockBits(0, 0) == "11110000");
    }
    RemoveAllWithRetry(data_dir);
}

// A cleanly closed store must not leave an odd generation on disk: the next
// reader would otherwise fail closed against a perfectly coherent directory.
void TestStoreCloseClosesTheBracket() {
    const auto data_dir = TempDataDir("close");
    const auto config = BuildConfig(data_dir);
    {
        chunkdb::ChunkStore store(config);
        store.SetSnapshotGenerationLingerForTests(
            kForeverMs, kUnboundedBrackets);
        WriteEvictingChunks(&store, 32);
        assert(store.SnapshotGenerationLingerPendingForTests());
        assert((store.SnapshotGenerationForTests() & 1U) != 0U);
    }

    assert((PersistedGeneration(data_dir) & 1U) == 0U);
    {
        chunkdb::ChunkStore reader(ReadOnlyConfig(BuildConfig(data_dir)));
        assert(reader.GetBlockBits(0, 0) == "11110000");
    }
    RemoveAllWithRetry(data_dir);
}

// The reader-side half of the fix. Before it, LoadStableReadOnlyChunkDiskSnapshot
// spun eight times without sleeping and threw, so any deliberately lengthened
// odd epoch became a hard chunk-load failure in a read-only process.
void TestReadOnlyReaderRidesOutALongOddEpoch() {
    const auto data_dir = TempDataDir("reader-backoff");
    const auto config = BuildConfig(data_dir);
    {
        chunkdb::ChunkStore store(config);
        store.SetSnapshotGenerationLingerForTests(
            kForeverMs, kUnboundedBrackets);
        store.SetBlockBits(0, 0, "11110000");
        assert(store.SnapshotGenerationLingerPendingForTests());

        // Deterministic: the epoch stays odd until this test closes it by
        // hand, well after the reader has burned its sleep-free attempts.
        std::string bits;
        std::exception_ptr reader_error;
        std::thread reader_thread([&] {
            try {
                chunkdb::ChunkStore reader(ReadOnlyConfig(config));
                bits = reader.GetBlockBits(0, 0);
            } catch (...) {
                reader_error = std::current_exception();
            }
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(60));
        assert(store.SnapshotGenerationLingerPendingForTests());
        store.FlushSnapshotGenerationLingerForTests();

        reader_thread.join();
        if (reader_error != nullptr) {
            std::rethrow_exception(reader_error);
        }
        assert(bits == "11110000");
    }
    RemoveAllWithRetry(data_dir);
}

// The backoff is bounded, not unbounded: an epoch that never resolves still
// fails the load closed rather than hanging the reader.
void TestReadOnlyReaderStillFailsClosedOnAnUnresolvedEpoch() {
    const auto data_dir = TempDataDir("reader-fails-closed");
    const auto config = BuildConfig(data_dir);
    {
        chunkdb::ChunkStore store(config);
        store.SetSnapshotGenerationLingerForTests(
            kForeverMs, kUnboundedBrackets);
        store.SetBlockBits(0, 0, "11110000");
        assert(store.SnapshotGenerationLingerPendingForTests());

        bool failed_closed = false;
        try {
            chunkdb::ChunkStore reader(ReadOnlyConfig(config));
            (void)reader.GetBlockBits(0, 0);
        } catch (const std::exception& error) {
            failed_closed =
                std::string(error.what()).find("remained unstable after") !=
                std::string::npos;
        }
        assert(failed_closed);
    }
    RemoveAllWithRetry(data_dir);
}

int RunLingerCrashChild(const std::filesystem::path& data_dir) {
    const auto config = BuildConfig(data_dir);
    chunkdb::ChunkStore store(config);
    store.SetSnapshotGenerationLingerForTests(kForeverMs, kUnboundedBrackets);
    WriteEvictingChunks(&store, 32);
    store.SetBlockBits(0, 0, "00001111");
    assert(store.SnapshotGenerationLingerPendingForTests());
    assert((store.SnapshotGenerationForTests() & 1U) != 0U);
    // Abrupt exit while the even record is deliberately unpublished. No
    // destructor, no close, no even publication.
    std::_Exit(88);
}

// A crash during a linger must cost nothing beyond ordinary recovery: the
// generation is odd (readers fail closed), and the next writer start raises a
// fresh odd generation and republishes even, exactly as for a crash inside a
// real transition.
void TestCrashWhileLingeringRecovers(const std::string& executable) {
    const auto data_dir = TempDataDir("crash");
    const auto config = BuildConfig(data_dir);

    std::string command =
        "\"" + executable + "\" --linger-crash-child \"" +
        data_dir.string() + "\"";
#ifdef _WIN32
    command = "\"" + command + "\"";
#endif
    const int status = std::system(command.c_str());
    assert(status != 0);

    // Crashed mid-linger: the persisted generation is odd and read-only
    // access fails closed rather than latching an unbracketed state.
    assert((PersistedGeneration(data_dir) & 1U) != 0U);
    {
        bool failed_closed = false;
        try {
            chunkdb::ChunkStore reader(ReadOnlyConfig(config));
            (void)reader.GetBlockBits(0, 0);
        } catch (const std::exception& error) {
            const std::string what = error.what();
            failed_closed =
                what.find("remained unstable after") != std::string::npos ||
                what.find("snapshot generation") != std::string::npos;
        }
        assert(failed_closed);
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "00001111");
        assert(recovered.GetBlockBits(31, 0) == "11110000");
        assert((recovered.SnapshotGenerationForTests() & 1U) == 0U);
    }

    assert((PersistedGeneration(data_dir) & 1U) == 0U);
    {
        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        assert(reader.GetBlockBits(0, 0) == "00001111");
    }
    RemoveAllWithRetry(data_dir);
}

// Turning the linger off must restore immediate publication, so the knob is
// a real escape hatch for the crash/failpoint tests that rely on it.
void TestDisabledLingerPublishesImmediately() {
    const auto data_dir = TempDataDir("disabled");
    const auto config = BuildConfig(data_dir);
    {
        chunkdb::ChunkStore store(config);
        store.SetSnapshotGenerationLingerForTests(0, 0);
        store.SetBlockBits(0, 0, "11110000");
        assert(!store.SnapshotGenerationLingerPendingForTests());
        assert((store.SnapshotGenerationForTests() & 1U) == 0U);
        assert(store.SnapshotGenerationEvenPublicationsForTests() >= 1);
        assert(
            store.SnapshotGenerationEvenPublicationsForTests() ==
            store.SnapshotGenerationOddPublicationsForTests());
    }
    RemoveAllWithRetry(data_dir);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 3 && std::string(argv[1]) == "--linger-crash-child") {
        return RunLingerCrashChild(argv[2]);
    }
    TestConsecutiveTransitionsShareOneBracket();
    TestBracketBudgetClosesTheEpoch();
    TestLingerWindowClosesTheEpochWhileIdle();
    TestWalBarrierClosesTheBracket();
    TestStoreCloseClosesTheBracket();
    TestReadOnlyReaderRidesOutALongOddEpoch();
    TestReadOnlyReaderStillFailsClosedOnAnUnresolvedEpoch();
    TestDisabledLingerPublishesImmediately();
    TestCrashWhileLingeringRecovers(argv[0]);
    return 0;
}
