// Snapshot generations cover active artifact publication only. Completed
// writes are readable without relying on a background closer thread.
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#include <sys/wait.h>
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

// Freeze the former idle publication timer with its existing test control.
// A completed publication must be stable without scheduling another thread.
void TestCompletedWriteIsStableWithoutACloser() {
    const auto data_dir = TempDataDir("completed-write");
    const auto config = BuildConfig(data_dir);
    {
        chunkdb::ChunkStore writer(config);
        writer.SetSnapshotGenerationLingerForTests(kForeverMs, kUnboundedBrackets);
        const auto start = std::chrono::steady_clock::now();
        writer.SetBlockBits(0, 0, "11110000");
        const auto returned = std::chrono::steady_clock::now();
        std::cerr << "completed-write: SetBlockBits returned after "
                  << std::chrono::duration_cast<std::chrono::microseconds>(returned - start).count()
                  << "us; persisted generation=" << PersistedGeneration(data_dir)
                  << "; pending=" << writer.SnapshotGenerationLingerPendingForTests() << '\n';
        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        assert(reader.GetBlockBits(0, 0) == "11110000");
        assert((PersistedGeneration(data_dir) & 1U) == 0U);
        assert(!writer.SnapshotGenerationLingerPendingForTests());
        std::cerr << "completed-write: stable reader completed after "
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - returned).count()
                  << "us; no closer release\n";
    }
    RemoveAllWithRetry(data_dir);
}


void TestEvictedWritesPublishStableGenerations() {
    const auto data_dir = TempDataDir("eviction");
    {
        chunkdb::ChunkStore writer(BuildConfig(data_dir));
        for (int i = 0; i < 64; ++i) {
            writer.SetBlockBits(i, 0, "11110000");
            assert((PersistedGeneration(data_dir) & 1U) == 0U);
        }
        assert(writer.RuntimeStats().evictions > 0);
        assert(writer.SnapshotGenerationOddPublicationsForTests() ==
               writer.SnapshotGenerationEvenPublicationsForTests());
        chunkdb::ChunkStore reader(ReadOnlyConfig(BuildConfig(data_dir)));
        assert(reader.GetBlockBits(0, 0) == "11110000");
        assert(reader.GetBlockBits(63, 0) == "11110000");
    }
    RemoveAllWithRetry(data_dir);
}

// The paused outer conditional publication contains a nested WAL publication.
// A second chunk's completed write must not close the first one's epoch.
void TestConcurrentAndNestedPublicationsStayBracketed() {
    const auto data_dir = TempDataDir("concurrent-nested");
    const auto config = BuildConfig(data_dir);
    {
        chunkdb::ChunkStore writer(config);
        writer.SetBlockBits(0, 0, "11110000");
        const auto version = writer.GetChunkVersion(0, 0);
        const auto odd_before = writer.SnapshotGenerationOddPublicationsForTests();
        const auto even_before = writer.SnapshotGenerationEvenPublicationsForTests();
        writer.ArmConditionalMutationPauseForTests(
            chunkdb::ConditionalMutationPausePoint::kAfterWalAppend);
        std::exception_ptr mutation_error;
        std::thread mutation([&] {
            try {
                const auto result = writer.CasChunkState(0, 0, version,
                    std::string(128, '1'), std::string(16, '1'));
                assert(result.ok);
            } catch (...) { mutation_error = std::current_exception(); }
        });
        if (!writer.WaitForConditionalMutationPauseForTests()) {
            writer.ResumeConditionalMutationForTests();
            mutation.join();
            throw std::runtime_error("concurrent-nested publication: timed out waiting for conditional WAL pause");
        }
        try {
            assert((PersistedGeneration(data_dir) & 1U) != 0U);
            writer.SetBlockBits(4, 0, "00001111");
            assert((PersistedGeneration(data_dir) & 1U) != 0U);
            assert(writer.SnapshotGenerationOddPublicationsForTests() == odd_before + 1);
            assert(writer.SnapshotGenerationEvenPublicationsForTests() == even_before);
            bool failed_closed = false;
            try {
                chunkdb::ChunkStore reader(ReadOnlyConfig(config));
                (void)reader.GetBlockBits(4, 0);
            } catch (const std::exception& error) {
                failed_closed = std::string(error.what()).find("remained unstable after") != std::string::npos;
            }
            assert(failed_closed);
        } catch (...) {
            writer.ResumeConditionalMutationForTests();
            mutation.join();
            throw;
        }
        writer.ResumeConditionalMutationForTests();
        mutation.join();
        if (mutation_error) std::rethrow_exception(mutation_error);
        assert((PersistedGeneration(data_dir) & 1U) == 0U);
        assert(writer.SnapshotGenerationOddPublicationsForTests() == odd_before + 1);
        assert(writer.SnapshotGenerationEvenPublicationsForTests() == even_before + 1);
        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        assert(reader.GetBlockBits(0, 0) == "11111111");
        assert(reader.GetBlockBits(4, 0) == "00001111");
    }
    RemoveAllWithRetry(data_dir);
}

void TestBarrierAndCloseLeaveStableGenerations() {
    const auto data_dir = TempDataDir("barrier-close");
    auto config = BuildConfig(data_dir);
    config.wal_group_commit_updates = 1000;
    {
        chunkdb::ChunkStore writer(config);
        WriteEvictingChunks(&writer, 32);
        writer.WalBarrier();
        assert((PersistedGeneration(data_dir) & 1U) == 0U);
        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        assert(reader.GetBlockBits(31, 0) == "11110000");
    }
    assert((PersistedGeneration(data_dir) & 1U) == 0U);
    {
        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        assert(reader.GetBlockBits(0, 0) == "11110000");
    }
    RemoveAllWithRetry(data_dir);
}

int RunPublicationCrashChild(const std::filesystem::path& data_dir) {
    chunkdb::ChunkStore writer(BuildConfig(data_dir));
    writer.SetBlockBits(0, 0, "11110000");
#ifdef _WIN32
    _putenv_s("CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_AFTER_BEGIN_ONCE", "1");
#else
    setenv("CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_AFTER_BEGIN_ONCE", "1", 1);
#endif
    writer.SetBlockBits(4, 0, "00001111");
    return 0;
}

void TestCrashDuringPublicationFailsClosedAndRecovers(const std::string& executable) {
    const auto data_dir = TempDataDir("publication-crash");
    std::string command = "\"" + executable + "\" --publication-crash-child \"" + data_dir.string() + "\"";
#ifdef _WIN32
    command = "\"" + command + "\"";
#endif
    const int status = std::system(command.c_str());
#ifdef _WIN32
    assert(status == 86);
#else
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 86);
#endif
    assert((PersistedGeneration(data_dir) & 1U) != 0U);
    bool failed_closed = false;
    try {
        chunkdb::ChunkStore reader(ReadOnlyConfig(BuildConfig(data_dir)));
        (void)reader.GetBlockBits(0, 0);
    } catch (const std::exception& error) {
        failed_closed = std::string(error.what()).find("remained unstable after") != std::string::npos;
    }
    assert(failed_closed);
    {
        chunkdb::ChunkStore recovered(BuildConfig(data_dir));
        assert(recovered.GetBlockBits(0, 0) == "11110000");
        assert((PersistedGeneration(data_dir) & 1U) == 0U);
    }
    {
        chunkdb::ChunkStore reader(ReadOnlyConfig(BuildConfig(data_dir)));
        assert(reader.GetBlockBits(0, 0) == "11110000");
    }
    RemoveAllWithRetry(data_dir);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 3 && std::string(argv[1]) == "--publication-crash-child") {
        return RunPublicationCrashChild(argv[2]);
    }
    TestCompletedWriteIsStableWithoutACloser();
    TestEvictedWritesPublishStableGenerations();
    TestConcurrentAndNestedPublicationsStayBracketed();
    TestBarrierAndCloseLeaveStableGenerations();
    TestCrashDuringPublicationFailsClosedAndRecovers(argv[0]);
    return 0;
}
