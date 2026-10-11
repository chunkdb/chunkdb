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

// Observe the actual live coordination bytes rather than a store's cached
// diagnostic counter. The checked CKSG record is now a writer-lifetime crash
// fence; its live session remains odd even between completed publications.
std::vector<std::uint8_t> ReadBytes(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in.is_open());
    return {(std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>()};
}

std::uint64_t ReadU64(const std::vector<std::uint8_t>& bytes, std::size_t offset) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i) value |= static_cast<std::uint64_t>(bytes[offset + i]) << (8U * i);
    return value;
}

std::uint64_t ObservedGeneration(const std::filesystem::path& data_dir) {
    const auto marker = ReadBytes(data_dir / "chunkdb.snapshot");
    assert(marker.size() == 16);
    const auto session = ReadU64(marker, 4);
    if ((session & 1U) == 0U) return 0;
    const auto live = ReadBytes(data_dir / "chunkdb.snapshot.live");
    assert(live.size() == 32);
    assert(live[0] == 'C' && live[1] == 'K' && live[2] == 'S' && live[3] == 'L');
    assert(ReadU64(live, 8) == session);
    return ReadU64(live, 24);
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
        const auto session_before = ReadBytes(data_dir / "chunkdb.snapshot");
        const auto start = std::chrono::steady_clock::now();
        writer.SetBlockBits(0, 0, "11110000");
        const auto returned = std::chrono::steady_clock::now();
        assert(ReadBytes(data_dir / "chunkdb.snapshot") == session_before);
        std::cerr << "completed-write: SetBlockBits returned after "
                  << std::chrono::duration_cast<std::chrono::microseconds>(returned - start).count()
                  << "us; observed live generation=" << ObservedGeneration(data_dir)
                  << "; pending=" << writer.SnapshotGenerationLingerPendingForTests() << '\n';
        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        assert(reader.GetBlockBits(0, 0) == "11110000");
        assert((ObservedGeneration(data_dir) & 1U) == 0U);
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
            assert((ObservedGeneration(data_dir) & 1U) == 0U);
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
            assert((ObservedGeneration(data_dir) & 1U) != 0U);
            writer.SetBlockBits(4, 0, "00001111");
            assert((ObservedGeneration(data_dir) & 1U) != 0U);
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
        assert((ObservedGeneration(data_dir) & 1U) == 0U);
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
        assert((ObservedGeneration(data_dir) & 1U) == 0U);
        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        assert(reader.GetBlockBits(31, 0) == "11110000");
    }
    assert((ObservedGeneration(data_dir) & 1U) == 0U);
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
    assert((ObservedGeneration(data_dir) & 1U) != 0U);
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
        assert((ObservedGeneration(data_dir) & 1U) == 0U);
    }
    {
        chunkdb::ChunkStore reader(ReadOnlyConfig(BuildConfig(data_dir)));
        assert(reader.GetBlockBits(0, 0) == "11110000");
    }
    RemoveAllWithRetry(data_dir);
}

// A crash while no guard is active can leave even mapped bytes in the page
// cache. OS writer ownership, not that cached value, must gate readers.
int RunIdleCrashChild(const std::filesystem::path& data_dir) {
    chunkdb::ChunkStore writer(BuildConfig(data_dir));
    writer.SetBlockBits(0, 0, "11110000");
    assert((ObservedGeneration(data_dir) & 1U) == 0U);
    std::_Exit(87);
}

void TestIdleCrashCannotReuseCachedEven(const std::string& executable) {
    const auto dir = TempDataDir("idle-crash");
    std::string command = "\"" + executable + "\" --idle-crash-child \"" + dir.string() + "\"";
#ifdef _WIN32
    command = "\"" + command + "\"";
#endif
    const int status = std::system(command.c_str());
#ifdef _WIN32
    assert(status == 87);
#else
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 87);
#endif
    assert((ObservedGeneration(dir) & 1U) == 0U);
    bool rejected = false;
    try {
        chunkdb::ChunkStore reader(ReadOnlyConfig(BuildConfig(dir)));
        (void)reader.GetBlockBits(0, 0);
    } catch (const std::exception& error) {
        rejected = std::string(error.what()).find("remained unstable after") != std::string::npos;
    }
    assert(rejected);
    {
        chunkdb::ChunkStore writer(BuildConfig(dir));
        assert(writer.GetBlockBits(0, 0) == "11110000");
        chunkdb::ChunkStore reader(ReadOnlyConfig(BuildConfig(dir)));
        assert(reader.GetBlockBits(0, 0) == "11110000");
    }
    RemoveAllWithRetry(dir);
}

void TestRestartSessionRejectsCounterABA() {
    const auto dir = TempDataDir("restart-aba");
    const auto config = BuildConfig(dir);
    auto writer = std::make_unique<chunkdb::ChunkStore>(config);
    writer->SetBlockBits(0, 0, "11110000");
    const auto old_counter = ObservedGeneration(dir);
    const auto old_session = ReadBytes(dir / "chunkdb.snapshot");
    chunkdb::ChunkStore reader(ReadOnlyConfig(config));
    reader.ArmReadOnlySnapshotPausesForTests({
        {1, chunkdb::ReadOnlySnapshotArtifact::kWal},
    });
    std::exception_ptr error;
    std::string bits;
    std::thread collection([&] {
        try { bits = reader.GetBlockBits(0, 0); } catch (...) { error = std::current_exception(); }
    });
    if (!reader.WaitForReadOnlySnapshotPauseForTests()) {
        reader.ResumeReadOnlySnapshotForTests();
        collection.join();
        throw std::runtime_error("restart ABA: waiting for first collected WAL");
    }
    try {
        writer.reset();
        writer = std::make_unique<chunkdb::ChunkStore>(config);
        writer->SetBlockBits(0, 0, "00001111");
        assert(ObservedGeneration(dir) == old_counter);
        assert(ReadBytes(dir / "chunkdb.snapshot") != old_session);
    } catch (...) {
        reader.ResumeReadOnlySnapshotForTests();
        collection.join();
        throw;
    }
    reader.ResumeReadOnlySnapshotForTests();
    collection.join();
    if (error) std::rethrow_exception(error);
    assert(bits == "00001111");
    writer.reset();
    RemoveAllWithRetry(dir);
}

void TestSecondWriterRequiresOwnership() {
    const auto dir = TempDataDir("ownership");
    const auto config = BuildConfig(dir); // existing unsafe-option API retained
    {
        chunkdb::ChunkStore writer(config);
        writer.SetBlockBits(0, 0, "11110000");
        const auto session = ReadBytes(dir / "chunkdb.snapshot");
        bool rejected = false;
        try { chunkdb::ChunkStore second(config); }
        catch (const std::exception& error) {
            rejected = std::string(error.what()).find("single active writer") != std::string::npos;
        }
        assert(rejected);
        assert(ReadBytes(dir / "chunkdb.snapshot") == session);
        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        assert(reader.GetBlockBits(0, 0) == "11110000");
    }
    RemoveAllWithRetry(dir);
}

void TestLiveHeaderDamageFailsClosedAndRebuilds() {
    const auto dir = TempDataDir("live-header");
    const auto config = BuildConfig(dir);
    {
        chunkdb::ChunkStore writer(config);
        writer.SetBlockBits(0, 0, "11110000");
        {
            // Overwrite only the immutable header, never truncate an inode
            // that a running writer has mapped. The CRC must reject it.
            std::fstream damage(dir / "chunkdb.snapshot.live", std::ios::in | std::ios::out | std::ios::binary);
            assert(damage.is_open());
            damage.seekp(8);
            damage.put('x');
            damage.flush();
            assert(damage.good());
        }
        bool rejected = false;
        try {
            chunkdb::ChunkStore reader(ReadOnlyConfig(config));
            (void)reader.GetBlockBits(0, 0);
        } catch (const std::exception& error) {
            rejected = std::string(error.what()).find("live metadata is malformed") != std::string::npos;
        }
        assert(rejected);
    }
    {
        chunkdb::ChunkStore writer(config); // rebuild only volatile metadata
        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        assert(reader.GetBlockBits(0, 0) == "11110000");
    }
    RemoveAllWithRetry(dir);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 3 && std::string(argv[1]) == "--publication-crash-child") {
        return RunPublicationCrashChild(argv[2]);
    }
    if (argc == 3 && std::string(argv[1]) == "--idle-crash-child") return RunIdleCrashChild(argv[2]);
    TestCompletedWriteIsStableWithoutACloser();
    TestEvictedWritesPublishStableGenerations();
    TestConcurrentAndNestedPublicationsStayBracketed();
    TestBarrierAndCloseLeaveStableGenerations();
    TestCrashDuringPublicationFailsClosedAndRecovers(argv[0]);
    TestIdleCrashCannotReuseCachedEven(argv[0]);
    TestRestartSessionRejectsCounterABA();
    TestSecondWriterRequiresOwnership();
    TestLiveHeaderDamageFailsClosedAndRebuilds();
    return 0;
}
