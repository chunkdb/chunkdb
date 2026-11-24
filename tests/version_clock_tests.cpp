// Version clock ceiling extension: the next reserved range is written on the
// extension thread while writers take tokens below the durable ceiling, a
// writer at the ceiling waits for the write in flight instead of starting
// another, a failed early write is written again by the writer that needs
// its ceiling, and a crash during an extension leaves a valid clock above
// every issued token.

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <stdexcept>
#include <string>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "test_utils.hpp"

#ifndef _WIN32
#include <sys/wait.h>
#endif

namespace {

constexpr std::uint64_t kRange = 65536;
constexpr std::uint64_t kLead = kRange / 2U;

chunkdb::StoreConfig Config(const std::filesystem::path& path) {
    chunkdb::StoreConfig config;
    config.data_dir = path;
    config.geometry.chunk_width_blocks = 4;
    config.geometry.chunk_height_blocks = 4;
    config.geometry.block_bits = 5;
    config.durability_mode = chunkdb::DurabilityMode::kRelaxed;
    config.background_maintenance = false;
    config.max_loaded_chunks = 16;
    config.checkpoint_update_interval = 1000;
    config.wal_group_commit_updates = 1;
    return config;
}

std::uint64_t PersistedCeiling(const std::filesystem::path& data_dir) {
    std::uint64_t ceiling = 0;
    assert(chunkdb::TryParseVersionClockRecord(chunkdb::LoadFile(data_dir / "chunkdb.version"), &ceiling));
    return ceiling;
}

bool HasClockTempFile(const std::filesystem::path& data_dir) {
    for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
        if (entry.path().filename().string().rfind("chunkdb.version.tmp", 0) == 0) return true;
    }
    return false;
}

// Takes every token from `first` through `last`, in order.
void ReserveThrough(chunkdb::ChunkStore& store, std::uint64_t first, std::uint64_t last) {
    for (std::uint64_t token = first; token <= last; ++token) {
        assert(store.ReserveChunkVersionForTests() == token);
    }
}

// The first write of an opened store writes the first range itself. Takes
// tokens up to the one half a range below its ceiling, which starts the next
// range's write, and holds that write before it begins. Returns the ceiling
// while it is held.
std::uint64_t HoldExtension(chunkdb::ChunkStore& store) {
    const auto first = store.ReserveChunkVersionForTests();
    const auto ceiling = first + kRange;
    assert(store.VersionClockCeilingForTests() == ceiling);
    ReserveThrough(store, first + 1U, ceiling - kLead - 1U);
    assert(store.RuntimeStats().version_clock_extensions == 1U);
    store.ArmVersionClockExtensionPauseForTests();
    // The writer of this token returns at once: the write runs elsewhere.
    assert(store.ReserveChunkVersionForTests() == ceiling - kLead);
    assert(store.WaitForVersionClockExtensionPauseForTests());
    return ceiling;
}

bool IsInjectedFailure(const std::exception& error) {
    return std::string(error.what()).find("injected version clock extension failure") != std::string::npos;
}

void TestWritersBelowTheCeilingDoNotWait() {
    chunkdb::test::ScopedTempDir dir("chunkdb-version-clock-early");
    chunkdb::ChunkStore store(Config(dir.path()));
    const auto ceiling = HoldExtension(store);
    const auto waited_us = store.RuntimeStats().version_clock_wait_us;

    // With the extension held before its write, the rest of the range is
    // issued, to mutations as well, and no writer waits.
    store.SetBlockBits(0, 0, "10101");
    assert(store.GetChunkVersion(0, 0) == ceiling - kLead + 1U);
    ReserveThrough(store, ceiling - kLead + 2U, ceiling - 1U);
    assert(store.RuntimeStats().version_clock_wait_us == waited_us);
    assert(store.VersionClockCeilingForTests() == ceiling);
    assert(PersistedCeiling(dir.path()) == ceiling);

    // A writer at the ceiling waits for the write in flight and writes no
    // record itself.
    auto waiter = std::async(std::launch::async, [&] { return store.ReserveChunkVersionForTests(); });
    assert(waiter.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout);
    store.ResumeVersionClockExtensionForTests(false);
    assert(waiter.get() == ceiling);
    assert(store.VersionClockCeilingForTests() == ceiling + kRange);
    assert(PersistedCeiling(dir.path()) == ceiling + kRange);
    const auto stats = store.RuntimeStats();
    assert(stats.version_clock_extensions == 2U);
    assert(stats.version_clock_wait_us > waited_us);
}

void TestFailedExtensionIsWrittenByTheWriterAtTheCeiling() {
    chunkdb::test::ScopedTempDir dir("chunkdb-version-clock-fail");
    chunkdb::ChunkStore store(Config(dir.path()));

    // The writer waiting at the ceiling writes the record when the early
    // write fails; the failed write is not counted.
    const auto ceiling = HoldExtension(store);
    ReserveThrough(store, ceiling - kLead + 1U, ceiling - 1U);
    auto waiter = std::async(std::launch::async, [&] { return store.ReserveChunkVersionForTests(); });
    assert(waiter.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout);
    store.ResumeVersionClockExtensionForTests(true);
    assert(waiter.get() == ceiling);
    const auto next = ceiling + kRange;
    assert(store.VersionClockCeilingForTests() == next);
    assert(PersistedCeiling(dir.path()) == next);
    assert(store.RuntimeStats().version_clock_extensions == 2U);

    // When the writer's own write fails too, the mutation fails, changes
    // nothing, and the next one writes the record.
    ReserveThrough(store, ceiling + 1U, next - kLead - 1U);
    store.ArmVersionClockExtensionPauseForTests();
    assert(store.ReserveChunkVersionForTests() == next - kLead);
    assert(store.WaitForVersionClockExtensionPauseForTests());
    ReserveThrough(store, next - kLead + 1U, next - 1U);
    auto failing = std::async(std::launch::async, [&] {
        try {
            store.SetBlockBits(4, 0, "00011");
        } catch (const std::runtime_error& error) {
            return IsInjectedFailure(error);
        }
        return false;
    });
    assert(failing.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout);
    // Armed again, the pause fails the writer's write without holding it.
    store.ArmVersionClockExtensionPauseForTests();
    store.ResumeVersionClockExtensionForTests(true);
    assert(failing.get());
    assert(store.GetBlockBits(4, 0) == "00000");
    assert(store.VersionClockCeilingForTests() == next);
    assert(PersistedCeiling(dir.path()) == next);
    store.SetBlockBits(4, 0, "00011");
    assert(store.GetChunkVersion(1, 0) == next);
    assert(PersistedCeiling(dir.path()) == next + kRange);
    assert(store.RuntimeStats().version_clock_extensions == 3U);
}

// Closing the store waits for the extension in flight; its ceiling is durable
// afterwards and the next open starts there.
void TestCloseWaitsForTheExtensionInFlight() {
    chunkdb::test::ScopedTempDir dir("chunkdb-version-clock-close");
    std::uint64_t ceiling = 0;
    {
        chunkdb::ChunkStore store(Config(dir.path()));
        ceiling = HoldExtension(store);
    }
    assert(PersistedCeiling(dir.path()) == ceiling + kRange);
    assert(!HasClockTempFile(dir.path()));
    chunkdb::ChunkStore reopened(Config(dir.path()));
    assert(reopened.ReserveChunkVersionForTests() == ceiling + kRange);
}

void SetFailpoint(const char* key) {
#ifdef _WIN32
    _putenv_s(key, "1");
#else
    setenv(key, "1", 1);
#endif
}

// Child: takes tokens up to the ceiling while an extension is held, or while
// one crashes in its write, and ends during that extension.
int RunCrashChild(const std::filesystem::path& data_dir, const std::string& point) {
    chunkdb::ChunkStore store(Config(data_dir));
    if (point == "held") {
        const auto ceiling = HoldExtension(store);
        ReserveThrough(store, ceiling - kLead + 1U, ceiling - 1U);
        chunkdb::ExitAbruptly(86);
    }
    assert(point == "before-replace");
    const auto first = store.ReserveChunkVersionForTests();
    const auto ceiling = first + kRange;
    SetFailpoint("CHUNKDB_FAILPOINT_CRASH_VERSION_EXTENSION_BEFORE_REPLACE_ONCE");
    // The extension started half a range below the ceiling ends the process
    // after writing its temporary file; this thread then waits at the ceiling.
    ReserveThrough(store, first + 1U, ceiling - 1U);
    (void)store.ReserveChunkVersionForTests();
    return 3;
}

std::string Quote(const std::string& text) {
#ifdef _WIN32
    return '"' + text + '"';
#else
    std::string result = "'";
    for (const char c : text) result += c == '\'' ? "'\\''" : std::string(1U, c);
    return result + "'";
#endif
}

void RunCrashChildProcess(const char* executable, const std::filesystem::path& data_dir, const char* point) {
    const auto command = Quote(std::filesystem::absolute(executable).string()) + " --crash-child " +
        Quote(data_dir.string()) + " " + point;
#ifdef _WIN32
    assert(std::system(('"' + command + '"').c_str()) == 86);
#else
    const auto result = std::system(command.c_str());
    assert(result != -1 && WIFEXITED(result) && WEXITSTATUS(result) == 86);
#endif
}

void TestCrashDuringTheExtensionLeavesAValidClock(const char* executable) {
    for (const auto* point : {"held", "before-replace"}) {
        const bool before_replace = std::string(point) == "before-replace";
        if (before_replace && !chunkdb::kFailpointsCompiled) continue;
        chunkdb::test::ScopedTempDir dir("chunkdb-version-clock-crash");
        {
            chunkdb::ChunkStore store(Config(dir.path()));
            store.SetBlockBits(0, 0, "10101");
        }
        // The child's first token is the ceiling it opens with, and every
        // token it issues is below one range above it.
        const auto opened = PersistedCeiling(dir.path());
        RunCrashChildProcess(executable, dir.path(), point);
        assert(PersistedCeiling(dir.path()) == opened + kRange);
        assert(HasClockTempFile(dir.path()) == before_replace);
        chunkdb::ChunkStore reopened(Config(dir.path()));
        assert(!HasClockTempFile(dir.path()));
        assert(reopened.GetBlockBits(0, 0) == "10101");
        assert(reopened.ReserveChunkVersionForTests() == opened + kRange);
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::string(argv[1]) == "--crash-child") {
        return RunCrashChild(argv[2], argv[3]);
    }
    TestWritersBelowTheCeilingDoNotWait();
    TestFailedExtensionIsWrittenByTheWriterAtTheCeiling();
    TestCloseWaitsForTheExtensionInFlight();
    TestCrashDuringTheExtensionLeavesAValidClock(argv[0]);
    return 0;
}
