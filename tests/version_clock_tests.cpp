// Version clock ceiling extension: the next reserved range is written on the
// extension thread while writers take tokens below the durable ceiling, a
// writer at the ceiling waits for the write in flight instead of starting
// another, a failed early write is written again by the writer that needs
// its ceiling, and a crash during an extension leaves a valid clock above
// every issued token.

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/logging.hpp"
#include "test_utils.hpp"

#ifndef _WIN32
#include <sys/wait.h>
#endif

namespace {

constexpr std::uint64_t kRange = 65536;
constexpr std::uint64_t kLead = kRange / 2U;
constexpr auto kWaitTimeout = std::chrono::seconds(10);

class WaitHook final : public chunkdb::VersionClockWaitTestHook {
  public:
    explicit WaitHook(bool hold_after_publish = false) : hold_after_publish_(hold_after_publish) {}

    void Run(Point point) override {
        std::unique_lock lock(mutex_);
        if (point == Point::kBeforeWait) {
            waiting_ = true;
            changed_.notify_all();
        } else if (hold_after_publish_) {
            published_ = true;
            changed_.notify_all();
            changed_.wait(lock, [this] { return resumed_; });
        }
    }

    void WaitForWriter() {
        std::unique_lock lock(mutex_);
        assert(changed_.wait_for(lock, kWaitTimeout, [this] { return waiting_; }));
    }

    void WaitForPublication() {
        std::unique_lock lock(mutex_);
        assert(changed_.wait_for(lock, kWaitTimeout, [this] { return published_; }));
    }

    void ResumeAfterPublication() {
        std::lock_guard lock(mutex_);
        resumed_ = true;
        changed_.notify_all();
    }

  private:
    const bool hold_after_publish_;
    std::mutex mutex_;
    std::condition_variable changed_;
    bool waiting_ = false;
    bool published_ = false;
    bool resumed_ = false;
};

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

void TestWritersBelowTheCeilingDoNotWait() {
    chunkdb::test::ScopedTempDir dir("chunkdb-version-clock-early");
    WaitHook hook;
    chunkdb::ChunkStore store(Config(dir.path()));
    store.SetVersionClockWaitHookForTests(&hook);
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
    hook.WaitForWriter();
    assert(waiter.wait_for(std::chrono::seconds(0)) == std::future_status::timeout);
    store.ResumeVersionClockExtensionForTests(false);
    assert(waiter.get() == ceiling);
    assert(store.VersionClockCeilingForTests() == ceiling + kRange);
    assert(PersistedCeiling(dir.path()) == ceiling + kRange);
    const auto stats = store.RuntimeStats();
    assert(stats.version_clock_extensions == 2U);
    assert(stats.version_clock_wait_us > waited_us);
}

void TestWaiterUsesPublishedRangeWhileNextExtensionIsHeld() {
    chunkdb::test::ScopedTempDir dir("chunkdb-version-clock-successive");
    WaitHook hook(/*hold_after_publish=*/true);
    chunkdb::ChunkStore store(Config(dir.path()));
    store.SetVersionClockWaitHookForTests(&hook);
    const auto ceiling = HoldExtension(store);
    ReserveThrough(store, ceiling - kLead + 1U, ceiling - 1U);

    auto waiter = std::async(std::launch::async, [&] { return store.ReserveChunkVersionForTests(); });
    hook.WaitForWriter();
    store.ResumeVersionClockExtensionForTests(false);
    hook.WaitForPublication();

    const auto next = ceiling + kRange;
    ReserveThrough(store, ceiling, next - kLead - 1U);
    store.ArmVersionClockExtensionPauseForTests();
    assert(store.ReserveChunkVersionForTests() == next - kLead);
    assert(store.WaitForVersionClockExtensionPauseForTests());

    hook.ResumeAfterPublication();
    assert(waiter.wait_for(kWaitTimeout) == std::future_status::ready);
    assert(waiter.get() == next - kLead + 1U);
    assert(PersistedCeiling(dir.path()) == next);
    store.ResumeVersionClockExtensionForTests(false);
}

void TestFailedExtensionIsWrittenByTheWriterAtTheCeiling() {
    chunkdb::test::ScopedTempDir dir("chunkdb-version-clock-fail");
    WaitHook hook;
    chunkdb::ChunkStore store(Config(dir.path()));
    store.SetVersionClockWaitHookForTests(&hook);

    // The writer waiting at the ceiling writes the record when the early
    // write fails; the failed write is not counted.
    const auto ceiling = HoldExtension(store);
    ReserveThrough(store, ceiling - kLead + 1U, ceiling - 1U);
    std::atomic<unsigned> failed_warnings{0};
    const auto log_level = chunkdb::GetLogLevel();
    chunkdb::SetLogLevel(chunkdb::LogLevel::kWarn);
    chunkdb::SetLogSinkForTests([&](const std::string& line) {
        if (line.find("version clock extension failed") != std::string::npos) {
            failed_warnings.fetch_add(1U, std::memory_order_relaxed);
            throw std::bad_alloc();
        }
    });
    auto waiter = std::async(std::launch::async, [&] { return store.ReserveChunkVersionForTests(); });
    hook.WaitForWriter();
    store.ResumeVersionClockExtensionForTests(true);
    assert(waiter.get() == ceiling);
    chunkdb::ResetLogSinkForTests();
    chunkdb::SetLogLevel(log_level);
    assert(failed_warnings.load(std::memory_order_relaxed) == 1U);
    const auto next = ceiling + kRange;
    assert(store.VersionClockCeilingForTests() == next);
    assert(PersistedCeiling(dir.path()) == next);
    assert(store.RuntimeStats().version_clock_extensions == 2U);
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
    assert(_putenv_s(key, "1") == 0);
#else
    assert(setenv(key, "1", 1) == 0);
#endif
}

void TestFailedDirectorySyncKeepsPreviousCeiling() {
    chunkdb::test::ScopedTempDir dir("chunkdb-version-clock-dir-sync");
    chunkdb::ChunkStore store(Config(dir.path()));
    const auto ceiling = store.VersionClockCeilingForTests();
    const auto before = store.RuntimeStats();
    SetFailpoint("CHUNKDB_FAILPOINT_VERSION_EXTENSION_AFTER_REPLACE_BEFORE_DIR_SYNC_ONCE");
    bool failed = false;
    try {
        store.SetBlockBits(0, 0, "00011");
    } catch (const std::runtime_error& error) {
        failed = std::string_view(error.what()).starts_with("injected atomic write failure after replace before directory sync");
    }
    assert(failed);
    assert(PersistedCeiling(dir.path()) == ceiling + kRange);
    assert(store.VersionClockCeilingForTests() == ceiling);
    assert(store.GetChunkVersion(0, 0) == 0U);
    assert(store.GetBlockBits(0, 0) == "00000");
    const auto after = store.RuntimeStats();
    assert(after.version_clock_extensions == before.version_clock_extensions);
    assert(after.version_clock_wait_us > before.version_clock_wait_us);

    store.SetBlockBits(0, 0, "00011");
    assert(store.GetChunkVersion(0, 0) == ceiling);
    assert(store.VersionClockCeilingForTests() == ceiling + kRange);
    assert(store.RuntimeStats().version_clock_extensions == before.version_clock_extensions + 1U);
}

// Fill the current range before letting the extension reach its crash point.
int RunCrashChild(const std::filesystem::path& data_dir, const std::string& point) {
    chunkdb::ChunkStore store(Config(data_dir));
    const auto ceiling = HoldExtension(store);
    ReserveThrough(store, ceiling - kLead + 1U, ceiling - 1U);
    if (point == "held") chunkdb::ExitAbruptly(86);
    assert(point == "before-replace" || point == "after-replace");
    SetFailpoint(point == "before-replace" ? "CHUNKDB_FAILPOINT_CRASH_VERSION_EXTENSION_BEFORE_REPLACE_ONCE"
                                          : "CHUNKDB_FAILPOINT_CRASH_VERSION_EXTENSION_AFTER_REPLACE_ONCE");
    store.ResumeVersionClockExtensionForTests(false);
    // Closing joins the worker; returning 3 means it missed its crash point.
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
    for (const std::string_view point : {"held", "before-replace", "after-replace"}) {
        chunkdb::test::ScopedTempDir dir("chunkdb-version-clock-crash");
        {
            chunkdb::ChunkStore store(Config(dir.path()));
            store.SetBlockBits(0, 0, "10101");
        }
        // The child's first token is the ceiling it opens with, and every
        // token it issues is below one range above it.
        const auto opened = PersistedCeiling(dir.path());
        RunCrashChildProcess(executable, dir.path(), point.data());
        // A process exit leaves a completed rename visible to the next opener.
        const auto expected = opened + (point == "after-replace" ? 2U : 1U) * kRange;
        assert(PersistedCeiling(dir.path()) == expected);
        assert(HasClockTempFile(dir.path()) == (point == "before-replace"));
        chunkdb::ChunkStore reopened(Config(dir.path()));
        assert(!HasClockTempFile(dir.path()));
        assert(reopened.GetBlockBits(0, 0) == "10101");
        assert(reopened.ReserveChunkVersionForTests() == expected);
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::string(argv[1]) == "--crash-child") {
        return RunCrashChild(argv[2], argv[3]);
    }
    if (argc == 2 && std::string_view(argv[1]) == "--crash-boundaries") {
        TestCrashDuringTheExtensionLeavesAValidClock(argv[0]);
        return 0;
    }
    TestWritersBelowTheCeilingDoNotWait();
    TestWaiterUsesPublishedRangeWhileNextExtensionIsHeld();
    TestFailedExtensionIsWrittenByTheWriterAtTheCeiling();
    TestFailedDirectorySyncKeepsPreviousCeiling();
    TestCloseWaitsForTheExtensionInFlight();
    return 0;
}
