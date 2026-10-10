// Abrupt exits at each archive publication step preserve both current state
// and the historical rows, before and after a subsequent checkpoint retry.
#include <array>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <future>
#include <stdexcept>
#include <iostream>
#include <map>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <cerrno>
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "chunkdb/file_layout.hpp"
#include "chunk_store_internal.hpp"
#include "feed_slots.hpp"
#include "feed_test_utils.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::feed_test;

constexpr int kCrashExit = 86;
constexpr std::array<const char*, 4U> kPoints{
    "CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_WAL_FLUSH_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_BASE_LINK_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_IMAGE_PUBLISH_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_WAL_RENAME_ONCE"};
constexpr std::array<DurabilityMode, 3U> kModes{
    DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal, DurabilityMode::kFsyncCheckpoint};

CatalogConfig CrashConfig(const std::filesystem::path& path, DurabilityMode mode) {
    auto config = Config(path);
    config.default_options.durability_mode = mode;
    config.default_options.wal_group_commit_updates = 1000U;
    config.slot_sync_interval = std::chrono::hours(1);
    return config;
}

#ifdef _WIN32
std::wstring QuoteArgument(const std::wstring& argument) {
    std::wstring quoted = L"\"";
    std::size_t slashes = 0U;
    for (const auto character : argument) {
        if (character == L'\\') { ++slashes; continue; }
        quoted.append(character == L'"' ? slashes * 2U + 1U : slashes, L'\\');
        quoted += character;
        slashes = 0U;
    }
    quoted.append(slashes * 2U, L'\\');
    return quoted + L'"';
}
#endif

int RunChild(const std::string& executable, const std::vector<std::string>& arguments,
             std::chrono::milliseconds timeout = std::chrono::seconds(60)) {
    std::string context = executable;
    for (const auto& argument : arguments) context += " [" + argument + "]";
#ifdef _WIN32
    const auto application = std::filesystem::path(executable).wstring();
    auto command = QuoteArgument(application);
    for (const auto& argument : arguments)
        command += L" " + QuoteArgument(std::filesystem::path(argument).wstring());
    STARTUPINFOW startup{};
    startup.cb = sizeof(startup);
    startup.dwFlags = STARTF_USESTDHANDLES;
    startup.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
    startup.hStdOutput = GetStdHandle(STD_OUTPUT_HANDLE);
    startup.hStdError = GetStdHandle(STD_ERROR_HANDLE);
    PROCESS_INFORMATION child{};
    if (!CreateProcessW(application.c_str(), command.data(), nullptr, nullptr, TRUE,
                        0U, nullptr, nullptr, &startup, &child))
        throw std::runtime_error("cannot launch child: " + context + " error=" + std::to_string(GetLastError()));
    struct Handles {
        PROCESS_INFORMATION& child;
        ~Handles() { CloseHandle(child.hThread); CloseHandle(child.hProcess); }
    } handles{child};
    const auto result = WaitForSingleObject(child.hProcess, static_cast<DWORD>(timeout.count()));
    if (result != WAIT_OBJECT_0) {
        const auto error = GetLastError();
        if (!TerminateProcess(child.hProcess, 124U))
            throw std::runtime_error("cannot terminate child pid=" + std::to_string(child.dwProcessId) + " " + context);
        if (WaitForSingleObject(child.hProcess, 5000U) != WAIT_OBJECT_0)
            throw std::runtime_error("child termination did not complete pid=" + std::to_string(child.dwProcessId) + " " + context);
        throw std::runtime_error("child " + std::string(result == WAIT_TIMEOUT ? "deadline exceeded" : "wait failed") +
            " pid=" + std::to_string(child.dwProcessId) + " timeout_ms=" + std::to_string(timeout.count()) +
            " " + context + " error=" + std::to_string(error));
    }
    DWORD exit_code = 0U;
    if (!GetExitCodeProcess(child.hProcess, &exit_code))
        throw std::runtime_error("cannot read child exit: " + context);
    return static_cast<int>(exit_code);
#else
    std::vector<char*> argv{const_cast<char*>(executable.c_str())};
    for (const auto& argument : arguments) argv.push_back(const_cast<char*>(argument.c_str()));
    argv.push_back(nullptr);
    const auto pid = fork();
    if (pid < 0) throw std::runtime_error("cannot fork child: " + context);
    if (pid == 0) { execv(executable.c_str(), argv.data()); std::_Exit(127); }
    struct Child {
        pid_t pid;
        ~Child() {
            // The wait thread observes exit without reaping, so this PID still
            // belongs to this child even when it has already exited.
            (void)kill(pid, SIGKILL);
            while (waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {}
        }
    } child{pid};
    auto finished = std::async(std::launch::async, [pid] {
        siginfo_t info{};
        while (waitid(P_PID, static_cast<id_t>(pid), &info, WEXITED | WNOWAIT) < 0) {
            if (errno != EINTR) throw std::runtime_error("waitid failed pid=" + std::to_string(pid));
        }
        return info.si_code == CLD_EXITED ? info.si_status : -info.si_status;
    });
    if (finished.wait_for(timeout) != std::future_status::ready) {
        (void)kill(pid, SIGKILL);
        (void)finished.get();
        throw std::runtime_error("child deadline exceeded pid=" + std::to_string(pid) +
            " timeout_ms=" + std::to_string(timeout.count()) + " " + context);
    }
    return finished.get();
#endif
}

void ChildDeadline(const std::string& executable) {
    bool refused = false;
    try { (void)RunChild(executable, {"--wait-child"}, std::chrono::milliseconds(100)); }
    catch (const std::runtime_error& error) {
        const std::string message = error.what();
        refused = message.find("deadline exceeded") != std::string::npos &&
            message.find("--wait-child") != std::string::npos && message.find("pid=") != std::string::npos;
    }
    assert(refused);
    std::cout << "child deadline and cleanup passed\n" << std::flush;
}

int Crash(const std::filesystem::path& path, DurabilityMode mode, const char* point, bool empty) {
    TableCatalog catalog(CrashConfig(path, mode));
    auto table = catalog.Find("default");
    auto lease = table->Acquire();
    auto& store = lease->store();
    ScopedWriteUser identity("alice");
    store.SetBlockBits(0, 0, Bits(11U));
    store.SetBlockBits(0, 0, Bits(22U));
    if (empty) assert(store.UnsetBlock(0, 0, store.GetChunkVersion(0, 0)).ok);
    // Arm only the checkpoint, after all user mutations are staged.
    txn_test::ScopedEnv armed(point, "1");
    store.CheckpointForTests(0, 0);
    return 3;  // Reaching here means the claimed crash point was not reached.
}

using Row = std::pair<std::optional<std::uint32_t>, std::optional<std::uint32_t>>;

void Value(const std::optional<std::vector<ColumnValue>>& actual, std::optional<std::uint32_t> value) {
    assert(actual.has_value() == value.has_value());
    if (value) assert(*actual == std::vector<ColumnValue>{BitsValue{Bits(*value)}});
}

std::vector<FeedPosition> Rows(Table& table, FeedPosition start, const std::vector<Row>& expected) {
    auto reader = table.ReadFeedArchive(start);
    std::vector<FeedPosition> positions;
    auto previous = start.revision;
    for (const auto& [before, after] : expected) {
        const auto entry = reader.Next();
        assert(entry && entry->kind == FeedEntry::Kind::kChange && entry->schema_version == 1U);
        assert(entry->position.epoch == start.epoch && entry->position.revision > previous);
        assert(entry->user == "alice" && entry->blocks.size() == 1U);
        const auto& block = entry->blocks.front();
        assert((block.chunk == ChunkCoord{0, 0}) && block.local_x == 0U && block.local_y == 0U);
        assert(block.x == 0 && block.y == 0);
        Value(block.before, before);
        Value(block.after, after);
        previous = entry->position.revision;
        positions.push_back(entry->position);
    }
    assert(!reader.Next());  // No extra event for the collection maintenance frame.
    return positions;
}

std::map<std::filesystem::path, std::vector<std::uint8_t>> Bases(const std::filesystem::path& root) {
    std::map<std::filesystem::path, std::vector<std::uint8_t>> images;
    const auto directory = root / "tables" / "default" / kFeedArchiveDirName;
    if (std::filesystem::exists(directory)) for (const auto& item : std::filesystem::directory_iterator(directory))
        if (item.path().extension() == ".chk") images.emplace(item.path(), LoadFile(item.path()));
    return images;
}

void Case(const std::string& executable, DurabilityMode mode, bool base, bool empty, const char* point) {
    test::ScopedTempDir directory("chunkdb-feed-slot-crash");
    std::cout << DurabilityModeName(mode) << " base=" << base << " empty=" << empty << ' ' << point
              << " " << directory.path().string() << '\n' << std::flush;
    const auto config = CrashConfig(directory.path(), mode);
    FeedPosition start;
    {
        TableCatalog catalog(config);
        (void)feed_test::CreateDefault(catalog);
        auto table = catalog.Find("default");
        if (base) {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(0, 0, Bits(7U));
            lease->store().CheckpointForTests(0, 0);
        }
        start = table->CreateFeedSlot("consumer").position;
    }
    std::cout << "phase=child base=" << base << " empty=" << empty << '\n' << std::flush;
    const auto exit_code = RunChild(executable, {"--crash", directory.path().string(), DurabilityModeName(mode), point,
                                               empty ? "empty" : "present"});
    if (exit_code != kCrashExit)
        throw std::runtime_error("crash child did not reach " + std::string(point) + " mode=" + DurabilityModeName(mode) +
            " base=" + std::to_string(base) + " empty=" + std::to_string(empty) + " exit=" + std::to_string(exit_code));
    std::cout << "phase=recovery " << point << '\n' << std::flush;
    std::vector<Row> expected{{base ? std::optional<std::uint32_t>{7U} : std::nullopt, 11U}, {11U, 22U}};
    if (empty) expected.emplace_back(22U, std::nullopt);
    std::vector<FeedPosition> original;
    {
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        const auto slots = table->ListFeedSlots();
        assert(slots.size() == 1U && slots[0].position == start);
        {
            auto lease = table->Acquire();
            const auto state = lease->store().ReadChunkState(0, 0);
            assert(txn_test::CounterOf(state) == (empty ? 0U : 22U));
            assert(BlockPresent(state.presence_bitmap, 0U) != empty);
        }
        FeedSlotTestAccess::Sync(*table);
        // Read before retrying publication: this exercises the live WAL with
        // a newly published current image and the old linked base image.
        original = Rows(*table, start, expected);
        const auto bases = Bases(directory.path());
        {
            auto lease = table->Acquire();
            lease->store().CheckpointForTests(0, 0);
        }
        FeedSlotTestAccess::Sync(*table);
        assert(Rows(*table, start, expected) == original);
        for (const auto& [path, bytes] : bases) assert(std::filesystem::exists(path) && LoadFile(path) == bytes);
        {
            ScopedWriteUser identity("alice");
            auto lease = table->Acquire();
            lease->store().SetBlockBits(0, 0, Bits(33U));
        }
        FeedSlotTestAccess::Sync(*table);
    }
    expected.emplace_back(empty ? std::nullopt : std::optional<std::uint32_t>{22U}, 33U);
    {
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        const auto all = Rows(*table, start, expected);
        assert(std::equal(original.begin(), original.end(), all.begin()));
        auto lease = table->Acquire();
        assert(txn_test::CounterOf(lease->store().ReadChunkState(0, 0)) == 33U);
    }
}
}  // namespace

int main(int argc, char** argv) {
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    _set_error_mode(_OUT_TO_STDERR);
    _set_abort_behavior(0U, _CALL_REPORTFAULT);
#endif
    if (argc == 2 && std::string_view(argv[1]) == "--wait-child") {
        std::promise<void> never;
        never.get_future().wait();
        return 3;
    }
    try {
        if (argc == 6 && std::string_view(argv[1]) == "--crash")
            return Crash(argv[2], ParseDurabilityMode(argv[3]), argv[4], std::string_view(argv[5]) == "empty");
        assert(argc == 1);
        ChildDeadline(std::filesystem::absolute(argv[0]).string());
        std::size_t count = 0U;
        for (const auto mode : kModes) for (const bool base : {false, true}) for (const bool empty : {false, true})
            for (const auto* point : kPoints) {
                Case(std::filesystem::absolute(argv[0]).string(), mode, base, empty, point);
                ++count;
            }
        std::cout << count << " archive crash scenarios passed\n";
    } catch (const std::exception& error) {
        std::cerr << "FAIL feed_slots_crash: " << error.what() << '\n';
        return 1;
    }
}
