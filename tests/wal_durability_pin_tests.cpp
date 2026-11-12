#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

#include "chunkdb/wal_append_stream.hpp"
#include "test_utils.hpp"

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {
using namespace chunkdb;
constexpr auto kAppend = std::ios::binary | std::ios::app;
static_assert(std::is_nothrow_move_constructible_v<WalDurabilityPin>);
static_assert(std::is_nothrow_move_assignable_v<WalDurabilityPin>);
static_assert(!std::is_copy_constructible_v<WalDurabilityPin>);

std::string Read(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input.good());
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

template <typename F> std::error_code Refuse(F action) {
    try { action(); }
    catch (const std::system_error& error) { return error.code(); }
    assert(false);
    return {};
}

void NativeGenerationAndBufferVisibility() {
    test::ScopedTempDir directory("chunkdb-wal-native-generation");
    const auto path = directory.path() / "wal";
    const auto retained = directory.path() / "retained";
    const auto streams = WalAppendStream::OpenCountForTests();
    WalDurabilityPin pin;
    {
        WalAppendStream output(path, kAppend);
        assert(output.good() && output.is_open());
        output.write("first", 5U);
        pin = output.FlushAndPin();
        assert(pin.is_open() && output.good() && Read(path) == "first");
        assert(WalAppendStream::OpenCountForTests() == streams + 1U);
        pin.Sync();
        output.write("+last", 5U).flush();
        assert(output.good());
        std::filesystem::rename(path, retained);
        pin.Sync();
        output.write("+end", 4U).flush();
        assert(output.good());
    }
    assert(WalAppendStream::OpenCountForTests() == streams);
    std::filesystem::create_directory(path);
    pin.Sync();
    assert(Read(retained) == "first+last+end");
    assert(std::filesystem::is_directory(path));
    auto moved = std::move(pin);
    assert(moved.is_open() && !pin.is_open());
    assert(Refuse([&] { pin.Sync(); }) == std::errc::bad_file_descriptor);
    moved.Sync();
}

#ifdef _WIN32
void ExclusiveOpen(const std::filesystem::path& path, bool succeeds) {
    const auto handle = CreateFileW(path.c_str(), GENERIC_READ, 0U, nullptr,
        OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
    if (succeeds) {
        assert(handle != INVALID_HANDLE_VALUE);
        assert(CloseHandle(handle) != 0);
    } else {
        assert(handle == INVALID_HANDLE_VALUE && GetLastError() == ERROR_SHARING_VIOLATION);
    }
}
#endif

void MoveOwnership() {
    test::ScopedTempDir directory("chunkdb-wal-pin-moves");
    const auto path_a = directory.path() / "a";
    const auto path_b = directory.path() / "b";
    WalAppendStream a(path_a, kAppend), b(path_b, kAppend);
    a.write("a", 1U); b.write("b", 1U);
    auto first = a.FlushAndPin();
    auto second = b.FlushAndPin();
    a.close(); b.close();
    first = std::move(second);
    assert(first.is_open() && !second.is_open());
    first.Sync();
    assert(Refuse([&] { second.Sync(); }) == std::errc::bad_file_descriptor);
#ifdef _WIN32
    ExclusiveOpen(path_a, true);
    ExclusiveOpen(path_b, false);
#endif
    first = {};
    assert(!first.is_open());
#ifdef _WIN32
    ExclusiveOpen(path_b, true);
#endif
    assert(Read(path_a) == "a" && Read(path_b) == "b");
}

void FailedAndClosedStreams() {
    WalAppendStream closed;
    assert(Refuse([&] { (void)closed.FlushAndPin(); }) == std::errc::bad_file_descriptor);
    assert(!closed.is_open() && closed.error() == 0);
    WalAppendStream invalid;
    invalid.open({}, std::ios::binary);
    const auto original = invalid.error();
    assert(original == EINVAL && !invalid.is_open());
    assert(Refuse([&] { (void)invalid.FlushAndPin(); }).value() == original);
    assert(invalid.error() == original);
    test::ScopedTempDir directory("chunkdb-wal-pin-closed");
    WalAppendStream output(directory.path() / "wal", kAppend);
    output.write("ok", 2U);
    output.close();
    assert(output.good());
    assert(Refuse([&] { (void)output.FlushAndPin(); }) == std::errc::bad_file_descriptor);
    assert(output.good());
}

#ifndef _WIN32
void DescriptorOwnershipAndExhaustion() {
    test::ScopedTempDir directory("chunkdb-wal-pin-descriptors");
    const auto path = directory.path() / "wal";
    const auto child = ::fork();
    assert(child >= 0);
    if (child == 0) {
        WalAppendStream output(path, kAppend);
        output.write("first", 5U);
        assert(output.good());
        const auto probe_a = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        const auto probe_b = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        assert(probe_a >= 0 && probe_b > probe_a);
        assert(::close(probe_a) == 0 && ::close(probe_b) == 0);
        struct rlimit original{};
        assert(::getrlimit(RLIMIT_NOFILE, &original) == 0);
        auto limited = original;
        limited.rlim_cur = static_cast<rlim_t>(probe_b) + 1U;
        assert(::setrlimit(RLIMIT_NOFILE, &limited) == 0);
        for (unsigned i = 0U; i < 32U; ++i) {
            auto first = output.FlushAndPin();
            auto second = output.FlushAndPin();
            const auto first_flags = ::fcntl(probe_a, F_GETFD);
            const auto second_flags = ::fcntl(probe_b, F_GETFD);
            assert(first_flags >= 0 && (first_flags & FD_CLOEXEC) != 0);
            assert(second_flags >= 0 && (second_flags & FD_CLOEXEC) != 0);
            first = std::move(second);
            auto final = std::move(first);
            assert(!first.is_open() && !second.is_open());
            final.Sync();
        }
        limited.rlim_cur = static_cast<rlim_t>(probe_a);
        assert(::setrlimit(RLIMIT_NOFILE, &limited) == 0);
        const auto error = Refuse([&] { (void)output.FlushAndPin(); });
        assert(::setrlimit(RLIMIT_NOFILE, &original) == 0);
        assert(error == std::errc::too_many_files_open && output.good() && output.is_open());
        output.write("+tail", 5U);
        { auto pin = output.FlushAndPin(); pin.Sync(); }
        output.close();
        assert(output.good() && Read(path) == "first+tail");
        std::_Exit(0);
    }
    int status = 0;
    assert(::waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}
#endif

#if defined(__APPLE__) && defined(CHUNKDB_FAILPOINTS)
void FullSyncFailurePreservesOwnership() {
    test::ScopedTempDir directory("chunkdb-wal-pin-full-sync");
    const auto path = directory.path() / "wal";
    WalAppendStream output(path, kAppend);
    output.write("before", 6U);
    auto pin = output.FlushAndPin();
    constexpr auto key = "CHUNKDB_FAILPOINT_FULL_SYNC_FILE_FAIL_ONCE";
    assert(::setenv(key, "1", 1) == 0);
    const auto error = Refuse([&] { pin.Sync(); });
    const auto* remaining = std::getenv(key);
    assert(remaining == nullptr || remaining[0] == '\0');
    assert(::unsetenv(key) == 0);
    assert(error == std::errc::io_error && pin.is_open() && output.good() && output.is_open());
    pin.Sync();
    output.write("+after", 6U);
    auto next = output.FlushAndPin();
    next.Sync();
    assert(Read(path) == "before+after");
}
#endif

#if defined(__linux__)
void FullDeviceFlushFailure() {
    if (!std::filesystem::exists("/dev/full")) return;
    WalAppendStream output;
    output.open("/dev/full", kAppend, true);
    assert(output.good() && output.is_open());
    output.write("buffered", 8U);
    assert(output.good());
    const auto error = Refuse([&] { (void)output.FlushAndPin(); });
    assert(error == std::errc::no_space_on_device && output.error() == ENOSPC && output.is_open());
    assert(Refuse([&] { (void)output.FlushAndPin(); }) == error);
    output.close();
    assert(output.error() == ENOSPC);
}
#endif
}

int main() {
    NativeGenerationAndBufferVisibility();
    MoveOwnership();
    FailedAndClosedStreams();
#ifndef _WIN32
    DescriptorOwnershipAndExhaustion();
#endif
#if defined(__APPLE__) && defined(CHUNKDB_FAILPOINTS)
    FullSyncFailurePreservesOwnership();
#endif
#if defined(__linux__)
    FullDeviceFlushFailure();
#endif
}
