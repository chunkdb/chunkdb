#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include "checkpoint.hpp"
#include "file_permissions.hpp"
#include "test_utils.hpp"

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;

void Env(const char* name, const char* value) {
#ifdef _WIN32
    _putenv_s(name, value);
#else
    if (*value == '\0') unsetenv(name); else setenv(name, value, 1);
#endif
}

void ProducerFailureDoesNotPublish() {
    test::ScopedTempDir directory("chunkdb-stream-producer-failure");
    const auto path = directory.path() / "index";
    const Bytes previous{1, 2, 3}, first{4, 0, 5};
    AtomicWrite(path, previous, true, true);
    for (const bool write_prefix : {false, true}) {
        bool replaced = true, failed = false;
        try {
            AtomicWriteStream(path, [&](const AtomicFileSink& sink) {
                if (write_prefix) sink(first);
                throw std::invalid_argument("invalid index record");
            }, true, true, &replaced);
        } catch (const std::invalid_argument&) { failed = true; }
        assert(failed && !replaced && LoadFile(path) == previous);
        std::size_t count = 0;
        for (const auto& item : std::filesystem::directory_iterator(directory.path())) {
            assert(item.path() == path);
            ++count;
        }
        assert(count == 1U);
    }
    bool replaced_by_nonstandard = true, nonstandard_failed = false;
    try {
        AtomicWriteStream(path, [&](const AtomicFileSink& sink) { sink(first); throw 7; },
            true, true, &replaced_by_nonstandard);
    } catch (int value) { nonstandard_failed = value == 7; }
    assert(nonstandard_failed && !replaced_by_nonstandard && LoadFile(path) == previous);
    for (const auto& item : std::filesystem::directory_iterator(directory.path())) assert(item.path() == path);
    const Bytes second{6, 7, 0, 8};
    bool replaced = false;
    AtomicWriteStream(path, [&](const AtomicFileSink& sink) { sink(first); sink({}); sink(second); },
        true, true, &replaced);
    assert(replaced && LoadFile(path) == (Bytes{4, 0, 5, 6, 7, 0, 8}));
    assert(FileIsOwnerOnly(path));
}

void IoFailureRetainsThePublicationBoundary() {
    if (!kFailpointsCompiled) return;
    test::ScopedTempDir directory("chunkdb-stream-io-failure");
    const auto path = directory.path() / "index";
    const Bytes previous{1, 2, 3}, next{4, 5, 6};
    AtomicWrite(path, previous, true, true);
    for (const auto* point : {"CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_WRITE_FAIL_ONCE",
            "CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_SYNC_FAIL_ONCE",
            "CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_CLOSE_FAIL_ONCE"}) {
        Env(point, "1");
        bool replaced = true, failed = false;
        try { AtomicWriteStream(path, [&](const AtomicFileSink& sink) { sink(next); }, true, true, &replaced); }
        catch (const std::runtime_error&) { failed = true; }
        Env(point, "");
        assert(failed && !replaced && LoadFile(path) == previous);
    }
    Env("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
    bool replaced = false, failed = false;
    try { AtomicWriteStream(path, [&](const AtomicFileSink& sink) { sink(next); }, true, true, &replaced); }
    catch (const std::runtime_error&) { failed = true; }
    Env("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "");
    assert(failed && replaced && LoadFile(path) == next);
    assert(FileIsOwnerOnly(path));
}

#ifndef _WIN32
// A failed close is never repeated: the descriptor may be released already
// and its number taken by another open, here a sentinel opened in that
// number right after the close.
void FailedCloseIsNotRepeated() {
    if (!kFailpointsCompiled) return;
    test::ScopedTempDir directory("chunkdb-close-once");
    const auto path = directory.path() / "index";
    const auto sentinel_path = directory.path() / "sentinel";
    const Bytes previous{1, 2, 3};
    AtomicWrite(path, previous, true, true);
    { std::ofstream(sentinel_path) << "sentinel"; }
    const std::vector<std::function<void()>> steps{
        [&] { AtomicWrite(path, Bytes{4, 5, 6}, true, true); },  // the temporary file
        [&] { SyncFilePath(path); },
        [&] { SyncDirectoryPath(directory.path()); },
    };
    for (const auto& step : steps) {
        int sentinel = -1;
        SetDescriptorCloseObserverForTests([&](int closed) {
            if (sentinel >= 0) return;
            const int opened = ::open(sentinel_path.c_str(), O_RDONLY);
            assert(opened >= 0);
            if (opened != closed) {
                assert(::dup2(opened, closed) == closed);
                (void)::close(opened);
            }
            sentinel = closed;
        });
        Env("CHUNKDB_FAILPOINT_CLOSE_INTERRUPTED_ONCE", "1");
        bool failed = false;
        try { step(); } catch (const std::system_error&) { failed = true; }
        Env("CHUNKDB_FAILPOINT_CLOSE_INTERRUPTED_ONCE", "");
        SetDescriptorCloseObserverForTests({});
        assert(failed && sentinel >= 0);
        // Still open: nothing closed the number again.
        assert(::fcntl(sentinel, F_GETFD) != -1);
        (void)::close(sentinel);
    }
    assert(LoadFile(path) == previous);
}
#endif
}

int main() {
    ProducerFailureDoesNotPublish();
    IoFailureRetainsThePublicationBoundary();
#ifndef _WIN32
    FailedCloseIsNotRepeated();
#endif
}
