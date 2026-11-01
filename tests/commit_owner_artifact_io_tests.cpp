#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "checkpoint.hpp"
#include "commit_owner_artifact_identity.hpp"
#include "commit_owner_artifact_io.hpp"
#include "directory_alias_test_utils.hpp"
#include "owner_artifact_allocation_guard.hpp"
#include "process_lock.hpp"
#include "test_utils.hpp"

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
const CommitOwnerScope kScope{{1U}, {2U}, CommitOwnerRole::kCatalog};
constexpr const char* kObjects = "chunkdb.groups.objects";
constexpr const char* kRoot = "chunkdb.groups.owner";
std::string Hex(std::span<const std::uint8_t> bytes) {
    const char* digits = "0123456789abcdef"; std::string value;
    for (auto byte : bytes) { value += digits[byte >> 4U]; value += digits[byte & 15U]; }
    return value;
}
void Save(const std::filesystem::path& path, std::span<const std::uint8_t> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close(); assert(output.good());
}
template <typename F> void Reject(F action) {
    bool rejected = false;
    try { action(); }
    catch (const std::runtime_error&) { rejected = true; }
    catch (const std::invalid_argument&) { rejected = true; }
    assert(rejected);
}
CommitOwnerRoot Inactive() { CommitOwnerRoot value; value.scope = kScope; return value; }
CommitOwnerPage Page(std::uint64_t generation = 2U) {
    CommitOwnerPage value; value.scope = kScope; value.generation = generation; return value;
}
struct Fixture {
    test::ScopedTempDir temporary{"chunkdb-owner-artifacts"};
    std::filesystem::path root = std::filesystem::canonical(temporary.path());
    ProcessLock lock{root};
    const std::array<std::uint8_t, kCommitOwnerRootBytes> selected = SerializeCommitOwnerRoot(Inactive());
    Fixture() { Save(root / kRoot, selected); Save(root / "sentinel", Bytes{'s'}); }
    void Unchanged() const {
        assert(LoadFile(root / kRoot) == Bytes(selected.begin(), selected.end()));
        assert(LoadFile(root / "sentinel") == Bytes{'s'});
    }
};
struct Observers {
    ~Observers() { SetFileSyncObserverForTests({}); SetDirectorySyncObserverForTests({}); }
};

void CompleteNativeIdentity() {
    CommitOwnerNativeIdentity first; first.volume = 7U; first.file[0U] = 3U;
    auto next = first; next.file[8U] = 1U;
    assert(first != next);  // Same low64, different upper64.
    next = first; next.file[15U] = 1U; assert(first != next);
    next = first; next.volume |= std::uint64_t{1U} << 40U; assert(first != next);
    assert(first == first);
#ifdef _WIN32
    test::ScopedTempDir directory("chunkdb-owner-full-native-id");
    const auto original = directory.path() / "original", alias = directory.path() / "alias", distinct = directory.path() / "distinct";
    Save(original, Bytes{'a'}); Save(distinct, Bytes{'a'});
    std::filesystem::create_hard_link(original, alias);
    const auto open = [](const auto& path) {
        const auto handle = CreateFileW(path.c_str(), GENERIC_READ | FILE_READ_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
        assert(handle != INVALID_HANDLE_VALUE); return handle;
    };
    const auto own = open(original), linked = open(alias), other = open(distinct);
    const auto identity = ReadCommitOwnerNativeIdentity(own);
    assert(identity == ReadCommitOwnerNativeIdentity(linked));
    assert(identity != ReadCommitOwnerNativeIdentity(other));
    assert(CloseHandle(own) != 0 && CloseHandle(linked) != 0 && CloseHandle(other) != 0);
#endif
}

void RoundTripAndReadOnly() {
    Fixture f;
    CommitOwnerArtifactIo reader(f.root, kScope);
    const auto selected = reader.ReadCanonicalRoot();
    assert(selected && selected->value == Inactive() && selected->reference.bytes == kCommitOwnerRootBytes);
    assert(!std::filesystem::exists(f.root / kObjects));
    Reject([&] { (void)reader.StageInactive(); });
    CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
    const auto inactive = writer.StageInactive(); assert(inactive.installed);
    assert(writer.ReadRoot(inactive.reference, 1U).value == Inactive());
    assert(!writer.StageInactive().installed);
    const auto page = writer.StagePage(Page()); assert(page.installed);
    assert(reader.ReadPage(page.reference, CommitOwnerTreeKind::kCoverage, 2U).value == Page());
    Reject([&] { (void)reader.ReadPage(page.reference, CommitOwnerTreeKind::kGroups, 2U); });
    Reject([&] { (void)reader.ReadPage(page.reference, CommitOwnerTreeKind::kCoverage, 1U); });
    Reject([&] { (void)reader.ReadRoot(inactive.reference, 2U); });
    auto bad_kind = page.reference; bad_kind.kind = CommitOwnerObjectKind::kObservation;
    Reject([&] { (void)reader.ReadPage(bad_kind, CommitOwnerTreeKind::kCoverage, 2U); });
    auto bad_length = page.reference; ++bad_length.bytes;
    Reject([&] { (void)reader.ReadPage(bad_length, CommitOwnerTreeKind::kCoverage, 2U); });
    auto foreign = Page(); ++foreign.scope.owner_id[0U];
    const auto foreign_leaf = f.root / kObjects / Hex(crypto::Sha256Of(SerializeCommitOwnerPage(foreign)));
    Reject([&] { (void)writer.StagePage(foreign); });
    assert(!std::filesystem::exists(foreign_leaf));
    auto wrong_scope = kScope; wrong_scope.role = CommitOwnerRole::kStandalone;
    CommitOwnerArtifactIo wrong_reader(f.root, wrong_scope);
    Reject([&] { (void)wrong_reader.ReadCanonicalRoot(); });
    f.Unchanged();
}
void WriterAndNamespaceAdmission() {
    Fixture f;
    test::ScopedTempDir other("chunkdb-owner-wrong-lock");
    Reject([&] { CommitOwnerArtifactIo writer(other.path(), kScope, f.lock, false); });
    Reject([&] { CommitOwnerArtifactIo writer(f.root, kScope, f.lock, true); });
    assert(!std::filesystem::exists(other.path() / kObjects));
    assert(!std::filesystem::exists(f.root / kObjects));
    std::filesystem::create_directory(f.root / kObjects);
    const auto foreign = f.root / kObjects / "foreign"; Save(foreign, Bytes{'f'});
    CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
    Reject([&] { (void)writer.StageInactive(); });
    Reject([&] { (void)writer.StageInactive(); });
    assert(LoadFile(foreign) == Bytes{'f'} && !std::filesystem::exists(f.root / kObjects / ".scope"));
    std::filesystem::remove(foreign);
    auto wrong_generation = Inactive(); wrong_generation.generation = 2U;
    const auto noninitial_marker = SerializeCommitOwnerRoot(wrong_generation);
    Save(f.root / kObjects / ".scope", noninitial_marker);
    Reject([&] { (void)writer.StageInactive(); });
    assert(LoadFile(f.root / kObjects / ".scope") == Bytes(noninitial_marker.begin(), noninitial_marker.end()));
    auto bad_scope = Inactive(); bad_scope.scope.owner_id[0U] = 9U;
    const auto marker = SerializeCommitOwnerRoot(bad_scope); Save(f.root / kObjects / ".scope", marker);
    Reject([&] { (void)writer.StageInactive(); });
    assert(LoadFile(f.root / kObjects / ".scope") == Bytes(marker.begin(), marker.end()));
    f.Unchanged();
}
void WriterRootGenerationAndCleanup() {
#ifndef _WIN32
    test::ScopedTempDir temporary("chunkdb-owner-root-lease");
    const auto directory = std::filesystem::canonical(temporary.path());
    auto original_lock = std::make_unique<ProcessLock>(directory);
    const auto selected = SerializeCommitOwnerRoot(Inactive()); Save(directory / kRoot, selected);
    const auto moved = directory.parent_path() / (directory.filename().string() + "-original");
    std::filesystem::rename(directory, moved);
    std::filesystem::create_directory(directory);
    std::filesystem::create_directory(directory / ".chunkdb.lock");
    std::filesystem::create_hard_link(moved / ".chunkdb.lock/writer.lock", directory / ".chunkdb.lock/writer.lock");
    Save(directory / "foreign", Bytes{'f'});
    Reject([&] { CommitOwnerArtifactIo wrong(directory, kScope, *original_lock, false); });
    assert(LoadFile(directory / "foreign") == Bytes{'f'} && !std::filesystem::exists(directory / kObjects));
    // Join the existing heartbeat before removing names it may still visit.
    original_lock.reset();
    std::filesystem::remove_all(directory);
    std::filesystem::rename(moved, directory);
    ProcessLock stable_lock(directory);
    const auto count_root = [&] {
        struct stat root{}; assert(::stat(directory.c_str(), &root) == 0);
        unsigned count = 0U;
        for (int fd = 0; fd < 512; ++fd) {
            struct stat opened{};
            if (::fstat(fd, &opened) == 0 && S_ISDIR(opened.st_mode) &&
                opened.st_dev == root.st_dev && opened.st_ino == root.st_ino) ++count;
        }
        return count;
    };
    assert(count_root() == 1U);  // The retained ProcessLock directory owner.
    for (unsigned attempt = 0U; attempt < 8U; ++attempt) {
        auto invalid = kScope; invalid.owner_id = {};
        Reject([&] { CommitOwnerArtifactIo rejected(directory, invalid); });
        { CommitOwnerArtifactIo reader(directory, kScope); assert(count_root() == 2U); }
        assert(count_root() == 1U);
        Reject([&] { ProcessLock duplicate(directory); });
        assert(count_root() == 1U);
    }
    assert(LoadFile(directory / kRoot) == Bytes(selected.begin(), selected.end()));
#endif
}
void RawBindingAndPinnedDirectory() {
    Fixture f;
    CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
    const auto staged = writer.StagePage(Page());
    const auto path = f.root / kObjects / Hex(staged.reference.sha256);
    const auto original = LoadFile(path);
    const auto replacement = SerializeCommitOwnerPage(Page(3U));
    assert(replacement.size() == original.size() && replacement != original);
    Save(path, replacement);
    Reject([&] { (void)writer.ReadPage(staged.reference, CommitOwnerTreeKind::kCoverage, 3U); });
    Reject([&] { (void)writer.StagePage(Page()); });
    Save(path, original);
    // A harmless hard link remains an admitted plain-file generation.
    const auto hardlink = f.root / "hardlink"; std::filesystem::create_hard_link(path, hardlink);
    assert(writer.ReadPage(staged.reference, CommitOwnerTreeKind::kCoverage, 2U).value == Page());
#ifndef _WIN32
    // Replacing the lexical object directory cannot retarget its pinned owner.
    const auto moved = f.root / "held-object-generation";
    std::filesystem::rename(f.root / kObjects, moved);
    std::filesystem::create_directory(f.root / kObjects);
    Save(f.root / kObjects / "foreign", Bytes{'x'});
    const auto next = writer.StagePage(Page(4U));
    assert(std::filesystem::exists(moved / Hex(next.reference.sha256)));
    assert(!std::filesystem::exists(f.root / kObjects / Hex(next.reference.sha256)));
    assert(LoadFile(f.root / kObjects / "foreign") == Bytes{'x'});
    std::filesystem::remove_all(f.root / kObjects);
    std::filesystem::rename(moved, f.root / kObjects);
#endif
    f.Unchanged();
}
void PinnedRootReadAfterRename() {
    test::ScopedTempDir temporary("chunkdb-owner-pinned-reader");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto selected = SerializeCommitOwnerRoot(Inactive()); Save(root / kRoot, selected);
    CommitOwnerArtifactIo reader(root, kScope);
    const auto moved = root.parent_path() / (root.filename().string() + "-reader");
#ifdef _WIN32
    assert(MoveFileExW(root.c_str(), moved.c_str(), MOVEFILE_WRITE_THROUGH) != 0);
#else
    std::filesystem::rename(root, moved);
#endif
    std::filesystem::create_directory(root);
    auto foreign = Inactive(); foreign.scope.owner_id[0U] = 9U;
    const auto bytes = SerializeCommitOwnerRoot(foreign); Save(root / kRoot, bytes);
    // The opened root owns the old generation; this must return its old body.
    assert(reader.ReadCanonicalRoot()->value == Inactive());
    assert(LoadFile(root / kRoot) == Bytes(bytes.begin(), bytes.end()));
    std::filesystem::remove_all(root);
    std::filesystem::rename(moved, root);
    assert(LoadFile(root / kRoot) == Bytes(selected.begin(), selected.end()));
}
void NativeTypeAndExtentAdmission() {
    Fixture f;
    CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
    const auto staged = writer.StagePage(Page());
    const auto path = f.root / kObjects / Hex(staged.reference.sha256);
    const auto bytes = LoadFile(path);
    Save(path, Bytes{});
    Reject([&] { (void)writer.ReadPage(staged.reference, CommitOwnerTreeKind::kCoverage, 2U); });
    Save(path, bytes);
#ifdef _WIN32
    const auto sparse = CreateFileW(path.c_str(), GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
    assert(sparse != INVALID_HANDLE_VALUE);
    DWORD returned = 0U;
    assert(DeviceIoControl(sparse, FSCTL_SET_SPARSE, nullptr, 0U, nullptr, 0U, &returned, nullptr) != 0);
    assert(CloseHandle(sparse) != 0);
#endif
    std::filesystem::resize_file(path, std::uint64_t{1} << 30U);
    {
        owner_artifact_allocation::Guard guard(128U * 1024U);
        const auto before = owner_artifact_allocation::calls.load();
        auto* positive = ::operator new(64U); ::operator delete(positive);
        assert(owner_artifact_allocation::calls.load() > before);
        const auto denied = owner_artifact_allocation::denied.load();
        Reject([&] { (void)writer.ReadPage(staged.reference, CommitOwnerTreeKind::kCoverage, 2U); });
        assert(owner_artifact_allocation::denied.load() == denied);
    }
    std::filesystem::resize_file(path, bytes.size());
    std::filesystem::remove(path);
    std::filesystem::create_directory(path);
    Reject([&] { (void)writer.ReadPage(staged.reference, CommitOwnerTreeKind::kCoverage, 2U); });
    std::filesystem::remove(path);
#ifndef _WIN32
    // O_NONBLOCK must reach regular-file admission instead of blocking in open.
    for (const auto& leaf : {path, f.root / kRoot, f.root / kObjects / ".scope"}) {
        const bool selected = leaf == f.root / kRoot;
        const auto previous = std::filesystem::exists(leaf) ? LoadFile(leaf) : Bytes{};
        if (std::filesystem::exists(leaf)) std::filesystem::remove(leaf);
        assert(::mkfifo(leaf.c_str(), 0600) == 0);
        if (selected) Reject([&] { (void)writer.ReadCanonicalRoot(); });
        else Reject([&] { (void)writer.ReadPage(staged.reference, CommitOwnerTreeKind::kCoverage, 2U); });
        assert(std::filesystem::is_fifo(std::filesystem::symlink_status(leaf)));
        std::filesystem::remove(leaf);
        if (!previous.empty()) Save(leaf, previous);
    }
    // Link and directory leaves refuse without following or repairing them.
    std::filesystem::create_symlink(f.root / "sentinel", path);
    Reject([&] { (void)writer.ReadPage(staged.reference, CommitOwnerTreeKind::kCoverage, 2U); });
    assert(LoadFile(f.root / "sentinel") == Bytes{'s'});
    std::filesystem::remove(path);
    std::filesystem::create_symlink(f.root / kObjects, f.root / "object-alias");
    std::filesystem::rename(f.root / kObjects, f.root / "held-objects");
    std::filesystem::create_directory_symlink(f.root / "held-objects", f.root / kObjects);
    Reject([&] { (void)writer.ReadPage(staged.reference, CommitOwnerTreeKind::kCoverage, 2U); });
    std::filesystem::remove(f.root / kObjects);
    std::filesystem::rename(f.root / "held-objects", f.root / kObjects);
#endif
    f.Unchanged();
}
void ReparseAdmission() {
    Fixture f;
    CommitOwnerObjectRef reference;
    {
        CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
        reference = writer.StagePage(Page()).reference;
    }
    const auto held = f.root / "held-objects";
    std::filesystem::rename(f.root / kObjects, held);
    {
        test::ScopedDirectoryAlias alias(held, f.root / kObjects);
        CommitOwnerArtifactIo reader(f.root, kScope);
        Reject([&] { (void)reader.ReadPage(reference, CommitOwnerTreeKind::kCoverage, 2U); });
        CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
        Reject([&] { (void)writer.StageInactive(); });
    }
    std::filesystem::rename(held, f.root / kObjects);
    for (const auto& path : {f.root / kRoot, f.root / kObjects / ".scope", f.root / kObjects / Hex(reference.sha256)}) {
        const auto previous = LoadFile(path);
        std::filesystem::remove(path);
        {
            test::ScopedDirectoryAlias alias(f.root, path);
            CommitOwnerArtifactIo reader(f.root, kScope);
            if (path == f.root / kRoot) Reject([&] { (void)reader.ReadCanonicalRoot(); });
            else Reject([&] { (void)reader.ReadPage(reference, CommitOwnerTreeKind::kCoverage, 2U); });
        }
        Save(path, previous);
    }
    const auto alias_path = f.root / "root-alias";
    {
        test::ScopedDirectoryAlias alias(f.root, alias_path);
        Reject([&] { CommitOwnerArtifactIo reader(alias_path, kScope); });
    }
    f.Unchanged();
}
void ReuseSettlesDurability() {
    Fixture f;
    CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
    (void)writer.StageInactive();
    const auto expected = SerializeCommitOwnerPage(Page());
    const auto leaf = Hex(crypto::Sha256Of(expected));
    Observers observers;
    bool failed_once = false;
    SetDirectorySyncObserverForTests([&](const auto& path) {
        if (path == f.root / kObjects && !failed_once) {
            failed_once = true; throw std::runtime_error("injected directory sync uncertainty");
        }
    });
    Reject([&] { (void)writer.StagePage(Page()); });
    assert(failed_once && LoadFile(f.root / kObjects / leaf) == expected);
    unsigned files = 0U, directories = 0U, markers = 0U;
    SetFileSyncObserverForTests([&](const auto& path) {
        if (path == f.root / kObjects / leaf) ++files;
        if (path == f.root / kObjects / ".scope") ++markers;
    });
    SetDirectorySyncObserverForTests([&](const auto& path) { if (path == f.root / kObjects) ++directories; });
    const auto reused = writer.StagePage(Page());
    assert(!reused.installed && files == 1U && directories == 1U);
    CommitOwnerArtifactIo reopened(f.root, kScope, f.lock, false);
    const auto reused_again = reopened.StagePage(Page());
    assert(!reused_again.installed && files == 2U && markers == 1U && directories == 3U);
    f.Unchanged();
}
void FileSyncFailureDoesNotPublish() {
    Fixture f;
    CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
    Observers observers;
    bool marker_failed = false;
    SetFileSyncObserverForTests([&](const auto& path) {
        if (path.filename().string().starts_with(".owner-marker-")) {
            marker_failed = true; throw std::runtime_error("injected marker sync failure");
        }
    });
    Reject([&] { (void)writer.StageInactive(); });
    assert(marker_failed && !std::filesystem::exists(f.root / kObjects / ".scope"));
    SetFileSyncObserverForTests({});
    (void)writer.StageInactive();
    const auto page = SerializeCommitOwnerPage(Page());
    const auto digest = f.root / kObjects / Hex(crypto::Sha256Of(page));
    bool file_failed = false;
    SetFileSyncObserverForTests([&](const auto& path) {
        if (path.filename().string().starts_with(".owner-stage-")) {
            file_failed = true; throw std::runtime_error("injected object sync failure");
        }
    });
    Reject([&] { (void)writer.StagePage(Page()); });
    assert(file_failed && !std::filesystem::exists(digest));
    SetFileSyncObserverForTests({});
    assert(writer.StagePage(Page()).installed && LoadFile(digest) == page);
    f.Unchanged();
}
void TemporaryGenerationSubstitution() {
#ifndef _WIN32
    Fixture f;
    CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
    (void)writer.StageInactive();
    const auto bytes = SerializeCommitOwnerPage(Page());
    const auto sentinel = f.root / "foreign-generation";
    Save(sentinel, bytes);
    const auto held = f.root / "owned-generation";
    bool substituted = false;
    Observers observers;
    SetFileSyncObserverForTests([&](const auto& path) {
        if (!substituted && path.filename().string().starts_with(".owner-stage-")) {
            substituted = true;
            std::filesystem::rename(path, held);
            std::filesystem::create_hard_link(sentinel, path);
        }
    });
    Reject([&] { (void)writer.StagePage(Page()); });
    assert(substituted && LoadFile(sentinel) == bytes && LoadFile(held) == bytes);
    const auto digest = f.root / kObjects / Hex(crypto::Sha256Of(bytes));
    assert(std::filesystem::equivalent(digest, sentinel));
    assert(!std::filesystem::equivalent(digest, held));
    f.Unchanged();
#endif
}
void PublicationDoesNotReplaceAnArrival() {
    Fixture f;
    CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
    (void)writer.StageInactive();
    const auto bytes = SerializeCommitOwnerPage(Page());
    const auto foreign = SerializeCommitOwnerPage(Page(3U));
    const auto leaf = f.root / kObjects / Hex(crypto::Sha256Of(bytes));
    bool arrived = false;
    Observers observers;
    SetFileSyncObserverForTests([&](const auto& path) {
        if (!arrived && path.filename().string().starts_with(".owner-stage-")) {
            arrived = true; Save(leaf, foreign);
        }
    });
    Reject([&] { (void)writer.StagePage(Page()); });
    assert(arrived && LoadFile(leaf) == foreign);
    f.Unchanged();
}

#ifdef _WIN32
std::wstring Quote(const std::wstring& input) {
    std::wstring result = L"\""; std::size_t slashes = 0U;
    for (const auto ch : input) {
        if (ch == L'\\') { ++slashes; continue; }
        result.append(ch == L'"' ? slashes * 2U + 1U : slashes, L'\\'); result += ch; slashes = 0U;
    }
    result.append(slashes * 2U, L'\\'); return result + L'"';
}
#endif
int CrashChild(const std::filesystem::path& executable, const std::filesystem::path& root, unsigned point) {
#ifdef _WIN32
    auto command = Quote(executable.wstring()) + L" --crash " + Quote(root.wstring()) + L" " + std::to_wstring(point);
    STARTUPINFOW startup{}; startup.cb = sizeof(startup); PROCESS_INFORMATION process{};
    assert(CreateProcessW(executable.c_str(), command.data(), nullptr, nullptr, FALSE, 0U, nullptr, nullptr,
        &startup, &process) != 0);
    const auto waited = WaitForSingleObject(process.hProcess, 30'000U);
    if (waited != WAIT_OBJECT_0) {
        (void)TerminateProcess(process.hProcess, 124U); (void)WaitForSingleObject(process.hProcess, INFINITE);
    }
    DWORD result = 0U; assert(GetExitCodeProcess(process.hProcess, &result) != 0);
    assert(CloseHandle(process.hThread) != 0 && CloseHandle(process.hProcess) != 0);
    assert(waited == WAIT_OBJECT_0); return static_cast<int>(result);
#else
    const auto number = std::to_string(point);
    const auto pid = ::fork(); assert(pid >= 0);
    if (pid == 0) { ::execl(executable.c_str(), executable.c_str(), "--crash", root.c_str(), number.c_str(), nullptr); std::_Exit(127); }
    int status = 0; assert(::waitpid(pid, &status, 0) == pid && WIFEXITED(status)); return WEXITSTATUS(status);
#endif
}
void Crash(unsigned point, const std::filesystem::path& root) {
    ProcessLock lock(root);
    CommitOwnerArtifactIo writer(root, kScope, lock, false);
    unsigned object_directories = 0U;
    SetDirectorySyncObserverForTests([&](const auto& path) {
        if ((point == 0U && path == root) ||
            (path == root / kObjects && ++object_directories == (point == 2U ? 1U : 2U) && (point == 2U || point == 4U)))
            std::_Exit(86);
    });
    SetFileSyncObserverForTests([&](const auto& path) {
        const auto name = path.filename().string();
        if ((point == 1U && name.starts_with(".owner-marker-")) || (point == 3U && name.starts_with(".owner-stage-")))
            std::_Exit(86);
    });
    (void)writer.StagePage(Page());
    std::_Exit(87);
}
void CrashBoundaries(const std::filesystem::path& executable) {
    for (unsigned point = 0U; point <= 4U; ++point) {
        test::ScopedTempDir temporary("chunkdb-owner-artifact-crash");
        const auto root = std::filesystem::canonical(temporary.path());
        const auto selected = SerializeCommitOwnerRoot(Inactive()); Save(root / kRoot, selected);
        Save(root / "sentinel", Bytes{'s'});
        assert(CrashChild(executable, root, point) == 86);
        ProcessLock lock(root); CommitOwnerArtifactIo writer(root, kScope, lock, false);
        const auto staged = writer.StagePage(Page());
        assert(writer.ReadPage(staged.reference, CommitOwnerTreeKind::kCoverage, 2U).value == Page());
        assert(LoadFile(root / kRoot) == Bytes(selected.begin(), selected.end()));
        assert(LoadFile(root / "sentinel") == Bytes{'s'});
    }
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::string(argv[1]) == "--crash") { Crash(static_cast<unsigned>(std::stoul(argv[3])), argv[2]); return 2; }
    using Case = std::pair<std::string_view, void(*)()>;
    const std::array cases{
        Case{"identity", CompleteNativeIdentity},
        Case{"roundtrip", RoundTripAndReadOnly}, Case{"namespace", WriterAndNamespaceAdmission},
        Case{"lease", WriterRootGenerationAndCleanup}, Case{"binding", RawBindingAndPinnedDirectory},
        Case{"pinned-root", PinnedRootReadAfterRename}, Case{"types", NativeTypeAndExtentAdmission},
        Case{"reparse", ReparseAdmission}, Case{"reuse", ReuseSettlesDurability},
        Case{"sync-failure", FileSyncFailureDoesNotPublish}, Case{"substitution", TemporaryGenerationSubstitution},
        Case{"no-replace", PublicationDoesNotReplaceAnArrival}};
    if (argc == 3 && std::string_view(argv[1]) == "--case") {
        for (const auto& [name, run] : cases) if (name == argv[2]) { run(); return 0; }
        if (std::string_view(argv[2]) == "crash") { CrashBoundaries(std::filesystem::absolute(argv[0])); return 0; }
        return 2;
    }
    if (argc != 1) return 2;
    for (const auto& [name, run] : cases) { (void)name; run(); }
    CrashBoundaries(std::filesystem::absolute(argv[0]));
}
