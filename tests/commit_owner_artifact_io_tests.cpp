#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
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
    ProcessLock lock{root, ProcessLock::PinRootForArtifacts{}};
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
    auto original_lock = std::make_unique<ProcessLock>(directory, ProcessLock::PinRootForArtifacts{});
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
    ProcessLock stable_lock(directory, ProcessLock::PinRootForArtifacts{});
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

constexpr const char* kJournals = "chunkdb.groups.journal";
const CommitJournalContext kJournal{kScope.directory_id, CommitGroupId{0x5AU}, 1U};
std::filesystem::path JournalLeaf(const std::filesystem::path& root, const CommitJournalContext& context = kJournal) {
    return root / kJournals / CommitJournalLeafName(context);
}
Bytes JournalHeader(const CommitJournalContext& context = kJournal) {
    const auto header = SerializeCommitJournalHeader(context); return {header.begin(), header.end()};
}
CommitJournalInventory EmptyInventory() {
    return {kJournal, 1U, {64U, {}, CommitRecordKind::kNone, crypto::Sha256Of(JournalHeader())}};
}
Bytes Decision(const CommitGroupId& id, const CommitGroupId& directory = kScope.directory_id) {
    CommitGroupDecision decision; decision.directory_id = directory; decision.id = id; decision.created_ms = 1U;
    return SerializeCommitGroupDecision(decision);
}
template <typename F> void RejectWith(std::string_view reason, F action) {
    bool rejected = false;
    try { action(); }
    catch (const std::exception& error) {
        rejected = std::string_view(error.what()).find(reason) != std::string_view::npos;
        if (!rejected) std::fprintf(stderr, "unexpected refusal: %s\n", error.what());
    }
    assert(rejected);
}
void MakeSparse(const std::filesystem::path& path, std::uint64_t bytes) {
#ifdef _WIN32
    const auto sparse = CreateFileW(path.c_str(), GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
    assert(sparse != INVALID_HANDLE_VALUE);
    DWORD returned = 0U;
    assert(DeviceIoControl(sparse, FSCTL_SET_SPARSE, nullptr, 0U, nullptr, 0U, &returned, nullptr) != 0);
    assert(CloseHandle(sparse) != 0);
#endif
    std::filesystem::resize_file(path, bytes);
}

void InventoryObjects() {
    Fixture f;
    CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
    const auto inventory = EmptyInventory();
    const auto staged = writer.StageJournalInventory(inventory);
    assert(staged.installed && staged.reference.kind == CommitOwnerObjectKind::kJournalInventory && staged.reference.bytes == 128U);
    assert(writer.ReadJournalInventory(staged.reference).value == inventory);
    const CommitIdInventory ids{kJournal, 1U, inventory.certified, {}};
    const auto id_staged = writer.StageIdInventory(ids);
    const auto id_read = CommitOwnerArtifactIo(f.root, kScope).ReadIdInventory(id_staged.reference, kJournal);
    assert(id_read.value == ids && id_read.header.record_bytes == 164U && id_read.header.entry_count == 0U);
    {
        Observers observers;
        unsigned files = 0U, directories = 0U;
        const auto leaf = f.root / kObjects / Hex(staged.reference.sha256);
        SetFileSyncObserverForTests([&](const auto& path) { if (path == leaf) ++files; });
        SetDirectorySyncObserverForTests([&](const auto& path) { if (path == f.root / kObjects) ++directories; });
        assert(!writer.StageJournalInventory(inventory).installed && files == 1U && directories == 1U);
    }
    auto wrong_kind = staged.reference; wrong_kind.kind = CommitOwnerObjectKind::kIdInventory;
    RejectWith("invalid object reference", [&] { (void)writer.ReadJournalInventory(wrong_kind); });
    RejectWith("invalid object reference", [&] { (void)writer.ReadIdInventory(staged.reference, kJournal); });
    for (const std::uint64_t bytes : {127U, 129U}) {
        auto reference = staged.reference; reference.bytes = bytes;
        RejectWith("invalid object reference", [&] { (void)writer.ReadJournalInventory(reference); });
    }
    for (const std::uint64_t bytes : {std::uint64_t{163U}, std::uint64_t{kMaxCommitIdInventoryBytes} + 1U}) {
        auto reference = id_staged.reference; reference.bytes = bytes;
        RejectWith("invalid object reference", [&] { (void)writer.ReadIdInventory(reference, kJournal); });
    }
    // A valid same-length inventory under the claimed digest fails the raw SHA binding.
    const auto path = f.root / kObjects / Hex(staged.reference.sha256);
    const auto original = LoadFile(path);
    auto other = inventory; other.generation = 2U;
    const auto replacement = SerializeCommitJournalInventory(other);
    assert(replacement.size() == original.size());
    Save(path, replacement);
    RejectWith("raw digest mismatch", [&] { (void)writer.ReadJournalInventory(staged.reference); });
    RejectWith("raw digest mismatch", [&] { (void)writer.StageJournalInventory(inventory); });
    Save(path, original);
    auto other_context = kJournal; other_context.generation = 2U;
    RejectWith("ID inventory context", [&] { (void)writer.ReadIdInventory(id_staged.reference, other_context); });
    auto foreign_directory = kJournal; foreign_directory.directory_id[0U] ^= 0x40U;
    RejectWith("journal context directory mismatch", [&] { (void)writer.ReadIdInventory(id_staged.reference, foreign_directory); });
    auto foreign = inventory; foreign.context = foreign_directory;
    RejectWith("staged inventory directory mismatch", [&] { (void)writer.StageJournalInventory(foreign); });
    // Sparse oversized files refuse from their measured or declared extent before a complete buffer.
    const auto id_path = f.root / kObjects / Hex(id_staged.reference.sha256);
    const auto id_bytes = LoadFile(id_path);
    MakeSparse(id_path, std::uint64_t{1} << 30U);
    auto declared = id_staged.reference; declared.bytes = std::uint64_t{1} << 27U;
    {
        owner_artifact_allocation::Guard guard(128U * 1024U);
        const auto calls = owner_artifact_allocation::calls.load();
        auto* positive = ::operator new(64U); ::operator delete(positive);
        assert(owner_artifact_allocation::calls.load() > calls);
        const auto denied = owner_artifact_allocation::denied.load();
        RejectWith("measured extent mismatch", [&] { (void)writer.ReadIdInventory(id_staged.reference, kJournal); });
        RejectWith("measured extent mismatch", [&] { (void)writer.ReadIdInventory(declared, kJournal); });
        std::filesystem::resize_file(id_path, declared.bytes);
        RejectWith("declared extent mismatch", [&] { (void)writer.ReadIdInventory(declared, kJournal); });
        assert(owner_artifact_allocation::denied.load() == denied);
    }
    std::filesystem::remove(id_path);
    Save(id_path, id_bytes);
    assert(writer.ReadIdInventory(id_staged.reference, kJournal).value == ids);
    f.Unchanged();
}
void JournalCreate() {
    Fixture f;
    RejectWith("read-only artifact view cannot create", [&] { (void)CommitOwnerArtifactIo(f.root, kScope).CreateJournal(kJournal); });
    assert(!std::filesystem::exists(f.root / kJournals));
    CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
    std::vector<std::filesystem::path> files, directories;
    {
        Observers observers;
        SetFileSyncObserverForTests([&](const auto& path) { files.push_back(path); });
        SetDirectorySyncObserverForTests([&](const auto& path) { directories.push_back(path); });
        auto journal = writer.CreateJournal(kJournal);
        assert(journal.writable() && journal.MeasuredBytes() == 64U && journal.context() == kJournal);
    }
    assert(LoadFile(JournalLeaf(f.root)) == JournalHeader());
    // J0 root directory, J1 stage file, J2 final leaf, J3 journal directory.
    assert(directories.size() == 2U && directories[0] == f.root && directories[1] == f.root / kJournals);
    assert(files.size() == 2U && files[0].filename().string().starts_with(".journal-stage-") && files[1] == JournalLeaf(f.root));
    RejectWith("journal generation already exists", [&] { (void)writer.CreateJournal(kJournal); });
    assert(LoadFile(JournalLeaf(f.root)) == JournalHeader());
    {
        auto view = CommitOwnerArtifactIo(f.root, kScope).OpenJournal(kJournal);
        assert(!view.writable());
        const auto scan = view.Scan(EmptyInventory());
        assert(scan.complete_bytes == 64U && scan.measured_bytes == 64U);
        RejectWith("read-only journal view cannot append", [&] { (void)view.Append(Decision(CommitGroupId{0xA1U})); });
        RejectWith("read-only journal view cannot sync", [&] { view.Sync(); });
        auto other = EmptyInventory(); other.context.generation = 2U;
        RejectWith("inventory names another journal generation", [&] { (void)view.Scan(other); });
    }
    auto foreign = kJournal; foreign.directory_id[0U] ^= 0x40U;
    RejectWith("journal context directory mismatch", [&] { (void)writer.OpenJournal(foreign); });
    RejectWith("journal context directory mismatch", [&] { (void)writer.CreateJournal(foreign); });
    auto zero = kJournal; zero.journal_id = {};
    RejectWith("journal context", [&] { (void)writer.CreateJournal(zero); });
    auto second = kJournal; second.generation = 2U;
    Reject([&] { (void)writer.OpenJournal(second); });
    // A leaf holding another generation's header refuses.
    Save(JournalLeaf(f.root, second), JournalHeader());
    RejectWith("journal header does not match its generation", [&] { (void)writer.OpenJournal(second); });
    Save(JournalLeaf(f.root, second), Bytes(10U, 0U));
    RejectWith("journal is shorter than its header", [&] { (void)writer.OpenJournal(second); });
    std::filesystem::remove(JournalLeaf(f.root, second));
#ifndef _WIN32
    auto third = kJournal; third.generation = 3U;
    std::filesystem::create_symlink(f.root / "sentinel", JournalLeaf(f.root, third));
    Reject([&] { (void)writer.OpenJournal(third); });
    Reject([&] { (void)writer.CreateJournal(third); });
    assert(std::filesystem::is_symlink(JournalLeaf(f.root, third)));
    std::filesystem::remove(JournalLeaf(f.root, third));
#endif
    // A redirected journal directory refuses without following it.
    const auto held = f.root / "held-journals";
    std::filesystem::rename(f.root / kJournals, held);
    {
        test::ScopedDirectoryAlias alias(held, f.root / kJournals);
        Reject([&] { (void)CommitOwnerArtifactIo(f.root, kScope).OpenJournal(kJournal); });
        auto fourth = kJournal; fourth.generation = 4U;
        Reject([&] { (void)writer.CreateJournal(fourth); });
        assert(!std::filesystem::exists(held / CommitJournalLeafName(fourth)));
    }
    std::filesystem::rename(held, f.root / kJournals);
    assert(LoadFile(JournalLeaf(f.root)) == JournalHeader());
    f.Unchanged();
}
void JournalPublicationRaces() {
    Fixture f;
    CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
    const auto foreign = JournalHeader(CommitJournalContext{kScope.directory_id, CommitGroupId{0x77U}, 1U});
    {
        Observers observers;
        bool arrived = false;
        SetFileSyncObserverForTests([&](const auto& path) {
            if (!arrived && path.filename().string().starts_with(".journal-stage-")) { arrived = true; Save(JournalLeaf(f.root), foreign); }
        });
        RejectWith("journal generation already exists", [&] { (void)writer.CreateJournal(kJournal); });
        assert(arrived && LoadFile(JournalLeaf(f.root)) == foreign);
    }
    std::filesystem::remove(JournalLeaf(f.root));
#ifndef _WIN32
    const auto sentinel = f.root / "foreign-generation", held = f.root / "owned-generation";
    Save(sentinel, JournalHeader());
    {
        Observers observers;
        bool substituted = false;
        SetFileSyncObserverForTests([&](const auto& path) {
            if (!substituted && path.filename().string().starts_with(".journal-stage-")) {
                substituted = true; std::filesystem::rename(path, held); std::filesystem::create_hard_link(sentinel, path);
            }
        });
        RejectWith("journal generation substituted", [&] { (void)writer.CreateJournal(kJournal); });
        assert(substituted && LoadFile(sentinel) == JournalHeader() && LoadFile(held) == JournalHeader());
        assert(std::filesystem::equivalent(JournalLeaf(f.root), sentinel));
    }
#endif
    f.Unchanged();
}
void JournalAppend() {
    Fixture f;
    CommitOwnerArtifactIo writer(f.root, kScope, f.lock, false);
    const CommitGroupId a{0xA1U}, b{0xB2U}, c{0xC3U};
    const auto leaf = JournalLeaf(f.root);
    const auto first = Decision(a);
    {
        auto journal = writer.CreateJournal(kJournal);
        assert(journal.Append(first) == 64U + first.size());
        const auto receipt = SerializeCommitGroupReceipt({kScope.directory_id, a, crypto::Sha256Of(first)});
        const auto end = journal.Append(receipt);
        assert(end == LoadFile(leaf).size() && journal.MeasuredBytes() == end);
        const auto clean = LoadFile(leaf);
        auto bad_crc = Decision(b); bad_crc.back() ^= 0x01U;
        RejectWith("record at offset", [&] { (void)journal.Append(bad_crc); });
        RejectWith("record at offset", [&] { (void)journal.Append(Decision(b, CommitGroupId{0x99U})); });
        RejectWith("conflicting decisions", [&] { (void)journal.Append(first); });
        assert(LoadFile(leaf) == clean);
        // An outside extension changes the admitted end.
        { std::ofstream output(leaf, std::ios::binary | std::ios::app); output.put('x'); }
        RejectWith("journal end changed", [&] { (void)journal.Append(Decision(b)); });
        std::filesystem::resize_file(leaf, clean.size());
        assert(journal.Append(Decision(b)) == LoadFile(leaf).size());
    }
    const auto clean = LoadFile(leaf);
    {
        auto journal = writer.OpenJournal(kJournal);
        RejectWith("journal end is not admitted", [&] { (void)journal.Append(Decision(c)); });
        const auto scan = journal.Scan(EmptyInventory());
        assert(scan.uncertified.size() == 3U && scan.complete_bytes == clean.size());
        // A torn final record is a tail; appending after it would turn it into damage.
        const auto torn = Decision(c);
        { std::ofstream output(leaf, std::ios::binary | std::ios::app); output.write(reinterpret_cast<const char*>(torn.data()), 70); }
        const auto with_tail = LoadFile(leaf);
        const auto tailed = journal.Scan(EmptyInventory());
        assert(tailed.complete_bytes == clean.size() && tailed.measured_bytes == clean.size() + 70U);
        RejectWith("journal has an uncertified tail", [&] { (void)journal.Append(Decision(c)); });
        assert(LoadFile(leaf) == with_tail);
    }
    std::filesystem::resize_file(leaf, clean.size());
    {
        // A refused scan withdraws the end that an earlier scan admitted.
        auto journal = writer.OpenJournal(kJournal);
        (void)journal.Scan(EmptyInventory());
        auto damaged = clean;
        damaged[24] ^= 0x01U;
        const auto rewrite = [&](std::uint8_t byte) {
            std::fstream header(leaf, std::ios::binary | std::ios::in | std::ios::out);
            header.seekp(24); header.put(static_cast<char>(byte)); header.close(); assert(!header.fail());
        };
        rewrite(damaged[24]);
        RejectWith("header", [&] { (void)journal.Scan(EmptyInventory()); });
        RejectWith("journal end is not admitted", [&] { (void)journal.Append(Decision(c)); });
        assert(LoadFile(leaf) == damaged);
        rewrite(clean[24]);
        (void)journal.Scan(EmptyInventory());
        assert(journal.Append(Decision(c)) == clean.size() + Decision(c).size());
    }
    std::filesystem::resize_file(leaf, clean.size());
    {
        auto journal = writer.OpenJournal(kJournal);
        (void)journal.Scan(EmptyInventory());
        Observers observers;
        bool failed = false;
        SetFileSyncObserverForTests([&](const auto& path) {
            if (path == leaf) { failed = true; throw std::runtime_error("injected journal sync failure"); }
        });
        RejectWith("injected journal sync failure", [&] { journal.Sync(); });
        SetFileSyncObserverForTests({});
        assert(failed);
        RejectWith("journal is fenced", [&] { (void)journal.Append(Decision(c)); });
        RejectWith("journal is fenced", [&] { journal.Sync(); });
        assert(LoadFile(leaf) == clean);
    }
    f.Unchanged();
}
void CanonicalInactive() {
    test::ScopedTempDir temporary("chunkdb-owner-canonical");
    const auto root = std::filesystem::canonical(temporary.path());
    ProcessLock lock(root, ProcessLock::PinRootForArtifacts{});
    Save(root / "sentinel", Bytes{'s'});
    const auto inactive = SerializeCommitOwnerRoot(Inactive());
    const Bytes expected(inactive.begin(), inactive.end());
    {
        CommitOwnerArtifactIo reader(root, kScope);
        RejectWith("read-only artifact view cannot install", [&] { (void)reader.InstallCanonicalInactive(); });
        RejectWith("read-only artifact view cannot install", [&] { (void)reader.SettleCanonicalInactive(); });
    }
    CommitOwnerArtifactIo writer(root, kScope, lock, false);
    RejectWith("canonical owner root is missing", [&] { (void)writer.SettleCanonicalInactive(); });
    assert(!std::filesystem::exists(root / kRoot));
    {
        Observers observers;
        unsigned temporaries = 0U, files = 0U, directories = 0U;
        SetFileSyncObserverForTests([&](const auto& path) {
            if (path.filename().string().starts_with(".owner-root-")) ++temporaries;
            if (path == root / kRoot) ++files;
        });
        SetDirectorySyncObserverForTests([&](const auto& path) { if (path == root) ++directories; });
        const auto installed = writer.InstallCanonicalInactive();
        assert(installed.installed && installed.reference.bytes == 512U && LoadFile(root / kRoot) == expected);
        assert(temporaries == 1U && files == 1U && directories == 1U);
        assert(!writer.InstallCanonicalInactive().installed && files == 2U && directories == 2U);
        assert(!writer.SettleCanonicalInactive().installed && files == 3U && directories == 3U && temporaries == 1U);
    }
    CommitOwnerRoot active;
    active.scope = kScope; active.generation = 2U; active.state = CommitOwnerState::kActive;
    active.coverage_tree = {CommitOwnerObjectKind::kPage, 92U, {1U}};
    active.group_tree = {CommitOwnerObjectKind::kPage, 92U, {2U}};
    active.journal_inventory = {CommitOwnerObjectKind::kJournalInventory, 128U, {3U}};
    active.id_inventory = {CommitOwnerObjectKind::kIdInventory, 164U, {4U}};
    active.reference_context = kJournal; active.reference_replay = EmptyInventory().certified;
    auto other_scope = Inactive(); other_scope.scope.owner_id[0U] = 9U;
    auto second_generation = Inactive(); second_generation.generation = 2U;
    for (const auto& root_value : {active, other_scope, second_generation}) {
        const auto bytes = SerializeCommitOwnerRoot(root_value);
        Save(root / kRoot, bytes);
        RejectWith("not the generation-one INACTIVE root", [&] { (void)writer.InstallCanonicalInactive(); });
        RejectWith("not the generation-one INACTIVE root", [&] { (void)writer.SettleCanonicalInactive(); });
        assert(LoadFile(root / kRoot) == Bytes(bytes.begin(), bytes.end()));
    }
    Save(root / kRoot, Bytes(511U, 0U));
    RejectWith("not the generation-one INACTIVE root", [&] { (void)writer.SettleCanonicalInactive(); });
    std::filesystem::remove(root / kRoot);
    // An identical arrival during publication is admitted; a foreign one is refused unchanged.
    for (const bool identical : {true, false}) {
        const auto arrival = identical ? inactive : SerializeCommitOwnerRoot(other_scope);
        Observers observers;
        bool arrived = false;
        SetFileSyncObserverForTests([&](const auto& path) {
            if (!arrived && path.filename().string().starts_with(".owner-root-")) { arrived = true; Save(root / kRoot, arrival); }
        });
        if (identical) assert(!writer.InstallCanonicalInactive().installed);
        else RejectWith("not the generation-one INACTIVE root", [&] { (void)writer.InstallCanonicalInactive(); });
        assert(arrived && LoadFile(root / kRoot) == Bytes(arrival.begin(), arrival.end()));
        std::filesystem::remove(root / kRoot);
    }
#ifndef _WIN32
    const auto sentinel = root / "foreign-root", held = root / "owned-root";
    Save(sentinel, inactive);
    {
        Observers observers;
        bool substituted = false;
        SetFileSyncObserverForTests([&](const auto& path) {
            if (!substituted && path.filename().string().starts_with(".owner-root-")) {
                substituted = true; std::filesystem::rename(path, held); std::filesystem::create_hard_link(sentinel, path);
            }
        });
        RejectWith("canonical root generation substituted", [&] { (void)writer.InstallCanonicalInactive(); });
        assert(substituted && LoadFile(sentinel) == expected && LoadFile(held) == expected);
    }
#endif
    assert(LoadFile(root / "sentinel") == Bytes{'s'});
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
// Points 0-4 stage a page, 5-7 install the canonical root (I0 temporary sync,
// I1 root sync, I2 root directory sync), and 8-11 create a journal (J0 root
// directory, J1 stage file, J2 leaf, J3 journal directory). A child stops after
// its real sync, so these check outcome shapes after a process crash; sync
// order is checked by the observer counts of the other cases.
void Crash(unsigned point, const std::filesystem::path& root) {
    ProcessLock lock(root, ProcessLock::PinRootForArtifacts{});
    CommitOwnerArtifactIo writer(root, kScope, lock, false);
    unsigned object_directories = 0U;
    SetDirectorySyncObserverForTests([&](const auto& path) {
        if ((point == 0U && path == root) ||
            (path == root / kObjects && ++object_directories == (point == 2U ? 1U : 2U) && (point == 2U || point == 4U)) ||
            ((point == 7U || point == 8U) && path == root) || (point == 11U && path == root / kJournals))
            std::_Exit(86);
    });
    SetFileSyncObserverForTests([&](const auto& path) {
        const auto name = path.filename().string();
        if ((point == 1U && name.starts_with(".owner-marker-")) || (point == 3U && name.starts_with(".owner-stage-")) ||
            (point == 5U && name.starts_with(".owner-root-")) || (point == 6U && path == root / kRoot) ||
            (point == 9U && name.starts_with(".journal-stage-")) || (point == 10U && path == JournalLeaf(root)))
            std::_Exit(86);
    });
    if (point <= 4U) (void)writer.StagePage(Page());
    else if (point <= 7U) (void)writer.InstallCanonicalInactive();
    else (void)writer.CreateJournal(kJournal);
    std::_Exit(87);
}
void CrashBoundaries(const std::filesystem::path& executable) {
    for (unsigned point = 0U; point <= 11U; ++point) {
        test::ScopedTempDir temporary("chunkdb-owner-artifact-crash");
        const auto root = std::filesystem::canonical(temporary.path());
        const auto selected = SerializeCommitOwnerRoot(Inactive());
        const Bytes inactive(selected.begin(), selected.end());
        const bool installing = point >= 5U && point <= 7U;
        if (!installing) Save(root / kRoot, selected);
        Save(root / "sentinel", Bytes{'s'});
        assert(CrashChild(executable, root, point) == 86);
        ProcessLock lock(root, ProcessLock::PinRootForArtifacts{}); CommitOwnerArtifactIo writer(root, kScope, lock, false);
        if (point <= 4U) {
            const auto staged = writer.StagePage(Page());
            assert(writer.ReadPage(staged.reference, CommitOwnerTreeKind::kCoverage, 2U).value == Page());
        } else if (installing) {
            if (point == 5U) {
                assert(!std::filesystem::exists(root / kRoot));
                bool temporary_left = false;
                for (const auto& entry : std::filesystem::directory_iterator(root))
                    temporary_left = temporary_left || entry.path().filename().string().starts_with(".owner-root-");
                assert(temporary_left);
            } else {
                assert(LoadFile(root / kRoot) == inactive);
            }
            assert(writer.InstallCanonicalInactive().installed == (point == 5U));
        } else if (point <= 9U) {
            assert(!std::filesystem::exists(JournalLeaf(root)));
            (void)writer.CreateJournal(kJournal);
            assert(LoadFile(JournalLeaf(root)) == JournalHeader());
        } else {
            assert(LoadFile(JournalLeaf(root)) == JournalHeader());
            RejectWith("journal generation already exists", [&] { (void)writer.CreateJournal(kJournal); });
            auto journal = writer.OpenJournal(kJournal);
            assert(journal.Scan(EmptyInventory()).complete_bytes == 64U);
        }
        assert(LoadFile(root / kRoot) == inactive);
        assert(LoadFile(root / "sentinel") == Bytes{'s'});
    }
}
// Only a lock that pinned its root may stage, and the pin refuses a symlinked
// root before the lock directory is created.
void ExplicitRootPin() {
    test::ScopedTempDir temporary("chunkdb-owner-explicit-pin");
    const auto root = std::filesystem::canonical(temporary.path());
    {
        ProcessLock lock(root);
        Reject([&] { CommitOwnerArtifactIo writer(root, kScope, lock, false); });
        assert(!std::filesystem::exists(root / kObjects));
    }
    const auto target = root / "target";
    const auto alias = root / "alias";
    std::filesystem::create_directory(target);
    std::error_code error;
    std::filesystem::create_directory_symlink(target, alias, error);
    if (error) return;  // Symlink creation may be blocked by policy.
    Reject([&] { ProcessLock pinned(alias, ProcessLock::PinRootForArtifacts{}); });
    assert(!std::filesystem::exists(target / ".chunkdb.lock"));
    std::filesystem::remove(alias);
}
}  // namespace

int main(int argc, char** argv) {
    owner_artifact_allocation::CalibrateAligned();
    if (argc == 4 && std::string(argv[1]) == "--crash") { Crash(static_cast<unsigned>(std::stoul(argv[3])), argv[2]); return 2; }
    using Case = std::pair<std::string_view, void(*)()>;
    const std::array cases{
        Case{"identity", CompleteNativeIdentity},
        Case{"roundtrip", RoundTripAndReadOnly}, Case{"namespace", WriterAndNamespaceAdmission},
        Case{"lease", WriterRootGenerationAndCleanup}, Case{"binding", RawBindingAndPinnedDirectory},
        Case{"pinned-root", PinnedRootReadAfterRename}, Case{"types", NativeTypeAndExtentAdmission},
        Case{"reparse", ReparseAdmission}, Case{"reuse", ReuseSettlesDurability},
        Case{"sync-failure", FileSyncFailureDoesNotPublish}, Case{"substitution", TemporaryGenerationSubstitution},
        Case{"no-replace", PublicationDoesNotReplaceAnArrival}, Case{"explicit-pin", ExplicitRootPin},
        Case{"inventory-objects", InventoryObjects}, Case{"journal-create", JournalCreate},
        Case{"journal-races", JournalPublicationRaces}, Case{"journal-append", JournalAppend},
        Case{"canonical-inactive", CanonicalInactive}};
    if (argc == 3 && std::string_view(argv[1]) == "--case") {
        for (const auto& [name, run] : cases) if (name == argv[2]) { run(); return 0; }
        if (std::string_view(argv[2]) == "crash") { CrashBoundaries(std::filesystem::absolute(argv[0])); return 0; }
        return 2;
    }
    if (argc != 1) return 2;
    for (const auto& [name, run] : cases) { (void)name; run(); }
}
