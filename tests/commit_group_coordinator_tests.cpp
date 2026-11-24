#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "checkpoint.hpp"
#include "commit_group_coordinator.hpp"
#include "process_lock.hpp"
#include "test_utils.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
using Path = std::filesystem::path;
const CommitOwnerScope kScope{{1U}, {2U}, CommitOwnerRole::kCatalog};
constexpr const char* kRoot = "chunkdb.groups.owner";
constexpr const char* kObjects = "chunkdb.groups.objects";
constexpr const char* kJournals = "chunkdb.groups.journal";

void Save(const Path& path, std::span<const std::uint8_t> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close(); assert(output.good());
}
void Append(const Path& path, std::span<const std::uint8_t> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::app);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close(); assert(output.good());
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
Bytes InactiveBytes() {
    CommitOwnerRoot root; root.scope = kScope;
    const auto bytes = SerializeCommitOwnerRoot(root); return {bytes.begin(), bytes.end()};
}
Bytes Decision(const CommitGroupId& id) {
    CommitGroupDecision decision; decision.directory_id = kScope.directory_id; decision.id = id; decision.created_ms = 1U;
    return SerializeCommitGroupDecision(decision);
}
Bytes Receipt(const CommitGroupId& id, std::span<const std::uint8_t> decision) {
    const auto bytes = SerializeCommitGroupReceipt({kScope.directory_id, id, crypto::Sha256Of(decision)});
    return {bytes.begin(), bytes.end()};
}
Path Leaf(const Path& root, const CommitJournalContext& context) { return root / kJournals / CommitJournalLeafName(context); }

struct Fixture {
    test::ScopedTempDir temporary{"chunkdb-group-coordinator"};
    Path root = std::filesystem::canonical(temporary.path());
    ProcessLock lock{root, ProcessLock::PinRootForArtifacts{}};
    Fixture() { Save(root / "sentinel", Bytes{'s'}); }
    void Sentinel() const { assert(LoadFile(root / "sentinel") == Bytes{'s'}); }
};

void Absent() {
    Fixture f;
    CommitGroupCoordinator writer(f.root, kScope, f.lock, false);
    assert(!writer.ReadOwner());
    RejectWith("canonical owner is missing", [&] { (void)writer.StageActivation(); });
    assert(!std::filesystem::exists(f.root / kRoot) && !std::filesystem::exists(f.root / kJournals));
    assert(!std::filesystem::exists(f.root / kObjects));
    f.Sentinel();
}
void Bootstrap() {
    Fixture f;
    {
        CommitGroupCoordinator reader(f.root, kScope);
        assert(!reader.writable());
        RejectWith("read-only coordinator cannot install", [&] { (void)reader.InstallInactiveOwner(); });
        RejectWith("read-only coordinator cannot stage", [&] { (void)reader.StageActivation(); });
    }
    assert(!std::filesystem::exists(f.root / kRoot));
    CommitGroupCoordinator writer(f.root, kScope, f.lock, false);
    assert(writer.InstallInactiveOwner().installed);
    const auto owner = writer.ReadOwner();
    assert(owner && owner->value.state == CommitOwnerState::kInactive && owner->value.generation == 1U);
    assert(LoadFile(f.root / kRoot) == InactiveBytes());
    assert(!writer.InstallInactiveOwner().installed && LoadFile(f.root / kRoot) == InactiveBytes());
    RejectWith("incompatible exclusive writer scope", [&] { CommitGroupCoordinator shared(f.root, kScope, f.lock, true); });
    f.Sentinel();
}
void InactiveGeneration() {
    Fixture f;
    CommitOwnerRoot second; second.scope = kScope; second.generation = 2U;
    const auto bytes = SerializeCommitOwnerRoot(second);
    Save(f.root / kRoot, bytes);
    CommitGroupCoordinator writer(f.root, kScope, f.lock, false);
    RejectWith("INACTIVE owner root generation is not one", [&] { (void)writer.ReadOwner(); });
    RejectWith("INACTIVE owner root generation is not one", [&] { (void)writer.StageActivation(); });
    RejectWith("not the generation-one INACTIVE root", [&] { (void)writer.InstallInactiveOwner(); });
    assert(LoadFile(f.root / kRoot) == Bytes(bytes.begin(), bytes.end()));
    assert(!std::filesystem::exists(f.root / kJournals));
}
void Activation() {
    Fixture f;
    CommitGroupActivation candidate;
    {
        CommitGroupCoordinator writer(f.root, kScope, f.lock, false);
        (void)writer.InstallInactiveOwner();
        candidate = writer.StageActivation();
        const auto& value = candidate.value;
        assert(value.state == CommitOwnerState::kActive && value.generation == 2U && value.scope == kScope);
        assert(value.reference_context.directory_id == kScope.directory_id && value.reference_context.generation == 1U);
        assert(value.reference_replay.bytes == 64U && value.counter_owner == CommitGroupId{} && value.counter_ceiling == 0U);
        const auto header = SerializeCommitJournalHeader(value.reference_context);
        assert(LoadFile(Leaf(f.root, value.reference_context)) == Bytes(header.begin(), header.end()));
        // A second activation uses a fresh journal identity and leaves the first unselected.
        const auto again = writer.StageActivation();
        assert(again.value.reference_context.journal_id != value.reference_context.journal_id);
        writer.ValidateActivation(candidate);
    }
    assert(LoadFile(f.root / kRoot) == InactiveBytes());
    CommitGroupCoordinator reader(f.root, kScope);
    const auto owner = reader.ReadOwner();
    assert(owner && owner->value.state == CommitOwnerState::kInactive);
    reader.ValidateActivation(candidate);
    const auto selection = reader.AdmitSelection(candidate.value);
    assert(selection.scan.certified.cut.bytes == 64U && selection.scan.cursors.size() == 2U);
    assert(selection.journal.value.generation == 1U && selection.ids.header.generation == 1U);
    f.Sentinel();
}
void ActivationBinding() {
    Fixture f;
    const auto inactive = InactiveBytes();
    CommitGroupCoordinator writer(f.root, kScope, f.lock, false);
    (void)writer.InstallInactiveOwner();
    const auto candidate = writer.StageActivation();
    const auto& context = candidate.value.reference_context;
    const auto leaf = Leaf(f.root, context);
    const auto header = LoadFile(leaf);
    // A same-length valid header of another journal generation refuses.
    auto other = context; other.journal_id[0U] ^= 0x80U;
    const auto foreign = SerializeCommitJournalHeader(other);
    Save(leaf, foreign);
    RejectWith("journal header does not match its generation", [&] { writer.ValidateActivation(candidate); });
    Save(leaf, header);
    writer.ValidateActivation(candidate);
    // Any record above the cut means the candidate no longer names an empty journal.
    const auto decision = Decision(CommitGroupId{0xA1U});
    Append(leaf, decision);
    RejectWith("activation journal is not empty", [&] { writer.ValidateActivation(candidate); });
    Save(leaf, header);
    std::filesystem::remove(leaf);
    RejectWith("open relative owner object", [&] { writer.ValidateActivation(candidate); });
    Save(leaf, header);
    writer.ValidateActivation(candidate);
    CommitOwnerArtifactIo io(f.root, kScope, f.lock, false);
    // A generation-three object is refused when read as a generation-two successor.
    auto third = candidate.value; third.generation = 3U;
    const CommitGroupActivation late{io.StageRoot(third).reference, third};
    RejectWith("generation", [&] { writer.ValidateActivation(late); });
    // A generation-two INACTIVE object reaches the successor check and fails it.
    CommitOwnerRoot second; second.scope = kScope; second.generation = 2U;
    const CommitGroupActivation inactive_successor{io.StageRoot(second).reference, second};
    RejectWith("inactive successor", [&] { writer.ValidateActivation(inactive_successor); });
    // A candidate whose object differs from its value refuses.
    auto mismatched = candidate; mismatched.value.reference_replay.sha256[0U] ^= 1U;
    RejectWith("does not match its candidate", [&] { writer.ValidateActivation(mismatched); });
    // ID entries are not admitted by this build.
    const auto empty = candidate.value.reference_replay;
    CommitIdEntry entry;
    entry.user = "u"; entry.id = {'a'}; entry.group_id = CommitGroupId{0xA1U};
    entry.decision_sha256 = crypto::Sha256Of(decision); entry.fingerprint = crypto::Sha256Of(Bytes{'f'}); entry.created_ms = 1U;
    auto with_ids = candidate.value;
    with_ids.id_inventory = io.StageIdInventory({context, 1U, empty, {entry}}).reference;
    const CommitGroupActivation retained{io.StageRoot(with_ids).reference, with_ids};
    RejectWith("retained ID entries are not admitted by this build", [&] { writer.ValidateActivation(retained); });
    // A canonical root that already names the candidate is not an INACTIVE predecessor.
    const auto objects = f.root / kObjects;
    std::string digest;
    for (const auto byte : candidate.root.sha256) { constexpr char d[] = "0123456789abcdef"; digest += d[byte >> 4U]; digest += d[byte & 15U]; }
    Save(f.root / kRoot, LoadFile(objects / digest));
    RejectWith("canonical owner is not INACTIVE", [&] { writer.ValidateActivation(candidate); });
    RejectWith("canonical owner is not INACTIVE", [&] { (void)writer.StageActivation(); });
    Save(f.root / kRoot, inactive);
    writer.ValidateActivation(candidate);
    f.Sentinel();
}
void AdmitSelection() {
    Fixture f;
    CommitGroupCoordinator writer(f.root, kScope, f.lock, false);
    (void)writer.InstallInactiveOwner();
    const auto candidate = writer.StageActivation();
    const auto& context = candidate.value.reference_context;
    const auto a = Decision(CommitGroupId{0xA1U});
    const auto receipt = Receipt(CommitGroupId{0xA1U}, a);
    CommitOwnerArtifactIo io(f.root, kScope, f.lock, false);
    std::uint64_t end = 0U;
    {
        auto journal = io.OpenJournal(context);
        (void)journal.Scan(io.ReadJournalInventory(candidate.value.journal_inventory).value);
        (void)journal.Append(a);
        end = journal.Append(receipt);
        journal.Sync();
    }
    // Test-only: select the candidate canonically to admit a journal with records.
    Save(f.root / kRoot, SerializeCommitOwnerRoot(candidate.value));
    const auto owner = writer.ReadOwner();
    assert(owner && owner->value == candidate.value);
    const auto selection = writer.AdmitSelection(owner->value);
    assert(selection.scan.uncertified.size() == 2U);
    assert(selection.scan.uncertified[0].kind == CommitRecordKind::kDecision && selection.scan.uncertified[1].kind == CommitRecordKind::kReceipt);
    assert(selection.scan.uncertified[0].group == selection.scan.uncertified[1].group);
    assert(selection.scan.complete_bytes == end && selection.scan.measured_bytes == end);
    // A generation-two inventory certifying both records, selected by a generation-three root.
    Bytes prefix = LoadFile(Leaf(f.root, context));
    assert(prefix.size() == end);
    const CommitJournalCut cut{end, CommitGroupId{0xA1U}, CommitRecordKind::kReceipt, crypto::Sha256Of(prefix)};
    auto next = candidate.value; next.generation = 3U;
    next.journal_inventory = io.StageJournalInventory({context, 2U, cut}).reference;
    Save(f.root / kRoot, SerializeCommitOwnerRoot(next));
    const auto certified = writer.AdmitSelection(next);
    assert(certified.scan.certified.cut == cut && certified.scan.uncertified.empty());
    assert(certified.scan.cursors[0].cut.bytes == 64U && certified.scan.cursors[1].cut.bytes == 64U);
    // A shortened certified prefix is damage.
    std::filesystem::resize_file(Leaf(f.root, context), end - 1U);
    RejectWith("shorter than its certified cut", [&] { (void)writer.AdmitSelection(next); });
    // A pending replacement is not admitted.
    auto pending = next; pending.state = CommitOwnerState::kPending;
    pending.replacement_plan = {CommitOwnerObjectKind::kReplacementPlan, 68U, {7U}};
    pending.previous_root = {CommitOwnerObjectKind::kRoot, 512U, {8U}};
    RejectWith("pending owner replacement is not admitted", [&] { (void)writer.AdmitSelection(pending); });
    CommitOwnerRoot inactive; inactive.scope = kScope;
    RejectWith("owner root has no selections", [&] { (void)writer.AdmitSelection(inactive); });
    f.Sentinel();
}

// Crash points in one fresh InstallInactiveOwner then StageActivation run. Each
// child stops after the real sync with the given label and ordinal, so the
// children check outcome shapes after a process crash; sync order is checked by
// the observer counts of the artifact I/O tests.
struct CrashPoint {
    const char* name;
    bool directory;
    std::function<bool(const Path& root, const Path& path)> match;
    unsigned ordinal;
};
bool Starts(const Path& path, std::string_view prefix) { return path.filename().string().starts_with(prefix); }
const std::vector<CrashPoint>& CrashPoints() {
    static const std::vector<CrashPoint> points{
        {"I0 root temporary", false, [](const Path&, const Path& p) { return Starts(p, ".owner-root-"); }, 1U},
        {"I1 root file", false, [](const Path& r, const Path& p) { return p == r / kRoot; }, 1U},
        {"I2 root directory", true, [](const Path& r, const Path& p) { return p == r; }, 1U},
        // Ordinal 2 of each is the activation's own re-sync of the INACTIVE root.
        {"J0 root directory", true, [](const Path& r, const Path& p) { return p == r; }, 3U},
        {"J1 journal temporary", false, [](const Path&, const Path& p) { return Starts(p, ".journal-stage-"); }, 1U},
        {"J2 journal leaf", false, [](const Path& r, const Path& p) { return p.parent_path() == r / kJournals && !Starts(p, "."); }, 1U},
        {"J3 journal directory", true, [](const Path& r, const Path& p) { return p == r / kJournals; }, 1U},
        {"N0 root directory", true, [](const Path& r, const Path& p) { return p == r; }, 4U},
        {"N1 marker temporary", false, [](const Path&, const Path& p) { return Starts(p, ".owner-marker-"); }, 1U},
        {"N2 objects directory", true, [](const Path& r, const Path& p) { return p == r / kObjects; }, 1U},
        // Two pages and two inventories precede the root object.
        {"R0 root object temporary", false, [](const Path&, const Path& p) { return Starts(p, ".owner-stage-"); }, 5U},
    };
    return points;
}
// The index of the crash point whose name starts with `prefix`.
std::size_t PointIndex(std::string_view prefix) {
    const auto& points = CrashPoints();
    for (std::size_t i = 0U; i < points.size(); ++i) if (std::string_view(points[i].name).starts_with(prefix)) return i;
    std::abort();
}
// Published journal generations; a stage name kept by a hard-link fallback is not one.
std::vector<Path> JournalLeaves(const Path& root) {
    std::vector<Path> leaves;
    if (!std::filesystem::exists(root / kJournals)) return leaves;
    for (const auto& entry : std::filesystem::directory_iterator(root / kJournals))
        if (!Starts(entry.path(), ".")) leaves.push_back(entry.path());
    return leaves;
}
[[noreturn]] void Crash(std::size_t index, const Path& root) {
    ProcessLock lock(root, ProcessLock::PinRootForArtifacts{});
    CommitGroupCoordinator writer(root, kScope, lock, false);
    const auto& point = CrashPoints().at(index);
    unsigned seen = 0U;
    const auto observe = [&](bool directory, const Path& path) {
        if (directory == point.directory && point.match(root, path) && ++seen == point.ordinal) std::_Exit(86);
    };
    SetFileSyncObserverForTests([&](const auto& path) { observe(false, path); });
    SetDirectorySyncObserverForTests([&](const auto& path) { observe(true, path); });
    (void)writer.InstallInactiveOwner();
    (void)writer.StageActivation();
    std::_Exit(87);
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
int CrashChild(const Path& executable, const Path& root, std::size_t point) {
#ifdef _WIN32
    auto command = Quote(executable.wstring()) + L" --crash " + Quote(root.wstring()) + L" " + std::to_wstring(point);
    STARTUPINFOW startup{}; startup.cb = sizeof(startup); PROCESS_INFORMATION process{};
    assert(CreateProcessW(executable.c_str(), command.data(), nullptr, nullptr, FALSE, 0U, nullptr, nullptr,
        &startup, &process) != 0);
    const auto waited = WaitForSingleObject(process.hProcess, 60'000U);
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
void CrashBoundaries(const Path& executable) {
    const auto inactive = InactiveBytes();
    for (std::size_t index = 0U; index < CrashPoints().size(); ++index) {
        test::ScopedTempDir temporary("chunkdb-group-coordinator-crash");
        const auto root = std::filesystem::canonical(temporary.path());
        Save(root / "sentinel", Bytes{'s'});
        const auto status = CrashChild(executable, root, index);
        if (status != 86) std::fprintf(stderr, "crash point %s exited %d\n", CrashPoints()[index].name, status);
        assert(status == 86);
        // Before I1 the canonical name is absent; from I1 it is exactly INACTIVE.
        if (index == 0U) assert(!std::filesystem::exists(root / kRoot));
        else assert(LoadFile(root / kRoot) == inactive);
        // The journal leaf exists from its file sync (J2) on, holding only its header, and the
        // object directory exists from the root-directory sync that follows its creation (N0) on.
        const auto leaves = JournalLeaves(root);
        if (index < PointIndex("J2")) assert(leaves.empty());
        else assert(leaves.size() == 1U && LoadFile(leaves.front()).size() == kCommitJournalHeaderBytes);
        assert(std::filesystem::is_directory(root / kObjects) == (index >= PointIndex("N0")));
        ProcessLock lock(root, ProcessLock::PinRootForArtifacts{});
        {
            CommitGroupCoordinator writer(root, kScope, lock, false);
            assert(writer.InstallInactiveOwner().installed == (index == 0U));
            const auto candidate = writer.StageActivation();
            CommitGroupCoordinator(root, kScope).ValidateActivation(candidate);
        }
        assert(LoadFile(root / kRoot) == inactive);
        assert(LoadFile(root / "sentinel") == Bytes{'s'});
    }
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::string_view(argv[1]) == "--crash") Crash(std::stoul(argv[3]), argv[2]);
    using Case = std::pair<std::string_view, void (*)()>;
    const std::array cases{Case{"absent", Absent}, Case{"bootstrap", Bootstrap}, Case{"inactive-generation", InactiveGeneration},
        Case{"activation", Activation}, Case{"activation-binding", ActivationBinding}, Case{"admit-selection", AdmitSelection}};
    if (argc == 3 && std::string_view(argv[1]) == "--case") {
        for (const auto& [name, run] : cases) if (name == argv[2]) { run(); return 0; }
        if (std::string_view(argv[2]) == "crash") { CrashBoundaries(std::filesystem::absolute(argv[0])); return 0; }
        return 2;
    }
    if (argc != 1) return 2;
    for (const auto& [name, run] : cases) { (void)name; run(); }
}
