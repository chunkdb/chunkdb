#include <algorithm>
#include <array>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "checkpoint.hpp"
#include "chunkdb/table_catalog.hpp"
#include "chunk_coordinate_index.hpp"
#include "chunkdb/file_layout.hpp"
#include "commit_group_coordinator.hpp"
#include "feature_flags.hpp"
#include "process_lock.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "verify.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
using Path = std::filesystem::path;
const CommitOwnerScope kScope{{1U}, {2U}, CommitOwnerRole::kCatalog};
constexpr const char* kRoot = "chunkdb.groups.owner";
constexpr const char* kObjects = "chunkdb.groups.objects";
constexpr const char* kJournals = "chunkdb.groups.journal";

void Save(const Path& path, std::span<const std::uint8_t> bytes) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close(); assert(output.good());
}
void Append(const Path& path, std::span<const std::uint8_t> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::app);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close(); assert(output.good());
}
std::map<Path, Bytes> Snapshot(const Path& root) {
    std::map<Path, Bytes> files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        const auto relative = entry.path().lexically_relative(root);
        if (entry.is_symlink()) files.emplace(relative, Bytes{'@'});
        else if (entry.is_regular_file()) files.emplace(relative, LoadFile(entry.path()));
        else files.emplace(relative, Bytes{});
    }
    return files;
}
// A path token as verify prints it: quoted, with backslashes and quotes escaped.
std::string Quoted(const Path& path) {
    std::string token = "\"";
    for (const char ch : path.string()) {
        if (ch == '\\' || ch == '"') token.push_back('\\');
        token.push_back(ch);
    }
    return token + "\"";
}
std::size_t Count(const std::string& text, std::string_view needle) {
    std::size_t count = 0U;
    for (auto at = text.find(needle); at != std::string::npos; at = text.find(needle, at + 1U)) ++count;
    return count;
}
std::string Hex(std::span<const std::uint8_t> bytes) {
    constexpr char digits[] = "0123456789abcdef"; std::string value;
    for (const auto byte : bytes) { value += digits[byte >> 4U]; value += digits[byte & 15U]; }
    return value;
}
Bytes Decision(const CommitGroupId& id, std::uint64_t created = 1U) {
    CommitGroupDecision decision; decision.directory_id = kScope.directory_id; decision.id = id; decision.created_ms = created;
    return SerializeCommitGroupDecision(decision);
}
Bytes Receipt(const CommitGroupId& id, std::span<const std::uint8_t> decision) {
    const auto bytes = SerializeCommitGroupReceipt({kScope.directory_id, id, crypto::Sha256Of(decision)});
    return {bytes.begin(), bytes.end()};
}

// Runs the owner verifier and requires the tree to be byte-identical afterwards.
struct Result {
    VerifyCounters counters;
    std::string text;
};
Result VerifyOwner(const Path& root, const CommitOwnerScope& scope = kScope) {
    const auto before = Snapshot(root);
    std::ostringstream output;
    Result result{VerifyCommitGroupOwner(root, scope, output), {}};
    result.text = output.str();
    assert(Snapshot(root) == before);
    return result;
}
void Expect(const Result& result, std::uint64_t errors, std::uint64_t warnings, std::string_view code = {}) {
    if (result.counters.errors != errors || result.counters.warnings != warnings ||
        (!code.empty() && result.text.find(code) == std::string::npos)) std::cerr << result.text;
    assert(result.counters.errors == errors && result.counters.warnings == warnings);
    assert(code.empty() || result.text.find(code) != std::string::npos);
}

// An owner root with a canonical INACTIVE root and one staged activation; the
// lock and every handle are released before a case changes the tree.
struct Owner {
    test::ScopedTempDir temporary{"chunkdb-group-verify"};
    Path root = std::filesystem::canonical(temporary.path());
    CommitGroupActivation candidate;
    explicit Owner(bool activate = true) {
        ProcessLock lock(root, ProcessLock::PinRootForArtifacts{});
        CommitGroupCoordinator writer(root, kScope, lock, false);
        (void)writer.InstallInactiveOwner();
        if (activate) candidate = writer.StageActivation();
    }
    Path Leaf() const { return root / kJournals / CommitJournalLeafName(candidate.value.reference_context); }
    // Test-only: select a root canonically, which no path of this build does.
    void Select(const CommitOwnerRoot& value) const { Save(root / kRoot, SerializeCommitOwnerRoot(value)); }
    template <typename F> void Write(F action) const {
        ProcessLock lock(root, ProcessLock::PinRootForArtifacts{});
        CommitOwnerArtifactIo io(root, kScope, lock, false);
        action(io);
    }
};

void Missing() {
    test::ScopedTempDir temporary("chunkdb-group-verify-missing");
    const auto root = std::filesystem::canonical(temporary.path());
    const auto result = VerifyOwner(root);
    Expect(result, 1U, 0U, "VERIFY error group_owner_missing");
    assert(result.text.find(kRoot) != std::string::npos);
}
void Inactive() {
    const Owner owner;
    const auto result = VerifyOwner(owner.root);
    Expect(result, 0U, 0U);
    assert(result.text.empty());
}
void OwnerInvalid() {
    const Owner owner(false);
    const auto original = LoadFile(owner.root / kRoot);
    CommitOwnerRoot other_scope; other_scope.scope = kScope; other_scope.scope.owner_id[0U] = 9U;
    CommitOwnerRoot second; second.scope = kScope; second.generation = 2U;
    auto flipped = original; flipped[508U] ^= 0x01U;
    const auto encode = [](const CommitOwnerRoot& value) { const auto bytes = SerializeCommitOwnerRoot(value); return Bytes(bytes.begin(), bytes.end()); };
    const std::array<std::pair<Bytes, std::string_view>, 3U> shapes{std::pair<Bytes, std::string_view>{encode(other_scope), "group_owner_invalid"},
        {encode(second), "INACTIVE owner root generation is not one"}, {flipped, "group_owner_invalid"}};
    for (const auto& [bytes, reason] : shapes) {
        Save(owner.root / kRoot, bytes);
        Expect(VerifyOwner(owner.root), 1U, 0U, reason);
    }
    // The verifier's own scope must also match.
    Save(owner.root / kRoot, original);
    auto foreign = kScope; foreign.owner_id[0U] = 9U;
    Expect(VerifyOwner(owner.root, foreign), 1U, 0U, "group_owner_invalid");
    std::filesystem::remove(owner.root / kRoot);
    std::filesystem::create_directory(owner.root / kRoot);
    Expect(VerifyOwner(owner.root), 1U, 0U, "group_owner_invalid");
    std::filesystem::remove(owner.root / kRoot);
#ifndef _WIN32
    Save(owner.root / "held-root", original);
    std::filesystem::create_symlink(owner.root / "held-root", owner.root / kRoot);
    Expect(VerifyOwner(owner.root), 1U, 0U, "group_owner_invalid");
    std::filesystem::remove(owner.root / kRoot);
#endif
}
void ActiveEmpty() {
    const Owner owner;
    owner.Select(owner.candidate.value);
    const auto result = VerifyOwner(owner.root);
    Expect(result, 0U, 0U, "VERIFY info group_journal");
    assert(result.text.find("generation 1 certified 64 bytes uncertified 0 records; members not checked") != std::string::npos);
    assert(result.text.find(CommitJournalLeafName(owner.candidate.value.reference_context)) != std::string::npos);
}
void ActiveRecords() {
    const Owner owner;
    owner.Select(owner.candidate.value);
    const CommitGroupId a{0xA1U};
    const auto decision = Decision(a);
    std::uint64_t end = 0U;
    owner.Write([&](CommitOwnerArtifactIo& io) {
        auto journal = io.OpenJournal(owner.candidate.value.reference_context);
        (void)journal.Scan(io.ReadJournalInventory(owner.candidate.value.journal_inventory).value);
        (void)journal.Append(decision);
        end = journal.Append(Receipt(a, decision));
        journal.Sync();
    });
    const auto clean = LoadFile(owner.Leaf());
    Expect(VerifyOwner(owner.root), 0U, 0U, "uncertified 2 records");
    // A torn final append is a tail, reported with its exact length.
    const auto torn = Decision(CommitGroupId{0xB2U});
    Append(owner.Leaf(), std::span(torn).first(70U));
    const auto tail = VerifyOwner(owner.root);
    Expect(tail, 0U, 1U, "VERIFY warning group_journal_tail");
    assert(tail.text.find("70 bytes after the last complete record") != std::string::npos);
    assert(tail.text.find("uncertified 2 records") != std::string::npos);
    // A second decision for one group is damage.
    Save(owner.Leaf(), clean);
    Append(owner.Leaf(), Decision(a, 2U));
    Expect(VerifyOwner(owner.root), 1U, 0U, "conflicting decisions");
    // A flipped certified header byte is damage at the journal leaf.
    auto flipped = clean; flipped[20U] ^= 0x01U;
    Save(owner.Leaf(), flipped);
    const auto damaged = VerifyOwner(owner.root);
    Expect(damaged, 1U, 0U, "VERIFY error group_journal_invalid");
    assert(damaged.text.find(CommitJournalLeafName(owner.candidate.value.reference_context)) != std::string::npos);
    // Certify both records, then shorten the certified prefix.
    Save(owner.Leaf(), clean);
    auto next = owner.candidate.value; next.generation = 3U;
    const CommitJournalCut cut{end, a, CommitRecordKind::kReceipt, crypto::Sha256Of(clean)};
    owner.Write([&](CommitOwnerArtifactIo& io) {
        next.journal_inventory = io.StageJournalInventory({next.reference_context, 2U, cut}).reference;
    });
    owner.Select(next);
    Expect(VerifyOwner(owner.root), 0U, 0U, "certified " + std::to_string(end) + " bytes uncertified 0 records");
    std::filesystem::resize_file(owner.Leaf(), end - 1U);
    Expect(VerifyOwner(owner.root), 1U, 0U, "shorter than its certified cut");
    // A missing journal is reported at its leaf.
    std::filesystem::remove(owner.Leaf());
    const auto missing = VerifyOwner(owner.root);
    Expect(missing, 1U, 0U, "group_journal_invalid");
    assert(missing.text.find(CommitJournalLeafName(owner.candidate.value.reference_context)) != std::string::npos);
    Save(owner.Leaf(), clean);
    Expect(VerifyOwner(owner.root), 0U, 0U);
    // A missing inventory object is reported at the object directory.
    std::filesystem::remove(owner.root / kObjects / Hex(next.journal_inventory.sha256));
    const auto object = VerifyOwner(owner.root);
    Expect(object, 1U, 0U, "group_journal_invalid");
    assert(object.text.find(Quoted(owner.root / kObjects)) != std::string::npos);
}
void RetainedIds() {
    const Owner owner;
    auto value = owner.candidate.value;
    CommitIdEntry entry;
    entry.user = "u"; entry.id = {'a'}; entry.group_id = CommitGroupId{0xA1U};
    entry.decision_sha256 = crypto::Sha256Of(Decision(entry.group_id)); entry.fingerprint = crypto::Sha256Of(Bytes{'f'});
    entry.created_ms = 1U;
    owner.Write([&](CommitOwnerArtifactIo& io) {
        value.id_inventory = io.StageIdInventory({value.reference_context, 1U, value.reference_replay, {entry}}).reference;
    });
    owner.Select(value);
    Expect(VerifyOwner(owner.root), 1U, 0U, "retained ID entries are not admitted by this build");
}
void Pending() {
    const Owner owner;
    auto pending = owner.candidate.value;
    pending.generation = 3U; pending.state = CommitOwnerState::kPending;
    pending.replacement_plan = {CommitOwnerObjectKind::kReplacementPlan, 68U, {7U}};
    pending.previous_root = {CommitOwnerObjectKind::kRoot, 512U, {8U}};
    owner.Select(pending);
    Expect(VerifyOwner(owner.root), 0U, 1U, "VERIFY warning group_owner_pending");
}

// A data directory and one table that verify clean before owner files appear.
struct DataDirectory {
    test::ScopedTempDir directory{"chunkdb-group-verify-unclaimed"};
    Path root = directory.path();
    Path table = root / "tables" / "t";
    explicit DataDirectory(std::uint32_t extra = 0U) {
        DataDirManifest manifest{.data_dir_id = StoreId{9U}, .options = {}};
        manifest.features.incompat |= extra;
        Save(DataDirManifestPath(root), SerializeDataDirManifest(manifest));
        GeometryConfig shape{2U, 2U, 2U, 2U, 8U};
        const StoreManifest store{.geometry = shape, .store_id = StoreId{1U},
            .options = EncodeTableOptions(TableOptions{}), .schema = SingleBitsColumnSchema(8U), .chunk_schema = TableSchema{}};
        Save(StoreManifestPath(table), SerializeStoreManifest(store));
        Save(table / "chunkdb.version", SerializeVersionClockRecord(1000U));
        std::ostringstream index(std::ios::binary);
        WriteIndexInventory(index, {.table_id = store.store_id, .dims = 2U, .compaction_pending = false,
            .retained_deletion_floor = 0U, .compaction_through = 0U, .count = 0U}, {});
        const auto bytes = index.str();
        Save(table / kChunkIndexFileName, Bytes(bytes.begin(), bytes.end()));
    }
    Result Check() const {
        const auto before = Snapshot(root);
        std::ostringstream output;
        Result result{VerifyDataDirectory(root, output), {}};
        result.text = output.str();
        assert(Snapshot(root) == before);
        return result;
    }
    void Populate(const Path& at, bool with_root = true) const {
        if (with_root) Save(at / kRoot, Bytes(512U, 0U));
        std::filesystem::create_directories(at / kObjects);
        std::filesystem::create_directories(at / kJournals);
        Save(at / kJournals / "x", Bytes{'j'});
    }
};
void Unclaimed() {
    const DataDirectory data;
    Expect(data.Check(), 0U, 0U);
    data.Populate(data.root, false);
    data.Populate(data.table);
    const auto result = data.Check();
    Expect(result, 0U, 5U, "VERIFY warning group_owner_unclaimed");
    assert(Count(result.text, "group_owner_unclaimed") == 5U);
    assert(result.text.find("unexpected_entry") == std::string::npos && result.text.find("foreign_entry") == std::string::npos);
    for (const auto* name : {kObjects, kJournals}) assert(result.text.find(Quoted(data.root / name)) != std::string::npos);
    for (const auto* name : {kRoot, kObjects, kJournals}) assert(result.text.find(Quoted(data.table / name)) != std::string::npos);
    assert(Count(result.text, "commit group owner state is not admitted by this build") == 5U);
    // A canonical root in a directory whose manifest does not claim commit_groups is damage.
    Save(data.root / kRoot, Bytes(512U, 0U));
    const auto damaged = data.Check();
    if (damaged.counters.errors != 1U) std::cerr << damaged.text;
    assert(damaged.counters.errors == 1U && damaged.text.find("unclaimed canonical commit owner exists") != std::string::npos);
    std::filesystem::remove(data.root / kRoot);
    // A table checked directly reports the same names.
    std::ostringstream table;
    const auto counters = VerifyTableDirectory(data.table, table);
    assert(counters.errors == 0U && counters.warnings == 3U && Count(table.str(), "group_owner_unclaimed") == 3U);
}
// A directory created by this build claims commit_groups and its INACTIVE root;
// object and journal directories stay unclaimed, and a lost root is damage.
void ClaimedOwner() {
    test::ScopedTempDir directory("chunkdb-group-verify-claimed");
    const Path root = directory.path();
    {
        CatalogConfig config; config.data_dir = root;
        TableCatalog catalog(config);
        (void)catalog.Create("t", {2U, 2U, 2U, 2U, 8U}, config.default_options);
    }
    const auto manifest = *ReadDataDirManifest(root);
    assert((manifest.features.incompat & kFeatureCommitGroups) != 0U && manifest.commit_owner);
    assert(manifest.commit_owner->role == CommitOwnerRole::kCatalog);
    assert(ReadStoreManifest(root / "tables" / "t")->commit_owner == manifest.commit_owner);
    const auto check = [&] {
        const auto before = Snapshot(root);
        std::ostringstream output;
        Result result{VerifyDataDirectory(root, output), {}};
        result.text = output.str();
        assert(Snapshot(root) == before);
        return result;
    };
    Expect(check(), 0U, 0U);
    std::filesystem::create_directories(root / kObjects);
    std::filesystem::create_directories(root / "tables" / "t" / kJournals);
    Expect(check(), 0U, 2U, "VERIFY warning group_owner_unclaimed");
    std::filesystem::remove_all(root / kObjects);
    std::filesystem::remove_all(root / "tables" / "t" / kJournals);
    const auto owner = LoadFile(root / kRoot);
    std::filesystem::remove(root / kRoot);
    Expect(check(), 1U, 0U, "data_dir_manifest_invalid");
    Save(root / kRoot, owner);
    Expect(check(), 0U, 0U);
}
}  // namespace

int main(int argc, char** argv) {
    using Case = std::pair<std::string_view, void (*)()>;
    const std::array cases{Case{"missing", Missing}, Case{"inactive", Inactive}, Case{"owner-invalid", OwnerInvalid},
        Case{"active-empty", ActiveEmpty}, Case{"active-records", ActiveRecords}, Case{"retained-ids", RetainedIds},
        Case{"pending", Pending}, Case{"unclaimed", Unclaimed}, Case{"claimed-owner", ClaimedOwner}};
    if (argc == 3 && std::string_view(argv[1]) == "--case") {
        for (const auto& [name, run] : cases) if (name == argv[2]) { run(); return 0; }
        return 2;
    }
    if (argc != 1) return 2;
    for (const auto& [name, run] : cases) { (void)name; run(); }
}
