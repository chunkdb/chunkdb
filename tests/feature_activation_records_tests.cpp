#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feature_activation_records.hpp"
#include "feature_flags.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "txn_test_utils.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;

void Save(const std::filesystem::path& path, const Bytes& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(out.good());
}
template <typename Fn> void Reject(Fn&& fn, std::string_view reason) {
    try { fn(); }
    catch (const std::exception& error) {
        assert(std::string_view(error.what()).find(reason) != std::string_view::npos); return;
    }
    assert(false && "activation should have been refused");
}
Bytes Checksum(Bytes bytes) {
    const auto crc = Crc32(bytes.data(), bytes.size() - 4U);
    for (unsigned i = 0; i < 4U; ++i) bytes[bytes.size() - 4U + i] = static_cast<std::uint8_t>(crc >> (8U * i));
    return bytes;
}
std::map<std::string, Bytes> Tree(const std::filesystem::path& root) {
    std::map<std::string, Bytes> tree;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        tree.emplace(entry.path().lexically_relative(root).generic_string(),
            entry.is_regular_file() ? LoadFile(entry.path()) : Bytes{});
    }
    return tree;
}

FeatureActivationJournal Seed(const std::filesystem::path& root) {
    CatalogConfig config; config.data_dir = root;
    const GeometryConfig geometry{1U, 1U, 2U, 2U, 8U};
    {
        TableCatalog catalog(config);
        (void)catalog.Create("alpha", geometry, config.default_options);
        (void)catalog.Create("beta", geometry, config.default_options);
    }
    auto directory = *ReadDataDirManifest(root);
    directory.features = {.incompat = kFeatureActivation, .compat = 1U << 30U};
    Save(DataDirManifestPath(root), SerializeDataDirManifest(directory));
    FeatureActivationJournal journal{directory.data_dir_id, kFeatureSlotFilters, {}};
    for (const char* name : {"alpha", "beta"}) {
        auto manifest = *ReadStoreManifest(root / "tables" / name);
        manifest.features = {.incompat = kFeatureFeedSlots, .compat = 1U << 29U};
        manifest.options.insert(manifest.options.end(), {99, 0, 1, 0, 0xAC});
        Save(StoreManifestPath(root / "tables" / name), SerializeStoreManifest(manifest));
        journal.tables.push_back({name, manifest.store_id});
    }
    return journal;
}

void Codec() {
    test::ScopedTempDir dir("chunkdb-feature-codec");
    const auto journal = Seed(dir.path());
    const auto good = EncodeFeatureActivationJournal(journal);
    assert(DecodeFeatureActivationJournal(good) == journal);
    auto bad = good; bad[0] ^= 1U; Reject([&] { (void)DecodeFeatureActivationJournal(Checksum(bad)); }, "magic");
    bad = good; bad[4] = 2; Reject([&] { (void)DecodeFeatureActivationJournal(Checksum(bad)); }, "version");
    bad = good; bad[6] = 1; Reject([&] { (void)DecodeFeatureActivationJournal(Checksum(bad)); }, "reserved");
    bad = good; bad[10] ^= 1U; Reject([&] { (void)DecodeFeatureActivationJournal(bad); }, "checksum");
    bad = good; bad.insert(bad.end() - 4, 0); Reject([&] { (void)DecodeFeatureActivationJournal(Checksum(bad)); }, "trailing");
    bad = good; bad.erase(bad.end() - 5); Reject([&] { (void)DecodeFeatureActivationJournal(Checksum(bad)); }, "truncated");
    bad = good; std::fill(bad.begin() + 28, bad.begin() + 32, 0xFF); Reject([&] { (void)DecodeFeatureActivationJournal(Checksum(bad)); }, "count");
    auto malformed = journal; malformed.data_dir_id = {}; Reject([&] { (void)EncodeFeatureActivationJournal(malformed); }, "zero data");
    for (const auto bit : {0U, kFeatureActivation, 3U, 1U << 31U}) {
        malformed = journal; malformed.feature = bit;
        Reject([&] { (void)EncodeFeatureActivationJournal(malformed); }, "feature");
    }
    malformed = journal; malformed.tables[1] = malformed.tables[0];
    Reject([&] { (void)EncodeFeatureActivationJournal(malformed); }, "duplicated");
    malformed = journal; std::swap(malformed.tables[0], malformed.tables[1]);
    Reject([&] { (void)EncodeFeatureActivationJournal(malformed); }, "unordered");
    malformed = journal; malformed.tables[0].name = "../escape";
    Reject([&] { (void)EncodeFeatureActivationJournal(malformed); }, "name");
    malformed = journal; malformed.tables[0].table_id = {};
    Reject([&] { (void)EncodeFeatureActivationJournal(malformed); }, "zero table");
    malformed = journal; malformed.tables.resize(kMaxFeatureJournalTables + 1U);
    Reject([&] { (void)EncodeFeatureActivationJournal(malformed); }, "too many");
    auto empty = journal; empty.tables.clear();
    assert(DecodeFeatureActivationJournal(EncodeFeatureActivationJournal(empty)) == empty);
}

void ValidationBeforePublication() {
    for (const char* damage : {"root_id", "table_id", "inventory", "owner", "unknown", "table_owner", "missing", "schema", "conflict", "inventory_type"}) {
        test::ScopedTempDir dir("chunkdb-feature-validate");
        auto journal = Seed(dir.path());
        const auto table_path = dir.path() / "tables" / "beta";
        if (std::string_view(damage) == "root_id") journal.data_dir_id = NewStoreId();
        else if (std::string_view(damage) == "table_id") journal.tables.back().table_id = NewStoreId();
        else if (std::string_view(damage) == "inventory") journal.tables.pop_back();
        else if (std::string_view(damage) == "owner") {
            auto root = *ReadDataDirManifest(dir.path()); root.features.incompat = 0U;
            Save(DataDirManifestPath(dir.path()), SerializeDataDirManifest(root));
        } else if (std::string_view(damage) == "missing") std::filesystem::remove(StoreManifestPath(table_path));
        else if (std::string_view(damage) == "conflict") Save(dir.path() / "chunkdb.migration.pending", {1});
        else if (std::string_view(damage) == "inventory_type") {
            std::filesystem::rename(dir.path() / "tables", dir.path() / "saved_tables");
            Save(dir.path() / "tables", {1});
        }
        else {
            auto table = *ReadStoreManifest(table_path);
            if (std::string_view(damage) == "unknown") table.features.incompat |= 1U << 31U;
            else if (std::string_view(damage) == "table_owner") table.features.incompat |= kFeatureActivation;
            auto bytes = SerializeStoreManifest(table);
            if (std::string_view(damage) == "schema") bytes[bytes.size() - 5U] ^= 1U;
            Save(StoreManifestPath(table_path), bytes);
        }
        const auto before = Tree(dir.path());
        Reject([&] { WriteFeatureActivationJournal(dir.path(), journal); }, "");
        assert(Tree(dir.path()) == before);
        Save(dir.path() / kFeaturePendingFileName, EncodeFeatureActivationJournal(journal));
        const auto decided = Tree(dir.path());
        Reject([&] { CompleteFeatureActivationJournal(dir.path(), journal); }, "");
        assert(Tree(dir.path()) == decided);
    }
}

void RecoveryPreservesCurrentMetadata() {
    test::ScopedTempDir dir("chunkdb-feature-recover");
    const auto journal = Seed(dir.path());
    WriteFeatureActivationJournal(dir.path(), journal);
    Reject([&] { WriteFeatureActivationJournal(dir.path(), journal); }, "already pending");
    const auto before_read_only = Tree(dir.path());
    Reject([&] { RecoverFeatureActivation(dir.path(), AccessMode::kReadOnly); }, "read-write");
    assert(Tree(dir.path()) == before_read_only);
    auto table = *ReadStoreManifest(dir.path() / "tables" / "beta");
    auto options = DecodeTableOptions(table.options); options.checkpoint_update_interval = 17U;
    ReplaceTableOptions(&table, options);
    table.schema = RenameColumn(table.schema, table.schema.columns.front().name, "renamed");
    Save(StoreManifestPath(dir.path() / "tables" / "beta"), SerializeStoreManifest(table));
    RecoverFeatureActivation(dir.path(), AccessMode::kReadWrite);
    assert(!ReadFeatureActivationJournal(dir.path()));
    const auto root = *ReadDataDirManifest(dir.path());
    assert(root.features.incompat == (kFeatureActivation | kFeatureSlotFilters));
    assert(root.features.compat == (1U << 30U));
    const auto restored = *ReadStoreManifest(dir.path() / "tables" / "beta");
    assert(restored.features.incompat == (kFeatureFeedSlots | kFeatureSlotFilters));
    assert(restored.features.compat == table.features.compat);
    assert(restored.options == table.options && restored.schema == table.schema);
    const auto complete = Tree(dir.path());
    RecoverFeatureActivation(dir.path(), AccessMode::kReadWrite);
    assert(Tree(dir.path()) == complete);
    // Replaying an already-applied but retained decision is also idempotent.
    WriteFeatureActivationJournal(dir.path(), journal);
    RecoverFeatureActivation(dir.path(), AccessMode::kReadWrite);
    assert(Tree(dir.path()) == complete);
}

void DirectoryOnlyActivation() {
    test::ScopedTempDir dir("chunkdb-feature-root-only");
    auto journal = Seed(dir.path()); journal.feature = kFeatureMigrations;
    auto before = Tree(dir.path()); before.erase(std::string(kDataDirManifestFileName));
    WriteFeatureActivationJournal(dir.path(), journal);
    CompleteFeatureActivationJournal(dir.path(), journal);
    auto after = Tree(dir.path()); after.erase(std::string(kDataDirManifestFileName));
    assert(after == before);
    const auto directory = *ReadDataDirManifest(dir.path());
    assert(directory.features.incompat == (kFeatureActivation | kFeatureMigrations));
    assert(directory.features.compat == (1U << 30U));
    // Root-only activation still refuses a missing target before any publication.
    journal.tables.pop_back();
    const auto complete = Tree(dir.path());
    Reject([&] { WriteFeatureActivationJournal(dir.path(), journal); }, "inventory");
    assert(Tree(dir.path()) == complete);
}

int RunChild(const std::string& executable, const std::filesystem::path& root, const char* point, bool recover) {
    std::string command = "\"" + executable + "\" \"" + (recover ? "--recover" : "--activate") +
        "\" \"" + root.string() + "\" \"" + point + "\"";
#ifdef _WIN32
    return std::system(("\"" + command + "\"").c_str());
#else
    const auto result = std::system(command.c_str());
    return WIFEXITED(result) ? WEXITSTATUS(result) : -1;
#endif
}

void CrashRecovery(const std::string& executable) {
    const char* points[]{
        "CHUNKDB_FAILPOINT_CRASH_FEATURE_BEFORE_DECISION_ONCE", "CHUNKDB_FAILPOINT_CRASH_FEATURE_AFTER_DECISION_ONCE",
        "CHUNKDB_FAILPOINT_CRASH_FEATURE_ROOT_BEFORE_PUBLISH_ONCE", "CHUNKDB_FAILPOINT_CRASH_FEATURE_ROOT_AFTER_PUBLISH_ONCE",
        "CHUNKDB_FAILPOINT_FEATURE_TABLE_0_BEFORE_PUBLISH_ONCE", "CHUNKDB_FAILPOINT_FEATURE_TABLE_0_AFTER_PUBLISH_ONCE",
        "CHUNKDB_FAILPOINT_FEATURE_TABLE_1_BEFORE_PUBLISH_ONCE", "CHUNKDB_FAILPOINT_FEATURE_TABLE_1_AFTER_PUBLISH_ONCE",
        "CHUNKDB_FAILPOINT_CRASH_FEATURE_BEFORE_PENDING_REMOVE_ONCE", "CHUNKDB_FAILPOINT_CRASH_FEATURE_AFTER_PENDING_REMOVE_ONCE",
        "CHUNKDB_FAILPOINT_FEATURE_DECISION_SYNC_FAIL_ONCE", "CHUNKDB_FAILPOINT_FEATURE_ROOT_SYNC_FAIL_ONCE",
        "CHUNKDB_FAILPOINT_FEATURE_TABLE_0_SYNC_FAIL_ONCE", "CHUNKDB_FAILPOINT_FEATURE_TABLE_1_SYNC_FAIL_ONCE",
        "CHUNKDB_FAILPOINT_FEATURE_PENDING_REMOVE_SYNC_FAIL_ONCE"};
    for (const char* point : points) {
        test::ScopedTempDir dir("chunkdb-feature-crash");
        const auto journal = Seed(dir.path());
        assert(RunChild(executable, dir.path(), point, false) == 86);
        const bool applied = std::string_view(point) != points[0];
        RecoverFeatureActivation(dir.path(), AccessMode::kReadWrite);
        assert(!ReadFeatureActivationJournal(dir.path()));
        assert(((ReadDataDirManifest(dir.path())->features.incompat & kFeatureSlotFilters) != 0U) == applied);
        for (const auto& target : journal.tables)
            assert(((ReadStoreManifest(dir.path() / "tables" / target.name)->features.incompat & kFeatureSlotFilters) != 0U) == applied);
    }
    test::ScopedTempDir dir("chunkdb-feature-repeated-crash");
    const auto journal = Seed(dir.path()); WriteFeatureActivationJournal(dir.path(), journal);
    assert(RunChild(executable, dir.path(), points[5], true) == 86);
    assert(RunChild(executable, dir.path(), points[7], true) == 86);
    RecoverFeatureActivation(dir.path(), AccessMode::kReadWrite);
    assert(!ReadFeatureActivationJournal(dir.path()));
    assert(ReadDataDirManifest(dir.path())->features.incompat == (kFeatureActivation | kFeatureSlotFilters));
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 4) {
        txn_test::ScopedEnv armed(argv[3], "1");
        try {
            if (std::string_view(argv[1]) == "--recover") RecoverFeatureActivation(argv[2], AccessMode::kReadWrite);
            else {
                const auto root = *ReadDataDirManifest(argv[2]);
                FeatureActivationJournal journal{root.data_dir_id, kFeatureSlotFilters, {}};
                for (const char* name : {"alpha", "beta"})
                    journal.tables.push_back({name, ReadStoreManifest(std::filesystem::path(argv[2]) / "tables" / name)->store_id});
                WriteFeatureActivationJournal(argv[2], journal);
                CompleteFeatureActivationJournal(argv[2], journal);
            }
        } catch (const std::exception&) {
            if (std::string_view(argv[3]).find("SYNC_FAIL") == std::string_view::npos) throw;
            const bool removed = std::string_view(argv[3]).find("PENDING_REMOVE") != std::string_view::npos;
            assert(ReadFeatureActivationJournal(argv[2]).has_value() != removed);
            std::_Exit(86);
        }
        return 3;
    }
    assert(argc == 1);
    Codec(); ValidationBeforePublication(); RecoveryPreservesCurrentMetadata(); DirectoryOnlyActivation(); CrashRecovery(argv[0]);
}
