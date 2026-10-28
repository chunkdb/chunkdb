#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <map>
#include <string>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feature_activation_records.hpp"
#include "feature_flags.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "txn_test_utils.hpp"

namespace chunkdb {
struct FeatureActivationTestAccess {
    static bool MaintenanceRunning(const Table& table) {
        return table.store_ && table.store_->maintenance_thread_.joinable();
    }
    static bool MetadataExcluded(TableCatalog& catalog) {
        if (!catalog.feature_metadata_gate_.try_lock_shared()) return true;
        catalog.feature_metadata_gate_.unlock_shared(); return false;
    }
};
}

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
        const auto relative = entry.path().lexically_relative(root).generic_string();
        // The empty writer lock is held without sharing on Windows. Preserve
        // its inventory entry without reopening the live OS lock handle.
        if (relative == ".chunkdb.lock/writer.lock")
            assert(entry.is_regular_file() && entry.file_size() == 0U);
        tree.emplace(relative, entry.is_regular_file() && relative != ".chunkdb.lock/writer.lock"
            ? LoadFile(entry.path()) : Bytes{});
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

void RuntimeActivation() {
    test::ScopedTempDir dir("chunkdb-feature-runtime");
    (void)Seed(dir.path());
    CatalogConfig config; config.data_dir = dir.path();
    {
        TableCatalog catalog(config);
        assert(catalog.EnableFeature("slot_filters"));
        assert(!ReadFeatureActivationJournal(dir.path()));
        const auto status = catalog.FeatureStatus();
        assert(!status.activation_pending && status.tables.size() == 2U);
        assert((status.directory.incompat & kFeatureSlotFilters) != 0U);
        for (const auto& [name, flags] : status.tables) {
            assert((flags.incompat & kFeatureSlotFilters) != 0U);
            assert((flags.compat & (1U << 29U)) != 0U);
            auto table = catalog.Find(name);
            FeedSlotFilters filters; filters.no_before = true;
            assert(table->CreateFeedSlot("filtered", false, filters).filters == filters);
        }
        const auto before = Tree(dir.path());
        assert(!catalog.EnableFeature("slot_filters"));
        assert(Tree(dir.path()) == before);
        Reject([&] { (void)catalog.EnableFeature("missing"); }, "unknown feature");
        const auto table_flags = ReadStoreManifest(dir.path() / "tables/alpha")->features;
        assert(catalog.EnableFeature("migrations"));
        assert(DescribeFeatures(ReadStoreManifest(dir.path() / "tables/alpha")->features) == DescribeFeatures(table_flags));
        const auto new_table = catalog.Create("gamma", {1U, 1U, 2U, 2U, 8U}, config.default_options);
        const auto lease = new_table->Acquire();
        assert(lease->store().features().incompat == TableScopedFeatures(status.directory).incompat);
    }
    auto root = *ReadDataDirManifest(dir.path()); root.features.incompat &= ~kFeatureActivation;
    Save(DataDirManifestPath(dir.path()), SerializeDataDirManifest(root));
    {
        TableCatalog catalog(config);
        try { (void)catalog.EnableFeature("table_lineage"); assert(false); }
        catch (const FeatureAdmissionError& error) {
            assert(error.reason() == "feature_required" && error.feature() == "feature_activation");
        }
        assert(catalog.EnableFeature("feature_activation"));
        assert(catalog.EnableFeature("table_lineage"));
    }
    test::ScopedTempDir fresh("chunkdb-feature-baseline"); config.data_dir = fresh.path();
    {
        TableCatalog catalog(config);
        assert(DescribeFeatures(ReadDataDirManifest(fresh.path())->features) == DescribeFeatures(kKnownFeatures));
        auto table = catalog.Create("world", {1U, 1U, 2U, 2U, 8U}, config.default_options);
        assert(DescribeFeatures(table->Acquire()->store().features()) == DescribeFeatures(kTableBaselineFeatures));
    }
}

void RuntimeAdmissionAndRecovery() {
    test::ScopedTempDir dir("chunkdb-feature-admission");
    auto journal = Seed(dir.path());
    auto table = *ReadStoreManifest(dir.path() / "tables/alpha"); table.features.incompat = 0U;
    Save(StoreManifestPath(dir.path() / "tables/alpha"), SerializeStoreManifest(table));
    CatalogConfig config; config.data_dir = dir.path();
    {
        auto catalog = std::make_shared<TableCatalog>(config);
        CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
        SessionState session; (void)engine.Execute(session, "HELLO 3\r\n");
        const auto before = Tree(dir.path());
        const auto disabled = engine.Execute(session, "CREATE SLOT 'slot' ON alpha\r\n");
        assert(disabled.find("INVALID_ARGUMENT") != std::string::npos);
        assert(disabled.find("reason=feature_disabled") != std::string::npos);
        assert(disabled.find("feature=feed_slots") != std::string::npos);
        assert(Tree(dir.path()) == before);
        const auto disabled_migration = engine.Execute(session, "MIGRATE 'step' CREATE TABLE blocked (bits bits(8)) CHUNK 2 x 2\r\n");
        assert(disabled_migration.find("reason=feature_disabled") != std::string::npos &&
            disabled_migration.find("feature=migrations") != std::string::npos);
        assert(Tree(dir.path()) == before);
        assert(engine.Execute(session, "ENABLE FEATURE 'feed_slots'\r\n") == "+OK\r\n");
        assert(engine.Execute(session, "CREATE SLOT 'slot' ON alpha\r\n") == "+OK\r\n");
        assert(engine.Execute(session, "SHOW STATUS\r\n").starts_with("%3\r\n"));
    }
    WriteFeatureActivationJournal(dir.path(), journal);
    config.access_mode = AccessMode::kReadOnly;
    const auto before = Tree(dir.path());
    Reject([&] { TableCatalog catalog(config); }, "read-write");
    assert(Tree(dir.path()) == before);
    config.access_mode = AccessMode::kReadWrite;
    {
        TableCatalog catalog(config);
        assert(!ReadFeatureActivationJournal(dir.path()));
        assert((catalog.Find("alpha")->Acquire()->store().features().incompat & kFeatureSlotFilters) != 0U);
    }
}

void RuntimePublishedFailure() {
    test::ScopedTempDir dir("chunkdb-feature-outcome");
    (void)Seed(dir.path()); CatalogConfig config; config.data_dir = dir.path(); config.background_maintenance = true;
    {
        TableCatalog catalog(config);
        assert(FeatureActivationTestAccess::MaintenanceRunning(*catalog.Find("alpha")));
        auto subscriber = catalog.Find("alpha")->SubscribeFeed();
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_FEATURE_ROOT_SYNC_FAIL_ONCE", "1");
        try { (void)catalog.EnableFeature("slot_filters"); assert(false); }
        catch (const WriteOutcomeUnknownError& error) { assert(error.stage() == "feature_activation"); }
        assert(ReadFeatureActivationJournal(dir.path()));
        assert(!FeatureActivationTestAccess::MaintenanceRunning(*catalog.Find("alpha")));
        Reject([&] { (void)subscriber->Next(std::chrono::milliseconds(0)); }, "feature activation completion");
        Reject([&] { (void)catalog.FeatureStatus(); }, "feature activation completion");
        Reject([&] { (void)catalog.Find("alpha")->Acquire(); }, "feature activation completion");
    }
    TableCatalog catalog(config);
    assert(!ReadFeatureActivationJournal(dir.path()));
    assert((catalog.FeatureStatus().directory.incompat & kFeatureSlotFilters) != 0U);
}

void MetadataAdmission() {
    test::ScopedTempDir dir("chunkdb-feature-admission-lock"); (void)Seed(dir.path());
    CatalogConfig config; config.data_dir = dir.path(); TableCatalog catalog(config);
    struct Hook : MigrationTestHook {
        std::promise<void> entered;
        std::shared_future<void> released;
        bool once = false;
        explicit Hook(std::shared_future<void> release) : released(std::move(release)) {}
        void Run(Point point, std::string_view) override {
            if (point != Point::kBeforeTableExclusive || once) return;
            once = true; entered.set_value();
            assert(released.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
        }
    };
    std::promise<void> release; Hook hook(release.get_future().share());
    auto entered = hook.entered.get_future(); catalog.SetMigrationTestHook(&hook);
    const auto table = catalog.Find("alpha"); auto held = table->Acquire();
    auto activation = std::async(std::launch::async, [&] { return catalog.EnableFeature("slot_filters"); });
    assert(entered.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    assert(FeatureActivationTestAccess::MetadataExcluded(catalog));
    std::promise<void> ddl_started; auto started = ddl_started.get_future();
    auto ddl = std::async(std::launch::async, [&] {
        ddl_started.set_value(); return catalog.Create("after", {1U, 1U, 2U, 2U, 8U}, config.default_options);
    });
    assert(started.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    assert(!ReadFeatureActivationJournal(dir.path()) && !std::filesystem::exists(dir.path() / "tables/after"));
    release.set_value(); held.reset();
    assert(activation.wait_for(std::chrono::seconds(10)) == std::future_status::ready && activation.get());
    assert(ddl.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    const auto created = ddl.get();
    assert((created->Acquire()->store().features().incompat & kFeatureSlotFilters) != 0U);
    catalog.SetMigrationTestHook(nullptr);
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

int RunChild(const std::string& executable, const std::filesystem::path& root, const char* point, bool recover, bool bootstrap = false) {
    std::string command = "\"" + executable + "\" \"" + (bootstrap ? "--bootstrap" : recover ? "--recover" : "--activate") +
        "\" \"" + root.string() + "\" \"" + point + "\"";
#ifdef _WIN32
    return std::system(("\"" + command + "\"").c_str());
#else
    const auto result = std::system(command.c_str());
    return WIFEXITED(result) ? WEXITSTATUS(result) : -1;
#endif
}

void BootstrapCrashRecovery(const std::string& executable) {
    for (const char* point : {"CHUNKDB_FAILPOINT_CRASH_FEATURE_ACTIVATION_BEFORE_PUBLISH_ONCE",
        "CHUNKDB_FAILPOINT_CRASH_FEATURE_ACTIVATION_AFTER_PUBLISH_ONCE", "CHUNKDB_FAILPOINT_FEATURE_ACTIVATION_SYNC_FAIL_ONCE"}) {
        test::ScopedTempDir dir("chunkdb-feature-bootstrap-crash"); (void)Seed(dir.path());
        auto root = *ReadDataDirManifest(dir.path()); root.features.incompat = 0U;
        Save(DataDirManifestPath(dir.path()), SerializeDataDirManifest(root));
        const auto table = LoadFile(StoreManifestPath(dir.path() / "tables/alpha"));
        assert(RunChild(executable, dir.path(), point, false, true) == 86);
        assert(!ReadFeatureActivationJournal(dir.path()));
        const bool applied = std::string_view(point).find("BEFORE") == std::string_view::npos;
        assert(((ReadDataDirManifest(dir.path())->features.incompat & kFeatureActivation) != 0U) == applied);
        CatalogConfig config; config.data_dir = dir.path(); TableCatalog catalog(config);
        assert(LoadFile(StoreManifestPath(dir.path() / "tables/alpha")) == table);
        assert(catalog.EnableFeature("feature_activation") != applied);
        assert(!catalog.EnableFeature("feature_activation"));
    }
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
            if (std::string_view(argv[1]) == "--bootstrap") {
                CatalogConfig config; config.data_dir = argv[2]; TableCatalog catalog(config);
                try { (void)catalog.EnableFeature("feature_activation"); }
                catch (const WriteOutcomeUnknownError& error) {
                    assert(error.stage() == "feature_activation" && !ReadFeatureActivationJournal(argv[2]));
                    Reject([&] { (void)catalog.FeatureStatus(); }, "feature activation completion");
                    std::_Exit(86);
                }
            } else if (std::string_view(argv[1]) == "--recover") RecoverFeatureActivation(argv[2], AccessMode::kReadWrite);
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
    RuntimeActivation(); RuntimeAdmissionAndRecovery(); RuntimePublishedFailure(); MetadataAdmission(); Codec(); ValidationBeforePublication(); RecoveryPreservesCurrentMetadata(); DirectoryOnlyActivation(); BootstrapCrashRecovery(argv[0]); CrashRecovery(argv[0]);
}
