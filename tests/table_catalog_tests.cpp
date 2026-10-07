// Tables (#41): a data directory holds named tables, each with its own
// geometry and options, sharing one writer lock and one cache budget.

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/table_catalog.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"

namespace {

using chunkdb::CatalogConfig;
using chunkdb::GeometryConfig;
using chunkdb::TableCatalog;
using chunkdb::TableOptions;

constexpr GeometryConfig kDefaultGeometry{
    .large_chunk_width_chunks = 4,
    .large_chunk_height_chunks = 4,
    .chunk_width_blocks = 8,
    .chunk_height_blocks = 8,
    .block_bits = 4,
};

constexpr GeometryConfig kTerrainGeometry{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 3,
    .chunk_width_blocks = 16,
    .chunk_height_blocks = 4,
    .block_bits = 9,
};

CatalogConfig Config(const std::filesystem::path& data_dir) {
    CatalogConfig config;
    config.data_dir = data_dir;
    config.default_geometry = kDefaultGeometry;
    return config;
}

bool Contains(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

template <typename Fn>
std::string ErrorOf(Fn&& fn) {
    try {
        fn();
    } catch (const std::exception& e) {
        return e.what();
    }
    return {};
}

std::vector<std::string> Names(const std::filesystem::path& dir) {
    std::vector<std::string> names;
    if (!std::filesystem::exists(dir)) {
        return names;
    }
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        names.push_back(entry.path().filename().string());
    }
    std::sort(names.begin(), names.end());
    return names;
}

void WriteBits(TableCatalog& catalog, const std::string& table, std::int64_t x, std::int64_t y,
               const std::string& bits) {
    auto handle = catalog.Find(table);
    assert(handle != nullptr);
    auto lease = handle->Acquire();
    assert(lease.has_value());
    lease->store().SetBlockBits(x, y, bits);
}

std::string ReadBits(TableCatalog& catalog, const std::string& table, std::int64_t x,
                     std::int64_t y) {
    auto handle = catalog.Find(table);
    assert(handle != nullptr);
    auto lease = handle->Acquire();
    assert(lease.has_value());
    return lease->store().GetBlockBits(x, y);
}

void TestNewDataDirectoryCreatesDefault() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-new");
    {
        TableCatalog catalog(Config(dir.path()));
        assert(catalog.TableCount() == 1U);
        const auto tables = catalog.List();
        assert(tables[0].name == "default");
        assert(chunkdb::SameGeometry(tables[0].geometry, kDefaultGeometry));
        WriteBits(catalog, "default", 3, -5, "1011");
    }
    assert(std::filesystem::is_regular_file(dir.path() / "chunkdb.manifest"));
    assert(std::filesystem::is_regular_file(dir.path() / "tables" / "default" / "table.manifest"));
    // No store state at the top level: it belongs to the tables.
    for (const auto& name : Names(dir.path())) {
        assert(name == "chunkdb.manifest" || name == "tables" || name == ".chunkdb.lock" ||
               name == ".chunkdb.staging");
    }
    const auto manifest = chunkdb::ReadDataDirManifest(dir.path());
    assert(manifest.has_value());

    // Reopened without geometry flags: the stored geometry is used.
    auto config = Config(dir.path());
    config.default_geometry.block_bits = 12;
    config.default_geometry_fields = 0;
    TableCatalog reopened(config);
    assert(reopened.TableCount() == 1U);
    assert(reopened.Find("default")->geometry().block_bits == 4U);
    assert(ReadBits(reopened, "default", 3, -5) == "1011");
}

void TestTablesWithDifferentGeometry() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-two");
    chunkdb::StoreId terrain_id{};
    {
        TableCatalog catalog(Config(dir.path()));
        TableOptions options;
        options.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
        options.checkpoint_update_interval = 7;
        const auto terrain = catalog.Create("terrain", kTerrainGeometry, options);
        terrain_id = terrain->store_id();
        assert(terrain_id != catalog.Find("default")->store_id());
        WriteBits(catalog, "terrain", -40, 17, "110011001");
        WriteBits(catalog, "default", -40, 17, "0110");
        // Same coordinates, independent worlds.
        assert(ReadBits(catalog, "terrain", -40, 17) == "110011001");
        assert(ReadBits(catalog, "default", -40, 17) == "0110");
    }
    TableCatalog reopened(Config(dir.path()));
    const auto tables = reopened.List();
    assert(tables.size() == 2U);
    assert(tables[0].name == "default" && tables[1].name == "terrain");
    assert(chunkdb::SameGeometry(tables[1].geometry, kTerrainGeometry));
    assert(tables[1].store_id == terrain_id);
    assert(tables[1].options.durability_mode == chunkdb::DurabilityMode::kFsyncWal);
    assert(tables[1].options.checkpoint_update_interval == 7U);
    assert(ReadBits(reopened, "terrain", -40, 17) == "110011001");
    assert(ReadBits(reopened, "default", -40, 17) == "0110");
    {
        auto lease = reopened.Find("terrain")->Acquire();
        assert(lease->store().durability_mode() == chunkdb::DurabilityMode::kFsyncWal);
    }
}

void TestTableNames() {
    for (const std::string& valid : std::vector<std::string>{
             "a", "0", "terrain", "world_2", "my-table", "com", "com10", "lpt", "connect",
             std::string(64, 'x')}) {
        assert(chunkdb::IsValidTableName(valid));
    }
    for (const std::string& invalid : std::vector<std::string>{
             "", "Terrain", "-x", "_x", "a.b", "a b", "a/b", "..", "con", "nul", "aux", "prn",
             "com1", "lpt9", std::string(65, 'x'), "t\xc3\xa9"}) {
        assert(!chunkdb::IsValidTableName(invalid));
    }

    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-names");
    TableCatalog catalog(Config(dir.path()));
    assert(Contains(ErrorOf([&] { (void)catalog.Create("Bad", kTerrainGeometry, {}); }),
                    "invalid table name"));
    bool exists = false;
    try {
        (void)catalog.Create("default", kTerrainGeometry, {});
    } catch (const chunkdb::TableExistsError&) {
        exists = true;
    }
    assert(exists);
    GeometryConfig bad = kTerrainGeometry;
    bad.block_bits = 0;
    assert(!ErrorOf([&] { (void)catalog.Create("bad", bad, {}); }).empty());
    TableOptions bad_options;
    bad_options.checkpoint_wal_bytes = 0;
    assert(Contains(ErrorOf([&] { (void)catalog.Create("bad", kTerrainGeometry, bad_options); }),
                    "checkpoint_wal_bytes"));
    // Refused creates leave nothing behind.
    assert(Names(dir.path() / "tables") == std::vector<std::string>{"default"});
    assert(Names(dir.path() / ".chunkdb.staging").empty());
}

// A table that reached tables/ but could not be made durable is taken back
// out: the command fails and the name stays free.
void TestCreateUndoneWhenNotDurable() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-create-undo");
    TableCatalog catalog(Config(dir.path()));
#ifdef _WIN32
    _putenv_s("CHUNKDB_FAILPOINT_TABLE_CREATE_SYNC_FAIL_ONCE", "1");
#else
    setenv("CHUNKDB_FAILPOINT_TABLE_CREATE_SYNC_FAIL_ONCE", "1", 1);
#endif
    assert(Contains(ErrorOf([&] { (void)catalog.Create("terrain", kTerrainGeometry, {}); }),
                    "injected directory sync failure"));
    assert(catalog.Find("terrain") == nullptr);
    assert(!std::filesystem::exists(dir.path() / "tables" / "terrain"));
    assert(Names(dir.path() / ".chunkdb.dropped").empty());
    const auto created = catalog.Create("terrain", kTerrainGeometry, {});
    assert(created != nullptr);
}

void TestDrop() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-drop");
    TableCatalog catalog(Config(dir.path()));
    (void)catalog.Create("scratch", kTerrainGeometry, {});
    WriteBits(catalog, "scratch", 1, 1, "111111111");
    const auto handle = catalog.Find("scratch");

    catalog.Drop("scratch");
    assert(catalog.Find("scratch") == nullptr);
    assert(!handle->Acquire().has_value());
    assert(!std::filesystem::exists(dir.path() / "tables" / "scratch"));
    assert(Names(dir.path() / ".chunkdb.dropped").empty());
    bool not_found = false;
    try {
        catalog.Drop("scratch");
    } catch (const chunkdb::TableNotFoundError&) {
        not_found = true;
    }
    assert(not_found);

    // A new table of the same name is another table: the old handle stays
    // gone, and the data is not resurrected.
    GeometryConfig other = kTerrainGeometry;
    other.block_bits = 3;
    const auto recreated = catalog.Create("scratch", other, {});
    assert(recreated->store_id() != handle->store_id());
    assert(!handle->Acquire().has_value());
    {
        auto lease = recreated->Acquire();
        assert(!lease->store().BlockExists(1, 1));
    }

    // Dropping every table leaves a valid, empty data directory; a writer
    // that finds no table creates `default` again.
    catalog.Drop("scratch");
    catalog.Drop("default");
    assert(catalog.TableCount() == 0U);
}

void TestDropWaitsForRunningCommands() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-drop-wait");
    TableCatalog catalog(Config(dir.path()));
    (void)catalog.Create("busy", kTerrainGeometry, {});
    const auto handle = catalog.Find("busy");
    std::atomic<bool> dropped{false};
    std::thread dropper;
    {
        auto lease = handle->Acquire();
        assert(lease.has_value());
        dropper = std::thread([&] {
            catalog.Drop("busy");
            dropped.store(true);
        });
        // The drop cannot finish while the command holds its lease, and the
        // store stays usable until the command ends.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        assert(!dropped.load());
        lease->store().SetBlockBits(0, 0, "101010101");
    }
    dropper.join();
    assert(dropped.load());
    assert(!handle->Acquire().has_value());
}

// A command that arrives while a drop waits for running commands waits too,
// and then finds the table gone.
void TestAcquireDuringDropWaitsThenFails() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-drop-acquire");
    TableCatalog catalog(Config(dir.path()));
    (void)catalog.Create("busy", kTerrainGeometry, {});
    const auto handle = catalog.Find("busy");
    std::atomic<int> late_result{-1};  // -1 pending, 0 gone, 1 leased
    std::thread dropper;
    std::thread late;
    {
        auto lease = handle->Acquire();
        dropper = std::thread([&] { catalog.Drop("busy"); });
        // Give the drop time to mark the table busy (it then blocks on this
        // lease), then start a command.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        late = std::thread([&] {
            auto late_lease = handle->Acquire();
            late_result.store(late_lease.has_value() ? 1 : 0);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        assert(late_result.load() == -1);
    }
    dropper.join();
    late.join();
    assert(late_result.load() == 0);
}

// Option changes and drops never leave a table busy: whatever fails, the
// table is served again or taken out of service, and commands do not hang.
void TestFailedTableOperationsDoNotHang() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-failures");
    TableCatalog catalog(Config(dir.path()));
    (void)catalog.Create("terrain", kTerrainGeometry, {});
    WriteBits(catalog, "terrain", 4, 4, "110000011");
    const auto manifest = dir.path() / "tables" / "terrain" / "table.manifest";

    // A damaged manifest fails TABLESET before anything changes.
    std::ifstream in(manifest, std::ios::binary);
    std::vector<char> good((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    in.close();
    {
        auto damaged = good;
        damaged[20] ^= 0x01;
        std::ofstream(manifest, std::ios::binary | std::ios::trunc)
            .write(damaged.data(), static_cast<std::streamsize>(damaged.size()));
    }
    chunkdb::TableOptionsUpdate update;
    update.checkpoint_update_interval = 9;
    assert(Contains(ErrorOf([&] { catalog.SetOptions("terrain", update); }), "damaged"));
    assert(ReadBits(catalog, "terrain", 4, 4) == "110000011");
    std::ofstream(manifest, std::ios::binary | std::ios::trunc)
        .write(good.data(), static_cast<std::streamsize>(good.size()));

    // A drop that fails before its rename serves the table again.
#ifdef _WIN32
    _putenv_s("CHUNKDB_FAILPOINT_TABLE_DROP_RENAME_FAIL_ONCE", "1");
#else
    setenv("CHUNKDB_FAILPOINT_TABLE_DROP_RENAME_FAIL_ONCE", "1", 1);
#endif
    assert(Contains(ErrorOf([&] { catalog.Drop("terrain"); }), "injected failure"));
    assert(ReadBits(catalog, "terrain", 4, 4) == "110000011");

    // A table whose new store cannot open is taken out of service until
    // restart; connections holding it get "gone" instead of waiting.
    const auto handle = catalog.Find("terrain");
#ifdef _WIN32
    _putenv_s("CHUNKDB_FAILPOINT_TABLESET_REOPEN_FAIL_ONCE", "1");
#else
    setenv("CHUNKDB_FAILPOINT_TABLESET_REOPEN_FAIL_ONCE", "1", 1);
#endif
    assert(Contains(ErrorOf([&] { catalog.SetOptions("terrain", update); }), "injected failure"));
    assert(!handle->Acquire().has_value());
    assert(catalog.Find("terrain") == nullptr);
    catalog.WalBarrier();  // does not wait for the retired table
}

// TABLESET changes only the options it names.
void TestOptionUpdatesArePatches() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-patch");
    TableCatalog catalog(Config(dir.path()));
    chunkdb::TableOptionsUpdate first;
    first.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    chunkdb::TableOptionsUpdate second;
    second.checkpoint_update_interval = 5;
    assert(second.ApplyTo(TableOptions{}).durability_mode == chunkdb::DurabilityMode::kRelaxed);
    catalog.SetOptions("default", first);
    catalog.SetOptions("default", second);
    const auto options = catalog.Find("default")->Info().options;
    assert(options.durability_mode == chunkdb::DurabilityMode::kFsyncWal);
    assert(options.checkpoint_update_interval == 5U);
    assert(chunkdb::TableOptionsUpdate{}.empty());
}

void TestSetOptions() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-set");
    {
        TableCatalog catalog(Config(dir.path()));
        WriteBits(catalog, "default", 2, 2, "0011");
        TableOptions options;
        options.durability_mode = chunkdb::DurabilityMode::kFsyncCheckpoint;
        options.checkpoint_update_interval = 3;
        options.checkpoint_wal_bytes = 4096;
        options.wal_group_commit_updates = 2;
        options.checkpoint_compression = chunkdb::CheckpointCompression::kZrle;
        const auto handle = catalog.Find("default");
        catalog.SetOptions("default", options);
        // The handle a connection holds keeps working, on the reopened store.
        auto lease = handle->Acquire();
        assert(lease.has_value());
        assert(lease->store().durability_mode() == chunkdb::DurabilityMode::kFsyncCheckpoint);
        assert(lease->store().checkpoint_compression() == chunkdb::CheckpointCompression::kZrle);
        assert(lease->store().GetBlockBits(2, 2) == "0011");
        lease->store().SetBlockBits(2, 3, "1100");
        assert(Contains(ErrorOf([&] {
                            TableOptions invalid;
                            invalid.checkpoint_update_interval = 0;
                            catalog.SetOptions("default", invalid);
                        }),
                        "checkpoint_updates"));
        bool not_found = false;
        try {
            catalog.SetOptions("missing", options);
        } catch (const chunkdb::TableNotFoundError&) {
            not_found = true;
        }
        assert(not_found);
    }
    TableCatalog reopened(Config(dir.path()));
    const auto info = reopened.Find("default")->Info();
    assert(info.options.durability_mode == chunkdb::DurabilityMode::kFsyncCheckpoint);
    assert(info.options.checkpoint_update_interval == 3U);
    assert(info.options.checkpoint_wal_bytes == 4096U);
    assert(info.options.wal_group_commit_updates == 2U);
    assert(info.options.checkpoint_compression == chunkdb::CheckpointCompression::kZrle);
    assert(ReadBits(reopened, "default", 2, 2) == "0011");
    assert(ReadBits(reopened, "default", 2, 3) == "1100");
}

void TestDefaultGeometryFlags() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-flags");
    { TableCatalog catalog(Config(dir.path())); }
    auto config = Config(dir.path());
    config.default_geometry.block_bits = 6;
    config.default_geometry_fields = chunkdb::kGeometryBlockBits;
    const auto error = ErrorOf([&] { TableCatalog catalog(config); });
    assert(Contains(error, "table 'default'"));
    assert(Contains(error, "block_bits 6 (stored 4)"));

    // An option flag must match the options every table stores: a server
    // started with --durability fsync-wal must not serve a relaxed table.
    config = Config(dir.path());
    config.default_geometry_fields = 0;
    {
        TableCatalog catalog(config);
        TableOptions sky;
        sky.checkpoint_update_interval = 64;
        (void)catalog.Create("sky", kTerrainGeometry, sky);
    }
    const auto before = Names(dir.path() / "tables" / "sky");
    config.default_options.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.default_options.checkpoint_update_interval = 7;
    config.default_option_fields =
        chunkdb::kOptionFieldDurabilityMode | chunkdb::kOptionFieldCheckpointUpdates;
    const auto refused = ErrorOf([&] { TableCatalog catalog(config); });
    assert(Contains(refused, "--durability fsync-wal (table 'default' stores relaxed)"));
    assert(Contains(refused, "--durability fsync-wal (table 'sky' stores relaxed)"));
    assert(Contains(refused, "--checkpoint-updates 7 (table 'sky' stores 64)"));
    assert(Names(dir.path() / "tables" / "sky") == before);

    // Flags that are not given, or that match, open as usual.
    config.default_options.durability_mode = chunkdb::DurabilityMode::kRelaxed;
    config.default_options.checkpoint_update_interval = 64;
    config.default_option_fields = chunkdb::kOptionFieldDurabilityMode;
    { TableCatalog catalog(config); }
    config.default_option_fields = chunkdb::kOptionFieldCheckpointUpdates;
    assert(Contains(ErrorOf([&] { TableCatalog catalog(config); }),
                    "--checkpoint-updates 64 (table 'default' stores 256)"));
    config.default_option_fields = 0;
    TableCatalog reopened(config);
    assert(reopened.TableCount() == 2U);
}

void TestDirectoryRules() {
    {
        // Something chunkdb writes, without a data-directory manifest.
        chunkdb::test::ScopedTempDir dir("chunkdb-catalog-unknown");
        std::filesystem::create_directories(dir.path() / "L_0_0");
        const auto error = ErrorOf([&] { TableCatalog catalog(Config(dir.path())); });
        assert(Contains(error, "has no chunkdb.manifest but holds chunkdb data (found 'L_0_0')"));
        assert(Names(dir.path()) == (std::vector<std::string>{".chunkdb.lock", "L_0_0"}));
    }
    {
        // A single store of a 2.0 development build before tables.
        chunkdb::test::ScopedTempDir dir("chunkdb-catalog-single-store");
        chunkdb::StoreManifest manifest{
            .features = {},
            .geometry = kDefaultGeometry,
            .store_id = chunkdb::NewStoreId(),
            .options = {},
        };
        const auto bytes = chunkdb::SerializeStoreManifest(manifest);
        std::ofstream(dir.path() / "chunkdb.manifest", std::ios::binary)
            .write(reinterpret_cast<const char*>(bytes.data()),
                   static_cast<std::streamsize>(bytes.size()));
        const auto error = ErrorOf([&] { TableCatalog catalog(Config(dir.path())); });
        assert(Contains(error, "single-store data directory"));
    }
    {
        // Entries chunkdb never creates are not chunkdb's.
        chunkdb::test::ScopedTempDir dir("chunkdb-catalog-foreign");
        std::filesystem::create_directories(dir.path() / "lost+found");
        std::ofstream(dir.path() / "notes.txt") << "x";
        TableCatalog catalog(Config(dir.path()));
        assert(catalog.TableCount() == 1U);
    }
    {
        chunkdb::test::ScopedTempDir dir("chunkdb-catalog-tables-dir");
        { TableCatalog catalog(Config(dir.path())); }
        // An OS metadata file in tables/ is ignored ...
        std::ofstream(dir.path() / "tables" / ".DS_Store") << "x";
        { TableCatalog catalog(Config(dir.path())); }
        // ... but a table directory without a manifest is damage: tables are
        // only ever published complete.
        std::filesystem::create_directories(dir.path() / "tables" / "orphan");
        const auto error = ErrorOf([&] { TableCatalog catalog(Config(dir.path())); });
        assert(Contains(error, "has no table.manifest"));
        assert(std::filesystem::is_directory(dir.path() / "tables" / "orphan"));
    }
}

void TestReadOnly() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-read-only");
    auto config = Config(dir.path());
    config.access_mode = chunkdb::AccessMode::kReadOnly;
    assert(Contains(ErrorOf([&] { TableCatalog catalog(config); }), "has no chunkdb.manifest"));
    {
        TableCatalog writer(Config(dir.path()));
        (void)writer.Create("terrain", kTerrainGeometry, {});
        WriteBits(writer, "terrain", 5, 5, "000111000");
    }
    TableCatalog reader(config);
    assert(reader.TableCount() == 2U);
    assert(ReadBits(reader, "terrain", 5, 5) == "000111000");
    {
        // A table the writer drops is not read as an empty one: its
        // generation record, which a writer never removes, is gone.
        TableCatalog writer(Config(dir.path()));
        WriteBits(writer, "terrain", 900, 900, "111111111");
        writer.WalBarrier();
        writer.Drop("terrain");
    }
    const auto terrain = reader.Find("terrain");
    auto lease = terrain->Acquire();
    assert(Contains(ErrorOf([&] { (void)lease->store().GetBlockBits(900, 900); }),
                    "disappeared after the store was opened"));
    assert(Contains(ErrorOf([&] { (void)lease->store().ScanPopulatedChunks(false, {}, 10); }),
                    "disappeared after the store was opened"));
    lease.reset();
    assert(Contains(ErrorOf([&] { (void)reader.Create("x", kTerrainGeometry, {}); }),
                    "read-only"));
    assert(Contains(ErrorOf([&] { reader.Drop("terrain"); }), "read-only"));
    assert(Contains(ErrorOf([&] { reader.SetOptions("terrain", TableOptions{}); }), "read-only"));
}

void TestSingleWriter() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-lock");
    TableCatalog writer(Config(dir.path()));
    assert(Contains(ErrorOf([&] { TableCatalog second(Config(dir.path())); }),
                    "already has an active writer"));
    // The tables are not separately lockable: their stores never take a lock.
    assert(!std::filesystem::exists(dir.path() / "tables" / "default" / ".chunkdb.lock"));
    // A store opened directly on a table takes the data directory's lock, so
    // it cannot write beside the catalog that owns it.
    chunkdb::StoreConfig store_config;
    store_config.geometry = kDefaultGeometry;
    store_config.data_dir = dir.path() / "tables" / "default";
    assert(Contains(ErrorOf([&] { chunkdb::ChunkStore store(store_config); }),
                    "already has an active writer"));
}

// One cache budget for all tables: a busy table takes the memory an idle one
// does not use.
void TestSharedCacheBudget() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-cache");
    auto config = Config(dir.path());
    config.max_loaded_chunks = 1000;  // evicts down to 744 when exceeded
    TableCatalog catalog(config);
    (void)catalog.Create("hot", kTerrainGeometry, {});
    const auto& geometry = kDefaultGeometry;
    const auto& hot_geometry = kTerrainGeometry;
    {
        auto cold = catalog.Find("default")->Acquire();
        for (std::int64_t i = 0; i < 600; ++i) {
            cold->store().SetBlockBits(i * geometry.chunk_width_blocks, 0, "0001");
        }
        assert(cold->store().ApproxLoadedChunkCount() == 600U);
    }
    auto hot = catalog.Find("hot")->Acquire();
    for (std::int64_t i = 0; i < 600; ++i) {
        hot->store().SetBlockBits(i * hot_geometry.chunk_width_blocks, 0, "000000001");
    }
    const auto resources = catalog.resources();
    assert(resources->LoadedChunkCount() <= 1000U);
    assert(hot->store().ApproxLoadedChunkCount() == 600U);
    {
        auto cold = catalog.Find("default")->Acquire();
        const auto cold_loaded = cold->store().ApproxLoadedChunkCount();
        assert(cold_loaded < 600U);
        assert(cold_loaded + 600U == resources->LoadedChunkCount());
        // Evicted chunks were flushed, not lost.
        for (std::int64_t i = 0; i < 600; i += 37) {
            assert(cold->store().GetBlockBits(i * geometry.chunk_width_blocks, 0) == "0001");
        }
    }
    // A dropped table's chunks leave the shared count with it.
    hot.reset();
    const auto before = resources->LoadedChunkCount();
    catalog.Drop("hot");
    assert(resources->LoadedChunkCount() == before - 600U);
}

// File descriptors belong to the process: one stream budget for all tables.
void TestSharedWalStreamBudget() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-streams");
    auto config = Config(dir.path());
    config.max_open_wal_streams = 4;
    config.default_options.wal_group_commit_updates = 1;
    TableCatalog catalog(config);
    (void)catalog.Create("other", kTerrainGeometry, config.default_options);
    const auto resources = catalog.resources();
    {
        auto lease = catalog.Find("default")->Acquire();
        for (std::int64_t i = 0; i < 4; ++i) {
            lease->store().SetBlockBits(i * kDefaultGeometry.chunk_width_blocks, 0, "0101");
        }
        assert(lease->store().OpenWalStreamCountForTests() == 4U);
    }
    {
        auto lease = catalog.Find("other")->Acquire();
        for (std::int64_t i = 0; i < 3; ++i) {
            lease->store().SetBlockBits(i * kTerrainGeometry.chunk_width_blocks, 0, "010101010");
        }
        assert(lease->store().OpenWalStreamCountForTests() == 3U);
    }
    assert(resources->OpenWalStreamCount() <= 4U);
    {
        auto lease = catalog.Find("default")->Acquire();
        assert(lease->store().OpenWalStreamCountForTests() == 1U);
    }
    catalog.Drop("other");
    assert(resources->OpenWalStreamCount() == 1U);
}

// Streams opened concurrently by several tables stay within the shared cap.
void TestSharedWalStreamCapUnderConcurrency() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-streams-concurrent");
    auto config = Config(dir.path());
    config.max_open_wal_streams = 3;
    config.default_options.wal_group_commit_updates = 1;
    TableCatalog catalog(config);
    for (const auto* name : {"a", "b", "c"}) {
        (void)catalog.Create(name, kDefaultGeometry, config.default_options);
    }
    const auto resources = catalog.resources();
    std::atomic<bool> done{false};
    std::atomic<std::size_t> peak{0};
    std::thread sampler([&] {
        while (!done.load()) {
            const auto open = resources->OpenWalStreamCount();
            if (open > peak.load()) {
                peak.store(open);
            }
        }
    });
    std::vector<std::thread> writers;
    for (const auto* name : {"default", "a", "b", "c"}) {
        writers.emplace_back([&, name] {
            const auto table = catalog.Find(name);
            for (std::int64_t i = 0; i < 60; ++i) {
                auto lease = table->Acquire();
                lease->store().SetBlockBits(i * kDefaultGeometry.chunk_width_blocks, 0, "1001");
            }
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }
    done.store(true);
    sampler.join();
    assert(peak.load() <= 3U);
    for (const auto* name : {"default", "a", "b", "c"}) {
        assert(ReadBits(catalog, name, 59 * kDefaultGeometry.chunk_width_blocks, 0) == "1001");
    }
}

void TestWalBarrierCoversAllTables() {
    chunkdb::test::ScopedTempDir dir("chunkdb-catalog-barrier");
    TableCatalog catalog(Config(dir.path()));
    (void)catalog.Create("second", kTerrainGeometry, {});
    WriteBits(catalog, "default", 0, 0, "1111");
    WriteBits(catalog, "second", 0, 0, "111111111");
    catalog.WalBarrier();
    for (const auto& name : {"default", "second"}) {
        auto lease = catalog.Find(name)->Acquire();
        assert(lease->store().RuntimeStats().wal_barriers == 1U);
    }
}

// Crash boundaries: a child process opens the catalog with one failpoint
// armed and dies there (exit code 86).
int RunCrashChild(const std::filesystem::path& data_dir, const std::string& action) {
    CatalogConfig config = Config(data_dir);
    TableCatalog catalog(config);
    if (action == "create") {
        (void)catalog.Create("terrain", kTerrainGeometry, {});
    } else if (action == "drop") {
        catalog.Drop("terrain");
    }
    return 0;
}

void RunChild(const std::string& executable, const std::filesystem::path& data_dir,
              const std::string& action, const char* failpoint) {
    std::string command = "\"" + executable + "\" --crash-child \"" + data_dir.string() + "\" " +
                          action;
#ifdef _WIN32
    _putenv_s(failpoint, "1");
    command = "\"" + command + "\"";
#else
    setenv(failpoint, "1", 1);
#endif
    const int status = std::system(command.c_str());
#ifdef _WIN32
    _putenv_s(failpoint, "");
#else
    unsetenv(failpoint);
#endif
    assert(status != 0);
}

void TestCrashBoundaries(const std::string& executable) {
    {
        chunkdb::test::ScopedTempDir dir("chunkdb-catalog-crash-manifest-before");
        RunChild(executable, dir.path(), "open",
                 "CHUNKDB_FAILPOINT_CRASH_DATA_DIR_MANIFEST_BEFORE_PUBLISH_ONCE");
        assert(!std::filesystem::exists(dir.path() / "chunkdb.manifest"));
        TableCatalog catalog(Config(dir.path()));
        assert(catalog.TableCount() == 1U);
        for (const auto& name : Names(dir.path())) {
            assert(name.rfind("chunkdb.manifest.tmp.", 0) != 0);
        }
    }
    {
        // Published, but no table yet: the next writer start creates default.
        chunkdb::test::ScopedTempDir dir("chunkdb-catalog-crash-manifest-after");
        RunChild(executable, dir.path(), "open",
                 "CHUNKDB_FAILPOINT_CRASH_DATA_DIR_MANIFEST_AFTER_PUBLISH_ONCE");
        assert(std::filesystem::exists(dir.path() / "chunkdb.manifest"));
        assert(!std::filesystem::exists(dir.path() / "tables"));
        TableCatalog catalog(Config(dir.path()));
        assert(catalog.Find("default") != nullptr);
    }
    {
        // Before the rename: no table, and the staging leftover goes away.
        chunkdb::test::ScopedTempDir dir("chunkdb-catalog-crash-create-before");
        { TableCatalog catalog(Config(dir.path())); }
        RunChild(executable, dir.path(), "create",
                 "CHUNKDB_FAILPOINT_CRASH_TABLE_CREATE_BEFORE_PUBLISH_ONCE");
        assert(!std::filesystem::exists(dir.path() / "tables" / "terrain"));
        assert(Names(dir.path() / ".chunkdb.staging").size() == 1U);
        TableCatalog catalog(Config(dir.path()));
        assert(catalog.Find("terrain") == nullptr);
        assert(Names(dir.path() / ".chunkdb.staging").empty());
        (void)catalog.Create("terrain", kTerrainGeometry, {});
    }
    {
        // After the rename: the complete table.
        chunkdb::test::ScopedTempDir dir("chunkdb-catalog-crash-create-after");
        { TableCatalog catalog(Config(dir.path())); }
        RunChild(executable, dir.path(), "create",
                 "CHUNKDB_FAILPOINT_CRASH_TABLE_CREATE_AFTER_PUBLISH_ONCE");
        assert(Names(dir.path() / "tables" / "terrain") ==
               std::vector<std::string>{"table.manifest"});
        TableCatalog catalog(Config(dir.path()));
        const auto terrain = catalog.Find("terrain");
        assert(terrain != nullptr);
        assert(chunkdb::SameGeometry(terrain->geometry(), kTerrainGeometry));
        WriteBits(catalog, "terrain", 1, 2, "100000001");
        assert(ReadBits(catalog, "terrain", 1, 2) == "100000001");
    }
    {
        // After the drop's rename (its commit point): the table is gone and
        // its leftovers are removed at the next start.
        chunkdb::test::ScopedTempDir dir("chunkdb-catalog-crash-drop");
        {
            TableCatalog catalog(Config(dir.path()));
            (void)catalog.Create("terrain", kTerrainGeometry, {});
            WriteBits(catalog, "terrain", 1, 2, "100000001");
        }
        RunChild(executable, dir.path(), "drop",
                 "CHUNKDB_FAILPOINT_CRASH_TABLE_DROP_AFTER_RENAME_ONCE");
        assert(!std::filesystem::exists(dir.path() / "tables" / "terrain"));
        assert(Names(dir.path() / ".chunkdb.dropped").size() == 1U);
        TableCatalog catalog(Config(dir.path()));
        assert(catalog.Find("terrain") == nullptr);
        assert(Names(dir.path() / ".chunkdb.dropped").empty());
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::string(argv[1]) == "--crash-child") {
        return RunCrashChild(argv[2], argv[3]);
    }
    chunkdb::SetLogLevel(chunkdb::LogLevel::kWarn);
    TestNewDataDirectoryCreatesDefault();
    TestTablesWithDifferentGeometry();
    TestTableNames();
    TestCreateUndoneWhenNotDurable();
    TestDrop();
    TestDropWaitsForRunningCommands();
    TestAcquireDuringDropWaitsThenFails();
    TestFailedTableOperationsDoNotHang();
    TestOptionUpdatesArePatches();
    TestSetOptions();
    TestDefaultGeometryFlags();
    TestDirectoryRules();
    TestReadOnly();
    TestSingleWriter();
    TestSharedCacheBudget();
    TestSharedWalStreamBudget();
    TestSharedWalStreamCapUnderConcurrency();
    TestWalBarrierCoversAllTables();
    TestCrashBoundaries(argv[0]);
    std::cout << "table catalog tests passed\n";
    return 0;
}
