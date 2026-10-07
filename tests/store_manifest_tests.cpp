// Table manifest (`table.manifest`): a table (store) directory records its
// geometry and options when it is created, and a store refuses to open it with
// any other geometry instead of reading existing data as zeros and mixing new
// writes into it. The server and chunkdb_verify work on a data directory whose
// `default` table is such a store.
//
// argv[1] is the chunkdb_server binary and argv[2] the chunkdb_verify binary;
// both are run as child processes to check the command-line behavior.

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/table_catalog.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"

namespace {

using chunkdb::test::ScopedTempDir;

const chunkdb::GeometryConfig kDefaultGeometry{
    .large_chunk_width_chunks = 8,
    .large_chunk_height_chunks = 8,
    .chunk_width_blocks = 16,
    .chunk_height_blocks = 16,
    .block_bits = 16,
};

chunkdb::StoreConfig Config(
    const std::filesystem::path& data_dir,
    chunkdb::GeometryConfig geometry = kDefaultGeometry) {
    chunkdb::StoreConfig config;
    config.geometry = geometry;
    config.data_dir = data_dir;
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    return config;
}

chunkdb::StoreConfig ReadOnly(chunkdb::StoreConfig config) {
    config.access_mode = chunkdb::AccessMode::kReadOnly;
    return config;
}

// Creates a data directory whose default table has `geometry`, runs `write`
// on that table, and returns the table directory.
std::filesystem::path CreateDataDir(
    const std::filesystem::path& data_dir,
    chunkdb::GeometryConfig geometry = kDefaultGeometry,
    const std::function<void(chunkdb::ChunkStore&)>& write = {},
    std::size_t checkpoint_update_interval = 256) {
    auto config = Config(data_dir, geometry);
    config.checkpoint_update_interval = checkpoint_update_interval;
    chunkdb::TableCatalog catalog(chunkdb::CatalogConfigFromStoreConfig(config));
    if (write) {
        auto lease = *catalog.Find("default")->Acquire();
        write(lease.store());
    }
    return data_dir / "tables" / "default";
}

std::vector<std::uint8_t> ReadBytes(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void WriteBytes(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    assert(out);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(out);
}

std::string ReadText(const std::filesystem::path& path) {
    const auto bytes = ReadBytes(path);
    return std::string(bytes.begin(), bytes.end());
}

// Every entry under `root` with the bytes of every file, so a test can show
// that a refused open left the directory exactly as it was.
std::map<std::string, std::vector<std::uint8_t>> Tree(const std::filesystem::path& root) {
    std::map<std::string, std::vector<std::uint8_t>> tree;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        const auto relative = std::filesystem::relative(entry.path(), root).generic_string();
        tree[relative] = entry.is_regular_file() ? ReadBytes(entry.path())
                                                 : std::vector<std::uint8_t>{'<', 'd', '>'};
    }
    return tree;
}

// Runs `open` and returns the message it throws; fails the test if it does
// not throw.
std::string ExpectRefused(const std::function<void()>& open) {
    try {
        open();
    } catch (const std::exception& e) {
        return e.what();
    }
    assert(false && "the store opened but should have been refused");
    return {};
}

bool Contains(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

std::vector<std::uint8_t> WithCrc(std::vector<std::uint8_t> bytes) {
    const std::size_t crc_offset = bytes.size() - 4U;
    const auto crc = chunkdb::Crc32(bytes.data(), crc_offset);
    for (std::size_t i = 0; i < 4U; ++i) {
        bytes[crc_offset + i] = static_cast<std::uint8_t>((crc >> (8U * i)) & 0xFFU);
    }
    return bytes;
}

int ExitCode(int status) {
#ifdef _WIN32
    return status;
#else
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
#endif
}

// Runs `executable args` with stdout and stderr captured; returns the exit
// code and stores the output in `*output`.
int Run(
    const std::string& executable,
    const std::string& args,
    const std::filesystem::path& output_path,
    std::string* output) {
    std::string command =
        "\"" + executable + "\" " + args + " > \"" + output_path.string() + "\" 2>&1";
#ifdef _WIN32
    // cmd strips the outermost quote pair of the whole line.
    command = "\"" + command + "\"";
#endif
    const int status = std::system(command.c_str());
    *output = ReadText(output_path);
    return ExitCode(status);
}

void TestNewStoreRecordsGeometryAndId() {
    ScopedTempDir dir("chunkdb-manifest-new");
    const auto data_dir = dir.path() / "data";
    const chunkdb::GeometryConfig geometry{
        .large_chunk_width_chunks = 2,
        .large_chunk_height_chunks = 3,
        .chunk_width_blocks = 4,
        .chunk_height_blocks = 5,
        .block_bits = 7,
    };
    chunkdb::StoreId first_id{};
    {
        chunkdb::ChunkStore store(Config(data_dir, geometry));
        first_id = store.store_id();
        store.SetBlockBits(3, 4, "1010101");
    }
    const auto bytes = ReadBytes(chunkdb::StoreManifestPath(data_dir));
    const auto manifest = chunkdb::ParseStoreManifest(bytes);
    assert(chunkdb::SameGeometry(manifest.geometry, geometry));
    assert(manifest.features.incompat == 0U && manifest.features.ro_compat == 0U &&
           manifest.features.compat == 0U);
    // Every option is recorded, as the store was opened with it.
    const auto config = Config(data_dir, geometry);
    assert(manifest.options == chunkdb::EncodeTableOptions(chunkdb::TableOptions{
                                   .durability_mode = config.durability_mode,
                                   .checkpoint_update_interval = config.checkpoint_update_interval,
                                   .checkpoint_wal_bytes = config.checkpoint_wal_bytes,
                                   .wal_group_commit_updates = config.wal_group_commit_updates,
                                   .checkpoint_compression = config.checkpoint_compression,
                               }));
    assert(manifest.options.size() == 46U);
    // A table created with a block width is one column bits(block_bits).
    assert(manifest.schema == chunkdb::SingleBitsColumnSchema(7));
    assert(bytes.size() == chunkdb::kStoreManifestMinSize + manifest.options.size() +
                               chunkdb::EncodeTableSchema(manifest.schema).size());
    const auto options = chunkdb::DecodeTableOptions(manifest.options);
    assert(options.durability_mode == chunkdb::DurabilityMode::kFsyncWal);
    assert(options.checkpoint_update_interval == config.checkpoint_update_interval);
    assert(manifest.store_id == first_id);
    assert(manifest.store_id != chunkdb::StoreId{});

    {
        chunkdb::ChunkStore reopened(Config(data_dir, geometry));
        assert(reopened.store_id() == first_id);
        assert(reopened.GetBlockBits(3, 4) == "1010101");
    }
    // The manifest is never rewritten.
    assert(ReadBytes(chunkdb::StoreManifestPath(data_dir)) == bytes);

    ScopedTempDir other("chunkdb-manifest-new-other");
    chunkdb::ChunkStore second(Config(other.path(), geometry));
    assert(second.store_id() != first_id);
}

void TestGeometryMismatchRefusedWithoutChanges() {
    ScopedTempDir dir("chunkdb-manifest-mismatch");
    {
        chunkdb::ChunkStore store(Config(dir.path()));
        store.SetBlockBits(100, 5, "1111000011110000");
        store.SetBlockBits(300, 5, "1010101010101010");
    }
    const auto before = Tree(dir.path());

    struct Change {
        const char* field;
        std::function<void(chunkdb::GeometryConfig*)> apply;
    };
    const std::vector<Change> changes = {
        {"large_chunk_width 4 (stored 8)",
         [](chunkdb::GeometryConfig* g) { g->large_chunk_width_chunks = 4; }},
        {"large_chunk_height 4 (stored 8)",
         [](chunkdb::GeometryConfig* g) { g->large_chunk_height_chunks = 4; }},
        {"chunk_width 32 (stored 16)",
         [](chunkdb::GeometryConfig* g) { g->chunk_width_blocks = 32; }},
        {"chunk_height 8 (stored 16)",
         [](chunkdb::GeometryConfig* g) { g->chunk_height_blocks = 8; }},
        {"block_bits 32 (stored 16)", [](chunkdb::GeometryConfig* g) { g->block_bits = 32; }},
    };
    for (const auto& change : changes) {
        auto geometry = kDefaultGeometry;
        change.apply(&geometry);
        for (const bool read_only : {false, true}) {
            auto config = Config(dir.path(), geometry);
            if (read_only) {
                config = ReadOnly(config);
            }
            const auto message = ExpectRefused([&] { chunkdb::ChunkStore store(config); });
            assert(Contains(message, change.field));
            assert(Contains(message, "stored geometry is " +
                                         chunkdb::DescribeGeometry(kDefaultGeometry)));
            assert(Tree(dir.path()) == before);
        }
    }

    // Several differing fields are all named.
    auto geometry = kDefaultGeometry;
    geometry.block_bits = 32;
    geometry.large_chunk_width_chunks = 4;
    const auto message =
        ExpectRefused([&] { chunkdb::ChunkStore store(Config(dir.path(), geometry)); });
    assert(Contains(message, "large_chunk_width 4 (stored 8)"));
    assert(Contains(message, "block_bits 32 (stored 16)"));
    assert(Tree(dir.path()) == before);

    chunkdb::ChunkStore store(Config(dir.path()));
    assert(store.GetBlockBits(100, 5) == "1111000011110000");
    assert(store.GetBlockBits(300, 5) == "1010101010101010");
}

void TestOmittedFieldsUseStoredGeometry() {
    ScopedTempDir dir("chunkdb-manifest-adopt");
    const chunkdb::GeometryConfig stored{
        .large_chunk_width_chunks = 4,
        .large_chunk_height_chunks = 4,
        .chunk_width_blocks = 8,
        .chunk_height_blocks = 8,
        .block_bits = 32,
    };
    const std::string bits(32, '1');
    {
        chunkdb::ChunkStore store(Config(dir.path(), stored));
        store.SetBlockBits(-9, 17, bits);
    }

    // No fields given: the request's geometry values are ignored.
    auto config = Config(dir.path(), kDefaultGeometry);
    config.geometry_fields = 0;
    {
        chunkdb::ChunkStore store(config);
        assert(chunkdb::SameGeometry(store.geometry().config(), stored));
        assert(store.GetBlockBits(-9, 17) == bits);
    }
    {
        chunkdb::ChunkStore reader(ReadOnly(config));
        assert(chunkdb::SameGeometry(reader.geometry().config(), stored));
        assert(reader.GetBlockBits(-9, 17) == bits);
    }

    // A given field must match; the others are still taken from the store.
    config.geometry_fields = chunkdb::kGeometryBlockBits;
    config.geometry.block_bits = 32;
    {
        chunkdb::ChunkStore store(config);
        assert(chunkdb::SameGeometry(store.geometry().config(), stored));
    }
    config.geometry.block_bits = 16;
    const auto message = ExpectRefused([&] { chunkdb::ChunkStore store(config); });
    assert(Contains(message, "block_bits 16 (stored 32)"));
    assert(!Contains(message, "large_chunk_width 8"));
}

void TestDamagedManifestRefused() {
    ScopedTempDir dir("chunkdb-manifest-damaged");
    { chunkdb::ChunkStore store(Config(dir.path())); }
    const auto manifest_path = chunkdb::StoreManifestPath(dir.path());
    const auto good = ReadBytes(manifest_path);

    struct Damage {
        const char* name;
        std::vector<std::uint8_t> bytes;
        const char* reason;
    };
    // Field offsets: version 4, reserved 6, flags 8..20, geometry 20..36
    // (chunk_height 32..36), store id 36..52, options size 52, options, schema
    // size, schema, CRC at the end.
    std::vector<Damage> damages;
    damages.push_back({"truncated", {good.begin(), good.begin() + 63}, "size is 63 bytes"});
    damages.push_back({"cut", {good.begin(), good.end() - 1}, "checksum mismatch"});
    {
        auto bytes = good;
        bytes.insert(bytes.end() - 4, 0);  // one byte the options size does not cover
        damages.push_back({"extended", WithCrc(bytes), "schema size"});
    }
    {
        auto bytes = good;
        bytes[0] = 'X';
        damages.push_back({"magic", WithCrc(bytes), "bad magic"});
    }
    {
        auto bytes = good;
        bytes[36] ^= 0x01U;  // a store id byte, CRC left stale
        damages.push_back({"crc", bytes, "checksum mismatch"});
    }
    {
        auto bytes = good;
        bytes[4] = 4;
        damages.push_back({"version", WithCrc(bytes), "unsupported manifest version 4"});
    }
    {
        // The #38 manifest layout, written by development builds.
        auto bytes = good;
        bytes[4] = 1;
        bytes.resize(46);
        damages.push_back({"version 1", bytes, "manifest version 1 was written by a 2.0 development"});
    }
    {
        // The manifest before typed columns, written by development builds.
        auto bytes = good;
        bytes[4] = 2;
        damages.push_back({"version 2", WithCrc(bytes), "manifest version 2 was written by a 2.0 development"});
    }
    {
        auto bytes = good;
        bytes[6] = 1;
        damages.push_back({"reserved", WithCrc(bytes), "reserved field is not zero"});
    }
    {
        auto bytes = good;
        for (std::size_t i = 32; i < 36; ++i) {
            bytes[i] = 0;  // chunk_height = 0
        }
        damages.push_back({"geometry", WithCrc(bytes), "invalid geometry"});
    }
    {
        auto bytes = good;
        for (std::size_t i = 36; i < 52; ++i) {
            bytes[i] = 0;
        }
        damages.push_back({"store id", WithCrc(bytes), "store id is zero"});
    }
    {
        // An option entry of a type no feature in the flags explains.
        auto manifest = chunkdb::ParseStoreManifest(good);
        manifest.options = {99, 0, 1, 0, 0xAB};
        damages.push_back({"option", chunkdb::SerializeStoreManifest(manifest), "unknown option type 99"});
    }
    {
        auto manifest = chunkdb::ParseStoreManifest(good);
        manifest.options = {99, 0, 9, 0, 0xAB};  // length 9, one byte present
        damages.push_back({"option length", chunkdb::SerializeStoreManifest(manifest), "overruns"});
    }
    // Malformed known options.
    const auto with_options = [&](std::vector<std::uint8_t> options) {
        auto manifest = chunkdb::ParseStoreManifest(good);
        manifest.options = std::move(options);
        return chunkdb::SerializeStoreManifest(manifest);
    };
    damages.push_back({"durability", with_options({1, 0, 1, 0, 9}), "unknown durability mode 9"});
    damages.push_back(
        {"compression", with_options({5, 0, 1, 0, 2}), "unknown checkpoint compression 2"});
    damages.push_back(
        {"repeated", with_options({1, 0, 1, 0, 0, 1, 0, 1, 0, 1}), "option 1 appears twice"});
    damages.push_back({"u64 length", with_options({2, 0, 1, 0, 5}), "option 2 has length 1"});
    damages.push_back(
        {"zero", with_options({3, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0}), "option 3 has value 0"});
    {
        auto bytes = good;
        bytes.resize(chunkdb::kStoreManifestMaxSize + 1U, 0);
        damages.push_back({"oversized", bytes, "more than 1048576"});
    }
    {
        // A schema area whose column count is zero: the schema starts after
        // the options and its size, with version (8) and next id (4) first.
        auto bytes = good;
        const std::size_t schema_at = 56U + chunkdb::ParseStoreManifest(good).options.size() + 4U;
        for (std::size_t i = schema_at + 12U; i < schema_at + 16U; ++i) {
            bytes[i] = 0;
        }
        bytes.resize(schema_at + 16U);
        const auto schema_size = static_cast<std::uint32_t>(16U);
        for (std::size_t i = 0; i < 4U; ++i) {
            bytes[schema_at - 4U + i] = static_cast<std::uint8_t>((schema_size >> (8U * i)) & 0xFFU);
        }
        bytes.resize(bytes.size() + 4U);
        damages.push_back({"schema", WithCrc(bytes), "invalid schema: a table needs at least one column"});
    }

    for (const auto& damage : damages) {
        WriteBytes(manifest_path, damage.bytes);
        const auto before = Tree(dir.path());
        for (const bool read_only : {false, true}) {
            auto config = Config(dir.path());
            config.geometry_fields = 0;
            if (read_only) {
                config = ReadOnly(config);
            }
            const auto message = ExpectRefused([&] { chunkdb::ChunkStore store(config); });
            assert(Contains(message, "table manifest"));
            assert(Contains(message, damage.reason));
            assert(Tree(dir.path()) == before);
        }
    }

    // A manifest that is not a regular file.
    std::filesystem::remove(manifest_path);
    std::filesystem::create_directory(manifest_path);
    const auto message =
        ExpectRefused([&] { chunkdb::ChunkStore store(Config(dir.path())); });
    assert(Contains(message, "not a regular file"));
}

std::string ServerArgs(const std::filesystem::path& data_dir);

// Rewrites the manifest of an existing store with other feature flags and
// options, as a newer build that uses those features would have written it.
void RewriteManifest(
    const std::filesystem::path& data_dir,
    chunkdb::FeatureFlags features,
    std::vector<std::uint8_t> options = {}) {
    const auto path = chunkdb::StoreManifestPath(data_dir);
    auto manifest = chunkdb::ParseStoreManifest(ReadBytes(path));
    manifest.features = features;
    manifest.options = std::move(options);
    WriteBytes(path, chunkdb::SerializeStoreManifest(manifest));
}

// An unknown incompat bit refuses every open, an unknown ro_compat bit
// refuses writing but allows reading, an unknown compat bit is ignored.
void TestUnknownFeatureFlags(const std::string& server, const std::string& verify) {
    const chunkdb::FeatureFlags incompat{.incompat = 1U << 5U, .ro_compat = 0, .compat = 0};
    const chunkdb::FeatureFlags ro_compat{.incompat = 0, .ro_compat = 1U << 3U, .compat = 0};
    const chunkdb::FeatureFlags compat{.incompat = 0, .ro_compat = 0, .compat = 1U << 9U};
    // An option owned by the unknown feature: skipped, not corruption.
    const std::vector<std::uint8_t> foreign_option = {99, 0, 2, 0, 0xAB, 0xCD};

    ScopedTempDir dir("chunkdb-manifest-features");
    const auto data_dir = dir.path() / "data";
    const auto table_dir = CreateDataDir(data_dir, kDefaultGeometry, [](chunkdb::ChunkStore& store) {
        store.SetBlockBits(5, 5, "1100110011001100");
    });
    const auto log = dir.path() / "child.log";
    std::string output;

    RewriteManifest(table_dir, incompat, foreign_option);
    auto before = Tree(data_dir);
    for (const bool read_only : {false, true}) {
        auto config = Config(table_dir);
        if (read_only) {
            config = ReadOnly(config);
        }
        const auto message = ExpectRefused([&] { chunkdb::ChunkStore store(config); });
        assert(Contains(message, "does not support (unknown incompat=0x20"));
        assert(Tree(data_dir) == before);
    }
    assert(Run(verify, "--data-dir \"" + data_dir.string() + "\"", log, &output) == 1);
    assert(Contains(output, "VERIFY error manifest_unknown_features"));

    RewriteManifest(table_dir, ro_compat, foreign_option);
    before = Tree(data_dir);
    {
        const auto message =
            ExpectRefused([&] { chunkdb::ChunkStore store(Config(table_dir)); });
        assert(Contains(message, "can only be opened read-only"));
        assert(Contains(message, "ro_compat=0x8"));
        assert(Tree(data_dir) == before);
    }
    {
        chunkdb::ChunkStore reader(ReadOnly(Config(table_dir)));
        assert(reader.features().ro_compat == ro_compat.ro_compat);
        assert(reader.GetBlockBits(5, 5) == "1100110011001100");
    }
    // The server always opens read-write.
    assert(Run(server, ServerArgs(data_dir), log, &output) != 0);
    assert(Contains(output, "table 'default'"));
    assert(Contains(output, "can only be opened read-only"));
    assert(!Contains(output, "store initialized"));
    assert(Tree(data_dir) == before);
    assert(Run(verify, "--data-dir \"" + data_dir.string() + "\"", log, &output) == 1);
    assert(Contains(output, "VERIFY warning manifest_unknown_features"));
    assert(Contains(output, " errors=0"));

    RewriteManifest(table_dir, compat, foreign_option);
    {
        chunkdb::ChunkStore store(Config(table_dir));
        assert(store.features().compat == compat.compat);
        assert(store.GetBlockBits(5, 5) == "1100110011001100");
        store.SetBlockBits(6, 6, "0011001100110011");
    }
    {
        chunkdb::ChunkStore reopened(Config(table_dir));
        assert(reopened.GetBlockBits(6, 6) == "0011001100110011");
    }
}

void TestDirectoryWithoutManifestRefused() {
    // An initialized store whose manifest was lost: its data is never
    // reinterpreted with whatever geometry the next start requests.
    {
        ScopedTempDir dir("chunkdb-manifest-lost");
        {
            chunkdb::ChunkStore store(Config(dir.path()));
            store.SetBlockBits(0, 0, "1111000011110000");
        }
        std::filesystem::remove(chunkdb::StoreManifestPath(dir.path()));
        const auto before = Tree(dir.path());
        for (const bool read_only : {false, true}) {
            auto config = Config(dir.path());
            if (read_only) {
                config = ReadOnly(config);
            }
            const auto message = ExpectRefused([&] { chunkdb::ChunkStore store(config); });
            assert(Contains(message, "has no table.manifest"));
            assert(Tree(dir.path()) == before);
        }
    }
    // Chunk data written by an older build (no manifest, chunk directories).
    {
        ScopedTempDir dir("chunkdb-manifest-legacy");
        std::filesystem::create_directories(dir.path() / "L_0_0");
        WriteBytes(dir.path() / "L_0_0" / "C_0_0.wal", {1, 2, 3});
        const auto before = Tree(dir.path());
        const auto message =
            ExpectRefused([&] { chunkdb::ChunkStore store(Config(dir.path())); });
        assert(Contains(message, "found 'L_0_0'"));
        assert(Contains(message, "not a table of this chunkdb build"));
        assert(Tree(dir.path()) == before);
    }
    // Bookkeeping of an older store counts as chunkdb data too.
    for (const char* name : {"chunkdb.version", ".chunkdb.initialized", "chunkdb.snapshot"}) {
        ScopedTempDir dir("chunkdb-manifest-legacy-bookkeeping");
        WriteBytes(dir.path() / name, {1});
        const auto before = Tree(dir.path());
        const auto message =
            ExpectRefused([&] { chunkdb::ChunkStore store(Config(dir.path())); });
        assert(Contains(message, std::string("found '") + name + "'"));
        assert(Tree(dir.path()) == before);
    }
    // Entries chunkdb never creates (a volume root's lost+found, desktop
    // metadata) do not keep a new store from being created, and stay as is.
    {
        ScopedTempDir dir("chunkdb-manifest-foreign");
        std::filesystem::create_directories(dir.path() / "lost+found");
        WriteBytes(dir.path() / ".DS_Store", {'h', 'i'});
        WriteBytes(dir.path() / "L_notes", {'x'});
        {
            chunkdb::ChunkStore store(Config(dir.path()));
            store.SetBlockBits(0, 0, "1111000011110000");
        }
        assert(ReadBytes(dir.path() / ".DS_Store") == (std::vector<std::uint8_t>{'h', 'i'}));
        assert(std::filesystem::is_directory(dir.path() / "lost+found"));
        chunkdb::ChunkStore reopened(Config(dir.path()));
        assert(reopened.GetBlockBits(0, 0) == "1111000011110000");
    }
    // Read-only mode never initializes a store.
    {
        ScopedTempDir dir("chunkdb-manifest-read-only-empty");
        const auto message =
            ExpectRefused([&] { chunkdb::ChunkStore store(ReadOnly(Config(dir.path()))); });
        assert(Contains(message, "has no table.manifest"));
        assert(std::filesystem::is_empty(dir.path()));
        const auto missing = dir.path() / "missing";
        (void)ExpectRefused([&] { chunkdb::ChunkStore store(ReadOnly(Config(missing))); });
        assert(!std::filesystem::exists(missing));
    }
}

void TestInterruptedInitializationStartsOver() {
    // What a crash during initialization can leave: the writer lock and a
    // manifest that was written but never published.
    ScopedTempDir dir("chunkdb-manifest-interrupted");
    std::filesystem::create_directories(dir.path() / ".chunkdb.lock");
    const auto stale_tmp = dir.path() / "table.manifest.tmp.2147483000.1.2.3";
    WriteBytes(
        stale_tmp,
        chunkdb::SerializeStoreManifest(
            {.features = {}, .geometry = kDefaultGeometry, .store_id = chunkdb::NewStoreId(),
             .options = {}, .schema = chunkdb::SingleBitsColumnSchema(kDefaultGeometry.block_bits)}));

    auto geometry = kDefaultGeometry;
    geometry.block_bits = 9;
    {
        chunkdb::ChunkStore store(Config(dir.path(), geometry));
        store.SetBlockBits(1, 1, "101010101");
    }
    assert(!std::filesystem::exists(stale_tmp));
    const auto manifest = chunkdb::ReadStoreManifest(dir.path());
    assert(manifest.has_value());
    assert(chunkdb::SameGeometry(manifest->geometry, geometry));
}

// A store at a volume root shares it with `lost+found`, which ext4 creates
// readable only by root. Nothing in the store may read or descend into it:
// not opening, not the full-directory barrier sync that runs when change
// tracking overflows, and not chunkdb_verify.
void TestStoreBesideUnreadableForeignDirectory(const std::string& verify) {
    ScopedTempDir dir("chunkdb-manifest-volume-root");
    const auto data_dir = dir.path() / "data";
    const auto foreign = data_dir / "lost+found";
    std::filesystem::create_directories(foreign / "inner");
    std::filesystem::permissions(foreign, std::filesystem::perms::none);
    {
        auto config = Config(data_dir);
        config.durability_mode = chunkdb::DurabilityMode::kRelaxed;
        chunkdb::TableCatalog catalog(chunkdb::CatalogConfigFromStoreConfig(config));
        auto lease = *catalog.Find("default")->Acquire();
        lease.store().SetBlockBits(0, 0, "1111000011110000");
        lease.store().ForceUnsyncedOverflowForTests();
        catalog.WalBarrier();
    }
    {
        chunkdb::TableCatalog catalog(chunkdb::CatalogConfigFromStoreConfig(Config(data_dir)));
        auto lease = *catalog.Find("default")->Acquire();
        assert(lease.store().GetBlockBits(0, 0) == "1111000011110000");
    }
    std::string output;
    assert(Run(verify, "--data-dir \"" + data_dir.string() + "\"", dir.path() / "verify.log",
               &output) == 0);
    assert(Contains(output, "VERIFY info foreign_entry"));
    assert(Contains(output, " errors=0"));
    std::filesystem::permissions(foreign, std::filesystem::perms::owner_all);
}

// A crash while the version clock, its marker, the snapshot-generation
// record, a conditional intent or the writer's heartbeat was being replaced
// leaves a temp file of a
// process that is gone. The next writer removes them; a read-only store
// leaves them alone.
void TestInterruptedRecordReplacementsAreRemoved() {
    ScopedTempDir dir("chunkdb-manifest-record-tmp");
    {
        chunkdb::ChunkStore store(Config(dir.path(), kDefaultGeometry));
        store.SetBlockBits(0, 0, "1010101010101010");
    }
    std::filesystem::create_directories(dir.path() / ".chunkdb.intents");
    const std::vector<std::filesystem::path> stale = {
        dir.path() / "chunkdb.version.tmp.2147483000.1.2.3",
        dir.path() / ".chunkdb.initialized.tmp.2147483000.1.2.3",
        dir.path() / "chunkdb.snapshot.tmp.2147483000.1.2.3",
        dir.path() / ".chunkdb.intents" / "L_0_0__C_0_0.wal.rollback.tmp.2147483000.1.2.3",
        dir.path() / ".chunkdb.lock" / "writer.meta.tmp.2147483000.1.2.3",
    };
    for (const auto& path : stale) {
        WriteBytes(path, std::vector<std::uint8_t>{1, 2, 3});
    }
    {
        auto read_only = Config(dir.path(), kDefaultGeometry);
        read_only.access_mode = chunkdb::AccessMode::kReadOnly;
        chunkdb::ChunkStore store(read_only);
        assert(store.GetBlockBits(0, 0) == "1010101010101010");
    }
    for (const auto& path : stale) {
        assert(std::filesystem::exists(path));
    }
    {
        chunkdb::ChunkStore store(Config(dir.path(), kDefaultGeometry));
        assert(store.GetBlockBits(0, 0) == "1010101010101010");
    }
    for (const auto& path : stale) {
        assert(!std::filesystem::exists(path));
    }
}

// The data-directory manifest's version_floor option round-trips; a wrong
// length or a second entry is damage.
void TestDataDirVersionFloorOption() {
    chunkdb::DataDirManifest manifest{.features = {}, .data_dir_id = chunkdb::NewStoreId(), .options = {}};
    assert(chunkdb::DataDirVersionFloor(manifest) == 0U);
    chunkdb::SetDataDirVersionFloor(&manifest, 16385);
    chunkdb::SetDataDirVersionFloor(&manifest, 32769);
    const auto parsed = chunkdb::ParseDataDirManifest(chunkdb::SerializeDataDirManifest(manifest));
    assert(chunkdb::DataDirVersionFloor(parsed) == 32769U);
    const std::vector<std::uint8_t> entry = {1, 0, 8, 0, 1, 0, 0, 0, 0, 0, 0, 0};
    for (const auto& options : {std::vector<std::uint8_t>{1, 0, 4, 0, 1, 0, 0, 0},
                                [&] { auto twice = entry; twice.insert(twice.end(), entry.begin(), entry.end()); return twice; }()}) {
        manifest.options = options;
        bool refused = false;
        try {
            (void)chunkdb::ParseDataDirManifest(chunkdb::SerializeDataDirManifest(manifest));
        } catch (const std::exception&) {
            refused = true;
        }
        assert(refused);
    }
}

void TestPublishNewFileNeverReplaces() {
    ScopedTempDir dir("chunkdb-manifest-publish");
    const auto target = dir.path() / "chunkdb.manifest";
    assert(chunkdb::PublishNewFile(target, {1, 2, 3}));
    assert(ReadBytes(target) == (std::vector<std::uint8_t>{1, 2, 3}));
    assert(!chunkdb::PublishNewFile(target, {4, 5, 6}));
    assert(ReadBytes(target) == (std::vector<std::uint8_t>{1, 2, 3}));
    std::size_t entries = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path())) {
        (void)entry;
        ++entries;
    }
    assert(entries == 1U);  // no temporary file is left behind
}

// Server arguments that end the run right after the store is opened: TLS
// with a certificate that does not exist (or a build without TLS) makes
// creating the network server fail, with no network access involved.
std::string ServerArgs(const std::filesystem::path& data_dir) {
    const auto missing = data_dir.string() + "-missing.pem";
    return "--no-auth --listen-uri chunks://127.0.0.1:4242/ --tls-cert \"" + missing +
           "\" --tls-key \"" + missing + "\" --data-dir \"" + data_dir.string() + "\"";
}

// Steps 2 and 4 of the reported defect, through the server binary.
void TestServerRefusesChangedGeometryFlags(const std::string& server) {
    ScopedTempDir dir("chunkdb-manifest-server");
    const auto data_dir = dir.path() / "data";
    const auto table_dir =
        CreateDataDir(data_dir, kDefaultGeometry, [](chunkdb::ChunkStore& store) {
            store.SetBlockBits(100, 5, "1111000011110000");
            store.SetBlockBits(300, 5, "1010101010101010");
        });
    const auto before = Tree(data_dir);
    const std::string common = ServerArgs(data_dir);
    const auto log = dir.path() / "server.log";
    std::string output;

    assert(Run(server, common + " --block-bits 32", log, &output) != 0);
    assert(Contains(output, "block_bits 32 (stored 16)"));
    assert(Contains(output, "stored geometry is " + chunkdb::DescribeGeometry(kDefaultGeometry)));
    assert(!Contains(output, "store initialized"));
    assert(Tree(data_dir) == before);

    assert(Run(server, common + " --large-chunk-width 4", log, &output) != 0);
    assert(Contains(output, "large_chunk_width 4 (stored 8)"));
    assert(!Contains(output, "store initialized"));
    assert(Tree(data_dir) == before);

    // An option flag that differs from what the table stores refuses the
    // start too (the table was created fsync-wal), instead of serving it with
    // other options than the command line says.
    assert(Run(server, common + " --durability relaxed", log, &output) != 0);
    assert(Contains(output, "--durability relaxed (table 'default' stores fsync-wal)"));
    assert(!Contains(output, "store initialized"));
    assert(Tree(data_dir) == before);
    assert(Run(server, common + " --durability fsync-wal", log, &output) != 0);
    assert(Contains(output, "store initialized"));

    // Without geometry flags, and with matching ones, the store opens with its
    // recorded geometry; the run then ends when the network server fails.
    for (const std::string flags : {"", " --block-bits 16 --large-chunk-width 8"}) {
        assert(Run(server, common + flags, log, &output) != 0);
        assert(Contains(output, "store initialized"));
        assert(Contains(output, chunkdb::DescribeGeometry(kDefaultGeometry)));
    }

    {
        chunkdb::ChunkStore store(Config(table_dir));
        assert(store.GetBlockBits(100, 5) == "1111000011110000");
        assert(store.GetBlockBits(300, 5) == "1010101010101010");
    }

    // A store created with non-default geometry restarts without repeating
    // its geometry flags: omitted flags are not compared with the defaults.
    auto wide = kDefaultGeometry;
    wide.block_bits = 32;
    wide.large_chunk_width_chunks = 4;
    const auto wide_dir = dir.path() / "wide";
    (void)CreateDataDir(wide_dir, wide);
    const std::string wide_common = ServerArgs(wide_dir);
    assert(Run(server, wide_common, log, &output) != 0);
    assert(Contains(output, "store initialized"));
    assert(Contains(output, chunkdb::DescribeGeometry(wide)));
    assert(Run(server, wide_common + " --block-bits 16", log, &output) != 0);
    assert(Contains(output, "block_bits 16 (stored 32)"));
    assert(!Contains(output, "store initialized"));
}

void TestVerifyUsesManifestGeometry(const std::string& verify) {
    ScopedTempDir dir("chunkdb-manifest-verify");
    const auto data_dir = dir.path() / "data";
    // Not the default geometry: before the manifest, verify needed it passed
    // as flags and reported every artifact as invalid without them.
    const chunkdb::GeometryConfig geometry{
        .large_chunk_width_chunks = 2,
        .large_chunk_height_chunks = 2,
        .chunk_width_blocks = 4,
        .chunk_height_blocks = 4,
        .block_bits = 5,
    };
    const auto table_dir = CreateDataDir(
        data_dir, geometry,
        [](chunkdb::ChunkStore& store) {
            for (std::int64_t i = 0; i < 20; ++i) {
                store.SetBlockBits(i * 3, -i * 5, "10011");
            }
        },
        /*checkpoint_update_interval=*/2);
    const auto log = dir.path() / "verify.log";
    const std::string args = "--data-dir \"" + data_dir.string() + "\"";
    std::string output;

    assert(Run(verify, args, log, &output) == 0);
    assert(Contains(output, "SUMMARY checked="));
    assert(Contains(output, " errors=0"));

    // Geometry flags are gone: the manifest is the only source.
    assert(Run(verify, args + " --block-bits 5", log, &output) == 2);

    const auto manifest_path = chunkdb::StoreManifestPath(table_dir);
    const auto good = ReadBytes(manifest_path);
    auto damaged = good;
    damaged[30] ^= 0xFFU;
    WriteBytes(manifest_path, damaged);
    assert(Run(verify, args, log, &output) == 1);
    assert(Contains(output, "VERIFY error manifest_invalid"));
    assert(Contains(output, "checksum mismatch"));

    std::filesystem::remove(manifest_path);
    assert(Run(verify, args, log, &output) == 1);
    assert(Contains(output, "VERIFY error manifest_missing"));
    WriteBytes(manifest_path, good);
    assert(Run(verify, args, log, &output) == 0);

    // The data-directory manifest is checked first; without a usable one no
    // table is checked.
    const auto root_manifest = chunkdb::DataDirManifestPath(data_dir);
    const auto root_good = ReadBytes(root_manifest);
    auto root_damaged = root_good;
    root_damaged[10] ^= 0x01U;
    WriteBytes(root_manifest, root_damaged);
    assert(Run(verify, args, log, &output) == 1);
    assert(Contains(output, "VERIFY error data_dir_manifest_invalid"));
    assert(Contains(output, "checksum mismatch"));
    assert(Contains(output, "checked=1 "));
    std::filesystem::remove(root_manifest);
    assert(Run(verify, args, log, &output) == 1);
    assert(Contains(output, "VERIFY error data_dir_manifest_missing"));
    // A single store of a 2.0 development build before tables.
    WriteBytes(root_manifest, good);
    assert(Run(verify, args, log, &output) == 1);
    assert(Contains(output, "single-store data directory"));
    WriteBytes(root_manifest, root_good);

    // Leftovers of interrupted table operations and store state outside a
    // table are reported.
    std::filesystem::create_directories(data_dir / ".chunkdb.staging" / "terrain.0123");
    std::filesystem::create_directories(data_dir / ".chunkdb.dropped" / "old.4567");
    std::filesystem::create_directories(data_dir / "L_0_0");
    WriteBytes(data_dir / "tables" / "notes.txt", {'x'});
    assert(Run(verify, args, log, &output) == 1);
    assert(Contains(output, "VERIFY warning interrupted_table_create"));
    assert(Contains(output, "VERIFY warning interrupted_table_drop"));
    assert(Contains(output, "VERIFY warning unexpected_entry"));
    assert(Contains(output, "VERIFY info foreign_entry"));
    assert(Contains(output, " errors=0"));
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        throw std::invalid_argument(
            "usage: chunkdb_store_manifest_test <chunkdb_server> <chunkdb_verify>");
    }
    TestNewStoreRecordsGeometryAndId();
    TestGeometryMismatchRefusedWithoutChanges();
    TestOmittedFieldsUseStoredGeometry();
    TestDamagedManifestRefused();
    TestDirectoryWithoutManifestRefused();
    TestInterruptedInitializationStartsOver();
    TestInterruptedRecordReplacementsAreRemoved();
    TestDataDirVersionFloorOption();
    TestPublishNewFileNeverReplaces();
    TestStoreBesideUnreadableForeignDirectory(argv[2]);
    TestServerRefusesChangedGeometryFlags(argv[1]);
    TestUnknownFeatureFlags(argv[1], argv[2]);
    TestVerifyUsesManifestGeometry(argv[2]);
    return 0;
}
