#include <cassert>
#include <cstdlib>
#include <functional>
#include <fstream>
#include <iterator>
#include <string>

#include "chunkdb/chunk_columns.hpp"
#include "chunkdb/table_catalog.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;

std::vector<std::uint8_t> ReadBytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

CatalogConfig Config(const std::filesystem::path& path) {
    CatalogConfig config;
    config.data_dir = path;
    return config;
}

Column MetadataColumn(std::string name, bool required = false, bool with_default = false) {
    return {.id = 0U, .name = std::move(name), .type = {ColumnKind::kUnsigned, 8U},
        .required = required, .has_default = with_default,
        .default_value = with_default ? std::vector<std::uint8_t>{7U} : std::vector<std::uint8_t>{}};
}

void Reject(const std::function<void()>& action) {
    bool refused = false;
    try { action(); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}

class Failure {
  public:
    explicit Failure(const char* name) : name_(name) {
#ifdef _WIN32
        _putenv_s(name_, "1");
#else
        setenv(name_, "1", 1);
#endif
    }
    ~Failure() {
#ifdef _WIN32
        _putenv_s(name_, "");
#else
        unsetenv(name_);
#endif
    }
  private:
    const char* name_;
};

void Create(TableCatalog& catalog, std::string_view name) {
    GeometryConfig geometry;
    geometry.chunk_width_blocks = 2U;
    geometry.chunk_height_blocks = 2U;
    geometry.block_bits = 4U;
    (void)catalog.Create(name, geometry, catalog.default_options());
}

void SharedRevisionAndReopen() {
    test::ScopedTempDir directory("chunkdb-table-chunk-columns");
    {
        TableCatalog catalog(Config(directory.path()));
        Create(catalog, "t");
        {
            auto lease = catalog.Find("t")->Acquire();
            lease->store().SetBlockBits(0, 0, "0101");
        }
        catalog.ChangeChunkColumns("t", [](const auto& schema) {
            return AddChunkColumn(schema, MetadataColumn("owner", true, true));
        });
        const auto info = catalog.Find("t")->Info();
        ValidateSchemaDomains(info.schema, info.chunk_schema);
        assert(info.schema.version == 2U && info.chunk_schema.version == 2U);
        assert(info.schema.history.back().changes.front().kind == SchemaChange::Kind::kOtherDomain);
        assert(info.schema.columns.front().id == info.chunk_schema.columns.front().id);
        {
            auto lease = catalog.Find("t")->Acquire();
            assert(lease->store().GetBlockBits(0, 0) == "0101");
            const auto state = lease->store().ReadChunkState(0, 0);
            assert(state.chunk_columns);
            assert(std::get<std::uint64_t>(DecodeChunkColumns(info.chunk_schema, state.chunk_columns->bytes).front()) == 7U);
        }
        const auto manifest = ReadBytes(StoreManifestPath(directory.path() / "tables/t"));
        assert(!catalog.ChangeChunkColumnsIfNeeded("t", [](const auto&) { return std::optional<TableSchema>{}; }));
        assert(ReadBytes(StoreManifestPath(directory.path() / "tables/t")) == manifest);
        Reject([&] { catalog.ChangeChunkColumns("t", [](const auto& schema) {
            return AddChunkColumn(schema, MetadataColumn("owner"));
        }); });
        assert(ReadBytes(StoreManifestPath(directory.path() / "tables/t")) == manifest);
        catalog.ChangeChunkColumns("t", [](const auto& schema) { return RenameChunkColumn(schema, "owner", "label"); });
        assert(catalog.Find("t")->Info().schema.version == 3U);
    }
    {
        TableCatalog catalog(Config(directory.path()));
        const auto info = catalog.Find("t")->Info();
        assert(info.chunk_schema.columns.front().name == "label" && info.chunk_schema.version == 3U);
        ValidateSchemaDomains(info.schema, info.chunk_schema);
        catalog.ChangeChunkColumns("t", [](const auto& schema) { return DropChunkColumn(schema, "label"); });
        const auto empty = catalog.Find("t")->Info();
        assert(empty.chunk_schema.columns.empty() && empty.chunk_schema.version == 4U && empty.schema.version == 4U);
        ValidateSchemaDomains(empty.schema, empty.chunk_schema);
    }
}

void RequiredIncludesTombstones() {
    test::ScopedTempDir directory("chunkdb-required-chunk-columns");
    {
        TableCatalog catalog(Config(directory.path()));
        Create(catalog, "empty");
        {
            auto lease = catalog.Find("empty")->Acquire();
            assert(lease->store().GetChunkVersion(9, 9) == 0U);
        }
        catalog.ChangeChunkColumns("empty", [](const auto& schema) {
            return AddChunkColumn(schema, MetadataColumn("required", true), true);
        });
        Create(catalog, "tombstone");
        {
            auto lease = catalog.Find("tombstone")->Acquire();
            lease->store().SetBlockBits(0, 0, "0011");
            lease->store().UnsetBlock(0, 0);
            assert(!lease->store().HasPresentBlocks());
            assert(lease->store().HasWrittenChunkIdentities());
        }
        Reject([&] { catalog.ChangeChunkColumns("tombstone", [](const auto& schema) {
            return AddChunkColumn(schema, MetadataColumn("required", true), true);
        }); });
        assert(catalog.Find("tombstone")->Info().chunk_schema.columns.empty());
    }
    TableCatalog catalog(Config(directory.path()));
    Reject([&] { catalog.ChangeChunkColumns("tombstone", [](const auto& schema) {
        return AddChunkColumn(schema, MetadataColumn("required", true), true);
    }); });
    assert(catalog.Find("tombstone")->Info().chunk_schema.version == 1U);
}

void PublicationFailuresAndReadOnly() {
    test::ScopedTempDir directory("chunkdb-chunk-column-failures");
    {
        TableCatalog catalog(Config(directory.path()));
        Create(catalog, "t");
        {
            Failure fail("CHUNKDB_FAILPOINT_ALTER_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
            bool failed = false;
            try { catalog.ChangeChunkColumns("t", [](const auto& schema) {
                return AddChunkColumn(schema, MetadataColumn("published"));
            }); } catch (const std::runtime_error&) { failed = true; }
            assert(failed);
        }
        assert(catalog.Find("t")->Info().chunk_schema.columns.front().name == "published");
        {
            Failure fail("CHUNKDB_FAILPOINT_TABLESET_REOPEN_FAIL_ONCE");
            bool failed = false;
            try { catalog.ChangeChunkColumns("t", [](const auto& schema) {
                return RenameChunkColumn(schema, "published", "recovered");
            }); } catch (const std::runtime_error&) { failed = true; }
            assert(failed);
        }
        assert(catalog.Find("t") == nullptr && catalog.Fences().size() == 1U);
        catalog.RecoverTable("t");
        assert(catalog.Find("t")->Info().chunk_schema.columns.front().name == "recovered");
    }
    auto config = Config(directory.path());
    config.access_mode = AccessMode::kReadOnly;
    TableCatalog reader(config);
    bool callback = false;
    Reject([&] { reader.ChangeChunkColumns("t", [&](const auto& schema) { callback = true; return schema; }); });
    assert(!callback);
}
}  // namespace

int main() {
    SharedRevisionAndReopen();
    RequiredIncludesTombstones();
    PublicationFailuresAndReadOnly();
}
