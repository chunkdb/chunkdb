#include <cassert>
#include <charconv>
#include <iostream>
#include <sstream>

#include "archive_base_format.hpp"
#include "backup.hpp"
#include "chunk_coordinate_index.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_columns.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/file_layout.hpp"
#include "feed_archive.hpp"
#include "feed_slot_records.hpp"
#include "feed_slots.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "verify.hpp"

namespace {
using namespace chunkdb;
constexpr ChunkCoord kLive{-1, -1, -1};
constexpr ChunkCoord kDeleted{-2, 1, -3};

CatalogConfig Config(const std::filesystem::path& root, DurabilityMode mode) {
    CatalogConfig config;
    config.data_dir = root;
    config.background_maintenance = false;
    config.default_options.durability_mode = mode;
    config.default_options.checkpoint_update_interval = 1U;
    config.default_options.wal_group_commit_updates = 1U;
    return config;
}

struct Commands {
    explicit Commands(const std::shared_ptr<TableCatalog>& catalog)
        : engine(EngineConfig{.require_auth = false}, catalog) {
        assert(engine.Execute(session, "HELLO 3\r\n").starts_with("%"));
    }
    std::string Run(std::string_view cql) {
        auto reply = engine.Execute(session, std::string(cql) + "\r\n");
        if (reply.starts_with("-")) std::cerr << cql << ": " << reply;
        assert(!reply.starts_with("-"));
        return reply;
    }
    std::uint64_t Write(std::string_view cql) {
        const auto reply = Run(cql);
        assert(reply.starts_with(":"));
        std::uint64_t revision = 0U;
        const auto parsed = std::from_chars(reply.data() + 1U, reply.data() + reply.size() - 2U, revision);
        assert(parsed.ec == std::errc{} && parsed.ptr == reply.data() + reply.size() - 2U && revision != 0U);
        return revision;
    }
    CommandEngine engine;
    SessionState session;
};

ChunkState State(TableCatalog& catalog, ChunkCoord coord) {
    auto lease = catalog.Require("t")->Acquire();
    return lease->store().ReadChunkState(coord);
}

void Identity(const ChunkState& actual, const ChunkState& expected) {
    assert(actual.written && actual.version == expected.version && actual.commit_time_ms == expected.commit_time_ms);
    assert(actual.payload == expected.payload && actual.presence_bitmap == expected.presence_bitmap);
    assert(actual.chunk_columns && expected.chunk_columns && actual.chunk_columns->bytes == expected.chunk_columns->bytes);
}

void Verify(const std::filesystem::path& root) {
    std::ostringstream diagnostics;
    const auto result = VerifyDataDirectory(root, diagnostics);
    if (result.errors != 0U) std::cerr << diagnostics.str();
    assert(result.errors == 0U);
}

void Archive(TableCatalog& catalog, const std::filesystem::path& table_root, const StoreId& epoch,
    std::uint64_t initialized, std::uint64_t populated, std::uint64_t deleted) {
    const auto table = catalog.Require("t");
    FeedSlotTestAccess::Sync(*table);
    auto reader = table->ReadFeedArchive({epoch, 0U});
    bool saw_initial = false, saw_population = false, saw_deleted = false;
    while (const auto entry = reader.Next()) {
        assert(entry->dims == 3U);
        if (entry->position.revision == initialized) {
            assert(entry->blocks.empty() && entry->chunk_changes.size() == 1U);
            const auto& row = entry->chunk_changes.front();
            assert(row.chunk == kLive && !row.before && row.after);
            assert(std::get<std::uint64_t>(row.after->front()) == 0U);
            saw_initial = true;
        }
        if (entry->position.revision == populated) {
            assert(entry->blocks.size() == 1U);
            const auto& block = entry->blocks.front();
            assert(block.chunk == kLive && block.x == -1 && block.y == -1 && block.z == -1);
            assert(block.local_x == 1U && block.local_y == 1U && block.local_z == 1U);
            assert(!block.before && block.after && std::get<std::uint64_t>(block.after->front()) == 7U);
            saw_population = true;
        }
        if (entry->position.revision == deleted) {
            assert(entry->blocks.empty() && entry->chunk_changes.size() == 1U);
            assert(entry->chunk_changes.front().chunk == kDeleted);
            saw_deleted = true;
        }
    }
    assert(saw_initial && saw_population && saw_deleted);
    bool retained_base = false;
    for (const auto& file : std::filesystem::directory_iterator(table_root / kFeedArchiveDirName)) {
        ChunkCoord coord;
        std::uint64_t first = 0U;
        if (!ParseArchiveBaseFileName(file.path().filename().string(), 3U, &coord, &first)) continue;
        const auto bytes = LoadFile(file.path());
        assert(bytes.size() == 124U);
        const auto descriptor = ParseArchiveBaseDescriptor(bytes, 3U, epoch, coord, first, kMaxChunkImageFileBytes);
        if (coord == kLive && first == populated) {
            assert(descriptor.kind == ArchiveBaseKind::kRetainedEmpty && descriptor.base_revision == initialized);
            retained_base = true;
        }
    }
    assert(retained_base);
}

void Persistence(DurabilityMode mode, bool required) {
    test::ScopedTempDir temporary("chunkdb-persistence-3d");
    const auto source = temporary.path() / "source";
    const auto backup = temporary.path() / "backup";
    const auto restored = temporary.path() / "restored";
    const auto config = Config(source, mode);
    const auto table_root = source / "tables/t";
    StoreId epoch{};
    ChunkState live, empty;
    std::uint64_t initialized = 0U, populated = 0U, deleted = 0U, cut = 0U;
    {
        auto catalog = std::make_shared<TableCatalog>(config);
        Commands commands(catalog);
        assert(commands.Run(std::string("CREATE TABLE t (value u8 DEFAULT 0) CHUNK 2 x 2 x 2 LARGE 2 x 2 x 2 CHUNK COLUMNS (owner u8 ") +
            (required ? "REQUIRED)" : "DEFAULT 0)")) == "+OK\r\n");
        const FeedSlotFilters filters{.area = FeedArea{{-2, -1, -3}, {-1, 1, -1}, 3U}};
        epoch = catalog->Require("t")->store_id();
        (void)catalog->Require("t")->CreateFeedSlot("consumer", false, filters);
        initialized = commands.Write("SET CHUNK -1 -1 -1 IN t META owner=0");
        const auto baseline = State(*catalog, kLive);
        assert(baseline.version == initialized && baseline.written && baseline.commit_time_ms != 0U);
        assert(!std::filesystem::exists(ChunkDataPath(table_root, catalog->Require("t")->geometry(), kLive)));
        populated = commands.Write("SET BLOCK -1 -1 -1 IN t value=7");
        live = State(*catalog, kLive);
        assert(live.version == populated && live.commit_time_ms != 0U && live.presence_bitmap == std::vector<std::uint8_t>{128U});
        assert(live.payload[7U] == 7U && live.chunk_columns);
        assert(std::get<std::uint64_t>(DecodeChunkColumns(catalog->Require("t")->geometry().chunk_schema(), live.chunk_columns->bytes).front()) == 0U);
        const auto image = ChunkDataPath(table_root, catalog->Require("t")->geometry(), kLive);
        assert(image.filename() == "C_-1_-1_-1.chk" && image.parent_path().filename() == "L_-1_-1_-1");
        assert(std::filesystem::is_regular_file(image));
        deleted = commands.Write("SET CHUNK -2 1 -3 IN t META owner=0");
        empty = State(*catalog, kDeleted);
        assert(empty.written && empty.version == deleted && empty.commit_time_ms != 0U && empty.presence_bitmap == std::vector<std::uint8_t>{0U});
        Archive(*catalog, table_root, epoch, initialized, populated, deleted);
        assert(catalog->CompactTombstones("t", initialized + 1U) == initialized);
        Identity(State(*catalog, kLive), live);
        Identity(State(*catalog, kDeleted), empty);
        const auto result = catalog->BackupTo(backup, {});
        assert(result.tables.size() == 1U && result.tables.front().epoch == epoch);
        cut = result.tables.front().revision;
        assert(cut >= deleted);
        Verify(backup);
    }
    {
        TableCatalog reopened(config);
        Identity(State(reopened, kLive), live);
        Identity(State(reopened, kDeleted), empty);
        auto lease = reopened.Require("t")->Acquire();
        assert(lease->store().RetainedDeletionFloor() == initialized && lease->store().RetainedTombstoneCount() == 1U);
        const auto page = lease->store().ScanChunkIdentities(false, {}, 10U, initialized, true);
        assert(page.entries.size() == 2U && page.entries.front().coord == kDeleted && !page.entries.front().data);
        assert(page.entries.back().coord == kLive && page.entries.back().data);
    }
    Verify(source);
    RestoreBackup(backup, restored);
    Verify(restored);
    const auto manifest = *ReadStoreManifest(restored / "tables/t");
    assert(manifest.geometry.dims == 3U && manifest.store_id != epoch && manifest.lineage);
    assert(manifest.lineage->parent_epoch == epoch && manifest.lineage->fork_revision == cut);
    const auto slots = *ReadFeedSlotRecords(restored / "tables/t", manifest.store_id, 3U);
    assert(slots.dims == 3U && slots.slots.size() == 1U && slots.slots.front().written == cut);
    assert(slots.slots.front().filters.area == std::optional<FeedArea>(FeedArea{{-2, -1, -3}, {-1, 1, -1}, 3U}));
    {
        auto restored_config = config; restored_config.data_dir = restored;
        auto catalog = std::make_shared<TableCatalog>(restored_config);
        Identity(State(*catalog, kLive), live);
        Identity(State(*catalog, kDeleted), empty);
        {
            auto lease = catalog->Require("t")->Acquire();
            assert(lease->store().RetainedDeletionFloor() == initialized);
        }
        Commands commands(catalog);
        const auto resurrected = commands.Write("SET BLOCK -3 2 -5 IN t value=9");
        assert(resurrected > cut);
        const auto state = State(*catalog, kDeleted);
        assert(state.version == resurrected && state.chunk_columns && state.presence_bitmap == std::vector<std::uint8_t>{32U});
        assert(std::get<std::uint64_t>(DecodeChunkColumns(catalog->Require("t")->geometry().chunk_schema(), state.chunk_columns->bytes).front()) == 0U);
    }
    Verify(restored);
}
}

int main() {
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal})
        for (const auto required : {false, true}) Persistence(mode, required);
    std::cout << "4 dimensional persistence combinations passed\n";
}
