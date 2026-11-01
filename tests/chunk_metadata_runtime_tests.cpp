#include <cassert>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>

#include "chunk_store_internal.hpp"
#include "checkpoint.hpp"
#include "chunkdb/chunk_store.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;

void Reject(const std::function<void()>& call) {
    bool refused = false;
    try { call(); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}
void Failpoint(const char* name, const char* value) {
#ifdef _WIN32
    _putenv_s(name, value);
#else
    if (*value == '\0') unsetenv(name); else setenv(name, value, 1);
#endif
}
TableSchema MetadataSchema() {
    TableSchema schema;
    schema.next_column_id = 3;
    schema.columns = {
        {.id = 1, .name = "owner", .type = {ColumnKind::kUnsigned, 8}, .required = true, .default_value = {}},
        {.id = 2, .name = "label", .type = {ColumnKind::kText, 128}, .nullable = true, .default_value = {}},
    };
    return schema;
}
StoreConfig Config(const std::filesystem::path& directory, bool variable_blocks = false) {
    StoreConfig config;
    config.data_dir = directory;
    config.geometry.chunk_width_blocks = 4;
    config.geometry.chunk_height_blocks = 4;
    config.geometry.block_bits = 8;
    config.chunk_schema = MetadataSchema();
    config.background_maintenance = false;
    config.max_loaded_chunks = 1;
    config.checkpoint_update_interval = 1000;
    config.wal_group_commit_updates = 1;
    config.durability_mode = DurabilityMode::kFsyncWal;
    if (variable_blocks) {
        config.schema = SingleBitsColumnSchema(8);
        config.schema->columns.push_back(Column{.id = 2, .name = "data", .type = {ColumnKind::kText, 128},
            .nullable = true, .default_value = {}});
        config.schema->next_column_id = 3;
    }
    return config;
}
ChunkState Changed(ChunkStore& store, ChunkCoord coord, std::uint64_t owner, std::optional<std::string> label) {
    auto state = store.ReadChunkState(coord);
    state.chunk_columns = std::make_shared<const ChunkColumnState>(AssignChunkColumns(
        store.geometry().chunk_schema(), state.chunk_columns.get(),
        {{"owner", owner}, {"label", label ? ColumnValue(*label) : ColumnValue(std::monostate{})}}, !state.written));
    return state;
}
void ExpectMetadata(const ChunkState& state, std::uint64_t owner, std::optional<std::string> label) {
    assert(state.chunk_columns);
    const auto values = DecodeChunkColumns(MetadataSchema(), state.chunk_columns->bytes);
    assert(std::get<std::uint64_t>(values[0]) == owner);
    if (label) assert(std::get<std::string>(values[1]) == *label);
    else assert(std::holds_alternative<std::monostate>(values[1]));
}

void MetadataSurvivesWalImageEvictionAndRefresh() {
    test::ScopedTempDir directory("chunkdb-metadata-runtime");
    const auto config = Config(directory.path());
    std::uint64_t final_version = 0, final_time = 0;
    {
        ChunkStore store(config);
        Reject([&] { store.SetBlockBits(0, 0, "10000000"); });
        assert(!store.ReadChunkState(0, 0).written);
        const auto initialized = store.WriteChunkState({0, 0}, Changed(store, {0, 0}, 0, {}), {});
        auto state = store.ReadChunkState(0, 0);
        assert(initialized.ok && initialized.version != 0 && state.written && state.commit_time_ms != 0);
        assert(!ChunkPopulated(store.geometry(), state.presence_bitmap, state.chunk_columns.get()));
        const auto put = store.WriteChunkState({0, 0}, Changed(store, {0, 0}, 7, "stored"), {}, true);
        assert(put.ok && put.version > initialized.version);
        const auto refused = store.WriteChunkState({0, 0}, Changed(store, {0, 0}, 9, "ignored"), {}, true);
        assert(!refused.ok && refused.version == put.version);
        state = store.ReadChunkState(0, 0);
        const auto saved_version = state.version, saved_time = state.commit_time_ms;
        ExpectMetadata(state, 7, "stored");
        if (kFailpointsCompiled) {
            auto rejected = Changed(store, {0, 0}, 8, "rollback");
            Failpoint("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE", "1");
            bool injected = false;
            try { (void)store.WriteChunkState({0, 0}, std::move(rejected), saved_version); }
            catch (const std::runtime_error&) { injected = true; }
            Failpoint("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE", "");
            assert(injected);
            state = store.ReadChunkState(0, 0);
            assert(state.version == saved_version && state.commit_time_ms == saved_time);
            ExpectMetadata(state, 7, "stored");
        }
        store.CheckpointForTests(0, 0);
        (void)store.ReadChunkState(2, 0);
        ExpectMetadata(store.ReadChunkState(0, 0), 7, "stored");
        const auto area = store.ReadChunkRange(0, 0, 0, 0, true);
        assert(area.size() == 1 && area.front().chunk_columns && area.front().commit_time_ms == saved_time);
        auto readonly = config;
        readonly.access_mode = AccessMode::kReadOnly;
        ChunkStore reader(readonly);
        ExpectMetadata(reader.ReadChunkState(0, 0), 7, "stored");
        auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(30));
        assert(store.WriteChunkState({0, 0}, Changed(store, {0, 0}, 10, "updated"), {}).ok);
        ExpectMetadata(store.ReadChunkStateAt(*snapshot, ChunkCoord{0, 0}), 7, "stored");
        ExpectMetadata(reader.ReadChunkState(0, 0), 10, "updated");
        store.EndTxnSnapshot(*snapshot);
        const auto final = store.ReadChunkState(0, 0);
        final_version = final.version;
        final_time = final.commit_time_ms;
    }
    ChunkStore recovered(config);
    const auto state = recovered.ReadChunkState(0, 0);
    assert(state.version == final_version && state.commit_time_ms == final_time);
    ExpectMetadata(state, 10, "updated");
}

void CombinedQuotaAllowsShrinkingAfterLimitReduction() {
    test::ScopedTempDir directory("chunkdb-metadata-quota");
    auto config = Config(directory.path(), true);
    config.var_max_chunk_bytes = 40;
    {
        ChunkStore store(config);
        assert(store.WriteChunkState({0, 0}, Changed(store, {0, 0}, 1, "meta"), {}).ok);
        store.SetBlock(0, 0, {{"data", std::string("12345678")}});
        Reject([&] { store.SetBlock(0, 0, {{"data", std::string("1234567890123456")}}); });
        Reject([&] { (void)store.WriteChunkState({0, 0}, Changed(store, {0, 0}, 1, "123456789"), {}); });
    }
    config.var_max_chunk_bytes = 20;
    ChunkStore store(config);
    ExpectMetadata(store.ReadChunkState(0, 0), 1, "meta");
    store.SetBlock(0, 0, {{"data", std::string("x")}});
    assert(store.WriteChunkState({0, 0}, Changed(store, {0, 0}, 1, std::string{}), {}).ok);
    Reject([&] { (void)store.WriteChunkState({0, 0}, Changed(store, {0, 0}, 1, "x"), {}); });
    ExpectMetadata(store.ReadChunkState(0, 0), 1, std::string{});
}

void TransactionPublishesMetadataRowsTogether() {
    test::ScopedTempDir directory("chunkdb-metadata-transaction");
    auto config = Config(directory.path());
    config.max_loaded_chunks = 16;
    ChunkStore store(config);
    assert(store.WriteChunkState({0, 0}, Changed(store, {0, 0}, 1, "first"), {}).ok);
    assert(store.WriteChunkState({1, 0}, Changed(store, {1, 0}, 2, "second"), {}).ok);
    auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(30));
    const auto version = store.CommitTransaction(*snapshot, {}, {
        {{0, 0}, Changed(store, {0, 0}, 3, "after first")},
        {{1, 0}, Changed(store, {1, 0}, 4, "after second")},
    });
    const auto first = store.ReadChunkState(0, 0), second = store.ReadChunkState(1, 0);
    assert(version != 0 && first.version == version && second.version == version);
    assert(first.commit_time_ms != 0 && first.commit_time_ms == second.commit_time_ms);
    ExpectMetadata(first, 3, "after first");
    ExpectMetadata(second, 4, "after second");
}

void ColdPopulationUsesTheTranslatedMetadataBaseline() {
    for (const bool add_default : {false, true}) {
        test::ScopedTempDir directory("chunkdb-cold-metadata-baseline");
        auto config = Config(directory.path());
        {
            ChunkStore store(config);
            assert(store.WriteChunkState({0, 0}, Changed(store, {0, 0}, 7U, {}), {}).ok);
            store.CheckpointForTests(0, 0);
        }
        auto manifest = *ReadStoreManifest(directory.path());
        manifest.chunk_schema = DropChunkColumn(*manifest.chunk_schema, "owner");
        manifest.schema = AdvanceOtherDomain(manifest.schema, ColumnDomain::kBlock);
        if (add_default) {
            Column score;
            score.name = "score";
            score.type = {ColumnKind::kUnsigned, 8U};
            score.has_default = true;
            score.default_value = EncodeColumnValue(score, ColumnValue{std::uint64_t{5U}});
            manifest.chunk_schema = AddChunkColumn(*manifest.chunk_schema, score);
            manifest.schema = AdvanceOtherDomain(manifest.schema, ColumnDomain::kBlock);
        }
        AtomicWrite(StoreManifestPath(directory.path()), SerializeStoreManifest(manifest), true, true);
        config.schema = manifest.schema;
        config.chunk_schema = manifest.chunk_schema;
        ChunkStore store(config);
        assert(!store.IsChunkLoadedForTests(0, 0));
        assert(store.ScanPopulatedChunks(false, {}, 10U).coords.empty());
        assert(store.ReadChunkRange(0, 0, 0, 0, true).empty());
        assert(!store.IsChunkLoadedForTests(0, 0));
        const auto state = store.ReadChunkState(0, 0);
        assert(state.written && !ChunkPopulated(store.geometry(), state.presence_bitmap, state.chunk_columns.get()));
        assert(store.ScanPopulatedChunks(false, {}, 10U).coords.empty());
    }
}
}

int main() {
    MetadataSurvivesWalImageEvictionAndRefresh();
    CombinedQuotaAllowsShrinkingAfterLimitReduction();
    TransactionPublishesMetadataRowsTogether();
    ColdPopulationUsesTheTranslatedMetadataBaseline();
}
