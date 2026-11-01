#include <array>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>

#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "test_utils.hpp"
#include "txn_test_utils.hpp"

namespace {
using namespace chunkdb;

CatalogConfig Config(const std::filesystem::path& path, QueryLimits limits) {
    CatalogConfig config;
    config.data_dir = path;
    config.background_maintenance = false;
    config.query_limits = limits;
    config.default_options.durability_mode = DurabilityMode::kRelaxed;
    return config;
}

template <typename Action> void Invalid(Action action) {
    bool refused = false;
    try { action(); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}

void DefaultsAndEarlyRefusal() {
    const QueryLimits defaults;
    assert(defaults.max_area_chunks == 4096U && defaults.max_area_response_bytes == 64U * 1024U * 1024U &&
        defaults.txn_max_written_chunks == 1024U && defaults.txn_max_read_chunks == 8192U);
    const std::array<std::size_t QueryLimits::*, 4> members{&QueryLimits::max_area_chunks,
        &QueryLimits::max_area_response_bytes, &QueryLimits::txn_max_written_chunks, &QueryLimits::txn_max_read_chunks};
    for (const auto member : members) {
        test::ScopedTempDir directory("chunkdb-query-limit-refusal");
        auto limits = defaults;
        limits.*member = 0U;
        const auto store_path = directory.path() / "direct";
        auto store_config = txn_test::Config(store_path, DurabilityMode::kRelaxed);
        store_config.query_limits = limits;
        Invalid([&] { ChunkStore store(store_config); });
        assert(!std::filesystem::exists(store_path));
        const auto catalog_path = directory.path() / "catalog";
        Invalid([&] { TableCatalog catalog(Config(catalog_path, limits)); });
        assert(!std::filesystem::exists(catalog_path));
    }
}

void PropagationAndReopen() {
    test::ScopedTempDir directory("chunkdb-query-limit-propagation");
    const QueryLimits limits{.max_area_chunks = 17U, .max_area_response_bytes = 12345U,
        .txn_max_written_chunks = 3U, .txn_max_read_chunks = 5U};
    {
        auto catalog = std::make_shared<TableCatalog>(Config(directory.path(), limits));
        assert(catalog->query_limits() == limits);
        auto table = catalog->Create("t", {2U, 2U, 1U, 1U, 8U}, catalog->default_options());
        { auto lease = table->Acquire(); assert(lease->store().query_limits() == limits); }
        TableOptionsUpdate update;
        update.checkpoint_update_interval = 7U;
        catalog->SetOptions("t", update);
        { auto lease = table->Acquire(); assert(lease->store().query_limits() == limits); }
        CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
        SessionState session;
        const auto hello = engine.Execute(session, "HELLO 3\r\n");
        for (const auto& [name, value] : {std::pair{"max_area_chunks", "17"},
            {"max_response_bytes", "12345"}, {"max_area_response_bytes", "12345"},
            {"txn_max_written_chunks", "3"}, {"txn_max_read_chunks", "5"}}) {
            const auto field = "$" + std::to_string(std::string_view(name).size()) + "\r\n" + name + "\r\n:" + value + "\r\n";
            assert(hello.find(field) != std::string::npos);
        }
        const auto field = "$15\r\nmax_area_chunks\r\n:17\r\n";
        assert(hello.find(field) != hello.rfind(field));
    }
    TableCatalog reopened(Config(directory.path(), limits));
    auto lease = reopened.Require("t")->Acquire();
    assert(lease->store().query_limits() == limits);
    auto direct = txn_test::Config(directory.path() / "direct", DurabilityMode::kRelaxed);
    direct.query_limits = limits;
    assert(CatalogConfigFromStoreConfig(direct).query_limits == limits);
    ChunkStore store(direct);
    assert(store.query_limits() == limits);
}

void DirectTransactionAdmission() {
    test::ScopedTempDir directory("chunkdb-query-txn-limits");
    auto config = txn_test::Config(directory.path(), DurabilityMode::kRelaxed);
    config.query_limits.txn_max_written_chunks = 1U;
    config.query_limits.txn_max_read_chunks = 2U;
    config.query_limits.max_area_chunks = 2U;
    ChunkStore store(config);
    auto snapshot = store.BeginTxnSnapshot(std::chrono::seconds(30));
    auto first = store.ReadChunkStateAt(*snapshot, 0, 0);
    txn_test::SetCounter(&first, 3U);
    auto second = store.ReadChunkStateAt(*snapshot, 1, 0);
    txn_test::SetCounter(&second, 4U);
    Invalid([&] { (void)store.CommitTransaction(*snapshot, {}, {{{0, 0}, first}, {{1, 0}, second}}); });
    Invalid([&] { (void)store.CommitTransaction(*snapshot, {{0, 0}, {1, 0}, {2, 0}}, {}); });
    Invalid([&] { (void)store.ReadChunkRangeAt(*snapshot, 0, 0, 2, 0); });
    Invalid([&] { (void)store.ReadChunkRadiusAt(*snapshot, 0, 0, 1); });
    assert(store.TxnRegisteredCountForTests() == 1U);
    assert(store.ReadChunkState(0, 0).version == 0U && store.ReadChunkState(1, 0).version == 0U);
    const auto revision = store.CommitTransaction(*snapshot, {{0, 0}, {1, 0}, {1, 0}}, {{{0, 0}, first}});
    assert(revision != 0U && txn_test::ReadCounter(store, {0, 0}) == 3U && txn_test::ReadCounter(store, {1, 0}) == 0U);
    assert(store.TxnRegisteredCountForTests() == 0U);
}
}  // namespace

int main() {
    DefaultsAndEarlyRefusal();
    PropagationAndReopen();
    DirectTransactionAdmission();
}
