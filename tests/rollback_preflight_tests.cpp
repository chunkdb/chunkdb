#include <cassert>
#include <functional>
#include <fstream>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "txn_history.hpp"
#include "txn_intent_format.hpp"
#include "txn_test_utils.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
void Reject(const std::function<void()>& action) {
    bool refused = false;
    try { action(); } catch (const std::exception&) { refused = true; }
    assert(refused);
}
Bytes Conditional(bool committed, std::uint64_t boundary) {
    Bytes bytes{'C', 'K', 'R', static_cast<std::uint8_t>(committed ? 'C' : 'B')};
    WriteLe64(bytes, boundary);
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    return bytes;
}
struct Fixture {
    test::ScopedTempDir directory{"chunkdb-rollback-preflight"};
    StoreConfig config = txn_test::Config(directory.path(), DurabilityMode::kFsyncWal, 8U);
    StoreId id{};
    Geometry geometry{config.geometry};
    std::filesystem::path wal, intent;
    Bytes original;
    Fixture() {
        config.background_maintenance = false;
        config.checkpoint_update_interval = 1000U;
        {
            ChunkStore store(config);
            id = store.store_id();
            store.SetBlockBits(0, 0, "10000000");
            wal = ChunkWalPath(directory.path(), geometry, {0, 0});
        }
        original = LoadFile(wal);
        intent = ConditionalIntentDirectory(directory.path()) / CanonicalConditionalIntentFileName(geometry, {0, 0});
        std::filesystem::create_directories(intent.parent_path());
    }
    void CheckUnchanged(const std::map<std::filesystem::path, Bytes>& files) {
        Reject([&] { ChunkStore refused(config); });
        for (const auto& [path, bytes] : files) assert(LoadFile(path) == bytes);
        assert(LoadFile(wal) == original);
    }
};
void AdmissionPrecedesEveryCleanup() {
    for (bool committed : {false, true}) {
        Fixture fixture;
        const auto valid = Conditional(committed, ChunkWalHeaderBytes(2U));
        AtomicWrite(fixture.intent, valid, true, true);
        const auto malformed = fixture.intent.parent_path() / "L_0_0__C_1_0.wal.rollback";
        const Bytes bad{'C', 'K', 'R', 'B'};
        AtomicWrite(malformed, bad, true, true);
        fixture.CheckUnchanged({{fixture.intent, valid}, {malformed, bad}});
    }
    Fixture fixture;
    const auto committed = Conditional(true, fixture.original.size());
    AtomicWrite(fixture.intent, committed, true, true);
    // The inactive z coordinate and wrong large-shard placement both refuse.
    for (const auto& name : {"L_0_0_0__C_1_0_0.wal.rollback", "L_1_0__C_1_0.wal.rollback"}) {
        const auto malformed = fixture.intent.parent_path() / name;
        AtomicWrite(malformed, committed, true, true);
        fixture.CheckUnchanged({{fixture.intent, committed}, {malformed, committed}});
        std::filesystem::remove(malformed);
    }
}
void PendingCutsMustAgreeAcrossDecisionKinds() {
    for (const auto conditional_cut : {static_cast<std::uint64_t>(ChunkWalHeaderBytes(2U)), std::uint64_t{0U}}) {
        Fixture fixture;
        const auto conditional = Conditional(false, conditional_cut);
        AtomicWrite(fixture.intent, conditional, true, true);
        const auto transaction_path = TxnIntentPath(fixture.directory.path(), 100U);
        const auto transaction = SerializeTxnIntent({TxnIntentState::kRollback, 100U,
            {{{0, 0}, fixture.original.size()}}}, 2U, fixture.id);
        AtomicWrite(transaction_path, transaction, true, true);
        {
            auto readonly = fixture.config; readonly.access_mode = AccessMode::kReadOnly;
            ChunkStore reader(readonly);
            Reject([&] { (void)reader.ReadChunkState(0, 0); });
        }
        fixture.CheckUnchanged({{fixture.intent, conditional}, {transaction_path, transaction}});
    }
    for (bool committed : {false, true}) {
        Fixture fixture;
        const auto conditional = Conditional(committed, committed ? 0U : fixture.original.size());
        AtomicWrite(fixture.intent, conditional, true, true);
        const auto transaction_path = TxnIntentPath(fixture.directory.path(), 100U);
        AtomicWrite(transaction_path, SerializeTxnIntent({TxnIntentState::kRollback, 100U,
            {{{0, 0}, fixture.original.size()}}}, 2U, fixture.id), true, true);
        ChunkStore recovered(fixture.config);
        assert(recovered.GetBlockBits(0, 0) == "10000000");
        assert(!std::filesystem::exists(fixture.intent) && !std::filesystem::exists(transaction_path));
    }
}
void ConflictingTransactionsAndBoundedRecords() {
    Fixture fixture;
    const auto first = TxnIntentPath(fixture.directory.path(), 100U);
    const auto second = TxnIntentPath(fixture.directory.path(), 101U);
    const auto first_bytes = SerializeTxnIntent({TxnIntentState::kRollback, 100U,
        {{{0, 0}, fixture.original.size()}}}, 2U, fixture.id);
    const auto second_bytes = SerializeTxnIntent({TxnIntentState::kRollback, 101U,
        {{{0, 0}, ChunkWalHeaderBytes(2U)}}}, 2U, fixture.id);
    AtomicWrite(first, first_bytes, true, true);
    AtomicWrite(second, second_bytes, true, true);
    {
        auto readonly = fixture.config; readonly.access_mode = AccessMode::kReadOnly;
        ChunkStore reader(readonly);
        Reject([&] { (void)reader.ReadChunkState(0, 0); });
    }
    fixture.CheckUnchanged({{first, first_bytes}, {second, second_bytes}});
    std::filesystem::remove(first);
    std::filesystem::remove(second);
    {
        std::ofstream output(first, std::ios::binary);
        output.seekp(4U * 1024U * 1024U);
        output.put('\0');
        output.close();
        assert(output);
    }
    Reject([&] { (void)LoadRecoveryTxnIntent(first); });
    assert(std::filesystem::file_size(first) == 4U * 1024U * 1024U + 1U);
    Reject([&] { ChunkStore refused(fixture.config); });
    assert(LoadFile(fixture.wal) == fixture.original);
}
void RollbackPrefixUsesRetainedRequiredMetadata() {
    test::ScopedTempDir directory("chunkdb-rollback-retained-base");
    auto config = txn_test::Config(directory.path(), DurabilityMode::kFsyncWal, 8U);
    config.background_maintenance = false;
    config.checkpoint_update_interval = 1000U;
    config.chunk_schema = TableSchema{.next_column_id = 2U, .columns = {
        {.id = 1U, .name = "owner", .type = {ColumnKind::kUnsigned, 8U}, .required = true, .default_value = {}}}};
    std::filesystem::path wal;
    StoreId id{};
    std::uint64_t revision = 0U;
    {
        ChunkStore store(config);
        id = store.store_id();
        auto state = store.ReadChunkState(0, 0);
        state.chunk_columns = std::make_shared<const ChunkColumnState>(AssignChunkColumns(
            store.geometry().chunk_schema(), nullptr, {{"owner", std::uint64_t{0U}}}, true));
        assert(store.WriteChunkState({0, 0}, std::move(state), {}).ok);
        store.CheckpointForTests(0, 0);
        store.SetBlockBits(0, 0, "10000000");
        revision = store.GetChunkVersion(0, 0);
        wal = ChunkWalPath(directory.path(), store.geometry(), {0, 0});
    }
    const auto original = LoadFile(wal);
    const auto transaction = TxnIntentPath(directory.path(), 100U);
    AtomicWrite(transaction, SerializeTxnIntent({TxnIntentState::kRollback, 100U,
        {{{0, 0}, original.size()}}}, 2U, id), true, true);
    ChunkStore store(config);
    assert(store.GetChunkVersion(0, 0) == revision && store.GetBlockBits(0, 0) == "10000000");
    assert(!std::filesystem::exists(transaction));
    assert(LoadFile(wal) == original);
}
}
int main() {
    AdmissionPrecedesEveryCleanup();
    PendingCutsMustAgreeAcrossDecisionKinds();
    RollbackPrefixUsesRetainedRequiredMetadata();
    ConflictingTransactionsAndBoundedRecords();
}
