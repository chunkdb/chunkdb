#pragma once
// Fixtures for RESTORE TABLE and SWAP TABLES tests (docs/design/RESTORE_TABLE.md).
#include <array>
#include <cassert>
#include <iostream>
#include <sstream>

#include "backup.hpp"
#include "checkpoint.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "verify.hpp"
#include "scram.hpp"
#include "user_registry.hpp"

namespace chunkdb::restore_table_test {

template <typename Exception = std::exception, typename Function>
std::string Throws(Function&& function) {
    try { function(); } catch (const Exception& error) { return error.what(); }
    assert(false && "expected failure");
    return {};
}

inline std::size_t Entries(const std::filesystem::path& directory) {
    if (!std::filesystem::exists(directory)) return 0U;
    return static_cast<std::size_t>(std::distance(std::filesystem::directory_iterator(directory), {}));
}

// The users of `data`; the first open creates them with `admin`.
inline std::shared_ptr<UserRegistry> OpenUsers(const std::filesystem::path& data) {
    return std::make_shared<UserRegistry>(data, std::make_pair(std::string("admin"),
        scram::MakeVerifier("secret", crypto::RandomBytes(16), scram::kMinIterations)), std::array<std::uint8_t, 32>{7});
}

// The right `admin` holds on `table` by name.
inline std::optional<Right> GrantOn(const std::filesystem::path& data, const std::string& table) {
    const auto users = ReadUsersFile(data);
    assert(users);
    const auto& grants = users->users.at("admin").grants;
    const auto found = grants.find(table);
    return found == grants.end() ? std::nullopt : std::optional<Right>(found->second);
}

inline void Verified(const std::filesystem::path& data_dir) {
    std::ostringstream findings;
    const auto counters = VerifyDataDirectory(data_dir, findings);
    if (counters.errors != 0U) std::cerr << findings.str();
    assert(counters.errors == 0U);
}

// A table `t` with a slot, a checkpointed image, a WAL-only chunk and
// tombstones in two large chunks, backed up to `backup`; a later write is
// not in the backup.
struct Source {
    test::ScopedTempDir temporary{"chunkdb-restore-table"};
    std::filesystem::path root = std::filesystem::canonical(temporary.path());
    std::filesystem::path data = root / "data", backup = root / "snap";
    DurabilityMode mode;
    StoreId epoch{};
    std::uint64_t cut = 0U, live = 0U, wal_only = 0U;
    std::array<std::uint64_t, 2U> deleted{};

    explicit Source(DurabilityMode durability = DurabilityMode::kRelaxed) : mode(durability) {
        TableCatalog catalog(Config());
        GeometryConfig geometry;
        geometry.large_chunk_width_chunks = 2U; geometry.large_chunk_height_chunks = 2U;
        geometry.chunk_width_blocks = 2U; geometry.chunk_height_blocks = 2U; geometry.block_bits = 8U;
        const auto table = catalog.Create("t", geometry, catalog.default_options());
        epoch = table->store_id();
        (void)table->CreateFeedSlot("consumer");
        {
            auto lease = table->Acquire();
            auto& store = lease->store();
            store.SetBlockBits(8, 0, "00000011"); live = store.GetChunkVersion(4, 0);
            store.CheckpointForTests(4, 0);
            for (std::size_t i = 0; i < deleted.size(); ++i) {
                const auto x = static_cast<std::int64_t>(4U * i);
                store.SetBlockBits(x, 0, "10000000"); store.UnsetBlock(x, 0);
                deleted[i] = store.GetChunkVersion(2 * static_cast<std::int64_t>(i), 0);
                store.CheckpointForTests(2 * static_cast<std::int64_t>(i), 0);
            }
            store.SetBlockBits(9, 3, "00000101"); wal_only = store.GetChunkVersion(4, 1);
            assert(store.RetainedTombstoneCount() == 2U);
            store.WalBarrier();
        }
        const auto result = catalog.BackupTo(backup, {});
        assert(result.tables.size() == 1U && result.tables.front().epoch == epoch);
        cut = result.tables.front().revision;
        auto lease = table->Acquire();
        lease->store().SetBlockBits(8, 0, "11111111");
        lease->store().WalBarrier();
    }
    [[nodiscard]] CatalogConfig Config() const {
        CatalogConfig config;
        config.data_dir = data;
        config.background_maintenance = false;
        config.default_options.durability_mode = mode;
        return config;
    }
    [[nodiscard]] TableRestoreRequest Request(std::string name = "t_restored") const {
        return TableRestoreRequest{.backup = backup, .table = "t", .name = std::move(name)};
    }
    // The restored table holds the backup cut under a new epoch whose lineage
    // is the backed-up epoch at the cut; the live table keeps its later write.
    void CheckRestored(TableCatalog& catalog, const std::string& name, const StoreId& restored) const {
        assert(restored != epoch);
        const auto table = catalog.Require(name);
        assert(table->store_id() == restored);
        assert(table->lineage() && *table->lineage() == (TableLineage{epoch, cut}));
        {
            auto lease = table->Acquire();
            auto& store = lease->store();
            assert(store.GetBlockBits(8, 0) == "00000011" && store.GetChunkVersion(4, 0) == live);
            assert(store.GetBlockBits(9, 3) == "00000101" && store.GetChunkVersion(4, 1) == wal_only);
            assert(store.RetainedTombstoneCount() == 2U);
            for (std::size_t i = 0; i < deleted.size(); ++i) {
                const auto state = store.ReadChunkState(2 * static_cast<std::int64_t>(i), 0);
                assert(state.written && state.version == deleted[i]);
            }
            assert(store.CompletedRevision() && *store.CompletedRevision() >= cut);
        }
        const auto slots = table->ListFeedSlots();
        assert(slots.size() == 1U && slots[0].name == "consumer" && slots[0].position == (FeedPosition{restored, cut}));
        auto live_table = catalog.Require("t");
        assert(live_table->store_id() == epoch);
        assert(live_table->Acquire()->store().GetBlockBits(8, 0) == "11111111");
    }
};

}  // namespace chunkdb::restore_table_test
