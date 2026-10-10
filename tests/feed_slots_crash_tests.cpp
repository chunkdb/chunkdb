// Abrupt exits at each archive publication step preserve both current state
// and the historical rows, before and after a subsequent checkpoint retry.
#include <array>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <map>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "chunkdb/file_layout.hpp"
#include "chunk_store_internal.hpp"
#include "feed_slots.hpp"
#include "feed_test_utils.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::feed_test;

constexpr int kCrashExit = 86;
constexpr std::array<const char*, 4U> kPoints{
    "CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_WAL_FLUSH_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_BASE_LINK_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_IMAGE_PUBLISH_ONCE",
    "CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_WAL_RENAME_ONCE"};
constexpr std::array<DurabilityMode, 3U> kModes{
    DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal, DurabilityMode::kFsyncCheckpoint};

CatalogConfig CrashConfig(const std::filesystem::path& path, DurabilityMode mode) {
    auto config = Config(path);
    config.default_options.durability_mode = mode;
    config.default_options.wal_group_commit_updates = 1000U;
    config.slot_sync_interval = std::chrono::hours(1);
    return config;
}

int RunChild(const std::string& executable, const std::vector<std::string>& arguments) {
    // The same subprocess convention used by the transaction crash tests;
    // these arguments are test-generated paths and fixed mode/point names.
    std::string command = "\"" + executable + "\"";
    for (const auto& argument : arguments) command += " \"" + argument + "\"";
#ifdef _WIN32
    command = "\"" + command + "\"";
    return std::system(command.c_str());
#else
    const auto status = std::system(command.c_str());
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
#endif
}

int Crash(const std::filesystem::path& path, DurabilityMode mode, const char* point, bool empty) {
    TableCatalog catalog(CrashConfig(path, mode));
    auto table = catalog.Find("default");
    auto lease = table->Acquire();
    auto& store = lease->store();
    ScopedWriteUser identity("alice");
    store.SetBlockBits(0, 0, Bits(11U));
    store.SetBlockBits(0, 0, Bits(22U));
    if (empty) assert(store.UnsetBlock(0, 0, store.GetChunkVersion(0, 0)).ok);
    // Arm only the checkpoint, after all user mutations are staged.
    txn_test::ScopedEnv armed(point, "1");
    store.CheckpointForTests(0, 0);
    return 3;  // Reaching here means the claimed crash point was not reached.
}

using Row = std::pair<std::optional<std::uint32_t>, std::optional<std::uint32_t>>;

void Value(const std::optional<std::vector<ColumnValue>>& actual, std::optional<std::uint32_t> value) {
    assert(actual.has_value() == value.has_value());
    if (value) assert(*actual == std::vector<ColumnValue>{BitsValue{Bits(*value)}});
}

std::vector<FeedPosition> Rows(Table& table, FeedPosition start, const std::vector<Row>& expected) {
    auto reader = table.ReadFeedArchive(start);
    std::vector<FeedPosition> positions;
    auto previous = start.revision;
    for (const auto& [before, after] : expected) {
        const auto entry = reader.Next();
        assert(entry && entry->kind == FeedEntry::Kind::kChange && entry->schema_version == 1U);
        assert(entry->position.epoch == start.epoch && entry->position.revision > previous);
        assert(entry->user == "alice" && entry->blocks.size() == 1U);
        const auto& block = entry->blocks.front();
        assert((block.chunk == ChunkCoord{0, 0}) && block.local_x == 0U && block.local_y == 0U);
        assert(block.x == 0 && block.y == 0);
        Value(block.before, before);
        Value(block.after, after);
        previous = entry->position.revision;
        positions.push_back(entry->position);
    }
    assert(!reader.Next());  // No extra event for the collection maintenance frame.
    return positions;
}

std::map<std::filesystem::path, std::vector<std::uint8_t>> Bases(const std::filesystem::path& root) {
    std::map<std::filesystem::path, std::vector<std::uint8_t>> images;
    const auto directory = root / "tables" / "default" / kFeedArchiveDirName;
    if (std::filesystem::exists(directory)) for (const auto& item : std::filesystem::directory_iterator(directory))
        if (item.path().extension() == ".chk") images.emplace(item.path(), LoadFile(item.path()));
    return images;
}

void Case(const std::string& executable, DurabilityMode mode, bool base, bool empty, const char* point) {
    test::ScopedTempDir directory("chunkdb-feed-slot-crash");
    std::cout << DurabilityModeName(mode) << " base=" << base << " empty=" << empty << ' ' << point
              << " " << directory.path().string() << '\n' << std::flush;
    const auto config = CrashConfig(directory.path(), mode);
    FeedPosition start;
    {
        TableCatalog catalog(config);
        (void)feed_test::CreateDefault(catalog);
        auto table = catalog.Find("default");
        if (base) {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(0, 0, Bits(7U));
            lease->store().CheckpointForTests(0, 0);
        }
        start = table->CreateFeedSlot("consumer").position;
    }
    assert(RunChild(executable, {"--crash", directory.path().string(), DurabilityModeName(mode), point,
                                empty ? "empty" : "present"}) == kCrashExit);
    std::vector<Row> expected{{base ? std::optional<std::uint32_t>{7U} : std::nullopt, 11U}, {11U, 22U}};
    if (empty) expected.emplace_back(22U, std::nullopt);
    std::vector<FeedPosition> original;
    {
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        const auto slots = table->ListFeedSlots();
        assert(slots.size() == 1U && slots[0].position == start);
        {
            auto lease = table->Acquire();
            const auto state = lease->store().ReadChunkState(0, 0);
            assert(txn_test::CounterOf(state) == (empty ? 0U : 22U));
            assert(BlockPresent(state.presence_bitmap, 0U) != empty);
        }
        FeedSlotTestAccess::Sync(*table);
        // Read before retrying publication: this exercises the live WAL with
        // a newly published current image and the old linked base image.
        original = Rows(*table, start, expected);
        const auto bases = Bases(directory.path());
        {
            auto lease = table->Acquire();
            lease->store().CheckpointForTests(0, 0);
        }
        FeedSlotTestAccess::Sync(*table);
        assert(Rows(*table, start, expected) == original);
        for (const auto& [path, bytes] : bases) assert(std::filesystem::exists(path) && LoadFile(path) == bytes);
        {
            ScopedWriteUser identity("alice");
            auto lease = table->Acquire();
            lease->store().SetBlockBits(0, 0, Bits(33U));
        }
        FeedSlotTestAccess::Sync(*table);
    }
    expected.emplace_back(empty ? std::nullopt : std::optional<std::uint32_t>{22U}, 33U);
    {
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        const auto all = Rows(*table, start, expected);
        assert(std::equal(original.begin(), original.end(), all.begin()));
        auto lease = table->Acquire();
        assert(txn_test::CounterOf(lease->store().ReadChunkState(0, 0)) == 33U);
    }
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 6 && std::string_view(argv[1]) == "--crash")
        return Crash(argv[2], ParseDurabilityMode(argv[3]), argv[4], std::string_view(argv[5]) == "empty");
    assert(argc == 1);
    std::size_t count = 0U;
    for (const auto mode : kModes) for (const bool base : {false, true}) for (const bool empty : {false, true})
        for (const auto* point : kPoints) {
            Case(std::filesystem::absolute(argv[0]).string(), mode, base, empty, point);
            ++count;
        }
    std::cout << count << " archive crash scenarios passed\n";
}
