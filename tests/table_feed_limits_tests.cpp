#include <atomic>
#include <barrier>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <thread>

#include "chunkdb/table_catalog.hpp"
#include "change_feed.hpp"
#include "feed_slots.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;
using namespace std::chrono_literals;
using chunkdb::test::ScopedTempDir;

constexpr GeometryConfig kGeometry{
    .large_chunk_width_chunks = 2, .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 4, .chunk_height_blocks = 4, .block_bits = 32,
};

CatalogConfig Config(const std::filesystem::path& path) {
    CatalogConfig config;
    config.data_dir = path;
    config.feed_linger = 0ms;
    config.slot_sync_interval = 1h;
    config.default_options.checkpoint_update_interval = 100'000;
    config.default_options.checkpoint_wal_bytes = 1ULL << 30U;
    return config;
}

bool Throws(const std::function<void()>& action) {
    try { action(); }
    catch (const std::exception&) { return true; }
    return false;
}

class ScopedFailpoint {
  public:
    explicit ScopedFailpoint(const char* name) : name_(name) {
#ifdef _WIN32
        _putenv_s(name_, "1");
#else
        setenv(name_, "1", 1);
#endif
    }
    ~ScopedFailpoint() {
#ifdef _WIN32
        _putenv_s(name_, "");
#else
        unsetenv(name_);
#endif
    }
  private:
    const char* name_;
};

std::shared_ptr<const FeedEntry> Next(FeedSubscription& sub) {
    auto entry = sub.Next(10s);
    assert(entry);
    return entry;
}

void Write(Table& table, std::uint32_t value) {
    std::string bits(32, '0');
    for (std::size_t i = 0; i < bits.size(); ++i)
        if ((value & (1U << i)) != 0U) bits[i] = '1';
    auto lease = table.Acquire();
    assert(lease);
    lease->store().SetBlockBits(0, 0, bits);
}

void CodecAndValidation() {
    TableOptions raw;
    auto decoded = DecodeTableOptions(EncodeTableOptions(raw));
    assert(!decoded.feed_buffer_bytes && !decoded.slot_max_bytes);
    raw.feed_buffer_bytes = 4096;
    raw.slot_max_bytes = 8192;
    const auto bytes = EncodeTableOptions(raw);
    decoded = DecodeTableOptions(bytes);
    assert(decoded.feed_buffer_bytes == 4096U && decoded.slot_max_bytes == 8192U);
    auto repeated = bytes;
    // The six original entries occupy 58 bytes; type 8 follows them.
    repeated.insert(repeated.end(), bytes.begin() + 58, bytes.begin() + 70);
    assert(Throws([&] { (void)DecodeTableOptions(repeated); }));
    assert(Throws([&] { (void)DecodeTableOptions({8, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0}); }));
    assert(Throws([&] { (void)DecodeTableOptions({9, 0, 1, 0, 1}); }));
    ScopedTempDir dir("chunkdb-table-limits-invalid");
    auto config = Config(dir.path() / "invalid");
    config.slot_max_bytes = 0;
    assert(Throws([&] { TableCatalog catalog(config); }));
    assert(!std::filesystem::exists(config.data_dir));
}

void PersistenceAndInheritance() {
    ScopedTempDir dir("chunkdb-table-limits-options");
    auto config = Config(dir.path());
    config.feed_buffer_bytes = 7777;
    config.slot_max_bytes = 8888;
    {
        TableCatalog catalog(config);
        const auto inherited = catalog.Create("inherited", kGeometry, config.default_options);
        const auto info = inherited->Info();
        assert(!info.options.feed_buffer_bytes && !info.options.slot_max_bytes);
        assert(info.feed_buffer_bytes == 7777U && info.slot_max_bytes == 8888U);
        auto options = config.default_options;
        options.feed_buffer_bytes = 4096;
        options.slot_max_bytes = 8192;
        const auto explicit_table = catalog.Create("explicit", kGeometry, options);
        assert(explicit_table->Info().feed_buffer_bytes == 4096U);
        assert(explicit_table->Info().slot_max_bytes == 8192U);
        TableOptionsUpdate unrelated;
        unrelated.checkpoint_update_interval = 1000;
        catalog.SetOptions("inherited", unrelated);
        const auto manifest = ReadStoreManifest(dir.path() / "tables" / "inherited");
        assert(manifest);
        const auto raw = DecodeTableOptions(manifest->options);
        assert(!raw.feed_buffer_bytes && !raw.slot_max_bytes);
        assert(raw.checkpoint_update_interval == 1000U);
        const auto before = SerializeStoreManifest(*ReadStoreManifest(dir.path() / "tables" / "explicit"));
        TableOptionsUpdate invalid;
        invalid.feed_buffer_bytes = 0;
        assert(Throws([&] { catalog.SetOptions("explicit", invalid); }));
        invalid.feed_buffer_bytes.reset();
        invalid.slot_max_bytes = 0;
        assert(Throws([&] { catalog.SetOptions("explicit", invalid); }));
        assert(SerializeStoreManifest(*ReadStoreManifest(dir.path() / "tables" / "explicit")) == before);
        assert(explicit_table->Info().feed_buffer_bytes == 4096U);
        assert(explicit_table->Info().slot_max_bytes == 8192U);
        options.feed_buffer_bytes = 0;
        assert(Throws([&] { (void)catalog.Create("invalid", kGeometry, options); }));
        assert(!catalog.Find("invalid"));
    }
    config.feed_buffer_bytes = 9999;
    config.slot_max_bytes = 10000;
    {
        TableCatalog reopened(config);
        auto info = reopened.Find("inherited")->Info();
        assert(!info.options.feed_buffer_bytes && !info.options.slot_max_bytes);
        assert(info.feed_buffer_bytes == 9999U && info.slot_max_bytes == 10000U);
        info = reopened.Find("explicit")->Info();
        assert(info.options.feed_buffer_bytes == 4096U && info.options.slot_max_bytes == 8192U);
        assert(info.feed_buffer_bytes == 4096U && info.slot_max_bytes == 8192U);
    }
}

void PublicationFailures() {
    ScopedTempDir dir("chunkdb-table-limits-publication");
    auto config = Config(dir.path());
    config.default_options.wal_group_commit_updates = 1000;
    {
        TableCatalog catalog(config);
        auto options = config.default_options;
        options.feed_buffer_bytes = 4096;
        options.slot_max_bytes = 8192;
        const auto table = catalog.Create("world", kGeometry, options);
        const auto before = SerializeStoreManifest(*ReadStoreManifest(dir.path() / "tables" / "world"));
        Write(*table, 1);  // still in the WAL batch
        TableOptionsUpdate update;
        update.feed_buffer_bytes = 16384;
        update.slot_max_bytes = 16384;
        {
            ScopedFailpoint failure("CHUNKDB_FAILPOINT_WAL_OPEN_ONCE");
            assert(Throws([&] { catalog.SetOptions("world", update); }));
        }
        assert(SerializeStoreManifest(*ReadStoreManifest(dir.path() / "tables" / "world")) == before);
        assert(table->Info().feed_buffer_bytes == 4096U && table->Info().slot_max_bytes == 8192U);
        auto feed = table->SubscribeFeed();
        {
            ScopedFailpoint failure("CHUNKDB_FAILPOINT_TABLESET_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
            assert(Throws([&] { catalog.SetOptions("world", update); }));
        }
        // The manifest was published: the reported sync error does not
        // restore obsolete limits in the successfully reopened store.
        assert(table->Info().feed_buffer_bytes == 16384U && table->Info().slot_max_bytes == 16384U);
        assert(feed->buffer_bytes() == 16384U);
        Write(*table, 2);
        assert(Next(*feed)->kind == FeedEntry::Kind::kChange);
        update.feed_buffer_bytes = 32768;
        {
            ScopedFailpoint failure("CHUNKDB_FAILPOINT_TABLESET_REOPEN_FAIL_ONCE");
            assert(Throws([&] { catalog.SetOptions("world", update); }));
        }
        assert(!table->Acquire());
        assert(Next(*feed)->kind == FeedEntry::Kind::kEnd);
    }
    TableCatalog reopened(config);
    assert(reopened.Find("world")->Info().feed_buffer_bytes == 32768U);
    assert(reopened.Find("world")->Info().slot_max_bytes == 16384U);
}

void LiveRingLimitsAndResize() {
    ScopedTempDir dir("chunkdb-table-limits-ring");
    auto config = Config(dir.path());
    TableCatalog catalog(config);
    auto options = config.default_options;
    options.feed_buffer_bytes = 8192;
    const auto small = catalog.Create("small", kGeometry, options);
    options.feed_buffer_bytes = 65536;
    const auto large = catalog.Create("large", kGeometry, options);
    auto slow_small = small->SubscribeFeed();
    auto fast_small = small->SubscribeFeed();
    auto slow_large = large->SubscribeFeed();
    auto fast_large = large->SubscribeFeed();
    for (std::uint32_t i = 1; i <= 32; ++i) {
        Write(*small, i);
        Write(*large, i);
        assert(Next(*fast_small)->kind == FeedEntry::Kind::kChange);
        assert(Next(*fast_large)->kind == FeedEntry::Kind::kChange);
    }
    assert(Next(*slow_small)->kind == FeedEntry::Kind::kResync);
    assert(Next(*slow_large)->kind == FeedEntry::Kind::kChange);
    assert(FeedTestAccess::BufferedBytes(*small) <= 8192U);
    assert(FeedTestAccess::BufferedBytes(*large) <= 65536U);
    // An existing reader races the budget's update; it does not acquire a
    // table lease and must keep the same subscription after the resize.
    std::barrier start(2);
    std::jthread reader([&](std::stop_token stopped) {
        start.arrive_and_wait();
        while (!stopped.stop_requested()) {
            const auto budget = fast_large->buffer_bytes();
            assert(budget == 65536U || budget == 256U);
        }
    });
    start.arrive_and_wait();
    TableOptionsUpdate shrink;
    shrink.feed_buffer_bytes = 256;
    catalog.SetOptions("large", shrink);
    reader.request_stop();
    reader.join();
    assert(fast_large->buffer_bytes() == 256U);
    assert(FeedTestAccess::BufferedBytes(*large) <= 256U);
    assert(Next(*slow_large)->kind == FeedEntry::Kind::kResync);
    (void)fast_large->Resync();
    TableOptionsUpdate grow;
    grow.feed_buffer_bytes = 65536;
    catalog.SetOptions("large", grow);
    assert(fast_large->buffer_bytes() == 65536U);
    Write(*large, 33);
    assert(Next(*fast_large)->kind == FeedEntry::Kind::kChange);
}

FeedSlot Slot(Table& table) {
    const auto slots = table.ListFeedSlots(true);
    assert(slots.size() == 1U);
    return slots[0];
}

void SlotRetentionLimits() {
    ScopedTempDir dir("chunkdb-table-limits-slots");
    auto config = Config(dir.path());
    TableCatalog catalog(config);
    auto options = config.default_options;
    options.slot_max_bytes = 1;
    const auto small = catalog.Create("small", kGeometry, options);
    options.slot_max_bytes = 65536;
    const auto large = catalog.Create("large", kGeometry, options);
    const auto small_start = small->CreateFeedSlot("consumer").position;
    const auto large_start = large->CreateFeedSlot("consumer").position;
    auto small_feed = small->SubscribeFeed();
    auto large_feed = large->SubscribeFeed();
    Write(*small, 1);
    Write(*large, 1);
    const auto small_change = Next(*small_feed);
    const auto large_change = Next(*large_feed);
    assert(small_change->kind == FeedEntry::Kind::kChange);
    assert(large_change->kind == FeedEntry::Kind::kChange);
    assert(small_change->position.epoch == small_start.epoch);
    assert(large_change->position.epoch == large_start.epoch);
    assert(small_change->position.revision > small_start.revision);
    assert(large_change->position.revision > large_start.revision);
    // Retention charges archived history, rather than the current live WAL.
    // Rotate each known write into an archive before testing either limit.
    for (const auto& table : {small, large}) {
        auto lease = table->Acquire();
        assert(lease);
        lease->store().CheckpointForTests(0, 0);
        const auto archive_dir = dir.path() / "tables" / table->Info().name / kFeedArchiveDirName;
        std::size_t archived_wals = 0;
        for (const auto& entry : std::filesystem::directory_iterator(archive_dir)) {
            if (entry.path().extension() != ".wal") continue;
            assert(std::filesystem::file_size(entry.path()) > 1U);
            ++archived_wals;
        }
        assert(archived_wals == 1U);
    }
    FeedSlotTestAccess::Sync(*small);
    FeedSlotTestAccess::Sync(*large);
    FeedSlotTestAccess::Retain(*small);
    FeedSlotTestAccess::Retain(*large);
    const auto small_slot = Slot(*small);
    const auto large_slot = Slot(*large);
    assert(small_slot.position == small_start && large_slot.position == large_start);
    assert(small_slot.durable_watermark >= small_change->position.revision);
    assert(large_slot.durable_watermark >= large_change->position.revision);
    assert(small_slot.lost);
    assert(!large_slot.lost);
    assert(large_slot.retained_bytes > 1U && large_slot.retained_bytes <= 65536U);
    TableOptionsUpdate shrink;
    shrink.slot_max_bytes = 1;
    catalog.SetOptions("large", shrink);
    FeedSlotTestAccess::Retain(*large);
    const auto shrunk_slot = Slot(*large);
    assert(shrunk_slot.lost);
    assert(shrunk_slot.position == large_start);
    assert(shrunk_slot.durable_watermark == large_slot.durable_watermark);
    assert(Slot(*small).lost && small->Info().slot_max_bytes == 1U);
    assert(large->Info().slot_max_bytes == 1U);
}
}  // namespace

int main() {
    CodecAndValidation();
    PersistenceAndInheritance();
    PublicationFailures();
    LiveRingLimitsAndResize();
    SlotRetentionLimits();
}
