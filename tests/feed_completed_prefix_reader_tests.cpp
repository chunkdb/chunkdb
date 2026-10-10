#include <array>
#include <atomic>
#include <future>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <fstream>
#include <iostream>

#include "chunkdb/file_layout.hpp"
#include "chunkdb/crc32.hpp"
#include "feed_archive.hpp"
#include "feed_prefix.hpp"
#include "feed_protocol.hpp"
#include "feed_slots.hpp"
#include "feed_test_utils.hpp"
#include "slot_watch.hpp"
#include "wal_replay.hpp"

#ifndef _WIN32
#include <sys/wait.h>
#endif
#include "test_utils.hpp"
#include "wal_writer.hpp"

namespace {
using namespace chunkdb;
void Save(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    file.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(file.good());
}
void Reject(const std::function<void()>& action) {
    bool rejected = false;
    try { action(); } catch (const std::runtime_error&) { rejected = true; }
    assert(rejected);
}
struct Fixture {
    test::ScopedTempDir directory{"chunkdb-completed-feed-prefix"};
    Geometry geometry{{2U, 2U, 2U, 1U, 8U}};
    StoreId epoch{};
    Fixture() { epoch[0] = 9U; }
    std::vector<std::uint8_t> Frame(std::uint64_t revision, std::uint8_t value) {
        std::vector<std::uint8_t> bytes;
        const std::array<std::uint8_t, 3> state{value, 0U, 1U};
        WalFrameBuilder frame(&bytes, 1U);
        frame.AppendSpan(0U, state.data(), state.size());
        (void)frame.Finish(revision, revision * 10U);
        return bytes;
    }
    std::vector<std::uint8_t> Wal(ChunkCoord coord, const std::vector<std::vector<std::uint8_t>>& frames) {
        auto wal = BuildWalHeader(coord, epoch, {});
        for (const auto& frame : frames) wal.insert(wal.end(), frame.begin(), frame.end());
        return wal;
    }
    std::filesystem::path Live(ChunkCoord coord = {}) { return ChunkWalPath(directory.path(), geometry, coord); }
    std::filesystem::path Archive(ChunkCoord coord, std::uint64_t first, std::uint64_t last) {
        return directory.path() / ".chunkdb.feed" / ("C_" + std::to_string(coord.x) + "_" + std::to_string(coord.y) + "." +
            std::to_string(first) + "-" + std::to_string(last) + ".wal");
    }
    FeedArchiveReader Reader(const std::vector<FeedWalPrefix>& prefixes, std::uint64_t through, std::uint64_t from = 0U,
        std::shared_ptr<void> pin = {}) {
        return FeedArchiveAccess::CreateCompletedPrefix(directory.path(), geometry, epoch, {epoch, from}, through, std::move(pin), prefixes);
    }
};
void Value(const std::optional<std::vector<ColumnValue>>& row, std::uint8_t value) {
    assert(row && row->size() == 1U);
    const Geometry geometry{{2U, 2U, 2U, 1U, 8U}};
    const auto expected = DecodeBlockColumns(geometry.layout(), {value, 0U}, {}, 0U);
    assert(SameFeedValues(*row, expected));
}
void MutableTailIsNeverRead() {
    Fixture fixture;
    const auto first = fixture.Frame(2U, 8U);
    auto future = fixture.Frame(3U, 9U);
    future[4U] ^= 1U; // A future header's checksum is intentionally invalid.
    Save(fixture.Live(), fixture.Wal({}, {first, future}));
    const auto limit = kWalHeaderSize + first.size();
    auto reader = fixture.Reader({{{0, 0}, 2U, 2U, limit}}, 2U);
    // A concurrent rollback may truncate the future tail after capture.
    std::filesystem::resize_file(fixture.Live(), limit + 1U);
    auto entry = reader.Next();
    assert(entry && entry->position.revision == 2U && entry->blocks.size() == 1U);
    assert(!entry->blocks[0].before); Value(entry->blocks[0].after, 8U);
    assert(!reader.Next() && reader.through().revision == 2U);
    assert(std::filesystem::file_size(fixture.Live()) == limit + 1U);
}
void RenameAndReusedPartialLive(bool had_base) {
    Fixture fixture;
    const auto first = fixture.Frame(2U, 8U), second = fixture.Frame(3U, 9U);
    const auto wal = fixture.Wal({}, {first, second});
    Save(fixture.Live(), wal);
    const auto current = ChunkDataPath(fixture.directory.path(), fixture.geometry, {});
    const auto linked = fixture.directory.path() / ".chunkdb.feed" / "C_0_0.2.chk";
    if (had_base) Save(current, SerializeChunkImage(fixture.geometry, {}, {7U, 0U}, {1U},
        CheckpointCompression::kNone, 1U, 10U, fixture.epoch));
    auto pin = std::make_shared<int>(1);
    std::weak_ptr<int> weak = pin;
    {
        auto reader = fixture.Reader({{{0, 0}, 2U, 3U, wal.size()}}, 3U, 0U, pin); pin.reset();
        assert(!weak.expired());
        std::filesystem::create_directories(linked.parent_path());
        if (had_base) {
            std::filesystem::create_hard_link(current, linked);
            std::filesystem::remove(current); // Replacing the image preserves its immutable linked inode.
        }
        Save(current, SerializeChunkImage(fixture.geometry, {}, {99U, 0U}, {1U},
            CheckpointCompression::kNone, 3U, 30U, fixture.epoch));
        std::filesystem::rename(fixture.Live(), fixture.Archive({}, 2U, 3U));
        Save(fixture.Live(), {0xffU, 0x01U}); // A new writer's partial header must be ignored entirely.
        auto entry = reader.Next(); assert(entry && entry->position.revision == 2U);
        if (had_base) Value(entry->blocks[0].before, 7U); else assert(!entry->blocks[0].before);
        Value(entry->blocks[0].after, 8U);
        entry = reader.Next(); assert(entry && entry->position.revision == 3U);
        Value(entry->blocks[0].before, 8U); Value(entry->blocks[0].after, 9U);
        assert(!reader.Next());
        assert(LoadFile(fixture.Live()) == std::vector<std::uint8_t>({0xffU, 0x01U}));
    }
    assert(weak.expired());
}
void CurrentOriginalBase() {
    Fixture fixture;
    const auto current = ChunkDataPath(fixture.directory.path(), fixture.geometry, {});
    Save(current, SerializeChunkImage(fixture.geometry, {}, {7U, 0U}, {1U},
        CheckpointCompression::kNone, 1U, 10U, fixture.epoch));
    const auto wal = fixture.Wal({}, {fixture.Frame(2U, 8U)}); Save(fixture.Live(), wal);
    auto reader = fixture.Reader({{{0, 0}, 2U, 2U, wal.size()}}, 2U);
    const auto entry = reader.Next(); assert(entry && entry->position.revision == 2U);
    Value(entry->blocks[0].before, 7U); Value(entry->blocks[0].after, 8U); assert(!reader.Next());
}
void TransactionAcrossPrefixes() {
    Fixture fixture;
    const auto a = fixture.Wal({}, {fixture.Frame(2U, 8U)});
    const auto b = fixture.Wal({1, 0}, {fixture.Frame(2U, 9U)});
    Save(fixture.Live(), a); Save(fixture.Archive({1, 0}, 2U, 2U), b);
    auto reader = fixture.Reader({{{0, 0}, 2U, 2U, a.size()}, {{1, 0}, 2U, 2U, b.size()}}, 2U);
    const auto entry = reader.Next(); assert(entry && entry->position.revision == 2U && entry->blocks.size() == 2U);
    for (const auto& block : entry->blocks) { assert(!block.before); Value(block.after, block.chunk.x == 0 ? 8U : 9U); }
    assert(!reader.Next());
}
void CompletedPrefixDamageIsTerminal() {
    for (const bool archived : {false, true}) {
        for (const unsigned torn : {0U, 1U, 2U, 3U}) {
            Fixture fixture;
            const auto wal = fixture.Wal({}, {fixture.Frame(2U, 8U)});
            const auto path = archived ? fixture.Archive({}, 2U, 2U) : fixture.Live();
            Save(path, wal);
            const auto limit = torn == 0U ? 0U : torn == 1U ? kWalHeaderSize / 2U :
                torn == 2U ? kWalHeaderSize + 3U : wal.size() - 1U;
            // Archive size is observed during construction. A completed live
            // prefix instead retains its previously captured exact byte end.
            if (archived) std::filesystem::resize_file(path, limit);
            auto reader = fixture.Reader(archived ? std::vector<FeedWalPrefix>{} :
                std::vector<FeedWalPrefix>{{{0, 0}, 2U, 2U, wal.size()}}, 2U);
            if (!archived) std::filesystem::resize_file(path, limit);
            Reject([&] { (void)reader.Next(); }); Reject([&] { (void)reader.Next(); });
        }
    }
}

void IndexPublicationAndWatermark() {
    Fixture fixture; FeedWalPrefixIndex index;
    const auto first = fixture.Frame(2U, 8U), second = fixture.Frame(4U, 9U), third = fixture.Frame(5U, 10U);
    const auto wal = fixture.Wal({}, {first, second});
    auto prepared = index.Prepare({}, 0U, wal);
    assert(index.Capture(UINT64_MAX).empty()); // Reservation is not publication.
    index.Commit(std::move(prepared));
    assert(index.Capture(1U).empty());
    auto captured = index.Capture(3U); assert(captured.size() == 1U);
    assert(captured[0].first == 2U && captured[0].last == 2U && captured[0].limit == kWalHeaderSize + first.size());
    captured = index.Capture(4U); assert(captured[0].last == 4U && captured[0].limit == wal.size());
    index.Commit(index.Prepare({}, wal.size(), third));
    captured = index.Capture(5U); assert(captured[0].first == 2U && captured[0].last == 5U);
    assert(captured[0].limit == wal.size() + third.size());
}
void IndexTransactionRollback() {
    Fixture fixture; FeedWalPrefixIndex index;
    const auto old = fixture.Wal({}, {fixture.Frame(2U, 7U)});
    index.Commit(index.Prepare({}, 0U, old));
    const auto transaction_a = fixture.Frame(6U, 8U);
    const auto transaction_b = fixture.Wal({1, 0}, {fixture.Frame(6U, 9U)});
    auto a = index.Prepare({}, old.size(), transaction_a);
    auto b = index.Prepare({1, 0}, 0U, transaction_b);
    index.Commit(std::move(a)); index.Commit(std::move(b));
    auto captured = index.Capture(5U); assert(captured.size() == 1U && captured[0].last == 2U);
    captured = index.Capture(6U); assert(captured.size() == 2U && captured[0].last == 6U && captured[1].last == 6U);
    index.Truncate({}, old.size()); index.Truncate({1, 0}, 0U);
    captured = index.Capture(6U); assert(captured.size() == 1U && captured[0].last == 2U && captured[0].limit == old.size());
}
void IndexRetireAndNewSegment() {
    Fixture fixture; FeedWalPrefixIndex index;
    const auto old = fixture.Wal({}, {fixture.Frame(2U, 7U)});
    index.Commit(index.Prepare({}, 0U, old)); index.Truncate({}, 0U);
    assert(index.Capture(UINT64_MAX).empty());
    const auto next = fixture.Wal({}, {fixture.Frame(10U, 8U)});
    index.Commit(index.Prepare({}, 0U, next));
    auto captured = index.Capture(10U); assert(captured.size() == 1U);
    assert(captured[0].first == 10U && captured[0].last == 10U && captured[0].limit == next.size());
    // An already-created new WAL header is an equally valid reset boundary.
    const auto later = fixture.Frame(12U, 9U);
    index.Commit(index.Prepare({}, kWalHeaderSize, later));
    captured = index.Capture(12U); assert(captured[0].first == 12U && captured[0].last == 12U);
    assert(captured[0].limit == kWalHeaderSize + later.size());
}
void IndexSeedAndVariableHeaders() {
    Fixture fixture;
    const auto first = fixture.Frame(2U, 7U), second = fixture.Frame(4U, 8U);
    auto partial = fixture.Frame(5U, 9U); partial.pop_back();
    Save(fixture.Live(), fixture.Wal({}, {first, second, partial}));
    FeedWalPrefixIndex index; index.SeedFile(fixture.directory.path(), fixture.geometry, {}, fixture.epoch, {});
    const auto captured = index.Capture(UINT64_MAX); assert(captured.size() == 1U);
    assert(captured[0].first == 2U && captured[0].last == 4U);
    assert(captured[0].limit == kWalHeaderSize + first.size() + second.size());
    std::vector<std::uint8_t> frame;
    WalFrameBuilder builder(&frame, 1U, {0x01U, 0x02U}, "writer");
    const std::array<std::uint8_t, 3> state{10U, 0U, 1U};
    builder.AppendSpan(0U, state.data(), state.size()); (void)builder.Finish(7U, 70U);
    auto extended = BuildWalHeader({}, fixture.epoch, {.incompat = kFeatureFeedSlots});
    extended.insert(extended.end(), frame.begin(), frame.end());
    index.Truncate({}, 0U); index.Commit(index.Prepare({}, 0U, extended));
    const auto with_tlv = index.Capture(7U); assert(with_tlv.size() == 1U && with_tlv[0].first == 7U);
    assert(with_tlv[0].last == 7U && with_tlv[0].limit == extended.size());
    Save(fixture.Live(), extended); index.Clear(); index.SeedFile(fixture.directory.path(), fixture.geometry, {}, fixture.epoch, {.incompat = kFeatureFeedSlots});
    const auto reopened = index.Capture(7U); assert(reopened.size() == 1U && reopened[0].limit == extended.size());
}
void IndexSeedCrashTails() {
    for (const unsigned defect : {0U, 1U, 2U, 3U}) {
        Fixture fixture;
        const auto first = fixture.Frame(2U, 7U);
        auto tail = fixture.Frame(4U, 8U);
        if (defect == 0U) tail[tail.size() - 5U] ^= 1U; // Intact header, torn payload with full extent.
        if (defect == 1U) tail[4U] ^= 1U; // Torn header checksum.
        if (defect == 2U) tail.resize(10U); // Cut inside the fixed header.
        if (defect == 3U) tail.pop_back(); // Cut inside the frame body/trailer.
        Save(fixture.Live(), fixture.Wal({}, {first, tail}));
        FeedWalPrefixIndex index; index.SeedFile(fixture.directory.path(), fixture.geometry, {}, fixture.epoch, {});
        const auto captured = index.Capture(UINT64_MAX);
        assert(captured.size() == 1U && captured[0].first == 2U && captured[0].last == 2U);
        assert(captured[0].limit == kWalHeaderSize + first.size());
        auto reader = fixture.Reader(captured, 4U);
        const auto change = reader.Next(); assert(change && change->position.revision == 2U);
        Value(change->blocks[0].after, 7U); assert(!reader.Next());
    }
}
void IndexSeedDamageFailsClosed() {
    for (const unsigned defect : {0U, 1U, 2U}) {
        Fixture fixture;
        auto damaged = fixture.Frame(2U, 8U);
        if (defect == 0U) damaged[4U] ^= 1U; // Header damage before a later valid frame.
        if (defect == 1U) damaged[damaged.size() - 5U] ^= 1U; // Payload damage before a later valid frame.
        if (defect == 2U) {
            // Whole CRC-valid frame with an invalid record is damage even at EOF.
            const auto header = kWalFrameFixedHeaderSize + ReadLe16(damaged, 22U) + kWalFrameHeaderCrcSize;
            damaged[header] = 0xffU;
            const auto crc = Crc32(damaged.data() + header, ReadLe32(damaged, 28U));
            for (unsigned i = 0; i < 4U; ++i) damaged[damaged.size() - 4U + i] = static_cast<std::uint8_t>(crc >> (8U * i));
        }
        Save(fixture.Live(), fixture.Wal({}, defect == 2U ? std::vector<std::vector<std::uint8_t>>{damaged} :
            std::vector<std::vector<std::uint8_t>>{damaged, fixture.Frame(4U, 9U)}));
        FeedWalPrefixIndex index; index.SeedFile(fixture.directory.path(), fixture.geometry, {}, fixture.epoch, {});
        Reject([&] { (void)index.Capture(UINT64_MAX); });
    }
}
void IndexSeedUsesImageState() {
    Fixture fixture;
    const FeatureFlags features{.incompat = kFeatureFeedSlots};
    std::vector<std::uint8_t> gc;
    WalFrameBuilder builder(&gc, 1U, {}, std::nullopt, true);
    const std::array<std::uint8_t, 2U> payload{};
    const std::uint8_t presence = 0U;
    builder.AppendSpan(0U, payload.data(), payload.size());
    builder.AppendSpan(2U, &presence, 1U); (void)builder.Finish(4U, 40U);
    auto wal = BuildWalHeader({}, fixture.epoch, features);
    const auto first = fixture.Frame(2U, 7U);
    wal.insert(wal.end(), first.begin(), first.end()); wal.insert(wal.end(), gc.begin(), gc.end());
    Save(fixture.Live(), wal);
    const auto image = ChunkDataPath(fixture.directory.path(), fixture.geometry, {});
    Save(image, SerializeChunkImage(fixture.geometry, {}, {9U, 0U}, {1U},
        CheckpointCompression::kNone, 3U, 30U, fixture.epoch));
    FeedWalPrefixIndex index; index.SeedFile(fixture.directory.path(), fixture.geometry, {}, fixture.epoch, features);
    // Framing alone is valid, but GC cannot discard a present base state.
    Reject([&] { (void)index.Capture(UINT64_MAX); });
    Save(image, SerializeChunkImage(fixture.geometry, {}, {0U, 0U}, {0U},
        CheckpointCompression::kNone, 4U, 40U, fixture.epoch));
    index.Clear(); index.SeedFile(fixture.directory.path(), fixture.geometry, {}, fixture.epoch, features);
    // Frames already represented by a newer image still belong in catch-up.
    const auto captured = index.Capture(UINT64_MAX);
    assert(captured.size() == 1U && captured[0].first == 2U && captured[0].last == 4U && captured[0].limit == wal.size());
}
constexpr int kCrashExit = 86;
CatalogConfig CrashConfig(const std::filesystem::path& path, DurabilityMode mode) {
    auto config = feed_test::Config(path);
    config.default_options.durability_mode = mode;
    config.default_options.wal_group_commit_updates = 1000U;
    config.slot_sync_interval = std::chrono::hours(1);
    return config;
}
int CrashTail(const std::filesystem::path& path, DurabilityMode mode, bool header) {
    TableCatalog catalog(CrashConfig(path, mode));
    auto table = catalog.Find("default");
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, feed_test::Bits(11U));
    }
    FeedSlotTestAccess::Sync(*table);
    const auto wal_path = ChunkWalPath(path / "tables" / "default", Geometry{{2U, 2U, 4U, 4U, 32U}}, {});
    const auto valid_size = std::filesystem::file_size(wal_path);
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, feed_test::Bits(22U));
    }
    FeedSlotTestAccess::Sync(*table);
    auto wal = LoadFile(wal_path);
    assert(wal.size() > valid_size + kWalFrameFixedHeaderSize);
    wal[header ? valid_size + 4U : wal.size() - 5U] ^= 1U;
    Save(wal_path, wal);
    std::_Exit(kCrashExit); // Preserve the WAL and durable watermark without destructor recovery/checkpoint.
}
constexpr std::int64_t kColdChunks = 24;
int CrashCold(const std::filesystem::path& path) {
    TableCatalog catalog(CrashConfig(path, DurabilityMode::kRelaxed));
    auto table = catalog.Find("default");
    {
        auto lease = table->Acquire();
        for (std::int64_t x = 0; x < kColdChunks; ++x) {
            txn_test::WriteCounter(lease->store(), {x, 0}, static_cast<std::uint32_t>(10 + x));
            lease->store().CheckpointForTests(x, 0);
        }
    }
    (void)table->CreateFeedSlot("consumer");
    {
        auto lease = table->Acquire();
        for (std::int64_t x = 0; x < kColdChunks; ++x)
            txn_test::WriteCounter(lease->store(), {x, 0}, static_cast<std::uint32_t>(20 + x));
    }
    FeedSlotTestAccess::Sync(*table);
    std::_Exit(kCrashExit);
}
void MakeCold(const std::string& executable, const std::filesystem::path& path) {
    std::string command = "\"" + executable + "\" --crash-cold \"" + path.string() + "\"";
#ifdef _WIN32
    command = "\"" + command + "\"";
    assert(std::system(command.c_str()) == kCrashExit);
#else
    const auto status = std::system(command.c_str());
    assert(WIFEXITED(status) && WEXITSTATUS(status) == kCrashExit);
#endif
}
struct PrefixHook : FeedWalPrefixTestHook {
    std::atomic<unsigned> images{0};
    std::mutex mutex;
    std::condition_variable cv;
    bool pause = false, entered = false, released = false;
    void Run(Point point, ChunkCoord coord) override {
        if (point == Point::kImageRead) ++images;
        if (point != Point::kBeforeCatchUpSeed || coord != ChunkCoord{} || !pause) return;
        std::unique_lock lock(mutex);
        if (entered) return;
        entered = true; cv.notify_all();
        cv.wait(lock, [&] { return released; });
    }
    void Wait() {
        std::unique_lock lock(mutex);
        assert(cv.wait_for(lock, std::chrono::seconds(10), [&] { return entered; }));
    }
    void Release() {
        std::lock_guard lock(mutex);
        released = true; cv.notify_all();
    }
};
void CheckColdChanges(FeedArchiveReader& reader, bool later = false) {
    std::int64_t seen = 0;
    std::array<bool, kColdChunks> coordinates{};
    while (auto entry = reader.Next()) {
        assert(entry->blocks.size() == 1U);
        const auto& block = entry->blocks[0];
        assert(block.after && block.before);
        if (seen < kColdChunks) {
            assert(block.chunk.x >= 0 && block.chunk.x < kColdChunks && block.chunk.y == 0);
            assert(!coordinates[static_cast<std::size_t>(block.chunk.x)]);
            coordinates[static_cast<std::size_t>(block.chunk.x)] = true;
        } else {
            assert(block.chunk == ChunkCoord{});
        }
        const auto before = seen < kColdChunks ? 10 + block.chunk.x : seen == kColdChunks ? 20 : 70;
        assert(block.before->at(0) == ColumnValue{BitsValue{feed_test::Bits(static_cast<std::uint32_t>(before))}});
        const auto value = block.after->at(0);
        const auto expected = seen < kColdChunks ? 20 + block.chunk.x : seen == kColdChunks ? 70 : 80;
        const auto expected_bits = feed_test::Bits(static_cast<std::uint32_t>(expected));
        assert(value == ColumnValue{BitsValue{expected_bits}});
        ++seen;
    }
    assert(seen == kColdChunks + (later ? 2 : 0));
}
void OpenAndColdCatchUp(const std::string& executable, bool load_first) {
    test::ScopedTempDir directory("chunkdb-lazy-prefix-cold");
    MakeCold(executable, directory.path());
    PrefixHook hook;
    FeedWalPrefixTestAccess::SetHook(&hook);
    {
        TableCatalog catalog(CrashConfig(directory.path(), DurabilityMode::kRelaxed));
        auto table = catalog.Find("default");
        const auto start = table->ListFeedSlots()[0].position;
        assert(hook.images == 0U); // Observe actual image reads, including off-cache seed reads.
        if (load_first) {
            auto lease = table->Acquire();
            assert(txn_test::ReadCounter(lease->store(), {}) == 20U);
            assert(hook.images == 1U);
        }
        auto reader = FeedSlotTestAccess::CompletedPrefix(*table, "consumer", start);
        assert(hook.images == kColdChunks); // Loaded chunk reuses its replay; cold chunks each read once.
        std::cout << "cold index: open image reads=0, capture image reads=" << hook.images
                  << ", load first=" << load_first << "\n";
        CheckColdChanges(reader);
        FeedWalPrefixTestAccess::SetHook(nullptr);
    }
}
void LoadWinsCatchUp(const std::string& executable, bool replace_segment) {
    test::ScopedTempDir directory("chunkdb-lazy-prefix-race");
    MakeCold(executable, directory.path());
    PrefixHook hook; hook.pause = true;
    FeedWalPrefixTestAccess::SetHook(&hook);
    {
        TableCatalog catalog(CrashConfig(directory.path(), DurabilityMode::kRelaxed));
        auto table = catalog.Find("default");
        const auto start = table->ListFeedSlots()[0].position;
        auto future = std::async(std::launch::async, [&] {
            return FeedSlotTestAccess::CompletedPrefix(*table, "consumer", start);
        });
        hook.Wait(); // The cold replay is complete; only its optimistic publication is paused.
        {
            auto lease = table->Acquire();
            assert(txn_test::ReadCounter(lease->store(), {}) == 20U);
            txn_test::WriteCounter(lease->store(), {}, 70U);
            if (replace_segment) lease->store().CheckpointForTests(0, 0);
            txn_test::WriteCounter(lease->store(), {}, 80U);
        }
        FeedSlotTestAccess::Sync(*table);
        hook.Release();
        auto reader = future.get();
        CheckColdChanges(reader); // The original durable frontier excludes both concurrent mutations.
        auto newer = FeedSlotTestAccess::CompletedPrefix(*table, "consumer", start);
        CheckColdChanges(newer, true); // Stale seed cannot erase appended/new-segment boundaries.
        FeedWalPrefixTestAccess::SetHook(nullptr);
    }
}
void SeedRetirementAndDirectoryRace() {
    Fixture fixture;
    FeedWalPrefixIndex index;
    const auto token = index.BeginSeed({});
    index.Truncate({}, 0U);
    index.SeedReplay({}, {{2U, 100U}}, &token);
    assert(index.Capture(UINT64_MAX).empty());
    const auto reused = index.BeginSeed({});
    index.SeedReplay({}, {{4U, 200U}}, &reused);
    index.SeedReplay({}, {{2U, 100U}}, &token);
    assert(index.Capture(UINT64_MAX)[0].last == 4U);
    index.Clear();
    const auto reset = index.BeginSeed({1, 0});
    index.Clear();
    index.SeedReplay({1, 0}, {{2U, 100U}}, &reset);
    assert(index.Capture(UINT64_MAX).empty());
    Save(fixture.Live(), fixture.Wal({}, {fixture.Frame(2U, 7U)}));
    struct Removed : FeedWalPrefixTestHook {
        std::filesystem::path directory;
        void Run(Point point, ChunkCoord) override {
            if (point == Point::kBeforeDirectoryRead) std::filesystem::remove_all(directory);
        }
    } hook;
    hook.directory = fixture.Live().parent_path();
    FeedWalPrefixTestAccess::SetHook(&hook);
    std::mutex publication;
    index.SeedMissing(fixture.directory.path(), fixture.geometry, fixture.epoch, {}, publication);
    FeedWalPrefixTestAccess::SetHook(nullptr);
    assert(index.Capture(UINT64_MAX).empty());
    const auto misplaced = fixture.directory.path() / "L_99_99" / fixture.Live().filename();
    Save(misplaced, fixture.Wal({}, {fixture.Frame(2U, 7U)}));
    Reject([&] { index.SeedMissing(fixture.directory.path(), fixture.geometry, fixture.epoch, {}, publication); });
}
void RecoveryTrimKeepsCapturedPrefix(const std::string& executable) {
    test::ScopedTempDir directory("chunkdb-lazy-prefix-trim");
    const auto config = CrashConfig(directory.path(), DurabilityMode::kRelaxed);
    FeedPosition start;
    {
        TableCatalog catalog(config);
        start = catalog.Find("default")->CreateFeedSlot("consumer").position;
    }
    std::string command = "\"" + executable + "\" --crash-tail \"" + directory.path().string() + "\" relaxed payload";
#ifdef _WIN32
    command = "\"" + command + "\"";
    assert(std::system(command.c_str()) == kCrashExit);
#else
    const auto status = std::system(command.c_str());
    assert(WIFEXITED(status) && WEXITSTATUS(status) == kCrashExit);
#endif
    struct TrimPause : FeedWalPrefixTestHook {
        std::mutex mutex;
        std::condition_variable cv;
        bool entered = false, released = false;
        void Run(Point point, ChunkCoord) override {
            if (point != Point::kAfterRecoveryTrim) return;
            std::unique_lock lock(mutex);
            entered = true; cv.notify_all();
            cv.wait(lock, [&] { return released; });
        }
        void Wait() {
            std::unique_lock lock(mutex);
            assert(cv.wait_for(lock, std::chrono::seconds(10), [&] { return entered; }));
        }
        void Release() {
            std::lock_guard lock(mutex);
            released = true; cv.notify_all();
        }
    } hook;
    FeedWalPrefixTestAccess::SetHook(&hook);
    {
        TableCatalog catalog(config);
        auto table = catalog.Find("default");
        auto load = std::async(std::launch::async, [&] {
            auto lease = table->Acquire();
            return txn_test::ReadCounter(lease->store(), {});
        });
        hook.Wait(); // Trim has completed; the regular chunk is not admitted yet.
        auto reader = FeedSlotTestAccess::CompletedPrefix(*table, "consumer", start);
        const auto change = reader.Next();
        assert(change && change->blocks.size() == 1U);
        assert(change->blocks[0].after->at(0) == ColumnValue{BitsValue{feed_test::Bits(11U)}});
        assert(!reader.Next());
        hook.Release();
        assert(load.get() == 11U);
        FeedWalPrefixTestAccess::SetHook(nullptr);
    }
}
void RestartWriteAndCatchUp(const std::string& executable, DurabilityMode mode, bool header, bool cold_catch_up = true) {
    test::ScopedTempDir directory("chunkdb-completed-feed-crash-tail");
    const auto config = CrashConfig(directory.path(), mode);
    FeedPosition start;
    {
        TableCatalog catalog(config);
        start = catalog.Find("default")->CreateFeedSlot("consumer").position;
    }
    std::string command = "\"" + executable + "\" --crash-tail \"" + directory.path().string() + "\" " +
        DurabilityModeName(mode) + (header ? " header" : " payload");
#ifdef _WIN32
    command = "\"" + command + "\"";
    assert(std::system(command.c_str()) == kCrashExit);
#else
    const auto status = std::system(command.c_str());
    assert(WIFEXITED(status) && WEXITSTATUS(status) == kCrashExit);
#endif
    TableCatalog catalog(config);
    auto table = catalog.Find("default");
    assert(table->ListFeedSlots().size() == 1U && table->ListFeedSlots()[0].position == start);
    const auto wal_path = ChunkWalPath(directory.path() / "tables" / "default", Geometry{{2U, 2U, 4U, 4U, 32U}}, {});
    const auto torn = LoadFile(wal_path);
    const auto catch_up = [&](const std::vector<std::pair<std::uint64_t, std::uint8_t>>& revisions) {
        auto watch = SlotWatch::Create(table, "consumer", {}); watch->Activate();
        for (const auto& [revision, value] : revisions) {
            watch->WorkStep();
            auto output = watch->Take(SIZE_MAX);
            assert(output && !output->close);
            if (!output->revision) { // Initial schema precedes the first change.
                watch->Consumed(output->bytes->size());
                output = watch->Take(SIZE_MAX);
            }
            assert(output && !output->close && output->revision == revision);
            assert(output->bytes->find("change") != std::string::npos);
            std::string after = "*1\r\n$4\r\n";
            after.push_back(static_cast<char>(value)); after.append(3U, '\0'); after += "\r\n";
            assert(output->bytes->ends_with(after));
            watch->Sent(revision); watch->Consumed(output->bytes->size());
        }
        watch->WorkStep();
        assert(!watch->Take(SIZE_MAX)); // Torn mutation is never delivered.
        watch->Cancel(); watch->WorkStep();
    };
    // Catch-up before any chunk load/write must ignore the crash tail on a cold chunk.
    const auto recovered = ReadLe64(torn, kWalHeaderSize + 4U);
    if (cold_catch_up) {
        catch_up({{recovered, 11U}});
        assert(LoadFile(wal_path) == torn);
    }
    std::uint64_t next;
    {
        auto lease = table->Acquire();
        assert(txn_test::CounterOf(lease->store().ReadChunkState(0, 0)) == 11U);
        lease->store().SetBlockBits(0, 0, feed_test::Bits(33U));
        next = lease->store().GetChunkVersion(0, 0);
    }
    FeedSlotTestAccess::Sync(*table);
    catch_up({{recovered, 11U}, {next, 33U}});
    assert(table->ListFeedSlots().size() == 1U && !table->ListFeedSlots()[0].lost);
}
} // namespace
int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--trim-race") {
        RecoveryTrimKeepsCapturedPrefix(std::filesystem::absolute(argv[0]).string()); return 0;
    }
    if (argc == 3 && std::string_view(argv[1]) == "--crash-cold") return CrashCold(argv[2]);
    if (argc == 2 && std::string_view(argv[1]) == "--lazy-prefix") {
        const auto executable = std::filesystem::absolute(argv[0]).string();
        OpenAndColdCatchUp(executable, false); OpenAndColdCatchUp(executable, true);
        LoadWinsCatchUp(executable, false); LoadWinsCatchUp(executable, true);
        SeedRetirementAndDirectoryRace(); RecoveryTrimKeepsCapturedPrefix(executable); return 0;
    }
    if (argc == 5 && std::string_view(argv[1]) == "--crash-tail")
        return CrashTail(argv[2], ParseDurabilityMode(argv[3]), std::string_view(argv[4]) == "header");
    if (argc == 2 && std::string_view(argv[1]) == "--seed-crash-tails") { IndexSeedCrashTails(); return 0; }
    if (argc == 2 && std::string_view(argv[1]) == "--seed-damage") { IndexSeedDamageFailsClosed(); return 0; }
    if (argc == 2 && std::string_view(argv[1]) == "--restart-payload") {
        RestartWriteAndCatchUp(std::filesystem::absolute(argv[0]).string(), DurabilityMode::kRelaxed, false); return 0;
    }
    if (argc == 2 && std::string_view(argv[1]) == "--restart-header") {
        RestartWriteAndCatchUp(std::filesystem::absolute(argv[0]).string(), DurabilityMode::kRelaxed, true); return 0;
    }
    if (argc == 2 && std::string_view(argv[1]) == "--restart-write") {
        RestartWriteAndCatchUp(std::filesystem::absolute(argv[0]).string(), DurabilityMode::kRelaxed, false, false); return 0;
    }
    assert(argc == 1);
    MutableTailIsNeverRead(); RenameAndReusedPartialLive(false); RenameAndReusedPartialLive(true);
    CurrentOriginalBase(); TransactionAcrossPrefixes(); CompletedPrefixDamageIsTerminal();
    IndexPublicationAndWatermark(); IndexTransactionRollback(); IndexRetireAndNewSegment();
    IndexSeedAndVariableHeaders(); IndexSeedCrashTails(); IndexSeedDamageFailsClosed(); IndexSeedUsesImageState();
    for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal}) for (const bool header : {false, true})
        RestartWriteAndCatchUp(std::filesystem::absolute(argv[0]).string(), mode, header);
    const auto executable = std::filesystem::absolute(argv[0]).string();
    OpenAndColdCatchUp(executable, false); OpenAndColdCatchUp(executable, true);
    LoadWinsCatchUp(executable, false); LoadWinsCatchUp(executable, true);
    SeedRetirementAndDirectoryRace(); RecoveryTrimKeepsCapturedPrefix(executable);
    std::cout << "6 completed-prefix reader + 7 index groups + 4 crash recovery scenarios + 6 lazy index groups passed\n";
}
