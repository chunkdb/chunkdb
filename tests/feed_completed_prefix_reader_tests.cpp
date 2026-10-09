#include <array>
#include <cassert>
#include <functional>
#include <fstream>
#include <iostream>

#include "chunkdb/file_layout.hpp"
#include "feed_archive.hpp"
#include "feed_prefix.hpp"
#include "feed_protocol.hpp"
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
    FeedWalPrefixIndex index; index.Seed(fixture.directory.path(), fixture.epoch, {});
    const auto captured = index.Capture(UINT64_MAX); assert(captured.size() == 1U);
    assert(captured[0].first == 2U && captured[0].last == 4U);
    assert(captured[0].limit == kWalHeaderSize + first.size() + second.size());
    std::vector<std::uint8_t> frame;
    WalFrameBuilder builder(&frame, 2U, {0x01U, 0x02U}, "writer");
    const std::array<std::uint8_t, 3> state{10U, 0U, 1U};
    builder.AppendSpan(0U, state.data(), state.size()); (void)builder.Finish(7U, 70U);
    auto extended = BuildWalHeader({}, fixture.epoch, {.incompat = kFeatureFeedSlots});
    extended.insert(extended.end(), frame.begin(), frame.end());
    index.Truncate({}, 0U); index.Commit(index.Prepare({}, 0U, extended));
    const auto with_tlv = index.Capture(7U); assert(with_tlv.size() == 1U && with_tlv[0].first == 7U);
    assert(with_tlv[0].last == 7U && with_tlv[0].limit == extended.size());
    Save(fixture.Live(), extended); index.Seed(fixture.directory.path(), fixture.epoch, {.incompat = kFeatureFeedSlots});
    const auto reopened = index.Capture(7U); assert(reopened.size() == 1U && reopened[0].limit == extended.size());
}
void IndexSeedDamageFailsClosed() {
    Fixture fixture;
    auto frame = fixture.Frame(2U, 8U); frame[4U] ^= 1U;
    Save(fixture.Live(), fixture.Wal({}, {frame}));
    FeedWalPrefixIndex index; index.Seed(fixture.directory.path(), fixture.epoch, {});
    Reject([&] { (void)index.Capture(UINT64_MAX); });
}
} // namespace
int main() {
    MutableTailIsNeverRead(); RenameAndReusedPartialLive(false); RenameAndReusedPartialLive(true);
    CurrentOriginalBase(); TransactionAcrossPrefixes(); CompletedPrefixDamageIsTerminal();
    IndexPublicationAndWatermark(); IndexTransactionRollback(); IndexRetireAndNewSegment();
    IndexSeedAndVariableHeaders(); IndexSeedDamageFailsClosed();
    std::cout << "6 completed-prefix reader + 5 index groups passed\n";
}
