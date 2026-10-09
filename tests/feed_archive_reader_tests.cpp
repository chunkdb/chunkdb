#include <cassert>
#include <bit>
#include <functional>
#include <fstream>
#include <limits>

#include "chunkdb/file_layout.hpp"
#include "chunkdb/crc32.hpp"
#include "feed_archive.hpp"
#include "feed_protocol.hpp"
#include "feature_flags.hpp"
#include "test_utils.hpp"
#include "txn_history.hpp"
#include "wal_writer.hpp"

namespace {
using namespace chunkdb;

void Save(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream file(path, std::ios::binary);
    file.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(file.good());
}

void Reject(const std::function<void()>& action) {
    bool rejected = false;
    try { action(); } catch (const std::exception&) { rejected = true; }
    assert(rejected);
}

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-feed-archive-reader"};
    Geometry geometry{{2U, 2U, 2U, 1U, 8U}};
    StoreId epoch{};
    Fixture() { epoch[0] = 7U; }
    std::filesystem::path Archive(ChunkCoord coord, std::uint64_t first, std::uint64_t last) const {
        return directory.path() / ".chunkdb.feed" / ("C_" + std::to_string(coord.x) + "_" + std::to_string(coord.y) +
            "." + std::to_string(first) + "-" + std::to_string(last) + ".wal");
    }
    std::filesystem::path Base(ChunkCoord coord, std::uint64_t first) const {
        return directory.path() / ".chunkdb.feed" / ("C_" + std::to_string(coord.x) + "_" + std::to_string(coord.y) +
            "." + std::to_string(first) + ".chk");
    }
    FeedArchiveReader Reader(std::uint64_t from, std::uint64_t through, std::shared_ptr<void> pin = {}) {
        return FeedArchiveAccess::Create(directory.path(), geometry, epoch, {epoch, from}, through, std::move(pin));
    }
    std::vector<std::uint8_t> Frame(std::uint64_t revision, std::uint8_t value, bool present = true) const {
        std::vector<std::uint8_t> bytes;
        std::vector<std::uint8_t> state{value, 0U, static_cast<std::uint8_t>(present ? 1U : 0U)};
        WalFrameBuilder frame(&bytes, 1U);
        frame.AppendSpan(0U, state.data(), state.size());
        (void)frame.Finish(revision, revision * 10U);
        return bytes;
    }
    std::vector<std::uint8_t> Wal(ChunkCoord coord, const std::vector<std::vector<std::uint8_t>>& frames) const {
        auto result = BuildWalHeader(coord, epoch, {});
        for (const auto& frame : frames) result.insert(result.end(), frame.begin(), frame.end());
        return result;
    }
    void Image(const std::filesystem::path& path, ChunkCoord coord, std::uint64_t revision, std::uint8_t value) const {
        Save(path, SerializeChunkImage(geometry, coord, {value, 0U}, {1U}, CheckpointCompression::kNone,
            revision, revision * 10U, epoch));
    }
};

void Equal(const std::optional<std::vector<ColumnValue>>& values, std::optional<std::uint8_t> value) {
    assert(values.has_value() == value.has_value());
    if (!value) return;
    const Geometry shape({2U, 2U, 2U, 1U, 8U});
    const auto expected = DecodeBlockColumns(shape.layout(), {*value, 0U}, {}, 0U);
    assert(SameFeedValues(*values, expected));
}

void MergeAndDurability() {
    Fixture fixture;
    Save(fixture.Archive({0, 0}, 2U, 4U), fixture.Wal({0, 0}, {fixture.Frame(2U, 8U), fixture.Frame(4U, 9U)}));
    Save(ChunkWalPath(fixture.directory.path(), fixture.geometry, {1, 0}),
        fixture.Wal({1, 0}, {fixture.Frame(3U, 10U), fixture.Frame(4U, 11U), fixture.Frame(5U, 12U)}));
    auto reader = fixture.Reader(0U, 4U);
    auto entry = reader.Next();
    assert(entry && entry->position.revision == 2U && entry->blocks.size() == 1U);
    Equal(entry->blocks[0].before, std::nullopt);
    Equal(entry->blocks[0].after, 8U);
    entry = reader.Next();
    assert(entry && entry->position.revision == 3U && entry->blocks.size() == 1U);
    entry = reader.Next();
    assert(entry && entry->position.revision == 4U && entry->blocks.size() == 2U);
    for (const auto& block : entry->blocks) {
        Equal(block.before, block.chunk.x == 0 ? 8U : 10U);
        Equal(block.after, block.chunk.x == 0 ? 9U : 11U);
    }
    assert(!reader.Next() && reader.position().revision == 4U);
    auto resumed = fixture.Reader(3U, 4U);
    entry = resumed.Next();
    assert(entry && entry->position.revision == 4U && entry->blocks.size() == 2U && !resumed.Next());
}

void LinkedBaseAndRollover() {
    Fixture fixture;
    const ChunkCoord coord{0, 0};
    const auto live = ChunkWalPath(fixture.directory.path(), fixture.geometry, coord);
    const auto current = ChunkDataPath(fixture.directory.path(), fixture.geometry, coord);
    fixture.Image(current, coord, 1U, 7U);
    Save(live, fixture.Wal(coord, {fixture.Frame(2U, 8U), fixture.Frame(3U, 9U, false)}));
    auto pin = std::make_shared<int>(42);
    std::weak_ptr<int> weak = pin;
    {
        auto reader = fixture.Reader(1U, 3U, pin);
        pin.reset();
        assert(!weak.expired());
        // A checkpoint after the reader captured its live cursor links the
        // original image and publishes a new image before renaming the WAL.
        std::filesystem::create_directories(fixture.Base(coord, 2U).parent_path());
        std::filesystem::create_hard_link(current, fixture.Base(coord, 2U));
        std::filesystem::remove(current);
        fixture.Image(current, coord, 3U, 99U);
        auto entry = reader.Next();
        Equal(entry->blocks[0].before, 7U);
        Equal(entry->blocks[0].after, 8U);
        std::filesystem::rename(live, fixture.Archive(coord, 2U, 3U));
        // The live name may immediately be reused by a later WAL.
        Save(live, fixture.Wal(coord, {fixture.Frame(4U, 100U)}));
        entry = reader.Next();
        assert(entry && entry->position.revision == 3U);
        Equal(entry->blocks[0].before, 8U);
        Equal(entry->blocks[0].after, std::nullopt);
        assert(!reader.Next());
    }
    assert(weak.expired());

    Fixture absent;
    Save(ChunkWalPath(absent.directory.path(), absent.geometry, coord), absent.Wal(coord, {absent.Frame(2U, 8U)}));
    absent.Image(ChunkDataPath(absent.directory.path(), absent.geometry, coord), coord, 2U, 8U);
    auto reader = absent.Reader(0U, 2U);
    auto entry = reader.Next();
    Equal(entry->blocks[0].before, std::nullopt);
    Equal(entry->blocks[0].after, 8U);
}

void PartialAndBoundaries() {
    Fixture fixture;
    const ChunkCoord coord{0, 0};
    const auto live = ChunkWalPath(fixture.directory.path(), fixture.geometry, coord);
    const auto first = fixture.Frame(2U, 8U);
    const auto second = fixture.Frame(3U, 9U);
    auto partial = second;
    partial.pop_back();
    Save(live, fixture.Wal(coord, {first, partial}));
    const auto before = LoadFile(live);
    auto reader = fixture.Reader(0U, 3U);
    assert(reader.Next()->position.revision == 2U && !reader.Next());
    assert(LoadFile(live) == before);
    Save(live, fixture.Wal(coord, {first, second}));
    const auto boundary = kWalHeaderSize + first.size();
    Save(TxnIntentPath(fixture.directory.path(), 3U), SerializeTxnIntent({TxnIntentState::kRollback, 3U, {{coord, boundary}}}));
    auto rollback = fixture.Reader(0U, 3U);
    assert(rollback.Next()->position.revision == 2U && !rollback.Next());
    assert(LoadFile(live) == fixture.Wal(coord, {first, second}));
    Save(TxnIntentPath(fixture.directory.path(), 3U), SerializeTxnIntent({TxnIntentState::kCommitted, 3U, {{coord, boundary}}}));
    auto committed = fixture.Reader(0U, 3U);
    assert(committed.Next()->position.revision == 2U && committed.Next()->position.revision == 3U && !committed.Next());
    Save(TxnIntentPath(fixture.directory.path(), 3U), SerializeTxnIntent({TxnIntentState::kRollback, 3U, {{coord, 0U}}}));
    auto zero = fixture.Reader(0U, 3U);
    assert(!zero.Next());
    Save(TxnIntentPath(fixture.directory.path(), 3U), SerializeTxnIntent({TxnIntentState::kRollback, 3U, {{coord, 999999U}}}));
    Reject([&] { (void)fixture.Reader(0U, 3U); });
}

void ConditionalBoundaries() {
    Fixture fixture;
    const ChunkCoord coord{0, 0};
    const auto live = ChunkWalPath(fixture.directory.path(), fixture.geometry, coord);
    const auto a = fixture.Frame(2U, 8U), b = fixture.Frame(3U, 9U);
    Save(live, fixture.Wal(coord, {a, b}));
    const auto intent = ConditionalIntentPathForWal(fixture.directory.path(), live);
    const auto save_intent = [&](bool committed, std::uint64_t boundary) {
        std::vector<std::uint8_t> bytes{'C', 'K', 'R', static_cast<std::uint8_t>(committed ? 'C' : 'B')};
        WriteLe64(bytes, boundary);
        WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
        Save(intent, bytes);
    };
    save_intent(false, kWalHeaderSize + a.size());
    auto rollback = fixture.Reader(0U, 3U);
    assert(rollback.Next()->position.revision == 2U && !rollback.Next());
    Save(TxnIntentPath(fixture.directory.path(), 4U), SerializeTxnIntent({TxnIntentState::kRollback, 4U, {{coord, 0U}}}));
    auto smaller = fixture.Reader(0U, 3U);
    assert(!smaller.Next());
    std::filesystem::remove(TxnIntentPath(fixture.directory.path(), 4U));
    save_intent(true, kWalHeaderSize + a.size());
    auto committed = fixture.Reader(0U, 3U);
    assert(committed.Next()->position.revision == 2U && committed.Next()->position.revision == 3U && !committed.Next());
    save_intent(false, 1U);
    std::filesystem::remove(live);
    Reject([&] { (void)fixture.Reader(0U, 3U); });
    save_intent(false, 0U);
    auto missing_zero = fixture.Reader(0U, 3U);
    assert(!missing_zero.Next());
}

void UserCollectionAndCoordinates() {
    Fixture fixture;
    const FeatureFlags features{.incompat = kFeatureFeedSlots};
    const ChunkCoord coord{std::numeric_limits<std::int64_t>::max(), std::numeric_limits<std::int64_t>::min()};
    auto make = [&](std::uint64_t revision, bool present, bool gc, std::optional<std::string_view> user) {
        std::vector<std::uint8_t> bytes;
        WalFrameBuilder frame(&bytes, 1U, {}, user, gc);
        const std::vector<std::uint8_t> payload{static_cast<std::uint8_t>(present ? 8U : 0U), 0U};
        const std::uint8_t presence = present ? 1U : 0U;
        frame.AppendSpan(0U, payload.data(), payload.size());
        frame.AppendSpan(2U, &presence, 1U);
        (void)frame.Finish(revision, revision * 10U);
        return bytes;
    };
    auto wal = BuildWalHeader(coord, fixture.epoch, features);
    for (const auto& frame : {make(2U, true, false, "alice"), make(3U, false, false, "bob"),
                            make(4U, false, true, std::nullopt), make(5U, true, false, std::nullopt)})
        wal.insert(wal.end(), frame.begin(), frame.end());
    Save(fixture.Archive(coord, 2U, 5U), wal);
    auto reader = FeedArchiveAccess::Create(fixture.directory.path(), fixture.geometry, fixture.epoch,
        {fixture.epoch, 0U}, 5U, {}, features);
    const auto a = reader.Next();
    assert(a->user == "alice" && a->position.revision == 2U && !a->blocks[0].x && !a->blocks[0].y);
    const auto b = reader.Next();
    assert(b->user == "bob" && b->position.revision == 3U);
    const auto c = reader.Next();
    assert(!c->user && c->position.revision == 5U && !c->blocks[0].before);
    assert(!reader.Next());
}

void HistoricLayout() {
    Fixture fixture;
    const auto old = SingleBitsColumnSchema(8U);
    const auto changed = ChangeColumnType(old, "bits", {ColumnKind::kBits, 16U}, Conversion::kExact);
    fixture.geometry = Geometry({2U, 2U, 2U, 1U, 16U}, changed);
    auto frames = std::vector<std::vector<std::uint8_t>>{fixture.Frame(2U, 8U)};
    std::vector<std::uint8_t> next;
    WalFrameBuilder frame(&next, 2U);
    const std::vector<std::uint8_t> state{9U, 0U, 0U, 0U, 1U};
    frame.AppendSpan(0U, state.data(), state.size());
    (void)frame.Finish(3U, 30U);
    frames.push_back(next);
    Save(fixture.Archive({0, 0}, 2U, 3U), fixture.Wal({0, 0}, frames));
    auto reader = fixture.Reader(0U, 3U);
    const auto a = reader.Next();
    assert(a->schema_version == 1U);
    Equal(a->blocks[0].after, 8U);
    const auto b = reader.Next();
    assert(b->schema_version == 2U && b->blocks.size() == 1U);
    assert(std::get<BitsValue>(b->blocks[0].before->at(0)).digits.size() == 16U);
    assert(std::get<BitsValue>(b->blocks[0].after->at(0)).digits.size() == 16U);
    assert(!reader.Next());
}

void TypedValuesAndBitEquality() {
    Fixture fixture;
    TableSchema schema{.version = 1U, .next_column_id = 5U,
        .columns = {{.id = 1U, .name = "small", .type = {ColumnKind::kUnsigned, 3U}, .nullable = true},
                    {.id = 2U, .name = "float", .type = {ColumnKind::kFloat32, 32U}},
                    {.id = 3U, .name = "text", .type = {ColumnKind::kText, 16U}, .nullable = true},
                    {.id = 4U, .name = "bytes", .type = {ColumnKind::kBytes, 16U}}}};
    fixture.geometry = Geometry({2U, 2U, 2U, 1U, 35U}, schema);
    const auto& layout = fixture.geometry.layout();
    const auto nan = std::bit_cast<float>(std::uint32_t{0x7fc00001U});
    const auto state = [&](std::uint64_t small, float value, std::string_view text) {
        ChunkState result;
        result.payload.assign(layout.payload_bytes(), 0U);
        result.presence_bitmap = {3U};
        for (const auto& fixed : layout.fixed_columns()) {
            const auto& column = schema.columns[fixed.column];
            for (std::size_t b = 0U; b < 2U; ++b) {
                const ColumnValue v = fixed.column == 0U ? ColumnValue{b == 0U ? small : 7U} : ColumnValue{b == 0U ? value : nan};
                const auto encoded = EncodeColumnValue(column, v);
                WriteValueBits(result.payload.data(), fixed.values * 8U + b * fixed.width, encoded.data(), fixed.width);
                if (fixed.validity != ChunkLayout::kNoValidity) result.payload[fixed.validity] |= static_cast<std::uint8_t>(1U << b);
            }
        }
        const std::vector<std::uint8_t> text_bytes(text.begin(), text.end()), bytes{0U, 0xffU, 0x80U};
        result.vars.Assign({3U, 0U}, text_bytes);
        result.vars.Assign({4U, 0U}, bytes);
        return result;
    };
    const auto a = state(1U, 0.0F, "hello"), b = state(2U, -0.0F, "world");
    const auto frame = [&](std::uint64_t revision, const ChunkState& value) {
        std::vector<std::uint8_t> bytes;
        auto packed = value.payload;
        packed.insert(packed.end(), value.presence_bitmap.begin(), value.presence_bitmap.end());
        WalFrameBuilder builder(&bytes, 1U);
        builder.AppendSpan(0U, packed.data(), packed.size());
        builder.AppendVarReplace(value.vars);
        (void)builder.Finish(revision, revision * 10U);
        return bytes;
    };
    Save(fixture.Archive({0, 0}, 2U, 3U), fixture.Wal({0, 0}, {frame(2U, a), frame(3U, b)}));
    auto reader = fixture.Reader(0U, 3U);
    const auto inserted = reader.Next();
    assert(inserted->blocks.size() == 2U);
    const auto changed = reader.Next();
    // Bit-packed neighbouring rows and unchanged NaN bits do not cause an
    // extra row; +0 to -0 must remain an observable change.
    assert(changed->blocks.size() == 1U && changed->blocks[0].local_x == 0U);
    assert(SameFeedValues(*changed->blocks[0].before, DecodeBlockColumns(layout, a.payload, a.vars, 0U)));
    assert(SameFeedValues(*changed->blocks[0].after, DecodeBlockColumns(layout, b.payload, b.vars, 0U)));
    assert(std::bit_cast<std::uint32_t>(std::get<float>(changed->blocks[0].after->at(1U))) == 0x80000000U);
    assert(!reader.Next());
}

void DamageAndEpoch() {
    Fixture fixture;
    auto wrong = fixture.epoch;
    wrong[0] ^= 1U;
    Reject([&] { (void)FeedArchiveAccess::Create(fixture.directory.path(), fixture.geometry, fixture.epoch, {wrong, 0U}, 1U, {}); });
    Reject([&] { (void)fixture.Reader(2U, 1U); });
    auto bad = fixture.Frame(2U, 8U);
    bad.back() ^= 1U;
    Save(fixture.Archive({0, 0}, 2U, 2U), fixture.Wal({0, 0}, {bad}));
    auto reader = fixture.Reader(0U, 2U);
    Reject([&] { (void)reader.Next(); });
}
}  // namespace

int main() {
    MergeAndDurability();
    LinkedBaseAndRollover();
    PartialAndBoundaries();
    ConditionalBoundaries();
    UserCollectionAndCoordinates();
    HistoricLayout();
    TypedValuesAndBitEquality();
    DamageAndEpoch();
}
