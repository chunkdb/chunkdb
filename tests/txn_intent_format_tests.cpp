#include "txn_intent_format.hpp"

#include <cassert>
#include <bit>
#include <cstdint>
#include <functional>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/crc32.hpp"

namespace {

using Bytes = std::vector<std::uint8_t>;
const chunkdb::StoreId kTable = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

void At(Bytes* bytes, std::size_t at, std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) (*bytes)[at + i] = static_cast<std::uint8_t>(value >> (8U * i));
}

void Checksum(Bytes* bytes) {
    At(bytes, bytes->size() - 4U, chunkdb::Crc32(bytes->data(), bytes->size() - 4U), 4);
}

Bytes Fixture(const chunkdb::TxnIntent& intent, std::uint8_t dims) {
    const auto member_bytes = 8U * static_cast<std::size_t>(dims) + 8U;
    Bytes bytes(40U + member_bytes * intent.entries.size(), 0);
    bytes[0] = 'C'; bytes[1] = 'K'; bytes[2] = 'T'; bytes[3] = 'I';
    At(&bytes, 4, 2, 2);
    bytes[6] = intent.state == chunkdb::TxnIntentState::kRollback ? 0 : 1;
    bytes[7] = dims;
    for (std::size_t i = 0; i < kTable.size(); ++i) bytes[8U + i] = kTable[i];
    At(&bytes, 24, intent.version, 8);
    At(&bytes, 32, intent.entries.size(), 4);
    for (std::size_t i = 0; i < intent.entries.size(); ++i) {
        const auto& entry = intent.entries[i];
        const auto at = 36U + i * member_bytes;
        At(&bytes, at, std::bit_cast<std::uint64_t>(entry.coord.x), 8);
        At(&bytes, at + 8, std::bit_cast<std::uint64_t>(entry.coord.y), 8);
        if (dims == 3) At(&bytes, at + 16, std::bit_cast<std::uint64_t>(entry.coord.z), 8);
        At(&bytes, at + 8U * dims, entry.wal_boundary, 8);
    }
    Checksum(&bytes);
    return bytes;
}

void Reject(const std::function<void()>& call, const std::string& reason) {
    try {
        call();
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()).find(reason) != std::string::npos);
        return;
    }
    assert(false && "expected checked intent rejection");
}

void Same(const chunkdb::TxnIntent& a, const chunkdb::TxnIntent& b) {
    assert(a.state == b.state && a.version == b.version && a.entries.size() == b.entries.size());
    for (std::size_t i = 0; i < a.entries.size(); ++i) {
        assert(a.entries[i].coord == b.entries[i].coord);
        assert(a.entries[i].wal_boundary == b.entries[i].wal_boundary);
    }
}

void TestExactEncoding() {
    const auto low = std::numeric_limits<std::int64_t>::min();
    const auto high = std::numeric_limits<std::int64_t>::max();
    const auto boundary = std::numeric_limits<std::uint64_t>::max();
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        for (const auto state : {chunkdb::TxnIntentState::kRollback, chunkdb::TxnIntentState::kCommitted}) {
            const chunkdb::TxnIntent intent{
                .state = state, .version = boundary,
                .entries = {{{low, low, dims == 3 ? low : 0}, 0},
                            {{low, 1, dims == 3 ? -1 : 0}, 4096},
                            {{high, high, dims == 3 ? high : 0}, boundary}},
            };
            const auto expected = Fixture(intent, dims);
            assert(expected.size() == 40U + intent.entries.size() * (8U * dims + 8U));
            const auto actual = chunkdb::SerializeCheckedTxnIntent(intent, dims, kTable, 3);
            assert(actual == expected);
            Same(chunkdb::ParseCheckedTxnIntent(actual, dims, kTable, 3), intent);
            Reject([&] { (void)chunkdb::SerializeCheckedTxnIntent(intent, dims, kTable, 2); }, "written-chunk limit");
            Reject([&] { (void)chunkdb::ParseCheckedTxnIntent(actual, dims, kTable, 2); }, "written-chunk limit");
        }
    }
    // Sorting includes z when x/y agree; zero and negative z are real axes.
    chunkdb::TxnIntent z_only{.version = 42, .entries = {{{0, 0, -1}, 0}, {{0, 0, 0}, 1}, {{0, 0, 1}, 2}}};
    const auto bytes = chunkdb::SerializeCheckedTxnIntent(z_only, 3, kTable, 3);
    Same(chunkdb::ParseCheckedTxnIntent(bytes, 3, kTable, 3), z_only);
    Reject([&] { (void)chunkdb::SerializeCheckedTxnIntent(z_only, 2, kTable, 3); }, "inactive");
    std::swap(z_only.entries[1], z_only.entries[2]);
    Reject([&] { (void)chunkdb::SerializeCheckedTxnIntent(z_only, 3, kTable, 3); }, "increasing");
    z_only.entries[2] = z_only.entries[1];
    Reject([&] { (void)chunkdb::SerializeCheckedTxnIntent(z_only, 3, kTable, 3); }, "unique");
}

void TestMalformedRecords() {
    const chunkdb::TxnIntent intent{.version = 42, .entries = {{{-1, 2, 3}, 0}, {{4, 5, 6}, 4096}}};
    const auto good = Fixture(intent, 3);
    const auto parse = [&](const Bytes& bytes) { return chunkdb::ParseCheckedTxnIntent(bytes, 3, kTable, 2); };
    for (std::size_t length = 0; length < good.size(); ++length) {
        const Bytes shortened(good.begin(), good.begin() + static_cast<std::ptrdiff_t>(length));
        Reject([&] { (void)parse(shortened); }, "transaction intent");
    }
    const auto corrupt = [&](std::size_t at, std::uint64_t value, unsigned width, const std::string& reason) {
        auto bytes = good; At(&bytes, at, value, width); Checksum(&bytes);
        Reject([&] { (void)parse(bytes); }, reason);
    };
    corrupt(0, 'X', 1, "CKTI");
    corrupt(3, 'B', 1, "CKTI");
    corrupt(4, 1, 2, "version");
    corrupt(4, 3, 2, "version");
    corrupt(6, 2, 1, "state");
    corrupt(7, 0, 1, "dimensions");
    corrupt(7, 4, 1, "dimensions");
    corrupt(7, 2, 1, "dimensions differ");
    corrupt(8, 0, 1, "another table");
    corrupt(24, 0, 8, "revision is zero");
    corrupt(32, 0, 4, "written-chunk limit");
    corrupt(32, 3, 4, "written-chunk limit");
    corrupt(32, 0xFFFFFFFFU, 4, "written-chunk limit");
    corrupt(32, 1, 4, "wrong length");
    auto extra = good; extra.push_back(0);
    Reject([&] { (void)parse(extra); }, "wrong length");
    auto reversed = good;
    for (unsigned i = 0; i < 24; ++i) std::swap(reversed[36U + i], reversed[68U + i]);
    Checksum(&reversed);
    Reject([&] { (void)parse(reversed); }, "increasing");
    auto duplicate = good;
    for (unsigned i = 0; i < 24; ++i) duplicate[68U + i] = duplicate[36U + i];
    Checksum(&duplicate);
    Reject([&] { (void)parse(duplicate); }, "unique");

    // Every field and every entry byte is covered by the final checksum. For
    // structurally valid changes the checksum itself must be the refusal reason.
    for (std::size_t at = 0; at < good.size(); ++at) {
        auto damaged = good; damaged[at] ^= 1;
        Reject([&] { (void)parse(damaged); }, "transaction intent");
    }
    for (const auto at : {std::size_t{24}, std::size_t{36}, std::size_t{60}, std::size_t{92}, good.size() - 1}) {
        auto damaged = good; damaged[at] ^= 1;
        Reject([&] { (void)parse(damaged); }, "checksum mismatch");
    }
    auto other = kTable; other[15] ^= 1;
    Reject([&] { (void)chunkdb::ParseCheckedTxnIntent(good, 3, other, 2); }, "another table");
    Reject([&] { (void)chunkdb::ParseCheckedTxnIntent(good, 2, kTable, 2); }, "dimensions differ");
    Reject([&] { (void)chunkdb::ParseCheckedTxnIntent(good, 1, kTable, 2); }, "dimensions");
    Reject([&] { (void)chunkdb::ParseCheckedTxnIntent(good, 3, kTable, 0); }, "written-chunk limit");
    auto invalid = intent; invalid.version = 0;
    Reject([&] { (void)chunkdb::SerializeCheckedTxnIntent(invalid, 3, kTable, 2); }, "revision is zero");
    invalid = intent; invalid.state = static_cast<chunkdb::TxnIntentState>(9);
    Reject([&] { (void)chunkdb::SerializeCheckedTxnIntent(invalid, 3, kTable, 2); }, "state");
    invalid = intent; invalid.entries.clear();
    Reject([&] { (void)chunkdb::SerializeCheckedTxnIntent(invalid, 3, kTable, 2); }, "written-chunk limit");
    Reject([&] { (void)chunkdb::SerializeCheckedTxnIntent(intent, 4, kTable, 2); }, "dimensions");
}

void TestCanonicalConditionalNames() {
    const auto low = std::numeric_limits<std::int64_t>::min();
    const auto high = std::numeric_limits<std::int64_t>::max();
    const chunkdb::Geometry two(chunkdb::GeometryConfig{
        .large_chunk_width_chunks = 4, .large_chunk_height_chunks = 3,
        .chunk_width_blocks = 4, .chunk_height_blocks = 5, .block_bits = 1});
    auto config = two.config(); config.dims = 3; config.chunk_depth_blocks = 3; config.large_chunk_depth_chunks = 2;
    const chunkdb::Geometry three(config);
    for (const auto coord : {chunkdb::ChunkCoord{0, 0, 0}, chunkdb::ChunkCoord{-5, -8, -3},
                             chunkdb::ChunkCoord{low, low, low}, chunkdb::ChunkCoord{high, high, high}}) {
        const auto name = chunkdb::CanonicalConditionalIntentFileName(three, coord);
        chunkdb::ChunkCoord parsed{11, 22, 33};
        assert(chunkdb::ParseConditionalIntentFileName(name, three, &parsed) && parsed == coord);
        parsed = {11, 22, 33};
        assert(!chunkdb::ParseConditionalIntentFileName(name, two, &parsed));
        assert((parsed == chunkdb::ChunkCoord{11, 22, 33}));
    }
    const auto name = chunkdb::CanonicalConditionalIntentFileName(two, {-5, -8});
    assert(name == "L_-2_-3__C_-5_-8.wal.rollback");
    chunkdb::ChunkCoord parsed;
    assert(chunkdb::ParseConditionalIntentFileName(name, two, &parsed) && (parsed == chunkdb::ChunkCoord{-5, -8}));
    Reject([&] { (void)chunkdb::CanonicalConditionalIntentFileName(two, {0, 0, 1}); }, "inactive");
    for (const auto* invalid : {
             "L_0_0__C_0_0.wal", "L_0_0__C_0_0.chk.rollback", "..__C_0_0.wal.rollback",
             "L_0_0__..__C_0_0.wal.rollback", "__L_0_0__C_0_0.wal.rollback",
             "L_0_0____C_0_0.wal.rollback", "L_0_0__C_0_0.wal__.rollback",
             "L_0_0__C_0_0.wal.rollback.extra", "L_0_0__C_+0_0.wal.rollback",
             "L_00_0__C_0_0.wal.rollback", "L_0_0__C_-0_0.wal.rollback",
             "L_0_0__C_01_0.wal.rollback", "L_0_0__C_1_9223372036854775808.wal.rollback",
             "L_0_0__C_0_0_0.wal.rollback", "L_1_0__C_0_0.wal.rollback",
             "L_0_0__/C_0_0.wal.rollback", "L_0_0__C_0_0.wal\\.rollback"}) {
        parsed = {11, 22, 33};
        assert(!chunkdb::ParseConditionalIntentFileName(invalid, two, &parsed));
        assert((parsed == chunkdb::ChunkCoord{11, 22, 33}));
    }
    assert(!chunkdb::ParseConditionalIntentFileName(name, two, nullptr));
}

}  // namespace

int main() {
    TestExactEncoding();
    TestMalformedRecords();
    TestCanonicalConditionalNames();
}
