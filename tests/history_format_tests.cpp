// Block history segment encoding (docs/HISTORY_DESIGN.md): diffs, records
// and segment headers.

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "chunk_store_internal.hpp"
#include "history_format.hpp"

namespace {

using Bytes = std::vector<std::uint8_t>;
namespace history = chunkdb::history;

chunkdb::Geometry MakeGeometry(std::uint32_t width, std::uint32_t height, std::uint32_t block_bits) {
    return chunkdb::Geometry(chunkdb::GeometryConfig{
        .large_chunk_width_chunks = 2,
        .large_chunk_height_chunks = 2,
        .chunk_width_blocks = width,
        .chunk_height_blocks = height,
        .block_bits = block_bits,
    });
}

std::uint64_t g_state = 0x243F6A8885A308D3ULL;
std::uint64_t Next(std::uint64_t bound) {
    g_state ^= g_state << 13U;
    g_state ^= g_state >> 7U;
    g_state ^= g_state << 17U;
    return g_state % bound;
}

Bytes RandomValue(const chunkdb::Geometry& geometry) {
    const std::uint32_t bits = geometry.config().block_bits;
    Bytes value((bits + 7U) / 8U);
    for (auto& byte : value) {
        byte = static_cast<std::uint8_t>(Next(256));
    }
    if (bits % 8U != 0U) {
        value.back() &= static_cast<std::uint8_t>(0xFFU >> (8U - bits % 8U));
    }
    return value;
}

void SetBlock(const chunkdb::Geometry& geometry, Bytes* payload, Bytes* presence, std::uint32_t block, const Bytes* value) {
    const std::uint32_t bits = geometry.config().block_bits;
    for (std::uint32_t i = 0; i < bits; ++i) {
        const std::size_t bit = static_cast<std::size_t>(block) * bits + i;
        const bool on = value != nullptr && (((*value)[i / 8U] >> (i % 8U)) & 1U) != 0U;
        auto& byte = (*payload)[bit / 8U];
        byte = static_cast<std::uint8_t>(on ? byte | (1U << (bit % 8U)) : byte & ~(1U << (bit % 8U)));
    }
    auto& p = (*presence)[block / 8U];
    p = static_cast<std::uint8_t>(value != nullptr ? p | (1U << (block % 8U)) : p & ~(1U << (block % 8U)));
}

void TestDiffAndTouchedBlocks() {
    for (const std::uint32_t bits : {1U, 5U, 8U, 16U, 33U}) {
        const auto geometry = MakeGeometry(4, 3, bits);
        Bytes payload(geometry.ChunkPayloadBytes(), 0U);
        Bytes presence(chunkdb::ChunkPresenceBitmapBytes(geometry), 0U);
        const auto v1 = RandomValue(geometry);
        auto v2 = RandomValue(geometry);
        v2[0] ^= 0x01U;
        SetBlock(geometry, &payload, &presence, 2, &v1);
        SetBlock(geometry, &payload, &presence, 5, &v1);
        SetBlock(geometry, &payload, &presence, 7, &v1);
        auto after_payload = payload;
        auto after_presence = presence;
        SetBlock(geometry, &after_payload, &after_presence, 2, nullptr);  // removed
        SetBlock(geometry, &after_payload, &after_presence, 5, &v2);      // changed
        SetBlock(geometry, &after_payload, &after_presence, 7, &v1);      // unchanged
        SetBlock(geometry, &after_payload, &after_presence, 11, &v2);     // added
        const auto changes = history::DiffBlocks(geometry, payload, presence, after_payload, after_presence, nullptr);
        assert(changes.size() == 3U);
        assert(changes[0] == (history::BlockChange{.block_index = 2, .present = false}));
        assert(changes[1] == (history::BlockChange{.block_index = 5, .present = true, .bits = v2}));
        assert(changes[2] == (history::BlockChange{.block_index = 11, .present = true, .bits = v2}));
        const std::vector<std::uint32_t> only = {5, 7};
        const auto restricted = history::DiffBlocks(geometry, payload, presence, after_payload, after_presence, &only);
        assert(restricted.size() == 1U && restricted[0].block_index == 5U);
        // Every block a byte range can change is reported.
        for (std::size_t offset = 0; offset < chunkdb::ChunkStateBytes(geometry); ++offset) {
            for (std::size_t size = 1; offset + size <= chunkdb::ChunkStateBytes(geometry); ++size) {
                const auto touched = history::BlocksTouchedBySpan(geometry, offset, size);
                for (std::uint32_t block = 0; block < geometry.ChunkBlockCount(); ++block) {
                    bool can_change = false;
                    const std::size_t first_bit = static_cast<std::size_t>(block) * bits;
                    for (std::size_t bit = first_bit; bit < first_bit + bits; ++bit) {
                        can_change = can_change || (bit / 8U >= offset && bit / 8U < offset + size);
                    }
                    const std::size_t presence_byte = geometry.ChunkPayloadBytes() + block / 8U;
                    can_change = can_change || (presence_byte >= offset && presence_byte < offset + size);
                    const bool listed = std::find(touched.begin(), touched.end(), block) != touched.end();
                    assert(!can_change || listed);
                }
            }
        }
    }
}

history::Mutation RandomMutation(const chunkdb::Geometry& geometry, std::uint64_t revision, std::uint64_t time_ms, bool dense) {
    history::Mutation mutation{.revision = revision, .time_ms = time_ms};
    if (Next(3) == 0U) {
        mutation.tag.resize(Next(history::kMaxTagBytes + 1U));
        for (auto& byte : mutation.tag) {
            byte = static_cast<std::uint8_t>(Next(256));
        }
    }
    const std::size_t count = geometry.ChunkBlockCount();
    for (std::uint32_t block = 0; block < count; ++block) {
        if (dense ? Next(10) < 8U : Next(count) == 0U || (block == count - 1U && mutation.changes.empty())) {
            const bool present = Next(5) != 0U;
            mutation.changes.push_back(history::BlockChange{
                .block_index = block, .present = present, .bits = present ? RandomValue(geometry) : Bytes{}});
        }
    }
    return mutation;
}

void TestRecordRoundTrip() {
    for (const auto& [width, height, bits] : {std::tuple{4U, 4U, 1U}, std::tuple{4U, 4U, 5U}, std::tuple{16U, 16U, 16U},
                                             std::tuple{3U, 5U, 33U}, std::tuple{64U, 64U, 8U}}) {
        const auto geometry = MakeGeometry(width, height, bits);
        for (int round = 0; round < 20; ++round) {
            std::vector<history::Mutation> mutations;
            std::uint64_t revision = 1 + Next(1000);
            std::uint64_t time_ms = 1727786400000ULL + Next(1000);
            const std::size_t count = 1 + Next(40);
            for (std::size_t m = 0; m < count; ++m) {
                mutations.push_back(RandomMutation(geometry, revision, time_ms, Next(4) == 0U));
                revision += 1 + Next(20000);
                time_ms += Next(3) == 0U ? 0U : Next(100000);
            }
            Bytes encoded = {0xEE};  // records are appended to what is there
            history::EncodeRecord(geometry, mutations, &encoded);
            const auto read = history::ReadRecord(geometry, encoded.data() + 1, encoded.size() - 1, true);
            assert(read.status == history::RecordStatus::kOk);
            assert(read.mutations == mutations);
            assert(read.summary.size == encoded.size() - 1U);
            assert(read.summary.first_revision == mutations.front().revision);
            assert(read.summary.last_revision == mutations.back().revision);
            std::size_t events = 0;
            history::BlockMask mask{};
            for (const auto& mutation : mutations) {
                events += mutation.changes.size();
                for (const auto& change : mutation.changes) {
                    const auto bit = history::BlockMaskBit(geometry, change.block_index);
                    mask[bit / 8U] = static_cast<std::uint8_t>(mask[bit / 8U] | (1U << (bit % 8U)));
                }
            }
            assert(read.summary.event_count == events && read.summary.block_mask == mask);
            const auto skim = history::ReadRecord(geometry, encoded.data() + 1, encoded.size() - 1, false);
            assert(skim.status == history::RecordStatus::kOk && skim.mutations.empty());
        }
    }
}

void TestRecordDamageAndTruncation() {
    const auto geometry = MakeGeometry(8, 8, 5);
    std::vector<history::Mutation> mutations;
    for (std::uint64_t r = 0; r < 6; ++r) {
        mutations.push_back(RandomMutation(geometry, 10 + r * 3, 1000 + r, r == 2));
    }
    Bytes encoded;
    history::EncodeRecord(geometry, mutations, &encoded);
    // Every prefix is an interrupted append, never a valid record.
    for (std::size_t size = 0; size < encoded.size(); ++size) {
        const auto read = history::ReadRecord(geometry, encoded.data(), size, true);
        assert(read.status == history::RecordStatus::kTruncated);
    }
    // Every single-byte change is caught.
    for (std::size_t at = 0; at < encoded.size(); ++at) {
        for (const std::uint8_t flip : {std::uint8_t{0x01}, std::uint8_t{0x80}}) {
            auto damaged = encoded;
            damaged[at] ^= flip;
            const auto read = history::ReadRecord(geometry, damaged.data(), damaged.size(), true);
            assert(read.status != history::RecordStatus::kOk);
        }
    }
}

void TestEncodeValidation() {
    const auto geometry = MakeGeometry(4, 4, 5);
    const auto expect_invalid = [&](const std::vector<history::Mutation>& mutations) {
        Bytes out;
        bool thrown = false;
        try {
            history::EncodeRecord(geometry, mutations, &out);
        } catch (const std::invalid_argument&) {
            thrown = true;
        }
        assert(thrown && out.empty());
    };
    const history::BlockChange ok{.block_index = 1, .present = true, .bits = {0x1F}};
    expect_invalid({});
    expect_invalid({history::Mutation{.revision = 0, .changes = {ok}}});
    expect_invalid({history::Mutation{.revision = 5, .changes = {}}});
    expect_invalid({history::Mutation{.revision = 5, .changes = {ok}}, history::Mutation{.revision = 5, .changes = {ok}}});
    expect_invalid({history::Mutation{.revision = 5, .time_ms = 9, .changes = {ok}},
                    history::Mutation{.revision = 6, .time_ms = 8, .changes = {ok}}});
    expect_invalid({history::Mutation{.revision = 5, .tag = Bytes(256, 1), .changes = {ok}}});
    expect_invalid({history::Mutation{.revision = 5, .changes = {ok, ok}}});
    expect_invalid({history::Mutation{.revision = 5, .changes = {{.block_index = 16, .present = false}}}});
    expect_invalid({history::Mutation{.revision = 5, .changes = {{.block_index = 1, .present = true, .bits = {0x20}}}}});
    expect_invalid({history::Mutation{.revision = 5, .changes = {{.block_index = 1, .present = false, .bits = {0x01}}}}});
}

// The encoding stays near the sizes the design measured: a few bytes per
// event for point writes, and whole-chunk rewrites use the bitmap form.
void TestEncodedSizes() {
    const auto geometry = MakeGeometry(16, 16, 16);
    std::vector<history::Mutation> point;
    for (std::uint64_t r = 0; r < 384; ++r) {
        const auto block = static_cast<std::uint32_t>(Next(256));
        point.push_back(history::Mutation{
            .revision = 100 + r * 2, .time_ms = 5000 + r * 3,
            .changes = {{.block_index = block, .present = true, .bits = RandomValue(geometry)}}});
    }
    Bytes encoded;
    history::EncodeRecord(geometry, point, &encoded);
    const double per_event = static_cast<double>(encoded.size()) / 384.0;
    std::printf("point writes, 16-bit blocks: %.2f bytes per event\n", per_event);
    assert(per_event < 7.5);

    std::vector<history::Mutation> dense;
    for (std::uint64_t r = 0; r < 16; ++r) {
        history::Mutation mutation{.revision = 10 + r, .time_ms = 50};
        for (std::uint32_t block = 0; block < 256; ++block) {
            mutation.changes.push_back({.block_index = block, .present = true, .bits = RandomValue(geometry)});
        }
        dense.push_back(std::move(mutation));
    }
    Bytes dense_encoded;
    history::EncodeRecord(geometry, dense, &dense_encoded);
    const double dense_per_event = static_cast<double>(dense_encoded.size()) / (16.0 * 256.0);
    std::printf("whole-chunk rewrites, 16-bit blocks: %.2f bytes per event\n", dense_per_event);
    assert(dense_per_event < 2.4);
}

void TestSegmentHeader() {
    const auto geometry = MakeGeometry(8, 8, 5);
    const chunkdb::StoreId store = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    const chunkdb::ChunkCoord chunk{-4, 9};
    Bytes state(chunkdb::ChunkStateBytes(geometry), 0U);
    state[3] = 0x5A;
    state.back() = 0x81;
    for (const bool keyframe : {false, true}) {
        history::SegmentHeader header{
            .store_id = store, .chunk = chunk, .history_start = 77, .base_revision = 1000, .base_time_ms = 123};
        if (keyframe) {
            header.keyframe_state = state;
        }
        auto bytes = history::EncodeSegmentHeader(geometry, header);
        bytes.push_back(0xAB);  // a record follows
        std::size_t size = 0;
        const auto read = history::ReadSegmentHeader(geometry, bytes, store, chunk, &size);
        assert(size == bytes.size() - 1U);
        assert(read.history_start == 77U && read.base_revision == 1000U && read.base_time_ms == 123U);
        assert(read.keyframe_state.has_value() == keyframe);
        if (keyframe) {
            assert(*read.keyframe_state == state);
        }
        const auto expect_refused = [&](const Bytes& damaged, const chunkdb::StoreId& s, chunkdb::ChunkCoord c) {
            bool thrown = false;
            try {
                std::size_t ignored = 0;
                (void)history::ReadSegmentHeader(geometry, damaged, s, c, &ignored);
            } catch (const std::runtime_error&) {
                thrown = true;
            }
            assert(thrown);
        };
        for (std::size_t at = 0; at + 1U < bytes.size(); ++at) {
            auto damaged = bytes;
            damaged[at] ^= 0x04U;
            expect_refused(damaged, store, chunk);
        }
        auto other = store;
        other[0] ^= 1U;
        expect_refused(bytes, other, chunk);
        expect_refused(bytes, store, chunkdb::ChunkCoord{-4, 8});
    }
}

}  // namespace

int main() {
    TestDiffAndTouchedBlocks();
    TestRecordRoundTrip();
    TestRecordDamageAndTruncation();
    TestEncodeValidation();
    TestEncodedSizes();
    TestSegmentHeader();
    std::puts("history format tests passed");
    return 0;
}
