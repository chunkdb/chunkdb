// The 2.0 chunk image (docs/STORAGE_FORMAT.md Section 3): a header with a
// section directory, sections that may be zrle-compressed, and strict
// validation of everything a reader relies on.

#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/zrle.hpp"
#include "test_utils.hpp"

namespace {

using Bytes = std::vector<std::uint8_t>;
using chunkdb::test::ScopedTempDir;

const chunkdb::Geometry kGeometry(chunkdb::GeometryConfig{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 4,
    .chunk_height_blocks = 4,
    .block_bits = 5,
});
constexpr std::size_t kPayloadBytes = 10;
constexpr std::size_t kPresenceBytes = 2;
const chunkdb::StoreId kStoreId = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
const chunkdb::FeatureFlags kNoFeatures{.incompat = 0, .ro_compat = 0, .compat = 0};
const chunkdb::ChunkCoord kCoord{-3, 7};

void Le16(Bytes* out, std::uint16_t v) {
    for (int i = 0; i < 2; ++i) out->push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}
void Le32(Bytes* out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out->push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}
void Le64(Bytes* out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) out->push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}

struct Section {
    std::uint16_t type;
    std::uint16_t flags;
    Bytes stored;
    std::uint32_t raw_size;
    std::uint32_t crc;
};

Section Raw(std::uint16_t type, const Bytes& raw) {
    return {type, 0, raw, static_cast<std::uint32_t>(raw.size()), chunkdb::Crc32(raw)};
}

struct Header {
    chunkdb::FeatureFlags features = kNoFeatures;
    chunkdb::StoreId store_id = kStoreId;
    chunkdb::ChunkCoord coord = kCoord;
    std::uint64_t revision = 42;
    std::uint64_t commit_time_ms = 1727786400123ULL;
};

Header HeaderWith(const std::function<void(Header&)>& change) {
    Header header;
    change(header);
    return header;
}

// Builds an image with any directory, so every malformed layout can be made.
Bytes BuildImage(const Header& header, const std::vector<Section>& sections) {
    Bytes bytes = {'C', 'H', 'K', 'I', 'M', 'A', 'G', 'E'};
    Le16(&bytes, 1);
    Le16(&bytes, static_cast<std::uint16_t>(sections.size()));
    Le32(&bytes, header.features.incompat);
    Le32(&bytes, header.features.ro_compat);
    Le32(&bytes, header.features.compat);
    bytes.insert(bytes.end(), header.store_id.begin(), header.store_id.end());
    Le64(&bytes, static_cast<std::uint64_t>(header.coord.x));
    Le64(&bytes, static_cast<std::uint64_t>(header.coord.y));
    Le64(&bytes, header.revision);
    Le64(&bytes, header.commit_time_ms);
    for (const auto& section : sections) {
        Le16(&bytes, section.type);
        Le16(&bytes, section.flags);
        Le32(&bytes, static_cast<std::uint32_t>(section.stored.size()));
        Le32(&bytes, section.raw_size);
        Le32(&bytes, section.crc);
    }
    Le32(&bytes, chunkdb::Crc32(bytes.data(), bytes.size()));
    for (const auto& section : sections) {
        bytes.insert(bytes.end(), section.stored.begin(), section.stored.end());
    }
    return bytes;
}

Bytes SamplePayload() {
    Bytes payload(kPayloadBytes, 0U);
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<std::uint8_t>(0x11U * (i + 1));
    }
    return payload;
}
const Bytes kPresence = {0xA5, 0x3C};

chunkdb::ChunkStateImage Parse(
    const Bytes& bytes,
    const chunkdb::FeatureFlags& store_features = kNoFeatures) {
    return chunkdb::ParseChunkImage(bytes, kGeometry, kCoord, kStoreId, store_features);
}

void ExpectRejected(const Bytes& bytes, const std::string& reason,
                    const chunkdb::FeatureFlags& store_features = kNoFeatures) {
    try {
        (void)Parse(bytes, store_features);
    } catch (const std::runtime_error& e) {
        if (std::string(e.what()).find(reason) == std::string::npos) {
            std::fprintf(stderr, "expected '%s', got '%s'\n", reason.c_str(), e.what());
            assert(false);
        }
        return;
    }
    std::fprintf(stderr, "expected rejection '%s'\n", reason.c_str());
    assert(false);
}

void TestRoundTripWithAndWithoutCompression() {
    const auto payload = SamplePayload();
    for (const auto compression :
         {chunkdb::CheckpointCompression::kNone, chunkdb::CheckpointCompression::kZrle}) {
        const auto bytes = chunkdb::SerializeChunkImage(
            kGeometry, kCoord, payload, kPresence, compression, 42, 1727786400123ULL, kStoreId);
        const auto image = Parse(bytes);
        assert(image.payload == payload);
        assert(image.presence_bitmap == kPresence);
        assert(image.revision == 42U);
        assert(image.commit_time_ms == 1727786400123ULL);
        // Section flags say how each body is stored.
        const std::uint16_t flags = static_cast<std::uint16_t>(bytes[74] | (bytes[75] << 8));
        assert(flags == (compression == chunkdb::CheckpointCompression::kZrle ? 1U : 0U));
    }
    // The format as specified: 108 header bytes for two sections, then bodies.
    const auto raw = chunkdb::SerializeChunkImage(
        kGeometry, kCoord, payload, kPresence, chunkdb::CheckpointCompression::kNone, 42, 0, kStoreId);
    assert(raw.size() == 108U + kPayloadBytes + kPresenceBytes);
    assert(raw == BuildImage(HeaderWith([&](Header& h) { h.commit_time_ms = 0; }), {Raw(1, payload), Raw(2, kPresence)}));
}

void TestMalformedImagesRejected() {
    const auto payload = SamplePayload();
    const auto good = BuildImage(Header{}, {Raw(1, payload), Raw(2, kPresence)});
    (void)Parse(good);

    {
        auto bytes = good;
        bytes[0] = 'X';
        ExpectRejected(bytes, "not a 2.0 chunk image");
    }
    {
        auto bytes = good;
        bytes[8] = 2;
        ExpectRejected(bytes, "unsupported chunk image version 2");
    }
    ExpectRejected(Bytes(good.begin(), good.begin() + 60), "too small");
    {
        auto bytes = good;
        bytes[60] ^= 0x01U;  // the revision, CRC left stale
        ExpectRejected(bytes, "header checksum mismatch");
    }
    {
        auto bytes = good;
        bytes.back() ^= 0x01U;  // a presence body byte
        ExpectRejected(bytes, "section 2 checksum mismatch");
    }
    ExpectRejected(BuildImage(Header{}, {Raw(2, kPresence), Raw(1, payload)}), "ascending");
    ExpectRejected(BuildImage(Header{}, {Raw(1, payload), Raw(1, payload)}), "ascending");
    ExpectRejected(BuildImage(Header{}, {Raw(1, payload)}), "lacks its payload or presence");
    ExpectRejected(BuildImage(Header{}, {Raw(1, payload), Raw(2, kPresence), Raw(9, {7})}),
                   "unknown chunk image section 9");
    {
        auto section = Raw(2, kPresence);
        section.flags = 0x0004;
        ExpectRejected(BuildImage(Header{}, {Raw(1, payload), section}), "unknown flags");
    }
    ExpectRejected(BuildImage(Header{}, {Raw(1, Bytes(kPayloadBytes + 1, 0)), Raw(2, kPresence)}),
                   "wrong size for the store geometry");
    {
        auto section = Raw(2, kPresence);
        section.raw_size = 3;
        ExpectRejected(BuildImage(Header{}, {Raw(1, payload), section}), "stored and raw sizes differ");
    }
    {
        auto bytes = good;
        bytes.pop_back();
        ExpectRejected(bytes, "extends past the end");
        bytes = good;
        bytes.push_back(0);
        ExpectRejected(bytes, "bytes after its last section");
    }
    {
        // A compressed body that decodes to the wrong size.
        Section section{2, 1, chunkdb::ZrleCompress(Bytes(3, 0)), 2, chunkdb::Crc32(kPresence)};
        try {
            (void)Parse(BuildImage(Header{}, {Raw(1, payload), section}));
            assert(false);
        } catch (const std::exception&) {
        }
    }
    {
        auto other = kStoreId;
        other[0] ^= 0xFFU;
        ExpectRejected(BuildImage(HeaderWith([&](Header& h) { h.store_id = other; }), {Raw(1, payload), Raw(2, kPresence)}),
                       "belongs to another store");
    }
    ExpectRejected(BuildImage(HeaderWith([&](Header& h) { h.coord = {0, 0}; }), {Raw(1, payload), Raw(2, kPresence)}),
                   "coordinate mismatch");
    ExpectRejected(BuildImage(HeaderWith([&](Header& h) { h.revision = 0; }), {Raw(1, payload), Raw(2, kPresence)}),
                   "revision is zero");
    {
        std::vector<Section> many(65, Raw(9, {1}));
        ExpectRejected(BuildImage(Header{}, many), "too many sections");
    }
}

void TestFeatureFlagsInImages() {
    const auto payload = SamplePayload();
    const chunkdb::FeatureFlags compat{.incompat = 0, .ro_compat = 0, .compat = 1U << 4U};
    const auto with_extra = BuildImage(
        HeaderWith([&](Header& h) { h.features = compat; }), {Raw(1, payload), Raw(2, kPresence), Raw(7, {9, 9, 9})});

    // The image may not use a feature the store does not record.
    ExpectRejected(with_extra, "uses features the store does not");
    // When the store has it and this build does not know it, the section it
    // owns is checked and skipped; the known state still loads.
    const auto image = Parse(with_extra, compat);
    assert(image.payload == payload && image.presence_bitmap == kPresence);
    assert(image.features.compat == compat.compat);
    // Its CRC is still checked.
    auto damaged = with_extra;
    damaged.back() ^= 0x01U;
    ExpectRejected(damaged, "section 7 checksum mismatch", compat);
}

std::uint64_t NowMs() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

Bytes ReadFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

// Through the store: images written with and without compression load to the
// same state under either setting, and carry the commit time of the last
// captured mutation.
void TestStoreImages() {
    ScopedTempDir dir("chunkdb-chunk-image-store");
    chunkdb::StoreConfig config;
    config.geometry = kGeometry.config();
    config.data_dir = dir.path();
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.checkpoint_update_interval = 1;
    const auto image_path = chunkdb::ChunkDataPath(dir.path(), kGeometry, {0, 0});

    const auto before = NowMs();
    std::uint64_t first_time = 0;
    {
        config.checkpoint_compression = chunkdb::CheckpointCompression::kZrle;
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(1, 2, "10011");
        const auto image = chunkdb::ParseChunkImage(
            ReadFile(image_path), kGeometry, {0, 0}, store.store_id(), store.features());
        assert(image.revision == store.GetChunkVersion(0, 0));
        assert(image.commit_time_ms >= before && image.commit_time_ms <= NowMs());
        first_time = image.commit_time_ms;
        store.SetBlockBits(2, 2, "11111");
        const auto second = chunkdb::ParseChunkImage(
            ReadFile(image_path), kGeometry, {0, 0}, store.store_id(), store.features());
        assert(second.commit_time_ms >= first_time);
        assert(second.revision > image.revision);
    }
    {
        config.checkpoint_compression = chunkdb::CheckpointCompression::kNone;
        chunkdb::ChunkStore store(config);
        assert(store.GetBlockBits(1, 2) == "10011");
        assert(store.GetBlockBits(2, 2) == "11111");
        store.SetBlockBits(3, 3, "00001");
    }
    {
        config.checkpoint_compression = chunkdb::CheckpointCompression::kZrle;
        chunkdb::ChunkStore store(config);
        assert(store.GetBlockBits(1, 2) == "10011");
        assert(store.GetBlockBits(3, 3) == "00001");
    }
}

}  // namespace

int main() {
    TestRoundTripWithAndWithoutCompression();
    TestMalformedImagesRejected();
    TestFeatureFlagsInImages();
    TestStoreImages();
    return 0;
}
