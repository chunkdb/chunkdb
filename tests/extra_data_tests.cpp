// Per-block extra data (#44): the value and section codec, the EXTRA image
// section, the WAL records and their replay checks, store semantics and
// limits, durability across checkpoint, eviction, restart and crash,
// rollback, table options and the feature flag, offline verification, and
// the protocol commands.

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <thread>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/extra_data.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/table_catalog.hpp"
#include "chunkdb/zrle.hpp"
#include "feature_flags.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "verify.hpp"
#include "wal_replay.hpp"
#include "wal_writer.hpp"

namespace {

using Bytes = std::vector<std::uint8_t>;
using chunkdb::ChunkExtra;
using chunkdb::ExtraPadding;
using chunkdb::ExtraValue;
using chunkdb::test::ScopedTempDir;

const chunkdb::FeatureFlags kNoFeatures{};
const chunkdb::FeatureFlags kExtraFeature{.ro_compat = chunkdb::kFeatureExtraData};

void SetEnvVar(const char* key, const char* value) {
#ifdef _WIN32
    const int rc = _putenv_s(key, value);
#else
    const int rc = setenv(key, value, 1);
#endif
    assert(rc == 0);
    (void)rc;
}

void UnsetEnvVar(const char* key) {
#ifdef _WIN32
    const int rc = _putenv_s(key, "");
#else
    const int rc = unsetenv(key);
#endif
    assert(rc == 0);
    (void)rc;
}

bool Contains(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

template <typename Exception = std::exception, typename Fn>
std::string ExpectThrow(Fn fn, const std::string& part) {
    try {
        fn();
    } catch (const Exception& e) {
        if (!Contains(e.what(), part)) {
            std::fprintf(stderr, "expected '%s', got '%s'\n", part.c_str(), e.what());
            assert(false);
        }
        return e.what();
    }
    std::fprintf(stderr, "expected an exception with '%s'\n", part.c_str());
    assert(false);
    return {};
}

void Le16(Bytes* out, std::uint16_t v) {
    for (int i = 0; i < 2; ++i) out->push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}
void Le32(Bytes* out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out->push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}
void Le64(Bytes* out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) out->push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}

Bytes ReadFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void WriteFile(const std::filesystem::path& path, const Bytes& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(out);
}

// The files of a data directory as a crash would leave them, without the
// writer lock (whose heartbeat files change while the store is open).
void CopyCrashImage(const std::filesystem::path& from, const std::filesystem::path& to) {
    std::filesystem::create_directories(to);
    for (const auto& entry : std::filesystem::directory_iterator(from)) {
        if (entry.path().filename() == ".chunkdb.lock") {
            continue;
        }
        std::filesystem::copy(
            entry.path(), to / entry.path().filename(), std::filesystem::copy_options::recursive);
    }
}

// A value of `bits` bits with a pattern from `seed`; padding bits clear.
ExtraValue Val(std::uint32_t bits, std::uint8_t seed) {
    Bytes bytes(chunkdb::ExtraValueBytes(bits));
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        bytes[i] = static_cast<std::uint8_t>(seed + i * 37U);
    }
    if (bits % 8U != 0U) {
        bytes.back() &= static_cast<std::uint8_t>(0xFFU >> (8U - bits % 8U));
    }
    return ExtraValue{.bit_length = bits, .bytes = std::move(bytes)};
}

// One encoded EXTRA entry, any content.
Bytes Entry(std::uint32_t block_index, std::uint32_t bit_length, const Bytes& value) {
    Bytes out;
    Le32(&out, block_index);
    Le32(&out, bit_length);
    out.insert(out.end(), value.begin(), value.end());
    return out;
}

Bytes Concat(std::initializer_list<Bytes> parts) {
    Bytes out;
    for (const auto& part : parts) {
        out.insert(out.end(), part.begin(), part.end());
    }
    return out;
}

ChunkExtra DecodeStrict(const Bytes& bytes, std::size_t block_count) {
    return ChunkExtra::Decode(bytes.data(), bytes.size(), block_count, ExtraPadding::kReject);
}

// --- codec -----------------------------------------------------------------

void TestValueAndSectionCodec() {
    ExpectThrow<std::invalid_argument>(
        [] { (void)chunkdb::MakeExtraValue(0, {}, ExtraPadding::kReject); }, "at least 1 bit");
    ExpectThrow<std::invalid_argument>(
        [] { (void)chunkdb::MakeExtraValue(9, {1}, ExtraPadding::kReject); }, "takes 2 bytes");
    ExpectThrow<std::invalid_argument>(
        [] { (void)chunkdb::MakeExtraValue(3, {0x08}, ExtraPadding::kReject); }, "past its bit length");
    assert(chunkdb::MakeExtraValue(3, {0xFF}, ExtraPadding::kClear).bytes == Bytes{0x07});
    assert(chunkdb::MakeExtraValue(8, {0xFF}, ExtraPadding::kReject).bytes == Bytes{0xFF});

    ChunkExtra extra;
    assert(extra.empty() && extra.encoded_size() == 0U);
    assert(!extra.Put(9, Val(17, 1)).has_value());
    assert(!extra.Put(2, Val(1, 2)).has_value());
    assert(!extra.Put(40, Val(64, 3)).has_value());
    assert(extra.size() == 3U);
    assert(extra.encoded_size() == 3U * 8U + 3U + 1U + 8U);
    assert(extra.entry(0).block_index == 2U && extra.entry(2).block_index == 40U);
    const auto previous = extra.Put(9, Val(8, 4));
    assert(previous.has_value() && *previous == Val(17, 1));
    assert(extra.encoded_size() == 3U * 8U + 1U + 1U + 8U);
    assert(*extra.Find(9) == Val(8, 4));
    assert(!extra.Find(10).has_value());
    const auto encoded = extra.Encode();
    assert(encoded.size() == extra.encoded_size());
    assert(DecodeStrict(encoded, 64) == extra);
    assert(extra.Erase(2).has_value() && !extra.Erase(2).has_value());
    assert(extra.encoded_size() == 2U * 8U + 1U + 8U);
    assert(DecodeStrict({}, 64).empty());
    // A ChunkExtra never holds a value its own decoder would refuse.
    {
        ChunkExtra strict = extra;
        ExpectThrow<std::invalid_argument>(
            [&] { strict.Assign(5, ExtraValue{.bit_length = 3, .bytes = {0x08}}); }, "past its bit length");
        ExpectThrow<std::invalid_argument>(
            [&] { strict.Assign(5, ExtraValue{.bit_length = 0, .bytes = {}}); }, "malformed");
        std::vector<chunkdb::ExtraChange> padded;
        padded.push_back({.block_index = 5, .value = ExtraValue{.bit_length = 3, .bytes = {0xFF}}});
        ExpectThrow<std::invalid_argument>([&] { (void)ChunkExtra::Merge(strict, padded); }, "past its bit length");
        assert(strict == extra);
    }

    const Bytes one = {0x01};
    ExpectThrow<std::invalid_argument>(
        [&] { (void)DecodeStrict(Concat({Entry(3, 8, one), Entry(1, 8, one)}), 64); },
        "not strictly ascending");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)DecodeStrict(Concat({Entry(3, 8, one), Entry(3, 8, one)}), 64); },
        "not strictly ascending");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)DecodeStrict(Entry(64, 8, one), 64); }, "the chunk has 64 blocks");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)DecodeStrict(Entry(0, 0, {}), 64); }, "has 0 bits");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)DecodeStrict(Bytes{1, 0, 0, 0, 8, 0, 0}, 64); }, "header extends past");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)DecodeStrict(Entry(0, 9, one), 64); }, "value extends past");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)DecodeStrict(Entry(0, 4, {0x10}), 64); }, "past its bit length");
    const Bytes padded = Entry(0, 4, {0xF3});
    const auto cleared =
        ChunkExtra::Decode(padded.data(), padded.size(), 64, ExtraPadding::kClear);
    assert(cleared.Find(0)->ToValue().bytes == Bytes{0x03});
    const Bytes oversized(chunkdb::kExtraMaxChunkBytesLimit + 1U, 0U);
    ExpectThrow<std::invalid_argument>(
        [&] { (void)DecodeStrict(oversized, 64); }, "exceeds");

    ExpectThrow<std::invalid_argument>([] { chunkdb::RequireValidExtraLimits(0, 1024); }, "between 1");
    ExpectThrow<std::invalid_argument>(
        [] { chunkdb::RequireValidExtraLimits(chunkdb::kExtraMaxBlockBitsLimit + 1U, 1024); },
        "between 1");
    ExpectThrow<std::invalid_argument>([] { chunkdb::RequireValidExtraLimits(1, 8); }, "between 9");
    ExpectThrow<std::invalid_argument>(
        [] { chunkdb::RequireValidExtraLimits(1, chunkdb::kExtraMaxChunkBytesLimit + 1U); },
        "between 9");
    ExpectThrow<std::invalid_argument>([] { chunkdb::RequireValidExtraLimits(73, 17); }, "must hold one value");
    chunkdb::RequireValidExtraLimits(72, 17);
    chunkdb::RequireValidExtraLimits(1, 9);
    chunkdb::RequireValidExtraLimits(
        chunkdb::kExtraMaxBlockBitsLimit, chunkdb::kExtraMaxChunkBytesLimit);

    // An update must change something, in order, and undo restores it.
    ChunkExtra base;
    (void)base.Put(1, Val(5, 1));
    (void)base.Put(4, Val(9, 2));
    const ChunkExtra original = base;
    chunkdb::ExtraUpdate noop;
    noop.changes.push_back({.block_index = 1, .value = Val(5, 1)});
    ExpectThrow<std::logic_error>([&] { (void)chunkdb::ApplyExtraUpdate(&base, noop); }, "does not change");
    chunkdb::ExtraUpdate missing;
    missing.changes.push_back({.block_index = 2});
    ExpectThrow<std::logic_error>([&] { (void)chunkdb::ApplyExtraUpdate(&base, missing); }, "does not change");
    chunkdb::ExtraUpdate unordered;
    unordered.changes.push_back({.block_index = 4});
    unordered.changes.push_back({.block_index = 1});
    ExpectThrow<std::logic_error>([&] { (void)chunkdb::ApplyExtraUpdate(&base, unordered); }, "ascending");
    assert(base == original);
    chunkdb::ExtraUpdate update;
    update.changes.push_back({.block_index = 0, .value = Val(3, 3)});
    update.changes.push_back({.block_index = 1});
    update.changes.push_back({.block_index = 4, .value = Val(30, 4)});
    assert(chunkdb::ExtraSizeAfter(base, update) == 2U * 8U + 1U + 4U);
    auto undo = chunkdb::ApplyExtraUpdate(&base, update);
    assert(base.size() == 2U && *base.Find(4) == Val(30, 4) && !base.Find(1).has_value());
    chunkdb::UndoExtraUpdate(&base, std::move(undo));
    assert(base == original);
    chunkdb::ExtraUpdate replace;
    replace.replace = ChunkExtra{};
    undo = chunkdb::ApplyExtraUpdate(&base, replace);
    assert(base.empty());
    chunkdb::UndoExtraUpdate(&base, std::move(undo));
    assert(base == original);
}

// The flat representation against a map model: random sets of every size,
// removals, updates and their undo, checked through every accessor and the
// encoding after each step.
void TestChunkExtraAgainstModel() {
    std::uint64_t state = 0x9E3779B97F4A7C15ULL;
    const auto next = [&state](std::uint64_t bound) {
        state ^= state << 13U;
        state ^= state >> 7U;
        state ^= state << 17U;
        return state % bound;
    };
    const auto check = [](const ChunkExtra& extra, const std::map<std::uint32_t, ExtraValue>& model) {
        assert(extra.size() == model.size());
        std::size_t size = 0;
        std::size_t i = 0;
        for (const auto& [block, value] : model) {
            const auto found = extra.Find(block);
            assert(found.has_value() && *found == value);
            const auto entry = extra.entry(i++);
            assert(entry.block_index == block && entry.value == value);
            size += 8U + value.bytes.size();
        }
        assert(extra.encoded_size() == size);
        assert(DecodeStrict(extra.Encode(), 64) == extra);
        std::size_t iterated = 0;
        for (const auto entry : extra) {
            assert(model.at(entry.block_index) == entry.value);
            ++iterated;
        }
        assert(iterated == model.size());
    };
    ChunkExtra extra;
    std::map<std::uint32_t, ExtraValue> model;
    for (int step = 0; step < 4000; ++step) {
        const auto block = static_cast<std::uint32_t>(next(64));
        if (next(3) == 0U) {
            assert(extra.Remove(block) == (model.erase(block) == 1U));
            assert(!extra.Find(block).has_value());
        } else {
            const auto value = Val(1 + static_cast<std::uint32_t>(next(300)), static_cast<std::uint8_t>(step));
            const auto previous = extra.Put(block, value);
            const auto it = model.find(block);
            assert(previous.has_value() == (it != model.end()));
            if (previous.has_value()) {
                assert(*previous == it->second);
            }
            model[block] = value;
        }
        check(extra, model);
        if (step % 50 == 0) {
            // A random update and its undo restore the exact encoding.
            const ChunkExtra before = extra;
            chunkdb::ExtraUpdate update;
            for (std::uint32_t b = 0; b < 64; b += 1 + static_cast<std::uint32_t>(next(6))) {
                const bool has = model.count(b) != 0U;
                if (has && next(2) == 0U) {
                    update.changes.push_back({.block_index = b});
                } else if (next(2) == 0U) {
                    auto value = Val(1 + static_cast<std::uint32_t>(next(500)), static_cast<std::uint8_t>(b + step));
                    if (!has || !(model.at(b) == value)) {
                        update.changes.push_back({.block_index = b, .value = std::move(value)});
                    }
                }
            }
            const auto expected_size = chunkdb::ExtraSizeAfter(extra, update);
            auto applied = update;
            auto undo = chunkdb::ApplyExtraUpdate(&extra, std::move(applied));
            assert(extra.encoded_size() == expected_size);
            for (const auto& change : update.changes) {
                const auto found = extra.Find(change.block_index);
                assert(found.has_value() == change.value.has_value());
                if (change.value.has_value()) {
                    assert(*found == *change.value);
                }
            }
            chunkdb::UndoExtraUpdate(&extra, std::move(undo));
            assert(extra == before);
            check(extra, model);
        }
    }
}

// --- image -----------------------------------------------------------------

const chunkdb::Geometry kSmall(chunkdb::GeometryConfig{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 4,
    .chunk_height_blocks = 4,
    .block_bits = 5,
});
constexpr std::size_t kSmallPayloadBytes = 10;
const chunkdb::StoreId kStoreId = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
const chunkdb::ChunkCoord kCoord{-3, 7};

struct Section {
    std::uint16_t type;
    Bytes raw;
    std::uint32_t raw_size;
};

Section Raw(std::uint16_t type, const Bytes& raw) {
    return {type, raw, static_cast<std::uint32_t>(raw.size())};
}

// An image with any sections (uncompressed), so malformed ones can be made.
Bytes BuildImage(
    const chunkdb::FeatureFlags& features,
    const std::vector<Section>& sections,
    const chunkdb::StoreId& store_id = kStoreId,
    chunkdb::ChunkCoord coord = kCoord,
    std::uint64_t revision = 42) {
    Bytes bytes = {'C', 'H', 'K', 'I', 'M', 'A', 'G', 'E'};
    Le16(&bytes, 1);
    Le16(&bytes, static_cast<std::uint16_t>(sections.size()));
    Le32(&bytes, features.incompat);
    Le32(&bytes, features.ro_compat);
    Le32(&bytes, features.compat);
    bytes.insert(bytes.end(), store_id.begin(), store_id.end());
    Le64(&bytes, static_cast<std::uint64_t>(coord.x));
    Le64(&bytes, static_cast<std::uint64_t>(coord.y));
    Le64(&bytes, revision);
    Le64(&bytes, 1727786400123ULL);
    for (const auto& section : sections) {
        Le16(&bytes, section.type);
        Le16(&bytes, 0);
        Le32(&bytes, static_cast<std::uint32_t>(section.raw.size()));
        Le32(&bytes, section.raw_size);
        Le32(&bytes, chunkdb::Crc32(section.raw));
    }
    Le32(&bytes, chunkdb::Crc32(bytes.data(), bytes.size()));
    for (const auto& section : sections) {
        bytes.insert(bytes.end(), section.raw.begin(), section.raw.end());
    }
    return bytes;
}

void TestImageExtraSection() {
    Bytes payload(kSmallPayloadBytes);
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<std::uint8_t>(0x11U * (i + 1));
    }
    const Bytes presence = {0x23, 0x80};  // blocks 0, 1, 5, 15
    ChunkExtra extra;
    (void)extra.Put(0, Val(1, 7));
    (void)extra.Put(5, Val(13, 8));
    (void)extra.Put(15, Val(200, 9));

    for (const auto compression :
         {chunkdb::CheckpointCompression::kNone, chunkdb::CheckpointCompression::kZrle}) {
        // Without extra data the image is exactly what it was before: two
        // sections and no feature bit.
        const auto plain = chunkdb::SerializeChunkImage(
            kSmall, kCoord, payload, presence, compression, 42, 1, kStoreId);
        const ChunkExtra empty;
        assert(plain == chunkdb::SerializeChunkImage(
                            kSmall, kCoord, payload, presence, compression, 42, 1, kStoreId, &empty));
        assert(chunkdb::ReadLe16(plain, 10) == 2U && chunkdb::ReadLe32(plain, 16) == 0U);
        const auto parsed_plain = chunkdb::ParseChunkImage(plain, kSmall, kCoord, kStoreId, kExtraFeature);
        assert(parsed_plain.extra.empty());

        const auto with_extra = chunkdb::SerializeChunkImage(
            kSmall, kCoord, payload, presence, compression, 42, 1, kStoreId, &extra);
        assert(chunkdb::ReadLe16(with_extra, 10) == 3U);
        assert(chunkdb::ReadLe32(with_extra, 16) == chunkdb::kFeatureExtraData);
        const auto image = chunkdb::ParseChunkImage(with_extra, kSmall, kCoord, kStoreId, kExtraFeature);
        assert(image.payload == payload && image.presence_bitmap == presence);
        assert(image.extra == extra);
        // A store without the feature refuses the image.
        ExpectThrow<std::runtime_error>(
            [&] { (void)chunkdb::ParseChunkImage(with_extra, kSmall, kCoord, kStoreId, kNoFeatures); },
            "uses features the store does not");
    }

    const auto parse = [&](const Bytes& bytes) {
        return chunkdb::ParseChunkImage(bytes, kSmall, kCoord, kStoreId, kExtraFeature);
    };
    const auto section = extra.Encode();
    // The section needs the image's feature bit.
    ExpectThrow<std::runtime_error>(
        [&] { (void)parse(BuildImage(kNoFeatures, {Raw(1, payload), Raw(2, presence), Raw(3, section)})); },
        "without the image's extra-data feature");
    const auto image_with = [&](const Bytes& extra_section) {
        return BuildImage(kExtraFeature, {Raw(1, payload), Raw(2, presence), Raw(3, extra_section)});
    };
    assert(parse(image_with(section)).extra == extra);
    // Entries for absent blocks, out of order, repeated or malformed.
    const Bytes one = {0x01};
    ExpectThrow<std::runtime_error>(
        [&] { (void)parse(image_with(Entry(2, 8, one))); }, "absent block index 2");
    ExpectThrow<std::runtime_error>(
        [&] { (void)parse(image_with(Concat({Entry(5, 8, one), Entry(1, 8, one)}))); },
        "not strictly ascending");
    ExpectThrow<std::runtime_error>(
        [&] { (void)parse(image_with(Concat({Entry(5, 8, one), Entry(5, 8, one)}))); },
        "not strictly ascending");
    ExpectThrow<std::runtime_error>(
        [&] { (void)parse(image_with(Entry(16, 8, one))); }, "the chunk has 16 blocks");
    ExpectThrow<std::runtime_error>(
        [&] { (void)parse(image_with(Entry(5, 3, {0x0F}))); }, "past its bit length");
    // An empty or oversized section is not one the writer produces.
    ExpectThrow<std::runtime_error>([&] { (void)parse(image_with({})); }, "has size 0");
    auto oversized = image_with(section);
    {
        // A zrle section declaring a raw size above the hard limit: refused
        // before decompression allocates it (the header CRC is recomputed so
        // only the size is wrong).
        const std::size_t entry_at = chunkdb::kImageFixedHeaderSize + 2U * 16U;
        oversized[entry_at + 2U] = static_cast<std::uint8_t>(chunkdb::kImageSectionFlagZrle);
        for (int i = 0; i < 4; ++i) {
            oversized[entry_at + 8U + i] = static_cast<std::uint8_t>(
                (chunkdb::kExtraMaxChunkBytesLimit + 1U) >> (8 * i));
        }
        const std::size_t crc_at = chunkdb::kImageFixedHeaderSize + 3U * 16U;
        const auto crc = chunkdb::Crc32(oversized.data(), crc_at);
        for (int i = 0; i < 4; ++i) {
            oversized[crc_at + i] = static_cast<std::uint8_t>(crc >> (8 * i));
        }
    }
    ExpectThrow<std::runtime_error>([&] { (void)parse(oversized); }, "(extra data) has size");
    // The section CRC covers it.
    auto damaged = image_with(section);
    damaged.back() ^= 0x01U;
    ExpectThrow<std::runtime_error>([&] { (void)parse(damaged); }, "section 3 checksum mismatch");
}

// --- WAL -------------------------------------------------------------------

using Record = std::pair<std::uint8_t, Bytes>;

Bytes SpanBody(std::uint32_t offset, const Bytes& bytes) {
    Bytes out;
    Le32(&out, offset);
    out.insert(out.end(), bytes.begin(), bytes.end());
    return out;
}

Bytes PutBody(std::uint32_t block_index, const ExtraValue& value) {
    return Entry(block_index, value.bit_length, value.bytes);
}

Bytes DelBody(std::uint32_t block_index) {
    Bytes out;
    Le32(&out, block_index);
    return out;
}

// A frame with any records, so the writer's ordering rules can be broken.
Bytes RawFrame(std::uint64_t revision, const std::vector<Record>& records) {
    Bytes body;
    for (const auto& [type, record] : records) {
        body.push_back(type);
        Le32(&body, static_cast<std::uint32_t>(record.size()));
        body.insert(body.end(), record.begin(), record.end());
    }
    Bytes frame = {'F', 'R', 'M', '2'};
    Le64(&frame, revision);
    Le64(&frame, 1000U + revision);
    Le16(&frame, 0);
    Le16(&frame, 0);
    Le32(&frame, static_cast<std::uint32_t>(records.size()));
    Le32(&frame, static_cast<std::uint32_t>(body.size()));
    Le32(&frame, chunkdb::Crc32(frame.data() + 4, frame.size() - 4));
    frame.insert(frame.end(), body.begin(), body.end());
    Le32(&frame, chunkdb::Crc32(body));
    return frame;
}

struct Replayed {
    chunkdb::WalReplayResult result;
    Bytes payload;
    Bytes presence;
    ChunkExtra extra;
};

Replayed Replay(const Bytes& wal, const chunkdb::FeatureFlags& store_features = kExtraFeature) {
    Replayed out;
    out.payload.assign(kSmallPayloadBytes, 0U);
    out.presence.assign(2, 0U);
    out.result = chunkdb::ReplayWal(
        wal, kSmall, kCoord, kStoreId, store_features, 0, &out.payload, &out.presence, &out.extra);
    return out;
}

Bytes Wal(const chunkdb::FeatureFlags& header_features, const std::vector<Bytes>& frames) {
    auto wal = chunkdb::BuildWalHeader(kCoord, kStoreId, header_features);
    for (const auto& frame : frames) {
        wal.insert(wal.end(), frame.begin(), frame.end());
    }
    return wal;
}

void TestWalExtraRecords() {
    const auto v3 = Val(3, 1);
    const auto v17 = Val(17, 2);
    const auto v9 = Val(9, 3);
    const auto v2 = Val(2, 4);
    ChunkExtra replacement;
    (void)replacement.Put(1, v2);
    // Blocks 0 and 1 become present with values; then block 1 changes and
    // block 0 loses its value; then the section is replaced.
    const auto f1 = RawFrame(
        1,
        {{chunkdb::kWalRecordSpan, SpanBody(kSmallPayloadBytes, {0x03, 0x00})},
         {chunkdb::kWalRecordExtraPut, PutBody(0, v3)},
         {chunkdb::kWalRecordExtraPut, PutBody(1, v17)}});
    const auto f2 = RawFrame(
        2, {{chunkdb::kWalRecordExtraDel, DelBody(0)}, {chunkdb::kWalRecordExtraPut, PutBody(1, v9)}});
    const auto f3 = RawFrame(3, {{chunkdb::kWalRecordExtraReplace, replacement.Encode()}});
    const auto good = Wal(kExtraFeature, {f1, f2, f3});
    {
        const auto r = Replay(good);
        assert(r.result.replayable && !r.result.tail_truncated_or_corrupt);
        assert(r.result.applied_frames == 3U && r.result.revision == 3U);
        assert(r.presence == (Bytes{0x03, 0x00}));
        assert(r.extra == replacement);
        const auto after_two = Replay(Wal(kExtraFeature, {f1, f2}));
        assert(after_two.extra.size() == 1U && *after_two.extra.Find(1) == v9);
    }

    // The writer produces the same records.
    {
        Bytes batch;
        chunkdb::WalFrameBuilder frame(&batch);
        frame.AppendSpan(kSmallPayloadBytes, Bytes{0x03, 0x00}.data(), 2);
        frame.AppendExtraPut(0, v3);
        frame.AppendExtraPut(1, v17);
        (void)frame.Finish(1, 1001);
        assert(batch == f1);
        Bytes replace_batch;
        chunkdb::WalFrameBuilder replace_frame(&replace_batch);
        replace_frame.AppendExtraReplace(replacement);
        (void)replace_frame.Finish(3, 1003);
        assert(replace_batch == f3);
        // Out-of-order records are a writer bug.
        Bytes scratch;
        chunkdb::WalFrameBuilder unordered(&scratch);
        unordered.AppendExtraDel(4);
        ExpectThrow<std::logic_error>([&] { unordered.AppendExtraPut(4, v3); }, "out of order");
        ExpectThrow<std::logic_error>([&] { unordered.AppendExtraReplace(replacement); }, "out of order");
    }

    // A torn frame with extra-data records is ignored as a whole.
    {
        const auto f4 = RawFrame(
            4,
            {{chunkdb::kWalRecordExtraDel, DelBody(1)},
             {chunkdb::kWalRecordSpan, SpanBody(kSmallPayloadBytes, {0x00, 0x00})}});
        for (std::size_t cut = 1; cut < f4.size(); cut += 5) {
            auto torn = good;
            torn.insert(torn.end(), f4.begin(), f4.end() - static_cast<std::ptrdiff_t>(cut));
            const auto r = Replay(torn);
            assert(r.result.tail_truncated_or_corrupt && r.result.stopped_at_crash_tail);
            assert(r.result.applied_frames == 3U && r.extra == replacement);
            assert(r.presence == (Bytes{0x03, 0x00}));
        }
        const auto whole = Replay(Wal(kExtraFeature, {f1, f2, f3, f4}));
        assert(!whole.result.tail_truncated_or_corrupt && whole.extra.empty());
        assert(whole.presence == (Bytes{0x00, 0x00}));
    }

    // Each malformed frame stops replay with nothing of it applied. It is
    // whole and checksum-valid, so the stop is damage, never a crash tail,
    // also as the last frame.
    const auto expect_stop = [&](const Bytes& bad, const std::string& reason) {
        const auto later = RawFrame(9, {{chunkdb::kWalRecordSpan, SpanBody(0, {0x55})}});
        for (const bool last : {true, false}) {
            auto wal = good;
            wal.insert(wal.end(), bad.begin(), bad.end());
            if (!last) {
                wal.insert(wal.end(), later.begin(), later.end());
            }
            const auto r = Replay(wal);
            if (r.result.stop_reason != reason) {
                std::fprintf(stderr, "expected %s, got %s\n", reason.c_str(), r.result.stop_reason.c_str());
                assert(false);
            }
            assert(r.result.tail_truncated_or_corrupt && !r.result.stopped_at_crash_tail);
            assert(r.result.applied_frames == 3U && r.extra == replacement);
            assert(r.presence == (Bytes{0x03, 0x00}) && r.payload == Bytes(kSmallPayloadBytes, 0U));
        }
    };
    expect_stop(
        RawFrame(4, {{chunkdb::kWalRecordExtraPut, PutBody(1, v3)}, {chunkdb::kWalRecordExtraPut, PutBody(0, v3)}}),
        "record_extra_order");
    expect_stop(
        RawFrame(4, {{chunkdb::kWalRecordExtraPut, PutBody(1, v3)}, {chunkdb::kWalRecordExtraDel, DelBody(1)}}),
        "record_extra_order");
    expect_stop(
        RawFrame(4, {{chunkdb::kWalRecordExtraReplace, {}}, {chunkdb::kWalRecordExtraDel, DelBody(1)}}),
        "record_extra_order");
    expect_stop(
        RawFrame(4, {{chunkdb::kWalRecordExtraPut, PutBody(1, v3)}, {chunkdb::kWalRecordExtraReplace, {}}}),
        "record_extra_order");
    expect_stop(RawFrame(4, {{chunkdb::kWalRecordExtraPut, Entry(1, 3, {0x08})}}), "record_extra_invalid");
    expect_stop(RawFrame(4, {{chunkdb::kWalRecordExtraPut, Entry(1, 0, {})}}), "record_extra_invalid");
    expect_stop(RawFrame(4, {{chunkdb::kWalRecordExtraPut, Entry(1, 9, {0x01})}}), "record_extra_invalid");
    expect_stop(RawFrame(4, {{chunkdb::kWalRecordExtraDel, Bytes{1, 0, 0}}}), "record_extra_invalid");
    expect_stop(RawFrame(4, {{chunkdb::kWalRecordExtraDel, DelBody(16)}}), "record_out_of_range");
    expect_stop(
        RawFrame(4, {{chunkdb::kWalRecordExtraReplace, Concat({Entry(1, 8, {1}), Entry(0, 8, {1})})}}),
        "record_extra_invalid");

    // Records overwrite like spans: deleting a missing value is a no-op, and
    // the invariants are checked on the state replay ends in, not per frame
    // (a WAL older than its image passes through states the image never had).
    const auto expect_problem = [&](const Bytes& frame, const std::string& problem) {
        auto wal = good;
        wal.insert(wal.end(), frame.begin(), frame.end());
        const auto r = Replay(wal);
        assert(!r.result.tail_truncated_or_corrupt && r.result.applied_frames == 4U);
        if (!Contains(r.result.extra_problem, problem)) {
            std::fprintf(stderr, "expected '%s', got '%s'\n", problem.c_str(), r.result.extra_problem.c_str());
            assert(false);
        }
    };
    expect_problem(RawFrame(4, {{chunkdb::kWalRecordExtraDel, DelBody(0)}}), "");
    {
        auto wal = good;
        const auto del = RawFrame(4, {{chunkdb::kWalRecordExtraDel, DelBody(0)}});
        wal.insert(wal.end(), del.begin(), del.end());
        assert(Replay(wal).result.extra_problem.empty() && Replay(wal).extra == replacement);
    }
    expect_problem(
        RawFrame(4, {{chunkdb::kWalRecordSpan, SpanBody(0, {0x1F})}, {chunkdb::kWalRecordExtraPut, PutBody(5, v3)}}),
        "absent block index 5");
    // Making block 1 absent without removing its value.
    expect_problem(
        RawFrame(4, {{chunkdb::kWalRecordSpan, SpanBody(kSmallPayloadBytes, {0x01, 0x00})}}), "absent block index 1");
    expect_problem(
        RawFrame(4, {{chunkdb::kWalRecordSpan, SpanBody(0, Bytes(kSmallPayloadBytes + 2U, 0U))}}),
        "absent block index 1");
    expect_problem(RawFrame(4, {{chunkdb::kWalRecordExtraReplace, Entry(7, 8, {1})}}), "absent block index 7");
    // A value on an absent block that a later frame removes is fine.
    {
        auto wal = good;
        for (const auto& frame :
             {RawFrame(4, {{chunkdb::kWalRecordExtraPut, PutBody(9, v3)}}),
              RawFrame(5, {{chunkdb::kWalRecordExtraDel, DelBody(9)}})}) {
            wal.insert(wal.end(), frame.begin(), frame.end());
        }
        const auto r = Replay(wal);
        assert(r.result.extra_problem.empty() && r.extra == replacement);
    }
    // Two values that together pass the chunk limit.
    {
        const std::uint32_t half_bits = static_cast<std::uint32_t>(chunkdb::kExtraMaxChunkBytesLimit / 2U) * 8U;
        const Bytes half(chunkdb::kExtraMaxChunkBytesLimit / 2U, 0x5AU);
        expect_problem(
            RawFrame(
                4,
                {{chunkdb::kWalRecordSpan, SpanBody(kSmallPayloadBytes, {0x03, 0x00})},
                 {chunkdb::kWalRecordExtraPut, Entry(0, half_bits, half)},
                 {chunkdb::kWalRecordExtraPut, Entry(1, half_bits, half)}}),
            "more than 16777216");
    }

    // A WAL replays to the same state over the image a checkpoint made from
    // it (a crash before the WAL was removed) and, for a WAL that ends with
    // the chunk empty, over no image (empty-chunk collection).
    {
        const auto f4 = RawFrame(
            4,
            {{chunkdb::kWalRecordSpan, SpanBody(0, {0x1F, 0x00})},
             {chunkdb::kWalRecordSpan, SpanBody(kSmallPayloadBytes, {0x07, 0x00})},
             {chunkdb::kWalRecordExtraPut, PutBody(0, v17)},
             {chunkdb::kWalRecordExtraPut, PutBody(2, v9)}});
        const auto f5 = RawFrame(
            5, {{chunkdb::kWalRecordSpan, SpanBody(kSmallPayloadBytes, {0x05, 0x00})}, {chunkdb::kWalRecordExtraDel, DelBody(1)}});
        const auto wal = Wal(kExtraFeature, {f1, f2, f3, f4, f5});
        const auto final_state = Replay(wal);
        assert(final_state.result.extra_problem.empty() && final_state.extra.size() == 2U);
        Replayed again;
        again.payload = final_state.payload;
        again.presence = final_state.presence;
        again.extra = final_state.extra;
        again.result = chunkdb::ReplayWal(
            wal, kSmall, kCoord, kStoreId, kExtraFeature, final_state.result.revision, &again.payload,
            &again.presence, &again.extra);
        assert(again.result.extra_problem.empty() && !again.result.tail_truncated_or_corrupt);
        assert(again.result.skipped_frames == 5U && again.result.applied_frames == 0U);
        assert(again.payload == final_state.payload && again.presence == final_state.presence);
        assert(again.extra == final_state.extra);

        const auto emptied = Wal(
            kExtraFeature,
            {f1, f2, f3, f4, f5,
             RawFrame(
                 6,
                 {{chunkdb::kWalRecordSpan, SpanBody(0, Bytes(kSmallPayloadBytes + 2U, 0U))},
                  {chunkdb::kWalRecordExtraReplace, {}}})});
        const auto empty = Replay(emptied);
        assert(empty.result.extra_problem.empty() && empty.extra.empty());
        assert(empty.presence == (Bytes{0x00, 0x00}));
    }
    // Removing the value with the block in one frame is valid.
    {
        const auto r = Replay(Wal(
            kExtraFeature,
            {f1, f2, f3,
             RawFrame(
                 4,
                 {{chunkdb::kWalRecordSpan, SpanBody(kSmallPayloadBytes, {0x01, 0x00})},
                  {chunkdb::kWalRecordExtraDel, DelBody(1)}})}));
        assert(!r.result.tail_truncated_or_corrupt && r.extra.empty());
    }

    // Extra-data records in a store without the feature are damage.
    {
        const auto r = Replay(Wal(kNoFeatures, {f1}), kNoFeatures);
        assert(r.result.stop_reason == "record_extra_disabled" && r.extra.empty());
        assert(r.presence == (Bytes{0x00, 0x00}));
    }

    // A feature this build does not know owns unknown record types even in a
    // WAL whose header predates it (created before the feature was enabled).
    {
        const chunkdb::FeatureFlags unknown{.ro_compat = 1U << 3U};
        const auto frame = RawFrame(
            1, {{chunkdb::kWalRecordSpan, SpanBody(0, {0x1F})}, {9, Bytes{1, 2, 3}}});
        const auto skipped = Replay(Wal(kNoFeatures, {frame}), unknown);
        assert(!skipped.result.tail_truncated_or_corrupt && skipped.payload[0] == 0x1F);
        const auto refused = Replay(Wal(kNoFeatures, {frame}), kNoFeatures);
        assert(refused.result.stop_reason == "record_unknown_type");
    }
}

// --- store -----------------------------------------------------------------

const chunkdb::GeometryConfig kStoreGeometry{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 8,
    .chunk_height_blocks = 8,
    .block_bits = 4,
};

chunkdb::StoreConfig Config(
    const std::filesystem::path& dir,
    std::uint32_t max_block_bits = 4096,
    std::size_t max_chunk_bytes = 4096) {
    chunkdb::StoreConfig config;
    config.geometry = kStoreGeometry;
    config.data_dir = dir;
    config.extra_max_block_bits = max_block_bits;
    config.extra_max_chunk_bytes = max_chunk_bytes;
    return config;
}

void ExpectValue(chunkdb::ChunkStore& store, std::int64_t x, std::int64_t y, const std::optional<ExtraValue>& want) {
    const auto got = store.GetBlockExtra(x, y);
    if (got != want) {
        std::fprintf(
            stderr, "block (%lld,%lld): extra %s, expected %s\n", static_cast<long long>(x),
            static_cast<long long>(y), got ? std::to_string(got->bit_length).c_str() : "none",
            want ? std::to_string(want->bit_length).c_str() : "none");
        assert(false);
    }
}

Bytes StateBytes(chunkdb::ChunkStore& store, std::int64_t cx, std::int64_t cy) {
    return store.GetChunkStateBytes(cx, cy);
}

void TestStoreSemantics() {
    ScopedTempDir dir("chunkdb-extra-semantics");
    chunkdb::ChunkStore store(Config(dir.path(), 64, 200));
    const auto v = Val(12, 1);

    // Extra data belongs to a present block.
    const auto initial = store.GetChunkVersion(0, 0);
    ExpectThrow<std::invalid_argument>([&] { (void)store.PutBlockExtra(1, 1, v); }, "is not set");
    assert(store.GetChunkVersion(0, 0) == initial);
    ExpectValue(store, 1, 1, std::nullopt);

    store.SetBlockBits(1, 1, "1010");
    const auto after_set = store.GetChunkVersion(0, 0);
    const auto put_version = store.PutBlockExtra(1, 1, v);
    assert(put_version > after_set && put_version == store.GetChunkVersion(0, 0));
    ExpectValue(store, 1, 1, v);
    // The same value again changes nothing.
    assert(store.PutBlockExtra(1, 1, v) == put_version);
    // SET keeps it, UNSET removes it, a later SET does not bring it back.
    store.SetBlockBits(1, 1, "0110");
    ExpectValue(store, 1, 1, v);
    assert(store.GetBlockBits(1, 1) == "0110");
    store.UnsetBlock(1, 1);
    ExpectValue(store, 1, 1, std::nullopt);
    store.SetBlockBits(1, 1, "0110");
    ExpectValue(store, 1, 1, std::nullopt);

    // Delete, and delete of nothing.
    (void)store.PutBlockExtra(1, 1, v);
    const auto before_delete = store.GetChunkVersion(0, 0);
    assert(store.DeleteBlockExtra(1, 1) > before_delete);
    ExpectValue(store, 1, 1, std::nullopt);
    const auto deleted = store.GetChunkVersion(0, 0);
    assert(store.DeleteBlockExtra(1, 1) == deleted);
    assert(store.DeleteBlockExtra(5, 5) == deleted);

    // Malformed values and limits are refused before anything changes.
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.PutBlockExtra(1, 1, ExtraValue{.bit_length = 3, .bytes = {0x08}}); },
        "past its bit length");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.PutBlockExtra(1, 1, ExtraValue{.bit_length = 9, .bytes = {0x01}}); },
        "takes 2 bytes");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.PutBlockExtra(1, 1, Val(65, 1)); }, "exceeds extra_max_block_bits (64)");
    (void)store.PutBlockExtra(1, 1, Val(64, 1));
    // 200 bytes: 12 entries of 16 bytes fit, the 13th does not.
    for (int i = 0; i < 11; ++i) {
        store.SetBlockBits(2 + i % 6, 2 + i / 6, "0001");
        (void)store.PutBlockExtra(2 + i % 6, 2 + i / 6, Val(64, static_cast<std::uint8_t>(i)));
    }
    store.SetBlockBits(7, 7, "0001");
    const auto full = store.GetChunkVersion(0, 0);
    const auto full_state = StateBytes(store, 0, 0);
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.PutBlockExtra(7, 7, Val(1, 1)); }, "more than extra_max_chunk_bytes (200)");
    assert(store.GetChunkVersion(0, 0) == full);
    // Replacing a value with one of the same size still fits.
    (void)store.PutBlockExtra(1, 1, Val(64, 99));
    ExpectValue(store, 1, 1, Val(64, 99));
    assert(StateBytes(store, 0, 0) == full_state);

    // GetChunkStateExtraBytes is the state followed by the section.
    {
        const auto bytes = store.GetChunkStateExtraBytes(0, 0);
        const auto state = StateBytes(store, 0, 0);
        assert(bytes.size() > state.size());
        assert(Bytes(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(state.size())) == state);
        const auto extra = DecodeStrict(Bytes(bytes.begin() + static_cast<std::ptrdiff_t>(state.size()), bytes.end()), 64);
        assert(extra.size() == 12U && *extra.Find(9) == Val(64, 99));
        // A chunk without extra data reads as the state alone.
        assert(store.GetChunkStateExtraBytes(5, 5) == StateBytes(store, 5, 5));
    }
}

void TestFullChunkWrites() {
    ScopedTempDir dir("chunkdb-extra-full");
    chunkdb::ChunkStore store(Config(dir.path()));
    const chunkdb::Geometry geometry(kStoreGeometry);
    store.SetBlockBits(0, 0, "0001");
    store.SetBlockBits(1, 0, "0010");
    store.SetBlockBits(2, 0, "0011");
    (void)store.PutBlockExtra(0, 0, Val(5, 1));
    (void)store.PutBlockExtra(1, 0, Val(50, 2));
    (void)store.PutBlockExtra(2, 0, Val(500, 3));

    // Without extra: values of blocks that stay present are kept, values of
    // blocks that become absent are dropped.
    auto payload = store.GetChunkPayloadBytes(0, 0);
    Bytes presence(8, 0U);
    presence[0] = 0x05;  // blocks 0 and 2
    (void)store.SetChunkStateBytes(0, 0, payload, presence);
    ExpectValue(store, 0, 0, Val(5, 1));
    ExpectValue(store, 1, 0, std::nullopt);
    ExpectValue(store, 2, 0, Val(500, 3));
    assert(!store.BlockExists(1, 0));

    // With extra: the section is replaced as a whole.
    ChunkExtra replacement;
    (void)replacement.Put(2, Val(7, 4));
    (void)replacement.Put(9, Val(70, 5));
    presence[1] = 0x02;  // block 9
    const auto before = store.GetChunkVersion(0, 0);
    const auto after = store.SetChunkStateBytes(0, 0, payload, presence, replacement);
    assert(after > before);
    ExpectValue(store, 0, 0, std::nullopt);
    ExpectValue(store, 2, 0, Val(7, 4));
    ExpectValue(store, 1, 1, Val(70, 5));
    // The same write again changes nothing.
    assert(store.SetChunkStateBytes(0, 0, payload, presence, replacement) == after);
    // Only extra data changing is still a write.
    ChunkExtra other = replacement;
    (void)other.Put(9, Val(71, 5));
    assert(store.SetChunkStateBytes(0, 0, payload, presence, other) > after);
    ExpectValue(store, 1, 1, Val(71, 5));

    // A value for a block the state leaves absent, or over a limit, is
    // refused with nothing changed.
    const auto current = store.GetChunkVersion(0, 0);
    const auto current_bytes = store.GetChunkStateExtraBytes(0, 0);
    ChunkExtra stray;
    (void)stray.Put(3, Val(8, 1));
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.SetChunkStateBytes(0, 0, payload, presence, stray); },
        "block index 3, which the chunk state leaves absent");
    ChunkExtra out_of_range;
    (void)out_of_range.Put(64, Val(8, 1));
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.SetChunkStateBytes(0, 0, payload, presence, out_of_range); },
        "block index 64");
    ChunkExtra too_wide;
    (void)too_wide.Put(2, Val(4097, 1));
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.SetChunkStateBytes(0, 0, payload, presence, too_wide); },
        "exceeds extra_max_block_bits");
    assert(store.GetChunkVersion(0, 0) == current);
    assert(store.GetChunkStateExtraBytes(0, 0) == current_bytes);

    // Conditional replace.
    const auto mismatch = store.CasChunkStateBytes(0, 0, current + 1000, payload, presence, ChunkExtra{});
    assert(!mismatch.ok && mismatch.version == current);
    ExpectValue(store, 1, 1, Val(71, 5));
    const auto cas = store.CasChunkStateBytes(0, 0, current, payload, presence, ChunkExtra{});
    assert(cas.ok && cas.version > current);
    ExpectValue(store, 1, 1, std::nullopt);
    ExpectValue(store, 2, 0, std::nullopt);
    // A conditional write without extra drops values of absent blocks too.
    (void)store.PutBlockExtra(2, 0, Val(9, 9));
    presence[0] = 0x01;
    const auto cas_drop =
        store.CasChunkStateBytes(0, 0, store.GetChunkVersion(0, 0), payload, presence);
    assert(cas_drop.ok);
    ExpectValue(store, 2, 0, std::nullopt);
    (void)geometry;
}

chunkdb::ChunkBatchOp SetOp(std::int64_t x, std::int64_t y, std::string bits) {
    return chunkdb::ChunkBatchOp{.set = true, .x = x, .y = y, .bits = std::move(bits)};
}
chunkdb::ChunkBatchOp UnsetOp(std::int64_t x, std::int64_t y) {
    return chunkdb::ChunkBatchOp{.set = false, .x = x, .y = y};
}
chunkdb::ChunkBatchOp XPutOp(std::int64_t x, std::int64_t y, ExtraValue value) {
    return chunkdb::ChunkBatchOp{
        .x = x, .y = y, .kind = chunkdb::ChunkBatchOpKind::kExtraPut, .extra = std::move(value)};
}
chunkdb::ChunkBatchOp XDelOp(std::int64_t x, std::int64_t y) {
    return chunkdb::ChunkBatchOp{.x = x, .y = y, .kind = chunkdb::ChunkBatchOpKind::kExtraDel};
}

void TestBatch() {
    ScopedTempDir dir("chunkdb-extra-batch");
    chunkdb::ChunkStore store(Config(dir.path(), 64, 4096));

    // Set a block and its data in one step.
    auto result = store.ApplyChunkBatch(0, 0, false, 0, {SetOp(3, 3, "1111"), XPutOp(3, 3, Val(10, 1))});
    assert(result.ok);
    ExpectValue(store, 3, 3, Val(10, 1));
    assert(store.GetBlockBits(3, 3) == "1111");

    // Operations apply in order: XPUT needs the block present at that point.
    const auto version = store.GetChunkVersion(0, 0);
    const auto state = store.GetChunkStateExtraBytes(0, 0);
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.ApplyChunkBatch(0, 0, false, 0, {XPutOp(4, 4, Val(1, 1)), SetOp(4, 4, "0001")}); },
        "is not set at its XPUT");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.ApplyChunkBatch(0, 0, false, 0, {SetOp(5, 5, "0001"), UnsetOp(3, 3), XPutOp(3, 3, Val(1, 1))}); },
        "is not set at its XPUT");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.ApplyChunkBatch(0, 0, false, 0, {SetOp(5, 5, "0001"), XPutOp(5, 5, Val(65, 1))}); },
        "exceeds extra_max_block_bits");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.ApplyChunkBatch(0, 0, false, 0, {XPutOp(3, 3, ExtraValue{.bit_length = 2, .bytes = {0x04}})}); },
        "past its bit length");
    assert(store.GetChunkVersion(0, 0) == version);
    assert(store.GetChunkStateExtraBytes(0, 0) == state);

    // Later operations on a block override earlier ones.
    result = store.ApplyChunkBatch(
        0, 0, true, version,
        {XPutOp(3, 3, Val(20, 2)), SetOp(6, 6, "0110"), XPutOp(6, 6, Val(30, 3)), XPutOp(3, 3, Val(21, 4)),
         UnsetOp(6, 6), SetOp(6, 6, "0111")});
    assert(result.ok && result.version > version);
    ExpectValue(store, 3, 3, Val(21, 4));
    ExpectValue(store, 6, 6, std::nullopt);
    assert(store.GetBlockBits(6, 6) == "0111");

    // XDEL, and XDEL of nothing.
    result = store.ApplyChunkBatch(0, 0, false, 0, {XDelOp(3, 3), XDelOp(6, 6)});
    assert(result.ok);
    ExpectValue(store, 3, 3, std::nullopt);
    // A batch that changes nothing keeps the version.
    const auto unchanged = store.GetChunkVersion(0, 0);
    result = store.ApplyChunkBatch(0, 0, false, 0, {XDelOp(3, 3), SetOp(6, 6, "0111")});
    assert(result.ok && result.version == unchanged);
    // A version mismatch changes nothing.
    result = store.ApplyChunkBatch(0, 0, true, unchanged + 1, {XPutOp(6, 6, Val(1, 1))});
    assert(!result.ok && result.version == unchanged);
    ExpectValue(store, 6, 6, std::nullopt);
}

void TestDisabledStore() {
    ScopedTempDir dir("chunkdb-extra-disabled");
    chunkdb::ChunkStore store(Config(dir.path(), 0, chunkdb::kDefaultExtraMaxChunkBytes));
    assert(store.extra_max_block_bits() == 0U && !chunkdb::HasExtraData(store.features()));
    store.SetBlockBits(0, 0, "0001");
    ExpectThrow<std::invalid_argument>([&] { (void)store.PutBlockExtra(0, 0, Val(1, 1)); }, "not enabled");
    ExpectThrow<std::invalid_argument>([&] { (void)store.DeleteBlockExtra(0, 0); }, "not enabled");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.ApplyChunkBatch(0, 0, false, 0, {XDelOp(0, 0)}); }, "not enabled");
    const auto payload = store.GetChunkPayloadBytes(0, 0);
    const Bytes presence(8, 0xFFU);
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.SetChunkStateBytes(0, 0, payload, presence, ChunkExtra{}); }, "not enabled");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)store.CasChunkStateBytes(0, 0, 0, payload, presence, ChunkExtra{}); }, "not enabled");
    // Reads and every other write work.
    ExpectValue(store, 0, 0, std::nullopt);
    assert(store.GetChunkStateExtraBytes(0, 0) == StateBytes(store, 0, 0));
    store.UnsetBlock(0, 0);
    (void)store.SetChunkStateBytes(0, 0, payload, presence);
    assert(store.ApplyChunkBatch(0, 0, false, 0, {UnsetOp(1, 0)}).ok);
}

// Values of many lengths, on many chunks, compared with a model after every
// way a chunk leaves memory and comes back.
using Model = std::map<std::pair<std::int64_t, std::int64_t>, ExtraValue>;

void CheckModel(chunkdb::ChunkStore& store, const Model& model) {
    for (const auto& [coord, value] : model) {
        ExpectValue(store, coord.first, coord.second, value);
        assert(store.BlockExists(coord.first, coord.second));
    }
}

Model WriteLengths(chunkdb::ChunkStore& store, std::uint32_t max_bits) {
    const std::vector<std::uint32_t> lengths = {1, 2, 7, 8, 9, 15, 16, 17, 63, 64, 65, 255, 256, 1000, max_bits};
    Model model;
    std::uint8_t seed = 1;
    for (std::int64_t chunk = 0; chunk < 6; ++chunk) {
        const std::int64_t base_x = chunk * 8 - 16;
        for (std::size_t i = 0; i < lengths.size(); ++i) {
            const std::int64_t x = base_x + static_cast<std::int64_t>(i % 8);
            const std::int64_t y = static_cast<std::int64_t>(i / 8) * 3 - 4;
            store.SetBlockBits(x, y, "1001");
            const auto value = Val(lengths[(i + static_cast<std::size_t>(chunk)) % lengths.size()], seed++);
            (void)store.PutBlockExtra(x, y, value);
            model[{x, y}] = value;
        }
        // Some values change, some go, some go with their block.
        const std::int64_t x = base_x;
        (void)store.PutBlockExtra(x, -4, Val(33, seed));
        model[{x, -4}] = Val(33, seed++);
        (void)store.DeleteBlockExtra(x + 1, -4);
        model.erase({x + 1, -4});
        store.UnsetBlock(x + 2, -4);
        model.erase({x + 2, -4});
    }
    return model;
}

void TestDurability() {
    constexpr std::uint32_t kMaxBits = 8000;
    for (const auto compression :
         {chunkdb::CheckpointCompression::kNone, chunkdb::CheckpointCompression::kZrle}) {
        for (const std::size_t checkpoint_updates : {std::size_t{1}, std::size_t{7}, std::size_t{100000}}) {
            ScopedTempDir dir("chunkdb-extra-durability");
            auto config = Config(dir.path() / "data", kMaxBits, 16384);
            config.checkpoint_compression = compression;
            config.checkpoint_update_interval = checkpoint_updates;
            config.checkpoint_wal_bytes = 1U << 30U;
            config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
            Model model;
            {
                chunkdb::ChunkStore store(config);
                model = WriteLengths(store, kMaxBits);
                CheckModel(store, model);

                // A crash: the files as they are now, with the store still open.
                store.WalBarrier();
                CopyCrashImage(dir.path() / "data", dir.path() / "crash");
            }
            // Restart.
            {
                chunkdb::ChunkStore store(config);
                CheckModel(store, model);
            }
            {
                auto crash_config = config;
                crash_config.data_dir = dir.path() / "crash";
                chunkdb::ChunkStore store(crash_config);
                CheckModel(store, model);
            }
            // Eviction: a cache of two chunks.
            {
                auto small = config;
                small.max_loaded_chunks = 2;
                chunkdb::ChunkStore store(small);
                for (int round = 0; round < 2; ++round) {
                    CheckModel(store, model);
                }
                assert(store.RuntimeStats().evictions > 0U);
                // Writes through eviction too.
                const auto& [coord, value] = *model.begin();
                (void)store.PutBlockExtra(coord.first, coord.second, Val(kMaxBits, 77));
                model[coord] = Val(kMaxBits, 77);
                CheckModel(store, model);
            }
            {
                chunkdb::ChunkStore store(config);
                CheckModel(store, model);
            }
            // Read-only opens read it the same way.
            {
                auto read_only = config;
                read_only.access_mode = chunkdb::AccessMode::kReadOnly;
                chunkdb::ChunkStore store(read_only);
                CheckModel(store, model);
            }
        }
    }
}

// Writers on shared chunks, each owning its own blocks, with readers and a
// cache small enough to evict: every writer's final values are there, in
// memory and after a restart.
void TestConcurrentWriters() {
    ScopedTempDir dir("chunkdb-extra-concurrent");
    auto config = Config(dir.path(), 256, 16384);
    config.max_loaded_chunks = 3;
    config.checkpoint_update_interval = 5;
    constexpr int kThreads = 6;
    constexpr int kRounds = 300;
    std::vector<Model> models(kThreads);
    {
        chunkdb::ChunkStore store(config);
        std::atomic<bool> stop{false};
        std::vector<std::thread> threads;
        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&, t] {
                auto& model = models[static_cast<std::size_t>(t)];
                for (int round = 0; round < kRounds; ++round) {
                    // Thread t owns column t of every chunk in a 4x1 strip.
                    const std::int64_t x = (round % 4) * 8 + t;
                    const std::int64_t y = (round / 4) % 8;
                    const auto seed = static_cast<std::uint8_t>(round + t);
                    const auto value = Val(1 + static_cast<std::uint32_t>((round * 37 + t) % 256), seed);
                    switch (round % 5) {
                        case 0:
                        case 1:
                            store.SetBlockBits(x, y, "0101");
                            (void)store.PutBlockExtra(x, y, value);
                            model[{x, y}] = value;
                            break;
                        case 2:
                            assert(store.ApplyChunkBatch(
                                           x / 8, 0, false, 0, {SetOp(x, y, "0011"), XPutOp(x, y, value)})
                                       .ok);
                            model[{x, y}] = value;
                            break;
                        case 3:
                            if (model.count({x, y}) != 0U) {
                                (void)store.DeleteBlockExtra(x, y);
                                model.erase({x, y});
                            }
                            break;
                        default:
                            store.UnsetBlock(x, y);
                            model.erase({x, y});
                            break;
                    }
                }
            });
        }
        std::thread reader([&] {
            while (!stop.load()) {
                for (std::int64_t cx = 0; cx < 4; ++cx) {
                    const auto bytes = store.GetChunkStateExtraBytes(cx, 0);
                    const std::size_t state = 32U + 8U;
                    assert(bytes.size() >= state);
                    const auto extra = DecodeStrict(Bytes(bytes.begin() + state, bytes.end()), 64);
                    for (const auto entry : extra) {
                        // Every value belongs to a present block in the same read.
                        assert((bytes[32U + entry.block_index / 8U] >> (entry.block_index % 8U)) & 1U);
                    }
                }
            }
        });
        for (auto& thread : threads) {
            thread.join();
        }
        stop.store(true);
        reader.join();
        for (const auto& model : models) {
            CheckModel(store, model);
        }
        // Eviction skips a chunk in use, so whether one happened under the
        // writers depends on scheduling. A fifth chunk evicts the idle ones
        // now, and every value must come back from disk.
        store.SetBlockBits(1000, 0, "0001");
        assert(store.RuntimeStats().evictions > 0U);
        for (const auto& model : models) {
            CheckModel(store, model);
        }
    }
    chunkdb::ChunkStore store(config);
    for (const auto& model : models) {
        CheckModel(store, model);
    }
}

// The windows where a WAL outlives the image made from it: a checkpoint
// that published its image but could not remove the WAL (crash, or a failed
// removal followed by eviction and reload), and empty-chunk collection that
// removed the image but not the WAL. Replaying the old WAL over what is
// there must give the state the store had.
void TestStaleWalWindows() {
    const auto config_of = [](const std::filesystem::path& dir) {
        auto config = Config(dir, 4096, 4096);
        config.checkpoint_update_interval = 3;
        config.checkpoint_wal_bytes = 1U << 30U;
        config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
        return config;
    };
    struct Scenario {
        const char* name;
        std::function<void(chunkdb::ChunkStore&)> before_failure;
        const char* failpoint;
        std::function<void(chunkdb::ChunkStore&)> failing_write;
    };
    const std::vector<Scenario> scenarios = {
        {"xdel then writes",
         [](chunkdb::ChunkStore& store) {
             (void)store.DeleteBlockExtra(0, 0);
             store.SetBlockBits(2, 0, "0110");
             store.SetBlockBits(5, 0, "0101");
         },
         "CHUNKDB_FAILPOINT_CHECKPOINT_WAL_REMOVE_FAIL_ONCE",
         [](chunkdb::ChunkStore& store) { store.SetBlockBits(3, 0, "0011"); }},
        {"xput then unset",
         [](chunkdb::ChunkStore& store) {
             (void)store.PutBlockExtra(0, 0, Val(20, 0x11));
             store.UnsetBlock(0, 0);
             store.SetBlockBits(5, 0, "0101");
         },
         "CHUNKDB_FAILPOINT_CHECKPOINT_WAL_REMOVE_FAIL_ONCE",
         [](chunkdb::ChunkStore& store) { store.SetBlockBits(3, 0, "0011"); }},
        {"set and unset with value",
         [](chunkdb::ChunkStore& store) {
             store.SetBlockBits(6, 0, "0001");
             (void)store.PutBlockExtra(6, 0, Val(9, 3));
         },
         "CHUNKDB_FAILPOINT_CHECKPOINT_WAL_REMOVE_FAIL_ONCE",
         [](chunkdb::ChunkStore& store) { store.UnsetBlock(6, 0); }},
        {"empty-chunk collection, value removed first",
         [](chunkdb::ChunkStore& store) {
             store.UnsetBlock(1, 0);
             store.UnsetBlock(0, 0);
         },
         "CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_IMAGE_REMOVE_ONCE",
         [](chunkdb::ChunkStore& store) { store.UnsetBlock(4, 0); }},
        {"empty-chunk collection, value removed last",
         [](chunkdb::ChunkStore& store) {
             store.UnsetBlock(1, 0);
             store.UnsetBlock(4, 0);
         },
         "CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_IMAGE_REMOVE_ONCE",
         [](chunkdb::ChunkStore& store) { store.UnsetBlock(0, 0); }},
    };
    for (const auto& scenario : scenarios) {
        for (const bool evict : {false, true}) {
            ScopedTempDir dir("chunkdb-extra-stale-wal");
            const auto data = dir.path() / "data";
            Bytes expected;
            std::uint64_t expected_version = 0;
            {
                auto config = config_of(data);
                config.max_loaded_chunks = 1;
                chunkdb::ChunkStore store(config);
                // The first image holds block (0,0) and its value.
                store.SetBlockBits(0, 0, "1001");
                (void)store.PutBlockExtra(0, 0, Val(13, 0x5A));
                store.SetBlockBits(1, 0, "1111");
                store.SetBlockBits(4, 0, "1111");
                scenario.before_failure(store);
                SetEnvVar(scenario.failpoint, "1");
                try {
                    scenario.failing_write(store);
                } catch (const std::exception&) {
                    // The write is committed; only the WAL removal failed.
                }
                UnsetEnvVar(scenario.failpoint);
                expected = store.GetChunkStateExtraBytes(0, 0);
                expected_version = store.GetChunkVersion(0, 0);
                store.WalBarrier();
                CopyCrashImage(data, dir.path() / "crash");
                if (evict) {
                    // Another chunk pushes this one out; it comes back from
                    // the image and the WAL left next to it.
                    store.SetBlockBits(100, 100, "0001");
                    (void)store.GetBlockBits(100, 100);
                    assert(!store.IsChunkLoadedForTests(0, 0));
                    if (store.GetChunkStateExtraBytes(0, 0) != expected) {
                        std::fprintf(stderr, "%s: wrong state after reload\n", scenario.name);
                        assert(false);
                    }
                    assert(store.GetChunkVersion(0, 0) == expected_version);
                }
            }
            for (const auto& reopened : {data, dir.path() / "crash"}) {
                for (const bool read_only : {false, true}) {
                    auto config = config_of(reopened);
                    if (read_only) {
                        config.access_mode = chunkdb::AccessMode::kReadOnly;
                    }
                    chunkdb::ChunkStore store(config);
                    if (store.GetChunkStateExtraBytes(0, 0) != expected) {
                        std::fprintf(stderr, "%s: wrong state after reopen\n", scenario.name);
                        assert(false);
                    }
                    if (!read_only) {
                        assert(store.GetChunkVersion(0, 0) == expected_version || !store.ChunkExists(0, 0));
                    }
                }
            }
        }
    }
}

// A full-chunk write that changes thousands of values costs one pass, not
// one per value.
void TestLargeUpdates() {
    ScopedTempDir dir("chunkdb-extra-large");
    auto config = Config(dir.path(), 64, 1U << 20U);
    config.geometry.chunk_width_blocks = 256;
    config.geometry.chunk_height_blocks = 256;
    const chunkdb::Geometry geometry(config.geometry);
    chunkdb::ChunkStore store(config);
    const std::size_t blocks = geometry.ChunkBlockCount();
    Bytes payload(geometry.ChunkPayloadBytes(), 0x11U);
    Bytes presence((blocks + 7U) / 8U, 0xFFU);
    ChunkExtra all;
    for (std::uint32_t i = 0; i < blocks; ++i) {
        all.Assign(i, Val(9, static_cast<std::uint8_t>(i)));
    }
    const auto started = std::chrono::steady_clock::now();
    (void)store.SetChunkStateBytes(0, 0, payload, presence, all);
    // Half the blocks go, and their values with them.
    for (std::size_t i = 0; i < presence.size(); i += 2) {
        presence[i] = 0;
    }
    (void)store.SetChunkStateBytes(0, 0, payload, presence);
    std::vector<chunkdb::ChunkBatchOp> ops;
    for (std::int64_t x = 0; x < 256 && ops.size() < chunkdb::kMaxChunkBatchOps; x += 1) {
        ops.push_back(XDelOp(x, 1));
    }
    assert(store.ApplyChunkBatch(0, 0, false, 0, ops).ok);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    std::printf(
        "large updates: %lld ms\n",
        static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()));
    std::size_t expected_values = 0;
    for (std::uint32_t i = 0; i < blocks; ++i) {
        const bool present = ((presence[i / 8U] >> (i % 8U)) & 1U) != 0U;
        const bool deleted = i / 256U == 1U;
        const auto value = store.GetBlockExtra(static_cast<std::int64_t>(i % 256U), static_cast<std::int64_t>(i / 256U));
        assert(value.has_value() == (present && !deleted));
        expected_values += present && !deleted ? 1U : 0U;
    }
    const auto bytes = store.GetChunkStateExtraBytes(0, 0);
    assert(bytes.size() == geometry.ChunkPayloadBytes() + presence.size() + expected_values * 10U);
    store.WalBarrier();
    auto read_only = config;
    read_only.access_mode = chunkdb::AccessMode::kReadOnly;
    chunkdb::ChunkStore reopened(read_only);
    assert(reopened.GetChunkStateExtraBytes(0, 0) == bytes);
}

// Enabling extra data whose manifest write cannot be made durable takes the
// table out of service: extra data written then could not be read with the
// manifest a crash might bring back.
void TestEnableNotDurable() {
    ScopedTempDir dir("chunkdb-extra-enable-sync");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    config.default_geometry = kStoreGeometry;
    {
        chunkdb::TableCatalog catalog(config);
        chunkdb::TableOptionsUpdate enable;
        enable.extra_max_block_bits = 16;
        SetEnvVar("CHUNKDB_FAILPOINT_TABLESET_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
        ExpectThrow<std::runtime_error>([&] { catalog.SetOptions("default", enable); }, "injected");
        UnsetEnvVar("CHUNKDB_FAILPOINT_TABLESET_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
        assert(catalog.Find("default") == nullptr);
        // Other options keep serving what the manifest says.
        chunkdb::TableOptions plain;
        (void)catalog.Create("other", kStoreGeometry, plain);
        chunkdb::TableOptionsUpdate interval;
        interval.checkpoint_update_interval = 99;
        SetEnvVar("CHUNKDB_FAILPOINT_TABLESET_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
        ExpectThrow<std::runtime_error>([&] { catalog.SetOptions("other", interval); }, "injected");
        UnsetEnvVar("CHUNKDB_FAILPOINT_TABLESET_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
        assert(catalog.Find("other") != nullptr);
        assert(catalog.Find("other")->Info().options.checkpoint_update_interval == 99U);
    }
    // A restart serves whichever manifest is on disk: here the new one.
    chunkdb::TableCatalog catalog(config);
    assert(catalog.Find("default")->Info().options.extra_max_block_bits == 16U);
}

// A failed write leaves memory and disk as they were, on the ordinary and
// the conditional path.
void TestRollback() {
    ScopedTempDir dir("chunkdb-extra-rollback");
    auto config = Config(dir.path(), 64, 4096);
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.checkpoint_update_interval = 100000;
    Bytes state_before;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "0001");
        store.SetBlockBits(1, 0, "0010");
        (void)store.PutBlockExtra(0, 0, Val(10, 1));
        (void)store.PutBlockExtra(1, 0, Val(20, 2));
        state_before = store.GetChunkStateExtraBytes(0, 0);
        const auto version = store.GetChunkVersion(0, 0);

        const std::vector<std::pair<std::string, std::function<void()>>> ordinary = {
            {"put", [&] { (void)store.PutBlockExtra(0, 0, Val(11, 3)); }},
            {"delete", [&] { (void)store.DeleteBlockExtra(1, 0); }},
            {"unset", [&] { store.UnsetBlock(1, 0); }},
            {"replace",
             [&] {
                 ChunkExtra replacement;
                 (void)replacement.Put(0, Val(3, 3));
                 (void)store.SetChunkStateBytes(
                     0, 0, store.GetChunkPayloadBytes(0, 0), Bytes(8, 0xFFU), replacement);
             }},
            {"drop",
             [&] {
                 Bytes presence(8, 0U);
                 presence[0] = 0x01;
                 (void)store.SetChunkStateBytes(0, 0, store.GetChunkPayloadBytes(0, 0), presence);
             }},
        };
        for (const auto& [name, write] : ordinary) {
            SetEnvVar("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
            ExpectThrow([&] { write(); }, "");
            UnsetEnvVar("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE");
            if (store.GetChunkStateExtraBytes(0, 0) != state_before || store.GetChunkVersion(0, 0) != version) {
                std::fprintf(stderr, "ordinary %s was not rolled back\n", name.c_str());
                assert(false);
            }
        }
        const std::vector<std::pair<std::string, std::function<void()>>> conditional = {
            {"batch",
             [&] {
                 (void)store.ApplyChunkBatch(
                     0, 0, false, 0, {XPutOp(0, 0, Val(12, 4)), XDelOp(1, 0), SetOp(2, 0, "0011")});
             }},
            {"cas",
             [&] {
                 ChunkExtra replacement;
                 (void)replacement.Put(1, Val(5, 5));
                 (void)store.CasChunkStateBytes(
                     0, 0, version, store.GetChunkPayloadBytes(0, 0), Bytes(8, 0xFFU), replacement);
             }},
        };
        for (const auto& [name, write] : conditional) {
            SetEnvVar("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE", "1");
            ExpectThrow([&] { write(); }, "injected");
            UnsetEnvVar("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE");
            if (store.GetChunkStateExtraBytes(0, 0) != state_before || store.GetChunkVersion(0, 0) != version) {
                std::fprintf(stderr, "conditional %s was not rolled back\n", name.c_str());
                assert(false);
            }
        }
        // The store still works.
        (void)store.PutBlockExtra(0, 0, Val(10, 1));
        assert(store.GetChunkStateExtraBytes(0, 0) == state_before);
    }
    // Nothing of the failed writes comes back.
    chunkdb::ChunkStore store(config);
    assert(store.GetChunkStateExtraBytes(0, 0) == state_before);
}

// --- options and the feature flag --------------------------------------------

void TestFeatureAndOptions() {
    // A new store with extra data records the feature and the limits.
    {
        ScopedTempDir dir("chunkdb-extra-feature");
        { chunkdb::ChunkStore store(Config(dir.path(), 100, 5000)); }
        const auto manifest = chunkdb::ReadStoreManifest(dir.path());
        assert(manifest.has_value() && chunkdb::HasExtraData(manifest->features));
        const auto options = chunkdb::DecodeTableOptions(manifest->options);
        assert(options.extra_max_block_bits == 100U && options.extra_max_chunk_bytes == 5000U);
        // It opens only with extra data enabled.
        ExpectThrow<std::invalid_argument>(
            [&] { chunkdb::ChunkStore store(Config(dir.path(), 0, chunkdb::kDefaultExtraMaxChunkBytes)); },
            "has extra data");
        { chunkdb::ChunkStore store(Config(dir.path(), 200, 9000)); }
    }
    {
        ScopedTempDir dir("chunkdb-extra-nofeature");
        { chunkdb::ChunkStore store(Config(dir.path(), 0, chunkdb::kDefaultExtraMaxChunkBytes)); }
        const auto manifest = chunkdb::ReadStoreManifest(dir.path());
        assert(manifest->features.ro_compat == 0U);
        // Options without extra data hold no extra-data entries.
        assert(manifest->options == chunkdb::EncodeTableOptions(chunkdb::TableOptions{}));
        ExpectThrow<std::invalid_argument>(
            [&] { chunkdb::ChunkStore store(Config(dir.path(), 64, 4096)); }, "has no extra data");
        ExpectThrow<std::invalid_argument>(
            [&] { chunkdb::ChunkStore store(Config(dir.path(), 64, 9)); }, "must hold one value");
    }

    // The manifest must have the limits exactly when it has the feature.
    {
        chunkdb::TableOptions with_extra;
        with_extra.extra_max_block_bits = 8;
        chunkdb::StoreManifest manifest{
            .features = kNoFeatures,
            .geometry = kStoreGeometry,
            .store_id = kStoreId,
            .options = chunkdb::EncodeTableOptions(with_extra),
            .schema = chunkdb::SingleBitsColumnSchema(kStoreGeometry.block_bits),
        };
        ExpectThrow<std::runtime_error>(
            [&] { (void)chunkdb::ParseStoreManifest(chunkdb::SerializeStoreManifest(manifest)); },
            "extra-data limits without the extra-data feature");
        manifest.features = kExtraFeature;
        (void)chunkdb::ParseStoreManifest(chunkdb::SerializeStoreManifest(manifest));
        manifest.options = chunkdb::EncodeTableOptions(chunkdb::TableOptions{});
        ExpectThrow<std::runtime_error>(
            [&] { (void)chunkdb::ParseStoreManifest(chunkdb::SerializeStoreManifest(manifest)); },
            "extra-data feature without extra-data limits");
        // One limit without the other, or limits that do not fit.
        Bytes only_bits = chunkdb::EncodeTableOptions(chunkdb::TableOptions{});
        Le16(&only_bits, chunkdb::kOptionExtraMaxBlockBits);
        Le16(&only_bits, 8);
        Le64(&only_bits, 8);
        manifest.options = only_bits;
        ExpectThrow<std::runtime_error>(
            [&] { (void)chunkdb::ParseStoreManifest(chunkdb::SerializeStoreManifest(manifest)); },
            "must appear together");
        Bytes misfit = only_bits;
        Le16(&misfit, chunkdb::kOptionExtraMaxChunkBytes);
        Le16(&misfit, 8);
        Le64(&misfit, 8);
        manifest.options = misfit;
        ExpectThrow<std::runtime_error>(
            [&] { (void)chunkdb::ParseStoreManifest(chunkdb::SerializeStoreManifest(manifest)); },
            "between 9");
    }

    // Tables: created with or without it; enabled later with the feature in
    // the same manifest write; never disabled.
    ScopedTempDir dir("chunkdb-extra-catalog");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    config.default_geometry = kStoreGeometry;
    {
        chunkdb::TableCatalog catalog(config);
        chunkdb::TableOptions with_extra;
        with_extra.extra_max_block_bits = 16;
        with_extra.extra_max_chunk_bytes = 1000;
        (void)catalog.Create("withextra", kStoreGeometry, with_extra);
        assert(chunkdb::HasExtraData(
            chunkdb::ReadStoreManifest(dir.path() / "tables" / "withextra")->features));
        chunkdb::TableOptions chunk_bytes_only;
        chunk_bytes_only.extra_max_chunk_bytes = 1000;
        ExpectThrow<std::invalid_argument>(
            [&] { (void)catalog.Create("bad", kStoreGeometry, chunk_bytes_only); },
            "applies only to a table with extra data");

        const auto default_dir = dir.path() / "tables" / "default";
        assert(!chunkdb::HasExtraData(chunkdb::ReadStoreManifest(default_dir)->features));
        {
            auto lease = catalog.Find("default")->Acquire();
            lease->store().SetBlockBits(0, 0, "0001");
            ExpectThrow<std::invalid_argument>(
                [&] { (void)lease->store().PutBlockExtra(0, 0, Val(4, 1)); }, "not enabled");
        }
        chunkdb::TableOptionsUpdate chunk_bytes;
        chunk_bytes.extra_max_chunk_bytes = 64;
        ExpectThrow<std::invalid_argument>(
            [&] { catalog.SetOptions("default", chunk_bytes); }, "applies only to a table with extra data");
        chunkdb::TableOptionsUpdate enable;
        enable.extra_max_block_bits = 32;
        catalog.SetOptions("default", enable);
        const auto manifest = chunkdb::ReadStoreManifest(default_dir);
        assert(chunkdb::HasExtraData(manifest->features));
        assert(chunkdb::DecodeTableOptions(manifest->options).extra_max_block_bits == 32U);
        {
            auto lease = catalog.Find("default")->Acquire();
            assert(lease->store().GetBlockBits(0, 0) == "0001");
            (void)lease->store().PutBlockExtra(0, 0, Val(32, 1));
            (void)lease->store().PutBlockExtra(0, 0, Val(31, 1));
        }
        chunkdb::TableOptionsUpdate disable;
        disable.extra_max_block_bits = 0;
        ExpectThrow<std::invalid_argument>(
            [&] { catalog.SetOptions("default", disable); }, "cannot be disabled");
        // Limits only grow; a refused change leaves the options as they were.
        chunkdb::TableOptionsUpdate lower;
        lower.extra_max_block_bits = 8;
        ExpectThrow<std::invalid_argument>([&] { catalog.SetOptions("default", lower); }, "can only be raised");
        chunkdb::TableOptionsUpdate lower_bytes;
        lower_bytes.extra_max_chunk_bytes = 4096;
        ExpectThrow<std::invalid_argument>([&] { catalog.SetOptions("default", lower_bytes); }, "can only be raised");
        chunkdb::TableOptionsUpdate raise;
        raise.extra_max_block_bits = 40;
        raise.extra_max_chunk_bytes = 70000;
        catalog.SetOptions("default", raise);
        {
            auto lease = catalog.Find("default")->Acquire();
            auto& store = lease->store();
            ExpectValue(store, 0, 0, Val(31, 1));
            (void)store.PutBlockExtra(0, 0, Val(40, 1));
            ExpectThrow<std::invalid_argument>(
                [&] { (void)store.PutBlockExtra(0, 0, Val(41, 1)); }, "exceeds extra_max_block_bits (40)");
            store.SetBlockBits(1, 0, "0001");
            (void)store.PutBlockExtra(1, 0, Val(1, 1));
        }
    }
    // The options and the feature survive a restart.
    chunkdb::TableCatalog catalog(config);
    const auto info = catalog.Find("default")->Info();
    assert(info.options.extra_max_block_bits == 40U && info.options.extra_max_chunk_bytes == 70000U);
    ExpectValue(catalog.Find("default")->Acquire()->store(), 1, 0, Val(1, 1));
    ExpectValue(catalog.Find("default")->Acquire()->store(), 0, 0, Val(40, 1));

    // A store opened directly with limits below what it holds (no catalog
    // to refuse the change) keeps the data, and writes may shrink a chunk
    // that is over them but not grow it.
    {
        ScopedTempDir standalone("chunkdb-extra-lowered");
        {
            chunkdb::ChunkStore store(Config(standalone.path(), 64, 4096));
            store.SetBlockBits(0, 0, "0001");
            store.SetBlockBits(1, 0, "0001");
            (void)store.PutBlockExtra(0, 0, Val(64, 1));
            (void)store.PutBlockExtra(1, 0, Val(64, 2));
        }
        chunkdb::ChunkStore store(Config(standalone.path(), 8, 17));
        ExpectValue(store, 0, 0, Val(64, 1));
        ExpectThrow<std::invalid_argument>(
            [&] { (void)store.PutBlockExtra(0, 0, Val(64, 3)); }, "exceeds extra_max_block_bits (8)");
        (void)store.PutBlockExtra(0, 0, Val(8, 3));
        (void)store.DeleteBlockExtra(1, 0);
        store.SetBlockBits(2, 0, "0001");
        ExpectThrow<std::invalid_argument>(
            [&] { (void)store.PutBlockExtra(2, 0, Val(8, 1)); }, "more than extra_max_chunk_bytes (17)");
    }
}

// A build that does not know a ro_compat feature (as a build without extra
// data would treat this one) opens the store read-only, reads payload and
// presence, and skips the sections and records the feature owns; it refuses
// to open it for writing.
void TestUnknownFeatureReadOnly() {
    ScopedTempDir dir("chunkdb-extra-unknown");
    auto config = Config(dir.path(), 0, chunkdb::kDefaultExtraMaxChunkBytes);
    config.checkpoint_update_interval = 1;
    const chunkdb::Geometry geometry(kStoreGeometry);
    chunkdb::StoreId store_id{};
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(1, 1, "0101");
        store_id = store.store_id();
    }
    const chunkdb::FeatureFlags unknown{.ro_compat = 1U << 3U};
    {
        const auto path = chunkdb::StoreManifestPath(dir.path());
        auto manifest = chunkdb::ParseStoreManifest(ReadFile(path));
        manifest.features = unknown;
        WriteFile(path, chunkdb::SerializeStoreManifest(manifest));
    }
    // The image gains a section of the unknown feature (an image names the
    // features it uses), and the WAL, whose header predates the feature, a
    // frame with a record of it.
    const auto image_path = chunkdb::ChunkDataPath(dir.path(), geometry, {0, 0});
    const auto image = chunkdb::ParseChunkImage(ReadFile(image_path), geometry, {0, 0}, store_id, unknown);
    WriteFile(
        image_path,
        BuildImage(
            unknown, {Raw(1, image.payload), Raw(2, image.presence_bitmap), Raw(9, {7, 7, 7})}, store_id,
            {0, 0}, image.revision));
    auto wal = chunkdb::BuildWalHeader({0, 0}, store_id, kNoFeatures);
    // Block (2, 1) = index 10: payload bits 40..43 (byte 5, low nibble),
    // presence byte 1 bit 2.
    const auto frame = RawFrame(
        image.revision + 1,
        {{chunkdb::kWalRecordSpan, SpanBody(5, {static_cast<std::uint8_t>(image.payload[5] | 0x09U)})},
         {chunkdb::kWalRecordSpan,
          SpanBody(32U + 1U, {static_cast<std::uint8_t>(image.presence_bitmap[1] | 0x04U)})},
         {9, {1, 2, 3, 4}}});
    wal.insert(wal.end(), frame.begin(), frame.end());
    WriteFile(chunkdb::ChunkWalPath(dir.path(), geometry, {0, 0}), wal);

    ExpectThrow<std::runtime_error>(
        [&] {
            (void)chunkdb::ParseChunkImage(
                BuildImage(
                    kNoFeatures, {Raw(1, image.payload), Raw(2, image.presence_bitmap), Raw(9, {7})}, store_id,
                    {0, 0}, image.revision),
                geometry, {0, 0}, store_id, unknown);
        },
        "unknown chunk image section 9");
    ExpectThrow<std::runtime_error>([&] { chunkdb::ChunkStore store(config); }, "only be opened read-only");
    auto read_only = config;
    read_only.access_mode = chunkdb::AccessMode::kReadOnly;
    chunkdb::ChunkStore store(read_only);
    assert(store.GetBlockBits(1, 1) == "0101");
    assert(store.GetBlockBits(2, 1) == "1001");
    ExpectThrow<std::invalid_argument>([&] { store.SetBlockBits(3, 3, "0001"); }, "read-only");
}

// --- verify ------------------------------------------------------------------

void TestVerifyFindsDamagedExtraData() {
    ScopedTempDir dir("chunkdb-extra-verify");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    config.default_geometry = kStoreGeometry;
    config.default_options.extra_max_block_bits = 64;
    config.default_options.checkpoint_update_interval = 1;
    {
        chunkdb::TableCatalog catalog(config);
        auto lease = catalog.Find("default")->Acquire();
        auto& store = lease->store();
        store.SetBlockBits(0, 0, "0001");
        store.SetBlockBits(3, 0, "0001");
        (void)store.PutBlockExtra(0, 0, Val(9, 1));
        (void)store.PutBlockExtra(3, 0, Val(40, 2));
    }
    const auto run_verify = [&] {
        std::ostringstream out;
        const auto counters = chunkdb::VerifyDataDirectory(dir.path(), out);
        return std::make_pair(counters, out.str());
    };
    {
        const auto [counters, out] = run_verify();
        if (counters.errors != 0U || counters.warnings != 0U) {
            std::fprintf(stderr, "%s", out.c_str());
            assert(false);
        }
    }
    const chunkdb::Geometry geometry(kStoreGeometry);
    const auto table_dir = dir.path() / "tables" / "default";
    const auto manifest = chunkdb::ReadStoreManifest(table_dir);
    const auto image_path = chunkdb::ChunkDataPath(table_dir, geometry, {0, 0});
    const auto original = ReadFile(image_path);
    const auto image =
        chunkdb::ParseChunkImage(original, geometry, {0, 0}, manifest->store_id, manifest->features);
    assert(image.extra.size() == 2U);
    const Bytes one = {0x01};
    const std::vector<std::pair<Bytes, std::string>> damages = {
        {Concat({Entry(3, 8, one), Entry(0, 8, one)}), "not strictly ascending"},
        {Concat({Entry(3, 8, one), Entry(3, 8, one)}), "not strictly ascending"},
        {Concat({Entry(0, 8, one), Entry(4, 8, one)}), "absent block index 4"},
        {Entry(0, 4, {0x30}), "past its bit length"},
    };
    for (const auto& [section, reason] : damages) {
        WriteFile(
            image_path,
            BuildImage(
                kExtraFeature, {Raw(1, image.payload), Raw(2, image.presence_bitmap), Raw(3, section)},
                manifest->store_id, {0, 0}, image.revision));
        const auto [counters, out] = run_verify();
        if (counters.errors == 0U || !Contains(out, "chunk_image_invalid") || !Contains(out, reason)) {
            std::fprintf(stderr, "verify missed '%s':\n%s", reason.c_str(), out.c_str());
            assert(false);
        }
    }
    WriteFile(image_path, original);

    // WALs: one that leaves a value on a block it makes absent, and one with
    // a malformed frame before a valid one.
    const auto wal_path = chunkdb::ChunkWalPath(table_dir, geometry, {0, 0});
    const auto check_wal = [&](const std::vector<Bytes>& frames, const std::string& code, const std::string& detail) {
        auto wal = chunkdb::BuildWalHeader({0, 0}, manifest->store_id, manifest->features);
        for (const auto& frame : frames) {
            wal.insert(wal.end(), frame.begin(), frame.end());
        }
        WriteFile(wal_path, wal);
        const auto [counters, out] = run_verify();
        if (counters.errors == 0U || !Contains(out, code) || !Contains(out, detail)) {
            std::fprintf(stderr, "verify missed %s (%s):\n%s", code.c_str(), detail.c_str(), out.c_str());
            assert(false);
        }
        // A load refuses the same files.
        chunkdb::CatalogConfig read_only = config;
        read_only.access_mode = chunkdb::AccessMode::kReadOnly;
        chunkdb::TableCatalog catalog(read_only);
        auto lease = catalog.Find("default")->Acquire();
        ExpectThrow<std::runtime_error>([&] { (void)lease->store().GetBlockExtra(0, 0); }, detail);
    };
    const Bytes presence_without_block_0 = {static_cast<std::uint8_t>(image.presence_bitmap[0] & ~0x01U)};
    check_wal(
        {RawFrame(image.revision + 1, {{chunkdb::kWalRecordSpan, SpanBody(32, presence_without_block_0)}})},
        "wal_extra_inconsistent", "absent block index 0");
    check_wal(
        {RawFrame(
             image.revision + 1,
             {{chunkdb::kWalRecordExtraDel, DelBody(3)}, {chunkdb::kWalRecordExtraDel, DelBody(0)}}),
         RawFrame(image.revision + 2, {{chunkdb::kWalRecordExtraDel, DelBody(0)}})},
        "wal_damaged", "record_extra_order");
    std::filesystem::remove(wal_path);
    const auto [counters, out] = run_verify();
    assert(counters.errors == 0U);
}

// --- protocol ------------------------------------------------------------------

std::string BulkBody(const std::string& reply) {
    assert(reply.rfind("$", 0) == 0);
    const auto header_end = reply.find("\r\n");
    const auto length = std::stoull(reply.substr(1, header_end - 1));
    assert(reply.size() == header_end + 2 + length + 2);
    return reply.substr(header_end + 2, length);
}

std::string AsText(const Bytes& bytes) {
    return std::string(bytes.begin(), bytes.end());
}

Bytes AsBytes(const std::string& text) {
    return Bytes(text.begin(), text.end());
}

void TestProtocol() {
    ScopedTempDir dir("chunkdb-extra-protocol");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    config.default_geometry = kStoreGeometry;
    auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
    chunkdb::EngineConfig engine_config;
    engine_config.require_auth = false;
    chunkdb::CommandEngine engine(engine_config, catalog);
    const auto hello = [&](chunkdb::SessionState& session, const std::string& line) {
        const auto reply = engine.Execute(session, line);
        assert(reply[0] == '$');
        return BulkBody(reply);
    };

    // What the server can do, whatever the table; TABLEINFO says per table.
    chunkdb::SessionState plain;
    const auto plain_hello = hello(plain, "HELLO 2\n");
    assert(Contains(plain_hello, "capabilities=zrle,extra-data\n"));
    assert(Contains(plain_hello, "max_extra_chunk_bytes=16777216\n"));
    assert(Contains(plain_hello, "extra_max_block_bits=0\nextra_max_chunk_bytes=0\n"));
    assert(engine.Execute(
               plain,
               "TABLECREATE ext block_bits 4 chunk_width_blocks 8 chunk_height_blocks 8 "
               "large_chunk_width_chunks 2 large_chunk_height_chunks 2 extra_max_block_bits 64 "
               "extra_max_chunk_bytes 1000\n") == "+OK\r\n");
    assert(Contains(
        engine.Execute(plain, "TABLECREATE bad block_bits 4 extra_max_chunk_bytes 100\n"),
        "-ERR INVALID_ARGUMENT extra_max_chunk_bytes applies only"));
    assert(Contains(
        engine.Execute(plain, "TABLECREATE bad block_bits 4 extra_max_block_bits x\n"),
        "-ERR INVALID_ARGUMENT extra_max_block_bits must be"));
    assert(engine.Execute(plain, "TABLECREATE dflt block_bits 4 extra_max_block_bits 8\n") == "+OK\r\n");
    assert(Contains(
        BulkBody(engine.Execute(plain, "TABLEINFO dflt\n")), "extra_max_block_bits=8\nextra_max_chunk_bytes=65536\n"));
    const auto info = BulkBody(engine.Execute(plain, "TABLEINFO ext\n"));
    assert(Contains(info, "extra_max_block_bits=64\nextra_max_chunk_bytes=1000\n"));
    assert(Contains(
        BulkBody(engine.Execute(plain, "TABLEINFO default\n")),
        "extra_max_block_bits=0\nextra_max_chunk_bytes=0\n"));

    // Runs a payload command the way the connection does: a refused header
    // is answered unread (the connection then closes), a refused payload is
    // read, dropped and answered.
    const auto send = [&](chunkdb::SessionState& on, const std::string& line, const Bytes& payload) {
        const auto plan = engine.PlanPayload(on, line);
        switch (plan.plan) {
            case chunkdb::CommandEngine::PayloadPlan::kReject:
                return "unread " + plan.reject_response;
            case chunkdb::CommandEngine::PayloadPlan::kDiscard:
                assert(plan.bytes == payload.size());
                return engine.ExecuteDiscarded(on, line, plan.reject_response);
            case chunkdb::CommandEngine::PayloadPlan::kRead:
                assert(plan.bytes == payload.size());
                return engine.Execute(on, line, AsText(payload));
            case chunkdb::CommandEngine::PayloadPlan::kNone:
                break;
        }
        assert(false);
        return std::string();
    };

    // A table without extra data refuses every extra-data request, reads
    // included; payloads are read and dropped, the connection stays.
    {
        assert(engine.Execute(plain, "SET 0 0 0001\n") == "+OK\r\n");
        const std::string disabled = "-ERR INVALID_ARGUMENT extra data is not enabled on table 'default'";
        assert(send(plain, "XPUT 0 0 8 1\n", {1}).rfind(disabled, 0) == 0);
        assert(send(plain, "XPUT 0 0 64 8\n", Bytes(8, 1)).rfind(disabled, 0) == 0);
        assert(send(plain, "CHUNKPUT 0 0 STATE EXTRA 40\n", Bytes(40, 0)).rfind(disabled, 0) == 0);
        assert(engine.Execute(plain, "XGET 0 0\n").rfind(disabled, 0) == 0);
        assert(engine.Execute(plain, "CHUNKGET 0 0 STATE EXTRA\n").rfind(disabled, 0) == 0);
        assert(engine.Execute(plain, "XDEL 0 0\n").rfind(disabled, 0) == 0);
        assert(Contains(
            engine.Execute(plain, "CHUNKBATCH 0 0 XDEL 0 0\n"), "-ERR INVALID_ARGUMENT extra data is not enabled"));
        assert(engine.Execute(plain, "GET 0 0\n") == "$4\r\n0001\r\n");
    }

    chunkdb::SessionState session;
    assert(Contains(hello(session, "HELLO 2 TABLE ext\n"), "capabilities=zrle,extra-data\n"));
    assert(engine.Execute(session, "SET 1 1 1010\n") == "+OK\r\n");
    const auto xput = [&](const std::string& line, const Bytes& value) { return send(session, line, value); };

    // Only a header that does not parse or a length over the protocol cap is
    // refused unread; table limits are refused after the bytes.
    assert(xput("XPUT 1 1 8\n", {}).rfind("unread -ERR INVALID_ARGUMENT", 0) == 0);
    assert(xput("XPUT 1 1 x 1\n", {}).rfind("unread -ERR INVALID_ARGUMENT", 0) == 0);
    assert(xput("XPUT 1 1 64 16777209\n", {}).rfind("unread -ERR BAD_REQUEST", 0) == 0);
    {
        const auto at_cap = engine.PlanPayload(session, "XPUT 1 1 64 16777208\n");
        assert(at_cap.plan == chunkdb::CommandEngine::PayloadPlan::kDiscard && at_cap.bytes == 16777208U);
    }
    assert(xput("XPUT 1 1 0 0\n", {}).rfind("-ERR INVALID_ARGUMENT XPUT bit_length must be between 1 and", 0) == 0);
    assert(xput("XPUT 1 1 65 9\n", Bytes(9, 1)).rfind("-ERR INVALID_ARGUMENT XPUT bit_length", 0) == 0);
    assert(xput("XPUT 1 1 64 9\n", Bytes(9, 1)).rfind("-ERR INVALID_ARGUMENT XPUT of 64 bits takes 8 bytes", 0) == 0);
    assert(xput("XPUT 1 1 12 1\n", {0x01}).rfind("-ERR INVALID_ARGUMENT XPUT of 12 bits takes 2 bytes", 0) == 0);
    // Padding bits are ignored and stored as zero.
    assert(xput("XPUT 1 1 12 2\n", {0xAB, 0xFF}) == "+OK\r\n");
    assert(AsBytes(BulkBody(engine.Execute(session, "XGET 1 1\n"))) == (Bytes{12, 0, 0, 0, 0xAB, 0x0F}));
    assert(Contains(xput("XPUT 2 2 8 1\n", {0x01}), "-ERR INVALID_ARGUMENT block (2,2) is not set"));
    assert(xput("XPUT 1 1 64 8\n", Bytes(8, 1)) == "+OK\r\n");
    assert(engine.Execute(session, "XDEL 1 1\n") == "+OK\r\n");
    assert(engine.Execute(session, "XGET 1 1\n") == "$-1\r\n");
    assert(engine.Execute(session, "XDEL 1 1\n") == "+OK\r\n");

    // A table dropped under the connection answers NO_TABLE, whether its
    // payload was read or dropped.
    {
        chunkdb::SessionState doomed;
        (void)hello(doomed, "HELLO 2 TABLE dflt\n");
        assert(engine.Execute(plain, "TABLEDROP dflt\n") == "+OK\r\n");
        assert(send(doomed, "XPUT 0 0 8 1\n", {1}).rfind("-ERR NO_TABLE", 0) == 0);
        assert(send(doomed, "XPUT 0 0 9 1\n", {1}).rfind("-ERR NO_TABLE", 0) == 0);
    }

    // CHUNKBATCH: XPUT values are bit strings, bit i first.
    {
        const auto reply = engine.Execute(session, "CHUNKBATCH 0 0 SET 2 2 0001 XPUT 2 2 101 XPUT 1 1 11110000111\n");
        assert(reply[0] == '$');
        assert(AsBytes(BulkBody(engine.Execute(session, "XGET 2 2\n"))) == (Bytes{3, 0, 0, 0, 0x05}));
        assert(AsBytes(BulkBody(engine.Execute(session, "XGET 1 1\n"))) == (Bytes{11, 0, 0, 0, 0x0F, 0x07}));
        assert(Contains(
            engine.Execute(session, "CHUNKBATCH 0 0 XPUT 3 3 1\n"), "-ERR INVALID_ARGUMENT block (3,3) is not set"));
        assert(Contains(engine.Execute(session, "CHUNKBATCH 0 0 XPUT 2 2 102\n"), "-ERR INVALID_ARGUMENT XPUT bits"));
        assert(Contains(engine.Execute(session, "CHUNKBATCH 0 0 XPUT 2 2\n"), "-ERR INVALID_ARGUMENT"));
        assert(engine.Execute(session, "CHUNKBATCH 0 0 XDEL 2 2 UNSET 1 1\n")[0] == '$');
        assert(engine.Execute(session, "XGET 2 2\n") == "$-1\r\n");
        assert(engine.Execute(session, "XGET 1 1\n") == "$-1\r\n");
    }

    // CHUNKGET ... STATE EXTRA returns the state and then the section;
    // CHUNKPUT ... STATE EXTRA writes them back, also zrle-encoded and with IF.
    assert(engine.Execute(session, "SET 1 1 1010\n") == "+OK\r\n");
    assert(xput("XPUT 1 1 20 3\n", {1, 2, 3}) == "+OK\r\n");
    const auto state_extra = AsBytes(BulkBody(engine.Execute(session, "CHUNKGET 0 0 STATE EXTRA\n")));
    const auto state = AsBytes(BulkBody(engine.Execute(session, "CHUNKGET 0 0 STATE\n")));
    assert(state_extra.size() == state.size() + 8U + 3U);
    assert(Bytes(state_extra.begin(), state_extra.begin() + static_cast<std::ptrdiff_t>(state.size())) == state);
    assert(AsBytes(BulkBody(engine.Execute(session, "CHUNKGET 0 0 EXTRA ZRLE STATE\n"))) ==
           chunkdb::ZrleCompress(state_extra));
    assert(Contains(engine.Execute(session, "CHUNKGET 0 0 EXTRA\n"), "-ERR INVALID_ARGUMENT CHUNKGET EXTRA requires STATE"));
    assert(Contains(
        engine.Execute(session, "CHUNKRANGE 0 0 0 0 STATE EXTRA\n"), "-ERR INVALID_ARGUMENT CHUNKRANGE options are STATE and ZRLE"));
    {
        // Unread only over the protocol cap (state + 16 MiB, + 16 with
        // ZRLE); over the table's limit the bytes are read and refused.
        const auto plan_of = [&](const std::string& line) { return engine.PlanPayload(session, line).plan; };
        using Plan = chunkdb::CommandEngine::PayloadPlan;
        assert(plan_of("CHUNKPUT 0 0 STATE EXTRA 16777257\n") == Plan::kReject);
        assert(plan_of("CHUNKPUT 0 0 STATE EXTRA 16777256\n") == Plan::kDiscard);
        assert(plan_of("CHUNKPUT 0 0 STATE EXTRA 1041\n") == Plan::kDiscard);
        assert(plan_of("CHUNKPUT 0 0 STATE EXTRA 1040\n") == Plan::kRead);
        assert(plan_of("CHUNKPUT 0 0 STATE EXTRA ZRLE 16777273\n") == Plan::kReject);
        assert(plan_of("CHUNKPUT 0 0 STATE EXTRA ZRLE 16777272\n") == Plan::kRead);
        assert(plan_of("CHUNKPUT 0 0 EXTRA 40\n") == Plan::kReject);
        assert(send(session, "CHUNKPUT 0 0 STATE EXTRA 1041\n", Bytes(1041, 0))
                   .rfind("-ERR INVALID_ARGUMENT extra data section of 1001 bytes exceeds extra_max_chunk_bytes (1000)", 0) == 0);
    }
    // Move block (1,1)'s value to (2,2) in another chunk with one write.
    Bytes moved = state;
    moved[32 + 2] |= 0x04U;  // block (2,2) = index 18 present
    ChunkExtra moved_extra;
    (void)moved_extra.Put(18, Val(20, 9));
    const auto moved_body = Concat({moved, moved_extra.Encode()});
    const auto put_line = "CHUNKPUT 1 0 STATE EXTRA " + std::to_string(moved_body.size()) + "\n";
    assert(engine.PlanPayload(session, put_line).plan == chunkdb::CommandEngine::PayloadPlan::kRead);
    const auto put_reply = engine.Execute(session, put_line, AsText(moved_body));
    assert(put_reply[0] == '$');
    assert(AsBytes(BulkBody(engine.Execute(session, "CHUNKGET 1 0 STATE EXTRA\n"))) == moved_body);
    assert(AsBytes(BulkBody(engine.Execute(session, "XGET 10 2\n"))) ==
           Concat({{20, 0, 0, 0}, Val(20, 9).bytes}));
    // The chunk round-trips through zrle and IF.
    const auto version = BulkBody(put_reply);
    const auto zrle_body = chunkdb::ZrleCompress(state_extra);
    const auto zrle_line =
        "CHUNKPUT 1 0 STATE EXTRA ZRLE IF " + version + " " + std::to_string(zrle_body.size()) + "\n";
    assert(engine.Execute(session, zrle_line, AsText(zrle_body))[0] == '$');
    assert(AsBytes(BulkBody(engine.Execute(session, "CHUNKGET 1 0 STATE EXTRA\n"))) == state_extra);
    assert(Contains(engine.Execute(session, zrle_line, AsText(zrle_body)), "-ERR VERSION_MISMATCH"));
    // Bad sections and sizes fail without changing the chunk.
    const auto bad_put = [&](const Bytes& body, const std::string& reason) {
        const auto line = "CHUNKPUT 1 0 STATE EXTRA " + std::to_string(body.size()) + "\n";
        const auto reply = engine.Execute(session, line, AsText(body));
        if (!Contains(reply, reason)) {
            std::fprintf(stderr, "expected %s, got %s\n", reason.c_str(), reply.c_str());
            assert(false);
        }
        assert(AsBytes(BulkBody(engine.Execute(session, "CHUNKGET 1 0 STATE EXTRA\n"))) == state_extra);
    };
    bad_put(Bytes(state.begin(), state.end() - 1), "-ERR INVALID_ARGUMENT payload length");
    bad_put(Concat({state, Entry(0, 8, {1})}), "which the chunk state leaves absent");
    bad_put(Concat({state, Entry(9, 8, {1}), Entry(9, 8, {1})}), "not strictly ascending");
    bad_put(Concat({state, Bytes{9, 0, 0}}), "header extends past");
    {
        // A zrle body that declares more than the bound.
        Bytes big = chunkdb::ZrleCompress(Bytes(state.size() + 1001U, 0U));
        const auto line = "CHUNKPUT 1 0 STATE EXTRA ZRLE " + std::to_string(big.size()) + "\n";
        assert(Contains(engine.Execute(session, line, AsText(big)), "-ERR INVALID_ARGUMENT zrle payload is invalid"));
    }

    // Options through TABLESET: extra data cannot be disabled, and its limits
    // only grow.
    assert(Contains(
        engine.Execute(session, "TABLESET ext extra_max_block_bits 0\n"), "-ERR INVALID_ARGUMENT extra data cannot be disabled"));
    assert(Contains(
        engine.Execute(session, "TABLESET ext extra_max_block_bits 63\n"), "-ERR INVALID_ARGUMENT extra-data limits of table 'ext' can only be raised"));
    assert(Contains(
        engine.Execute(session, "TABLESET ext extra_max_chunk_bytes 999\n"), "can only be raised"));
    assert(engine.Execute(session, "TABLESET ext extra_max_block_bits 128 extra_max_chunk_bytes 2000\n") == "+OK\r\n");
    assert(Contains(BulkBody(engine.Execute(session, "TABLEINFO ext\n")), "extra_max_block_bits=128\nextra_max_chunk_bytes=2000\n"));
    assert(xput("XPUT 1 1 128 16\n", Bytes(16, 7)) == "+OK\r\n");
    assert(Contains(engine.Execute(session, "TABLESET default extra_max_block_bits 32 extra_max_chunk_bytes 9\n"),
                    "must hold one value"));
    assert(engine.Execute(session, "TABLESET default extra_max_block_bits 32\n") == "+OK\r\n");
    // The connection that greeted on `default` uses it without a new HELLO.
    assert(engine.Execute(plain, "XGET 0 0\n") == "$-1\r\n");
    assert(send(plain, "XPUT 0 0 32 4\n", {1, 2, 3, 4}) == "+OK\r\n");
    assert(AsBytes(BulkBody(engine.Execute(plain, "XGET 0 0\n"))) == (Bytes{32, 0, 0, 0, 1, 2, 3, 4}));

    // Metrics: XGET is a point read, XPUT/XDEL point writes.
    const auto metrics = engine.Execute(plain, "METRICS\n");
    assert(Contains(metrics, "class=\"point_read\""));
}

}  // namespace

int main() {
    chunkdb::SetLogLevel(chunkdb::LogLevel::kError);
    TestValueAndSectionCodec();
    TestChunkExtraAgainstModel();
    TestImageExtraSection();
    TestWalExtraRecords();
    TestStoreSemantics();
    TestFullChunkWrites();
    TestBatch();
    TestDisabledStore();
    TestDurability();
    TestConcurrentWriters();
    TestStaleWalWindows();
    TestLargeUpdates();
    TestEnableNotDurable();
    TestRollback();
    TestFeatureAndOptions();
    TestUnknownFeatureReadOnly();
    TestVerifyFindsDamagedExtraData();
    TestProtocol();
    std::puts("extra data tests passed");
    return 0;
}
