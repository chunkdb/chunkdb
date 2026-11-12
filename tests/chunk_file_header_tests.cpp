#include "chunk_file_header.hpp"

#include <cassert>
#include <bit>
#include <functional>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/crc32.hpp"

namespace {

using Bytes = std::vector<std::uint8_t>;
using Section = chunkdb::ChunkImageSectionDescriptor;
constexpr chunkdb::ChunkImageHeaderLimits kLimits{4096, 1024, 2048, 3072};
constexpr chunkdb::FeatureFlags kFeatures{.incompat = 32U | 8U, .ro_compat = 4U, .compat = 128U};
const chunkdb::ChunkImageHeaderInfo kInfo{kFeatures, 7, 700};
const std::vector<Section> kSections{{1, 0, 3, 3, 123}, {2, 1, 2, 10, 456}, {9, 0, 4, 4, 789}};

bool SameFeatures(const chunkdb::FeatureFlags& lhs, const chunkdb::FeatureFlags& rhs) {
    return lhs.incompat == rhs.incompat && lhs.ro_compat == rhs.ro_compat && lhs.compat == rhs.compat;
}

chunkdb::ChunkFileContext Context(std::uint8_t dims = 2) {
    return {.dims = dims, .epoch = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
            .coord = {-2, 3, dims == 3 ? -4 : 0}, .admitted_features = kFeatures};
}

void At(Bytes* bytes, std::size_t at, std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) (*bytes)[at + i] = static_cast<std::uint8_t>(value >> (8U * i));
}

void Checksum(Bytes* bytes) {
    At(bytes, bytes->size() - 4U, chunkdb::Crc32(bytes->data(), bytes->size() - 4U), 4);
}

// Independent wire fixtures use literal format offsets, not production helpers.
Bytes Fixture(const chunkdb::ChunkFileContext& context, bool image,
              const chunkdb::ChunkImageHeaderInfo& info, const std::vector<Section>& sections) {
    Bytes bytes((image ? 64U + 16U * sections.size() : 48U) + 8U * context.dims, 0);
    const std::string magic = image ? "CHKIMAGE" : "CHKWALOG";
    for (std::size_t i = 0; i < magic.size(); ++i) bytes[i] = static_cast<std::uint8_t>(magic[i]);
    At(&bytes, 8, 2, 2); At(&bytes, 10, image ? sections.size() : 0, 2);
    bytes[12] = context.dims;
    At(&bytes, 16, info.features.incompat, 4); At(&bytes, 20, info.features.ro_compat, 4);
    At(&bytes, 24, info.features.compat, 4);
    for (std::size_t i = 0; i < 16; ++i) bytes[28U + i] = context.epoch[i];
    At(&bytes, 44, std::bit_cast<std::uint64_t>(context.coord.x), 8);
    At(&bytes, 52, std::bit_cast<std::uint64_t>(context.coord.y), 8);
    if (context.dims == 3) At(&bytes, 60, std::bit_cast<std::uint64_t>(context.coord.z), 8);
    if (image) {
        At(&bytes, 44U + 8U * context.dims, info.revision, 8);
        At(&bytes, 52U + 8U * context.dims, info.commit_time_ms, 8);
        for (std::size_t i = 0; i < sections.size(); ++i) {
            const auto at = 60U + 8U * context.dims + 16U * i;
            At(&bytes, at, sections[i].kind, 2); At(&bytes, at + 2, sections[i].flags, 2);
            At(&bytes, at + 4, sections[i].stored_size, 4); At(&bytes, at + 8, sections[i].raw_size, 4);
            At(&bytes, at + 12, sections[i].raw_crc32, 4);
        }
    }
    Checksum(&bytes);
    return bytes;
}

std::uint64_t FileBytes(const Bytes& header, const std::vector<Section>& sections) {
    std::uint64_t result = header.size();
    for (const auto& entry : sections) result += entry.stored_size;
    return result;
}

void Reject(const std::function<void()>& call, const std::string& reason = "") {
    try { call(); }
    catch (const std::invalid_argument& e) {
        assert(std::string(e.what()).find(reason) != std::string::npos);
        return;
    }
    assert(false && "expected header refusal");
}

void TestIndependentEncodingAndExtremes() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        auto context = Context(dims);
        context.coord = {std::numeric_limits<std::int64_t>::min(), std::numeric_limits<std::int64_t>::max(),
                         dims == 3 ? std::numeric_limits<std::int64_t>::min() : 0};
        const chunkdb::ChunkImageHeaderInfo info{kFeatures, std::numeric_limits<std::uint64_t>::max(),
                                                std::numeric_limits<std::uint64_t>::max()};
        const auto image = Fixture(context, true, info, kSections);
        const auto wal = Fixture(context, false, info, {});
        assert(chunkdb::SerializeChunkImageHeader(context, info, kSections, kLimits) == image);
        assert(chunkdb::SerializeChunkWalHeader(context, info.features) == wal);
        assert(chunkdb::ChunkImageFixedHeaderBytes(dims) == 60U + 8U * dims);
        assert(chunkdb::ChunkImageHeaderBytes(dims, 3) == image.size());
        assert(chunkdb::ChunkWalHeaderBytes(dims) == wal.size());
        const auto parsed = chunkdb::ParseChunkImageHeader(image, FileBytes(image, kSections), context, kLimits);
        assert(parsed.info.revision == info.revision && parsed.info.commit_time_ms == info.commit_time_ms);
        assert(SameFeatures(parsed.info.features, kFeatures) && parsed.sections == kSections);
        assert(parsed.header_bytes == image.size() && parsed.file_bytes == FileBytes(image, kSections));
        assert(SameFeatures(chunkdb::ParseChunkWalHeader(wal, context), kFeatures));
        // The opaque section 9 and compressed section 2 are only descriptors.
        // There are no bodies: parsing does not validate raw CRC, compression,
        // required sections, or grant permission to skip an unknown section.
        const auto no_sections = Fixture(context, true, info, {});
        assert(chunkdb::ParseChunkImageHeader(no_sections, no_sections.size(), context, kLimits).sections.empty());
        assert(chunkdb::SerializeChunkImageHeader(context, info, {}, kLimits) == no_sections);
        auto subset = info; subset.features = {.incompat = 32U};
        const auto subset_wal = chunkdb::SerializeChunkWalHeader(context, subset.features);
        assert(SameFeatures(chunkdb::ParseChunkWalHeader(subset_wal, context), subset.features));
    }
}

void TestLengthsChecksumsAndDevelopmentRefusal() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        const auto context = Context(dims);
        for (const bool image : {false, true}) {
            const auto bytes = Fixture(context, image, kInfo, image ? kSections : std::vector<Section>{});
            const auto parse = [&](const Bytes& input) {
                if (image) (void)chunkdb::ParseChunkImageHeader(input, FileBytes(bytes, kSections), context, kLimits);
                else (void)chunkdb::ParseChunkWalHeader(input, context);
            };
            for (std::size_t length = 0; length < bytes.size(); ++length)
                Reject([&] { parse(Bytes(bytes.begin(), bytes.begin() + length)); });
            for (std::size_t at = 0; at < bytes.size(); ++at) {
                auto corrupt = bytes; corrupt[at] ^= 0x80U;
                Reject([&] { parse(corrupt); });
            }
            auto extra = bytes; extra.push_back(0x46); // FRM2 belongs outside the WAL header API.
            Reject([&] { parse(extra); }, "length");
            auto earlier = bytes; At(&earlier, 8, 1, 2);
            earlier.resize(10); // No old header/directory is consumed or allocated.
            Reject([&] { parse(earlier); }, "created by an earlier development build of 2.0; recreate it");
            for (const auto version : {0U, 3U, 65535U}) {
                auto unknown = bytes; At(&unknown, 8, version, 2); Checksum(&unknown);
                Reject([&] { parse(unknown); }, "unsupported");
            }
        }
    }
}

void TestCrcValidIdentityAndFeatures() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        const auto context = Context(dims);
        for (const bool image : {false, true}) {
            const auto original = Fixture(context, image, kInfo, image ? kSections : std::vector<Section>{});
            const auto parse = [&](const Bytes& input, const chunkdb::ChunkFileContext& expected) {
                if (image) (void)chunkdb::ParseChunkImageHeader(input, FileBytes(original, kSections), expected, kLimits);
                else (void)chunkdb::ParseChunkWalHeader(input, expected);
            };
            const auto malformed = [&](std::size_t at, std::uint64_t value, unsigned width, const std::string& reason) {
                auto bytes = original; At(&bytes, at, value, width); Checksum(&bytes);
                Reject([&] { parse(bytes, context); }, reason);
            };
            for (const auto stored_dims : {0U, 1U, dims == 2 ? 3U : 2U, 255U})
                malformed(12, stored_dims, 1, "dimensions differ");
            for (const auto at : {13U, 14U, 15U}) malformed(at, 1, 1, "reserved bytes");
            malformed(28, 0, 1, "epoch");
            malformed(44, 0, 8, "another chunk"); malformed(52, 0, 8, "another chunk");
            if (dims == 3) malformed(60, 0, 8, "another chunk");
            malformed(16, 8, 4, "lacks chunk model");
            for (const auto at : {16U, 20U, 24U}) malformed(at, 0x80000000U | 32U, 4, "exceed");
            if (image) malformed(44U + 8U * dims, 0, 8, "revision is zero");
            else malformed(10, 1, 2, "reserved field");
            auto invalid = context; invalid.dims = 4;
            Reject([&] { parse(original, invalid); }, "dimensions must");
            invalid = context; invalid.admitted_features.incompat = 8;
            Reject([&] { parse(original, invalid); }, "envelope lacks");
            if (dims == 2) {
                invalid = context; invalid.coord.z = 1;
                Reject([&] { parse(original, invalid); }, "inactive");
                Reject([&] { (void)chunkdb::SerializeChunkWalHeader(invalid, kFeatures); }, "inactive");
            }
        }
    }
}

void TestDirectoryBounds() {
    const auto context = Context(3);
    const auto original = Fixture(context, true, kInfo, kSections);
    const auto complete = FileBytes(original, kSections);
    const auto parse = [&](const Bytes& bytes, std::uint64_t size, chunkdb::ChunkImageHeaderLimits limits = kLimits) {
        return chunkdb::ParseChunkImageHeader(bytes, size, context, limits);
    };
    Reject([&] { (void)parse(original, complete - 1); }, "fill complete file");
    Reject([&] { (void)parse(original, complete + 1); }, "fill complete file");
    Reject([&] { (void)parse(original, std::numeric_limits<std::uint64_t>::max()); }, "fill complete file");
    const auto malformed = [&](std::size_t at, std::uint64_t value, unsigned width, const std::string& reason) {
        auto bytes = original; At(&bytes, at, value, width); Checksum(&bytes);
        Reject([&] { (void)parse(bytes, complete); }, reason);
    };
    constexpr std::size_t directory = 84;
    malformed(10, 65, 2, "too many");
    malformed(10, 65535, 2, "too many");
    malformed(directory + 16, 1, 2, "ascending");
    malformed(directory + 32, 0, 2, "ascending");
    malformed(directory + 2, 2, 2, "flags");
    malformed(directory + 2, 65535, 2, "flags");
    malformed(directory + 8, 4, 4, "sizes differ");
    malformed(directory + 16 + 4, 1025, 4, "section byte limit");
    malformed(directory + 16 + 8, 2049, 4, "section byte limit");
    auto limits = kLimits; limits.max_file_bytes = original.size() - 1;
    Reject([&] { (void)parse(original, complete, limits); }, "file byte limit");
    limits = kLimits; limits.max_file_bytes = complete - 1;
    Reject([&] { (void)parse(original, complete, limits); }, "file byte limit");
    limits = kLimits; limits.max_total_raw_bytes = 16;
    Reject([&] { (void)parse(original, complete, limits); }, "total raw byte limit");
    limits.max_total_raw_bytes = 17;
    assert(parse(original, complete, limits).sections == kSections);

    // Maximum u32 section sizes are checked arithmetically without allocating
    // any body; complete-file and total-raw counters use u64, not wrapping u32.
    const auto max = std::numeric_limits<std::uint32_t>::max();
    const std::vector<Section> huge{{1, 0, max, max, 0}, {2, 0, max, max, 0}};
    const auto huge_header = Fixture(context, true, kInfo, huge);
    const auto huge_file = FileBytes(huge_header, huge);
    const chunkdb::ChunkImageHeaderLimits huge_limits{huge_file, max, max, 2ULL * max};
    assert(chunkdb::SerializeChunkImageHeader(context, kInfo, huge, huge_limits) == huge_header);
    assert(parse(huge_header, huge_file, huge_limits).file_bytes > max);
    auto smaller = huge_limits; --smaller.max_total_raw_bytes;
    Reject([&] { (void)parse(huge_header, huge_file, smaller); }, "total raw");
    smaller = huge_limits; --smaller.max_file_bytes;
    Reject([&] { (void)parse(huge_header, huge_file, smaller); }, "file byte limit");
    Reject([&] { (void)parse(huge_header, huge_file); }, "section byte limit");

    std::vector<Section> maximum;
    for (std::uint16_t kind = 0; kind < 64; ++kind) maximum.push_back({kind, 0, 0, 0, 0});
    const auto max_header = Fixture(context, true, kInfo, maximum);
    assert(parse(max_header, max_header.size()).sections == maximum);
    maximum.push_back({64, 0, 0, 0, 0});
    Reject([&] { (void)chunkdb::SerializeChunkImageHeader(context, kInfo, maximum, kLimits); }, "too many");
    Reject([&] { (void)chunkdb::ChunkImageHeaderBytes(3, 65); }, "too many");
}

void TestSerializerRefusals() {
    const auto context = Context();
    auto bad_info = kInfo; bad_info.revision = 0;
    Reject([&] { (void)chunkdb::SerializeChunkImageHeader(context, bad_info, kSections, kLimits); }, "revision");
    bad_info = kInfo; bad_info.features.incompat = 0;
    Reject([&] { (void)chunkdb::SerializeChunkImageHeader(context, bad_info, kSections, kLimits); }, "lacks");
    Reject([&] { (void)chunkdb::SerializeChunkWalHeader(context, bad_info.features); }, "lacks");
    bad_info.features.incompat = 32U | 0x80000000U;
    Reject([&] { (void)chunkdb::SerializeChunkImageHeader(context, bad_info, kSections, kLimits); }, "exceed");
    Reject([&] { (void)chunkdb::SerializeChunkWalHeader(context, bad_info.features); }, "exceed");
    for (const auto bad_entry : {Section{1, 2, 3, 3, 0}, Section{1, 0, 3, 4, 0}, Section{1, 1, 1025, 3, 0},
                                 Section{1, 1, 3, 2049, 0}}) {
        auto entries = kSections; entries[0] = bad_entry;
        Reject([&] { (void)chunkdb::SerializeChunkImageHeader(context, kInfo, entries, kLimits); });
    }
    auto entries = kSections; entries[1].kind = 1;
    Reject([&] { (void)chunkdb::SerializeChunkImageHeader(context, kInfo, entries, kLimits); }, "ascending");
    auto limits = kLimits; limits.max_total_raw_bytes = 16;
    Reject([&] { (void)chunkdb::SerializeChunkImageHeader(context, kInfo, kSections, limits); }, "total raw");
    limits = kLimits; limits.max_file_bytes = 1;
    Reject([&] { (void)chunkdb::SerializeChunkImageHeader(context, kInfo, kSections, limits); }, "file byte limit");
    Reject([&] { (void)chunkdb::ChunkWalHeaderBytes(1); }, "dimensions");
    Reject([&] { (void)chunkdb::ChunkImageFixedHeaderBytes(255); }, "dimensions");
}

}  // namespace

int main() {
    TestIndependentEncodingAndExtremes();
    TestLengthsChecksumsAndDevelopmentRefusal();
    TestCrcValidIdentityAndFeatures();
    TestDirectoryBounds();
    TestSerializerRefusals();
}
