#include "archive_base_format.hpp"

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
const chunkdb::StoreId kEpoch = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
constexpr std::uint64_t kImageLimit = 1024U * 1024U;

void At(Bytes* bytes, std::size_t at, std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) (*bytes)[at + i] = static_cast<std::uint8_t>(value >> (8U * i));
}

void Checksum(Bytes* bytes) {
    At(bytes, bytes->size() - 4U, chunkdb::Crc32(bytes->data(), bytes->size() - 4U), 4);
}

Bytes Fixture(const chunkdb::ArchiveBaseDescriptor& base) {
    Bytes bytes(100U + 8U * base.dims, 0);
    bytes[0] = 'C'; bytes[1] = 'K'; bytes[2] = 'F'; bytes[3] = 'B';
    At(&bytes, 4, 2, 2); bytes[6] = base.dims; bytes[7] = static_cast<std::uint8_t>(base.kind);
    for (std::size_t i = 0; i < 16; ++i) bytes[8U + i] = base.epoch[i];
    At(&bytes, 24, std::bit_cast<std::uint64_t>(base.coord.x), 8);
    At(&bytes, 32, std::bit_cast<std::uint64_t>(base.coord.y), 8);
    if (base.dims == 3) At(&bytes, 40, std::bit_cast<std::uint64_t>(base.coord.z), 8);
    const auto at = 24U + 8U * base.dims;
    At(&bytes, at, base.first, 8); At(&bytes, at + 8, base.base_revision, 8);
    At(&bytes, at + 16, base.base_time_ms, 8); At(&bytes, at + 24, base.base_schema, 8);
    At(&bytes, at + 32, base.image_length, 8);
    for (std::size_t i = 0; i < 32U; ++i) bytes[at + 40U + i] = base.image_sha256[i];
    Checksum(&bytes); return bytes;
}

void Reject(const std::function<void()>& call, const std::string& reason) {
    try {
        call();
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()).find(reason) != std::string::npos);
        return;
    }
    assert(false && "expected archive base refusal");
}

chunkdb::ArchiveBaseDescriptor Parse(const Bytes& bytes, const chunkdb::ArchiveBaseDescriptor& expected) {
    return chunkdb::ParseArchiveBaseDescriptor(bytes, expected.dims, expected.epoch, expected.coord, expected.first, kImageLimit);
}

void TestExactEncoding() {
    const auto low = std::numeric_limits<std::int64_t>::min();
    const auto high = std::numeric_limits<std::int64_t>::max();
    const auto max = std::numeric_limits<std::uint64_t>::max();
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        for (const auto kind : {chunkdb::ArchiveBaseKind::kNeverWritten, chunkdb::ArchiveBaseKind::kImage,
                               chunkdb::ArchiveBaseKind::kRetainedEmpty}) {
            const chunkdb::ArchiveBaseDescriptor base{
                .dims = dims, .kind = kind, .epoch = kEpoch, .coord = {low, high, dims == 3 ? low : 0},
                .first = max, .base_revision = kind == chunkdb::ArchiveBaseKind::kNeverWritten ? 0 : max - 1U,
                .base_time_ms = kind == chunkdb::ArchiveBaseKind::kNeverWritten ? 0 : max,
                .base_schema = max, .image_length = kind == chunkdb::ArchiveBaseKind::kImage ? 100U : 0U,
                .image_sha256 = kind == chunkdb::ArchiveBaseKind::kImage ? chunkdb::crypto::Sha256Digest{1U, 2U, 3U, 4U} : chunkdb::crypto::Sha256Digest{},
            };
            const auto expected = Fixture(base);
            assert(chunkdb::SerializeArchiveBaseDescriptor(base, kImageLimit) == expected);
            assert(expected.size() == 100U + 8U * dims);
            const auto parsed = Parse(expected, base);
            assert(parsed.dims == dims && parsed.kind == kind && parsed.epoch == kEpoch && parsed.coord == base.coord);
            assert(parsed.first == max && parsed.base_revision == base.base_revision && parsed.base_time_ms == base.base_time_ms);
            assert(parsed.base_schema == max && parsed.image_length == base.image_length && parsed.image_sha256 == base.image_sha256);
        }
    }
}

void TestOriginalBaseBoundaryAndImageBinding() {
    // This codec binds raw bytes and already validated image metadata. Actual
    // image decoding/replay and crash publication remain separate format gates.
    const Bytes original_image{11, 22, 33, 44, 55};
    const chunkdb::ArchiveBaseDescriptor base{
        .dims = 3, .kind = chunkdb::ArchiveBaseKind::kImage, .epoch = kEpoch, .coord = {-1, 2, -3},
        .first = 6, .base_revision = 5, .base_time_ms = 500, .base_schema = 2,
        .image_length = original_image.size(), .image_sha256 = chunkdb::crypto::Sha256Of(original_image),
    };
    const chunkdb::PinnedBaseImageIdentity identity{3, kEpoch, {-1, 2, -3}, 5, 500, 2};
    const auto bytes = chunkdb::SerializeArchiveBaseDescriptor(base, kImageLimit);
    chunkdb::ValidatePinnedArchiveBaseImage(Parse(bytes, base), identity, original_image, kImageLimit);
    auto emptying = identity; emptying.revision = 7; emptying.commit_time_ms = 700;
    Reject([&] { chunkdb::ValidatePinnedArchiveBaseImage(base, emptying, original_image, kImageLimit); }, "identity mismatch");
    auto same_length_different_image = original_image; same_length_different_image[0] ^= 1;
    Reject([&] { chunkdb::ValidatePinnedArchiveBaseImage(base, identity, same_length_different_image, kImageLimit); }, "digest mismatch");
    auto protected_bytes = original_image;
    protected_bytes.resize(protected_bytes.size() + 4U);
    Checksum(&protected_bytes);
    auto substituted = protected_bytes;
    substituted[0] ^= 1U;
    Checksum(&substituted);
    assert(chunkdb::Crc32(protected_bytes) == chunkdb::Crc32(substituted));
    auto protected_base = base;
    protected_base.image_length = protected_bytes.size();
    protected_base.image_sha256 = chunkdb::crypto::Sha256Of(protected_bytes);
    chunkdb::ValidatePinnedArchiveBaseImage(protected_base, identity, protected_bytes, kImageLimit);
    Reject([&] { chunkdb::ValidatePinnedArchiveBaseImage(protected_base, identity, substituted, kImageLimit); }, "digest mismatch");
    Reject([&] { chunkdb::ValidatePinnedArchiveBaseImage(base, identity, {}, kImageLimit); }, "length");
    auto wrong = identity; wrong.epoch[0] ^= 1;
    Reject([&] { chunkdb::ValidatePinnedArchiveBaseImage(base, wrong, original_image, kImageLimit); }, "identity mismatch");
    wrong = identity; wrong.coord.z = 0;
    Reject([&] { chunkdb::ValidatePinnedArchiveBaseImage(base, wrong, original_image, kImageLimit); }, "identity mismatch");
    wrong = identity; wrong.schema_version = 3;
    Reject([&] { chunkdb::ValidatePinnedArchiveBaseImage(base, wrong, original_image, kImageLimit); }, "identity mismatch");
    wrong = identity; wrong.dims = 2;
    Reject([&] { chunkdb::ValidatePinnedArchiveBaseImage(base, wrong, original_image, kImageLimit); }, "identity mismatch");

    auto collected = base;
    collected.kind = chunkdb::ArchiveBaseKind::kRetainedEmpty;
    collected.base_revision = 7; collected.base_time_ms = 700;
    collected.image_length = 0; collected.image_sha256 = {};
    // The empty-producing version7 cannot replace the original base5 of WAL6--7.
    Reject([&] { (void)chunkdb::SerializeArchiveBaseDescriptor(collected, kImageLimit); }, "strictly below");
    Reject([&] { (void)Parse(Fixture(collected), collected); }, "strictly below");
    collected.first = 7;
    Reject([&] { (void)chunkdb::SerializeArchiveBaseDescriptor(collected, kImageLimit); }, "strictly below");
    collected.first = 8;
    assert(Parse(Fixture(collected), collected).base_revision == 7);
    Reject([&] { chunkdb::ValidatePinnedArchiveBaseImage(collected, identity, original_image, kImageLimit); }, "does not require an image");
    auto never = collected; never.kind = chunkdb::ArchiveBaseKind::kNeverWritten;
    never.base_revision = 0; never.base_time_ms = 0; never.first = 1;
    assert(Parse(Fixture(never), never).base_revision == 0);
}

void TestMalformedDescriptors() {
    const chunkdb::ArchiveBaseDescriptor base{
        .dims = 3, .kind = chunkdb::ArchiveBaseKind::kImage, .epoch = kEpoch, .coord = {-1, 2, -3},
        .first = 6, .base_revision = 5, .base_time_ms = 500, .base_schema = 2, .image_length = 100, .image_sha256 = {1U, 2U, 3U, 4U},
    };
    const auto good = Fixture(base);
    const auto corrupt = [&](std::size_t at, std::uint64_t value, unsigned width, const std::string& reason) {
        auto changed = good; At(&changed, at, value, width); Checksum(&changed);
        Reject([&] { (void)Parse(changed, base); }, reason);
    };
    corrupt(0, 'X', 1, "CKFB");
    corrupt(4, 1, 2, "version");
    corrupt(4, 3, 2, "version");
    corrupt(6, 2, 1, "dimensions");
    corrupt(7, 3, 1, "kind");
    corrupt(8, 0, 1, "epoch");
    corrupt(40, 0, 8, "another chunk");
    corrupt(48, 7, 8, "another segment");
    corrupt(56, 6, 8, "strictly below");
    corrupt(56, 7, 8, "strictly below");
    corrupt(56, 0, 8, "image revision");
    corrupt(72, 0, 8, "schema");
    corrupt(80, 0, 8, "length");
    corrupt(80, kImageLimit + 1U, 8, "length");
    corrupt(80, std::numeric_limits<std::uint64_t>::max(), 8, "length");
    for (std::size_t i = 0; i < good.size(); ++i) {
        auto changed = good; changed[i] ^= 1;
        Reject([&] { (void)Parse(changed, base); }, "archive base");
    }
    for (std::size_t length = 0; length < good.size(); ++length)
        Reject([&] { (void)Parse(Bytes(good.begin(), good.begin() + static_cast<std::ptrdiff_t>(length)), base); }, "length");
    auto extra = good; extra.push_back(0);
    Reject([&] { (void)Parse(extra, base); }, "length");
    auto invalid = base; invalid.first = 0;
    Reject([&] { (void)chunkdb::SerializeArchiveBaseDescriptor(invalid, kImageLimit); }, "strictly below");
    invalid = base; invalid.dims = 2;
    Reject([&] { (void)chunkdb::SerializeArchiveBaseDescriptor(invalid, kImageLimit); }, "inactive");
    invalid = base; invalid.dims = 4;
    Reject([&] { (void)chunkdb::SerializeArchiveBaseDescriptor(invalid, kImageLimit); }, "dimensions");
    invalid = base; invalid.kind = chunkdb::ArchiveBaseKind::kNeverWritten; invalid.base_revision = 0;
    Reject([&] { (void)chunkdb::SerializeArchiveBaseDescriptor(invalid, kImageLimit); }, "producing commit");
    invalid.base_time_ms = 0;
    Reject([&] { (void)chunkdb::SerializeArchiveBaseDescriptor(invalid, kImageLimit); }, "image fields");
    invalid = base; invalid.kind = chunkdb::ArchiveBaseKind::kRetainedEmpty; invalid.base_revision = 0;
    Reject([&] { (void)chunkdb::SerializeArchiveBaseDescriptor(invalid, kImageLimit); }, "no producing commit");
    Reject([&] { (void)chunkdb::ParseArchiveBaseDescriptor(good, 3, kEpoch, base.coord, 0, kImageLimit); }, "another segment");
}

void TestCanonicalNames() {
    const auto max = std::numeric_limits<std::uint64_t>::max();
    const auto low = std::numeric_limits<std::int64_t>::min();
    const auto high = std::numeric_limits<std::int64_t>::max();
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        const chunkdb::ChunkCoord coord{low, high, dims == 3 ? low : 0};
        const auto name = chunkdb::ArchiveBaseFileName(coord, dims, max);
        chunkdb::ChunkCoord decoded; std::uint64_t first = 0;
        assert(chunkdb::ParseArchiveBaseFileName(name, dims, &decoded, &first));
        assert(decoded == coord && first == max);
    }
    for (const auto* invalid : {"C_0_0.0.base", "C_0_0.01.base", "C_0_0.+1.base", "C_0_0.-1.base",
                               "C_0_0.18446744073709551616.base", "C_0_0.1.base.extra", "C_0_0..base",
                               "C_0_0.1.2.base", "C_0_0_0.1.base", "C_00_0.1.base", "C_-0_0.1.base",
                               "../C_0_0.1.base", "C_0_0\\.1.base", "C_9223372036854775808_0.1.base"}) {
        chunkdb::ChunkCoord decoded{11, 22, 33}; std::uint64_t first = 99;
        assert(!chunkdb::ParseArchiveBaseFileName(invalid, 2, &decoded, &first));
        assert((decoded == chunkdb::ChunkCoord{11, 22, 33}) && first == 99);
    }
    chunkdb::ChunkCoord decoded; std::uint64_t first = 0;
    assert(!chunkdb::ParseArchiveBaseFileName("C_0_0.1.base", 2, nullptr, &first));
    assert(!chunkdb::ParseArchiveBaseFileName("C_0_0.1.base", 2, &decoded, nullptr));
    assert(!chunkdb::ParseArchiveBaseFileName("C_0_0.1.base", 4, &decoded, &first));
    Reject([&] { (void)chunkdb::ArchiveBaseFileName({}, 2, 0); }, "zero");
    Reject([&] { (void)chunkdb::ArchiveBaseFileName({0, 0, 1}, 2, 1); }, "inactive");
}

}  // namespace

int main() {
    TestExactEncoding();
    TestOriginalBaseBoundaryAndImageBinding();
    TestMalformedDescriptors();
    TestCanonicalNames();
}
