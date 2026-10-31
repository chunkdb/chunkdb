#include "populated_index_format.hpp"

#include <cassert>
#include <bit>
#include <functional>
#include <limits>
#include <sstream>
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

chunkdb::Geometry Shape(std::uint8_t dims) {
    return chunkdb::Geometry(chunkdb::GeometryConfig{
        .large_chunk_width_chunks = 4, .large_chunk_height_chunks = 3,
        .chunk_width_blocks = 3, .chunk_height_blocks = 5, .block_bits = 1,
        .dims = dims, .chunk_depth_blocks = dims == 3 ? 7U : 1U,
        .large_chunk_depth_chunks = dims == 3 ? 2U : 1U});
}

void Reject(const std::function<void()>& call, const std::string& reason) {
    try {
        call();
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()).find(reason) != std::string::npos);
        return;
    }
    assert(false && "expected index format rejection");
}

Bytes HeaderFixture(std::uint8_t dims, const chunkdb::LargeChunkCoord& coord) {
    Bytes bytes(28U + 8U * dims, 0);
    bytes[0] = 'C'; bytes[1] = 'K'; bytes[2] = 'I'; bytes[3] = 'X';
    At(&bytes, 4, 1, 2); bytes[6] = dims;
    for (std::size_t i = 0; i < 16; ++i) bytes[8U + i] = kTable[i];
    At(&bytes, 24, std::bit_cast<std::uint64_t>(coord.x), 8);
    At(&bytes, 32, std::bit_cast<std::uint64_t>(coord.y), 8);
    if (dims == 3) At(&bytes, 40, std::bit_cast<std::uint64_t>(coord.z), 8);
    Checksum(&bytes); return bytes;
}

Bytes RecordFixture(std::uint8_t dims, const chunkdb::ChunkIndexRecord& record) {
    Bytes bytes(36U + 8U * dims, 0);
    At(&bytes, 0, bytes.size(), 4); bytes[4] = static_cast<std::uint8_t>(record.kind);
    At(&bytes, 8, std::bit_cast<std::uint64_t>(record.coord.x), 8);
    At(&bytes, 16, std::bit_cast<std::uint64_t>(record.coord.y), 8);
    if (dims == 3) At(&bytes, 24, std::bit_cast<std::uint64_t>(record.coord.z), 8);
    At(&bytes, 8U + 8U * dims, record.revision, 8);
    At(&bytes, 16U + 8U * dims, record.commit_time_ms, 8);
    At(&bytes, 24U + 8U * dims, record.schema_version, 8);
    Checksum(&bytes); return bytes;
}

void TestShardHeadersAndRecords() {
    const auto max = std::numeric_limits<std::uint64_t>::max();
    const auto min_axis = std::numeric_limits<std::int64_t>::min();
    const auto max_axis = std::numeric_limits<std::int64_t>::max();
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        const auto geometry = Shape(dims);
        for (const auto coord : {chunkdb::ChunkCoord{-5, -8, dims == 3 ? -3 : 0},
                                 chunkdb::ChunkCoord{min_axis, max_axis, dims == 3 ? min_axis : 0}}) {
            const auto large = geometry.ChunkToLarge(coord);
            const chunkdb::ChunkIndexHeader header{kTable, dims, large};
            const auto expected = HeaderFixture(dims, large);
            assert(chunkdb::SerializeChunkIndexHeader(header) == expected);
            const auto parsed_header = chunkdb::ParseChunkIndexHeader(expected, geometry, kTable, large);
            assert(parsed_header.table_id == kTable && parsed_header.dims == dims && parsed_header.large_coord == large);
            for (const auto kind : {chunkdb::ChunkIndexKind::kLive, chunkdb::ChunkIndexKind::kDeleted, chunkdb::ChunkIndexKind::kForget}) {
                const chunkdb::ChunkIndexRecord record{kind, coord, max, max, max};
                const auto bytes = RecordFixture(dims, record);
                assert(chunkdb::SerializeChunkIndexRecord(record, geometry, large) == bytes);
                const auto parsed = chunkdb::ParseChunkIndexRecord(bytes, geometry, large);
                assert(parsed.kind == kind && parsed.coord == coord && parsed.revision == max && parsed.commit_time_ms == max && parsed.schema_version == max);
            }
        }
    }
    const auto geometry = Shape(3);
    const chunkdb::ChunkCoord coord{-5, -8, -3};
    const auto large = geometry.ChunkToLarge(coord);
    const auto good = HeaderFixture(3, large);
    const auto corrupt_header = [&](std::size_t at, std::uint64_t value, unsigned width, const std::string& reason) {
        auto changed = good; At(&changed, at, value, width); Checksum(&changed);
        Reject([&] { (void)chunkdb::ParseChunkIndexHeader(changed, geometry, kTable, large); }, reason);
    };
    corrupt_header(0, 'X', 1, "CKIX");
    corrupt_header(4, 0, 2, "version");
    corrupt_header(6, 2, 1, "dimensions");
    corrupt_header(7, 1, 1, "reserved");
    corrupt_header(8, 0, 1, "another table");
    corrupt_header(24, 1, 8, "another large chunk");
    for (std::size_t i = 0; i < good.size(); ++i) {
        auto changed = good; changed[i] ^= 1;
        Reject([&] { (void)chunkdb::ParseChunkIndexHeader(changed, geometry, kTable, large); }, "index");
    }
    for (std::size_t length = 0; length < good.size(); ++length)
        Reject([&] { (void)chunkdb::ParseChunkIndexHeader(std::span(good).first(length), geometry, kTable, large); }, "length");
    auto extra = good; extra.push_back(0);
    Reject([&] { (void)chunkdb::ParseChunkIndexHeader(extra, geometry, kTable, large); }, "length");
    Reject([&] { (void)chunkdb::SerializeChunkIndexHeader({kTable, 4, {}}); }, "dimensions");
    Reject([&] { (void)chunkdb::SerializeChunkIndexHeader({kTable, 2, {0, 0, 1}}); }, "inactive");

    const chunkdb::ChunkIndexRecord record{chunkdb::ChunkIndexKind::kDeleted, coord, 7, 100, 1};
    const auto bytes = RecordFixture(3, record);
    const auto corrupt_record = [&](std::size_t at, std::uint64_t value, unsigned width, const std::string& reason) {
        auto changed = bytes; At(&changed, at, value, width); Checksum(&changed);
        Reject([&] { (void)chunkdb::ParseChunkIndexRecord(changed, geometry, large); }, reason);
    };
    corrupt_record(0, 59, 4, "length");
    corrupt_record(4, 4, 1, "kind");
    corrupt_record(5, 1, 1, "reserved");
    corrupt_record(24, 10, 8, "another large chunk");
    corrupt_record(32, 0, 8, "nonzero");
    corrupt_record(48, 0, 8, "nonzero");
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        auto changed = bytes; changed[i] ^= 1;
        Reject([&] { (void)chunkdb::ParseChunkIndexRecord(changed, geometry, large); }, "index");
    }
    for (std::size_t length = 0; length < bytes.size(); ++length)
        Reject([&] { (void)chunkdb::ParseChunkIndexRecord(std::span(bytes).first(length), geometry, large); }, "length");
    auto invalid = record; invalid.revision = 0;
    Reject([&] { (void)chunkdb::SerializeChunkIndexRecord(invalid, geometry, large); }, "nonzero");
    invalid = record; invalid.coord.z = 10;
    Reject([&] { (void)chunkdb::SerializeChunkIndexRecord(invalid, geometry, large); }, "another large chunk");
}

Bytes InventoryFixture(const chunkdb::IndexInventoryHeader& header, const std::vector<chunkdb::LargeChunkCoord>& coords) {
    Bytes bytes(52U + 8U * header.dims * coords.size(), 0);
    bytes[0] = 'C'; bytes[1] = 'K'; bytes[2] = 'I'; bytes[3] = 'I';
    At(&bytes, 4, 1, 2); bytes[6] = header.dims; bytes[7] = header.compaction_pending ? 1 : 0;
    for (std::size_t i = 0; i < 16; ++i) bytes[8U + i] = header.table_id[i];
    At(&bytes, 24, header.retained_deletion_floor, 8); At(&bytes, 32, header.compaction_through, 8);
    At(&bytes, 40, header.count, 8);
    for (std::size_t i = 0; i < coords.size(); ++i) {
        const auto at = 48U + 8U * header.dims * i;
        At(&bytes, at, std::bit_cast<std::uint64_t>(coords[i].x), 8);
        At(&bytes, at + 8, std::bit_cast<std::uint64_t>(coords[i].y), 8);
        if (header.dims == 3) At(&bytes, at + 16, std::bit_cast<std::uint64_t>(coords[i].z), 8);
    }
    Checksum(&bytes); return bytes;
}

chunkdb::IndexInventoryHeader ReadInventory(const Bytes& bytes, const chunkdb::Geometry& shape,
                                           std::uint64_t declared_length = 0) {
    const std::string text(bytes.begin(), bytes.end());
    std::istringstream input(text);
    return chunkdb::ReadIndexInventory(input, declared_length == 0 ? bytes.size() : declared_length, shape, kTable);
}

void TestInventory() {
    const auto low = std::numeric_limits<std::int64_t>::min();
    const auto high = std::numeric_limits<std::int64_t>::max();
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        const auto shape = Shape(dims);
        const std::vector<chunkdb::LargeChunkCoord> coords{{low, low, dims == 3 ? low : 0},
                                                          {0, 0, dims == 3 ? -1 : 0},
                                                          {high, high, dims == 3 ? high : 0}};
        const chunkdb::IndexInventoryHeader header{kTable, dims, true, 50, 9, coords.size()};
        const auto expected = InventoryFixture(header, coords);
        std::ostringstream output;
        chunkdb::WriteIndexInventory(output, header, coords);
        assert(output.str() == std::string(expected.begin(), expected.end()));
        assert(chunkdb::IndexInventoryFileBytes(header) == expected.size());
        const auto parsed = ReadInventory(expected, shape);
        assert(parsed.dims == dims && parsed.table_id == kTable && parsed.compaction_pending);
        assert(parsed.retained_deletion_floor == 50 && parsed.compaction_through == 9 && parsed.count == 3);
        const chunkdb::IndexInventoryHeader empty{kTable, dims, false, 0, 0, 0};
        const auto fresh = InventoryFixture(empty, {});
        assert(ReadInventory(fresh, shape).count == 0 && fresh.size() == 52);
    }
    const auto shape = Shape(3);
    const chunkdb::IndexInventoryHeader header{kTable, 3, true, 50, 9, 2};
    const std::vector<chunkdb::LargeChunkCoord> coords{{0, 0, -1}, {0, 0, 1}};
    const auto good = InventoryFixture(header, coords);
    const auto corrupt = [&](std::size_t at, std::uint64_t value, unsigned width, const std::string& reason) {
        auto changed = good; At(&changed, at, value, width); Checksum(&changed);
        Reject([&] { (void)ReadInventory(changed, shape); }, reason);
    };
    corrupt(0, 'X', 1, "CKII");
    corrupt(4, 2, 2, "version");
    corrupt(6, 2, 1, "dimensions");
    corrupt(7, 2, 1, "flags");
    corrupt(7, 0, 1, "compaction bounds");
    corrupt(8, 0, 1, "another table");
    corrupt(24, 8, 8, "compaction bounds");
    corrupt(32, 51, 8, "compaction bounds");
    corrupt(40, 1, 8, "length");
    corrupt(40, std::numeric_limits<std::uint64_t>::max(), 8, "overflows");
    corrupt(88, std::bit_cast<std::uint64_t>(std::int64_t{-1}), 8, "unique");
    corrupt(88, std::bit_cast<std::uint64_t>(std::int64_t{-2}), 8, "increasing");
    for (std::size_t i = 0; i < good.size(); ++i) {
        auto changed = good; changed[i] ^= 1;
        Reject([&] { (void)ReadInventory(changed, shape); }, "index");
    }
    for (std::size_t length = 0; length < good.size(); ++length) {
        const Bytes shortened(good.begin(), good.begin() + static_cast<std::ptrdiff_t>(length));
        Reject([&] { (void)ReadInventory(shortened, shape); }, "index");
    }
    auto extra = good; extra.push_back(0);
    Reject([&] { (void)ReadInventory(extra, shape); }, "length");
    Reject([&] { (void)ReadInventory(extra, shape, good.size()); }, "after index inventory");
    const auto no_output = [&](chunkdb::IndexInventoryHeader changed, std::vector<chunkdb::LargeChunkCoord> entries) {
        std::ostringstream output; output << "unchanged";
        Reject([&] { chunkdb::WriteIndexInventory(output, changed, entries); }, "index");
        assert(output.str() == "unchanged");
    };
    auto bad = header; bad.compaction_pending = false; no_output(bad, coords);
    bad = header; bad.compaction_through = 51; no_output(bad, coords);
    bad = header; bad.count = 1; no_output(bad, coords);
    bad = header; bad.dims = 1; no_output(bad, coords);
    bad = header; bad.dims = 2; no_output(bad, coords);
    no_output(header, {{0, 0, 1}, {0, 0, -1}});
    no_output(header, {{0, 0, 1}, {0, 0, 1}});

    // More than typical bounded metadata-array limits: no inventory count cap.
    std::vector<chunkdb::LargeChunkCoord> many;
    for (std::int64_t i = 0; i < 10000; ++i) many.push_back({0, 0, i});
    auto large = header; large.count = many.size();
    const auto bytes = InventoryFixture(large, many);
    struct NonSeekingBuffer : std::stringbuf {
        explicit NonSeekingBuffer(const std::string& text) : std::stringbuf(text) {}
        pos_type seekoff(off_type, std::ios_base::seekdir, std::ios_base::openmode) override { return pos_type(off_type(-1)); }
        pos_type seekpos(pos_type, std::ios_base::openmode) override { return pos_type(off_type(-1)); }
    } buffer(std::string(bytes.begin(), bytes.end()));
    std::istream input(&buffer);
    assert(chunkdb::ReadIndexInventory(input, bytes.size(), shape, kTable).count == 10000);
    std::istringstream throwing(std::string(good.begin(), good.end()));
    throwing.exceptions(std::ios_base::badbit | std::ios_base::failbit);
    assert(chunkdb::ReadIndexInventory(throwing, good.size(), shape, kTable).count == 2);
    std::istringstream failed(std::string(good.begin(), good.begin() + 48));
    failed.exceptions(std::ios_base::badbit | std::ios_base::failbit);
    bool propagated = false;
    try {
        (void)chunkdb::ReadIndexInventory(failed, good.size(), shape, kTable);
    } catch (const std::ios_base::failure&) { propagated = true; }
    assert(propagated);
    std::ostringstream unwritable;
    unwritable.setstate(std::ios_base::badbit);
    propagated = false;
    try {
        chunkdb::WriteIndexInventory(unwritable, header, coords);
    } catch (const std::runtime_error& e) {
        propagated = std::string(e.what()).find("failed to write") != std::string::npos;
    }
    assert(propagated);
    auto boundary = header;
    boundary.count = (std::numeric_limits<std::uint64_t>::max() - 52U) / 24U;
    assert(chunkdb::IndexInventoryFileBytes(boundary) == 52U + boundary.count * 24U);
    ++boundary.count;
    Reject([&] { (void)chunkdb::IndexInventoryFileBytes(boundary); }, "overflows");
}

void TestCompactionBounds() {
    assert(!chunkdb::ComputeTombstoneCompactionBounds(0, 100, 50));
    const auto normal = *chunkdb::ComputeTombstoneCompactionBounds(10, 100, 0);
    assert(normal.through == 9 && normal.retained_deletion_floor == 9);
    assert(chunkdb::TombstoneVersionDiscarded(8, normal.through));
    assert(chunkdb::TombstoneVersionDiscarded(9, normal.through));
    assert(!chunkdb::TombstoneVersionDiscarded(10, normal.through));
    const auto later_lower = *chunkdb::ComputeTombstoneCompactionBounds(10, 100, 50);
    assert(later_lower.through == 9 && later_lower.retained_deletion_floor == 50);
    assert(!chunkdb::TombstoneVersionDiscarded(40, later_lower.through));
    // Redo reloads the exact operation bound, never the higher cumulative floor.
    const auto shape = Shape(2);
    const chunkdb::IndexInventoryHeader decision{kTable, 2, true, 50, 9, 0};
    const auto recovered = ReadInventory(InventoryFixture(decision, {}), shape);
    for (const auto revision : {8U, 9U, 10U, 40U, 50U})
        assert(chunkdb::TombstoneVersionDiscarded(revision, recovered.compaction_through) ==
               chunkdb::TombstoneVersionDiscarded(revision, later_lower.through));
    const auto clamped = *chunkdb::ComputeTombstoneCompactionBounds(10, 5, 0);
    assert(clamped.through == 5 && chunkdb::TombstoneVersionDiscarded(5, clamped.through));
    assert(!chunkdb::TombstoneVersionDiscarded(6, clamped.through));
    const auto one = *chunkdb::ComputeTombstoneCompactionBounds(1, 100, 50);
    assert(one.through == 0 && one.retained_deletion_floor == 50);
    assert(!chunkdb::TombstoneVersionDiscarded(0, one.through));
    const auto max = std::numeric_limits<std::uint64_t>::max();
    const auto upper = *chunkdb::ComputeTombstoneCompactionBounds(max, max, 0);
    assert(upper.through == max - 1U && upper.retained_deletion_floor == max - 1U);
    assert(chunkdb::TombstoneVersionDiscarded(max - 1U, upper.through));
    assert(!chunkdb::TombstoneVersionDiscarded(max, upper.through));
}

chunkdb::CheckedChunkIndexLog ReadLog(const Bytes& bytes, const chunkdb::Geometry& geometry,
                                    const chunkdb::LargeChunkCoord& large, bool exceptions = false,
                                    std::optional<std::uint64_t> measured_bytes = std::nullopt) {
    std::istringstream input(std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size()), std::ios::binary);
    if (exceptions) input.exceptions(std::ios::badbit | std::ios::failbit);
    return chunkdb::ReadChunkIndexLog(input, measured_bytes.value_or(bytes.size()), geometry, kTable, large);
}

void TestLogRecovery() {
    using Kind = chunkdb::ChunkIndexKind;
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        const auto geometry = Shape(dims);
        const chunkdb::ChunkCoord coord{-5, -8, dims == 3 ? -3 : 0};
        const auto large = geometry.ChunkToLarge(coord);
        auto bytes = HeaderFixture(dims, large);
        const auto header_bytes = bytes.size();
        assert(ReadLog(bytes, geometry, large).latest.empty());
        assert(ReadLog(bytes, geometry, large, true).valid_bytes == header_bytes);
        const auto append = [&](const chunkdb::ChunkIndexRecord& record) {
            const auto encoded = RecordFixture(dims, record);
            bytes.insert(bytes.end(), encoded.begin(), encoded.end());
        };
        const chunkdb::ChunkIndexRecord live{Kind::kLive, coord, 7, 100, 1};
        const chunkdb::ChunkIndexRecord deleted{Kind::kDeleted, coord, 7, 100, 1};
        const chunkdb::ChunkIndexRecord forgotten{Kind::kForget, coord, 7, 100, 1};
        append(live);
        append(live);  // An identical publication is idempotent.
        append(deleted);
        append(deleted);
        append(forgotten);
        append(forgotten);
        const auto old_size = bytes.size();
        auto recovered = ReadLog(bytes, geometry, large);
        assert(recovered.valid_bytes == old_size && !recovered.interrupted_tail);
        assert(recovered.latest.size() == 1 && recovered.latest.at(coord).kind == Kind::kForget);

        const auto resurrection = RecordFixture(dims, {Kind::kLive, coord, 8, 101, 1});
        // Every prefix a crash can leave preserves the last complete decision.
        for (std::size_t cut = 1; cut < resurrection.size(); ++cut) {
            auto torn = bytes;
            torn.insert(torn.end(), resurrection.begin(), resurrection.begin() + static_cast<std::ptrdiff_t>(cut));
            recovered = ReadLog(torn, geometry, large);
            assert(recovered.valid_bytes == old_size && recovered.interrupted_tail);
            assert(recovered.latest.at(coord).kind == Kind::kForget);
            // A complete later append after that prefix proves interior damage.
            torn.insert(torn.end(), resurrection.begin(), resurrection.end());
            Reject([&] { (void)ReadLog(torn, geometry, large); }, "index log");
        }
        append({Kind::kLive, coord, 8, 101, 1});
        recovered = ReadLog(bytes, geometry, large);
        assert(recovered.latest.at(coord).revision == 8 && recovered.latest.at(coord).kind == Kind::kLive);
        assert(recovered.valid_bytes == bytes.size() && !recovered.interrupted_tail);

        const auto reject_append = [&](chunkdb::ChunkIndexRecord record, const std::string& reason) {
            auto bad = bytes;
            const auto encoded = RecordFixture(dims, record);
            bad.insert(bad.end(), encoded.begin(), encoded.end());
            Reject([&] { (void)ReadLog(bad, geometry, large); }, reason);
        };
        reject_append(live, "backwards");
        reject_append({Kind::kDeleted, coord, 8, 102, 1}, "conflicts");
        reject_append({Kind::kDeleted, coord, 9, 102, 2}, "newer than table");
        reject_append({Kind::kForget, coord, 8, 101, 1}, "equal-version transition");
        reject_append({Kind::kForget, coord, 9, 102, 1}, "changes the deleted version");
        auto first_forget = HeaderFixture(dims, large);
        const auto encoded_forget = RecordFixture(dims, forgotten);
        first_forget.insert(first_forget.end(), encoded_forget.begin(), encoded_forget.end());
        Reject([&] { (void)ReadLog(first_forget, geometry, large); }, "no deleted predecessor");
        auto backwards = HeaderFixture(dims, large);
        const auto d = RecordFixture(dims, deleted);
        const auto l = RecordFixture(dims, live);
        backwards.insert(backwards.end(), d.begin(), d.end());
        backwards.insert(backwards.end(), l.begin(), l.end());
        Reject([&] { (void)ReadLog(backwards, geometry, large); }, "equal-version transition");

        // An intact malformed final record is damage, including a repaired CRC.
        for (const auto offset : {0U, 4U, 5U}) {
            auto encoded = resurrection;
            encoded[offset] ^= 1;
            Checksum(&encoded);
            auto malformed = bytes;
            malformed.insert(malformed.end(), encoded.begin(), encoded.end());
            Reject([&] { (void)ReadLog(malformed, geometry, large); }, "index");
        }
        auto bad_tail = resurrection;
        bad_tail.back() ^= 1;
        auto interrupted = bytes;
        interrupted.insert(interrupted.end(), bad_tail.begin(), bad_tail.end());
        Reject([&] { (void)ReadLog(interrupted, geometry, large); }, "index log");
        interrupted.insert(interrupted.end(), resurrection.begin(), resurrection.end());
        Reject([&] { (void)ReadLog(interrupted, geometry, large); }, "index log");

        for (std::size_t cut = 0; cut < header_bytes; ++cut) {
            const Bytes truncated(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(cut));
            Reject([&] { (void)ReadLog(truncated, geometry, large); }, "header is truncated");
        }
        Reject([&] { (void)ReadLog(bytes, geometry, large, false, bytes.size() + 1U); }, "unreadable");
        Reject([&] { (void)ReadLog(bytes, geometry, large, false, bytes.size() - 1U); }, "bytes after");
        recovered = ReadLog(bytes, geometry, large, true);
        assert(recovered.valid_bytes == bytes.size() && !recovered.interrupted_tail);
    }
}

void TestCompleteDeletedBoundaryCannotBeDiscarded() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        const auto geometry = Shape(dims);
        const chunkdb::ChunkCoord coord{-5, -8, dims == 3 ? -3 : 0};
        const auto large = geometry.ChunkToLarge(coord);
        const auto header = HeaderFixture(dims, large);
        const auto deleted = RecordFixture(dims, {chunkdb::ChunkIndexKind::kDeleted, coord, 7, 100, 1});
        // Once collection removed the image and WAL, this synced final record
        // can be the only remaining producing identity. No complete damaged
        // boundary is permission to return an empty candidate index.
        for (std::size_t at = 0; at < deleted.size(); ++at) {
            auto damaged = deleted;
            damaged[at] ^= 1;
            auto bytes = header;
            bytes.insert(bytes.end(), damaged.begin(), damaged.end());
            Reject([&] { (void)ReadLog(bytes, geometry, large); }, "index log");
        }
    }
}

}  // namespace

int main() {
    TestShardHeadersAndRecords();
    TestInventory();
    TestCompactionBounds();
    TestCompleteDeletedBoundaryCannotBeDiscarded();
    TestLogRecovery();
}
