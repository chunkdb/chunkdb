#include "sectioned_chunk_form.hpp"

#include <cassert>
#include <cstdint>
#include <functional>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunkdb/schema.hpp"

namespace {

using Bytes = std::vector<std::uint8_t>;
using Sections = std::vector<std::pair<std::uint16_t, Bytes>>;
constexpr std::size_t kLimit = 1024U * 1024U;

void At(Bytes* out, std::size_t at, std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) (*out)[at + i] = static_cast<std::uint8_t>(value >> (8U * i));
}

// Independent fixture writer accepts arbitrary bodies and directory corruption.
Bytes Fixture(const Sections& sections, chunkdb::ChunkFormHeader header = {0, 42, 7, 1727786400123ULL}) {
    Bytes bytes(36U + 12U * sections.size(), 0);
    bytes[0] = 'C'; bytes[1] = 'K'; bytes[2] = 'C'; bytes[3] = 'F';
    At(&bytes, 4, 1, 2);
    At(&bytes, 6, header.flags, 2);
    At(&bytes, 8, header.chunk_version, 8);
    At(&bytes, 16, header.schema_version, 8);
    At(&bytes, 24, header.commit_time_ms, 8);
    At(&bytes, 32, sections.size(), 2);
    for (std::size_t i = 0; i < sections.size(); ++i) {
        At(&bytes, 36U + 12U * i, sections[i].first, 2);
        At(&bytes, 40U + 12U * i, bytes.size(), 4);
        At(&bytes, 44U + 12U * i, sections[i].second.size(), 4);
        bytes.insert(bytes.end(), sections[i].second.begin(), sections[i].second.end());
    }
    return bytes;
}

chunkdb::ChunkFormSections Parse(const Bytes& bytes, chunkdb::ChunkFormReadMode mode = chunkdb::ChunkFormReadMode::kRead) {
    return chunkdb::ParseChunkFormSections(bytes, kLimit, mode);
}

void Reject(const std::function<void()>& call, const std::string& reason) {
    try {
        call();
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()).find(reason) != std::string::npos);
        return;
    }
    assert(false && "expected chunk form rejection");
}

void TestFraming() {
    const auto bytes = Fixture({{1, {7}}, {2, {11, 22}}, {6, {0, 0, 0, 0}}});
    const auto parsed = Parse(bytes, chunkdb::ChunkFormReadMode::kMutation);
    assert(parsed.header.flags == 0 && parsed.header.chunk_version == 42 && parsed.header.schema_version == 7);
    assert(parsed.header.commit_time_ms == 1727786400123ULL);
    assert(parsed.sections.size() == 3 && parsed.sections[0].body[0] == 7 && parsed.sections[1].body[1] == 22);
    assert(parsed.sections[0].body.data() == bytes.data() + 72);
    assert(chunkdb::SerializeChunkFormSections(parsed.header, parsed.sections, kLimit) == bytes);
    assert(chunkdb::SerializeChunkFormSections(parsed.header, parsed.sections, bytes.size()) == bytes);
    Reject([&] { (void)chunkdb::SerializeChunkFormSections(parsed.header, parsed.sections, bytes.size() - 1); }, "byte limit");
    Reject([&] { (void)chunkdb::ParseChunkFormSections(bytes, bytes.size() - 1, chunkdb::ChunkFormReadMode::kRead); }, "byte limit");
    for (std::size_t length = 0; length < bytes.size(); ++length) {
        const Bytes shortened(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(length));
        Reject([&] { (void)Parse(shortened); }, "chunk form");
    }
    const auto corrupt = [&](std::size_t at, std::uint64_t value, unsigned width, const std::string& reason) {
        auto changed = bytes;
        At(&changed, at, value, width);
        Reject([&] { (void)Parse(changed); }, reason);
    };
    corrupt(0, 'X', 1, "CKCF");
    corrupt(4, 0, 2, "version");
    corrupt(4, 2, 2, "version");
    corrupt(6, 4, 2, "flags");
    corrupt(16, 0, 8, "schema version is zero");
    corrupt(34, 1, 2, "reserved");
    corrupt(32, 65, 2, "too many");
    corrupt(32, 4, 2, "directory is truncated");
    corrupt(38, 1, 2, "section flags");
    corrupt(48, 1, 2, "strictly increasing");
    corrupt(48, 0, 2, "strictly increasing");
    corrupt(40, 71, 4, "bounds");
    corrupt(40, 73, 4, "bounds");
    corrupt(40, std::numeric_limits<std::uint32_t>::max(), 4, "bounds");
    corrupt(44, std::numeric_limits<std::uint32_t>::max(), 4, "bounds");
    corrupt(52, 72, 4, "bounds");
    corrupt(56, 0, 4, "bounds");
    auto extra = bytes; extra.push_back(0);
    Reject([&] { (void)Parse(extra); }, "after last");
    Reject([&] { (void)Parse(Fixture({{1, {1}}})); }, "lacks presence");
    Reject([&] { (void)Parse(Fixture({{6, {0, 0, 0, 0}}})); }, "lacks presence");
    Reject([&] { (void)Parse(Bytes(16, 0)); }, "header is truncated");

    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    const auto exact = Parse(Fixture({{1, {1}}, {6, {0, 0, 0, 0}}}, {1, maximum, maximum, maximum}));
    assert(exact.header.chunk_version == maximum && exact.header.schema_version == maximum && exact.header.commit_time_ms == maximum);
    const auto private_bytes = Fixture({{1, {1}}, {6, {0, 0, 0, 0}}}, {3, 0, 7, 0});
    const auto private_form = Parse(private_bytes);
    assert(private_form.header.flags == 3 && private_form.header.chunk_version == 0 && private_form.header.commit_time_ms == 0);
    assert(chunkdb::SerializeChunkFormSections(private_form.header, private_form.sections, kLimit) == private_bytes);
    Reject([&] { (void)Parse(Fixture({{1, {1}}, {6, {0, 0, 0, 0}}}, {2, 1, 7, 0})); }, "producing commit");
    Reject([&] { (void)Parse(Fixture({{1, {1}}, {6, {0, 0, 0, 0}}}, {2, 0, 7, 1})); }, "producing commit");
    Reject([&] { (void)chunkdb::SerializeChunkFormSections({4, 0, 7, 0}, private_form.sections, kLimit); }, "flags");
    Reject([&] { (void)chunkdb::SerializeChunkFormSections({2, 1, 7, 0}, private_form.sections, kLimit); }, "producing commit");
    Reject([&] { (void)chunkdb::SerializeChunkFormSections({0, 1, 0, 0}, private_form.sections, kLimit); }, "schema version is zero");
    auto duplicate = parsed.sections; duplicate[1].kind = 1;
    Reject([&] { (void)chunkdb::SerializeChunkFormSections(parsed.header, duplicate, kLimit); }, "strictly increasing");
    auto missing = parsed.sections; missing.erase(missing.begin());
    Reject([&] { (void)chunkdb::SerializeChunkFormSections(parsed.header, missing, kLimit); }, "lacks presence");
    const std::vector<chunkdb::ChunkFormSection> too_many(65);
    Reject([&] { (void)chunkdb::SerializeChunkFormSections(parsed.header, too_many, kLimit); }, "too many");
    Reject([&] { (void)chunkdb::SerializeChunkFormSections(parsed.header, parsed.sections, 35); }, "byte limit");
}

void TestUnknownSections() {
    auto bytes = Fixture({{0, {}}, {1, {7}}, {2, {31, 41}}, {6, {0, 0, 0, 0}}, {99, {0xFF, 0, 8, 9}}});
    const auto parsed = Parse(bytes);
    assert(parsed.sections.size() == 5 && parsed.sections[2].body[0] == 31 && parsed.sections[4].body[3] == 9);
    Reject([&] { (void)Parse(bytes, chunkdb::ChunkFormReadMode::kMutation); }, "unknown_form_section");
    Reject([&] { (void)chunkdb::SerializeChunkFormSections(parsed.header, parsed.sections, kLimit); }, "unknown_form_section");
    At(&bytes, 88, 0xFFFFFFFFU, 4);  // Unknown final body still needs valid framing.
    Reject([&] { (void)Parse(bytes); }, "bounds");
    Reject([&] { (void)Parse(bytes, chunkdb::ChunkFormReadMode::kMutation); }, "bounds");
    Sections many{{1, {1}}, {6, {0, 0, 0, 0}}};
    for (std::uint16_t kind = 7; kind < 69; ++kind) many.emplace_back(kind, Bytes{});
    assert(Parse(Fixture(many)).sections.size() == 64);
    many.emplace_back(69, Bytes{});
    Reject([&] { (void)Parse(Fixture(many)); }, "too many");
}

void Descriptor(Bytes* spans, std::size_t index, std::uint32_t id, std::uint32_t width,
                std::uint32_t offset, std::uint32_t length, std::uint32_t null_offset = 0,
                std::uint32_t null_length = 0) {
    const auto base = 4U + index * 24U;
    At(spans, base, id, 4); At(spans, base + 4, width, 4);
    At(spans, base + 8, offset, 4); At(spans, base + 12, length, 4);
    At(spans, base + 16, null_offset, 4); At(spans, base + 20, null_length, 4);
}

void TestOpaqueColumnBetweenKnownNeighbours() {
    Bytes spans(76, 0); At(&spans, 0, 3, 4);
    // Requested order differs from stable IDs; the unknown middle column uses
    // width 9 across byte boundaries, not a guessed schema type width.
    Descriptor(&spans, 0, 20, 3, 0, 2);
    Descriptor(&spans, 1, 17, 9, 2, 4, 0, 1);
    Descriptor(&spans, 2, 4, 8, 6, 3);
    Bytes values{0, 0, 0, 0, 0, 0, 11, 22, 33};
    const auto set = [&](std::size_t start, unsigned width, std::uint32_t value) {
        for (unsigned bit = 0; bit < width; ++bit)
            if ((value & (1U << bit)) != 0) values[(start + bit) / 8U] |= static_cast<std::uint8_t>(1U << ((start + bit) % 8U));
    };
    set(0, 3, 1); set(3, 3, 5); set(6, 3, 7);
    set(16, 9, 511); set(25, 9, 256); set(34, 9, 1);
    auto bytes = Fixture({{1, {7}}, {2, values}, {3, {5}}, {6, spans}, {99, {0xAD}}});
    const auto columns = chunkdb::ParseChunkFormBlockColumns(Parse(bytes), 3);
    assert(columns.size() == 3 && columns[1].column_id == 17 && columns[1].width_bits == 9);
    assert(columns[1].validity.size() == 1 && columns[1].validity[0] == 5);
    chunkdb::Column left{.id = 20, .name = "left", .type = {chunkdb::ColumnKind::kUnsigned, 3}, .default_value = {}};
    chunkdb::Column right{.id = 4, .name = "right", .type = {chunkdb::ColumnKind::kUnsigned, 8}, .default_value = {}};
    const std::uint64_t expected_left[] = {1, 5, 7};
    const std::uint64_t expected_right[] = {11, 22, 33};
    for (std::size_t i = 0; i < 3; ++i) {
        const auto a = chunkdb::ChunkFormRawBlockValue(columns[0], 3, i);
        const auto b = chunkdb::ChunkFormRawBlockValue(columns[2], 3, i);
        assert(std::get<std::uint64_t>(chunkdb::DecodeColumnValue(left, a.data())) == expected_left[i]);
        assert(std::get<std::uint64_t>(chunkdb::DecodeColumnValue(right, b.data())) == expected_right[i]);
    }
    assert(chunkdb::ChunkFormRawBlockValue(columns[1], 3, 0) == (Bytes{255, 1}));
    assert(chunkdb::ChunkFormRawBlockValue(columns[1], 3, 1) == (Bytes{0, 1}));
    assert(chunkdb::ChunkFormRawBlockValue(columns[1], 3, 2) == (Bytes{1, 0}));
    Reject([&] { (void)chunkdb::ChunkFormRawBlockValue(columns[1], 3, 3); }, "out of range");

    const auto reject_spans = [&](std::size_t at, std::uint64_t value, const std::string& reason) {
        auto bad = spans; At(&bad, at, value, 4);
        const auto form = Fixture({{1, {7}}, {2, values}, {3, {5}}, {6, bad}});
        Reject([&] { (void)chunkdb::ParseChunkFormBlockColumns(Parse(form), 3); }, reason);
    };
    reject_spans(0, 1025, "count or length");
    reject_spans(0, 2, "count or length");
    reject_spans(4, 0, "unique");
    reject_spans(28, 20, "unique");
    reject_spans(8, 0, "width");
    reject_spans(8, 65536, "width");
    reject_spans(12, 0xFFFFFFFFU, "bounds");
    reject_spans(16, 0xFFFFFFFFU, "bounds");
    reject_spans(16, 1, "length");
    reject_spans(20, 1, "null offset");
    reject_spans(44, 0xFFFFFFFFU, "bounds");
    reject_spans(48, 2, "bounds");
    // A range shifted onto another column is individually in bounds and has
    // zero padding, but complete coverage must still reject the overlap.
    reject_spans(60, 5, "overlap");
    auto padding = values; padding[1] |= 0x80;
    auto bad = Fixture({{1, {7}}, {2, padding}, {3, {5}}, {6, spans}});
    Reject([&] { (void)chunkdb::ParseChunkFormBlockColumns(Parse(bad), 3); }, "padding");
    bad = Fixture({{1, {0x87}}, {2, values}, {3, {5}}, {6, spans}});
    Reject([&] { (void)chunkdb::ParseChunkFormBlockColumns(Parse(bad), 3); }, "padding");
    bad = Fixture({{1, {7}}, {2, values}, {3, {0x85}}, {6, spans}});
    Reject([&] { (void)chunkdb::ParseChunkFormBlockColumns(Parse(bad), 3); }, "padding");
    Reject([&] { (void)chunkdb::ParseChunkFormBlockColumns(Parse(bytes), 0); }, "block count");
    Reject([&] { (void)chunkdb::ParseChunkFormBlockColumns(Parse(bytes), 1048577); }, "block count");
    const auto variable_only = Fixture({{1, {7}}, {6, {0, 0, 0, 0}}});
    assert(chunkdb::ParseChunkFormBlockColumns(Parse(variable_only), 3).empty());
}

void TestChunkColumnDirectory() {
    Bytes row(84, 0); At(&row, 0, 5, 4);
    for (std::size_t i = 0; i < 5; ++i) {
        At(&row, 4U + 16U * i, i + 1, 4);
        At(&row, 12U + 16U * i, row.size(), 4);
        if (i < 3) {
            const Bytes value = i == 0 ? Bytes{11} : (i == 1 ? Bytes{0xFF, 0x01} : Bytes{33});
            At(&row, 16U + 16U * i, value.size(), 4);
            row.insert(row.end(), value.begin(), value.end());
        } else if (i == 3) {
            At(&row, 8U + 16U * i, 1, 2);  // Explicit NULL.
        }  // Last entry is non-NULL empty bytes at the same offset.
    }
    const auto bytes = Fixture({{1, {7}}, {5, row}, {6, {0, 0, 0, 0}}});
    const auto columns = chunkdb::ParseChunkFormChunkColumns(Parse(bytes));
    assert(columns.size() == 5 && columns[0].value[0] == 11 && columns[2].value[0] == 33);
    assert(columns[1].value.size() == 2 && columns[1].value[0] == 255 && columns[1].value[1] == 1);
    chunkdb::Column scalar{.id = 1, .name = "scalar", .type = {chunkdb::ColumnKind::kUnsigned, 8}, .default_value = {}};
    assert(std::get<std::uint64_t>(chunkdb::DecodeColumnValue(scalar, columns[0].value.data())) == 11);
    assert(std::get<std::uint64_t>(chunkdb::DecodeColumnValue(scalar, columns[2].value.data())) == 33);
    assert(columns[3].is_null && columns[3].value.empty());
    assert(!columns[4].is_null && columns[4].value.empty());
    const auto reject = [&](Bytes changed, const std::string& reason) {
        const auto form = Fixture({{1, {7}}, {5, changed}, {6, {0, 0, 0, 0}}});
        Reject([&] { (void)chunkdb::ParseChunkFormChunkColumns(Parse(form)); }, reason);
    };
    const auto corrupt = [&](std::size_t at, std::uint64_t value, unsigned width, const std::string& reason) {
        auto changed = row; At(&changed, at, value, width); reject(std::move(changed), reason);
    };
    corrupt(0, 1025, 4, "count or length");
    corrupt(0, 6, 4, "count or length");
    corrupt(4, 0, 4, "IDs");
    corrupt(20, 1, 4, "IDs");
    corrupt(8, 2, 2, "flags");
    corrupt(10, 1, 2, "reserved");
    corrupt(12, 83, 4, "bounds");
    corrupt(12, 85, 4, "bounds");
    corrupt(16, 0xFFFFFFFFU, 4, "bounds");
    corrupt(8, 1, 2, "NULL");
    auto extra = row; extra.push_back(0); reject(std::move(extra), "after last");
    reject({}, "truncated");
    reject({0}, "truncated");
    const auto omitted = Fixture({{1, {7}}, {6, {0, 0, 0, 0}}});
    assert(chunkdb::ParseChunkFormChunkColumns(Parse(omitted)).empty());
}

}  // namespace

int main() {
    TestFraming();
    TestUnknownSections();
    TestOpaqueColumnBetweenKnownNeighbours();
    TestChunkColumnDirectory();
}
