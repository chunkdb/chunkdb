#include <algorithm>
#include <cassert>
#include <functional>
#include <limits>
#include <stdexcept>
#include <string>

#include "chunkdb/chunk_columns.hpp"
#include "chunk_form.hpp"
#include "sectioned_chunk_form.hpp"
#include "typed_chunk_form.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;

void Reject(const std::function<void()>& call) {
    bool rejected = false;
    try { call(); } catch (const std::invalid_argument&) { rejected = true; }
    assert(rejected);
}

Column MakeColumn(std::uint32_t id, std::string name, ColumnKind kind, std::uint32_t width, bool nullable = false) {
    Column column;
    column.id = id; column.name = std::move(name); column.type = {kind, width}; column.nullable = nullable;
    return column;
}

TableSchema Schema(std::vector<Column> columns) {
    TableSchema schema;
    schema.columns = std::move(columns);
    for (const auto& column : schema.columns) schema.next_column_id = std::max(schema.next_column_id, column.id + 1U);
    return schema;
}

Geometry Shape(TableSchema blocks, TableSchema metadata = {}, std::uint32_t count = 3U) {
    GeometryConfig config;
    config.chunk_width_blocks = count; config.chunk_height_blocks = 1U;
    config.block_bits = FixedBitsPerBlock(blocks);
    return Geometry(config, std::move(blocks), std::move(metadata));
}

ChunkState Empty(const Geometry& geometry) {
    ChunkState state;
    state.payload.resize(geometry.ChunkPayloadBytes(), 0U);
    state.presence_bitmap.resize((geometry.ChunkBlockCount() + 7U) / 8U, 0U);
    state.chunk_columns = geometry.default_chunk_columns();
    return state;
}

void Present(ChunkState& state, std::size_t block) {
    state.presence_bitmap[block / 8U] |= static_cast<std::uint8_t>(1U << (block % 8U));
}

void Fixed(const Geometry& geometry, ChunkState& state, std::size_t index, std::size_t block, const ColumnValue& value) {
    const auto& column = geometry.layout().schema().columns[index];
    const auto* fixed = geometry.layout().FixedColumnAt(index);
    assert(fixed);
    const auto bytes = EncodeColumnValue(column, value);
    WriteValueBits(state.payload.data() + fixed->values, block * fixed->width, bytes.data(), fixed->width);
    if (column.nullable) state.payload[fixed->validity + block / 8U] |= static_cast<std::uint8_t>(1U << (block % 8U));
}

ColumnValue ReadFixed(const Geometry& geometry, const ChunkState& state, std::size_t index, std::size_t block) {
    const auto* fixed = geometry.layout().FixedColumnAt(index);
    assert(fixed);
    if (geometry.layout().schema().columns[index].nullable &&
        (state.payload[fixed->validity + block / 8U] & (1U << (block % 8U))) == 0U) return std::monostate{};
    Bytes value((fixed->width + 7U) / 8U, 0U);
    ReadValueBits(state.payload.data() + fixed->values, block * fixed->width, value.data(), fixed->width);
    return DecodeColumnValue(geometry.layout().schema().columns[index], value.data());
}

Bytes Hex(std::string_view text) {
    Bytes bytes;
    const auto digit = [](char c) { return c <= '9' ? c - '0' : c - 'a' + 10; };
    for (std::size_t i = 0U; i < text.size(); i += 2U)
        bytes.push_back(static_cast<std::uint8_t>(digit(text[i]) * 16 + digit(text[i + 1U])));
    return bytes;
}

Bytes WithoutMetadata(const Bytes& bytes, bool projected) {
    auto form = ParseChunkFormSections(bytes, bytes.size(), ChunkFormReadMode::kMutation);
    form.header.flags = projected ? kChunkFormProjected : 0U;
    std::erase_if(form.sections, [](const ChunkFormSection& section) {
        return section.kind == static_cast<std::uint16_t>(ChunkFormSectionKind::kChunkColumns);
    });
    return SerializeChunkFormSections(form.header, form.sections, std::numeric_limits<std::uint32_t>::max());
}

void GoldenAndIdentity() {
    auto blocks = Schema({MakeColumn(1, "n", ColumnKind::kUnsigned, 8)});
    TableSchema metadata;
    for (unsigned step = 0U; step < 6U; ++step) {
        blocks = RenameColumn(blocks, step % 2U == 0U ? "n" : "value", step % 2U == 0U ? "value" : "n");
        metadata = AdvanceOtherDomain(metadata, ColumnDomain::kChunk);
    }
    GeometryConfig config;
    config.chunk_width_blocks = 2U; config.chunk_height_blocks = 2U; config.chunk_depth_blocks = 2U;
    config.large_chunk_depth_chunks = 8U; config.dims = 3U; config.block_bits = 8U;
    const Geometry geometry(config, blocks, metadata);
    auto state = Empty(geometry);
    state.version = 42U; state.commit_time_ms = 11U; state.written = true;
    Present(state, 5U); Fixed(geometry, state, 0U, 5U, std::uint64_t{9});
    const auto expected = Hex("434b4346010000002a0000000000000007000000000000000b0000000000000003000000"
        "01000000480000000100000002000000490000000800000006000000510000001c000000"
        "20000000000009000001000000010000000800000000000000080000000000000000000000");
    const auto bytes = EncodeSectionedChunkState(geometry, state, kVarMaxChunkBytesLimit);
    assert(bytes == expected);
    const auto decoded = DecodeSectionedChunkState(geometry, bytes, kVarMaxChunkBytesLimit);
    assert(decoded.version == 0U && decoded.commit_time_ms == 0U && !decoded.written);
    assert(decoded.payload == state.payload && decoded.presence_bitmap == state.presence_bitmap);
    const std::string text(bytes.begin(), bytes.end());
    assert(ChunkFormSchemaVersion(text) == 7U);
    const auto facade = DecodeChunkForm(geometry, text);
    assert(facade.payload == state.payload && facade.presence_bitmap == state.presence_bitmap && !facade.written);
    Reject([&] { (void)ChunkFormSchemaVersion(std::string(16U, '\0')); });
    const auto private_bytes = EncodeSectionedChunkState(geometry, state, kVarMaxChunkBytesLimit, {}, true);
    const auto private_form = ParseChunkFormSections(private_bytes, 1024U, ChunkFormReadMode::kMutation);
    assert(private_form.header.flags == kChunkFormPrivate && private_form.header.chunk_version == 0U && private_form.header.commit_time_ms == 0U);
    for (std::size_t cut = 0U; cut < bytes.size(); ++cut)
        Reject([&] { (void)DecodeSectionedChunkState(geometry, std::span(bytes).first(cut), kVarMaxChunkBytesLimit); });
    Reject([&] { (void)EncodeSectionedChunkState(geometry, state, kVarMaxChunkBytesLimit, {}, false, bytes.size() - 1U); });
    auto historical = bytes;
    historical[16] = 6U;
    Reject([&] { (void)DecodeSectionedChunkState(geometry, historical, kVarMaxChunkBytesLimit); });
    auto form = ParseChunkFormSections(bytes, bytes.size(), ChunkFormReadMode::kMutation);
    form.sections.insert(form.sections.end() - 1, {static_cast<std::uint16_t>(ChunkFormSectionKind::kChunkColumns), {}});
    const auto empty_row = SerializeChunkFormSections(form.header, form.sections, 1024U);
    Reject([&] { (void)DecodeSectionedChunkState(geometry, empty_row, kVarMaxChunkBytesLimit); });
}

void ProjectionDescriptorsAndMetadata() {
    const auto geometry = Shape(Schema({MakeColumn(1, "n", ColumnKind::kUnsigned, 3),
        MakeColumn(2, "right", ColumnKind::kUnsigned, 8), MakeColumn(3, "label", ColumnKind::kText, 16, true)}),
        Schema({MakeColumn(8, "title", ColumnKind::kText, 16, true)}));
    auto state = Empty(geometry);
    Present(state, 0U);
    Fixed(geometry, state, 0U, 0U, std::uint64_t{3});
    Fixed(geometry, state, 1U, 0U, std::uint64_t{9});
    state.vars.Assign({3, 0}, Bytes{'h', 'i'});
    state.chunk_columns = std::make_shared<const ChunkColumnState>(EncodeChunkColumns(geometry.chunk_schema(), {std::string("base")}));
    const auto bytes = EncodeSectionedChunkState(geometry, state, 30U);
    Reject([&] { (void)EncodeSectionedChunkState(geometry, state, 29U); });
    Reject([&] { (void)DecodeSectionedChunkState(geometry, bytes, 29U); });
    (void)DecodeSectionedChunkState(geometry, bytes, kVarMaxChunkBytesLimit);
    const std::vector<std::size_t> projection{1U, 0U, 2U};
    const auto projected = EncodeSectionedChunkState(geometry, state, 30U, projection);
    const auto decoded = DecodeSectionedChunkState(geometry, projected, 30U, projection);
    assert(decoded.payload == state.payload && decoded.vars == state.vars);
    assert(decoded.chunk_columns && decoded.chunk_columns->bytes == state.chunk_columns->bytes);
    Reject([&] { (void)DecodeSectionedChunkState(geometry, projected, 30U); });
    const std::vector<std::size_t> wrong_order{0U, 1U, 2U};
    Reject([&] { (void)DecodeSectionedChunkState(geometry, projected, 30U, wrong_order); });
    const auto no_row = WithoutMetadata(projected, true);
    assert(!DecodeSectionedChunkState(geometry, no_row, 30U, projection).chunk_columns);
    Reject([&] { (void)DecodeSectionedChunkState(geometry, WithoutMetadata(bytes, false), 30U); });
    auto bad = bytes;
    const auto parsed = ParseChunkFormSections(bad, bad.size(), ChunkFormReadMode::kMutation);
    for (const auto& section : parsed.sections) if (section.kind == 6U) {
        const auto at = static_cast<std::size_t>(section.body.data() - bad.data());
        bad[at + 8U] = 4U; // Same array byte count and padding; a different schema width.
    }
    Reject([&] { (void)DecodeSectionedChunkState(geometry, bad, 30U); });
    auto forged = std::make_shared<ChunkColumnState>(*state.chunk_columns);
    forged->variable_bytes = 0U;
    state.chunk_columns = forged;
    Reject([&] { (void)EncodeSectionedChunkState(geometry, state, 29U); });
}

void PartialPresenceAndDefaults() {
    auto default_n = MakeColumn(2, "kept", ColumnKind::kUnsigned, 8);
    default_n.has_default = true; default_n.default_value = {5};
    auto default_label = MakeColumn(3, "label", ColumnKind::kText, 16, true);
    default_label.has_default = true; default_label.default_value = {'b'};
    const auto geometry = Shape(Schema({MakeColumn(1, "n", ColumnKind::kUnsigned, 8), default_n, default_label}),
        Schema({MakeColumn(8, "title", ColumnKind::kText, 16, true)}));
    auto before = Empty(geometry), patch = Empty(geometry);
    Present(before, 0U); Present(before, 1U);
    Fixed(geometry, before, 1U, 0U, std::uint64_t{7});
    Fixed(geometry, before, 1U, 1U, std::uint64_t{8});
    before.vars.Assign({3, 0}, Bytes{'o'}); before.vars.Assign({3, 1}, Bytes{'x'});
    before.chunk_columns = std::make_shared<const ChunkColumnState>(EncodeChunkColumns(geometry.chunk_schema(), {std::string("kept")}));
    Present(patch, 0U); Present(patch, 2U);
    Fixed(geometry, patch, 0U, 0U, std::uint64_t{9}); Fixed(geometry, patch, 0U, 2U, std::uint64_t{3});
    patch.chunk_columns = std::make_shared<const ChunkColumnState>(EncodeChunkColumns(geometry.chunk_schema(), {std::string("ignored")}));
    const std::vector<std::size_t> projection{0U};
    const auto next = MergeProjectedChunkState(geometry, before, patch, projection, 64U);
    assert(next.presence_bitmap == Bytes{5});
    assert(next.chunk_columns == before.chunk_columns);
    assert(ReadFixed(geometry, next, 0U, 0U) == ColumnValue(std::uint64_t{9}));
    assert(ReadFixed(geometry, next, 1U, 0U) == ColumnValue(std::uint64_t{7}));
    assert(ReadFixed(geometry, next, 1U, 1U) == ColumnValue(std::uint64_t{0}));
    assert(ReadFixed(geometry, next, 1U, 2U) == ColumnValue(std::uint64_t{5}));
    assert(next.vars.Find({3, 0}) && (*next.vars.Find({3, 0}))[0] == 'o');
    assert(!next.vars.Find({3, 1}));
    assert(next.vars.Find({3, 2}) && (*next.vars.Find({3, 2}))[0] == 'b');
    default_n.required = true; default_n.has_default = false; default_n.default_value.clear();
    const auto required = Shape(Schema({MakeColumn(1, "n", ColumnKind::kUnsigned, 8), default_n}));
    auto missing_before = Empty(required), missing_patch = Empty(required);
    Present(missing_patch, 0U);
    Reject([&] { (void)MergeProjectedChunkState(required, missing_before, missing_patch, projection, 64U); });
}

void LoweredQuotaAllowsShrinking() {
    const auto geometry = Shape(Schema({MakeColumn(1, "n", ColumnKind::kUnsigned, 8),
        MakeColumn(2, "label", ColumnKind::kText, 16, true)}), Schema({MakeColumn(8, "title", ColumnKind::kText, 16, true)}), 1U);
    auto before = Empty(geometry), patch = Empty(geometry);
    Present(before, 0U); Present(patch, 0U);
    before.vars.Assign({2, 0}, Bytes{'h', 'i'});
    before.chunk_columns = std::make_shared<const ChunkColumnState>(EncodeChunkColumns(geometry.chunk_schema(), {std::string("base")}));
    patch.vars.Assign({2, 0}, Bytes{'x'});
    const std::vector<std::size_t> projection{1U};
    const auto smaller = MergeProjectedChunkState(geometry, before, patch, projection, 25U);
    assert(smaller.vars.encoded_size() == 13U);
    patch.vars.Assign({2, 0}, Bytes{'x', 'y', 'z'});
    Reject([&] { (void)MergeProjectedChunkState(geometry, before, patch, projection, 25U); });
}

void NullableProjectedFixedColumns() {
    auto required_default = MakeColumn(3, "kept", ColumnKind::kUnsigned, 8);
    required_default.required = true; required_default.has_default = true; required_default.default_value = {5};
    const auto geometry = Shape(Schema({MakeColumn(1, "small", ColumnKind::kUnsigned, 3, true),
        MakeColumn(2, "wide", ColumnKind::kUnsigned, 9, true), required_default}));
    auto before = Empty(geometry), patch = Empty(geometry);
    Present(before, 0U);
    Fixed(geometry, before, 0U, 0U, std::uint64_t{5});
    Fixed(geometry, before, 2U, 0U, std::uint64_t{4});
    Present(patch, 0U); Present(patch, 2U);
    Fixed(geometry, patch, 1U, 0U, std::uint64_t{511});
    Fixed(geometry, patch, 0U, 2U, std::uint64_t{3});
    const std::vector<std::size_t> projection{1U, 0U};
    const auto encoded = EncodeSectionedChunkState(geometry, patch, kVarMaxChunkBytesLimit, projection);
    const auto decoded = DecodeSectionedChunkState(geometry, encoded, kVarMaxChunkBytesLimit, projection);
    assert(decoded.payload == patch.payload);
    const auto merged = MergeProjectedChunkState(geometry, before, decoded, projection, 64U);
    assert(ReadFixed(geometry, merged, 0U, 0U) == ColumnValue(std::monostate{}));
    assert(ReadFixed(geometry, merged, 1U, 0U) == ColumnValue(std::uint64_t{511}));
    assert(ReadFixed(geometry, merged, 0U, 2U) == ColumnValue(std::uint64_t{3}));
    assert(ReadFixed(geometry, merged, 1U, 2U) == ColumnValue(std::monostate{}));
    assert(ReadFixed(geometry, merged, 2U, 0U) == ColumnValue(std::uint64_t{4}));
    // Partial creation uses an unnamed column's declared DEFAULT, including
    // REQUIRED columns, rather than requiring an assignment for that name.
    assert(ReadFixed(geometry, merged, 2U, 2U) == ColumnValue(std::uint64_t{5}));
}

// The form as separate section bodies joined by SerializeChunkFormSections.
Bytes ReferenceForm(const Geometry& geometry, const ChunkState& state, std::span<const std::size_t> projection,
    bool private_state, const ChunkFormScale* scale) {
    const auto& layout = geometry.layout();
    const auto blocks = geometry.ChunkBlockCount();
    const auto put32 = [](Bytes& bytes, std::uint32_t value) {
        for (unsigned bit = 0U; bit < 32U; bit += 8U) bytes.push_back(static_cast<std::uint8_t>(value >> bit));
    };
    std::vector<std::size_t> indexes(projection.begin(), projection.end());
    if (indexes.empty()) for (std::size_t index = 0U; index < layout.schema().columns.size(); ++index) indexes.push_back(index);
    Bytes fixed, validity, vars, descriptors, scale_body;
    std::vector<std::uint32_t> selected_vars;
    std::uint32_t count = 0U;
    Bytes spans;
    for (const auto index : indexes) {
        const auto& column = layout.schema().columns[index];
        const auto* span = layout.FixedColumnAt(index);
        if (span == nullptr) { selected_vars.push_back(column.id); continue; }
        ++count;
        const auto length = (blocks * span->width + 7U) / 8U;
        put32(spans, column.id); put32(spans, span->width);
        put32(spans, static_cast<std::uint32_t>(fixed.size())); put32(spans, static_cast<std::uint32_t>(length));
        put32(spans, column.nullable ? static_cast<std::uint32_t>(validity.size()) : 0U);
        put32(spans, column.nullable ? static_cast<std::uint32_t>(state.presence_bitmap.size()) : 0U);
        fixed.insert(fixed.end(), state.payload.begin() + static_cast<std::ptrdiff_t>(span->values),
            state.payload.begin() + static_cast<std::ptrdiff_t>(span->values + length));
        if (column.nullable) validity.insert(validity.end(), state.payload.begin() + static_cast<std::ptrdiff_t>(span->validity),
            state.payload.begin() + static_cast<std::ptrdiff_t>(span->validity + state.presence_bitmap.size()));
    }
    put32(descriptors, count);
    descriptors.insert(descriptors.end(), spans.begin(), spans.end());
    for (const auto entry : state.vars) {
        if (std::find(selected_vars.begin(), selected_vars.end(), entry.key.column_id) == selected_vars.end()) continue;
        put32(vars, entry.key.column_id); put32(vars, entry.key.block_index);
        put32(vars, static_cast<std::uint32_t>(entry.value.size()));
        vars.insert(vars.end(), entry.value.begin(), entry.value.end());
    }
    std::vector<ChunkFormSection> sections{{static_cast<std::uint16_t>(ChunkFormSectionKind::kPresence), state.presence_bitmap}};
    const auto add = [&](ChunkFormSectionKind kind, std::span<const std::uint8_t> body) {
        if (!body.empty()) sections.push_back({static_cast<std::uint16_t>(kind), body});
    };
    add(ChunkFormSectionKind::kFixedValues, fixed);
    add(ChunkFormSectionKind::kNullBits, validity);
    add(ChunkFormSectionKind::kVariableValues, vars);
    if (state.chunk_columns) add(ChunkFormSectionKind::kChunkColumns, state.chunk_columns->bytes);
    add(ChunkFormSectionKind::kBlockSpans, descriptors);
    if (scale != nullptr) {
        put32(scale_body, scale->factor);
        put32(scale_body, static_cast<std::uint32_t>(scale->reduction));
        add(ChunkFormSectionKind::kScale, scale_body);
    }
    ChunkFormHeader header;
    header.flags = static_cast<std::uint16_t>((projection.empty() ? 0U : kChunkFormProjected) |
        (private_state ? kChunkFormPrivate : 0U));
    header.schema_version = layout.schema().version;
    if (!private_state) { header.chunk_version = state.version; header.commit_time_ms = state.commit_time_ms; }
    return SerializeChunkFormSections(header, sections, std::numeric_limits<std::uint32_t>::max());
}

// The encoder writes every body in place: its forms are byte for byte those
// joined from separate section bodies, for each section kind.
void InPlaceEncodingMatchesSections() {
    const auto geometry = Shape(Schema({MakeColumn(1, "small", ColumnKind::kUnsigned, 3, true),
        MakeColumn(2, "label", ColumnKind::kText, 16, true), MakeColumn(3, "wide", ColumnKind::kUnsigned, 9),
        MakeColumn(4, "flag", ColumnKind::kUnsigned, 1, true), MakeColumn(5, "note", ColumnKind::kBytes, 16, true)}),
        Schema({MakeColumn(8, "title", ColumnKind::kText, 16, true)}), 11U);
    auto state = Empty(geometry);
    state.version = 9U; state.commit_time_ms = 4U; state.written = true;
    for (const std::size_t block : {0U, 3U, 8U, 10U}) {
        Present(state, block);
        Fixed(geometry, state, 0U, block, std::uint64_t{block % 8U});
        Fixed(geometry, state, 2U, block, std::uint64_t{500U - block});
        if (block != 3U) Fixed(geometry, state, 3U, block, std::uint64_t{1});
    }
    state.vars.Assign({2, 0}, Bytes{'h', 'i'});
    state.vars.Assign({2, 10}, Bytes{'z'});
    state.vars.Assign({5, 8}, Bytes{1, 2, 3});
    state.chunk_columns = std::make_shared<const ChunkColumnState>(EncodeChunkColumns(geometry.chunk_schema(), {std::string("base")}));
    const ChunkFormScale scale{.factor = 1U, .reduction = ChunkFormReduction::kMax};
    const std::vector<std::vector<std::size_t>> projections{{}, {2U, 0U}, {4U, 3U, 1U}, {2U}, {0U}};
    for (const auto& projection : projections) {
        for (const bool private_state : {false, true}) {
            for (const ChunkFormScale* with : {static_cast<const ChunkFormScale*>(nullptr), &scale}) {
                const auto expected = ReferenceForm(geometry, state, projection, private_state, with);
                const auto bytes = EncodeSectionedChunkState(geometry, state, kVarMaxChunkBytesLimit, projection,
                    private_state, std::numeric_limits<std::uint32_t>::max(), with);
                assert(bytes == expected);
                assert(SectionedChunkStateBytes(geometry, state, kVarMaxChunkBytesLimit, projection, with) == bytes.size());
                (void)EncodeSectionedChunkState(geometry, state, kVarMaxChunkBytesLimit, projection, private_state, bytes.size(), with);
                Reject([&] { (void)EncodeSectionedChunkState(geometry, state, kVarMaxChunkBytesLimit, projection,
                    private_state, bytes.size() - 1U, with); });
            }
        }
    }
    // Without chunk columns the form has no metadata section.
    const auto plain = Shape(Schema({MakeColumn(1, "n", ColumnKind::kUnsigned, 8, true)}), {}, 9U);
    auto bare = Empty(plain);
    Present(bare, 4U); Fixed(plain, bare, 0U, 4U, std::uint64_t{200});
    assert(EncodeSectionedChunkState(plain, bare, kVarMaxChunkBytesLimit) == ReferenceForm(plain, bare, {}, false, nullptr));
}
}  // namespace

int main() {
    GoldenAndIdentity();
    ProjectionDescriptorsAndMetadata();
    PartialPresenceAndDefaults();
    LoweredQuotaAllowsShrinking();
    NullableProjectedFixedColumns();
    InPlaceEncodingMatchesSections();
}
