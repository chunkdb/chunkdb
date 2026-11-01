#include "sectioned_chunk_form.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>

#include "chunkdb/schema.hpp"

namespace chunkdb {
namespace {

constexpr std::size_t kHeaderBytes = 36;
constexpr std::size_t kDirectoryEntryBytes = 12;
constexpr std::size_t kMaxSections = 64;

std::uint64_t ReadLe(const std::uint8_t* at, unsigned bytes) noexcept {
    std::uint64_t value = 0;
    for (unsigned i = 0; i < bytes; ++i) value |= static_cast<std::uint64_t>(at[i]) << (8U * i);
    return value;
}

void WriteLe(std::vector<std::uint8_t>* out, std::uint64_t value, unsigned bytes) {
    for (unsigned i = 0; i < bytes; ++i) out->push_back(static_cast<std::uint8_t>(value >> (8U * i)));
}

void ValidateHeader(const ChunkFormHeader& header) {
    if (header.schema_version == 0)
        throw std::invalid_argument("chunk form schema version is zero");
    if ((header.flags & ~(kChunkFormProjected | kChunkFormPrivate)) != 0)
        throw std::invalid_argument("unknown chunk form flags");
    if ((header.flags & kChunkFormPrivate) != 0 &&
        (header.chunk_version != 0 || header.commit_time_ms != 0))
        throw std::invalid_argument("private chunk form has a producing commit");
}

void ValidateKinds(std::uint16_t kind, std::uint16_t previous, std::size_t index) {
    if (index != 0 && kind <= previous)
        throw std::invalid_argument("chunk form section kinds must be strictly increasing");
}

bool KnownKind(std::uint16_t kind) noexcept {
    return kind >= static_cast<std::uint16_t>(ChunkFormSectionKind::kPresence) &&
           kind <= static_cast<std::uint16_t>(ChunkFormSectionKind::kBlockSpans);
}

void RequireSections(bool presence, bool spans) {
    if (!presence || !spans) throw std::invalid_argument("chunk form lacks presence or block spans");
}

std::size_t WireLimit(std::size_t max_bytes) noexcept {
    return std::min(max_bytes, static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()));
}

std::span<const std::uint8_t> Body(const ChunkFormSections& form, ChunkFormSectionKind kind) {
    for (const auto& section : form.sections)
        if (section.kind == static_cast<std::uint16_t>(kind)) return section.body;
    return {};
}

void ValidatePadding(std::span<const std::uint8_t> body, std::uint64_t bits) {
    if (body.size() != (bits + 7U) / 8U) throw std::invalid_argument("chunk form array has wrong length");
    if (bits % 8U != 0 && (body.back() >> (bits % 8U)) != 0)
        throw std::invalid_argument("chunk form array has nonzero padding");
}

using Range = std::pair<std::size_t, std::size_t>;

void ValidateCoverage(std::vector<Range>* ranges, std::size_t length) {
    std::sort(ranges->begin(), ranges->end());
    std::size_t next = 0;
    for (const auto& range : *ranges) {
        if (range.first != next) throw std::invalid_argument("chunk form arrays overlap or leave unused bytes");
        next += range.second;
    }
    if (next != length) throw std::invalid_argument("chunk form arrays leave unused bytes");
}

std::span<const std::uint8_t> Array(
    std::span<const std::uint8_t> body, std::size_t offset, std::size_t length) {
    if (offset > body.size() || length > body.size() - offset)
        throw std::invalid_argument("chunk form array bounds are invalid");
    return body.subspan(offset, length);
}

}  // namespace

ChunkFormSections ParseChunkFormSections(
    std::span<const std::uint8_t> bytes, std::size_t max_bytes, ChunkFormReadMode mode) {
    if (bytes.size() > WireLimit(max_bytes)) throw std::invalid_argument("chunk form exceeds byte limit");
    if (bytes.size() < kHeaderBytes) throw std::invalid_argument("chunk form header is truncated");
    if (bytes[0] != 'C' || bytes[1] != 'K' || bytes[2] != 'C' || bytes[3] != 'F')
        throw std::invalid_argument("not a CKCF chunk form");
    if (ReadLe(bytes.data() + 4, 2) != 1) throw std::invalid_argument("unsupported chunk form version");
    ChunkFormHeader header{
        .flags = static_cast<std::uint16_t>(ReadLe(bytes.data() + 6, 2)),
        .chunk_version = ReadLe(bytes.data() + 8, 8),
        .schema_version = ReadLe(bytes.data() + 16, 8),
        .commit_time_ms = ReadLe(bytes.data() + 24, 8),
    };
    ValidateHeader(header);
    const auto count = static_cast<std::size_t>(ReadLe(bytes.data() + 32, 2));
    if (ReadLe(bytes.data() + 34, 2) != 0) throw std::invalid_argument("chunk form reserved field is nonzero");
    if (count > kMaxSections) throw std::invalid_argument("too many chunk form sections");
    const auto body_begin = kHeaderBytes + count * kDirectoryEntryBytes;
    if (bytes.size() < body_begin) throw std::invalid_argument("chunk form directory is truncated");

    std::size_t next = body_begin;
    std::uint16_t previous = 0;
    bool presence = false, spans = false, unknown = false;
    for (std::size_t i = 0; i < count; ++i) {
        const auto* entry = bytes.data() + kHeaderBytes + i * kDirectoryEntryBytes;
        const auto kind = static_cast<std::uint16_t>(ReadLe(entry, 2));
        ValidateKinds(kind, previous, i);
        previous = kind;
        if (ReadLe(entry + 2, 2) != 0) throw std::invalid_argument("unknown chunk form section flags");
        const auto offset = static_cast<std::size_t>(ReadLe(entry + 4, 4));
        const auto length = static_cast<std::size_t>(ReadLe(entry + 8, 4));
        if (offset != next || length > bytes.size() - next)
            throw std::invalid_argument("invalid chunk form section bounds");
        next += length;
        presence |= kind == static_cast<std::uint16_t>(ChunkFormSectionKind::kPresence);
        spans |= kind == static_cast<std::uint16_t>(ChunkFormSectionKind::kBlockSpans);
        unknown |= !KnownKind(kind);
    }
    if (next != bytes.size()) throw std::invalid_argument("bytes after last chunk form section");
    RequireSections(presence, spans);
    if (unknown && mode == ChunkFormReadMode::kMutation)
        throw std::invalid_argument("unknown_form_section");

    ChunkFormSections result{header, {}};
    result.sections.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        const auto* entry = bytes.data() + kHeaderBytes + i * kDirectoryEntryBytes;
        result.sections.push_back({
            static_cast<std::uint16_t>(ReadLe(entry, 2)),
            bytes.subspan(static_cast<std::size_t>(ReadLe(entry + 4, 4)),
                          static_cast<std::size_t>(ReadLe(entry + 8, 4))),
        });
    }
    return result;
}

std::vector<std::uint8_t> SerializeChunkFormSections(
    const ChunkFormHeader& header, std::span<const ChunkFormSection> sections,
    std::size_t max_bytes) {
    ValidateHeader(header);
    if (sections.size() > kMaxSections) throw std::invalid_argument("too many chunk form sections");
    const auto limit = WireLimit(max_bytes);
    auto total = kHeaderBytes + sections.size() * kDirectoryEntryBytes;
    if (total > limit) throw std::invalid_argument("chunk form exceeds byte limit");
    std::uint16_t previous = 0;
    bool presence = false, spans = false;
    for (std::size_t i = 0; i < sections.size(); ++i) {
        const auto& section = sections[i];
        ValidateKinds(section.kind, previous, i);
        previous = section.kind;
        if (!KnownKind(section.kind)) throw std::invalid_argument("unknown_form_section");
        if (section.body.size() > limit - total)
            throw std::invalid_argument("chunk form exceeds byte limit");
        total += section.body.size();
        presence |= section.kind == static_cast<std::uint16_t>(ChunkFormSectionKind::kPresence);
        spans |= section.kind == static_cast<std::uint16_t>(ChunkFormSectionKind::kBlockSpans);
    }
    RequireSections(presence, spans);

    std::vector<std::uint8_t> out;
    out.reserve(total);
    out.insert(out.end(), {'C', 'K', 'C', 'F'});
    WriteLe(&out, 1, 2);
    WriteLe(&out, header.flags, 2);
    WriteLe(&out, header.chunk_version, 8);
    WriteLe(&out, header.schema_version, 8);
    WriteLe(&out, header.commit_time_ms, 8);
    WriteLe(&out, sections.size(), 2);
    WriteLe(&out, 0, 2);
    auto offset = kHeaderBytes + sections.size() * kDirectoryEntryBytes;
    for (const auto& section : sections) {
        WriteLe(&out, section.kind, 2);
        WriteLe(&out, 0, 2);
        WriteLe(&out, offset, 4);
        WriteLe(&out, section.body.size(), 4);
        offset += section.body.size();
    }
    for (const auto& section : sections) out.insert(out.end(), section.body.begin(), section.body.end());
    return out;
}

std::vector<ChunkFormBlockColumn> ParseChunkFormBlockColumns(
    const ChunkFormSections& form, std::size_t block_count) {
    if (block_count == 0 || block_count > 1048576U)
        throw std::invalid_argument("chunk form block count is out of range");
    ValidatePadding(Body(form, ChunkFormSectionKind::kPresence), block_count);
    const auto descriptors = Body(form, ChunkFormSectionKind::kBlockSpans);
    if (descriptors.size() < 4) throw std::invalid_argument("chunk form block spans are truncated");
    const auto count = static_cast<std::size_t>(ReadLe(descriptors.data(), 4));
    if (count > kMaxColumnsPerTable || descriptors.size() != 4U + count * 24U)
        throw std::invalid_argument("chunk form block spans have invalid count or length");
    const auto values = Body(form, ChunkFormSectionKind::kFixedValues);
    const auto validity = Body(form, ChunkFormSectionKind::kNullBits);
    std::vector<ChunkFormBlockColumn> columns;
    std::vector<Range> value_ranges, validity_ranges;
    columns.reserve(count);
    value_ranges.reserve(count);
    validity_ranges.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        const auto* descriptor = descriptors.data() + 4U + 24U * i;
        const auto id = static_cast<std::uint32_t>(ReadLe(descriptor, 4));
        const auto width = static_cast<std::uint32_t>(ReadLe(descriptor + 4, 4));
        if (id == 0 || std::any_of(columns.begin(), columns.end(), [id](const auto& c) { return c.column_id == id; }))
            throw std::invalid_argument("chunk form block column IDs must be nonzero and unique");
        if (width == 0 || width > kMaxFixedBitsPerBlock)
            throw std::invalid_argument("chunk form block column width is out of range");
        const auto value_offset = static_cast<std::size_t>(ReadLe(descriptor + 8, 4));
        const auto value_length = static_cast<std::size_t>(ReadLe(descriptor + 12, 4));
        const auto null_offset = static_cast<std::size_t>(ReadLe(descriptor + 16, 4));
        const auto null_length = static_cast<std::size_t>(ReadLe(descriptor + 20, 4));
        const auto value_array = Array(values, value_offset, value_length);
        ValidatePadding(value_array, static_cast<std::uint64_t>(width) * block_count);
        const auto null_array = Array(validity, null_offset, null_length);
        if (null_length != 0) {
            ValidatePadding(null_array, block_count);
            validity_ranges.emplace_back(null_offset, null_length);
        } else if (null_offset != 0) {
            throw std::invalid_argument("nonnullable chunk form array has a null offset");
        }
        value_ranges.emplace_back(value_offset, value_length);
        columns.push_back({id, width, value_array, null_array});
    }
    ValidateCoverage(&value_ranges, values.size());
    ValidateCoverage(&validity_ranges, validity.size());
    return columns;
}

std::vector<std::uint8_t> ChunkFormRawBlockValue(
    const ChunkFormBlockColumn& column, std::size_t block_count, std::size_t block_index) {
    if (block_count == 0 || block_count > 1048576U || block_index >= block_count ||
        column.width_bits == 0 || column.width_bits > kMaxFixedBitsPerBlock)
        throw std::invalid_argument("chunk form block value is out of range");
    ValidatePadding(column.values, static_cast<std::uint64_t>(column.width_bits) * block_count);
    std::vector<std::uint8_t> value((column.width_bits + 7U) / 8U, 0);
    const auto begin = static_cast<std::uint64_t>(block_index) * column.width_bits;
    for (std::uint32_t bit = 0; bit < column.width_bits; ++bit)
        if ((column.values[(begin + bit) / 8U] & (1U << ((begin + bit) % 8U))) != 0)
            value[bit / 8U] |= static_cast<std::uint8_t>(1U << (bit % 8U));
    return value;
}

std::vector<ChunkFormChunkColumn> ParseChunkFormChunkColumns(const ChunkFormSections& form) {
    const auto body = Body(form, ChunkFormSectionKind::kChunkColumns);
    if (body.empty()) {
        // An omitted optional section differs from a present truncated one.
        for (const auto& section : form.sections)
            if (section.kind == static_cast<std::uint16_t>(ChunkFormSectionKind::kChunkColumns))
                throw std::invalid_argument("chunk form chunk columns are truncated");
        return {};
    }
    const auto count = ValidateChunkColumnDirectory(body);
    std::vector<ChunkFormChunkColumn> columns;
    columns.reserve(count);
    for (std::size_t i = 0; i < count; ++i) columns.push_back(ChunkColumnDirectoryEntry(body, i));
    return columns;
}

std::size_t ValidateChunkColumnDirectory(std::span<const std::uint8_t> body) {
    if (body.empty()) return 0U;
    if (body.size() < 4) throw std::invalid_argument("chunk form chunk columns are truncated");
    const auto count = static_cast<std::size_t>(ReadLe(body.data(), 4));
    if (count > kMaxColumnsPerTable || body.size() < 4U + 16U * count)
        throw std::invalid_argument("chunk form chunk columns have invalid count or length");
    auto next = 4U + 16U * count;
    std::uint32_t previous = 0;
    for (std::size_t i = 0; i < count; ++i) {
        const auto* entry = body.data() + 4U + 16U * i;
        const auto id = static_cast<std::uint32_t>(ReadLe(entry, 4));
        if (id <= previous) throw std::invalid_argument("chunk form chunk column IDs must be nonzero and increasing");
        previous = id;
        const auto flags = ReadLe(entry + 4, 2);
        if (flags > 1 || ReadLe(entry + 6, 2) != 0)
            throw std::invalid_argument("chunk form chunk column flags or reserved field are invalid");
        const auto offset = static_cast<std::size_t>(ReadLe(entry + 8, 4));
        const auto length = static_cast<std::size_t>(ReadLe(entry + 12, 4));
        if (offset != next || length > body.size() - next)
            throw std::invalid_argument("chunk form chunk column bounds are invalid");
        if (flags == 1 && length != 0) throw std::invalid_argument("NULL chunk column has value bytes");
        next += length;
    }
    if (next != body.size()) throw std::invalid_argument("bytes after last chunk form chunk column");
    return count;
}
ChunkFormChunkColumn ChunkColumnDirectoryEntry(std::span<const std::uint8_t> body, std::size_t index) noexcept {
    const auto* entry = body.data() + 4U + 16U * index;
    return {static_cast<std::uint32_t>(ReadLe(entry, 4)), ReadLe(entry + 4, 2) == 1,
        body.subspan(static_cast<std::size_t>(ReadLe(entry + 8, 4)),
            static_cast<std::size_t>(ReadLe(entry + 12, 4)))};
}

}  // namespace chunkdb
