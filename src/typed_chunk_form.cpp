#include "typed_chunk_form.hpp"

#include <algorithm>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <unordered_set>

#include "chunkdb/chunk_columns.hpp"
#include "sectioned_chunk_form.hpp"

namespace chunkdb {
namespace {
using Bytes = std::vector<std::uint8_t>;
using Span = std::span<const std::uint8_t>;

[[noreturn]] void Invalid(const char* what) { throw std::invalid_argument(what); }

void Put32(Bytes& bytes, std::uint32_t value) {
    for (unsigned bit = 0U; bit < 32U; bit += 8U) bytes.push_back(static_cast<std::uint8_t>(value >> bit));
}

bool Bit(Span bytes, std::size_t at) { return (bytes[at / 8U] & (1U << (at % 8U))) != 0U; }
void SetBit(Bytes& bytes, std::size_t at, bool value) {
    const auto mask = static_cast<std::uint8_t>(1U << (at % 8U));
    bytes[at / 8U] = static_cast<std::uint8_t>((bytes[at / 8U] & ~mask) | (value ? mask : 0U));
}

bool Padding(Span bytes, std::size_t bits) {
    return bytes.size() == (bits + 7U) / 8U &&
        (bits % 8U == 0U || (bytes.back() >> (bits % 8U)) == 0U);
}

std::optional<Span> Section(const ChunkFormSections& form, ChunkFormSectionKind kind) {
    for (const auto& section : form.sections) {
        if (section.kind == static_cast<std::uint16_t>(kind)) return section.body;
    }
    return std::nullopt;
}

std::vector<std::size_t> Columns(const ChunkLayout& layout, std::span<const std::size_t> projection) {
    std::vector<std::size_t> indexes;
    if (projection.empty()) {
        indexes.resize(layout.schema().columns.size());
        std::iota(indexes.begin(), indexes.end(), 0U);
    } else indexes.assign(projection.begin(), projection.end());
    std::vector<bool> seen(layout.schema().columns.size(), false);
    for (const auto index : indexes) {
        if (index >= seen.size() || seen[index]) Invalid("invalid or duplicate block projection");
        seen[index] = true;
    }
    return indexes;
}

void VariableBound(std::size_t block_bytes, std::size_t metadata_bytes, std::size_t bound) {
    if (bound > kVarMaxChunkBytesLimit || block_bytes > bound || metadata_bytes > bound - block_bytes)
        Invalid("combined variable storage exceeds chunk limit");
}

void CheckBlockState(const Geometry& geometry, const ChunkState& state) {
    const auto& layout = geometry.layout();
    const auto blocks = geometry.ChunkBlockCount();
    if (state.payload.size() != layout.payload_bytes() || !Padding(state.presence_bitmap, blocks))
        Invalid("chunk state has invalid payload or presence shape");
    for (const auto& fixed : layout.fixed_columns()) {
        const auto bytes = (blocks * fixed.width + 7U) / 8U;
        if (!Padding(Span(state.payload).subspan(fixed.values, bytes), blocks * fixed.width))
            Invalid("chunk state has nonzero fixed padding");
        if (fixed.validity != ChunkLayout::kNoValidity &&
            !Padding(Span(state.payload).subspan(fixed.validity, state.presence_bitmap.size()), blocks))
            Invalid("chunk state has nonzero validity padding");
    }
    layout.RequireValidVars(state.vars, state.presence_bitmap);
}

std::shared_ptr<const ChunkColumnState> Metadata(const Geometry& geometry, const ChunkState& state) {
    if (state.chunk_columns) return state.chunk_columns;
    if (state.written && geometry.chunk_columns_require_initialization())
        Invalid("written chunk is missing required metadata");
    return geometry.default_chunk_columns();
}

std::size_t MetadataBytes(const Geometry& geometry, const std::shared_ptr<const ChunkColumnState>& metadata) {
    return ValidateChunkColumns(geometry.chunk_schema(), metadata ? Span(metadata->bytes) : Span{});
}

void AddVar(Bytes& output, VarKey key, Span value, std::size_t bound) {
    if (output.size() > bound || kVarEntryHeaderBytes > bound - output.size() ||
        value.size() > bound - output.size() - kVarEntryHeaderBytes)
        Invalid("variable storage exceeds chunk limit");
    Put32(output, key.column_id);
    Put32(output, key.block_index);
    Put32(output, static_cast<std::uint32_t>(value.size()));
    output.insert(output.end(), value.begin(), value.end());
}

struct EncodingPlan {
    std::shared_ptr<const ChunkColumnState> metadata;
    std::vector<std::size_t> indexes;
    std::unordered_set<std::uint32_t> selected_vars;
    std::size_t count = 0U;
    std::size_t fixed_size = 0U;
    std::size_t validity_size = 0U;
    std::size_t variable_size = 0U;
    std::size_t total_size = 0U;
};

EncodingPlan PlanEncoding(
    const Geometry& geometry, const ChunkState& state, std::size_t max_variable_bytes,
    std::span<const std::size_t> projection) {
    CheckBlockState(geometry, state);
    EncodingPlan plan;
    plan.metadata = Metadata(geometry, state);
    VariableBound(state.vars.encoded_size(), MetadataBytes(geometry, plan.metadata), max_variable_bytes);
    const auto& layout = geometry.layout();
    const auto blocks = geometry.ChunkBlockCount();
    plan.indexes = Columns(layout, projection);
    for (const auto index : plan.indexes) {
        if (const auto* fixed = layout.FixedColumnAt(index)) {
            ++plan.count;
            plan.fixed_size += (blocks * fixed->width + 7U) / 8U;
            if (layout.schema().columns[index].nullable) plan.validity_size += state.presence_bitmap.size();
        } else plan.selected_vars.insert(layout.schema().columns[index].id);
    }
    for (const auto entry : state.vars) if (plan.selected_vars.contains(entry.key.column_id))
        plan.variable_size += kVarEntryHeaderBytes + entry.value.size();
    const auto metadata_size = plan.metadata ? plan.metadata->bytes.size() : 0U;
    const auto section_count = 2U + static_cast<unsigned>(plan.fixed_size != 0U) +
        static_cast<unsigned>(plan.validity_size != 0U) + static_cast<unsigned>(plan.variable_size != 0U) +
        static_cast<unsigned>(metadata_size != 0U);
    plan.total_size = 36U + 12U * section_count + state.presence_bitmap.size() + plan.fixed_size +
        plan.validity_size + plan.variable_size + metadata_size + 4U + 24U * plan.count;
    return plan;
}
}  // namespace

std::size_t SectionedChunkStateBytes(const Geometry& geometry, const ChunkState& state,
    std::size_t max_variable_bytes, std::span<const std::size_t> projection) {
    return PlanEncoding(geometry, state, max_variable_bytes, projection).total_size;
}

std::vector<std::uint8_t> EncodeSectionedChunkState(
    const Geometry& geometry, const ChunkState& state, std::size_t max_variable_bytes,
    std::span<const std::size_t> projection, bool private_state, std::size_t max_form_bytes) {
    const auto plan = PlanEncoding(geometry, state, max_variable_bytes, projection);
    if (plan.total_size > std::min(max_form_bytes, static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())))
        Invalid("chunk form exceeds its byte limit");
    const auto& layout = geometry.layout();
    const auto blocks = geometry.ChunkBlockCount();
    Bytes fixed, validity, descriptors;
    fixed.reserve(plan.fixed_size); validity.reserve(plan.validity_size); descriptors.reserve(4U + 24U * plan.count);
    Put32(descriptors, static_cast<std::uint32_t>(plan.count));
    for (const auto index : plan.indexes) {
        const auto& column = layout.schema().columns[index];
        const auto* span = layout.FixedColumnAt(index);
        if (span == nullptr) continue;
        const auto length = (blocks * span->width + 7U) / 8U;
        Put32(descriptors, column.id);
        Put32(descriptors, span->width);
        Put32(descriptors, static_cast<std::uint32_t>(fixed.size()));
        Put32(descriptors, static_cast<std::uint32_t>(length));
        Put32(descriptors, column.nullable ? static_cast<std::uint32_t>(validity.size()) : 0U);
        Put32(descriptors, column.nullable ? static_cast<std::uint32_t>(state.presence_bitmap.size()) : 0U);
        const auto values = Span(state.payload).subspan(span->values, length);
        fixed.insert(fixed.end(), values.begin(), values.end());
        if (column.nullable) {
            const auto nulls = Span(state.payload).subspan(span->validity, state.presence_bitmap.size());
            validity.insert(validity.end(), nulls.begin(), nulls.end());
        }
    }
    Bytes vars;
    vars.reserve(plan.variable_size);
    for (const auto entry : state.vars) if (plan.selected_vars.contains(entry.key.column_id))
        AddVar(vars, entry.key, entry.value, max_variable_bytes);
    std::vector<ChunkFormSection> sections{{static_cast<std::uint16_t>(ChunkFormSectionKind::kPresence), state.presence_bitmap}};
    const auto add = [&](ChunkFormSectionKind kind, Span body) {
        if (!body.empty()) sections.push_back({static_cast<std::uint16_t>(kind), body});
    };
    add(ChunkFormSectionKind::kFixedValues, fixed);
    add(ChunkFormSectionKind::kNullBits, validity);
    add(ChunkFormSectionKind::kVariableValues, vars);
    if (plan.metadata) add(ChunkFormSectionKind::kChunkColumns, plan.metadata->bytes);
    add(ChunkFormSectionKind::kBlockSpans, descriptors);
    ChunkFormHeader header;
    header.flags = static_cast<std::uint16_t>((projection.empty() ? 0U : kChunkFormProjected) |
                                             (private_state ? kChunkFormPrivate : 0U));
    header.schema_version = layout.schema().version;
    if (!private_state) { header.chunk_version = state.version; header.commit_time_ms = state.commit_time_ms; }
    return SerializeChunkFormSections(header, sections, max_form_bytes);
}

ChunkState DecodeSectionedChunkState(
    const Geometry& geometry, Span bytes, std::size_t max_variable_bytes,
    std::span<const std::size_t> projection, std::size_t max_form_bytes) {
    const auto form = ParseChunkFormSections(bytes, max_form_bytes, ChunkFormReadMode::kMutation);
    const auto& layout = geometry.layout();
    if (form.header.schema_version != layout.schema().version) Invalid("chunk form schema version mismatch");
    if (((form.header.flags & kChunkFormProjected) != 0U) != !projection.empty())
        Invalid("chunk form projection flag differs from the statement");
    const auto blocks = geometry.ChunkBlockCount();
    const auto indexes = Columns(layout, projection);
    const auto spans = ParseChunkFormBlockColumns(form, blocks);
    std::unordered_set<std::uint32_t> variables;
    std::size_t next = 0U;
    ChunkState state;
    state.payload.resize(layout.payload_bytes(), 0U);
    const auto presence = *Section(form, ChunkFormSectionKind::kPresence);
    state.presence_bitmap.assign(presence.begin(), presence.end());
    for (const auto index : indexes) {
        const auto& column = layout.schema().columns[index];
        const auto* fixed = layout.FixedColumnAt(index);
        if (fixed == nullptr) { variables.insert(column.id); continue; }
        if (next == spans.size()) Invalid("missing fixed block descriptor");
        const auto& span = spans[next++];
        if (span.column_id != column.id || span.width_bits != fixed->width ||
            span.validity.empty() != !column.nullable)
            Invalid("fixed descriptor differs from schema or projection order");
        std::copy(span.values.begin(), span.values.end(), state.payload.begin() + static_cast<std::ptrdiff_t>(fixed->values));
        if (column.nullable) std::copy(span.validity.begin(), span.validity.end(),
            state.payload.begin() + static_cast<std::ptrdiff_t>(fixed->validity));
    }
    if (next != spans.size()) Invalid("unexpected fixed block descriptor");
    const auto vars = Section(form, ChunkFormSectionKind::kVariableValues).value_or(Span{});
    if (!vars.empty()) state.vars = ChunkVars::Decode(vars.data(), vars.size(), blocks);
    for (const auto entry : state.vars) if (!variables.contains(entry.key.column_id))
        Invalid("variable entry is outside the block projection");
    layout.RequireValidVars(state.vars, state.presence_bitmap);
    layout.ClearEmptyValues(state.presence_bitmap, &state.payload);
    const auto row = Section(form, ChunkFormSectionKind::kChunkColumns);
    std::size_t metadata_bytes = 0U;
    if (row) {
        (void)ParseChunkFormChunkColumns(form);
        metadata_bytes = ValidateChunkColumns(geometry.chunk_schema(), *row);
        if (!geometry.chunk_schema().columns.empty())
            state.chunk_columns = std::make_shared<const ChunkColumnState>(ChunkColumnState{Bytes(row->begin(), row->end()), metadata_bytes});
    } else if (projection.empty() && !geometry.chunk_schema().columns.empty()) {
        Invalid("full chunk form is missing its metadata row");
    }
    VariableBound(state.vars.encoded_size(), projection.empty() ? metadata_bytes : 0U, max_variable_bytes);
    return state;
}

ChunkState MergeProjectedChunkState(
    const Geometry& geometry, const ChunkState& before, const ChunkState& patch,
    std::span<const std::size_t> projection, std::size_t var_max_chunk_bytes) {
    RequireValidVarLimit(var_max_chunk_bytes);
    if (projection.empty()) Invalid("a block-only mutation needs a projection");
    CheckBlockState(geometry, before);
    CheckBlockState(geometry, patch);
    const auto& layout = geometry.layout();
    const auto indexes = Columns(layout, projection);
    std::vector<bool> selected(layout.schema().columns.size(), false);
    for (const auto index : indexes) selected[index] = true;
    std::unordered_set<std::uint32_t> selected_vars;
    for (const auto index : indexes) if (layout.FixedColumnAt(index) == nullptr)
        selected_vars.insert(layout.schema().columns[index].id);
    for (const auto entry : patch.vars) if (!selected_vars.contains(entry.key.column_id))
        Invalid("patch variable entry is outside the block projection");
    // An optional incoming metadata row is validated but never applied.
    if (patch.chunk_columns) (void)MetadataBytes(geometry, patch.chunk_columns);
    const auto metadata = Metadata(geometry, before);
    const auto metadata_bytes = MetadataBytes(geometry, metadata);
    VariableBound(before.vars.encoded_size(), metadata_bytes, kVarMaxChunkBytesLimit);
    const auto allowed = std::max(var_max_chunk_bytes, before.vars.encoded_size() + metadata_bytes);
    const auto block_bound = allowed - metadata_bytes;
    const auto blocks = geometry.ChunkBlockCount();
    bool new_blocks = false;
    for (std::size_t block = 0U; block < blocks; ++block) {
        if (Bit(patch.presence_bitmap, block) && !Bit(before.presence_bitmap, block)) { new_blocks = true; break; }
    }
    if (new_blocks) for (std::size_t index = 0U; index < selected.size(); ++index) {
        const auto& column = layout.schema().columns[index];
        if (!selected[index] && column.required && !column.has_default)
            throw std::invalid_argument("new blocks must supply REQUIRED column " + column.name);
    }
    ChunkState next;
    next.payload = before.payload;
    next.presence_bitmap = patch.presence_bitmap;
    next.chunk_columns = before.chunk_columns;
    for (const auto& fixed : layout.fixed_columns()) {
        const auto& column = layout.schema().columns[fixed.column];
        if (selected[fixed.column]) {
            const auto length = (blocks * fixed.width + 7U) / 8U;
            std::copy_n(patch.payload.begin() + static_cast<std::ptrdiff_t>(fixed.values), length,
                        next.payload.begin() + static_cast<std::ptrdiff_t>(fixed.values));
            if (column.nullable) std::copy_n(
                patch.payload.begin() + static_cast<std::ptrdiff_t>(fixed.validity), next.presence_bitmap.size(),
                next.payload.begin() + static_cast<std::ptrdiff_t>(fixed.validity));
        } else if (new_blocks) {
            const Bytes zero((fixed.width + 7U) / 8U, 0U);
            const auto& value = column.has_default ? column.default_value : zero;
            for (std::size_t block = 0U; block < blocks; ++block) {
                if (!Bit(next.presence_bitmap, block) || Bit(before.presence_bitmap, block)) continue;
                WriteValueBits(next.payload.data() + fixed.values, block * fixed.width, value.data(), fixed.width);
                if (column.nullable) {
                    const auto at = fixed.validity * 8U + block;
                    SetBit(next.payload, at, column.has_default);
                }
            }
        }
    }
    std::vector<std::size_t> variable_indexes;
    for (std::size_t index = 0U; index < selected.size(); ++index)
        if (layout.FixedColumnAt(index) == nullptr) variable_indexes.push_back(index);
    std::sort(variable_indexes.begin(), variable_indexes.end(), [&](std::size_t a, std::size_t b) {
        return layout.schema().columns[a].id < layout.schema().columns[b].id;
    });
    Bytes variables;
    auto old = before.vars.begin(), incoming = patch.vars.begin();
    for (const auto index : variable_indexes) {
        const auto& column = layout.schema().columns[index];
        if (selected[index]) {
            while (old != before.vars.end() && (*old).key.column_id == column.id) ++old;
            while (incoming != patch.vars.end() && (*incoming).key.column_id == column.id) {
                const auto entry = *incoming;
                ++incoming;
                AddVar(variables, entry.key, entry.value, block_bound);
            }
        } else if (new_blocks && column.has_default && (column.nullable || !column.default_value.empty())) {
            for (std::size_t block = 0U; block < blocks; ++block) {
                if (old != before.vars.end() && (*old).key.column_id == column.id && (*old).key.block_index == block) {
                    const auto entry = *old;
                    ++old;
                    if (Bit(next.presence_bitmap, block)) AddVar(variables, entry.key, entry.value, block_bound);
                } else if (Bit(next.presence_bitmap, block) && !Bit(before.presence_bitmap, block)) {
                    AddVar(variables, {column.id, static_cast<std::uint32_t>(block)}, column.default_value, block_bound);
                }
            }
        } else {
            while (old != before.vars.end() && (*old).key.column_id == column.id) {
                const auto entry = *old;
                ++old;
                if (Bit(next.presence_bitmap, entry.key.block_index)) AddVar(variables, entry.key, entry.value, block_bound);
            }
        }
    }
    if (!variables.empty()) next.vars = ChunkVars::Decode(variables.data(), variables.size(), blocks);
    layout.ClearEmptyValues(next.presence_bitmap, &next.payload);
    layout.RequireValidVars(next.vars, next.presence_bitmap);
    return next;
}

}  // namespace chunkdb
