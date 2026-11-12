#include "chunkdb/chunk_columns.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

#include "chunkdb/chunk_vars.hpp"
#include "sectioned_chunk_form.hpp"

namespace chunkdb {
namespace {

ColumnValue InitialValue(const Column& column) {
    if (column.has_default) {
        if (IsFixedWidth(column.type.kind)) return DecodeColumnValue(column, column.default_value.data());
        return DecodeVarValue(column, column.default_value);
    }
    if (column.nullable) return std::monostate{};
    if (!IsFixedWidth(column.type.kind)) return DecodeVarValue(column, {});
    const std::vector<std::uint8_t> zero((FixedWidthBits(column.type) + 7U) / 8U, 0U);
    return DecodeColumnValue(column, zero.data());
}

void Put(std::vector<std::uint8_t>* bytes, std::size_t at, std::uint64_t value, std::size_t width) {
    for (std::size_t i = 0U; i < width; ++i)
        (*bytes)[at + i] = static_cast<std::uint8_t>(value >> (8U * i));
}

std::vector<ChunkFormChunkColumn> Entries(std::span<const std::uint8_t> bytes) {
    if (bytes.empty()) return {};
    return ParseChunkFormChunkColumns(ChunkFormSections{
        .header = {}, .sections = {{static_cast<std::uint16_t>(ChunkFormSectionKind::kChunkColumns), bytes}}});
}

void AddVariableBytes(const Column& column, bool is_null, std::size_t length, std::size_t* total) {
    if (IsFixedWidth(column.type.kind) || is_null || (!column.nullable && length == 0U)) return;
    const auto bytes = kVarEntryHeaderBytes + length;
    if (bytes > kVarMaxChunkBytesLimit - *total)
        throw std::invalid_argument("chunk columns exceed the variable-byte limit");
    *total += bytes;
}

std::unordered_map<std::uint32_t, std::size_t> ColumnIndexes(const TableSchema& schema) {
    std::unordered_map<std::uint32_t, std::size_t> indexes;
    indexes.reserve(schema.columns.size());
    for (std::size_t i = 0U; i < schema.columns.size(); ++i) indexes.emplace(schema.columns[i].id, i);
    return indexes;
}

}  // namespace

std::size_t ValidateChunkColumns(const TableSchema& schema, std::span<const std::uint8_t> bytes) {
    ValidateChunkSchema(schema);
    const auto entries = Entries(bytes);
    if (entries.size() != schema.columns.size())
        throw std::invalid_argument("chunk columns must contain the complete schema row");
    const auto indexes = ColumnIndexes(schema);
    std::size_t variable_bytes = 0U;
    for (const auto& entry : entries) {
        const auto found = indexes.find(entry.column_id);
        if (found == indexes.end()) throw std::invalid_argument("unknown chunk column ID");
        const auto& column = schema.columns[found->second];
        if (entry.is_null) {
            if (!column.nullable) throw std::invalid_argument("chunk column " + column.name + " cannot be NULL");
        } else if (IsFixedWidth(column.type.kind)) {
            const auto width = FixedWidthBits(column.type);
            if (entry.value.size() != (width + 7U) / 8U)
                throw std::invalid_argument("chunk column " + column.name + " has the wrong scalar width");
            if (width % 8U != 0U && (entry.value.back() >> (width % 8U)) != 0U)
                throw std::invalid_argument("chunk column " + column.name + " has nonzero padding");
        } else {
            if (entry.value.size() > column.type.size)
                throw std::invalid_argument("chunk column " + column.name + " exceeds its value limit");
            if (column.type.kind == ColumnKind::kText && !IsUtf8(entry.value))
                throw std::invalid_argument("chunk column " + column.name + " is not UTF-8");
        }
        AddVariableBytes(column, entry.is_null, entry.value.size(), &variable_bytes);
    }
    return variable_bytes;
}

std::vector<ColumnValue> DecodeChunkColumns(const TableSchema& schema, std::span<const std::uint8_t> bytes) {
    (void)ValidateChunkColumns(schema, bytes);
    const auto indexes = ColumnIndexes(schema);
    std::vector<ColumnValue> values(schema.columns.size());
    for (const auto& entry : Entries(bytes)) {
        const auto at = indexes.at(entry.column_id);
        const auto& column = schema.columns[at];
        if (entry.is_null) values[at] = std::monostate{};
        else if (IsFixedWidth(column.type.kind)) values[at] = DecodeColumnValue(column, entry.value.data());
        else values[at] = DecodeVarValue(column, entry.value);
    }
    return values;
}

ChunkColumnState EncodeChunkColumns(const TableSchema& schema, const std::vector<ColumnValue>& values) {
    ValidateChunkSchema(schema);
    if (values.size() != schema.columns.size())
        throw std::invalid_argument("chunk column values must match the complete schema row");
    if (schema.columns.empty()) return {};
    std::vector<std::size_t> order;
    order.reserve(schema.columns.size());
    for (std::size_t i = 0U; i < schema.columns.size(); ++i) order.push_back(i);
    std::sort(order.begin(), order.end(), [&](std::size_t a, std::size_t b) {
        return schema.columns[a].id < schema.columns[b].id;
    });
    ChunkColumnState state{.bytes = std::vector<std::uint8_t>(4U + 16U * values.size(), 0U)};
    Put(&state.bytes, 0U, values.size(), 4U);
    for (std::size_t i = 0U; i < order.size(); ++i) {
        const auto at = order[i];
        const auto& column = schema.columns[at];
        const bool is_null = std::holds_alternative<std::monostate>(values[at]);
        if (is_null && !column.nullable)
            throw std::invalid_argument("chunk column " + column.name + " cannot be NULL");
        const auto value = is_null ? std::vector<std::uint8_t>{} : IsFixedWidth(column.type.kind) ?
            EncodeColumnValue(column, values[at]) : EncodeVarValue(column, values[at]);
        AddVariableBytes(column, is_null, value.size(), &state.variable_bytes);
        if (value.size() > std::numeric_limits<std::uint32_t>::max() - state.bytes.size())
            throw std::invalid_argument("chunk columns exceed the section byte limit");
        Put(&state.bytes, 4U + 16U * i, column.id, 4U);
        Put(&state.bytes, 8U + 16U * i, is_null ? 1U : 0U, 2U);
        Put(&state.bytes, 12U + 16U * i, state.bytes.size(), 4U);
        Put(&state.bytes, 16U + 16U * i, value.size(), 4U);
        state.bytes.insert(state.bytes.end(), value.begin(), value.end());
    }
    return state;
}

ChunkColumnState DefaultChunkColumns(const TableSchema& schema) {
    ValidateChunkSchema(schema);
    std::vector<ColumnValue> values;
    values.reserve(schema.columns.size());
    for (const auto& column : schema.columns) values.push_back(InitialValue(column));
    return EncodeChunkColumns(schema, values);
}

ChunkColumnState AssignChunkColumns(const TableSchema& schema, const ChunkColumnState* current,
    const std::vector<ColumnAssignment>& assignments, bool initializing) {
    std::vector<ColumnValue> values;
    if (current == nullptr) {
        const auto baseline = DefaultChunkColumns(schema);
        values = DecodeChunkColumns(schema, baseline.bytes);
    } else values = DecodeChunkColumns(schema, current->bytes);
    std::unordered_set<std::string> assigned;
    for (const auto& assignment : assignments) {
        if (!assigned.insert(assignment.column).second)
            throw std::invalid_argument("chunk column " + assignment.column + " is assigned twice");
        const auto found = std::find_if(schema.columns.begin(), schema.columns.end(), [&](const Column& column) {
            return column.name == assignment.column;
        });
        if (found == schema.columns.end()) throw std::invalid_argument("unknown chunk column " + assignment.column);
        values[static_cast<std::size_t>(found - schema.columns.begin())] = assignment.value;
    }
    if (initializing) for (const auto& column : schema.columns)
        if (column.required && !column.has_default && !assigned.contains(column.name))
            throw std::invalid_argument("chunk column " + column.name + " is REQUIRED");
    return EncodeChunkColumns(schema, values);
}

ChunkColumnState TranslateChunkColumns(const TableSchema& from, const TableSchema& to, const ChunkColumnState& state) {
    ValidateChunkSchema(to);
    if (to.version < from.version) throw std::invalid_argument("chunk columns cannot move to an earlier schema");
    if (SchemaAtVersion(to, from.version) != from)
        throw std::invalid_argument("chunk column schemas do not share the same history");
    if (to.version > from.version + 1U) {
        auto current = state;
        auto previous = from;
        for (auto version = from.version + 1U; version <= to.version; ++version) {
            auto next = SchemaAtVersion(to, version);
            current = TranslateChunkColumns(previous, next, current);
            previous = std::move(next);
        }
        return current;
    }
    const auto before = DecodeChunkColumns(from, state.bytes);
    const auto indexes = ColumnIndexes(from);
    std::vector<ColumnValue> after;
    after.reserve(to.columns.size());
    for (const auto& column : to.columns) {
        const auto found = indexes.find(column.id);
        if (found == indexes.end()) { after.push_back(InitialValue(column)); continue; }
        const auto& previous = from.columns[found->second];
        auto value = before[found->second];
        if (previous.type != column.type) {
            if (to.version != from.version + 1U)
                throw std::invalid_argument("chunk column type conversion needs one schema step");
            const auto& changes = to.history.back().changes;
            const auto change = std::find_if(changes.begin(), changes.end(), [&](const SchemaChange& candidate) {
                return candidate.kind == SchemaChange::Kind::kChangeType && candidate.column.id == column.id;
            });
            if (change == changes.end()) throw std::invalid_argument("chunk column conversion is missing its history");
            value = ConvertValue(previous, column, change->conversion, value);
        }
        after.push_back(std::move(value));
    }
    return EncodeChunkColumns(to, after);
}

}  // namespace chunkdb
