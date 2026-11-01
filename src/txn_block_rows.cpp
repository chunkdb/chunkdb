#include "txn_block_rows.hpp"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <variant>

namespace chunkdb {
namespace {
std::pmr::memory_resource* Resource(std::pmr::memory_resource* resource) {
    if (!resource) throw std::invalid_argument("a transaction row needs a memory resource");
    return resource;
}
bool Bit(std::span<const std::uint8_t> bytes, std::size_t index) noexcept {
    return ((bytes[index / 8U] >> (index % 8U)) & 1U) != 0U;
}
void Shape(const ChunkLayout& layout, std::span<const std::uint8_t> payload,
    std::span<const std::uint8_t> presence, std::size_t index) {
    if (index >= layout.block_count() || payload.size() != layout.payload_bytes() ||
        presence.size() != (layout.block_count() + 7U) / 8U)
        throw std::invalid_argument("transaction block source has an invalid shape or index");
}
void CellValue(const Column& column, const TxnBlockCell& cell) {
    if (cell.column_id != column.id) throw std::invalid_argument("transaction row column identity differs");
    if (cell.null) {
        if (!column.nullable || !cell.bytes.empty()) throw std::invalid_argument("invalid transaction row NULL");
        return;
    }
    if (IsFixedWidth(column.type.kind)) {
        const auto width = FixedWidthBits(column.type);
        if (cell.bytes.size() != (width + 7U) / 8U ||
            (width % 8U != 0U && (cell.bytes.back() >> (width % 8U)) != 0U))
            throw std::invalid_argument("invalid transaction row fixed value");
    } else if (cell.bytes.size() > column.type.size ||
               (column.type.kind == ColumnKind::kText && !IsUtf8(cell.bytes))) {
        throw std::invalid_argument("invalid transaction row variable value");
    }
}
void SetCell(const Column& column, const ColumnValue& value, TxnBlockCell& cell) {
    cell.null = std::holds_alternative<std::monostate>(value);
    if (cell.null) {
        if (!column.nullable) throw std::invalid_argument("column " + column.name + " cannot be NULL");
        cell.bytes.clear();
        return;
    }
    if (IsFixedWidth(column.type.kind)) {
        cell.bytes.resize((FixedWidthBits(column.type) + 7U) / 8U);
        EncodeColumnValue(column, value, cell.bytes.data());
        return;
    }
    if (!ValueFits(column.type, value)) throw std::invalid_argument("invalid value for column " + column.name);
    if (const auto* text = std::get_if<std::string>(&value)) {
        const auto* begin = reinterpret_cast<const std::uint8_t*>(text->data());
        cell.bytes.assign(begin, begin + text->size());
    } else if (const auto* bytes = std::get_if<BytesValue>(&value)) {
        cell.bytes.assign(bytes->bytes.begin(), bytes->bytes.end());
    } else {
        throw std::invalid_argument("invalid variable value for column " + column.name);
    }
}
}  // namespace

TxnBlockCell::TxnBlockCell(std::pmr::memory_resource* resource) : bytes(Resource(resource)) {}
TxnBlockRow::TxnBlockRow(std::pmr::memory_resource* resource) : cells(Resource(resource)) {}

TxnBlockRow CaptureTxnBlockRow(const ChunkLayout& layout, std::span<const std::uint8_t> payload,
    std::span<const std::uint8_t> presence, const ChunkVars& vars, std::size_t block_index,
    std::pmr::memory_resource* resource) {
    Shape(layout, payload, presence, block_index);
    TxnBlockRow row(resource);
    row.schema_version = layout.schema().version;
    row.present = Bit(presence, block_index);
    if (!row.present) return row;
    row.cells.reserve(layout.schema().columns.size());
    for (std::size_t i = 0U; i < layout.schema().columns.size(); ++i) {
        const auto& column = layout.schema().columns[i];
        row.cells.emplace_back(resource);
        auto& cell = row.cells.back(); cell.column_id = column.id;
        if (const auto* fixed = layout.FixedColumnAt(i)) {
            cell.null = fixed->validity != ChunkLayout::kNoValidity && !Bit(payload, fixed->validity * 8U + block_index);
            if (!cell.null) {
                cell.bytes.resize((fixed->width + 7U) / 8U);
                ReadValueBits(payload.data(), fixed->values * 8U + block_index * fixed->width, cell.bytes.data(), fixed->width);
            }
        } else {
            const auto value = vars.Find({column.id, static_cast<std::uint32_t>(block_index)});
            cell.null = column.nullable && !value.has_value();
            if (value) cell.bytes.assign(value->begin(), value->end());
        }
    }
    ValidateTxnBlockRow(layout, row);
    return row;
}

TxnBlockRow CloneTxnBlockRow(const TxnBlockRow& row, std::pmr::memory_resource* resource) {
    TxnBlockRow result(resource); result.schema_version = row.schema_version; result.present = row.present;
    result.cells.reserve(row.cells.size());
    for (const auto& cell : row.cells) {
        result.cells.emplace_back(resource);
        auto& copy = result.cells.back(); copy.column_id = cell.column_id; copy.null = cell.null;
        copy.bytes.assign(cell.bytes.begin(), cell.bytes.end());
    }
    return result;
}

void ValidateTxnBlockRow(const ChunkLayout& layout, const TxnBlockRow& row) {
    if (row.schema_version != layout.schema().version ||
        row.cells.size() != (row.present ? layout.schema().columns.size() : 0U))
        throw std::invalid_argument("transaction row schema or shape differs");
    for (std::size_t i = 0U; i < row.cells.size(); ++i) {
        if (row.cells[i].bytes.get_allocator().resource() != row.resource())
            throw std::invalid_argument("transaction row values use another memory resource");
        CellValue(layout.schema().columns[i], row.cells[i]);
    }
}

TxnBlockRow AssignTxnBlockRow(const ChunkLayout& layout, const TxnBlockRow& current,
    std::span<const ColumnAssignment> assignments, std::pmr::memory_resource* resource) {
    ValidateTxnBlockRow(layout, current);
    auto next = CloneTxnBlockRow(current, resource);
    if (!next.present) {
        next.present = true; next.cells.reserve(layout.schema().columns.size());
        for (const auto& column : layout.schema().columns) {
            next.cells.emplace_back(resource);
            auto& cell = next.cells.back(); cell.column_id = column.id;
            cell.null = column.nullable && !column.has_default;
            if (column.has_default) cell.bytes.assign(column.default_value.begin(), column.default_value.end());
            else if (!cell.null && IsFixedWidth(column.type.kind)) cell.bytes.resize((FixedWidthBits(column.type) + 7U) / 8U);
        }
    }
    std::pmr::vector<std::uint8_t> given(layout.schema().columns.size(), 0U, resource);
    for (const auto& assignment : assignments) {
        const auto index = layout.FindColumn(assignment.column);
        if (index == std::string_view::npos || given[index] != 0U)
            throw std::invalid_argument("unknown or duplicate transaction row column");
        const auto& column = layout.schema().columns[index];
        if (layout.schema().pending && layout.schema().pending->column_id == column.id &&
            !ValueFits(layout.schema().pending->type, assignment.value))
            throw std::invalid_argument("transaction value does not fit the pending column type");
        SetCell(column, assignment.value, next.cells[index]); given[index] = 1U;
    }
    if (!current.present) {
        for (std::size_t i = 0U; i < given.size(); ++i)
            if (given[i] == 0U && layout.schema().columns[i].required)
                throw std::invalid_argument("a new transaction row must supply its REQUIRED columns");
    }
    ValidateTxnBlockRow(layout, next);
    return next;
}

bool MatchesTxnBlockRow(const ChunkLayout& layout, std::span<const std::uint8_t> payload,
    std::span<const std::uint8_t> presence, const ChunkVars& vars, std::size_t block_index,
    const TxnBlockRow& expected) {
    Shape(layout, payload, presence, block_index); ValidateTxnBlockRow(layout, expected);
    if (Bit(presence, block_index) != expected.present) return false;
    if (!expected.present) return true;
    for (std::size_t i = 0U; i < expected.cells.size(); ++i) {
        const auto& cell = expected.cells[i]; const auto& column = layout.schema().columns[i];
        if (const auto* fixed = layout.FixedColumnAt(i)) {
            const bool null = fixed->validity != ChunkLayout::kNoValidity && !Bit(payload, fixed->validity * 8U + block_index);
            if (null != cell.null) return false;
            if (!null) {
                for (std::size_t bit = 0U; bit < fixed->width; ++bit)
                    if (Bit(payload, fixed->values * 8U + block_index * fixed->width + bit) != Bit(cell.bytes, bit)) return false;
            }
        } else {
            const auto value = vars.Find({column.id, static_cast<std::uint32_t>(block_index)});
            if ((column.nullable && !value.has_value()) != cell.null) return false;
            if (!cell.null && (value ? !std::equal(value->begin(), value->end(), cell.bytes.begin(), cell.bytes.end()) : !cell.bytes.empty())) return false;
        }
    }
    return true;
}

}  // namespace chunkdb
