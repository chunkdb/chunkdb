#include "aggregate.hpp"

#include <array>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <variant>

#include "chunkdb/protocol.hpp"
#include "statement_control.hpp"

namespace chunkdb {

namespace {

using Kind = cql::AggregateFunction::Kind;

// Blocks added between statement cancellation checks.
constexpr std::size_t kBlocksPerCheck = 65536U;
// The most bytes one RESP3 integer, big number or double takes: a sign, 39
// digits of a 128-bit integer, its prefix and CRLF.
constexpr std::size_t kValueBytes = 44U;

// a < b, with NaN above every other value.
[[nodiscard]] bool Less(double a, double b) noexcept {
    if (std::isnan(a)) return false;
    if (std::isnan(b)) return true;
    return a < b;
}

void AppendWide(std::string& out, __int128 value) {
    if (value >= std::numeric_limits<std::int64_t>::min() && value <= std::numeric_limits<std::int64_t>::max()) {
        Protocol::AppendInteger(out, static_cast<std::int64_t>(value));
        return;
    }
    const bool negative = value < 0;
    // The magnitude of the smallest value does not fit in __int128.
    auto magnitude = negative ? static_cast<unsigned __int128>(0) - static_cast<unsigned __int128>(value)
                              : static_cast<unsigned __int128>(value);
    std::array<char, 40U> digits{};
    std::size_t at = digits.size();
    do {
        digits[--at] = static_cast<char>('0' + static_cast<int>(magnitude % 10U));
        magnitude /= 10U;
    } while (magnitude != 0U);
    out += '(';
    if (negative) out += '-';
    out.append(digits.data() + at, digits.size() - at);
    out += "\r\n";
}

}  // namespace

Aggregator::Aggregator(const ChunkLayout& layout, std::span<const cql::AggregateFunction> functions)
    : block_count_(layout.block_count()), payload_bytes_(layout.payload_bytes()) {
    accumulators_.reserve(functions.size());
    for (const auto& function : functions) {
        Accumulator accumulator{.function = function};
        if (function.kind != Kind::kCount) {
            const auto index = layout.FindColumn(function.column);
            if (index == std::string_view::npos)
                throw std::invalid_argument("the table has no column " + function.column);
            const auto& column = layout.schema().columns[index];
            const auto kind = column.type.kind;
            if (kind != ColumnKind::kUnsigned && kind != ColumnKind::kSigned &&
                kind != ColumnKind::kFloat32 && kind != ColumnKind::kFloat64)
                throw std::invalid_argument(cql::AggregateFunctionName(function) +
                    " takes an integer or float column; " + function.column + " is not one");
            accumulator.fixed = layout.FixedColumnAt(index);
            accumulator.column = &column;
            accumulator.floating = kind == ColumnKind::kFloat32 || kind == ColumnKind::kFloat64;
        }
        accumulators_.push_back(accumulator);
    }
}

void Aggregator::Add(std::span<const std::uint8_t> payload, std::span<const std::uint8_t> presence) {
    if (payload.size() < payload_bytes_ || presence.size() < (block_count_ + 7U) / 8U)
        throw std::logic_error("a chunk state does not match the table's layout");
    for (std::size_t byte = 0U; byte < (block_count_ + 7U) / 8U; ++byte) {
        if (byte % (kBlocksPerCheck / 8U) == 0U) CheckStatementCancellation();
        const auto bits = presence[byte];
        if (bits == 0U) continue;
        for (std::size_t bit = 0U; bit < 8U; ++bit) {
            const auto block = byte * 8U + bit;
            if (((bits >> bit) & 1U) == 0U || block >= block_count_) continue;
            ++blocks_;
            for (auto& accumulator : accumulators_)
                if (accumulator.fixed != nullptr) AddValue(accumulator, payload.data(), block);
        }
    }
}

void Aggregator::AddValue(Accumulator& accumulator, const std::uint8_t* payload, std::size_t block) {
    const auto& fixed = *accumulator.fixed;
    if (fixed.validity != ChunkLayout::kNoValidity && ((payload[fixed.validity + block / 8U] >> (block % 8U)) & 1U) == 0U)
        return;
    std::array<std::uint8_t, 8U> bytes{};
    ReadValueBits(payload, fixed.values * 8U + block * fixed.width, bytes.data(), fixed.width);
    const auto value = DecodeColumnValue(*accumulator.column, bytes.data());
    const auto kind = accumulator.function.kind;
    const bool first = accumulator.values == 0U;
    ++accumulator.values;
    if (accumulator.floating) {
        const double number = std::holds_alternative<float>(value) ? static_cast<double>(std::get<float>(value))
                                                                    : std::get<double>(value);
        if (kind == Kind::kSum || kind == Kind::kAvg) accumulator.number += number;
        else if (first || (kind == Kind::kMin ? Less(number, accumulator.number) : Less(accumulator.number, number)))
            accumulator.number = number;
        return;
    }
    const __int128 integer = std::holds_alternative<std::uint64_t>(value)
        ? static_cast<__int128>(std::get<std::uint64_t>(value)) : static_cast<__int128>(std::get<std::int64_t>(value));
    if (kind == Kind::kSum || kind == Kind::kAvg) {
        if (__builtin_add_overflow(accumulator.integer, integer, &accumulator.integer))
            throw std::out_of_range(cql::AggregateFunctionName(accumulator.function) +
                " is outside the 128-bit integer range");
    } else if (first || (kind == Kind::kMin ? integer < accumulator.integer : integer > accumulator.integer)) {
        accumulator.integer = integer;
    }
}

void Aggregator::Reset() noexcept {
    blocks_ = 0U;
    for (auto& accumulator : accumulators_) {
        accumulator.values = 0U;
        accumulator.integer = 0;
        accumulator.number = 0.0;
    }
}

void Aggregator::AppendResult(std::string& out, std::size_t index) const {
    const auto& accumulator = accumulators_.at(index);
    const auto kind = accumulator.function.kind;
    if (kind == Kind::kCount) {
        Protocol::AppendInteger(out, blocks_);
    } else if (accumulator.values == 0U) {
        Protocol::AppendNull(out);
    } else if (kind == Kind::kAvg) {
        const double sum = accumulator.floating ? accumulator.number : static_cast<double>(accumulator.integer);
        Protocol::AppendDouble(out, sum / static_cast<double>(accumulator.values));
    } else if (accumulator.floating) {
        // MIN and MAX of f32 keep the column's type.
        if (kind != Kind::kSum && accumulator.column->type.kind == ColumnKind::kFloat32)
            Protocol::AppendFloat(out, static_cast<float>(accumulator.number));
        else
            Protocol::AppendDouble(out, accumulator.number);
    } else if (kind == Kind::kSum) {
        AppendWide(out, accumulator.integer);
    } else if (accumulator.column->type.kind == ColumnKind::kUnsigned) {
        Protocol::AppendInteger(out, static_cast<std::uint64_t>(accumulator.integer));
    } else {
        Protocol::AppendInteger(out, static_cast<std::int64_t>(accumulator.integer));
    }
}

void Aggregator::AppendResults(std::string& out) const {
    for (std::size_t index = 0U; index < accumulators_.size(); ++index) AppendResult(out, index);
}

std::size_t Aggregator::ResultBytesBound() const noexcept {
    return accumulators_.size() * kValueBytes;
}

}  // namespace chunkdb
