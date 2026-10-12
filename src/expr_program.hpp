#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <string_view>
#include <vector>

#include "expr.hpp"

// The planned form of expressions (expr.cpp), which evaluation walks
// (expr_eval.cpp).
namespace chunkdb::expr::detail {

// Integers compute exactly in 128 bits.
using Int = __int128;
using UInt = unsigned __int128;

enum class Category : std::uint8_t {
    kUnknown,  // planning only: parameters that have no type yet
    kNull,     // only NULL
    kInt,
    kFloat,  // computed as f64
    kBool,
    kText,
    kBytes,
    kBits,
};

// One value while evaluating. Text, bytes and the digits of bits are views
// of the inputs or of Program::strings, so evaluation does not allocate.
struct Value {
    Category category = Category::kNull;
    Int integer = 0;
    double number = 0.0;
    bool boolean = false;
    std::string_view bytes{};
};

// One node of an expression, planned.
struct Step {
    Op op = Op::kLiteral;
    // The type of a value; kFloat when any operand is a float.
    Category category = Category::kNull;
    std::size_t column = 0;
    // The operands: Program::operands[first, first + count).
    std::uint32_t first = 0;
    std::uint32_t count = 0;
    // kColumn, kChunkColumn: the slot; kParameter: the index - 1.
    std::size_t slot = 0;
    // kColumn, kChunkColumn, kParameter: the type its value has.
    ColumnType type{};
    Value literal{};
};

struct Target {
    ColumnBinding column;
    std::string name;
    std::uint32_t root = 0;
};

struct Program {
    std::vector<Step> steps;
    std::vector<std::uint32_t> operands;
    // Stable storage of the literals' text, bytes and bits.
    std::deque<std::string> strings;
    // A condition's root.
    std::uint32_t root = 0;
    std::vector<Target> targets;
    std::vector<ColumnBinding> target_columns;
    std::vector<ParameterType> parameters;
};

}  // namespace chunkdb::expr::detail
