#include <cmath>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <variant>

#include "expr.hpp"
#include "expr_program.hpp"

namespace chunkdb::expr {

namespace {

using detail::Category;
using detail::Int;
using detail::Program;
using detail::Step;
using detail::UInt;
using detail::Value;

enum class Truth : std::uint8_t {
    kFalse,
    kTrue,
    kUnknown,
};

enum class Order : std::uint8_t {
    kLess,
    kEqual,
    kGreater,
    kUnordered,
};

constexpr Int kIntMax = static_cast<Int>((static_cast<UInt>(1) << 127U) - 1U);
constexpr Int kIntMin = -kIntMax - 1;
constexpr UInt kLow64 = std::numeric_limits<std::uint64_t>::max();

[[noreturn]] void Overflow() {
    throw std::out_of_range("an integer result is outside the 128-bit range");
}

[[nodiscard]] std::string Decimal(Int value) {
    UInt magnitude = value < 0 ? static_cast<UInt>(0) - static_cast<UInt>(value) : static_cast<UInt>(value);
    std::string digits;
    do {
        digits.insert(digits.begin(), static_cast<char>('0' + static_cast<int>(magnitude % 10U)));
        magnitude /= 10U;
    } while (magnitude != 0U);
    return value < 0 ? "-" + digits : digits;
}

// Two's complement sums wrap; they overflowed when both operands have a
// sign the result lacks.
[[nodiscard]] Int Add(Int a, Int b) {
    const auto out = static_cast<Int>(static_cast<UInt>(a) + static_cast<UInt>(b));
    if (((a ^ out) & (b ^ out)) < 0) {
        Overflow();
    }
    return out;
}

[[nodiscard]] Int Subtract(Int a, Int b) {
    const auto out = static_cast<Int>(static_cast<UInt>(a) - static_cast<UInt>(b));
    if (((a ^ b) & (a ^ out)) < 0) {
        Overflow();
    }
    return out;
}

[[nodiscard]] UInt Magnitude(Int value) {
    return value < 0 ? static_cast<UInt>(0) - static_cast<UInt>(value) : static_cast<UInt>(value);
}

// The product of the magnitudes from 64-bit halves, so no signed 128-bit
// multiplication (and no runtime helper for its overflow check) is needed.
[[nodiscard]] Int Multiply(Int a, Int b) {
    const UInt x = Magnitude(a);
    const UInt y = Magnitude(b);
    if (x > kLow64 && y > kLow64) {
        Overflow();
    }
    const UInt small = x <= kLow64 ? x : y;
    const UInt big = x <= kLow64 ? y : x;
    const UInt high = small * (big >> 64U);
    if (high > kLow64) {
        Overflow();
    }
    const UInt low = small * (big & kLow64);
    const UInt product = low + (high << 64U);
    if (product < low) {
        Overflow();
    }
    const bool negative = (a < 0) != (b < 0);
    const UInt limit = static_cast<UInt>(1) << 127U;
    if (negative ? product > limit : product >= limit) {
        Overflow();
    }
    return negative ? static_cast<Int>(static_cast<UInt>(0) - product) : static_cast<Int>(product);
}

// Truncates toward zero.
[[nodiscard]] Int Divide(Int a, Int b) {
    if (b == 0) {
        throw std::invalid_argument("division by zero");
    }
    if (a == kIntMin && b == -1) {
        Overflow();
    }
    return a / b;
}

// Takes the sign of the dividend.
[[nodiscard]] Int Modulo(Int a, Int b) {
    if (b == 0) {
        throw std::invalid_argument("modulo by zero");
    }
    return b == -1 ? 0 : a % b;
}

[[nodiscard]] double ToDouble(const Value& value) {
    return value.category == Category::kInt ? static_cast<double>(value.integer) : value.number;
}

// Exact: the integer part of the float compares as an integer, then its
// fraction decides a tie.
[[nodiscard]] Order CompareIntFloat(Int integer, double number) {
    if (std::isnan(number)) {
        return Order::kUnordered;
    }
    constexpr double kTwo127 = 0x1p127;
    if (number >= kTwo127) {
        return Order::kLess;
    }
    if (number < -kTwo127) {
        return Order::kGreater;
    }
    const double whole = std::trunc(number);
    const auto truncated = static_cast<Int>(whole);
    if (integer != truncated) {
        return integer < truncated ? Order::kLess : Order::kGreater;
    }
    const double fraction = number - whole;
    return fraction > 0.0 ? Order::kLess : fraction < 0.0 ? Order::kGreater : Order::kEqual;
}

[[nodiscard]] Order Reverse(Order order) {
    return order == Order::kLess ? Order::kGreater : order == Order::kGreater ? Order::kLess : order;
}

// Neither is NULL.
[[nodiscard]] Order Compare(const Value& a, const Value& b) {
    if (a.category == Category::kInt && b.category == Category::kInt) {
        return a.integer < b.integer ? Order::kLess : a.integer > b.integer ? Order::kGreater : Order::kEqual;
    }
    if (a.category == Category::kInt && b.category == Category::kFloat) {
        return CompareIntFloat(a.integer, b.number);
    }
    if (a.category == Category::kFloat && b.category == Category::kInt) {
        return Reverse(CompareIntFloat(b.integer, a.number));
    }
    if (a.category == Category::kFloat && b.category == Category::kFloat) {
        if (std::isnan(a.number) || std::isnan(b.number)) {
            return Order::kUnordered;
        }
        return a.number < b.number ? Order::kLess : a.number > b.number ? Order::kGreater : Order::kEqual;
    }
    if (a.category == Category::kBool && b.category == Category::kBool) {
        return a.boolean == b.boolean ? Order::kEqual : a.boolean ? Order::kGreater : Order::kLess;
    }
    if (a.category != b.category) {
        throw std::logic_error("expression values of different types");
    }
    // Text, bytes and bits: bytewise, unsigned.
    const std::size_t common = std::min(a.bytes.size(), b.bytes.size());
    const int order = common == 0U ? 0 : std::memcmp(a.bytes.data(), b.bytes.data(), common);
    if (order != 0) {
        return order < 0 ? Order::kLess : Order::kGreater;
    }
    return a.bytes.size() < b.bytes.size() ? Order::kLess
         : a.bytes.size() > b.bytes.size() ? Order::kGreater
                                           : Order::kEqual;
}

[[nodiscard]] bool Holds(Op op, Order order) {
    if (order == Order::kUnordered) {
        return op == Op::kNotEqual;
    }
    switch (op) {
        case Op::kEqual:
            return order == Order::kEqual;
        case Op::kNotEqual:
            return order != Order::kEqual;
        case Op::kLess:
            return order == Order::kLess;
        case Op::kLessEqual:
            return order != Order::kGreater;
        case Op::kGreater:
            return order == Order::kGreater;
        case Op::kGreaterEqual:
            return order != Order::kLess;
        default:
            throw std::logic_error("not a comparison");
    }
}

[[nodiscard]] Truth Not(Truth truth) {
    return truth == Truth::kTrue ? Truth::kFalse : truth == Truth::kFalse ? Truth::kTrue : Truth::kUnknown;
}

[[nodiscard]] Truth And(Truth a, Truth b) {
    if (a == Truth::kFalse || b == Truth::kFalse) {
        return Truth::kFalse;
    }
    return a == Truth::kTrue && b == Truth::kTrue ? Truth::kTrue : Truth::kUnknown;
}

[[nodiscard]] Truth Compared(Op op, const Value& a, const Value& b) {
    if (a.category == Category::kNull || b.category == Category::kNull) {
        return Truth::kUnknown;
    }
    return Holds(op, Compare(a, b)) ? Truth::kTrue : Truth::kFalse;
}

// The value of a column or parameter of `type`; false when `value` holds
// another type.
[[nodiscard]] bool Load(const ColumnValue& value, const ColumnType& type, Value* out) {
    *out = Value{};
    if (std::holds_alternative<std::monostate>(value)) {
        return true;
    }
    switch (type.kind) {
        case ColumnKind::kUnsigned:
            if (const auto* number = std::get_if<std::uint64_t>(&value)) {
                out->category = Category::kInt;
                out->integer = *number;
                return true;
            }
            return false;
        case ColumnKind::kSigned:
            if (const auto* number = std::get_if<std::int64_t>(&value)) {
                out->category = Category::kInt;
                out->integer = *number;
                return true;
            }
            return false;
        case ColumnKind::kBool:
            if (const auto* flag = std::get_if<bool>(&value)) {
                out->category = Category::kBool;
                out->boolean = *flag;
                return true;
            }
            return false;
        case ColumnKind::kFloat32:
            if (const auto* number = std::get_if<float>(&value)) {
                out->category = Category::kFloat;
                out->number = *number;
                return true;
            }
            return false;
        case ColumnKind::kFloat64:
            if (const auto* number = std::get_if<double>(&value)) {
                out->category = Category::kFloat;
                out->number = *number;
                return true;
            }
            return false;
        case ColumnKind::kBits:
            if (const auto* bits = std::get_if<BitsValue>(&value); bits != nullptr && bits->digits.size() == type.size) {
                out->category = Category::kBits;
                out->bytes = bits->digits;
                return true;
            }
            return false;
        case ColumnKind::kText:
            if (const auto* text = std::get_if<std::string>(&value)) {
                out->category = Category::kText;
                out->bytes = *text;
                return true;
            }
            return false;
        case ColumnKind::kBytes:
            if (const auto* bytes = std::get_if<BytesValue>(&value)) {
                out->category = Category::kBytes;
                out->bytes = std::string_view(reinterpret_cast<const char*>(bytes->bytes.data()), bytes->bytes.size());
                return true;
            }
            return false;
    }
    return false;
}

class Evaluator {
  public:
    Evaluator(const Program& program, const Inputs& inputs) : program_(program), inputs_(inputs) {}

    [[nodiscard]] Value ValueOf(std::uint32_t index) const {
        const Step& step = program_.steps[index];
        switch (step.op) {
            case Op::kLiteral:
                return step.literal;
            case Op::kParameter: {
                const std::string name = "$" + std::to_string(step.slot + 1U);
                if (step.slot >= inputs_.parameters.size()) {
                    throw std::invalid_argument(name + " has no value");
                }
                Value value;
                if (!Load(inputs_.parameters[step.slot], step.type, &value)) {
                    throw std::invalid_argument(name + " must be a " + ColumnTypeName(step.type) + " value");
                }
                return value;
            }
            case Op::kColumn:
                if (!inputs_.exists) {
                    return Value{};
                }
                return Stored(inputs_.block, step);
            case Op::kChunkColumn:
                return Stored(inputs_.chunk, step);
            case Op::kPosX:
                return Integer(inputs_.x);
            case Op::kPosY:
                return Integer(inputs_.y);
            case Op::kPosZ:
                return Integer(inputs_.z);
            case Op::kNow:
                return Integer(inputs_.now);
            case Op::kNegate: {
                Value value = ValueOf(Operand(step, 0));
                if (value.category == Category::kInt) {
                    if (value.integer == kIntMin) {
                        Overflow();
                    }
                    value.integer = -value.integer;
                } else if (value.category == Category::kFloat) {
                    value.number = -value.number;
                }
                return value;
            }
            case Op::kAdd:
            case Op::kSubtract:
            case Op::kMultiply:
            case Op::kDivide:
            case Op::kModulo: {
                const Value a = ValueOf(Operand(step, 0));
                const Value b = ValueOf(Operand(step, 1));
                return Arithmetic(step.op, a, b);
            }
            case Op::kLeast:
            case Op::kGreatest:
                return Extreme(step);
            case Op::kCoalesce:
                for (std::uint32_t k = 0; k < step.count; ++k) {
                    const Value value = ValueOf(Operand(step, k));
                    if (value.category != Category::kNull) {
                        return Widen(value, step.category);
                    }
                }
                return Value{};
            default:
                throw std::logic_error("a predicate is not a value");
        }
    }

    [[nodiscard]] Truth TruthOf(std::uint32_t index) const {
        const Step& step = program_.steps[index];
        switch (step.op) {
            case Op::kEqual:
            case Op::kNotEqual:
            case Op::kLess:
            case Op::kLessEqual:
            case Op::kGreater:
            case Op::kGreaterEqual: {
                const Value a = ValueOf(Operand(step, 0));
                const Value b = ValueOf(Operand(step, 1));
                return Compared(step.op, a, b);
            }
            case Op::kIsNull:
            case Op::kIsNotNull: {
                const bool null = ValueOf(Operand(step, 0)).category == Category::kNull;
                return null == (step.op == Op::kIsNull) ? Truth::kTrue : Truth::kFalse;
            }
            case Op::kIn:
            case Op::kNotIn: {
                // True when an item equals the value; else unknown when the
                // value or an item is NULL.
                const Value value = ValueOf(Operand(step, 0));
                bool found = false;
                bool null = value.category == Category::kNull;
                for (std::uint32_t k = 1; k < step.count; ++k) {
                    const Value item = ValueOf(Operand(step, k));
                    if (item.category == Category::kNull) {
                        null = true;
                    } else if (value.category != Category::kNull && Compare(value, item) == Order::kEqual) {
                        found = true;
                    }
                }
                const Truth truth = value.category != Category::kNull && found ? Truth::kTrue
                                  : null                                       ? Truth::kUnknown
                                                                               : Truth::kFalse;
                return step.op == Op::kIn ? truth : Not(truth);
            }
            case Op::kBetween:
            case Op::kNotBetween: {
                const Value value = ValueOf(Operand(step, 0));
                const Value low = ValueOf(Operand(step, 1));
                const Value high = ValueOf(Operand(step, 2));
                const Truth truth = And(Compared(Op::kGreaterEqual, value, low), Compared(Op::kLessEqual, value, high));
                return step.op == Op::kBetween ? truth : Not(truth);
            }
            case Op::kExists:
                return inputs_.exists ? Truth::kTrue : Truth::kFalse;
            case Op::kNot:
                return Not(TruthOf(Operand(step, 0)));
            case Op::kAnd:
            case Op::kOr: {
                // Left to right, stopping once the result is decided.
                const Truth decides = step.op == Op::kAnd ? Truth::kFalse : Truth::kTrue;
                Truth result = Not(decides);
                for (std::uint32_t k = 0; k < step.count; ++k) {
                    const Truth truth = TruthOf(Operand(step, k));
                    if (truth == decides) {
                        return decides;
                    }
                    if (truth == Truth::kUnknown) {
                        result = Truth::kUnknown;
                    }
                }
                return result;
            }
            default: {
                const Value value = ValueOf(index);
                if (value.category == Category::kNull) {
                    return Truth::kUnknown;
                }
                if (value.category != Category::kBool) {
                    throw std::logic_error("a condition that is not bool");
                }
                return value.boolean ? Truth::kTrue : Truth::kFalse;
            }
        }
    }

  private:
    [[nodiscard]] std::uint32_t Operand(const Step& step, std::uint32_t k) const {
        return program_.operands[step.first + k];
    }

    [[nodiscard]] static Value Integer(Int value) {
        Value out;
        out.category = Category::kInt;
        out.integer = value;
        return out;
    }

    [[nodiscard]] static Value Stored(std::span<const ColumnValue> values, const Step& step) {
        if (step.slot >= values.size()) {
            throw std::logic_error("no value for column slot " + std::to_string(step.slot));
        }
        Value value;
        if (!Load(values[step.slot], step.type, &value)) {
            throw std::logic_error("a column value is not of its column's type " + ColumnTypeName(step.type));
        }
        return value;
    }

    // An integer as a float where the expression is a float.
    [[nodiscard]] static Value Widen(Value value, Category category) {
        if (category == Category::kFloat && value.category == Category::kInt) {
            value.number = static_cast<double>(value.integer);
            value.category = Category::kFloat;
        }
        return value;
    }

    [[nodiscard]] static Value Arithmetic(Op op, const Value& a, const Value& b) {
        if (a.category == Category::kNull || b.category == Category::kNull) {
            return Value{};
        }
        if (a.category == Category::kInt && b.category == Category::kInt) {
            switch (op) {
                case Op::kAdd:
                    return Integer(Add(a.integer, b.integer));
                case Op::kSubtract:
                    return Integer(Subtract(a.integer, b.integer));
                case Op::kMultiply:
                    return Integer(Multiply(a.integer, b.integer));
                case Op::kDivide:
                    return Integer(Divide(a.integer, b.integer));
                default:
                    return Integer(Modulo(a.integer, b.integer));
            }
        }
        const double x = ToDouble(a);
        const double y = ToDouble(b);
        Value out;
        out.category = Category::kFloat;
        switch (op) {
            case Op::kAdd:
                out.number = x + y;
                break;
            case Op::kSubtract:
                out.number = x - y;
                break;
            case Op::kMultiply:
                out.number = x * y;
                break;
            case Op::kDivide:
                out.number = x / y;
                break;
            default:
                out.number = std::fmod(x, y);
                break;
        }
        return out;
    }

    // LEAST or GREATEST: every argument is evaluated; NULLs are skipped, a
    // NaN wins, and of equal values the first stays.
    [[nodiscard]] Value Extreme(const Step& step) const {
        const Order better = step.op == Op::kLeast ? Order::kLess : Order::kGreater;
        Value best;
        bool nan = false;
        for (std::uint32_t k = 0; k < step.count; ++k) {
            const Value value = ValueOf(Operand(step, k));
            if (value.category == Category::kNull) {
                continue;
            }
            if (value.category == Category::kFloat && std::isnan(value.number)) {
                nan = true;
            } else if (best.category == Category::kNull || Compare(value, best) == better) {
                best = value;
            }
        }
        if (nan) {
            Value out;
            out.category = Category::kFloat;
            out.number = std::numeric_limits<double>::quiet_NaN();
            return out;
        }
        return Widen(best, step.category);
    }

    const Program& program_;
    const Inputs& inputs_;
};

[[noreturn]] void DoesNotHold(const detail::Target& target, const std::string& value) {
    throw std::out_of_range(
        "column " + target.name + " does not hold " + value + ": " + ColumnTypeRange(target.column.type));
}

// `value` as the new value of `target`'s column.
[[nodiscard]] ColumnValue Store(const detail::Target& target, const Value& value) {
    const ColumnType& type = target.column.type;
    if (value.category == Category::kNull) {
        if (!target.column.nullable) {
            throw std::invalid_argument("column " + target.name + " cannot be NULL");
        }
        return std::monostate{};
    }
    switch (type.kind) {
        case ColumnKind::kUnsigned:
        case ColumnKind::kSigned: {
            if (value.category != Category::kInt) {
                throw std::logic_error("an integer column assigned a value that is not an integer");
            }
            const Int number = value.integer;
            if (type.kind == ColumnKind::kUnsigned) {
                const Int high = (static_cast<Int>(1) << type.size) - 1;
                if (number < 0 || number > high) {
                    DoesNotHold(target, Decimal(number));
                }
                return static_cast<std::uint64_t>(number);
            }
            const Int half = static_cast<Int>(1) << (type.size - 1U);
            if (number < -half || number >= half) {
                DoesNotHold(target, Decimal(number));
            }
            return static_cast<std::int64_t>(number);
        }
        case ColumnKind::kFloat32: {
            if (value.category == Category::kInt) {
                return static_cast<float>(value.integer);
            }
            const double number = value.number;
            if (std::isfinite(number) && std::fabs(number) > static_cast<double>(std::numeric_limits<float>::max())) {
                DoesNotHold(target, "a float beyond f32");
            }
            return static_cast<float>(number);
        }
        case ColumnKind::kFloat64:
            return ToDouble(value);
        case ColumnKind::kBool:
            return value.boolean;
        case ColumnKind::kText: {
            if (value.bytes.size() > type.size) {
                DoesNotHold(target, std::to_string(value.bytes.size()) + " bytes");
            }
            const auto* data = reinterpret_cast<const std::uint8_t*>(value.bytes.data());
            if (!IsUtf8(std::span<const std::uint8_t>(data, value.bytes.size()))) {
                throw std::invalid_argument("column " + target.name + " takes UTF-8 text");
            }
            return std::string(value.bytes);
        }
        case ColumnKind::kBytes: {
            if (value.bytes.size() > type.size) {
                DoesNotHold(target, std::to_string(value.bytes.size()) + " bytes");
            }
            const auto* data = reinterpret_cast<const std::uint8_t*>(value.bytes.data());
            return BytesValue{.bytes = std::vector<std::uint8_t>(data, data + value.bytes.size())};
        }
        case ColumnKind::kBits:
            return BitsValue{.digits = std::string(value.bytes)};
    }
    throw std::logic_error("unknown column kind");
}

}  // namespace

bool EvaluateCondition(const ConditionPlan& plan, const Inputs& inputs) {
    const Program& program = plan.program();
    return Evaluator(program, inputs).TruthOf(program.root) == Truth::kTrue;
}

std::vector<ColumnValue> EvaluateAssignments(const AssignmentsPlan& plan, const Inputs& inputs) {
    const Program& program = plan.program();
    const Evaluator evaluator(program, inputs);
    std::vector<ColumnValue> values;
    values.reserve(program.targets.size());
    for (const detail::Target& target : program.targets) {
        values.push_back(Store(target, evaluator.ValueOf(target.root)));
    }
    return values;
}

}  // namespace chunkdb::expr
