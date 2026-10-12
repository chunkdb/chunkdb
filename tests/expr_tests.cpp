// Expressions (#69, docs/design/EXPRESSIONS_DESIGN.md): the grammar,
// typing, evaluation and limits; a property test against a second, simple
// evaluator; and random input to the parser.

#include <algorithm>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <limits>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "cql.hpp"
#include "expr.hpp"

namespace {

namespace cql = chunkdb::cql;
namespace expr = chunkdb::expr;
using chunkdb::BitsValue;
using chunkdb::BytesValue;
using chunkdb::Column;
using chunkdb::ColumnKind;
using chunkdb::ColumnType;
using chunkdb::ColumnValue;
using expr::Op;

[[noreturn]] void Failure(const std::string& message) {
    std::fprintf(stderr, "FAILED: %s\n", message.c_str());
    std::fflush(stderr);
    std::abort();
}

void Check(bool condition, const std::string& message) {
    if (!condition) {
        Failure(message);
    }
}

// ---------------------------------------------------------------------------
// Printing.

std::string Hex(const std::string& bytes) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out;
    for (const char c : bytes) {
        const auto byte = static_cast<unsigned char>(c);
        out += kDigits[byte >> 4U];
        out += kDigits[byte & 0x0fU];
    }
    return out;
}

std::string Number(double value) {
    if (std::isnan(value)) {
        return std::signbit(value) ? "-nan" : "nan";
    }
    if (std::isinf(value)) {
        return value < 0 ? "-inf" : "inf";
    }
    char buffer[64];
    std::snprintf(buffer, sizeof buffer, "%.17g", value);
    std::string text = buffer;
    if (text.find_first_of(".e") == std::string::npos) {
        text += ".0";
    }
    return text;
}

std::string LiteralText(const cql::Literal& literal) {
    return std::visit(
        [](const auto& value) -> std::string {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, cql::Null>) {
                return "NULL";
            } else if constexpr (std::is_same_v<T, cql::Integer>) {
                return (value.negative ? "-" : "") + std::to_string(value.magnitude);
            } else if constexpr (std::is_same_v<T, double>) {
                return Number(value);
            } else if constexpr (std::is_same_v<T, bool>) {
                return value ? "TRUE" : "FALSE";
            } else if constexpr (std::is_same_v<T, cql::Text>) {
                return "'" + value.value + "'";
            } else if constexpr (std::is_same_v<T, cql::Bytes>) {
                return "x'" + Hex(std::string(value.value.begin(), value.value.end())) + "'";
            } else if constexpr (std::is_same_v<T, cql::Bits>) {
                return "b'" + value.digits + "'";
            } else {
                return "$" + std::to_string(value.index);
            }
        },
        literal);
}

std::string OpText(Op op) {
    switch (op) {
        case Op::kAdd: return "+";
        case Op::kSubtract: return "-";
        case Op::kMultiply: return "*";
        case Op::kDivide: return "/";
        case Op::kModulo: return "%";
        case Op::kEqual: return "=";
        case Op::kNotEqual: return "<>";
        case Op::kLess: return "<";
        case Op::kLessEqual: return "<=";
        case Op::kGreater: return ">";
        case Op::kGreaterEqual: return ">=";
        case Op::kLeast: return "LEAST";
        case Op::kGreatest: return "GREATEST";
        case Op::kCoalesce: return "COALESCE";
        case Op::kAnd: return "AND";
        case Op::kOr: return "OR";
        default: return "?";
    }
}

// Every node in parentheses, so the tree shows.
std::string Canon(const expr::Expression& expression, std::uint32_t index) {
    const expr::Node& node = expression.nodes[index];
    const auto operand = [&](std::size_t k) { return Canon(expression, node.operands[k]); };
    const auto list = [&](std::size_t from) {
        std::string out;
        for (std::size_t k = from; k < node.operands.size(); ++k) {
            out += (k == from ? "" : ", ") + operand(k);
        }
        return out;
    };
    switch (node.op) {
        case Op::kLiteral:
        case Op::kParameter:
            return LiteralText(node.literal);
        case Op::kColumn: return node.name;
        case Op::kChunkColumn: return "chunk." + node.name;
        case Op::kPosX: return "POS_X";
        case Op::kPosY: return "POS_Y";
        case Op::kPosZ: return "POS_Z";
        case Op::kNow: return "NOW()";
        case Op::kNegate: return "(-" + operand(0) + ")";
        case Op::kAdd:
        case Op::kSubtract:
        case Op::kMultiply:
        case Op::kDivide:
        case Op::kModulo:
        case Op::kEqual:
        case Op::kNotEqual:
        case Op::kLess:
        case Op::kLessEqual:
        case Op::kGreater:
        case Op::kGreaterEqual:
            return "(" + operand(0) + " " + OpText(node.op) + " " + operand(1) + ")";
        case Op::kLeast:
        case Op::kGreatest:
        case Op::kCoalesce:
        case Op::kAnd:
        case Op::kOr:
            return OpText(node.op) + "(" + list(0) + ")";
        case Op::kIsNull: return "(" + operand(0) + " IS NULL)";
        case Op::kIsNotNull: return "(" + operand(0) + " IS NOT NULL)";
        case Op::kIn: return "(" + operand(0) + " IN (" + list(1) + "))";
        case Op::kNotIn: return "(" + operand(0) + " NOT IN (" + list(1) + "))";
        case Op::kBetween: return "(" + operand(0) + " BETWEEN " + operand(1) + " AND " + operand(2) + ")";
        case Op::kNotBetween: return "(" + operand(0) + " NOT BETWEEN " + operand(1) + " AND " + operand(2) + ")";
        case Op::kExists: return "EXISTS";
        case Op::kNot: return "NOT(" + operand(0) + ")";
    }
    return "?";
}

std::string Tree(const std::string& text) {
    const auto condition = expr::ParseCondition(text);
    return Canon(condition.expression, condition.expression.root);
}

std::string AssignTree(const std::string& text) {
    const auto assignments = expr::ParseAssignments(text);
    std::string out;
    for (const auto& value : assignments.values) {
        out += (out.empty() ? "" : "; ") + value.column + " = " + Canon(value.value, value.value.root);
    }
    return out;
}

void ExpectTree(const std::string& text, const std::string& tree) {
    std::string got;
    try {
        got = Tree(text);
    } catch (const std::exception& e) {
        Failure(text + ": expected " + tree + ", got error " + e.what());
    }
    Check(got == tree, text + ": expected " + tree + ", got " + got);
}

// The error code a client sees, in the order the engine tests them:
// ParseError is SYNTAX where a statement is parsed; CommandEngine::ErrorReply
// sends INVALID_ARGUMENT for std::invalid_argument, OUT_OF_RANGE for
// std::out_of_range and INTERNAL for anything else.
std::string CodeOf(const std::function<void()>& action, std::string* message = nullptr) {
    try {
        action();
        return "OK";
    } catch (const cql::ParseError& e) {
        if (message != nullptr) *message = e.what();
        return "SYNTAX";
    } catch (const std::invalid_argument& e) {
        if (message != nullptr) *message = e.what();
        return "INVALID_ARGUMENT";
    } catch (const std::out_of_range& e) {
        if (message != nullptr) *message = e.what();
        return "OUT_OF_RANGE";
    } catch (const std::exception& e) {
        if (message != nullptr) *message = e.what();
        return "INTERNAL";
    }
}

void ExpectCode(const std::function<void()>& action, const std::string& code, const std::string& part, const std::string& what) {
    std::string message;
    const std::string got = CodeOf(action, &message);
    Check(got == code, what + ": expected " + code + ", got " + got + " (" + message + ")");
    Check(message.find(part) != std::string::npos, what + ": expected a message with '" + part + "', got '" + message + "'");
}

void ExpectSyntax(const std::string& text, const std::string& part = "") {
    ExpectCode([&] { (void)expr::ParseCondition(text); }, "SYNTAX", part, text);
}

void ExpectAssignSyntax(const std::string& text, const std::string& part = "") {
    ExpectCode([&] { (void)expr::ParseAssignments(text); }, "SYNTAX", part, text);
}

// ---------------------------------------------------------------------------
// A table to plan and evaluate against.

Column MakeColumn(std::uint32_t id, std::string name, ColumnType type, bool nullable) {
    Column column;
    column.id = id;
    column.name = std::move(name);
    column.type = type;
    column.nullable = nullable;
    return column;
}

constexpr ColumnType U(std::uint32_t n) { return ColumnType{.kind = ColumnKind::kUnsigned, .size = n}; }
constexpr ColumnType I(std::uint32_t n) { return ColumnType{.kind = ColumnKind::kSigned, .size = n}; }
constexpr ColumnType kBool{.kind = ColumnKind::kBool, .size = 1};
constexpr ColumnType kF32{.kind = ColumnKind::kFloat32, .size = 32};
constexpr ColumnType kF64{.kind = ColumnKind::kFloat64, .size = 64};
constexpr ColumnType Text(std::uint32_t n) { return ColumnType{.kind = ColumnKind::kText, .size = n}; }
constexpr ColumnType Bytes(std::uint32_t n) { return ColumnType{.kind = ColumnKind::kBytes, .size = n}; }
constexpr ColumnType Bits(std::uint32_t n) { return ColumnType{.kind = ColumnKind::kBits, .size = n}; }

ColumnValue ZeroOf(const ColumnType& type) {
    switch (type.kind) {
        case ColumnKind::kUnsigned: return std::uint64_t{0};
        case ColumnKind::kSigned: return std::int64_t{0};
        case ColumnKind::kBool: return false;
        case ColumnKind::kFloat32: return 0.0F;
        case ColumnKind::kFloat64: return 0.0;
        case ColumnKind::kBits: return BitsValue{.digits = std::string(type.size, '0')};
        case ColumnKind::kText: return std::string();
        case ColumnKind::kBytes: return BytesValue{};
    }
    return std::monostate{};
}

struct Table {
    std::vector<Column> block;
    std::vector<Column> chunk;
    std::vector<ColumnValue> row;
    std::vector<ColumnValue> chunk_row;
    std::vector<ColumnValue> parameters;
    bool exists = true;
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::int64_t z = 0;
    std::uint64_t now = 0;

    Table() {
        const std::vector<std::pair<std::string, std::pair<ColumnType, bool>>> columns{
            {"u8c", {U(8), false}},     {"n8", {U(8), true}},       {"u16c", {U(16), false}},
            {"u64c", {U(64), false}},   {"i8c", {I(8), false}},     {"i16c", {I(16), false}},
            {"i64c", {I(64), false}},   {"n16", {I(16), true}},     {"f32c", {kF32, false}},
            {"f64c", {kF64, false}},    {"nf", {kF64, true}},       {"flag", {kBool, false}},
            {"nb", {kBool, true}},      {"nb2", {kBool, true}},     {"t", {Text(3), false}},
            {"tl", {Text(20), true}},   {"by", {Bytes(2), false}},  {"b4", {Bits(4), false}},
            {"b8", {Bits(8), false}},   {"version", {U(32), false}}, {"exists", {U(8), false}},
            {"u1c", {U(1), false}},     {"i2c", {I(2), false}},
        };
        std::uint32_t id = 1;
        for (const auto& [name, spec] : columns) {
            block.push_back(MakeColumn(id++, name, spec.first, spec.second));
            row.push_back(spec.second ? ColumnValue{} : ZeroOf(spec.first));
        }
        chunk.push_back(MakeColumn(100, "k", I(32), false));
        chunk.push_back(MakeColumn(101, "kn", U(8), true));
        chunk.push_back(MakeColumn(102, "kt", Text(5), false));
        chunk_row = {std::int64_t{0}, ColumnValue{}, std::string()};
    }

    [[nodiscard]] expr::ColumnList Schema() const { return expr::ColumnList(block, chunk); }

    ColumnValue& operator[](const std::string& name) {
        for (std::size_t i = 0; i < block.size(); ++i) {
            if (block[i].name == name) return row[i];
        }
        for (std::size_t i = 0; i < chunk.size(); ++i) {
            if (chunk[i].name == name) return chunk_row[i];
        }
        Failure("no test column " + name);
    }

    [[nodiscard]] expr::Inputs Inputs() const {
        return expr::Inputs{
            .exists = exists,
            .block = row,
            .chunk = chunk_row,
            .x = x,
            .y = y,
            .z = z,
            .now = now,
            .parameters = parameters,
        };
    }

    [[nodiscard]] expr::ConditionPlan Plan(const std::string& text) const {
        return expr::PlanCondition(expr::ParseCondition(text).expression, Schema());
    }

    [[nodiscard]] bool Eval(const std::string& text) const {
        try {
            return expr::EvaluateCondition(Plan(text), Inputs());
        } catch (const std::exception& e) {
            Failure(text + ": unexpected error " + e.what());
        }
    }

    [[nodiscard]] std::vector<ColumnValue> Assign(const std::string& text) const {
        try {
            const auto parsed = expr::ParseAssignments(text);
            return expr::EvaluateAssignments(expr::PlanAssignments(parsed.values, Schema()), Inputs());
        } catch (const std::exception& e) {
            Failure(text + ": unexpected error " + e.what());
        }
    }

    [[nodiscard]] std::string Code(const std::string& text, std::string* message = nullptr) const {
        return CodeOf([&] { (void)expr::EvaluateCondition(Plan(text), Inputs()); }, message);
    }

    [[nodiscard]] std::string AssignCode(const std::string& text, std::string* message = nullptr) const {
        return CodeOf(
            [&] {
                const auto parsed = expr::ParseAssignments(text);
                (void)expr::EvaluateAssignments(expr::PlanAssignments(parsed.values, Schema()), Inputs());
            },
            message);
    }

    void True(const std::string& text) const { Check(Eval(text), text + ": expected true"); }
    void False(const std::string& text) const { Check(!Eval(text), text + ": expected false"); }
    void Error(const std::string& text, const std::string& code, const std::string& part = "") const {
        std::string message;
        const std::string got = Code(text, &message);
        Check(got == code, text + ": expected " + code + ", got " + got + " (" + message + ")");
        Check(message.find(part) != std::string::npos, text + ": expected '" + part + "' in '" + message + "'");
    }
    void AssignError(const std::string& text, const std::string& code, const std::string& part = "") const {
        std::string message;
        const std::string got = AssignCode(text, &message);
        Check(got == code, text + ": expected " + code + ", got " + got + " (" + message + ")");
        Check(message.find(part) != std::string::npos, text + ": expected '" + part + "' in '" + message + "'");
    }
    // Unknown: false, and so is its negation.
    void Unknown(const std::string& text) const {
        False(text);
        False("NOT (" + text + ")");
    }

    [[nodiscard]] std::vector<expr::ParameterType> Parameters(const std::string& text) const {
        return Plan(text).parameters();
    }
};

bool SameValue(const ColumnValue& a, const ColumnValue& b) {
    if (a.index() != b.index()) return false;
    if (const auto* x = std::get_if<double>(&a)) {
        const double y = std::get<double>(b);
        return (std::isnan(*x) && std::isnan(y)) || std::bit_cast<std::uint64_t>(*x) == std::bit_cast<std::uint64_t>(y);
    }
    if (const auto* x = std::get_if<float>(&a)) {
        const float y = std::get<float>(b);
        return (std::isnan(*x) && std::isnan(y)) || std::bit_cast<std::uint32_t>(*x) == std::bit_cast<std::uint32_t>(y);
    }
    return a == b;
}

std::string ValueText(const ColumnValue& value) {
    return std::visit(
        [](const auto& v) -> std::string {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
                return "NULL";
            } else if constexpr (std::is_same_v<T, std::uint64_t>) {
                return std::to_string(v) + "u";
            } else if constexpr (std::is_same_v<T, std::int64_t>) {
                return std::to_string(v) + "i";
            } else if constexpr (std::is_same_v<T, bool>) {
                return v ? "TRUE" : "FALSE";
            } else if constexpr (std::is_same_v<T, float>) {
                return Number(static_cast<double>(v)) + "f";
            } else if constexpr (std::is_same_v<T, double>) {
                return Number(v);
            } else if constexpr (std::is_same_v<T, BitsValue>) {
                return "b'" + v.digits + "'";
            } else if constexpr (std::is_same_v<T, std::string>) {
                return "'" + v + "' (" + Hex(v) + ")";
            } else {
                return "x'" + Hex(std::string(v.bytes.begin(), v.bytes.end())) + "'";
            }
        },
        value);
}

void ExpectValues(const std::vector<ColumnValue>& got, const std::vector<ColumnValue>& want, const std::string& what) {
    Check(got.size() == want.size(), what + ": wrong number of values");
    for (std::size_t i = 0; i < got.size(); ++i) {
        Check(SameValue(got[i], want[i]), what + ": value " + std::to_string(i) + " is " + ValueText(got[i]) + ", expected " + ValueText(want[i]));
    }
}

// ---------------------------------------------------------------------------
// Grammar.

void TestPrecedenceAndAssociativity() {
    ExpectTree("a + b * c = 1", "((a + (b * c)) = 1)");
    ExpectTree("a * b + c = 1", "(((a * b) + c) = 1)");
    ExpectTree("a - b - c = 1", "(((a - b) - c) = 1)");
    ExpectTree("a - (b - c) = 1", "((a - (b - c)) = 1)");
    ExpectTree("a / b % c * d = 1", "((((a / b) % c) * d) = 1)");
    ExpectTree("a + b - c + d = 1", "((((a + b) - c) + d) = 1)");
    ExpectTree("-a * b = 1", "(((-a) * b) = 1)");
    ExpectTree("- - a = 1", "((-(-a)) = 1)");
    ExpectTree("a - -b = 1", "((a - (-b)) = 1)");
    ExpectTree("a--1 = 1", "((a - -1) = 1)");
    ExpectTree("a-1 = 1", "((a - 1) = 1)");
    ExpectTree("a = -5", "(a = -5)");
    ExpectTree("a = -(5)", "(a = (-5))");
    ExpectTree("a = - 5", "(a = -5)");
    ExpectTree("a = --5", "(a = (--5))");
    ExpectTree("a = -1.5", "(a = -1.5)");
    ExpectTree("a = -inf", "(a = -inf)");
    ExpectTree("a = -NaN", "(a = -nan)");
    ExpectTree("a = -9223372036854775808", "(a = -9223372036854775808)");
    ExpectTree("a = -0", "(a = 0)");
    ExpectTree("a OR b AND c", "OR(a, AND(b, c))");
    ExpectTree("a AND b OR c AND d", "OR(AND(a, b), AND(c, d))");
    ExpectTree("a OR b OR c", "OR(a, b, c)");
    ExpectTree("a AND b AND c", "AND(a, b, c)");
    ExpectTree("(a OR b) AND c", "AND(OR(a, b), c)");
    ExpectTree("NOT a AND b", "AND(NOT(a), b)");
    ExpectTree("NOT a = 1", "NOT((a = 1))");
    ExpectTree("NOT NOT a", "NOT(NOT(a))");
    ExpectTree("NOT (a OR b)", "NOT(OR(a, b))");
    ExpectTree("(count = 1 AND status = 1) OR status = 0", "OR(AND((count = 1), (status = 1)), (status = 0))");
    ExpectTree("a + 1 < b * 2", "((a + 1) < (b * 2))");
    ExpectTree("a <> b", "(a <> b)");
    ExpectTree("a<=b", "(a <= b)");
    ExpectTree("a>=b", "(a >= b)");
    ExpectTree("a<b", "(a < b)");
    ExpectTree("a>b", "(a > b)");
}

void TestParenthesesAndPredicates() {
    ExpectTree("(a + b) * c < 3", "(((a + b) * c) < 3)");
    ExpectTree("((a)) + 1 < 2", "((a + 1) < 2)");
    ExpectTree("(a) = 1", "(a = 1)");
    ExpectTree("(a = 1)", "(a = 1)");
    ExpectTree("((a = 1))", "(a = 1)");
    ExpectTree("(a = 1) AND (b)", "AND((a = 1), b)");
    ExpectTree("(a) IS NULL", "(a IS NULL)");
    ExpectTree("(a) * 2 + 1 BETWEEN 1 AND 3", "(((a * 2) + 1) BETWEEN 1 AND 3)");
    ExpectTree("a IS NULL", "(a IS NULL)");
    ExpectTree("a IS NOT NULL", "(a IS NOT NULL)");
    ExpectTree("a + 1 IS NULL", "((a + 1) IS NULL)");
    ExpectTree("a IN (1, -2, 2.5, 'x', NULL, TRUE, x'0a', b'01', $1, -inf)",
               "(a IN (1, -2, 2.5, 'x', NULL, TRUE, x'0a', b'01', $1, -inf))");
    ExpectTree("a NOT IN (1)", "(a NOT IN (1))");
    ExpectTree("a + 1 IN (2)", "((a + 1) IN (2))");
    ExpectTree("a BETWEEN 1 AND b + 1", "(a BETWEEN 1 AND (b + 1))");
    ExpectTree("a BETWEEN 1 AND 2 AND c", "AND((a BETWEEN 1 AND 2), c)");
    ExpectTree("a NOT BETWEEN b AND c OR d", "OR((a NOT BETWEEN b AND c), d)");
    ExpectTree("EXISTS", "EXISTS");
    ExpectTree("NOT EXISTS", "NOT(EXISTS)");
    ExpectTree("EXISTS AND a = 1", "AND(EXISTS, (a = 1))");
    ExpectTree("flag", "flag");
    ExpectTree("TRUE", "TRUE");
    ExpectTree("NULL", "NULL");
    ExpectTree("COALESCE(flag, FALSE)", "COALESCE(flag, FALSE)");
    ExpectTree("POS_X + POS_Y * POS_Z > NOW()", "((POS_X + (POS_Y * POS_Z)) > NOW())");
    ExpectTree("LEAST(a, 1, b + 1) = GREATEST(a) + COALESCE(a, b)",
               "(LEAST(a, 1, (b + 1)) = (GREATEST(a) + COALESCE(a, b)))");
    ExpectTree("LEAST((a), (b + 1)) = 1", "(LEAST(a, (b + 1)) = 1)");
    ExpectTree("chunk.k = 1", "(chunk.k = 1)");
    ExpectTree("CHUNK . k = chunk.\"and\"", "(chunk.k = chunk.and)");
    ExpectTree("a = 'it''s'", "(a = 'it's')");
    ExpectTree("a = ''", "(a = '')");
    ExpectTree("a = 1e3", "(a = 1000.0)");
    ExpectTree("a = 1.5E-1", "(a = 0.14999999999999999)");
    ExpectTree("a = 18446744073709551615", "(a = 18446744073709551615)");
    ExpectTree("$2 = a AND $1 = b", "AND(($2 = a), ($1 = b))");
}

void TestKeywordsAndNames() {
    // Keywords are case-insensitive.
    ExpectTree("a = 1 and b = 2 Or NoT c", "OR(AND((a = 1), (b = 2)), NOT(c))");
    ExpectTree("exists AND Exists AND eXiStS", "AND(EXISTS, EXISTS, EXISTS)");
    ExpectTree("a is not null", "(a IS NOT NULL)");
    ExpectTree("a Is NuLl", "(a IS NULL)");
    ExpectTree("a iN (1) AND a not between 1 and 2", "AND((a IN (1)), (a NOT BETWEEN 1 AND 2))");
    ExpectTree("least(a) = Greatest(a) + coalesce(a)", "(LEAST(a) = (GREATEST(a) + COALESCE(a)))");
    ExpectTree("Now() = pos_x + Pos_Y + POS_z", "(NOW() = ((POS_X + POS_Y) + POS_Z))");
    ExpectTree("a = true OR a = False OR a = NULL OR a = Inf OR a = NAN",
               "OR((a = TRUE), (a = FALSE), (a = NULL), (a = inf), (a = nan))");
    ExpectTree("Chunk.k = 1", "(chunk.k = 1)");
    // A column whose name is a keyword is quoted; others may be.
    ExpectTree("\"exists\" = 1", "(exists = 1)");
    ExpectTree("\"and\" = \"or\"", "(and = or)");
    ExpectTree("\"null\" IS NULL", "(null IS NULL)");
    ExpectTree("\"pos_x\" = pos_x", "(pos_x = POS_X)");
    ExpectTree("\"a\" = a", "(a = a)");
    ExpectTree("\"chunk\" = 1", "(chunk = 1)");
    ExpectTree("version = 3", "(version = 3)");
    ExpectSyntax("VERSION = 3", "names are lowercase: 'VERSION'");
    ExpectSyntax("exists = 1", "expected the end of the expression, got '='");
    ExpectSyntax("a = and", "expected a value, got 'and'; write \"and\" for a column of that name");
    ExpectSyntax("a = IF", "write \"if\"");
    ExpectSyntax("where = 1", "expected a value, got 'where'");
    ExpectSyntax("chunk = 1", "expected . after CHUNK");
    ExpectSyntax("chunk.and = 1", "'and' is a keyword");
    ExpectSyntax("Coins = 1", "names are lowercase: 'Coins'");
    ExpectSyntax("\"Coins\" = 1", "a quoted name must match");
    ExpectSyntax("\"\" = 1", "a quoted name must match");
    ExpectSyntax("\"a b\" = 1", "a quoted name must match");
    ExpectSyntax("\"a\"\"b\" = 1", "'\"a\"b\"'");
    ExpectSyntax("\"open = 1", "the quoted name has no closing quote");
    ExpectSyntax("a = 'open", "the quoted value has no closing quote");
    const std::string longest(63, 'n');
    ExpectTree(longest + " = 1", "(" + longest + " = 1)");
    ExpectTree("\"" + longest + "\" = 1", "(" + longest + " = 1)");
    ExpectSyntax(longest + "n = 1", "a column name has at most 63 bytes");
    ExpectSyntax("\"" + longest + "n\" = 1", "a quoted name must match");
}

void TestSyntaxErrors() {
    ExpectSyntax("", "expected a value, got the end of the statement");
    ExpectSyntax("a <", "expected a value");
    ExpectSyntax("a =", "expected a value");
    ExpectSyntax("(a", "expected ), got the end");
    ExpectSyntax("a)", "expected the end of the expression, got ')'");
    ExpectSyntax("a +", "expected a value");
    ExpectSyntax("NOT", "expected a value");
    ExpectSyntax("a NOT LIKE 'x'", "expected IN or BETWEEN after NOT, got 'LIKE'");
    ExpectSyntax("a IS 1", "expected NULL, got '1'");
    ExpectSyntax("a IS NOT 1", "expected NULL");
    ExpectSyntax("a IN ()", "expected a value or a parameter, got ')'");
    ExpectSyntax("a IN (b)", "expected a value or a parameter, got 'b'");
    ExpectSyntax("a IN (1 + 2)", "expected ), got '+'");
    ExpectSyntax("a IN (-b)", "expected a number after -, got 'b'");
    ExpectSyntax("a IN (-'x')", "expected a number after -");
    ExpectSyntax("a IN 1", "expected (, got '1'");
    ExpectSyntax("a BETWEEN 1", "expected AND, got the end");
    ExpectSyntax("a BETWEEN 1 OR 2", "expected AND, got 'OR'");
    ExpectSyntax("LEAST() = 1", "expected a value, got ')'");
    ExpectSyntax("LEAST = 1", "expected (, got '='");
    ExpectSyntax("COALESCE(a,) = 1", "expected a value, got ')'");
    ExpectSyntax("NOW(1) = 1", "expected ), got '1'");
    ExpectSyntax("NOW = 1", "expected (, got '='");
    ExpectSyntax("chunk k = 1", "expected . after CHUNK, got 'k'");
    ExpectSyntax("chunk. = 1", "expected a chunk column name, got '='");
    ExpectSyntax("chunk.1 = 1", "expected a chunk column name");
    ExpectSyntax("a < b < c", "expected the end of the expression, got '<'");
    ExpectSyntax("a = 1 = 2", "got '='");
    ExpectSyntax("(a = 1) + 1", "got '+'");
    ExpectSyntax("(a = 1) = TRUE", "got '='");
    ExpectSyntax("(EXISTS) = TRUE", "got '='");
    ExpectSyntax("a + (b = 1)", "expected ), got '='");
    ExpectSyntax("-(a = 1)", "expected ), got '='");
    ExpectSyntax("LEAST(a = 1)", "expected ), got '='");
    ExpectSyntax("a = NOT b", "expected a value, got 'NOT'");
    ExpectSyntax("a AND", "expected a value");
    ExpectSyntax("a ! b", "unexpected '!'");
    ExpectSyntax("a != b", "unexpected '!'");
    ExpectSyntax("a == b", "expected a value, got '='");
    ExpectSyntax("a = 1;", "unexpected ';'");
    ExpectSyntax("a = $", "a parameter is $ followed by its number");
    ExpectSyntax("a = $0", "parameters are numbered from $1 to $65535");
    ExpectSyntax("a = $65536", "parameters are numbered from $1 to $65535");
    ExpectSyntax("a = $1 OR b = $1", "$1 is used twice");
    ExpectSyntax("a = $2", "parameters must be numbered $1 to $2 without gaps");
    ExpectSyntax("a = 18446744073709551616", "18446744073709551616 is not a 64-bit integer");
    ExpectSyntax("a = 1.5.5", "1.5.5 is not a number");
    ExpectSyntax("a = 1e", "1e is not a number");
    ExpectSyntax("a = x'0'", "x'..' takes pairs of hexadecimal digits");
    ExpectSyntax("a = b'2'", "b'..' takes the digits 0 and 1");
    ExpectSyntax("a = b'01' 'x'", "got 'x'");
    ExpectSyntax("a.b = 1", "expected the end of the expression, got '.'");
    ExpectSyntax("a = \"b\"\"", "the quoted name has no closing quote");
    ExpectSyntax("a = .5", "expected a value, got '.'");
    // The error names the column of the line where it is.
    ExpectSyntax("a = 1 AND b = ", "column 15:");
    ExpectSyntax("  a < ;", "column 7: unexpected ';'");
    // Parameters with gaps, numbering checked over the whole text.
    const auto counted = expr::ParseCondition("a = $3 AND b IN ($1, $2)");
    Check(counted.parameters == 3, "three parameters");
    Check(expr::ParseCondition("a = 1").parameters == 0, "no parameters");
}

void TestAssignmentsGrammar() {
    Check(AssignTree("coins = coins + $1") == "coins = (coins + $1)", "simple assignment");
    Check(AssignTree("a = 1, b = a * 2 - -3, \"exists\" = NULL") == "a = 1; b = ((a * 2) - -3); exists = NULL", "three assignments");
    Check(AssignTree("a = LEAST(a, 5)") == "a = LEAST(a, 5)", "function assignment");
    Check(AssignTree("a = (a)") == "a = a", "parenthesized");
    Check(AssignTree("a = 'x', b = x'00', c = b'1', d = TRUE") == "a = 'x'; b = x'00'; c = b'1'; d = TRUE", "literals");
    const auto parsed = expr::ParseAssignments("a = $2, b = $1");
    Check(parsed.parameters == 2 && parsed.values.size() == 2 && parsed.values[1].at == 9, "assignment parameters");
    ExpectAssignSyntax("", "expected a column name, got the end");
    ExpectAssignSyntax("a", "expected =, got the end");
    ExpectAssignSyntax("a = ", "expected a value");
    ExpectAssignSyntax("a = 1,", "expected a column name");
    ExpectAssignSyntax("a = b = 1", "expected the end of the expression, got '='");
    ExpectAssignSyntax("a = b IS NULL", "got 'IS'");
    ExpectAssignSyntax("a = b AND c", "got 'AND'");
    ExpectAssignSyntax("a = EXISTS", "expected a value, got 'EXISTS'");
    ExpectAssignSyntax("pos_x = 1", "'pos_x' is a keyword");
    ExpectAssignSyntax("chunk.k = 1", "'chunk' is a keyword");
    ExpectAssignSyntax("A = 1", "names are lowercase");
    ExpectAssignSyntax("1 = 1", "expected a column name, got '1'");
    ExpectAssignSyntax("a = $1, b = $1", "$1 is used twice");
    ExpectAssignSyntax("a = $2", "without gaps");
    Check(AssignTree("\"pos_x\" = 1") == "pos_x = 1", "quoted reserved name");
}

void TestStatementEmbedding() {
    // A condition inside a statement stops where the statement goes on.
    std::set<std::size_t> parameters;
    const std::string line = "DELETE BLOCK 1 2 FROM t IF a = $1 AND b SET x";
    std::size_t position = line.find("IF") + 2U;
    const auto condition = expr::ParseConditionAt(line, &position, &parameters);
    Check(Canon(condition, condition.root) == "AND((a = $1), b)", "embedded condition");
    Check(line.substr(position) == "SET x", "embedded condition position: " + line.substr(position));
    Check(parameters == std::set<std::size_t>{1}, "embedded parameters");
    const std::string set = "SET BLOCK 3 4 IN world coins = coins + $2, n = -1 IF VERSION 7";
    position = set.find("coins");
    const auto values = expr::ParseAssignmentsAt(set, &position, &parameters);
    Check(values.size() == 2 && values[0].column == "coins" && values[1].column == "n", "embedded assignments");
    Check(set.substr(position) == "IF VERSION 7", "embedded assignments position");
    Check(values[0].at == set.find("coins") + 1U, "assignment column");
    Check(expr::IsVersionClause(set.substr(position + 2U)), "version clause after assignments");
    Check(parameters == std::set<std::size_t>{1, 2}, "parameters shared with the statement");
    position = set.find("coins");
    ExpectCode([&] { (void)expr::ParseAssignmentsAt(set, &position, &parameters); }, "SYNTAX",
               "column " + std::to_string(set.find("$2") + 1U) + ": $2 is used twice", "shared parameters");
    // Ending at the end of the line.
    std::string tail = "GET x WHERE a = 1   ";
    position = tail.find("a =");
    (void)expr::ParseConditionAt(tail, &position, &parameters);
    Check(position == tail.size(), "position at the end");
    // Node columns count from the start of the line.
    tail = "WHERE a = 1";
    position = 6;
    const auto at = expr::ParseConditionAt(tail, &position, &parameters);
    Check(at.nodes[at.root].column == 9, "operator column in the line");
}

void TestStatementsUnchanged() {
    // Statements still lex '-' with a number and refuse expression tokens.
    const auto parsed = cql::Parse("SET BLOCK 0 0 IN t a = -5, b = -inf");
    const auto& set = std::get<cql::SetBlock>(parsed.statement);
    Check(std::get<cql::Integer>(set.values[0].value) == cql::Integer{.negative = true, .magnitude = 5}, "statement -5");
    Check(std::isinf(std::get<double>(set.values[1].value)), "statement -inf");
    for (const auto& [line, part] : std::vector<std::pair<std::string, std::string>>{
             {"GET BLOCK 1 2 FROM t <", "column 22: unexpected '<'"},
             {"SET BLOCK 0 0 IN t a = \"x\"", "unexpected '\"'"},
             {"SET BLOCK 0 0 IN t a = 1 + 2", "unexpected '+'"},
             {"SET BLOCK 0 0 IN t a = - 5", "unexpected '-'"},
             {"GET BLOCK 1 2 FROM t.x", "unexpected '.'"},
             {"SET BLOCK 0 0 IN t a = $1, b = $1", "$1 is used twice"},
             {"SET BLOCK 0 0 IN t a = $2", "parameters must be numbered $1 to $2 without gaps"},
             {"SET BLOCK 0 0 IN t a = 18446744073709551616", "is not a 64-bit integer"},
         }) {
        ExpectCode([&] { (void)cql::Parse(line); }, "SYNTAX", part, line);
    }
}

void TestVersionClause() {
    for (const char* text : {"VERSION 3", "version 3", "Version\t42", "  VERSION 0", "VERSION $1", "VERSION -1",
                             "VERSION 1.5", "VERSION 3 AND a = 1", "VERSION 18446744073709551616"}) {
        Check(expr::IsVersionClause(text), std::string("version clause: ") + text);
    }
    for (const char* text : {"", "VERSION", "VERSION   ", "VERSION = 3", "version = 3", "VERSION - 1 > 0", "VERSION -x",
                             "VERSION IS NULL", "\"version\" 3", "versions 3", "VERSION x", "VERSION !", "VERSION -",
                             "a = 1", "(VERSION 3)", "! VERSION 3", "VERSION + 3", "VERSION '3'"}) {
        Check(!expr::IsVersionClause(text), std::string("not a version clause: ") + text);
    }
}

std::string Repeat(const std::string& part, std::size_t count) {
    std::string out;
    out.reserve(part.size() * count);
    for (std::size_t i = 0; i < count; ++i) out += part;
    return out;
}

std::size_t DepthOf(const expr::Expression& expression, std::uint32_t index) {
    std::size_t depth = 0;
    for (const auto operand : expression.nodes[index].operands) {
        depth = std::max(depth, DepthOf(expression, operand));
    }
    return depth + 1U;
}

void TestLimits() {
    // Nodes: an IN list of 254 items has 256 nodes.
    const auto items = [](std::size_t n) {
        std::string out = "a IN (";
        for (std::size_t i = 0; i < n; ++i) out += (i == 0 ? "" : ", ") + std::to_string(i);
        return out + ")";
    };
    Check(expr::ParseCondition(items(254)).expression.nodes.size() == 256, "256 nodes");
    ExpectSyntax(items(255), "the expression is too long: at most 256 nodes");
    Check(expr::ParseCondition("a" + Repeat(" OR a", 254)).expression.nodes.size() == 256, "256 nodes of OR");
    ExpectSyntax("a" + Repeat(" OR a", 255), "too long");
    // Depth: 31 NOTs over a column is 32 levels.
    Check(DepthOf(expr::ParseCondition(Repeat("NOT ", 31) + "a").expression, expr::ParseCondition(Repeat("NOT ", 31) + "a").expression.root) == 32, "32 levels");
    ExpectSyntax(Repeat("NOT ", 32) + "a", "the expression is nested too deeply: at most 32 levels");
    (void)expr::ParseCondition(Repeat("- ", 30) + "a = 1");
    ExpectSyntax(Repeat("- ", 31) + "a = 1", "nested too deeply");
    // A chain of 31 operators nests 32 levels, under the comparison 33.
    (void)expr::ParseCondition("a" + Repeat(" + a", 30) + " = 1");
    ExpectSyntax("a" + Repeat(" + a", 31) + " = 1", "nested too deeply");
    (void)expr::ParseAssignments("x = a" + Repeat(" * a", 31));
    ExpectAssignSyntax("x = a" + Repeat(" * a", 32), "nested too deeply");
    // Parentheses: 32 levels.
    (void)expr::ParseCondition(Repeat("(", 32) + "a" + Repeat(")", 32));
    ExpectSyntax(Repeat("(", 33) + "a" + Repeat(")", 33), "parentheses are nested too deeply: at most 32 levels");
    ExpectSyntax(Repeat("LEAST(", 33) + "a" + Repeat(")", 33), "nested too deeply");
    // Huge inputs fail fast and without deep recursion.
    const auto start = std::chrono::steady_clock::now();
    for (const std::string& text : {Repeat("(", 200000) + "a", Repeat("NOT ", 200000) + "a", Repeat("-", 200000) + "a = 1",
                                    "a" + Repeat(" + a", 200000) + " = 1", "a IN (" + Repeat("1, ", 200000) + "1)",
                                    Repeat("a = 1 AND ", 100000) + "b", Repeat("LEAST(", 200000)}) {
        ExpectSyntax(text);
    }
    Check(std::chrono::steady_clock::now() - start < std::chrono::seconds(5), "huge inputs are refused quickly");
    // Custom limits.
    const expr::Limits small{.max_nodes = 5, .max_depth = 3};
    (void)expr::ParseCondition("a + b = c", small);
    ExpectCode([&] { (void)expr::ParseCondition("- - - a = 1", small); }, "SYNTAX", "at most 3 levels", "small depth");
    ExpectCode([&] { (void)expr::ParseCondition("a IN (1, 2, 3, 4)", small); }, "SYNTAX", "at most 5 nodes", "small nodes");
    (void)expr::ParseCondition("(((a)))", small);
    ExpectCode([&] { (void)expr::ParseCondition("((((a))))", small); }, "SYNTAX", "at most 3 levels", "small parentheses");
    // Each assignment is its own expression.
    (void)expr::ParseAssignments("x = 1 + 2, y = 3 + 4, z = 5 + 6", small);
    // Planning a deeper hand-built tree than its limits is refused too.
    const auto deep = expr::ParseCondition(Repeat("NOT ", 10) + "flag").expression;
    const Table table;
    ExpectCode([&] { (void)expr::PlanCondition(deep, table.Schema(), expr::Limits{.max_nodes = 256, .max_depth = 5}); }, "SYNTAX", "nested too deeply", "plan depth");
    ExpectCode([&] { (void)expr::PlanCondition(deep, table.Schema(), expr::Limits{.max_nodes = 4, .max_depth = 32}); }, "SYNTAX", "too long", "plan nodes");
}

void TestMalformedTrees() {
    const Table table;
    const auto plan = [&](expr::Expression expression) {
        return CodeOf([&] { (void)expr::PlanCondition(expression, table.Schema()); });
    };
    Check(plan(expr::Expression{}) == "INVALID_ARGUMENT", "empty tree");
    expr::Expression cycle;
    cycle.nodes.push_back(expr::Node{.op = Op::kNot, .column = 1, .literal = {}, .name = {}, .operands = {0}});
    Check(plan(cycle) == "INVALID_ARGUMENT", "cycle");
    expr::Expression arity;
    arity.nodes.push_back(expr::Node{.op = Op::kExists});
    arity.nodes.push_back(expr::Node{.op = Op::kAdd, .column = 1, .literal = {}, .name = {}, .operands = {0}});
    arity.root = 1;
    Check(plan(arity) == "INVALID_ARGUMENT", "operand count");
    expr::Expression predicate_operand;
    predicate_operand.nodes.push_back(expr::Node{.op = Op::kExists});
    predicate_operand.nodes.push_back(expr::Node{.op = Op::kIsNull, .column = 1, .literal = {}, .name = {}, .operands = {0}});
    predicate_operand.root = 1;
    Check(plan(predicate_operand) == "INVALID_ARGUMENT", "predicate as a value");
    expr::Expression shared;
    shared.nodes.push_back(expr::Node{.op = Op::kPosX});
    shared.nodes.push_back(expr::Node{.op = Op::kAdd, .column = 1, .literal = {}, .name = {}, .operands = {0, 0}});
    shared.nodes.push_back(expr::Node{.op = Op::kIsNull, .column = 1, .literal = {}, .name = {}, .operands = {1}});
    shared.root = 2;
    Check(plan(shared) == "INVALID_ARGUMENT", "shared operand");
    shared.nodes[1].operands = {0};
    shared.nodes.insert(shared.nodes.begin() + 1, expr::Node{.op = Op::kPosY});
    shared.nodes[2].operands = {0, 1};
    shared.nodes[3].operands = {2};
    shared.root = 3;
    Check(plan(shared) == "OK", "distinct operands");
    expr::Expression literal_parameter;
    literal_parameter.nodes.push_back(expr::Node{.op = Op::kLiteral, .column = 1, .literal = cql::Parameter{.index = 1}});
    Check(plan(literal_parameter) == "INVALID_ARGUMENT", "parameter literal");
    const auto exists = expr::ParseCondition("EXISTS").expression;
    ExpectCode([&] {
        const std::vector<expr::AssignmentExpression> values{{.column = "u8c", .at = 1, .value = exists}};
        (void)expr::PlanAssignments(values, table.Schema());
    }, "INVALID_ARGUMENT", "an assignment takes a value", "predicate assigned");
    ExpectCode([] { (void)expr::EvaluateCondition(expr::ConditionPlan(), expr::Inputs{}); }, "INTERNAL", "", "unplanned condition");
}

// ---------------------------------------------------------------------------
// Typing.

std::string Types(const std::vector<expr::ParameterType>& parameters) {
    std::string out;
    for (const auto& parameter : parameters) {
        out += (out.empty() ? "" : " ") + ("$" + std::to_string(parameter.index) + ":" + chunkdb::ColumnTypeName(parameter.type));
    }
    return out;
}

void ExpectTypes(const Table& table, const std::string& text, const std::string& types) {
    std::string got;
    try {
        got = Types(table.Parameters(text));
    } catch (const std::exception& e) {
        Failure(text + ": expected " + types + ", got error " + e.what());
    }
    Check(got == types, text + ": expected " + types + ", got " + got);
}

void ExpectAssignTypes(const Table& table, const std::string& text, const std::string& types) {
    const auto parsed = expr::ParseAssignments(text);
    const std::string got = Types(expr::PlanAssignments(parsed.values, table.Schema()).parameters());
    Check(got == types, text + ": expected " + types + ", got " + got);
}

void TestParameterTypes() {
    const Table table;
    ExpectTypes(table, "u8c < $1", "$1:u8");
    ExpectTypes(table, "$1 > u8c", "$1:u8");
    ExpectTypes(table, "u8c + $1 < 5", "$1:u8");
    ExpectTypes(table, "$1 * u8c < 5", "$1:u8");
    ExpectTypes(table, "u8c < $1 + $2", "$1:u8 $2:u8");
    ExpectTypes(table, "u8c < $1 * 1.5", "$1:u8");
    ExpectTypes(table, "u8c < -$1", "$1:u8");
    ExpectTypes(table, "LEAST($1, $2) < i16c", "$1:i16 $2:i16");
    ExpectTypes(table, "COALESCE($1, $2) = t", "$1:text(3) $2:text(3)");
    ExpectTypes(table, "COALESCE(n8, $1) = 1", "$1:u8");
    ExpectTypes(table, "u8c + i16c < $1", "$1:i64");
    ExpectTypes(table, "u8c + u16c < $1", "$1:u64");
    ExpectTypes(table, "u8c + u8c < $1", "$1:u8");
    ExpectTypes(table, "-u8c < $1", "$1:i64");
    ExpectTypes(table, "-i8c < $1", "$1:i8");
    ExpectTypes(table, "u8c + 1.5 < $1", "$1:f64");
    ExpectTypes(table, "u8c + $1 * 1.5 < 2", "$1:f64");
    ExpectTypes(table, "f32c + f32c < $1", "$1:f32");
    ExpectTypes(table, "f32c + 1.5 < $1", "$1:f32");
    ExpectTypes(table, "f32c + 1 < $1", "$1:f64");
    ExpectTypes(table, "f32c + f64c < $1", "$1:f64");
    ExpectTypes(table, "f32c = $1", "$1:f32");
    ExpectTypes(table, "t = $1 OR tl < $2 OR by = $3", "$1:text(3) $2:text(20) $3:bytes(2)");
    ExpectTypes(table, "flag = $1 AND b4 <> $2", "$1:bool $2:bits(4)");
    ExpectTypes(table, "u8c IN (1, $1, $2)", "$1:u8 $2:u8");
    ExpectTypes(table, "u8c BETWEEN $1 AND $2", "$1:u8 $2:u8");
    ExpectTypes(table, "$1 BETWEEN i16c AND 5", "$1:i16");
    ExpectTypes(table, "$1 BETWEEN 5 AND i16c", "$1:i16");
    ExpectTypes(table, "5 BETWEEN $1 AND i16c", "$1:i16");
    ExpectTypes(table, "POS_X < $1 AND NOW() > $2", "$1:i64 $2:u64");
    ExpectTypes(table, "chunk.k < $1 AND chunk.kt = $2", "$1:i32 $2:text(5)");
    ExpectTypes(table, "\"exists\" = $1 AND version = $2", "$1:u8 $2:u32");
    ExpectTypes(table, "u8c IS NULL", "");
    ExpectAssignTypes(table, "u8c = $1", "$1:u8");
    ExpectAssignTypes(table, "u8c = $1 + 1, f32c = $2 * 2", "$1:u8 $2:f32");
    ExpectAssignTypes(table, "f64c = LEAST($1, i8c)", "$1:i8");
    ExpectAssignTypes(table, "t = COALESCE($1, tl)", "$1:text(20)");
    ExpectAssignTypes(table, "b4 = $1, by = $2", "$1:bits(4) $2:bytes(2)");
    // A parameter needs a column-typed operand.
    for (const char* text : {"$1 < $2", "LEAST($1, $2) = 1", "$1 IN (1, 2)", "$1 IS NULL", "$1", "NOT $1",
                             "$1 + 1 < 2", "$1 = NULL", "COALESCE($1, 1) = 2", "1 IN ($1)", "$1 BETWEEN 1 AND 2",
                             "-$1 = 5", "u8c = 1 AND $1 = 2"}) {
        table.Error(text, "INVALID_ARGUMENT", "has no column to take its type from");
    }
    const auto parsed = expr::ParseAssignments("u8c = 1");
    Check(expr::PlanAssignments(parsed.values, table.Schema()).targets().size() == 1, "targets");
    Check(expr::PlanAssignments(parsed.values, table.Schema()).targets()[0].id == 1, "target id");
}

void TestTypeErrors() {
    const Table table;
    for (const auto& [text, part] : std::vector<std::pair<std::string, std::string>>{
             {"t + 1 = 1", "+ takes numbers, got text"},
             {"-t = 'a'", "- takes numbers, got text"},
             {"flag * 2 = 1", "* takes numbers, got bool"},
             {"b4 % 2 = 1", "% takes numbers, got bits(4)"},
             {"by - 1 = 1", "- takes numbers, got bytes"},
             {"'a' / 2 = 1", "/ takes numbers, got text"},
             {"TRUE + 1 = 2", "+ takes numbers, got bool"},
             {"t < 1", "cannot compare text with an integer"},
             {"t = x'00'", "cannot compare text with bytes"},
             {"by < 'a'", "cannot compare bytes with text"},
             {"u8c = 'a'", "cannot compare u8 with text"},
             {"f64c = TRUE", "cannot compare f64 with bool"},
             {"flag = 1", "cannot compare bool with an integer"},
             {"flag < TRUE", "bool supports only = and <>"},
             {"flag >= nb", "bool supports only = and <>"},
             {"flag < NULL", "bool supports only = and <>"},
             {"b4 < b'0000'", "bits(4) supports only = and <>"},
             {"b4 = b'000'", "cannot compare bits(4) with bits(3)"},
             {"b4 = b8", "cannot compare bits(4) with bits(8)"},
             {"b4 = 5", "cannot compare bits(4) with an integer"},
             {"u8c", "a condition must be bool, got u8"},
             {"t", "a condition must be bool, got text"},
             {"1", "a condition must be bool, got an integer"},
             {"NOT u8c", "a condition must be bool"},
             {"u8c AND flag", "a condition must be bool"},
             {"flag OR 1.5", "a condition must be bool, got a float"},
             {"LEAST(t, 'a') = 'a'", "LEAST takes numbers, got text"},
             {"GREATEST(flag) = TRUE", "GREATEST takes numbers, got bool"},
             {"COALESCE(t, 1) = 1", "COALESCE takes arguments of one type, got text and an integer"},
             {"COALESCE(b4, b8) = b'0000'", "COALESCE takes arguments of one type, got bits(4) and bits(8)"},
             {"COALESCE(t, by) = 'x'", "got text and bytes"},
             {"t BETWEEN 1 AND 2", "cannot compare text with an integer"},
             {"flag BETWEEN FALSE AND TRUE", "bool supports only = and <>"},
             {"NULL BETWEEN FALSE AND TRUE", "bool supports only = and <>"},
             {"NULL BETWEEN 1 AND 'a'", "cannot compare an integer with text"},
             {"u8c IN ('a')", "cannot compare u8 with text"},
             {"b4 IN (b'01')", "cannot compare bits(4) with bits(2)"},
             {"zz = 1", "the table has no column zz"},
             {"chunk.zz = 1", "the table has no chunk column zz"},
             {"chunk.u8c = 1", "the table has no chunk column u8c"},
             {"k = 1", "the table has no column k"},
             {"\"pos_x\" = 1", "the table has no column pos_x"},
         }) {
        table.Error(text, "INVALID_ARGUMENT", part);
    }
    // The position of the operator is named.
    table.Error("u8c = 1 AND t < 1", "INVALID_ARGUMENT", "(at column 15)");
    // Unknown columns read as the engine's own message, which ErrorReply
    // extends with the DESCRIBE hint.
    std::string message;
    (void)table.Code("zz = 1", &message);
    Check(message.starts_with("the table has no column "), "unknown column message");
    // Comparisons the rules allow.
    for (const char* text : {"b4 IN (b'0101', NULL)", "flag IN (TRUE, NULL)", "flag = nb", "b4 <> b'0000'", "x'00' < x'01'",
                             "'a' < 'b'", "u8c < 1.5", "f32c = u64c", "t BETWEEN 'a' AND tl", "NULL = NULL", "NULL IS NULL",
                             "NULL", "nb", "u8c IS NULL", "flag IS NOT NULL", "COALESCE(nf, u8c) = 1", "LEAST(u8c, f32c) < 1",
                             "COALESCE(b4, NULL) = b'0000'", "-NULL IS NULL", "NULL + NULL IS NULL", "NOT NULL"}) {
        (void)table.Plan(text);
    }
    // A float literal compared with f32 is rounded to f32 when planned.
    table.Error("f32c < 1e39", "OUT_OF_RANGE", "the number at column 8 is out of range for f32");
    table.Error("f32c + 1e39 < 0", "OUT_OF_RANGE", "out of range for f32");
    (void)table.Plan("f64c < 1e39");
    (void)table.Plan("f32c < inf AND f32c <> nan AND f32c > -inf");
    (void)table.Plan("u8c < 1e300");
}

void TestAssignmentTyping() {
    const Table table;
    for (const auto& [text, code, part] : std::vector<std::tuple<std::string, std::string, std::string>>{
             {"u8c = 1.5", "INVALID_ARGUMENT", "column u8c (u8) cannot take a float"},
             {"u8c = f32c", "INVALID_ARGUMENT", "cannot take f32"},
             {"u8c = u8c * 1.0", "INVALID_ARGUMENT", "cannot take f64"},
             {"u8c = 'a'", "INVALID_ARGUMENT", "cannot take text"},
             {"t = by", "INVALID_ARGUMENT", "column t (text(3)) cannot take bytes"},
             {"b4 = b'01'", "INVALID_ARGUMENT", "column b4 (bits(4)) cannot take bits(2)"},
             {"b4 = b8", "INVALID_ARGUMENT", "cannot take bits(8)"},
             {"flag = 1", "INVALID_ARGUMENT", "cannot take an integer"},
             {"f64c = TRUE", "INVALID_ARGUMENT", "cannot take bool"},
             {"u8c = NULL", "INVALID_ARGUMENT", "column u8c cannot be NULL"},
             {"u8c = 1, u8c = 2", "INVALID_ARGUMENT", "column u8c is assigned twice"},
             {"zz = 1", "INVALID_ARGUMENT", "the table has no column zz"},
             {"u8c = zz", "INVALID_ARGUMENT", "the table has no column zz"},
             {"u8c = $1 < 2", "SYNTAX", "expected the end of the expression"},
             {"u8c = t + 1", "INVALID_ARGUMENT", "+ takes numbers, got text"},
             {"f32c = 1e39", "OUT_OF_RANGE", "out of range for f32"},
         }) {
        table.AssignError(text, code, part);
    }
    for (const char* text : {"f64c = u8c", "f32c = i64c + 1", "f64c = 1", "n8 = NULL", "nf = NULL + 1.5", "tl = t",
                             "b4 = b'0101'", "b8 = COALESCE(b8, b'00000000')", "flag = nb", "by = x'0102'",
                             "u8c = LEAST(u8c, n8)", "u8c = COALESCE(n8, 0)", "t = 'abc'"}) {
        (void)expr::PlanAssignments(expr::ParseAssignments(text).values, table.Schema());
    }
}

// ---------------------------------------------------------------------------
// Evaluation.

void TestIntegers() {
    Table table;
    table["u64c"] = std::numeric_limits<std::uint64_t>::max();
    table["i64c"] = std::numeric_limits<std::int64_t>::min();
    table["u8c"] = std::uint64_t{255};
    table["i8c"] = std::int64_t{-128};
    // Exact in 128 bits.
    table.True("u64c + u64c = u64c * 2");
    table.True("u64c + u64c > u64c");
    table.True("u64c + 1 - 1 = u64c");
    table.True("u64c + 1 > u64c");
    table.True("i64c - 1 < i64c");
    table.True("i64c * i64c = 9223372036854775808 * 9223372036854775808");
    table.True("-i64c = 9223372036854775808");
    table.True("i64c * -1 > 0");
    table.True("u64c * 2 / 2 = u64c");
    table.True("u8c + u8c = 510 AND i8c - u8c = -383");
    table.True("u8c * i8c = -32640");
    // Outside i128.
    table.Error("u64c * u64c = 1", "OUT_OF_RANGE", "an integer result is outside the 128-bit range");
    table.Error("i64c * i64c * 2 = 1", "OUT_OF_RANGE", "outside the 128-bit range");
    table.True("i64c * i64c * -2 < 0");
    table.Error("-(i64c * i64c * -2) = 1", "OUT_OF_RANGE", "128-bit");
    table.Error("i64c * i64c * -2 / -1 = 1", "OUT_OF_RANGE", "128-bit");
    table.True("i64c * i64c * -2 % -1 = 0");
    table.Error("i64c * i64c * -2 - 1 = 1", "OUT_OF_RANGE", "128-bit");
    table.True("i64c * i64c * -2 + 1 < 0");
    table.Error("(i64c * i64c * -2 + 1) * -1 + 1 = 0", "OUT_OF_RANGE", "128-bit");
    table.True("(i64c * i64c * -2 + 1) * -1 - 1 > 0");
    table.Error("u64c * u64c * 0 = 0", "OUT_OF_RANGE", "128-bit");
    table.True("u64c * 0 * u64c * u64c = 0");
    // Division truncates toward zero; modulo takes the dividend's sign.
    table.True("7 / 2 = 3 AND -7 / 2 = -3 AND 7 / -2 = -3 AND -7 / -2 = 3");
    table.True("7 % 2 = 1 AND -7 % 2 = -1 AND 7 % -2 = 1 AND -7 % -2 = -1");
    table.True("i64c / -1 = 9223372036854775808 AND i64c % -1 = 0");
    table.True("u8c / 256 = 0 AND u8c % 256 = 255");
    table.Error("u8c / 0 = 1", "INVALID_ARGUMENT", "division by zero");
    table.Error("u8c % 0 = 1", "INVALID_ARGUMENT", "modulo by zero");
    table.Error("u8c / (u8c - 255) = 1", "INVALID_ARGUMENT", "division by zero");
    // NULL operands give NULL without an error.
    table.Unknown("n8 / 0 = 1");
    table.Unknown("NULL % 0 = 1");
    table.Unknown("u8c / NULL = 1");
    table.Error("NULL + 1 / 0 = 1", "INVALID_ARGUMENT", "division by zero");
    // Every width.
    table["u1c"] = std::uint64_t{1};
    table["i2c"] = std::int64_t{-2};
    table.True("u1c + i2c = -1");
    table["u16c"] = std::uint64_t{65535};
    table["i16c"] = std::int64_t{-32768};
    table.True("u16c + i16c = 32767");
}

void TestNumbersMixed() {
    Table table;
    const std::uint64_t two53 = std::uint64_t{1} << 53U;
    table["u64c"] = two53 + 1U;
    // Exact against floats beyond 2^53.
    table.True("u64c > 9007199254740992.0");
    table.False("u64c = 9007199254740992.0");
    table.True("u64c <> 9007199254740993.0");  // the literal is 2^53
    table.True("u64c - 1 = 9007199254740992.0");
    table.True("u64c > 9.007199254740992e15");
    table["f64c"] = 9007199254740992.0;
    table.True("u64c > f64c AND f64c < u64c AND u64c <> f64c AND NOT u64c = f64c");
    table.True("u64c - 1 = f64c AND f64c = u64c - 1 AND u64c - 1 <= f64c AND u64c - 1 >= f64c");
    // Arithmetic with a float is f64.
    table.True("u64c + 0.0 = f64c");
    table.True("u64c * 1.0 = 9007199254740992");
    table.True("u64c * 1.0 < u64c");
    table["u64c"] = std::numeric_limits<std::uint64_t>::max();
    table.True("u64c < 18446744073709551616.0 AND u64c + 1 = 18446744073709551616.0");
    table.True("u64c < 1.8446744073709552e19");
    table["i64c"] = std::numeric_limits<std::int64_t>::min();
    table.True("i64c = -9223372036854775808.0 AND i64c - 1 < -9223372036854775808.0");
    table.True("(i64c * i64c - 1) * 2 + 1 < 1.7014118346046923e38");
    table.True("(i64c * i64c - 1) * 2 + 1 + 0.0 = 1.7014118346046923e38");
    table.True("i64c * i64c * -2 = -1.7014118346046923e38");
    table.True("i64c * i64c * -2 < -1.7014118346046921e38");
    // Fractions decide ties.
    table.True("1 < 1.5 AND 2 > 1.5 AND -1 > -1.5 AND -2 < -1.5 AND 2 = 2.0 AND 0 = -0.0 AND 0.5 > 0");
    table.True("-1 < -0.5 AND -1 < -0.0000001 AND 0 > -1e-300 AND 0 < 1e-300");
    // NaN and infinities.
    table["f64c"] = std::numeric_limits<double>::quiet_NaN();
    table.True("u64c <> f64c AND f64c <> u64c AND f64c <> f64c AND f64c <> 1.5");
    table.False("u64c = f64c OR u64c < f64c OR u64c > f64c OR u64c <= f64c OR u64c >= f64c");
    table.False("f64c = f64c OR f64c < 1 OR f64c >= 1 OR 1 <= f64c");
    table.True("u64c < inf AND u64c > -inf AND (i64c * i64c - 1) * 2 + 1 < inf AND -inf < i64c * i64c * -2");
    table.True("NOT u8c = nan");
    table.True("u8c <> nan AND 1 / 0.0 = inf AND -1 / 0.0 = -inf AND 0 / 0.0 <> 0 / 0.0");
    table.True("-7.5 % 2 = -1.5 AND 7.5 % -2 = 1.5 AND 5 % 0.0 <> 5 % 0.0 AND 5.5 % inf = 5.5");
    table.True("inf - inf <> 0 AND 0 * inf <> 0 * inf");
    table["f64c"] = -0.0;
    table.True("f64c = 0 AND f64c = 0.0 AND 0.0 = -0.0 AND NOT f64c < 0 AND f64c >= 0");
    table.True("-f64c = f64c");
    // f32 widens exactly.
    table["f32c"] = 0.1F;
    table.True("f32c = 0.1");  // 0.1 takes f32
    table.True("f32c + 0.0 = 0.1 + 0.0");  // both sides take f32
    table.True("NOT f32c * 1 = 0.1 AND f32c * 1 > 0.1");  // f64 on both sides
    table.True("f32c IN (0.1) AND f32c BETWEEN 0.1 AND 0.1");
    table["f64c"] = 0.1;
    table.True("f64c = 0.1 AND f32c <> f64c AND f32c > f64c");
    table["f32c"] = 16777216.0F;
    table.True("f32c = 16777216 AND f32c < 16777217 AND f32c + 1 = 16777217");
    table["f32c"] = std::numeric_limits<float>::max();
    table.True("f32c = 3.4028234663852886e38");
    table.True("f32c * 2 > 6e38");
}

void TestTextBytesBitsBool() {
    Table table;
    table["t"] = std::string("\xc3\xa9");
    table["tl"] = std::string("z");
    table.True("t > tl AND t > 'z' AND 'a' < 'ab' AND '' < 'a' AND 'b' > 'ab' AND t = '\xc3\xa9'");
    table.True("t <> tl AND t >= t AND NOT t < t");
    table.True("t BETWEEN 'a' AND '\xff' AND t IN ('x', '\xc3\xa9')");
    table["by"] = BytesValue{.bytes = {0xff}};
    table.True("by > x'00ff' AND by = x'ff' AND by < x'ff00' AND x'' < x'00' AND by IN (x'ff')");
    table["b4"] = BitsValue{.digits = "0101"};
    table.True("b4 = b'0101' AND b4 <> b'1010' AND b4 IN (b'1111', b'0101') AND NOT b4 NOT IN (b'0101')");
    table.True("b8 = b'00000000'");
    table["flag"] = true;
    table.True("flag AND flag = TRUE AND flag <> FALSE AND NOT flag = FALSE AND flag IN (TRUE)");
    table.True("(flag) AND (flag = TRUE) AND COALESCE(nb, flag)");
    table.False("NOT flag");
    table["nb"] = false;
    table.False("nb");
    table.True("NOT nb AND nb = FALSE AND flag <> nb");
    // Chunk columns and positions.
    table["k"] = std::int64_t{-5};
    table["kt"] = std::string("abc");
    table.True("chunk.k = -5 AND chunk.kt = 'abc' AND chunk.kn IS NULL");
    table.x = -3;
    table.y = std::numeric_limits<std::int64_t>::max();
    table.z = std::numeric_limits<std::int64_t>::min();
    table.now = std::numeric_limits<std::uint64_t>::max();
    table.True("POS_X = -3 AND POS_Y = 9223372036854775807 AND POS_Z = -9223372036854775808");
    table.True("NOW() = 18446744073709551615 AND NOW() + 1 = 18446744073709551616.0 AND NOW() + POS_Z = 9223372036854775807");
    table.True("\"exists\" = 0 AND version = 0");
}

void TestThreeValuedLogic() {
    // p and q are T, F or NULL; SQL tables for AND, OR and NOT.
    const std::vector<ColumnValue> values{true, false, std::monostate{}};
    const char kT = 'T', kF = 'F', kU = 'U';
    const auto truth = [&](std::size_t i) { return i == 0 ? kT : i == 1 ? kF : kU; };
    const auto and3 = [&](char a, char b) { return a == kF || b == kF ? kF : a == kT && b == kT ? kT : kU; };
    const auto or3 = [&](char a, char b) { return a == kT || b == kT ? kT : a == kF && b == kF ? kF : kU; };
    const auto not3 = [&](char a) { return a == kT ? kF : a == kF ? kT : kU; };
    for (std::size_t i = 0; i < 3; ++i) {
        for (std::size_t j = 0; j < 3; ++j) {
            Table table;
            table["nb"] = values[i];
            table["nb2"] = values[j];
            const auto expect = [&](const std::string& text, char want) {
                const bool holds = table.Eval(text);
                const bool negated = table.Eval("NOT (" + text + ")");
                const char got = holds ? kT : negated ? kF : kU;
                Check(got == want, text + " with " + truth(i) + "," + truth(j) + ": got " + got + ", expected " + want);
            };
            expect("nb AND nb2", and3(truth(i), truth(j)));
            expect("nb OR nb2", or3(truth(i), truth(j)));
            expect("NOT nb", not3(truth(i)));
            expect("nb AND nb2 AND nb", and3(and3(truth(i), truth(j)), truth(i)));
            expect("nb = nb2", i == 2 || j == 2 ? kU : i == j ? kT : kF);
            expect("nb IS NULL", i == 2 ? kT : kF);
            expect("nb IS NOT NULL", i == 2 ? kF : kT);
        }
    }
    Table table;
    // Comparisons with NULL are unknown; IS NULL never is.
    table.Unknown("n8 = 1");
    table.Unknown("n8 <> 1");
    table.Unknown("n8 < n8");
    table.Unknown("NULL = NULL");
    table.Unknown("n8 + 1 > 0");
    table.Unknown("NULL");
    table.True("n8 IS NULL AND n8 + 1 IS NULL AND -n8 IS NULL AND NULL IS NULL AND NOT n8 IS NOT NULL");
    table.False("NOT (n8 IS NULL)");
    // IN and BETWEEN follow SQL.
    table.True("1 IN (1, NULL) AND 2 NOT IN (1, 3)");
    table.Unknown("2 IN (1, NULL)");
    table.Unknown("2 NOT IN (1, NULL)");
    table.Unknown("n8 IN (1, 2)");
    table.Unknown("n8 NOT IN (1, 2)");
    table.Unknown("n8 IN (NULL)");
    table.False("nan IN (nan, 1.5)");
    table.True("nan NOT IN (nan)");
    table.True("0 IN (-0.0) AND -0.0 IN (0)");
    table.True("5 NOT BETWEEN NULL AND 3 AND NOT 5 BETWEEN 6 AND NULL");
    table.Unknown("5 BETWEEN NULL AND 6");
    table.Unknown("5 BETWEEN 1 AND NULL");
    table.Unknown("n8 BETWEEN 1 AND 2");
    table.True("2 BETWEEN 1 AND 3 AND 1 BETWEEN 1 AND 1 AND 3 NOT BETWEEN 2 AND 1");
    table.False("nan BETWEEN -inf AND inf");
    // AND and OR stop once decided, left to right.
    table.True("TRUE OR 1 / 0 = 1");
    table.False("FALSE AND 1 / 0 = 1");
    table.True("u8c = 0 OR 1 / u8c > 2");
    table.Error("1 / 0 = 1 OR TRUE", "INVALID_ARGUMENT", "division by zero");
    table.Error("NULL OR 1 / 0 = 1", "INVALID_ARGUMENT", "division by zero");
    table.Error("2 BETWEEN 1 / 0 AND 3", "INVALID_ARGUMENT", "division by zero");
    table.Error("NOT EXISTS OR 1 / 0 = 1", "INVALID_ARGUMENT", "division by zero");
}

void TestFunctions() {
    Table table;
    table.True("COALESCE(n8, 7) = 7 AND COALESCE(n8, NULL, 3, 1 / 0) = 3 AND COALESCE(1, 1 / 0) = 1");
    table.Unknown("COALESCE(n8, NULL) = 1");
    table.True("COALESCE(n8, NULL) IS NULL AND COALESCE(NULL) IS NULL");
    table.Error("COALESCE(n8, 1 / 0) = 1", "INVALID_ARGUMENT", "division by zero");
    table.True("COALESCE(tl, t, 'x') = t AND COALESCE(nb, TRUE) AND COALESCE(nb, FALSE) = FALSE");
    // NULL arguments are ignored; NULL only when every one is.
    table.True("LEAST(n8, 3) = 3 AND GREATEST(n8, 3, NULL) = 3 AND LEAST(5, 2, 9) = 2 AND GREATEST(5, 2, 9) = 9");
    table.True("LEAST(n8, NULL) IS NULL AND GREATEST(NULL) IS NULL");
    table.True("LEAST(-1, -1.5) = -1.5 AND GREATEST(1, 1.5) = 1.5 AND LEAST(3) = 3");
    // Every argument is evaluated.
    table.Error("LEAST(1, 1 / 0) = 1", "INVALID_ARGUMENT", "division by zero");
    // NaN wins.
    table.True("LEAST(1, nan) <> LEAST(1, nan) AND GREATEST(nan, 1) <> 1");
    table.False("LEAST(1, nan) = 1 OR LEAST(1, nan) < 2");
    // Exact comparison picks the argument; a float result then.
    table["u64c"] = (std::uint64_t{1} << 53U) + 1U;
    table.True("LEAST(u64c, 9007199254740992.0) = 9007199254740992 AND GREATEST(u64c, 9007199254740992.0) = 9007199254740992");
    table.True("GREATEST(u64c, 1) = u64c AND COALESCE(u64c, 0.5) = 9007199254740992");
    // Of equal values the first stays.
    table["f64c"] = -0.0;
    const auto values = table.Assign("nf = LEAST(f64c, 0.0), f64c = GREATEST(0.0, f64c)");
    Check(std::signbit(std::get<double>(values[0])) && !std::signbit(std::get<double>(values[1])), "first of equal values");
}

void TestAbsentBlock() {
    Table table;
    table.exists = false;
    table["k"] = std::int64_t{9};
    table["u8c"] = std::uint64_t{5};
    table.False("EXISTS");
    table.True("NOT EXISTS");
    table.True("u8c IS NULL AND t IS NULL AND flag IS NULL AND b4 IS NULL AND chunk.k = 9");
    table.Unknown("u8c = 5");
    table.Unknown("flag");
    table.True("COALESCE(u8c, 0) = 0");
    ExpectValues(table.Assign("u8c = COALESCE(u8c, 0) + 1, n8 = u8c"), {std::uint64_t{1}, std::monostate{}}, "absent block");
    table.AssignError("u8c = u8c + 1", "INVALID_ARGUMENT", "column u8c cannot be NULL");
    table.exists = true;
    table.True("EXISTS AND u8c = 5");
}

void TestParameters() {
    Table table;
    table["u8c"] = std::uint64_t{10};
    table.parameters = {std::uint64_t{10}, std::string("ab"), std::monostate{}};
    table.True("u8c = $1 AND t < $2 AND COALESCE(n8, $3) IS NULL");
    table.parameters = {std::uint64_t{10}, std::monostate{}};
    table.Unknown("u8c + $1 > $2");
    // Integer parameters carry their 64-bit value.
    table.parameters = {std::uint64_t{1000}};
    table.True("u8c < $1");
    ExpectValues(table.Assign("u16c = u8c + $1"), {std::uint64_t{1010}}, "wide parameter");
    table.AssignError("u8c = $1", "OUT_OF_RANGE", "column u8c does not hold 1000: u8 holds 0..255");
    // Of another type, missing, or bits of another width.
    table.parameters = {std::int64_t{1}};
    table.Error("u8c = $1", "INVALID_ARGUMENT", "$1 must be a u8 value");
    table.parameters = {1.0F};
    table.Error("f64c = $1", "INVALID_ARGUMENT", "$1 must be a f64 value");
    table.parameters = {};
    table.Error("u8c = $1", "INVALID_ARGUMENT", "$1 has no value");
    table.parameters = {BitsValue{.digits = "01"}};
    table.Error("b4 = $1", "INVALID_ARGUMENT", "$1 must be a bits(4) value");
    table.parameters = {BitsValue{.digits = "0110"}, BytesValue{.bytes = {1, 2}}, 2.5F, true};
    table.True("b4 <> $1 AND by < $2 AND f32c < $3 AND flag <> $4");
    // NULL parameters.
    table.parameters = {std::monostate{}};
    table.Unknown("u8c = $1");
    table.AssignError("u8c = $1", "INVALID_ARGUMENT", "column u8c cannot be NULL");
    ExpectValues(table.Assign("n8 = $1"), {std::monostate{}}, "NULL parameter");
    // A stored value of another type is a broken caller.
    table["u8c"] = std::int64_t{1};
    table.Error("u8c = 1", "INTERNAL");
    table.row.pop_back();
    table.Error("i2c = 1", "INTERNAL");
}

void TestAssignmentValues() {
    Table table;
    table["u8c"] = std::uint64_t{254};
    ExpectValues(table.Assign("u8c = u8c + 1"), {std::uint64_t{255}}, "u8 max");
    table["u8c"] = std::uint64_t{255};
    table.AssignError("u8c = u8c + 1", "OUT_OF_RANGE", "column u8c does not hold 256: u8 holds 0..255");
    table.AssignError("u8c = u8c - 256", "OUT_OF_RANGE", "does not hold -1");
    ExpectValues(table.Assign("i8c = -128, u64c = 18446744073709551615, i64c = -9223372036854775808, u1c = 1, i2c = -2"),
                 {std::int64_t{-128}, std::numeric_limits<std::uint64_t>::max(), std::numeric_limits<std::int64_t>::min(),
                  std::uint64_t{1}, std::int64_t{-2}},
                 "extremes");
    table.AssignError("i8c = -129", "OUT_OF_RANGE", "i8 holds -128..127");
    table.AssignError("i8c = 128", "OUT_OF_RANGE", "does not hold 128");
    table.AssignError("u64c = 18446744073709551615 + 1", "OUT_OF_RANGE", "does not hold 18446744073709551616");
    table.AssignError("u64c = -1", "OUT_OF_RANGE", "does not hold -1");
    table.AssignError("i64c = 9223372036854775808", "OUT_OF_RANGE", "i64 holds");
    table.AssignError("u1c = 2", "OUT_OF_RANGE", "u1 holds 0..1");
    table.AssignError("i2c = 2", "OUT_OF_RANGE", "i2 holds -2..1");
    table.AssignError("u8c = 1 / 0", "INVALID_ARGUMENT", "division by zero");
    // Floats.
    ExpectValues(table.Assign("f32c = 0.1, f64c = 0.1, nf = 1"), {0.1F, 0.1, 1.0}, "float literals");
    ExpectValues(table.Assign("f32c = 16777217"), {16777216.0F}, "f32 rounds to nearest");
    ExpectValues(table.Assign("f32c = 16777219"), {16777220.0F}, "f32 rounds half to even");
    table["u64c"] = std::numeric_limits<std::uint64_t>::max();
    ExpectValues(table.Assign("f32c = u64c, f64c = u64c"), {18446744073709551616.0F, 18446744073709551616.0}, "u64 to floats");
    table["i64c"] = std::numeric_limits<std::int64_t>::min();
    ExpectValues(table.Assign("f32c = (i64c * i64c - 1) * 2 + 1, f64c = (i64c * i64c - 1) * 2 + 1"),
                 {1.7014118346046923e38F, 1.7014118346046923e38}, "i128 to floats");
    // 2^53 + 1 in an integer, rounded once only.
    table["u64c"] = (std::uint64_t{1} << 53U) + 1U;
    ExpectValues(table.Assign("f64c = u64c"), {9007199254740992.0}, "f64 from 2^53+1");
    table["u64c"] = (std::uint64_t{1} << 24U) + 1U + (std::uint64_t{1} << 40U);
    ExpectValues(table.Assign("f32c = u64c"), {static_cast<float>(static_cast<double>((std::uint64_t{1} << 24U) + 1U + (std::uint64_t{1} << 40U)))}, "f32 from integer");
    // 2^64 + 2^40 + 1 is above the midpoint of two f32 values; through f64
    // it would round to the midpoint, then to even.
    ExpectValues(table.Assign("f32c = 18446744073709551615 + 1099511627778"), {18446746272732807168.0F}, "f32 rounds once");
    table["f64c"] = 1e300;
    table.AssignError("f32c = f64c", "OUT_OF_RANGE", "column f32c does not hold a float beyond f32");
    table["f64c"] = -3.4028235677973366e38;  // just past -FLT_MAX
    table.AssignError("f32c = f64c", "OUT_OF_RANGE", "beyond f32");
    table["f64c"] = static_cast<double>(std::numeric_limits<float>::max());
    ExpectValues(table.Assign("f32c = f64c, nf = -f64c"), {std::numeric_limits<float>::max(), -static_cast<double>(std::numeric_limits<float>::max())}, "FLT_MAX");
    table["f64c"] = std::numeric_limits<double>::infinity();
    ExpectValues(table.Assign("f32c = f64c, nf = -f64c"), {std::numeric_limits<float>::infinity(), -std::numeric_limits<double>::infinity()}, "infinities");
    table["f64c"] = std::numeric_limits<double>::quiet_NaN();
    ExpectValues(table.Assign("f32c = f64c, nf = f64c"), {std::numeric_limits<float>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN()}, "NaN");
    ExpectValues(table.Assign("f64c = -0.0, f32c = -0.0, nf = 0 * -1.0"), {-0.0, -0.0F, -0.0}, "negative zero");
    table["f64c"] = 1e-310;
    ExpectValues(table.Assign("f32c = f64c"), {0.0F}, "underflow to zero");
    // NULL.
    table.AssignError("u8c = n8", "INVALID_ARGUMENT", "column u8c cannot be NULL");
    ExpectValues(table.Assign("n8 = NULL, nf = n8 + 1.5, tl = NULL"), {std::monostate{}, std::monostate{}, std::monostate{}}, "NULLs");
    // Text, bytes, bits, bool.
    ExpectValues(table.Assign("t = 'abc', tl = '\xc3\xa9', by = x'0102', b4 = b'1100', flag = TRUE"),
                 {std::string("abc"), std::string("\xc3\xa9"), BytesValue{.bytes = {1, 2}}, BitsValue{.digits = "1100"}, true},
                 "other types");
    table.AssignError("t = 'abcd'", "OUT_OF_RANGE", "column t does not hold 4 bytes: text(3) holds 0..3 UTF-8 bytes");
    table.AssignError("t = '\xff'", "INVALID_ARGUMENT", "column t takes UTF-8 text");
    table.AssignError("by = x'010203'", "OUT_OF_RANGE", "bytes(2) holds 0..2 bytes");
    table["tl"] = std::string("abcd");
    table.AssignError("t = tl", "OUT_OF_RANGE", "does not hold 4 bytes");
    ExpectValues(table.Assign("t = COALESCE(NULL, 'ab')"), {std::string("ab")}, "COALESCE text");
    // Every assignment reads the block as it was.
    table["u8c"] = std::uint64_t{1};
    table["u16c"] = std::uint64_t{2};
    ExpectValues(table.Assign("u8c = u16c, u16c = u8c, i16c = u8c + u16c"), {std::uint64_t{2}, std::uint64_t{1}, std::int64_t{3}}, "swap");
    // The first failing assignment decides.
    table.AssignError("u8c = 1 / 0, u16c = 99999999", "INVALID_ARGUMENT", "division by zero");
    table.AssignError("u16c = 99999999, u8c = 1 / 0", "OUT_OF_RANGE", "does not hold 99999999");
}

void TestErrorCodes() {
    // Each exception type reaches the client as the code the engine maps it
    // to.
    Table table;
    Check(CodeOf([] { (void)expr::ParseCondition("a <"); }) == "SYNTAX", "syntax");
    Check(table.Code("t < 1") == "INVALID_ARGUMENT", "type error");
    Check(table.Code("1 / 0 = 1") == "INVALID_ARGUMENT", "division by zero");
    Check(table.Code("18446744073709551615 * 18446744073709551615 = 1") == "OUT_OF_RANGE", "overflow");
    Check(table.AssignCode("u8c = 256") == "OUT_OF_RANGE", "assignment range");
    Check(table.AssignCode("u8c = NULL") == "INVALID_ARGUMENT", "NULL assignment");
    try {
        (void)expr::ParseCondition(")");
        Failure("expected a ParseError");
    } catch (const cql::ParseError& e) {
        Check(std::string(e.what()) == "column 1: expected a value, got ')'", e.what());
    }
}

}  // namespace

// The property and fuzz tests follow in this file.
namespace property {
int Run();
}  // namespace property

int main() {
    TestPrecedenceAndAssociativity();
    TestParenthesesAndPredicates();
    TestKeywordsAndNames();
    TestSyntaxErrors();
    TestAssignmentsGrammar();
    TestStatementEmbedding();
    TestStatementsUnchanged();
    TestVersionClause();
    TestLimits();
    TestMalformedTrees();
    TestParameterTypes();
    TestTypeErrors();
    TestAssignmentTyping();
    TestIntegers();
    TestNumbersMixed();
    TestTextBytesBitsBool();
    TestThreeValuedLogic();
    TestFunctions();
    TestAbsentBlock();
    TestParameters();
    TestAssignmentValues();
    TestErrorCodes();
    std::printf("expr unit tests passed\n");
    return property::Run();
}

// ---------------------------------------------------------------------------
// The property test: random expressions over random tables, evaluated by the
// engine and by a second evaluator written for clarity, not speed. It keeps
// integers as unbounded sign-magnitude numbers, compares integers with
// floats through exact binary fractions, converts integers to floats by its
// own rounding, and spells out three-valued logic as tables. Run more cases
// with CHUNKDB_EXPR_CASES=<n> and another seed with CHUNKDB_EXPR_SEED=<n>.

namespace property {

namespace {

namespace cql = chunkdb::cql;
namespace expr = chunkdb::expr;
using chunkdb::BitsValue;
using chunkdb::BytesValue;
using chunkdb::Column;
using chunkdb::ColumnKind;
using chunkdb::ColumnType;
using chunkdb::ColumnValue;

[[noreturn]] void Fail(const std::string& message) {
    std::fprintf(stderr, "FAILED: %s\n", message.c_str());
    std::fflush(stderr);
    std::abort();
}

// ---- Unbounded integers: sign and magnitude, 32-bit limbs, lowest first.

struct Big {
    bool negative = false;
    std::vector<std::uint32_t> limbs;
};

using Limbs = std::vector<std::uint32_t>;

void Trim(Limbs* limbs) {
    while (!limbs->empty() && limbs->back() == 0U) limbs->pop_back();
}

Big Normal(bool negative, Limbs limbs) {
    Trim(&limbs);
    return Big{.negative = negative && !limbs.empty(), .limbs = std::move(limbs)};
}

Big FromMagnitude(std::uint64_t magnitude, bool negative) {
    return Normal(negative, {static_cast<std::uint32_t>(magnitude), static_cast<std::uint32_t>(magnitude >> 32U)});
}

Big FromUnsigned(std::uint64_t value) { return FromMagnitude(value, false); }

Big FromSigned(std::int64_t value) {
    return FromMagnitude(value < 0 ? 0U - static_cast<std::uint64_t>(value) : static_cast<std::uint64_t>(value), value < 0);
}

int CompareLimbs(const Limbs& a, const Limbs& b) {
    if (a.size() != b.size()) return a.size() < b.size() ? -1 : 1;
    for (std::size_t i = a.size(); i-- > 0;) {
        if (a[i] != b[i]) return a[i] < b[i] ? -1 : 1;
    }
    return 0;
}

Limbs AddLimbs(const Limbs& a, const Limbs& b) {
    Limbs out;
    std::uint64_t carry = 0;
    for (std::size_t i = 0; i < std::max(a.size(), b.size()) || carry != 0U; ++i) {
        const std::uint64_t sum = carry + (i < a.size() ? a[i] : 0U) + (i < b.size() ? b[i] : 0U);
        out.push_back(static_cast<std::uint32_t>(sum));
        carry = sum >> 32U;
    }
    Trim(&out);
    return out;
}

// a - b with a >= b.
Limbs SubtractLimbs(const Limbs& a, const Limbs& b) {
    Limbs out;
    std::int64_t borrow = 0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        std::int64_t difference = static_cast<std::int64_t>(a[i]) - (i < b.size() ? b[i] : 0) - borrow;
        borrow = difference < 0 ? 1 : 0;
        if (difference < 0) difference += std::int64_t{1} << 32U;
        out.push_back(static_cast<std::uint32_t>(difference));
    }
    Trim(&out);
    return out;
}

Limbs MultiplyLimbs(const Limbs& a, const Limbs& b) {
    Limbs out(a.size() + b.size() + 1U, 0U);
    for (std::size_t i = 0; i < a.size(); ++i) {
        std::uint64_t carry = 0;
        for (std::size_t j = 0; j < b.size() || carry != 0U; ++j) {
            const std::uint64_t product = out[i + j] + carry + (j < b.size() ? static_cast<std::uint64_t>(a[i]) * b[j] : 0U);
            out[i + j] = static_cast<std::uint32_t>(product);
            carry = product >> 32U;
        }
    }
    Trim(&out);
    return out;
}

std::size_t BitLength(const Limbs& limbs) {
    if (limbs.empty()) return 0;
    return (limbs.size() - 1U) * 32U + static_cast<std::size_t>(std::bit_width(limbs.back()));
}

bool Bit(const Limbs& limbs, std::size_t index) {
    const std::size_t limb = index / 32U;
    return limb < limbs.size() && ((limbs[limb] >> (index % 32U)) & 1U) != 0U;
}

Limbs ShiftLeft(const Limbs& limbs, std::size_t bits) {
    Limbs out(bits / 32U, 0U);
    const std::size_t shift = bits % 32U;
    std::uint32_t carry = 0;
    for (const std::uint32_t limb : limbs) {
        out.push_back(shift == 0U ? limb : (limb << shift) | carry);
        carry = shift == 0U ? 0U : limb >> (32U - shift);
    }
    out.push_back(carry);
    Trim(&out);
    return out;
}

// Long division, one bit at a time.
std::pair<Limbs, Limbs> DivideLimbs(const Limbs& a, const Limbs& b) {
    Limbs quotient(a.size(), 0U);
    Limbs remainder;
    for (std::size_t i = BitLength(a); i-- > 0;) {
        remainder = ShiftLeft(remainder, 1);
        if (Bit(a, i)) remainder = AddLimbs(remainder, {1U});
        if (CompareLimbs(remainder, b) >= 0) {
            remainder = SubtractLimbs(remainder, b);
            quotient[i / 32U] |= 1U << (i % 32U);
        }
    }
    Trim(&quotient);
    return {quotient, remainder};
}

int Compare(const Big& a, const Big& b) {
    if (a.negative != b.negative) return a.negative ? -1 : 1;
    const int magnitude = CompareLimbs(a.limbs, b.limbs);
    return a.negative ? -magnitude : magnitude;
}

Big Negate(Big a) { return Normal(!a.negative, std::move(a.limbs)); }

Big Add(const Big& a, const Big& b) {
    if (a.negative == b.negative) return Normal(a.negative, AddLimbs(a.limbs, b.limbs));
    if (CompareLimbs(a.limbs, b.limbs) >= 0) return Normal(a.negative, SubtractLimbs(a.limbs, b.limbs));
    return Normal(b.negative, SubtractLimbs(b.limbs, a.limbs));
}

Big Subtract(const Big& a, const Big& b) { return Add(a, Negate(b)); }

Big Multiply(const Big& a, const Big& b) { return Normal(a.negative != b.negative, MultiplyLimbs(a.limbs, b.limbs)); }

Big Power2(std::size_t n) { return Normal(false, ShiftLeft({1U}, n)); }

Big Shifted(const Big& a, std::size_t bits) { return Normal(a.negative, ShiftLeft(a.limbs, bits)); }

bool IsZero(const Big& a) { return a.limbs.empty(); }

std::string Decimal(const Big& a) {
    if (IsZero(a)) return "0";
    std::string digits;
    Limbs rest = a.limbs;
    while (!rest.empty()) {
        auto [quotient, remainder] = DivideLimbs(rest, {10U});
        digits.insert(digits.begin(), static_cast<char>('0' + (remainder.empty() ? 0U : remainder[0])));
        rest = quotient;
    }
    return (a.negative ? "-" : "") + digits;
}

// The nearest float with `precision` bits of mantissa, ties to even; the
// magnitudes here are far inside the exponent range.
double Nearest(const Big& a, int precision) {
    const std::size_t length = BitLength(a.limbs);
    if (length == 0U) return 0.0;
    double magnitude = 0.0;
    if (length <= static_cast<std::size_t>(precision)) {
        for (std::size_t i = length; i-- > 0;) magnitude = magnitude * 2.0 + (Bit(a.limbs, i) ? 1.0 : 0.0);
    } else {
        const std::size_t shift = length - static_cast<std::size_t>(precision);
        std::uint64_t top = 0;
        for (std::size_t i = length; i-- > shift;) top = top * 2U + (Bit(a.limbs, i) ? 1U : 0U);
        const bool half = Bit(a.limbs, shift - 1U);
        bool sticky = false;
        for (std::size_t i = 0; i + 1U < shift; ++i) sticky = sticky || Bit(a.limbs, i);
        if (half && (sticky || (top & 1U) != 0U)) ++top;
        magnitude = std::ldexp(static_cast<double>(top), static_cast<int>(shift));
    }
    return a.negative ? -magnitude : magnitude;
}

// -1, 0 or 1 for an integer against a finite float, by writing the float
// as M * 2^e and scaling one side.
int CompareExact(const Big& integer, double number) {
    int exponent = 0;
    const double fraction = std::frexp(number, &exponent);
    const auto mantissa = static_cast<std::int64_t>(std::ldexp(fraction, 53));
    const int scale = exponent - 53;
    Big left = integer;
    Big right = FromSigned(mantissa);
    if (scale >= 0) {
        right = Shifted(right, static_cast<std::size_t>(scale));
    } else {
        left = Shifted(left, static_cast<std::size_t>(-scale));
    }
    return Compare(left, right);
}

// ---- Reference values and trees.

enum class Kind { kNull, kInt, kFloat, kBool, kText, kBytes, kBits };

struct Value {
    Kind kind = Kind::kNull;
    Big integer{};
    double number = 0.0;
    bool boolean = false;
    std::string bytes{};
};

struct Error {
    std::string code;
};

void Overflowed() { throw Error{"OUT_OF_RANGE"}; }

const Big& I128Max() {
    static const Big value = Subtract(Power2(127), FromUnsigned(1));
    return value;
}
const Big& I128Min() {
    static const Big value = Negate(Power2(127));
    return value;
}

Big InRange(Big value) {
    if (Compare(value, I128Max()) > 0 || Compare(value, I128Min()) < 0) Overflowed();
    return value;
}

enum class Node {
    kLiteral, kParameter, kColumn, kChunk, kPosX, kPosY, kPosZ, kNow, kNegate, kArithmetic, kLeast, kGreatest,
    kCoalesce, kCompare, kIsNull, kIsNotNull, kIn, kNotIn, kBetween, kNotBetween, kExists, kNot, kAnd, kOr,
};

struct Tree {
    Node node = Node::kLiteral;
    Value literal{};
    char op = '+';    // kArithmetic: + - * / %
    int compare = 0;  // kCompare: an index into kComparisons
    std::size_t index = 0;  // a column's slot or a parameter's number
    std::vector<Tree> operands{};
    bool parenthesized = false;
};

constexpr std::array<const char*, 6> kComparisons{"=", "<>", "<", "<=", ">", ">="};

// The static type of a value: its kind (kNull for NULL alone) and, for
// bits, the width.
struct Type {
    Kind kind = Kind::kNull;
    std::uint32_t width = 0;
};

Kind KindOf(const ColumnType& type) {
    switch (type.kind) {
        case ColumnKind::kUnsigned:
        case ColumnKind::kSigned: return Kind::kInt;
        case ColumnKind::kFloat32:
        case ColumnKind::kFloat64: return Kind::kFloat;
        case ColumnKind::kBool: return Kind::kBool;
        case ColumnKind::kText: return Kind::kText;
        case ColumnKind::kBytes: return Kind::kBytes;
        case ColumnKind::kBits: return Kind::kBits;
    }
    return Kind::kNull;
}

bool IsNumeric(Kind kind) { return kind == Kind::kInt || kind == Kind::kFloat; }

struct Case {
    std::vector<Column> block;
    std::vector<Column> chunk;
    std::vector<ColumnValue> row;
    std::vector<ColumnValue> chunk_row;
    bool exists = true;
    std::int64_t x = 0, y = 0, z = 0;
    std::uint64_t now = 0;
    std::vector<ColumnType> parameter_types;  // $n at n - 1
    std::vector<ColumnValue> parameters;
};

Value Load(const ColumnValue& stored) {
    Value value;
    std::visit(
        [&](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::uint64_t>) {
                value.kind = Kind::kInt;
                value.integer = FromUnsigned(v);
            } else if constexpr (std::is_same_v<T, std::int64_t>) {
                value.kind = Kind::kInt;
                value.integer = FromSigned(v);
            } else if constexpr (std::is_same_v<T, bool>) {
                value.kind = Kind::kBool;
                value.boolean = v;
            } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
                value.kind = Kind::kFloat;
                value.number = static_cast<double>(v);
            } else if constexpr (std::is_same_v<T, std::string>) {
                value.kind = Kind::kText;
                value.bytes = v;
            } else if constexpr (std::is_same_v<T, BytesValue>) {
                value.kind = Kind::kBytes;
                value.bytes.assign(v.bytes.begin(), v.bytes.end());
            } else if constexpr (std::is_same_v<T, BitsValue>) {
                value.kind = Kind::kBits;
                value.bytes = v.digits;
            }
        },
        stored);
    return value;
}

// ---- Reference typing: what planning refuses (for trees made to be wrong).

Type Merge(const Type& a, const Type& b) {
    if (a.kind == Kind::kNull) return b;
    if (b.kind == Kind::kNull) return a;
    if (a.kind == Kind::kFloat || b.kind == Kind::kFloat) return Type{Kind::kFloat, 0};
    return Type{Kind::kInt, 0};
}

[[noreturn]] void Refused() { throw Error{"INVALID_ARGUMENT"}; }

class Reference {
  public:
    explicit Reference(const Case& c) : case_(c) {}

    Type TypeOf(const Tree& tree) const {
        const auto& ops = tree.operands;
        switch (tree.node) {
            case Node::kLiteral:
                return Type{tree.literal.kind, static_cast<std::uint32_t>(tree.literal.bytes.size())};
            case Node::kParameter: {
                const ColumnType& type = case_.parameter_types[tree.index - 1U];
                return Type{KindOf(type), type.size};
            }
            case Node::kColumn:
                return Type{KindOf(case_.block[tree.index].type), case_.block[tree.index].type.size};
            case Node::kChunk:
                return Type{KindOf(case_.chunk[tree.index].type), case_.chunk[tree.index].type.size};
            case Node::kPosX:
            case Node::kPosY:
            case Node::kPosZ:
            case Node::kNow:
                return Type{Kind::kInt, 0};
            case Node::kNegate: {
                const Type type = TypeOf(ops[0]);
                if (type.kind != Kind::kNull && !IsNumeric(type.kind)) Refused();
                return type;
            }
            case Node::kArithmetic:
            case Node::kLeast:
            case Node::kGreatest: {
                Type type;
                for (const Tree& operand : ops) {
                    const Type t = TypeOf(operand);
                    if (t.kind != Kind::kNull && !IsNumeric(t.kind)) Refused();
                    type = Merge(type, t);
                }
                return type;
            }
            case Node::kCoalesce: {
                Type type;
                for (const Tree& operand : ops) {
                    const Type t = TypeOf(operand);
                    if (type.kind == Kind::kNull) {
                        type = t;
                    } else if (t.kind != Kind::kNull) {
                        if (IsNumeric(type.kind) && IsNumeric(t.kind)) {
                            type = Merge(type, t);
                        } else if (type.kind != t.kind || (t.kind == Kind::kBits && t.width != type.width)) {
                            Refused();
                        }
                    }
                }
                return type;
            }
            case Node::kCompare:
                Comparable(TypeOf(ops[0]), TypeOf(ops[1]), tree.compare <= 1);
                return Type{Kind::kBool, 0};
            case Node::kIn:
            case Node::kNotIn:
                for (std::size_t k = 1; k < ops.size(); ++k) Comparable(TypeOf(ops[0]), TypeOf(ops[k]), true);
                return Type{Kind::kBool, 0};
            case Node::kBetween:
            case Node::kNotBetween: {
                Type value = TypeOf(ops[0]);
                const Type low = TypeOf(ops[1]);
                const Type high = TypeOf(ops[2]);
                if (value.kind == Kind::kNull) value = low.kind != Kind::kNull ? low : high;
                Comparable(value, low, false);
                Comparable(value, high, false);
                return Type{Kind::kBool, 0};
            }
            case Node::kIsNull:
            case Node::kIsNotNull:
                (void)TypeOf(ops[0]);
                return Type{Kind::kBool, 0};
            case Node::kExists:
                return Type{Kind::kBool, 0};
            case Node::kNot:
            case Node::kAnd:
            case Node::kOr:
                for (const Tree& operand : ops) {
                    const Kind kind = TypeOf(operand).kind;
                    if (kind != Kind::kBool && kind != Kind::kNull) Refused();
                }
                return Type{Kind::kBool, 0};
        }
        return Type{};
    }

    static void Comparable(const Type& a, const Type& b, bool equality) {
        if (a.kind != Kind::kNull && b.kind != Kind::kNull) {
            const bool numbers = IsNumeric(a.kind) && IsNumeric(b.kind);
            if (!numbers && (a.kind != b.kind || (a.kind == Kind::kBits && a.width != b.width))) Refused();
        }
        const Kind known = a.kind != Kind::kNull ? a.kind : b.kind;
        if ((known == Kind::kBool || known == Kind::kBits) && !equality) Refused();
    }

    // ---- Evaluation.

    static double AsFloat(const Value& value) {
        return value.kind == Kind::kInt ? Nearest(value.integer, 53) : value.number;
    }

    // -1, 0, 1, or 2 when unordered.
    static int Order(const Value& a, const Value& b) {
        if (a.kind == Kind::kInt && b.kind == Kind::kInt) return Compare(a.integer, b.integer);
        if (IsNumeric(a.kind) && IsNumeric(b.kind)) {
            if (a.kind == Kind::kInt || b.kind == Kind::kInt) {
                const bool flipped = a.kind == Kind::kFloat;
                const Big& integer = flipped ? b.integer : a.integer;
                const double number = flipped ? a.number : b.number;
                int order = 0;
                if (std::isnan(number)) return 2;
                if (std::isinf(number)) {
                    order = number > 0 ? -1 : 1;
                } else {
                    order = CompareExact(integer, number);
                }
                return flipped ? -order : order;
            }
            if (std::isnan(a.number) || std::isnan(b.number)) return 2;
            return a.number < b.number ? -1 : a.number > b.number ? 1 : 0;
        }
        if (a.kind == Kind::kBool) return a.boolean == b.boolean ? 0 : a.boolean ? 1 : -1;
        // Bytewise, each byte unsigned.
        for (std::size_t i = 0; i < std::min(a.bytes.size(), b.bytes.size()); ++i) {
            const auto x = static_cast<unsigned char>(a.bytes[i]);
            const auto y = static_cast<unsigned char>(b.bytes[i]);
            if (x != y) return x < y ? -1 : 1;
        }
        return a.bytes.size() < b.bytes.size() ? -1 : a.bytes.size() > b.bytes.size() ? 1 : 0;
    }

    // Three-valued truth: 'T', 'F' or 'U'.
    static char Holds(int compare, const Value& a, const Value& b) {
        if (a.kind == Kind::kNull || b.kind == Kind::kNull) return 'U';
        const int order = Order(a, b);
        if (order == 2) return compare == 1 ? 'T' : 'F';
        static constexpr std::array<std::array<bool, 3>, 6> kTable{{
            // less, equal, greater
            {false, true, false},  // =
            {true, false, true},   // <>
            {true, false, false},  // <
            {true, true, false},   // <=
            {false, false, true},  // >
            {false, true, true},   // >=
        }};
        return kTable[static_cast<std::size_t>(compare)][static_cast<std::size_t>(order + 1)] ? 'T' : 'F';
    }

    static char Not(char t) { return t == 'T' ? 'F' : t == 'F' ? 'T' : 'U'; }
    static char And(char a, char b) {
        static const std::string kTable = "TFU" "FFF" "UFU";  // rows a = T, F, U; columns b = T, F, U
        return kTable[std::string("TFU").find(a) * 3U + std::string("TFU").find(b)];
    }
    static char Or(char a, char b) {
        static const std::string kTable = "TTT" "TFU" "TUU";
        return kTable[std::string("TFU").find(a) * 3U + std::string("TFU").find(b)];
    }

    Value Int(Big value) const {
        Value out;
        out.kind = Kind::kInt;
        out.integer = std::move(value);
        return out;
    }

    Value ValueOf(const Tree& tree, Type type) const {
        const auto& ops = tree.operands;
        switch (tree.node) {
            case Node::kLiteral:
                return tree.literal;
            case Node::kParameter:
                return Load(case_.parameters[tree.index - 1U]);
            case Node::kColumn:
                return case_.exists ? Load(case_.row[tree.index]) : Value{};
            case Node::kChunk:
                return Load(case_.chunk_row[tree.index]);
            case Node::kPosX: return Int(FromSigned(case_.x));
            case Node::kPosY: return Int(FromSigned(case_.y));
            case Node::kPosZ: return Int(FromSigned(case_.z));
            case Node::kNow: return Int(FromUnsigned(case_.now));
            case Node::kNegate: {
                Value value = Eval(ops[0]);
                if (value.kind == Kind::kInt) value.integer = InRange(Negate(value.integer));
                if (value.kind == Kind::kFloat) value.number = -value.number;
                return value;
            }
            case Node::kArithmetic: {
                const Value a = Eval(ops[0]);
                const Value b = Eval(ops[1]);
                if (a.kind == Kind::kNull || b.kind == Kind::kNull) return Value{};
                if (a.kind == Kind::kInt && b.kind == Kind::kInt) {
                    switch (tree.op) {
                        case '+': return Int(InRange(Add(a.integer, b.integer)));
                        case '-': return Int(InRange(Subtract(a.integer, b.integer)));
                        case '*': return Int(InRange(Multiply(a.integer, b.integer)));
                        default: {
                            if (IsZero(b.integer)) throw Error{"INVALID_ARGUMENT"};
                            auto [quotient, remainder] = DivideLimbs(a.integer.limbs, b.integer.limbs);
                            if (tree.op == '/') return Int(InRange(Normal(a.integer.negative != b.integer.negative, quotient)));
                            return Int(Normal(a.integer.negative, remainder));
                        }
                    }
                }
                const double x = AsFloat(a);
                const double y = AsFloat(b);
                Value out;
                out.kind = Kind::kFloat;
                out.number = tree.op == '+' ? x + y : tree.op == '-' ? x - y : tree.op == '*' ? x * y : tree.op == '/' ? x / y : std::fmod(x, y);
                return out;
            }
            case Node::kLeast:
            case Node::kGreatest: {
                std::vector<Value> values;
                for (const Tree& operand : ops) values.push_back(Eval(operand));
                Value best;
                bool nan = false;
                for (const Value& value : values) {
                    if (value.kind == Kind::kNull) continue;
                    if (value.kind == Kind::kFloat && std::isnan(value.number)) {
                        nan = true;
                        continue;
                    }
                    const int better = tree.node == Node::kLeast ? -1 : 1;
                    if (best.kind == Kind::kNull || Order(value, best) == better) best = value;
                }
                if (nan) {
                    Value out;
                    out.kind = Kind::kFloat;
                    out.number = std::numeric_limits<double>::quiet_NaN();
                    return out;
                }
                return Widen(best, type);
            }
            case Node::kCoalesce:
                for (const Tree& operand : ops) {
                    const Value value = Eval(operand);
                    if (value.kind != Kind::kNull) return Widen(value, type);
                }
                return Value{};
            default:
                Fail("a predicate as a value");
        }
    }

    static Value Widen(Value value, const Type& type) {
        if (type.kind == Kind::kFloat && value.kind == Kind::kInt) {
            value.number = Nearest(value.integer, 53);
            value.kind = Kind::kFloat;
        }
        return value;
    }

    Value Eval(const Tree& tree) const { return ValueOf(tree, TypeOf(tree)); }

    char Truth(const Tree& tree) const {
        const auto& ops = tree.operands;
        switch (tree.node) {
            case Node::kCompare: {
                const Value a = Eval(ops[0]);
                const Value b = Eval(ops[1]);
                return Holds(tree.compare, a, b);
            }
            case Node::kIsNull: return Eval(ops[0]).kind == Kind::kNull ? 'T' : 'F';
            case Node::kIsNotNull: return Eval(ops[0]).kind == Kind::kNull ? 'F' : 'T';
            case Node::kIn:
            case Node::kNotIn: {
                const Value value = Eval(ops[0]);
                char truth = 'F';
                for (std::size_t k = 1; k < ops.size(); ++k) truth = Or(truth, Holds(0, value, Eval(ops[k])));
                return tree.node == Node::kIn ? truth : Not(truth);
            }
            case Node::kBetween:
            case Node::kNotBetween: {
                const Value value = Eval(ops[0]);
                const Value low = Eval(ops[1]);
                const Value high = Eval(ops[2]);
                const char truth = And(Holds(5, value, low), Holds(3, value, high));
                return tree.node == Node::kBetween ? truth : Not(truth);
            }
            case Node::kExists: return case_.exists ? 'T' : 'F';
            case Node::kNot: return Not(Truth(ops[0]));
            case Node::kAnd: {
                char truth = 'T';
                for (const Tree& operand : ops) {
                    truth = And(truth, Truth(operand));
                    if (truth == 'F') return truth;
                }
                return truth;
            }
            case Node::kOr: {
                char truth = 'F';
                for (const Tree& operand : ops) {
                    truth = Or(truth, Truth(operand));
                    if (truth == 'T') return truth;
                }
                return truth;
            }
            default: {
                const Value value = Eval(tree);
                return value.kind == Kind::kNull ? 'U' : value.boolean ? 'T' : 'F';
            }
        }
    }

    // The value a column takes, as the engine returns it.
    static ColumnValue Store(const Column& column, const Value& value) {
        if (value.kind == Kind::kNull) {
            if (!column.nullable) throw Error{"INVALID_ARGUMENT"};
            return std::monostate{};
        }
        const ColumnType& type = column.type;
        switch (type.kind) {
            case ColumnKind::kUnsigned: {
                if (value.integer.negative || BitLength(value.integer.limbs) > type.size) throw Error{"OUT_OF_RANGE"};
                std::uint64_t out = 0;
                for (std::size_t i = value.integer.limbs.size(); i-- > 0;) out = (out << 32U) | value.integer.limbs[i];
                return out;
            }
            case ColumnKind::kSigned: {
                const Big half = Power2(type.size - 1U);
                if (Compare(value.integer, Negate(half)) < 0 || Compare(value.integer, half) >= 0) throw Error{"OUT_OF_RANGE"};
                std::uint64_t magnitude = 0;
                for (std::size_t i = value.integer.limbs.size(); i-- > 0;) magnitude = (magnitude << 32U) | value.integer.limbs[i];
                return value.integer.negative ? static_cast<std::int64_t>(0U - magnitude) : static_cast<std::int64_t>(magnitude);
            }
            case ColumnKind::kFloat32: {
                if (value.kind == Kind::kInt) return static_cast<float>(Nearest(value.integer, 24));
                if (std::isfinite(value.number) && std::fabs(value.number) > static_cast<double>(std::numeric_limits<float>::max())) {
                    throw Error{"OUT_OF_RANGE"};
                }
                return static_cast<float>(value.number);
            }
            case ColumnKind::kFloat64:
                return AsFloat(value);
            case ColumnKind::kBool:
                return value.boolean;
            case ColumnKind::kText: {
                if (value.bytes.size() > type.size) throw Error{"OUT_OF_RANGE"};
                const auto* data = reinterpret_cast<const std::uint8_t*>(value.bytes.data());
                if (!chunkdb::IsUtf8(std::span<const std::uint8_t>(data, value.bytes.size()))) throw Error{"INVALID_ARGUMENT"};
                return value.bytes;
            }
            case ColumnKind::kBytes:
                if (value.bytes.size() > type.size) throw Error{"OUT_OF_RANGE"};
                return BytesValue{.bytes = std::vector<std::uint8_t>(value.bytes.begin(), value.bytes.end())};
            case ColumnKind::kBits:
                return BitsValue{.digits = value.bytes};
        }
        return std::monostate{};
    }

  private:
    const Case& case_;
};

// ---- Random tables, rows and trees, and the text of a tree.

using Random = std::mt19937_64;

// Concatenates in order: the elements of a braced list are evaluated left to
// right, unlike the operands of +, so a seed gives the same text with every
// compiler.
std::string Cat(std::initializer_list<std::string> parts) {
    std::string out;
    for (const std::string& part : parts) out += part;
    return out;
}

std::uint64_t Draw(Random& random, std::uint64_t below) { return below == 0U ? 0U : random() % below; }
bool Chance(Random& random, int percent) { return static_cast<int>(Draw(random, 100)) < percent; }

ColumnType RandomType(Random& random) {
    switch (Draw(random, 11)) {
        case 0: case 1: {
            static constexpr std::array<std::uint32_t, 8> kSizes{1, 7, 8, 16, 32, 53, 63, 64};
            return ColumnType{.kind = ColumnKind::kUnsigned, .size = kSizes[Draw(random, kSizes.size())]};
        }
        case 2: case 3: {
            static constexpr std::array<std::uint32_t, 8> kSizes{2, 3, 8, 16, 32, 54, 63, 64};
            return ColumnType{.kind = ColumnKind::kSigned, .size = kSizes[Draw(random, kSizes.size())]};
        }
        case 4: return ColumnType{.kind = ColumnKind::kFloat32, .size = 32};
        case 5: return ColumnType{.kind = ColumnKind::kFloat64, .size = 64};
        case 6: return ColumnType{.kind = ColumnKind::kBool, .size = 1};
        case 7: return ColumnType{.kind = ColumnKind::kText, .size = static_cast<std::uint32_t>(Draw(random, 6))};
        case 8: return ColumnType{.kind = ColumnKind::kBytes, .size = static_cast<std::uint32_t>(Draw(random, 4))};
        case 9: return ColumnType{.kind = ColumnKind::kBits, .size = static_cast<std::uint32_t>(1U + Draw(random, 6))};
        default: return ColumnType{.kind = ColumnKind::kSigned, .size = 64};
    }
}

std::string RandomText(Random& random, std::size_t max) {
    static const std::array<std::string, 7> kPieces{"a", "b", "z", "'", "\xc3\xa9", "\x7f", " "};
    std::string out;
    const std::size_t length = Draw(random, max + 1U);
    while (out.size() < length) {
        const std::string& piece = kPieces[Draw(random, kPieces.size())];
        if (out.size() + piece.size() <= length) out += piece; else out += 'a';
    }
    return out;
}

std::string RandomBytes(Random& random, std::size_t max) {
    std::string out(Draw(random, max + 1U), '\0');
    for (char& c : out) {
        static constexpr std::array<unsigned char, 5> kBytes{0x00, 0x01, 0x7f, 0x80, 0xff};
        c = static_cast<char>(Chance(random, 70) ? kBytes[Draw(random, kBytes.size())] : Draw(random, 256));
    }
    return out;
}

std::string RandomDigits(Random& random, std::size_t width) {
    std::string out(width, '0');
    for (char& c : out) c = Chance(random, 50) ? '1' : '0';
    return out;
}

double RandomDouble(Random& random) {
    static const std::array<double, 22> kValues{
        0.0, -0.0, 1.0, -1.0, 0.5, -2.5, 3.0, 1e300, -1e300, 0.1, 9007199254740992.0, 9007199254740994.0,
        9223372036854775808.0, -9223372036854775808.0, 18446744073709551616.0, 1.7014118346046923e38,
        -1.7014118346046923e38, std::numeric_limits<double>::max(), std::numeric_limits<double>::denorm_min(),
        std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()};
    if (Chance(random, 80)) return kValues[Draw(random, kValues.size())];
    double value = 0.0;
    do value = std::bit_cast<double>(random()); while (std::isnan(value));
    return Chance(random, 50) ? value : static_cast<double>(static_cast<std::int64_t>(random()) >> Draw(random, 64));
}

float RandomFloat(Random& random) {
    static const std::array<float, 14> kValues{
        0.0F, -0.0F, 1.0F, -1.0F, 0.5F, 0.1F, 16777216.0F, 16777218.0F, std::numeric_limits<float>::max(),
        -std::numeric_limits<float>::max(), std::numeric_limits<float>::denorm_min(), std::numeric_limits<float>::infinity(),
        -std::numeric_limits<float>::infinity(), std::numeric_limits<float>::quiet_NaN()};
    if (Chance(random, 80)) return kValues[Draw(random, kValues.size())];
    float value = 0.0F;
    do value = std::bit_cast<float>(static_cast<std::uint32_t>(random())); while (std::isnan(value));
    return value;
}

ColumnValue RandomValue(Random& random, const ColumnType& type) {
    switch (type.kind) {
        case ColumnKind::kUnsigned: {
            const std::uint64_t max = type.size == 64U ? std::numeric_limits<std::uint64_t>::max() : (std::uint64_t{1} << type.size) - 1U;
            const std::array<std::uint64_t, 5> picks{0, 1, max, max - (max > 0 ? 1 : 0), max / 2U};
            return Chance(random, 60) ? picks[Draw(random, picks.size())] : random() & max;
        }
        case ColumnKind::kSigned: {
            const std::int64_t max = type.size == 64U ? std::numeric_limits<std::int64_t>::max() : (std::int64_t{1} << (type.size - 1U)) - 1;
            const std::int64_t min = -max - 1;
            const std::array<std::int64_t, 7> picks{min, min + 1, -1, 0, 1, max, max - 1};
            if (Chance(random, 60)) return picks[Draw(random, picks.size())];
            // Two's complement: the low `size` bits, sign-extended.
            const std::uint64_t bits = random();
            const unsigned shift = 64U - type.size;
            return static_cast<std::int64_t>(bits << shift) >> shift;
        }
        case ColumnKind::kFloat32: return RandomFloat(random);
        case ColumnKind::kFloat64: return RandomDouble(random);
        case ColumnKind::kBool: return Chance(random, 50);
        case ColumnKind::kText: return RandomText(random, type.size);
        case ColumnKind::kBytes: {
            const std::string bytes = RandomBytes(random, type.size);
            return BytesValue{.bytes = std::vector<std::uint8_t>(bytes.begin(), bytes.end())};
        }
        case ColumnKind::kBits: return BitsValue{.digits = RandomDigits(random, type.size)};
    }
    return std::monostate{};
}

// A parameter value of `type`: integers of the whole 64-bit range of its
// sign, as their frames carry them.
ColumnValue RandomParameter(Random& random, const ColumnType& type) {
    if (Chance(random, 15)) return std::monostate{};
    if (type.kind == ColumnKind::kUnsigned && Chance(random, 30)) return random();
    if (type.kind == ColumnKind::kSigned && Chance(random, 30)) return static_cast<std::int64_t>(random());
    return RandomValue(random, type);
}

const std::array<std::string, 9> kNames{"c0", "c1", "c2", "count", "status", "version", "and", "null", "exists"};

bool Reserved(const std::string& name) { return name == "and" || name == "null" || name == "exists"; }

class Generator {
  public:
    Generator(Random& random, Case* c, bool f32_literals_only) : random_(random), case_(*c), f32_only_(f32_literals_only) {}

    std::string Keyword(const std::string& word) {
        std::string out = word;
        const auto pick = Draw(random_, 4);
        for (char& ch : out) {
            if (pick == 1 || (pick == 2 && Chance(random_, 50))) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        return out;
    }

    // Columns of a kind (and width for bits) in the block (false) or the
    // chunk (true).
    std::vector<std::pair<bool, std::size_t>> Columns(Kind kind, std::uint32_t width = 0) const {
        std::vector<std::pair<bool, std::size_t>> out;
        for (std::size_t i = 0; i < case_.block.size(); ++i) {
            if (KindOf(case_.block[i].type) == kind && (kind != Kind::kBits || case_.block[i].type.size == width)) out.emplace_back(false, i);
        }
        for (std::size_t i = 0; i < case_.chunk.size(); ++i) {
            if (KindOf(case_.chunk[i].type) == kind && (kind != Kind::kBits || case_.chunk[i].type.size == width)) out.emplace_back(true, i);
        }
        return out;
    }

    static Tree ColumnTree(std::pair<bool, std::size_t> column) {
        return Tree{.node = column.first ? Node::kChunk : Node::kColumn, .index = column.second};
    }

    const ColumnType& TypeOfColumn(std::pair<bool, std::size_t> column) const {
        return column.first ? case_.chunk[column.second].type : case_.block[column.second].type;
    }

    Tree Literal(Value value) { return Tree{.node = Node::kLiteral, .literal = std::move(value)}; }

    Tree Null() { return Literal(Value{}); }

    Tree IntLiteral() {
        static const std::array<std::pair<std::uint64_t, bool>, 12> kValues{{
            {0, false}, {1, false}, {1, true}, {2, false}, {7, true}, {255, false}, {256, false},
            {9223372036854775807ULL, false}, {9223372036854775808ULL, true}, {18446744073709551615ULL, false},
            {9223372036854775808ULL, false}, {3, false}}};
        Value value;
        value.kind = Kind::kInt;
        if (Chance(random_, 70)) {
            const auto& [magnitude, negative] = kValues[Draw(random_, kValues.size())];
            value.integer = FromMagnitude(magnitude, negative);
        } else {
            const std::uint64_t magnitude = random() >> Draw(random_, 64);
            value.integer = FromMagnitude(magnitude, Chance(random_, 40));
        }
        return Literal(value);
    }

    std::uint64_t random() { return random_(); }

    Tree FloatLiteral() {
        // Exact in f32, so a literal rounded to f32 keeps its value.
        static const std::array<double, 12> kExact{0.0, -0.0, 0.5, 1.5, -2.25, 3.0, 1e10, 16777216.0, 3.4028234663852886e38,
                                                   std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(),
                                                   std::numeric_limits<double>::quiet_NaN()};
        static const std::array<double, 9> kOther{0.1, 1e300, -1e-300, 9007199254740992.0, 9223372036854775808.0,
                                                  18446744073709551616.0, 1.7014118346046923e38, std::numeric_limits<double>::max(), -0.7};
        Value value;
        value.kind = Kind::kFloat;
        value.number = f32_only_ || Chance(random_, 50) ? kExact[Draw(random_, kExact.size())] : kOther[Draw(random_, kOther.size())];
        return Literal(value);
    }

    Tree TextLiteral(std::size_t max = 4) {
        Value value;
        value.kind = Kind::kText;
        value.bytes = RandomText(random_, max);
        if (Chance(random_, 5)) value.bytes += "\xff";
        return Literal(value);
    }

    Tree BytesLiteral() {
        Value value;
        value.kind = Kind::kBytes;
        value.bytes = RandomBytes(random_, 3);
        return Literal(value);
    }

    Tree BitsLiteral(std::uint32_t width) {
        Value value;
        value.kind = Kind::kBits;
        value.bytes = RandomDigits(random_, width);
        return Literal(value);
    }

    Tree BoolLiteral() {
        Value value;
        value.kind = Kind::kBool;
        value.boolean = Chance(random_, 50);
        return Literal(value);
    }

    Tree Parameter(const ColumnType& type) {
        case_.parameter_types.push_back(type);
        return Tree{.node = Node::kParameter, .index = case_.parameter_types.size()};
    }

    Tree Parenthesize(Tree tree) {
        tree.parenthesized = Chance(random_, 8);
        return tree;
    }

    Tree Integer(std::uint64_t magnitude, bool negative = false) {
        Value value;
        value.kind = Kind::kInt;
        value.integer = FromMagnitude(magnitude, negative);
        return Literal(value);
    }

    Tree Arithmetic(char op, Tree a, Tree b) {
        return Tree{.node = Node::kArithmetic, .op = op, .operands = {std::move(a), std::move(b)}};
    }

    // Values at the edges: the ends of the 128-bit range, an integer that
    // rounds differently to f32 through f64, and zeros of both signs.
    Tree Edge(bool integers) {
        const auto two63 = [this] { return Integer(9223372036854775808ULL); };
        switch (Draw(random_, integers ? 5 : 7)) {
            case 0:  // 2^127 - 1
                return Arithmetic('+', Arithmetic('*', Arithmetic('-', Arithmetic('*', two63(), two63()), Integer(1)), Integer(2)), Integer(1));
            case 1:  // -2^127
                return Arithmetic('*', Arithmetic('*', two63(), two63()), Integer(2, true));
            case 2:  // 2^126
                return Arithmetic('*', two63(), two63());
            case 3:  // 2^64 + 2^40 + 1
                return Arithmetic('+', Integer(18446744073709551615ULL), Integer(1099511627778ULL));
            case 4:  // -2^63
                return Integer(9223372036854775808ULL, true);
            default: {
                Value zero;
                zero.kind = Kind::kFloat;
                Value negative = zero;
                negative.number = -0.0;
                Tree tree{.node = Chance(random_, 50) ? Node::kLeast : Node::kGreatest};
                tree.operands = Chance(random_, 50) ? std::vector<Tree>{Literal(zero), Literal(negative)} : std::vector<Tree>{Literal(negative), Literal(zero)};
                return tree;
            }
        }
    }

    // A number: integers only when `integers`.
    Tree Number(int depth, bool integers) {
        if (depth > 0 && Chance(random_, 55)) {
            const auto pick = Draw(random_, 20);
            if (pick < 2) return Parenthesize(Tree{.node = Node::kNegate, .operands = {Number(depth - 1, integers)}});
            if (pick < 15) {
                static constexpr std::array<char, 5> kOps{'+', '-', '*', '/', '%'};
                return Parenthesize(Tree{.node = Node::kArithmetic, .op = kOps[Draw(random_, kOps.size())],
                                         .operands = {Number(depth - 1, integers), Number(depth - 1, integers)}});
            }
            static constexpr std::array<Node, 3> kFunctions{Node::kLeast, Node::kGreatest, Node::kCoalesce};
            Tree tree{.node = kFunctions[Draw(random_, kFunctions.size())]};
            const auto count = 1U + Draw(random_, 3);
            for (std::uint64_t k = 0; k < count; ++k) tree.operands.push_back(Number(depth - 1, integers));
            return Parenthesize(std::move(tree));
        }
        auto columns = Columns(Kind::kInt);
        if (!integers) {
            const auto floats = Columns(Kind::kFloat);
            columns.insert(columns.end(), floats.begin(), floats.end());
        }
        const auto pick = Draw(random_, 20);
        if (pick < 9 && !columns.empty()) return Parenthesize(ColumnTree(columns[Draw(random_, columns.size())]));
        if (pick < 10) return Tree{.node = Node::kPosX};
        if (pick < 11) return Tree{.node = Chance(random_, 50) ? Node::kPosY : Node::kPosZ};
        if (pick < 12) return Tree{.node = Node::kNow};
        if (pick < 13) return Null();
        if (pick < 16 && !integers) return FloatLiteral();
        if (pick < 18) return Edge(integers);
        return IntLiteral();
    }

    Tree Of(Kind kind, std::uint32_t width, int depth) {
        if (kind == Kind::kInt || kind == Kind::kFloat) return Number(depth, kind == Kind::kInt);
        const auto columns = Columns(kind, width);
        if (depth > 0 && Chance(random_, 15)) {
            Tree tree{.node = Node::kCoalesce};
            const auto count = 1U + Draw(random_, 3);
            for (std::uint64_t k = 0; k < count; ++k) tree.operands.push_back(Of(kind, width, depth - 1));
            return Parenthesize(std::move(tree));
        }
        if (!columns.empty() && Chance(random_, 60)) return Parenthesize(ColumnTree(columns[Draw(random_, columns.size())]));
        if (Chance(random_, 8)) return Null();
        switch (kind) {
            case Kind::kText: return TextLiteral();
            case Kind::kBytes: return BytesLiteral();
            case Kind::kBits: return BitsLiteral(width);
            default: return BoolLiteral();
        }
    }

    // A value of any kind, for comparisons made to be refused sometimes.
    Tree Any(int depth) {
        static constexpr std::array<Kind, 6> kKinds{Kind::kInt, Kind::kFloat, Kind::kBool, Kind::kText, Kind::kBytes, Kind::kBits};
        const Kind kind = kKinds[Draw(random_, kKinds.size())];
        return Of(kind, kind == Kind::kBits ? static_cast<std::uint32_t>(1U + Draw(random_, 6)) : 0U, depth);
    }

    // A literal or parameter for an IN list of a value of `type`.
    Tree Item(Kind kind, std::uint32_t width, const ColumnType* parameter) {
        if (parameter != nullptr && Chance(random_, 30)) return Parameter(*parameter);
        if (Chance(random_, 10)) return Null();
        switch (kind) {
            case Kind::kInt:
            case Kind::kFloat: return Chance(random_, 50) ? IntLiteral() : FloatLiteral();
            case Kind::kText: return TextLiteral();
            case Kind::kBytes: return BytesLiteral();
            case Kind::kBits: return BitsLiteral(width);
            default: return BoolLiteral();
        }
    }

    // A predicate on a column with parameters, whose type is the column's.
    Tree ParameterPredicate() {
        std::vector<std::pair<bool, std::size_t>> all;
        for (std::size_t i = 0; i < case_.block.size(); ++i) all.emplace_back(false, i);
        for (std::size_t i = 0; i < case_.chunk.size(); ++i) all.emplace_back(true, i);
        const auto column = all[Draw(random_, all.size())];
        const ColumnType& type = TypeOfColumn(column);
        const Kind kind = KindOf(type);
        const bool ordered = kind != Kind::kBool && kind != Kind::kBits;
        const auto pick = Draw(random_, 4);
        if (pick == 0) {
            Tree tree{.node = Chance(random_, 50) ? Node::kIn : Node::kNotIn, .operands = {ColumnTree(column)}};
            const auto count = 1U + Draw(random_, 3);
            for (std::uint64_t k = 0; k < count; ++k) tree.operands.push_back(Item(kind, type.size, &type));
            return tree;
        }
        if (pick == 1 && ordered) {
            Tree low = Parameter(type);
            Tree high = Parameter(type);
            return Tree{.node = Chance(random_, 50) ? Node::kBetween : Node::kNotBetween, .operands = {ColumnTree(column), std::move(low), std::move(high)}};
        }
        if (pick == 2 && IsNumeric(kind)) {
            static constexpr std::array<char, 5> kOps{'+', '-', '*', '/', '%'};
            Tree parameter = Parameter(type);
            Tree sum{.node = Node::kArithmetic, .op = kOps[Draw(random_, kOps.size())]};
            if (Chance(random_, 50)) {
                sum.operands = {ColumnTree(column), std::move(parameter)};
            } else {
                sum.operands = {std::move(parameter), ColumnTree(column)};
            }
            return Tree{.node = Node::kCompare, .compare = static_cast<int>(Draw(random_, 6)), .operands = {std::move(sum), Number(1, false)}};
        }
        Tree tree{.node = Node::kCompare, .compare = static_cast<int>(ordered ? Draw(random_, 6) : Draw(random_, 2))};
        Tree parameter = Parameter(type);
        if (Chance(random_, 50)) {
            tree.operands = {ColumnTree(column), std::move(parameter)};
        } else {
            tree.operands = {std::move(parameter), ColumnTree(column)};
        }
        return tree;
    }

    Tree Predicate(int depth, bool refusable) {
        const auto pick = Draw(random_, 100);
        const auto compare = [&](Kind kind, std::uint32_t width, bool ordered) {
            return Tree{.node = Node::kCompare, .compare = static_cast<int>(ordered ? Draw(random_, 6) : Draw(random_, 2)),
                        .operands = {Of(kind, width, depth - 1), Of(kind, width, depth - 1)}};
        };
        if (depth <= 0 || pick < 25) return Parenthesize(compare(Kind::kFloat, 0, true));
        if (pick < 29) return Parenthesize(compare(Kind::kText, 0, true));
        if (pick < 31) return Parenthesize(compare(Kind::kBytes, 0, true));
        if (pick < 33) return Parenthesize(compare(Kind::kBool, 0, false));
        if (pick < 35) {
            const auto width = static_cast<std::uint32_t>(1U + Draw(random_, 6));
            return Parenthesize(compare(Kind::kBits, width, false));
        }
        if (pick < 40) return Parenthesize(Tree{.node = Chance(random_, 50) ? Node::kIsNull : Node::kIsNotNull, .operands = {Any(depth - 1)}});
        if (pick < 46) {
            const bool text = Chance(random_, 25);
            const Kind kind = text ? Kind::kText : Kind::kFloat;
            Tree tree{.node = Chance(random_, 50) ? Node::kIn : Node::kNotIn, .operands = {Of(kind, 0, depth - 1)}};
            const auto count = 1U + Draw(random_, 4);
            for (std::uint64_t k = 0; k < count; ++k) tree.operands.push_back(Item(kind, 0, nullptr));
            return Parenthesize(std::move(tree));
        }
        if (pick < 52) {
            const Kind kind = Chance(random_, 25) ? Kind::kText : Kind::kFloat;
            return Parenthesize(Tree{.node = Chance(random_, 50) ? Node::kBetween : Node::kNotBetween,
                                     .operands = {Of(kind, 0, depth - 1), Of(kind, 0, depth - 1), Of(kind, 0, depth - 1)}});
        }
        if (pick < 55) return Parenthesize(Tree{.node = Node::kExists});
        if (pick < 61) return Parenthesize(Tree{.node = Node::kNot, .operands = {Predicate(depth - 1, refusable)}});
        if (pick < 79) {
            Tree tree{.node = Chance(random_, 50) ? Node::kAnd : Node::kOr};
            const auto count = 2U + Draw(random_, 2);
            for (std::uint64_t k = 0; k < count; ++k) tree.operands.push_back(Predicate(depth - 1, refusable));
            return Parenthesize(std::move(tree));
        }
        if (pick < 85) return Parenthesize(Of(Kind::kBool, 0, depth - 1));
        if (pick < 93) return Parenthesize(ParameterPredicate());
        if (refusable) {
            // Often refused: a comparison, or arithmetic, of any kinds.
            Tree left = Any(depth - 1);
            if (Chance(random_, 30)) {
                left = Tree{.node = Node::kArithmetic, .op = '+', .operands = {std::move(left), Any(depth - 1)}};
            }
            return Tree{.node = Node::kCompare, .compare = static_cast<int>(Draw(random_, 6)), .operands = {std::move(left), Any(depth - 1)}};
        }
        return Parenthesize(compare(Kind::kInt, 0, true));
    }

    // ---- Text.

    std::string Name(const std::string& name) {
        return Reserved(name) || Chance(random_, 15) ? "\"" + name + "\"" : name;
    }

    static int Precedence(const Tree& tree) {
        if (tree.parenthesized) return 9;
        switch (tree.node) {
            case Node::kOr: return 1;
            case Node::kAnd: return 2;
            case Node::kNot: return 3;
            case Node::kCompare: case Node::kIsNull: case Node::kIsNotNull: case Node::kIn: case Node::kNotIn:
            case Node::kBetween: case Node::kNotBetween: case Node::kExists: return 4;
            case Node::kArithmetic: return tree.op == '+' || tree.op == '-' ? 5 : 6;
            case Node::kNegate: return 7;
            default: return 9;
        }
    }

    std::string Wrapped(const Tree& tree, bool parentheses) {
        const std::string text = Text(tree);
        return parentheses ? "(" + text + ")" : text;
    }

    std::string LiteralText(const Value& value) {
        switch (value.kind) {
            case Kind::kNull: return Keyword("NULL");
            case Kind::kInt: return Decimal(value.integer);
            case Kind::kFloat: {
                if (std::isnan(value.number)) return Keyword("NAN");
                if (std::isinf(value.number)) return value.number < 0 ? "-" + Keyword("INF") : Keyword("INF");
                return ::Number(value.number);
            }
            case Kind::kBool: return Keyword(value.boolean ? "TRUE" : "FALSE");
            case Kind::kText: {
                std::string out = "'";
                for (const char c : value.bytes) out += c == '\'' ? std::string("''") : std::string(1, c);
                return out + "'";
            }
            case Kind::kBytes: return (Chance(random_, 50) ? "x'" : "X'") + Hex(value.bytes) + "'";
            case Kind::kBits: return (Chance(random_, 50) ? "b'" : "B'") + value.bytes + "'";
        }
        return "?";
    }

    std::string List(const std::vector<Tree>& operands, std::size_t from, const std::string& separator, int above) {
        std::string out;
        for (std::size_t k = from; k < operands.size(); ++k) {
            out += (k == from ? "" : separator) + Wrapped(operands[k], Precedence(operands[k]) <= above);
        }
        return out;
    }

    std::string Text(const Tree& tree) {
        std::string text = Bare(tree);
        return tree.parenthesized ? "(" + text + ")" : text;
    }

    std::string Bare(const Tree& tree) {
        const auto& ops = tree.operands;
        switch (tree.node) {
            case Node::kLiteral: return LiteralText(tree.literal);
            case Node::kParameter: return "$" + std::to_string(tree.index);
            case Node::kColumn: return Name(case_.block[tree.index].name);
            case Node::kChunk: return Cat({Keyword("CHUNK"), ".", Name(case_.chunk[tree.index].name)});
            case Node::kPosX: return Keyword("POS_X");
            case Node::kPosY: return Keyword("POS_Y");
            case Node::kPosZ: return Keyword("POS_Z");
            case Node::kNow: return Keyword("NOW") + "()";
            case Node::kNegate: return Cat({"-", Chance(random_, 50) ? " " : "", Wrapped(ops[0], Precedence(ops[0]) < 7)});
            case Node::kArithmetic: {
                const int own = Precedence(tree);
                return Cat({Wrapped(ops[0], Precedence(ops[0]) < own), " ", std::string(1, tree.op), " ", Wrapped(ops[1], Precedence(ops[1]) <= own)});
            }
            case Node::kLeast: return Cat({Keyword("LEAST"), "(", List(ops, 0, ", ", 0), ")"});
            case Node::kGreatest: return Cat({Keyword("GREATEST"), "(", List(ops, 0, ", ", 0), ")"});
            case Node::kCoalesce: return Cat({Keyword("COALESCE"), "(", List(ops, 0, ", ", 0), ")"});
            case Node::kCompare: return Cat({Text(ops[0]), " ", kComparisons[static_cast<std::size_t>(tree.compare)], " ", Text(ops[1])});
            case Node::kIsNull: return Cat({Text(ops[0]), " ", Keyword("IS NULL")});
            case Node::kIsNotNull: return Cat({Text(ops[0]), " ", Keyword("IS NOT NULL")});
            case Node::kIn: return Cat({Text(ops[0]), " ", Keyword("IN"), " (", List(ops, 1, ", ", 0), ")"});
            case Node::kNotIn: return Cat({Text(ops[0]), " ", Keyword("NOT IN"), " (", List(ops, 1, ", ", 0), ")"});
            case Node::kBetween:
            case Node::kNotBetween:
                return Cat({Text(ops[0]), " ", Keyword(tree.node == Node::kBetween ? "BETWEEN" : "NOT BETWEEN"), " ", Text(ops[1]), " ",
                            Keyword("AND"), " ", Text(ops[2])});
            case Node::kExists: return Keyword("EXISTS");
            case Node::kNot: return Cat({Keyword("NOT"), " ", Wrapped(ops[0], Precedence(ops[0]) < 3)});
            case Node::kAnd: return List(ops, 0, " " + Keyword("AND") + " ", 2);
            case Node::kOr: return List(ops, 0, " " + Keyword("OR") + " ", 1);
        }
        return "?";
    }

  private:
    Random& random_;
    Case& case_;
    bool f32_only_;
};

Case RandomCase(Random& random) {
    Case c;
    const auto count = 1U + Draw(random, 6);
    std::vector<std::string> names(kNames.begin(), kNames.end());
    std::shuffle(names.begin(), names.end(), random);
    for (std::uint64_t i = 0; i < count; ++i) {
        Column column;
        column.id = static_cast<std::uint32_t>(i + 1U);
        column.name = names[i];
        column.type = RandomType(random);
        column.nullable = Chance(random, 50);
        c.block.push_back(column);
        c.row.push_back(column.nullable && Chance(random, 25) ? ColumnValue{} : RandomValue(random, column.type));
    }
    const auto chunk_count = Draw(random, 3);
    for (std::uint64_t i = 0; i < chunk_count; ++i) {
        Column column;
        column.id = static_cast<std::uint32_t>(100U + i);
        column.name = "k" + std::to_string(i);
        column.type = RandomType(random);
        column.nullable = Chance(random, 50);
        c.chunk.push_back(column);
        c.chunk_row.push_back(column.nullable && Chance(random, 25) ? ColumnValue{} : RandomValue(random, column.type));
    }
    c.exists = !Chance(random, 15);
    const std::array<std::int64_t, 5> positions{0, -1, 1, std::numeric_limits<std::int64_t>::min(), std::numeric_limits<std::int64_t>::max()};
    c.x = Chance(random, 50) ? positions[Draw(random, positions.size())] : static_cast<std::int64_t>(random());
    c.y = Chance(random, 50) ? positions[Draw(random, positions.size())] : static_cast<std::int64_t>(random()) >> 40U;
    c.z = Chance(random, 50) ? positions[Draw(random, positions.size())] : 0;
    const std::array<std::uint64_t, 4> times{0, 1700000000000ULL, std::numeric_limits<std::uint64_t>::max(), 1};
    c.now = Chance(random, 70) ? times[Draw(random, times.size())] : random();
    return c;
}

bool HasF32(const Case& c) {
    for (const auto* columns : {&c.block, &c.chunk}) {
        for (const Column& column : *columns) {
            if (column.type.kind == ColumnKind::kFloat32) return true;
        }
    }
    return false;
}

std::string Describe(const Case& c, const std::string& text) {
    std::string out = "\n  expression: " + text + "\n  exists: " + (c.exists ? "yes" : "no") + "; x, y, z: " + std::to_string(c.x) + ", " +
                      std::to_string(c.y) + ", " + std::to_string(c.z) + "; now: " + std::to_string(c.now);
    for (std::size_t i = 0; i < c.block.size(); ++i) {
        out += "\n  " + c.block[i].name + " " + chunkdb::ColumnTypeName(c.block[i].type) + (c.block[i].nullable ? " NULL" : "") + " = " + ValueText(c.row[i]);
    }
    for (std::size_t i = 0; i < c.chunk.size(); ++i) {
        out += "\n  chunk." + c.chunk[i].name + " " + chunkdb::ColumnTypeName(c.chunk[i].type) + (c.chunk[i].nullable ? " NULL" : "") + " = " + ValueText(c.chunk_row[i]);
    }
    for (std::size_t i = 0; i < c.parameters.size(); ++i) {
        out += "\n  $" + std::to_string(i + 1U) + " " + chunkdb::ColumnTypeName(c.parameter_types[i]) + " = " + ValueText(c.parameters[i]);
    }
    return out;
}

expr::Inputs InputsOf(const Case& c) {
    return expr::Inputs{.exists = c.exists, .block = c.row, .chunk = c.chunk_row, .x = c.x, .y = c.y, .z = c.z, .now = c.now, .parameters = c.parameters};
}

void CheckParameterTypes(const std::vector<expr::ParameterType>& got, const Case& c, const std::string& context) {
    if (got.size() != c.parameter_types.size()) Fail("parameter count differs" + context);
    for (std::size_t i = 0; i < got.size(); ++i) {
        if (got[i].index != i + 1U || !(got[i].type == c.parameter_types[i])) {
            Fail("$" + std::to_string(i + 1U) + " took " + chunkdb::ColumnTypeName(got[i].type) + context);
        }
    }
}

struct Counts {
    std::size_t cases = 0;
    std::size_t truths = 0;
    std::size_t refused = 0;
    std::size_t out_of_range = 0;
    std::size_t invalid = 0;
};

void Tally(Counts* counts, const std::string& outcome) {
    ++counts->cases;
    if (outcome == "INVALID_ARGUMENT") ++counts->invalid;
    else if (outcome == "OUT_OF_RANGE") ++counts->out_of_range;
    else if (outcome == "true") ++counts->truths;
}

void ConditionCase(Random& random, std::size_t number, Counts* counts) {
    Case c = RandomCase(random);
    Generator generator(random, &c, HasF32(c));
    const auto depth = static_cast<int>(1U + Draw(random, 4));
    const Tree tree = generator.Predicate(depth, Chance(random, 10));
    const std::string text = generator.Text(tree);
    for (const ColumnType& type : c.parameter_types) c.parameters.push_back(RandomParameter(random, type));
    const std::string context = " in condition case " + std::to_string(number) + Describe(c, text);

    std::string engine;
    std::string message;
    expr::Condition condition;
    try {
        condition = expr::ParseCondition(text);
    } catch (const std::exception& e) {
        Fail(std::string("the parser refused a generated condition: ") + e.what() + context);
    }
    std::string truth;
    const std::string code = CodeOf([&] {
        const expr::ColumnList schema(c.block, c.chunk);
        const auto plan = expr::PlanCondition(condition.expression, schema);
        CheckParameterTypes(plan.parameters(), c, context);
        truth = expr::EvaluateCondition(plan, InputsOf(c)) ? "true" : "false";
    }, &message);
    engine = code == "OK" ? truth : code;

    std::string reference;
    try {
        const Reference evaluator(c);
        const Type type = evaluator.TypeOf(tree);
        if (type.kind != Kind::kBool && type.kind != Kind::kNull) Refused();
        reference = evaluator.Truth(tree) == 'T' ? "true" : "false";
    } catch (const Error& error) {
        reference = error.code;
    }
    if (engine != reference) Fail("engine " + engine + " (" + message + "), reference " + reference + context);
    Tally(counts, engine);
}

Tree AssignmentValue(Generator& generator, Random& random, const Column& column, Case& c, bool* refusable) {
    const Kind kind = KindOf(column.type);
    if (Chance(random, 10)) return generator.Parameter(column.type);
    if (Chance(random, 5)) {
        *refusable = true;
        return generator.Number(2, false);
    }
    if (kind == Kind::kInt && Chance(random, 10)) {
        // A parameter beside an integer column takes that column's type.
        const auto columns = generator.Columns(Kind::kInt);
        if (!columns.empty()) {
            const auto other = columns[Draw(random, columns.size())];
            Tree parameter = generator.Parameter(generator.TypeOfColumn(other));
            return Tree{.node = Node::kArithmetic, .op = Chance(random, 50) ? '+' : '-', .operands = {Generator::ColumnTree(other), std::move(parameter)}};
        }
    }
    (void)c;
    if (kind == Kind::kText && Chance(random, 20)) return generator.TextLiteral(column.type.size + 1U);
    return generator.Of(kind, column.type.size, static_cast<int>(Draw(random, 4)));
}

void AssignmentCase(Random& random, std::size_t number, Counts* counts) {
    Case c = RandomCase(random);
    Generator generator(random, &c, HasF32(c));
    std::vector<std::size_t> targets(c.block.size());
    for (std::size_t i = 0; i < targets.size(); ++i) targets[i] = i;
    std::shuffle(targets.begin(), targets.end(), random);
    targets.resize(1U + Draw(random, std::min<std::size_t>(3, targets.size())));
    std::vector<Tree> values;
    std::string text;
    for (const std::size_t target : targets) {
        bool refusable = false;
        values.push_back(AssignmentValue(generator, random, c.block[target], c, &refusable));
        text += Cat({text.empty() ? "" : ", ", generator.Name(c.block[target].name), " = ", generator.Text(values.back())});
    }
    for (const ColumnType& type : c.parameter_types) c.parameters.push_back(RandomParameter(random, type));
    const std::string context = " in assignment case " + std::to_string(number) + Describe(c, text);

    expr::Assignments assignments;
    try {
        assignments = expr::ParseAssignments(text);
    } catch (const std::exception& e) {
        Fail(std::string("the parser refused generated assignments: ") + e.what() + context);
    }
    std::vector<ColumnValue> got;
    std::string message;
    const std::string engine = CodeOf([&] {
        const expr::ColumnList schema(c.block, c.chunk);
        const auto plan = expr::PlanAssignments(assignments.values, schema);
        CheckParameterTypes(plan.parameters(), c, context);
        got = expr::EvaluateAssignments(plan, InputsOf(c));
    }, &message);

    std::string reference = "OK";
    std::vector<ColumnValue> want;
    try {
        const Reference evaluator(c);
        // Planning refuses before anything is evaluated.
        for (std::size_t i = 0; i < targets.size(); ++i) {
            const Column& column = c.block[targets[i]];
            const Type type = evaluator.TypeOf(values[i]);
            const Kind kind = KindOf(column.type);
            if (type.kind == Kind::kNull) {
                if (!column.nullable && values[i].node == Node::kLiteral) Refused();
            } else if ((type.kind != kind && !(kind == Kind::kFloat && type.kind == Kind::kInt)) ||
                       (kind == Kind::kBits && type.width != column.type.size)) {
                Refused();
            }
        }
        for (std::size_t i = 0; i < targets.size(); ++i) {
            want.push_back(Reference::Store(c.block[targets[i]], evaluator.Eval(values[i])));
        }
    } catch (const Error& error) {
        reference = error.code;
    }
    if (engine != reference) Fail("engine " + engine + " (" + message + "), reference " + reference + context);
    if (engine == "OK") {
        for (std::size_t i = 0; i < want.size(); ++i) {
            if (!SameValue(got[i], want[i])) {
                Fail("value " + std::to_string(i) + ": engine " + ValueText(got[i]) + ", reference " + ValueText(want[i]) + context);
            }
        }
    }
    Tally(counts, engine == "OK" ? "assigned" : engine);
}

// ---- Random input to the parser.

std::size_t Depth(const expr::Expression& expression, std::uint32_t index) {
    std::size_t depth = 0;
    for (const auto operand : expression.nodes[index].operands) depth = std::max(depth, Depth(expression, operand));
    return depth + 1U;
}

void CheckLimits(const expr::Expression& expression, const std::string& text) {
    if (expression.nodes.empty() || expression.nodes.size() > expr::kDefaultMaxNodes || expression.root != expression.nodes.size() - 1U) {
        Fail("a parsed expression breaks the node limit: " + text);
    }
    for (std::size_t i = 0; i < expression.nodes.size(); ++i) {
        for (const auto operand : expression.nodes[i].operands) {
            if (operand >= i) Fail("an operand after its node: " + text);
        }
    }
    if (Depth(expression, expression.root) > expr::kDefaultMaxDepth) Fail("a parsed expression breaks the depth limit: " + text);
}

// Parses, plans and evaluates `text` as a condition and as assignments; only
// the errors expressions throw may come out.
void Attempt(Random& random, const std::string& text) {
    static const std::vector<Column> block{
        MakeColumn(1, "a", U(8), false), MakeColumn(2, "b", I(64), true), MakeColumn(3, "flag", kBool, true),
        MakeColumn(4, "t", Text(4), false), MakeColumn(5, "version", U(32), false), MakeColumn(6, "exists", kF32, true),
        MakeColumn(7, "bb", Bits(2), false), MakeColumn(8, "by", Bytes(2), true)};
    static const std::vector<Column> chunk{MakeColumn(9, "k", I(16), false)};
    static const expr::ColumnList schema(block, chunk);
    std::vector<ColumnValue> row;
    for (const Column& column : block) row.push_back(column.nullable && Chance(random, 30) ? ColumnValue{} : RandomValue(random, column.type));
    const std::vector<ColumnValue> chunk_row{RandomValue(random, chunk[0].type)};
    const auto run = [&](const std::function<void()>& action) {
        try {
            action();
        } catch (const cql::ParseError&) {
        } catch (const std::invalid_argument&) {
        } catch (const std::out_of_range&) {
        } catch (const std::exception& e) {
            Fail(std::string("unexpected error ") + e.what() + " for: " + text);
        }
    };
    const auto start = std::chrono::steady_clock::now();
    run([&] {
        const auto condition = expr::ParseCondition(text);
        CheckLimits(condition.expression, text);
        const auto plan = expr::PlanCondition(condition.expression, schema);
        std::vector<ColumnValue> parameters;
        for (const auto& parameter : plan.parameters()) parameters.push_back(RandomParameter(random, parameter.type));
        (void)expr::EvaluateCondition(plan, expr::Inputs{.exists = Chance(random, 80), .block = row, .chunk = chunk_row, .x = 1, .y = 2, .z = 3, .now = 4, .parameters = parameters});
    });
    run([&] {
        const auto assignments = expr::ParseAssignments(text);
        for (const auto& value : assignments.values) CheckLimits(value.value, text);
        const auto plan = expr::PlanAssignments(assignments.values, schema);
        std::vector<ColumnValue> parameters;
        for (const auto& parameter : plan.parameters()) parameters.push_back(RandomParameter(random, parameter.type));
        (void)expr::EvaluateAssignments(plan, expr::Inputs{.exists = true, .block = row, .chunk = chunk_row, .parameters = parameters});
    });
    run([&] { (void)expr::IsVersionClause(text); });
    if (std::chrono::steady_clock::now() - start > std::chrono::seconds(1)) Fail("one input took over a second: " + text);
}

void Fuzz(Random& random, std::size_t cases) {
    static const std::vector<std::string> kTokens{
        "a", "b", "flag", "t", "version", "\"exists\"", "\"a\"", "chunk", ".", "k", "pos_x", "POS_Y", "NOW", "(", ")", ",",
        "LEAST", "GREATEST", "COALESCE", "AND", "OR", "NOT", "IS", "NULL", "IN", "BETWEEN", "EXISTS", "TRUE", "FALSE",
        "=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "0", "1", "255", "18446744073709551615",
        "18446744073709551616", "9223372036854775808", "1.5", "1e400", "-1e-400", "inf", "nan", "'x'", "''''", "''",
        "x'00'", "b'01'", "b'1'", "$1", "$2", "$3", "$0", "$65536", "\"\"", "\"A\"", "!", "'open", "\"open", ";",
        "if", "where", "VERSION", "a =", "b =", "bb", "by", "x'0102'"};
    for (std::size_t i = 0; i < cases; ++i) {
        std::string text;
        const auto pick = Draw(random, 10);
        if (pick < 7) {
            const auto length = Draw(random, 40);
            for (std::uint64_t k = 0; k < length; ++k) text += Cat({kTokens[Draw(random, kTokens.size())], Chance(random, 80) ? " " : ""});
        } else if (pick < 9) {
            // Long runs that press on the limits.
            static const std::vector<std::string> kRuns{"(", "NOT ", "-", "a + ", "a * ", "LEAST(", "a = 1 AND ", "a OR ", "1, "};
            const std::string& run = kRuns[Draw(random, kRuns.size())];
            const auto length = Draw(random, 400);
            text = Chance(random, 50) ? "a IN (" : "";
            for (std::uint64_t k = 0; k < length; ++k) text += run;
            text += Chance(random, 50) ? "a" : "1)";
            for (std::uint64_t k = Draw(random, 40); k > 0; --k) text += ")";
        } else {
            text.resize(Draw(random, 120));
            for (char& c : text) c = static_cast<char>(Chance(random, 70) ? 32U + Draw(random, 95) : Draw(random, 256));
        }
        Attempt(random, text);
    }
}

std::size_t FromEnvironment(const char* name, std::size_t fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') return fallback;
    return static_cast<std::size_t>(std::strtoull(value, nullptr, 10));
}

}  // namespace

int Run() {
    const std::size_t seed = FromEnvironment("CHUNKDB_EXPR_SEED", 20261012);
    const std::size_t cases = FromEnvironment("CHUNKDB_EXPR_CASES", 20000);
    const std::size_t fuzz = FromEnvironment("CHUNKDB_EXPR_FUZZ", 20000);
    Random random(seed);
    Counts conditions;
    Counts assignments;
    for (std::size_t i = 0; i < cases; ++i) {
        ConditionCase(random, i, &conditions);
        AssignmentCase(random, i, &assignments);
    }
    std::printf("expr property test (seed %zu): %zu conditions (%zu true, %zu INVALID_ARGUMENT, %zu OUT_OF_RANGE), "
                "%zu assignment lists (%zu INVALID_ARGUMENT, %zu OUT_OF_RANGE)\n",
                seed, conditions.cases, conditions.truths, conditions.invalid, conditions.out_of_range, assignments.cases,
                assignments.invalid, assignments.out_of_range);
    Fuzz(random, fuzz);
    std::printf("expr fuzz test: %zu random inputs\n", fuzz);
    return 0;
}

}  // namespace property
