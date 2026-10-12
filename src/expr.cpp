#include "expr.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <deque>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

#include "cql_lexer.hpp"
#include "expr_program.hpp"

namespace chunkdb::expr {

namespace {

using cql::ParseError;
using namespace cql::lexer;
using detail::Category;
using detail::Program;
using detail::Step;
using detail::Value;

// Words an expression gives a meaning, so a column of that name is written
// "name". IF and WHERE end an expression inside a statement.
constexpr std::array<std::string_view, 22> kKeywords{
    "and", "or", "not", "is", "null", "in", "between", "exists", "true", "false", "inf",
    "nan", "now", "least", "greatest", "coalesce", "pos_x", "pos_y", "pos_z", "chunk", "if", "where"};

[[nodiscard]] bool IsKeyword(std::string_view lower) {
    return std::find(kKeywords.begin(), kKeywords.end(), lower) != kKeywords.end();
}

[[nodiscard]] bool IsColumnName(std::string_view name) {
    const auto start = [](char c) { return (c >= 'a' && c <= 'z') || c == '_'; };
    return !name.empty() && name.size() <= kMaxColumnNameBytes && start(name.front()) &&
           std::all_of(name.begin(), name.end(), [&](char c) { return start(c) || (c >= '0' && c <= '9'); });
}

class Parser {
  public:
    Parser(std::string_view line, std::size_t position, std::set<std::size_t>* parameters, const Limits& limits)
        : line_(line), at_(position), parameters_(parameters), limits_(limits) {}

    [[nodiscard]] Expression ConditionExpression() {
        Begin();
        const std::uint32_t root = Or();
        return Finish(root);
    }

    [[nodiscard]] std::vector<AssignmentExpression> AssignmentList() {
        std::vector<AssignmentExpression> values;
        do {
            AssignmentExpression assignment;
            assignment.at = Peek().column;
            assignment.column = ColumnName("a column name");
            ExpectToken(TokenKind::kEquals, "=");
            Begin();
            const std::uint32_t root = Sum();
            assignment.value = Finish(root);
            values.push_back(std::move(assignment));
        } while (AcceptToken(TokenKind::kComma));
        return values;
    }

    // Where the first token not read starts.
    [[nodiscard]] std::size_t Position() {
        return Peek().column - 1U;
    }

    void ExpectEnd() {
        if (Peek().kind != TokenKind::kEnd) {
            Fail(Peek().column, "expected the end of the expression, got " + Quote(Peek()));
        }
    }

  private:
    // Counts a level of parentheses while it is open.
    class Nested {
      public:
        Nested(Parser& parser, std::size_t column) : parser_(parser) {
            if (parser_.nesting_ >= parser_.limits_.max_depth) {
                Fail(column, "parentheses are nested too deeply: at most " + std::to_string(parser_.limits_.max_depth) + " levels");
            }
            ++parser_.nesting_;
        }
        Nested(const Nested&) = delete;
        Nested& operator=(const Nested&) = delete;
        ~Nested() { --parser_.nesting_; }

      private:
        Parser& parser_;
    };

    [[nodiscard]] const Token& Peek() {
        if (next_ == tokens_.size()) {
            tokens_.push_back(Lex(line_, &at_, LexMode::kExpression));
        }
        return tokens_[next_];
    }
    const Token& Take() {
        const Token& token = Peek();
        if (token.kind != TokenKind::kEnd) {
            ++next_;
        }
        return token;
    }
    [[nodiscard]] static bool IsWord(const Token& token, std::string_view keyword) {
        return token.kind == TokenKind::kWord && token.text.size() == keyword.size() && Lower(token.text) == keyword;
    }
    bool AcceptWord(std::string_view keyword) {
        if (IsWord(Peek(), keyword)) {
            Take();
            return true;
        }
        return false;
    }
    void ExpectWord(std::string_view keyword) {
        if (!AcceptWord(keyword)) {
            std::string upper(keyword);
            std::transform(upper.begin(), upper.end(), upper.begin(), [](char c) {
                return (c >= 'a' && c <= 'z') ? static_cast<char>(c - 'a' + 'A') : c;
            });
            Fail(Peek().column, "expected " + upper + ", got " + Quote(Peek()));
        }
    }
    bool AcceptToken(TokenKind kind) {
        if (Peek().kind == kind) {
            Take();
            return true;
        }
        return false;
    }
    void ExpectToken(TokenKind kind, const char* what) {
        if (Peek().kind != kind) {
            Fail(Peek().column, std::string("expected ") + what + ", got " + Quote(Peek()));
        }
        Take();
    }

    void Begin() {
        current_ = Expression{};
        depths_.clear();
    }
    [[nodiscard]] Expression Finish(std::uint32_t root) {
        current_.root = root;
        return std::move(current_);
    }

    std::uint32_t Add(
        Op op,
        std::size_t column,
        std::vector<std::uint32_t> operands = {},
        cql::Literal literal = {},
        std::string name = {}) {
        if (current_.nodes.size() >= limits_.max_nodes) {
            Fail(column, "the expression is too long: at most " + std::to_string(limits_.max_nodes) + " nodes");
        }
        std::size_t depth = 1;
        for (const std::uint32_t operand : operands) {
            depth = std::max(depth, depths_[operand] + 1U);
        }
        if (depth > limits_.max_depth) {
            Fail(column, "the expression is nested too deeply: at most " + std::to_string(limits_.max_depth) + " levels");
        }
        current_.nodes.push_back(Node{
            .op = op,
            .column = column,
            .literal = std::move(literal),
            .name = std::move(name),
            .operands = std::move(operands),
        });
        depths_.push_back(depth);
        return static_cast<std::uint32_t>(current_.nodes.size() - 1U);
    }

    void TooDeep(std::size_t column) const {
        Fail(column, "the expression is nested too deeply: at most " + std::to_string(limits_.max_depth) + " levels");
    }

    // A column name: lowercase and not a keyword, or "quoted".
    [[nodiscard]] std::string ColumnName(const char* what) {
        const Token& token = Peek();
        if (token.kind == TokenKind::kQuotedName) {
            if (!IsColumnName(token.text)) {
                Fail(token.column, "a quoted name must match [a-z_][a-z0-9_]* and have 1 to 63 bytes, got " + Quote(token));
            }
            return Take().text;
        }
        if (token.kind != TokenKind::kWord) {
            Fail(token.column, std::string("expected ") + what + ", got " + Quote(token));
        }
        const std::string lower = Lower(token.text);
        if (IsKeyword(lower)) {
            Fail(token.column, Quote(token) + " is a keyword; write \"" + lower + "\" for a column of that name");
        }
        if (token.text != lower) {
            Fail(token.column, "names are lowercase: " + Quote(token));
        }
        if (token.text.size() > kMaxColumnNameBytes) {
            Fail(token.column, "a column name has at most " + std::to_string(kMaxColumnNameBytes) + " bytes");
        }
        return Take().text;
    }

    std::uint32_t Or() {
        const std::uint32_t first = And();
        if (!IsWord(Peek(), "or")) {
            return first;
        }
        const std::size_t column = Peek().column;
        std::vector<std::uint32_t> operands{first};
        while (AcceptWord("or")) {
            operands.push_back(And());
        }
        return Add(Op::kOr, column, std::move(operands));
    }

    std::uint32_t And() {
        const std::uint32_t first = NotExpression();
        if (!IsWord(Peek(), "and")) {
            return first;
        }
        const std::size_t column = Peek().column;
        std::vector<std::uint32_t> operands{first};
        while (AcceptWord("and")) {
            operands.push_back(NotExpression());
        }
        return Add(Op::kAnd, column, std::move(operands));
    }

    std::uint32_t NotExpression() {
        std::vector<std::size_t> nots;
        while (IsWord(Peek(), "not")) {
            if (nots.size() >= limits_.max_depth) {
                TooDeep(Peek().column);
            }
            nots.push_back(Take().column);
        }
        std::uint32_t node = Predicate();
        for (auto it = nots.rbegin(); it != nots.rend(); ++it) {
            node = Add(Op::kNot, *it, {node});
        }
        return node;
    }

    std::uint32_t Predicate() {
        const Token& token = Peek();
        if (token.kind == TokenKind::kOpen) {
            // '(' expr ')' or a sum that starts with '(' sum ')': a value in
            // parentheses may go on as a sum, then as a predicate.
            const std::size_t column = Take().column;
            std::uint32_t inner = 0;
            {
                const Nested nested(*this, column);
                inner = Or();
                ExpectToken(TokenKind::kClose, ")");
            }
            if (!IsValue(current_.nodes[inner].op)) {
                return inner;
            }
            return PredicateAfter(SumFrom(inner));
        }
        if (IsWord(token, "exists")) {
            return Add(Op::kExists, Take().column);
        }
        return PredicateAfter(Sum());
    }

    [[nodiscard]] static std::optional<Op> Comparison(const Token& token) {
        switch (token.kind) {
            case TokenKind::kEquals:
                return Op::kEqual;
            case TokenKind::kNotEqual:
                return Op::kNotEqual;
            case TokenKind::kLess:
                return Op::kLess;
            case TokenKind::kLessEqual:
                return Op::kLessEqual;
            case TokenKind::kGreater:
                return Op::kGreater;
            case TokenKind::kGreaterEqual:
                return Op::kGreaterEqual;
            default:
                return std::nullopt;
        }
    }

    // What follows the sum `value` in a predicate; nothing makes it a bool
    // operand.
    std::uint32_t PredicateAfter(std::uint32_t value) {
        const Token& token = Peek();
        const std::size_t column = token.column;
        if (const auto op = Comparison(token)) {
            Take();
            const std::uint32_t right = Sum();
            return Add(*op, column, {value, right});
        }
        if (IsWord(token, "is")) {
            Take();
            const bool negated = AcceptWord("not");
            ExpectWord("null");
            return Add(negated ? Op::kIsNotNull : Op::kIsNull, column, {value});
        }
        bool negated = false;
        if (IsWord(token, "not")) {
            Take();
            negated = true;
            if (!IsWord(Peek(), "in") && !IsWord(Peek(), "between")) {
                Fail(Peek().column, "expected IN or BETWEEN after NOT, got " + Quote(Peek()));
            }
        }
        if (AcceptWord("in")) {
            ExpectToken(TokenKind::kOpen, "(");
            std::vector<std::uint32_t> operands{value};
            do {
                operands.push_back(ListItem());
            } while (AcceptToken(TokenKind::kComma));
            ExpectToken(TokenKind::kClose, ")");
            return Add(negated ? Op::kNotIn : Op::kIn, column, std::move(operands));
        }
        if (AcceptWord("between")) {
            const std::uint32_t low = Sum();
            ExpectWord("and");
            const std::uint32_t high = Sum();
            return Add(negated ? Op::kNotBetween : Op::kBetween, column, {value, low, high});
        }
        return value;
    }

    // An item of an IN list: a value, a negative number, or a parameter.
    std::uint32_t ListItem() {
        const Token& token = Peek();
        const std::size_t column = token.column;
        if (token.kind == TokenKind::kParameter) {
            return ParameterNode();
        }
        if (token.kind == TokenKind::kMinus) {
            Take();
            if (const auto number = Number(true, column)) {
                return *number;
            }
            Fail(Peek().column, "expected a number after -, got " + Quote(Peek()));
        }
        if (const auto literal = Literal(column)) {
            return *literal;
        }
        Fail(column, "expected a value or a parameter, got " + Quote(token));
    }

    // A number literal, negated if `negate`; nothing for another token.
    std::optional<std::uint32_t> Number(bool negate, std::size_t column) {
        const Token& token = Peek();
        if (token.kind == TokenKind::kInteger) {
            cql::Integer value = IntegerOf(token);
            Take();
            value.negative = negate && value.magnitude != 0U;
            return Add(Op::kLiteral, column, {}, value);
        }
        double number = 0.0;
        if (token.kind == TokenKind::kFloat) {
            number = FloatOf(token);
        } else if (IsWord(token, "inf")) {
            number = std::numeric_limits<double>::infinity();
        } else if (IsWord(token, "nan")) {
            number = std::numeric_limits<double>::quiet_NaN();
        } else {
            return std::nullopt;
        }
        Take();
        return Add(Op::kLiteral, column, {}, negate ? -number : number);
    }

    // A literal value; nothing for another token.
    std::optional<std::uint32_t> Literal(std::size_t column) {
        if (const auto number = Number(false, column)) {
            return number;
        }
        const Token& token = Peek();
        switch (token.kind) {
            case TokenKind::kText:
                return Add(Op::kLiteral, column, {}, cql::Text{.value = Take().text});
            case TokenKind::kBytes:
                return Add(Op::kLiteral, column, {}, BytesOf(Take()));
            case TokenKind::kBits:
                return Add(Op::kLiteral, column, {}, cql::Bits{.digits = Take().text});
            default:
                break;
        }
        if (IsWord(token, "null")) {
            Take();
            return Add(Op::kLiteral, column, {}, cql::Null{});
        }
        if (IsWord(token, "true") || IsWord(token, "false")) {
            const bool value = IsWord(token, "true");
            Take();
            return Add(Op::kLiteral, column, {}, value);
        }
        return std::nullopt;
    }

    std::uint32_t ParameterNode() {
        const Token& token = Peek();
        const std::size_t column = token.column;
        const std::size_t index = ParameterIndexOf(token);
        Take();
        UseParameter(parameters_, index, column);
        return Add(Op::kParameter, column, {}, cql::Parameter{.index = index});
    }

    std::uint32_t Sum() {
        return SumFrom(Factor());
    }

    // The sum whose first factor is `first`.
    std::uint32_t SumFrom(std::uint32_t first) {
        std::uint32_t left = TermFrom(first);
        while (true) {
            const TokenKind kind = Peek().kind;
            if (kind != TokenKind::kPlus && kind != TokenKind::kMinus) {
                return left;
            }
            const std::size_t column = Take().column;
            const std::uint32_t right = TermFrom(Factor());
            left = Add(kind == TokenKind::kPlus ? Op::kAdd : Op::kSubtract, column, {left, right});
        }
    }

    std::uint32_t TermFrom(std::uint32_t first) {
        std::uint32_t left = first;
        while (true) {
            const TokenKind kind = Peek().kind;
            Op op = Op::kMultiply;
            if (kind == TokenKind::kSlash) {
                op = Op::kDivide;
            } else if (kind == TokenKind::kPercent) {
                op = Op::kModulo;
            } else if (kind != TokenKind::kStar) {
                return left;
            }
            const std::size_t column = Take().column;
            const std::uint32_t right = Factor();
            left = Add(op, column, {left, right});
        }
    }

    std::uint32_t Factor() {
        std::vector<std::size_t> minuses;
        while (Peek().kind == TokenKind::kMinus) {
            if (minuses.size() >= limits_.max_depth) {
                TooDeep(Peek().column);
            }
            minuses.push_back(Take().column);
        }
        // The innermost - of a number makes a negative literal.
        std::size_t wrap = minuses.size();
        std::optional<std::uint32_t> node;
        if (!minuses.empty()) {
            node = Number(true, minuses.back());
            if (node) {
                --wrap;
            }
        }
        if (!node) {
            node = Primary();
        }
        for (std::size_t i = wrap; i-- > 0;) {
            node = Add(Op::kNegate, minuses[i], {*node});
        }
        return *node;
    }

    std::uint32_t Function(Op op) {
        const std::size_t column = Take().column;
        ExpectToken(TokenKind::kOpen, "(");
        const Nested nested(*this, column);
        std::vector<std::uint32_t> operands;
        do {
            operands.push_back(Sum());
        } while (AcceptToken(TokenKind::kComma));
        ExpectToken(TokenKind::kClose, ")");
        return Add(op, column, std::move(operands));
    }

    std::uint32_t Primary() {
        const Token& token = Peek();
        const std::size_t column = token.column;
        if (const auto literal = Literal(column)) {
            return *literal;
        }
        switch (token.kind) {
            case TokenKind::kParameter:
                return ParameterNode();
            case TokenKind::kQuotedName:
                return Add(Op::kColumn, column, {}, {}, ColumnName("a column name"));
            case TokenKind::kOpen: {
                Take();
                const Nested nested(*this, column);
                const std::uint32_t inner = Sum();
                ExpectToken(TokenKind::kClose, ")");
                return inner;
            }
            case TokenKind::kWord:
                break;
            default:
                Fail(column, "expected a value, got " + Quote(token));
        }
        const std::string word = Lower(token.text);
        if (word == "pos_x" || word == "pos_y" || word == "pos_z") {
            Take();
            return Add(word == "pos_x" ? Op::kPosX : word == "pos_y" ? Op::kPosY : Op::kPosZ, column);
        }
        if (word == "now") {
            Take();
            ExpectToken(TokenKind::kOpen, "(");
            ExpectToken(TokenKind::kClose, ")");
            return Add(Op::kNow, column);
        }
        if (word == "least") {
            return Function(Op::kLeast);
        }
        if (word == "greatest") {
            return Function(Op::kGreatest);
        }
        if (word == "coalesce") {
            return Function(Op::kCoalesce);
        }
        if (word == "chunk") {
            Take();
            ExpectToken(TokenKind::kDot, ". after CHUNK");
            return Add(Op::kChunkColumn, column, {}, {}, ColumnName("a chunk column name"));
        }
        if (IsKeyword(word)) {
            Fail(column, "expected a value, got " + Quote(token) + "; write \"" + word + "\" for a column of that name");
        }
        return Add(Op::kColumn, column, {}, {}, ColumnName("a column name"));
    }

    std::string_view line_;
    std::size_t at_ = 0;
    std::deque<Token> tokens_;
    std::size_t next_ = 0;
    std::set<std::size_t>* parameters_;
    Limits limits_;
    Expression current_;
    std::vector<std::size_t> depths_;
    std::size_t nesting_ = 0;
};

// ---------------------------------------------------------------------------
// Planning.

constexpr ColumnType kI64{.kind = ColumnKind::kSigned, .size = 64};
constexpr ColumnType kU64{.kind = ColumnKind::kUnsigned, .size = 64};
constexpr ColumnType kF32{.kind = ColumnKind::kFloat32, .size = 32};
constexpr ColumnType kF64{.kind = ColumnKind::kFloat64, .size = 64};

// The static type of a value. `typed` when it comes from a column,
// chunk column, position or NOW() (else from literals, parameters and NULL
// alone). `repr` is the column type a parameter takes from it.
struct Type {
    Category category = Category::kNull;
    bool typed = false;
    ColumnType repr{};
};

[[nodiscard]] bool IsOpen(Category category) noexcept {
    return category == Category::kUnknown || category == Category::kNull;
}

[[nodiscard]] bool IsNumber(Category category) noexcept {
    return category == Category::kInt || category == Category::kFloat;
}

[[nodiscard]] Category CategoryOf(ColumnKind kind) noexcept {
    switch (kind) {
        case ColumnKind::kUnsigned:
        case ColumnKind::kSigned:
            return Category::kInt;
        case ColumnKind::kFloat32:
        case ColumnKind::kFloat64:
            return Category::kFloat;
        case ColumnKind::kBool:
            return Category::kBool;
        case ColumnKind::kText:
            return Category::kText;
        case ColumnKind::kBytes:
            return Category::kBytes;
        case ColumnKind::kBits:
            return Category::kBits;
    }
    return Category::kNull;
}

[[nodiscard]] Type TypeOf(const ColumnType& type) {
    return Type{.category = CategoryOf(type.kind), .typed = true, .repr = type};
}

[[nodiscard]] Type LiteralType(const cql::Literal& literal) {
    return std::visit(
        [](const auto& value) -> Type {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, cql::Integer>) {
                return Type{.category = Category::kInt, .typed = false, .repr = kI64};
            } else if constexpr (std::is_same_v<T, double>) {
                return Type{.category = Category::kFloat, .typed = false, .repr = kF64};
            } else if constexpr (std::is_same_v<T, bool>) {
                return Type{.category = Category::kBool, .typed = false, .repr = {.kind = ColumnKind::kBool, .size = 1}};
            } else if constexpr (std::is_same_v<T, cql::Text>) {
                return Type{.category = Category::kText, .typed = false, .repr = {.kind = ColumnKind::kText, .size = 0}};
            } else if constexpr (std::is_same_v<T, cql::Bytes>) {
                return Type{.category = Category::kBytes, .typed = false, .repr = {.kind = ColumnKind::kBytes, .size = 0}};
            } else if constexpr (std::is_same_v<T, cql::Bits>) {
                return Type{
                    .category = Category::kBits,
                    .typed = false,
                    .repr = {.kind = ColumnKind::kBits, .size = static_cast<std::uint32_t>(value.digits.size())},
                };
            } else if constexpr (std::is_same_v<T, cql::Parameter>) {
                return Type{.category = Category::kUnknown};
            } else {
                return Type{.category = Category::kNull};
            }
        },
        literal);
}

[[nodiscard]] std::string Describe(const Type& type) {
    switch (type.category) {
        case Category::kUnknown:
            return "a parameter";
        case Category::kNull:
            return "NULL";
        case Category::kInt:
            return type.typed ? ColumnTypeName(type.repr) : "an integer";
        case Category::kFloat:
            return type.typed ? ColumnTypeName(type.repr) : "a float";
        case Category::kBool:
            return "bool";
        case Category::kText:
            return "text";
        case Category::kBytes:
            return "bytes";
        case Category::kBits:
            return "bits(" + std::to_string(type.repr.size) + ")";
    }
    return "a value";
}

[[nodiscard]] std::string OpName(Op op) {
    switch (op) {
        case Op::kNegate:
            return "-";
        case Op::kAdd:
            return "+";
        case Op::kSubtract:
            return "-";
        case Op::kMultiply:
            return "*";
        case Op::kDivide:
            return "/";
        case Op::kModulo:
            return "%";
        case Op::kLeast:
            return "LEAST";
        case Op::kGreatest:
            return "GREATEST";
        case Op::kCoalesce:
            return "COALESCE";
        case Op::kIn:
        case Op::kNotIn:
            return "IN";
        case Op::kBetween:
        case Op::kNotBetween:
            return "BETWEEN";
        default:
            return "the operator";
    }
}

[[noreturn]] void TypeError(const Node& node, const std::string& message) {
    throw std::invalid_argument(message + " (at column " + std::to_string(node.column) + ")");
}

[[noreturn]] void Malformed(const std::string& message) {
    throw std::invalid_argument("malformed expression: " + message);
}

// The type of `a` combined with `b` by arithmetic: a float when either is
// one, else an integer. A parameter combined with an integer column takes
// that column's type; with integers of two types, i64 if either is signed,
// else u64. Floats are f32 only when every typed operand is f32.
[[nodiscard]] Type MergeNumbers(const Type& a, const Type& b) {
    if (IsOpen(a.category) && IsOpen(b.category)) {
        const bool unknown = a.category == Category::kUnknown || b.category == Category::kUnknown;
        return Type{.category = unknown ? Category::kUnknown : Category::kNull};
    }
    if (IsOpen(a.category)) {
        return b;
    }
    if (IsOpen(b.category)) {
        return a;
    }
    Type out;
    out.typed = a.typed || b.typed;
    if (a.category == Category::kFloat || b.category == Category::kFloat) {
        out.category = Category::kFloat;
        const auto f32 = [](const Type& t) {
            return t.category == Category::kFloat && (!t.typed || t.repr.kind == ColumnKind::kFloat32);
        };
        out.repr = out.typed && f32(a) && f32(b) ? kF32 : kF64;
        return out;
    }
    out.category = Category::kInt;
    if (a.typed && b.typed) {
        out.repr = a.repr == b.repr ? a.repr
                 : (a.repr.kind == ColumnKind::kSigned || b.repr.kind == ColumnKind::kSigned) ? kI64
                                                                                              : kU64;
    } else {
        out.repr = a.typed ? a.repr : b.typed ? b.repr : kI64;
    }
    return out;
}

// The one type of COALESCE's arguments.
[[nodiscard]] Type MergeAny(const Type& a, const Type& b, const Node& node) {
    if (IsOpen(a.category) || IsOpen(b.category) || (IsNumber(a.category) && IsNumber(b.category))) {
        if (IsOpen(a.category) && !IsOpen(b.category)) {
            return b;
        }
        if (IsOpen(b.category) && !IsOpen(a.category)) {
            return a;
        }
        return MergeNumbers(a, b);
    }
    if (a.category != b.category || (a.category == Category::kBits && a.repr.size != b.repr.size)) {
        TypeError(node, OpName(node.op) + " takes arguments of one type, got " + Describe(a) + " and " + Describe(b));
    }
    return a.typed ? a : b;
}

void CheckComparable(const Type& left, const Type& right, Op op, const Node& node) {
    const Type* known = !IsOpen(left.category) ? &left : !IsOpen(right.category) ? &right : nullptr;
    if (known == nullptr) {
        return;
    }
    if (!IsOpen(left.category) && !IsOpen(right.category)) {
        const bool numbers = IsNumber(left.category) && IsNumber(right.category);
        const bool same = left.category == right.category &&
                          (left.category != Category::kBits || left.repr.size == right.repr.size);
        if (!numbers && !same) {
            TypeError(node, "cannot compare " + Describe(left) + " with " + Describe(right));
        }
    }
    const bool equality = op == Op::kEqual || op == Op::kNotEqual;
    if ((known->category == Category::kBool || known->category == Category::kBits) && !equality) {
        TypeError(node, Describe(*known) + " supports only = and <>");
    }
}

[[nodiscard]] std::size_t OperandCount(Op op, std::size_t given) {
    switch (op) {
        case Op::kLiteral:
        case Op::kParameter:
        case Op::kColumn:
        case Op::kChunkColumn:
        case Op::kPosX:
        case Op::kPosY:
        case Op::kPosZ:
        case Op::kNow:
        case Op::kExists:
            return 0;
        case Op::kNegate:
        case Op::kIsNull:
        case Op::kIsNotNull:
        case Op::kNot:
            return 1;
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
            return 2;
        case Op::kBetween:
        case Op::kNotBetween:
            return 3;
        case Op::kLeast:
        case Op::kGreatest:
        case Op::kCoalesce:
            return given >= 1U ? given : 1U;
        case Op::kIn:
        case Op::kNotIn:
        case Op::kAnd:
        case Op::kOr:
            return given >= 2U ? given : 2U;
    }
    return 0;
}

// Plans one expression into `program`.
class Planner {
  public:
    Planner(const Expression& expression, const Schema& schema, const Limits& limits, Program* program)
        : expression_(expression), schema_(schema), program_(program) {
        const auto& nodes = expression_.nodes;
        if (nodes.empty() || expression_.root >= nodes.size()) {
            Malformed("no root");
        }
        if (nodes.size() > limits.max_nodes) {
            throw ParseError("the expression is too long: at most " + std::to_string(limits.max_nodes) + " nodes");
        }
        std::vector<std::size_t> depths(nodes.size(), 1);
        // Each node is an operand once at most, so evaluation visits it once.
        std::vector<bool> used(nodes.size(), false);
        for (std::size_t i = 0; i < nodes.size(); ++i) {
            const Node& node = nodes[i];
            if (node.operands.size() != OperandCount(node.op, node.operands.size())) {
                Malformed("wrong number of operands");
            }
            const bool parameter = std::holds_alternative<cql::Parameter>(node.literal);
            if ((node.op == Op::kParameter) != parameter) {
                Malformed("a parameter must be a kParameter node");
            }
            for (std::size_t k = 0; k < node.operands.size(); ++k) {
                const std::uint32_t operand = node.operands[k];
                if (operand >= i) {
                    Malformed("an operand must come before its node");
                }
                if (used[operand]) {
                    Malformed("a node is an operand twice");
                }
                used[operand] = true;
                // Logic takes predicates or bool values; everything else
                // takes values.
                const bool logic = node.op == Op::kNot || node.op == Op::kAnd || node.op == Op::kOr;
                if (!logic && !IsValue(nodes[operand].op)) {
                    Malformed("a predicate cannot be an operand of a value");
                }
                depths[i] = std::max(depths[i], depths[operand] + 1U);
            }
            if (depths[i] > limits.max_depth) {
                throw ParseError("the expression is nested too deeply: at most " + std::to_string(limits.max_depth) + " levels");
            }
        }
        types_.resize(nodes.size());
        bindings_.resize(nodes.size());
        declared_.resize(nodes.size());
        to_f32_.assign(nodes.size(), false);
    }

    // Plans a condition, or an assignment's value for a column of `target`.
    // Returns the root's index in Program::steps.
    std::uint32_t Plan(const std::optional<Type>& target) {
        const auto& nodes = expression_.nodes;
        const bool condition = !target.has_value();
        if (!condition && !IsValue(nodes[expression_.root].op)) {
            Malformed("an assignment takes a value");
        }
        Infer(false);
        if (condition) {
            CheckTruth(expression_.root);
        }
        Bind(target);
        Infer(true);
        return Compile();
    }

    [[nodiscard]] const Type& RootType() const {
        return types_[expression_.root];
    }

  private:
    void CheckTruth(std::uint32_t index) {
        const Node& node = expression_.nodes[index];
        if (!IsValue(node.op)) {
            return;
        }
        const Category category = types_[index].category;
        if (category != Category::kBool && !IsOpen(category)) {
            TypeError(node, "a condition must be bool, got " + Describe(types_[index]));
        }
    }

    void RequireNumber(std::uint32_t operand, const Node& node) {
        const Type& type = types_[operand];
        if (!IsNumber(type.category) && !IsOpen(type.category)) {
            TypeError(node, OpName(node.op) + " takes numbers, got " + Describe(type));
        }
    }

    // Types every node, operands first. Before binding, parameters have no
    // type; after it (`bound`), each has the type it took.
    void Infer(bool bound) {
        const auto& nodes = expression_.nodes;
        for (std::uint32_t i = 0; i < nodes.size(); ++i) {
            const Node& node = nodes[i];
            const auto& ops = node.operands;
            Type& type = types_[i];
            switch (node.op) {
                case Op::kLiteral:
                    type = LiteralType(node.literal);
                    break;
                case Op::kParameter:
                    type = bound ? TypeOf(*declared_[i]) : Type{.category = Category::kUnknown};
                    break;
                case Op::kColumn:
                case Op::kChunkColumn: {
                    if (!bindings_[i]) {
                        const bool chunk = node.op == Op::kChunkColumn;
                        bindings_[i] = chunk ? schema_.ChunkColumn(node.name) : schema_.BlockColumn(node.name);
                        if (!bindings_[i]) {
                            throw std::invalid_argument(
                                std::string(chunk ? "the table has no chunk column " : "the table has no column ") + node.name);
                        }
                    }
                    type = TypeOf(bindings_[i]->type);
                    break;
                }
                case Op::kPosX:
                case Op::kPosY:
                case Op::kPosZ:
                    type = TypeOf(kI64);
                    break;
                case Op::kNow:
                    type = TypeOf(kU64);
                    break;
                case Op::kNegate:
                    RequireNumber(ops[0], node);
                    type = types_[ops[0]];
                    if (type.category == Category::kInt && type.typed && type.repr.kind == ColumnKind::kUnsigned) {
                        type.repr = kI64;
                    }
                    break;
                case Op::kAdd:
                case Op::kSubtract:
                case Op::kMultiply:
                case Op::kDivide:
                case Op::kModulo:
                    RequireNumber(ops[0], node);
                    RequireNumber(ops[1], node);
                    type = MergeNumbers(types_[ops[0]], types_[ops[1]]);
                    break;
                case Op::kLeast:
                case Op::kGreatest:
                    for (std::size_t k = 0; k < ops.size(); ++k) {
                        RequireNumber(ops[k], node);
                        type = k == 0 ? types_[ops[k]] : MergeNumbers(type, types_[ops[k]]);
                    }
                    break;
                case Op::kCoalesce:
                    type = types_[ops[0]];
                    for (std::size_t k = 1; k < ops.size(); ++k) {
                        type = MergeAny(type, types_[ops[k]], node);
                    }
                    break;
                case Op::kEqual:
                case Op::kNotEqual:
                case Op::kLess:
                case Op::kLessEqual:
                case Op::kGreater:
                case Op::kGreaterEqual:
                    CheckComparable(types_[ops[0]], types_[ops[1]], node.op, node);
                    type = Type{.category = Category::kBool};
                    break;
                case Op::kIn:
                case Op::kNotIn:
                    for (std::size_t k = 1; k < ops.size(); ++k) {
                        CheckComparable(types_[ops[0]], types_[ops[k]], Op::kEqual, node);
                    }
                    type = Type{.category = Category::kBool};
                    break;
                case Op::kBetween:
                case Op::kNotBetween: {
                    const Type* value = &types_[ops[0]];
                    if (IsOpen(value->category)) {
                        value = !IsOpen(types_[ops[1]].category) ? &types_[ops[1]] : &types_[ops[2]];
                    }
                    CheckComparable(*value, types_[ops[1]], Op::kGreaterEqual, node);
                    CheckComparable(*value, types_[ops[2]], Op::kLessEqual, node);
                    type = Type{.category = Category::kBool};
                    break;
                }
                case Op::kNot:
                case Op::kAnd:
                case Op::kOr:
                    for (const std::uint32_t operand : ops) {
                        CheckTruth(operand);
                    }
                    type = Type{.category = Category::kBool};
                    break;
                case Op::kIsNull:
                case Op::kIsNotNull:
                case Op::kExists:
                    type = Type{.category = Category::kBool};
                    break;
            }
        }
    }

    // Gives each parameter the type of the column-typed operand it meets,
    // root first. A node that is not typed itself (only literals,
    // parameters and NULL) takes `context`, the type its parent passes down:
    // the other side of a comparison, the value an IN list or BETWEEN
    // tests, the assigned column, or the type of an enclosing typed
    // operation. A float literal that takes f32 is rounded to f32.
    void Bind(const std::optional<Type>& target) {
        const auto& nodes = expression_.nodes;
        std::vector<std::optional<Type>> context(nodes.size());
        context[expression_.root] = target;
        const auto typed = [this](std::uint32_t index) -> std::optional<Type> {
            return types_[index].typed ? std::optional<Type>(types_[index]) : std::nullopt;
        };
        for (std::size_t i = nodes.size(); i-- > 0;) {
            const Node& node = nodes[i];
            const auto& ops = node.operands;
            // Typing already checked that the context fits.
            const Type own = !types_[i].typed && context[i] ? *context[i] : types_[i];
            const std::optional<Type> down = own.typed ? std::optional<Type>(own) : std::nullopt;
            switch (node.op) {
                case Op::kParameter:
                    if (!own.typed) {
                        TypeError(
                            node,
                            "$" + std::to_string(std::get<cql::Parameter>(node.literal).index) +
                                " has no column to take its type from; compare, combine or assign it with a column");
                    }
                    declared_[i] = own.repr;
                    break;
                case Op::kLiteral:
                    if (std::holds_alternative<double>(node.literal) && own.typed && own.category == Category::kFloat &&
                        own.repr.kind == ColumnKind::kFloat32) {
                        const double value = std::get<double>(node.literal);
                        if (std::isfinite(value) && std::fabs(value) > static_cast<double>(std::numeric_limits<float>::max())) {
                            throw std::out_of_range(
                                "the number at column " + std::to_string(node.column) + " is out of range for f32");
                        }
                        to_f32_[i] = true;
                    }
                    break;
                case Op::kNegate:
                case Op::kAdd:
                case Op::kSubtract:
                case Op::kMultiply:
                case Op::kDivide:
                case Op::kModulo:
                case Op::kLeast:
                case Op::kGreatest:
                case Op::kCoalesce:
                    for (const std::uint32_t operand : ops) {
                        context[operand] = down;
                    }
                    break;
                case Op::kEqual:
                case Op::kNotEqual:
                case Op::kLess:
                case Op::kLessEqual:
                case Op::kGreater:
                case Op::kGreaterEqual:
                    context[ops[0]] = typed(ops[1]);
                    context[ops[1]] = typed(ops[0]);
                    break;
                case Op::kIn:
                case Op::kNotIn:
                    for (std::size_t k = 1; k < ops.size(); ++k) {
                        context[ops[k]] = typed(ops[0]);
                    }
                    break;
                case Op::kBetween:
                case Op::kNotBetween: {
                    std::optional<Type> first;
                    for (const std::uint32_t operand : ops) {
                        if (!first) {
                            first = typed(operand);
                        }
                    }
                    for (const std::uint32_t operand : ops) {
                        context[operand] = first;
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }

    [[nodiscard]] Value LiteralValue(std::uint32_t index) {
        const Node& node = expression_.nodes[index];
        Value value;
        value.category = types_[index].category;
        std::visit(
            [&](const auto& literal) {
                using T = std::decay_t<decltype(literal)>;
                if constexpr (std::is_same_v<T, cql::Integer>) {
                    const auto magnitude = static_cast<detail::Int>(literal.magnitude);
                    value.integer = literal.negative ? -magnitude : magnitude;
                } else if constexpr (std::is_same_v<T, double>) {
                    value.number = to_f32_[index] ? static_cast<double>(static_cast<float>(literal)) : literal;
                } else if constexpr (std::is_same_v<T, bool>) {
                    value.boolean = literal;
                } else if constexpr (std::is_same_v<T, cql::Text>) {
                    value.bytes = program_->strings.emplace_back(literal.value);
                } else if constexpr (std::is_same_v<T, cql::Bytes>) {
                    value.bytes = program_->strings.emplace_back(literal.value.begin(), literal.value.end());
                } else if constexpr (std::is_same_v<T, cql::Bits>) {
                    value.bytes = program_->strings.emplace_back(literal.digits);
                }
            },
            node.literal);
        return value;
    }

    std::uint32_t Compile() {
        const auto& nodes = expression_.nodes;
        const auto offset = static_cast<std::uint32_t>(program_->steps.size());
        for (std::uint32_t i = 0; i < nodes.size(); ++i) {
            const Node& node = nodes[i];
            Step step;
            step.op = node.op;
            step.category = types_[i].category;
            step.column = node.column;
            step.first = static_cast<std::uint32_t>(program_->operands.size());
            step.count = static_cast<std::uint32_t>(node.operands.size());
            for (const std::uint32_t operand : node.operands) {
                program_->operands.push_back(offset + operand);
            }
            if (node.op == Op::kColumn || node.op == Op::kChunkColumn) {
                step.slot = bindings_[i]->slot;
                step.type = bindings_[i]->type;
            } else if (node.op == Op::kParameter) {
                const std::size_t index = std::get<cql::Parameter>(node.literal).index;
                step.slot = index - 1U;
                step.type = *declared_[i];
                program_->parameters.push_back(ParameterType{.index = index, .type = *declared_[i]});
            } else if (node.op == Op::kLiteral) {
                step.literal = LiteralValue(i);
            }
            program_->steps.push_back(step);
        }
        return offset + expression_.root;
    }

    const Expression& expression_;
    const Schema& schema_;
    Program* program_;
    std::vector<Type> types_;
    std::vector<std::optional<ColumnBinding>> bindings_;
    std::vector<std::optional<ColumnType>> declared_;
    std::vector<bool> to_f32_;
};

void SortParameters(Program* program) {
    std::sort(program->parameters.begin(), program->parameters.end(), [](const auto& a, const auto& b) {
        return a.index < b.index;
    });
}

}  // namespace

bool IsValue(Op op) noexcept {
    return op <= Op::kCoalesce;
}

Condition ParseCondition(std::string_view text, const Limits& limits) {
    std::set<std::size_t> parameters;
    Parser parser(text, 0, &parameters, limits);
    Condition condition;
    condition.expression = parser.ConditionExpression();
    parser.ExpectEnd();
    condition.parameters = ParameterCount(parameters);
    return condition;
}

Assignments ParseAssignments(std::string_view text, const Limits& limits) {
    std::set<std::size_t> parameters;
    Parser parser(text, 0, &parameters, limits);
    Assignments assignments;
    assignments.values = parser.AssignmentList();
    parser.ExpectEnd();
    assignments.parameters = ParameterCount(parameters);
    return assignments;
}

Expression ParseConditionAt(
    std::string_view line,
    std::size_t* position,
    std::set<std::size_t>* parameters,
    const Limits& limits) {
    Parser parser(line, *position, parameters, limits);
    Expression expression = parser.ConditionExpression();
    *position = parser.Position();
    return expression;
}

std::vector<AssignmentExpression> ParseAssignmentsAt(
    std::string_view line,
    std::size_t* position,
    std::set<std::size_t>* parameters,
    const Limits& limits) {
    Parser parser(line, *position, parameters, limits);
    std::vector<AssignmentExpression> values = parser.AssignmentList();
    *position = parser.Position();
    return values;
}

bool IsVersionClause(std::string_view after_if) noexcept {
    try {
        std::size_t at = 0;
        const Token word = Lex(after_if, &at, LexMode::kExpression);
        if (word.kind != TokenKind::kWord || Lower(word.text) != "version") {
            return false;
        }
        while (at < after_if.size() && (after_if[at] == ' ' || after_if[at] == '\t')) {
            ++at;
        }
        if (at >= after_if.size()) {
            return false;
        }
        const char c = after_if[at];
        if (IsDigit(c) || c == '$') {
            return true;
        }
        return c == '-' && at + 1U < after_if.size() && IsDigit(after_if[at + 1U]);
    } catch (...) {
        return false;
    }
}

ColumnList::ColumnList(std::vector<Column> block, std::vector<Column> chunk)
    : block_(std::move(block)), chunk_(std::move(chunk)) {}

namespace {

std::optional<ColumnBinding> Find(const std::vector<Column>& columns, std::string_view name) {
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].name == name) {
            return ColumnBinding{.id = columns[i].id, .slot = i, .type = columns[i].type, .nullable = columns[i].nullable};
        }
    }
    return std::nullopt;
}

}  // namespace

std::optional<ColumnBinding> ColumnList::BlockColumn(std::string_view name) const {
    return Find(block_, name);
}

std::optional<ColumnBinding> ColumnList::ChunkColumn(std::string_view name) const {
    return Find(chunk_, name);
}

const detail::Program& ConditionPlan::program() const {
    if (!program_) {
        throw std::logic_error("the condition was not planned");
    }
    return *program_;
}

const detail::Program& AssignmentsPlan::program() const {
    if (!program_) {
        throw std::logic_error("the assignments were not planned");
    }
    return *program_;
}

const std::vector<ParameterType>& ConditionPlan::parameters() const {
    return program().parameters;
}

const std::vector<ParameterType>& AssignmentsPlan::parameters() const {
    return program().parameters;
}

const std::vector<ColumnBinding>& AssignmentsPlan::targets() const {
    return program().target_columns;
}

ConditionPlan PlanCondition(const Expression& condition, const Schema& schema, const Limits& limits) {
    auto program = std::make_shared<Program>();
    Planner planner(condition, schema, limits, program.get());
    program->root = planner.Plan(std::nullopt);
    SortParameters(program.get());
    return ConditionPlan(std::move(program));
}

AssignmentsPlan PlanAssignments(
    std::span<const AssignmentExpression> assignments,
    const Schema& schema,
    const Limits& limits) {
    auto program = std::make_shared<Program>();
    for (std::size_t i = 0; i < assignments.size(); ++i) {
        const AssignmentExpression& assignment = assignments[i];
        for (std::size_t k = 0; k < i; ++k) {
            if (assignments[k].column == assignment.column) {
                throw std::invalid_argument("column " + assignment.column + " is assigned twice");
            }
        }
        const auto column = schema.BlockColumn(assignment.column);
        if (!column) {
            throw std::invalid_argument("the table has no column " + assignment.column);
        }
        Planner planner(assignment.value, schema, limits, program.get());
        const std::uint32_t root = planner.Plan(TypeOf(column->type));
        // Integers fit floats; otherwise the value has the column's type.
        const Type& type = planner.RootType();
        const Category wanted = CategoryOf(column->type.kind);
        const Node& node = assignment.value.nodes[assignment.value.root];
        if (type.category == Category::kNull) {
            if (!column->nullable && node.op == Op::kLiteral) {
                throw std::invalid_argument("column " + assignment.column + " cannot be NULL");
            }
        } else if (
            (type.category != wanted && !(wanted == Category::kFloat && type.category == Category::kInt)) ||
            (wanted == Category::kBits && type.repr.size != column->type.size)) {
            TypeError(
                node,
                "column " + assignment.column + " (" + ColumnTypeName(column->type) + ") cannot take " + Describe(type));
        }
        program->targets.push_back(detail::Target{.column = *column, .name = assignment.column, .root = root});
        program->target_columns.push_back(*column);
    }
    SortParameters(program.get());
    return AssignmentsPlan(std::move(program));
}

}  // namespace chunkdb::expr
