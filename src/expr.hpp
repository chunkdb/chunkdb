#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "chunkdb/schema.hpp"
#include "cql.hpp"

// Expressions (docs/design/EXPRESSIONS_DESIGN.md): the conditions of IF and
// WHERE, and assignments computed from the block. One grammar serves both:
//
//   expr     := and_expr { OR and_expr }
//   and_expr := not_expr { AND not_expr }
//   not_expr := NOT not_expr | pred
//   pred     := '(' expr ')' | EXISTS | sum cmp sum | sum IS [NOT] NULL
//             | sum [NOT] IN '(' (value | $n) {',' (value | $n)} ')'
//             | sum [NOT] BETWEEN sum AND sum | bool_operand
//   sum      := term { ('+' | '-') term }
//   term     := factor { ('*' | '/' | '%') factor }
//   factor   := '-' factor | value | $n | column | chunk.column
//             | POS_X | POS_Y | POS_Z | NOW()
//             | LEAST '(' sum {',' sum} ')' | GREATEST '(' sum {',' sum} ')'
//             | COALESCE '(' sum {',' sum} ')' | '(' sum ')'
//   cmp      := '=' | '<>' | '<' | '<=' | '>' | '>='
//
// An assignment is `column = sum`. A text is parsed (ParseCondition,
// ParseAssignments), planned against the columns of a table (PlanCondition,
// PlanAssignments), which names and types every operand, then evaluated
// once per block (EvaluateCondition, EvaluateAssignments).
//
// Errors: ParseError (SYNTAX) for text the grammar or the limits refuse;
// std::invalid_argument (INVALID_ARGUMENT) for unknown names, type errors,
// parameters without a column to take their type from, division or modulo
// of integers by zero, and NULL for a column without NULL;
// std::out_of_range (OUT_OF_RANGE) for an integer outside the 128-bit range
// and a value its column does not hold.
//
// NULL: arithmetic with NULL is NULL, comparing with NULL is unknown, AND,
// OR and NOT follow three-valued logic and a condition that is unknown is
// false. COALESCE returns its first argument that is not NULL. LEAST and
// GREATEST ignore NULL arguments and are NULL only when every argument is
// NULL, as in PostgreSQL; a NaN argument makes them NaN.
namespace chunkdb::expr {

inline constexpr std::size_t kDefaultMaxNodes = 256;
inline constexpr std::size_t kDefaultMaxDepth = 32;

// The most nodes of one expression (each value, column, operator and list
// item is a node) and the most levels: of nested operators, and separately
// of nested parentheses. Parsing recursion never exceeds max_depth levels.
struct Limits {
    std::size_t max_nodes = kDefaultMaxNodes;
    std::size_t max_depth = kDefaultMaxDepth;
};

enum class Op : std::uint8_t {
    // Values, without operands.
    kLiteral,      // Node::literal, never a cql::Parameter
    kParameter,    // Node::literal holds the cql::Parameter
    kColumn,       // a block column, Node::name
    kChunkColumn,  // chunk.name
    kPosX,
    kPosY,
    kPosZ,
    kNow,
    // Numbers: kNegate takes one operand, the others two.
    kNegate,
    kAdd,
    kSubtract,
    kMultiply,
    kDivide,
    kModulo,
    // One or more operands.
    kLeast,
    kGreatest,
    kCoalesce,
    // Predicates. Comparisons take two operands.
    kEqual,
    kNotEqual,
    kLess,
    kLessEqual,
    kGreater,
    kGreaterEqual,
    kIsNull,     // one operand
    kIsNotNull,  // one operand
    kIn,         // the value, then the list
    kNotIn,
    kBetween,  // the value, the low bound, the high bound
    kNotBetween,
    kExists,
    kNot,  // one predicate
    kAnd,  // two or more predicates
    kOr,
};

// Whether `op` gives a value (else a predicate).
[[nodiscard]] bool IsValue(Op op) noexcept;

struct Node {
    Op op = Op::kLiteral;
    // 1-based column of the line where it is written (an operator's own
    // column for operators).
    std::size_t column = 0;
    cql::Literal literal{};
    std::string name{};
    // Indexes into Expression::nodes, all below this node's own.
    std::vector<std::uint32_t> operands{};
};

// The nodes of one expression, each after its operands; the root is last.
struct Expression {
    std::vector<Node> nodes;
    std::uint32_t root = 0;
};

struct Condition {
    Expression expression;
    // $1 to $parameters, each used once.
    std::size_t parameters = 0;
};

struct AssignmentExpression {
    std::string column;
    // 1-based column of the line where the column name is written.
    std::size_t at = 0;
    Expression value;
};

struct Assignments {
    std::vector<AssignmentExpression> values;
    std::size_t parameters = 0;
};

// The whole of `text` as a condition, or as `column = sum {, column = sum}`.
// Throws ParseError.
[[nodiscard]] Condition ParseCondition(std::string_view text, const Limits& limits = {});
[[nodiscard]] Assignments ParseAssignments(std::string_view text, const Limits& limits = {});

// For a statement that contains an expression: parses from `*position` of
// `line`, stops before the first token that cannot continue it, and leaves
// `*position` there. Parameters join `*parameters`, refusing one already
// there; the statement checks that $1 to $n have no gaps.
[[nodiscard]] Expression ParseConditionAt(
    std::string_view line,
    std::size_t* position,
    std::set<std::size_t>* parameters,
    const Limits& limits = {});
[[nodiscard]] std::vector<AssignmentExpression> ParseAssignmentsAt(
    std::string_view line,
    std::size_t* position,
    std::set<std::size_t>* parameters,
    const Limits& limits = {});

// Whether `after_if`, the text after IF, is the version clause: VERSION
// followed by an integer (a digit, or - and a digit) or a parameter. Anything
// else after IF is a condition, so `IF version = 3` tests a column named
// version. Text that does not lex is not the version clause.
[[nodiscard]] bool IsVersionClause(std::string_view after_if);

// A column an expression may name.
struct ColumnBinding {
    std::uint32_t id = 0;
    // Where its value is in the values given to evaluation.
    std::size_t slot = 0;
    ColumnType type{};
    bool nullable = false;
};

// The names an expression is planned against.
class Schema {
  public:
    virtual ~Schema() = default;
    [[nodiscard]] virtual std::optional<ColumnBinding> BlockColumn(std::string_view name) const = 0;
    [[nodiscard]] virtual std::optional<ColumnBinding> ChunkColumn(std::string_view name) const = 0;
};

// Columns by name; a column's slot is its index in its list.
class ColumnList final : public Schema {
  public:
    explicit ColumnList(std::vector<Column> block, std::vector<Column> chunk = {});
    [[nodiscard]] std::optional<ColumnBinding> BlockColumn(std::string_view name) const override;
    [[nodiscard]] std::optional<ColumnBinding> ChunkColumn(std::string_view name) const override;

  private:
    std::vector<Column> block_;
    std::vector<Column> chunk_;
};

// The type a parameter takes: that of the column it is compared, combined
// or assigned with. Its frame is decoded as a value of that type.
struct ParameterType {
    std::size_t index = 0;
    ColumnType type{};
};

namespace detail {
struct Program;
}  // namespace detail

class ConditionPlan {
  public:
    ConditionPlan() = default;
    explicit ConditionPlan(std::shared_ptr<const detail::Program> program) : program_(std::move(program)) {}
    // The parameters the condition uses, by index.
    [[nodiscard]] const std::vector<ParameterType>& parameters() const;
    // Throws std::logic_error for a plan that was never made.
    [[nodiscard]] const detail::Program& program() const;

  private:
    std::shared_ptr<const detail::Program> program_;
};

class AssignmentsPlan {
  public:
    AssignmentsPlan() = default;
    explicit AssignmentsPlan(std::shared_ptr<const detail::Program> program) : program_(std::move(program)) {}
    [[nodiscard]] const std::vector<ParameterType>& parameters() const;
    // The assigned columns, in the order of the assignments.
    [[nodiscard]] const std::vector<ColumnBinding>& targets() const;
    // Throws std::logic_error for a plan that was never made.
    [[nodiscard]] const detail::Program& program() const;

  private:
    std::shared_ptr<const detail::Program> program_;
};

// Throws std::invalid_argument for an unknown name, a type error, a
// parameter without a column to take its type from, a column assigned twice
// or NULL for a column without NULL; std::out_of_range for a number literal
// its float type does not hold.
[[nodiscard]] ConditionPlan PlanCondition(const Expression& condition, const Schema& schema, const Limits& limits = {});
[[nodiscard]] AssignmentsPlan PlanAssignments(
    std::span<const AssignmentExpression> assignments,
    const Schema& schema,
    const Limits& limits = {});

// What an expression reads about one block.
struct Inputs {
    // An absent block has no values: EXISTS is false and every block column
    // is NULL. Chunk columns keep their values.
    bool exists = false;
    // By slot: ColumnBinding::slot of the block and chunk columns.
    std::span<const ColumnValue> block{};
    std::span<const ColumnValue> chunk{};
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::int64_t z = 0;
    // NOW(), in milliseconds.
    std::uint64_t now = 0;
    // $n at index n - 1, of the type ParameterType gives (std::monostate
    // for NULL).
    std::span<const ColumnValue> parameters{};
};

// Whether the condition holds; unknown is false. Throws std::out_of_range
// for an integer outside the 128-bit range, std::invalid_argument for a
// division or modulo by zero or a parameter that is missing or not of its
// type.
[[nodiscard]] bool EvaluateCondition(const ConditionPlan& plan, const Inputs& inputs);
// The new values of AssignmentsPlan::targets(), all computed from the block
// as it was. Throws as EvaluateCondition, and also std::out_of_range for a
// value its column does not hold and std::invalid_argument for NULL in a
// column without NULL or text that is not UTF-8.
[[nodiscard]] std::vector<ColumnValue> EvaluateAssignments(const AssignmentsPlan& plan, const Inputs& inputs);

}  // namespace chunkdb::expr
