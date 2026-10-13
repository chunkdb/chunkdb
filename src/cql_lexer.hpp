#pragma once

#include <cstddef>
#include <set>
#include <string>
#include <string_view>

#include "cql.hpp"

// The CQL lexer, shared by the statement parser (cql.cpp) and the
// expression parser (expr.cpp).
namespace chunkdb::cql::lexer {

enum class TokenKind {
    kWord,
    kInteger,
    kFloat,
    kText,
    kBytes,
    kBits,
    kParameter,
    kComma,
    kOpen,
    kClose,
    kEquals,
    kStar,
    kEnd,
    // Expressions only (LexMode::kExpression).
    kLess,
    kLessEqual,
    kGreater,
    kGreaterEqual,
    kNotEqual,
    kPlus,
    kMinus,
    kSlash,
    kPercent,
    kDot,
    kQuotedName,
};

// Statements read `-5` and `-inf` as one number. Expressions read `-` as an
// operator, and also read < <= > >= <> + / % . and "quoted names".
enum class LexMode {
    kStatement,
    kExpression,
};

struct Token {
    TokenKind kind = TokenKind::kEnd;
    // As written; for kText, kBytes, kBits and kQuotedName the decoded
    // contents.
    std::string text;
    // 1-based column of its first character.
    std::size_t column = 0;
};

[[nodiscard]] bool IsWordStart(char c) noexcept;
[[nodiscard]] bool IsWordPart(char c) noexcept;
[[nodiscard]] bool IsDigit(char c) noexcept;
[[nodiscard]] std::string Lower(std::string_view text);
// `text` with control bytes as \xHH, so an error reply stays one line.
[[nodiscard]] std::string Printable(std::string_view text);
[[noreturn]] void Fail(std::size_t column, const std::string& message);
// The token as an error message names it.
[[nodiscard]] std::string Quote(const Token& token);

// Reads the token at `*position` and moves past it; the last token is kEnd.
// Throws ParseError for a character no token starts with.
[[nodiscard]] Token Lex(std::string_view line, std::size_t* position, LexMode mode = LexMode::kStatement);

// The value of a kInteger token (digits, in statements with a leading -).
[[nodiscard]] Integer IntegerOf(const Token& token);
// The value of a kFloat token.
[[nodiscard]] double FloatOf(const Token& token);
// The value of a kBytes token.
[[nodiscard]] Bytes BytesOf(const Token& token);
// The index of a kParameter token, 1 to kMaxParameters.
[[nodiscard]] std::size_t ParameterIndexOf(const Token& token);
// Records parameter `index`, written at `column`; each is used once.
void UseParameter(std::set<std::size_t>* used, std::size_t index, std::size_t column);
// The number of parameters of a statement that uses `used`: $1 to $n
// without gaps.
[[nodiscard]] std::size_t ParameterCount(const std::set<std::size_t>& used);

}  // namespace chunkdb::cql::lexer
