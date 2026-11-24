#include "cql.hpp"
#include "cql_lexer.hpp"
#include "expr.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <clocale>
#include <cmath>
#include <cstdlib>
#include <deque>
#include <limits>
#include <set>
#include <string>
#include <tuple>
#include <utility>

namespace chunkdb::cql {

namespace lexer {

[[nodiscard]] bool IsWordStart(char c) noexcept {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
}

[[nodiscard]] bool IsWordPart(char c) noexcept {
    return IsWordStart(c) || (c >= '0' && c <= '9');
}

[[nodiscard]] bool IsDigit(char c) noexcept {
    return c >= '0' && c <= '9';
}

[[nodiscard]] std::string Lower(std::string_view text) {
    std::string out(text);
    std::transform(out.begin(), out.end(), out.begin(), [](char c) {
        return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
    });
    return out;
}

// `text` with control bytes and bytes of 0x80 and above as \xHH, so an
// error reply stays one line of ASCII, valid UTF-8 whatever the input.
[[nodiscard]] std::string Printable(std::string_view text) {
    std::string out;
    for (const char c : text) {
        const auto byte = static_cast<unsigned char>(c);
        if (byte < 0x20U || byte >= 0x7fU) {
            constexpr std::string_view kHex = "0123456789abcdef";
            out += "\\x";
            out += kHex[byte >> 4U];
            out += kHex[byte & 0x0fU];
        } else {
            out += c;
        }
    }
    return out;
}

[[noreturn]] void Fail(std::size_t column, const std::string& message) {
    throw ParseError("column " + std::to_string(column) + ": " + message);
}

namespace {

// Reads a quoted body starting after the opening `quote` at `at`; a doubled
// quote is one quote. Returns the index after the closing quote.
std::size_t ReadQuoted(std::string_view line, std::size_t at, std::size_t column, std::string* out, char quote = '\'') {
    while (true) {
        if (at >= line.size()) {
            Fail(column, quote == '"' ? "the quoted name has no closing quote" : "the quoted value has no closing quote");
        }
        if (line[at] == quote) {
            if (at + 1 < line.size() && line[at + 1] == quote) {
                out->push_back(quote);
                at += 2;
                continue;
            }
            return at + 1;
        }
        out->push_back(line[at++]);
    }
}

}  // namespace

Token Lex(std::string_view line, std::size_t* position, LexMode mode) {
    std::size_t& at = *position;
    while (at < line.size() && (line[at] == ' ' || line[at] == '\t')) {
        ++at;
    }
    const std::size_t start = at;
    const std::size_t column = at + 1;
    if (at >= line.size()) {
        return Token{.kind = TokenKind::kEnd, .text = {}, .column = column};
    }
    const char c = line[at];
    if (c == '*') {
        ++at;
        return Token{.kind = TokenKind::kStar, .text = "*", .column = column};
    }
    if (mode == LexMode::kExpression) {
        const auto single = [&](TokenKind kind, std::size_t length) {
            at += length;
            return Token{.kind = kind, .text = std::string(line.substr(start, length)), .column = column};
        };
        const char next = at + 1 < line.size() ? line[at + 1] : '\0';
        switch (c) {
            case '<':
                return next == '=' ? single(TokenKind::kLessEqual, 2)
                     : next == '>' ? single(TokenKind::kNotEqual, 2)
                                   : single(TokenKind::kLess, 1);
            case '>':
                return next == '=' ? single(TokenKind::kGreaterEqual, 2) : single(TokenKind::kGreater, 1);
            case '+':
                return single(TokenKind::kPlus, 1);
            case '-':
                return single(TokenKind::kMinus, 1);
            case '/':
                return single(TokenKind::kSlash, 1);
            case '%':
                return single(TokenKind::kPercent, 1);
            case '.':
                return single(TokenKind::kDot, 1);
            case '"': {
                std::string name;
                at = ReadQuoted(line, at + 1, column, &name, '"');
                return Token{.kind = TokenKind::kQuotedName, .text = std::move(name), .column = column};
            }
            default:
                break;
        }
    }
    if (c == ',' || c == '(' || c == ')' || c == '=') {
        ++at;
        return Token{
            .kind = c == ',' ? TokenKind::kComma : c == '(' ? TokenKind::kOpen : c == ')' ? TokenKind::kClose : TokenKind::kEquals,
            .text = std::string(1, c),
            .column = column,
        };
    }
    if (c == '\'') {
        std::string text;
        at = ReadQuoted(line, at + 1, column, &text);
        return Token{.kind = TokenKind::kText, .text = std::move(text), .column = column};
    }
    if ((c == 'x' || c == 'X' || c == 'b' || c == 'B') && at + 1 < line.size() && line[at + 1] == '\'') {
        std::string body;
        at = ReadQuoted(line, at + 2, column, &body);
        const bool hex = c == 'x' || c == 'X';
        if (hex) {
            if (body.size() % 2U != 0U ||
                !std::all_of(body.begin(), body.end(), [](char d) { return std::isxdigit(static_cast<unsigned char>(d)) != 0; })) {
                Fail(column, "x'..' takes pairs of hexadecimal digits");
            }
        } else if (body.empty() || body.find_first_not_of("01") != std::string::npos) {
            Fail(column, "b'..' takes the digits 0 and 1");
        }
        return Token{.kind = hex ? TokenKind::kBytes : TokenKind::kBits, .text = std::move(body), .column = column};
    }
    if (c == '$') {
        std::size_t end = at + 1;
        while (end < line.size() && IsDigit(line[end])) {
            ++end;
        }
        if (end == at + 1) {
            Fail(column, "a parameter is $ followed by its number");
        }
        at = end;
        return Token{.kind = TokenKind::kParameter, .text = std::string(line.substr(start + 1, end - start - 1)), .column = column};
    }
    if (IsDigit(c) || (c == '-' && at + 1 < line.size() && (IsDigit(line[at + 1]) || IsWordStart(line[at + 1])))) {
        // A number, or -inf.
        std::size_t end = at + 1;
        bool is_float = false;
        if (c == '-' && IsWordStart(line[at + 1])) {
            while (end < line.size() && IsWordPart(line[end])) {
                ++end;
            }
            const auto word = Lower(line.substr(at + 1, end - at - 1));
            if (word != "inf") {
                Fail(column, "expected a number after -");
            }
            at = end;
            return Token{.kind = TokenKind::kFloat, .text = "-inf", .column = column};
        }
        while (end < line.size()) {
            const char d = line[end];
            if (IsDigit(d)) {
                ++end;
            } else if (d == '.' || d == 'e' || d == 'E') {
                is_float = true;
                ++end;
                if ((d == 'e' || d == 'E') && end < line.size() && (line[end] == '-' || line[end] == '+')) {
                    ++end;
                }
            } else {
                break;
            }
        }
        at = end;
        return Token{
            .kind = is_float ? TokenKind::kFloat : TokenKind::kInteger,
            .text = std::string(line.substr(start, end - start)),
            .column = column,
        };
    }
    if (IsWordStart(c)) {
        std::size_t end = at + 1;
        while (end < line.size() && IsWordPart(line[end])) {
            ++end;
        }
        at = end;
        return Token{.kind = TokenKind::kWord, .text = std::string(line.substr(start, end - start)), .column = column};
    }
    Fail(column, "unexpected '" + Printable(std::string_view(&c, 1)) + "'");
}

std::string Quote(const Token& token) {
    if (token.kind == TokenKind::kQuotedName) {
        return "'\"" + Printable(token.text) + "\"'";
    }
    return token.kind == TokenKind::kEnd ? "the end of the statement" : "'" + Printable(token.text) + "'";
}

Integer IntegerOf(const Token& token) {
    Integer value;
    std::string_view digits = token.text;
    if (!digits.empty() && digits.front() == '-') {
        value.negative = true;
        digits.remove_prefix(1);
    }
    const auto result = std::from_chars(digits.data(), digits.data() + digits.size(), value.magnitude);
    if (result.ec != std::errc() || result.ptr != digits.data() + digits.size()) {
        Fail(token.column, token.text + " is not a 64-bit integer");
    }
    if (value.negative && value.magnitude == 0U) {
        value.negative = false;
    }
    return value;
}

// The nearest f64, subnormals and zero included; a value beyond f64 is
// refused. The lexer passes only digits, '.', 'e' and signs, so strtod sees
// no hexadecimal, inf or nan; '.' becomes the C locale's decimal point.
double FloatOf(const Token& token) {
    std::string text = Lower(token.text);
    if (text == "-inf") {
        return -std::numeric_limits<double>::infinity();
    }
    const std::string_view point = std::localeconv()->decimal_point;
    if (const auto dot = text.find('.'); dot != std::string::npos && !point.empty() && point != ".") {
        text.replace(dot, 1U, point);
    }
    char* end = nullptr;
    errno = 0;
    const double value = std::strtod(text.c_str(), &end);
    if (end != text.c_str() + text.size()) {
        Fail(token.column, token.text + " is not a number");
    }
    if (errno == ERANGE && std::isinf(value)) {
        Fail(token.column, token.text + " is beyond the f64 range");
    }
    return value;
}

Bytes BytesOf(const Token& token) {
    const std::string& hex = token.text;
    Bytes bytes;
    for (std::size_t i = 0; i < hex.size(); i += 2) {
        bytes.value.push_back(static_cast<std::uint8_t>(std::stoi(hex.substr(i, 2), nullptr, 16)));
    }
    return bytes;
}

std::size_t ParameterIndexOf(const Token& token) {
    std::size_t index = 0;
    const auto result = std::from_chars(token.text.data(), token.text.data() + token.text.size(), index);
    if (result.ec != std::errc() || index == 0U || index > kMaxParameters) {
        Fail(token.column, "parameters are numbered from $1 to $" + std::to_string(kMaxParameters));
    }
    return index;
}

void UseParameter(std::set<std::size_t>* used, std::size_t index, std::size_t column) {
    if (!used->insert(index).second) {
        Fail(column, "$" + std::to_string(index) + " is used twice");
    }
}

std::size_t ParameterCount(const std::set<std::size_t>& used) {
    if (used.empty()) {
        return 0;
    }
    const std::size_t count = *used.rbegin();
    if (used.size() != count) {
        Fail(1, "parameters must be numbered $1 to $" + std::to_string(count) + " without gaps");
    }
    return count;
}

}  // namespace lexer

namespace {

using namespace lexer;

// The next step for an unknown statement word: the closest CQL keyword, the
// CQL form of a common SQL statement, or the reference.
std::string StatementSuggestion(std::string_view word) {
    constexpr std::string_view kReference = "; the CQL reference lists every statement";
    if (word.size() > 16U) {
        return std::string(kReference);
    }
    const auto lower = Lower(word);
    if (lower == "select") return "; read with GET BLOCK, GET AREA or GET CHUNK";
    if (lower == "insert" || lower == "update" || lower == "upsert") return "; write with SET BLOCK or SET CHUNK";
    const std::array<std::string_view, 27U> keywords{
        "get", "set", "delete", "create", "alter", "drop", "show", "describe", "grant", "revoke",
        "begin", "commit", "rollback", "ping", "flush", "watch", "unwatch", "ack", "backup", "migrate",
        "scan", "reload", "recover", "compact", "restore", "swap", "aggregate"};
    std::string_view closest;
    std::size_t best = word.size() < 4U ? 2U : 3U;
    bool tied = false;
    for (const auto keyword : keywords) {
        std::array<std::size_t, 17U> previous{}, next{};
        for (std::size_t j = 0; j <= keyword.size(); ++j) previous[j] = j;
        for (std::size_t i = 0; i < lower.size(); ++i) {
            next[0] = i + 1U;
            for (std::size_t j = 0; j < keyword.size(); ++j) {
                next[j + 1U] = std::min({previous[j + 1U] + 1U, next[j] + 1U,
                    previous[j] + (lower[i] == keyword[j] ? 0U : 1U)});
            }
            previous = next;
        }
        const auto distance = previous[keyword.size()];
        if (distance < best) {
            best = distance;
            closest = keyword;
            tied = false;
        } else if (distance == best) {
            tied = true;
        }
    }
    if (closest.empty() || tied) {
        return std::string(kReference);
    }
    std::string name(closest);
    std::transform(name.begin(), name.end(), name.begin(), [](char c) { return static_cast<char>(c - 'a' + 'A'); });
    return "; did you mean " + name + "?";
}

class Parser {
  public:
    explicit Parser(std::string_view line) : line_(line) {}

    [[nodiscard]] HelloRequest Hello() {
        HelloRequest result;
        Expect("hello");
        result.protocol = Unsigned("a protocol version", std::numeric_limits<std::uint64_t>::max());
        if (Accept("user")) {
            if (Peek().kind != TokenKind::kWord) Fail(Peek().column, "expected a user name");
            result.user = Take().text;
            const auto first = Value(true);
            const auto* parameter = std::get_if<Parameter>(&first);
            if (parameter == nullptr || parameter->index != 1U)
                Fail(Peek().column, "HELLO USER requires $1");
        }
        if (Accept("with")) {
            std::set<std::string> option_names;
            do {
                if (Peek().kind != TokenKind::kWord) Fail(Peek().column, "expected an option name");
                const auto name = Lower(Take().text);
                if (!option_names.insert(name).second)
                    throw DuplicateHelloOptionError(name);
                ExpectToken(TokenKind::kEquals, "=");
                result.options.push_back({name, Value(true)});
            } while (AcceptToken(TokenKind::kComma));
        }
        if (Peek().kind != TokenKind::kEnd) Fail(Peek().column, "expected the end of HELLO");
        if (!parameters_.empty()) {
            result.parameters = *parameters_.rbegin();
            if (parameters_.size() != result.parameters)
                Fail(1, "parameters must be numbered $1 to $" + std::to_string(result.parameters) + " without gaps");
        }
        return result;
    }

    [[nodiscard]] Parsed Statement() {
        Parsed parsed{.statement = ParseStatement(), .parameters = 0};
        if (Peek().kind != TokenKind::kEnd) {
            Fail(Peek().column, "expected the end of the statement, got " + Quote(Peek()));
        }
        parsed.parameters = ParameterCount(parameters_);
        return parsed;
    }

  private:
    // Tokens are read as the parser reaches them, so the first error in the
    // line is the one reported. A deque keeps earlier tokens in place.
    [[nodiscard]] const Token& Peek() {
        if (next_ == tokens_.size()) {
            tokens_.push_back(Lex(line_, &at_));
        }
        return tokens_[next_];
    }
    // The token after the next one; the end when the next one is the end.
    [[nodiscard]] const Token& PeekSecond() {
        if (Peek().kind == TokenKind::kEnd) {
            return tokens_[next_];
        }
        if (next_ + 1U == tokens_.size()) {
            tokens_.push_back(Lex(line_, &at_));
        }
        return tokens_[next_ + 1U];
    }
    const Token& Take() {
        const Token& token = Peek();
        if (token.kind != TokenKind::kEnd) {
            ++next_;
        }
        return token;
    }

    [[nodiscard]] bool IsKeyword(const Token& token, std::string_view keyword) const {
        return token.kind == TokenKind::kWord && Lower(token.text) == keyword;
    }
    bool Accept(std::string_view keyword) {
        if (IsKeyword(Peek(), keyword)) {
            Take();
            return true;
        }
        return false;
    }
    void Expect(std::string_view keyword) {
        if (!Accept(keyword)) {
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

    // Table, column and option names: [a-z_][a-z0-9_]*.
    [[nodiscard]] std::string Name(const char* what) {
        const Token& token = Peek();
        if (token.kind != TokenKind::kWord) {
            Fail(token.column, std::string("expected ") + what + ", got " + Quote(token));
        }
        if (token.text != Lower(token.text)) {
            Fail(token.column, "names are lowercase: " + Quote(token));
        }
        return Take().text;
    }

    [[nodiscard]] std::string SlotName() {
        const Token& token = Peek();
        if (token.kind != TokenKind::kText) {
            Fail(token.column, "expected a quoted slot name, got " + Quote(token));
        }
        const auto& name = token.text;
        const auto start = [](char c) { return (c >= 'a' && c <= 'z') || c == '_'; };
        if (name.empty() || name.size() > 63U || !start(name.front()) ||
            !std::all_of(name.begin(), name.end(), [&](char c) { return start(c) || (c >= '0' && c <= '9'); })) {
            Fail(token.column, "slot names must match [a-z_][a-z0-9_]* and have 1 to 63 bytes");
        }
        return Take().text;
    }

    [[nodiscard]] Integer IntegerToken(const char* what) {
        const Token& token = Peek();
        if (token.kind != TokenKind::kInteger) {
            Fail(token.column, std::string("expected ") + what + ", got " + Quote(token));
        }
        const Integer value = IntegerOf(token);
        Take();
        return value;
    }

    [[nodiscard]] std::int64_t Coordinate(const char* what) {
        const std::size_t column = Peek().column;
        const Integer value = IntegerToken(what);
        const std::uint64_t limit =
            static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) + (value.negative ? 1U : 0U);
        if (value.magnitude > limit) {
            Fail(column, std::string(what) + " is out of range");
        }
        return value.negative ? static_cast<std::int64_t>(0U - value.magnitude) : static_cast<std::int64_t>(value.magnitude);
    }

    [[nodiscard]] std::uint64_t Unsigned(const char* what, std::uint64_t max) {
        const std::size_t column = Peek().column;
        const Integer value = IntegerToken(what);
        if (value.negative || value.magnitude > max) {
            Fail(column, std::string(what) + " must be between 0 and " + std::to_string(max));
        }
        return value.magnitude;
    }

    [[nodiscard]] Literal Value(bool parameter_allowed) {
        const Token& token = Peek();
        switch (token.kind) {
            case TokenKind::kInteger:
                return IntegerToken("a value");
            case TokenKind::kFloat: {
                const double value = FloatOf(token);
                Take();
                return value;
            }
            case TokenKind::kText:
                return Text{.value = Take().text};
            case TokenKind::kBytes:
                return BytesOf(Take());
            case TokenKind::kBits:
                return Bits{.digits = Take().text};
            case TokenKind::kParameter: {
                const std::size_t column = token.column;
                if (!parameter_allowed) {
                    Fail(column, "parameters are values of SET BLOCK and SET CHUNK");
                }
                const std::size_t index = ParameterIndexOf(token);
                Take();
                UseParameter(&parameters_, index, column);
                return Parameter{.index = index};
            }
            case TokenKind::kWord: {
                const std::string word = Lower(token.text);
                if (word == "null" || word == "true" || word == "false" || word == "inf" || word == "nan") {
                    Take();
                    if (word == "null") {
                        return Null{};
                    }
                    if (word == "true" || word == "false") {
                        return word == "true";
                    }
                    return word == "inf" ? std::numeric_limits<double>::infinity() : std::numeric_limits<double>::quiet_NaN();
                }
                break;
            }
            default:
                break;
        }
        Fail(token.column, "expected a value, got " + Quote(token));
    }

    [[nodiscard]] std::vector<std::string> Columns() {
        std::vector<std::string> columns;
        if (Accept("columns")) {
            do {
                columns.push_back(Name("a column name"));
            } while (AcceptToken(TokenKind::kComma));
        }
        return columns;
    }

    [[nodiscard]] FeedArea FeedBounds() {
        FeedArea area;
        area.dims = Point(area.first.x, area.first.y, area.first.z, true);
        Expect("to");
        if (Point(area.last.x, area.last.y, area.last.z, true) != area.dims) Fail(1, "AREA endpoints have different dimensions");
        if (area.first.x > area.last.x || area.first.y > area.last.y || area.first.z > area.last.z)
            Fail(1, "AREA bounds are reversed");
        return area;
    }
    [[nodiscard]] std::vector<std::string> FeedColumns() {
        const bool enclosed = AcceptToken(TokenKind::kOpen);
        std::vector<std::string> columns;
        do { columns.push_back(Name("a column name")); } while (AcceptToken(TokenKind::kComma));
        if (enclosed) ExpectToken(TokenKind::kClose, ")");
        return columns;
    }
    template<class Filters>
    bool FeedFilter(Filters& filters) {
        const auto column = Peek().column;
        if (Accept("area")) {
            if constexpr (requires { filters.areas; }) {
                if (!filters.areas.empty()) Fail(column, "AREA is given twice");
                filters.areas.push_back(FeedBounds());
                while (AcceptToken(TokenKind::kComma)) {
                    const auto next = Peek().column;
                    Expect("area");
                    if (filters.areas.size() == kMaxFeedAreas) Fail(next, "a watch takes at most 64 areas");
                    filters.areas.push_back(FeedBounds());
                    if (filters.areas.back().dims != filters.areas.front().dims) Fail(next, "AREA dimensions differ");
                }
            } else {
                if (filters.area) Fail(column, "AREA is given twice");
                filters.area = FeedBounds();
                if (Peek().kind == TokenKind::kComma) Fail(Peek().column, "a slot takes one AREA");
            }
        } else if (Accept("columns")) {
            if (filters.columns) Fail(column, "COLUMNS is given twice");
            filters.columns = FeedColumns();
        } else if (Accept("no")) {
            if (filters.no_before) Fail(column, "NO BEFORE is given twice");
            Expect("before"); filters.no_before = true;
        } else if (Accept("chunks")) {
            if (filters.chunks) Fail(column, "CHUNKS is given twice");
            filters.chunks = true;
        } else return false;
        return true;
    }

    // IF starts IF [NOT] EXISTS only when NOT or EXISTS follows, so a table,
    // column, user or check named `if` reads as a name.
    [[nodiscard]] bool AcceptIf() {
        if (!IsKeyword(Peek(), "if")) return false;
        const Token& after = PeekSecond();
        if (!IsKeyword(after, "not") && !IsKeyword(after, "exists")) return false;
        Take();
        return true;
    }

    [[nodiscard]] bool IfNotExists() {
        if (!AcceptIf()) return false;
        Expect("not");
        Expect("exists");
        return true;
    }

    [[nodiscard]] bool IfExists() {
        if (!AcceptIf()) return false;
        Expect("exists");
        return true;
    }

    // Drops the tokens read ahead of the next one and returns the offset
    // where the next token starts, so an expression parser can take over.
    std::size_t Rewind() {
        if (next_ < tokens_.size()) {
            at_ = tokens_[next_].column - 1U;
            tokens_.resize(next_);
        }
        return at_;
    }

    // SET BLOCK's values: single values followed by IF, RETURNING or the
    // end, as before expressions; otherwise every value is an expression
    // (docs/design/EXPRESSIONS_DESIGN.md), parsed into `*expressions`.
    void BlockValues(std::vector<Assignment>* values, std::shared_ptr<expr::WriteExpressions>* expressions) {
        const std::size_t start = Rewind();
        const std::size_t mark = next_;
        const std::set<std::size_t> before = parameters_;
        try {
            *values = Assignments();
            if (Peek().kind == TokenKind::kEnd || IsKeyword(Peek(), "if") || IsKeyword(Peek(), "returning")) return;
        } catch (const ParseError&) {
            // Not single values: read again as expressions below.
        }
        values->clear();
        next_ = mark;
        tokens_.resize(mark);
        at_ = start;
        parameters_ = before;
        *expressions = std::make_shared<expr::WriteExpressions>();
        (*expressions)->assignments = expr::ParseAssignmentsAt(line_, &at_, &parameters_);
    }

    // IF VERSION n, or IF <expr> into `*expressions`, or nothing.
    void BlockCondition(std::optional<std::uint64_t>* if_version, std::shared_ptr<expr::WriteExpressions>* expressions) {
        if (!Accept("if")) return;
        const std::size_t after = Rewind();
        if (expr::IsVersionClause(line_.substr(after))) {
            Expect("version");
            if (Peek().kind != TokenKind::kParameter) {
                *if_version = Unsigned("a version", std::numeric_limits<std::uint64_t>::max());
                return;
            }
            // IF VERSION $n: read where the parameter's frame is.
            if (!*expressions) *expressions = std::make_shared<expr::WriteExpressions>();
            (*expressions)->version_parameter = std::get<Parameter>(Value(true)).index;
            return;
        }
        if (!*expressions) *expressions = std::make_shared<expr::WriteExpressions>();
        (*expressions)->condition = expr::ParseConditionAt(line_, &at_, &parameters_);
    }

    // RETURNING [OLD | NEW] (* | column, ...), or nothing. OLD is the default
    // of DELETE, which takes only OLD; NEW that of SET. OLD and NEW right
    // after RETURNING are always the keywords: RETURNING NEW old names a
    // column old.
    [[nodiscard]] std::shared_ptr<const Returning> ReturningClause(bool remove) {
        if (!Accept("returning")) return nullptr;
        auto returning = std::make_shared<Returning>();
        returning->new_values = !remove;
        const std::size_t column = Peek().column;
        if (Accept("old")) {
            returning->new_values = false;
        } else if (Accept("new")) {
            if (remove) Fail(column, "DELETE BLOCK returns only OLD values");
            returning->new_values = true;
        }
        if (AcceptToken(TokenKind::kStar)) return returning;
        do {
            returning->columns.push_back(Name("a column name or *"));
        } while (AcceptToken(TokenKind::kComma));
        return returning;
    }

    [[nodiscard]] std::uint8_t Point(std::int64_t& x, std::int64_t& y, std::int64_t& z, bool chunk = false) {
        x = Coordinate(chunk ? "a chunk x" : "x");
        y = Coordinate(chunk ? "a chunk y" : "y");
        if (Peek().kind == TokenKind::kInteger) {
            z = Coordinate(chunk ? "a chunk z" : "z");
            return 3;
        }
        z = 0;
        return 2;
    }

    // After WHERE: the condition (expr.hpp), up to the first token that
    // cannot continue it. Its parameters join the statement's.
    [[nodiscard]] std::shared_ptr<const expr::Expression> Where() {
        // The expression lexes from the end of the last token read, so no
        // token after WHERE may have been read yet.
        if (next_ != tokens_.size()) throw std::logic_error("WHERE was parsed after a lookahead");
        return std::make_shared<const expr::Expression>(expr::ParseConditionAt(line_, &at_, &parameters_));
    }

    // After AGGREGATE: CHUNK c | AREA a TO b | AREA AROUND c RADIUS r, then
    // FROM t, the functions, WHERE and BY CHUNK.
    [[nodiscard]] Aggregate AggregateStatement() {
        Aggregate aggregate;
        if (Accept("chunk")) {
            aggregate.dims = Point(aggregate.x0, aggregate.y0, aggregate.z0, true);
            aggregate.x1 = aggregate.x0;
            aggregate.y1 = aggregate.y0;
            aggregate.z1 = aggregate.z0;
        } else if (Accept("area")) {
            if (Accept("around")) {
                aggregate.around = true;
                aggregate.dims = Point(aggregate.x0, aggregate.y0, aggregate.z0);
                Expect("radius");
                aggregate.radius = static_cast<std::int64_t>(Unsigned("a radius", std::numeric_limits<std::int64_t>::max()));
            } else {
                aggregate.dims = Point(aggregate.x0, aggregate.y0, aggregate.z0);
                Expect("to");
                if (Point(aggregate.x1, aggregate.y1, aggregate.z1) != aggregate.dims)
                    Fail(1, "AREA endpoints have different dimensions");
            }
        } else {
            Fail(Peek().column, "expected CHUNK or AREA, got " + Quote(Peek()));
        }
        Expect("from");
        aggregate.table = Name("a table name");
        do {
            const auto column = Peek().column;
            AggregateFunction function;
            using Kind = AggregateFunction::Kind;
            if (Accept("count")) function.kind = Kind::kCount;
            else if (Accept("sum")) function.kind = Kind::kSum;
            else if (Accept("min")) function.kind = Kind::kMin;
            else if (Accept("max")) function.kind = Kind::kMax;
            else if (Accept("avg")) function.kind = Kind::kAvg;
            else Fail(column, "expected COUNT, SUM, MIN, MAX or AVG, got " + Quote(Peek()));
            ExpectToken(TokenKind::kOpen, "(");
            if (function.kind == Kind::kCount) {
                if (Peek().kind != TokenKind::kStar) Fail(Peek().column, "COUNT takes *, got " + Quote(Peek()));
                Take();
            } else {
                function.column = Name("a column name");
            }
            ExpectToken(TokenKind::kClose, ")");
            if (std::find(aggregate.functions.begin(), aggregate.functions.end(), function) != aggregate.functions.end())
                Fail(column, AggregateFunctionName(function) + " is given twice");
            aggregate.functions.push_back(std::move(function));
        } while (AcceptToken(TokenKind::kComma));
        while (true) {
            const auto column = Peek().column;
            if (Accept("by")) {
                if (aggregate.by_chunk) Fail(column, "BY CHUNK is given twice");
                Expect("chunk");
                aggregate.by_chunk = true;
            } else if (Accept("where")) {
                if (aggregate.where) Fail(column, "WHERE is given twice");
                aggregate.where = Where();
            } else {
                break;
            }
        }
        return aggregate;
    }

    [[nodiscard]] std::vector<Assignment> Assignments() {
        std::vector<Assignment> values;
        do {
            Assignment assignment;
            assignment.column = Name("a column name");
            ExpectToken(TokenKind::kEquals, "=");
            assignment.value = Value(true);
            values.push_back(std::move(assignment));
        } while (AcceptToken(TokenKind::kComma));
        return values;
    }

    [[nodiscard]] ColumnType Type() {
        const Token& token = Peek();
        if (token.kind != TokenKind::kWord) {
            Fail(token.column, "expected a column type, got " + Quote(token));
        }
        const std::string word = Lower(Take().text);
        const std::size_t column = token.column;
        if (word == "bool") {
            return ColumnType{.kind = ColumnKind::kBool, .size = 1};
        }
        if (word == "f32" || word == "f64") {
            return ColumnType{.kind = word == "f32" ? ColumnKind::kFloat32 : ColumnKind::kFloat64, .size = word == "f32" ? 32U : 64U};
        }
        if ((word[0] == 'u' || word[0] == 'i') && word.size() > 1 &&
            std::all_of(word.begin() + 1, word.end(), [](char c) { return IsDigit(c); })) {
            std::uint32_t size = 0;
            const auto result = std::from_chars(word.data() + 1, word.data() + word.size(), size);
            if (result.ec != std::errc()) {
                Fail(column, word + " is not a column type");
            }
            return ColumnType{.kind = word[0] == 'u' ? ColumnKind::kUnsigned : ColumnKind::kSigned, .size = size};
        }
        if (word == "bits" || word == "text" || word == "bytes") {
            ExpectToken(TokenKind::kOpen, "(");
            const auto size = static_cast<std::uint32_t>(Unsigned("a size", std::numeric_limits<std::uint32_t>::max()));
            ExpectToken(TokenKind::kClose, ")");
            const ColumnKind kind = word == "bits" ? ColumnKind::kBits : word == "text" ? ColumnKind::kText : ColumnKind::kBytes;
            return ColumnType{.kind = kind, .size = size};
        }
        Fail(column, word + " is not a column type");
    }

    [[nodiscard]] ColumnDefinition Definition() {
        ColumnDefinition definition;
        definition.name = Name("a column name");
        definition.type = Type();
        // NULL, REQUIRED and DEFAULT, each at most once, in any order.
        while (true) {
            const std::size_t column = Peek().column;
            if (Accept("null")) {
                if (definition.nullable) {
                    Fail(column, "NULL is given twice");
                }
                definition.nullable = true;
            } else if (Accept("required")) {
                if (definition.required) {
                    Fail(column, "REQUIRED is given twice");
                }
                definition.required = true;
            } else if (Accept("default")) {
                if (definition.default_value.has_value()) {
                    Fail(column, "DEFAULT is given twice");
                }
                definition.default_value = Value(false);
            } else if (Accept("check")) {
                definition.checks.push_back(Check());
            } else {
                return definition;
            }
        }
    }

    // After CHECK: [name] '(' condition ')'. The condition (expr.hpp) takes
    // no parameters; its text is kept as written.
    [[nodiscard]] CheckDefinition Check() {
        CheckDefinition check;
        if (Peek().kind == TokenKind::kWord) check.name = Name("a check name");
        ExpectToken(TokenKind::kOpen, "(");
        // The condition lexes from the end of the last token read, as WHERE.
        if (next_ != tokens_.size()) throw std::logic_error("CHECK was parsed after a lookahead");
        const std::size_t start = at_;
        std::set<std::size_t> parameters;
        (void)expr::ParseConditionAt(line_, &at_, &parameters);
        if (!parameters.empty()) Fail(start + 1U, "CHECK takes no parameters");
        std::string_view text = line_.substr(start, at_ - start);
        const auto first = text.find_first_not_of(" \t");
        const auto last = text.find_last_not_of(" \t");
        check.expression = std::string(text.substr(first, last + 1U - first));
        ExpectToken(TokenKind::kClose, ")");
        return check;
    }

    // A column of CHUNK COLUMNS or ADD COLUMN, which take no CHECK.
    [[nodiscard]] ColumnDefinition DefinitionWithoutCheck(const char* where) {
        const std::size_t column = Peek().column;
        auto definition = Definition();
        if (!definition.checks.empty()) Fail(column, std::string(where) + " takes no CHECK; " +
            (std::string_view(where) == "ADD COLUMN" ? "add the column, then ALTER TABLE ... ADD CHECK"
                                                     : "CHECK reads block columns"));
        return definition;
    }

    [[nodiscard]] Option OptionAssignment() {
        Option option;
        option.name = Name("an option name");
        ExpectToken(TokenKind::kEquals, "=");
        option.value = Value(false);
        return option;
    }

    [[nodiscard]] std::vector<std::uint32_t> Dimensions() {
        std::vector<std::uint32_t> axes{
            static_cast<std::uint32_t>(Unsigned("a width", std::numeric_limits<std::uint32_t>::max()))};
        // Compact forms lex as an integer followed by one word: x16x16.
        while (true) {
            if (Accept("x")) {
                if (axes.size() == 3U) Fail(Peek().column, "geometry has more than three dimensions");
                axes.push_back(static_cast<std::uint32_t>(Unsigned("a dimension", std::numeric_limits<std::uint32_t>::max())));
                continue;
            }
            const auto& token = Peek();
            if (token.kind != TokenKind::kWord || token.text.size() <= 1U ||
                (token.text.front() != 'x' && token.text.front() != 'X')) break;
            std::size_t at = 0U;
            while (at < token.text.size()) {
                if (axes.size() == 3U) Fail(token.column, "geometry has more than three dimensions");
                if (token.text[at] != 'x' && token.text[at] != 'X') Fail(token.column, "expected X between dimensions");
                const auto start = ++at;
                while (at < token.text.size() && IsDigit(token.text[at])) ++at;
                std::uint32_t axis = 0U;
                const auto result = std::from_chars(token.text.data() + start, token.text.data() + at, axis);
                if (start == at || result.ec != std::errc()) Fail(token.column, "the dimension is out of range");
                axes.push_back(axis);
            }
            Take();
        }
        if (axes.size() < 2U) Fail(Peek().column, "expected X, got " + Quote(Peek()));
        return axes;
    }

    // A verifier: a parameter, or a text literal.
    [[nodiscard]] Literal Verifier() {
        const std::size_t column = Peek().column;
        Literal value = Value(true);
        if (!std::holds_alternative<Parameter>(value) && !std::holds_alternative<Text>(value)) {
            Fail(column, "a verifier is a parameter or a quoted SCRAM-SHA-256$... value, never a password");
        }
        return value;
    }

    // GRANT <right>, ... ON <table | prefix* | *> TO <user>, or REVOKE ...
    // FROM <user>. Server rights go ON * only.
    [[nodiscard]] GrantRight GrantOrRevoke(bool revoke) {
        GrantRight grant;
        grant.revoke = revoke;
        const auto server = [&grant](ServerRight right) { grant.server_rights |= static_cast<std::uint8_t>(right); };
        do {
            Right right;
            if (Accept("monitor")) { server(ServerRight::kMonitor); continue; }
            if (Accept("backup")) { server(ServerRight::kBackup); continue; }
            if (Accept("replication")) { server(ServerRight::kReplication); continue; }
            if (Accept("read")) right = Right::kRead;
            else if (Accept("write")) right = Right::kWrite;
            else if (Accept("admin")) right = Right::kAdmin;
            else Fail(Peek().column, "expected READ, WRITE, ADMIN, MONITOR, BACKUP or REPLICATION, got " + Quote(Peek()));
            if (!grant.right || (revoke ? right < *grant.right : right > *grant.right)) grant.right = right;
        } while (AcceptToken(TokenKind::kComma));
        Expect("on");
        const std::size_t target = Peek().column;
        if (AcceptToken(TokenKind::kStar)) {
            grant.table = kEveryTable;
        } else {
            const std::size_t end = Peek().column + Peek().text.size();
            grant.table = Name("a table name, a prefix followed by *, or *");
            // `p*` written without a space is a prefix grant.
            if (Peek().kind == TokenKind::kStar && Peek().column == end) {
                Take();
                grant.table += '*';
            }
        }
        if (grant.server_rights != 0U && grant.table != kEveryTable)
            Fail(target, "MONITOR, BACKUP and REPLICATION are granted ON *");
        Expect(revoke ? "from" : "to");
        grant.user = Name("a user name");
        return grant;
    }

    [[nodiscard]] cql::Statement ParseStatement() {
        const Token& first = Peek();
        if (Accept("backup")) {
            Expect("to");
            const auto& path = Peek();
            if (path.kind != TokenKind::kText)
                Fail(path.column, "expected a quoted backup path, got " + Quote(path));
            return Backup{Take().text};
        }
        if (Accept("migrate")) {
            Migrate migration;
            migration.name = SlotName();
            const auto start = Peek().column - 1U;
            if (IsKeyword(Peek(), "migrate"))
                Fail(start + 1U, "MIGRATE cannot contain another MIGRATE");
            auto statement = ParseStatement();
            bool allowed = false;
            std::visit([&](auto&& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, CreateTable> || std::is_same_v<T, AlterTable> ||
                              std::is_same_v<T, DropTable> || std::is_same_v<T, GrantRight> ||
                              std::is_same_v<T, CreateSlot> || std::is_same_v<T, DropSlot>) {
                    migration.statement = std::move(value);
                    allowed = true;
                }
            }, statement);
            if (!allowed || !parameters_.empty())
                Fail(start + 1U, "MIGRATE takes one schema statement without parameters");
            if (const auto* alter = std::get_if<AlterTable>(&migration.statement);
                alter != nullptr && std::holds_alternative<RenameTable>(alter->change))
                Fail(start + 1U, "MIGRATE cannot rename a table; run ALTER TABLE ... RENAME TO on its own");
            const auto end = line_.find_last_not_of(" \t");
            migration.text = std::string(line_.substr(start, end == std::string_view::npos ? 0U : end + 1U - start));
            return migration;
        }
        if (Accept("get")) {
            if (Accept("block")) {
                GetBlock get;
                get.dims = Point(get.x, get.y, get.z);
                Expect("from");
                get.table = Name("a table name");
                bool columns = false;
                while (true) {
                    const auto option = Peek().column;
                    if (IsKeyword(Peek(), "columns")) {
                        if (columns) Fail(option, "COLUMNS is given twice");
                        columns = true;
                        get.columns = Columns();
                    } else if (Accept("with")) {
                        const auto column = Peek().column;
                        if (Accept("version")) {
                            if (get.with_version) Fail(column, "WITH VERSION is given twice");
                            get.with_version = true;
                        } else if (Accept("schema")) {
                            if (get.with_schema) Fail(column, "WITH SCHEMA is given twice");
                            get.with_schema = true;
                        } else Fail(column, "expected VERSION or SCHEMA");
                    } else break;
                }
                return get;
            }
            if (Accept("chunk")) {
                GetChunk get;
                get.dims = Point(get.chunk_x, get.chunk_y, get.chunk_z, true);
                Expect("from");
                get.table = Name("a table name");
                get.columns = Columns();
                return get;
            }
            if (Accept("area")) {
                GetArea get;
                if (Accept("around")) {
                    get.around = true;
                    get.dims = Point(get.x0, get.y0, get.z0);
                    Expect("radius");
                    get.radius = static_cast<std::int64_t>(Unsigned("a radius", std::numeric_limits<std::int64_t>::max()));
                } else {
                    get.dims = Point(get.x0, get.y0, get.z0);
                    Expect("to");
                    if (Point(get.x1, get.y1, get.z1) != get.dims) Fail(1, "AREA endpoints have different dimensions");
                }
                Expect("from");
                get.table = Name("a table name");
                bool columns = false;
                while (true) {
                    const auto column = Peek().column;
                    if (IsKeyword(Peek(), "columns")) {
                        if (columns) Fail(column, "COLUMNS is given twice");
                        columns = true;
                        get.columns = Columns();
                    } else if (Accept("with")) {
                        if (get.with_schema) Fail(column, "WITH SCHEMA is given twice");
                        Expect("schema");
                        get.with_schema = true;
                    } else if (Accept("limit")) {
                        if (get.limit) Fail(column, "LIMIT is given twice");
                        get.limit = Unsigned("a limit", std::numeric_limits<std::uint64_t>::max());
                    } else if (Accept("after")) {
                        if (get.after) Fail(column, "AFTER is given twice");
                        ChunkCoord coord;
                        if (Point(coord.x, coord.y, coord.z, true) != get.dims)
                            Fail(column, "AREA cursor has different dimensions");
                        get.after = coord;
                    } else if (Accept("scale")) {
                        if (get.scale) Fail(column, "SCALE is given twice");
                        ChunkFormScale scale;
                        const auto factor = Peek().column;
                        scale.factor = static_cast<std::uint32_t>(Unsigned("a scale", std::numeric_limits<std::uint32_t>::max()));
                        if (scale.factor == 0U) Fail(factor, "a scale must be positive");
                        Expect("using");
                        const auto reduction = Peek().column;
                        if (Accept("max")) scale.reduction = ChunkFormReduction::kMax;
                        else if (Accept("min")) scale.reduction = ChunkFormReduction::kMin;
                        else if (Accept("mode")) scale.reduction = ChunkFormReduction::kMode;
                        else if (Accept("first")) scale.reduction = ChunkFormReduction::kFirst;
                        else if (Accept("any")) scale.reduction = ChunkFormReduction::kAny;
                        else Fail(reduction, "expected MAX, MIN, MODE, FIRST or ANY, got " + Quote(Peek()));
                        get.scale = scale;
                    } else break;
                }
                return get;
            }
            Fail(Peek().column, "expected BLOCK, CHUNK or AREA, got " + Quote(Peek()));
        }
        if (Accept("aggregate")) return AggregateStatement();
        if (Accept("set")) {
            if (Accept("block")) {
                SetBlock set;
                set.dims = Point(set.x, set.y, set.z);
                Expect("in");
                set.table = Name("a table name");
                std::shared_ptr<expr::WriteExpressions> expressions;
                BlockValues(&set.values, &expressions);
                BlockCondition(&set.if_version, &expressions);
                set.expressions = std::move(expressions);
                set.returning = ReturningClause(false);
                return set;
            }
            if (Accept("chunk")) {
                SetChunk set;
                set.dims = Point(set.chunk_x, set.chunk_y, set.chunk_z, true);
                Expect("in");
                set.table = Name("a table name");
                if (Accept("meta")) {
                    set.metadata = Assignments();
                } else {
                    set.columns = Columns();
                    const std::size_t column = Peek().column;
                    const Literal state = Value(true);
                    if (!std::holds_alternative<Parameter>(state)) {
                        Fail(column, "the chunk is sent as a parameter ($1)");
                    }
                    set.state = std::get<Parameter>(state);
                }
                if (Accept("if")) {
                    if (Accept("absent")) set.if_absent = true;
                    else {
                        Expect("version");
                        set.if_version = Unsigned("a version", std::numeric_limits<std::uint64_t>::max());
                    }
                }
                return set;
            }
            Fail(Peek().column, "expected BLOCK or CHUNK, got " + Quote(Peek()));
        }
        if (Accept("delete")) {
            Expect("block");
            DeleteBlock del;
            del.dims = Point(del.x, del.y, del.z);
            Expect("from");
            del.table = Name("a table name");
            std::shared_ptr<expr::WriteExpressions> expressions;
            BlockCondition(&del.if_version, &expressions);
            del.expressions = std::move(expressions);
            del.returning = ReturningClause(true);
            return del;
        }
        if (Accept("create")) {
            if (Accept("slot")) {
                CreateSlot create;
                create.if_not_exists = IfNotExists();
                create.name = SlotName();
                Expect("on");
                create.table = Name("a table name");
                while (true) {
                    const auto column = Peek().column;
                    if (Accept("owner")) {
                        if (create.owner) Fail(column, "OWNER is given twice");
                        create.owner = Name("a user name");
                    } else if (!FeedFilter(create)) break;
                }
                return create;
            }
            if (Accept("user")) {
                CreateUser create;
                create.if_not_exists = IfNotExists();
                create.user = Name("a user name");
                Expect("verifier");
                create.verifier = Verifier();
                while (true) {
                    const auto column = Peek().column;
                    if (Accept("manages")) {
                        if (create.manages_users) Fail(column, "MANAGES USERS is given twice");
                        Expect("users");
                        create.manages_users = true;
                    } else if (Accept("require")) {
                        if (create.requires_channel_binding) Fail(column, "REQUIRE CHANNEL BINDING is given twice");
                        Expect("channel");
                        Expect("binding");
                        create.requires_channel_binding = true;
                    } else break;
                }
                return create;
            }
            Expect("table");
            CreateTable create;
            create.if_not_exists = IfNotExists();
            create.table = Name("a table name");
            ExpectToken(TokenKind::kOpen, "(");
            do {
                create.columns.push_back(Definition());
            } while (AcceptToken(TokenKind::kComma));
            ExpectToken(TokenKind::kClose, ")");
            bool chunk = false;
            bool with = false;
            bool chunk_columns = false;
            std::optional<std::size_t> large_dims;
            while (true) {
                const auto column = Peek().column;
                if (Accept("chunk")) {
                    if (Accept("columns")) {
                        if (chunk_columns) Fail(column, "CHUNK COLUMNS is given twice");
                        chunk_columns = true;
                        ExpectToken(TokenKind::kOpen, "(");
                        do { create.chunk_columns.push_back(DefinitionWithoutCheck("CHUNK COLUMNS")); } while (AcceptToken(TokenKind::kComma));
                        ExpectToken(TokenKind::kClose, ")");
                    } else {
                        if (chunk) Fail(column, "CHUNK is given twice");
                        chunk = true;
                        const auto axes = Dimensions();
                        create.chunk_width = axes[0];
                        create.chunk_height = axes[1];
                        create.dims = static_cast<std::uint8_t>(axes.size());
                        if (axes.size() == 3U) create.chunk_depth = axes[2];
                    }
                } else if (Accept("large")) {
                    if (create.large) Fail(column, "LARGE is given twice");
                    const auto axes = Dimensions();
                    create.large = std::make_pair(axes[0], axes[1]);
                    large_dims = axes.size();
                    if (axes.size() == 3U) create.large_depth = axes[2];
                } else if (Accept("check")) {
                    create.checks.push_back(Check());
                } else if (Accept("with")) {
                    if (with) Fail(column, "WITH is given twice");
                    with = true;
                    do {
                        create.options.push_back(OptionAssignment());
                    } while (AcceptToken(TokenKind::kComma));
                } else break;
            }
            if (large_dims && *large_dims != create.dims) Fail(1, "CHUNK and LARGE have different dimensions");
            return create;
        }
        if (Accept("alter")) {
            if (Accept("user")) {
                AlterUser alter;
                alter.user = Name("a user name");
                if (Accept("verifier")) {
                    alter.verifier = Verifier();
                } else if (Accept("add")) {
                    Expect("verifier");
                    alter.added_verifier = Verifier();
                } else if (Accept("drop")) {
                    Expect("old");
                    Expect("verifier");
                    alter.drop_old_verifier = true;
                } else if (Accept("limit")) {
                    do {
                        const auto column = Peek().column;
                        const auto name = Name("a limit name");
                        ExpectToken(TokenKind::kEquals, "=");
                        if (name == "max_connections") {
                            if (alter.max_connections) Fail(column, "max_connections is given twice");
                            alter.max_connections = Unsigned("max_connections", std::numeric_limits<std::uint64_t>::max());
                        } else if (name == "statements_per_sec") {
                            if (alter.statements_per_sec) Fail(column, "statements_per_sec is given twice");
                            alter.statements_per_sec = Unsigned("statements_per_sec", kMaxUserStatementsPerSecond);
                        } else {
                            Fail(column, "unknown user limit: " + name);
                        }
                    } while (AcceptToken(TokenKind::kComma));
                } else {
                    const bool no = Accept("no");
                    if (Accept("require")) {
                        Expect("channel");
                        Expect("binding");
                        alter.requires_channel_binding = !no;
                    } else {
                        Expect("manages");
                        Expect("users");
                        alter.manages_users = !no;
                    }
                }
                return alter;
            }
            Expect("table");
            AlterTable alter;
            alter.table = Name("a table name");
            if (Accept("add")) {
                if (Accept("check")) {
                    alter.change = AddCheck{.check = Check()};
                    return alter;
                }
                const bool chunk_column = Accept("chunk");
                Expect("column");
                const bool conditional = IfNotExists();
                alter.change = AddColumn{
                    .column = DefinitionWithoutCheck("ADD COLUMN"), .if_not_exists = conditional, .chunk_column = chunk_column};
            } else if (Accept("drop")) {
                if (Accept("check")) {
                    const bool conditional = IfExists();
                    alter.change = DropCheck{.name = Name("a check name"), .if_exists = conditional};
                    return alter;
                }
                const bool chunk_column = Accept("chunk");
                Expect("column");
                const bool conditional = IfExists();
                alter.change = DropColumn{.column = Name("a column name"), .if_exists = conditional, .chunk_column = chunk_column};
            } else if (Accept("rename")) {
                if (Accept("to")) {
                    alter.change = RenameTable{.new_name = Name("a table name")};
                    return alter;
                }
                const bool chunk_column = Accept("chunk");
                Expect("column");
                RenameColumn rename;
                rename.chunk_column = chunk_column;
                rename.column = Name("a column name");
                Expect("to");
                rename.new_name = Name("a column name");
                alter.change = rename;
            } else if (Accept("alter")) {
                Expect("column");
                const auto column_name = Name("a column name");
                const bool set = Accept("set");
                if (set || Accept("drop")) {
                    using Change = AlterColumnAttribute::Change;
                    AlterColumnAttribute attribute;
                    attribute.column = column_name;
                    if (Accept("default")) {
                        attribute.change = set ? Change::kSetDefault : Change::kDropDefault;
                        if (set) attribute.value = Value(false);
                    } else if (Accept("null")) {
                        attribute.change = set ? Change::kSetNull : Change::kDropNull;
                    } else if (Accept("required")) {
                        attribute.change = set ? Change::kSetRequired : Change::kDropRequired;
                    } else {
                        Fail(Peek().column, "expected DEFAULT, NULL or REQUIRED, got " + Quote(Peek()));
                    }
                    alter.change = std::move(attribute);
                    return alter;
                }
                AlterColumnType change;
                change.column = column_name;
                if (!IsKeyword(Peek(), "type")) Fail(Peek().column, "expected TYPE, SET or DROP, got " + Quote(Peek()));
                Expect("type");
                change.type = Type();
                if (Accept("using")) {
                    if (Accept("clamp")) {
                        change.conversion = Conversion::kClamp;
                    } else if (Accept("default")) {
                        change.conversion = Conversion::kDefault;
                    } else if (Accept("truncate")) {
                        change.conversion = Conversion::kTruncate;
                    } else {
                        Fail(Peek().column, "expected CLAMP, DEFAULT or TRUNCATE, got " + Quote(Peek()));
                    }
                }
                alter.change = change;
            } else if (Accept("set")) {
                alter.change = SetOption{.option = OptionAssignment()};
            } else {
                Fail(Peek().column, "expected ADD, DROP, RENAME, ALTER or SET, got " + Quote(Peek()));
            }
            return alter;
        }
        if (Accept("drop")) {
            if (Accept("slot")) {
                DropSlot drop;
                drop.if_exists = IfExists();
                drop.name = SlotName();
                Expect("on");
                drop.table = Name("a table name");
                return drop;
            }
            if (Accept("user")) {
                const bool conditional = IfExists();
                return DropUser{.user = Name("a user name"), .if_exists = conditional};
            }
            Expect("table");
            const bool conditional = IfExists();
            return DropTable{.table = Name("a table name"), .if_exists = conditional};
        }
        if (Accept("grant")) {
            return GrantOrRevoke(false);
        }
        if (Accept("revoke")) {
            return GrantOrRevoke(true);
        }
        if (Accept("enable")) {
            Expect("feature");
            if (Peek().kind != TokenKind::kWord && Peek().kind != TokenKind::kText)
                Fail(Peek().column, "expected a feature name");
            return EnableFeature{.name = Take().text};
        }
        if (Accept("show")) {
            if (Accept("connections")) return ShowConnections{};
            if (Accept("table")) {
                Expect("stats");
                return ShowTableStats{.table = Name("a table name")};
            }
            if (Accept("status")) return ShowStatus{};
            if (Accept("migrations")) return ShowMigrations{};
            if (Accept("slots")) {
                ShowSlots show;
                while (true) {
                    const auto column = Peek().column;
                    if (Accept("on")) {
                        if (show.table) Fail(column, "ON is given twice");
                        show.table = Name("a table name");
                    } else if (Accept("prefix")) {
                        if (show.prefix) Fail(column, "PREFIX is given twice");
                        show.prefix = SlotName();
                    } else break;
                }
                return show;
            }
            if (Accept("users")) {
                return ShowUsers{};
            }
            if (Accept("tables")) {
                return ShowTables{};
            }
            if (Accept("metrics")) {
                return ShowMetrics{};
            }
            Fail(Peek().column, "expected TABLES, METRICS, STATUS, CONNECTIONS, MIGRATIONS, USERS or SLOTS, got " + Quote(Peek()));
        }
        if (Accept("cancel")) return Cancel{Unsigned("a connection id", std::numeric_limits<std::uint64_t>::max())};
        if (Accept("kill")) {
            Expect("connection");
            return KillConnection{Unsigned("a connection id", std::numeric_limits<std::uint64_t>::max())};
        }
        if (Accept("describe")) {
            return Describe{.table = Name("a table name")};
        }
        if (Accept("recover")) {
            Expect("table");
            return RecoverTable{.table = Name("a table name")};
        }
        if (Accept("restore")) {
            Expect("table");
            RestoreTable restore{.table = Name("a table name"), .path = {}, .name = {}};
            Expect("from");
            const auto& path = Peek();
            if (path.kind != TokenKind::kText)
                Fail(path.column, "expected a quoted backup path, got " + Quote(path));
            restore.path = Take().text;
            restore.name = Accept("as") ? Name("a table name") : restore.table;
            return restore;
        }
        if (Accept("swap")) {
            Expect("tables");
            SwapTables swap{.first = Name("a table name"), .second = {}};
            ExpectToken(TokenKind::kComma, "a comma");
            swap.second = Name("a table name");
            return swap;
        }
        if (Accept("compact")) {
            Expect("tombstones");
            CompactTombstones compact{.table = Name("a table name")};
            Expect("before");
            compact.before = Unsigned("a revision", std::numeric_limits<std::uint64_t>::max());
            return compact;
        }
        if (Accept("flush")) {
            Expect("wal");
            return FlushWal{};
        }
        if (Accept("reload")) {
            Expect("tls");
            return ReloadTls{};
        }
        if (Accept("ping")) {
            return Ping{};
        }
        if (Accept("begin")) {
            Begin begin;
            if (Accept("read")) {
                Expect("only");
                begin.read_only = true;
            }
            return begin;
        }
        if (Accept("commit")) {
            return Commit{};
        }
        if (Accept("rollback")) {
            return Rollback{};
        }
        if (Accept("unwatch")) return Unwatch{};
        if (Accept("ack")) return Ack{.revision = Unsigned("a revision", std::numeric_limits<std::uint64_t>::max())};
        if (Accept("watch")) {
            Watch watch;
            // CATALOG is a keyword here: a table named catalog cannot be watched.
            if (Accept("catalog")) {
                watch.catalog = true;
                return watch;
            }
            watch.table = Name("a table name");
            while (true) {
                const auto column = Peek().column;
                if (Accept("slot")) {
                    if (watch.slot) Fail(column, "SLOT is given twice");
                    watch.slot = SlotName();
                } else if (FeedFilter(watch)) {
                } else if (Accept("takeover")) {
                    if (watch.takeover) Fail(column, "TAKEOVER is given twice");
                    watch.takeover = true;
                } else if (Accept("ack")) {
                    if (watch.ack_timeout_ms) Fail(column, "ACK TIMEOUT is given twice");
                    Expect("timeout");
                    watch.ack_timeout_ms = Unsigned("an ACK timeout", std::numeric_limits<std::uint64_t>::max());
                    if (*watch.ack_timeout_ms == 0U) Fail(column, "ACK TIMEOUT must be positive");
                } else if (Accept("after")) {
                    if (watch.after) Fail(column, "AFTER is given twice");
                    // AFTER consumed the lookahead. Read opaque hex without numeric lexing.
                    while (at_ < line_.size() && (line_[at_] == ' ' || line_[at_] == '\t')) ++at_;
                    const auto start = at_;
                    while (at_ < line_.size() && line_[at_] != ' ' && line_[at_] != '\t') ++at_;
                    const auto hex = line_.substr(start, at_ - start);
                    if (hex.size() != 32U || !std::all_of(hex.begin(), hex.end(), [](unsigned char c) {
                        return std::isxdigit(c) != 0;
                    })) Fail(start + 1U, "epoch must be 32 hex digits");
                    FeedPosition position;
                    const auto digit = [](char c) { return c <= '9' ? c - '0' : (c | 32) - 'a' + 10; };
                    for (std::size_t i = 0; i < position.epoch.size(); ++i)
                        position.epoch[i] = static_cast<std::uint8_t>(digit(hex[i * 2U]) * 16 + digit(hex[i * 2U + 1U]));
                    position.revision = Unsigned("a revision", std::numeric_limits<std::uint64_t>::max());
                    watch.after = position;
                } else break;
            }
            if (watch.ack_timeout_ms && !watch.slot) Fail(1, "ACK TIMEOUT requires SLOT");
            if (watch.takeover && !watch.slot) Fail(1, "TAKEOVER requires SLOT");
            return watch;
        }
        if (Accept("scan")) {
            Expect("chunks");
            Expect("from");
            ScanChunks scan;
            scan.table = Name("a table name");
            while (true) {
                const auto column = Peek().column;
                if (Accept("after")) {
                    if (scan.after) Fail(column, "AFTER is given twice");
                    std::int64_t x = 0, y = 0;
                    scan.dims = Point(x, y, scan.after_z, true);
                    scan.after = std::make_pair(x, y);
                } else if (Accept("limit")) {
                    if (scan.limit) Fail(column, "LIMIT is given twice");
                    scan.limit = Unsigned("a limit", std::numeric_limits<std::uint64_t>::max());
                } else if (Accept("since")) {
                    if (scan.since) Fail(column, "SINCE is given twice");
                    scan.since = Unsigned("a revision", std::numeric_limits<std::uint64_t>::max());
                } else if (Accept("with")) {
                    if (scan.with_data) Fail(column, "WITH DATA is given twice");
                    Expect("data");
                    scan.with_data = true;
                } else if (Accept("columns")) {
                    if (!scan.columns.empty()) Fail(column, "COLUMNS is given twice");
                    scan.columns = FeedColumns();
                } else if (Accept("where")) {
                    if (scan.where) Fail(column, "WHERE is given twice");
                    scan.where = Where();
                } else break;
            }
            if (!scan.columns.empty() && !scan.with_data) Fail(1, "COLUMNS requires WITH DATA");
            if (scan.where && scan.since && !scan.with_data) Fail(1, "WHERE with SINCE requires WITH DATA");
            return scan;
        }
        throw UnknownCommandError("column " + std::to_string(first.column) + ": unknown statement " +
                                  Quote(first) + StatementSuggestion(first.text));
    }

    std::string_view line_;
    std::size_t at_ = 0;
    std::deque<Token> tokens_;
    std::size_t next_ = 0;
    std::set<std::size_t> parameters_;
};

}  // namespace

bool MayHaveParameters(std::string_view line) noexcept {
    return CountParameterTokens(line) != 0U;
}

std::size_t CountParameterTokens(std::string_view line) noexcept {
    std::size_t count = 0U;
    char quote = 0;
    for (std::size_t at = 0U; at < line.size(); ++at) {
        const char c = line[at];
        if (quote != 0) {
            if (c == quote) {
                if (at + 1U < line.size() && line[at + 1U] == quote) ++at;
                else quote = 0;
            }
        } else if (c == '\'' || c == '"') {
            quote = c;
        } else if (c == '$' && at + 1U < line.size() && line[at + 1U] >= '0' && line[at + 1U] <= '9') {
            ++count;
            while (at + 1U < line.size() && line[at + 1U] >= '0' && line[at + 1U] <= '9') ++at;
        }
    }
    return count;
}

Parsed Parse(std::string_view line) {
    return Parser(line).Statement();
}

std::string AggregateFunctionName(const AggregateFunction& function) {
    using Kind = AggregateFunction::Kind;
    const char* name = function.kind == Kind::kCount ? "count" : function.kind == Kind::kSum ? "sum"
        : function.kind == Kind::kMin ? "min" : function.kind == Kind::kMax ? "max" : "avg";
    return std::string(name) + "(" + (function.kind == Kind::kCount ? std::string("*") : function.column) + ")";
}

HelloRequest ParseHello(std::string_view line) {
    while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) line.remove_suffix(1);
    return Parser(line).Hello();
}

}  // namespace chunkdb::cql
