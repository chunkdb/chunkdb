#include "cql.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <deque>
#include <limits>
#include <locale>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>

namespace chunkdb::cql {

namespace {

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
};

struct Token {
    TokenKind kind = TokenKind::kEnd;
    // As written; for kText, kBytes and kBits the decoded contents.
    std::string text;
    // 1-based column of its first character.
    std::size_t column = 0;
};

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

// `text` with control bytes as \xHH, so an error reply stays one line.
[[nodiscard]] std::string Printable(std::string_view text) {
    std::string out;
    for (const char c : text) {
        const auto byte = static_cast<unsigned char>(c);
        if (byte < 0x20U || byte == 0x7fU) {
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

std::string StatementSuggestion(std::string_view word) {
    if (word.size() > 16U) {
        return {};
    }
    const auto lower = Lower(word);
    const std::array<std::string_view, 21U> keywords{
        "get", "set", "delete", "create", "alter", "drop", "show", "describe", "grant", "revoke",
        "begin", "commit", "rollback", "ping", "flush", "watch", "unwatch", "ack", "backup", "migrate",
        "scan"};
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
        return {};
    }
    std::string name(closest);
    std::transform(name.begin(), name.end(), name.begin(), [](char c) { return static_cast<char>(c - 'a' + 'A'); });
    return "; did you mean " + name + "?";
}

// Reads a quoted body starting after the opening quote at `at`; a doubled
// quote is one quote. Returns the index after the closing quote.
std::size_t ReadQuoted(std::string_view line, std::size_t at, std::size_t column, std::string* out) {
    while (true) {
        if (at >= line.size()) {
            Fail(column, "the quoted value has no closing quote");
        }
        if (line[at] == '\'') {
            if (at + 1 < line.size() && line[at + 1] == '\'') {
                out->push_back('\'');
                at += 2;
                continue;
            }
            return at + 1;
        }
        out->push_back(line[at++]);
    }
}

// Reads the token at `*position` and moves past it; the last token is kEnd.
[[nodiscard]] Token Lex(std::string_view line, std::size_t* position) {
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

[[nodiscard]] std::string Quote(const Token& token) {
    return token.kind == TokenKind::kEnd ? "the end of the statement" : "'" + Printable(token.text) + "'";
}

class Parser {
  public:
    explicit Parser(std::string_view line) : line_(line) {}

    [[nodiscard]] Parsed Statement() {
        Parsed parsed{.statement = ParseStatement(), .parameters = 0};
        if (Peek().kind != TokenKind::kEnd) {
            Fail(Peek().column, "expected the end of the statement, got " + Quote(Peek()));
        }
        if (!parameters_.empty()) {
            const std::size_t count = *parameters_.rbegin();
            if (parameters_.size() != count) {
                Fail(1, "parameters must be numbered $1 to $" + std::to_string(count) + " without gaps");
            }
            parsed.parameters = count;
        }
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
                const std::string text = Lower(token.text);
                double value = 0.0;
                if (text == "-inf") {
                    value = -std::numeric_limits<double>::infinity();
                } else {
                    std::istringstream input(text);
                    input.imbue(std::locale::classic());
                    input >> value;
                    if (input.fail() || input.peek() != std::char_traits<char>::eof()) {
                        Fail(token.column, token.text + " is not a number");
                    }
                }
                Take();
                return value;
            }
            case TokenKind::kText:
                return Text{.value = Take().text};
            case TokenKind::kBytes: {
                const std::string hex = Take().text;
                Bytes bytes;
                for (std::size_t i = 0; i < hex.size(); i += 2) {
                    bytes.value.push_back(static_cast<std::uint8_t>(std::stoi(hex.substr(i, 2), nullptr, 16)));
                }
                return bytes;
            }
            case TokenKind::kBits:
                return Bits{.digits = Take().text};
            case TokenKind::kParameter: {
                const std::size_t column = token.column;
                if (!parameter_allowed) {
                    Fail(column, "parameters are values of SET BLOCK and SET CHUNK");
                }
                std::size_t index = 0;
                const auto result = std::from_chars(token.text.data(), token.text.data() + token.text.size(), index);
                if (result.ec != std::errc() || index == 0U || index > kMaxParameters) {
                    Fail(column, "parameters are numbered from $1 to $" + std::to_string(kMaxParameters));
                }
                Take();
                if (!parameters_.insert(index).second) {
                    Fail(column, "$" + std::to_string(index) + " is used twice");
                }
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

    [[nodiscard]] std::optional<std::uint64_t> IfVersion() {
        if (!Accept("if")) {
            return std::nullopt;
        }
        Expect("version");
        return Unsigned("a version", std::numeric_limits<std::uint64_t>::max());
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
            } else {
                return definition;
            }
        }
    }

    [[nodiscard]] Option OptionAssignment() {
        Option option;
        option.name = Name("an option name");
        ExpectToken(TokenKind::kEquals, "=");
        option.value = Value(false);
        return option;
    }

    [[nodiscard]] std::pair<std::uint32_t, std::uint32_t> Dimensions() {
        const auto width = static_cast<std::uint32_t>(Unsigned("a width", std::numeric_limits<std::uint32_t>::max()));
        // `16 x 16`, or `16x16`, which lexes as 16 and the word x16.
        const Token& by = Peek();
        if (by.kind == TokenKind::kWord && by.text.size() > 1 && (by.text[0] == 'x' || by.text[0] == 'X') &&
            std::all_of(by.text.begin() + 1, by.text.end(), [](char c) { return IsDigit(c); })) {
            std::uint32_t height = 0;
            const auto result = std::from_chars(by.text.data() + 1, by.text.data() + by.text.size(), height);
            if (result.ec != std::errc()) {
                Fail(by.column, "the height is out of range");
            }
            Take();
            return {width, height};
        }
        Expect("x");
        const auto height = static_cast<std::uint32_t>(Unsigned("a height", std::numeric_limits<std::uint32_t>::max()));
        return {width, height};
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

    // GRANT <right> ON <table | *> TO <user>, or REVOKE ... FROM <user>.
    [[nodiscard]] GrantRight GrantOrRevoke(bool revoke) {
        GrantRight grant;
        grant.revoke = revoke;
        if (Accept("read")) {
            grant.right = Right::kRead;
        } else if (Accept("write")) {
            grant.right = Right::kWrite;
        } else if (Accept("admin")) {
            grant.right = Right::kAdmin;
        } else {
            Fail(Peek().column, "expected READ, WRITE or ADMIN, got " + Quote(Peek()));
        }
        Expect("on");
        if (AcceptToken(TokenKind::kStar)) {
            grant.table = kEveryTable;
        } else {
            grant.table = Name("a table name or *");
        }
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
            const auto end = line_.find_last_not_of(" \t");
            migration.text = std::string(line_.substr(start, end == std::string_view::npos ? 0U : end + 1U - start));
            return migration;
        }
        if (Accept("get")) {
            if (Accept("block")) {
                GetBlock get;
                get.x = Coordinate("x");
                get.y = Coordinate("y");
                Expect("from");
                get.table = Name("a table name");
                get.columns = Columns();
                return get;
            }
            if (Accept("chunk")) {
                GetChunk get;
                get.chunk_x = Coordinate("a chunk x");
                get.chunk_y = Coordinate("a chunk y");
                Expect("from");
                get.table = Name("a table name");
                get.columns = Columns();
                return get;
            }
            if (Accept("area")) {
                GetArea get;
                if (Accept("around")) {
                    get.around = true;
                    get.x0 = Coordinate("x");
                    get.y0 = Coordinate("y");
                    Expect("radius");
                    get.radius = static_cast<std::int64_t>(Unsigned("a radius", std::numeric_limits<std::int64_t>::max()));
                } else {
                    get.x0 = Coordinate("x");
                    get.y0 = Coordinate("y");
                    Expect("to");
                    get.x1 = Coordinate("x");
                    get.y1 = Coordinate("y");
                }
                Expect("from");
                get.table = Name("a table name");
                get.columns = Columns();
                return get;
            }
            Fail(Peek().column, "expected BLOCK, CHUNK or AREA, got " + Quote(Peek()));
        }
        if (Accept("set")) {
            if (Accept("block")) {
                SetBlock set;
                set.x = Coordinate("x");
                set.y = Coordinate("y");
                Expect("in");
                set.table = Name("a table name");
                do {
                    Assignment assignment;
                    assignment.column = Name("a column name");
                    ExpectToken(TokenKind::kEquals, "=");
                    assignment.value = Value(true);
                    set.values.push_back(std::move(assignment));
                } while (AcceptToken(TokenKind::kComma));
                set.if_version = IfVersion();
                return set;
            }
            if (Accept("chunk")) {
                SetChunk set;
                set.chunk_x = Coordinate("a chunk x");
                set.chunk_y = Coordinate("a chunk y");
                Expect("in");
                set.table = Name("a table name");
                const std::size_t column = Peek().column;
                const Literal state = Value(true);
                if (!std::holds_alternative<Parameter>(state)) {
                    Fail(column, "the chunk is sent as a parameter ($1)");
                }
                set.state = std::get<Parameter>(state);
                set.if_version = IfVersion();
                return set;
            }
            Fail(Peek().column, "expected BLOCK or CHUNK, got " + Quote(Peek()));
        }
        if (Accept("delete")) {
            Expect("block");
            DeleteBlock del;
            del.x = Coordinate("x");
            del.y = Coordinate("y");
            Expect("from");
            del.table = Name("a table name");
            del.if_version = IfVersion();
            return del;
        }
        if (Accept("create")) {
            if (Accept("slot")) {
                CreateSlot create;
                create.name = SlotName();
                Expect("on");
                create.table = Name("a table name");
                return create;
            }
            if (Accept("user")) {
                CreateUser create;
                create.user = Name("a user name");
                Expect("verifier");
                create.verifier = Verifier();
                if (Accept("manages")) {
                    Expect("users");
                    create.manages_users = true;
                }
                return create;
            }
            Expect("table");
            CreateTable create;
            create.table = Name("a table name");
            ExpectToken(TokenKind::kOpen, "(");
            do {
                create.columns.push_back(Definition());
            } while (AcceptToken(TokenKind::kComma));
            ExpectToken(TokenKind::kClose, ")");
            Expect("chunk");
            std::tie(create.chunk_width, create.chunk_height) = Dimensions();
            if (Accept("large")) {
                create.large = Dimensions();
            }
            if (Accept("with")) {
                do {
                    create.options.push_back(OptionAssignment());
                } while (AcceptToken(TokenKind::kComma));
            }
            return create;
        }
        if (Accept("alter")) {
            if (Accept("user")) {
                AlterUser alter;
                alter.user = Name("a user name");
                if (Accept("verifier")) {
                    alter.verifier = Verifier();
                } else {
                    const bool no = Accept("no");
                    Expect("manages");
                    Expect("users");
                    alter.manages_users = !no;
                }
                return alter;
            }
            Expect("table");
            AlterTable alter;
            alter.table = Name("a table name");
            if (Accept("add")) {
                Expect("column");
                alter.change = AddColumn{.column = Definition()};
            } else if (Accept("drop")) {
                Expect("column");
                alter.change = DropColumn{.column = Name("a column name")};
            } else if (Accept("rename")) {
                Expect("column");
                RenameColumn rename;
                rename.column = Name("a column name");
                Expect("to");
                rename.new_name = Name("a column name");
                alter.change = rename;
            } else if (Accept("alter")) {
                Expect("column");
                AlterColumnType change;
                change.column = Name("a column name");
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
                drop.name = SlotName();
                Expect("on");
                drop.table = Name("a table name");
                return drop;
            }
            if (Accept("user")) {
                return DropUser{.user = Name("a user name")};
            }
            Expect("table");
            return DropTable{.table = Name("a table name")};
        }
        if (Accept("grant")) {
            return GrantOrRevoke(false);
        }
        if (Accept("revoke")) {
            return GrantOrRevoke(true);
        }
        if (Accept("show")) {
            if (Accept("migrations")) return ShowMigrations{};
            if (Accept("slots")) {
                ShowSlots show;
                if (Accept("on")) show.table = Name("a table name");
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
            Fail(Peek().column, "expected TABLES, METRICS, USERS or SLOTS, got " + Quote(Peek()));
        }
        if (Accept("describe")) {
            return Describe{.table = Name("a table name")};
        }
        if (Accept("flush")) {
            Expect("wal");
            return FlushWal{};
        }
        if (Accept("ping")) {
            return Ping{};
        }
        if (Accept("begin")) {
            return Begin{};
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
            watch.table = Name("a table name");
            if (Accept("slot")) watch.slot = SlotName();
            if (Accept("area")) {
                FeedArea area;
                area.first.x = Coordinate("a chunk x");
                area.first.y = Coordinate("a chunk y");
                Expect("to");
                area.last.x = Coordinate("a chunk x");
                area.last.y = Coordinate("a chunk y");
                if (area.first.x > area.last.x || area.first.y > area.last.y)
                    Fail(1, "AREA bounds are reversed");
                watch.area = area;
            }
            if (Accept("after")) {
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
            }
            return watch;
        }
        if (Accept("scan")) {
            Expect("chunks");
            Expect("from");
            ScanChunks scan;
            scan.table = Name("a table name");
            if (Accept("after")) {
                const std::int64_t x = Coordinate("a chunk x");
                scan.after = std::make_pair(x, Coordinate("a chunk y"));
            }
            if (Accept("limit")) {
                scan.limit = Unsigned("a limit", std::numeric_limits<std::uint64_t>::max());
            }
            return scan;
        }
        Fail(first.column, "unknown statement " + Quote(first) + StatementSuggestion(first.text));
    }

    std::string_view line_;
    std::size_t at_ = 0;
    std::deque<Token> tokens_;
    std::size_t next_ = 0;
    std::set<std::size_t> parameters_;
};

}  // namespace

bool MayHaveParameters(std::string_view line) noexcept {
    // A doubled quote inside a quoted value closes and reopens it, which
    // leaves the same state.
    bool quoted = false;
    for (const char c : line) {
        if (c == '\'') {
            quoted = !quoted;
        } else if (c == '$' && !quoted) {
            return true;
        }
    }
    return false;
}

Parsed Parse(std::string_view line) {
    return Parser(line).Statement();
}

}  // namespace chunkdb::cql
