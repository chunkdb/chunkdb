#include "docs_check.hpp"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <memory>
#include <sstream>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>

#ifndef _WIN32
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

extern char** environ;
#endif

namespace chunkdb::docs_check {

namespace fs = std::filesystem;

namespace {

using Clock = std::chrono::steady_clock;

constexpr std::string_view kReplacement = "\xEF\xBF\xBD";

// Decodes one UTF-8 sequence at `i`; false for an invalid sequence.
bool DecodeOne(std::string_view s, std::size_t i, char32_t* cp, std::size_t* len) {
    const auto c = static_cast<unsigned char>(s[i]);
    if (c < 0x80) {
        *cp = c;
        *len = 1;
        return true;
    }
    std::size_t n = 0;
    char32_t value = 0;
    if ((c & 0xE0) == 0xC0) {
        n = 2;
        value = c & 0x1F;
    } else if ((c & 0xF0) == 0xE0) {
        n = 3;
        value = c & 0x0F;
    } else if ((c & 0xF8) == 0xF0) {
        n = 4;
        value = c & 0x07;
    } else {
        return false;
    }
    if (i + n > s.size()) return false;
    for (std::size_t k = 1; k < n; ++k) {
        const auto cc = static_cast<unsigned char>(s[i + k]);
        if ((cc & 0xC0) != 0x80) return false;
        value = (value << 6) | (cc & 0x3F);
    }
    if ((n == 2 && value < 0x80) || (n == 3 && value < 0x800) || (n == 4 && value < 0x10000) || value > 0x10FFFF ||
        (value >= 0xD800 && value <= 0xDFFF)) {
        return false;
    }
    *cp = value;
    *len = n;
    return true;
}

// Invalid UTF-8 bytes become U+FFFD, for diagnostics.
std::string Sanitize(std::string_view s) {
    std::string out;
    std::size_t i = 0;
    while (i < s.size()) {
        char32_t cp = 0;
        std::size_t len = 0;
        if (DecodeOne(s, i, &cp, &len)) {
            out.append(s.substr(i, len));
            i += len;
        } else {
            out.append(kReplacement);
            ++i;
        }
    }
    return out;
}

// Invalid UTF-8 is an error.
std::string RequireUtf8(std::string text) {
    (void)DecodeUtf8(text);
    return text;
}

bool StartsWith(std::string_view text, std::string_view prefix) {
    return text.size() >= prefix.size() && text.compare(0, prefix.size(), prefix) == 0;
}

bool EndsWith(std::string_view text, std::string_view suffix) {
    return text.size() >= suffix.size() && text.compare(text.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool StartsWithAt(const std::u32string& text, std::size_t pos, std::u32string_view prefix) {
    return pos <= text.size() && text.size() - pos >= prefix.size() && text.compare(pos, prefix.size(), prefix) == 0;
}

bool IsAsciiDigit(char c) { return c >= '0' && c <= '9'; }
bool IsAsciiLower(char c) { return c >= 'a' && c <= 'z'; }
bool IsLowerHex(char c) { return IsAsciiDigit(c) || (c >= 'a' && c <= 'f'); }

std::size_t LineNumber(std::string_view text, std::size_t pos) {
    return static_cast<std::size_t>(std::count(text.begin(), text.begin() + static_cast<std::ptrdiff_t>(pos), '\n')) +
           1;
}

std::string Join(const std::vector<std::string>& parts, std::string_view separator) {
    std::string out;
    for (std::size_t i = 0; i < parts.size(); ++i) {
        if (i) out.append(separator);
        out += parts[i];
    }
    return out;
}

// Lowercase for Latin-1, Latin Extended-A, Greek and Cyrillic; other code
// points are unchanged.
char32_t Lower(char32_t c) {
    if (c >= 'A' && c <= 'Z') return c + 32;
    if (c < 0x80) return c;
    if ((c >= 0xC0 && c <= 0xDE && c != 0xD7)) return c + 32;
    if (c >= 0x100 && c <= 0x17F && c != 0x130 && c != 0x131 && c != 0x138 && c != 0x149 && c != 0x17F) {
        if (c == 0x178) return 0xFF;
        const bool odd_upper = (c >= 0x139 && c <= 0x148) || (c >= 0x179 && c <= 0x17E);
        if (odd_upper ? (c % 2 == 1) : (c % 2 == 0)) return c + 1;
        return c;
    }
    if (c >= 0x391 && c <= 0x3AB && c != 0x3A2) return c + 32;
    if (c >= 0x410 && c <= 0x42F) return c + 32;
    if (c >= 0x400 && c <= 0x40F) return c + 80;
    return c;
}

std::string ReadBytes(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot read " + path.string());
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (in.bad()) throw std::runtime_error("cannot read " + path.string());
    return data;
}

// Absolute, symlinks resolved, missing tails normalized lexically.
fs::path Resolve(const fs::path& path) { return fs::weakly_canonical(fs::absolute(path)); }

std::vector<std::string> Parts(const fs::path& path) {
    std::vector<std::string> parts;
    for (const auto& part : path) parts.push_back(part.string());
    return parts;
}

// Path ordering compares components, not the joined string.
void SortPaths(std::vector<fs::path>& paths) {
    std::sort(paths.begin(), paths.end(),
              [](const fs::path& a, const fs::path& b) { return Parts(a) < Parts(b); });
}

// Every *.md entry below `directory`, sorted.
std::vector<fs::path> MarkdownUnder(const fs::path& directory) {
    std::vector<fs::path> pages;
    for (const auto& entry : fs::recursive_directory_iterator(directory)) {
        if (EndsWith(entry.path().filename().string(), ".md")) pages.push_back(entry.path());
    }
    SortPaths(pages);
    return pages;
}

std::string RelativeTo(const fs::path& path, const fs::path& root) {
    const fs::path relative = path.lexically_relative(root);
    if (relative.empty() || *relative.begin() == "..") {
        throw std::runtime_error("'" + path.string() + "' is not in the subpath of '" + root.string() + "'");
    }
    return relative.generic_string();
}

// --- Normalization rules -----------------------------------------------------

class Scanner {
  public:
    Scanner(const std::string& text, std::size_t pos) : text_(&text), pos_(pos) {}

    std::size_t pos() const { return pos_; }

    bool Literal(std::string_view value) {
        if (text_->compare(pos_, value.size(), value) != 0) return false;
        pos_ += value.size();
        return true;
    }

    bool Digits() {
        const std::size_t begin = pos_;
        while (pos_ < text_->size() && IsAsciiDigit((*text_)[pos_])) ++pos_;
        return pos_ > begin;
    }

    // [1-9]\d*
    bool PositiveNumber() {
        if (pos_ >= text_->size() || (*text_)[pos_] < '1' || (*text_)[pos_] > '9') return false;
        ++pos_;
        Digits();
        return true;
    }

    // \r?\n
    bool Newline() { return Literal("\r\n") || Literal("\n"); }

    // [a-z][a-z0-9_]* or, with `underscore`, [a-z_][a-z0-9_]*
    bool Name(bool underscore) {
        if (pos_ >= text_->size()) return false;
        const char first = (*text_)[pos_];
        if (!IsAsciiLower(first) && !(underscore && first == '_')) return false;
        ++pos_;
        while (pos_ < text_->size() &&
               (IsAsciiLower((*text_)[pos_]) || IsAsciiDigit((*text_)[pos_]) || (*text_)[pos_] == '_')) {
            ++pos_;
        }
        return true;
    }

    // [0-9a-f]{32}
    bool Epoch() {
        for (std::size_t i = 0; i < 32; ++i) {
            if (pos_ + i >= text_->size() || !IsLowerHex((*text_)[pos_ + i])) return false;
        }
        pos_ += 32;
        return true;
    }

    // [^\r\n]+
    bool RestOfLine() {
        const std::size_t begin = pos_;
        while (pos_ < text_->size() && (*text_)[pos_] != '\r' && (*text_)[pos_] != '\n') ++pos_;
        return pos_ > begin;
    }

    void SpacesAndTabs() {
        while (pos_ < text_->size() && ((*text_)[pos_] == ' ' || (*text_)[pos_] == '\t')) ++pos_;
    }

  private:
    const std::string* text_;
    std::size_t pos_;
};

using Span = std::optional<std::pair<std::size_t, std::size_t>>;
using Rule = Span (*)(const std::string&, std::size_t);

// ^(Generated password: )[^\r\n]+
Span GeneratedPassword(const std::string& text, std::size_t line) {
    Scanner s(text, line);
    if (!s.Literal("Generated password: ")) return std::nullopt;
    const std::size_t begin = s.pos();
    if (!s.RestOfLine()) return std::nullopt;
    return std::pair{begin, s.pos()};
}

// ^(start |schema )[0-9a-f]{32}(?=:)
Span StreamEpoch(const std::string& text, std::size_t line) {
    Scanner s(text, line);
    if (!s.Literal("start ") && !s.Literal("schema ")) return std::nullopt;
    const std::size_t begin = s.pos();
    if (!s.Epoch()) return std::nullopt;
    const std::size_t end = s.pos();
    if (!s.Literal(":")) return std::nullopt;
    return std::pair{begin, end};
}

// ^([ \t]*\d+\) table = [a-z][a-z0-9_]*(?:, name = [a-z_][a-z0-9_]*)?, epoch = )[0-9a-f]{32}(?=,)
Span RowEpoch(const std::string& text, std::size_t line) {
    Scanner s(text, line);
    s.SpacesAndTabs();
    if (!s.Digits() || !s.Literal(") table = ") || !s.Name(false)) return std::nullopt;
    Scanner named = s;
    if (named.Literal(", name = ") && named.Name(true)) s = named;
    if (!s.Literal(", epoch = ")) return std::nullopt;
    const std::size_t begin = s.pos();
    if (!s.Epoch()) return std::nullopt;
    const std::size_t end = s.pos();
    if (!s.Literal(",")) return std::nullopt;
    return std::pair{begin, end};
}

// ^([1-9]\d*\)\r?\n  table = [a-z_][a-z0-9_]*\r?\n  name = [a-z_][a-z0-9_]*\r?\n  epoch = )[0-9a-f]{32}
// (?=\r?\n  acked = \d+\r?\n  retained_bytes = \d+\r?\n  lost = (?:true|false)
// \r?\n  lost_reason = [^\r\n]+\r?\n  filters:\r?\n)
Span SlotEpoch(const std::string& text, std::size_t line) {
    Scanner s(text, line);
    if (!s.PositiveNumber() || !s.Literal(")") || !s.Newline() || !s.Literal("  table = ") || !s.Name(true) ||
        !s.Newline() || !s.Literal("  name = ") || !s.Name(true) || !s.Newline() || !s.Literal("  epoch = ")) {
        return std::nullopt;
    }
    const std::size_t begin = s.pos();
    if (!s.Epoch()) return std::nullopt;
    const std::size_t end = s.pos();
    if (!s.Newline() || !s.Literal("  acked = ") || !s.Digits() || !s.Newline() ||
        !s.Literal("  retained_bytes = ") || !s.Digits() || !s.Newline() || !s.Literal("  lost = ") ||
        !(s.Literal("true") || s.Literal("false")) || !s.Newline() || !s.Literal("  lost_reason = ") ||
        !s.RestOfLine() || !s.Newline() || !s.Literal("  filters:") || !s.Newline()) {
        return std::nullopt;
    }
    return std::pair{begin, end};
}

// ^(table = [a-z_][a-z0-9_]*\r?\nversion = \d+\r?\nchunk = \d+ x \d+\r?\nlarge = \d+ x \d+\r?\nepoch = )
// [0-9a-f]{32}(?=\r?\nrevision = (?:\d+|NULL)\r?\ndims = \d+\r?\ncolumns:\r?\n)
Span TableEpoch(const std::string& text, std::size_t line) {
    Scanner s(text, line);
    const auto sides = [&]() {
        if (!s.Digits() || !s.Literal(" x ") || !s.Digits()) return false;
        return !s.Literal(" x ") || s.Digits();
    };
    if (!s.Literal("table = ") || !s.Name(true) || !s.Newline() || !s.Literal("version = ") || !s.Digits() ||
        !s.Newline() || !s.Literal("chunk = ") || !sides() ||
        !s.Newline() || !s.Literal("large = ") || !sides() ||
        !s.Newline() || !s.Literal("epoch = ")) {
        return std::nullopt;
    }
    const std::size_t begin = s.pos();
    if (!s.Epoch()) return std::nullopt;
    const std::size_t end = s.pos();
    if (!s.Newline() || !s.Literal("revision = ") || !(s.Digits() || s.Literal("NULL")) || !s.Newline() ||
        !s.Literal("dims = ") || !s.Digits() || !s.Newline() || !s.Literal("columns:") || !s.Newline()) {
        return std::nullopt;
    }
    return std::pair{begin, end};
}

Span ChunkTime(const std::string& text, std::size_t line) {
    Scanner s(text, line);
    s.SpacesAndTabs();
    const auto indent = text.substr(line, s.pos() - line);
    if (!s.Literal("version = ") || !(s.Digits() || s.Literal("NULL")) || !s.Newline() ||
        !s.Literal(indent) || !s.Literal("schema_version = ") || !s.Digits() || !s.Newline() ||
        !s.Literal(indent) || !s.Literal("commit_time_ms = ")) return std::nullopt;
    const auto begin = s.pos();
    if (!s.Digits()) return std::nullopt;
    const auto end = s.pos();
    if (!s.Newline() || !s.Literal(indent) || !s.Literal("present = ") || !s.Digits() ||
        !s.Literal(" of ") || !s.Digits() || !s.Literal(" blocks") || !s.Newline()) return std::nullopt;
    return std::pair{begin, end};
}

Span ScanEpoch(const std::string& text, std::size_t line) {
    Scanner s(text, line);
    if (s.Literal("chunks:")) {
        if (!s.Newline()) return std::nullopt;
        bool any = false;
        while (s.Literal("  ")) {
            if (!s.PositiveNumber() || !s.Literal(") [")) return std::nullopt;
            std::size_t values = 0U;
            do {
                s.Literal("-");
                if (!s.Digits()) return std::nullopt;
                ++values;
            } while (s.Literal(", "));
            if ((values != 3U && values != 4U) || !s.Literal("]") || !s.Newline()) return std::nullopt;
            any = true;
        }
        if (!any) return std::nullopt;
    } else if (!s.Literal("chunks = []") || !s.Newline()) return std::nullopt;
    if (!s.Literal("more = ") || !(s.Literal("true") || s.Literal("false")) ||
        !s.Newline() || !s.Literal("epoch = ")) return std::nullopt;
    const auto begin = s.pos();
    if (!s.Epoch()) return std::nullopt;
    const auto end = s.pos();
    if (!s.Newline() || !s.Literal("position = ") || !(s.Digits() || s.Literal("NULL")) ||
        !s.Newline() || !s.Literal("retained_deletion_floor = ") || !s.Digits() || !s.Newline()) return std::nullopt;
    return std::pair{begin, end};
}

// ^(change revision \d+ time_ms )-?\d+
Span ChangeTime(const std::string& text, std::size_t line) {
    Scanner s(text, line);
    if (!s.Literal("change revision ") || !s.Digits() || !s.Literal(" time_ms ")) return std::nullopt;
    const std::size_t begin = s.pos();
    s.Literal("-");
    if (!s.Digits()) return std::nullopt;
    return std::pair{begin, s.pos()};
}

// ^(\d+\) name = [a-z_][a-z0-9_]*, applied_ms = )\d+
Span MigrationTime(const std::string& text, std::size_t line) {
    Scanner s(text, line);
    if (!s.Digits() || !s.Literal(") name = ") || !s.Name(true) || !s.Literal(", applied_ms = ")) {
        return std::nullopt;
    }
    const std::size_t begin = s.pos();
    if (!s.Digits()) return std::nullopt;
    return std::pair{begin, s.pos()};
}

// ^(server_version = )[^\r\n]+
Span ServerVersion(const std::string& text, std::size_t line) {
    Scanner s(text, line);
    if (!s.Literal("server_version = ")) return std::nullopt;
    const std::size_t begin = s.pos();
    if (!s.RestOfLine()) return std::nullopt;
    return std::pair{begin, s.pos()};
}

// Replaces every match of `rule`; matches start at line starts and never
// overlap.
std::string ApplyRule(const std::string& text, Rule rule, std::string_view replacement) {
    std::string out;
    std::size_t copied = 0;
    std::size_t line = 0;
    while (line <= text.size()) {
        std::size_t from = line;
        if (const Span span = rule(text, line)) {
            out.append(text, copied, span->first - copied);
            out.append(replacement);
            copied = span->second;
            from = span->second;
        }
        const std::size_t newline = text.find('\n', from);
        if (newline == std::string::npos) break;
        line = newline + 1;
    }
    out.append(text, copied, std::string::npos);
    return out;
}

// --- Unified diff -----------------------------------------------------------------

struct Block {
    std::size_t a;
    std::size_t b;
    std::size_t size;
    bool operator<(const Block& other) const {
        return std::tie(a, b, size) < std::tie(other.a, other.b, other.size);
    }
};

struct Opcode {
    char tag;  // 'r'eplace, 'd'elete, 'i'nsert, 'e'qual
    std::size_t i1, i2, j1, j2;
};

// Ratcliff/Obershelp matching: the longest matching block first, then the
// same on both sides of it. Lines occurring more than 1% + 1 times in a long
// `b` (200 lines or more) cannot start a match.
class SequenceMatcher {
  public:
    SequenceMatcher(const std::vector<std::string>& a, const std::vector<std::string>& b) : a_(a), b_(b) {
        for (std::size_t j = 0; j < b.size(); ++j) b2j_[b[j]].push_back(j);
        if (b.size() >= 200) {
            const std::size_t ntest = b.size() / 100 + 1;
            for (auto it = b2j_.begin(); it != b2j_.end();) {
                if (it->second.size() > ntest) {
                    it = b2j_.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }

    std::vector<Opcode> Opcodes() const {
        std::vector<Opcode> answer;
        std::size_t i = 0;
        std::size_t j = 0;
        for (const Block& block : MatchingBlocks()) {
            char tag = 0;
            if (i < block.a && j < block.b) {
                tag = 'r';
            } else if (i < block.a) {
                tag = 'd';
            } else if (j < block.b) {
                tag = 'i';
            }
            if (tag) answer.push_back({tag, i, block.a, j, block.b});
            i = block.a + block.size;
            j = block.b + block.size;
            if (block.size) answer.push_back({'e', block.a, i, block.b, j});
        }
        return answer;
    }

  private:
    Block LongestMatch(std::size_t alo, std::size_t ahi, std::size_t blo, std::size_t bhi) const {
        std::size_t besti = alo;
        std::size_t bestj = blo;
        std::size_t bestsize = 0;
        std::unordered_map<std::size_t, std::size_t> j2len;
        for (std::size_t i = alo; i < ahi; ++i) {
            std::unordered_map<std::size_t, std::size_t> newj2len;
            const auto found = b2j_.find(a_[i]);
            if (found != b2j_.end()) {
                for (const std::size_t j : found->second) {
                    if (j < blo) continue;
                    if (j >= bhi) break;
                    std::size_t k = 1;
                    if (j > 0) {
                        const auto previous = j2len.find(j - 1);
                        if (previous != j2len.end()) k = previous->second + 1;
                    }
                    newj2len[j] = k;
                    if (k > bestsize) {
                        besti = i - k + 1;
                        bestj = j - k + 1;
                        bestsize = k;
                    }
                }
            }
            j2len = std::move(newj2len);
        }
        while (besti > alo && bestj > blo && a_[besti - 1] == b_[bestj - 1]) {
            --besti;
            --bestj;
            ++bestsize;
        }
        while (besti + bestsize < ahi && bestj + bestsize < bhi && a_[besti + bestsize] == b_[bestj + bestsize]) {
            ++bestsize;
        }
        return {besti, bestj, bestsize};
    }

    std::vector<Block> MatchingBlocks() const {
        std::vector<std::array<std::size_t, 4>> queue{{0, a_.size(), 0, b_.size()}};
        std::vector<Block> blocks;
        while (!queue.empty()) {
            const auto [alo, ahi, blo, bhi] = queue.back();
            queue.pop_back();
            const Block x = LongestMatch(alo, ahi, blo, bhi);
            if (x.size) {
                blocks.push_back(x);
                if (alo < x.a && blo < x.b) queue.push_back({alo, x.a, blo, x.b});
                if (x.a + x.size < ahi && x.b + x.size < bhi) queue.push_back({x.a + x.size, ahi, x.b + x.size, bhi});
            }
        }
        std::sort(blocks.begin(), blocks.end());
        std::vector<Block> merged;
        Block current{0, 0, 0};
        for (const Block& block : blocks) {
            if (current.a + current.size == block.a && current.b + current.size == block.b) {
                current.size += block.size;
            } else {
                if (current.size) merged.push_back(current);
                current = block;
            }
        }
        if (current.size) merged.push_back(current);
        merged.push_back({a_.size(), b_.size(), 0});
        return merged;
    }

    const std::vector<std::string>& a_;
    const std::vector<std::string>& b_;
    std::unordered_map<std::string, std::vector<std::size_t>> b2j_;
};

std::vector<std::vector<Opcode>> GroupedOpcodes(std::vector<Opcode> codes, std::size_t n) {
    if (codes.empty()) codes.push_back({'e', 0, 1, 0, 1});
    if (codes.front().tag == 'e') {
        Opcode& c = codes.front();
        c.i1 = std::max(c.i1, c.i2 >= n ? c.i2 - n : 0);
        c.j1 = std::max(c.j1, c.j2 >= n ? c.j2 - n : 0);
    }
    if (codes.back().tag == 'e') {
        Opcode& c = codes.back();
        c.i2 = std::min(c.i2, c.i1 + n);
        c.j2 = std::min(c.j2, c.j1 + n);
    }
    const std::size_t nn = n + n;
    std::vector<std::vector<Opcode>> groups;
    std::vector<Opcode> group;
    for (Opcode c : codes) {
        if (c.tag == 'e' && c.i2 - c.i1 > nn) {
            group.push_back({c.tag, c.i1, std::min(c.i2, c.i1 + n), c.j1, std::min(c.j2, c.j1 + n)});
            groups.push_back(std::move(group));
            group.clear();
            c.i1 = std::max(c.i1, c.i2 >= n ? c.i2 - n : 0);
            c.j1 = std::max(c.j1, c.j2 >= n ? c.j2 - n : 0);
        }
        group.push_back(c);
    }
    if (!group.empty() && !(group.size() == 1 && group.front().tag == 'e')) groups.push_back(std::move(group));
    return groups;
}

std::string FormatRange(std::size_t start, std::size_t stop) {
    std::size_t beginning = start + 1;
    const std::size_t length = stop - start;
    if (length == 1) return std::to_string(beginning);
    if (length == 0) beginning -= 1;
    return std::to_string(beginning) + "," + std::to_string(length);
}

// --- Links and anchors ------------------------------------------------------

std::u32string RemoveFences(const std::u32string& text) {
    // Removes ^```.*?^```\s*$ (multi-line, '.' matches newlines).
    const std::size_t n = text.size();
    auto next_line = [&](std::size_t pos) -> std::size_t {
        const std::size_t newline = text.find(U'\n', pos);
        return newline == std::u32string::npos ? n + 1 : newline + 1;
    };
    std::u32string out;
    std::size_t copied = 0;
    std::size_t line = 0;
    while (line <= n) {
        if (StartsWithAt(text, line, U"```")) {
            std::optional<std::size_t> end;
            for (std::size_t q = next_line(line); q <= n && !end; q = next_line(q)) {
                if (!StartsWithAt(text, q, U"```")) continue;
                const std::size_t after = q + 3;
                std::size_t run = after;
                while (run < n && IsSpace(text[run])) ++run;
                if (run == n) {
                    end = n;
                } else {
                    for (std::size_t k = run; k > after; --k) {
                        if (text[k - 1] == U'\n') {
                            end = k - 1;
                            break;
                        }
                    }
                }
            }
            if (end) {
                out.append(text, copied, line - copied);
                copied = *end;
                line = (*end > 0 && *end < n && text[*end - 1] == U'\n') ? *end : next_line(*end);
                continue;
            }
        }
        line = next_line(line);
    }
    if (copied < n) out.append(text, copied, std::u32string::npos);
    return out;
}

// \s*#*$ at `pos`: the end of the match, as the regex engine would choose it.
std::optional<std::size_t> HeadingTail(const std::u32string& text, std::size_t pos) {
    const std::size_t n = text.size();
    std::size_t spaces = pos;
    while (spaces < n && IsSpace(text[spaces])) ++spaces;
    for (std::size_t v = spaces + 1; v-- > pos;) {
        std::size_t hashes = v;
        while (hashes < n && text[hashes] == U'#') ++hashes;
        for (std::size_t w = hashes + 1; w-- > v;) {
            if (w == n || text[w] == U'\n') return w;
        }
    }
    return std::nullopt;
}

struct Heading {
    std::u32string title;
    std::size_t end;
};

// ^#{1,6}\s+(.+?)\s*#*$ at a line start, with the same backtracking.
std::optional<Heading> MatchHeading(const std::u32string& text, std::size_t line) {
    const std::size_t n = text.size();
    std::size_t hashes = 0;
    while (line + hashes < n && text[line + hashes] == U'#') ++hashes;
    for (std::size_t k = std::min<std::size_t>(hashes, 6); k >= 1; --k) {
        const std::size_t first = line + k;
        if (first >= n || !IsSpace(text[first])) continue;
        std::size_t spaces = first;
        while (spaces < n && IsSpace(text[spaces])) ++spaces;
        for (std::size_t start = spaces; start > first; --start) {
            for (std::size_t end = start; end < n && text[end] != U'\n';) {
                ++end;
                if (const auto tail = HeadingTail(text, end)) {
                    return Heading{text.substr(start, end - start), *tail};
                }
            }
        }
    }
    return std::nullopt;
}

// --- Processes ------------------------------------------------------------------

#ifndef _WIN32

void WriteBytes(const fs::path& path, const std::string& data) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out << data;
    out.close();
    if (!out) throw std::runtime_error("cannot write " + path.string());
}

std::string RStripNewlines(std::string text) {
    while (!text.empty() && text.back() == '\n') text.pop_back();
    return text;
}

using Environment = std::map<std::string, std::string>;

Environment CurrentEnvironment() {
    Environment env;
    for (char** entry = environ; *entry; ++entry) {
        const std::string value(*entry);
        const std::size_t equals = value.find('=');
        if (equals == std::string::npos || equals == 0) continue;
        env.emplace(value.substr(0, equals), value.substr(equals + 1));
    }
    return env;
}

// ['arg', ...], for timeout messages.
std::string Repr(const std::vector<std::string>& argv) {
    std::string out = "[";
    for (std::size_t i = 0; i < argv.size(); ++i) {
        if (i) out += ", ";
        out += "'" + ReplaceAll(ReplaceAll(argv[i], "\\", "\\\\"), "\n", "\\n") + "'";
    }
    return out + "]";
}

std::string Seconds(std::chrono::milliseconds timeout) {
    if (timeout.count() % 1000 == 0) return std::to_string(timeout.count() / 1000);
    std::ostringstream out;
    out << static_cast<double>(timeout.count()) / 1000.0;
    return out.str();
}

std::string FindExecutable(const std::string& name, const Environment& env) {
    if (name.find('/') != std::string::npos) return name;
    const auto path = env.find("PATH");
    const std::string search = path == env.end() ? "/bin:/usr/bin" : path->second;
    std::size_t begin = 0;
    while (true) {
        const std::size_t colon = search.find(':', begin);
        std::string directory = search.substr(begin, colon == std::string::npos ? std::string::npos : colon - begin);
        if (directory.empty()) directory = ".";
        const std::string candidate = directory + "/" + name;
        struct stat info {};
        if (::stat(candidate.c_str(), &info) == 0 && S_ISREG(info.st_mode) && ::access(candidate.c_str(), X_OK) == 0) {
            return candidate;
        }
        if (colon == std::string::npos) break;
        begin = colon + 1;
    }
    throw std::runtime_error("No such file or directory: '" + name + "'");
}

void SetCloseOnExec(int fd) {
    const int flags = ::fcntl(fd, F_GETFD);
    if (flags < 0 || ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0) {
        throw std::runtime_error(std::string("fcntl: ") + std::strerror(errno));
    }
}

void MakePipe(int fds[2]) {
    if (::pipe(fds) != 0) throw std::runtime_error(std::string("pipe: ") + std::strerror(errno));
    SetCloseOnExec(fds[0]);
    SetCloseOnExec(fds[1]);
}

void CloseFd(int& fd) {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

enum class Input { kInherit, kPipe };
enum class Output { kNull, kMerge, kSeparate };

struct Completed {
    int returncode = 0;
    std::string out;
    std::string err;
};

// A child process with optional stdin and output pipes.
class Child {
  public:
    Child(const std::vector<std::string>& argv, const Environment& env, Input input, Output output)
        : description_(Repr(argv)) {
        const std::string path = FindExecutable(argv.at(0), env);
        std::vector<char*> args;
        for (const auto& arg : argv) args.push_back(const_cast<char*>(arg.c_str()));
        args.push_back(nullptr);
        std::vector<std::string> entries;
        for (const auto& [key, value] : env) entries.push_back(key + "=" + value);
        std::vector<char*> envp;
        for (auto& entry : entries) envp.push_back(entry.data());
        envp.push_back(nullptr);

        int in_pipe[2] = {-1, -1};
        int out_pipe[2] = {-1, -1};
        int err_pipe[2] = {-1, -1};
        int exec_pipe[2] = {-1, -1};
        int null_fd = -1;
        auto close_all = [&] {
            for (int* fd : {&in_pipe[0], &in_pipe[1], &out_pipe[0], &out_pipe[1], &err_pipe[0], &err_pipe[1],
                            &exec_pipe[0], &exec_pipe[1], &null_fd}) {
                CloseFd(*fd);
            }
        };
        try {
            if (input == Input::kPipe) MakePipe(in_pipe);
            if (output == Output::kNull) {
                null_fd = ::open("/dev/null", O_WRONLY);
                if (null_fd < 0) throw std::runtime_error(std::string("/dev/null: ") + std::strerror(errno));
                SetCloseOnExec(null_fd);
            } else {
                MakePipe(out_pipe);
                if (output == Output::kSeparate) MakePipe(err_pipe);
            }
            // Reports an exec failure; closed by a successful exec.
            MakePipe(exec_pipe);
        } catch (...) {
            close_all();
            throw;
        }
        pid_ = ::fork();
        if (pid_ < 0) {
            const int error = errno;
            close_all();
            throw std::runtime_error(std::string("fork: ") + std::strerror(error));
        }
        if (pid_ == 0) {
            // Only async-signal-safe calls until exec. The child starts with
            // default SIGPIPE and SIGINT and no blocked signals.
            if (in_pipe[0] >= 0) ::dup2(in_pipe[0], 0);
            const int out_fd = output == Output::kNull ? null_fd : out_pipe[1];
            ::dup2(out_fd, 1);
            ::dup2(output == Output::kSeparate ? err_pipe[1] : out_fd, 2);
            ::signal(SIGPIPE, SIG_DFL);
            ::signal(SIGINT, SIG_DFL);
            sigset_t mask;
            sigemptyset(&mask);
            ::sigprocmask(SIG_SETMASK, &mask, nullptr);
            ::execve(path.c_str(), args.data(), envp.data());
            const int error = errno;
            const ssize_t written = ::write(exec_pipe[1], &error, sizeof(error));
            (void)written;
            ::_exit(127);
        }
        CloseFd(in_pipe[0]);
        CloseFd(out_pipe[1]);
        CloseFd(err_pipe[1]);
        CloseFd(exec_pipe[1]);
        CloseFd(null_fd);
        int exec_error = 0;
        ssize_t n = 0;
        while ((n = ::read(exec_pipe[0], &exec_error, sizeof(exec_error))) < 0 && errno == EINTR) {
        }
        CloseFd(exec_pipe[0]);
        if (n > 0) {
            close_all();
            int status = 0;
            while (::waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
            }
            returncode_ = 127;
            throw std::runtime_error(std::string(std::strerror(exec_error)) + ": '" + argv.at(0) + "'");
        }
        in_ = in_pipe[1];
        out_ = out_pipe[0];
        err_ = err_pipe[0];
    }

    Child(const Child&) = delete;
    Child& operator=(const Child&) = delete;

    // Error paths only: a child still running here is killed and reaped.
    ~Child() {
        CloseFd(in_);
        CloseFd(out_);
        CloseFd(err_);
        if (!returncode_) {
            ::kill(pid_, SIGKILL);
            int status = 0;
            while (::waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
            }
        }
    }

    int stdout_fd() const { return out_; }

    std::optional<int> Poll() {
        if (!returncode_) {
            int status = 0;
            const pid_t done = ::waitpid(pid_, &status, WNOHANG);
            if (done == pid_) Reaped(status);
        }
        return returncode_;
    }

    void Signal(int signal) {
        if (!Poll()) ::kill(pid_, signal);
    }

    void Kill() { Signal(SIGKILL); }

    void WriteStdin(std::string_view data) {
        while (!data.empty()) {
            const ssize_t n = ::write(in_, data.data(), data.size());
            if (n < 0) {
                if (errno == EINTR) continue;
                throw std::runtime_error(std::string("[Errno ") + std::to_string(errno) + "] " + std::strerror(errno));
            }
            data.remove_prefix(static_cast<std::size_t>(n));
        }
    }

    // Closes stdin, reads stdout and stderr to EOF, then waits for exit.
    Completed Communicate(std::optional<std::chrono::milliseconds> timeout) {
        const auto deadline = timeout ? std::optional(Clock::now() + *timeout) : std::nullopt;
        CloseFd(in_);
        while (out_ >= 0 || err_ >= 0) {
            pollfd fds[2];
            nfds_t count = 0;
            if (out_ >= 0) fds[count++] = {out_, POLLIN, 0};
            if (err_ >= 0) fds[count++] = {err_, POLLIN, 0};
            int wait_ms = -1;
            if (deadline) {
                const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(*deadline - Clock::now());
                if (remaining.count() <= 0) throw Timeout(*timeout);
                wait_ms = static_cast<int>(std::min<long long>(remaining.count(), 1000000));
            }
            const int ready = ::poll(fds, count, wait_ms);
            if (ready < 0) {
                if (errno == EINTR) continue;
                throw std::runtime_error(std::string("poll: ") + std::strerror(errno));
            }
            if (ready == 0) continue;
            for (nfds_t i = 0; i < count; ++i) {
                if (!fds[i].revents) continue;
                int& fd = fds[i].fd == out_ ? out_ : err_;
                std::string& sink = fds[i].fd == out_ ? out_data_ : err_data_;
                char buffer[65536];
                const ssize_t n = ::read(fd, buffer, sizeof(buffer));
                if (n > 0) {
                    sink.append(buffer, static_cast<std::size_t>(n));
                } else if (n == 0 || (errno != EINTR && errno != EAGAIN)) {
                    CloseFd(fd);
                }
            }
        }
        Wait(deadline, timeout);
        Completed result{*returncode_, std::move(out_data_), std::move(err_data_)};
        out_data_.clear();
        err_data_.clear();
        return result;
    }

  private:
    TimeoutError Timeout(std::chrono::milliseconds timeout) const {
        return TimeoutError("Command '" + description_ + "' timed out after " + Seconds(timeout) + " seconds");
    }

    void Reaped(int status) {
        if (WIFEXITED(status)) {
            returncode_ = WEXITSTATUS(status);
        } else if (WIFSIGNALED(status)) {
            returncode_ = -WTERMSIG(status);
        } else {
            returncode_ = -1;
        }
    }

    // Polls with delays growing to 50 ms.
    void Wait(std::optional<Clock::time_point> deadline, std::optional<std::chrono::milliseconds> timeout) {
        if (returncode_) return;
        if (!deadline) {
            int status = 0;
            while (::waitpid(pid_, &status, 0) < 0) {
                if (errno != EINTR) throw std::runtime_error(std::string("waitpid: ") + std::strerror(errno));
            }
            Reaped(status);
            return;
        }
        auto delay = std::chrono::microseconds(500);
        while (!Poll()) {
            const auto remaining = *deadline - Clock::now();
            if (remaining <= Clock::duration::zero()) throw Timeout(*timeout);
            delay = std::min({delay * 2, std::chrono::duration_cast<std::chrono::microseconds>(remaining),
                              std::chrono::microseconds(50000)});
            std::this_thread::sleep_for(delay);
        }
    }

    std::string description_;
    pid_t pid_ = -1;
    std::optional<int> returncode_;
    int in_ = -1;
    int out_ = -1;
    int err_ = -1;
    std::string out_data_;
    std::string err_data_;
};

// Runs a command to completion; it is killed on timeout.
Completed Run(const std::vector<std::string>& argv, const Environment& env, std::chrono::milliseconds timeout,
              Output output) {
    Child child(argv, env, Input::kInherit, output);
    try {
        return child.Communicate(timeout);
    } catch (const TimeoutError&) {
        child.Kill();
        (void)child.Communicate(std::nullopt);
        throw;
    }
}

// Reads a child's output until a predicate holds for everything read so far.
class Reader {
  public:
    explicit Reader(int fd) : fd_(fd) {}

    template <class Predicate>
    std::string Until(Predicate predicate, const std::string& label,
                      std::chrono::milliseconds limit = std::chrono::seconds(20)) {
        const auto deadline = Clock::now() + limit;
        while (!predicate(pending_)) {
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - Clock::now());
            if (remaining.count() <= 0) {
                throw TimeoutError("timeout waiting for " + label + ": " + Sanitize(pending_));
            }
            pollfd fd{fd_, POLLIN, 0};
            const int ready = ::poll(&fd, 1, static_cast<int>(remaining.count()));
            if (ready < 0) {
                if (errno == EINTR) continue;
                throw std::runtime_error(std::string("poll: ") + std::strerror(errno));
            }
            if (ready == 0) throw TimeoutError("timeout waiting for " + label + ": " + Sanitize(pending_));
            char buffer[65536];
            const ssize_t n = ::read(fd_, buffer, sizeof(buffer));
            if (n < 0) {
                if (errno == EINTR || errno == EAGAIN) continue;
                throw std::runtime_error(std::string("read: ") + std::strerror(errno));
            }
            if (n == 0) throw std::runtime_error("process ended waiting for " + label + ": " + Sanitize(pending_));
            pending_.append(buffer, static_cast<std::size_t>(n));
        }
        std::string result = std::move(pending_);
        pending_.clear();
        return RequireUtf8(std::move(result));
    }

  private:
    int fd_;
    std::string pending_;
};

// The prompt chunk\*?> $ ends the data, or precedes its one final newline.
bool PromptEndsAt(std::string_view data, std::size_t end) {
    const std::string_view head = data.substr(0, end);
    return EndsWith(head, "chunk> ") || EndsWith(head, "chunk*> ");
}

bool HasPrompt(const std::string& data) {
    return PromptEndsAt(data, data.size()) ||
           (!data.empty() && data.back() == '\n' && PromptEndsAt(data, data.size() - 1));
}

std::string StripPrompt(std::string reply) {
    for (std::string_view prompt : {"chunk> ", "chunk*> "}) {
        if (EndsWith(reply, prompt)) return reply.substr(0, reply.size() - prompt.size());
    }
    if (!reply.empty() && reply.back() == '\n') {
        const std::string_view head(reply.data(), reply.size() - 1);
        for (std::string_view prompt : {"chunk> ", "chunk*> "}) {
            if (EndsWith(head, prompt)) return std::string(head.substr(0, head.size() - prompt.size())) + "\n";
        }
    }
    return reply;
}

bool WatchStarted(const std::string& data) { return StartsWith(data, "start ") && EndsWith(data, "\n"); }

// change revision [^\n]+\n  block [^\n]+\n$ found anywhere, where $ is the end
// or the position before a final newline.
bool WatchChangeComplete(const std::string& data) {
    auto ends_at = [&](std::size_t end) {
        if (end < 2 || data[end - 1] != '\n') return false;
        const std::size_t block_newline = data.rfind('\n', end - 2);
        if (block_newline == std::string::npos) return false;
        const std::string_view block(data.data() + block_newline + 1, end - 1 - (block_newline + 1));
        if (!StartsWith(block, "  block ") || block.size() <= 8) return false;
        const std::size_t previous = block_newline == 0 ? std::string::npos : data.rfind('\n', block_newline - 1);
        const std::size_t line_start = previous == std::string::npos ? 0 : previous + 1;
        const std::string_view change(data.data() + line_start, block_newline - line_start);
        const std::size_t at = change.find("change revision ");
        return at != std::string_view::npos && at + 16 < change.size();
    };
    return ends_at(data.size()) || (!data.empty() && data.back() == '\n' && ends_at(data.size() - 1));
}

class Shell {
  public:
    Shell(const fs::path& cli, const Environment& env, std::chrono::milliseconds deadline = std::chrono::seconds(20))
        : process_({cli.string(), "--uri", env.at("CHUNKDB_URI"), "shell"}, env, Input::kPipe, Output::kMerge),
          reader_(process_.stdout_fd()) {
        try {
            reader_.Until(HasPrompt, "initial shell prompt", deadline);
        } catch (...) {
            // Reap the child and close its pipes before the error escapes.
            process_.Kill();
            (void)process_.Communicate(std::nullopt);
            throw;
        }
    }

    std::string Run(const std::string& code) {
        std::string output;
        for (const std::string& line : SplitLines(code)) {
            if (Strip(line).empty()) throw DocsError("checked CQL must not contain empty lines");
            process_.WriteStdin(line + "\n");
            output += StripPrompt(reader_.Until(HasPrompt, "reply to " + line));
        }
        return output;
    }

    void Close() {
        if (!process_.Poll()) process_.WriteStdin("exit\n");
        Completed result;
        try {
            result = process_.Communicate(std::chrono::seconds(10));
        } catch (const TimeoutError&) {
            process_.Kill();
            (void)process_.Communicate(std::nullopt);
            throw std::runtime_error("CLI shell did not exit");
        }
        if (result.returncode) throw std::runtime_error("CLI shell exited " + std::to_string(result.returncode));
    }

  private:
    Child process_;
    Reader reader_;
};

int ReservePort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error(std::string("socket: ") + std::strerror(errno));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = 0;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof(address);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
        ::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
        const int error = errno;
        ::close(fd);
        throw std::runtime_error(std::string("bind: ") + std::strerror(error));
    }
    ::close(fd);
    return ntohs(address.sin_port);
}

// TMPDIR, TEMP or TMP, else /tmp.
std::string TempRoot() {
    for (const char* name : {"TMPDIR", "TEMP", "TMP"}) {
        const char* value = std::getenv(name);
        std::error_code ec;
        if (value && *value && fs::is_directory(value, ec)) {
            std::string root = fs::absolute(value).lexically_normal().string();
            while (root.size() > 1 && root.back() == '/') root.pop_back();
            return root;
        }
    }
    return "/tmp";
}

class TempDir {
  public:
    explicit TempDir(const std::string& prefix) {
        std::string pattern = TempRoot() + "/" + prefix + "XXXXXX";
        if (!::mkdtemp(pattern.data())) throw std::runtime_error(std::string("mkdtemp: ") + std::strerror(errno));
        path_ = pattern;
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path_, ec);
    }
    const std::string& path() const { return path_; }

  private:
    std::string path_;
};

// A server with a fresh data directory, the CLI shells connected to it and at
// most one running watch.
class Fixture {
  public:
    Fixture(const fs::path& server, const fs::path& cli, const std::string& directory)
        : cli_(cli), env_(CurrentEnvironment()) {
        const std::string port = std::to_string(ReservePort());
        env_["CHUNKDB_ADMIN_USER"] = "admin";
        env_["CHUNKDB_ADMIN_PASSWORD"] = "docs-fixture-private-password";
        env_["CHUNKDB_PASSWORD"] = env_["CHUNKDB_ADMIN_PASSWORD"];
        env_.erase("CHUNKDB_ADMIN_PASSWORD_FILE");
        env_["CHUNKDB_URI"] = "chunk://admin@127.0.0.1:" + port + "/";
        env_["EXAMPLE_DIR"] = directory;
        env_["PATH"] = cli.parent_path().string() + ":" + env_.at("PATH");
        process_ = std::make_unique<Child>(
            std::vector<std::string>{server.string(), "--listen-uri", "chunk://127.0.0.1:" + port + "/", "--data-dir",
                                     directory + "/data", "--backup-dir", directory + "/backups", "--workers", "2"},
            env_, Input::kInherit, Output::kNull);
    }

    void Start() {
        // A real PING is the readiness signal. Startup races are bounded by an
        // explicit deadline; they do not weaken any production behavior.
        const auto deadline = Clock::now() + std::chrono::seconds(20);
        while (true) {
            Completed result = Run({cli_.string(), "--uri", env_.at("CHUNKDB_URI"), "PING"}, env_,
                                   std::chrono::seconds(5), Output::kSeparate);
            const std::string out = TranslateNewlines(result.out);
            const std::string err = TranslateNewlines(Sanitize(result.err));
            if (result.returncode == 0 && out == "PONG\n") return;
            if (process_->Poll() || Clock::now() >= deadline) {
                throw std::runtime_error("server did not answer readiness PING: " + err);
            }
            std::string lower = err;
            std::transform(lower.begin(), lower.end(), lower.begin(),
                           [](char c) { return c >= 'A' && c <= 'Z' ? static_cast<char>(c + 32) : c; });
            if (lower.find("connection refused") == std::string::npos) {
                throw std::runtime_error("server readiness failed: " + err);
            }
        }
    }

    std::string Execute(const std::string& kind, const std::string& session, const std::string& code) {
        if (kind == "cql") {
            auto found = std::find_if(shells_.begin(), shells_.end(),
                                      [&](const auto& entry) { return entry.first == session; });
            if (found == shells_.end()) {
                shells_.emplace_back(session, std::make_unique<Shell>(cli_, env_));
                found = std::prev(shells_.end());
            }
            return found->second->Run(code);
        }
        if (kind == "shell") {
            Completed result =
                Run({"bash", "-eu", "-o", "pipefail", "-c", code}, env_, std::chrono::seconds(20), Output::kMerge);
            const std::string out = TranslateNewlines(RequireUtf8(result.out));
            if (result.returncode) throw std::runtime_error("example shell failed: " + out);
            return out;
        }
        if (watch_) throw DocsError("only one active watch example per page is supported");
        watch_ = std::make_unique<Child>(std::vector<std::string>{"bash", "-c", "exec " + code}, env_, Input::kInherit,
                                         Output::kMerge);
        watch_reader_ = std::make_unique<Reader>(watch_->stdout_fd());
        if (kind == "replay") return FinishWatch();
        return watch_reader_->Until(WatchStarted, "watch start");
    }

    std::string FinishWatch() {
        // Read the complete change, including its after row, before stopping.
        // Checked watches use one-block changes, whose row completes the event.
        std::string output = watch_reader_->Until(WatchChangeComplete, "watch change");
        watch_->Signal(SIGINT);
        Completed tail = watch_->Communicate(std::chrono::seconds(10));
        if (tail.returncode) throw std::runtime_error("watch did not stop successfully: " + Sanitize(tail.out));
        watch_reader_.reset();
        watch_.reset();
        return output + RequireUtf8(tail.out);
    }

    void Close() {
        std::vector<std::string> errors;
        for (auto& [session, shell] : shells_) {
            try {
                shell->Close();
            } catch (const std::exception& error) {
                errors.push_back(error.what());
            }
        }
        if (watch_ && !watch_->Poll()) {
            watch_->Signal(SIGINT);
            try {
                (void)watch_->Communicate(std::chrono::seconds(10));
            } catch (const TimeoutError&) {
                watch_->Kill();
                (void)watch_->Communicate(std::nullopt);
                errors.push_back("watch failed to stop during cleanup");
            }
        }
        process_->Signal(SIGTERM);
        Completed result;
        try {
            result = process_->Communicate(std::chrono::seconds(10));
        } catch (const TimeoutError&) {
            process_->Kill();
            result = process_->Communicate(std::nullopt);
            errors.push_back("server failed to stop during cleanup");
        }
        if (result.returncode) errors.push_back("server exited " + std::to_string(result.returncode));
        if (!errors.empty()) throw std::runtime_error(Join(errors, "; "));
    }

  private:
    fs::path cli_;
    Environment env_;
    std::unique_ptr<Child> process_;
    std::vector<std::pair<std::string, std::unique_ptr<Shell>>> shells_;
    std::unique_ptr<Child> watch_;
    std::unique_ptr<Reader> watch_reader_;
};

// Runs `body`, then closes the fixture; both failures are reported.
template <class Body>
void WithFixture(Fixture& fixture, Body body) {
    try {
        body();
    } catch (const std::exception& error) {
        try {
            fixture.Close();
        } catch (const std::exception& cleanup) {
            throw std::runtime_error(std::string(error.what()) + "\nduring cleanup: " + cleanup.what());
        }
        throw;
    }
    fixture.Close();
}

bool IsSetBlock(const std::string& code) {
    return StartsWith(code, "SET BLOCK ") && code.size() > 10 && code.find('\n') == std::string::npos;
}

std::size_t CheckPage(const fs::path& page, const fs::path& root, const fs::path& server, const fs::path& cli,
                      bool record) {
    std::string source = ReadText(page);
    std::vector<Example> blocks;
    try {
        blocks = Examples(source);
    } catch (const DocsError& error) {
        throw DocsError(page.string() + ": " + error.what());
    }
    if (blocks.empty()) return 0;
    TempDir directory("chunkdb-docs-");
    Fixture fixture(server, cli, directory.path());
    WithFixture(fixture, [&] {
        fixture.Start();
        std::vector<std::string> results;
        std::optional<std::size_t> watcher;
        for (std::size_t index = 0; index < blocks.size(); ++index) {
            const Example& example = blocks[index];
            results.push_back(fixture.Execute(example.kind, example.session, example.code));
            if (example.kind == "watch") {
                watcher = index;
            } else if (watcher) {
                if (!IsSetBlock(example.code)) throw DocsError("a watch example requires one following SET BLOCK");
                results[*watcher] += fixture.FinishWatch();
                watcher.reset();
            }
        }
        if (watcher) throw DocsError("watch requires a following checked change statement");
        std::vector<std::tuple<std::size_t, std::size_t, std::string>> replacements;
        for (std::size_t index = 0; index < blocks.size(); ++index) {
            const std::string actual =
                RStripNewlines(Normalize(ReplaceAll(results[index], directory.path(), "<example-dir>")));
            const std::string& expected = blocks[index].output.code;
            if (record) {
                replacements.emplace_back(blocks[index].output.code_start, blocks[index].output.code_end, actual);
            } else if (actual != expected) {
                const auto difference = UnifiedDiff(SplitLines(expected), SplitLines(actual), "expected", "actual");
                throw std::runtime_error(RelativeTo(page, root) + " example " + std::to_string(index + 1) + "\n" +
                                         Join(difference, "\n"));
            }
        }
        if (record) {
            for (auto it = replacements.rbegin(); it != replacements.rend(); ++it) {
                const auto& [start, end, replacement] = *it;
                source = source.substr(0, start) + replacement + source.substr(end);
            }
            WriteBytes(page, source);
        }
    });
    return blocks.size();
}

#endif  // _WIN32

}  // namespace

// --- Text helpers -----------------------------------------------------------------

std::u32string DecodeUtf8(std::string_view text) {
    std::u32string out;
    out.reserve(text.size());
    std::size_t i = 0;
    while (i < text.size()) {
        char32_t cp = 0;
        std::size_t len = 0;
        if (!DecodeOne(text, i, &cp, &len)) {
            throw std::runtime_error("'utf-8' codec can't decode byte at position " + std::to_string(i));
        }
        out.push_back(cp);
        i += len;
    }
    return out;
}

std::string EncodeUtf8(std::u32string_view text) {
    std::string out;
    out.reserve(text.size());
    for (const char32_t c : text) {
        if (c < 0x80) {
            out.push_back(static_cast<char>(c));
        } else if (c < 0x800) {
            out.push_back(static_cast<char>(0xC0 | (c >> 6)));
            out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
        } else if (c < 0x10000) {
            out.push_back(static_cast<char>(0xE0 | (c >> 12)));
            out.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
        } else {
            out.push_back(static_cast<char>(0xF0 | (c >> 18)));
            out.push_back(static_cast<char>(0x80 | ((c >> 12) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
        }
    }
    return out;
}

// Unicode whitespace, also \s in the documented patterns.
bool IsSpace(char32_t c) {
    return (c >= 0x09 && c <= 0x0D) || (c >= 0x1C && c <= 0x20) || c == 0x85 || c == 0xA0 || c == 0x1680 ||
           (c >= 0x2000 && c <= 0x200A) || c == 0x2028 || c == 0x2029 || c == 0x202F || c == 0x205F ||
           c == 0x3000;
}

// \w: letters, digits and numerals, or '_'. Exact for ASCII, Latin-1 and the
// punctuation and symbol blocks; other scripts count as letters.
bool IsWord(char32_t c) {
    if (c < 0x80) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
    }
    if (c < 0x100) {
        return c == 0xAA || c == 0xB2 || c == 0xB3 || c == 0xB5 || c == 0xB9 || c == 0xBA ||
               (c >= 0xBC && c <= 0xBE) || (c >= 0xC0 && c <= 0xFF && c != 0xD7 && c != 0xF7);
    }
    if (c >= 0x2B0 && c <= 0x2FF) {
        return (c <= 0x2C1) || (c >= 0x2C6 && c <= 0x2D1) || (c >= 0x2E0 && c <= 0x2E4) || c == 0x2EC ||
               c == 0x2EE;
    }
    if (c >= 0x300 && c <= 0x36F) return false;
    if (c == 0x375 || c == 0x37E || c == 0x384 || c == 0x385 || c == 0x387 || c == 0x3F6) return false;
    if (c >= 0x482 && c <= 0x489) return false;
    if (c >= 0x2070 && c <= 0x209F) {
        return c == 0x2070 || c == 0x2071 || (c >= 0x2074 && c <= 0x2079) || c == 0x207F ||
               (c >= 0x2080 && c <= 0x2089) || (c >= 0x2090 && c <= 0x209C);
    }
    if (c >= 0x2150 && c <= 0x2189) return true;
    if ((c >= 0x2460 && c <= 0x249B) || (c >= 0x24EA && c <= 0x24FF)) return true;
    if (c >= 0x2000 && c <= 0x2BFF) return false;
    if (c >= 0x2E00 && c <= 0x2E7F) return false;
    if (c >= 0x3000 && c <= 0x303F) {
        return (c >= 0x3005 && c <= 0x3007) || (c >= 0x3021 && c <= 0x3029) || (c >= 0x3031 && c <= 0x3035) ||
               (c >= 0x3038 && c <= 0x303C);
    }
    if ((c >= 0xE000 && c <= 0xF8FF) || (c >= 0xFE00 && c <= 0xFE0F) || (c >= 0xFE30 && c <= 0xFE4F) ||
        (c >= 0xFF00 && c <= 0xFF0F) || (c >= 0xFFF0 && c <= 0xFFFF) || (c >= 0x1F000 && c <= 0x1FAFF)) {
        return false;
    }
    return true;
}

std::string Strip(std::string_view text) {
    const std::u32string decoded = DecodeUtf8(text);
    std::size_t begin = 0;
    std::size_t end = decoded.size();
    while (begin < end && IsSpace(decoded[begin])) ++begin;
    while (end > begin && IsSpace(decoded[end - 1])) --end;
    return EncodeUtf8(std::u32string_view(decoded).substr(begin, end - begin));
}

// Lines split at \n, \r, \r\n, \v, \f, \x1c-\x1e, U+0085, U+2028 and U+2029;
// a final line break adds no empty line.
std::vector<std::string> SplitLines(std::string_view text) {
    const std::u32string decoded = DecodeUtf8(text);
    std::vector<std::string> lines;
    std::u32string line;
    for (std::size_t i = 0; i < decoded.size(); ++i) {
        const char32_t c = decoded[i];
        const bool boundary = c == U'\n' || c == U'\r' || c == 0x0B || c == 0x0C || c == 0x1C || c == 0x1D ||
                              c == 0x1E || c == 0x85 || c == 0x2028 || c == 0x2029;
        if (!boundary) {
            line.push_back(c);
            continue;
        }
        if (c == U'\r' && i + 1 < decoded.size() && decoded[i + 1] == U'\n') ++i;
        lines.push_back(EncodeUtf8(line));
        line.clear();
    }
    if (!line.empty()) lines.push_back(EncodeUtf8(line));
    return lines;
}

std::string TranslateNewlines(std::string_view text) {
    std::string out;
    out.reserve(text.size());
    for (std::size_t i = 0; i < text.size(); ++i) {
        if (text[i] == '\r') {
            out.push_back('\n');
            if (i + 1 < text.size() && text[i + 1] == '\n') ++i;
        } else {
            out.push_back(text[i]);
        }
    }
    return out;
}

std::string ReadText(const fs::path& path) { return TranslateNewlines(RequireUtf8(ReadBytes(path))); }

std::string ReplaceAll(std::string text, std::string_view from, std::string_view to) {
    if (from.empty()) return text;
    std::string out;
    std::size_t copied = 0;
    for (std::size_t at = text.find(from); at != std::string::npos; at = text.find(from, copied)) {
        out.append(text, copied, at - copied);
        out.append(to);
        copied = at + from.size();
    }
    out.append(text, copied, std::string::npos);
    return out;
}

// --- Checked examples ---------------------------------------------------------------

std::vector<Example> Examples(const std::string& text) {
    struct Opening {
        std::size_t start;
        std::size_t end;
        std::string rest;
    };
    auto line_error = [&](const std::string& what, std::size_t start) {
        return DocsError(what + " at line " + std::to_string(LineNumber(text, start)));
    };
    std::vector<Fence> fences;
    std::optional<Opening> opening;
    for (std::size_t line = 0; line <= text.size();) {
        std::size_t end = text.find('\n', line);
        if (end == std::string::npos) end = text.size();
        if (text.compare(line, 3, "```") == 0) {
            const std::string rest = text.substr(line + 3, end - line - 3);
            const std::string tag = Strip(rest);
            if (!opening) {
                opening = Opening{line, end, rest};
            } else if (tag.empty()) {
                // The closing line holds only spaces and tabs after the
                // backticks, and the fence holds at least one line.
                const bool closing = rest.find_first_not_of(" \t") == std::string::npos;
                if (!closing || line < opening->end + 2) throw line_error("invalid fence", opening->start);
                Fence fence;
                fence.tag = opening->rest;
                fence.start = opening->start;
                fence.end = end;
                fence.code_start = opening->end + 1;
                fence.code_end = line - 1;
                fence.code = text.substr(fence.code_start, fence.code_end - fence.code_start);
                fences.push_back(std::move(fence));
                opening.reset();
            } else if (opening->rest.find("docs-check") != std::string::npos ||
                       tag.find("docs-check") != std::string::npos) {
                throw line_error("unclosed checked fence", opening->start);
            }
        }
        if (end == text.size()) break;
        line = end + 1;
    }
    if (opening) throw line_error("unclosed fence", opening->start);

    std::vector<Example> result;
    for (std::size_t i = 0; i < fences.size(); ++i) {
        const Fence& fence = fences[i];
        if (fence.tag.find("docs-check") == std::string::npos) continue;
        if (i + 1 == fences.size() || fences[i + 1].tag != "output") {
            throw DocsError("checked input must be followed by an output fence");
        }
        if (!Strip(std::string_view(text).substr(fence.end, fences[i + 1].start - fence.end)).empty()) {
            throw DocsError("checked input and output must be adjacent");
        }
        Example example;
        example.session = "main";
        constexpr std::string_view kCql = "cql docs-check";
        constexpr std::string_view kSession = " session=";
        if (fence.tag == kCql) {
            example.kind = "cql";
        } else if (StartsWith(fence.tag, std::string(kCql) + std::string(kSession)) &&
                   fence.tag.size() > kCql.size() + kSession.size() &&
                   std::all_of(fence.tag.begin() + static_cast<std::ptrdiff_t>(kCql.size() + kSession.size()),
                               fence.tag.end(), IsAsciiLower)) {
            example.kind = "cql";
            example.session = fence.tag.substr(kCql.size() + kSession.size());
        } else if (fence.tag == "sh docs-check-watch") {
            example.kind = "watch";
        } else if (fence.tag == "sh docs-check-replay") {
            example.kind = "replay";
        } else if (fence.tag == "sh docs-check-shell") {
            example.kind = "shell";
        } else {
            throw DocsError("unknown example tag: " + fence.tag);
        }
        example.code = fence.code;
        example.output = fences[i + 1];
        result.push_back(std::move(example));
    }
    return result;
}

std::string Normalize(std::string text) {
    // Only fields with documented volatility are replaced. Numeric data and
    // versions remain exact: each page runs against a fresh data directory.
    text = ApplyRule(text, GeneratedPassword, "<generated-password>");
    text = ApplyRule(text, StreamEpoch, "<epoch>");
    text = ApplyRule(text, RowEpoch, "<epoch>");
    text = ApplyRule(text, SlotEpoch, "<epoch>");
    text = ApplyRule(text, TableEpoch, "<epoch>");
    text = ApplyRule(text, ScanEpoch, "<epoch>");
    text = ApplyRule(text, ChunkTime, "<time-ms>");
    text = ApplyRule(text, ChangeTime, "<time-ms>");
    text = ApplyRule(text, MigrationTime, "<time-ms>");
    text = ApplyRule(text, ServerVersion, "<server-version>");
    return ReplaceAll(std::move(text), "\r\n", "\n");
}

std::vector<std::string> UnifiedDiff(const std::vector<std::string>& a, const std::vector<std::string>& b,
                                     const std::string& fromfile, const std::string& tofile) {
    std::vector<std::string> lines;
    const SequenceMatcher matcher(a, b);
    for (const auto& group : GroupedOpcodes(matcher.Opcodes(), 3)) {
        if (lines.empty()) {
            lines.push_back("--- " + fromfile);
            lines.push_back("+++ " + tofile);
        }
        lines.push_back("@@ -" + FormatRange(group.front().i1, group.back().i2) + " +" +
                        FormatRange(group.front().j1, group.back().j2) + " @@");
        for (const Opcode& code : group) {
            if (code.tag == 'e') {
                for (std::size_t i = code.i1; i < code.i2; ++i) lines.push_back(" " + a[i]);
                continue;
            }
            if (code.tag == 'r' || code.tag == 'd') {
                for (std::size_t i = code.i1; i < code.i2; ++i) lines.push_back("-" + a[i]);
            }
            if (code.tag == 'r' || code.tag == 'i') {
                for (std::size_t j = code.j1; j < code.j2; ++j) lines.push_back("+" + b[j]);
            }
        }
    }
    return lines;
}

// --- Links ------------------------------------------------------------------------

// !?\[[^\]]*\]\(([^\s)]+)(?:\s+"[^"]*")?\)
std::vector<Link> FindLinks(const std::u32string& text) {
    const std::size_t n = text.size();
    std::vector<Link> links;
    std::size_t pos = 0;
    while (pos < n) {
        std::size_t bracket = pos;
        if (text[bracket] == U'!' && bracket + 1 < n && text[bracket + 1] == U'[') ++bracket;
        std::optional<std::size_t> end;
        std::size_t url_start = 0;
        std::size_t url_end = 0;
        if (text[bracket] == U'[') {
            const std::size_t close = text.find(U']', bracket + 1);
            if (close != std::u32string::npos && close + 1 < n && text[close + 1] == U'(') {
                url_start = close + 2;
                url_end = url_start;
                while (url_end < n && !IsSpace(text[url_end]) && text[url_end] != U')') ++url_end;
                if (url_end > url_start && url_end < n) {
                    if (text[url_end] == U')') {
                        end = url_end + 1;
                    } else {
                        std::size_t quote = url_end;
                        while (quote < n && IsSpace(text[quote])) ++quote;
                        if (quote < n && text[quote] == U'"') {
                            const std::size_t closing = text.find(U'"', quote + 1);
                            if (closing != std::u32string::npos && closing + 1 < n && text[closing + 1] == U')') {
                                end = closing + 2;
                            }
                        }
                    }
                }
            }
        }
        if (!end) {
            ++pos;
            continue;
        }
        const std::u32string url = text.substr(url_start, url_end - url_start);
        links.push_back({EncodeUtf8(url), pos});
        pos = *end;
    }
    return links;
}

// Splits scheme, //netloc, path, ?query and #fragment.
SplitUrl UrlSplit(std::string_view input) {
    std::string url(input);
    std::size_t lead = 0;
    while (lead < url.size() && static_cast<unsigned char>(url[lead]) <= 0x20) ++lead;
    url.erase(0, lead);
    url.erase(std::remove_if(url.begin(), url.end(), [](char c) { return c == '\t' || c == '\r' || c == '\n'; }),
              url.end());
    SplitUrl result;
    const std::size_t colon = url.find(':');
    auto is_alpha = [](char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'); };
    if (colon != std::string::npos && colon > 0 && is_alpha(url[0])) {
        const bool valid = std::all_of(url.begin(), url.begin() + static_cast<std::ptrdiff_t>(colon), [&](char c) {
            return is_alpha(c) || IsAsciiDigit(c) || c == '+' || c == '-' || c == '.';
        });
        if (valid) {
            result.scheme = url.substr(0, colon);
            std::transform(result.scheme.begin(), result.scheme.end(), result.scheme.begin(),
                           [](char c) { return c >= 'A' && c <= 'Z' ? static_cast<char>(c + 32) : c; });
            url.erase(0, colon + 1);
        }
    }
    if (StartsWith(url, "//")) {
        std::size_t delimiter = url.find_first_of("/?#", 2);
        if (delimiter == std::string::npos) delimiter = url.size();
        result.netloc = url.substr(2, delimiter - 2);
        url.erase(0, delimiter);
        const bool open = result.netloc.find('[') != std::string::npos;
        const bool close = result.netloc.find(']') != std::string::npos;
        if (open != close) throw std::runtime_error("Invalid IPv6 URL");
    }
    if (const std::size_t hash = url.find('#'); hash != std::string::npos) {
        result.fragment = url.substr(hash + 1);
        url.erase(hash);
    }
    if (const std::size_t question = url.find('?'); question != std::string::npos) {
        result.query = url.substr(question + 1);
        url.erase(question);
    }
    result.path = url;
    return result;
}

// Decodes %XX escapes as UTF-8; invalid sequences become U+FFFD.
std::string Unquote(std::string_view text) {
    if (text.find('%') == std::string_view::npos) return std::string(text);
    auto hex = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    std::string bytes;
    for (std::size_t i = 0; i < text.size(); ++i) {
        if (text[i] == '%' && i + 2 < text.size() && hex(text[i + 1]) >= 0 && hex(text[i + 2]) >= 0) {
            bytes.push_back(static_cast<char>(hex(text[i + 1]) * 16 + hex(text[i + 2])));
            i += 2;
        } else {
            bytes.push_back(text[i]);
        }
    }
    return Sanitize(bytes);
}

std::set<std::string> Anchors(const std::string& source) {
    const std::u32string text = RemoveFences(DecodeUtf8(source));
    std::set<std::string> result;
    std::map<std::u32string, int> counts;
    const std::size_t n = text.size();
    std::size_t line = 0;
    while (line <= n) {
        std::size_t from = line;
        if (const auto heading = MatchHeading(text, line)) {
            std::u32string title;
            for (const char32_t c : heading->title) {
                const char32_t lower = Lower(c);
                if (lower == U' ') {
                    title.push_back(U'-');
                } else if (lower == 0x130) {
                    title.push_back(U'i');
                } else if (IsWord(lower) || lower == U'-') {
                    title.push_back(lower);
                }
            }
            const int number = counts[title]++;
            result.insert(EncodeUtf8(title) + (number ? "-" + std::to_string(number) : ""));
            from = heading->end;
            if (from > 0 && from <= n && text[from - 1] == U'\n') {
                line = from;
                continue;
            }
        }
        const std::size_t newline = text.find(U'\n', from);
        if (newline == std::u32string::npos) break;
        line = newline + 1;
    }
    return result;
}

int RunLinks(const fs::path& root_input, std::ostream& out, std::ostream& err) {
    const fs::path root = Resolve(root_input);
    std::vector<fs::path> pages{root / "README.md", root / "CONTRIBUTING.md", root / "CHANGELOG.md"};
    for (const auto& page : MarkdownUnder(root / "docs")) pages.push_back(page);
    std::map<fs::path, std::set<std::string>> anchors;
    std::vector<std::string> failures;
    std::size_t count = 0;
    for (const fs::path& page : pages) {
        const std::string source = ReadText(page);
        const std::u32string text = DecodeUtf8(source);
        for (const Link& link : FindLinks(text)) {
            const SplitUrl url = UrlSplit(link.url);
            if (!url.scheme.empty() || !url.netloc.empty()) continue;
            fs::path target = page;
            if (!url.path.empty()) {
                // Relative to the page; empty and '.' parts drop out.
                const std::string path = Unquote(url.path);
                fs::path joined = StartsWith(path, "/") ? fs::path("/") : page.parent_path();
                std::size_t begin = 0;
                while (begin <= path.size()) {
                    std::size_t slash = path.find('/', begin);
                    if (slash == std::string::npos) slash = path.size();
                    const std::string part = path.substr(begin, slash - begin);
                    if (!part.empty() && part != ".") joined /= part;
                    begin = slash + 1;
                }
                target = Resolve(joined);
            }
            const auto before = text.begin() + static_cast<std::ptrdiff_t>(link.start);
            const std::size_t line = static_cast<std::size_t>(std::count(text.begin(), before, U'\n')) + 1;
            const std::string label = RelativeTo(page, root) + ":" + std::to_string(line);
            std::error_code ec;
            if (!fs::exists(target, ec)) {
                failures.push_back(label + ": missing " + link.url);
            } else if (!url.fragment.empty() && target.extension() == ".md") {
                auto found = anchors.find(target);
                if (found == anchors.end()) found = anchors.emplace(target, Anchors(ReadText(target))).first;
                if (!found->second.count(Unquote(url.fragment))) {
                    failures.push_back(label + ": missing anchor " + link.url);
                }
            }
            ++count;
        }
    }
    if (!failures.empty()) {
        err << Join(failures, "\n") << "\n";
        return 1;
    }
    out << "PASS: " << count << " local documentation links\n";
    return 0;
}

// --- Docker quick-start ----------------------------------------------------------------

const std::vector<std::string> kQuickStartBlocks{"start", "health", "login", "write", "read",
                                                 "watch", "watch-update", "reset", "reset-login"};

// <!-- docker-quickstart:([\w-]+) -->\s*```(sh(?: docs-check-watch)?|cql docs-check[^\n]*)\n(.*?)\n```
std::map<std::string, std::string> ExtractQuickStart(const std::string& source) {
    const std::u32string text = DecodeUtf8(source);
    const std::u32string_view marker = U"<!-- docker-quickstart:";
    const std::size_t n = text.size();
    std::map<std::string, std::string> blocks;
    std::size_t search = 0;
    while (true) {
        const std::size_t at = text.find(marker, search);
        if (at == std::u32string::npos) break;
        search = at + 1;
        std::size_t pos = at + marker.size();
        const std::size_t name_start = pos;
        while (pos < n && (IsWord(text[pos]) || text[pos] == U'-')) ++pos;
        if (pos == name_start || !StartsWithAt(text, pos, U" -->")) continue;
        const std::u32string name = text.substr(name_start, pos - name_start);
        pos += 4;
        while (pos < n && IsSpace(text[pos])) ++pos;
        if (!StartsWithAt(text, pos, U"```")) continue;
        pos += 3;
        std::u32string tag;
        if (StartsWithAt(text, pos, U"sh docs-check-watch\n")) {
            tag = U"sh docs-check-watch";
        } else if (StartsWithAt(text, pos, U"sh\n")) {
            tag = U"sh";
        } else if (StartsWithAt(text, pos, U"cql docs-check")) {
            const std::size_t newline = text.find(U'\n', pos);
            if (newline == std::u32string::npos) continue;
            tag = text.substr(pos, newline - pos);
        } else {
            continue;
        }
        const std::size_t code_start = pos + tag.size() + 1;
        const std::size_t code_end = text.find(U"\n```", code_start);
        if (code_end == std::u32string::npos) continue;
        std::string code = EncodeUtf8(std::u32string_view(text).substr(code_start, code_end - code_start));
        if (tag[0] == U'c') code = "chunk-cli --uri \"$CHUNKDB_URI\" shell <<'CQL'\n" + code + "\nexit\nCQL";
        const std::string key = EncodeUtf8(name);
        if (blocks.count(key)) throw DocsError("duplicate quick-start block: " + key);
        blocks[key] = code;
        search = code_end + 4;
    }
    std::set<std::string> difference;
    for (const auto& [name, code] : blocks) {
        if (std::find(kQuickStartBlocks.begin(), kQuickStartBlocks.end(), name) == kQuickStartBlocks.end()) {
            difference.insert(name);
        }
    }
    for (const auto& name : kQuickStartBlocks) {
        if (!blocks.count(name)) difference.insert(name);
    }
    if (!difference.empty()) {
        std::vector<std::string> quoted;
        for (const auto& name : difference) quoted.push_back("'" + name + "'");
        throw DocsError("quick-start blocks differ: {" + Join(quoted, ", ") + "}");
    }
    return blocks;
}

std::string ReadQuickStartPages(const fs::path& page, const fs::path& reset_page) {
    return ReadText(page) + "\n" + ReadText(reset_page);
}

QuickStartFixture MakeQuickStartFixture(const std::string& pages, const std::string& container,
                                        const std::string& image) {
    QuickStartFixture fixture;
    fixture.blocks = ExtractQuickStart(pages);
    for (const std::string name : {"start", "reset"}) {
        // Every ghcr\.io/chunkdb/chunkdb:([^\s]+) tag must be exactly '2'.
        const std::u32string code = DecodeUtf8(fixture.blocks.at(name));
        const std::u32string_view image_prefix = U"ghcr.io/chunkdb/chunkdb:";
        std::vector<std::u32string> tags;
        std::size_t search = 0;
        while (true) {
            const std::size_t at = code.find(image_prefix, search);
            if (at == std::u32string::npos) break;
            std::size_t end = at + image_prefix.size();
            while (end < code.size() && !IsSpace(code[end])) ++end;
            if (end == at + image_prefix.size()) {
                search = at + 1;
                continue;
            }
            tags.push_back(code.substr(at + image_prefix.size(), end - at - image_prefix.size()));
            search = end;
        }
        if (tags != std::vector<std::u32string>{U"2"}) {
            throw DocsError("expected published image changed in page block: " + name);
        }
    }
    fixture.container = container;
    fixture.volume = container + "-data";
    fixture.image = image;
    return fixture;
}

std::string QuickStartCode(const QuickStartFixture& fixture, const std::string& name) {
    const auto found = fixture.blocks.find(name);
    if (found == fixture.blocks.end()) throw DocsError("unknown quick-start block: " + name);
    std::string code = ReplaceAll(found->second, "chunkdb-quickstart-data", fixture.volume);
    code = ReplaceAll(code, "chunkdb-quickstart", fixture.container);
    code = ReplaceAll(code, "ghcr.io/chunkdb/chunkdb:2", ShellQuote(fixture.image));
    code = ReplaceAll(code, "127.0.0.1:4242:4242", "127.0.0.1::4242");
    if (fixture.port) code = ReplaceAll(code, "127.0.0.1:4242/", "127.0.0.1:" + *fixture.port + "/");
    return code;
}

std::string QuickStartBlock(const QuickStartFixture& fixture, const std::string& name) {
    std::string code = QuickStartCode(fixture, name);
    if (name == "login") {
        code = ReplaceAll(code, "chunk-cli --uri \"$CHUNKDB_URI\" shell",
                          "printf 'PING\\nexit\\n' | chunk-cli --uri \"$CHUNKDB_URI\" shell");
    }
    return "set -eu\n" + code;
}

std::string QuickStartPasswordExport(const QuickStartFixture& fixture, const std::string& name) {
    std::vector<std::string> exports;
    for (const std::string& line : SplitLines(QuickStartCode(fixture, name))) {
        if (StartsWith(line, "export CHUNKDB_PASSWORD=")) exports.push_back(line);
    }
    if (exports.size() != 1) throw DocsError("expected one password export in page block: " + name);
    return exports.front();
}

std::string QuickStartLastLine(const QuickStartFixture& fixture, const std::string& name) {
    const std::vector<std::string> lines = SplitLines(QuickStartCode(fixture, name));
    if (lines.empty()) throw DocsError("empty page block: " + name);
    return lines.back();
}

std::string Redact(std::string text, const std::vector<std::string>& secrets) {
    // (Generated password: )[^\r\n]+ -> \1<redacted>, anywhere in a line.
    constexpr std::string_view kGenerated = "Generated password: ";
    std::string out;
    std::size_t copied = 0;
    std::size_t search = 0;
    while (true) {
        const std::size_t at = text.find(kGenerated, search);
        if (at == std::string::npos) break;
        const std::size_t value = at + kGenerated.size();
        std::size_t end = value;
        while (end < text.size() && text[end] != '\r' && text[end] != '\n') ++end;
        if (end == value) {
            search = at + 1;
            continue;
        }
        out.append(text, copied, value - copied);
        out.append("<redacted>");
        copied = end;
        search = end;
    }
    out.append(text, copied, std::string::npos);
    for (const std::string& secret : secrets) out = ReplaceAll(std::move(out), secret, "<redacted>");
    return out;
}

// Single-quotes text unless it holds only [A-Za-z0-9_@%+=:,./-].
std::string ShellQuote(std::string_view text) {
    if (text.empty()) return "''";
    const bool safe = std::all_of(text.begin(), text.end(), [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || IsAsciiDigit(c) ||
               std::string_view("_@%+=:,./-").find(c) != std::string_view::npos;
    });
    if (safe) return std::string(text);
    return "'" + ReplaceAll(std::string(text), "'", "'\"'\"'") + "'";
}

// --- Checks with processes ----------------------------------------------------------

#ifndef _WIN32

bool ProcessChecksSupported() { return true; }

int RunExamples(const ExamplesOptions& options, std::ostream& out) {
    const fs::path root = Resolve(options.root);
    const fs::path server = Resolve(options.server.empty() ? root / "build/chunkdb_server" : options.server);
    const fs::path cli = Resolve(options.cli);
    std::error_code ec;
    if (!fs::is_regular_file(server, ec) || !fs::is_regular_file(cli, ec) || cli.filename() != "chunk-cli") {
        throw UsageError("provide current built server and a CLI binary named chunk-cli");
    }
    std::vector<fs::path> pages = options.pages;
    if (pages.empty()) {
        pages.push_back(root / "README.md");
        for (const auto& page : MarkdownUnder(root / "docs")) {
            const auto parts = Parts(page.lexically_relative(root / "docs"));
            const bool archived = std::any_of(parts.begin(), parts.end(), [](const std::string& part) {
                return part == "design" || part == "releases" || part == "benchmarks";
            });
            if (!archived) pages.push_back(page);
        }
    }
    std::size_t count = 0;
    for (const fs::path& page : pages) {
        const std::size_t checked = CheckPage(Resolve(page), root, server, cli, options.record);
        if (checked) out << RelativeTo(Resolve(page), root) << ": " << checked << " examples\n" << std::flush;
        count += checked;
    }
    if (!count) throw UsageError("no checked examples found");
    out << "PASS: " << count << " examples from the current build\n";
    return 0;
}

int RunWalkthrough(const ExamplesOptions& options, std::ostream& out) {
    const fs::path root = Resolve(options.root);
    const fs::path server = Resolve(options.server.empty() ? root / "build/chunkdb_server" : options.server);
    const std::vector<std::string> pages{"docs/QUICK_START.md", "README.md", "docs/reference/BLOCKS.md",
                                         "docs/reference/MIGRATIONS.md", "docs/reference/FEED.md"};
    TempDir directory("chunkdb-walkthrough-");
    Fixture fixture(server, Resolve(options.cli), directory.path());
    std::size_t count = 0;
    WithFixture(fixture, [&] {
        fixture.Start();
        for (const std::string& page : pages) {
            const std::vector<Example> blocks = Examples(ReadText(root / page));
            std::vector<std::string> results;
            std::optional<std::size_t> watcher;
            for (std::size_t index = 0; index < blocks.size(); ++index) {
                results.push_back(fixture.Execute(blocks[index].kind, blocks[index].session, blocks[index].code));
                if (blocks[index].kind == "watch") {
                    watcher = index;
                } else if (watcher) {
                    results[*watcher] += fixture.FinishWatch();
                    watcher.reset();
                }
            }
            if (watcher) throw std::runtime_error(page);
            for (std::size_t index = 0; index < blocks.size(); ++index) {
                const std::string actual = RStripNewlines(Normalize(results[index]));
                if (actual != blocks[index].output.code) {
                    throw std::runtime_error(page + " example " + std::to_string(index + 1) + ":\n" + actual);
                }
            }
            count += blocks.size();
            out << page << ": PASS on the same server\n" << std::flush;
        }
    });
    out << "PASS: " << count << " examples and a durable-slot replay after the tutorial\n";
    return 0;
}

int RunImageTags(const fs::path& workflow, std::ostream& out, std::ostream& err) {
    const std::string source = ReadText(workflow);
    constexpr std::string_view kStep = "      - name: Image tags from release tag\n";
    constexpr std::string_view kRun = "        run: |\n";
    const std::size_t step_at = source.find(kStep);
    if (step_at == std::string::npos) {
        throw std::runtime_error("no 'Image tags from release tag' step in " + workflow.string());
    }
    std::string step = source.substr(step_at + kStep.size());
    step = step.substr(0, step.find("      - uses:"));
    const std::size_t run_at = step.find(kRun);
    if (run_at == std::string::npos) throw std::runtime_error("the image tag step has no run script");
    std::vector<std::string> script_lines;
    for (const std::string& line : SplitLines(step.substr(run_at + kRun.size()))) {
        script_lines.push_back(line.size() > 10 ? line.substr(10) : std::string());
    }
    const std::string script = Join(script_lines, "\n");

    // Lines ^            (ghcr.io/chunkdb/chunkdb:[^\n]+)$, where the unescaped
    // '.' matches any character but a newline.
    std::vector<std::string> published;
    for (std::size_t line = 0; line < source.size();) {
        std::size_t end = source.find('\n', line);
        if (end == std::string::npos) end = source.size();
        const std::string_view text(source.data() + line, end - line);
        constexpr std::string_view kIndent = "            ";
        if (StartsWith(text, kIndent) && text.size() > kIndent.size() + 24 &&
            text.compare(kIndent.size(), 4, "ghcr") == 0 &&
            text.compare(kIndent.size() + 5, 19, "io/chunkdb/chunkdb:") == 0) {
            published.emplace_back(text.substr(kIndent.size()));
        }
        line = end + 1;
    }

    struct Calculation {
        Completed result;
        std::vector<std::pair<std::string, std::string>> values;
        std::vector<std::string> tags;
    };
    auto calculate = [&](const std::string& tag) {
        TempDir directory("chunkdb-image-tags-");
        const std::string output = directory.path() + "/outputs";
        Environment env = CurrentEnvironment();
        env["RELEASE_TAG"] = tag;
        env["GITHUB_OUTPUT"] = output;
        Calculation calculation;
        calculation.result =
            Run({"bash", "-eu", "-o", "pipefail", "-c", script}, env, std::chrono::seconds(60), Output::kMerge);
        std::error_code ec;
        if (fs::exists(output, ec)) {
            for (const std::string& line : SplitLines(ReadText(output))) {
                const std::size_t equals = line.find('=');
                if (equals == std::string::npos) throw std::runtime_error("output line without '=': " + line);
                const std::string key = line.substr(0, equals);
                const std::string value = line.substr(equals + 1);
                auto existing = std::find_if(calculation.values.begin(), calculation.values.end(),
                                             [&](const auto& entry) { return entry.first == key; });
                if (existing == calculation.values.end()) {
                    calculation.values.emplace_back(key, value);
                } else {
                    existing->second = value;
                }
            }
        }
        calculation.tags = published;
        for (const auto& [key, value] : calculation.values) {
            for (std::string& item : calculation.tags) {
                item = ReplaceAll(item, "${{ steps.image.outputs." + key + " }}", value);
            }
        }
        return calculation;
    };
    auto as_map = [](const std::vector<std::pair<std::string, std::string>>& values) {
        return std::map<std::string, std::string>(values.begin(), values.end());
    };

    std::vector<std::string> failures;
    std::size_t cases = 0;
    const std::vector<std::array<std::string, 3>> valid{{"v2.0.0", "2.0.0", "2"},
                                                        {"v2.4.1", "2.4.1", "2"},
                                                        {"v3.0.0", "3.0.0", "3"},
                                                        {"v1.9.0", "1.9.0", "1"},
                                                        {"v2.1.0-rc.1", "2.1.0-rc.1", "2"}};
    for (const auto& [tag, version, major_tag] : valid) {
        ++cases;
        const Calculation c = calculate(tag);
        const std::vector<std::string> expected{"ghcr.io/chunkdb/chunkdb:" + version,
                                                "ghcr.io/chunkdb/chunkdb:" + major_tag,
                                                "ghcr.io/chunkdb/chunkdb:latest"};
        if (c.result.returncode != 0) {
            failures.push_back(tag + ": exit status " + std::to_string(c.result.returncode) + "\n" +
                               Sanitize(c.result.out));
        } else if (as_map(c.values) != std::map<std::string, std::string>{{"version", version}, {"major", major_tag}}) {
            failures.push_back(tag + ": unexpected outputs");
        } else if (c.tags != expected) {
            failures.push_back(tag + ": unexpected image tags: " + Join(c.tags, ", "));
        }
    }
    for (const std::string tag : {"2.0.0", "vnext", "v3", "v2.0", "v02.0.0", "v2.0.0+build", "v2.0.0;echo bad"}) {
        ++cases;
        const Calculation c = calculate(tag);
        if (c.result.returncode == 0) {
            failures.push_back(tag + ": accepted an invalid release tag");
        } else if (!c.values.empty()) {
            failures.push_back(tag + ": wrote outputs for an invalid release tag");
        }
    }
    for (const std::string& failure : failures) err << "FAIL: " << failure << "\n";
    if (!failures.empty()) return 1;
    out << "PASS: " << cases << " release image tag cases\n";
    return 0;
}

void OpenShell(const fs::path& cli, const std::string& uri, std::chrono::milliseconds deadline) {
    Environment env = CurrentEnvironment();
    env["CHUNKDB_URI"] = uri;
    Shell shell(cli, env, deadline);
    shell.Close();
}

#else

bool ProcessChecksSupported() { return false; }

namespace {
[[noreturn]] void Unsupported() { throw std::runtime_error("this check needs a POSIX system with bash"); }
}  // namespace

int RunExamples(const ExamplesOptions&, std::ostream&) { Unsupported(); }
int RunWalkthrough(const ExamplesOptions&, std::ostream&) { Unsupported(); }
int RunImageTags(const fs::path&, std::ostream&, std::ostream&) { Unsupported(); }
void OpenShell(const fs::path&, const std::string&, std::chrono::milliseconds) { Unsupported(); }

#endif  // _WIN32

}  // namespace chunkdb::docs_check
