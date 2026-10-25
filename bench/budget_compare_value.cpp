#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "budget_compare.hpp"

namespace chunkdb::budget_compare {

Error::Error(ErrorKind kind, const std::string& message) : std::runtime_error(message), kind_(kind) {}

namespace {

using Mag = std::vector<std::uint32_t>;

// Python's default limit on decimal conversion of integers.
constexpr std::size_t kMaxIntDigits = 4300;
constexpr int kMaxNesting = 1000;

void Trim(Mag& a) {
    while (!a.empty() && a.back() == 0) {
        a.pop_back();
    }
}

int CompareMag(const Mag& a, const Mag& b) {
    if (a.size() != b.size()) {
        return a.size() < b.size() ? -1 : 1;
    }
    for (std::size_t i = a.size(); i-- > 0;) {
        if (a[i] != b[i]) {
            return a[i] < b[i] ? -1 : 1;
        }
    }
    return 0;
}

Mag AddMag(const Mag& a, const Mag& b) {
    Mag out(std::max(a.size(), b.size()) + 1, 0);
    std::uint64_t carry = 0;
    for (std::size_t i = 0; i < out.size(); ++i) {
        std::uint64_t sum = carry;
        if (i < a.size()) {
            sum += a[i];
        }
        if (i < b.size()) {
            sum += b[i];
        }
        out[i] = static_cast<std::uint32_t>(sum);
        carry = sum >> 32;
    }
    Trim(out);
    return out;
}

// a - b for a >= b.
Mag SubMag(const Mag& a, const Mag& b) {
    Mag out(a.size(), 0);
    std::uint64_t borrow = 0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        const std::uint64_t subtrahend = (i < b.size() ? b[i] : 0U) + borrow;
        const std::uint64_t minuend = a[i];
        if (minuend >= subtrahend) {
            out[i] = static_cast<std::uint32_t>(minuend - subtrahend);
            borrow = 0;
        } else {
            out[i] = static_cast<std::uint32_t>((std::uint64_t{1} << 32) + minuend - subtrahend);
            borrow = 1;
        }
    }
    Trim(out);
    return out;
}

Mag MulMag(const Mag& a, const Mag& b) {
    if (a.empty() || b.empty()) {
        return {};
    }
    Mag out(a.size() + b.size(), 0);
    for (std::size_t i = 0; i < a.size(); ++i) {
        std::uint64_t carry = 0;
        for (std::size_t j = 0; j < b.size(); ++j) {
            const std::uint64_t current = static_cast<std::uint64_t>(a[i]) * b[j] + out[i + j] + carry;
            out[i + j] = static_cast<std::uint32_t>(current);
            carry = current >> 32;
        }
        for (std::size_t k = i + b.size(); carry != 0; ++k) {
            const std::uint64_t current = static_cast<std::uint64_t>(out[k]) + carry;
            out[k] = static_cast<std::uint32_t>(current);
            carry = current >> 32;
        }
    }
    Trim(out);
    return out;
}

Mag MagFromU64(std::uint64_t value) {
    Mag out{static_cast<std::uint32_t>(value), static_cast<std::uint32_t>(value >> 32)};
    Trim(out);
    return out;
}

std::size_t BitLength(const Mag& a) {
    if (a.empty()) {
        return 0;
    }
    std::size_t bits = (a.size() - 1) * 32;
    for (std::uint32_t top = a.back(); top != 0; top >>= 1) {
        ++bits;
    }
    return bits;
}

bool TestBit(const Mag& a, std::size_t bit) {
    const std::size_t limb = bit / 32;
    return limb < a.size() && ((a[limb] >> (bit % 32)) & 1U) != 0;
}

bool AnyBitBelow(const Mag& a, std::size_t bit) {
    const std::size_t limb = bit / 32;
    for (std::size_t i = 0; i < std::min(limb, a.size()); ++i) {
        if (a[i] != 0) {
            return true;
        }
    }
    if (limb < a.size() && bit % 32 != 0) {
        return (a[limb] & ((1U << (bit % 32)) - 1U)) != 0;
    }
    return false;
}

Mag ShlMag(const Mag& a, std::size_t bits) {
    if (a.empty()) {
        return {};
    }
    const std::size_t limbs = bits / 32;
    const unsigned shift = static_cast<unsigned>(bits % 32);
    Mag out(a.size() + limbs + 1, 0);
    for (std::size_t i = 0; i < a.size(); ++i) {
        const std::uint64_t moved = static_cast<std::uint64_t>(a[i]) << shift;
        out[i + limbs] |= static_cast<std::uint32_t>(moved);
        out[i + limbs + 1] |= static_cast<std::uint32_t>(moved >> 32);
    }
    Trim(out);
    return out;
}

Mag ShrMag(const Mag& a, std::size_t bits) {
    const std::size_t limbs = bits / 32;
    if (limbs >= a.size()) {
        return {};
    }
    const unsigned shift = static_cast<unsigned>(bits % 32);
    Mag out(a.size() - limbs, 0);
    for (std::size_t i = 0; i < out.size(); ++i) {
        std::uint64_t part = a[i + limbs] >> shift;
        if (shift != 0 && i + limbs + 1 < a.size()) {
            part |= static_cast<std::uint64_t>(a[i + limbs + 1]) << (32 - shift);
        }
        out[i] = static_cast<std::uint32_t>(part);
    }
    Trim(out);
    return out;
}

// Floor division of magnitudes, b nonzero.
void DivModMag(const Mag& a, const Mag& b, Mag& quotient, Mag& remainder) {
    quotient.assign(a.size(), 0);
    remainder.clear();
    if (b.size() == 1) {
        std::uint64_t rest = 0;
        for (std::size_t i = a.size(); i-- > 0;) {
            const std::uint64_t current = (rest << 32) | a[i];
            quotient[i] = static_cast<std::uint32_t>(current / b[0]);
            rest = current % b[0];
        }
        Trim(quotient);
        remainder = MagFromU64(rest);
        return;
    }
    for (std::size_t bit = BitLength(a); bit-- > 0;) {
        remainder = ShlMag(remainder, 1);
        if (TestBit(a, bit)) {
            if (remainder.empty()) {
                remainder.push_back(1);
            } else {
                remainder[0] |= 1U;
            }
        }
        if (CompareMag(remainder, b) >= 0) {
            remainder = SubMag(remainder, b);
            quotient[bit / 32] |= 1U << (bit % 32);
        }
    }
    Trim(quotient);
}

// floor(sqrt(a)).
Mag IsqrtMag(const Mag& a) {
    if (a.empty()) {
        return {};
    }
    Mag rest = a;
    Mag root;
    std::size_t top = BitLength(a) - 1;
    top -= top % 2;
    Mag bit = ShlMag(Mag{1}, top);
    while (!bit.empty()) {
        const Mag trial = AddMag(root, bit);
        if (CompareMag(rest, trial) >= 0) {
            rest = SubMag(rest, trial);
            root = AddMag(ShrMag(root, 1), bit);
        } else {
            root = ShrMag(root, 1);
        }
        bit = ShrMag(bit, 2);
    }
    return root;
}

std::uint64_t LowU64(const Mag& a) {
    std::uint64_t value = a.empty() ? 0 : a[0];
    if (a.size() > 1) {
        value |= static_cast<std::uint64_t>(a[1]) << 32;
    }
    return value;
}

// value * 2**exponent rounded half to even, where value is q, or lies strictly
// between q and q + 1 when sticky is set; q then needs at least 55 bits.
// std::nullopt on overflow.
std::optional<double> RoundToDouble(const Mag& q, long exponent, bool sticky, bool negative) {
    if (q.empty()) {
        return negative ? -0.0 : 0.0;
    }
    const long leading = static_cast<long>(BitLength(q)) - 1 + exponent;
    if (leading > 1023) {
        return std::nullopt;
    }
    const long lsb = std::max(leading - 52, -1074L);
    const long drop = lsb - exponent;
    double result = 0.0;
    if (drop <= 0) {
        result = std::ldexp(static_cast<double>(LowU64(q)), static_cast<int>(exponent));
    } else {
        const std::size_t bits = static_cast<std::size_t>(drop);
        std::uint64_t keep = LowU64(ShrMag(q, bits));
        const bool half = TestBit(q, bits - 1);
        const bool below = AnyBitBelow(q, bits - 1) || sticky;
        if (half && (below || (keep & 1U) != 0)) {
            ++keep;
        }
        result = std::ldexp(static_cast<double>(keep), static_cast<int>(lsb));
    }
    if (std::isinf(result)) {
        return std::nullopt;
    }
    return negative ? -result : result;
}

std::optional<double> TrueDivideMag(const Mag& a, const Mag& b, bool negative) {
    if (a.empty()) {
        return negative ? -0.0 : 0.0;
    }
    const long shift = 55 - (static_cast<long>(BitLength(a)) - static_cast<long>(BitLength(b)));
    Mag quotient;
    Mag remainder;
    if (shift >= 0) {
        DivModMag(ShlMag(a, static_cast<std::size_t>(shift)), b, quotient, remainder);
    } else {
        DivModMag(a, ShlMag(b, static_cast<std::size_t>(-shift)), quotient, remainder);
    }
    return RoundToDouble(quotient, -shift, !remainder.empty(), negative);
}

[[noreturn]] void Fail(ErrorKind kind, const std::string& message) { throw Error(kind, message); }

}  // namespace

BigInt::BigInt(std::int64_t value) {
    negative_ = value < 0;
    const std::uint64_t magnitude =
        negative_ ? static_cast<std::uint64_t>(-(value + 1)) + 1U : static_cast<std::uint64_t>(value);
    magnitude_ = MagFromU64(magnitude);
}

BigInt::BigInt(bool negative, std::vector<std::uint32_t> magnitude) : magnitude_(std::move(magnitude)) {
    Trim(magnitude_);
    negative_ = negative && !magnitude_.empty();
}

std::optional<BigInt> BigInt::Parse(std::string_view text) {
    bool negative = false;
    if (!text.empty() && text.front() == '-') {
        negative = true;
        text.remove_prefix(1);
    }
    if (text.empty()) {
        return std::nullopt;
    }
    Mag magnitude;
    for (std::size_t start = 0; start < text.size();) {
        const std::size_t length = std::min<std::size_t>(9, text.size() - start);
        std::uint32_t chunk = 0;
        std::uint32_t scale = 1;
        for (std::size_t i = start; i < start + length; ++i) {
            if (text[i] < '0' || text[i] > '9') {
                return std::nullopt;
            }
            chunk = chunk * 10 + static_cast<std::uint32_t>(text[i] - '0');
            scale *= 10;
        }
        magnitude = AddMag(MulMag(magnitude, Mag{scale}), MagFromU64(chunk));
        start += length;
    }
    return BigInt(negative, std::move(magnitude));
}

std::string BigInt::ToString() const {
    if (magnitude_.empty()) {
        return "0";
    }
    std::vector<std::uint32_t> chunks;
    Mag rest = magnitude_;
    while (!rest.empty()) {
        Mag quotient;
        Mag remainder;
        DivModMag(rest, Mag{1000000000U}, quotient, remainder);
        chunks.push_back(static_cast<std::uint32_t>(LowU64(remainder)));
        rest = std::move(quotient);
    }
    std::string out = negative_ ? "-" : "";
    out += std::to_string(chunks.back());
    for (std::size_t i = chunks.size() - 1; i-- > 0;) {
        const std::string part = std::to_string(chunks[i]);
        out += std::string(9 - part.size(), '0') + part;
    }
    return out;
}

std::size_t BigInt::DecimalDigits() const {
    // Only needed against Python's 4300-digit limit; a bit length bound decides
    // nearly every case without converting.
    const std::size_t bits = BitLength(magnitude_);
    if (bits < 14000) {
        const std::string text = ToString();
        return text.size() - (negative_ ? 1 : 0);
    }
    return bits * 30103 / 100000;
}

std::optional<std::int64_t> BigInt::ToInt64() const noexcept {
    if (magnitude_.size() > 2) {
        return std::nullopt;
    }
    const std::uint64_t value = LowU64(magnitude_);
    if (!negative_) {
        if (value > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
            return std::nullopt;
        }
        return static_cast<std::int64_t>(value);
    }
    if (value > (std::uint64_t{1} << 63)) {
        return std::nullopt;
    }
    if (value == (std::uint64_t{1} << 63)) {
        return std::numeric_limits<std::int64_t>::min();
    }
    return -static_cast<std::int64_t>(value);
}

std::optional<double> BigInt::ToDouble() const { return RoundToDouble(magnitude_, 0, false, negative_); }

int Compare(const BigInt& a, const BigInt& b) {
    if (a.negative_ != b.negative_) {
        return a.negative_ ? -1 : 1;
    }
    const int magnitude = CompareMag(a.magnitude_, b.magnitude_);
    return a.negative_ ? -magnitude : magnitude;
}

BigInt operator+(const BigInt& a, const BigInt& b) {
    if (a.negative_ == b.negative_) {
        return BigInt(a.negative_, AddMag(a.magnitude_, b.magnitude_));
    }
    const int order = CompareMag(a.magnitude_, b.magnitude_);
    if (order == 0) {
        return BigInt();
    }
    if (order > 0) {
        return BigInt(a.negative_, SubMag(a.magnitude_, b.magnitude_));
    }
    return BigInt(b.negative_, SubMag(b.magnitude_, a.magnitude_));
}

BigInt operator-(const BigInt& a, const BigInt& b) { return a + BigInt(!b.negative_, b.magnitude_); }

BigInt operator*(const BigInt& a, const BigInt& b) {
    return BigInt(a.negative_ != b.negative_, MulMag(a.magnitude_, b.magnitude_));
}

std::optional<double> TrueDivide(const BigInt& a, const BigInt& b) {
    return TrueDivideMag(a.Magnitude(), b.Magnitude(), a.IsNegative() != b.IsNegative());
}

int CompareExact(const BigInt& a, double b) {
    if (b == 0.0) {
        return a.IsZero() ? 0 : (a.IsNegative() ? -1 : 1);
    }
    int exponent = 0;
    const double fraction = std::frexp(b, &exponent);
    const auto mantissa = static_cast<std::int64_t>(std::ldexp(fraction, 53));
    const int shift = exponent - 53;
    const BigInt m(mantissa);
    if (shift >= 0) {
        return Compare(a, BigInt(m.IsNegative(), ShlMag(m.Magnitude(), static_cast<std::size_t>(shift))));
    }
    return Compare(BigInt(a.IsNegative(), ShlMag(a.Magnitude(), static_cast<std::size_t>(-shift))), m);
}

double SampleStdev(const std::vector<double>& values) {
    // Every double is m * 2**e; with E the smallest e, x = X * 2**E for integers
    // X. The sample variance is then N / M * 4**E exactly, with N = n*S2 - S1**2
    // and M = n*(n - 1), and its square root is rounded once, as Python 3.11+
    // statistics.stdev rounds it (round to odd on at least 55 bits).
    if (values.size() < 2) {
        Fail(ErrorKind::kValue, "stdev requires at least two data points");
    }
    std::vector<std::pair<std::int64_t, int>> parts;
    int smallest = std::numeric_limits<int>::max();
    for (const double value : values) {
        int exponent = 0;
        const double fraction = std::frexp(value, &exponent);
        const auto mantissa = static_cast<std::int64_t>(std::ldexp(fraction, 53));
        const int scale = mantissa == 0 ? std::numeric_limits<int>::max() : exponent - 53;
        parts.emplace_back(mantissa, scale);
        smallest = std::min(smallest, scale);
    }
    if (smallest == std::numeric_limits<int>::max()) {
        return 0.0;
    }
    BigInt s1;
    BigInt s2;
    for (const auto& [mantissa, scale] : parts) {
        if (mantissa == 0) {
            continue;
        }
        const BigInt m(mantissa);
        const BigInt x(m.IsNegative(), ShlMag(m.Magnitude(), static_cast<std::size_t>(scale - smallest)));
        s1 = s1 + x;
        s2 = s2 + x * x;
    }
    const auto count = static_cast<std::int64_t>(values.size());
    const BigInt numerator = BigInt(count) * s2 - s1 * s1;
    if (numerator.IsZero()) {
        return 0.0;
    }
    const Mag& n = numerator.Magnitude();
    const Mag m = MagFromU64(static_cast<std::uint64_t>(count) * static_cast<std::uint64_t>(count - 1));
    const long difference = static_cast<long>(BitLength(n)) - static_cast<long>(BitLength(m)) - 109;
    const long q = difference >= 0 ? difference / 2 : -((-difference + 1) / 2);
    Mag quotient;
    Mag remainder;
    Mag root;
    bool inexact = false;
    if (q >= 0) {
        const Mag scaled = ShlMag(m, static_cast<std::size_t>(2 * q));
        DivModMag(n, scaled, quotient, remainder);
        root = IsqrtMag(quotient);
        inexact = CompareMag(MulMag(MulMag(root, root), scaled), n) != 0;
    } else {
        const Mag scaled = ShlMag(n, static_cast<std::size_t>(-2 * q));
        DivModMag(scaled, m, quotient, remainder);
        root = IsqrtMag(quotient);
        inexact = CompareMag(MulMag(MulMag(root, root), m), scaled) != 0;
    }
    if (inexact) {
        if (root.empty()) {
            root.push_back(1);
        } else {
            root[0] |= 1U;
        }
    }
    const auto result = RoundToDouble(root, q + smallest, false, false);
    if (!result) {
        Fail(ErrorKind::kOverflow, "integer division result too large for a float");
    }
    return *result;
}

// ---- Value ----

Value Value::Bool(bool value) {
    Value out;
    out.kind_ = Kind::kBool;
    out.bool_ = value;
    return out;
}

Value Value::Int(BigInt value) {
    Value out;
    out.kind_ = Kind::kInt;
    out.int_ = std::move(value);
    return out;
}

Value Value::Float(double value) {
    Value out;
    out.kind_ = Kind::kFloat;
    out.float_ = value;
    return out;
}

Value Value::Str(std::string value) {
    Value out;
    out.kind_ = Kind::kString;
    out.string_ = std::move(value);
    return out;
}

Value Value::Array() {
    Value out;
    out.kind_ = Kind::kArray;
    return out;
}

Value Value::Array(std::vector<Value> items) {
    Value out;
    out.kind_ = Kind::kArray;
    out.items_ = std::move(items);
    return out;
}

Value Value::Object() {
    Value out;
    out.kind_ = Kind::kObject;
    return out;
}

const Value* Value::Find(std::string_view key) const {
    for (const Member& member : members_) {
        if (member.key == key) {
            return &member.value;
        }
    }
    return nullptr;
}

Value* Value::Find(std::string_view key) {
    for (Member& member : members_) {
        if (member.key == key) {
            return &member.value;
        }
    }
    return nullptr;
}

Value& Value::Set(const std::string& key, Value value) {
    if (Value* existing = Find(key)) {
        *existing = std::move(value);
        return *existing;
    }
    members_.push_back(Member{key, std::move(value)});
    return members_.back().value;
}

void Value::Append(Value value) { items_.push_back(std::move(value)); }

bool Value::Erase(std::string_view key) {
    for (auto it = members_.begin(); it != members_.end(); ++it) {
        if (it->key == key) {
            members_.erase(it);
            return true;
        }
    }
    return false;
}

// ---- WTF-8 ----

namespace {

void AppendCodePoint(std::string& out, std::uint32_t code) {
    if (code < 0x80) {
        out += static_cast<char>(code);
    } else if (code < 0x800) {
        out += static_cast<char>(0xC0 | (code >> 6));
        out += static_cast<char>(0x80 | (code & 0x3F));
    } else if (code < 0x10000) {
        out += static_cast<char>(0xE0 | (code >> 12));
        out += static_cast<char>(0x80 | ((code >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (code & 0x3F));
    } else {
        out += static_cast<char>(0xF0 | (code >> 18));
        out += static_cast<char>(0x80 | ((code >> 12) & 0x3F));
        out += static_cast<char>(0x80 | ((code >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (code & 0x3F));
    }
}

// Code points of internal WTF-8 text; a stray byte (never produced by the
// decoders) reads as the surrogate surrogateescape would give it.
std::vector<std::uint32_t> CodePoints(std::string_view text) {
    std::vector<std::uint32_t> out;
    out.reserve(text.size());
    for (std::size_t i = 0; i < text.size();) {
        const auto lead = static_cast<unsigned char>(text[i]);
        std::size_t length = 1;
        std::uint32_t code = lead;
        if (lead >= 0xF0 && lead <= 0xF4) {
            length = 4;
            code = lead & 0x07U;
        } else if (lead >= 0xE0) {
            length = lead <= 0xEF ? 3 : 1;
            code = lead & 0x0FU;
        } else if (lead >= 0xC2) {
            length = 2;
            code = lead & 0x1FU;
        } else if (lead >= 0x80) {
            length = 1;
        }
        bool valid = lead < 0x80 || length > 1;
        if (valid && length > 1) {
            if (i + length > text.size()) {
                valid = false;
            } else {
                for (std::size_t k = 1; k < length; ++k) {
                    const auto next = static_cast<unsigned char>(text[i + k]);
                    if ((next & 0xC0) != 0x80) {
                        valid = false;
                        break;
                    }
                    code = (code << 6) | (next & 0x3FU);
                }
            }
        }
        if (!valid) {
            out.push_back(0xDC00U + lead);
            i += 1;
            continue;
        }
        out.push_back(code);
        i += length;
    }
    return out;
}

std::size_t CodePointIndex(std::string_view text, std::size_t byte_offset) {
    std::size_t index = 0;
    for (std::size_t i = 0; i < byte_offset && i < text.size(); ++i) {
        if ((static_cast<unsigned char>(text[i]) & 0xC0) != 0x80) {
            ++index;
        }
    }
    return index;
}

std::string Hex(std::uint32_t value, int width) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out(static_cast<std::size_t>(width), '0');
    for (int i = width - 1; i >= 0; --i) {
        out[static_cast<std::size_t>(i)] = kDigits[value & 0xFU];
        value >>= 4;
    }
    return out;
}

// ---- JSON parsing (json.loads) ----

class JsonParser {
  public:
    explicit JsonParser(std::string_view text) : text_(text) {}

    Value Document() {
        SkipSpace();
        Value value = ParseValueOrExpect();
        SkipSpace();
        if (pos_ != text_.size()) {
            Raise("Extra data", pos_);
        }
        return value;
    }

  private:
    [[noreturn]] void Raise(const std::string& message, std::size_t byte_offset) const {
        const std::size_t char_pos = CodePointIndex(text_, byte_offset);
        std::size_t line = 1;
        std::size_t last_newline = std::string_view::npos;
        for (std::size_t i = 0; i < byte_offset && i < text_.size(); ++i) {
            if (text_[i] == '\n') {
                ++line;
                last_newline = i;
            }
        }
        const std::size_t column = last_newline == std::string_view::npos
                                       ? char_pos + 1
                                       : char_pos - CodePointIndex(text_, last_newline);
        Fail(ErrorKind::kValue, message + ": line " + std::to_string(line) + " column " + std::to_string(column) +
                                    " (char " + std::to_string(char_pos) + ")");
    }

    void SkipSpace() {
        while (pos_ < text_.size() &&
               (text_[pos_] == ' ' || text_[pos_] == '\t' || text_[pos_] == '\n' || text_[pos_] == '\r')) {
            ++pos_;
        }
    }

    [[nodiscard]] bool At(char c) const { return pos_ < text_.size() && text_[pos_] == c; }
    [[nodiscard]] bool Literal(std::string_view word) const { return text_.substr(pos_, word.size()) == word; }

    Value ParseValueOrExpect() {
        std::optional<Value> value = ParseValue();
        if (!value) {
            Raise("Expecting value", pos_);
        }
        return std::move(*value);
    }

    std::optional<Value> ParseValue() {
        if (pos_ >= text_.size()) {
            return std::nullopt;
        }
        const char c = text_[pos_];
        if (c == '"') {
            return Value::Str(ParseString());
        }
        if (c == '{' || c == '[') {
            if (++depth_ > kMaxNesting) {
                Fail(ErrorKind::kRuntime, "maximum recursion depth exceeded while decoding a JSON document");
            }
            Value value = c == '{' ? ParseObject() : ParseArray();
            --depth_;
            return value;
        }
        if (c == 'n' && Literal("null")) {
            pos_ += 4;
            return Value();
        }
        if (c == 't' && Literal("true")) {
            pos_ += 4;
            return Value::Bool(true);
        }
        if (c == 'f' && Literal("false")) {
            pos_ += 5;
            return Value::Bool(false);
        }
        if (c == 'N' && Literal("NaN")) {
            pos_ += 3;
            return Value::Float(std::numeric_limits<double>::quiet_NaN());
        }
        if (c == 'I' && Literal("Infinity")) {
            pos_ += 8;
            return Value::Float(std::numeric_limits<double>::infinity());
        }
        if (c == '-' && Literal("-Infinity")) {
            pos_ += 9;
            return Value::Float(-std::numeric_limits<double>::infinity());
        }
        return ParseNumber();
    }

    std::optional<Value> ParseNumber() {
        const std::size_t start = pos_;
        std::size_t i = pos_;
        if (i < text_.size() && text_[i] == '-') {
            ++i;
        }
        auto digit = [&](std::size_t at) { return at < text_.size() && text_[at] >= '0' && text_[at] <= '9'; };
        if (!digit(i)) {
            return std::nullopt;
        }
        if (text_[i] == '0') {
            ++i;
        } else {
            while (digit(i)) {
                ++i;
            }
        }
        bool is_float = false;
        if (i < text_.size() && text_[i] == '.' && digit(i + 1)) {
            is_float = true;
            i += 1;
            while (digit(i)) {
                ++i;
            }
        }
        if (i < text_.size() && (text_[i] == 'e' || text_[i] == 'E')) {
            std::size_t e = i + 1;
            if (e < text_.size() && (text_[e] == '+' || text_[e] == '-')) {
                ++e;
            }
            if (digit(e)) {
                while (digit(e)) {
                    ++e;
                }
                is_float = true;
                i = e;
            }
        }
        const std::string_view number = text_.substr(start, i - start);
        pos_ = i;
        if (!is_float) {
            std::optional<BigInt> value = BigInt::Parse(number);
            const std::size_t digits = number.size() - (number.front() == '-' ? 1 : 0);
            if (digits > kMaxIntDigits) {
                Fail(ErrorKind::kValue, "Exceeds the limit (4300 digits) for integer string conversion: value has " +
                                            std::to_string(digits) +
                                            " digits; use sys.set_int_max_str_digits() to increase the limit");
            }
            return Value::Int(std::move(*value));
        }
        return Value::Float(ParseDouble(number));
    }

    static double ParseDouble(std::string_view number) {
        double value = 0.0;
        const auto result = std::from_chars(number.data(), number.data() + number.size(), value);
        if (result.ec != std::errc::result_out_of_range) {
            return value;
        }
        // Out of range: Python gives infinity or zero, by the decimal exponent of
        // the leading significant digit.
        const bool negative = number.front() == '-';
        const std::size_t e = number.find_first_of("eE");
        long exponent = 0;
        if (e != std::string_view::npos) {
            std::string_view digits = number.substr(e + 1);
            const bool minus = !digits.empty() && digits.front() == '-';
            if (!digits.empty() && (digits.front() == '-' || digits.front() == '+')) {
                digits.remove_prefix(1);
            }
            for (const char c : digits) {
                exponent = std::min(exponent * 10 + (c - '0'), 1000000L);
            }
            exponent = minus ? -exponent : exponent;
        }
        std::string_view mantissa = number.substr(negative ? 1 : 0, e == std::string_view::npos ? e : e - (negative ? 1 : 0));
        const std::size_t dot = mantissa.find('.');
        const std::string_view whole = mantissa.substr(0, dot);
        const std::string_view fraction = dot == std::string_view::npos ? std::string_view{} : mantissa.substr(dot + 1);
        long leading = 0;
        const std::size_t first = whole.find_first_not_of('0');
        if (first != std::string_view::npos) {
            leading = static_cast<long>(whole.size() - first) - 1;
        } else {
            leading = -static_cast<long>(fraction.find_first_not_of('0')) - 1;
        }
        if (leading + exponent > 0) {
            return negative ? -std::numeric_limits<double>::infinity() : std::numeric_limits<double>::infinity();
        }
        return negative ? -0.0 : 0.0;
    }

    static int HexValue(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        return -1;
    }

    std::optional<std::uint32_t> Hex4(std::size_t at) const {
        if (at + 4 > text_.size()) {
            return std::nullopt;
        }
        std::uint32_t value = 0;
        for (std::size_t k = at; k < at + 4; ++k) {
            const int digit = HexValue(text_[k]);
            if (digit < 0) {
                return std::nullopt;
            }
            value = value * 16 + static_cast<std::uint32_t>(digit);
        }
        return value;
    }

    std::string ParseString() {
        const std::size_t begin = pos_;
        ++pos_;
        std::string out;
        while (true) {
            if (pos_ >= text_.size()) {
                Raise("Unterminated string starting at", begin);
            }
            const auto c = static_cast<unsigned char>(text_[pos_]);
            if (c == '"') {
                ++pos_;
                return out;
            }
            if (c < 0x20) {
                Raise("Invalid control character at", pos_);
            }
            if (c != '\\') {
                out += static_cast<char>(c);
                ++pos_;
                continue;
            }
            if (pos_ + 1 >= text_.size()) {
                Raise("Unterminated string starting at", begin);
            }
            const char escape = text_[pos_ + 1];
            if (escape != 'u') {
                switch (escape) {
                    case '"': out += '"'; break;
                    case '\\': out += '\\'; break;
                    case '/': out += '/'; break;
                    case 'b': out += '\b'; break;
                    case 'f': out += '\f'; break;
                    case 'n': out += '\n'; break;
                    case 'r': out += '\r'; break;
                    case 't': out += '\t'; break;
                    default: Raise("Invalid \\escape", pos_);
                }
                pos_ += 2;
                continue;
            }
            const std::optional<std::uint32_t> code = Hex4(pos_ + 2);
            if (!code) {
                Raise("Invalid \\uXXXX escape", pos_ + 1);
            }
            pos_ += 6;
            std::uint32_t value = *code;
            if (value >= 0xD800 && value <= 0xDBFF && pos_ + 1 < text_.size() && text_[pos_] == '\\' &&
                text_[pos_ + 1] == 'u') {
                const std::optional<std::uint32_t> low = Hex4(pos_ + 2);
                if (!low) {
                    Raise("Invalid \\uXXXX escape", pos_ + 1);
                }
                if (*low >= 0xDC00 && *low <= 0xDFFF) {
                    value = 0x10000 + ((value - 0xD800) << 10) + (*low - 0xDC00);
                    pos_ += 6;
                }
            }
            AppendCodePoint(out, value);
        }
    }

    Value ParseObject() {
        Value object = Value::Object();
        ++pos_;
        SkipSpace();
        if (At('}')) {
            ++pos_;
            return object;
        }
        while (true) {
            if (!At('"')) {
                Raise("Expecting property name enclosed in double quotes", pos_);
            }
            std::string key = ParseString();
            SkipSpace();
            if (!At(':')) {
                Raise("Expecting ':' delimiter", pos_);
            }
            ++pos_;
            SkipSpace();
            Value value = ParseValueOrExpect();
            object.Set(key, std::move(value));
            SkipSpace();
            if (At('}')) {
                ++pos_;
                return object;
            }
            if (!At(',')) {
                Raise("Expecting ',' delimiter", pos_);
            }
            const std::size_t comma = pos_;
            ++pos_;
            SkipSpace();
            if (!At('"')) {
                if (At('}')) {
                    Raise("Illegal trailing comma before end of object", comma);
                }
                Raise("Expecting property name enclosed in double quotes", pos_);
            }
        }
    }

    Value ParseArray() {
        Value array = Value::Array();
        ++pos_;
        SkipSpace();
        if (At(']')) {
            ++pos_;
            return array;
        }
        while (true) {
            array.Append(ParseValueOrExpect());
            SkipSpace();
            if (At(']')) {
                ++pos_;
                return array;
            }
            if (!At(',')) {
                Raise("Expecting ',' delimiter", pos_);
            }
            const std::size_t comma = pos_;
            ++pos_;
            SkipSpace();
            if (At(']')) {
                Raise("Illegal trailing comma before end of array", comma);
            }
        }
    }

    std::string_view text_;
    std::size_t pos_ = 0;
    int depth_ = 0;
};

// ---- Decoding bytes ----

[[noreturn]] void DecodeFailure(const std::string& codec, std::string_view bytes, std::size_t start, std::size_t end,
                                const std::string& reason) {
    std::string message = "'" + codec + "' codec can't decode ";
    if (end - start == 1) {
        message += "byte 0x" + Hex(static_cast<unsigned char>(bytes[start]), 2) + " in position " +
                   std::to_string(start);
    } else {
        message += "bytes in position " + std::to_string(start) + "-" + std::to_string(end - 1);
    }
    Fail(ErrorKind::kValue, message + ": " + reason);
}

// Python's UTF-8 decoder; surrogatepass also accepts encoded surrogates.
std::string DecodeUtf8(std::string_view bytes, bool surrogatepass) {
    std::string out;
    out.reserve(bytes.size());
    for (std::size_t i = 0; i < bytes.size();) {
        const auto lead = static_cast<unsigned char>(bytes[i]);
        if (lead < 0x80) {
            out += static_cast<char>(lead);
            ++i;
            continue;
        }
        std::size_t length = 0;
        unsigned char low = 0x80;
        unsigned char high = 0xBF;
        if (lead >= 0xC2 && lead <= 0xDF) {
            length = 2;
        } else if (lead >= 0xE0 && lead <= 0xEF) {
            length = 3;
            if (lead == 0xE0) {
                low = 0xA0;
            } else if (lead == 0xED && !surrogatepass) {
                high = 0x9F;
            }
        } else if (lead >= 0xF0 && lead <= 0xF4) {
            length = 4;
            if (lead == 0xF0) {
                low = 0x90;
            } else if (lead == 0xF4) {
                high = 0x8F;
            }
        } else {
            DecodeFailure("utf-8", bytes, i, i + 1, "invalid start byte");
        }
        for (std::size_t k = 1; k < length; ++k) {
            if (i + k >= bytes.size()) {
                DecodeFailure("utf-8", bytes, i, bytes.size(), "unexpected end of data");
            }
            const auto next = static_cast<unsigned char>(bytes[i + k]);
            const unsigned char min = k == 1 ? low : 0x80;
            const unsigned char max = k == 1 ? high : 0xBF;
            if (next < min || next > max) {
                DecodeFailure("utf-8", bytes, i, i + k, "invalid continuation byte");
            }
        }
        out.append(bytes.substr(i, length));
        i += length;
    }
    return out;
}

std::string DecodeUtf16(std::string_view bytes, bool big_endian, const std::string& codec) {
    std::string out;
    std::size_t i = 0;
    auto unit = [&](std::size_t at) {
        const auto a = static_cast<unsigned char>(bytes[at]);
        const auto b = static_cast<unsigned char>(bytes[at + 1]);
        return big_endian ? static_cast<std::uint32_t>((a << 8) | b) : static_cast<std::uint32_t>((b << 8) | a);
    };
    while (i + 1 < bytes.size()) {
        std::uint32_t code = unit(i);
        i += 2;
        if (code >= 0xD800 && code <= 0xDBFF && i + 1 < bytes.size()) {
            const std::uint32_t next = unit(i);
            if (next >= 0xDC00 && next <= 0xDFFF) {
                code = 0x10000 + ((code - 0xD800) << 10) + (next - 0xDC00);
                i += 2;
            }
        }
        AppendCodePoint(out, code);
    }
    if (i < bytes.size()) {
        DecodeFailure(codec, bytes, i, bytes.size(), "truncated data");
    }
    return out;
}

std::string DecodeUtf32(std::string_view bytes, bool big_endian, const std::string& codec) {
    std::string out;
    std::size_t i = 0;
    for (; i + 3 < bytes.size(); i += 4) {
        std::uint32_t code = 0;
        for (int k = 0; k < 4; ++k) {
            const auto byte = static_cast<unsigned char>(bytes[i + static_cast<std::size_t>(big_endian ? k : 3 - k)]);
            code = (code << 8) | byte;
        }
        if (code > 0x10FFFF) {
            DecodeFailure(codec, bytes, i, i + 4, "code point not in range(0x110000)");
        }
        AppendCodePoint(out, code);
    }
    if (i < bytes.size()) {
        DecodeFailure(codec, bytes, i, bytes.size(), "truncated data");
    }
    return out;
}

// ---- Printing ----

bool IsPrintable(std::uint32_t code) {
    // str.isprintable() for the code points a report can plausibly hold.
    if (code < 0x20 || (code >= 0x7F && code <= 0xA0) || code == 0xAD) {
        return false;
    }
    if (code >= 0xD800 && code <= 0xDFFF) {
        return false;
    }
    if (code == 0x1680 || (code >= 0x2000 && code <= 0x200F) || (code >= 0x2028 && code <= 0x202F) ||
        (code >= 0x205F && code <= 0x2064) || code == 0x3000 || code == 0xFEFF || (code >= 0xFFF9 && code <= 0xFFFB)) {
        return false;
    }
    if ((code >= 0xE000 && code <= 0xF8FF) || code >= 0xF0000 || (code >= 0xE0000 && code <= 0xE007F)) {
        return false;
    }
    return !((code & 0xFFFE) == 0xFFFE);
}

std::string ReprString(const std::string& text) {
    const std::vector<std::uint32_t> codes = CodePoints(text);
    bool single = false;
    bool dbl = false;
    for (const std::uint32_t code : codes) {
        single = single || code == '\'';
        dbl = dbl || code == '"';
    }
    const char quote = single && !dbl ? '"' : '\'';
    std::string out(1, quote);
    for (const std::uint32_t code : codes) {
        if (code == static_cast<std::uint32_t>(quote) || code == '\\') {
            out += '\\';
            out += static_cast<char>(code);
        } else if (code == '\t') {
            out += "\\t";
        } else if (code == '\n') {
            out += "\\n";
        } else if (code == '\r') {
            out += "\\r";
        } else if (code < 0x20 || code == 0x7F) {
            out += "\\x" + Hex(code, 2);
        } else if (code < 0x7F) {
            out += static_cast<char>(code);
        } else if (IsPrintable(code)) {
            AppendCodePoint(out, code);
        } else if (code <= 0xFF) {
            out += "\\x" + Hex(code, 2);
        } else if (code <= 0xFFFF) {
            out += "\\u" + Hex(code, 4);
        } else {
            out += "\\U" + Hex(code, 8);
        }
    }
    out += quote;
    return out;
}

std::string GroupThousands(const std::string& digits) {
    std::string out;
    const std::size_t first = digits.size() % 3 == 0 ? 3 : digits.size() % 3;
    out += digits.substr(0, first);
    for (std::size_t i = first; i < digits.size(); i += 3) {
        out += ',';
        out += digits.substr(i, 3);
    }
    return out;
}

// Grouping of the integer part of a positional number text without a sign.
std::string GroupNumberText(const std::string& text) {
    const std::size_t point = text.find('.');
    const std::string whole = text.substr(0, point);
    if (whole.empty() || whole.find_first_not_of("0123456789") != std::string::npos) {
        return text;
    }
    return GroupThousands(whole) + (point == std::string::npos ? "" : text.substr(point));
}

std::string FixedText(double value, int precision) {
    if (std::isnan(value)) {
        return "nan";
    }
    if (std::isinf(value)) {
        return value < 0 ? "-inf" : "inf";
    }
    char buffer[512];
    const auto result = std::to_chars(buffer, buffer + sizeof(buffer), value, std::chars_format::fixed, precision);
    if (result.ec != std::errc()) {
        Fail(ErrorKind::kRuntime, "float formatting failed");
    }
    return std::string(buffer, result.ptr);
}

double FloatOf(const Value& value) {
    if (value.IsFloat()) {
        return value.AsFloat();
    }
    if (value.IsBool()) {
        return value.AsBool() ? 1.0 : 0.0;
    }
    if (value.IsInt()) {
        const std::optional<double> converted = value.AsInt().ToDouble();
        if (!converted) {
            Fail(ErrorKind::kOverflow, "int too large to convert to float");
        }
        return *converted;
    }
    Fail(ErrorKind::kValue, "Unknown format code 'f' for object of type '" +
                                std::string(value.IsString() ? "str" : value.IsNull() ? "NoneType"
                                                                     : value.IsArray() ? "list"
                                                                                       : "dict") +
                                "'");
}

void DumpString(std::string& out, const std::string& text) {
    out += '"';
    for (const std::uint32_t code : CodePoints(text)) {
        switch (code) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            default:
                if (code >= 0x20 && code < 0x7F) {
                    out += static_cast<char>(code);
                } else if (code < 0x10000) {
                    out += "\\u" + Hex(code, 4);
                } else {
                    const std::uint32_t v = code - 0x10000;
                    out += "\\u" + Hex(0xD800 + (v >> 10), 4) + "\\u" + Hex(0xDC00 + (v & 0x3FF), 4);
                }
        }
    }
    out += '"';
}

std::string IntText(const BigInt& value) {
    if (value.DecimalDigits() > kMaxIntDigits) {
        Fail(ErrorKind::kValue,
             "Exceeds the limit (4300 digits) for integer string conversion; use sys.set_int_max_str_digits() to "
             "increase the limit");
    }
    return value.ToString();
}

void Dump(std::string& out, const Value& value, int indent, int depth) {
    if (depth > kMaxNesting) {
        Fail(ErrorKind::kRuntime, "maximum recursion depth exceeded while encoding a JSON object");
    }
    switch (value.kind()) {
        case Value::Kind::kNull: out += "null"; return;
        case Value::Kind::kBool: out += value.AsBool() ? "true" : "false"; return;
        case Value::Kind::kInt: out += IntText(value.AsInt()); return;
        case Value::Kind::kFloat:
            if (!std::isfinite(value.AsFloat())) {
                Fail(ErrorKind::kValue, "Out of range float values are not JSON compliant: " + FloatRepr(value.AsFloat()));
            }
            out += FloatRepr(value.AsFloat());
            return;
        case Value::Kind::kString: DumpString(out, value.AsString()); return;
        case Value::Kind::kArray: {
            if (value.Items().empty()) {
                out += "[]";
                return;
            }
            const std::string inner(static_cast<std::size_t>(indent + 2), ' ');
            out += "[\n";
            for (std::size_t i = 0; i < value.Items().size(); ++i) {
                out += inner;
                Dump(out, value.Items()[i], indent + 2, depth + 1);
                out += i + 1 < value.Items().size() ? ",\n" : "\n";
            }
            out += std::string(static_cast<std::size_t>(indent), ' ') + "]";
            return;
        }
        case Value::Kind::kObject: {
            if (value.Members().empty()) {
                out += "{}";
                return;
            }
            const std::string inner(static_cast<std::size_t>(indent + 2), ' ');
            out += "{\n";
            for (std::size_t i = 0; i < value.Members().size(); ++i) {
                out += inner;
                DumpString(out, value.Members()[i].key);
                out += ": ";
                Dump(out, value.Members()[i].value, indent + 2, depth + 1);
                out += i + 1 < value.Members().size() ? ",\n" : "\n";
            }
            out += std::string(static_cast<std::size_t>(indent), ' ') + "}";
            return;
        }
    }
}

}  // namespace

Value ParseJson(std::string_view text) { return JsonParser(text).Document(); }

Value ParseJsonBytes(std::string_view bytes) {
    // json.detect_encoding, then decoding with surrogatepass.
    auto starts = [&](std::string_view prefix) { return bytes.substr(0, prefix.size()) == prefix; };
    std::string text;
    if (starts(std::string_view("\x00\x00\xFE\xFF", 4))) {
        text = DecodeUtf32(bytes.substr(4), true, "utf-32");
    } else if (starts("\xFF\xFE") && bytes.size() >= 4 && bytes[2] == '\0' && bytes[3] == '\0') {
        text = DecodeUtf32(bytes.substr(4), false, "utf-32");
    } else if (starts("\xFE\xFF")) {
        text = DecodeUtf16(bytes.substr(2), true, "utf-16");
    } else if (starts("\xFF\xFE")) {
        text = DecodeUtf16(bytes.substr(2), false, "utf-16");
    } else if (starts("\xEF\xBB\xBF")) {
        text = DecodeUtf8(bytes.substr(3), true);
    } else if (bytes.size() >= 4 && bytes[0] == '\0') {
        text = bytes[1] != '\0' ? DecodeUtf16(bytes, true, "utf-16-be") : DecodeUtf32(bytes, true, "utf-32-be");
    } else if (bytes.size() >= 4 && bytes[1] == '\0') {
        text = bytes[2] != '\0' || bytes[3] != '\0' ? DecodeUtf16(bytes, false, "utf-16-le")
                                                    : DecodeUtf32(bytes, false, "utf-32-le");
    } else if (bytes.size() == 2 && bytes[0] == '\0') {
        text = DecodeUtf16(bytes, true, "utf-16-be");
    } else if (bytes.size() == 2 && bytes[1] == '\0') {
        text = DecodeUtf16(bytes, false, "utf-16-le");
    } else {
        text = DecodeUtf8(bytes, true);
    }
    return ParseJson(text);
}

std::string DecodeChildText(std::string_view bytes) {
    const std::string text = DecodeUtf8(bytes, false);
    std::string out;
    out.reserve(text.size());
    for (std::size_t i = 0; i < text.size(); ++i) {
        if (text[i] == '\r') {
            out += '\n';
            if (i + 1 < text.size() && text[i + 1] == '\n') {
                ++i;
            }
        } else {
            out += text[i];
        }
    }
    return out;
}

std::optional<std::string> Utf8EncodeError(const std::string& text) {
    const std::vector<std::uint32_t> codes = CodePoints(text);
    for (std::size_t i = 0; i < codes.size(); ++i) {
        if (codes[i] < 0xD800 || codes[i] > 0xDFFF) {
            continue;
        }
        std::size_t end = i + 1;
        while (end < codes.size() && codes[end] >= 0xD800 && codes[end] <= 0xDFFF) {
            ++end;
        }
        if (end - i == 1) {
            return "'utf-8' codec can't encode character '\\u" + Hex(codes[i], 4) + "' in position " +
                   std::to_string(i) + ": surrogates not allowed";
        }
        return "'utf-8' codec can't encode characters in position " + std::to_string(i) + "-" +
               std::to_string(end - 1) + ": surrogates not allowed";
    }
    return std::nullopt;
}

std::string DumpJson(const Value& value) {
    std::string out;
    Dump(out, value, 0, 0);
    return out;
}

std::string FloatRepr(double value) {
    if (std::isnan(value)) {
        return "nan";
    }
    if (std::isinf(value)) {
        return value < 0 ? "-inf" : "inf";
    }
    if (value == 0.0) {
        return std::signbit(value) ? "-0.0" : "0.0";
    }
    char buffer[64];
    const auto result = std::to_chars(buffer, buffer + sizeof(buffer), value, std::chars_format::scientific);
    const std::string text(buffer, result.ptr);
    const bool negative = text.front() == '-';
    const std::size_t e = text.find('e');
    std::string digits;
    for (std::size_t i = negative ? 1 : 0; i < e; ++i) {
        if (text[i] != '.') {
            digits += text[i];
        }
    }
    const int exponent = std::stoi(text.substr(e + 1));
    const int point = exponent + 1;  // value = 0.DIGITS * 10**point
    std::string out = negative ? "-" : "";
    if (point <= -4 || point > 16) {
        out += digits.substr(0, 1);
        if (digits.size() > 1) {
            out += "." + digits.substr(1);
        }
        const int shown = point - 1;
        std::string magnitude = std::to_string(shown < 0 ? -shown : shown);
        if (magnitude.size() < 2) {
            magnitude = "0" + magnitude;
        }
        out += std::string("e") + (shown < 0 ? "-" : "+") + magnitude;
    } else if (point <= 0) {
        out += "0." + std::string(static_cast<std::size_t>(-point), '0') + digits;
    } else if (static_cast<std::size_t>(point) >= digits.size()) {
        out += digits + std::string(static_cast<std::size_t>(point) - digits.size(), '0') + ".0";
    } else {
        out += digits.substr(0, static_cast<std::size_t>(point)) + "." + digits.substr(static_cast<std::size_t>(point));
    }
    return out;
}

std::string PyRepr(const Value& value) {
    switch (value.kind()) {
        case Value::Kind::kNull: return "None";
        case Value::Kind::kBool: return value.AsBool() ? "True" : "False";
        case Value::Kind::kInt: return IntText(value.AsInt());
        case Value::Kind::kFloat: return FloatRepr(value.AsFloat());
        case Value::Kind::kString: return ReprString(value.AsString());
        case Value::Kind::kArray: {
            std::string out = "[";
            for (std::size_t i = 0; i < value.Items().size(); ++i) {
                out += (i == 0 ? "" : ", ") + PyRepr(value.Items()[i]);
            }
            return out + "]";
        }
        case Value::Kind::kObject: {
            std::string out = "{";
            for (std::size_t i = 0; i < value.Members().size(); ++i) {
                out += (i == 0 ? "" : ", ") + ReprString(value.Members()[i].key) + ": " +
                       PyRepr(value.Members()[i].value);
            }
            return out + "}";
        }
    }
    return {};
}

std::string PyStr(const Value& value) { return value.IsString() ? value.AsString() : PyRepr(value); }

std::string FormatNumber(const Value& value, std::string_view spec) {
    if (spec == ",") {
        if (value.IsInt() || value.IsBool()) {
            const std::string text = value.IsBool() ? (value.AsBool() ? "1" : "0") : IntText(value.AsInt());
            const bool negative = !text.empty() && text.front() == '-';
            return (negative ? "-" : "") + GroupThousands(text.substr(negative ? 1 : 0));
        }
        if (value.IsFloat()) {
            const std::string text = FloatRepr(value.AsFloat());
            const bool negative = text.front() == '-';
            return (negative ? "-" : "") + GroupNumberText(text.substr(negative ? 1 : 0));
        }
        Fail(ErrorKind::kValue, "Cannot specify ',' with 's'.");
    }
    bool plus = false;
    bool group = false;
    std::string_view rest = spec;
    if (!rest.empty() && rest.front() == '+') {
        plus = true;
        rest.remove_prefix(1);
    }
    if (!rest.empty() && rest.front() == ',') {
        group = true;
        rest.remove_prefix(1);
    }
    // ".Nf"
    const int precision = std::stoi(std::string(rest.substr(1, rest.size() - 2)));
    const double number = FloatOf(value);
    std::string text = FixedText(number, precision);
    const bool negative = !text.empty() && text.front() == '-';
    std::string body = text.substr(negative ? 1 : 0);
    if (group) {
        body = GroupNumberText(body);
    }
    return (negative ? "-" : (plus ? "+" : "")) + body;
}

bool PyEqual(const Value& a, const Value& b) {
    const bool a_number = a.IsBool() || a.IsInt() || a.IsFloat();
    const bool b_number = b.IsBool() || b.IsInt() || b.IsFloat();
    if (a_number || b_number) {
        if (!a_number || !b_number) {
            return false;
        }
        if (a.IsFloat() && b.IsFloat()) {
            return a.AsFloat() == b.AsFloat();
        }
        auto integer = [](const Value& v) { return v.IsBool() ? BigInt(v.AsBool() ? 1 : 0) : v.AsInt(); };
        if (!a.IsFloat() && !b.IsFloat()) {
            return Compare(integer(a), integer(b)) == 0;
        }
        const double f = a.IsFloat() ? a.AsFloat() : b.AsFloat();
        if (!std::isfinite(f)) {
            return false;
        }
        return CompareExact(integer(a.IsFloat() ? b : a), f) == 0;
    }
    if (a.kind() != b.kind()) {
        return false;
    }
    switch (a.kind()) {
        case Value::Kind::kNull: return true;
        case Value::Kind::kString: return a.AsString() == b.AsString();
        case Value::Kind::kArray:
            if (a.Items().size() != b.Items().size()) {
                return false;
            }
            for (std::size_t i = 0; i < a.Items().size(); ++i) {
                if (!PyEqual(a.Items()[i], b.Items()[i])) {
                    return false;
                }
            }
            return true;
        case Value::Kind::kObject:
            if (a.Members().size() != b.Members().size()) {
                return false;
            }
            for (const Member& member : a.Members()) {
                const Value* other = b.Find(member.key);
                if (other == nullptr || !PyEqual(member.value, *other)) {
                    return false;
                }
            }
            return true;
        default: return false;
    }
}

// ---- Python's random.Random ----

PythonRandom::PythonRandom(std::uint64_t seed) {
    // random_seed(): the absolute value as little-endian 32-bit words.
    std::vector<std::uint32_t> key{static_cast<std::uint32_t>(seed)};
    if ((seed >> 32) != 0) {
        key.push_back(static_cast<std::uint32_t>(seed >> 32));
    }
    // init_genrand(19650218)
    state_[0] = 19650218U;
    for (std::uint32_t i = 1; i < 624; ++i) {
        state_[i] = 1812433253U * (state_[i - 1] ^ (state_[i - 1] >> 30)) + i;
    }
    // init_by_array(key)
    std::uint32_t i = 1;
    std::uint32_t j = 0;
    const auto length = static_cast<std::uint32_t>(key.size());
    for (std::uint32_t k = std::max<std::uint32_t>(624, length); k != 0; --k) {
        state_[i] = (state_[i] ^ ((state_[i - 1] ^ (state_[i - 1] >> 30)) * 1664525U)) + key[j] + j;
        ++i;
        ++j;
        if (i >= 624) {
            state_[0] = state_[623];
            i = 1;
        }
        if (j >= length) {
            j = 0;
        }
    }
    for (std::uint32_t k = 623; k != 0; --k) {
        state_[i] = (state_[i] ^ ((state_[i - 1] ^ (state_[i - 1] >> 30)) * 1566083941U)) - i;
        ++i;
        if (i >= 624) {
            state_[0] = state_[623];
            i = 1;
        }
    }
    state_[0] = 0x80000000U;
    index_ = 624;
}

std::uint32_t PythonRandom::NextUint32() {
    constexpr std::uint32_t kMag01[2] = {0U, 0x9908B0DFU};
    if (index_ >= 624) {
        int k = 0;
        for (; k < 624 - 397; ++k) {
            const std::uint32_t y = (state_[k] & 0x80000000U) | (state_[k + 1] & 0x7FFFFFFFU);
            state_[k] = state_[k + 397] ^ (y >> 1) ^ kMag01[y & 1U];
        }
        for (; k < 623; ++k) {
            const std::uint32_t y = (state_[k] & 0x80000000U) | (state_[k + 1] & 0x7FFFFFFFU);
            state_[k] = state_[k + (397 - 624)] ^ (y >> 1) ^ kMag01[y & 1U];
        }
        const std::uint32_t y = (state_[623] & 0x80000000U) | (state_[0] & 0x7FFFFFFFU);
        state_[623] = state_[396] ^ (y >> 1) ^ kMag01[y & 1U];
        index_ = 0;
    }
    std::uint32_t y = state_[index_++];
    y ^= (y >> 11);
    y ^= (y << 7) & 0x9D2C5680U;
    y ^= (y << 15) & 0xEFC60000U;
    y ^= (y >> 18);
    return y;
}

std::uint32_t PythonRandom::GetRandBits(int k) {
    if (k == 0) {
        return 0;
    }
    return NextUint32() >> (32 - k);
}

std::uint32_t PythonRandom::RandBelow(std::uint32_t n) {
    // _randbelow_with_getrandbits: k = n.bit_length(), redraw while r >= n.
    int k = 0;
    for (std::uint32_t rest = n; rest != 0; rest >>= 1) {
        ++k;
    }
    std::uint32_t r = GetRandBits(k);
    while (r >= n) {
        r = GetRandBits(k);
    }
    return r;
}

}  // namespace chunkdb::budget_compare
