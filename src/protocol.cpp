#include "chunkdb/protocol.hpp"

#include <array>
#include <cctype>
#include <charconv>
#include <cmath>
#include <stdexcept>

namespace chunkdb {

namespace {

std::string_view TrimTrailingCrLf(std::string_view line) {
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
        line.remove_suffix(1);
    }
    return line;
}

}  // namespace

bool Protocol::CommandEquals(std::string_view actual, std::string_view expected_upper) noexcept {
    if (actual.size() != expected_upper.size()) {
        return false;
    }

    for (std::size_t i = 0; i < actual.size(); ++i) {
        const char upper = static_cast<char>(
            std::toupper(static_cast<unsigned char>(actual[i])));
        if (upper != expected_upper[i]) {
            return false;
        }
    }

    return true;
}

std::string Protocol::SimpleString(std::string_view text) {
    return "+" + std::string(text) + "\r\n";
}

std::string Protocol::Error(std::string_view code, std::string_view message) {
    std::string result = "-ERR " + std::string(code);
    if (!message.empty()) {
        result += " " + std::string(message);
    }
    result += "\r\n";
    return result;
}

std::string Protocol::Bulk(std::string_view payload) {
    return "$" + std::to_string(payload.size()) + "\r\n" + std::string(payload) + "\r\n";
}

void Protocol::AppendBulk(std::string& out, std::string_view bytes) {
    out += '$';
    out += std::to_string(bytes.size());
    out += "\r\n";
    out += bytes;
    out += "\r\n";
}

void Protocol::AppendInteger(std::string& out, std::int64_t value) {
    out += ':';
    out += std::to_string(value);
    out += "\r\n";
}

void Protocol::AppendInteger(std::string& out, std::uint64_t value) {
    out += ':';
    out += std::to_string(value);
    out += "\r\n";
}

namespace {

template <typename Float>
void AppendFloating(std::string& out, Float value) {
    out += ',';
    if (std::isnan(value)) {
        out += "nan";
    } else if (std::isinf(value)) {
        out += value < 0 ? "-inf" : "inf";
    } else {
        std::array<char, 64> text{};
        const auto result = std::to_chars(text.data(), text.data() + text.size(), value);
        out.append(text.data(), result.ptr);
    }
    out += "\r\n";
}

}  // namespace

void Protocol::AppendDouble(std::string& out, double value) {
    AppendFloating(out, value);
}

void Protocol::AppendFloat(std::string& out, float value) {
    AppendFloating(out, value);
}

void Protocol::AppendBoolean(std::string& out, bool value) {
    out += value ? "#t\r\n" : "#f\r\n";
}

void Protocol::AppendNull(std::string& out) {
    out += "_\r\n";
}

void Protocol::AppendArrayHeader(std::string& out, std::size_t items) {
    out += '*';
    out += std::to_string(items);
    out += "\r\n";
}

void Protocol::AppendMapHeader(std::string& out, std::size_t pairs) {
    out += '%';
    out += std::to_string(pairs);
    out += "\r\n";
}

std::optional<std::size_t> Protocol::ParseFrameHeader(std::string_view line) {
    line = TrimTrailingCrLf(line);
    if (line == "$-1") {
        return std::nullopt;
    }
    std::uint64_t length = 0;
    const auto* begin = line.data() + 1;
    const auto* end = line.data() + line.size();
    if (line.size() < 2 || line.front() != '$' || line[1] == '+' || line[1] == '-') {
        throw std::invalid_argument("expected a parameter frame $<length> or $-1");
    }
    const auto result = std::from_chars(begin, end, length, 10);
    if (result.ec != std::errc() || result.ptr != end) {
        throw std::invalid_argument("expected a parameter frame $<length> or $-1");
    }
    return static_cast<std::size_t>(length);
}

}  // namespace chunkdb
