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

ParsedCommand Protocol::ParseLine(std::string_view line) {
    const ParsedCommandView view = ParseLineView(line);

    ParsedCommand parsed;
    parsed.name.assign(view.name.begin(), view.name.end());
    for (char& c : parsed.name) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }

    parsed.args.reserve(view.argc);
    for (std::size_t i = 0; i < view.argc; ++i) {
        parsed.args.emplace_back(view.args[i]);
    }

    return parsed;
}

ParsedCommandView Protocol::ParseLineView(std::string_view line) {
    line = TrimTrailingCrLf(line);

    ParsedCommandView parsed;

    std::size_t i = 0;
    while (i < line.size() && line[i] == ' ') {
        ++i;
    }
    if (i >= line.size()) {
        throw std::invalid_argument("empty command");
    }

    const std::size_t name_begin = i;
    while (i < line.size() && line[i] != ' ') {
        ++i;
    }
    parsed.name = line.substr(name_begin, i - name_begin);

    while (i < line.size()) {
        while (i < line.size() && line[i] == ' ') {
            ++i;
        }
        if (i >= line.size()) {
            break;
        }

        const std::size_t arg_begin = i;
        while (i < line.size() && line[i] != ' ') {
            ++i;
        }

        if (parsed.argc >= parsed.args.size()) {
            throw std::invalid_argument("too many command arguments");
        }

        parsed.args[parsed.argc] = line.substr(arg_begin, i - arg_begin);
        ++parsed.argc;
    }

    return parsed;
}

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

std::string Protocol::Array(const std::vector<std::string>& items) {
    std::string result = "*" + std::to_string(items.size()) + "\r\n";
    for (const auto& item : items) {
        result += Bulk(item);
    }
    return result;
}

std::string Protocol::Null() {
    return "$-1\r\n";
}

std::string Protocol::Array(const std::vector<std::optional<std::string>>& items) {
    std::string result = "*" + std::to_string(items.size()) + "\r\n";
    for (const auto& item : items) {
        result += item.has_value() ? Bulk(*item) : Null();
    }
    return result;
}

std::string Protocol::BulkBytes(const std::vector<std::uint8_t>& payload) {
    std::string result = "$" + std::to_string(payload.size()) + "\r\n";
    if (!payload.empty()) {
        result.append(reinterpret_cast<const char*>(payload.data()), payload.size());
    }
    result += "\r\n";
    return result;
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
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
        line.remove_suffix(1);
    }
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
