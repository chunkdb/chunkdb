#include "chunkdb/protocol.hpp"

#include <cctype>
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

std::string Protocol::BulkBytes(const std::vector<std::uint8_t>& payload) {
    std::string result = "$" + std::to_string(payload.size()) + "\r\n";
    if (!payload.empty()) {
        result.append(reinterpret_cast<const char*>(payload.data()), payload.size());
    }
    result += "\r\n";
    return result;
}

}  // namespace chunkdb
