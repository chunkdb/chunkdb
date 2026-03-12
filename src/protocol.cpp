#include "chunkdb/protocol.hpp"

#include <cctype>
#include <stdexcept>

namespace chunkdb {

ParsedCommand Protocol::ParseLine(std::string_view line) {
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
        line.remove_suffix(1);
    }

    std::vector<std::string> tokens;
    std::size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && line[i] == ' ') {
            ++i;
        }
        if (i >= line.size()) {
            break;
        }
        std::size_t begin = i;
        while (i < line.size() && line[i] != ' ') {
            ++i;
        }
        tokens.emplace_back(line.substr(begin, i - begin));
    }

    if (tokens.empty()) {
        throw std::invalid_argument("empty command");
    }

    ParsedCommand parsed;
    parsed.name = tokens.front();
    for (char& c : parsed.name) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }

    for (std::size_t t = 1; t < tokens.size(); ++t) {
        parsed.args.push_back(std::move(tokens[t]));
    }

    return parsed;
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

}  // namespace chunkdb
