#include "chunkdb/uri.hpp"

#include <stdexcept>

namespace chunkdb {

ConnectionUri ParseConnectionUri(const std::string& uri) {
    const std::string scheme_delimiter = "://";
    const auto scheme_pos = uri.find(scheme_delimiter);
    if (scheme_pos == std::string::npos) {
        throw std::invalid_argument("connection URI must contain '://'");
    }

    const std::string scheme = uri.substr(0, scheme_pos);
    ConnectionUri parsed;
    if (scheme == "chunk") {
        parsed.secure = false;
    } else if (scheme == "chunks") {
        parsed.secure = true;
    } else {
        throw std::invalid_argument("unsupported URI scheme: " + scheme);
    }

    const std::string rest = uri.substr(scheme_pos + scheme_delimiter.size());
    const auto slash_pos = rest.find('/');
    const std::string authority = slash_pos == std::string::npos ? rest : rest.substr(0, slash_pos);
    parsed.path = slash_pos == std::string::npos ? "/" : rest.substr(slash_pos);

    if (authority.empty()) {
        throw std::invalid_argument("URI authority is empty");
    }

    std::string host_port = authority;
    const auto at_pos = authority.find('@');
    if (at_pos != std::string::npos) {
        parsed.token = authority.substr(0, at_pos);
        host_port = authority.substr(at_pos + 1);
    }

    if (host_port.empty()) {
        throw std::invalid_argument("URI host is empty");
    }

    const auto colon_pos = host_port.rfind(':');
    if (colon_pos == std::string::npos) {
        parsed.host = host_port;
        return parsed;
    }

    parsed.host = host_port.substr(0, colon_pos);
    const std::string port_text = host_port.substr(colon_pos + 1);
    if (parsed.host.empty() || port_text.empty()) {
        throw std::invalid_argument("invalid host:port section");
    }

    std::size_t consumed = 0;
    const int port_value = std::stoi(port_text, &consumed, 10);
    if (consumed != port_text.size() || port_value <= 0 || port_value > 65535) {
        throw std::invalid_argument("invalid port: " + port_text);
    }
    parsed.port = static_cast<std::uint16_t>(port_value);
    return parsed;
}

}  // namespace chunkdb
