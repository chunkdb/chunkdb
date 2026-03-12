#pragma once

#include <cstdint>
#include <string>

namespace chunkdb {

struct ConnectionUri {
    bool secure = false;
    std::string token;
    std::string host;
    std::uint16_t port = 6752;
    std::string path = "/";
};

[[nodiscard]] ConnectionUri ParseConnectionUri(const std::string& uri);

}  // namespace chunkdb
