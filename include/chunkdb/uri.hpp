#pragma once

#include <cstdint>
#include <string>

namespace chunkdb {

// chunk[s]://[user[:password]@]host[:port][/table]; the user and password
// are percent-decoded.
struct ConnectionUri {
    bool secure = false;
    std::string user;
    std::string password;
    std::string host;
    std::uint16_t port = 4242;
    std::string path = "/";
};

[[nodiscard]] ConnectionUri ParseConnectionUri(const std::string& uri);

}  // namespace chunkdb
