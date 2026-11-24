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

// A URI a client connects to names ports 1-65535; one a server listens on may
// name port 0, a free port the system picks.
enum class UriUse { kConnect, kListen };

[[nodiscard]] ConnectionUri ParseConnectionUri(const std::string& uri, UriUse use = UriUse::kConnect);

}  // namespace chunkdb
