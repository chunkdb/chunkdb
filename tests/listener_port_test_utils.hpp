#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>

namespace chunkdb::test {

// The `port` field of the first log line with `message`, read from the file a
// process writes its output to; empty until that line is complete. A process
// given port 0 picks a free port and keeps it, so no other listener can take
// it between a choice and the bind.
inline std::optional<std::uint16_t> LogLinePort(const std::filesystem::path& log, const std::string& message) {
    std::ifstream input(log, std::ios::binary);
    std::string line;
    while (std::getline(input, line)) {
        if (input.eof()) break;  // A line without its newline is still being written.
        if (line.find(" " + message + " ") == std::string::npos) continue;
        const auto at = line.find(" port=");
        if (at == std::string::npos) throw std::runtime_error("log line without a port: " + line);
        return static_cast<std::uint16_t>(std::stoul(line.substr(at + 6U)));
    }
    return std::nullopt;
}

// The port a chunkdb_server process names in its INFO line `ready to accept
// connections`.
inline std::optional<std::uint16_t> ReadyLinePort(const std::filesystem::path& log) {
    return LogLinePort(log, "ready to accept connections");
}

}  // namespace chunkdb::test
