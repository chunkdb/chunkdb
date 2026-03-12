#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace chunkdb {

struct ParsedCommand {
    std::string name;
    std::vector<std::string> args;
};

class Protocol {
  public:
    static ParsedCommand ParseLine(std::string_view line);

    [[nodiscard]] static std::string SimpleString(std::string_view text);
    [[nodiscard]] static std::string Error(std::string_view code, std::string_view message);
    [[nodiscard]] static std::string Bulk(std::string_view payload);
    [[nodiscard]] static std::string BulkBytes(const std::vector<std::uint8_t>& payload);
};

}  // namespace chunkdb
