#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace chunkdb {

struct ParsedCommand {
    std::string name;
    std::vector<std::string> args;
};

// A request line split into the command name and at most 16 arguments;
// commands that take more (MSET, MGET, CHUNKBATCH, TABLECREATE, TABLESET)
// split their lines themselves.
struct ParsedCommandView {
    std::string_view name;
    std::array<std::string_view, 16> args{};
    std::size_t argc = 0;
};

class Protocol {
  public:
    static ParsedCommand ParseLine(std::string_view line);
    static ParsedCommandView ParseLineView(std::string_view line);

    [[nodiscard]] static bool CommandEquals(std::string_view actual, std::string_view expected_upper) noexcept;

    [[nodiscard]] static std::string SimpleString(std::string_view text);
    [[nodiscard]] static std::string Error(std::string_view code, std::string_view message);
    [[nodiscard]] static std::string Bulk(std::string_view payload);
    [[nodiscard]] static std::string BulkBytes(const std::vector<std::uint8_t>& payload);
    [[nodiscard]] static std::string Array(const std::vector<std::string>& items);
    // `$-1`: no value (an unset block).
    [[nodiscard]] static std::string Null();
    // An array whose absent items are `$-1`.
    [[nodiscard]] static std::string Array(const std::vector<std::optional<std::string>>& items);
};

}  // namespace chunkdb
