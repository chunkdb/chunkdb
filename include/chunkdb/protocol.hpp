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

struct ParsedCommandView {
    std::string_view name;
    std::array<std::string_view, 8> args{};
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

    // RESP3 value types of protocol 3 (docs/CQL_DESIGN.md), appended to `out`.
    static void AppendBulk(std::string& out, std::string_view bytes);
    // `:<n>`; an unsigned value above the int64 range is written as it is.
    static void AppendInteger(std::string& out, std::int64_t value);
    static void AppendInteger(std::string& out, std::uint64_t value);
    // `,<n>`, the shortest form that reads back as the same double, or
    // `inf`, `-inf`, `nan`.
    static void AppendDouble(std::string& out, double value);
    static void AppendFloat(std::string& out, float value);
    // `#t` or `#f`.
    static void AppendBoolean(std::string& out, bool value);
    // `_`: NULL, or an absent block.
    static void AppendNull(std::string& out);
    // `*<n>` and `%<n>`: the header of an array of n items, of a map of n
    // pairs.
    static void AppendArrayHeader(std::string& out, std::size_t items);
    static void AppendMapHeader(std::string& out, std::size_t pairs);
    // A bulk frame header after a protocol 3 statement: the length of the
    // value, or std::nullopt for `$-1` (NULL). Throws std::invalid_argument
    // for anything else.
    [[nodiscard]] static std::optional<std::size_t> ParseFrameHeader(std::string_view line);
};

}  // namespace chunkdb
