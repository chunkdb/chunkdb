#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "chunkdb/protocol.hpp"
#include "../src/feed_protocol.hpp"

namespace {

bool FrameHeaderThrows(std::string_view line) {
    try {
        (void)chunkdb::Protocol::ParseFrameHeader(line);
    } catch (const std::invalid_argument&) {
        return true;
    }
    return false;
}

}  // namespace

int main() {
    assert(chunkdb::Protocol::CommandEquals("SeT", "SET"));
    assert(chunkdb::Protocol::CommandEquals("hello", "HELLO"));
    assert(!chunkdb::Protocol::CommandEquals("SETX", "SET"));
    assert(!chunkdb::Protocol::CommandEquals("SE", "SET"));
    assert(!chunkdb::Protocol::CommandEquals("", "SET"));

    assert(chunkdb::Protocol::SimpleString("OK") == "+OK\r\n");
    assert(chunkdb::Protocol::Error("AUTH_REQUIRED", "use HELLO 3 USER <name> $1") ==
           "-ERR AUTH_REQUIRED reason=auth_required use HELLO 3 USER <name> $1\r\n");
    assert(chunkdb::Protocol::Error("INTERNAL", "") == "-ERR INTERNAL reason=internal\r\n");
    assert(chunkdb::Protocol::Error("INVALID_ARGUMENT", "a\r\nsecond reply", {
        {"reason", "value"}, {"table", "hello world%=\n"}, {"future", "\t"}}) ==
        "-ERR INVALID_ARGUMENT reason=value table=hello%20world%25%3D%0A future=%09 a  second reply\r\n");
    assert(chunkdb::Protocol::Error("SCHEMA_MISMATCH", "encode again", {{"current", "18446744073709551615"}}) ==
        "-ERR SCHEMA_MISMATCH current=18446744073709551615 reason=schema_mismatch encode again\r\n");
    assert(chunkdb::Protocol::Error("INVALID_ARGUMENT", "", {{"future", "\xC2\xA0\xCE\xBB"}}) ==
        "-ERR INVALID_ARGUMENT future=%C2%A0%CE%BB reason=invalid_argument\r\n");
    bool bad_error_key = false;
    try { (void)chunkdb::Protocol::Error("INTERNAL", "", {{"bad key", "value"}}); }
    catch (const std::invalid_argument&) { bad_error_key = true; }
    assert(bad_error_key);
    assert(chunkdb::Protocol::Bulk("1010") == "$4\r\n1010\r\n");
    assert(chunkdb::Protocol::Bulk(std::string_view("\xAA\x00\xBB", 3)) == std::string("$3\r\n\xAA\x00\xBB\r\n", 9));
    assert(chunkdb::Protocol::Bulk("") == "$0\r\n\r\n");

    // RESP3 values.
    {
        std::string out;
        chunkdb::Protocol::AppendBulk(out, std::string_view("\x00\xff", 2));
        assert(out == std::string("$2\r\n\x00\xff\r\n", 8));
    }
    {
        std::string out;
        chunkdb::Protocol::AppendInteger(out, std::int64_t{-12});
        chunkdb::Protocol::AppendInteger(out, std::numeric_limits<std::int64_t>::min());
        chunkdb::Protocol::AppendInteger(out, static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()));
        chunkdb::Protocol::AppendInteger(out, static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) + 1U);
        // Above the int64 range, encoded as a RESP3 big number.
        chunkdb::Protocol::AppendInteger(out, std::numeric_limits<std::uint64_t>::max());
        assert(out == ":-12\r\n:-9223372036854775808\r\n:9223372036854775807\r\n(9223372036854775808\r\n(18446744073709551615\r\n");
    }
    {
        std::string out;
        chunkdb::Protocol::AppendDouble(out, 1.5);
        chunkdb::Protocol::AppendDouble(out, 2e-3);
        chunkdb::Protocol::AppendDouble(out, -7.0);
        chunkdb::Protocol::AppendDouble(out, std::numeric_limits<double>::infinity());
        chunkdb::Protocol::AppendDouble(out, -std::numeric_limits<double>::infinity());
        chunkdb::Protocol::AppendDouble(out, std::numeric_limits<double>::quiet_NaN());
        assert(out == ",1.5\r\n,0.002\r\n,-7\r\n,inf\r\n,-inf\r\n,nan\r\n");
    }
    {
        chunkdb::FeedEntry entry;
        entry.kind = chunkdb::FeedEntry::Kind::kChange;
        entry.position.revision = std::numeric_limits<std::uint64_t>::max();
        entry.commit_time_ms = entry.position.revision;
        entry.schema_version = entry.position.revision;
        const auto out = chunkdb::EncodeFeedEntry(entry);
        assert(out.find(":18446744073709551615") == std::string::npos);
        const auto first = out.find("(18446744073709551615\r\n");
        const auto second = out.find("(18446744073709551615\r\n", first + 1U);
        const auto third = out.find("(18446744073709551615\r\n", second + 1U);
        assert(first != std::string::npos && second != std::string::npos && third != std::string::npos);
    }
    {
        // The shortest form that reads back as the same float.
        std::string out;
        chunkdb::Protocol::AppendFloat(out, 0.1F);
        chunkdb::Protocol::AppendFloat(out, -std::numeric_limits<float>::infinity());
        assert(out == ",0.1\r\n,-inf\r\n");
    }
    {
        std::string out;
        chunkdb::Protocol::AppendBoolean(out, true);
        chunkdb::Protocol::AppendBoolean(out, false);
        chunkdb::Protocol::AppendNull(out);
        chunkdb::Protocol::AppendArrayHeader(out, 3);
        chunkdb::Protocol::AppendMapHeader(out, 2);
        chunkdb::Protocol::AppendArrayHeader(out, 0);
        assert(out == "#t\r\n#f\r\n_\r\n*3\r\n%2\r\n*0\r\n");
    }

    // Parameter frame headers: $<length> or $-1 (NULL).
    assert(chunkdb::Protocol::ParseFrameHeader("$5\r\n") == std::optional<std::size_t>{5});
    assert(chunkdb::Protocol::ParseFrameHeader("$0\r\n") == std::optional<std::size_t>{0});
    assert(chunkdb::Protocol::ParseFrameHeader("$1048576") == std::optional<std::size_t>{1048576});
    assert(!chunkdb::Protocol::ParseFrameHeader("$-1\r\n").has_value());
    assert(!chunkdb::Protocol::ParseFrameHeader("$-1\n").has_value());
    for (const char* bad : {"\r\n", "$\r\n", "5\r\n", "+5\r\n", "$+5\r\n", "$-2\r\n", "$-0\r\n", "$5x\r\n",
                            "$ 5\r\n", "$5 \r\n", "$x\r\n", "$99999999999999999999\r\n", "GET BLOCK 0 0 FROM t\r\n"}) {
        assert(FrameHeaderThrows(bad));
    }

    return 0;
}
