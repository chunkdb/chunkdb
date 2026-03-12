#include <cassert>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include "chunkdb/protocol.hpp"

int main() {
    {
        const chunkdb::ParsedCommand command = chunkdb::Protocol::ParseLine("set 10 20 1010\r\n");
        assert(command.name == "SET");
        assert(command.args.size() == 3);
        assert(command.args[0] == "10");
        assert(command.args[1] == "20");
        assert(command.args[2] == "1010");
    }

    {
        bool thrown = false;
        try {
            (void)chunkdb::Protocol::ParseLine("   \r\n");
        } catch (const std::invalid_argument&) {
            thrown = true;
        }
        assert(thrown);
    }

    assert(chunkdb::Protocol::SimpleString("OK") == "+OK\r\n");
    assert(chunkdb::Protocol::Error("AUTH_REQUIRED", "use AUTH") == "-ERR AUTH_REQUIRED use AUTH\r\n");
    assert(chunkdb::Protocol::Bulk("1010") == "$4\r\n1010\r\n");
    assert(chunkdb::Protocol::BulkBytes(std::vector<std::uint8_t>{0xAA, 0xBB}) == "$2\r\n\xAA\xBB\r\n");

    return 0;
}
