#include <cassert>
#include <stdexcept>

#include "chunkdb/uri.hpp"

int main() {
    {
        const auto uri = chunkdb::ParseConnectionUri("chunk://token@localhost:4242/");
        assert(!uri.secure);
        assert(uri.token == "token");
        assert(uri.host == "localhost");
        assert(uri.port == 4242);
        assert(uri.path == "/");
    }

    {
        const auto uri = chunkdb::ParseConnectionUri("chunks://abc@127.0.0.1/");
        assert(uri.secure);
        assert(uri.token == "abc");
        assert(uri.host == "127.0.0.1");
        assert(uri.port == 4242);
    }

    {
        bool thrown = false;
        try {
            (void)chunkdb::ParseConnectionUri("http://localhost:1/");
        } catch (const std::invalid_argument&) {
            thrown = true;
        }
        assert(thrown);
    }

    {
        bool thrown = false;
        try {
            (void)chunkdb::ParseConnectionUri("chunk://:4242/");
        } catch (const std::invalid_argument&) {
            thrown = true;
        }
        assert(thrown);
    }

    return 0;
}
