#include <cassert>
#include <stdexcept>

#include "chunkdb/uri.hpp"

int main() {
    {
        const auto uri = chunkdb::ParseConnectionUri("chunk://bot:hunter2@localhost:4242/");
        assert(!uri.secure);
        assert(uri.user == "bot" && uri.password == "hunter2");
        assert(uri.host == "localhost");
        assert(uri.port == 4242);
        assert(uri.path == "/");
    }

    {
        const auto uri = chunkdb::ParseConnectionUri("chunks://abc@127.0.0.1/");
        assert(uri.secure);
        assert(uri.user == "abc" && uri.password.empty());
        assert(uri.host == "127.0.0.1");
        assert(uri.port == 4242);
    }

    {
        // Escapes let a password hold ':', '@' and '/'.
        const auto uri = chunkdb::ParseConnectionUri("chunk://bot:a%3Ab%40c%2Fd@host:1/world");
        assert(uri.user == "bot" && uri.password == "a:b@c/d" && uri.host == "host" && uri.path == "/world");
        bool thrown = false;
        try {
            (void)chunkdb::ParseConnectionUri("chunk://bot:%zz@host/");
        } catch (const std::invalid_argument&) {
            thrown = true;
        }
        assert(thrown);
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
