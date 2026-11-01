#include <cassert>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "chunkdb/types.hpp"
#include "crypto.hpp"

namespace {
bool fail = false;
unsigned calls = 0U;
}
namespace chunkdb::crypto {
// A standalone link substitute: no operating-system random source is needed
// to prove that seed failure propagates rather than selecting a fixed salt.
std::vector<std::uint8_t> RandomBytes(std::size_t bytes) {
    assert(bytes == 8U);
    ++calls;
    if (fail) throw std::runtime_error("random source unavailable");
    return std::vector<std::uint8_t>(bytes, 0xA5U);
}
}

int main(int argc, char** argv) {
    fail = argc == 2 && std::string_view(argv[1]) == "--failure";
    if (fail) {
        bool failed = false;
        try { (void)chunkdb::ChunkCoordHash{}; }
        catch (const std::runtime_error&) { failed = true; }
        assert(failed && calls == 1U);
        return 0;
    }
    assert(argc == 1);
    const chunkdb::ChunkCoordHash first;
    const chunkdb::LargeChunkCoordHash second;
    const auto expected = chunkdb::ChunkCoordHash{UINT64_C(0xa5a5a5a5a5a5a5a5)}({1, 2, 3});
    assert(first({1, 2, 3}) == expected && second({1, 2, 3}) == expected);
    assert(first({1, 2, 3}) == first({1, 2, 3}) && calls == 1U);
}
