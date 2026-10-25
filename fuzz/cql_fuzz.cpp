#include "fuzz_common.hpp"
#include "cql.hpp"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size > 65536U) return 0; // The server's request-line limit.
    chunkdb::fuzz::Rejectable([&] {
        const auto text = chunkdb::fuzz::Text(data, size);
        (void)chunkdb::cql::MayHaveParameters(text);
        (void)chunkdb::cql::Parse(text);
    });
    return 0;
}
