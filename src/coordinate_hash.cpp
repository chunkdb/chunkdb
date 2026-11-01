#include "chunkdb/types.hpp"

#include "crypto.hpp"

namespace chunkdb::detail {

std::uint64_t CoordinateHashSeed() {
    static const auto seed = [] {
        const auto bytes = crypto::RandomBytes(sizeof(std::uint64_t));
        std::uint64_t result = 0U;
        for (std::size_t i = 0; i < bytes.size(); ++i)
            result |= static_cast<std::uint64_t>(bytes[i]) << (8U * i);
        return result;
    }();
    return seed;
}

}  // namespace chunkdb::detail
