#include "chunkdb/crc32.hpp"

#include <array>
#include <cstring>

#if defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#endif

namespace chunkdb {

namespace {

#if !defined(__ARM_FEATURE_CRC32)
constexpr std::uint32_t kPolynomial = 0xEDB88320U;

// Slicing-by-8 tables: table[0] is the byte-at-a-time table, table[k][i] the
// CRC of byte i followed by k zero bytes.
struct Tables {
    std::array<std::array<std::uint32_t, 256>, 8> v{};
};

const Tables& SliceTables() noexcept {
    static const Tables tables = []() noexcept {
        Tables t;
        for (std::uint32_t i = 0; i < 256; ++i) {
            std::uint32_t value = i;
            for (int bit = 0; bit < 8; ++bit) {
                value = (value & 1U) != 0U ? (value >> 1U) ^ kPolynomial : value >> 1U;
            }
            t.v[0][i] = value;
        }
        for (std::uint32_t i = 0; i < 256; ++i) {
            for (std::size_t k = 1; k < 8; ++k) {
                t.v[k][i] = (t.v[k - 1][i] >> 8U) ^ t.v[0][t.v[k - 1][i] & 0xFFU];
            }
        }
        return t;
    }();
    return tables;
}
#endif

}  // namespace

std::uint32_t Crc32(const std::uint8_t* data, std::size_t length) noexcept {
    std::uint32_t crc = 0xFFFFFFFFU;
#if defined(__ARM_FEATURE_CRC32)
    // The CRC32 instructions compute this polynomial (not CRC32C).
    for (; length >= 8U; data += 8, length -= 8U) {
        std::uint64_t word = 0;
        std::memcpy(&word, data, 8);
        crc = __crc32d(crc, word);
    }
    for (; length > 0U; ++data, --length) {
        crc = __crc32b(crc, *data);
    }
#else
    const auto& t = SliceTables().v;
    for (; length >= 8U; data += 8, length -= 8U) {
        const std::uint32_t low = crc ^ (static_cast<std::uint32_t>(data[0]) | (static_cast<std::uint32_t>(data[1]) << 8U) |
                                         (static_cast<std::uint32_t>(data[2]) << 16U) |
                                         (static_cast<std::uint32_t>(data[3]) << 24U));
        crc = t[7][low & 0xFFU] ^ t[6][(low >> 8U) & 0xFFU] ^ t[5][(low >> 16U) & 0xFFU] ^ t[4][low >> 24U] ^
              t[3][data[4]] ^ t[2][data[5]] ^ t[1][data[6]] ^ t[0][data[7]];
    }
    for (; length > 0U; ++data, --length) {
        crc = (crc >> 8U) ^ t[0][(crc ^ *data) & 0xFFU];
    }
#endif
    return crc ^ 0xFFFFFFFFU;
}

std::uint32_t Crc32(const std::vector<std::uint8_t>& data) noexcept {
    return Crc32(data.data(), data.size());
}

}  // namespace chunkdb
