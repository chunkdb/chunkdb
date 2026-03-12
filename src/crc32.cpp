#include "chunkdb/crc32.hpp"

namespace chunkdb {

namespace {

constexpr std::uint32_t kPolynomial = 0xEDB88320U;

std::uint32_t ComputeTableEntry(std::uint32_t index) noexcept {
    std::uint32_t value = index;
    for (int i = 0; i < 8; ++i) {
        if ((value & 1U) != 0U) {
            value = (value >> 1U) ^ kPolynomial;
        } else {
            value >>= 1U;
        }
    }
    return value;
}

const std::uint32_t* Table() noexcept {
    static std::uint32_t table[256] = {};
    static bool initialized = false;
    if (!initialized) {
        for (std::uint32_t i = 0; i < 256; ++i) {
            table[i] = ComputeTableEntry(i);
        }
        initialized = true;
    }
    return table;
}

}  // namespace

std::uint32_t Crc32(const std::uint8_t* data, std::size_t length) noexcept {
    const std::uint32_t* table = Table();
    std::uint32_t crc = 0xFFFFFFFFU;
    for (std::size_t i = 0; i < length; ++i) {
        const std::uint8_t index = static_cast<std::uint8_t>((crc ^ data[i]) & 0xFFU);
        crc = (crc >> 8U) ^ table[index];
    }
    return crc ^ 0xFFFFFFFFU;
}

std::uint32_t Crc32(const std::vector<std::uint8_t>& data) noexcept {
    return Crc32(data.data(), data.size());
}

}  // namespace chunkdb
