#include "chunkdb/bit_codec.hpp"

#include <stdexcept>

namespace chunkdb::BitCodec {

bool IsBitString(std::string_view bits) noexcept {
    for (const char c : bits) {
        if (c != '0' && c != '1') {
            return false;
        }
    }
    return true;
}

std::string ExtractBits(
    const std::vector<std::uint8_t>& data,
    std::size_t bit_offset,
    std::size_t bit_count) {
    const std::size_t total_bits = data.size() * 8U;
    if (bit_offset + bit_count > total_bits) {
        throw std::out_of_range("bit range out of bounds");
    }

    std::string result;
    result.reserve(bit_count);
    for (std::size_t i = 0; i < bit_count; ++i) {
        const std::size_t absolute_bit = bit_offset + i;
        const std::size_t byte_index = absolute_bit / 8U;
        const std::size_t bit_index = absolute_bit % 8U;
        const std::uint8_t mask = static_cast<std::uint8_t>(1U << bit_index);
        result.push_back((data[byte_index] & mask) != 0U ? '1' : '0');
    }
    return result;
}

void WriteBits(std::vector<std::uint8_t>& data, std::size_t bit_offset, std::string_view bits) {
    if (!IsBitString(bits)) {
        throw std::invalid_argument("bits must contain only 0 or 1");
    }

    const std::size_t total_bits = data.size() * 8U;
    if (bit_offset + bits.size() > total_bits) {
        throw std::out_of_range("bit range out of bounds");
    }

    for (std::size_t i = 0; i < bits.size(); ++i) {
        const std::size_t absolute_bit = bit_offset + i;
        const std::size_t byte_index = absolute_bit / 8U;
        const std::size_t bit_index = absolute_bit % 8U;
        const std::uint8_t mask = static_cast<std::uint8_t>(1U << bit_index);

        if (bits[i] == '1') {
            data[byte_index] = static_cast<std::uint8_t>(data[byte_index] | mask);
        } else {
            data[byte_index] = static_cast<std::uint8_t>(data[byte_index] & ~mask);
        }
    }
}

}  // namespace chunkdb::BitCodec
