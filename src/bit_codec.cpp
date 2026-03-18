#include "chunkdb/bit_codec.hpp"

#include <array>
#include <cstring>
#include <stdexcept>

namespace chunkdb::BitCodec {

namespace {

const std::array<std::array<char, 8>, 256>& ByteToBitCharsLsb() {
    static const std::array<std::array<char, 8>, 256> table = [] {
        std::array<std::array<char, 8>, 256> built{};
        for (std::size_t value = 0; value < built.size(); ++value) {
            for (std::size_t bit = 0; bit < 8; ++bit) {
                built[value][bit] = ((value >> bit) & 1U) != 0U ? '1' : '0';
            }
        }
        return built;
    }();
    return table;
}

std::uint8_t ParseBitByteLsb(const char* bits) noexcept {
    std::uint8_t value = 0U;
    for (std::size_t bit = 0; bit < 8; ++bit) {
        if (bits[bit] == '1') {
            value = static_cast<std::uint8_t>(value | (1U << bit));
        }
    }
    return value;
}

void WriteSingleBit(std::vector<std::uint8_t>& data, std::size_t bit_index, bool one) noexcept {
    const std::size_t byte_index = bit_index / 8U;
    const std::size_t offset = bit_index % 8U;
    const std::uint8_t mask = static_cast<std::uint8_t>(1U << offset);
    if (one) {
        data[byte_index] = static_cast<std::uint8_t>(data[byte_index] | mask);
    } else {
        data[byte_index] = static_cast<std::uint8_t>(data[byte_index] & ~mask);
    }
}

}  // namespace

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
    if (bit_count == 0) {
        return {};
    }
    const std::size_t total_bits = data.size() * 8U;
    if (bit_offset + bit_count > total_bits) {
        throw std::out_of_range("bit range out of bounds");
    }

    std::string result(bit_count, '0');
    const auto& lut = ByteToBitCharsLsb();

    std::size_t absolute_bit = bit_offset;
    std::size_t out_index = 0;
    std::size_t remaining = bit_count;

    // Align to byte boundary first.
    while (remaining > 0 && (absolute_bit % 8U) != 0U) {
        const std::size_t byte_index = absolute_bit / 8U;
        const std::size_t bit_index = absolute_bit % 8U;
        const std::uint8_t mask = static_cast<std::uint8_t>(1U << bit_index);
        result[out_index++] = (data[byte_index] & mask) != 0U ? '1' : '0';
        ++absolute_bit;
        --remaining;
    }

    while (remaining >= 8U) {
        const std::size_t byte_index = absolute_bit / 8U;
        std::uint8_t merged = 0U;
        const std::size_t bit_index = absolute_bit % 8U;
        if (bit_index == 0U) {
            merged = data[byte_index];
        } else {
            const std::uint8_t lhs = static_cast<std::uint8_t>(data[byte_index] >> bit_index);
            const std::uint8_t rhs = static_cast<std::uint8_t>(data[byte_index + 1U] << (8U - bit_index));
            merged = static_cast<std::uint8_t>(lhs | rhs);
        }
        std::memcpy(result.data() + out_index, lut[merged].data(), 8U);
        absolute_bit += 8U;
        out_index += 8U;
        remaining -= 8U;
    }

    while (remaining > 0) {
        const std::size_t byte_index = absolute_bit / 8U;
        const std::size_t bit_index = absolute_bit % 8U;
        const std::uint8_t mask = static_cast<std::uint8_t>(1U << bit_index);
        result[out_index++] = (data[byte_index] & mask) != 0U ? '1' : '0';
        ++absolute_bit;
        --remaining;
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

    if (bits.empty()) {
        return;
    }

    std::size_t absolute_bit = bit_offset;
    std::size_t input_index = 0;
    const char* raw_bits = bits.data();

    while (input_index < bits.size() && (absolute_bit % 8U) != 0U) {
        WriteSingleBit(data, absolute_bit, raw_bits[input_index] == '1');
        ++absolute_bit;
        ++input_index;
    }

    while ((input_index + 8U) <= bits.size()) {
        const std::size_t byte_index = absolute_bit / 8U;
        data[byte_index] = ParseBitByteLsb(raw_bits + input_index);
        absolute_bit += 8U;
        input_index += 8U;
    }

    while (input_index < bits.size()) {
        WriteSingleBit(data, absolute_bit, raw_bits[input_index] == '1');
        ++absolute_bit;
        ++input_index;
    }
}

}  // namespace chunkdb::BitCodec
