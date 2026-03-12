#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace chunkdb::BitCodec {

[[nodiscard]] bool IsBitString(std::string_view bits) noexcept;
[[nodiscard]] std::string ExtractBits(
    const std::vector<std::uint8_t>& data,
    std::size_t bit_offset,
    std::size_t bit_count);
void WriteBits(std::vector<std::uint8_t>& data, std::size_t bit_offset, std::string_view bits);

}  // namespace chunkdb::BitCodec
