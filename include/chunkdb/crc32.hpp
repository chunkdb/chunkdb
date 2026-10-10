#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chunkdb {

// Extend a completed CRC with another block; zero is the CRC of no bytes.
[[nodiscard]] std::uint32_t Crc32Extend(std::uint32_t previous, const std::uint8_t* data, std::size_t length) noexcept;
[[nodiscard]] std::uint32_t Crc32(const std::uint8_t* data, std::size_t length) noexcept;
[[nodiscard]] std::uint32_t Crc32(const std::vector<std::uint8_t>& data) noexcept;

}  // namespace chunkdb
