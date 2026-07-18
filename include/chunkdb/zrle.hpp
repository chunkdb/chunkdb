#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chunkdb {

// Dependency-free byte codec used for optional chunk-state compression on
// disk and on the wire. Format "CZ1" (codec id 1):
//
//   [0x01][u32le uncompressed_size][token...]
//   token := 0x00 <uleb128 n>            n zero bytes
//          | 0x01 <uleb128 n> <n bytes>  n literal bytes
//
// Decompression is bounded: the caller supplies the exact expected output
// size, and any input that is truncated, malformed, oversized, or that
// declares or produces a different size fails with std::runtime_error.
inline constexpr std::uint8_t kZrleCodecId = 1;

[[nodiscard]] std::vector<std::uint8_t> ZrleCompress(const std::vector<std::uint8_t>& input);

[[nodiscard]] std::vector<std::uint8_t> ZrleDecompress(
    const std::uint8_t* data,
    std::size_t size,
    std::size_t expected_output_size);

[[nodiscard]] std::vector<std::uint8_t> ZrleDecompress(
    const std::vector<std::uint8_t>& input,
    std::size_t expected_output_size);

}  // namespace chunkdb
