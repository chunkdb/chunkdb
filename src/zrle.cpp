#include "chunkdb/zrle.hpp"

#include <limits>
#include <stdexcept>

namespace chunkdb {

namespace {

void AppendUleb128(std::vector<std::uint8_t>* out, std::uint64_t value) {
    while (value >= 0x80U) {
        out->push_back(static_cast<std::uint8_t>((value & 0x7FU) | 0x80U));
        value >>= 7U;
    }
    out->push_back(static_cast<std::uint8_t>(value));
}

[[nodiscard]] std::uint64_t ReadUleb128(
    const std::uint8_t* data,
    std::size_t size,
    std::size_t* cursor) {
    std::uint64_t value = 0;
    unsigned shift = 0;
    while (true) {
        if (*cursor >= size) {
            throw std::runtime_error("zrle: truncated varint");
        }
        if (shift >= 63U) {
            throw std::runtime_error("zrle: varint too large");
        }
        const std::uint8_t byte = data[*cursor];
        ++*cursor;
        value |= static_cast<std::uint64_t>(byte & 0x7FU) << shift;
        if ((byte & 0x80U) == 0U) {
            return value;
        }
        shift += 7U;
    }
}

}  // namespace

std::vector<std::uint8_t> ZrleCompress(const std::vector<std::uint8_t>& input) {
    if (input.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("zrle: input too large");
    }

    std::vector<std::uint8_t> out;
    out.reserve(16 + input.size() / 8);
    out.push_back(kZrleCodecId);
    const auto size32 = static_cast<std::uint32_t>(input.size());
    out.push_back(static_cast<std::uint8_t>(size32 & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((size32 >> 8U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((size32 >> 16U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((size32 >> 24U) & 0xFFU));

    std::size_t i = 0;
    while (i < input.size()) {
        if (input[i] == 0U) {
            std::size_t run = 1;
            while (i + run < input.size() && input[i + run] == 0U) {
                ++run;
            }
            out.push_back(0x00U);
            AppendUleb128(&out, run);
            i += run;
        } else {
            std::size_t run = 1;
            // Treat short embedded zero gaps as literals so tiny gaps do not
            // fragment the literal stream.
            while (i + run < input.size()) {
                if (input[i + run] != 0U) {
                    ++run;
                    continue;
                }
                std::size_t zeros = 0;
                while (i + run + zeros < input.size() && input[i + run + zeros] == 0U) {
                    ++zeros;
                }
                if (zeros <= 2 && i + run + zeros < input.size()) {
                    run += zeros + 1;
                    continue;
                }
                break;
            }
            out.push_back(0x01U);
            AppendUleb128(&out, run);
            out.insert(out.end(), input.begin() + static_cast<std::ptrdiff_t>(i),
                       input.begin() + static_cast<std::ptrdiff_t>(i + run));
            i += run;
        }
    }
    return out;
}

std::vector<std::uint8_t> ZrleDecompress(
    const std::uint8_t* data,
    std::size_t size,
    std::size_t expected_output_size) {
    if (data == nullptr || size < 5) {
        throw std::runtime_error("zrle: input too small");
    }
    if (data[0] != kZrleCodecId) {
        throw std::runtime_error("zrle: unsupported codec id");
    }
    const std::uint32_t declared = static_cast<std::uint32_t>(data[1]) |
                                   (static_cast<std::uint32_t>(data[2]) << 8U) |
                                   (static_cast<std::uint32_t>(data[3]) << 16U) |
                                   (static_cast<std::uint32_t>(data[4]) << 24U);
    if (declared != expected_output_size) {
        throw std::runtime_error("zrle: declared size does not match expected size");
    }

    std::vector<std::uint8_t> out;
    out.reserve(expected_output_size);
    std::size_t cursor = 5;
    while (cursor < size) {
        const std::uint8_t token = data[cursor];
        ++cursor;
        const std::uint64_t run = ReadUleb128(data, size, &cursor);
        if (run == 0) {
            throw std::runtime_error("zrle: zero-length run");
        }
        if (run > expected_output_size - out.size()) {
            throw std::runtime_error("zrle: output overflows expected size");
        }
        if (token == 0x00U) {
            out.insert(out.end(), static_cast<std::size_t>(run), std::uint8_t{0});
        } else if (token == 0x01U) {
            if (run > size - cursor) {
                throw std::runtime_error("zrle: truncated literal run");
            }
            out.insert(out.end(), data + cursor, data + cursor + static_cast<std::size_t>(run));
            cursor += static_cast<std::size_t>(run);
        } else {
            throw std::runtime_error("zrle: unknown token");
        }
    }
    if (out.size() != expected_output_size) {
        throw std::runtime_error("zrle: output smaller than expected size");
    }
    return out;
}

std::vector<std::uint8_t> ZrleDecompress(
    const std::vector<std::uint8_t>& input,
    std::size_t expected_output_size) {
    return ZrleDecompress(input.data(), input.size(), expected_output_size);
}

}  // namespace chunkdb
