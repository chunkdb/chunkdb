#include "chunkdb/crc32.hpp"

#include <array>
#include <cstdlib>

#include "crc32_impl.hpp"

namespace chunkdb {

namespace crc32_detail {

namespace {

constexpr std::uint32_t kPolynomial = 0xEDB88320U;
// Bytes the slicing kernel consumes per step.
constexpr std::size_t kSlices = 16;

using Tables = std::array<std::array<std::uint32_t, 256>, kSlices>;

// tables[0] advances the register by one byte; tables[k] by one byte
// followed by k zero bytes.
constexpr Tables BuildTables() noexcept {
    Tables tables{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t value = i;
        for (int bit = 0; bit < 8; ++bit) {
            value = (value & 1U) != 0U ? (value >> 1U) ^ kPolynomial : value >> 1U;
        }
        tables[0][i] = value;
    }
    for (std::size_t k = 1; k < kSlices; ++k) {
        for (std::uint32_t i = 0; i < 256; ++i) {
            const std::uint32_t previous = tables[k - 1][i];
            tables[k][i] = (previous >> 8U) ^ tables[0][previous & 0xFFU];
        }
    }
    return tables;
}

constexpr Tables kTables = BuildTables();

inline std::uint32_t LoadLe32(const std::uint8_t* p) noexcept {
    return static_cast<std::uint32_t>(p[0]) | (static_cast<std::uint32_t>(p[1]) << 8U) |
           (static_cast<std::uint32_t>(p[2]) << 16U) | (static_cast<std::uint32_t>(p[3]) << 24U);
}

// The four bytes of `word`, little end first, each looked up in the table
// for its distance from the end of the step.
inline std::uint32_t Slice4(std::uint32_t word, std::size_t last) noexcept {
    return kTables[last][word & 0xFFU] ^ kTables[last - 1U][(word >> 8U) & 0xFFU] ^
           kTables[last - 2U][(word >> 16U) & 0xFFU] ^ kTables[last - 3U][word >> 24U];
}

Crc32Kernel KernelFor(Crc32Impl impl) noexcept {
    switch (impl) {
        case Crc32Impl::kTable:
            return TableKernel;
        case Crc32Impl::kSlicing:
            return SlicingKernel;
        case Crc32Impl::kClmul:
#ifdef CHUNKDB_CRC32_CLMUL
            return ClmulKernel;
#else
            break;
#endif
        case Crc32Impl::kArmv8:
#ifdef CHUNKDB_CRC32_ARMV8
            return Armv8Kernel;
#else
            break;
#endif
    }
    return SlicingKernel;
}

}  // namespace

std::uint32_t TableKernel(std::uint32_t state, const std::uint8_t* data, std::size_t length) noexcept {
    for (std::size_t i = 0; i < length; ++i) {
        state = (state >> 8U) ^ kTables[0][(state ^ data[i]) & 0xFFU];
    }
    return state;
}

std::uint32_t SlicingKernel(std::uint32_t state, const std::uint8_t* data, std::size_t length) noexcept {
    while (length >= kSlices) {
        state = Slice4(LoadLe32(data) ^ state, 15) ^ Slice4(LoadLe32(data + 4), 11) ^
                Slice4(LoadLe32(data + 8), 7) ^ Slice4(LoadLe32(data + 12), 3);
        data += kSlices;
        length -= kSlices;
    }
    return TableKernel(state, data, length);
}

const char* Crc32ImplName(Crc32Impl impl) noexcept {
    switch (impl) {
        case Crc32Impl::kTable:
            return "table";
        case Crc32Impl::kSlicing:
            return "slicing";
        case Crc32Impl::kClmul:
            return "clmul";
        case Crc32Impl::kArmv8:
            return "armv8";
    }
    return "unknown";
}

bool Crc32ImplAvailable(Crc32Impl impl) noexcept {
    switch (impl) {
        case Crc32Impl::kTable:
        case Crc32Impl::kSlicing:
            return true;
        case Crc32Impl::kClmul:
#ifdef CHUNKDB_CRC32_CLMUL
            return ClmulSupported();
#else
            return false;
#endif
        case Crc32Impl::kArmv8:
#ifdef CHUNKDB_CRC32_ARMV8
            return Armv8Supported();
#else
            return false;
#endif
    }
    return false;
}

Crc32Impl SelectCrc32Impl(const char* requested) noexcept {
    if (requested != nullptr) {
        for (const Crc32Impl impl : kAllCrc32Impls) {
            if (std::string_view(requested) == Crc32ImplName(impl) && Crc32ImplAvailable(impl)) {
                return impl;
            }
        }
    }
    for (const Crc32Impl impl : {Crc32Impl::kClmul, Crc32Impl::kArmv8}) {
        if (Crc32ImplAvailable(impl)) {
            return impl;
        }
    }
    return Crc32Impl::kSlicing;
}

Crc32Impl ActiveCrc32Impl() noexcept {
    static const Crc32Impl impl = SelectCrc32Impl(std::getenv("CHUNKDB_CRC32_IMPL"));
    return impl;
}

std::uint32_t Crc32ExtendWith(
    Crc32Impl impl, std::uint32_t previous, const std::uint8_t* data, std::size_t length) noexcept {
    return KernelFor(impl)(previous ^ 0xFFFFFFFFU, data, length) ^ 0xFFFFFFFFU;
}

}  // namespace crc32_detail

std::uint32_t Crc32Extend(std::uint32_t previous, const std::uint8_t* data, std::size_t length) noexcept {
    static const crc32_detail::Crc32Kernel kernel = crc32_detail::KernelFor(crc32_detail::ActiveCrc32Impl());
    return kernel(previous ^ 0xFFFFFFFFU, data, length) ^ 0xFFFFFFFFU;
}

std::uint32_t Crc32(const std::uint8_t* data, std::size_t length) noexcept {
    return Crc32Extend(0U, data, length);
}

std::uint32_t Crc32(const std::vector<std::uint8_t>& data) noexcept {
    return Crc32(data.data(), data.size());
}

}  // namespace chunkdb
