#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

// The implementations behind Crc32 (crc32.hpp). All compute the same CRC:
// the reflected polynomial 0xEDB88320 with pre- and post-inversion.
namespace chunkdb::crc32_detail {

#if defined(__GNUC__) && (defined(__x86_64__) || defined(_M_X64))
#define CHUNKDB_CRC32_CLMUL 1
#endif
#if defined(__GNUC__) && defined(__aarch64__) && !defined(__ARM_BIG_ENDIAN)
#define CHUNKDB_CRC32_ARMV8 1
#endif

enum class Crc32Impl {
    kTable,    // one table lookup per byte
    kSlicing,  // slicing-by-N tables, portable
    kClmul,    // x86-64 PCLMULQDQ folding with SSE4.1
    kArmv8,    // ARMv8 CRC32 instructions
};

inline constexpr Crc32Impl kAllCrc32Impls[] = {
    Crc32Impl::kTable, Crc32Impl::kSlicing, Crc32Impl::kClmul, Crc32Impl::kArmv8};

// A kernel advances the CRC register: the CRC without its pre- and
// post-inversion.
using Crc32Kernel = std::uint32_t (*)(std::uint32_t state, const std::uint8_t* data, std::size_t length) noexcept;

[[nodiscard]] const char* Crc32ImplName(Crc32Impl impl) noexcept;
// Whether this build and this CPU can run `impl`.
[[nodiscard]] bool Crc32ImplAvailable(Crc32Impl impl) noexcept;
// The implementation used for a CHUNKDB_CRC32_IMPL value (null when unset):
// the one named if it is available, otherwise the fastest available.
[[nodiscard]] Crc32Impl SelectCrc32Impl(const char* requested) noexcept;
// The implementation Crc32 uses, chosen once per process.
[[nodiscard]] Crc32Impl ActiveCrc32Impl() noexcept;
// Crc32Extend with `impl`, which must be available.
[[nodiscard]] std::uint32_t Crc32ExtendWith(
    Crc32Impl impl, std::uint32_t previous, const std::uint8_t* data, std::size_t length) noexcept;

[[nodiscard]] std::uint32_t TableKernel(std::uint32_t state, const std::uint8_t* data, std::size_t length) noexcept;
[[nodiscard]] std::uint32_t SlicingKernel(std::uint32_t state, const std::uint8_t* data, std::size_t length) noexcept;
#ifdef CHUNKDB_CRC32_CLMUL
[[nodiscard]] bool ClmulSupported() noexcept;
[[nodiscard]] std::uint32_t ClmulKernel(std::uint32_t state, const std::uint8_t* data, std::size_t length) noexcept;
#endif
#ifdef CHUNKDB_CRC32_ARMV8
[[nodiscard]] bool Armv8Supported() noexcept;
[[nodiscard]] std::uint32_t Armv8Kernel(std::uint32_t state, const std::uint8_t* data, std::size_t length) noexcept;
#endif

}  // namespace chunkdb::crc32_detail
