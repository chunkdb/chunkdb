// CRC32 by the ARMv8 CRC32 instructions, which compute this polynomial
// (the CRC32C ones are a different polynomial and are not used). The
// functions are compiled for the CRC extension through target attributes
// only, and called only once the CPU reports it, so the binary still runs
// on CPUs without it.

#include "crc32_impl.hpp"

#ifdef CHUNKDB_CRC32_ARMV8

#include <arm_acle.h>

#include <cstring>

#if defined(__APPLE__)
#include <sys/sysctl.h>
#elif defined(__linux__)
#include <sys/auxv.h>
#endif

#if defined(__clang__)
#define CHUNKDB_ARMV8_CRC_TARGET __attribute__((target("crc")))
#else
#define CHUNKDB_ARMV8_CRC_TARGET __attribute__((target("+crc")))
#endif

namespace chunkdb::crc32_detail {

bool Armv8Supported() noexcept {
    static const bool supported = [] {
#if defined(__ARM_FEATURE_CRC32)
        return true;
#elif defined(__APPLE__)
        int value = 0;
        std::size_t size = sizeof(value);
        return sysctlbyname("hw.optional.armv8_crc32", &value, &size, nullptr, 0) == 0 && value != 0;
#elif defined(__linux__)
        constexpr unsigned long kHwcapCrc32 = 1UL << 7U;  // HWCAP_CRC32
        return (getauxval(AT_HWCAP) & kHwcapCrc32) != 0U;
#else
        return false;
#endif
    }();
    return supported;
}

CHUNKDB_ARMV8_CRC_TARGET std::uint32_t Armv8Kernel(
    std::uint32_t state, const std::uint8_t* data, std::size_t length) noexcept {
    // Bytes up to an 8-byte boundary, then words, then the tail.
    while (length > 0U && (reinterpret_cast<std::uintptr_t>(data) & 7U) != 0U) {
        state = __crc32b(state, *data++);
        --length;
    }
    while (length >= 32U) {
        std::uint64_t words[4];
        std::memcpy(words, data, sizeof(words));
        state = __crc32d(state, words[0]);
        state = __crc32d(state, words[1]);
        state = __crc32d(state, words[2]);
        state = __crc32d(state, words[3]);
        data += 32;
        length -= 32U;
    }
    while (length >= 8U) {
        std::uint64_t word = 0;
        std::memcpy(&word, data, sizeof(word));
        state = __crc32d(state, word);
        data += 8;
        length -= 8U;
    }
    while (length > 0U) {
        state = __crc32b(state, *data++);
        --length;
    }
    return state;
}

}  // namespace chunkdb::crc32_detail

#endif  // CHUNKDB_CRC32_ARMV8
