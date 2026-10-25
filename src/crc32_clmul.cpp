// CRC32 by carry-less multiplication (PCLMULQDQ), folding 64 bytes per
// step, after "Fast CRC Computation for Generic Polynomials Using PCLMULQDQ
// Instruction" (Gopal et al., Intel, 2009). The SSE4.2 crc32 instruction
// computes CRC32C, another polynomial, and is not used.
//
// The functions are compiled for PCLMULQDQ and SSE4.1 through target
// attributes only, and called only once CPUID reports both, so the binary
// still runs on CPUs without them.

#include "crc32_impl.hpp"

#ifdef CHUNKDB_CRC32_CLMUL

#include <cpuid.h>
#include <immintrin.h>

namespace chunkdb::crc32_detail {

namespace {

// Folding constants for the reflected polynomial 0xEDB88320, as x^n mod P
// bit-reflected: k1/k2 fold 512 bits, k3/k4 128 bits, k5 64 bits; P and mu
// are the polynomial and its Barrett constant.
alignas(16) constexpr std::uint64_t kK1K2[2] = {0x0154442bd4ULL, 0x01c6e41596ULL};
alignas(16) constexpr std::uint64_t kK3K4[2] = {0x01751997d0ULL, 0x00ccaa009eULL};
alignas(16) constexpr std::uint64_t kK5K0[2] = {0x0163cd6124ULL, 0x0000000000ULL};
alignas(16) constexpr std::uint64_t kPolyMu[2] = {0x01db710641ULL, 0x01f7011641ULL};

// Below this many bytes folding does not pay for its setup and reduction.
constexpr std::size_t kMinFoldLength = 64;

#define CHUNKDB_CLMUL_TARGET __attribute__((target("pclmul,sse4.1")))

CHUNKDB_CLMUL_TARGET inline __m128i Fold(__m128i value, __m128i constants, __m128i next) noexcept {
    const __m128i low = _mm_clmulepi64_si128(value, constants, 0x00);
    const __m128i high = _mm_clmulepi64_si128(value, constants, 0x11);
    return _mm_xor_si128(_mm_xor_si128(low, high), next);
}

CHUNKDB_CLMUL_TARGET inline __m128i Load(const std::uint8_t* p) noexcept {
    return _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
}

// Advances `state` over `length` bytes, a multiple of 16 and at least 64.
CHUNKDB_CLMUL_TARGET std::uint32_t FoldBlocks(std::uint32_t state, const std::uint8_t* data, std::size_t length) noexcept {
    __m128i x1 = _mm_xor_si128(Load(data), _mm_cvtsi32_si128(static_cast<int>(state)));
    __m128i x2 = Load(data + 16);
    __m128i x3 = Load(data + 32);
    __m128i x4 = Load(data + 48);
    data += 64;
    length -= 64;

    __m128i k = _mm_load_si128(reinterpret_cast<const __m128i*>(kK1K2));
    while (length >= 64) {
        x1 = Fold(x1, k, Load(data));
        x2 = Fold(x2, k, Load(data + 16));
        x3 = Fold(x3, k, Load(data + 32));
        x4 = Fold(x4, k, Load(data + 48));
        data += 64;
        length -= 64;
    }

    // Four lanes into one.
    k = _mm_load_si128(reinterpret_cast<const __m128i*>(kK3K4));
    x1 = Fold(x1, k, x2);
    x1 = Fold(x1, k, x3);
    x1 = Fold(x1, k, x4);
    while (length >= 16) {
        x1 = Fold(x1, k, Load(data));
        data += 16;
        length -= 16;
    }

    // 128 bits to 64.
    const __m128i low32 = _mm_setr_epi32(~0, 0, ~0, 0);
    x2 = _mm_clmulepi64_si128(x1, k, 0x10);
    x1 = _mm_xor_si128(_mm_srli_si128(x1, 8), x2);
    k = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(kK5K0));
    x2 = _mm_srli_si128(x1, 4);
    x1 = _mm_xor_si128(_mm_clmulepi64_si128(_mm_and_si128(x1, low32), k, 0x00), x2);

    // Barrett reduction to 32 bits.
    k = _mm_load_si128(reinterpret_cast<const __m128i*>(kPolyMu));
    x2 = _mm_and_si128(_mm_clmulepi64_si128(_mm_and_si128(x1, low32), k, 0x10), low32);
    x2 = _mm_clmulepi64_si128(x2, k, 0x00);
    x1 = _mm_xor_si128(x1, x2);
    return static_cast<std::uint32_t>(_mm_extract_epi32(x1, 1));
}

}  // namespace

bool ClmulSupported() noexcept {
    static const bool supported = [] {
        unsigned int eax = 0;
        unsigned int ebx = 0;
        unsigned int ecx = 0;
        unsigned int edx = 0;
        if (__get_cpuid(1, &eax, &ebx, &ecx, &edx) == 0) {
            return false;
        }
        return (ecx & bit_PCLMUL) != 0U && (ecx & bit_SSE4_1) != 0U;
    }();
    return supported;
}

std::uint32_t ClmulKernel(std::uint32_t state, const std::uint8_t* data, std::size_t length) noexcept {
    if (length >= kMinFoldLength) {
        const std::size_t folded = length & ~static_cast<std::size_t>(15);
        state = FoldBlocks(state, data, folded);
        data += folded;
        length -= folded;
    }
    return SlicingKernel(state, data, length);
}

}  // namespace chunkdb::crc32_detail

#endif  // CHUNKDB_CRC32_CLMUL
