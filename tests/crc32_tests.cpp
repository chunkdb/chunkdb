#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "chunkdb/crc32.hpp"
#include "crc32_impl.hpp"

namespace {

using chunkdb::crc32_detail::Crc32ExtendWith;
using chunkdb::crc32_detail::Crc32Impl;
using chunkdb::crc32_detail::Crc32ImplAvailable;
using chunkdb::crc32_detail::Crc32ImplName;

constexpr std::size_t kMaxLength = 65536;
constexpr std::size_t kAlignments = 16;

std::vector<Crc32Impl> Available() {
    std::vector<Crc32Impl> impls;
    for (const Crc32Impl impl : chunkdb::crc32_detail::kAllCrc32Impls) {
        if (Crc32ImplAvailable(impl)) {
            impls.push_back(impl);
        }
    }
    return impls;
}

std::uint32_t Reference(const std::uint8_t* data, std::size_t length, std::uint32_t previous = 0) {
    return Crc32ExtendWith(Crc32Impl::kTable, previous, data, length);
}

void Expect(bool ok, Crc32Impl impl, const std::string& what) {
    if (!ok) {
        std::fprintf(stderr, "crc32 %s: %s\n", Crc32ImplName(impl), what.c_str());
        assert(false);
    }
}

void TestKnownVectors(Crc32Impl impl) {
    const std::string check = "123456789";
    const auto* bytes = reinterpret_cast<const std::uint8_t*>(check.data());
    Expect(Crc32ExtendWith(impl, 0, bytes, check.size()) == 0xCBF43926U, impl, "check value");
    Expect(Crc32ExtendWith(impl, 0, nullptr, 0) == 0U, impl, "empty");
    Expect(Crc32ExtendWith(impl, 0x12345678U, nullptr, 0) == 0x12345678U, impl, "empty extension");
    const std::string fox = "The quick brown fox jumps over the lazy dog";
    Expect(Crc32ExtendWith(impl, 0, reinterpret_cast<const std::uint8_t*>(fox.data()), fox.size()) == 0x414FA339U,
           impl, "quick brown fox");
}

// Every length from 0 to 1024 and random lengths up to 64 KiB, at every
// start alignment from 0 to 15.
void TestRandomBuffers(Crc32Impl impl, std::mt19937_64& random) {
    std::vector<std::uint8_t> buffer(kMaxLength + kAlignments);
    for (auto& byte : buffer) {
        byte = static_cast<std::uint8_t>(random());
    }
    std::uniform_int_distribution<std::size_t> any_length(0, kMaxLength);
    for (std::size_t alignment = 0; alignment < kAlignments; ++alignment) {
        const std::uint8_t* data = buffer.data() + alignment;
        for (std::size_t length = 0; length <= 1024; ++length) {
            Expect(Crc32ExtendWith(impl, 0, data, length) == Reference(data, length), impl,
                   "length " + std::to_string(length) + " at alignment " + std::to_string(alignment));
        }
        for (int round = 0; round < 64; ++round) {
            const std::size_t length = round == 0 ? kMaxLength : any_length(random);
            const auto previous = static_cast<std::uint32_t>(random());
            Expect(Crc32ExtendWith(impl, previous, data, length) == Reference(data, length, previous), impl,
                   "length " + std::to_string(length) + " at alignment " + std::to_string(alignment));
        }
    }
}

// A CRC extended piece by piece, split at random points, equals the CRC of
// the whole.
void TestIncremental(Crc32Impl impl, std::mt19937_64& random) {
    std::vector<std::uint8_t> buffer(kMaxLength);
    for (auto& byte : buffer) {
        byte = static_cast<std::uint8_t>(random());
    }
    for (int round = 0; round < 200; ++round) {
        const std::size_t length = std::uniform_int_distribution<std::size_t>(0, kMaxLength)(random);
        const std::size_t pieces = std::uniform_int_distribution<std::size_t>(1, 8)(random);
        std::vector<std::size_t> cuts{0, length};
        for (std::size_t i = 1; i < pieces; ++i) {
            cuts.push_back(std::uniform_int_distribution<std::size_t>(0, length)(random));
        }
        std::sort(cuts.begin(), cuts.end());
        std::uint32_t crc = 0;
        for (std::size_t i = 0; i + 1 < cuts.size(); ++i) {
            crc = Crc32ExtendWith(impl, crc, buffer.data() + cuts[i], cuts[i + 1] - cuts[i]);
        }
        Expect(crc == Reference(buffer.data(), length), impl, "incremental, length " + std::to_string(length));
    }
}

void TestUniformBuffers(Crc32Impl impl) {
    for (const std::uint8_t fill : {std::uint8_t{0x00}, std::uint8_t{0xFF}}) {
        const std::vector<std::uint8_t> buffer(kMaxLength + 1, fill);
        for (std::size_t length = 0; length <= kMaxLength; length = length < 300 ? length + 1 : length * 2 + 7) {
            Expect(Crc32ExtendWith(impl, 0, buffer.data(), length) == Reference(buffer.data(), length), impl,
                   "fill " + std::to_string(fill) + " length " + std::to_string(length));
        }
        Expect(Crc32ExtendWith(impl, 0, buffer.data(), kMaxLength) == Reference(buffer.data(), kMaxLength), impl,
               "fill " + std::to_string(fill) + " 64 KiB");
    }
    // Known values of 32 zero bytes and 32 0xFF bytes.
    const std::vector<std::uint8_t> zeros(32, 0x00);
    const std::vector<std::uint8_t> ones(32, 0xFF);
    Expect(Crc32ExtendWith(impl, 0, zeros.data(), zeros.size()) == 0x190A55ADU, impl, "32 zero bytes");
    Expect(Crc32ExtendWith(impl, 0, ones.data(), ones.size()) == 0xFF6CAB0BU, impl, "32 0xFF bytes");
}

void TestSelection() {
    using chunkdb::crc32_detail::SelectCrc32Impl;
    const Crc32Impl best = SelectCrc32Impl(nullptr);
    assert(Crc32ImplAvailable(best));
    assert(best != Crc32Impl::kTable);
    for (const Crc32Impl impl : chunkdb::crc32_detail::kAllCrc32Impls) {
        const Crc32Impl chosen = SelectCrc32Impl(Crc32ImplName(impl));
        assert(chosen == (Crc32ImplAvailable(impl) ? impl : best));
    }
    assert(SelectCrc32Impl("") == best);
    assert(SelectCrc32Impl("crc32c") == best);
    assert(Crc32ImplAvailable(chunkdb::crc32_detail::ActiveCrc32Impl()));

    // The public functions use the active implementation.
    std::vector<std::uint8_t> buffer(4099);
    for (std::size_t i = 0; i < buffer.size(); ++i) {
        buffer[i] = static_cast<std::uint8_t>(i * 131U + 7U);
    }
    assert(chunkdb::Crc32(buffer) == Reference(buffer.data(), buffer.size()));
    assert(chunkdb::Crc32(buffer.data() + 3, 1000) == Reference(buffer.data() + 3, 1000));
    assert(chunkdb::Crc32Extend(chunkdb::Crc32(buffer.data(), 77), buffer.data() + 77, 4000) ==
           Reference(buffer.data(), 4077));
}

}  // namespace

int main() {
    std::mt19937_64 random(20261012);
    std::string names;
    for (const Crc32Impl impl : Available()) {
        TestKnownVectors(impl);
        TestRandomBuffers(impl, random);
        TestIncremental(impl, random);
        TestUniformBuffers(impl);
        names += std::string(names.empty() ? "" : ", ") + Crc32ImplName(impl);
    }
    TestSelection();
    std::printf("crc32 tests passed: %s (active: %s)\n", names.c_str(),
                Crc32ImplName(chunkdb::crc32_detail::ActiveCrc32Impl()));
    return 0;
}
