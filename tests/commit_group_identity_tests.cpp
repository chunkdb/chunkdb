#include <array>
#include <cassert>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

#include "commit_group_identity.hpp"

namespace {
using namespace chunkdb;

template <typename Action>
void Reject(Action action) {
    bool rejected = false;
    try { action(); } catch (const std::invalid_argument&) { rejected = true; }
    assert(rejected);
}

void RoundTrip() {
    CommitGroupId id{};
    for (std::size_t i = 0; i < id.size(); ++i) id[i] = static_cast<std::uint8_t>(i + 1U);
    const WalGroupRef reference{id, UINT64_C(0x8877665544332211), UINT64_C(0x0807060504030201)};
    const auto encoded = SerializeWalGroupRef(reference);
    assert(encoded.size() == 32U && encoded[0] == 1U && encoded[15] == 16U);
    const std::array<std::uint8_t, 8> count{0x11U, 0x22U, 0x33U, 0x44U, 0x55U, 0x66U, 0x77U, 0x88U};
    const std::array<std::uint8_t, 8> ordinal{1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U};
    for (std::size_t i = 0; i < count.size(); ++i) {
        assert(encoded[16U + i] == count[i] && encoded[24U + i] == ordinal[i]);
    }
    assert(ParseWalGroupRef(encoded) == reference);
    const WalGroupRef maximum{id, std::numeric_limits<std::uint64_t>::max(),
        std::numeric_limits<std::uint64_t>::max() - 1U};
    assert(ParseWalGroupRef(SerializeWalGroupRef(maximum)) == maximum);
    assert((ParseWalGroupRef(SerializeWalGroupRef({id, 1U, 0U})) == WalGroupRef{id, 1U, 0U}));
}

void InvalidReferences() {
    CommitGroupId id{}; id.back() = 1U;
    for (const auto& reference : {WalGroupRef{{}, 1U, 0U}, WalGroupRef{id, 0U, 0U},
            WalGroupRef{id, 1U, 1U}, WalGroupRef{id, 1U, UINT64_MAX}}) {
        Reject([&] { (void)SerializeWalGroupRef(reference); });
    }
    const auto valid = SerializeWalGroupRef({id, 1U, 0U});
    auto oversized = std::vector<std::uint8_t>(valid.begin(), valid.end());
    oversized.push_back(0U);
    Reject([&] { (void)ParseWalGroupRef(oversized); });
    for (std::size_t size = 0; size < valid.size(); ++size)
        Reject([&] { (void)ParseWalGroupRef(std::span(valid).first(size)); });
    auto invalid = valid;
    invalid[15U] = 0U;
    Reject([&] { (void)ParseWalGroupRef(invalid); });
    invalid = valid; invalid[16U] = 0U;
    Reject([&] { (void)ParseWalGroupRef(invalid); });
    invalid = valid; invalid[24U] = 1U;
    Reject([&] { (void)ParseWalGroupRef(invalid); });
}
}

int main() {
    RoundTrip();
    InvalidReferences();
}
