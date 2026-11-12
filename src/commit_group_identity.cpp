#include "commit_group_identity.hpp"

#include <algorithm>
#include <stdexcept>

namespace chunkdb {
namespace {

void RequireReference(const WalGroupRef& reference) {
    if (std::all_of(reference.id.begin(), reference.id.end(), [](auto byte) { return byte == 0U; }))
        throw std::invalid_argument("WAL group identity is zero");
    if (reference.member_count == 0U || reference.member_ordinal >= reference.member_count)
        throw std::invalid_argument("WAL group member ordinal is outside its count");
}

}  // namespace

std::array<std::uint8_t, kWalGroupRefBytes> SerializeWalGroupRef(const WalGroupRef& reference) {
    RequireReference(reference);
    std::array<std::uint8_t, kWalGroupRefBytes> bytes{};
    std::copy(reference.id.begin(), reference.id.end(), bytes.begin());
    for (unsigned i = 0; i < 8U; ++i) {
        bytes[16U + i] = static_cast<std::uint8_t>(reference.member_count >> (8U * i));
        bytes[24U + i] = static_cast<std::uint8_t>(reference.member_ordinal >> (8U * i));
    }
    return bytes;
}

WalGroupRef ParseWalGroupRef(std::span<const std::uint8_t> bytes) {
    if (bytes.size() != kWalGroupRefBytes)
        throw std::invalid_argument("WAL group reference has wrong length");
    WalGroupRef reference;
    std::copy_n(bytes.begin(), reference.id.size(), reference.id.begin());
    for (unsigned i = 0; i < 8U; ++i) {
        reference.member_count |= static_cast<std::uint64_t>(bytes[16U + i]) << (8U * i);
        reference.member_ordinal |= static_cast<std::uint64_t>(bytes[24U + i]) << (8U * i);
    }
    RequireReference(reference);
    return reference;
}

}  // namespace chunkdb
