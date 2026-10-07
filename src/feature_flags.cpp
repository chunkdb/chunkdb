#include "feature_flags.hpp"

#include <cstdio>
#include <stdexcept>

namespace chunkdb {

namespace {

[[nodiscard]] std::string Hex(std::uint32_t value) {
    char text[16];
    std::snprintf(text, sizeof(text), "0x%x", static_cast<unsigned>(value));
    return text;
}

}  // namespace

std::string DescribeFeatures(const FeatureFlags& flags) {
    return "incompat=" + Hex(flags.incompat) + " ro_compat=" + Hex(flags.ro_compat) +
           " compat=" + Hex(flags.compat);
}

void RequireOpenableFeatures(const FeatureFlags& flags, AccessMode access_mode) {
    const auto unknown = UnknownFeatures(flags);
    if (unknown.incompat != 0U) {
        throw std::runtime_error(
            "store uses features this build does not support (unknown " +
            DescribeFeatures(unknown) + "); it cannot be opened");
    }
    if (unknown.ro_compat != 0U && access_mode != AccessMode::kReadOnly) {
        throw std::runtime_error(
            "store uses features this build can read but not write (unknown " +
            DescribeFeatures(unknown) + "); it can only be opened read-only");
    }
}

}  // namespace chunkdb
