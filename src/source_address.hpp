#pragma once

#include <string>

namespace chunkdb {

// The source a remote address is tracked under (failed authentication,
// connections before HELLO): an IPv4 address exactly (also when a dual-stack
// listener reports it v4-mapped), an IPv6 address by its /64 prefix, since
// one allocation controls all of its interface identifiers.
[[nodiscard]] std::string SourceAddressKey(const std::string& remote_address);

}  // namespace chunkdb
