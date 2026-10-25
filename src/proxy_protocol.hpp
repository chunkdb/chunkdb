#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

#include "server_socket.hpp"

// PROXY protocol version 2 (haproxy.org/download/2.9/doc/proxy-protocol.txt):
// the binary header a proxy sends before the client's bytes, carrying the
// client's address. Read only when --proxy-protocol v2 is set.
namespace chunkdb::proxy_protocol {

// The fixed part: signature, version and command, family, payload length.
inline constexpr std::size_t kPrefixBytes = 16;
// The payload holds the addresses and optional TLVs; longer ones are refused.
inline constexpr std::size_t kMaxPayloadBytes = 1024;

struct Source {
    // LOCAL (a proxy's own health check) or an unspecified family: the
    // connection's own peer stands.
    bool use_peer = false;
    // The client's address, as PeerAddressForSocket formats it, and its
    // "[address]:port" endpoint.
    std::string address;
    std::string endpoint;
};

// The payload length the prefix announces. Throws std::invalid_argument for a
// wrong signature, version, command or family, or an oversized length.
[[nodiscard]] std::size_t PayloadLength(std::span<const std::uint8_t> prefix);

// The source of a complete header (prefix and payload). Throws
// std::invalid_argument when it is malformed.
[[nodiscard]] Source Parse(std::span<const std::uint8_t> header);

// Reads one header from `socket` within `timeout_ms`, reading no byte past
// it. False, with `termination` describing why, when the header is absent,
// malformed, truncated or late.
[[nodiscard]] bool Read(
    server_detail::SocketHandle socket,
    std::size_t timeout_ms,
    Source* source,
    server_detail::ConnectionTermination* termination);

}  // namespace chunkdb::proxy_protocol
