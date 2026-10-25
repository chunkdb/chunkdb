#include "proxy_protocol.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <vector>

namespace chunkdb::proxy_protocol {

namespace {

constexpr std::array<std::uint8_t, 12> kSignature = {
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};

constexpr std::uint8_t kCommandLocal = 0x0;
constexpr std::uint8_t kCommandProxy = 0x1;
constexpr std::uint8_t kFamilyUnspecified = 0x00;
constexpr std::uint8_t kFamilyTcp4 = 0x11;
constexpr std::uint8_t kFamilyTcp6 = 0x21;
// Source and destination addresses, then source and destination ports.
constexpr std::size_t kTcp4Bytes = 12;
constexpr std::size_t kTcp6Bytes = 36;

// The address and endpoint of `address`, as the socket helpers format them.
void Describe(const sockaddr* address, std::size_t size, Source* source) {
    std::array<char, NI_MAXHOST> host{};
    std::array<char, NI_MAXSERV> port{};
    if (getnameinfo(address, static_cast<socklen_t>(size), host.data(), static_cast<socklen_t>(host.size()),
                    port.data(), static_cast<socklen_t>(port.size()), NI_NUMERICHOST | NI_NUMERICSERV) != 0) {
        throw std::invalid_argument("the PROXY header's source address cannot be formatted");
    }
    source->address = host.data();
    source->endpoint = "[" + source->address + "]:" + port.data();
}

}  // namespace

std::size_t PayloadLength(std::span<const std::uint8_t> prefix) {
    if (prefix.size() < kPrefixBytes || !std::equal(kSignature.begin(), kSignature.end(), prefix.begin())) {
        throw std::invalid_argument("expected a PROXY protocol v2 header");
    }
    const std::uint8_t version = prefix[12] >> 4U;
    const std::uint8_t command = prefix[12] & 0x0FU;
    if (version != 2U) {
        throw std::invalid_argument("PROXY protocol version " + std::to_string(version) + " is not v2");
    }
    if (command != kCommandLocal && command != kCommandProxy) {
        throw std::invalid_argument("unknown PROXY protocol command " + std::to_string(command));
    }
    const std::size_t length = (static_cast<std::size_t>(prefix[14]) << 8U) | prefix[15];
    if (length > kMaxPayloadBytes) {
        throw std::invalid_argument(
            "the PROXY header announces " + std::to_string(length) + " bytes; at most " +
            std::to_string(kMaxPayloadBytes) + " are accepted");
    }
    return length;
}

Source Parse(std::span<const std::uint8_t> header) {
    const std::size_t length = PayloadLength(header);
    if (header.size() != kPrefixBytes + length) {
        throw std::invalid_argument("the PROXY header is truncated");
    }
    const auto payload = header.subspan(kPrefixBytes);
    const std::uint8_t command = header[12] & 0x0FU;
    const std::uint8_t family = header[13];
    Source source;
    if (command == kCommandLocal) {
        source.use_peer = true;
        return source;
    }
    switch (family) {
        case kFamilyUnspecified:
            source.use_peer = true;
            return source;
        case kFamilyTcp4: {
            if (payload.size() < kTcp4Bytes) throw std::invalid_argument("the PROXY header's IPv4 addresses are truncated");
            sockaddr_in address{};
            address.sin_family = AF_INET;
            std::memcpy(&address.sin_addr, payload.data(), 4);
            std::memcpy(&address.sin_port, payload.data() + 8, 2);
            Describe(reinterpret_cast<const sockaddr*>(&address), sizeof(address), &source);
            return source;
        }
        case kFamilyTcp6: {
            if (payload.size() < kTcp6Bytes) throw std::invalid_argument("the PROXY header's IPv6 addresses are truncated");
            sockaddr_in6 address{};
            address.sin6_family = AF_INET6;
            std::memcpy(&address.sin6_addr, payload.data(), 16);
            std::memcpy(&address.sin6_port, payload.data() + 32, 2);
            Describe(reinterpret_cast<const sockaddr*>(&address), sizeof(address), &source);
            return source;
        }
        default:
            throw std::invalid_argument("the PROXY header carries no TCP over IPv4 or IPv6 connection");
    }
}

bool Read(
    server_detail::SocketHandle socket,
    std::size_t timeout_ms,
    Source* source,
    server_detail::ConnectionTermination* termination) {
    using namespace server_detail;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    const auto fail = [&](std::string reason, std::string error) {
        termination->should_log = true;
        termination->phase = "proxy_header";
        termination->reason = std::move(reason);
        termination->error = std::move(error);
        return false;
    };
    std::vector<std::uint8_t> header(kPrefixBytes);
    // Exactly `size` bytes into header[at...], waiting for each until the
    // deadline, so nothing after the header is consumed.
    const auto read_exact = [&](std::size_t at, std::size_t size) -> bool {
        while (size > 0U) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) return fail("timeout", "the PROXY header did not arrive in time");
            int wait_error = 0;
            const auto ready = WaitForSocketReady(
                socket, true, false, std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now) +
                    std::chrono::milliseconds(1), &wait_error);
            if (ready == SocketWaitResult::kTimeout) return fail("timeout", "the PROXY header did not arrive in time");
            if (ready == SocketWaitResult::kError) return fail("socket_error", FormatSocketError(wait_error));
#ifdef _WIN32
            const int received = recv(socket, reinterpret_cast<char*>(header.data() + at), static_cast<int>(size), 0);
#else
            const auto received = recv(socket, header.data() + at, size, 0);
#endif
            if (received == 0) return fail("peer_closed", "the connection closed inside the PROXY header");
            if (received < 0) {
                const int code = CurrentSocketErrorCode();
                if (IsSocketInterruptedError(code) || IsSocketTimeoutError(code)) continue;
                return fail("socket_error", FormatSocketError(code));
            }
            at += static_cast<std::size_t>(received);
            size -= static_cast<std::size_t>(received);
        }
        return true;
    };
    if (!read_exact(0U, kPrefixBytes)) return false;
    try {
        const std::size_t length = PayloadLength(header);
        header.resize(kPrefixBytes + length);
        if (!read_exact(kPrefixBytes, length)) return false;
        *source = Parse(header);
    } catch (const std::invalid_argument& error) {
        return fail("invalid_proxy_header", error.what());
    }
    return true;
}

}  // namespace chunkdb::proxy_protocol
