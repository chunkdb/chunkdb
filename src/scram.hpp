#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "crypto.hpp"

// The server side of SCRAM-SHA-256 and SCRAM-SHA-256-PLUS (RFC 5802,
// RFC 7677) as HELLO uses it (docs/design/USERS_DESIGN.md): channel binding
// to the TLS connection, no extensions.
namespace chunkdb::scram {

inline constexpr std::uint32_t kMinIterations = 4096;
// A verifier's iteration count is sent to the client in the server-first
// message, and the client spends that many PBKDF2 rounds on every login.
// Bounded so a verifier cannot make logging in as that user arbitrarily
// expensive.
inline constexpr std::uint32_t kMaxIterations = 1'000'000;
// A verifier's salt length: at least 16 bytes, and bounded so it fits the
// users file and the server-first message stays small.
inline constexpr std::size_t kMinSaltBytes = 16;
inline constexpr std::size_t kMaxSaltBytes = 256;

// What the server stores for a user instead of the password.
struct Verifier {
    std::uint32_t iterations = 0;
    std::vector<std::uint8_t> salt;
    crypto::Sha256Digest stored_key{};
    crypto::Sha256Digest server_key{};

    friend bool operator==(const Verifier&, const Verifier&) = default;
};

// The verifier of `password`; clients compute it, the server only for the
// first administrator and the reset tool.
[[nodiscard]] Verifier MakeVerifier(
    std::string_view password,
    std::span<const std::uint8_t> salt,
    std::uint32_t iterations);
// The verifier a login of unknown user `name` runs against: a salt derived
// from `secret` and the name, so the same name always gets the same reply,
// and keys no proof matches. The exchange then fails like a wrong password.
[[nodiscard]] Verifier DecoyVerifier(std::span<const std::uint8_t> secret, std::string_view name);
// A fresh server nonce: printable, without commas.
[[nodiscard]] std::string NewNonce();

// `SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>`, base64
// parts: the form CREATE USER and ALTER USER take.
[[nodiscard]] std::string FormatVerifier(const Verifier& verifier);
// Throws std::invalid_argument naming what is wrong, also for an iteration
// count outside kMinIterations..kMaxIterations or a salt length outside
// kMinSaltBytes..kMaxSaltBytes.
[[nodiscard]] Verifier ParseVerifier(std::string_view text);

// The channel binding types a login can name (RFC 9266, RFC 5929).
inline constexpr std::string_view kTlsExporter = "tls-exporter";
inline constexpr std::string_view kTlsServerEndPoint = "tls-server-end-point";

// What the GS2 header of a client-first message says about channel binding
// (RFC 5802 section 7).
enum class BindingFlag : std::uint8_t {
    // `n`: the client does not bind the login.
    kNone,
    // `y`: the client could bind it but believes the server cannot.
    kUnused,
    // `p=<type>`: the login is bound to the connection.
    kBound,
};

// `<header>n=<user>,r=<nonce>`, the header being `n,,`, `y,,` or
// `p=<type>,,`.
struct ClientFirst {
    std::string user;
    std::string nonce;
    // Without the header: part of the signed message.
    std::string bare;
    // The header, which the client-final message repeats in `c=`.
    std::string gs2_header = "n,,";
    BindingFlag binding = BindingFlag::kNone;
    // The type `p=` names; empty otherwise.
    std::string binding_type;
};
// Throws std::invalid_argument for anything else, an authorization identity
// included.
[[nodiscard]] ClientFirst ParseClientFirst(std::string_view message);

// The `c=` attribute of a client-final message: `c=<base64 of the header
// and the channel binding data>`.
[[nodiscard]] std::string ChannelBindingAttribute(std::string_view gs2_header, std::span<const std::uint8_t> data);

// A client-final message of a bound login names other channel binding data
// than the server sees on its connection.
class ChannelMismatchError : public std::runtime_error {
  public:
    ChannelMismatchError() : std::runtime_error("the login is bound to another connection than the one the server sees") {}
};

// One login attempt after the client-first message.
class ServerExchange {
  public:
    // `binding_data` is what the connection gives for the type a bound
    // login names; empty for a login that is not bound.
    ServerExchange(
        ClientFirst first,
        Verifier verifier,
        std::string_view server_nonce,
        std::span<const std::uint8_t> binding_data = {});
    // `r=<client nonce + server nonce>,s=<salt>,i=<iterations>`.
    [[nodiscard]] const std::string& ServerFirst() const noexcept { return server_first_; }
    // The server-final message `v=<signature>` when the client's proof is
    // right, std::nullopt when it is wrong. Throws std::invalid_argument for
    // a message that is not a client-final message of this exchange, and
    // ChannelMismatchError when a bound login carries other binding data.
    [[nodiscard]] std::optional<std::string> Finish(std::string_view client_final) const;

  private:
    ClientFirst first_;
    Verifier verifier_;
    std::string nonce_;
    std::string server_first_;
    // `c=<base64 of the header and the binding data>`.
    std::string channel_binding_;
};

// The client side, for tests, the bench and tools: the messages a client
// sends and the server signature it must then receive.
struct ClientLogin {
    std::string first;       // <header>n=<user>,r=<nonce>
    std::string first_bare;  // n=<user>,r=<nonce>
    std::string nonce;
    std::string gs2_header = "n,,";  // or p=<type>,,
};
// `binding_type` empty: a login that is not bound.
[[nodiscard]] ClientLogin StartClientLogin(
    std::string_view user,
    std::string_view nonce,
    std::string_view binding_type = {});
struct ClientFinal {
    std::string message;           // c=<binding>,r=<nonce>,p=<proof>
    std::string server_signature;  // v=<signature> the server must answer
};
// Throws std::invalid_argument when `server_first` is not a server-first
// message continuing `login`.
[[nodiscard]] ClientFinal FinishClientLogin(
    const ClientLogin& login,
    std::string_view password,
    std::string_view server_first,
    std::span<const std::uint8_t> binding_data = {});

}  // namespace chunkdb::scram
