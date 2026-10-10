#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "chunkdb/engine.hpp"
// Not in the include path of every test target; relative to this file.
#include "../src/scram.hpp"
#include "../src/user_registry.hpp"

// Logins with users (SCRAM-SHA-256, docs/design/USERS_DESIGN.md) for tests: a
// registry with a first administrator, a login through CommandEngine, and
// the bytes a socket client sends.
namespace chunkdb::test {

// The users of `data_dir` (created when missing) with `admin` as the first
// administrator: manages users, ADMIN on `*`.
inline std::shared_ptr<UserRegistry> MakeUsers(
    const std::filesystem::path& data_dir,
    const std::string& admin,
    const std::string& password) {
    std::filesystem::create_directories(data_dir);
    const auto salt = crypto::RandomBytes(16);
    const auto random = crypto::RandomBytes(32);
    std::array<std::uint8_t, 32> secret{};
    std::copy(random.begin(), random.end(), secret.begin());
    return std::make_shared<UserRegistry>(
        data_dir,
        std::make_pair(admin, scram::MakeVerifier(password, salt, scram::kMinIterations)),
        secret);
}

// A line followed by one parameter frame, as a socket client sends it.
inline std::string LineWithFrame(std::string_view line, std::string_view frame) {
    std::string bytes(line);
    bytes += "\r\n$" + std::to_string(frame.size()) + "\r\n";
    bytes += frame;
    bytes += "\r\n";
    return bytes;
}

// The server-first message of a `+SCRAM <server-first>` reply; the reply
// itself when it is not one.
inline std::string ServerFirstOf(std::string_view reply) {
    while (!reply.empty() && (reply.back() == '\r' || reply.back() == '\n')) {
        reply.remove_suffix(1);
    }
    constexpr std::string_view kPrefix = "+SCRAM ";
    if (reply.substr(0, kPrefix.size()) == kPrefix) {
        reply.remove_prefix(kPrefix.size());
    }
    return std::string(reply);
}

// A client-final message continuing `server_first` with a proof no password
// gives, without the PBKDF2 a real one costs: a failed login.
inline std::string WrongProofFinal(std::string_view server_first) {
    const auto salt_at = server_first.find(",s=");
    if (server_first.substr(0, 2) != "r=" || salt_at == std::string_view::npos) {
        throw std::runtime_error("not a SCRAM server-first message: " + std::string(server_first));
    }
    const std::vector<std::uint8_t> proof(32, 0);
    return "c=biws,r=" + std::string(server_first.substr(2, salt_at - 2)) + ",p=" + crypto::Base64Encode(proof);
}

// HELLO 3 USER <user> $1: the line, and the line with its frame.
inline std::string HelloUserLine(std::string_view user) {
    return "HELLO 3 USER " + std::string(user) + " $1";
}
inline std::string HelloUserBytes(const scram::ClientLogin& login, std::string_view user) {
    return LineWithFrame(HelloUserLine(user), login.first);
}

// AUTH $1, line and frame, answering `server_reply` (`+SCRAM <server-first>`
// or the server-first message), and the server signature the HELLO map must
// then carry.
struct AuthStep {
    std::string bytes;
    std::string server_signature;
};
inline AuthStep AuthBytes(const scram::ClientLogin& login, std::string_view password, std::string_view server_reply) {
    const auto client_final = scram::FinishClientLogin(login, password, ServerFirstOf(server_reply));
    return AuthStep{
        .bytes = LineWithFrame("AUTH $1", client_final.message),
        .server_signature = client_final.server_signature,
    };
}
// AUTH $1 with a wrong proof: a failed login.
inline std::string WrongAuthBytes(std::string_view server_reply) {
    return LineWithFrame("AUTH $1", WrongProofFinal(ServerFirstOf(server_reply)));
}

// HELLO 3 USER and AUTH through CommandEngine::Execute. Returns the first
// reply that is not +SCRAM: the HELLO map, whose server_signature is checked,
// or the error. A null `password` sends a wrong proof without PBKDF2.
inline std::string LoginOnEngine(
    CommandEngine& engine,
    SessionState& session,
    const std::string& user,
    const std::optional<std::string>& password) {
    using Parameters = std::vector<std::optional<std::string>>;
    const auto login = scram::StartClientLogin(user, scram::NewNonce());
    const std::string first_reply = engine.Execute(session, HelloUserLine(user) + "\r\n", Parameters{login.first});
    if (first_reply.rfind("+SCRAM ", 0) != 0) {
        return first_reply;
    }
    const std::string server_first = ServerFirstOf(first_reply);
    if (!password.has_value()) {
        return engine.Execute(session, "AUTH $1\r\n", Parameters{WrongProofFinal(server_first)});
    }
    const auto client_final = scram::FinishClientLogin(login, *password, server_first);
    std::string reply = engine.Execute(session, "AUTH $1\r\n", Parameters{client_final.message});
    if (reply.rfind("%8\r\n", 0) == 0) {
        const std::string signature_entry = "$16\r\nserver_signature\r\n$" +
                                            std::to_string(client_final.server_signature.size()) + "\r\n" +
                                            client_final.server_signature + "\r\n";
        if (reply.find(signature_entry) == std::string::npos) {
            throw std::runtime_error("the HELLO map does not carry the expected server signature");
        }
    }
    return reply;
}
// A login with a wrong proof.
inline std::string FailedLoginOnEngine(CommandEngine& engine, SessionState& session, const std::string& user) {
    return LoginOnEngine(engine, session, user, std::nullopt);
}

}  // namespace chunkdb::test
