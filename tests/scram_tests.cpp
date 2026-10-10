// The server side of SCRAM-SHA-256 login (docs/design/USERS_DESIGN.md): the
// exchange of RFC 7677, wrong passwords, malformed messages and verifiers.

#include <cassert>
#include <cstdio>
#include <functional>
#include <stdexcept>
#include <string>

#include "scram.hpp"

namespace {

namespace crypto = chunkdb::crypto;
namespace scram = chunkdb::scram;

void ExpectInvalid(const std::function<void()>& action, const std::string& part) {
    try {
        action();
    } catch (const std::invalid_argument& e) {
        if (std::string(e.what()).find(part) == std::string::npos) {
            std::fprintf(stderr, "expected '%s', got '%s'\n", part.c_str(), e.what());
            assert(false);
        }
        return;
    }
    std::fprintf(stderr, "expected an error with '%s'\n", part.c_str());
    assert(false);
}

constexpr const char* kClientNonce = "rOprNGfwEbeRWgbNEkqO";
constexpr const char* kServerNonce = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
constexpr const char* kClientFinal =
    "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";

scram::Verifier PencilVerifier() {
    return scram::MakeVerifier("pencil", *crypto::Base64Decode("W22ZaJ0SNY7soEsUEjb6gQ=="), 4096);
}

// RFC 7677 section 3.
void TestRfcExchange() {
    const auto first = scram::ParseClientFirst(std::string("n,,n=user,r=") + kClientNonce);
    assert(first.user == "user" && first.nonce == kClientNonce && first.bare == std::string("n=user,r=") + kClientNonce);
    const scram::ServerExchange exchange(first, PencilVerifier(), kServerNonce);
    assert(exchange.ServerFirst() == "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096");
    const auto final_message = exchange.Finish(kClientFinal);
    assert(final_message == std::string("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="));
}

// The client helpers produce exactly the RFC's messages.
void TestClientSide() {
    const auto login = scram::StartClientLogin("user", kClientNonce);
    assert(login.first == std::string("n,,n=user,r=") + kClientNonce);
    const auto final_message = scram::FinishClientLogin(
        login, "pencil", "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096");
    assert(final_message.message == kClientFinal);
    assert(final_message.server_signature == "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
    ExpectInvalid([&] { (void)scram::FinishClientLogin(login, "pencil", "r=other,s=AAAA,i=4096"); }, "does not continue");
    ExpectInvalid([&] { (void)scram::FinishClientLogin(login, "pencil", "nonsense"); }, "not a SCRAM server-first");
}

void TestWrongPassword() {
    const auto first = scram::ParseClientFirst(std::string("n,,n=user,r=") + kClientNonce);
    const auto other = scram::MakeVerifier("pencils", *crypto::Base64Decode("W22ZaJ0SNY7soEsUEjb6gQ=="), 4096);
    const scram::ServerExchange exchange(first, other, kServerNonce);
    assert(!exchange.Finish(kClientFinal).has_value());
}

void TestMalformedMessages() {
    ExpectInvalid([] { (void)scram::ParseClientFirst("y,,n=user,r=abc"); }, "starts with n,,");
    ExpectInvalid([] { (void)scram::ParseClientFirst("p=tls-unique,,n=user,r=abc"); }, "starts with n,,");
    ExpectInvalid([] { (void)scram::ParseClientFirst("n,,r=abc"); }, "names the user");
    ExpectInvalid([] { (void)scram::ParseClientFirst("n,,n=user"); }, "carries a nonce");
    ExpectInvalid([] { (void)scram::ParseClientFirst("n,,n=user,r="); }, "printable ASCII");
    ExpectInvalid([] { (void)scram::ParseClientFirst("n,,n=user,r=a b"); }, "printable ASCII");

    const auto first = scram::ParseClientFirst(std::string("n,,n=user,r=") + kClientNonce);
    const scram::ServerExchange exchange(first, PencilVerifier(), kServerNonce);
    // Another nonce, channel binding, or a proof of the wrong size.
    ExpectInvalid([&] { (void)exchange.Finish("c=biws,r=rOprNGfwEbeRWgbNEkqOxx,p=AAAA"); }, "does not continue");
    ExpectInvalid(
        [&] { (void)exchange.Finish("c=eSws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=AAAA"); },
        "does not continue");
    ExpectInvalid(
        [&] { (void)exchange.Finish("c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=AAAA"); },
        "32 bytes");
}

void TestDecoyAndNonce() {
    const std::vector<std::uint8_t> secret(32, 0x42);
    const auto ghost = scram::DecoyVerifier(secret, "ghost");
    assert(ghost == scram::DecoyVerifier(secret, "ghost"));
    assert(!(ghost == scram::DecoyVerifier(secret, "phantom")));
    assert(!(ghost == scram::DecoyVerifier(std::vector<std::uint8_t>(32, 0x43), "ghost")));
    assert(ghost.salt.size() == 16 && ghost.iterations == scram::kMinIterations);
    // The client cannot know the decoy's keys: the proof never matches.
    const auto first = scram::ParseClientFirst(std::string("n,,n=ghost,r=") + kClientNonce);
    const scram::ServerExchange exchange(first, ghost, kServerNonce);
    assert(!exchange.Finish(kClientFinal).has_value());

    const std::string a = scram::NewNonce();
    const std::string b = scram::NewNonce();
    assert(a.size() == 24 && a != b && a.find(',') == std::string::npos);
}

void TestVerifierText() {
    const auto verifier = PencilVerifier();
    const std::string text = scram::FormatVerifier(verifier);
    assert(text.rfind("SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$", 0) == 0);
    assert(scram::ParseVerifier(text) == verifier);
    ExpectInvalid([] { (void)scram::ParseVerifier("pencil"); }, "SCRAM-SHA-256$<iterations>");
    ExpectInvalid([&] { (void)scram::ParseVerifier("SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ=="); }, "SCRAM-SHA-256$<iterations>");
    const auto weak = scram::MakeVerifier("pencil", *crypto::Base64Decode("W22ZaJ0SNY7soEsUEjb6gQ=="), 1000);
    ExpectInvalid([&] { (void)scram::ParseVerifier(scram::FormatVerifier(weak)); }, "at least 4096 iterations");
    auto short_salt = verifier;
    short_salt.salt.resize(8);
    ExpectInvalid([&] { (void)scram::ParseVerifier(scram::FormatVerifier(short_salt)); }, "salt must be at least 16 bytes");
    std::string broken = text;
    broken.back() = '!';
    ExpectInvalid([&] { (void)scram::ParseVerifier(broken); }, "ServerKey must be 32 bytes");
}

}  // namespace

int main() {
    TestRfcExchange();
    TestClientSide();
    TestWrongPassword();
    TestMalformedMessages();
    TestDecoyAndNonce();
    TestVerifierText();
    return 0;
}
