// The server side of SCRAM-SHA-256 login (docs/design/USERS_DESIGN.md): the
// exchange of RFC 7677, wrong passwords, malformed messages and verifiers.

#include <cassert>
#include <cstdio>
#include <functional>
#include <stdexcept>
#include <string>

#include "scram.hpp"
#include "login_helpers.hpp"
#include "test_utils.hpp"

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
    const scram::ServerExchange correct(first, PencilVerifier(), kServerNonce);
    const auto login = scram::StartClientLogin("user", kClientNonce);
    const auto wrong_client = scram::FinishClientLogin(login, "pencils", correct.ServerFirst());
    assert(!correct.Finish(wrong_client.message));
}

void TestPreparedSocketLogin() {
    const auto salt = *crypto::Base64Decode("W22ZaJ0SNY7soEsUEjb6gQ==");
    const auto prepared = chunkdb::test::PrepareLoginVerifier("pencil", salt, 4096);
    assert(prepared == PencilVerifier());
    const auto login = scram::StartClientLogin("user", kClientNonce);
    const scram::ServerExchange exchange(scram::ParseClientFirst(login.first), prepared, kServerNonce);
    const auto final = chunkdb::test::AuthBytes(login, "pencil", "+SCRAM " + exchange.ServerFirst());
    assert(final.bytes == chunkdb::test::LineWithFrame("AUTH $1", kClientFinal));
    assert(exchange.Finish(kClientFinal) == final.server_signature);
    // A fixture must prepare credentials before opening a connection.
    // Falling back to PBKDF2 here reintroduces work inside the HELLO deadline.
    bool refused = false;
    try { (void)chunkdb::test::AuthBytes(login, "not-prepared", exchange.ServerFirst()); }
    catch (const std::logic_error&) { refused = true; }
    assert(refused);
    ExpectInvalid([&] { (void)chunkdb::test::AuthBytes(login, "pencil", "r=other,s=AAAA,i=4096"); },
                  "does not continue");
    ExpectInvalid([&] { (void)chunkdb::test::AuthBytes(login, "pencil", "nonsense"); },
                  "not a SCRAM server-first");
    const auto wrong = chunkdb::test::PrepareLoginVerifier("pencils", salt, 4096);
    assert(wrong != prepared);
    const auto wrong_final = chunkdb::test::AuthBytes(login, "pencils", exchange.ServerFirst());
    const auto prefix = wrong_final.bytes.find("\r\n", wrong_final.bytes.find("\r\n") + 2U) + 2U;
    assert(!exchange.Finish(std::string_view(wrong_final.bytes).substr(prefix, wrong_final.bytes.size() - prefix - 2U)));
}

void TestPrepareExistingFixtureUser() {
    chunkdb::test::ScopedTempDir directory("chunkdb-prepared-existing-user");
    scram::Verifier stored;
    {
        const auto users = chunkdb::test::MakeUsers(directory.path(), "admin", "secret");
        stored = users->Find("admin")->verifier;
    }
    // Opening the same fixture in a new process has no cached credentials.
    {
        std::lock_guard lock(chunkdb::test::login_detail::keys_mutex);
        chunkdb::test::login_detail::prepared_keys.clear();
    }
    const auto users = chunkdb::test::MakeUsers(directory.path(), "admin", "secret");
    assert(users->Find("admin")->verifier == stored);
    const auto login = scram::StartClientLogin("admin", kClientNonce);
    const scram::ServerExchange exchange(scram::ParseClientFirst(login.first), stored, kServerNonce);
    const auto final = chunkdb::test::AuthBytes(login, "secret", exchange.ServerFirst());
    const auto frame = final.bytes.find("\r\n", final.bytes.find("\r\n") + 2U) + 2U;
    assert(exchange.Finish(std::string_view(final.bytes).substr(frame, final.bytes.size() - frame - 2U)) ==
           final.server_signature);
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
    ExpectInvalid([&] { (void)scram::ParseVerifier(scram::FormatVerifier(weak)); }, "4096 to 1000000 iterations");
    // An iteration count every client would have to spend on each login is
    // refused as well; the count itself needs no PBKDF2 run to reject.
    auto costly = verifier;
    costly.iterations = scram::kMaxIterations + 1U;
    ExpectInvalid([&] { (void)scram::ParseVerifier(scram::FormatVerifier(costly)); }, "4096 to 1000000 iterations");
    auto most = verifier;
    most.iterations = scram::kMaxIterations;
    assert(scram::ParseVerifier(scram::FormatVerifier(most)).iterations == scram::kMaxIterations);
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
    TestPreparedSocketLogin();
    TestPrepareExistingFixtureUser();
    TestMalformedMessages();
    TestDecoyAndNonce();
    TestVerifierText();
    return 0;
}
