// The built-in hash functions of SCRAM-SHA-256 login (docs/design/USERS_DESIGN.md)
// against the published test vectors.

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <string>
#include <vector>

#include "crypto.hpp"

namespace {

namespace crypto = chunkdb::crypto;

std::string Hex(std::span<const std::uint8_t> bytes) {
    constexpr const char* kDigits = "0123456789abcdef";
    std::string out;
    for (const auto byte : bytes) {
        out += kDigits[byte >> 4U];
        out += kDigits[byte & 0x0fU];
    }
    return out;
}

void TestSha256() {
    assert(Hex(crypto::Sha256Of(crypto::Bytes(""))) == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    assert(Hex(crypto::Sha256Of(crypto::Bytes("abc"))) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    assert(Hex(crypto::Sha256Of(crypto::Bytes("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"))) ==
           "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
    // A million 'a' fed in uneven pieces.
    crypto::Sha256 hash;
    const std::string piece(997, 'a');
    std::size_t fed = 0;
    while (fed < 1000000) {
        const std::size_t take = std::min<std::size_t>(piece.size(), 1000000 - fed);
        hash.Update(std::string_view(piece).substr(0, take));
        fed += take;
    }
    assert(Hex(hash.Finish()) == "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

void TestHmacSha256() {
    // RFC 4231 test cases 1, 2 and 6 (a key longer than a block).
    const std::vector<std::uint8_t> key1(20, 0x0b);
    assert(Hex(crypto::HmacSha256(key1, crypto::Bytes("Hi There"))) ==
           "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
    assert(Hex(crypto::HmacSha256(crypto::Bytes("Jefe"), crypto::Bytes("what do ya want for nothing?"))) ==
           "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
    const std::vector<std::uint8_t> key6(131, 0xaa);
    assert(Hex(crypto::HmacSha256(key6, crypto::Bytes("Test Using Larger Than Block-Size Key - Hash Key First"))) ==
           "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54");
}

void TestPbkdf2() {
    const auto password = crypto::Bytes("password");
    const auto salt = crypto::Bytes("salt");
    assert(Hex(crypto::Pbkdf2Sha256(password, salt, 1)) == "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b");
    assert(Hex(crypto::Pbkdf2Sha256(password, salt, 2)) == "ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43");
    assert(Hex(crypto::Pbkdf2Sha256(password, salt, 4096)) == "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a");
}

void TestBase64() {
    for (const auto& [plain, encoded] : std::vector<std::pair<std::string, std::string>>{
             {"", ""}, {"f", "Zg=="}, {"fo", "Zm8="}, {"foo", "Zm9v"}, {"foob", "Zm9vYg=="}, {"fooba", "Zm9vYmE="},
             {"foobar", "Zm9vYmFy"}}) {
        assert(crypto::Base64Encode(crypto::Bytes(plain)) == encoded);
        const auto decoded = crypto::Base64Decode(encoded);
        assert(decoded.has_value() && std::string(decoded->begin(), decoded->end()) == plain);
    }
    for (const char* bad : {"Zg", "Zg=", "Z===", "Zh==", "Zm9v!", "=Zm9", "Zg==Zm9v"}) {
        assert(!crypto::Base64Decode(bad).has_value());
    }
}

// RFC 7677 section 3: the whole SCRAM-SHA-256 exchange for user "user" with
// password "pencil", computed from the primitives.
void TestScramExample() {
    const auto salt = crypto::Base64Decode("W22ZaJ0SNY7soEsUEjb6gQ==");
    assert(salt.has_value());
    const auto salted = crypto::Pbkdf2Sha256(crypto::Bytes("pencil"), *salt, 4096);
    const auto client_key = crypto::HmacSha256(salted, crypto::Bytes("Client Key"));
    const auto stored_key = crypto::Sha256Of(client_key);
    const auto server_key = crypto::HmacSha256(salted, crypto::Bytes("Server Key"));
    const std::string auth_message =
        "n=user,r=rOprNGfwEbeRWgbNEkqO,"
        "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,"
        "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    const auto signature = crypto::HmacSha256(stored_key, crypto::Bytes(auth_message));
    crypto::Sha256Digest proof{};
    for (std::size_t i = 0; i < proof.size(); ++i) {
        proof[i] = static_cast<std::uint8_t>(client_key[i] ^ signature[i]);
    }
    assert(crypto::Base64Encode(proof) == "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=");
    const auto server_signature = crypto::HmacSha256(server_key, crypto::Bytes(auth_message));
    assert(crypto::Base64Encode(server_signature) == "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
}

void TestConstantTimeEqual() {
    assert(crypto::ConstantTimeEqual(crypto::Bytes("abc"), crypto::Bytes("abc")));
    assert(!crypto::ConstantTimeEqual(crypto::Bytes("abc"), crypto::Bytes("abd")));
    assert(!crypto::ConstantTimeEqual(crypto::Bytes("abc"), crypto::Bytes("ab")));
}

}  // namespace

int main() {
    TestSha256();
    TestHmacSha256();
    TestPbkdf2();
    TestBase64();
    TestScramExample();
    TestConstantTimeEqual();
    return 0;
}
