// chunkdb.users (docs/design/USERS_DESIGN.md): what a round trip keeps, how rights
// combine, and how a damaged file is refused.

#include <cassert>
#include <cstdio>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

#include "test_utils.hpp"
#include "users.hpp"

namespace {

using chunkdb::Right;
using chunkdb::User;
using chunkdb::Users;

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

Users Sample() {
    Users users;
    for (std::size_t i = 0; i < users.secret.size(); ++i) {
        users.secret[i] = static_cast<std::uint8_t>(i * 7 + 1);
    }
    const std::vector<std::uint8_t> salt(16, 0x5a);
    users.users["admin"] = User{
        .verifier = chunkdb::scram::MakeVerifier("secret", salt, 4096),
        .manages_users = true,
        .grants = {{"*", Right::kAdmin}},
    };
    users.users["bot"] = User{
        .verifier = chunkdb::scram::MakeVerifier("hunter2", salt, 8192),
        .manages_users = false,
        .grants = {{"world", Right::kWrite}, {"*", Right::kRead}},
    };
    users.users["nobody"] = User{
        .verifier = chunkdb::scram::MakeVerifier("x", salt, 4096),
        .manages_users = false,
        .grants = {},
    };
    return users;
}

void TestRoundTrip() {
    const Users users = Sample();
    assert(chunkdb::DecodeUsers(chunkdb::EncodeUsers(users)) == users);
    assert(chunkdb::DecodeUsers(chunkdb::EncodeUsers(Users{})) == Users{});
}

void TestRights() {
    const Users users = Sample();
    const User& bot = users.users.at("bot");
    assert(chunkdb::RightOn(bot, "world") == Right::kWrite);
    assert(chunkdb::RightOn(bot, "other") == Right::kRead);
    assert(chunkdb::RightOn(users.users.at("admin"), "anything") == Right::kAdmin);
    assert(!chunkdb::RightOn(users.users.at("nobody"), "world").has_value());
    assert(Right::kRead < Right::kWrite && Right::kWrite < Right::kAdmin);
}

void TestDamagedFiles() {
    const auto bytes = chunkdb::EncodeUsers(Sample());
    ExpectInvalid([] { (void)chunkdb::DecodeUsers({}); }, "not a chunkdb users file");
    auto magic = bytes;
    magic[0] = 'X';
    ExpectInvalid([&] { (void)chunkdb::DecodeUsers(magic); }, "not a chunkdb users file");
    // Every single flipped byte past the magic is caught by the checksum.
    for (std::size_t i = 4; i < bytes.size(); ++i) {
        auto flipped = bytes;
        flipped[i] ^= 0x01U;
        ExpectInvalid([&] { (void)chunkdb::DecodeUsers(flipped); }, "checksum does not match");
    }
    auto truncated = bytes;
    truncated.resize(bytes.size() / 2);
    ExpectInvalid([&] { (void)chunkdb::DecodeUsers(truncated); }, "checksum does not match");
}

void TestFile() {
    chunkdb::test::ScopedTempDir dir("chunkdb-users-file");
    assert(!chunkdb::ReadUsersFile(dir.path()).has_value());
    const Users users = Sample();
    chunkdb::WriteUsersFile(dir.path(), users);
    assert(chunkdb::ReadUsersFile(dir.path()) == users);
    Users fewer = users;
    fewer.users.erase("bot");
    chunkdb::WriteUsersFile(dir.path(), fewer);
    assert(chunkdb::ReadUsersFile(dir.path()) == fewer);
}

}  // namespace

int main() {
    TestRoundTrip();
    TestRights();
    TestDamagedFiles();
    TestFile();
    return 0;
}
