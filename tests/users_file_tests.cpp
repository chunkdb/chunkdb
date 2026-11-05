// chunkdb.users (docs/design/USERS_DESIGN.md): what a round trip keeps, how rights
// combine, and how a damaged file is refused.

#include <cassert>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

#include "test_utils.hpp"
#include "chunkdb/crc32.hpp"
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

// The file carries every user's salt, iteration count, StoredKey and
// ServerKey. Readable by anyone they allow offline password guessing at the
// cost of the iteration count, so only the owner may read it -- on the first
// write and on every rewrite.
void TestFileIsOwnerOnly() {
#ifndef _WIN32
    chunkdb::test::ScopedTempDir dir("chunkdb-users-mode");
    const auto path = dir.path() / chunkdb::kUsersFileName;
    const auto owner_only = [&path] {
        const auto mode = std::filesystem::status(path).permissions();
        return (mode & ~(std::filesystem::perms::owner_read | std::filesystem::perms::owner_write)) ==
               std::filesystem::perms::none;
    };
    chunkdb::WriteUsersFile(dir.path(), Sample());
    assert(owner_only());
    chunkdb::WriteUsersFile(dir.path(), Sample());
    assert(owner_only());
#endif
}

// A verifier whose iteration count every login would have to spend is not
// readable back either: the count reaches clients in the server-first message.
void TestIterationBounds() {
    Users users = Sample();
    auto costly = users.users["bot"].verifier;
    costly.iterations = chunkdb::scram::kMaxIterations + 1U;
    users.users["bot"].verifier = costly;
    ExpectInvalid([&] { (void)chunkdb::DecodeUsers(chunkdb::EncodeUsers(users)); }, "verifier iterations");

    Users weak = Sample();
    weak.users["bot"].verifier.iterations = chunkdb::scram::kMinIterations - 1U;
    ExpectInvalid([&] { (void)chunkdb::DecodeUsers(chunkdb::EncodeUsers(weak)); }, "verifier iterations");
}


// One user in the users file layout, with each field as given, so a file the
// encoder would refuse can still be read.
struct RawUser {
    std::string name = "bot";
    std::size_t salt_bytes = 16;
    std::string table = "world";
};

std::vector<std::uint8_t> RawUsersFile(const RawUser& raw) {
    std::vector<std::uint8_t> out;
    out.reserve(160U + raw.name.size() + raw.salt_bytes + raw.table.size());
    for (const char c : std::string("CKDU")) out.push_back(static_cast<std::uint8_t>(c));
    const auto u16 = [&](std::size_t value) {
        out.push_back(static_cast<std::uint8_t>(value));
        out.push_back(static_cast<std::uint8_t>(value >> 8U));
    };
    const auto u32 = [&](std::uint32_t value) {
        for (std::size_t i = 0; i < 4; ++i) out.push_back(static_cast<std::uint8_t>(value >> (8U * i)));
    };
    const auto text = [&](const std::string& value) {
        u16(value.size());
        out.insert(out.end(), value.begin(), value.end());
    };
    u32(1);
    out.insert(out.end(), 32, 0x07);
    u32(1);
    text(raw.name);
    out.push_back(0);
    u32(chunkdb::scram::kMinIterations);
    u16(raw.salt_bytes);
    out.insert(out.end(), raw.salt_bytes, 0x5a);
    out.insert(out.end(), 64, 0x11);
    u32(1);
    text(raw.table);
    out.push_back(static_cast<std::uint8_t>(Right::kRead));
    u32(chunkdb::Crc32(out));
    return out;
}

// A salt length, user name or grant table name the statements refuse is
// refused by the encoder or the decoder too, so what one writes the other
// reads back field for field.
void TestFieldBounds() {
    const auto valid = chunkdb::DecodeUsers(RawUsersFile({}));
    assert(valid.users.at("bot").verifier.salt.size() == 16U && valid.users.at("bot").grants.at("world") == Right::kRead);
    const auto longest = chunkdb::DecodeUsers(RawUsersFile({.salt_bytes = chunkdb::scram::kMaxSaltBytes}));
    assert(chunkdb::EncodeUsers(longest) == RawUsersFile({.salt_bytes = chunkdb::scram::kMaxSaltBytes}));
    (void)chunkdb::DecodeUsers(RawUsersFile({.table = "*"}));

    ExpectInvalid([] { (void)chunkdb::DecodeUsers(RawUsersFile({.salt_bytes = 15})); }, "15-byte verifier salt, outside 16..256");
    ExpectInvalid([] { (void)chunkdb::DecodeUsers(RawUsersFile({.salt_bytes = 257})); }, "257-byte verifier salt, outside 16..256");
    for (const auto* name : {"", "Bot", "9bot", "bot-1", "a\nb"}) {
        ExpectInvalid([&] { (void)chunkdb::DecodeUsers(RawUsersFile({.name = name})); }, "invalid user name");
    }
    ExpectInvalid([] { (void)chunkdb::DecodeUsers(RawUsersFile({.name = std::string(64, 'a')})); }, "invalid user name");
    for (const auto* table : {"", "con", "_world", "World", "a/b", "**"}) {
        ExpectInvalid([&] { (void)chunkdb::DecodeUsers(RawUsersFile({.table = table})); }, "invalid table name");
    }
    ExpectInvalid([] { (void)chunkdb::DecodeUsers(RawUsersFile({.table = std::string(65, 'a')})); }, "invalid table name");

    // A salt longer than its 16-bit length field is refused, not cut short.
    for (const std::size_t size : {std::size_t{15}, chunkdb::scram::kMaxSaltBytes + 1U, std::size_t{65536 + 16}}) {
        Users users = Sample();
        users.users["bot"].verifier.salt.assign(size, 0x5a);
        ExpectInvalid([&] { (void)chunkdb::EncodeUsers(users); }, "-byte verifier salt; chunkdb.users takes 16..256 bytes");
    }
    Users long_name = Sample();
    long_name.users[std::string(0x10000, 'a')] = long_name.users["bot"];
    ExpectInvalid([&] { (void)chunkdb::EncodeUsers(long_name); }, "longer than 65535 bytes");
    Users long_table = Sample();
    long_table.users["bot"].grants[std::string(0x10000, 'a')] = Right::kRead;
    ExpectInvalid([&] { (void)chunkdb::EncodeUsers(long_table); }, "longer than 65535 bytes");
}

}  // namespace

int main() {
    TestRoundTrip();
    TestRights();
    TestDamagedFiles();
    TestFile();
    TestFileIsOwnerOnly();
    TestIterationBounds();
    TestFieldBounds();
    return 0;
}
