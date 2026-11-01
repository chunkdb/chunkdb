// chunkdb.users (docs/design/USERS_DESIGN.md): what a round trip keeps, how rights
// combine, and how a damaged file is refused.

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <fstream>
#include <limits>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

#include "test_utils.hpp"
#include "users.hpp"
#include "user_registry.hpp"
#include "chunkdb/crc32.hpp"

#ifdef _WIN32
#include <aclapi.h>
#endif

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

void RefreshChecksum(std::vector<std::uint8_t>& bytes) {
    assert(bytes.size() >= 4U);
    const auto crc = chunkdb::Crc32(bytes.data(), bytes.size() - 4U);
    for (std::size_t i = 0; i < 4U; ++i) {
        bytes[bytes.size() - 4U + i] = static_cast<std::uint8_t>(crc >> (8U * i));
    }
}

void TestIterationBounds() {
    Users one;
    one.users.emplace("admin", Sample().users.at("admin"));
    const auto valid = chunkdb::EncodeUsers(one);
    const std::size_t iterations_at = 4U + 4U + 32U + 4U + 2U + 5U + 1U;
    for (const auto iterations : {chunkdb::scram::kMinIterations - 1U, chunkdb::scram::kMaxIterations + 1U}) {
        one.users.at("admin").verifier.iterations = iterations;
        ExpectInvalid([&] { (void)chunkdb::EncodeUsers(one); }, "verifier iterations");
        auto malformed = valid;
        for (std::size_t i = 0; i < 4U; ++i) {
            malformed[iterations_at + i] = static_cast<std::uint8_t>(iterations >> (8U * i));
        }
        RefreshChecksum(malformed);
        ExpectInvalid([&] { (void)chunkdb::DecodeUsers(malformed); }, "verifier iterations");
    }
}

void TestWriterValidation() {
    const auto good = Sample();
    std::vector<std::pair<Users, std::string>> cases;
    auto bad = good;
    bad.users.at("admin").verifier.salt.assign(65536U, 0x5a);
    cases.emplace_back(bad, "salt");
    for (const auto iterations : {0U, chunkdb::scram::kMinIterations - 1U, chunkdb::scram::kMaxIterations + 1U}) {
        bad = good;
        bad.users.at("admin").verifier.iterations = iterations;
        cases.emplace_back(bad, "verifier iterations");
    }
    for (const auto right : {0U, 4U, 255U}) {
        bad = good;
        bad.users.at("admin").grants["*"] = static_cast<Right>(right);
        cases.emplace_back(bad, "unknown right");
    }
    bad = good;
    bad.users.emplace(std::string(65536U, 'n'), good.users.at("admin"));
    cases.emplace_back(bad, "name");
    bad = good;
    bad.users.at("admin").grants.emplace(std::string(65536U, 't'), Right::kRead);
    cases.emplace_back(bad, "name");
    bad = good;
    bad.users.at("admin").statements_per_sec = chunkdb::kMaxUserStatementsPerSecond + 1U;
    cases.emplace_back(bad, "statements_per_sec");

    chunkdb::test::ScopedTempDir dir("chunkdb-users-writer-validation");
    chunkdb::WriteUsersFile(dir.path(), good);
    const auto path = dir.path() / chunkdb::kUsersFileName;
#ifdef _WIN32
    auto name = path.wstring();
    assert(SetNamedSecurityInfoW(name.data(), SE_FILE_OBJECT,
        DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
        nullptr, nullptr, nullptr, nullptr) == ERROR_SUCCESS);
    const auto permissions = [&] {
        DWORD size = 0;
        (void)GetFileSecurityW(name.c_str(), DACL_SECURITY_INFORMATION, nullptr, 0, &size);
        assert(size != 0U);
        std::vector<std::uint8_t> descriptor(size);
        assert(GetFileSecurityW(name.c_str(), DACL_SECURITY_INFORMATION,
            reinterpret_cast<PSECURITY_DESCRIPTOR>(descriptor.data()), size, &size));
        return descriptor;
    };
#else
    std::filesystem::permissions(path, std::filesystem::perms::owner_read |
        std::filesystem::perms::owner_write | std::filesystem::perms::group_read);
    const auto permissions = [&] { return std::filesystem::status(path).permissions(); };
#endif
    const auto read_bytes = [&] {
        std::ifstream file(path, std::ios::binary);
        assert(file.good());
        return std::vector<std::uint8_t>(std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{});
    };
    const auto entries = [&] {
        std::vector<std::filesystem::path> names;
        for (const auto& entry : std::filesystem::directory_iterator(dir.path())) names.push_back(entry.path().filename());
        std::sort(names.begin(), names.end());
        return names;
    };
    const auto before_bytes = read_bytes();
    const auto before_mode = permissions();
    const auto before_time = std::filesystem::last_write_time(path);
    const auto before_dir_mode = std::filesystem::status(dir.path()).permissions();
    const auto missing = dir.path() / "missing-parent" / "nested";
    const auto new_store = dir.path() / "new-store";
    std::filesystem::create_directory(new_store);
    // Capture after creating this legitimate empty store; each invalid write
    // must leave it empty and must not create an absent parent either.
    const auto existing_names = entries();
    const auto existing_dir_time = std::filesystem::last_write_time(dir.path());
    const auto new_store_time = std::filesystem::last_write_time(new_store);
    for (const auto& [invalid, error] : cases) {
        ExpectInvalid([&] { (void)chunkdb::EncodeUsers(invalid); }, error);
        ExpectInvalid([&] { chunkdb::WriteUsersFile(dir.path(), invalid); }, error);
        assert(read_bytes() == before_bytes && chunkdb::ReadUsersFile(dir.path()) == good);
        assert(permissions() == before_mode && std::filesystem::last_write_time(path) == before_time);
        ExpectInvalid([&] { chunkdb::WriteUsersFile(missing, invalid); }, error);
        ExpectInvalid([&] { chunkdb::WriteUsersFile(new_store, invalid); }, error);
        assert(!std::filesystem::exists(dir.path() / "missing-parent"));
        assert(std::filesystem::is_empty(new_store) && std::filesystem::last_write_time(new_store) == new_store_time);
        assert(entries() == existing_names && std::filesystem::last_write_time(dir.path()) == existing_dir_time);
        assert(std::filesystem::status(dir.path()).permissions() == before_dir_mode);
    }
    // Sizes and verifier bounds admitted by the decoder remain writable.
    auto extremes = good;
    auto user = good.users.at("admin");
    user.verifier.salt.assign(65535U, 0x5a);
    user.verifier.iterations = chunkdb::scram::kMaxIterations;
    user.grants.emplace(std::string(65535U, 't'), Right::kRead);
    user.max_connections = std::numeric_limits<std::uint64_t>::max();
    user.statements_per_sec = chunkdb::kMaxUserStatementsPerSecond;
    extremes.users.emplace(std::string(65535U, 'n'), user);
    extremes.users.at("nobody").verifier.salt.clear();
    assert(chunkdb::DecodeUsers(chunkdb::EncodeUsers(extremes)) == extremes);
    chunkdb::WriteUsersFile(dir.path(), extremes);
    assert(chunkdb::ReadUsersFile(dir.path()) == extremes);
}

void TestLimitsAndMandatoryFields() {
    auto users = Sample();
    users.users.at("admin").max_connections = std::numeric_limits<std::uint64_t>::max();
    users.users.at("admin").statements_per_sec = chunkdb::kMaxUserStatementsPerSecond;
    users.users.at("bot").max_connections = 256U;
    users.users.at("bot").statements_per_sec = 1U;
    assert(chunkdb::DecodeUsers(chunkdb::EncodeUsers(users)) == users);
    chunkdb::test::ScopedTempDir dir("chunkdb-users-limits");
    chunkdb::WriteUsersFile(dir.path(), users);
    assert(chunkdb::ReadUsersFile(dir.path()) == users);

    chunkdb::UserRegistry registry(dir.path(), std::nullopt, users.secret);
    assert(registry.Snapshot() == users);
    registry.SetVerifier("admin", users.users.at("bot").verifier);
    registry.Grant("admin", "extra", Right::kWrite);
    const auto saved_admin = chunkdb::ReadUsersFile(dir.path())->users.at("admin");
    assert(saved_admin.max_connections == std::numeric_limits<std::uint64_t>::max() &&
        saved_admin.statements_per_sec == chunkdb::kMaxUserStatementsPerSecond);

    Users one;
    one.users.emplace("admin", users.users.at("admin"));
    const auto bytes = chunkdb::EncodeUsers(one);
    assert(bytes[4] == 2U && bytes[5] == 0U && bytes[6] == 0U && bytes[7] == 0U);
    // The last user's two mandatory u64 fields precede the complete CRC.
    for (std::size_t i = 0; i < 8U; ++i) assert(bytes[bytes.size() - 20U + i] == 0xffU);
    assert(bytes[bytes.size() - 12U] == 0U && bytes[bytes.size() - 10U] == 1U);
    for (const std::size_t missing : {1U, 8U, 16U}) {
        auto short_record = bytes;
        short_record.erase(short_record.end() - 4 - static_cast<std::ptrdiff_t>(missing), short_record.end() - 4);
        RefreshChecksum(short_record);
        ExpectInvalid([&] { (void)chunkdb::DecodeUsers(short_record); }, "truncated");
    }
    auto excessive_rate = bytes;
    excessive_rate[excessive_rate.size() - 12U] = 1U; // 65537, with a valid CRC.
    RefreshChecksum(excessive_rate);
    ExpectInvalid([&] { (void)chunkdb::DecodeUsers(excessive_rate); }, "statements_per_sec");
    one.users.at("admin").statements_per_sec = chunkdb::kMaxUserStatementsPerSecond + 1U;
    ExpectInvalid([&] { (void)chunkdb::EncodeUsers(one); }, "statements_per_sec");
    const auto before = chunkdb::ReadUsersFile(dir.path());
    ExpectInvalid([&] { chunkdb::WriteUsersFile(dir.path(), one); }, "statements_per_sec");
    assert(chunkdb::ReadUsersFile(dir.path()) == before);
}

void TestDevelopmentAdmissionIsReadOnly() {
    chunkdb::test::ScopedTempDir dir("chunkdb-users-development");
    auto legacy = chunkdb::EncodeUsers(Users{});
    legacy[4] = 1U; // Exact empty CKDU v1 record, not a new record with guessed fields.
    RefreshChecksum(legacy);
    const auto path = dir.path() / chunkdb::kUsersFileName;
    {
        std::ofstream file(path, std::ios::binary);
        file.write(reinterpret_cast<const char*>(legacy.data()), static_cast<std::streamsize>(legacy.size()));
        assert(file.good());
    }
#ifdef _WIN32
    auto name = path.wstring();
    assert(SetNamedSecurityInfoW(name.data(), SE_FILE_OBJECT,
        DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
        nullptr, nullptr, nullptr, nullptr) == ERROR_SUCCESS);
    const auto permissions = [&] {
        DWORD size = 0;
        (void)GetFileSecurityW(name.c_str(), DACL_SECURITY_INFORMATION, nullptr, 0, &size);
        assert(size != 0U);
        std::vector<std::uint8_t> descriptor(size);
        assert(GetFileSecurityW(name.c_str(), DACL_SECURITY_INFORMATION,
            reinterpret_cast<PSECURITY_DESCRIPTOR>(descriptor.data()), size, &size));
        return descriptor;
    };
#else
    std::filesystem::permissions(path, std::filesystem::perms::owner_read |
        std::filesystem::perms::owner_write | std::filesystem::perms::group_read);
    const auto permissions = [&] { return std::filesystem::status(path).permissions(); };
#endif
    const auto before_mode = permissions();
    const auto before_time = std::filesystem::last_write_time(path);
    const auto before_entries = std::distance(std::filesystem::directory_iterator(dir.path()), {});
    const std::array<std::uint8_t, 32> secret{};
    ExpectInvalid([&] { chunkdb::UserRegistry registry(dir.path(), std::nullopt, secret); }, "recreate");
    assert(permissions() == before_mode);
    assert(std::filesystem::last_write_time(path) == before_time);
    assert(std::distance(std::filesystem::directory_iterator(dir.path()), {}) == before_entries);
    std::ifstream file(path, std::ios::binary);
    const std::vector<std::uint8_t> retained(std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{});
    assert(retained == legacy);

    chunkdb::test::ScopedTempDir fresh("chunkdb-users-standalone");
    const auto verifier = Sample().users.at("admin").verifier;
    chunkdb::UserRegistry created(fresh.path(), std::make_pair(std::string("admin"), verifier), secret);
    const auto admin = created.Find("admin");
    assert(admin && admin->max_connections == 0U && admin->statements_per_sec == 0U);
    assert(std::distance(std::filesystem::directory_iterator(fresh.path()), {}) == 1);
    chunkdb::UserRegistry reopened(fresh.path(), std::nullopt, secret);
    assert(reopened.Snapshot() == created.Snapshot());
    // An interrupted atomic-save scratch file is not the published record.
    const auto scratch = fresh.path() / "chunkdb.users.tmp";
    { std::ofstream pending(scratch, std::ios::binary); pending << "incomplete"; }
    const auto scratch_time = std::filesystem::last_write_time(scratch);
    chunkdb::UserRegistry with_scratch(fresh.path(), std::nullopt, secret);
    assert(with_scratch.Snapshot() == created.Snapshot());
    assert(std::filesystem::file_size(scratch) == 10U && std::filesystem::last_write_time(scratch) == scratch_time);
}

}  // namespace

int main() {
    TestRoundTrip();
    TestRights();
    TestDamagedFiles();
    TestFile();
    TestFileIsOwnerOnly();
    TestIterationBounds();
    TestWriterValidation();
    TestLimitsAndMandatoryFields();
    TestDevelopmentAdmissionIsReadOnly();
    return 0;
}
