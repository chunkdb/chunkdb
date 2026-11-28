#include "users.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>

#include "checkpoint.hpp"
#include "statement_control.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/table_catalog.hpp"
#include "user_registry.hpp"

namespace chunkdb {

namespace {

constexpr std::array<std::uint8_t, 4> kMagic = {'C', 'K', 'D', 'U'};
// Version 3 adds the added verifier of a user, the server rights in flag bits
// 2..4 and prefix grants. Version 2 files, which have none of them, are still
// read. Builds that read version 3 without the server rights refuse the bits
// as unknown flags and a prefix grant as an invalid table name.
constexpr std::uint32_t kVersion = 3;
constexpr std::uint32_t kVersionWithoutAddedVerifier = 2;
constexpr unsigned kServerRightsShift = 2U;

void PutU16(std::vector<std::uint8_t>* out, std::uint16_t value) {
    out->push_back(static_cast<std::uint8_t>(value));
    out->push_back(static_cast<std::uint8_t>(value >> 8U));
}

void PutU32(std::vector<std::uint8_t>* out, std::uint32_t value) {
    for (std::size_t i = 0; i < 4; ++i) {
        out->push_back(static_cast<std::uint8_t>(value >> (8U * i)));
    }
}

void PutU64(std::vector<std::uint8_t>* out, std::uint64_t value) {
    for (std::size_t i = 0; i < 8; ++i) {
        out->push_back(static_cast<std::uint8_t>(value >> (8U * i)));
    }
}

void ValidateLimits(const User& user) {
    if (user.statements_per_sec > kMaxUserStatementsPerSecond) {
        throw std::invalid_argument("statements_per_sec exceeds 65536 in chunkdb.users");
    }
}

void PutBytes(std::vector<std::uint8_t>* out, const std::uint8_t* data, std::size_t size) {
    out->insert(out->end(), data, data + size);
}

void ValidateShortString(const std::string& text) {
    if (text.size() > 0xFFFFU) {
        throw std::invalid_argument("a name in chunkdb.users is longer than 65535 bytes");
    }
}

[[nodiscard]] bool SaltLengthFits(std::size_t size) noexcept {
    return size >= scram::kMinSaltBytes && size <= scram::kMaxSaltBytes;
}

[[nodiscard]] std::string SaltBounds() {
    return std::to_string(scram::kMinSaltBytes) + ".." + std::to_string(scram::kMaxSaltBytes);
}

void ValidateUsers(const Users& users) {
    if (users.users.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("chunkdb.users has more than 4294967295 users");
    }
    for (const auto& [name, user] : users.users) {
        ValidateShortString(name);
        ValidateLimits(user);
        if (user.verifier.iterations < scram::kMinIterations || user.verifier.iterations > scram::kMaxIterations) {
            throw std::invalid_argument(
                "chunkdb.users gives user " + name + " " + std::to_string(user.verifier.iterations) +
                " verifier iterations, outside " + std::to_string(scram::kMinIterations) + ".." +
                std::to_string(scram::kMaxIterations));
        }
        if (!SaltLengthFits(user.verifier.salt.size())) {
            throw std::invalid_argument(
                "user " + name + " has a " + std::to_string(user.verifier.salt.size()) +
                "-byte verifier salt; chunkdb.users takes " + SaltBounds() + " bytes");
        }
        if (user.added_verifier.has_value()) {
            RequireAddableVerifier(user.verifier, *user.added_verifier);
        }
        if (user.grants.size() > std::numeric_limits<std::uint32_t>::max() - user.prefix_grants.size()) {
            throw std::invalid_argument("chunkdb.users has more than 4294967295 grants for user " + name);
        }
        if ((user.server_rights & ~kKnownServerRights) != 0U) {
            throw std::invalid_argument("chunkdb.users has an unknown server right for user " + name);
        }
        const auto check = [&name](const std::string& table, Right right) {
            ValidateShortString(table);
            if (right < Right::kRead || right > Right::kAdmin) {
                throw std::invalid_argument("chunkdb.users has an unknown right for user " + name);
            }
        };
        for (const auto& [table, right] : user.grants) {
            if (IsPrefixGrant(table)) {
                throw std::invalid_argument("user " + name + " has a prefix grant among its table grants");
            }
            check(table, right);
        }
        for (const auto& [prefix, right] : user.prefix_grants) {
            if (!IsValidGrantTable(prefix + "*")) {
                throw std::invalid_argument("user " + name + " has a prefix grant on an invalid table prefix");
            }
            check(prefix + "*", right);
        }
    }
}

void PutShortString(std::vector<std::uint8_t>* out, const std::string& text) {
    PutU16(out, static_cast<std::uint16_t>(text.size()));
    PutBytes(out, reinterpret_cast<const std::uint8_t*>(text.data()), text.size());
}

void PutCount(std::vector<std::uint8_t>* out, std::size_t count) {
    if (count > 0xFFFFFFFFU) {
        throw std::invalid_argument("chunkdb.users holds at most 4294967295 users, and a user at most as many grants");
    }
    PutU32(out, static_cast<std::uint32_t>(count));
}

class Reader {
  public:
    explicit Reader(const std::vector<std::uint8_t>& bytes, std::size_t end) : bytes_(bytes), end_(end) {}

    [[nodiscard]] const std::uint8_t* Take(std::size_t size) {
        if (end_ - at_ < size) {
            throw std::invalid_argument("chunkdb.users is truncated");
        }
        const std::uint8_t* data = bytes_.data() + at_;
        at_ += size;
        return data;
    }
    [[nodiscard]] std::uint8_t U8() { return *Take(1); }
    [[nodiscard]] std::uint16_t U16() {
        const auto* data = Take(2);
        return static_cast<std::uint16_t>(data[0] | (data[1] << 8U));
    }
    [[nodiscard]] std::uint32_t U32() {
        const auto* data = Take(4);
        std::uint32_t value = 0;
        for (std::size_t i = 0; i < 4; ++i) {
            value |= static_cast<std::uint32_t>(data[i]) << (8U * i);
        }
        return value;
    }
    [[nodiscard]] std::uint64_t U64() {
        const auto* data = Take(8);
        std::uint64_t value = 0;
        for (std::size_t i = 0; i < 8; ++i) {
            value |= static_cast<std::uint64_t>(data[i]) << (8U * i);
        }
        return value;
    }
    [[nodiscard]] std::string ShortString() {
        const std::uint16_t size = U16();
        const auto* data = Take(size);
        return std::string(reinterpret_cast<const char*>(data), size);
    }
    [[nodiscard]] bool AtEnd() const noexcept { return at_ == end_; }

  private:
    const std::vector<std::uint8_t>& bytes_;
    std::size_t end_ = 0;
    std::size_t at_ = 0;
};

}  // namespace

namespace {

// What could start a table name: the characters IsValidTableName takes, at
// most kMaxTableNameLength of them, a letter or digit first. A prefix may be
// a reserved name itself (`con*` covers `console`).
[[nodiscard]] bool IsValidTablePrefix(std::string_view prefix) noexcept {
    const auto lower_alnum = [](char c) { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); };
    return !prefix.empty() && prefix.size() <= kMaxTableNameLength && lower_alnum(prefix.front()) &&
        std::all_of(prefix.begin(), prefix.end(), [&](char c) { return lower_alnum(c) || c == '_' || c == '-'; });
}

}  // namespace

bool IsPrefixGrant(std::string_view table) noexcept {
    return table.size() > 1U && table.back() == '*';
}

bool IsValidGrantTable(std::string_view table) noexcept {
    if (IsPrefixGrant(table)) return IsValidTablePrefix(table.substr(0U, table.size() - 1U));
    return table == kEveryTable || IsValidTableName(table);
}

void RequireValidGrantTable(std::string_view table) {
    if (IsPrefixGrant(table)) {
        if (!IsValidGrantTable(table)) throw std::invalid_argument("invalid table prefix '" + std::string(table) + "'");
        return;
    }
    if (table != kEveryTable) {
        RequireValidTableName(table);
    }
}

bool HoldsVerifier(const User& user, const scram::Verifier& verifier) noexcept {
    return user.verifier == verifier || user.added_verifier == verifier;
}

void RequireAddableVerifier(const scram::Verifier& verifier, const scram::Verifier& added) {
    if (added.salt != verifier.salt || added.iterations != verifier.iterations) {
        throw std::invalid_argument(
            "an added verifier must use the salt and iteration count of the user's verifier, which a login as "
            "the user receives in the SCRAM server-first message");
    }
    if (added == verifier) {
        throw std::invalid_argument("an added verifier must differ from the user's verifier");
    }
}

std::optional<Right> RightOn(const User& user, const std::string& table) {
    return RightOn(user.grants, user.prefix_grants, table);
}

std::optional<Right> RightOn(const std::map<std::string, Right>& grants,
    const std::map<std::string, Right>& prefix_grants, const std::string& table) {
    std::optional<Right> best;
    const auto take = [&best](Right right) {
        if (!best.has_value() || right > *best) best = right;
    };
    if (const auto found = grants.find(table); found != grants.end()) take(found->second);
    if (const auto found = grants.find(kEveryTable); found != grants.end()) take(found->second);
    // A prefix of `table` sorts at or before it.
    for (auto at = prefix_grants.begin(); at != prefix_grants.end() && at->first <= table; ++at) {
        if (table.starts_with(at->first)) take(at->second);
    }
    return best;
}

bool HasAnyGrant(const User& user, Right right) noexcept {
    const auto reaches = [right](const auto& grant) { return grant.second >= right; };
    return std::any_of(user.grants.begin(), user.grants.end(), reaches) ||
        std::any_of(user.prefix_grants.begin(), user.prefix_grants.end(), reaches);
}

void ApplyGrant(User& user, const std::string& table, std::optional<Right> right, std::uint8_t server_rights, bool revoke) {
    if ((server_rights & ~kKnownServerRights) != 0U) throw std::invalid_argument("unknown server right");
    if (server_rights != 0U && table != kEveryTable)
        throw std::invalid_argument("MONITOR, BACKUP and REPLICATION are granted ON *");
    if (revoke) user.server_rights = static_cast<std::uint8_t>(user.server_rights & ~server_rights);
    else user.server_rights = static_cast<std::uint8_t>(user.server_rights | server_rights);
    if (!right.has_value()) return;
    const bool prefix = IsPrefixGrant(table);
    auto& grants = prefix ? user.prefix_grants : user.grants;
    const std::string key = prefix ? table.substr(0U, table.size() - 1U) : table;
    const auto found = grants.find(key);
    if (!revoke) {
        if (found == grants.end() || found->second < *right) grants[key] = *right;
        return;
    }
    if (found == grants.end() || found->second < *right) return;
    if (*right == Right::kRead) grants.erase(found);
    else found->second = static_cast<Right>(static_cast<std::uint8_t>(*right) - 1U);
}

std::vector<std::uint8_t> EncodeUsers(const Users& users) {
    ValidateUsers(users);
    std::vector<std::uint8_t> out(kMagic.begin(), kMagic.end());
    PutU32(&out, kVersion);
    PutBytes(&out, users.secret.data(), users.secret.size());
    PutCount(&out, users.users.size());
    for (const auto& [name, user] : users.users) {
        PutShortString(&out, name);
        out.push_back(static_cast<std::uint8_t>((user.manages_users ? 1U : 0U) | (user.requires_channel_binding ? 2U : 0U) |
            (static_cast<unsigned>(user.server_rights) << kServerRightsShift)));
        PutU32(&out, user.verifier.iterations);
        PutU16(&out, static_cast<std::uint16_t>(user.verifier.salt.size()));
        PutBytes(&out, user.verifier.salt.data(), user.verifier.salt.size());
        PutBytes(&out, user.verifier.stored_key.data(), user.verifier.stored_key.size());
        PutBytes(&out, user.verifier.server_key.data(), user.verifier.server_key.size());
        out.push_back(user.added_verifier.has_value() ? 1U : 0U);
        if (user.added_verifier.has_value()) {
            PutBytes(&out, user.added_verifier->stored_key.data(), user.added_verifier->stored_key.size());
            PutBytes(&out, user.added_verifier->server_key.data(), user.added_verifier->server_key.size());
        }
        PutCount(&out, user.grants.size() + user.prefix_grants.size());
        for (const auto& [table, right] : user.grants) {
            PutShortString(&out, table);
            out.push_back(static_cast<std::uint8_t>(right));
        }
        for (const auto& [prefix, right] : user.prefix_grants) {
            PutShortString(&out, prefix + "*");
            out.push_back(static_cast<std::uint8_t>(right));
        }
        PutU64(&out, user.max_connections);
        PutU64(&out, user.statements_per_sec);
    }
    PutU32(&out, Crc32(out));
    if (out.size() > kMaxUsersFileBytes) {
        throw std::invalid_argument("chunkdb.users would take " + std::to_string(out.size()) + " bytes, more than " +
            std::to_string(kMaxUsersFileBytes));
    }
    return out;
}

Users DecodeUsers(const std::vector<std::uint8_t>& bytes) {
    if (bytes.size() > kMaxUsersFileBytes) {
        throw std::invalid_argument("chunkdb.users is larger than " + std::to_string(kMaxUsersFileBytes) + " bytes");
    }
    if (bytes.size() < kMagic.size() + 8U || !std::equal(kMagic.begin(), kMagic.end(), bytes.begin())) {
        throw std::invalid_argument("chunkdb.users is not a chunkdb users file");
    }
    const std::size_t body = bytes.size() - 4U;
    std::uint32_t stored_crc = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        stored_crc |= static_cast<std::uint32_t>(bytes[body + i]) << (8U * i);
    }
    if (Crc32(bytes.data(), body) != stored_crc) {
        throw std::invalid_argument("chunkdb.users is damaged: its checksum does not match");
    }
    Reader reader(bytes, body);
    (void)reader.Take(kMagic.size());
    const std::uint32_t version = reader.U32();
    if (version != kVersion && version != kVersionWithoutAddedVerifier) {
        if (version < kVersionWithoutAddedVerifier) {
            throw std::invalid_argument(
                "chunkdb.users was created by an earlier development build of 2.0; recreate it");
        }
        throw std::invalid_argument("chunkdb.users has version " + std::to_string(version) + ", which this build does not read");
    }
    Users users;
    std::copy_n(reader.Take(users.secret.size()), users.secret.size(), users.secret.begin());
    const std::uint32_t count = reader.U32();
    for (std::uint32_t i = 0; i < count; ++i) {
        std::string name = reader.ShortString();
        if (!IsValidUserName(name)) {
            throw std::invalid_argument("chunkdb.users has an invalid user name");
        }
        User user;
        const std::uint8_t flags = reader.U8();
        const unsigned known_flags =
            version >= kVersion ? 3U | (static_cast<unsigned>(kKnownServerRights) << kServerRightsShift) : 3U;
        if ((flags & ~known_flags) != 0U) {
            throw std::invalid_argument("chunkdb.users has unknown flags for user " + name);
        }
        user.manages_users = (flags & 1U) != 0U;
        user.requires_channel_binding = (flags & 2U) != 0U;
        user.server_rights = static_cast<std::uint8_t>(flags >> kServerRightsShift);
        user.verifier.iterations = reader.U32();
        if (user.verifier.iterations < scram::kMinIterations || user.verifier.iterations > scram::kMaxIterations) {
            throw std::invalid_argument(
                "chunkdb.users gives user " + name + " " + std::to_string(user.verifier.iterations) +
                " verifier iterations, outside " + std::to_string(scram::kMinIterations) + ".." +
                std::to_string(scram::kMaxIterations));
        }
        const std::uint16_t salt_size = reader.U16();
        if (!SaltLengthFits(salt_size)) {
            throw std::invalid_argument(
                "chunkdb.users gives user " + name + " a " + std::to_string(salt_size) +
                "-byte verifier salt, outside " + SaltBounds());
        }
        const auto* salt = reader.Take(salt_size);
        user.verifier.salt.assign(salt, salt + salt_size);
        std::copy_n(reader.Take(32), 32, user.verifier.stored_key.begin());
        std::copy_n(reader.Take(32), 32, user.verifier.server_key.begin());
        if (version >= kVersion) {
            const std::uint8_t added = reader.U8();
            if (added > 1U) {
                throw std::invalid_argument("chunkdb.users gives user " + name + " more than two verifiers");
            }
            if (added == 1U) {
                scram::Verifier verifier{.iterations = user.verifier.iterations, .salt = user.verifier.salt,
                    .stored_key = {}, .server_key = {}};
                std::copy_n(reader.Take(32), 32, verifier.stored_key.begin());
                std::copy_n(reader.Take(32), 32, verifier.server_key.begin());
                if (verifier == user.verifier) {
                    throw std::invalid_argument("chunkdb.users repeats a verifier of user " + name);
                }
                user.added_verifier = std::move(verifier);
            }
        }
        const std::uint32_t grants = reader.U32();
        for (std::uint32_t g = 0; g < grants; ++g) {
            std::string table = reader.ShortString();
            if (!IsValidGrantTable(table) || (version < kVersion && IsPrefixGrant(table))) {
                throw std::invalid_argument(
                    "chunkdb.users grants user " + name + " a right on an invalid table name");
            }
            const std::uint8_t right = reader.U8();
            if (right < static_cast<std::uint8_t>(Right::kRead) || right > static_cast<std::uint8_t>(Right::kAdmin)) {
                throw std::invalid_argument("chunkdb.users has an unknown right for user " + name);
            }
            const bool prefix = IsPrefixGrant(table);
            if (prefix) table.pop_back();
            if (!(prefix ? user.prefix_grants : user.grants).emplace(std::move(table), static_cast<Right>(right)).second) {
                throw std::invalid_argument("chunkdb.users repeats a grant of user " + name);
            }
        }
        user.max_connections = reader.U64();
        user.statements_per_sec = reader.U64();
        ValidateLimits(user);
        if (!users.users.emplace(name, std::move(user)).second) {
            throw std::invalid_argument("chunkdb.users repeats user " + name);
        }
    }
    if (!reader.AtEnd()) {
        throw std::invalid_argument("chunkdb.users has bytes after its users");
    }
    return users;
}

std::optional<std::vector<std::uint8_t>> ReadUsersFileImage(const std::filesystem::path& data_dir) {
    const auto path = data_dir / kUsersFileName;
    if (!std::filesystem::exists(path)) {
        return std::nullopt;
    }
    // Checked before the file is read whole.
    if (std::filesystem::file_size(path) > kMaxUsersFileBytes) {
        throw std::invalid_argument("chunkdb.users is larger than " + std::to_string(kMaxUsersFileBytes) + " bytes");
    }
    return LoadFile(path);
}

std::optional<Users> ReadUsersFile(const std::filesystem::path& data_dir) {
    const auto image = ReadUsersFileImage(data_dir);
    if (!image) return std::nullopt;
    return DecodeUsers(*image);
}

void WriteUsersFile(const std::filesystem::path& data_dir, const Users& users, bool* out_replaced) {
    if (out_replaced) *out_replaced = false;
    // Owner-only: the file holds every user's salt, iteration count, StoredKey
    // and ServerKey. They are not login-equivalent, but they let anyone who
    // reads them guess passwords offline at the cost of the iteration count,
    // and impersonate the server to a client.
    const auto bytes = EncodeUsers(users);
    AtomicWrite(
        data_dir / kUsersFileName, bytes, /*fsync_file=*/true, /*fsync_directory=*/true,
        out_replaced, /*after_rename_failpoint=*/nullptr,
        /*enable_generic_failpoints=*/true, EnterStatementDecision);
}

}  // namespace chunkdb
