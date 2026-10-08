#include "users.hpp"

#include <algorithm>
#include <stdexcept>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"

namespace chunkdb {

namespace {

constexpr std::array<std::uint8_t, 4> kMagic = {'C', 'K', 'D', 'U'};
constexpr std::uint32_t kVersion = 1;

void PutU16(std::vector<std::uint8_t>* out, std::uint16_t value) {
    out->push_back(static_cast<std::uint8_t>(value));
    out->push_back(static_cast<std::uint8_t>(value >> 8U));
}

void PutU32(std::vector<std::uint8_t>* out, std::uint32_t value) {
    for (std::size_t i = 0; i < 4; ++i) {
        out->push_back(static_cast<std::uint8_t>(value >> (8U * i)));
    }
}

void PutBytes(std::vector<std::uint8_t>* out, const std::uint8_t* data, std::size_t size) {
    out->insert(out->end(), data, data + size);
}

void PutShortString(std::vector<std::uint8_t>* out, const std::string& text) {
    if (text.size() > 0xFFFFU) {
        throw std::invalid_argument("a name in chunkdb.users is longer than 65535 bytes");
    }
    PutU16(out, static_cast<std::uint16_t>(text.size()));
    PutBytes(out, reinterpret_cast<const std::uint8_t*>(text.data()), text.size());
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

std::optional<Right> RightOn(const User& user, const std::string& table) {
    std::optional<Right> best;
    for (const auto& name : {table, std::string(kEveryTable)}) {
        if (const auto found = user.grants.find(name); found != user.grants.end()) {
            if (!best.has_value() || found->second > *best) {
                best = found->second;
            }
        }
    }
    return best;
}

std::vector<std::uint8_t> EncodeUsers(const Users& users) {
    std::vector<std::uint8_t> out(kMagic.begin(), kMagic.end());
    PutU32(&out, kVersion);
    PutBytes(&out, users.secret.data(), users.secret.size());
    PutU32(&out, static_cast<std::uint32_t>(users.users.size()));
    for (const auto& [name, user] : users.users) {
        PutShortString(&out, name);
        out.push_back(user.manages_users ? 1U : 0U);
        PutU32(&out, user.verifier.iterations);
        PutU16(&out, static_cast<std::uint16_t>(user.verifier.salt.size()));
        PutBytes(&out, user.verifier.salt.data(), user.verifier.salt.size());
        PutBytes(&out, user.verifier.stored_key.data(), user.verifier.stored_key.size());
        PutBytes(&out, user.verifier.server_key.data(), user.verifier.server_key.size());
        PutU32(&out, static_cast<std::uint32_t>(user.grants.size()));
        for (const auto& [table, right] : user.grants) {
            PutShortString(&out, table);
            out.push_back(static_cast<std::uint8_t>(right));
        }
    }
    PutU32(&out, Crc32(out));
    return out;
}

Users DecodeUsers(const std::vector<std::uint8_t>& bytes) {
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
    if (const std::uint32_t version = reader.U32(); version != kVersion) {
        throw std::invalid_argument("chunkdb.users has version " + std::to_string(version) + ", which this build does not read");
    }
    Users users;
    std::copy_n(reader.Take(users.secret.size()), users.secret.size(), users.secret.begin());
    const std::uint32_t count = reader.U32();
    for (std::uint32_t i = 0; i < count; ++i) {
        std::string name = reader.ShortString();
        User user;
        const std::uint8_t flags = reader.U8();
        if ((flags & ~1U) != 0U) {
            throw std::invalid_argument("chunkdb.users has unknown flags for user " + name);
        }
        user.manages_users = (flags & 1U) != 0U;
        user.verifier.iterations = reader.U32();
        const std::uint16_t salt_size = reader.U16();
        const auto* salt = reader.Take(salt_size);
        user.verifier.salt.assign(salt, salt + salt_size);
        std::copy_n(reader.Take(32), 32, user.verifier.stored_key.begin());
        std::copy_n(reader.Take(32), 32, user.verifier.server_key.begin());
        const std::uint32_t grants = reader.U32();
        for (std::uint32_t g = 0; g < grants; ++g) {
            std::string table = reader.ShortString();
            const std::uint8_t right = reader.U8();
            if (right < static_cast<std::uint8_t>(Right::kRead) || right > static_cast<std::uint8_t>(Right::kAdmin)) {
                throw std::invalid_argument("chunkdb.users has an unknown right for user " + name);
            }
            if (!user.grants.emplace(std::move(table), static_cast<Right>(right)).second) {
                throw std::invalid_argument("chunkdb.users repeats a grant of user " + name);
            }
        }
        if (!users.users.emplace(name, std::move(user)).second) {
            throw std::invalid_argument("chunkdb.users repeats user " + name);
        }
    }
    if (!reader.AtEnd()) {
        throw std::invalid_argument("chunkdb.users has bytes after its users");
    }
    return users;
}

std::optional<Users> ReadUsersFile(const std::filesystem::path& data_dir) {
    const auto path = data_dir / kUsersFileName;
    if (!std::filesystem::exists(path)) {
        return std::nullopt;
    }
    return DecodeUsers(LoadFile(path));
}

void WriteUsersFile(const std::filesystem::path& data_dir, const Users& users) {
    AtomicWrite(data_dir / kUsersFileName, EncodeUsers(users), /*fsync_file=*/true, /*fsync_directory=*/true);
}

}  // namespace chunkdb
