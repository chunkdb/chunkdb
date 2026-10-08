#include "admin.hpp"

#include <stdexcept>

#include "process_lock.hpp"
#include "user_registry.hpp"

namespace chunkdb {

void ResetPassword(const std::filesystem::path& data_dir, const std::string& user, std::string_view password) {
    if (password.empty()) {
        throw std::invalid_argument("the new password is empty");
    }
    const auto lock = AcquireWriterLock(data_dir, AccessMode::kReadWrite, /*allow_multiple_processes=*/false);
    if (!ReadUsersFile(data_dir).has_value()) {
        throw std::invalid_argument("the data directory " + data_dir.string() + " has no users");
    }
    UserRegistry registry(data_dir, std::nullopt, {});
    registry.SetVerifier(user, scram::MakeVerifier(password, crypto::RandomBytes(16), scram::kMinIterations));
}

}  // namespace chunkdb
