#include "admin.hpp"

#include <stdexcept>

#include "process_lock.hpp"
#include "migrations_records.hpp"
#include "user_registry.hpp"

namespace chunkdb {

void ResetPassword(const std::filesystem::path& data_dir, const std::string& user, std::string_view password) {
    if (password.empty()) {
        throw std::invalid_argument("the new password is empty");
    }
    const auto no_users = [&data_dir] {
        return std::invalid_argument("the data directory " + data_dir.string() + " has no users");
    };
    // Checked before the lock too, so a wrong directory gets no lock file.
    if (!std::filesystem::exists(data_dir / kUsersFileName)) {
        throw no_users();
    }
    const auto lock = AcquireWriterLock(data_dir, AccessMode::kReadWrite, /*allow_multiple_processes=*/false);
    // Recovery must precede any verifier change: pending users images must
    // still match their prepared before/after states. The writer lock covers
    // both recovery and the password publication.
    RecoverMigrations(data_dir, AccessMode::kReadWrite);
    if (!ReadUsersFile(data_dir).has_value()) {
        throw no_users();
    }
    UserRegistry registry(data_dir, std::nullopt, {});
    registry.SetVerifier(user, scram::MakeVerifier(password, crypto::RandomBytes(16), scram::kMinIterations));
}

}  // namespace chunkdb
