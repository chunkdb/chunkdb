#pragma once

#include <filesystem>
#include <string>
#include <string_view>

namespace chunkdb {

// Gives `user` of the data directory a new password, offline: takes the
// writer lock, so it fails while a server has the directory open, and writes
// only the new verifier (docs/design/USERS_DESIGN.md). Throws std::invalid_argument
// for a directory without users, an unknown user or an empty password.
void ResetPassword(const std::filesystem::path& data_dir, const std::string& user, std::string_view password);

}  // namespace chunkdb
