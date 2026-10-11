#pragma once

#include <filesystem>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace chunkdb {

// False only for an existing directory. A symlink or other object is an error.
[[nodiscard]] bool CreateOwnerOnlyDirectory(const std::filesystem::path& path);
// Restricts an existing regular file without following a symlink. Returns true
// if its permissions changed; throws if the restriction cannot be enforced.
[[nodiscard]] bool RestrictFileToOwner(const std::filesystem::path& path);

#ifdef _WIN32
// Effective user's SID, a protected DACL and no inherited grants. Lifetime
// covers the create call; both files and directories receive the owner SID.
class OwnerOnlySecurity {
  public:
    explicit OwnerOnlySecurity(const std::filesystem::path& path, bool directory = false);
    OwnerOnlySecurity(const OwnerOnlySecurity&) = delete;
    OwnerOnlySecurity& operator=(const OwnerOnlySecurity&) = delete;
    SECURITY_ATTRIBUTES* attributes() noexcept { return &attributes_; }

  private:
    std::vector<unsigned char> user_;
    std::vector<unsigned char> acl_;
    SECURITY_DESCRIPTOR descriptor_{};
    SECURITY_ATTRIBUTES attributes_{};
};
#endif

}  // namespace chunkdb
