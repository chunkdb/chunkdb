#include "file_permissions.hpp"

#include <cerrno>
#include <memory>
#include <stdexcept>
#include <system_error>

#ifdef _WIN32
#include <aclapi.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace chunkdb {
namespace {
#ifdef _WIN32
void CheckWindows(bool ok, const char* action) {
    if (!ok) throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), action);
}
struct HandleCloser {
    void operator()(void* handle) const noexcept { (void)CloseHandle(handle); }
};
using Handle = std::unique_ptr<void, HandleCloser>;
void RequirePersistentAcls(HANDLE handle) {
    DWORD flags = 0;
    CheckWindows(GetVolumeInformationByHandleW(handle, nullptr, 0, nullptr, nullptr,
        &flags, nullptr, 0) != 0, "inspect filesystem ACL support");
    if ((flags & FILE_PERSISTENT_ACLS) == 0U) {
        throw std::runtime_error("owner-only files require a filesystem with persistent ACLs");
    }
}
struct LocalFreeDeleter {
    void operator()(void* memory) const noexcept { (void)LocalFree(memory); }
};
std::vector<unsigned char> OwnerAcl(PSID owner, bool directory) {
    std::vector<unsigned char> bytes(sizeof(ACL) + sizeof(ACCESS_ALLOWED_ACE) - sizeof(DWORD) + GetLengthSid(owner));
    auto* acl = reinterpret_cast<PACL>(bytes.data());
    CheckWindows(InitializeAcl(acl, static_cast<DWORD>(bytes.size()), ACL_REVISION) != 0, "initialize owner ACL");
    CheckWindows(AddAccessAllowedAceEx(acl, ACL_REVISION,
        directory ? OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE : 0,
        FILE_ALL_ACCESS, owner) != 0, "grant owner access");
    return bytes;
}
#endif
}  // namespace

#ifdef _WIN32
OwnerOnlySecurity::OwnerOnlySecurity(const std::filesystem::path& path, bool directory) {
    // Inspect the resolved parent volume before creation: FAT ignores the
    // supplied security descriptor, and a post-create check would be too late.
    const auto parent = std::filesystem::absolute(path).parent_path();
    Handle parent_handle(CreateFileW(parent.c_str(), FILE_READ_ATTRIBUTES,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
        OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, nullptr));
    if (parent_handle.get() == INVALID_HANDLE_VALUE) {
        parent_handle.release();
        CheckWindows(false, "open owner-only parent directory");
    }
    RequirePersistentAcls(parent_handle.get());
    HANDLE token = nullptr;
    if (OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, TRUE, &token) == 0) {
        if (GetLastError() != ERROR_NO_TOKEN) CheckWindows(false, "open effective user token");
        CheckWindows(OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token) != 0, "open process token");
    }
    const Handle owned_token(token);
    DWORD size = 0;
    if (GetTokenInformation(token, TokenUser, nullptr, 0, &size) != 0 || GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
        throw std::runtime_error("cannot size effective user token");
    }
    user_.resize(size);
    CheckWindows(GetTokenInformation(token, TokenUser, user_.data(), size, &size) != 0, "read effective user token");
    const auto owner = reinterpret_cast<TOKEN_USER*>(user_.data())->User.Sid;
    acl_ = OwnerAcl(owner, directory);
    CheckWindows(InitializeSecurityDescriptor(&descriptor_, SECURITY_DESCRIPTOR_REVISION) != 0, "initialize owner descriptor");
    CheckWindows(SetSecurityDescriptorOwner(&descriptor_, owner, FALSE) != 0, "set file owner");
    CheckWindows(SetSecurityDescriptorDacl(&descriptor_, TRUE, reinterpret_cast<PACL>(acl_.data()), FALSE) != 0, "set owner ACL");
    CheckWindows(SetSecurityDescriptorControl(&descriptor_, SE_DACL_PROTECTED, SE_DACL_PROTECTED) != 0, "protect owner ACL");
    attributes_ = {sizeof(SECURITY_ATTRIBUTES), &descriptor_, FALSE};
}
#endif

bool CreateOwnerOnlyDirectory(const std::filesystem::path& path) {
#ifdef _WIN32
    OwnerOnlySecurity security(path, true);
    if (CreateDirectoryW(path.c_str(), security.attributes()) != 0) return true;
    const auto error = GetLastError();
    if (error != ERROR_ALREADY_EXISTS) {
        throw std::system_error(static_cast<int>(error), std::system_category(), "create owner-only directory: " + path.string());
    }
#else
    if (::mkdir(path.c_str(), 0700) == 0) return true;
    if (errno != EEXIST) throw std::system_error(errno, std::generic_category(), "create owner-only directory: " + path.string());
#endif
    if (!std::filesystem::is_directory(std::filesystem::symlink_status(path))) {
        throw std::runtime_error("expected directory path but found non-directory: " + path.string());
    }
    return false;
}

bool RestrictFileToOwner(const std::filesystem::path& path) {
#ifdef _WIN32
    Handle handle(CreateFileW(path.c_str(), READ_CONTROL | WRITE_DAC | FILE_READ_ATTRIBUTES,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
        FILE_FLAG_OPEN_REPARSE_POINT, nullptr));
    if (handle.get() == INVALID_HANDLE_VALUE) {
        handle.release();
        CheckWindows(false, "open file for owner-only ACL");
    }
    RequirePersistentAcls(handle.get());
    BY_HANDLE_FILE_INFORMATION information{};
    CheckWindows(GetFileInformationByHandle(handle.get(), &information) != 0, "inspect owner-only file");
    if ((information.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) != 0U) {
        throw std::runtime_error("owner-only path is not a regular file: " + path.string());
    }
    PSID owner = nullptr;
    PACL previous_acl = nullptr;
    PSECURITY_DESCRIPTOR raw_descriptor = nullptr;
    const auto error = GetSecurityInfo(handle.get(), SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
        &owner, nullptr, &previous_acl, nullptr, &raw_descriptor);
    if (error != ERROR_SUCCESS) throw std::system_error(static_cast<int>(error), std::system_category(), "read file ACL");
    const std::unique_ptr<void, LocalFreeDeleter> descriptor(raw_descriptor);
    SECURITY_DESCRIPTOR_CONTROL control = 0;
    DWORD revision = 0;
    CheckWindows(GetSecurityDescriptorControl(raw_descriptor, &control, &revision) != 0, "read ACL control");
    if (previous_acl != nullptr && previous_acl->AceCount == 1 && (control & SE_DACL_PROTECTED) != 0U) {
        void* raw_ace = nullptr;
        CheckWindows(GetAce(previous_acl, 0, &raw_ace) != 0, "read owner ACE");
        const auto* ace = static_cast<const ACCESS_ALLOWED_ACE*>(raw_ace);
        if (ace->Header.AceType == ACCESS_ALLOWED_ACE_TYPE && ace->Header.AceFlags == 0 &&
            ace->Mask == FILE_ALL_ACCESS && EqualSid(owner, const_cast<DWORD*>(&ace->SidStart)) != 0) return false;
    }
    auto acl = OwnerAcl(owner, false);
    const auto applied = SetSecurityInfo(handle.get(), SE_FILE_OBJECT,
        DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION, nullptr, nullptr,
        reinterpret_cast<PACL>(acl.data()), nullptr);
    if (applied != ERROR_SUCCESS) throw std::system_error(static_cast<int>(applied), std::system_category(), "restrict file ACL to owner");
    return true;
#else
    const int fd = ::open(path.c_str(), O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    if (fd < 0) throw std::system_error(errno, std::generic_category(), "open owner-only file: " + path.string());
    struct stat status {};
    const int inspected = ::fstat(fd, &status);
    if (inspected != 0 || !S_ISREG(status.st_mode)) {
        const int error = inspected != 0 ? errno : EINVAL;
        (void)::close(fd);
        throw std::system_error(error, std::generic_category(), "inspect owner-only file: " + path.string());
    }
    const bool changed = (status.st_mode & 07777) != 0600;
    const int result = changed ? ::fchmod(fd, 0600) : 0;
    const int error = errno;
    const int closed = ::close(fd);
    if (result != 0) throw std::system_error(error, std::generic_category(), "restrict file to owner: " + path.string());
    if (closed != 0) throw std::system_error(errno, std::generic_category(), "close owner-only file: " + path.string());
    return changed;
#endif
}

}  // namespace chunkdb
