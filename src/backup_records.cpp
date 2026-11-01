#include "backup.hpp"
#include "backup_windows_path.hpp"
#include "backup_directory.hpp"
#include "migrations_records.hpp"

#include <algorithm>
#include <array>
#include <fstream>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <cwchar>
#ifdef _WIN32
#include <windows.h>
#else
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#endif
#include <limits>
#include <map>
#include <set>
#include <system_error>

#include "checkpoint.hpp"
#include "file_permissions.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feature_flags.hpp"
#include "feed_slot_records.hpp"
#include "store_manifest.hpp"
#include "commit_owner_bootstrap.hpp"
#include "wal_replay.hpp"
#include "chunk_coordinate_index.hpp"
#include "shared_file_input.hpp"

namespace chunkdb {
namespace {
constexpr std::array<std::uint8_t, 4> kMagic{'C', 'K', 'B', 'P'};
void Cancelled(const BackupCancel& cancelled) {
    if (cancelled.stop_requested()) throw std::runtime_error("backup cancelled");
}
void Crash(const char* point) noexcept { if (ConsumeFailpointEnv(point)) std::_Exit(86); }
bool Present(const std::filesystem::path& path) {
    const auto status = std::filesystem::symlink_status(path);
    return status.type() != std::filesystem::file_type::not_found;
}
void RequirePath(const std::filesystem::path& path) {
    if (path.native().find(typename std::filesystem::path::value_type{}) != std::filesystem::path::string_type::npos)
        throw std::invalid_argument("backup path contains a NUL byte");
}
#ifdef _WIN32
std::filesystem::path AbsoluteWindowsBackupPath(const std::filesystem::path& path) {
    const auto required = GetFullPathNameW(path.c_str(), 0U, nullptr, nullptr);
    if (required == 0U)
        throw std::filesystem::filesystem_error("cannot make backup path absolute", path,
            std::error_code(static_cast<int>(GetLastError()), std::system_category()));
    if (required == std::numeric_limits<DWORD>::max()) throw std::length_error("backup path is too long");
    std::wstring native(static_cast<std::size_t>(required) + 1U, L'\0');
    const auto length = GetFullPathNameW(path.c_str(), static_cast<DWORD>(native.size()), native.data(), nullptr);
    if (length == 0U)
        throw std::filesystem::filesystem_error("cannot make backup path absolute", path,
            std::error_code(static_cast<int>(GetLastError()), std::system_category()));
    if (length >= native.size()) throw std::invalid_argument("backup path changed while making it absolute");
    native.resize(length);
    return std::filesystem::path(BackupWindowsApiName(SplitBackupWindowsPath(native).Name(),
        path.native().starts_with(L"\\\\?\\")));
}

bool WindowsBackupDirectoryEmpty(const std::filesystem::path& path) {
    auto pattern = path.native();
    if (pattern.empty() || (pattern.back() != L'\\' && pattern.back() != L'/')) pattern += L'\\';
    pattern += L'*';
    WIN32_FIND_DATAW entry{};
    const auto search = FindFirstFileW(pattern.c_str(), &entry);
    if (search == INVALID_HANDLE_VALUE) {
        const auto error = GetLastError();
        if (error == ERROR_FILE_NOT_FOUND) return true;
        throw std::filesystem::filesystem_error("cannot list backup directory", path,
            std::error_code(static_cast<int>(error), std::system_category()));
    }
    struct FindCloseGuard {
        HANDLE handle;
        ~FindCloseGuard() { FindClose(handle); }
    } guard{search};
    for (;;) {
        if (std::wcscmp(entry.cFileName, L".") != 0 && std::wcscmp(entry.cFileName, L"..") != 0) return false;
        if (FindNextFileW(search, &entry) != 0) continue;
        const auto error = GetLastError();
        if (error == ERROR_NO_MORE_FILES) return true;
        throw std::filesystem::filesystem_error("cannot continue backup directory listing", path,
            std::error_code(static_cast<int>(error), std::system_category()));
    }
}
#endif
void SafeDescendants(const std::filesystem::path& root, const std::filesystem::path& relative) {
    auto at = CanonicalBackupPath(root);
    for (const auto& part : relative) {
        at /= part;
#ifdef _WIN32
        const auto attributes = GetFileAttributesW(at.c_str());
        if (attributes == INVALID_FILE_ATTRIBUTES) {
            const auto error = GetLastError();
            if (error != ERROR_FILE_NOT_FOUND && error != ERROR_PATH_NOT_FOUND)
                throw std::filesystem::filesystem_error("cannot inspect backup path", at,
                    std::error_code(static_cast<int>(error), std::system_category()));
        } else if ((attributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0U) {
            throw std::invalid_argument("backup target must not contain reparse points below --backup-dir: " + at.string());
        }
#else
        std::error_code error;
        const auto status = std::filesystem::symlink_status(at, error);
        if (error && error != std::errc::no_such_file_or_directory)
            throw std::filesystem::filesystem_error("cannot inspect backup path", at, error);
        if (status.type() == std::filesystem::file_type::symlink)
            throw std::invalid_argument("backup target must not contain symlinks below --backup-dir: " + at.string());
#endif
    }
}
bool Within(const std::filesystem::path& path, const std::filesystem::path& root) {
#ifdef _WIN32
    const auto compared_path = SplitBackupWindowsPath(path.native());
    const auto compared_root = SplitBackupWindowsPath(root.native());
    return compared_path.root == compared_root.root &&
        compared_root.components.size() <= compared_path.components.size() &&
        std::equal(compared_root.components.begin(), compared_root.components.end(), compared_path.components.begin());
#else
    const auto& compared_path = path;
    const auto& compared_root = root;
    auto p = compared_path.begin();
    for (auto r = compared_root.begin(); r != compared_root.end(); ++r, ++p)
        if (p == compared_path.end() || *p != *r) return false;
    return true;
#endif
}
void RequireRelative(const std::filesystem::path& path) {
    if (path.empty() || path.is_absolute() || path.has_root_path() || path != path.lexically_normal())
        throw std::runtime_error("invalid backup inventory path");
    for (const auto& part : path)
        if (part == "." || part == ".." || part.empty()) throw std::runtime_error("invalid backup inventory path");
    const auto spelling = path.generic_string();
    if (spelling.find('\\') != std::string::npos || spelling.find('\0') != std::string::npos ||
        spelling != path.lexically_normal().generic_string() ||
        (spelling.size() >= 2U && spelling[1] == ':' &&
            ((spelling[0] >= 'A' && spelling[0] <= 'Z') || (spelling[0] >= 'a' && spelling[0] <= 'z'))))
        throw std::runtime_error("invalid backup inventory path");
}
std::filesystem::path ParseInventoryPath(const std::string& spelling) {
    // Check the marker's spelling before Windows converts native separators.
    if (spelling.find('\\') != std::string::npos || spelling.find('\0') != std::string::npos)
        throw std::runtime_error("invalid backup inventory path spelling");
    std::filesystem::path path(spelling);
    RequireRelative(path);
    if (path.generic_string() != spelling)
        throw std::runtime_error("invalid backup inventory path spelling");
    path.make_preferred();
    return path;
}
void ValidateRecord(const BackupRecord& record) {
    std::set<std::string> names;
    for (const auto& table : record.tables) {
        if (!IsValidTableName(table.name) || !names.insert(table.name).second ||
            std::all_of(table.epoch.begin(), table.epoch.end(), [](auto b) { return b == 0U; }))
            throw std::runtime_error("invalid or repeated backup table cut");
    }
    std::set<std::string> paths;
    for (const auto& file : record.files) {
        RequireRelative(file.relative_path);
        if (!paths.insert(file.relative_path.generic_string()).second || file.relative_path == kBackupMarkerName ||
            file.relative_path == kBackupIncompleteName || file.relative_path == kRestoreIncompleteName)
            throw std::runtime_error("invalid or repeated backup inventory file");
    }
}
void PutString(std::vector<std::uint8_t>& out, const std::string& value) {
    if (value.size() > std::numeric_limits<std::uint32_t>::max()) throw std::length_error("backup record string too long");
    WriteLe32(out, static_cast<std::uint32_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}
class Reader {
  public:
    Reader(const std::vector<std::uint8_t>& bytes, std::size_t end) : bytes_(bytes), end_(end) {}
    std::uint16_t U16() { Need(2U); auto v = ReadLe16(bytes_, at_); at_ += 2U; return v; }
    std::uint32_t U32() { Need(4U); auto v = ReadLe32(bytes_, at_); at_ += 4U; return v; }
    std::uint64_t U64() { Need(8U); auto v = ReadLe64(bytes_, at_); at_ += 8U; return v; }
    StoreId Id() { Need(16U); StoreId id{}; std::copy_n(bytes_.begin() + static_cast<std::ptrdiff_t>(at_), id.size(), id.begin()); at_ += id.size(); return id; }
    std::string String() { const auto n = U32(); Need(n); std::string s(bytes_.begin() + static_cast<std::ptrdiff_t>(at_), bytes_.begin() + static_cast<std::ptrdiff_t>(at_ + n)); at_ += n; return s; }
    std::size_t left() const { return end_ - at_; }
  private:
    void Need(std::size_t n) { if (n > left()) throw std::runtime_error("truncated backup record"); }
    const std::vector<std::uint8_t>& bytes_; std::size_t end_, at_ = 4U;
};
void RequireRegular(const std::filesystem::path& path) {
    if (std::filesystem::symlink_status(path).type() != std::filesystem::file_type::regular)
        throw std::runtime_error("backup inventory requires a regular file: " + path.string());
}
#ifndef _WIN32
void SyncBackupFd(int fd, bool file) {
#ifdef __APPLE__
    const auto failpoint = file ? "CHUNKDB_FAILPOINT_FULL_SYNC_FILE_FAIL_ONCE" :
        "CHUNKDB_FAILPOINT_FULL_SYNC_DIRECTORY_FAIL_ONCE";
    if (ConsumeFailpointEnv(failpoint)) throw std::runtime_error(file ?
        "backup F_FULLFSYNC file failed" : "backup F_FULLFSYNC directory failed");
    if (::fcntl(fd, F_FULLFSYNC, 0) == 0) return;
    const int error = errno;
    if (error != EINVAL && error != ENOTSUP && error != ENOTTY)
        throw std::runtime_error(file ? "backup F_FULLFSYNC file failed" : "backup F_FULLFSYNC directory failed");
#else
    (void)file;
#endif
    if (::fsync(fd) != 0) throw std::runtime_error("backup durability sync failed");
}
#endif
// Every component is opened without following links. POSIX writes use the
// held directory descriptor; Windows keeps all parents open without delete
// sharing so a checked component cannot be replaced before a child is opened.
class SafeDirectory {
  public:
    SafeDirectory() = default;
    SafeDirectory(const SafeDirectory&) = delete;
    SafeDirectory& operator=(const SafeDirectory&) = delete;
    ~SafeDirectory() {
#ifdef _WIN32
        for (const auto handle : handles_) CloseHandle(handle);
#else
        if (fd_ >= 0) ::close(fd_);
#endif
    }
    bool Open(const std::filesystem::path& path, bool create, bool missing_ok = false) {
#ifdef _WIN32
        path_ = AbsoluteWindowsBackupPath(path);
        const auto components = SplitBackupWindowsPath(path_.native());
        auto at = components.root;
        const auto api_name = [&](const auto& component) {
            return std::filesystem::path(BackupWindowsApiName(component, path_.native().starts_with(L"\\\\?\\")));
        };
        OpenComponent(api_name(at));
        for (const auto& part : components.components) {
            if (at.back() != L'\\') at += L'\\';
            at += part;
            const auto api = api_name(at);
            if (create) (void)CreateOwnerOnlyDirectory(api);
            if (!OpenComponent(api, missing_ok)) return false;
        }
#else
        (void)missing_ok;
        path_ = std::filesystem::absolute(path).lexically_normal();
        fd_ = ::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd_ < 0) throw std::runtime_error("cannot open backup path root");
        for (const auto& part : path_.relative_path()) {
            const auto name = part.string();
            bool created = false;
            if (create) {
                const int result = ::mkdirat(fd_, name.c_str(), 0700);
                if (result != 0 && errno != EEXIST) throw std::runtime_error("cannot create safe backup directory");
                created = result == 0;
                if (created) SyncBackupFd(fd_, false);
            }
            const int next = ::openat(fd_, name.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
            if (next < 0) {
                if (created) (void)::unlinkat(fd_, name.c_str(), AT_REMOVEDIR);
                throw std::invalid_argument("backup path component is not a safe directory: " + name);
            }
            if (created) {
                try {
                    RequireOwnerOnlyDescriptor(next, path_, true);
                } catch (...) {
                    ::close(next);
                    (void)::unlinkat(fd_, name.c_str(), AT_REMOVEDIR);
                    throw;
                }
            }
            ::close(fd_); fd_ = next;
        }
#endif
        return true;
    }
    bool Empty() const {
#ifdef _WIN32
        return WindowsBackupDirectoryEmpty(path_);
#else
        const int copy = ::dup(fd_);
        if (copy < 0) throw std::runtime_error("cannot inspect backup directory");
        DIR* entries = ::fdopendir(copy);
        if (!entries) { ::close(copy); throw std::runtime_error("cannot inspect backup directory"); }
        bool empty = true;
        errno = 0;
        while (const auto* entry = ::readdir(entries)) {
            if (std::strcmp(entry->d_name, ".") != 0 && std::strcmp(entry->d_name, "..") != 0) { empty = false; break; }
        }
        const int error = errno;
        ::closedir(entries);
        if (error != 0) throw std::runtime_error("cannot read backup directory entries");
        return empty;
#endif
    }
    void Sync() const {
#ifdef _WIN32
        SyncDirectoryPath(path_);
#else
        SyncBackupFd(fd_, false);
#endif
    }
    void Replace(std::string_view from, std::string_view to) const {
#ifdef _WIN32
        if (!MoveFileExW((path_ / from).wstring().c_str(), (path_ / to).wstring().c_str(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
            throw std::runtime_error("cannot publish safe backup file");
#else
        if (::renameat(fd_, std::string(from).c_str(), fd_, std::string(to).c_str()) != 0)
            throw std::runtime_error("cannot publish safe backup file");
#endif
    }
    void Remove(std::string_view name) const {
#ifdef _WIN32
        if (!DeleteFileW((path_ / name).wstring().c_str())) throw std::runtime_error("cannot remove publication guard");
#else
        if (::unlinkat(fd_, std::string(name).c_str(), 0) != 0) throw std::runtime_error("cannot remove publication guard");
#endif
    }
    const std::filesystem::path& path() const { return path_; }
#ifdef _WIN32
#else
    int fd() const { return fd_; }
#endif
  private:
    std::filesystem::path path_;
#ifdef _WIN32
    bool OpenComponent(const std::filesystem::path& path, bool missing_ok = false) {
        const auto handle = CreateFileW(path.wstring().c_str(), FILE_READ_ATTRIBUTES | FILE_LIST_DIRECTORY,
            FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
        if (handle == INVALID_HANDLE_VALUE) {
            const auto error = GetLastError();
            if (missing_ok && (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND)) return false;
            throw std::invalid_argument("cannot open safe backup directory: " + path.string());
        }
        BY_HANDLE_FILE_INFORMATION information{};
        if (!GetFileInformationByHandle(handle, &information) ||
            (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0U ||
            (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0U) {
            CloseHandle(handle);
            throw std::invalid_argument("backup path component is not a safe directory: " + path.string());
        }
        handles_.push_back(handle);
        return true;
    }
    std::vector<HANDLE> handles_;
#else
    int fd_ = -1;
#endif
};
#ifdef _WIN32
// Deletion uses the opened link, never a path that can start following a
// replacement junction. Directory handles deny renames while children are open.
std::error_code RemoveWindowsStagingNode(const std::filesystem::path& path) {
    const auto attributes = GetFileAttributesW(path.c_str());
    if (attributes == INVALID_FILE_ATTRIBUTES) {
        const auto error = GetLastError();
        if (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND) return {};
        return {static_cast<int>(error), std::system_category()};
    }
    const bool pin_directory = (attributes & FILE_ATTRIBUTE_DIRECTORY) != 0U &&
        (attributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0U;
    const auto handle = CreateFileW(path.c_str(), DELETE | FILE_READ_ATTRIBUTES |
        (pin_directory ? FILE_LIST_DIRECTORY : 0U),
        FILE_SHARE_READ | FILE_SHARE_WRITE | (pin_directory ? 0U : FILE_SHARE_DELETE),
        nullptr, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        const auto error = GetLastError();
        if (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND) return {};
        return {static_cast<int>(error), std::system_category()};
    }
    struct Close {
        HANDLE handle;
        ~Close() { CloseHandle(handle); }
    } close{handle};
    BY_HANDLE_FILE_INFORMATION information{};
    if (!GetFileInformationByHandle(handle, &information))
        return {static_cast<int>(GetLastError()), std::system_category()};
    if ((information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0U &&
        (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0U) {
        // A file changed into a directory before open; it has not been pinned.
        if (!pin_directory) return std::make_error_code(std::errc::operation_not_permitted);
        WIN32_FIND_DATAW entry{};
        const auto search = FindFirstFileW((path / L"*").c_str(), &entry);
        if (search == INVALID_HANDLE_VALUE) {
            const auto error = GetLastError();
            if (error != ERROR_FILE_NOT_FOUND) return {static_cast<int>(error), std::system_category()};
        } else {
            struct FindCloseGuard {
                HANDLE handle;
                ~FindCloseGuard() { FindClose(handle); }
            } search_guard{search};
            for (;;) {
                const std::wstring_view name(entry.cFileName);
                if (name != L"." && name != L"..")
                    if (const auto error = RemoveWindowsStagingNode(path / name); error) return error;
                if (!FindNextFileW(search, &entry)) {
                    const auto error = GetLastError();
                    if (error != ERROR_NO_MORE_FILES) return {static_cast<int>(error), std::system_category()};
                    break;
                }
            }
        }
    }
    FILE_DISPOSITION_INFO disposition{};
    disposition.DeleteFile = TRUE;
    if (!SetFileInformationByHandle(handle, FileDispositionInfo, &disposition, sizeof(disposition)))
        return {static_cast<int>(GetLastError()), std::system_category()};
    return {};
}

std::error_code RemoveWindowsStaging(const std::filesystem::path& staging) {
    const auto absolute = AbsoluteWindowsBackupPath(staging);
    auto parts = SplitBackupWindowsPath(absolute.native());
    if (parts.components.empty()) return std::make_error_code(std::errc::operation_not_permitted);
    const auto leaf = parts.components.back();
    parts.components.pop_back();
    SafeDirectory parent;
    // Prove absence only beneath pinned non-reparse ancestors; a replacement
    // junction can otherwise hide the private tree and its incomplete guard.
    if (!parent.Open(std::filesystem::path(BackupWindowsApiName(parts.Name(), absolute.native().starts_with(L"\\\\?\\"))),
            false, true)) return {};
    return RemoveWindowsStagingNode(parent.path() / leaf);
}
#endif
class ExclusiveOutput {
  public:
    ExclusiveOutput(const SafeDirectory& directory, const std::filesystem::path& name) {
#ifdef _WIN32
        OwnerOnlySecurity security(directory.path() / name);
        handle_ = CreateFileW((directory.path() / name).wstring().c_str(), GENERIC_WRITE, 0, security.attributes(), CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
        if (handle_ == INVALID_HANDLE_VALUE) throw std::runtime_error("cannot exclusively create backup file");
#else
        fd_ = ::openat(directory.fd(), name.string().c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
        if (fd_ < 0) throw std::runtime_error("cannot exclusively create safe backup file");
        try {
            RequireOwnerOnlyDescriptor(fd_, directory.path() / name);
        } catch (...) {
            ::close(fd_); fd_ = -1;
            (void)::unlinkat(directory.fd(), name.string().c_str(), 0);
            throw;
        }
#endif
    }
    ~ExclusiveOutput() {
#ifdef _WIN32
        if (handle_ != INVALID_HANDLE_VALUE) CloseHandle(handle_);
#else
        if (fd_ >= 0) ::close(fd_);
#endif
    }
    void Write(const std::uint8_t* bytes, std::size_t count) {
        while (count != 0U) {
#ifdef _WIN32
            DWORD written = 0U;
            if (!WriteFile(handle_, bytes, static_cast<DWORD>(count), &written, nullptr) || written == 0U)
                throw std::runtime_error("backup copy write failed");
#else
            const auto written = ::write(fd_, bytes, count);
            if (written < 0 && errno == EINTR) continue;
            if (written <= 0) throw std::runtime_error("backup copy write failed");
#endif
            bytes += written; count -= static_cast<std::size_t>(written);
        }
    }
    void Finish() {
#ifdef _WIN32
        if (!FlushFileBuffers(handle_)) throw std::runtime_error("backup copy sync failed");
        const auto handle = handle_; handle_ = INVALID_HANDLE_VALUE;
        if (!CloseHandle(handle)) throw std::runtime_error("backup copy close failed");
#else
        SyncBackupFd(fd_, true);
        const auto fd = fd_; fd_ = -1;
        if (::close(fd) != 0) throw std::runtime_error("backup copy close failed");
#endif
    }
  private:
#ifdef _WIN32
    HANDLE handle_ = INVALID_HANDLE_VALUE;
#else
    int fd_ = -1;
#endif
};
void PublishSafeFile(SafeDirectory& directory, const std::filesystem::path& name, const std::vector<std::uint8_t>& bytes) {
    ExclusiveOutput output(directory, name);
    output.Write(bytes.data(), bytes.size());
    output.Finish();
    directory.Sync();
}
void ReplaceSafeFile(SafeDirectory& directory, std::string_view name, const std::vector<std::uint8_t>& bytes) {
    const auto temporary = std::string(name) + ".tmp." + StoreIdHex(NewStoreId());
    PublishSafeFile(directory, temporary, bytes);
    directory.Replace(temporary, name);
    directory.Sync();
}
// Staging links can outlive their live name. Windows must let checkpoints
// replace or remove that name while a backup is copying the linked inode.
class SharedInput {
  public:
    explicit SharedInput(const std::filesystem::path& path) {
#ifdef _WIN32
        handle_ = CreateFileW(path.wstring().c_str(), GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
        if (handle_ == INVALID_HANDLE_VALUE) throw std::runtime_error("cannot open backup source");
#else
        input_.open(path, std::ios::binary);
        if (!input_) throw std::runtime_error("cannot open backup source");
#endif
    }
    ~SharedInput() {
#ifdef _WIN32
        if (handle_ != INVALID_HANDLE_VALUE) CloseHandle(handle_);
#endif
    }
    std::size_t Read(std::uint8_t* bytes, std::size_t count) {
#ifdef _WIN32
        DWORD read = 0U;
        if (!ReadFile(handle_, bytes, static_cast<DWORD>(count), &read, nullptr))
            throw std::runtime_error("backup file read failed");
        return read;
#else
        input_.read(reinterpret_cast<char*>(bytes), static_cast<std::streamsize>(count));
        if (!input_ && !input_.eof()) throw std::runtime_error("backup file read failed");
        return static_cast<std::size_t>(input_.gcount());
#endif
    }
  private:
#ifdef _WIN32
    HANDLE handle_ = INVALID_HANDLE_VALUE;
#else
    std::ifstream input_;
#endif
};
void ValidateInventory(const std::filesystem::path& root, const BackupRecord& record, bool completing, const BackupCancel& cancelled = {}) {
    ValidateRecord(record);
    RequirePath(root);
    if (!std::filesystem::is_directory(root)) throw std::runtime_error("backup directory is missing");
    if (!completing && (Present(root / kRestoreIncompleteName) || Present(root / kBackupIncompleteName)))
        throw std::runtime_error("backup publication is incomplete");
    std::set<std::string> expected_files, expected_dirs{"tables"};
    for (const auto& table : record.tables) expected_dirs.insert("tables/" + table.name);
    for (const auto& file : record.files) {
        const auto relative = file.relative_path.generic_string();
        expected_files.insert(relative);
        auto parent = file.relative_path.parent_path();
        while (!parent.empty()) { expected_dirs.insert(parent.generic_string()); parent = parent.parent_path(); }
        const auto actual = InspectBackupFile(root, file.relative_path, cancelled);
        if (actual.size != file.size || actual.crc32 != file.crc32)
            throw std::runtime_error("backup inventory checksum or length mismatch: " + relative);
    }
    backup_detail::WalkDirectory(root, [&](const auto& entry) {
#ifdef _WIN32
        const auto relative_path = root.native().starts_with(L"\\\\?\\")
            ? std::filesystem::path(BackupWindowsRelativeName(entry.path().native(), root.native()))
            : entry.path().lexically_relative(root);
#else
        const auto relative_path = entry.path().lexically_relative(root);
#endif
        const auto relative = relative_path.generic_string();
        const auto type = entry.symlink_status().type();
        if (type == std::filesystem::file_type::directory) {
            if (!expected_dirs.erase(relative)) throw std::runtime_error("unexpected backup directory: " + relative);
        } else if (type == std::filesystem::file_type::regular) {
            if (relative == kBackupMarkerName || (completing && (relative == kBackupIncompleteName || relative == kRestoreIncompleteName))) return;
            if (!expected_files.erase(relative)) throw std::runtime_error("unexpected backup file: " + relative);
        } else throw std::runtime_error("unsafe backup entry: " + relative);
    });
    if (!expected_files.empty() || !expected_dirs.empty()) throw std::runtime_error("backup inventory entry is missing");
    if (!ReadDataDirManifest(root)) throw std::runtime_error("backup data-directory manifest is missing");
    const auto directory_manifest = *ReadDataDirManifest(root);
    RequireOpenableFeatures(directory_manifest.features, AccessMode::kReadOnly);
    if (Present(root / kUsersFileName)) (void)DecodeUsers(LoadFile(root / kUsersFileName));
    (void)ReadMigrationRecords(root);
    std::set<std::string> allowed{"chunkdb.manifest", "chunkdb.users", std::string(kMigrationsFileName)};
    if (directory_manifest.commit_owner) {
        (void)CommitOwnerBootstrap::ReadInactiveRoot(root, *directory_manifest.commit_owner);
        allowed.insert("chunkdb.groups.owner");
    } else if (Present(root / "chunkdb.groups.owner")) throw std::runtime_error("unclaimed canonical commit owner exists");
    std::set<StoreId> epochs;
    for (const auto& cut : record.tables) {
        Cancelled(cancelled);
        const auto dir = root / "tables" / cut.name;
        const auto manifest = ReadStoreManifest(dir);
        if (!manifest || manifest->store_id != cut.epoch) throw std::runtime_error("backup table epoch mismatch: " + cut.name);
        if (manifest->commit_owner != directory_manifest.commit_owner) throw std::runtime_error("backup table owner scope mismatch");
        if (!epochs.insert(manifest->store_id).second) throw std::runtime_error("backup contains duplicate table epochs");
        RequireOpenableFeatures(manifest->features, AccessMode::kReadOnly);
        Geometry geometry(manifest->geometry, manifest->schema, *manifest->chunk_schema);
        std::uint64_t ceiling = 0U, generation = 0U;
        if (!TryParseVersionClockRecord(LoadFile(dir / "chunkdb.version"), &ceiling) || ceiling <= cut.revision)
            throw std::runtime_error("backup clock is not above its cut");
        if (!TryParseSnapshotGenerationRecord(LoadFile(dir / "chunkdb.snapshot"), &generation) || (generation & 1U) != 0U)
            throw std::runtime_error("backup snapshot generation is not stable");
        if (!IsValidInitializedStoreMarker(LoadFile(dir / ".chunkdb.initialized"))) throw std::runtime_error("invalid backup initialized marker");
        if (auto slots = ReadFeedSlotRecords(dir, cut.epoch, geometry.dims())) {
            if ((manifest->features.incompat & kFeatureFeedSlots) == 0U || slots->durable_watermark > cut.revision)
                throw std::runtime_error("backup slot frontier exceeds cut or has no feature");
            if (slots->filter_format && (manifest->features.incompat & kFeatureSlotFilters) == 0U)
                throw std::runtime_error("backup slot filters have no storage feature");
            for (const auto& slot : slots->slots) if (slot.written > cut.revision) throw std::runtime_error("backup slot exceeds cut");
        }
        const auto prefix = "tables/" + cut.name + "/";
        for (const auto* name : {"table.manifest", "chunkdb.version", "chunkdb.snapshot", ".chunkdb.initialized", "chunkdb.slots", "chunkdb.index"})
            allowed.insert(prefix + name);
        ChunkCoordinateIndex retained(dir, geometry, cut.epoch, true);
        if (retained.maximum_recovered_revision() > cut.revision || retained.retained_deletion_floor() > cut.revision)
            throw std::runtime_error("backup retained identity exceeds cut");
        SharedFileInput inventory(dir / kChunkIndexFileName);
        if (!inventory) throw std::runtime_error("backup coordinate inventory is missing");
        const auto index_header = ReadIndexInventory(inventory, std::filesystem::file_size(dir / kChunkIndexFileName),
            geometry, cut.epoch, [&](const IndexInventoryEntry& entry) {
                const auto local = LargeChunkDirectory({}, entry.coord, geometry.dims()) / kChunkIndexFileName;
                if (std::filesystem::file_size(dir / local) != entry.committed_bytes)
                    throw std::runtime_error("backup retained log contains an unpublished suffix");
                allowed.insert(prefix + local.generic_string());
            });
        if (index_header.compaction_pending) throw std::runtime_error("backup contains pending coordinate compaction");
        for (const auto& file : record.files) {
            Cancelled(cancelled);
            const auto relative = file.relative_path.generic_string();
            if (relative.rfind(prefix, 0U) != 0U || allowed.contains(relative)) continue;
            const auto local = file.relative_path.lexically_relative(std::filesystem::path("tables") / cut.name);
            if (std::distance(local.begin(), local.end()) != 2) throw std::runtime_error("unknown backup table artifact: " + relative);
            const auto stem = local.stem().string();
            ChunkCoord coord;
            if (!ParseChunkFilePath(local, geometry, &coord)) throw std::runtime_error("noncanonical backup chunk path");
            const auto full = root / file.relative_path;
            if (local.extension() == ".chk") {
                auto image = ParseChunkImage(LoadChunkImageFile(full, geometry, coord, cut.epoch, manifest->features),
                    geometry, coord, cut.epoch, manifest->features);
                if (image.revision > cut.revision) throw std::runtime_error("backup image exceeds cut");
                const auto boundary = retained.EmptyBoundary(coord);
                if (boundary)
                    (void)ApplyRetainedEmptyBase(geometry, *boundary, image.revision, image.commit_time_ms,
                        image.schema_version, image.payload, image.presence_bitmap, image.vars, image.chunk_columns);
            } else if (local.extension() == ".wal") {
                std::vector<std::uint8_t> payload(geometry.ChunkPayloadBytes(), 0U), presence(ChunkPresenceBitmapBytes(geometry), 0U);
                ChunkVars vars; std::uint64_t base = 0U, schema = 0U, time = 0U;
                std::shared_ptr<const ChunkColumnState> chunk_columns;
                const auto image_path = full.parent_path() / (stem + ".chk");
                if (Present(image_path)) {
                    auto image = ParseChunkImage(LoadChunkImageFile(image_path, geometry, coord, cut.epoch, manifest->features),
                        geometry, coord, cut.epoch, manifest->features);
                    base = image.revision; schema = image.schema_version; time = image.commit_time_ms;
                    payload = std::move(image.payload); presence = std::move(image.presence_bitmap); vars = std::move(image.vars);
                    chunk_columns = std::move(image.chunk_columns);
                }
                const auto boundary = retained.EmptyBoundary(coord);
                if (boundary)
                    (void)ApplyRetainedEmptyBase(geometry, *boundary, base, time, schema,
                        payload, presence, vars, chunk_columns);
                std::vector<WalFrameBoundary> boundaries;
                auto replay = ReplayWal(LoadFile(full), geometry, coord, cut.epoch, manifest->features, base, schema,
                    &payload, &presence, &vars, &boundaries, &chunk_columns, time,
                    boundary ? std::optional{WalKnownIdentity{boundary->revision, boundary->commit_time_ms}} : std::nullopt);
                if ((!replay.replayable && !replay.torn_creation) ||
                    (replay.tail_truncated_or_corrupt && !replay.stopped_at_crash_tail) || !replay.vars_problem.empty() ||
                    (!boundaries.empty() && boundaries.back().revision > cut.revision)) throw std::runtime_error("backup WAL is damaged or exceeds cut");
            } else throw std::runtime_error("unknown backup chunk artifact: " + relative);
            allowed.insert(relative);
        }
    }
    for (const auto& file : record.files)
        if (!allowed.contains(file.relative_path.generic_string())) throw std::runtime_error("unknown backup inventory artifact");
}
void SyncTreeImpl(const std::filesystem::path& root, const BackupCancel& cancelled) {
#ifdef _WIN32
    const auto absolute = AbsoluteWindowsBackupPath(root);
#else
    const auto absolute = std::filesystem::absolute(root).lexically_normal();
#endif
    std::vector<std::filesystem::path> dirs{absolute};
    backup_detail::WalkDirectory(absolute, [&](const auto& entry) {
        Cancelled(cancelled);
        if (entry.is_directory()) dirs.push_back(entry.path());
        else { RequireRegular(entry.path()); SyncFilePath(entry.path()); }
    });
    for (auto it = dirs.rbegin(); it != dirs.rend(); ++it) { Cancelled(cancelled); SyncDirectoryPath(*it); }
    SyncDirectoryPath(absolute.parent_path());
}
} // namespace

namespace {
bool HexNonce(std::string_view value) {
    return value.size() == 32U && std::all_of(value.begin(), value.end(), [](char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    });
}
std::string_view StagingNonce(const std::string& name, const StoreId& data_dir_id) {
    if (name.size() == 65U && name[32] == '.' && name.substr(0U, 32U) == StoreIdHex(data_dir_id) &&
        HexNonce(std::string_view(name).substr(33U))) return std::string_view(name).substr(33U);
    return {};
}
}
void WriteBackupStagingOwner(const std::filesystem::path& staging, const StoreId& data_dir_id) {
    const auto name = staging.filename().string();
    const auto nonce = StagingNonce(name, data_dir_id);
    if (nonce.empty() || std::all_of(data_dir_id.begin(), data_dir_id.end(), [](auto b) { return b == 0U; }))
        throw std::invalid_argument("invalid backup staging identity");
    std::vector<std::uint8_t> bytes{'C', 'K', 'B', 'S'};
    bytes.insert(bytes.end(), data_dir_id.begin(), data_dir_id.end());
    bytes.insert(bytes.end(), nonce.begin(), nonce.end());
    WriteLe32(bytes, Crc32(bytes));
    SafeDirectory directory;
    directory.Open(staging, false);
    PublishSafeFile(directory, kBackupStagingOwnerName, bytes);
}

bool IsOwnedBackupStaging(const std::filesystem::path& staging, const StoreId& data_dir_id) {
    const auto name = staging.filename().string();
    const auto nonce = StagingNonce(name, data_dir_id);
    if (nonce.empty() || std::all_of(data_dir_id.begin(), data_dir_id.end(), [](auto b) { return b == 0U; })) return false;
#ifdef _WIN32
    const auto attributes = GetFileAttributesW(staging.c_str());
    if (attributes == INVALID_FILE_ATTRIBUTES) {
        const auto error = GetLastError();
        if (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND) return false;
        throw std::filesystem::filesystem_error("cannot inspect backup staging", staging,
            std::error_code(static_cast<int>(error), std::system_category()));
    }
    if ((attributes & FILE_ATTRIBUTE_DIRECTORY) == 0U || (attributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0U) return false;
#else
    std::error_code error;
    const auto directory = std::filesystem::symlink_status(staging, error);
    if (error && error != std::errc::no_such_file_or_directory)
        throw std::filesystem::filesystem_error("cannot inspect backup staging", staging, error);
    if (directory.type() != std::filesystem::file_type::directory) return false;
#endif
    // Creation itself records ownership, including a crash before the guard.
    return true;
}

std::error_code RemoveBackupTemporaryTree(const std::filesystem::path& root) {
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_BACKUP_TEMPORARY_CLEANUP_INVALID_PATH_ONCE"))
            throw std::invalid_argument("injected backup temporary cleanup path refusal");
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_BACKUP_TEMPORARY_CLEANUP_OPEN_FAIL_ONCE"))
            throw std::filesystem::filesystem_error("injected backup temporary cleanup open failure", root,
                std::make_error_code(std::errc::permission_denied));
#ifdef _WIN32
        return RemoveWindowsStaging(root);
#else
        std::error_code error;
        std::filesystem::remove_all(root, error);
        return error;
#endif
    } catch (const std::filesystem::filesystem_error& failure) {
        return failure.code();
    } catch (const std::invalid_argument&) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
}

void RemoveBackupStaging(const std::filesystem::path& staging) noexcept {
    std::error_code error;
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_BACKUP_STAGING_CLEANUP_FAIL_ONCE"))
        error = std::make_error_code(std::errc::permission_denied);
    else {
#ifdef _WIN32
        try { error = RemoveWindowsStaging(staging); }
        catch (const std::filesystem::filesystem_error& failure) { error = failure.code(); }
        catch (const std::invalid_argument&) { error = std::make_error_code(std::errc::operation_not_permitted); }
#else
        std::filesystem::remove_all(staging, error);
#endif
    }
    if (!error) return;
    // Logging is best effort: this also runs from a destructor, where an
    // escaping exception (e.g. bad_alloc while formatting) would terminate.
    try {
        LogMessage(LogLevel::kWarn, LogComponent::kStore, "backup staging cleanup failed",
            {{"path", staging.string()}, {"error", error.message()}});
    } catch (...) {
    }
}

void RemoveOrphanedBackupStaging(const std::filesystem::path& data_dir, const StoreId& data_dir_id) {
    auto root = data_dir / kBackupStagingName;
    const auto warn = [](std::string_view message, const std::filesystem::path& path, const std::error_code& error) {
        LogMessage(LogLevel::kWarn, LogComponent::kRecovery, message, {{"path", path.string()}, {"error", error.message()}});
    };
#ifdef _WIN32
    // Resolve the configured alias before admission, then keep every parent
    // pinned through listing, ownership checks and removal of its children.
    try { root = CanonicalBackupPath(root); }
    catch (const std::filesystem::filesystem_error& failure) { warn("cannot resolve backup staging", root, failure.code()); return; }
    catch (const std::runtime_error& failure) { warn(failure.what(), root, std::make_error_code(std::errc::io_error)); return; }
#endif
    std::error_code error;
    // Follow a configured root alias; the root itself is never removed.
    const auto status = std::filesystem::status(root, error);
    if (status.type() == std::filesystem::file_type::not_found || (!error && !std::filesystem::is_directory(status))) return;
    if (error) { warn("cannot inspect backup staging", root, error); return; }
#ifdef _WIN32
    SafeDirectory pinned;
    try { pinned.Open(root, false); }
    catch (const std::filesystem::filesystem_error& failure) { warn("cannot pin backup staging", root, failure.code()); return; }
    catch (const std::invalid_argument&) { warn("cannot pin backup staging", root, std::make_error_code(std::errc::operation_not_permitted)); return; }
#endif
    std::vector<std::filesystem::path> entries;
#ifdef _WIN32
    try {
        for (const auto& entry : backup_detail::DirectoryEntries(root)) entries.push_back(entry.path());
    } catch (const std::filesystem::filesystem_error& failure) {
        warn("cannot list backup staging", root, failure.code()); return;
    }
#else
    std::filesystem::directory_iterator it(root, error);
    for (const std::filesystem::directory_iterator end; !error && it != end; it.increment(error)) entries.push_back(it->path());
    if (error) { warn("cannot list backup staging", root, error); return; }
#endif
    for (const auto& entry : entries) {
        bool owned = false;
        try { owned = IsOwnedBackupStaging(entry, data_dir_id); }
        catch (const std::filesystem::filesystem_error& failure) { warn("cannot inspect backup staging", entry, failure.code()); continue; }
        if (!owned) {
            LogMessage(LogLevel::kWarn, LogComponent::kRecovery, "unowned backup staging entry retained", {{"path", entry.string()}});
            continue;
        }
        // Staging never holds a backup marker or guard, so such an entry is a
        // published or unfinished destination. One that cannot be inspected is kept too.
        const auto holds = [&](std::string_view name) {
            std::error_code inspect;
            return std::filesystem::symlink_status(entry / name, inspect).type() != std::filesystem::file_type::not_found;
        };
        if (holds(kBackupMarkerName) || holds(kBackupIncompleteName) || holds(kRestoreIncompleteName)) {
            LogMessage(LogLevel::kWarn, LogComponent::kRecovery, "backup destination in backup staging retained", {{"path", entry.string()}});
            continue;
        }
        LogMessage(LogLevel::kInfo, LogComponent::kRecovery, "removing interrupted backup staging", {{"path", entry.string()}});
        RemoveBackupStaging(entry);
    }
}

std::filesystem::path CanonicalBackupPath(const std::filesystem::path& path) {
    RequirePath(path);
#ifdef _WIN32
    auto ancestor = AbsoluteWindowsBackupPath(path);
    const bool explicit_namespace = path.native().starts_with(L"\\\\?\\");
    std::vector<std::wstring> suffix;
    for (;;) {
        const auto handle = CreateFileW(ancestor.c_str(), FILE_READ_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS, nullptr);
        if (handle == INVALID_HANDLE_VALUE) {
            const auto error = GetLastError();
            auto parts = SplitBackupWindowsPath(ancestor.native());
            if ((error != ERROR_FILE_NOT_FOUND && error != ERROR_PATH_NOT_FOUND) || parts.components.empty())
                throw std::filesystem::filesystem_error("cannot resolve backup path", ancestor,
                    std::error_code(static_cast<int>(error), std::system_category()));
            suffix.push_back(parts.components.back());
            parts.components.pop_back();
            ancestor = std::filesystem::path(BackupWindowsApiName(parts.Name(), explicit_namespace));
            continue;
        }
        struct Close {
            HANDLE handle;
            ~Close() { CloseHandle(handle); }
        } close{handle};
        const auto required = GetFinalPathNameByHandleW(handle, nullptr, 0U, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS);
        if (required == 0U)
            throw std::filesystem::filesystem_error("cannot resolve backup path", ancestor,
                std::error_code(static_cast<int>(GetLastError()), std::system_category()));
        if (required == std::numeric_limits<DWORD>::max()) throw std::length_error("resolved backup path is too long");
        std::wstring native(static_cast<std::size_t>(required) + 1U, L'\0');
        const auto length = GetFinalPathNameByHandleW(handle, native.data(), static_cast<DWORD>(native.size()),
            FILE_NAME_NORMALIZED | VOLUME_NAME_DOS);
        if (length == 0U)
            throw std::filesystem::filesystem_error("cannot resolve backup path", ancestor,
                std::error_code(static_cast<int>(GetLastError()), std::system_category()));
        if (length >= native.size()) throw std::runtime_error("backup path changed while resolving its name");
        native.resize(length);
        auto resolved = SplitBackupWindowsPath(native);
        for (auto part = suffix.rbegin(); part != suffix.rend(); ++part) resolved.components.push_back(*part);
        return std::filesystem::path(BackupWindowsApiName(resolved.Name(), explicit_namespace));
    }
#else
    return std::filesystem::weakly_canonical(path);
#endif
}

std::filesystem::path ResolveBackupTarget(const std::filesystem::path& directory, const std::filesystem::path& requested,
    BackupTestHook* hook) {
    if (directory.empty()) throw std::invalid_argument("BACKUP requires --backup-dir; set --backup-dir to a backup directory");
    RequirePath(directory); RequirePath(requested);
    if (requested.empty() || requested.is_absolute() || requested.has_root_path() ||
        requested.native().find(static_cast<std::filesystem::path::value_type>('\\')) != std::filesystem::path::string_type::npos)
        throw std::invalid_argument("BACKUP TO requires a relative name under --backup-dir");
    for (const auto& part : requested)
        if (part == "..") throw std::invalid_argument("BACKUP TO must not contain '..' components");
    const auto relative = requested.lexically_normal();
    if (relative == ".") throw std::invalid_argument("BACKUP TO requires a backup name");
    const auto root = CanonicalBackupPath(directory);
    SafeDescendants(root, relative);
    if (hook) hook->Run(BackupTestHook::Point::kAfterTargetValidation, {}, 0U);
    // Keep descendant names for the later no-follow pin; resolving again
    // could erase a junction inserted after SafeDescendants inspected it.
#ifdef _WIN32
    const auto target = AbsoluteWindowsBackupPath(root / relative);
#else
    const auto target = (root / relative).lexically_normal();
#endif
    if (!Within(target, root) || target == root) throw std::invalid_argument("backup destination escapes --backup-dir");
    return target;
}
void RequireBackupTarget(const std::filesystem::path& source, const std::filesystem::path& target) {
    if (target.empty()) throw std::invalid_argument("backup target is empty");
    RequirePath(source); RequirePath(target);
    const auto source_path = CanonicalBackupPath(source), target_path = CanonicalBackupPath(target);
    if (Within(source_path, target_path) || Within(target_path, source_path)) throw std::invalid_argument("backup and source directories overlap");
    if (Present(target) && (!std::filesystem::is_directory(target) ||
#ifdef _WIN32
            !WindowsBackupDirectoryEmpty(AbsoluteWindowsBackupPath(target))))
#else
            !std::filesystem::is_empty(target)))
#endif
        throw std::invalid_argument("backup destination must be absent or empty");
}
void PrepareBackupTarget(const std::filesystem::path& source, const std::filesystem::path& target, BackupTestHook* hook) {
    RequireBackupTarget(source, target);
    if (hook) hook->Run(BackupTestHook::Point::kBeforeTargetCreate, {}, 0U);
    SafeDirectory directory;
    directory.Open(target, true);
    if (!directory.Empty()) throw std::invalid_argument("backup destination must be absent or empty");
    PublishSafeFile(directory, kBackupIncompleteName, {'C', 'K', 'B', 'I'});
}
void EnsureBackupDirectory(const std::filesystem::path& path) {
    RequirePath(path); SafeDirectory directory; directory.Open(path, true); directory.Sync();
}
void RequireNotBackupDirectory(const std::filesystem::path& path, AccessMode access_mode) {
    auto check = [access_mode](const auto& dir) {
        if (Present(dir / kBackupIncompleteName) || Present(dir / kRestoreIncompleteName))
            throw std::runtime_error("backup or incomplete restore directory cannot be opened; use chunkdb_restore");
        if (!Present(dir / kBackupMarkerName)) return;
        // Read-only opening writes nothing, so a completed backup stays one.
        if (access_mode == AccessMode::kReadOnly &&
            std::filesystem::symlink_status(dir / kBackupMarkerName).type() == std::filesystem::file_type::regular)
            return;
        throw std::runtime_error(
            "backup or incomplete restore directory cannot be opened; use chunkdb_restore, or open a completed "
            "backup read-only");
    };
#ifdef _WIN32
    const auto absolute = AbsoluteWindowsBackupPath(path);
#else
    const auto absolute = std::filesystem::absolute(path).lexically_normal();
#endif
    check(absolute);
    if (absolute.parent_path().filename() == "tables") check(absolute.parent_path().parent_path());
}
BackupFileRecord InspectBackupFile(const std::filesystem::path& root, const std::filesystem::path& relative, const BackupCancel& cancelled) {
    RequirePath(root); RequireRelative(relative); SafeDescendants(root, relative); RequireRegular(root / relative);
    SharedInput input(root / relative);
    std::array<std::uint8_t, 65536U> buffer{}; std::uint64_t size = 0; std::uint32_t crc = 0;
    for (;;) {
        Cancelled(cancelled);
        const auto count = input.Read(buffer.data(), buffer.size());
        if (count == 0U) break;
        if (count > std::numeric_limits<std::uint64_t>::max() - size) throw std::length_error("backup file too large");
        size += count; crc = Crc32Extend(crc, buffer.data(), count);
    }
    return {relative, size, crc};
}
BackupFileRecord CopyBackupFile(const std::filesystem::path& source, const std::filesystem::path& root,
    const std::filesystem::path& relative, std::uint64_t size, const BackupCancel& cancelled, BackupTestHook* hook) {
    RequirePath(source); RequirePath(root); RequireRelative(relative); RequireRegular(source); SafeDescendants(root, relative);
    if (hook) hook->Run(BackupTestHook::Point::kBeforeCopyCreate, {}, 0U);
#ifdef _WIN32
    const auto target = AbsoluteWindowsBackupPath(root) / relative;
#else
    const auto target = std::filesystem::absolute(root).lexically_normal() / relative;
#endif
    SafeDirectory directory;
    directory.Open(target.parent_path(), true);
    SharedInput input(source);
    ExclusiveOutput output(directory, target.filename());
    std::array<std::uint8_t, 65536U> buffer{}; auto remaining = size; std::uint32_t crc = 0;
    while (remaining != 0U) {
        Cancelled(cancelled); const auto count = static_cast<std::size_t>(std::min<std::uint64_t>(remaining, buffer.size()));
        std::size_t read = 0U;
        while (read < count) {
            const auto n = input.Read(buffer.data() + read, count - read);
            if (n == 0U) throw std::runtime_error("backup required prefix was shortened");
            read += n;
        }
        output.Write(buffer.data(), count);
        remaining -= count; crc = Crc32Extend(crc, buffer.data(), count);
    }
    output.Finish();
    directory.Sync();
    return {relative, size, crc};
}
std::vector<std::uint8_t> ReadBackupFile(const std::filesystem::path& source, std::uint64_t size, const BackupCancel& cancelled) {
    RequirePath(source); RequireRegular(source);
    if (size > std::numeric_limits<std::size_t>::max()) throw std::length_error("backup file too large");
    SharedInput input(source);
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(size));
    std::size_t at = 0U;
    while (at != bytes.size()) {
        Cancelled(cancelled);
        const auto count = std::min<std::size_t>(bytes.size() - at, 65536U);
        const auto read = input.Read(bytes.data() + at, count);
        if (read == 0U) throw std::runtime_error("backup required prefix was shortened");
        at += read;
    }
    return bytes;
}
std::vector<std::uint8_t> SerializeBackupRecord(const BackupRecord& record) {
    ValidateRecord(record);
    if (record.tables.size() > std::numeric_limits<std::uint32_t>::max() || record.files.size() > std::numeric_limits<std::uint32_t>::max())
        throw std::length_error("too many backup inventory records");
    std::vector<std::uint8_t> bytes(kMagic.begin(), kMagic.end()); WriteLe16(bytes, 1U); WriteLe16(bytes, 0U);
    WriteLe64(bytes, record.created_at_ms); WriteLe32(bytes, static_cast<std::uint32_t>(record.tables.size()));
    for (const auto& table : record.tables) { PutString(bytes, table.name); bytes.insert(bytes.end(), table.epoch.begin(), table.epoch.end()); WriteLe64(bytes, table.revision); }
    WriteLe32(bytes, static_cast<std::uint32_t>(record.files.size()));
    for (const auto& file : record.files) { PutString(bytes, file.relative_path.generic_string()); WriteLe64(bytes, file.size); WriteLe32(bytes, file.crc32); }
    WriteLe32(bytes, Crc32(bytes)); return bytes;
}
BackupRecord ParseBackupRecord(const std::vector<std::uint8_t>& bytes) {
    if (bytes.size() < 28U || !std::equal(kMagic.begin(), kMagic.end(), bytes.begin()) ||
        ReadLe32(bytes, bytes.size() - 4U) != Crc32(bytes.data(), bytes.size() - 4U)) throw std::runtime_error("invalid backup marker or checksum");
    Reader reader(bytes, bytes.size() - 4U); if (reader.U16() != 1U || reader.U16() != 0U) throw std::runtime_error("unsupported backup marker version");
    BackupRecord record; record.created_at_ms = reader.U64(); auto tables = reader.U32();
    if (tables > reader.left() / 28U) throw std::runtime_error("truncated backup tables");
    for (std::uint32_t i = 0; i < tables; ++i) { BackupTableCut cut; cut.name = reader.String(); cut.epoch = reader.Id(); cut.revision = reader.U64(); record.tables.push_back(std::move(cut)); }
    auto files = reader.U32(); if (files > reader.left() / 16U) throw std::runtime_error("truncated backup inventory");
    for (std::uint32_t i = 0; i < files; ++i) { BackupFileRecord file; file.relative_path = ParseInventoryPath(reader.String()); file.size = reader.U64(); file.crc32 = reader.U32(); record.files.push_back(std::move(file)); }
    if (reader.left() != 0U) throw std::runtime_error("backup marker has trailing bytes");
    ValidateRecord(record);
    return record;
}
BackupRecord ReadBackupRecord(const std::filesystem::path& root) {
    RequireRegular(root / kBackupMarkerName); return ParseBackupRecord(LoadFile(root / kBackupMarkerName));
}
void ValidateBackupContents(const std::filesystem::path& root, const BackupRecord& record) { ValidateInventory(root, record, true); }
void ValidateBackupInventory(const std::filesystem::path& root, const BackupRecord& record) { ValidateInventory(root, record, false); }
void SyncBackupTree(const std::filesystem::path& root, const BackupCancel& cancelled) { SyncTreeImpl(root, cancelled); }
void CompleteBackupGuard(const std::filesystem::path& root, std::string_view guard, std::string_view phase) {
    SafeDirectory directory;
    directory.Open(root, false);
    const auto failpoint = [&](std::string_view suffix) {
        return "CHUNKDB_FAILPOINT_" + std::string(phase) + "_" + std::string(suffix) + "_ONCE";
    };
    const auto remove_fail = failpoint("GUARD_REMOVE_FAIL");
    const auto sync_fail = failpoint("COMPLETE_SYNC_FAIL");
    const auto reinstate_fail = failpoint("GUARD_REINSTATE_FAIL");
    const auto crash_remove = "CHUNKDB_FAILPOINT_CRASH_" + std::string(phase) + "_AFTER_GUARD_REMOVE_ONCE";
    const auto crash_complete = "CHUNKDB_FAILPOINT_CRASH_" + std::string(phase) + "_AFTER_COMPLETE_ONCE";
    if (ConsumeFailpointEnv(remove_fail.c_str())) throw std::runtime_error("injected publication guard removal failure");
    directory.Remove(guard);
    Crash(crash_remove.c_str());
    try {
        if (ConsumeFailpointEnv(sync_fail.c_str())) throw std::runtime_error("injected publication completion sync failure");
        directory.Sync();
    } catch (const std::exception& completion) {
        try {
            if (ConsumeFailpointEnv(reinstate_fail.c_str())) throw std::runtime_error("injected guard reinstatement failure");
            PublishSafeFile(directory, guard, {'C', 'K', 'I', 'N'});
        } catch (const std::exception& reinstatement) {
            throw BackupPublicationUnknownError(std::string("publication outcome is unknown: ") + completion.what() + "; guard reinstatement failed: " + reinstatement.what());
        }
        throw;
    }
    Crash(crash_complete.c_str());
}
void CompleteBackup(const std::filesystem::path& root, const BackupRecord& record, const BackupCancel& cancelled) {
    if (!Present(root / kBackupIncompleteName)) throw std::runtime_error("backup incomplete guard is missing");
    ValidateInventory(root, record, true, cancelled); Cancelled(cancelled); SyncBackupTree(root, cancelled);
    Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_BEFORE_MARKER_ONCE");
    SafeDirectory directory;
    directory.Open(root, false);
    ReplaceSafeFile(directory, kBackupMarkerName, SerializeBackupRecord(record));
    Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_MARKER_ONCE");
    Cancelled(cancelled);
    CompleteBackupGuard(root, kBackupIncompleteName, "BACKUP");
}
} // namespace chunkdb
