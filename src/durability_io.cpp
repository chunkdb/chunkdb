#include "checkpoint.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <vector>

#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/zrle.hpp"
#include "durability_io.hpp"
#include "file_permissions.hpp"
#include "wal_stream_pool.hpp"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#if defined(__linux__)
#include <sys/syscall.h>
#endif
#endif

namespace chunkdb {

namespace {
std::atomic<bool> g_directory_sync_observed{false};
std::mutex g_directory_sync_observer_mutex;
std::function<void(const std::filesystem::path&)> g_directory_sync_observer;

void NoteDirectorySynced(const std::filesystem::path& path) {
    if (!g_directory_sync_observed.load(std::memory_order_acquire)) {
        return;
    }
    std::lock_guard lock(g_directory_sync_observer_mutex);
    if (g_directory_sync_observer) {
        g_directory_sync_observer(path);
    }
}

std::atomic<bool> g_file_sync_observed{false};
std::mutex g_file_sync_observer_mutex;
std::function<void(const std::filesystem::path&)> g_file_sync_observer;

void NoteFileSynced(const std::filesystem::path& path) {
    if (!g_file_sync_observed.load(std::memory_order_acquire)) {
        return;
    }
    std::lock_guard lock(g_file_sync_observer_mutex);
    if (g_file_sync_observer) {
        g_file_sync_observer(path);
    }
}
}  // namespace

void SetDirectorySyncObserverForTests(std::function<void(const std::filesystem::path&)> observer) {
    std::lock_guard lock(g_directory_sync_observer_mutex);
    g_directory_sync_observed.store(static_cast<bool>(observer), std::memory_order_release);
    g_directory_sync_observer = std::move(observer);
}

void SetFileSyncObserverForTests(std::function<void(const std::filesystem::path&)> observer) {
    std::lock_guard lock(g_file_sync_observer_mutex);
    g_file_sync_observed.store(static_cast<bool>(observer), std::memory_order_release);
    g_file_sync_observer = std::move(observer);
}

#ifdef _WIN32
std::mutex g_windows_durability_warning_mutex;
std::unordered_set<std::string> g_windows_directory_sync_warning_paths;

void LogWindowsDirectorySyncUnavailableOnce(
    const std::filesystem::path& path,
    DWORD error_code,
    const std::string& error_message) {
    const std::string path_key = CanonicalPathKey(path);
    {
        std::lock_guard lock(g_windows_durability_warning_mutex);
        if (!g_windows_directory_sync_warning_paths.insert(path_key).second) {
            return;
        }
    }

    LogMessage(
        LogLevel::kError,
        LogComponent::kStore,
        "strict durability unavailable on Windows; directory sync capability missing",
        {
            {"path", path_key},
            {"step", "directory_sync"},
            {"error_code", std::to_string(static_cast<unsigned long>(error_code))},
            {"error_message", error_message},
            {"impact", "strict durability mode cannot be honored on this filesystem/runtime"},
        });
}
#endif
[[nodiscard]] std::system_error BuildErrnoError(
    const std::string& action,
    const std::filesystem::path& path,
    int err) {
    return std::system_error(err, std::generic_category(),
        action + ": " + path.string() +
        " (errno=" + std::to_string(err) +
        ", msg='" + std::strerror(err) + "')");
}
#ifdef _WIN32
[[nodiscard]] std::system_error BuildWin32Error(
    const std::string& action,
    const std::filesystem::path& path,
    DWORD code) {
    const auto cause = code == ERROR_DISK_FULL || code == ERROR_HANDLE_DISK_FULL
        ? std::error_code(ENOSPC, std::generic_category())
        : std::error_code(static_cast<int>(code), std::system_category());
    return std::system_error(cause,
        action + ": " + path.string() +
        " (win32=" + std::to_string(static_cast<unsigned long>(code)) +
        ", msg='" + std::system_category().message(static_cast<int>(code)) + "')");
}
#endif
#ifdef _WIN32
void CloseHandleChecked(HANDLE handle, const std::filesystem::path& path, const std::string& action) {
    if (CloseHandle(handle) == 0) {
        throw BuildWin32Error(action, path, GetLastError());
    }
}

void FlushHandleChecked(HANDLE handle, const std::filesystem::path& path, const std::string& action) {
    if (FlushFileBuffers(handle) == 0) {
        throw BuildWin32Error(action, path, GetLastError());
    }
}

void WriteAtomicTempFile(
    const std::filesystem::path& tmp_path,
    const std::vector<std::uint8_t>& bytes,
    bool durable_sync,
    bool enable_generic_failpoints) {
    OwnerOnlySecurity security(tmp_path);
    const std::wstring tmp_w = tmp_path.wstring();
    // CREATE_NEW (not CREATE_ALWAYS) so an existing name is an error rather than a
    // silent truncate, and OPEN_REPARSE_POINT so we never write through a symlink
    // or junction. Mirrors the O_EXCL | O_NOFOLLOW stance on POSIX.
    HANDLE handle = CreateFileW(
        tmp_w.c_str(),
        GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        security.attributes(),
        CREATE_NEW,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
        nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        throw BuildWin32Error("failed to open temporary file", tmp_path, GetLastError());
    }

    try {
        if (enable_generic_failpoints &&
            ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_WRITE_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected temporary file write failure: " + tmp_path.string());
        }
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            const std::size_t remaining = bytes.size() - offset;
            const DWORD chunk = static_cast<DWORD>(std::min<std::size_t>(remaining, 1U << 20U));
            DWORD written = 0;
            const BOOL ok = WriteFile(
                handle,
                bytes.data() + offset,
                chunk,
                &written,
                nullptr);
            if (ok == 0 || written != chunk) {
                throw BuildWin32Error("failed to write temporary file", tmp_path, GetLastError());
            }
            offset += written;
        }

        if (durable_sync) {
            if (enable_generic_failpoints &&
                ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_SYNC_FAIL_ONCE")) {
                throw std::runtime_error(
                    "injected temporary file sync failure: " + tmp_path.string());
            }
            FlushHandleChecked(handle, tmp_path, "failed to flush temporary file");
        }

        if (enable_generic_failpoints &&
            ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_CLOSE_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected temporary file close failure: " + tmp_path.string());
        }

        CloseHandleChecked(handle, tmp_path, "failed to close temporary file");
        handle = INVALID_HANDLE_VALUE;
    } catch (...) {
        if (handle != INVALID_HANDLE_VALUE) {
            (void)CloseHandle(handle);
        }
        std::error_code cleanup_ec;
        std::filesystem::remove(tmp_path, cleanup_ec);
        throw;
    }
}

namespace {

std::error_code RenameByHandle(
    const std::filesystem::path& tmp_path,
    const std::filesystem::path& target_path,
    bool replace_if_exists) {
    const std::wstring tmp_w = tmp_path.wstring();
    HANDLE tmp_handle = CreateFileW(
        tmp_w.c_str(),
        DELETE | SYNCHRONIZE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        nullptr);
    if (tmp_handle == INVALID_HANDLE_VALUE) {
        return std::error_code(static_cast<int>(GetLastError()), std::system_category());
    }

    // Preserve native absolute names: CRT absolute-path normalization can
    // damage UNC roots and extended namespace prefixes before rename.
    const std::wstring target_w = target_path.native().starts_with(L"\\\\") ?
        target_path.native() : std::filesystem::absolute(target_path).wstring();
    std::vector<std::uint8_t> rename_bytes(
        sizeof(FILE_RENAME_INFO) + target_w.size() * sizeof(wchar_t));
    auto* rename_info = reinterpret_cast<FILE_RENAME_INFO*>(rename_bytes.data());
    rename_info->ReplaceIfExists = replace_if_exists ? TRUE : FALSE;
    rename_info->RootDirectory = nullptr;
    rename_info->FileNameLength = static_cast<DWORD>(target_w.size() * sizeof(wchar_t));
    std::memcpy(rename_info->FileName, target_w.data(), rename_info->FileNameLength);

    const BOOL rename_ok = SetFileInformationByHandle(
        tmp_handle,
        FileRenameInfo,
        rename_info,
        static_cast<DWORD>(rename_bytes.size()));
    const DWORD rename_error = rename_ok != 0 ? ERROR_SUCCESS : GetLastError();

    const BOOL close_ok = CloseHandle(tmp_handle);
    const DWORD close_error = close_ok != 0 ? ERROR_SUCCESS : GetLastError();

    if (rename_ok == 0) {
        return std::error_code(static_cast<int>(rename_error), std::system_category());
    }
    if (close_ok == 0) {
        return std::error_code(static_cast<int>(close_error), std::system_category());
    }
    return {};
}

}  // namespace

std::error_code ReplacePathAtomically(
    const std::filesystem::path& tmp_path,
    const std::filesystem::path& target_path) {
    return RenameByHandle(tmp_path, target_path, /*replace_if_exists=*/true);
}

std::error_code MovePathNoReplace(
    const std::filesystem::path& tmp_path,
    const std::filesystem::path& target_path) {
    const auto ec = RenameByHandle(tmp_path, target_path, /*replace_if_exists=*/false);
    if (ec.value() == ERROR_ALREADY_EXISTS || ec.value() == ERROR_FILE_EXISTS) {
        return std::make_error_code(std::errc::file_exists);
    }
    return ec;
}

std::error_code MoveDirectoryNoReplace(
    const std::filesystem::path& from,
    const std::filesystem::path& to) {
    // Without MOVEFILE_REPLACE_EXISTING the move fails when `to` exists.
    if (MoveFileExW(from.wstring().c_str(), to.wstring().c_str(), MOVEFILE_WRITE_THROUGH) != 0) {
        return {};
    }
    const DWORD error = GetLastError();
    if (error == ERROR_ALREADY_EXISTS || error == ERROR_FILE_EXISTS) {
        return std::make_error_code(std::errc::file_exists);
    }
    return std::error_code(static_cast<int>(error), std::system_category());
}

namespace {
// Returns false, without syncing, when `missing_ok` and `path` does not exist.
bool SyncFileAt(const std::filesystem::path& path, bool missing_ok) {
    HANDLE handle = CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
        OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        const DWORD open_error = GetLastError();
        if (missing_ok && (open_error == ERROR_FILE_NOT_FOUND || open_error == ERROR_PATH_NOT_FOUND)) {
            return false;
        }
        throw BuildWin32Error("failed to open file for durability sync", path, open_error);
    }
    const BOOL synced = FlushFileBuffers(handle);
    const DWORD sync_error = synced != 0 ? ERROR_SUCCESS : GetLastError();
    const BOOL closed = CloseHandle(handle);
    const DWORD close_error = closed != 0 ? ERROR_SUCCESS : GetLastError();
    if (synced == 0) {
        throw BuildWin32Error("failed to sync file", path, sync_error);
    }
    if (closed == 0) {
        throw BuildWin32Error("failed to close synced file", path, close_error);
    }
    NoteFileSynced(path);
    return true;
}

bool SyncDirectoryAt(const std::filesystem::path& path, bool missing_ok) {
    const std::wstring path_w = path.wstring();
    HANDLE handle = CreateFileW(
        path_w.c_str(),
        GENERIC_READ | GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS,
        nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        const DWORD open_error = GetLastError();
        if (missing_ok && (open_error == ERROR_FILE_NOT_FOUND || open_error == ERROR_PATH_NOT_FOUND)) {
            return false;
        }
        throw BuildWin32Error("failed to open directory for durability sync", path, open_error);
    }
    try {
        DWORD flush_error = ERROR_SUCCESS;
        bool flush_failed = false;
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WINDOWS_DIRECTORY_SYNC_CAPABILITY_ERROR_ONCE")) {
            flush_failed = true;
            flush_error = ERROR_INVALID_FUNCTION;
        } else if (FlushFileBuffers(handle) == 0) {
            flush_failed = true;
            flush_error = GetLastError();
        }
        if (flush_failed) {
            // Some Windows filesystems/runtimes do not allow directory handle flush.
            // In strict durability paths, this means the configured contract cannot be honored.
            if (flush_error != ERROR_ACCESS_DENIED &&
                flush_error != ERROR_INVALID_HANDLE &&
                flush_error != ERROR_INVALID_FUNCTION) {
                throw BuildWin32Error("failed to sync directory", path, flush_error);
            }
            LogWindowsDirectorySyncUnavailableOnce(
                path,
                flush_error,
                std::system_category().message(static_cast<int>(flush_error)));
            throw BuildWin32Error(
                "strict durability requires Windows directory sync capability",
                path,
                flush_error);
        }
        CloseHandleChecked(handle, path, "failed to close synced directory");
    } catch (...) {
        (void)CloseHandle(handle);
        throw;
    }
    NoteDirectorySynced(path);
    return true;
}
}  // namespace

#else
void CloseFdChecked(int fd, const std::filesystem::path& path, const std::string& action) {
    while (::close(fd) != 0) {
        if (errno == EINTR) {
            continue;
        }
        throw BuildErrnoError(action, path, errno);
    }
}

void SyncFdDurably(int fd, const std::filesystem::path& path, bool full_sync) {
#if defined(__APPLE__)
    if (full_sync) {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_FULL_SYNC_FILE_FAIL_ONCE")) {
            throw BuildErrnoError("failed to F_FULLFSYNC file", path, EIO);
        }
        if (::fcntl(fd, F_FULLFSYNC, 0) == 0) {
            return;
        }
        const int fullsync_error = errno;
        // F_FULLFSYNC can be unsupported on some macOS filesystems/devices.
        // Fall back to fsync while surfacing non-capability errors.
        if (fullsync_error != EINVAL && fullsync_error != ENOTSUP && fullsync_error != ENOTTY) {
            throw BuildErrnoError("failed to F_FULLFSYNC file", path, fullsync_error);
        }
    }
    if (::fsync(fd) != 0) {
        throw BuildErrnoError("failed to fsync file", path, errno);
    }
#else
    (void)full_sync;
    if (::fdatasync(fd) != 0) {
        const int data_sync_error = errno;
        if (data_sync_error == EINVAL) {
            if (::fsync(fd) != 0) {
                throw BuildErrnoError("failed to fsync file", path, errno);
            }
            return;
        }
        throw BuildErrnoError("failed to fdatasync file", path, data_sync_error);
    }
#endif
}

void WriteAtomicTempFile(
    const std::filesystem::path& tmp_path,
    const std::vector<std::uint8_t>& bytes,
    bool durable_sync,
    bool enable_generic_failpoints) {
    // O_EXCL: BuildAtomicTmpPath already makes the name unique per attempt, so an
    // existing name means either a stale artifact or a planted file — either way,
    // fail rather than silently truncate it. O_NOFOLLOW: never write through a
    // symlink left in the target directory. EnsureDirectoryPathExists applies the
    // same stance on the directory axis.
    const int fd =
        ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
    if (fd < 0) {
        throw BuildErrnoError("failed to open temporary file", tmp_path, errno);
    }

    try {
        RequireOwnerOnlyDescriptor(fd, tmp_path);
        if (enable_generic_failpoints &&
            ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_WRITE_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected temporary file write failure: " + tmp_path.string());
        }
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            const std::size_t remaining = bytes.size() - offset;
            const ssize_t written = ::write(fd, bytes.data() + offset, remaining);
            if (written < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw BuildErrnoError("failed to write temporary file", tmp_path, errno);
            }
            if (written == 0) {
                throw std::runtime_error(
                    "short write while writing temporary file: " + tmp_path.string());
            }
            offset += static_cast<std::size_t>(written);
        }

        if (durable_sync) {
            if (enable_generic_failpoints &&
                ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_SYNC_FAIL_ONCE")) {
                throw std::runtime_error(
                    "injected temporary file sync failure: " + tmp_path.string());
            }
            SyncFdDurably(fd, tmp_path, true);
        }

        if (enable_generic_failpoints &&
            ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_CLOSE_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected temporary file close failure: " + tmp_path.string());
        }

        CloseFdChecked(fd, tmp_path, "failed to close temporary file");
    } catch (...) {
        (void)::close(fd);
        std::error_code cleanup_ec;
        std::filesystem::remove(tmp_path, cleanup_ec);
        throw;
    }
}

std::error_code ReplacePathAtomically(
    const std::filesystem::path& tmp_path,
    const std::filesystem::path& target_path) {
    std::error_code ec;
    std::filesystem::rename(tmp_path, target_path, ec);
    return ec;
}

namespace {

// errno values with which an exclusive rename reports that the kernel or the
// filesystem does not implement it, as opposed to a real failure.
[[nodiscard]] bool ExclusiveRenameUnsupported(int error) {
    if (error == EINVAL || error == ENOSYS || error == ENOTSUP) {
        return true;
    }
#if defined(EOPNOTSUPP) && EOPNOTSUPP != ENOTSUP
    if (error == EOPNOTSUPP) {
        return true;
    }
#endif
    return false;
}

}  // namespace

std::error_code MovePathNoReplace(
    const std::filesystem::path& tmp_path,
    const std::filesystem::path& target_path) {
    // rename() always replaces. Use the exclusive rename where the platform
    // has one; it fails with EEXIST instead of replacing.
#if defined(__linux__) && defined(SYS_renameat2)
    constexpr unsigned int kRenameNoReplace = 1U;  // RENAME_NOREPLACE
    if (::syscall(SYS_renameat2, AT_FDCWD, tmp_path.c_str(), AT_FDCWD, target_path.c_str(),
                  kRenameNoReplace) == 0) {
        return {};
    }
    if (!ExclusiveRenameUnsupported(errno)) {
        return std::error_code(errno, std::generic_category());
    }
#elif defined(__APPLE__)
    if (::renamex_np(tmp_path.c_str(), target_path.c_str(), RENAME_EXCL) == 0) {
        return {};
    }
    if (!ExclusiveRenameUnsupported(errno)) {
        return std::error_code(errno, std::generic_category());
    }
#endif
    // No exclusive rename here: a hard link never replaces either, so the
    // publication is still the existence check. The temporary name is
    // dropped afterwards. A filesystem without hard links fails here.
    if (::link(tmp_path.c_str(), target_path.c_str()) != 0) {
        return std::error_code(errno, std::generic_category());
    }
    if (::unlink(tmp_path.c_str()) != 0) {
        return std::error_code(errno, std::generic_category());
    }
    return {};
}

std::error_code MoveDirectoryNoReplace(
    const std::filesystem::path& from,
    const std::filesystem::path& to) {
#if defined(__linux__) && defined(SYS_renameat2)
    constexpr unsigned int kRenameNoReplace = 1U;  // RENAME_NOREPLACE
    if (::syscall(SYS_renameat2, AT_FDCWD, from.c_str(), AT_FDCWD, to.c_str(),
                  kRenameNoReplace) == 0) {
        return {};
    }
    if (!ExclusiveRenameUnsupported(errno)) {
        return std::error_code(errno, std::generic_category());
    }
#elif defined(__APPLE__)
    if (::renamex_np(from.c_str(), to.c_str(), RENAME_EXCL) == 0) {
        return {};
    }
    if (!ExclusiveRenameUnsupported(errno)) {
        return std::error_code(errno, std::generic_category());
    }
#endif
    // rename() of a directory replaces an existing empty directory, so check
    // first; the caller's writer lock keeps the name free until the rename.
    struct stat existing {};
    if (::lstat(to.c_str(), &existing) == 0) {
        return std::make_error_code(std::errc::file_exists);
    }
    if (errno != ENOENT) {
        return std::error_code(errno, std::generic_category());
    }
    if (::rename(from.c_str(), to.c_str()) != 0) {
        return std::error_code(errno, std::generic_category());
    }
    return {};
}

// Every caller makes a promise of durability (a WAL acknowledgement in a
// synced mode, WALFLUSH, a rollback boundary), so on macOS this asks the
// drive to flush its cache as well (F_FULLFSYNC); plain fsync does not.
namespace {
// Returns false, without syncing, when `missing_ok` and `path` does not exist.
bool SyncFileAt(const std::filesystem::path& path, bool missing_ok) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        const int open_error = errno;
        if (missing_ok && open_error == ENOENT) {
            return false;
        }
        throw BuildErrnoError("failed to open file for durability sync", path, open_error);
    }
    try {
        SyncFdDurably(fd, path, true);
        CloseFdChecked(fd, path, "failed to close synced file");
    } catch (...) {
        (void)::close(fd);
        throw;
    }
    NoteFileSynced(path);
    return true;
}

bool SyncDirectoryAt(const std::filesystem::path& path, bool missing_ok) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        const int open_error = errno;
        if (missing_ok && open_error == ENOENT) {
            return false;
        }
        throw BuildErrnoError("failed to open directory for durability sync", path, open_error);
    }
    try {
#if defined(__APPLE__)
        // As for files: a directory entry is durable only once the drive's
        // cache is flushed too.
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_FULL_SYNC_DIRECTORY_FAIL_ONCE")) {
            throw BuildErrnoError("failed to F_FULLFSYNC directory", path, EIO);
        }
        if (::fcntl(fd, F_FULLFSYNC, 0) != 0) {
            const int fullsync_error = errno;
            if (fullsync_error != EINVAL && fullsync_error != ENOTSUP && fullsync_error != ENOTTY) {
                throw BuildErrnoError("failed to F_FULLFSYNC directory", path, fullsync_error);
            }
            if (::fsync(fd) != 0) {
                throw BuildErrnoError("failed to sync directory", path, errno);
            }
        }
#else
        if (::fsync(fd) != 0) {
            throw BuildErrnoError("failed to sync directory", path, errno);
        }
#endif
        CloseFdChecked(fd, path, "failed to close synced directory");
    } catch (...) {
        (void)::close(fd);
        throw;
    }
    NoteDirectorySynced(path);
    return true;
}
}  // namespace
#endif

void SyncFilePath(const std::filesystem::path& path) {
    (void)SyncFileAt(path, false);
}

bool SyncFilePathIfPresent(const std::filesystem::path& path) {
    return SyncFileAt(path, true);
}

void SyncDirectoryPath(const std::filesystem::path& path) {
    (void)SyncDirectoryAt(path, false);
}

bool SyncDirectoryPathIfPresent(const std::filesystem::path& path) {
    return SyncDirectoryAt(path, true);
}

void SyncParentDirectory(const std::filesystem::path& path) {
    std::filesystem::path parent = path.parent_path();
    if (parent.empty()) {
        std::error_code current_ec;
        parent = std::filesystem::current_path(current_ec);
        if (current_ec) {
            throw std::runtime_error(
                "failed to resolve current directory for durability sync of: " + path.string() +
                " (error " + std::to_string(current_ec.value()) + ": " + current_ec.message() + ")");
        }
    }
    SyncDirectoryPath(parent);
}

bool EnsureDirectoryPathExists(
    const std::filesystem::path& path,
    bool durable_sync) {
    if (path.empty()) {
        return false;
    }

    std::error_code status_ec;
    const auto status = std::filesystem::symlink_status(path, status_ec);
    if (!status_ec) {
        if (std::filesystem::is_directory(status)) {
            return false;
        }
        throw std::runtime_error(
            "expected directory path but found non-directory: " + path.string());
    }
    if (status_ec != std::errc::no_such_file_or_directory) {
        throw std::runtime_error(
            "failed to inspect directory path: " + path.string() +
            " (error " + std::to_string(status_ec.value()) + ": " + status_ec.message() + ")");
    }

    const auto parent = path.parent_path();
    if (!parent.empty() && parent != path) {
        (void)EnsureDirectoryPathExists(parent, durable_sync);
    }

    const bool created = CreateOwnerOnlyDirectory(path);

    if (!created || !durable_sync) {
        return created;
    }

    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_DIR_CREATE_BEFORE_PARENT_SYNC_ONCE")) {
        throw std::runtime_error(
            "injected directory durability failure before parent sync: " + path.string());
    }

    SyncParentDirectory(path);
    return true;
}

}  // namespace chunkdb
