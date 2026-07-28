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
#include "wal_stream_pool.hpp"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace chunkdb {

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
[[nodiscard]] std::runtime_error BuildErrnoError(
    const std::string& action,
    const std::filesystem::path& path,
    int err) {
    return std::runtime_error(
        action + ": " + path.string() +
        " (errno=" + std::to_string(err) +
        ", msg='" + std::strerror(err) + "')");
}
#ifdef _WIN32
[[nodiscard]] std::runtime_error BuildWin32Error(
    const std::string& action,
    const std::filesystem::path& path,
    DWORD code) {
    return std::runtime_error(
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
    const std::wstring tmp_w = tmp_path.wstring();
    // CREATE_NEW (not CREATE_ALWAYS) so an existing name is an error rather than a
    // silent truncate, and OPEN_REPARSE_POINT so we never write through a symlink
    // or junction. Mirrors the O_EXCL | O_NOFOLLOW stance on POSIX.
    HANDLE handle = CreateFileW(
        tmp_w.c_str(),
        GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
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

std::error_code ReplacePathAtomically(
    const std::filesystem::path& tmp_path,
    const std::filesystem::path& target_path) {
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

    const std::wstring target_w = std::filesystem::absolute(target_path).wstring();
    std::vector<std::uint8_t> rename_bytes(
        sizeof(FILE_RENAME_INFO) + target_w.size() * sizeof(wchar_t));
    auto* rename_info = reinterpret_cast<FILE_RENAME_INFO*>(rename_bytes.data());
    rename_info->ReplaceIfExists = TRUE;
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

void SyncFilePath(const std::filesystem::path& path) {
    const std::string path_u8 = path.string();
    const int fd = _open(path_u8.c_str(), _O_RDWR | _O_BINARY);
    if (fd < 0) {
        throw std::runtime_error(
            "failed to open file for durability sync: " + path.string());
    }
    const int sync_rc = _commit(fd);
    const int close_rc = _close(fd);
    if (sync_rc != 0) {
        throw std::runtime_error("failed to sync file: " + path.string());
    }
    if (close_rc != 0) {
        throw std::runtime_error("failed to close synced file: " + path.string());
    }
}

void SyncDirectoryPath(const std::filesystem::path& path) {
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
        throw BuildWin32Error("failed to open directory for durability sync", path, GetLastError());
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
}

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
    const int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0644);
    if (fd < 0) {
        throw BuildErrnoError("failed to open temporary file", tmp_path, errno);
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

void SyncFilePath(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw BuildErrnoError("failed to open file for durability sync", path, errno);
    }
    try {
        SyncFdDurably(fd, path, false);
        CloseFdChecked(fd, path, "failed to close synced file");
    } catch (...) {
        (void)::close(fd);
        throw;
    }
}

void SyncDirectoryPath(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        throw BuildErrnoError("failed to open directory for durability sync", path, errno);
    }
    try {
        if (::fsync(fd) != 0) {
            throw BuildErrnoError("failed to sync directory", path, errno);
        }
        CloseFdChecked(fd, path, "failed to close synced directory");
    } catch (...) {
        (void)::close(fd);
        throw;
    }
}
#endif

void EnsureDirectoryPathExists(
    const std::filesystem::path& path,
    bool durable_sync) {
    if (path.empty()) {
        return;
    }

    std::error_code status_ec;
    const auto status = std::filesystem::symlink_status(path, status_ec);
    if (!status_ec) {
        if (std::filesystem::is_directory(status)) {
            return;
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
        EnsureDirectoryPathExists(parent, durable_sync);
    }

    std::error_code create_ec;
    const bool created = std::filesystem::create_directory(path, create_ec);
    if (create_ec) {
        if (create_ec == std::errc::file_exists) {
            std::error_code restat_ec;
            const auto restat = std::filesystem::symlink_status(path, restat_ec);
            if (!restat_ec && std::filesystem::is_directory(restat)) {
                return;
            }
        }
        throw std::runtime_error(
            "failed to create directory path: " + path.string() +
            " (error " + std::to_string(create_ec.value()) + ": " + create_ec.message() + ")");
    }

    if (!created || !durable_sync) {
        return;
    }

    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_DIR_CREATE_BEFORE_PARENT_SYNC_ONCE")) {
        throw std::runtime_error(
            "injected directory durability failure before parent sync: " + path.string());
    }

    std::filesystem::path sync_parent = parent;
    if (sync_parent.empty()) {
        std::error_code current_ec;
        sync_parent = std::filesystem::current_path(current_ec);
        if (current_ec) {
            throw std::runtime_error(
                "failed to resolve current directory for durability sync after creating: " + path.string() +
                " (error " + std::to_string(current_ec.value()) + ": " + current_ec.message() + ")");
        }
    }
    SyncDirectoryPath(sync_parent);
}

}  // namespace chunkdb
