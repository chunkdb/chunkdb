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

[[nodiscard]] std::size_t CheckpointHysteresisTarget(std::size_t lower_bound) noexcept {
    if (lower_bound <= 1) {
        return lower_bound;
    }

    const std::size_t extra = std::max<std::size_t>(1, lower_bound / 2);
    if (lower_bound > std::numeric_limits<std::size_t>::max() - extra) {
        return std::numeric_limits<std::size_t>::max();
    }
    return lower_bound + extra;
}

[[nodiscard]] bool CheckpointMetricReachedTrigger(std::size_t value, std::size_t lower_bound) noexcept {
    if (lower_bound == 0) {
        return false;
    }
    return value >= CheckpointHysteresisTarget(lower_bound);
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
bool IsAtomicWriteTransientError(const std::error_code& ec) {
    if (!ec) {
        return false;
    }
#ifdef _WIN32
    const int code = ec.value();
    return code == ERROR_ACCESS_DENIED ||
           code == ERROR_SHARING_VIOLATION ||
           code == ERROR_LOCK_VIOLATION ||
           code == ERROR_FILE_EXISTS ||
           code == ERROR_ALREADY_EXISTS ||
           code == ERROR_BUSY;
#else
    const auto c = static_cast<std::errc>(ec.value());
    return c == std::errc::permission_denied ||
           c == std::errc::device_or_resource_busy ||
           c == std::errc::resource_unavailable_try_again ||
           c == std::errc::operation_not_permitted ||
           c == std::errc::text_file_busy ||
           c == std::errc::file_exists;
#endif
}

std::filesystem::path BuildAtomicTmpPath(const std::filesystem::path& path) {
    static std::atomic<std::uint64_t> seq{0};
    const auto now = static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const auto tid = static_cast<std::uint64_t>(
        std::hash<std::thread::id>{}(std::this_thread::get_id()));
    const auto id = seq.fetch_add(1, std::memory_order_relaxed) + 1;
    return path.string() + ".tmp." + std::to_string(CurrentProcessIdValue()) +
           "." + std::to_string(tid) +
           "." + std::to_string(now) +
           "." + std::to_string(id);
}
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
std::uint64_t TmpArtifactAgeMs(const std::filesystem::path& tmp_path) {
    std::error_code ts_ec;
    const auto ts = std::filesystem::last_write_time(tmp_path, ts_ec);
    if (ts_ec) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    const auto now = std::filesystem::file_time_type::clock::now();
    if (now <= ts) {
        return 0;
    }
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now - ts).count());
}

bool ShouldCleanupTmpArtifactForCurrentProcess(
    const std::filesystem::path& target_path,
    const std::filesystem::path& tmp_path) {
    const std::string prefix = target_path.filename().string() + ".tmp.";
    const std::string name = tmp_path.filename().string();
    if (name.rfind(prefix, 0) != 0) {
        return false;
    }

    const std::string suffix = name.substr(prefix.size());
    const std::size_t dot = suffix.find('.');
    if (dot == std::string::npos || dot == 0) {
        return true;
    }

    std::int64_t pid = -1;
    if (!TryParseInt64(suffix.substr(0, dot), &pid) || pid <= 0) {
        return true;
    }

    const auto current_pid = static_cast<std::int64_t>(CurrentProcessIdValue());
    if (pid != current_pid) {
        return !IsProcessAlive(pid);
    }
    return TmpArtifactAgeMs(tmp_path) >= kAtomicTmpCurrentPidCleanupMinAgeMs;
}

void CleanupAtomicTmpArtifacts(const std::filesystem::path& target_path) {
    const auto parent = target_path.parent_path();
    std::error_code exists_ec;
    if (!std::filesystem::exists(parent, exists_ec)) {
        if (exists_ec) {
            throw std::runtime_error(
                "failed to inspect chunk directory for temporary artifacts: " + parent.string() +
                " (ec=" + std::to_string(exists_ec.value()) +
                ", msg='" + exists_ec.message() + "')");
        }
        return;
    }

    for (const auto& entry : std::filesystem::directory_iterator(parent)) {
        const auto& tmp_path = entry.path();
        if (!entry.is_regular_file()) {
            continue;
        }
        if (!ShouldCleanupTmpArtifactForCurrentProcess(target_path, tmp_path)) {
            continue;
        }
        std::error_code remove_ec;
        std::filesystem::remove(tmp_path, remove_ec);
        if (remove_ec) {
            throw std::runtime_error(
                "failed to remove stale temporary chunk artifact: " + tmp_path.string() +
                " (ec=" + std::to_string(remove_ec.value()) +
                ", msg='" + remove_ec.message() + "')");
        }
    }
}
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
    HANDLE handle = CreateFileW(
        tmp_w.c_str(),
        GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
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
    const int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
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
std::vector<std::uint8_t> SerializeChunkImage(
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    const std::vector<std::uint8_t>& payload,
    const std::vector<std::uint8_t>& presence_bitmap,
    CheckpointCompression compression) {
    const auto state = BuildChunkStateBytes(geometry, payload, presence_bitmap);

    std::vector<std::uint8_t> bytes;
    bytes.reserve(64U + state.size());

    bytes.insert(bytes.end(), kChunkMagic, kChunkMagic + kChunkMagicSize);
    WriteLe16(
        bytes,
        compression == CheckpointCompression::kZrle ? kChunkFileVersionCompressed
                                                    : kChunkFileVersion);
    WriteLe16(bytes, static_cast<std::uint16_t>(geometry.config().block_bits));
    WriteLe32(bytes, geometry.config().chunk_width_blocks);
    WriteLe32(bytes, geometry.config().chunk_height_blocks);
    WriteLe64(bytes, static_cast<std::uint64_t>(chunk_coord.x));
    WriteLe64(bytes, static_cast<std::uint64_t>(chunk_coord.y));
    WriteLe32(bytes, static_cast<std::uint32_t>(payload.size()));
    WriteLe32(bytes, Crc32(state));

    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto millis = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
    WriteLe64(bytes, millis);

    if (compression == CheckpointCompression::kZrle) {
        const auto compressed = ZrleCompress(state);
        bytes.insert(bytes.end(), compressed.begin(), compressed.end());
    } else {
        bytes.insert(bytes.end(), state.begin(), state.end());
    }
    return bytes;
}
void AtomicWrite(
    const std::filesystem::path& path,
    const std::vector<std::uint8_t>& bytes,
    bool fsync_file,
    bool fsync_directory,
    bool* out_replaced,
    const char* after_rename_failpoint,
    bool enable_generic_failpoints) {
    if (out_replaced != nullptr) {
        *out_replaced = false;
    }
    const auto parent = path.parent_path();
    EnsureDirectoryPathExists(parent, fsync_directory);

    const std::filesystem::path tmp_path = BuildAtomicTmpPath(path);

    WriteAtomicTempFile(
        tmp_path, bytes, fsync_file, enable_generic_failpoints);

    if (enable_generic_failpoints &&
        ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_TEMP_FLUSH_ONCE")) {
        // Simulates a crash after the temp file is durable but before the
        // rename: the temp artifact is intentionally left behind for a later
        // load to clean up, exactly as a real crash would.
        throw std::runtime_error(
            "injected atomic write failure after temp flush and close before replace: " + tmp_path.string());
    }

    std::error_code rename_ec;
    for (int attempt = 0; attempt < kAtomicWriteRetryCount; ++attempt) {
        rename_ec = ReplacePathAtomically(tmp_path, path);
        if (!rename_ec) {
            break;
        }

        if (attempt + 1 < kAtomicWriteRetryCount && IsAtomicWriteTransientError(rename_ec)) {
            std::this_thread::sleep_for(kAtomicWriteRetryBaseDelay * (attempt + 1));
            continue;
        }

        break;
    }

    if (rename_ec) {
        std::error_code cleanup_ec;
        std::filesystem::remove(tmp_path, cleanup_ec);
        throw std::runtime_error(
            "failed to move temporary file into place: " + path.string() +
            " (tmp=" + tmp_path.string() +
            ", ec=" + std::to_string(rename_ec.value()) +
            ", msg='" + rename_ec.message() + "')");
    }

    // The target now holds the new bytes. Any later failure is a durability
    // (not a visibility) problem for this write.
    if (out_replaced != nullptr) {
        *out_replaced = true;
    }

    if (fsync_directory) {
        const bool generic_failure =
            enable_generic_failpoints &&
            ConsumeFailpointEnv(
                "CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
        const bool targeted_failure =
            after_rename_failpoint != nullptr &&
            ConsumeFailpointEnv(after_rename_failpoint);
        if (generic_failure || targeted_failure) {
            throw std::runtime_error(
                "injected atomic write failure after replace before directory sync: " + path.string());
        }
        SyncDirectoryPath(parent);
    }
}
bool ChunkStore::IsCheckpointDue(const std::shared_ptr<RegularChunk>& chunk) noexcept {
    if (chunk == nullptr) {
        return false;
    }

    const bool updates_eligible = chunk->pending_updates >= checkpoint_update_interval_;
    const bool wal_eligible = chunk->wal_bytes >= checkpoint_wal_bytes_;
    const bool eligible = updates_eligible || wal_eligible;

    if (!eligible) {
        chunk->checkpoint_due_armed = false;
        return false;
    }

    chunk->checkpoint_due_armed = true;

    if (CheckpointMetricReachedTrigger(chunk->pending_updates, checkpoint_update_interval_) ||
        CheckpointMetricReachedTrigger(chunk->wal_bytes, checkpoint_wal_bytes_)) {
        return true;
    }

    return false;
}

void ChunkStore::MaybeCheckpointChunk(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool* out_image_committed) {
    if (out_image_committed != nullptr) {
        *out_image_committed = false;
    }
    if (!IsCheckpointDue(chunk)) {
        return;
    }

    if (background_maintenance_ && !chunk->background_checkpoint_failed) {
        // Bounded deferral: while checkpoints run in the background the WAL
        // may keep growing, but only up to a hard multiple of the configured
        // thresholds. Past that the writer checkpoints inline (backpressure).
        const bool hard_bound_exceeded =
            chunk->pending_updates >= checkpoint_update_interval_ * 4U ||
            chunk->wal_bytes >= checkpoint_wal_bytes_ * 4U;
        if (!hard_bound_exceeded && EnqueueBackgroundCheckpoint(chunk_coord)) {
            return;
        }
    }

    // A failed background checkpoint is retried inline on the next eligible
    // write so the error reaches a caller instead of only the log.
    CheckpointChunk(chunk_coord, chunk, out_image_committed);
    chunk->background_checkpoint_failed = false;
}

void ChunkStore::CheckpointChunk(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool* out_image_committed) {
    if (out_image_committed != nullptr) {
        *out_image_committed = false;
    }
    SnapshotGenerationWriteGuard snapshot_write(this);
    bool image_committed = false;
    const auto data_path =
        (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1)
            ? ChunkDataPath(data_dir_, geometry_, chunk_coord)
            : RegionDataPath(data_dir_, chunk_coord, experimental_region_span_chunks_);
    const auto wal_path = LayoutWalPath(data_dir_, geometry_, chunk_coord, storage_layout_mode_);

    // Serialize the entire publish step (image replace + WAL removal + unsynced
    // bookkeeping) against WalBarrier's drain+sync. Without this a barrier could
    // drain the tracked WAL, then this checkpoint could remove that WAL and add
    // an unsynced replacement image that the in-flight barrier never syncs,
    // silently dropping a write the barrier promised durable. Lock ordering is
    // chunk->mutex (already held) -> checkpoint_publish_mutex_ -> RegionIoMutex;
    // WalBarrier's step 2 takes only checkpoint_publish_mutex_, so there is no
    // cycle.
    NoteCheckpointPublishAttemptForTests();
    std::unique_lock<std::mutex> publish_lock(checkpoint_publish_mutex_);

    try {
        // The checkpoint replaces the WAL, so in every mode where
        // acknowledgements promise durability (fsync-wal as well as
        // fsync-checkpoint) the image must be durable before the WAL is
        // removed; otherwise removing a durable WAL would silently downgrade
        // the contract to the strength of an unsynced image.
        const bool strict =
            durability_mode_ != DurabilityMode::kRelaxed ||
            barrier_durability_floor_.load(std::memory_order_acquire);
        const bool chunk_populated = ChunkPresent(chunk->presence_bitmap);
        if (!chunk_populated) {
            // Empty-chunk garbage collection: a chunk with no present blocks
            // is observably identical to an absent chunk, so its storage
            // artifacts are reclaimed instead of writing an empty image. The
            // data image is removed before the WAL so a crash between the two
            // steps replays the (empty-state) WAL over an absent image.
            if (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1) {
                std::error_code remove_ec;
                std::filesystem::remove(data_path, remove_ec);
                if (remove_ec) {
                    throw std::runtime_error(
                        "failed to remove empty chunk image: " + data_path.string() +
                        " (ec=" + std::to_string(remove_ec.value()) +
                        ", msg='" + remove_ec.message() + "')");
                }
                if (ConsumeFailpointEnv(
                        "CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_IMAGE_REMOVE_ONCE")) {
                    throw std::runtime_error(
                        "injected empty-chunk GC failure after image removal");
                }
                // In strict modes, make the image removal durable before
                // deleting the WAL that carries the empty state. A crash at
                // any later boundary then sees either the old image plus the
                // empty WAL, or no image plus the empty WAL.
                if (strict) {
                    SyncDirectoryPath(data_path.parent_path());
                    if (ConsumeFailpointEnv(
                            "CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_IMAGE_DIR_SYNC_ONCE")) {
                        throw std::runtime_error(
                            "injected empty-chunk GC failure after image directory sync");
                    }
                } else {
                    NoteUnsyncedDir(data_path.parent_path());
                }
                image_committed = true;
            } else {
                const auto addr = ComputeRegionChunkAddress(chunk_coord, experimental_region_span_chunks_);
                std::lock_guard region_lock(RegionIoMutex());
                if (std::filesystem::exists(data_path)) {
                    const auto region_bytes = LoadFile(data_path);
                    auto region_image =
                        ParseRegionFileImage(region_bytes, geometry_, addr, experimental_region_span_chunks_);
                    SetRegionSlotPresent(&region_image, addr.slot_index, false);
                    region_image.slot_crc[addr.slot_index] = 0;
                    std::fill_n(
                        region_image.slot_payloads.begin() +
                            static_cast<std::ptrdiff_t>(
                                static_cast<std::size_t>(addr.slot_index) * region_image.payload_bytes),
                        region_image.payload_bytes,
                        std::uint8_t{0});
                    bool any_present = false;
                    for (std::uint32_t slot = 0; slot < region_image.slot_count; ++slot) {
                        if (RegionSlotPresent(region_image, slot)) {
                            any_present = true;
                            break;
                        }
                    }
                    if (any_present) {
                        const auto serialized = SerializeRegionFileImage(geometry_, region_image);
                        AtomicWrite(data_path, serialized, strict, strict, &image_committed);
                        if (!strict) {
                            NoteUnsyncedFile(data_path);
                        }
                    } else {
                        std::error_code remove_ec;
                        std::filesystem::remove(data_path, remove_ec);
                        if (remove_ec) {
                            throw std::runtime_error(
                                "failed to remove empty region file: " + data_path.string() +
                                " (ec=" + std::to_string(remove_ec.value()) +
                                ", msg='" + remove_ec.message() + "')");
                        }
                        if (ConsumeFailpointEnv(
                                "CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_IMAGE_REMOVE_ONCE")) {
                            throw std::runtime_error(
                                "injected empty-region GC failure after image removal");
                        }
                        if (strict) {
                            SyncDirectoryPath(data_path.parent_path());
                            if (ConsumeFailpointEnv(
                                    "CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_IMAGE_DIR_SYNC_ONCE")) {
                                throw std::runtime_error(
                                    "injected empty-region GC failure after image directory sync");
                            }
                        } else {
                            NoteUnsyncedDir(data_path.parent_path());
                        }
                        image_committed = true;
                    }
                } else {
                    // No region file: the slot is already absent on disk.
                    image_committed = true;
                }
            }
            stats_empty_chunk_gcs_.fetch_add(1, std::memory_order_relaxed);
        } else if (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1) {
            const auto image = SerializeChunkImage(
                geometry_,
                chunk_coord,
                chunk->payload,
                chunk->presence_bitmap,
                checkpoint_compression_);
            if (ConsumeFailpointEnv(
                    "CHUNKDB_FAILPOINT_CHECKPOINT_BEFORE_IMAGE_REPLACE_ONCE")) {
                throw std::runtime_error(
                    "injected checkpoint failure before image replacement");
            }
            AtomicWrite(data_path, image, strict, strict, &image_committed);
            if (checkpoint_compression_ == CheckpointCompression::kZrle) {
                stats_compressed_checkpoint_images_.fetch_add(1, std::memory_order_relaxed);
            }
            if (!strict) {
                NoteUnsyncedFile(data_path);
            }
        } else {
            const auto addr = ComputeRegionChunkAddress(chunk_coord, experimental_region_span_chunks_);
            std::lock_guard region_lock(RegionIoMutex());
            RegionFileImage region_image = BuildEmptyRegionFileImage(geometry_, addr, experimental_region_span_chunks_);
            if (std::filesystem::exists(data_path)) {
                const auto region_bytes = LoadFile(data_path);
                region_image = ParseRegionFileImage(region_bytes, geometry_, addr, experimental_region_span_chunks_);
            }
            WriteRegionSlotState(
                &region_image,
                addr.slot_index,
                BuildChunkStateBytes(geometry_, chunk->payload, chunk->presence_bitmap));
            const auto serialized = SerializeRegionFileImage(geometry_, region_image);
            AtomicWrite(data_path, serialized, strict, strict, &image_committed);
            if (!strict) {
                NoteUnsyncedFile(data_path);
            }
        }

        if (ConsumeFailpointEnv(
                "CHUNKDB_FAILPOINT_CHECKPOINT_AFTER_IMAGE_REPLACE_ONCE")) {
            throw std::runtime_error(
                "injected checkpoint failure after image replacement");
        }
        PauseCheckpointBeforeWalRemovalForTests();
        CloseWalAppendStream(chunk);
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CHECKPOINT_WAL_REMOVE_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected checkpoint WAL removal failure: " + wal_path.string());
        }
        std::error_code ec;
        std::filesystem::remove(wal_path, ec);
        if (ec) {
            throw std::runtime_error(
                "failed to remove checkpointed WAL: " + wal_path.string() +
                " (ec=" + std::to_string(ec.value()) +
                ", msg='" + ec.message() + "')");
        }
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_WAL_REMOVE_ONCE")) {
            throw std::runtime_error(
                "injected empty-chunk GC failure after WAL removal");
        }
        if (strict) {
            SyncDirectoryPath(data_path.parent_path());
            if (wal_path.parent_path() != data_path.parent_path()) {
                SyncDirectoryPath(wal_path.parent_path());
            }
        } else {
            NoteUnsyncedDir(data_path.parent_path());
            if (wal_path.parent_path() != data_path.parent_path()) {
                NoteUnsyncedDir(wal_path.parent_path());
            }
        }

        if (!chunk_populated && storage_layout_mode_ == StorageLayoutMode::kFsSplitV1) {
            // Opportunistically drop the per-large-chunk directory once it is
            // empty. Losing the race against a concurrent create is fine: the
            // remove fails with directory-not-empty, or the creator retries
            // through the existing invalidate-and-recreate path.
            const auto parent = data_path.parent_path();
            std::error_code dir_ec;
            const bool dir_removed = std::filesystem::remove(parent, dir_ec);
            if (dir_ec && dir_ec != std::errc::directory_not_empty &&
                dir_ec != std::errc::no_such_file_or_directory) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kStore,
                    "failed to remove empty chunk directory",
                    {
                        {"path", parent.string()},
                        {"ec", std::to_string(dir_ec.value())},
                        {"msg", dir_ec.message()},
                    });
            }
            if (dir_removed) {
                InvalidateWalParentDirectoryCache(parent);
                if (strict) {
                    SyncDirectoryPath(data_dir_);
                } else {
                    NoteUnsyncedDir(data_dir_);
                }
            }
        }

        stats_checkpoints_.fetch_add(1, std::memory_order_relaxed);
        chunk->pending_updates = 0;
        chunk->wal_bytes = 0;
        chunk->checkpoint_due_armed = false;
        chunk->deferred_wal_compaction = false;
        chunk->pending_wal_flush_updates = 0;
        chunk->wal_batch.clear();
        chunk->wal_header_written = false;
        if (out_image_committed != nullptr) {
            *out_image_committed = image_committed;
        }
        snapshot_write.Finish();
    } catch (const std::exception& e) {
        if (out_image_committed != nullptr) {
            *out_image_committed = image_committed;
        }
        // Every checkpoint failure boundary leaves a reader-safe old/new
        // combination: the prior WAL remains until image publication is
        // complete, and a published image may coexist with that WAL. Close
        // this generation so the existing same-process retry path remains
        // available; a failure publishing the even generation still leaves
        // the store odd and fail-closed.
        snapshot_write.Finish();
        LogMessage(
            LogLevel::kError,
            LogComponent::kRecovery,
            "checkpoint failed",
            {
                {"chunk_x", std::to_string(chunk_coord.x)},
                {"chunk_y", std::to_string(chunk_coord.y)},
                {"image_committed", image_committed ? "true" : "false"},
                {"error", e.what()},
            });
        throw;
    }
}

}  // namespace chunkdb
