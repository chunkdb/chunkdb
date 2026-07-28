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

}  // namespace chunkdb
