#include "process_lock.hpp"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>

#include "checkpoint.hpp"
#include "chunkdb/logging.hpp"
#include "store_manifest.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <signal.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace chunkdb {

std::unordered_map<std::string, std::string> ParseKv(const std::string& text) {
    std::unordered_map<std::string, std::string> out;
    std::istringstream input(text);
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        const std::size_t eq = line.find('=');
        if (eq == std::string::npos || eq == 0 || eq + 1 >= line.size()) {
            continue;
        }
        out.emplace(line.substr(0, eq), line.substr(eq + 1));
    }
    return out;
}

bool TryParseInt64(const std::string& text, std::int64_t* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const long long value = std::stoll(text, &consumed, 10);
        if (consumed != text.size()) {
            return false;
        }
        *out = static_cast<std::int64_t>(value);
        return true;
    } catch (...) {
        return false;
    }
}

bool TryParseUint64(const std::string& text, std::uint64_t* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const auto value = std::stoull(text, &consumed, 10);
        if (consumed != text.size()) {
            return false;
        }
        *out = static_cast<std::uint64_t>(value);
        return true;
    } catch (...) {
        return false;
    }
}

bool IsProcessAlive(std::int64_t pid) {
#ifdef _WIN32
    HANDLE process = OpenProcess(SYNCHRONIZE, FALSE, static_cast<DWORD>(pid));
    if (process == nullptr) {
        return false;
    }
    DWORD exit_code = 0;
    const bool alive = GetExitCodeProcess(process, &exit_code) != 0 && exit_code == STILL_ACTIVE;
    CloseHandle(process);
    return alive;
#else
    if (pid <= 0) {
        return false;
    }
    if (::kill(static_cast<pid_t>(pid), 0) == 0) {
        return true;
    }
    return errno == EPERM;
#endif
}

std::string LoadTextFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        return "";
    }
    std::string out(
        (std::istreambuf_iterator<char>(input)),
        std::istreambuf_iterator<char>());
    return out;
}

bool MetadataLooksStale(const std::unordered_map<std::string, std::string>& meta) {
    const auto now_ms = UnixMillisNow();

    std::int64_t pid = -1;
    std::uint64_t heartbeat_ms = 0;
    const auto pid_it = meta.find("pid");
    const auto hb_it = meta.find("heartbeat_ms");

    if (pid_it == meta.end() || hb_it == meta.end()) {
        return true;
    }
    if (!TryParseInt64(pid_it->second, &pid) || !TryParseUint64(hb_it->second, &heartbeat_ms)) {
        return true;
    }

    const bool pid_alive = IsProcessAlive(pid);
    const bool heartbeat_stale = now_ms > heartbeat_ms && (now_ms - heartbeat_ms) > kWriterStaleThresholdMs;
    return !pid_alive || heartbeat_stale;
}

std::string GenerateSessionId() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<std::uint64_t> dist;
    std::ostringstream out;
    out << std::hex << UnixMillisNow() << "-" << dist(gen);
    return out.str();
}

[[nodiscard]] std::filesystem::path BuildLegacyLockPath(
    const std::filesystem::path& lock_path) {
    std::filesystem::path legacy =
        lock_path.parent_path() /
        (lock_path.filename().string() + ".legacy." + std::to_string(UnixMillisNow()));

    int suffix = 0;
    std::error_code exists_ec;
    while (std::filesystem::exists(legacy, exists_ec) && !exists_ec) {
        ++suffix;
        legacy =
            lock_path.parent_path() /
            (lock_path.filename().string() + ".legacy." + std::to_string(UnixMillisNow()) + "." +
             std::to_string(suffix));
    }
    return legacy;
}

void EnsureProcessLockDirectory(
    const std::filesystem::path& lock_path) {
    std::error_code status_ec;
    auto st = std::filesystem::symlink_status(lock_path, status_ec);
    if (status_ec == std::errc::no_such_file_or_directory) {
        status_ec.clear();
        st = std::filesystem::file_status(std::filesystem::file_type::not_found);
    } else if (status_ec) {
        throw std::runtime_error(
            "failed to inspect process lock path: " + lock_path.string() +
            " (error " + std::to_string(status_ec.value()) + ": " + status_ec.message() + ")");
    }

    if (!std::filesystem::exists(st)) {
        std::error_code mkdir_ec;
        std::filesystem::create_directories(lock_path, mkdir_ec);
        if (mkdir_ec) {
            throw std::runtime_error(
                "failed to create process lock directory: " + lock_path.string() +
                " (error " + std::to_string(mkdir_ec.value()) + ": " + mkdir_ec.message() + ")");
        }
        return;
    }

    if (std::filesystem::is_directory(st)) {
        return;
    }

    if (std::filesystem::is_regular_file(st)) {
        const auto legacy_path = BuildLegacyLockPath(lock_path);
        std::error_code rename_ec;
        std::filesystem::rename(lock_path, legacy_path, rename_ec);
        if (rename_ec) {
            throw std::runtime_error(
                "failed to move legacy lock file before lock bootstrap: " + lock_path.string() +
                " -> " + legacy_path.string() +
                " (error " + std::to_string(rename_ec.value()) + ": " + rename_ec.message() + ")");
        }

        std::error_code mkdir_ec;
        std::filesystem::create_directories(lock_path, mkdir_ec);
        if (mkdir_ec) {
            throw std::runtime_error(
                "failed to create process lock directory after legacy migration: " + lock_path.string() +
                " (error " + std::to_string(mkdir_ec.value()) + ": " + mkdir_ec.message() + ")");
        }

        LogMessage(
            LogLevel::kWarn,
            LogComponent::kLock,
            "legacy lock file migrated to lock directory",
            {
                {"from", lock_path.string()},
                {"to", legacy_path.string()},
            });
        return;
    }

    throw std::runtime_error(
        "unsupported process lock path type at " + lock_path.string() +
        "; expected directory. Remove or rename this path and restart.");
}
std::string ProcessLock::BuildWriterMetadata() const {
#ifdef _WIN32
    const std::uint64_t pid = static_cast<std::uint64_t>(GetCurrentProcessId());
#else
    const std::uint64_t pid = static_cast<std::uint64_t>(::getpid());
#endif

    std::ostringstream out;
    out << "version=1\n";
    out << "mode=writer\n";
    out << "session_id=" << session_id_ << "\n";
    out << "pid=" << pid << "\n";
    out << "heartbeat_ms=" << UnixMillisNow() << "\n";
    out << "stale_after_ms=" << kWriterStaleThresholdMs << "\n";
    return out.str();
}

void ProcessLock::WriteWriterMetadata() {
    if (meta_path_.empty()) {
        return;
    }

    const std::string text = BuildWriterMetadata();
    const std::vector<std::uint8_t> bytes(text.begin(), text.end());

    std::lock_guard lock(meta_mutex_);
    // The heartbeat runs on its own thread; it must not consume the generic
    // ATOMICWRITE failpoints a test has armed for a concurrent data-path write,
    // or that write silently succeeds and the test observes a missing failure.
    AtomicWrite(
        meta_path_,
        bytes,
        /*fsync_file=*/false,
        /*fsync_directory=*/false,
        /*out_replaced=*/nullptr,
        /*after_rename_failpoint=*/nullptr,
        /*enable_generic_failpoints=*/false);
}

void ProcessLock::StartWriterHeartbeat() {
    heartbeat_stop_.store(false, std::memory_order_release);
    heartbeat_thread_ = std::thread([this]() {
        while (!heartbeat_stop_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(kWriterHeartbeatIntervalMs));
            if (heartbeat_stop_.load(std::memory_order_acquire)) {
                break;
            }
            try {
                WriteWriterMetadata();
            } catch (...) {
                // Heartbeat failures are best-effort; writer ownership still relies on OS file lock.
            }
        }
    });
}

void ProcessLock::StopWriterHeartbeat() noexcept {
    heartbeat_stop_.store(true, std::memory_order_release);
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
}

std::unique_ptr<ProcessLock> AcquireWriterLock(
    const std::filesystem::path& data_dir,
    AccessMode access_mode,
    bool allow_multiple_processes) {
    if (allow_multiple_processes || access_mode == AccessMode::kReadOnly) {
        if (allow_multiple_processes) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kLock,
                "single-writer guard disabled by configuration",
                {{"allow_multi_process", "on"}});
        }
        if (access_mode == AccessMode::kReadOnly) {
            LogMessage(
                LogLevel::kInfo,
                LogComponent::kLock,
                "read-only mode; writer lock not acquired",
                {{"access_mode", AccessModeName(access_mode)}});
        }
        return nullptr;
    }
    return std::make_unique<ProcessLock>(data_dir);
}

ProcessLock::ProcessLock(const std::filesystem::path& data_dir) : data_dir_(data_dir) {
    lock_dir_ = data_dir_ / std::string(kProcessLockDirName);
    lock_file_path_ = lock_dir_ / "writer.lock";
    meta_path_ = lock_dir_ / "writer.meta";

    EnsureProcessLockDirectory(lock_dir_);

#ifdef _WIN32
    HANDLE handle = CreateFileA(
        lock_file_path_.string().c_str(),
        GENERIC_READ | GENERIC_WRITE,
        0,
        nullptr,
        OPEN_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        const std::string existing_meta = LoadTextFile(meta_path_);
        const auto parsed = ParseKv(existing_meta);
        std::string detail;
        if (!parsed.empty()) {
            const auto sid = parsed.find("session_id");
            const auto pid = parsed.find("pid");
            if (sid != parsed.end()) {
                detail += " session=" + sid->second;
            }
            if (pid != parsed.end()) {
                detail += " pid=" + pid->second;
            }
        }
        LogMessage(
            LogLevel::kError,
            LogComponent::kLock,
            "failed to acquire writer lock file",
            {
                {"path", lock_file_path_.string()},
                {"metadata", meta_path_.string()},
                {"details", detail.empty() ? "none" : detail},
            });
        throw std::runtime_error(
            "failed to acquire writer lock file:" + detail +
            " (lock_file=" + lock_file_path_.string() +
            ", metadata=" + meta_path_.string() + ")");
    }
    lock_handle_ = handle;
#else
    const int fd = ::open(lock_file_path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        LogMessage(
            LogLevel::kError,
            LogComponent::kLock,
            "failed to open writer lock file",
            {{"path", lock_file_path_.string()}});
        throw std::runtime_error(
            "failed to open writer lock file: " + lock_file_path_.string());
    }

    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        const std::string existing_meta = LoadTextFile(meta_path_);
        const auto parsed = ParseKv(existing_meta);
        std::string detail;
        if (!parsed.empty()) {
            const auto sid = parsed.find("session_id");
            const auto pid = parsed.find("pid");
            if (sid != parsed.end()) {
                detail += " session=" + sid->second;
            }
            if (pid != parsed.end()) {
                detail += " pid=" + pid->second;
            }
        }
        LogMessage(
            LogLevel::kError,
            LogComponent::kLock,
            "active writer already holds lock",
            {
                {"data_dir", data_dir_.string()},
                {"lock_file", lock_file_path_.string()},
                {"metadata", meta_path_.string()},
                {"details", detail.empty() ? "none" : detail},
            });
        ::close(fd);
        throw std::runtime_error(
            "data directory already has an active writer:" + detail +
            " (dir=" + data_dir_.string() +
            ", lock_file=" + lock_file_path_.string() +
            ", metadata=" + meta_path_.string() +
            "; stop the other writer or remove stale lock metadata only after verifying no writer is running)");
    }

    lock_fd_ = fd;
#endif

    // The OS lock is held from here on; a failure below must not leave it
    // (or the heartbeat thread) behind, since no destructor runs.
    try {
        const std::string existing_meta = LoadTextFile(meta_path_);
        if (!existing_meta.empty()) {
            const auto parsed = ParseKv(existing_meta);
            if (MetadataLooksStale(parsed)) {
                std::string stale_session = "unknown";
                std::string stale_pid = "unknown";
                const auto sid = parsed.find("session_id");
                const auto pid = parsed.find("pid");
                if (sid != parsed.end() && !sid->second.empty()) {
                    stale_session = sid->second;
                }
                if (pid != parsed.end() && !pid->second.empty()) {
                    stale_pid = pid->second;
                }
                const auto stale_path = lock_dir_ /
                                        ("writer.meta.stale." + std::to_string(UnixMillisNow()));
                std::error_code ec;
                std::filesystem::rename(meta_path_, stale_path, ec);
                if (!ec) {
                    LogMessage(
                        LogLevel::kWarn,
                        LogComponent::kLock,
                        "stale writer metadata moved for takeover recovery",
                        {
                            {"from", meta_path_.string()},
                            {"to", stale_path.string()},
                            {"stale_session", stale_session},
                            {"stale_pid", stale_pid},
                        });
                } else {
                    LogMessage(
                        LogLevel::kWarn,
                        LogComponent::kLock,
                        "stale writer metadata detected but could not be moved",
                        {
                            {"path", meta_path_.string()},
                            {"error", ec.message()},
                        });
                }
            }
        }

        session_id_ = GenerateSessionId();
        WriteWriterMetadata();
        StartWriterHeartbeat();
        LogMessage(
            LogLevel::kInfo,
            LogComponent::kLock,
            "lock acquired",
            {
                {"data_dir", data_dir_.string()},
                {"session_id", session_id_},
            });
    } catch (...) {
        Release();
        throw;
    }
}

ProcessLock::~ProcessLock() {
    Release();
}

void ProcessLock::Release() noexcept {
    const std::string session_id = session_id_;
    const bool had_lock =
        !session_id.empty() ||
#ifdef _WIN32
        lock_handle_ != nullptr;
#else
        lock_fd_ >= 0;
#endif

    StopWriterHeartbeat();

    if (!meta_path_.empty()) {
        std::error_code ec;
        std::filesystem::remove(meta_path_, ec);
        if (ec) {
            try {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kLock,
                    "failed to remove writer metadata on shutdown",
                    {
                        {"path", meta_path_.string()},
                        {"error", ec.message()},
                    });
            } catch (...) {
            }
        }
    }

#ifdef _WIN32
    if (lock_handle_ != nullptr) {
        const HANDLE handle = static_cast<HANDLE>(lock_handle_);
        if (CloseHandle(handle) == 0) {
            const DWORD code = GetLastError();
            try {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kLock,
                    "failed to release writer lock handle",
                    {{"error", std::system_category().message(static_cast<int>(code))}});
            } catch (...) {
            }
        }
        lock_handle_ = nullptr;
    }
#else
    if (lock_fd_ >= 0) {
        (void)::flock(lock_fd_, LOCK_UN);
        (void)::close(lock_fd_);
        lock_fd_ = -1;
    }
#endif

    if (had_lock) {
        try {
            LogMessage(
                LogLevel::kInfo,
                LogComponent::kLock,
                "lock release result",
                {
                    {"session_id", session_id.empty() ? "unknown" : session_id},
                    {"result", "released"},
                });
        } catch (...) {
        }
    }

    session_id_.clear();
    meta_path_.clear();
    lock_file_path_.clear();
    lock_dir_.clear();
}

void ChunkStore::AcquireProcessLock(bool allow_multiple_processes) {
    if (!acquire_process_lock_) {
        // The data directory this store belongs to is locked by its owner.
        return;
    }
    // A table of a data directory is locked with the data directory, whose
    // lock a running server holds: a store opened directly on one of its
    // tables must not write beside it.
    std::filesystem::path lock_dir = data_dir_.lexically_normal();
    if (lock_dir.filename().empty()) {
        lock_dir = lock_dir.parent_path();
    }
    const auto parent = lock_dir.parent_path();
    std::error_code ec;
    if (parent.filename() == "tables" &&
        std::filesystem::exists(DataDirManifestPath(parent.parent_path()), ec)) {
        lock_dir = parent.parent_path();
    }
    process_lock_ = AcquireWriterLock(lock_dir, access_mode_, allow_multiple_processes);
}

void ChunkStore::ReleaseProcessLock() noexcept {
    process_lock_.reset();
}

}  // namespace chunkdb
