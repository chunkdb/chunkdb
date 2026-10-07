#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "chunk_store_internal.hpp"

namespace chunkdb {

// The writer lock of a data directory (`.chunkdb.lock/`): an exclusive OS
// lock on `writer.lock` plus `writer.meta` naming the holder, refreshed by a
// heartbeat thread. Held for the lifetime of the object.
class ProcessLock {
  public:
    // Throws when another process holds the lock or it cannot be created.
    explicit ProcessLock(const std::filesystem::path& data_dir);
    ~ProcessLock();

    ProcessLock(const ProcessLock&) = delete;
    ProcessLock& operator=(const ProcessLock&) = delete;

  private:
    [[nodiscard]] std::string BuildWriterMetadata() const;
    void WriteWriterMetadata();
    void StartWriterHeartbeat();
    void StopWriterHeartbeat() noexcept;
    void Release() noexcept;

    std::filesystem::path data_dir_;
#ifdef _WIN32
    void* lock_handle_ = nullptr;
#else
    int lock_fd_ = -1;
#endif
    std::filesystem::path lock_dir_;
    std::filesystem::path lock_file_path_;
    std::filesystem::path meta_path_;
    std::string session_id_;
    std::atomic<bool> heartbeat_stop_{false};
    std::thread heartbeat_thread_;
    mutable std::mutex meta_mutex_;
};

// The writer lock for a read-write open of `data_dir`, or nullptr when the
// open does not take one (read-only, or the single-writer guard disabled).
[[nodiscard]] std::unique_ptr<ProcessLock> AcquireWriterLock(
    const std::filesystem::path& data_dir,
    AccessMode access_mode,
    bool allow_multiple_processes);

}  // namespace chunkdb
