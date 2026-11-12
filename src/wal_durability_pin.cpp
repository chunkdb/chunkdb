#include "chunkdb/wal_durability_pin.hpp"

#include <cerrno>
#include <system_error>
#include <utility>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#if defined(__APPLE__)
#include "chunk_store_internal.hpp"
#endif
#endif

namespace chunkdb {

WalDurabilityPin::~WalDurabilityPin() { Close(); }

WalDurabilityPin::WalDurabilityPin(WalDurabilityPin&& other) noexcept
    : native_(std::exchange(other.native_, -1)) {}

WalDurabilityPin& WalDurabilityPin::operator=(WalDurabilityPin&& other) noexcept {
    if (this != &other) {
        Close();
        native_ = std::exchange(other.native_, -1);
    }
    return *this;
}

void WalDurabilityPin::Close() noexcept {
    if (!is_open()) return;
#ifdef _WIN32
    (void)CloseHandle(reinterpret_cast<HANDLE>(native_));
#else
    (void)::close(static_cast<int>(native_));
#endif
    native_ = -1;
}

void WalDurabilityPin::Sync() const {
    if (!is_open())
        throw std::system_error(EBADF, std::generic_category(), "cannot sync an empty WAL generation pin");
#ifdef _WIN32
    if (FlushFileBuffers(reinterpret_cast<HANDLE>(native_)) == 0)
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "failed to sync pinned WAL generation");
#else
    const auto fd = static_cast<int>(native_);
#if defined(__APPLE__)
    // Preserve docs/DURABILITY_CONTRACT.md and SyncFdDurably's full-sync contract.
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_FULL_SYNC_FILE_FAIL_ONCE"))
        throw std::system_error(EIO, std::generic_category(), "failed to F_FULLFSYNC pinned WAL generation");
    if (::fcntl(fd, F_FULLFSYNC, 0) == 0) return;
    const auto error = errno;
    if (error != EINVAL && error != ENOTSUP && error != ENOTTY)
        throw std::system_error(error, std::generic_category(), "failed to F_FULLFSYNC pinned WAL generation");
#endif
    if (::fsync(fd) != 0)
        throw std::system_error(errno, std::generic_category(), "failed to fsync pinned WAL generation");
#endif
}

}  // namespace chunkdb
