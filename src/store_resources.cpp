#include "chunkdb/chunk_store.hpp"

#include <algorithm>
#include <mutex>
#include <stdexcept>
#include <string>

#include "chunk_store_internal.hpp"
#include "chunkdb/logging.hpp"

#ifdef _WIN32
#include <stdio.h>
#else
#include <sys/resource.h>
#endif

namespace chunkdb {

StoreResources::StoreResources(std::size_t max_loaded_chunks, std::size_t max_open_wal_streams)
    : max_loaded_chunks_(max_loaded_chunks),
      max_open_wal_streams_(max_open_wal_streams) {
    if (max_loaded_chunks_ == 0) {
        throw std::invalid_argument("max_loaded_chunks must be > 0");
    }
    if (max_open_wal_streams_ == 0) {
        throw std::invalid_argument("max_open_wal_streams must be > 0");
    }

#ifndef _WIN32
    {
        struct rlimit limit {};
        if (getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur != RLIM_INFINITY) {
            const std::size_t soft_limit = static_cast<std::size_t>(limit.rlim_cur);
            const std::size_t clamped =
                soft_limit > kWalOpenStreamsFdReserve
                    ? (soft_limit - kWalOpenStreamsFdReserve)
                    : 1U;
            if (max_open_wal_streams_ > clamped) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kStore,
                    "max_open_wal_streams clamped by RLIMIT_NOFILE reserve",
                    {
                        {"configured", std::to_string(max_open_wal_streams_)},
                        {"effective", std::to_string(clamped)},
                        {"rlimit_nofile_soft", std::to_string(soft_limit)},
                        {"reserve", std::to_string(kWalOpenStreamsFdReserve)},
                    });
                max_open_wal_streams_ = clamped;
            }
        }
    }
#else
    {
        // Windows has no RLIMIT_NOFILE, but the C runtime caps the number of
        // simultaneously open stdio streams (_getmaxstdio, default 512). The
        // WAL stream pool can keep up to max_open_wal_streams files open, so
        // without this an open-heavy workload hits EMFILE ("Too many open
        // files"). Raise the CRT limit toward its maximum, then clamp the WAL
        // pool to the effective limit minus a reserve for other handles.
        constexpr int kWindowsStdioTarget = 8192;  // CRT hard maximum
        if (_getmaxstdio() < kWindowsStdioTarget) {
            (void)_setmaxstdio(kWindowsStdioTarget);
        }
        const int effective_stdio = _getmaxstdio();
        if (effective_stdio > 0) {
            const std::size_t budget = static_cast<std::size_t>(effective_stdio);
            const std::size_t clamped =
                budget > kWalOpenStreamsFdReserve
                    ? (budget - kWalOpenStreamsFdReserve)
                    : 1U;
            if (max_open_wal_streams_ > clamped) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kStore,
                    "max_open_wal_streams clamped by Windows CRT stdio limit",
                    {
                        {"configured", std::to_string(max_open_wal_streams_)},
                        {"effective", std::to_string(clamped)},
                        {"crt_maxstdio", std::to_string(effective_stdio)},
                        {"reserve", std::to_string(kWalOpenStreamsFdReserve)},
                    });
                max_open_wal_streams_ = clamped;
            }
        }
    }
#endif

}

std::size_t StoreResources::OpenWalStreamCount() const {
    std::lock_guard lock(wal_stream_mutex_);
    return open_wal_streams_.size();
}

void StoreResources::RegisterStore(ChunkStore* store) {
    std::unique_lock lock(stores_mutex_);
    stores_.push_back(store);
}

void StoreResources::UnregisterStore(ChunkStore* store) noexcept {
    {
        // Waits for running eviction passes, which hold the lock shared.
        std::unique_lock lock(stores_mutex_);
        const auto it = std::find(stores_.begin(), stores_.end(), store);
        if (it != stores_.end()) {
            stores_.erase(it);
        }
    }
    loaded_chunks_.fetch_sub(
        store->loaded_chunk_count_.exchange(0, std::memory_order_relaxed),
        std::memory_order_relaxed);
}

void StoreResources::EraseWalStreamLocked(
    std::unordered_map<ChunkStore::RegularChunk*, WalStreamState>::iterator entry) noexcept {
    wal_stream_lru_.erase(entry->second.lru_position);
    open_wal_streams_.erase(entry);
}

void StoreResources::ForgetWalStreams(const ChunkStore* store) noexcept {
    {
        std::lock_guard lock(wal_stream_mutex_);
        for (auto it = open_wal_streams_.begin(); it != open_wal_streams_.end();) {
            const auto next = std::next(it);
            if (it->second.owner == store) {
                EraseWalStreamLocked(it);
            }
            it = next;
        }
    }
    wal_stream_cv_.notify_all();
}

}  // namespace chunkdb
