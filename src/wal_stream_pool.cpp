#include "wal_stream_pool.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>

#include "checkpoint.hpp"
#include "file_permissions.hpp"
#include "chunkdb/file_layout.hpp"
#include "txn_history.hpp"
#include "wal_writer.hpp"

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#endif

namespace chunkdb {

namespace {
std::atomic<std::size_t> g_open_wal_append_streams{0};
std::atomic<std::size_t> g_peak_open_wal_append_streams{0};
}  // namespace

void WalAppendStream::NoteOpened() noexcept {
    const std::size_t open = g_open_wal_append_streams.fetch_add(1, std::memory_order_acq_rel) + 1U;
    std::size_t peak = g_peak_open_wal_append_streams.load(std::memory_order_relaxed);
    while (open > peak &&
           !g_peak_open_wal_append_streams.compare_exchange_weak(peak, open, std::memory_order_relaxed)) {
    }
}

void WalAppendStream::NoteClosed() noexcept {
    g_open_wal_append_streams.fetch_sub(1, std::memory_order_acq_rel);
}

std::size_t WalAppendStream::OpenCountForTests() noexcept {
    return g_open_wal_append_streams.load(std::memory_order_acquire);
}

std::size_t WalAppendStream::PeakOpenCountForTests() noexcept {
    return g_peak_open_wal_append_streams.load(std::memory_order_relaxed);
}

void WalAppendStream::ResetPeakOpenCountForTests() noexcept {
    g_peak_open_wal_append_streams.store(
        g_open_wal_append_streams.load(std::memory_order_acquire), std::memory_order_relaxed);
}

#ifdef _WIN32
namespace {
void WalStreamErrno(DWORD code) noexcept {
    switch (code) {
        case ERROR_FILE_NOT_FOUND:
        case ERROR_PATH_NOT_FOUND: errno = ENOENT; break;
        case ERROR_ACCESS_DENIED:
        case ERROR_SHARING_VIOLATION: errno = EACCES; break;
        case ERROR_DISK_FULL:
        case ERROR_HANDLE_DISK_FULL: errno = ENOSPC; break;
        case ERROR_TOO_MANY_OPEN_FILES: errno = EMFILE; break;
        default: errno = EIO; break;
    }
}
}

WalAppendStream::WalAppendStream(const std::filesystem::path& path, std::ios_base::openmode mode) {
    open(path, mode);
}
WalAppendStream::~WalAppendStream() { close(); }

void WalAppendStream::open(const std::filesystem::path& path, std::ios_base::openmode mode, bool existing) {
    // OPEN_ALWAYS creates or opens in one call either way.
    (void)existing;
    if (is_open() || mode != (std::ios::binary | std::ios::app)) {
        good_ = false;
        errno = EINVAL;
        return;
    }
    OwnerOnlySecurity security(path);
    const auto handle = CreateFileW(path.c_str(), FILE_APPEND_DATA,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, security.attributes(),
        OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        WalStreamErrno(GetLastError());
        good_ = false;
        return;
    }
    handle_ = handle;
    good_ = true;
    NoteOpened();
}

void WalAppendStream::close() noexcept {
    if (handle_ != nullptr) {
        if (CloseHandle(handle_) == 0) {
            WalStreamErrno(GetLastError());
            good_ = false;
        }
        handle_ = nullptr;
        NoteClosed();
    }
}

WalAppendStream& WalAppendStream::write(const char* bytes, std::streamsize size) {
    if (!is_open() || !good_ || size < 0) {
        good_ = false;
        errno = EIO;
        return *this;
    }
    auto remaining = static_cast<std::size_t>(size);
    while (remaining > 0U) {
        const auto length = static_cast<DWORD>(std::min(remaining,
            static_cast<std::size_t>(std::numeric_limits<DWORD>::max())));
        DWORD written = 0;
        if (WriteFile(handle_, bytes, length, &written, nullptr) == 0) {
            WalStreamErrno(GetLastError());
            good_ = false;
            break;
        }
        if (written == 0U) {
            errno = EIO;
            good_ = false;
            break;
        }
        bytes += written;
        remaining -= written;
    }
    return *this;
}

WalAppendStream& WalAppendStream::flush() noexcept { return *this; }
#else
WalAppendStream::WalAppendStream(const std::filesystem::path& path, std::ios_base::openmode mode) {
    open(path, mode);
}
WalAppendStream::~WalAppendStream() { close(); }

void WalAppendStream::open(const std::filesystem::path& path, std::ios_base::openmode mode, bool existing) {
    if (is_open() || mode != (std::ios::binary | std::ios::app)) {
        good_ = false;
        errno = EINVAL;
        return;
    }
    constexpr int kFlags = O_WRONLY | O_APPEND | O_NOFOLLOW | O_CLOEXEC;
    // A file expected to exist is opened as it is; only when it is missing
    // does the owner-only create run.
    int fd = existing ? ::open(path.c_str(), kFlags) : -1;
    if (fd < 0 && (!existing || errno == ENOENT)) {
        fd = OpenOwnerOnlyFile(path, kFlags);
    }
    if (fd < 0) {
        good_ = false;
        return;
    }
    handle_ = ::fdopen(fd, "ab");
    if (handle_ == nullptr) {
        const int error = errno;
        (void)::close(fd);
        errno = error;
        good_ = false;
        return;
    }
    good_ = true;
    NoteOpened();
}

void WalAppendStream::close() noexcept {
    if (handle_ != nullptr) {
        if (::fclose(static_cast<FILE*>(handle_)) != 0) good_ = false;
        handle_ = nullptr;
        NoteClosed();
    }
}

WalAppendStream& WalAppendStream::write(const char* bytes, std::streamsize size) {
    if (!is_open() || !good_ || size < 0) {
        good_ = false;
        errno = EIO;
        return *this;
    }
    const auto count = static_cast<std::size_t>(size);
    if (::fwrite(bytes, 1, count, static_cast<FILE*>(handle_)) != count) good_ = false;
    return *this;
}

WalAppendStream& WalAppendStream::flush() noexcept {
    if (!is_open() || ::fflush(static_cast<FILE*>(handle_)) != 0) good_ = false;
    return *this;
}
#endif

void ChunkStore::EnsureWalParentDirectoryCached(
    const std::filesystem::path& wal_parent_path,
    bool force_refresh,
    bool durable_sync) {
    const std::string key = CanonicalPathKey(wal_parent_path);
    {
        std::unique_lock lock(wal_parent_cache_mutex_);
        if (!force_refresh && wal_parent_dir_cache_.contains(key)) {
            return;
        }
        // A directory another thread is preparing may exist before its entry
        // is durable: wait for that thread rather than take it as prepared.
        if (wal_parent_dir_preparing_.contains(key)) {
            ++wal_parent_dir_waiters_;
            wal_parent_cache_cv_.wait(lock, [&] { return !wal_parent_dir_preparing_.contains(key); });
            --wal_parent_dir_waiters_;
        }
        if (!force_refresh && wal_parent_dir_cache_.contains(key)) {
            return;
        }
        wal_parent_dir_preparing_.insert(key);
    }
    const auto finish = [&](bool prepared) noexcept {
        {
            std::lock_guard lock(wal_parent_cache_mutex_);
            wal_parent_dir_preparing_.erase(key);
            if (prepared) {
                wal_parent_dir_cache_.insert(key);
            }
        }
        wal_parent_cache_cv_.notify_all();
    };

    try {
        EnsureDirectoryPathExists(wal_parent_path, durable_sync);
        PauseWalDirPrepareForTests();
    } catch (...) {
        finish(false);
        throw;
    }

    stats_wal_parent_prepare_calls_.fetch_add(1, std::memory_order_relaxed);
    finish(true);
}

void ChunkStore::InvalidateWalParentDirectoryCache(const std::filesystem::path& wal_parent_path) {
    const std::string key = CanonicalPathKey(wal_parent_path);
    std::lock_guard lock(wal_parent_cache_mutex_);
    wal_parent_dir_cache_.erase(key);
}

void ChunkStore::EnsureWalAppendStream(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool* first_create,
    bool* artifact_touched) {
    if (first_create != nullptr) {
        *first_create = false;
    }
    if (artifact_touched != nullptr) {
        *artifact_touched = false;
    }

    if (chunk->wal_stream_initialized.load(std::memory_order_acquire) && WalAppendStreamOpen(*chunk)) {
        TouchWalStreamState(chunk);
        return;
    }

    // The caller holds the chunk's mutex, so no other thread opens or closes
    // this chunk's stream. Every filesystem call below runs without a lock
    // shared by other chunks: the pool's mutex is held only to reserve a
    // place, and a stream closed to make room is closed after it is released.
    if (chunk->wal_path.empty()) {
        chunk->wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);
    }
    const auto& wal_path = chunk->wal_path;
    const auto wal_parent_path = wal_path.parent_path();
    EnsureWalParentDirectoryCached(
        wal_parent_path,
        false,
        durability_mode_ != DurabilityMode::kRelaxed);

    ReserveWalStreamSlot(chunk);
    // Until the stream is registered, a failure closes whatever was opened
    // and only then gives the reserved place back, so the pool neither leaks
    // the place nor lets another chunk open a file while this one is open.
    bool registered = false;
    struct ReleaseOnFailure {
        ChunkStore* store;
        const std::shared_ptr<RegularChunk>& chunk;
        const bool& registered;
        ~ReleaseOnFailure() {
            if (!registered) {
                store->CloseWalAppendStream(chunk);
            }
        }
    } release{this, chunk, registered};

    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_OPEN_ONCE")) {
        throw std::runtime_error("injected WAL open failure: " + wal_path.string());
    }

    const bool needs_header = !chunk->wal_header_written;

    // The stream object (and the ~4 KiB buffer its filebuf allocates in the
    // constructor) exists only while the chunk holds an open stream. Every
    // exit below either leaves it open or releases it again.
    if (chunk->wal_append_stream == nullptr) {
        chunk->wal_append_stream = std::make_unique<WalAppendStream>();
    }
    WalAppendStream& stream = *chunk->wal_append_stream;

    stream.clear();
    if (artifact_touched != nullptr) {
        *artifact_touched = true;
    }
    PauseWalOpenForTests(chunk_coord);
    // A WAL with its header written exists, so it is opened as it is.
    stream.open(wal_path, std::ios::binary | std::ios::app, !needs_header);
    if (!stream.is_open()) {
        int open_err = errno;
        InvalidateWalParentDirectoryCache(wal_parent_path);
        EnsureWalParentDirectoryCached(
            wal_parent_path,
            true,
            durability_mode_ != DurabilityMode::kRelaxed);
        stream.clear();
        stream.open(wal_path, std::ios::binary | std::ios::app);
        if (!stream.is_open()) {
            open_err = errno;
            // Keep wal_stream_initialized a faithful mirror of "this chunk owns
            // an open stream" before releasing the object: the stream cache
            // scans that flag for other chunks holding only its own mutex
            // alone, and must never be told to look at a stream that is gone.
            chunk->wal_stream_initialized.store(false, std::memory_order_release);
            chunk->wal_append_stream.reset();
            throw BuildWalOpenError(wal_path, open_err);
        }
    }

    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_AFTER_OPEN_FAIL_ONCE")) {
        throw std::runtime_error("injected WAL failure after open: " + wal_path.string());
    }

    if (needs_header) {
        const auto wal_header = BuildWalHeader(chunk_coord, store_id_, features_);
        // The failpoint tears the header: half of it reaches the file.
        const bool torn = ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_HEADER_WRITE_FAIL_ONCE");
        stream.write(
            reinterpret_cast<const char*>(wal_header.data()),
            static_cast<std::streamsize>(torn ? wal_header.size() / 2U : wal_header.size()));
        stream.flush();
        if (!stream.good() || torn) {
            stream.close();
            chunk->wal_stream_initialized.store(false, std::memory_order_release);
            chunk->wal_append_stream.reset();
            throw std::runtime_error("failed to append WAL header: " + wal_path.string());
        }
        if (first_create != nullptr) {
            *first_create = true;
        }
        chunk->wal_header_written = true;
    }

    chunk->wal_stream_initialized.store(true, std::memory_order_release);
    stats_wal_open_count_.fetch_add(1, std::memory_order_relaxed);
    bool wake = false;
    {
        std::lock_guard lock(resources_->wal_stream_mutex_);
        auto& lru = resources_->wal_stream_lru_;
        const auto existing = resources_->open_wal_streams_.find(chunk.get());
        ++resources_->wal_stream_progress_;
        wake = resources_->wal_stream_waiters_ > 0U;
        if (existing != resources_->open_wal_streams_.end()) {
            lru.splice(lru.end(), lru, existing->second.lru_position);
            existing->second.chunk = chunk;
            existing->second.owner = this;
            if (existing->second.opening) {
                existing->second.opening = false;
                --resources_->wal_stream_opening_;
            }
        } else {
            lru.push_back(chunk.get());
            resources_->open_wal_streams_.emplace(
                chunk.get(),
                StoreResources::WalStreamState{
                    .chunk = chunk,
                    .owner = this,
                    .lru_position = std::prev(lru.end()),
                });
        }
    }
    registered = true;
    if (wake) {
        resources_->wal_stream_cv_.notify_all();
    }
}

void ChunkStore::CloseWalAppendStream(const std::shared_ptr<RegularChunk>& chunk) noexcept {
    if (chunk == nullptr) {
        return;
    }
    if (chunk->wal_append_stream != nullptr) {
        if (chunk->wal_append_stream->is_open()) {
            chunk->wal_append_stream->flush();
            chunk->wal_append_stream->close();
        }
        // Release the stream object and its filebuf buffer: an idle resident
        // chunk must not keep several KiB of stdio buffer alive.
        chunk->wal_append_stream.reset();
    }
    chunk->wal_stream_initialized.store(false, std::memory_order_release);
    {
        std::lock_guard lock(resources_->wal_stream_mutex_);
        const auto entry = resources_->open_wal_streams_.find(chunk.get());
        if (entry != resources_->open_wal_streams_.end()) {
            resources_->EraseWalStreamLocked(entry);
        }
    }
    resources_->wal_stream_cv_.notify_all();
}

void ChunkStore::CloseAllWalStreams() noexcept {
    std::vector<std::shared_ptr<LargeChunk>> large_chunks;
    {
        std::lock_guard lock(large_chunks_mutex_);
        large_chunks.reserve(large_chunks_.size());
        for (const auto& [_, large_chunk] : large_chunks_) {
            large_chunks.push_back(large_chunk);
        }
    }
    for (const auto& large_chunk : large_chunks) {
        std::vector<std::shared_ptr<RegularChunk>> chunks;
        {
            std::lock_guard lock(large_chunk->mutex);
            chunks.reserve(large_chunk->chunks.size());
            for (const auto& [_, chunk] : large_chunk->chunks) {
                chunks.push_back(chunk);
            }
        }
        for (const auto& chunk : chunks) {
            std::unique_lock chunk_lock(chunk->mutex);
            CloseWalAppendStream(chunk);
        }
    }
}

std::shared_ptr<ChunkStore::RegularChunk> ChunkStore::TakeIdleWalStreamLocked(
    const std::shared_ptr<RegularChunk>& opening_chunk) {
    // Any store's idle stream will do: the pool is shared. The search starts
    // at the least recently used entry, or where the last search stopped,
    // and visits a bounded number of entries, so it never holds the pool's
    // mutex long enough to delay appends. Entries whose chunk is gone are
    // dropped on the way, and places still being opened are skipped.
    auto& open_wal_streams = resources_->open_wal_streams_;
    auto& lru = resources_->wal_stream_lru_;
    auto position = lru.begin();
    if (resources_->wal_stream_scan_resume_ != nullptr) {
        const auto resume = open_wal_streams.find(resources_->wal_stream_scan_resume_);
        if (resume != open_wal_streams.end()) {
            position = resume->second.lru_position;
        }
        resources_->wal_stream_scan_resume_ = nullptr;
    }
    for (std::size_t budget = std::min(kWalStreamScanBudget, lru.size()); budget > 0 && !lru.empty(); --budget) {
        if (position == lru.end()) {
            position = lru.begin();
        }
        const auto entry = open_wal_streams.find(*position);
        ++position;
        if (entry->second.opening) {
            continue;
        }
        auto current = entry->second.chunk.lock();
        if (!current || !current->wal_stream_initialized.load(std::memory_order_acquire)) {
            resources_->EraseWalStreamLocked(entry);
            continue;
        }
        // try_lock on a mutex this thread already owns is undefined: skip
        // the opening chunk and every chunk a commit of this thread holds.
        // try_lock never waits, so trying under the pool's mutex is safe.
        if (current == opening_chunk || ThreadHoldsChunkLock(current.get()) || !current->mutex.try_lock()) {
            continue;
        }
        resources_->EraseWalStreamLocked(entry);
        return current;
    }
    if (position != lru.end()) {
        resources_->wal_stream_scan_resume_ = *position;
    }
    return nullptr;
}

void ChunkStore::ReserveWalStreamSlot(const std::shared_ptr<RegularChunk>& opening_chunk) {
    StoreResources& pool = *resources_;
    const std::size_t max_open_wal_streams = pool.max_open_wal_streams_;
    auto& open_wal_streams = pool.open_wal_streams_;
    auto& lru = pool.wal_stream_lru_;
    // Under the pool's mutex: records the place as being opened. Entries
    // whose chunk is gone still count until a search drops them, so the cap
    // is never exceeded, only reached early.
    const auto reserve_locked = [&]() {
        const auto existing = open_wal_streams.find(opening_chunk.get());
        if (existing != open_wal_streams.end()) {
            if (!existing->second.opening) {
                existing->second.opening = true;
                ++pool.wal_stream_opening_;
            }
            return;
        }
        lru.push_back(opening_chunk.get());
        open_wal_streams.emplace(
            opening_chunk.get(),
            StoreResources::WalStreamState{
                .chunk = opening_chunk,
                .owner = this,
                .lru_position = std::prev(lru.end()),
                .opening = true,
            });
        ++pool.wal_stream_opening_;
    };

    std::unique_lock lock(pool.wal_stream_mutex_);
    // A chunk that still has an entry already holds its place.
    if (open_wal_streams.contains(opening_chunk.get())) {
        reserve_locked();
        return;
    }
    const std::uint64_t ticket = pool.wal_stream_next_ticket_++;
    // Passes the turn to the next opener still waiting.
    const auto end_turn_locked = [&]() {
        ++pool.wal_stream_serving_ticket_;
        while (pool.wal_stream_abandoned_tickets_.erase(pool.wal_stream_serving_ticket_) != 0U) {
            ++pool.wal_stream_serving_ticket_;
        }
    };

    std::uint64_t progress = pool.wal_stream_progress_;
    auto deadline = std::chrono::steady_clock::now() + kWalStreamCapacityWaitTimeout;
    while (true) {
        const bool served = ticket == pool.wal_stream_serving_ticket_;
        if (served) {
            std::shared_ptr<RegularChunk> victim;
            if (open_wal_streams.size() >= max_open_wal_streams) {
                victim = TakeIdleWalStreamLocked(opening_chunk);
            }
            // The search may also have dropped entries of chunks that are gone.
            if (victim != nullptr || open_wal_streams.size() < max_open_wal_streams) {
                // A victim's place becomes this chunk's. Its stream is still
                // open until closed below, but this thread opens nothing
                // before that, so the open files never exceed the cap.
                reserve_locked();
                end_turn_locked();
                const bool wake = pool.wal_stream_waiters_ > 0U;
                lock.unlock();
                if (wake) {
                    pool.wal_stream_cv_.notify_all();
                }
                if (victim != nullptr) {
                    std::unique_lock<RegularChunkMutex> victim_lock(victim->mutex, std::adopt_lock);
                    CloseWalAppendStream(victim);
                }
                return;
            }
        }

        // Give up only after a whole timeout in which no place was freed, no
        // stream registered and no stream was being opened: a slow open or a
        // long queue that keeps moving is not a failure.
        const auto now = std::chrono::steady_clock::now();
        if (pool.wal_stream_progress_ != progress || pool.wal_stream_opening_ > 0U) {
            progress = pool.wal_stream_progress_;
            deadline = now + kWalStreamCapacityWaitTimeout;
        } else if (now >= deadline) {
            break;
        }
        // Freed places, registrations and turns are signalled; a stream that
        // merely stops being busy is not, so the opener being served also
        // looks again on a short interval.
        auto wait = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now) + std::chrono::milliseconds(1);
        if (served) {
            wait = std::min(wait, std::chrono::duration_cast<std::chrono::milliseconds>(kWalStreamCapacityRetryInterval));
        }
        ++pool.wal_stream_waiters_;
        pool.wal_stream_cv_.wait_for(lock, wait);
        --pool.wal_stream_waiters_;
    }

    if (ticket == pool.wal_stream_serving_ticket_) {
        end_turn_locked();
    } else {
        pool.wal_stream_abandoned_tickets_.insert(ticket);
    }
    const std::size_t open_count = open_wal_streams.size();
    lock.unlock();
    pool.wal_stream_cv_.notify_all();
    throw std::runtime_error(
        "timed out waiting for WAL stream capacity"
        " (cap=" + std::to_string(max_open_wal_streams) +
        ", open=" + std::to_string(open_count) + ")");
}

void ChunkStore::TouchWalStreamState(const std::shared_ptr<RegularChunk>& chunk) noexcept {
    if (chunk == nullptr) {
        return;
    }
    std::lock_guard lock(resources_->wal_stream_mutex_);
    auto it = resources_->open_wal_streams_.find(chunk.get());
    if (it == resources_->open_wal_streams_.end()) {
        return;
    }
    auto& lru = resources_->wal_stream_lru_;
    lru.splice(lru.end(), lru, it->second.lru_position);
}

}  // namespace chunkdb
