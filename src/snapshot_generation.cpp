#include "chunkdb/chunk_store.hpp"

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "eviction.hpp"
#include "process_lock.hpp"
#include "wal_replay.hpp"
#include "wal_stream_pool.hpp"
#include "wal_writer.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <vector>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/zrle.hpp"

#include "snapshot_generation.hpp"
#include "txn_history.hpp"
#include "file_permissions.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace chunkdb {

namespace {

// Immediate (sleep-free) attempts. A writer bracket that is merely being
// raced normally resolves inside these.
constexpr std::size_t kReadOnlySnapshotSpinAttempts = 8;
// After the spins, keep retrying with exponential backoff until this much
// wall time has been spent sleeping. Only active artifact publications hold
// an odd epoch; completed transitions publish even on their own thread.
constexpr std::uint64_t kReadOnlySnapshotBackoffBudgetMs = 250;
constexpr std::uint64_t kReadOnlySnapshotBackoffStartUs = 250;
constexpr std::uint64_t kReadOnlySnapshotBackoffMaxUs = 20'000;
constexpr std::string_view kSnapshotGenerationFile = "chunkdb.snapshot";
constexpr std::array<std::uint8_t, 4> kSnapshotGenerationMagic = {
    'C', 'K', 'S', 'G'};
constexpr std::size_t kSnapshotGenerationRecordSize = 16;
thread_local std::vector<ChunkStore*> g_snapshot_write_stack;

// A snapshot artifact that exists but cannot be read *right now*. On Windows
// an atomic replace makes the target briefly unopenable (sharing violation)
// even though nothing is damaged, so this is namespace instability like a
// vanished path, not proof of corruption. The bracket protocol retries it
// within its bounded budget and still fails closed once the budget is spent.
struct SnapshotArtifactUnstable : std::runtime_error {
    using std::runtime_error::runtime_error;
};

constexpr std::string_view kSnapshotLiveFile = "chunkdb.snapshot.live";
constexpr std::string_view kSnapshotWriterFile = "snapshot.writer";
constexpr std::string_view kSnapshotOwnerFile = "snapshot.owner";
constexpr std::size_t kLiveRecordSize = 32;
constexpr std::size_t kLiveCounterOffset = 24;
constexpr std::uint64_t kGenerationOdd = 1U;
constexpr std::uint64_t kGenerationFailed = 2U;
constexpr std::uint64_t kGenerationCountOne = 4U;
constexpr std::uint64_t kGenerationEpochStep = 1ULL << 18U;
constexpr std::uint64_t kGenerationCountMask = kGenerationEpochStep - 4U;

void RequireMappedAtomics() {
    if (!std::atomic_ref<std::uint64_t>::is_always_lock_free ||
        std::atomic_ref<std::uint64_t>::required_alignment > 8U ||
        std::endian::native != std::endian::little) {
        throw std::runtime_error("snapshot generation requires lock-free aligned little-endian 64-bit mapped atomics");
    }
}

[[nodiscard]] std::filesystem::path SnapshotLeasePath(
    const std::filesystem::path& directory, bool writer) {
    const auto lock_directory = directory / ".chunkdb.lock";
    if (writer) {
        (void)CreateOwnerOnlyDirectory(lock_directory);
    } else {
        std::error_code error;
        const auto status = std::filesystem::symlink_status(lock_directory, error);
        if (error != std::errc::no_such_file_or_directory &&
            (error || (std::filesystem::exists(status) && !std::filesystem::is_directory(status)))) {
            throw std::runtime_error("snapshot writer lease directory is unavailable or not a directory");
        }
    }
    return lock_directory / kSnapshotWriterFile;
}

// An OS lock, not a PID/heartbeat heuristic. Separate from the replaced live
// mapping so a crash/restart can never detach ownership from its lock inode.
//
// A reader probes the lease with a shared lock it drops at once, never
// waiting while it holds it. A writer must not mistake such a probe for
// another writer, so writers exclude each other on the owner file, which
// readers never open (kOwner). Holding it, the writer tests the lease with a
// shared lock, which only an exclusive holder refuses, and then waits for
// the exclusive lock (kWriter): the wait ends when the probes held at that
// moment are dropped, so a reader can delay a writer's start but not stop it,
// unless it stays inside its probe past the bound of WaitForExclusive.
class SnapshotWriterLease {
  public:
    enum class Role { kReader, kOwner, kWriter };
    SnapshotWriterLease(const std::filesystem::path& path, Role role) {
        const bool writer = role != Role::kReader;
#ifdef _WIN32
        OwnerOnlySecurity security(path);
        handle_ = CreateFileW(path.c_str(), writer ? GENERIC_READ | GENERIC_WRITE : GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            writer ? security.attributes() : nullptr, writer ? OPEN_ALWAYS : OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
        if (handle_ == INVALID_HANDLE_VALUE) {
            const auto error = GetLastError();
            if (!writer && (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND)) return;
            throw std::system_error(static_cast<int>(error), std::system_category(), "open snapshot writer lease");
        }
        BY_HANDLE_FILE_INFORMATION info{};
        if (!GetFileInformationByHandle(handle_, &info) ||
            (info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) != 0U) {
            CloseHandle(handle_); handle_ = INVALID_HANDLE_VALUE;
            throw std::runtime_error("snapshot writer lease is not a regular file");
        }
        OVERLAPPED overlap{};
        const DWORD flags = LOCKFILE_FAIL_IMMEDIATELY | (role == Role::kOwner ? LOCKFILE_EXCLUSIVE_LOCK : 0U);
        if (LockFileEx(handle_, flags, 0, 1, 0, &overlap)) {
            locked_ = true;
        } else {
            const auto error = GetLastError();
            if (error != ERROR_LOCK_VIOLATION) {
                CloseHandle(handle_); handle_ = INVALID_HANDLE_VALUE;
                throw std::system_error(static_cast<int>(error), std::system_category(), "lock snapshot writer lease");
            }
            writer_present_ = true;
        }
        if (role == Role::kWriter && locked_) {
            OVERLAPPED shared{};
            (void)UnlockFileEx(handle_, 0, 1, 0, &shared);
            locked_ = false;
            WaitForExclusive(path);
        }
#else
        fd_ = writer ? OpenOwnerOnlyFile(path, O_RDWR | O_CLOEXEC)
                     : ::open(path.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK);
        if (fd_ < 0) {
            if (!writer && errno == ENOENT) return;
            throw std::system_error(errno, std::generic_category(), "open snapshot writer lease");
        }
        struct stat info{};
        if (::fstat(fd_, &info) != 0 || !S_ISREG(info.st_mode)) {
            ::close(fd_); fd_ = -1;
            throw std::runtime_error("snapshot writer lease is not a regular file");
        }
        if (::flock(fd_, (role == Role::kOwner ? LOCK_EX : LOCK_SH) | LOCK_NB) == 0) {
            locked_ = true;
        } else {
            const int error = errno;
            if (error != EWOULDBLOCK && error != EAGAIN) {
                ::close(fd_); fd_ = -1;
                throw std::system_error(error, std::generic_category(), "lock snapshot writer lease");
            }
            writer_present_ = true;
        }
        if (role == Role::kWriter && locked_) {
            (void)::flock(fd_, LOCK_UN);
            locked_ = false;
            WaitForExclusive(path);
        }
#endif
        if (writer && writer_present_) {
            Close();
            throw std::runtime_error("snapshot generation requires a single active writer per table");
        }
    }
    ~SnapshotWriterLease() { Close(); }
    [[nodiscard]] bool WriterPresent() const { return writer_present_; }
    SnapshotWriterLease(const SnapshotWriterLease&) = delete;
    SnapshotWriterLease& operator=(const SnapshotWriterLease&) = delete;
  private:
    // The shared test lock is dropped before the exclusive one is taken: no
    // lock converts in place on every platform. Writers of this version
    // cannot take the lease in between, because each holds snapshot.owner
    // first. A writer of an older version takes the lease without the owner
    // file and can; this writer then waits for it, fails at the bound and
    // leaves it the table, so the two never write together.
    //
    // Only readers' probes, dropped as soon as they are looked at, hold the
    // lease while this writer waits. A reader stopped inside its probe would
    // hold it for good, so the wait is bounded and names that cause.
    void WaitForExclusive(const std::filesystem::path& path) {
        constexpr auto kLimit = std::chrono::seconds(5);
        constexpr auto kLogAfter = std::chrono::milliseconds(100);
        const auto started = std::chrono::steady_clock::now();
        bool logged = false;
        for (;;) {
#ifdef _WIN32
            OVERLAPPED exclusive{};
            if (LockFileEx(handle_, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &exclusive)) break;
            const auto error = GetLastError();
            if (error != ERROR_LOCK_VIOLATION) {
                Close();
                throw std::system_error(static_cast<int>(error), std::system_category(), "lock snapshot writer lease");
            }
#else
            if (::flock(fd_, LOCK_EX | LOCK_NB) == 0) break;
            const int error = errno;
            if (error == EINTR) continue;
            if (error != EWOULDBLOCK && error != EAGAIN) {
                Close();
                throw std::system_error(error, std::generic_category(), "lock snapshot writer lease");
            }
#endif
            const auto waited = std::chrono::steady_clock::now() - started;
            if (waited >= kLimit) {
                Close();
                throw std::runtime_error(
                    "snapshot writer lease " + path.string() + " stayed locked for " +
                    std::to_string(std::chrono::duration_cast<std::chrono::seconds>(kLimit).count()) +
                    " s: a reader holds its probe of it (a read-only process may be stopped inside the probe), "
                    "or a writer of an older version holds the table");
            }
            if (!logged && waited >= kLogAfter) {
                LogMessage(LogLevel::kWarn, LogComponent::kLock,
                    "writer start waits for a reader holding its probe of the snapshot writer lease",
                    {{"path", path.string()}});
                logged = true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        locked_ = true;
    }
  private:
    void Close() noexcept {
#ifdef _WIN32
        if (handle_ != INVALID_HANDLE_VALUE) {
            if (locked_) { OVERLAPPED overlap{}; (void)UnlockFileEx(handle_, 0, 1, 0, &overlap); }
            (void)CloseHandle(handle_); handle_ = INVALID_HANDLE_VALUE;
        }
#else
        if (fd_ >= 0) { if (locked_) (void)::flock(fd_, LOCK_UN); (void)::close(fd_); fd_ = -1; }
#endif
    }
#ifdef _WIN32
    HANDLE handle_ = INVALID_HANDLE_VALUE;
#else
    int fd_ = -1;
#endif
    bool locked_ = false;
    bool writer_present_ = false;
};

class MappedSnapshotCounter {
  public:
    MappedSnapshotCounter(const std::filesystem::path& path, bool writable) {
        RequireMappedAtomics();
        try {
#ifdef _WIN32
            file_ = CreateFileW(path.c_str(), GENERIC_READ | (writable ? GENERIC_WRITE : 0U),
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
                FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
            if (file_ == INVALID_HANDLE_VALUE) throw SnapshotArtifactUnstable("snapshot live mapping is unavailable: " + path.string());
            BY_HANDLE_FILE_INFORMATION info{};
            LARGE_INTEGER size{};
            if (!GetFileInformationByHandle(file_, &info) || !GetFileSizeEx(file_, &size) ||
                (info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) != 0U ||
                size.QuadPart != static_cast<LONGLONG>(kLiveRecordSize))
                throw std::runtime_error("snapshot live metadata is malformed: " + path.string());
            mapping_ = CreateFileMappingW(file_, nullptr, writable ? PAGE_READWRITE : PAGE_READONLY, 0, 0, nullptr);
            if (!mapping_) throw SnapshotArtifactUnstable("cannot map snapshot live metadata: " + path.string());
            bytes_ = static_cast<std::uint8_t*>(MapViewOfFile(mapping_, writable ? FILE_MAP_WRITE : FILE_MAP_READ, 0, 0, kLiveRecordSize));
            if (!bytes_) throw SnapshotArtifactUnstable("cannot view snapshot live metadata: " + path.string());
#else
            fd_ = ::open(path.c_str(), (writable ? O_RDWR : O_RDONLY) | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK);
            if (fd_ < 0) throw SnapshotArtifactUnstable("snapshot live mapping is unavailable: " + path.string());
            struct stat info{};
            if (::fstat(fd_, &info) != 0 || !S_ISREG(info.st_mode) || info.st_size != static_cast<off_t>(kLiveRecordSize))
                throw std::runtime_error("snapshot live metadata is malformed: " + path.string());
            void* address = ::mmap(nullptr, kLiveRecordSize, PROT_READ | (writable ? PROT_WRITE : 0), MAP_SHARED, fd_, 0);
            if (address == MAP_FAILED) throw SnapshotArtifactUnstable("cannot map snapshot live metadata: " + path.string());
            bytes_ = static_cast<std::uint8_t*>(address);
#endif
            // The immutable header is checked separately from the changing
            // atomic word; applying a CRC to live counter bytes would race it.
            const std::vector<std::uint8_t> header(bytes_, bytes_ + kLiveCounterOffset);
            if (header[0] != 'C' || header[1] != 'K' || header[2] != 'S' || header[3] != 'L' ||
                ReadLe32(header, 4) != 1U || ReadLe32(header, 16) != Crc32(header.data(), 16) ||
                ReadLe32(header, 20) != 0U) throw std::runtime_error("snapshot live metadata is malformed: " + path.string());
            session_ = ReadLe64(header, 8);
        } catch (...) {
            Close();
            throw;
        }
    }
    ~MappedSnapshotCounter() { Close(); }
    MappedSnapshotCounter(const MappedSnapshotCounter&) = delete;
    MappedSnapshotCounter& operator=(const MappedSnapshotCounter&) = delete;
    [[nodiscard]] std::uint64_t Session() const { return session_; }
    [[nodiscard]] std::uint64_t Load() const {
        // Full fence also orders a reader's completed artifact collection
        // before its second observation; acquire alone only orders later work.
        std::atomic_thread_fence(std::memory_order_seq_cst);
        return Counter().load(std::memory_order_seq_cst);
    }
    bool CompareExchange(std::uint64_t& expected, std::uint64_t desired) {
        // The first CAS orders odd before subsequent artifact I/O; the last
        // CAS releases every completed writer's artifact I/O before even.
        // Acquiring preceding CAS updates chains concurrent/nested writers.
        return Counter().compare_exchange_strong(expected, desired, std::memory_order_seq_cst);
    }
  private:
    [[nodiscard]] std::atomic_ref<std::uint64_t> Counter() const {
        // mmap/MapViewOfFile returns page-aligned storage; offset 24 is aligned
        // to 8. Read-only instances only call load, never an RMW operation.
        return std::atomic_ref<std::uint64_t>(*reinterpret_cast<std::uint64_t*>(bytes_ + kLiveCounterOffset));
    }
    void Close() noexcept {
#ifdef _WIN32
        if (bytes_) (void)UnmapViewOfFile(bytes_);
        if (mapping_) (void)CloseHandle(mapping_);
        if (file_ != INVALID_HANDLE_VALUE) (void)CloseHandle(file_);
        mapping_ = nullptr; file_ = INVALID_HANDLE_VALUE;
#else
        if (bytes_) (void)::munmap(bytes_, kLiveRecordSize);
        if (fd_ >= 0) (void)::close(fd_);
        fd_ = -1;
#endif
        bytes_ = nullptr;
    }
#ifdef _WIN32
    HANDLE file_ = INVALID_HANDLE_VALUE;
    HANDLE mapping_ = nullptr;
#else
    int fd_ = -1;
#endif
    std::uint8_t* bytes_ = nullptr;
    std::uint64_t session_ = 0;
};

[[nodiscard]] SnapshotGenerationObservation ReadLiveSnapshotGeneration(
    const std::filesystem::path& directory, std::uint64_t session) {
    if ((session & 1U) == 0U) return {session, 0};
    SnapshotWriterLease lease(SnapshotLeasePath(directory, false), SnapshotWriterLease::Role::kReader);
    if (!lease.WriterPresent()) return {session, kGenerationOdd};
    MappedSnapshotCounter counter(directory / kSnapshotLiveFile, false);
    if (counter.Session() != session)
        throw SnapshotArtifactUnstable("snapshot live mapping belongs to another writer session");
    const auto state = counter.Load();
    if ((state & kGenerationOdd) == 0U &&
        (state & (kGenerationFailed | kGenerationCountMask)) != 0U)
        throw std::runtime_error("snapshot live counter is malformed");
    return {session, state};
}

void CrashAtSnapshotFailpoint(const char* key) {
    if (ConsumeFailpointEnv(key)) {
        std::_Exit(86);
    }
}

[[nodiscard]] ReadOnlyArtifactSnapshot ReadArtifactForSnapshot(
    const std::filesystem::path& path) {
    std::error_code status_ec;
    const auto status = std::filesystem::symlink_status(path, status_ec);
    if (status_ec == std::errc::no_such_file_or_directory ||
        (!status_ec && !std::filesystem::exists(status))) {
        return {};
    }
    if (status_ec) {
        throw std::runtime_error(
            "read-only snapshot cannot inspect artifact " + path.string() +
            ": " + status_ec.message());
    }
    if (!std::filesystem::is_regular_file(status)) {
        throw std::runtime_error(
            "read-only snapshot expected a regular file at " + path.string());
    }

    try {
        return {
            .present = true,
            .bytes = LoadFile(path),
        };
    } catch (const std::exception& load_error) {
        // An atomic replacement/removal between status and open is namespace
        // instability, not proof of damage. If the path is now absent, record
        // that observation and let the generation bracket decide whether it
        // is stable. A still-present unreadable artifact fails closed.
        std::error_code restat_ec;
        const auto restat = std::filesystem::symlink_status(path, restat_ec);
        if (restat_ec == std::errc::no_such_file_or_directory ||
            (!restat_ec && !std::filesystem::exists(restat))) {
            return {};
        }
        if (restat_ec) {
            throw std::runtime_error(
                "read-only snapshot cannot re-inspect artifact " +
                path.string() + " after a read failure: " +
                restat_ec.message());
        }
        throw SnapshotArtifactUnstable(
            "read-only snapshot cannot read artifact " + path.string() +
            ": " + load_error.what());
    }
}

// The table's pending transaction intents, in name order. They are rare
// and short-lived, so listing them costs little.
[[nodiscard]] std::vector<ReadOnlyArtifactSnapshot> ReadTxnIntentsForSnapshot(
    const std::filesystem::path& intent_dir) {
    std::error_code list_ec;
    std::filesystem::directory_iterator iterator(intent_dir, list_ec);
    if (list_ec == std::errc::no_such_file_or_directory) {
        return {};
    }
    if (list_ec) {
        throw SnapshotArtifactUnstable(
            "read-only snapshot cannot list intents under " + intent_dir.string() + ": " + list_ec.message());
    }
    std::vector<std::string> names;
    for (const std::filesystem::directory_iterator end; iterator != end;) {
        auto name = iterator->path().filename().string();
        if (IsTxnIntentFileName(name)) {
            names.push_back(std::move(name));
        }
        iterator.increment(list_ec);
        if (list_ec) {
            throw SnapshotArtifactUnstable(
                "read-only snapshot cannot list intents under " + intent_dir.string() + ": " + list_ec.message());
        }
    }
    std::sort(names.begin(), names.end());
    std::vector<ReadOnlyArtifactSnapshot> intents;
    for (const auto& name : names) {
        auto intent = ReadArtifactForSnapshot(intent_dir / name);
        if (intent.present) {
            intents.push_back(std::move(intent));
        }
    }
    return intents;
}

[[nodiscard]] ReadOnlyChunkDiskSnapshot CollectReadOnlyChunkDiskSnapshot(
    const std::filesystem::path& data_path,
    const std::filesystem::path& wal_path,
    const std::filesystem::path& intent_path,
    const std::filesystem::path& txn_intent_dir,
    std::size_t collection,
    const std::function<void(
        std::size_t,
        ReadOnlySnapshotArtifact)>& observation) {
    ReadOnlyChunkDiskSnapshot snapshot;
    snapshot.image = ReadArtifactForSnapshot(data_path);
    observation(collection, ReadOnlySnapshotArtifact::kImage);
    snapshot.wal = ReadArtifactForSnapshot(wal_path);
    observation(collection, ReadOnlySnapshotArtifact::kWal);
    snapshot.intent = ReadArtifactForSnapshot(intent_path);
    snapshot.txn_intents = ReadTxnIntentsForSnapshot(txn_intent_dir);
    observation(collection, ReadOnlySnapshotArtifact::kIntent);
    return snapshot;
}

[[nodiscard]] SnapshotGenerationObservation ReadSnapshotGeneration(
    const std::filesystem::path& path,
    bool record_required) {
    const auto artifact = ReadArtifactForSnapshot(path);
    if (!artifact.present) {
        // Generation zero is the implicit stable epoch of a store no writer
        // has opened yet. A writer durably creates an odd record before it
        // changes any snapshot artifact, and never removes the record: once
        // seen, its absence means the store directory itself went away.
        if (record_required) {
            throw std::runtime_error(
                "snapshot generation record " + path.string() +
                " disappeared after the store was opened: the table was dropped or its "
                "directory removed");
        }
        return {};
    }
    std::uint64_t generation = 0;
    if (!TryParseSnapshotGenerationRecord(
            artifact.bytes, &generation)) {
        throw std::runtime_error(
            "read-only snapshot generation metadata is malformed: " +
            path.string());
    }
    return ReadLiveSnapshotGeneration(path.parent_path(), generation);
}

[[nodiscard]] bool ForceReadOnlySnapshotInstabilityForTests(
    const ChunkCoord& chunk_coord) {
    if constexpr (!kFailpointsCompiled) {
        (void)chunk_coord;
        return false;
    }
    const char* value =
        std::getenv("CHUNKDB_FAILPOINT_READ_ONLY_SNAPSHOT_RETRY_EXHAUST");
    if (value == nullptr || value[0] == '\0') {
        return false;
    }
    return std::string_view(value) ==
           (std::to_string(chunk_coord.x) + "," +
            std::to_string(chunk_coord.y));
}

}  // namespace

struct SnapshotGenerationMapping::Impl {
    explicit Impl(const std::filesystem::path& path)
        : directory(path),
          owner(SnapshotLeasePath(path, true).parent_path() / kSnapshotOwnerFile, SnapshotWriterLease::Role::kOwner),
          lease(SnapshotLeasePath(path, true), SnapshotWriterLease::Role::kWriter) { RequireMappedAtomics(); }
    std::filesystem::path directory;
    // Declared first, so it is taken before the lease and released after it.
    SnapshotWriterLease owner;
    SnapshotWriterLease lease;
    std::unique_ptr<MappedSnapshotCounter> counter;
};

SnapshotGenerationMapping::SnapshotGenerationMapping(const std::filesystem::path& directory)
    : impl_(std::make_unique<Impl>(directory)) {}
SnapshotGenerationMapping::~SnapshotGenerationMapping() = default;

void SnapshotGenerationMapping::Initialize(std::uint64_t session) {
    std::vector<std::uint8_t> record{'C', 'K', 'S', 'L'};
    WriteLe32(record, 1U);
    WriteLe64(record, session);
    WriteLe32(record, Crc32(record));
    WriteLe32(record, 0U);
    WriteLe64(record, kGenerationOdd);
    const auto path = impl_->directory / kSnapshotLiveFile;
    CleanupAtomicTmpArtifacts(path);
    AtomicWrite(path, record, /*fsync_file=*/false, /*fsync_directory=*/false,
        nullptr, nullptr, /*enable_generic_failpoints=*/false);
    impl_->counter = std::make_unique<MappedSnapshotCounter>(path, true);
}

std::uint64_t SnapshotGenerationMapping::Load() const { return impl_->counter->Load(); }
bool SnapshotGenerationMapping::IsFailed() const {
    const auto state = Load();
    return (state & kGenerationFailed) != 0U ||
        ((state & kGenerationOdd) != 0U && (state & kGenerationCountMask) == 0U);
}

bool SnapshotGenerationMapping::CompareExchange(std::uint64_t& expected, std::uint64_t desired) {
    return impl_->counter->CompareExchange(expected, desired);
}

SnapshotGenerationObservation ReadSnapshotGenerationForScan(
    const std::filesystem::path& path,
    bool record_required) {
    return ReadSnapshotGeneration(path, record_required);
}

std::optional<SnapshotGenerationObservation> StableSnapshotGeneration(
    const std::filesystem::path& path,
    bool record_required) {
    try {
        const auto observed = ReadSnapshotGeneration(path, record_required);
        if ((observed.second & 1U) != 0U) return std::nullopt;
        return observed;
    } catch (const SnapshotArtifactUnstable&) {
        return std::nullopt;
    }
}

[[nodiscard]] ReadOnlyChunkDiskSnapshot LoadStableReadOnlyChunkDiskSnapshot(
    const std::filesystem::path& data_path,
    const std::filesystem::path& wal_path,
    const std::filesystem::path& intent_path,
    const std::filesystem::path& txn_intent_dir,
    const std::filesystem::path& generation_path,
    bool generation_record_required,
    const ChunkCoord& chunk_coord,
    const std::function<void(
        std::size_t,
        ReadOnlySnapshotArtifact)>& observation) {
    // Bounded backoff, not an unbounded wait: the reader still fails closed,
    // it just stops treating a normal-length writer bracket as damage.
    const auto backoff_budget =
        std::chrono::microseconds(kReadOnlySnapshotBackoffBudgetMs * 1000U);
    std::chrono::microseconds slept{0};
    std::chrono::microseconds next_sleep{kReadOnlySnapshotBackoffStartUs};
    std::size_t attempt = 0;
    std::string last_unstable;
    while (true) {
        ++attempt;
        try {
            const auto before =
                ReadSnapshotGeneration(generation_path, generation_record_required);
            auto snapshot = CollectReadOnlyChunkDiskSnapshot(
                data_path, wal_path, intent_path, txn_intent_dir, attempt, observation);
            const auto after =
                ReadSnapshotGeneration(generation_path, generation_record_required);
            if ((before.second & 1U) == 0U && before == after &&
                !ForceReadOnlySnapshotInstabilityForTests(chunk_coord)) {
                snapshot.generation = before;
                return snapshot;
            }
        } catch (const SnapshotArtifactUnstable& unstable) {
            // Retry it like any other unstable observation. A genuinely
            // damaged artifact stays unreadable for the whole budget and
            // still fails closed below, carrying this message with it.
            last_unstable = unstable.what();
            observation(attempt, ReadOnlySnapshotArtifact::kUnstableObservation);
        }
        if (attempt < kReadOnlySnapshotSpinAttempts) {
            continue;
        }
        if (slept >= backoff_budget) {
            break;
        }
        const auto sleep_for = std::min(next_sleep, backoff_budget - slept);
        std::this_thread::sleep_for(sleep_for);
        slept += sleep_for;
        next_sleep = std::min(
            next_sleep * 2,
            std::chrono::microseconds(kReadOnlySnapshotBackoffMaxUs));
    }

    throw std::runtime_error(
        "read-only chunk snapshot remained unstable after " +
        std::to_string(attempt) + " bounded attempts spanning " +
        std::to_string(kReadOnlySnapshotBackoffBudgetMs) +
        "ms for chunk (" +
        std::to_string(chunk_coord.x) + "," +
        std::to_string(chunk_coord.y) + ")" +
        (last_unstable.empty() ? "" : "; last read failure: " + last_unstable));
}

std::vector<std::uint8_t> SerializeSnapshotGenerationRecord(
    std::uint64_t generation) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kSnapshotGenerationRecordSize);
    bytes.insert(
        bytes.end(),
        kSnapshotGenerationMagic.begin(),
        kSnapshotGenerationMagic.end());
    WriteLe64(bytes, generation);
    WriteLe32(bytes, Crc32(bytes));
    return bytes;
}

bool TryParseSnapshotGenerationRecord(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* generation) {
    if (generation == nullptr ||
        bytes.size() != kSnapshotGenerationRecordSize ||
        !std::equal(
            kSnapshotGenerationMagic.begin(),
            kSnapshotGenerationMagic.end(),
            bytes.begin())) {
        return false;
    }
    const std::uint32_t expected_crc = ReadLe32(bytes, 12U);
    if (Crc32(bytes.data(), 12U) != expected_crc) {
        return false;
    }
    *generation = ReadLe64(bytes, 4U);
    return true;
}

ChunkStore::SnapshotGenerationWriteGuard::SnapshotGenerationWriteGuard(
    ChunkStore* store)
    : store_(store),
      nested_on_same_thread_(
          std::find(
              g_snapshot_write_stack.begin(),
              g_snapshot_write_stack.end(),
              store_) != g_snapshot_write_stack.end()) {
    g_snapshot_write_stack.push_back(store_);
    try {
        store_->BeginSnapshotGenerationWrite();
    } catch (...) {
        g_snapshot_write_stack.pop_back();
        throw;
    }
}

ChunkStore::SnapshotGenerationWriteGuard::~SnapshotGenerationWriteGuard() {
    if (!finished_) {
        store_->AbandonSnapshotGenerationWrite(
            !nested_on_same_thread_);
        g_snapshot_write_stack.pop_back();
    }
}

void ChunkStore::SnapshotGenerationWriteGuard::Finish() {
    if (finished_) {
        return;
    }
    finished_ = true;
    g_snapshot_write_stack.pop_back();
    store_->FinishSnapshotGenerationWrite();
}

void ChunkStore::InitializeSnapshotGeneration(bool store_preexisting) {
    (void)store_preexisting;
    snapshot_generation_path_ =
        data_dir_ / std::string(kSnapshotGenerationFile);
    if (access_mode_ != AccessMode::kReadOnly) {
        // A crash while the record was being replaced leaves its temp file.
        CleanupAtomicTmpArtifacts(snapshot_generation_path_);
    }

    const auto artifact =
        ReadArtifactForSnapshot(snapshot_generation_path_);
    std::uint64_t persisted = 0;
    if (artifact.present &&
        !TryParseSnapshotGenerationRecord(
            artifact.bytes, &persisted)) {
        throw std::runtime_error(
            "snapshot generation metadata is malformed: " +
            snapshot_generation_path_.string());
    }

    snapshot_generation_ = persisted;
    snapshot_generation_record_seen_ = artifact.present;
    if (access_mode_ == AccessMode::kReadOnly) {
        // A writer holds its lease from before it publishes an odd session
        // until after it closes the session even. An odd session that no
        // writer holds, and that is still there after the lease was seen
        // free, is a writer that stopped without closing: its recovery has
        // not run, and only a writer runs it.
        if ((persisted & 1U) != 0U &&
            !SnapshotWriterLease(SnapshotLeasePath(data_dir_, false), SnapshotWriterLease::Role::kReader).WriterPresent()) {
            const auto again = ReadArtifactForSnapshot(snapshot_generation_path_);
            std::uint64_t current = 0;
            if (again.present && !TryParseSnapshotGenerationRecord(again.bytes, &current)) {
                throw std::runtime_error(
                    "snapshot generation metadata is malformed: " +
                    snapshot_generation_path_.string());
            }
            if (again.present && current == persisted) {
                throw std::runtime_error(
                    "table " + data_dir_.string() +
                    " needs recovery: its last writer stopped without closing it; a writer starting now may "
                    "be recovering it, so try again, and with no writer running open it read-write once to recover it");
            }
        }
        return;
    }
    if (persisted >
        std::numeric_limits<std::uint64_t>::max() - 2U) {
        throw std::overflow_error(
            "snapshot generation exhausted; refusing to wrap");
    }

    // Startup itself is a writer transition because rollback-intent recovery
    // can truncate WALs and remove intent files. If the previous process
    // crashed while odd, skip to a new odd value rather than reusing its
    // epoch. The durable odd publication precedes every recovery mutation.
    const std::uint64_t recovery_generation =
        (persisted & 1U) == 0U ? persisted + 1U : persisted + 2U;
    if (ConsumeFailpointEnv(
            "CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_BEGIN_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected snapshot generation begin failure");
    }
    // Prepare the new volatile map before switching the checked session.
    // A reader racing startup sees either a closed old session or a session
    // mismatch and retries; it never sees new session + stale damaged map.
    // Artifact recovery still begins only after the durable odd fence.
    snapshot_generation_mapping_->Initialize(recovery_generation);
    bool recovery_generation_replaced = false;
    try {
        AtomicWrite(
            snapshot_generation_path_,
            SerializeSnapshotGenerationRecord(recovery_generation),
            /*fsync_file=*/true,
            /*fsync_directory=*/true,
            &recovery_generation_replaced,
            /*after_rename_failpoint=*/nullptr,
            /*enable_generic_failpoints=*/false);
    } catch (...) {
        if (recovery_generation_replaced) {
            snapshot_generation_ = recovery_generation;
        }
        throw;
    }
    snapshot_generation_ = recovery_generation;
    snapshot_generation_record_seen_ = true;
    CrashAtSnapshotFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_AFTER_BEGIN_ONCE");
}

void ChunkStore::FinishSnapshotGenerationRecovery() {
    if (access_mode_ == AccessMode::kReadOnly) {
        return;
    }
    if ((snapshot_generation_ & 1U) == 0U ||
        snapshot_generation_ ==
            std::numeric_limits<std::uint64_t>::max()) {
        throw std::runtime_error(
            "invalid snapshot generation at recovery completion");
    }
    if (ConsumeFailpointEnv(
            "CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_END_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected snapshot generation end failure");
    }
    CrashAtSnapshotFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_BEFORE_END_ONCE");
    // Recovery has completed all artifact mutations. The durable session
    // remains odd until clean shutdown; live readers use this mapped bracket.
    auto expected = kGenerationOdd;
    if (!snapshot_generation_mapping_->CompareExchange(expected, kGenerationEpochStep)) {
        throw std::logic_error("snapshot recovery counter was modified");
    }
}

void ChunkStore::BeginSnapshotGenerationWrite() {
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("read-only store cannot begin a snapshot generation write");
    }
    auto current = snapshot_generation_mapping_->Load();
    for (;;) {
        if ((current & kGenerationFailed) != 0U ||
            ((current & kGenerationOdd) != 0U && (current & kGenerationCountMask) == 0U)) {
            throw std::runtime_error("snapshot generation is unresolved; reopen the store with RECOVER TABLE or a restart to complete recovery");
        }
        if ((current & kGenerationCountMask) == kGenerationCountMask ||
            current > std::numeric_limits<std::uint64_t>::max() - kGenerationEpochStep) {
            throw std::overflow_error("snapshot generation exhausted; refusing to wrap");
        }
        const auto desired = (current + kGenerationCountOne) | kGenerationOdd;
        const bool first = (current & kGenerationCountMask) == 0U;
        if (first && ConsumeFailpointEnv("CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_BEGIN_FAIL_ONCE")) {
            throw std::runtime_error("injected snapshot generation begin failure");
        }
        if (snapshot_generation_mapping_->CompareExchange(current, desired)) {
            if (first) {
                stats_snapshot_generation_odd_publications_.fetch_add(1, std::memory_order_relaxed);
                CrashAtSnapshotFailpoint("CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_AFTER_BEGIN_ONCE");
            }
            break;
        }
    }
}

void ChunkStore::FinishSnapshotGenerationWrite() {
    auto current = snapshot_generation_mapping_->Load();
    for (;;) {
        if ((current & kGenerationCountMask) == 0U) {
            throw std::logic_error("snapshot generation active-writer underflow");
        }
        const bool last = (current & kGenerationCountMask) == kGenerationCountOne;
        const bool failed = (current & kGenerationFailed) != 0U;
        if (last && !failed) {
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_END_FAIL_ONCE")) {
                AbandonSnapshotGenerationWrite(true);
                throw std::runtime_error("injected snapshot generation end failure");
            }
            CrashAtSnapshotFailpoint("CHUNKDB_FAILPOINT_CRASH_SNAPSHOT_GENERATION_BEFORE_END_ONCE");
        }
        auto desired = current - kGenerationCountOne;
        if (last && !failed) desired = (desired + kGenerationEpochStep) & ~kGenerationOdd;
        if (snapshot_generation_mapping_->CompareExchange(current, desired)) {
            if (last && failed) {
                throw std::runtime_error("a concurrent snapshot transition failed; snapshot generation remains odd until RECOVER TABLE or a writer restart");
            }
            if (last) stats_snapshot_generation_even_publications_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }
}

void ChunkStore::AbandonSnapshotGenerationWrite(bool fail_epoch) noexcept {
    auto current = snapshot_generation_mapping_->Load();
    for (;;) {
        auto desired = current;
        if (fail_epoch) desired |= kGenerationFailed | kGenerationOdd;
        if ((current & kGenerationCountMask) != 0U) desired -= kGenerationCountOne;
        if (snapshot_generation_mapping_->CompareExchange(current, desired)) return;
    }
}

void ChunkStore::CloseSnapshotGeneration() noexcept {
    // Artifact guards certify coherence independently of write durability.
    // A retained durable CKRB can safely bound read-only replay even when its
    // failed local WAL repair poisoned further writes. Uncertain publication
    // instead abandons its guard and leaves the mapped epoch failed/odd.
    if (!snapshot_generation_mapping_ ||
        (snapshot_generation_mapping_->Load() & (kGenerationOdd | kGenerationFailed | kGenerationCountMask)) != 0U) return;
    try {
        AtomicWrite(snapshot_generation_path_,
            SerializeSnapshotGenerationRecord(snapshot_generation_ + 1U),
            /*fsync_file=*/true, /*fsync_directory=*/true,
            nullptr, nullptr, /*enable_generic_failpoints=*/false);
    } catch (const std::exception& error) {
        try {
            LogMessage(LogLevel::kError, LogComponent::kStore,
                "close could not publish stable snapshot session",
                {{"data_dir", data_dir_.string()}, {"error", error.what()}});
        } catch (...) {
            // Logging cannot throw from shutdown. AtomicWrite may already
            // have replaced the stable close record before a sync failure;
            // all artifacts were coherent before this close publication.
        }
    }
}

void ChunkStore::SetSnapshotGenerationLingerForTests(
    std::uint64_t window_ms,
    std::size_t max_brackets) {
    // Retained for source compatibility with older test harnesses. Completed
    // publications cannot defer their stable generation, regardless of policy.
    (void)window_ms;
    (void)max_brackets;
}

bool ChunkStore::SnapshotGenerationLingerPendingForTests() const {
    return false;
}

std::uint64_t ChunkStore::SnapshotGenerationForTests() const {
    return snapshot_generation_mapping_
        ? snapshot_generation_mapping_->Load()
        : ReadSnapshotGeneration(snapshot_generation_path_, snapshot_generation_record_seen_).second;
}

std::uint64_t ChunkStore::SnapshotGenerationOddPublicationsForTests()
    const noexcept {
    return stats_snapshot_generation_odd_publications_.load(
        std::memory_order_relaxed);
}

std::uint64_t ChunkStore::SnapshotGenerationEvenPublicationsForTests()
    const noexcept {
    return stats_snapshot_generation_even_publications_.load(
        std::memory_order_relaxed);
}

void ChunkStore::FlushSnapshotGenerationLingerForTests() {
    // Compatibility no-op: the last active publication already closes itself.
}

}  // namespace chunkdb
