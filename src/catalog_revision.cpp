#include "catalog_revision.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/logging.hpp"
#include "store_manifest.hpp"

namespace chunkdb {
namespace {

constexpr std::array<std::uint8_t, 4> kMagic = {'C', 'K', 'C', 'R'};
constexpr std::uint16_t kFormat = 1U;
constexpr std::uint16_t kInProgress = 1U;
constexpr std::size_t kFlagsOffset = 6U;
constexpr std::size_t kEpochOffset = 8U;
constexpr std::size_t kRevisionOffset = 24U;
constexpr std::size_t kDigestOffset = 32U;
constexpr std::size_t kReservedOffset = 48U;
constexpr std::size_t kCrcOffset = 52U;

}  // namespace

std::vector<std::uint8_t> EncodeCatalogRevision(const CatalogRevisionRecord& record) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kCatalogRevisionFileSize);
    bytes.insert(bytes.end(), kMagic.begin(), kMagic.end());
    WriteLe16(bytes, kFormat);
    WriteLe16(bytes, record.in_progress ? kInProgress : 0U);
    bytes.insert(bytes.end(), record.epoch.begin(), record.epoch.end());
    WriteLe64(bytes, record.revision);
    bytes.insert(bytes.end(), record.digest.begin(), record.digest.end());
    WriteLe32(bytes, 0U);
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    return bytes;
}

CatalogRevisionRecord DecodeCatalogRevision(const std::vector<std::uint8_t>& bytes) {
    if (bytes.size() != kCatalogRevisionFileSize)
        throw std::runtime_error("size is " + std::to_string(bytes.size()) + " bytes, expected " +
            std::to_string(kCatalogRevisionFileSize));
    if (!std::equal(kMagic.begin(), kMagic.end(), bytes.begin())) throw std::runtime_error("bad magic");
    if (ReadLe32(bytes, kCrcOffset) != Crc32(bytes.data(), kCrcOffset)) throw std::runtime_error("checksum mismatch");
    if (const auto format = ReadLe16(bytes, 4U); format != kFormat)
        throw std::runtime_error("unsupported format " + std::to_string(format));
    const auto flags = ReadLe16(bytes, kFlagsOffset);
    if ((flags & ~kInProgress) != 0U) throw std::runtime_error("unknown flags " + std::to_string(flags));
    if (ReadLe32(bytes, kReservedOffset) != 0U) throw std::runtime_error("reserved field is not zero");
    CatalogRevisionRecord record;
    std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(kEpochOffset), record.epoch.size(), record.epoch.begin());
    if (record.epoch == StoreId{}) throw std::runtime_error("catalog epoch is zero");
    record.revision = ReadLe64(bytes, kRevisionOffset);
    std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(kDigestOffset), record.digest.size(), record.digest.begin());
    record.in_progress = (flags & kInProgress) != 0U;
    return record;
}

std::optional<CatalogRevisionRecord> ReadCatalogRevision(const std::filesystem::path& data_dir) {
    const auto path = data_dir / std::string(kCatalogRevisionFileName);
    std::error_code error;
    const auto status = std::filesystem::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory || (!error && status.type() == std::filesystem::file_type::not_found))
        return std::nullopt;
    if (error) throw std::runtime_error("cannot inspect " + path.string() + ": " + error.message());
    if (status.type() != std::filesystem::file_type::regular)
        throw std::runtime_error(path.string() + " is not a regular file");
    try {
        return DecodeCatalogRevision(LoadFile(path));
    } catch (const std::exception& decode) {
        throw std::runtime_error("catalog revision record " + path.string() + " is not usable (" + decode.what() + ")");
    }
}

void MarkCatalogRevision(const std::filesystem::path& data_dir) {
    std::optional<CatalogRevisionRecord> record;
    try {
        record = ReadCatalogRevision(data_dir);
    } catch (const std::runtime_error&) {
        return;  // The start replaces it with a new epoch.
    }
    if (!record || record->in_progress) return;
    record->in_progress = true;
    AtomicWrite(data_dir / std::string(kCatalogRevisionFileName), EncodeCatalogRevision(*record),
        /*fsync_file=*/true, /*fsync_directory=*/true, /*out_replaced=*/nullptr, /*after_rename_failpoint=*/nullptr,
        /*enable_generic_failpoints=*/false);
}

CatalogClock::CatalogClock(std::filesystem::path data_dir, Mode mode, std::function<CatalogRevisionDigest()> digest)
    : data_dir_(std::move(data_dir)), mode_(mode), digest_(std::move(digest)) {
    if (mode_ == Mode::kMemory) {
        epoch_ = NewStoreId();
        return;
    }
    // A crash during an earlier replace can leave its temporary file. With
    // several processes another one may be replacing the record now.
    if (mode_ == Mode::kDurable) (void)CleanupAtomicTmpArtifacts(data_dir_ / std::string(kCatalogRevisionFileName));
    std::optional<CatalogRevisionRecord> record;
    try {
        record = ReadCatalogRevision(data_dir_);
    } catch (const std::runtime_error& error) {
        // Consumers of the old epoch rebuild their catalog; no revision of it
        // can name another state.
        LogMessage(LogLevel::kWarn, LogComponent::kStore, "catalog revision record replaced; a new catalog epoch starts",
            {{"error", error.what()}});
    }
    if (record && record->revision < std::numeric_limits<std::uint64_t>::max()) {
        epoch_ = record->epoch;
        // An interrupted statement or changed startup inputs invalidate the
        // recorded revision. Only an actual disk mark can cover a new write.
        const bool changed = record->in_progress || (mode_ == Mode::kDurable && record->digest != CurrentDigest());
        revision_ = record->revision + (changed ? 1U : 0U);
        recorded_ = record->revision;
        marked_ = record->in_progress;
        recorded_digest_ = record->digest;
    } else {
        epoch_ = NewStoreId();
        revision_ = 0U;
    }
}

CatalogRevisionDigest CatalogClock::CurrentDigest() const {
    // Read before the clock mutex: a statement that changes users holds the
    // registry lock while its mark takes the clock mutex.
    return digest_ && mode_ == Mode::kDurable ? digest_() : CatalogRevisionDigest{};
}

std::uint64_t CatalogClock::revision() const {
    std::lock_guard lock(mutex_);
    return revision_;
}

void CatalogClock::Write(std::uint64_t revision, bool in_progress, const CatalogRevisionDigest& current) {
    // A mark keeps the last digest, since a start moves past it anyway.
    const auto digest = in_progress ? recorded_digest_ : current;
    // Unknown until the replace completes: a failure, even after the rename,
    // may leave either record, so the next mark and revision are written again.
    marked_ = false;
    recorded_.reset();
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CATALOG_REVISION_WRITE_FAIL_ONCE"))
        throw std::runtime_error("injected catalog revision write failure");
    AtomicWrite(data_dir_ / std::string(kCatalogRevisionFileName),
        EncodeCatalogRevision(CatalogRevisionRecord{.epoch = epoch_, .revision = revision, .in_progress = in_progress, .digest = digest}),
        /*fsync_file=*/true, /*fsync_directory=*/true, /*out_replaced=*/nullptr, /*after_rename_failpoint=*/nullptr,
        /*enable_generic_failpoints=*/false);
    marked_ = in_progress;
    recorded_ = revision;
    recorded_digest_ = digest;
    if (in_progress && ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CRASH_CATALOG_AFTER_MARK_ONCE")) ExitAbruptly(86);
    if (!in_progress && ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CRASH_CATALOG_AFTER_REVISION_ONCE")) ExitAbruptly(86);
}

void CatalogClock::Begin() {
    std::lock_guard lock(mutex_);
    // Shared: the first statement marks the record, and nothing clears it.
    if (mode_ != Mode::kMemory && !marked_) Write(revision_, true, recorded_digest_);
    ++running_;
}

std::uint64_t CatalogClock::End(bool decided) {
    const auto current = CurrentDigest();
    std::lock_guard lock(mutex_);
    if (running_ == 0U) throw std::logic_error("catalog clock ended a statement it did not begin");
    --running_;
    if (!decided) {
        // Under the feed's refresh mutex, any earlier revision has finished
        // refreshing. Keep the mark for other statements or unread tables.
        if (mode_ == Mode::kDurable && running_ == 0U && !holding_ && marked_) Write(revision_, false, current);
        return revision_;
    }
    if (revision_ == std::numeric_limits<std::uint64_t>::max()) throw std::runtime_error("catalog revisions are exhausted");
    ++revision_;
    // Recorded before it is returned; on failure it is never handed out.
    if (mode_ == Mode::kDurable) Write(revision_, running_ != 0U || holding_, current);
    return revision_;
}

std::uint64_t CatalogClock::Snapshot() {
    const auto current = CurrentDigest();
    std::lock_guard lock(mutex_);
    if (running_ == 0U && mode_ != Mode::kDurable) return revision_;
    if (running_ == 0U && !marked_ && (!recorded_ || *recorded_ == revision_)) {
        // Recorded before it is handed out: a failed write may have left an
        // earlier revision on disk.
        if (recorded_ != revision_) Write(revision_, holding_, current);
        return revision_;
    }
    // A statement runs, the disk record is marked, or startup moved past it:
    // the read takes a new revision.
    if (revision_ == std::numeric_limits<std::uint64_t>::max()) throw std::runtime_error("catalog revisions are exhausted");
    ++revision_;
    if (mode_ == Mode::kDurable) Write(revision_, running_ != 0U || holding_, current);
    return revision_;
}

void CatalogClock::Hold(bool hold) {
    std::lock_guard lock(mutex_);
    holding_ = hold;
    if (mode_ == Mode::kDurable && hold && !marked_) Write(revision_, true, recorded_digest_);
}

void CatalogClock::Settle(std::uint64_t revision) {
    const auto current = CurrentDigest();
    std::lock_guard lock(mutex_);
    // The refresh that took `revision` read every table it needed: published
    // under it, the records are whole, so an idle server does not stay marked.
    if (mode_ == Mode::kDurable && !holding_ && marked_ && running_ == 0U && revision_ == revision) Write(revision_, false, current);
}

}  // namespace chunkdb
