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
constexpr std::size_t kReservedOffset = 32U;
constexpr std::size_t kCrcOffset = 36U;

}  // namespace

std::vector<std::uint8_t> EncodeCatalogRevision(const CatalogRevisionRecord& record) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kCatalogRevisionFileSize);
    bytes.insert(bytes.end(), kMagic.begin(), kMagic.end());
    WriteLe16(bytes, kFormat);
    WriteLe16(bytes, record.in_progress ? kInProgress : 0U);
    bytes.insert(bytes.end(), record.epoch.begin(), record.epoch.end());
    WriteLe64(bytes, record.revision);
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

CatalogClock::CatalogClock(std::filesystem::path data_dir, Mode mode) : data_dir_(std::move(data_dir)), mode_(mode) {
    if (mode_ == Mode::kMemory) {
        epoch_ = NewStoreId();
        return;
    }
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
        // A statement may have changed the catalog after the recorded
        // revision named it: the catalog as it is now gets a new one.
        revision_ = record->revision + (record->in_progress ? 1U : 0U);
    } else {
        epoch_ = NewStoreId();
        revision_ = 0U;
    }
    marked_ = mode_ == Mode::kShared;
    Write(revision_, marked_);
}

std::uint64_t CatalogClock::revision() const {
    std::lock_guard lock(mutex_);
    return revision_;
}

void CatalogClock::Write(std::uint64_t revision, bool in_progress) {
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CATALOG_REVISION_WRITE_FAIL_ONCE"))
        throw std::runtime_error("injected catalog revision write failure");
    AtomicWrite(data_dir_ / std::string(kCatalogRevisionFileName),
        EncodeCatalogRevision(CatalogRevisionRecord{.epoch = epoch_, .revision = revision, .in_progress = in_progress}),
        /*fsync_file=*/true, /*fsync_directory=*/true, /*out_replaced=*/nullptr, /*after_rename_failpoint=*/nullptr,
        /*enable_generic_failpoints=*/false);
    if (in_progress && ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CRASH_CATALOG_AFTER_MARK_ONCE")) ExitAbruptly(86);
    if (!in_progress && ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CRASH_CATALOG_AFTER_REVISION_ONCE")) ExitAbruptly(86);
}

void CatalogClock::Begin() {
    std::lock_guard lock(mutex_);
    if (mode_ == Mode::kDurable && !marked_) {
        Write(revision_, true);
        marked_ = true;
    }
    ++running_;
}

std::uint64_t CatalogClock::End() {
    std::lock_guard lock(mutex_);
    if (running_ == 0U) throw std::logic_error("catalog clock ended a statement it did not begin");
    --running_;
    if (revision_ == std::numeric_limits<std::uint64_t>::max()) throw std::runtime_error("catalog revisions are exhausted");
    ++revision_;
    if (mode_ == Mode::kDurable) {
        // On failure the record keeps the mark Begin wrote, so a start moves
        // past every revision handed out.
        Write(revision_, running_ != 0U);
        marked_ = running_ != 0U;
    }
    return revision_;
}

std::uint64_t CatalogClock::Snapshot() {
    std::lock_guard lock(mutex_);
    if (running_ == 0U) return revision_;
    if (revision_ == std::numeric_limits<std::uint64_t>::max()) throw std::runtime_error("catalog revisions are exhausted");
    ++revision_;
    // Still marked: a statement runs.
    if (mode_ == Mode::kDurable) Write(revision_, true);
    return revision_;
}

}  // namespace chunkdb
