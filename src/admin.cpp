#include "admin.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <system_error>

#include "backup.hpp"
#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "feature_flags.hpp"
#include "process_lock.hpp"
#include "migrations_records.hpp"
#include "store_manifest.hpp"
#include "user_registry.hpp"
#include "verify.hpp"

namespace chunkdb {

void ResetPassword(const std::filesystem::path& data_dir, const std::string& user, std::string_view password) {
    if (password.empty()) {
        throw std::invalid_argument("the new password is empty");
    }
    const auto no_users = [&data_dir] {
        return std::invalid_argument("the data directory " + data_dir.string() + " has no users");
    };
    // A backup is never opened as a data directory; refuse it before the lock
    // creates anything inside it.
    RequireNotBackupDirectory(data_dir, AccessMode::kReadWrite);
    // Checked before the lock too, so a wrong directory gets no lock file.
    if (!std::filesystem::exists(data_dir / kUsersFileName)) {
        throw no_users();
    }
    // Format admission precedes writer-lock and recovery bookkeeping.
    (void)ReadUsersFile(data_dir);
    const auto lock = AcquireWriterLock(data_dir, AccessMode::kReadWrite, /*allow_multiple_processes=*/false);
    // The reset writes where a read-write open writes, so it admits only what
    // such an open admits, before recovery changes anything.
    if (const auto root = ReadDataDirManifest(data_dir)) RequireOpenableFeatures(root->features, AccessMode::kReadWrite);
    // Recovery must precede any verifier change: pending users images must
    // still match their prepared before/after states. The writer lock covers
    // both recovery and the password publication.
    RecoverMigrations(data_dir, AccessMode::kReadWrite);
    if (!ReadUsersFile(data_dir).has_value()) {
        throw no_users();
    }
    UserRegistry registry(data_dir, std::nullopt, {});
    registry.SetVerifier(user, scram::MakeVerifier(password, crypto::RandomBytes(16), scram::kMinIterations));
}

namespace {

// Reads a clock record or initialized marker; std::nullopt when it is absent.
// Inspection and read errors throw, since an unreadable clock may hold a
// higher ceiling than the rebuild would choose.
std::optional<std::vector<std::uint8_t>> ReadClockRecord(const std::filesystem::path& path) {
    std::error_code error;
    const bool present = std::filesystem::exists(path, error);
    if (error) throw std::runtime_error("cannot inspect " + path.string() + ": " + error.message());
    if (!present) return std::nullopt;
    return LoadFile(path);
}

void PublishClockRecord(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    AtomicWrite(path, bytes, /*fsync_file=*/true, /*fsync_directory=*/true, /*out_replaced=*/nullptr,
        /*after_rename_failpoint=*/nullptr, /*enable_generic_failpoints=*/false);
}

}  // namespace

std::vector<RebuiltVersionClock> RebuildVersionClock(const std::filesystem::path& data_dir, std::ostream& findings) {
    // A backup is never opened as a data directory; refuse it before the lock
    // creates anything inside it.
    RequireNotBackupDirectory(data_dir, AccessMode::kReadWrite);
    // Checked before the lock too, so a wrong directory gets no lock file.
    if (!std::filesystem::exists(DataDirManifestPath(data_dir))) {
        throw std::invalid_argument("the directory " + data_dir.string() + " is not a chunkdb data directory");
    }
    const auto lock = AcquireWriterLock(data_dir, AccessMode::kReadWrite, /*allow_multiple_processes=*/false);
    const auto survey = SurveyRevisions(data_dir, findings);
    if (survey.damage != 0U) {
        throw std::runtime_error("the checks found " + std::to_string(survey.damage) +
            " errors other than a lost clock (see the findings above), so the highest revision on disk is unknown; "
            "no clock was changed. Restore a consistent backup");
    }
    // The rebuild writes where a read-write open writes, so it admits only
    // what such an open admits: records of a feature this build can only read
    // may hold revisions the checks skipped.
    if (const auto root = ReadDataDirManifest(data_dir)) RequireOpenableFeatures(root->features, AccessMode::kReadWrite);
    for (const auto& table : survey.tables) {
        if (const auto manifest = ReadStoreManifest(table.table_dir))
            RequireOpenableFeatures(manifest->features, AccessMode::kReadWrite);
    }
    // Every table is decided before any file changes, so a refusal leaves
    // the directory as it was.
    std::vector<RebuiltVersionClock> result;
    for (const auto& table : survey.tables) {
        RebuiltVersionClock rebuilt{
            .table = table.table_dir.filename().string(),
            .highest_revision = table.highest,
            .previous_ceiling = std::nullopt,
            .ceiling = 0,
            .rewritten = false,
            .marker_rewritten = false,
        };
        if (const auto bytes = ReadClockRecord(table.table_dir / "chunkdb.version")) {
            std::uint64_t ceiling = 0;
            if (TryParseVersionClockRecord(*bytes, &ceiling)) rebuilt.previous_ceiling = ceiling;
        }
        const auto marker = ReadClockRecord(table.table_dir / ".chunkdb.initialized");
        const bool keep = rebuilt.previous_ceiling && *rebuilt.previous_ceiling > table.highest;
        if (keep) {
            rebuilt.ceiling = *rebuilt.previous_ceiling;
        } else {
            const std::uint64_t base = std::max(table.highest, survey.version_floor);
            if (base > std::numeric_limits<std::uint64_t>::max() - kRebuiltVersionClockMargin)
                throw std::overflow_error(
                    "chunk version clock exhausted for table " + rebuilt.table + "; no clock was changed");
            rebuilt.ceiling = std::max(base + kRebuiltVersionClockMargin, rebuilt.previous_ceiling.value_or(0U));
        }
        rebuilt.rewritten = !keep;
        rebuilt.marker_rewritten = !marker || !IsValidInitializedStoreMarker(*marker);
        result.push_back(std::move(rebuilt));
    }
    for (std::size_t i = 0; i < result.size(); ++i) {
        const auto& table_dir = survey.tables[i].table_dir;
        if (result[i].rewritten)
            PublishClockRecord(table_dir / "chunkdb.version", SerializeVersionClockRecord(result[i].ceiling));
        // Like first initialization: the clock is durable before the marker.
        if (result[i].marker_rewritten)
            PublishClockRecord(table_dir / ".chunkdb.initialized", SerializeInitializedStoreMarker());
    }
    return result;
}

}  // namespace chunkdb
