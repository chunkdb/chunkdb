#include "admin.hpp"

#include <algorithm>
#include <limits>
#include <sstream>
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
#include "chunk_check.hpp"
#include "chunk_coordinate_index.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feed_slot_records.hpp"
#include "file_permissions.hpp"
#include "quarantine_record.hpp"
#include "populated_index_format.hpp"
#include "shared_file_input.hpp"
#include "txn_history.hpp"
#include "txn_intent_format.hpp"

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

namespace {

void CrashAtFailpoint(const char* point) {
    if (ConsumeFailpointEnv(point)) std::_Exit(86);
}

struct FileSum {
    std::uint64_t size = 0;
    std::uint32_t crc32 = 0;
};

FileSum Sum(const std::filesystem::path& path) {
    const auto bytes = LoadFile(path);
    return {bytes.size(), Crc32(bytes)};
}

// Recovery truncates a WAL a pending rollback intent names to the length it
// records, so such a chunk cannot lose its WAL.
void RequireNoIntentFor(const std::filesystem::path& table_dir, const Geometry& geometry, const StoreId& epoch,
    const ChunkCoord& coord) {
    const auto directory = ConditionalIntentDirectory(table_dir);
    std::error_code error;
    if (!std::filesystem::exists(directory, error) && !error) return;
    for (const auto& file : std::filesystem::directory_iterator(directory)) {
        const auto name = file.path().filename().string();
        if (name.find(".tmp.") != std::string::npos || file.path().extension() != ".rollback") continue;
        bool names = false;
        if (IsTxnIntentArtifactName(name)) {
            TxnIntent intent;
            if (!TryParseTxnIntent(LoadRecoveryTxnIntent(file.path(), geometry.dims(), epoch), &intent, geometry.dims(), epoch))
                throw std::runtime_error("cannot read the recovery intent " + file.path().string());
            names = std::any_of(intent.entries.begin(), intent.entries.end(),
                [&](const TxnIntentEntry& entry) { return entry.coord == coord; });
        } else {
            ChunkCoord named;
            if (!ParseConditionalIntentFileName(name, geometry, &named))
                throw std::runtime_error("cannot read the recovery intent " + file.path().string());
            names = named == coord;
        }
        if (names)
            throw std::runtime_error("the recovery intent " + file.path().string() +
                " needs this chunk's WAL; restore the table from a backup instead");
    }
}

// Recovery needs the artifact of a deletion boundary that the coordinate
// inventory does not list yet; without it the table does not open.
void RequirePublishedBoundary(const std::filesystem::path& table_dir, const Geometry& geometry, const StoreId& epoch,
    const ChunkCoord& coord) {
    const auto large = geometry.ChunkToLarge(coord);
    const auto log_path = LargeChunkDirectory(table_dir, large, geometry.dims()) / kChunkIndexFileName;
    if (!std::filesystem::exists(log_path)) return;
    bool listed = false;
    {
        const auto inventory_path = table_dir / kChunkIndexFileName;
        SharedFileInput inventory(inventory_path);
        if (!inventory) throw std::runtime_error("cannot read the coordinate inventory " + inventory_path.string());
        (void)ReadIndexInventory(inventory, std::filesystem::file_size(inventory_path), geometry, epoch,
            [&](const IndexInventoryEntry& entry) { listed = listed || entry.coord == large; });
    }
    if (listed) return;
    SharedFileInput log(log_path);
    if (!log) throw std::runtime_error("cannot read the coordinate log " + log_path.string());
    if (ReadChunkIndexLog(log, std::filesystem::file_size(log_path), geometry, epoch, large).empty_boundaries.contains(coord))
        throw std::runtime_error("the chunk has a deletion the coordinate inventory does not list yet, and opening the "
            "table needs its files; restore the table from a backup instead");
}

// Marks every slot that is not lost so that its next watch starts with
// resync; returns how many it marked.
std::size_t RequireSlotResync(const std::filesystem::path& table_dir, const StoreId& epoch, std::uint8_t dims) {
    auto records = ReadFeedSlotRecords(table_dir, epoch, dims);
    if (!records) return 0U;
    std::size_t marked = 0U;
    for (auto& slot : records->slots) {
        if (slot.lost || slot.resync) continue;
        slot.resync = true;
        ++marked;
    }
    if (marked != 0U) WriteFeedSlotRecords(table_dir, *records);
    return marked;
}

}  // namespace

QuarantinedChunk QuarantineChunk(const std::filesystem::path& data_dir, const std::string& table, std::uint8_t dims,
    const ChunkCoord& coord, bool force, std::ostream& findings) {
    RequireNotBackupDirectory(data_dir, AccessMode::kReadWrite);
    RequireValidTableName(table);
    if (!std::filesystem::exists(DataDirManifestPath(data_dir)))
        throw std::invalid_argument("the directory " + data_dir.string() + " is not a chunkdb data directory");
    const auto lock = AcquireWriterLock(data_dir, AccessMode::kReadWrite, /*allow_multiple_processes=*/false);
    // A decided migration may still change the table's files.
    RecoverMigrations(data_dir, AccessMode::kReadWrite);
    const auto table_dir = data_dir / "tables" / table;
    const auto manifest = ReadStoreManifest(table_dir);
    if (!manifest) throw std::invalid_argument("the data directory has no table " + table);
    RequireOpenableFeatures(manifest->features, AccessMode::kReadWrite);
    const Geometry geometry(manifest->geometry, manifest->schema, *manifest->chunk_schema);
    if (dims != geometry.dims())
        throw std::invalid_argument("table " + table + " has " + std::to_string(geometry.dims()) +
            " dimensions; give one coordinate for each");
    const auto stem = ChunkFileStem(coord, dims);
    const auto described = "chunk " + stem.substr(2U) + " of table " + table;
    RequireNoIntentFor(table_dir, geometry, manifest->store_id, coord);
    // The WAL first: removing it first never leaves WAL frames without the
    // image they apply to.
    std::vector<std::filesystem::path> present;
    for (const auto& path : {ChunkWalPath(table_dir, geometry, coord), ChunkDataPath(table_dir, geometry, coord)}) {
        const auto status = std::filesystem::symlink_status(path);
        if (status.type() == std::filesystem::file_type::not_found) continue;
        if (status.type() != std::filesystem::file_type::regular)
            throw std::runtime_error(path.string() + " is not a regular file");
        present.push_back(path);
    }
    QuarantinedChunk result;
    const auto root = table_dir / kQuarantineDirName;
    std::filesystem::path entry;
    if (std::filesystem::exists(root)) {
        for (const auto& candidate : std::filesystem::directory_iterator(root)) {
            const auto name = candidate.path().filename().string();
            if (!name.starts_with(stem + ".") || !std::filesystem::is_directory(candidate.symlink_status())) continue;
            const auto record_path = candidate.path() / kQuarantineRecordName;
            if (!std::filesystem::exists(record_path)) {
                // Interrupted before its record: it holds only links to files
                // still in place.
                std::filesystem::remove_all(candidate.path());
                SyncDirectoryPath(root);
                continue;
            }
            const auto record = ParseQuarantineRecord(LoadFile(record_path));
            if (record.table_epoch != manifest->store_id || record.coord != coord) continue;
            // Interrupted after its record: every remaining file is in it.
            const bool holds_all = std::all_of(present.begin(), present.end(), [&](const std::filesystem::path& path) {
                const auto sum = Sum(path);
                return std::any_of(record.files.begin(), record.files.end(), [&](const QuarantineFile& file) {
                    return file.name == path.filename().string() && file.size == sum.size && file.crc32 == sum.crc32;
                });
            });
            if (holds_all && !result.resumed) {
                entry = candidate.path();
                result.resumed = true;
                for (const auto& file : record.files) result.files.push_back(file.name);
            }
        }
    }
    if (present.empty()) {
        if (!result.resumed) throw std::invalid_argument(described + " has no image or WAL");
        // An earlier run finished: nothing is left to move or to mark.
        result.already_quarantined = true;
        result.entry = entry.lexically_relative(data_dir);
        return result;
    }
    if (!result.resumed) {
        RequirePublishedBoundary(table_dir, geometry, manifest->store_id, coord);
        const auto check = CheckChunkFiles(table_dir, geometry, manifest->store_id, manifest->features, coord,
            ChunkCoordinateIndex(table_dir, geometry, manifest->store_id, true).EmptyBoundary(coord));
        for (const auto& finding : check.findings)
            findings << (finding.error ? "error " : "warning ") << finding.code << ' '
                     << finding.path.lexically_relative(data_dir).generic_string() << ": " << finding.detail << '\n';
        if (const auto* damage = check.damage()) result.damage = damage->code;
        else if (!force)
            throw std::invalid_argument("the chunkdb_verify checks find no damage in " + described +
                "; give --force to quarantine it anyway");
        EnsureDirectoryPathExists(root, /*durable_sync=*/true);
        entry = root / (stem + "." + std::to_string(UnixMillisNow()) + "." + StoreIdHex(NewStoreId()).substr(0U, 8U));
        if (!CreateOwnerOnlyDirectory(entry)) throw std::runtime_error("quarantine entry already exists: " + entry.string());
        SyncDirectoryPath(root);
        QuarantineRecord record{.table_epoch = manifest->store_id, .dims = dims, .coord = coord,
            .quarantined_at_ms = UnixMillisNow(), .files = {}};
        for (const auto& path : present) {
            const auto name = path.filename().string();
            std::filesystem::create_hard_link(path, entry / name);
            SyncFilePath(entry / name);
            const auto sum = Sum(entry / name);
            record.files.push_back({name, sum.size, sum.crc32});
            result.files.push_back(name);
        }
        SyncDirectoryPath(entry);
        CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_QUARANTINE_BEFORE_RECORD_ONCE");
        AtomicWrite(entry / kQuarantineRecordName, SerializeQuarantineRecord(record), /*fsync_file=*/true,
            /*fsync_directory=*/true, nullptr, nullptr, /*enable_generic_failpoints=*/false);
        CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_QUARANTINE_AFTER_RECORD_ONCE");
    }
    // Consumers saw changes of this chunk that its removal takes back.
    result.slots_marked = RequireSlotResync(table_dir, manifest->store_id, dims);
    for (const auto& path : present) {
        std::filesystem::remove(path);
        SyncDirectoryPath(path.parent_path());
        CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_QUARANTINE_AFTER_REMOVE_ONCE");
    }
    result.entry = entry.lexically_relative(data_dir);
    std::ostringstream remaining;
    result.table_errors = VerifyTableDirectory(table_dir, remaining).errors;
    findings << remaining.str();
    return result;
}

}  // namespace chunkdb
