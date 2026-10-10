#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <fstream>
#include <memory>
#include <map>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "chunkdb/chunk_vars.hpp"
#include "chunkdb/geometry.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/server_defaults.hpp"
#include "chunkdb/types.hpp"

namespace chunkdb {

#if defined(__MINGW32__) && \
    (!defined(CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS) || CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS)
// MinGW winpthreads shared_mutex has shown unstable lock assertions under high contention
// in CI; fallback preserves correctness by serializing shared/exclusive access on Windows+MinGW.
class RegularChunkMutex {
  public:
    void lock() { mutex_.lock(); }
    bool try_lock() { return mutex_.try_lock(); }
    void unlock() { mutex_.unlock(); }

    void lock_shared() { mutex_.lock(); }
    bool try_lock_shared() { return mutex_.try_lock(); }
    void unlock_shared() { mutex_.unlock(); }

  private:
    std::mutex mutex_;
};
inline constexpr const char* kChunkLockModeName = "serial-mutex";
#else
using RegularChunkMutex = std::shared_mutex;
inline constexpr const char* kChunkLockModeName = "shared-mutex";
#endif

[[nodiscard]] inline constexpr const char* ChunkLockModeName() noexcept {
    return kChunkLockModeName;
}

enum class DurabilityMode {
    kRelaxed = 0,
    kFsyncWal = 1,
    kFsyncCheckpoint = 2,
};

enum class AccessMode {
    kReadWrite = 0,
    kReadOnly = 1,
};

enum class CheckpointCompression {
    kNone = 0,
    kZrle = 1,
};

enum class ConditionalMutationPausePoint {
    kNone = 0,
    kBeforeRollbackIntentPublish,
    kAfterRollbackIntentPublish,
    kAfterWalAppend,
    kAfterCommitIntentPublish,
    kAfterCommitIntentUnlink,
};

enum class ReadOnlySnapshotArtifact {
    kImage,
    kWal,
    kIntent,
};

struct ReadOnlySnapshotPausePoint {
    std::size_t collection = 0;
    ReadOnlySnapshotArtifact artifact = ReadOnlySnapshotArtifact::kImage;
};

[[nodiscard]] CheckpointCompression ParseCheckpointCompression(std::string_view text);
[[nodiscard]] const char* CheckpointCompressionName(CheckpointCompression compression) noexcept;

[[nodiscard]] DurabilityMode ParseDurabilityMode(std::string_view text);
[[nodiscard]] const char* DurabilityModeName(DurabilityMode mode) noexcept;
[[nodiscard]] const char* AccessModeName(AccessMode mode) noexcept;

struct StoreRuntimeStats {
    std::uint64_t evictions = 0;
    std::uint64_t checkpoints = 0;
    std::uint64_t wal_batch_flushes = 0;
    std::uint64_t unique_loaded_chunks = 0;
    std::uint64_t open_wal_streams = 0;
    std::uint64_t eviction_snapshot_builds = 0;
    std::uint64_t eviction_probes = 0;
    std::uint64_t eviction_no_progress_cycles = 0;
    std::uint64_t eviction_forced_wal_flushes = 0;
    std::uint64_t eviction_forced_wal_flushes_with_data = 0;
    std::uint64_t eviction_forced_wal_flushes_empty_batch = 0;
    std::uint64_t eviction_recency_skips = 0;
    std::uint64_t empty_chunk_gcs = 0;
    std::uint64_t wal_barriers = 0;
    std::uint64_t wal_barrier_full_syncs = 0;
    std::uint64_t background_checkpoints = 0;
    std::uint64_t background_checkpoint_failures = 0;
    std::uint64_t background_queue_full_inline = 0;
    std::uint64_t background_queue_depth = 0;
    std::uint64_t compressed_checkpoint_images = 0;
};

// Random identity of a data directory, recorded in its manifest.
using StoreId = std::array<std::uint8_t, 16>;

// Feature flags of a store and of each file it writes
// (docs/STORAGE_FORMAT.md). A reader that does not know a bit of
//   incompat  - must not open the store,
//   ro_compat - may open it read-only and must not write,
//   compat    - may ignore it.
struct FeatureFlags {
    std::uint32_t incompat = 0;
    std::uint32_t ro_compat = 0;
    std::uint32_t compat = 0;
};

// Bits of StoreConfig::geometry_fields, one per GeometryConfig field.
enum GeometryField : std::uint32_t {
    kGeometryLargeChunkWidth = 1U << 0U,
    kGeometryLargeChunkHeight = 1U << 1U,
    kGeometryChunkWidth = 1U << 2U,
    kGeometryChunkHeight = 1U << 3U,
    kGeometryBlockBits = 1U << 4U,
};
inline constexpr std::uint32_t kAllGeometryFields =
    kGeometryLargeChunkWidth | kGeometryLargeChunkHeight | kGeometryChunkWidth |
    kGeometryChunkHeight | kGeometryBlockBits;

// Settings of a table, recorded in its manifest and changeable after
// creation (docs/STORAGE_FORMAT.md).
struct TableOptions {
    DurabilityMode durability_mode = DurabilityMode::kRelaxed;
    std::size_t checkpoint_update_interval = 256;
    std::size_t checkpoint_wal_bytes = 1024 * 1024;
    std::size_t wal_group_commit_updates = kDefaultWalGroupCommitUpdates;
    CheckpointCompression checkpoint_compression = CheckpointCompression::kNone;
    // The most bytes the values of a chunk's text and bytes columns may take,
    // as ChunkVars::encoded_size.
    std::size_t var_max_chunk_bytes = kDefaultVarMaxChunkBytes;
};

class StoreResources;
class ProcessLock;

// Default limit of the chunk states a store keeps for open transactions.
inline constexpr std::size_t kDefaultTxnHistoryBytes = 64ULL * 1024ULL * 1024ULL;

struct StoreConfig {
    // Geometry is fixed when a store is created and recorded in its manifest.
    // A new store is created with `geometry`. An existing store opens with the
    // recorded geometry, and every field named in `geometry_fields` must
    // match it or the store refuses to open.
    GeometryConfig geometry;
    std::uint32_t geometry_fields = kAllGeometryFields;
    // The columns of a new store; geometry.block_bits must then be their
    // FixedBitsPerBlock. Without it a new store has one column
    // bits(geometry.block_bits). An existing store opens with its recorded
    // columns, and a given schema must equal them.
    std::optional<TableSchema> schema = std::nullopt;
    std::filesystem::path data_dir;

    DurabilityMode durability_mode = DurabilityMode::kRelaxed;
    std::size_t checkpoint_update_interval = 256;
    std::size_t checkpoint_wal_bytes = 1024 * 1024;
    std::size_t wal_group_commit_updates = kDefaultWalGroupCommitUpdates;

    std::size_t max_loaded_chunks = kDefaultMaxLoadedChunks;
    std::size_t max_open_wal_streams = 1024;
    bool allow_multiple_processes = false;
    AccessMode access_mode = AccessMode::kReadWrite;

    // When true, checkpoint compaction and cache eviction run on a dedicated
    // maintenance thread with a bounded queue instead of on request threads.
    bool background_maintenance = false;
    std::size_t background_checkpoint_queue_limit = 4096;

    // Optional compression for newly written checkpoint images. Off by
    // default; images written by older versions remain readable either way.
    CheckpointCompression checkpoint_compression = CheckpointCompression::kNone;

    // As TableOptions.
    std::size_t var_max_chunk_bytes = kDefaultVarMaxChunkBytes;

    // Cache and WAL-stream budgets shared with the other stores of this
    // process (the tables of one server). When null, the store gets budgets
    // of its own sized by max_loaded_chunks and max_open_wal_streams, which
    // are then ignored for a shared one.
    std::shared_ptr<StoreResources> resources = nullptr;
    // False when the caller already holds the writer lock of the data
    // directory this store belongs to (a table of a data directory).
    bool acquire_process_lock = true;
    // A new version clock starts at this token instead of 1; an existing
    // clock ignores it. The table catalog sets it to the data directory's
    // version floor, so a table dropped and created again under the same
    // name never reuses a token of the earlier table.
    std::uint64_t initial_version_floor = 0;
    // The most bytes of chunk states the store keeps for open transactions
    // (docs/TRANSACTIONS_DESIGN.md); past it the oldest transactions are
    // unregistered.
    std::size_t txn_history_bytes = kDefaultTxnHistoryBytes;
    // Per durable change-feed slot, including archived base images.
    std::size_t slot_max_bytes = 1024ULL * 1024ULL * 1024ULL;
    std::chrono::milliseconds slot_sync_interval{100};
};

// Server-side hard limits for world-oriented read operations.
inline constexpr std::size_t kMaxChunkRangeChunks = 256;
inline constexpr std::size_t kMaxChunkScanLimit = 1024;
inline constexpr std::size_t kMaxChunkBatchOps = 1024;
// Hard byte budget for one CHUNKRANGE/CHUNKRADIUS response body. Enforced
// before per-chunk state strings are extracted so a request over a large
// geometry fails with a bounded error instead of allocating the response.
inline constexpr std::size_t kMaxChunkRangeResponseBytes = 64ULL * 1024ULL * 1024ULL;
// Snapshot-generation bracket linger. Each odd->even bracket costs three
// durable syncs of a 16-byte record, so publishing the even record the
// instant a transition ends makes one cache eviction cost one full bracket.
// Instead the even publication is deferred for this window, letting
// back-to-back transitions run inside one already-open odd epoch exactly the
// way concurrent writers already coalesce. The window bounds how long a
// read-only reader can see an odd (unstable) generation, so it is kept well
// inside the reader's retry budget (see kReadOnlySnapshotBackoffBudgetMs in
// src/snapshot_generation.cpp).
inline constexpr std::uint64_t kDefaultSnapshotGenerationLingerMs = 50;
// Hard cap on transitions served by one odd epoch, so a pathologically fast
// writer cannot keep an epoch open on bracket count alone.
inline constexpr std::size_t kDefaultSnapshotGenerationLingerMaxBrackets = 512;

struct ChunkScanPage {
    std::vector<ChunkCoord> coords;
    bool has_more = false;
};

struct ChunkRangeEntry {
    ChunkCoord coord;
    // Packed, as GetChunkStateBytes returns them.
    std::vector<std::uint8_t> payload;
    std::vector<std::uint8_t> presence_bitmap;
    // The chunk version, as GetChunkVersion returns it.
    std::uint64_t version = 0;
    // The text and bytes values, when the read asked for them.
    ChunkVars vars{};
};

// A chunk's whole state: what GET CHUNK and SET CHUNK carry
// (docs/CQL.md).
struct ChunkState {
    std::uint64_t version = 0;
    std::vector<std::uint8_t> payload{};
    std::vector<std::uint8_t> presence_bitmap{};
    ChunkVars vars{};
};

// SET (`set` true, `bits`) or UNSET.
struct ChunkBatchOp {
    bool set = false;
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::string bits{};
};

struct ChunkMutationResult {
    bool ok = false;
    // On success: the chunk version after the mutation.
    // On version mismatch: the current chunk version.
    std::uint64_t version = 0;
};

// Bounded scan-candidate accumulator; defined in world_read.cpp.
class ScanCandidateAccumulator;

// A write failed after a point where it may already be applied, for
// example its WAL bytes could not be removed after a failed append. The
// store is fail-closed until restart; the caller must treat the outcome as
// unknown (any other write error means "not applied").
class WriteOutcomeUnknownError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Transactions (docs/TRANSACTIONS_DESIGN.md).

// The most chunks one transaction may write and read.
inline constexpr std::size_t kMaxTxnWrittenChunks = 64;
inline constexpr std::size_t kMaxTxnReadChunks = 1024;

// Why a transaction ended without writing anything.
enum class TxnConflictReason {
    // A chunk it read or wrote changed after its snapshot.
    kChunkChanged,
    // It was open longer than its duration limit.
    kDuration,
    // The store's kept states reached their byte limit.
    kHistoryLimit,
    // The table was altered or dropped after the snapshot.
    kTableChanged,
};

[[nodiscard]] const char* TxnConflictReasonName(TxnConflictReason reason) noexcept;

// The transaction ended without changing anything; running it again may
// succeed.
class TransactionConflictError : public std::runtime_error {
  public:
    TransactionConflictError(TxnConflictReason reason, const std::string& message)
        : std::runtime_error(message), reason_(reason) {}

    [[nodiscard]] TxnConflictReason reason() const noexcept { return reason_; }

  private:
    TxnConflictReason reason_;
};

class TxnHistory;
struct TxnKeep;

// A transaction's snapshot of one store: reads through it see the store as
// of version(), the last version the store had issued when it was taken.
// It stays registered until the transaction commits, ends, or is
// unregistered by a limit; destroying it ends it.
class TxnSnapshot {
  public:
    TxnSnapshot(const TxnSnapshot&) = delete;
    TxnSnapshot& operator=(const TxnSnapshot&) = delete;
    ~TxnSnapshot();

    [[nodiscard]] std::uint64_t version() const noexcept { return version_; }

  private:
    friend class TxnHistory;
    friend class ChunkStore;

    TxnSnapshot() = default;

    std::shared_ptr<TxnHistory> history_;
    std::uint64_t version_ = 0;
    // Guarded by the history's lock.
    bool registered_ = false;
    // Set when a limit unregistered the snapshot.
    std::optional<TxnConflictReason> end_reason_;
    std::multimap<std::uint64_t, TxnSnapshot*>::iterator by_version_{};
    std::multimap<std::chrono::steady_clock::time_point, TxnSnapshot*>::iterator by_deadline_{};
};

// A chunk a transaction writes and its new state; state.version is not
// used.
struct TxnChunkWrite {
    ChunkCoord coord{};
    ChunkState state{};
};

// A transaction's area read: its private copy of a chunk in place of the
// snapshot's (null when it has none), and the chunks the read covered,
// populated or not.
struct TxnAreaRead {
    std::function<const ChunkState*(const ChunkCoord&)> overlay{};
    std::vector<ChunkCoord>* covered = nullptr;
};

// Test pause points of transactions.
enum class TxnPausePoint {
    kNone = 0,
    // A snapshot registration has read its version and is about to publish
    // it, under the history lock.
    kRegisterBeforePublish,
    // A plain write took its version and found an open transaction, under
    // its chunk's lock.
    kWriteAfterOpenCount,
    // The transaction released its chunk locks, before publishing any
    // uncertain outcome and completing its post-commit bookkeeping.
    kBeforePostCommitOutcome,
};

class ChunkStore {
  public:
    explicit ChunkStore(StoreConfig config);
    ~ChunkStore();

    ChunkStore(const ChunkStore&) = delete;
    ChunkStore& operator=(const ChunkStore&) = delete;

    [[nodiscard]] const Geometry& geometry() const noexcept { return geometry_; }
    [[nodiscard]] const std::shared_ptr<StoreResources>& resources() const noexcept {
        return resources_;
    }
    [[nodiscard]] const StoreId& store_id() const noexcept { return store_id_; }
    [[nodiscard]] const FeatureFlags& features() const noexcept { return features_; }
    [[nodiscard]] const std::filesystem::path& data_dir() const noexcept { return data_dir_; }
    [[nodiscard]] DurabilityMode durability_mode() const noexcept { return durability_mode_; }
    [[nodiscard]] AccessMode access_mode() const noexcept { return access_mode_; }
    [[nodiscard]] CheckpointCompression checkpoint_compression() const noexcept {
        return checkpoint_compression_;
    }
    [[nodiscard]] std::size_t var_max_chunk_bytes() const noexcept { return var_max_chunk_bytes_; }

    [[nodiscard]] bool BlockExists(std::int64_t block_x, std::int64_t block_y);
    [[nodiscard]] std::string GetBlockBits(std::int64_t block_x, std::int64_t block_y);
    // The block's bits, or std::nullopt when it is unset; one read of the
    // chunk, so presence and bits always agree.
    [[nodiscard]] std::optional<std::string> ReadBlockBits(
        std::int64_t block_x,
        std::int64_t block_y);
    void SetBlockBits(std::int64_t block_x, std::int64_t block_y, std::string_view bits);
    // Removes the block with all its values.
    void UnsetBlock(std::int64_t block_x, std::int64_t block_y);
    // UnsetBlock that, given `expected_version`, changes nothing and returns
    // the current version when the chunk's version differs; otherwise the
    // chunk version after the write.
    [[nodiscard]] ChunkMutationResult UnsetBlock(
        std::int64_t block_x,
        std::int64_t block_y,
        std::optional<std::uint64_t> expected_version);
    // Writes the given columns of a block (docs/COLUMNS_DESIGN.md). A new
    // block takes, for each column not given, its DEFAULT, NULL for a NULL
    // column, or zero; it is refused while a REQUIRED column is missing.
    // Throws std::invalid_argument for an unknown or repeated column and for
    // a value that does not fit its column; nothing changes then.
    void SetBlock(std::int64_t block_x, std::int64_t block_y, const std::vector<ColumnAssignment>& values);
    // SetBlock with `expected_version` as for UnsetBlock above.
    [[nodiscard]] ChunkMutationResult SetBlock(
        std::int64_t block_x,
        std::int64_t block_y,
        const std::vector<ColumnAssignment>& values,
        std::optional<std::uint64_t> expected_version);
    // One value per column of geometry().layout().schema(), in its order, or
    // std::nullopt when the block is absent.
    [[nodiscard]] std::optional<std::vector<ColumnValue>> GetBlock(std::int64_t block_x, std::int64_t block_y);
    // The first stored value of column `column_id` that `type` does not
    // hold, as "block (x, y) holds <value>", or std::nullopt when every one
    // fits. Reads every populated chunk (TableCatalog::NarrowColumn).
    [[nodiscard]] std::optional<std::string> FindValueNotFitting(std::uint32_t column_id, const ColumnType& type);

    [[nodiscard]] bool ChunkExists(std::int64_t chunk_x, std::int64_t chunk_y);
    void SetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y, std::string_view bits);
    [[nodiscard]] std::string GetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] std::vector<std::uint8_t> GetChunkPayloadBytes(std::int64_t chunk_x, std::int64_t chunk_y);
    void SetChunkStateBits(
        std::int64_t chunk_x,
        std::int64_t chunk_y,
        std::string_view payload_bits,
        std::string_view presence_bits);
    [[nodiscard]] std::string GetChunkStateBits(std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] std::vector<std::uint8_t> GetChunkStateBytes(std::int64_t chunk_x, std::int64_t chunk_y);
    // Packed-byte counterparts of SetChunkBits / SetChunkStateBits. The
    // payload must be exactly ChunkPayloadBytes() long and the presence bitmap
    // exactly ChunkPresenceBitmapBytes() long (the layout CHUNKGET STATE returns);
    // padding bits past the used range are ignored and stored as zero.
    // Both return the chunk version after the write (the current one when
    // nothing changed).
    std::uint64_t SetChunkPayloadBytes(
        std::int64_t chunk_x,
        std::int64_t chunk_y,
        const std::vector<std::uint8_t>& payload);
    std::uint64_t SetChunkStateBytes(
        std::int64_t chunk_x,
        std::int64_t chunk_y,
        const std::vector<std::uint8_t>& payload,
        const std::vector<std::uint8_t>& presence_bitmap);


    // World-oriented reads. Populated-ness of each chunk is evaluated
    // atomically per chunk at read time. Absent chunks probed by these reads
    // are not inserted into the cache, with one bounded exception: after
    // kNoCacheReadAttempts contended attempts on one chunk, the read falls
    // back to the authoritative cache path, which loads (and caches) that
    // chunk to preserve read-your-writes consistency.
    [[nodiscard]] ChunkScanPage ScanPopulatedChunks(
        bool has_cursor,
        ChunkCoord cursor,
        std::size_t limit);
    // With `with_vars`, each entry carries its text and bytes values, and
    // they count toward the response-byte limit.
    [[nodiscard]] std::vector<ChunkRangeEntry> ReadChunkRange(
        std::int64_t chunk_x0,
        std::int64_t chunk_y0,
        std::int64_t chunk_x1,
        std::int64_t chunk_y1,
        bool with_vars = false);
    // Radius-oriented world read: returns the populated chunks whose chunk
    // coordinate lies within Euclidean distance `radius_chunks` of the
    // center, ordered by ascending cx then cy. Bounded by the same chunk
    // count and response-byte limits as ReadChunkRange.
    [[nodiscard]] std::vector<ChunkRangeEntry> ReadChunkRadius(
        std::int64_t center_x,
        std::int64_t center_y,
        std::int64_t radius_chunks,
        bool with_vars = false);

    // The chunk's state read under one lock. A chunk without blocks has an
    // empty presence bitmap and still a version, which a conditional write
    // that creates it compares against.
    [[nodiscard]] ChunkState ReadChunkState(std::int64_t chunk_x, std::int64_t chunk_y);
    // Replaces the chunk's payload, presence and text and bytes values as
    // one mutation; `state.version` is not used. Throws std::invalid_argument
    // when a size does not match the table, a value is not one of a present
    // block of a text or bytes column, or a value does not fit; nothing
    // changes then. With `expected_version` as SetBlock.
    [[nodiscard]] ChunkMutationResult WriteChunkState(
        std::int64_t chunk_x,
        std::int64_t chunk_y,
        ChunkState state,
        std::optional<std::uint64_t> expected_version);

    // Chunk concurrency primitives. Versions are opaque 64-bit tokens drawn
    // from a store-wide monotonic clock whose ceiling is persisted in the
    // data directory, so on a read-write store a version issued before a
    // mutation, eviction, or restart can never match a version issued
    // afterwards. Read-only stores fall back to random epoch tokens (they
    // cannot persist the clock and reject CAS anyway).
    [[nodiscard]] std::uint64_t GetChunkVersion(std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] ChunkMutationResult CasChunkState(
        std::int64_t chunk_x,
        std::int64_t chunk_y,
        std::uint64_t expected_version,
        std::string_view payload_bits,
        std::string_view presence_bits);
    // CasChunkState with packed bytes (the CHUNKGET STATE layout); sizes as
    // for SetChunkStateBytes.
    [[nodiscard]] ChunkMutationResult CasChunkStateBytes(
        std::int64_t chunk_x,
        std::int64_t chunk_y,
        std::uint64_t expected_version,
        const std::vector<std::uint8_t>& payload,
        const std::vector<std::uint8_t>& presence_bitmap);
    [[nodiscard]] ChunkMutationResult ApplyChunkBatch(
        std::int64_t chunk_x,
        std::int64_t chunk_y,
        bool has_expected_version,
        std::uint64_t expected_version,
        const std::vector<ChunkBatchOp>& ops);

    // Explicit global durability barrier: when this returns, every write
    // acknowledged before the call began is durable on stable storage,
    // regardless of the configured durability mode. Failures propagate.
    void WalBarrier();

    // Transactions (docs/TRANSACTIONS_DESIGN.md). A snapshot sees the store
    // as of the last version issued when it was taken, until `max_duration`
    // passes. Throws std::invalid_argument on a read-only store and on one
    // opened with allow_multiple_processes: its history would not see every
    // write.
    [[nodiscard]] std::unique_ptr<TxnSnapshot> BeginTxnSnapshot(std::chrono::milliseconds max_duration);
    // Whether the snapshot was taken of this store: a table altered or
    // dropped since has another.
    [[nodiscard]] bool OwnsTxnSnapshot(const TxnSnapshot& snapshot) const noexcept;
    // Unregisters the snapshot (ROLLBACK); a later use of it is an error.
    void EndTxnSnapshot(TxnSnapshot& snapshot) noexcept;
    // Reads as of the snapshot. Throw TransactionConflictError when a limit
    // unregistered it, and std::invalid_argument when it has ended or belongs
    // to another store. The version reported is the chunk's as of the
    // snapshot.
    [[nodiscard]] ChunkState ReadChunkStateAt(const TxnSnapshot& snapshot, std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] std::optional<std::vector<ColumnValue>> GetBlockAt(
        const TxnSnapshot& snapshot,
        std::int64_t block_x,
        std::int64_t block_y);
    [[nodiscard]] std::vector<ChunkRangeEntry> ReadChunkRangeAt(
        const TxnSnapshot& snapshot,
        std::int64_t chunk_x0,
        std::int64_t chunk_y0,
        std::int64_t chunk_x1,
        std::int64_t chunk_y1,
        bool with_vars = false,
        const TxnAreaRead& txn_read = {});
    [[nodiscard]] std::vector<ChunkRangeEntry> ReadChunkRadiusAt(
        const TxnSnapshot& snapshot,
        std::int64_t center_x,
        std::int64_t center_y,
        std::int64_t radius_chunks,
        bool with_vars = false,
        const TxnAreaRead& txn_read = {});
    // Whether a write changed the chunk after the snapshot.
    [[nodiscard]] bool ChunkChangedSince(const TxnSnapshot& snapshot, std::int64_t chunk_x, std::int64_t chunk_y);
    // Commits a transaction: `writes` (each chunk once) apply all together
    // or not at all, also across a crash, and are durable when this returns
    // in every durability mode. Every chunk in `read_set` and `writes` must
    // be unchanged since the snapshot. Returns the version every changed
    // chunk now has, or 0 when no write changes its chunk (without writes
    // the snapshot only has to be still registered). Throws
    // std::invalid_argument past kMaxTxnWrittenChunks or kMaxTxnReadChunks
    // or for an invalid state, leaving the snapshot registered; otherwise
    // the snapshot ends whatever happens. Throws TransactionConflictError
    // when a chunk changed or a limit unregistered the snapshot, and other
    // errors when the commit failed; in both cases nothing changed, except
    // for a WriteOutcomeUnknownError (the commit record could not be made
    // durable).
    std::uint64_t CommitTransaction(
        TxnSnapshot& snapshot,
        const std::vector<ChunkCoord>& read_set,
        std::vector<TxnChunkWrite> writes);
    // A transaction's writes go to its private copy of a chunk: SET BLOCK and
    // DELETE BLOCK applied to `state`, the chunk holding the block, with the
    // checks SetBlock and UnsetBlock make. Throw std::invalid_argument and
    // leave `state` unchanged when a value does not fit.
    void SetBlockInState(
        ChunkState& state,
        std::int64_t block_x,
        std::int64_t block_y,
        const std::vector<ColumnAssignment>& values) const;
    void UnsetBlockInState(ChunkState& state, std::int64_t block_x, std::int64_t block_y) const;
    // A whole new state for a transaction's copy (SET CHUNK), checked and
    // canonicalized as WriteChunkState does; `current` is the copy it
    // replaces.
    void PrepareTxnChunkState(ChunkState& state, const ChunkVars& current) const;
    // The block's values in `state`, the chunk holding it, as GetBlock
    // returns them.
    [[nodiscard]] std::optional<std::vector<ColumnValue>> BlockInState(
        const ChunkState& state,
        std::int64_t block_x,
        std::int64_t block_y) const;

    [[nodiscard]] std::size_t ApproxLoadedChunkCount() const;
    [[nodiscard]] StoreRuntimeStats RuntimeStats() const noexcept;
    // Commit time (Unix ms) of the chunk's last mutation, as loaded or set.
    [[nodiscard]] std::uint64_t ChunkCommitTimeForTests(std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] std::uint64_t WalOpenCountForTests() const noexcept;
    [[nodiscard]] std::uint64_t WalParentPrepareCountForTests() const noexcept;
    [[nodiscard]] std::uint64_t OpenWalStreamCountForTests() const noexcept;
    [[nodiscard]] std::size_t MaxOpenWalStreamsForTests() const noexcept;
    [[nodiscard]] std::uint64_t EvictionSnapshotBuildCountForTests() const noexcept;
    // Number of L_<lx>_<ly> directories whose files were listed by CHUNKSCAN
    // candidate collection (cumulative). Lets tests prove a page only visits
    // the large-chunk columns it needs.
    [[nodiscard]] std::uint64_t ScanLargeDirsListedForTests() const noexcept;
    [[nodiscard]] std::uint64_t ScanCatalogBuildsForTests() const noexcept;
    // Number of resident large chunks whose cached chunks were merged into
    // CHUNKSCAN candidate collection (cumulative). Lets tests prove a page
    // merges only the large chunks it visits instead of the whole cache.
    [[nodiscard]] std::uint64_t ScanCachedLargeChunksMergedForTests() const noexcept;
    [[nodiscard]] std::uint64_t EvictionRefillLargeChunkScanCountForTests() const noexcept;
    [[nodiscard]] std::size_t EvictionLargeChunkRingSizeForTests() const noexcept;
    [[nodiscard]] std::uint64_t EvictionPostPassLargeChunkCheckCountForTests() const noexcept;
    void ClearEvictionCandidatesForTests();
    // Checkpoints the chunk now, as a due checkpoint would.
    void CheckpointForTests(std::int64_t chunk_x, std::int64_t chunk_y);
    [[nodiscard]] bool IsChunkLoadedForTests(std::int64_t chunk_x, std::int64_t chunk_y) const;
    void ForceUnsyncedOverflowForTests();
    [[nodiscard]] std::size_t UnsyncedTrackedCountForTests() const;
    [[nodiscard]] bool UnsyncedOverflowFlagForTests() const;
    void ArmBarrierAfterDrainPauseForTests();
    [[nodiscard]] bool WaitForBarrierAfterDrainForTests();
    void ResumeBarrierAfterDrainForTests();
    void ArmCheckpointBeforeWalRemovalPauseForTests();
    [[nodiscard]] bool WaitForCheckpointBeforeWalRemovalForTests();
    void ResumeCheckpointBeforeWalRemovalForTests();
    void ArmCheckpointPublishAttemptForTests();
    [[nodiscard]] bool WaitForCheckpointPublishAttemptForTests();
    void ArmConditionalMutationPauseForTests(
        ConditionalMutationPausePoint point);
    [[nodiscard]] bool WaitForConditionalMutationPauseForTests();
    void ResumeConditionalMutationForTests();
    void ArmReadOnlySnapshotPausesForTests(
        std::vector<ReadOnlySnapshotPausePoint> points);
    // Non-zero means the cache handed out two live objects for one chunk;
    // see the duplicate-instance check in GetOrLoadRegularChunk.
    [[nodiscard]] std::uint64_t DuplicateChunkInstancesForTests() const noexcept {
        return stats_duplicate_chunk_instances_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool WaitForReadOnlySnapshotPauseForTests();
    void ResumeReadOnlySnapshotForTests();

    // Transactions: what the history holds, and pause points. The first
    // thread to reach an armed point waits there until it is resumed.
    [[nodiscard]] std::size_t TxnKeptStateCountForTests() const;
    [[nodiscard]] std::size_t TxnKeptBytesForTests() const;
    [[nodiscard]] std::size_t TxnRegisteredCountForTests() const;
    void ArmTxnPauseForTests(TxnPausePoint point);
    [[nodiscard]] bool WaitForTxnPauseForTests(TxnPausePoint point);
    void ResumeTxnForTests(TxnPausePoint point);

    // Snapshot-generation linger controls (see docs/DURABILITY_CONTRACT.md).
    // The odd->even bracket around snapshot-artifact transitions is not closed
    // immediately: the even publication is deferred so that back-to-back
    // transitions (notably a cache-eviction pass) coalesce into one bracket.
    // A window of zero disables lingering and restores immediate publication.
    void SetSnapshotGenerationLingerForTests(
        std::uint64_t window_ms,
        std::size_t max_brackets);
    [[nodiscard]] bool SnapshotGenerationLingerPendingForTests() const;
    [[nodiscard]] std::uint64_t SnapshotGenerationForTests() const;
    [[nodiscard]] std::uint64_t SnapshotGenerationOddPublicationsForTests()
        const noexcept;
    [[nodiscard]] std::uint64_t SnapshotGenerationEvenPublicationsForTests()
        const noexcept;
    // Publishes a deferred even record now, if one is pending. Never throws
    // for an absent linger; a failing publication propagates.
    void FlushSnapshotGenerationLingerForTests();

    // WAL writers capture identity only while durable feed slots are active.
    [[nodiscard]] std::optional<std::string_view> SlotWriteUser() const noexcept;

  private:
    friend class StoreResources;
    friend class TableCatalog;
    friend class Table;
    friend class ChangeFeed;
    friend class FeedWriteGuard;
    friend class FeedSlots;
    friend struct FeedSlotTestAccess;
    friend struct BackupTestAccess;

    // What a WAL barrier still has to sync (see unsynced_files_).
    struct UnsyncedArtifacts {
        std::unordered_set<std::string> files;
        std::unordered_set<std::string> dirs;
        bool overflow = false;
    };
    // TABLESET and a failed TABLEDROP replace a table's store with a new one
    // and must carry over what the old one owed: its group-commit batches,
    // flushed by a call that reports failure (the destructor's flush only
    // logs), and the artifacts a later WALFLUSH must still sync, which the
    // destructor hands to `sink` after its own last flush and the new store
    // adopts.
    void FlushWalBatchesForReopen();
    // Read-only stores: fails when the directory now holds another store (a
    // table dropped and created again under the same name).
    void RequireStoreStillOnDisk() const;
    void HandOverUnsyncedOnClose(std::shared_ptr<UnsyncedArtifacts> sink);
    // Called by the destructor: a WALFLUSH-like sync, unless the store is
    // handed over, read-only or fail-closed. Failures are logged.
    void SyncUnsyncedOnClose() noexcept;
    void AdoptUnsynced(const UnsyncedArtifacts& artifacts);

    class SnapshotGenerationWriteGuard {
      public:
        explicit SnapshotGenerationWriteGuard(ChunkStore* store);
        ~SnapshotGenerationWriteGuard();

        SnapshotGenerationWriteGuard(
            const SnapshotGenerationWriteGuard&) = delete;
        SnapshotGenerationWriteGuard& operator=(
            const SnapshotGenerationWriteGuard&) = delete;

        void Finish();

      private:
        ChunkStore* store_ = nullptr;
        bool nested_on_same_thread_ = false;
        bool finished_ = false;
    };

    struct RegularChunk {
        explicit RegularChunk(
            std::vector<std::uint8_t> payload_bytes,
            std::vector<std::uint8_t> presence_bytes)
            : payload(std::move(payload_bytes)),
              presence_bitmap(std::move(presence_bytes)) {}

        std::vector<std::uint8_t> payload;
        std::vector<std::uint8_t> presence_bitmap;
        // Values of text and bytes columns (ChunkLayout::RequireValidVars).
        ChunkVars vars;
        std::size_t pending_updates = 0;
        std::size_t wal_bytes = 0;
        bool checkpoint_due_armed = false;
        bool deferred_wal_compaction = false;

        std::size_t pending_wal_flush_updates = 0;
        std::uint64_t version = 0;
        // Commit time (Unix ms) of the mutation that produced `version`; zero
        // when unknown.
        std::uint64_t commit_time_ms = 0;
        bool background_checkpoint_failed = false;
        // A failed write's WAL bytes could not be removed (the store is
        // poisoned). Memory holds the chunk's committed state and the WAL
        // past its last good point does not, so the chunk stays cached
        // until a restart.
        bool wal_repair_failed = false;
        // This process made the chunk's image, its WAL (file and directory
        // entry), their directory's entry and the absence of whatever was
        // removed durable, and nothing changed them without a sync since
        // (MakeWalBoundaryDurableLocked). Cleared by an unsynced append, a
        // new WAL, a truncation and a checkpoint.
        bool wal_boundary_durable = false;
        std::vector<std::uint8_t> wal_batch;
        std::vector<std::uint8_t> scratch_before;
        std::filesystem::path wal_path;
        // Held by pointer, not by value: an inline std::ofstream costs 568 B
        // inside every resident chunk plus the ~4 KiB stream buffer libc++
        // allocates in the basic_filebuf constructor - before the stream is
        // ever opened. Sparse workloads keep the overwhelming majority of
        // resident chunks with no open WAL stream (the pool caps open streams
        // at max_open_wal_streams anyway), so both costs are paid lazily:
        // EnsureWalAppendStream creates it, CloseWalAppendStream destroys it.
        // Created, dereferenced and destroyed only under `mutex` (the open
        // path additionally holds the stream pool's open mutex), exactly like the inline
        // member it replaces.
        std::unique_ptr<std::ofstream> wal_append_stream;
        bool wal_header_written = false;
        // Mirrors WalAppendStreamOpen(*this). Written only under `mutex`;
        // atomic because the shared WAL stream pool reads it for other chunks
        // (of any store) under its own mutex alone, where touching the
        // ofstream - or the pointer to it - is a race.
        std::atomic<bool> wal_stream_initialized{false};

        std::atomic<std::uint64_t> last_access_tick{0};
        mutable RegularChunkMutex mutex;
    };

    struct EvictionCandidate {
        LargeChunkCoord large_coord;
        ChunkCoord chunk_coord;
        std::uint64_t recorded_tick = 0;
    };

    struct LargeChunk {
        std::mutex mutex;
        std::unordered_map<ChunkCoord, std::shared_ptr<RegularChunk>, ChunkCoordHash> chunks;
        // Set under `mutex` when this container is dropped from
        // `large_chunks_` because it went empty. A thread that fetched this
        // object before the removal and then blocked on `mutex` must not
        // insert into it: the container is unreachable, so a later lookup
        // would build a second live instance of the same chunk and the
        // survivor's checkpoint would discard whatever was written through
        // the orphan. Such a thread rechecks this flag and retries.
        bool retired = false;
    };

    Geometry geometry_;
    std::filesystem::path data_dir_;
    DurabilityMode durability_mode_;
    AccessMode access_mode_;
    bool allow_multiple_processes_;
    std::size_t checkpoint_update_interval_;
    std::size_t checkpoint_wal_bytes_;
    std::size_t wal_group_commit_updates_;
    CheckpointCompression checkpoint_compression_ = CheckpointCompression::kNone;
    std::size_t var_max_chunk_bytes_ = kDefaultVarMaxChunkBytes;
    std::shared_ptr<StoreResources> resources_;
    bool acquire_process_lock_ = true;
    std::uint64_t initial_version_floor_ = 0;

    // Held while the store is open, unless the directory's owner holds it.
    std::unique_ptr<ProcessLock> process_lock_;

    std::filesystem::path snapshot_generation_path_;
    StoreId store_id_{};
    FeatureFlags features_{};
    std::uint64_t snapshot_generation_ = 0;
    // The generation record existed when the store opened (a writer never
    // removes it); a later read that finds none fails closed.
    bool snapshot_generation_record_seen_ = false;
    std::size_t snapshot_generation_active_writers_ = 0;
    bool snapshot_generation_epoch_failed_ = false;
    mutable std::mutex snapshot_generation_mutex_;
    // Deferred even publication ("linger"). All of these are guarded by
    // snapshot_generation_mutex_.
    std::condition_variable snapshot_generation_linger_cv_;
    std::thread snapshot_generation_linger_thread_;
    bool snapshot_generation_linger_stop_ = false;
    // True while the epoch is odd, no writer is active, and the even record
    // has deliberately not been written yet.
    bool snapshot_generation_linger_pending_ = false;
    // Set once the open epoch has outlived its window; the next Finish (or
    // Begin) publishes even instead of lingering again.
    bool snapshot_generation_epoch_expired_ = false;
    std::size_t snapshot_generation_epoch_brackets_ = 0;
    std::chrono::steady_clock::time_point snapshot_generation_epoch_deadline_{};
    std::chrono::milliseconds snapshot_generation_linger_window_{
        kDefaultSnapshotGenerationLingerMs};
    std::size_t snapshot_generation_linger_max_brackets_ =
        kDefaultSnapshotGenerationLingerMaxBrackets;
    std::atomic<std::uint64_t> stats_snapshot_generation_odd_publications_{0};
    std::atomic<std::uint64_t> stats_snapshot_generation_even_publications_{0};

    // Chunks of this store in the cache; resources_ counts all stores.
    std::atomic<std::uint64_t> loaded_chunk_count_{0};
    // Until this steady-clock time (ms), eviction passes of other stores skip
    // this one: flushing one of its chunks failed (a fail-closed store would
    // otherwise be retried, and logged, on every load in every store).
    std::atomic<std::int64_t> eviction_skip_until_ms_{0};
    std::atomic<std::uint64_t> stats_evictions_{0};
    std::atomic<std::uint64_t> stats_checkpoints_{0};
    std::atomic<std::uint64_t> stats_wal_batch_flushes_{0};
    std::atomic<std::uint64_t> stats_unique_loaded_chunks_{0};
    std::atomic<std::uint64_t> stats_wal_open_count_{0};
    std::atomic<std::uint64_t> stats_eviction_snapshot_builds_{0};
    std::atomic<std::uint64_t> stats_eviction_probes_{0};
    std::atomic<std::uint64_t> stats_eviction_no_progress_cycles_{0};
    std::atomic<std::uint64_t> stats_eviction_post_pass_large_chunk_checks_{0};
    std::atomic<std::uint64_t> stats_eviction_forced_wal_flushes_{0};
    std::atomic<std::uint64_t> stats_eviction_forced_wal_flushes_with_data_{0};
    std::atomic<std::uint64_t> stats_eviction_forced_wal_flushes_empty_batch_{0};
    std::atomic<std::uint64_t> stats_eviction_refill_large_chunk_scans_{0};
    mutable std::atomic<std::uint64_t> stats_scan_large_dirs_listed_{0};
    mutable std::atomic<std::uint64_t> stats_scan_catalog_builds_{0};
    mutable std::atomic<std::uint64_t> stats_scan_cached_large_chunks_merged_{0};
    std::atomic<std::uint64_t> stats_wal_parent_prepare_calls_{0};
    std::atomic<std::uint64_t> stats_eviction_recency_skips_{0};
    std::atomic<std::uint64_t> stats_empty_chunk_gcs_{0};
    std::atomic<std::uint64_t> stats_wal_barriers_{0};
    std::atomic<std::uint64_t> stats_wal_barrier_full_syncs_{0};
    std::atomic<std::uint64_t> stats_background_checkpoints_{0};
    std::atomic<std::uint64_t> stats_background_checkpoint_failures_{0};
    std::atomic<std::uint64_t> stats_background_eviction_failures_{0};
    std::atomic<std::uint64_t> stats_background_queue_full_inline_{0};
    std::atomic<std::uint64_t> stats_compressed_checkpoint_images_{0};

    // Mutation scopes remain registered through postcommit durability work.
    // Registration and backup cut sampling share this mutex; chunk locks are
    // never acquired while it is held.
    struct WriteCompletion {
        std::uint64_t bound = 0;
        WriteCompletion* previous = nullptr;
        WriteCompletion* next = nullptr;
    };
    std::mutex write_completion_mutex_;
    std::condition_variable write_completion_cv_;
    WriteCompletion* write_completions_ = nullptr;
    void RegisterWriteCompletion(WriteCompletion& completion);
    void UnregisterWriteCompletion(WriteCompletion& completion) noexcept;

    // Checkpoints take the shared side without waiting under chunk locks.
    // Backup holds the exclusive side only while linking its file set.
    std::shared_timed_mutex backup_maintenance_mutex_;

    // Store-wide monotonic chunk version clock. Versions are issued strictly
    // below version_clock_ceiling_, and the ceiling is persisted (fsynced)
    // before any version in its range is issued, so versions never repeat
    // across eviction or restart of a read-write store. Read-only stores
    // leave the clock unused and issue random epoch tokens instead.
    std::atomic<std::uint64_t> version_clock_{0};
    std::atomic<std::uint64_t> version_clock_ceiling_{0};
    // Latest commit time this store instance issued (Unix ms).
    std::atomic<std::uint64_t> last_commit_time_ms_{0};
    std::mutex version_clock_mutex_;
    std::filesystem::path version_clock_path_;

    // Set when a durability/rollback step could not be completed. Once set the
    // store refuses further mutations and barriers (fail-closed); the reason is
    // preserved for the error surfaced to callers.
    std::atomic<bool> durability_poisoned_{false};
    mutable std::mutex poison_mutex_;
    std::string poison_reason_;

    // Serializes a checkpoint's image-replace + WAL-removal "publish" step
    // against WalBarrier's drain+sync, so a barrier can never observe a tracked
    // WAL disappear without also covering the replacement image that now
    // carries the promised state.
    mutable std::mutex checkpoint_publish_mutex_;

    // Files/directories written without an fsync in relaxed paths. The WAL
    // barrier drains this set. When the set would exceed its bound, the
    // overflow flag forces the next barrier to sync the whole data directory.
    mutable std::mutex unsynced_mutex_;
    std::unordered_set<std::string> unsynced_files_;
    std::unordered_set<std::string> unsynced_dirs_;
    bool unsynced_overflow_ = false;
    std::shared_ptr<UnsyncedArtifacts> unsynced_handover_;
    // Serializes concurrent WalBarrier callers so each caller's guarantee
    // covers everything acknowledged before its own call began.
    mutable std::mutex wal_barrier_mutex_;
    mutable std::mutex durability_hook_mutex_;
    std::condition_variable durability_hook_cv_;
    bool barrier_after_drain_armed_ = false;
    bool barrier_after_drain_reached_ = false;
    bool barrier_after_drain_resumed_ = false;
    bool checkpoint_before_wal_remove_armed_ = false;
    bool checkpoint_before_wal_remove_reached_ = false;
    bool checkpoint_before_wal_remove_resumed_ = false;
    bool checkpoint_publish_attempt_armed_ = false;
    bool checkpoint_publish_attempt_reached_ = false;
    ConditionalMutationPausePoint conditional_pause_point_ =
        ConditionalMutationPausePoint::kNone;
    bool conditional_pause_reached_ = false;
    bool conditional_pause_resumed_ = false;
    mutable std::mutex read_only_snapshot_pause_mutex_;
    std::condition_variable read_only_snapshot_pause_cv_;
    std::vector<ReadOnlySnapshotPausePoint> read_only_snapshot_pause_points_;
    std::size_t read_only_snapshot_pause_index_ = 0;
    bool read_only_snapshot_pause_reached_ = false;
    bool read_only_snapshot_pause_resumed_ = false;
    // Once a successful barrier has established a durability floor, later
    // relaxed-mode checkpoint/GC replacement must not downgrade durable state
    // by deleting a synced WAL in favor of an unsynced image.
    std::atomic<bool> barrier_durability_floor_{false};

    // Snapshots of open transactions and the chunk states they need
    // (txn_history.hpp). Shared with the snapshots, which may outlive the
    // store.
    std::shared_ptr<TxnHistory> txn_history_;

    // Background maintenance (checkpoints + eviction) state.
    bool background_maintenance_ = false;
    std::size_t background_checkpoint_queue_limit_ = 4096;
    mutable std::mutex maintenance_mutex_;
    std::condition_variable maintenance_cv_;
    std::vector<ChunkCoord> maintenance_checkpoint_queue_;
    std::unordered_set<std::string> maintenance_checkpoint_queued_keys_;
    bool maintenance_eviction_requested_ = false;
    bool maintenance_stop_ = false;
    std::thread maintenance_thread_;

    mutable std::mutex wal_parent_cache_mutex_;
    std::unordered_set<std::string> wal_parent_dir_cache_;

    mutable std::mutex large_chunks_mutex_;
    std::unordered_map<LargeChunkCoord, std::shared_ptr<LargeChunk>, LargeChunkCoordHash> large_chunks_;
    // Lazy union of disk directories and resident large chunks. Protected by
    // large_chunks_mutex_; no full registry copy or directory listing per page.
    mutable std::map<std::pair<std::int64_t, std::int64_t>, std::filesystem::path> scan_catalog_;
    mutable bool scan_catalog_ready_ = false;
    mutable std::uint64_t scan_catalog_generation_ = 0;
    std::vector<LargeChunkCoord> eviction_large_chunk_ring_;
    // The last RegularChunk handed out per coordinate. If a fresh load finds
    // the previous instance still alive, two objects for one chunk exist at
    // once and writes made through the older one can be discarded by whoever
    // checkpoints from the newer.
    mutable std::mutex live_chunk_instances_mutex_;
    std::unordered_map<ChunkCoord, std::weak_ptr<RegularChunk>, ChunkCoordHash>
        live_chunk_instances_;
    std::atomic<std::uint64_t> stats_duplicate_chunk_instances_{0};
    std::size_t eviction_large_chunk_cursor_ = 0;
    mutable std::mutex eviction_state_mutex_;
    std::vector<EvictionCandidate> eviction_candidates_;
    std::size_t eviction_cursor_ = 0;

    struct LoadedChunkPayload {
        std::vector<std::uint8_t> payload;
        std::vector<std::uint8_t> presence_bitmap;
        // The schema version the state is laid out by; 0 for the empty state
        // of a chunk without an image (docs/COLUMNS_DESIGN.md).
        std::uint64_t schema_version = 0;
        // Persisted chunk revision from the image and the last WAL frame;
        // zero when the chunk has no artifact, in which case the loader
        // reserves a fresh token.
        std::uint64_t revision = 0;
        std::uint64_t commit_time_ms = 0;
        std::size_t wal_bytes = 0;
        bool deferred_wal_compaction = false;
        bool wal_header_written = false;
        std::filesystem::path wal_path;
        ChunkVars vars{};
    };

    [[nodiscard]] std::shared_ptr<LargeChunk> GetOrCreateLargeChunk(const LargeChunkCoord& large_coord);
    [[nodiscard]] std::shared_ptr<RegularChunk> GetOrLoadRegularChunk(const ChunkCoord& chunk_coord);

    [[nodiscard]] std::vector<std::uint8_t> EmptyPayload() const;
    [[nodiscard]] std::vector<std::uint8_t> EmptyPresenceBitmap() const;
    // Shared tail of every full-chunk replace: takes canonical-size packed
    // buffers, canonicalizes absent blocks, and applies them under the chunk
    // lock with the ordinary WAL/rollback discipline, dropping the values of
    // blocks that become absent. Returns the chunk version after the write.
    std::uint64_t ApplyChunkState(
        const ChunkCoord& chunk_coord,
        std::vector<std::uint8_t> payload,
        std::vector<std::uint8_t> presence_bitmap);
    // ApplyChunkState with the chunk's mutex held and the value changes
    // given; returns the chunk version after it.
    std::uint64_t ApplyChunkStateLocked(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& regular_chunk,
        std::vector<std::uint8_t> payload,
        std::vector<std::uint8_t> presence_bitmap,
        VarUpdate var_update);
    // The value update of a full-chunk write leaving `presence`: removing the
    // values of blocks that become absent. Empty when nothing changes.
    [[nodiscard]] static VarUpdate VarUpdateForState(
        const ChunkVars& current,
        const std::vector<std::uint8_t>& presence);
    // Throws std::invalid_argument when `update` would make the chunk's values
    // take more than var_max_chunk_bytes; a chunk already over a lowered
    // limit may still shrink.
    void RequireVarWrite(const ChunkVars& current, const VarUpdate& update) const;
    [[nodiscard]] LoadedChunkPayload LoadChunkPayload(const ChunkCoord& chunk_coord, bool authoritative_prefix = false);
    // Before a loaded chunk can append, drops what replay could not use: the
    // bytes after the last valid frame, or (`keep_bytes` zero) a WAL left by
    // an interrupted creation. Appending after them would put new frames
    // where replay never reaches. Runs as a snapshot-generation transition.
    void TrimWalForAppend(const ChunkCoord& chunk_coord, const std::filesystem::path& wal_path, std::size_t keep_bytes);

    void TouchChunk(const std::shared_ptr<RegularChunk>& chunk) noexcept;
    void RegisterEvictionCandidate(
        const LargeChunkCoord& large_coord,
        const ChunkCoord& chunk_coord,
        std::uint64_t recorded_tick);
    void RemoveLargeChunkFromEvictionRing(const LargeChunkCoord& large_coord);
    [[nodiscard]] bool RefillEvictionCandidatesBounded();
    [[nodiscard]] bool TryEvictCandidate(
        const EvictionCandidate& candidate,
        bool respect_recency,
        std::size_t* removed,
        std::vector<LargeChunkCoord>* maybe_empty_large_chunks);
    // Recorded tick of the next eviction candidate of this store, refilling
    // the candidate list when it is used up; std::nullopt when this store has
    // no evictable chunk.
    [[nodiscard]] std::optional<std::uint64_t> PeekEvictionCandidateTick();
    [[nodiscard]] bool PopEvictionCandidate(EvictionCandidate* candidate);
    // Bookkeeping after an eviction pass removed `removed` chunks of this
    // store: counters and the large chunks the pass may have emptied.
    void FinishEvictedChunks(
        std::size_t removed,
        std::vector<LargeChunkCoord> maybe_empty_large_chunks);
    // Brings the cache shared through resources_ back under its budget by
    // evicting the coldest chunks of any store sharing it.
    void MaybeEvictChunks();
    void RequestEviction();

    [[nodiscard]] std::shared_ptr<RegularChunk> TryGetLoadedChunk(const ChunkCoord& chunk_coord) const;
    // Whether the chunk has a present block; fills `out` (payload,
    // presence, version, and the values with `with_vars`) unless it is null.
    // With `snapshot`, as of it.
    [[nodiscard]] bool ReadPopulatedChunkStateNoCache(
        const ChunkCoord& chunk_coord,
        ChunkRangeEntry* out,
        bool with_vars,
        const TxnSnapshot* snapshot = nullptr);
    // `vars_problem` receives WalReplayResult::vars_problem (empty when the
    // values are consistent).
    [[nodiscard]] bool ReadPopulatedChunkStateFromDisk(
        const ChunkCoord& chunk_coord,
        ChunkRangeEntry* out,
        bool with_vars,
        std::string* vars_problem);
    // Feeds `candidates` from both sources — on-disk artifacts and the
    // resident cache — visiting large chunks in scan order so the cursor and
    // the page window prune both.
    void CollectScanCandidates(ScanCandidateAccumulator* candidates) const;
    void EnsureScanCatalog() const;
    void MergeCachedCandidates(
        const std::shared_ptr<LargeChunk>& large_chunk,
        ScanCandidateAccumulator* candidates) const;
    [[nodiscard]] std::size_t ChunkRangeEntryCostBytes() const noexcept;
    // Adds the chunk when populated. `vars_bytes` counts the values read so
    // far with `with_vars`; they and max_entries entries share the
    // response-byte limit.
    void AppendPopulatedChunkRangeEntry(
        const ChunkCoord& coord,
        std::size_t max_entries,
        const char* operation_name,
        bool with_vars,
        std::size_t* vars_bytes,
        std::vector<ChunkRangeEntry>* entries,
        const TxnSnapshot* snapshot,
        const TxnAreaRead* txn_read);

    // Issues the next version token; requires a read-write store.
    [[nodiscard]] std::uint64_t NextChunkVersion();
    // Commit time for a mutation of `chunk`: the wall clock in Unix ms, never
    // below a time this store instance already issued or the chunk's own
    // last commit time. Requires the chunk's exclusive lock.
    [[nodiscard]] std::uint64_t NextCommitTimeMs(const RegularChunk& chunk);
    // Loads, initializes, or migrates the persisted version clock.
    // `store_preexisting` says chunk data or bookkeeping already exists, so a
    // missing clock can be reported; only the checked initialized marker
    // proves that this store previously exposed deterministic version tokens.
    void InitializeVersionClock(bool store_preexisting);
    void ExtendVersionClockCeilingLocked(std::uint64_t minimum_exclusive);
    // Moves the clock past a persisted chunk revision seen at load time, so
    // tokens issued from now on are above every revision the store holds even
    // when the clock bookkeeping was lost and restarted low.
    void RaiseVersionClockAbove(std::uint64_t revision);
    void RecoverConditionalRollbackIntents();
    // Creates the manifest of a new store, or confirms the existing one
    // still records the geometry this store opened with.
    void InitializeStoreManifest();
    void InitializeSnapshotGeneration(bool store_preexisting);
    void FinishSnapshotGenerationRecovery();
    void BeginSnapshotGenerationWriteLocked();
    void FinishSnapshotGenerationWriteLocked();
    void AbandonSnapshotGenerationWriteLocked(
        bool fail_epoch) noexcept;
    // Writes the even (stable) record that closes the current odd epoch.
    // Callers must hold snapshot_generation_mutex_ and must have already
    // established that no writer is active and the epoch did not fail.
    [[nodiscard]] bool SnapshotGenerationLingerEnabledLocked() const noexcept;
    void PublishStableSnapshotGenerationLocked();
    // Closes a deferred (lingering) even publication if one is pending.
    // Throws whatever the publication throws.
    void FlushSnapshotGenerationLinger();
    // Same, but swallows and logs failures; used on shutdown paths.
    void FlushSnapshotGenerationLingerQuietly() noexcept;
    void StartSnapshotGenerationLingerThreadLocked();
    void SnapshotGenerationLingerLoop();
    void ShutdownSnapshotGenerationLinger() noexcept;

    // Fail-closed durability guard. When a rollback or durability step cannot
    // be completed, the store is poisoned so it stops accepting mutations and
    // barriers rather than continuing to serve while on-disk state may be
    // inconsistent with acknowledged results.
    void PoisonDurability(std::string reason) noexcept;
    void ThrowIfDurabilityPoisoned() const;

    [[nodiscard]] bool ApplyFullChunkStateLocked(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        std::vector<std::uint8_t> new_payload,
        std::vector<std::uint8_t> new_presence,
        VarUpdate var_update);

    void NoteUnsyncedFile(const std::filesystem::path& path);
    void NoteUnsyncedDir(const std::filesystem::path& path);

    void StartMaintenanceThread();
    void StopMaintenanceThread() noexcept;
    void MaintenanceLoop();
    [[nodiscard]] bool EnqueueBackgroundCheckpoint(const ChunkCoord& chunk_coord);
    void RunBackgroundCheckpoint(const ChunkCoord& chunk_coord) noexcept;

    void FlushWalBatch(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool force_sync);
    // Before a conditional write or a transaction names the WAL's size as a
    // rollback boundary: writes the chunk's batch with a sync and, unless
    // this process already made them durable (wal_boundary_durable), syncs
    // the chunk's image, its WAL, their directory and the table directory,
    // and drops them from the unsynced set. A recovery truncating to the
    // boundary then finds the WAL at least that long, over the image it was
    // written over, after a power loss too. `own_stream` writes the batch
    // without taking a stream from the shared pool. Requires the chunk's
    // exclusive lock.
    void MakeWalBoundaryDurableLocked(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool own_stream);
    // Removes `files` and `dirs` from the unsynced set; returns those that
    // were in it.
    [[nodiscard]] UnsyncedArtifacts ForgetUnsynced(
        const std::vector<std::filesystem::path>& files,
        const std::vector<std::filesystem::path>& dirs);
    [[nodiscard]] std::uint64_t CurrentWalFileSize(
        const std::shared_ptr<RegularChunk>& chunk) const;
    // Truncates the chunk's WAL file back to `committed_size` bytes (removing
    // records appended by a failed mutation) and re-syncs it in synced modes.
    // A committed_size of zero removes the WAL file entirely.
    void TruncateWalTail(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        std::uint64_t committed_size,
        bool force_sync);
    [[nodiscard]] std::filesystem::path WriteConditionalRollbackIntent(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        std::uint64_t committed_size);
    [[nodiscard]] bool PublishConditionalCommitIntent(
        const std::filesystem::path& intent_path,
        std::uint64_t committed_size);
    void ClearCommittedConditionalIntent(
        const std::filesystem::path& intent_path);
    void PauseCheckpointBeforeWalRemovalForTests();
    void NoteCheckpointPublishAttemptForTests();
    void PauseConditionalMutationForTests(
        ConditionalMutationPausePoint point);
    void PauseReadOnlySnapshotForTests(
        std::size_t collection,
        ReadOnlySnapshotArtifact artifact);
    void FlushWalBatchForEviction(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool force_sync);
    void EnsureWalAppendStream(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool* first_create,
        bool* artifact_touched);
    void EnsureWalParentDirectoryCached(
        const std::filesystem::path& wal_parent_path,
        bool force_refresh,
        bool durable_sync);
    void InvalidateWalParentDirectoryCache(const std::filesystem::path& wal_parent_path);
    // True when the chunk currently owns an open WAL append stream. Reads the
    // lazily allocated stream, so the caller must hold the chunk's `mutex`;
    // code that only holds the stream pool's mutex must use the
    // wal_stream_initialized flag instead.
    [[nodiscard]] static bool WalAppendStreamOpen(const RegularChunk& chunk) noexcept {
        return chunk.wal_append_stream != nullptr && chunk.wal_append_stream->is_open();
    }
    void CloseWalAppendStream(const std::shared_ptr<RegularChunk>& chunk) noexcept;
    [[nodiscard]] bool TryCloseLeastRecentlyUsedIdleWalStream(
        const std::shared_ptr<RegularChunk>& opening_chunk);
    void EnsureWalStreamCapacity(const std::shared_ptr<RegularChunk>& opening_chunk);
    void TouchWalStreamState(const std::shared_ptr<RegularChunk>& chunk) noexcept;

    void FlushAllPendingWalBatches() noexcept;
    // Closes the WAL append stream of every cached chunk (shutdown).
    void CloseAllWalStreams() noexcept;
    [[nodiscard]] bool IsCheckpointDue(const std::shared_ptr<RegularChunk>& chunk) noexcept;

    // Shared tail of every ordinary (non-conditional) mutation. The caller
    // has already staged this mutation's delta records into chunk->wal_batch
    // (memory only). Bumps the pending counters, performs the mode-required
    // flush, and — once the flush has succeeded (the commit point) — assigns
    // the reserved version and runs the inline checkpoint, whose failure is
    // post-commit and therefore logged and retained for retry rather than
    // returned as a command error. A flush failure throws with the WAL file
    // already neutralized by FlushWalBatch's repair; the caller must then
    // restore its memory state, wal_batch, and counters.
    // The columns one block write sets (block_ops.cpp).
    struct BlockWrite;
    // This thread's BlockWrite, emptied.
    static BlockWrite& ThreadBlockWrite();
    // SET BLOCK's values of block `block_index`, checked and encoded, in this
    // thread's BlockWrite.
    BlockWrite& PrepareSetBlockWrite(const std::vector<ColumnAssignment>& values, std::size_t block_index) const;
    // Adds the columns a new block takes when not given (DEFAULT, NULL or
    // zero); throws for a REQUIRED one.
    void AddNewBlockColumns(BlockWrite& write, std::size_t block_index) const;
    // Clears every column of block `block_index`, in this thread's
    // BlockWrite.
    BlockWrite& PrepareUnsetBlockWrite(std::size_t block_index) const;
    // Applies `write` to a state outside the store.
    void ApplyBlockWriteToState(ChunkState& state, std::size_t block_index, BlockWrite& write, bool present) const;
    // Writes `write` and the block's presence as one WAL frame, or changes
    // nothing when it throws. Called with the chunk's mutex held.
    void WriteBlockColumnsLocked(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        std::size_t block_index,
        BlockWrite& write,
        bool present);
    // Writes one block of a bit-string table (`present`) or removes it
    // (`bits` all zero), as one WAL frame. Called with the chunk's mutex
    // held.
    void WriteBlockBitsLocked(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& regular_chunk,
        std::size_t block_index,
        std::string_view bits,
        bool present);
    // The bit-string commands need a table with one bits(N) column.
    void RequireBitStringBlocks() const;
    // While a narrowing is in progress, throws std::invalid_argument when a
    // whole-chunk write's state holds a value of that column the narrower
    // type does not; `vars` are the write's text and bytes values, when it
    // carries them.
    void RequirePendingFits(
        const std::vector<std::uint8_t>& payload,
        const std::vector<std::uint8_t>& presence,
        const ChunkVars* vars = nullptr) const;
    // `keep`, when it holds a state, is published to the transaction
    // history once the commit point passed.
    void FinishOrdinaryMutationLocked(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        std::size_t appended_bytes,
        std::uint64_t reserved_version,
        std::uint64_t commit_time_ms,
        TxnKeep* keep);
    // Called by a write under its chunk's exclusive lock after it took
    // `version` and before it changes the chunk: while a transaction is open,
    // a copy of the chunk's state to keep, tagged `version`; otherwise empty
    // (one atomic load).
    [[nodiscard]] TxnKeep PrepareTxnKeepLocked(
        const ChunkCoord& chunk_coord,
        const RegularChunk& chunk,
        std::uint64_t version);
    // Resolves the transaction intents a crash left (writer open).
    void RecoverTransactionIntents();
    // Throws std::invalid_argument unless `snapshot` is of this store.
    void RequireOwnTxnSnapshot(const TxnSnapshot& snapshot) const;
    // Replaces `entry`'s state with the one `snapshot` sees when a write
    // changed the chunk after it; `entry` holds the chunk's current state,
    // read under its lock or from its files before this call.
    void ApplyTxnHistory(
        const TxnSnapshot& snapshot,
        const ChunkCoord& chunk_coord,
        bool with_vars,
        ChunkRangeEntry* entry);
    [[nodiscard]] std::vector<ChunkRangeEntry> ReadChunkRangeImpl(
        const TxnSnapshot* snapshot,
        const TxnAreaRead* txn_read,
        std::int64_t chunk_x0,
        std::int64_t chunk_y0,
        std::int64_t chunk_x1,
        std::int64_t chunk_y1,
        bool with_vars);
    [[nodiscard]] std::vector<ChunkRangeEntry> ReadChunkRadiusImpl(
        const TxnSnapshot* snapshot,
        const TxnAreaRead* txn_read,
        std::int64_t center_x,
        std::int64_t center_y,
        std::int64_t radius_chunks,
        bool with_vars);
    // Commit helper (txn_commit.cpp), called with the chunk's lock held.
    // Appends `bytes` (a frame, after a WAL header when `new_wal`) to the
    // chunk's WAL through a stream of its own, syncs and closes it.
    void AppendTxnFrameLocked(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        const std::vector<std::uint8_t>& bytes,
        bool new_wal);

    void MaybeCheckpointChunk(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool* out_image_committed = nullptr);
    // Writes/removes the on-disk image for the chunk and drops the WAL. When
    // `out_image_committed` is non-null it is set true once the on-disk image
    // reflects the checkpoint's target state, so a caller whose atomicity
    // depends on the image can distinguish a pre-replace failure (nothing
    // committed, safe to roll back) from a post-replace durability failure.
    bool CheckpointChunk(
        const ChunkCoord& chunk_coord,
        const std::shared_ptr<RegularChunk>& chunk,
        bool* out_image_committed = nullptr);

    void AcquireProcessLock(bool allow_multiple_processes);
    void ReleaseProcessLock() noexcept;

    // Keep the existing hot fields at their offsets and the read-mostly
    // attachment away from counters changed by ordinary writes.
    std::atomic<class ChangeFeed*> feed_{nullptr};
    std::atomic<bool> feed_slots_active_{false};
    std::atomic<bool> feed_watchers_active_{false};
    std::shared_ptr<class FeedSlots> feed_slots_;
};

// Budgets that the stores of one process share: the number of cached chunks
// and the number of open WAL append streams (file descriptors belong to the
// process). Eviction and stream reuse pick victims across every store that
// uses the same StoreResources, ordered by one access clock.
class StoreResources {
  public:
    // max_open_wal_streams is lowered to fit the process file limit.
    StoreResources(std::size_t max_loaded_chunks, std::size_t max_open_wal_streams);

    StoreResources(const StoreResources&) = delete;
    StoreResources& operator=(const StoreResources&) = delete;

    [[nodiscard]] std::size_t max_loaded_chunks() const noexcept { return max_loaded_chunks_; }
    [[nodiscard]] std::size_t max_open_wal_streams() const noexcept {
        return max_open_wal_streams_;
    }
    // Chunks cached by all stores.
    [[nodiscard]] std::uint64_t LoadedChunkCount() const noexcept {
        return loaded_chunks_.load(std::memory_order_relaxed);
    }
    // WAL append streams open in all stores.
    [[nodiscard]] std::size_t OpenWalStreamCount() const;

  private:
    friend class ChunkStore;
    friend class Table;

    struct WalStreamState {
        std::weak_ptr<ChunkStore::RegularChunk> chunk;
        const ChunkStore* owner = nullptr;
        // The entry's place in wal_stream_lru_.
        std::list<ChunkStore::RegularChunk*>::iterator lru_position;
    };

    // Under wal_stream_mutex_: forgets an entry, in the map and the order.
    void EraseWalStreamLocked(
        std::unordered_map<ChunkStore::RegularChunk*, WalStreamState>::iterator entry) noexcept;

    void RegisterStore(ChunkStore* store);
    // Removes `store` from eviction, waiting for running passes, and drops
    // its chunks from the shared count.
    void UnregisterStore(ChunkStore* store) noexcept;
    // Drops whatever stream entries of `store` are left; its chunks close
    // their streams before it goes away.
    void ForgetWalStreams(const ChunkStore* store) noexcept;
    [[nodiscard]] std::uint64_t NextAccessTick() noexcept {
        return access_clock_.fetch_add(1, std::memory_order_relaxed) + 1U;
    }

    std::size_t max_loaded_chunks_;
    std::size_t max_open_wal_streams_;
    std::atomic<std::uint64_t> loaded_chunks_{0};
    std::atomic<std::uint64_t> access_clock_{0};
    // Held shared by an eviction pass, exclusively to add or remove a store,
    // so a store never leaves while a pass works on its chunks.
    mutable std::shared_mutex stores_mutex_;
    std::vector<ChunkStore*> stores_;

    // Serializes opening a stream (capacity check through registration) in
    // every store, so the shared cap holds; taken before wal_stream_mutex_.
    std::mutex wal_open_mutex_;
    mutable std::mutex wal_stream_mutex_;
    std::condition_variable wal_stream_cv_;
    std::unordered_map<ChunkStore::RegularChunk*, WalStreamState> open_wal_streams_;
    // The keys of open_wal_streams_, least recently used first. Touching an
    // entry moves it to the back and nothing walks the whole list on the
    // write path: an entry whose chunk is gone is dropped when it reaches
    // the front.
    std::list<ChunkStore::RegularChunk*> wal_stream_lru_;
};

}  // namespace chunkdb
