#include "wal_writer.hpp"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "checkpoint.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "wal_stream_pool.hpp"

namespace chunkdb {

[[nodiscard]] const char* ErrnoName(int err) {
    switch (err) {
#ifdef EACCES
        case EACCES:
            return "EACCES";
#endif
#ifdef EMFILE
        case EMFILE:
            return "EMFILE";
#endif
#ifdef ENFILE
        case ENFILE:
            return "ENFILE";
#endif
#ifdef ENOSPC
        case ENOSPC:
            return "ENOSPC";
#endif
#ifdef EROFS
        case EROFS:
            return "EROFS";
#endif
        default:
            return "UNKNOWN";
    }
}

[[nodiscard]] std::runtime_error BuildWalOpenError(
    const std::filesystem::path& path,
    int err) {
    return std::runtime_error(
        "failed to open WAL file for append: " + path.string() +
        " (errno=" + std::to_string(err) +
        ", code=" + ErrnoName(err) +
        ", msg='" + std::strerror(err) + "')");
}
WalFrameBuilder::WalFrameBuilder(
    std::vector<std::uint8_t>* batch,
    std::uint64_t schema_version,
    const std::vector<std::uint8_t>& tag)
    : batch_(batch), header_index_(batch == nullptr ? 0 : batch->size()), records_begin_(0) {
    if (batch_ == nullptr) {
        throw std::invalid_argument("WAL batch must not be null");
    }
    // The TLV area (a tag and the schema version) has a u16 size.
    if (tag.size() > std::numeric_limits<std::uint16_t>::max() - 2U * kWalTlvHeaderSize - 8U) {
        throw std::invalid_argument("WAL frame tag is too long");
    }
    // Reserve the fixed header; Finish() fills it once the body is known.
    batch_->resize(batch_->size() + kWalFrameFixedHeaderSize, 0U);
    if (!tag.empty()) {
        WriteLe16(*batch_, kWalTlvTag);
        WriteLe16(*batch_, static_cast<std::uint16_t>(tag.size()));
        batch_->insert(batch_->end(), tag.begin(), tag.end());
        tlv_size_ = static_cast<std::uint16_t>(kWalTlvHeaderSize + tag.size());
    }
    if (schema_version > 1U) {
        WriteLe16(*batch_, kWalTlvSchema);
        WriteLe16(*batch_, 8U);
        WriteLe64(*batch_, schema_version);
        tlv_size_ = static_cast<std::uint16_t>(tlv_size_ + kWalTlvHeaderSize + 8U);
    }
    // The header CRC slot follows the TLV area.
    batch_->resize(batch_->size() + kWalFrameHeaderCrcSize, 0U);
    records_begin_ = batch_->size();
}

void WalFrameBuilder::AppendSpan(
    std::uint32_t byte_offset,
    const std::uint8_t* bytes,
    std::size_t size) {
    if (finished_) {
        throw std::logic_error("WAL frame already finished");
    }
    if (bytes == nullptr || size == 0) {
        throw std::invalid_argument("WAL delta payload must not be empty");
    }
    if (size > std::numeric_limits<std::uint32_t>::max() - kWalSpanOffsetSize ||
        size > std::numeric_limits<std::uint32_t>::max() - byte_offset) {
        throw std::invalid_argument("WAL span too large");
    }
    if (record_count_ >= std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("WAL frame record count overflow");
    }
    batch_->reserve(batch_->size() + kWalRecordHeaderSize + kWalSpanOffsetSize + size);
    batch_->push_back(kWalRecordSpan);
    WriteLe32(*batch_, static_cast<std::uint32_t>(kWalSpanOffsetSize + size));
    WriteLe32(*batch_, byte_offset);
    batch_->insert(batch_->end(), bytes, bytes + size);
    record_count_ += 1;
}

void WalFrameBuilder::BeginRecord(std::uint8_t type, std::size_t body_size) {
    if (finished_) {
        throw std::logic_error("WAL frame already finished");
    }
    if (body_size > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("WAL record too large");
    }
    if (record_count_ >= std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("WAL frame record count overflow");
    }
    batch_->push_back(type);
    WriteLe32(*batch_, static_cast<std::uint32_t>(body_size));
    record_count_ += 1;
}

void WalFrameBuilder::RequireVarOrder(VarKey key) {
    if (has_var_replace_ || (last_var_key_.has_value() && key <= *last_var_key_)) {
        throw std::logic_error("value records of a WAL frame out of order");
    }
    last_var_key_ = key;
}

void WalFrameBuilder::AppendVarPut(VarKey key, std::span<const std::uint8_t> value) {
    RequireVarOrder(key);
    BeginRecord(kWalRecordVarPut, kVarEntryHeaderBytes + value.size());
    WriteLe32(*batch_, key.column_id);
    WriteLe32(*batch_, key.block_index);
    WriteLe32(*batch_, static_cast<std::uint32_t>(value.size()));
    batch_->insert(batch_->end(), value.begin(), value.end());
}

void WalFrameBuilder::AppendVarDel(VarKey key) {
    RequireVarOrder(key);
    BeginRecord(kWalRecordVarDel, 8U);
    WriteLe32(*batch_, key.column_id);
    WriteLe32(*batch_, key.block_index);
}

void WalFrameBuilder::AppendVarReplace(const ChunkVars& vars) {
    if (has_var_replace_ || last_var_key_.has_value()) {
        throw std::logic_error("value records of a WAL frame out of order");
    }
    has_var_replace_ = true;
    BeginRecord(kWalRecordVarReplace, vars.encoded_size());
    batch_->insert(batch_->end(), vars.Encode().begin(), vars.Encode().end());
}

void WalFrameBuilder::AppendVarUpdate(const ChunkVars& vars, const VarUndo& undo) {
    if (undo.replaced) {
        AppendVarReplace(vars);
        return;
    }
    // One reservation for all records, so a large update does not copy the
    // batch once per record.
    std::size_t bytes = 0;
    for (const auto key : undo.keys) {
        const auto value = vars.Find(key);
        bytes += kWalRecordHeaderSize + (value.has_value() ? kVarEntryHeaderBytes + value->size() : 8U);
    }
    batch_->reserve(batch_->size() + bytes);
    for (const auto key : undo.keys) {
        if (const auto value = vars.Find(key); value.has_value()) {
            AppendVarPut(key, *value);
        } else {
            AppendVarDel(key);
        }
    }
}

std::size_t WalFrameBuilder::Finish(std::uint64_t revision, std::uint64_t commit_time_ms) {
    if (finished_) {
        throw std::logic_error("WAL frame already finished");
    }
    if (record_count_ == 0) {
        throw std::invalid_argument("WAL frame must contain at least one record");
    }
    const std::size_t body_size = batch_->size() - records_begin_;
    if (body_size > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("WAL frame body too large");
    }

    std::vector<std::uint8_t> header;
    header.reserve(kWalFrameFixedHeaderSize);
    header.insert(header.end(), kWalFrameMagic, kWalFrameMagic + kWalFrameMagicSize);
    WriteLe64(header, revision);
    WriteLe64(header, commit_time_ms);
    WriteLe16(header, 0U);  // frame flags
    WriteLe16(header, tlv_size_);
    WriteLe32(header, static_cast<std::uint32_t>(record_count_));
    WriteLe32(header, static_cast<std::uint32_t>(body_size));
    std::copy(header.begin(), header.end(), batch_->begin() + static_cast<std::ptrdiff_t>(header_index_));

    // The header CRC covers everything after the magic up to the end of the
    // TLV area, so a tag is as protected as the revision.
    const std::size_t crc_at = records_begin_ - kWalFrameHeaderCrcSize;
    const std::uint32_t header_crc = Crc32(
        batch_->data() + header_index_ + kWalFrameMagicSize,
        crc_at - header_index_ - kWalFrameMagicSize);
    for (std::size_t i = 0; i < 4U; ++i) {
        (*batch_)[crc_at + i] = static_cast<std::uint8_t>((header_crc >> (8U * i)) & 0xFFU);
    }

    WriteLe32(*batch_, Crc32(batch_->data() + records_begin_, body_size));
    finished_ = true;
    return batch_->size() - header_index_;
}
std::uint64_t ChunkStore::CurrentWalFileSize(
    const std::shared_ptr<RegularChunk>& chunk) const {
    if (chunk->wal_path.empty()) {
        return 0;
    }
    std::error_code exists_ec;
    const bool present = std::filesystem::exists(chunk->wal_path, exists_ec);
    if (exists_ec) {
        throw std::runtime_error(
            "failed to stat WAL for rollback baseline: " + chunk->wal_path.string() +
            " (ec=" + std::to_string(exists_ec.value()) + ", msg='" + exists_ec.message() + "')");
    }
    if (!present) {
        // Definitively absent: there is no committed WAL to preserve.
        return 0;
    }
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_SIZE_STAT_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected WAL size inspection failure: " + chunk->wal_path.string());
    }
    std::error_code ec;
    const auto size = std::filesystem::file_size(chunk->wal_path, ec);
    if (ec) {
        // Never interpret an inspection failure as an empty WAL: doing so would
        // let a failed rollback remove or under-truncate a WAL that still holds
        // committed records. Fail closed so the caller can neutralize safely.
        throw std::runtime_error(
            "failed to determine WAL size for rollback baseline: " + chunk->wal_path.string() +
            " (ec=" + std::to_string(ec.value()) + ", msg='" + ec.message() + "')");
    }
    return static_cast<std::uint64_t>(size);
}

void ChunkStore::TruncateWalTail(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    std::uint64_t committed_size,
    bool force_sync) {
    SnapshotGenerationWriteGuard snapshot_write(this);
    if (chunk->wal_path.empty()) {
        chunk->wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);
    }
    chunk->wal_boundary_durable = false;

    // Close the append stream so its buffered position cannot resurrect the
    // truncated tail on the next write.
    CloseWalAppendStream(chunk);

    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_EXISTS_STAT_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected WAL existence inspection failure during rollback: " + chunk->wal_path.string());
    }

    std::error_code exists_ec;
    const bool present = std::filesystem::exists(chunk->wal_path, exists_ec);
    if (exists_ec) {
        // An errored stat is not proof of absence. Treating it as "nothing to
        // neutralize" would leave the rejected record on disk while the caller
        // reports a clean rollback, so fail closed instead.
        throw std::runtime_error(
            "failed to stat WAL during rollback: " + chunk->wal_path.string() +
            " (ec=" + std::to_string(exists_ec.value()) + ", msg='" + exists_ec.message() + "')");
    }
    if (!present) {
        // Nothing on disk to neutralize.
        chunk->wal_header_written = false;
        snapshot_write.Finish();
        return;
    }

    if (committed_size == 0) {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_REMOVE_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected WAL removal failure during rollback: " + chunk->wal_path.string());
        }
        std::error_code remove_ec;
        std::filesystem::remove(chunk->wal_path, remove_ec);
        if (remove_ec) {
            throw std::runtime_error(
                "failed to remove WAL during rollback: " + chunk->wal_path.string() +
                " (ec=" + std::to_string(remove_ec.value()) + ", msg='" + remove_ec.message() + "')");
        }
        chunk->wal_header_written = false;
        if (force_sync) {
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_ROLLBACK_SYNC_FAIL_ONCE")) {
                throw std::runtime_error(
                    "injected WAL rollback sync failure: " + chunk->wal_path.string());
            }
            SyncDirectoryPath(chunk->wal_path.parent_path());
        } else {
            NoteUnsyncedDir(chunk->wal_path.parent_path());
        }
        snapshot_write.Finish();
        return;
    }

    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected WAL truncation failure during rollback: " + chunk->wal_path.string());
    }
    std::error_code resize_ec;
    std::filesystem::resize_file(chunk->wal_path, committed_size, resize_ec);
    if (resize_ec) {
        throw std::runtime_error(
            "failed to truncate WAL during rollback: " + chunk->wal_path.string() +
            " (ec=" + std::to_string(resize_ec.value()) + ", msg='" + resize_ec.message() + "')");
    }
    chunk->wal_header_written = committed_size >= kWalHeaderSize;
    if (force_sync) {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_ROLLBACK_SYNC_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected WAL rollback sync failure: " + chunk->wal_path.string());
        }
        SyncFilePath(chunk->wal_path);
    } else {
        NoteUnsyncedFile(chunk->wal_path);
    }
    snapshot_write.Finish();
}

void ChunkStore::FlushWalBatch(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool force_sync) {
    if (chunk->wal_batch.empty()) {
        return;
    }

    // Captured before any file mutation: on a failed or torn append the WAL
    // is truncated back to this boundary, so a later retry of the retained
    // batch can never duplicate records behind partial bytes, and a rejected
    // ordinary mutation cannot leave its records durable.
    const std::uint64_t pre_flush_size = CurrentWalFileSize(chunk);

    SnapshotGenerationWriteGuard snapshot_write(this);
    bool first_create = false;
    bool stream_artifact_touched = false;
    bool batch_write_started = false;
    try {
        EnsureWalAppendStream(
            chunk_coord,
            chunk,
            &first_create,
            &stream_artifact_touched);

        if (const auto hold = ConsumeFailpointDelayMs("CHUNKDB_FAILPOINT_WAL_APPEND_HOLD_MS_ONCE");
            hold.count() > 0) {
            std::this_thread::sleep_for(hold);
        }

        // EnsureWalAppendStream returned normally, so the lazily created
        // stream exists and is open.
        std::ofstream& output = *chunk->wal_append_stream;
        batch_write_started = true;
        output.write(
            reinterpret_cast<const char*>(chunk->wal_batch.data()),
            static_cast<std::streamsize>(chunk->wal_batch.size()));
        output.flush();
        if (!output.good()) {
            throw std::runtime_error("failed to append WAL record batch: " + chunk->wal_path.string());
        }
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected WAL batch sync failure: " + chunk->wal_path.string());
        }

        // A new WAL's directory's own entry, or bytes written without a
        // sync, are not known durable.
        if (!force_sync || first_create) {
            chunk->wal_boundary_durable = false;
        }
        if (force_sync) {
            SyncFilePath(chunk->wal_path);
            if (first_create) {
                if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_AFTER_FILE_SYNC_BEFORE_DIR_SYNC_ONCE")) {
                    throw std::runtime_error(
                        "injected WAL sync failure after file sync before directory sync: " + chunk->wal_path.string());
                }
                SyncDirectoryPath(chunk->wal_path.parent_path());
            }
        } else {
            NoteUnsyncedFile(chunk->wal_path);
            if (first_create) {
                NoteUnsyncedDir(chunk->wal_path.parent_path());
            }
        }
    } catch (...) {
        // Reset append stream on any failure so the next write can reopen cleanly.
        CloseWalAppendStream(chunk);
        if (!stream_artifact_touched && !batch_write_started) {
            // Capacity and injected pre-open failures have not changed image,
            // WAL, or intent state. They can close this generation normally
            // without masking another overlapping successful transition.
            snapshot_write.Finish();
            throw;
        }
        // Bytes may have reached the file (fully or torn). Neutralize the
        // appended tail while still inside this odd generation so no reader
        // and no later retry can observe records that were never
        // acknowledged. The batch itself is retained for the caller.
        try {
            TruncateWalTail(chunk_coord, chunk, pre_flush_size, force_sync);
            snapshot_write.Finish();
        } catch (const std::exception& repair_error) {
            // The un-acknowledged tail could not be removed. Fail closed:
            // the generation stays odd and the store stops serving
            // durability-changing operations until restart recovery, which
            // may replay those records.
            const std::string reason =
                "WAL append repair failed for chunk (" + std::to_string(chunk_coord.x) + "," +
                std::to_string(chunk_coord.y) + "): " + repair_error.what();
            PoisonDurability(reason);
            chunk->wal_repair_failed = true;
            throw WriteOutcomeUnknownError(
                "the records of this flush may or may not be applied: " + reason +
                "; the store is fail-closed until restart");
        }
        throw;
    }

    stats_wal_batch_flushes_.fetch_add(1, std::memory_order_relaxed);
    chunk->wal_batch.clear();
    chunk->pending_wal_flush_updates = 0;
    // The batch is durably appended and cleared — this flush has committed.
    // Publishing the even snapshot generation is post-commit bookkeeping: if
    // it fails, the generation stays odd, which fail-closes the store on the
    // next transition (the designed recovery path). Surfacing that failure as
    // a throw here would make an ordinary-write caller roll back and return
    // -ERR for a write that is already durable, resurrecting it after
    // restart. Log and swallow instead; never contradict the commit.
    try {
        snapshot_write.Finish();
    } catch (const std::exception& publish_error) {
        LogMessage(
            LogLevel::kWarn,
            LogComponent::kStore,
            "WAL batch committed but snapshot generation could not be republished; "
            "store will fail closed until writer restart",
            {
                {"chunk_x", std::to_string(chunk_coord.x)},
                {"chunk_y", std::to_string(chunk_coord.y)},
                {"error", publish_error.what()},
            });
    }
}

ChunkStore::UnsyncedArtifacts ChunkStore::ForgetUnsynced(
    const std::vector<std::filesystem::path>& files,
    const std::vector<std::filesystem::path>& dirs) {
    UnsyncedArtifacts forgotten;
    std::lock_guard lock(unsynced_mutex_);
    for (const auto& file : files) {
        if (unsynced_files_.erase(file.string()) > 0U) {
            forgotten.files.insert(file.string());
        }
    }
    for (const auto& dir : dirs) {
        if (unsynced_dirs_.erase(dir.string()) > 0U) {
            forgotten.dirs.insert(dir.string());
        }
    }
    return forgotten;
}

void ChunkStore::MakeWalBoundaryDurableLocked(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool own_stream) {
    if (chunk->wal_path.empty()) {
        chunk->wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);
    }
    // Every acknowledged frame goes before the boundary.
    if (own_stream) {
        FlushWalBatchForEviction(chunk_coord, chunk, /*force_sync=*/true);
    } else {
        FlushWalBatch(chunk_coord, chunk, /*force_sync=*/true);
    }
    if (chunk->wal_boundary_durable) {
        return;
    }
    // An earlier relaxed write, checkpoint or empty-chunk collection (in
    // this process, or in one that crashed) may have left any of these
    // without a sync: the image a frame applies over, the WAL's bytes and
    // entry, an image or WAL removal, the large-chunk directory's entry.
    const auto image_path = ChunkDataPath(data_dir_, geometry_, chunk_coord);
    const auto& wal_path = chunk->wal_path;
    const auto directory = wal_path.parent_path();
    // Dropped from the unsynced set before the syncs, so a change noted
    // after this point stays tracked.
    const auto forgotten = ForgetUnsynced({image_path, wal_path}, {directory, data_dir_});
    try {
        const auto exists = [](const std::filesystem::path& path) {
            std::error_code ec;
            const bool present = std::filesystem::exists(path, ec);
            if (ec) {
                throw std::runtime_error(
                    "failed to inspect " + path.string() + " before a rollback boundary: " + ec.message());
            }
            return present;
        };
        if (exists(image_path)) {
            SyncFilePath(image_path);
        }
        if (exists(wal_path)) {
            SyncFilePath(wal_path);
        }
        if (exists(directory)) {
            SyncDirectoryPath(directory);
        }
        SyncDirectoryPath(data_dir_);
    } catch (...) {
        for (const auto& file : forgotten.files) {
            NoteUnsyncedFile(file);
        }
        for (const auto& dir : forgotten.dirs) {
            NoteUnsyncedDir(dir);
        }
        throw;
    }
    chunk->wal_boundary_durable = true;
}

void ChunkStore::FlushWalBatchForEviction(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool force_sync) {
    if (chunk->wal_batch.empty()) {
        return;
    }

    // On eviction, bypass WAL stream-cache tracking when no stream is currently open.
    // This avoids extra map/mutex churn for streams that will be closed immediately.
    if (!chunk->wal_stream_initialized.load(std::memory_order_acquire) ||
        !WalAppendStreamOpen(*chunk)) {
        if (chunk->wal_path.empty()) {
            chunk->wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);
        }
        // Capture the rollback baseline BEFORE the generation guard, so a stat
        // failure is a clean pre-transition error rather than abandoning the
        // odd epoch (which would fail-close the whole store until restart).
        const std::uint64_t pre_flush_size = CurrentWalFileSize(chunk);
        SnapshotGenerationWriteGuard snapshot_write(this);
        bool file_write_started = false;
        try {
            const auto wal_parent_path = chunk->wal_path.parent_path();
            EnsureWalParentDirectoryCached(
                wal_parent_path,
                false,
                durability_mode_ != DurabilityMode::kRelaxed);

            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_OPEN_ONCE")) {
                throw std::runtime_error("injected WAL open failure: " + chunk->wal_path.string());
            }

            const bool needs_header = !chunk->wal_header_written;
            std::ofstream out(chunk->wal_path, std::ios::binary | std::ios::app);
            if (!out.is_open()) {
                int open_err = errno;
                InvalidateWalParentDirectoryCache(wal_parent_path);
                EnsureWalParentDirectoryCached(
                    wal_parent_path,
                    true,
                    durability_mode_ != DurabilityMode::kRelaxed);
                out.clear();
                out.open(chunk->wal_path, std::ios::binary | std::ios::app);
                if (!out.is_open()) {
                    open_err = errno;
                    throw BuildWalOpenError(chunk->wal_path, open_err);
                }
            }
            file_write_started = true;

            if (needs_header) {
                const auto wal_header = BuildWalHeader(chunk_coord, store_id_, features_);
                out.write(
                    reinterpret_cast<const char*>(wal_header.data()),
                    static_cast<std::streamsize>(wal_header.size()));
                if (!out.good()) {
                    throw std::runtime_error("failed to append WAL header: " + chunk->wal_path.string());
                }
                chunk->wal_header_written = true;
            }

            out.write(
                reinterpret_cast<const char*>(chunk->wal_batch.data()),
                static_cast<std::streamsize>(chunk->wal_batch.size()));
            out.flush();
            if (!out.good()) {
                throw std::runtime_error("failed to append WAL record batch: " + chunk->wal_path.string());
            }
            if (!force_sync || needs_header) {
                chunk->wal_boundary_durable = false;
            }

            if (force_sync) {
                SyncFilePath(chunk->wal_path);
                if (needs_header) {
                    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_AFTER_FILE_SYNC_BEFORE_DIR_SYNC_ONCE")) {
                        throw std::runtime_error(
                            "injected WAL sync failure after file sync before directory sync: " +
                            chunk->wal_path.string());
                    }
                    SyncDirectoryPath(wal_parent_path);
                }
            } else {
                NoteUnsyncedFile(chunk->wal_path);
                if (needs_header) {
                    NoteUnsyncedDir(wal_parent_path);
                }
            }
        } catch (...) {
            if (!file_write_started) {
                snapshot_write.Finish();
                throw;
            }
            // Same torn-append repair as FlushWalBatch: the retained batch
            // must never be re-appended behind partial bytes.
            try {
                TruncateWalTail(chunk_coord, chunk, pre_flush_size, force_sync);
                snapshot_write.Finish();
            } catch (const std::exception& repair_error) {
                PoisonDurability(
                    "WAL append repair failed for chunk (" +
                    std::to_string(chunk_coord.x) + "," +
                    std::to_string(chunk_coord.y) + "): " + repair_error.what());
                chunk->wal_repair_failed = true;
            }
            throw;
        }

        stats_wal_batch_flushes_.fetch_add(1, std::memory_order_relaxed);
        chunk->wal_batch.clear();
        chunk->pending_wal_flush_updates = 0;
        // Post-commit even-generation publication: a failure here leaves the
        // generation odd (store fail-closes on the next transition), which
        // must not be surfaced as an eviction error for an already-durable
        // flush. Log and swallow, matching FlushWalBatch.
        try {
            snapshot_write.Finish();
        } catch (const std::exception& publish_error) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kStore,
                "eviction WAL flush committed but snapshot generation could not be "
                "republished; store will fail closed until writer restart",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"error", publish_error.what()},
                });
        }
        return;
    }

    // Stream already open: FlushWalBatch owns its own generation guard and its
    // own commit/repair/publish handling, so delegate without wrapping it in a
    // second outer guard (which a throw would abandon, fail-closing the store).
    FlushWalBatch(chunk_coord, chunk, force_sync);
}
void ChunkStore::FlushWalBatchesForReopen() {
    std::vector<std::shared_ptr<LargeChunk>> large_chunks;
    {
        std::lock_guard lock(large_chunks_mutex_);
        large_chunks.reserve(large_chunks_.size());
        for (const auto& [_, large_chunk] : large_chunks_) {
            large_chunks.push_back(large_chunk);
        }
    }
    for (const auto& large_chunk : large_chunks) {
        std::vector<std::pair<ChunkCoord, std::shared_ptr<RegularChunk>>> chunks;
        {
            std::lock_guard lock(large_chunk->mutex);
            chunks.reserve(large_chunk->chunks.size());
            for (const auto& [coord, chunk] : large_chunk->chunks) {
                chunks.emplace_back(coord, chunk);
            }
        }
        for (const auto& [coord, chunk] : chunks) {
            std::unique_lock chunk_lock(chunk->mutex);
            FlushWalBatch(coord, chunk, durability_mode_ != DurabilityMode::kRelaxed);
        }
    }
}

void ChunkStore::FlushAllPendingWalBatches() noexcept {
    std::vector<std::shared_ptr<LargeChunk>> large_chunks;
    {
        std::lock_guard lock(large_chunks_mutex_);
        large_chunks.reserve(large_chunks_.size());
        for (const auto& [_, large_chunk] : large_chunks_) {
            large_chunks.push_back(large_chunk);
        }
    }

    for (const auto& large_chunk : large_chunks) {
        std::vector<std::pair<ChunkCoord, std::shared_ptr<RegularChunk>>> chunks;
        {
            std::lock_guard lock(large_chunk->mutex);
            chunks.reserve(large_chunk->chunks.size());
            for (const auto& [coord, chunk] : large_chunk->chunks) {
                chunks.emplace_back(coord, chunk);
            }
        }

        for (const auto& [coord, chunk] : chunks) {
            std::unique_lock chunk_lock(chunk->mutex);
            try {
                FlushWalBatch(
                    coord,
                    chunk,
                    durability_mode_ != DurabilityMode::kRelaxed);
            } catch (const std::exception& e) {
                try {
                    LogMessage(
                        LogLevel::kError,
                        LogComponent::kStore,
                        "shutdown WAL flush failed; durability may be violated",
                        {
                            {"chunk_x", std::to_string(coord.x)},
                            {"chunk_y", std::to_string(coord.y)},
                            {"wal_path", chunk->wal_path.empty() ? "unset" : chunk->wal_path.string()},
                            {"pending_batch_bytes", std::to_string(chunk->wal_batch.size())},
                            {"durability_mode", DurabilityModeName(durability_mode_)},
                            {"error", e.what()},
                        });
                } catch (...) {
                }
            } catch (...) {
                try {
                    LogMessage(
                        LogLevel::kError,
                        LogComponent::kStore,
                        "shutdown WAL flush failed; durability may be violated",
                        {
                            {"chunk_x", std::to_string(coord.x)},
                            {"chunk_y", std::to_string(coord.y)},
                            {"wal_path", chunk->wal_path.empty() ? "unset" : chunk->wal_path.string()},
                            {"pending_batch_bytes", std::to_string(chunk->wal_batch.size())},
                            {"durability_mode", DurabilityModeName(durability_mode_)},
                            {"error", "unknown"},
                        });
                } catch (...) {
                }
            }
            CloseWalAppendStream(chunk);
        }
    }
}

}  // namespace chunkdb
