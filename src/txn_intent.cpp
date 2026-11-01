#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "txn_history.hpp"
#include "txn_intent_format.hpp"
#include "wal_replay.hpp"

namespace chunkdb {

namespace {

void CrashAtRecoveryFailpoint(const char* key) {
    if (ConsumeFailpointEnv(key)) {
        std::_Exit(86);
    }
}

void ValidateRollbackMember(const std::filesystem::path& data_dir, const Geometry& geometry,
    const StoreId& table_id, FeatureFlags features, const TxnIntentEntry& entry) {
    const auto wal_path = ChunkWalPath(data_dir, geometry, entry.coord);
    std::error_code ec;
    const auto status = std::filesystem::symlink_status(wal_path, ec);
    if (ec && ec != std::errc::no_such_file_or_directory)
        throw std::runtime_error("failed to inspect rollback WAL " + wal_path.string() + ": " + ec.message());
    if (!std::filesystem::exists(status)) {
        if (entry.wal_boundary != 0U)
            throw std::runtime_error("missing rollback WAL " + wal_path.string());
        return;
    }
    if (!std::filesystem::is_regular_file(status))
        throw std::runtime_error("rollback WAL is not a regular file: " + wal_path.string());
    const auto size = std::filesystem::file_size(wal_path, ec);
    if (ec || size < entry.wal_boundary)
        throw std::runtime_error("rollback WAL is shorter than its committed boundary: " + wal_path.string());
    if (entry.wal_boundary == 0U) return;
    if (entry.wal_boundary > std::numeric_limits<std::size_t>::max() ||
        entry.wal_boundary > static_cast<std::uint64_t>(std::numeric_limits<std::streamsize>::max()))
        throw std::runtime_error("rollback WAL boundary exceeds addressable input: " + wal_path.string());
    std::ifstream file(wal_path, std::ios::binary);
    if (!file) throw std::runtime_error("failed to open rollback WAL " + wal_path.string());
    std::vector<std::uint8_t> prefix(static_cast<std::size_t>(entry.wal_boundary));
    file.read(reinterpret_cast<char*>(prefix.data()), static_cast<std::streamsize>(prefix.size()));
    if (file.gcount() != static_cast<std::streamsize>(prefix.size()))
        throw std::runtime_error("failed to read rollback WAL boundary: " + wal_path.string());
    auto payload = std::vector<std::uint8_t>(geometry.ChunkPayloadBytes(), 0U);
    auto presence = std::vector<std::uint8_t>(ChunkPresenceBitmapBytes(geometry), 0U);
    ChunkVars vars;
    std::shared_ptr<const ChunkColumnState> chunk_columns;
    std::uint64_t base = 0U, schema = 0U;
    const auto image_path = ChunkDataPath(data_dir, geometry, entry.coord);
    if (std::filesystem::exists(image_path)) {
        auto image = ParseChunkImage(LoadChunkImageFile(image_path, geometry, entry.coord, table_id, features),
            geometry, entry.coord, table_id, features);
        base = image.revision;
        schema = image.schema_version;
        payload = std::move(image.payload);
        presence = std::move(image.presence_bitmap);
        vars = std::move(image.vars);
        chunk_columns = std::move(image.chunk_columns);
    }
    const auto replay = ReplayWal(prefix, geometry, entry.coord, table_id, features,
        base, schema, &payload, &presence, &vars, nullptr, &chunk_columns);
    if (!replay.replayable || replay.tail_truncated_or_corrupt ||
        replay.valid_end != prefix.size() || !replay.vars_problem.empty())
        throw std::runtime_error("rollback WAL boundary is not a complete committed prefix: " + wal_path.string());
}

}  // namespace

std::vector<std::uint8_t> SerializeTxnIntent(const TxnIntent& intent, std::uint8_t dims, const StoreId& table_id) {
    return SerializeCheckedTxnIntent(intent, dims, table_id, kMaxTxnWrittenChunks);
}

bool TryParseTxnIntent(const std::vector<std::uint8_t>& bytes, TxnIntent* out,
    std::uint8_t dims, const StoreId& table_id) {
    if (out == nullptr) return false;
    try { *out = ParseCheckedTxnIntent(bytes, dims, table_id, kMaxTxnWrittenChunks); }
    catch (const std::invalid_argument&) { return false; }
    return true;
}

std::filesystem::path TxnIntentPath(const std::filesystem::path& data_dir, std::uint64_t version) {
    return ConditionalIntentDirectory(data_dir) /
           (std::string(kTxnIntentPrefix) + std::to_string(version) + std::string(kTxnIntentSuffix));
}

bool IsTxnIntentFileName(std::string_view name) noexcept {
    if (name.size() <= kTxnIntentPrefix.size() + kTxnIntentSuffix.size() || !name.starts_with(kTxnIntentPrefix) ||
        !name.ends_with(kTxnIntentSuffix)) {
        return false;
    }
    const auto digits =
        name.substr(kTxnIntentPrefix.size(), name.size() - kTxnIntentPrefix.size() - kTxnIntentSuffix.size());
    return digits.front() != '0' &&
        std::all_of(digits.begin(), digits.end(), [](char c) { return c >= '0' && c <= '9'; });
}

bool IsTxnIntentArtifactName(std::string_view name) noexcept {
    return name.starts_with(kTxnIntentPrefix);
}


std::optional<std::uint64_t> TxnRollbackBoundaryForChunk(
    const std::vector<std::vector<std::uint8_t>>& intents,
    const ChunkCoord& coord, const Geometry& geometry, const StoreId& table_id) {
    std::optional<std::uint64_t> boundary;
    for (const auto& bytes : intents) {
        TxnIntent intent;
        if (!TryParseTxnIntent(bytes, &intent, geometry.dims(), table_id)) {
            throw std::runtime_error(
                "read-only chunk snapshot contains a malformed transaction intent for chunk (" +
                std::to_string(coord.x) + "," + std::to_string(coord.y) + ")");
        }
        if (intent.state != TxnIntentState::kRollback) {
            continue;
        }
        for (const auto& entry : intent.entries) {
            if (entry.coord == coord) {
                boundary = std::min(boundary.value_or(entry.wal_boundary), entry.wal_boundary);
            }
        }
    }
    return boundary;
}

void ChunkStore::RecoverTransactionIntents(bool validate_only) {
    if (access_mode_ == AccessMode::kReadOnly) {
        return;
    }
    const auto intent_dir = ConditionalIntentDirectory(data_dir_);
    std::error_code dir_ec;
    const bool intent_dir_present = std::filesystem::exists(intent_dir, dir_ec);
    if (dir_ec) {
        throw std::runtime_error(
            "failed to inspect intent directory " + intent_dir.string() + ": " + dir_ec.message());
    }
    if (!intent_dir_present) {
        return;
    }

    std::vector<std::pair<std::uint64_t, std::filesystem::path>> intents;
    // Intents whose temporary files a crash left behind.
    std::vector<std::filesystem::path> interrupted;
    std::error_code iterator_ec;
    std::filesystem::directory_iterator iterator(intent_dir, std::filesystem::directory_options::none, iterator_ec);
    if (iterator_ec) {
        throw std::runtime_error(
            "failed to list transaction intents under " + intent_dir.string() + ": " + iterator_ec.message());
    }
    for (const std::filesystem::directory_iterator end; iterator != end;) {
        const auto path = iterator->path();
        std::error_code type_ec;
        const bool regular = iterator->is_regular_file(type_ec);
        if (type_ec) {
            throw std::runtime_error("failed to inspect intent " + path.string() + ": " + type_ec.message());
        }
        iterator.increment(iterator_ec);
        if (iterator_ec) {
            throw std::runtime_error(
                "failed while listing transaction intents under " + intent_dir.string() + ": " +
                iterator_ec.message());
        }
        const std::string name = path.filename().string();
        if (!regular || !IsTxnIntentArtifactName(name)) {
            continue;
        }
        if (const auto tmp = name.find(".tmp."); tmp != std::string::npos) {
            interrupted.push_back(intent_dir / name.substr(0, tmp));
            continue;
        }
        if (!IsTxnIntentFileName(name)) {
            throw std::runtime_error("unexpected transaction intent file " + path.string());
        }
        std::uint64_t version = 0;
        const auto digits = name.substr(kTxnIntentPrefix.size(), name.size() - kTxnIntentPrefix.size() - kTxnIntentSuffix.size());
        if (!TryParseUint64(digits, &version)) {
            throw std::runtime_error("unexpected transaction intent file " + path.string());
        }
        intents.emplace_back(version, path);
    }
    std::sort(intents.begin(), intents.end());

    // Every intent and member is checked before any WAL changes.
    std::vector<TxnIntent> parsed(intents.size());
    for (std::size_t i = 0; i < intents.size(); ++i) {
        const auto& [version, path] = intents[i];
        if (!TryParseTxnIntent(LoadFile(path), &parsed[i], geometry_.dims(), store_id_) || parsed[i].version != version) {
            throw std::runtime_error("invalid transaction intent: " + path.string());
        }
    }
    for (const auto& intent : parsed) {
        if (intent.state != TxnIntentState::kRollback) continue;
        for (const auto& entry : intent.entries)
            ValidateRollbackMember(data_dir_, geometry_, store_id_, features_, entry);
    }
    if (validate_only) return;

    for (std::size_t i = 0; i < intents.size(); ++i) {
        const auto& path = intents[i].second;
        const TxnIntent& intent = parsed[i];
        if (intent.state == TxnIntentState::kRollback) {
            // Crashed before its commit point: every listed WAL goes back to
            // its boundary. Each step is synced before the intent goes, so a
            // crash in between repeats it at the next start.
            for (const auto& entry : intent.entries) {
                const auto wal_path = ChunkWalPath(data_dir_, geometry_, entry.coord);
                std::error_code exists_ec;
                const bool wal_present = std::filesystem::exists(wal_path, exists_ec);
                if (exists_ec) {
                    throw std::runtime_error(
                        "failed to inspect WAL " + wal_path.string() + " for transaction intent " + path.string() +
                        ": " + exists_ec.message());
                }
                if (!wal_present) {
                    if (entry.wal_boundary != 0U) {
                        throw std::runtime_error(
                            "WAL " + wal_path.string() + " required by transaction intent " + path.string() +
                            " is missing");
                    }
                    std::error_code parent_ec;
                    if (std::filesystem::exists(wal_path.parent_path(), parent_ec)) {
                        SyncDirectoryPath(wal_path.parent_path());
                    } else if (parent_ec) {
                        throw std::runtime_error(
                            "failed to inspect " + wal_path.parent_path().string() + ": " + parent_ec.message());
                    }
                    continue;
                }
                std::error_code size_ec;
                const auto size = std::filesystem::file_size(wal_path, size_ec);
                if (size_ec) {
                    throw std::runtime_error(
                        "failed to inspect WAL size " + wal_path.string() + ": " + size_ec.message());
                }
                if (size < entry.wal_boundary) {
                    throw std::runtime_error(
                        "WAL " + wal_path.string() + " is shorter than the boundary transaction intent " +
                        path.string() + " records");
                }
                if (entry.wal_boundary == 0U) {
                    std::error_code remove_ec;
                    std::filesystem::remove(wal_path, remove_ec);
                    if (remove_ec) {
                        throw std::runtime_error(
                            "failed to remove WAL " + wal_path.string() + " of an uncommitted transaction: " +
                            remove_ec.message());
                    }
                    SyncDirectoryPath(wal_path.parent_path());
                    continue;
                }
                if (size != entry.wal_boundary) {
                    std::error_code resize_ec;
                    std::filesystem::resize_file(wal_path, entry.wal_boundary, resize_ec);
                    if (resize_ec) {
                        throw std::runtime_error(
                            "failed to truncate WAL " + wal_path.string() + " of an uncommitted transaction: " +
                            resize_ec.message());
                    }
                }
                SyncFilePath(wal_path);
                CrashAtRecoveryFailpoint("CHUNKDB_FAILPOINT_CRASH_TXN_RECOVERY_AFTER_TRUNCATE_ONCE");
            }
        }
        // Committed frames remain valid if a checkpoint has removed their WAL.
        std::error_code remove_ec;
        std::filesystem::remove(path, remove_ec);
        if (remove_ec) {
            throw std::runtime_error("failed to remove transaction intent " + path.string() + ": " + remove_ec.message());
        }
        SyncDirectoryPath(intent_dir);
        LogMessage(
            LogLevel::kInfo,
            LogComponent::kRecovery,
            intent.state == TxnIntentState::kRollback ? "rolled back an uncommitted transaction"
                                                      : "kept a committed transaction",
            {
                {"version", std::to_string(intent.version)},
                {"chunks", std::to_string(intent.entries.size())},
            });
    }
    for (const auto& target : interrupted) {
        CleanupAtomicTmpArtifacts(target);
    }
}

}  // namespace chunkdb
