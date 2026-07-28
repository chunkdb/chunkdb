#include <algorithm>
#include <array>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunk_store_internal.hpp"
#include "checkpoint.hpp"
#include "chunkdb/bit_codec.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "wal_writer.hpp"

namespace chunkdb {

namespace {

constexpr std::array<std::uint8_t, 4> kRollbackIntentMagic = {'C', 'K', 'R', 'B'};
constexpr std::array<std::uint8_t, 4> kCommittedIntentMagic = {'C', 'K', 'R', 'C'};
constexpr std::size_t kRollbackIntentRecordSize = 16;
constexpr std::string_view kRollbackIntentSuffix = ".rollback";

[[nodiscard]] std::vector<std::uint8_t> SerializeConditionalIntent(
    ConditionalIntentState state,
    std::uint64_t committed_wal_size) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kRollbackIntentRecordSize);
    const auto& magic =
        state == ConditionalIntentState::kRollback
            ? kRollbackIntentMagic
            : kCommittedIntentMagic;
    bytes.insert(bytes.end(), magic.begin(), magic.end());
    WriteLe64(bytes, committed_wal_size);
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    return bytes;
}

[[nodiscard]] bool TryParseIntent(
    const std::vector<std::uint8_t>& bytes,
    ConditionalIntentState* out_state,
    std::uint64_t* out_committed_wal_size) {
    if (bytes.size() != kRollbackIntentRecordSize ||
        ReadLe32(bytes, 12U) != Crc32(bytes.data(), 12U)) {
        return false;
    }
    if (std::equal(kRollbackIntentMagic.begin(), kRollbackIntentMagic.end(), bytes.begin())) {
        *out_state = ConditionalIntentState::kRollback;
    } else if (
        std::equal(kCommittedIntentMagic.begin(), kCommittedIntentMagic.end(), bytes.begin())) {
        *out_state = ConditionalIntentState::kCommitted;
    } else {
        return false;
    }
    *out_committed_wal_size = ReadLe64(bytes, 4U);
    return true;
}

}  // namespace

std::filesystem::path ConditionalIntentDirectory(
    const std::filesystem::path& data_dir) {
    return data_dir / std::string(kConditionalIntentDirName);
}

std::filesystem::path ConditionalIntentPathForWal(
    const std::filesystem::path& data_dir,
    const std::filesystem::path& wal_path) {
    auto relative = wal_path.lexically_relative(data_dir).generic_string();
    if (relative.empty() || relative.rfind("..", 0) == 0) {
        // A WAL outside the data directory cannot occur through the layout
        // helpers; fail loudly rather than fabricate an ambiguous name.
        throw std::invalid_argument(
            "WAL path is not inside the data directory: " + wal_path.string());
    }
    std::string flat;
    flat.reserve(relative.size());
    for (const char c : relative) {
        if (c == '/') {
            flat += "__";
        } else {
            flat += c;
        }
    }
    return ConditionalIntentDirectory(data_dir) /
           (flat + std::string(kRollbackIntentSuffix));
}

std::filesystem::path WalPathForConditionalIntent(
    const std::filesystem::path& data_dir,
    const std::filesystem::path& intent_path) {
    const std::string name = intent_path.filename().string();
    if (name.size() <= kRollbackIntentSuffix.size() ||
        name.substr(name.size() - kRollbackIntentSuffix.size()) !=
            kRollbackIntentSuffix) {
        throw std::invalid_argument(
            "not a conditional intent file name: " + intent_path.string());
    }
    const std::string flat =
        name.substr(0, name.size() - kRollbackIntentSuffix.size());
    std::filesystem::path wal_path = data_dir;
    std::size_t start = 0;
    while (true) {
        const std::size_t separator = flat.find("__", start);
        const std::string component = separator == std::string::npos
            ? flat.substr(start)
            : flat.substr(start, separator - start);
        // Components are always "L_<int>_<int>" or "C_<int>_<int>.wal" as produced
        // by the layout helpers, so none can be empty, "..", or absolute. Reject
        // anything else: the reconstructed path feeds resize_file() and remove()
        // during startup recovery, and this is the one place where a path is built
        // from a filename read off disk rather than from numeric coordinates.
        if (component.empty() || component == "." || component == ".." ||
            component.find('/') != std::string::npos ||
            component.find('\\') != std::string::npos) {
            throw std::invalid_argument(
                "malformed conditional intent file name: " + intent_path.string());
        }
        wal_path /= component;
        if (separator == std::string::npos) {
            break;
        }
        start = separator + 2U;
    }

    // Belt and braces: the component grammar above already forbids traversal, but
    // assert containment the same way ConditionalIntentPathForWal does before the
    // caller acts on this path destructively.
    const auto relative = wal_path.lexically_relative(data_dir).generic_string();
    if (relative.empty() || relative == ".." || relative.rfind("../", 0) == 0) {
        throw std::invalid_argument(
            "conditional intent resolves outside the data directory: " + intent_path.string());
    }
    return wal_path;
}

namespace {

void CrashAtFailpoint(const char* key) {
    if (ConsumeFailpointEnv(key)) {
        std::_Exit(86);
    }
}

}  // namespace

bool TryParseConditionalRollbackIntent(
    const std::vector<std::uint8_t>& bytes,
    std::uint64_t* out_committed_wal_size) {
    ConditionalIntentState state = ConditionalIntentState::kRollback;
    return TryParseIntent(bytes, &state, out_committed_wal_size) &&
           state == ConditionalIntentState::kRollback;
}

bool TryParseConditionalIntent(
    const std::vector<std::uint8_t>& bytes,
    ConditionalIntentState* out_state,
    std::uint64_t* out_committed_wal_size) {
    return TryParseIntent(bytes, out_state, out_committed_wal_size);
}

void ChunkStore::RecoverConditionalRollbackIntents() {
    if (access_mode_ == AccessMode::kReadOnly) {
        return;
    }

    // Intents live in one dedicated shallow directory, so recovery cost is
    // proportional to the number of pending intents, not to world size.
    const auto intent_dir = ConditionalIntentDirectory(data_dir_);
    std::error_code dir_exists_ec;
    const bool intent_dir_present =
        std::filesystem::exists(intent_dir, dir_exists_ec);
    if (dir_exists_ec) {
        throw std::runtime_error(
            "failed to inspect conditional intent directory " +
            intent_dir.string() + ": " + dir_exists_ec.message());
    }
    if (!intent_dir_present) {
        return;
    }

    std::error_code iterator_ec;
    std::filesystem::directory_iterator iterator(
        intent_dir,
        std::filesystem::directory_options::none,
        iterator_ec);
    if (iterator_ec) {
        throw std::runtime_error(
            "failed to inspect conditional rollback intents under " +
            intent_dir.string() + ": " + iterator_ec.message());
    }

    const std::filesystem::directory_iterator end;
    while (iterator != end) {
        std::error_code type_ec;
        const bool regular = iterator->is_regular_file(type_ec);
        if (type_ec) {
            throw std::runtime_error(
                "failed to inspect conditional rollback artifact " +
                iterator->path().string() + ": " + type_ec.message());
        }
        const auto intent_path = iterator->path();
        iterator.increment(iterator_ec);
        if (iterator_ec) {
            throw std::runtime_error(
                "failed while scanning conditional rollback intents under " +
                intent_dir.string() + ": " + iterator_ec.message());
        }
        if (!regular ||
            intent_path.string().size() <= kRollbackIntentSuffix.size() ||
            intent_path.string().substr(
                intent_path.string().size() - kRollbackIntentSuffix.size()) !=
                kRollbackIntentSuffix) {
            continue;
        }

        std::uint64_t committed_size = 0;
        ConditionalIntentState intent_state = ConditionalIntentState::kRollback;
        const auto bytes = LoadFile(intent_path);
        if (!TryParseIntent(bytes, &intent_state, &committed_size)) {
            throw std::runtime_error(
                "invalid conditional rollback intent: " + intent_path.string());
        }
        const std::filesystem::path wal_path =
            WalPathForConditionalIntent(data_dir_, intent_path);

        if (intent_state == ConditionalIntentState::kRollback) {
            std::error_code exists_ec;
            const bool wal_present = std::filesystem::exists(wal_path, exists_ec);
            if (exists_ec) {
                throw std::runtime_error(
                    "failed to inspect WAL while recovering rollback intent " +
                    intent_path.string() + ": " + exists_ec.message());
            }
            if (wal_present) {
                std::error_code size_ec;
                const auto actual_size = std::filesystem::file_size(wal_path, size_ec);
                if (size_ec) {
                    throw std::runtime_error(
                        "failed to inspect WAL size while recovering rollback intent " +
                        intent_path.string() + ": " + size_ec.message());
                }
                if (actual_size < committed_size) {
                    throw std::runtime_error(
                        "WAL is shorter than the committed rollback boundary for " +
                        intent_path.string());
                }
                if (committed_size == 0U) {
                    std::error_code remove_ec;
                    std::filesystem::remove(wal_path, remove_ec);
                    if (remove_ec) {
                        throw std::runtime_error(
                            "failed to remove rejected WAL while recovering " +
                            intent_path.string() + ": " + remove_ec.message());
                    }
                    SyncDirectoryPath(wal_path.parent_path());
                } else {
                    std::error_code resize_ec;
                    std::filesystem::resize_file(wal_path, committed_size, resize_ec);
                    if (resize_ec) {
                        throw std::runtime_error(
                            "failed to truncate rejected WAL while recovering " +
                            intent_path.string() + ": " + resize_ec.message());
                    }
                    SyncFilePath(wal_path);
                }
            } else if (committed_size != 0U) {
                throw std::runtime_error(
                    "WAL required by conditional rollback intent is missing: " +
                    intent_path.string());
            }
        }

        std::error_code remove_intent_ec;
        std::filesystem::remove(intent_path, remove_intent_ec);
        if (remove_intent_ec) {
            throw std::runtime_error(
                "failed to clear recovered conditional rollback intent " +
                intent_path.string() + ": " + remove_intent_ec.message());
        }
        SyncDirectoryPath(intent_path.parent_path());
    }
}

std::filesystem::path ChunkStore::WriteConditionalRollbackIntent(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    std::uint64_t committed_size) {
    const auto wal_path =
        chunk->wal_path.empty()
            ? LayoutWalPath(data_dir_, geometry_, chunk_coord, storage_layout_mode_)
            : chunk->wal_path;
    const auto intent_path = ConditionalIntentPathForWal(data_dir_, wal_path);
    // Intent publication is durable in every mode, so its directory must be
    // durably present as well.
    EnsureDirectoryPathExists(intent_path.parent_path(), /*durable_sync=*/true);

    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ROLLBACK_INTENT_INSPECT_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected conditional intent inspection failure: " +
            intent_path.string());
    }
    std::error_code exists_ec;
    const bool existing = std::filesystem::exists(intent_path, exists_ec);
    if (exists_ec) {
        throw std::runtime_error(
            "failed to inspect conditional intent " + intent_path.string() +
            ": " + exists_ec.message());
    }
    if (existing) {
        ConditionalIntentState existing_state = ConditionalIntentState::kRollback;
        std::uint64_t existing_boundary = 0;
        if (!TryParseIntent(
                LoadFile(intent_path), &existing_state, &existing_boundary)) {
            PoisonDurability(
                "invalid pre-existing conditional intent " +
                intent_path.string());
            throw std::runtime_error(
                "invalid pre-existing conditional intent: " +
                intent_path.string());
        }
        (void)existing_boundary;
        if (existing_state == ConditionalIntentState::kRollback) {
            PoisonDurability(
                "unresolved rollback intent appeared while the store was live: " +
                intent_path.string());
            throw std::runtime_error(
                "unresolved rollback intent appeared while the store was live: " +
                intent_path.string());
        }
        // A committed marker retained after a prior unlink failure is safe,
        // but it must be durably removed before publishing a new rollback
        // boundary for this chunk.
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ROLLBACK_INTENT_REPLACE_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected failure replacing pre-existing committed intent: " +
                intent_path.string());
        }
        ClearCommittedConditionalIntent(intent_path);
    }

    PauseConditionalMutationForTests(
        ConditionalMutationPausePoint::kBeforeRollbackIntentPublish);
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CONDITIONAL_BEFORE_INTENT_PUBLISH_ONCE")) {
        throw std::runtime_error(
            "injected conditional failure before rollback-intent publication");
    }
    CrashAtFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_INTENT_PUBLISH_ONCE");
    bool published = false;
    try {
        AtomicWrite(
            intent_path,
            SerializeConditionalIntent(
                ConditionalIntentState::kRollback, committed_size),
            /*fsync_file=*/true,
            /*fsync_directory=*/true,
            &published);
    } catch (...) {
        if (published) {
            // The WAL has not been touched. The visible but not yet
            // directory-synced intent must be removed durably before another
            // operation can append to this WAL.
            try {
                std::error_code remove_ec;
                const bool removed =
                    std::filesystem::remove(intent_path, remove_ec);
                if (remove_ec || !removed) {
                    throw std::runtime_error(
                        "failed to remove ambiguously published rollback intent");
                }
                SyncDirectoryPath(intent_path.parent_path());
            } catch (const std::exception& cleanup_error) {
                PoisonDurability(
                    "rollback-intent publication failed and cleanup was not "
                    "durable: " +
                    std::string(cleanup_error.what()));
            }
        }
        throw;
    }
    PauseConditionalMutationForTests(
        ConditionalMutationPausePoint::kAfterRollbackIntentPublish);
    CrashAtFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_AFTER_INTENT_PUBLISH_ONCE");
    return intent_path;
}

bool ChunkStore::PublishConditionalCommitIntent(
    const std::filesystem::path& intent_path,
    std::uint64_t committed_size) {
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CONDITIONAL_BEFORE_COMMIT_PUBLISH_ONCE")) {
        throw std::runtime_error(
            "injected conditional failure before commit-intent publication");
    }
    CrashAtFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_COMMIT_PUBLISH_ONCE");
    bool replaced = false;
    try {
        AtomicWrite(
            intent_path,
            SerializeConditionalIntent(
                ConditionalIntentState::kCommitted, committed_size),
            /*fsync_file=*/true,
            /*fsync_directory=*/true,
            &replaced,
            "CHUNKDB_FAILPOINT_COMMIT_INTENT_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
    } catch (...) {
        if (!replaced) {
            throw;
        }
        // The committed state is visible. Complete the known missing
        // directory durability step without repeating the replace.
        try {
            if (ConsumeFailpointEnv(
                    "CHUNKDB_FAILPOINT_COMMIT_INTENT_COMPLETION_SYNC_FAIL_ONCE")) {
                throw std::runtime_error(
                    "injected commit-intent completion sync failure");
            }
            SyncDirectoryPath(intent_path.parent_path());
        } catch (const std::exception& sync_error) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kRecovery,
                "commit intent is visible but requires a later directory sync",
                {
                    {"path", intent_path.string()},
                    {"error", sync_error.what()},
                });
            return false;
        }
    }
    PauseConditionalMutationForTests(
        ConditionalMutationPausePoint::kAfterCommitIntentPublish);
    CrashAtFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_AFTER_COMMIT_PUBLISH_ONCE");
    return true;
}

void ChunkStore::ClearCommittedConditionalIntent(
    const std::filesystem::path& intent_path) {
    CrashAtFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_INTENT_CLEAR_ONCE");
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CONDITIONAL_INTENT_UNLINK_FAIL_ONCE")) {
        throw std::runtime_error(
            "injected conditional commit-intent unlink failure: " +
            intent_path.string());
    }
    std::error_code remove_ec;
    const bool removed = std::filesystem::remove(intent_path, remove_ec);
    if (remove_ec) {
        throw std::runtime_error(
            "failed to clear conditional rollback intent " +
            intent_path.string() + ": " + remove_ec.message());
    }
    if (!removed) {
        throw std::runtime_error(
            "conditional committed intent disappeared before cleanup: " +
            intent_path.string());
    }
    PauseConditionalMutationForTests(
        ConditionalMutationPausePoint::kAfterCommitIntentUnlink);
    if (ConsumeFailpointEnv(
            "CHUNKDB_FAILPOINT_CONDITIONAL_INTENT_AFTER_UNLINK_BEFORE_DIR_SYNC_ONCE")) {
        throw std::runtime_error(
            "injected conditional intent cleanup failure after unlink before "
            "directory sync: " +
            intent_path.string());
    }
    CrashAtFailpoint(
        "CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_AFTER_INTENT_UNLINK_ONCE");
    SyncDirectoryPath(intent_path.parent_path());
}

void ChunkStore::PoisonDurability(std::string reason) noexcept {
    try {
        std::lock_guard lock(poison_mutex_);
        if (poison_reason_.empty()) {
            poison_reason_ = std::move(reason);
        }
    } catch (...) {
        // Setting the reason string is best-effort; the atomic flag below is
        // the authoritative fail-closed signal.
    }
    durability_poisoned_.store(true, std::memory_order_release);
}

void ChunkStore::ThrowIfDurabilityPoisoned() const {
    if (!durability_poisoned_.load(std::memory_order_acquire)) {
        return;
    }
    std::string reason;
    {
        std::lock_guard lock(poison_mutex_);
        reason = poison_reason_;
    }
    throw std::runtime_error(
        "store is fail-closed after an unrecoverable durability error: " +
        (reason.empty() ? std::string("durability rollback could not be completed")
                        : reason) +
        "; restart the store to run crash recovery");
}

}  // namespace chunkdb
