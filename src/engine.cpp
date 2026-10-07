#include "chunkdb/engine.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <thread>
#include <vector>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/zrle.hpp"
#include "store_manifest.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#endif

namespace chunkdb {

namespace {

// Hard bound on distinct tracked auth-failure sources. When the table is
// full, the least-recently-updated entry is evicted so an address spray
// cannot grow memory without bound.
constexpr std::size_t kMaxTrackedAuthFailureSources = 4096;

// IPv6 sources are bucketed by their /64 prefix: interface identifiers are
// attacker-controlled within one allocation, so per-address tracking would
// let a single /64 create billions of distinct entries. IPv4 and unparsable
// addresses are tracked exactly.
[[nodiscard]] std::string AuthFailureKey(const std::string& remote_address) {
    if (remote_address.find(':') == std::string::npos) {
        return remote_address;
    }
    in6_addr address{};
    if (inet_pton(AF_INET6, remote_address.c_str(), &address) != 1) {
        return remote_address;
    }
    // A dual-stack listener reports IPv4 peers as v4-mapped IPv6
    // ("::ffff:a.b.c.d"). Those addresses only differ in their low 4 bytes, so
    // /64-masking would collapse EVERY IPv4 client into one bucket — banning
    // them together and disabling per-source tracking. Track the embedded
    // IPv4 address exactly instead. (The deprecated v4-compatible form
    // "::a.b.c.d" is intentionally not special-cased: it does not occur from a
    // real dual-stack peer, and some platform macros misclassify low IPv6
    // addresses such as ::1 as v4-compatible.)
    const auto* bytes = reinterpret_cast<const std::uint8_t*>(&address);
    if (IN6_IS_ADDR_V4MAPPED(&address)) {
        in_addr v4{};
        std::memcpy(&v4, bytes + 12, 4);
        char v4_text[INET_ADDRSTRLEN] = {};
        if (inet_ntop(AF_INET, &v4, v4_text, sizeof(v4_text)) == nullptr) {
            return remote_address;
        }
        return v4_text;
    }
    std::memset(reinterpret_cast<std::uint8_t*>(&address) + 8, 0, 8);
    char prefix_text[INET6_ADDRSTRLEN] = {};
    if (inet_ntop(AF_INET6, &address, prefix_text, sizeof(prefix_text)) == nullptr) {
        return remote_address;
    }
    return std::string(prefix_text) + "/64";
}

[[nodiscard]] std::string_view ExtractCommandName(std::string_view line) noexcept {
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
        line.remove_suffix(1);
    }
    std::size_t i = 0;
    while (i < line.size() && line[i] == ' ') ++i;
    const std::size_t start = i;
    while (i < line.size() && line[i] != ' ') ++i;
    return line.substr(start, i - start);
}

[[nodiscard]] std::vector<std::string_view> ParseVarTokens(std::string_view line) {
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
        line.remove_suffix(1);
    }
    std::vector<std::string_view> tokens;
    std::size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && line[i] == ' ') ++i;
        if (i >= line.size()) break;
        const std::size_t start = i;
        while (i < line.size() && line[i] != ' ') ++i;
        tokens.push_back(line.substr(start, i - start));
    }
    return tokens;
}

[[nodiscard]] bool ConstantTimeEqual(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size()) {
        return false;
    }
    volatile unsigned char diff = 0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        diff |= static_cast<unsigned char>(a[i]) ^ static_cast<unsigned char>(b[i]);
    }
    return diff == 0;
}

[[nodiscard]] bool IsStateMode(std::string_view token) noexcept {
    return Protocol::CommandEquals(token, "STATE");
}

void SplitChunkStateArgument(
    std::string_view state,
    std::string_view* payload_bits,
    std::string_view* presence_bits) {
    if (payload_bits == nullptr || presence_bits == nullptr) {
        throw std::invalid_argument("chunk state outputs must not be null");
    }

    const std::size_t separator = state.find('|');
    if (separator == std::string_view::npos || separator != state.rfind('|')) {
        throw std::invalid_argument(
            "CHUNKSET STATE requires <payload_bits>|<presence_bits>");
    }

    *payload_bits = state.substr(0, separator);
    *presence_bits = state.substr(separator + 1);
}

// Case-insensitive match of a key/value key (TABLECREATE, TABLESET).
[[nodiscard]] bool KeyIs(std::string_view actual, std::string_view key) noexcept {
    return actual.size() == key.size() &&
           std::equal(actual.begin(), actual.end(), key.begin(), [](char lhs, char rhs) {
               return std::tolower(static_cast<unsigned char>(lhs)) ==
                      std::tolower(static_cast<unsigned char>(rhs));
           });
}

[[nodiscard]] bool IsGeometryKey(std::string_view key) noexcept {
    return KeyIs(key, "block_bits") ||
           KeyIs(key, "chunk_width_blocks") ||
           KeyIs(key, "chunk_height_blocks") ||
           KeyIs(key, "large_chunk_width_chunks") ||
           KeyIs(key, "large_chunk_height_chunks");
}

[[nodiscard]] std::uint32_t ParseGeometryValue(std::string_view key, std::string_view value) {
    std::uint32_t parsed = 0;
    const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed, 10);
    if (value.empty() || result.ec != std::errc() || result.ptr != value.data() + value.size() ||
        parsed == 0U) {
        throw std::invalid_argument(
            std::string(key) + " must be a positive 32-bit integer, got '" + std::string(value) +
            "'");
    }
    return parsed;
}

[[nodiscard]] std::size_t ParsePositiveSize(std::string_view key, std::string_view value) {
    std::uint64_t parsed = 0;
    const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed, 10);
    if (value.empty() || result.ec != std::errc() || result.ptr != value.data() + value.size() ||
        parsed == 0U || parsed > std::numeric_limits<std::size_t>::max()) {
        throw std::invalid_argument(
            std::string(key) + " must be a positive integer, got '" + std::string(value) + "'");
    }
    return static_cast<std::size_t>(parsed);
}

// Sets the option named `key` (as TABLEINFO prints it) from `value`.
void ApplyTableOption(TableOptionsUpdate* update, std::string_view key, std::string_view value) {
    if (KeyIs(key, "durability_mode")) {
        update->durability_mode = ParseDurabilityMode(value);
    } else if (KeyIs(key, "checkpoint_updates")) {
        update->checkpoint_update_interval = ParsePositiveSize(key, value);
    } else if (KeyIs(key, "checkpoint_wal_bytes")) {
        update->checkpoint_wal_bytes = ParsePositiveSize(key, value);
    } else if (KeyIs(key, "wal_group_commit_updates")) {
        update->wal_group_commit_updates = ParsePositiveSize(key, value);
    } else if (KeyIs(key, "checkpoint_compression")) {
        update->checkpoint_compression = ParseCheckpointCompression(value);
    } else if (IsGeometryKey(key)) {
        throw std::invalid_argument(
            std::string(key) + " is part of the geometry, which is fixed when a table is "
            "created");
    } else {
        throw std::invalid_argument("unknown table option '" + std::string(key) + "'");
    }
}

// The key=value lines TABLEINFO and USE reply with.
[[nodiscard]] std::string RenderTableInfo(const TableInfo& info) {
    std::string out;
    out += "table=" + info.name + "\n";
    out += "store_id=" + StoreIdHex(info.store_id) + "\n";
    out += "block_bits=" + std::to_string(info.geometry.block_bits) + "\n";
    out += "chunk_width_blocks=" + std::to_string(info.geometry.chunk_width_blocks) + "\n";
    out += "chunk_height_blocks=" + std::to_string(info.geometry.chunk_height_blocks) + "\n";
    out += "large_chunk_width_chunks=" +
           std::to_string(info.geometry.large_chunk_width_chunks) + "\n";
    out += "large_chunk_height_chunks=" +
           std::to_string(info.geometry.large_chunk_height_chunks) + "\n";
    out += "durability_mode=" + std::string(DurabilityModeName(info.options.durability_mode)) +
           "\n";
    out += "checkpoint_updates=" + std::to_string(info.options.checkpoint_update_interval) + "\n";
    out += "checkpoint_wal_bytes=" + std::to_string(info.options.checkpoint_wal_bytes) + "\n";
    out += "wal_group_commit_updates=" + std::to_string(info.options.wal_group_commit_updates) +
           "\n";
    out += "checkpoint_compression=" +
           std::string(CheckpointCompressionName(info.options.checkpoint_compression)) + "\n";
    return out;
}

void AddRuntimeStats(StoreRuntimeStats* total, const StoreRuntimeStats& add) {
    total->evictions += add.evictions;
    total->checkpoints += add.checkpoints;
    total->wal_batch_flushes += add.wal_batch_flushes;
    total->unique_loaded_chunks += add.unique_loaded_chunks;
    total->open_wal_streams += add.open_wal_streams;
    total->eviction_snapshot_builds += add.eviction_snapshot_builds;
    total->eviction_probes += add.eviction_probes;
    total->eviction_no_progress_cycles += add.eviction_no_progress_cycles;
    total->eviction_forced_wal_flushes += add.eviction_forced_wal_flushes;
    total->eviction_forced_wal_flushes_with_data += add.eviction_forced_wal_flushes_with_data;
    total->eviction_forced_wal_flushes_empty_batch += add.eviction_forced_wal_flushes_empty_batch;
    total->eviction_recency_skips += add.eviction_recency_skips;
    total->empty_chunk_gcs += add.empty_chunk_gcs;
    total->wal_barriers += add.wal_barriers;
    total->wal_barrier_full_syncs += add.wal_barrier_full_syncs;
    total->background_checkpoints += add.background_checkpoints;
    total->background_checkpoint_failures += add.background_checkpoint_failures;
    total->background_queue_full_inline += add.background_queue_full_inline;
    total->background_queue_depth += add.background_queue_depth;
    total->compressed_checkpoint_images += add.compressed_checkpoint_images;
}

[[nodiscard]] std::size_t PresenceBytes(const Geometry& geometry) noexcept {
    return (geometry.ChunkBlockCount() + 7U) / 8U;
}

struct MSetItem {
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::string_view bits;
};

}  // namespace

CommandEngine::CommandEngine(
    EngineConfig config,
    std::shared_ptr<TableCatalog> catalog,
    std::shared_ptr<MetricsRegistry> metrics)
    : config_(std::move(config)),
      catalog_(std::move(catalog)),
      metrics_(metrics != nullptr ? std::move(metrics) : std::make_shared<MetricsRegistry>()) {
    if (!catalog_) {
        throw std::invalid_argument("catalog must not be null");
    }
    if (config_.require_auth && config_.auth_token.empty()) {
        throw std::invalid_argument("auth_token must be set when require_auth=true");
    }
    if (config_.max_auth_failures == 0) {
        throw std::invalid_argument("max_auth_failures must be > 0");
    }
}

const std::shared_ptr<Table>& CommandEngine::SelectedTable(SessionState& session) const {
    if (session.table == nullptr) {
        session.table = catalog_->Find(kDefaultTableName);
    }
    return session.table;
}

Table::Lease CommandEngine::AcquireTable(SessionState& session) const {
    const auto& table = SelectedTable(session);
    if (table == nullptr) {
        throw TableNotFoundError(
            "no table selected and table 'default' does not exist; select one with USE <name>");
    }
    auto lease = table->Acquire();
    if (!lease.has_value()) {
        throw TableNotFoundError(
            "table '" + table->name() + "' was dropped; select a table with USE <name>");
    }
    return std::move(*lease);
}

std::size_t CommandEngine::ParsePayloadLength(std::string_view token) {
    std::uint64_t value = 0;
    const auto* begin = token.data();
    const auto* end = token.data() + token.size();
    const auto result = std::from_chars(begin, end, value, 10);
    if (token.empty() || result.ec != std::errc() || result.ptr != end) {
        throw std::invalid_argument("payload length must be a non-negative integer");
    }
    return static_cast<std::size_t>(value);
}

CommandEngine::PayloadRequest CommandEngine::PlanPayload(
    SessionState& session,
    std::string_view line) const {
    PayloadRequest request;
    if (!Protocol::CommandEquals(ExtractCommandName(line), "CHUNKSETBIN")) {
        return request;
    }
    if (IsAuthRequired() && !session.authenticated) {
        request.plan = PayloadPlan::kReject;
        request.reject_response = Protocol::Error("AUTH_REQUIRED", "use AUTH <token>");
        return request;
    }
    try {
        const ParsedCommandView command = Protocol::ParseLineView(line);
        if (command.argc != 3 && command.argc != 4) {
            throw std::invalid_argument(
                "CHUNKSETBIN requires <cx> <cy> [STATE] <payload_length>");
        }
        request.bytes = ParsePayloadLength(command.args[command.argc - 1]);
    } catch (const std::invalid_argument& e) {
        request.plan = PayloadPlan::kReject;
        request.reject_response = Protocol::Error("INVALID_ARGUMENT", e.what());
        return request;
    }
    // The largest payload any CHUNKSETBIN form can legitimately carry is the
    // full chunk state for the selected table's geometry. Anything larger
    // cannot be a mistake worth draining, so refuse before buffering it. A
    // dropped table still has its geometry: the payload is read and the
    // command then fails with NO_TABLE.
    const auto& table = SelectedTable(session);
    if (table == nullptr) {
        request.plan = PayloadPlan::kReject;
        request.reject_response = Protocol::Error(
            "NO_TABLE",
            "no table selected and table 'default' does not exist; select one with USE <name>");
        return request;
    }
    const Geometry geometry(table->geometry());
    const std::size_t max_bytes = geometry.ChunkPayloadBytes() + PresenceBytes(geometry);
    if (request.bytes > max_bytes) {
        request.plan = PayloadPlan::kReject;
        request.reject_response = Protocol::Error(
            "BAD_REQUEST",
            "payload length exceeds chunk state size (" + std::to_string(max_bytes) + ")");
        return request;
    }
    request.plan = PayloadPlan::kRead;
    return request;
}

std::string CommandEngine::Execute(
    SessionState& session,
    std::string_view line,
    std::string_view payload) {
    const auto command_name = ExtractCommandName(line);
    const auto started = std::chrono::steady_clock::now();
    std::string response = ExecuteInternal(session, line, command_name, payload);
    const auto elapsed = std::chrono::steady_clock::now() - started;

    const bool ok = response.empty() || response[0] != '-';
    metrics_->ObserveCommand(
        MetricsRegistry::ClassifyCommand(command_name),
        std::chrono::duration<double>(elapsed).count(),
        ok);
    if (!ok) {
        // Error frames look like "-ERR <CODE> <message>\r\n".
        std::string_view code = response;
        constexpr std::string_view kErrPrefix = "-ERR ";
        if (code.rfind(kErrPrefix, 0) == 0) {
            code.remove_prefix(kErrPrefix.size());
            const auto code_end = code.find_first_of(" \r\n");
            if (code_end != std::string_view::npos) {
                code = code.substr(0, code_end);
            }
        } else {
            code = {};
        }
        metrics_->CountError(MetricsRegistry::ClassifyErrorCode(code));
        if (code == "AUTH_FAILED") {
            metrics_->IncAuthFailure();
        }
    }
    return response;
}

std::string CommandEngine::ExecuteInternal(
    SessionState& session,
    std::string_view line,
    std::string_view command_name,
    std::string_view payload) {
    try {
        // MSET/MGET/CHUNKBATCH and the table commands that take options
        // accept variable numbers of arguments that exceed ParseLineView's
        // 8-arg limit, so intercept them before calling ParseLineView.
        const auto& name = command_name;
        if (Protocol::CommandEquals(name, "MSET") || Protocol::CommandEquals(name, "MGET") ||
            Protocol::CommandEquals(name, "CHUNKBATCH") ||
            Protocol::CommandEquals(name, "TABLECREATE") ||
            Protocol::CommandEquals(name, "TABLESET")) {
            if (IsAuthRequired() && !session.authenticated) {
                return Protocol::Error("AUTH_REQUIRED", "use AUTH <token>");
            }
            if (Protocol::CommandEquals(name, "TABLECREATE")) {
                return HandleTableCreate(line);
            }
            if (Protocol::CommandEquals(name, "TABLESET")) {
                return HandleTableSet(line);
            }
            const auto lease = AcquireTable(session);
            if (Protocol::CommandEquals(name, "MSET")) {
                return HandleMSet(lease.store(), line);
            }
            if (Protocol::CommandEquals(name, "CHUNKBATCH")) {
                return HandleChunkBatch(lease.store(), line);
            }
            return HandleMGet(lease.store(), line);
        }

        const ParsedCommandView command = Protocol::ParseLineView(line);

        if (Protocol::CommandEquals(command.name, "PING")) {
            return Protocol::SimpleString("PONG");
        }

        if (Protocol::CommandEquals(command.name, "AUTH")) {
            return HandleAuth(session, command);
        }

        if (Protocol::CommandEquals(command.name, "QUIT")) {
            session.close_after_reply = true;
            return Protocol::SimpleString("BYE");
        }

        if (IsAuthRequired() && !session.authenticated) {
            return Protocol::Error("AUTH_REQUIRED", "use AUTH <token>");
        }

        // Commands that are not about the selected table.
        if (Protocol::CommandEquals(command.name, "USE")) {
            return HandleUse(session, command);
        }
        if (Protocol::CommandEquals(command.name, "TABLES")) {
            return HandleTables(command);
        }
        if (Protocol::CommandEquals(command.name, "TABLEINFO")) {
            return HandleTableInfo(command);
        }
        if (Protocol::CommandEquals(command.name, "TABLEDROP")) {
            return HandleTableDrop(command);
        }
        if (Protocol::CommandEquals(command.name, "WALFLUSH")) {
            return HandleWalFlush(command);
        }
        if (Protocol::CommandEquals(command.name, "METRICS")) {
            return HandleMetrics();
        }

        static constexpr std::array<std::string_view, 16> kTableCommands = {
            "GET", "EXISTS", "SET", "UNSET", "CHUNKEXISTS", "CHUNK", "CHUNKSET",
            "CHUNKSETBIN", "CHUNKBIN", "CHUNKBINC", "INFO", "CHUNKSCAN", "CHUNKRANGE",
            "CHUNKRADIUS", "CHUNKVER", "CHUNKCAS"};
        const bool table_command = std::any_of(
            kTableCommands.begin(), kTableCommands.end(),
            [&](std::string_view known) { return Protocol::CommandEquals(command.name, known); });
        if (!table_command) {
            return Protocol::Error("UNKNOWN_COMMAND", command.name);
        }

        const auto lease = AcquireTable(session);
        ChunkStore& store = lease.store();
        if (Protocol::CommandEquals(command.name, "GET")) {
            return HandleGet(store, command);
        }
        if (Protocol::CommandEquals(command.name, "EXISTS")) {
            return HandleExists(store, command);
        }
        if (Protocol::CommandEquals(command.name, "SET")) {
            return HandleSet(store, command);
        }
        if (Protocol::CommandEquals(command.name, "UNSET")) {
            return HandleUnset(store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKEXISTS")) {
            return HandleChunkExists(store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNK")) {
            return HandleChunk(store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKSET")) {
            return HandleChunkSet(store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKSETBIN")) {
            return HandleChunkSetBinary(store, command, payload);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKBIN")) {
            return HandleChunkBinary(store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKBINC")) {
            return HandleChunkBinaryCompressed(store, command);
        }
        if (Protocol::CommandEquals(command.name, "INFO")) {
            return HandleInfo(*session.table, store);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKSCAN")) {
            return HandleChunkScan(store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKRANGE")) {
            return HandleChunkRange(store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKRADIUS")) {
            return HandleChunkRadius(store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKVER")) {
            return HandleChunkVersion(store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKCAS")) {
            return HandleChunkCas(store, command);
        }
        return Protocol::Error("UNKNOWN_COMMAND", command.name);
    } catch (const TableNotFoundError& e) {
        return Protocol::Error("NO_TABLE", e.what());
    } catch (const TableExistsError& e) {
        return Protocol::Error("TABLE_EXISTS", e.what());
    } catch (const std::invalid_argument& e) {
        return Protocol::Error("INVALID_ARGUMENT", e.what());
    } catch (const std::out_of_range& e) {
        return Protocol::Error("OUT_OF_RANGE", e.what());
    } catch (const std::exception& e) {
        LogMessage(
            LogLevel::kError,
            LogComponent::kStore,
            "command execution error",
            {{"error", e.what()}});
        return Protocol::Error("INTERNAL", "internal error");
    }
}

std::string CommandEngine::HandleAuth(SessionState& session, const ParsedCommandView& command) {
    if (command.argc != 1) {
        throw std::invalid_argument("AUTH requires exactly 1 argument");
    }

    if (!IsAuthRequired()) {
        session.authenticated = true;
        session.failed_auth_attempts = 0;
        return Protocol::SimpleString("OK");
    }

    const bool track_remote_ip =
        !session.remote_address.empty() && config_.max_auth_failures_per_ip > 0;
    const std::string failure_key =
        track_remote_ip ? AuthFailureKey(session.remote_address) : std::string();
    const auto now = std::chrono::steady_clock::now();
    std::chrono::milliseconds auth_failure_delay{0};
    bool temporarily_banned = false;

    if (track_remote_ip) {
        std::lock_guard lock(auth_failures_mutex_);
        const auto it = auth_failures_by_ip_.find(failure_key);
        if (it != auth_failures_by_ip_.end() && it->second.banned_until > now) {
            temporarily_banned = true;
            session.close_after_reply = true;
            if (config_.auth_failure_delay_ms > 0) {
                auth_failure_delay = std::chrono::milliseconds(config_.auth_failure_delay_ms);
            }
        }
    }

    if (temporarily_banned) {
        if (auth_failure_delay.count() > 0) {
            std::this_thread::sleep_for(auth_failure_delay);
        }
        return Protocol::Error("AUTH_FAILED", "temporary auth ban");
    }

    if (ConstantTimeEqual(command.args[0], config_.auth_token)) {
        session.authenticated = true;
        session.failed_auth_attempts = 0;
        if (track_remote_ip) {
            std::lock_guard lock(auth_failures_mutex_);
            auth_failures_by_ip_.erase(failure_key);
        }
        return Protocol::SimpleString("OK");
    }

    ++session.failed_auth_attempts;
    if (session.failed_auth_attempts >= config_.max_auth_failures) {
        session.close_after_reply = true;
    }
    if (track_remote_ip) {
        std::lock_guard lock(auth_failures_mutex_);
        auto it = auth_failures_by_ip_.find(failure_key);
        if (it == auth_failures_by_ip_.end()) {
            if (auth_failures_by_ip_.size() >= kMaxTrackedAuthFailureSources) {
                // Evict the least-recently-updated entry so the table stays
                // hard-bounded under an address spray — but never evict an
                // entry with an active ban, or an attacker could lift their
                // own ban by spraying fresh sources until the aged banned
                // entry is chosen. Only fall back to a banned victim if every
                // tracked entry is currently banned.
                auto victim = auth_failures_by_ip_.end();
                auto oldest_banned = auth_failures_by_ip_.end();
                for (auto candidate = auth_failures_by_ip_.begin();
                     candidate != auth_failures_by_ip_.end();
                     ++candidate) {
                    const bool banned = candidate->second.banned_until > now;
                    if (banned) {
                        if (oldest_banned == auth_failures_by_ip_.end() ||
                            candidate->second.last_update <
                                oldest_banned->second.last_update) {
                            oldest_banned = candidate;
                        }
                        continue;
                    }
                    if (victim == auth_failures_by_ip_.end() ||
                        candidate->second.last_update < victim->second.last_update) {
                        victim = candidate;
                    }
                }
                if (victim == auth_failures_by_ip_.end()) {
                    victim = oldest_banned;
                }
                auth_failures_by_ip_.erase(victim);
            }
            it = auth_failures_by_ip_.try_emplace(failure_key).first;
        }
        auto& ip_state = it->second;
        ip_state.last_update = now;
        ++ip_state.failures;
        if (ip_state.failures >= config_.max_auth_failures_per_ip) {
            if (config_.auth_failure_ban_ms > 0) {
                ip_state.banned_until = now + std::chrono::milliseconds(config_.auth_failure_ban_ms);
            }
            if (config_.auth_failure_delay_ms > 0) {
                auth_failure_delay = std::chrono::milliseconds(config_.auth_failure_delay_ms);
            }
        }
    }
    if (auth_failure_delay.count() > 0) {
        std::this_thread::sleep_for(auth_failure_delay);
    }
    return Protocol::Error("AUTH_FAILED", "invalid token");
}

std::size_t CommandEngine::AuthFailureTrackedSourcesForTests() {
    std::lock_guard lock(auth_failures_mutex_);
    return auth_failures_by_ip_.size();
}

std::string CommandEngine::HandleGet(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("GET requires 2 arguments: GET <x> <y>");
    }

    const std::int64_t x = ParseInt64(command.args[0]);
    const std::int64_t y = ParseInt64(command.args[1]);

    const std::string bits = store.GetBlockBits(x, y);
    return Protocol::Bulk(bits);
}

std::string CommandEngine::HandleExists(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("EXISTS requires 2 arguments: EXISTS <x> <y>");
    }

    const std::int64_t x = ParseInt64(command.args[0]);
    const std::int64_t y = ParseInt64(command.args[1]);

    return Protocol::SimpleString(store.BlockExists(x, y) ? "1" : "0");
}

std::string CommandEngine::HandleSet(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 3) {
        throw std::invalid_argument("SET requires 3 arguments: SET <x> <y> <bits>");
    }

    const std::int64_t x = ParseInt64(command.args[0]);
    const std::int64_t y = ParseInt64(command.args[1]);

    store.SetBlockBits(x, y, command.args[2]);
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleUnset(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("UNSET requires 2 arguments: UNSET <x> <y>");
    }

    const std::int64_t x = ParseInt64(command.args[0]);
    const std::int64_t y = ParseInt64(command.args[1]);

    store.UnsetBlock(x, y);
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleChunk(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 2 && command.argc != 3) {
        throw std::invalid_argument("CHUNK requires 2 arguments or CHUNK <cx> <cy> STATE");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);

    if (command.argc == 3 && !IsStateMode(command.args[2])) {
        throw std::invalid_argument("CHUNK mode must be STATE when provided");
    }

    const std::string bits = command.argc == 3
                                 ? store.GetChunkStateBits(chunk_x, chunk_y)
                                 : store.GetChunkBits(chunk_x, chunk_y);
    return Protocol::Bulk(bits);
}

std::string CommandEngine::HandleChunkExists(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("CHUNKEXISTS requires 2 arguments: CHUNKEXISTS <cx> <cy>");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);

    return Protocol::SimpleString(store.ChunkExists(chunk_x, chunk_y) ? "1" : "0");
}

std::string CommandEngine::HandleChunkSet(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 3 && command.argc != 4) {
        throw std::invalid_argument(
            "CHUNKSET requires 3 arguments or CHUNKSET <cx> <cy> STATE <payload_bits>|<presence_bits>");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);

    if (command.argc == 4) {
        if (!IsStateMode(command.args[2])) {
            throw std::invalid_argument("CHUNKSET mode must be STATE when provided");
        }

        std::string_view payload_bits;
        std::string_view presence_bits;
        SplitChunkStateArgument(command.args[3], &payload_bits, &presence_bits);
        store.SetChunkStateBits(chunk_x, chunk_y, payload_bits, presence_bits);
    } else {
        store.SetChunkBits(chunk_x, chunk_y, command.args[2]);
    }
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleChunkSetBinary(
    ChunkStore& store,
    const ParsedCommandView& command,
    std::string_view payload) {
    if (command.argc != 3 && command.argc != 4) {
        throw std::invalid_argument("CHUNKSETBIN requires <cx> <cy> [STATE] <payload_length>");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);
    const bool state_mode = command.argc == 4;
    if (state_mode && !IsStateMode(command.args[2])) {
        throw std::invalid_argument("CHUNKSETBIN mode must be STATE when provided");
    }
    const std::size_t declared = ParsePayloadLength(command.args[command.argc - 1]);
    if (declared != payload.size()) {
        // The connection reads exactly the declared length, so this only
        // trips for callers that bypass the wire path.
        throw std::invalid_argument("payload length does not match the bytes received");
    }

    const std::size_t payload_bytes = store.geometry().ChunkPayloadBytes();
    const std::size_t expected = state_mode ? payload_bytes + PresenceBytes(store.geometry()) : payload_bytes;
    if (payload.size() != expected) {
        throw std::invalid_argument(
            "payload length " + std::to_string(payload.size()) + " does not match expected " +
            std::to_string(expected) + " bytes for " + (state_mode ? "CHUNKSETBIN STATE" : "CHUNKSETBIN"));
    }

    const auto* bytes = reinterpret_cast<const std::uint8_t*>(payload.data());
    const std::vector<std::uint8_t> packed_payload(bytes, bytes + payload_bytes);
    if (state_mode) {
        const std::vector<std::uint8_t> presence(bytes + payload_bytes, bytes + payload.size());
        store.SetChunkStateBytes(chunk_x, chunk_y, packed_payload, presence);
    } else {
        store.SetChunkPayloadBytes(chunk_x, chunk_y, packed_payload);
    }
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleChunkBinary(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 2 && command.argc != 3) {
        throw std::invalid_argument("CHUNKBIN requires 2 arguments or CHUNKBIN <cx> <cy> STATE");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);

    if (command.argc == 3 && !IsStateMode(command.args[2])) {
        throw std::invalid_argument("CHUNKBIN mode must be STATE when provided");
    }

    const auto payload = command.argc == 3
                             ? store.GetChunkStateBytes(chunk_x, chunk_y)
                             : store.GetChunkPayloadBytes(chunk_x, chunk_y);
    return Protocol::BulkBytes(payload);
}

std::string CommandEngine::HandleChunkBinaryCompressed(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 2 && command.argc != 3) {
        throw std::invalid_argument("CHUNKBINC requires 2 arguments or CHUNKBINC <cx> <cy> STATE");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);

    if (command.argc == 3 && !IsStateMode(command.args[2])) {
        throw std::invalid_argument("CHUNKBINC mode must be STATE when provided");
    }

    const auto payload = command.argc == 3
                             ? store.GetChunkStateBytes(chunk_x, chunk_y)
                             : store.GetChunkPayloadBytes(chunk_x, chunk_y);
    return Protocol::BulkBytes(ZrleCompress(payload));
}

std::string CommandEngine::HandleInfo(const Table& table, ChunkStore& store) const {
    const auto runtime_stats = store.RuntimeStats();
    std::string info;
    info += "chunkdb_version=1\n";
    info += "table=" + table.name() + "\n";
    info += "tables=" + std::to_string(catalog_->TableCount()) + "\n";
    const auto& cfg = store.geometry().config();
    info += "block_bits=" + std::to_string(cfg.block_bits) + "\n";
    info += "chunk_width_blocks=" + std::to_string(cfg.chunk_width_blocks) + "\n";
    info += "chunk_height_blocks=" + std::to_string(cfg.chunk_height_blocks) + "\n";
    info += "large_chunk_width_chunks=" + std::to_string(cfg.large_chunk_width_chunks) + "\n";
    info += "large_chunk_height_chunks=" + std::to_string(cfg.large_chunk_height_chunks) + "\n";
    info += "durability_mode=" + std::string(DurabilityModeName(store.durability_mode())) + "\n";
    info += "access_mode=" + std::string(AccessModeName(store.access_mode())) + "\n";
    info += "chunk_lock_mode=" + std::string(ChunkLockModeName()) + "\n";
    info +=
        "checkpoint_compression=" +
        std::string(CheckpointCompressionName(store.checkpoint_compression())) + "\n";
    info += "loaded_chunks=" + std::to_string(store.ApproxLoadedChunkCount()) + "\n";
    info += "evictions=" + std::to_string(runtime_stats.evictions) + "\n";
    info += "checkpoints=" + std::to_string(runtime_stats.checkpoints) + "\n";
    info += "wal_batch_flushes=" + std::to_string(runtime_stats.wal_batch_flushes) + "\n";
    info += "unique_loaded_chunks=" + std::to_string(runtime_stats.unique_loaded_chunks) + "\n";
    info += "open_wal_streams=" + std::to_string(runtime_stats.open_wal_streams) + "\n";
    info += "eviction_snapshot_builds=" + std::to_string(runtime_stats.eviction_snapshot_builds) + "\n";
    info += "eviction_probes=" + std::to_string(runtime_stats.eviction_probes) + "\n";
    info += "eviction_no_progress_cycles=" + std::to_string(runtime_stats.eviction_no_progress_cycles) + "\n";
    info += "eviction_forced_wal_flushes=" + std::to_string(runtime_stats.eviction_forced_wal_flushes) + "\n";
    info +=
        "eviction_forced_wal_flushes_with_data=" +
        std::to_string(runtime_stats.eviction_forced_wal_flushes_with_data) + "\n";
    info +=
        "eviction_forced_wal_flushes_empty_batch=" +
        std::to_string(runtime_stats.eviction_forced_wal_flushes_empty_batch) + "\n";
    info += "eviction_recency_skips=" + std::to_string(runtime_stats.eviction_recency_skips) + "\n";
    info += "empty_chunk_gcs=" + std::to_string(runtime_stats.empty_chunk_gcs) + "\n";
    info += "wal_barriers=" + std::to_string(runtime_stats.wal_barriers) + "\n";
    info += "wal_barrier_full_syncs=" + std::to_string(runtime_stats.wal_barrier_full_syncs) + "\n";
    info +=
        "compressed_checkpoint_images=" +
        std::to_string(runtime_stats.compressed_checkpoint_images) + "\n";
    info += "background_checkpoints=" + std::to_string(runtime_stats.background_checkpoints) + "\n";
    info +=
        "background_checkpoint_failures=" +
        std::to_string(runtime_stats.background_checkpoint_failures) + "\n";
    info +=
        "background_queue_full_inline=" +
        std::to_string(runtime_stats.background_queue_full_inline) + "\n";
    info += "background_queue_depth=" + std::to_string(runtime_stats.background_queue_depth) + "\n";
    return Protocol::Bulk(info);
}

std::string CommandEngine::HandleChunkScan(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 1 && command.argc != 3) {
        throw std::invalid_argument(
            "CHUNKSCAN requires CHUNKSCAN <limit> or CHUNKSCAN <limit> <cursor_cx> <cursor_cy>");
    }

    const std::uint64_t limit = ParseUint64(command.args[0]);
    const bool has_cursor = command.argc == 3;
    ChunkCoord cursor{};
    if (has_cursor) {
        cursor.x = ParseInt64(command.args[1]);
        cursor.y = ParseInt64(command.args[2]);
    }

    const auto page = store.ScanPopulatedChunks(
        has_cursor,
        cursor,
        static_cast<std::size_t>(limit));

    std::vector<std::string> items;
    items.reserve(page.coords.size() + 1);
    if (page.has_more && !page.coords.empty()) {
        const auto& last = page.coords.back();
        items.push_back("CURSOR " + std::to_string(last.x) + " " + std::to_string(last.y));
    } else {
        items.emplace_back("END");
    }
    for (const auto& coord : page.coords) {
        items.push_back(std::to_string(coord.x) + " " + std::to_string(coord.y));
    }
    return Protocol::Array(items);
}

std::string CommandEngine::HandleChunkRange(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 4) {
        throw std::invalid_argument(
            "CHUNKRANGE requires 4 arguments: CHUNKRANGE <cx0> <cy0> <cx1> <cy1>");
    }

    const auto entries = store.ReadChunkRange(
        ParseInt64(command.args[0]),
        ParseInt64(command.args[1]),
        ParseInt64(command.args[2]),
        ParseInt64(command.args[3]));

    std::vector<std::string> items;
    items.reserve(entries.size());
    for (const auto& entry : entries) {
        items.push_back(
            std::to_string(entry.coord.x) + " " + std::to_string(entry.coord.y) + " " +
            entry.payload_bits + "|" + entry.presence_bits);
    }
    return Protocol::Array(items);
}

std::string CommandEngine::HandleChunkRadius(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 3) {
        throw std::invalid_argument(
            "CHUNKRADIUS requires 3 arguments: CHUNKRADIUS <cx> <cy> <radius_chunks>");
    }

    const auto entries = store.ReadChunkRadius(
        ParseInt64(command.args[0]),
        ParseInt64(command.args[1]),
        ParseInt64(command.args[2]));

    std::vector<std::string> items;
    items.reserve(entries.size());
    for (const auto& entry : entries) {
        items.push_back(
            std::to_string(entry.coord.x) + " " + std::to_string(entry.coord.y) + " " +
            entry.payload_bits + "|" + entry.presence_bits);
    }
    return Protocol::Array(items);
}

std::string CommandEngine::HandleChunkVersion(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("CHUNKVER requires 2 arguments: CHUNKVER <cx> <cy>");
    }
    const std::uint64_t version =
        store.GetChunkVersion(ParseInt64(command.args[0]), ParseInt64(command.args[1]));
    return Protocol::Bulk(std::to_string(version));
}

std::string CommandEngine::HandleChunkCas(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 5 || !IsStateMode(command.args[3])) {
        throw std::invalid_argument(
            "CHUNKCAS requires CHUNKCAS <cx> <cy> <version> STATE <payload_bits>|<presence_bits>");
    }

    std::string_view payload_bits;
    std::string_view presence_bits;
    SplitChunkStateArgument(command.args[4], &payload_bits, &presence_bits);

    const auto result = store.CasChunkState(
        ParseInt64(command.args[0]),
        ParseInt64(command.args[1]),
        ParseUint64(command.args[2]),
        payload_bits,
        presence_bits);
    if (!result.ok) {
        return Protocol::Error("VERSION_MISMATCH", "current=" + std::to_string(result.version));
    }
    return Protocol::Bulk(std::to_string(result.version));
}

std::string CommandEngine::HandleChunkBatch(ChunkStore& store, std::string_view line) {
    const auto tokens = ParseVarTokens(line);
    if (tokens.size() < 5) {
        throw std::invalid_argument(
            "CHUNKBATCH requires CHUNKBATCH <cx> <cy> <version|-> then SET <x> <y> <bits> "
            "and/or UNSET <x> <y> operations");
    }

    const std::int64_t chunk_x = ParseInt64(tokens[1]);
    const std::int64_t chunk_y = ParseInt64(tokens[2]);
    const bool has_expected_version = tokens[3] != "-";
    const std::uint64_t expected_version =
        has_expected_version ? ParseUint64(tokens[3]) : std::uint64_t{0};

    std::vector<ChunkBatchOp> ops;
    std::size_t i = 4;
    while (i < tokens.size()) {
        if (ops.size() >= kMaxChunkBatchOps) {
            throw std::invalid_argument(
                "batch must contain at most " + std::to_string(kMaxChunkBatchOps) + " operations");
        }
        if (Protocol::CommandEquals(tokens[i], "SET")) {
            if (i + 3 >= tokens.size()) {
                throw std::invalid_argument("SET operation requires <x> <y> <bits>");
            }
            ops.push_back(ChunkBatchOp{
                .set = true,
                .x = ParseInt64(tokens[i + 1]),
                .y = ParseInt64(tokens[i + 2]),
                .bits = std::string(tokens[i + 3]),
            });
            i += 4;
        } else if (Protocol::CommandEquals(tokens[i], "UNSET")) {
            if (i + 2 >= tokens.size()) {
                throw std::invalid_argument("UNSET operation requires <x> <y>");
            }
            ops.push_back(ChunkBatchOp{
                .set = false,
                .x = ParseInt64(tokens[i + 1]),
                .y = ParseInt64(tokens[i + 2]),
                .bits = {},
            });
            i += 3;
        } else {
            throw std::invalid_argument(
                "batch operations must start with SET or UNSET, got: " + std::string(tokens[i]));
        }
    }

    const auto result = store.ApplyChunkBatch(
        chunk_x,
        chunk_y,
        has_expected_version,
        expected_version,
        ops);
    if (!result.ok) {
        return Protocol::Error("VERSION_MISMATCH", "current=" + std::to_string(result.version));
    }
    return Protocol::Bulk(std::to_string(result.version));
}

std::string CommandEngine::HandleWalFlush(const ParsedCommandView& command) {
    if (command.argc != 0) {
        throw std::invalid_argument("WALFLUSH takes no arguments");
    }
    // Every table: a connection may have written to several.
    catalog_->WalBarrier();
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleMetrics() const {
    // Store counters summed over all tables; the cache is shared by them.
    StoreRuntimeStats total;
    for (const auto& info : catalog_->List()) {
        const auto table = catalog_->Find(info.name);
        if (table == nullptr) {
            continue;
        }
        const auto lease = table->Acquire();
        if (!lease.has_value()) {
            continue;
        }
        AddRuntimeStats(&total, lease->store().RuntimeStats());
    }
    return Protocol::Bulk(metrics_->RenderPrometheus(
        total, static_cast<std::size_t>(catalog_->resources()->LoadedChunkCount())));
}

std::string CommandEngine::HandleTables(const ParsedCommandView& command) const {
    if (command.argc != 0) {
        throw std::invalid_argument("TABLES takes no arguments");
    }
    std::vector<std::string> names;
    for (const auto& info : catalog_->List()) {
        names.push_back(info.name);
    }
    return Protocol::Array(names);
}

std::string CommandEngine::HandleTableInfo(const ParsedCommandView& command) const {
    if (command.argc != 1) {
        throw std::invalid_argument("TABLEINFO requires 1 argument: TABLEINFO <name>");
    }
    const auto table = catalog_->Find(command.args[0]);
    if (table == nullptr) {
        throw TableNotFoundError("table '" + std::string(command.args[0]) + "' does not exist");
    }
    return Protocol::Bulk(RenderTableInfo(table->Info()));
}

std::string CommandEngine::HandleUse(SessionState& session, const ParsedCommandView& command) {
    if (command.argc != 1) {
        throw std::invalid_argument("USE requires 1 argument: USE <name>");
    }
    auto table = catalog_->Find(command.args[0]);
    if (table == nullptr) {
        // The connection keeps the table it had.
        throw TableNotFoundError("table '" + std::string(command.args[0]) + "' does not exist");
    }
    std::string reply = Protocol::Bulk(RenderTableInfo(table->Info()));
    session.table = std::move(table);
    return reply;
}

std::string CommandEngine::HandleTableCreate(std::string_view line) {
    const auto tokens = ParseVarTokens(line);
    if (tokens.size() < 2 || tokens.size() % 2 != 0) {
        throw std::invalid_argument(
            "TABLECREATE requires TABLECREATE <name> block_bits <n> [<key> <value> ...]");
    }
    GeometryConfig geometry;
    bool has_block_bits = false;
    TableOptionsUpdate options;
    std::vector<std::string_view> seen;
    for (std::size_t i = 2; i < tokens.size(); i += 2) {
        const auto key = tokens[i];
        const auto value = tokens[i + 1];
        for (const auto previous : seen) {
            if (KeyIs(previous, key)) {
                throw std::invalid_argument("'" + std::string(key) + "' is given twice");
            }
        }
        seen.push_back(key);
        if (KeyIs(key, "block_bits")) {
            geometry.block_bits = ParseGeometryValue(key, value);
            has_block_bits = true;
        } else if (KeyIs(key, "chunk_width_blocks")) {
            geometry.chunk_width_blocks = ParseGeometryValue(key, value);
        } else if (KeyIs(key, "chunk_height_blocks")) {
            geometry.chunk_height_blocks = ParseGeometryValue(key, value);
        } else if (KeyIs(key, "large_chunk_width_chunks")) {
            geometry.large_chunk_width_chunks = ParseGeometryValue(key, value);
        } else if (KeyIs(key, "large_chunk_height_chunks")) {
            geometry.large_chunk_height_chunks = ParseGeometryValue(key, value);
        } else {
            ApplyTableOption(&options, key, value);
        }
    }
    if (!has_block_bits) {
        throw std::invalid_argument("TABLECREATE requires block_bits");
    }
    (void)catalog_->Create(tokens[1], geometry, options.ApplyTo(catalog_->default_options()));
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleTableDrop(const ParsedCommandView& command) {
    if (command.argc != 1) {
        throw std::invalid_argument("TABLEDROP requires 1 argument: TABLEDROP <name>");
    }
    catalog_->Drop(command.args[0]);
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleTableSet(std::string_view line) {
    const auto tokens = ParseVarTokens(line);
    if (tokens.size() < 4 || tokens.size() % 2 != 0) {
        throw std::invalid_argument(
            "TABLESET requires TABLESET <name> <option> <value> [<option> <value> ...]");
    }
    // Only the named options change; the catalog applies them to the current
    // options under its own lock.
    TableOptionsUpdate update;
    std::vector<std::string_view> seen;
    for (std::size_t i = 2; i < tokens.size(); i += 2) {
        for (const auto previous : seen) {
            if (KeyIs(previous, tokens[i])) {
                throw std::invalid_argument("'" + std::string(tokens[i]) + "' is given twice");
            }
        }
        seen.push_back(tokens[i]);
        ApplyTableOption(&update, tokens[i], tokens[i + 1]);
    }
    catalog_->SetOptions(tokens[1], update);
    return Protocol::SimpleString("OK");
}

std::int64_t CommandEngine::ParseInt64(std::string_view token) {
    std::int64_t value = 0;
    const char* begin = token.data();
    const char* end = begin + token.size();
    const auto [ptr, ec] = std::from_chars(begin, end, value, 10);
    if (ec != std::errc{} || ptr != end) {
        throw std::invalid_argument("invalid integer: " + std::string(token));
    }
    return value;
}

std::uint64_t CommandEngine::ParseUint64(std::string_view token) {
    std::uint64_t value = 0;
    const char* begin = token.data();
    const char* end = begin + token.size();
    const auto [ptr, ec] = std::from_chars(begin, end, value, 10);
    if (ec != std::errc{} || ptr != end) {
        throw std::invalid_argument("invalid unsigned integer: " + std::string(token));
    }
    return value;
}

bool CommandEngine::IsAuthRequired() const noexcept {
    return config_.require_auth && !config_.auth_token.empty();
}

std::string CommandEngine::HandleMSet(ChunkStore& store, std::string_view line) {
    const auto tokens = ParseVarTokens(line);
    const std::size_t arg_count = tokens.size() - 1;
    if (arg_count == 0 || arg_count % 3 != 0) {
        throw std::invalid_argument(
            "MSET requires one or more x y bits triples: MSET x1 y1 bits1 ...");
    }
    std::vector<MSetItem> items;
    items.reserve(arg_count / 3);
    const std::size_t block_bits = store.geometry().config().block_bits;
    for (std::size_t i = 1; i < tokens.size(); i += 3) {
        const auto bits = tokens[i + 2];
        if (bits.size() != block_bits) {
            throw std::invalid_argument("bit string length does not match configured block_bits");
        }
        if (!BitCodec::IsBitString(bits)) {
            throw std::invalid_argument("bit string must contain only 0 and 1");
        }
        items.push_back(MSetItem{
            .x = ParseInt64(tokens[i]),
            .y = ParseInt64(tokens[i + 1]),
            .bits = bits,
        });
    }
    for (const auto& item : items) {
        store.SetBlockBits(item.x, item.y, item.bits);
    }
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleMGet(ChunkStore& store, std::string_view line) {
    const auto tokens = ParseVarTokens(line);
    const std::size_t arg_count = tokens.size() - 1;
    if (arg_count == 0 || arg_count % 2 != 0) {
        throw std::invalid_argument(
            "MGET requires one or more x y pairs: MGET x1 y1 x2 y2 ...");
    }
    std::vector<std::string> results;
    results.reserve(arg_count / 2);
    for (std::size_t i = 1; i < tokens.size(); i += 2) {
        results.push_back(store.GetBlockBits(ParseInt64(tokens[i]), ParseInt64(tokens[i + 1])));
    }
    return Protocol::Array(results);
}

}  // namespace chunkdb
