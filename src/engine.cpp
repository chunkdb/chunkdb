#include "chunkdb/engine.hpp"

#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <vector>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/zrle.hpp"

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

struct MSetItem {
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::string_view bits;
};

}  // namespace

CommandEngine::CommandEngine(
    EngineConfig config,
    std::shared_ptr<ChunkStore> store,
    std::shared_ptr<MetricsRegistry> metrics)
    : config_(std::move(config)),
      store_(std::move(store)),
      metrics_(metrics != nullptr ? std::move(metrics) : std::make_shared<MetricsRegistry>()) {
    if (!store_) {
        throw std::invalid_argument("store must not be null");
    }
    if (config_.require_auth && config_.auth_token.empty()) {
        throw std::invalid_argument("auth_token must be set when require_auth=true");
    }
    if (config_.max_auth_failures == 0) {
        throw std::invalid_argument("max_auth_failures must be > 0");
    }
}

std::string CommandEngine::Execute(SessionState& session, std::string_view line) {
    const auto command_name = ExtractCommandName(line);
    const auto started = std::chrono::steady_clock::now();
    std::string response = ExecuteInternal(session, line, command_name);
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
    std::string_view command_name) {
    try {
        // MSET/MGET/CHUNKBATCH accept variable numbers of arguments that
        // exceed ParseLineView's 8-arg limit, so intercept them before
        // calling ParseLineView.
        const auto& name = command_name;
        if (Protocol::CommandEquals(name, "MSET") || Protocol::CommandEquals(name, "MGET") ||
            Protocol::CommandEquals(name, "CHUNKBATCH")) {
            if (IsAuthRequired() && !session.authenticated) {
                return Protocol::Error("AUTH_REQUIRED", "use AUTH <token>");
            }
            if (Protocol::CommandEquals(name, "MSET")) {
                return HandleMSet(line);
            }
            if (Protocol::CommandEquals(name, "CHUNKBATCH")) {
                return HandleChunkBatch(line);
            }
            return HandleMGet(line);
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

        if (Protocol::CommandEquals(command.name, "GET")) {
            return HandleGet(command);
        }
        if (Protocol::CommandEquals(command.name, "EXISTS")) {
            return HandleExists(command);
        }
        if (Protocol::CommandEquals(command.name, "SET")) {
            return HandleSet(command);
        }
        if (Protocol::CommandEquals(command.name, "UNSET")) {
            return HandleUnset(command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKEXISTS")) {
            return HandleChunkExists(command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNK")) {
            return HandleChunk(command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKSET")) {
            return HandleChunkSet(command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKBIN")) {
            return HandleChunkBinary(command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKBINC")) {
            return HandleChunkBinaryCompressed(command);
        }
        if (Protocol::CommandEquals(command.name, "INFO")) {
            return HandleInfo();
        }
        if (Protocol::CommandEquals(command.name, "CHUNKSCAN")) {
            return HandleChunkScan(command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKRANGE")) {
            return HandleChunkRange(command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKRADIUS")) {
            return HandleChunkRadius(command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKVER")) {
            return HandleChunkVersion(command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKCAS")) {
            return HandleChunkCas(command);
        }
        if (Protocol::CommandEquals(command.name, "WALFLUSH")) {
            return HandleWalFlush(command);
        }
        if (Protocol::CommandEquals(command.name, "METRICS")) {
            return HandleMetrics();
        }

        return Protocol::Error("UNKNOWN_COMMAND", command.name);
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

std::string CommandEngine::HandleGet(const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("GET requires 2 arguments: GET <x> <y>");
    }

    const std::int64_t x = ParseInt64(command.args[0]);
    const std::int64_t y = ParseInt64(command.args[1]);

    const std::string bits = store_->GetBlockBits(x, y);
    return Protocol::Bulk(bits);
}

std::string CommandEngine::HandleExists(const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("EXISTS requires 2 arguments: EXISTS <x> <y>");
    }

    const std::int64_t x = ParseInt64(command.args[0]);
    const std::int64_t y = ParseInt64(command.args[1]);

    return Protocol::SimpleString(store_->BlockExists(x, y) ? "1" : "0");
}

std::string CommandEngine::HandleSet(const ParsedCommandView& command) {
    if (command.argc != 3) {
        throw std::invalid_argument("SET requires 3 arguments: SET <x> <y> <bits>");
    }

    const std::int64_t x = ParseInt64(command.args[0]);
    const std::int64_t y = ParseInt64(command.args[1]);

    store_->SetBlockBits(x, y, command.args[2]);
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleUnset(const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("UNSET requires 2 arguments: UNSET <x> <y>");
    }

    const std::int64_t x = ParseInt64(command.args[0]);
    const std::int64_t y = ParseInt64(command.args[1]);

    store_->UnsetBlock(x, y);
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleChunk(const ParsedCommandView& command) {
    if (command.argc != 2 && command.argc != 3) {
        throw std::invalid_argument("CHUNK requires 2 arguments or CHUNK <cx> <cy> STATE");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);

    if (command.argc == 3 && !IsStateMode(command.args[2])) {
        throw std::invalid_argument("CHUNK mode must be STATE when provided");
    }

    const std::string bits = command.argc == 3
                                 ? store_->GetChunkStateBits(chunk_x, chunk_y)
                                 : store_->GetChunkBits(chunk_x, chunk_y);
    return Protocol::Bulk(bits);
}

std::string CommandEngine::HandleChunkExists(const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("CHUNKEXISTS requires 2 arguments: CHUNKEXISTS <cx> <cy>");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);

    return Protocol::SimpleString(store_->ChunkExists(chunk_x, chunk_y) ? "1" : "0");
}

std::string CommandEngine::HandleChunkSet(const ParsedCommandView& command) {
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
        store_->SetChunkStateBits(chunk_x, chunk_y, payload_bits, presence_bits);
    } else {
        store_->SetChunkBits(chunk_x, chunk_y, command.args[2]);
    }
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleChunkBinary(const ParsedCommandView& command) {
    if (command.argc != 2 && command.argc != 3) {
        throw std::invalid_argument("CHUNKBIN requires 2 arguments or CHUNKBIN <cx> <cy> STATE");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);

    if (command.argc == 3 && !IsStateMode(command.args[2])) {
        throw std::invalid_argument("CHUNKBIN mode must be STATE when provided");
    }

    const auto payload = command.argc == 3
                             ? store_->GetChunkStateBytes(chunk_x, chunk_y)
                             : store_->GetChunkPayloadBytes(chunk_x, chunk_y);
    return Protocol::BulkBytes(payload);
}

std::string CommandEngine::HandleChunkBinaryCompressed(const ParsedCommandView& command) {
    if (command.argc != 2 && command.argc != 3) {
        throw std::invalid_argument("CHUNKBINC requires 2 arguments or CHUNKBINC <cx> <cy> STATE");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);

    if (command.argc == 3 && !IsStateMode(command.args[2])) {
        throw std::invalid_argument("CHUNKBINC mode must be STATE when provided");
    }

    const auto payload = command.argc == 3
                             ? store_->GetChunkStateBytes(chunk_x, chunk_y)
                             : store_->GetChunkPayloadBytes(chunk_x, chunk_y);
    return Protocol::BulkBytes(ZrleCompress(payload));
}

std::string CommandEngine::HandleInfo() const {
    const auto& cfg = store_->geometry().config();
    const auto runtime_stats = store_->RuntimeStats();
    std::string info;
    info += "chunkdb_version=1\n";
    info += "block_bits=" + std::to_string(cfg.block_bits) + "\n";
    info += "chunk_width_blocks=" + std::to_string(cfg.chunk_width_blocks) + "\n";
    info += "chunk_height_blocks=" + std::to_string(cfg.chunk_height_blocks) + "\n";
    info += "large_chunk_width_chunks=" + std::to_string(cfg.large_chunk_width_chunks) + "\n";
    info += "large_chunk_height_chunks=" + std::to_string(cfg.large_chunk_height_chunks) + "\n";
    info += "durability_mode=" + std::string(DurabilityModeName(store_->durability_mode())) + "\n";
    info += "access_mode=" + std::string(AccessModeName(store_->access_mode())) + "\n";
    info += "chunk_lock_mode=" + std::string(ChunkLockModeName()) + "\n";
    info +=
        "checkpoint_compression=" +
        std::string(CheckpointCompressionName(store_->checkpoint_compression())) + "\n";
    info += "loaded_chunks=" + std::to_string(store_->ApproxLoadedChunkCount()) + "\n";
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

std::string CommandEngine::HandleChunkScan(const ParsedCommandView& command) {
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

    const auto page = store_->ScanPopulatedChunks(
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

std::string CommandEngine::HandleChunkRange(const ParsedCommandView& command) {
    if (command.argc != 4) {
        throw std::invalid_argument(
            "CHUNKRANGE requires 4 arguments: CHUNKRANGE <cx0> <cy0> <cx1> <cy1>");
    }

    const auto entries = store_->ReadChunkRange(
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

std::string CommandEngine::HandleChunkRadius(const ParsedCommandView& command) {
    if (command.argc != 3) {
        throw std::invalid_argument(
            "CHUNKRADIUS requires 3 arguments: CHUNKRADIUS <cx> <cy> <radius_chunks>");
    }

    const auto entries = store_->ReadChunkRadius(
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

std::string CommandEngine::HandleChunkVersion(const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("CHUNKVER requires 2 arguments: CHUNKVER <cx> <cy>");
    }
    const std::uint64_t version =
        store_->GetChunkVersion(ParseInt64(command.args[0]), ParseInt64(command.args[1]));
    return Protocol::Bulk(std::to_string(version));
}

std::string CommandEngine::HandleChunkCas(const ParsedCommandView& command) {
    if (command.argc != 5 || !IsStateMode(command.args[3])) {
        throw std::invalid_argument(
            "CHUNKCAS requires CHUNKCAS <cx> <cy> <version> STATE <payload_bits>|<presence_bits>");
    }

    std::string_view payload_bits;
    std::string_view presence_bits;
    SplitChunkStateArgument(command.args[4], &payload_bits, &presence_bits);

    const auto result = store_->CasChunkState(
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

std::string CommandEngine::HandleChunkBatch(std::string_view line) {
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

    const auto result = store_->ApplyChunkBatch(
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
    store_->WalBarrier();
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleMetrics() const {
    return Protocol::Bulk(
        metrics_->RenderPrometheus(store_->RuntimeStats(), store_->ApproxLoadedChunkCount()));
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

std::string CommandEngine::HandleMSet(std::string_view line) {
    const auto tokens = ParseVarTokens(line);
    const std::size_t arg_count = tokens.size() - 1;
    if (arg_count == 0 || arg_count % 3 != 0) {
        throw std::invalid_argument(
            "MSET requires one or more x y bits triples: MSET x1 y1 bits1 ...");
    }
    std::vector<MSetItem> items;
    items.reserve(arg_count / 3);
    const std::size_t block_bits = store_->geometry().config().block_bits;
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
        store_->SetBlockBits(item.x, item.y, item.bits);
    }
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleMGet(std::string_view line) {
    const auto tokens = ParseVarTokens(line);
    const std::size_t arg_count = tokens.size() - 1;
    if (arg_count == 0 || arg_count % 2 != 0) {
        throw std::invalid_argument(
            "MGET requires one or more x y pairs: MGET x1 y1 x2 y2 ...");
    }
    std::vector<std::string> results;
    results.reserve(arg_count / 2);
    for (std::size_t i = 1; i < tokens.size(); i += 2) {
        results.push_back(store_->GetBlockBits(ParseInt64(tokens[i]), ParseInt64(tokens[i + 1])));
    }
    return Protocol::Array(results);
}

}  // namespace chunkdb
