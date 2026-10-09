#include "chunkdb/engine.hpp"
#include "source_address.hpp"

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

#include "chunkdb/change_feed.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/protocol.hpp"
#include "cql.hpp"
#include "scram.hpp"
#include "user_registry.hpp"
#include "table_options_text.hpp"

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

}  // namespace

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
    } else if (KeyIs(key, "var_max_chunk_bytes")) {
        update->var_max_chunk_bytes = ParsePositiveSize(key, value);
    } else if (IsGeometryKey(key)) {
        throw std::invalid_argument(
            std::string(key) + " is part of the geometry, which is fixed when a table is "
            "created");
    } else {
        throw std::invalid_argument("unknown table option '" + std::string(key) + "'");
    }
}
namespace {

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
    if (config_.require_auth && config_.users == nullptr) {
        throw std::invalid_argument("logins with users need the users of the data directory");
    }
    if (config_.max_auth_failures == 0) {
        throw std::invalid_argument("max_auth_failures must be > 0");
    }
}

// The longest SCRAM message HELLO and AUTH take as a parameter frame.
constexpr std::size_t kMaxScramMessageBytes = 1024;

// A login between HELLO 3 USER and AUTH.
struct PendingLogin {
    std::string user;
    // An unknown user runs against a decoy and always fails.
    bool known = false;
    scram::ServerExchange exchange;
};

std::string CommandEngine::Execute(
    SessionState& session,
    std::string_view line,
    std::span<const std::optional<std::string>> parameters) {
    const auto command_name = ExtractCommandName(line);
    const auto started = std::chrono::steady_clock::now();
    const bool hello = Protocol::CommandEquals(command_name, "HELLO");
    if (hello || (!session.greeted && Protocol::CommandEquals(command_name, "AUTH"))) {
        std::string response;
        try {
            response = hello ? HandleHello(session, line, parameters) : HandleAuth(session, line, parameters);
        } catch (const std::exception& e) {
            response = ErrorReply(e);
            session.pending_login.reset();
        }
        // Every failed HELLO or AUTH counts toward max_auth_failures
        // (AUTH_FAILED counts itself), so a connection that never completes
        // the login cannot hold a worker by repeating it.
        if (!session.greeted && !response.empty() && response[0] == '-' &&
            response.rfind("-ERR AUTH_FAILED", 0) != 0 && !session.close_after_reply) {
            ++session.failed_auth_attempts;
            if (session.failed_auth_attempts >= config_.max_auth_failures) {
                session.close_after_reply = true;
            }
        }
        ObserveReply(MetricsRegistry::CommandClass::kAuth, started, response);
        return response;
    }
    if (!session.greeted) {
        // A client of another protocol learns at once what this server speaks
        // instead of misreading a later reply.
        session.close_after_reply = true;
        std::string response = Protocol::Error("PROTOCOL", "expected HELLO 3");
        ObserveReply(MetricsRegistry::CommandClass::kOther, started, response);
        return response;
    }
    auto command_class = MetricsRegistry::CommandClass::kOther;
    ScopedWriteUser write_user(config_.require_auth ? std::string_view(session.user) : std::string_view{});
    std::string response = ExecuteStatement(session, line, parameters, command_class);
    ObserveReply(command_class, started, response);
    return response;
}

void CommandEngine::ObserveReply(
    MetricsRegistry::CommandClass command_class,
    std::chrono::steady_clock::time_point started,
    const std::string& response) {
    const auto elapsed = std::chrono::steady_clock::now() - started;

    const bool ok = response.empty() || response[0] != '-';
    metrics_->ObserveCommand(
        command_class,
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
}

std::string CommandEngine::AuthBanReply(SessionState& session) {
    if (session.remote_address.empty() || config_.max_auth_failures_per_ip == 0) {
        return {};
    }
    const std::string failure_key = SourceAddressKey(session.remote_address);
    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard lock(auth_failures_mutex_);
        const auto it = auth_failures_by_ip_.find(failure_key);
        if (it == auth_failures_by_ip_.end() || it->second.banned_until <= now) {
            return {};
        }
    }
    session.close_after_reply = true;
    if (config_.auth_failure_delay_ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.auth_failure_delay_ms));
    }
    return Protocol::Error("AUTH_FAILED", "temporary auth ban");
}

void CommandEngine::RecordAuthSuccess(SessionState& session) {
    session.authenticated = true;
    session.failed_auth_attempts = 0;
    if (!session.remote_address.empty() && config_.max_auth_failures_per_ip > 0) {
        std::lock_guard lock(auth_failures_mutex_);
        auth_failures_by_ip_.erase(SourceAddressKey(session.remote_address));
    }
}

std::string CommandEngine::RecordAuthFailure(SessionState& session) {
    const bool track_remote_ip = !session.remote_address.empty() && config_.max_auth_failures_per_ip > 0;
    const std::string failure_key = track_remote_ip ? SourceAddressKey(session.remote_address) : std::string();
    const auto now = std::chrono::steady_clock::now();
    std::chrono::milliseconds auth_failure_delay{0};
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
    return Protocol::Error("AUTH_FAILED", "invalid user or password");
}

CommandEngine::PayloadRequest CommandEngine::PlanPayload(
    SessionState& session,
    std::string_view line) const {
    if (!session.greeted) {
        // HELLO 3 USER <name> $1 and AUTH $1 carry one SCRAM message; Execute
        // refuses anything else and closes the connection.
        const auto tokens = ParseVarTokens(line);
        const auto name = ExtractCommandName(line);
        if (!tokens.empty() && tokens.back() == "$1" &&
            (Protocol::CommandEquals(name, "HELLO") || Protocol::CommandEquals(name, "AUTH"))) {
            return PayloadRequest{
                .plan = PayloadPlan::kParameters,
                .parameter_limits = {kMaxScramMessageBytes},
                .reject_response = {},
            };
        }
        return PayloadRequest{};
    }
    return PlanParameters(session, line);
}

std::string CommandEngine::ErrorReply(const std::exception& error) {
    // In the order a catch chain would test them: the first matching type
    // decides.
    if (dynamic_cast<const TableNotFoundError*>(&error) != nullptr) {
        return Protocol::Error("NO_TABLE", error.what());
    }
    if (dynamic_cast<const PermissionDeniedError*>(&error) != nullptr) {
        return Protocol::Error("PERMISSION_DENIED", error.what());
    }
    if (dynamic_cast<const TableExistsError*>(&error) != nullptr) {
        return Protocol::Error("TABLE_EXISTS", error.what());
    }
    if (const auto* conflict = dynamic_cast<const TransactionConflictError*>(&error); conflict != nullptr) {
        return Protocol::Error("CONFLICT", std::string(TxnConflictReasonName(conflict->reason())) + " " + error.what());
    }
    if (dynamic_cast<const std::invalid_argument*>(&error) != nullptr) {
        return Protocol::Error("INVALID_ARGUMENT", error.what());
    }
    if (dynamic_cast<const std::out_of_range*>(&error) != nullptr) {
        return Protocol::Error("OUT_OF_RANGE", error.what());
    }
    if (dynamic_cast<const WriteOutcomeUnknownError*>(&error) != nullptr) {
        LogMessage(
            LogLevel::kError,
            LogComponent::kStore,
            "command execution error; outcome unknown",
            {{"error", error.what()}});
        return Protocol::Error(
            "INTERNAL",
            "write outcome unknown: it may or may not be applied; the table is fail-closed until the "
            "server restarts");
    }
    LogMessage(
        LogLevel::kError,
        LogComponent::kStore,
        "command execution error",
        {{"error", error.what()}});
    return Protocol::Error("INTERNAL", "internal error");
}

std::string CommandEngine::HandleHello(
    SessionState& session,
    std::string_view line,
    std::span<const std::optional<std::string>> parameters) {
    const auto tokens = ParseVarTokens(line);
    if (session.greeted) {
        return Protocol::Error("PROTOCOL", "HELLO was already sent on this connection");
    }
    if (tokens.size() < 2 || tokens[1] != std::to_string(kProtocolVersion)) {
        session.close_after_reply = true;
        return Protocol::Error("PROTOCOL", "expected HELLO 3");
    }
    session.pending_login.reset();
    if (tokens.size() == 2) {
        if (config_.require_auth) {
            return Protocol::Error("AUTH_REQUIRED", "use HELLO 3 USER <name> $1 with a SCRAM-SHA-256 client-first message");
        }
        RecordAuthSuccess(session);
        session.greeted = true;
        return HelloReply({});
    }
    if (tokens.size() != 5 || !Protocol::CommandEquals(tokens[2], "USER") || tokens[4] != "$1") {
        throw std::invalid_argument("HELLO is HELLO 3, or HELLO 3 USER <name> $1");
    }
    if (!config_.require_auth) {
        throw std::invalid_argument("this server runs without users (--auth none): use HELLO 3");
    }
    if (parameters.size() != 1U || !parameters[0].has_value()) {
        return Protocol::Error("PROTOCOL", "HELLO 3 USER is followed by the SCRAM client-first message as $1");
    }
    if (std::string banned = AuthBanReply(session); !banned.empty()) {
        return banned;
    }
    auto first = scram::ParseClientFirst(*parameters[0]);
    const std::string user(tokens[3]);
    if (first.user != user) {
        throw std::invalid_argument("the SCRAM client-first message names another user than HELLO");
    }
    const auto found = config_.users->Find(user);
    const auto secret = config_.users->Secret();
    scram::Verifier verifier = found.has_value() ? found->verifier : scram::DecoyVerifier(secret, user);
    session.pending_login = std::make_shared<PendingLogin>(PendingLogin{
        .user = user,
        .known = found.has_value(),
        .exchange = scram::ServerExchange(std::move(first), std::move(verifier), scram::NewNonce()),
    });
    return Protocol::SimpleString("SCRAM " + session.pending_login->exchange.ServerFirst());
}

std::string CommandEngine::HandleAuth(
    SessionState& session,
    std::string_view line,
    std::span<const std::optional<std::string>> parameters) {
    const auto tokens = ParseVarTokens(line);
    const auto pending = std::move(session.pending_login);
    if (pending == nullptr) {
        session.close_after_reply = true;
        return Protocol::Error("PROTOCOL", "AUTH follows HELLO 3 USER <name> $1");
    }
    if (tokens.size() != 2 || tokens[1] != "$1" || parameters.size() != 1U || !parameters[0].has_value()) {
        throw std::invalid_argument("AUTH is AUTH $1 with the SCRAM client-final message");
    }
    // A source banned since its HELLO does not finish logging in either.
    if (std::string banned = AuthBanReply(session); !banned.empty()) {
        return banned;
    }
    const auto server_final = pending->exchange.Finish(*parameters[0]);
    if (!server_final.has_value() || !pending->known) {
        return RecordAuthFailure(session);
    }
    RecordAuthSuccess(session);
    session.greeted = true;
    session.user = pending->user;
    return HelloReply(*server_final);
}

std::string CommandEngine::HelloReply(std::string_view server_signature) const {
    std::string reply;
    const auto entry = [&reply](std::string_view key, std::uint64_t value) {
        Protocol::AppendBulk(reply, key);
        Protocol::AppendInteger(reply, value);
    };
    Protocol::AppendMapHeader(reply, 8);
    entry("protocol", kProtocolVersion);
    Protocol::AppendBulk(reply, "server_version");
    Protocol::AppendBulk(reply, config_.server_version);
    entry("max_line_bytes", config_.max_line_bytes);
    entry("max_parameters", cql::kMaxParameters);
    entry("max_area_chunks", kMaxChunkRangeChunks);
    entry("max_response_bytes", kMaxChunkRangeResponseBytes);
    entry("max_scan_limit", kMaxChunkScanLimit);
    // The client checks it to know the server holds the user's verifier.
    Protocol::AppendBulk(reply, "server_signature");
    if (server_signature.empty()) {
        Protocol::AppendNull(reply);
    } else {
        Protocol::AppendBulk(reply, server_signature);
    }
    return reply;
}

std::size_t CommandEngine::AuthFailureTrackedSourcesForTests() {
    std::lock_guard lock(auth_failures_mutex_);
    return auth_failures_by_ip_.size();
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


// IPv6 sources are bucketed by their /64 prefix: interface identifiers are
// attacker-controlled within one allocation, so per-address tracking would
// let a single /64 create billions of distinct entries. IPv4 and unparsable
// addresses are tracked exactly.
std::string SourceAddressKey(const std::string& remote_address) {
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

}  // namespace chunkdb
