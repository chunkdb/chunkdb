#include "chunkdb/engine.hpp"
#include "backup.hpp"
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
#include <system_error>
#include <thread>
#include <vector>

#include "chunkdb/change_feed.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/protocol.hpp"
#include "cql.hpp"
#include "feature_flags.hpp"
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

class HelloOptionError : public std::invalid_argument {
  public:
    HelloOptionError(std::string option, std::string_view reason)
        : std::invalid_argument("invalid HELLO option value"), option_(std::move(option)), reason_(reason) {}
    [[nodiscard]] const std::string& option() const noexcept { return option_; }
    [[nodiscard]] std::string_view reason() const noexcept { return reason_; }
  private:
    std::string option_;
    std::string_view reason_;
};

void ValidateHelloOptionNames(const cql::HelloRequest& request) {
    std::set<std::string_view> names;
    for (const auto& option : request.options) {
        if (!names.insert(option.name).second)
            throw HelloOptionError(option.name, "duplicate_option");
    }
}

SessionHelloOptions ParseHelloOptions(
    const cql::HelloRequest& request,
    std::span<const std::optional<std::string>> parameters,
    std::size_t max_text_bytes) {
    ValidateHelloOptionNames(request);
    SessionHelloOptions result;
    for (const auto& option : request.options) {
        const auto invalid = [&]() -> void {
            throw HelloOptionError(option.name, "option_value");
        };
        if (option.name != "client_name" && option.name != "statement_timeout_ms" && option.name != "default_tag") {
            result.unknown_options.push_back(option.name);
            continue;
        }
        const auto* parameter = std::get_if<cql::Parameter>(&option.value);
        const std::optional<std::string>* frame = nullptr;
        if (parameter != nullptr) {
            if (parameter->index == 0U || parameter->index > parameters.size()) invalid();
            frame = &parameters[parameter->index - 1U];
            if (!frame->has_value()) invalid();
        }
        if (option.name == "statement_timeout_ms") {
            std::uint64_t value = 0;
            if (frame != nullptr) {
                if ((*frame)->size() != 8U) invalid();
                for (std::size_t i = 0; i < 8U; ++i)
                    value |= static_cast<std::uint64_t>(static_cast<unsigned char>((**frame)[i])) << (8U * i);
            } else {
                const auto* integer = std::get_if<cql::Integer>(&option.value);
                if (integer == nullptr || integer->negative) invalid();
                value = integer->magnitude;
            }
            result.statement_timeout_ms = value;
            continue;
        }
        std::string value;
        if (frame != nullptr) value = **frame;
        else if (const auto* text = std::get_if<cql::Text>(&option.value)) value = text->value;
        else if (const auto* bytes = std::get_if<cql::Bytes>(&option.value); bytes && option.name == "default_tag")
            value.assign(bytes->value.begin(), bytes->value.end());
        else invalid();
        if (option.name == "client_name") {
            if (value.size() > max_text_bytes || !IsUtf8(std::span(
                    reinterpret_cast<const std::uint8_t*>(value.data()), value.size()))) invalid();
            result.client_name = std::move(value);
        } else {
            if (value.size() > 256U) invalid();
            result.default_tag = std::move(value);
        }
    }
    return result;
}

std::uint64_t NewConnectionId() {
    static std::atomic<std::uint64_t> next{1U};
    auto value = next.load(std::memory_order_relaxed);
    for (;;) {
        if (value == 0U) throw std::overflow_error("connection ID counter exhausted");
        const auto following = value == std::numeric_limits<std::uint64_t>::max() ? 0U : value + 1U;
        if (next.compare_exchange_weak(value, following, std::memory_order_relaxed)) return value;
    }
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
    } else if (KeyIs(key, "feed_buffer_bytes")) {
        update->feed_buffer_bytes = ParsePositiveSize(key, value);
    } else if (KeyIs(key, "slot_max_bytes")) {
        update->slot_max_bytes = ParsePositiveSize(key, value);
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
    if (config_.users) {
        if (!std::filesystem::equivalent(config_.users->data_dir(), catalog_->data_dir()))
            throw std::invalid_argument("users belong to another data directory");
        config_.users->SetMigrationHealth(catalog_->migration_health());
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
    SessionHelloOptions options;
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
        bool count_failure = true;
        try {
            response = hello ? HandleHello(session, line, parameters) : HandleAuth(session, line, parameters);
        } catch (const cql::DuplicateHelloOptionError& e) {
            response = ErrorReply(e);
            session.pending_login.reset();
            count_failure = false;
        } catch (const HelloOptionError& e) {
            response = ErrorReply(e);
            session.pending_login.reset();
            count_failure = false;
        } catch (const cql::ParseError& e) {
            response = Protocol::Error("SYNTAX", e.what());
            session.pending_login.reset();
        } catch (const std::exception& e) {
            response = ErrorReply(e);
            session.pending_login.reset();
        }
        // Login/grammar failures count toward max_auth_failures (AUTH_FAILED
        // counts itself). Known option validation is not an auth failure;
        // the transport handshake deadline still bounds ungreeted peers.
        if (count_failure && !session.greeted && !response.empty() && response[0] == '-' &&
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

std::string CommandEngine::RejectPayload(SessionState& session, std::string_view line, std::string response) {
    const auto started = std::chrono::steady_clock::now();
    const auto name = ExtractCommandName(line);
    const bool auth = Protocol::CommandEquals(name, "HELLO") || (!session.greeted && Protocol::CommandEquals(name, "AUTH"));
    if (!session.greeted) {
        session.pending_login.reset();
        if (response.starts_with("-ERR PROTOCOL")) session.close_after_reply = true;
        const bool option_error = response.starts_with("-ERR INVALID_ARGUMENT reason=option_value ") ||
            response.starts_with("-ERR INVALID_ARGUMENT reason=duplicate_option ");
        if (auth && !option_error && !session.close_after_reply &&
            ++session.failed_auth_attempts >= config_.max_auth_failures)
            session.close_after_reply = true;
    }
    ObserveReply(auth ? MetricsRegistry::CommandClass::kAuth : MetricsRegistry::CommandClass::kOther, started, response);
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
    std::chrono::steady_clock::time_point until;
    {
        std::lock_guard lock(auth_failures_mutex_);
        const auto it = auth_failures_by_ip_.find(failure_key);
        if (it == auth_failures_by_ip_.end() || it->second.banned_until <= now) {
            return {};
        }
        until = it->second.banned_until;
    }
    session.close_after_reply = true;
    if (config_.auth_failure_delay_ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.auth_failure_delay_ms));
    }
    const auto remaining = std::max<std::int64_t>(0, std::chrono::ceil<std::chrono::milliseconds>(
        until - std::chrono::steady_clock::now()).count());
    return Protocol::Error("RATE_LIMITED", "temporary auth ban", {
        {"reason", "auth_ban"}, {"retry_after_ms", std::to_string(remaining)}});
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
    return Protocol::Error("AUTH_FAILED", "invalid user or password; check the username and password in your connection URI");
}

CommandEngine::PayloadRequest CommandEngine::PlanPayload(
    SessionState& session,
    std::string_view line) const {
    const auto name = ExtractCommandName(line);
    if (Protocol::CommandEquals(name, "HELLO") && cql::MayHaveParameters(line)) {
        const auto count = cql::CountParameterTokens(line);
        const auto tokens = ParseVarTokens(line);
        if (session.greeted || tokens.size() < 2U || tokens[1] != "3")
            return PayloadRequest{.plan = PayloadPlan::kDiscard, .reject_response = Protocol::Error("PROTOCOL",
                session.greeted ? "HELLO was already sent on this connection" : "expected HELLO 3"),
                .frame_count = session.greeted ? count : 0U};
        try {
            const auto hello = cql::ParseHello(line);
            ValidateHelloOptionNames(hello);
            PayloadRequest request{.plan = PayloadPlan::kParameters};
            request.parameter_limits.resize(hello.parameters, std::numeric_limits<std::size_t>::max());
            request.opaque_parameters.resize(hello.parameters, true);
            request.parameter_rejections.resize(hello.parameters);
            if (hello.user) {
                request.parameter_limits[0] = kMaxScramMessageBytes;
                request.opaque_parameters[0] = false;
            }
            for (const auto& option : hello.options) {
                const auto* parameter = std::get_if<cql::Parameter>(&option.value);
                if (parameter == nullptr) continue;
                std::size_t limit = 0U;
                if (option.name == "client_name") limit = config_.max_line_bytes;
                else if (option.name == "default_tag") limit = 256U;
                else if (option.name == "statement_timeout_ms") limit = 8U;
                else continue;
                request.parameter_limits[parameter->index - 1U] = limit;
                request.parameter_rejections[parameter->index - 1U] =
                    Protocol::Error("INVALID_ARGUMENT", "invalid HELLO option value", {
                        {"reason", "option_value"}, {"option", option.name}});
                request.opaque_parameters[parameter->index - 1U] = false;
            }
            return request;
        } catch (const cql::DuplicateHelloOptionError& e) {
            return PayloadRequest{.plan = PayloadPlan::kDiscard, .reject_response = ErrorReply(e), .frame_count = count};
        } catch (const HelloOptionError& e) {
            return PayloadRequest{.plan = PayloadPlan::kDiscard, .reject_response = ErrorReply(e), .frame_count = count};
        } catch (const cql::ParseError& e) {
            return PayloadRequest{.plan = PayloadPlan::kDiscard,
                .reject_response = Protocol::Error("SYNTAX", e.what()), .frame_count = count};
        }
    }
    if (!session.greeted) {
        const auto count = cql::CountParameterTokens(line);
        const auto tokens = ParseVarTokens(line);
        if (Protocol::CommandEquals(name, "AUTH") && tokens.size() == 2U && tokens[1] == "$1")
            return PayloadRequest{.plan = PayloadPlan::kParameters, .parameter_limits = {kMaxScramMessageBytes}};
        if (count != 0U)
            // This unauthenticated stream is closed after the error; waiting
            // for advertised payloads would weaken immediate handshake rejection.
            return PayloadRequest{.plan = PayloadPlan::kDiscard,
                .reject_response = Protocol::Error("PROTOCOL", "expected HELLO 3"), .frame_count = 0U};
        return PayloadRequest{};
    }
    return PlanParameters(session, line);
}

std::string CommandEngine::ErrorReply(const std::exception& error) {
    // In the order a catch chain would test them: the first matching type
    // decides.
    if (const auto* option = dynamic_cast<const HelloOptionError*>(&error))
        return Protocol::Error("INVALID_ARGUMENT", error.what(), {{"reason", option->reason()}, {"option", option->option()}});
    if (const auto* option = dynamic_cast<const cql::DuplicateHelloOptionError*>(&error))
        return Protocol::Error("INVALID_ARGUMENT", error.what(), {{"reason", "duplicate_option"}, {"option", option->option()}});
    if (const auto* feature = dynamic_cast<const FeatureAdmissionError*>(&error))
        return Protocol::Error("INVALID_ARGUMENT", error.what(), {{"reason", feature->reason()}, {"feature", feature->feature()}});
    if (dynamic_cast<const BackupBusyError*>(&error) != nullptr) {
        return Protocol::Error("BUSY", error.what());
    }
    if (dynamic_cast<const BackupPublicationUnknownError*>(&error) != nullptr) {
        return Protocol::Error("OUTCOME_UNKNOWN", error.what(), {{"stage", "publication_guard_reinstatement"}});
    }
    if (dynamic_cast<const FeatureRecoveryRequiredError*>(&error) != nullptr)
        return Protocol::Error("INTERNAL", error.what());
    if (dynamic_cast<const MigrationOutcomeUnknownError*>(&error) != nullptr) {
        LogMessage(LogLevel::kError, LogComponent::kStore, "migration outcome unknown", {{"error", error.what()}});
        return Protocol::Error("OUTCOME_UNKNOWN", std::string(kMigrationRecoveryRequiredMessage), {{"stage", "migration_completion"}});
    }
    if (dynamic_cast<const MigrationRecoveryRequiredError*>(&error) != nullptr) {
        LogMessage(LogLevel::kError, LogComponent::kStore, "migration recovery required", {{"error", error.what()}});
        return Protocol::Error("INTERNAL", std::string(kMigrationRecoveryRequiredMessage));
    }
    if (dynamic_cast<const FeedRecoveryRequiredError*>(&error) != nullptr) {
        return Protocol::Error("INTERNAL", std::string(kFeedRecoveryRequiredMessage));
    }
    if (dynamic_cast<const MigrationConflictError*>(&error) != nullptr)
        return Protocol::Error("MIGRATION_MISMATCH", error.what());
    if (dynamic_cast<const FeedSlotFiltersError*>(&error) != nullptr)
        return Protocol::Error("INVALID_ARGUMENT", error.what(), {{"reason", "slot_filters"}});
    if (const auto* busy = dynamic_cast<const FeedSlotBusyError*>(&error)) {
        return Protocol::Error("SLOT_BUSY", error.what(), {{"reason", busy->reason()}});
    }
    if (const auto* lost = dynamic_cast<const FeedSlotLostError*>(&error)) {
        return Protocol::Error("SLOT_LOST", error.what(), {{"reason", lost->reason()}});
    }
    if (dynamic_cast<const FeedSlotNotFoundError*>(&error) != nullptr) {
        return Protocol::Error("INVALID_ARGUMENT", error.what());
    }
    if (dynamic_cast<const TableNotFoundError*>(&error) != nullptr) {
        return Protocol::Error("NO_TABLE", std::string(error.what()) + "; use SHOW TABLES to list accessible tables");
    }
    if (dynamic_cast<const PermissionDeniedError*>(&error) != nullptr) {
        return Protocol::Error("PERMISSION_DENIED", std::string(error.what()) + "; ask an administrator to grant this right");
    }
    if (dynamic_cast<const TableExistsError*>(&error) != nullptr) {
        return Protocol::Error("TABLE_EXISTS", error.what());
    }
    if (const auto* conflict = dynamic_cast<const TransactionConflictError*>(&error); conflict != nullptr) {
        return Protocol::Error("CONFLICT", error.what(), {{"reason", TxnConflictReasonName(conflict->reason())}});
    }
    if (dynamic_cast<const std::invalid_argument*>(&error) != nullptr) {
        if (std::string_view(error.what()).starts_with("the table has no column "))
            return Protocol::Error("INVALID_ARGUMENT", std::string(error.what()) + "; use DESCRIBE <table> to check column names");
        return Protocol::Error("INVALID_ARGUMENT", error.what());
    }
    if (dynamic_cast<const std::out_of_range*>(&error) != nullptr) {
        return Protocol::Error("OUT_OF_RANGE", error.what());
    }
    if (const auto* outcome = dynamic_cast<const WriteOutcomeUnknownError*>(&error)) {
        LogMessage(
            LogLevel::kError,
            LogComponent::kStore,
            "command execution error; outcome unknown",
            {{"error", error.what()}});
        return Protocol::Error(
            "OUTCOME_UNKNOWN",
            "write outcome unknown: it may or may not be applied; the table is fail-closed until the "
            "server restarts", {{"stage", outcome->stage()}});
    }
    if (const auto* io = dynamic_cast<const std::system_error*>(&error);
        io != nullptr && io->code() == std::errc::no_space_on_device)
        return Protocol::Error("DISK_FULL", "storage has no free space; free space before retrying");
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
    const auto hello = cql::ParseHello(line);
    auto options = ParseHelloOptions(hello, parameters, config_.max_line_bytes);
    if (session.connection_id == 0U) session.connection_id = NewConnectionId();
    if (!hello.user) {
        if (config_.require_auth) {
            return Protocol::Error("AUTH_REQUIRED", "use HELLO 3 USER <name> $1 with a SCRAM-SHA-256 client-first message; set the username and password in your client connection URI");
        }
        RecordAuthSuccess(session);
        session.hello_options = std::move(options);
        session.greeted = true;
        return HelloReply(session, {});
    }
    if (!config_.require_auth)
        throw std::invalid_argument("this server runs without users (--auth none): use HELLO 3");
    if (parameters.size() != hello.parameters || parameters.empty() || !parameters[0].has_value())
        return Protocol::Error("PROTOCOL", "HELLO 3 USER is followed by the SCRAM client-first message as $1");
    if (std::string banned = AuthBanReply(session); !banned.empty()) {
        return banned;
    }
    auto first = scram::ParseClientFirst(*parameters[0]);
    const std::string user(*hello.user);
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
        .options = std::move(options),
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
    session.hello_options = std::move(pending->options);
    return HelloReply(session, *server_final);
}

std::string CommandEngine::HelloReply(const SessionState& session, std::string_view server_signature) const {
    std::string reply;
    const auto entry = [&reply](std::string_view key, std::uint64_t value) {
        Protocol::AppendBulk(reply, key);
        Protocol::AppendInteger(reply, value);
    };
    Protocol::AppendMapHeader(reply, 14);
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
    entry("connection_id", session.connection_id);
    Protocol::AppendBulk(reply, "features");
    Protocol::AppendArrayHeader(reply, 4);
    Protocol::AppendBulk(reply, "protocol_foundation");
    Protocol::AppendBulk(reply, "ordered_versions");
    Protocol::AppendBulk(reply, "read_metadata");
    Protocol::AppendBulk(reply, "feed_progress");
    Protocol::AppendBulk(reply, "options");
    Protocol::AppendMapHeader(reply, session.hello_options.client_name ? 1U : 0U);
    if (session.hello_options.client_name) {
        Protocol::AppendBulk(reply, "client_name");
        Protocol::AppendBulk(reply, *session.hello_options.client_name);
    }
    Protocol::AppendBulk(reply, "not_applied");
    Protocol::AppendMapHeader(reply, session.hello_options.unknown_options.size() +
        (session.hello_options.statement_timeout_ms ? 1U : 0U) + (session.hello_options.default_tag ? 1U : 0U));
    const auto deferred = [&reply](std::string_view name, std::string_view reason) {
        Protocol::AppendBulk(reply, name);
        Protocol::AppendMapHeader(reply, 1);
        Protocol::AppendBulk(reply, "reason");
        Protocol::AppendBulk(reply, reason);
    };
    if (session.hello_options.statement_timeout_ms) deferred("statement_timeout_ms", "implementation_pending");
    if (session.hello_options.default_tag) deferred("default_tag", "implementation_pending");
    for (const auto& name : session.hello_options.unknown_options) deferred(name, "unknown_option");
    entry("idle_timeout_ms", session.idle_timeout_ms);
    Protocol::AppendBulk(reply, "limits");
    Protocol::AppendMapHeader(reply, 7);
    entry("max_area_chunks", kMaxChunkRangeChunks);
    entry("max_area_response_bytes", kMaxChunkRangeResponseBytes);
    entry("txn_max_written_chunks", kMaxTxnWrittenChunks);
    entry("txn_max_read_chunks", kMaxTxnReadChunks);
    entry("max_watches", session.max_watches);
    entry("max_returning_rows", 65536U);
    entry("max_expression_nodes", 256U);
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
