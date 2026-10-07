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
#include "chunk_store_internal.hpp"
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
    } else if (KeyIs(key, "extra_max_block_bits")) {
        std::uint32_t parsed = 0;
        const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed, 10);
        if (value.empty() || result.ec != std::errc() || result.ptr != value.data() + value.size()) {
            throw std::invalid_argument(
                "extra_max_block_bits must be a 32-bit integer, got '" + std::string(value) + "'");
        }
        update->extra_max_block_bits = parsed;
    } else if (KeyIs(key, "extra_max_chunk_bytes")) {
        update->extra_max_chunk_bytes = ParsePositiveSize(key, value);
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
    // Both 0 for a table without extra data.
    const bool extra = info.options.extra_max_block_bits != 0U;
    out += "extra_max_block_bits=" + std::to_string(info.options.extra_max_block_bits) + "\n";
    out += "extra_max_chunk_bytes=" +
           std::to_string(extra ? info.options.extra_max_chunk_bytes : 0U) + "\n";
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

// Extra bytes a zrle CHUNKPUT payload may have over the data it encodes.
// It covers the most the server's own encoder adds, so a CHUNKGET ... ZRLE
// reply can always be written back with CHUNKPUT ... ZRLE.
constexpr std::size_t kZrleChunkPutSlackBytes = 16;
static_assert(kZrleChunkPutSlackBytes >= kZrleMaxOverheadBytes);
// The largest XPUT payload: one value of a chunk's whole extra-data cap.
constexpr std::size_t kMaxXPutPayloadBytes = ExtraValueBytes(kExtraMaxBlockBitsLimit);

[[nodiscard]] std::vector<std::uint8_t> FullPresence(const Geometry& geometry) {
    std::vector<std::uint8_t> presence((geometry.ChunkBlockCount() + 7U) / 8U, 0xFFU);
    return presence;
}

[[nodiscard]] std::size_t PresenceBytes(const Geometry& geometry) noexcept {
    return (geometry.ChunkBlockCount() + 7U) / 8U;
}

// XGET reply layout of one value: bit_length u32le, then the value bytes.
[[nodiscard]] std::vector<std::uint8_t> EncodeExtraReply(const ExtraValue& value) {
    std::vector<std::uint8_t> out;
    out.reserve(4U + value.bytes.size());
    for (unsigned shift = 0; shift < 32U; shift += 8U) {
        out.push_back(static_cast<std::uint8_t>((value.bit_length >> shift) & 0xFFU));
    }
    out.insert(out.end(), value.bytes.begin(), value.bytes.end());
    return out;
}

// A CHUNKBATCH XPUT value: one bit per character, as SET bit strings.
[[nodiscard]] ExtraValue ExtraValueFromBits(std::string_view bits) {
    if (bits.empty() || !BitCodec::IsBitString(bits)) {
        throw std::invalid_argument("XPUT bits must be a non-empty string of 0 and 1");
    }
    if (bits.size() > kExtraMaxBlockBitsLimit) {
        throw std::invalid_argument("XPUT bit string is too long");
    }
    const auto bit_length = static_cast<std::uint32_t>(bits.size());
    std::vector<std::uint8_t> bytes(ExtraValueBytes(bit_length), 0U);
    BitCodec::WriteBits(bytes, 0, bits);
    return MakeExtraValue(bit_length, std::move(bytes), ExtraPadding::kReject);
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

// HELLO selected the connection's table (none when `default` did not
// exist); only USE changes it, so a `default` created later is not bound
// behind the client's back.
constexpr std::string_view kNoTableSelected =
    "no table selected (table 'default' did not exist at HELLO); select one with USE <name>";

Table::Lease CommandEngine::AcquireTable(SessionState& session) const {
    const auto& table = session.table;
    if (table == nullptr) {
        throw TableNotFoundError(std::string(kNoTableSelected));
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

std::string CommandEngine::Execute(
    SessionState& session,
    std::string_view line,
    std::string_view payload) {
    const auto command_name = ExtractCommandName(line);
    const auto started = std::chrono::steady_clock::now();
    std::string response = ExecuteInternal(session, line, command_name, payload);
    // Every failed HELLO counts toward max_auth_failures (AUTH_FAILED counts
    // itself), so a connection that never completes the handshake cannot
    // hold a worker by repeating it.
    if (!session.greeted && Protocol::CommandEquals(command_name, "HELLO") && !response.empty() &&
        response[0] == '-' && response.rfind("-ERR AUTH_FAILED", 0) != 0 && !session.close_after_reply) {
        ++session.failed_auth_attempts;
        if (session.failed_auth_attempts >= config_.max_auth_failures) {
            session.close_after_reply = true;
        }
    }
    ObserveReply(command_name, started, response);
    return response;
}

std::string CommandEngine::ExecuteDiscarded(
    SessionState& session,
    std::string_view line,
    std::string refusal) {
    const auto command_name = ExtractCommandName(line);
    const auto started = std::chrono::steady_clock::now();
    std::string response;
    try {
        // As for a request that was read: a dropped table answers NO_TABLE.
        (void)AcquireTable(session);
        response = std::move(refusal);
    } catch (const TableNotFoundError& e) {
        response = Protocol::Error("NO_TABLE", e.what());
    }
    ObserveReply(command_name, started, response);
    return response;
}

void CommandEngine::ObserveReply(
    std::string_view command_name,
    std::chrono::steady_clock::time_point started,
    const std::string& response) {
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
}

std::string CommandEngine::Authenticate(SessionState& session, std::string_view token) {
    if (!IsAuthRequired()) {
        session.authenticated = true;
        session.failed_auth_attempts = 0;
        return {};
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

    if (ConstantTimeEqual(token, config_.auth_token)) {
        session.authenticated = true;
        session.failed_auth_attempts = 0;
        if (track_remote_ip) {
            std::lock_guard lock(auth_failures_mutex_);
            auth_failures_by_ip_.erase(failure_key);
        }
        return {};
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

CommandEngine::ChunkForm CommandEngine::ParseChunkForm(
    const ParsedCommandView& command,
    std::size_t begin,
    std::size_t end,
    std::string_view command_name,
    bool allow_extra) {
    ChunkForm form;
    for (std::size_t i = begin; i < end; ++i) {
        bool* flag = Protocol::CommandEquals(command.args[i], "STATE")  ? &form.state
                     : Protocol::CommandEquals(command.args[i], "ZRLE") ? &form.zrle
                     : allow_extra && Protocol::CommandEquals(command.args[i], "EXTRA")
                         ? &form.extra
                         : nullptr;
        if (flag == nullptr) {
            throw std::invalid_argument(
                std::string(command_name) +
                (allow_extra ? " options are STATE, EXTRA and ZRLE, got '"
                             : " options are STATE and ZRLE, got '") +
                std::string(command.args[i]) + "'");
        }
        if (*flag) {
            throw std::invalid_argument(
                std::string(command_name) + " option " + std::string(command.args[i]) +
                " is given twice");
        }
        *flag = true;
    }
    if (form.extra && !form.state) {
        throw std::invalid_argument(std::string(command_name) + " EXTRA requires STATE");
    }
    return form;
}

CommandEngine::ChunkPutRequest CommandEngine::ParseChunkPut(const ParsedCommandView& command) {
    if (command.argc < 3) {
        throw std::invalid_argument(
            "CHUNKPUT requires CHUNKPUT <cx> <cy> [STATE] [EXTRA] [ZRLE] [IF <version>] <length>");
    }
    ChunkPutRequest put;
    put.chunk_x = ParseInt64(command.args[0]);
    put.chunk_y = ParseInt64(command.args[1]);
    put.length = ParsePayloadLength(command.args[command.argc - 1]);
    std::size_t options_end = command.argc - 1;
    // IF <version> comes last among the options.
    if (options_end >= 4 && Protocol::CommandEquals(command.args[options_end - 2], "IF")) {
        put.has_if = true;
        put.if_version = ParseUint64(command.args[options_end - 1]);
        options_end -= 2;
    }
    const auto form = ParseChunkForm(command, 2, options_end, "CHUNKPUT", /*allow_extra=*/true);
    put.state = form.state;
    put.zrle = form.zrle;
    put.extra = form.extra;
    return put;
}

CommandEngine::XPutRequest CommandEngine::ParseXPut(const ParsedCommandView& command) {
    if (command.argc != 4) {
        throw std::invalid_argument("XPUT requires XPUT <x> <y> <bit_length> <length>");
    }
    XPutRequest put;
    put.x = ParseInt64(command.args[0]);
    put.y = ParseInt64(command.args[1]);
    put.bit_length = ParseUint64(command.args[2]);
    put.length = ParsePayloadLength(command.args[3]);
    return put;
}

std::string CommandEngine::CheckXPut(const XPutRequest& put, const TableInfo& info) {
    if (info.options.extra_max_block_bits == 0U) {
        return ExtraDataDisabled(info.name);
    }
    if (put.bit_length == 0U || put.bit_length > info.options.extra_max_block_bits) {
        return "XPUT bit_length must be between 1 and extra_max_block_bits (" +
               std::to_string(info.options.extra_max_block_bits) + ")";
    }
    const auto bit_length = static_cast<std::uint32_t>(put.bit_length);
    if (put.length != ExtraValueBytes(bit_length)) {
        return "XPUT of " + std::to_string(bit_length) + " bits takes " +
               std::to_string(ExtraValueBytes(bit_length)) + " bytes, got " +
               std::to_string(put.length);
    }
    return {};
}

std::string CommandEngine::ExtraDataDisabled(const std::string& table_name) {
    return "extra data is not enabled on table '" + table_name +
           "' (TABLESET " + table_name + " extra_max_block_bits <bits>)";
}

CommandEngine::PayloadRequest CommandEngine::PlanPayload(
    SessionState& session,
    std::string_view line) const {
    PayloadRequest request;
    const auto command_name = ExtractCommandName(line);
    const bool xput = Protocol::CommandEquals(command_name, "XPUT");
    if (!xput && !Protocol::CommandEquals(command_name, "CHUNKPUT")) {
        return request;
    }
    // A payload that cannot be framed safely is refused unread and the
    // connection closes: the bytes that follow could not be told apart from
    // the next command.
    const auto reject = [&](std::string response) {
        request.plan = PayloadPlan::kReject;
        request.reject_response = std::move(response);
        return request;
    };
    if (!session.greeted) {
        return reject(Protocol::Error("PROTOCOL", "expected HELLO 2"));
    }
    ChunkPutRequest put;
    XPutRequest xput_request;
    try {
        const auto command = Protocol::ParseLineView(line);
        if (xput) {
            xput_request = ParseXPut(command);
        } else {
            put = ParseChunkPut(command);
        }
    } catch (const std::invalid_argument& e) {
        return reject(Protocol::Error("INVALID_ARGUMENT", e.what()));
    }
    // Sized by the selected table. A dropped table still has its geometry
    // and options: the payload is read and the command then fails with
    // NO_TABLE.
    const auto& table = session.table;
    if (table == nullptr) {
        return reject(Protocol::Error("NO_TABLE", kNoTableSelected));
    }
    // The bound depends only on the request line, the geometry and protocol
    // caps, never on options TABLESET can change; a request within it that
    // breaks a table limit is read, dropped and refused, and the connection
    // stays usable.
    const auto read = [&](std::size_t bytes) {
        request.plan = PayloadPlan::kRead;
        request.bytes = bytes;
        return request;
    };
    const auto discard = [&](std::size_t bytes, std::string message) {
        request.plan = PayloadPlan::kDiscard;
        request.bytes = bytes;
        request.reject_response = Protocol::Error("INVALID_ARGUMENT", message);
        return request;
    };
    if (xput) {
        if (xput_request.length > kMaxXPutPayloadBytes) {
            return reject(Protocol::Error(
                "BAD_REQUEST",
                "payload length exceeds the largest value (" + std::to_string(kMaxXPutPayloadBytes) +
                    " bytes)"));
        }
        if (auto problem = CheckXPut(xput_request, table->Info()); !problem.empty()) {
            return discard(xput_request.length, std::move(problem));
        }
        return read(xput_request.length);
    }
    const Geometry geometry(table->geometry());
    const std::size_t state_bytes =
        geometry.ChunkPayloadBytes() + (put.state ? PresenceBytes(geometry) : 0U);
    const std::size_t raw_bytes = state_bytes + (put.extra ? kExtraMaxChunkBytesLimit : 0U);
    // A zrle payload larger than the data it encodes is not worth accepting:
    // send such a chunk uncompressed.
    const std::size_t max_bytes = put.zrle ? raw_bytes + kZrleChunkPutSlackBytes : raw_bytes;
    if (put.length > max_bytes) {
        return reject(Protocol::Error(
            "BAD_REQUEST",
            "payload length exceeds the chunk size (" + std::to_string(max_bytes) + " bytes)"));
    }
    if (put.extra) {
        const TableInfo info = table->Info();
        if (info.options.extra_max_block_bits == 0U) {
            return discard(put.length, ExtraDataDisabled(info.name));
        }
        if (!put.zrle && put.length > state_bytes + info.options.extra_max_chunk_bytes) {
            return discard(
                put.length,
                "extra data section of " + std::to_string(put.length - state_bytes) +
                    " bytes exceeds extra_max_chunk_bytes (" +
                    std::to_string(info.options.extra_max_chunk_bytes) + ")");
        }
    }
    return read(put.length);
}

std::string CommandEngine::ExecuteInternal(
    SessionState& session,
    std::string_view line,
    std::string_view command_name,
    std::string_view payload) {
    try {
        const auto& name = command_name;
        if (Protocol::CommandEquals(name, "HELLO")) {
            return HandleHello(session, line);
        }
        if (!session.greeted) {
            // A 1.x client (or anything else) learns at once what this
            // server speaks instead of misreading a later reply.
            session.close_after_reply = true;
            return Protocol::Error("PROTOCOL", "expected HELLO 2");
        }

        // MSET/MGET/CHUNKBATCH and the table commands that take options
        // accept variable numbers of arguments that exceed ParseLineView's
        // 8-arg limit, so intercept them before calling ParseLineView.
        if (Protocol::CommandEquals(name, "MSET") || Protocol::CommandEquals(name, "MGET") ||
            Protocol::CommandEquals(name, "CHUNKBATCH") ||
            Protocol::CommandEquals(name, "TABLECREATE") ||
            Protocol::CommandEquals(name, "TABLESET")) {
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
        if (Protocol::CommandEquals(command.name, "QUIT")) {
            session.close_after_reply = true;
            return Protocol::SimpleString("BYE");
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

        static constexpr std::array<std::string_view, 14> kTableCommands = {
            "GET", "SET", "UNSET", "CHUNKEXISTS", "CHUNKGET", "CHUNKPUT", "INFO",
            "CHUNKSCAN", "CHUNKRANGE", "CHUNKRADIUS", "CHUNKVER", "XGET", "XPUT", "XDEL"};
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
        if (Protocol::CommandEquals(command.name, "SET")) {
            return HandleSet(store, command);
        }
        if (Protocol::CommandEquals(command.name, "UNSET")) {
            return HandleUnset(store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKEXISTS")) {
            return HandleChunkExists(store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKGET")) {
            return HandleChunkGet(*session.table, store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKPUT")) {
            return HandleChunkPut(store, command, payload);
        }
        if (Protocol::CommandEquals(command.name, "INFO")) {
            return HandleInfo(*session.table, store);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKSCAN")) {
            return HandleChunkScan(store, command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKRANGE")) {
            return HandleChunkArea(store, command, /*radius=*/false);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKRADIUS")) {
            return HandleChunkArea(store, command, /*radius=*/true);
        }
        if (Protocol::CommandEquals(command.name, "CHUNKVER")) {
            return HandleChunkVersion(store, command);
        }
        if (Protocol::CommandEquals(command.name, "XGET")) {
            return HandleXGet(*session.table, store, command);
        }
        if (Protocol::CommandEquals(command.name, "XPUT")) {
            return HandleXPut(*session.table, store, command, payload);
        }
        if (Protocol::CommandEquals(command.name, "XDEL")) {
            return HandleXDel(store, command);
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

std::string CommandEngine::HandleHello(SessionState& session, std::string_view line) {
    const auto tokens = ParseVarTokens(line);
    if (session.greeted) {
        return Protocol::Error("PROTOCOL", "HELLO was already sent on this connection");
    }
    if (tokens.size() < 2 || tokens[1] != std::to_string(kProtocolVersion)) {
        session.close_after_reply = true;
        return Protocol::Error("PROTOCOL", "expected HELLO 2");
    }
    std::optional<std::string_view> token;
    std::optional<std::string_view> table_name;
    for (std::size_t i = 2; i < tokens.size(); i += 2) {
        if (i + 1 >= tokens.size()) {
            throw std::invalid_argument("HELLO options are AUTH <token> and TABLE <name>");
        }
        auto* target = Protocol::CommandEquals(tokens[i], "AUTH")    ? &token
                       : Protocol::CommandEquals(tokens[i], "TABLE") ? &table_name
                                                                     : nullptr;
        if (target == nullptr) {
            throw std::invalid_argument(
                "unknown HELLO option '" + std::string(tokens[i]) +
                "'; options are AUTH <token> and TABLE <name>");
        }
        if (target->has_value()) {
            throw std::invalid_argument("HELLO option " + std::string(tokens[i]) + " is given twice");
        }
        *target = tokens[i + 1];
    }

    if (token.has_value()) {
        if (std::string failure = Authenticate(session, *token); !failure.empty()) {
            return failure;
        }
    } else if (IsAuthRequired() && !session.authenticated) {
        return Protocol::Error("AUTH_REQUIRED", "use HELLO 2 AUTH <token>");
    }

    std::shared_ptr<Table> table;
    if (table_name.has_value()) {
        table = catalog_->Find(*table_name);
        if (table == nullptr) {
            throw TableNotFoundError("table '" + std::string(*table_name) + "' does not exist");
        }
    } else {
        table = catalog_->Find(kDefaultTableName);
    }

    std::string reply;
    reply += "protocol=" + std::to_string(kProtocolVersion) + "\n";
    reply += "server_version=" + config_.server_version + "\n";
    // What this server can do; whether a table has extra data is in its
    // extra_max_block_bits line.
    reply += "capabilities=zrle,extra-data\n";
    reply += "max_line_bytes=" + std::to_string(config_.max_line_bytes) + "\n";
    reply += "max_area_chunks=" + std::to_string(kMaxChunkRangeChunks) + "\n";
    reply += "max_response_bytes=" + std::to_string(kMaxChunkRangeResponseBytes) + "\n";
    reply += "max_scan_limit=" + std::to_string(kMaxChunkScanLimit) + "\n";
    reply += "max_batch_ops=" + std::to_string(kMaxChunkBatchOps) + "\n";
    reply += "max_extra_chunk_bytes=" + std::to_string(kExtraMaxChunkBytesLimit) + "\n";
    if (table != nullptr) {
        // Without `default` and without TABLE, the connection has no table
        // until USE selects one.
        reply += RenderTableInfo(table->Info());
    }
    session.greeted = true;
    session.table = std::move(table);
    return Protocol::Bulk(reply);
}

std::string CommandEngine::HandleGet(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("GET requires 2 arguments: GET <x> <y>");
    }
    const auto bits = store.ReadBlockBits(ParseInt64(command.args[0]), ParseInt64(command.args[1]));
    return bits.has_value() ? Protocol::Bulk(*bits) : Protocol::Null();
}

std::string CommandEngine::HandleMGet(ChunkStore& store, std::string_view line) {
    const auto tokens = ParseVarTokens(line);
    const std::size_t arg_count = tokens.size() - 1;
    if (arg_count == 0 || arg_count % 2 != 0) {
        throw std::invalid_argument(
            "MGET requires one or more x y pairs: MGET x1 y1 x2 y2 ...");
    }
    // Bounded like area reads: each item is block_bits bytes of text plus
    // its framing.
    const std::size_t item_bytes = static_cast<std::size_t>(store.geometry().config().block_bits) + 16U;
    if (arg_count / 2 > kMaxChunkRangeResponseBytes / item_bytes) {
        throw std::out_of_range(
            "MGET reply would exceed " + std::to_string(kMaxChunkRangeResponseBytes) +
            " bytes; read fewer blocks per command or the chunk with CHUNKGET");
    }
    std::vector<std::optional<std::string>> results;
    results.reserve(arg_count / 2);
    for (std::size_t i = 1; i < tokens.size(); i += 2) {
        results.push_back(store.ReadBlockBits(ParseInt64(tokens[i]), ParseInt64(tokens[i + 1])));
    }
    return Protocol::Array(results);
}

std::string CommandEngine::HandleChunkGet(
    const Table& table,
    ChunkStore& store,
    const ParsedCommandView& command) {
    if (command.argc < 2 || command.argc > 5) {
        throw std::invalid_argument("CHUNKGET requires CHUNKGET <cx> <cy> [STATE] [EXTRA] [ZRLE]");
    }
    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);
    const auto form = ParseChunkForm(command, 2, command.argc, "CHUNKGET", /*allow_extra=*/true);
    if (form.extra && store.extra_max_block_bits() == 0U) {
        throw std::invalid_argument(ExtraDataDisabled(table.name()));
    }
    auto bytes = form.extra   ? store.GetChunkStateExtraBytes(chunk_x, chunk_y)
                 : form.state ? store.GetChunkStateBytes(chunk_x, chunk_y)
                              : store.GetChunkPayloadBytes(chunk_x, chunk_y);
    return Protocol::BulkBytes(form.zrle ? ZrleCompress(bytes) : bytes);
}

std::string CommandEngine::HandleChunkPut(
    ChunkStore& store,
    const ParsedCommandView& command,
    std::string_view payload) {
    const ChunkPutRequest put = ParseChunkPut(command);
    if (put.length != payload.size()) {
        // The connection reads exactly the declared length, so this only
        // trips for callers that bypass the wire path.
        throw std::invalid_argument("payload length does not match the bytes received");
    }
    const Geometry& geometry = store.geometry();
    const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
    const std::size_t expected = payload_bytes + (put.state ? PresenceBytes(geometry) : 0U);
    // With EXTRA the state is followed by an EXTRA section of 0 to
    // extra_max_chunk_bytes bytes.
    const std::size_t max_raw = put.extra ? expected + store.extra_max_chunk_bytes() : expected;
    const auto* data = reinterpret_cast<const std::uint8_t*>(payload.data());
    std::vector<std::uint8_t> raw;
    if (put.zrle) {
        try {
            const std::size_t declared =
                put.extra ? ZrleDeclaredSize(data, payload.size()) : expected;
            if (declared < expected || declared > max_raw) {
                throw std::runtime_error(
                    "it declares " + std::to_string(declared) + " bytes, expected " +
                    std::to_string(expected) +
                    (put.extra ? ".." + std::to_string(max_raw) : std::string()));
            }
            raw = ZrleDecompress(data, payload.size(), declared);
        } catch (const std::runtime_error& e) {
            throw std::invalid_argument(std::string("zrle payload is invalid: ") + e.what());
        }
    } else {
        if (payload.size() < expected || payload.size() > max_raw) {
            throw std::invalid_argument(
                "payload length " + std::to_string(payload.size()) + " does not match expected " +
                std::to_string(expected) +
                (put.extra ? ".." + std::to_string(max_raw) : std::string()) +
                " bytes for CHUNKPUT" + (put.state ? " STATE" : "") + (put.extra ? " EXTRA" : ""));
        }
        raw.assign(data, data + payload.size());
    }
    std::vector<std::uint8_t> packed_payload(raw.begin(), raw.begin() + static_cast<std::ptrdiff_t>(payload_bytes));
    std::vector<std::uint8_t> presence =
        put.state ? std::vector<std::uint8_t>(
                        raw.begin() + static_cast<std::ptrdiff_t>(payload_bytes),
                        raw.begin() + static_cast<std::ptrdiff_t>(expected))
                  : FullPresence(geometry);
    std::optional<ChunkExtra> extra;
    if (put.extra) {
        extra = ChunkExtra::Decode(
            raw.data() + expected, raw.size() - expected, geometry.ChunkBlockCount(),
            ExtraPadding::kClear);
    }

    if (put.has_if) {
        const auto result =
            extra.has_value()
                ? store.CasChunkStateBytes(
                      put.chunk_x, put.chunk_y, put.if_version, packed_payload, presence, *extra)
                : store.CasChunkStateBytes(
                      put.chunk_x, put.chunk_y, put.if_version, packed_payload, presence);
        if (!result.ok) {
            return Protocol::Error("VERSION_MISMATCH", "current=" + std::to_string(result.version));
        }
        return Protocol::Bulk(std::to_string(result.version));
    }
    const std::uint64_t version =
        extra.has_value()
            ? store.SetChunkStateBytes(put.chunk_x, put.chunk_y, packed_payload, presence, *extra)
            : store.SetChunkStateBytes(put.chunk_x, put.chunk_y, packed_payload, presence);
    return Protocol::Bulk(std::to_string(version));
}

std::string CommandEngine::HandleXGet(
    const Table& table,
    ChunkStore& store,
    const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("XGET requires 2 arguments: XGET <x> <y>");
    }
    if (store.extra_max_block_bits() == 0U) {
        throw std::invalid_argument(ExtraDataDisabled(table.name()));
    }
    const auto value = store.GetBlockExtra(ParseInt64(command.args[0]), ParseInt64(command.args[1]));
    return value.has_value() ? Protocol::BulkBytes(EncodeExtraReply(*value)) : Protocol::Null();
}

std::string CommandEngine::HandleXPut(
    const Table& table,
    ChunkStore& store,
    const ParsedCommandView& command,
    std::string_view payload) {
    const XPutRequest put = ParseXPut(command);
    if (put.length != payload.size()) {
        // The connection reads exactly the declared length, so this only
        // trips for callers that bypass the wire path.
        throw std::invalid_argument("payload length does not match the bytes received");
    }
    // PlanPayload checked this against the options the table had then; the
    // store checks its own limits again under the chunk lock.
    if (auto problem = CheckXPut(put, table.Info()); !problem.empty()) {
        throw std::invalid_argument(problem);
    }
    const auto* data = reinterpret_cast<const std::uint8_t*>(payload.data());
    (void)store.PutBlockExtra(
        put.x, put.y,
        MakeExtraValue(
            static_cast<std::uint32_t>(put.bit_length),
            std::vector<std::uint8_t>(data, data + payload.size()), ExtraPadding::kClear));
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleXDel(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("XDEL requires 2 arguments: XDEL <x> <y>");
    }
    (void)store.DeleteBlockExtra(ParseInt64(command.args[0]), ParseInt64(command.args[1]));
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleChunkArea(
    ChunkStore& store,
    const ParsedCommandView& command,
    bool radius) {
    const std::size_t coords = radius ? 3U : 4U;
    if (command.argc < coords || command.argc > coords + 2U) {
        throw std::invalid_argument(
            radius ? "CHUNKRADIUS requires CHUNKRADIUS <cx> <cy> <radius_chunks> [STATE] [ZRLE]"
                   : "CHUNKRANGE requires CHUNKRANGE <cx0> <cy0> <cx1> <cy1> [STATE] [ZRLE]");
    }
    const auto form =
        ParseChunkForm(command, coords, command.argc, radius ? "CHUNKRADIUS" : "CHUNKRANGE");
    const auto entries =
        radius ? store.ReadChunkRadius(
                     ParseInt64(command.args[0]), ParseInt64(command.args[1]),
                     ParseInt64(command.args[2]))
               : store.ReadChunkRange(
                     ParseInt64(command.args[0]), ParseInt64(command.args[1]),
                     ParseInt64(command.args[2]), ParseInt64(command.args[3]));

    // Each populated chunk is a "<cx> <cy>" item followed by its bytes, as
    // CHUNKGET with the same options returns them.
    std::vector<std::string> items;
    items.reserve(entries.size() * 2U);
    for (const auto& entry : entries) {
        items.push_back(std::to_string(entry.coord.x) + " " + std::to_string(entry.coord.y));
        std::vector<std::uint8_t> body = entry.payload;
        if (form.state) {
            body.insert(body.end(), entry.presence_bitmap.begin(), entry.presence_bitmap.end());
        }
        if (form.zrle) {
            body = ZrleCompress(body);
        }
        items.emplace_back(body.begin(), body.end());
    }
    return Protocol::Array(items);
}

std::string CommandEngine::HandleChunkBatch(ChunkStore& store, std::string_view line) {
    const auto tokens = ParseVarTokens(line);
    if (tokens.size() < 6) {
        throw std::invalid_argument(
            "CHUNKBATCH requires CHUNKBATCH <cx> <cy> [IF <version>] then SET <x> <y> <bits>, "
            "UNSET <x> <y>, XPUT <x> <y> <bits> and/or XDEL <x> <y> operations");
    }

    const std::int64_t chunk_x = ParseInt64(tokens[1]);
    const std::int64_t chunk_y = ParseInt64(tokens[2]);
    std::size_t i = 3;
    const bool has_expected_version = Protocol::CommandEquals(tokens[i], "IF");
    std::uint64_t expected_version = 0;
    if (has_expected_version) {
        if (i + 1 >= tokens.size()) {
            throw std::invalid_argument("IF requires a version");
        }
        expected_version = ParseUint64(tokens[i + 1]);
        i += 2;
    }

    std::vector<ChunkBatchOp> ops;
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
        } else if (Protocol::CommandEquals(tokens[i], "XPUT")) {
            if (i + 3 >= tokens.size()) {
                throw std::invalid_argument("XPUT operation requires <x> <y> <bits>");
            }
            ops.push_back(ChunkBatchOp{
                .x = ParseInt64(tokens[i + 1]),
                .y = ParseInt64(tokens[i + 2]),
                .kind = ChunkBatchOpKind::kExtraPut,
                .extra = ExtraValueFromBits(tokens[i + 3]),
            });
            i += 4;
        } else if (Protocol::CommandEquals(tokens[i], "XDEL")) {
            if (i + 2 >= tokens.size()) {
                throw std::invalid_argument("XDEL operation requires <x> <y>");
            }
            ops.push_back(ChunkBatchOp{
                .x = ParseInt64(tokens[i + 1]),
                .y = ParseInt64(tokens[i + 2]),
                .kind = ChunkBatchOpKind::kExtraDel,
            });
            i += 3;
        } else {
            throw std::invalid_argument(
                "batch operations must start with SET, UNSET, XPUT or XDEL, got: " +
                std::string(tokens[i]));
        }
    }
    if (ops.empty()) {
        throw std::invalid_argument("CHUNKBATCH requires at least one operation");
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

std::size_t CommandEngine::AuthFailureTrackedSourcesForTests() {
    std::lock_guard lock(auth_failures_mutex_);
    return auth_failures_by_ip_.size();
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

std::string CommandEngine::HandleChunkExists(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("CHUNKEXISTS requires 2 arguments: CHUNKEXISTS <cx> <cy>");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);

    return Protocol::SimpleString(store.ChunkExists(chunk_x, chunk_y) ? "1" : "0");
}

std::string CommandEngine::HandleInfo(const Table& table, ChunkStore& store) const {
    const auto runtime_stats = store.RuntimeStats();
    std::string info;
    info += "table=" + table.name() + "\n";
    info += "tables=" + std::to_string(catalog_->TableCount()) + "\n";
    info += "access_mode=" + std::string(AccessModeName(store.access_mode())) + "\n";
    info += "chunk_lock_mode=" + std::string(ChunkLockModeName()) + "\n";
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

std::string CommandEngine::HandleChunkVersion(ChunkStore& store, const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("CHUNKVER requires 2 arguments: CHUNKVER <cx> <cy>");
    }
    const std::uint64_t version =
        store.GetChunkVersion(ParseInt64(command.args[0]), ParseInt64(command.args[1]));
    return Protocol::Bulk(std::to_string(version));
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

}  // namespace chunkdb
