#include "chunkdb/engine.hpp"

#include <charconv>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/protocol.hpp"

namespace chunkdb {

namespace {

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

CommandEngine::CommandEngine(EngineConfig config, std::shared_ptr<ChunkStore> store)
    : config_(std::move(config)), store_(std::move(store)) {
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
    try {
        // MSET/MGET accept variable numbers of arguments that exceed ParseLineView's
        // 8-arg limit, so intercept them before calling ParseLineView.
        const auto name = ExtractCommandName(line);
        if (Protocol::CommandEquals(name, "MSET") || Protocol::CommandEquals(name, "MGET")) {
            if (IsAuthRequired() && !session.authenticated) {
                return Protocol::Error("AUTH_REQUIRED", "use AUTH <token>");
            }
            if (Protocol::CommandEquals(name, "MSET")) {
                return HandleMSet(line);
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
        if (Protocol::CommandEquals(command.name, "INFO")) {
            return HandleInfo();
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

    if (ConstantTimeEqual(command.args[0], config_.auth_token)) {
        session.authenticated = true;
        session.failed_auth_attempts = 0;
        return Protocol::SimpleString("OK");
    }

    ++session.failed_auth_attempts;
    if (session.failed_auth_attempts >= config_.max_auth_failures) {
        session.close_after_reply = true;
    }
    return Protocol::Error("AUTH_FAILED", "invalid token");
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
    return Protocol::Bulk(info);
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
