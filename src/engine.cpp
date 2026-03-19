#include "chunkdb/engine.hpp"

#include <charconv>
#include <stdexcept>

#include "chunkdb/protocol.hpp"

namespace chunkdb {

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
        if (Protocol::CommandEquals(command.name, "SET")) {
            return HandleSet(command);
        }
        if (Protocol::CommandEquals(command.name, "CHUNK")) {
            return HandleChunk(command);
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
        return Protocol::Error("INTERNAL", e.what());
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

    if (command.args[0] == config_.auth_token) {
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

std::string CommandEngine::HandleSet(const ParsedCommandView& command) {
    if (command.argc != 3) {
        throw std::invalid_argument("SET requires 3 arguments: SET <x> <y> <bits>");
    }

    const std::int64_t x = ParseInt64(command.args[0]);
    const std::int64_t y = ParseInt64(command.args[1]);

    store_->SetBlockBits(x, y, command.args[2]);
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleChunk(const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("CHUNK requires 2 arguments: CHUNK <cx> <cy>");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);

    const std::string bits = store_->GetChunkBits(chunk_x, chunk_y);
    return Protocol::Bulk(bits);
}

std::string CommandEngine::HandleChunkBinary(const ParsedCommandView& command) {
    if (command.argc != 2) {
        throw std::invalid_argument("CHUNKBIN requires 2 arguments: CHUNKBIN <cx> <cy>");
    }

    const std::int64_t chunk_x = ParseInt64(command.args[0]);
    const std::int64_t chunk_y = ParseInt64(command.args[1]);

    const auto payload = store_->GetChunkPayloadBytes(chunk_x, chunk_y);
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

}  // namespace chunkdb
