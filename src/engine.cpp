#include "chunkdb/engine.hpp"

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
        const ParsedCommand command = Protocol::ParseLine(line);

        if (command.name == "PING") {
            return Protocol::SimpleString("PONG");
        }

        if (command.name == "AUTH") {
            return HandleAuth(session, command.args);
        }

        if (command.name == "QUIT") {
            session.close_after_reply = true;
            return Protocol::SimpleString("BYE");
        }

        if (IsAuthRequired() && !session.authenticated) {
            return Protocol::Error("AUTH_REQUIRED", "use AUTH <token>");
        }

        if (command.name == "GET") {
            return HandleGet(command.args);
        }
        if (command.name == "SET") {
            return HandleSet(command.args);
        }
        if (command.name == "CHUNK") {
            return HandleChunk(command.args);
        }
        if (command.name == "CHUNKBIN") {
            return HandleChunkBinary(command.args);
        }
        if (command.name == "INFO") {
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

std::string CommandEngine::HandleAuth(SessionState& session, const std::vector<std::string>& args) {
    if (args.size() != 1) {
        throw std::invalid_argument("AUTH requires exactly 1 argument");
    }

    if (!IsAuthRequired()) {
        session.authenticated = true;
        session.failed_auth_attempts = 0;
        return Protocol::SimpleString("OK");
    }

    if (args[0] == config_.auth_token) {
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

std::string CommandEngine::HandleGet(const std::vector<std::string>& args) {
    if (args.size() != 2) {
        throw std::invalid_argument("GET requires 2 arguments: GET <x> <y>");
    }

    const std::int64_t x = ParseInt64(args[0]);
    const std::int64_t y = ParseInt64(args[1]);

    const std::string bits = store_->GetBlockBits(x, y);
    return Protocol::Bulk(bits);
}

std::string CommandEngine::HandleSet(const std::vector<std::string>& args) {
    if (args.size() != 3) {
        throw std::invalid_argument("SET requires 3 arguments: SET <x> <y> <bits>");
    }

    const std::int64_t x = ParseInt64(args[0]);
    const std::int64_t y = ParseInt64(args[1]);
    const std::string& bits = args[2];

    store_->SetBlockBits(x, y, bits);
    return Protocol::SimpleString("OK");
}

std::string CommandEngine::HandleChunk(const std::vector<std::string>& args) {
    if (args.size() != 2) {
        throw std::invalid_argument("CHUNK requires 2 arguments: CHUNK <cx> <cy>");
    }

    const std::int64_t chunk_x = ParseInt64(args[0]);
    const std::int64_t chunk_y = ParseInt64(args[1]);

    const std::string bits = store_->GetChunkBits(chunk_x, chunk_y);
    return Protocol::Bulk(bits);
}

std::string CommandEngine::HandleChunkBinary(const std::vector<std::string>& args) {
    if (args.size() != 2) {
        throw std::invalid_argument("CHUNKBIN requires 2 arguments: CHUNKBIN <cx> <cy>");
    }

    const std::int64_t chunk_x = ParseInt64(args[0]);
    const std::int64_t chunk_y = ParseInt64(args[1]);

    const auto payload = store_->GetChunkPayloadBytes(chunk_x, chunk_y);
    return Protocol::BulkBytes(payload);
}

std::string CommandEngine::HandleInfo() const {
    const auto& cfg = store_->geometry().config();
    std::string info;
    info += "chunkdb_version=1\n";
    info += "block_bits=" + std::to_string(cfg.block_bits) + "\n";
    info += "chunk_width_blocks=" + std::to_string(cfg.chunk_width_blocks) + "\n";
    info += "chunk_height_blocks=" + std::to_string(cfg.chunk_height_blocks) + "\n";
    info += "large_chunk_width_chunks=" + std::to_string(cfg.large_chunk_width_chunks) + "\n";
    info += "large_chunk_height_chunks=" + std::to_string(cfg.large_chunk_height_chunks) + "\n";
    info += "durability_mode=" + std::string(DurabilityModeName(store_->durability_mode())) + "\n";
    return Protocol::Bulk(info);
}

std::int64_t CommandEngine::ParseInt64(const std::string& token) {
    std::size_t consumed = 0;
    const std::int64_t value = std::stoll(token, &consumed, 10);
    if (consumed != token.size()) {
        throw std::invalid_argument("invalid integer: " + token);
    }
    return value;
}

bool CommandEngine::IsAuthRequired() const noexcept {
    return config_.require_auth && !config_.auth_token.empty();
}

}  // namespace chunkdb
