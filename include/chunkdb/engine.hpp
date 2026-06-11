#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/protocol.hpp"

namespace chunkdb {

struct EngineConfig {
    std::string auth_token;
    bool require_auth = true;
    std::size_t max_auth_failures = 5;
    std::size_t max_auth_failures_per_ip = 5;
    std::size_t auth_failure_delay_ms = 50;
    std::size_t auth_failure_ban_ms = 1000;
};

struct SessionState {
    std::string remote_address;
    bool authenticated = false;
    std::size_t failed_auth_attempts = 0;
    bool close_after_reply = false;
};

class CommandEngine {
  public:
    CommandEngine(EngineConfig config, std::shared_ptr<ChunkStore> store);

    [[nodiscard]] std::string Execute(SessionState& session, std::string_view line);

  private:
    struct IpAuthFailureState {
        std::size_t failures = 0;
        std::chrono::steady_clock::time_point banned_until{};
    };

    EngineConfig config_;
    std::shared_ptr<ChunkStore> store_;
    std::mutex auth_failures_mutex_;
    std::unordered_map<std::string, IpAuthFailureState> auth_failures_by_ip_;

    [[nodiscard]] std::string HandleAuth(SessionState& session, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleExists(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleGet(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleSet(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleUnset(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkExists(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunk(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkSet(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkBinary(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleInfo() const;
    [[nodiscard]] std::string HandleMSet(std::string_view line);
    [[nodiscard]] std::string HandleMGet(std::string_view line);

    static std::int64_t ParseInt64(std::string_view token);
    [[nodiscard]] bool IsAuthRequired() const noexcept;
};

}  // namespace chunkdb
