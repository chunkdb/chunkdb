#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/protocol.hpp"

namespace chunkdb {

struct EngineConfig {
    std::string auth_token;
    bool require_auth = true;
    std::size_t max_auth_failures = 5;
};

struct SessionState {
    bool authenticated = false;
    std::size_t failed_auth_attempts = 0;
    bool close_after_reply = false;
};

class CommandEngine {
  public:
    CommandEngine(EngineConfig config, std::shared_ptr<ChunkStore> store);

    [[nodiscard]] std::string Execute(SessionState& session, std::string_view line);

  private:
    EngineConfig config_;
    std::shared_ptr<ChunkStore> store_;

    [[nodiscard]] std::string HandleAuth(SessionState& session, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleGet(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleSet(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunk(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkBinary(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleInfo() const;

    static std::int64_t ParseInt64(std::string_view token);
    [[nodiscard]] bool IsAuthRequired() const noexcept;
};

}  // namespace chunkdb
