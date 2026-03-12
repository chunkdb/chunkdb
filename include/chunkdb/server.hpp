#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "chunkdb/engine.hpp"

namespace chunkdb {

struct ServerConfig {
    std::string host = "127.0.0.1";
    std::uint16_t port = 6752;
    std::size_t max_line_bytes = 65536;

    bool tls_enabled = false;
    std::string tls_cert_path;
    std::string tls_key_path;
};

class ChunkServer {
  public:
    ChunkServer(ServerConfig config, std::shared_ptr<CommandEngine> engine);
    ~ChunkServer();

    void Run();
    void Stop();

  private:
    ServerConfig config_;
    std::shared_ptr<CommandEngine> engine_;
    std::atomic<bool> running_;

#ifdef _WIN32
    std::uintptr_t listen_socket_;
#else
    int listen_socket_;
#endif

#ifdef CHUNKDB_WITH_OPENSSL
    struct ssl_ctx_st* tls_context_;
#endif

    void HandleClient(
#ifdef _WIN32
        std::uintptr_t client_socket
#else
        int client_socket
#endif
    );
};

}  // namespace chunkdb
