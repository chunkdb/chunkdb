#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/engine.hpp"

namespace chunkdb {

struct ServerConfig {
    std::string host = "127.0.0.1";
    std::uint16_t port = 4242;
    std::size_t max_line_bytes = 65536;
    std::size_t worker_threads = 4;

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

#ifdef _WIN32
    std::queue<std::uintptr_t> pending_clients_;
    std::vector<std::uintptr_t> active_clients_;
#else
    std::queue<int> pending_clients_;
    std::vector<int> active_clients_;
#endif
    std::mutex lifecycle_mutex_;
    std::mutex pending_clients_mutex_;
    std::mutex active_clients_mutex_;
    std::condition_variable pending_clients_cv_;
    std::vector<std::thread> workers_;

    void StartWorkers();
    void JoinWorkers();
    void WorkerLoop();

    void HandleClient(
#ifdef _WIN32
        std::uintptr_t client_socket
#else
        int client_socket
#endif
    );
};

}  // namespace chunkdb
