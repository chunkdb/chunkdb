#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <thread>
#include <vector>

#include "chunkdb/engine.hpp"

// OpenSSL's SSL_CTX. Declared here so the class layout does not depend on
// whether a translation unit is built with CHUNKDB_WITH_OPENSSL.
struct ssl_ctx_st;

namespace chunkdb {
struct ServerConnection;
class FeedIo;
struct FeedDeliveryTestHook;

struct ServerConfig {
    std::string host = "127.0.0.1";
    std::uint16_t port = 4242;
    std::size_t max_line_bytes = 65536;
    std::size_t worker_threads = 4;
    std::size_t client_io_timeout_ms = 5000;
    std::size_t idle_connection_timeout_ms = 60000;
    std::size_t max_pending_clients = 1024;
    // Connections from one source (an IPv4 address, an IPv6 /64) that may
    // hold a worker at once before HELLO succeeds (TLS handshake included);
    // more get -ERR BUSY. 0: no limit.
    std::size_t max_handshakes_per_ip = 0;

    std::size_t feed_buffer_bytes = kDefaultFeedBufferBytes;
    std::size_t feed_linger_ms = 30000;
    std::size_t max_watches = 64;

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

    // Connections from `source` (a SourceAddressKey) that hold a worker
    // before HELLO, counted while max_handshakes_per_ip is set.
    [[nodiscard]] std::size_t HandshakesInProgressForTests(const std::string& source);

  private:
    friend class FeedIo;
    friend struct FeedDeliveryTestAccess;
    struct PendingClient {
        std::shared_ptr<ServerConnection> resumed{};
#ifdef _WIN32
        std::uintptr_t socket = 0;
#else
        int socket = -1;
#endif
        std::chrono::steady_clock::time_point accepted_at{};
    };

    ServerConfig config_;
    std::shared_ptr<CommandEngine> engine_;
    std::atomic<bool> running_;
    // Isolated schedule observer; installed hooks outlive server shutdown.
    std::atomic<FeedDeliveryTestHook*> delivery_test_hook_{nullptr};
    std::stop_source backup_stop_;

#ifdef _WIN32
    std::uintptr_t listen_socket_;
#else
    int listen_socket_;
#endif

    // Null unless the server was built with TLS support and TLS is enabled.
    ssl_ctx_st* tls_context_ = nullptr;

#ifdef _WIN32
    std::queue<PendingClient> pending_clients_;
    std::vector<std::uintptr_t> active_clients_;
#else
    std::queue<PendingClient> pending_clients_;
    std::vector<int> active_clients_;
#endif
    std::mutex lifecycle_mutex_;
    std::mutex pending_clients_mutex_;
    std::mutex active_clients_mutex_;
    std::condition_variable pending_clients_cv_;
    std::vector<std::thread> workers_;
    std::atomic<bool> pending_queue_overload_warned_{false};
    // Workers held by connections before HELLO, by source address key.
    std::mutex handshakes_mutex_;
    std::unordered_map<std::string, std::size_t> handshakes_;
    std::atomic<bool> handshake_limit_warned_{false};

    [[nodiscard]] bool TryAcquireHandshake(const std::string& source);
    void ReleaseHandshake(const std::string& source) noexcept;

    void RequestBackupStop();
    void StartWorkers();
    void JoinWorkers();
    void WorkerLoop();

    bool HandleClient(
#ifdef _WIN32
        std::uintptr_t client_socket
#else
        int client_socket
#endif
        , ServerConnection& connection
    );
    bool HandOff(ServerConnection& connection);
    void ReturnClient(std::shared_ptr<ServerConnection> connection);
    void CloseClient(ServerConnection& connection);
    std::shared_ptr<FeedIo> feed_io_;
    std::shared_ptr<FeedIo> FeedIoHandle();
    std::atomic<std::size_t> watch_count_{0};
};

}  // namespace chunkdb
