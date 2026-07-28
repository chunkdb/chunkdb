#include "chunkdb/server.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>

#include "chunkdb/logging.hpp"
#include "chunkdb/protocol.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <unistd.h>
#endif

#ifdef CHUNKDB_WITH_OPENSSL
#include <openssl/err.h>
#include <openssl/ssl.h>
#endif

#include "server_socket.hpp"
#include "server_io.hpp"
#include "server_tls.hpp"

namespace chunkdb {

using namespace server_detail;

void SetServerTimeoutConfigFailpointForTests(
    std::size_t send_failures,
    std::size_t recv_failures) noexcept {
    g_test_send_timeout_config_failures.store(send_failures, std::memory_order_relaxed);
    g_test_recv_timeout_config_failures.store(recv_failures, std::memory_order_relaxed);
}

void ResetServerTimeoutConfigCountersForTests() noexcept {
    g_test_recv_timeout_config_calls.store(0, std::memory_order_relaxed);
}

std::uint64_t ServerRecvTimeoutConfigCallsForTests() noexcept {
    return g_test_recv_timeout_config_calls.load(std::memory_order_relaxed);
}

ChunkServer::ChunkServer(ServerConfig config, std::shared_ptr<CommandEngine> engine)
    : config_(std::move(config)),
      engine_(std::move(engine)),
      running_(false),
      listen_socket_(kInvalidSocket)
#ifdef CHUNKDB_WITH_OPENSSL
      ,
      tls_context_(nullptr)
#endif
{
    if (!engine_) {
        throw std::invalid_argument("engine must not be null");
    }
    if (config_.max_line_bytes == 0) {
        throw std::invalid_argument("max_line_bytes must be > 0");
    }
    if (config_.worker_threads == 0) {
        throw std::invalid_argument("worker_threads must be > 0");
    }
    if (config_.client_io_timeout_ms == 0) {
        throw std::invalid_argument("client_io_timeout_ms must be > 0");
    }
    if (config_.idle_connection_timeout_ms == 0) {
        throw std::invalid_argument("idle_connection_timeout_ms must be > 0");
    }
    if (config_.max_pending_clients == 0) {
        throw std::invalid_argument("max_pending_clients must be > 0");
    }

#ifndef CHUNKDB_WITH_OPENSSL
    if (config_.tls_enabled) {
        throw std::invalid_argument("TLS requested but build does not include OpenSSL");
    }
#else
    if (config_.tls_enabled) {
        if (config_.tls_cert_path.empty() || config_.tls_key_path.empty()) {
            throw std::invalid_argument("TLS requires both tls_cert_path and tls_key_path");
        }

        SSL_load_error_strings();
        OpenSSL_add_ssl_algorithms();

        tls_context_ = BuildTlsContext(config_);
    }
#endif
}

ChunkServer::~ChunkServer() {
    Stop();
#ifdef CHUNKDB_WITH_OPENSSL
    if (tls_context_ != nullptr) {
        SSL_CTX_free(tls_context_);
        tls_context_ = nullptr;
    }
#endif
}

void ChunkServer::StartWorkers() {
    workers_.reserve(config_.worker_threads);
    for (std::size_t i = 0; i < config_.worker_threads; ++i) {
        workers_.emplace_back(&ChunkServer::WorkerLoop, this);
    }
}

void ChunkServer::JoinWorkers() {
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
}

void ChunkServer::Run() {
    const auto run_started = std::chrono::steady_clock::now();
#ifdef _WIN32
    WSADATA wsa_data;
    if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
        LogMessage(
            LogLevel::kError,
            LogComponent::kServer,
            "server runtime initialization failed",
            {{"error", "WSAStartup failed"}});
        throw std::runtime_error("WSAStartup failed");
    }
#endif

    try {
        const SocketHandle listen_socket = CreateListenSocket(config_.host, config_.port);
        {
            std::lock_guard lock(lifecycle_mutex_);
            listen_socket_ = static_cast<decltype(listen_socket_)>(listen_socket);
        }
        running_.store(true);

        StartWorkers();
        LogMessage(
            LogLevel::kInfo,
            LogComponent::kServer,
            "ready to accept connections",
            {
                {"protocol", config_.tls_enabled ? "tls" : "tcp"},
                {"host", config_.host},
                {"port", std::to_string(config_.port)},
                {"tls", config_.tls_enabled ? "on" : "off"},
                {"workers", std::to_string(config_.worker_threads)},
            });

        while (running_.load()) {
#ifdef _WIN32
            WSAPOLLFD poll_fd{};
            poll_fd.fd = listen_socket;
            poll_fd.events = POLLRDNORM;
            const int ready = WSAPoll(&poll_fd, 1, 200);
#else
            pollfd poll_fd{};
            poll_fd.fd = listen_socket;
            poll_fd.events = POLLIN;
            const int ready = poll(&poll_fd, 1, 200);
#endif
            if (ready == 0) {
                continue;
            }
            if (ready < 0) {
                if (IsSocketInterruptedError(CurrentSocketErrorCode())) {
                    continue;
                }
                if (!running_.load()) {
                    break;
                }
                continue;
            }

            sockaddr_storage client_address;
#ifdef _WIN32
            int client_size = sizeof(client_address);
#else
            socklen_t client_size = sizeof(client_address);
#endif

            SocketHandle client_socket = accept(
                listen_socket,
                reinterpret_cast<sockaddr*>(&client_address),
                &client_size);

            if (client_socket == kInvalidSocket) {
                if (IsSocketInterruptedError(CurrentSocketErrorCode())) {
                    continue;
                }
                if (!running_.load()) {
                    break;
                }
                continue;
            }

            std::string nodelay_error;
            if (!EnableTcpNoDelay(client_socket, &nodelay_error)) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kServer,
                    "failed to set TCP_NODELAY",
                    {{"error", nodelay_error}});
            }

            std::string timeout_error;
            const bool send_timeout_ok =
                !ConsumeTestFailureBudget(&g_test_send_timeout_config_failures) &&
                ConfigureSocketSendTimeout(client_socket, config_.client_io_timeout_ms, &timeout_error);
            if (!send_timeout_ok) {
                if (timeout_error.empty()) {
                    timeout_error = "injected timeout config failure";
                }
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kServer,
                    "failed to configure client send timeout; closing connection",
                    {
                        {"timeout_ms", std::to_string(config_.client_io_timeout_ms)},
                        {"error", timeout_error},
                    });
                CloseSocket(client_socket);
                continue;
            }

            bool enqueued = false;
            std::size_t pending_after = 0;
            const auto accepted_at = std::chrono::steady_clock::now();
            {
                std::lock_guard lock(pending_clients_mutex_);
                while (!pending_clients_.empty() &&
                       PendingClientExpired(
                           pending_clients_.front().accepted_at,
                           accepted_at,
                           config_.idle_connection_timeout_ms)) {
                    CloseSocket(static_cast<SocketHandle>(pending_clients_.front().socket));
                    pending_clients_.pop();
                }
                if (pending_clients_.size() < config_.max_pending_clients) {
                    pending_clients_.push(
                        PendingClient{
                            .socket = static_cast<decltype(listen_socket_)>(client_socket),
                            .accepted_at = accepted_at,
                        });
                    pending_after = pending_clients_.size();
                    enqueued = true;
                } else {
                    pending_after = pending_clients_.size();
                }
            }
            if (enqueued) {
                engine_->metrics()->SetPendingConnections(pending_after);
                if (pending_after < config_.max_pending_clients) {
                    pending_queue_overload_warned_.store(false, std::memory_order_relaxed);
                }
                pending_clients_cv_.notify_one();
                continue;
            }

            engine_->metrics()->CountConnectionRejected();
#ifdef CHUNKDB_WITH_OPENSSL
            if (!config_.tls_enabled) {
                SendPlainBusyResponse(client_socket, config_.client_io_timeout_ms);
            }
#else
            SendPlainBusyResponse(client_socket, config_.client_io_timeout_ms);
#endif
            CloseSocket(client_socket);
            if (!pending_queue_overload_warned_.exchange(true, std::memory_order_relaxed)) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kServer,
                    "pending client queue full; rejecting new connections",
                    {
                        {"max_pending_clients", std::to_string(config_.max_pending_clients)},
                        {"pending_clients", std::to_string(pending_after)},
                    });
            }
        }

        running_.store(false);
        {
            std::lock_guard lock(lifecycle_mutex_);
            listen_socket_ = kInvalidSocket;
        }
        pending_clients_cv_.notify_all();
        JoinWorkers();

        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - run_started);
        LogMessage(
            LogLevel::kInfo,
            LogComponent::kServer,
            "shutdown complete",
            {{"elapsed_ms", std::to_string(elapsed.count())}});
    } catch (const std::exception& e) {
        LogMessage(
            LogLevel::kError,
            LogComponent::kServer,
            "server run loop failed",
            {{"error", e.what()}});
        running_.store(false);
        pending_clients_cv_.notify_all();
        JoinWorkers();
#ifdef _WIN32
        WSACleanup();
#endif
        throw;
    }

#ifdef _WIN32
    WSACleanup();
#endif
}

void ChunkServer::Stop() {
    const bool was_running = running_.exchange(false);

    SocketHandle listen_socket = kInvalidSocket;
    {
        std::lock_guard lock(lifecycle_mutex_);
        if (listen_socket_ != kInvalidSocket) {
            listen_socket = static_cast<SocketHandle>(listen_socket_);
            listen_socket_ = kInvalidSocket;
        }
    }

    std::size_t pending_count = 0;
    std::size_t active_count = 0;

    if (listen_socket != kInvalidSocket) {
        ShutdownSocket(listen_socket);
        CloseSocket(listen_socket);
    }

    pending_clients_cv_.notify_all();

    {
        std::lock_guard lock(pending_clients_mutex_);
        pending_count = pending_clients_.size();
        while (!pending_clients_.empty()) {
            CloseSocket(static_cast<SocketHandle>(pending_clients_.front().socket));
            pending_clients_.pop();
        }
    }

    {
        std::lock_guard lock(active_clients_mutex_);
        active_count = active_clients_.size();
        for (const auto client_socket : active_clients_) {
            ShutdownSocket(static_cast<SocketHandle>(client_socket));
        }
        active_clients_.clear();
    }

    if (was_running || listen_socket != kInvalidSocket || pending_count > 0 || active_count > 0) {
        LogMessage(
            LogLevel::kInfo,
            LogComponent::kServer,
            "stop requested",
            {
                {"pending_clients", std::to_string(pending_count)},
                {"active_clients", std::to_string(active_count)},
            });
        LogMessage(
            LogLevel::kInfo,
            LogComponent::kServer,
            "workers and connections draining",
            {
                {"pending_clients", std::to_string(pending_count)},
                {"active_clients", std::to_string(active_count)},
            });
    }
}

void ChunkServer::WorkerLoop() {
    while (true) {
        decltype(listen_socket_) client_socket = kInvalidSocket;

        {
            std::unique_lock lock(pending_clients_mutex_);
            pending_clients_cv_.wait(lock, [&]() {
                return !running_.load() || !pending_clients_.empty();
            });

            if (pending_clients_.empty()) {
                if (!running_.load()) {
                    return;
                }
                continue;
            }

            while (!pending_clients_.empty()) {
                const PendingClient pending_client = pending_clients_.front();
                pending_clients_.pop();
                if (pending_clients_.size() < config_.max_pending_clients) {
                    pending_queue_overload_warned_.store(false, std::memory_order_relaxed);
                }

                if (PendingClientExpired(
                        pending_client.accepted_at,
                        std::chrono::steady_clock::now(),
                        config_.idle_connection_timeout_ms)) {
                    CloseSocket(static_cast<SocketHandle>(pending_client.socket));
                    continue;
                }

                client_socket = pending_client.socket;
                break;
            }

            engine_->metrics()->SetPendingConnections(pending_clients_.size());

            if (client_socket == kInvalidSocket) {
                if (!running_.load()) {
                    return;
                }
                continue;
            }
        }

        {
            std::lock_guard lock(active_clients_mutex_);
            active_clients_.push_back(client_socket);
        }

        engine_->metrics()->IncActiveConnections();
        HandleClient(client_socket);
        engine_->metrics()->DecActiveConnections();

        {
            std::lock_guard lock(active_clients_mutex_);
            const auto it = std::find(active_clients_.begin(), active_clients_.end(), client_socket);
            if (it != active_clients_.end()) {
                active_clients_.erase(it);
            }
        }
    }
}

}  // namespace chunkdb
