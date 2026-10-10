#include "chunkdb/server.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <csignal>
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
#include "server_feed.hpp"

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
      listen_socket_(kInvalidSocket) {
    if (!engine_) {
        throw std::invalid_argument("engine must not be null");
    }
    if (config_.max_line_bytes == 0) {
        throw std::invalid_argument("max_line_bytes must be > 0");
    }
    if (config_.worker_threads == 0) {
        throw std::invalid_argument("worker_threads must be > 0");
    }

    // Deadlines are steady_clock time points; a day keeps them far from
    // overflow on every platform.
    constexpr std::size_t kMaxTimeoutMs = 24U * 60U * 60U * 1000U;
    if (config_.client_io_timeout_ms == 0 || config_.client_io_timeout_ms > kMaxTimeoutMs) {
        throw std::invalid_argument("client_io_timeout_ms must be between 1 and 86400000");
    }
    if (config_.idle_connection_timeout_ms == 0 || config_.idle_connection_timeout_ms > kMaxTimeoutMs) {
        throw std::invalid_argument("idle_connection_timeout_ms must be between 1 and 86400000");
    }
    if (config_.feed_buffer_bytes == 0 || config_.max_watches == 0) {
        throw std::invalid_argument("feed_buffer_bytes and max_watches must be positive");
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

std::shared_ptr<FeedIo> ChunkServer::FeedIoHandle() {
    std::lock_guard lock(lifecycle_mutex_);
    return feed_io_;
}

void ChunkServer::StartWorkers() {
    auto io = std::make_shared<FeedIo>(*this);
    { std::lock_guard lock(lifecycle_mutex_); feed_io_ = io; }
    io->Start();
    workers_.reserve(config_.worker_threads);
    for (std::size_t i = 0; i < config_.worker_threads; ++i) {
        workers_.emplace_back(&ChunkServer::WorkerLoop, this);
    }
}

void ChunkServer::JoinWorkers() {
    if (auto io = FeedIoHandle()) io->Stop();
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
    { std::lock_guard lock(lifecycle_mutex_); feed_io_.reset(); }
}

void ChunkServer::Run() {
    const auto run_started = std::chrono::steady_clock::now();
#ifndef _WIN32
    // A write to a connection the peer has reset raises SIGPIPE, whose
    // default action ends the process; TLS writes go through OpenSSL's
    // write(), which takes no flag against it. Ignored unless the embedding
    // program already chose a disposition.
    struct sigaction sigpipe_action {};
    if (sigaction(SIGPIPE, nullptr, &sigpipe_action) == 0 && sigpipe_action.sa_handler == SIG_DFL) {
        std::signal(SIGPIPE, SIG_IGN);
    }
#endif
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
            backup_stop_ = std::stop_source{};
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

#if defined(SO_NOSIGPIPE)
            // Where send() has no MSG_NOSIGNAL (macOS), per socket.
            {
                const int on = 1;
                (void)setsockopt(client_socket, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
            }
#endif
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
                while (!pending_clients_.empty() && !pending_clients_.front().resumed &&
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
        RequestBackupStop();
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
        RequestBackupStop();
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

bool ChunkServer::TryAcquireHandshake(const std::string& source) {
    std::lock_guard lock(handshakes_mutex_);
    auto& count = handshakes_[source];
    if (count >= config_.max_handshakes_per_ip) {
        if (count == 0U) {
            handshakes_.erase(source);
        }
        return false;
    }
    ++count;
    return true;
}

std::size_t ChunkServer::HandshakesInProgressForTests(const std::string& source) {
    std::lock_guard lock(handshakes_mutex_);
    const auto it = handshakes_.find(source);
    return it == handshakes_.end() ? 0U : it->second;
}

void ChunkServer::ReleaseHandshake(const std::string& source) noexcept {
    std::lock_guard lock(handshakes_mutex_);
    const auto it = handshakes_.find(source);
    if (it != handshakes_.end() && --it->second == 0U) {
        handshakes_.erase(it);
    }
}

void ChunkServer::RequestBackupStop() {
    std::stop_source source(std::nostopstate);
    { std::lock_guard lock(lifecycle_mutex_); source = backup_stop_; }
    source.request_stop();
}

void ChunkServer::Stop() {
    const bool was_running = running_.exchange(false);
    RequestBackupStop();

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
    if (auto io = FeedIoHandle()) io->Wake();

    {
        std::lock_guard lock(pending_clients_mutex_);
        pending_count = pending_clients_.size();
        while (!pending_clients_.empty()) {
            auto client = std::move(pending_clients_.front());
            pending_clients_.pop();
            if (client.resumed) CloseClient(*client.resumed);
            else CloseSocket(static_cast<SocketHandle>(client.socket));
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
        std::shared_ptr<ServerConnection> connection;

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

                if (!pending_client.resumed && PendingClientExpired(
                        pending_client.accepted_at,
                        std::chrono::steady_clock::now(),
                        config_.idle_connection_timeout_ms)) {
                    CloseSocket(static_cast<SocketHandle>(pending_client.socket));
                    continue;
                }

                client_socket = pending_client.socket;
                connection = pending_client.resumed;
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

        ServerConnection initial;
        if (!connection) {
            initial.socket = static_cast<SocketHandle>(client_socket);
            { std::lock_guard lock(active_clients_mutex_); active_clients_.push_back(client_socket); }
            engine_->metrics()->IncActiveConnections();
        }
        auto& current = connection ? *connection : initial;
        if (!HandleClient(client_socket, current)) CloseClient(current);
    }
}

}  // namespace chunkdb
