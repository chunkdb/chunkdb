#include "chunkdb/server.hpp"

#include <array>
#include <cstring>
#include <stdexcept>
#include <thread>

#include "chunkdb/protocol.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace chunkdb {

namespace {

#ifdef _WIN32
using SocketHandle = SOCKET;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
using SocketHandle = int;
constexpr SocketHandle kInvalidSocket = -1;
#endif

void CloseSocket(SocketHandle socket_fd) {
#ifdef _WIN32
    closesocket(socket_fd);
#else
    close(socket_fd);
#endif
}

bool WriteAll(SocketHandle socket_fd, const char* data, std::size_t size) {
    std::size_t written = 0;
    while (written < size) {
#ifdef _WIN32
        const int result = send(
            socket_fd,
            data + static_cast<int>(written),
            static_cast<int>(size - written),
            0);
#else
        const ssize_t result = send(
            socket_fd,
            data + written,
            size - written,
            0);
#endif
        if (result <= 0) {
            return false;
        }
        written += static_cast<std::size_t>(result);
    }
    return true;
}

bool ReadLine(SocketHandle socket_fd, std::string& out, std::size_t max_line_bytes) {
    out.clear();

    for (;;) {
        char ch = 0;
#ifdef _WIN32
        const int read = recv(socket_fd, &ch, 1, 0);
#else
        const ssize_t read = recv(socket_fd, &ch, 1, 0);
#endif
        if (read == 0) {
            return !out.empty();
        }
        if (read < 0) {
            return false;
        }

        out.push_back(ch);
        if (out.size() > max_line_bytes) {
            throw std::runtime_error("request line exceeds max_line_bytes");
        }
        if (ch == '\n') {
            return true;
        }
    }
}

SocketHandle CreateListenSocket(const std::string& host, std::uint16_t port) {
    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    struct addrinfo* result = nullptr;
    const std::string port_text = std::to_string(port);

    const int gai = getaddrinfo(host.empty() ? nullptr : host.c_str(), port_text.c_str(), &hints, &result);
    if (gai != 0) {
        throw std::runtime_error("getaddrinfo failed");
    }

    SocketHandle listen_socket = kInvalidSocket;

    for (auto* ai = result; ai != nullptr; ai = ai->ai_next) {
        listen_socket = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (listen_socket == kInvalidSocket) {
            continue;
        }

        int reuse = 1;
#ifdef _WIN32
        setsockopt(
            listen_socket,
            SOL_SOCKET,
            SO_REUSEADDR,
            reinterpret_cast<const char*>(&reuse),
            sizeof(reuse));
#else
        setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
#endif

        if (bind(listen_socket, ai->ai_addr, static_cast<int>(ai->ai_addrlen)) == 0 &&
            listen(listen_socket, SOMAXCONN) == 0) {
            break;
        }

        CloseSocket(listen_socket);
        listen_socket = kInvalidSocket;
    }

    freeaddrinfo(result);

    if (listen_socket == kInvalidSocket) {
        throw std::runtime_error("failed to create listening socket");
    }

    return listen_socket;
}

}  // namespace

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
}

ChunkServer::~ChunkServer() {
    Stop();
}

void ChunkServer::Run() {
#ifdef _WIN32
    WSADATA wsa_data;
    if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
        throw std::runtime_error("WSAStartup failed");
    }
#endif

    listen_socket_ = CreateListenSocket(config_.host, config_.port);
    running_.store(true);

    while (running_.load()) {
        sockaddr_storage client_address;
#ifdef _WIN32
        int client_size = sizeof(client_address);
#else
        socklen_t client_size = sizeof(client_address);
#endif

        SocketHandle client_socket = accept(
            static_cast<SocketHandle>(listen_socket_),
            reinterpret_cast<sockaddr*>(&client_address),
            &client_size);

        if (client_socket == kInvalidSocket) {
            if (!running_.load()) {
                break;
            }
            continue;
        }

        std::thread(&ChunkServer::HandleClient, this, static_cast<decltype(listen_socket_)>(client_socket)).detach();
    }

#ifdef _WIN32
    WSACleanup();
#endif
}

void ChunkServer::Stop() {
    const bool was_running = running_.exchange(false);
    if (!was_running) {
        return;
    }

    if (listen_socket_ != kInvalidSocket) {
        CloseSocket(static_cast<SocketHandle>(listen_socket_));
        listen_socket_ = kInvalidSocket;
    }
}

void ChunkServer::HandleClient(
#ifdef _WIN32
    std::uintptr_t client_socket
#else
    int client_socket
#endif
) {
    SessionState session;
    std::string line;

    while (running_.load()) {
        bool has_line = false;
        try {
            has_line = ReadLine(static_cast<SocketHandle>(client_socket), line, config_.max_line_bytes);
        } catch (const std::exception& e) {
            const std::string response = Protocol::Error("BAD_REQUEST", e.what());
            (void)WriteAll(static_cast<SocketHandle>(client_socket), response.data(), response.size());
            break;
        }

        if (!has_line) {
            break;
        }

        const std::string response = engine_->Execute(session, line);
        if (!WriteAll(static_cast<SocketHandle>(client_socket), response.data(), response.size())) {
            break;
        }

        if (session.close_after_reply) {
            break;
        }
    }

    CloseSocket(static_cast<SocketHandle>(client_socket));
}

}  // namespace chunkdb
