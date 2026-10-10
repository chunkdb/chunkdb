#include "server_socket.hpp"

#include <cassert>
#include <cstdio>
#include <stdexcept>

namespace {
using namespace chunkdb::server_detail;
struct Loopback {
    SocketHandle listener = kInvalidSocket, client = kInvalidSocket, accepted = kInvalidSocket;
    Loopback() {
#ifdef _WIN32
        static const int started = [] { WSADATA data{}; return WSAStartup(MAKEWORD(2, 2), &data); }();
        assert(started == 0);
#endif
        listener = CreateListenSocket("127.0.0.1", 0);
        sockaddr_in address{};
#ifdef _WIN32
        int length = sizeof(address);
#else
        socklen_t length = sizeof(address);
#endif
        assert(getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length) == 0);
        client = socket(AF_INET, SOCK_STREAM, 0); assert(client != kInvalidSocket);
        assert(connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        accepted = accept(listener, nullptr, nullptr); assert(accepted != kInvalidSocket);
    }
    ~Loopback() { for (const auto fd : {accepted, client, listener}) if (fd != kInvalidSocket) CloseSocket(fd); }
    void ClosePeer(bool reset) {
        if (reset) {
            const linger immediate{1, 0};
#ifdef _WIN32
            assert(setsockopt(client, SOL_SOCKET, SO_LINGER, reinterpret_cast<const char*>(&immediate), sizeof(immediate)) == 0);
#else
            assert(setsockopt(client, SOL_SOCKET, SO_LINGER, &immediate, sizeof(immediate)) == 0);
#endif
        }
        CloseSocket(client); client = kInvalidSocket;
        int code = 0;
        assert(WaitForSocketReady(accepted, true, false, std::chrono::seconds(5), &code) == SocketWaitResult::kReady);
    }
};
int InvalidArgument() {
#ifdef _WIN32
    return WSAEINVAL;
#else
    return EINVAL;
#endif
}
void LiveClientAndPendingData() {
    Loopback connection;
    std::string error; int code = -1;
    assert(ConfigureSocketRecvTimeout(connection.accepted, 5000U, &error, &code) && code == 0);
    assert(!SocketTimeoutFailureIsPeerClose(connection.accepted, InvalidArgument()));
    const auto endpoint = PeerEndpointForSocket(connection.accepted);
    assert(endpoint.find("127.0.0.1") != std::string::npos && endpoint.find(':') != std::string::npos);
    assert(send(connection.client, "x", 1, 0) == 1);
    assert(WaitForSocketReady(connection.accepted, true, false, std::chrono::seconds(5), &code) == SocketWaitResult::kReady);
    assert(!SocketTimeoutFailureIsPeerClose(connection.accepted, InvalidArgument()));
    char byte{}; assert(recv(connection.accepted, &byte, 1, 0) == 1 && byte == 'x');
}
void ClosedPeer(bool reset) {
    Loopback connection; connection.ClosePeer(reset);
    std::string error; int code = -1;
    const bool configured = ConfigureSocketRecvTimeout(connection.accepted, 5000U, &error, &code);
    std::printf("peer=%s configured=%d socket_error=%d error=%s\n", reset ? "reset" : "FIN", configured, code, error.c_str());
#ifdef __APPLE__
    if (reset) assert(!configured && code == EINVAL);
#endif
    assert(SocketTimeoutFailureIsPeerClose(connection.accepted, InvalidArgument()));
    assert(!SocketTimeoutFailureIsPeerClose(connection.accepted, 0));
}
void InvalidDescriptorRemainsFailure() {
    std::string error; int code = 0;
    assert(!ConfigureSocketRecvTimeout(kInvalidSocket, 5000U, &error, &code));
    assert(code != 0 && !error.empty());
    assert(!SocketTimeoutFailureIsPeerClose(kInvalidSocket, InvalidArgument()));
}
}
int main() {
    LiveClientAndPendingData(); ClosedPeer(false); ClosedPeer(true); InvalidDescriptorRemainsFailure();
    std::puts("socket timeout passed: live, pending data, FIN, reset, invalid descriptor");
}
