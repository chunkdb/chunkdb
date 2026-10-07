// The chunkdb_server binary as a process: what only a real process shows,
// such as the default action of SIGPIPE when a peer resets a connection.
// Usage: chunkdb_server_process_test <chunkdb_server>

#include <cassert>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "test_utils.hpp"

#ifndef _WIN32
namespace {

int FreePort() {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    assert(bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
    socklen_t size = sizeof(address);
    assert(getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size) == 0);
    close(fd);
    return ntohs(address.sin_port);
}

int Connect(int port) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<std::uint16_t>(port));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        close(fd);
        return -1;
    }
    timeval timeout{.tv_sec = 10, .tv_usec = 0};
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    return fd;
}

void SendAll(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = send(fd, data.data() + sent, data.size() - sent, 0);
        if (n <= 0) {
            return;  // the server may already have closed: what the caller tests
        }
        sent += static_cast<std::size_t>(n);
    }
}

// Reads one reply line (bulk bodies included) from a fresh connection.
std::string Command(int port, const std::string& line) {
    const int fd = Connect(port);
    assert(fd >= 0);
    SendAll(fd, "HELLO 2\r\n" + line + "\r\nQUIT\r\n");
    std::string all;
    char buffer[4096];
    for (;;) {
        const ssize_t n = recv(fd, buffer, sizeof(buffer), 0);
        if (n <= 0) {
            break;
        }
        all.append(buffer, static_cast<std::size_t>(n));
    }
    close(fd);
    // Skip the HELLO bulk reply.
    const auto header_end = all.find("\r\n");
    const auto length = std::stoull(all.substr(1, header_end - 1));
    return all.substr(header_end + 2 + length + 2);
}

class ServerProcess {
  public:
    ServerProcess(const std::string& binary, const std::filesystem::path& data_dir) {
        // Another process can take the free port before the server binds
        // it; the server then exits and is started again on a new port.
        for (int attempt = 0; attempt < 5; ++attempt) {
            if (Start(binary, data_dir)) {
                return;
            }
        }
        assert(false && "server did not start");
    }

    ServerProcess(const ServerProcess&) = delete;
    ServerProcess& operator=(const ServerProcess&) = delete;

    ~ServerProcess() {
        if (pid_ > 0 && Alive()) {
            kill(pid_, SIGKILL);
            (void)waitpid(pid_, nullptr, 0);
        }
    }

    [[nodiscard]] int port() const noexcept { return port_; }

    [[nodiscard]] bool Alive() {
        int status = 0;
        const pid_t done = waitpid(pid_, &status, WNOHANG);
        if (done == pid_) {
            exit_status_ = status;
            pid_ = -1;
            return false;
        }
        return pid_ > 0;
    }

    // SIGTERM and the exit status.
    int Terminate() {
        assert(pid_ > 0);
        kill(pid_, SIGTERM);
        int status = 0;
        assert(waitpid(pid_, &status, 0) == pid_);
        pid_ = -1;
        return status;
    }

    [[nodiscard]] int exit_status() const noexcept { return exit_status_; }

  private:
    bool Start(const std::string& binary, const std::filesystem::path& data_dir) {
        port_ = FreePort();
        pid_ = fork();
        assert(pid_ >= 0);
        if (pid_ == 0) {
            // A test runner may ignore SIGPIPE; the server must not rely on it.
            signal(SIGPIPE, SIG_DFL);
            const std::string port = std::to_string(port_);
            const std::string dir = data_dir.string();
            execl(binary.c_str(), binary.c_str(), "--host", "127.0.0.1", "--port", port.c_str(), "--no-auth",
                  "--data-dir", dir.c_str(), "--log-level", "error", "--workers", "2",
                  "--wal-group-commit-updates", "100", static_cast<char*>(nullptr));
            _exit(127);
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        for (;;) {
            const int fd = Connect(port_);
            if (fd >= 0) {
                close(fd);
                return Alive();  // not another process on that port
            }
            if (!Alive()) {
                return false;
            }
            assert(std::chrono::steady_clock::now() < deadline);
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }

    int port_ = 0;
    pid_t pid_ = -1;
    int exit_status_ = 0;
};

// Peers that send many requests and close the connection without reading
// the replies make the server write to a dead socket. Both a reset and a
// plain close: Linux raises SIGPIPE on the write after the peer's reset of a
// closed socket, not on the first write after a reset.
void TestResetPeersDoNotKillTheServer(const std::string& binary) {
    chunkdb::test::ScopedTempDir dir("chunkdb-process-sigpipe");
    ServerProcess server(binary, dir.path() / "data");
    for (int attempt = 0; attempt < 20; ++attempt) {
        const int fd = Connect(server.port());
        assert(fd >= 0);
        std::string flood = "HELLO 2\r\n";
        for (int i = 0; i < 2000; ++i) {
            flood += "PING\r\n";
        }
        SendAll(fd, flood);
        if (attempt % 2 == 0) {
            linger reset{.l_onoff = 1, .l_linger = 0};
            (void)setsockopt(fd, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
        }
        close(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        if (!server.Alive()) {
            std::fprintf(stderr, "server died on attempt %d (status %d)\n", attempt, server.exit_status());
            assert(false);
        }
    }
    assert(Command(server.port(), "PING") == "+PONG\r\n+BYE\r\n");
}

// SIGTERM with a client in the middle of a line stops the server cleanly,
// and acknowledged relaxed writes still in the group-commit batch survive.
void TestTerminateKeepsAcknowledgedWrites(const std::string& binary) {
    chunkdb::test::ScopedTempDir dir("chunkdb-process-term");
    const auto data = dir.path() / "data";
    {
        ServerProcess server(binary, data);
        for (int i = 0; i < 3; ++i) {
            assert(Command(server.port(), "SET " + std::to_string(i) + " 0 1111000011110000") == "+OK\r\n+BYE\r\n");
        }
        const int partial = Connect(server.port());
        SendAll(partial, "HELLO 2\r\nPIN");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        const int status = server.Terminate();
        close(partial);
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
    ServerProcess restarted(binary, data);
    for (int i = 0; i < 3; ++i) {
        assert(Command(restarted.port(), "GET " + std::to_string(i) + " 0") == "$16\r\n1111000011110000\r\n+BYE\r\n");
    }
}

}  // namespace
#endif

int main(int argc, char** argv) {
    if (argc != 2) {
        throw std::invalid_argument("usage: chunkdb_server_process_test <chunkdb_server>");
    }
#ifdef _WIN32
    (void)argv;
    std::puts("server process tests are POSIX-only (SIGPIPE); skipped");
#else
    TestResetPeersDoNotKillTheServer(argv[1]);
    TestTerminateKeepsAcknowledgedWrites(argv[1]);
    std::puts("server process tests passed");
#endif
    return 0;
}
