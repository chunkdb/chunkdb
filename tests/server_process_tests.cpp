// The chunkdb_server binary as a process: what only a real process shows,
// such as the default action of SIGPIPE when a peer resets a connection.
// Usage: chunkdb_server_process_test <chunkdb_server>

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
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

[[noreturn]] void ExecServer(const std::string& binary, std::vector<std::string> arguments) {
    // A stale bootstrap username must not affect a persisted registry.
    assert(setenv("CHUNKDB_ADMIN_USER", "ignored", 1) == 0);
    assert(unsetenv("CHUNKDB_ADMIN_PASSWORD") == 0);
    arguments.insert(arguments.begin(), binary);
    std::vector<char*> argv;
    for (auto& argument : arguments) argv.push_back(argument.data());
    argv.push_back(nullptr);
    execv(binary.c_str(), argv.data());
    _exit(127);
}

void WriteFile(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary);
    out << bytes;
    assert(out.good());
}

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in.good());
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

std::string StartupFailure(const std::string& binary, const std::filesystem::path& log,
                           std::vector<std::string> arguments) {
    const pid_t pid = fork();
    assert(pid >= 0);
    if (pid == 0) {
        FILE* out = std::fopen(log.c_str(), "w");
        assert(out != nullptr);
        assert(dup2(fileno(out), STDERR_FILENO) >= 0);
        assert(dup2(fileno(out), STDOUT_FILENO) >= 0);
        ExecServer(binary, std::move(arguments));
    }
    int status = 0;
    assert(waitpid(pid, &status, 0) == pid);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 1);
    return ReadFile(log);
}

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

// The length of the HELLO 3 reply at the start of `all`: a map of eight
// pairs whose keys and values are bulk strings, integers or null (the
// server signature without a user).
std::size_t HelloReplySize(const std::string& all) {
    assert(all.rfind("%8\r\n", 0) == 0);
    std::size_t cursor = 4;
    for (int i = 0; i < 16; ++i) {
        const auto line_end = all.find("\r\n", cursor);
        assert(line_end != std::string::npos);
        if (all[cursor] == '$') {
            const auto length = std::stoull(all.substr(cursor + 1, line_end - cursor - 1));
            cursor = line_end + 2 + length + 2;
        } else {
            assert(all[cursor] == ':' || all[cursor] == '_');
            cursor = line_end + 2;
        }
    }
    return cursor;
}

// The reply to one statement on a fresh connection: the client sends HELLO 3
// and the statement, then closes its side, and the server closes after
// answering.
std::string Command(int port, const std::string& line) {
    const int fd = Connect(port);
    assert(fd >= 0);
    SendAll(fd, "HELLO 3\r\n" + line + "\r\n");
    assert(shutdown(fd, SHUT_WR) == 0);
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
    return all.substr(HelloReplySize(all));
}

class ServerProcess {
  public:
    ServerProcess(const std::string& binary, const std::filesystem::path& data_dir,
                  const std::vector<std::string>& options = {"--auth", "none"}) {
        // Another process can take the free port before the server binds
        // it; the server then exits and is started again on a new port.
        for (int attempt = 0; attempt < 5; ++attempt) {
            if (Start(binary, data_dir, options)) {
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
    bool Start(const std::string& binary, const std::filesystem::path& data_dir,
               const std::vector<std::string>& options) {
        port_ = FreePort();
        pid_ = fork();
        assert(pid_ >= 0);
        if (pid_ == 0) {
            // A test runner may ignore SIGPIPE; the server must not rely on it.
            signal(SIGPIPE, SIG_DFL);
            const std::string port = std::to_string(port_);
            const std::string dir = data_dir.string();
            std::vector<std::string> arguments = {"--host", "127.0.0.1", "--port", port, "--data-dir", dir,
                "--log-level", "error", "--workers", "2", "--wal-group-commit-updates", "100"};
            arguments.insert(arguments.end(), options.begin(), options.end());
            ExecServer(binary, std::move(arguments));
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
        std::string flood = "HELLO 3\r\n";
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
    assert(Command(server.port(), "PING") == "+PONG\r\n");
}

// SIGTERM with a client in the middle of a line stops the server cleanly,
// and acknowledged relaxed writes still in the group-commit batch survive.
void TestTerminateKeepsAcknowledgedWrites(const std::string& binary) {
    chunkdb::test::ScopedTempDir dir("chunkdb-process-term");
    const auto data = dir.path() / "data";
    {
        ServerProcess server(binary, data);
        assert(Command(server.port(), "CREATE TABLE default (bits bits(16))") == "+OK\r\n");
        for (int i = 0; i < 3; ++i) {
            const std::string reply =
                Command(server.port(), "SET BLOCK " + std::to_string(i) + " 0 IN default bits = b'1111000011110000'");
            assert(reply.rfind(":", 0) == 0 && reply.size() > 3 && reply.find("\r\n") == reply.size() - 2);
        }
        const int partial = Connect(server.port());
        SendAll(partial, "HELLO 3\r\nPIN");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        const int status = server.Terminate();
        close(partial);
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
    ServerProcess restarted(binary, data);
    for (int i = 0; i < 3; ++i) {
        // bits(16), lowest bit first: 0x0f 0x0f.
        assert(Command(restarted.port(), "GET BLOCK " + std::to_string(i) + " 0 FROM default") ==
               "*1\r\n$2\r\n\x0f\x0f\r\n");
    }
}

void TestStartupDiagnostics(const std::string& binary) {
    chunkdb::test::ScopedTempDir dir("chunkdb-process-startup-errors");
    const auto log = dir.path() / "startup.log";
    auto failure = [&](const std::filesystem::path& data, std::vector<std::string> extra = {}) {
        std::vector<std::string> args = {"--auth", "none", "--data-dir", data.string(), "--log-level", "error"};
        args.insert(args.end(), extra.begin(), extra.end());
        return StartupFailure(binary, log, std::move(args));
    };
    const auto blocked = dir.path() / "not-a-directory";
    WriteFile(blocked, "file");
    auto message = failure(blocked / "data");
    assert(message.find("--data-dir") != std::string::npos);
    assert(message.find("mount/permissions") != std::string::npos);

    const auto foreign = dir.path() / "foreign";
    std::filesystem::create_directory(foreign);
    WriteFile(foreign / "chunkdb.manifest", std::string(64, 'x'));
    message = failure(foreign);
    assert(message.find("chunkdb.manifest") != std::string::npos);
    assert(message.find("storage format") != std::string::npos);

    const auto backup = dir.path() / "backup";
    std::filesystem::create_directory(backup);
    WriteFile(backup / ".chunkdb.backup.incomplete", "CKBI");
    message = failure(backup);
    assert(message.find("use chunkdb_restore") != std::string::npos);

    message = failure(dir.path() / "tls", {"--listen-uri", "chunks://127.0.0.1:6499"});
    assert(message.find("set both flags to readable PEM") != std::string::npos);
    for (const auto* value : {"-1", "2147483648", "no", "1ms"}) {
        message = failure(dir.path() / "linger", {"--feed-linger-ms", value});
        assert(message.find("invalid --feed-linger-ms") != std::string::npos);
    }

    if (geteuid() != 0) {
        const auto locked = dir.path() / "locked";
        std::filesystem::create_directory(locked);
        std::filesystem::permissions(locked, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec);
        message = failure(locked / "missing-data");
        std::filesystem::permissions(locked, std::filesystem::perms::owner_all);
        assert(message.find("mount/permissions") != std::string::npos);
    }
}

void TestListenWarnings(const std::string& binary) {
    chunkdb::test::ScopedTempDir dir("chunkdb-process-listen-warnings");
    const auto blocked = dir.path() / "file";
    WriteFile(blocked, "file");
    auto warning = [&](const std::string& host, bool tls) {
        std::vector<std::string> arguments{"--host", host, "--auth", "none", "--log-level", "warn",
            "--data-dir", (blocked / "data").string()};
        if (tls) arguments.insert(arguments.end(), {"--listen-uri", "chunks://" + host + ":6499"});
        return StartupFailure(binary, dir.path() / "warnings.log", std::move(arguments));
    };
    const auto plain = warning("0.0.0.0", false);
    assert(plain.find("authentication disabled on non-loopback bind address") != std::string::npos);
    assert(plain.find("listening beyond localhost without TLS") != std::string::npos);
    const auto loopback = warning("127.0.0.1", false);
    assert(loopback.find("authentication disabled on non-loopback") == std::string::npos);
    assert(loopback.find("listening beyond localhost without TLS") == std::string::npos);
    const auto tls = warning("0.0.0.0", true);
    assert(tls.find("authentication disabled on non-loopback bind address") != std::string::npos);
    assert(tls.find("listening beyond localhost without TLS") == std::string::npos);
}

void TestFeedLingerFlag(const std::string& binary) {
    chunkdb::test::ScopedTempDir dir("chunkdb-process-linger");
    for (const auto* value : {"0", "30000"}) {
        ServerProcess server(binary, dir.path() / value, {"--auth", "none", "--feed-linger-ms", value});
        assert(Command(server.port(), "PING") == "+PONG\r\n");
        const int status = server.Terminate();
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
}

void TestPersistedUsersIgnoreBootstrap(const std::string& binary) {
    chunkdb::test::ScopedTempDir dir("chunkdb-process-bootstrap");
    const auto data = dir.path() / "data";
    const auto password = dir.path() / "password";
    WriteFile(password, "secret\n");
    {
        ServerProcess server(binary, data, {"--auth", "scram", "--admin-user", "admin",
                                           "--admin-password-file", password.string()});
        const int status = server.Terminate();
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
    const auto persisted = ReadFile(data / "chunkdb.users");
    for (const auto& options : std::vector<std::vector<std::string>>{
             {"--auth", "scram"},
             {"--auth", "scram", "--admin-password-file", (dir.path() / "missing-password").string()}}) {
        ServerProcess server(binary, data, options);
        const int fd = Connect(server.port());
        assert(fd >= 0);
        SendAll(fd, "HELLO 3\r\n");
        char buffer[1024];
        std::string reply;
        while (reply.find("\r\n") == std::string::npos) {
            const auto n = recv(fd, buffer, sizeof(buffer), 0);
            assert(n > 0);
            reply.append(buffer, static_cast<std::size_t>(n));
        }
        assert(reply.rfind("-ERR AUTH_REQUIRED", 0) == 0);
        close(fd);
        const int status = server.Terminate();
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        assert(ReadFile(data / "chunkdb.users") == persisted);
    }
    WriteFile(data / "chunkdb.users", "corrupt persisted users");
    const auto message = StartupFailure(binary, dir.path() / "corrupt.log",
        {"--data-dir", data.string(), "--auth", "scram", "--admin-user", "admin",
         "--admin-password-file", (dir.path() / "missing-password").string()});
    assert(message.find("chunkdb.users") != std::string::npos);
    assert(message.find("missing-password") == std::string::npos);
    assert(ReadFile(data / "chunkdb.users") == "corrupt persisted users");
}

}  // namespace
#endif

int main(int argc, char** argv) {
    if (argc != 2 && !(argc == 3 && std::string(argv[2]) == "--startup-errors")) {
        throw std::invalid_argument("usage: chunkdb_server_process_test <chunkdb_server>");
    }
#ifdef _WIN32
    (void)argv;
    std::puts("server process tests are POSIX-only (SIGPIPE); skipped");
#else
    TestStartupDiagnostics(argv[1]);
    TestListenWarnings(argv[1]);
    TestFeedLingerFlag(argv[1]);
    TestPersistedUsersIgnoreBootstrap(argv[1]);
    if (argc == 3) {
        std::puts("startup diagnostics and persisted bootstrap tests passed");
        return 0;
    }
    TestResetPeersDoNotKillTheServer(argv[1]);
    TestTerminateKeepsAcknowledgedWrites(argv[1]);
    std::puts("server process tests passed");
#endif
    return 0;
}
