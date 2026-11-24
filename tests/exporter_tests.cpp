// chunkdb_exporter as a process against a chunkdb_server process: flags,
// login with the MONITOR right, scrapes, refusals, a server restart and TLS.
// Usage: chunkdb_exporter_test <chunkdb_exporter> <chunkdb_server>
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>

#include "../src/exporter_client.hpp"
#include "../src/scram.hpp"
#include "listener_port_test_utils.hpp"
#include "test_utils.hpp"

namespace {

using Clock = std::chrono::steady_clock;
using chunkdb::exporter::ChunkConnection;
using chunkdb::exporter::ExporterOptions;

// Created owner-only before anything is written, as the exporter requires of
// a password file.
void WriteFile(const std::filesystem::path& path, const std::string& bytes) {
    std::filesystem::remove(path);
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    assert(fd >= 0);
    std::size_t written = 0;
    while (written < bytes.size()) {
        const auto n = ::write(fd, bytes.data() + written, bytes.size() - written);
        assert(n > 0);
        written += static_cast<std::size_t>(n);
    }
    assert(::close(fd) == 0);
}

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

int Connect(int port) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(fd >= 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<std::uint16_t>(port));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        const int error = errno;  // For the caller's message.
        close(fd);
        errno = error;
        return -1;
    }
    return fd;
}

// A child process with its output in `log`.
class Process {
  public:
    Process(const std::string& binary, std::vector<std::string> arguments, const std::filesystem::path& log)
        : log_(log) {
        // Emptied before the fork: a restart that reuses `log` must not show the
        // previous process's port to WaitListening.
        const int out = open(log.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
        assert(out >= 0);
        pid_ = fork();
        assert(pid_ >= 0);
        if (pid_ == 0) {
            if (dup2(out, STDERR_FILENO) < 0 || dup2(out, STDOUT_FILENO) < 0) _exit(126);
            arguments.insert(arguments.begin(), binary);
            std::vector<char*> argv;
            for (auto& argument : arguments) argv.push_back(argument.data());
            argv.push_back(nullptr);
            execv(binary.c_str(), argv.data());
            _exit(127);
        }
        close(out);
        live_.push_back(this);
    }
    ~Process() {
        live_.erase(std::remove(live_.begin(), live_.end(), this), live_.end());
        if (pid_ > 0) {
            kill(pid_, SIGKILL);
            (void)waitpid(pid_, nullptr, 0);
        }
    }
    Process(const Process&) = delete;
    Process& operator=(const Process&) = delete;

    // The port in the `port` field of the log line `message`, which the process
    // writes once it listens; fails if the process ends first.
    int WaitListening(const std::string& message) {
        const auto deadline = Clock::now() + std::chrono::seconds(20);
        for (;;) {
            if (const auto port = chunkdb::test::LogLinePort(log_, message)) return *port;
            if (!Running() || Clock::now() >= deadline)
                throw std::runtime_error("no log line \"" + message + "\" within 20 s" + Describe());
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
    // SIGTERM, then the exit status.
    int Stop() {
        kill(pid_, SIGTERM);
        int status = 0;
        assert(waitpid(pid_, &status, 0) == pid_);
        pid_ = -1;
        return status;
    }
    // The exit code of a process that ends by itself.
    int Wait() {
        int status = 0;
        assert(waitpid(pid_, &status, 0) == pid_);
        pid_ = -1;
        assert(WIFEXITED(status));
        return WEXITSTATUS(status);
    }

    // Each live child: pid, state and the end of its log, for a failure message.
    static std::string DescribeAll() {
        std::string out;
        for (const auto* process : live_) out += process->Describe();
        return out;
    }

  private:
    // Whether the child has not exited; it is not reaped, so Stop and Wait
    // still see its status.
    [[nodiscard]] bool Running() const {
        if (pid_ <= 0) return false;
        siginfo_t info{};
        return waitid(P_PID, static_cast<id_t>(pid_), &info, WEXITED | WNOHANG | WNOWAIT) == 0 && info.si_pid == 0;
    }
    [[nodiscard]] std::string Describe() const {
        std::string state = "reaped";
        if (pid_ > 0) {
            siginfo_t info{};
            if (waitid(P_PID, static_cast<id_t>(pid_), &info, WEXITED | WNOHANG | WNOWAIT) != 0) state = "unknown";
            else if (info.si_pid == 0) state = "running";
            else state = (info.si_code == CLD_EXITED ? "exited " : "killed by signal ") + std::to_string(info.si_status);
        }
        std::ifstream in(log_, std::ios::binary);
        const std::string text{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
        return "\n--- pid=" + std::to_string(pid_) + " " + state + " log=" + log_.string() + "\n" +
               text.substr(text.size() > 4000U ? text.size() - 4000U : 0U);
    }

    static inline std::vector<const Process*> live_;
    std::filesystem::path log_;
    pid_t pid_ = -1;
};

// One HTTP exchange; the whole response.
std::string Http(int port, const std::string& request) {
    const int fd = Connect(port);
    if (fd < 0) {
        const int error = errno;
        throw std::runtime_error("cannot connect to 127.0.0.1:" + std::to_string(port) + ": " +
                                 std::strerror(error) + Process::DescribeAll());
    }
    assert(send(fd, request.data(), request.size(), 0) == static_cast<ssize_t>(request.size()));
    std::string response;
    char buffer[4096];
    for (;;) {
        const auto n = recv(fd, buffer, sizeof(buffer), 0);
        if (n <= 0) break;
        response.append(buffer, static_cast<std::size_t>(n));
    }
    close(fd);
    return response;
}

std::string StatusLine(const std::string& response) { return response.substr(0, response.find("\r\n")); }

std::string Body(const std::string& response) {
    const auto end = response.find("\r\n\r\n");
    assert(end != std::string::npos);
    return response.substr(end + 4U);
}

std::string Scrape(int port) { return Http(port, "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n"); }

struct Fixture {
    chunkdb::test::ScopedTempDir dir{"chunkdb-exporter"};
    std::string exporter;
    std::string server_binary;
    // 0 until the first start, where the server picks it; restarts keep it.
    int server_port = 0;
    std::filesystem::path admin_password = dir.path() / "admin.password";
    std::vector<std::string> server_tls;
    std::string idle_timeout_ms = "60000";

    std::unique_ptr<Process> StartServer(const std::string& log_name = "server.log") {
        std::vector<std::string> arguments = {
            "--host", "127.0.0.1", "--data-dir", (dir.path() / "data").string(),
            "--auth", "scram", "--admin-user", "admin", "--admin-password-file", admin_password.string(),
            "--log-level", "info", "--workers", "2", "--idle-connection-timeout-ms", idle_timeout_ms};
        arguments.insert(arguments.end(), server_tls.begin(), server_tls.end());
        // After a --listen-uri in server_tls, so this port is the one used.
        arguments.insert(arguments.end(), {"--port", std::to_string(server_port)});
        auto server = std::make_unique<Process>(server_binary, arguments, dir.path() / log_name);
        const int port = server->WaitListening("ready to accept connections");
        assert(server_port == 0 || port == server_port);
        server_port = port;
        return server;
    }

    std::string Uri(const std::string& user) const {
        return std::string(server_tls.empty() ? "chunk" : "chunks") + "://" + user + "@127.0.0.1:" +
               std::to_string(server_port) + "/";
    }

    // Runs `statements` as the administrator; the reply of the last.
    std::string Admin(const std::vector<std::string>& statements) {
        ExporterOptions options;
        options.uri = chunkdb::ParseConnectionUri(Uri("admin"));
        options.password_file = admin_password.string();
        options.tls_ca_file = server_tls.empty() ? std::string() : (dir.path() / "server.pem").string();
        const auto deadline = Clock::now() + std::chrono::seconds(10);
        ChunkConnection admin(options, deadline, {});
        std::string reply;
        for (const auto& statement : statements) reply = admin.Query(statement, deadline, {});
        return reply;
    }

    // chunkdb_auth_failures_total of the server.
    std::uint64_t AuthFailures() {
        const auto metrics = Admin({"SHOW METRICS"});
        constexpr std::string_view kName = "\nchunkdb_auth_failures_total ";
        const auto at = metrics.find(kName);
        assert(at != std::string::npos);
        return std::stoull(metrics.substr(at + kName.size()));
    }

    void CreateUser(const std::string& name, const std::string& password) {
        const auto verifier = chunkdb::scram::MakeVerifier(password, chunkdb::crypto::RandomBytes(16),
                                                           chunkdb::scram::kMinIterations);
        Admin({"CREATE USER " + name + " VERIFIER '" + chunkdb::scram::FormatVerifier(verifier) + "'"});
        WriteFile(dir.path() / (name + ".password"), password + "\n");
    }

    // The exporter listens on a free port, which `port` receives.
    std::unique_ptr<Process> StartExporter(const std::string& user, int& port, std::vector<std::string> extra = {}) {
        std::vector<std::string> arguments = {"--uri", Uri(user), "--password-file",
                                              (dir.path() / (user + ".password")).string(),
                                              "--listen", "127.0.0.1:0", "--timeout-ms", "2000"};
        if (!server_tls.empty()) {
            arguments.push_back("--tls-ca-file");
            arguments.push_back((dir.path() / "server.pem").string());
        }
        arguments.insert(arguments.end(), extra.begin(), extra.end());
        auto process = std::make_unique<Process>(exporter, arguments, dir.path() / ("exporter-" + user + ".log"));
        port = process->WaitListening("serving metrics");
        return process;
    }
};

// Bad flags end the exporter before it listens.
void TestFlags(const std::string& exporter) {
    chunkdb::test::ScopedTempDir dir("chunkdb-exporter-flags");
    const auto log = dir.path() / "exporter.log";
    const auto password = dir.path() / "monitor.password";
    WriteFile(password, "secret\n");
    const auto refused = [&](std::vector<std::string> arguments, int code, std::string_view message) {
        Process process(exporter, std::move(arguments), log);
        assert(process.Wait() == code);
        assert(ReadFile(log).find(message) != std::string::npos);
    };
    refused({}, 2, "--uri is required");
    refused({"--uri", "chunk://m:pw@127.0.0.1:4242/"}, 2, "must not carry a password");
    refused({"--uri", "chunk://m@127.0.0.1:4242/"}, 2, "--password-file is required");
    refused({"--uri", "chunk://m@127.0.0.1:4242/", "--password-file", (dir.path() / "missing").string()}, 2,
            "cannot open the password file");
    refused({"--uri", "chunk://m@127.0.0.1:4242/", "--password-file", password.string(), "--listen", "127.0.0.1"}, 2,
            "invalid --listen");
    refused({"--uri", "chunk://m@127.0.0.1:4242/", "--password-file", password.string(), "--timeout-ms", "0"}, 2,
            "--timeout-ms");
    refused({"--uri", "chunk://m@127.0.0.1:4242/", "--password-file", password.string(), "--listen",
             "0.0.0.0:0"},
            1, "--allow-remote");
    refused({"--bogus"}, 2, "unknown option --bogus");
    const auto shared = dir.path() / "shared.password";
    WriteFile(shared, "secret\n");
    std::filesystem::permissions(shared, std::filesystem::perms::group_read, std::filesystem::perm_options::add);
    refused({"--uri", "chunk://m@127.0.0.1:4242/", "--password-file", shared.string()}, 2, "accessible by group or others");
}

void TestAgainstServer(Fixture& fixture) {
    WriteFile(fixture.admin_password, "admin-password\n");
    auto server = fixture.StartServer();
    fixture.CreateUser("monitor", "monitor-password");
    fixture.CreateUser("plain", "plain-password");
    fixture.Admin({"GRANT MONITOR ON * TO monitor", "CREATE TABLE terrain (bits bits(16))"});

    int port = 0;
    auto exporter = fixture.StartExporter("monitor", port);
    auto response = Scrape(port);
    assert(StatusLine(response) == "HTTP/1.1 200 OK");
    assert(response.find("\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n") != std::string::npos);
    auto body = Body(response);
    assert(body.find("\nchunkdb_up 1\n") != std::string::npos);
    assert(body.find("\nchunkdb_checkpoints_total{table=\"terrain\"} ") != std::string::npos);
    assert(body.find("# TYPE chunkdb_commands_total counter\n") != std::string::npos);
    // Kept connection: a second scrape works too.
    assert(StatusLine(Scrape(port)) == "HTTP/1.1 200 OK");

    const auto head = Http(port, "HEAD /metrics HTTP/1.1\r\n\r\n");
    assert(StatusLine(head) == "HTTP/1.1 200 OK" && Body(head).empty());
    assert(StatusLine(Http(port, "GET / HTTP/1.1\r\n\r\n")) == "HTTP/1.1 404 Not Found");
    assert(StatusLine(Http(port, "POST /metrics HTTP/1.1\r\n\r\n")) == "HTTP/1.1 405 Method Not Allowed");
    assert(StatusLine(Http(port, "nonsense\r\n\r\n")) == "HTTP/1.1 400 Bad Request");

    // A user without MONITOR: the server refuses SHOW METRICS.
    int plain_port = 0;
    auto plain = fixture.StartExporter("plain", plain_port);
    const auto refused = Scrape(plain_port);
    assert(StatusLine(refused) == "HTTP/1.1 503 Service Unavailable");
    assert(Body(refused).find("\nchunkdb_up 0\n") != std::string::npos);
    assert(Body(refused).find("chunkdb_commands_total") == std::string::npos);
    plain.reset();
    assert(ReadFile(fixture.dir.path() / "exporter-plain.log").find("SHOW METRICS refused") != std::string::npos);

    // A wrong password: the login fails.
    WriteFile(fixture.dir.path() / "plain.password", "wrong\n");
    const auto failures_before = fixture.AuthFailures();
    auto wrong = fixture.StartExporter("plain", plain_port);
    // A burst of scrapes after a failed login tries no new login until the
    // backoff ends; logins at every scrape would also get the source banned.
    for (int i = 0; i < 10; ++i) {
        const auto response = Scrape(plain_port);
        assert(StatusLine(response) == "HTTP/1.1 503 Service Unavailable");
        assert(Body(response).find("\nchunkdb_up 0\n") != std::string::npos);
    }
    assert(fixture.AuthFailures() - failures_before <= 2U);
    wrong.reset();
    assert(ReadFile(fixture.dir.path() / "exporter-plain.log").find("login refused") != std::string::npos);

    // A kept connection the server closed as idle: the scrape opens a new one.
    assert(StatusLine(Scrape(port)) == "HTTP/1.1 200 OK");
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    assert(StatusLine(Scrape(port)) == "HTTP/1.1 200 OK");

    // A restart between two scrapes: the second opens a new connection.
    int server_status = server->Stop();
    assert(WIFEXITED(server_status));
    server = fixture.StartServer("server-between-scrapes.log");
    assert(StatusLine(Scrape(port)) == "HTTP/1.1 200 OK");
    assert(ReadFile(fixture.dir.path() / "exporter-monitor.log").find("cannot read") == std::string::npos);

    // The server stops: 503; it starts again: the exporter logs in again
    // once its backoff ends.
    server_status = server->Stop();
    assert(WIFEXITED(server_status));
    response = Scrape(port);
    assert(StatusLine(response) == "HTTP/1.1 503 Service Unavailable");
    assert(Body(response) == std::string("# HELP chunkdb_up Whether chunkdb_exporter read the server's metrics in this "
                                         "scrape.\n# TYPE chunkdb_up gauge\nchunkdb_up 0\n"));
    server = fixture.StartServer("server-restarted.log");
    const auto restarted_deadline = Clock::now() + std::chrono::seconds(10);
    for (response = Scrape(port); StatusLine(response) != "HTTP/1.1 200 OK"; response = Scrape(port)) {
        assert(Clock::now() < restarted_deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    assert(Body(response).find("\nchunkdb_up 1\n") != std::string::npos);

    // SIGTERM ends the exporter.
    const int exporter_status = exporter->Stop();
    assert(WIFEXITED(exporter_status) && WEXITSTATUS(exporter_status) == 0);
    const auto log = ReadFile(fixture.dir.path() / "exporter-monitor.log");
    assert(log.find("cannot read the server's metrics") != std::string::npos);
    assert(log.find("reading the server's metrics again") != std::string::npos);
}

// chunks://: the exporter verifies the certificate and binds the login.
void TestTls(const std::string& exporter, const std::string& server_binary) {
#ifndef CHUNKDB_WITH_OPENSSL
    (void)exporter;
    (void)server_binary;
    std::puts("built without TLS; chunks:// not tested");
    return;
#else
    Fixture fixture;
    fixture.exporter = exporter;
    fixture.server_binary = server_binary;
    const auto base = (fixture.dir.path() / "server").string();
    if (std::system(("openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 -subj /CN=chunkdb "
                     "-addext subjectAltName=IP:127.0.0.1 -keyout " + base + ".key -out " + base + ".pem > /dev/null 2>&1")
                        .c_str()) != 0) {
        std::puts("openssl cannot make a test certificate; chunks:// not tested");
        return;
    }
    fixture.server_tls = {"--listen-uri", "chunks://127.0.0.1:0/",
                          "--tls-cert", base + ".pem", "--tls-key", base + ".key", "--auth-channel-binding", "require"};
    WriteFile(fixture.admin_password, "admin-password\n");
    auto server = fixture.StartServer();
    fixture.CreateUser("monitor", "monitor-password");
    fixture.Admin({"GRANT MONITOR ON * TO monitor"});
    int port = 0;
    {
        auto process = fixture.StartExporter("monitor", port);
        const auto response = Scrape(port);
        assert(StatusLine(response) == "HTTP/1.1 200 OK");
        assert(Body(response).find("\nchunkdb_up 1\n") != std::string::npos);
    }
    // Without binding, a server that requires it refuses the login.
    {
        auto process = fixture.StartExporter("monitor", port, {"--no-channel-binding"});
        assert(StatusLine(Scrape(port)) == "HTTP/1.1 503 Service Unavailable");
    }
    // A certificate the exporter does not trust.
    {
        std::vector<std::string> arguments = {"--uri", fixture.Uri("monitor"), "--password-file",
            (fixture.dir.path() / "monitor.password").string(), "--listen", "127.0.0.1:0"};
        Process process(exporter, arguments, fixture.dir.path() / "exporter-untrusted.log");
        port = process.WaitListening("serving metrics");
        assert(StatusLine(Scrape(port)) == "HTTP/1.1 503 Service Unavailable");
    }
    assert(ReadFile(fixture.dir.path() / "exporter-untrusted.log").find("not trusted") != std::string::npos);
#endif
}

}  // namespace

int main(int argc, char** argv) {
    assert(argc == 3);
    signal(SIGPIPE, SIG_IGN);
    // A caught failure unwinds, so each Process destructor kills its child.
    try {
        TestFlags(argv[1]);
        {
            Fixture fixture;
            fixture.exporter = argv[1];
            fixture.server_binary = argv[2];
            fixture.idle_timeout_ms = "500";
            TestAgainstServer(fixture);
        }
        TestTls(argv[1], argv[2]);
    } catch (const std::exception& error) {
        std::fprintf(stderr, "FAIL exporter: %s\n", error.what());
        return 1;
    }
    std::puts("exporter tests passed");
    return 0;
}
