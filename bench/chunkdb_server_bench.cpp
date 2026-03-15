#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/server.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace {

using Clock = std::chrono::steady_clock;

struct BenchResult {
    std::string name;
    std::size_t ops = 0;
    double seconds = 0.0;
    double ops_per_sec = 0.0;
    double p50_us = 0.0;
    double p95_us = 0.0;
    double p99_us = 0.0;
};

double Percentile(std::vector<double> values, double p) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const double rank = (p / 100.0) * static_cast<double>(values.size() - 1);
    const std::size_t index = static_cast<std::size_t>(rank);
    return values[index];
}

BenchResult Measure(
    std::string name,
    std::size_t ops,
    const std::function<void(std::size_t)>& fn) {
    std::vector<double> latencies_us;
    latencies_us.reserve(ops);

    const auto start = Clock::now();
    for (std::size_t i = 0; i < ops; ++i) {
        const auto op_start = Clock::now();
        fn(i);
        const auto op_end = Clock::now();
        latencies_us.push_back(std::chrono::duration<double, std::micro>(op_end - op_start).count());
    }
    const auto end = Clock::now();

    BenchResult result;
    result.name = std::move(name);
    result.ops = ops;
    result.seconds = std::chrono::duration<double>(end - start).count();
    result.ops_per_sec = result.seconds == 0.0 ? 0.0 : static_cast<double>(ops) / result.seconds;
    result.p50_us = Percentile(latencies_us, 50.0);
    result.p95_us = Percentile(latencies_us, 95.0);
    result.p99_us = Percentile(latencies_us, 99.0);
    return result;
}

void Print(const BenchResult& r) {
    std::cout << std::left << std::setw(24) << r.name
              << " ops=" << std::setw(8) << r.ops
              << " total_s=" << std::setw(10) << std::fixed << std::setprecision(4) << r.seconds
              << " ops_s=" << std::setw(12) << std::fixed << std::setprecision(2) << r.ops_per_sec
              << " p50_us=" << std::setw(9) << std::fixed << std::setprecision(2) << r.p50_us
              << " p95_us=" << std::setw(9) << std::fixed << std::setprecision(2) << r.p95_us
              << " p99_us=" << std::setw(9) << std::fixed << std::setprecision(2) << r.p99_us
              << "\n";
}

std::string ExtractInfoField(std::string_view payload, std::string_view key) {
    std::size_t begin = 0;
    while (begin < payload.size()) {
        std::size_t end = payload.find('\n', begin);
        if (end == std::string_view::npos) {
            end = payload.size();
        }

        std::string_view line = payload.substr(begin, end - begin);
        if (!line.empty() && line.back() == '\r') {
            line.remove_suffix(1);
        }

        const std::size_t sep = line.find('=');
        if (sep != std::string_view::npos && line.substr(0, sep) == key) {
            return std::string(line.substr(sep + 1));
        }

        begin = end + 1;
    }

    return {};
}

struct Args {
    std::size_t ops = 5000;
    std::uint16_t port = 4242;
    chunkdb::LogLevel log_level = chunkdb::LogLevel::kInfo;
};

Args ParseArgs(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto require_value = [&](const char* name) {
            if (i + 1 >= argc) {
                throw std::invalid_argument(std::string("missing value for ") + name);
            }
            ++i;
            return std::string(argv[i]);
        };

        if (arg == "--ops") {
            std::size_t consumed = 0;
            const auto value = require_value("--ops");
            args.ops = std::stoull(value, &consumed, 10);
            if (consumed != value.size() || args.ops == 0) {
                throw std::invalid_argument("invalid --ops value: " + value);
            }
        } else if (arg == "--port") {
            std::size_t consumed = 0;
            const auto value = require_value("--port");
            const int parsed = std::stoi(value, &consumed, 10);
            if (consumed != value.size() || parsed <= 0 || parsed > 65535) {
                throw std::invalid_argument("invalid --port value: " + value);
            }
            args.port = static_cast<std::uint16_t>(parsed);
        } else if (arg == "--log-level") {
            const auto value = require_value("--log-level");
            args.log_level = chunkdb::ParseLogLevel(value);
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: chunkdb_server_bench [--ops N] [--port 4242] [--log-level info|warn|error]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument("unknown argument: " + arg);
        }
    }
    return args;
}

#ifdef _WIN32
using SocketHandle = SOCKET;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
using SocketHandle = int;
constexpr SocketHandle kInvalidSocket = -1;
#endif

void CloseSocket(SocketHandle s) {
#ifdef _WIN32
    closesocket(s);
#else
    close(s);
#endif
}

bool IsWindowsSharingViolation(const std::error_code& ec) {
#ifdef _WIN32
    return ec.category() == std::system_category() &&
           (ec.value() == ERROR_SHARING_VIOLATION || ec.value() == ERROR_LOCK_VIOLATION);
#else
    (void)ec;
    return false;
#endif
}

void RemoveDataDirForBenchmark(const std::filesystem::path& data_dir) {
    if (!std::filesystem::exists(data_dir)) {
        return;
    }

#ifdef _WIN32
    constexpr int kMaxRetries = 12;
    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
        std::error_code ec;
        std::filesystem::remove_all(data_dir, ec);
        if (!std::filesystem::exists(data_dir)) {
            return;
        }
        if (!ec || !IsWindowsSharingViolation(ec)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10 * (attempt + 1)));
    }
#endif

    std::error_code ec;
    std::filesystem::remove_all(data_dir, ec);
    if (!std::filesystem::exists(data_dir)) {
        return;
    }

    std::string message = "cannot remove benchmark temp dir '" + data_dir.string() + "'";
    if (ec) {
        message += " (error " + std::to_string(ec.value()) + ": " + ec.message() + ")";
    }

#ifdef _WIN32
    const auto writer_lock = data_dir / ".chunkdb.lock" / "writer.lock";
    if (std::filesystem::exists(writer_lock)) {
        message += " writer_lock=" + writer_lock.string();
    }
#endif

    throw std::runtime_error(message);
}

class Client {
  public:
    Client(std::string host, std::uint16_t port) : host_(std::move(host)), port_(port) {
#ifdef _WIN32
        WSADATA wsa_data;
        if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
            throw std::runtime_error("WSAStartup failed");
        }
#endif
        socket_ = Connect(host_, port_);
    }

    ~Client() {
        if (socket_ != kInvalidSocket) {
            CloseSocket(socket_);
            socket_ = kInvalidSocket;
        }
#ifdef _WIN32
        WSACleanup();
#endif
    }

    std::string ExecSimple(const std::string& cmd) {
        Send(cmd + "\r\n");
        const std::string line = ReadLine();
        if (line.empty() || (line[0] != '+' && line[0] != '-')) {
            throw std::runtime_error("unexpected simple response");
        }
        return line;
    }

    std::string ExecBulkText(const std::string& cmd) {
        Send(cmd + "\r\n");
        const std::string header = ReadLine();
        if (header.empty() || header[0] != '$') {
            throw std::runtime_error("unexpected bulk header");
        }
        const std::size_t len = ParseBulkLength(header);
        std::string payload = ReadExact(len);
        const std::string crlf = ReadExact(2);
        if (crlf != "\r\n") {
            throw std::runtime_error("invalid bulk terminator");
        }
        return payload;
    }

    std::vector<std::uint8_t> ExecBulkBytes(const std::string& cmd) {
        Send(cmd + "\r\n");
        const std::string header = ReadLine();
        if (header.empty() || header[0] != '$') {
            throw std::runtime_error("unexpected bulk header");
        }
        const std::size_t len = ParseBulkLength(header);
        const std::string bytes = ReadExact(len);
        const std::string crlf = ReadExact(2);
        if (crlf != "\r\n") {
            throw std::runtime_error("invalid bulk terminator");
        }
        return std::vector<std::uint8_t>(bytes.begin(), bytes.end());
    }

  private:
    SocketHandle socket_ = kInvalidSocket;
    std::string host_;
    std::uint16_t port_ = 0;
    std::string pending_;

    static SocketHandle Connect(const std::string& host, std::uint16_t port) {
        struct addrinfo hints;
        std::memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        struct addrinfo* result = nullptr;
        const std::string port_text = std::to_string(port);
        if (getaddrinfo(host.c_str(), port_text.c_str(), &hints, &result) != 0) {
            throw std::runtime_error("getaddrinfo failed");
        }

        SocketHandle s = kInvalidSocket;
        for (auto* ai = result; ai != nullptr; ai = ai->ai_next) {
            s = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (s == kInvalidSocket) {
                continue;
            }
            if (connect(s, ai->ai_addr, static_cast<int>(ai->ai_addrlen)) == 0) {
                break;
            }
            CloseSocket(s);
            s = kInvalidSocket;
        }

        freeaddrinfo(result);
        if (s == kInvalidSocket) {
            throw std::runtime_error("connect failed");
        }
        return s;
    }

    void Send(const std::string& data) {
        std::size_t off = 0;
        while (off < data.size()) {
#ifdef _WIN32
            const int written = send(
                socket_,
                data.data() + static_cast<int>(off),
                static_cast<int>(data.size() - off),
                0);
#else
            const ssize_t written = send(socket_, data.data() + off, data.size() - off, 0);
#endif
            if (written <= 0) {
                throw std::runtime_error("send failed");
            }
            off += static_cast<std::size_t>(written);
        }
    }

    std::string ReadLine() {
        while (true) {
            const auto nl = pending_.find('\n');
            if (nl != std::string::npos) {
                std::string line = pending_.substr(0, nl + 1);
                pending_.erase(0, nl + 1);
                return line;
            }

            char buffer[4096];
#ifdef _WIN32
            const int read = recv(socket_, buffer, static_cast<int>(sizeof(buffer)), 0);
#else
            const ssize_t read = recv(socket_, buffer, sizeof(buffer), 0);
#endif
            if (read <= 0) {
                throw std::runtime_error("recv failed while reading line");
            }
            pending_.append(buffer, static_cast<std::size_t>(read));
        }
    }

    std::string ReadExact(std::size_t n) {
        std::string out;
        out.reserve(n);

        while (out.size() < n) {
            if (!pending_.empty()) {
                const std::size_t take = std::min(n - out.size(), pending_.size());
                out.append(pending_.data(), take);
                pending_.erase(0, take);
                continue;
            }

            char buffer[4096];
#ifdef _WIN32
            const int read = recv(socket_, buffer, static_cast<int>(sizeof(buffer)), 0);
#else
            const ssize_t read = recv(socket_, buffer, sizeof(buffer), 0);
#endif
            if (read <= 0) {
                throw std::runtime_error("recv failed while reading payload");
            }
            pending_.append(buffer, static_cast<std::size_t>(read));
        }

        return out;
    }

    static std::size_t ParseBulkLength(const std::string& header) {
        std::string text = header;
        while (!text.empty() && (text.back() == '\r' || text.back() == '\n')) {
            text.pop_back();
        }
        if (text.empty() || text[0] != '$') {
            throw std::runtime_error("invalid bulk header");
        }

        std::size_t consumed = 0;
        const std::size_t len = std::stoull(text.substr(1), &consumed, 10);
        if (consumed != text.size() - 1) {
            throw std::runtime_error("invalid bulk length");
        }
        return len;
    }
};

}  // namespace

int main(int argc, char** argv) {
    std::filesystem::path data_dir;
    try {
        const Args args = ParseArgs(argc, argv);
        chunkdb::SetLogLevel(args.log_level);

        data_dir = std::filesystem::temp_directory_path() /
                   ("chunkdb-server-bench-" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));

        std::vector<BenchResult> results;
        std::string chunk_lock_mode = "unknown";

        {
            auto store = std::make_shared<chunkdb::ChunkStore>(chunkdb::StoreConfig{
                .geometry = {
                    .large_chunk_width_chunks = 8,
                    .large_chunk_height_chunks = 8,
                    .chunk_width_blocks = 16,
                    .chunk_height_blocks = 16,
                    .block_bits = 16,
                },
                .data_dir = data_dir,
                .durability_mode = chunkdb::DurabilityMode::kRelaxed,
                .checkpoint_update_interval = 512,
                .checkpoint_wal_bytes = 1024 * 1024,
                .wal_group_commit_updates = 8,
                .max_loaded_chunks = 16384,
                .allow_multiple_processes = false,
            });

            auto engine = std::make_shared<chunkdb::CommandEngine>(
                chunkdb::EngineConfig{
                    .auth_token = "",
                    .require_auth = false,
                    .max_auth_failures = 5,
                },
                store);

            chunkdb::ChunkServer server(
                chunkdb::ServerConfig{
                    .host = "127.0.0.1",
                    .port = args.port,
                    .max_line_bytes = 65536,
                    .worker_threads = 4,
                    .tls_enabled = false,
                    .tls_cert_path = "",
                    .tls_key_path = "",
                },
                engine);

            std::thread server_thread([&]() { server.Run(); });
            auto stop_and_join = [&]() {
                server.Stop();
                if (server_thread.joinable()) {
                    server_thread.join();
                }
            };

            std::unique_ptr<Client> client;
            try {
                for (int i = 0; i < 50; ++i) {
                    try {
                        client = std::make_unique<Client>("127.0.0.1", args.port);
                        break;
                    } catch (...) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(20));
                    }
                }
                if (!client) {
                    throw std::runtime_error("failed to connect to benchmark server");
                }
                Client& conn = *client;

                const std::string info = conn.ExecBulkText("INFO");
                if (info.find("chunkdb_version=") == std::string::npos) {
                    throw std::runtime_error("unexpected INFO payload");
                }
                chunk_lock_mode = ExtractInfoField(info, "chunk_lock_mode");
                if (chunk_lock_mode.empty()) {
                    chunk_lock_mode = "unknown";
                }

                std::mt19937 rng(1337);
                std::uniform_int_distribution<int> dense(0, 511);
                const std::size_t chunk_ops = std::max<std::size_t>(1, args.ops / 4);

                results.reserve(8);
                results.push_back(Measure("protocol_ping", args.ops, [&](std::size_t) {
                    const std::string reply = conn.ExecSimple("PING");
                    if (reply.rfind("+PONG", 0) != 0) {
                        throw std::runtime_error("unexpected PING reply");
                    }
                }));

                results.push_back(Measure("protocol_info", args.ops, [&](std::size_t) {
                    const std::string info_payload = conn.ExecBulkText("INFO");
                    if (info_payload.find("chunkdb_version=") == std::string::npos) {
                        throw std::runtime_error("unexpected INFO payload");
                    }
                }));

                results.push_back(Measure("protocol_set", args.ops, [&](std::size_t i) {
                    const int x = dense(rng);
                    const int y = dense(rng);
                    const std::string bits = (i % 2 == 0) ? "1111000011110000" : "0000111100001111";
                    const std::string reply = conn.ExecSimple(
                        "SET " + std::to_string(x) + " " + std::to_string(y) + " " + bits);
                    if (reply.rfind("+OK", 0) != 0) {
                        throw std::runtime_error("unexpected SET reply");
                    }
                }));

                results.push_back(Measure("protocol_get", args.ops, [&](std::size_t) {
                    const int x = dense(rng);
                    const int y = dense(rng);
                    const std::string bits = conn.ExecBulkText(
                        "GET " + std::to_string(x) + " " + std::to_string(y));
                    if (bits.size() != 16) {
                        throw std::runtime_error("unexpected GET payload length");
                    }
                }));

                results.push_back(Measure("protocol_chunk", chunk_ops, [&](std::size_t i) {
                    const int cx = static_cast<int>(i % 8);
                    const int cy = static_cast<int>((i / 8) % 8);
                    const std::string bits = conn.ExecBulkText(
                        "CHUNK " + std::to_string(cx) + " " + std::to_string(cy));
                    if (bits.size() != 4096) {
                        throw std::runtime_error("unexpected CHUNK payload length");
                    }
                }));

                results.push_back(Measure("protocol_chunkbin", chunk_ops, [&](std::size_t i) {
                    const int cx = static_cast<int>(i % 8);
                    const int cy = static_cast<int>((i / 8) % 8);
                    const auto payload = conn.ExecBulkBytes(
                        "CHUNKBIN " + std::to_string(cx) + " " + std::to_string(cy));
                    if (payload.size() != 512) {
                        throw std::runtime_error("unexpected CHUNKBIN payload length");
                    }
                }));

                results.push_back(Measure("protocol_mixed_70_30", args.ops, [&](std::size_t i) {
                    const int x = dense(rng);
                    const int y = dense(rng);
                    if ((i % 10) < 7) {
                        (void)conn.ExecBulkText("GET " + std::to_string(x) + " " + std::to_string(y));
                    } else {
                        const std::string bits = (i % 2 == 0) ? "1010101010101010" : "0101010101010101";
                        (void)conn.ExecSimple(
                            "SET " + std::to_string(x) + " " + std::to_string(y) + " " + bits);
                    }
                }));

                client.reset();
                stop_and_join();
            } catch (...) {
                client.reset();
                stop_and_join();
                throw;
            }
        }

        std::cout << "chunkdb server-path benchmark\n";
        std::cout << "port=" << args.port << " ops=" << args.ops << "\n";
        std::cout << "log_level=" << chunkdb::LogLevelName(args.log_level) << "\n";
        std::cout << "chunk_lock_mode=" << chunk_lock_mode << "\n\n";
        for (const auto& r : results) {
            Print(r);
        }

        RemoveDataDirForBenchmark(data_dir);
        return 0;
    } catch (const std::exception& e) {
        try {
            if (!data_dir.empty()) {
                RemoveDataDirForBenchmark(data_dir);
            }
        } catch (const std::exception& cleanup_error) {
            std::cerr << "server benchmark cleanup failed: " << cleanup_error.what() << std::endl;
        }
        std::cerr << "server benchmark failed: " << e.what() << std::endl;
        return 1;
    }
}
