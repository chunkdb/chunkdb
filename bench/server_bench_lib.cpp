#include "chunkdb/server_bench.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/server.hpp"
#include "chunkdb/uri.hpp"

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

namespace chunkdb::server_bench {
namespace {

using Clock = std::chrono::steady_clock;

#ifdef _WIN32
using SocketHandle = SOCKET;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
using SocketHandle = int;
constexpr SocketHandle kInvalidSocket = -1;
#endif

constexpr std::array<double, 8> kPercentiles{
    50.0,
    75.0,
    90.0,
    95.0,
    97.5,
    99.0,
    99.5,
    99.9,
};

[[nodiscard]] std::size_t CeilDiv(std::size_t num, std::size_t den) {
    return (num + den - 1) / den;
}

[[nodiscard]] std::string TrimCrLf(std::string value) {
    while (!value.empty() && (value.back() == '\r' || value.back() == '\n')) {
        value.pop_back();
    }
    return value;
}

[[nodiscard]] std::string JsonEscape(std::string_view text) {
    std::string out;
    out.reserve(text.size() + 8);
    for (const char ch : text) {
        switch (ch) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    return out;
}

[[nodiscard]] std::string JoinResultNames(const std::vector<ScenarioResult>& results) {
    std::ostringstream out;
    for (std::size_t i = 0; i < results.size(); ++i) {
        if (i != 0) {
            out << ",";
        }
        out << results[i].name;
    }
    return out.str();
}

[[nodiscard]] std::vector<std::string> SplitCsv(std::string_view text) {
    std::vector<std::string> out;
    std::size_t begin = 0;
    while (begin <= text.size()) {
        const std::size_t comma = text.find(',', begin);
        const std::size_t end = comma == std::string_view::npos ? text.size() : comma;
        std::string token(text.substr(begin, end - begin));
        if (!token.empty()) {
            out.push_back(std::move(token));
        }
        if (comma == std::string_view::npos) {
            break;
        }
        begin = comma + 1;
    }
    return out;
}

[[nodiscard]] Scenario ParseScenarioToken(std::string_view token) {
    if (token == "ping") {
        return Scenario::kPing;
    }
    if (token == "info") {
        return Scenario::kInfo;
    }
    if (token == "set") {
        return Scenario::kSet;
    }
    if (token == "get") {
        return Scenario::kGet;
    }
    if (token == "chunk") {
        return Scenario::kChunk;
    }
    if (token == "chunkbin") {
        return Scenario::kChunkBin;
    }
    if (token == "mixed") {
        return Scenario::kMixed;
    }
    throw std::invalid_argument("invalid --tests entry: " + std::string(token));
}

[[nodiscard]] std::size_t ParsePositiveSize(std::string_view text, const char* arg_name) {
    std::size_t consumed = 0;
    const auto value = std::stoull(std::string(text), &consumed, 10);
    if (consumed != text.size() || value == 0) {
        throw std::invalid_argument("invalid value for " + std::string(arg_name) + ": " + std::string(text));
    }
    return static_cast<std::size_t>(value);
}

[[nodiscard]] std::uint32_t ParseU32(std::string_view text, const char* arg_name) {
    std::size_t consumed = 0;
    const auto value = std::stoull(std::string(text), &consumed, 10);
    if (consumed != text.size() || value > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("invalid value for " + std::string(arg_name) + ": " + std::string(text));
    }
    return static_cast<std::uint32_t>(value);
}

[[nodiscard]] std::uint16_t ParsePort(std::string_view text) {
    std::size_t consumed = 0;
    const auto value = std::stoul(std::string(text), &consumed, 10);
    if (consumed != text.size() || value == 0 || value > 65535) {
        throw std::invalid_argument("invalid --port value: " + std::string(text));
    }
    return static_cast<std::uint16_t>(value);
}

[[nodiscard]] ServerMode ParseServerMode(std::string_view text) {
    if (text == "external") {
        return ServerMode::kExternal;
    }
    if (text == "spawn") {
        return ServerMode::kSpawn;
    }
    throw std::invalid_argument("invalid --server-mode value: " + std::string(text));
}

[[nodiscard]] OutputMode ParseOutputMode(std::string_view text) {
    if (text == "human") {
        return OutputMode::kHuman;
    }
    if (text == "json") {
        return OutputMode::kJson;
    }
    throw std::invalid_argument("invalid --output value: " + std::string(text));
}

void CloseSocket(SocketHandle s) {
#ifdef _WIN32
    closesocket(s);
#else
    close(s);
#endif
}

class ScopedSocketPlatform {
  public:
    ScopedSocketPlatform() {
#ifdef _WIN32
        WSADATA wsa_data;
        const int rc = WSAStartup(MAKEWORD(2, 2), &wsa_data);
        if (rc != 0) {
            throw std::runtime_error("WSAStartup failed: " + std::to_string(rc));
        }
#endif
    }
    ~ScopedSocketPlatform() {
#ifdef _WIN32
        WSACleanup();
#endif
    }
};

#ifdef _WIN32
[[nodiscard]] bool IsWindowsSharingViolation(const std::error_code& ec) {
    return ec.category() == std::system_category() &&
           (ec.value() == ERROR_SHARING_VIOLATION || ec.value() == ERROR_LOCK_VIOLATION);
}
#endif

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
    throw std::runtime_error(message);
}

class Client {
  public:
    Client(std::string host, std::uint16_t port)
        : host_(std::move(host)),
          port_(port),
          socket_(Connect(host_, port_)) {}

    ~Client() {
        if (socket_ != kInvalidSocket) {
            CloseSocket(socket_);
            socket_ = kInvalidSocket;
        }
    }

    void SendLine(std::string_view command) {
        std::string line(command);
        line += "\r\n";
        SendRaw(line);
    }

    [[nodiscard]] std::string ReadSimpleLine() {
        const std::string line = ReadLine();
        if (line.empty() || (line[0] != '+' && line[0] != '-')) {
            throw std::runtime_error("unexpected simple response line");
        }
        return line;
    }

    [[nodiscard]] std::string ReadBulkText() {
        const std::string header = ReadLine();
        if (!header.empty() && header[0] == '-') {
            throw std::runtime_error("server error response: " + TrimCrLf(header));
        }
        if (header.empty() || header[0] != '$') {
            throw std::runtime_error("unexpected bulk header");
        }
        const std::size_t len = ParseBulkLength(header);
        std::string payload = ReadExact(len);
        const std::string term = ReadExact(2);
        if (term != "\r\n") {
            throw std::runtime_error("invalid bulk terminator");
        }
        return payload;
    }

    [[nodiscard]] std::vector<std::uint8_t> ReadBulkBytes() {
        const std::string payload = ReadBulkText();
        return std::vector<std::uint8_t>(payload.begin(), payload.end());
    }

  private:
    std::string host_;
    std::uint16_t port_;
    SocketHandle socket_ = kInvalidSocket;
    std::string pending_;

    static SocketHandle Connect(const std::string& host, std::uint16_t port) {
        struct addrinfo hints;
        std::memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        struct addrinfo* result = nullptr;
        const std::string port_text = std::to_string(port);
        if (getaddrinfo(host.c_str(), port_text.c_str(), &hints, &result) != 0) {
            throw std::runtime_error("getaddrinfo failed for " + host + ":" + port_text);
        }

        SocketHandle socket = kInvalidSocket;
        for (auto* ai = result; ai != nullptr; ai = ai->ai_next) {
            socket = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (socket == kInvalidSocket) {
                continue;
            }
            if (::connect(socket, ai->ai_addr, static_cast<int>(ai->ai_addrlen)) == 0) {
                break;
            }
            CloseSocket(socket);
            socket = kInvalidSocket;
        }
        freeaddrinfo(result);

        if (socket == kInvalidSocket) {
            throw std::runtime_error("connect failed to " + host + ":" + port_text);
        }
        return socket;
    }

    void SendRaw(const std::string& bytes) {
        std::size_t off = 0;
        while (off < bytes.size()) {
#ifdef _WIN32
            const int written = send(
                socket_,
                bytes.data() + static_cast<int>(off),
                static_cast<int>(bytes.size() - off),
                0);
#else
            const ssize_t written = send(socket_, bytes.data() + off, bytes.size() - off, 0);
#endif
            if (written <= 0) {
                throw std::runtime_error("send failed");
            }
            off += static_cast<std::size_t>(written);
        }
    }

    [[nodiscard]] std::string ReadLine() {
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

    [[nodiscard]] std::string ReadExact(std::size_t size) {
        std::string out;
        out.reserve(size);
        while (out.size() < size) {
            if (!pending_.empty()) {
                const std::size_t take = std::min(size - out.size(), pending_.size());
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
                throw std::runtime_error("recv failed while reading exact bytes");
            }
            pending_.append(buffer, static_cast<std::size_t>(read));
        }
        return out;
    }

    static std::size_t ParseBulkLength(const std::string& header) {
        const std::string text = TrimCrLf(header);
        if (text.empty() || text[0] != '$') {
            throw std::runtime_error("invalid bulk header");
        }
        std::size_t consumed = 0;
        const auto length = std::stoull(text.substr(1), &consumed, 10);
        if (consumed != text.size() - 1) {
            throw std::runtime_error("invalid bulk length");
        }
        return static_cast<std::size_t>(length);
    }
};

[[nodiscard]] std::string ExtractInfoField(std::string_view payload, std::string_view key) {
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
        const std::size_t eq = line.find('=');
        if (eq != std::string_view::npos && line.substr(0, eq) == key) {
            return std::string(line.substr(eq + 1));
        }
        begin = end + 1;
    }
    return {};
}

[[nodiscard]] std::size_t ParseInfoFieldSize(std::string_view payload, std::string_view key) {
    const std::string value = ExtractInfoField(payload, key);
    if (value.empty()) {
        throw std::runtime_error("missing INFO field: " + std::string(key));
    }
    std::size_t consumed = 0;
    const auto parsed = std::stoull(value, &consumed, 10);
    if (consumed != value.size()) {
        throw std::runtime_error("invalid INFO field value: " + std::string(key));
    }
    return static_cast<std::size_t>(parsed);
}

struct GeometryInfo {
    std::size_t block_bits = 0;
    std::size_t chunk_width_blocks = 0;
    std::size_t chunk_height_blocks = 0;
    std::size_t chunk_bits = 0;
    std::size_t chunk_bytes = 0;
    std::string chunk_lock_mode = "unknown";
};

void AuthorizeIfNeeded(Client& client, const std::string& token) {
    if (token.empty()) {
        return;
    }
    client.SendLine("AUTH " + token);
    const std::string reply = TrimCrLf(client.ReadSimpleLine());
    if (reply.rfind("+OK", 0) != 0) {
        throw std::runtime_error("AUTH failed for benchmark client");
    }
}

[[nodiscard]] GeometryInfo LoadGeometryInfo(
    const std::string& host,
    std::uint16_t port,
    const std::string& token) {
    Client client(host, port);
    AuthorizeIfNeeded(client, token);
    client.SendLine("INFO");
    const std::string info = client.ReadBulkText();
    if (info.find("chunkdb_version=") == std::string::npos) {
        throw std::runtime_error("unexpected INFO payload");
    }

    GeometryInfo geometry;
    geometry.block_bits = ParseInfoFieldSize(info, "block_bits");
    geometry.chunk_width_blocks = ParseInfoFieldSize(info, "chunk_width_blocks");
    geometry.chunk_height_blocks = ParseInfoFieldSize(info, "chunk_height_blocks");
    geometry.chunk_bits =
        geometry.block_bits * geometry.chunk_width_blocks * geometry.chunk_height_blocks;
    geometry.chunk_bytes = CeilDiv(geometry.chunk_bits, static_cast<std::size_t>(8));
    geometry.chunk_lock_mode = ExtractInfoField(info, "chunk_lock_mode");
    if (geometry.chunk_lock_mode.empty()) {
        geometry.chunk_lock_mode = "unknown";
    }
    return geometry;
}

[[nodiscard]] std::string AlternatingBits(std::size_t bits, bool first_one) {
    std::string out(bits, '0');
    for (std::size_t i = 0; i < bits; ++i) {
        const bool one = ((i % 2) == 0) ? first_one : !first_one;
        out[i] = one ? '1' : '0';
    }
    return out;
}

struct ExpectedResponse {
    enum class Kind {
        kSimplePrefix,
        kBulkTextLength,
        kBulkTextContains,
        kBulkBytesLength,
    };

    Kind kind = Kind::kSimplePrefix;
    std::string prefix;
    std::size_t length = 0;
    std::string contains;
};

struct RequestPlan {
    std::string command;
    ExpectedResponse expected;
};

struct ScenarioPayload {
    std::size_t bytes = 0;
    std::string label;
};

[[nodiscard]] ScenarioPayload ScenarioPayloadInfo(
    Scenario scenario,
    const GeometryInfo& geometry) {
    switch (scenario) {
        case Scenario::kPing:
            return ScenarioPayload{0, "simple"};
        case Scenario::kInfo:
            return ScenarioPayload{0, "bulk-text(info)"};
        case Scenario::kSet:
            return ScenarioPayload{geometry.block_bits, "simple"};
        case Scenario::kGet:
            return ScenarioPayload{geometry.block_bits, "bulk-text(bits)"};
        case Scenario::kChunk:
            return ScenarioPayload{geometry.chunk_bits, "bulk-text(chunk-bits)"};
        case Scenario::kChunkBin:
            return ScenarioPayload{geometry.chunk_bytes, "bulk-bytes(chunk)"};
        case Scenario::kMixed:
            return ScenarioPayload{geometry.block_bits, "mixed(get/set)"};
    }
    return ScenarioPayload{0, "unknown"};
}

[[nodiscard]] RequestPlan BuildRequestPlan(
    Scenario scenario,
    std::size_t request_index,
    std::mt19937& rng,
    const Args& args,
    const GeometryInfo& geometry) {
    std::uniform_int_distribution<int> coords(0, static_cast<int>(args.keyspace - 1));
    const int x = coords(rng);
    const int y = coords(rng);

    RequestPlan plan;
    switch (scenario) {
        case Scenario::kPing:
            plan.command = "PING";
            plan.expected = ExpectedResponse{
                .kind = ExpectedResponse::Kind::kSimplePrefix,
                .prefix = "+PONG",
            };
            break;
        case Scenario::kInfo:
            plan.command = "INFO";
            plan.expected = ExpectedResponse{
                .kind = ExpectedResponse::Kind::kBulkTextContains,
                .contains = "chunkdb_version=",
            };
            break;
        case Scenario::kSet: {
            const std::string bits = AlternatingBits(geometry.block_bits, (request_index % 2) == 0);
            plan.command = "SET " + std::to_string(x) + " " + std::to_string(y) + " " + bits;
            plan.expected = ExpectedResponse{
                .kind = ExpectedResponse::Kind::kSimplePrefix,
                .prefix = "+OK",
            };
            break;
        }
        case Scenario::kGet:
            plan.command = "GET " + std::to_string(x) + " " + std::to_string(y);
            plan.expected = ExpectedResponse{
                .kind = ExpectedResponse::Kind::kBulkTextLength,
                .length = geometry.block_bits,
            };
            break;
        case Scenario::kChunk:
            plan.command = "CHUNK " + std::to_string(x) + " " + std::to_string(y);
            plan.expected = ExpectedResponse{
                .kind = ExpectedResponse::Kind::kBulkTextLength,
                .length = geometry.chunk_bits,
            };
            break;
        case Scenario::kChunkBin:
            plan.command = "CHUNKBIN " + std::to_string(x) + " " + std::to_string(y);
            plan.expected = ExpectedResponse{
                .kind = ExpectedResponse::Kind::kBulkBytesLength,
                .length = geometry.chunk_bytes,
            };
            break;
        case Scenario::kMixed: {
            if ((request_index % 10) < 7) {
                plan.command = "GET " + std::to_string(x) + " " + std::to_string(y);
                plan.expected = ExpectedResponse{
                    .kind = ExpectedResponse::Kind::kBulkTextLength,
                    .length = geometry.block_bits,
                };
            } else {
                const std::string bits = AlternatingBits(geometry.block_bits, (request_index % 2) == 0);
                plan.command = "SET " + std::to_string(x) + " " + std::to_string(y) + " " + bits;
                plan.expected = ExpectedResponse{
                    .kind = ExpectedResponse::Kind::kSimplePrefix,
                    .prefix = "+OK",
                };
            }
            break;
        }
    }
    return plan;
}

void ValidateResponse(
    Client& client,
    const ExpectedResponse& expected,
    std::string_view scenario_name) {
    switch (expected.kind) {
        case ExpectedResponse::Kind::kSimplePrefix: {
            const std::string line = TrimCrLf(client.ReadSimpleLine());
            if (line.rfind(expected.prefix, 0) != 0) {
                throw std::runtime_error(
                    "scenario=" + std::string(scenario_name) +
                    " validation failed: expected simple prefix '" + expected.prefix +
                    "', got '" + line + "'");
            }
            return;
        }
        case ExpectedResponse::Kind::kBulkTextLength: {
            const std::string payload = client.ReadBulkText();
            if (payload.size() != expected.length) {
                throw std::runtime_error(
                    "scenario=" + std::string(scenario_name) +
                    " validation failed: expected bulk text length " + std::to_string(expected.length) +
                    ", got " + std::to_string(payload.size()));
            }
            return;
        }
        case ExpectedResponse::Kind::kBulkTextContains: {
            const std::string payload = client.ReadBulkText();
            if (payload.find(expected.contains) == std::string::npos) {
                throw std::runtime_error(
                    "scenario=" + std::string(scenario_name) +
                    " validation failed: expected substring '" + expected.contains + "'");
            }
            return;
        }
        case ExpectedResponse::Kind::kBulkBytesLength: {
            const auto payload = client.ReadBulkBytes();
            if (payload.size() != expected.length) {
                throw std::runtime_error(
                    "scenario=" + std::string(scenario_name) +
                    " validation failed: expected bulk bytes length " + std::to_string(expected.length) +
                    ", got " + std::to_string(payload.size()));
            }
            return;
        }
    }
}

[[nodiscard]] double PercentileFromSorted(
    const std::vector<double>& sorted_ms,
    double p) {
    if (sorted_ms.empty()) {
        return 0.0;
    }
    if (p <= 0.0) {
        return sorted_ms.front();
    }
    if (p >= 100.0) {
        return sorted_ms.back();
    }
    const double rank = (p / 100.0) * static_cast<double>(sorted_ms.size() - 1);
    const std::size_t lower = static_cast<std::size_t>(rank);
    const std::size_t upper = std::min<std::size_t>(lower + 1, sorted_ms.size() - 1);
    const double fraction = rank - static_cast<double>(lower);
    return sorted_ms[lower] + (sorted_ms[upper] - sorted_ms[lower]) * fraction;
}

struct PendingRequest {
    Clock::time_point sent_at;
    ExpectedResponse expected;
};

struct ThreadWork {
    std::size_t client_id = 0;
    std::size_t request_count = 0;
};

[[nodiscard]] std::vector<ThreadWork> BuildWorkSplit(
    std::size_t requests,
    std::size_t clients) {
    std::vector<ThreadWork> split;
    split.reserve(clients);
    const std::size_t base = requests / clients;
    const std::size_t extra = requests % clients;
    for (std::size_t i = 0; i < clients; ++i) {
        split.push_back(ThreadWork{
            .client_id = i,
            .request_count = base + (i < extra ? 1U : 0U),
        });
    }
    return split;
}

[[nodiscard]] std::size_t CountActiveWorkers(const std::vector<ThreadWork>& split) {
    std::size_t active = 0;
    for (const auto& work : split) {
        if (work.request_count != 0) {
            ++active;
        }
    }
    return active;
}

[[nodiscard]] ScenarioResult RunScenario(
    Scenario scenario,
    const Args& args,
    const GeometryInfo& geometry) {
    const auto payload = ScenarioPayloadInfo(scenario, geometry);
    const auto work_split = BuildWorkSplit(args.requests, args.clients);

    std::mutex latencies_mutex;
    std::vector<double> latencies_ms;
    latencies_ms.reserve(args.requests);

    std::atomic<std::size_t> max_in_flight{0};
    std::atomic<bool> failed{false};
    std::mutex error_mutex;
    std::string first_error;

    const auto start = Clock::now();
    std::vector<std::thread> workers;
    workers.reserve(work_split.size());

    for (const auto& work : work_split) {
        if (work.request_count == 0) {
            continue;
        }
        workers.emplace_back([&, work]() {
            try {
                Client client(args.host, args.port);
                AuthorizeIfNeeded(client, args.auth_token);

                std::mt19937 rng(
                    args.seed ^
                    static_cast<std::uint32_t>((work.client_id + 1U) * 2654435761ULL) ^
                    static_cast<std::uint32_t>(static_cast<int>(scenario) * 2246822519ULL));

                std::deque<PendingRequest> pending;
                std::vector<double> local_latencies_ms;
                local_latencies_ms.reserve(work.request_count);
                std::size_t local_max_in_flight = 0;

                auto consume_one = [&]() {
                    if (pending.empty()) {
                        return;
                    }
                    const PendingRequest request = pending.front();
                    pending.pop_front();
                    ValidateResponse(client, request.expected, ScenarioName(scenario));
                    const auto now = Clock::now();
                    const auto latency_ms =
                        std::chrono::duration<double, std::milli>(now - request.sent_at).count();
                    local_latencies_ms.push_back(latency_ms);
                };

                for (std::size_t i = 0; i < work.request_count; ++i) {
                    if (failed.load(std::memory_order_acquire)) {
                        return;
                    }
                    const RequestPlan plan = BuildRequestPlan(scenario, i, rng, args, geometry);
                    client.SendLine(plan.command);
                    pending.push_back(PendingRequest{
                        .sent_at = Clock::now(),
                        .expected = plan.expected,
                    });
                    local_max_in_flight = std::max(local_max_in_flight, pending.size());

                    if (pending.size() >= args.pipeline) {
                        consume_one();
                    }
                }
                while (!pending.empty()) {
                    consume_one();
                }

                {
                    std::lock_guard lock(latencies_mutex);
                    latencies_ms.insert(
                        latencies_ms.end(),
                        local_latencies_ms.begin(),
                        local_latencies_ms.end());
                }

                std::size_t observed = max_in_flight.load(std::memory_order_relaxed);
                while (observed < local_max_in_flight &&
                       !max_in_flight.compare_exchange_weak(
                           observed,
                           local_max_in_flight,
                           std::memory_order_relaxed,
                           std::memory_order_relaxed)) {
                }
            } catch (const std::exception& e) {
                failed.store(true, std::memory_order_release);
                std::lock_guard lock(error_mutex);
                if (first_error.empty()) {
                    first_error = e.what();
                }
            }
        });
    }
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    const auto end = Clock::now();

    if (failed.load(std::memory_order_acquire)) {
        throw std::runtime_error(
            "scenario=" + std::string(ScenarioName(scenario)) +
            " failed: " + (first_error.empty() ? std::string("unknown error") : first_error));
    }
    if (latencies_ms.size() != args.requests) {
        throw std::runtime_error(
            "scenario=" + std::string(ScenarioName(scenario)) +
            " completed requests mismatch: expected=" + std::to_string(args.requests) +
            " got=" + std::to_string(latencies_ms.size()));
    }

    std::sort(latencies_ms.begin(), latencies_ms.end());

    ScenarioResult result;
    result.name = ScenarioName(scenario);
    result.completed_requests = latencies_ms.size();
    result.duration_s = std::chrono::duration<double>(end - start).count();
    result.throughput_req_s =
        result.duration_s > 0.0
            ? static_cast<double>(result.completed_requests) / result.duration_s
            : 0.0;
    result.latency_min_ms = latencies_ms.empty() ? 0.0 : latencies_ms.front();
    result.latency_max_ms = latencies_ms.empty() ? 0.0 : latencies_ms.back();
    result.latency_avg_ms =
        latencies_ms.empty()
            ? 0.0
            : (std::accumulate(latencies_ms.begin(), latencies_ms.end(), 0.0) /
               static_cast<double>(latencies_ms.size()));
    result.latency_p50_ms = PercentileFromSorted(latencies_ms, 50.0);
    result.latency_p95_ms = PercentileFromSorted(latencies_ms, 95.0);
    result.latency_p99_ms = PercentileFromSorted(latencies_ms, 99.0);
    result.percentiles_ms.reserve(kPercentiles.size());
    for (const double p : kPercentiles) {
        result.percentiles_ms.push_back({p, PercentileFromSorted(latencies_ms, p)});
    }
    result.payload_bytes = payload.bytes;
    result.payload_label = payload.label;
    result.max_in_flight = max_in_flight.load(std::memory_order_relaxed);
    result.keepalive = true;
    return result;
}

void WaitForServerReady(const Args& args) {
    std::string last_error = "unknown";
    for (int attempt = 0; attempt < 100; ++attempt) {
        try {
            Client client(args.host, args.port);
            AuthorizeIfNeeded(client, args.auth_token);
            client.SendLine("PING");
            const std::string pong = TrimCrLf(client.ReadSimpleLine());
            if (pong.rfind("+PONG", 0) == 0) {
                return;
            }
            last_error = "unexpected PING response: " + pong;
        } catch (const std::exception& e) {
            last_error = e.what();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    throw std::runtime_error("failed to connect to spawned benchmark server: " + last_error);
}

}  // namespace

const char* ServerModeName(ServerMode mode) noexcept {
    switch (mode) {
        case ServerMode::kExternal:
            return "external";
        case ServerMode::kSpawn:
            return "spawn";
    }
    return "unknown";
}

const char* OutputModeName(OutputMode mode) noexcept {
    switch (mode) {
        case OutputMode::kHuman:
            return "human";
        case OutputMode::kJson:
            return "json";
    }
    return "unknown";
}

const char* ScenarioName(Scenario scenario) noexcept {
    switch (scenario) {
        case Scenario::kPing:
            return "ping";
        case Scenario::kInfo:
            return "info";
        case Scenario::kSet:
            return "set";
        case Scenario::kGet:
            return "get";
        case Scenario::kChunk:
            return "chunk";
        case Scenario::kChunkBin:
            return "chunkbin";
        case Scenario::kMixed:
            return "mixed";
    }
    return "unknown";
}

std::vector<Scenario> DefaultScenarios() {
    return {
        Scenario::kPing,
        Scenario::kInfo,
        Scenario::kSet,
        Scenario::kGet,
        Scenario::kChunk,
        Scenario::kChunkBin,
        Scenario::kMixed,
    };
}

std::string UsageText() {
    std::ostringstream out;
    out
        << "Usage: chunkdb_server_bench [options]\n"
        << "  --server-mode <external|spawn>   default: external\n"
        << "  --uri <chunk://token@host:port/> optional endpoint URI\n"
        << "  --host <host>                    default: 127.0.0.1\n"
        << "  --port <port>                    default: 4242\n"
        << "  --clients <N>                    default: 50\n"
        << "  --pipeline <N>                   default: 1\n"
        << "  --requests <N>                   default: 5000\n"
        << "  --ops <N>                        alias for --requests\n"
        << "  --tests <list>                   comma list: ping,info,set,get,chunk,chunkbin,mixed\n"
        << "  --keyspace <N>                   default: 512\n"
        << "  --seed <N>                       default: 1337\n"
        << "  --token <token>                  optional AUTH token\n"
        << "  --log-level <info|warn|error>    default: info\n"
        << "  --output <human|json>            default: human\n";
    return out.str();
}

Args ParseArgs(int argc, char** argv) {
    std::vector<std::string> args;
    args.reserve(static_cast<std::size_t>(argc));
    for (int i = 0; i < argc; ++i) {
        args.emplace_back(argv[i] == nullptr ? "" : argv[i]);
    }
    return ParseArgs(args);
}

Args ParseArgs(const std::vector<std::string>& argv) {
    Args args;
    args.tests = DefaultScenarios();
    std::optional<ConnectionUri> parsed_uri;
    bool host_overridden = false;
    bool port_overridden = false;
    bool token_overridden = false;

    for (std::size_t i = 1; i < argv.size(); ++i) {
        const std::string& arg = argv[i];
        auto require_value = [&](const char* name) -> std::string {
            if (i + 1 >= argv.size()) {
                throw std::invalid_argument(std::string("missing value for ") + name);
            }
            ++i;
            return argv[i];
        };

        if (arg == "--help" || arg == "-h") {
            args.show_help = true;
            return args;
        }
        if (arg == "--server-mode") {
            args.server_mode = ParseServerMode(require_value("--server-mode"));
            continue;
        }
        if (arg == "--uri") {
            const auto uri = ParseConnectionUri(require_value("--uri"));
            if (uri.secure) {
                throw std::invalid_argument(
                    "chunks:// is not supported by chunkdb_server_bench yet; use chunk:// or add TLS benchmark transport support.");
            }
            parsed_uri = uri;
            continue;
        }
        if (arg == "--host") {
            args.host = require_value("--host");
            host_overridden = true;
            continue;
        }
        if (arg == "--port") {
            args.port = ParsePort(require_value("--port"));
            port_overridden = true;
            continue;
        }
        if (arg == "--clients") {
            args.clients = ParsePositiveSize(require_value("--clients"), "--clients");
            continue;
        }
        if (arg == "--pipeline") {
            args.pipeline = ParsePositiveSize(require_value("--pipeline"), "--pipeline");
            continue;
        }
        if (arg == "--requests" || arg == "--ops") {
            const std::string value = require_value(arg.c_str());
            args.requests = ParsePositiveSize(value, arg.c_str());
            continue;
        }
        if (arg == "--tests") {
            const auto tokens = SplitCsv(require_value("--tests"));
            if (tokens.empty()) {
                throw std::invalid_argument("--tests must not be empty");
            }
            std::vector<Scenario> parsed;
            parsed.reserve(tokens.size());
            for (const auto& token : tokens) {
                parsed.push_back(ParseScenarioToken(token));
            }
            args.tests = std::move(parsed);
            continue;
        }
        if (arg == "--keyspace") {
            args.keyspace = ParsePositiveSize(require_value("--keyspace"), "--keyspace");
            continue;
        }
        if (arg == "--seed") {
            args.seed = ParseU32(require_value("--seed"), "--seed");
            continue;
        }
        if (arg == "--token") {
            args.auth_token = require_value("--token");
            token_overridden = true;
            continue;
        }
        if (arg == "--log-level") {
            args.log_level = ParseLogLevel(require_value("--log-level"));
            continue;
        }
        if (arg == "--output") {
            args.output_mode = ParseOutputMode(require_value("--output"));
            continue;
        }

        throw std::invalid_argument("unknown argument: " + arg);
    }

    if (parsed_uri.has_value()) {
        if (!host_overridden) {
            args.host = parsed_uri->host;
        }
        if (!port_overridden) {
            args.port = parsed_uri->port;
        }
        if (!token_overridden && !parsed_uri->token.empty()) {
            args.auth_token = parsed_uri->token;
        }
    }

    if (args.host.empty()) {
        throw std::invalid_argument("--host must not be empty");
    }
    if (args.clients == 0) {
        throw std::invalid_argument("--clients must be > 0");
    }
    if (args.pipeline == 0) {
        throw std::invalid_argument("--pipeline must be > 0");
    }
    if (args.requests == 0) {
        throw std::invalid_argument("--requests must be > 0");
    }
    if (args.tests.empty()) {
        throw std::invalid_argument("--tests must not be empty");
    }
    if (args.keyspace == 0) {
        throw std::invalid_argument("--keyspace must be > 0");
    }

    return args;
}

BenchmarkReport Run(const Args& args) {
    ScopedSocketPlatform socket_platform;
    (void)socket_platform;
    SetLogLevel(args.log_level);

    BenchmarkReport report;
    report.server_mode = args.server_mode;
    report.spawned_server = false;
    report.host = args.host;
    report.port = args.port;
    report.requested_clients = args.clients;
    report.active_clients = 0;
    report.pipeline = args.pipeline;
    report.requests = args.requests;
    report.keyspace = args.keyspace;
    report.seed = args.seed;

    auto run_against_endpoint = [&]() {
        const GeometryInfo geometry = LoadGeometryInfo(args.host, args.port, args.auth_token);
        report.chunk_lock_mode = geometry.chunk_lock_mode;
        const auto split = BuildWorkSplit(args.requests, args.clients);
        report.active_clients = CountActiveWorkers(split);
        report.results.reserve(args.tests.size());
        for (const Scenario scenario : args.tests) {
            report.results.push_back(RunScenario(scenario, args, geometry));
        }
    };

    if (args.server_mode == ServerMode::kExternal) {
        run_against_endpoint();
        return report;
    }

    const auto data_dir = std::filesystem::temp_directory_path() /
                          ("chunkdb-server-bench-" +
                           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));

    std::shared_ptr<ChunkStore> store;
    std::shared_ptr<CommandEngine> engine;
    std::unique_ptr<ChunkServer> server;
    std::thread server_thread;
    try {
        store = std::make_shared<ChunkStore>(StoreConfig{
            .geometry = {
                .large_chunk_width_chunks = 8,
                .large_chunk_height_chunks = 8,
                .chunk_width_blocks = 16,
                .chunk_height_blocks = 16,
                .block_bits = 16,
            },
            .data_dir = data_dir,
            .durability_mode = DurabilityMode::kRelaxed,
            .checkpoint_update_interval = 512,
            .checkpoint_wal_bytes = 1024 * 1024,
            .wal_group_commit_updates = 8,
            .max_loaded_chunks = 16384,
            .max_open_wal_streams = 1024,
            .allow_multiple_processes = false,
        });

        engine = std::make_shared<CommandEngine>(
            EngineConfig{
                .auth_token = args.auth_token,
                .require_auth = !args.auth_token.empty(),
                .max_auth_failures = 5,
            },
            store);

        server = std::make_unique<ChunkServer>(
            ServerConfig{
                .host = args.host,
                .port = args.port,
                .max_line_bytes = 65536,
                .worker_threads = 4,
                .tls_enabled = false,
                .tls_cert_path = "",
                .tls_key_path = "",
            },
            engine);

        server_thread = std::thread([&]() { server->Run(); });
        WaitForServerReady(args);

        report.spawned_server = true;
        run_against_endpoint();

        server->Stop();
        if (server_thread.joinable()) {
            server_thread.join();
        }
        server.reset();
        engine.reset();
        store.reset();
        RemoveDataDirForBenchmark(data_dir);
        return report;
    } catch (...) {
        if (server != nullptr) {
            server->Stop();
        }
        if (server_thread.joinable()) {
            server_thread.join();
        }
        server.reset();
        engine.reset();
        store.reset();
        std::error_code cleanup_ec;
        try {
            RemoveDataDirForBenchmark(data_dir);
        } catch (...) {
            cleanup_ec = std::make_error_code(std::errc::io_error);
        }
        if (cleanup_ec) {
            throw std::runtime_error("benchmark failed and cleanup failed");
        }
        throw;
    }
}

std::string RenderHumanReport(const BenchmarkReport& report) {
    std::ostringstream out;
    out << "chunkdb protocol benchmark\n";
    out << "server_mode=" << ServerModeName(report.server_mode);
    if (report.server_mode == ServerMode::kSpawn) {
        out << " (opt-in)";
    }
    out << " spawned=" << (report.spawned_server ? "yes" : "no") << "\n";
    out << "endpoint=" << report.host << ":" << report.port
        << " chunk_lock_mode=" << report.chunk_lock_mode << "\n";
    out << "requests=" << report.requests
        << " requested_clients=" << report.requested_clients
        << " active_clients=" << report.active_clients
        << " pipeline=" << report.pipeline
        << " keyspace=" << report.keyspace
        << " seed=" << report.seed
        << " keepalive=on\n";
    if (report.active_clients < report.requested_clients) {
        out << "some clients were idle due to requests distribution\n";
    }
    out << "tests=" << JoinResultNames(report.results) << "\n";

    for (const auto& result : report.results) {
        out << "\n[" << result.name << "]\n";
        out << "Completed Requests: " << result.completed_requests
            << "  Duration(s): " << std::fixed << std::setprecision(4) << result.duration_s
            << "  Requested Clients: " << report.requested_clients
            << "  Active Clients: " << report.active_clients
            << "  Pipeline: " << report.pipeline
            << "  Payload: " << result.payload_label
            << " (" << result.payload_bytes << ")"
            << "  Keepalive: " << (result.keepalive ? "on" : "off")
            << "  Max In-Flight: " << result.max_in_flight
            << "\n";
        out << "Throughput (req/s): " << std::fixed << std::setprecision(2) << result.throughput_req_s << "\n";
        out << "Latency (ms): avg=" << std::fixed << std::setprecision(4) << result.latency_avg_ms
            << " min=" << result.latency_min_ms
            << " p50=" << result.latency_p50_ms
            << " p95=" << result.latency_p95_ms
            << " p99=" << result.latency_p99_ms
            << " max=" << result.latency_max_ms
            << "\n";
        out << "Percentile Distribution (ms):\n";
        for (const auto& [p, value] : result.percentiles_ms) {
            out << "  p" << std::fixed << std::setprecision((p < 100.0 && std::fmod(p, 1.0) != 0.0) ? 1 : 0)
                << p
                << " = " << std::fixed << std::setprecision(4) << value
                << "\n";
        }
    }

    return out.str();
}

std::string RenderJsonReport(const BenchmarkReport& report) {
    std::ostringstream out;
    out << "{";
    out << "\"server_mode\":\"" << JsonEscape(ServerModeName(report.server_mode)) << "\",";
    out << "\"spawned_server\":" << (report.spawned_server ? "true" : "false") << ",";
    out << "\"host\":\"" << JsonEscape(report.host) << "\",";
    out << "\"port\":" << report.port << ",";
    out << "\"requests\":" << report.requests << ",";
    out << "\"requested_clients\":" << report.requested_clients << ",";
    out << "\"active_clients\":" << report.active_clients << ",";
    out << "\"pipeline\":" << report.pipeline << ",";
    out << "\"keyspace\":" << report.keyspace << ",";
    out << "\"seed\":" << report.seed << ",";
    out << "\"keepalive\":\"on\",";
    out << "\"chunk_lock_mode\":\"" << JsonEscape(report.chunk_lock_mode) << "\",";
    out << "\"results\":[";
    for (std::size_t i = 0; i < report.results.size(); ++i) {
        if (i != 0) {
            out << ",";
        }
        const auto& r = report.results[i];
        out << "{";
        out << "\"test\":\"" << JsonEscape(r.name) << "\",";
        out << "\"completed_requests\":" << r.completed_requests << ",";
        out << "\"duration_s\":" << std::fixed << std::setprecision(6) << r.duration_s << ",";
        out << "\"throughput_req_s\":" << std::fixed << std::setprecision(3) << r.throughput_req_s << ",";
        out << "\"payload\":\"" << JsonEscape(r.payload_label) << "\",";
        out << "\"payload_bytes\":" << r.payload_bytes << ",";
        out << "\"max_in_flight\":" << r.max_in_flight << ",";
        out << "\"keepalive\":\"" << (r.keepalive ? "on" : "off") << "\",";
        out << "\"latency_ms\":{";
        out << "\"avg\":" << std::fixed << std::setprecision(6) << r.latency_avg_ms << ",";
        out << "\"min\":" << r.latency_min_ms << ",";
        out << "\"p50\":" << r.latency_p50_ms << ",";
        out << "\"p95\":" << r.latency_p95_ms << ",";
        out << "\"p99\":" << r.latency_p99_ms << ",";
        out << "\"max\":" << r.latency_max_ms;
        out << "},";
        out << "\"percentiles_ms\":[";
        for (std::size_t j = 0; j < r.percentiles_ms.size(); ++j) {
            if (j != 0) {
                out << ",";
            }
            out << "{"
                << "\"p\":" << std::fixed << std::setprecision(1) << r.percentiles_ms[j].first << ","
                << "\"value\":" << std::fixed << std::setprecision(6) << r.percentiles_ms[j].second
                << "}";
        }
        out << "]";
        out << "}";
    }
    out << "]";
    out << "}";
    return out.str();
}

}  // namespace chunkdb::server_bench
