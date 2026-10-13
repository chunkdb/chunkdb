#include "chunkdb/server_bench.hpp"
#include "chunkdb/bench_build.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
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
#include "chunkdb/table_catalog.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/server.hpp"
#include "chunkdb/uri.hpp"
#include "crypto.hpp"
#include "scram.hpp"
#include "user_registry.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/select.h>
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
    if (token == "set") {
        return Scenario::kSet;
    }
    if (token == "get") {
        return Scenario::kGet;
    }
    if (token == "chunkgetstate") {
        return Scenario::kChunkGetState;
    }
    if (token == "mixed") {
        return Scenario::kMixed;
    }
    if (token == "world") {
        return Scenario::kWorld;
    }
    if (token == "canvas") {
        return Scenario::kCanvas;
    }
    if (token == "simulation") {
        return Scenario::kSimulation;
    }
    if (token == "takeover") return Scenario::kTakeover;
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

class PeerClosed : public std::runtime_error {
  public: PeerClosed() : std::runtime_error("peer closed the connection") {}
};

class Client {
  public:
    Client(std::string host, std::uint16_t port, std::size_t io_timeout_ms)
        : host_(std::move(host)),
          port_(port),
          io_timeout_ms_(io_timeout_ms),
          socket_(Connect(host_, port_, io_timeout_ms)) {}

    ~Client() {
        if (socket_ != kInvalidSocket) {
            CloseSocket(socket_);
            socket_ = kInvalidSocket;
        }
    }

    void Shutdown() noexcept {
#ifdef _WIN32
        (void)shutdown(socket_, SD_BOTH);
#else
        (void)shutdown(socket_, SHUT_RDWR);
#endif
    }
    void SetStopDeadline(const std::atomic<Clock::duration::rep>* deadline) noexcept { stop_deadline_ = deadline; }
    [[nodiscard]] std::size_t consumed_bytes() const noexcept { return consumed_bytes_; }
    void CheckReadDeadline() const {
        if (stop_deadline_) {
            const auto deadline = stop_deadline_->load(std::memory_order_acquire);
            if (deadline != 0 && Clock::now().time_since_epoch().count() >= deadline)
                throw std::runtime_error("stream read exceeded its absolute I/O deadline");
        }
    }
    // Readiness waits allow a quiet feed without turning silence into an I/O
    // error. A partially received frame retains the normal receive timeout.
    [[nodiscard]] bool WaitReply(std::chrono::milliseconds wait) const {
        return !pending_.empty() || WaitSocket(wait);
    }
    [[nodiscard]] bool WaitSocket(std::chrono::milliseconds wait) const {
        CheckReadDeadline();
        if (stop_deadline_) {
            const auto deadline = stop_deadline_->load(std::memory_order_acquire);
            if (deadline != 0) {
                const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                    Clock::time_point(Clock::duration(deadline)) - Clock::now());
                wait = std::min(wait, std::max(std::chrono::milliseconds(1), remaining));
            }
        }
        fd_set readable;
        FD_ZERO(&readable); FD_SET(socket_, &readable);
        timeval timeout{static_cast<decltype(timeval{}.tv_sec)>(wait.count() / 1000),
                        static_cast<decltype(timeval{}.tv_usec)>((wait.count() % 1000) * 1000)};
#ifdef _WIN32
        const int ready = select(0, &readable, nullptr, nullptr, &timeout);
#else
        const int ready = select(socket_ + 1, &readable, nullptr, nullptr, &timeout);
#endif
        if (ready < 0) throw IoFailure("watch readiness", true);
        return ready != 0;
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

    // A bulk reply, or std::nullopt for `$-1`.
    [[nodiscard]] std::optional<std::string> ReadBulkTextOrNull() {
        const std::string header = ReadLine();
        if (TrimCrLf(header) == "$-1") {
            return std::nullopt;
        }
        Unread(header);
        return ReadBulkText();
    }

    // Puts `text` back in front of what the next read returns.
    void Unread(const std::string& text) {
        pending_ = text + pending_;
        consumed_bytes_ -= text.size();
    }

    // One reply line of any type: headers and scalars.
    [[nodiscard]] std::string ReadReplyLine() {
        return ReadLine();
    }

    [[nodiscard]] std::vector<std::uint8_t> ReadBulkBytes() {
        const std::string payload = ReadBulkText();
        return std::vector<std::uint8_t>(payload.begin(), payload.end());
    }

    // A request line followed by its payload, in one send.
    void SendLineWithPayload(std::string_view command, const std::string& payload) {
        std::string bytes(command);
        bytes += "\r\n";
        bytes += payload;
        bytes += "\r\n";
        SendRaw(bytes);
    }

    // `*<n>`: the number of items that follow.
    [[nodiscard]] std::size_t ReadArrayHeader() {
        const std::string header = TrimCrLf(ReadLine());
        if (!header.empty() && header[0] == '-') {
            throw std::runtime_error("server error response: " + header);
        }
        if (header.size() < 2 || header[0] != '*') {
            throw std::runtime_error("unexpected array header");
        }
        std::size_t consumed = 0;
        const auto count = std::stoull(header.substr(1), &consumed, 10);
        if (consumed != header.size() - 1) {
            throw std::runtime_error("invalid array length");
        }
        return static_cast<std::size_t>(count);
    }

  private:
    std::string host_;
    std::uint16_t port_;
    std::size_t io_timeout_ms_;
    const std::atomic<Clock::duration::rep>* stop_deadline_ = nullptr;
    SocketHandle socket_ = kInvalidSocket;
    std::string pending_;
    std::size_t consumed_bytes_ = 0;

    [[nodiscard]] std::ptrdiff_t ReadIncoming(char* buffer, std::size_t capacity) {
        if (stop_deadline_) {
            const auto read_deadline = Clock::now() + std::chrono::milliseconds(io_timeout_ms_);
            for (;;) {
                const auto stop = stop_deadline_->load(std::memory_order_acquire);
                if (Clock::now() >= read_deadline || (stop != 0 && Clock::now().time_since_epoch().count() >= stop))
                    throw std::runtime_error("watch partial frame exceeded its I/O deadline");
                if (WaitSocket(std::chrono::milliseconds(100))) break;
            }
        }
#ifdef _WIN32
        return recv(socket_, buffer, static_cast<int>(capacity), 0);
#else
        return recv(socket_, buffer, capacity, 0);
#endif
    }

    static SocketHandle Connect(const std::string& host, std::uint16_t port, std::size_t io_timeout_ms) {
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
#ifdef _WIN32
        const DWORD timeout = static_cast<DWORD>(io_timeout_ms);
#else
        const timeval timeout{
            static_cast<time_t>(io_timeout_ms / 1000),
            static_cast<suseconds_t>((io_timeout_ms % 1000) * 1000)};
#endif
        for (const auto option : {SO_RCVTIMEO, SO_SNDTIMEO}) {
            if (setsockopt(socket, SOL_SOCKET, option,
                    reinterpret_cast<const char*>(&timeout), sizeof(timeout)) != 0) {
                CloseSocket(socket);
                throw std::runtime_error("failed to configure benchmark socket I/O timeout");
            }
        }
        return socket;
    }

    static std::runtime_error IoFailure(const char* operation, bool failed) {
#ifdef _WIN32
        const bool timed_out = failed && WSAGetLastError() == WSAETIMEDOUT;
#else
        const bool timed_out = failed &&
            (errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT);
#endif
        return std::runtime_error(std::string(operation) + (timed_out ? " timed out" : " failed"));
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
                throw IoFailure("send", written < 0);
            }
            off += static_cast<std::size_t>(written);
        }
    }

    [[nodiscard]] std::string ReadLine() {
        while (true) {
            CheckReadDeadline();
            const auto nl = pending_.find('\n');
            if ((nl == std::string::npos ? pending_.size() : nl) > 65536U)
                throw std::runtime_error("reply line exceeds limit");
            if (nl != std::string::npos) {
                std::string line = pending_.substr(0, nl + 1);
                pending_.erase(0, nl + 1);
                consumed_bytes_ += line.size();
                return line;
            }
            char buffer[4096];
            const auto read = ReadIncoming(buffer, sizeof(buffer));
            if (read == 0 && pending_.empty()) throw PeerClosed();
            if (read <= 0) {
                throw IoFailure("recv while reading line", read < 0);
            }
            pending_.append(buffer, static_cast<std::size_t>(read));
        }
    }

    [[nodiscard]] std::string ReadExact(std::size_t size) {
        std::string out;
        out.reserve(size);
        while (out.size() < size) {
            CheckReadDeadline();
            if (!pending_.empty()) {
                const std::size_t take = std::min(size - out.size(), pending_.size());
                out.append(pending_.data(), take);
                consumed_bytes_ += take;
                pending_.erase(0, take);
                continue;
            }
            char buffer[4096];
            const auto read = ReadIncoming(buffer, sizeof(buffer));
            if (read <= 0) {
                throw IoFailure("recv while reading exact bytes", read < 0);
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

struct GeometryInfo {
    std::size_t block_bits = 0;
    std::size_t chunk_width_blocks = 0;
    std::size_t chunk_height_blocks = 0;
    std::size_t chunk_bits = 0;
    std::size_t chunk_bytes = 0;
    std::size_t presence_bytes = 0;
    // The schema version a chunk form must be encoded for.
    std::uint64_t schema_version = 1;
};

[[nodiscard]] std::string LittleEndian64(std::uint64_t value) {
    std::string out(8, '\0');
    for (std::size_t i = 0; i < 8; ++i) {
        out[i] = static_cast<char>((value >> (8U * i)) & 0xffU);
    }
    return out;
}

void ReadReplyScalars(Client& client, std::vector<std::string>* out);
void ValidateHelloReply(Client& client, const std::string& expected_signature);

// Every connection starts with HELLO 3, logging in with SCRAM-SHA-256 when
// the bench has a user. The reply is a map: server_version a bulk string,
// server_signature the SCRAM server-final message (or `_`), the rest
// integers or extensible nested values.
void Hello(Client& client, const Args& args) {
    std::string expected_signature;
    if (args.user.empty()) {
        client.SendLine("HELLO 3");
    } else {
        const auto login = chunkdb::scram::StartClientLogin(args.user, chunkdb::scram::NewNonce());
        client.SendLineWithPayload("HELLO 3 USER " + args.user + " $1", "$" + std::to_string(login.first.size()) + "\r\n" + login.first);
        const std::string server_first = TrimCrLf(client.ReadReplyLine());
        if (server_first.rfind("+SCRAM ", 0) != 0) {
            throw std::runtime_error("login refused: " + server_first);
        }
        const auto final_message = chunkdb::scram::FinishClientLogin(login, args.password, server_first.substr(7));
        client.SendLineWithPayload(
            "AUTH $1", "$" + std::to_string(final_message.message.size()) + "\r\n" + final_message.message);
        expected_signature = final_message.server_signature;
    }
    ValidateHelloReply(client, expected_signature);
}

// A typed, bounded RESP value. Unknown map fields are fully consumed without
// confusing null arrays/bulks with unsigned lengths or flattening identities.
struct ReplyValue {
    char type = '_';
    std::string text;
    std::vector<ReplyValue> children;
};
std::uint64_t Unsigned(std::string_view digits) {
    std::uint64_t value = 0;
    const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), value);
    if (digits.empty() || parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size())
        throw std::runtime_error("invalid unsigned reply value");
    return value;
}
struct FrameBudget {
    static constexpr std::size_t kWireBytes = 64U * 1024U * 1024U;
    std::size_t start = 0;
    std::size_t nodes_left = 1048575U; // The top-level stack value is the first node.
    std::size_t allocations_left = 128U * 1024U * 1024U;
    void Wire(const Client& client, std::size_t additional = 0) const {
        const auto consumed = client.consumed_bytes() - start;
        if (consumed > kWireBytes || additional > kWireBytes - consumed)
            throw std::runtime_error("reply exceeds frame wire budget");
    }
    void Allocation(std::size_t bytes) {
        if (bytes > allocations_left) throw std::runtime_error("reply exceeds frame allocation budget");
        allocations_left -= bytes;
    }
    void Children(std::size_t count) {
        if (count > nodes_left) throw std::runtime_error("reply exceeds frame node budget");
        Allocation(count * sizeof(ReplyValue));
        nodes_left -= count;
    }
};
ReplyValue ReadFrameValue(Client& client, FrameBudget& budget, std::size_t depth) {
    client.CheckReadDeadline();
    if (depth > 64U) throw std::runtime_error("reply nesting exceeds limit");
    const auto raw = [&] {
        try { return client.ReadReplyLine(); }
        catch (const PeerClosed&) {
            if (depth != 0U) throw std::runtime_error("peer truncated a reply aggregate");
            throw;
        }
    }();
    if (raw.size() < 2U || raw.substr(raw.size() - 2U) != "\r\n") throw std::runtime_error("invalid reply line terminator");
    const auto line = raw.substr(0, raw.size() - 2U);
    if (line.empty()) throw std::runtime_error("empty reply");
    budget.Wire(client);
    budget.Allocation(raw.size());
    ReplyValue value; value.type = line.front(); value.text = line.substr(1);
    if (value.type == '-') throw std::runtime_error("server error response: " + line);
    if (value.type == '*' || value.type == '%' || value.type == '>') {
        if (value.text == "-1" && value.type == '*') { value.type = '_'; value.text.clear(); return value; }
        auto count = Unsigned(value.text);
        if (count > 1048576U) throw std::runtime_error("reply item count exceeds limit");
        if (value.type == '%') count *= 2U;
        budget.Children(static_cast<std::size_t>(count));
        value.children.reserve(static_cast<std::size_t>(count));
        for (std::uint64_t i = 0; i < count; ++i) value.children.push_back(ReadFrameValue(client, budget, depth + 1U));
    } else if (value.type == '$') {
        if (value.text == "-1") { value.type = '_'; value.text.clear(); return value; }
        const auto bytes = Unsigned(value.text);
        if (bytes > FrameBudget::kWireBytes) throw std::runtime_error("reply bulk exceeds limit");
        budget.Wire(client, static_cast<std::size_t>(bytes) + 2U);
        // Covers both retained payload and a possible ReadBulkText return copy.
        budget.Allocation(static_cast<std::size_t>(bytes) * 2U);
        client.Unread(line + "\r\n"); value.text = client.ReadBulkText();
    } else if (value.type == ':' || value.type == '(') {
        auto digits = std::string_view(value.text);
        if (!digits.empty() && digits.front() == '-') digits.remove_prefix(1U);
        if (digits.empty() || digits.find_first_not_of("0123456789") != std::string_view::npos)
            throw std::runtime_error("invalid integer reply value");
    } else if ((value.type == '_' && !value.text.empty()) ||
               (value.type == '#' && value.text != "t" && value.text != "f") ||
               (value.type != '_' && value.type != '#' && value.type != ',' && value.type != '+')) {
        throw std::runtime_error("invalid reply scalar");
    }
    budget.Wire(client);
    return value;
}
ReplyValue ReadValue(Client& client) {
    FrameBudget budget; budget.start = client.consumed_bytes();
    return ReadFrameValue(client, budget, 0U);
}
const ReplyValue& Field(const ReplyValue& map, std::string_view key) {
    if (map.type != '%') throw std::runtime_error("expected reply map");
    const ReplyValue* found = nullptr;
    for (std::size_t i = 0; i < map.children.size(); i += 2U) {
        if (map.children[i].type != '$') throw std::runtime_error("invalid map key");
        if (map.children[i].text == key) {
            if (found) throw std::runtime_error("duplicate reply field");
            found = &map.children[i + 1U];
        }
    }
    if (!found) throw std::runtime_error("missing reply field " + std::string(key));
    return *found;
}
void ValidateHelloReply(Client& client, const std::string& expected_signature) {
    const auto hello = ReadValue(client);
    const auto& protocol = Field(hello, "protocol");
    if (protocol.type != ':' || protocol.text != "3") throw std::runtime_error("unexpected HELLO protocol");
    if (Field(hello, "server_version").type != '$') throw std::runtime_error("invalid HELLO server version");
    const auto& signature = Field(hello, "server_signature");
    if ((signature.type != '$' && signature.type != '_') || signature.text != expected_signature)
        throw std::runtime_error("the server did not prove it knows the user's password");
}
void Flatten(const ReplyValue& value, std::vector<std::string>* out) {
    if (value.type == '*' || value.type == '%' || value.type == '>') {
        for (const auto& child : value.children) Flatten(child, out);
    } else out->push_back(value.text);
}
void ReadReplyScalars(Client& client, std::vector<std::string>* out) { Flatten(ReadValue(client), out); }
struct WatchPosition { std::string epoch; std::uint64_t revision = 0; };
void ValidateEpoch(std::string_view epoch) {
    if (epoch.size() != 32U || epoch.find_first_not_of("0123456789abcdef") != std::string_view::npos)
        throw std::runtime_error("invalid watch epoch");
}
std::uint64_t Revision(const ReplyValue& value) {
    if (value.type != ':' && value.type != '(') throw std::runtime_error("expected integer revision");
    return Unsigned(value.text);
}
WatchPosition WatchStart(Client& client) {
    auto value = ReadValue(client);
    WatchPosition position;
    if (value.type == '%') {
        const auto& epoch = Field(value, "epoch");
        if (epoch.type != '$') throw std::runtime_error("invalid watch epoch type");
        position.epoch = epoch.text;
        position.revision = Revision(Field(value, "revision"));
    } else if (value.type == '+') {
        std::istringstream input(value.text);
        std::string ok, revision, extra;
        if (!(input >> ok >> position.epoch >> revision) || ok != "OK" || (input >> extra))
            throw std::runtime_error("invalid legacy WATCH start");
        position.revision = Unsigned(revision);
    } else throw std::runtime_error("unexpected WATCH start");
    ValidateEpoch(position.epoch);
    return position;
}

// Only the reader parses input; Finish sends UNWATCH and sets a bounded stop
// deadline. Quiet readiness is allowed; EOF, malformed data and recv errors
// remain failures. No mutation/progress ACK is sent on this ephemeral watch.
class WatchingClient {
  public:
    explicit WatchingClient(const Args& args) : client_(args.host, args.port, args.io_timeout_ms),
        timeout_(args.io_timeout_ms), args_(args) {
        Hello(client_, args);
        std::string command = "WATCH " + args.watch_table;
        if (args.watch_mode == "progress") command += " AREA -1 -1 TO -1 -1";
        if (args.watch_mode == "filtered") command += " AREA 0 0 TO 0 0 COLUMNS bits NO BEFORE";
        if (args.watch_mode == "chunks") command += " CHUNKS";
        client_.SendLine(command);
        position_ = WatchStart(client_);
        initial_bytes_ = client_.consumed_bytes();
        start_ = Clock::now();
        client_.SetStopDeadline(&stop_deadline_);
        reader_ = std::thread([this] {
            try {
                for (;;) {
                    const auto deadline = stop_deadline_.load(std::memory_order_acquire);
                    if (deadline != 0 && Clock::now().time_since_epoch().count() >= deadline)
                        throw std::runtime_error("watch waited for UNWATCH completion past its I/O deadline");
                    if (!client_.WaitReply(std::chrono::milliseconds(100))) continue;
                    const auto value = ReadValue(client_);
                    if (value.type == '+' && value.text == "OK") {
                        if (stop_deadline_.load(std::memory_order_acquire) == 0)
                            throw std::runtime_error("unsolicited UNWATCH completion");
                        return;
                    }
                    { std::lock_guard lock(event_mutex_); ValidatePush(value); }
                    event_cv_.notify_all();
                }
            } catch (const std::exception&) {
                { std::lock_guard lock(event_mutex_); error_ = std::current_exception(); }
                event_cv_.notify_all();
            }
        });
    }
    ~WatchingClient() { client_.Shutdown(); if (reader_.joinable()) reader_.join(); }
    void Finish(ScenarioResult* result, std::optional<std::uint64_t> write_frontier) {
        if (args_.watch_mode == "progress") {
            Client metadata(args_.host, args_.port, timeout_);
            Hello(metadata, args_);
            metadata.SendLine("DESCRIBE " + args_.watch_table);
            const auto describe = ReadValue(metadata);
            if (Field(describe, "epoch").text != position_.epoch)
                throw std::runtime_error("progress target changed epoch");
            const auto target = Revision(Field(describe, "revision"));
            std::unique_lock lock(event_mutex_);
            if (!event_cv_.wait_for(lock, std::chrono::milliseconds(timeout_), [&] {
                    return error_ || (progress_ != 0U && position_.revision >= target);
                })) throw std::runtime_error("watch waited for filtered progress frontier past its I/O deadline");
            if (error_) std::rethrow_exception(error_);
        }
        if (args_.watch_table == "default" && write_frontier) {
            std::unique_lock lock(event_mutex_);
            if (!event_cv_.wait_for(lock, std::chrono::milliseconds(timeout_), [&] {
                    return error_ || position_.revision >= *write_frontier;
                })) throw std::runtime_error("watch waited for completed write frontier past its I/O deadline");
            if (error_) std::rethrow_exception(error_);
        }
        stop_deadline_.store((Clock::now() + std::chrono::milliseconds(timeout_)).time_since_epoch().count(),
                             std::memory_order_release);
        client_.SendLine("UNWATCH"); reader_.join();
        if (error_) std::rethrow_exception(error_);
        result->watch_bytes = client_.consumed_bytes() - initial_bytes_;
        result->watch_changes = changes_; result->watch_progress = progress_; result->watch_chunks = chunks_; result->watch_other = other_;
        result->watch_duration_s = std::chrono::duration<double>(Clock::now() - start_).count();
    }
  private:
    void ValidatePush(const ReplyValue& value) {
        if (value.type != '>' || value.children.empty() || value.children[0].type != '$')
            throw std::runtime_error("invalid watch push");
        const auto& kind = value.children[0].text;
        const auto count = value.children.size();
        if (kind == "resync" || kind == "schema" || kind == "end")
            throw std::runtime_error("watch invalidated the fixed schema or delivery prefix: " + kind);
        if (kind != "change" && kind != "chunks" && kind != "progress") { ++other_; return; }
        if ((kind == "change" && count != 7U && count < 10U) ||
            (kind == "chunks" && count < 9U) || (kind == "progress" && count < 3U) ||
            (kind != "change" && kind != "chunks" && kind != "progress"))
            throw std::runtime_error("unexpected watch event " + kind);
        if (value.children[1].type != '$' || value.children[1].text != position_.epoch)
            throw std::runtime_error("watch epoch changed");
        const auto revision = Revision(value.children[2]);
        if (revision < position_.revision || (kind != "progress" && revision == position_.revision))
            throw std::runtime_error("watch revision went backwards");
        position_.revision = revision;
        if (kind == "progress") { ++progress_; return; }
        if (value.children[6].type != '*') throw std::runtime_error("invalid watch entries");
        for (const auto& entry : value.children[6].children) {
            if (entry.type != '*' || entry.children.size() != 4U ||
                entry.children[0].type != ':' || entry.children[1].type != ':')
                throw std::runtime_error("invalid watch entry");
            if (kind == "chunks") {
                (void)Revision(entry.children[2]);
                if (entry.children[3].type != '$' && entry.children[3].type != '_')
                    throw std::runtime_error("invalid chunk form event");
            } else {
                for (std::size_t side = 2U; side <= 3U; ++side)
                    if (entry.children[side].type != '*' && entry.children[side].type != '_')
                        throw std::runtime_error("invalid change row");
            }
        }
        if (kind == "change") ++changes_; else ++chunks_;
    }
    Client client_;
    std::size_t timeout_, initial_bytes_ = 0;
    Args args_;
    std::mutex event_mutex_;
    std::condition_variable event_cv_;
    WatchPosition position_;
    Clock::time_point start_;
    std::atomic<Clock::duration::rep> stop_deadline_{0};
    std::size_t changes_ = 0, progress_ = 0, chunks_ = 0, other_ = 0;
    std::thread reader_;
    std::exception_ptr error_;
};

// The geometry of table `default`, from DESCRIBE: one bits(N) column and the
// chunk size.
[[nodiscard]] GeometryInfo LoadGeometryInfo(const Args& args) {
    Client client(args.host, args.port, args.io_timeout_ms);
    Hello(client, args);
    client.SendLine("DESCRIBE default");
    std::vector<std::string> scalars;
    ReadReplyScalars(client, &scalars);
    const auto after = [&scalars](std::string_view key, std::size_t offset) -> const std::string& {
        const auto at = std::find(scalars.begin(), scalars.end(), key);
        if (at == scalars.end() || static_cast<std::size_t>(scalars.end() - at) <= offset) {
            throw std::runtime_error("DESCRIBE default has no " + std::string(key));
        }
        return *(at + static_cast<std::ptrdiff_t>(offset));
    };
    const std::string& type = after("type", 1);
    if (type.rfind("bits(", 0) != 0 || std::count(scalars.begin(), scalars.end(), "type") != 1) {
        throw std::runtime_error("the bench needs table default with one bits(N) column, got " + type);
    }

    GeometryInfo geometry;
    geometry.block_bits = std::stoull(type.substr(5));
    geometry.schema_version = std::stoull(after("version", 1));
    geometry.chunk_width_blocks = std::stoull(after("chunk", 1));
    geometry.chunk_height_blocks = std::stoull(after("chunk", 2));
    geometry.chunk_bits =
        geometry.block_bits * geometry.chunk_width_blocks * geometry.chunk_height_blocks;
    geometry.chunk_bytes = CeilDiv(geometry.chunk_bits, static_cast<std::size_t>(8));
    geometry.presence_bytes = CeilDiv(
        geometry.chunk_width_blocks * geometry.chunk_height_blocks, static_cast<std::size_t>(8));
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

enum class RequestKind { kPing, kSetBlock, kGetBlock, kSetChunk, kGetChunk, kGetArea };
constexpr std::array<const char*, 6> kRequestNames{"ping", "set_block", "get_block", "set_chunk", "get_chunk", "get_area"};

struct ExpectedResponse {
    enum class Kind {
        kSimplePrefix,
        // A block read: `*1` and the bits as `length` bytes, or `_` for an
        // absent block.
        kBlockValuesOrNull,
        // A write: `:` and the chunk version.
        kInteger,
        // An area read: `[cx, cy, chunk form]` per chunk, each form `length`
        // bytes.
        kAreaArray,
        kBulkBytesLength,
        kChunkFormOrNull,
    };

    Kind kind = Kind::kSimplePrefix;
    std::string prefix;
    std::size_t length = 0;
    bool with_version = false;
    bool with_schema = false;
    std::uint64_t schema_version = 0;
};

struct RequestPlan {
    std::string command;
    // Sent after the line when not empty: a parameter frame.
    std::string payload;
    ExpectedResponse expected;
    RequestKind kind = RequestKind::kPing;
};

// Where a client of the world scenario stands, in chunk coordinates.
struct ClientState {
    std::int64_t player_cx = 0;
    std::int64_t player_cy = 0;
};

// The chunks a grid-world scenario works on: [0, chunks_x) x [0, chunks_y),
// covering `keyspace` blocks along each axis.
struct ScenarioRegion {
    std::int64_t chunks_x = 1;
    std::int64_t chunks_y = 1;
};

[[nodiscard]] ScenarioRegion RegionFor(const Args& args, const GeometryInfo& geometry) {
    return ScenarioRegion{
        .chunks_x = static_cast<std::int64_t>(std::max<std::size_t>(1, args.keyspace / geometry.chunk_width_blocks)),
        .chunks_y = static_cast<std::int64_t>(std::max<std::size_t>(1, args.keyspace / geometry.chunk_height_blocks)),
    };
}

[[nodiscard]] bool IsGridScenario(Scenario scenario) {
    return scenario == Scenario::kWorld || scenario == Scenario::kCanvas || scenario == Scenario::kSimulation;
}

// A chunk state with every block present and payload bytes `fill`; padding
// bits past the last block stay zero, as the protocol requires.
[[nodiscard]] std::string FullChunkState(const GeometryInfo& geometry, std::uint8_t fill) {
    std::string state(geometry.chunk_bytes + geometry.presence_bytes, '\0');
    std::fill(state.begin(), state.begin() + static_cast<std::ptrdiff_t>(geometry.chunk_bytes), static_cast<char>(fill));
    const std::size_t payload_tail = geometry.chunk_bits % 8U;
    if (payload_tail != 0U) {
        state[geometry.chunk_bytes - 1U] =
            static_cast<char>(fill & static_cast<std::uint8_t>((1U << payload_tail) - 1U));
    }
    const std::size_t blocks = geometry.chunk_width_blocks * geometry.chunk_height_blocks;
    std::fill(state.begin() + static_cast<std::ptrdiff_t>(geometry.chunk_bytes), state.end(), static_cast<char>(0xFF));
    const std::size_t presence_tail = blocks % 8U;
    if (presence_tail != 0U) {
        state.back() = static_cast<char>((1U << presence_tail) - 1U);
    }
    return state;
}

[[nodiscard]] ExpectedResponse IntegerExpected() {
    return ExpectedResponse{
        .kind = ExpectedResponse::Kind::kInteger,
        .prefix = {},
        .length = 0,
    };
}

// The chunk form (docs/CQL.md) of a bits table: version, schema version,
// presence, payload.
[[nodiscard]] std::size_t ChunkFormBytes(const GeometryInfo& geometry) {
    return 16U + geometry.presence_bytes + geometry.chunk_bytes;
}

[[nodiscard]] RequestPlan ChunkPutPlan(std::int64_t cx, std::int64_t cy, const GeometryInfo& geometry, std::uint8_t fill) {
    // The state is payload then presence; the form is presence then payload,
    // after a version SET CHUNK does not read.
    const std::string state = FullChunkState(geometry, fill);
    const std::string form =
        std::string(8, '\0') + LittleEndian64(geometry.schema_version) + state.substr(geometry.chunk_bytes) +
        state.substr(0, geometry.chunk_bytes);
    return RequestPlan{
        .command = "SET CHUNK " + std::to_string(cx) + " " + std::to_string(cy) + " IN default $1",
        .payload = "$" + std::to_string(form.size()) + "\r\n" + form,
        .expected = IntegerExpected(),
        .kind = RequestKind::kSetChunk,
    };
}

[[nodiscard]] RequestPlan ChunkGetPlan(std::int64_t cx, std::int64_t cy, const GeometryInfo& geometry) {
    return RequestPlan{
        .command = "GET CHUNK " + std::to_string(cx) + " " + std::to_string(cy) + " FROM default",
        .payload = {},
        .expected = ExpectedResponse{
            .kind = ExpectedResponse::Kind::kBulkBytesLength,
            .prefix = {},
            .length = ChunkFormBytes(geometry),
        },
        .kind = RequestKind::kGetChunk,
    };
}

[[nodiscard]] RequestPlan BlockSetPlan(std::int64_t x, std::int64_t y, const GeometryInfo& geometry, std::size_t request_index) {
    return RequestPlan{
        .command = "SET BLOCK " + std::to_string(x) + " " + std::to_string(y) + " IN default bits = b'" +
                   AlternatingBits(geometry.block_bits, (request_index % 2) == 0) + "'",
        .payload = {},
        .expected = IntegerExpected(),
        .kind = RequestKind::kSetBlock,
    };
}

[[nodiscard]] RequestPlan BlockGetPlan(std::int64_t x, std::int64_t y, const GeometryInfo& geometry) {
    return RequestPlan{
        .command = "GET BLOCK " + std::to_string(x) + " " + std::to_string(y) + " FROM default",
        .payload = {},
        .expected = ExpectedResponse{
            .kind = ExpectedResponse::Kind::kBlockValuesOrNull,
            .prefix = {},
            .length = CeilDiv(geometry.block_bits, static_cast<std::size_t>(8)),
        },
        .kind = RequestKind::kGetBlock,
    };
}

[[nodiscard]] ExpectedResponse AreaExpected(const GeometryInfo& geometry) {
    return ExpectedResponse{
        .kind = ExpectedResponse::Kind::kAreaArray,
        .prefix = {},
        .length = ChunkFormBytes(geometry),
    };
}

// A game world: each client is a player that walks one chunk at a time.
// Every 50 requests it moves and loads the chunks within 2 chunks of it
// (GET AREA AROUND), once saves its chunk whole (SET CHUNK), and otherwise writes
// (76%) and reads (20%) blocks within 2 chunks of it.
[[nodiscard]] RequestPlan WorldPlan(
    std::size_t request_index,
    std::mt19937& rng,
    ClientState& state,
    const ScenarioRegion& region,
    const GeometryInfo& geometry) {
    const std::size_t phase = request_index % 50U;
    if (phase == 0U) {
        std::uniform_int_distribution<int> step(-1, 1);
        state.player_cx = std::clamp<std::int64_t>(state.player_cx + step(rng), 0, region.chunks_x - 1);
        state.player_cy = std::clamp<std::int64_t>(state.player_cy + step(rng), 0, region.chunks_y - 1);
        RequestPlan plan;
        plan.command = "GET AREA AROUND " + std::to_string(state.player_cx) + " " +
                       std::to_string(state.player_cy) + " RADIUS 2 FROM default";
        plan.expected = AreaExpected(geometry);
        plan.kind = RequestKind::kGetArea;
        return plan;
    }
    if (phase == 25U) {
        return ChunkPutPlan(state.player_cx, state.player_cy, geometry, static_cast<std::uint8_t>(request_index));
    }
    const auto width = static_cast<std::int64_t>(geometry.chunk_width_blocks);
    const auto height = static_cast<std::int64_t>(geometry.chunk_height_blocks);
    std::uniform_int_distribution<std::int64_t> dx(-2 * width, 3 * width - 1);
    std::uniform_int_distribution<std::int64_t> dy(-2 * height, 3 * height - 1);
    const std::int64_t x = std::clamp<std::int64_t>(state.player_cx * width + dx(rng), 0, region.chunks_x * width - 1);
    const std::int64_t y = std::clamp<std::int64_t>(state.player_cy * height + dy(rng), 0, region.chunks_y * height - 1);
    if (phase % 5U == 1U) {
        return BlockGetPlan(x, y, geometry);
    }
    return BlockSetPlan(x, y, geometry, request_index);
}

// A shared canvas: clients write random blocks anywhere (95%), and one
// request in 20 reads a 4x4-chunk viewport (GET AREA).
[[nodiscard]] RequestPlan CanvasPlan(
    std::size_t request_index,
    std::mt19937& rng,
    const ScenarioRegion& region,
    const GeometryInfo& geometry) {
    if (request_index % 20U == 0U) {
        std::uniform_int_distribution<std::int64_t> cx(0, std::max<std::int64_t>(0, region.chunks_x - 4));
        std::uniform_int_distribution<std::int64_t> cy(0, std::max<std::int64_t>(0, region.chunks_y - 4));
        const std::int64_t x0 = cx(rng);
        const std::int64_t y0 = cy(rng);
        RequestPlan plan;
        const std::string x1 = std::to_string(std::min(x0 + 3, region.chunks_x - 1));
        const std::string y1 = std::to_string(std::min(y0 + 3, region.chunks_y - 1));
        plan.command =
            "GET AREA " + std::to_string(x0) + " " + std::to_string(y0) + " TO " + x1 + " " + y1 + " FROM default";
        plan.expected = AreaExpected(geometry);
        plan.kind = RequestKind::kGetArea;
        return plan;
    }
    std::uniform_int_distribution<std::int64_t> x(0, region.chunks_x * static_cast<std::int64_t>(geometry.chunk_width_blocks) - 1);
    std::uniform_int_distribution<std::int64_t> y(0, region.chunks_y * static_cast<std::int64_t>(geometry.chunk_height_blocks) - 1);
    return BlockSetPlan(x(rng), y(rng), geometry, request_index);
}

// A simulation step: each client sweeps the region from its own offset,
// reading a chunk whole and writing it back whole.
[[nodiscard]] RequestPlan SimulationPlan(
    std::size_t request_index,
    std::size_t client_id,
    const ScenarioRegion& region,
    const GeometryInfo& geometry) {
    const auto chunks = static_cast<std::size_t>(region.chunks_x * region.chunks_y);
    const std::size_t index = (client_id * 7919U + request_index / 2U) % chunks;
    const auto cx = static_cast<std::int64_t>(index) % region.chunks_x;
    const auto cy = static_cast<std::int64_t>(index) / region.chunks_x;
    if (request_index % 2U == 0U) {
        return ChunkGetPlan(cx, cy, geometry);
    }
    return ChunkPutPlan(cx, cy, geometry, static_cast<std::uint8_t>(request_index));
}

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
        case Scenario::kSet:
            return ScenarioPayload{geometry.block_bits, "simple"};
        case Scenario::kGet:
            return ScenarioPayload{geometry.block_bits, "bulk-bytes(bits)"};
        case Scenario::kChunkGetState:
            return ScenarioPayload{ChunkFormBytes(geometry), "bulk-bytes(chunk form) or null"};
        case Scenario::kMixed:
            return ScenarioPayload{geometry.block_bits, "mixed(get/set)"};
        case Scenario::kWorld:
            return ScenarioPayload{geometry.block_bits, "world(set/get block, get area around, set chunk)"};
        case Scenario::kCanvas:
            return ScenarioPayload{geometry.block_bits, "canvas(set block, get area)"};
        case Scenario::kTakeover: return ScenarioPayload{0, "idle slot ownership cycle"};
        case Scenario::kSimulation:
            return ScenarioPayload{
                geometry.chunk_bytes + geometry.presence_bytes, "simulation(chunkget/chunkput state)"};
    }
    return ScenarioPayload{0, "unknown"};
}

[[nodiscard]] RequestPlan BuildPlainRequestPlan(
    Scenario scenario,
    std::size_t request_index,
    std::size_t client_id,
    std::mt19937& rng,
    ClientState& state,
    const Args& args,
    const GeometryInfo& geometry) {
    switch (scenario) {
        case Scenario::kWorld:
            return WorldPlan(request_index, rng, state, RegionFor(args, geometry), geometry);
        case Scenario::kCanvas:
            return CanvasPlan(request_index, rng, RegionFor(args, geometry), geometry);
        case Scenario::kSimulation:
            return SimulationPlan(request_index, client_id, RegionFor(args, geometry), geometry);
        default:
            break;
    }
    std::uniform_int_distribution<int> coords(0, static_cast<int>(args.keyspace - 1));
    const int x = coords(rng);
    const int y = coords(rng);

    switch (scenario) {
        case Scenario::kPing:
            return RequestPlan{
                .command = "PING",
                .payload = {},
                .expected = ExpectedResponse{
                    .kind = ExpectedResponse::Kind::kSimplePrefix,
                    .prefix = "+PONG",
                    .length = 0,
                },
            };
        case Scenario::kChunkGetState: {
            auto plan = ChunkGetPlan(x, y, geometry);
            plan.expected.kind = ExpectedResponse::Kind::kChunkFormOrNull;
            return plan;
        }
        case Scenario::kGet:
            return BlockGetPlan(x, y, geometry);
        case Scenario::kMixed:
            return (request_index % 10) < 7 ? BlockGetPlan(x, y, geometry)
                                            : BlockSetPlan(x, y, geometry, request_index);
        default:
            return BlockSetPlan(x, y, geometry, request_index);
    }
}

[[nodiscard]] RequestPlan BuildRequestPlan(
    Scenario scenario, std::size_t request_index, std::size_t client_id,
    std::mt19937& rng, ClientState& state, const Args& args, const GeometryInfo& geometry) {
    auto plan = BuildPlainRequestPlan(scenario, request_index, client_id, rng, state, args, geometry);
    if (plan.kind == RequestKind::kGetBlock) {
        plan.expected.with_version = args.with_metadata == "version" || args.with_metadata == "both";
        plan.expected.with_schema = args.with_metadata == "schema" || args.with_metadata == "both";
    } else if (plan.kind == RequestKind::kGetArea) {
        plan.expected.with_schema = args.with_metadata == "schema" || args.with_metadata == "both";
    }
    plan.expected.schema_version = geometry.schema_version;
    if (plan.expected.with_version) plan.command += " WITH VERSION";
    if (plan.expected.with_schema) plan.command += " WITH SCHEMA";
    return plan;
}

std::size_t ValidateResponse(
    Client& client,
    const ExpectedResponse& expected,
    std::string_view scenario_name, std::uint64_t* write_version = nullptr) {
    if (expected.with_version || expected.with_schema) {
        const auto count = client.ReadArrayHeader();
        if (count != 1U + static_cast<std::size_t>(expected.with_version) + static_cast<std::size_t>(expected.with_schema))
            throw std::runtime_error("invalid read metadata envelope");
        if (expected.with_version) {
            const auto version = ReadValue(client);
            if (version.type != '_') (void)Revision(version);
        }
        if (expected.with_schema && Revision(ReadValue(client)) != expected.schema_version)
            throw std::runtime_error("read schema disagrees with DESCRIBE");
    }
    switch (expected.kind) {
        case ExpectedResponse::Kind::kSimplePrefix: {
            const std::string line = TrimCrLf(client.ReadSimpleLine());
            if (line.rfind(expected.prefix, 0) != 0) {
                throw std::runtime_error(
                    "scenario=" + std::string(scenario_name) +
                    " validation failed: expected simple prefix '" + expected.prefix +
                    "', got '" + line + "'");
            }
            return 0;
        }
        case ExpectedResponse::Kind::kBlockValuesOrNull: {
            const std::string header = TrimCrLf(client.ReadReplyLine());
            if (header == "_") {
                return 0;
            }
            if (header != "*1") {
                throw std::runtime_error(
                    "scenario=" + std::string(scenario_name) + " validation failed: expected *1 or _, got '" +
                    header + "'");
            }
            const std::string payload = client.ReadBulkText();
            if (payload.size() != expected.length) {
                throw std::runtime_error(
                    "scenario=" + std::string(scenario_name) + " validation failed: expected " +
                    std::to_string(expected.length) + " bytes of bits, got " + std::to_string(payload.size()));
            }
            return 0;
        }
        case ExpectedResponse::Kind::kAreaArray: {
            const std::string header = TrimCrLf(client.ReadReplyLine());
            if (header.size() < 2 || header[0] != '*') {
                throw std::runtime_error(
                    "scenario=" + std::string(scenario_name) + " validation failed: expected an array, got '" +
                    header + "'");
            }
            const std::size_t items = std::stoull(header.substr(1));
            for (std::size_t i = 0; i < items; ++i) {
                const std::string triple = TrimCrLf(client.ReadReplyLine());
                const std::string cx = TrimCrLf(client.ReadReplyLine());
                const std::string cy = TrimCrLf(client.ReadReplyLine());
                const std::string form = client.ReadBulkText();
                if (triple != "*3" || cx.empty() || cx[0] != ':' || cy.empty() || cy[0] != ':' ||
                    form.size() != expected.length) {
                    throw std::runtime_error(
                        "scenario=" + std::string(scenario_name) + " validation failed: area entry " + triple + " " +
                        cx + " " + cy + " with " + std::to_string(form.size()) + " bytes");
                }
            }
            return items;
        }
        case ExpectedResponse::Kind::kInteger: {
            const std::string line = TrimCrLf(client.ReadReplyLine());
            if (line.size() < 2 || (line[0] != ':' && line[0] != '(')) {
                throw std::runtime_error(
                    "scenario=" + std::string(scenario_name) + " validation failed: expected :<version>, got '" +
                    line + "'");
            }
            const auto version = Unsigned(std::string_view(line).substr(1U));
            if (write_version) *write_version = version;
            return 0;
        }
        case ExpectedResponse::Kind::kChunkFormOrNull:
        case ExpectedResponse::Kind::kBulkBytesLength: {
            if (expected.kind == ExpectedResponse::Kind::kChunkFormOrNull) {
                const auto header = client.ReadReplyLine();
                if (header == "_\r\n") {
                    return 0;
                }
                client.Unread(header);
            }
            const auto payload = client.ReadBulkBytes();
            if (payload.size() != expected.length) {
                throw std::runtime_error(
                    "scenario=" + std::string(scenario_name) +
                    " validation failed: expected bulk bytes length " + std::to_string(expected.length) +
                    ", got " + std::to_string(payload.size()));
            }
            return 0;
        }
    }
    throw std::logic_error("unknown benchmark response kind");
}

// Writes every chunk of a grid scenario's region whole, so area reads and
// block reads find data; not timed.
void FillRegion(const Args& args, const GeometryInfo& geometry) {
    const ScenarioRegion region = RegionFor(args, geometry);
    Client client(args.host, args.port, args.io_timeout_ms);
    Hello(client, args);
    constexpr std::size_t kWindow = 64;
    std::size_t in_flight = 0;
    for (std::int64_t cy = 0; cy < region.chunks_y; ++cy) {
        for (std::int64_t cx = 0; cx < region.chunks_x; ++cx) {
            const RequestPlan plan = ChunkPutPlan(cx, cy, geometry, static_cast<std::uint8_t>(cx + cy));
            client.SendLineWithPayload(plan.command, plan.payload);
            if (++in_flight == kWindow) {
                ValidateResponse(client, plan.expected, "fill");
                --in_flight;
            }
        }
    }
    const ExpectedResponse version = IntegerExpected();
    for (; in_flight > 0; --in_flight) {
        ValidateResponse(client, version, "fill");
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
    RequestKind kind = RequestKind::kPing;
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
    if (IsGridScenario(scenario)) {
        FillRegion(args, geometry);
    }

    std::unique_ptr<WatchingClient> watch;
    if (!args.watch_table.empty()) watch = std::make_unique<WatchingClient>(args);

    std::mutex latencies_mutex;
    std::vector<double> latencies_ms;
    latencies_ms.reserve(args.requests);
    const auto chunk_blocks = geometry.chunk_width_blocks * geometry.chunk_height_blocks;
    std::size_t completed_blocks = 0;
    std::size_t returned_area_chunks = 0;
    std::size_t response_bytes = 0;
    std::size_t metadata_envelopes = 0;
    std::optional<std::uint64_t> write_frontier;
    std::array<std::size_t, kRequestNames.size()> request_mix{};

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
                Client client(args.host, args.port, args.io_timeout_ms);
                Hello(client, args);

                std::mt19937 rng(
                    args.seed ^
                    static_cast<std::uint32_t>((work.client_id + 1U) * 2654435761ULL) ^
                    static_cast<std::uint32_t>(static_cast<int>(scenario) * 2246822519ULL));

                const ScenarioRegion region = RegionFor(args, geometry);
                ClientState state{
                    .player_cx = static_cast<std::int64_t>(work.client_id) % region.chunks_x,
                    .player_cy = static_cast<std::int64_t>(work.client_id / static_cast<std::size_t>(region.chunks_x)) %
                                 region.chunks_y,
                };
                std::deque<PendingRequest> pending;
                std::vector<double> local_latencies_ms;
                local_latencies_ms.reserve(work.request_count);
                std::size_t local_max_in_flight = 0;
                std::size_t local_blocks = 0;
                std::size_t local_area_chunks = 0;
                std::size_t local_response_bytes = 0;
                std::size_t local_metadata_envelopes = 0;
                std::optional<std::uint64_t> local_write_frontier;
                std::array<std::size_t, kRequestNames.size()> local_mix{};

                auto consume_one = [&]() {
                    if (pending.empty()) {
                        return;
                    }
                    const PendingRequest request = pending.front();
                    pending.pop_front();
                    const auto before_bytes = client.consumed_bytes();
                    std::uint64_t write_version = 0;
                    const auto area_chunks = ValidateResponse(client, request.expected, ScenarioName(scenario), &write_version);
                    if (request.kind == RequestKind::kSetBlock || request.kind == RequestKind::kSetChunk)
                        local_write_frontier = std::max(local_write_frontier.value_or(0U), write_version);
                    local_response_bytes += client.consumed_bytes() - before_bytes;
                    if (request.expected.with_version || request.expected.with_schema) ++local_metadata_envelopes;
                    ++local_mix[static_cast<std::size_t>(request.kind)];
                    switch (request.kind) {
                        case RequestKind::kPing: break;
                        case RequestKind::kSetBlock:
                        case RequestKind::kGetBlock: ++local_blocks; break;
                        case RequestKind::kSetChunk:
                        case RequestKind::kGetChunk: local_blocks += chunk_blocks; break;
                        case RequestKind::kGetArea:
                            local_area_chunks += area_chunks;
                            local_blocks += area_chunks * chunk_blocks;
                            break;
                    }
                    const auto now = Clock::now();
                    const auto latency_ms =
                        std::chrono::duration<double, std::milli>(now - request.sent_at).count();
                    local_latencies_ms.push_back(latency_ms);
                };

                for (std::size_t i = 0; i < work.request_count; ++i) {
                    if (failed.load(std::memory_order_acquire)) {
                        return;
                    }
                    const RequestPlan plan = BuildRequestPlan(scenario, i, work.client_id, rng, state, args, geometry);
                    if (plan.payload.empty()) {
                        client.SendLine(plan.command);
                    } else {
                        client.SendLineWithPayload(plan.command, plan.payload);
                    }
                    pending.push_back(PendingRequest{
                        .sent_at = Clock::now(),
                        .expected = plan.expected,
                        .kind = plan.kind,
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
                    completed_blocks += local_blocks;
                    returned_area_chunks += local_area_chunks;
                    response_bytes += local_response_bytes;
                    metadata_envelopes += local_metadata_envelopes;
                    if (local_write_frontier) write_frontier = std::max(write_frontier.value_or(0U), *local_write_frontier);
                    for (std::size_t i = 0; i < request_mix.size(); ++i) request_mix[i] += local_mix[i];
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
    result.completed_blocks = completed_blocks;
    result.returned_area_chunks = returned_area_chunks;
    result.response_bytes = response_bytes;
    result.metadata_envelopes = metadata_envelopes;
    result.blocks_per_request = static_cast<double>(completed_blocks) / result.completed_requests;
    for (std::size_t i = 0; i < request_mix.size(); ++i) result.request_mix.emplace_back(kRequestNames[i], request_mix[i]);
    result.duration_s = std::chrono::duration<double>(end - start).count();
    result.throughput_req_s =
        result.duration_s > 0.0
            ? static_cast<double>(result.completed_requests) / result.duration_s
            : 0.0;
    result.throughput_blocks_s = result.duration_s > 0.0 ? static_cast<double>(completed_blocks) / result.duration_s : 0.0;
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
    if (watch) watch->Finish(&result, write_frontier);
    return result;
}

// Error fields precede the human message. Unknown fields may appear in any
// order; the known reason must occur exactly once with valid percent encoding.
bool TakeoverTerminal(std::string_view line) {
    std::istringstream input{std::string(line)};
    std::string marker, code, token;
    if (!(input >> marker >> code) || marker != "-ERR" || code != "KILLED") return false;
    std::optional<std::string> reason;
    const auto hex = [](char digit) -> int {
        if (digit >= '0' && digit <= '9') return digit - '0';
        if (digit >= 'a' && digit <= 'f') return digit - 'a' + 10;
        if (digit >= 'A' && digit <= 'F') return digit - 'A' + 10;
        return -1;
    };
    while (input >> token) {
        const auto equal = token.find('=');
        if (equal == std::string::npos) break; // The remaining tokens are message text.
        const auto key = token.substr(0, equal);
        if (key.empty() || key.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789_") != std::string::npos)
            return false;
        std::string value;
        for (std::size_t i = equal + 1U; i < token.size(); ++i) {
            if (token[i] == '%') {
                if (token.size() - i < 3U || hex(token[i + 1U]) < 0 || hex(token[i + 2U]) < 0) return false;
                value += static_cast<char>(hex(token[i + 1U]) * 16 + hex(token[i + 2U])); i += 2U;
            } else {
                const auto byte = static_cast<unsigned char>(token[i]);
                if (byte <= 0x20U || byte >= 0x7fU || byte == '=') return false;
                value += token[i];
            }
        }
        if (key == "reason") { if (reason) return false; reason = std::move(value); }
    }
    return reason && *reason == "slot_takeover";
}

// Idle takeover measures an accepted new owner plus the old peer's closure.
// There are no delivered mutations and no ACKs; no unsent revision is ACKed.
[[nodiscard]] ScenarioResult RunTakeover(const Args& args) {
    Client admin(args.host, args.port, args.io_timeout_ms);
    Hello(admin, args);
    const std::string slot = "bench_" + std::to_string(Clock::now().time_since_epoch().count());
    admin.SendLine("CREATE SLOT '" + slot + "' ON default");
    if (TrimCrLf(admin.ReadSimpleLine()) != "+OK") throw std::runtime_error("takeover CREATE SLOT failed");
    auto owner = std::make_unique<Client>(args.host, args.port, args.io_timeout_ms);
    Hello(*owner, args);
    owner->SendLine("WATCH default SLOT '" + slot + "'");
    const auto baseline = WatchStart(*owner);
    admin.SendLine("SHOW SLOTS ON default");
    const auto slots = ReadValue(admin);
    if (slots.type != '*') throw std::runtime_error("invalid SHOW SLOTS reply");
    bool found = false;
    for (const auto& record : slots.children) {
        if (Field(record, "name").text != slot) continue;
        if (found || Field(record, "epoch").text != baseline.epoch ||
            Revision(Field(record, "acked")) != baseline.revision)
            throw std::runtime_error("takeover baseline disagrees with persisted slot frontier");
        found = true;
    }
    if (!found) throw std::runtime_error("takeover slot missing from SHOW SLOTS");
    std::vector<double> latencies;
    latencies.reserve(args.requests);
    ScenarioResult result; result.name = "takeover"; result.max_in_flight = 1;
    result.payload_label = "ownership_cycles (connect+HELLO+WATCH TAKEOVER+old closure), ACKs=0";
    const auto start = Clock::now();
    for (std::size_t i = 0; i < args.requests; ++i) {
        const auto cycle_start = Clock::now();
        auto next = std::make_unique<Client>(args.host, args.port, args.io_timeout_ms);
        Hello(*next, args);
        next->SendLine("WATCH default SLOT '" + slot + "' TAKEOVER");
        const auto position = WatchStart(*next);
        if (position.epoch != baseline.epoch || position.revision != baseline.revision)
            throw std::runtime_error("idle takeover start changed the slot frontier");
        const auto old_before = owner->consumed_bytes();
        const auto deadline = Clock::now() + std::chrono::milliseconds(args.io_timeout_ms);
        const std::atomic<Clock::duration::rep> old_deadline{deadline.time_since_epoch().count()};
        owner->SetStopDeadline(&old_deadline);
        bool killed = false;
        for (;;) {
            if (Clock::now() >= deadline || !owner->WaitReply(std::chrono::milliseconds(args.io_timeout_ms)))
                throw std::runtime_error("takeover waited for old owner closure past its I/O deadline");
            try {
                const auto line = owner->ReadReplyLine();
                if (line.size() < 2U || line.substr(line.size() - 2U) != "\r\n")
                    throw std::runtime_error("invalid takeover terminal line terminator");
                const auto trimmed = TrimCrLf(line);
                if (!trimmed.empty() && trimmed.front() == '-') {
                    if (killed || !TakeoverTerminal(trimmed)) throw std::runtime_error("invalid takeover terminal error fields");
                    killed = true; continue;
                }
                owner->Unread(line);
                const auto push = ReadValue(*owner);
                if (killed || push.type != '>' || push.children.size() < 3U ||
                    push.children[0].type != '$' || push.children[0].text != "progress" ||
                    push.children[1].type != '$' || push.children[1].text != baseline.epoch ||
                    Revision(push.children[2]) != baseline.revision)
                    throw std::runtime_error("unexpected idle takeover terminal event");
            } catch (const PeerClosed&) { break; }
        }
        result.response_bytes += next->consumed_bytes() + owner->consumed_bytes() - old_before;
        owner = std::move(next);
        latencies.push_back(std::chrono::duration<double, std::milli>(Clock::now() - cycle_start).count());
    }
    const auto end = Clock::now();
    owner->SendLine("UNWATCH");
    const auto cleanup_deadline = Clock::now() + std::chrono::milliseconds(args.io_timeout_ms);
    const std::atomic<Clock::duration::rep> cleanup_read_deadline{cleanup_deadline.time_since_epoch().count()};
    owner->SetStopDeadline(&cleanup_read_deadline);
    for (;;) {
        if (Clock::now() >= cleanup_deadline) throw std::runtime_error("takeover waited for UNWATCH cleanup past deadline");
        const auto reply = ReadValue(*owner);
        if (reply.type == '+' && reply.text == "OK") break;
        if (reply.type != '>' || reply.children.size() < 3U || reply.children[0].type != '$' || reply.children[0].text != "progress" ||
            reply.children[1].type != '$' || reply.children[1].text != baseline.epoch || Revision(reply.children[2]) != baseline.revision)
            throw std::runtime_error("takeover cleanup received an unexpected event");
    }
    owner.reset();
    admin.SendLine("DROP SLOT '" + slot + "' ON default");
    if (TrimCrLf(admin.ReadSimpleLine()) != "+OK") throw std::runtime_error("takeover DROP SLOT failed");
    std::sort(latencies.begin(), latencies.end());
    result.ownership_cycles = latencies.size();
    result.completed_requests = latencies.size() * (args.user.empty() ? 2U : 3U);
    result.duration_s = std::chrono::duration<double>(end - start).count();
    result.throughput_req_s = result.duration_s > 0.0 ? result.completed_requests / result.duration_s : 0.0;
    result.request_mix = {{"hello", args.requests}, {"watch_takeover", args.requests}};
    if (!args.user.empty()) result.request_mix.emplace_back("auth", args.requests);
    result.latency_min_ms = latencies.front(); result.latency_max_ms = latencies.back();
    result.latency_avg_ms = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    result.latency_p50_ms = PercentileFromSorted(latencies, 50.0);
    result.latency_p95_ms = PercentileFromSorted(latencies, 95.0);
    result.latency_p99_ms = PercentileFromSorted(latencies, 99.0);
    for (const auto p : kPercentiles) result.percentiles_ms.emplace_back(p, PercentileFromSorted(latencies, p));
    return result;
}

void ValidateEnabledArgs(const Args& args) {
    if (args.with_metadata != "none" && args.with_metadata != "version" &&
        args.with_metadata != "schema" && args.with_metadata != "both") throw std::invalid_argument("invalid --with value");
    if (args.watch_mode != "ordinary" && args.watch_mode != "progress" &&
        args.watch_mode != "filtered" && args.watch_mode != "chunks") throw std::invalid_argument("invalid --watch-mode value");
    if (args.watch_mode != "ordinary" && args.watch_table != "default")
        throw std::invalid_argument("enabled watch modes require --watch default");
    if (std::find(args.tests.begin(), args.tests.end(), Scenario::kTakeover) != args.tests.end() &&
        (args.clients != 1U || args.pipeline != 1U || !args.watch_table.empty() || args.with_metadata != "none"))
        throw std::invalid_argument("takeover requires clients=1, pipeline=1, no --watch or --with");
}

void WaitForServerReady(const Args& args) {
    std::string last_error = "unknown";
    for (int attempt = 0; attempt < 100; ++attempt) {
        try {
            Client client(args.host, args.port, args.io_timeout_ms);
            Hello(client, args);
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
        case Scenario::kSet:
            return "set";
        case Scenario::kGet:
            return "get";
        case Scenario::kChunkGetState:
            return "chunkgetstate";
        case Scenario::kMixed:
            return "mixed";
        case Scenario::kWorld:
            return "world";
        case Scenario::kCanvas:
            return "canvas";
        case Scenario::kSimulation:
            return "simulation";
        case Scenario::kTakeover: return "takeover";
    }
    return "unknown";
}

std::vector<Scenario> DefaultScenarios() {
    return {
        Scenario::kPing,
        Scenario::kSet,
        Scenario::kGet,
        Scenario::kChunkGetState,
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
        << "  --io-timeout-ms <N>              send/receive wait: 1..86400000 ms (default: 5000);\n"
        << "                                   also configures the spawned server\n"
        << "  --requests <N>                   default: 5000\n"
        << "  --ops <N>                        alias for --requests\n"
        << "  --tests <list>                   comma list: ping,set,get,chunkgetstate,mixed,\n"
        << "                                   world,canvas,simulation (grid workloads over keyspace x\n"
        << "                                   keyspace blocks, filled before they are timed)\n"
        << "  --keyspace <N>                   default: 512\n"
        << "  --seed <N>                       default: 1337\n"
        << "  --durability-mode <mode>         spawn mode: relaxed (default), fsync-wal, fsync-checkpoint\n"
        << "  --watch <table>                  read pushes after each scenario warmup\n"
        << "  --watch-mode <mode>              report-only: ordinary (default), progress, filtered, chunks\n"
        << "  --with <none|version|schema|both> report-only metadata on applicable block/area reads\n"
        << "  --tests takeover                report-only idle ownership cycles; clients/pipeline must be 1\n"
        << "  --server-workers <N>             spawn mode: server worker threads (default: 4); a\n"
        << "                                   connection holds one, so use at least --clients\n"
        << "  --user <name>                    log in as this user (also from --uri)\n"
        << "  --password-file <path>           the user's password (first line)\n"
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
    bool user_overridden = false;

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
        if (arg == "--watch") {
            args.watch_table = require_value("--watch");
            if (args.watch_table.empty() || args.watch_table.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789_") != std::string::npos ||
                (args.watch_table.front() >= '0' && args.watch_table.front() <= '9'))
                throw std::invalid_argument("--watch takes a lowercase table name");
            continue;
        }
        if (arg == "--with") {
            args.with_metadata = require_value("--with");
            if (args.with_metadata != "none" && args.with_metadata != "version" &&
                args.with_metadata != "schema" && args.with_metadata != "both")
                throw std::invalid_argument("invalid --with value");
            continue;
        }
        if (arg == "--watch-mode") {
            args.watch_mode = require_value("--watch-mode");
            if (args.watch_mode != "ordinary" && args.watch_mode != "progress" &&
                args.watch_mode != "filtered" && args.watch_mode != "chunks")
                throw std::invalid_argument("invalid --watch-mode value");
            continue;
        }
        if (arg == "--io-timeout-ms") {
            const auto value = require_value("--io-timeout-ms");
            const auto parsed = std::from_chars(value.data(), value.data() + value.size(), args.io_timeout_ms);
            if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size() ||
                args.io_timeout_ms == 0 || args.io_timeout_ms > 86400000) {
                throw std::invalid_argument("--io-timeout-ms must be between 1 and 86400000");
            }
            continue;
        }
        if (arg == "--server-workers") {
            args.server_workers = ParsePositiveSize(require_value("--server-workers"), "--server-workers");
            continue;
        }
        if (arg == "--durability-mode") {
            args.durability_mode = require_value("--durability-mode");
            (void)ParseDurabilityMode(args.durability_mode);
            continue;
        }
        if (arg == "--user") {
            args.user = require_value("--user");
            user_overridden = true;
            continue;
        }
        if (arg == "--password-file") {
            std::ifstream file(require_value("--password-file"));
            if (!std::getline(file, args.password)) {
                throw std::invalid_argument("cannot read --password-file");
            }
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
        if (!user_overridden && !parsed_uri->user.empty()) {
            args.user = parsed_uri->user;
            // --password-file wins over a password in the URI.
            if (args.password.empty()) {
                args.password = parsed_uri->password;
            }
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
    ValidateEnabledArgs(args);
    return args;
}

BenchmarkReport Run(const Args& args) {
    ValidateEnabledArgs(args);
    if (args.io_timeout_ms == 0 || args.io_timeout_ms > 86400000) {
        throw std::invalid_argument("--io-timeout-ms must be between 1 and 86400000");
    }
    ScopedSocketPlatform socket_platform;
    (void)socket_platform;
    SetLogLevel(args.log_level);

    BenchmarkReport report;
    report.source_commit = CHUNKDB_BENCH_SOURCE_COMMIT;
    report.source_state = CHUNKDB_BENCH_SOURCE_STATE;
    report.benchmark_source_sha256 = CHUNKDB_BENCH_SOURCE_SHA256;
    report.build_type = CHUNKDB_BENCH_BUILD_TYPE;
    report.server_mode = args.server_mode;
    report.spawned_server = false;
    report.host = args.host;
    report.port = args.port;
    report.requested_clients = args.clients;
    report.active_clients = 0;
    report.pipeline = args.pipeline;
    report.io_timeout_ms = args.io_timeout_ms;
    report.requests = args.requests;
    report.keyspace = args.keyspace;
    report.seed = args.seed;
    report.with_metadata = args.with_metadata;
    report.watch_mode = args.watch_mode;
    report.watch_table = args.watch_table;

    auto run_against_endpoint = [&]() {
        const GeometryInfo geometry = LoadGeometryInfo(args);
        report.chunk_width_blocks = geometry.chunk_width_blocks;
        report.chunk_height_blocks = geometry.chunk_height_blocks;
        const auto split = BuildWorkSplit(args.requests, args.clients);
        report.active_clients = CountActiveWorkers(split);
        report.results.reserve(args.tests.size());
        for (const Scenario scenario : args.tests) {
            report.results.push_back(scenario == Scenario::kTakeover
                ? RunTakeover(args) : RunScenario(scenario, args, geometry));
        }
    };

    if (args.server_mode == ServerMode::kExternal) {
        run_against_endpoint();
        return report;
    }

    const auto data_dir = std::filesystem::temp_directory_path() /
                          ("chunkdb-server-bench-" +
                           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));

    std::shared_ptr<TableCatalog> catalog;
    std::shared_ptr<CommandEngine> engine;
    std::unique_ptr<ChunkServer> server;
    std::thread server_thread;
    try {
        const StoreConfig store_config{
            .geometry = {
                .large_chunk_width_chunks = 8,
                .large_chunk_height_chunks = 8,
                .chunk_width_blocks = 16,
                .chunk_height_blocks = 16,
                .block_bits = 16,
            },
            .data_dir = data_dir,
            .durability_mode = ParseDurabilityMode(args.durability_mode),
            .checkpoint_update_interval = 512,
            .checkpoint_wal_bytes = 1024 * 1024,
            .wal_group_commit_updates = 8,
            .max_loaded_chunks = 16384,
            .max_open_wal_streams = 1024,
            .allow_multiple_processes = false,
        };
        catalog = std::make_shared<TableCatalog>(CatalogConfigFromStoreConfig(store_config));
        (void)catalog->Create("default", store_config.geometry, catalog->default_options());

        // With a user, the spawned server requires logins and that user is
        // its first administrator.
        std::shared_ptr<UserRegistry> users;
        if (!args.user.empty()) {
            const auto secret_bytes = chunkdb::crypto::RandomBytes(32);
            std::array<std::uint8_t, 32> secret{};
            std::copy(secret_bytes.begin(), secret_bytes.end(), secret.begin());
            users = std::make_shared<UserRegistry>(
                data_dir,
                std::make_pair(
                    args.user,
                    chunkdb::scram::MakeVerifier(args.password, chunkdb::crypto::RandomBytes(16), chunkdb::scram::kMinIterations)),
                secret);
        }
        engine = std::make_shared<CommandEngine>(
            EngineConfig{
                .require_auth = !args.user.empty(),
                .users = users,
                .max_auth_failures = 5,
            },
            catalog);

        server = std::make_unique<ChunkServer>(
            ServerConfig{
                .host = args.host,
                .port = args.port,
                .max_line_bytes = 65536,
                .worker_threads = args.server_workers,
                .client_io_timeout_ms = args.io_timeout_ms,
                .tls_enabled = false,
                .tls_cert_path = "",
                .tls_key_path = "",
            },
            engine);

        server_thread = std::thread([&]() { server->Run(); });
        WaitForServerReady(args);

        report.spawned_server = true;
        report.chunk_lock_mode = chunkdb::ChunkLockModeName();
        report.durability_mode = DurabilityModeName(ParseDurabilityMode(args.durability_mode));
        run_against_endpoint();

        server->Stop();
        if (server_thread.joinable()) {
            server_thread.join();
        }
        server.reset();
        engine.reset();
        catalog.reset();
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
        catalog.reset();
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
    out << "source_commit=" << report.source_commit << " source_state=" << report.source_state
        << " build_type=" << report.build_type << " chunk=" << report.chunk_width_blocks
        << "x" << report.chunk_height_blocks << "\n";
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
        << " io_timeout_ms=" << report.io_timeout_ms
        << " pipeline=" << report.pipeline
        << " keyspace=" << report.keyspace
        << " seed=" << report.seed
        << " keepalive=on\n";
    if (!report.durability_mode.empty()) {
        out << "durability_mode=" << report.durability_mode << "\n";
    }
    if (report.active_clients < report.requested_clients) {
        out << "some clients were idle due to requests distribution\n";
    }
    out << "tests=" << JoinResultNames(report.results) << "\n";
    const bool descriptive = report.with_metadata != "none" || !report.watch_table.empty();
    if (descriptive) out << "report_only with=" << report.with_metadata << " watch=" << report.watch_table
                         << " watch_mode=" << report.watch_mode << "\n";

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
        if (descriptive || result.ownership_cycles != 0U) {
            out << "Response bytes: " << result.response_bytes << " Metadata envelopes: " << result.metadata_envelopes
                << " Watch bytes: " << result.watch_bytes << " change=" << result.watch_changes
                << " progress=" << result.watch_progress << " chunks=" << result.watch_chunks << " other=" << result.watch_other
                << " watch_duration_s=" << result.watch_duration_s << "\n";
            if (result.ownership_cycles != 0U)
                out << "Ownership cycles: " << result.ownership_cycles << " cycles/s="
                    << result.ownership_cycles / result.duration_s << " latency_unit=ownership_cycle ACKs=0\n";
        }
        out << "Throughput (req/s): " << std::fixed << std::setprecision(2) << result.throughput_req_s << "\n";
        out << "Blocks: " << result.completed_blocks << "  Blocks/request: " << result.blocks_per_request
            << "  Blocks/s: " << result.throughput_blocks_s << "  Mix:";
        for (const auto& [kind, count] : result.request_mix) out << " " << kind << "=" << count;
        out << "\n";
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
    out << "\"io_timeout_ms\":" << report.io_timeout_ms << ",";
    out << "\"keyspace\":" << report.keyspace << ",";
    out << "\"seed\":" << report.seed << ",";
    out << "\"durability_mode\":\"" << JsonEscape(report.durability_mode) << "\",";
    out << "\"keepalive\":\"on\",";
    out << "\"chunk_lock_mode\":\"" << JsonEscape(report.chunk_lock_mode) << "\",";
    out << "\"source_commit\":\"" << JsonEscape(report.source_commit) << "\",";
    out << "\"source_state\":\"" << JsonEscape(report.source_state) << "\",";
    out << "\"benchmark_source_sha256\":\"" << JsonEscape(report.benchmark_source_sha256) << "\",";
    out << "\"build_type\":\"" << JsonEscape(report.build_type) << "\",";
    out << "\"chunk_width_blocks\":" << report.chunk_width_blocks << ",";
    out << "\"chunk_height_blocks\":" << report.chunk_height_blocks << ",";
    const bool descriptive = report.with_metadata != "none" || !report.watch_table.empty();
    if (descriptive) out << "\"enabled_config\":{\"report_only\":true,\"with\":\"" << JsonEscape(report.with_metadata)
        << "\",\"watch\":\"" << JsonEscape(report.watch_table) << "\",\"watch_mode\":\""
        << JsonEscape(report.watch_mode) << "\"},";
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
        out << "\"completed_blocks\":" << r.completed_blocks << ",";
        out << "\"blocks_per_request\":" << std::fixed << std::setprecision(6) << r.blocks_per_request << ",";
        out << "\"throughput_blocks_s\":" << r.throughput_blocks_s << ",";
        out << "\"returned_area_chunks\":" << r.returned_area_chunks << ",";
        if (descriptive || r.ownership_cycles != 0U) {
            out << "\"response_bytes\":" << r.response_bytes << ",\"metadata_envelopes\":" << r.metadata_envelopes
                << ",\"watch_bytes\":" << r.watch_bytes << ",\"watch_changes\":" << r.watch_changes
                << ",\"watch_progress\":" << r.watch_progress << ",\"watch_chunks\":" << r.watch_chunks
                << ",\"watch_other\":" << r.watch_other << ",\"watch_duration_s\":" << r.watch_duration_s << ",";
            if (r.ownership_cycles != 0U)
                out << "\"report_only\":true,\"ownership_cycles\":" << r.ownership_cycles
                    << ",\"ownership_cycles_s\":" << r.ownership_cycles / r.duration_s
                    << ",\"latency_unit\":\"ownership_cycle\",\"acks_sent\":0,";
        }
        out << "\"request_mix\":{";
        for (std::size_t k = 0; k < r.request_mix.size(); ++k) {
            if (k) out << ",";
            out << "\"" << JsonEscape(r.request_mix[k].first) << "\":" << r.request_mix[k].second;
        }
        out << "},";
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
