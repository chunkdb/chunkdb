#include "chunkdb/server_bench.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <charconv>
#include <cmath>
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

    void Shutdown() noexcept {
#ifdef _WIN32
        (void)shutdown(socket_, SD_BOTH);
#else
        (void)shutdown(socket_, SHUT_RDWR);
#endif
    }
    // Transfer buffered input to a dedicated stream reader after its start reply.
    [[nodiscard]] std::string TakePending() { return std::move(pending_); }
    [[nodiscard]] std::size_t ReadRaw(char* buffer, std::size_t capacity) {
#ifdef _WIN32
        const auto read = recv(socket_, buffer, static_cast<int>(capacity), 0);
#else
        const auto read = recv(socket_, buffer, capacity, 0);
#endif
        if (read <= 0) throw std::runtime_error("recv failed while reading watch");
        return static_cast<std::size_t>(read);
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
        pending_ = header + pending_;
        return ReadBulkText();
    }

    // Puts `text` back in front of what the next read returns.
    void Unread(const std::string& text) {
        pending_ = text + pending_;
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

// Every connection starts with HELLO 3, logging in with SCRAM-SHA-256 when
// the bench has a user. The reply is a map: server_version a bulk string,
// server_signature the SCRAM server-final message (or `_`), the rest
// integers.
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
    const std::string header = TrimCrLf(client.ReadReplyLine());
    if (header.size() < 2 || header[0] != '%') {
        throw std::runtime_error("unexpected HELLO reply: " + header);
    }
    const std::size_t pairs = std::stoull(header.substr(1));
    for (std::size_t pair = 0; pair < pairs; ++pair) {
        const std::string key = client.ReadBulkText();
        if (key == "server_version") {
            (void)client.ReadBulkText();
            continue;
        }
        if (key == "server_signature") {
            const std::string line = TrimCrLf(client.ReadReplyLine());
            std::string signature;
            if (line != "_") {
                client.Unread(line + "\r\n");
                signature = client.ReadBulkText();
            }
            if (signature != expected_signature) {
                throw std::runtime_error("the server did not prove it knows the user's password");
            }
            continue;
        }
        const std::string value = TrimCrLf(client.ReadReplyLine());
        if (value.empty() || value[0] != ':' || (key == "protocol" && value != ":3")) {
            throw std::runtime_error("unexpected HELLO value for " + key + ": " + value);
        }
    }
}

// Every scalar of one reply, in order, with arrays and maps flattened.
void ReadReplyScalars(Client& client, std::vector<std::string>* out) {
    const std::string line = TrimCrLf(client.ReadReplyLine());
    if (line.empty()) {
        throw std::runtime_error("empty reply line");
    }
    if (line[0] == '-') {
        throw std::runtime_error("server error response: " + line);
    }
    if (line[0] == '*' || line[0] == '%') {
        const std::size_t items = std::stoull(line.substr(1)) * (line[0] == '%' ? 2U : 1U);
        for (std::size_t i = 0; i < items; ++i) {
            ReadReplyScalars(client, out);
        }
        return;
    }
    if (line[0] == '$') {
        client.Unread(line + "\r\n");
        out->push_back(client.ReadBulkText());
        return;
    }
    out->push_back(line.substr(1));
}

// One reader owns all reply parsing; the caller only sends UNWATCH.
class WatchingClient {
  public:
    explicit WatchingClient(const Args& args) : client_(args.host, args.port) {
        Hello(client_, args);
        client_.SendLine("WATCH " + args.watch_table);
        const auto reply = client_.ReadSimpleLine();
        if (reply.rfind("+OK ", 0) != 0) throw std::runtime_error("WATCH failed: " + reply);
        input_ = client_.TakePending();
        reader_ = std::thread([this] {
            try {
                for (;;) {
                    const auto line = Line();
                    if (line == "+OK") return;
                    if (line.empty() || line[0] != '>') throw std::runtime_error("unexpected watch reply: " + std::string(line));
                    const auto count = Count(std::string_view(line).substr(1));
                    for (std::size_t i = 0; i < count; ++i) DrainValue();
                }
            } catch (const std::bad_alloc&) { error_ = std::current_exception(); }
              catch (const std::logic_error&) { error_ = std::current_exception(); }
              catch (const std::runtime_error&) { error_ = std::current_exception(); }
        });
    }
    ~WatchingClient() { client_.Shutdown(); if (reader_.joinable()) reader_.join(); }
    void Finish() {
        client_.SendLine("UNWATCH"); reader_.join();
        if (error_) std::rethrow_exception(error_);
    }
  private:
    static std::size_t Count(std::string_view digits) {
        std::size_t count = 0U;
        const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), count);
        if (digits.empty() || parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size())
            throw std::runtime_error("invalid watch length");
        return count;
    }
    void Refill() {
        // Compact only when reading another socket buffer, not per RESP scalar.
        input_.erase(0, at_);
        at_ = 0U;
        char buffer[4096];
        const auto read = client_.ReadRaw(buffer, sizeof(buffer));
        input_.append(buffer, read);
    }
    std::string_view Line() {
        for (;;) {
            const auto end = input_.find('\n', at_);
            if (end != std::string::npos) {
                if (end == at_ || input_[end - 1U] != '\r')
                    throw std::runtime_error("invalid watch line terminator");
                // Used before another read/refill: no scalar needs an owned copy.
                auto line = std::string_view(input_).substr(at_, end - at_ - 1U);
                at_ = end + 1U;
                return line;
            }
            Refill();
        }
    }
    char Byte() {
        if (at_ == input_.size()) Refill();
        return input_[at_++];
    }
    void Bulk(std::size_t size) {
        while (size != 0U) {
            if (at_ == input_.size()) Refill();
            const auto take = std::min(size, input_.size() - at_);
            at_ += take;
            size -= take;
        }
        if (Byte() != '\r' || Byte() != '\n') throw std::runtime_error("invalid watch bulk terminator");
    }
    void DrainValue() {
        const auto line = Line();
        if (line.empty()) throw std::runtime_error("empty watch value");
        if (line[0] == '*' || line[0] == '%') {
            auto count = Count(std::string_view(line).substr(1));
            if (line[0] == '%') {
                if (count > std::numeric_limits<std::size_t>::max() / 2U)
                    throw std::runtime_error("watch map length overflows");
                count *= 2U;
            }
            for (std::size_t i = 0; i < count; ++i) DrainValue();
        } else if (line[0] == '$') {
            Bulk(Count(std::string_view(line).substr(1)));
        } else if (line[0] != ':' && line[0] != ',' && line[0] != '#' && line[0] != '_') {
            throw std::runtime_error("invalid watch value: " + std::string(line));
        }
    }
    Client client_;
    std::string input_;
    std::size_t at_ = 0U;
    std::thread reader_;
    std::exception_ptr error_;
};

// The geometry of table `default`, from DESCRIBE: one bits(N) column and the
// chunk size.
[[nodiscard]] GeometryInfo LoadGeometryInfo(const Args& args) {
    Client client(args.host, args.port);
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
};

struct RequestPlan {
    std::string command;
    // Sent after the line when not empty: a parameter frame.
    std::string payload;
    ExpectedResponse expected;
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
    };
}

[[nodiscard]] RequestPlan BlockSetPlan(std::int64_t x, std::int64_t y, const GeometryInfo& geometry, std::size_t request_index) {
    return RequestPlan{
        .command = "SET BLOCK " + std::to_string(x) + " " + std::to_string(y) + " IN default bits = b'" +
                   AlternatingBits(geometry.block_bits, (request_index % 2) == 0) + "'",
        .payload = {},
        .expected = IntegerExpected(),
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
        case Scenario::kSimulation:
            return ScenarioPayload{
                geometry.chunk_bytes + geometry.presence_bytes, "simulation(chunkget/chunkput state)"};
    }
    return ScenarioPayload{0, "unknown"};
}

[[nodiscard]] RequestPlan BuildRequestPlan(
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
        case ExpectedResponse::Kind::kBlockValuesOrNull: {
            const std::string header = TrimCrLf(client.ReadReplyLine());
            if (header == "_") {
                return;
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
            return;
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
            return;
        }
        case ExpectedResponse::Kind::kInteger: {
            const std::string line = TrimCrLf(client.ReadReplyLine());
            if (line.size() < 2 || line[0] != ':') {
                throw std::runtime_error(
                    "scenario=" + std::string(scenario_name) + " validation failed: expected :<version>, got '" +
                    line + "'");
            }
            return;
        }
        case ExpectedResponse::Kind::kChunkFormOrNull:
        case ExpectedResponse::Kind::kBulkBytesLength: {
            if (expected.kind == ExpectedResponse::Kind::kChunkFormOrNull) {
                const auto header = client.ReadReplyLine();
                if (header == "_\r\n") {
                    return;
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
            return;
        }
    }
}

// Writes every chunk of a grid scenario's region whole, so area reads and
// block reads find data; not timed.
void FillRegion(const Args& args, const GeometryInfo& geometry) {
    const ScenarioRegion region = RegionFor(args, geometry);
    Client client(args.host, args.port);
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
                    const RequestPlan plan = BuildRequestPlan(scenario, i, work.client_id, rng, state, args, geometry);
                    if (plan.payload.empty()) {
                        client.SendLine(plan.command);
                    } else {
                        client.SendLineWithPayload(plan.command, plan.payload);
                    }
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
        << "  --requests <N>                   default: 5000\n"
        << "  --ops <N>                        alias for --requests\n"
        << "  --tests <list>                   comma list: ping,set,get,chunkgetstate,mixed,\n"
        << "                                   world,canvas,simulation (grid workloads over keyspace x\n"
        << "                                   keyspace blocks, filled before they are timed)\n"
        << "  --keyspace <N>                   default: 512\n"
        << "  --seed <N>                       default: 1337\n"
        << "  --durability-mode <mode>         spawn mode: relaxed (default), fsync-wal, fsync-checkpoint\n"
        << "  --watch <table>                  read every push throughout the run\n"
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
        std::unique_ptr<WatchingClient> watch;
        if (!args.watch_table.empty()) watch = std::make_unique<WatchingClient>(args);
        const GeometryInfo geometry = LoadGeometryInfo(args);
        const auto split = BuildWorkSplit(args.requests, args.clients);
        report.active_clients = CountActiveWorkers(split);
        report.results.reserve(args.tests.size());
        for (const Scenario scenario : args.tests) {
            report.results.push_back(RunScenario(scenario, args, geometry));
        }
        if (watch) watch->Finish();
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
    if (!report.durability_mode.empty()) {
        out << "durability_mode=" << report.durability_mode << "\n";
    }
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
    out << "\"durability_mode\":\"" << JsonEscape(report.durability_mode) << "\",";
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
