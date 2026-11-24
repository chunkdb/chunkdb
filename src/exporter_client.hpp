#pragma once

#include <chrono>
#include <memory>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>

#include "chunkdb/uri.hpp"
#include "metrics_http.hpp"

// The chunkdb side of chunkdb_exporter (docs/EXPORTER.md): one connection to
// a server, logged in with SCRAM-SHA-256 when the URI names a user, over
// which it reads SHOW METRICS.
namespace chunkdb::exporter {

struct ExporterOptions {
    ConnectionUri uri;
    // The file whose first line is the password of uri.user.
    std::string password_file;
    // Certificates that verify a chunks:// server; the system's when empty.
    std::string tls_ca_file;
    // Bind the login to its TLS connection, as the chunkdb clients do.
    bool channel_binding = true;
    // For one scrape: connecting, logging in and reading the reply.
    std::chrono::milliseconds timeout{5000};
};

// An error reply: the statement was refused, the connection stays usable.
class ServerReplyError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// The first line of `path`, without its line ending; throws
// std::runtime_error when the file cannot be read or that line is empty.
[[nodiscard]] std::string ReadPasswordFile(const std::string& path);

class ChunkConnection {
  public:
    // Connects, completes TLS for chunks:// and logs in, by `deadline`;
    // throws std::runtime_error otherwise, ServerReplyError for a refused
    // login.
    ChunkConnection(const ExporterOptions& options, std::chrono::steady_clock::time_point deadline,
                    std::stop_token stop);
    ~ChunkConnection();
    ChunkConnection(const ChunkConnection&) = delete;
    ChunkConnection& operator=(const ChunkConnection&) = delete;

    // Sends `statement` and returns the text of its bulk or simple reply;
    // ServerReplyError for an error reply, std::runtime_error when the
    // connection fails.
    [[nodiscard]] std::string Query(std::string_view statement, std::chrono::steady_clock::time_point deadline,
                                    std::stop_token stop);

  private:
    struct State;
    std::unique_ptr<State> state_;
};

// The body of a scrape that could not read the server: chunkdb_up 0.
[[nodiscard]] std::string DownBody();

// SHOW METRICS with chunkdb_up appended: 1 and the server's samples when the
// scrape worked, DownBody (served with 503) when it did not. The connection
// is kept between scrapes; when a kept one fails, a new one is opened once
// in the same scrape. After a failed connect or login no new one is tried
// for 1 second, doubled after each failure up to 30 seconds.
class Exporter {
  public:
    explicit Exporter(ExporterOptions options);
    ~Exporter();
    [[nodiscard]] server_detail::MetricsScrape Scrape(std::stop_token stop);

  private:
    ExporterOptions options_;
    std::unique_ptr<ChunkConnection> connection_;
    // Whether the last scrape worked, so that a change is logged once.
    bool up_ = true;
    std::chrono::milliseconds backoff_{0};
    std::chrono::steady_clock::time_point retry_at_{};
};

}  // namespace chunkdb::exporter
