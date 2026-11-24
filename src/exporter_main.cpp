// chunkdb_exporter: serves the metrics of a chunkdb server to Prometheus
// (docs/EXPORTER.md).

#include <charconv>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

#ifndef _WIN32
#include <pthread.h>
#include <signal.h>
#endif

#include "chunkdb/logging.hpp"
#include "chunkdb/uri.hpp"
#include "exporter_client.hpp"
#include "metrics_http.hpp"

namespace {

constexpr const char* kDefaultListen = "127.0.0.1:14242";

// `value` as host:port, with an IPv6 host in brackets ([::1]:14242).
std::pair<std::string, std::uint16_t> ParseHostPort(const std::string& value, const char* flag) {
    const auto refuse = [&]() {
        return std::invalid_argument(std::string("invalid ") + flag + ": " + value +
                                     " (expected host:port, such as " + kDefaultListen + ")");
    };
    std::string host;
    std::string port;
    if (!value.empty() && value.front() == '[') {
        const auto close = value.find(']');
        if (close == std::string::npos || close + 1U >= value.size() || value[close + 1U] != ':') throw refuse();
        host = value.substr(1U, close - 1U);
        port = value.substr(close + 2U);
    } else {
        const auto colon = value.find(':');
        if (colon == std::string::npos || value.find(':', colon + 1U) != std::string::npos) throw refuse();
        host = value.substr(0U, colon);
        port = value.substr(colon + 1U);
    }
    std::uint16_t parsed = 0U;
    const auto result = std::from_chars(port.data(), port.data() + port.size(), parsed);
    if (host.empty() || result.ec != std::errc() || result.ptr != port.data() + port.size() || parsed == 0U) {
        throw refuse();
    }
    return {host, parsed};
}

void PrintUsage() {
    std::cout
        << "Usage: chunkdb_exporter --uri <chunk[s]://user@host:port/> --password-file <path> [options]\n"
        << "  --uri <uri>                 The server; chunks:// for TLS. A user without a password.\n"
        << "  --password-file <path>      First line: the password of the URI's user (MONITOR right).\n"
        << "  --listen <host:port>        Where GET /metrics is served (" << kDefaultListen << ").\n"
        << "  --allow-remote              Allow --listen on an address that is not loopback.\n"
        << "  --tls-ca-file <path>        Certificates that verify the server; the system's otherwise.\n"
        << "  --no-channel-binding        Do not bind the login to its TLS connection.\n"
        << "  --timeout-ms <ms>           Limit of one scrape of the server (5000).\n"
        << "  --log-level <level>         debug, info, warn or error (info).\n";
}

#ifdef _WIN32
std::mutex g_stop_mutex;
std::condition_variable g_stop_cv;
bool g_stop = false;

BOOL WINAPI OnConsoleEvent(DWORD) {
    {
        std::lock_guard lock(g_stop_mutex);
        g_stop = true;
    }
    g_stop_cv.notify_all();
    return TRUE;
}
#endif

}  // namespace

int main(int argc, char** argv) {
    chunkdb::exporter::ExporterOptions options;
    std::string listen = kDefaultListen;
    std::pair<std::string, std::uint16_t> listen_address;
    bool allow_remote = false;
    bool uri_given = false;
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            const auto value = [&](const char* flag) -> std::string {
                if (i + 1 >= argc) throw std::invalid_argument(std::string(flag) + " needs a value");
                return argv[++i];
            };
            if (arg == "--uri") {
                options.uri = chunkdb::ParseConnectionUri(value("--uri"));
                uri_given = true;
            } else if (arg == "--password-file") {
                options.password_file = value("--password-file");
            } else if (arg == "--listen") {
                listen = value("--listen");
            } else if (arg == "--allow-remote") {
                allow_remote = true;
            } else if (arg == "--tls-ca-file") {
                options.tls_ca_file = value("--tls-ca-file");
            } else if (arg == "--no-channel-binding") {
                options.channel_binding = false;
            } else if (arg == "--timeout-ms") {
                const auto text = value("--timeout-ms");
                std::uint32_t ms = 0;
                const auto parsed = std::from_chars(text.data(), text.data() + text.size(), ms);
                if (parsed.ec != std::errc() || parsed.ptr != text.data() + text.size() || ms == 0U || ms > 600000U) {
                    throw std::invalid_argument("--timeout-ms is 1 to 600000, got " + text);
                }
                options.timeout = std::chrono::milliseconds(ms);
            } else if (arg == "--log-level") {
                chunkdb::SetLogLevel(chunkdb::ParseLogLevel(value("--log-level")));
            } else if (arg == "--help" || arg == "-h") {
                PrintUsage();
                return 0;
            } else {
                throw std::invalid_argument("unknown option " + arg);
            }
        }
        if (!uri_given) throw std::invalid_argument("--uri is required");
        if (!options.uri.password.empty()) {
            throw std::invalid_argument("the URI must not carry a password; put it in --password-file");
        }
        if (!options.uri.user.empty() && options.password_file.empty()) {
            throw std::invalid_argument("the URI names a user: --password-file is required");
        }
        if (options.uri.user.empty() && !options.password_file.empty()) {
            throw std::invalid_argument("--password-file needs a user in the URI, such as chunk://monitor@host:4242/");
        }
        if (!options.password_file.empty()) (void)chunkdb::exporter::ReadPasswordFile(options.password_file);
        listen_address = ParseHostPort(listen, "--listen");
    } catch (const std::exception& error) {
        std::cerr << "chunkdb_exporter: " << error.what() << "\n";
        return 2;
    }

#ifdef _WIN32
    WSADATA wsa_data;
    if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
        std::cerr << "chunkdb_exporter: WSAStartup failed\n";
        return 1;
    }
    SetConsoleCtrlHandler(OnConsoleEvent, TRUE);
#else
    // Writes to a server that reset the connection must not end the process,
    // and only this thread takes the stop signals.
    std::signal(SIGPIPE, SIG_IGN);
    sigset_t stop_signals;
    sigemptyset(&stop_signals);
    sigaddset(&stop_signals, SIGINT);
    sigaddset(&stop_signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &stop_signals, nullptr);
#endif

    try {
        const auto& [host, port] = listen_address;
        const auto timeout = options.timeout;
        chunkdb::exporter::Exporter exporter(std::move(options));
        // The listener's limit for a scrape outlasts the exporter's own, so
        // a server that does not answer gives chunkdb_up 0 rather than a
        // bare timeout.
        chunkdb::server_detail::MetricsHttpListener listener(
            host, port, allow_remote, static_cast<std::size_t>(timeout.count()) + 1000U,
            [&exporter](std::stop_token stop) { return exporter.Scrape(stop); });
        chunkdb::LogMessage(chunkdb::LogLevel::kInfo, chunkdb::LogComponent::kServer, "serving metrics",
                            {{"listen", listen}});
#ifdef _WIN32
        std::unique_lock lock(g_stop_mutex);
        g_stop_cv.wait(lock, [] { return g_stop; });
#else
        int signal_number = 0;
        (void)sigwait(&stop_signals, &signal_number);
#endif
        chunkdb::LogMessage(chunkdb::LogLevel::kInfo, chunkdb::LogComponent::kServer, "stopping");
    } catch (const std::exception& error) {
        chunkdb::LogMessage(chunkdb::LogLevel::kError, chunkdb::LogComponent::kServer, "chunkdb_exporter failed",
                            {{"error", error.what()}});
#ifdef _WIN32
        WSACleanup();
#endif
        return 1;
    }
#ifdef _WIN32
    WSACleanup();
#endif
    return 0;
}
