#include "chunkdb/logging.hpp"

#include <array>
#include <chrono>
#include <ctime>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace chunkdb {

namespace {

std::atomic<LogLevel> g_log_level{LogLevel::kInfo};
std::mutex g_sink_mutex;
LogSink g_sink;

std::tm LocalTime(std::time_t now) {
    std::tm tm_local{};
#ifdef _WIN32
    localtime_s(&tm_local, &now);
#else
    localtime_r(&now, &tm_local);
#endif
    return tm_local;
}

bool NeedsQuotes(std::string_view value) {
    for (const char c : value) {
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '"' || c == '\\') {
            return true;
        }
    }
    return false;
}

std::string FormatFieldValue(std::string_view value) {
    if (!NeedsQuotes(value)) {
        return std::string(value);
    }

    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('"');
    for (const char c : value) {
        if (c == '"' || c == '\\') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

std::string FormatTimestampLocalMs() {
    const auto now = std::chrono::system_clock::now();
    const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    const std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    const std::tm local_tm = LocalTime(now_time);

    std::ostringstream out;
    out << (local_tm.tm_year + 1900) << "-";
    if (local_tm.tm_mon + 1 < 10) {
        out << "0";
    }
    out << (local_tm.tm_mon + 1) << "-";
    if (local_tm.tm_mday < 10) {
        out << "0";
    }
    out << local_tm.tm_mday << " ";
    if (local_tm.tm_hour < 10) {
        out << "0";
    }
    out << local_tm.tm_hour << ":";
    if (local_tm.tm_min < 10) {
        out << "0";
    }
    out << local_tm.tm_min << ":";
    if (local_tm.tm_sec < 10) {
        out << "0";
    }
    out << local_tm.tm_sec << ".";
    if (millis.count() < 100) {
        out << "0";
    }
    if (millis.count() < 10) {
        out << "0";
    }
    out << millis.count();
    return out.str();
}

}  // namespace

LogLevel ParseLogLevel(std::string_view text) {
    if (text == "info") {
        return LogLevel::kInfo;
    }
    if (text == "warn") {
        return LogLevel::kWarn;
    }
    if (text == "error") {
        return LogLevel::kError;
    }
    throw std::invalid_argument("invalid log level: " + std::string(text) + " (expected info|warn|error)");
}

const char* LogLevelName(LogLevel level) noexcept {
    switch (level) {
        case LogLevel::kInfo:
            return "INFO";
        case LogLevel::kWarn:
            return "WARN";
        case LogLevel::kError:
            return "ERROR";
    }
    return "UNKNOWN";
}

const char* LogComponentName(LogComponent component) noexcept {
    switch (component) {
        case LogComponent::kServer:
            return "server";
        case LogComponent::kStore:
            return "store";
        case LogComponent::kLock:
            return "lock";
        case LogComponent::kRecovery:
            return "recovery";
    }
    return "unknown";
}

std::uint64_t CurrentProcessId() noexcept {
#ifdef _WIN32
    return static_cast<std::uint64_t>(GetCurrentProcessId());
#else
    return static_cast<std::uint64_t>(::getpid());
#endif
}

void SetLogLevel(LogLevel level) noexcept {
    g_log_level.store(level, std::memory_order_relaxed);
}

LogLevel GetLogLevel() noexcept {
    return g_log_level.load(std::memory_order_relaxed);
}

void SetLogSinkForTests(LogSink sink) {
    std::lock_guard lock(g_sink_mutex);
    g_sink = std::move(sink);
}

void ResetLogSinkForTests() {
    std::lock_guard lock(g_sink_mutex);
    g_sink = nullptr;
}

void LogMessage(
    LogLevel level,
    LogComponent component,
    std::string_view message,
    std::initializer_list<LogField> fields) {
    if (static_cast<int>(level) < static_cast<int>(GetLogLevel())) {
        return;
    }

    std::ostringstream out;
    out << FormatTimestampLocalMs()
        << " " << LogLevelName(level)
        << " " << LogComponentName(component)
        << " pid=" << CurrentProcessId()
        << " " << message;

    for (const auto& field : fields) {
        if (field.key.empty()) {
            continue;
        }
        out << " " << field.key << "=" << FormatFieldValue(field.value);
    }

    const std::string line = out.str();

    std::lock_guard lock(g_sink_mutex);
    if (g_sink) {
        g_sink(line);
        return;
    }

    std::cout << line << "\n";
    std::cout.flush();
}

}  // namespace chunkdb
