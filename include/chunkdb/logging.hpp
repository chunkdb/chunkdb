#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <string>
#include <string_view>

namespace chunkdb {

enum class LogLevel {
    kInfo = 0,
    kWarn = 1,
    kError = 2,
};

enum class LogComponent {
    kServer,
    kStore,
    kLock,
    kRecovery,
};

struct LogField {
    std::string_view key;
    std::string_view value;
};

using LogSink = std::function<void(const std::string&)>;

[[nodiscard]] LogLevel ParseLogLevel(std::string_view text);
[[nodiscard]] const char* LogLevelName(LogLevel level) noexcept;
[[nodiscard]] const char* LogComponentName(LogComponent component) noexcept;
[[nodiscard]] std::uint64_t CurrentProcessId() noexcept;

void SetLogLevel(LogLevel level) noexcept;
[[nodiscard]] LogLevel GetLogLevel() noexcept;

void SetLogSinkForTests(LogSink sink);
void ResetLogSinkForTests();

void LogMessage(
    LogLevel level,
    LogComponent component,
    std::string_view message,
    std::initializer_list<LogField> fields = {});

}  // namespace chunkdb
