#include "audit_log.hpp"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <system_error>

#include "chunkdb/logging.hpp"

namespace chunkdb {

namespace {

std::unique_ptr<WalAppendStream> OpenAppend(const std::filesystem::path& path) {
    auto stream = std::make_unique<WalAppendStream>();
    stream->open(path, std::ios::binary | std::ios::app);
    if (!stream->is_open()) {
        const int error = errno;
        throw std::system_error(error, std::generic_category(), "failed to open the audit log " + path.string());
    }
    return stream;
}

void AppendField(std::string& out, std::string_view key, std::string_view value) {
    out += ",\"";
    out += key;
    out += "\":";
    AppendJsonString(out, value);
}

}  // namespace

void AppendJsonString(std::string& out, std::string_view text) {
    static constexpr char kHex[] = "0123456789abcdef";
    out += '"';
    for (const char c : text) {
        const auto byte = static_cast<unsigned char>(c);
        if (c == '"' || c == '\\') {
            out += '\\';
            out += c;
        } else if (byte < 0x20U || byte == 0x7FU) {
            out += "\\u00";
            out += kHex[byte >> 4U];
            out += kHex[byte & 0x0FU];
        } else {
            out += c;
        }
    }
    out += '"';
}

AuditLog::AuditLog(std::filesystem::path path) : path_(std::move(path)), stream_(OpenAppend(path_)) {}

void AuditLog::Reopen() {
    auto next = OpenAppend(path_);
    std::lock_guard lock(mutex_);
    stream_.swap(next);
    write_failed_ = false;
    // `next` now holds the old stream; it closes when `next` goes away.
}

void AuditLog::Write(const Event& event) noexcept {
    try {
        const auto time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
        std::lock_guard lock(mutex_);
        line_.clear();
        line_ += "{\"time_ms\":";
        line_ += std::to_string(time_ms);
        line_ += ",\"connection\":";
        line_ += std::to_string(event.connection);
        AppendField(line_, "event", event.event);
        AppendField(line_, "user", event.user);
        AppendField(line_, "peer", event.peer);
        line_ += event.ok ? ",\"ok\":true" : ",\"ok\":false";
        if (!event.error_code.empty()) AppendField(line_, "error_code", event.error_code);
        if (!event.reason.empty()) AppendField(line_, "reason", event.reason);
        if (!event.table.empty()) AppendField(line_, "table", event.table);
        if (!event.target_user.empty()) AppendField(line_, "target_user", event.target_user);
        if (!event.slot.empty()) AppendField(line_, "slot", event.slot);
        if (!event.migration.empty()) AppendField(line_, "migration", event.migration);
        line_ += "}\n";
        stream_->write(line_.data(), static_cast<std::streamsize>(line_.size()));
        stream_->flush();
        if (!stream_->good()) {
            const int error = errno;
            stream_->clear();
            if (!write_failed_) {
                write_failed_ = true;
                LogMessage(LogLevel::kError, LogComponent::kServer, "failed to write the audit log",
                           {{"path", path_.string()}, {"error", std::strerror(error)}});
            }
        } else {
            write_failed_ = false;
        }
    } catch (...) {
        // Building the line ran out of memory; the statement still answers.
    }
}

}  // namespace chunkdb
