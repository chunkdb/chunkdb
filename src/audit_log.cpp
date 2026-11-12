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
    // Only a regular file is opened: a FIFO or device put at the path would
    // block the open on the thread that also handles SIGTERM.
    std::error_code status_error;
    const auto status = std::filesystem::symlink_status(path, status_error);
    if (!status_error && status.type() != std::filesystem::file_type::regular) {
        throw std::runtime_error("the audit log path is not a regular file: " + path.string());
    }
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

namespace {

// The length of the well-formed UTF-8 sequence starting at `text[at]` (a
// byte of 0x80 or more), or 0 when it is not one (RFC 3629: no overlong
// forms, no surrogates, nothing above U+10FFFF).
std::size_t Utf8SequenceLength(std::string_view text, std::size_t at) noexcept {
    const auto byte = [&](std::size_t i) { return static_cast<unsigned char>(text[i]); };
    const unsigned char lead = byte(at);
    std::size_t length = 0;
    unsigned char low = 0x80U;
    unsigned char high = 0xBFU;
    if (lead >= 0xC2U && lead <= 0xDFU) {
        length = 2;
    } else if (lead >= 0xE0U && lead <= 0xEFU) {
        length = 3;
        if (lead == 0xE0U) low = 0xA0U;
        if (lead == 0xEDU) high = 0x9FU;
    } else if (lead >= 0xF0U && lead <= 0xF4U) {
        length = 4;
        if (lead == 0xF0U) low = 0x90U;
        if (lead == 0xF4U) high = 0x8FU;
    } else {
        return 0;
    }
    if (text.size() - at < length) return 0;
    if (byte(at + 1) < low || byte(at + 1) > high) return 0;
    for (std::size_t i = 2; i < length; ++i) {
        if (byte(at + i) < 0x80U || byte(at + i) > 0xBFU) return 0;
    }
    return length;
}

}  // namespace

void AppendJsonString(std::string& out, std::string_view text) {
    static constexpr char kHex[] = "0123456789abcdef";
    out += '"';
    for (std::size_t i = 0; i < text.size();) {
        const char c = text[i];
        const auto byte = static_cast<unsigned char>(c);
        if (c == '"' || c == '\\') {
            out += '\\';
            out += c;
        } else if (byte < 0x20U || byte == 0x7FU) {
            out += "\\u00";
            out += kHex[byte >> 4U];
            out += kHex[byte & 0x0FU];
        } else if (byte >= 0x80U) {
            // JSON text is UTF-8: a byte that does not start a well-formed
            // sequence becomes U+FFFD, so every line stays valid JSON.
            const std::size_t length = Utf8SequenceLength(text, i);
            if (length == 0) {
                out += "\\ufffd";
                ++i;
            } else {
                out.append(text.substr(i, length));
                i += length;
            }
            continue;
        } else {
            out += c;
        }
        ++i;
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
        if (event.target_connection) {
            line_ += ",\"target_connection\":";
            line_ += std::to_string(*event.target_connection);
        }
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
    } catch (const std::exception& error) {
        // The line could not be built (out of memory, or the lock failed):
        // the event is dropped and the statement still answers. The first
        // drop is logged; logging itself may fail the same way.
        if (!drop_logged_.exchange(true, std::memory_order_relaxed)) {
            try {
                LogMessage(LogLevel::kError, LogComponent::kServer, "dropped an audit log event",
                           {{"path", path_.string()}, {"error", error.what()}});
            } catch (const std::exception&) {
            }
        }
    }
}

}  // namespace chunkdb
