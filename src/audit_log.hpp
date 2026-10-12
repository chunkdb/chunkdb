#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "chunkdb/wal_append_stream.hpp"

namespace chunkdb {

// The audit log (--audit-log): JSON Lines, one object per event, appended to
// an owner-only file and flushed after each line without fsync. Only names,
// addresses and outcomes are written: never passwords, verifiers, parameter
// values or block data.
class AuditLog {
  public:
    // Opens `path` for appending, creating it owner-only; throws when it
    // cannot.
    explicit AuditLog(std::filesystem::path path);
    AuditLog(const AuditLog&) = delete;
    AuditLog& operator=(const AuditLog&) = delete;

    struct Event {
        std::string_view event;
        std::uint64_t connection = 0;
        std::string_view user;
        std::string_view peer;
        bool ok = true;
        // Empty fields are left out.
        std::string_view error_code{};
        std::string_view reason{};
        std::string_view table{};
        std::string_view target_user{};
        std::string_view slot{};
        std::string_view migration{};
    };
    // Appends one line; a failure is logged, never thrown.
    void Write(const Event& event) noexcept;

    // Closes the file and opens `path` again, so a renamed file stops
    // growing (log rotation). Throws when the file cannot be opened; the
    // log then keeps writing to the file it had.
    void Reopen();

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

  private:
    const std::filesystem::path path_;
    std::mutex mutex_;
    std::unique_ptr<WalAppendStream> stream_;
    std::string line_;
    bool write_failed_ = false;
};

// Appends `text` to `out` as a JSON string, quotes included.
void AppendJsonString(std::string& out, std::string_view text);

}  // namespace chunkdb
