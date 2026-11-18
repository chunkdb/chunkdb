#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>

#include "chunkdb/wal_durability_pin.hpp"

namespace chunkdb {

// Append-only owner-private file. Windows handles have delete sharing so a
// staged hard link can be removed while the live WAL remains open. POSIX
// writes are buffered until flush; durable sync stays in the existing paths.
class WalAppendStream {
  public:
    WalAppendStream() = default;
    WalAppendStream(const std::filesystem::path& path, std::ios_base::openmode mode);
    ~WalAppendStream();
    WalAppendStream(const WalAppendStream&) = delete;
    WalAppendStream& operator=(const WalAppendStream&) = delete;

    // `existing`: the file is expected to exist, so it is opened as it is
    // and created only when missing.
    void open(const std::filesystem::path& path, std::ios_base::openmode mode, bool existing = false);
    void close() noexcept;
    void clear() noexcept { error_ = 0; }
    [[nodiscard]] bool is_open() const noexcept { return handle_ != nullptr; }
    [[nodiscard]] bool good() const noexcept { return error_ == 0; }
    [[nodiscard]] bool fail() const noexcept { return error_ != 0; }
    [[nodiscard]] int error() const noexcept { return error_; }
    WalAppendStream& write(const char* bytes, std::streamsize size);
    WalAppendStream& flush() noexcept;
    // Flush this stream's own buffer and retain its native writable generation.
    [[nodiscard]] WalDurabilityPin FlushAndPin();

    // Size of the open file with every write flushed: read from the file
    // once, then kept up by this stream's own writes, so asking costs no
    // system call. Empty while closed, with unflushed bytes, or after a
    // failure. Whoever changes the file another way closes the stream first.
    [[nodiscard]] std::optional<std::uint64_t> size() noexcept;

    // Streams of this process with an open file, and the most there were at
    // once since the last reset.
    [[nodiscard]] static std::size_t OpenCountForTests() noexcept;
    [[nodiscard]] static std::size_t PeakOpenCountForTests() noexcept;
    static void ResetPeakOpenCountForTests() noexcept;

  private:
    void NoteOpened() noexcept;
    void NoteClosed() noexcept;

    void* handle_ = nullptr;
    // The first I/O cause survives cleanup; callers capture it before reset.
    int error_ = 0;
    std::optional<std::uint64_t> size_;
    std::uint64_t unflushed_ = 0;
};

}  // namespace chunkdb
