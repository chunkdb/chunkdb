#pragma once

#include <cstddef>
#include <filesystem>
#include <fstream>

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
    void clear() noexcept { good_ = true; }
    [[nodiscard]] bool is_open() const noexcept { return handle_ != nullptr; }
    [[nodiscard]] bool good() const noexcept { return good_; }
    [[nodiscard]] bool fail() const noexcept { return !good_; }
    WalAppendStream& write(const char* bytes, std::streamsize size);
    WalAppendStream& flush() noexcept;

    // Streams of this process with an open file, and the most there were at
    // once since the last reset.
    [[nodiscard]] static std::size_t OpenCountForTests() noexcept;
    [[nodiscard]] static std::size_t PeakOpenCountForTests() noexcept;
    static void ResetPeakOpenCountForTests() noexcept;

  private:
    void NoteOpened() noexcept;
    void NoteClosed() noexcept;

    void* handle_ = nullptr;
    bool good_ = true;
};

}  // namespace chunkdb
