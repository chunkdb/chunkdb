#pragma once

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

    void open(const std::filesystem::path& path, std::ios_base::openmode mode);
    void close() noexcept;
    void clear() noexcept { error_ = 0; }
    [[nodiscard]] bool is_open() const noexcept { return handle_ != nullptr; }
    [[nodiscard]] bool good() const noexcept { return error_ == 0; }
    [[nodiscard]] bool fail() const noexcept { return error_ != 0; }
    [[nodiscard]] int error() const noexcept { return error_; }
    WalAppendStream& write(const char* bytes, std::streamsize size);
    WalAppendStream& flush() noexcept;

  private:
    void* handle_ = nullptr;
    // The first I/O cause survives cleanup; callers capture it before reset.
    int error_ = 0;
};

}  // namespace chunkdb
