#pragma once

#include <filesystem>
#include <fstream>

namespace chunkdb {

#ifdef _WIN32
// Append-only Windows handle with delete sharing, so a staged hard link can
// be removed while the live WAL remains open. Writes reach the OS cache as
// ofstream::flush does; durable flushing stays in the existing sync paths.
class WalAppendStream {
  public:
    WalAppendStream() = default;
    WalAppendStream(const std::filesystem::path& path, std::ios_base::openmode mode);
    ~WalAppendStream();
    WalAppendStream(const WalAppendStream&) = delete;
    WalAppendStream& operator=(const WalAppendStream&) = delete;

    void open(const std::filesystem::path& path, std::ios_base::openmode mode);
    void close() noexcept;
    void clear() noexcept { good_ = true; }
    [[nodiscard]] bool is_open() const noexcept { return handle_ != nullptr; }
    [[nodiscard]] bool good() const noexcept { return good_; }
    WalAppendStream& write(const char* bytes, std::streamsize size);
    WalAppendStream& flush() noexcept { return *this; }

  private:
    void* handle_ = nullptr;
    bool good_ = true;
};
#else
using WalAppendStream = std::ofstream;
#endif

}  // namespace chunkdb
