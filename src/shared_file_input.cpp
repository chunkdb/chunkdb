#include "shared_file_input.hpp"

#include <array>
#include <cerrno>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <streambuf>
#include <system_error>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace chunkdb {

class SharedFileInput::Buffer final : public std::streambuf {
  public:
    explicit Buffer(const std::filesystem::path& path) : path_(path) {
#ifdef _WIN32
        handle_ = CreateFileW(path.c_str(), GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
        if (handle_ == INVALID_HANDLE_VALUE) Error(static_cast<int>(GetLastError()), "open");
        BY_HANDLE_FILE_INFORMATION information{};
        if (!GetFileInformationByHandle(handle_, &information)) {
            const auto error = static_cast<int>(GetLastError());
            Close();
            Error(error, "inspect");
        }
        const bool regular = GetFileType(handle_) == FILE_TYPE_DISK &&
            (information.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0U;
#else
        // Nonblocking admission also rejects FIFOs without waiting for a writer.
        handle_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
        if (handle_ < 0) Error(errno, "open");
        struct stat information {};
        if (fstat(handle_, &information) != 0) {
            const auto error = errno;
            Close();
            Error(error, "inspect");
        }
        const bool regular = S_ISREG(information.st_mode);
#endif
        if (!regular) {
            Close();
            throw std::runtime_error("shared file input is not a plain regular file: " + path_.string());
        }
        Empty();
    }
    ~Buffer() override { Close(); }
    [[nodiscard]] bool IsOpen() const noexcept {
#ifdef _WIN32
        return handle_ != INVALID_HANDLE_VALUE;
#else
        return handle_ >= 0;
#endif
    }
    void Close() noexcept {
        if (IsOpen()) {
#ifdef _WIN32
            CloseHandle(handle_);
            handle_ = INVALID_HANDLE_VALUE;
#else
            ::close(handle_);
            handle_ = -1;
#endif
        }
        Empty();
    }

  private:
    [[noreturn]] void Error(int error, const char* operation) const {
#ifdef _WIN32
        const auto& category = std::system_category();
#else
        const auto& category = std::generic_category();
#endif
        throw std::system_error(error, category,
            std::string("shared file input ") + operation + ": " + path_.string());
    }
    void Empty() noexcept { setg(bytes_.data(), bytes_.data(), bytes_.data()); }
    int_type underflow() override {
        if (gptr() != egptr()) return traits_type::to_int_type(*gptr());
        if (!IsOpen()) return traits_type::eof();
#ifdef _WIN32
        DWORD count = 0U;
        if (!ReadFile(handle_, bytes_.data(), static_cast<DWORD>(bytes_.size()), &count, nullptr))
            Error(static_cast<int>(GetLastError()), "read");
        const auto read = static_cast<std::size_t>(count);
#else
        ssize_t count;
        do { count = ::read(handle_, bytes_.data(), bytes_.size()); } while (count < 0 && errno == EINTR);
        if (count < 0) Error(errno, "read");
        const auto read = static_cast<std::size_t>(count);
#endif
        if (read == 0U) return traits_type::eof();
        if (read > static_cast<std::uint64_t>(std::numeric_limits<off_type>::max()) - native_position_)
            throw std::overflow_error("shared file input position exceeds the stream range");
        native_position_ += read;
        setg(bytes_.data(), bytes_.data(), bytes_.data() + read);
        return traits_type::to_int_type(*gptr());
    }
    std::uint64_t Size() const {
#ifdef _WIN32
        LARGE_INTEGER size{};
        if (!GetFileSizeEx(handle_, &size)) Error(static_cast<int>(GetLastError()), "size");
        if (size.QuadPart < 0) throw std::runtime_error("shared file input has a negative size");
        return static_cast<std::uint64_t>(size.QuadPart);
#else
        struct stat information {};
        if (fstat(handle_, &information) != 0) Error(errno, "size");
        if (information.st_size < 0) throw std::runtime_error("shared file input has a negative size");
        return static_cast<std::uint64_t>(information.st_size);
#endif
    }
    pos_type seekoff(off_type offset, std::ios_base::seekdir direction, std::ios_base::openmode mode) override {
        static_assert(std::numeric_limits<off_type>::is_signed && std::numeric_limits<off_type>::digits <= 63);
        if (!IsOpen() || mode != std::ios_base::in) return pos_type(off_type(-1));
        std::uint64_t base;
        if (direction == std::ios_base::beg) base = 0U;
        else if (direction == std::ios_base::cur)
            base = native_position_ - static_cast<std::uint64_t>(egptr() - gptr());
        else if (direction == std::ios_base::end) base = Size();
        else return pos_type(off_type(-1));
        std::uint64_t target;
        const auto maximum = static_cast<std::uint64_t>(std::numeric_limits<off_type>::max());
        if (offset < 0) {
            const auto magnitude = static_cast<std::uint64_t>(-(offset + 1)) + 1U;
            if (magnitude > base) return pos_type(off_type(-1));
            target = base - magnitude;
        } else {
            const auto magnitude = static_cast<std::uint64_t>(offset);
            if (base > maximum || magnitude > maximum - base) return pos_type(off_type(-1));
            target = base + magnitude;
        }
        if (target > maximum) return pos_type(off_type(-1));
#ifdef _WIN32
        LARGE_INTEGER position{};
        position.QuadPart = static_cast<LONGLONG>(target);
        if (!SetFilePointerEx(handle_, position, nullptr, FILE_BEGIN)) return pos_type(off_type(-1));
#else
        if (target > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()) ||
            lseek(handle_, static_cast<off_t>(target), SEEK_SET) < 0) return pos_type(off_type(-1));
#endif
        native_position_ = target;
        Empty();
        return pos_type(static_cast<off_type>(target));
    }
    pos_type seekpos(pos_type position, std::ios_base::openmode mode) override {
        return seekoff(static_cast<off_type>(position), std::ios_base::beg, mode);
    }
    const std::filesystem::path path_;
#ifdef _WIN32
    HANDLE handle_ = INVALID_HANDLE_VALUE;
#else
    int handle_ = -1;
#endif
    std::array<char, 8192> bytes_{};
    std::uint64_t native_position_ = 0U;
};

SharedFileInput::SharedFileInput(const std::filesystem::path& path)
    : std::istream(nullptr), buffer_(std::make_unique<Buffer>(path)) { rdbuf(buffer_.get()); }
SharedFileInput::~SharedFileInput() = default;
bool SharedFileInput::is_open() const noexcept { return buffer_->IsOpen(); }
void SharedFileInput::close() noexcept { buffer_->Close(); }

}  // namespace chunkdb
