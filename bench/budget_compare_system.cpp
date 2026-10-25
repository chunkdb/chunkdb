#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "budget_compare.hpp"

#ifdef CHUNKDB_WITH_OPENSSL
#include <openssl/evp.h>
#else
#include "crypto.hpp"
#endif

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <shellapi.h>
#include <fcntl.h>
#include <io.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __APPLE__
#include <sys/sysctl.h>
#endif
#endif

namespace chunkdb::budget_compare {

namespace {

[[noreturn]] void Fail(ErrorKind kind, const std::string& message) { throw Error(kind, message); }

std::string OsErrorMessage(int error, const std::string& path) {
    return "[Errno " + std::to_string(error) + "] " + std::strerror(error) + ": " + PyRepr(Value::Str(path));
}

#ifdef _WIN32

// WTF-8 <-> UTF-16, keeping lone surrogates.
std::wstring Widen(const std::string& text) {
    std::wstring out;
    for (std::size_t i = 0; i < text.size();) {
        const auto lead = static_cast<unsigned char>(text[i]);
        std::uint32_t code = lead;
        std::size_t length = 1;
        if (lead >= 0xF0) {
            length = 4;
            code = lead & 0x07U;
        } else if (lead >= 0xE0) {
            length = 3;
            code = lead & 0x0FU;
        } else if (lead >= 0xC0) {
            length = 2;
            code = lead & 0x1FU;
        }
        for (std::size_t k = 1; k < length && i + k < text.size(); ++k) {
            code = (code << 6) | (static_cast<unsigned char>(text[i + k]) & 0x3FU);
        }
        i += length;
        if (code >= 0x10000) {
            code -= 0x10000;
            out += static_cast<wchar_t>(0xD800 + (code >> 10));
            out += static_cast<wchar_t>(0xDC00 + (code & 0x3FF));
        } else {
            out += static_cast<wchar_t>(code);
        }
    }
    return out;
}

void AppendWtf8(std::string& out, std::uint32_t code) {
    if (code < 0x80) {
        out += static_cast<char>(code);
    } else if (code < 0x800) {
        out += static_cast<char>(0xC0 | (code >> 6));
        out += static_cast<char>(0x80 | (code & 0x3F));
    } else if (code < 0x10000) {
        out += static_cast<char>(0xE0 | (code >> 12));
        out += static_cast<char>(0x80 | ((code >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (code & 0x3F));
    } else {
        out += static_cast<char>(0xF0 | (code >> 18));
        out += static_cast<char>(0x80 | ((code >> 12) & 0x3F));
        out += static_cast<char>(0x80 | ((code >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (code & 0x3F));
    }
}

std::string Narrow(std::wstring_view text) {
    std::string out;
    for (std::size_t i = 0; i < text.size(); ++i) {
        std::uint32_t code = static_cast<std::uint16_t>(text[i]);
        if (code >= 0xD800 && code <= 0xDBFF && i + 1 < text.size()) {
            const std::uint32_t low = static_cast<std::uint16_t>(text[i + 1]);
            if (low >= 0xDC00 && low <= 0xDFFF) {
                code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                ++i;
            }
        }
        AppendWtf8(out, code);
    }
    return out;
}

std::filesystem::path OsPath(const std::string& path) { return std::filesystem::path(Widen(path)); }

#else

// os.fsdecode: UTF-8 with surrogateescape for undecodable bytes.
std::string FromOsBytes(std::string_view bytes) {
    std::string out;
    for (std::size_t i = 0; i < bytes.size();) {
        const auto lead = static_cast<unsigned char>(bytes[i]);
        std::size_t length = 0;
        unsigned char low = 0x80;
        unsigned char high = 0xBF;
        if (lead < 0x80) {
            length = 1;
        } else if (lead >= 0xC2 && lead <= 0xDF) {
            length = 2;
        } else if (lead >= 0xE0 && lead <= 0xEF) {
            length = 3;
            low = lead == 0xE0 ? 0xA0 : 0x80;
            high = lead == 0xED ? 0x9F : 0xBF;
        } else if (lead >= 0xF0 && lead <= 0xF4) {
            length = 4;
            low = lead == 0xF0 ? 0x90 : 0x80;
            high = lead == 0xF4 ? 0x8F : 0xBF;
        }
        bool valid = length != 0 && i + length <= bytes.size();
        for (std::size_t k = 1; valid && k < length; ++k) {
            const auto next = static_cast<unsigned char>(bytes[i + k]);
            valid = next >= (k == 1 ? low : 0x80) && next <= (k == 1 ? high : 0xBF);
        }
        if (!valid) {
            const std::uint32_t code = 0xDC00U + lead;
            out += static_cast<char>(0xE0 | (code >> 12));
            out += static_cast<char>(0x80 | ((code >> 6) & 0x3F));
            out += static_cast<char>(0x80 | (code & 0x3F));
            ++i;
            continue;
        }
        out.append(bytes.substr(i, length));
        i += length;
    }
    return out;
}

// os.fsencode: back to bytes, escaped surrogates to their bytes.
std::string ToOsBytes(const std::string& text) {
    std::string out;
    for (std::size_t i = 0; i < text.size(); ++i) {
        const auto lead = static_cast<unsigned char>(text[i]);
        if (lead == 0xED && i + 2 < text.size()) {
            const auto second = static_cast<unsigned char>(text[i + 1]);
            const auto third = static_cast<unsigned char>(text[i + 2]);
            const std::uint32_t code = 0xD000U | ((second & 0x3FU) << 6) | (third & 0x3FU);
            if (code >= 0xDC80 && code <= 0xDCFF) {
                out += static_cast<char>(code - 0xDC00);
                i += 2;
                continue;
            }
        }
        out += text[i];
    }
    return out;
}

std::filesystem::path OsPath(const std::string& path) { return std::filesystem::path(ToOsBytes(path)); }

#endif

// ---- Processes ----

#ifdef _WIN32

// subprocess.list2cmdline
std::wstring CommandLine(const std::vector<std::string>& command) {
    std::wstring line;
    for (const std::string& arg_text : command) {
        const std::wstring arg = Widen(arg_text);
        if (!line.empty()) {
            line += L' ';
        }
        const bool quote = arg.empty() || arg.find_first_of(L" \t") != std::wstring::npos;
        if (quote) {
            line += L'"';
        }
        std::size_t backslashes = 0;
        for (const wchar_t c : arg) {
            if (c == L'\\') {
                ++backslashes;
            } else if (c == L'"') {
                line.append(backslashes * 2, L'\\');
                backslashes = 0;
                line += L"\\\"";
            } else {
                line.append(backslashes, L'\\');
                backslashes = 0;
                line += c;
            }
        }
        line.append(backslashes, L'\\');
        if (quote) {
            line.append(backslashes, L'\\');
            line += L'"';
        }
    }
    return line;
}

HANDLE OpenInheritable(const std::wstring& path, bool write) {
    SECURITY_ATTRIBUTES attributes{};
    attributes.nLength = sizeof(attributes);
    attributes.bInheritHandle = TRUE;
    return CreateFileW(path.c_str(), write ? GENERIC_WRITE : GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                       &attributes, write ? CREATE_ALWAYS : OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
}

// Starts command with stdout and stderr to the two files; std::nullopt when
// it cannot start or does not finish within timeout_ms.
std::optional<DWORD> RunWindows(const std::vector<std::string>& command, const std::wstring& out_path,
                                const std::wstring& err_path, DWORD timeout_ms) {
    HANDLE out = OpenInheritable(out_path, true);
    HANDLE err = OpenInheritable(err_path, true);
    if (out == INVALID_HANDLE_VALUE || err == INVALID_HANDLE_VALUE) {
        if (out != INVALID_HANDLE_VALUE) {
            CloseHandle(out);
        }
        if (err != INVALID_HANDLE_VALUE) {
            CloseHandle(err);
        }
        Fail(ErrorKind::kOs, "cannot create benchmark output files for " + PyRepr(Value::Str(command.front())));
    }
    STARTUPINFOW startup{};
    startup.cb = sizeof(startup);
    startup.dwFlags = STARTF_USESTDHANDLES;
    startup.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
    startup.hStdOutput = out;
    startup.hStdError = err;
    PROCESS_INFORMATION process{};
    std::wstring line = CommandLine(command);
    const BOOL started =
        CreateProcessW(nullptr, line.data(), nullptr, nullptr, TRUE, 0, nullptr, nullptr, &startup, &process);
    CloseHandle(out);
    CloseHandle(err);
    if (!started) {
        return std::nullopt;
    }
    CloseHandle(process.hThread);
    std::optional<DWORD> code;
    if (WaitForSingleObject(process.hProcess, timeout_ms) == WAIT_OBJECT_0) {
        DWORD exit_code = 0;
        if (GetExitCodeProcess(process.hProcess, &exit_code)) {
            code = exit_code;
        }
    } else {
        TerminateProcess(process.hProcess, 1);
        WaitForSingleObject(process.hProcess, INFINITE);
    }
    CloseHandle(process.hProcess);
    return code;
}

#endif

#ifndef _WIN32

// fork and exec command with stdout and stderr on the two descriptors, as
// Python's subprocess does: an exec failure comes back through a close-on-exec
// pipe. The pid, or -1 with the errno in error.
pid_t StartChild(const std::vector<std::string>& command, int out_fd, int err_fd, bool search_path, int& error) {
    std::vector<std::string> bytes;
    for (const std::string& part : command) {
        bytes.push_back(ToOsBytes(part));
    }
    std::vector<char*> argv;
    for (std::string& part : bytes) {
        argv.push_back(part.data());
    }
    argv.push_back(nullptr);
    int report[2];
    if (pipe(report) != 0) {
        error = errno;
        return -1;
    }
    fcntl(report[0], F_SETFD, FD_CLOEXEC);
    fcntl(report[1], F_SETFD, FD_CLOEXEC);
    const pid_t pid = fork();
    if (pid < 0) {
        error = errno;
        close(report[0]);
        close(report[1]);
        return -1;
    }
    if (pid == 0) {
        if (dup2(out_fd, 1) >= 0 && dup2(err_fd, 2) >= 0) {
            if (search_path) {
                execvp(argv[0], argv.data());
            } else {
                execv(argv[0], argv.data());
            }
        }
        const int failure = errno;
        if (write(report[1], &failure, sizeof(failure)) < 0) {
            _exit(127);
        }
        _exit(127);
    }
    close(report[1]);
    int failure = 0;
    ssize_t got = 0;
    while ((got = read(report[0], &failure, sizeof(failure))) < 0 && errno == EINTR) {
    }
    close(report[0]);
    if (got == static_cast<ssize_t>(sizeof(failure))) {
        int status = 0;
        while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {
        }
        error = failure;
        return -1;
    }
    return pid;
}

int WaitChild(pid_t pid) {
    int status = 0;
    while (waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) {
            Fail(ErrorKind::kOs, OsErrorMessage(errno, "waitpid"));
        }
    }
    return status;
}

#endif

ExitStatus SpawnToFiles(const std::vector<std::string>& command, const std::string& stdout_path,
                        const std::string& stderr_path) {
#ifdef _WIN32
    const std::optional<DWORD> code = RunWindows(command, Widen(stdout_path), Widen(stderr_path), INFINITE);
    if (!code) {
        Fail(ErrorKind::kOs, "[WinError " + std::to_string(GetLastError()) + "] cannot start " +
                                 PyRepr(Value::Str(command.front())));
    }
    return ExitStatus{true, static_cast<int>(*code)};
#else
    const int out = open(ToOsBytes(stdout_path).c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (out < 0) {
        Fail(ErrorKind::kOs, OsErrorMessage(errno, stdout_path));
    }
    const int err = open(ToOsBytes(stderr_path).c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (err < 0) {
        const int error = errno;
        close(out);
        Fail(ErrorKind::kOs, OsErrorMessage(error, stderr_path));
    }
    int error = 0;
    const pid_t pid = StartChild(command, out, err, false, error);
    close(out);
    close(err);
    if (pid < 0) {
        Fail(ErrorKind::kOs, OsErrorMessage(error, command.front()));
    }
    const int status = WaitChild(pid);
    if (WIFSIGNALED(status)) {
        return ExitStatus{false, WTERMSIG(status)};
    }
    return ExitStatus{true, WEXITSTATUS(status)};
#endif
}

// subprocess.run(command, check=True, capture_output=True, timeout=10).stdout,
// or std::nullopt where the query fails, as the machine queries read it.
std::optional<std::string> CommandOutput(const std::vector<std::string>& command) {
#ifdef _WIN32
    wchar_t directory[MAX_PATH + 1];
    wchar_t file[MAX_PATH + 1];
    if (GetTempPathW(MAX_PATH + 1, directory) == 0 || GetTempFileNameW(directory, L"cbc", 0, file) == 0) {
        return std::nullopt;
    }
    const std::optional<DWORD> code = RunWindows(command, file, L"NUL", 10000);
    std::optional<std::string> output;
    if (code && *code == 0) {
        std::ifstream in(std::filesystem::path(file), std::ios::binary);
        output = std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    }
    DeleteFileW(file);
    return output;
#else
    int fds[2];
    if (pipe(fds) != 0) {
        return std::nullopt;
    }
    fcntl(fds[0], F_SETFD, FD_CLOEXEC);
    fcntl(fds[1], F_SETFD, FD_CLOEXEC);
    const int null = open("/dev/null", O_WRONLY | O_CLOEXEC);
    int error = 0;
    const pid_t pid = null < 0 ? -1 : StartChild(command, fds[1], null, true, error);
    if (null >= 0) {
        close(null);
    }
    close(fds[1]);
    if (pid < 0) {
        close(fds[0]);
        return std::nullopt;
    }
    std::string output;
    bool timed_out = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (true) {
        const auto left =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        if (left.count() <= 0) {
            timed_out = true;
            break;
        }
        pollfd entry{fds[0], POLLIN, 0};
        const int ready = poll(&entry, 1, static_cast<int>(left.count()));
        if (ready < 0 && errno == EINTR) {
            continue;
        }
        if (ready <= 0) {
            timed_out = ready == 0;
            break;
        }
        char buffer[4096];
        const ssize_t got = read(fds[0], buffer, sizeof(buffer));
        if (got < 0 && errno == EINTR) {
            continue;
        }
        if (got <= 0) {
            break;
        }
        output.append(buffer, static_cast<std::size_t>(got));
    }
    close(fds[0]);
    if (timed_out) {
        kill(pid, SIGKILL);
    }
    const int status = WaitChild(pid);
    if (timed_out || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        return std::nullopt;
    }
    return output;
#endif
}

#if !defined(_WIN32) && !defined(__APPLE__)
std::string TextQuery(const std::vector<std::string>& command) {
    const std::optional<std::string> output = CommandOutput(command);
    if (!output || output->empty()) {
        return "unavailable";
    }
    return Strip(DecodeChildText(*output));
}
#endif

int FreePort() {
#ifdef _WIN32
    WSADATA data;
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) {
        Fail(ErrorKind::kOs, "WSAStartup failed");
    }
    const SOCKET s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == INVALID_SOCKET) {
        WSACleanup();
        Fail(ErrorKind::kOs, "socket failed");
    }
#else
    const int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) {
        Fail(ErrorKind::kOs, OsErrorMessage(errno, "socket"));
    }
#endif
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    sockaddr_in bound{};
    socklen_t length = sizeof(bound);
    const bool ok = bind(s, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0 &&
                    getsockname(s, reinterpret_cast<sockaddr*>(&bound), &length) == 0;
#ifdef _WIN32
    closesocket(s);
    WSACleanup();
#else
    close(s);
#endif
    if (!ok) {
        Fail(ErrorKind::kOs, "cannot bind a free port on 127.0.0.1");
    }
    return ntohs(bound.sin_port);
}

// ---- Paths ----

std::string CurrentDirectory() {
    std::error_code error;
    const std::filesystem::path path = std::filesystem::current_path(error);
    if (error) {
        Fail(ErrorKind::kOs, "[Errno " + std::to_string(error.value()) + "] " + error.message());
    }
#ifdef _WIN32
    return Narrow(path.wstring());
#else
    return FromOsBytes(path.string());
#endif
}

#ifndef _WIN32

// posixpath.normpath
std::string NormPath(const std::string& path) {
    if (path.empty()) {
        return ".";
    }
    std::size_t initial = 0;
    if (path[0] == '/') {
        initial = path.rfind("//", 0) == 0 && path.rfind("///", 0) != 0 ? 2 : 1;
    }
    std::vector<std::string> parts;
    std::size_t start = 0;
    while (start <= path.size()) {
        std::size_t end = path.find('/', start);
        if (end == std::string::npos) {
            end = path.size();
        }
        const std::string part = path.substr(start, end - start);
        start = end + 1;
        if (part.empty() || part == ".") {
            continue;
        }
        if (part != ".." || (initial == 0 && parts.empty()) || (!parts.empty() && parts.back() == "..")) {
            parts.push_back(part);
        } else if (!parts.empty()) {
            parts.pop_back();
        }
    }
    std::string out(initial, '/');
    for (std::size_t i = 0; i < parts.size(); ++i) {
        out += (i == 0 ? "" : "/") + parts[i];
    }
    return out.empty() ? "." : out;
}

std::string JoinPosix(const std::string& a, const std::string& b) {
    if (!b.empty() && b[0] == '/') {
        return b;
    }
    if (a.empty() || a.back() == '/') {
        return a + b;
    }
    return a + "/" + b;
}

std::pair<std::string, std::string> SplitPosix(const std::string& path) {
    const std::size_t slash = path.rfind('/');
    const std::size_t cut = slash == std::string::npos ? 0 : slash + 1;
    std::string head = path.substr(0, cut);
    const std::string tail = path.substr(cut);
    if (!head.empty() && head.find_first_not_of('/') != std::string::npos) {
        while (!head.empty() && head.back() == '/') {
            head.pop_back();
        }
    }
    return {head, tail};
}

// posixpath._joinrealpath, non-strict.
std::pair<std::string, bool> JoinRealPath(std::string path, std::string rest, std::map<std::string, std::optional<std::string>>& seen) {
    if (!rest.empty() && rest[0] == '/') {
        rest = rest.substr(1);
        path = "/";
    }
    while (!rest.empty()) {
        const std::size_t slash = rest.find('/');
        const std::string name = rest.substr(0, slash);
        rest = slash == std::string::npos ? "" : rest.substr(slash + 1);
        if (name.empty() || name == ".") {
            continue;
        }
        if (name == "..") {
            if (!path.empty()) {
                auto [head, tail] = SplitPosix(path);
                path = head;
                if (tail == "..") {
                    path = JoinPosix(JoinPosix(path, ".."), "..");
                }
            } else {
                path = "..";
            }
            continue;
        }
        const std::string next = JoinPosix(path, name);
        struct stat info{};
        const bool is_link = lstat(ToOsBytes(next).c_str(), &info) == 0 && S_ISLNK(info.st_mode);
        if (!is_link) {
            path = next;
            continue;
        }
        const auto cached = seen.find(next);
        if (cached != seen.end()) {
            if (cached->second) {
                path = *cached->second;
                continue;
            }
            return {JoinPosix(next, rest), false};
        }
        seen[next] = std::nullopt;
        std::string target(4096, '\0');
        const ssize_t length = readlink(ToOsBytes(next).c_str(), target.data(), target.size());
        if (length < 0) {
            Fail(ErrorKind::kOs, OsErrorMessage(errno, next));
        }
        target.resize(static_cast<std::size_t>(length));
        auto [resolved, ok] = JoinRealPath(path, FromOsBytes(target), seen);
        if (!ok) {
            return {JoinPosix(resolved, rest), false};
        }
        path = resolved;
        seen[next] = path;
    }
    return {path, true};
}

// pathlib.PurePosixPath: root and the parts without empty or '.' parts.
std::pair<std::string, std::vector<std::string>> PureParts(const std::string& path) {
    std::string root;
    if (!path.empty() && path[0] == '/') {
        root = path.rfind("//", 0) == 0 && path.rfind("///", 0) != 0 ? "//" : "/";
    }
    std::vector<std::string> parts;
    std::size_t start = 0;
    while (start <= path.size()) {
        std::size_t end = path.find('/', start);
        if (end == std::string::npos) {
            end = path.size();
        }
        const std::string part = path.substr(start, end - start);
        if (!part.empty() && part != ".") {
            parts.push_back(part);
        }
        start = end + 1;
    }
    return {root, parts};
}

std::string PureString(const std::string& root, const std::vector<std::string>& parts, std::size_t count) {
    std::string out = root;
    for (std::size_t i = 0; i < count; ++i) {
        out += (i == 0 ? "" : "/") + parts[i];
    }
    return out.empty() ? "." : out;
}

#else

// pathlib.PureWindowsPath, without UNC shares: drive, root and parts.
struct WindowsParts {
    std::string drive;
    std::string root;
    std::vector<std::string> parts;
};

WindowsParts PureWindowsParts(const std::string& path) {
    WindowsParts out;
    std::size_t i = 0;
    if (path.size() >= 2 && path[1] == ':') {
        out.drive = path.substr(0, 2);
        i = 2;
    }
    if (i < path.size() && (path[i] == '\\' || path[i] == '/')) {
        out.root = "\\";
    }
    std::string part;
    for (; i <= path.size(); ++i) {
        if (i == path.size() || path[i] == '\\' || path[i] == '/') {
            if (!part.empty() && part != ".") {
                out.parts.push_back(part);
            }
            part.clear();
        } else {
            part += path[i];
        }
    }
    return out;
}

std::string WindowsString(const WindowsParts& parts, std::size_t count) {
    std::string out = parts.drive + parts.root;
    for (std::size_t i = 0; i < count; ++i) {
        out += (i == 0 ? "" : "\\") + parts.parts[i];
    }
    return out.empty() ? "." : out;
}

#endif

// Lowercase hex of a digest.
std::string HexDigest(const unsigned char* digest, std::size_t size) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out;
    for (std::size_t i = 0; i < size; ++i) {
        out += kDigits[digest[i] >> 4];
        out += kDigits[digest[i] & 0xF];
    }
    return out;
}

class Sha256Stream {
  public:
    Sha256Stream() {
#ifdef CHUNKDB_WITH_OPENSSL
        context_ = EVP_MD_CTX_new();
        if (context_ == nullptr || EVP_DigestInit_ex(context_, EVP_sha256(), nullptr) != 1) {
            Fail(ErrorKind::kRuntime, "OpenSSL SHA256 initialisation failed");
        }
#endif
    }
    ~Sha256Stream() {
#ifdef CHUNKDB_WITH_OPENSSL
        EVP_MD_CTX_free(context_);
#endif
    }
    Sha256Stream(const Sha256Stream&) = delete;
    Sha256Stream& operator=(const Sha256Stream&) = delete;

    void Update(std::string_view data) {
#ifdef CHUNKDB_WITH_OPENSSL
        if (EVP_DigestUpdate(context_, data.data(), data.size()) != 1) {
            Fail(ErrorKind::kRuntime, "OpenSSL SHA256 update failed");
        }
#else
        hash_.Update(data);
#endif
    }

    std::string Hex() {
#ifdef CHUNKDB_WITH_OPENSSL
        unsigned char digest[EVP_MAX_MD_SIZE];
        unsigned int size = 0;
        if (EVP_DigestFinal_ex(context_, digest, &size) != 1) {
            Fail(ErrorKind::kRuntime, "OpenSSL SHA256 final failed");
        }
        return HexDigest(digest, size);
#else
        const crypto::Sha256Digest digest = hash_.Finish();
        return HexDigest(digest.data(), digest.size());
#endif
    }

  private:
#ifdef CHUNKDB_WITH_OPENSSL
    EVP_MD_CTX* context_ = nullptr;
#else
    crypto::Sha256 hash_;
#endif
};

// open(path, "rb") with Python's errors.
std::FILE* OpenForRead(const std::string& path) {
#ifdef _WIN32
    std::FILE* file = _wfopen(Widen(path).c_str(), L"rb");
#else
    std::FILE* file = std::fopen(ToOsBytes(path).c_str(), "rb");
#endif
    if (file == nullptr) {
        Fail(ErrorKind::kOs, OsErrorMessage(errno, path));
    }
    std::error_code error;
    if (std::filesystem::is_directory(OsPath(path), error)) {
        std::fclose(file);
        Fail(ErrorKind::kOs, OsErrorMessage(EISDIR, path));
    }
    return file;
}

template <typename Sink>
void ReadChunks(const std::string& path, Sink&& sink) {
    std::FILE* file = OpenForRead(path);
    std::vector<char> buffer(1024 * 1024);
    while (true) {
        const std::size_t got = std::fread(buffer.data(), 1, buffer.size(), file);
        if (got > 0) {
            sink(std::string_view(buffer.data(), got));
        }
        if (got < buffer.size()) {
            const bool failed = std::ferror(file) != 0;
            const int error = errno;
            std::fclose(file);
            if (failed) {
                Fail(ErrorKind::kOs, OsErrorMessage(error, path));
            }
            return;
        }
    }
}

// ---- Machine ----

#ifdef __APPLE__
std::string PlistString(const std::string& xml, std::string_view key) {
    const std::string marker = "<key>" + std::string(key) + "</key>";
    const std::size_t at = xml.find(marker);
    if (at == std::string::npos) {
        return {};
    }
    std::size_t open = at + marker.size();
    while (open < xml.size() && (xml[open] == ' ' || xml[open] == '\t' || xml[open] == '\n' || xml[open] == '\r')) {
        ++open;
    }
    if (xml.compare(open, 8, "<string>") != 0) {
        return {};
    }
    const std::size_t start = open + 8;
    const std::size_t end = xml.find("</string>", start);
    if (end == std::string::npos) {
        return {};
    }
    std::string value = xml.substr(start, end - start);
    for (const auto& [entity, text] : std::vector<std::pair<std::string, std::string>>{
             {"&lt;", "<"}, {"&gt;", ">"}, {"&quot;", "\""}, {"&apos;", "'"}, {"&amp;", "&"}}) {
        for (std::size_t pos = value.find(entity); pos != std::string::npos; pos = value.find(entity, pos + 1)) {
            value.replace(pos, entity.size(), text);
        }
    }
    return value;
}

bool HasPlistKey(const std::string& xml, std::string_view key) {
    return xml.find("<key>" + std::string(key) + "</key>") != std::string::npos;
}

#endif

#ifndef _WIN32
std::string ReadTextIfFile(const std::string& path, bool& exists) {
    std::error_code error;
    exists = std::filesystem::is_regular_file(OsPath(path), error);
    if (!exists) {
        return {};
    }
    return DecodeChildText(ReadFileBytes(path));
}
#endif

#ifdef __APPLE__
std::optional<std::string> SysctlString(const char* name) {
    std::size_t size = 0;
    if (sysctlbyname(name, nullptr, &size, nullptr, 0) != 0) {
        return std::nullopt;
    }
    std::string value(size, '\0');
    if (sysctlbyname(name, value.data(), &size, nullptr, 0) != 0) {
        return std::nullopt;
    }
    value.resize(std::strlen(value.c_str()));
    return value;
}

std::optional<std::int64_t> SysctlInteger(const char* name) {
    std::int64_t value = 0;
    std::size_t size = sizeof(value);
    if (sysctlbyname(name, &value, &size, nullptr, 0) != 0) {
        return std::nullopt;
    }
    if (size == sizeof(std::int32_t)) {
        std::int32_t narrow = 0;
        std::memcpy(&narrow, &value, sizeof(narrow));
        return narrow;
    }
    return value;
}
#endif

#if !defined(_WIN32) && !defined(__APPLE__)
// platform._platform
std::string PlatformString(const std::vector<std::string>& pieces) {
    std::string text;
    for (const std::string& piece : pieces) {
        if (piece.empty()) {
            continue;
        }
        text += (text.empty() ? "" : "-") + Strip(piece);
    }
    for (char& c : text) {
        if (c == ' ') {
            c = '_';
        } else if (c == '/' || c == '\\' || c == ':' || c == ';' || c == '"' || c == '(' || c == ')') {
            c = '-';
        }
    }
    for (std::size_t at = text.find("unknown"); at != std::string::npos; at = text.find("unknown")) {
        text.erase(at, 7);
    }
    for (std::size_t at = text.find("--"); at != std::string::npos; at = text.find("--")) {
        text.erase(at, 1);
    }
    while (!text.empty() && text.back() == '-') {
        text.pop_back();
    }
    return text;
}
#endif

Value FilesystemInfo(const std::string& directory) {
    Value result = Value::Object();
    result.Set("path", Value::Str(RealPath(directory)));
    result.Set("type", Value::Str("unavailable"));
#if defined(__APPLE__)
    const std::optional<std::string> output = CommandOutput({"df", "-P", directory});
    if (output && !output->empty()) {
        std::string text = DecodeChildText(*output);
        while (!text.empty() && text.back() == '\n') {
            text.pop_back();
        }
        const std::string row = text.substr(text.rfind('\n') == std::string::npos ? 0 : text.rfind('\n') + 1);
        // row.split(None, 5)
        std::vector<std::string> fields;
        std::size_t i = 0;
        while (fields.size() < 5) {
            while (i < row.size() && (row[i] == ' ' || row[i] == '\t')) {
                ++i;
            }
            if (i >= row.size()) {
                break;
            }
            const std::size_t end = row.find_first_of(" \t", i);
            fields.push_back(row.substr(i, end == std::string::npos ? std::string::npos : end - i));
            i = end == std::string::npos ? row.size() : end;
        }
        while (i < row.size() && (row[i] == ' ' || row[i] == '\t')) {
            ++i;
        }
        if (i < row.size()) {
            fields.push_back(row.substr(i));
        }
        if (fields.size() == 6) {
            result.Set("mount", Value::Str(fields.back()));
            const std::optional<std::string> info = CommandOutput({"diskutil", "info", "-plist", fields.back()});
            if (info && !info->empty()) {
                const std::string xml = DecodeChildText(*info);
                const std::string key = HasPlistKey(xml, "FilesystemType") ? "FilesystemType" : "FilesystemName";
                result.Set("type", Value::Str(HasPlistKey(xml, key) ? PlistString(xml, key) : "unavailable"));
            }
        }
    }
#elif defined(_WIN32)
    wchar_t volume[1024];
    if (GetVolumePathNameW(Widen(directory).c_str(), volume, 1024)) {
        wchar_t name[256];
        if (GetVolumeInformationW(volume, nullptr, 0, nullptr, nullptr, nullptr, name, 256)) {
            result.Set("type", Value::Str(Narrow(name)));
            result.Set("mount", Value::Str(Narrow(volume)));
        }
    }
#else
    result.Set("type", Value::Str(TextQuery({"stat", "-f", "-c", "%T", directory})));
#endif
    return result;
}

#ifdef _WIN32
std::string WindowsRelease(DWORD major, DWORD minor, DWORD build, bool client) {
    struct Release {
        DWORD major;
        DWORD minor;
        DWORD build;
        const char* name;
    };
    static constexpr Release kClient[] = {{10, 1, 0, "post11"}, {10, 0, 22000, "11"}, {6, 4, 0, "10"},
                                          {6, 3, 0, "8.1"},     {6, 2, 0, "8"},       {6, 1, 0, "7"},
                                          {6, 0, 0, "Vista"},   {5, 2, 3790, "XP64"}, {5, 2, 0, "XPMedia"},
                                          {5, 1, 0, "XP"},      {5, 0, 0, "2000"}};
    static constexpr Release kServer[] = {{10, 1, 0, "post2025Server"}, {10, 0, 26100, "2025Server"},
                                          {10, 0, 20348, "2022Server"}, {10, 0, 17763, "2019Server"},
                                          {6, 4, 0, "2016Server"},      {6, 3, 0, "2012ServerR2"},
                                          {6, 2, 0, "2012Server"},      {6, 1, 0, "2008ServerR2"},
                                          {6, 0, 0, "2008Server"},      {5, 2, 0, "2003Server"},
                                          {5, 0, 0, "2000Server"}};
    const auto pick = [&](const auto& table) -> std::string {
        for (const Release& release : table) {
            if (std::tie(release.major, release.minor, release.build) <= std::tie(major, minor, build)) {
                return release.name;
            }
        }
        return {};
    };
    return client ? pick(kClient) : pick(kServer);
}
#endif

}  // namespace


// ---- Exported services ----

std::string BytesSha256(std::string_view bytes) {
    Sha256Stream hash;
    hash.Update(bytes);
    return hash.Hex();
}

std::string FileSha256(const std::string& path) {
    Sha256Stream hash;
    ReadChunks(path, [&](std::string_view chunk) { hash.Update(chunk); });
    return hash.Hex();
}

std::string ReadFileBytes(const std::string& path) {
    std::string out;
    ReadChunks(path, [&](std::string_view chunk) { out.append(chunk); });
    return out;
}

void WriteTextFile(const std::string& path, std::string_view text) {
    // Text mode, as Path.write_text writes: newlines become CRLF on Windows.
    std::ofstream out(OsPath(path));
    if (!out) {
        Fail(ErrorKind::kOs, OsErrorMessage(errno != 0 ? errno : EIO, path));
    }
    out.write(text.data(), static_cast<std::streamsize>(text.size()));
    out.close();
    if (!out) {
        Fail(ErrorKind::kOs, OsErrorMessage(EIO, path));
    }
}

void CreateDirectories(const std::string& path) {
    std::error_code error;
    std::filesystem::create_directories(OsPath(path), error);
    if (error) {
        Fail(ErrorKind::kOs, "[Errno " + std::to_string(error.value()) + "] " + error.message() + ": " +
                                 PyRepr(Value::Str(path)));
    }
}

std::int64_t NowMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string BenchBinaryName() {
#ifdef _WIN32
    return "chunkdb_server_bench.exe";
#else
    return "chunkdb_server_bench";
#endif
}

std::string AbsolutePath(const std::string& path) {
#ifdef _WIN32
    const std::wstring wide = Widen(path.empty() ? std::string(".") : path);
    const DWORD size = GetFullPathNameW(wide.c_str(), 0, nullptr, nullptr);
    std::wstring full(size, L'\0');
    const DWORD written = GetFullPathNameW(wide.c_str(), size, full.data(), nullptr);
    if (written == 0 || written >= size) {
        return path;
    }
    full.resize(written);
    return Narrow(full);
#else
    return NormPath(!path.empty() && path[0] == '/' ? path : JoinPosix(CurrentDirectory(), path));
#endif
}

std::string RealPath(const std::string& path) {
#ifdef _WIN32
    const std::string absolute = AbsolutePath(path);
    HANDLE file = CreateFileW(Widen(absolute).c_str(), 0, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                              nullptr, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, nullptr);
    if (file == INVALID_HANDLE_VALUE) {
        return absolute;
    }
    wchar_t buffer[32768];
    const DWORD length = GetFinalPathNameByHandleW(file, buffer, 32768, FILE_NAME_NORMALIZED);
    CloseHandle(file);
    if (length == 0 || length >= 32768) {
        return absolute;
    }
    std::wstring final_path(buffer, length);
    if (final_path.rfind(L"\\\\?\\UNC\\", 0) == 0) {
        final_path = L"\\\\" + final_path.substr(8);
    } else if (final_path.rfind(L"\\\\?\\", 0) == 0) {
        final_path = final_path.substr(4);
    }
    return Narrow(final_path);
#else
    std::map<std::string, std::optional<std::string>> seen;
    const auto [resolved, ok] = JoinRealPath("", path, seen);
    (void)ok;
    return AbsolutePath(resolved);
#endif
}

std::string PathParent(const std::string& path) {
#ifdef _WIN32
    const WindowsParts parts = PureWindowsParts(path);
    return WindowsString(parts, parts.parts.empty() ? 0 : parts.parts.size() - 1);
#else
    const auto [root, parts] = PureParts(path);
    return PureString(root, parts, parts.empty() ? 0 : parts.size() - 1);
#endif
}

std::string PathJoin(const std::string& directory, const std::string& name) {
#ifdef _WIN32
    const WindowsParts parts = PureWindowsParts(directory + "\\" + name);
    return WindowsString(parts, parts.parts.size());
#else
    const auto [root, parts] = PureParts(JoinPosix(directory, name));
    return PureString(root, parts, parts.size());
#endif
}

std::string TemporaryDirectory() {
    // tempfile.gettempdir(): the first candidate where a file can be created.
    std::vector<std::string> candidates;
    for (const char* name : {"TMPDIR", "TEMP", "TMP"}) {
#ifdef _WIN32
        const std::wstring wide(name, name + std::strlen(name));
        const wchar_t* value = _wgetenv(wide.c_str());
        if (value != nullptr && *value != L'\0') {
            candidates.push_back(Narrow(value));
        }
#else
        const char* value = std::getenv(name);
        if (value != nullptr && *value != '\0') {
            candidates.push_back(FromOsBytes(value));
        }
#endif
    }
#ifdef _WIN32
    const wchar_t* profile = _wgetenv(L"USERPROFILE");
    if (profile != nullptr) {
        candidates.push_back(Narrow(profile) + "\\AppData\\Local\\Temp");
    }
    const wchar_t* root = _wgetenv(L"SYSTEMROOT");
    if (root != nullptr) {
        candidates.push_back(Narrow(root) + "\\Temp");
    }
    for (const char* fixed : {"c:\\temp", "c:\\tmp", "\\temp", "\\tmp"}) {
        candidates.emplace_back(fixed);
    }
#else
    for (const char* fixed : {"/tmp", "/var/tmp", "/usr/tmp"}) {
        candidates.emplace_back(fixed);
    }
#endif
    candidates.push_back(CurrentDirectory());
    for (const std::string& candidate : candidates) {
        const std::string directory = AbsolutePath(candidate);
        for (int attempt = 0; attempt < 100; ++attempt) {
            const std::string probe =
                PathJoin(directory, "chunkdb_budget_compare_probe_" + std::to_string(NowMicros()) + "_" +
                                        std::to_string(attempt));
#ifdef _WIN32
            const int fd = _wopen(Widen(probe).c_str(), _O_RDWR | _O_CREAT | _O_EXCL | _O_BINARY, 0600);
#else
            const int fd = open(ToOsBytes(probe).c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
#endif
            if (fd >= 0) {
#ifdef _WIN32
                _close(fd);
                _wunlink(Widen(probe).c_str());
#else
                close(fd);
                unlink(ToOsBytes(probe).c_str());
#endif
                return directory;
            }
            if (errno != EEXIST) {
                break;
            }
        }
    }
    Fail(ErrorKind::kOs, "[Errno 2] No usable temporary directory found in " + PyRepr([&] {
             Value list = Value::Array();
             for (const std::string& candidate : candidates) {
                 list.Append(Value::Str(candidate));
             }
             return list;
         }()));
}

Value LoadAverage() {
#ifdef _WIN32
    return Value();
#else
    double values[3] = {0.0, 0.0, 0.0};
    if (getloadavg(values, 3) != 3) {
        Fail(ErrorKind::kOs, "Load averages are unobtainable");
    }
    return Value::Array({Value::Float(values[0]), Value::Float(values[1]), Value::Float(values[2])});
#endif
}

Value MachineInfo() {
    Value data = Value::Object();
    data.Set("model", Value::Str("unavailable"));
    data.Set("cpu", Value::Str("unavailable"));
#ifdef _WIN32
    const DWORD logical = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
    data.Set("logical_cores", logical > 0 ? Value::Int(BigInt(static_cast<std::int64_t>(logical))) : Value());
#else
    const long logical = sysconf(_SC_NPROCESSORS_ONLN);
    data.Set("logical_cores", logical >= 1 ? Value::Int(BigInt(static_cast<std::int64_t>(logical))) : Value());
#endif
    data.Set("physical_cores", Value());
    data.Set("ram_bytes", Value());
#ifdef _WIN32
    SYSTEM_INFO system{};
    GetNativeSystemInfo(&system);
    std::string architecture;
    switch (system.wProcessorArchitecture) {
        case PROCESSOR_ARCHITECTURE_AMD64: architecture = "AMD64"; break;
        case PROCESSOR_ARCHITECTURE_ARM64: architecture = "ARM64"; break;
        case PROCESSOR_ARCHITECTURE_INTEL: architecture = "x86"; break;
        case PROCESSOR_ARCHITECTURE_ARM: architecture = "ARM"; break;
        default: break;
    }
    data.Set("architecture", Value::Str(architecture));
    data.Set("os", Value::Str("Windows"));
    // win32_ver(): version, service pack and release names from the kernel.
    using RtlGetVersionFunction = LONG(WINAPI*)(OSVERSIONINFOEXW*);
    OSVERSIONINFOEXW version{};
    version.dwOSVersionInfoSize = sizeof(version);
    const HMODULE ntdll = GetModuleHandleW(L"ntdll.dll");
    const auto rtl_get_version = ntdll != nullptr ? reinterpret_cast<RtlGetVersionFunction>(
                                                        reinterpret_cast<void*>(GetProcAddress(ntdll, "RtlGetVersion")))
                                                  : nullptr;
    std::string os_version = "Windows";
    if (rtl_get_version != nullptr && rtl_get_version(&version) == 0) {
        const std::string number = std::to_string(version.dwMajorVersion) + "." +
                                   std::to_string(version.dwMinorVersion) + "." + std::to_string(version.dwBuildNumber);
        std::string csd = "SP" + std::to_string(version.wServicePackMajor);
        if (version.wServicePackMinor != 0) {
            csd += "." + std::to_string(version.wServicePackMinor);
        }
        const std::string release = WindowsRelease(version.dwMajorVersion, version.dwMinorVersion,
                                                   version.dwBuildNumber, version.wProductType == VER_NT_WORKSTATION);
        os_version = "Windows";
        for (const std::string& piece : {release, number, csd}) {
            if (!piece.empty()) {
                os_version += "-" + piece;
            }
        }
    }
    data.Set("os_version", Value::Str(os_version));
#else
    utsname name{};
    uname(&name);
    data.Set("architecture", Value::Str(FromOsBytes(name.machine)));
    data.Set("os", Value::Str(FromOsBytes(name.sysname)));
#ifdef __APPLE__
    data.Set("os_version", Value::Str(""));
#else
    // platform.platform() on Linux: system-release-machine[-processor]-with-libc.
    std::string processor = TextQuery({"uname", "-p"});
    if (processor == "unavailable" || processor == "unknown") {
        processor.clear();
    }
    if (processor == name.machine) {
        processor.clear();
    }
    std::string libc;
#ifdef _CS_GNU_LIBC_VERSION
    const std::size_t libc_size = confstr(_CS_GNU_LIBC_VERSION, nullptr, 0);
    if (libc_size > 0) {
        std::string version(libc_size, '\0');
        confstr(_CS_GNU_LIBC_VERSION, version.data(), libc_size);
        version.resize(std::strlen(version.c_str()));
        const std::size_t space = version.find(' ');
        if (space != std::string::npos) {
            libc = version.substr(0, space) + Strip(version.substr(space + 1));
        }
    }
#endif
    data.Set("os_version", Value::Str(PlatformString({FromOsBytes(name.sysname), FromOsBytes(name.release),
                                                      FromOsBytes(name.machine), processor, "with", libc})));
#endif
#endif
    data.Set("filesystem", FilesystemInfo(TemporaryDirectory()));
    data.Set("load_sampling",
             Value::Str("start and end of each benchmark process; observed ranges, not continuous sampling"));
#if defined(__APPLE__)
    if (const auto model = SysctlString("hw.model"); model && !model->empty()) {
        data.Set("model", Value::Str(Strip(*model)));
    }
    if (const auto cpu = SysctlString("machdep.cpu.brand_string"); cpu && !cpu->empty()) {
        data.Set("cpu", Value::Str(Strip(*cpu)));
    }
    if (const auto cores = SysctlInteger("hw.physicalcpu"); cores && *cores >= 0) {
        data.Set("physical_cores", Value::Int(BigInt(*cores)));
    }
    if (const auto memory = SysctlInteger("hw.memsize"); memory && *memory >= 0) {
        data.Set("ram_bytes", Value::Int(BigInt(*memory)));
    }
    bool exists = false;
    const std::string plist = ReadTextIfFile("/System/Library/CoreServices/SystemVersion.plist", exists);
    data.Set("os_version", Value::Str("macOS " + PlistString(plist, "ProductVersion") + " (" +
                                      FromOsBytes(name.release) + ")"));
#elif defined(_WIN32)
    // Built-in OS queries only; absence remains visible in the report.
    const std::optional<std::string> output = CommandOutput(
        {"powershell.exe", "-NoProfile", "-Command",
         "$c=Get-CimInstance Win32_ComputerSystem; $p=@(Get-CimInstance Win32_Processor); "
         "@{model=$c.Model;cpu=($p.Name -join '; ');ram_bytes=$c.TotalPhysicalMemory;"
         "physical_cores=($p.NumberOfCores | Measure-Object -Sum).Sum} | ConvertTo-Json -Compress"});
    if (output && !output->empty()) {
        const Value queried = ParseJsonBytes(*output);
        if (!queried.IsObject()) {
            Fail(ErrorKind::kType, "machine query did not return an object");
        }
        for (const Member& member : queried.Members()) {
            data.Set(member.key, member.value);
        }
    }
#else
    bool exists = false;
    const std::string product = ReadTextIfFile("/sys/devices/virtual/dmi/id/product_name", exists);
    if (exists) {
        data.Set("model", Value::Str(Strip(product)));
    }
    const std::string cpuinfo = ReadTextIfFile("/proc/cpuinfo", exists);
    if (exists) {
        std::size_t start = 0;
        while (start < cpuinfo.size()) {
            std::size_t end = cpuinfo.find('\n', start);
            if (end == std::string::npos) {
                end = cpuinfo.size();
            }
            const std::string row = cpuinfo.substr(start, end - start);
            start = end + 1;
            if (row.rfind("model name", 0) == 0 && row.find(':') != std::string::npos) {
                data.Set("cpu", Value::Str(Strip(row.substr(row.find(':') + 1))));
                break;
            }
        }
    }
    const std::string meminfo = ReadTextIfFile("/proc/meminfo", exists);
    if (exists) {
        std::size_t start = 0;
        while (start < meminfo.size()) {
            std::size_t end = meminfo.find('\n', start);
            if (end == std::string::npos) {
                end = meminfo.size();
            }
            const std::string row = meminfo.substr(start, end - start);
            start = end + 1;
            if (row.rfind("MemTotal:", 0) == 0) {
                std::string number;
                std::size_t i = 9;
                while (i < row.size() && (row[i] == ' ' || row[i] == '\t')) {
                    ++i;
                }
                while (i < row.size() && row[i] >= '0' && row[i] <= '9') {
                    number += row[i++];
                }
                if (const std::optional<BigInt> kib = BigInt::Parse(number)) {
                    data.Set("ram_bytes", Value::Int(*kib * BigInt(1024)));
                }
            }
        }
    }
    std::set<std::string> topology;
    std::error_code error;
    for (std::filesystem::directory_iterator it("/sys/devices/system/cpu", error), end; !error && it != end;
         it.increment(error)) {
        const std::string entry = it->path().filename().string();
        if (entry.size() < 4 || entry.rfind("cpu", 0) != 0 || entry[3] < '0' || entry[3] > '9') {
            continue;
        }
        const std::string base = it->path().string() + "/topology/";
        bool core_exists = false;
        bool package_exists = false;
        const std::string core = ReadTextIfFile(base + "core_id", core_exists);
        const std::string package = ReadTextIfFile(base + "physical_package_id", package_exists);
        if (core_exists && package_exists) {
            topology.insert(Strip(core) + ":" + Strip(package));
        }
    }
    if (!topology.empty()) {
        data.Set("physical_cores", Value::Int(BigInt(static_cast<std::int64_t>(topology.size()))));
    }
#endif
#ifndef _WIN32
    errno = 0;
    const int niceness = getpriority(PRIO_PROCESS, 0);
    if (!(niceness == -1 && errno != 0)) {
        data.Set("process_niceness", Value::Int(BigInt(niceness)));
    }
#endif
    return data;
}

std::vector<std::string> CommandLineArguments(int argc, char** argv) {
    std::vector<std::string> args;
#ifdef _WIN32
    (void)argc;
    (void)argv;
    int count = 0;
    wchar_t** wide = CommandLineToArgvW(GetCommandLineW(), &count);
    for (int i = 1; wide != nullptr && i < count; ++i) {
        args.push_back(Narrow(wide[i]));
    }
    LocalFree(wide);
#else
    for (int i = 1; i < argc; ++i) {
        args.push_back(FromOsBytes(argv[i]));
    }
#endif
    return args;
}

RunEnvironment SystemRunEnvironment() {
    RunEnvironment environment;
    environment.file_hash = FileSha256;
    environment.load_average = LoadAverage;
    environment.free_port = FreePort;
    environment.now_micros = NowMicros;
    environment.spawn = SpawnToFiles;
    return environment;
}

Hooks SystemHooks() {
    Hooks hooks;
    const RunEnvironment environment = SystemRunEnvironment();
    hooks.run_once = [environment](const RunRequest& request) { return RunOnce(request, environment); };
    hooks.machine_info = MachineInfo;
    hooks.load_average = LoadAverage;
    hooks.file_hash = FileSha256;
    hooks.now_micros = NowMicros;
    return hooks;
}

}  // namespace chunkdb::budget_compare
