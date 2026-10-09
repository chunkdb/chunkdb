// SIGKILL cannot be simulated by an in-process server fixture.
#include <iostream>
#include "server_slots_test_utils.hpp"
#ifndef _WIN32
#include <csignal>
#include <sys/wait.h>

namespace {
using namespace chunkdb::slot_socket_test;
class Process {
  public:
    std::uint16_t port = FreePort();
    Process(const std::string& binary, const std::filesystem::path& directory, const std::string& mode) {
        const std::string port_string = std::to_string(port);
        const std::string path = directory.string();
        pid_ = fork(); assert(pid_ >= 0);
        if (pid_ == 0) {
            signal(SIGPIPE, SIG_DFL);
            execl(binary.c_str(), binary.c_str(), "--host", "127.0.0.1", "--port", port_string.c_str(),
                "--auth", "none", "--data-dir", path.c_str(), "--log-level", "error", "--workers", "4",
                "--durability", mode.c_str(), "--checkpoint-updates", "2", "--checkpoint-wal-bytes", "1048576",
                "--wal-group-commit-updates", "1000", "--slot-sync-ms", "100", static_cast<char*>(nullptr));
            _exit(127);
        }
        const auto deadline = Clock::now() + 20s;
        for (;;) {
            int status = 0;
            if (waitpid(pid_, &status, WNOHANG) == pid_) {
                pid_ = -1; throw std::runtime_error("server exited before listening: " + std::to_string(status));
            }
            try { Client probe(port); probe.Hello(); break; }
            catch (const std::runtime_error&) {
                if (Clock::now() >= deadline) { Kill(); throw; }
                std::this_thread::sleep_for(10ms); // Listener startup only.
            }
        }
    }
    ~Process() { if (pid_ > 0) Kill(); }
    void Kill() {
        assert(pid_ > 0 && kill(pid_, SIGKILL) == 0);
        int status = 0; assert(waitpid(pid_, &status, 0) == pid_);
        assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL); pid_ = -1;
    }
  private:
    pid_t pid_ = -1;
};
void CrashResume(const std::string& binary, const std::string& mode) {
    chunkdb::test::ScopedTempDir directory("chunkdb-slot-sigkill-" + mode);
    std::string epoch;
    std::uint64_t written = 0;
    std::vector<Reply> unacknowledged;
    {
        Process process(binary, directory.path(), mode);
        Client writer(process.port); writer.Hello();
        writer.Ok("CREATE TABLE t (n u8) CHUNK 4 x 4"); writer.Ok("CREATE SLOT 'consumer' ON t");
        Client watch(process.port); watch.Hello();
        epoch = Start(watch.Command("WATCH t SLOT 'consumer'")).first;
        written = Number(writer.Command("SET BLOCK 0 0 IN t n = 1"));
        assert(Change(NextChange(watch)) == written);
        watch.Line("ACK " + std::to_string(written)); Unwatch(watch); // Written position, no timing assumptions.
        assert(Number(Field(Slot(writer.Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == written);
        assert(Start(watch.Command("WATCH t SLOT 'consumer'")) == std::make_pair(epoch, written));
        for (unsigned value = 2; value <= 5; ++value) {
            const auto revision = Number(writer.Command("SET BLOCK 0 0 IN t n = " + std::to_string(value)));
            unacknowledged.push_back(NextChange(watch)); assert(Change(unacknowledged.back()) == revision);
        }
        const auto deleted = Number(writer.Command("DELETE BLOCK 0 0 FROM t"));
        unacknowledged.push_back(NextChange(watch)); assert(Change(unacknowledged.back()) == deleted);
        // Every received frame is through a durable slot frontier. Nothing after
        // the written acknowledgement was ACKed, so the exact suffix must repeat.
        process.Kill();
    }
    {
        Process restarted(binary, directory.path(), mode);
        Client observer(restarted.port); observer.Hello();
        assert(Number(Field(Slot(observer.Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == written);
        Client watch(restarted.port); watch.Hello();
        assert(Start(watch.Command("WATCH t SLOT 'consumer'")) == std::make_pair(epoch, written));
        for (const auto& expected : unacknowledged) assert(NextChange(watch) == expected);
        Unwatch(watch);
        const auto own_position = Change(unacknowledged.back());
        // An independently stored AFTER suppresses the replay suffix without ACK.
        assert(Start(watch.Command("WATCH t SLOT 'consumer' AFTER " + epoch + " " + std::to_string(own_position))) ==
            std::make_pair(epoch, own_position));
        const auto live = Number(observer.Command("SET BLOCK 0 0 IN t n = 9"));
        const auto change = NextChange(watch); assert(Change(change) == live);
        assert(change.items[6].items[0].items[2].type == '_');
        watch.Line("ACK " + std::to_string(live)); Unwatch(watch);
        assert(Number(Field(Slot(observer.Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == live);
        restarted.Kill();
    }
    {
        Process restarted(binary, directory.path(), mode);
        Client watch(restarted.port); watch.Hello();
        const auto start = Start(watch.Command("WATCH t SLOT 'consumer'"));
        assert(start.first == epoch && start.second > Change(unacknowledged.back()));
        assert(!watch.Ready(150ms)); Unwatch(watch);
    }
    std::cout << mode << ": SIGKILL/written ACK/AFTER/live restart passed\n";
}
} // namespace
#endif
int main(int argc, char** argv) {
#ifndef _WIN32
    assert(argc == 2);
    CrashResume(argv[1], "relaxed"); CrashResume(argv[1], "fsync-wal");
#else
    (void)argc; (void)argv;
    std::cout << "SIGKILL slot restart requires POSIX\n";
#endif
}
