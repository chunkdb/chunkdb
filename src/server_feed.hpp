#pragma once
#include "chunkdb/server.hpp"
#include "server_io.hpp"
#include "server_poller.hpp"
#include "server_credit.hpp"
#include "statement_control.hpp"
#include "slot_watch.hpp"
#include <deque>
#include <type_traits>
struct ssl_st;
namespace chunkdb {
struct ConnectionRecord {
    std::mutex mutex;
    std::uint64_t incarnation = 0U;
    std::string user, peer, state = "handshake", verb;
    std::shared_ptr<StatementControl> statement;
    std::atomic<bool> killed{false};
};
struct ServerConnection : public std::enable_shared_from_this<ServerConnection> {
    server_detail::SocketHandle socket = server_detail::kInvalidSocket;
    ssl_st* tls = nullptr;
    SessionState session;
    server_detail::PendingLineBuffer pending;
    bool watch_slot = false;
    bool read_ready = false;
    std::size_t completed_replies = 0U;
    std::chrono::steady_clock::time_point idle_deadline{};
    std::optional<std::size_t> recv_timeout_ms;
    enum class Phase { kStarting, kProxy, kTls, kInput, kReady, kReply };
    Phase phase = Phase::kStarting;
    short io_wait = POLLIN;
    std::chrono::steady_clock::time_point handshake_deadline{};
    server_detail::PhaseDeadline input_deadline, output_deadline;
    bool record_deadline = false;
    std::string handshake_source, proxy_header;
    bool handshake_slot = false;
    std::size_t proxy_size = 16U;
    std::string request_line, payload_rejection, response;
    CommandEngine::PayloadRequest payload;
    std::unique_ptr<server_detail::ParameterFrameDecoder> frames;
    std::vector<std::optional<std::string>> parameters;
    std::size_t response_offset = 0U;
    int response_write_size = 0;
    std::unique_ptr<ReplyContinuation> reply_continuation;
    std::shared_ptr<server_detail::ConnectionByteBudget::Credit> line_credit, payload_credit;
    std::shared_ptr<server_detail::ConnectionByteBudget::Credit> reply_credit;
    std::size_t reply_credit_bytes = 0U;
    bool waiting_credit = false, waiting_line_credit = false, control_request = false, framing_started = false;
    std::size_t payload_credit_bytes = 0U;
    std::shared_ptr<ConnectionRecord> record = std::make_shared<ConnectionRecord>();
    bool worker_queued = false;
    // The client's address and "[address]:port": the socket's peer, or the
    // source a PROXY header carried. Set when the connection starts.
    std::string peer_address;
    std::string peer_endpoint;
};
static_assert(std::is_nothrow_move_constructible_v<ServerConnection>);
static_assert(std::is_nothrow_move_assignable_v<ServerConnection>);
class FeedIo : public std::enable_shared_from_this<FeedIo> {
  public:
    explicit FeedIo(ChunkServer& server);
    ~FeedIo();
    void Start();
    void Stop();
    void Wake() noexcept;
    bool Add(std::shared_ptr<ServerConnection> connection);
    bool RegisterSlot(std::shared_ptr<SlotWatch> state);
  private:
    friend struct FeedDeliveryTestAccess;
    friend struct AckDeadlineIoTestAccess;
    std::atomic<FeedDeliveryTestHook*> hook_{nullptr};
    struct Watch {
        std::shared_ptr<ServerConnection> connection;
        struct Output { std::shared_ptr<const std::string> bytes; std::optional<std::uint64_t> revision{}; bool charged = false; };
        std::deque<Output> output;
        std::size_t offset = 0, bytes = 0;
        int write_size = 0; // TLS retries pin the front buffer and this length.
        short write_wait = POLLOUT, read_wait = POLLIN;
        std::array<char, 4096> input{};
        bool unwatch = false, return_ready = false, close = false, dead = false, read_paused = false;
        bool close_immediate = false;
        std::size_t share = 0U, buffer_bytes = 0U;
        std::chrono::steady_clock::time_point next_progress{};
    };
    void Run();
    void WakeCatchUp() noexcept;
    void CatchUp();
    void Pull(Watch& watch, std::size_t share);
    void Read(Watch& watch);
    void Write(Watch& watch);
    void WritePlain(Watch& watch);
    void Advance(Watch& watch, std::size_t bytes);
    void Queue(Watch& watch, std::shared_ptr<const std::string> bytes, std::optional<std::uint64_t> revision = {}, bool charged = false);
    void Reject(Watch& watch, std::string message);
    ChunkServer& server_;
    server_detail::SocketPoller poller_;
    std::mutex mutex_;
    std::vector<std::shared_ptr<ServerConnection>> incoming_;
    std::thread thread_;
    std::mutex catchup_mutex_;
    std::condition_variable catchup_cv_;
    bool catchup_stop_ = false, catchup_wake_ = false;
    // Every subscription notifies through Wake. Only registered slot watches
    // need the catch-up worker, and one signal covers its whole next scan.
    std::atomic<std::size_t> slot_count_{0};
    std::atomic<bool> catchup_signaled_{false};
    std::vector<std::shared_ptr<SlotWatch>> slots_;
    std::thread catchup_thread_;
};
}
