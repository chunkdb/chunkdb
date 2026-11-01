#include "server_poller.hpp"

#include <array>
#include <cassert>
#include <cstdio>
#include <future>
#include <limits>
#include <stdexcept>
#include <thread>

#ifndef _WIN32
#include <sys/resource.h>
#endif

namespace {
using namespace chunkdb::server_detail;
using Clock = std::chrono::steady_clock;

struct Loopback {
    SocketHandle listener = kInvalidSocket, writer = kInvalidSocket, reader = kInvalidSocket;
    Loopback() {
        listener = CreateListenSocket("127.0.0.1", 0);
        sockaddr_in address{};
#ifdef _WIN32
        int length = sizeof(address);
#else
        socklen_t length = sizeof(address);
#endif
        assert(getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length) == 0);
        writer = socket(AF_INET, SOCK_STREAM, 0);
        assert(writer != kInvalidSocket);
        assert(connect(writer, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        reader = accept(listener, nullptr, nullptr);
        assert(reader != kInvalidSocket);
    }
    ~Loopback() { CloseSocket(reader); CloseSocket(writer); CloseSocket(listener); }
};

void WakeOnlyAndCapacity() {
    SocketPoller poller(0);
    assert(!poller.Wait({}, 0));
    for (int i = 0; i < 1000; ++i) poller.Wake();
    assert(poller.Wait({}, 0));
    poller.DrainWake();
    assert(!poller.Wait({}, 0));
    poller.Wake();
    assert(poller.Wait({}, 0));
    poller.DrainWake();

    Loopback connection;
    std::array<SocketInterest, 1> interests{{{connection.reader, POLLIN, 0}}};
    bool refused = false;
    try { (void)poller.Wait(interests, 0); }
    catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
    refused = false;
    try { (void)poller.Wait({}, -2); }
    catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
    refused = false;
    try { SocketPoller too_many(std::numeric_limits<std::size_t>::max()); }
    catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}

// Work is published before Wake. A promise identifies each wait boundary; no
// timing assumption is needed whether Wake arrives before or during poll.
void ConcurrentPublication() {
    SocketPoller poller(0);
    std::atomic<std::size_t> published{0};
    for (std::size_t sequence = 1; sequence <= 64; ++sequence) {
        poller.DrainWake();
        std::promise<void> waiting, complete;
        auto entered = waiting.get_future();
        auto done = complete.get_future();
        std::thread owner([&] {
            waiting.set_value();
            const auto deadline = Clock::now() + std::chrono::seconds(5);
            while (published.load(std::memory_order_acquire) != sequence) {
                const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - Clock::now()).count();
                if (remaining <= 0 || !poller.Wait({}, static_cast<int>(remaining))) {
                    std::fprintf(stderr, "poller publication: timed out waiting for sequence %zu\n", sequence);
                    assert(false);
                }
                poller.DrainWake();
            }
            complete.set_value();
        });
        assert(entered.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        published.store(sequence, std::memory_order_release);
        poller.Wake();
        if (done.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
            std::fprintf(stderr, "poller publication: owner did not consume sequence %zu\n", sequence);
            assert(false);
        }
        owner.join();
    }
    poller.DrainWake();
    std::array<std::thread, 4> publishers;
    for (auto& publisher : publishers) publisher = std::thread([&] {
        for (int i = 0; i < 1000; ++i) poller.Wake();
    });
    for (auto& publisher : publishers) publisher.join();
    assert(poller.Wait({}, 0));
    poller.DrainWake();
    assert(!poller.Wait({}, 0));
}

// The existing signal makes the publisher coalesce its notification. The
// relaxed completion marker supplies no acquire for the payload, and the join
// follows the scan. This exercises the contract; native/TSan success alone does
// not prove the C++ ordering argument or require the old code to fail.
void CoalescedRelaxedPublication() {
    SocketPoller poller(0);
    std::atomic<std::size_t> payload{0};
    for (std::size_t sequence = 1; sequence <= 64; ++sequence) {
        poller.Wake();
        assert(poller.Wait({}, 0));
        std::atomic<bool> completed{false};
        std::thread publisher([&] {
            payload.store(sequence, std::memory_order_relaxed);
            poller.Wake();
            completed.store(true, std::memory_order_relaxed);
        });
        const auto deadline = Clock::now() + std::chrono::seconds(5);
        while (!completed.load(std::memory_order_relaxed)) {
            if (Clock::now() >= deadline) {
                std::fprintf(stderr, "coalesced publication: waiting for relaxed completion of sequence %zu\n", sequence);
                assert(false);
            }
            std::this_thread::yield();
        }
        poller.DrainWake();
        assert(payload.load(std::memory_order_relaxed) == sequence);
        assert(!poller.Wait({}, 0));
        publisher.join(); // Deliberately after the owner scan.
    }
}

void ReadinessAndOwnership() {
    Loopback first, second;
    {
        SocketPoller poller(2);
        std::array<SocketInterest, 1> interests{{{first.reader, POLLIN, POLLOUT}}};
        assert(!poller.Wait(interests, 0) && interests[0].ready == 0);
        assert(send(second.writer, "b", 1, 0) == 1);
        assert(!poller.Wait(interests, 0)); // An unregistered socket cannot wake this list.
        assert(send(first.writer, "a", 1, 0) == 1);
        assert(poller.Wait(interests, 5000) && (interests[0].ready & POLLIN));
        char byte = 0;
        assert(recv(first.reader, &byte, 1, 0) == 1 && byte == 'a');
        assert(!poller.Wait(interests, 0) && interests[0].ready == 0);
        // Replace the registration list; readiness refers to the new entry.
        interests[0] = {second.reader, POLLIN, 0};
        assert(poller.Wait(interests, 5000) && (interests[0].ready & POLLIN));
        assert(recv(second.reader, &byte, 1, 0) == 1 && byte == 'b');
        interests[0].events = POLLOUT;
        assert(poller.Wait(interests, 5000) && (interests[0].ready & POLLOUT));
        CloseSocket(second.writer);
        second.writer = kInvalidSocket;
        interests[0].events = POLLIN;
        assert(poller.Wait(interests, 5000));
        assert(interests[0].ready & (POLLIN | POLLHUP));
        assert(recv(second.reader, &byte, 1, 0) == 0);
    }
    // Only the wake pair belongs to the poller. Caller-owned sockets survive.
    assert(send(first.writer, "c", 1, 0) == 1);
    char byte = 0;
    assert(recv(first.reader, &byte, 1, 0) == 1 && byte == 'c');
}

void HighDescriptor() {
#ifndef _WIN32
    Loopback connection;
    rlimit limit{};
    assert(getrlimit(RLIMIT_NOFILE, &limit) == 0);
    constexpr int high = FD_SETSIZE + 16;
    if (limit.rlim_cur <= static_cast<rlim_t>(high)) {
        assert(limit.rlim_max > static_cast<rlim_t>(high));
        limit.rlim_cur = high + 1;
        assert(setrlimit(RLIMIT_NOFILE, &limit) == 0);
    }
    const int duplicate = fcntl(connection.reader, F_DUPFD, high);
    assert(duplicate >= high);
    SocketPoller poller(1);
    std::array<SocketInterest, 1> interests{{{duplicate, POLLIN, 0}}};
    assert(send(connection.writer, "x", 1, 0) == 1);
    assert(poller.Wait(interests, 5000) && (interests[0].ready & POLLIN));
    char byte = 0;
    assert(recv(duplicate, &byte, 1, 0) == 1 && byte == 'x');
    CloseSocket(duplicate);
#endif
}
}  // namespace

int main() {
#ifdef _WIN32
    WSADATA data{};
    assert(WSAStartup(MAKEWORD(2, 2), &data) == 0);
#endif
    WakeOnlyAndCapacity();
    ConcurrentPublication();
    CoalescedRelaxedPublication();
    ReadinessAndOwnership();
    HighDescriptor();
    std::puts("poller passed: bounded lists, coalesced publication, readiness, close, ownership, high descriptors");
#ifdef _WIN32
    WSACleanup();
#endif
}
