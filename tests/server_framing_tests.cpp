#include "server_io.hpp"
#include "server_credit.hpp"

#include <array>
#include <cassert>
#include <functional>
#include <atomic>
#include <barrier>
#include <thread>
#include <stdexcept>

namespace {
using namespace chunkdb::server_detail;
using namespace std::string_literals;

void Feed(ParameterFrameDecoder& decoder, PendingLineBuffer& pending,
          std::string_view bytes, std::size_t fragment) {
    for (std::size_t at = 0U; at < bytes.size();) {
        const auto count = std::min(fragment, bytes.size() - at);
        pending.append(bytes.data() + at, count);
        at += count;
        (void)decoder.Consume(pending, 1024U);
    }
}

void FragmentedFramesKeepRequestBoundary() {
    const std::array<std::size_t, 4> limits{5U, 0U, 0U, 3U};
    const std::string bytes = "$5\r\na\0b\nc\r\n$-1\r\n$0\n\n$3\r\nxyz\r\n"s;
    for (const auto fragment : {1U, 2U, 7U, 4096U}) {
        ParameterFrameDecoder decoder({.frame_count = limits.size(), .limits = limits, .total_limit = 8U});
        PendingLineBuffer pending;
        Feed(decoder, pending, bytes + "PING\r\n", fragment);
        assert(decoder.complete() && !decoder.total_exceeded() && decoder.rejection().empty());
        assert(decoder.Consume(pending, 1024U));
        const auto values = decoder.TakeParameters();
        assert(values.size() == 4U && values[0] == std::string("a\0b\nc", 5U));
        assert(!values[1] && values[2] == "" && values[3] == "xyz");
        std::string next;
        assert(pending.extract_line(&next, 1024U) && next == "PING\r\n");
    }
}

void TruncatedInputStaysIncomplete() {
    const std::array<std::size_t, 1> limits{3U};
    const std::string bytes = "$3\r\nabc\r\n";
    for (std::size_t cut = 0U; cut < bytes.size(); ++cut) {
        ParameterFrameDecoder decoder({.frame_count = 1U, .limits = limits});
        PendingLineBuffer pending;
        Feed(decoder, pending, std::string_view(bytes).substr(0U, cut), 1U);
        assert(!decoder.complete());
        Feed(decoder, pending, std::string_view(bytes).substr(cut), 1U);
        assert(decoder.complete() && decoder.TakeParameters()[0] == "abc");
    }
}

void Reject(const std::function<void()>& operation) {
    bool refused = false;
    try { operation(); }
    catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}

void BoundsAndDiscard() {
    const std::array<std::size_t, 2> limits{2U, 2U};
    ParameterFrameDecoder total({.frame_count = 2U, .limits = limits, .total_limit = 3U});
    PendingLineBuffer pending;
    Feed(total, pending, "$2\r\nab\r\n$2\r\n", 1U);
    assert(total.complete() && total.total_exceeded());
    // The refused body was neither allocated nor consumed.
    pending.append("cd\r\n", 4U);
    assert(total.Consume(pending, 1024U) && pending.unconsumed_size() == 4U);

    Reject([&] {
        ParameterFrameDecoder bounded({.frame_count = 2U, .limits = limits});
        PendingLineBuffer input;
        Feed(bounded, input, "$1000000000\r\n", 1U);
    });
    Reject([&] {
        ParameterFrameDecoder bounded({.frame_count = 2U, .limits = limits});
        PendingLineBuffer input;
        Feed(bounded, input, "$1\r\naX\r\n", 1U);
    });
    ParameterFrameDecoder discard({.frame_count = 1U, .total_limit = 32768U,
        .discard = true, .opaque_limit = 32768U});
    PendingLineBuffer discarded;
    Feed(discard, discarded, "$32768\r\n" + std::string(32768U, 'x') + "\r\n", 17U);
    assert(discard.complete() && discard.TakeParameters().empty() && discarded.empty());
    Reject([&] {
        ParameterFrameDecoder bounded({.frame_count = 1U, .discard = true, .opaque_limit = 2U});
        PendingLineBuffer input;
        Feed(bounded, input, "$3\r\n", 1U);
    });
    Reject([&] { ParameterFrameDecoder invalid({.frame_count = 1U}); });
}

void RejectedOptionsDrainWithoutLosingFollowingFrames() {
    const std::array<std::size_t, 2> limits{1U, 1U};
    const std::vector<std::string> rejection{"option is too long", ""};
    ParameterFrameDecoder decoder({.frame_count = 2U, .limits = limits,
        .total_limit = 4U, .rejections = &rejection, .opaque_limit = 3U});
    PendingLineBuffer pending;
    Feed(decoder, pending, "$3\r\nabc\r\n$1\r\nq\r\nPING\r\n", 4096U);
    const auto parameters = decoder.TakeParameters();
    assert(decoder.complete() && decoder.rejection() == rejection.front());
    assert(parameters.size() == 2U && !parameters[0] && parameters[1] == "q");
    std::string next;
    assert(pending.extract_line(&next, 1024U) && next == "PING\r\n");

    const std::array<std::size_t, 1> nullable_limit{0U};
    ParameterFrameDecoder nullable({.frame_count = 1U, .limits = nullable_limit});
    Feed(nullable, pending, "$-1\r\nPING\r\n", 4096U);
    assert(nullable.complete() && !nullable.TakeParameters()[0]);
    assert(pending.extract_line(&next, 1024U) && next == "PING\r\n");
}

void AggregateCreditIsBoundedAndReturnedOnce() {
    std::atomic<std::size_t> returned{0U};
    auto data = std::make_shared<ConnectionByteBudget>(10U, [&] { ++returned; });
    auto controls = std::make_shared<ConnectionByteBudget>(5U, [] {});
    auto first = data->TryAcquire(7U);
    auto second = data->TryAcquire(3U);
    assert(first && second && !data->TryAcquire(1U));
    assert(controls->TryAcquire(5U));
    auto copy = first;
    first.reset();
    assert(returned.load() == 0U && !data->TryAcquire(1U));
    copy.reset();
    assert(returned.load() == 1U && data->TryAcquire(7U));
    Reject([&] { (void)data->TryAcquire(11U); });
    second.reset();
    auto growing = data->TryAcquire(4U);
    auto remainder = data->TryAcquire(6U);
    assert(growing && remainder && !data->TryResize(growing, 5U));
    assert(!controls->TryResize(growing, 5U));
    remainder.reset();
    assert(data->TryResize(growing, 10U) && !data->TryAcquire(1U));
    assert(data->TryResize(growing, 3U));
    auto rest = data->TryAcquire(7U);
    assert(rest && !data->TryAcquire(1U));
    rest.reset(); growing.reset();
    assert(data->TryAcquire(10U));
    std::atomic<std::size_t> admitted{0U};
    auto one = std::make_shared<ConnectionByteBudget>(1U, [] {});
    std::barrier acquired(9), release(9);
    std::vector<std::thread> workers;
    for (std::size_t i = 0U; i < 8U; ++i) workers.emplace_back([&] {
        auto credit = one->TryAcquire(1U);
        if (credit) ++admitted;
        acquired.arrive_and_wait();
        release.arrive_and_wait();
    });
    acquired.arrive_and_wait();
    assert(admitted.load() == 1U);
    release.arrive_and_wait();
    for (auto& worker : workers) worker.join();
    assert(one->TryAcquire(1U));
}
}  // namespace

int main() {
    FragmentedFramesKeepRequestBoundary();
    TruncatedInputStaysIncomplete();
    BoundsAndDiscard();
    RejectedOptionsDrainWithoutLosingFollowingFrames();
    AggregateCreditIsBoundedAndReturnedOnce();
}
