#include "chunkdb/engine.hpp"

#include <limits>
#include <stdexcept>

#include "users.hpp"

namespace chunkdb {
namespace {

class ConnectionResponseLimit : public std::length_error {
  public:
    ConnectionResponseLimit() : std::length_error("connection listing exceeds the response limit") {}
};

std::size_t Digits(std::size_t value) {
    std::size_t count = 1U;
    while (value >= 10U) { value /= 10U; ++count; }
    return count;
}

}  // namespace

SessionControlAccess CommandEngine::ControlAccess(SessionState& session) const {
    SessionControlAccess access{.requester_connection_id = session.connection_id};
    if (!config_.require_auth) {
        access.admin_all = true;
        return access;
    }
    if (const User* user = CurrentUser(session)) {
        access.user_incarnation = session.user_incarnation;
        access.admin_all = RightOn(*user, "*") == Right::kAdmin;
    }
    return access;
}

std::string CommandEngine::ListConnections(SessionState& session) const {
    const auto controls = session.connection_controls;
    if (!controls || !controls->enumerate)
        throw std::invalid_argument("SHOW CONNECTIONS is not available on this connection");
    try {
        // The longest array header fits in this prefix, including on 64-bit hosts.
        constexpr std::size_t kPrefix = std::numeric_limits<std::size_t>::digits10 + 4U;
        std::string reply(kPrefix, ' ');
        std::size_t count = 0U;
        std::size_t header = 4U;
        const auto check = [&](std::size_t bytes) {
            const auto body = reply.size() - kPrefix;
            const auto limit = controls->max_response_bytes;
            if (header > limit || body > limit - header || bytes > limit - header - body)
                throw ConnectionResponseLimit();
        };
        const auto bulk = [&](std::string_view value) {
            check(Digits(value.size()) + 5U);
            check(value.size());
            // Check the complete bulk frame before copying its contents.
            const auto framing = Digits(value.size()) + 5U;
            const auto available = controls->max_response_bytes - header - (reply.size() - kPrefix);
            if (value.size() > available - framing) throw ConnectionResponseLimit();
            Protocol::AppendBulk(reply, value);
        };
        check(0U);
        controls->enumerate(ControlAccess(session), [&](const ConnectionInfo& item) {
            if (count == std::numeric_limits<std::size_t>::max()) throw ConnectionResponseLimit();
            header = Digits(count + 1U) + 3U;
            check(4U);
            Protocol::AppendMapHeader(reply, 5U);
            bulk("id");
            std::string id;
            Protocol::AppendInteger(id, item.id);
            check(id.size());
            reply += id;
            const auto field = [&](std::string_view key, std::string_view value, bool nullable) {
                bulk(key);
                if (nullable && value.empty()) { check(3U); Protocol::AppendNull(reply); }
                else bulk(value);
            };
            field("user", item.user, true);
            field("peer", item.peer, false);
            field("state", item.state, false);
            field("statement", item.statement, true);
            ++count;
        });
        std::string prefix;
        Protocol::AppendArrayHeader(prefix, count);
        reply.replace(0U, kPrefix, prefix);
        return reply;
    } catch (const ConnectionResponseLimit& error) {
        return Protocol::Error("OUT_OF_RANGE", error.what(), {{"reason", "response_limit"}});
    }
}

std::string CommandEngine::ControlConnection(SessionState& session, std::uint64_t id, bool kill) const {
    const auto controls = session.connection_controls;
    if (!controls || (kill ? !controls->kill : !controls->cancel))
        throw std::invalid_argument(kill ? "KILL CONNECTION is not available on this connection" :
                                          "CANCEL is not available on this connection");
    const auto reply = [&](bool accepted) {
        std::string out;
        Protocol::AppendMapHeader(out, 1U);
        Protocol::AppendBulk(out, kill ? "killed" : "cancelled");
        Protocol::AppendBoolean(out, accepted);
        return out;
    };
    auto accepted_reply = reply(true);
    auto refused_reply = reply(false);
    if (!kill && id == session.connection_id) return refused_reply;
    const auto access = ControlAccess(session);
    const bool accepted = kill ? controls->kill(access, id) : controls->cancel(access, id);
    if (accepted && kill && id == session.connection_id) session.close_after_reply = true;
    return accepted ? std::move(accepted_reply) : std::move(refused_reply);
}

}  // namespace chunkdb
