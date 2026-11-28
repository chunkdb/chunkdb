#include "replication_push.hpp"

#include <algorithm>
#include <charconv>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

namespace chunkdb {

namespace {

[[noreturn]] void Bad(const std::string& what) {
    throw std::invalid_argument("malformed replication push: " + what);
}

class Reader {
  public:
    Reader(std::string_view bytes, std::size_t max_values) : bytes_(bytes), values_left_(max_values) {}

    ReplicationPushValue Value(std::size_t depth) {
        if (at_ >= bytes_.size()) Bad("it ends inside a value");
        if (values_left_ == 0U) Bad("it holds more than " + std::to_string(kMaxReplicationPushValues) + " values");
        --values_left_;
        ReplicationPushValue value;
        value.type = bytes_[at_++];
        switch (value.type) {
            case '>':
            case '*': {
                if (depth == 0U) Bad("it nests too deeply");
                const auto count = Count(Line());
                // Every element takes at least three bytes ("_\r\n").
                if (count > (bytes_.size() - at_) / 3U || count > values_left_) Bad("an array is longer than the push");
                // Grown as elements arrive: a declared count reserves little.
                value.items.reserve(std::min<std::size_t>(count, 64U));
                for (std::size_t i = 0; i < count; ++i) value.items.push_back(Value(depth - 1U));
                return value;
            }
            case '$': {
                const auto size = Count(Line());
                if (size > bytes_.size() - at_ || bytes_.size() - at_ - size < 2U) Bad("a string is longer than the push");
                value.text = bytes_.substr(at_, size);
                at_ += size;
                if (bytes_.substr(at_, 2U) != "\r\n") Bad("a string does not end with CRLF");
                at_ += 2U;
                return value;
            }
            case '+':
                value.text = Line();
                return value;
            case ':': {
                const auto line = Line();
                const auto* first = line.data();
                const auto* last = line.data() + line.size();
                if (line.empty() || (line[0] != '-' && (line[0] < '0' || line[0] > '9'))) Bad("an integer is not a number");
                const auto [end, error] = std::from_chars(first, last, value.integer);
                if (error != std::errc{} || end != last) Bad("an integer is not a 64-bit number");
                value.negative = value.integer < 0;
                if (!value.negative) value.unsigned_value = static_cast<std::uint64_t>(value.integer);
                return value;
            }
            case '(': {
                const auto line = Line();
                if (line.empty() || line[0] < '0' || line[0] > '9') Bad("a big number is not an unsigned number");
                const auto [end, error] = std::from_chars(line.data(), line.data() + line.size(), value.unsigned_value);
                if (error != std::errc{} || end != line.data() + line.size()) Bad("a big number does not fit 64 bits");
                return value;
            }
            case '_':
                if (!Line().empty()) Bad("a null has a value");
                return value;
            default:
                Bad("unexpected type '" + std::string(1, value.type) + "'");
        }
    }

    [[nodiscard]] bool done() const noexcept { return at_ == bytes_.size(); }

  private:
    std::string_view Line() {
        const auto end = bytes_.find("\r\n", at_);
        if (end == std::string_view::npos) Bad("a line does not end with CRLF");
        const auto line = bytes_.substr(at_, end - at_);
        at_ = end + 2U;
        return line;
    }

    static std::size_t Count(std::string_view line) {
        std::size_t count = 0U;
        if (line.empty() || line[0] < '0' || line[0] > '9') Bad("a length is not a number");
        const auto [end, error] = std::from_chars(line.data(), line.data() + line.size(), count);
        if (error != std::errc{} || end != line.data() + line.size()) Bad("a length is not a number");
        return count;
    }

    std::string_view bytes_;
    std::size_t values_left_;
    std::size_t at_ = 0U;
};

std::string_view String(const ReplicationPushValue& value, const char* what) {
    if (value.type != '$' && value.type != '+') Bad(std::string(what) + " is not a string");
    return value.text;
}

std::uint64_t Unsigned(const ReplicationPushValue& value, const char* what) {
    if (value.type == '(' || (value.type == ':' && !value.negative)) return value.unsigned_value;
    Bad(std::string(what) + " is not an unsigned integer");
}

std::int64_t Signed(const ReplicationPushValue& value, const char* what) {
    if (value.type != ':') Bad(std::string(what) + " is not an integer");
    return value.integer;
}

bool IsNull(const ReplicationPushValue& value) noexcept { return value.type == '_'; }

const std::vector<ReplicationPushValue>& Array(const ReplicationPushValue& value, const char* what) {
    if (value.type != '*') Bad(std::string(what) + " is not an array");
    return value.items;
}

std::vector<std::optional<std::string>> Row(const ReplicationPushValue& value, const char* what) {
    std::vector<std::optional<std::string>> row;
    const auto& items = Array(value, what);
    row.reserve(items.size());
    for (const auto& item : items) {
        if (IsNull(item)) row.emplace_back(std::nullopt);
        else if (item.type == '$') row.emplace_back(std::string(item.text));
        else Bad(std::string(what) + " holds a value that is not a string or null");
    }
    return row;
}

const std::vector<ReplicationPushValue>& PushItems(const ReplicationPushValue& push) {
    if (push.type != '>' && push.type != '*') Bad("it is not a push");
    if (push.items.empty()) Bad("it is empty");
    return push.items;
}

}  // namespace

ReplicationPushValue DecodeReplicationValue(std::string_view bytes, std::size_t max_depth, std::size_t max_values) {
    Reader reader(bytes, max_values);
    auto value = reader.Value(max_depth);
    if (!reader.done()) Bad("bytes follow the push");
    return value;
}

std::optional<StoreId> ParseEpochHex(std::string_view hex) {
    StoreId epoch{};
    if (hex.size() != epoch.size() * 2U) return std::nullopt;
    const auto digit = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    for (std::size_t i = 0; i < epoch.size(); ++i) {
        const int high = digit(hex[i * 2U]);
        const int low = digit(hex[i * 2U + 1U]);
        if (high < 0 || low < 0) return std::nullopt;
        epoch[i] = static_cast<std::uint8_t>(high * 16 + low);
    }
    return epoch;
}

ReplicationPush ReadReplicationPushHeader(const ReplicationPushValue& value) {
    const auto& items = PushItems(value);
    ReplicationPush push;
    const auto kind = String(items[0], "the push kind");
    if (kind == "replication") {
        if (items.size() != 9U && items.size() != 10U) Bad("a replication push has 9 or 10 elements");
        push.kind = ReplicationPushKind::kReplication;
    } else if (kind == "progress") {
        if (items.size() != 3U) Bad("a progress push has 3 elements");
        push.kind = ReplicationPushKind::kProgress;
    } else {
        throw std::invalid_argument("APPLY takes replication and progress pushes, not " + std::string(kind.substr(0U, 32U)) +
            (kind == "resync" ? "; after a resync the follower must be built again" : ""));
    }
    const auto epoch = ParseEpochHex(String(items[1], "the epoch"));
    if (!epoch) Bad("the epoch is not 32 hex digits");
    push.epoch = *epoch;
    push.revision = Unsigned(items[2], "the revision");
    return push;
}

ReplicationPush ReadReplicationPush(const ReplicationPushValue& value, std::uint8_t dims, std::string_view table) {
    auto push = ReadReplicationPushHeader(value);
    if (push.kind == ReplicationPushKind::kProgress) return push;
    if (dims != 2U && dims != 3U) throw std::logic_error("a table has 2 or 3 dimensions");
    const auto& items = value.items;
    push.commit_time_ms = Unsigned(items[3], "the commit time");
    if (!IsNull(items[4])) push.user = std::string(String(items[4], "the user"));
    push.schema_version = Unsigned(items[5], "the schema version");
    if (!IsNull(items[7])) throw std::invalid_argument("APPLY does not take a push with a tag yet");
    if (!IsNull(items[8])) throw std::invalid_argument("a push of a cross-table transaction needs APPLY GROUP");
    if (items.size() == 10U && !IsNull(items[9])) {
        const auto& tables = items[9];
        if (tables.type != '*' || tables.items.size() != 1U ||
            (tables.items.front().type != '$' && tables.items.front().type != '+') ||
            tables.items.front().text != table) {
            throw std::invalid_argument("APPLY requires txn_tables to be null or a list naming only follower table '" +
                std::string(table) + "'. Cross-table commits need APPLY GROUP");
        }
    }
    const auto& chunks = Array(items[6], "the chunks");
    push.chunks.reserve(chunks.size());
    for (const auto& entry : chunks) {
        const auto& fields = Array(entry, "a chunk");
        if (fields.size() != static_cast<std::size_t>(dims) + 3U) Bad("a chunk does not have the table's axes");
        ReplicatedChunk chunk;
        chunk.coord.x = Signed(fields[0], "a chunk coordinate");
        chunk.coord.y = Signed(fields[1], "a chunk coordinate");
        if (dims == 3U) chunk.coord.z = Signed(fields[2], "a chunk coordinate");
        chunk.version = Unsigned(fields[dims], "a chunk version");
        const auto kind = String(fields[dims + 1U], "a chunk kind");
        const auto& data = fields[dims + 2U];
        if (kind == "form") {
            if (!IsNull(data)) {
                if (data.type != '$') Bad("a chunk form is not a string");
                chunk.form = std::string(data.text);
            }
        } else if (kind == "rows") {
            chunk.rows = true;
            const auto& parts = Array(data, "chunk rows");
            if (parts.size() != 2U) Bad("chunk rows are [blocks, chunk_row]");
            for (const auto& block : Array(parts[0], "the blocks")) {
                const auto& place = Array(block, "a block");
                if (place.size() != static_cast<std::size_t>(dims) + 1U) Bad("a block does not have the table's axes");
                ReplicatedBlockRow row;
                row.x = Signed(place[0], "a block coordinate");
                row.y = Signed(place[1], "a block coordinate");
                if (dims == 3U) row.z = Signed(place[2], "a block coordinate");
                if (!IsNull(place[dims])) row.after = Row(place[dims], "a block row");
                chunk.blocks.push_back(std::move(row));
            }
            if (!IsNull(parts[1])) chunk.chunk_row = Row(parts[1], "the chunk row");
        } else {
            Bad("a chunk kind is form or rows");
        }
        push.chunks.push_back(std::move(chunk));
    }
    return push;
}

}  // namespace chunkdb
